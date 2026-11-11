//! The Bolt server: handshake, framing and the connection state machine
//! (chapter 43, "State machines" and "Bolt front end"; build steps 17
//! and 18), over a [`BoltService`] that logs in and runs queries.
//!
//! ```text
//! Negotiation ──version──► Connected ──HELLO──► Authentication (5.1+) ──LOGON──► Ready
//! Ready ──RUN──► Streaming ──PULL/DISCARD, has_more false──► Ready
//! Ready ──BEGIN──► TxReady ──RUN──► TxStreaming ──last PULL/DISCARD──► TxReady
//! TxReady ──COMMIT/ROLLBACK──► Ready          Ready ──LOGOFF──► Authentication
//! any ──FAILURE──► Failed ──RESET──► Ready    (in Failed, other requests are IGNORED)
//! any ──GOODBYE, protocol error, drain──► Defunct
//! ```
//!
//! **Results stream.** A `RUN`'s result may carry a [`RecordStream`]:
//! `PULL {n}` takes `n` records from it as the client asks, so the
//! client's pace bounds memory, and the last `SUCCESS` carries the
//! stream's summary. A statement in a transaction while an earlier result
//! is still open first buffers that result, up to
//! [`Config::max_buffered`] bytes (39918 past it), as the chapter says.
//! `DISCARD`, `RESET` and `ROLLBACK` drop open streams.

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::BytesMut;
use sprig_common::CancellationToken;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::message::{BoltVersion, Dechunker, MAGIC, Request, Response, negotiate, write_chunked};
use crate::packstream::PackValue;

/// What the service knows about a connection.
#[derive(Debug, Clone)]
pub struct ConnInfo {
    /// The client address.
    pub peer: SocketAddr,
    /// Whether the listener finished TLS.
    pub tls: bool,
    /// The TLS server name.
    pub sni: Option<String>,
    /// The negotiated version.
    pub version: BoltVersion,
    /// `connection_id` sent in `HELLO`'s answer.
    pub connection_id: String,
    /// `user_agent` from `HELLO`.
    pub user_agent: String,
}

/// A failure sent to the client: a Neo4j status code and a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoltFailure {
    /// `Neo.ClientError.Statement.SyntaxError` and the like.
    pub code: String,
    /// The message.
    pub message: String,
}

impl BoltFailure {
    /// A failure.
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    /// `Neo.ClientError.Request.Invalid`.
    #[must_use]
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new("Neo.ClientError.Request.Invalid", message)
    }
}

/// The end of a streamed result.
#[derive(Debug, Clone, Default)]
pub struct StreamEnd {
    /// Update counters, only those above zero.
    pub stats: BTreeMap<String, i64>,
    /// The bookmark of an auto-commit statement's commit.
    pub bookmark: Option<String>,
}

/// Records produced on demand: what `PULL` takes after a result's
/// buffered records. Dropping it stops the producer.
#[async_trait]
pub trait RecordStream: Send {
    /// The next record; `Ok(None)` at the end, after which [`Self::end`]
    /// holds the summary.
    async fn next(&mut self) -> Result<Option<Vec<PackValue>>, BoltFailure>;

    /// The summary, once [`Self::next`] returned `None`.
    fn end(&mut self) -> StreamEnd;
}

/// One query's result: buffered records, then an optional stream.
#[derive(Default)]
pub struct QueryResult {
    /// Column names.
    pub fields: Vec<String>,
    /// The records.
    pub records: VecDeque<Vec<PackValue>>,
    /// `r`, `w`, `rw` or `s`.
    pub qtype: &'static str,
    /// Update counters (`nodes-created`, ...), only those above zero.
    pub stats: BTreeMap<String, i64>,
    /// The database (graph) it ran in.
    pub db: Option<String>,
    /// Milliseconds until the result was available.
    pub t_first: i64,
    /// More records, produced as the client pulls them.
    pub stream: Option<Box<dyn RecordStream>>,
    /// More entries of the final `SUCCESS` (`plan`, `profile`,
    /// `notifications`).
    pub summary: Vec<(&'static str, PackValue)>,
}

impl std::fmt::Debug for QueryResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResult")
            .field("fields", &self.fields)
            .field("records", &self.records.len())
            .field("qtype", &self.qtype)
            .field("streamed", &self.stream.is_some())
            .finish_non_exhaustive()
    }
}

/// A logged-in session: what a Bolt connection runs queries through.
#[async_trait]
pub trait BoltSession: Send {
    /// Runs `query`. Outside an explicit transaction it commits before it
    /// returns (the result is buffered).
    async fn run(
        &mut self,
        query: &str,
        params: &PackValue,
        extra: &PackValue,
    ) -> Result<QueryResult, BoltFailure>;

    /// `BEGIN` with its extra (`db`, `mode`, `tx_timeout`, `bookmarks`).
    async fn begin(&mut self, extra: &PackValue) -> Result<(), BoltFailure>;

    /// `COMMIT`: the bookmark of the commit.
    async fn commit(&mut self) -> Result<Option<String>, BoltFailure>;

    /// `ROLLBACK`.
    async fn rollback(&mut self) -> Result<(), BoltFailure>;

    /// The bookmark of the last auto-commit `RUN`.
    fn last_bookmark(&self) -> Option<String>;

    /// `RESET`, `LOGOFF` or the end of the connection: roll back whatever
    /// is open.
    async fn reset(&mut self);

    /// `ROUTE`: the routing table (`rt`).
    async fn route(
        &mut self,
        routing: &PackValue,
        extra: &PackValue,
    ) -> Result<PackValue, BoltFailure>;
}

/// Logins.
#[async_trait]
pub trait BoltService: Send + Sync {
    /// Logs in with an auth token (`scheme`, `principal`, `credentials`).
    async fn logon(
        &self,
        info: &ConnInfo,
        auth: &PackValue,
    ) -> Result<Box<dyn BoltSession>, BoltFailure>;
}

/// Server settings.
#[derive(Debug, Clone)]
pub struct Config {
    /// `server` in `HELLO`'s answer (`bolt.server_agent`).
    pub server_agent: String,
    /// Versions offered, highest first, up to `bolt.max_version`.
    pub versions: Vec<BoltVersion>,
    /// `bolt.recv_timeout_seconds`: a hint; idle connections close after
    /// twice it.
    pub recv_timeout: Duration,
    /// Time to finish the handshake.
    pub handshake_timeout: Duration,
    /// Largest message accepted.
    pub max_message: usize,
    /// Prefix of `connection_id`.
    pub id_prefix: String,
    /// Bytes of an open result buffered when a transaction runs another
    /// statement (`bolt.max_buffered_result_mb`); 39918 past it.
    pub max_buffered: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server_agent: "Neo4j/5.26.0".into(),
            versions: crate::message::SUPPORTED.to_vec(),
            recv_timeout: Duration::from_secs(120),
            handshake_timeout: Duration::from_secs(10),
            max_message: 16 << 20,
            id_prefix: "bolt-".into(),
            max_buffered: 64 << 20,
        }
    }
}

static NEXT_CONN: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Connected,
    Authentication,
    Ready,
    Streaming,
    TxReady,
    TxStreaming,
    Failed,
    Defunct,
}

/// Where a connection ended, for logs and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum End {
    /// No version in common (39912): `00 00 00 00` was sent.
    NoVersion,
    /// The client said `GOODBYE` or closed.
    Closed,
    /// A protocol error or failed login closed it.
    Defunct(String),
    /// Idle past twice the receive timeout, or the handshake timed out.
    Timeout,
    /// The gateway drains.
    Drained,
}

struct Conn {
    state: State,
    /// Failed came from this state's family: in a transaction or not.
    in_tx: bool,
    info: ConnInfo,
    session: Option<Box<dyn BoltSession>>,
    results: Vec<(i64, QueryResult)>,
    next_qid: i64,
    out: BytesMut,
}

fn int_of(extra: &PackValue, key: &str, default: i64) -> i64 {
    extra
        .get(key)
        .and_then(PackValue::as_int)
        .unwrap_or(default)
}

impl Conn {
    fn send(&mut self, r: &Response) {
        write_chunked(&r.encode(), &mut self.out);
    }

    fn fail(&mut self, f: &BoltFailure) {
        metrics::counter!("sprig_bolt_messages_total", "message" => "FAILURE", "result" => "sent")
            .increment(1);
        self.send(&Response::Failure {
            code: f.code.clone(),
            message: f.message.clone(),
        });
        self.in_tx = matches!(self.state, State::TxReady | State::TxStreaming);
        self.results.clear();
        self.state = State::Failed;
    }

    fn success(&mut self, m: Vec<(&str, PackValue)>) {
        self.send(&Response::Success(PackValue::map(m)));
    }

    async fn reset_session(&mut self) {
        self.results.clear();
        if let Some(s) = self.session.as_mut() {
            s.reset().await;
        }
    }

    fn hello_answer(&mut self, cfg: &Config) {
        let mut hints = vec![(
            "connection.recv_timeout_seconds",
            PackValue::Int(i64::try_from(cfg.recv_timeout.as_secs()).unwrap_or(120)),
        )];
        if self.info.version >= BoltVersion::new(5, 4) {
            hints.push(("telemetry.enabled", PackValue::Bool(false)));
        }
        self.success(vec![
            ("server", PackValue::str(cfg.server_agent.clone())),
            (
                "connection_id",
                PackValue::str(self.info.connection_id.clone()),
            ),
            ("hints", PackValue::map(hints)),
        ]);
    }

    async fn logon(&mut self, svc: &dyn BoltService, auth: &PackValue) -> bool {
        match svc.logon(&self.info, auth).await {
            Ok(s) => {
                metrics::counter!("sprig_bolt_auth_total", "scheme" => scheme_of(auth), "result" => "ok")
                    .increment(1);
                self.session = Some(s);
                self.state = State::Ready;
                true
            }
            Err(f) => {
                metrics::counter!("sprig_bolt_auth_total", "scheme" => scheme_of(auth), "result" => "refused")
                    .increment(1);
                self.fail(&f);
                self.state = State::Defunct;
                false
            }
        }
    }

    /// The result `qid` names (-1: the last one).
    fn result_index(&self, qid: i64) -> Option<usize> {
        if qid < 0 {
            return self.results.len().checked_sub(1);
        }
        self.results.iter().position(|(q, _)| *q == qid)
    }

    #[allow(clippy::too_many_lines)] // take, look ahead, summarize
    async fn pull(&mut self, extra: &PackValue, discard: bool) {
        let n = int_of(extra, "n", -1);
        let qid = int_of(extra, "qid", -1);
        let Some(i) = self.result_index(qid) else {
            self.fail(&BoltFailure::invalid("no result to pull"));
            return;
        };
        if discard && n < 0 {
            // Everything: drop the stream (its producer stops).
            let r = &mut self.results[i].1;
            r.records.clear();
            r.stream = None;
        }
        let take = if n < 0 {
            usize::MAX
        } else {
            usize::try_from(n).unwrap_or(usize::MAX)
        };
        let mut rows = Vec::new();
        let mut end: Option<StreamEnd> = None;
        let mut failed: Option<BoltFailure> = None;
        {
            let r = &mut self.results[i].1;
            let mut sent = 0usize;
            while sent < take {
                let rec = if let Some(rec) = r.records.pop_front() {
                    rec
                } else if let Some(s) = r.stream.as_mut() {
                    match s.next().await {
                        Ok(Some(rec)) => rec,
                        Ok(None) => {
                            end = Some(s.end());
                            r.stream = None;
                            break;
                        }
                        Err(f) => {
                            failed = Some(f);
                            break;
                        }
                    }
                } else {
                    break;
                };
                if !discard {
                    rows.push(rec);
                }
                sent += 1;
            }
            // Look ahead one record, so `has_more` is exact.
            if failed.is_none()
                && r.records.is_empty()
                && let Some(s) = r.stream.as_mut()
            {
                match s.next().await {
                    Ok(Some(rec)) => r.records.push_back(rec),
                    Ok(None) => {
                        end = Some(s.end());
                        r.stream = None;
                    }
                    Err(f) => failed = Some(f),
                }
            }
        }
        for rec in rows {
            self.send(&Response::Record(rec));
        }
        if let Some(f) = failed {
            self.fail(&f);
            return;
        }
        let r = &self.results[i].1;
        if r.records.is_empty() && r.stream.is_none() {
            let (_, r) = self.results.remove(i);
            let mut stats = r.stats;
            let mut bookmark = None;
            if let Some(e) = end {
                for (k, v) in e.stats {
                    *stats.entry(k).or_default() += v;
                }
                bookmark = e.bookmark;
            }
            let mut meta = vec![
                ("type", PackValue::str(r.qtype)),
                ("t_last", PackValue::Int(0)),
            ];
            if let Some(db) = r.db {
                meta.push(("db", PackValue::String(db)));
            }
            meta.extend(r.summary);
            if !stats.is_empty() {
                meta.push((
                    "stats",
                    PackValue::Map(
                        stats
                            .into_iter()
                            .map(|(k, v)| (k, PackValue::Int(v)))
                            .collect(),
                    ),
                ));
            }
            if self.state == State::Streaming
                && let Some(b) =
                    bookmark.or_else(|| self.session.as_ref().and_then(|s| s.last_bookmark()))
            {
                meta.push(("bookmark", PackValue::String(b)));
            }
            self.success(meta);
            self.state = match self.state {
                State::TxStreaming if !self.results.is_empty() => State::TxStreaming,
                State::TxStreaming => State::TxReady,
                _ => State::Ready,
            };
        } else {
            self.success(vec![("has_more", PackValue::Bool(true))]);
        }
    }

    /// Buffers every open stream (a transaction runs another statement
    /// while earlier results are open), up to `limit` bytes per result.
    async fn buffer_streams(&mut self, limit: usize) -> Result<(), BoltFailure> {
        for (_, r) in &mut self.results {
            let Some(mut s) = r.stream.take() else {
                continue;
            };
            let mut bytes: usize = r.records.iter().flatten().map(PackValue::encoded_len).sum();
            while let Some(rec) = s.next().await? {
                bytes += rec.iter().map(PackValue::encoded_len).sum::<usize>();
                if bytes > limit {
                    return Err(BoltFailure::new(
                        "Neo.ClientError.Statement.SprigBoltResultBufferLimit",
                        format!(
                            "an open result is larger than bolt.max_buffered_result_mb = {} (39918)",
                            limit >> 20
                        ),
                    ));
                }
                r.records.push_back(rec);
            }
            let e = s.end();
            for (k, v) in e.stats {
                *r.stats.entry(k).or_default() += v;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)] // one arm per message
    async fn handle(&mut self, req: Request, svc: &dyn BoltService, cfg: &Config) {
        let name = req.name();
        if self.state == State::Failed && !matches!(req, Request::Reset | Request::Goodbye) {
            self.send(&Response::Ignored);
            metrics::counter!("sprig_bolt_messages_total", "message" => name, "result" => "ignored")
                .increment(1);
            return;
        }
        metrics::counter!("sprig_bolt_messages_total", "message" => name, "result" => "received")
            .increment(1);
        match (self.state, req) {
            (_, Request::Goodbye) => {
                self.reset_session().await;
                self.state = State::Defunct;
            }
            (_, Request::Reset) => {
                self.reset_session().await;
                self.state = if self.session.is_some() {
                    State::Ready
                } else if self.state == State::Connected {
                    State::Connected
                } else {
                    State::Authentication
                };
                self.success(vec![]);
            }
            (State::Connected, Request::Hello(extra)) => {
                extra
                    .get("user_agent")
                    .and_then(PackValue::as_str)
                    .unwrap_or_default()
                    .clone_into(&mut self.info.user_agent);
                if self.info.version < BoltVersion::new(5, 1) {
                    // 5.0: credentials ride in HELLO.
                    if self.logon(svc, &extra).await {
                        self.hello_answer(cfg);
                    }
                } else {
                    self.state = State::Authentication;
                    self.hello_answer(cfg);
                }
            }
            (State::Authentication, Request::Logon(auth)) => {
                if self.logon(svc, &auth).await {
                    self.success(vec![]);
                }
            }
            (State::Ready, Request::Logoff) => {
                self.reset_session().await;
                self.session = None;
                self.state = State::Authentication;
                self.success(vec![]);
            }
            (State::Ready | State::TxReady, Request::Telemetry(_)) => {
                self.success(vec![]);
            }
            (
                State::Ready | State::TxReady,
                Request::Run {
                    query,
                    params,
                    extra,
                },
            ) => {
                let in_tx = self.state == State::TxReady;
                let started = Instant::now();
                let Some(s) = self.session.as_mut() else {
                    self.fail(&BoltFailure::invalid("not logged in"));
                    return;
                };
                match s.run(&query, &params, &extra).await {
                    Ok(mut r) => {
                        r.t_first = i64::try_from(started.elapsed().as_millis()).unwrap_or(0);
                        let fields = PackValue::List(
                            r.fields.iter().cloned().map(PackValue::String).collect(),
                        );
                        let mut meta =
                            vec![("fields", fields), ("t_first", PackValue::Int(r.t_first))];
                        let qid = self.next_qid;
                        if in_tx {
                            self.next_qid += 1;
                            meta.push(("qid", PackValue::Int(qid)));
                        } else {
                            self.results.clear();
                        }
                        self.results.push((qid, r));
                        self.success(meta);
                        self.state = if in_tx {
                            State::TxStreaming
                        } else {
                            State::Streaming
                        };
                    }
                    Err(f) => self.fail(&f),
                }
            }
            (
                State::TxStreaming,
                Request::Run {
                    query,
                    params,
                    extra,
                },
            ) => {
                // Another statement in the transaction while an earlier
                // result is still open: allowed, results keep their qid;
                // open streams are buffered first (they hold the
                // transaction's connection).
                if let Err(f) = self.buffer_streams(cfg.max_buffered).await {
                    self.fail(&f);
                    return;
                }
                self.state = State::TxReady;
                Box::pin(self.handle(
                    Request::Run {
                        query,
                        params,
                        extra,
                    },
                    svc,
                    cfg,
                ))
                .await;
                if self.state == State::TxReady && !self.results.is_empty() {
                    self.state = State::TxStreaming;
                }
            }
            (State::Streaming | State::TxStreaming, Request::Pull(extra)) => {
                self.pull(&extra, false).await;
            }
            (State::Streaming | State::TxStreaming, Request::Discard(extra)) => {
                self.pull(&extra, true).await;
            }
            (State::Ready, Request::Begin(extra)) => {
                let Some(s) = self.session.as_mut() else {
                    self.fail(&BoltFailure::invalid("not logged in"));
                    return;
                };
                match s.begin(&extra).await {
                    Ok(()) => {
                        self.next_qid = 0;
                        self.state = State::TxReady;
                        self.success(vec![]);
                    }
                    Err(f) => self.fail(&f),
                }
            }
            (State::TxReady | State::TxStreaming, Request::Commit) => {
                self.results.clear();
                let Some(s) = self.session.as_mut() else {
                    self.fail(&BoltFailure::invalid("not logged in"));
                    return;
                };
                match s.commit().await {
                    Ok(b) => {
                        self.state = State::Ready;
                        let mut m = vec![];
                        if let Some(b) = b {
                            m.push(("bookmark", PackValue::String(b)));
                        }
                        self.success(m);
                    }
                    Err(f) => {
                        self.fail(&f);
                        self.in_tx = false;
                    }
                }
            }
            (State::TxReady | State::TxStreaming, Request::Rollback) => {
                self.results.clear();
                if let Some(s) = self.session.as_mut() {
                    let _ = s.rollback().await;
                }
                self.state = State::Ready;
                self.success(vec![]);
            }
            (State::Ready, Request::Route { routing, extra, .. }) => {
                let Some(s) = self.session.as_mut() else {
                    self.fail(&BoltFailure::invalid("not logged in"));
                    return;
                };
                match s.route(&routing, &extra).await {
                    Ok(rt) => self.success(vec![("rt", rt)]),
                    Err(f) => self.fail(&f),
                }
            }
            (state, req) => {
                self.fail(&BoltFailure::invalid(format!(
                    "{} is not valid in state {state:?}",
                    req.name()
                )));
                if matches!(state, State::Connected | State::Authentication) {
                    self.state = State::Defunct;
                }
            }
        }
    }
}

fn scheme_of(auth: &PackValue) -> &'static str {
    match auth.get("scheme").and_then(PackValue::as_str) {
        Some("basic") => "basic",
        Some("bearer") => "bearer",
        Some("none") => "none",
        _ => "other",
    }
}

/// Serves one connection until it ends.
///
/// # Errors
/// I/O errors of the socket.
pub async fn serve<S>(
    mut io: S,
    peer: SocketAddr,
    tls: bool,
    sni: Option<String>,
    svc: &dyn BoltService,
    cfg: &Config,
    close: CancellationToken,
) -> std::io::Result<End>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let mut hs = [0u8; 20];
    match tokio::time::timeout(cfg.handshake_timeout, io.read_exact(&mut hs)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(End::Closed),
        Ok(Err(e)) => return Err(e),
        Err(_) => return Ok(End::Timeout),
    }
    if hs[..4] != MAGIC {
        return Ok(End::Defunct("bad magic preamble".into()));
    }
    let mut props = [[0u8; 4]; 4];
    for (i, p) in props.iter_mut().enumerate() {
        p.copy_from_slice(&hs[4 + i * 4..8 + i * 4]);
    }
    let Some(version) = negotiate(&props, &cfg.versions) else {
        tracing::info!(%peer, proposals = ?props, code = 39912, "Bolt: no version in common");
        io.write_all(&[0, 0, 0, 0]).await?;
        return Ok(End::NoVersion);
    };
    io.write_all(&version.to_bytes()).await?;
    let id = NEXT_CONN.fetch_add(1, Ordering::Relaxed);
    let mut conn = Conn {
        state: State::Connected,
        in_tx: false,
        info: ConnInfo {
            peer,
            tls,
            sni,
            version,
            connection_id: format!("{}{id}", cfg.id_prefix),
            user_agent: String::new(),
        },
        session: None,
        results: Vec::new(),
        next_qid: 0,
        out: BytesMut::new(),
    };
    let gauge = metrics::gauge!("sprig_bolt_connections", "version" => version.to_string());
    gauge.increment(1.0);
    let r = run_loop(&mut io, &mut conn, svc, cfg, close).await;
    gauge.decrement(1.0);
    conn.reset_session().await;
    r
}

async fn run_loop<S>(
    io: &mut S,
    conn: &mut Conn,
    svc: &dyn BoltService,
    cfg: &Config,
    close: CancellationToken,
) -> std::io::Result<End>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let mut inbuf = BytesMut::with_capacity(8192);
    let mut dechunk = Dechunker::new(cfg.max_message);
    let idle = cfg
        .recv_timeout
        .saturating_mul(2)
        .max(Duration::from_secs(1));
    loop {
        // Every complete message in the buffer, then one flush.
        loop {
            match dechunk.next(&mut inbuf) {
                Ok(Some(msg)) => match Request::decode(&msg) {
                    Ok(req) => conn.handle(req, svc, cfg).await,
                    Err(e) => {
                        conn.fail(&BoltFailure::invalid(e.to_string()));
                        conn.state = State::Defunct;
                    }
                },
                Ok(None) => break,
                Err(e) => {
                    conn.fail(&BoltFailure::invalid(e.to_string()));
                    conn.state = State::Defunct;
                }
            }
            if conn.state == State::Defunct {
                break;
            }
        }
        if !conn.out.is_empty() {
            io.write_all(&conn.out).await?;
            io.flush().await?;
            conn.out.clear();
        }
        if conn.state == State::Defunct {
            return Ok(End::Defunct("closed by the server".into()));
        }
        // Drain: close between requests, outside transactions.
        let idle_now = matches!(
            conn.state,
            State::Ready | State::Connected | State::Authentication
        );
        if close.is_cancelled() && idle_now {
            return Ok(End::Drained);
        }
        let read = tokio::select! {
            r = tokio::time::timeout(idle, io.read_buf(&mut inbuf)) => r,
            () = close.cancelled(), if idle_now => return Ok(End::Drained),
        };
        match read {
            Ok(Ok(0)) => return Ok(End::Closed),
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => return Ok(End::Timeout),
        }
    }
}
