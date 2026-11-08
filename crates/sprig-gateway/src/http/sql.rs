//! `Sql` and `Transactions` (chapter 37, "Transactions"; build step 6):
//! statements from HTTP run through the gateway's executor exactly as
//! statements from the MySQL wire do.
//!
//! **Same path as the wire.** Each request opens a gateway [`Session`] (it
//! shows in `SHOW PROCESSLIST` and answers `KILL`), opens the executor's
//! session for the authenticated principal ([`Executor::open`], as a MySQL
//! login does), runs `USE <database>` through [`SessionExec::init_db`],
//! then hands each request to [`SessionExec::execute`] as a `COM_QUERY`.
//! So name virtualization (chapter 07), privileges, workload capture
//! (chapter 12) and error mapping are the wire's. Statements the gateway
//! answers itself (persona variables, `KILL`, `SHOW PROCESSLIST`) go
//! through the same answers as the MySQL front end
//! ([`crate::mysql::answer`]).
//!
//! **Transactions.** `Begin` keeps the session and its executor in the
//! [`TxnRegistry`] under a handle; calls naming the handle run in it.
//!
//! ```text
//! call with `transaction`:
//!   1. handle names another gateway: forward the call with the caller's
//!      credentials (owner unreachable: 39788; forwarded but not ours: 39781)
//!   2. unknown session: 39781; another principal or tenant: 39783
//!   3. run the call in the session; reset the idle deadline
//! reaper, every second: roll back sessions past either deadline; later use
//! gets 39782
//! ```

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use sprig_api::handle::TxnHandle;
use sprig_api::proto::v1::{ExecuteResponse, QueryResponse, Transaction};
use sprig_auth::Principal;
use sprig_common::ids::TenantId;
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::state::{ProtoState, TlsInfo as ProtoTls};
use sprig_proto_mysql::{Framing, Persona};
use tokio::sync::mpsc;

use super::settings::{ApiSettings, ApiSettingsSource};
use super::sink::{ChannelSink, Item, http_caps};
use crate::frontend::{Protocol, TlsInfo};
use crate::mysql::answer::{LocalEnv, answer};
use crate::mysql::local;
use crate::pipeline::{
    Abandon, ExecCtx, LoginInfo, Request, ResultSink, SessionExec, SharedExecutor,
};
use crate::session::{Session, SessionState, SessionTable};

/// Messages buffered between the executor and the HTTP response.
pub const CHANNEL_DEPTH: usize = 4;
/// How long an expired handle is remembered, so its use gets 39782.
const EXPIRED_MEMORY: Duration = Duration::from_secs(600);
/// Expired handles remembered at most.
const EXPIRED_MAX: usize = 10_000;

/// Who sent a request, and over what.
#[derive(Debug, Clone)]
pub struct Caller {
    /// The authenticated principal.
    pub principal: Principal,
    /// The client address.
    pub peer: SocketAddr,
    /// TLS of the client connection.
    pub tls: Option<TlsInfo>,
    /// The listener (`http` or `https`).
    pub listener: &'static str,
    /// Its generation at accept.
    pub generation: u64,
    /// The `authorization` header, for forwarding.
    pub authorization: Option<String>,
}

/// One statement text to run.
#[derive(Debug, Clone)]
pub struct SqlCall {
    /// The logical database; `-` or empty for none.
    pub database: String,
    /// The text.
    pub sql: String,
    /// `CLIENT_MULTI_STATEMENTS`.
    pub multi: bool,
}

fn no_database(db: &str) -> bool {
    db.is_empty() || db == "-"
}

/// What one session needs between calls.
struct Live {
    session: Session,
    exec: Box<dyn SessionExec>,
    proto: ProtoState,
    db: Option<String>,
}

/// An open transaction handle.
struct TxnEntry {
    handle: TxnHandle,
    tenant: Option<TenantId>,
    user: String,
    deadline: Instant,
    idle_timeout: Duration,
    idle_deadline: Mutex<Instant>,
    expired: AtomicBool,
    /// The session's shared half, to stop a call running past the lifetime.
    session: Arc<crate::session::SessionHandle>,
    live: tokio::sync::Mutex<Option<Live>>,
}

impl TxnEntry {
    fn touch(&self) {
        *self
            .idle_deadline
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Instant::now() + self.idle_timeout;
    }

    fn overdue(&self, now: Instant) -> bool {
        now >= self.deadline
            || now
                >= *self
                    .idle_deadline
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
    }
}

/// The open transaction handles of this gateway (chapter 37's
/// `TxnRegistry`).
#[derive(Default)]
pub struct TxnRegistry {
    next: AtomicU64,
    open: Mutex<HashMap<u64, Arc<TxnEntry>>>,
    expired: Mutex<VecDeque<(u64, Instant)>>,
}

impl std::fmt::Debug for TxnRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnRegistry")
            .field("open", &self.len())
            .finish_non_exhaustive()
    }
}

impl TxnRegistry {
    /// Open handles.
    #[must_use]
    pub fn len(&self) -> usize {
        self.open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// No open handles.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn tenant_open(&self, tenant: Option<TenantId>) -> usize {
        self.open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .filter(|e| e.tenant == tenant)
            .count()
    }

    fn get(&self, id: u64) -> Option<Arc<TxnEntry>> {
        self.open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&id)
            .cloned()
    }

    fn remove(&self, id: u64) -> Option<Arc<TxnEntry>> {
        let e = self
            .open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&id);
        metrics::gauge!("sprig_api_open_transactions").set(gauge(self.len()));
        e
    }

    fn remember_expired(&self, id: u64) {
        let now = Instant::now();
        let mut q = self.expired.lock().unwrap_or_else(PoisonError::into_inner);
        while q
            .front()
            .is_some_and(|(_, t)| now.duration_since(*t) > EXPIRED_MEMORY)
            || q.len() >= EXPIRED_MAX
        {
            q.pop_front();
        }
        q.push_back((id, now));
    }

    fn was_expired(&self, id: u64) -> bool {
        self.expired
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|(i, _)| *i == id)
    }

    /// Waits until no handle is open or `timeout` passes (drain).
    pub async fn wait_empty(&self, timeout: Duration) {
        let end = Instant::now() + timeout;
        while !self.is_empty() && Instant::now() < end {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// A count as a gauge value.
fn gauge(n: usize) -> f64 {
    f64::from(u32::try_from(n).unwrap_or(u32::MAX))
}

/// Unix milliseconds of an `Instant` in the future.
fn unix_ms(at: Instant) -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let ahead = at.saturating_duration_since(Instant::now()).as_millis();
    i64::try_from(now + ahead).unwrap_or(i64::MAX)
}

/// Runs SQL for the HTTP services.
pub struct SqlEngine {
    pub(crate) executor: SharedExecutor,
    pub(crate) sessions: Arc<SessionTable>,
    pub(crate) api: ApiSettingsSource,
    pub(crate) frontend: crate::mysql::settings::SettingsSource,
    pub(crate) hostname: String,
    pub(crate) dc: String,
    pub(crate) gateway_id: String,
    pub(crate) peers: Option<Arc<crate::peer::Peers>>,
    pub(crate) txns: TxnRegistry,
    pub(crate) draining: Arc<AtomicBool>,
}

impl std::fmt::Debug for SqlEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlEngine")
            .field("gateway_id", &self.gateway_id)
            .field("txns", &self.txns)
            .finish_non_exhaustive()
    }
}

fn interrupted() -> SprigError {
    SprigError::standard(1317, *b"70100", "Query execution was interrupted")
}

impl SqlEngine {
    /// The values requests use now.
    #[must_use]
    pub fn settings(&self) -> ApiSettings {
        self.api.current()
    }

    fn proto(&self, caller: &Caller, multi: bool) -> ProtoState {
        let fs = self.frontend.current();
        ProtoState {
            persona: Persona::MySql84,
            caps: http_caps(),
            collation: u16::from(Persona::MySql84.greeting_collation()),
            multi_statements: multi,
            framing: Framing::Plain,
            max_packet: fs.max_allowed_packet,
            tls: caller.tls.as_ref().map(|t| ProtoTls {
                version: t.version,
                cipher: t.cipher,
                sni: t.sni.clone(),
            }),
            nonce: [0; 20],
        }
    }

    /// Opens a session for `caller` with `database` as its default.
    async fn open(&self, caller: &Caller, database: &str, multi: bool) -> Result<Live> {
        let mut session = self.sessions.open_parts(
            Protocol::Http,
            caller.listener,
            caller.generation,
            caller.peer,
            caller.tls.clone(),
        )?;
        session.set_principal(caller.principal.clone());
        session.set_state(SessionState::Ready);
        let proto = self.proto(caller, multi);
        let mut exec = self.executor.open(&LoginInfo {
            principal: caller.principal.clone(),
            backend: caller.principal.backend.clone(),
            persona: Persona::MySql84,
        });
        let mut db = None;
        if !no_database(database) {
            let mut cx = ExecCtx {
                session: &mut session,
                proto: &proto,
                db: None,
            };
            if let Err(e) = exec.init_db(&mut cx, database).await {
                exec.close().await;
                return Err(super::sink::as_wire(&e));
            }
            db = Some(database.to_owned());
            session.set_db(db.clone());
        }
        Ok(Live {
            session,
            exec,
            proto,
            db,
        })
    }

    /// Runs one text in `live`, writing to `sink`.
    async fn statement(&self, live: &mut Live, sql: String, sink: &mut ChannelSink) -> Result<()> {
        let Live {
            session,
            exec,
            proto,
            db,
        } = live;
        session.begin_command("Query", Some(sql.chars().take(1024).collect()));
        let r = self
            .statement_inner(session, exec, proto, db, sql, sink)
            .await;
        session.end_command();
        r
    }

    async fn statement_inner(
        &self,
        session: &mut Session,
        exec: &mut Box<dyn SessionExec>,
        proto: &ProtoState,
        db: &mut Option<String>,
        sql: String,
        sink: &mut ChannelSink,
    ) -> Result<()> {
        enum End {
            Done(Result<crate::pipeline::ExecSummary>),
            Killed,
        }
        let bytes = Bytes::from(sql);
        if let Some(l) = local::classify(&bytes) {
            let fs = self.frontend.current();
            let env = LocalEnv {
                sessions: &self.sessions,
                persona: Persona::MySql84,
                version: fs.version(Persona::MySql84),
                max_allowed_packet: fs.max_allowed_packet,
                hostname: &self.hostname,
                status: sink
                    .last_status
                    .unwrap_or(sprig_proto_mysql::StatusFlags::AUTOCOMMIT),
                peers: self.peers.as_deref(),
            };
            return answer(&l, session, &env, sink).await;
        }
        let handle = Arc::clone(session.handle());
        let mut cx = ExecCtx {
            session,
            proto,
            db: db.as_deref(),
        };
        let req = Request::Query {
            sql: bytes,
            attrs: QueryAttrs::default(),
        };
        let end = tokio::select! {
            biased;
            r = exec.execute(&mut cx, req, sink) => End::Done(r),
            () = handle.flags().query_killed() => End::Killed,
        };
        match end {
            End::Done(Ok(summary)) => {
                if sink.closed() {
                    exec.abandoned(&mut cx, Abandon::ClientGone).await;
                } else if !sink.finished() {
                    sink.error(&SprigError::new(
                        SprigCode::BackendProtocol,
                        "the executor finished without a response",
                    ))
                    .await?;
                }
                if let Some(d) = summary.db {
                    cx.session.set_db(d.clone());
                    *db = d;
                }
                Ok(())
            }
            End::Done(Err(e)) => {
                if sink.closed() {
                    exec.abandoned(&mut cx, Abandon::ClientGone).await;
                    return Ok(());
                }
                if !sink.finished() {
                    sink.error(&e).await?;
                }
                Ok(())
            }
            End::Killed => {
                exec.abandoned(&mut cx, Abandon::Killed).await;
                handle.flags().take_kill_query();
                sink.error(&interrupted()).await
            }
        }
    }

    /// Runs `call` for `caller`, autocommit in a fresh session, or in the
    /// transaction `txn` names. Results and the final error go to `tx`.
    pub async fn run(
        &self,
        caller: &Caller,
        call: SqlCall,
        txn: Option<&str>,
        tx: mpsc::Sender<Item>,
    ) {
        let batch = self.settings().batch_rows;
        let mut sink = ChannelSink::new(tx.clone(), batch);
        let r = match txn.filter(|t| !t.is_empty()) {
            None => self.autocommit(caller, call, &mut sink).await,
            Some(name) => self.in_txn(caller, name, call, &mut sink).await,
        };
        if let Err(e) = r
            && !sink.finished()
        {
            let _ = tx.send(Item::Error(e)).await;
        }
    }

    async fn autocommit(
        &self,
        caller: &Caller,
        call: SqlCall,
        sink: &mut ChannelSink,
    ) -> Result<()> {
        let mut live = self.open(caller, &call.database, call.multi).await?;
        let r = self.statement(&mut live, call.sql, sink).await;
        live.session.set_state(SessionState::Closing);
        live.exec.close().await;
        r
    }

    /// The local entry for `name`, checked against `caller`.
    fn entry(&self, caller: &Caller, h: &TxnHandle) -> Result<Arc<TxnEntry>> {
        let Some(e) = self.txns.get(h.session).filter(|e| e.handle == *h) else {
            if self.txns.was_expired(h.session) {
                return Err(SprigError::new(
                    SprigCode::TxnHandleExpired,
                    "the transaction expired and was rolled back",
                ));
            }
            return Err(SprigError::new(
                SprigCode::TxnHandleUnknown,
                "no open transaction has this handle",
            ));
        };
        if e.tenant != caller.principal.tenant || e.user != caller.principal.user {
            return Err(SprigError::new(
                SprigCode::TxnHandleForbidden,
                "the transaction belongs to another user",
            ));
        }
        Ok(e)
    }

    async fn in_txn(
        &self,
        caller: &Caller,
        name: &str,
        call: SqlCall,
        sink: &mut ChannelSink,
    ) -> Result<()> {
        let h = TxnHandle::parse(name)?;
        let e = self.entry(caller, &h)?;
        let mut guard = e.live.lock().await;
        let Some(live) = guard.as_mut() else {
            return Err(SprigError::new(
                SprigCode::TxnHandleExpired,
                "the transaction expired and was rolled back",
            ));
        };
        if live.session.flags().conn_killed() {
            let live = guard.take();
            drop(guard);
            self.close_entry(&e, live).await;
            return Err(SprigError::new(
                SprigCode::TxnHandleUnknown,
                "the transaction's session was killed",
            ));
        }
        live.proto.multi_statements = call.multi;
        let r = self.statement(live, call.sql, sink).await;
        e.touch();
        if e.expired.load(Ordering::SeqCst) {
            let live = guard.take();
            drop(guard);
            self.close_entry(&e, live).await;
        }
        r
    }

    async fn close_entry(&self, e: &TxnEntry, live: Option<Live>) {
        self.txns.remove(e.handle.session);
        if let Some(mut live) = live {
            live.session.set_state(SessionState::Closing);
            live.exec.close().await;
        }
    }

    /// `Transactions.Begin`.
    ///
    /// # Errors
    /// 39000 while draining; 39001 at `api.txn.max_open_per_tenant`; the
    /// errors of `USE` and `START TRANSACTION`.
    pub async fn begin(
        &self,
        caller: &Caller,
        database: &str,
        read_only: bool,
    ) -> Result<Transaction> {
        if self.draining.load(Ordering::SeqCst) {
            return Err(SprigError::new(
                SprigCode::GatewayDraining,
                "the gateway is draining: begin the transaction on another gateway",
            ));
        }
        let api = self.settings();
        if self.txns.tenant_open(caller.principal.tenant) >= api.txn_max_open {
            return Err(SprigError::new(
                SprigCode::TenantConnectionLimit,
                format!(
                    "the tenant has {} open transactions on this gateway (api.txn.max_open_per_tenant)",
                    api.txn_max_open
                ),
            ));
        }
        let mut live = self.open(caller, database, false).await?;
        let start = if read_only {
            "START TRANSACTION READ ONLY"
        } else {
            "START TRANSACTION"
        };
        let (tx, mut rx) = mpsc::channel(CHANNEL_DEPTH);
        let mut sink = ChannelSink::new(tx, api.batch_rows);
        let r = self.statement(&mut live, start.to_owned(), &mut sink).await;
        drop(sink);
        let mut failed = r.err();
        while let Some(item) = rx.recv().await {
            if let Item::Error(e) = item {
                failed = Some(e);
            }
        }
        if let Some(e) = failed {
            live.exec.close().await;
            return Err(e);
        }
        let id = self.txns.next.fetch_add(1, Ordering::SeqCst) + 1;
        let handle = TxnHandle::new(&self.dc, &self.gateway_id, id)?;
        let now = Instant::now();
        let entry = Arc::new(TxnEntry {
            handle: handle.clone(),
            tenant: caller.principal.tenant,
            user: caller.principal.user.clone(),
            deadline: now + api.txn_lifetime,
            idle_timeout: api.txn_idle,
            idle_deadline: Mutex::new(now + api.txn_idle),
            expired: AtomicBool::new(false),
            session: Arc::clone(live.session.handle()),
            live: tokio::sync::Mutex::new(Some(live)),
        });
        let reply = Transaction {
            name: handle.name(),
            idle_deadline_ms: unix_ms(now + api.txn_idle),
            deadline_ms: unix_ms(entry.deadline),
        };
        self.txns
            .open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id, entry);
        metrics::gauge!("sprig_api_open_transactions").set(gauge(self.txns.len()));
        Ok(reply)
    }

    /// `Transactions.Commit` (`commit`) or `Transactions.Rollback`.
    ///
    /// # Errors
    /// 39781, 39782, 39783 as in the module notes; the `COMMIT`'s error.
    pub async fn end(&self, caller: &Caller, name: &str, commit: bool) -> Result<()> {
        let h = TxnHandle::parse(name)?;
        let e = self.entry(caller, &h)?;
        let mut guard = e.live.lock().await;
        let live = guard.take();
        drop(guard);
        self.txns.remove(h.session);
        let Some(mut live) = live else {
            return Err(SprigError::new(
                SprigCode::TxnHandleExpired,
                "the transaction expired and was rolled back",
            ));
        };
        let (tx, mut rx) = mpsc::channel(CHANNEL_DEPTH);
        let mut sink = ChannelSink::new(tx, 1);
        let stmt = if commit { "COMMIT" } else { "ROLLBACK" };
        let r = self.statement(&mut live, stmt.to_owned(), &mut sink).await;
        drop(sink);
        let mut failed = r.err();
        while let Some(item) = rx.recv().await {
            if let Item::Error(e) = item {
                failed = Some(e);
            }
        }
        live.session.set_state(SessionState::Closing);
        live.exec.close().await;
        failed.map_or(Ok(()), Err)
    }

    /// Rolls back handles past their deadlines, every second until `stop`.
    pub async fn reap(self: Arc<Self>, stop: CancellationToken) {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                () = stop.cancelled() => break,
                _ = tick.tick() => {}
            }
            let now = Instant::now();
            let overdue: Vec<Arc<TxnEntry>> = self
                .txns
                .open
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .values()
                .filter(|e| e.overdue(now))
                .cloned()
                .collect();
            for e in overdue {
                e.expired.store(true, Ordering::SeqCst);
                self.txns.remember_expired(e.handle.session);
                if let Ok(mut guard) = e.live.try_lock() {
                    let live = guard.take();
                    drop(guard);
                    self.close_entry(&e, live).await;
                    metrics::counter!("sprig_api_transactions_expired_total").increment(1);
                } else if now >= e.deadline {
                    // A call is running past the lifetime: stop it; the
                    // call closes the handle when it returns.
                    e.session.flags().kill_query();
                }
            }
        }
        // Shutting down: roll back whatever is left.
        let all: Vec<Arc<TxnEntry>> = self
            .txns
            .open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        for e in all {
            let live = e.live.lock().await.take();
            self.close_entry(&e, live).await;
        }
    }
}

/// Collects a run into `Sql.Execute`'s reply, up to `max_bytes` of cells.
///
/// # Errors
/// The run's error; 39785 past `max_bytes`.
pub async fn collect(mut rx: mpsc::Receiver<Item>, max_bytes: usize) -> Result<ExecuteResponse> {
    let mut parts: Vec<QueryResponse> = Vec::new();
    let mut size = 0usize;
    while let Some(item) = rx.recv().await {
        match item {
            Item::Part(p) => {
                size += prost::Message::encoded_len(&p);
                if size > max_bytes {
                    return Err(SprigError::new(
                        SprigCode::ResultTooLarge,
                        format!(
                            "the result is over api.rest.max_buffered_bytes ({max_bytes}); stream it with sql:query and Accept: application/x-ndjson"
                        ),
                    ));
                }
                parts.push(p);
            }
            Item::Error(e) => return Err(e),
        }
    }
    Ok(sprig_api::results::collect(parts))
}

/// Shared handle on the draining flag.
pub type DrainFlag = Arc<AtomicBool>;

/// A fresh, not-draining flag.
#[must_use]
pub fn drain_flag() -> DrainFlag {
    Arc::new(AtomicBool::new(false))
}
