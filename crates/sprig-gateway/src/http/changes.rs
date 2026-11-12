//! Change streams over the API (chapter 37, "Change streams"; chapter 38;
//! build step 11; RLS round 5, decision RL45): `Changes.Watch` over gRPC
//! streaming, and `GET /v1/databases/{db}/changes` as Server-Sent Events
//! (`Accept: text/event-stream`), WebSocket (`Upgrade: websocket`, HTTP/1.1)
//! or NDJSON (anything else).
//!
//! ```text
//! watch(caller, database, objects, start_after | Last-Event-ID | now):
//!   every requested table: SELECT (the image check below, no image), else 1142
//!   tail the tenant's feed (chapter 38's TenantTail) from the token or now
//!   per batch of feed events (up to 256, or what arrived within 20 ms):
//!     events of the database (of `objects` when given; internal tables never)
//!     the images checked in one session of the caller (exec::rls_images):
//!       SELECT on the table, and its row-level security with the caller's
//!       session values; checked at every batch, so a grant, role or policy
//!       change applies from the next one
//!     insert: after visible        delete: before visible
//!     update: both → update; before only → delete; after only → insert
//!     ddl, truncate: SELECT on the table
//!   no event for api.stream heartbeat (10 s): a heartbeat with the last token
//!   framing: gRPC one ChangeEvent; SSE "id: <token>\nevent: <op>\ndata: <JSON>\n\n";
//!   WebSocket one text frame of JSON; NDJSON one JSON line
//! ```
//!
//! The resume token of an event the caller may not see still moves the
//! stream on (heartbeats carry it), so a resumed stream never sees it
//! either. `start_at_hlc` is refused (39802): the feed can't position by
//! time yet.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use serde_json::{Value, json};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_feed::{
    Clusters, FeedItem, Op, PlacementView, ResumeToken, StartAt, TailOptions, TenantTail,
};
use tokio::sync::mpsc;

use super::Core;
use super::sql::{Caller, SqlCall, collect};
use crate::exec::rls_images::{ImagesRequest, TableImages, answer, statement};

/// Events kept for a slow client before the tail waits.
const DEPTH: usize = 256;
/// A heartbeat after this long without an event.
const HEARTBEAT: Duration = Duration::from_secs(10);

/// What to watch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WatchSpec {
    /// The logical database.
    pub database: String,
    /// Tables; empty for all the caller may read.
    pub objects: Vec<String>,
    /// A resume token, or empty for now.
    pub start_after: String,
    /// Not supported yet (39802 when set).
    pub start_at_hlc: u64,
    /// Whole rows after updates.
    pub full_document: bool,
}

/// One event (or heartbeat) as chapter 37's `ChangeEvent`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WatchEvent {
    /// Resume after it.
    pub resume_token: String,
    /// The commit clamp.
    pub commit_hlc: u64,
    /// The table.
    pub object: String,
    /// `insert`, `update`, `delete`, `ddl`, `truncate`, `heartbeat`.
    pub op: String,
    /// JSON.
    pub key: String,
    /// JSON.
    pub after: String,
    /// JSON.
    pub update: String,
    /// The DDL statement.
    pub ddl: String,
}

impl WatchEvent {
    /// As the proto message.
    #[must_use]
    pub fn proto(&self) -> sprig_api::proto::v1::ChangeEvent {
        sprig_api::proto::v1::ChangeEvent {
            resume_token: self.resume_token.clone(),
            commit_hlc: self.commit_hlc,
            object: self.object.clone(),
            model: if self.object.is_empty() {
                String::new()
            } else {
                "table".to_owned()
            },
            op: self.op.clone(),
            key: self.key.clone(),
            after: self.after.clone(),
            update: self.update.clone(),
            ddl: self.ddl.clone(),
        }
    }

    /// As proto3 JSON (empty fields left out).
    #[must_use]
    pub fn json(&self) -> Value {
        let mut m = serde_json::Map::new();
        let mut put = |k: &str, v: &str| {
            if !v.is_empty() {
                m.insert(k.to_owned(), Value::String(v.to_owned()));
            }
        };
        put("resume_token", &self.resume_token);
        put("object", &self.object);
        if !self.object.is_empty() {
            put("model", "table");
        }
        put("op", &self.op);
        put("key", &self.key);
        put("after", &self.after);
        put("update", &self.update);
        put("ddl", &self.ddl);
        if self.commit_hlc != 0 {
            m.insert(
                "commit_hlc".into(),
                Value::String(self.commit_hlc.to_string()),
            );
        }
        Value::Object(m)
    }
}

type FeedParts = (
    Arc<dyn Clusters>,
    Arc<dyn PlacementView>,
    Arc<dyn sprig_feed::names::Resolver>,
);

/// The feed's parts, one set per process.
fn feed(svc: &crate::tenant::TenantServices) -> Result<FeedParts> {
    static F: OnceLock<FeedParts> = OnceLock::new();
    if let Some(f) = F.get() {
        return Ok(f.clone());
    }
    let locator: Arc<dyn sprig_catalog::CatalogLocator> = svc.locator.clone();
    let parts = crate::mongo::changes::feed_parts(
        Arc::clone(&svc.global),
        svc.locator.dc_store(),
        locator,
    )?;
    Ok(F.get_or_init(|| parts).clone())
}

/// Checks `req` in a fresh session of `caller` (`exec::rls_images`).
async fn check(core: &Core, caller: &Caller, req: &ImagesRequest) -> Result<Vec<Vec<bool>>> {
    let (tx, rx) = mpsc::channel(4);
    let engine = Arc::clone(&core.engine);
    let call = SqlCall {
        database: "-".to_owned(),
        sql: statement(req),
        multi: false,
    };
    let c = caller.clone();
    let task = tokio::spawn(async move { engine.run(&c, call, None, tx).await });
    let resp = collect(rx, 64 << 20, None).await;
    let _ = task.await;
    let resp = resp?;
    let rows: Vec<Vec<Option<String>>> = resp
        .results
        .first()
        .map(|r| {
            r.rows
                .iter()
                .map(|row| {
                    row.values
                        .iter()
                        .map(|v| {
                            sprig_api::results::cell_bytes(v, false)
                                .map(|b| String::from_utf8_lossy(&b).into_owned())
                        })
                        .collect()
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(answer(req, &rows))
}

fn denied(caller: &Caller, db: &str, table: &str) -> SprigError {
    SprigError::standard(
        1142,
        *b"42000",
        format!(
            "SELECT command denied to user '{}'@'{}' for table `{db}`.`{table}`",
            caller.principal.user, caller.principal.host
        ),
    )
}

/// Opens a watch: its events, then (on failure) one error, then the end.
///
/// # Errors
/// 1142 for a requested table the caller can't read; 39801 for a bad or
/// foreign token; 39802 for `start_at_hlc`; the feed's.
pub async fn open(
    core: &Arc<Core>,
    caller: Caller,
    spec: WatchSpec,
) -> Result<mpsc::Receiver<Result<WatchEvent>>> {
    let svc = core
        .engine
        .tenants
        .clone()
        .ok_or_else(|| SprigError::internal("this gateway serves no tenant catalogs"))?;
    let tenant = caller.principal.tenant.ok_or_else(|| {
        SprigError::new(
            SprigCode::FeedSourceUnsupported,
            "change streams are for tenant users",
        )
    })?;
    if spec.start_at_hlc != 0 {
        return Err(SprigError::new(
            SprigCode::FeedSourceUnsupported,
            "ER_SPRIG_FEED_SOURCE_UNSUPPORTED: start_at_hlc isn't supported yet; use start_after",
        ));
    }
    if spec.database.is_empty() {
        return Err(SprigError::standard(
            1046,
            *b"3D000",
            "No database selected".to_owned(),
        ));
    }
    // 1. SELECT on every requested table.
    if !spec.objects.is_empty() {
        let req = ImagesRequest {
            tables: spec
                .objects
                .iter()
                .map(|o| TableImages {
                    db: spec.database.clone(),
                    table: o.clone(),
                    images: vec![None],
                })
                .collect(),
            plain_binary: false,
        };
        let ok = check(core, &caller, &req).await?;
        if let Some(i) = ok.iter().position(|v| !v.first().copied().unwrap_or(false)) {
            return Err(denied(&caller, &spec.database, &spec.objects[i]));
        }
    }
    let start = if spec.start_after.is_empty() {
        StartAt::Now
    } else {
        let t = ResumeToken::from_hex(&spec.start_after)?;
        if t.tenant != tenant {
            return Err(SprigError::new(
                SprigCode::FeedBadResumeToken,
                "ER_SPRIG_FEED_BAD_RESUME_TOKEN: the resume token is another tenant's",
            ));
        }
        StartAt::After(t)
    };
    let (clusters, placement, names) = feed(&svc)?;
    let tail = TenantTail::start(
        tenant,
        start,
        clusters,
        Arc::clone(&placement),
        names,
        TailOptions {
            progress_every: Duration::from_millis(500),
            ..TailOptions::default()
        },
    )
    .await?;
    let first = if spec.start_after.is_empty() {
        let segment = placement
            .history(tenant)
            .await?
            .last()
            .map_or(0, |h| h.map_version);
        ResumeToken {
            tenant,
            clamp: 0,
            cluster: tail.cluster().clone(),
            segment,
            pos: tail.position().clone(),
            until: None,
            progress: true,
        }
        .to_hex()
    } else {
        spec.start_after.clone()
    };
    let (tx, rx) = mpsc::channel(DEPTH);
    let core = Arc::clone(core);
    tokio::spawn(async move {
        metrics::gauge!("sprig_api_change_streams").increment(1.0);
        let r = run(&core, &caller, tenant, &spec, tail, first, &tx).await;
        if let Err(e) = r {
            let _ = tx.send(Err(e)).await;
        }
        metrics::gauge!("sprig_api_change_streams").decrement(1.0);
    });
    Ok(rx)
}

fn op_name(op: Op) -> &'static str {
    match op {
        Op::Insert => "insert",
        Op::Update => "update",
        Op::Delete => "delete",
        Op::Ddl(_) => "ddl",
        Op::Truncate => "truncate",
    }
}

fn image(img: Option<&sprig_feed::Image>) -> Option<serde_json::Map<String, Value>> {
    match img.map(sprig_streams::envelope::image_json) {
        Some(Value::Object(m)) => Some(m),
        _ => None,
    }
}

/// Where an event's images are in the check.
struct Pending {
    event: Box<sprig_feed::ChangeEvent>,
    table: usize,
    before: Option<usize>,
    after: Option<usize>,
    /// DDL and truncate: the privilege alone.
    probe: Option<usize>,
}

/// The event `p` becomes as the caller may see it (`None`: hidden).
fn shown(p: &Pending, seen: &[Vec<bool>], full: bool) -> Option<WatchEvent> {
    let v = |i: Option<usize>| i.is_some_and(|i| seen[p.table].get(i).copied().unwrap_or(false));
    let e = &p.event;
    let key = sprig_streams::envelope::image_json(&e.key).to_string();
    let mut out = WatchEvent {
        resume_token: e.token.to_hex(),
        commit_hlc: e.hlc,
        object: e.table.clone(),
        key,
        ..WatchEvent::default()
    };
    let after = || {
        e.after
            .as_ref()
            .map(|a| sprig_streams::envelope::image_json(a).to_string())
            .unwrap_or_default()
    };
    match &e.op {
        Op::Insert => {
            if !v(p.after) {
                return None;
            }
            out.op = "insert".into();
            out.after = after();
        }
        Op::Delete => {
            if !v(p.before) {
                return None;
            }
            out.op = "delete".into();
        }
        Op::Update => match (v(p.before), v(p.after)) {
            (true, true) => {
                out.op = "update".into();
                let a = image(e.after.as_ref()).unwrap_or_default();
                let changed: serde_json::Map<String, Value> = match &e.delta {
                    Some(cols) => a
                        .iter()
                        .filter(|(k, _)| cols.contains(k))
                        .map(|(k, x)| (k.clone(), x.clone()))
                        .collect(),
                    None => a.clone(),
                };
                out.update = Value::Object(changed).to_string();
                if full {
                    out.after = after();
                }
            }
            (true, false) => out.op = "delete".into(),
            (false, true) => {
                out.op = "insert".into();
                out.after = after();
            }
            (false, false) => return None,
        },
        op @ (Op::Ddl(_) | Op::Truncate) => {
            if !v(p.probe) {
                return None;
            }
            out.op = op_name(*op).into();
            out.ddl = e.ddl.clone().unwrap_or_default();
        }
    }
    Some(out)
}

/// Whether the event is one the stream watches.
fn wanted(spec: &WatchSpec, e: &sprig_feed::ChangeEvent) -> bool {
    e.db_name.eq_ignore_ascii_case(&spec.database)
        && !e.table.starts_with("__sprig")
        && !e.table.starts_with("_sprig")
        && (spec.objects.is_empty()
            || spec
                .objects
                .iter()
                .any(|o| o.eq_ignore_ascii_case(&e.table)))
}

#[allow(clippy::too_many_lines)] // the loop of the module docs
async fn run(
    core: &Core,
    caller: &Caller,
    tenant: TenantId,
    spec: &WatchSpec,
    mut tail: TenantTail,
    first: String,
    tx: &mpsc::Sender<Result<WatchEvent>>,
) -> Result<()> {
    let _ = tenant;
    let mut last = first;
    let mut quiet = tokio::time::Instant::now();
    loop {
        // A batch: the first item (or a heartbeat's timeout), then what
        // follows within 20 ms, up to 256 events.
        let mut items = Vec::new();
        let until = quiet + HEARTBEAT;
        tokio::select! {
            () = tx.closed() => return Ok(()),
            r = tokio::time::timeout_at(until, tail.next()) => if let Ok(i) = r {
                items.push(i?);
            } else {
                let hb = WatchEvent { resume_token: last.clone(), op: "heartbeat".into(), ..WatchEvent::default() };
                if tx.send(Ok(hb)).await.is_err() {
                    return Ok(());
                }
                quiet = tokio::time::Instant::now();
                continue;
            },
        }
        let more_until = tokio::time::Instant::now() + Duration::from_millis(20);
        while items.len() < DEPTH {
            match tokio::time::timeout_at(more_until, tail.next()).await {
                Ok(i) => items.push(i?),
                Err(_) => break,
            }
        }
        let mut req = ImagesRequest::default();
        let mut tables: HashMap<String, usize> = HashMap::new();
        let mut pending: Vec<Pending> = Vec::new();
        for it in items {
            match it {
                FeedItem::Event(e) => {
                    last = e.token.to_hex();
                    if !wanted(spec, &e) {
                        continue;
                    }
                    let k = e.table.to_lowercase();
                    let t = *tables.entry(k).or_insert_with(|| {
                        req.tables.push(TableImages {
                            db: e.db_name.clone(),
                            table: e.table.clone(),
                            images: Vec::new(),
                        });
                        req.tables.len() - 1
                    });
                    let mut push = |img: Option<serde_json::Map<String, Value>>| {
                        let imgs = &mut req.tables[t].images;
                        imgs.push(img);
                        imgs.len() - 1
                    };
                    let (mut before, mut after, mut probe) = (None, None, None);
                    match e.op {
                        Op::Insert => after = image(e.after.as_ref()).map(|m| push(Some(m))),
                        Op::Delete => before = image(e.before.as_ref()).map(|m| push(Some(m))),
                        Op::Update => {
                            before = image(e.before.as_ref()).map(|m| push(Some(m)));
                            after = image(e.after.as_ref()).map(|m| push(Some(m)));
                        }
                        Op::Ddl(_) | Op::Truncate => probe = Some(push(None)),
                    }
                    pending.push(Pending {
                        event: e,
                        table: t,
                        before,
                        after,
                        probe,
                    });
                }
                FeedItem::TxnEnd { token } | FeedItem::Progress { token } => {
                    last = token.to_hex();
                }
            }
        }
        if pending.is_empty() {
            continue;
        }
        let seen = check(core, caller, &req).await?;
        let mut sent = 0u64;
        for p in &pending {
            if let Some(ev) = shown(p, &seen, spec.full_document) {
                if tx.send(Ok(ev)).await.is_err() {
                    return Ok(());
                }
                sent += 1;
                quiet = tokio::time::Instant::now();
            }
        }
        metrics::counter!("sprig_api_change_events_sent_total").increment(sent);
    }
}

/// The REST request's spec: path variable `database`, query `objects`
/// (comma-separated, repeatable), `start_after`, `start_at_hlc`,
/// `full_document`; `Last-Event-ID` (SSE's resume) when no `start_after`.
#[must_use]
pub fn spec_of(database: &str, query: Option<&str>, last_event_id: Option<&str>) -> WatchSpec {
    let mut s = WatchSpec {
        database: database.to_owned(),
        ..WatchSpec::default()
    };
    for pair in query.unwrap_or("").split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let v = percent_decode(v);
        match k {
            "objects" => s
                .objects
                .extend(v.split(',').filter(|x| !x.is_empty()).map(str::to_owned)),
            "start_after" => s.start_after = v,
            "start_at_hlc" => s.start_at_hlc = v.parse().unwrap_or(0),
            "full_document" => s.full_document = v == "true" || v == "1",
            _ => {}
        }
    }
    if s.start_after.is_empty()
        && let Some(id) = last_event_id
    {
        id.trim().clone_into(&mut s.start_after);
    }
    s
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => match u8::from_str_radix(&s[i + 1..i + 3], 16) {
                Ok(x) => {
                    out.push(x);
                    i += 2;
                }
                Err(_) => out.push(b'%'),
            },
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hdr(req: &hyper::Request<hyper::body::Incoming>, name: &str) -> Option<String> {
    req.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

/// `GET /v1/databases/{database}/changes` (see the module docs).
///
/// # Errors
/// [`open`]'s; 39791 for a WebSocket upgrade over HTTP/2.
pub async fn rest(
    core: &Arc<Core>,
    info: &super::ConnInfo,
    database: &str,
    mut req: hyper::Request<hyper::body::Incoming>,
) -> Result<hyper::Response<tonic::body::Body>> {
    use futures::StreamExt as _;
    use http_body_util::StreamBody;
    use hyper::body::Frame;
    use hyper::header::{self, HeaderValue};
    let caller = core
        .caller(
            info,
            hdr(&req, "authorization"),
            hdr(&req, super::rest::TENANT_HEADER),
            hdr(&req, "host"),
        )
        .await?;
    let spec = spec_of(
        database,
        req.uri().query(),
        hdr(&req, "last-event-id").as_deref(),
    );
    let ws = hdr(&req, "upgrade").is_some_and(|u| u.eq_ignore_ascii_case("websocket"));
    let sse = hdr(&req, "accept").is_some_and(|a| a.contains("text/event-stream"));
    if ws && req.version() != hyper::Version::HTTP_11 {
        return Err(SprigError::new(
            SprigCode::TransportUnsupported,
            "ER_SPRIG_TRANSPORT_UNSUPPORTED: WebSocket change streams need HTTP/1.1",
        ));
    }
    let key = if ws {
        Some(hdr(&req, "sec-websocket-key").ok_or_else(|| {
            SprigError::new(
                SprigCode::AdminRequestInvalid,
                "a WebSocket upgrade without Sec-WebSocket-Key",
            )
        })?)
    } else {
        None
    };
    let rx = open(core, caller, spec).await?;
    if let Some(key) = key {
        let on = hyper::upgrade::on(&mut req);
        tokio::spawn(async move {
            match on.await {
                Ok(up) => ws_serve(hyper_util::rt::TokioIo::new(up), rx).await,
                Err(e) => tracing::debug!(error = %e, "WebSocket upgrade failed"),
            }
        });
        let mut r = hyper::Response::new(tonic::body::Body::empty());
        *r.status_mut() = hyper::StatusCode::SWITCHING_PROTOCOLS;
        let h = r.headers_mut();
        h.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        h.insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
        if let Ok(v) = HeaderValue::from_str(&ws_accept(&key)) {
            h.insert("sec-websocket-accept", v);
        }
        return Ok(r);
    }
    let stream = futures::stream::unfold(Some(rx), move |rx| async move {
        let mut rx = rx?;
        let item = rx.recv().await?;
        let (bytes, next) = match item {
            Ok(ev) if sse => (sse_frame(&ev), Some(rx)),
            Ok(ev) => {
                let mut b = ev.json().to_string().into_bytes();
                b.push(b'\n');
                (Bytes::from(b), Some(rx))
            }
            Err(e) => {
                let (_, body) = sprig_api::status::to_rest(&e);
                let j = json!({ "error": body }).to_string();
                let b = if sse {
                    Bytes::from(format!("event: error\ndata: {j}\n\n"))
                } else {
                    Bytes::from(format!("{j}\n"))
                };
                (b, None)
            }
        };
        Some((Ok::<_, std::convert::Infallible>(Frame::data(bytes)), next))
    })
    .boxed();
    let mut r = hyper::Response::new(tonic::body::Body::new(StreamBody::new(stream)));
    let h = r.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(if sse {
            "text/event-stream"
        } else {
            "application/x-ndjson"
        }),
    );
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    Ok(r)
}

/// An SSE frame of `ev`.
#[must_use]
pub fn sse_frame(ev: &WatchEvent) -> Bytes {
    Bytes::from(format!(
        "id: {}\nevent: {}\ndata: {}\n\n",
        ev.resume_token,
        ev.op,
        ev.json()
    ))
}

// ---------------------------------------------------------------------
// WebSocket (RFC 6455), server side, text frames out; the client's frames
// are read for close and ping only.

/// `Sec-WebSocket-Accept` for `key`.
#[must_use]
pub fn ws_accept(key: &str) -> String {
    use base64::Engine as _;
    use sha1::Digest as _;
    let mut h = sha1::Sha1::new();
    h.update(key.trim().as_bytes());
    h.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    base64::engine::general_purpose::STANDARD.encode(h.finalize())
}

/// A server frame (unmasked) of `opcode` with `payload`.
#[must_use]
pub fn ws_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
    let mut f = vec![0x80 | opcode];
    let n = payload.len();
    if n < 126 {
        f.push(u8::try_from(n).unwrap_or(0));
    } else if let Ok(n16) = u16::try_from(n) {
        f.push(126);
        f.extend_from_slice(&n16.to_be_bytes());
    } else {
        f.push(127);
        f.extend_from_slice(&(n as u64).to_be_bytes());
    }
    f.extend_from_slice(payload);
    f
}

/// Serves an upgraded WebSocket: events as text frames until the client
/// closes or the stream ends (an error is a text frame `{"error": ...}`,
/// then a close frame).
pub async fn ws_serve<IO>(io: IO, mut rx: mpsc::Receiver<Result<WatchEvent>>)
where
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (mut r, mut w) = tokio::io::split(io);
    let (ctl_tx, mut ctl_rx) = mpsc::channel::<Vec<u8>>(8);
    // Reader: pings get pongs; a close (or the end) stops the stream.
    let reader = tokio::spawn(async move {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            match r.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
            while let Some((op, payload, used)) = ws_parse(&buf) {
                buf.drain(..used);
                match op {
                    0x8 => {
                        let _ = ctl_tx.send(ws_frame(0x8, &payload)).await;
                        return;
                    }
                    0x9 => {
                        let _ = ctl_tx.send(ws_frame(0xA, &payload)).await;
                    }
                    _ => {}
                }
            }
        }
    });
    loop {
        tokio::select! {
            c = ctl_rx.recv() => match c {
                Some(f) => {
                    let close = f.first() == Some(&0x88);
                    if w.write_all(&f).await.is_err() || close {
                        break;
                    }
                }
                None => break,
            },
            ev = rx.recv() => match ev {
                Some(Ok(e)) => {
                    let f = ws_frame(0x1, e.json().to_string().as_bytes());
                    if w.write_all(&f).await.is_err() {
                        break;
                    }
                }
                Some(Err(e)) => {
                    let (_, body) = sprig_api::status::to_rest(&e);
                    let t = json!({ "error": body }).to_string();
                    let _ = w.write_all(&ws_frame(0x1, t.as_bytes())).await;
                    let _ = w.write_all(&ws_frame(0x8, &1011u16.to_be_bytes())).await;
                    break;
                }
                None => {
                    let _ = w.write_all(&ws_frame(0x8, &1000u16.to_be_bytes())).await;
                    break;
                }
            },
        }
    }
    let _ = w.flush().await;
    reader.abort();
}

/// One client frame of `buf`: (opcode, unmasked payload, bytes used).
fn ws_parse(buf: &[u8]) -> Option<(u8, Vec<u8>, usize)> {
    if buf.len() < 2 {
        return None;
    }
    let op = buf[0] & 0x0F;
    let masked = buf[1] & 0x80 != 0;
    let mut n = usize::from(buf[1] & 0x7F);
    let mut at = 2;
    if n == 126 {
        n = usize::from(u16::from_be_bytes([*buf.get(2)?, *buf.get(3)?]));
        at = 4;
    } else if n == 127 {
        let mut b = [0u8; 8];
        b.copy_from_slice(buf.get(2..10)?);
        n = usize::try_from(u64::from_be_bytes(b)).ok()?;
        at = 10;
    }
    let mask = if masked {
        let m = [
            *buf.get(at)?,
            *buf.get(at + 1)?,
            *buf.get(at + 2)?,
            *buf.get(at + 3)?,
        ];
        at += 4;
        Some(m)
    } else {
        None
    };
    let body = buf.get(at..at + n)?;
    let payload = match mask {
        Some(m) => body.iter().enumerate().map(|(i, b)| b ^ m[i % 4]).collect(),
        None => body.to_vec(),
    };
    Some((op, payload, at + n))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rest_specs() {
        let s = spec_of(
            "shop",
            Some("objects=docs,notes&objects=x&full_document=true&start_after=ab%20"),
            Some("cd"),
        );
        assert_eq!(s.objects, vec!["docs", "notes", "x"]);
        assert!(s.full_document);
        assert_eq!(s.start_after, "ab ");
        let s = spec_of("shop", None, Some("cd"));
        assert_eq!(s.start_after, "cd");
    }

    #[test]
    fn websocket_frames() {
        // RFC 6455's example.
        assert_eq!(
            ws_accept("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
        let f = ws_frame(0x1, b"hi");
        assert_eq!(f, vec![0x81, 2, b'h', b'i']);
        assert_eq!(ws_frame(0x1, &[0u8; 300])[1], 126);
        // A masked client close frame.
        let m = [1u8, 2, 3, 4];
        let payload = [0x03u8, 0xE8];
        let mut c = vec![0x88, 0x80 | 2];
        c.extend_from_slice(&m);
        c.extend(payload.iter().enumerate().map(|(i, b)| b ^ m[i % 4]));
        assert_eq!(ws_parse(&c), Some((0x8, payload.to_vec(), 8)));
        assert_eq!(ws_parse(&c[..5]), None);
    }

    #[test]
    fn sse() {
        let e = WatchEvent {
            resume_token: "t1".into(),
            op: "insert".into(),
            object: "docs".into(),
            ..WatchEvent::default()
        };
        let f = String::from_utf8(sse_frame(&e).to_vec()).unwrap_or_default();
        assert!(f.starts_with("id: t1\nevent: insert\ndata: {"), "{f}");
        assert!(f.ends_with("}\n\n"));
    }
}
