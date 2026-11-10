//! `Cursors` (chapter 37, "APIs for Studio"; chapter 44 open question 7,
//! decided as ST23 and built as ST30): fetching an arbitrary query's rows
//! page by page past the row cap.
//!
//! A cursor rides the router's own stream: `Open` starts the statement
//! through the same [`SqlEngine`](super::super::sql::SqlEngine) path as
//! `Sql.Query`, into a channel of [`CHANNEL_DEPTH`] messages. Nothing reads
//! the channel between `Fetch` calls, so the executor stops reading from
//! the backend, as a slow HTTP client makes it stop: backpressure, not a
//! buffer. The cursor never keeps a backend session of its own (unlike a
//! transaction handle): closing it, or `studio.cursor_idle_seconds`
//! without a `Fetch`, drops the channel, and the executor kills the
//! backend statement and closes that connection
//! ([`Abandon::ClientGone`](crate::pipeline::Abandon)).
//!
//! ```text
//! POST /v1/databases/{db}/cursors {sql, skip?, page_rows?}
//!   1. the caller's open cursors here at studio.cursor_max_open: close the
//!      least recently used
//!   2. run sql (first result only); wait for its header (an error, or no
//!      result set: answered at once, no cursor kept)
//!   3. read and drop `skip` rows; then the first page
//!   4. → {name, columns, rows, position, done, idle_seconds}; done: closed
//! POST /v1/cursors/{h}:fetch {rows?} → {rows, position, done}
//!   another gateway's cursor: forwarded over the peer service, as
//!   transaction handles are; another principal's: 39934, as if unknown
//! POST /v1/cursors/{h}:close
//! reaper, every second: cursors idle past their deadline and not busy
//!   → dropped (39935 for 10 minutes after)
//! ```
//!
//! A page is at most `studio.max_result_rows` rows for a Studio session
//! (10,000 for other callers), and a `Fetch` waits at most
//! `studio.statement_timeout_ms` for rows: it then returns what it has
//! (perhaps nothing) with `done: false`.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};
use std::time::{Duration, Instant};

use hyper::body::Incoming;
use hyper::{Request, Response};
use serde_json::{Value, json};
use sprig_api::handle::TxnHandle;
use sprig_api::proto::internal::ForwardRequest;
use sprig_api::proto::v1::query_response::Part;
use sprig_api::results;
use sprig_common::ids::TenantId;
use sprig_common::{SprigCode, SprigError};
use tokio::sync::mpsc;
use tonic::body::Body;

use super::super::sink::Item;
use super::super::sql::{CHANNEL_DEPTH, Caller, Caps, SqlCall};
use super::super::{ConnInfo, Core};
use super::{hdr, invalid, json_response, parse_json, read_body, session_client};

/// The prefix of a cursor's name.
pub const PREFIX: &str = "cursors/";
/// Rows per page for callers that aren't Studio sessions, at most.
pub const API_PAGE_MAX: u64 = 10_000;
/// Rows per page when the request names none.
pub const DEFAULT_PAGE: u64 = 1000;
/// How long an expired cursor is remembered, so its use gets 39935.
const EXPIRED_MEMORY: Duration = Duration::from_secs(600);
/// Expired cursors remembered at most.
const EXPIRED_MAX: usize = 10_000;

/// The stream a cursor reads.
struct Stream {
    rx: mpsc::Receiver<Item>,
    /// Rows received and not yet delivered.
    pending: VecDeque<Value>,
    /// Rows delivered or skipped so far.
    position: u64,
    /// The first result ended (or the statement failed after its header).
    done: bool,
    /// Rows kept out (`skip`) still to drop.
    to_skip: u64,
}

impl Stream {
    /// Waits for rows until `want` are pending, the first result ends or
    /// `deadline` passes.
    ///
    /// # Errors
    /// The statement's error after the header.
    async fn fill(&mut self, want: usize, deadline: Instant) -> Result<(), SprigError> {
        while !self.done && self.pending.len() < want {
            let item = match tokio::time::timeout_at(deadline.into(), self.rx.recv()).await {
                Err(_) => return Ok(()),
                Ok(None) => {
                    self.done = true;
                    return Ok(());
                }
                Ok(Some(i)) => i,
            };
            match item {
                Item::Part(p) => match p.part {
                    Some(Part::Rows(b)) => {
                        for r in &b.rows {
                            if self.to_skip > 0 {
                                self.to_skip -= 1;
                                self.position += 1;
                            } else {
                                self.pending.push_back(row_json(r));
                            }
                        }
                    }
                    // The first result is all a cursor reads.
                    Some(Part::End(_)) => self.done = true,
                    Some(Part::Header(_)) | None => {}
                },
                Item::Error(e) => {
                    self.done = true;
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Up to `n` pending rows.
    fn take(&mut self, n: usize) -> Vec<Value> {
        let k = n.min(self.pending.len());
        let out: Vec<Value> = self.pending.drain(..k).collect();
        self.position += out.len() as u64;
        out
    }

    /// Everything delivered: the stream ended and nothing is pending.
    fn exhausted(&self) -> bool {
        self.done && self.pending.is_empty()
    }
}

fn row_json(r: &prost_types::ListValue) -> Value {
    let v = results::query_part_json(&sprig_api::proto::v1::QueryResponse {
        part: Some(Part::Rows(sprig_api::proto::v1::RowBatch {
            rows: vec![r.clone()],
        })),
    });
    v["rows"]["rows"][0].clone()
}

/// One open cursor.
struct Cursor {
    handle: TxnHandle,
    tenant: Option<TenantId>,
    user: String,
    idle: Duration,
    idle_deadline: Mutex<Instant>,
    last_used: Mutex<Instant>,
    /// `None` once closed.
    stream: tokio::sync::Mutex<Option<Stream>>,
}

impl Cursor {
    fn touch(&self) {
        let now = Instant::now();
        *self
            .idle_deadline
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = now + self.idle;
        *self.last_used.lock().unwrap_or_else(PoisonError::into_inner) = now;
    }

    fn overdue(&self, now: Instant) -> bool {
        now >= *self
            .idle_deadline
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn last_used(&self) -> Instant {
        *self.last_used.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn owned_by(&self, tenant: Option<TenantId>, user: &str) -> bool {
        self.tenant == tenant && self.user == user
    }
}

/// The cursors open on this gateway.
#[derive(Default)]
pub struct CursorRegistry {
    open: Mutex<HashMap<u64, Arc<Cursor>>>,
    expired: Mutex<VecDeque<(u64, Instant)>>,
    next: AtomicU64,
    reaper: OnceLock<()>,
}

impl std::fmt::Debug for CursorRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CursorRegistry")
            .field("open", &self.len())
            .finish_non_exhaustive()
    }
}

/// What a page answer holds.
#[derive(Debug, Clone, PartialEq)]
pub struct Page {
    /// The rows, as `Sql.Query`'s row arrays.
    pub rows: Vec<Value>,
    /// Rows delivered or skipped so far, this page included.
    pub position: u64,
    /// The result has no more rows; the cursor is closed.
    pub done: bool,
}

/// What `open` returns.
#[derive(Debug, Clone, PartialEq)]
pub struct Opened {
    /// `cursors/<h>`; empty when `done`.
    pub name: String,
    /// The columns, as `Sql.Query`'s header lists them; empty without a
    /// result set.
    pub columns: Vec<Value>,
    /// The first page.
    pub page: Page,
    /// The idle timeout.
    pub idle_seconds: u64,
    /// The end of a statement without a result set.
    pub end: Option<Value>,
}

fn unknown() -> SprigError {
    SprigError::new(
        SprigCode::StudioCursorUnknown,
        "no open cursor has this name",
    )
}

fn expired() -> SprigError {
    SprigError::new(
        SprigCode::StudioCursorExpired,
        "the cursor was idle past studio.cursor_idle_seconds and closed; open it again",
    )
}

/// `cursors/<h>` for `h`.
fn cursor_name(h: &TxnHandle) -> String {
    let n = h.name();
    format!(
        "{PREFIX}{}",
        n.strip_prefix(sprig_api::handle::PREFIX).unwrap_or(&n)
    )
}

/// Parses `cursors/<h>` (or a bare `<h>`).
///
/// # Errors
/// 39934 for anything else.
pub fn parse_name(name: &str) -> Result<TxnHandle, SprigError> {
    let h = name.strip_prefix(PREFIX).unwrap_or(name);
    if h.is_empty() || h.contains('/') {
        return Err(unknown());
    }
    TxnHandle::parse(&format!("{}{h}", sprig_api::handle::PREFIX)).map_err(|_| unknown())
}

impl CursorRegistry {
    /// Open cursors.
    #[must_use]
    pub fn len(&self) -> usize {
        self.open.lock().unwrap_or_else(PoisonError::into_inner).len()
    }

    /// No open cursors.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn gauge(&self) {
        #[allow(clippy::cast_precision_loss)] // a gauge
        metrics::gauge!("sprig_studio_cursors").set(self.len() as f64);
    }

    fn remember_expired(&self, id: u64) {
        let now = Instant::now();
        let mut q = self.expired.lock().unwrap_or_else(PoisonError::into_inner);
        while q
            .front()
            .is_some_and(|(_, at)| now.duration_since(*at) > EXPIRED_MEMORY)
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

    fn get(&self, h: &TxnHandle) -> Option<Arc<Cursor>> {
        self.open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&h.session)
            .filter(|c| c.handle == *h)
            .cloned()
    }

    fn remove(&self, id: u64) -> Option<Arc<Cursor>> {
        let c = self
            .open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&id);
        self.gauge();
        c
    }

    /// Closes `c`: its stream is dropped, so the statement behind it is
    /// killed and its backend connection closed.
    async fn close_cursor(&self, c: &Cursor) {
        self.remove(c.handle.session);
        let s = c.stream.lock().await.take();
        drop(s);
    }

    /// The user's cursors past `max - 1`, least recently used first,
    /// closed to make room for one more.
    async fn make_room(&self, tenant: Option<TenantId>, user: &str, max: usize) {
        let mut mine: Vec<Arc<Cursor>> = self
            .open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .filter(|c| c.owned_by(tenant, user))
            .cloned()
            .collect();
        if mine.len() < max {
            return;
        }
        mine.sort_by_key(|c| c.last_used());
        let excess = mine.len() + 1 - max.max(1);
        for c in mine.into_iter().take(excess) {
            self.remember_expired(c.handle.session);
            self.close_cursor(&c).await;
            metrics::counter!("sprig_studio_cursors_closed_total", "reason" => "evicted")
                .increment(1);
        }
    }

    /// Registers a stream that already has its header, reads `skip` rows
    /// and the first page, and keeps the cursor unless that ended it.
    ///
    /// # Errors
    /// The statement's error.
    #[allow(clippy::too_many_arguments)] // one call site, all parts of one request
    async fn register(
        self: &Arc<Self>,
        owner: (&str, &str, Option<TenantId>, &str),
        rx: mpsc::Receiver<Item>,
        skip: u64,
        page: usize,
        idle: Duration,
        max_open: usize,
        wait: Duration,
    ) -> Result<(String, Page), SprigError> {
        let (dc, gateway, tenant, user) = owner;
        let mut s = Stream {
            rx,
            pending: VecDeque::new(),
            position: 0,
            done: false,
            to_skip: skip,
        };
        // Skipping shares the first page's wait.
        s.fill(page, Instant::now() + wait).await?;
        let rows = s.take(page);
        if s.exhausted() {
            return Ok((
                String::new(),
                Page {
                    rows,
                    position: s.position,
                    done: true,
                },
            ));
        }
        self.make_room(tenant, user, max_open).await;
        let id = self.next.fetch_add(1, Ordering::SeqCst) + 1;
        let handle = TxnHandle::new(dc, gateway, id)?;
        let now = Instant::now();
        let position = s.position;
        let c = Arc::new(Cursor {
            handle: handle.clone(),
            tenant,
            user: user.to_owned(),
            idle,
            idle_deadline: Mutex::new(now + idle),
            last_used: Mutex::new(now),
            stream: tokio::sync::Mutex::new(Some(s)),
        });
        self.open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id, c);
        self.gauge();
        self.start_reaper();
        Ok((
            cursor_name(&handle),
            Page {
                rows,
                position,
                done: false,
            },
        ))
    }

    /// The next page of `h` for `(tenant, user)`.
    ///
    /// # Errors
    /// 39934 (unknown, or another principal's), 39935 (expired); the
    /// statement's error, which closes the cursor.
    pub async fn fetch(
        &self,
        h: &TxnHandle,
        tenant: Option<TenantId>,
        user: &str,
        page: usize,
        wait: Duration,
    ) -> Result<Page, SprigError> {
        let c = self.owned(h, tenant, user)?;
        let mut guard = c.stream.lock().await;
        let Some(s) = guard.as_mut() else {
            return Err(expired());
        };
        let r = s.fill(page, Instant::now() + wait).await;
        let rows = s.take(page);
        let (position, exhausted) = (s.position, s.exhausted());
        c.touch();
        if r.is_err() || exhausted {
            guard.take();
            drop(guard);
            self.remove(h.session);
        }
        r?;
        Ok(Page {
            rows,
            position,
            done: exhausted,
        })
    }

    /// Closes `h`; `false` if it was already gone.
    ///
    /// # Errors
    /// 39934 for another principal's cursor.
    pub async fn close(
        &self,
        h: &TxnHandle,
        tenant: Option<TenantId>,
        user: &str,
    ) -> Result<bool, SprigError> {
        match self.owned(h, tenant, user) {
            Ok(c) => {
                self.close_cursor(&c).await;
                metrics::counter!("sprig_studio_cursors_closed_total", "reason" => "closed")
                    .increment(1);
                Ok(true)
            }
            Err(e) if e.code() == Some(SprigCode::StudioCursorExpired) => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn owned(
        &self,
        h: &TxnHandle,
        tenant: Option<TenantId>,
        user: &str,
    ) -> Result<Arc<Cursor>, SprigError> {
        match self.get(h) {
            Some(c) if c.owned_by(tenant, user) => Ok(c),
            Some(_) => Err(unknown()),
            None if self.was_expired(h.session) => Err(expired()),
            None => Err(unknown()),
        }
    }

    /// Closes cursors idle past their deadline that no call is using;
    /// the number closed.
    pub async fn reap_once(&self, now: Instant) -> usize {
        let overdue: Vec<Arc<Cursor>> = self
            .open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .filter(|c| c.overdue(now))
            .cloned()
            .collect();
        let mut n = 0;
        for c in overdue {
            // A busy cursor (a Fetch waiting for rows) isn't idle.
            if let Ok(mut g) = c.stream.try_lock() {
                let s = g.take();
                drop(g);
                drop(s);
                self.remember_expired(c.handle.session);
                self.remove(c.handle.session);
                metrics::counter!("sprig_studio_cursors_closed_total", "reason" => "idle")
                    .increment(1);
                n += 1;
            }
        }
        n
    }

    fn start_reaper(self: &Arc<Self>) {
        self.reaper.get_or_init(|| {
            if let Ok(h) = tokio::runtime::Handle::try_current() {
                h.spawn(reap(Arc::downgrade(self)));
            }
        });
    }
}

/// Runs [`CursorRegistry::reap_once`] every second while the registry
/// lives.
async fn reap(reg: Weak<CursorRegistry>) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tick.tick().await;
        let Some(r) = reg.upgrade() else { return };
        r.reap_once(Instant::now()).await;
    }
}

/// Waits for the first result's header: its columns, or the statement's
/// end without a result set, or its error.
async fn header(
    rx: &mut mpsc::Receiver<Item>,
    wait: Duration,
) -> Result<Result<Vec<Value>, Value>, SprigError> {
    let deadline = Instant::now() + wait;
    loop {
        let item = match tokio::time::timeout_at(deadline.into(), rx.recv()).await {
            Err(_) => {
                return Err(SprigError::standard(
                    3024,
                    *b"HY000",
                    "Query execution was interrupted, maximum statement execution time exceeded",
                ));
            }
            Ok(None) => return Ok(Err(json!({}))),
            Ok(Some(i)) => i,
        };
        match item {
            Item::Error(e) => return Err(e),
            Item::Part(p) => {
                let v = results::query_part_json(&p);
                match p.part {
                    Some(Part::Header(_)) => {
                        let cols = v["header"]["columns"]
                            .as_array()
                            .cloned()
                            .unwrap_or_default();
                        return Ok(Ok(cols));
                    }
                    Some(Part::End(_)) => return Ok(Err(v["end"].clone())),
                    _ => {}
                }
            }
        }
    }
}

/// The page size and fetch wait for `caller` at `database`.
fn limits(core: &Arc<Core>, caller: &Caller, database: &str, asked: u64) -> (usize, Duration) {
    use sprig_studio::settings as k;
    let st = &core.studio;
    let t = caller.principal.tenant;
    let studio = session_client(caller.authorization.as_deref()).as_deref() == Some("studio");
    let max = if studio {
        st.tenant_get(&k::MAX_RESULT_ROWS, t, Some(database))
    } else {
        API_PAGE_MAX
    };
    let asked = if asked == 0 { DEFAULT_PAGE } else { asked };
    let page = usize::try_from(Caps::clamp(asked, max)).unwrap_or(usize::MAX);
    let wait = Duration::from_millis(st.tenant_get(&k::STATEMENT_TIMEOUT_MS, t, Some(database)));
    (page.max(1), wait)
}

fn num(v: &Value, k: &str) -> u64 {
    match v.get(k) {
        Some(Value::Number(n)) => n.as_u64().unwrap_or(0),
        Some(Value::String(s)) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

fn page_json(p: &Page) -> Value {
    json!({
        "rows": p.rows,
        "position": p.position.to_string(),
        "done": p.done,
    })
}

/// `Cursors.Open` for `caller`.
///
/// # Errors
/// The statement's errors; 3024 if no header comes within
/// `studio.statement_timeout_ms`.
pub async fn open(
    core: &Arc<Core>,
    caller: &Caller,
    database: &str,
    body: &Value,
) -> Result<Value, SprigError> {
    use sprig_studio::settings as k;
    let sql = body.get("sql").and_then(Value::as_str).unwrap_or("");
    if sql.trim().is_empty() {
        return Err(invalid("sql is required"));
    }
    let (page, wait) = limits(core, caller, database, num(body, "page_rows"));
    let st = &core.studio;
    let t = caller.principal.tenant;
    let idle = Duration::from_secs(st.tenant_get(&k::CURSOR_IDLE_SECONDS, t, None));
    let max_open = usize::try_from(st.tenant_get(&k::CURSOR_MAX_OPEN, t, None)).unwrap_or(4);
    let (tx, mut rx) = mpsc::channel(CHANNEL_DEPTH);
    let engine = Arc::clone(&core.engine);
    let c2 = caller.clone();
    let call = SqlCall {
        database: database.to_owned(),
        sql: sql.to_owned(),
        multi: false,
    };
    // No caps and no statement timeout: the stream waits for Fetch, and
    // the idle timeout ends it.
    tokio::spawn(async move { engine.run_capped(&c2, call, None, Caps::default(), tx).await });
    let columns = match header(&mut rx, wait).await? {
        Ok(c) => c,
        Err(end) => {
            return Ok(json!({
                "name": "",
                "columns": [],
                "rows": [],
                "position": "0",
                "done": true,
                "end": end,
            }));
        }
    };
    let (name, p) = st
        .cursors
        .register(
            (
                &core.engine.dc,
                &core.engine.gateway_id,
                t,
                &caller.principal.user,
            ),
            rx,
            num(body, "skip"),
            page,
            idle,
            max_open,
            wait,
        )
        .await?;
    metrics::counter!("sprig_studio_cursors_opened_total").increment(1);
    let mut out = page_json(&p);
    out["name"] = json!(name);
    out["columns"] = Value::Array(columns);
    out["idle_seconds"] = json!(idle.as_secs());
    Ok(out)
}

/// `Cursors.Fetch` or `Cursors.Close` (`verb`) on this gateway.
///
/// # Errors
/// As [`CursorRegistry::fetch`] and [`CursorRegistry::close`].
pub async fn local(
    core: &Arc<Core>,
    caller: &Caller,
    verb: &str,
    h: &TxnHandle,
    body: &Value,
) -> Result<Value, SprigError> {
    let reg = &core.studio.cursors;
    let (t, u) = (caller.principal.tenant, caller.principal.user.as_str());
    if verb == "close" {
        let closed = reg.close(h, t, u).await?;
        return Ok(json!({ "closed": closed }));
    }
    // The page size is the database's; a cursor doesn't keep its
    // database, so the tenant's value applies.
    let (page, wait) = limits(core, caller, "-", num(body, "rows"));
    let p = reg.fetch(h, t, u, page, wait).await?;
    Ok(page_json(&p))
}

/// A forwarded `Cursors.Fetch` or `Cursors.Close`; `None` for any other
/// method (the peer service's `Forward` handler calls this first).
pub async fn forwarded(
    core: &Arc<Core>,
    caller: &Caller,
    method: &str,
    body: &Value,
) -> Option<Result<(u16, Value), SprigError>> {
    let verb = method.strip_prefix("Cursors.")?;
    let verb = match verb {
        "Fetch" => "fetch",
        "Close" => "close",
        _ => return Some(Err(invalid(format!("can't forward {method}")))),
    };
    let name = body.get("cursor").and_then(Value::as_str).unwrap_or("");
    Some(async {
        let h = parse_name(name)?;
        // A forwarded call is never forwarded again.
        if h.gateway_id != core.engine.gateway_id {
            return Err(unknown());
        }
        local(core, caller, verb, &h, body).await.map(|v| (200, v))
    }.await)
}

/// Routes `POST /v1/databases/{db}/cursors` and
/// `POST /v1/cursors/{h}:fetch | :close`.
///
/// # Errors
/// As the calls; the request back when it isn't a cursor route.
pub async fn route(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Result<Response<Body>, SprigError>, Request<Incoming>> {
    let path = req.uri().path().to_owned();
    if req.method() != hyper::Method::POST {
        return Err(req);
    }
    let open_db = path
        .strip_prefix("/v1/databases/")
        .and_then(|r| r.strip_suffix("/cursors"))
        .filter(|d| !d.is_empty() && !d.contains('/'))
        .map(super::catalog::percent_decode);
    let on = path.strip_prefix("/v1/cursors/").and_then(|r| {
        r.strip_suffix(":fetch")
            .map(|h| (h.to_owned(), "fetch"))
            .or_else(|| r.strip_suffix(":close").map(|h| (h.to_owned(), "close")))
    });
    if open_db.is_none() && on.is_none() {
        return Err(req);
    }
    let auth = hdr(&req, "authorization");
    let tenant = hdr(&req, super::super::rest::TENANT_HEADER);
    let host = hdr(&req, "host");
    Ok(async {
        let caller = core.caller(info, auth, tenant, host).await?;
        let body = parse_json(&read_body(req).await?)?;
        let v = match (open_db, on) {
            (Some(db), _) => open(core, &caller, &db, &body).await?,
            (None, Some((h, verb))) => {
                let name = format!("{PREFIX}{h}");
                let handle = parse_name(&name)?;
                if handle.gateway_id == core.engine.gateway_id {
                    local(core, &caller, verb, &handle, &body).await?
                } else {
                    return forward(core, &caller, verb, &name, &body, &handle).await;
                }
            }
            (None, None) => unreachable!("checked above"),
        };
        Ok(json_response(200, &v))
    }
    .await)
}

/// Sends a cursor call to the gateway that owns it.
async fn forward(
    core: &Arc<Core>,
    caller: &Caller,
    verb: &str,
    name: &str,
    body: &Value,
    h: &TxnHandle,
) -> Result<Response<Body>, SprigError> {
    let peers = core.engine.peers.as_ref().ok_or_else(|| {
        SprigError::new(
            SprigCode::ForwardFailed,
            format!(
                "the cursor is on gateway {}, and this gateway has no peer service",
                h.gateway_id
            ),
        )
    })?;
    let mut b = body.clone();
    if !b.is_object() {
        b = json!({});
    }
    b["cursor"] = json!(name);
    let method = if verb == "close" {
        "Cursors.Close"
    } else {
        "Cursors.Fetch"
    };
    metrics::counter!("sprig_studio_cursors_forwarded_total").increment(1);
    let reply = peers
        .forward(
            &h.gateway_id,
            ForwardRequest {
                authorization: caller.authorization.clone().unwrap_or_default(),
                method: method.to_owned(),
                request_json: b.to_string(),
                client_addr: caller.peer.to_string(),
            },
        )
        .await?;
    let v: Value = serde_json::from_str(&reply.body_json).unwrap_or_else(|_| json!({}));
    Ok(json_response(
        u16::try_from(reply.http_status).unwrap_or(500),
        &v,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_types::{ListValue, Value as PValue, value::Kind};
    use sprig_api::proto::v1::{QueryResponse, ResultEnd, RowBatch};

    fn row(i: u64) -> ListValue {
        ListValue {
            values: vec![PValue {
                kind: Some(Kind::StringValue(i.to_string())),
            }],
        }
    }

    /// A producer of `n` rows in batches of 10 that reports when its
    /// channel closes (the executor killing the backend statement).
    fn producer(n: u64) -> (mpsc::Receiver<Item>, tokio::sync::oneshot::Receiver<u64>) {
        let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut sent = 0;
            while sent < n {
                let b = RowBatch {
                    rows: (sent..(sent + 10).min(n)).map(row).collect(),
                };
                let k = b.rows.len() as u64;
                let p = QueryResponse {
                    part: Some(Part::Rows(b)),
                };
                if tx.send(Item::Part(p)).await.is_err() {
                    let _ = done_tx.send(sent);
                    return;
                }
                sent += k;
            }
            let end = QueryResponse {
                part: Some(Part::End(ResultEnd::default())),
            };
            let _ = tx.send(Item::Part(end)).await;
            let _ = done_tx.send(sent);
        });
        (rx, done_rx)
    }

    const WAIT: Duration = Duration::from_secs(5);

    async fn opened(
        reg: &Arc<CursorRegistry>,
        user: &str,
        n: u64,
        skip: u64,
        idle: Duration,
    ) -> (String, Page, tokio::sync::oneshot::Receiver<u64>) {
        let (rx, done) = producer(n);
        let (name, p) = reg
            .register(("dc-a", "gw-1", Some(TenantId(7)), user), rx, skip, 25, idle, 4, WAIT)
            .await
            .unwrap();
        (name, p, done)
    }

    fn first(rows: &[Value]) -> &str {
        rows[0][0].as_str().unwrap()
    }

    #[tokio::test]
    async fn pages_follow_each_other_to_the_end() {
        let reg = Arc::new(CursorRegistry::default());
        let (name, p, done) = opened(&reg, "alice", 60, 0, WAIT).await;
        assert!(name.starts_with(PREFIX));
        assert_eq!((p.rows.len(), p.position, p.done), (25, 25, false));
        let h = parse_name(&name).unwrap();
        let p2 = reg.fetch(&h, Some(TenantId(7)), "alice", 25, WAIT).await.unwrap();
        assert_eq!(first(&p2.rows), "25");
        let p3 = reg.fetch(&h, Some(TenantId(7)), "alice", 25, WAIT).await.unwrap();
        assert_eq!((p3.rows.len(), p3.position, p3.done), (10, 60, true));
        assert_eq!(done.await.unwrap(), 60);
        // Done closes it.
        assert!(reg.is_empty());
        let e = reg.fetch(&h, Some(TenantId(7)), "alice", 25, WAIT).await.unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::StudioCursorUnknown));
    }

    #[tokio::test]
    async fn skip_starts_after_the_rows_already_shown() {
        let reg = Arc::new(CursorRegistry::default());
        let (_, p, _) = opened(&reg, "alice", 100, 40, WAIT).await;
        assert_eq!(first(&p.rows), "40");
        assert_eq!(p.position, 65);
    }

    #[tokio::test]
    async fn backpressure_stops_the_producer_between_fetches() {
        let reg = Arc::new(CursorRegistry::default());
        let (name, _, mut done) = opened(&reg, "alice", 1_000_000, 0, WAIT).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        // Only the channel's few batches beyond the page were produced.
        assert!(done.try_recv().is_err(), "the producer is still waiting");
        let h = parse_name(&name).unwrap();
        assert!(reg.close(&h, Some(TenantId(7)), "alice").await.unwrap());
        let sent = done.await.unwrap();
        assert!(sent < 25 + 10 * (CHANNEL_DEPTH as u64 + 2), "produced {sent}");
    }

    #[tokio::test]
    async fn an_idle_cursor_is_closed_and_its_stream_released() {
        let reg = Arc::new(CursorRegistry::default());
        let (name, _, done) = opened(&reg, "alice", 1_000_000, 0, Duration::from_millis(50)).await;
        let h = parse_name(&name).unwrap();
        assert_eq!(reg.reap_once(Instant::now()).await, 0);
        assert_eq!(
            reg.reap_once(Instant::now() + Duration::from_millis(60)).await,
            1
        );
        // The producer saw its channel close: the executor would kill the
        // backend statement and close its connection here.
        let sent = tokio::time::timeout(WAIT, done).await.unwrap().unwrap();
        assert!(sent < 1_000_000);
        assert!(reg.is_empty());
        let e = reg.fetch(&h, Some(TenantId(7)), "alice", 25, WAIT).await.unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::StudioCursorExpired));
        // Closing an expired cursor is not an error.
        assert!(!reg.close(&h, Some(TenantId(7)), "alice").await.unwrap());
    }

    #[tokio::test]
    async fn another_user_sees_no_cursor() {
        let reg = Arc::new(CursorRegistry::default());
        let (name, _, _) = opened(&reg, "alice", 100, 0, WAIT).await;
        let h = parse_name(&name).unwrap();
        for (t, u) in [(Some(TenantId(7)), "bob"), (Some(TenantId(8)), "alice")] {
            let e = reg.fetch(&h, t, u, 25, WAIT).await.unwrap_err();
            assert_eq!(e.code(), Some(SprigCode::StudioCursorUnknown));
            let e = reg.close(&h, t, u).await.unwrap_err();
            assert_eq!(e.code(), Some(SprigCode::StudioCursorUnknown));
        }
        assert_eq!(reg.len(), 1);
    }

    #[tokio::test]
    async fn the_least_recently_used_cursor_makes_room() {
        let reg = Arc::new(CursorRegistry::default());
        let mut names = Vec::new();
        for _ in 0..4 {
            names.push(opened(&reg, "alice", 1000, 0, WAIT).await.0);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Use the first, so the second is the least recently used.
        let h0 = parse_name(&names[0]).unwrap();
        reg.fetch(&h0, Some(TenantId(7)), "alice", 1, WAIT).await.unwrap();
        let _fifth = opened(&reg, "alice", 1000, 0, WAIT).await;
        assert_eq!(reg.len(), 4);
        let h1 = parse_name(&names[1]).unwrap();
        let e = reg.fetch(&h1, Some(TenantId(7)), "alice", 1, WAIT).await.unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::StudioCursorExpired));
        // Another user's cursors don't count.
        let _bob = opened(&reg, "bob", 1000, 0, WAIT).await;
        assert_eq!(reg.len(), 5);
    }

    #[tokio::test]
    async fn a_short_result_needs_no_cursor() {
        let reg = Arc::new(CursorRegistry::default());
        let (name, p, _) = opened(&reg, "alice", 20, 0, WAIT).await;
        assert_eq!((name.as_str(), p.rows.len(), p.done), ("", 20, true));
        assert!(reg.is_empty());
    }

    #[test]
    fn names_round_trip_and_refuse_transaction_handles_shapes() {
        let h = TxnHandle::new("dc-a", "gw-1", 9).unwrap();
        let n = cursor_name(&h);
        assert!(n.starts_with("cursors/"));
        assert_eq!(parse_name(&n).unwrap(), h);
        for bad in ["cursors/", "cursors/a/b", "nonsense", ""] {
            assert_eq!(
                parse_name(bad).unwrap_err().code(),
                Some(SprigCode::StudioCursorUnknown)
            );
        }
    }
}
