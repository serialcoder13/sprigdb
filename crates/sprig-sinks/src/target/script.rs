//! Script sinks (chapter 51, "Script sinks", "Script sink runtime"; build
//! steps 14 and 19): the delivery code is a JavaScript, KIRun or Lua
//! function the tenant writes, run on chapter 41's runtimes
//! (`sprig-logic-js`: QuickJS, native KIRun, the shared Lua 5.1 runtime).
//!
//! ```text
//! deliver_script(sink s, batch b):
//!   rt := the language's runtime, fresh per batch attempt
//!   budgets := sink.script_budget_ms (wall), logic memory and steps,
//!              sink.script_max_fetches, sink.script_db_max_queries,
//!              sink.script_db_max_rows, sink.script_db_budget_ms
//!   res := rt.call(deliver, b as JSON, ctx)
//!   budget exceeded → |b| > 1: TooLarge (the runtime halves it, no failure)
//!                     else: the record fails permanently (39220)
//!   map res onto outcomes; malformed or a thrown error → retryable 39220
//! ```
//!
//! **The contract.** `deliver(batch, ctx)` with `batch = {id, attempt,
//! records}` (each record as [`SinkRecord::to_json`]). It returns nothing,
//! `true` or `{ok: true}` (all delivered); `{retry: true, retryAfterMs?}`
//! (the whole batch, destination-wide); `{fail: reason}` (the whole batch
//! fails: the runtime halves it to find the record); or `{results: [...]}`
//! with `"ok"`, `"retry"`, `{retry: reason}` or `{fail: reason}` per record.
//! A thrown `SinkPermanentError` (JavaScript), `error({permanent = true,
//! reason = ...})` (Lua) or the `permanentError` event (KIRun) fails the
//! batch; KIRun's `retry` event retries it.
//!
//! **Forms.** JavaScript: a module exporting `deliver` (or a default
//! export), a script defining `function deliver`, or a bare body with
//! `batch` and `ctx` in scope. Lua: a chunk defining the global function
//! `deliver(batch, ctx)` or returning a function. KIRun: a definition with
//! the parameters `batch` and `ctx`.
//!
//! **What a script reaches.** Generated wrappers (JavaScript and Lua) give
//! `ctx` the sink's helpers, all served by this module's host:
//! - `fetch(url, {connection: 'name', ...})` (`sprig.fetch` in Lua;
//!   KIRun's `Sprig.Http.Fetch` with an `x-sprig-connection` header): the
//!   sink's own destination rules ([`Destinations`]: the allowlist,
//!   residency, address pinning; 39213 off the list), never
//!   `logic.outbound_allowlist`; a named connection (one of `USING
//!   CONNECTIONS`) adds its `bearer`, `basic` or `header` authentication,
//!   and response headers echoing authorization are removed;
//! - `ctx.sign(connection, name, data)`: hex HMAC-SHA256 under that
//!   connection's credential `name`, whose value the script never sees;
//! - `ctx.connections`: names and kinds only;
//! - `ctx.db.sql | query | get` (JavaScript), `sprig.db.query | get` (Lua),
//!   `Sprig.Db.Query | QueryOne` (KIRun): the **read-only `db` handle**
//!   ([`ReadOnlyDb`]): one `START TRANSACTION READ ONLY` per batch
//!   attempt, as the sink's owner; a single `SELECT` or `WITH` only, else
//!   39235;
//! - `ctx.targets.webhook(name).post(records)` (Lua
//!   `sprig.sink.webhook_post(name, records)`): the webhook target's client
//!   over a named connection (`Sprig.Sink.Webhook.Post`).
//!
//! Internal calls travel as `fetch` to `sprig-sink://...` URLs, which the
//! host answers itself; the runtimes need no new host functions.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::client::conn::http1;
use hyper::header::{HeaderName, HeaderValue};
use hyper_util::rt::TokioIo;
use serde_json::{Map, Value, json};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic_js::Limits;
use sprig_logic_js::bridge::{ColKind, Column, DbBridge, DbOp, Reply, Shape};
use sprig_logic_js::engine::{ParamTy, ScriptSig};
use sprig_logic_js::kirun::Host;
use sprig_logic_js::outbound::{Outbound, Request, Response};
use sprig_logic_js::program::Program;
use tokio::sync::Mutex as AsyncMutex;

use super::webhook::WebhookTarget;
use super::{Conn, MemSecrets, TargetCtx};
use crate::model::{
    Batch, BatchOutcome, ConnKind, Outcome, Position, ScriptLanguage, SinkRecord, TargetSpec,
    bad_option, delivery_failed, err,
};
use crate::policy::{Destinations, Unpinned, url_host_port};
use crate::runtime::Target;

/// The scheme of the host's own calls.
const INTERNAL: &str = "sprig-sink://";
/// The request header naming a `USING CONNECTIONS` connection.
const CONN_HEADER: &str = "x-sprig-connection";
/// Response bodies a script may read, at most.
const MAX_BODY: usize = 4 << 20;

// ---- the read-only db handle ------------------------------------------------

/// One batch attempt's read-only session.
#[async_trait]
pub trait ReadSession: Send {
    /// Runs one checked `SELECT`; the columns and rows as text.
    ///
    /// # Errors
    /// The statement's error (1142 for a missing grant, ...).
    async fn query(&mut self, sql: &str) -> Result<(Vec<Column>, Vec<Vec<Option<String>>>)>;

    /// Ends the session (`ROLLBACK`).
    async fn end(self: Box<Self>);
}

/// The read-only `db` handle of script sinks (Kiran, 9 November 2026,
/// answer 6): reads as the sink's owner.
#[async_trait]
pub trait ReadOnlyDb: Send + Sync {
    /// Opens a session: one read-only transaction for one batch attempt.
    ///
    /// # Errors
    /// The backend's (a destination-wide retry of the batch).
    async fn begin(&self) -> Result<Box<dyn ReadSession>>;
}

/// A [`ReadOnlyDb`] over a pool connected as the sink owner's backend
/// account, so MariaDB itself enforces the owner's grants. Names in
/// statements resolve in the pool's default schema.
#[derive(Debug, Clone)]
pub struct MysqlReadOnly {
    pool: mysql_async::Pool,
}

impl MysqlReadOnly {
    /// Over `pool` (min 0 needs an inactive TTL: the brief's rule 6).
    #[must_use]
    pub fn new(pool: mysql_async::Pool) -> Self {
        Self { pool }
    }
}

fn mysql_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => {
            let mut state = *b"HY000";
            if s.state.len() == 5 {
                state.copy_from_slice(s.state.as_bytes());
            }
            SprigError::backend(s.code, state, s.message.clone())
        }
        other => {
            let mut x = delivery_failed(format!("script db: {other}"));
            x.retryable = true;
            x
        }
    }
}

struct MysqlSession {
    conn: mysql_async::Conn,
}

fn col_kind(t: mysql_async::consts::ColumnType) -> ColKind {
    use mysql_async::consts::ColumnType as T;
    match t {
        T::MYSQL_TYPE_TINY
        | T::MYSQL_TYPE_SHORT
        | T::MYSQL_TYPE_LONG
        | T::MYSQL_TYPE_INT24
        | T::MYSQL_TYPE_LONGLONG
        | T::MYSQL_TYPE_YEAR => ColKind::Int,
        T::MYSQL_TYPE_DECIMAL
        | T::MYSQL_TYPE_NEWDECIMAL
        | T::MYSQL_TYPE_FLOAT
        | T::MYSQL_TYPE_DOUBLE => ColKind::Float,
        T::MYSQL_TYPE_JSON => ColKind::Json,
        _ => ColKind::Str,
    }
}

#[async_trait]
impl ReadSession for MysqlSession {
    async fn query(&mut self, sql: &str) -> Result<(Vec<Column>, Vec<Vec<Option<String>>>)> {
        use mysql_async::prelude::Queryable;
        let mut r = self.conn.query_iter(sql).await.map_err(mysql_err)?;
        let cols: Vec<Column> = r
            .columns()
            .map(|cs| {
                cs.iter()
                    .map(|c| Column {
                        name: c.name_str().into_owned(),
                        kind: col_kind(c.column_type()),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let rows: Vec<mysql_async::Row> = r.collect().await.map_err(mysql_err)?;
        drop(r);
        let rows = rows
            .into_iter()
            .map(|row| {
                (0..row.len())
                    .map(|i| match row.as_ref(i) {
                        None | Some(mysql_async::Value::NULL) => None,
                        Some(mysql_async::Value::Bytes(b)) => {
                            Some(String::from_utf8_lossy(b).into_owned())
                        }
                        Some(v) => Some(v.as_sql(true).trim_matches('\'').to_owned()),
                    })
                    .collect()
            })
            .collect();
        Ok((cols, rows))
    }

    async fn end(mut self: Box<Self>) {
        use mysql_async::prelude::Queryable;
        let _ = self.conn.query_drop("ROLLBACK").await;
    }
}

#[async_trait]
impl ReadOnlyDb for MysqlReadOnly {
    async fn begin(&self) -> Result<Box<dyn ReadSession>> {
        use mysql_async::prelude::Queryable;
        let mut conn = self.pool.get_conn().await.map_err(mysql_err)?;
        conn.query_drop("START TRANSACTION READ ONLY")
            .await
            .map_err(mysql_err)?;
        Ok(Box::new(MysqlSession { conn }))
    }
}

/// 39235 unless `sql` is one `SELECT` or `WITH` query (no `INTO`).
///
/// # Errors
/// 39235.
pub fn check_read_only(sql: &str) -> Result<()> {
    use sqlparser::ast::{SetExpr, Statement};
    let refuse = |why: &str| {
        err(
            SprigCode::SinkScriptReadOnly,
            format!("the script's db handle only reads: {why}"),
        )
    };
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::MySqlDialect {}, sql)
        .map_err(|e| refuse(&format!("the statement doesn't parse as a read ({e})")))?;
    let [Statement::Query(q)] = stmts.as_slice() else {
        return Err(refuse("only one SELECT or WITH statement is allowed"));
    };
    if let SetExpr::Select(s) = &*q.body
        && s.into.is_some()
    {
        return Err(refuse("SELECT ... INTO writes"));
    }
    Ok(())
}

fn budget(setting: &str, limit: u64) -> SprigError {
    SprigError::new(
        SprigCode::LogicBudgetExceeded,
        format!("logic budget exceeded: {setting} = {limit}"),
    )
}

/// The read limits of one batch attempt.
#[derive(Debug, Clone, Copy)]
struct DbLimits {
    queries: u64,
    rows: u64,
    budget: Duration,
}

/// The `db` bridge a script sees: reads only, through one session per
/// batch attempt, within the read limits.
struct ReadBridge {
    db: Option<Arc<dyn ReadOnlyDb>>,
    session: AsyncMutex<Option<Box<dyn ReadSession>>>,
    limits: DbLimits,
    queries: AtomicU64,
    rows: AtomicU64,
    spent_us: AtomicU64,
    language: &'static str,
}

impl ReadBridge {
    async fn end(&self) {
        if let Some(s) = self.session.lock().await.take() {
            s.end().await;
        }
    }

    async fn run(&self, op: &DbOp) -> Result<Reply> {
        if !matches!(op.shape, Shape::One | Shape::Many) {
            return Err(err(
                SprigCode::SinkScriptReadOnly,
                format!(
                    "the script's db handle only reads: {} isn't allowed",
                    op.shape.function()
                ),
            ));
        }
        check_read_only(&op.sql)?;
        let Some(db) = &self.db else {
            return Err(err(
                SprigCode::SinkScriptReadOnly,
                "this sink has no db handle",
            ));
        };
        let n = self.queries.fetch_add(1, Ordering::SeqCst) + 1;
        if n > self.limits.queries {
            return Err(budget("sink.script_db_max_queries", self.limits.queries));
        }
        let spent = Duration::from_micros(self.spent_us.load(Ordering::SeqCst));
        let left = self.limits.budget.saturating_sub(spent);
        if left.is_zero() {
            return Err(budget(
                "sink.script_db_budget_ms",
                u64::try_from(self.limits.budget.as_millis()).unwrap_or(u64::MAX),
            ));
        }
        let mut g = self.session.lock().await;
        if g.is_none() {
            *g = Some(db.begin().await?);
        }
        let Some(s) = g.as_mut() else {
            return Err(SprigError::internal("no read session"));
        };
        let t0 = Instant::now();
        let r = tokio::time::timeout(left, s.query(&op.sql)).await;
        self.spent_us.fetch_add(
            u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX),
            Ordering::SeqCst,
        );
        let (columns, rows) = match r {
            Ok(r) => r?,
            Err(_) => {
                // The statement may still run: drop the session.
                g.take();
                return Err(budget(
                    "sink.script_db_budget_ms",
                    u64::try_from(self.limits.budget.as_millis()).unwrap_or(u64::MAX),
                ));
            }
        };
        let total = self.rows.fetch_add(rows.len() as u64, Ordering::SeqCst) + rows.len() as u64;
        if total > self.limits.rows {
            return Err(budget("sink.script_db_max_rows", self.limits.rows));
        }
        Ok(Reply::Rows { columns, rows })
    }
}

#[async_trait]
impl DbBridge for ReadBridge {
    async fn exec(&self, op: DbOp) -> Result<Reply> {
        let r = self.run(&op).await;
        let result = match &r {
            Ok(_) => "ok",
            Err(e) if e.code() == Some(SprigCode::SinkScriptReadOnly) => "refused",
            Err(e) if e.code() == Some(SprigCode::LogicBudgetExceeded) => "budget",
            Err(_) => "error",
        };
        metrics::counter!("sprig_sink_script_db_queries_total", "language" => self.language, "result" => result)
            .increment(1);
        r
    }
}

// ---- outbound ---------------------------------------------------------------

/// What every batch attempt's host shares.
struct Shared {
    ctx: TargetCtx,
    dest: Arc<Destinations>,
    conns: HashMap<String, Conn>,
    max_fetches: u64,
    timeout: Duration,
    webhooks: StdMutex<HashMap<String, Arc<WebhookTarget>>>,
}

/// The sink's `fetch`: the sink's destination rules, connection
/// authentication, and the host's own `sprig-sink://` calls.
struct SinkOutbound {
    sh: Arc<Shared>,
    fetches: AtomicU64,
}

fn header<'a>(req: &'a Request, name: &str) -> Option<&'a str> {
    req.headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

fn ok_json(v: &Value) -> Response {
    Response {
        status: 200,
        headers: vec![("content-type".into(), "application/json".into())],
        body: v.to_string(),
    }
}

fn hmac_hex(key: &[u8], data: &[u8]) -> String {
    let k = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key);
    crate::model::hex(ring::hmac::sign(&k, data).as_ref())
}

/// A record a script hands to a target client: `{key?, value?, headers?,
/// idempotencyKey?}`, or any other value as the value.
fn record_of(v: &Value, i: usize, batch: &str) -> SinkRecord {
    let o = v.as_object();
    let field = |k: &str| o.and_then(|o| o.get(k));
    let bytes = |x: &Value| match x {
        Value::String(s) => s.clone().into_bytes(),
        other => other.to_string().into_bytes(),
    };
    let is_record = o.is_some_and(|o| o.contains_key("value") || o.contains_key("key"));
    let value = if is_record {
        field("value").filter(|x| !x.is_null()).map(bytes)
    } else {
        Some(bytes(v))
    };
    let headers = field("headers")
        .and_then(Value::as_object)
        .map(|h| {
            h.iter()
                .map(|(k, x)| (k.clone(), bytes(x)))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let idem = field("idempotencyKey")
        .and_then(Value::as_str)
        .map_or_else(|| format!("script/{batch}/{i}"), str::to_owned);
    SinkRecord {
        key: field("key").filter(|x| !x.is_null()).map(bytes),
        value,
        headers,
        op: None,
        before: None,
        after: None,
        ts_ms: 0,
        idem_key: idem.clone(),
        pos: Position::Queue {
            queue: 0,
            msg_id: idem,
            delivery: String::new(),
        },
    }
}

fn outcome_text(o: &Outcome) -> Value {
    match o {
        Outcome::Ok => json!({"status": "ok"}),
        Outcome::Retry { after, reason } => json!({
            "status": "retry",
            "reason": reason,
            "retryAfterMs": after.map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
        }),
        Outcome::Fail { reason } => json!({"status": "fail", "reason": reason}),
    }
}

impl SinkOutbound {
    fn conn(&self, name: &str) -> Result<&Conn> {
        self.sh.conns.get(name).ok_or_else(|| {
            err(
                SprigCode::SinkPrivilege,
                format!("connection {name} isn't among the sink's USING CONNECTIONS"),
            )
        })
    }

    async fn internal(&self, req: &Request) -> Result<Response> {
        let path = req.url[INTERNAL.len()..].to_owned();
        let body: Value = req
            .body
            .as_deref()
            .and_then(|b| serde_json::from_str(b).ok())
            .unwrap_or(Value::Null);
        if path == "sign" {
            let c = self.conn(body["connection"].as_str().unwrap_or(""))?;
            let name = body["name"].as_str().unwrap_or("");
            let secret = c.secret(name)?;
            let data = match &body["data"] {
                Value::String(s) => s.clone().into_bytes(),
                Value::Null => Vec::new(),
                other => other.to_string().into_bytes(),
            };
            return Ok(ok_json(
                &json!({"hmac": hmac_hex(secret.as_bytes(), &data)}),
            ));
        }
        if let Some(name) = path.strip_prefix("webhook/") {
            let name = percent_decode(name);
            let target = self.webhook(&name)?;
            let items = match &body["records"] {
                Value::Array(a) => a.clone(),
                Value::Null => Vec::new(),
                other => vec![other.clone()],
            };
            let batch_seed = crate::model::hex(&rand8());
            let recs: Vec<SinkRecord> = items
                .iter()
                .enumerate()
                .map(|(i, v)| record_of(v, i, &batch_seed))
                .collect();
            let b = Batch::new(recs, 0);
            let out = match target.deliver(&b).await {
                Ok(BatchOutcome::All(o)) => outcome_text(&o),
                Ok(BatchOutcome::Each(v)) => {
                    json!({"status": "each", "results": v.iter().map(outcome_text).collect::<Vec<_>>()})
                }
                Ok(BatchOutcome::TooLarge) => {
                    json!({"status": "fail", "reason": "the destination says the batch is too large"})
                }
                Err(e) => json!({"status": "retry", "reason": e.message}),
            };
            return Ok(ok_json(&out));
        }
        Err(bad_option(format!("unknown sink call {path}")))
    }

    fn webhook(&self, name: &str) -> Result<Arc<WebhookTarget>> {
        let c = self.conn(name)?.clone();
        if c.kind != ConnKind::Webhook {
            return Err(bad_option(format!(
                "connection {name} is a {} connection, not a webhook",
                c.kind
            )));
        }
        let mut m = self
            .sh
            .webhooks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(t) = m.get(name) {
            return Ok(Arc::clone(t));
        }
        let mut cx = self.sh.ctx.clone();
        cx.conn = Some(c);
        let t = Arc::new(WebhookTarget::new(&cx)?);
        m.insert(name.to_owned(), Arc::clone(&t));
        Ok(t)
    }

    async fn http(&self, mut req: Request) -> Result<Response> {
        let n = self.fetches.fetch_add(1, Ordering::SeqCst) + 1;
        if n > self.sh.max_fetches {
            return Err(budget("sink.script_max_fetches", self.sh.max_fetches));
        }
        let named = header(&req, CONN_HEADER).map(str::to_owned);
        req.headers.retain(|(k, _)| !k.eq_ignore_ascii_case(CONN_HEADER));
        let (conn, dest) = match &named {
            Some(n) => {
                let c = self.conn(n)?.clone();
                let d = Arc::clone(&c.dest);
                (Some(c), d)
            }
            None => (None, Arc::clone(&self.sh.dest)),
        };
        // The connection's authentication; its values are never shown.
        let mut secrets: Vec<String> = Vec::new();
        if let Some(c) = &conn {
            let auth = c.opt("auth").unwrap_or_else(|| "none".into());
            match auth.to_ascii_lowercase().as_str() {
                "bearer" => {
                    let t = c.secret("token")?;
                    secrets.push(t.to_string());
                    req.headers
                        .push(("authorization".into(), format!("Bearer {}", t.as_str())));
                }
                "basic" => {
                    let u = c.secret("username")?;
                    let p = c.secret("password")?;
                    let v = B64.encode(format!("{}:{}", u.as_str(), p.as_str()));
                    secrets.push(p.to_string());
                    secrets.push(v.clone());
                    req.headers
                        .push(("authorization".into(), format!("Basic {v}")));
                }
                "header" => {
                    let h = c.need("auth_header")?;
                    let v = c.secret("header_value")?;
                    secrets.push(v.to_string());
                    req.headers.push((h, v.to_string()));
                }
                _ => {}
            }
        }
        let r = tokio::time::timeout(self.sh.timeout, self.send(&req, conn.as_ref(), &dest)).await;
        let mut resp = match r {
            Ok(r) => r?,
            Err(_) => {
                return Err(delivery_failed(format!(
                    "fetch {}: no answer within sink.http_timeout_ms = {} ms",
                    host_text(&req.url),
                    self.sh.timeout.as_millis()
                )));
            }
        };
        // Response headers that echo authorization never reach the script.
        resp.headers.retain(|(k, v)| {
            !k.contains("authorization")
                && !secrets.iter().any(|s| !s.is_empty() && v.contains(s.as_str()))
        });
        Ok(resp)
    }

    async fn send(&self, req: &Request, conn: Option<&Conn>, dest: &Destinations) -> Result<Response> {
        let (host, port, tls) = url_host_port(&req.url)?;
        let uri: hyper::Uri = req
            .url
            .parse()
            .map_err(|e| bad_option(format!("bad url: {e}")))?;
        let addr = dest
            .resolve(&host, port, Some(ConnKind::Webhook), conn.map(|c| &c.options))
            .await?;
        let tcp = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|e| delivery_failed(format!("fetch {host}:{port}: connect failed: {e}")))?;
        let _ = tcp.set_nodelay(true);
        let path = uri.path_and_query().map_or("/", |p| p.as_str()).to_owned();
        let authority = if (tls && port == 443) || (!tls && port == 80) {
            host.clone()
        } else {
            format!("{host}:{port}")
        };
        let mut b = hyper::Request::builder()
            .method(req.method.as_str())
            .uri(path)
            .header(hyper::header::HOST, authority);
        for (k, v) in &req.headers {
            let name = HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| bad_option(format!("header {k}: {e}")))?;
            let mut value = HeaderValue::from_str(v)
                .map_err(|e| bad_option(format!("header {k}: {e}")))?;
            value.set_sensitive(true);
            b = b.header(name, value);
        }
        let hreq = b
            .body(Full::new(Bytes::from(req.body.clone().unwrap_or_default())))
            .map_err(|e| bad_option(format!("request: {e}")))?;
        let (status, headers, body) = if tls {
            let plain;
            let c = match conn {
                Some(c) => c,
                None => {
                    plain = plain_conn(dest);
                    &plain
                }
            };
            let cfg = super::tls_config(c, &[b"http/1.1"])?;
            let name = rustls::pki_types::ServerName::try_from(host.clone())
                .map_err(|e| bad_option(format!("host '{host}': {e}")))?;
            let s = tokio_rustls::TlsConnector::from(cfg)
                .connect(name, tcp)
                .await
                .map_err(|e| delivery_failed(format!("fetch {host}: TLS failed: {e}")))?;
            exchange(TokioIo::new(s), hreq, &host).await?
        } else {
            exchange(TokioIo::new(tcp), hreq, &host).await?
        };
        Ok(Response {
            status,
            headers,
            body,
        })
    }
}

fn plain_conn(dest: &Destinations) -> Conn {
    let _ = dest;
    Conn {
        name: String::new(),
        kind: ConnKind::Webhook,
        options: Map::new(),
        secrets: Arc::new(MemSecrets::default()),
        dest: Arc::new(Destinations::allow_list(&[])),
    }
}

async fn exchange<T>(
    io: T,
    req: hyper::Request<Full<Bytes>>,
    host: &str,
) -> Result<(u16, Vec<(String, String)>, String)>
where
    T: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    let (mut s, c) = http1::handshake(io)
        .await
        .map_err(|e| delivery_failed(format!("fetch {host}: handshake: {e}")))?;
    tokio::spawn(async move {
        let _ = c.await;
    });
    let resp = s
        .send_request(req)
        .await
        .map_err(|e| delivery_failed(format!("fetch {host}: request failed: {e}")))?;
    let (parts, body) = resp.into_parts();
    let bytes = Limited::new(body, MAX_BODY)
        .collect()
        .await
        .map_err(|e| delivery_failed(format!("fetch {host}: response body: {e}")))?
        .to_bytes();
    let headers = parts
        .headers
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_ascii_lowercase(),
                String::from_utf8_lossy(v.as_bytes()).into_owned(),
            )
        })
        .collect();
    Ok((
        parts.status.as_u16(),
        headers,
        String::from_utf8_lossy(&bytes).into_owned(),
    ))
}

fn host_text(url: &str) -> String {
    url_host_port(url).map_or_else(|_| url.chars().take(80).collect(), |(h, _, _)| h)
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len() + 0
            && let Ok(v) = u8::from_str_radix(std::str::from_utf8(&b[i + 1..i + 3]).unwrap_or(""), 16)
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn rand8() -> [u8; 8] {
    let mut b = [0u8; 8];
    let _ = getrandom::fill(&mut b);
    b
}

#[async_trait]
impl Outbound for SinkOutbound {
    async fn fetch(&self, req: Request) -> Result<Response> {
        if req.url.starts_with(INTERNAL) {
            return self.internal(&req).await;
        }
        self.http(req).await
    }
}

// ---- programs ---------------------------------------------------------------

/// The JavaScript wrapper, appended to the script: `fetch` with a
/// `connection`, `SinkPermanentError`, and `ctx`'s helpers.
const JS_WRAP: &str = r#"
;const __sprig_fetch0 = globalThis.fetch;
globalThis.fetch = (url, init) => {
  if (init && init.connection !== undefined && init.connection !== null) {
    const h = Object.assign({}, init.headers || {});
    h["x-sprig-connection"] = String(init.connection);
    const i = Object.assign({}, init, { headers: h });
    delete i.connection;
    init = i;
  }
  return __sprig_fetch0(url, init);
};
globalThis.SinkPermanentError = class SinkPermanentError extends Error {
  constructor(message) { super(message); this.name = "SinkPermanentError"; }
};
const __sprig_internal = (path, payload) =>
  JSON.parse(__sprig_fetch0("sprig-sink://" + path, { method: "POST", body: JSON.stringify(payload) }).body);
const __sprig_ctx = (c) => {
  const out = Object.assign({}, c);
  out.log = (...xs) => console.log(...xs);
  out.db = Object.freeze({
    sql: (s, a) => db.sql(s, a), query: (s, a) => db.sql(s, a), get: (s, a) => db.sqlOne(s, a),
  });
  out.sign = (connection, name, data) => __sprig_internal("sign", {
    connection, name, data: typeof data === "string" ? data : JSON.stringify(data),
  }).hmac;
  out.retryAfter = (res) => {
    const h = res && res.headers ? (res.headers["retry-after"] ?? null) : null;
    if (h === null) return null;
    const n = Number(h);
    if (Number.isFinite(n)) return Math.max(0, n * 1000);
    const t = Date.parse(h);
    return Number.isFinite(t) ? Math.max(0, t - Date.now()) : null;
  };
  out.targets = Object.freeze({
    webhook: (name) => Object.freeze({
      post: (records) => __sprig_internal("webhook/" + encodeURIComponent(name), { records }),
    }),
  });
  return Object.freeze(out);
};
export default async function (__sprig_b, __sprig_c) {
  try {
    return await __SPRIG_USER__(__sprig_b, __sprig_ctx(__sprig_c));
  } catch (e) {
    if (e && e.name === "SinkPermanentError") return { __sprig_permanent: String(e.message) };
    throw e;
  }
}
"#;

/// The Lua wrapper around the script (`__SPRIG_SOURCE__` is replaced).
const LUA_WRAP: &str = r#"local deliver
local __sprig_user = (function()
__SPRIG_SOURCE__
end)()
if __sprig_user == nil then __sprig_user = deliver end
if type(__sprig_user) ~= "function" then
  error("the script defines no function deliver(batch, ctx)", 0)
end
local __fetch0 = fetch
local function __sprig_fetch(url, init)
  if type(init) == "table" and init.connection ~= nil then
    local i = {}
    for k, v in pairs(init) do if k ~= "connection" then i[k] = v end end
    local h = {}
    if type(init.headers) == "table" then for k, v in pairs(init.headers) do h[k] = v end end
    h["x-sprig-connection"] = tostring(init.connection)
    i.headers = h
    init = i
  end
  return __fetch0(url, init)
end
fetch = __sprig_fetch
sprig.fetch = __sprig_fetch
local function __internal(path, payload)
  local r = __fetch0("sprig-sink://" .. path, {method = "POST", body = cjson.encode(payload)})
  return cjson.decode(r.body)
end
sprig.db = {
  query = function(s, ...) return db.sql(s, ...) end,
  get = function(s, ...) return db.sqlOne(s, ...) end,
}
sprig.sink = {
  webhook_post = function(name, records) return __internal("webhook/" .. name, {records = records}) end,
}
local c = ctx
if type(c) ~= "table" then c = {} end
c.sign = function(connection, name, data)
  return __internal("sign", {connection = connection, name = name, data = data}).hmac
end
c.log = log
c.db = sprig.db
local ok, res = pcall(__sprig_user, batch, c)
if not ok then
  if type(res) == "table" and res.permanent then
    return {__sprig_permanent = tostring(res.reason or res.message or "permanent failure")}
  end
  error(res, 0)
end
return res
"#;

fn is_module(source: &str) -> bool {
    source.lines().any(|l| {
        let t = l.trim_start();
        ["export ", "export{", "import ", "import{"]
            .iter()
            .any(|p| t.starts_with(p))
    })
}

/// The JavaScript program text for `source` (see the module docs).
#[must_use]
pub fn js_source(source: &str) -> String {
    let (body, user) = if is_module(source) {
        if let Some(i) = source.find("export default") {
            let mut s = source.to_owned();
            s.replace_range(i..i + "export default".len(), "const __sprig_user =");
            (s, "__sprig_user")
        } else {
            (source.to_owned(), "deliver")
        }
    } else if source.contains("function deliver") {
        (source.to_owned(), "deliver")
    } else {
        (
            format!("async function __sprig_user(batch, ctx) {{{source}\n}}"),
            "__sprig_user",
        )
    };
    format!("{body}\n{}", JS_WRAP.replace("__SPRIG_USER__", user))
}

/// The Lua program text for `source`.
#[must_use]
pub fn lua_source(source: &str) -> String {
    LUA_WRAP.replace("__SPRIG_SOURCE__", source)
}

fn sig() -> ScriptSig {
    ScriptSig {
        params: vec![("batch".into(), ParamTy::Json), ("ctx".into(), ParamTy::Json)],
        scalar: false,
    }
}

/// Compiles a script sink's program.
///
/// # Errors
/// 39861 for source that doesn't compile (KIRun: without the parameters
/// `batch` and `ctx` too).
pub fn program(language: ScriptLanguage, name: &str, source: &str) -> Result<Program> {
    let p = match language {
        ScriptLanguage::Javascript => Program::new("javascript", name, &js_source(source), sig())?,
        ScriptLanguage::Lua => Program::new("lua", name, &lua_source(source), sig())?,
        ScriptLanguage::Kirun => {
            let p = Program::new("kirun", name, source, sig())?;
            if let Program::Kirun(k) = &p
                && k.params() != ["batch", "ctx"]
            {
                return Err(SprigError::new(
                    SprigCode::LogicSyntax,
                    format!(
                        "{name}: a KIRun sink script takes the parameters batch and ctx, not {:?}",
                        k.params()
                    ),
                ));
            }
            p
        }
    };
    Ok(p)
}

// ---- outcomes ---------------------------------------------------------------

fn malformed(why: &str) -> SprigError {
    let mut e = err(
        SprigCode::SinkScriptFailed,
        format!("the script returned a malformed result: {why}"),
    );
    e.retryable = true;
    e
}

fn reason_of(v: &Value, default: &str) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => default.to_owned(),
        Value::Object(o) => o
            .get("reason")
            .or_else(|| o.get("message"))
            .and_then(Value::as_str)
            .map_or_else(|| v.to_string(), str::to_owned),
        other => other.to_string(),
    }
}

fn ms_of(v: Option<&Value>) -> Option<Duration> {
    v.and_then(Value::as_f64)
        .filter(|f| f.is_finite() && *f >= 0.0)
        .map(|f| Duration::from_millis(f as u64))
}

fn per_record(v: &Value) -> std::result::Result<Outcome, String> {
    match v {
        Value::String(s) if s == "ok" => Ok(Outcome::Ok),
        Value::String(s) if s == "retry" => Ok(Outcome::Retry {
            after: None,
            reason: "the script asked to retry the record".into(),
        }),
        Value::Bool(true) => Ok(Outcome::Ok),
        Value::Object(o) if o.get("ok") == Some(&Value::Bool(true)) => Ok(Outcome::Ok),
        Value::Object(o) if o.contains_key("retry") => Ok(Outcome::Retry {
            after: ms_of(o.get("retryAfterMs")),
            reason: reason_of(&o["retry"], "the script asked to retry the record"),
        }),
        Value::Object(o) if o.contains_key("fail") => Ok(Outcome::Fail {
            reason: reason_of(&o["fail"], "the script failed the record"),
        }),
        other => Err(format!("bad per-record result {other}")),
    }
}

/// Maps a run's event and fields onto outcomes (the contract above).
///
/// # Errors
/// A retryable 39220 for a malformed result.
pub fn map_result(event: &str, fields: &Value, n: usize) -> Result<BatchOutcome> {
    match event {
        "output" => {}
        "retry" => {
            return Ok(BatchOutcome::All(Outcome::Retry {
                after: ms_of(fields.get("retryAfterMs")),
                reason: format!(
                    "ER_SPRIG_SINK_SCRIPT_FAILED: the script asked to retry: {}",
                    reason_of(fields, "retry")
                ),
            }));
        }
        "permanentError" => {
            return Ok(BatchOutcome::All(Outcome::Fail {
                reason: reason_of(fields, "the script failed the batch"),
            }));
        }
        other => return Err(malformed(&format!("the event {other}"))),
    }
    let Some(o) = fields.as_object() else {
        return Err(malformed("not an object"));
    };
    if o.is_empty() || o.get("ok") == Some(&Value::Bool(true)) {
        return Ok(BatchOutcome::All(Outcome::Ok));
    }
    if o.len() == 1
        && let Some(v) = o.get("value")
    {
        return match v {
            Value::Bool(true) | Value::Null => Ok(BatchOutcome::All(Outcome::Ok)),
            other => Err(malformed(&format!("the value {other}"))),
        };
    }
    if let Some(r) = o.get("__sprig_permanent") {
        return Ok(BatchOutcome::All(Outcome::Fail {
            reason: reason_of(r, "SinkPermanentError"),
        }));
    }
    if o.get("retry") == Some(&Value::Bool(true)) {
        return Ok(BatchOutcome::All(Outcome::Retry {
            after: ms_of(o.get("retryAfterMs")),
            reason: o.get("reason").map_or_else(
                || "the script asked to retry the batch".to_owned(),
                |r| reason_of(r, "retry"),
            ),
        }));
    }
    if let Some(f) = o.get("fail") {
        return Ok(BatchOutcome::All(Outcome::Fail {
            reason: reason_of(f, "the script failed the batch"),
        }));
    }
    if let Some(Value::Array(rs)) = o.get("results") {
        if rs.len() != n {
            return Err(malformed(&format!(
                "{} results for {n} records",
                rs.len()
            )));
        }
        let v = rs
            .iter()
            .map(per_record)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| malformed(&e))?;
        return Ok(BatchOutcome::Each(v));
    }
    Err(malformed(&Value::Object(o.clone()).to_string()))
}

// ---- the target -------------------------------------------------------------

/// What a script target is built with besides its context.
#[derive(Default)]
pub struct ScriptOptions {
    /// The source of `TO SCRIPT FUNCTION f`, looked up by the job (the
    /// active version of the deployed function).
    pub source: Option<String>,
    /// The sink's destination rules for fetches without a connection
    /// (default: the first `USING CONNECTIONS` connection's, else none).
    pub dest: Option<Arc<Destinations>>,
}

/// A script sink's target.
pub struct ScriptTarget {
    program: Program,
    language: ScriptLanguage,
    sh: Arc<Shared>,
    db: Option<Arc<dyn ReadOnlyDb>>,
    db_limits: DbLimits,
    limits: Limits,
}

impl std::fmt::Debug for ScriptTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScriptTarget")
            .field("language", &self.language)
            .field("sink", &self.sh.ctx.sink.name)
            .finish_non_exhaustive()
    }
}

/// A script target for `ctx` (chapter 51's `TO SCRIPT`).
///
/// # Errors
/// 39218 for a sink that isn't a script sink or has no source; 39861 for
/// source that doesn't compile.
pub fn build(ctx: &TargetCtx, db: Option<Arc<dyn ReadOnlyDb>>) -> Result<Box<dyn Target>> {
    Ok(Box::new(ScriptTarget::new(ctx, db, ScriptOptions::default())?))
}

fn lang_label(l: ScriptLanguage) -> &'static str {
    match l {
        ScriptLanguage::Javascript => "javascript",
        ScriptLanguage::Kirun => "kirun",
        ScriptLanguage::Lua => "lua",
    }
}

impl ScriptTarget {
    /// See [`build`].
    ///
    /// # Errors
    /// See [`build`].
    pub fn new(
        ctx: &TargetCtx,
        db: Option<Arc<dyn ReadOnlyDb>>,
        o: ScriptOptions,
    ) -> Result<Self> {
        let TargetSpec::Script {
            language,
            source,
            function,
        } = &ctx.sink.target
        else {
            return Err(bad_option(format!("sink {} isn't a script sink", ctx.sink.name)));
        };
        let src = o.source.or_else(|| source.clone()).ok_or_else(|| {
            bad_option(format!(
                "sink {}: no script source{}",
                ctx.sink.name,
                function
                    .as_ref()
                    .map(|f| format!(" (function {f} not looked up)"))
                    .unwrap_or_default()
            ))
        })?;
        let program = program(*language, &format!("__sink_{}", ctx.sink.name), &src)?;
        let opts = &ctx.opts;
        let dest = o.dest.unwrap_or_else(|| {
            ctx.extra.first().map_or_else(
                || {
                    Arc::new(Destinations {
                        allow: Vec::new(),
                        residency: Box::new(Unpinned),
                        platform_hosts: None,
                        metadata_allowed: false,
                    })
                },
                |c| Arc::clone(&c.dest),
            )
        });
        let max_fetches = opts.u64("script_max_fetches", 1000)?;
        let db_limits = DbLimits {
            queries: opts.u64("script_db_max_queries", 100)?,
            rows: opts.u64("script_db_max_rows", 10_000)?,
            budget: Duration::from_millis(opts.u64("script_db_budget_ms", 5000)?),
        };
        let defaults = Limits::default();
        let limits = Limits {
            wall: Duration::from_millis(opts.u64("script_budget_ms", 30_000)?),
            max_steps: opts.u64("logic.kirun_max_steps", defaults.max_steps)?,
            memory_bytes: opts.u64("logic.memory_limit_mb", defaults.memory_bytes >> 20)? << 20,
            // Fetches and reads have their own limits; these only bound
            // them from above.
            max_data_ops: max_fetches + db_limits.queries + 64,
            max_rows_read: u64::MAX / 4,
            wall_setting: "sink.script_budget_ms",
            ..defaults
        };
        let sh = Arc::new(Shared {
            conns: ctx
                .extra
                .iter()
                .map(|c| (c.name.clone(), c.clone()))
                .collect(),
            ctx: ctx.clone(),
            dest,
            max_fetches,
            timeout: Duration::from_millis(opts.u64("http_timeout_ms", 30_000)?),
            webhooks: StdMutex::new(HashMap::new()),
        });
        Ok(Self {
            program,
            language: *language,
            sh,
            db,
            db_limits,
            limits,
        })
    }

    fn ctx_json(&self, b: &Batch) -> Value {
        let c = &self.sh.ctx;
        json!({
            "tenant": c.task.tenant,
            "sink": c.sink.name,
            "database": c.db_name,
            "task": c.task.task_no,
            "attempt": b.attempt,
            "batchId": b.id,
            "connections": c.extra.iter().map(|x| json!({"name": x.name, "kind": x.kind.as_str()})).collect::<Vec<_>>(),
        })
    }
}

#[async_trait]
impl Target for ScriptTarget {
    fn kind(&self) -> &'static str {
        "script"
    }

    async fn deliver(&self, b: &Batch) -> Result<BatchOutcome> {
        let label = lang_label(self.language);
        let bridge = Arc::new(ReadBridge {
            db: self.db.clone(),
            session: AsyncMutex::new(None),
            limits: self.db_limits,
            queries: AtomicU64::new(0),
            rows: AtomicU64::new(0),
            spent_us: AtomicU64::new(0),
            language: label,
        });
        let host = Host {
            db: Arc::clone(&bridge) as Arc<dyn DbBridge>,
            outbound: Arc::new(SinkOutbound {
                sh: Arc::clone(&self.sh),
                fetches: AtomicU64::new(0),
            }),
            default_db: None,
            info: json!({
                "tenant": self.sh.ctx.task.tenant,
                "database": self.sh.ctx.db_name,
                "function": format!("__sink_{}", self.sh.ctx.sink.name),
            }),
        };
        let batch = json!({
            "id": b.id,
            "attempt": b.attempt,
            "records": b.records.iter().map(SinkRecord::to_json).collect::<Vec<_>>(),
        });
        let started = Instant::now();
        let r = self
            .program
            .run(vec![batch, self.ctx_json(b)], host, self.limits.clone())
            .await;
        bridge.end().await;
        metrics::histogram!("sprig_sink_script_seconds", "language" => label)
            .record(started.elapsed().as_secs_f64());
        let n = b.records.len();
        match r {
            Ok(o) => map_result(&o.event, &o.result, n),
            Err(e) if e.code() == Some(SprigCode::LogicBudgetExceeded) => {
                if n > 1 {
                    Ok(BatchOutcome::TooLarge)
                } else {
                    Ok(BatchOutcome::All(Outcome::Fail {
                        reason: format!(
                            "ER_SPRIG_SINK_SCRIPT_FAILED: a single record exceeded the script's budget: {}",
                            e.message
                        ),
                    }))
                }
            }
            Err(e) => {
                let code = e.code().map_or_else(
                    || e.kind.to_string(),
                    |c| format!("{} {}", c.number(), c.name()),
                );
                let mut x = err(
                    SprigCode::SinkScriptFailed,
                    format!("the script failed ({code}): {}", e.message),
                );
                x.retryable = true;
                Err(x)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn results_map_onto_outcomes() {
        let all_ok = BatchOutcome::All(Outcome::Ok);
        assert_eq!(map_result("output", &json!({}), 2).unwrap(), all_ok);
        assert_eq!(map_result("output", &json!({"value": true}), 2).unwrap(), all_ok);
        assert_eq!(map_result("output", &json!({"ok": true}), 2).unwrap(), all_ok);
        assert_eq!(
            map_result("output", &json!({"retry": true, "retryAfterMs": 1500}), 2).unwrap(),
            BatchOutcome::All(Outcome::Retry {
                after: Some(Duration::from_millis(1500)),
                reason: "the script asked to retry the batch".into()
            })
        );
        assert!(matches!(
            map_result("output", &json!({"fail": "bad"}), 2).unwrap(),
            BatchOutcome::All(Outcome::Fail { reason }) if reason == "bad"
        ));
        let each = map_result(
            "output",
            &json!({"results": ["ok", "retry", {"retry": "busy"}, {"fail": "no"}]}),
            4,
        )
        .unwrap();
        let BatchOutcome::Each(v) = each else {
            panic!("{each:?}")
        };
        assert_eq!(v[0], Outcome::Ok);
        assert!(matches!(&v[2], Outcome::Retry { reason, .. } if reason == "busy"));
        assert!(matches!(&v[3], Outcome::Fail { reason } if reason == "no"));
        for bad in [json!({"results": ["ok"]}), json!({"value": 3}), json!({"x": 1})] {
            let e = map_result("output", &bad, 4).unwrap_err();
            assert_eq!(e.code(), Some(SprigCode::SinkScriptFailed));
            assert!(e.retryable);
        }
        assert!(matches!(
            map_result("permanentError", &json!({"message": "m"}), 1).unwrap(),
            BatchOutcome::All(Outcome::Fail { reason }) if reason == "m"
        ));
        assert!(matches!(
            map_result("retry", &json!({"retryAfterMs": 10}), 1).unwrap(),
            BatchOutcome::All(Outcome::Retry { after: Some(_), .. })
        ));
        assert!(map_result("whatever", &json!({}), 1).is_err());
    }

    #[test]
    fn only_reads_pass_the_check() {
        for ok in [
            "SELECT 1",
            "SELECT a FROM t WHERE b = 2",
            "WITH x AS (SELECT 1 AS a) SELECT a FROM x",
        ] {
            check_read_only(ok).unwrap();
        }
        for bad in [
            "UPDATE t SET a = 1",
            "DELETE FROM t",
            "INSERT INTO t VALUES (1)",
            "CALL p()",
            "SELECT 1; SELECT 2",
            "SELECT a INTO @x FROM t",
            "DROP TABLE t",
            "SET @a = 1",
        ] {
            let e = check_read_only(bad).unwrap_err();
            assert_eq!(e.code(), Some(SprigCode::SinkScriptReadOnly), "{bad}");
        }
    }

    #[test]
    fn sources_are_wrapped() {
        let s = js_source("export async function deliver(batch, ctx) { return; }");
        assert!(s.contains("await deliver(__sprig_b"));
        let s = js_source("export default async function (batch, ctx) { return; }");
        assert!(s.starts_with("const __sprig_user = async function"));
        assert!(s.contains("await __sprig_user("));
        let s = js_source("return { ok: true };");
        assert!(s.starts_with("async function __sprig_user(batch, ctx) {return"));
        for l in [ScriptLanguage::Javascript, ScriptLanguage::Lua] {
            let src = match l {
                ScriptLanguage::Javascript => "export function deliver(b, c) { return true; }",
                _ => "function deliver(b, c) return true end",
            };
            program(l, "t", src).unwrap();
        }
        assert!(program(ScriptLanguage::Javascript, "t", "export function deliver( {").is_err());
        assert!(program(ScriptLanguage::Lua, "t", "function deliver(").is_err());
        assert_eq!(percent_decode("a%20b%2Fc"), "a b/c");
        assert_eq!(hmac_hex(b"k", b"d").len(), 64);
    }
}
