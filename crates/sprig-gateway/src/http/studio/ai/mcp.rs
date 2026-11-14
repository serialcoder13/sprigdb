//! The MCP server at `/v1/mcp`: the assistants' tools for outside AI
//! agents, over Streamable HTTP (JSON-RPC 2.0). [`sprig_ai::mcp`] decides
//! which tools a caller sees and runs each call; this module is the
//! transport.
//!
//! | Request | Does |
//! |---|---|
//! | `POST /v1/mcp?role=data\|dba` | One JSON-RPC request or notification |
//! | `DELETE /v1/mcp` | Ends a session (`Mcp-Session-Id`) |
//! | `GET /v1/mcp` | 405: there are no server-initiated messages |
//!
//! **Two eras on one endpoint.**
//! - *Stateless* (revision 2026-07-28): every request carries
//!   `_meta["io.modelcontextprotocol/protocolVersion"]` and
//!   `clientCapabilities`, with the `MCP-Protocol-Version`, `Mcp-Method`
//!   and (for `tools/call`) `Mcp-Name` headers matching the body (400
//!   `-32020` otherwise; 400 `-32022` for an unknown version). Methods:
//!   `server/discover`, `tools/list`, `tools/call`, `ping`; any other is 404
//!   `-32601`. Results carry `resultType` and `serverInfo`.
//! - *Sessions* (2025-03-26 to 2025-11-25): `initialize` answers with an
//!   `Mcp-Session-Id`; later requests need it (400 without, 404 for an
//!   unknown or expired one, or another user's). Sessions live in this
//!   gateway's memory for an hour of idleness; behind a load balancer a
//!   client that lands elsewhere gets 404 and initializes again.
//!
//! **Who.** Every request signs in again (`Authorization: Bearer` with a
//! token or API key, or Basic); none is anonymous (401). It needs a
//! tenant login, `ai.enabled` and `ai.mcp.enabled` (403, 39991). An
//! `Origin` header must be this listener's own origin or one of
//! `studio.public_origins` (403).
//!
//! **Calls.** `tools/call` is limited to `ai.mcp.calls_per_minute` per
//! user, audited in a conversation per user, role, client and UTC day
//! (`MCP: <client>`, in the caller's assistant conversations), and as one
//! `ai.mcp` event on the tenant's audit stream.
//!
//! **Errors.** JSON-RPC errors carry SprigDB's error in `data` (`code`,
//! `name`, `message`, `sqlstate`). Sign-in and settings errors use
//! SprigDB's number as the JSON-RPC code and its HTTP status; a tool the
//! caller isn't offered is `-32602` with 39996 in `data`.

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::{self, HeaderMap, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use serde_json::{Value, json};
use sprig_ai::audit::{AuditStore, ConvId};
use sprig_ai::glossary::{GlossaryStore, NewTerm};
use sprig_ai::mcp::{self as core_mcp, Audit, McpEnv};
use sprig_ai::settings::{Lookup, McpSettings, SHARE_DATA};
use sprig_ai::{Proposal, Role, ToolHost, ToolRegistry};
use sprig_common::audit::AuditStream;
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{SprigCode, SprigError};
use tonic::body::Body;

use super::super::super::sql::Caller;
use super::super::super::{ConnInfo, Core};
use super::{GatewayLookup, host::GatewayHost, stores};

/// The endpoint.
pub const PATH: &str = "/v1/mcp";
/// The session header (sessions era).
pub const SESSION_HEADER: &str = "mcp-session-id";
/// The protocol version header.
pub const VERSION_HEADER: &str = "mcp-protocol-version";
/// Idle time after which a session ends.
pub const SESSION_IDLE: Duration = Duration::from_secs(3600);
/// Sessions one user may hold; the least recently used goes first.
pub const SESSIONS_PER_USER: usize = 32;
/// Sessions this gateway holds at most.
const MAX_SESSIONS: usize = 10_000;
/// Audit conversations remembered at most.
const MAX_CONVS: usize = 10_000;

const META_VERSION: &str = "io.modelcontextprotocol/protocolVersion";
const META_CAPS: &str = "io.modelcontextprotocol/clientCapabilities";
const META_CLIENT: &str = "io.modelcontextprotocol/clientInfo";
const META_SERVER: &str = "io.modelcontextprotocol/serverInfo";

// ---- what the transport needs from the gateway ----

/// The signed-in caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Who {
    /// The tenant.
    pub tenant: TenantId,
    /// The user.
    pub user: UserId,
    /// The login, for the audit stream.
    pub login: String,
}

/// A signed-in request.
pub struct Signed {
    /// Who.
    pub who: Who,
    /// The tenant's `ai.*` values for MCP.
    pub settings: McpSettings,
    /// Runs tools as the caller.
    pub env: Arc<dyn McpCaller>,
}

/// [`McpEnv`] and where the caller's calls are audited.
#[async_trait]
pub trait McpCaller: McpEnv {
    /// The caller's conversation store.
    ///
    /// # Errors
    /// The tenant catalog's.
    async fn audit_store(&self) -> Result<Arc<dyn AuditStore>, SprigError>;
}

/// Sign-in and origins: the gateway, or a test.
#[async_trait]
pub trait Backend: Send + Sync {
    /// Signs a request in from its headers (an `Authorization` header is
    /// present).
    ///
    /// # Errors
    /// 1045 and the like for bad credentials; 39792 for a login without a
    /// tenant.
    async fn sign_in(&self, headers: &HeaderMap) -> Result<Signed, SprigError>;
    /// Whether a request with `Origin: origin` and `Host: host` may call.
    fn origin_ok(&self, origin: &str, host: Option<&str>) -> bool;
}

/// `origin` is the listener's own (`scheme://host`) or listed.
#[must_use]
pub fn origin_allowed(origin: &str, host: Option<&str>, tls: bool, listed: &[String]) -> bool {
    let own = host.map(|h| format!("{}://{h}", if tls { "https" } else { "http" }));
    own.as_deref() == Some(origin) || listed.iter().any(|p| p == origin)
}

// ---- state ----

struct Session {
    who: Who,
    role: Role,
    client: String,
    last: Instant,
}

type ConvKey = (TenantId, UserId, Role, String, u64);

/// Sessions, audit conversations and call counts.
#[derive(Default)]
pub struct McpState {
    sessions: Mutex<HashMap<String, Session>>,
    convs: Mutex<HashMap<ConvKey, ConvId>>,
    calls: Mutex<HashMap<(TenantId, UserId), (Instant, u64)>>,
}

impl std::fmt::Debug for McpState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpState").finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl McpState {
    fn open(&self, who: &Who, role: Role, client: &str) -> Result<String, SprigError> {
        let mut b = [0u8; 32];
        getrandom::fill(&mut b)
            .map_err(|e| SprigError::internal(format!("no random session id: {e}")))?;
        let id: String = b.iter().map(|x| format!("{x:02x}")).collect();
        let mut s = lock(&self.sessions);
        let now = Instant::now();
        s.retain(|_, v| now.duration_since(v.last) < SESSION_IDLE);
        let mut mine: Vec<(String, Instant)> = s
            .iter()
            .filter(|(_, v)| v.who.tenant == who.tenant && v.who.user == who.user)
            .map(|(k, v)| (k.clone(), v.last))
            .collect();
        mine.sort_by_key(|(_, t)| *t);
        while mine.len() >= SESSIONS_PER_USER {
            let (k, _) = mine.remove(0);
            s.remove(&k);
        }
        if s.len() >= MAX_SESSIONS {
            return Err(SprigError::new(
                SprigCode::RateLimited,
                "this gateway holds too many MCP sessions; try again later",
            ));
        }
        s.insert(
            id.clone(),
            Session {
                who: who.clone(),
                role,
                client: client.to_owned(),
                last: now,
            },
        );
        Ok(id)
    }

    /// The session `id` when it is `who`'s and alive: its role and client.
    fn touch(&self, id: &str, who: &Who) -> Option<(Role, String)> {
        let mut s = lock(&self.sessions);
        let now = Instant::now();
        let v = s.get_mut(id)?;
        if now.duration_since(v.last) >= SESSION_IDLE {
            s.remove(id);
            return None;
        }
        if v.who.tenant != who.tenant || v.who.user != who.user {
            return None;
        }
        v.last = now;
        Some((v.role, v.client.clone()))
    }

    fn close(&self, id: &str, who: &Who) -> bool {
        let mut s = lock(&self.sessions);
        match s.get(id) {
            Some(v) if v.who.tenant == who.tenant && v.who.user == who.user => {
                s.remove(id);
                true
            }
            _ => false,
        }
    }

    /// Counts a call; `Err(seconds to wait)` past `limit` a minute.
    fn count(&self, who: &Who, limit: u64) -> Result<(), u64> {
        let mut c = lock(&self.calls);
        let now = Instant::now();
        if c.len() > MAX_SESSIONS {
            c.retain(|_, (t, _)| now.duration_since(*t) < Duration::from_secs(60));
        }
        let e = c.entry((who.tenant, who.user)).or_insert((now, 0));
        if now.duration_since(e.0) >= Duration::from_secs(60) {
            *e = (now, 0);
        }
        if e.1 >= limit {
            return Err(60u64
                .saturating_sub(now.duration_since(e.0).as_secs())
                .max(1));
        }
        e.1 += 1;
        Ok(())
    }

    async fn audit(
        &self,
        env: &dyn McpCaller,
        who: &Who,
        role: Role,
        client: &str,
    ) -> Result<Audit, SprigError> {
        let day = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() / 86_400);
        let key = (who.tenant, who.user, role, client.to_owned(), day);
        let store = env.audit_store().await?;
        if let Some(c) = lock(&self.convs).get(&key) {
            return Ok(Audit { store, conv: *c });
        }
        let conv = store
            .create(who.user, role, &format!("MCP: {client}"))
            .await?;
        let mut m = lock(&self.convs);
        if m.len() >= MAX_CONVS {
            m.retain(|k, _| k.4 == day);
            if m.len() >= MAX_CONVS {
                m.clear();
            }
        }
        let conv = *m.entry(key).or_insert(conv);
        Ok(Audit { store, conv })
    }
}

/// This process's state.
pub fn state() -> &'static McpState {
    static STATE: OnceLock<McpState> = OnceLock::new();
    STATE.get_or_init(McpState::default)
}

// ---- replies ----

/// A failure: its HTTP status and JSON-RPC error.
#[derive(Debug)]
struct Fail {
    status: u16,
    code: i64,
    message: String,
    data: Option<Value>,
    challenge: bool,
}

impl Fail {
    fn rpc(status: u16, code: i64, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            data: None,
            challenge: false,
        }
    }

    fn with(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }

    /// SprigDB's error, at its own status and number.
    fn sprig(e: &SprigError) -> Self {
        let (status, body) = sprig_api::status::to_rest(e);
        Self {
            status,
            code: i64::from(body.code),
            message: e.message.clone(),
            data: serde_json::to_value(&body).ok(),
            challenge: status == 401,
        }
    }

    /// SprigDB's error as `code` at `status`.
    fn sprig_as(status: u16, code: i64, e: &SprigError) -> Self {
        let mut f = Self::sprig(e);
        f.status = status;
        f.code = code;
        f.challenge = false;
        f
    }

    fn body(&self, id: Option<&Value>) -> Value {
        let mut err = json!({"code": self.code, "message": self.message});
        if let Some(d) = &self.data {
            err["data"] = d.clone();
        }
        let mut v = json!({"jsonrpc": "2.0", "error": err});
        if let Some(id) = id {
            v["id"] = id.clone();
        }
        v
    }
}

fn invalid(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::AdminRequestInvalid, m)
}

fn json_reply(status: u16, v: &Value, sse: bool) -> Response<Body> {
    let (bytes, ctype) = if sse && status == 200 {
        (
            format!("event: message\ndata: {v}\n\n").into_bytes(),
            "text/event-stream",
        )
    } else {
        (
            serde_json::to_vec(v).unwrap_or_default(),
            "application/json",
        )
    };
    let mut r = Response::new(Body::new(Full::new(Bytes::from(bytes))));
    *r.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
    let h = r.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static(ctype));
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if sse {
        h.insert("x-accel-buffering", HeaderValue::from_static("no"));
    }
    r
}

fn empty(status: StatusCode) -> Response<Body> {
    let mut r = Response::new(Body::new(Full::new(Bytes::new())));
    *r.status_mut() = status;
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

fn fail_reply(f: &Fail, id: Option<&Value>) -> Response<Body> {
    let mut r = json_reply(f.status, &f.body(id), false);
    if f.challenge {
        r.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static(super::super::super::auth::WWW_AUTHENTICATE),
        );
    }
    r
}

fn get<'a>(h: &'a HeaderMap, name: &str) -> Option<&'a str> {
    h.get(name).and_then(|v| v.to_str().ok())
}

fn query_value<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == key).then_some(v)
    })
}

/// A header value, decoded when it uses the `=?base64?…?=` form.
fn header_text(v: &str) -> Option<String> {
    match v
        .strip_prefix("=?base64?")
        .and_then(|r| r.strip_suffix("?="))
    {
        Some(b) => base64::engine::general_purpose::STANDARD
            .decode(b)
            .ok()
            .and_then(|b| String::from_utf8(b).ok()),
        None => Some(v.to_owned()),
    }
}

/// A client's name, as audit titles show it.
fn client_name(info: Option<&Value>) -> String {
    let n: String = info
        .and_then(|i| i.get("name"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .chars()
        .filter(|c| !c.is_control())
        .take(64)
        .collect();
    let n = n.trim();
    if n.is_empty() {
        "client".to_owned()
    } else {
        n.to_owned()
    }
}

// ---- serving ----

/// One HTTP request to the endpoint.
#[derive(Debug)]
pub struct McpRequest<'a> {
    /// GET, POST, DELETE, ...
    pub method: &'a Method,
    /// The query string.
    pub query: &'a str,
    /// The headers.
    pub headers: &'a HeaderMap,
    /// The body (POST).
    pub body: Bytes,
}

/// What one request was, for `sprig_ai_mcp_requests_total`.
struct Counted {
    method: String,
    result: &'static str,
}

/// Answers one request.
pub async fn serve(b: &dyn Backend, st: &McpState, r: McpRequest<'_>) -> Response<Body> {
    let mut c = Counted {
        method: "other".to_owned(),
        result: "refused",
    };
    let resp = serve_counted(b, st, r, &mut c).await;
    sprig_ai::metrics::mcp_request(&c.method, c.result);
    resp
}

/// Where a request is in the protocol.
enum Era {
    /// Stateless, per-request metadata.
    Modern,
    /// Sessions; `None` for `initialize`.
    Legacy(Option<(Role, String)>),
}

#[allow(clippy::too_many_lines)] // the checks in the module docs' order
async fn serve_counted(
    b: &dyn Backend,
    st: &McpState,
    r: McpRequest<'_>,
    c: &mut Counted,
) -> Response<Body> {
    let h = r.headers;
    // 1. Origin, before anything else.
    if let Some(o) = get(h, "origin")
        && !b.origin_ok(o, get(h, "host"))
    {
        let e = SprigError::new(
            SprigCode::StudioOriginRejected,
            format!("origin {o} may not call the MCP server"),
        );
        let mut f = Fail::sprig(&e);
        f.status = 403;
        return fail_reply(&f, None);
    }
    // 2. The HTTP method.
    match *r.method {
        Method::POST => {}
        Method::DELETE => {
            c.method = "delete".to_owned();
            return delete(b, st, h, c).await;
        }
        _ => {
            let mut resp = empty(StatusCode::METHOD_NOT_ALLOWED);
            resp.headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("POST, DELETE"));
            return resp;
        }
    }
    // 3. One JSON-RPC message.
    let msg: Value = match serde_json::from_slice(&r.body) {
        Ok(v) => v,
        Err(e) => return fail_reply(&Fail::rpc(400, -32700, format!("parse error: {e}")), None),
    };
    let Some(m) = msg.as_object() else {
        return fail_reply(
            &Fail::rpc(
                400,
                -32600,
                "send one JSON-RPC message per request; batches aren't supported",
            ),
            None,
        );
    };
    let id = m.get("id").filter(|v| !v.is_null());
    let method = m.get("method").and_then(Value::as_str);
    if m.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || (method.is_none() && !(m.contains_key("result") || m.contains_key("error")))
    {
        return fail_reply(
            &Fail::rpc(400, -32600, "not a JSON-RPC 2.0 request or notification"),
            id,
        );
    }
    let Some(method) = method else {
        // A response: this server sends no requests, so there is nothing
        // to match it to.
        c.result = "ok";
        return empty(StatusCode::ACCEPTED);
    };
    c.method = if id.is_none() {
        "notification".to_owned()
    } else {
        method.to_owned()
    };
    let params = m.get("params").cloned().unwrap_or(Value::Null);
    // 4. Sign in: never anonymous.
    if get(h, "authorization").is_none_or(|a| a.trim().is_empty()) {
        let e = SprigError::standard(
            1045,
            *b"28000",
            "Access denied: the MCP server needs Authorization: Bearer <token or API key>, or Basic",
        );
        return fail_reply(&Fail::sprig(&e), id);
    }
    let signed = match b.sign_in(h).await {
        Ok(s) => s,
        Err(e) => return fail_reply(&Fail::sprig(&e), id),
    };
    // 5. Turned on for the tenant.
    if let Err(e) = signed.settings.check() {
        return fail_reply(&Fail::sprig(&e), id);
    }
    // 6. Notifications need nothing back.
    let Some(id) = id else {
        c.result = "ok";
        return empty(StatusCode::ACCEPTED);
    };
    // 7. The era, and its checks.
    let hv = get(h, VERSION_HEADER);
    let meta = params.get("_meta");
    let meta_version = meta
        .and_then(|m| m.get(META_VERSION))
        .and_then(Value::as_str);
    let era = if method == "initialize" {
        Era::Legacy(None)
    } else if meta_version.is_some() || hv == Some(core_mcp::MODERN_VERSION) {
        if let Err(f) = modern_checks(h, method, &params) {
            return fail_reply(&f, Some(id));
        }
        Era::Modern
    } else {
        let v = hv.unwrap_or(core_mcp::ASSUMED_VERSION);
        if !core_mcp::is_legacy(v) {
            return fail_reply(
                &Fail::rpc(
                    400,
                    -32600,
                    format!(
                        "unsupported MCP-Protocol-Version {v}; this server speaks {}",
                        core_mcp::supported_versions().join(", ")
                    ),
                ),
                Some(id),
            );
        }
        let Some(sid) = get(h, SESSION_HEADER) else {
            return fail_reply(
                &Fail::rpc(
                    400,
                    -32600,
                    "the Mcp-Session-Id header is missing: send initialize first",
                ),
                Some(id),
            );
        };
        match st.touch(sid, &signed.who) {
            Some(s) => Era::Legacy(Some(s)),
            None => {
                return fail_reply(
                    &Fail::rpc(
                        404,
                        -32600,
                        "no such MCP session (it ended or expired): initialize again",
                    ),
                    Some(id),
                );
            }
        }
    };
    let modern = matches!(era, Era::Modern);
    let sse = get(h, "accept").is_some_and(|a| {
        a.contains("text/event-stream") && !a.contains("application/json") && !a.contains("*/*")
    });
    // 8. The role and the client: the session's, or this request's.
    let (role, client) = match &era {
        Era::Legacy(Some((role, client))) => (*role, client.clone()),
        _ => {
            let role = match core_mcp::role(query_value(r.query, "role")) {
                Ok(r) => r,
                Err(m) => return fail_reply(&Fail::sprig(&invalid(m)), Some(id)),
            };
            let info = if modern {
                meta.and_then(|m| m.get(META_CLIENT))
            } else {
                params.get("clientInfo")
            };
            (role, client_name(info))
        }
    };
    let ctx = Ctx {
        st,
        signed: &signed,
        role,
        client: &client,
        modern,
    };
    let out = match era {
        Era::Legacy(None) => initialize(&ctx, &params),
        _ => dispatch(&ctx, method, &params).await,
    };
    match out {
        Ok((mut result, session)) => {
            if modern {
                result["resultType"] = json!("complete");
                result["_meta"][META_SERVER] = core_mcp::server_info();
            }
            c.result = "ok";
            let mut resp = json_reply(
                200,
                &json!({"jsonrpc": "2.0", "id": id, "result": result}),
                sse,
            );
            if let Some(s) = session
                && let Ok(v) = HeaderValue::from_str(&s)
            {
                resp.headers_mut().insert(SESSION_HEADER, v);
            }
            resp
        }
        Err(f) => {
            c.result = "error";
            fail_reply(&f, Some(id))
        }
    }
}

/// The stateless era's checks: the headers match the body, the version is
/// served, the required metadata is there.
fn modern_checks(h: &HeaderMap, method: &str, params: &Value) -> Result<(), Fail> {
    let meta = params.get("_meta");
    let Some(v) = meta
        .and_then(|m| m.get(META_VERSION))
        .and_then(Value::as_str)
    else {
        return Err(Fail::rpc(
            400,
            -32602,
            format!("params._meta[\"{META_VERSION}\"] is required"),
        ));
    };
    let Some(hv) = get(h, VERSION_HEADER) else {
        return Err(Fail::rpc(
            400,
            -32020,
            "the MCP-Protocol-Version header is missing",
        ));
    };
    if hv != v {
        return Err(Fail::rpc(
            400,
            -32020,
            format!("header mismatch: MCP-Protocol-Version {hv} but _meta says {v}"),
        ));
    }
    if v != core_mcp::MODERN_VERSION {
        return Err(
            Fail::rpc(400, -32022, "unsupported protocol version").with(json!({
                "supported": [core_mcp::MODERN_VERSION],
                "requested": v,
            })),
        );
    }
    if !meta
        .and_then(|m| m.get(META_CAPS))
        .is_some_and(Value::is_object)
    {
        return Err(Fail::rpc(
            400,
            -32602,
            format!("params._meta[\"{META_CAPS}\"] is required"),
        ));
    }
    match get(h, "mcp-method") {
        Some(m) if m == method => {}
        Some(m) => {
            return Err(Fail::rpc(
                400,
                -32020,
                format!("header mismatch: Mcp-Method {m} but the body's method is {method}"),
            ));
        }
        None => return Err(Fail::rpc(400, -32020, "the Mcp-Method header is missing")),
    }
    if method == "tools/call" {
        let name = params.get("name").and_then(Value::as_str).unwrap_or("");
        match get(h, "mcp-name").map(header_text) {
            Some(Some(n)) if n == name => {}
            Some(_) => {
                return Err(Fail::rpc(
                    400,
                    -32020,
                    format!("header mismatch: Mcp-Name doesn't match the tool name {name}"),
                ));
            }
            None => return Err(Fail::rpc(400, -32020, "the Mcp-Name header is missing")),
        }
    }
    Ok(())
}

struct Ctx<'a> {
    st: &'a McpState,
    signed: &'a Signed,
    role: Role,
    client: &'a str,
    modern: bool,
}

impl Ctx<'_> {
    fn tools(&self) -> Vec<Arc<sprig_ai::ToolSpec>> {
        core_mcp::exposed(
            &ToolRegistry::builtin(),
            self.role,
            &core_mcp::AllowList::parse(&self.signed.settings.tools),
        )
    }

    /// A request-level error: 400 in the stateless era, else in a 200.
    fn bad(&self, code: i64, m: impl Into<String>) -> Fail {
        Fail::rpc(if self.modern { 400 } else { 200 }, code, m)
    }
}

type Answer = Result<(Value, Option<String>), Fail>;

fn initialize(ctx: &Ctx<'_>, params: &Value) -> Answer {
    let Some(asked) = params.get("protocolVersion").and_then(Value::as_str) else {
        return Err(Fail::rpc(
            200,
            -32602,
            "initialize needs params.protocolVersion",
        ));
    };
    let version = core_mcp::negotiate(asked);
    let sid = ctx
        .st
        .open(&ctx.signed.who, ctx.role, ctx.client)
        .map_err(|e| Fail::sprig(&e))?;
    Ok((
        json!({
            "protocolVersion": version,
            "capabilities": {"tools": {"listChanged": false}},
            "serverInfo": core_mcp::server_info(),
            "instructions": core_mcp::instructions(ctx.role),
        }),
        Some(sid),
    ))
}

async fn dispatch(ctx: &Ctx<'_>, method: &str, params: &Value) -> Answer {
    match method {
        "ping" => Ok((json!({}), None)),
        "server/discover" if ctx.modern => Ok((
            json!({
                "supportedVersions": [core_mcp::MODERN_VERSION],
                "capabilities": {"tools": {"listChanged": false}},
                "instructions": core_mcp::instructions(ctx.role),
            }),
            None,
        )),
        "tools/list" => {
            let tools: Vec<Value> = ctx.tools().iter().map(|s| core_mcp::tool_def(s)).collect();
            Ok((json!({"tools": tools}), None))
        }
        "tools/call" => call(ctx, params).await.map(|v| (v, None)),
        _ => Err(Fail::rpc(
            if ctx.modern { 404 } else { 200 },
            -32601,
            format!("method not found: {method}"),
        )),
    }
}

async fn call(ctx: &Ctx<'_>, params: &Value) -> Result<Value, Fail> {
    let Some(name) = params.get("name").and_then(Value::as_str) else {
        return Err(ctx.bad(-32602, "tools/call needs params.name"));
    };
    let args = params.get("arguments");
    if args.is_some_and(|a| !a.is_object() && !a.is_null()) {
        return Err(ctx.bad(-32602, "params.arguments is an object"));
    }
    let tools = ctx.tools();
    let who = &ctx.signed.who;
    if !tools.iter().any(|s| s.name == name) {
        // Refused before anything is counted or audited.
        let e = core_mcp::call(&*ctx.signed.env, ctx.role, &tools, name, None, None)
            .await
            .err()
            .unwrap_or_else(|| sprig_ai::error::tool_denied(name.to_owned()));
        return Err(Fail::sprig_as(
            if ctx.modern { 400 } else { 200 },
            -32602,
            &e,
        ));
    }
    if let Err(wait) = ctx.st.count(who, ctx.signed.settings.calls_per_minute) {
        let tool = tools
            .iter()
            .find(|s| s.name == name)
            .map_or("other", |s| s.name);
        sprig_ai::metrics::mcp_tool_call(ctx.role, tool, "limited");
        return Ok(core_mcp::error_result(&format!(
            "error 39780: more than ai.mcp.calls_per_minute ({}) tool calls this minute; wait {wait}s",
            ctx.signed.settings.calls_per_minute
        )));
    }
    let audit = ctx
        .st
        .audit(&*ctx.signed.env, who, ctx.role, ctx.client)
        .await
        .map_err(|e| Fail::sprig_as(500, -32603, &e))?;
    let called = core_mcp::call(&*ctx.signed.env, ctx.role, &tools, name, args, Some(&audit))
        .await
        .map_err(|e| Fail::sprig_as(if ctx.modern { 400 } else { 200 }, -32602, &e))?;
    sprig_common::audit::record(
        AuditStream::Tenant(who.tenant),
        "ai.mcp",
        json!({
            "user": who.login, "user_id": who.user.0, "role": ctx.role.as_str(),
            "client": ctx.client, "tool": name, "result": called.outcome,
            "reduced": called.reduced, "conversation": audit.conv.0,
            "proposal": called.proposal.as_ref().map(|p| p.id.clone()),
        }),
    );
    Ok(called.result)
}

async fn delete(b: &dyn Backend, st: &McpState, h: &HeaderMap, c: &mut Counted) -> Response<Body> {
    if get(h, "authorization").is_none_or(|a| a.trim().is_empty()) {
        let e = SprigError::standard(1045, *b"28000", "Access denied: sign in to end a session");
        return fail_reply(&Fail::sprig(&e), None);
    }
    let signed = match b.sign_in(h).await {
        Ok(s) => s,
        Err(e) => return fail_reply(&Fail::sprig(&e), None),
    };
    let Some(sid) = get(h, SESSION_HEADER) else {
        return fail_reply(
            &Fail::rpc(400, -32600, "the Mcp-Session-Id header is missing"),
            None,
        );
    };
    if st.close(sid, &signed.who) {
        c.result = "ok";
        empty(StatusCode::NO_CONTENT)
    } else {
        fail_reply(&Fail::rpc(404, -32600, "no such MCP session"), None)
    }
}

// ---- the gateway's side ----

struct CoreBackend<'a> {
    core: &'a Arc<Core>,
    info: &'a ConnInfo,
}

#[async_trait]
impl Backend for CoreBackend<'_> {
    async fn sign_in(&self, h: &HeaderMap) -> Result<Signed, SprigError> {
        let s = |n: &str| get(h, n).map(str::to_owned);
        let caller = self
            .core
            .caller(
                self.info,
                s("authorization"),
                s(super::super::super::rest::TENANT_HEADER),
                s("host"),
            )
            .await?;
        let (Some(tenant), Some(user)) = (caller.principal.tenant, caller.principal.user_id) else {
            return Err(invalid("the MCP server needs a tenant login"));
        };
        let settings = McpSettings::read(&GatewayLookup {
            st: &self.core.studio,
            tenant: Some(tenant),
            db: None,
        });
        Ok(Signed {
            who: Who {
                tenant,
                user,
                login: caller.principal.user.clone(),
            },
            settings,
            env: Arc::new(CoreEnv {
                core: Arc::clone(self.core),
                caller,
            }),
        })
    }

    fn origin_ok(&self, origin: &str, host: Option<&str>) -> bool {
        origin_allowed(
            origin,
            host,
            self.info.tls.is_some(),
            &self.core.studio.config().public_origins,
        )
    }
}

/// Tools as one caller, through the gateway.
struct CoreEnv {
    core: Arc<Core>,
    caller: Caller,
}

#[async_trait]
impl McpEnv for CoreEnv {
    fn share_data(&self, db: Option<&str>) -> bool {
        GatewayLookup {
            st: &self.core.studio,
            tenant: self.caller.principal.tenant,
            db,
        }
        .flag(&SHARE_DATA)
    }

    fn host(&self, share_data: bool) -> Arc<dyn ToolHost> {
        Arc::new(GatewayHost::new(
            Arc::clone(&self.core),
            self.caller.clone(),
            share_data,
        ))
    }

    async fn file_glossary(&self, p: &Proposal) -> sprig_common::Result<()> {
        let st = stores(&self.core, &self.caller).await?;
        let t = NewTerm::from_json(&p.body)?;
        st.glossary
            .propose(&t, st.user, &self.caller.principal.user, Some(&p.id))
            .await
    }
}

#[async_trait]
impl McpCaller for CoreEnv {
    async fn audit_store(&self) -> Result<Arc<dyn AuditStore>, SprigError> {
        Ok(stores(&self.core, &self.caller).await?.audit as Arc<dyn AuditStore>)
    }
}

/// Answers `/v1/mcp`.
pub(in crate::http::studio) async fn route(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Response<Body> {
    let method = req.method().clone();
    let query = req.uri().query().unwrap_or("").to_owned();
    let headers = req.headers().clone();
    let body = if method == Method::POST {
        match super::super::read_body(req).await {
            Ok(b) => b,
            Err(e) => return fail_reply(&Fail::sprig(&e), None),
        }
    } else {
        Bytes::new()
    };
    serve(
        &CoreBackend { core, info },
        state(),
        McpRequest {
            method: &method,
            query: &query,
            headers: &headers,
            body,
        },
    )
    .await
}
