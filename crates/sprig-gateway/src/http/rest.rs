//! The REST router (chapter 37, "REST transcoding"): requests are matched
//! against [`sprig_api::rest::ROUTES`] and run the same implementation as
//! the gRPC services.
//!
//! - `Sql.Execute`: one JSON reply, up to `api.rest.max_buffered_bytes`.
//! - `Sql.Query`: NDJSON with `Accept: application/x-ndjson` (one line per
//!   message; an error ends the stream with `{"error": {...}}`), otherwise
//!   one JSON array of the messages.
//! - `Transactions.*`: handles.
//! - `Gateways.*`: this gateway; every other `/admin/v1/` route is
//!   forwarded to the controllers.
//! - `GET /v1/openapi.json`, `GET /admin/v1/openapi.json`.
//!
//! Errors are SprigDB's REST error JSON with the status of chapter 37's
//! table ([`sprig_api::status`]); 401 carries `WWW-Authenticate`, 429 and
//! 503 carry `Retry-After`.

use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt as _;
use http_body_util::{BodyExt, Full, Limited, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::header::{self, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use serde_json::{Value, json};
use sprig_api::proto::internal::{ForwardReply, ForwardRequest};
use sprig_api::rest::{self as routes, Matched};
use sprig_api::results;
use sprig_api::status::{self, ErrorBody};
use sprig_common::{SprigCode, SprigError};
use tokio::sync::mpsc;
use tonic::body::Body;

use super::sink::Item;
use super::sql::{CHANNEL_DEPTH, Caller, SqlCall, collect};
use super::{ConnInfo, Core};

/// The `x-sprig-tenant` header (chapter 37, "Authentication").
pub const TENANT_HEADER: &str = "x-sprig-tenant";
/// Set on calls forwarded between gateways.
pub const FORWARDED_HEADER: &str = "x-sprig-forwarded";

fn json_response(status: u16, v: &Value) -> Response<Body> {
    let body = Bytes::from(serde_json::to_vec(v).unwrap_or_default());
    bytes_response(status, body)
}

fn bytes_response(status: u16, body: Bytes) -> Response<Body> {
    let mut r = Response::new(Body::new(Full::new(body)));
    *r.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    r
}

/// The REST error response for `e`.
#[must_use]
pub fn error_response(e: &SprigError) -> Response<Body> {
    let (code, body) = status::to_rest(e);
    let mut r = json_response(code, &serde_json::to_value(&body).unwrap_or_default());
    let h = r.headers_mut();
    if code == 401 {
        h.insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static(super::auth::WWW_AUTHENTICATE),
        );
    }
    if matches!(code, 429 | 503) {
        h.insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    }
    r
}

fn header(req: &Request<Incoming>, name: &str) -> Option<String> {
    req.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

async fn read_body(req: Request<Incoming>, max: usize) -> Result<Bytes, SprigError> {
    let limited = Limited::new(req.into_body(), max);
    match limited.collect().await {
        Ok(c) => Ok(c.to_bytes()),
        Err(e) if e.is::<http_body_util::LengthLimitError>() => Err(SprigError::new(
            SprigCode::RequestTooLarge,
            format!("the request body is over api.max_request_bytes ({max})"),
        )),
        Err(e) => Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            format!("can't read the request body: {e}"),
        )),
    }
}

fn parse_json(body: &[u8]) -> Result<Value, SprigError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(json!({}));
    }
    serde_json::from_slice(body).map_err(|e| {
        SprigError::new(
            SprigCode::AdminRequestInvalid,
            format!("the body isn't JSON: {e}"),
        )
    })
}

fn s(v: &Value, k: &str) -> String {
    v.get(k).and_then(Value::as_str).unwrap_or("").to_owned()
}

fn b(v: &Value, k: &str) -> bool {
    v.get(k).and_then(Value::as_bool).unwrap_or(false)
}

/// Handles one REST request.
pub async fn handle(core: &Arc<Core>, info: &ConnInfo, req: Request<Incoming>) -> Response<Body> {
    let path = req.uri().path().to_owned();
    let method = req.method().clone();
    if method == Method::GET && path == "/v1/openapi.json" {
        return json_response(200, &routes::openapi(false));
    }
    if method == Method::GET && path == "/admin/v1/openapi.json" {
        return json_response(200, &routes::openapi(true));
    }
    let Some(m) = routes::find(method.as_str(), &path) else {
        let (status_code, msg) = if routes::path_known(&path) {
            (405, format!("{method} is not allowed on {path}"))
        } else {
            (404, format!("no route {method} {path}"))
        };
        let e = SprigError::new(SprigCode::AdminRequestInvalid, msg);
        let (_, body) = status::to_rest(&e);
        return json_response(status_code, &serde_json::to_value(body).unwrap_or_default());
    };
    let rpc = m.route.short_name();
    metrics::counter!("sprig_api_requests_total", "service" => m.route.service, "method" => m.route.rpc, "protocol" => "rest").increment(1);
    let r = if m.route.is_admin() {
        admin(core, m, req).await
    } else {
        data(core, info, &rpc, m, req).await
    };
    r.unwrap_or_else(|e| error_response(&e))
}

async fn admin(
    core: &Arc<Core>,
    m: Matched,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let auth = header(&req, "authorization");
    if m.route.service == "sprig.admin.v1.Gateways" {
        let v = core
            .admin
            .gateways(
                m.route.rpc,
                m.vars.get("name").map(String::as_str),
                auth.as_deref(),
            )
            .await?;
        return Ok(json_response(200, &v));
    }
    let method = req.method().clone();
    let pq = req
        .uri()
        .path_and_query()
        .map_or_else(|| req.uri().path().to_owned(), |p| p.as_str().to_owned());
    let max = core.engine.settings().max_request;
    let body = read_body(req, max).await?;
    let r = core
        .admin
        .forward(&method, &pq, auth.as_deref(), body)
        .await?;
    Ok(bytes_response(r.status, r.body))
}

async fn data(
    core: &Arc<Core>,
    info: &ConnInfo,
    rpc: &str,
    m: Matched,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let auth = header(&req, "authorization");
    let tenant = header(&req, TENANT_HEADER);
    let host = header(&req, "host");
    let ndjson = header(&req, "accept").is_some_and(|a| a.contains("application/x-ndjson"));
    let api = core.engine.settings();
    let body = parse_json(&read_body(req, api.max_request).await?)?;
    let caller = core.caller(info, auth, tenant, host).await?;
    let transaction = m
        .vars
        .get("transaction")
        .cloned()
        .unwrap_or_else(|| s(&body, "transaction"));
    // A handle owned by another gateway: forward the whole call.
    if !transaction.is_empty()
        && let Some(reply) = forward_if_remote(core, &caller, rpc, &m, &body, &transaction).await?
    {
        return Ok(bytes_response(
            u16::try_from(reply.http_status).unwrap_or(500),
            Bytes::from(reply.body_json),
        ));
    }
    let (status_code, v) = run_local(core, &caller, rpc, &m, &body, ndjson).await?;
    match v {
        Local::Json(v) => Ok(json_response(status_code, &v)),
        Local::Stream(resp) => Ok(resp),
    }
}

enum Local {
    Json(Value),
    Stream(Response<Body>),
}

/// The call's request as proto3 JSON, with its path variables merged in.
fn request_json(m: &Matched, body: &Value) -> Value {
    let mut v = body.clone();
    if !v.is_object() {
        v = json!({});
    }
    for (k, val) in &m.vars {
        v[k] = Value::String(val.clone());
    }
    v
}

async fn forward_if_remote(
    core: &Arc<Core>,
    caller: &Caller,
    rpc: &str,
    m: &Matched,
    body: &Value,
    transaction: &str,
) -> Result<Option<ForwardReply>, SprigError> {
    let h = sprig_api::handle::TxnHandle::parse(transaction)?;
    if h.gateway_id == core.engine.gateway_id {
        return Ok(None);
    }
    let peers = core.engine.peers.as_ref().ok_or_else(|| {
        SprigError::new(
            SprigCode::ForwardFailed,
            format!(
                "the transaction is on gateway {}, and this gateway has no peer service",
                h.gateway_id
            ),
        )
    })?;
    metrics::counter!("sprig_api_txn_forwarded_total").increment(1);
    let req = ForwardRequest {
        authorization: caller.authorization.clone().unwrap_or_default(),
        method: rpc.to_owned(),
        request_json: request_json(m, body).to_string(),
        client_addr: caller.peer.to_string(),
    };
    Ok(Some(peers.forward(&h.gateway_id, req).await?))
}

/// Runs a data call here; `(status, reply)`.
async fn run_local(
    core: &Arc<Core>,
    caller: &Caller,
    rpc: &str,
    m: &Matched,
    body: &Value,
    ndjson: bool,
) -> Result<(u16, Local), SprigError> {
    let engine = &core.engine;
    let api = engine.settings();
    let database = m
        .vars
        .get("database")
        .cloned()
        .unwrap_or_else(|| s(body, "database"));
    let transaction = m
        .vars
        .get("transaction")
        .cloned()
        .unwrap_or_else(|| s(body, "transaction"));
    match rpc {
        "Sql.Execute" | "Sql.Query" => {
            let call = SqlCall {
                database,
                sql: s(body, "sql"),
                multi: b(body, "multi_statements"),
            };
            let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);
            let e2 = Arc::clone(engine);
            let c2 = caller.clone();
            let txn = (!transaction.is_empty()).then_some(transaction);
            tokio::spawn(async move { e2.run(&c2, call, txn.as_deref(), tx).await });
            if rpc == "Sql.Query" && ndjson {
                return Ok((200, Local::Stream(ndjson_response(rx))));
            }
            let reply = collect(rx, api.max_buffered).await?;
            if rpc == "Sql.Query" {
                let parts = split_parts(&reply);
                return Ok((200, Local::Json(Value::Array(parts))));
            }
            Ok((200, Local::Json(results::execute_json(&reply))))
        }
        "Transactions.Begin" => {
            let t = engine
                .begin(caller, &database, b(body, "read_only"))
                .await?;
            Ok((200, Local::Json(results::transaction_json(&t))))
        }
        "Transactions.Commit" | "Transactions.Rollback" => {
            engine
                .end(caller, &transaction, rpc == "Transactions.Commit")
                .await?;
            Ok((200, Local::Json(json!({}))))
        }
        _ => Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            format!("{rpc} has no REST form"),
        )),
    }
}

/// `Sql.Query`'s messages for a buffered reply.
fn split_parts(r: &sprig_api::proto::v1::ExecuteResponse) -> Vec<Value> {
    use sprig_api::proto::v1::{
        QueryResponse, ResultEnd, ResultHeader, RowBatch, query_response::Part,
    };
    let mut out = Vec::new();
    let n = r.results.len();
    for (i, res) in r.results.iter().enumerate() {
        let index = u32::try_from(i).unwrap_or(u32::MAX);
        if !res.columns.is_empty() {
            out.push(results::query_part_json(&QueryResponse {
                part: Some(Part::Header(ResultHeader {
                    index,
                    columns: res.columns.clone(),
                })),
            }));
            if !res.rows.is_empty() {
                out.push(results::query_part_json(&QueryResponse {
                    part: Some(Part::Rows(RowBatch {
                        rows: res.rows.clone(),
                    })),
                }));
            }
        }
        out.push(results::query_part_json(&QueryResponse {
            part: Some(Part::End(ResultEnd {
                index,
                affected_rows: res.affected_rows,
                last_insert_id: res.last_insert_id,
                warnings: res.warnings,
                info: res.info.clone(),
                more_results: i + 1 < n,
            })),
        }));
    }
    out
}

fn ndjson_response(rx: mpsc::Receiver<Item>) -> Response<Body> {
    let stream = futures::stream::unfold(Some(rx), |rx| async move {
        let mut rx = rx?;
        let item = rx.recv().await?;
        let (line, next) = match item {
            Item::Part(p) => (results::query_part_json(&p), Some(rx)),
            Item::Error(e) => {
                let (_, body): (u16, ErrorBody) = status::to_rest(&e);
                (json!({ "error": body }), None)
            }
        };
        let mut bytes = serde_json::to_vec(&line).unwrap_or_default();
        bytes.push(b'\n');
        Some((
            Ok::<_, std::convert::Infallible>(Frame::data(Bytes::from(bytes))),
            next,
        ))
    })
    .boxed();
    let mut r = Response::new(Body::new(StreamBody::new(stream)));
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson"),
    );
    r
}

/// Runs a call forwarded by another gateway (the peer service's
/// `Forward`): authenticate again, then run it here.
pub async fn forwarded(core: &Arc<Core>, req: ForwardRequest) -> ForwardReply {
    let reply = async {
        let peer: std::net::SocketAddr = req
            .client_addr
            .parse()
            .unwrap_or_else(|_| std::net::SocketAddr::from(([0, 0, 0, 0], 0)));
        let info = ConnInfo {
            peer,
            tls: None,
            listener: "peer",
            generation: 0,
        };
        let body: Value = serde_json::from_str(&req.request_json).map_err(|e| {
            SprigError::new(
                SprigCode::AdminRequestInvalid,
                format!("forwarded body: {e}"),
            )
        })?;
        let auth = (!req.authorization.is_empty()).then(|| req.authorization.clone());
        let caller = core.caller(&info, auth, None, None).await?;
        let route = routes::by_name(&req.method).ok_or_else(|| {
            SprigError::new(
                SprigCode::AdminRequestInvalid,
                format!("can't forward {}", req.method),
            )
        })?;
        // A forwarded call is never forwarded again.
        let transaction = s(&body, "transaction");
        if let Ok(h) = sprig_api::handle::TxnHandle::parse(&transaction)
            && h.gateway_id != core.engine.gateway_id
        {
            return Err(SprigError::new(
                SprigCode::TxnHandleUnknown,
                "no open transaction has this handle",
            ));
        }
        let mut vars = std::collections::BTreeMap::new();
        for k in ["database", "transaction"] {
            let v = s(&body, k);
            if !v.is_empty() {
                vars.insert(k.to_owned(), v);
            }
        }
        let m = Matched { route, vars };
        let (status_code, out) = run_local(core, &caller, &req.method, &m, &body, false).await?;
        match out {
            Local::Json(v) => Ok((status_code, v)),
            Local::Stream(_) => Err(SprigError::internal("a forwarded call can't stream")),
        }
    }
    .await;
    match reply {
        Ok((status_code, v)) => ForwardReply {
            http_status: u32::from(status_code),
            body_json: v.to_string(),
        },
        Err(e) => {
            let (status_code, body) = status::to_rest(&e);
            ForwardReply {
                http_status: u32::from(status_code),
                body_json: serde_json::to_string(&body).unwrap_or_default(),
            }
        }
    }
}
