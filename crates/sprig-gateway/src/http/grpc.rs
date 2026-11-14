//! The gRPC services on 8080 (h2c) and 8443 (ALPN h2): `sprig.v1.Sql`,
//! `sprig.v1.Transactions`, `sprig.v1.Sinks` and `Connections` (their `Test`
//! calls), every `sprig.admin.v1` service (`Operators` too: sign-in and
//! tokens relay to the controller, whose guard decides) and
//! `grpc.health.v1.Health`.
//!
//! They run the same code as REST: SQL through [`SqlEngine`](super::sql::SqlEngine),
//! admin calls through [`AdminApi`](super::admin::AdminApi) (the gRPC
//! request's path parameters, query string and JSON body make the REST
//! call; the reply carries its JSON). Errors are `Status`es with
//! `google.rpc.Status` details ([`sprig_api::status::to_status`]).

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;
use hyper::Method;
use sprig_api::proto::admin::{
    BodyRequest, JsonReply, ListRequest, NameRequest, TenantObjectRequest, TenantRequest,
};
use sprig_api::proto::health::health_check_response::ServingStatus;
use sprig_api::proto::health::health_server::{Health, HealthServer};
use sprig_api::proto::health::{HealthCheckRequest, HealthCheckResponse};
use sprig_api::proto::internal::ForwardRequest;
use sprig_api::proto::v1::functions_server::{Functions, FunctionsServer};
use sprig_api::proto::v1::sql_server::{Sql, SqlServer};
use sprig_api::proto::v1::transactions_server::{Transactions, TransactionsServer};
use sprig_api::proto::v1::{
    BeginRequest, EndTransactionRequest, EndTransactionResponse, ExecuteResponse, QueryRequest,
    QueryResponse, Transaction,
};
use sprig_api::proto::v1::{CallRequest, CallResponse};
use sprig_api::rest::{by_name, percent_encode};
use sprig_api::status::{ErrorBody, from_rest, to_status};
use sprig_common::{SprigCode, SprigError};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use super::rest::TENANT_HEADER;
use super::sink::Item;
use super::sql::{CHANNEL_DEPTH, Caller, SqlCall, collect};
use super::{ConnInfo, Core};

type GrpcResult<T> = std::result::Result<Response<T>, Status>;

pub(crate) fn st(e: &SprigError) -> Status {
    to_status(e)
}

fn meta(req: &Request<impl Sized>, name: &str) -> Option<String> {
    req.metadata()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

pub(crate) async fn caller<T>(
    core: &Core,
    req: &Request<T>,
) -> std::result::Result<Caller, Status> {
    let info = req
        .extensions()
        .get::<ConnInfo>()
        .cloned()
        .ok_or_else(|| Status::internal("no connection info"))?;
    let mut c = core
        .caller(
            &info,
            meta(req, "authorization"),
            meta(req, TENANT_HEADER),
            None,
        )
        .await
        .map_err(|e| st(&e))?;
    // The client's DC, as gRPC metadata `sprig-dc`.
    c.client_dc = crate::dc::hint("grpc", meta(req, "sprig-dc").as_deref()).known();
    Ok(c)
}

/// A reply from a gateway the call was forwarded to, as the error or the
/// JSON it carries.
fn forwarded_json(
    reply: &sprig_api::proto::internal::ForwardReply,
) -> std::result::Result<serde_json::Value, Status> {
    if reply.http_status >= 400 {
        let body: ErrorBody = serde_json::from_str(&reply.body_json)
            .map_err(|e| Status::internal(format!("forwarded error: {e}")))?;
        return Err(st(&from_rest(&body)));
    }
    serde_json::from_str(&reply.body_json)
        .map_err(|e| Status::internal(format!("forwarded reply: {e}")))
}

/// Forwards `rpc` when `transaction` names another gateway.
async fn forward(
    core: &Core,
    caller: &Caller,
    rpc: &str,
    transaction: &str,
    request: serde_json::Value,
) -> std::result::Result<Option<serde_json::Value>, Status> {
    if transaction.is_empty() {
        return Ok(None);
    }
    let h = sprig_api::handle::TxnHandle::parse(transaction).map_err(|e| st(&e))?;
    if h.gateway_id == core.engine.gateway_id {
        return Ok(None);
    }
    let peers = core.engine.peers.as_ref().ok_or_else(|| {
        st(&SprigError::new(
            SprigCode::ForwardFailed,
            "this gateway has no peer service to reach the transaction's gateway",
        ))
    })?;
    metrics::counter!("sprig_api_txn_forwarded_total").increment(1);
    let reply = peers
        .forward(
            &h.gateway_id,
            ForwardRequest {
                authorization: caller.authorization.clone().unwrap_or_default(),
                method: rpc.to_owned(),
                request_json: request.to_string(),
                client_addr: caller.peer.to_string(),
            },
        )
        .await
        .map_err(|e| st(&e))?;
    forwarded_json(&reply).map(Some)
}

fn query_json(r: &QueryRequest) -> serde_json::Value {
    serde_json::json!({
        "database": r.database, "sql": r.sql, "transaction": r.transaction,
        "multi_statements": r.multi_statements,
        "max_rows": r.max_rows, "max_bytes": r.max_bytes, "timeout_ms": r.timeout_ms,
        "language": r.language, "output": r.output,
    })
}

/// The caps a gRPC request asks for, clamped for Studio sessions.
fn caps_of(core: &Core, c: &super::sql::Caller, r: &QueryRequest) -> super::sql::Caps {
    core.studio.caps_for(
        c,
        &r.database,
        super::sql::Caps {
            max_rows: r.max_rows,
            max_bytes: r.max_bytes,
            timeout: (r.timeout_ms > 0).then(|| std::time::Duration::from_millis(r.timeout_ms)),
        },
    )
}

#[derive(Clone)]
struct SqlSvc(Arc<Core>);

type QueryStream = Pin<Box<dyn Stream<Item = std::result::Result<QueryResponse, Status>> + Send>>;

#[tonic::async_trait]
impl Sql for SqlSvc {
    type QueryStream = QueryStream;

    async fn query(&self, req: Request<QueryRequest>) -> GrpcResult<QueryStream> {
        metrics::counter!("sprig_api_requests_total", "service" => "sprig.v1.Sql", "method" => "Query", "protocol" => "grpc").increment(1);
        let c = caller(&self.0, &req).await?;
        let r = req.into_inner();
        if let Some(v) = forward(&self.0, &c, "Sql.Query", &r.transaction, query_json(&r)).await? {
            let parts: Vec<_> = v
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(sprig_api::results::query_part_from_json)
                        .map(Ok)
                        .collect()
                })
                .unwrap_or_default();
            return Ok(Response::new(Box::pin(futures::stream::iter(parts))));
        }
        let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);
        let engine = Arc::clone(&self.0.engine);
        let caps = caps_of(&self.0, &c, &r);
        let call = SqlCall {
            database: r.database,
            sql: r.sql,
            multi: r.multi_statements,
        };
        let txn = (!r.transaction.is_empty()).then_some(r.transaction);
        let language = super::lang::with_output(&r.language, &r.output);
        tokio::spawn(async move {
            engine
                .run_in(&c, &language, call, txn.as_deref(), caps, tx)
                .await;
        });
        let stream = futures::stream::unfold(Some(rx), |rx| async move {
            let mut rx = rx?;
            match rx.recv().await? {
                Item::Part(p) => Some((Ok(p), Some(rx))),
                Item::Error(e) => Some((Err(st(&e)), None)),
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn execute(&self, req: Request<QueryRequest>) -> GrpcResult<ExecuteResponse> {
        metrics::counter!("sprig_api_requests_total", "service" => "sprig.v1.Sql", "method" => "Execute", "protocol" => "grpc").increment(1);
        let c = caller(&self.0, &req).await?;
        let r = req.into_inner();
        if let Some(v) = forward(&self.0, &c, "Sql.Execute", &r.transaction, query_json(&r)).await?
        {
            return sprig_api::results::execute_from_json(&v)
                .map(Response::new)
                .map_err(|e| st(&e));
        }
        let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);
        let engine = Arc::clone(&self.0.engine);
        let max = engine.settings().max_buffered;
        let charge = engine.charge(&c);
        let caps = caps_of(&self.0, &c, &r);
        let call = SqlCall {
            database: r.database,
            sql: r.sql,
            multi: r.multi_statements,
        };
        let txn = (!r.transaction.is_empty()).then_some(r.transaction);
        let language = super::lang::with_output(&r.language, &r.output);
        tokio::spawn(async move {
            engine
                .run_in(&c, &language, call, txn.as_deref(), caps, tx)
                .await;
        });
        collect(rx, max, charge)
            .await
            .map(Response::new)
            .map_err(|e| st(&e))
    }
}

#[derive(Clone)]
struct FunctionsSvc(Arc<Core>);

#[tonic::async_trait]
impl Functions for FunctionsSvc {
    async fn call(&self, req: Request<CallRequest>) -> GrpcResult<CallResponse> {
        metrics::counter!("sprig_api_requests_total", "service" => "sprig.v1.Functions", "method" => "Call", "protocol" => "grpc").increment(1);
        let c = caller(&self.0, &req).await?;
        let r = req.into_inner();
        let body = super::functions::request_json(&r);
        if let Some(v) = forward(&self.0, &c, "Functions.Call", &r.transaction, body).await? {
            return Ok(Response::new(super::functions::response(&v)));
        }
        let args = r
            .args
            .as_ref()
            .map_or(serde_json::Value::Null, super::functions::from_proto);
        super::functions::call(
            &self.0.engine,
            &c,
            "grpc",
            r.database,
            &r.name,
            args,
            &r.transaction,
        )
        .await
        .map(|v| Response::new(super::functions::response(&v)))
        .map_err(|e| st(&e))
    }
}

/// `sprig.v1.Logic`.
#[derive(Clone)]
struct LogicSvc(Arc<Core>);

#[tonic::async_trait]
impl sprig_api::proto::v1::logic_server::Logic for LogicSvc {
    async fn test_run(
        &self,
        req: Request<sprig_api::proto::v1::TestRunRequest>,
    ) -> GrpcResult<sprig_api::proto::v1::TestRunResponse> {
        metrics::counter!("sprig_api_requests_total", "service" => "sprig.v1.Logic", "method" => "TestRun", "protocol" => "grpc").increment(1);
        let c = caller(&self.0, &req).await?;
        let r = req.into_inner();
        let body = super::functions::test_run_json(&r);
        super::functions::test_run(&self.0.engine, &c, r.database, body)
            .await
            .map(|v| Response::new(super::functions::test_run_response(&v)))
            .map_err(|e| st(&e))
    }
}

#[derive(Clone)]
struct KvSvc(Arc<Core>);

impl KvSvc {
    async fn run(
        &self,
        c: &Caller,
        rpc: &str,
        body: serde_json::Value,
    ) -> std::result::Result<serde_json::Value, Status> {
        metrics::counter!("sprig_api_requests_total", "service" => "sprig.v1.KeyValue", "method" => rpc.to_owned(), "protocol" => "grpc").increment(1);
        super::kv::call(&self.0.engine, c, &format!("KeyValue.{rpc}"), &body)
            .await
            .map_err(|e| st(&e))
    }
}

fn kv_entry(v: &serde_json::Value) -> sprig_api::proto::v1::KvEntry {
    use base64::Engine as _;
    let s = |k: &str| {
        v.get(k)
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_owned()
    };
    sprig_api::proto::v1::KvEntry {
        key: s("key"),
        value: base64::engine::general_purpose::STANDARD
            .decode(s("value"))
            .unwrap_or_default(),
        text: s("text"),
        r#type: s("type"),
        version: s("version").parse().unwrap_or(0),
        expires_at_ms: s("expires_at_ms").parse().unwrap_or(0),
    }
}

#[tonic::async_trait]
impl sprig_api::proto::v1::key_value_server::KeyValue for KvSvc {
    async fn get(
        &self,
        req: Request<sprig_api::proto::v1::KvGetRequest>,
    ) -> GrpcResult<sprig_api::proto::v1::KvGetResponse> {
        let c = caller(&self.0, &req).await?;
        let r = req.into_inner();
        let v = self
            .run(
                &c,
                "Get",
                serde_json::json!({"database": r.database, "namespace": r.namespace, "key": r.key}),
            )
            .await?;
        Ok(Response::new(sprig_api::proto::v1::KvGetResponse {
            entry: v.get("entry").map(kv_entry),
            found: v
                .get("found")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
        }))
    }

    async fn put(
        &self,
        req: Request<sprig_api::proto::v1::KvPutRequest>,
    ) -> GrpcResult<sprig_api::proto::v1::KvPutResponse> {
        use base64::Engine as _;
        let c = caller(&self.0, &req).await?;
        let r = req.into_inner();
        let mut body = serde_json::json!({
            "database": r.database, "namespace": r.namespace, "key": r.key,
            "text": r.text, "if_absent": r.if_absent, "if_exists": r.if_exists,
            "ttl_ms": r.ttl_ms,
        });
        if !r.value.is_empty() {
            body["value"] = serde_json::Value::String(
                base64::engine::general_purpose::STANDARD.encode(&r.value),
            );
        }
        if let Some(v) = r.if_version {
            body["if_version"] = serde_json::Value::String(v.to_string());
        }
        if let Some(l) = &r.location {
            body["location"] =
                serde_json::json!({"latitude": l.latitude, "longitude": l.longitude});
        }
        let v = self.run(&c, "Put", body).await?;
        Ok(Response::new(sprig_api::proto::v1::KvPutResponse {
            version: v
                .get("version")
                .and_then(serde_json::Value::as_str)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
        }))
    }

    async fn delete(
        &self,
        req: Request<sprig_api::proto::v1::KvDeleteRequest>,
    ) -> GrpcResult<sprig_api::proto::v1::KvDeleteResponse> {
        let c = caller(&self.0, &req).await?;
        let r = req.into_inner();
        let mut body =
            serde_json::json!({"database": r.database, "namespace": r.namespace, "key": r.key});
        if let Some(v) = r.if_version {
            body["if_version"] = serde_json::Value::String(v.to_string());
        }
        let v = self.run(&c, "Delete", body).await?;
        Ok(Response::new(sprig_api::proto::v1::KvDeleteResponse {
            deleted: v
                .get("deleted")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
        }))
    }

    async fn scan(
        &self,
        req: Request<sprig_api::proto::v1::KvScanRequest>,
    ) -> GrpcResult<sprig_api::proto::v1::KvScanResponse> {
        let c = caller(&self.0, &req).await?;
        let r = req.into_inner();
        let v = self
            .run(&c, "Scan", serde_json::json!({"database": r.database, "namespace": r.namespace, "prefix": r.prefix, "page_token": r.page_token, "limit": r.limit}))
            .await?;
        Ok(Response::new(sprig_api::proto::v1::KvScanResponse {
            entries: v
                .get("entries")
                .and_then(serde_json::Value::as_array)
                .map(|a| a.iter().map(kv_entry).collect())
                .unwrap_or_default(),
            next_page_token: v
                .get("next_page_token")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .to_owned(),
        }))
    }

    async fn exec(
        &self,
        req: Request<sprig_api::proto::v1::KvExecRequest>,
    ) -> GrpcResult<sprig_api::proto::v1::KvExecResponse> {
        let c = caller(&self.0, &req).await?;
        let r = req.into_inner();
        let v = self
            .run(&c, "Exec", serde_json::json!({"database": r.database, "namespace": r.namespace, "command": r.command}))
            .await?;
        Ok(Response::new(sprig_api::proto::v1::KvExecResponse {
            reply: v.get("reply").map(super::functions::to_proto),
        }))
    }

    async fn geo_search(
        &self,
        req: Request<sprig_api::proto::v1::KvGeoSearchRequest>,
    ) -> GrpcResult<sprig_api::proto::v1::KvGeoSearchResponse> {
        let c = caller(&self.0, &req).await?;
        let r = req.into_inner();
        let ll = |p: Option<&sprig_api::proto::v1::LatLng>| {
            p.map(|p| serde_json::json!({"latitude": p.latitude, "longitude": p.longitude}))
        };
        let mut body = serde_json::json!({"database": r.database, "namespace": r.namespace,
            "limit": r.limit, "max_distance_m": r.max_distance_m, "near": ll(r.near.as_ref())});
        if let Some(g) = &r.region {
            body["region"] = serde_json::json!({"center": ll(g.center.as_ref()), "radius_m": g.radius_m,
                "south_west": ll(g.south_west.as_ref()), "north_east": ll(g.north_east.as_ref()),
                "polygon": g.polygon.iter().map(|p| ll(Some(p))).collect::<Vec<_>>()});
        }
        let v = self.run(&c, "GeoSearch", body).await?;
        let hits = v
            .get("hits")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .map(|h| {
                let loc = h.get("location");
                let f = |k: &str| {
                    loc.and_then(|l| l.get(k))
                        .and_then(serde_json::Value::as_f64)
                        .unwrap_or(0.0)
                };
                sprig_api::proto::v1::KvGeoHit {
                    entry: h.get("entry").map(kv_entry),
                    location: Some(sprig_api::proto::v1::LatLng {
                        latitude: f("latitude"),
                        longitude: f("longitude"),
                    }),
                    distance_m: h
                        .get("distance_m")
                        .and_then(serde_json::Value::as_f64)
                        .unwrap_or(0.0),
                }
            })
            .collect();
        Ok(Response::new(sprig_api::proto::v1::KvGeoSearchResponse {
            hits,
        }))
    }
}

#[derive(Clone)]
struct TxnSvc(Arc<Core>);

#[tonic::async_trait]
impl Transactions for TxnSvc {
    async fn begin(&self, req: Request<BeginRequest>) -> GrpcResult<Transaction> {
        let c = caller(&self.0, &req).await?;
        let r = req.into_inner();
        self.0
            .engine
            .begin(&c, &r.database, r.read_only)
            .await
            .map(Response::new)
            .map_err(|e| st(&e))
    }

    async fn commit(
        &self,
        req: Request<EndTransactionRequest>,
    ) -> GrpcResult<EndTransactionResponse> {
        self.end(req, true).await
    }

    async fn rollback(
        &self,
        req: Request<EndTransactionRequest>,
    ) -> GrpcResult<EndTransactionResponse> {
        self.end(req, false).await
    }
}

impl TxnSvc {
    async fn end(
        &self,
        req: Request<EndTransactionRequest>,
        commit: bool,
    ) -> GrpcResult<EndTransactionResponse> {
        let c = caller(&self.0, &req).await?;
        let r = req.into_inner();
        let rpc = if commit {
            "Transactions.Commit"
        } else {
            "Transactions.Rollback"
        };
        let body = serde_json::json!({ "transaction": r.transaction });
        if forward(&self.0, &c, rpc, &r.transaction, body)
            .await?
            .is_some()
        {
            return Ok(Response::new(EndTransactionResponse {}));
        }
        self.0
            .engine
            .end(&c, &r.transaction, commit)
            .await
            .map(|()| Response::new(EndTransactionResponse {}))
            .map_err(|e| st(&e))
    }
}

/// The REST parts of an admin request.
trait AdminReq {
    fn parts(&self) -> (BTreeMap<String, String>, &str, &str);
}

impl AdminReq for ListRequest {
    fn parts(&self) -> (BTreeMap<String, String>, &str, &str) {
        (BTreeMap::new(), &self.query, "")
    }
}

impl AdminReq for BodyRequest {
    fn parts(&self) -> (BTreeMap<String, String>, &str, &str) {
        (BTreeMap::new(), &self.query, &self.body_json)
    }
}

impl AdminReq for NameRequest {
    fn parts(&self) -> (BTreeMap<String, String>, &str, &str) {
        (
            BTreeMap::from([("name".to_owned(), self.name.clone())]),
            &self.query,
            &self.body_json,
        )
    }
}

impl AdminReq for TenantRequest {
    fn parts(&self) -> (BTreeMap<String, String>, &str, &str) {
        (
            BTreeMap::from([("tenant".to_owned(), self.tenant.clone())]),
            &self.query,
            &self.body_json,
        )
    }
}

impl AdminReq for TenantObjectRequest {
    fn parts(&self) -> (BTreeMap<String, String>, &str, &str) {
        (
            BTreeMap::from([
                ("tenant".to_owned(), self.tenant.clone()),
                ("name".to_owned(), self.name.clone()),
            ]),
            &self.query,
            &self.body_json,
        )
    }
}

/// `sprig.admin.v1.Recommendations` for a tenant login: the same caller
/// claim as the REST face `/v1/recommendations`. `None` when the credential
/// isn't a tenant login (operators' tokens go through the Admin API as they
/// are).
async fn recommendations_as_tenant<R: AdminReq>(
    core: &Arc<Core>,
    rpc: &str,
    req: &Request<R>,
) -> std::result::Result<Option<JsonReply>, Status> {
    let Ok(c) = caller(core, req).await else {
        return Ok(None);
    };
    if c.principal.tenant_name.is_none() {
        return Ok(None);
    }
    let (vars, query, body) = req.get_ref().parts();
    let name = vars.get("name").cloned().unwrap_or_default();
    let body_v: serde_json::Value = if body.trim().is_empty() {
        serde_json::json!({})
    } else {
        serde_json::from_str(body)
            .map_err(|e| Status::invalid_argument(format!("body_json: {e}")))?
    };
    let (method, rest) = match rpc.rsplit('.').next().unwrap_or(rpc) {
        "ListRecommendations" => (Method::GET, String::new()),
        "GetRecommendation" => (Method::GET, format!("/{name}")),
        "ActOnRecommendation" => {
            let action = body_v
                .get("action")
                .and_then(serde_json::Value::as_str)
                .filter(|a| !a.is_empty() && a.chars().all(|c| c.is_ascii_lowercase()))
                .ok_or_else(|| {
                    Status::invalid_argument(
                        "body_json needs \"action\": approve, ignore, snooze, reopen or comment",
                    )
                })?;
            (Method::POST, format!("/{name}:{action}"))
        }
        "BulkAct" => (Method::POST, ":bulk".to_owned()),
        "GetApprovalPolicy" => (Method::GET, ":policy".to_owned()),
        "ListSubscriptions" => (Method::GET, ":subscriptions".to_owned()),
        "SetSubscription" => (Method::POST, ":subscriptions".to_owned()),
        _ => {
            return Err(Status::permission_denied(
                "tenants set approval policy with ALTER ... SPRIG SET (39974)",
            ));
        }
    };
    let reply = super::studio::recommendations::proxy(
        core,
        &c,
        &method,
        &rest,
        query.trim_start_matches('?'),
        body.as_bytes(),
    )
    .await
    .map_err(|e| st(&e))?;
    if reply.status >= 400 {
        let e = serde_json::from_slice::<ErrorBody>(&reply.body).map_or_else(
            |_| SprigError::internal(format!("the controller answered {}", reply.status)),
            |b| from_rest(&b),
        );
        return Err(st(&e));
    }
    Ok(Some(JsonReply {
        json: String::from_utf8_lossy(&reply.body).into_owned(),
    }))
}

async fn admin_call<R: AdminReq>(
    core: &Arc<Core>,
    rpc: &str,
    req: Request<R>,
) -> GrpcResult<JsonReply> {
    metrics::counter!("sprig_api_requests_total", "service" => "sprig.admin.v1", "method" => rpc.to_owned(), "protocol" => "grpc").increment(1);
    if rpc.starts_with("Recommendations.")
        && let Some(r) = recommendations_as_tenant(core, rpc, &req).await?
    {
        return Ok(Response::new(r));
    }
    let auth = meta(&req, "authorization");
    let info = req.extensions().get::<ConnInfo>().cloned();
    let r = req.into_inner();
    let route = by_name(rpc).ok_or_else(|| Status::unimplemented(rpc.to_owned()))?;
    // `studio.operator_mtls` covers the Operators service.
    if route.service == "sprig.admin.v1.Operators" {
        let mtls = core
            .studio
            .tenant_get(&sprig_studio::settings::OPERATOR_MTLS, None, None);
        super::studio::ops::operators_cert_check(
            mtls.as_ref(),
            info.as_ref()
                .and_then(|i| i.tls.as_ref())
                .is_some_and(|t| t.client_cert),
            "/admin/v1/operators",
        )
        .map_err(|e| st(&e))?;
    }
    let (vars, query, body) = r.parts();
    if route.service == "sprig.admin.v1.Gateways" {
        let v = core
            .admin
            .gateways(
                route.rpc,
                vars.get("name").map(String::as_str),
                auth.as_deref(),
            )
            .await
            .map_err(|e| st(&e))?;
        return Ok(Response::new(JsonReply {
            json: v.to_string(),
        }));
    }
    let encoded: BTreeMap<String, String> = vars
        .into_iter()
        .map(|(k, v)| (k, percent_encode(&v)))
        .collect();
    let mut path = route.path(&encoded);
    if !query.is_empty() {
        path.push('?');
        path.push_str(query.trim_start_matches('?'));
    }
    let method = Method::from_bytes(route.method.as_bytes()).unwrap_or(Method::GET);
    // The controller records the client's address.
    let reply = core
        .admin
        .forward_from(
            &method,
            &path,
            auth.as_deref(),
            Bytes::from(body.to_owned()),
            info.as_ref().map(|i| i.peer.ip()),
        )
        .await
        .map_err(|e| st(&e))?;
    if reply.status >= 400 {
        let e = serde_json::from_slice::<ErrorBody>(&reply.body).map_or_else(
            |_| SprigError::internal(format!("the controller answered {}", reply.status)),
            |b| from_rest(&b),
        );
        return Err(st(&e));
    }
    Ok(Response::new(JsonReply {
        json: String::from_utf8_lossy(&reply.body).into_owned(),
    }))
}

macro_rules! admin_service {
    ($svc:ident, $module:ident :: $trait:ident, $server:ident, $prefix:literal, { $($fn:ident : $req:ty => $rpc:literal),* $(,)? }) => {
        #[derive(Clone)]
        struct $svc(Arc<Core>);

        #[tonic::async_trait]
        impl sprig_api::proto::admin::$module::$trait for $svc {
            $(
                async fn $fn(&self, req: Request<$req>) -> GrpcResult<JsonReply> {
                    admin_call(&self.0, concat!($prefix, ".", $rpc), req).await
                }
            )*
        }
    };
}

admin_service!(ClustersSvc, clusters_server::Clusters, ClustersServer, "Clusters", {
    register_cluster: BodyRequest => "RegisterCluster",
    list_clusters: ListRequest => "ListClusters",
    get_cluster: NameRequest => "GetCluster",
    probe_cluster: NameRequest => "ProbeCluster",
    drain_cluster: NameRequest => "DrainCluster",
});
admin_service!(DcsSvc, data_centers_server::DataCenters, DataCentersServer, "DataCenters", {
    register_dc: BodyRequest => "RegisterDc",
    list_dcs: ListRequest => "ListDcs",
    get_dc: NameRequest => "GetDc",
});
admin_service!(TenantsSvc, tenants_server::Tenants, TenantsServer, "Tenants", {
    create_tenant: BodyRequest => "CreateTenant",
    list_tenants: ListRequest => "ListTenants",
    get_tenant: NameRequest => "GetTenant",
    delete_tenant: NameRequest => "DeleteTenant",
    suspend_tenant: NameRequest => "SuspendTenant",
    resume_tenant: NameRequest => "ResumeTenant",
});
admin_service!(UsersSvc, users_server::Users, UsersServer, "Users", {
    create_user: NameRequest => "CreateUser",
});
admin_service!(SettingsSvc, settings_server::Settings, SettingsServer, "Settings", {
    list_setting_keys: ListRequest => "ListSettingKeys",
    get_setting: NameRequest => "GetSetting",
    explain_setting: NameRequest => "ExplainSetting",
    set_setting: NameRequest => "SetSetting",
    unset_setting: NameRequest => "UnsetSetting",
    reset_setting: NameRequest => "ResetSetting",
    set_bounds: NameRequest => "SetBounds",
    lock_setting: NameRequest => "LockSetting",
    unlock_setting: NameRequest => "UnlockSetting",
    tenant_settings: NameRequest => "TenantSettings",
});
admin_service!(IndexesSvc, indexes_server::Indexes, IndexesServer, "Indexes", {
    list_indexes: TenantRequest => "ListIndexes",
    list_recommendations: TenantRequest => "ListRecommendations",
    get_index: TenantObjectRequest => "GetIndex",
    apply: TenantObjectRequest => "Apply",
    dismiss: TenantObjectRequest => "Dismiss",
    drop: TenantObjectRequest => "Drop",
    freeze: TenantObjectRequest => "Freeze",
    unfreeze: TenantObjectRequest => "Unfreeze",
});
admin_service!(ExplainSvc, explain_server::Explain, ExplainServer, "Explain", {
    explain_query: TenantObjectRequest => "ExplainQuery",
});
admin_service!(JobsSvc, jobs_server::Jobs, JobsServer, "Jobs", {
    list_jobs: ListRequest => "ListJobs",
    get_job: NameRequest => "GetJob",
    cancel_job: NameRequest => "CancelJob",
});
admin_service!(GatewaysSvc, gateways_server::Gateways, GatewaysServer, "Gateways", {
    list_gateways: ListRequest => "ListGateways",
    get_gateway: NameRequest => "GetGateway",
    drain_gateway: NameRequest => "DrainGateway",
});
admin_service!(RecommendationsSvc, recommendations_server::Recommendations, RecommendationsServer, "Recommendations", {
    list_recommendations: ListRequest => "ListRecommendations",
    get_recommendation: NameRequest => "GetRecommendation",
    act_on_recommendation: NameRequest => "ActOnRecommendation",
    bulk_act: BodyRequest => "BulkAct",
    get_approval_policy: ListRequest => "GetApprovalPolicy",
    set_approval_policy: BodyRequest => "SetApprovalPolicy",
    list_subscriptions: ListRequest => "ListSubscriptions",
    set_subscription: BodyRequest => "SetSubscription",
});
admin_service!(OperatorsSvc, operators_server::Operators, OperatorsServer, "Operators", {
    login: BodyRequest => "Login",
    enrol: BodyRequest => "Enrol",
    logout: BodyRequest => "Logout",
    who_am_i: ListRequest => "WhoAmI",
    list_operators: ListRequest => "ListOperators",
    create_operator: BodyRequest => "CreateOperator",
    get_operator: NameRequest => "GetOperator",
    disable_operator: NameRequest => "DisableOperator",
    enable_operator: NameRequest => "EnableOperator",
    reset_operator_factor: NameRequest => "ResetOperatorFactor",
    set_operator_password: NameRequest => "SetOperatorPassword",
    create_token: BodyRequest => "CreateToken",
    list_tokens: ListRequest => "ListTokens",
    revoke_token: NameRequest => "RevokeToken",
});

/// `sprig.v1.Sinks.Test` and `sprig.v1.Connections.Test`: the statements
/// the REST routes run, as the caller.
struct SinksSvc(Arc<Core>);

/// The columns and rows of a test as the reply; a `NULL` cell is a null.
fn test_reply(
    (columns, rows): (Vec<String>, Vec<Vec<Option<String>>>),
) -> sprig_api::proto::v1::TestReply {
    use prost_types::value::Kind;
    let cell = |c: Option<String>| prost_types::Value {
        kind: Some(c.map_or(Kind::NullValue(0), Kind::StringValue)),
    };
    sprig_api::proto::v1::TestReply {
        columns,
        rows: rows
            .into_iter()
            .map(|r| prost_types::ListValue {
                values: r.into_iter().map(cell).collect(),
            })
            .collect(),
    }
}

#[tonic::async_trait]
impl sprig_api::proto::v1::sinks_server::Sinks for SinksSvc {
    async fn test(
        &self,
        req: Request<sprig_api::proto::v1::SinkTestRequest>,
    ) -> GrpcResult<sprig_api::proto::v1::TestReply> {
        metrics::counter!("sprig_api_requests_total", "service" => "sprig.v1.Sinks", "method" => "Test", "protocol" => "grpc").increment(1);
        let c = caller(&self.0, &req).await?;
        let r = req.get_ref();
        let sample = if r.sample == 0 { 10 } else { r.sample };
        let body = serde_json::json!({ "sample": sample });
        super::studio::messaging::run_test(&self.0, &c, &r.database, "sinks", &r.sink, &body)
            .await
            .map(|x| Response::new(test_reply(x)))
            .map_err(|e| st(&e))
    }
}

#[tonic::async_trait]
impl sprig_api::proto::v1::connections_server::Connections for SinksSvc {
    async fn test(
        &self,
        req: Request<sprig_api::proto::v1::ConnectionTestRequest>,
    ) -> GrpcResult<sprig_api::proto::v1::TestReply> {
        metrics::counter!("sprig_api_requests_total", "service" => "sprig.v1.Connections", "method" => "Test", "protocol" => "grpc").increment(1);
        let c = caller(&self.0, &req).await?;
        let r = req.get_ref();
        let mut body = serde_json::json!({ "platform": r.platform, "write": r.write });
        if r.write && !r.to.is_empty() {
            body["to"] = serde_json::Value::from(r.to.clone());
        }
        super::studio::messaging::run_test(
            &self.0,
            &c,
            &r.database,
            "connections",
            &r.connection,
            &body,
        )
        .await
        .map(|x| Response::new(test_reply(x)))
        .map_err(|e| st(&e))
    }
}

#[derive(Clone)]
struct HealthSvc(Arc<Core>);

impl HealthSvc {
    fn status(&self) -> HealthCheckResponse {
        let s = if self.0.engine.draining.load(Ordering::SeqCst) {
            ServingStatus::NotServing
        } else {
            ServingStatus::Serving
        };
        HealthCheckResponse { status: s as i32 }
    }
}

type HealthStream =
    Pin<Box<dyn Stream<Item = std::result::Result<HealthCheckResponse, Status>> + Send>>;

#[tonic::async_trait]
impl Health for HealthSvc {
    async fn check(&self, _req: Request<HealthCheckRequest>) -> GrpcResult<HealthCheckResponse> {
        Ok(Response::new(self.status()))
    }

    type WatchStream = HealthStream;

    async fn watch(&self, _req: Request<HealthCheckRequest>) -> GrpcResult<HealthStream> {
        let me = self.clone();
        let stream = futures::stream::unfold((me, None::<i32>), |(me, last)| async move {
            loop {
                let now = me.status();
                if last != Some(now.status) {
                    let s = now.status;
                    return Some((Ok(now), (me, Some(s))));
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }
}

/// `sprig.v1.Changes`.
struct ChangesSvc(Arc<Core>);

#[tonic::async_trait]
impl sprig_api::proto::v1::changes_server::Changes for ChangesSvc {
    type WatchStream = std::pin::Pin<
        Box<
            dyn futures::Stream<
                    Item = std::result::Result<sprig_api::proto::v1::ChangeEvent, Status>,
                > + Send,
        >,
    >;

    async fn watch(
        &self,
        req: Request<sprig_api::proto::v1::WatchRequest>,
    ) -> GrpcResult<Self::WatchStream> {
        metrics::counter!("sprig_api_requests_total", "service" => "sprig.v1.Changes", "method" => "Watch", "protocol" => "grpc").increment(1);
        let c = caller(&self.0, &req).await?;
        let r = req.into_inner();
        let spec = super::changes::WatchSpec {
            database: r.database,
            objects: r.objects,
            start_after: r.start_after,
            start_at_hlc: r.start_at_hlc,
            full_document: r.full_document,
        };
        let rx = super::changes::open(&self.0, c, spec)
            .await
            .map_err(|e| st(&e))?;
        let stream = futures::stream::unfold(Some(rx), |rx| async move {
            let mut rx = rx?;
            match rx.recv().await? {
                Ok(ev) => Some((Ok(ev.proto()), Some(rx))),
                Err(e) => Some((Err(st(&e)), None)),
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Every gRPC service over `core`, as one router.
#[must_use]
pub fn routes(core: &Arc<Core>) -> tonic::service::Routes {
    use sprig_api::proto::admin::clusters_server::ClustersServer;
    use sprig_api::proto::admin::data_centers_server::DataCentersServer;
    use sprig_api::proto::admin::explain_server::ExplainServer;
    use sprig_api::proto::admin::gateways_server::GatewaysServer;
    use sprig_api::proto::admin::indexes_server::IndexesServer;
    use sprig_api::proto::admin::jobs_server::JobsServer;
    use sprig_api::proto::admin::settings_server::SettingsServer;
    use sprig_api::proto::admin::tenants_server::TenantsServer;
    use sprig_api::proto::admin::users_server::UsersServer;
    let max = core.engine.settings().max_request;
    let c = || Arc::clone(core);
    tonic::service::Routes::new(SqlServer::new(SqlSvc(c())).max_decoding_message_size(max))
        .add_service(TransactionsServer::new(TxnSvc(c())))
        .add_service(FunctionsServer::new(FunctionsSvc(c())).max_decoding_message_size(max))
        .add_service(
            sprig_api::proto::v1::logic_server::LogicServer::new(LogicSvc(c()))
                .max_decoding_message_size(max),
        )
        .add_service(
            sprig_api::proto::v1::key_value_server::KeyValueServer::new(KvSvc(c()))
                .max_decoding_message_size(max),
        )
        .add_service(
            sprig_api::proto::v1::vectors_server::VectorsServer::new(super::vectors::VectorsSvc(
                c(),
            ))
            .max_decoding_message_size(max),
        )
        .add_service(
            sprig_api::proto::v1::analytics_server::AnalyticsServer::new(
                super::analytics::AnalyticsSvc(c()),
            )
            .max_decoding_message_size(max),
        )
        .add_service(
            sprig_api::proto::v1::search_server::SearchServer::new(super::search::SearchSvc(c()))
                .max_decoding_message_size(max),
        )
        .add_service(sprig_api::proto::v1::changes_server::ChangesServer::new(
            ChangesSvc(c()),
        ))
        .add_service(sprig_api::proto::v1::sinks_server::SinksServer::new(
            SinksSvc(c()),
        ))
        .add_service(
            sprig_api::proto::v1::connections_server::ConnectionsServer::new(SinksSvc(c())),
        )
        .add_service(HealthServer::new(HealthSvc(c())))
        .add_service(ClustersServer::new(ClustersSvc(c())))
        .add_service(DataCentersServer::new(DcsSvc(c())))
        .add_service(TenantsServer::new(TenantsSvc(c())))
        .add_service(UsersServer::new(UsersSvc(c())))
        .add_service(SettingsServer::new(SettingsSvc(c())))
        .add_service(IndexesServer::new(IndexesSvc(c())))
        .add_service(ExplainServer::new(ExplainSvc(c())))
        .add_service(JobsServer::new(JobsSvc(c())))
        .add_service(GatewaysServer::new(GatewaysSvc(c())))
        .add_service(
            sprig_api::proto::admin::recommendations_server::RecommendationsServer::new(
                RecommendationsSvc(c()),
            ),
        )
        .add_service(
            sprig_api::proto::admin::operators_server::OperatorsServer::new(OperatorsSvc(c())),
        )
        .prepare()
}

#[cfg(test)]
mod tests {
    use prost_types::value::Kind;

    /// `Sinks.Test` and `Connections.Test`'s reply: the columns, and each
    /// row's cells with `NULL` as a null value.
    #[test]
    fn a_test_reply_keeps_nulls() {
        let r = super::test_reply((
            vec!["step".into(), "detail".into()],
            vec![vec![Some("request".into()), None]],
        ));
        assert_eq!(r.columns, ["step", "detail"]);
        assert_eq!(r.rows.len(), 1);
        assert_eq!(
            r.rows[0].values[0].kind,
            Some(Kind::StringValue("request".into()))
        );
        assert_eq!(r.rows[0].values[1].kind, Some(Kind::NullValue(0)));
    }
}
