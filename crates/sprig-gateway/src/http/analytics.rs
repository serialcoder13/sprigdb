//! `sprig.v1.Analytics` (chapter 35, "Analytics query"; chapter 37):
//! dimensional models and their queries over REST
//! (`/v1/databases/{db}/models[/{model}[:query|:explain|:drill]]`) and gRPC.
//!
//! A call opens the caller's own executor session, as the key-value API
//! does; the work is `sprig-dim`'s. Admission control (chapter 35,
//! "Admission control"): at most `analytics.max_concurrent_per_tenant` (2)
//! queries per tenant and `analytics.max_concurrent_per_gateway` (8) per
//! gateway; a query waits up to 1 s for a slot, then gets 39780.

use std::sync::Arc;

use serde_json::{Value, json};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_dim::merge::OutRow;
use sprig_dim::query::Query;
use sprig_dim::store;
use tonic::{Request, Response, Status};

use super::sql::{Caller, SqlEngine};
use crate::redis::SqlSession;

fn s(v: &Value, k: &str) -> String {
    v.get(k).and_then(Value::as_str).unwrap_or("").to_owned()
}

fn bad(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::AdminRequestInvalid, m)
}

fn open(engine: &Arc<SqlEngine>, caller: &Caller) -> Result<SqlSession> {
    SqlSession::open(
        &engine.sessions,
        &engine.executor,
        &caller.principal,
        caller.listener,
        caller.generation,
        caller.peer,
        caller.tls.clone(),
    )
}

fn info_json(database: &str, st: &store::Stored) -> Value {
    json!({"database": database, "model": st.model.name, "definition": st.definition,
           "version": st.version.to_string(), "state": st.state})
}

/// Runs `Analytics.<rpc>` for `caller` with the request `body` (proto3
/// JSON, path variables merged); the reply as proto3 JSON.
///
/// # Errors
/// 39750-39757, 39780 and the backend's.
#[allow(clippy::too_many_lines)] // one arm per RPC
pub async fn call(
    engine: &Arc<SqlEngine>,
    caller: &Caller,
    rpc: &str,
    body: &Value,
) -> Result<Value> {
    let database = s(body, "database");
    let model = s(body, "model");
    let mut sql = open(engine, caller)?;
    match rpc {
        "Analytics.CreateModel" | "Analytics.UpdateModel" => {
            let mut def = body.get("definition").cloned().unwrap_or(Value::Null);
            if !def.is_object() {
                return Err(bad("definition must be a JSON object"));
            }
            if def.get("model").is_none() && !model.is_empty() {
                def["model"] = json!(model);
            }
            if rpc == "Analytics.UpdateModel" && !model.is_empty() && s(&def, "model") != model {
                return Err(bad("the definition's model differs from the path's"));
            }
            let st = store::put(&mut sql, &database, &def, rpc == "Analytics.UpdateModel").await?;
            Ok(info_json(&database, &st))
        }
        "Analytics.GetModel" => {
            let st = store::get(&mut sql, &database, &model).await?;
            Ok(info_json(&database, &st))
        }
        "Analytics.ListModels" => {
            let all = store::list(&mut sql, &database).await?;
            Ok(json!({"models": all.iter().map(|m| info_json(&database, m)).collect::<Vec<_>>()}))
        }
        "Analytics.DropModel" => {
            store::drop(&mut sql, &database, &model).await?;
            Ok(json!({"dropped": true}))
        }
        "Analytics.Query" => {
            let (plan, rows) = answer(engine, caller, &mut sql, &database, body).await?;
            let rows: Vec<Value> = rows.iter().map(row_json).collect();
            Ok(json!({"rows": rows, "plan": plan}))
        }
        "Analytics.Explain" => {
            let q = Query::parse(body)?;
            let st = store::get(&mut sql, &database, &q.model).await?;
            let c = sprig_dim::query::compile(&st.model, &q)?;
            let mut stmts = vec![c.sql.clone()];
            stmts.extend(c.levels.iter().cloned());
            Ok(json!({"plan": c.plan, "sql": stmts}))
        }
        "Analytics.Drill" => {
            let mut qv = body.get("query").cloned().unwrap_or(Value::Null);
            if qv.get("model").is_none() {
                qv["model"] = json!(model);
            }
            let q = Query::parse(&qv)?;
            let st = store::get(&mut sql, &database, &q.model).await?;
            let member = body.get("member").filter(|m| !m.is_null());
            let up = body.get("up").and_then(Value::as_bool).unwrap_or(false);
            let next = store::drill(&st.model, &q, &s(body, "dimension"), member, up)?;
            let mut out = next.to_json();
            out["database"] = json!(database);
            Ok(out)
        }
        other => Err(bad(format!("no analytics call {other}"))),
    }
}

fn row_json(r: &OutRow) -> Value {
    json!({"keys": r.keys, "values": r.values, "grouping": r.grouping})
}

/// Runs `Analytics.Query` under admission control and the live limits:
/// the plan and the rows (detail rows, subtotals, the grand total last).
async fn answer(
    engine: &Arc<SqlEngine>,
    caller: &Caller,
    sql: &mut SqlSession,
    database: &str,
    body: &Value,
) -> Result<(&'static str, Vec<OutRow>)> {
    let q = Query::parse(body)?;
    // Live `analytics.*` settings (R6 round 3), at the tenant's and the
    // database's scopes (R6 round 4).
    let limits = super::model_settings::analytics_for(engine, caller, database);
    let _admitted =
        super::model_settings::admit(caller.principal.tenant.map_or(0, |t| t.0), limits).await?;
    let t = std::time::Instant::now();
    let a = tokio::time::timeout(
        limits.timeout,
        store::query(sql, database, &q, limits.max_groups),
    )
    .await
    .map_err(|_| {
        SprigError::standard(
            3024,
            *b"HY000",
            "Query execution was interrupted, maximum statement execution time exceeded (analytics.query_timeout_seconds)",
        )
    })??;
    metrics::counter!("sprig_dim_queries_total", "plan" => a.compiled.plan).increment(1);
    metrics::histogram!("sprig_dim_query_duration_seconds").record(t.elapsed().as_secs_f64());
    #[allow(clippy::cast_precision_loss)] // a group count, far below 2^52
    metrics::histogram!("sprig_dim_merge_groups").record(a.rows.len() as f64);
    Ok((a.compiled.plan, a.rows))
}

/// Rows per streamed message of `Analytics.Query` (gRPC messages, NDJSON
/// lines).
pub const STREAM_BATCH_ROWS: usize = 1000;

/// One part of a streamed `Analytics.Query` answer.
#[derive(Debug)]
pub enum Part {
    /// First: how the query ran.
    Header {
        /// `local` or `per_level`.
        plan: &'static str,
    },
    /// Up to [`STREAM_BATCH_ROWS`] rows, in order.
    Rows(Vec<OutRow>),
    /// Last, after every row: how many there were.
    End {
        /// Rows sent.
        rows: usize,
    },
    /// The query failed; nothing follows.
    Error(SprigError),
}

/// `Analytics.Query` as a stream (chapter 37: gRPC server streaming, REST
/// NDJSON): the merged answer goes out [`STREAM_BATCH_ROWS`] rows at a
/// time, each batch turned into JSON or protobuf only when the receiver
/// takes it. The merge itself holds every group (at most
/// `analytics.max_groups`), since subtotals need them all.
#[must_use]
pub fn query_stream(
    engine: Arc<SqlEngine>,
    caller: Caller,
    body: Value,
) -> tokio::sync::mpsc::Receiver<Part> {
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tokio::spawn(async move {
        let database = s(&body, "database");
        let run = async {
            let mut sql = open(&engine, &caller)?;
            answer(&engine, &caller, &mut sql, &database, &body).await
        };
        match run.await {
            Err(e) => {
                let _ = tx.send(Part::Error(e)).await;
            }
            Ok((plan, mut rows)) => {
                if tx.send(Part::Header { plan }).await.is_err() {
                    return;
                }
                let n = rows.len();
                while !rows.is_empty() {
                    let rest = rows.split_off(rows.len().min(STREAM_BATCH_ROWS));
                    if tx.send(Part::Rows(rows)).await.is_err() {
                        return;
                    }
                    rows = rest;
                }
                let _ = tx.send(Part::End { rows: n }).await;
            }
        }
    });
    rx
}

/// A streamed part as one NDJSON line's value: `{"header": {"plan"}}`,
/// `{"rows": [...]}`, `{"end": {"rows"}}`, or `{"error": ...}` (the
/// caller renders errors).
#[must_use]
pub fn part_json(p: &Part) -> Option<Value> {
    match p {
        Part::Header { plan } => Some(json!({"header": {"plan": plan}})),
        Part::Rows(rows) => Some(json!({"rows": rows.iter().map(row_json).collect::<Vec<_>>()})),
        Part::End { rows } => Some(json!({"end": {"rows": rows}})),
        Part::Error(_) => None,
    }
}

// ---- gRPC ----

use sprig_api::proto::v1 as pb;

use super::functions::{from_proto, to_proto};

/// `sprig.v1.Analytics` over gRPC: each message as the REST JSON.
#[derive(Clone)]
pub struct AnalyticsSvc(pub Arc<super::Core>);

impl AnalyticsSvc {
    async fn run<T>(
        &self,
        req: &Request<T>,
        rpc: &str,
        body: Value,
    ) -> std::result::Result<Value, Status> {
        metrics::counter!("sprig_api_requests_total", "service" => "sprig.v1.Analytics", "method" => rpc.to_owned(), "protocol" => "grpc").increment(1);
        let c = super::grpc::caller(&self.0, req).await?;
        call(&self.0.engine, &c, &format!("Analytics.{rpc}"), &body)
            .await
            .map_err(|e| super::grpc::st(&e))
    }
}

fn struct_json(s: Option<&prost_types::Struct>) -> Value {
    s.map_or(Value::Null, |s| {
        from_proto(&prost_types::Value {
            kind: Some(prost_types::value::Kind::StructValue(s.clone())),
        })
    })
}

fn json_struct(v: &Value) -> Option<prost_types::Struct> {
    match to_proto(v).kind {
        Some(prost_types::value::Kind::StructValue(s)) => Some(s),
        _ => None,
    }
}

fn list(v: Option<&Value>) -> Option<prost_types::ListValue> {
    match to_proto(v.unwrap_or(&Value::Null)).kind {
        Some(prost_types::value::Kind::ListValue(l)) => Some(l),
        _ => None,
    }
}

fn info_pb(v: &Value) -> pb::ModelInfo {
    pb::ModelInfo {
        database: s(v, "database"),
        model: s(v, "model"),
        definition: v.get("definition").and_then(json_struct),
        version: s(v, "version").parse().unwrap_or(0),
        state: s(v, "state"),
    }
}

fn query_json(q: &pb::AnalyticsQuery) -> Value {
    json!({
        "database": q.database, "model": q.model, "measures": q.measures,
        "dimensions": q.dimensions.iter().map(|d| json!({"dimension": d.dimension, "level": d.level})).collect::<Vec<_>>(),
        "filters": q.filters.iter().map(|f| json!({"dimension": f.dimension, "level": f.level, "op": f.op,
            "values": f.values.as_ref().map_or(Value::Array(Vec::new()), |l| from_proto(&prost_types::Value { kind: Some(prost_types::value::Kind::ListValue(l.clone())) }))})).collect::<Vec<_>>(),
        "totals": q.totals,
        "order": q.order.iter().map(|o| if o.measure.is_empty() {
            json!({"dimension": o.dimension, "level": o.level, "desc": o.desc})
        } else {
            json!({"measure": o.measure, "desc": o.desc})
        }).collect::<Vec<_>>(),
        "limit": q.limit, "attribution": q.attribution,
    })
}

fn query_pb(v: &Value) -> pb::AnalyticsQuery {
    let arr = |k: &str| {
        v.get(k)
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    };
    pb::AnalyticsQuery {
        database: s(v, "database"),
        model: s(v, "model"),
        measures: arr("measures")
            .iter()
            .filter_map(|x| x.as_str().map(str::to_owned))
            .collect(),
        dimensions: arr("dimensions")
            .iter()
            .map(|d| pb::LevelRef {
                dimension: s(d, "dimension"),
                level: s(d, "level"),
            })
            .collect(),
        filters: arr("filters")
            .iter()
            .map(|f| pb::AnalyticsFilter {
                dimension: s(f, "dimension"),
                level: s(f, "level"),
                op: s(f, "op"),
                values: list(f.get("values")),
            })
            .collect(),
        totals: v.get("totals").and_then(Value::as_bool).unwrap_or(false),
        order: arr("order")
            .iter()
            .map(|o| pb::AnalyticsOrder {
                measure: s(o, "measure"),
                dimension: s(o, "dimension"),
                level: s(o, "level"),
                desc: o.get("desc").and_then(Value::as_bool).unwrap_or(false),
            })
            .collect(),
        limit: v.get("limit").and_then(Value::as_u64).unwrap_or(0),
        attribution: s(v, "attribution"),
    }
}

#[tonic::async_trait]
impl pb::analytics_server::Analytics for AnalyticsSvc {
    async fn create_model(
        &self,
        req: Request<pb::ModelRequest>,
    ) -> std::result::Result<Response<pb::ModelInfo>, Status> {
        let r = req.get_ref();
        let body = json!({"database": r.database, "model": r.model, "definition": struct_json(r.definition.as_ref())});
        Ok(Response::new(info_pb(
            &self.run(&req, "CreateModel", body).await?,
        )))
    }

    async fn get_model(
        &self,
        req: Request<pb::GetModelRequest>,
    ) -> std::result::Result<Response<pb::ModelInfo>, Status> {
        let body = json!({"database": req.get_ref().database, "model": req.get_ref().model});
        Ok(Response::new(info_pb(
            &self.run(&req, "GetModel", body).await?,
        )))
    }

    async fn list_models(
        &self,
        req: Request<pb::ListModelsRequest>,
    ) -> std::result::Result<Response<pb::ListModelsResponse>, Status> {
        let body = json!({"database": req.get_ref().database});
        let v = self.run(&req, "ListModels", body).await?;
        Ok(Response::new(pb::ListModelsResponse {
            models: v
                .get("models")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(info_pb)
                .collect(),
        }))
    }

    async fn update_model(
        &self,
        req: Request<pb::ModelRequest>,
    ) -> std::result::Result<Response<pb::ModelInfo>, Status> {
        let r = req.get_ref();
        let body = json!({"database": r.database, "model": r.model, "definition": struct_json(r.definition.as_ref())});
        Ok(Response::new(info_pb(
            &self.run(&req, "UpdateModel", body).await?,
        )))
    }

    async fn drop_model(
        &self,
        req: Request<pb::GetModelRequest>,
    ) -> std::result::Result<Response<pb::DropModelResponse>, Status> {
        let body = json!({"database": req.get_ref().database, "model": req.get_ref().model});
        self.run(&req, "DropModel", body).await?;
        Ok(Response::new(pb::DropModelResponse { dropped: true }))
    }

    type QueryStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = std::result::Result<pb::AnalyticsResult, Status>> + Send>,
    >;

    async fn query(
        &self,
        req: Request<pb::AnalyticsQuery>,
    ) -> std::result::Result<Response<Self::QueryStream>, Status> {
        metrics::counter!("sprig_api_requests_total", "service" => "sprig.v1.Analytics", "method" => "Query", "protocol" => "grpc").increment(1);
        let c = super::grpc::caller(&self.0, &req).await?;
        let body = query_json(req.get_ref());
        let rx = query_stream(Arc::clone(&self.0.engine), c, body);
        let stream = futures::stream::unfold(Some(rx), |rx| async move {
            let mut rx = rx?;
            let item = match rx.recv().await? {
                Part::Header { plan } => Ok(pb::AnalyticsResult {
                    rows: Vec::new(),
                    plan: plan.to_owned(),
                }),
                Part::Rows(rows) => Ok(pb::AnalyticsResult {
                    rows: rows
                        .iter()
                        .map(|r| {
                            let v = row_json(r);
                            pb::AnalyticsRow {
                                keys: list(v.get("keys")),
                                values: list(v.get("values")),
                                grouping: r.grouping,
                            }
                        })
                        .collect(),
                    plan: String::new(),
                }),
                Part::End { .. } => return None,
                Part::Error(e) => return Some((Err(super::grpc::st(&e)), None)),
            };
            Some((item, Some(rx)))
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn explain(
        &self,
        req: Request<pb::AnalyticsQuery>,
    ) -> std::result::Result<Response<pb::AnalyticsExplain>, Status> {
        let body = query_json(req.get_ref());
        let v = self.run(&req, "Explain", body).await?;
        Ok(Response::new(pb::AnalyticsExplain {
            plan: s(&v, "plan"),
            sql: v
                .get("sql")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|x| x.as_str().map(str::to_owned))
                .collect(),
        }))
    }

    async fn drill(
        &self,
        req: Request<pb::DrillRequest>,
    ) -> std::result::Result<Response<pb::AnalyticsQuery>, Status> {
        let r = req.get_ref();
        let body = json!({"database": r.database, "model": r.model,
            "query": r.query.as_ref().map_or(Value::Null, query_json), "dimension": r.dimension,
            "member": r.member.as_ref().map_or(Value::Null, from_proto), "up": r.up});
        let v = self.run(&req, "Drill", body).await?;
        Ok(Response::new(query_pb(&v)))
    }
}
