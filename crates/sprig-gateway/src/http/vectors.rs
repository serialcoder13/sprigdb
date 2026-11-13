//! `sprig.v1.Vectors` (chapter 34, "Vectors API"; chapter 37): standalone
//! vector indexes over REST (`/v1/databases/{db}/vectorIndexes...`) and
//! gRPC.
//!
//! A call opens the caller's own executor session, as the key-value API
//! does, so privileges, name virtualization and the router apply; the
//! work is `sprig-vector`'s. An `ann_mode = auto` index gets its HNSW graph
//! in the background once it holds `vector.ann_min_rows` rows, kept only
//! when its recall at 10 reaches `vector.min_recall` (chapter 34,
//! "Automatic vector indexes").

use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use base64::Engine as _;
use serde_json::{Value, json};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_geo::region::Region;
use sprig_vector::index::{self, AnnMode, IndexDef, Similarity};
use sprig_vector::items::{self, Item};
use sprig_vector::search::{GeoFilter, SearchSpec, auto_ann, search};
use tonic::{Request, Response, Status};

use super::sql::{Caller, SqlEngine};
use crate::redis::SqlSession;

/// `geo.max_vertices`.
const MAX_VERTICES: usize = 100_000;

fn s(v: &Value, k: &str) -> String {
    v.get(k).and_then(Value::as_str).unwrap_or("").to_owned()
}

fn u(v: &Value, k: &str) -> u64 {
    match v.get(k) {
        Some(Value::Number(n)) => n.as_u64().unwrap_or(0),
        Some(Value::String(s)) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

fn bad(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::AdminRequestInvalid, m)
}

fn b64(s: &str) -> Result<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| bad(format!("an id isn't base64: {e}")))
}

#[allow(clippy::cast_possible_truncation)] // vectors are FLOAT32
fn floats(v: Option<&Value>) -> Result<Vec<f32>> {
    v.and_then(Value::as_array).map_or_else(
        || Ok(Vec::new()),
        |a| {
            a.iter()
                .map(|x| {
                    x.as_f64()
                        .map(|f| f as f32)
                        .ok_or_else(|| bad("a vector holds a non-number"))
                })
                .collect()
        },
    )
}

fn lat_lng(v: Option<&Value>) -> Option<(f64, f64)> {
    let v = v?;
    Some((v.get("latitude")?.as_f64()?, v.get("longitude")?.as_f64()?))
}

/// A `GeoFilter` message as a region.
///
/// # Errors
/// 39765 for a malformed or out-of-range region; 39768 past the vertices.
pub fn region_of(g: &Value) -> Result<Option<Region>> {
    let poly = g.get("polygon").and_then(Value::as_array);
    let r = if let Some(p) = poly.filter(|p| !p.is_empty()) {
        let v: Option<Vec<(f64, f64)>> = p.iter().map(|x| lat_lng(Some(x))).collect();
        Region::Polygon(v.ok_or_else(|| {
            SprigError::new(
                SprigCode::GeoInvalid,
                "a polygon vertex needs latitude and longitude",
            )
        })?)
    } else if let (Some(sw), Some(ne)) =
        (lat_lng(g.get("south_west")), lat_lng(g.get("north_east")))
    {
        Region::Box {
            south: sw.0,
            west: sw.1,
            north: ne.0,
            east: ne.1,
        }
    } else if let Some(c) = lat_lng(g.get("center")) {
        Region::Circle {
            lat: c.0,
            lng: c.1,
            radius_m: g.get("radius_m").and_then(Value::as_f64).unwrap_or(0.0),
        }
    } else {
        return Ok(None);
    };
    r.validate(MAX_VERTICES).map_err(|e| {
        let code = if e.0.contains("vertices;") {
            SprigCode::GeoTooComplex
        } else {
            SprigCode::GeoInvalid
        };
        SprigError::new(code, e.0)
    })?;
    Ok(Some(r))
}

fn index_json(d: &IndexDef, rows: u64) -> Value {
    json!({
        "database": d.database, "index": d.name, "dimensions": d.dimensions,
        "similarity": d.similarity.name(), "m": d.m, "ann_mode": d.ann_mode.name(),
        "ann_state": d.ann_state.name(), "geo": d.geo, "recall": d.recall.unwrap_or(0.0),
        "rows": rows.to_string(),
    })
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

/// Indexes whose automatic graph is being built on this gateway.
fn building() -> &'static Mutex<HashSet<String>> {
    static B: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    B.get_or_init(|| Mutex::new(HashSet::new()))
}

/// What chapter 52's gate said about an automatic graph.
enum AnnGate {
    /// Build it; report with the ticket when an item tracks it.
    Go(Option<Value>),
    /// Not now.
    Skip,
}

/// When a held graph asks the gate again.
fn next_ask() -> &'static Mutex<std::collections::HashMap<String, std::time::Instant>> {
    static N: OnceLock<Mutex<std::collections::HashMap<String, std::time::Instant>>> =
        OnceLock::new();
    N.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

/// Chapter 52 for `vector.auto_ann`: `on` builds as before; `off` never;
/// `approve` and `recommend` ask the controller's gate once the index is
/// due (`vector.ann_min_rows`), at most once a minute per index while it
/// waits. Unreachable approvals hold the graph.
async fn ann_gate(
    sql: &mut SqlSession,
    def: &IndexDef,
    settings: &sprig_vector::plan::Settings,
    mode: &str,
    tenant: Option<sprig_common::ids::TenantId>,
    key: &str,
) -> AnnGate {
    match mode {
        "on" | "" => return AnnGate::Go(None),
        "off" => return AnnGate::Skip,
        _ => {}
    }
    if next_ask()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(key)
        .is_some_and(|at| std::time::Instant::now() < *at)
    {
        return AnnGate::Skip;
    }
    let Ok(rows) = index::rows(sql, def).await else {
        return AnnGate::Skip;
    };
    if rows < settings.ann_min_rows {
        return AnnGate::Skip;
    }
    next_ask()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(
            key.to_owned(),
            std::time::Instant::now() + std::time::Duration::from_secs(60),
        );
    let (Some(t), Some(admin)) = (tenant, crate::tenant::rec_sql::admin()) else {
        return AnnGate::Skip;
    };
    let body = json!({
        "op": "check",
        "kind": "vector.ann",
        "tenant": t.0,
        "db": def.database,
        "target": format!("{}/vector/{}", def.database, def.name),
        "action": format!(
            "build the HNSW graph of vector index {}.{} (M {}), kept if recall reaches vector.min_recall {}",
            def.database, def.name, def.m, settings.min_recall
        ),
        "machine": {"database": def.database, "index": def.name, "m": def.m},
        "evidence": {"rows": rows, "dimensions": def.dimensions, "ann_min_rows": settings.ann_min_rows},
        "summary": format!(
            "vector index {}.{} holds {rows} rows: build its ANN graph",
            def.database, def.name
        ),
        "score": f64::from(u32::try_from(rows).unwrap_or(u32::MAX)),
    });
    let reply = admin
        .forward_internal(
            &hyper::Method::POST,
            "/admin/v1/recommendations:gate",
            bytes::Bytes::from(body.to_string()),
        )
        .await;
    match reply {
        Ok(r) if r.status < 300 => {
            let v: Value = serde_json::from_slice(&r.body).unwrap_or_default();
            if v.get("go").and_then(Value::as_bool) == Some(true) {
                AnnGate::Go(v.get("ticket").cloned().filter(|t| !t.is_null()))
            } else {
                let why = v.get("why").and_then(Value::as_str).unwrap_or("");
                tracing::info!(index = %def.name, why, "automatic vector graph held for approval");
                AnnGate::Skip
            }
        }
        Ok(r) => {
            tracing::warn!(index = %def.name, status = r.status, "automatic vector graph: the approvals gate refused the check");
            AnnGate::Skip
        }
        Err(e) => {
            tracing::warn!(index = %def.name, error = %e, "automatic vector graph: approvals unreachable; held");
            AnnGate::Skip
        }
    }
}

/// Starts the automatic graph of an `auto` index in the background, once.
fn maybe_auto(engine: &Arc<SqlEngine>, caller: &Caller, def: &IndexDef) {
    if def.ann_mode != AnnMode::Auto || def.ann_state == index::AnnState::Ready {
        return;
    }
    let key = format!(
        "{}/{}/{}",
        caller.principal.tenant.map_or(0, |t| t.0),
        def.database,
        def.name
    );
    if !building()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(key.clone())
    {
        return;
    }
    let Ok(mut sql) = open(engine, caller) else {
        building()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&key);
        return;
    };
    let mut def = def.clone();
    let settings = super::model_settings::vector_for(engine, caller, &def.database);
    // Chapter 52: `vector.auto_ann` holds the graph for an approval.
    let mode = super::model_settings::vector_auto_ann(engine, caller, &def.database);
    let tenant = caller.principal.tenant;
    let (engine, caller) = (Arc::clone(engine), caller.clone());
    tokio::spawn(async move {
        // `on` builds at once: once the index is due, read the mode again
        // from a catalog at least as new as the directory's, so an
        // `approve` set a moment ago holds the graph (approvals round 3).
        // Not due yet: nothing to build; the next write asks again.
        let mode = if mode == "on" {
            if index::rows(&mut sql, &def)
                .await
                .is_ok_and(|n| n < settings.ann_min_rows)
            {
                building()
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&key);
                return;
            }
            super::model_settings::vector_auto_ann_fresh(&engine, &caller, &def.database).await
        } else {
            mode
        };
        let ticket = match ann_gate(&mut sql, &def, &settings, &mode, tenant, &key).await {
            AnnGate::Go(t) => t,
            AnnGate::Skip => {
                building()
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&key);
                return;
            }
        };
        let r = auto_ann(&mut sql, &mut def, &settings, 20, 10).await;
        match &r {
            Ok(Some(r)) => {
                tracing::info!(index = %def.name, recall = r, kept = def.ann_state == index::AnnState::Ready, "automatic vector graph validated");
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(index = %def.name, error = %e, "automatic vector graph failed");
            }
        }
        if let Some(t) = ticket {
            let (ok, detail) = match &r {
                Ok(Some(recall)) if def.ann_state == index::AnnState::Ready => {
                    (true, format!("graph built, recall {recall:.3}"))
                }
                Ok(Some(recall)) => (
                    false,
                    format!("graph dropped: recall {recall:.3} below vector.min_recall"),
                ),
                Ok(None) => (false, "nothing was due".to_owned()),
                Err(e) => (false, e.message.clone()),
            };
            let body = json!({"op": "finished", "ticket": t, "ok": ok, "detail": detail});
            if let Some(admin) = crate::tenant::rec_sql::admin() {
                let _ = admin
                    .forward_internal(
                        &hyper::Method::POST,
                        "/admin/v1/recommendations:gate",
                        bytes::Bytes::from(body.to_string()),
                    )
                    .await;
            }
        }
        building()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&key);
    });
}

/// Runs `Vectors.<rpc>` for `caller` with the request `body` (proto3 JSON,
/// path variables merged); the reply as proto3 JSON.
///
/// # Errors
/// 39735-39740, 39765-39768 and the backend's.
#[allow(clippy::too_many_lines)] // one arm per RPC
pub async fn call(
    engine: &Arc<SqlEngine>,
    caller: &Caller,
    rpc: &str,
    body: &Value,
) -> Result<Value> {
    let database = s(body, "database");
    let name = s(body, "index");
    // Live `vector.*` settings (R6 round 3), at the tenant's and the
    // database's scopes (R6 round 4).
    let settings = super::model_settings::vector_for(engine, caller, &database);
    let mut sql = open(engine, caller)?;
    match rpc {
        "Vectors.CreateIndex" => {
            let m = u(body, "m");
            let def = IndexDef::new(
                &database,
                &name,
                u32::try_from(u(body, "dimensions")).unwrap_or(0),
                Similarity::parse(&s(body, "similarity"))?,
                if m == 0 {
                    settings.default_m
                } else {
                    u8::try_from(m).unwrap_or(200)
                },
                AnnMode::parse(&s(body, "ann_mode"))?,
                body.get("geo").and_then(Value::as_bool).unwrap_or(false),
            )?;
            let def = index::create(&mut sql, def).await?;
            Ok(index_json(&def, 0))
        }
        "Vectors.GetIndex" => {
            let def = index::describe(&mut sql, &database, &name).await?;
            let rows = index::rows(&mut sql, &def).await?;
            Ok(index_json(&def, rows))
        }
        "Vectors.DropIndex" => {
            index::drop(&mut sql, &database, &name).await?;
            Ok(json!({"dropped": true}))
        }
        "Vectors.Upsert" => {
            let def = index::describe(&mut sql, &database, &name).await?;
            let mut list = Vec::new();
            for it in body
                .get("items")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let metadata = match it.get("metadata") {
                    Some(Value::String(t)) if !t.is_empty() => Some(
                        serde_json::from_str::<Value>(t)
                            .map_err(|e| bad(format!("metadata isn't JSON: {e}")))?,
                    ),
                    Some(o @ Value::Object(_)) => Some(o.clone()),
                    _ => None,
                };
                list.push(Item {
                    id: b64(&s(it, "id"))?,
                    vector: floats(it.get("vector"))?,
                    metadata,
                    location: lat_lng(it.get("location")),
                });
            }
            let n = items::upsert(&mut sql, &def, &list).await?;
            maybe_auto(engine, caller, &def);
            Ok(json!({"upserted": n.to_string()}))
        }
        "Vectors.Delete" => {
            let def = index::describe(&mut sql, &database, &name).await?;
            let ids: Vec<Vec<u8>> = body
                .get("ids")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(|x| b64(x.as_str().unwrap_or("")))
                .collect::<Result<_>>()?;
            let n = items::delete(&mut sql, &def, &ids).await?;
            Ok(json!({"deleted": n.to_string()}))
        }
        "Vectors.Search" => {
            let def = index::describe(&mut sql, &database, &name).await?;
            if !s(body, "type").is_empty() {
                return Err(SprigError::new(
                    SprigCode::VectorFilterNotIndexed,
                    "type filters aren't offered on vector indexes in this release",
                ));
            }
            if body.get("text").is_some_and(|t| !s(t, "query").is_empty()) {
                return Err(SprigError::new(
                    SprigCode::VectorFilterNotIndexed,
                    "hybrid search with full text isn't offered in this release",
                ));
            }
            let filter = match s(body, "filter") {
                f if f.trim().is_empty() => None,
                f => {
                    let v: Value = serde_json::from_str(&f).map_err(|e| {
                        SprigError::new(
                            SprigCode::VectorFilterNotIndexed,
                            format!("filter isn't JSON: {e}"),
                        )
                    })?;
                    match bson::Bson::try_from(v) {
                        Ok(bson::Bson::Document(d)) => Some(d),
                        _ => {
                            return Err(SprigError::new(
                                SprigCode::VectorFilterNotIndexed,
                                "filter must be a JSON object",
                            ));
                        }
                    }
                }
            };
            let geo = match body.get("geo") {
                Some(g) if g.is_object() => {
                    region_of(g)?.map(|r| Box::new(r) as Box<dyn GeoFilter>)
                }
                _ => None,
            };
            let nc = u(body, "num_candidates");
            let spec = SearchSpec {
                query: floats(body.get("query"))?,
                k: u32::try_from(u(body, "k")).unwrap_or(u32::MAX),
                num_candidates: (nc > 0).then(|| u32::try_from(nc).unwrap_or(u32::MAX)),
                exact: body.get("exact").and_then(Value::as_bool).unwrap_or(false),
                filter,
                geo,
            };
            let t = std::time::Instant::now();
            let out = search(&mut sql, &def, &spec, &settings).await?;
            metrics::counter!("sprig_vector_searches_total", "plan" => out.plan.name())
                .increment(1);
            metrics::histogram!("sprig_vector_search_duration_seconds", "plan" => out.plan.name())
                .record(t.elapsed().as_secs_f64());
            if out.requeries > 0 {
                metrics::counter!("sprig_vector_overfetch_retries_total")
                    .increment(u64::from(out.requeries));
            }
            maybe_auto(engine, caller, &def);
            let hits: Vec<Value> = out
                .hits
                .iter()
                .map(|h| {
                    let mut o = json!({
                        "id": base64::engine::general_purpose::STANDARD.encode(&h.id),
                        "distance": h.distance, "score": h.score,
                        "metadata": h.metadata.as_ref().map(Value::to_string).unwrap_or_default(),
                    });
                    if let Some((la, lo)) = h.location {
                        o["location"] = json!({"latitude": la, "longitude": lo});
                    }
                    o
                })
                .collect();
            Ok(json!({"hits": hits, "plan": out.plan.name()}))
        }
        other => Err(bad(format!("no vector call {other}"))),
    }
}

// ---- gRPC ----

use sprig_api::proto::v1 as pb;

/// `sprig.v1.Vectors` over gRPC: each message as the REST JSON.
#[derive(Clone)]
pub struct VectorsSvc(pub Arc<super::Core>);

impl VectorsSvc {
    async fn run<T>(
        &self,
        req: &Request<T>,
        rpc: &str,
        body: Value,
    ) -> std::result::Result<Value, Status> {
        metrics::counter!("sprig_api_requests_total", "service" => "sprig.v1.Vectors", "method" => rpc.to_owned(), "protocol" => "grpc").increment(1);
        let c = super::grpc::caller(&self.0, req).await?;
        call(&self.0.engine, &c, &format!("Vectors.{rpc}"), &body)
            .await
            .map_err(|e| super::grpc::st(&e))
    }
}

fn ll(l: Option<&pb::LatLng>) -> Value {
    l.map_or(
        Value::Null,
        |l| json!({"latitude": l.latitude, "longitude": l.longitude}),
    )
}

fn index_pb(v: &Value) -> pb::VectorIndex {
    pb::VectorIndex {
        database: s(v, "database"),
        index: s(v, "index"),
        dimensions: u32::try_from(u(v, "dimensions")).unwrap_or(0),
        similarity: s(v, "similarity"),
        m: u32::try_from(u(v, "m")).unwrap_or(0),
        ann_mode: s(v, "ann_mode"),
        ann_state: s(v, "ann_state"),
        geo: v.get("geo").and_then(Value::as_bool).unwrap_or(false),
        recall: v.get("recall").and_then(Value::as_f64).unwrap_or(0.0),
        rows: u(v, "rows"),
    }
}

#[tonic::async_trait]
impl pb::vectors_server::Vectors for VectorsSvc {
    async fn create_index(
        &self,
        req: Request<pb::CreateVectorIndexRequest>,
    ) -> std::result::Result<Response<pb::VectorIndex>, Status> {
        let r = req.get_ref();
        let body = json!({"database": r.database, "index": r.index, "dimensions": r.dimensions,
            "similarity": r.similarity, "m": r.m, "ann_mode": r.ann_mode, "geo": r.geo});
        let v = self.run(&req, "CreateIndex", body).await?;
        Ok(Response::new(index_pb(&v)))
    }

    async fn get_index(
        &self,
        req: Request<pb::GetVectorIndexRequest>,
    ) -> std::result::Result<Response<pb::VectorIndex>, Status> {
        let body = json!({"database": req.get_ref().database, "index": req.get_ref().index});
        let v = self.run(&req, "GetIndex", body).await?;
        Ok(Response::new(index_pb(&v)))
    }

    async fn drop_index(
        &self,
        req: Request<pb::GetVectorIndexRequest>,
    ) -> std::result::Result<Response<pb::DropVectorIndexResponse>, Status> {
        let body = json!({"database": req.get_ref().database, "index": req.get_ref().index});
        self.run(&req, "DropIndex", body).await?;
        Ok(Response::new(pb::DropVectorIndexResponse { dropped: true }))
    }

    async fn upsert(
        &self,
        req: Request<pb::UpsertVectorsRequest>,
    ) -> std::result::Result<Response<pb::UpsertVectorsResponse>, Status> {
        let r = req.get_ref();
        let items: Vec<Value> = r
            .items
            .iter()
            .map(|i| {
                json!({"id": base64::engine::general_purpose::STANDARD.encode(&i.id), "vector": i.vector,
                    "metadata": i.metadata, "location": ll(i.location.as_ref())})
            })
            .collect();
        let body = json!({"database": r.database, "index": r.index, "items": items});
        let v = self.run(&req, "Upsert", body).await?;
        Ok(Response::new(pb::UpsertVectorsResponse {
            upserted: u(&v, "upserted"),
        }))
    }

    async fn delete(
        &self,
        req: Request<pb::DeleteVectorsRequest>,
    ) -> std::result::Result<Response<pb::DeleteVectorsResponse>, Status> {
        let r = req.get_ref();
        let ids: Vec<String> = r
            .ids
            .iter()
            .map(|i| base64::engine::general_purpose::STANDARD.encode(i))
            .collect();
        let body = json!({"database": r.database, "index": r.index, "ids": ids});
        let v = self.run(&req, "Delete", body).await?;
        Ok(Response::new(pb::DeleteVectorsResponse {
            deleted: u(&v, "deleted"),
        }))
    }

    #[allow(clippy::cast_possible_truncation)] // the proto's distances are floats
    async fn search(
        &self,
        req: Request<pb::SearchVectorsRequest>,
    ) -> std::result::Result<Response<pb::SearchVectorsResponse>, Status> {
        let r = req.get_ref();
        let mut body = json!({"database": r.database, "index": r.index, "query": r.query, "k": r.k,
            "num_candidates": r.num_candidates, "exact": r.exact, "filter": r.filter, "type": r.r#type});
        if let Some(g) = &r.geo {
            body["geo"] = json!({"center": ll(g.center.as_ref()), "radius_m": g.radius_m,
                "south_west": ll(g.south_west.as_ref()), "north_east": ll(g.north_east.as_ref()),
                "polygon": g.polygon.iter().map(|p| ll(Some(p))).collect::<Vec<_>>()});
        }
        if let Some(t) = &r.text {
            body["text"] = json!({"query": t.query});
        }
        let v = self.run(&req, "Search", body).await?;
        let hits = v
            .get("hits")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|h| pb::VectorHit {
                id: base64::engine::general_purpose::STANDARD
                    .decode(s(h, "id"))
                    .unwrap_or_default(),
                distance: h.get("distance").and_then(Value::as_f64).unwrap_or(0.0) as f32,
                score: h.get("score").and_then(Value::as_f64).unwrap_or(0.0) as f32,
                metadata: s(h, "metadata"),
                location: lat_lng(h.get("location")).map(|(la, lo)| pb::LatLng {
                    latitude: la,
                    longitude: lo,
                }),
            })
            .collect();
        Ok(Response::new(pb::SearchVectorsResponse {
            hits,
            plan: s(&v, "plan"),
        }))
    }
}
