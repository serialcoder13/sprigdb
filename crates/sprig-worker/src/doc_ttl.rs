//! `doc_ttl` (chapter 31, "Index kinds, origins and TTL"; wireup, 13 Nov
//! 2026): MongoDB's TTL monitor. One job per DC (the controller's
//! `doc_ttl` loop), so each collection is expired by one task whatever the
//! number of gateways.
//!
//! ```text
//! every doc.ttl_interval_seconds (read at each pass):
//!   for each cluster of the DC:
//!     catalogs t<tenant>_sprig with doc_indexes, whose catalog shard map
//!       here is the tenant's home (role home) on this cluster
//!     each one's collections with a ready TTL index (expire_seconds)
//!   for each such collection, its shard schemas t<tenant>_d<db>_g<g>_s<s>
//!     holding its table on any cluster, kept only where the group's shard
//!     map (role home) has shard s on that cluster, serving; a group without
//!     a map of its own (a database's default group, R0) where the
//!     catalog's map puts the catalog
//!   round robin over the (collection, shard) items: one batch each turn,
//!     doc.multi_write_batch documents (the tenant's value), deleted where
//!     the field holds a date (or an array with a date) before
//!     now - expireAfterSeconds (sprig_doc::engine::ttl)
//!   each tenant's deletes stay under doc.ttl_tenant_deletes_per_second
//! ```
//!
//! **Moves.** A shard is expired only where its shard map places it, so a
//! mover's target copy is never touched before its cutover. A delete on
//! the source before the fence is in the binlog the mover replays; after
//! the fence the source's tables are renamed away (1146), and the next
//! pass finds the shard on its new cluster. A held range is skipped for
//! that pass.
//!
//! **Change streams.** The deletes are ordinary row deletes, so the feed
//! renders them as `delete` events, as MongoDB's TTL deletes are.
//!
//! As built, the job runs as `sprig_ctl` straight on the shard schemas
//! (like `kv_sweep`), not through a gateway: TTL needs no user's
//! privileges, and the shard maps are the gateways' own routing.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bson::Document;
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use sprig_catalog::placement::{MapRole, RangeState, ShardMap};
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::names::SchemaName;
use sprig_controller::catalogs::ClusterPools;
use sprig_controller::dispatch::JobClass;
use sprig_controller::doc_ttl_loop::{
    MULTI_WRITE_BATCH, TTL_INTERVAL_SECONDS, TTL_TENANT_DELETES_PER_SECOND,
};
use sprig_doc::engine::{Done, Engine, EngineConfig, Ns, Rows, SqlError, SqlRunner};
use sprig_doc::meta::{CollMeta, IndexDef};
use sprig_meta::{Consistency, MetaStore, Record};

use crate::runtime::{JobCtx, JobHandler};

pub use sprig_controller::doc_ttl_loop::JOB_KIND;

/// A `sprig_ctl` connection as the document engine's SQL.
struct Ctl(mysql_async::Conn);

fn sql_err(e: &mysql_async::Error) -> SqlError {
    match e {
        mysql_async::Error::Server(s) => SqlError {
            errno: Some(s.code),
            message: s.message.clone(),
            mongo: None,
        },
        other => SqlError {
            errno: None,
            message: other.to_string(),
            mongo: None,
        },
    }
}

#[async_trait]
impl SqlRunner for Ctl {
    async fn query(&mut self, sql: &str) -> Result<Rows, SqlError> {
        let rows: Vec<mysql_async::Row> = self.0.query(sql).await.map_err(|e| sql_err(&e))?;
        Ok(rows
            .into_iter()
            .map(|r| {
                r.unwrap()
                    .into_iter()
                    .map(|v| match v {
                        mysql_async::Value::NULL => None,
                        mysql_async::Value::Bytes(b) => Some(b),
                        other => Some(other.as_sql(true).into_bytes()),
                    })
                    .collect()
            })
            .collect())
    }

    async fn exec(&mut self, sql: &str) -> Result<Done, SqlError> {
        self.0.query_drop(sql).await.map_err(|e| sql_err(&e))?;
        Ok(Done {
            affected: self.0.affected_rows(),
            info: String::new(),
        })
    }
}

/// What the job needs.
#[derive(Clone)]
pub struct DocTtlEnv {
    /// This DC's tier (shard maps).
    pub dc: Arc<dyn MetaStore>,
    /// `sprig_ctl` pools per cluster.
    pub pools: Arc<dyn ClusterPools>,
    /// The worker's live settings.
    pub settings: crate::settings::WorkerSettings,
}

impl std::fmt::Debug for DocTtlEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocTtlEnv").finish_non_exhaustive()
    }
}

/// See the module docs.
#[derive(Debug, Clone)]
pub struct DocTtl(pub DocTtlEnv);

/// One TTL index of one collection.
#[derive(Debug, Clone)]
struct TtlColl {
    tenant: TenantId,
    db: u32,
    meta: CollMeta,
    path: String,
    expire: i64,
}

/// One shard of a TTL collection, and where the next batch starts.
#[derive(Debug)]
struct Item {
    coll: Arc<TtlColl>,
    cluster: ClusterId,
    schema: String,
    after: Option<Vec<u8>>,
    done: bool,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// A group's shard map in this DC's tier, if this DC is its home.
async fn home_map(
    dc: &dyn MetaStore,
    tenant: TenantId,
    group: u32,
) -> Result<Option<ShardMap>, String> {
    let key = dc
        .tier()
        .key(&format!("shardmaps/{tenant}/{group}"))
        .map_err(|e| e.to_string())?;
    let Some(kv) = dc
        .get(&key, Consistency::Linearizable)
        .await
        .map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    let m = Record::<ShardMap>::decode(&kv.value)
        .map_err(|e| e.to_string())?
        .body;
    Ok((m.role == MapRole::Home).then_some(m))
}

/// Every ready index of the collections in catalog `q` (quoted) that have
/// a TTL index, by collection.
async fn ttl_indexes(
    conn: &mut mysql_async::Conn,
    q: &str,
) -> Result<BTreeMap<u64, Vec<IndexDef>>, mysql_async::Error> {
    let idx: Vec<mysql_async::Row> = conn
        .query(format!(
            "SELECT i.collection_id, i.index_id, i.name, i.key_pattern, i.expire_seconds, \
             i.mariadb_name, i.is_unique, i.is_sparse, i.origin FROM {q}.doc_indexes i \
             WHERE i.state = 'ready' AND i.collection_id IN \
             (SELECT collection_id FROM {q}.doc_indexes WHERE expire_seconds IS NOT NULL) \
             ORDER BY i.collection_id, i.index_id"
        ))
        .await?;
    let mut by_coll: BTreeMap<u64, Vec<IndexDef>> = BTreeMap::new();
    for r in idx {
        let Ok((cid, id, name, key, expire, mariadb, uniq, sparse, origin)) =
            mysql_async::from_row_opt::<(
                u64,
                u32,
                String,
                Vec<u8>,
                Option<i64>,
                Option<String>,
                i8,
                i8,
                String,
            )>(r)
        else {
            continue;
        };
        let Ok(k) = bson::from_slice::<Document>(&key) else {
            continue;
        };
        let key = k
            .iter()
            .map(|(p, v)| {
                let d = sprig_doc::filter::as_i64(v).unwrap_or(1);
                (p.clone(), if d < 0 { -1 } else { 1 })
            })
            .collect();
        let mariadb = mariadb.filter(|m| *m != format!("sprig_dix_{id}"));
        by_coll.entry(cid).or_default().push(IndexDef {
            id,
            name,
            key,
            unique: uniq != 0,
            sparse: sparse != 0,
            expire_after: expire,
            origin,
            extra: Document::new(),
            mariadb,
            auto_index_id: None,
        });
    }
    Ok(by_coll)
}

/// The TTL collections in catalog `schema` (see the module docs).
async fn ttl_collections(
    conn: &mut mysql_async::Conn,
    schema: &str,
    tenant: TenantId,
) -> Result<Vec<TtlColl>, mysql_async::Error> {
    let q = sprig_common::names::quote(schema);
    let mut by_coll = ttl_indexes(conn, &q).await?;
    if by_coll.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<String> = by_coll.keys().map(ToString::to_string).collect();
    let rows: Vec<mysql_async::Row> = conn
        .query(format!(
            "SELECT collection_id, db_id, name, table_name, coll_uuid, \
             CAST(array_paths AS CHAR), array_paths_full, options, next_index \
             FROM {q}.collections WHERE state = 'ready' AND collection_id IN ({})",
            ids.join(", ")
        ))
        .await?;
    let mut out = Vec::new();
    for r in rows {
        let Ok((cid, db, name, table, uuid, paths, full, options, next)) =
            mysql_async::from_row_opt::<(
                u64,
                u32,
                String,
                String,
                Vec<u8>,
                Option<String>,
                i8,
                Option<Vec<u8>>,
                u32,
            )>(r)
        else {
            continue;
        };
        let indexes = by_coll.remove(&cid).unwrap_or_default();
        let array_paths: BTreeSet<String> = paths
            .and_then(|p| serde_json::from_str::<Vec<String>>(&p).ok())
            .unwrap_or_default()
            .into_iter()
            .collect();
        let options = options
            .and_then(|o| bson::from_slice::<Document>(&o).ok())
            .unwrap_or_default();
        // Time series collections aren't built; a view has no table.
        if options.contains_key("timeseries") || options.contains_key("viewOn") {
            continue;
        }
        let meta = CollMeta {
            name,
            table,
            uuid: uuid.try_into().unwrap_or([0; 16]),
            array_paths,
            arrays_full: full != 0,
            pending_paths: BTreeSet::new(),
            indexes,
            options,
            next_index: next,
        };
        for i in &meta.indexes {
            if let Some((path, expire)) = sprig_doc::engine::ttl::ttl_of(i) {
                out.push(TtlColl {
                    tenant,
                    db,
                    meta: meta.clone(),
                    path: path.to_owned(),
                    expire,
                });
            }
        }
    }
    Ok(out)
}

/// Per-tenant pacing: deletes so far this pass and since when.
#[derive(Debug)]
struct Pace {
    started: Instant,
    deleted: u64,
}

impl DocTtl {
    /// Every TTL collection of tenants homed here, by cluster scan.
    async fn discover(&self, clusters: &[ClusterId]) -> Vec<TtlColl> {
        let mut out = Vec::new();
        let mut seen: BTreeSet<u64> = BTreeSet::new();
        for cluster in clusters {
            let Ok(pool) = self.0.pools.writer(cluster).await else {
                continue;
            };
            let Ok(mut conn) = pool.get_conn().await else {
                continue;
            };
            let schemas: Vec<String> = match conn
                .query(
                    "SELECT table_schema FROM information_schema.tables \
                     WHERE table_name = 'doc_indexes' AND table_schema REGEXP '^t[0-9]+_sprig$'",
                )
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    tracing::debug!(%cluster, error = %e, "TTL: catalogs not listed");
                    continue;
                }
            };
            for schema in schemas {
                let Ok(SchemaName::Catalog { tenant }) = schema.parse::<SchemaName>() else {
                    continue;
                };
                // The tenant's catalog here, homed in this DC (not a
                // mover's copy, a retained source or a standby).
                match home_map(self.0.dc.as_ref(), tenant, 0).await {
                    Ok(Some(m)) if m.catalog_cluster() == Some(cluster) => {}
                    _ => continue,
                }
                if !seen.insert(tenant.0) {
                    continue;
                }
                match ttl_collections(&mut conn, &schema, tenant).await {
                    Ok(c) => out.extend(c),
                    Err(e) => {
                        tracing::debug!(%tenant, error = %e, "TTL: collections not read");
                    }
                }
            }
        }
        out
    }

    /// The shards of `c` where their maps place them, serving.
    async fn shards(&self, c: &Arc<TtlColl>, clusters: &[ClusterId]) -> Vec<Item> {
        let mut out = Vec::new();
        let mut maps: HashMap<u32, Option<ShardMap>> = HashMap::new();
        let like = format!("t{}\\_d{}\\_g%\\_s%", c.tenant.0, c.db);
        for cluster in clusters {
            let Ok(pool) = self.0.pools.writer(cluster).await else {
                continue;
            };
            let Ok(mut conn) = pool.get_conn().await else {
                continue;
            };
            let found: Vec<String> = conn
                .exec(
                    "SELECT table_schema FROM information_schema.tables \
                     WHERE table_name = ? AND table_schema LIKE ?",
                    (c.meta.table.as_str(), like.as_str()),
                )
                .await
                .unwrap_or_default();
            for schema in found {
                let Ok(SchemaName::Shard { group, shard, .. }) = schema.parse::<SchemaName>()
                else {
                    continue;
                };
                if let std::collections::hash_map::Entry::Vacant(v) = maps.entry(group.0) {
                    v.insert(
                        home_map(self.0.dc.as_ref(), c.tenant, group.0)
                            .await
                            .ok()
                            .flatten(),
                    );
                }
                // A group with its own map (R1's sharded groups) by its
                // ranges; a database's default group lives with the
                // catalog, so the catalog's map (group 0) places it.
                let own = maps.get(&group.0).and_then(Option::as_ref);
                let serving = match own {
                    Some(m) if group.0 != 0 => m.ranges.iter().any(|r| {
                        r.shard_id == shard.0
                            && &r.cluster_id == cluster
                            && r.state == RangeState::Serving
                    }),
                    _ => {
                        if let std::collections::hash_map::Entry::Vacant(v) = maps.entry(0) {
                            v.insert(
                                home_map(self.0.dc.as_ref(), c.tenant, 0)
                                    .await
                                    .ok()
                                    .flatten(),
                            );
                        }
                        maps.get(&0).and_then(Option::as_ref).is_some_and(|m| {
                            m.ranges.first().is_some_and(|r| {
                                &r.cluster_id == cluster && r.state == RangeState::Serving
                            })
                        })
                    }
                };
                if serving {
                    out.push(Item {
                        coll: Arc::clone(c),
                        cluster: cluster.clone(),
                        schema,
                        after: None,
                        done: false,
                    });
                }
            }
        }
        out
    }

    /// One pass; `(collections, shards, deleted)`.
    async fn pass(&self, cx: &JobCtx, engine: &Engine, budget: Duration) -> (usize, usize, u64) {
        let s = &self.0.settings;
        let Ok(clusters) = self.0.pools.sweepable().await else {
            return (0, 0, 0);
        };
        let colls: Vec<Arc<TtlColl>> = self
            .discover(&clusters)
            .await
            .into_iter()
            .map(Arc::new)
            .collect();
        let mut items = Vec::new();
        for c in &colls {
            items.extend(self.shards(c, &clusters).await);
        }
        let started = Instant::now();
        let mut pace: HashMap<u64, Pace> = HashMap::new();
        let mut conns: HashMap<ClusterId, Ctl> = HashMap::new();
        let mut total = 0u64;
        while items.iter().any(|i| !i.done) && !cx.cancel.is_cancelled() {
            if started.elapsed() > budget {
                break;
            }
            for it in items.iter_mut().filter(|i| !i.done) {
                if cx.cancel.is_cancelled() {
                    break;
                }
                let tenant = it.coll.tenant;
                let batch = usize::try_from(s.tenant_get(tenant, &MULTI_WRITE_BATCH))
                    .unwrap_or(100)
                    .max(1);
                let rate = s.tenant_get(tenant, &TTL_TENANT_DELETES_PER_SECOND);
                let p = pace.entry(tenant.0).or_insert_with(|| Pace {
                    started: Instant::now(),
                    deleted: 0,
                });
                // Pacing: at most `rate` deletes a second for the tenant.
                if rate > 0 {
                    #[allow(clippy::cast_precision_loss)]
                    let due = Duration::from_secs_f64(p.deleted as f64 / rate as f64);
                    if let Some(wait) = due.checked_sub(p.started.elapsed()) {
                        tokio::select! {
                            () = cx.cancel.cancelled() => break,
                            () = tokio::time::sleep(wait) => {}
                        }
                    }
                }
                let sql = if let Some(c) = conns.get_mut(&it.cluster) {
                    c
                } else {
                    let conn = match self.0.pools.writer(&it.cluster).await {
                        Ok(pool) => pool.get_conn().await.ok(),
                        Err(_) => None,
                    };
                    let Some(conn) = conn else {
                        it.done = true;
                        continue;
                    };
                    conns.entry(it.cluster.clone()).or_insert(Ctl(conn))
                };
                let ns = Ns {
                    db: it.schema.clone(),
                    coll: it.coll.meta.name.clone(),
                };
                let cutoff = now_ms().saturating_sub(it.coll.expire.saturating_mul(1000));
                match engine
                    .expire_batch(
                        sql,
                        &ns,
                        &it.coll.meta,
                        &it.coll.path,
                        cutoff,
                        it.after.take(),
                        batch,
                    )
                    .await
                {
                    Ok(r) => {
                        if r.deleted > 0 {
                            metrics::counter!("sprig_doc_ttl_deleted_total", "cluster" => it.cluster.to_string())
                                .increment(r.deleted);
                            tracing::debug!(tenant = %tenant, schema = %it.schema, coll = %it.coll.meta.name, deleted = r.deleted, "TTL deletes");
                        }
                        p.deleted += r.deleted;
                        total += r.deleted;
                        it.done = r.next.is_none();
                        it.after = r.next;
                    }
                    Err(e) => {
                        tracing::warn!(tenant = %tenant, schema = %it.schema, coll = %it.coll.meta.name, error = %e.message, "TTL batch failed; retried next pass");
                        conns.remove(&it.cluster);
                        it.done = true;
                    }
                }
            }
        }
        (colls.len(), items.len(), total)
    }
}

#[async_trait]
impl JobHandler for DocTtl {
    fn kind(&self) -> &'static str {
        JOB_KIND
    }

    fn class(&self) -> JobClass {
        JobClass::Consumer
    }

    async fn run(&self, cx: &JobCtx) -> Result<Value, String> {
        let engine = Engine::new(EngineConfig::default());
        let mut passes = 0u64;
        let mut deleted = 0u64;
        while !cx.cancel.is_cancelled() {
            let interval = Duration::from_secs(self.0.settings.get(&TTL_INTERVAL_SECONDS).max(1));
            let t0 = Instant::now();
            // A pass may run past the interval to finish a backlog, up to a
            // minute (MongoDB's sub-pass target).
            let (colls, shards, n) = self
                .pass(cx, &engine, interval.max(Duration::from_secs(60)))
                .await;
            passes += 1;
            deleted += n;
            metrics::counter!("sprig_doc_ttl_passes_total").increment(1);
            let _ = cx
                .progress(json!({
                    "passes": passes,
                    "deleted": deleted,
                    "last_pass": {"collections": colls, "shards": shards, "deleted": n,
                                  "ms": u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX)},
                }))
                .await;
            let wait = interval
                .saturating_sub(t0.elapsed())
                .max(Duration::from_millis(200));
            tokio::select! {
                () = cx.cancel.cancelled() => {}
                () = tokio::time::sleep(wait) => {}
            }
        }
        Ok(json!({"end": "cancelled", "passes": passes, "deleted": deleted}))
    }
}
