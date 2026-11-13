//! The sequencer job and what it needs around the [`Sequencer`] (chapter
//! 45, "Sequencing and offsets", "Retention and compaction").
//!
//! ```text
//! CREATE STREAM (gateway) → feed subscription stream:<id>, kind `stream`
//! controller (DC leader, every second): a `stream` subscription without a
//!   live `stream_sequencer` job → submit one (spec: tenant, stream)
//! worker: run_job:
//!   load the stream's row; gone or dropping → end
//!   Sequencer over a MariaLog on the tenant's cluster now, its feed a
//!     TenantTail that keeps the stream's outbox (chapter 38's filter rule 1
//!     would drop __sprig_ tables)
//!   every second: progress (the job runtime's lease: a lost job is cancelled)
//!   every minute: retention and compaction (crate::retention), with the
//!     stream's settings read from the tenant catalog
//! ```
//!
//! **As built:** the worker resolves `stream.*` settings from the tenant
//! catalog's rows (object, database, tenant) and the defaults; dc and
//! global values aren't read (the worker reads no live settings yet, as
//! for the mover). A change topic's retention comes from its table's
//! object scope; an event stream's from the stream's own.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use mysql_async::Pool;
use mysql_async::prelude::Queryable;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_feed::subs::{Subscription, Subscriptions};
use sprig_meta::{MetaStore, Op, Record, Txn, Versioned};
use tokio::sync::Mutex;

use crate::envelope::ChangeCtx;
use crate::maria::{MariaLog, PoolSource};
use crate::model::{
    LeaderEpoch, Offset, PartitionNo, StreamDef, StreamId, StreamKind, StreamState,
};
use crate::sequencer::{FeedOpener, Filter, RunEnd, SeqOpts, Sequencer, Watermarks};
use crate::settings::Retention;
use crate::store::StreamsStore;

/// The job kind.
pub const JOB_KIND: &str = "stream_sequencer";

/// The feed subscription kind.
pub const SUB_KIND: &str = "stream";

/// The subscription of a stream.
#[must_use]
pub fn subscription_name(id: StreamId) -> String {
    format!("stream:{}", id.0)
}

/// The stream of a subscription name.
#[must_use]
pub fn stream_of(name: &str) -> Option<StreamId> {
    name.strip_prefix("stream:")?.parse().ok().map(StreamId)
}

fn now_ms() -> u64 {
    u64::try_from(crate::maria::wall_ms()).unwrap_or(0)
}

/// Registers stream `def`'s subscription (the controller then starts its
/// sequencer). Idempotent.
///
/// # Errors
/// The DC tier's.
pub async fn subscribe(dc: Arc<dyn MetaStore>, tenant: TenantId, def: &StreamDef) -> Result<()> {
    let subs = Subscriptions::new(dc);
    match subs
        .register(&Subscription {
            tenant,
            name: subscription_name(def.id),
            kind: SUB_KIND.into(),
            ordering: "merged".into(),
            owner_ref: Some(format!("stream:{}:{}", def.db.0, def.name)),
            state: "active".into(),
            created_at_ms: now_ms(),
        })
        .await
    {
        Err(e) if e.code() == Some(SprigCode::FeedSubscriptionExists) => Ok(()),
        r => r,
    }
}

/// Removes stream `id`'s subscription (its sequencer ends).
///
/// # Errors
/// The DC tier's.
pub async fn unsubscribe(dc: Arc<dyn MetaStore>, tenant: TenantId, id: StreamId) -> Result<()> {
    match Subscriptions::new(dc)
        .drop_subscription(tenant, &subscription_name(id))
        .await
    {
        Err(e) if e.code() == Some(SprigCode::FeedSubscriptionNotFound) => Ok(()),
        r => r,
    }
}

/// `streams/<tenant>/<stream>/<shard>` in the DC tier: the high watermarks
/// a log shard's sequencer published last.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatermarkRecord {
    /// The sequencer's leader epoch.
    pub epoch: u32,
    /// Next offsets, by partition.
    pub marks: BTreeMap<u32, u64>,
    /// When written (ms since the Unix epoch).
    pub at_ms: u64,
}

impl Versioned for WatermarkRecord {
    const V: u32 = 1;
}

/// High watermarks to etcd, at most every `every`. A mark held back by
/// that limit is written when it ends (a trailing write), so the last
/// marks of a burst always reach the gateways' long polls.
pub struct MetaWatermarks {
    dc: Arc<dyn MetaStore>,
    key: String,
    every: Duration,
    state: Arc<Mutex<(Option<Instant>, WatermarkRecord)>>,
    trailing: Arc<std::sync::atomic::AtomicBool>,
}

impl std::fmt::Debug for MetaWatermarks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaWatermarks")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl MetaWatermarks {
    /// Watermarks of stream `s`'s log shard `shard` of `tenant`.
    #[must_use]
    pub fn new(dc: Arc<dyn MetaStore>, tenant: TenantId, s: StreamId, shard: u32) -> Self {
        Self {
            dc,
            key: format!("streams/{tenant}/{}/{shard}", s.0),
            every: Duration::from_millis(50),
            state: Arc::new(Mutex::new((
                None,
                WatermarkRecord {
                    epoch: 0,
                    marks: BTreeMap::new(),
                    at_ms: 0,
                },
            ))),
            trailing: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
}

async fn write_marks(dc: &Arc<dyn MetaStore>, key: &str, rec: WatermarkRecord) {
    let Ok(key) = dc.tier().key(key) else {
        return;
    };
    let Ok(value) = Record::new(rec).encode() else {
        return;
    };
    let r = dc
        .txn(Txn {
            when: Vec::new(),
            then: vec![Op::Put {
                key,
                value,
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await;
    if let Err(e) = r {
        tracing::debug!(error = %e, "stream watermarks not published");
    }
}

#[async_trait::async_trait]
impl Watermarks for MetaWatermarks {
    async fn publish(&self, epoch: LeaderEpoch, marks: &BTreeMap<PartitionNo, Offset>) {
        let mut st = self.state.lock().await;
        st.1.epoch = epoch.0;
        for (p, o) in marks {
            st.1.marks.insert(p.0, o.0);
        }
        if let Some(t) = st.0
            && t.elapsed() < self.every
        {
            // Held back: one trailing write when the interval ends.
            if !self
                .trailing
                .swap(true, std::sync::atomic::Ordering::AcqRel)
            {
                let wait = self.every.saturating_sub(t.elapsed());
                let (dc, key, state, trailing) = (
                    Arc::clone(&self.dc),
                    self.key.clone(),
                    Arc::clone(&self.state),
                    Arc::clone(&self.trailing),
                );
                tokio::spawn(async move {
                    tokio::time::sleep(wait).await;
                    let rec = {
                        let mut st = state.lock().await;
                        trailing.store(false, std::sync::atomic::Ordering::Release);
                        st.0 = Some(Instant::now());
                        st.1.at_ms = now_ms();
                        st.1.clone()
                    };
                    write_marks(&dc, &key, rec).await;
                });
            }
            return;
        }
        st.0 = Some(Instant::now());
        st.1.at_ms = now_ms();
        let rec = st.1.clone();
        drop(st);
        write_marks(&self.dc, &self.key, rec).await;
    }
}

/// `stream.*` settings of `db` and `object` from the tenant catalog's
/// `settings` rows (object > database > tenant > default).
///
/// # Errors
/// The backend's.
pub async fn resolve_retention<Q: Queryable + Send>(
    q: &mut Q,
    catalog: &str,
    db: DbId,
    object: ObjectId,
) -> Result<Retention> {
    resolve_retention_over(q, catalog, db, object, Retention::default()).await
}

/// The `stream.*` retention keys at the global scope of `etcd` (what a
/// catalog row doesn't set falls back to), resolved in `dc`.
#[must_use]
pub fn global_retention(
    etcd: &sprig_settings::MemSettings<'_>,
    dc: &sprig_common::ids::DcId,
) -> Retention {
    use crate::settings::{
        COMPACTION_LAG_MINUTES, RETENTION_BYTES, RETENTION_HOURS, TOMBSTONE_HOURS,
    };
    let ctx = sprig_settings::SettingCtx::process(dc);
    Retention {
        hours: etcd.get(&RETENTION_HOURS, &ctx),
        bytes: etcd.get(&RETENTION_BYTES, &ctx),
        compaction_lag_minutes: etcd.get(&COMPACTION_LAG_MINUTES, &ctx),
        tombstone_hours: etcd.get(&TOMBSTONE_HOURS, &ctx),
    }
}

/// As [`resolve_retention`], over `base` (the global values) instead of
/// the defaults: object > database > tenant > `base`.
///
/// # Errors
/// The backend's.
pub async fn resolve_retention_over<Q: Queryable + Send>(
    q: &mut Q,
    catalog: &str,
    db: DbId,
    object: ObjectId,
    base: Retention,
) -> Result<Retention> {
    let rows: Vec<(String, u64, String, Option<String>)> = q
        .exec(
            format!(
                "SELECT scope_kind, scope_id, setting_key, CAST(value AS CHAR) FROM {}.settings \
                 WHERE setting_key LIKE 'stream.%' AND ((scope_kind = 'object' AND scope_id = ?) \
                 OR (scope_kind = 'database' AND scope_id = ?) OR scope_kind = 'tenant')",
                sprig_common::names::quote(catalog)
            ),
            (object.0, db.0),
        )
        .await
        .map_err(crate::maria::map_err)?;
    let rank = |k: &str| match k {
        "object" => 0,
        "database" => 1,
        _ => 2,
    };
    let mut best: BTreeMap<String, (u8, u64)> = BTreeMap::new();
    for (kind, _, key, value) in rows {
        let Some(v) = value.and_then(|v| v.trim().trim_matches('"').parse::<u64>().ok()) else {
            continue;
        };
        let r = rank(&kind);
        if best.get(&key).is_none_or(|(br, _)| r < *br) {
            best.insert(key, (r, v));
        }
    }
    let get = |k: &str, d: u64| best.get(k).map_or(d, |(_, v)| *v);
    let def = base;
    Ok(Retention {
        hours: get("stream.retention_hours", def.hours),
        bytes: get("stream.retention_bytes", def.bytes),
        compaction_lag_minutes: get("stream.compaction_lag_minutes", def.compaction_lag_minutes),
        tombstone_hours: get("stream.tombstone_hours", def.tombstone_hours),
    })
}

/// What a sequencer job needs from its worker.
#[async_trait::async_trait]
pub trait JobEnv: Send + Sync {
    /// The tenant catalog's pool and schema.
    ///
    /// # Errors
    /// The catalog is unreachable.
    async fn catalog(&self, tenant: TenantId) -> Result<(Pool, String)>;
    /// The pool of the cluster holding the tenant's shards now.
    ///
    /// # Errors
    /// The cluster is unknown or unreachable (retryable).
    async fn shard_pool(&self, tenant: TenantId) -> Result<Pool>;
    /// The tenant's feed for `opener`'s starts, keeping `keep_internal`
    /// tables.
    fn feed(&self, tenant: TenantId, keep_internal: Vec<String>) -> Arc<dyn FeedOpener>;
    /// The DC tier, for watermarks.
    fn dc(&self) -> Arc<dyn MetaStore>;
    /// The tenant's name, for change envelopes.
    async fn tenant_name(&self, tenant: TenantId) -> Option<String>;
    /// The worker's live settings (etcd scopes) and its DC: global
    /// `stream.*` values under the catalog's rows. `None`: the defaults.
    fn settings(&self) -> Option<(sprig_settings::Snapshot, sprig_common::ids::DcId)> {
        None
    }
}

/// Where a job reports progress (chapter 23's job runtime).
#[async_trait::async_trait]
pub trait Report: Send + Sync {
    /// Reports progress; `false` when the job was lost.
    async fn progress(&self, v: Value) -> bool;
}

struct TenantPool {
    env: Arc<dyn JobEnv>,
    tenant: TenantId,
}

#[async_trait::async_trait]
impl PoolSource for TenantPool {
    async fn pool(&self) -> Result<Pool> {
        self.env.shard_pool(self.tenant).await
    }
}

/// How a job ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobEnd {
    /// The stream is gone or being dropped.
    Gone,
    /// Cancelled (lost, or the worker stops).
    Stopped,
    /// Another sequencer took the log shard.
    Fenced,
}

/// Runs stream `id`'s sequencer and maintenance until `stop` (see the
/// module docs).
///
/// # Errors
/// The sequencer's and the catalog's.
#[allow(clippy::too_many_lines)]
pub async fn run_job(
    env: Arc<dyn JobEnv>,
    tenant: TenantId,
    id: StreamId,
    stop: CancellationToken,
    report: &dyn Report,
) -> Result<JobEnd> {
    let (cat_pool, cat_schema) = env.catalog(tenant).await?;
    let def = {
        let mut c = cat_pool.get_conn().await.map_err(crate::maria::map_err)?;
        StreamsStore::new(cat_schema.clone())
            .get(&mut c, id)
            .await?
    };
    let Some(def) = def.filter(|d| d.state == StreamState::Active) else {
        return Ok(JobEnd::Gone);
    };
    let Some(log) = def.log.first().cloned() else {
        return Err(SprigError::internal(format!(
            "stream {} has no log shard",
            id.0
        )));
    };
    let log_schema = log.slot.schema(tenant, def.db).to_string();
    let outbox_schemas: Vec<String> = def
        .outbox
        .iter()
        .map(|s| s.schema(tenant, def.db).to_string())
        .collect();
    let pools: Arc<dyn PoolSource> = Arc::new(TenantPool {
        env: Arc::clone(&env),
        tenant,
    });
    let store = Arc::new(MariaLog::new(
        Arc::clone(&pools),
        id,
        log_schema.clone(),
        outbox_schemas.clone(),
    ));
    let (filter, keep) = match def.kind {
        StreamKind::Events => (
            Filter::Outbox {
                db: def.db,
                table: def.outbox_table(),
                partitions: def.partitions,
            },
            vec![def.outbox_table()],
        ),
        StreamKind::Changes => (
            Filter::Changes {
                db: def.db,
                object: def.source_object.unwrap_or(ObjectId(0)),
                table: def.source_name.clone().unwrap_or_default(),
                cx: ChangeCtx {
                    partitions: def.partitions,
                    tenant_name: env.tenant_name(tenant).await,
                },
            },
            Vec::new(),
        ),
    };
    let seq = Sequencer {
        filter,
        owned: log.partitions.clone(),
        store,
        feed: env.feed(tenant, keep),
        marks: Arc::new(MetaWatermarks::new(env.dc(), tenant, id, log.slot.shard.0)),
        opts: SeqOpts::default(),
        stats: Arc::default(),
    };
    let stats = Arc::clone(&seq.stats);
    let settings_object = match def.kind {
        StreamKind::Events => ObjectId(id.0),
        StreamKind::Changes => def.source_object.unwrap_or(ObjectId(id.0)),
    };
    tracing::info!(%tenant, stream = id.0, name = %def.name, "stream sequencer started");
    let subs = Subscriptions::new(env.dc());
    let run = seq.run(&stop);
    tokio::pin!(run);
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut last_maint: Option<Instant> = None;
    let mut gone = false;
    let end = loop {
        tokio::select! {
            r = &mut run => break r?,
            _ = tick.tick() => {
                let p = json!({
                    "stream": id.0,
                    "appended": stats.appended.load(Ordering::Relaxed),
                    "duplicates": stats.duplicates.load(Ordering::Relaxed),
                    "batches": stats.batches.load(Ordering::Relaxed),
                    "items": stats.items.load(Ordering::Relaxed),
                    "lag_ms": stats.lag_ms.load(Ordering::Relaxed),
                });
                // Lag per instance (R9, r9feed): one series per stream
                // sequencer on this worker.
                #[allow(clippy::cast_precision_loss)]
                let lag = stats.lag_ms.load(Ordering::Relaxed) as f64 / 1000.0;
                metrics::gauge!(
                    "sprig_stream_sequencer_lag_seconds",
                    "tenant" => tenant.to_string(),
                    "stream" => id.0.to_string()
                )
                .set(lag);
                if !report.progress(p).await {
                    stop.cancel();
                }
                // DROP STREAM removes the subscription first: stop at once,
                // so the job's slot is free within a second (r9triggers;
                // the catalog check below runs only every minute).
                if subscription_gone(&subs, tenant, id).await {
                    tracing::info!(stream = id.0, "stream's subscription dropped; its sequencer stops");
                    stop.cancel();
                    gone = true;
                }
                if last_maint.is_none_or(|t| t.elapsed() >= Duration::from_secs(60)) {
                    last_maint = Some(Instant::now());
                    let r = maintain_once(&env, tenant, &cat_pool, &cat_schema, &def, settings_object, &log_schema, &outbox_schemas).await;
                    match r {
                        Ok(Some(m)) if m != crate::retention::Maintained::default() => {
                            tracing::info!(stream = id.0, ?m, "stream maintenance");
                        }
                        Ok(Some(_)) => {}
                        Ok(None) => {
                            // Dropped (or its database was): stop.
                            tracing::info!(stream = id.0, "stream gone; its sequencer stops");
                            stop.cancel();
                            gone = true;
                        }
                        Err(e) => tracing::warn!(stream = id.0, error = %e, "stream maintenance failed"),
                    }
                }
            }
        }
    };
    tracing::info!(%tenant, stream = id.0, ?end, "stream sequencer stopped");
    metrics::gauge!(
        "sprig_stream_sequencer_lag_seconds",
        "tenant" => tenant.to_string(),
        "stream" => id.0.to_string()
    )
    .set(0.0);
    Ok(match end {
        RunEnd::Stopped if gone => JobEnd::Gone,
        RunEnd::Stopped => JobEnd::Stopped,
        RunEnd::Fenced => JobEnd::Fenced,
    })
}

/// Whether stream `id`'s subscription is gone or no longer active (a read
/// error counts as present: the minute's catalog check still runs).
async fn subscription_gone(subs: &Subscriptions, tenant: TenantId, id: StreamId) -> bool {
    match subs.get(tenant, &subscription_name(id)).await {
        Ok(s) => s.state != "active",
        Err(e) => e.code() == Some(SprigCode::FeedSubscriptionNotFound),
    }
}

/// One retention pass; `None` when the stream is gone.
#[allow(clippy::too_many_arguments)]
async fn maintain_once(
    env: &Arc<dyn JobEnv>,
    tenant: TenantId,
    cat_pool: &Pool,
    cat_schema: &str,
    def: &StreamDef,
    settings_object: ObjectId,
    log_schema: &str,
    outbox_schemas: &[String],
) -> Result<Option<crate::retention::Maintained>> {
    let (r, live) = {
        let mut c = cat_pool.get_conn().await.map_err(crate::maria::map_err)?;
        let live = StreamsStore::new(cat_schema).get(&mut c, def.id).await?;
        (
            resolve_retention_over(
                &mut c,
                cat_schema,
                def.db,
                settings_object,
                env.settings()
                    .map_or_else(Retention::default, |(s, dc)| global_retention(&s, &dc)),
            )
            .await?,
            live,
        )
    };
    let Some(live) = live.filter(|l| l.state == StreamState::Active) else {
        return Ok(None);
    };
    let pool = env.shard_pool(tenant).await?;
    let mut c = pool.get_conn().await.map_err(crate::maria::map_err)?;
    crate::retention::maintain(
        &mut c,
        log_schema,
        outbox_schemas,
        def.id,
        live.partitions,
        live.cleanup,
        &r,
        crate::maria::wall_ms(),
    )
    .await
    .map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert_eq!(subscription_name(StreamId(41)), "stream:41");
        assert_eq!(stream_of("stream:41"), Some(StreamId(41)));
        assert_eq!(stream_of("logic:b4"), None);
    }
}
