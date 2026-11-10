//! Logic jobs (chapter 41, R7 order 5) on chapter 23's job runtime:
//!
//! - `logic_trigger`: one per after-commit binding, long-running. It
//!   reads the tenant's feed from the binding's subscription checkpoint
//!   and runs `sprig_logic::triggers::run`; every second it reports
//!   progress, which is also how it learns it lost the job (another worker
//!   took over after this one's lease ended); every few seconds it replays
//!   dead letters marked `replaying`; every hour it purges old markers.
//! - `logic_schedule`: one run of a schedule (`sprig_logic::schedule::run`),
//!   only if the catalog still names this job as the live run.
//!
//! Both reach shards and catalogs as `sprig_ctl`; compiled routines run
//! with their definer's rights (`SQL SECURITY DEFINER`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mysql_async::Pool;
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use sprig_binlog::GtidPos;
use sprig_catalog::CatalogLocator;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_controller::catalogs::{ClusterPools, ShardMapLocator};
use sprig_controller::dispatch::JobClass;
use sprig_feed::binlog::{BinlogTxns, SERVER_ID_BASE};
use sprig_feed::names::{CatalogPools, CatalogResolver};
use sprig_feed::subs::Subscriptions;
use sprig_feed::{Clusters, MetaPlacement, StartAt, TailOptions, TenantTail, TxnSource};
use sprig_logic::LogicStore;
use sprig_logic::triggers::maria::{CatalogOutcomes, CatalogVersions, MariaApply, ShardPools};
use sprig_logic::triggers::{self, Filter, RunOpts, Stats};
use sprig_meta::MetaStore;
use sprig_mover::mysql::ClusterAccess;

use crate::runtime::{JobCtx, JobHandler};

/// What the logic handlers share.
#[derive(Clone)]
pub struct LogicEnv {
    /// This DC's tier (jobs, subscriptions, the schedule index).
    pub dc: Arc<dyn MetaStore>,
    /// Clusters by id.
    pub registry: ClusterRegistry,
    /// `sprig_ctl` pools per cluster.
    pub pools: Arc<dyn ClusterPools>,
    /// Tenant catalogs.
    pub locator: Arc<ShardMapLocator>,
    /// Binlog sources for the feed.
    pub clusters: Arc<RegistryClusters>,
    /// The runner's settings, following the worker's live settings
    /// ([`crate::settings::WorkerSettings::run_opts`]).
    pub opts: RunOpts,
    /// The worker's live settings: `logic.trigger_budget_ms` when a
    /// trigger job starts, `logic.job_budget_ms` per scheduled run.
    pub settings: crate::settings::WorkerSettings,
}

impl std::fmt::Debug for LogicEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogicEnv").finish_non_exhaustive()
    }
}

/// The feed's binlog sources, found through the cluster registry.
pub struct RegistryClusters {
    registry: ClusterRegistry,
    pools: Arc<dyn ClusterPools>,
    access: Mutex<HashMap<ClusterId, ClusterAccess>>,
    next_server_id: AtomicU32,
}

impl RegistryClusters {
    /// Sources from `registry`; positions read through `pools`.
    #[must_use]
    pub fn new(registry: ClusterRegistry, pools: Arc<dyn ClusterPools>) -> Self {
        // Above the feed's own base, per process, so two processes' dumps
        // on one node don't end each other.
        let offset = std::process::id() % 10_000 * 1_000 + 500;
        Self {
            registry,
            pools,
            access: Mutex::new(HashMap::new()),
            next_server_id: AtomicU32::new(SERVER_ID_BASE + offset),
        }
    }

    async fn access(&self, cluster: &ClusterId) -> Result<ClusterAccess> {
        if let Some(a) = self
            .access
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(cluster)
            .cloned()
        {
            return Ok(a);
        }
        let rec = self.registry.get(cluster).await?.ok_or_else(|| {
            SprigError::new(SprigCode::ClusterNotFound, format!("no cluster {cluster}"))
        })?;
        let a = ClusterAccess::discover(rec.body()).await?;
        self.access
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(cluster.clone(), a.clone());
        Ok(a)
    }
}

#[async_trait]
impl Clusters for RegistryClusters {
    async fn open(
        &self,
        cluster: &ClusterId,
        tenant: TenantId,
        from: &GtidPos,
    ) -> Result<Box<dyn TxnSource>> {
        let a = self.access(cluster).await?;
        let id = self.next_server_id.fetch_add(1, Ordering::Relaxed);
        match BinlogTxns::connect(&a.repl, tenant, from, id).await {
            Ok(t) => Ok(Box::new(t)),
            Err(e) => {
                // The writer may have moved: look it up again next time.
                self.access
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(cluster);
                Err(e)
            }
        }
    }

    async fn position(&self, cluster: &ClusterId) -> Result<GtidPos> {
        let pool = self.pools.writer(cluster).await?;
        let mut c = pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::BackendUnavailable, format!("{cluster}: {e}"))
        })?;
        let p: Option<String> = c
            .query_first("SELECT @@gtid_binlog_pos")
            .await
            .map_err(|e| {
                SprigError::new(SprigCode::BackendUnavailable, format!("{cluster}: {e}"))
            })?;
        p.unwrap_or_default().parse()
    }
}

/// The tenant catalog's pool, for the feed's name resolver.
struct Catalogs(Arc<ShardMapLocator>);

#[async_trait]
impl CatalogPools for Catalogs {
    async fn pool(&self, tenant: TenantId) -> Result<Pool> {
        Ok(self.0.writer(tenant).await?.pool)
    }
}

/// Shards are where the tenant's catalog is (R1 moves them together).
struct TenantShards {
    locator: Arc<ShardMapLocator>,
    pools: Arc<dyn ClusterPools>,
}

#[async_trait]
impl ShardPools for TenantShards {
    async fn pool(&self, tenant: TenantId) -> Result<Pool> {
        let cluster = self.locator.cluster_of(tenant).await?.ok_or_else(|| {
            let mut e = SprigError::new(
                SprigCode::TenantNotReady,
                format!("tenant {tenant} has no shard map here"),
            );
            e.retryable = true;
            e
        })?;
        self.pools.writer(&cluster).await
    }
}

fn spec_u64(spec: &Value, k: &str) -> std::result::Result<u64, String> {
    spec[k]
        .as_u64()
        .ok_or_else(|| format!("spec.{k} is missing"))
}

/// `logic_trigger` (see the module docs).
#[derive(Debug, Clone)]
pub struct LogicTrigger(pub LogicEnv);

impl LogicTrigger {
    #[allow(clippy::too_many_lines)]
    async fn serve(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let env = &self.0;
        let tenant = TenantId(spec_u64(&cx.spec, "tenant")?);
        let binding_id = spec_u64(&cx.spec, "binding")?;
        let ep = env
            .locator
            .writer(tenant)
            .await
            .map_err(|e| e.to_string())?;
        let store = LogicStore::new(ep.store.schema());
        let (b, module) = {
            let mut c = ep.pool.get_conn().await.map_err(|e| e.to_string())?;
            let b = store
                .binding_by_id(&mut c, binding_id)
                .await
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("binding {binding_id} is gone"))?;
            let m = store
                .module_by_id(&mut c, b.module_id)
                .await
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("module {} is gone", b.module_id))?;
            (b, m)
        };
        if b.state != "active" || module.state != "active" {
            return Ok(json!({"stopped": format!("binding {binding_id} is {}", b.state)}));
        }
        let filter = Filter {
            db: module.db_id,
            object: b.object_id.unwrap_or(0),
            table: String::new(),
            events: b
                .events
                .split(',')
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect(),
            when_fields: b.when_fields.clone(),
        };
        let subs = Subscriptions::new(Arc::clone(&env.dc));
        let name = triggers::subscription_name(binding_id);
        let start = match subs
            .checkpoint(tenant, &name)
            .await
            .map_err(|e| e.to_string())?
        {
            Some(cp) => StartAt::After(cp.resume_token().map_err(|e| e.to_string())?),
            None => StartAt::Now,
        };
        let tail = TenantTail::start(
            tenant,
            start,
            Arc::clone(&env.clusters) as Arc<dyn Clusters>,
            Arc::new(MetaPlacement::new(Arc::clone(&env.dc))),
            Arc::new(CatalogResolver::new(Arc::new(Catalogs(Arc::clone(
                &env.locator,
            ))))),
            TailOptions::default(),
        )
        .await
        .map_err(|e| e.to_string())?;
        let locator: Arc<dyn CatalogLocator> = Arc::clone(&env.locator) as Arc<dyn CatalogLocator>;
        let versions = Arc::new(CatalogVersions::new(
            Arc::clone(&locator),
            tenant,
            module.module_id,
        ));
        let apply = Arc::new(MariaApply {
            tenant,
            binding_id,
            module_id: module.module_id,
            pools: Arc::new(TenantShards {
                locator: Arc::clone(&env.locator),
                pools: Arc::clone(&env.pools),
            }),
            versions,
            budget: env.settings.trigger_budget(),
        });
        let out = Arc::new(CatalogOutcomes {
            locator: Arc::clone(&locator),
            tenant,
            binding_id,
            module_id: module.module_id,
            subs,
        });
        let stats = Arc::new(Stats::default());
        tracing::info!(%tenant, binding = binding_id, job = %cx.job_id, "after-commit trigger started");
        triggers::Outcomes::log(
            out.as_ref(),
            "info",
            &format!("worker job {} started the trigger", cx.job_id),
        )
        .await;
        let stop = cx.cancel.clone();
        let run = triggers::run(
            Box::new(tail),
            &filter,
            Arc::clone(&apply) as Arc<dyn triggers::Apply>,
            Arc::clone(&out) as Arc<dyn triggers::Outcomes>,
            &env.opts,
            Arc::clone(&stats),
            &stop,
        );
        tokio::pin!(run);
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let mut last_replay = Instant::now();
        let mut last_purge = Instant::now();
        let result = loop {
            tokio::select! {
                r = &mut run => break r.map_err(|e| e.to_string()),
                _ = tick.tick() => {
                    let p = json!({
                        "binding": binding_id,
                        "wanted": stats.wanted.load(Ordering::Relaxed),
                        "ran": stats.ran.load(Ordering::Relaxed),
                        "already_done": stats.already.load(Ordering::Relaxed),
                        "retries": stats.retries.load(Ordering::Relaxed),
                        "dead": stats.dead.load(Ordering::Relaxed),
                        "acks": stats.acks.load(Ordering::Relaxed),
                    });
                    // Ok(false): the job was lost, and progress() fired the cancel.
                    if let Err(e) = cx.progress(p).await {
                        tracing::warn!(error = %e, "trigger progress not written");
                    }
                    if last_replay.elapsed() > Duration::from_secs(3) {
                        last_replay = Instant::now();
                        match sprig_logic::triggers::maria::replay(locator.as_ref(), tenant, binding_id, module.module_id, apply.as_ref()).await {
                            Ok(0) => {}
                            Ok(n) => tracing::info!(binding = binding_id, replayed = n, "dead letters replayed"),
                            Err(e) => tracing::warn!(error = %e, "dead letter replay failed"),
                        }
                    }
                    if last_purge.elapsed() > Duration::from_secs(3600) {
                        last_purge = Instant::now();
                        purge(env, tenant, binding_id, &store).await;
                    }
                }
            }
        };
        tracing::info!(%tenant, binding = binding_id, ran = stats.ran.load(Ordering::Relaxed), "after-commit trigger stopped");
        result.map(|()| json!({"stopped": "cancelled"}))
    }
}

/// Markers past `logic.done_retention_hours` (168) and old log lines.
async fn purge(env: &LogicEnv, tenant: TenantId, binding_id: u64, store: &LogicStore) {
    let Ok(ep) = env.locator.writer(tenant).await else {
        return;
    };
    if let Ok(mut c) = ep.pool.get_conn().await {
        let _ = store.purge(&mut c, 14, 14, 10_000).await;
        let schemas: Vec<String> = c
            .exec(
                "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME = '__sprig_ldone' \
                 AND TABLE_SCHEMA LIKE ?",
                (format!("t{tenant}\\_d%"),),
            )
            .await
            .unwrap_or_default();
        for s in schemas {
            let _ = sprig_logic::triggers::maria::purge_markers(&mut c, &s, binding_id, 168).await;
        }
    }
}

#[async_trait]
impl JobHandler for LogicTrigger {
    fn kind(&self) -> &'static str {
        "logic_trigger"
    }

    fn class(&self) -> JobClass {
        JobClass::Other
    }

    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        self.serve(cx).await
    }
}

/// `logic_schedule` (see the module docs).
#[derive(Debug, Clone)]
pub struct LogicSchedule(pub LogicEnv);

#[async_trait]
impl JobHandler for LogicSchedule {
    fn kind(&self) -> &'static str {
        sprig_logic::schedule::JOB_KIND
    }

    fn class(&self) -> JobClass {
        JobClass::Other
    }

    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let env = &self.0;
        let tenant = TenantId(spec_u64(&cx.spec, "tenant")?);
        let module_id = spec_u64(&cx.spec, "module")?;
        let schema = cx.spec["schema"]
            .as_str()
            .ok_or("spec.schema is missing")?
            .to_owned();
        // The run the controller claimed in the catalog (not this job's
        // id): only that run may execute.
        let run: sprig_common::ids::JobId = cx.spec["run"]
            .as_str()
            .ok_or("spec.run is missing")?
            .parse()
            .map_err(|e| format!("spec.run: {e}"))?;
        let ep = env
            .locator
            .writer(tenant)
            .await
            .map_err(|e| e.to_string())?;
        let mut catalog = ep.pool.get_conn().await.map_err(|e| e.to_string())?;
        let cluster = env
            .locator
            .cluster_of(tenant)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("the tenant has no shard map here")?;
        let pool = env
            .pools
            .writer(&cluster)
            .await
            .map_err(|e| e.to_string())?;
        let mut shard = pool.get_conn().await.map_err(|e| e.to_string())?;
        let store = LogicStore::new(ep.store.schema());
        let end = sprig_logic::schedule::run(
            &mut catalog,
            &store,
            &mut shard,
            &schema,
            module_id,
            run,
            env.settings.job_budget(),
        )
        .await
        .map_err(|e| e.to_string())?;
        tracing::info!(%tenant, module = module_id, job = %cx.job_id, %run, outcome = %end.outcome, "schedule run ended");
        if end.queued {
            // A queued run: make the index due now.
            let _ = sprig_logic::schedule::put_index(
                env.dc.as_ref(),
                &sprig_logic::schedule::ScheduleIndex {
                    tenant,
                    module_id,
                    schema: schema.clone(),
                    next_fire_ms: 0,
                },
            )
            .await;
        }
        match end.error {
            None => Ok(json!({"outcome": end.outcome})),
            Some(e) => Err(format!("{}: {e}", end.outcome)),
        }
    }
}

/// The runner's settings at the global scope, with the
/// `SPRIG_LOGIC_TRIGGER_*` test overrides; a running worker uses
/// [`crate::settings::WorkerSettings::run_opts`], which follows changes.
#[must_use]
pub fn run_opts_from_env() -> RunOpts {
    let dc = sprig_common::ids::DcId::new("none").ok();
    dc.and_then(|dc| crate::settings::WorkerSettings::defaults(dc).ok())
        .map_or_else(RunOpts::default, |s| {
            let mut o = s.run_opts();
            o.live = None;
            o
        })
}
