//! The `auto_index` loop's view of the deployment: catalogs through their
//! shard maps, objects and their shards from the catalog snapshot,
//! settings resolved per object (chapter 13, "Settings scopes and
//! budgets"), and clusters from the registry (chapter 09).
//!
//! **Settings.** Per-object keys resolve through the tenant's catalog rows
//! and the etcd mirrors ([`Layered`]); the shared cluster keys
//! (`max_builds_per_cluster`, `build_window`, `abort_replica_lag_seconds`)
//! resolve along the cluster's chain and data scopes can only tighten
//! them ([`SharedLimits`]).
//!
//! **Clusters.** The writer is the `registry` loop's driver choice when it
//! has one, else the record's first node (primary-replica) or the
//! heaviest Galera writer; the other nodes are replicas or Galera peers.
//! The cluster is unhealthy unless its published health is `healthy`.
//!
//! [`EvalShim`] is the loop's trigger evaluation: the evaluator of
//! `sprig_advisor::autoindex::evaluate` over the same catalog snapshot,
//! the cluster writer's statistics and the settings.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use mysql_async::Conn;
use sprig_advisor::autoindex::evaluate::{CatalogStore, EvalCx, Evaluator, NodeStats, ObjectMeta};
use sprig_advisor::autoindex::observe::ObserveSettings;
use sprig_advisor::autoindex::policy::{Layered, ObjectPolicy, SettingSource, SharedLimits};
use sprig_advisor::autoindex::qualify::WorkloadSource as QualifySource;
use sprig_advisor::autoindex::replay::{ReplaySettings, Thresholds};
use sprig_advisor::autoindex::rollout::{ClusterView, Nodes, RolloutSettings, ShardTable};
use sprig_advisor::autoindex::settings as k;
use sprig_advisor::autoindex::stats::StatsReader;
use sprig_advisor::autoindex::store::{AUTOINDEX_MODULE, AutoIndexStore, DecisionLog};
use sprig_advisor::autoindex::validate::ValidationSettings;
use sprig_catalog::cache::{Loaded, TenantCatalog};
use sprig_catalog::{CatalogLocator, Migrator, ObjectState, ddl::shard_schemas};
use sprig_cluster::driver::{DdlKind, NodeConnector, TableTraits, ddl_method_for};
use sprig_cluster::health::ClusterHealth;
use sprig_cluster::record::TopologyKind;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::{ClusterId, DcId, ObjectId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_provision::credentials::CredentialConnector;
use sprig_settings::{LiveSettings, MemSettings, Registry, SettingCtx};

use crate::autoindex_loop::{AutoIndexEnv, CandidateSource, Evaluated, JobShardLocks, ObjectEnv};
use crate::catalogs::ShardMapLocator;
use crate::monitor::ClusterMonitor;

/// How long a catalog snapshot may read once it has its connection.
pub(crate) const SNAPSHOT_WORK: Duration = Duration::from_secs(60);

/// [`AutoIndexEnv`] over the controller's registry, catalogs and settings.
pub struct ControllerEnv {
    dc: DcId,
    locator: ShardMapLocator,
    registry: ClusterRegistry,
    monitor: Option<Arc<ClusterMonitor>>,
    live: Option<LiveSettings>,
    jobs: JobShardLocks,
    migrated: std::sync::Mutex<std::collections::HashSet<TenantId>>,
    /// Where a pass's long-held catalog connection comes from: its own
    /// budget (`catalogs` module docs); `locator`'s when unset (tests).
    sessions: Option<ShardMapLocator>,
}

impl std::fmt::Debug for ControllerEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControllerEnv")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

impl ControllerEnv {
    /// The environment of DC `dc`.
    #[must_use]
    pub fn new(
        dc: DcId,
        locator: ShardMapLocator,
        registry: ClusterRegistry,
        monitor: Option<Arc<ClusterMonitor>>,
        live: Option<LiveSettings>,
        jobs: JobShardLocks,
    ) -> Self {
        Self {
            dc,
            locator,
            registry,
            monitor,
            live,
            jobs,
            migrated: std::sync::Mutex::new(std::collections::HashSet::new()),
            sessions: None,
        }
    }

    /// The same environment, taking each pass's catalog connection from
    /// `sessions` (the loop's own connection budget), so a pass that holds
    /// one while it reads snapshots, samples or decisions through the
    /// shared budget can never starve the controller's other catalog calls.
    #[must_use]
    pub fn with_sessions(mut self, sessions: ShardMapLocator) -> Self {
        self.sessions = Some(sessions);
        self
    }

    /// The cluster as the rollout sees it.
    ///
    /// # Errors
    /// 39104 for an unknown cluster; 39998 for credentials that don't
    /// resolve.
    pub async fn view(&self, cluster: &ClusterId) -> Result<ClusterView> {
        let stored = self.registry.get(cluster).await?.ok_or_else(|| {
            SprigError::new(
                SprigCode::ClusterNotFound,
                format!("no cluster {cluster} in dc {}", self.dc),
            )
        })?;
        let rec = stored.body();
        let conn = CredentialConnector::ctl(&rec.secrets);
        let chosen = match &self.monitor {
            Some(m) => m.writer_node(cluster).await,
            None => None,
        }
        .or_else(|| {
            rec.galera.as_ref().and_then(|g| {
                g.writers
                    .iter()
                    .max_by(|a, b| a.weight.total_cmp(&b.weight))
                    .map(|w| w.node.clone())
            })
        })
        .or_else(|| rec.nodes.first().map(|n| n.node.clone()))
        .ok_or_else(|| SprigError::new(SprigCode::ClusterUnavailable, "a cluster without nodes"))?;
        let mut eps = Vec::new();
        for n in &rec.nodes {
            eps.push(conn.endpoint(n)?);
        }
        let galera = rec.topology == TopologyKind::Galera;
        let nodes = match rec.topology {
            TopologyKind::Galera => Nodes::Galera {
                nodes: eps,
                writer: chosen,
            },
            TopologyKind::PrimaryReplica => {
                let (w, r): (Vec<_>, Vec<_>) = eps.into_iter().partition(|e| e.node == chosen);
                Nodes::PrimaryReplica {
                    writer: w.into_iter().next().ok_or_else(|| {
                        SprigError::new(SprigCode::ClusterUnavailable, "the writer isn't a node")
                    })?,
                    replicas: r,
                }
            }
            TopologyKind::SingleNode | TopologyKind::External => Nodes::Single {
                writer: eps
                    .into_iter()
                    .find(|e| e.node == chosen)
                    .ok_or_else(|| SprigError::new(SprigCode::ClusterUnavailable, "no writer"))?,
            },
        };
        let profile = rec.profile.as_ref();
        Ok(ClusterView {
            cluster: cluster.clone(),
            nodes,
            ddl_method: ddl_method_for(galera, profile, DdlKind::AddIndex, &TableTraits::default()),
            instant_ignored_toggle: profile.is_some_and(|p| p.features.instant_ignored_toggle),
            unhealthy: match rec.health.as_ref().map(|h| h.state) {
                Some(ClusterHealth::Healthy) => None,
                Some(h) => Some(h.to_string()),
                None => Some("not probed yet".into()),
            },
        })
    }

    fn registry_ref() -> Result<&'static Registry> {
        Registry::global()
    }
}

/// A number from a key, else `default`.
fn num(src: &dyn SettingSource, key: &str, ctx: &SettingCtx<'_>, default: f64) -> f64 {
    src.resolve(key, ctx)
        .ok()
        .and_then(|r| r.value.as_f64())
        .unwrap_or(default)
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn secs(v: f64) -> Duration {
    Duration::from_secs(v.max(0.0) as u64)
}

/// The validation settings of one object: the replay keys and verdict
/// thresholds resolved at the object, lock waits and limits from its
/// rollout settings.
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub fn validation_settings(
    src: &dyn SettingSource,
    object_ctx: &SettingCtx<'_>,
    policy: &ObjectPolicy,
    rollout: &RolloutSettings,
) -> ValidationSettings {
    let d = ValidationSettings::default();
    let n = |key: &str, default: f64| num(src, key, object_ctx, default);
    let replay = ReplaySettings {
        samples_per_fingerprint: n(k::REPLAY_SAMPLES.key, 20.0).max(1.0) as usize,
        rounds: n(k::REPLAY_ROUNDS.key, 3.0).max(1.0) as usize,
        timeout: Duration::from_secs_f64(n(k::REPLAY_TIMEOUT_SECONDS.key, 10.0).max(1.0)),
        regression_fingerprints: n(k::REGRESSION_FINGERPRINTS.key, 20.0).max(0.0) as usize,
        thresholds: Thresholds {
            min_improvement_pct: policy.observe.min_improvement_pct,
            max_regression_pct: policy.observe.max_regression_pct,
            ..Thresholds::default()
        },
        ..d.replay
    };
    ValidationSettings {
        replay,
        ddl_lock_wait: rollout.ddl_lock_wait,
        abort_replica_lag: rollout.abort_replica_lag,
        max_build: rollout.max_build,
        estimate_margin: policy.estimate_margin,
        canary_hours: n(k::CANARY_HOURS.key, 24.0).max(1.0) as u32,
        max_cover_columns: policy.generation.max_cover_columns,
        ..d
    }
}

/// The rollout and observe settings of one object on `cluster`.
///
/// # Errors
/// Resolution errors.
pub fn resolve_object(
    src: &dyn SettingSource,
    dc: &DcId,
    cluster: &ClusterId,
    object_ctx: &SettingCtx<'_>,
) -> Result<(ObjectPolicy, RolloutSettings, ObserveSettings)> {
    let policy = ObjectPolicy::resolve(src, object_ctx)?;
    let cluster_ctx = SettingCtx::process(dc).with_cluster(cluster);
    let shared = SharedLimits::resolve(src, &cluster_ctx, object_ctx)?;
    let d = RolloutSettings::default();
    let rollout = RolloutSettings {
        ddl_lock_wait: secs(num(src, k::DDL_LOCK_WAIT_SECONDS.key, object_ctx, 5.0)),
        abort_replica_lag: Duration::from_secs(u64::from(shared.abort_replica_lag_seconds)),
        max_build: Duration::from_secs_f64(
            num(src, k::MAX_BUILD_HOURS.key, object_ctx, 6.0).max(0.01) * 3600.0,
        ),
        rollout_pause: secs(num(src, k::ROLLOUT_PAUSE_MINUTES.key, object_ctx, 10.0) * 60.0),
        user_ddl_cooldown: secs(num(src, k::COOLDOWN_HOURS.key, object_ctx, 24.0) * 3600.0),
        max_builds_per_cluster: shared.max_builds_per_cluster,
        build_window: shared.build_window,
        ..d
    };
    let observe = ObserveSettings {
        // Reads are counted as often as the advisor evaluates (decision
        // O1): a cadence, never a period of disuse.
        usage_every: secs(num(src, k::EVAL_INTERVAL_MINUTES.key, object_ctx, 60.0) * 60.0),
        ..ObserveSettings::default()
    };
    Ok((policy, rollout, observe))
}

#[async_trait::async_trait]
impl AutoIndexEnv for ControllerEnv {
    async fn catalog(&self, tenant: TenantId) -> Result<(Conn, AutoIndexStore)> {
        let locator = self.sessions.as_ref().unwrap_or(&self.locator);
        let ep = locator.writer(tenant).await?;
        let mut conn = crate::catalogs::acquire(&ep.pool, "auto_index pass").await?;
        // Tenants bootstrapped before the module joined bootstrap get its
        // tables here; the migrator is idempotent.
        let migrated = self
            .migrated
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&tenant);
        if !migrated {
            crate::catalogs::migrate_catalog(
                locator,
                tenant,
                &mut conn,
                ep.store.schema(),
                &Migrator::new(&[AUTOINDEX_MODULE])?,
            )
            .await?;
            self.migrated
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(tenant);
        }
        Ok((conn, AutoIndexStore::of(&ep.store)))
    }

    async fn objects(
        &self,
        tenant: TenantId,
        objects: &BTreeSet<ObjectId>,
    ) -> Result<BTreeMap<ObjectId, ObjectEnv>> {
        let mut out = BTreeMap::new();
        if objects.is_empty() {
            return Ok(out);
        }
        let ep = self.locator.writer(tenant).await?;
        let snap = crate::catalogs::bounded(
            "auto_index objects",
            SNAPSHOT_WORK,
            ep.store.snapshot(&ep.pool, None),
        )
        .await?;
        let reg = Self::registry_ref()?;
        let cat = TenantCatalog::build(reg, tenant, Loaded::core(snap));
        // R0: a tenant's shards live on its catalog's cluster.
        let Some(cluster) = self.locator.cluster_of(tenant).await? else {
            return Ok(out);
        };
        let view = self.view(&cluster).await?;
        let empty = MemSettings::new(reg);
        let held = self.live.as_ref().map(LiveSettings::snapshot);
        let etcd = held.as_deref().unwrap_or(&empty);
        let src = Layered {
            etcd,
            tenant: Some(&cat.settings),
        };
        for &object in objects {
            let Some(o) = cat.objects.get(object) else {
                continue;
            };
            let Some(db) = cat.databases.get(o.db_id) else {
                continue;
            };
            let shards: Vec<ShardTable> = shard_schemas(tenant, db)
                .into_iter()
                .filter_map(|s| match s {
                    SchemaName::Shard { shard, .. } => Some(ShardTable {
                        shard: shard.0.to_string(),
                        schema: s.to_string(),
                        table: o.name.clone(),
                    }),
                    _ => None,
                })
                .collect();
            if shards.is_empty() {
                continue;
            }
            let ctx = cat.object_ctx(&self.dc, object);
            let (policy, rollout, observe) = resolve_object(&src, &self.dc, &cluster, &ctx)?;
            let validation = validation_settings(&src, &ctx, &policy, &rollout);
            out.insert(
                object,
                ObjectEnv {
                    object,
                    view: view.clone(),
                    shards,
                    mode: policy.mode,
                    rules: policy.observe,
                    rollout,
                    observe,
                    validation,
                },
            );
        }
        Ok(out)
    }

    async fn external_builds(&self, cluster: &ClusterId) -> Result<u32> {
        self.jobs.builds_on(cluster).await
    }
}

/// Trigger evaluation and selection for the loop: builds the evaluator's
/// context from the tenant's catalog snapshot, its cluster (statistics from
/// the writer) and the settings, and runs
/// [`Evaluator::evaluate_at`](sprig_advisor::autoindex::evaluate::Evaluator::evaluate_at).
/// The evaluator writes its own rows; the retirements selection wants are
/// handed back to the loop.
pub struct EvalShim {
    env: Arc<ControllerEnv>,
    evaluator: Evaluator,
    workload: Arc<dyn QualifySource>,
    decisions: Arc<dyn DecisionLog>,
}

impl std::fmt::Debug for EvalShim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvalShim").finish_non_exhaustive()
    }
}

impl EvalShim {
    /// Evaluation over `env`, reading the workload from `workload`.
    #[must_use]
    pub fn new(
        env: Arc<ControllerEnv>,
        clock: Arc<dyn sprig_common::time::Clock>,
        workload: Arc<dyn QualifySource>,
        decisions: Arc<dyn DecisionLog>,
    ) -> Self {
        Self {
            env,
            evaluator: Evaluator::new(clock),
            workload,
            decisions,
        }
    }
}

#[async_trait::async_trait]
impl CandidateSource for EvalShim {
    async fn evaluate(&self, tenant: TenantId, now: SystemTime) -> Result<Evaluated> {
        let env = &self.env;
        let ep = env.locator.writer(tenant).await?;
        let snap = crate::catalogs::bounded(
            "auto_index evaluation",
            SNAPSHOT_WORK,
            ep.store.snapshot(&ep.pool, None),
        )
        .await?;
        let reg = ControllerEnv::registry_ref()?;
        let cat = TenantCatalog::build(reg, tenant, Loaded::core(snap));
        let Some(cluster) = env.locator.cluster_of(tenant).await? else {
            return Ok(Evaluated::default());
        };
        let validation_node = env
            .registry
            .get(&cluster)
            .await?
            .and_then(|c| {
                c.body()
                    .profile
                    .as_ref()
                    .map(|p| p.derived.validation.is_some())
            })
            .unwrap_or(false);
        let mut metas: BTreeMap<ObjectId, ObjectMeta> = BTreeMap::new();
        let mut tables: BTreeMap<ObjectId, (String, String)> = BTreeMap::new();
        for o in cat.objects.iter() {
            if !matches!(o.state, ObjectState::Active | ObjectState::Altering) {
                continue;
            }
            let Some(db) = cat.databases.get(o.db_id) else {
                continue;
            };
            metas.insert(
                o.object_id,
                ObjectMeta {
                    db: o.db_id,
                    data_bytes: None,
                    validation_node,
                    build_rate_bytes_per_sec: None,
                },
            );
            if let Some(s) = shard_schemas(tenant, db).first() {
                tables.insert(o.object_id, (s.to_string(), o.name.clone()));
            }
        }
        let stats = NodeStats {
            pool: env.locator.pools().writer(&cluster).await?,
            tables,
            reader: StatsReader::default(),
        };
        let store = CatalogStore::new(ep.pool.clone(), ep.store.schema());
        let empty = MemSettings::new(reg);
        let held = env.live.as_ref().map(LiveSettings::snapshot);
        let etcd = held.as_deref().unwrap_or(&empty);
        let src = Layered {
            etcd,
            tenant: Some(&cat.settings),
        };
        let cx = EvalCx {
            tenant,
            settings: &src,
            ctx: cat.setting_ctx(&env.dc),
            workload: &*self.workload,
            objects: &metas,
            stats: &stats,
            store: &store,
            decisions: &*self.decisions,
        };
        // Paced by `auto_index.eval_interval_minutes`, resolved at the
        // tenant; the loop only asks often.
        let _ = now;
        let Some(ev) = self.evaluator.evaluate_if_due(&cx).await? else {
            return Ok(Evaluated::default());
        };
        Ok(Evaluated {
            retire: ev
                .tables
                .iter()
                .filter_map(|t| t.retire.clone().map(|(n, r)| (t.object, n, r)))
                .collect(),
        })
    }
}

/// The `auto_index` loop's clock for scenario tests: the process clock
/// moved forward by the whole seconds in a file (`[controller]
/// autoindex_test_clock_file`), re-read at most once a second. A missing,
/// empty or unreadable file is no offset. Only the automatic-indexing loop
/// (evaluation, rollout, observation) reads it, never leases or elections,
/// so a test can run "31 days later" to see unused indexes dropped
/// without changing any setting. Development only.
pub struct SkewClock {
    inner: Arc<dyn sprig_common::time::Clock>,
    file: std::path::PathBuf,
    cached: std::sync::Mutex<(std::time::Instant, Duration)>,
}

impl std::fmt::Debug for SkewClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SkewClock")
            .field("file", &self.file)
            .finish_non_exhaustive()
    }
}

impl SkewClock {
    /// `inner` moved forward by the seconds in `file`.
    #[must_use]
    pub fn new(inner: Arc<dyn sprig_common::time::Clock>, file: std::path::PathBuf) -> Self {
        let past = std::time::Instant::now()
            .checked_sub(Duration::from_secs(2))
            .unwrap_or_else(std::time::Instant::now);
        Self {
            inner,
            file,
            cached: std::sync::Mutex::new((past, Duration::ZERO)),
        }
    }

    fn offset(&self) -> Duration {
        let mut c = self
            .cached
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if c.0.elapsed() >= Duration::from_secs(1) {
            let secs = std::fs::read_to_string(&self.file)
                .ok()
                .and_then(|t| t.trim().parse::<u64>().ok())
                .unwrap_or(0);
            let d = Duration::from_secs(secs);
            if d != c.1 {
                tracing::warn!(offset_s = secs, "auto_index test clock moved");
            }
            *c = (std::time::Instant::now(), d);
        }
        c.1
    }
}

impl sprig_common::time::Clock for SkewClock {
    fn now(&self) -> std::time::Instant {
        self.inner.now() + self.offset()
    }
    fn wall(&self) -> SystemTime {
        self.inner.wall() + self.offset()
    }
}
