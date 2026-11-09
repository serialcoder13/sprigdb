//! The catalog reconciler (chapter 08, "Reconciler"; build step 11).
//!
//! Every `catalog.reconcile_interval_seconds` the home DC leader compares
//! each tenant's catalog with `information_schema` on its cluster and
//! repairs what diverged, recording each action as a decision:
//!
//! | Finding (`finding` label) | Action |
//! |---|---|
//! | DDL job older than `catalog.stuck_state_minutes` (`stuck_job`) | Finish if the physical state matches, else roll back; drops roll forward ([`DdlEngine::resolve`]) |
//! | Shard schema missing (`schema_missing`) | Alert (R0 has no mover-created shards to recreate) |
//! | Object missing (`object_missing`) | Alert |
//! | Physical schema with no catalog entry (`orphan_schema`) | Quarantine; drop after `catalog.orphan_retention_days` if empty |
//! | Schema of a dropped database still there (`dropped_schema_exists`) | Drop it: the catalog committed the drop |
//! | Object created outside SprigDB (`outside_object`) | Adopt it ([`DdlEngine::adopt`]) |
//! | Tombstones past `catalog.tombstone_retention_hours` (`tombstones_purged`) | Purge |
//! | Setting values clamped or ignored (`clamped_values`) | Count in `sprig_settings_clamped_values` |
//!
//! Rules that keep repairs from fighting live DDL:
//! - a job younger than the stuck age is left alone, and so is a schema in
//!   which MariaDB is running DDL right now ([`DdlExecutor::busy`]);
//! - a database with any job in flight (its own or one of its objects') is
//!   not compared: a rename that ran but hasn't been published would look
//!   like an outside table.
//!
//! **Purging by age.** Tables have no `changed_at` (chapter 08, open
//! question 7), so the loop keeps marks: on every pass it notes the
//! committed catalog version with the time its version row was last
//! written (`set_at`, never earlier than that version's commit). It purges
//! up to the newest mark older than the retention. A new leader starts
//! with one mark from the version row itself, so a tenant idle for longer
//! than the retention is purged on the first pass, and a busy one a
//! retention after the leader started: late, never early.
//!
//! Quarantine of orphan schemas is remembered by this leader only: a new
//! leader starts the clock again (late, never early). Definition drift and
//! backend grants are not checked yet (chapters 21 and 29).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};

use sprig_catalog::cache::{Loaded, TenantCatalog};
use sprig_catalog::ddl::{
    DdlBackend, DdlEngine, DdlExecutor, DdlIntent, DdlTx, IntentOp, LOWER_CASE_TABLE_NAMES,
    MariaExecutor, ORPHAN_RETENTION_DAYS, RECONCILE_INTERVAL_SECONDS, Resolution, RoutedExecutor,
    STUCK_STATE_MINUTES, TOMBSTONE_RETENTION_HOURS, drop_schema, object_key, shard_schemas,
};
use sprig_catalog::store::{DbState, ObjectKind, ObjectState, PurgeReport, Snapshot};
use sprig_common::ids::{ClusterId, DbId, DcId, GroupId, TenantId};
use sprig_common::names::{SchemaName, is_internal_object};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigError};
use sprig_settings::{LiveSettings, MemSettings, Registry, SettingCtx, TenantScope, Value};

use crate::catalogs::ShardMapLocator;
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler};
use crate::tree::{Directory, StoredEntry, TenantState};

/// Where the reconciler reaches a tenant's cluster.
#[async_trait::async_trait]
pub trait ExecutorSource: Send + Sync + 'static {
    /// An executor on `tenant`'s cluster (R0: the catalog's).
    ///
    /// # Errors
    /// 39072 if the tenant has no catalog here; the cluster's errors.
    async fn executor(&self, tenant: TenantId) -> Result<Arc<dyn DdlExecutor>>;

    /// An executor that also reaches every shard of a multi-shard DDL job
    /// (chapter 21), each on its recorded cluster. The default is
    /// [`executor`](Self::executor) (every shard on the catalog's cluster).
    ///
    /// # Errors
    /// As [`executor`](Self::executor); a shard's cluster unreachable.
    async fn shard_executor(
        &self,
        tenant: TenantId,
        _shards: &[(SchemaName, Option<ClusterId>)],
    ) -> Result<Arc<dyn DdlExecutor>> {
        self.executor(tenant).await
    }
}

/// [`ExecutorSource`] through the catalog's shard map and the cluster
/// pools (`sprig_ctl`).
#[derive(Debug, Clone)]
pub struct PoolExecutors {
    locator: ShardMapLocator,
}

impl PoolExecutors {
    /// Executors on the clusters `locator` finds.
    #[must_use]
    pub fn new(locator: ShardMapLocator) -> Self {
        Self { locator }
    }
}

#[async_trait::async_trait]
impl ExecutorSource for PoolExecutors {
    async fn executor(&self, tenant: TenantId) -> Result<Arc<dyn DdlExecutor>> {
        let cluster = self.locator.cluster_of(tenant).await?.ok_or_else(|| {
            SprigError::new(
                sprig_common::SprigCode::TenantNotReady,
                format!("tenant {tenant} has no catalog in this data center"),
            )
        })?;
        let pool = self.locator.pools().writer(&cluster).await?;
        Ok(Arc::new(MariaExecutor::new(pool)))
    }

    async fn shard_executor(
        &self,
        tenant: TenantId,
        shards: &[(SchemaName, Option<ClusterId>)],
    ) -> Result<Arc<dyn DdlExecutor>> {
        let cluster = self.locator.cluster_of(tenant).await?.ok_or_else(|| {
            SprigError::new(
                sprig_common::SprigCode::TenantNotReady,
                format!("tenant {tenant} has no catalog in this data center"),
            )
        })?;
        let pools = self.locator.pools();
        let mut routed = RoutedExecutor::new(MariaExecutor::new(pools.writer(&cluster).await?));
        for (schema, c) in shards {
            let c = c.clone().unwrap_or_else(|| cluster.clone());
            let pool = pools.writer(&c).await?;
            routed = routed.route(*schema, c, MariaExecutor::new(pool));
        }
        Ok(Arc::new(routed))
    }
}

/// One executor for every tenant (tests, single-cluster tools).
#[derive(Clone)]
pub struct FixedExecutor(pub Arc<dyn DdlExecutor>);

impl std::fmt::Debug for FixedExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FixedExecutor").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl ExecutorSource for FixedExecutor {
    async fn executor(&self, _tenant: TenantId) -> Result<Arc<dyn DdlExecutor>> {
        Ok(Arc::clone(&self.0))
    }
}

/// What the reconciler found, the metric label `finding`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Finding {
    /// A DDL job past the stuck age.
    StuckJob,
    /// A database's shard schema is missing.
    SchemaMissing,
    /// An object in the catalog is missing from its schema.
    ObjectMissing,
    /// A physical schema of the tenant with no catalog entry.
    OrphanSchema,
    /// A dropped database's schema still exists.
    DroppedSchemaExists,
    /// An object created outside SprigDB.
    OutsideObject,
    /// Tombstones purged.
    TombstonesPurged,
    /// The tenant's count of clamped or ignored setting values changed.
    ClampedValues,
    /// A database of a tenant being deleted, dropped so the deletion can
    /// finish.
    DeletedTenantDatabase,
}

impl Finding {
    /// The metric label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Finding::StuckJob => "stuck_job",
            Finding::SchemaMissing => "schema_missing",
            Finding::ObjectMissing => "object_missing",
            Finding::OrphanSchema => "orphan_schema",
            Finding::DroppedSchemaExists => "dropped_schema_exists",
            Finding::OutsideObject => "outside_object",
            Finding::TombstonesPurged => "tombstones_purged",
            Finding::ClampedValues => "clamped_values",
            Finding::DeletedTenantDatabase => "deleted_tenant_database",
        }
    }
}

/// One finding and what was done about it (chapter 11's decision log
/// takes these once it exists).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileDecision {
    /// The tenant.
    pub tenant: TenantId,
    /// What was found.
    pub finding: Finding,
    /// What it was about: a job, a schema, a database or object name.
    pub subject: String,
    /// What was done: `finished`, `rolled_back`, `rolled_forward`,
    /// `left`, `alert`, `quarantined`, `dropped`, `adopted`, `purged`,
    /// `counted`.
    pub action: &'static str,
    /// Details for people.
    pub detail: String,
}

/// Where decisions go.
#[async_trait::async_trait]
pub trait DecisionLog: Send + Sync + 'static {
    /// Records `d`.
    async fn record(&self, d: &ReconcileDecision);
}

/// Logs decisions as `decision` events until chapter 11's log exists.
#[derive(Debug, Clone, Copy, Default)]
pub struct LogDecisions;

#[async_trait::async_trait]
impl DecisionLog for LogDecisions {
    async fn record(&self, d: &ReconcileDecision) {
        let level_warn = matches!(d.action, "alert" | "left" | "quarantined");
        if level_warn {
            tracing::warn!(event = "decision", tenant = %d.tenant, finding = d.finding.label(),
                subject = %d.subject, action = d.action, detail = %d.detail, "catalog reconciler");
        } else {
            tracing::info!(event = "decision", tenant = %d.tenant, finding = d.finding.label(),
                subject = %d.subject, action = d.action, detail = %d.detail, "catalog reconciler");
        }
    }
}

/// The loop's timing and limits, resolved per pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileConfig {
    /// `catalog.reconcile_interval_seconds`.
    pub interval: Duration,
    /// `catalog.stuck_state_minutes`.
    pub stuck: Duration,
    /// `catalog.orphan_retention_days`.
    pub orphan_retention: Duration,
    /// `catalog.tombstone_retention_hours`.
    pub tombstone_retention: Duration,
}

impl Default for ReconcileConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(RECONCILE_INTERVAL_SECONDS.default),
            stuck: Duration::from_secs(STUCK_STATE_MINUTES.default * 60),
            orphan_retention: Duration::from_secs(ORPHAN_RETENTION_DAYS.default * 86_400),
            tombstone_retention: Duration::from_secs(TOMBSTONE_RETENTION_HOURS.default * 3600),
        }
    }
}

impl ReconcileConfig {
    /// The keys' values for this DC.
    #[must_use]
    pub fn resolve(live: &LiveSettings, dc: &DcId) -> Self {
        let ctx = SettingCtx::process(dc);
        Self {
            interval: Duration::from_secs(live.get(&RECONCILE_INTERVAL_SECONDS, &ctx)),
            stuck: Duration::from_secs(live.get(&STUCK_STATE_MINUTES, &ctx).saturating_mul(60)),
            orphan_retention: Duration::from_secs(
                live.get(&ORPHAN_RETENTION_DAYS, &ctx)
                    .saturating_mul(86_400),
            ),
            tombstone_retention: Duration::from_secs(
                live.get(&TOMBSTONE_RETENTION_HOURS, &ctx)
                    .saturating_mul(3600),
            ),
        }
    }
}

/// What one pass over a tenant did.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PassReport {
    /// Stuck jobs and how each was resolved.
    pub resolved: Vec<(String, Resolution)>,
    /// Jobs left alone: too young, or their schema busy.
    pub jobs_waiting: usize,
    /// Objects adopted.
    pub adopted: usize,
    /// Shard schemas missing.
    pub schemas_missing: Vec<SchemaName>,
    /// Objects missing, as `database.name`.
    pub objects_missing: Vec<String>,
    /// Orphan schemas seen.
    pub orphans: Vec<SchemaName>,
    /// Orphan schemas dropped.
    pub orphans_dropped: Vec<SchemaName>,
    /// Schemas of dropped databases dropped.
    pub dropped_schemas: Vec<SchemaName>,
    /// What a purge removed, if one ran.
    pub purged: Option<(u64, PurgeReport)>,
    /// Clamped or ignored setting values, by scope label.
    pub clamped: BTreeMap<&'static str, usize>,
    /// Steps that failed (the pass carried on).
    pub errors: Vec<String>,
    /// Databases of a tenant being deleted, dropped.
    pub deleted_databases: Vec<String>,
}

#[derive(Debug, Default)]
struct TenantMemory {
    /// (when the version row was written, the version): purge marks.
    marks: Vec<(SystemTime, u64)>,
    purged_to: u64,
    /// Orphan schemas and when this leader first saw them.
    orphans: HashMap<SchemaName, SystemTime>,
    last_pass: Option<std::time::Instant>,
    clamped: BTreeMap<&'static str, usize>,
}

/// The `catalog_reconciler` loop.
pub struct CatalogReconciler<B: DdlBackend>
where
    B::Tx: DdlTx,
{
    engine: DdlEngine<B>,
    directory: Directory,
    executors: Arc<dyn ExecutorSource>,
    decisions: Arc<dyn DecisionLog>,
    dc: DcId,
    live: Option<LiveSettings>,
    fixed: Option<ReconcileConfig>,
    clock: Arc<dyn Clock>,
    memory: Mutex<HashMap<TenantId, TenantMemory>>,
    /// The groups with shard maps (chapter 20): their shard schemas are
    /// never orphans. `None`: no shard schema of a non-default group is.
    group_maps: Option<Arc<dyn GroupMaps>>,
}

/// The table groups of a tenant that have shard maps (chapters 15 and
/// 20). A split's new shards, a merge's target and a retired source all
/// belong to such a group: their schemas are the mover's and the sharding
/// loop's to create and drop, never the reconciler's.
#[async_trait::async_trait]
pub trait GroupMaps: Send + Sync {
    /// Group ids with a shard map (group 0, the catalog's, excluded).
    ///
    /// # Errors
    /// The store's.
    async fn groups(&self, tenant: TenantId) -> Result<BTreeSet<GroupId>>;
}

#[async_trait::async_trait]
impl GroupMaps for sprig_shard::ShardMapStore {
    async fn groups(&self, tenant: TenantId) -> Result<BTreeSet<GroupId>> {
        Ok(self
            .list(tenant)
            .await?
            .into_iter()
            .map(|m| m.group)
            .filter(|g| g.0 != 0)
            .collect())
    }
}

impl<B: DdlBackend> std::fmt::Debug for CatalogReconciler<B>
where
    B::Tx: DdlTx,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogReconciler")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

fn scope_label(s: TenantScope) -> &'static str {
    match s {
        TenantScope::Tenant => "tenant",
        TenantScope::Database(_) => "database",
        TenantScope::Object(_) => "object",
        TenantScope::Ancestor(_) => "ancestor",
    }
}

fn short(text: &str) -> String {
    let t: String = text.chars().take(120).collect();
    if t.len() < text.len() {
        format!("{t}...")
    } else {
        t
    }
}

impl<B: DdlBackend> CatalogReconciler<B>
where
    B::Tx: DdlTx,
{
    /// The loop for tenants homed in `dc`. `live` gives the `catalog.*`
    /// keys and the etcd scopes the clamped count resolves against.
    pub fn new(
        engine: DdlEngine<B>,
        directory: Directory,
        executors: Arc<dyn ExecutorSource>,
        decisions: Arc<dyn DecisionLog>,
        dc: DcId,
        live: Option<LiveSettings>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            engine,
            directory,
            executors,
            decisions,
            dc,
            live,
            fixed: None,
            clock,
            memory: Mutex::new(HashMap::new()),
            group_maps: None,
        }
    }

    /// Recognises the shard schemas of groups with shard maps in
    /// `maps` (chapter 20); without it, no shard schema of a group other
    /// than a database's default group is ever an orphan.
    #[must_use]
    pub fn with_group_maps(mut self, maps: Arc<dyn GroupMaps>) -> Self {
        self.group_maps = Some(maps);
        self
    }

    /// The same loop with fixed timing (tests).
    #[must_use]
    pub fn with_config(mut self, cfg: ReconcileConfig) -> Self {
        self.fixed = Some(cfg);
        self
    }

    fn config(&self) -> ReconcileConfig {
        match (&self.fixed, &self.live) {
            (Some(c), _) => *c,
            (None, Some(l)) => ReconcileConfig::resolve(l, &self.dc),
            (None, None) => ReconcileConfig::default(),
        }
    }

    fn memory(&self) -> MutexGuard<'_, HashMap<TenantId, TenantMemory>> {
        self.memory.lock().unwrap_or_else(PoisonError::into_inner)
    }

    async fn decide(
        &self,
        tenant: TenantId,
        finding: Finding,
        subject: String,
        action: &'static str,
        detail: String,
    ) {
        metrics::counter!("sprig_catalog_reconcile_findings_total", "finding" => finding.label())
            .increment(1);
        self.decisions
            .record(&ReconcileDecision {
                tenant,
                finding,
                subject,
                action,
                detail,
            })
            .await;
    }

    /// One full pass over `tenant` (the loop runs it every interval).
    ///
    /// # Errors
    /// Only when the catalog or the cluster can't be read at all; a step
    /// that fails is reported in [`PassReport::errors`] and the pass
    /// carries on.
    #[allow(clippy::too_many_lines)] // the chapter's table, in order
    pub async fn pass(&self, tenant: TenantId) -> Result<PassReport> {
        let cfg = self.config();
        let now = self.clock.wall();
        let backend = self.engine.versioner().backend();
        let exec = self.executors.executor(tenant).await?;
        let mut report = PassReport::default();
        let mut snap = backend.full_snapshot(tenant).await?;

        // 1. Stuck jobs.
        let mut resolved_any = false;
        for intent in DdlIntent::all(&snap.settings) {
            let age = now.duration_since(intent.started()).unwrap_or_default();
            // Chapter 21: a multi-shard job reaches every shard's cluster.
            let (exec, schemas) = match &intent.shards {
                Some(set) => {
                    let routes = set.routes();
                    match self.executors.shard_executor(tenant, &routes).await {
                        Ok(e) => (e, routes.into_iter().map(|(s, _)| s).collect()),
                        Err(e) => {
                            report.errors.push(format!("job {}: {e}", intent.job));
                            continue;
                        }
                    }
                }
                None => (Arc::clone(&exec), intent_schemas(tenant, &intent, &snap)),
            };
            let mut busy = false;
            for s in &schemas {
                busy |= exec.busy(s).await.unwrap_or(true);
            }
            if age < cfg.stuck || busy {
                report.jobs_waiting += 1;
                continue;
            }
            let subject = format!("job {}", intent.job);
            match self.engine.resolve(tenant, &intent, &snap, &*exec).await {
                Ok(r) => {
                    resolved_any = true;
                    let detail = match &r {
                        Resolution::Left { reason } => format!("{reason}: {}", short(&intent.ddl)),
                        _ => format!("{} after {} s", short(&intent.ddl), age.as_secs()),
                    };
                    self.decide(
                        tenant,
                        Finding::StuckJob,
                        subject.clone(),
                        r.label(),
                        detail,
                    )
                    .await;
                    report.resolved.push((intent.job.to_string(), r));
                }
                Err(e) => report.errors.push(format!("{subject}: {e}")),
            }
        }
        if resolved_any {
            snap = backend.full_snapshot(tenant).await?;
        }
        let lctn = self.lower_case_table_names(tenant, &snap);

        // A tenant being deleted: nobody can log in to drop its databases
        // any more, and cleanup waits for them (chapter 08, "Deleting a
        // tenant"), so they go here, through the DDL path, once no job
        // holds them; the rest of the pass doesn't apply.
        if self
            .directory
            .get(tenant)
            .await?
            .is_some_and(|e| e.body().state == TenantState::Deleting)
        {
            let held = held_databases(&snap);
            for db in snap
                .databases
                .iter()
                .filter(|d| d.state != DbState::Dropped && !held.contains(&d.db_id))
            {
                let sql = format!(
                    "DROP DATABASE IF EXISTS {}",
                    sprig_common::names::quote(&db.name)
                );
                let req = sprig_catalog::ddl::DdlRequest {
                    tenant,
                    statement: sprig_catalog::ddl::DdlStatement::DropDatabase {
                        name: db.name.clone(),
                        if_exists: true,
                    },
                    sql: &sql,
                    logical_sql: &sql,
                    current_db: None,
                    user: None,
                    lower_case_table_names: lctn,
                };
                match self.engine.execute(&req, &*exec).await {
                    Ok(_) => {
                        self.decide(
                            tenant,
                            Finding::DeletedTenantDatabase,
                            db.name.clone(),
                            "dropped",
                            format!("database {} of a tenant being deleted", db.name),
                        )
                        .await;
                        report.deleted_databases.push(db.name.clone());
                    }
                    Err(e) => report.errors.push(format!("database {}: {e}", db.name)),
                }
            }
            return Ok(report);
        }

        // 2. Databases against their schemas.
        let held: BTreeSet<DbId> = held_databases(&snap);
        for db in snap
            .databases
            .iter()
            .filter(|d| d.state == DbState::Active && !held.contains(&d.db_id))
        {
            for s in shard_schemas(tenant, db) {
                if exec.busy(&s).await.unwrap_or(true) {
                    continue;
                }
                let found = match exec.schema(&s).await {
                    Ok(Some(_)) => match exec.objects(&s).await {
                        Ok(f) => f,
                        Err(e) => {
                            report.errors.push(format!("{s}: {e}"));
                            continue;
                        }
                    },
                    Ok(None) => {
                        self.decide(
                            tenant,
                            Finding::SchemaMissing,
                            db.name.clone(),
                            "alert",
                            format!("shard schema {s} of database {} is missing", db.name),
                        )
                        .await;
                        report.schemas_missing.push(s);
                        continue;
                    }
                    Err(e) => {
                        report.errors.push(format!("{s}: {e}"));
                        continue;
                    }
                };
                let rows: Vec<_> = snap
                    .objects
                    .iter()
                    .filter(|o| o.db_id == db.db_id && o.state != ObjectState::Dropped)
                    .collect();
                let known: BTreeSet<(_, String)> = rows
                    .iter()
                    .map(|o| (o.namespace, o.name_key.clone()))
                    .collect();
                let physical: BTreeSet<(_, String)> = found
                    .iter()
                    .map(|p| (p.kind.namespace(), object_key(p.kind, &p.name, lctn)))
                    .collect();
                let outside: Vec<_> = found
                    .iter()
                    .filter(|p| !is_internal_object(&p.name))
                    .filter(|p| {
                        !known.contains(&(p.kind.namespace(), object_key(p.kind, &p.name, lctn)))
                    })
                    .cloned()
                    .collect();
                for o in rows
                    .iter()
                    .filter(|o| o.state == ObjectState::Active && sql_object(o.kind))
                {
                    if !physical.contains(&(o.namespace, o.name_key.clone())) {
                        let name = format!("{}.{}", db.name, o.name);
                        self.decide(
                            tenant,
                            Finding::ObjectMissing,
                            name.clone(),
                            "alert",
                            format!("{} {name} is in the catalog but not in {s}", o.kind),
                        )
                        .await;
                        report.objects_missing.push(name);
                    }
                }
                if !outside.is_empty() {
                    match self.engine.adopt(tenant, db, &outside, &*exec, lctn).await {
                        Ok(Some((v, n))) => {
                            let names: Vec<String> =
                                outside.iter().map(|p| p.name.clone()).collect();
                            self.decide(
                                tenant,
                                Finding::OutsideObject,
                                db.name.clone(),
                                "adopted",
                                format!("{n} object(s) at version {v}: {}", names.join(", ")),
                            )
                            .await;
                            report.adopted += n;
                        }
                        Ok(None) => {}
                        Err(e) => report.errors.push(format!("adopting in {s}: {e}")),
                    }
                }
            }
        }

        // 3. Schemas of dropped databases.
        for db in snap
            .databases
            .iter()
            .filter(|d| d.state == DbState::Dropped)
        {
            for s in shard_schemas(tenant, db) {
                match exec.schema(&s).await {
                    Ok(Some(_)) => match exec.execute(&drop_schema(s)).await {
                        Ok(()) => {
                            self.decide(
                                tenant,
                                Finding::DroppedSchemaExists,
                                s.to_string(),
                                "dropped",
                                format!(
                                    "database {} was dropped at version {}",
                                    db.name, db.changed_version
                                ),
                            )
                            .await;
                            report.dropped_schemas.push(s);
                        }
                        Err(e) => report.errors.push(format!("dropping {s}: {e}")),
                    },
                    Ok(None) => {}
                    Err(e) => report.errors.push(format!("{s}: {e}")),
                }
            }
        }

        // 4. Orphan schemas.
        let expected: BTreeSet<SchemaName> = snap
            .databases
            .iter()
            .flat_map(|d| shard_schemas(tenant, d))
            .collect();
        // Shard schemas of table groups (splits, merges, the advisor's
        // regroup) are known when their group has a shard map; when the
        // maps can't be read, every one of them is known.
        let default_groups: BTreeSet<GroupId> = expected
            .iter()
            .filter_map(|s| match s {
                SchemaName::Shard { group, .. } => Some(*group),
                _ => None,
            })
            .collect();
        let mapped: Option<BTreeSet<GroupId>> = match &self.group_maps {
            Some(m) => m.groups(tenant).await.ok(),
            None => None,
        };
        let known_group = |s: &SchemaName| match s {
            SchemaName::Shard { group, .. } if !default_groups.contains(group) => {
                mapped.as_ref().is_none_or(|m| m.contains(group))
            }
            _ => false,
        };
        match exec.tenant_schemas(tenant).await {
            Ok(present) => {
                let orphans: Vec<SchemaName> = present
                    .into_iter()
                    .filter(|s| !expected.contains(s) && !known_group(s))
                    .collect();
                let mut fresh = Vec::new();
                let mut due = Vec::new();
                {
                    let mut mem = self.memory();
                    let m = mem.entry(tenant).or_default();
                    m.orphans.retain(|s, _| orphans.contains(s));
                    for s in &orphans {
                        match m.orphans.get(s) {
                            None => {
                                m.orphans.insert(*s, now);
                                fresh.push(*s);
                            }
                            Some(seen)
                                if now.duration_since(*seen).unwrap_or_default()
                                    >= cfg.orphan_retention =>
                            {
                                due.push(*s);
                            }
                            Some(_) => {}
                        }
                    }
                }
                for s in fresh {
                    self.decide(
                        tenant,
                        Finding::OrphanSchema,
                        s.to_string(),
                        "quarantined",
                        "a physical schema with no catalog entry".to_owned(),
                    )
                    .await;
                }
                for s in due {
                    match exec.objects(&s).await {
                        Ok(objs) if objs.is_empty() => match exec.execute(&drop_schema(s)).await {
                            Ok(()) => {
                                self.memory().entry(tenant).or_default().orphans.remove(&s);
                                self.decide(
                                    tenant,
                                    Finding::OrphanSchema,
                                    s.to_string(),
                                    "dropped",
                                    "empty after the quarantine".to_owned(),
                                )
                                .await;
                                report.orphans_dropped.push(s);
                            }
                            Err(e) => report.errors.push(format!("dropping {s}: {e}")),
                        },
                        Ok(_) => {}
                        Err(e) => report.errors.push(format!("{s}: {e}")),
                    }
                }
                report.orphans = orphans;
            }
            Err(e) => report.errors.push(format!("listing schemas: {e}")),
        }

        // 5. Purge tombstones past the retention.
        match backend.version_mark(tenant).await {
            Ok((v, at)) => {
                let up_to = {
                    let mut mem = self.memory();
                    let m = mem.entry(tenant).or_default();
                    match m.marks.last_mut() {
                        // The same version seen written earlier: keep the
                        // earlier time (still no earlier than its commit).
                        Some((t, last)) if *last == v => *t = (*t).min(at),
                        _ => m.marks.push((at, v)),
                    }
                    let cutoff = now
                        .checked_sub(cfg.tombstone_retention)
                        .unwrap_or(SystemTime::UNIX_EPOCH);
                    let up_to = m
                        .marks
                        .iter()
                        .filter(|(t, _)| *t <= cutoff)
                        .map(|(_, v)| *v)
                        .max();
                    // Keep the newest old mark and every younger one.
                    if let Some(u) = up_to {
                        m.marks.retain(|(_, v)| *v >= u);
                    }
                    up_to.filter(|u| *u > m.purged_to)
                };
                if let Some(u) = up_to {
                    match backend.purge(tenant, u).await {
                        Ok(r) => {
                            self.memory().entry(tenant).or_default().purged_to = u;
                            let n = r.databases + r.objects + r.settings + r.schema_versions;
                            if n > 0 {
                                self.decide(tenant, Finding::TombstonesPurged, format!("up to version {u}"), "purged",
                                    format!("{} database(s), {} object(s), {} setting row(s), {} schema version(s)",
                                        r.databases, r.objects, r.settings, r.schema_versions)).await;
                            }
                            report.purged = Some((u, r));
                        }
                        Err(e) => report.errors.push(format!("purge: {e}")),
                    }
                }
            }
            Err(e) => report.errors.push(format!("version mark: {e}")),
        }

        // 6. Clamped and ignored setting values.
        match self.clamped(tenant, snap) {
            Ok(counts) => {
                let changed = {
                    let mut mem = self.memory();
                    let m = mem.entry(tenant).or_default();
                    let changed = m.clamped != counts;
                    m.clamped.clone_from(&counts);
                    changed
                };
                self.publish_clamped();
                if changed {
                    let total: usize = counts.values().sum();
                    self.decide(
                        tenant,
                        Finding::ClampedValues,
                        "settings".to_owned(),
                        "counted",
                        format!("{total} value(s) clamped or ignored: {counts:?}"),
                    )
                    .await;
                }
                report.clamped = counts;
            }
            Err(e) => report.errors.push(format!("clamped values: {e}")),
        }

        self.memory().entry(tenant).or_default().last_pass = Some(self.clock.now());
        for e in &report.errors {
            tracing::warn!(%tenant, error = %e, "catalog reconciler step failed");
        }
        Ok(report)
    }

    fn registry(&self) -> Result<&'static Registry> {
        match &self.live {
            Some(l) => Ok(l.registry()),
            None => Registry::global(),
        }
    }

    fn lower_case_table_names(&self, tenant: TenantId, snap: &Snapshot) -> bool {
        let Ok(reg) = self.registry() else {
            return false;
        };
        let cat = TenantCatalog::build(reg, tenant, Loaded::core(snap.clone()));
        let empty = MemSettings::new(reg);
        let held = self.live.as_ref().map(LiveSettings::snapshot);
        let etcd = held.as_deref().unwrap_or(&empty);
        cat.settings
            .resolve(etcd, LOWER_CASE_TABLE_NAMES.key, &cat.setting_ctx(&self.dc))
            .is_ok_and(|r| r.value == Value::Int(1))
    }

    fn clamped(&self, tenant: TenantId, snap: Snapshot) -> Result<BTreeMap<&'static str, usize>> {
        let reg = self.registry()?;
        let cat = TenantCatalog::build(reg, tenant, Loaded::core(snap));
        let empty = MemSettings::new(reg);
        let held = self.live.as_ref().map(LiveSettings::snapshot);
        let etcd = held.as_deref().unwrap_or(&empty);
        let mut counts = BTreeMap::new();
        for c in cat.clamped(etcd, &self.dc) {
            *counts.entry(scope_label(c.scope)).or_insert(0) += 1;
        }
        Ok(counts)
    }

    /// Sets `sprig_settings_clamped_values{scope}` to the sum over the
    /// tenants this leader reconciled.
    fn publish_clamped(&self) {
        let mut sums: BTreeMap<&'static str, usize> = ["tenant", "database", "object"]
            .into_iter()
            .map(|s| (s, 0))
            .collect();
        for m in self.memory().values() {
            for (k, v) in &m.clamped {
                *sums.entry(k).or_insert(0) += v;
            }
        }
        for (scope, n) in sums {
            #[allow(clippy::cast_precision_loss)] // counts far below 2^52
            metrics::gauge!("sprig_settings_clamped_values", "scope" => scope).set(n as f64);
        }
    }
}

/// The SQL kinds the physical comparison covers.
fn sql_object(kind: ObjectKind) -> bool {
    matches!(
        kind,
        ObjectKind::Table
            | ObjectKind::View
            | ObjectKind::Sequence
            | ObjectKind::Procedure
            | ObjectKind::Function
            | ObjectKind::Trigger
            | ObjectKind::Event
    )
}

/// Databases with a job in flight: their own, or one of their objects'.
fn held_databases(snap: &Snapshot) -> BTreeSet<DbId> {
    let mut held = BTreeSet::new();
    for i in DdlIntent::all(&snap.settings) {
        if let Some(db) = i.database() {
            held.insert(db);
        }
        if let IntentOp::Rename { moves } = &i.op {
            held.extend(moves.iter().map(|m| m.db));
        }
    }
    for o in &snap.objects {
        if o.pending_job_id.is_some() || o.state == ObjectState::Creating {
            held.insert(o.db_id);
        }
    }
    for d in &snap.databases {
        if d.state == DbState::Creating {
            held.insert(d.db_id);
        }
    }
    held
}

/// The schemas a job works in.
fn intent_schemas(tenant: TenantId, intent: &DdlIntent, snap: &Snapshot) -> Vec<SchemaName> {
    let mut dbs: BTreeSet<DbId> = BTreeSet::new();
    if let Some(db) = intent.database() {
        dbs.insert(db);
    }
    for id in intent.objects() {
        if let Some(o) = snap.objects.iter().find(|o| o.object_id == id) {
            dbs.insert(o.db_id);
        }
    }
    if let IntentOp::Rename { moves } = &intent.op {
        dbs.extend(moves.iter().map(|m| m.db));
    }
    snap.databases
        .iter()
        .filter(|d| dbs.contains(&d.db_id))
        .flat_map(|d| shard_schemas(tenant, d))
        .collect()
}

/// Whether the loop reconciles a tenant in `state`.
const fn reconcilable(state: TenantState) -> bool {
    // A tenant being deleted too: a stuck job or a database left behind
    // would otherwise keep it `deleting` for ever.
    matches!(state, TenantState::Active | TenantState::Deleting)
}

#[async_trait::async_trait]
impl<B: DdlBackend> Reconciler for CatalogReconciler<B>
where
    B::Tx: DdlTx,
{
    type Key = TenantId;

    fn name(&self) -> &'static str {
        "catalog_reconciler"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<TenantId> {
        // Periodic only: every resync lists the tenants, and each pass
        // asks to come back after the interval.
        Vec::new()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<TenantId>> {
        let entries = self.directory.list().await?;
        Ok(entries
            .iter()
            .map(StoredEntry::body)
            .filter(|e| e.home_dc == self.dc && reconcilable(e.state))
            .map(|e| e.tenant_id)
            .collect())
    }

    async fn reconcile(&self, _cx: &LoopCx, key: &TenantId) -> Result<Outcome> {
        let cfg = self.config();
        let Some(entry) = self.directory.get(*key).await? else {
            self.memory().remove(key);
            self.publish_clamped();
            return Ok(Outcome::Done);
        };
        let e = entry.body();
        if e.home_dc != self.dc || !reconcilable(e.state) {
            return Ok(Outcome::Done);
        }
        let since = self
            .memory()
            .get(key)
            .and_then(|m| m.last_pass)
            .map(|t| self.clock.now().saturating_duration_since(t));
        if let Some(s) = since
            && s < cfg.interval
        {
            return Ok(Outcome::RequeueAfter(cfg.interval.saturating_sub(s)));
        }
        self.pass(*key).await?;
        Ok(Outcome::RequeueAfter(cfg.interval))
    }
}

#[cfg(test)]
mod live_tests;
#[cfg(test)]
mod tests;
