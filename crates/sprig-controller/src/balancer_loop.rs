//! Chapter 11's balancer on the DC leader (build steps 6 and 7): every
//! `balancer.interval_seconds` it builds the fleet view, polls moves in
//! flight, plans, schedules, and keeps the recommendations; it also serves
//! `sprigctl explain placement` and `sprigctl explain decision`.
//!
//! **The fleet view in R0.** A tenant's placement unit is its whole R0
//! footprint (catalog and every database's shard schema, on the cluster
//! its `shardmaps/<tenant>/0` names), so the view has one unit per tenant,
//! keyed `t<id>/g0/s0`, as chapter 10's R0 mover moves it. Clusters come
//! from the registry: `active` and `healthy` are placeable, `degraded`
//! takes nothing, `draining` sheds its tenants; capacity is the declared
//! `capacity`; capability flags come from the published profile. Storage,
//! schemas and tables per tenant come from one index-range read of
//! `mysql.innodb_table_stats` (`database_name LIKE 't<id>\_%'`, the
//! table's primary key prefix, never `information_schema`). Busy time,
//! writes and connections wait for the gateways' load reports, which are
//! not sent yet: until then only storage and the caps drive plans.
//!
//! **Moves.** Without a [`MoveExecutor`] (no mover workers in this
//! deployment yet), `auto` runs as `recommend`: decisions are written,
//! nothing moves. With one, `auto` submits chapter 10 jobs.
//!
//! **Settings** are resolved at DC scope (and cluster scope for the
//! per-cluster keys); tenant-scope values wait for the catalogs' settings
//! to be read here, as in the other DC loops.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use mysql_async::prelude::Queryable;
use serde::{Deserialize, Serialize};
use sprig_balancer::decisions::{CatalogDecisionLog, DecisionLog, DecisionRow};
use sprig_balancer::explain::{PlacementExplanation, explain_placement};
use sprig_balancer::model::{
    Capacity, ClusterCaps, ClusterState, ClusterView, FleetView, LoadVector, ShardKey, ShardView,
    TenantView, Timestamp,
};
use sprig_balancer::recommend::{RecommendationBook, RecommendationStore};
use sprig_balancer::settings::{BalancerMode, BalancerSettings, ClusterLimits, GB, TenantKnobs};
use sprig_balancer::{MoveExecutor, PlacementPolicy, PlanReport, Planner, Scheduler};
use sprig_catalog::CatalogLocator;
use sprig_catalog::placement::ShardMap;
use sprig_cluster::health::ClusterHealth;
use sprig_cluster::record::AdminState;
use sprig_cluster::{ClusterRecord, ClusterRegistry};
use sprig_common::ids::{ClusterId, DcId, TenantId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{ListOpts, MetaStore, Record};
use sprig_settings::{LiveSettings, MemSettings, SettingCtx};
use tokio::sync::Mutex;

use crate::catalogs::ClusterPools;
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler};
use crate::tree::Directory;

/// What the loop and the Admin API share.
struct Inner {
    planner: Planner,
    scheduler: Scheduler,
    book: RecommendationBook,
    last_view: Option<FleetView>,
    last_report: Option<PlanReport>,
}

/// The DC leader's balancer (module docs).
pub struct BalancerLoop {
    dc: DcId,
    registry: ClusterRegistry,
    dc_store: Arc<dyn MetaStore>,
    pools: Arc<dyn ClusterPools>,
    dir: Directory,
    live: Option<LiveSettings>,
    clock: Arc<dyn Clock>,
    exec: Option<Arc<dyn MoveExecutor>>,
    log: Arc<CatalogDecisionLog>,
    recs: RecommendationStore,
    inner: Mutex<Inner>,
}

impl std::fmt::Debug for BalancerLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BalancerLoop")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

fn secs(clock: &dyn Clock) -> Timestamp {
    clock
        .wall()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

impl BalancerLoop {
    /// The loop for `dc`. `exec` runs moves; `None` makes `auto` behave
    /// as `recommend`.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dc: DcId,
        registry: ClusterRegistry,
        dc_store: Arc<dyn MetaStore>,
        pools: Arc<dyn ClusterPools>,
        locator: Arc<dyn CatalogLocator>,
        dir: Directory,
        live: Option<LiveSettings>,
        clock: Arc<dyn Clock>,
        exec: Option<Arc<dyn MoveExecutor>>,
        actor: String,
    ) -> Self {
        let tier = dc_store.tier().clone();
        Self {
            recs: RecommendationStore::new(Arc::clone(&dc_store), tier),
            log: Arc::new(CatalogDecisionLog::new(locator)),
            inner: Mutex::new(Inner {
                planner: Planner::new(BalancerSettings::default()),
                scheduler: Scheduler::new(dc.to_string(), actor),
                book: RecommendationBook::new(),
                last_view: None,
                last_report: None,
            }),
            dc,
            registry,
            dc_store,
            pools,
            dir,
            live,
            clock,
            exec,
        }
    }

    fn settings(&self) -> BalancerSettings {
        match &self.live {
            Some(l) => {
                let snap = l.snapshot();
                let ctx = SettingCtx::process(&self.dc);
                resolve_balancer(&snap, &ctx)
            }
            None => BalancerSettings::default(),
        }
    }

    fn limits(&self, c: &ClusterId) -> ClusterLimits {
        let copy_slots = match &self.live {
            Some(l) => {
                let ctx = SettingCtx {
                    cluster: Some(c),
                    ..SettingCtx::process(&self.dc)
                };
                u32::try_from(l.get(&crate::settings::MAX_COPIES_PER_CLUSTER, &ctx))
                    .unwrap_or(u32::MAX)
            }
            None => {
                u32::try_from(crate::settings::MAX_COPIES_PER_CLUSTER.default).unwrap_or(u32::MAX)
            }
        };
        match &self.live {
            Some(l) => {
                let snap = l.snapshot();
                let ctx = SettingCtx {
                    cluster: Some(c),
                    ..SettingCtx::process(&self.dc)
                };
                resolve_limits(&snap, &ctx, copy_slots)
            }
            None => ClusterLimits {
                copy_slots,
                ..ClusterLimits::default()
            },
        }
    }

    /// Builds the view (module docs).
    ///
    /// # Errors
    /// The stores'; a cluster whose scan fails is marked stale instead.
    pub async fn view(&self) -> Result<FleetView> {
        let mut v = FleetView::default();
        for sc in self.registry.list().await? {
            let r = sc.body();
            if r.dc_id != self.dc {
                continue;
            }
            v.add_cluster(cluster_view(r, self.limits(&r.cluster_id)));
        }
        // One unit per tenant homed here: its catalog's cluster.
        let prefix = self.dc_store.tier().key("shardmaps/")?;
        let mut opts = ListOpts::default();
        let mut units: Vec<(TenantId, ClusterId)> = Vec::new();
        loop {
            let page = self.dc_store.list(&prefix, opts.clone()).await?;
            for kv in &page.kvs {
                if !kv.key.as_str().ends_with("/0") {
                    continue;
                }
                if let Ok(m) = Record::<ShardMap>::decode(&kv.value)
                    && let Some(c) = m.body.catalog_cluster()
                {
                    units.push((m.body.tenant_id, c.clone()));
                }
            }
            if !page.more {
                break;
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
        let mut by_cluster: BTreeMap<ClusterId, Vec<TenantId>> = BTreeMap::new();
        for (t, c) in units {
            by_cluster.entry(c).or_default().push(t);
        }
        for (c, tenants) in by_cluster {
            let sizes = match self.scan(&c, &tenants).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(cluster = %c, error = %e, "balancer storage scan failed");
                    if let Some(cv) = v.clusters.get_mut(&c) {
                        cv.probe_stale = true;
                    }
                    BTreeMap::new()
                }
            };
            for t in tenants {
                let load = sizes.get(&t).copied().unwrap_or(LoadVector {
                    schemas: 1.0,
                    ..LoadVector::default()
                });
                let mut tv = TenantView::new(t);
                tv.knobs = self.tenant_knobs(t);
                v.tenants.insert(t, tv);
                v.add_shard(ShardView::new(ShardKey::new(t.0, 0, 0), c.clone(), load));
            }
        }
        Ok(v)
    }

    fn tenant_knobs(&self, t: TenantId) -> TenantKnobs {
        match &self.live {
            Some(l) => {
                let snap = l.snapshot();
                let ctx = SettingCtx::process(&self.dc).for_tenant(t, &[]);
                resolve_knobs(&snap, &ctx)
            }
            None => TenantKnobs::default(),
        }
    }

    /// Bytes, tables and schemas of each tenant on `c`.
    async fn scan(
        &self,
        c: &ClusterId,
        tenants: &[TenantId],
    ) -> Result<BTreeMap<TenantId, LoadVector>> {
        let pool = self.pools.writer(c).await?;
        let mut conn = pool.get_conn().await.map_err(scan_err)?;
        let page = sprig_balancer::load::page_size(&mut conn).await?;
        let mut out = BTreeMap::new();
        for t in tenants {
            let row: Option<(u64, u64, u64)> = conn
                .exec_first(
                    "SELECT COUNT(DISTINCT database_name), COUNT(*), \
                     CAST(COALESCE(SUM(clustered_index_size + sum_of_other_index_sizes), 0) \
                     AS UNSIGNED) FROM mysql.innodb_table_stats WHERE database_name LIKE ?",
                    (format!("t{t}\\_%"),),
                )
                .await
                .map_err(scan_err)?;
            let (schemas, tables, pages) = row.unwrap_or((0, 0, 0));
            #[allow(clippy::cast_precision_loss)]
            out.insert(
                *t,
                LoadVector {
                    storage_bytes: (pages * page) as f64,
                    schemas: schemas.max(1) as f64,
                    tables: tables as f64,
                    ..LoadVector::default()
                },
            );
        }
        Ok(out)
    }

    /// One planning run.
    ///
    /// # Errors
    /// Building the view.
    pub async fn run_once(&self) -> Result<PlanReport> {
        let now = secs(&*self.clock);
        let settings = self.settings();
        let mut view = self.view().await?;
        let mut g = self.inner.lock().await;
        if g.planner.settings != settings {
            let mut p = Planner::new(settings.clone());
            p.avoid_move_back = g.planner.avoid_move_back;
            g.planner = p;
        }
        let no_mover = sprig_balancer::schedule::NoMover;
        let exec: &dyn MoveExecutor = self.exec.as_deref().unwrap_or(&no_mover);
        g.scheduler.poll(&view, exec, &*self.log, now).await;
        g.scheduler.annotate(&mut view, now);
        let report = g.planner.rebalance(&view, now);
        let has_mover = self.exec.is_some();
        let dc_mode = settings.mode;
        let knobs: BTreeMap<TenantId, BalancerMode> = view
            .tenants
            .iter()
            .map(|(t, v)| (*t, v.knobs.mode))
            .collect();
        let mode_of = move |t: TenantId| {
            let m = match (dc_mode, knobs.get(&t).copied().unwrap_or(dc_mode)) {
                (BalancerMode::Off, _) | (_, BalancerMode::Off) => BalancerMode::Off,
                (_, m) => m,
            };
            if m == BalancerMode::Auto && !has_mover {
                BalancerMode::Recommend
            } else {
                m
            }
        };
        if dc_mode != BalancerMode::Off {
            let max = settings.max_concurrent_moves_per_dc as usize;
            g.scheduler
                .schedule(&report, &view, &mode_of, max, exec, &*self.log, now)
                .await;
        }
        let changes = g.book.observe(&view, &report, now);
        if let Err(e) = self.recs.apply(&changes).await {
            tracing::warn!(error = %e, "recommendations not written");
        }
        for (k, n) in g.book.counts() {
            #[allow(clippy::cast_precision_loss)]
            metrics::gauge!("sprig_balancer_recommendations", "kind" => k.as_str()).set(n as f64);
        }
        for (c, f) in &report.fullness {
            metrics::gauge!("sprig_balancer_cluster_full_ratio", "cluster" => c.to_string(),
                "dimension" => f.bottleneck.to_string())
            .set(f.value);
        }
        for (c, l) in view.loads() {
            metrics::gauge!("sprig_balancer_shard_schemas", "cluster" => c.to_string())
                .set(l.schemas);
        }
        metrics::gauge!("sprig_balancer_constraint_violations").set(0.0);
        tracing::info!(dc = %self.dc, clusters = view.clusters.len(), units = view.shards.len(),
            spread = report.spread, trigger = ?report.trigger, moves = report.moves.len(),
            notes = report.notes.len(), "balancer run");
        g.last_view = Some(view);
        g.last_report = Some(report.clone());
        Ok(report)
    }

    /// `sprigctl explain placement <tenant>`.
    ///
    /// # Errors
    /// 39070-family errors building the view; 39360-family when the
    /// tenant doesn't exist.
    pub async fn explain_placement(&self, tenant: &str) -> Result<PlacementExplanation> {
        let t = self.tenant_id(tenant).await?;
        let now = secs(&*self.clock);
        let g = self.inner.lock().await;
        let Some(view) = g.last_view.clone() else {
            drop(g);
            let v = self.view().await?;
            return Ok(explain_placement(
                &v,
                None,
                t,
                self.settings().mode.as_str(),
                now,
            ));
        };
        Ok(explain_placement(
            &view,
            g.last_report.as_ref(),
            t,
            self.tenant_knobs(t).mode.as_str(),
            now,
        ))
    }

    /// `sprigctl explain decision <id> --tenant <name>`.
    ///
    /// # Errors
    /// The catalog's; 39792 when there is no such decision.
    pub async fn decision(&self, tenant: &str, id: u64) -> Result<DecisionRow> {
        let t = self.tenant_id(tenant).await?;
        self.log.get(t, id).await?.ok_or_else(|| {
            SprigError::new(
                SprigCode::AdminRequestInvalid,
                format!("tenant {tenant} has no decision {id}"),
            )
        })
    }

    async fn tenant_id(&self, name: &str) -> Result<TenantId> {
        if let Ok(id) = name.parse::<u64>() {
            return Ok(TenantId(id));
        }
        self.dir
            .by_name(name)
            .await?
            .map(|e| e.body().tenant_id)
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::AdminRequestInvalid,
                    format!("no tenant named {name}"),
                )
            })
    }

    /// The decision log (for other components that record through it).
    #[must_use]
    pub fn decisions(&self) -> Arc<dyn DecisionLog> {
        Arc::clone(&self.log) as Arc<dyn DecisionLog>
    }
}

fn scan_err(e: mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::ClusterUnavailable, "storage scan failed").with_source(e)
}

/// The placement view of one registered cluster.
#[must_use]
pub fn cluster_view(r: &ClusterRecord, limits: ClusterLimits) -> ClusterView {
    #[allow(clippy::cast_precision_loss)]
    let cap = Capacity {
        storage_bytes: r.capacity.storage_gb.unwrap_or(0) as f64 * GB,
        busy_ms_per_sec: r.capacity.busy_ms_per_sec.unwrap_or(0) as f64,
        write_rows_per_sec: r.capacity.write_rows_per_sec.unwrap_or(0) as f64,
        connections: r.capacity.connections.unwrap_or(0) as f64,
        threads: 0.0,
    };
    let health = r.health.as_ref().map(|h| h.state);
    let state = match (r.admin_state, health) {
        (AdminState::Active, Some(ClusterHealth::Healthy)) => ClusterState::Placeable,
        (AdminState::Active, Some(ClusterHealth::Degraded)) => ClusterState::Degraded,
        (AdminState::Draining, _) => ClusterState::Draining,
        _ => ClusterState::Unavailable,
    };
    let mut c = ClusterView::new(r.cluster_id.clone(), r.dc_id.clone(), cap);
    c.state = state;
    c.limits = limits;
    c.caps = match &r.profile {
        Some(p) => ClusterCaps {
            mover_source: p.derived.mover_source,
            mover_target: p.derived.mover_target,
            xa: p.derived.xa,
            zero_loss: p.derived.zero_loss_failover,
            features: std::collections::BTreeSet::new(),
        },
        None => ClusterCaps::default(),
    };
    if let Some(d) = &r.declared {
        c.replica_dcs.clone_from(&d.replica_dcs);
    }
    c.over_table_budget = r.gaps.iter().any(|g| g.contains("over table budget"));
    c
}

fn resolve_balancer(s: &MemSettings<'_>, ctx: &SettingCtx<'_>) -> BalancerSettings {
    BalancerSettings::resolve(s, ctx)
}

fn resolve_limits(s: &MemSettings<'_>, ctx: &SettingCtx<'_>, slots: u32) -> ClusterLimits {
    ClusterLimits::resolve(s, ctx, slots)
}

fn resolve_knobs(s: &MemSettings<'_>, ctx: &SettingCtx<'_>) -> TenantKnobs {
    TenantKnobs::resolve(s, ctx)
}

/// The loop's one key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Dc(pub DcId);

impl std::fmt::Display for Dc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[async_trait::async_trait]
impl Reconciler for Arc<BalancerLoop> {
    type Key = Dc;

    fn name(&self) -> &'static str {
        "balancer"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<Dc> {
        Vec::new()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<Dc>> {
        Ok(vec![Dc(self.dc.clone())])
    }

    async fn reconcile(&self, _cx: &LoopCx, _key: &Dc) -> Result<Outcome> {
        let started = std::time::Instant::now();
        let interval = self.settings().interval;
        if let Err(e) = self.run_once().await {
            tracing::warn!(error = %e, "balancer run failed");
        }
        metrics::histogram!("sprig_balancer_plan_duration_seconds")
            .record(started.elapsed().as_secs_f64());
        Ok(Outcome::RequeueAfter(interval.max(Duration::from_secs(5))))
    }
}

#[cfg(test)]
mod tests;

/// Bootstrap hooks that add chapter 11's `decisions` module to `H`'s, so
/// every new tenant catalog gets its decision log with its core tables.
/// (Older catalogs get it on the first decision: see
/// `CatalogDecisionLog`.)
#[derive(Debug, Clone, Copy, Default)]
pub struct WithDecisions<H>(pub H);

#[async_trait::async_trait]
impl<H: crate::tenants::BootstrapHooks> crate::tenants::BootstrapHooks for WithDecisions<H> {
    fn modules(&self) -> Vec<sprig_catalog::Module> {
        let mut m = self.0.modules();
        if !m
            .iter()
            .any(|x| x.name == sprig_balancer::DECISIONS_MODULE.name)
        {
            m.push(sprig_balancer::DECISIONS_MODULE);
        }
        m
    }

    async fn catalog_ready(
        &self,
        tenant: &crate::tree::DirectoryEntry,
        catalog: &sprig_catalog::CatalogEndpoint,
    ) -> Result<()> {
        self.0.catalog_ready(tenant, catalog).await
    }

    async fn dropping(
        &self,
        tenant: &crate::tree::DirectoryEntry,
        catalog: Option<&sprig_catalog::CatalogEndpoint>,
    ) -> Result<()> {
        self.0.dropping(tenant, catalog).await
    }
}
