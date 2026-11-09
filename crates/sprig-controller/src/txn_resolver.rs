//! Chapter 18's resolver on the DC leader (build step 6): finishes
//! distributed transactions left in doubt on this DC's clusters, and
//! deletes decision rows that are no longer needed.
//!
//! ```text
//! txn_resolver, keyed by cluster, on the DC leader:
//!   keys: the registry's clusters that are active, draining or drained
//!   reconcile(C): settings → resolver config (txn.resolver_* at dc scope)
//!                 scan(C): XA RECOVER; branches prepared longer than
//!                   txn.resolver_min_age_seconds are decided by insert race
//!                   at their home anchor and committed or rolled back
//!                 sweep(C): delete finished commit and pending rows, and
//!                   abort rows past txn.abort_record_retention_hours
//!                 again after txn.resolver_interval_seconds
//! ```
//!
//! The pieces:
//!
//! - the admin is `sprig_txn::mysql::MyAdmin` as `sprig_ctl` on the
//!   registry's writer pools, a budget of its own ([`POOL_MAX`] per
//!   cluster), so a stuck resolver never starves catalog calls;
//! - anchors are found with `sprig_txn::MapLocator`: a sharded group's map
//!   (`shardmaps/<tenant>/<group>`) routes the anchor's hash point; a
//!   database's default group lives with the catalog, on the cluster of
//!   the tenant map (`shardmaps/<tenant>/0`), in its one schema there;
//! - one `Resolver` serves every cluster, so the first-seen times of
//!   prepared branches survive across passes (and are lost on a new term:
//!   a new leader waits `min_age` again, which is only slower).
//!
//! Metrics are the resolver's own (`sprig_txn_in_doubt_branches`,
//! `sprig_txn_oldest_prepared_age_seconds`, `sprig_txn_resolutions_total`,
//! `sprig_txn_rollbacks_total{reason="resolver"}`) plus the runtime's loop
//! metrics with `loop="txn_resolver"`.
//!
//! Where this differs from the book, for now: the maps are read in this
//! DC's tier, so tenants homed in another DC aren't located until the relay
//! (R2) carries their maps; a branch owned by a stalled gateway's live
//! connection is retried on every pass, never killed (decision X5).

use std::sync::Arc;

use mysql_async::Pool;
use sprig_cluster::record::AdminState;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::Result;
use sprig_common::ids::{ClusterId, DcId};
use sprig_settings::{LiveSettings, SettingCtx};
use sprig_shard::{ShardMapStore, TenantMaps};
use sprig_txn::mysql::{MyAdmin, PoolSource};
use sprig_txn::{
    AnchorLocator, ClusterAdmin, DbError, DbResult, MapLocator, Resolver, ResolverConfig,
};

use crate::catalogs::{ClusterPools, RegistryPools};
use crate::monitor::ClusterMonitor;
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, Stores};

/// The resolver's connections per cluster.
pub const POOL_MAX: usize = 2;

/// Clusters scanned at once.
pub const WORKERS: usize = 4;

/// [`PoolSource`] over the controller's cluster pools (writers as
/// `sprig_ctl`).
pub struct CtlPools(pub Arc<dyn ClusterPools>);

impl std::fmt::Debug for CtlPools {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CtlPools").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl PoolSource for CtlPools {
    async fn pool(&self, cluster: &ClusterId) -> DbResult<Pool> {
        self.0
            .writer(cluster)
            .await
            .map_err(|e| DbError::new(0, e.to_string()))
    }
}

/// Whether the resolver looks at a cluster in `state`: anything that may
/// hold tenant data.
#[must_use]
pub fn scanned(state: AdminState) -> bool {
    matches!(
        state,
        AdminState::Active | AdminState::Draining | AdminState::Drained
    )
}

/// The resolver loop (module docs).
pub struct ResolverLoop {
    dc: DcId,
    registry: ClusterRegistry,
    live: Option<LiveSettings>,
    resolver: Arc<Resolver>,
}

impl std::fmt::Debug for ResolverLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolverLoop")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

impl ResolverLoop {
    /// A loop over `registry`'s clusters, acting through `admin` and
    /// finding anchors with `locator`.
    #[must_use]
    pub fn new(
        dc: DcId,
        registry: ClusterRegistry,
        admin: Arc<dyn ClusterAdmin>,
        locator: Arc<dyn AnchorLocator>,
        live: Option<LiveSettings>,
    ) -> Self {
        let cfg = resolve(live.as_ref(), &dc);
        Self {
            dc,
            registry,
            live,
            resolver: Arc::new(Resolver::new(admin, locator, cfg)),
        }
    }

    /// The controller's loop: clusters and maps from `stores`, writers from
    /// `monitor`, settings from `live`.
    ///
    /// # Errors
    /// `Internal` never in practice (the registry's prefixes are valid).
    pub fn for_controller(
        dc: &DcId,
        stores: &Stores,
        monitor: Arc<ClusterMonitor>,
        live: LiveSettings,
    ) -> Result<Self> {
        let registry = ClusterRegistry::new(Arc::clone(&stores.global), Arc::clone(&stores.dc))?;
        let pools: Arc<dyn ClusterPools> = Arc::new(
            RegistryPools::new(registry.clone(), Some(monitor)).budget("txn_resolver", POOL_MAX),
        );
        let admin = Arc::new(MyAdmin::over(Arc::new(CtlPools(pools))));
        let locator = MapLocator::new(
            ShardMapStore::new(Arc::clone(&stores.dc)),
            TenantMaps::new(Arc::clone(&stores.dc)),
            Arc::clone(&admin) as Arc<dyn sprig_txn::GroupSchemas>,
        );
        Ok(Self::new(
            dc.clone(),
            registry,
            admin,
            Arc::new(locator),
            Some(live),
        ))
    }

    /// The resolver, for drains and operator commands.
    #[must_use]
    pub fn resolver(&self) -> &Arc<Resolver> {
        &self.resolver
    }

    /// The configuration now, applied to the resolver.
    fn refresh(&self) -> ResolverConfig {
        let cfg = resolve(self.live.as_ref(), &self.dc);
        if cfg != self.resolver.config() {
            tracing::info!(?cfg, "resolver settings changed");
            self.resolver.set_config(cfg.clone());
        }
        cfg
    }
}

/// `txn.resolver_*` and `txn.abort_record_retention_hours` at DC scope.
fn resolve(live: Option<&LiveSettings>, dc: &DcId) -> ResolverConfig {
    live.map_or_else(ResolverConfig::default, |l| {
        ResolverConfig::resolve(&l.snapshot(), &SettingCtx::process(dc))
    })
}

#[async_trait::async_trait]
impl Reconciler for ResolverLoop {
    type Key = ClusterId;

    fn name(&self) -> &'static str {
        "txn_resolver"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    /// No watches: a new cluster joins at the next resync, and each
    /// cluster requeues itself every interval. (Cluster records change on
    /// every health publication, which would only add scans.)
    fn keys_for(&self, _ev: &MetaEvent) -> Vec<ClusterId> {
        Vec::new()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<ClusterId>> {
        Ok(self
            .registry
            .list()
            .await?
            .iter()
            .map(sprig_cluster::registry::StoredCluster::body)
            .filter(|c| scanned(c.admin_state))
            .map(|c| c.cluster_id.clone())
            .collect())
    }

    /// A deposed leader's late pass is harmless: every outcome is decided
    /// by the insert race, and finishing a finished branch is a no-op.
    async fn reconcile(&self, _cx: &LoopCx, key: &ClusterId) -> Result<Outcome> {
        let Some(rec) = self.registry.get(key).await? else {
            return Ok(Outcome::Done);
        };
        if !scanned(rec.body().admin_state) {
            return Ok(Outcome::Done);
        }
        let cfg = self.refresh();
        let scan = match self.resolver.scan(key).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(cluster = %key, error = %e, "resolver scan failed");
                return Ok(Outcome::Blocked {
                    reason: "cluster unreachable",
                });
            }
        };
        if scan.committed + scan.rolled_back + scan.unresolved > 0 {
            tracing::info!(cluster = %key, ?scan, "resolver scan");
        } else if scan.listed > 0 {
            tracing::debug!(cluster = %key, ?scan, "resolver scan");
        }
        // Sprig 2PC (R3): held branches whose transaction was lost.
        match self.resolver.scan_redo(key).await {
            Ok(r) if r.replayed + r.dropped + r.hazards + r.unresolved > 0 => {
                tracing::info!(cluster = %key, ?r, "resolver redo scan");
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(cluster = %key, error = %e, "resolver redo scan failed"),
        }
        match self.resolver.sweep(key).await {
            Ok(s) if s != sprig_txn::SweepReport::default() => {
                tracing::debug!(cluster = %key, ?s, "decision rows swept");
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(cluster = %key, error = %e, "resolver sweep failed"),
        }
        Ok(Outcome::RequeueAfter(cfg.interval))
    }

    fn workers(&self) -> usize {
        WORKERS
    }
}

#[cfg(test)]
mod tests;
