//! The XA resolver for the mover's cutovers: before and after the fence, the cutover finishes every
//! prepared distributed-transaction branch of the moving tenant on the
//! source cluster, each by its recorded decision (or abort, through the
//! insert race), instead of waiting for the controller's resolver.
//!
//! The worker builds its own `sprig_txn::Resolver` for this, over the
//! registry's writer pools as `sprig_ctl` with a budget of its own
//! ([`POOL_MAX`] per cluster), finding anchors through the DC tier's shard
//! maps exactly as the controller's `txn_resolver` loop does. It never
//! scans or sweeps: only cutovers call it.

use std::sync::Arc;
use std::time::Duration;

use sprig_cluster::registry::ClusterRegistry;
use sprig_common::Result;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_controller::catalogs::{ClusterPools, RegistryPools};
use sprig_controller::txn_resolver::CtlPools;
use sprig_meta::MetaStore;
use sprig_mover::{Drained, PreparedDrain};
use sprig_shard::{ShardMapStore, TenantMaps};
use sprig_txn::mysql::MyAdmin;
use sprig_txn::{MapLocator, Resolver, ResolverConfig};

/// The drain's connections per cluster.
pub const POOL_MAX: usize = 2;

/// [`PreparedDrain`] over a resolver.
pub struct ResolverDrain(pub Arc<Resolver>);

impl std::fmt::Debug for ResolverDrain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolverDrain").finish_non_exhaustive()
    }
}

impl ResolverDrain {
    /// A resolver over `registry`'s writers and the DC tier `dc`'s maps;
    /// rows it inserts name `worker` as their coordinator.
    #[must_use]
    pub fn new(dc: &Arc<dyn MetaStore>, registry: ClusterRegistry, worker: &str) -> Self {
        let pools: Arc<dyn ClusterPools> =
            Arc::new(RegistryPools::new(registry, None).budget("txn_drain", POOL_MAX));
        let admin = Arc::new(MyAdmin::over(Arc::new(CtlPools(pools))));
        let locator = MapLocator::new(
            ShardMapStore::new(Arc::clone(dc)),
            TenantMaps::new(Arc::clone(dc)),
            Arc::clone(&admin) as Arc<dyn sprig_txn::GroupSchemas>,
        );
        let cfg = ResolverConfig {
            id: format!("mover:{worker}"),
            ..ResolverConfig::default()
        };
        Self(Arc::new(Resolver::new(admin, Arc::new(locator), cfg)))
    }
}

#[async_trait::async_trait]
impl PreparedDrain for ResolverDrain {
    async fn drain(
        &self,
        cluster: &ClusterId,
        tenant: TenantId,
        within: Duration,
    ) -> Result<Drained> {
        let r = self
            .0
            .drain_prepared(cluster, tenant, tokio::time::Instant::now() + within)
            .await?;
        Ok(Drained {
            committed: r.committed,
            rolled_back: r.rolled_back,
            remaining: r.remaining,
        })
    }
}
