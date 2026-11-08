//! Where a tenant lives now, and where it lived: the shard map and the
//! handoff records ([`crate::ownership`]).

use std::sync::Arc;

use sprig_common::Result;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_meta::MetaStore;
use sprig_shard::{Placement, TenantMaps};

use crate::ownership::{self, Handoff};

/// The tenant's placement as the feed reads it.
#[async_trait::async_trait]
pub trait PlacementView: Send + Sync {
    /// The cluster the tenant's map points at now, and the map version.
    ///
    /// # Errors
    /// The store's errors; 39072 for a tenant without a map.
    async fn current(&self, tenant: TenantId) -> Result<(ClusterId, u64)>;

    /// The tenant's handoffs, oldest first.
    ///
    /// # Errors
    /// The store's errors.
    async fn history(&self, tenant: TenantId) -> Result<Vec<Handoff>>;
}

/// [`PlacementView`] on the DC tier: chapter 15's tenant map and the
/// handoff records next to it.
#[derive(Clone)]
pub struct MetaPlacement {
    store: Arc<dyn MetaStore>,
    maps: TenantMaps,
}

impl std::fmt::Debug for MetaPlacement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaPlacement").finish_non_exhaustive()
    }
}

impl MetaPlacement {
    /// Reads `store`, the tenant's home DC tier.
    #[must_use]
    pub fn new(store: Arc<dyn MetaStore>) -> Self {
        Self {
            maps: TenantMaps::new(store.clone()),
            store,
        }
    }
}

#[async_trait::async_trait]
impl PlacementView for MetaPlacement {
    async fn current(&self, tenant: TenantId) -> Result<(ClusterId, u64)> {
        let (m, _) = self.maps.load(tenant).await?;
        let p = Placement::of(&m)?;
        Ok((p.cluster, p.map_version))
    }

    async fn history(&self, tenant: TenantId) -> Result<Vec<Handoff>> {
        ownership::history(&self.store, tenant, 0).await
    }
}
