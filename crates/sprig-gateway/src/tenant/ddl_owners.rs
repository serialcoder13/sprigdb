//! Whose DDL jobs are abandoned (bugfix-38).
//!
//! Every DDL job's intent row names the engine running it,
//! `gateway:<dc>@<process token>.<n>` for a gateway's
//! ([`sprig_catalog::ddl::DdlEngine::owner_id`]). A gateway process keeps
//! `gateway-processes/<token>` in its DC's tier under its registration
//! lease ([`crate::registry::PROCESS_PREFIX`]). A DDL that waits for a
//! target another gateway's job holds asks [`GatewayOwners::gone`]: once
//! that process's key has gone (it stopped, crashed or restarted, so its
//! lease ran out) and the job is older than [`MIN_AGE`], the job is
//! resolved at once instead of after `catalog.stuck_state_minutes`.
//!
//! A job this gateway's own engine abandons (the client hung up or ran
//! `KILL QUERY` during the statement, or its outcome was unknown) is
//! resolved by the engine's janitor within seconds, through
//! [`CtlExecutors`]; and by the next DDL that waits for it.
//!
//! [`MIN_AGE`] covers a live gateway whose lease lapsed for a moment
//! (its key comes back when it registers again): a job that young is
//! never taken from its owner. Owners of another form (the controller's
//! engine, rows written before) are never gone here.

use std::sync::Arc;
use std::time::Duration;

use sprig_catalog::ddl::{DdlExecutor, JobExecutors, MariaExecutor, OwnerCheck, owner_parts};
use sprig_common::Result;
use sprig_common::ids::{DcId, TenantId};
use sprig_meta::{Consistency, MetaStore, Tier, TierView};

use super::locator::GatewayLocator;
use crate::registry::PROCESS_PREFIX;

/// A job younger than this is never treated as abandoned by another
/// process: several registration leases (`gateway.lease_ttl_seconds`, 10).
pub const MIN_AGE: Duration = Duration::from_secs(60);

/// The label of a gateway's DDL engine in `dc`.
#[must_use]
pub fn label(dc: &DcId) -> String {
    format!("gateway:{dc}")
}

/// [`OwnerCheck`] over the gateways' process keys, read through `store`
/// (any DC's tier is reached through it).
pub struct GatewayOwners {
    store: Arc<dyn MetaStore>,
}

impl GatewayOwners {
    /// Reads process keys through `store`.
    #[must_use]
    pub fn new(store: Arc<dyn MetaStore>) -> Self {
        Self { store }
    }
}

impl std::fmt::Debug for GatewayOwners {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayOwners").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl OwnerCheck for GatewayOwners {
    async fn gone(&self, owner: &str, age: Duration) -> bool {
        if age < MIN_AGE {
            return false;
        }
        let Some((label, token)) = owner_parts(owner) else {
            return false;
        };
        let Some(dc) = label
            .strip_prefix("gateway:")
            .and_then(|d| DcId::new(d).ok())
        else {
            return false;
        };
        let store = TierView::over(&self.store, Tier::Dc(dc));
        let Ok(key) = store.tier().key(&format!("{PROCESS_PREFIX}{token}")) else {
            return false;
        };
        // An unreadable store is doubt: not gone.
        matches!(store.get(&key, Consistency::Linearizable).await, Ok(None))
    }
}

/// The DDL engine's janitor reaches a tenant's cluster as `sprig_ctl`, as
/// the reconciler does ([`JobExecutors`]).
pub struct CtlExecutors {
    locator: Arc<GatewayLocator>,
}

impl CtlExecutors {
    /// Executors on the clusters `locator` finds.
    #[must_use]
    pub fn new(locator: Arc<GatewayLocator>) -> Self {
        Self { locator }
    }
}

impl std::fmt::Debug for CtlExecutors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CtlExecutors").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl JobExecutors for CtlExecutors {
    async fn executor(&self, tenant: TenantId) -> Result<Arc<dyn DdlExecutor>> {
        let cluster = self.locator.cluster(tenant).await?;
        let pool = self.locator.ctl_pool(&cluster).await?;
        Ok(Arc::new(MariaExecutor::new(pool)))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use sprig_meta::{MemStore, Op, Txn};

    #[tokio::test]
    async fn a_gateway_process_is_gone_once_its_key_is() {
        let dc = DcId::new("dc-a").unwrap();
        let store: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(dc.clone())));
        let owners = GatewayOwners::new(Arc::clone(&store));
        let live = format!("{}@livetoken.1", label(&dc));
        let dead = format!("{}@deadtoken.1", label(&dc));
        let key = store
            .tier()
            .key(&format!("{PROCESS_PREFIX}livetoken"))
            .unwrap();
        store
            .txn(Txn {
                then: vec![Op::Put {
                    key,
                    value: b"gw-1".to_vec(),
                    lease: None,
                }],
                ..Txn::default()
            })
            .await
            .unwrap();
        let old = MIN_AGE + Duration::from_secs(1);
        assert!(!owners.gone(&live, old).await, "its key is there");
        assert!(owners.gone(&dead, old).await, "no key: gone");
        assert!(
            !owners.gone(&dead, Duration::from_secs(5)).await,
            "too young to take"
        );
        assert!(!owners.gone("engine@x.1", old).await, "not a gateway");
        assert!(!owners.gone("gateway:dc-a", old).await, "no process");
    }
}
