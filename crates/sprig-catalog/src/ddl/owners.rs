//! Whose DDL jobs are abandoned, from the gateways' process keys (used by
//! DDL engines and the catalog reconciler).
//!
//! Every DDL job's intent row names the engine running it,
//! `gateway:<dc>@<process token>.<n>` for a gateway's
//! ([`DdlEngine::owner_id`](super::DdlEngine)). A gateway process keeps
//! [`GATEWAY_PROCESS_PREFIX`]`<token>` in its DC's tier under its
//! registration lease. Once that key has gone (the process stopped,
//! crashed or restarted, so its lease ran out) and the job is older than
//! [`OWNER_MIN_AGE`], the job is abandoned: the next DDL waiting for its
//! target resolves it at once, and so does the controller's catalog
//! reconciler at its next look, instead of after
//! `catalog.stuck_state_minutes`.
//!
//! [`OWNER_MIN_AGE`] covers a live gateway whose lease lapsed for a moment
//! (its key comes back when it registers again): a job that young is never
//! taken from its owner. Owners of another form (the controller's engine,
//! rows written before owners were recorded) are never gone here.

use std::sync::Arc;
use std::time::Duration;

use sprig_common::ids::DcId;
use sprig_meta::{Consistency, MetaStore, Tier, TierView};

use super::{OwnerCheck, owner_parts};

/// `gateway-processes/<token>` in a DC's tier: a running gateway process,
/// under its registration lease; the value is the gateway's id.
pub const GATEWAY_PROCESS_PREFIX: &str = "gateway-processes/";

/// A job younger than this is never treated as abandoned by another
/// process: several registration leases (`gateway.lease_ttl_seconds`, 10).
pub const OWNER_MIN_AGE: Duration = Duration::from_secs(60);

/// The label of a gateway's DDL engine in `dc` (`gateway:<dc>`).
#[must_use]
pub fn gateway_label(dc: &DcId) -> String {
    format!("gateway:{dc}")
}

/// Whether `owner` (an intent's owner) names a gateway's engine.
#[must_use]
pub fn is_gateway_owner(owner: &str) -> bool {
    owner_parts(owner).is_some_and(|(label, _)| label.starts_with("gateway:"))
}

/// [`OwnerCheck`] over the gateways' process keys, read through `store`
/// (any DC's tier is reached through it).
pub struct ProcessOwners {
    store: Arc<dyn MetaStore>,
}

impl ProcessOwners {
    /// Reads process keys through `store`.
    #[must_use]
    pub fn new(store: Arc<dyn MetaStore>) -> Self {
        Self { store }
    }
}

impl std::fmt::Debug for ProcessOwners {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessOwners").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl OwnerCheck for ProcessOwners {
    async fn gone(&self, owner: &str, age: Duration) -> bool {
        if age < OWNER_MIN_AGE {
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
        let Ok(key) = store
            .tier()
            .key(&format!("{GATEWAY_PROCESS_PREFIX}{token}"))
        else {
            return false;
        };
        // An unreadable store is doubt: not gone.
        matches!(store.get(&key, Consistency::Linearizable).await, Ok(None))
    }
}
