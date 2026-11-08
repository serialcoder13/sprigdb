//! Another data center's tier over the same store (chapter 24, single-etcd
//! mode).
//!
//! In R0 one etcd serves the global tier and every DC tier
//! (`/sprig/g/`, `/sprig/dc/<dc_id>/`), so a process can read another
//! DC's shard maps and cluster records with its own connection. Code that
//! takes a store and builds keys from [`MetaStore::tier`] (the cluster
//! registry, tenant maps, the mover's job store) then works on DC `x`
//! through [`TierView`]`::over(store, Tier::Dc(x))`. From R2 the relay's
//! `RemoteMaps` replaces these reads (chapter 23, "The relay").

use std::sync::Arc;
use std::time::Duration;

use sprig_common::Result;

use crate::{
    Consistency, Key, KeyValue, LeaseId, ListOpts, ListPage, MetaStore, Revision, Tier, Txn,
    TxnResult, WatchStream,
};

/// `inner` seen as tier `tier`: every call goes to `inner` unchanged; only
/// [`MetaStore::tier`] differs.
#[derive(Clone)]
pub struct TierView {
    inner: Arc<dyn MetaStore>,
    tier: Tier,
}

impl std::fmt::Debug for TierView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TierView")
            .field("tier", &self.tier)
            .field("over", self.inner.tier())
            .finish()
    }
}

impl TierView {
    /// `store` as `tier`; `store` itself when it already is that tier.
    #[must_use]
    pub fn over(store: &Arc<dyn MetaStore>, tier: Tier) -> Arc<dyn MetaStore> {
        if *store.tier() == tier {
            return Arc::clone(store);
        }
        Arc::new(Self {
            inner: Arc::clone(store),
            tier,
        })
    }
}

#[async_trait::async_trait]
impl MetaStore for TierView {
    fn tier(&self) -> &Tier {
        &self.tier
    }
    async fn get(&self, key: &Key, c: Consistency) -> Result<Option<KeyValue>> {
        self.inner.get(key, c).await
    }
    async fn list(&self, prefix: &Key, opts: ListOpts) -> Result<ListPage> {
        self.inner.list(prefix, opts).await
    }
    async fn txn(&self, txn: Txn) -> Result<TxnResult> {
        self.inner.txn(txn).await
    }
    async fn watch(&self, prefix: &Key, from: Revision) -> Result<WatchStream> {
        self.inner.watch(prefix, from).await
    }
    async fn grant_lease(&self, ttl: Duration) -> Result<LeaseId> {
        self.inner.grant_lease(ttl).await
    }
    async fn keep_alive(&self, lease: LeaseId) -> Result<Option<Duration>> {
        self.inner.keep_alive(lease).await
    }
    async fn revoke_lease(&self, lease: LeaseId) -> Result<()> {
        self.inner.revoke_lease(lease).await
    }
}

#[cfg(test)]
mod tests {
    use sprig_common::ids::DcId;

    use super::*;
    use crate::{MemStore, Op};

    #[tokio::test]
    async fn a_view_reads_and_writes_the_same_store() {
        let a = Tier::Dc(DcId::new("dc-a").unwrap());
        let b = Tier::Dc(DcId::new("dc-b").unwrap());
        let store: Arc<dyn MetaStore> = Arc::new(MemStore::new(a.clone()));
        assert!(Arc::ptr_eq(&TierView::over(&store, a), &store));
        let view = TierView::over(&store, b.clone());
        assert_eq!(view.tier(), &b);
        let key = b.key("clusters/dc-b-c1").unwrap();
        view.txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key: key.clone(),
                value: b"{}".to_vec(),
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .unwrap();
        assert!(
            store
                .get(&key, Consistency::Linearizable)
                .await
                .unwrap()
                .is_some()
        );
    }
}
