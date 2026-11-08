//! [`MapIo`] for a move between data centers (chapter 24, "Orchestrating a
//! move"): the hold and release are the source DC's tenant map, as for a
//! move inside one DC (every gateway routing to the tenant follows that
//! map, whatever its own DC); the commit is the home change of
//! [`sprig_shard::home::commit`], which flips the source map, writes the
//! target DC's map and moves the directory's `home_dc` in one transaction.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use sprig_common::Result;
use sprig_common::ids::{ClusterId, DcId, JobId, TenantId};
use sprig_meta::MetaStore;
use sprig_shard::home::{self, HomeChange, HomeCommit};

use crate::io::{CommitOutcome, MapIo, MapState, Released};
use crate::maps::{TenantMapIo, holder};

/// One tenant's move from DC `from` to DC `to`.
#[derive(Clone)]
pub struct HomeMapIo {
    source: TenantMapIo,
    store: Arc<dyn MetaStore>,
    tenant: TenantId,
    from: DcId,
    to: DcId,
}

impl std::fmt::Debug for HomeMapIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HomeMapIo")
            .field("source", &self.source)
            .field("tenant", &self.tenant)
            .field("from", &self.from)
            .field("to", &self.to)
            .finish_non_exhaustive()
    }
}

impl HomeMapIo {
    /// `tenant`'s move from `from` to `to`; `store` is any tier of the one
    /// etcd (single-etcd mode).
    #[must_use]
    pub fn new(store: Arc<dyn MetaStore>, tenant: TenantId, from: DcId, to: DcId) -> Self {
        Self {
            source: TenantMapIo::new(home::maps_in(&store, &from), tenant),
            store,
            tenant,
            from,
            to,
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[async_trait::async_trait]
impl MapIo for HomeMapIo {
    async fn hold(&self, job: JobId, token: u64, until_ms: u64) -> Result<u64> {
        self.source.hold(job, token, until_ms).await
    }

    async fn release(&self, job: JobId, token: u64) -> Result<Released> {
        self.source.release(job, token).await
    }

    async fn commit(&self, job: JobId, token: u64, to: &ClusterId) -> Result<CommitOutcome> {
        let change = HomeChange {
            tenant: self.tenant,
            from: self.from.clone(),
            to: self.to.clone(),
            cluster: to.clone(),
            holder: holder(job, token),
            now_ms: now_ms(),
        };
        match home::commit(&self.store, &change).await {
            Ok(HomeCommit::Committed(epoch)) => {
                tracing::info!(tenant = %self.tenant, from = %self.from, to = %self.to, epoch, "home DC changed");
                Ok(CommitOutcome::Committed)
            }
            Ok(HomeCommit::Rejected(why)) => {
                tracing::warn!(tenant = %self.tenant, why, "home change rejected");
                Ok(CommitOutcome::Rejected)
            }
            Err(e) if e.retryable => {
                tracing::warn!(tenant = %self.tenant, error = %e, "home change outcome unknown");
                Ok(CommitOutcome::Unknown)
            }
            Err(e) => Err(e),
        }
    }

    async fn bump(&self, _job: JobId, _token: u64) -> Result<CommitOutcome> {
        // Copy-and-swap never crosses DCs.
        Ok(CommitOutcome::Rejected)
    }

    async fn read(&self) -> Result<MapState> {
        self.source.read().await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use serde_json::json;
    use sprig_catalog::placement::{MapRole, ShardMap};
    use sprig_meta::{MemStore, Op, Record, Tier, Txn};

    use super::*;

    #[tokio::test]
    async fn hold_commit_release_across_dcs() {
        let mum1 = DcId::new("mum1").unwrap();
        let fra1 = DcId::new("fra1").unwrap();
        let t = TenantId(9);
        let store: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(mum1.clone())));
        let m = ShardMap::catalog(t, ClusterId::new("mum1-c1").unwrap(), 1);
        store
            .txn(Txn {
                when: vec![],
                then: vec![
                    Op::Put {
                        key: ShardMap::catalog_key(&Tier::Dc(mum1.clone()), t).unwrap(),
                        value: Record::new(m).encode().unwrap(),
                        lease: None,
                    },
                    Op::Put {
                        key: home::directory_key(t).unwrap(),
                        value: serde_json::to_vec(&json!({"v": 1, "home_dc": "mum1",
                            "epoch": 1, "state": "migrating", "migration": {"to": "fra1"}}))
                        .unwrap(),
                        lease: None,
                    },
                ],
                otherwise: vec![],
            })
            .await
            .unwrap();
        let io = HomeMapIo::new(store.clone(), t, mum1.clone(), fra1.clone());
        let job = JobId::new_v7();
        let to = ClusterId::new("fra1-c1").unwrap();
        io.hold(job, 3, u64::MAX).await.unwrap();
        // An older owner can't commit.
        assert_eq!(
            io.commit(job, 2, &to).await.unwrap(),
            CommitOutcome::Rejected
        );
        assert_eq!(
            io.commit(job, 3, &to).await.unwrap(),
            CommitOutcome::Committed
        );
        assert_eq!(
            io.commit(job, 3, &to).await.unwrap(),
            CommitOutcome::Committed
        );
        assert_eq!(io.release(job, 3).await.unwrap(), Released::Committed);
        let st = io.read().await.unwrap();
        assert_eq!((st.cluster, st.epoch), (to, 2));
        let (y, _) = home::maps_in(&store, &fra1).load(t).await.unwrap();
        assert_eq!(y.role, MapRole::Home);
        let d = home::read_home(&*store, t).await.unwrap().unwrap();
        assert_eq!(d.home_dc, fra1);
    }
}
