//! [`MapIo`] on chapter 15's R0 tenant map (`shardmaps/<tenant>/0`),
//! through `sprig-shard`'s compare-and-put (chapter 10, "Cutover";
//! chapter 15, "Epochs").
//!
//! A hold's `job_id` field is `<job uuid>#<owner token>`, so a worker that
//! lost the job (its token is older) can neither commit nor clear its
//! successor's hold; gateways only care that the range is held.

use sprig_catalog::placement::ShardMap;
use sprig_common::ids::{ClusterId, JobId, TenantId};
use sprig_common::{ErrorKind, Result, SprigCode};
use sprig_shard::TenantMaps;
use sprig_shard::map::{self, Placement};

use crate::io::{CommitOutcome, MapIo, MapState, Released};

/// One tenant's map, as a job's [`MapIo`].
#[derive(Debug, Clone)]
pub struct TenantMapIo {
    maps: TenantMaps,
    tenant: TenantId,
}

impl TenantMapIo {
    /// `tenant`'s map in `maps`.
    #[must_use]
    pub fn new(maps: TenantMaps, tenant: TenantId) -> Self {
        Self { maps, tenant }
    }
}

/// The holder text of `job`'s owner `token`.
#[must_use]
pub fn holder(job: JobId, token: u64) -> String {
    format!("{}#{token}", job.0)
}

/// A holder text's job and token (a bare job id has token 0).
#[must_use]
pub fn parse_holder(s: &str) -> Option<(JobId, u64)> {
    let (j, t) = s.split_once('#').unwrap_or((s, "0"));
    Some((j.parse().ok()?, t.parse().ok()?))
}

/// The map already shows `job`'s commit.
fn already_moved(m: &ShardMap, job: JobId) -> Result<bool> {
    let p = Placement::of(m)?;
    Ok(p.hold.is_none()
        && p.moved_by
            .as_deref()
            .and_then(parse_holder)
            .is_some_and(|(j, _)| j == job))
}

fn rejected(e: &sprig_common::SprigError) -> bool {
    matches!(e.kind, ErrorKind::Sprig(SprigCode::MoveCutoverAborted))
}

#[async_trait::async_trait]
impl MapIo for TenantMapIo {
    async fn hold(&self, job: JobId, token: u64, until_ms: u64) -> Result<u64> {
        let me = holder(job, token);
        let m = self
            .maps
            .update(self.tenant, |m| {
                // An older owner's hold of the same job is taken over.
                if let Some(h) = Placement::of(m)?.hold
                    && let Some((j, t)) = parse_holder(&h.job)
                    && j == job
                    && t < token
                {
                    map::release(m, &h.job);
                }
                map::hold(m, &me, until_ms)?;
                Ok(true)
            })
            .await?;
        Ok(Placement::of(&m)?.epoch)
    }

    async fn release(&self, job: JobId, token: u64) -> Result<Released> {
        let mut out = Released::NotHeld;
        self.maps
            .update(self.tenant, |m| {
                let held = Placement::of(m)?
                    .hold
                    .and_then(|h| parse_holder(&h.job).map(|x| (h.job, x)));
                match held {
                    Some((text, (j, t))) if j == job && t <= token => {
                        map::release(m, &text);
                        out = Released::Ours;
                        Ok(true)
                    }
                    Some((_, (j, _))) if j == job => {
                        out = Released::Newer;
                        Ok(false)
                    }
                    Some(_) => {
                        out = Released::NotHeld;
                        Ok(false)
                    }
                    None => {
                        out = if already_moved(m, job)? {
                            Released::Committed
                        } else {
                            Released::NotHeld
                        };
                        Ok(false)
                    }
                }
            })
            .await?;
        Ok(out)
    }

    async fn commit(&self, job: JobId, token: u64, to: &ClusterId) -> Result<CommitOutcome> {
        let me = holder(job, token);
        match self
            .maps
            .update(self.tenant, |m| {
                if already_moved(m, job)? {
                    return Ok(false); // a retry after a lost reply
                }
                map::flip(m, &me, to)?;
                Ok(true)
            })
            .await
        {
            Ok(_) => Ok(CommitOutcome::Committed),
            Err(e) if rejected(&e) => Ok(CommitOutcome::Rejected),
            Err(e) if e.retryable => {
                tracing::warn!(job = %me, error = %e, "commit outcome unknown");
                Ok(CommitOutcome::Unknown)
            }
            Err(e) => Err(e),
        }
    }

    async fn bump(&self, job: JobId, token: u64) -> Result<CommitOutcome> {
        let me = holder(job, token);
        let r = self
            .maps
            .update(self.tenant, |m| {
                if already_moved(m, job)? {
                    return Ok(false);
                }
                let cluster = Placement::of(m)?.cluster;
                map::flip(m, &me, &cluster)?;
                Ok(true)
            })
            .await;
        match r {
            Ok(_) => Ok(CommitOutcome::Committed),
            Err(e) if rejected(&e) => Ok(CommitOutcome::Rejected),
            Err(e) => Err(e),
        }
    }

    async fn read(&self) -> Result<MapState> {
        let (m, _) = self.maps.load(self.tenant).await?;
        let p = Placement::of(&m)?;
        Ok(MapState {
            cluster: p.cluster,
            epoch: p.epoch,
            held_by: p.hold.and_then(|h| parse_holder(&h.job)),
            moved_by: p.moved_by.as_deref().and_then(parse_holder),
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::sync::Arc;

    use sprig_common::ids::DcId;
    use sprig_meta::{Cmp, MemStore, MetaStore, Op, Record, Tier, Txn};

    use super::*;

    async fn setup() -> TenantMapIo {
        let tier = Tier::Dc(DcId::new("dc-a").unwrap());
        let s: Arc<dyn MetaStore> = Arc::new(MemStore::new(tier.clone()));
        let key = ShardMap::catalog_key(&tier, TenantId(3)).unwrap();
        let m = ShardMap::catalog(TenantId(3), ClusterId::new("dc-a-c1").unwrap(), 1);
        s.txn(Txn {
            when: vec![Cmp::Missing(key.clone())],
            then: vec![Op::Put {
                key,
                value: Record::new(m).encode().unwrap(),
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .unwrap();
        TenantMapIo::new(TenantMaps::new(s), TenantId(3))
    }

    #[tokio::test]
    async fn an_older_owner_cant_undo_a_newer_ones_hold() {
        let io = setup().await;
        let job = JobId::new_v7();
        let to = ClusterId::new("dc-a-c2").unwrap();
        io.hold(job, 1, u64::MAX).await.unwrap();
        // The successor (token 2) takes the hold over.
        io.hold(job, 2, u64::MAX).await.unwrap();
        assert_eq!(io.read().await.unwrap().held_by, Some((job, 2)));
        assert_eq!(io.release(job, 1).await.unwrap(), Released::Newer);
        assert_eq!(io.commit(job, 1, &to).await.unwrap(), CommitOutcome::Rejected);
        assert_eq!(io.commit(job, 2, &to).await.unwrap(), CommitOutcome::Committed);
        // A retried commit after a lost reply is still committed.
        assert_eq!(io.commit(job, 2, &to).await.unwrap(), CommitOutcome::Committed);
        assert_eq!(io.release(job, 2).await.unwrap(), Released::Committed);
        let st = io.read().await.unwrap();
        assert_eq!((st.cluster, st.epoch), (to, 2));
        // Another job's hold is not ours.
        let other = JobId::new_v7();
        io.hold(other, 1, u64::MAX).await.unwrap();
        assert_eq!(io.release(job, 9).await.unwrap(), Released::NotHeld);
        assert_eq!(io.release(other, 1).await.unwrap(), Released::Ours);
    }
}
