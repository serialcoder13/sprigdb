//! [`MapIo`] on the whole-tenant map (`shardmaps/<tenant>/0`), through
//! `sprig-shard`'s compare-and-put.
//!
//! A hold's `job_id` field is `<job uuid>#<owner token>`, so a worker that
//! lost the job (its token is older) can neither commit nor clear its
//! successor's hold; gateways only care that the range is held.

use sprig_catalog::placement::ShardMap;
use sprig_common::ids::{ClusterId, JobId, TenantId};
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};
use sprig_feed::ownership::{self, Handoff};
use sprig_meta::{Cmp, Op, Record, Txn};
use sprig_shard::TenantMaps;
use sprig_shard::map::{self, Placement};

use crate::io::{CommitOutcome, HandoffPos, MapIo, MapState, Released};

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

    /// The flip and the feed's handoff record in one etcd transaction, so
    /// no reader sees the new owner without knowing where the source's
    /// changes end and the target's begin.
    async fn commit_handoff(
        &self,
        job: JobId,
        token: u64,
        to: &ClusterId,
        h: &HandoffPos,
    ) -> Result<CommitOutcome> {
        let me = holder(job, token);
        let store = self.maps.store().clone();
        let tier = store.tier().clone();
        let attempt = async {
            for _ in 0..8 {
                let (mut m, rev) = self.maps.load(self.tenant).await?;
                if already_moved(&m, job)? {
                    return Ok(false); // a retry after a lost reply
                }
                let from = Placement::of(&m)?.cluster;
                map::flip(&mut m, &me, to)?;
                map::validate(&m)?;
                let rec = Handoff {
                    tenant: self.tenant,
                    group: m.group_id,
                    map_version: m.map_version,
                    from_cluster: from,
                    fence_pos: h.fence.to_string(),
                    to_cluster: to.clone(),
                    start_pos: h.start.to_string(),
                    hlc_floor: sprig_feed::hlc::floor_at(h.at_ms),
                    job_id: me.clone(),
                    committed_at_ms: h.at_ms,
                };
                let key = ShardMap::catalog_key(&tier, self.tenant)?;
                let r = store
                    .txn(Txn {
                        when: vec![Cmp::ModRev(key.clone(), rev)],
                        then: vec![
                            Op::Put {
                                key,
                                value: Record::new(m).encode()?,
                                lease: None,
                            },
                            ownership::put(&tier, &rec)?,
                        ],
                        otherwise: Vec::new(),
                    })
                    .await?;
                if r.succeeded {
                    return Ok(true);
                }
            }
            Err(SprigError::new(
                SprigCode::CatalogConflict,
                format!("tenant {}'s shard map kept changing", self.tenant),
            ))
        };
        match attempt.await {
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
        assert_eq!(
            io.commit(job, 1, &to).await.unwrap(),
            CommitOutcome::Rejected
        );
        assert_eq!(
            io.commit(job, 2, &to).await.unwrap(),
            CommitOutcome::Committed
        );
        // A retried commit after a lost reply is still committed.
        assert_eq!(
            io.commit(job, 2, &to).await.unwrap(),
            CommitOutcome::Committed
        );
        assert_eq!(io.release(job, 2).await.unwrap(), Released::Committed);
        let st = io.read().await.unwrap();
        assert_eq!((st.cluster, st.epoch), (to, 2));
        // Another job's hold is not ours.
        let other = JobId::new_v7();
        io.hold(other, 1, u64::MAX).await.unwrap();
        assert_eq!(io.release(job, 9).await.unwrap(), Released::NotHeld);
        assert_eq!(io.release(other, 1).await.unwrap(), Released::Ours);
    }

    #[tokio::test]
    async fn the_flip_records_the_feed_handoff() {
        let io = setup().await;
        let job = JobId::new_v7();
        let to = ClusterId::new("dc-a-c2").unwrap();
        let h = HandoffPos {
            fence: "1-101-40".parse().unwrap(),
            start: "2-201-77".parse().unwrap(),
            at_ms: 5_000,
        };
        io.hold(job, 1, u64::MAX).await.unwrap();
        assert_eq!(
            io.commit_handoff(job, 1, &to, &h).await.unwrap(),
            CommitOutcome::Committed
        );
        // A retry after a lost reply neither flips nor records twice.
        assert_eq!(
            io.commit_handoff(job, 1, &to, &h).await.unwrap(),
            CommitOutcome::Committed
        );
        let hist = ownership::history(io.maps.store(), TenantId(3), 0)
            .await
            .unwrap();
        assert_eq!(hist.len(), 1);
        let r = &hist[0];
        assert_eq!(r.from_cluster, ClusterId::new("dc-a-c1").unwrap());
        assert_eq!(
            (r.to_cluster.clone(), r.fence_pos.as_str()),
            (to, "1-101-40")
        );
        assert_eq!(r.start_pos, "2-201-77");
        assert_eq!(r.hlc_floor, 6 << 32);
        let (m, _) = io.maps.load(TenantId(3)).await.unwrap();
        assert_eq!(r.map_version, m.map_version);
        // Without the hold, nothing is recorded.
        let other = JobId::new_v7();
        assert_eq!(
            io.commit_handoff(other, 1, &ClusterId::new("dc-a-c3").unwrap(), &h)
                .await
                .unwrap(),
            CommitOutcome::Rejected
        );
        assert_eq!(
            ownership::history(io.maps.store(), TenantId(3), 0)
                .await
                .unwrap()
                .len(),
            1
        );
    }
}
