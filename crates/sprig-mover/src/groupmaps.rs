//! [`MapIo`] on a table group's shard map (chapter 15, "Shard maps in
//! etcd"; chapter 20): the hold, commit and release of a split, merge or
//! shard move.
//!
//! The job's source shards are ranges of the group's map. A hold marks
//! each `HELD` with the holder text `<job uuid>#<owner token>` and the
//! deadline; the commit replaces them, in one compare-and-put on the map's
//! `mod_revision`, with the job's target shards: new shard ids, the target
//! cluster, epoch `max(source epochs) + 1`, `SERVING`, and the holder text
//! as the job that made them (so a retried commit after a lost reply, or a
//! successor worker, can tell the cutover committed). `map_version` and
//! `next_shard_id` rise. The feed's handoff record for the group is written
//! in the same etcd transaction (chapter 38, "Ownership history").
//!
//! [`MapState`] for such a job: before the commit, the sources' cluster
//! and epoch, and their hold; after it, the targets' cluster and epoch,
//! moved by the job.

use std::sync::Arc;

use sprig_common::ids::{ClusterId, GroupId, JobId, ShardId, TenantId};
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};
use sprig_feed::ownership::{self, Handoff};
use sprig_meta::{Cmp, MetaStore, Op, Record, Txn};
use sprig_shard::shardmap::{RangeState, map_key};
use sprig_shard::{RangeEntry, ShardMap, ShardMapStore};

use crate::api::{HashRange, JobSpec};
use crate::io::{CommitOutcome, HandoffPos, MapIo, MapState, Released};
use crate::maps::{holder, parse_holder};

/// How often a compare-and-put is retried against concurrent map writers
/// (other jobs' cutovers of the same group).
const CAS_TRIES: usize = 16;

/// One group's map, as a hash-range job's [`MapIo`].
#[derive(Clone)]
pub struct GroupMapIo {
    meta: Arc<dyn MetaStore>,
    store: ShardMapStore,
    tenant: TenantId,
    group: GroupId,
    sources: Vec<u32>,
    targets: Vec<(u32, HashRange)>,
    to: ClusterId,
    /// A re-key's new `key_spec_digest`, written by the flip (chapter 16):
    /// gateways route the new shards by the new key from then on.
    new_digest: Option<u64>,
}

impl std::fmt::Debug for GroupMapIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupMapIo")
            .field("tenant", &self.tenant)
            .field("group", &self.group)
            .field("sources", &self.sources)
            .finish_non_exhaustive()
    }
}

fn aborted(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::MoveCutoverAborted, m)
}

fn rejected(e: &SprigError) -> bool {
    matches!(e.kind, ErrorKind::Sprig(SprigCode::MoveCutoverAborted))
}

/// Where the job's shards are in a map.
enum Found {
    /// Every source shard is in the map: their indexes.
    Sources(Vec<usize>),
    /// The sources are gone and the targets are there, made by this job.
    Committed,
}

impl GroupMapIo {
    /// The map of `spec`'s group (a hash-range job) in `meta`, the
    /// tenant's home DC tier.
    ///
    /// # Errors
    /// `Internal` for a job that doesn't route by hash range.
    pub fn new(meta: Arc<dyn MetaStore>, spec: &JobSpec) -> Result<Self> {
        let group = spec
            .group()
            .ok_or_else(|| SprigError::internal("a group map needs a hash-range job"))?;
        let to = spec
            .target_cluster()
            .cloned()
            .ok_or_else(|| SprigError::internal("a job without a target"))?;
        let targets = spec
            .targets
            .iter()
            .map(|t| {
                t.range
                    .map(|r| (t.shard, r))
                    .ok_or_else(|| SprigError::internal("a hash-range target without a range"))
            })
            .collect::<Result<Vec<_>>>()?;
        let new_digest = match &spec.routing {
            crate::api::Routing::Rekey { key, .. } => Some(
                sprig_shard::KeySpec::from_json(key)
                    .map_err(|e| SprigError::internal(format!("the re-key's key: {e}")))?
                    .digest,
            ),
            _ => None,
        };
        Ok(Self {
            store: ShardMapStore::new(meta.clone()),
            meta,
            tenant: spec.tenant,
            group: GroupId(group),
            sources: spec.sources.iter().map(|s| s.shard).collect(),
            targets,
            to,
            new_digest,
        })
    }

    fn find(&self, m: &ShardMap, job: JobId) -> Result<Found> {
        let idx: Vec<Option<usize>> = self
            .sources
            .iter()
            .map(|s| m.ranges.iter().position(|r| r.shard == ShardId(*s)))
            .collect();
        if idx.iter().all(Option::is_some) {
            return Ok(Found::Sources(idx.into_iter().flatten().collect()));
        }
        let made_by_job = self.targets.iter().all(|(s, _)| {
            m.shard(ShardId(*s)).is_some_and(|r| {
                r.job_id
                    .as_deref()
                    .and_then(parse_holder)
                    .is_some_and(|(j, _)| j == job)
            })
        });
        if idx.iter().all(Option::is_none) && made_by_job {
            return Ok(Found::Committed);
        }
        Err(SprigError::internal(format!(
            "group {} of tenant {}: the map has neither all of shards {:?} nor the job's \
             targets",
            self.group, self.tenant, self.sources
        )))
    }

    /// Loads, edits and compare-and-puts the map until it sticks; `f`
    /// returns `false` to leave the map as it is.
    async fn update<F>(&self, mut f: F) -> Result<ShardMap>
    where
        F: FnMut(&mut ShardMap) -> Result<bool>,
    {
        for _ in 0..CAS_TRIES {
            let (mut m, rev) = self.store.load(self.tenant, self.group).await?;
            if !f(&mut m)? {
                return Ok(m);
            }
            m.version += 1;
            if self.store.compare_and_put(rev, &m).await?.is_some() {
                return Ok(m);
            }
        }
        Err(SprigError::new(
            SprigCode::CatalogConflict,
            format!(
                "group {} of tenant {}: the shard map kept changing",
                self.group, self.tenant
            ),
        ))
    }

    /// Replaces the held sources with the targets (the flip). The
    /// sources must be contiguous and held by `me`; the targets must cover
    /// exactly their union.
    fn flip(&self, m: &mut ShardMap, idx: &[usize], me: &str) -> Result<()> {
        let mut idx = idx.to_vec();
        idx.sort_unstable();
        if idx.windows(2).any(|w| w[1] != w[0] + 1) {
            return Err(aborted("the job's source shards aren't adjacent"));
        }
        for &i in &idx {
            let r = &m.ranges[i];
            if r.state != RangeState::Held || r.job_id.as_deref() != Some(me) {
                return Err(aborted(format!(
                    "shard {} isn't held by {me} (held by {:?})",
                    r.shard, r.job_id
                )));
            }
        }
        let (first, last) = (idx[0], idx[idx.len() - 1]);
        let lo = m.ranges[first].lo;
        let hi = m.hi(last);
        let mut targets = self.targets.clone();
        targets.sort_by_key(|(_, r)| r.lo);
        let mut at = Some(lo);
        for (s, r) in &targets {
            if Some(r.lo) != at {
                return Err(aborted(format!(
                    "target shard {s} doesn't continue the sources' range"
                )));
            }
            at = r.hi;
            if at.is_none() {
                break;
            }
        }
        if at != hi || targets.last().map(|(_, r)| r.hi) != Some(hi) {
            return Err(aborted("the targets don't cover the sources' range"));
        }
        let epoch = idx.iter().map(|&i| m.ranges[i].epoch).max().unwrap_or(0) + 1;
        let new: Vec<RangeEntry> = targets
            .iter()
            .map(|(s, r)| RangeEntry {
                lo: r.lo,
                shard: ShardId(*s),
                cluster: self.to.clone(),
                epoch,
                state: RangeState::Serving,
                job_id: Some(me.to_owned()),
                hold_until_ms: None,
            })
            .collect();
        m.ranges.splice(first..=last, new);
        let top = targets.iter().map(|(s, _)| *s).max().unwrap_or(0);
        m.next_shard_id = m.next_shard_id.max(top + 1);
        if let Some(d) = self.new_digest {
            // A re-key replaces every range: the map now hashes by the new
            // key, in the same compare-and-put.
            m.key_spec_digest = d;
        }
        Ok(())
    }

    async fn commit_inner(
        &self,
        job: JobId,
        token: u64,
        handoff: Option<&HandoffPos>,
    ) -> Result<CommitOutcome> {
        let me = holder(job, token);
        let tier = self.meta.tier().clone();
        let key = map_key(&tier, self.tenant, self.group)?;
        let attempt = async {
            for _ in 0..CAS_TRIES {
                let (mut m, rev) = self.store.load(self.tenant, self.group).await?;
                let idx = match self.find(&m, job)? {
                    Found::Committed => return Ok(()), // a retry after a lost reply
                    Found::Sources(i) => i,
                };
                let from = m.ranges[idx[0]].cluster.clone();
                self.flip(&mut m, &idx, &me)?;
                m.version += 1;
                m.validate()?;
                let mut then = vec![Op::Put {
                    key: key.clone(),
                    value: Record::new(m.to_record()).encode()?,
                    lease: None,
                }];
                if let Some(h) = handoff {
                    then.push(ownership::put(
                        &tier,
                        &Handoff {
                            tenant: self.tenant,
                            group: self.group.0,
                            map_version: m.version,
                            from_cluster: from,
                            fence_pos: h.fence.to_string(),
                            to_cluster: self.to.clone(),
                            start_pos: h.start.to_string(),
                            hlc_floor: sprig_feed::hlc::floor_at(h.at_ms),
                            job_id: me.clone(),
                            committed_at_ms: h.at_ms,
                        },
                    )?);
                }
                let r = self
                    .meta
                    .txn(Txn {
                        when: vec![Cmp::ModRev(key.clone(), rev)],
                        then,
                        otherwise: Vec::new(),
                    })
                    .await?;
                if r.succeeded {
                    return Ok(());
                }
            }
            Err(SprigError::new(
                SprigCode::CatalogConflict,
                format!(
                    "group {} of tenant {}: the shard map kept changing",
                    self.group, self.tenant
                ),
            ))
        };
        match attempt.await {
            Ok(()) => Ok(CommitOutcome::Committed),
            Err(e) if rejected(&e) => Ok(CommitOutcome::Rejected),
            Err(e) if e.retryable => {
                tracing::warn!(job = %me, error = %e, "commit outcome unknown");
                Ok(CommitOutcome::Unknown)
            }
            Err(e) => Err(e),
        }
    }
}

#[async_trait::async_trait]
impl MapIo for GroupMapIo {
    async fn hold(&self, job: JobId, token: u64, until_ms: u64) -> Result<u64> {
        let me = holder(job, token);
        let mut epoch = 0;
        self.update(|m| {
            let Found::Sources(idx) = self.find(m, job)? else {
                return Err(aborted("the job's cutover already committed"));
            };
            epoch = 0;
            for i in idx {
                let r = &mut m.ranges[i];
                if r.state == RangeState::Held {
                    let theirs = r.job_id.as_deref().and_then(parse_holder);
                    match theirs {
                        // An older owner's hold of this job is taken over.
                        Some((j, t)) if j == job && t <= token => {}
                        _ => {
                            return Err(aborted(format!(
                                "shard {} is held by {:?}",
                                r.shard, r.job_id
                            )));
                        }
                    }
                }
                r.state = RangeState::Held;
                r.job_id = Some(me.clone());
                r.hold_until_ms = Some(until_ms);
                epoch = epoch.max(r.epoch);
            }
            Ok(true)
        })
        .await?;
        Ok(epoch)
    }

    async fn release(&self, job: JobId, token: u64) -> Result<Released> {
        let mut out = Released::NotHeld;
        self.update(|m| {
            let idx = match self.find(m, job)? {
                Found::Committed => {
                    out = Released::Committed;
                    return Ok(false);
                }
                Found::Sources(i) => i,
            };
            out = Released::NotHeld;
            let mut changed = false;
            for i in idx {
                let r = &mut m.ranges[i];
                if r.state != RangeState::Held {
                    continue;
                }
                match r.job_id.as_deref().and_then(parse_holder) {
                    Some((j, t)) if j == job && t <= token => {
                        r.state = RangeState::Serving;
                        r.job_id = None;
                        r.hold_until_ms = None;
                        changed = true;
                        out = Released::Ours;
                    }
                    Some((j, _)) if j == job => {
                        out = Released::Newer;
                        return Ok(false);
                    }
                    _ => {}
                }
            }
            Ok(changed)
        })
        .await?;
        Ok(out)
    }

    async fn commit(&self, job: JobId, token: u64, to: &ClusterId) -> Result<CommitOutcome> {
        if to != &self.to {
            return Err(SprigError::internal("a group commit to another cluster"));
        }
        self.commit_inner(job, token, None).await
    }

    async fn commit_handoff(
        &self,
        job: JobId,
        token: u64,
        to: &ClusterId,
        handoff: &HandoffPos,
    ) -> Result<CommitOutcome> {
        if to != &self.to {
            return Err(SprigError::internal("a group commit to another cluster"));
        }
        self.commit_inner(job, token, Some(handoff)).await
    }

    async fn bump(&self, _job: JobId, _token: u64) -> Result<CommitOutcome> {
        Err(SprigError::new(
            SprigCode::MoveUnsupported,
            "copy-and-swap doesn't run on group maps",
        ))
    }

    async fn read(&self) -> Result<MapState> {
        let (m, _) = self.store.load(self.tenant, self.group).await?;
        // Any job id: `find` only needs it for the committed case, which
        // is told apart below by the targets' holder.
        let src: Vec<&RangeEntry> = self
            .sources
            .iter()
            .filter_map(|s| m.shard(ShardId(*s)))
            .collect();
        if src.len() == self.sources.len() {
            let held = src
                .iter()
                .find(|r| r.state == RangeState::Held)
                .and_then(|r| r.job_id.as_deref().and_then(parse_holder));
            let moved = src
                .iter()
                .find(|r| r.state == RangeState::Serving)
                .and_then(|r| r.job_id.as_deref().and_then(parse_holder));
            return Ok(MapState {
                cluster: src[0].cluster.clone(),
                epoch: src.iter().map(|r| r.epoch).max().unwrap_or(0),
                held_by: held,
                moved_by: if held.is_some() { None } else { moved },
            });
        }
        let tgt: Vec<&RangeEntry> = self
            .targets
            .iter()
            .filter_map(|(s, _)| m.shard(ShardId(*s)))
            .collect();
        if src.is_empty() && tgt.len() == self.targets.len() {
            return Ok(MapState {
                cluster: tgt[0].cluster.clone(),
                epoch: tgt.iter().map(|r| r.epoch).max().unwrap_or(0),
                held_by: None,
                moved_by: tgt[0].job_id.as_deref().and_then(parse_holder),
            });
        }
        Err(SprigError::internal(format!(
            "group {} of tenant {}: the map has neither the job's sources nor its targets",
            self.group, self.tenant
        )))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_common::ids::DcId;
    use sprig_meta::{MemStore, Tier};

    use super::*;

    const HALF: u64 = 1 << 63;

    async fn setup() -> (Arc<dyn MetaStore>, ClusterId) {
        let tier = Tier::Dc(DcId::new("dc-a").unwrap());
        let s: Arc<dyn MetaStore> = Arc::new(MemStore::new(tier));
        let c = ClusterId::new("dc-a-c1").unwrap();
        let m =
            ShardMap::presplit(TenantId(3), GroupId(2), 7, 1, std::slice::from_ref(&c)).unwrap();
        ShardMapStore::new(s.clone()).create(&m).await.unwrap();
        (s, c)
    }

    fn split(c: &ClusterId) -> JobSpec {
        JobSpec::reshard(
            TenantId(3),
            2,
            c,
            vec![(1, vec!["t3_d1_g2_s1".into()], HashRange { lo: 0, hi: None })],
            c,
            vec![
                (
                    2,
                    HashRange {
                        lo: 0,
                        hi: Some(HALF),
                    },
                ),
                (3, HashRange { lo: HALF, hi: None }),
            ],
        )
    }

    #[tokio::test]
    async fn a_split_flips_one_range_into_two() {
        let (s, c) = setup().await;
        let io = GroupMapIo::new(s.clone(), &split(&c)).unwrap();
        let job = JobId::new_v7();
        assert_eq!(io.hold(job, 1, u64::MAX).await.unwrap(), 1);
        // The successor takes the hold over; the old owner can't undo it.
        io.hold(job, 2, u64::MAX).await.unwrap();
        assert_eq!(io.read().await.unwrap().held_by, Some((job, 2)));
        assert_eq!(io.release(job, 1).await.unwrap(), Released::Newer);
        assert_eq!(
            io.commit(job, 1, &c).await.unwrap(),
            CommitOutcome::Rejected
        );
        let h = HandoffPos {
            fence: "1-1-5".parse().unwrap(),
            start: "1-1-9".parse().unwrap(),
            at_ms: 1000,
        };
        assert_eq!(
            io.commit_handoff(job, 2, &c, &h).await.unwrap(),
            CommitOutcome::Committed
        );
        // A retry after a lost reply is committed too, and records nothing twice.
        assert_eq!(
            io.commit_handoff(job, 2, &c, &h).await.unwrap(),
            CommitOutcome::Committed
        );
        assert_eq!(io.release(job, 2).await.unwrap(), Released::Committed);
        let st = io.read().await.unwrap();
        assert_eq!(
            (st.epoch, st.moved_by, st.held_by),
            (2, Some((job, 2)), None)
        );
        let (m, _) = ShardMapStore::new(s.clone())
            .load(TenantId(3), GroupId(2))
            .await
            .unwrap();
        assert_eq!(m.version, 4); // hold, take-over, flip
        assert_eq!(m.next_shard_id, 4);
        let shards: Vec<(u64, u32)> = m.ranges.iter().map(|r| (r.lo, r.shard.0)).collect();
        assert_eq!(shards, vec![(0, 2), (HALF, 3)]);
        assert_eq!(
            ownership::history(&s, TenantId(3), 2).await.unwrap().len(),
            1
        );

        // Merge them back into shard 4.
        let merge = JobSpec::reshard(
            TenantId(3),
            2,
            &c,
            vec![
                (
                    2,
                    vec!["t3_d1_g2_s2".into()],
                    HashRange {
                        lo: 0,
                        hi: Some(HALF),
                    },
                ),
                (
                    3,
                    vec!["t3_d1_g2_s3".into()],
                    HashRange { lo: HALF, hi: None },
                ),
            ],
            &c,
            vec![(4, HashRange { lo: 0, hi: None })],
        );
        let io = GroupMapIo::new(s.clone(), &merge).unwrap();
        let j2 = JobId::new_v7();
        assert_eq!(io.hold(j2, 1, u64::MAX).await.unwrap(), 2);
        assert_eq!(
            io.commit(j2, 1, &c).await.unwrap(),
            CommitOutcome::Committed
        );
        let (m, _) = ShardMapStore::new(s)
            .load(TenantId(3), GroupId(2))
            .await
            .unwrap();
        assert_eq!(m.ranges.len(), 1);
        assert_eq!((m.ranges[0].shard.0, m.ranges[0].epoch), (4, 3));
    }

    #[tokio::test]
    async fn a_rekey_replaces_every_range_and_the_key_digest() {
        let (s, c) = setup().await;
        let key = sprig_shard::KeySpec::new(vec![sprig_shard::KeyClass::Binary]).unwrap();
        let spec = JobSpec::rekey(
            TenantId(3),
            2,
            &c,
            vec![(1, vec!["t3_d1_g2_s1".into()], HashRange { lo: 0, hi: None })],
            &c,
            vec![
                (
                    2,
                    HashRange {
                        lo: 0,
                        hi: Some(HALF),
                    },
                ),
                (3, HashRange { lo: HALF, hi: None }),
            ],
            key.to_json(),
            std::collections::BTreeMap::new(),
        );
        let io = GroupMapIo::new(s.clone(), &spec).unwrap();
        let job = JobId::new_v7();
        io.hold(job, 1, u64::MAX).await.unwrap();
        assert_eq!(
            io.commit(job, 1, &c).await.unwrap(),
            CommitOutcome::Committed
        );
        let (m, _) = ShardMapStore::new(s)
            .load(TenantId(3), GroupId(2))
            .await
            .unwrap();
        assert_eq!(m.key_spec_digest, key.digest);
        let shards: Vec<u32> = m.ranges.iter().map(|r| r.shard.0).collect();
        assert_eq!(shards, vec![2, 3]);
    }

    #[tokio::test]
    async fn another_jobs_hold_is_not_taken_and_release_restores_serving() {
        let (s, c) = setup().await;
        let io = GroupMapIo::new(s.clone(), &split(&c)).unwrap();
        let (a, b) = (JobId::new_v7(), JobId::new_v7());
        io.hold(a, 1, u64::MAX).await.unwrap();
        assert!(io.hold(b, 1, u64::MAX).await.is_err());
        assert_eq!(io.release(b, 1).await.unwrap(), Released::NotHeld);
        assert_eq!(io.release(a, 1).await.unwrap(), Released::Ours);
        let st = io.read().await.unwrap();
        assert_eq!((st.held_by, st.epoch), (None, 1));
        // Without the hold the commit is rejected.
        assert_eq!(io.commit(a, 1, &c).await.unwrap(), CommitOutcome::Rejected);
    }
}
