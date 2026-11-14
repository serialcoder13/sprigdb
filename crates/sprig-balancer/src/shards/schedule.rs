//! Scheduling split, merge and move proposals: limits, mutual exclusion
//! and dampers against thrash.
//!
//! [`admit`] is the guardrail check against running jobs: the tenant's job
//! limit, copy slots per cluster, lock conflicts (a job already holds one
//! of the shards, or the tenant moves whole), and the failed-job backoff.
//! [`build_spec`] turns an admitted proposal into the mover's hash-range
//! [`JobSpec`] with new shard ids, and [`decision`] the decision-log row
//! that records it, the same way placement records moves.

use std::collections::BTreeMap;

use serde_json::json;
use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_mover::api::HashRange;
use sprig_mover::{JobRecord, JobSpec};
use sprig_shard::{ShardMap, shard_schema};

use super::settings::{ShardingMode, ShardingSettings};
use super::triggers::{Proposal, ShardJob};
use crate::decisions::{Mode, NewDecision, Outcome};

/// One unfinished mover job, as the scheduler sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningJob {
    /// The tenant.
    pub tenant: TenantId,
    /// Its group; `None` for a whole-tenant move.
    pub group: Option<u32>,
    /// Source shards.
    pub sources: Vec<u32>,
    /// New shards.
    pub targets: Vec<u32>,
    /// Clusters it reads or writes.
    pub clusters: Vec<ClusterId>,
}

impl RunningJob {
    /// From a job record (any job, finished or not).
    #[must_use]
    pub fn of(r: &JobRecord) -> Self {
        let mut clusters: Vec<ClusterId> = r
            .spec
            .sources
            .iter()
            .map(|s| s.cluster.clone())
            .chain(r.spec.targets.iter().map(|t| t.cluster.clone()))
            .collect();
        clusters.sort();
        clusters.dedup();
        Self {
            tenant: r.tenant_id,
            group: r.spec.group(),
            sources: r.spec.sources.iter().map(|s| s.shard).collect(),
            targets: r.spec.targets.iter().map(|t| t.shard).collect(),
            clusters,
        }
    }
}

/// What the scheduler does with a proposal now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    /// Submit it.
    Submit,
    /// Log it as a recommendation (`sharding.mode = recommend`).
    Recommend,
    /// Not now: `limit`, `lock`, `slots` or `backoff`.
    Wait(&'static str),
}

/// Failed jobs' shards wait 1 h, doubling, at most 24 h.
#[derive(Debug, Clone, Default)]
pub struct Backoff {
    until: BTreeMap<(TenantId, u32, u32), (u64, u32)>,
}

/// The first backoff.
pub const BACKOFF_BASE_MS: u64 = 3_600_000;
/// The longest.
pub const BACKOFF_MAX_MS: u64 = 24 * 3_600_000;

impl Backoff {
    /// A job on these shards failed at `now_ms`.
    pub fn failed(&mut self, tenant: TenantId, group: u32, shards: &[u32], now_ms: u64) {
        for s in shards {
            let e = self.until.entry((tenant, group, *s)).or_insert((0, 0));
            e.1 += 1;
            let wait = BACKOFF_BASE_MS
                .saturating_mul(1 << (e.1 - 1).min(5))
                .min(BACKOFF_MAX_MS);
            e.0 = now_ms + wait;
        }
    }

    /// When the shard may run a job again, if it waits.
    #[must_use]
    pub fn until(&self, tenant: TenantId, group: u32, shard: u32, now_ms: u64) -> Option<u64> {
        self.until
            .get(&(tenant, group, shard))
            .map(|(u, _)| *u)
            .filter(|u| *u > now_ms)
    }
}

/// The `schedule()` check for one proposal against `running`
/// (unfinished jobs), with `cluster` the proposal's cluster.
#[must_use]
pub fn admit(
    p: &Proposal,
    running: &[RunningJob],
    s: &ShardingSettings,
    cluster: &ClusterId,
    backoff: &Backoff,
    now_ms: u64,
) -> Admission {
    let sources = p.job.sources();
    if sources
        .iter()
        .any(|x| backoff.until(p.tenant, p.group, *x, now_ms).is_some())
    {
        return Admission::Wait("backoff");
    }
    let mine: Vec<&RunningJob> = running.iter().filter(|r| r.tenant == p.tenant).collect();
    if mine.iter().any(|r| r.group.is_none()) {
        return Admission::Wait("lock"); // the tenant moves whole
    }
    if mine.iter().any(|r| {
        r.group == Some(p.group)
            && r.sources
                .iter()
                .chain(&r.targets)
                .any(|x| sources.contains(x))
    }) {
        return Admission::Wait("lock");
    }
    if s.mode == ShardingMode::Recommend {
        return Admission::Recommend;
    }
    if mine.len() >= s.max_jobs_per_tenant as usize {
        return Admission::Wait("limit");
    }
    let mut clusters = vec![cluster.clone()];
    if let ShardJob::Move { to, .. } = &p.job {
        clusters.push(to.clone());
    }
    for c in &clusters {
        let n = running.iter().filter(|r| r.clusters.contains(c)).count();
        if n >= s.max_copies_per_cluster as usize {
            return Admission::Wait("slots");
        }
    }
    Admission::Submit
}

/// The next free shard id of `map`'s group, past `running` jobs' targets.
#[must_use]
pub fn next_shard_id(map: &ShardMap, running: &[RunningJob]) -> u32 {
    running
        .iter()
        .filter(|r| r.tenant == map.tenant && r.group == Some(map.group.0))
        .flat_map(|r| r.targets.iter().map(|t| t + 1))
        .chain(std::iter::once(map.next_shard_id))
        .max()
        .unwrap_or(1)
}

fn not_allowed(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::SplitNotAllowed, m)
}

/// The mover job for `p` on `map`: new shard ids from `next`, children on
/// the source cluster, schemas of databases `dbs`, sources retired
/// after `retire_ms`.
///
/// # Errors
/// 39471 for a shard missing from the map, a split without points, or
/// neighbours that aren't adjacent.
pub fn build_spec(
    p: &Proposal,
    map: &ShardMap,
    dbs: &[DbId],
    next: u32,
    retire_ms: u64,
) -> Result<JobSpec> {
    let range_of = |shard: u32| -> Result<(usize, HashRange)> {
        let i = map
            .ranges
            .iter()
            .position(|r| r.shard == ShardId(shard))
            .ok_or_else(|| not_allowed(format!("shard {shard} isn't in the map")))?;
        Ok((
            i,
            HashRange {
                lo: map.ranges[i].lo,
                hi: map.hi(i),
            },
        ))
    };
    let schemas = |shard: u32| -> Vec<String> {
        dbs.iter()
            .map(|db| shard_schema(map.tenant, *db, GroupId(p.group), ShardId(shard)))
            .collect()
    };
    let mut spec = match &p.job {
        ShardJob::Split { shard, points } => {
            let (i, r) = range_of(*shard)?;
            if points.is_empty() {
                return Err(not_allowed("a split needs its points"));
            }
            let mut edges = vec![r.lo];
            edges.extend(points.iter().copied());
            let mut targets = Vec::new();
            for (n, w) in edges.windows(2).enumerate() {
                targets.push((
                    next + u32::try_from(n).unwrap_or(0),
                    HashRange {
                        lo: w[0],
                        hi: Some(w[1]),
                    },
                ));
            }
            targets.push((
                next + u32::try_from(points.len()).unwrap_or(0),
                HashRange {
                    lo: *edges.last().unwrap_or(&r.lo),
                    hi: r.hi,
                },
            ));
            let c = &map.ranges[i].cluster;
            JobSpec::reshard(
                map.tenant,
                p.group,
                c,
                vec![(*shard, schemas(*shard), r)],
                c,
                targets,
            )
        }
        ShardJob::Merge { left, right } => {
            let (i, a) = range_of(*left)?;
            let (j, b) = range_of(*right)?;
            if j != i + 1 {
                return Err(not_allowed(format!(
                    "shards {left} and {right} aren't neighbours"
                )));
            }
            if map.ranges[i].cluster != map.ranges[j].cluster {
                return Err(not_allowed(
                    "only neighbours on one cluster can be merged (move one first)",
                ));
            }
            let c = &map.ranges[i].cluster;
            JobSpec::reshard(
                map.tenant,
                p.group,
                c,
                vec![(*left, schemas(*left), a), (*right, schemas(*right), b)],
                c,
                vec![(next, HashRange { lo: a.lo, hi: b.hi })],
            )
        }
        ShardJob::Move { shard, to } => {
            let (i, r) = range_of(*shard)?;
            JobSpec::reshard(
                map.tenant,
                p.group,
                &map.ranges[i].cluster,
                vec![(*shard, schemas(*shard), r)],
                to,
                vec![(next, r)],
            )
        }
    };
    spec.retain_ms = Some(retire_ms);
    spec.requested_by = Some(format!("sharding:{}", p.reason.code()));
    Ok(spec)
}

/// The decision row of `p`.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn decision(
    p: &Proposal,
    mode: Mode,
    outcome: Outcome,
    summary: String,
    job_id: Option<[u8; 16]>,
    at_us: i64,
    actor: &str,
    dc: &str,
) -> NewDecision {
    let mut evidence = p.evidence.clone();
    if let (Some(o), ShardJob::Split { points, .. }) = (evidence.as_object_mut(), &p.job) {
        o.insert(
            "points".into(),
            json!(
                points
                    .iter()
                    .map(|x| format!("{x:#018x}"))
                    .collect::<Vec<_>>()
            ),
        );
    }
    NewDecision {
        decided_at_us: at_us,
        component: "balancer".into(),
        kind: format!("shard.{}", p.job.kind()),
        subject_type: "shard".into(),
        subject_id: format!(
            "t{}/g{}/s{}",
            p.tenant,
            p.group,
            p.job
                .sources()
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join("+")
        ),
        mode,
        outcome,
        reason_code: p.reason.code().into(),
        summary,
        evidence,
        alternatives: None,
        job_id,
        parent_id: None,
        actor: actor.into(),
        dc_id: dc.into(),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::shards::triggers::Reason;

    fn c(n: &str) -> ClusterId {
        ClusterId::new(n).unwrap()
    }

    fn prop(job: ShardJob) -> Proposal {
        Proposal {
            tenant: TenantId(42),
            group: 3,
            job,
            reason: Reason::Size,
            evidence: json!({}),
        }
    }

    fn split(shard: u32) -> Proposal {
        prop(ShardJob::Split {
            shard,
            points: vec![1 << 63],
        })
    }

    fn running(group: Option<u32>, sources: Vec<u32>, targets: Vec<u32>, cl: &str) -> RunningJob {
        RunningJob {
            tenant: TenantId(42),
            group,
            sources,
            targets,
            clusters: vec![c(cl)],
        }
    }

    #[test]
    fn the_matrix_limits_and_backoff() {
        let s = ShardingSettings::default();
        let c1 = c("dc-a-c1");
        let b = Backoff::default();
        assert_eq!(admit(&split(1), &[], &s, &c1, &b, 0), Admission::Submit);
        // A job on the shard: refused for now (lock).
        let r = vec![running(Some(3), vec![1], vec![5, 6], "dc-a-c1")];
        assert_eq!(
            admit(&split(1), &r, &s, &c1, &b, 0),
            Admission::Wait("lock")
        );
        // Another shard of the group may split beside it.
        assert_eq!(
            admit(&split(2), &r, &s, &c("dc-a-c2"), &b, 0),
            Admission::Submit
        );
        // A merge needs both neighbours idle.
        let m = prop(ShardJob::Merge { left: 2, right: 5 });
        assert_eq!(admit(&m, &r, &s, &c1, &b, 0), Admission::Wait("lock"));
        // A whole-tenant move blocks everything of the tenant.
        let t = vec![running(None, vec![0], vec![0], "dc-a-c9")];
        assert_eq!(
            admit(&split(1), &t, &s, &c1, &b, 0),
            Admission::Wait("lock")
        );
        // The tenant's limit (2), then copy slots per cluster (2).
        let two = vec![
            running(Some(3), vec![7], vec![9, 10], "dc-a-c2"),
            running(Some(4), vec![1], vec![2, 3], "dc-a-c3"),
        ];
        assert_eq!(
            admit(&split(1), &two, &s, &c1, &b, 0),
            Admission::Wait("limit")
        );
        let mut busy = vec![
            running(Some(3), vec![7], vec![9, 10], "dc-a-c1"),
            running(Some(1), vec![1], vec![2, 3], "dc-a-c1"),
        ];
        for r in &mut busy {
            r.tenant = TenantId(7);
        }
        assert_eq!(
            admit(&split(1), &busy, &s, &c1, &b, 0),
            Admission::Wait("slots")
        );
        // Backoff: 1 h, then 2 h.
        let mut b = Backoff::default();
        b.failed(TenantId(42), 3, &[1], 0);
        assert_eq!(
            admit(&split(1), &[], &s, &c1, &b, 1000),
            Admission::Wait("backoff")
        );
        assert_eq!(
            admit(&split(1), &[], &s, &c1, &b, BACKOFF_BASE_MS),
            Admission::Submit
        );
        b.failed(TenantId(42), 3, &[1], BACKOFF_BASE_MS);
        assert_eq!(b.until(TenantId(42), 3, 1, 0), Some(3 * BACKOFF_BASE_MS));
        // Recommend mode logs only.
        let rec = ShardingSettings {
            mode: ShardingMode::Recommend,
            ..s
        };
        assert_eq!(
            admit(&split(1), &[], &rec, &c1, &Backoff::default(), 0),
            Admission::Recommend
        );
    }

    #[test]
    fn specs_partition_the_parent_with_new_ids() {
        let c1 = c("dc-a-c1");
        let m =
            ShardMap::presplit(TenantId(42), GroupId(3), 9, 2, std::slice::from_ref(&c1)).unwrap();
        let p = prop(ShardJob::Split {
            shard: 2,
            points: vec![3 << 62],
        });
        let r = vec![running(Some(3), vec![1], vec![3, 4], "dc-a-c1")];
        let next = next_shard_id(&m, &r);
        assert_eq!(next, 5);
        let spec = build_spec(&p, &m, &[DbId(1), DbId(2)], next, 1000).unwrap();
        assert_eq!(spec.kind, sprig_mover::JobKind::SplitShard);
        assert_eq!(
            spec.sources[0].schemas,
            vec!["t42_d1_g3_s2", "t42_d2_g3_s2"]
        );
        let t: Vec<(u32, u64, Option<u64>)> = spec
            .targets
            .iter()
            .map(|t| (t.shard, t.range.unwrap().lo, t.range.unwrap().hi))
            .collect();
        assert_eq!(t, vec![(5, 1 << 63, Some(3 << 62)), (6, 3 << 62, None)]);
        assert_eq!(spec.retain_ms, Some(1000));
        let merge = build_spec(
            &prop(ShardJob::Merge { left: 1, right: 2 }),
            &m,
            &[DbId(1)],
            7,
            0,
        )
        .unwrap();
        assert_eq!(merge.kind, sprig_mover::JobKind::MergeShards);
        assert_eq!(merge.targets[0].range, Some(HashRange { lo: 0, hi: None }));
        assert!(
            build_spec(
                &prop(ShardJob::Merge { left: 2, right: 1 }),
                &m,
                &[DbId(1)],
                7,
                0
            )
            .is_err()
        );
    }
}
