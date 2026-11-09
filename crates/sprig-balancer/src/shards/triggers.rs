//! Trigger evaluation (chapter 20, "Triggers"): when a shard splits, when
//! neighbours merge, when a group pre-splits.
//!
//! Pure: the controller gathers a [`GroupStat`] (sizes, write shares, ages)
//! and [`evaluate`] returns proposals with their evidence. The bands are the
//! hysteresis: a split's children start near half the split size, far
//! above the merge size (at most a quarter of it, see
//! [`ShardingSettings::banded`]), and a merge needs the result under half
//! the split size and both shards older than `sharding.merge_cooldown_hours`.

use serde::{Deserialize, Serialize};
use serde_json::json;
use sprig_common::ids::{ClusterId, TenantId};

use super::settings::{ShardingMode, ShardingSettings};

/// What to do to a group's shards (chapter 20, `ShardJob`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ShardJob {
    /// Split `shard` at `points` (empty until the histogram chose them).
    Split {
        /// The shard.
        shard: u32,
        /// The split points.
        points: Vec<u64>,
    },
    /// Merge adjacent `left` and `right`.
    Merge {
        /// The lower range.
        left: u32,
        /// The upper range.
        right: u32,
    },
    /// Move `shard` to `to` (placement's).
    Move {
        /// The shard.
        shard: u32,
        /// The cluster.
        to: ClusterId,
    },
}

impl ShardJob {
    /// The shards the job reads.
    #[must_use]
    pub fn sources(&self) -> Vec<u32> {
        match self {
            Self::Split { shard, .. } | Self::Move { shard, .. } => vec![*shard],
            Self::Merge { left, right } => vec![*left, *right],
        }
    }

    /// `split`, `merge` or `move`.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Split { .. } => "split",
            Self::Merge { .. } => "merge",
            Self::Move { .. } => "move",
        }
    }
}

/// Why (chapter 20, `Reason`), in scheduling priority order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    /// A drain moves shards off a cluster.
    Drain,
    /// One hash dominates a hot shard.
    HotKey,
    /// Sustained write share on a hot cluster.
    Load,
    /// Above `sharding.split_size_gb`.
    Size,
    /// Fewer shards than `sharding.min_shards`.
    MinShards,
    /// Placement's balance.
    Balance,
    /// Small, quiet neighbours.
    Merge,
    /// An operator's command.
    Manual,
}

impl Reason {
    /// The decision log's `reason_code`.
    #[must_use]
    pub fn code(self) -> &'static str {
        match self {
            Self::Drain => "drain",
            Self::HotKey => "hot_key",
            Self::Load => "load",
            Self::Size => "size",
            Self::MinShards => "min_shards",
            Self::Balance => "balance",
            Self::Merge => "merge",
            Self::Manual => "manual",
        }
    }
}

/// One shard as the triggers see it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShardStat {
    /// The shard.
    pub shard: u32,
    /// Its first hash.
    pub lo: u64,
    /// One past its last; `None` is 2^64.
    pub hi: Option<u64>,
    /// Its cluster.
    pub cluster: ClusterId,
    /// `DATA_LENGTH + INDEX_LENGTH` over its schemas.
    pub size_bytes: f64,
    /// Its share of its cluster's writes, per minute, oldest first, since
    /// its last cutover (empty without gateway sketches).
    pub write_shares: Vec<f64>,
    /// Placement puts its cluster above the high watermark.
    pub cluster_hot: bool,
    /// When a job created it (ms since the epoch); `None` if older than
    /// the job records.
    pub born_ms: Option<u64>,
    /// A job holds it, or it is held in the map.
    pub busy: bool,
}

/// One active group.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupStat {
    /// The tenant.
    pub tenant: TenantId,
    /// The group.
    pub group: u32,
    /// `table_groups.min_shards`, overriding the setting.
    pub min_shards: Option<u32>,
    /// Shards by range.
    pub shards: Vec<ShardStat>,
}

/// A proposed job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Proposal {
    /// The tenant.
    pub tenant: TenantId,
    /// The group.
    pub group: u32,
    /// What.
    pub job: ShardJob,
    /// Why.
    pub reason: Reason,
    /// Trigger values: numbers and ids only.
    pub evidence: serde_json::Value,
}

fn width(s: &ShardStat) -> u128 {
    s.hi.map_or(1u128 << 64, u128::from) - u128::from(s.lo)
}

/// Chapter 20's `evaluate` for one group at `now_ms`.
#[must_use]
pub fn evaluate(g: &GroupStat, s: &ShardingSettings, now_ms: u64) -> Vec<Proposal> {
    if s.mode == ShardingMode::Off {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut taken = std::collections::BTreeSet::new();
    let sustain_min = usize::try_from(s.sustain.as_secs() / 60)
        .unwrap_or(usize::MAX)
        .max(1);
    let min_shards = g.min_shards.unwrap_or(s.min_shards).max(1);
    let propose = |shard: &ShardStat, reason: Reason, evidence: serde_json::Value| Proposal {
        tenant: g.tenant,
        group: g.group,
        job: ShardJob::Split {
            shard: shard.shard,
            points: Vec::new(),
        },
        reason,
        evidence,
    };
    for sh in &g.shards {
        if sh.busy {
            continue;
        }
        if sh.size_bytes > s.split_bytes && width(sh) > 1 {
            out.push(propose(
                sh,
                Reason::Size,
                json!({
                    "shard": sh.shard, "size_bytes": sh.size_bytes,
                    "split_bytes": s.split_bytes,
                }),
            ));
            taken.insert(sh.shard);
            continue;
        }
        let window = sh.write_shares.len() >= sustain_min;
        let sustained = window
            && sh.write_shares[sh.write_shares.len() - sustain_min..]
                .iter()
                .all(|x| *x >= s.split_write_share);
        if sustained && sh.cluster_hot && width(sh) > 1 {
            out.push(propose(
                sh,
                Reason::Load,
                json!({
                    "shard": sh.shard, "min_write_share": s.split_write_share,
                    "minutes": sustain_min,
                }),
            ));
            taken.insert(sh.shard);
        }
    }
    let count = g.shards.len();
    if count < min_shards as usize
        && let Some(widest) = g
            .shards
            .iter()
            .filter(|x| !x.busy && !taken.contains(&x.shard) && width(x) > 1)
            .max_by_key(|x| (width(x), std::cmp::Reverse(x.shard)))
    {
        out.push(propose(
            widest,
            Reason::MinShards,
            json!({ "shards": count, "min_shards": min_shards }),
        ));
        taken.insert(widest.shard);
    }
    if count > min_shards.max(1) as usize {
        let quiet = |x: &ShardStat| {
            let w = &x.write_shares;
            let tail = &w[w.len().saturating_sub(sustain_min)..];
            tail.iter().all(|v| *v < s.split_write_share / 5.0)
        };
        let old = |x: &ShardStat| {
            x.born_ms.is_none_or(|b| {
                now_ms.saturating_sub(b)
                    >= u64::try_from(s.merge_cooldown.as_millis()).unwrap_or(u64::MAX)
            })
        };
        let mut merges = 0usize;
        for pair in g.shards.windows(2) {
            let (l, r) = (&pair[0], &pair[1]);
            if count - merges <= min_shards.max(1) as usize {
                break;
            }
            if l.busy || r.busy || taken.contains(&l.shard) || taken.contains(&r.shard) {
                continue;
            }
            if l.hi != Some(r.lo) || l.cluster != r.cluster {
                continue; // R1 merges neighbours on one cluster
            }
            if l.size_bytes < s.merge_bytes
                && r.size_bytes < s.merge_bytes
                && l.size_bytes + r.size_bytes < s.split_bytes / 2.0
                && quiet(l)
                && quiet(r)
                && old(l)
                && old(r)
            {
                out.push(Proposal {
                    tenant: g.tenant,
                    group: g.group,
                    job: ShardJob::Merge {
                        left: l.shard,
                        right: r.shard,
                    },
                    reason: Reason::Merge,
                    evidence: json!({
                        "left": l.shard, "right": r.shard,
                        "left_bytes": l.size_bytes, "right_bytes": r.size_bytes,
                        "merge_bytes": s.merge_bytes, "split_bytes": s.split_bytes,
                    }),
                });
                taken.insert(l.shard);
                taken.insert(r.shard);
                merges += 1;
            }
        }
    }
    out.sort_by_key(|p| p.reason);
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::time::Duration;

    use super::*;
    use crate::settings::GB;

    fn shard(n: u32, lo: u64, hi: Option<u64>, gb: f64) -> ShardStat {
        ShardStat {
            shard: n,
            lo,
            hi,
            cluster: ClusterId::new("dc-a-c1").unwrap(),
            size_bytes: gb * GB,
            write_shares: Vec::new(),
            cluster_hot: false,
            born_ms: None,
            busy: false,
        }
    }

    fn group(shards: Vec<ShardStat>) -> GroupStat {
        GroupStat {
            tenant: TenantId(42),
            group: 3,
            min_shards: None,
            shards,
        }
    }

    #[test]
    fn size_splits_and_merges_follow_the_bands() {
        let s = ShardingSettings::default();
        let g = group(vec![shard(4, 0, None, 34.0)]);
        let p = evaluate(&g, &s, 0);
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].reason, Reason::Size);
        // Children near 17 GB never merge; two small quiet ones do.
        let half = 1u64 << 63;
        let g = group(vec![
            shard(5, 0, Some(half), 17.0),
            shard(6, half, None, 17.0),
        ]);
        assert!(evaluate(&g, &s, 0).is_empty());
        let g = group(vec![
            shard(5, 0, Some(half), 3.0),
            shard(6, half, None, 3.5),
        ]);
        let p = evaluate(&g, &s, 0);
        assert_eq!(p[0].job, ShardJob::Merge { left: 5, right: 6 });
        // Busy or young shards don't merge.
        let mut young = g.clone();
        young.shards[0].born_ms = Some(1_000);
        assert!(evaluate(&young, &s, 2_000).is_empty());
        assert_eq!(evaluate(&young, &s, 1_000 + 25 * 3_600_000).len(), 1);
        let mut busy = g.clone();
        busy.shards[1].busy = true;
        assert!(evaluate(&busy, &s, 0).is_empty());
        // A busy writer doesn't merge either.
        let mut loud = g;
        loud.shards[0].write_shares = vec![0.2; 30];
        assert!(evaluate(&loud, &s, 0).is_empty());
    }

    #[test]
    fn load_splits_need_a_hot_cluster_for_the_whole_window() {
        let s = ShardingSettings::default();
        let mut sh = shard(4, 0, None, 1.0);
        sh.write_shares = vec![0.9; 30];
        let g = group(vec![sh.clone()]);
        assert!(evaluate(&g, &s, 0).is_empty(), "an idle cluster");
        sh.cluster_hot = true;
        let p = evaluate(&group(vec![sh.clone()]), &s, 0);
        assert_eq!(p[0].reason, Reason::Load);
        sh.write_shares[10] = 0.3;
        assert!(evaluate(&group(vec![sh.clone()]), &s, 0).is_empty());
        sh.write_shares = vec![0.9; 29];
        assert!(evaluate(&group(vec![sh]), &s, 0).is_empty(), "too short");
    }

    #[test]
    fn min_shards_presplits_the_widest_and_blocks_merges_below_it() {
        let s = ShardingSettings {
            min_shards: 2,
            ..ShardingSettings::default()
        };
        let p = evaluate(&group(vec![shard(1, 0, None, 0.0)]), &s, 0);
        assert_eq!(p[0].reason, Reason::MinShards);
        let half = 1u64 << 63;
        let g = group(vec![
            shard(5, 0, Some(half), 0.1),
            shard(6, half, None, 0.1),
        ]);
        assert!(evaluate(&g, &s, 0).is_empty());
    }

    #[test]
    fn simulated_growth_and_shrinkage_never_cycle() {
        // Grow one shard to the split size, split it; shrink, merge; the
        // children of a split are never merge candidates and a merged shard
        // is never a split candidate.
        let s = ShardingSettings {
            split_bytes: 8.0 * GB,
            merge_bytes: 4.0 * GB,
            merge_cooldown: Duration::ZERO,
            ..ShardingSettings::default()
        }
        .banded();
        for size in 1..200 {
            let gb = f64::from(size) / 10.0;
            let half = 1u64 << 63;
            let one = group(vec![shard(1, 0, None, gb)]);
            let two = group(vec![
                shard(2, 0, Some(half), gb / 2.0),
                shard(3, half, None, gb / 2.0),
            ]);
            let split = evaluate(&one, &s, 0)
                .iter()
                .any(|p| p.reason == Reason::Size);
            let merge = evaluate(&two, &s, 0)
                .iter()
                .any(|p| p.reason == Reason::Merge);
            assert!(!(split && merge), "at {gb} GB both fire");
            if split {
                // The children of this split don't merge.
                assert!(!merge);
            }
        }
    }
}
