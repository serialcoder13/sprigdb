//! Whether one move is worth making (chapter 11, "Judging a move"): the
//! benefit a move brings against what it costs, both in points of `full`
//! (0.01 is one percent of a cluster), with the reasons in plain words for
//! the decision log.
//!
//! ```text
//! relief   = full[h] − full_after[h]                  # what the source sheds
//! excess   = max(0, full[h] − target[h])              # how far above target it is
//! gap      = full[h] − full[c]                        # imbalance of the pair
//! urgency  = 1 + 4 × max(excess / (1 − target[h]), (gap − spread_pct) / (1 − spread_pct))
//! evening  = max(0, gap − |full_after[h] − full_after[c]|)
//! benefit  = relief × urgency + evening / 2
//!
//! typical  = full[h] / units on h                     # what an average unit sheds
//! copy     = 0.25 × bytes / storage capacity of c     # copying work (1 TB if undeclared)
//! pause    = 0.5 × typical × min(1, writes per second / 500)   # write pause, catch-up
//! busy     = 0.5 × typical × the busiest of h and c (busy time or pressure)
//! recent   = 0.5 × typical if the tenant moved in the last 7 days
//!          + 0.25 × typical per move in flight on h or c
//! overhead = 0.25 × typical                           # verification, cutover, risk
//! cost     = copy + pause + busy + recent + overhead
//!
//! worth it when benefit ≥ balancer.benefit_cost_ratio × cost
//!          and relief ≥ balancer.min_gain_pct (an optional floor, 0 by default)
//! ```
//!
//! The costs other than copying are measured against what a typical unit
//! on the source sheds, so the rule behaves the same on a cluster of 50
//! large tenants and one of 5,000 small ones: an average tenant is worth
//! moving off a cluster that needs relief, a tenant far smaller than
//! average only when the source is far above target or the pair far apart.
//! Among moves worth making, the planner takes the one with the largest
//! benefit minus cost.

use serde::{Deserialize, Serialize};

use crate::settings::GB;

/// Copying work per byte, as a share of the target's storage.
pub const COPY_WEIGHT: f64 = 0.25;
/// Storage assumed when the target declares none.
pub const DEFAULT_STORAGE_BYTES: f64 = 1024.0 * GB;
/// The write-pause cost of a tenant writing `PAUSE_WRITES` rows a second or
/// more, in typical units.
pub const PAUSE_WEIGHT: f64 = 0.5;
/// Writes per second at which the pause cost is full.
pub const PAUSE_WRITES: f64 = 500.0;
/// The cost of copying while a cluster is fully busy, in typical units.
pub const BUSY_WEIGHT: f64 = 0.5;
/// The cost of moving a tenant that moved in the last 7 days.
pub const RECENT_TENANT: f64 = 0.5;
/// The cost per move already in flight on the source or target.
pub const IN_FLIGHT: f64 = 0.25;
/// Every move's fixed cost, in typical units.
pub const OVERHEAD: f64 = 0.25;

/// The rule's weights, each a `balancer.*` setting (dc and global scope);
/// the defaults are the constants above.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Weights {
    /// `balancer.benefit_urgency_weight`: how much a source's need for
    /// relief multiplies what it sheds (urgency = 1 + this × need).
    pub urgency: f64,
    /// `balancer.benefit_evening_weight`: how much evening the pair counts.
    pub evening: f64,
    /// `balancer.cost_copy_weight`: copying cost per byte, as a share of
    /// the target's storage.
    pub copy: f64,
    /// `balancer.cost_write_weight`: the write-rate cost at full rate, in
    /// typical units.
    pub write: f64,
    /// `balancer.cost_write_full_rows_per_sec`: the write rate at which
    /// the write-rate cost is full.
    pub write_full_rows_per_sec: f64,
    /// `balancer.cost_load_weight`: the cost of copying between fully
    /// busy clusters, in typical units.
    pub load: f64,
    /// `balancer.cost_recent_move_weight`: the cost of moving a tenant
    /// that moved in the last 7 days, in typical units.
    pub recent_move: f64,
    /// `balancer.cost_running_move_weight`: the cost per move in flight on
    /// the source or target, in typical units.
    pub running_move: f64,
    /// `balancer.cost_fixed_weight`: every move's fixed cost, in typical
    /// units.
    pub fixed: f64,
}

/// `balancer.benefit_urgency_weight`'s default.
pub const URGENCY_WEIGHT: f64 = 4.0;
/// `balancer.benefit_evening_weight`'s default.
pub const EVENING_WEIGHT: f64 = 0.5;

impl Default for Weights {
    fn default() -> Self {
        Self {
            urgency: URGENCY_WEIGHT,
            evening: EVENING_WEIGHT,
            copy: COPY_WEIGHT,
            write: PAUSE_WEIGHT,
            write_full_rows_per_sec: PAUSE_WRITES,
            load: BUSY_WEIGHT,
            recent_move: RECENT_TENANT,
            running_move: IN_FLIGHT,
            fixed: OVERHEAD,
        }
    }
}

impl Weights {
    /// One line for `explain placement`.
    #[must_use]
    pub fn line(&self) -> String {
        format!(
            "benefit: urgency {} evening {}; cost: copy {} writes {} (full at {} rows/s) load {} \
             recent move {} running moves {} fixed {}",
            self.urgency,
            self.evening,
            self.copy,
            self.write,
            self.write_full_rows_per_sec,
            self.load,
            self.recent_move,
            self.running_move,
            self.fixed
        )
    }
}

/// What a judgement needs.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct JudgeInput {
    /// `full` of the source before and after.
    pub source: (f64, f64),
    /// `full` of the target before and after.
    pub target: (f64, f64),
    /// The source's `balancer.target_pct` as a ratio.
    pub source_target_pct: f64,
    /// `balancer.spread_pct` as a ratio.
    pub spread_pct: f64,
    /// What an average unit on the source sheds: `full[h]` over its units.
    pub typical: f64,
    /// Bytes to copy.
    pub bytes: f64,
    /// The target's storage capacity (0 if undeclared).
    pub target_storage: f64,
    /// The unit's rows written per second.
    pub writes: f64,
    /// The busiest of source and target: busy-time utilization or
    /// pressure, whichever is higher.
    pub busiest: f64,
    /// The tenant moved in the last 7 days.
    pub tenant_moved_recently: bool,
    /// Moves in flight on the source or the target.
    pub in_flight: usize,
    /// `balancer.benefit_cost_ratio`.
    pub ratio: f64,
    /// `balancer.min_gain_pct` as a ratio (0: no floor).
    pub min_gain: f64,
    /// The weights.
    pub weights: Weights,
}

/// A move's benefit and cost, and the verdict.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Judgement {
    /// Points the source sheds.
    pub relief: f64,
    /// Points the source is above its target.
    pub excess: f64,
    /// The multiplier on relief.
    pub urgency: f64,
    /// Points of imbalance between source and target removed.
    pub evening: f64,
    /// The benefit.
    pub benefit: f64,
    /// Copying cost.
    pub copy: f64,
    /// Write-pause cost.
    pub pause: f64,
    /// Busy-cluster cost.
    pub busy: f64,
    /// Recent-move cost.
    pub recent: f64,
    /// Fixed cost.
    pub overhead: f64,
    /// The cost.
    pub cost: f64,
    /// The ratio required.
    pub ratio: f64,
    /// Benefit minus cost: how the planner ranks moves worth making.
    pub net: f64,
    /// Worth making.
    pub worth: bool,
    /// The relief is under `balancer.min_gain_pct`.
    pub below_floor: bool,
}

/// Judges one move.
#[must_use]
pub fn judge(i: &JudgeInput) -> Judgement {
    let relief = (i.source.0 - i.source.1).max(0.0);
    let excess = (i.source.0 - i.source_target_pct).max(0.0);
    let gap_before = i.source.0 - i.target.0;
    let hot = excess / (1.0 - i.source_target_pct).max(0.05);
    let apart = (gap_before - i.spread_pct).max(0.0) / (1.0 - i.spread_pct).max(0.05);
    let w = &i.weights;
    let urgency = 1.0 + w.urgency * hot.max(apart);
    let gap_after = (i.source.1 - i.target.1).abs();
    let evening = (gap_before - gap_after).max(0.0);
    let benefit = relief * urgency + evening * w.evening;
    let storage = if i.target_storage > 0.0 {
        i.target_storage
    } else {
        DEFAULT_STORAGE_BYTES
    };
    let t = i.typical.max(1e-6);
    let copy = w.copy * i.bytes / storage;
    let pause = t * w.write * (i.writes / w.write_full_rows_per_sec.max(1.0)).clamp(0.0, 1.0);
    let busy = t * w.load * i.busiest.clamp(0.0, 1.0);
    #[allow(clippy::cast_precision_loss)]
    let recent = t
        * (if i.tenant_moved_recently {
            w.recent_move
        } else {
            0.0
        } + w.running_move * i.in_flight as f64);
    let overhead = t * w.fixed;
    let cost = copy + pause + busy + recent + overhead;
    let below_floor = relief + 1e-9 < i.min_gain;
    let worth = !below_floor && benefit + 1e-12 >= i.ratio * cost;
    Judgement {
        relief,
        excess,
        urgency,
        evening,
        benefit,
        copy,
        pause,
        busy,
        recent,
        overhead,
        cost,
        ratio: i.ratio,
        net: benefit - cost,
        worth,
        below_floor,
    }
}

fn pts(x: f64) -> String {
    format!("{:.2}", x * 100.0)
}

impl Judgement {
    /// The largest part of the cost, in words.
    #[must_use]
    pub fn main_cost(&self) -> &'static str {
        let parts = [
            (self.copy, "the copy"),
            (self.pause, "the tenant's writes (pause and catch-up)"),
            (self.busy, "busy clusters"),
            (self.recent, "recent moves"),
            (self.overhead, "the fixed cost of a move"),
        ];
        parts
            .iter()
            .max_by(|a, b| a.0.total_cmp(&b.0))
            .map_or("the fixed cost of a move", |p| p.1)
    }

    /// The reason in plain words, for the decision's summary.
    #[must_use]
    pub fn sentence(&self, shard: &str, from: &str, to: &str, bytes: f64, writes: f64) -> String {
        let what = format!("{shard} ({:.1} GB, {writes:.0} writes/s)", bytes / GB);
        let why_benefit = format!(
            "it sheds {} points from {from}{} and evens {from} and {to} by {} points (benefit {})",
            pts(self.relief),
            if self.excess > 0.0 {
                format!(", which is {} points above its target", pts(self.excess))
            } else {
                String::new()
            },
            pts(self.evening),
            pts(self.benefit)
        );
        let why_cost = format!(
            "cost {} (copy {}, writes {}, busy {}, recent moves {}, fixed {})",
            pts(self.cost),
            pts(self.copy),
            pts(self.pause),
            pts(self.busy),
            pts(self.recent),
            pts(self.overhead)
        );
        let times = if self.cost > 0.0 {
            self.benefit / self.cost
        } else {
            f64::INFINITY
        };
        if self.worth {
            format!(
                "moved {what} from {from} to {to} because {why_benefit} against a {why_cost}: \
                 {times:.1}x its cost, {:.1}x needed",
                self.ratio
            )
        } else if self.below_floor {
            format!(
                "did not move {what} from {from} to {to}: it sheds only {} points, under \
                 balancer.min_gain_pct",
                pts(self.relief)
            )
        } else {
            format!(
                "did not move {what} from {from} to {to}: {why_benefit}, only {times:.1}x its \
                 {why_cost}; {:.1}x is needed, and the largest cost is {}",
                self.ratio,
                self.main_cost()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 500 GB source at 90% holding 45 units (2 points each).
    fn base() -> JudgeInput {
        JudgeInput {
            source: (0.90, 0.88),
            target: (0.40, 0.42),
            source_target_pct: 0.70,
            spread_pct: 0.25,
            typical: 0.02,
            bytes: 10.0 * GB,
            target_storage: 500.0 * GB,
            writes: 0.0,
            busiest: 0.0,
            tenant_moved_recently: false,
            in_flight: 0,
            ratio: 2.0,
            min_gain: 0.0,
            weights: Weights::default(),
        }
    }

    #[test]
    fn an_average_tenant_off_a_hot_cluster_is_worth_it() {
        let j = judge(&base());
        assert!((j.relief - 0.02).abs() < 1e-9);
        // 20 points over a 30-point band; the pair is 50 points apart,
        // 25 over the spread: the hotter reason wins.
        assert!((j.urgency - (1.0 + 8.0 / 3.0)).abs() < 1e-9);
        assert!((j.evening - 0.04).abs() < 1e-9);
        assert!((j.copy - 0.005).abs() < 1e-9);
        assert!((j.overhead - 0.005).abs() < 1e-9);
        assert!(j.worth, "{j:?}");
        let s = j.sentence("t1/g0/s0", "mum1-c1", "mum1-c2", 10.0 * GB, 0.0);
        assert!(
            s.starts_with("moved t1/g0/s0 (10.0 GB, 0 writes/s) from mum1-c1 to mum1-c2 because"),
            "{s}"
        );
        assert!(s.contains("20.00 points above its target"), "{s}");
    }

    #[test]
    fn a_tenant_far_below_average_is_not_worth_it_near_balance() {
        // 50% vs 22%: just over the spread; the tenant is a tenth of average.
        let i = JudgeInput {
            source: (0.50, 0.498),
            target: (0.22, 0.222),
            typical: 0.02,
            bytes: GB,
            ..base()
        };
        let j = judge(&i);
        assert!(!j.worth, "{j:?}");
        let s = j.sentence("t2/g0/s0", "mum1-c1", "mum1-c2", GB, 0.0);
        assert!(s.starts_with("did not move t2/g0/s0"), "{s}");
        assert!(s.contains("the fixed cost of a move"), "{s}");
        // The same tenant off a cluster at 98%: worth it.
        let hot = judge(&JudgeInput {
            source: (0.98, 0.978),
            ..i
        });
        assert!(hot.worth, "{hot:?}");
    }

    #[test]
    fn the_rule_does_not_depend_on_cluster_size() {
        // An average tenant on a 4 TB cluster of 4,000 one-GB tenants.
        let big = judge(&JudgeInput {
            source: (0.90, 0.90 - 0.9 / 4000.0),
            target: (0.40, 0.40 + 0.9 / 4000.0),
            typical: 0.9 / 4000.0,
            bytes: GB,
            target_storage: 4096.0 * GB,
            ..base()
        });
        assert!(big.worth, "{big:?}");
    }

    #[test]
    fn busy_clusters_writes_and_recent_moves_raise_the_cost() {
        let cool = JudgeInput {
            source: (0.55, 0.53),
            target: (0.25, 0.27),
            ..base()
        };
        let calm = judge(&cool);
        assert!(calm.worth, "{calm:?}");
        let busy = judge(&JudgeInput {
            busiest: 1.0,
            writes: 2000.0,
            tenant_moved_recently: true,
            in_flight: 1,
            ..cool
        });
        assert!(busy.cost > calm.cost + 0.02 * 1.7);
        assert!(!busy.worth, "{busy:?}");
    }

    #[test]
    fn the_floor_is_optional() {
        let j = judge(&JudgeInput {
            min_gain: 0.05,
            ..base()
        });
        assert!(j.below_floor && !j.worth);
        assert!(
            j.sentence("s", "a", "b", GB, 0.0)
                .contains("balancer.min_gain_pct")
        );
    }

    #[test]
    fn changed_weights_change_the_verdict() {
        // Worth it on defaults; not once copying costs 40 times more.
        let i = JudgeInput {
            source: (0.60, 0.58),
            target: (0.30, 0.32),
            ..base()
        };
        assert!(judge(&i).worth);
        let dear = JudgeInput {
            weights: Weights {
                copy: 10.0,
                ..Weights::default()
            },
            ..i
        };
        let j = judge(&dear);
        assert!(!j.worth, "{j:?}");
        assert_eq!(j.main_cost(), "the copy");
        // A tiny tenant rejected on defaults moves once the fixed cost is
        // lowered.
        let tiny = JudgeInput {
            source: (0.50, 0.498),
            target: (0.22, 0.222),
            bytes: GB,
            ..base()
        };
        assert!(!judge(&tiny).worth);
        let cheap = JudgeInput {
            weights: Weights {
                fixed: 0.02,
                ..Weights::default()
            },
            ..tiny
        };
        assert!(judge(&cheap).worth);
        // A larger urgency weight lets a hot source shed a small tenant.
        let small_hot = JudgeInput {
            source: (0.85, 0.849),
            target: (0.60, 0.601),
            bytes: GB,
            ..base()
        };
        assert!(!judge(&small_hot).worth);
        assert!(
            judge(&JudgeInput {
                weights: Weights {
                    urgency: 30.0,
                    ..Weights::default()
                },
                ..small_hot
            })
            .worth
        );
    }
}
