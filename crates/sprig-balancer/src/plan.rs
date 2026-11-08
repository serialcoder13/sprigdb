//! `place()` and the rebalance planner with hysteresis (chapter 11,
//! "Placing new shards" and "Rebalancing with hysteresis"; build step 5).
//!
//! The planner is pure: it reads a [`FleetView`] and returns a
//! [`PlanReport`] (moves, plus the reasoning `sprigctl explain placement`
//! prints). Scheduling the moves is [`crate::schedule`]'s job. The only
//! state it keeps between runs is when each condition started, for the
//! `balancer.sustain_minutes` windows.
//!
//! Dampers against ping-pong, all here or in the view the scheduler fills:
//! the band (`high_pct` to start, `target_pct` as the ceiling for targets),
//! sustain windows, shard and cluster cooldowns, the simulation that
//! rejects a move whose target would end up more than `spread_pct` fuller
//! than its source, concurrency limits, and (a SprigDB choice on top of
//! the book) no rebalancing move back to a cluster the shard left within
//! [`MOVE_BACK_WINDOW_SECS`].

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

use serde::{Deserialize, Serialize};
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};

use crate::constraints::{
    Blocker, Feasibility, Fit, Purpose, Unit, cowrite_split_rate, feasible_in, unit_of,
};
use crate::judge::{JudgeInput, Judgement, judge};
use crate::model::{ClusterState, FleetView, Fullness, LoadVector, ShardKey, Timestamp};
use crate::settings::{BalancerMode, BalancerSettings, GB};

/// A move back to a cluster left within this window counts as reverted
/// (chapter 11's thrash table: 7 days).
pub const MOVE_BACK_WINDOW_SECS: u64 = 7 * 86_400;

/// Why a new shard is being placed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlaceReason {
    /// A new tenant's first shards (its catalog and first database).
    NewTenant,
    /// A new table group.
    NewGroup,
    /// A split half.
    Split,
    /// A tenant moving between DCs.
    DcMigration,
}

/// A placement request: shards that must land together.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlaceRequest {
    /// The tenant.
    pub tenant: TenantId,
    /// The new shards.
    pub units: Vec<ShardKey>,
    /// Their predicted load.
    pub predicted: LoadVector,
    /// Why.
    pub reason: PlaceReason,
}

/// How a placement was chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlaceHow {
    /// One cluster scored clearly best.
    ClearWin,
    /// Several were near-equal; the least busy won.
    LeastBusy,
}

/// A cluster considered for a placement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Alternative {
    /// The cluster.
    pub cluster: ClusterId,
    /// Its score (lower is better).
    pub score: f64,
    /// `full` after.
    pub full_after: f64,
    /// Its least-busy value, when near-equal.
    pub least_busy: Option<f64>,
}

/// A placement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlacementDecision {
    /// Where.
    pub cluster: ClusterId,
    /// How it was chosen.
    pub how: PlaceHow,
    /// The best alternatives, chosen first, at most three.
    pub alternatives: Vec<Alternative>,
    /// Clusters that failed a constraint, and why.
    pub blocked: Vec<(ClusterId, Blocker)>,
}

/// Why a move is planned (the decision's `reason_code`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MoveReason {
    /// The source was above `balancer.high_pct` long enough.
    ClusterHot,
    /// Fullest minus emptiest above `balancer.spread_pct` long enough.
    Spread,
    /// The shard's cluster no longer satisfies residency.
    ResidencyChange,
    /// The source is draining.
    Drain,
    /// An operator asked (`sprigctl shard move`).
    Operator,
}

impl MoveReason {
    /// The `reason_code`.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::ClusterHot => "cluster_hot",
            Self::Spread => "spread",
            Self::ResidencyChange => "residency_change",
            Self::Drain => "drain",
            Self::Operator => "operator",
        }
    }
}

/// A move the planner wants.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlannedMove {
    /// The tenant.
    pub tenant: TenantId,
    /// The shards moving together (the first is the one chosen; the rest
    /// are bound to it by co-writes).
    pub shards: Vec<ShardKey>,
    /// Source.
    pub from: ClusterId,
    /// Target.
    pub to: ClusterId,
    /// Why.
    pub reason: MoveReason,
    /// `full[from]` before minus after.
    pub gain: f64,
    /// Benefit minus cost (the judgement's `net`); for forced moves, gain
    /// per GB copied.
    pub value: f64,
    /// Bytes to copy.
    pub size_bytes: f64,
    /// `full` of the source before and after.
    pub from_full: (f64, f64),
    /// `full` of the target before and after.
    pub to_full: (f64, f64),
    /// Other targets considered, with their `full` after.
    pub alternatives: Vec<(ClusterId, f64)>,
    /// The load moving.
    pub load: LoadVector,
    /// Benefit against cost (`crate::judge`); `None` for forced moves.
    #[serde(default)]
    pub judgement: Option<Judgement>,
    /// Why, in plain words.
    #[serde(default)]
    pub why: String,
    /// Other moves considered for this source, best first, with why they
    /// lost or weren't worth it.
    #[serde(default)]
    pub considered: Vec<Considered>,
}

/// A move considered and not made.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Considered {
    /// The tenant.
    pub tenant: TenantId,
    /// The shard.
    pub shard: ShardKey,
    /// Source.
    pub from: ClusterId,
    /// Target.
    pub to: ClusterId,
    /// Bytes.
    pub size_bytes: f64,
    /// Benefit against cost.
    pub judgement: Judgement,
    /// Why, in plain words.
    pub why: String,
}

/// Something the planner noticed that stopped or shaped a move.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Note {
    /// The guardrail or constraint (`shard_cooldown`, `cluster_cooldown`,
    /// `concurrency`, `tenant_daily`, `copy_slots`, `move_back`, `pinned`,
    /// `locked`, `size_limit`, `no_target`, `min_gain`, ...).
    pub guardrail: String,
    /// The cluster concerned.
    pub cluster: Option<ClusterId>,
    /// The shard concerned.
    pub shard: Option<ShardKey>,
    /// One line for people.
    pub text: String,
}

/// A move that would help but needs an operator: the shard is above
/// `migration.auto_size_limit_gb` (an `approve_move` recommendation).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalNeeded {
    /// The shard.
    pub shard: ShardKey,
    /// Its size.
    pub size_bytes: f64,
    /// Source.
    pub from: ClusterId,
    /// Best target.
    pub to: ClusterId,
    /// The gain it would bring.
    pub gain: f64,
}

/// A hot cluster.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HotCluster {
    /// The cluster.
    pub cluster: ClusterId,
    /// Its `full`.
    pub full: f64,
    /// Above `high_pct` (or the fullest under a spread) since.
    pub since: Timestamp,
}

/// What triggered planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Trigger {
    /// Nothing hot, spread within bounds.
    None,
    /// Clusters above `high_pct` for `sustain_minutes`.
    Hot,
    /// Spread above `spread_pct` for `sustain_minutes`.
    Spread,
}

/// One planning run's result and reasoning.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanReport {
    /// When.
    pub at: Timestamp,
    /// Every cluster's `full` before the run's moves.
    pub fullness: BTreeMap<ClusterId, Fullness>,
    /// Fullest minus emptiest before.
    pub spread: f64,
    /// What triggered it.
    pub trigger: Trigger,
    /// Hot clusters (sustained), fullest first.
    pub hot: Vec<HotCluster>,
    /// Clusters above `high_pct` (or the spread) not yet for long enough.
    pub warming: Vec<HotCluster>,
    /// Moves, in the order planned.
    pub moves: Vec<PlannedMove>,
    /// Why other moves didn't happen.
    pub notes: Vec<Note>,
    /// Moves that need approval.
    pub approvals: Vec<ApprovalNeeded>,
    /// Hot clusters for which no feasible move was found.
    pub stuck: Vec<ClusterId>,
    /// For stuck clusters, the best moves considered and not worth making.
    #[serde(default)]
    pub rejected: Vec<Considered>,
    /// The weights the run judged moves with.
    #[serde(default)]
    pub weights: crate::judge::Weights,
    /// Pinned tenants a move would have been considered for (off a hot
    /// cluster, or by a drain or residency change), with why they stay.
    #[serde(default)]
    pub pinned: Vec<PinnedSkip>,
}

/// A pinned tenant left where it is.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PinnedSkip {
    /// The tenant.
    pub tenant: TenantId,
    /// The shard.
    pub shard: ShardKey,
    /// Where it stays.
    pub cluster: ClusterId,
    /// `cluster_hot`, `spread`, `drain` or `residency_change`: what would
    /// have moved it.
    pub reason: MoveReason,
    /// Why, in plain words.
    pub why: String,
}

fn pin_text(t: Option<&crate::model::TenantView>) -> String {
    t.and_then(|t| t.pin.as_ref()).map_or_else(
        || "pinned by operator".to_owned(),
        crate::pins::TenantPin::describe,
    )
}

#[derive(Debug, Default)]
struct Sustain {
    hot_since: BTreeMap<ClusterId, Timestamp>,
    spread_since: Option<Timestamp>,
}

/// Chapter 11's interface: feasibility, placement and rebalancing.
pub trait PlacementPolicy: Send + Sync {
    /// Clusters that can take `unit`, with their fit.
    fn feasible(&self, view: &FleetView, unit: &Unit, now: Timestamp) -> Vec<(ClusterId, Fit)>;
    /// Places new shards.
    ///
    /// # Errors
    /// 39120 `ER_SPRIG_PLACEMENT_UNAVAILABLE` when no cluster can take them.
    fn place(
        &self,
        view: &FleetView,
        req: &PlaceRequest,
        now: Timestamp,
    ) -> Result<PlacementDecision>;
    /// Plans rebalancing moves.
    fn rebalance(&self, view: &FleetView, now: Timestamp) -> PlanReport;
}

/// The planner.
#[derive(Debug)]
pub struct Planner {
    /// The DC-level keys.
    pub settings: BalancerSettings,
    /// Refuse rebalancing moves back to a cluster the shard left within
    /// [`MOVE_BACK_WINDOW_SECS`] (on by default; the simulator turns it off
    /// to show the other dampers alone).
    pub avoid_move_back: bool,
    sustain: Mutex<Sustain>,
}

impl Planner {
    /// A planner with `settings`.
    #[must_use]
    pub fn new(settings: BalancerSettings) -> Self {
        Self {
            settings,
            avoid_move_back: true,
            sustain: Mutex::new(Sustain::default()),
        }
    }

    /// The soft-constraint score of `c` for `unit` (lower is better).
    #[must_use]
    pub fn score(&self, view: &FleetView, unit: &Unit, c: &ClusterId, fit: &Fit) -> f64 {
        let s = &self.settings;
        let cv = &view.clusters[c];
        let rate = cowrite_split_rate(view, unit, c);
        let split = rate / (rate + 100.0);
        let xa_missing = if rate > 0.0 && !cv.caps.xa { 1.0 } else { 0.0 };
        let rpo = view
            .tenants
            .get(&unit.tenant)
            .is_some_and(|t| t.rpo_zero && !cv.caps.zero_loss);
        fit.full_after
            + s.weight_cowrite * split
            + s.weight_xa * xa_missing
            + s.weight_rpo * if rpo { 1.0 } else { 0.0 }
    }

    /// Spare capacity after taking the unit, weighted by
    /// `placement.least_busy_weights`.
    #[must_use]
    pub fn least_busy(&self, fit: &Fit) -> f64 {
        let w = &self.settings.least_busy;
        let spare_storage = 1.0 - fit.storage_after;
        let spare_cpu = 1.0 - fit.busy_after.max(fit.pressure);
        let spare_conns = 1.0 - fit.connections_after;
        (w.storage * spare_storage + w.cpu * spare_cpu + w.connections * spare_conns)
            / (w.storage + w.cpu + w.connections)
    }

    fn place_unit(
        &self,
        view: &FleetView,
        unit: &Unit,
        feas: Feasibility,
    ) -> Result<PlacementDecision> {
        if feas.ok.is_empty() {
            let why = feas
                .main_blocker()
                .map_or_else(|| "no active cluster".to_owned(), ToString::to_string);
            return Err(SprigError::new(
                SprigCode::PlacementUnavailable,
                format!("no cluster in the tenant's allowed DCs can take a new shard: {why}"),
            ));
        }
        let mut scored: Vec<Alternative> = feas
            .ok
            .iter()
            .map(|(c, fit)| Alternative {
                cluster: c.clone(),
                score: self.score(view, unit, c, fit),
                full_after: fit.full_after,
                least_busy: None,
            })
            .collect();
        let min = scored.iter().map(|a| a.score).fold(f64::INFINITY, f64::min);
        let margin = self.settings.clear_win_margin;
        let near: Vec<usize> = (0..scored.len())
            .filter(|&i| scored[i].score <= min + margin)
            .collect();
        let (pick, how) = if near.len() == 1 {
            (near[0], PlaceHow::ClearWin)
        } else {
            let mut best: Option<(usize, f64)> = None;
            for &i in &near {
                let fit = &feas.ok[i].1;
                let lb = self.least_busy(fit);
                scored[i].least_busy = Some(lb);
                // Ties go to the lower cluster id: `near` is in id order and
                // only a strictly larger value replaces the best.
                if best.is_none_or(|(_, b)| lb > b) {
                    best = Some((i, lb));
                }
            }
            (best.map_or(near[0], |(i, _)| i), PlaceHow::LeastBusy)
        };
        let chosen = scored[pick].clone();
        let mut others: Vec<Alternative> = scored
            .into_iter()
            .enumerate()
            .filter(|(i, _)| *i != pick)
            .map(|(_, a)| a)
            .collect();
        others.sort_by(|a, b| a.score.total_cmp(&b.score).then(a.cluster.cmp(&b.cluster)));
        let mut alternatives = vec![chosen.clone()];
        alternatives.extend(others.into_iter().take(2));
        Ok(PlacementDecision {
            cluster: chosen.cluster,
            how,
            alternatives,
            blocked: feas.blocked,
        })
    }

    /// Updates the sustain windows and returns the hot clusters (sustained,
    /// fullest first), those still warming, and the trigger.
    fn sustained(
        &self,
        view: &FleetView,
        f: &BTreeMap<ClusterId, Fullness>,
        spread: f64,
        now: Timestamp,
    ) -> (Vec<HotCluster>, Vec<HotCluster>, Trigger) {
        let s = &self.settings;
        let sustain = s.sustain.as_secs();
        let mut st = self.sustain.lock().unwrap_or_else(PoisonError::into_inner);
        let mut hot = Vec::new();
        let mut warming = Vec::new();
        for (id, c) in &view.clusters {
            let full = f.get(id).map_or(0.0, |x| x.value);
            let eligible = c.state == ClusterState::Placeable && !c.probe_stale;
            if eligible && full > c.limits.high {
                let since = *st.hot_since.entry(id.clone()).or_insert(now);
                let h = HotCluster {
                    cluster: id.clone(),
                    full,
                    since,
                };
                if now >= since + sustain {
                    hot.push(h);
                } else {
                    warming.push(h);
                }
            } else {
                st.hot_since.remove(id);
            }
        }
        st.hot_since.retain(|id, _| view.clusters.contains_key(id));
        let spread_since = if spread > s.spread {
            Some(*st.spread_since.get_or_insert(now))
        } else {
            st.spread_since = None;
            None
        };
        drop(st);
        hot.sort_by(|a, b| b.full.total_cmp(&a.full).then(a.cluster.cmp(&b.cluster)));
        if !hot.is_empty() {
            return (hot, warming, Trigger::Hot);
        }
        let Some(since) = spread_since else {
            return (hot, warming, Trigger::None);
        };
        let fullest = view
            .clusters
            .values()
            .filter(|c| c.state == ClusterState::Placeable && !c.probe_stale)
            .filter_map(|c| f.get(&c.id).map(|x| (c.id.clone(), x.value)))
            .max_by(|a, b| a.1.total_cmp(&b.1).then(b.0.cmp(&a.0)));
        let Some((cluster, full)) = fullest else {
            return (hot, warming, Trigger::None);
        };
        let h = HotCluster {
            cluster,
            full,
            since,
        };
        if now >= since + sustain {
            (vec![h], warming, Trigger::Spread)
        } else {
            if !warming.iter().any(|w| w.cluster == h.cluster) {
                warming.push(h);
            }
            (hot, warming, Trigger::None)
        }
    }

    fn plan(&self, view: &FleetView, now: Timestamp) -> PlanReport {
        let s = &self.settings;
        let fullness = view.fullness();
        let spread = view.spread();
        let (hot, warming, trigger) = self.sustained(view, &fullness, spread, now);
        let mut r = PlanReport {
            at: now,
            fullness: fullness.clone(),
            spread,
            trigger,
            hot: hot.clone(),
            warming,
            moves: Vec::new(),
            notes: Vec::new(),
            approvals: Vec::new(),
            stuck: Vec::new(),
            rejected: Vec::new(),
            weights: s.weights,
            pinned: Vec::new(),
        };
        let mut w = Work::new(view);
        let max_active = s.max_concurrent_moves_per_dc as usize;
        // Residency changes and drains first: they skip cooldowns.
        Self::forced_moves(&mut w, &mut r, now, max_active);
        for h in &hot {
            if w.active() >= max_active {
                r.notes.push(note(
                    "concurrency",
                    Some(&h.cluster),
                    None,
                    format!(
                        "{} moves already run in this DC (balancer.max_concurrent_moves_per_dc)",
                        w.active()
                    ),
                ));
                break;
            }
            let hc = &view.clusters[&h.cluster];
            if let Some(t) = hc.last_move_off
                && now < t + hc.limits.cooldown_secs
            {
                r.notes.push(note(
                    "cluster_cooldown",
                    Some(&h.cluster),
                    None,
                    format!(
                        "cluster {} moved a shard at {t}; cooldown until {}",
                        h.cluster,
                        t + hc.limits.cooldown_secs
                    ),
                ));
                continue;
            }
            if w.slots_used(&h.cluster) >= hc.limits.copy_slots as usize {
                r.notes.push(note(
                    "copy_slots",
                    Some(&h.cluster),
                    None,
                    format!("cluster {} has no free copy slot", h.cluster),
                ));
                continue;
            }
            let reason = if trigger == Trigger::Spread {
                MoveReason::Spread
            } else {
                MoveReason::ClusterHot
            };
            match self.best_move(&w, &mut r, &h.cluster, reason, now) {
                Some(m) => {
                    w.apply(&m);
                    r.moves.push(m);
                }
                None if r.rejected.iter().any(|c| c.from == h.cluster) => {
                    r.notes.push(note(
                        "not_worth_it",
                        Some(&h.cluster),
                        None,
                        format!(
                            "no move off cluster {} is worth its cost: {}",
                            h.cluster,
                            r.rejected
                                .iter()
                                .find(|c| c.from == h.cluster)
                                .map_or(String::new(), |c| c.why.clone())
                        ),
                    ));
                    r.stuck.push(h.cluster.clone());
                }
                None => {
                    r.notes.push(note(
                        "no_target",
                        Some(&h.cluster),
                        None,
                        format!("no feasible move off cluster {}", h.cluster),
                    ));
                    r.stuck.push(h.cluster.clone());
                }
            }
        }
        r
    }

    /// Residency changes and drains.
    #[allow(clippy::too_many_lines)]
    fn forced_moves(w: &mut Work, r: &mut PlanReport, now: Timestamp, max_active: usize) {
        let view = w.view.clone();
        for s in view.shards.values() {
            let Some(c) = view.clusters.get(&s.cluster) else {
                continue;
            };
            let tenant = view.tenants.get(&s.key.tenant);
            let residency_bad = tenant
                .and_then(|t| t.allowed_dcs.as_ref())
                .is_some_and(|a| {
                    !a.contains(&c.dc) || c.replica_dcs.iter().any(|d| !a.contains(d))
                });
            let reason = if residency_bad {
                MoveReason::ResidencyChange
            } else if c.state == ClusterState::Draining {
                MoveReason::Drain
            } else {
                continue;
            };
            if w.moved.contains_key(&s.key) || s.locked {
                continue;
            }
            if s.pinned || tenant.is_some_and(|t| t.pinned) {
                let why = format!(
                    "did not move {} from {} ({}): {}; unpin it to let it move",
                    s.key,
                    s.cluster,
                    if reason == MoveReason::Drain {
                        "the cluster is draining"
                    } else {
                        "residency no longer allows the cluster"
                    },
                    pin_text(tenant)
                );
                r.notes
                    .push(note("pinned", Some(&s.cluster), Some(&s.key), why.clone()));
                r.pinned.push(PinnedSkip {
                    tenant: s.key.tenant,
                    shard: s.key,
                    cluster: s.cluster.clone(),
                    reason,
                    why,
                });
                continue;
            }
            if w.active() >= max_active {
                r.notes.push(note(
                    "concurrency",
                    Some(&s.cluster),
                    Some(&s.key),
                    format!("{} waits: the DC's move limit is reached", reason.code()),
                ));
                return;
            }
            let Some(unit) = unit_of(&w.view, s.key) else {
                continue;
            };
            let purpose = if reason == MoveReason::Drain {
                Purpose::Drain
            } else {
                Purpose::Residency
            };
            let feas = feasible_in(&w.view, &w.loads, &unit, purpose, now);
            let best = feas.ok.iter().min_by(|a, b| {
                a.1.full_after
                    .total_cmp(&b.1.full_after)
                    .then(a.0.cmp(&b.0))
            });
            let Some((to, fit)) = best else {
                r.notes.push(note(
                    "no_target",
                    Some(&s.cluster),
                    Some(&s.key),
                    format!("{} of {} has no feasible target", reason.code(), s.key),
                ));
                continue;
            };
            let before_from = w.full(&s.cluster);
            let after_from = w.full_without(&s.cluster, &unit.load);
            let m = PlannedMove {
                tenant: unit.tenant,
                shards: unit.shards.clone(),
                from: s.cluster.clone(),
                to: to.clone(),
                reason,
                gain: before_from - after_from,
                value: (before_from - after_from) / (unit.load.storage_bytes / GB + 1.0),
                size_bytes: unit.load.storage_bytes,
                from_full: (before_from, after_from),
                to_full: (w.full(to), fit.full_after),
                alternatives: Vec::new(),
                load: unit.load,
                judgement: None,
                why: format!(
                    "moved {} from {} to {}: {}",
                    s.key,
                    s.cluster,
                    to,
                    if reason == MoveReason::Drain {
                        "the source is draining"
                    } else {
                        "the tenant's residency no longer allows the source"
                    }
                ),
                considered: Vec::new(),
            };
            w.apply(&m);
            r.moves.push(m);
        }
    }

    /// The move off `h` worth making with the largest benefit minus cost
    /// (`crate::judge`); `None` if no move is worth making.
    #[allow(clippy::too_many_lines, clippy::many_single_char_names)]
    fn best_move(
        &self,
        w: &Work,
        r: &mut PlanReport,
        h: &ClusterId,
        reason: MoveReason,
        now: Timestamp,
    ) -> Option<PlannedMove> {
        let s = &self.settings;
        let view = &w.view;
        let full_h = w.full(h);
        let mut best: Option<PlannedMove> = None;
        let mut approval: Option<ApprovalNeeded> = None;
        let mut blocked_counts: BTreeMap<&'static str, usize> = BTreeMap::new();
        let mut others: Vec<Considered> = Vec::new();
        let hv = &view.clusters[h];
        let busy_h = w.busiest(h);
        #[allow(clippy::cast_precision_loss)]
        let typical = full_h
            / view
                .shards
                .values()
                .filter(|x| &x.cluster == h)
                .count()
                .max(1) as f64;
        for sv in view.shards.values().filter(|x| &x.cluster == h) {
            let Some(t) = view.tenants.get(&sv.key.tenant) else {
                continue;
            };
            if sv.pinned || t.pinned {
                r.pinned.push(PinnedSkip {
                    tenant: t.id,
                    shard: sv.key,
                    cluster: h.clone(),
                    reason,
                    why: format!(
                        "did not move {} from {}, which needs relief: {}",
                        sv.key,
                        h,
                        pin_text(Some(t))
                    ),
                });
            }
            let skip = if sv.pinned || t.pinned {
                Some("pinned")
            } else if sv.locked {
                Some("locked")
            } else if sv.cooldown_until > now {
                Some("shard_cooldown")
            } else if matches!(t.knobs.mode, BalancerMode::Off) {
                Some("tenant_off")
            } else if t.moves_last_day + w.planned_for(t.id) >= t.knobs.max_moves_per_day {
                Some("tenant_daily")
            } else if w.moved.contains_key(&sv.key) {
                Some("planned")
            } else {
                None
            };
            if let Some(g) = skip {
                *blocked_counts.entry(g).or_default() += 1;
                continue;
            }
            let Some(unit) = unit_of(view, sv.key) else {
                continue;
            };
            if unit.shards.iter().any(|k| {
                view.shards
                    .get(k)
                    .is_some_and(|x| x.pinned || x.locked || x.cooldown_until > now)
            }) {
                *blocked_counts.entry("bound_shard_busy").or_default() += 1;
                continue;
            }
            let too_big = unit.load.storage_bytes > t.knobs.auto_size_limit_bytes;
            let feas = feasible_in(view, &w.loads, &unit, Purpose::Rebalance, now);
            let after_h = w.full_without(h, &unit.load);
            let gain = full_h - after_h;
            let moved_recently = sv
                .history
                .iter()
                .any(|v| v.left_at + MOVE_BACK_WINDOW_SECS > now);
            let mut alts: Vec<(ClusterId, f64)> = Vec::new();
            // The best target for this shard: the largest benefit − cost.
            let mut cand: Option<(ClusterId, f64, f64, Judgement)> = None;
            for (c, fit) in &feas.ok {
                let cv = &view.clusters[c];
                if w.slots_used(c) >= cv.limits.copy_slots as usize {
                    *blocked_counts.entry("copy_slots").or_default() += 1;
                    continue;
                }
                if self.avoid_move_back
                    && sv
                        .history
                        .iter()
                        .any(|v| &v.cluster == c && v.left_at + MOVE_BACK_WINDOW_SECS > now)
                {
                    *blocked_counts.entry("move_back").or_default() += 1;
                    continue;
                }
                if fit.full_after > cv.limits.target {
                    *blocked_counts.entry("target_pct").or_default() += 1;
                    continue;
                }
                if fit.full_after - after_h > s.spread {
                    *blocked_counts.entry("would_invert").or_default() += 1;
                    continue;
                }
                alts.push((c.clone(), fit.full_after));
                let full_c = w.full(c);
                let j = judge(&JudgeInput {
                    source: (full_h, after_h),
                    target: (full_c, fit.full_after),
                    source_target_pct: hv.limits.target,
                    spread_pct: s.spread,
                    typical,
                    bytes: unit.load.storage_bytes,
                    target_storage: cv.cap.storage_bytes,
                    writes: unit.load.write_rows_per_sec,
                    busiest: busy_h.max(w.busiest(c)),
                    tenant_moved_recently: moved_recently,
                    in_flight: w.slots_used(h) + w.slots_used(c),
                    ratio: s.benefit_cost_ratio,
                    min_gain: s.min_gain,
                    weights: s.weights,
                });
                if cand.as_ref().is_none_or(|(_, _, _, b)| j.net > b.net) {
                    cand = Some((c.clone(), full_c, fit.full_after, j));
                }
            }
            let Some((to, to_before, to_after, j)) = cand else {
                continue;
            };
            let why = j.sentence(
                &sv.key.to_string(),
                h.as_str(),
                to.as_str(),
                unit.load.storage_bytes,
                unit.load.write_rows_per_sec,
            );
            if too_big {
                if j.worth && approval.as_ref().is_none_or(|a| gain > a.gain) {
                    approval = Some(ApprovalNeeded {
                        shard: sv.key,
                        size_bytes: unit.load.storage_bytes,
                        from: h.clone(),
                        to,
                        gain,
                    });
                }
                *blocked_counts.entry("size_limit").or_default() += 1;
                continue;
            }
            let considered = Considered {
                tenant: unit.tenant,
                shard: sv.key,
                from: h.clone(),
                to: to.clone(),
                size_bytes: unit.load.storage_bytes,
                judgement: j,
                why: why.clone(),
            };
            if !j.worth || best.as_ref().is_some_and(|b| b.value + 1e-12 >= j.net) {
                others.push(considered);
                continue;
            }
            if let Some(prev) = best.take() {
                others.push(Considered {
                    tenant: prev.tenant,
                    shard: prev.shards[0],
                    from: prev.from.clone(),
                    to: prev.to.clone(),
                    size_bytes: prev.size_bytes,
                    judgement: prev.judgement.unwrap_or(j),
                    why: prev
                        .why
                        .replacen("moved ", "a better move was found than moving ", 1),
                });
            }
            alts.retain(|(c, _)| c != &to);
            alts.sort_by(|a, b| a.1.total_cmp(&b.1));
            alts.truncate(2);
            best = Some(PlannedMove {
                tenant: unit.tenant,
                shards: unit.shards.clone(),
                from: h.clone(),
                to: to.clone(),
                reason,
                gain,
                value: j.net,
                size_bytes: unit.load.storage_bytes,
                from_full: (full_h, after_h),
                to_full: (to_before, to_after),
                alternatives: alts,
                load: unit.load,
                judgement: Some(j),
                why,
                considered: Vec::new(),
            });
        }
        others.sort_by(|a, b| b.judgement.net.total_cmp(&a.judgement.net));
        others.truncate(3);
        match &mut best {
            Some(b) => b.considered = others,
            None => r
                .rejected
                .extend(others.into_iter().filter(|c| !c.judgement.worth)),
        }
        for (g, n) in blocked_counts {
            r.notes.push(note(
                g,
                Some(h),
                None,
                format!("{n} candidate(s) on {h} skipped: {}", guardrail_text(g)),
            ));
        }
        if best.is_none()
            && let Some(a) = approval
        {
            r.approvals.push(a);
        }
        best
    }
}

fn guardrail_text(g: &str) -> &'static str {
    match g {
        "pinned" => "pinned by operator",
        "locked" => "locked by an active job",
        "shard_cooldown" => "in shard cooldown (balancer.shard_cooldown_hours)",
        "tenant_off" => "the tenant's balancer.mode is off",
        "tenant_daily" => "tenant at balancer.max_moves_per_tenant_per_day",
        "planned" => "already planned this run",
        "bound_shard_busy" => "a co-written shard is pinned, locked or cooling down",
        "copy_slots" => "target has no free copy slot (mover.max_copies_per_cluster)",
        "move_back" => "target is a cluster the shard left within 7 days",
        "target_pct" => "target would pass balancer.target_pct",
        "would_invert" => "target would end more than balancer.spread_pct fuller than the source",
        "size_limit" => "above migration.auto_size_limit_gb: needs approval",
        _ => "",
    }
}

fn note(g: &str, c: Option<&ClusterId>, s: Option<&ShardKey>, text: String) -> Note {
    Note {
        guardrail: g.to_owned(),
        cluster: c.cloned(),
        shard: s.copied(),
        text,
    }
}

/// The view as the run's planned moves change it.
struct Work {
    view: FleetView,
    loads: BTreeMap<ClusterId, LoadVector>,
    /// Shards planned this run, to their target.
    moved: BTreeMap<ShardKey, ClusterId>,
    planned: Vec<(ClusterId, ClusterId, TenantId)>,
}

impl Work {
    fn new(view: &FleetView) -> Self {
        Self {
            loads: view.loads(),
            view: view.clone(),
            moved: BTreeMap::new(),
            planned: Vec::new(),
        }
    }

    fn active(&self) -> usize {
        self.view.active.len() + self.planned.len()
    }

    fn slots_used(&self, c: &ClusterId) -> usize {
        self.view.active_on(c)
            + self
                .planned
                .iter()
                .filter(|(f, t, _)| f == c || t == c)
                .count()
    }

    fn planned_for(&self, t: TenantId) -> u32 {
        u32::try_from(self.planned.iter().filter(|(_, _, x)| *x == t).count()).unwrap_or(u32::MAX)
    }

    fn full(&self, c: &ClusterId) -> f64 {
        let l = self.loads.get(c).copied().unwrap_or_default();
        self.view
            .clusters
            .get(c)
            .map_or(0.0, |cv| cv.full_with(&l).value)
    }

    /// The higher of busy-time utilization and pressure on `c`.
    fn busiest(&self, c: &ClusterId) -> f64 {
        let l = self.loads.get(c).copied().unwrap_or_default();
        self.view.clusters.get(c).map_or(0.0, |cv| {
            cv.util_of(&l, crate::model::Dim::Busy)
                .max(cv.pressure.value(cv.cap.threads))
        })
    }

    fn full_without(&self, c: &ClusterId, load: &LoadVector) -> f64 {
        let l = (self.loads.get(c).copied().unwrap_or_default() - *load).clamp0();
        self.view
            .clusters
            .get(c)
            .map_or(0.0, |cv| cv.full_with(&l).value)
    }

    fn apply(&mut self, m: &PlannedMove) {
        if let Some(l) = self.loads.get_mut(&m.from) {
            *l = (*l - m.load).clamp0();
        }
        if let Some(l) = self.loads.get_mut(&m.to) {
            *l += m.load;
        }
        self.view.apply_move(&m.shards, &m.to);
        for k in &m.shards {
            self.moved.insert(*k, m.to.clone());
        }
        self.planned.push((m.from.clone(), m.to.clone(), m.tenant));
    }
}

impl PlacementPolicy for Planner {
    fn feasible(&self, view: &FleetView, unit: &Unit, now: Timestamp) -> Vec<(ClusterId, Fit)> {
        let purpose = if unit.from.is_some() {
            Purpose::Rebalance
        } else {
            Purpose::Place
        };
        crate::constraints::feasible(view, unit, purpose, now).ok
    }

    fn place(
        &self,
        view: &FleetView,
        req: &PlaceRequest,
        now: Timestamp,
    ) -> Result<PlacementDecision> {
        let unit = Unit {
            tenant: req.tenant,
            shards: req.units.clone(),
            load: req.predicted,
            from: None,
        };
        let feas = crate::constraints::feasible(view, &unit, Purpose::Place, now);
        self.place_unit(view, &unit, feas)
    }

    fn rebalance(&self, view: &FleetView, now: Timestamp) -> PlanReport {
        self.plan(view, now)
    }
}

#[cfg(test)]
mod tests;
