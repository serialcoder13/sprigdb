//! Scheduling planned moves (chapter 11, "Scheduling, guardrails and
//! recommendations"; build step 6).
//!
//! Each planned move becomes a `shard.move` decision and, in `auto` mode, a
//! job on a [`MoveExecutor`] carrying the decision id; the executor's
//! status updates the outcome. The real executor wraps chapter 10's
//! `Mover` (submit, status, cancel); the fleet simulator uses a fake one.
//!
//! The scheduler remembers what the view needs between runs and fills it
//! in with [`Scheduler::annotate`]: shard cooldowns, the last clusters each
//! shard left, targets excluded after a failure, the last move off each
//! cluster, each tenant's moves in the last day, and the moves in flight
//! (whose shards are locked and whose load is reserved on the target).
//!
//! Modes: `auto` writes `applied`/`scheduled` and submits; `recommend`
//! writes `recommended`/`proposed`; `dry_run` writes `dry_run`/`proposed`;
//! `off` writes nothing. A recommendation or dry-run decision is written
//! once, and again only when it changes materially (another target or
//! reason, or its net value moved by more than [`RECOMMEND_MATERIAL`]) or
//! [`RECOMMEND_REPEAT_SECS`] later (at least the shard cooldown). It used
//! to be written again after every shard cooldown, which in a busy fleet
//! repeated the same advice every few minutes.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::json;
use sprig_common::Result;
use sprig_common::ids::{ClusterId, TenantId};

use crate::decisions::{DecisionLog, Mode, NewDecision, Outcome};
use crate::model::{ActiveMove, FleetView, ShardKey, Timestamp, Visit};
use crate::plan::{MoveReason, PlanReport, PlannedMove};
use crate::settings::BalancerMode;
use crate::thrash::{ThrashCounters, cooldown_secs, is_move_back, remember};

/// A move to run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MoveOrder {
    /// The tenant.
    pub tenant: TenantId,
    /// The shards, moving together.
    pub shards: Vec<ShardKey>,
    /// Source.
    pub from: ClusterId,
    /// Target.
    pub to: ClusterId,
    /// Why.
    pub reason: MoveReason,
    /// The `shard.move` decision.
    pub decision_id: u64,
    /// Bytes to copy.
    pub size_bytes: f64,
}

/// An executor's handle on a move (chapter 10's job id as text).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MoveTicket(pub String);

/// Where a move stands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum MoveStatus {
    /// Waiting for a worker or a slot.
    Queued,
    /// Copying, catching up or cutting over.
    Running,
    /// The shard map points at the target.
    Succeeded,
    /// Rolled back; the shard stays on the source.
    Failed {
        /// Why.
        reason: String,
    },
    /// Cancelled before cutover.
    Cancelled,
}

/// Runs moves: chapter 10's mover in production, a fake in the simulator.
#[async_trait::async_trait]
pub trait MoveExecutor: Send + Sync {
    /// Starts a move.
    ///
    /// # Errors
    /// The executor's; the decision becomes `failed`.
    async fn submit(&self, order: &MoveOrder) -> Result<MoveTicket>;
    /// A move's status.
    ///
    /// # Errors
    /// The executor's; the move stays in flight and is asked again.
    async fn status(&self, ticket: &MoveTicket) -> Result<MoveStatus>;
    /// Cancels a move before cutover.
    ///
    /// # Errors
    /// The executor's.
    async fn cancel(&self, ticket: &MoveTicket, reason: &str) -> Result<()>;
}

/// An executor that runs nothing (no mover in this process): every
/// submission fails, so `auto` mode degrades to failed decisions with a
/// clear reason.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoMover;

#[async_trait::async_trait]
impl MoveExecutor for NoMover {
    async fn submit(&self, _order: &MoveOrder) -> Result<MoveTicket> {
        Err(sprig_common::SprigError::internal(
            "no mover in this process: moves can't run",
        ))
    }
    async fn status(&self, _ticket: &MoveTicket) -> Result<MoveStatus> {
        Ok(MoveStatus::Failed {
            reason: "no mover".to_owned(),
        })
    }
    async fn cancel(&self, _ticket: &MoveTicket, _reason: &str) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct ShardMemory {
    cooldown_until: Timestamp,
    history: Vec<Visit>,
    moves_back: u32,
    excluded: Vec<(ClusterId, Timestamp)>,
}

/// A move in flight.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InFlight {
    /// The executor's handle.
    pub ticket: MoveTicket,
    /// The order.
    pub order: MoveOrder,
    /// The load moving (reserved on the target).
    pub load: crate::model::LoadVector,
    /// When it was submitted.
    pub started_at: Timestamp,
}

/// A finished move.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Finished {
    /// The order.
    pub order: MoveOrder,
    /// Succeeded, failed or cancelled.
    pub outcome: Outcome,
    /// It went back to a cluster the shard left within 7 days.
    pub reverted: bool,
    /// When.
    pub at: Timestamp,
}

/// What one scheduling pass did with each planned move.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Scheduled {
    /// The planned move.
    pub planned: PlannedMove,
    /// The decision's id (0 if buffered or none was written).
    pub decision_id: u64,
    /// The mode it ran in.
    pub mode: BalancerMode,
    /// The executor's ticket, in `auto`.
    pub ticket: Option<MoveTicket>,
    /// A guardrail that stopped it here, if any.
    pub blocked: Option<String>,
}

/// A recommendation is written again after this long even when nothing
/// changed (a day; the shard cooldown when longer).
pub const RECOMMEND_REPEAT_SECS: Timestamp = 86_400;

/// A recommendation whose net value moved by more than this fraction is
/// written again at once.
pub const RECOMMEND_MATERIAL: f64 = 0.25;

/// The last recommendation (or dry-run decision) written for a shard.
#[derive(Debug, Clone, PartialEq)]
struct Recommended {
    to: ClusterId,
    reason: MoveReason,
    value: f64,
    at: Timestamp,
}

impl Recommended {
    /// Whether `m`, at `now`, says the same thing as this one did.
    fn same(&self, m: &PlannedMove, now: Timestamp, repeat: Timestamp) -> bool {
        let close = (m.value - self.value).abs() <= RECOMMEND_MATERIAL * self.value.abs().max(1e-9);
        self.to == m.to && self.reason == m.reason && close && self.at + repeat > now
    }
}

/// The scheduler and its memory.
#[derive(Debug, Clone, Default)]
pub struct Scheduler {
    /// The DC, for decisions.
    pub dc: String,
    /// The controller id, for decisions.
    pub actor: String,
    memory: BTreeMap<ShardKey, ShardMemory>,
    last_move_off: BTreeMap<ClusterId, Timestamp>,
    tenant_moves: BTreeMap<TenantId, Vec<Timestamp>>,
    recommended: BTreeMap<ShardKey, Recommended>,
    rejected_seen: BTreeMap<(ShardKey, ClusterId), Timestamp>,
    in_flight: Vec<InFlight>,
    /// The thrash counters.
    pub thrash: ThrashCounters,
    /// Moves by (reason, outcome).
    pub moves_total: BTreeMap<(String, String), u64>,
    /// Moves stopped by a guardrail, by guardrail.
    pub blocked_total: BTreeMap<String, u64>,
}

fn us(t: Timestamp) -> i64 {
    i64::try_from(t).unwrap_or(i64::MAX / 1_000_000) * 1_000_000
}

impl Scheduler {
    /// A scheduler deciding in `dc` as `actor`.
    #[must_use]
    pub fn new(dc: impl Into<String>, actor: impl Into<String>) -> Self {
        Self {
            dc: dc.into(),
            actor: actor.into(),
            ..Self::default()
        }
    }

    /// Moves in flight.
    #[must_use]
    pub fn in_flight(&self) -> &[InFlight] {
        &self.in_flight
    }

    /// Fills the view with what the scheduler remembers (see the module
    /// docs).
    pub fn annotate(&mut self, view: &mut FleetView, now: Timestamp) {
        for (k, m) in &mut self.memory {
            m.excluded.retain(|(_, until)| *until > now);
            if let Some(s) = view.shards.get_mut(k) {
                s.cooldown_until = s.cooldown_until.max(m.cooldown_until);
                s.history.clone_from(&m.history);
                s.excluded.clone_from(&m.excluded);
            }
        }
        for (c, t) in &self.last_move_off {
            if let Some(cv) = view.clusters.get_mut(c) {
                cv.last_move_off = Some(cv.last_move_off.map_or(*t, |x| x.max(*t)));
            }
        }
        for (t, v) in &mut self.tenant_moves {
            v.retain(|x| *x + 86_400 > now);
            if let Some(tv) = view.tenants.get_mut(t) {
                tv.moves_last_day = u32::try_from(v.len()).unwrap_or(u32::MAX);
            }
        }
        for f in &self.in_flight {
            for k in &f.order.shards {
                if let Some(s) = view.shards.get_mut(k) {
                    s.locked = true;
                }
            }
            if !view
                .active
                .iter()
                .any(|a| a.shards == f.order.shards && a.to == f.order.to)
            {
                view.active.push(ActiveMove {
                    shards: f.order.shards.clone(),
                    from: f.order.from.clone(),
                    to: f.order.to.clone(),
                    load: f.load,
                });
            }
        }
    }

    /// Turns a plan into decisions and (in `auto`) jobs. `mode_of` gives
    /// each tenant's `balancer.mode`; `max_active` is
    /// `balancer.max_concurrent_moves_per_dc`.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn schedule(
        &mut self,
        report: &PlanReport,
        view: &FleetView,
        mode_of: &(dyn Fn(TenantId) -> BalancerMode + Send + Sync),
        max_active: usize,
        exec: &dyn MoveExecutor,
        log: &dyn DecisionLog,
        now: Timestamp,
    ) -> Vec<Scheduled> {
        let mut out = Vec::new();
        for m in &report.moves {
            let mode = mode_of(m.tenant);
            let mut sch = Scheduled {
                planned: m.clone(),
                decision_id: 0,
                mode,
                ticket: None,
                blocked: None,
            };
            if mode == BalancerMode::Off {
                sch.blocked = Some("mode_off".to_owned());
                out.push(sch);
                continue;
            }
            if mode == BalancerMode::Approve {
                // Held in chapter 52's approvals inbox; the item is the
                // record, so no decision is written here.
                sch.blocked = Some("awaiting_approval".to_owned());
                out.push(sch);
                continue;
            }
            let forced = matches!(m.reason, MoveReason::ResidencyChange | MoveReason::Drain);
            if mode == BalancerMode::Auto && self.in_flight.len() >= max_active {
                self.block("concurrency");
                sch.blocked = Some("concurrency".to_owned());
                out.push(sch);
                continue;
            }
            if mode == BalancerMode::Auto
                && !forced
                && let Some(t) = view.tenants.get(&m.tenant)
                && self.moves_today(m.tenant, now) >= t.knobs.max_moves_per_day
            {
                self.block("tenant_daily");
                sch.blocked = Some("tenant_daily".to_owned());
                out.push(sch);
                continue;
            }
            if mode != BalancerMode::Auto {
                let cooldown = view
                    .tenants
                    .get(&m.tenant)
                    .map_or(86_400, |t| t.knobs.shard_cooldown_secs);
                let repeat = RECOMMEND_REPEAT_SECS.max(cooldown);
                if self
                    .recommended
                    .get(&m.shards[0])
                    .is_some_and(|r| r.same(m, now, repeat))
                {
                    sch.blocked = Some("already_recommended".to_owned());
                    out.push(sch);
                    continue;
                }
                self.recommended.insert(
                    m.shards[0],
                    Recommended {
                        to: m.to.clone(),
                        reason: m.reason,
                        value: m.value,
                        at: now,
                    },
                );
            }
            let (dmode, outcome) = match mode {
                BalancerMode::Auto => (Mode::Applied, Outcome::Scheduled),
                BalancerMode::Recommend => (Mode::Recommended, Outcome::Proposed),
                _ => (Mode::DryRun, Outcome::Proposed),
            };
            let d = self.decision(m, report, dmode, outcome, now);
            let id = match log.record(m.tenant, &d).await {
                Ok(id) => id,
                Err(e) => {
                    tracing::warn!(tenant = %m.tenant, error = %e, "shard.move decision not written");
                    if mode == BalancerMode::Auto {
                        // No move without its decision.
                        self.block("decision_log");
                        sch.blocked = Some("decision_log".to_owned());
                        out.push(sch);
                        continue;
                    }
                    0
                }
            };
            sch.decision_id = id;
            if mode != BalancerMode::Auto {
                self.count(
                    m.reason,
                    if mode == BalancerMode::Recommend {
                        "recommended"
                    } else {
                        "dry_run"
                    },
                );
                out.push(sch);
                continue;
            }
            let order = MoveOrder {
                tenant: m.tenant,
                shards: m.shards.clone(),
                from: m.from.clone(),
                to: m.to.clone(),
                reason: m.reason,
                decision_id: id,
                size_bytes: m.size_bytes,
            };
            match exec.submit(&order).await {
                Ok(ticket) => {
                    self.last_move_off.insert(m.from.clone(), now);
                    self.tenant_moves.entry(m.tenant).or_default().push(now);
                    self.in_flight.push(InFlight {
                        ticket: ticket.clone(),
                        order,
                        load: m.load,
                        started_at: now,
                    });
                    self.count(m.reason, "scheduled");
                    sch.ticket = Some(ticket);
                }
                Err(e) => {
                    tracing::warn!(tenant = %m.tenant, shard = %m.shards[0], error = %e,
                        "move not submitted");
                    let _ = log
                        .update_outcome(m.tenant, id, Outcome::Failed, us(now))
                        .await;
                    self.count(m.reason, "failed");
                    self.exclude(m, view, now);
                    sch.blocked = Some("submit_failed".to_owned());
                }
            }
            out.push(sch);
        }
        self.record_rejected(report, mode_of, log, now).await;
        out
    }

    /// Writes a `rejected` `shard.move` decision for the best moves off a
    /// stuck cluster that weren't worth their cost, once a day per shard
    /// and target, so `sprigctl explain decision` can say why nothing
    /// moved.
    async fn record_rejected(
        &mut self,
        report: &PlanReport,
        mode_of: &(dyn Fn(TenantId) -> BalancerMode + Send + Sync),
        log: &dyn DecisionLog,
        now: Timestamp,
    ) {
        for c in &report.rejected {
            let mode = match mode_of(c.tenant) {
                BalancerMode::Off => continue,
                BalancerMode::Auto => Mode::Applied,
                BalancerMode::Recommend | BalancerMode::Approve => Mode::Recommended,
                BalancerMode::DryRun => Mode::DryRun,
            };
            let key = (c.shard, c.to.clone());
            if self
                .rejected_seen
                .get(&key)
                .is_some_and(|at| *at + 86_400 > now)
            {
                continue;
            }
            self.rejected_seen.insert(key, now);
            let d = NewDecision {
                decided_at_us: us(now),
                component: "balancer".to_owned(),
                kind: "shard.move".to_owned(),
                subject_type: "shard".to_owned(),
                subject_id: c.shard.to_string(),
                mode,
                outcome: Outcome::Rejected,
                reason_code: if c.judgement.below_floor {
                    "below_min_gain"
                } else {
                    "not_worth_it"
                }
                .to_owned(),
                summary: c.why.clone(),
                evidence: json!({
                    "from": c.from.to_string(),
                    "to": c.to.to_string(),
                    "size_bytes": c.size_bytes,
                    "judgement": c.judgement,
                    "trigger": report.trigger,
                    "spread": report.spread,
                }),
                alternatives: None,
                job_id: None,
                parent_id: None,
                actor: self.actor.clone(),
                dc_id: self.dc.clone(),
            };
            if let Err(e) = log.record(c.tenant, &d).await {
                tracing::warn!(tenant = %c.tenant, error = %e, "rejected-move decision not written");
            }
            self.block("not_worth_it");
        }
        for p in &report.pinned {
            let mode = match mode_of(p.tenant) {
                BalancerMode::Off => continue,
                BalancerMode::Auto => Mode::Applied,
                BalancerMode::Recommend | BalancerMode::Approve => Mode::Recommended,
                BalancerMode::DryRun => Mode::DryRun,
            };
            let key = (p.shard, p.cluster.clone());
            if self
                .rejected_seen
                .get(&key)
                .is_some_and(|at| *at + 86_400 > now)
            {
                continue;
            }
            self.rejected_seen.insert(key, now);
            let d = NewDecision {
                decided_at_us: us(now),
                component: "balancer".to_owned(),
                kind: "shard.move".to_owned(),
                subject_type: "shard".to_owned(),
                subject_id: p.shard.to_string(),
                mode,
                outcome: Outcome::Rejected,
                reason_code: "pinned".to_owned(),
                summary: p.why.clone(),
                evidence: json!({
                    "cluster": p.cluster.to_string(),
                    "would_move_for": p.reason.code(),
                    "trigger": report.trigger,
                }),
                alternatives: None,
                job_id: None,
                parent_id: None,
                actor: self.actor.clone(),
                dc_id: self.dc.clone(),
            };
            if let Err(e) = log.record(p.tenant, &d).await {
                tracing::warn!(tenant = %p.tenant, error = %e, "pinned decision not written");
            }
        }
        self.rejected_seen.retain(|_, at| *at + 86_400 > now);
    }

    fn moves_today(&self, t: TenantId, now: Timestamp) -> u32 {
        let n = self
            .tenant_moves
            .get(&t)
            .map_or(0, |v| v.iter().filter(|x| **x + 86_400 > now).count());
        u32::try_from(n).unwrap_or(u32::MAX)
    }

    fn block(&mut self, g: &str) {
        *self.blocked_total.entry(g.to_owned()).or_default() += 1;
        metrics::counter!("sprig_balancer_blocked_total", "guardrail" => g.to_owned()).increment(1);
    }

    fn count(&mut self, reason: MoveReason, outcome: &str) {
        *self
            .moves_total
            .entry((reason.code().to_owned(), outcome.to_owned()))
            .or_default() += 1;
        metrics::counter!("sprig_balancer_moves_total",
            "reason" => reason.code(), "outcome" => outcome.to_owned())
        .increment(1);
    }

    fn exclude(&mut self, m: &PlannedMove, view: &FleetView, now: Timestamp) {
        let cooldown = view
            .tenants
            .get(&m.tenant)
            .map_or(86_400, |t| t.knobs.shard_cooldown_secs);
        for k in &m.shards {
            self.memory
                .entry(*k)
                .or_default()
                .excluded
                .push((m.to.clone(), now + cooldown));
        }
    }

    fn decision(
        &self,
        m: &PlannedMove,
        r: &PlanReport,
        mode: Mode,
        outcome: Outcome,
        now: Timestamp,
    ) -> NewDecision {
        let shards: Vec<String> = m.shards.iter().map(ToString::to_string).collect();
        NewDecision {
            decided_at_us: us(now),
            component: "balancer".to_owned(),
            kind: "shard.move".to_owned(),
            subject_type: "shard".to_owned(),
            subject_id: m.shards[0].to_string(),
            mode,
            outcome,
            reason_code: m.reason.code().to_owned(),
            summary: if m.why.is_empty() {
                format!(
                    "move {} from {} to {}: {}, gain {:.3}",
                    m.shards[0],
                    m.from,
                    m.to,
                    m.reason.code(),
                    m.gain
                )
            } else {
                m.why.clone()
            },
            evidence: json!({
                "shards": shards,
                "from": m.from.to_string(),
                "to": m.to.to_string(),
                "gain": m.gain,
                "net": m.value,
                "judgement": m.judgement,
                "size_bytes": m.size_bytes,
                "from_full": [m.from_full.0, m.from_full.1],
                "to_full": [m.to_full.0, m.to_full.1],
                "trigger": r.trigger,
                "spread": r.spread,
            }),
            alternatives: Some(json!({
                "targets": m
                    .alternatives
                    .iter()
                    .map(|(c, f)| json!({"cluster": c.to_string(), "full_after": f}))
                    .collect::<Vec<_>>(),
                "considered": m
                    .considered
                    .iter()
                    .map(|c| json!({
                        "shard": c.shard.to_string(), "to": c.to.to_string(),
                        "benefit": c.judgement.benefit, "cost": c.judgement.cost,
                        "worth": c.judgement.worth, "why": c.why,
                    }))
                    .collect::<Vec<_>>(),
            })),
            job_id: None,
            parent_id: None,
            actor: self.actor.clone(),
            dc_id: self.dc.clone(),
        }
    }

    /// Asks the executor about every move in flight, records outcomes and
    /// updates cooldowns, history and the thrash counters.
    pub async fn poll(
        &mut self,
        view: &FleetView,
        exec: &dyn MoveExecutor,
        log: &dyn DecisionLog,
        now: Timestamp,
    ) -> Vec<Finished> {
        let mut done = Vec::new();
        let mut keep = Vec::new();
        let flights = std::mem::take(&mut self.in_flight);
        for f in flights {
            let st = match exec.status(&f.ticket).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(ticket = %f.ticket.0, error = %e, "move status unknown");
                    keep.push(f);
                    continue;
                }
            };
            let outcome = match st {
                MoveStatus::Queued | MoveStatus::Running => {
                    keep.push(f);
                    continue;
                }
                MoveStatus::Succeeded => Outcome::Succeeded,
                MoveStatus::Failed { .. } => Outcome::Failed,
                MoveStatus::Cancelled => Outcome::Cancelled,
            };
            let _ = log
                .update_outcome(f.order.tenant, f.order.decision_id, outcome, us(now))
                .await;
            let knobs = view
                .tenants
                .get(&f.order.tenant)
                .map(|t| t.knobs)
                .unwrap_or_default();
            let mut reverted = false;
            if outcome == Outcome::Succeeded {
                {
                    for (i, k) in f.order.shards.iter().enumerate() {
                        let mem = self.memory.entry(*k).or_default();
                        let back = is_move_back(&mem.history, &f.order.to, now);
                        if back {
                            mem.moves_back += 1;
                            if i == 0 {
                                reverted = true;
                            }
                        }
                        remember(&mut mem.history, f.order.from.clone(), now);
                        mem.cooldown_until = now
                            + cooldown_secs(
                                knobs.shard_cooldown_secs,
                                mem.moves_back,
                                knobs.max_cooldown_secs,
                            );
                    }
                    if reverted {
                        let after_recovery = view
                            .clusters
                            .get(&f.order.to)
                            .and_then(|c| c.recovered_at)
                            .is_some_and(|t| t + crate::plan::MOVE_BACK_WINDOW_SECS > now);
                        self.thrash.reverted += 1;
                        if after_recovery {
                            self.thrash.reverted_after_recovery += 1;
                        }
                        metrics::counter!("sprig_balancer_moves_reverted_total",
                            "after_recovery" => if after_recovery { "true" } else { "false" })
                        .increment(1);
                    }
                    self.count(f.order.reason, "succeeded");
                }
            } else {
                {
                    for k in &f.order.shards {
                        self.memory
                            .entry(*k)
                            .or_default()
                            .excluded
                            .push((f.order.to.clone(), now + knobs.shard_cooldown_secs));
                    }
                    self.count(
                        f.order.reason,
                        if outcome == Outcome::Failed {
                            "failed"
                        } else {
                            "cancelled"
                        },
                    );
                }
            }
            done.push(Finished {
                order: f.order,
                outcome,
                reverted,
                at: now,
            });
        }
        self.in_flight = keep;
        done
    }
}

#[cfg(test)]
mod tests;
