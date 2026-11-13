//! Automatic moves between data centers (chapter 24, "The migration
//! decision" and "Damping DC ping-pong"; build steps 7 and 9, r2last).
//!
//! ```text
//! dc_migrate loop, on each DC leader, every migration.evaluate_seconds, per
//! tenant T homed here:
//! 1. skip if migration.mode = off, T is pinned (either pin), not active, or
//!    inside its cooldown: last_move + migration.cooldown_hours * 2^k, capped
//!    at migration.max_cooldown_days, k = last_move.moves_back
//! 2. w[d] = reads[d] + migration.write_weight * writes[d] per client DC with
//!    a known id over migration.window_minutes; W = sum w[d]
//! 3. W < migration.min_signal_requests -> relief only (step 6)
//! 4. C = active, non-witness DCs T's residency allows that have been stable
//!    (dc-load's stable_since) for recovery.stability_minutes, and the home;
//!    L(c) = sum w[d] * rtt(d, c) / W from chapter 23's latency matrix (c
//!    dropped when a needed pair is unmeasured); best = argmin L, ties to the
//!    home, then the least busy
//! 5. qualifies: best != home, gain >= L(home) * min_improvement_pct / 100 and
//!    gain >= min_improvement_ms; a best outside the residency (over every
//!    active DC) raises ResidencyRemoteTraffic, never a move
//! 6. relief, when nothing qualifies on latency: the home's lowest spare <
//!    placement.dc_relief_spare_pct and least_busy(C) beats the home's score
//!    by placement.least_busy_margin
//! 7. hysteresis: the same target must qualify for migration.sustain_minutes
//!    (a different target restarts the clock; not qualifying clears it)
//! 8. guards: size <= migration.auto_size_limit_gb (else it waits for an
//!    operator), fewer than migration.max_concurrent_per_dc moves at both
//!    ends; a job standby of T in the target is dropped first (decision
//!    RF11: the standby holds the tenant's lock there) and the move starts
//!    on the next pass
//! 9. auto: the move (chapter 24's one-command move, cause traffic or relief);
//!    recommend: only the decision
//! ```
//!
//! Each evaluation is kept in memory for `sprigctl tenant traffic` (its
//! `auto` field) and logged; the traffic window and the candidate's clock
//! are soft state, so a new leader starts both again.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sprig_balancer::settings::{
    AUTO_SIZE_LIMIT_GB, LEAST_BUSY_WEIGHTS, LeastBusyWeights, MAX_COOLDOWN_DAYS,
    RECOVERY_STABILITY_MINUTES,
};
use sprig_common::ids::{DcId, TenantId};
use sprig_common::{Result, SprigCode};
use sprig_settings::{Apply, HardBounds, Scopes, Setting};
use sprig_shard::home::{self, HomeEntry};

use crate::least_busy::{self, DcLoad};
use crate::migrate::{DcMoves, MoveTenantRequest};
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};
use crate::traffic::{MIN_IMPROVEMENT_PCT, MIN_SIGNAL_REQUESTS, WINDOW_MINUTES, WRITE_WEIGHT};

/// Tenant, dc and global.
const TDG: Scopes = Scopes(0b11_0100);

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $min:expr, $max:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: HardBounds::range($min, $max),
            apply: Apply::Live,
            operator_only: true,
            owner: 24,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

/// `migration.mode`: `auto`, `approve` (held for approval, chapter 52),
/// `recommend` (the dry run) or `off`.
pub static MODE: Setting<Cow<'static, str>> = Setting {
    key: "migration.mode",
    default: Cow::Borrowed("auto"),
    scopes: TDG,
    hard: HardBounds::one_of(&["auto", "approve", "recommend", "off"]),
    apply: Apply::Live,
    operator_only: true,
    owner: 24,
    doc: "auto, approve (held for approval), recommend (dry run) or off",
};
sprig_settings::register!(MODE);

key! {
    /// `migration.min_improvement_ms`.
    MIN_IMPROVEMENT_MS: u64 = "migration.min_improvement_ms", 5, TDG, 0, 60_000,
    "Absolute gain a move needs"
}
key! {
    /// `migration.evaluate_seconds`.
    EVALUATE_SECONDS: u64 = "migration.evaluate_seconds", 60, Scopes::PROCESS, 1, 86_400,
    "Decision period"
}
key! {
    /// `migration.sustain_minutes`.
    SUSTAIN_MINUTES: f64 = "migration.sustain_minutes", 15.0, TDG, 0, 10_080,
    "How long the candidate must hold"
}
key! {
    /// `migration.cooldown_hours`.
    COOLDOWN_HOURS: f64 = "migration.cooldown_hours", 6.0, TDG, 0, 8760,
    "Wait after a move; doubles per move back"
}
key! {
    /// `migration.max_concurrent_per_dc`.
    MAX_CONCURRENT_PER_DC: u64 = "migration.max_concurrent_per_dc", 2, Scopes::PROCESS, 0, 1000,
    "Moves at once per DC"
}

/// What the decision needs besides the traffic and the matrix.
#[derive(Debug, Clone, PartialEq)]
pub struct Params {
    /// `migration.min_signal_requests`.
    pub min_signal: u64,
    /// `migration.min_improvement_pct`.
    pub min_pct: u64,
    /// `migration.min_improvement_ms`.
    pub min_ms: u64,
    /// `placement.dc_relief_spare_pct`.
    pub relief_spare_pct: u64,
    /// `placement.least_busy_margin`.
    pub margin: f64,
    /// `placement.least_busy_weights`.
    pub weights: LeastBusyWeights,
}

/// One evaluation (steps 2-6).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Evaluation {
    /// `no_signal`, `stay`, `candidate` (on latency) or `relief`.
    pub decision: String,
    /// The home.
    pub home: DcId,
    /// `W`.
    pub weighted_total: u64,
    /// `L(c)` per candidate DC, ms.
    pub latency_ms: BTreeMap<String, f64>,
    /// The best DC (the target when it qualifies).
    pub best: Option<DcId>,
    /// `L(home) - L(best)`, ms.
    pub gain_ms: f64,
    /// Why it stays, when it does.
    pub reason: String,
    /// The best DC over every active DC when it lies outside the
    /// residency (the `ResidencyRemoteTraffic` alert).
    pub remote_best: Option<DcId>,
    /// Least-busy scores of the candidate DCs.
    pub scores: BTreeMap<String, f64>,
}

impl Evaluation {
    /// Whether it asks for a move.
    #[must_use]
    pub fn qualifies(&self) -> bool {
        matches!(self.decision.as_str(), "candidate" | "relief")
    }
}

#[allow(clippy::cast_precision_loss)]
fn weighted_latency(
    clients: &[(String, u64)],
    total: u64,
    c: &str,
    rtt: &dyn Fn(&str, &str) -> Option<f64>,
) -> Option<f64> {
    let mut sum = 0.0;
    for (d, w) in clients {
        sum += *w as f64 * rtt(d, c)?;
    }
    Some(sum / total.max(1) as f64)
}

/// Steps 2-6 (module docs), pure: the same inputs give the same decision.
/// `clients` holds `(client DC, w[d])` for known ids; `cands` the DCs of
/// step 4 (the home included); `active` every active, non-witness DC (for
/// the residency alert).
#[must_use]
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub fn evaluate(
    home: &DcId,
    clients: &[(String, u64)],
    cands: &[DcId],
    active: &[DcId],
    rtt: &dyn Fn(&str, &str) -> Option<f64>,
    loads: &BTreeMap<DcId, DcLoad>,
    now_ms: u64,
    p: &Params,
) -> Evaluation {
    let total: u64 = clients.iter().map(|(_, w)| *w).sum();
    let scores: BTreeMap<String, f64> = cands
        .iter()
        .map(|d| {
            (
                d.to_string(),
                least_busy::score(loads.get(d), &p.weights, now_ms),
            )
        })
        .collect();
    let mut ev = Evaluation {
        decision: "stay".into(),
        home: home.clone(),
        weighted_total: total,
        latency_ms: BTreeMap::new(),
        best: None,
        gain_ms: 0.0,
        reason: String::new(),
        remote_best: None,
        scores,
    };
    let latency_wins = if total == 0 || total < p.min_signal {
        ev.decision = "no_signal".into();
        ev.reason = format!(
            "{total} weighted requests with DC ids; {} needed",
            p.min_signal
        );
        false
    } else {
        for c in cands {
            if let Some(l) = weighted_latency(clients, total, c.as_str(), rtt) {
                ev.latency_ms.insert(c.to_string(), l);
            }
        }
        match ev.latency_ms.get(home.as_str()).copied() {
            None => {
                ev.reason = "the latency matrix lacks a pair the home needs".into();
                false
            }
            Some(lh) => {
                // argmin L; ties to the home, then the least busy, then the id.
                let mut best = (home.clone(), lh);
                for c in cands {
                    let Some(l) = ev.latency_ms.get(c.as_str()).copied() else {
                        continue;
                    };
                    let (b, bl) = &best;
                    let better = l < *bl - 1e-9
                        || ((l - *bl).abs() <= 1e-9
                            && b != home
                            && (ev.scores.get(c.as_str()) > ev.scores.get(b.as_str())
                                || (ev.scores.get(c.as_str()) == ev.scores.get(b.as_str())
                                    && c < b)));
                    if better {
                        best = (c.clone(), l);
                    }
                }
                let gain = lh - best.1;
                ev.gain_ms = gain;
                ev.best = Some(best.0.clone());
                // ResidencyRemoteTraffic: the best over every active DC.
                let mut remote: Option<(DcId, f64)> = None;
                for d in active {
                    if cands.contains(d) {
                        continue;
                    }
                    if let Some(l) = weighted_latency(clients, total, d.as_str(), rtt)
                        && l < best.1
                        && remote.as_ref().is_none_or(|(_, r)| l < *r)
                    {
                        remote = Some((d.clone(), l));
                    }
                }
                #[allow(clippy::cast_precision_loss)]
                let (min_pct, min_ms) = (p.min_pct as f64, p.min_ms as f64);
                if let Some((d, l)) = remote
                    && lh - l >= lh * min_pct / 100.0
                    && lh - l >= min_ms
                {
                    ev.remote_best = Some(d);
                }
                let pct_ok = gain >= lh * min_pct / 100.0;
                let ms_ok = gain >= min_ms;
                if best.0 != *home && gain > 0.0 && pct_ok && ms_ok {
                    ev.decision = "candidate".into();
                    true
                } else {
                    ev.reason = if best.0 == *home {
                        "the home has the lowest weighted latency".into()
                    } else {
                        format!(
                            "{} would gain {gain:.1} ms of {lh:.1} ms: below {}% or {} ms",
                            best.0, p.min_pct, p.min_ms
                        )
                    };
                    false
                }
            }
        }
    };
    if latency_wins {
        return ev;
    }
    // Relief: a busy home sheds the tenant to a clearly less busy DC.
    if let Some(h) = loads.get(home) {
        let low = h.storage_spare.min(h.cpu_spare).min(h.conn_spare);
        #[allow(clippy::cast_precision_loss)]
        if low < p.relief_spare_pct as f64 / 100.0 {
            let others: Vec<DcId> = cands.iter().filter(|d| *d != home).cloned().collect();
            let hs = least_busy::score(Some(h), &p.weights, now_ms);
            if let Some((t, s)) = least_busy::least_busy(&others, loads, &p.weights, now_ms)
                && s >= hs + p.margin
            {
                ev.decision = "relief".into();
                ev.best = Some(t.clone());
                ev.reason = format!(
                    "the home's lowest spare is {:.0}%; {t} scores {s:.2} against {hs:.2}",
                    low * 100.0
                );
            }
        }
    }
    ev
}

/// The cooldown after `last_move` (`at_ms`, `moves_back`): until when no
/// automatic move starts.
#[must_use]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
pub fn cooldown_until(last_move: Option<&Value>, hours: f64, max_days: u64) -> Option<u64> {
    let lm = last_move?;
    let at = lm.get("at_ms")?.as_u64()?;
    let k = lm.get("moves_back").and_then(Value::as_u64).unwrap_or(0);
    let base_ms = (hours.max(0.0) * 3_600_000.0) as u64;
    let factor = 1u64
        .checked_shl(u32::try_from(k.min(32)).unwrap_or(32))
        .unwrap_or(u64::MAX);
    let cap = max_days.saturating_mul(86_400_000).max(base_ms);
    Some(at.saturating_add(base_ms.saturating_mul(factor).min(cap)))
}

/// Soft state the loop, the Admin API and the `standby` loop share.
#[derive(Debug, Default)]
pub struct DecisionState {
    /// Per tenant: the qualifying target and since when (ms).
    candidates: Mutex<HashMap<TenantId, (DcId, u64)>>,
    /// Per tenant: the last evaluation and what was done.
    last: Mutex<HashMap<TenantId, Value>>,
    /// Per tenant: a DC a move is about to go to, until when (ms): the
    /// `standby` loop places no standby there meanwhile.
    intents: Mutex<HashMap<TenantId, (DcId, u64)>>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl DecisionState {
    /// The last evaluation of `tenant`, for `sprigctl tenant traffic`.
    #[must_use]
    pub fn last(&self, tenant: TenantId) -> Option<Value> {
        lock(&self.last).get(&tenant).cloned()
    }

    /// Whether a move of `tenant` to `dc` is intended (the `standby` loop
    /// keeps out of it).
    #[must_use]
    pub fn intends(&self, tenant: TenantId, dc: &DcId, now_ms: u64) -> bool {
        lock(&self.intents)
            .get(&tenant)
            .is_some_and(|(d, until)| d == dc && *until > now_ms)
    }
}

/// The loop (module docs).
pub struct MigrationDecider {
    moves: Arc<DcMoves>,
    /// Tenants' sizes (chapter 11's fleet view).
    balancer: Option<Arc<crate::balancer_loop::BalancerLoop>>,
    /// Chapter 52: `migration.mode = approve` holds moves for approval.
    approvals: Option<Arc<crate::approvals::ControllerApprovals>>,
}

impl std::fmt::Debug for MigrationDecider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MigrationDecider")
            .field("dc", self.moves.dc())
            .finish_non_exhaustive()
    }
}

fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

impl MigrationDecider {
    /// The loop over `moves`; `balancer` gives tenants' sizes.
    #[must_use]
    pub fn new(
        moves: Arc<DcMoves>,
        balancer: Option<Arc<crate::balancer_loop::BalancerLoop>>,
    ) -> Self {
        Self {
            moves,
            balancer,
            approvals: None,
        }
    }

    /// Chapter 52: hold moves for approval under `migration.mode = approve`
    /// (and record advice under `recommend`).
    #[must_use]
    pub fn with_approvals(mut self, a: Arc<crate::approvals::ControllerApprovals>) -> Self {
        self.approvals = Some(a);
        self
    }

    /// Chapter 52's gate for a qualifying move: `None` to go ahead
    /// (`auto`, or approved), `Some(why)` to hold.
    async fn approval_gate(
        &self,
        tenant: TenantId,
        name: &str,
        mode: &str,
        ev: &Evaluation,
        best: &DcId,
    ) -> Option<(Option<sprig_approvals::Ticket>, String)> {
        use sprig_approvals::{Action, Kind, Proposal, Verdict};
        let a = self.approvals.as_ref()?;
        if !matches!(mode, "approve" | "recommend") {
            return None;
        }
        let policy = a.policy(Kind::DcMove, Some(tenant), None, None, None).await;
        let improvement = ev
            .latency_ms
            .get(ev.home.as_str())
            .filter(|h| **h > 0.0)
            .map_or(0.0, |h| ev.gain_ms / h * 100.0);
        let p = Proposal {
            kind: Kind::DcMove,
            subject: a.subject(tenant).await,
            target: format!("t{tenant}/home"),
            action: Action {
                text: format!("sprigctl tenant move {name} --to {best}"),
                machine: serde_json::json!({ "to": best.to_string() }),
            },
            evidence: serde_json::json!({
                "decision": ev.decision,
                "home": ev.home.to_string(),
                "to": best.to_string(),
                "weighted_total": ev.weighted_total,
                "latency_ms": ev.latency_ms,
                "gain_ms": ev.gain_ms,
                "improvement_pct": improvement,
                "scores": ev.scores,
            }),
            score: if ev.decision == "relief" {
                100.0
            } else {
                improvement
            },
            summary: if ev.decision == "relief" {
                format!("move {name} from {} to {best}: capacity relief", ev.home)
            } else {
                format!(
                    "move {name} from {} to {best}: {:.0}% lower traffic-weighted latency ({:.1} ms)",
                    ev.home, improvement, ev.gain_ms
                )
            },
            // Relief moves free a DC running out of room (chapter 52,
            // "Safety rules").
            emergency: ev.decision == "relief",
        };
        match a.gate(&p, &policy).await {
            Verdict::Proceed(t) => Some((Some(t), String::new())),
            Verdict::Wait(id) => Some((None, format!("held for approval as {id}"))),
            Verdict::Advice(id) => Some((None, format!("recommended as {id}"))),
            Verdict::Suppressed(id) => Some((None, format!("ignored ({id})"))),
            Verdict::Off => Some((None, "approvals unavailable; holding".into())),
        }
    }

    fn state(&self) -> &DecisionState {
        &self.moves.decisions
    }

    fn period(&self) -> Duration {
        Duration::from_secs(self.moves.setting(&EVALUATE_SECONDS).max(1))
    }

    fn record(&self, tenant: TenantId, ev: Option<&Evaluation>, action: &str, why: &str) {
        let v = json!({
            "at_ms": now_ms(), "action": action, "why": why,
            "evaluation": ev,
            "candidate": lock(&self.state().candidates).get(&tenant)
                .map(|(d, since)| json!({"dc": d, "since_ms": since})),
        });
        lock(&self.state().last).insert(tenant, v);
    }

    fn clear_candidate(&self, tenant: TenantId) {
        lock(&self.state().candidates).remove(&tenant);
    }

    /// One tenant (module docs).
    ///
    /// # Errors
    /// The stores' errors.
    #[allow(clippy::too_many_lines)] // the chapter's nine steps, in order
    pub async fn reconcile_tenant(&self, tenant: TenantId) -> Result<Outcome> {
        let period = self.period();
        let m = &self.moves;
        let Some(e) = home::read_home(&*m.global(), tenant).await? else {
            return Ok(Outcome::Done);
        };
        if e.home_dc != *m.dc() {
            self.clear_candidate(tenant);
            return Ok(Outcome::Done);
        }
        let name = e
            .raw
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        // 1. Mode, pins, state, cooldown.
        let mode = m.tenant_setting(tenant, &MODE).await;
        if mode == "off" {
            self.clear_candidate(tenant);
            self.record(tenant, None, "none", "migration.mode = off");
            return Ok(Outcome::RequeueAfter(period));
        }
        if e.state != "active" {
            self.clear_candidate(tenant);
            self.record(tenant, None, "none", &format!("the tenant is {}", e.state));
            return Ok(Outcome::RequeueAfter(period));
        }
        if let Some(p) = &e.pinned_dc {
            self.clear_candidate(tenant);
            self.record(tenant, None, "none", &format!("pinned to {p}"));
            return Ok(Outcome::RequeueAfter(period));
        }
        if m.chapter11_pinned(tenant).await? {
            self.clear_candidate(tenant);
            self.record(tenant, None, "none", "pinned (sprigctl tenant pin)");
            return Ok(Outcome::RequeueAfter(period));
        }
        let now = now_ms();
        let hours = m.tenant_setting(tenant, &COOLDOWN_HOURS).await;
        let max_days = m.tenant_setting(tenant, &MAX_COOLDOWN_DAYS).await;
        if let Some(until) = cooldown_until(e.last_move.as_ref(), hours, max_days)
            && until > now
        {
            self.clear_candidate(tenant);
            self.record(
                tenant,
                None,
                "none",
                &format!("cooldown for {} s more", (until - now) / 1000),
            );
            return Ok(Outcome::RequeueAfter(period));
        }
        // 2-6. The evaluation.
        let (ev, active) = self.evaluate_tenant(tenant, &name, &e, now).await?;
        if let Some(r) = &ev.remote_best {
            tracing::warn!(tenant = %name, best = %r, home = %e.home_dc, "ResidencyRemoteTraffic: the best DC for the tenant's traffic is outside its residency");
            metrics::counter!("sprig_controller_residency_remote_traffic_total").increment(1);
        }
        let _ = active;
        if !ev.qualifies() {
            self.clear_candidate(tenant);
            self.record(tenant, Some(&ev), "stay", &ev.reason.clone());
            return Ok(Outcome::RequeueAfter(period));
        }
        let Some(best) = ev.best.clone() else {
            return Ok(Outcome::RequeueAfter(period));
        };
        // 7. Hysteresis.
        let since = {
            let mut c = lock(&self.state().candidates);
            match c.get(&tenant) {
                Some((d, since)) if *d == best => *since,
                _ => {
                    c.insert(tenant, (best.clone(), now));
                    now
                }
            }
        };
        let sustain = m.tenant_setting(tenant, &SUSTAIN_MINUTES).await;
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let sustain_ms = (sustain.max(0.0) * 60_000.0) as u64;
        if now.saturating_sub(since) < sustain_ms {
            self.record(
                tenant,
                Some(&ev),
                "wait",
                &format!(
                    "{best} has qualified for {} s of {} s",
                    (now - since) / 1000,
                    sustain_ms / 1000
                ),
            );
            return Ok(Outcome::RequeueAfter(period));
        }
        // 8. Guards.
        let limit_gb = m.tenant_setting(tenant, &AUTO_SIZE_LIMIT_GB).await;
        if let Some(b) = &self.balancer
            && let Some(bytes) = b.tenant_storage_bytes(tenant).await
        {
            #[allow(clippy::cast_precision_loss)]
            let limit = limit_gb as f64 * 1024.0 * 1024.0 * 1024.0;
            if bytes > limit {
                self.record(
                    tenant,
                    Some(&ev),
                    "needs_approval",
                    &format!(
                        "{:.1} GB is over migration.auto_size_limit_gb ({limit_gb}); \
                         sprigctl tenant move {name} --to {best}",
                        bytes / 1024.0 / 1024.0 / 1024.0
                    ),
                );
                metrics::gauge!("sprig_controller_migration_blocked", "reason" => "size").set(1.0);
                return Ok(Outcome::RequeueAfter(period));
            }
        }
        let max = m.setting(&MAX_CONCURRENT_PER_DC);
        let (here, there) = m.moves_touching(&e.home_dc, &best).await?;
        if here >= max || there >= max {
            self.record(
                tenant,
                Some(&ev),
                "wait",
                &format!(
                    "migration.max_concurrent_per_dc ({max}): {here} moves touch {}, {there} touch {best}",
                    e.home_dc
                ),
            );
            metrics::gauge!("sprig_controller_migration_blocked", "reason" => "concurrency")
                .set(1.0);
            return Ok(Outcome::RequeueAfter(period));
        }
        let mut ticket = None;
        if let Some((t, why)) = self.approval_gate(tenant, &name, &mode, &ev, &best).await {
            if t.is_none() {
                self.record(tenant, Some(&ev), "awaiting_approval", &why);
                return Ok(Outcome::RequeueAfter(period));
            }
            ticket = t;
        }
        if mode != "auto" && ticket.is_none() {
            self.record(
                tenant,
                Some(&ev),
                "recommend",
                &format!("migration.mode = {mode}: sprigctl tenant move {name} --to {best}"),
            );
            return Ok(Outcome::RequeueAfter(period));
        }
        // The tenant's job standby in the target holds its lock there
        // (RF11): drop it first, and keep the standby loop out of it.
        lock(&self.state().intents).insert(tenant, (best.clone(), now + 10 * 60 * 1000));
        if crate::standby::job_standbys(&e.raw).contains_key(&best) {
            crate::standby::drop_job_standby(
                m.store(),
                m.global(),
                m.clock(),
                tenant,
                &best,
                "the tenant moves there",
            )
            .await?;
            self.record(
                tenant,
                Some(&ev),
                "dropping_standby",
                &format!("the job standby in {best} is dropped before the move"),
            );
            return Ok(Outcome::RequeueAfter(period.min(Duration::from_secs(5))));
        }
        // 9. Move.
        let cause = if ev.decision == "relief" {
            "relief"
        } else {
            "traffic"
        };
        let req = MoveTenantRequest {
            to: best.to_string(),
            ..MoveTenantRequest::default()
        };
        match m.start_caused(tenant, &name, &req, cause).await {
            Ok(v) => {
                self.clear_candidate(tenant);
                tracing::info!(tenant = %name, from = %e.home_dc, to = %best, cause,
                    weighted_total = ev.weighted_total, gain_ms = ev.gain_ms,
                    latency = ?ev.latency_ms, "automatic DC move started");
                metrics::counter!("sprig_controller_migrations_total", "cause" => cause, "result" => "auto_started")
                    .increment(1);
                metrics::gauge!("sprig_controller_migration_blocked", "reason" => "size").set(0.0);
                metrics::gauge!("sprig_controller_migration_blocked", "reason" => "concurrency")
                    .set(0.0);
                self.record(
                    tenant,
                    Some(&ev),
                    "moved",
                    &format!(
                        "migration {} to {best}",
                        v.migration
                            .as_ref()
                            .and_then(|x| x.get("id"))
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                    ),
                );
            }
            Err(err) => {
                // Refused (39550), or the target still holds the dropped
                // standby's lock: the next pass tries again.
                let why = format!("the move didn't start: {err}");
                if let (Some(a), Some(t)) = (&self.approvals, &ticket) {
                    a.finished(t, false, &why).await;
                }
                tracing::warn!(tenant = %name, to = %best, error = %err, refused = (err.code() == Some(SprigCode::TenantMoveRefused)), "automatic DC move not started");
                self.record(tenant, Some(&ev), "refused", &why);
            }
        }
        Ok(Outcome::RequeueAfter(period))
    }

    async fn evaluate_tenant(
        &self,
        tenant: TenantId,
        name: &str,
        e: &HomeEntry,
        now: u64,
    ) -> Result<(Evaluation, Vec<DcId>)> {
        let m = &self.moves;
        let global = m.global();
        let dcs = crate::dc::list(&*global).await?;
        let loads = least_busy::read_all(&*global).await?;
        let stability_ms = m
            .setting(&RECOVERY_STABILITY_MINUTES)
            .saturating_mul(60_000);
        let active: Vec<DcId> = dcs
            .iter()
            .filter(|d| d.state == "active" && d.kind != "witness")
            .map(|d| d.dc_id.clone())
            .collect();
        let allowed = e.allowed_dcs();
        let cands: Vec<DcId> = active
            .iter()
            .filter(|d| {
                **d == e.home_dc
                    || (allowed.as_ref().is_none_or(|a| a.contains(*d))
                        && loads.get(*d).is_some_and(|l| {
                            l.stable_since_ms > 0
                                && now.saturating_sub(l.stable_since_ms) >= stability_ms
                        }))
            })
            .cloned()
            .collect();
        let view = m.traffic.view(
            tenant,
            name,
            &e.home_dc,
            crate::traffic::unix_minute(),
            m.setting(&WINDOW_MINUTES),
            m.setting(&WRITE_WEIGHT),
            0,
            0,
        );
        let known: BTreeSet<String> = dcs.iter().map(|d| d.dc_id.to_string()).collect();
        let clients: Vec<(String, u64)> = view
            .clients
            .iter()
            .filter(|c| known.contains(&c.client_dc))
            .map(|c| (c.client_dc.clone(), c.weighted))
            .collect();
        let matrix = crate::latency::Matrix::read(&*global)
            .await
            .unwrap_or_default();
        let rtt = |a: &str, b: &str| matrix.rtt(a, b);
        let p = Params {
            min_signal: m.setting(&MIN_SIGNAL_REQUESTS),
            min_pct: m.tenant_setting(tenant, &MIN_IMPROVEMENT_PCT).await,
            min_ms: m.tenant_setting(tenant, &MIN_IMPROVEMENT_MS).await,
            relief_spare_pct: m.setting(&least_busy::DC_RELIEF_SPARE_PCT),
            margin: m.setting_f64(&least_busy::LEAST_BUSY_MARGIN),
            weights: LeastBusyWeights::parse(&m.setting_str(&LEAST_BUSY_WEIGHTS)),
        };
        Ok((
            evaluate(&e.home_dc, &clients, &cands, &active, &rtt, &loads, now, &p),
            active,
        ))
    }
}

#[async_trait::async_trait]
impl Reconciler for MigrationDecider {
    type Key = TenantId;

    fn name(&self) -> &'static str {
        "dc_migrate"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        vec![WatchSpec::global("tenants/")]
    }

    fn keys_for(&self, ev: &MetaEvent) -> Vec<TenantId> {
        crate::tree::Directory::new(self.moves.global())
            .tenant_of(&ev.key)
            .into_iter()
            .collect()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<TenantId>> {
        let dir = crate::tree::Directory::new(self.moves.global());
        let keys: BTreeSet<TenantId> = dir
            .list()
            .await?
            .iter()
            .map(crate::tree::StoredEntry::body)
            .filter(|e| e.home_dc == *self.moves.dc())
            .map(|e| e.tenant_id)
            .collect();
        Ok(keys.into_iter().collect())
    }

    async fn reconcile(&self, _cx: &LoopCx, key: &TenantId) -> Result<Outcome> {
        self.reconcile_tenant(*key).await
    }
}

#[cfg(test)]
mod tests;
