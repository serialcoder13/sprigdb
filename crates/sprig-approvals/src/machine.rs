//! The state machine, as pure functions over [`Recommendation`]s:
//! the gate, ignore, snooze, escalation and auto-apply. Every store applies these under its own atomicity, so the
//! catalog, etcd and memory stores behave the same.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};

use crate::model::{
    Comment, Ignore, IgnoreFor, Kind, MAX_COMMENT, MAX_COMMENTS, MAX_TEXT, Micros, Mode,
    NotifyLevel, Proposal, RecId, Recommendation, State, Window, clip, micros,
};
use crate::policy::Policy;

/// Longest approval window.
pub const MAX_WINDOW: Duration = Duration::from_secs(30 * 86_400);

const DAY_US: Micros = 86_400 * 1_000_000;

/// What the gate tells a component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Act now; report through the ticket.
    Proceed(Ticket),
    /// Held for approval.
    Wait(RecId),
    /// Recorded as advice; don't act.
    Advice(RecId),
    /// An ignore covers it; don't act.
    Suppressed(RecId),
    /// Mode off: don't act.
    Off,
}

impl Verdict {
    /// Whether the component may act.
    #[must_use]
    pub const fn proceed(&self) -> bool {
        matches!(self, Verdict::Proceed(_))
    }
}

/// A component's handle on an action it was allowed to start.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ticket {
    /// The item, `None` when the mode is `on` (nothing is tracked).
    pub id: Option<RecId>,
    /// The item's kind.
    pub kind: Kind,
}

/// What [`raise`] decided, for a store to write.
#[derive(Debug, Clone, PartialEq)]
pub struct RaisePlan {
    /// Rows to write: the new or refreshed item, and superseded ones.
    /// A new item has id number 0; the store assigns it.
    pub writes: Vec<Recommendation>,
    /// Index into `writes` of the item the verdict is about, if any.
    pub main: Option<usize>,
    /// The verdict, with the main item's id filled in by the store when new.
    pub verdict: VerdictPlan,
    /// What happened, for events and metrics.
    pub event: Option<EventKind>,
}

/// The verdict before ids are known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerdictPlan {
    /// Act now.
    Proceed,
    /// Held.
    Wait,
    /// Advice.
    Advice,
    /// Suppressed by the ignore on `writes`' ... (the ignored item, unchanged).
    Suppressed,
}

/// Events, as webhooks, streams and emails name them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    /// A new item.
    New,
    /// A new item over an ignore whose evidence grew past the factor.
    Reraised,
    /// Escalated to the next level.
    Escalated,
    /// Approved by a person.
    Approved,
    /// Approved by the auto-apply timer.
    AutoApproved,
    /// Ignored.
    Ignored,
    /// Snoozed.
    Snoozed,
    /// Reopened by a person, or after a snooze.
    Reopened,
    /// The action started.
    Applying,
    /// The action finished.
    Applied,
    /// The action failed.
    Failed,
    /// The condition went away, or re-validation failed.
    Expired,
    /// Newer advice replaced it.
    Superseded,
    /// Applied at once as an emergency.
    Emergency,
    /// Somebody commented.
    Commented,
}

impl EventKind {
    /// `recommendation.<name>`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            EventKind::New => "recommendation.new",
            EventKind::Reraised => "recommendation.reraised",
            EventKind::Escalated => "recommendation.escalated",
            EventKind::Approved => "recommendation.approved",
            EventKind::AutoApproved => "recommendation.auto_approved",
            EventKind::Ignored => "recommendation.ignored",
            EventKind::Snoozed => "recommendation.snoozed",
            EventKind::Reopened => "recommendation.reopened",
            EventKind::Applying => "recommendation.applying",
            EventKind::Applied => "recommendation.applied",
            EventKind::Failed => "recommendation.failed",
            EventKind::Expired => "recommendation.expired",
            EventKind::Superseded => "recommendation.superseded",
            EventKind::Emergency => "recommendation.emergency",
            EventKind::Commented => "recommendation.commented",
        }
    }

    /// The decision-log kind (`approval.<x>`).
    #[must_use]
    pub const fn decision_kind(self) -> &'static str {
        match self {
            EventKind::New | EventKind::Reraised => "approval.raise",
            EventKind::Escalated => "approval.escalate",
            EventKind::Approved => "approval.approve",
            EventKind::AutoApproved => "approval.auto_apply",
            EventKind::Ignored => "approval.ignore",
            EventKind::Snoozed => "approval.snooze",
            EventKind::Reopened => "approval.reopen",
            EventKind::Applying => "approval.apply",
            EventKind::Applied => "approval.applied",
            EventKind::Failed => "approval.failed",
            EventKind::Expired => "approval.expire",
            EventKind::Superseded => "approval.supersede",
            EventKind::Emergency => "approval.emergency",
            EventKind::Commented => "approval.comment",
        }
    }

    /// Whether people should hear about it at once (the rest go to digests
    /// and the inbox).
    #[must_use]
    pub const fn notify_now(self) -> bool {
        matches!(
            self,
            EventKind::New
                | EventKind::Reraised
                | EventKind::Escalated
                | EventKind::Applied
                | EventKind::Failed
                | EventKind::Emergency
                | EventKind::AutoApproved
        )
    }
}

/// A new item from `p` under `policy`, not yet stored (id number 0).
#[must_use]
pub fn new_item(
    p: &Proposal,
    policy: &Policy,
    id: RecId,
    ancestry: &[TenantId],
    now: Micros,
) -> Recommendation {
    let advisory = policy.mode == Mode::Recommend;
    let esc = if advisory {
        None
    } else {
        policy.escalate_after
    };
    let auto = if advisory {
        None
    } else {
        policy.auto_apply_after
    };
    Recommendation {
        id,
        kind: p.kind,
        level: policy.level,
        advisory,
        subject: p.subject.clone(),
        target: clip(&p.target, 255),
        fingerprint: p.fingerprint(),
        state: State::Open,
        summary: clip(&p.summary, MAX_TEXT),
        action: p.action.clone(),
        evidence: p.evidence.clone(),
        score: p.score,
        mode_source: policy.mode_source.clone(),
        notify_level: NotifyLevel::of(policy.level),
        ancestry: ancestry.to_vec(),
        escalations: 0,
        escalate_after_s: esc.map(|d| d.as_secs()),
        auto_apply_after_s: auto.map(|d| d.as_secs()),
        escalate_at_us: esc.map(|d| now + micros(d)),
        auto_apply_at_us: auto.map(|d| now + micros(d)),
        window: None,
        snooze_until_us: None,
        ignore: None,
        emergency: false,
        decided_by: None,
        decided_at_us: None,
        reason: None,
        result: None,
        refreshes: 0,
        created_at_us: now,
        refreshed_at_us: now,
        updated_at_us: now,
        version: 1,
        history: Vec::new(),
        comments: Vec::new(),
    }
}

/// Whether an ignore still suppresses a proposal with `score` at `now`.
#[must_use]
pub fn ignore_covers(rec: &Recommendation, score: f64, now: Micros) -> bool {
    if rec.state != State::Ignored {
        return false;
    }
    let Some(ig) = rec.ignore else { return false };
    let in_time = ig.until_us.is_none_or(|u| now < u);
    in_time && score < ig.score * ig.factor
}

/// The gate's decision for proposal `p`.
///
/// `same_target` holds the live items with the same kind and target (any
/// fingerprint); `ignored` the newest ignored item with `p`'s fingerprint.
/// `new_id` is the id a new item gets.
#[must_use]
#[allow(clippy::too_many_lines)] // one branch per case of the chapter's pseudocode
pub fn raise(
    p: &Proposal,
    policy: &Policy,
    same_target: &[Recommendation],
    ignored: Option<&Recommendation>,
    ancestry: &[TenantId],
    new_id: RecId,
    now: Micros,
) -> RaisePlan {
    let fp = p.fingerprint();
    let mut writes = Vec::new();
    let live = same_target
        .iter()
        .find(|r| r.fingerprint == fp && r.state.is_live());
    let emergency = p.emergency && policy.allow_emergency;
    if let Some(r) = live {
        let mut r = r.clone();
        r.evidence = p.evidence.clone();
        r.score = p.score;
        r.summary = clip(&p.summary, MAX_TEXT);
        r.refreshed_at_us = now;
        r.refreshes += 1;
        r.version += 1;
        // A mode that moved to recommend makes it advice; to approve, held.
        if policy.mode == Mode::Recommend && !r.advisory && r.state == State::Open {
            r.advisory = true;
            r.escalate_at_us = None;
            r.auto_apply_at_us = None;
        }
        let (verdict, event) = match r.state {
            State::Approved => match r.window {
                Some(w) if now < w.start_us => (VerdictPlan::Wait, None),
                Some(w) if now > w.end_us => {
                    r.transition(State::Expired, "approvals", "approval window passed", now);
                    r.result = Some("approval window passed before the change could run".into());
                    (VerdictPlan::Wait, Some(EventKind::Expired))
                }
                _ => {
                    r.transition(State::Applying, "gate", "approved; proposed again", now);
                    (VerdictPlan::Proceed, Some(EventKind::Applying))
                }
            },
            // Re-entry: the component retries a start it began (a standby
            // to drop first, a refused start); its own start is idempotent.
            State::Applying => (VerdictPlan::Proceed, None),
            State::Open | State::Snoozed if emergency => {
                r.emergency = true;
                r.transition(State::Applying, "gate", "emergency", now);
                (VerdictPlan::Proceed, Some(EventKind::Emergency))
            }
            _ if r.advisory => (VerdictPlan::Advice, None),
            _ => (VerdictPlan::Wait, None),
        };
        writes.push(r);
        return RaisePlan {
            writes,
            main: Some(0),
            verdict,
            event,
        };
    }
    if let Some(ig) = ignored
        && ignore_covers(ig, p.score, now)
    {
        return RaisePlan {
            writes,
            main: None,
            verdict: VerdictPlan::Suppressed,
            event: None,
        };
    }
    for old in same_target
        .iter()
        .filter(|r| r.fingerprint != fp && r.state.is_live())
    {
        if old.state == State::Applying {
            // Never supersede a running action; the new advice waits.
            return RaisePlan {
                writes,
                main: None,
                verdict: if policy.mode == Mode::Recommend {
                    VerdictPlan::Advice
                } else {
                    VerdictPlan::Wait
                },
                event: None,
            };
        }
        let mut o = old.clone();
        o.transition(
            State::Superseded,
            "gate",
            "newer advice for the same target",
            now,
        );
        o.version += 1;
        writes.push(o);
    }
    let mut r = new_item(p, policy, new_id, ancestry, now);
    let mut event = if ignored.is_some_and(|i| i.state == State::Ignored) {
        r.evidence = with_reraise(&r.evidence, ignored.map(|i| i.id.to_string()));
        EventKind::Reraised
    } else {
        EventKind::New
    };
    let verdict = if emergency {
        r.emergency = true;
        r.history.push(crate::model::HistoryEntry {
            at_us: now,
            from: State::Open,
            to: State::Applying,
            by: "gate".into(),
            note: "emergency".into(),
        });
        r.state = State::Applying;
        event = EventKind::Emergency;
        VerdictPlan::Proceed
    } else if r.advisory {
        VerdictPlan::Advice
    } else {
        VerdictPlan::Wait
    };
    writes.insert(0, r);
    RaisePlan {
        writes,
        main: Some(0),
        verdict,
        event: Some(event),
    }
}

fn with_reraise(ev: &serde_json::Value, over: Option<String>) -> serde_json::Value {
    let mut ev = ev.clone();
    if let (serde_json::Value::Object(m), Some(id)) = (&mut ev, over) {
        m.insert("reraised_over".into(), serde_json::Value::String(id));
    }
    ev
}

/// A person's (or a timer's) action.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Act {
    /// Approve, optionally inside a window.
    Approve {
        /// Why.
        #[serde(default)]
        reason: String,
        /// Apply only inside this window.
        #[serde(default)]
        window: Option<Window>,
    },
    /// Ignore.
    Ignore {
        /// Why.
        #[serde(default)]
        reason: String,
        /// For N days.
        #[serde(default)]
        for_days: Option<u32>,
        /// Forever.
        #[serde(default)]
        forever: bool,
    },
    /// Snooze for some hours.
    Snooze {
        /// Hours, 1 to 720.
        hours: u32,
        /// Why.
        #[serde(default)]
        reason: String,
    },
    /// Back to open.
    Reopen {
        /// Why.
        #[serde(default)]
        reason: String,
    },
    /// A comment.
    Comment {
        /// The text.
        text: String,
    },
}

impl Act {
    /// The action's name.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Act::Approve { .. } => "approve",
            Act::Ignore { .. } => "ignore",
            Act::Snooze { .. } => "snooze",
            Act::Reopen { .. } => "reopen",
            Act::Comment { .. } => "comment",
        }
    }
}

fn state_err(r: &Recommendation, what: &str) -> SprigError {
    SprigError::new(
        SprigCode::RecommendationState,
        format!("recommendation {} is {}; it can't be {what}", r.id, r.state),
    )
}

/// Applies `act` by `by` to `r` at `now`; returns the event. The store
/// writes `r` if this succeeds.
///
/// # Errors
/// 39973 when the state doesn't allow it, 39976 for a bad window, 39977
/// for bad numbers.
#[allow(clippy::too_many_lines)] // one arm per action
pub fn act(
    r: &mut Recommendation,
    act: &Act,
    by: &str,
    ignore_once_days: u32,
    reraise_factor: f64,
    now: Micros,
) -> Result<EventKind> {
    let ev = match act {
        Act::Approve { reason, window } => {
            if !matches!(r.state, State::Open | State::Snoozed) {
                return Err(state_err(r, "approved"));
            }
            if let Some(w) = window {
                if w.end_us <= w.start_us || w.end_us <= now {
                    return Err(SprigError::new(
                        SprigCode::ApprovalWindowInvalid,
                        "the window must end after it starts, and in the future",
                    ));
                }
                if w.end_us - w.start_us > micros(MAX_WINDOW) {
                    return Err(SprigError::new(
                        SprigCode::ApprovalWindowInvalid,
                        "a window is at most 30 days long",
                    ));
                }
            }
            r.window = *window;
            r.decided_by = Some(clip(by, 128));
            r.decided_at_us = Some(now);
            r.reason = Some(clip(reason, MAX_TEXT));
            r.auto_apply_at_us = None;
            r.escalate_at_us = None;
            r.snooze_until_us = None;
            r.transition(State::Approved, by, reason, now);
            EventKind::Approved
        }
        Act::Ignore {
            reason,
            for_days,
            forever,
        } => {
            if !matches!(r.state, State::Open | State::Snoozed | State::Approved) {
                return Err(state_err(r, "ignored"));
            }
            let (how, until) = match (forever, for_days) {
                (true, _) => (IgnoreFor::Forever, None),
                (false, Some(d)) => {
                    if *d == 0 || *d > 3650 {
                        return Err(SprigError::new(
                            SprigCode::ApprovalPolicyInvalid,
                            "ignore for 1 to 3650 days",
                        ));
                    }
                    (IgnoreFor::Days, Some(now + i64::from(*d) * DAY_US))
                }
                (false, None) => (
                    IgnoreFor::Once,
                    Some(now + i64::from(ignore_once_days) * DAY_US),
                ),
            };
            r.ignore = Some(Ignore {
                how,
                until_us: until,
                score: r.score,
                factor: reraise_factor,
            });
            r.decided_by = Some(clip(by, 128));
            r.decided_at_us = Some(now);
            r.reason = Some(clip(reason, MAX_TEXT));
            r.auto_apply_at_us = None;
            r.escalate_at_us = None;
            r.transition(State::Ignored, by, reason, now);
            EventKind::Ignored
        }
        Act::Snooze { hours, reason } => {
            if r.state != State::Open {
                return Err(state_err(r, "snoozed"));
            }
            if *hours == 0 || *hours > 720 {
                return Err(SprigError::new(
                    SprigCode::ApprovalPolicyInvalid,
                    "snooze for 1 to 720 hours",
                ));
            }
            r.snooze_until_us = Some(now + i64::from(*hours) * 3_600_000_000);
            r.escalate_at_us = None;
            r.auto_apply_at_us = None;
            r.transition(State::Snoozed, by, reason, now);
            EventKind::Snoozed
        }
        Act::Reopen { reason } => {
            if !matches!(
                r.state,
                State::Ignored | State::Expired | State::Failed | State::Snoozed
            ) {
                return Err(state_err(r, "reopened"));
            }
            reopen(r, by, reason, now);
            EventKind::Reopened
        }
        Act::Comment { text } => {
            if text.trim().is_empty() {
                return Err(SprigError::new(
                    SprigCode::ApprovalPolicyInvalid,
                    "a comment needs text",
                ));
            }
            r.comments.push(Comment {
                at_us: now,
                author: clip(by, 128),
                body: clip(text, MAX_COMMENT),
            });
            if r.comments.len() > MAX_COMMENTS {
                r.comments.remove(0);
            }
            r.updated_at_us = now;
            EventKind::Commented
        }
    };
    r.version += 1;
    Ok(ev)
}

/// Back to open with fresh timers.
fn reopen(r: &mut Recommendation, by: &str, note: &str, now: Micros) {
    r.snooze_until_us = None;
    r.window = None;
    r.escalate_at_us = r.escalate_after_s.map(|s| now + secs(s));
    r.auto_apply_at_us = r.auto_apply_after_s.map(|s| now + secs(s));
    r.refreshed_at_us = now;
    r.transition(State::Open, by, note, now);
}

fn secs(s: u64) -> Micros {
    i64::try_from(s).unwrap_or(i64::MAX / 2_000_000) * 1_000_000
}

/// What a timer did to an item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickChange {
    /// Escalated to this level.
    Escalated(NotifyLevel),
    /// Auto-approved.
    AutoApproved,
    /// Snooze over.
    Unsnoozed,
    /// Not proposed again for the kind's expiry time.
    Expired,
    /// Stuck in applying past the timeout.
    Lost,
}

impl TickChange {
    /// The event.
    #[must_use]
    pub const fn event(self) -> EventKind {
        match self {
            TickChange::Escalated(_) => EventKind::Escalated,
            TickChange::AutoApproved => EventKind::AutoApproved,
            TickChange::Unsnoozed => EventKind::Reopened,
            TickChange::Expired => EventKind::Expired,
            TickChange::Lost => EventKind::Failed,
        }
    }
}

/// Runs the timers of one item at `now` (escalation and
/// auto-apply). Returns the change made, if any; the store writes it.
pub fn tick(r: &mut Recommendation, now: Micros, applying_timeout: Duration) -> Option<TickChange> {
    let change = match r.state {
        State::Snoozed if r.snooze_until_us.is_some_and(|u| u <= now) => {
            reopen(r, "approvals", "snooze over", now);
            TickChange::Unsnoozed
        }
        State::Open | State::Approved
            if now - r.refreshed_at_us > micros(r.kind.expire_after()) =>
        {
            r.result = Some("not proposed again: the condition went away".into());
            r.transition(State::Expired, "approvals", "not proposed again", now);
            TickChange::Expired
        }
        State::Applying if now - r.updated_at_us > micros(applying_timeout) => {
            r.result = Some("lost: no result from the component".into());
            r.transition(State::Failed, "approvals", "applying timed out", now);
            TickChange::Lost
        }
        State::Open if !r.advisory && r.auto_apply_at_us.is_some_and(|t| t <= now) => {
            r.decided_by = Some("auto-apply".into());
            r.decided_at_us = Some(now);
            r.auto_apply_at_us = None;
            r.escalate_at_us = None;
            r.transition(State::Approved, "auto-apply", "auto-apply timer", now);
            TickChange::AutoApproved
        }
        State::Open if !r.advisory && r.escalate_at_us.is_some_and(|t| t <= now) => {
            let Some(next) = r.notify_level.next(&r.ancestry) else {
                // Already at the top: nothing left to escalate to.
                r.escalate_at_us = None;
                r.version += 1;
                return None;
            };
            r.notify_level = next;
            r.escalations += 1;
            r.escalate_at_us = match next.next(&r.ancestry) {
                Some(_) => r.escalate_after_s.map(|s| now + secs(s)),
                None => None,
            };
            r.updated_at_us = now;
            TickChange::Escalated(next)
        }
        _ => return None,
    };
    r.version += 1;
    Some(change)
}

/// The next time [`tick`] could change `r`, for the store's due index.
#[must_use]
pub fn next_due(r: &Recommendation, applying_timeout: Duration) -> Option<Micros> {
    let expiry = r.refreshed_at_us + micros(r.kind.expire_after()) + 1;
    match r.state {
        State::Snoozed => r.snooze_until_us,
        State::Open => [Some(expiry), r.escalate_at_us, r.auto_apply_at_us]
            .into_iter()
            .flatten()
            .min(),
        State::Approved => Some(expiry),
        State::Applying => Some(r.updated_at_us + micros(applying_timeout) + 1),
        _ => None,
    }
}

/// The component reports a finished action.
pub fn finish(r: &mut Recommendation, ok: bool, detail: &str, now: Micros) -> Option<EventKind> {
    if r.state != State::Applying {
        return None;
    }
    r.result = Some(clip(detail, MAX_TEXT));
    let (to, ev) = if ok {
        (State::Applied, EventKind::Applied)
    } else {
        (State::Failed, EventKind::Failed)
    };
    r.transition(to, "component", detail, now);
    r.version += 1;
    Some(ev)
}

/// An applier found the item stale before applying.
pub fn expire(r: &mut Recommendation, why: &str, now: Micros) -> Option<EventKind> {
    if !r.state.is_live() || r.state == State::Applying {
        return None;
    }
    r.result = Some(clip(why, MAX_TEXT));
    r.transition(State::Expired, "approvals", why, now);
    r.version += 1;
    Some(EventKind::Expired)
}

/// The approvals loop starts an approved item through an applier.
pub fn start_applying(r: &mut Recommendation, now: Micros) -> bool {
    if r.state != State::Approved || r.window.is_some_and(|w| now < w.start_us) {
        return false;
    }
    r.transition(State::Applying, "approvals", "applier", now);
    r.version += 1;
    true
}

#[cfg(test)]
mod tests;
