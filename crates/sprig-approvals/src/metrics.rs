//! The approvals metrics (`sprig_approvals_*`). Names are literals in the
//! macros so `deploy/dashboards/gen.py --check` finds them.

use std::collections::BTreeMap;

use crate::machine::EventKind;
use crate::model::{Micros, Recommendation};

/// Counts one event.
pub fn event(kind: EventKind, r: &Recommendation) {
    let k = r.kind.as_str();
    match kind {
        EventKind::New | EventKind::Reraised => {
            metrics::counter!("sprig_approvals_raised_total", "kind" => k, "level" => r.level.as_str())
                .increment(1);
        }
        EventKind::Applied => {
            metrics::counter!("sprig_approvals_applied_total", "kind" => k, "result" => "ok")
                .increment(1);
        }
        EventKind::Failed => {
            metrics::counter!("sprig_approvals_applied_total", "kind" => k, "result" => "failed")
                .increment(1);
        }
        _ => {}
    }
    let action = match kind {
        EventKind::Approved => "approve",
        EventKind::Ignored => "ignore",
        EventKind::Snoozed => "snooze",
        EventKind::Reopened => "reopen",
        EventKind::AutoApproved => "auto_apply",
        EventKind::Escalated => "escalate",
        EventKind::Expired => "expire",
        EventKind::Emergency => "emergency",
        EventKind::Commented => "comment",
        _ => return,
    };
    metrics::counter!("sprig_approvals_actions_total", "kind" => k, "action" => action)
        .increment(1);
}

/// Records the time from raise to a person's decision.
#[allow(clippy::cast_precision_loss)]
pub fn decided(r: &Recommendation, now: Micros) {
    let secs = (now - r.created_at_us).max(0) as f64 / 1e6;
    metrics::histogram!("sprig_approvals_decision_seconds", "kind" => r.kind.as_str()).record(secs);
}

/// Counts one notification.
pub fn notified(channel: &'static str, result: &'static str) {
    metrics::counter!("sprig_approvals_notifications_total", "channel" => channel, "result" => result)
        .increment(1);
}

/// Sets the waiting gauges from every waiting item the loop saw; kinds and
/// levels with none are set to zero.
#[allow(clippy::cast_precision_loss)]
pub fn waiting(items: &[Recommendation], now: Micros) {
    let mut open: BTreeMap<(&'static str, &'static str), (f64, f64)> = BTreeMap::new();
    for k in crate::model::Kind::ALL {
        for l in crate::model::Level::ALL {
            open.insert((k.as_str(), l.as_str()), (0.0, 0.0));
        }
    }
    for r in items {
        if !matches!(
            r.state,
            crate::model::State::Open | crate::model::State::Approved
        ) {
            continue;
        }
        let e = open.entry((r.kind.as_str(), r.level.as_str())).or_default();
        e.0 += 1.0;
        e.1 = e.1.max((now - r.created_at_us).max(0) as f64 / 1e6);
    }
    for ((k, l), (n, age)) in open {
        metrics::gauge!("sprig_approvals_open", "kind" => k, "level" => l).set(n);
        metrics::gauge!("sprig_approvals_oldest_open_seconds", "kind" => k, "level" => l).set(age);
    }
}
