use super::*;
use crate::model::{Action, Level, Subject};
use sprig_common::ids::{DbId, DcId, ObjectId};

const H: Micros = 3_600_000_000;

fn proposal(score: f64, def: &str) -> Proposal {
    Proposal {
        kind: Kind::IndexCreate,
        subject: Subject {
            tenant: Some(TenantId(42)),
            db: Some(DbId(1)),
            object: Some(ObjectId(7)),
            dc: Some(DcId::new("mum1").unwrap()),
            ..Subject::default()
        },
        target: "t42/o7".into(),
        action: Action {
            text: format!("ALTER TABLE shop.orders ADD INDEX ({def})"),
            machine: serde_json::json!({"cols": def, "kind": "btree"}),
        },
        evidence: serde_json::json!({"gain_ms_h": score}),
        score,
        summary: "an index".into(),
        emergency: false,
    }
}

fn policy(mode: Mode) -> Policy {
    let mut p = Policy::fixed(mode, Level::Table);
    p.escalate_after = Some(Duration::from_secs(3600));
    p.auto_apply_after = Some(Duration::from_secs(10 * 3600));
    p
}

fn id(n: u64) -> RecId {
    RecId::Tenant(TenantId(42), n)
}

/// Applies a plan the way a store does, giving new items the next number.
fn store(rows: &mut Vec<Recommendation>, plan: &RaisePlan) {
    for w in &plan.writes {
        if let Some(i) = rows.iter().position(|r| r.id == w.id) {
            rows[i] = w.clone();
        } else {
            rows.push(w.clone());
        }
    }
}

fn raise_into(
    rows: &mut Vec<Recommendation>,
    p: &Proposal,
    pol: &Policy,
    now: Micros,
) -> RaisePlan {
    let same: Vec<_> = rows
        .iter()
        .filter(|r| r.kind == p.kind && r.target == p.target && r.state.is_live())
        .cloned()
        .collect();
    let fp = p.fingerprint();
    let ignored = rows
        .iter()
        .filter(|r| r.fingerprint == fp && r.state == State::Ignored)
        .max_by_key(|r| r.updated_at_us)
        .cloned();
    let plan = raise(
        p,
        pol,
        &same,
        ignored.as_ref(),
        &[TenantId(1), TenantId(9)],
        id(rows.len() as u64 + 1),
        now,
    );
    store(rows, &plan);
    plan
}

#[test]
fn fingerprints_ignore_evidence_but_not_the_action() {
    let a = proposal(10.0, "a");
    let b = proposal(99.0, "a");
    let c = proposal(10.0, "b");
    assert_eq!(a.fingerprint(), b.fingerprint());
    assert_ne!(a.fingerprint(), c.fingerprint());
    assert_eq!(a.fingerprint().len(), 32);
    // Key order in the machine form doesn't matter.
    let mut d = a.clone();
    d.action.machine = serde_json::json!({"kind": "btree", "cols": "a"});
    assert_eq!(a.fingerprint(), d.fingerprint());
}

#[test]
fn the_same_advice_is_one_item_refreshed() {
    let mut rows = Vec::new();
    let pol = policy(Mode::Approve);
    let p1 = raise_into(&mut rows, &proposal(10.0, "a"), &pol, 0);
    assert_eq!(p1.verdict, VerdictPlan::Wait);
    assert_eq!(p1.event, Some(EventKind::New));
    let p2 = raise_into(&mut rows, &proposal(12.0, "a"), &pol, H);
    assert_eq!(p2.verdict, VerdictPlan::Wait);
    assert_eq!(p2.event, None);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].refreshes, 1);
    assert!((rows[0].score - 12.0).abs() < f64::EPSILON);
    // Other advice for the same target supersedes it.
    let p3 = raise_into(&mut rows, &proposal(12.0, "b"), &pol, 2 * H);
    assert_eq!(p3.event, Some(EventKind::New));
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].state, State::Superseded);
    assert_eq!(rows[1].state, State::Open);
}

#[test]
fn approval_releases_on_the_next_proposal_and_windows_hold_it() {
    let mut rows = Vec::new();
    let pol = policy(Mode::Approve);
    raise_into(&mut rows, &proposal(10.0, "a"), &pol, 0);
    let ev = act(
        &mut rows[0],
        &Act::Approve {
            reason: "ok".into(),
            window: Some(Window {
                start_us: 5 * H,
                end_us: 6 * H,
            }),
        },
        "ana",
        1,
        2.0,
        H,
    )
    .unwrap();
    assert_eq!(ev, EventKind::Approved);
    assert_eq!(rows[0].state, State::Approved);
    assert_eq!(rows[0].auto_apply_at_us, None);
    // Before the window: wait. Inside: proceed.
    assert_eq!(
        raise_into(&mut rows, &proposal(10.0, "a"), &pol, 2 * H).verdict,
        VerdictPlan::Wait
    );
    let p = raise_into(&mut rows, &proposal(10.0, "a"), &pol, 5 * H + 1);
    assert_eq!(p.verdict, VerdictPlan::Proceed);
    assert_eq!(rows[0].state, State::Applying);
    // While applying, the component may re-enter its own start.
    assert_eq!(
        raise_into(&mut rows, &proposal(10.0, "a"), &pol, 5 * H + 2).verdict,
        VerdictPlan::Proceed
    );
    assert_eq!(
        finish(&mut rows[0], true, "built", 6 * H),
        Some(EventKind::Applied)
    );
    assert_eq!(rows[0].state, State::Applied);
}

#[test]
fn a_missed_window_expires() {
    let mut rows = Vec::new();
    let pol = policy(Mode::Approve);
    raise_into(&mut rows, &proposal(10.0, "a"), &pol, 0);
    act(
        &mut rows[0],
        &Act::Approve {
            reason: String::new(),
            window: Some(Window {
                start_us: H,
                end_us: 2 * H,
            }),
        },
        "ana",
        1,
        2.0,
        0,
    )
    .unwrap();
    let p = raise_into(&mut rows, &proposal(10.0, "a"), &pol, 3 * H);
    assert_eq!(p.event, Some(EventKind::Expired));
    assert_eq!(rows[0].state, State::Expired);
    // The next proposal opens a new item.
    raise_into(&mut rows, &proposal(10.0, "a"), &pol, 4 * H);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1].state, State::Open);
}

#[test]
fn bad_windows_and_states_are_refused() {
    let mut rows = Vec::new();
    raise_into(&mut rows, &proposal(10.0, "a"), &policy(Mode::Approve), 0);
    let r = &mut rows[0];
    let e = act(
        r,
        &Act::Approve {
            reason: String::new(),
            window: Some(Window {
                start_us: 5,
                end_us: 4,
            }),
        },
        "a",
        1,
        2.0,
        0,
    )
    .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ApprovalWindowInvalid));
    let e = act(
        r,
        &Act::Approve {
            reason: String::new(),
            window: Some(Window {
                start_us: 0,
                end_us: 31 * 24 * H,
            }),
        },
        "a",
        1,
        2.0,
        0,
    )
    .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ApprovalWindowInvalid));
    act(
        r,
        &Act::Approve {
            reason: String::new(),
            window: None,
        },
        "a",
        1,
        2.0,
        0,
    )
    .unwrap();
    let e = act(
        r,
        &Act::Approve {
            reason: String::new(),
            window: None,
        },
        "a",
        1,
        2.0,
        0,
    )
    .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::RecommendationState));
    let e = act(
        r,
        &Act::Snooze {
            hours: 2,
            reason: String::new(),
        },
        "a",
        1,
        2.0,
        0,
    )
    .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::RecommendationState));
}

#[test]
fn ignore_suppresses_until_the_evidence_doubles_or_time_passes() {
    let mut rows = Vec::new();
    let pol = policy(Mode::Approve);
    raise_into(&mut rows, &proposal(10.0, "a"), &pol, 0);
    act(
        &mut rows[0],
        &Act::Ignore {
            reason: "seasonal".into(),
            for_days: Some(30),
            forever: false,
        },
        "ana",
        1,
        2.0,
        H,
    )
    .unwrap();
    assert_eq!(rows[0].state, State::Ignored);
    // Same advice, a bit more evidence: suppressed, nothing written.
    let p = raise_into(&mut rows, &proposal(19.0, "a"), &pol, 2 * H);
    assert_eq!(p.verdict, VerdictPlan::Suppressed);
    assert_eq!(rows.len(), 1);
    // Past the factor: a new item, marked as re-raised.
    let p = raise_into(&mut rows, &proposal(21.0, "a"), &pol, 3 * H);
    assert_eq!(p.event, Some(EventKind::Reraised));
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1].evidence["reraised_over"], "t42.1");
    // After 30 days the old ignore no longer covers anything.
    let mut fresh = vec![rows[0].clone()];
    let p = raise_into(&mut fresh, &proposal(10.0, "a"), &pol, H + 30 * 24 * H + 1);
    assert_eq!(p.event, Some(EventKind::Reraised));
}

#[test]
fn ignore_once_and_forever() {
    let mut rows = Vec::new();
    let pol = policy(Mode::Approve);
    raise_into(&mut rows, &proposal(10.0, "a"), &pol, 0);
    act(
        &mut rows[0],
        &Act::Ignore {
            reason: String::new(),
            for_days: None,
            forever: false,
        },
        "a",
        1,
        2.0,
        0,
    )
    .unwrap();
    assert_eq!(rows[0].ignore.unwrap().how, IgnoreFor::Once);
    assert_eq!(
        raise_into(&mut rows, &proposal(10.0, "a"), &pol, 23 * H).verdict,
        VerdictPlan::Suppressed
    );
    assert_eq!(
        raise_into(&mut rows, &proposal(10.0, "a"), &pol, 25 * H).verdict,
        VerdictPlan::Wait
    );

    let mut rows = Vec::new();
    raise_into(&mut rows, &proposal(10.0, "a"), &pol, 0);
    act(
        &mut rows[0],
        &Act::Ignore {
            reason: String::new(),
            for_days: None,
            forever: true,
        },
        "a",
        1,
        2.0,
        0,
    )
    .unwrap();
    assert_eq!(
        raise_into(&mut rows, &proposal(15.0, "a"), &pol, 9000 * H).verdict,
        VerdictPlan::Suppressed
    );
    // Reopen brings it back with fresh timers.
    act(
        &mut rows[0],
        &Act::Reopen {
            reason: "changed my mind".into(),
        },
        "a",
        1,
        2.0,
        100 * H,
    )
    .unwrap();
    assert_eq!(rows[0].state, State::Open);
    assert_eq!(rows[0].escalate_at_us, Some(101 * H));
}

#[test]
fn escalation_climbs_the_tree_then_auto_apply_fires() {
    let mut rows = Vec::new();
    let pol = policy(Mode::Approve);
    raise_into(&mut rows, &proposal(10.0, "a"), &pol, 0);
    let r = &mut rows[0];
    let t = Duration::from_secs(24 * 3600);
    let mut seen = Vec::new();
    for hour in 1..=6 {
        if let Some(TickChange::Escalated(l)) = tick(r, hour * H, t) {
            seen.push(l.to_string());
        }
        r.refreshed_at_us = hour * H;
    }
    assert_eq!(
        seen,
        ["database", "tenant", "ancestor:9", "ancestor:1", "platform"]
    );
    assert_eq!(r.escalate_at_us, None);
    assert_eq!(tick(r, 10 * H, t), Some(TickChange::AutoApproved));
    assert_eq!(r.state, State::Approved);
    assert_eq!(r.decided_by.as_deref(), Some("auto-apply"));
}

#[test]
fn advice_has_no_timers_and_snooze_reopens() {
    let mut rows = Vec::new();
    let p = raise_into(&mut rows, &proposal(10.0, "a"), &policy(Mode::Recommend), 0);
    assert_eq!(p.verdict, VerdictPlan::Advice);
    assert!(rows[0].advisory);
    assert_eq!(rows[0].auto_apply_at_us, None);
    let t = Duration::from_secs(24 * 3600);
    assert_eq!(tick(&mut rows[0], 20 * H, t), None);

    let mut rows = Vec::new();
    raise_into(&mut rows, &proposal(10.0, "a"), &policy(Mode::Approve), 0);
    act(
        &mut rows[0],
        &Act::Snooze {
            hours: 48,
            reason: String::new(),
        },
        "a",
        1,
        2.0,
        0,
    )
    .unwrap();
    assert_eq!(tick(&mut rows[0], 47 * H, t), None);
    assert_eq!(tick(&mut rows[0], 48 * H, t), Some(TickChange::Unsnoozed));
    assert_eq!(rows[0].state, State::Open);
    assert_eq!(rows[0].auto_apply_at_us, Some(58 * H));
}

#[test]
fn unrefreshed_items_expire_and_stuck_ones_fail() {
    let mut rows = Vec::new();
    raise_into(&mut rows, &proposal(10.0, "a"), &policy(Mode::Recommend), 0);
    let t = Duration::from_secs(24 * 3600);
    assert_eq!(tick(&mut rows[0], 72 * H - 1, t), None);
    assert_eq!(tick(&mut rows[0], 72 * H + 1, t), Some(TickChange::Expired));
    let mut rows = Vec::new();
    raise_into(&mut rows, &proposal(10.0, "a"), &policy(Mode::Approve), 0);
    act(
        &mut rows[0],
        &Act::Approve {
            reason: String::new(),
            window: None,
        },
        "a",
        1,
        2.0,
        0,
    )
    .unwrap();
    assert!(start_applying(&mut rows[0], H));
    assert_eq!(tick(&mut rows[0], 25 * H + 1, t), Some(TickChange::Lost));
    assert_eq!(rows[0].state, State::Failed);
}

#[test]
fn emergencies_skip_the_wait_only_when_allowed() {
    let mut rows = Vec::new();
    let mut p = proposal(10.0, "a");
    p.emergency = true;
    let plan = raise_into(&mut rows, &p, &policy(Mode::Approve), 0);
    assert_eq!(plan.verdict, VerdictPlan::Proceed);
    assert_eq!(plan.event, Some(EventKind::Emergency));
    assert!(rows[0].emergency);
    let mut rows = Vec::new();
    let mut pol = policy(Mode::Approve);
    pol.allow_emergency = false;
    assert_eq!(
        raise_into(&mut rows, &p, &pol, 0).verdict,
        VerdictPlan::Wait
    );
}

#[test]
fn comments_are_kept_and_capped() {
    let mut rows = Vec::new();
    raise_into(&mut rows, &proposal(10.0, "a"), &policy(Mode::Approve), 0);
    for i in 0..105 {
        act(
            &mut rows[0],
            &Act::Comment {
                text: format!("c{i}"),
            },
            "a",
            1,
            2.0,
            i,
        )
        .unwrap();
    }
    assert_eq!(rows[0].comments.len(), MAX_COMMENTS);
    assert_eq!(rows[0].comments[0].body, "c5");
    assert!(
        act(
            &mut rows[0],
            &Act::Comment { text: " ".into() },
            "a",
            1,
            2.0,
            0
        )
        .is_err()
    );
}
