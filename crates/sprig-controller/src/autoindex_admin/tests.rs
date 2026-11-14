//! The pure half of the automatic-indexing calls: actions on the index
//! state machine, `{index}` references, filters, and explanations.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use sprig_advisor::autoindex::model::{
    AutoIndexId, Evidence as CandEvidence, KeyPart, PartSource, Role,
};
use sprig_advisor::autoindex::policy::{ObjectPolicy, SettingSource};
use sprig_advisor::autoindex::store::{
    Baseline, CandidateStatus, Definition, DropReason, IndexState, Model, ShardBuild, StoredKind,
    ValidationMethod, ValidationRow, Verdict,
};
use sprig_common::ids::{DcId, ObjectId, TenantId};
use sprig_settings::{MemSettings, Registry, SettingCtx};

use super::view::*;
use super::*;

const T0: u64 = 1_793_000_000; // 2026-10-26

fn at(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(T0 + secs)
}

fn names() -> ObjectNames {
    ObjectNames(BTreeMap::from([
        (ObjectId(1), ("shop".to_owned(), "orders".to_owned())),
        (ObjectId(2), ("shop".to_owned(), "events".to_owned())),
        (ObjectId(3), ("billing".to_owned(), "orders".to_owned())),
    ]))
}

const FP: Fingerprint = Fingerprint(0x9c41_d2e0_7b3a_5f18);

/// An index row for tests (also used by `sprigctl`'s golden tests through
/// the JSON views).
pub(crate) fn index_row(id: u64, object: u64, name: &str, state: IndexState) -> AutoIndexRow {
    AutoIndexRow {
        index_id: AutoIndexId(id),
        object: ObjectId(object),
        name: name.to_owned(),
        model: Model::Sql,
        kind: StoredKind::BTree,
        definition: Definition {
            parts: vec![
                KeyPart::col("customer_id"),
                KeyPart::col("created_at").desc(),
            ],
            options: sprig_advisor::autoindex::model::IndexOptions::default(),
        },
        state,
        prior_state: None,
        candidate_id: 10 + id,
        validation_id: Some(100 + id),
        decision_id: None,
        est_benefit_ms_h: 190_500.0,
        est_write_ms_h: 7_200.0,
        est_bytes: 2_240_000_000,
        actual_bytes: None,
        shards: BTreeMap::from([("17".to_owned(), ShardBuild::Built)]),
        baseline: BTreeMap::from([(
            FP.to_string(),
            Baseline {
                runs: 18_240,
                p95_ms: 412.0,
                mean_ms: 260.0,
            },
        )]),
        observed: None,
        last_used_at: None,
        drop_reason: None,
        created_at: at(3600),
        state_changed_at: at(7200),
    }
}

pub(crate) fn candidate_row(id: u64, object: u64, status: CandidateStatus) -> CandidateRow {
    CandidateRow {
        candidate_id: id,
        run_id: 7,
        object: ObjectId(object),
        def_hash: 1,
        kind: StoredKind::BTree,
        definition: Definition {
            parts: vec![
                KeyPart::col("customer_id"),
                KeyPart::col("created_at").desc(),
            ],
            options: sprig_advisor::autoindex::model::IndexOptions::default(),
        },
        covering: false,
        fingerprints: vec![(FP, Role::Primary)],
        status,
        reason: None,
        benefit_ms_h: Some(190_500.0),
        write_ms_h: Some(7_200.0),
        est_bytes: Some(2_240_000_000),
        build_seconds: Some(112),
        evidence: CandEvidence {
            parts: vec![PartSource::Eq, PartSource::Order],
            selectivity: BTreeMap::from([("customer_id".to_owned(), 8.3e-6)]),
            sel_source: BTreeMap::from([("customer_id".to_owned(), "avg_frequency".to_owned())]),
            entries_per_run: Some(858.0),
            ..CandEvidence::default()
        },
        created_at: at(0),
    }
}

pub(crate) fn validation_row(candidate: u64, verdict: Verdict) -> ValidationRow {
    ValidationRow {
        validation_id: 111,
        candidate_id: candidate,
        method: ValidationMethod::Replica,
        cluster_id: "mum1-c7".into(),
        node: "n2".into(),
        shard_id: 17,
        started_at: at(1200),
        finished_at: Some(at(2400)),
        build_seconds: Some(118),
        statements: 260,
        noisy: 3,
        target_p95_before: Some(405.0),
        target_p95_after: Some(11.2),
        worst_regression: Some(3.1),
        worst_fp: Some(Fingerprint(0x51aa_02c4_e9d0_f337)),
        recall_at_k: None,
        verdict,
        reason: None,
        details: serde_json::json!({"samples": [1, 2]}),
    }
}

fn policy() -> ObjectPolicy {
    let reg = Registry::global().unwrap();
    let mem = MemSettings::new(reg);
    let dc = DcId::new("mum1").unwrap();
    let ctx = SettingCtx::process(&dc);
    ObjectPolicy::resolve(&mem as &dyn SettingSource, &ctx).unwrap()
}

#[test]
fn actions_follow_the_state_machine() {
    use IndexState as S;
    let plan = |s: S, a: Action| plan_action(&index_row(1, 1, "sprig_auto_5be0c2a1", s), a);
    assert!(matches!(
        plan(S::Recommended, Action::Approve),
        Plan::Move {
            from: S::Recommended,
            to: S::RollingOut,
            drop_reason: None,
            ..
        }
    ));
    assert!(matches!(
        plan(S::Recommended, Action::Reject),
        Plan::Move {
            to: S::Dropped,
            drop_reason: Some(DropReason::Dismissed),
            ..
        }
    ));
    for s in [
        S::RollingOut,
        S::Observing,
        S::Active,
        S::Retiring,
        S::Failed,
    ] {
        assert!(
            matches!(
                plan(s, Action::Drop),
                Plan::Move {
                    to: S::Dropping,
                    drop_reason: Some(DropReason::Operator),
                    ..
                }
            ),
            "{s}"
        );
    }
    for s in [
        S::Validating,
        S::Recommended,
        S::RollingOut,
        S::Active,
        S::Retiring,
    ] {
        assert!(
            matches!(plan(s, Action::Pin), Plan::Move { to: S::Frozen, .. }),
            "{s}"
        );
    }
    assert_eq!(plan(S::Frozen, Action::Unpin), Plan::Unfreeze);
    // Already so: nothing to do.
    for (s, a) in [
        (S::Active, Action::Approve),
        (S::Dropping, Action::Drop),
        (S::Dropped, Action::Drop),
        (S::Frozen, Action::Pin),
        (S::Active, Action::Unpin),
    ] {
        assert!(matches!(plan(s, a), Plan::Noop(_)), "{s} {a:?}");
    }
    // Refused, with the reason.
    for (s, a, why) in [
        (S::Active, Action::Reject, "only a recommendation"),
        (S::Validating, Action::Approve, "only a recommendation"),
        (S::Recommended, Action::Drop, "reject the recommendation"),
        (S::Frozen, Action::Drop, "unpin it first"),
        (S::Validating, Action::Drop, "once validation finishes"),
        (S::Dropped, Action::Pin, "nothing left"),
    ] {
        match plan(s, a) {
            Plan::Refuse(m) => assert!(m.contains(why), "{m}"),
            p => panic!("{s} {a:?}: {p:?}"),
        }
    }
    let mut advice = index_row(2, 1, "sprig_auto_00000002", S::Recommended);
    advice.kind = StoredKind::AdviceSearch;
    assert!(matches!(
        plan_action(&advice, Action::Approve),
        Plan::Refuse(_)
    ));
    let mut dismissed = index_row(3, 1, "sprig_auto_00000003", S::Dropped);
    dismissed.drop_reason = Some(DropReason::Dismissed);
    assert!(matches!(
        plan_action(&dismissed, Action::Reject),
        Plan::Noop(_)
    ));
    // Route spellings, the alternative names included.
    for (m, a) in [
        ("approve", Action::Approve),
        ("apply", Action::Approve),
        ("dismiss", Action::Reject),
        ("freeze", Action::Pin),
        ("unfreeze", Action::Unpin),
    ] {
        assert_eq!(Action::parse(m), Some(a));
    }
    assert_eq!(Action::parse("build"), None);
}

#[test]
fn index_references() {
    let rows = vec![
        index_row(1, 1, "sprig_auto_5be0c2a1", IndexState::Active),
        index_row(2, 3, "sprig_auto_5be0c2a1", IndexState::Recommended),
        index_row(3, 2, "sprig_auto_0badc0de", IndexState::Dropped),
        index_row(4, 2, "sprig_auto_0badc0de", IndexState::Observing),
        index_row(5, 2, "sprig_auto_11111111", IndexState::Dropped),
        index_row(6, 2, "sprig_auto_11111111", IndexState::Rejected),
    ];
    let n = names();
    let id = |r: &str| resolve_index(&rows, &n, r).map(|r| r.index_id.0);
    assert_eq!(id("2").unwrap(), 2);
    assert_eq!(id("shop.orders.sprig_auto_5be0c2a1").unwrap(), 1);
    assert_eq!(id("billing.orders.SPRIG_AUTO_5BE0C2A1").unwrap(), 2);
    // The live row wins over its history; then the newest.
    assert_eq!(id("events.sprig_auto_0badc0de").unwrap(), 4);
    assert_eq!(id("sprig_auto_11111111").unwrap(), 6);
    let e = id("orders.sprig_auto_5be0c2a1").unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::AdminRequestInvalid));
    assert!(
        e.message.contains("1 = shop.orders.sprig_auto_5be0c2a1"),
        "{}",
        e.message
    );
    for missing in [
        "99",
        "sprig_auto_ffffffff",
        "crm.orders.sprig_auto_5be0c2a1",
    ] {
        let e = id(missing).unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::AutoIndexNotFound), "{missing}");
        assert_eq!(e.protocol().http_status, 404);
    }
}

#[test]
fn list_filters() {
    let rows = vec![
        index_row(1, 1, "sprig_auto_00000001", IndexState::Active),
        index_row(2, 3, "sprig_auto_00000002", IndexState::Recommended),
        index_row(3, 2, "sprig_auto_00000003", IndexState::Dropped),
    ];
    let n = names();
    let ids = |q: ListIndexesQuery, rec: bool| {
        filter_rows(&rows, &n, &q, rec)
            .unwrap()
            .iter()
            .map(|r| r.index_id.0)
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(ListIndexesQuery::default(), false), [1, 2]);
    assert_eq!(
        ids(
            ListIndexesQuery {
                all: Some(true),
                ..ListIndexesQuery::default()
            },
            false
        ),
        [1, 2, 3]
    );
    assert_eq!(ids(ListIndexesQuery::default(), true), [2]);
    assert_eq!(
        ids(
            ListIndexesQuery {
                table: Some("ORDERS".into()),
                ..ListIndexesQuery::default()
            },
            false
        ),
        [1, 2]
    );
    assert_eq!(
        ids(
            ListIndexesQuery {
                db: Some("shop".into()),
                state: Some("dropped".into()),
                ..ListIndexesQuery::default()
            },
            false
        ),
        [3]
    );
    let bad = ListIndexesQuery {
        state: Some("gone".into()),
        ..ListIndexesQuery::default()
    };
    assert_eq!(
        filter_rows(&rows, &n, &bad, false).unwrap_err().code(),
        Some(SprigCode::AdminRequestInvalid)
    );
    // From a query string.
    let q: ListIndexesQuery =
        serde_json::from_value(serde_json::json!({"all": "true", "db": "shop"})).unwrap();
    assert_eq!(q.all, Some(true));
    assert!(
        serde_json::from_value::<ListIndexesQuery>(serde_json::json!({"all": "maybe"})).is_err()
    );
}

#[test]
fn views_never_carry_sample_details() {
    let v = validation_view(&validation_row(11, Verdict::Accepted));
    let j = serde_json::to_string(&v).unwrap();
    assert!(!j.contains("samples"), "{j}");
    assert_eq!(v.improvement_pct, Some(97.2));
    let i = index_view(
        &index_row(1, 1, "sprig_auto_5be0c2a1", IndexState::Active),
        &names(),
        Some(&candidate_row(11, 1, CandidateStatus::Selected)),
    );
    assert_eq!(i.columns, ["customer_id", "created_at desc"]);
    assert_eq!(
        i.ddl.as_deref(),
        Some(
            "ALTER TABLE `shop`.`orders` ADD INDEX `sprig_auto_5be0c2a1` (`customer_id`, `created_at` DESC)"
        )
    );
    assert_eq!(i.fingerprints[0].role.as_deref(), Some("primary"));
    assert!((i.est_net_ms_h - 183_300.0).abs() < 1e-9);
    assert_eq!(i.shards["17"], "built");
}

#[test]
fn explain_with_no_evidence() {
    let e = explain("acme", FP, at(0), &Evidence::default(), &names());
    assert_eq!(e.fingerprint, "9c41d2e07b3a5f18");
    assert!(e.summary[0].starts_with("no candidate recorded for 9c41d2e07b3a5f18 since"));
    assert!(e.summary[1].contains("didn't qualify"));
    assert!(e.candidates.is_empty());
}

#[test]
fn explain_a_recommendation_with_its_gates_and_validation() {
    let cands = vec![candidate_row(11, 1, CandidateStatus::Recommended)];
    let mut ev = Evidence {
        candidates: &cands,
        ..Evidence::default()
    };
    ev.validations
        .insert(11, vec![validation_row(11, Verdict::Accepted)]);
    ev.indexes.insert(
        11,
        vec![index_row(
            1,
            1,
            "sprig_auto_5be0c2a1",
            IndexState::Recommended,
        )],
    );
    ev.policies.insert(ObjectId(1), policy());
    let e = explain("acme", FP, at(0), &ev, &names());
    assert_eq!(
        e.summary[0],
        "shop.orders: index sprig_auto_5be0c2a1 (customer_id, created_at desc) is recommended: \
         sprigctl index approve 1 --tenant acme rolls it out, sprigctl index reject turns it down"
    );
    let c = &e.candidates[0];
    assert_eq!(c.role, "primary");
    assert!(
        c.gates.iter().all(|g| g.passed == Some(true)),
        "{:?}",
        c.gates
    );
    assert_eq!(c.validations[0].verdict, "accepted");
    assert_eq!(e.policies[0].mode, "on");
    assert_eq!(
        e.policies[0].settings["auto_index.min_runs"],
        "50 (default)"
    );
    assert!(
        e.policies[0]
            .route
            .starts_with("on: validate, then roll out")
    );
}

#[test]
fn explain_refusals_and_skips() {
    let mut low = candidate_row(12, 1, CandidateStatus::Rejected);
    low.reason = Some("below_min_net".into());
    low.benefit_ms_h = Some(1_650.0);
    low.write_ms_h = Some(40_000.0);
    let mut covered = candidate_row(13, 2, CandidateStatus::Covered);
    covered.reason = Some("covered_by:idx_created".into());
    covered.benefit_ms_h = None;
    covered.write_ms_h = None;
    let mut skip = CandidateRow::skipped(
        7,
        ObjectId(2),
        FP,
        "function:LOWER",
        vec!["events.source".into()],
        at(0),
    );
    skip.candidate_id = 14;
    let mut older = candidate_row(9, 1, CandidateStatus::Deferred);
    older.run_id = 5;
    older.reason = Some("budget".into());
    let cands = vec![low, covered, skip, older];
    let mut ev = Evidence {
        candidates: &cands,
        ..Evidence::default()
    };
    ev.policies.insert(ObjectId(1), policy());
    let e = explain("acme", FP, at(0), &ev, &names());
    assert_eq!(
        e.summary,
        [
            "shop.orders: candidate (customer_id, created_at desc) rejected: its net benefit is below auto_index.min_net_ms_per_hour",
            "shop.events: candidate (customer_id, created_at desc) an existing index, idx_created, already serves it",
            "no candidate on shop.events: a predicate wraps its column in LOWER(), so no index can serve it",
            "1 earlier evaluation also recorded candidates for it",
        ]
    );
    let g = &e.candidates[0].gates;
    assert_eq!(g[0].gate, "min_net");
    assert_eq!(g[0].passed, Some(false));
    assert_eq!(g[1].passed, Some(false), "1,650 < 2 x 40,000");
    assert!(e.candidates[2].gates.is_empty(), "a skip has no gates");
    assert_eq!(e.candidates[2].candidate.notes, ["events.source"]);
    // Without a policy the gates aren't judged.
    assert!(e.candidates[1].gates.is_empty());
}

#[test]
fn history_is_in_time_order() {
    let r = index_row(1, 1, "sprig_auto_5be0c2a1", IndexState::Active);
    let c = candidate_row(11, 1, CandidateStatus::Selected);
    let h = history(&r, Some(&c), &[validation_row(11, Verdict::Accepted)]);
    let events: Vec<&str> = h.iter().map(|e| e.event.as_str()).collect();
    assert_eq!(
        events,
        ["candidate", "validation", "validation", "index", "state"]
    );
    assert_eq!(h[0].at, "2026-10-26T07:33:20Z");
    assert_eq!(h[4].detail, "now active");
    let _ = TenantId(1);
}
