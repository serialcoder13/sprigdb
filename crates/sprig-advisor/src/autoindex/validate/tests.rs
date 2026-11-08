//! Plan helpers, method choice, the covering variant and the estimate gate.

#![allow(clippy::unwrap_used, clippy::float_cmp)]

use sprig_common::ids::ObjectId;

use super::*;
use crate::autoindex::model::{Evidence, IndexDef, Role};
use crate::autoindex::replay::{SessionContext, StatementRole};

pub(crate) fn plan(cols: &[&str]) -> ValidationPlan {
    let def = IndexDef::btree(ObjectId(1201), cols);
    ValidationPlan {
        candidate_id: 9,
        candidate: Candidate {
            evidence: Evidence {
                parts: vec![PartSource::Range; def.parts.len()],
                ..Evidence::default()
            },
            def,
            covering: false,
            serves: vec![(Fingerprint(1), Role::Primary)],
        },
        target: IndexTarget {
            schema: "s1".into(),
            table: "t".into(),
            pk: vec!["id".into()],
        },
        statements: vec![ReplayStatement {
            fingerprint: Fingerprint(1),
            sample_id: 1,
            role: StatementRole::Target,
            sql: "SELECT id FROM t WHERE a = 1".into(),
            context: SessionContext::default(),
        }],
        predictions: vec![Prediction {
            fingerprint: Fingerprint(1),
            t_before_ms: 100.0,
            t_after_ms: 2.0,
            rows_assumed: 100_000.0,
        }],
        columns_read: BTreeMap::new(),
        keep_if_accepted: false,
        settings: ValidationSettings::default(),
    }
}

#[test]
fn methods_follow_the_profile() {
    assert_eq!(
        method_for(Some(ValidationMethod::ReplicaBuild), true),
        Method::Replica
    );
    assert_eq!(
        method_for(Some(ValidationMethod::GaleraRsu), true),
        Method::GaleraRsu
    );
    assert_eq!(method_for(None, true), Method::EstimateCanary);
    // Samples off: estimates and a canary only (chapter 13, failures).
    assert_eq!(
        method_for(Some(ValidationMethod::ReplicaBuild), false),
        Method::EstimateCanary
    );
    assert_eq!(Method::GaleraRsu.as_str(), "galera_rsu");
}

#[test]
fn the_plan_names_the_index_by_its_definition() {
    let p = plan(&["a"]);
    assert_eq!(p.index_name(), p.candidate.def.name());
    assert!(p.index_name().starts_with("sprig_auto_"));
    let t = p.replay_target();
    assert_eq!(t.index, p.index_name());
    assert_eq!(t.pk, vec!["id".to_owned()]);
    assert!(p.check().is_ok());
    let mut bad = p.clone();
    bad.candidate.def.parts.clear();
    assert!(bad.check().is_err());
}

#[test]
fn covering_variant_adds_read_columns() {
    let mut p = plan(&["state", "due_date"]);
    p.columns_read.insert(
        Fingerprint(6),
        vec![
            "state".into(),
            "due_date".into(),
            "amount".into(),
            "id".into(),
        ],
    );
    let c = covering_variant(&p, Fingerprint(6)).unwrap();
    assert_eq!(c.def.canonical(), "btree(state,due_date,amount)");
    assert!(c.covering);
    assert_eq!(c.evidence.parts.last(), Some(&PartSource::Cover));
    assert_ne!(c.def.name(), p.candidate.def.name());
    // Nothing to add, or too much.
    p.columns_read.insert(Fingerprint(7), vec!["state".into()]);
    assert!(covering_variant(&p, Fingerprint(7)).is_none());
    p.columns_read.insert(
        Fingerprint(8),
        vec!["w".into(), "x".into(), "y".into(), "z".into()],
    );
    assert!(covering_variant(&p, Fingerprint(8)).is_none());
    assert!(covering_variant(&p, Fingerprint(99)).is_none());
}

#[test]
fn predictions_must_clear_the_margin() {
    let mut p = plan(&["a"]);
    assert!(canary::check_predictions(&p).is_ok());
    // 40% predicted < 30% × 1.5 = 45%.
    p.predictions[0].t_after_ms = 60.0;
    let e = canary::check_predictions(&p).unwrap_err();
    assert!(e.starts_with("prediction_below_margin"), "{e}");
    p.predictions[0].t_after_ms = 54.0;
    assert!(canary::check_predictions(&p).is_ok());
    p.predictions.clear();
    assert!(
        canary::check_predictions(&p)
            .unwrap_err()
            .starts_with("no_prediction")
    );
}

#[test]
fn reports_render_the_validation_row() {
    let p = plan(&["a"]);
    let node = NodeId::new("n3").unwrap();
    let mut r = report(&p, Method::Replica, &node);
    r.build_ms = Some(220);
    r.finished_at_ms = r.started_at_ms + 1500;
    assert_eq!(r.build_seconds(), Some(1));
    assert_eq!(r.elapsed_ms(), 1500);
    assert_eq!(r.verdict.kind, VerdictKind::Inconclusive);
    abort(&mut r, "x".repeat(300));
    assert_eq!(r.verdict.kind, VerdictKind::Aborted);
    assert_eq!(r.verdict.reason.len(), 128);
    let j = r.details_json();
    assert_eq!(j["timings_ms"]["build"], 220);
    assert_eq!(j["node_check"]["restored"], true);
    r.node_check.bad("index still there");
    assert!(!r.node_check.restored);
}
