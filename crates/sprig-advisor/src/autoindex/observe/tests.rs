//! Unit tests of observation: the watch's rules and supersession.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;

use sprig_common::ids::{Fingerprint, ObjectId};

use super::*;
use crate::autoindex::model::{IndexDef, KeyPart, Origin};
use crate::autoindex::rollout::node::NodeIndex;
use crate::autoindex::store::{Baseline, DropReason};

fn rules() -> ObserveRules {
    ObserveRules {
        min_improvement_pct: 30.0,
        max_regression_pct: 10.0,
        observe_days: 7,
        unused_drop_days: 30,
        ignore_before_drop_hours: 24,
        recreate_block_days: 14,
    }
}

fn b(p95: f64, mean: f64) -> Baseline {
    Baseline {
        runs: 1000,
        p95_ms: p95,
        mean_ms: mean,
    }
}

fn l(runs: u64, p95: f64, mean: f64, uses: Option<bool>, write: bool) -> FpLatency {
    FpLatency {
        runs,
        p95_ms: p95,
        mean_ms: mean,
        uses_index: uses,
        write,
    }
}

const T: Fingerprint = Fingerprint(0x9c41_d2e0_7b3a_5f18);
const R: Fingerprint = Fingerprint(0x51aa_02c4_e9d0_f337);
const W: Fingerprint = Fingerprint(0x1111_2222_3333_4444);

fn base() -> BTreeMap<String, Baseline> {
    [(T, b(400.0, 260.0)), (R, b(100.0, 80.0)), (W, b(2.0, 1.0))]
        .into_iter()
        .map(|(f, b)| (f.to_string(), b))
        .collect()
}

#[test]
fn a_good_index_is_kept_after_the_period() {
    let now: BTreeMap<_, _> = [(T, l(500, 12.0, 9.0, Some(true), false))].into();
    let (v, m) = judge_watch(&base(), &[T], &now, 24.0, &rules(), 5, false);
    assert_eq!(v, WatchVerdict::Continue);
    assert!(m.improvement_pct.unwrap() > 95.0);
    let (v, _) = judge_watch(&base(), &[T], &now, 24.0 * 7.0, &rules(), 5, true);
    assert_eq!(v, WatchVerdict::Activate);
}

#[test]
fn no_gain_regression_and_write_cost_retire() {
    // The targets barely improved.
    let now: BTreeMap<_, _> = [(T, l(500, 350.0, 240.0, Some(true), false))].into();
    assert_eq!(
        judge_watch(&base(), &[T], &now, 24.0, &rules(), 5, false).0,
        WatchVerdict::Retire(DropReason::NoGain)
    );
    // Another reader now uses it and got 44% slower (worked example 3).
    let now: BTreeMap<_, _> = [
        (T, l(500, 4.0, 3.0, Some(true), false)),
        (R, l(40, 144.0, 120.0, Some(true), false)),
    ]
    .into();
    let (v, m) = judge_watch(&base(), &[T], &now, 24.0, &rules(), 5, false);
    assert_eq!(v, WatchVerdict::Retire(DropReason::Regression));
    assert_eq!(m.worst_fp.as_deref(), Some(R.to_string().as_str()));
    // The same slowdown in a reader that doesn't use the index is noise.
    let now: BTreeMap<_, _> = [
        (T, l(500, 4.0, 3.0, Some(true), false)),
        (R, l(40, 144.0, 120.0, Some(false), false)),
    ]
    .into();
    assert_eq!(
        judge_watch(&base(), &[T], &now, 24.0, &rules(), 5, false).0,
        WatchVerdict::Continue
    );
    // Writes 3x slower and many of them; the target runs rarely.
    let now: BTreeMap<_, _> = [
        (T, l(10, 4.0, 3.0, Some(true), false)),
        (W, l(1_000_000, 6.0, 3.0, None, true)),
    ]
    .into();
    let (v, m) = judge_watch(&base(), &[T], &now, 24.0, &rules(), 5, false);
    assert_eq!(v, WatchVerdict::Retire(DropReason::WriteCost));
    assert!(m.write_cost_ms_h.unwrap() > m.benefit_ms_h.unwrap());
    // Too few runs to judge: keep watching.
    let now: BTreeMap<_, _> = [(T, l(3, 390.0, 250.0, Some(true), false))].into();
    assert_eq!(
        judge_watch(&base(), &[T], &now, 24.0, &rules(), 5, false).0,
        WatchVerdict::Continue
    );
}

fn ni(name: &str, cols: &[&str]) -> NodeIndex {
    NodeIndex {
        name: name.into(),
        parts: cols.iter().map(|c| KeyPart::col(c)).collect(),
        unique: false,
        ignored: false,
    }
}

#[test]
fn user_and_migrated_indexes_supersede_ours() {
    let def = IndexDef::btree(ObjectId(7), &["customer_id", "created_at"]);
    let mut ix: BTreeMap<String, NodeIndex> = BTreeMap::new();
    ix.insert(
        "PRIMARY".into(),
        ni("PRIMARY", &["customer_id", "created_at"]),
    );
    ix.insert(def.name(), ni(&def.name(), &["customer_id", "created_at"]));
    ix.insert("idx_c".into(), ni("idx_c", &["customer_id"]));
    let rec = BTreeMap::new();
    // Neither the primary key, our own index nor a shorter one supersede.
    assert!(superseding(&ix, &rec, &def).is_none());
    // A user index that extends ours does.
    ix.insert(
        "idx_cc".into(),
        ni("idx_cc", &["customer_id", "created_at", "total"]),
    );
    assert_eq!(superseding(&ix, &rec, &def).unwrap().name, "idx_cc");
    // Unless it is ignored.
    ix.get_mut("idx_cc").unwrap().ignored = true;
    assert!(superseding(&ix, &rec, &def).is_none());
    // A migrated one counts too.
    ix.insert("imp".into(), ni("imp", &["customer_id", "created_at"]));
    let rec: BTreeMap<String, Origin> = [("imp".to_owned(), Origin::Migrated)].into();
    assert_eq!(superseding(&ix, &rec, &def).unwrap().name, "imp");
    // Directions and prefixes must match or be wider.
    let mut d = KeyPart::col("customer_id");
    d.desc = true;
    assert!(!supersedes(&[d, KeyPart::col("created_at")], &def.parts));
    let mut short = KeyPart::col("customer_id");
    short.prefix = Some(8);
    let mut long = KeyPart::col("customer_id");
    long.prefix = Some(16);
    assert!(supersedes(&[long.clone()], &[short.clone()]));
    assert!(!supersedes(&[short], &[long]));
}
