#![allow(clippy::unwrap_used, clippy::float_cmp, clippy::cast_precision_loss)]

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use sprig_common::ids::DcId;
use sprig_settings::{MemSettings, SettingCtx};
use sprig_workload::histogram::upper_bound_us;
use sprig_workload::shape::Shape;

use super::*;
use crate::autoindex::evaluate::synth::{DB, TENANT, probed, registry, scans, win};
use crate::autoindex::model::Role;
use crate::autoindex::store::{CandidateStatus, Definition, StoredKind};

const A: ObjectId = ObjectId(501);
const B: ObjectId = ObjectId(502);

fn defaults() -> ObjectPolicy {
    let s = MemSettings::new(registry());
    let dc = DcId::new("mum1").unwrap();
    let ctx = SettingCtx::process(&dc)
        .for_tenant(TENANT, &[])
        .with_db(DB)
        .with_object(A);
    ObjectPolicy::resolve(&s, &ctx).unwrap()
}

fn shape(json: &serde_json::Value) -> Arc<Shape> {
    Arc::new(Shape::from_json(&json.to_string()).unwrap())
}

/// `SELECT * FROM a WHERE c = ?` on object `o`.
fn eq_shape(o: ObjectId) -> Arc<Shape> {
    shape(&serde_json::json!({
        "v": 1, "type": "select",
        "tables": [{"alias": "a", "object_id": o.0, "table": "a"}],
        "predicates": [{"t": "a", "col": "c", "op": "eq", "args": [0]}],
        "joins": [], "order_by": [], "group_by": [], "select": {"a": ["*"]},
        "limit": false, "writes": null, "or_groups": 0}))
}

fn signals(runs: u64, p95_ms: f64) -> Signals {
    Signals {
        runs,
        p95_ms,
        total_ms: 0.0,
        examined_samples: 0,
        examined_per_returned: None,
        scan_runs: 0,
        scan_sessions: 0,
    }
}

struct Run<'a> {
    policies: BTreeMap<ObjectId, ObjectPolicy>,
    sizes: BTreeMap<ObjectId, u64>,
    decided: Vec<CandidateRow>,
    mode: Mode,
    include_studio: bool,
    now: SystemTime,
    _p: std::marker::PhantomData<&'a ()>,
}

impl Run<'_> {
    fn new() -> Self {
        let mut policies = BTreeMap::new();
        policies.insert(A, defaults());
        policies.insert(B, defaults());
        Self {
            policies,
            sizes: BTreeMap::new(),
            decided: Vec::new(),
            mode: Mode::On,
            include_studio: false,
            now: SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000),
            _p: std::marker::PhantomData,
        }
    }

    fn go(&self, entries: Vec<SourcedWindow>) -> Qualification {
        let inc = self.include_studio;
        let (w, excluded) = window_of(entries, &|_| inc);
        let policy = |o: ObjectId| self.policies.get(&o);
        let size = |o: ObjectId| self.sizes.get(&o).copied();
        qualify(
            w,
            &excluded,
            &QualifyInput {
                tenant_mode: self.mode,
                policy: &policy,
                data_bytes: &size,
                decided: &self.decided,
                now: self.now,
            },
        )
    }
}

fn miss_of(q: &Qualification, fp: u64) -> &Miss {
    &q.not_qualified
        .iter()
        .find(|n| n.fp == Fingerprint(fp))
        .unwrap_or_else(|| panic!("{fp:x} qualified: {:?}", q.qualified))
        .why
}

fn qualified(q: &Qualification, fp: u64) -> &Qualified {
    q.qualified
        .iter()
        .find(|x| x.fp == Fingerprint(fp))
        .unwrap_or_else(|| panic!("{fp:x} not qualified: {:?}", q.not_qualified))
}

#[test]
fn forty_nine_runs_do_not_qualify_and_fifty_do() {
    let p = defaults();
    assert_eq!(
        triggers(&signals(49, 150.0), 99, &p).unwrap_err().slow,
        SlowMiss::FewRuns { runs: 49, need: 50 }
    );
    assert_eq!(
        triggers(&signals(50, 150.0), 99, &p).unwrap(),
        [Trigger::SlowP95]
    );
    // Through a window: 49 runs of a 150 ms statement don't qualify.
    let r = Run::new();
    let q = r.go(vec![
        SourcedWindow::client(win(1, eq_shape(A), 49, 120.0, 150.0)),
        SourcedWindow::client(win(2, eq_shape(A), 50, 120.0, 150.0)),
    ]);
    assert!(matches!(
        miss_of(&q, 1),
        Miss::Below(Shortfall {
            slow: SlowMiss::FewRuns { runs: 49, .. },
            ..
        })
    ));
    let x = qualified(&q, 2);
    assert_eq!(x.trigger, Trigger::SlowP95);
    assert_eq!(x.object, A);
    assert!(!x.advise && !x.fast_only());
}

#[test]
fn p95_at_the_threshold_does_not_qualify() {
    let p = defaults();
    assert!(matches!(
        triggers(&signals(500, 100.0), 99, &p).unwrap_err().slow,
        SlowMiss::FastP95 { .. }
    ));
    assert_eq!(
        triggers(&signals(500, 100.001), 99, &p).unwrap(),
        [Trigger::SlowP95]
    );
    // The histogram's p95 is a bucket's upper bound: the last bucket at
    // or below 100 ms doesn't qualify, the next one does.
    let i = (0..96).rfind(|&i| upper_bound_us(i) <= 100_000).unwrap();
    let below = upper_bound_us(i) as f64 / 1000.0;
    let above = upper_bound_us(i + 1) as f64 / 1000.0;
    assert!(below <= 100.0 && above > 100.0);
    let q = Run::new().go(vec![
        SourcedWindow::client(win(1, eq_shape(A), 500, 50.0, below)),
        SourcedWindow::client(win(2, eq_shape(A), 500, 50.0, above)),
    ]);
    assert!(matches!(
        miss_of(&q, 1),
        Miss::Below(Shortfall {
            slow: SlowMiss::FastP95 { .. },
            ..
        })
    ));
    assert!(qualified(&q, 1 + 1).signals.p95_ms > 100.0);
}

/// Eleven fingerprints by total time, the heavy one at `rank`.
fn heavy_at(rank: u64, ratio: f64, probes: u64) -> Vec<SourcedWindow> {
    let mut v = Vec::new();
    for i in 1..=11u64 {
        // Total time falls with i; fast statements (p95 1 ms).
        let w = win(i, eq_shape(A), 1000, 100.0 - i as f64, 1.0);
        let w = if i == rank {
            probed(w, probes, ratio * 2.0, 2.0)
        } else {
            probed(w, 10, 2.0, 1.0)
        };
        v.push(SourcedWindow::client(w));
    }
    v
}

#[test]
fn top_ten_heavy_rule() {
    let r = Run::new();
    // Rank 10, 150 rows examined per row returned: top_total_time.
    let q = r.go(heavy_at(10, 150.0, 5));
    let x = qualified(&q, 10);
    assert_eq!((x.trigger, x.rank), (Trigger::TopTotalTime, 10));
    assert_eq!(q.qualified.len(), 1, "{:?}", q.qualified);
    // Rank 11: not among the top 10.
    let q = r.go(heavy_at(11, 150.0, 5));
    assert!(matches!(
        miss_of(&q, 11),
        Miss::Below(Shortfall {
            heavy: HeavyMiss::NotTop {
                rank: 11,
                top_n: 10
            },
            ..
        })
    ));
    // Exactly 100 rows per row returned: not more than 100.
    let q = r.go(heavy_at(3, 100.0, 5));
    assert!(matches!(
        miss_of(&q, 3),
        Miss::Below(Shortfall {
            heavy: HeavyMiss::LowRatio { .. },
            ..
        })
    ));
    // Four probes are too few (min_examined_samples 5).
    let q = r.go(heavy_at(3, 500.0, 4));
    assert!(matches!(
        miss_of(&q, 3),
        Miss::Below(Shortfall {
            heavy: HeavyMiss::FewProbes {
                samples: 4,
                need: 5
            },
            ..
        })
    ));
    // A larger top_n on the object lets rank 11 in.
    let mut r = Run::new();
    r.policies.get_mut(&A).unwrap().trigger.top_n = 11;
    let q = r.go(heavy_at(11, 150.0, 5));
    assert_eq!(qualified(&q, 11).trigger, Trigger::TopTotalTime);
}

#[test]
fn slow_and_heavy_both_fire_slow_is_the_reason() {
    let w = probed(
        win(7, eq_shape(A), 18_240, 260.0, 412.0),
        400,
        32_600.0,
        18.0,
    );
    let q = Run::new().go(vec![SourcedWindow::client(w)]);
    let x = qualified(&q, 7);
    assert_eq!(x.trigger, Trigger::SlowP95);
    assert_eq!(x.fired, [Trigger::SlowP95, Trigger::TopTotalTime]);
}

#[test]
fn modes_off_and_recommend() {
    let slow = || SourcedWindow::client(win(1, eq_shape(A), 100, 150.0, 200.0));
    // Tenant off: nothing at all.
    let mut r = Run::new();
    r.mode = Mode::Off;
    let q = r.go(vec![slow()]);
    assert!(q.qualified.is_empty());
    assert_eq!(miss_of(&q, 1), &Miss::TenantOff);
    // Object off while the tenant is on.
    let mut r = Run::new();
    r.policies.get_mut(&A).unwrap().mode = Mode::Off;
    let q = r.go(vec![slow()]);
    assert_eq!(miss_of(&q, 1), &Miss::ModeOff);
    // Recommend qualifies like on; the route differs (evaluate's tests).
    let mut r = Run::new();
    r.policies.get_mut(&A).unwrap().mode = Mode::Recommend;
    assert_eq!(qualified(&r.go(vec![slow()]), 1).trigger, Trigger::SlowP95);
}

#[test]
fn a_join_qualifies_per_object() {
    let join = shape(&serde_json::json!({
        "v": 1, "type": "select",
        "tables": [{"alias": "a", "object_id": A.0, "table": "a"},
                   {"alias": "b", "object_id": B.0, "table": "b"}],
        "predicates": [{"t": "a", "col": "c", "op": "eq", "args": [0]}],
        "joins": [{"l": ["a", "id"], "r": ["b", "a_id"], "op": "eq", "kind": "inner"}],
        "order_by": [], "group_by": [], "select": {"a": ["*"], "b": ["*"]},
        "limit": false, "writes": null, "or_groups": 0}));
    let mut r = Run::new();
    r.policies.get_mut(&B).unwrap().mode = Mode::Off;
    let q = r.go(vec![SourcedWindow::client(win(
        1,
        join.clone(),
        100,
        150.0,
        200.0,
    ))]);
    assert_eq!(q.objects(), [A]);
    assert!(
        q.not_qualified
            .iter()
            .any(|n| n.object == Some(B) && n.why == Miss::ModeOff)
    );
    // B's own thresholds: it needs 200 runs.
    let mut r = Run::new();
    r.policies.get_mut(&B).unwrap().trigger.min_runs = 200;
    let q = r.go(vec![SourcedWindow::client(win(1, join, 100, 150.0, 200.0))]);
    assert_eq!(q.objects(), [A]);
}

#[test]
fn studio_runs_are_excluded_unless_included() {
    let entries = || {
        vec![
            SourcedWindow::client(win(1, eq_shape(A), 30, 150.0, 200.0)),
            SourcedWindow::studio(win(1, eq_shape(A), 30, 150.0, 200.0)),
            SourcedWindow::studio(win(2, eq_shape(A), 500, 150.0, 200.0)),
        ]
    };
    let mut r = Run::new();
    let q = r.go(entries());
    // 30 client runs: too few; Studio's 500 runs of fp 2 don't count.
    assert!(matches!(miss_of(&q, 1), Miss::Below(_)));
    assert_eq!(miss_of(&q, 2), &Miss::ExcludedSource);
    assert!(!q.windows.contains_key(&Fingerprint(2)));
    // With include_studio, both count.
    r.include_studio = true;
    let q = r.go(entries());
    assert_eq!(qualified(&q, 1).signals.runs, 60);
    assert_eq!(qualified(&q, 2).trigger, Trigger::SlowP95);
}

#[test]
fn the_fast_trigger_needs_runs_and_sessions_and_only_recommends_on_large_tables() {
    // Large scans of 2 s each, 2 million rows examined, 10 returned.
    // Ten busier statements keep it out of the top 10 by total time, so
    // only the fast trigger can qualify it.
    let scan = |runs, sessions| {
        let w = probed(win(9, eq_shape(A), runs, 2000.0, 2000.0), runs, 2e6, 10.0);
        let mut v = vec![SourcedWindow::client(scans(w, runs, sessions))];
        for i in 100..110 {
            v.push(SourcedWindow::client(win(i, eq_shape(A), 10_000, 5.0, 5.0)));
        }
        v
    };
    let mut r = Run::new();
    r.sizes.insert(A, 10_000_000_000);
    let q = r.go(scan(5, 2));
    let x = qualified(&q, 9);
    assert_eq!(x.trigger, Trigger::LargeScan);
    assert!(x.fast_only() && !x.advise);
    // One session: someone exploring.
    assert!(matches!(
        miss_of(&r.go(scan(5, 1)), 9),
        Miss::Below(Shortfall {
            scan: ScanMiss::FewSessions {
                sessions: 1,
                need: 2
            },
            ..
        })
    ));
    // Four runs.
    assert!(matches!(
        miss_of(&r.go(scan(4, 3)), 9),
        Miss::Below(Shortfall {
            scan: ScanMiss::FewScanRuns { runs: 4, need: 5 },
            ..
        })
    ));
    // 150 GB: recommend only.
    r.sizes.insert(A, 150_000_000_000);
    assert!(qualified(&r.go(scan(5, 2)), 9).advise);
    // Exactly 100 GB is not above the limit.
    r.sizes.insert(A, 100_000_000_000);
    assert!(!qualified(&r.go(scan(5, 2)), 9).advise);
    // Unknown size: decided later from the statistics.
    r.sizes.clear();
    assert!(!qualified(&r.go(scan(5, 2)), 9).advise);
    // Slow as well: the slow trigger is the reason and it isn't fast-only.
    let w = probed(win(9, eq_shape(A), 60, 2000.0, 2000.0), 60, 2e6, 10.0);
    let q = r.go(vec![SourcedWindow::client(scans(w, 60, 2))]);
    let x = qualified(&q, 9);
    assert_eq!(x.fired.first(), Some(&Trigger::SlowP95));
    assert!(!x.fast_only());
}

fn decided(id: u64, object: ObjectId, fp: u64, at: SystemTime) -> CandidateRow {
    CandidateRow {
        candidate_id: id,
        run_id: 1,
        object,
        def_hash: 0,
        kind: StoredKind::BTree,
        definition: Definition::default(),
        covering: false,
        fingerprints: vec![(Fingerprint(fp), Role::Primary)],
        status: CandidateStatus::Deferred,
        reason: None,
        benefit_ms_h: None,
        write_ms_h: None,
        est_bytes: None,
        build_seconds: None,
        evidence: crate::autoindex::model::Evidence::default(),
        created_at: at,
    }
}

#[test]
fn cooldown_counts_decisions_on_the_same_object_within_the_window() {
    let slow = || {
        vec![SourcedWindow::client(win(
            1,
            eq_shape(A),
            100,
            150.0,
            200.0,
        ))]
    };
    let mut r = Run::new();
    r.decided = vec![decided(77, A, 1, r.now - Duration::from_secs(23 * 3600))];
    assert_eq!(
        miss_of(&r.go(slow()), 1),
        &Miss::Cooldown { candidate_id: 77 }
    );
    // 25 hours ago: over.
    r.decided = vec![decided(77, A, 1, r.now - Duration::from_secs(25 * 3600))];
    qualified(&r.go(slow()), 1);
    // Another object's decision doesn't hold this one back.
    r.decided = vec![decided(77, B, 1, r.now)];
    qualified(&r.go(slow()), 1);
}

#[test]
fn unsargable_not_indexable_and_no_objects() {
    let unsarg = shape(&serde_json::json!({
        "v": 1, "type": "select",
        "tables": [{"alias": "e", "object_id": A.0, "table": "events"}],
        "predicates": [{"t": "e", "col": "source", "op": "unsargable", "why": "function:LOWER"}],
        "joins": [], "order_by": [], "group_by": [], "select": {"e": ["*"]},
        "limit": false, "writes": null, "or_groups": 0}));
    let set = shape(&serde_json::json!({
        "v": 1, "type": "set", "tables": [], "predicates": [], "joins": [], "order_by": [],
        "group_by": [], "select": {}, "limit": false, "writes": null, "or_groups": 0}));
    let no_obj = shape(&serde_json::json!({
        "v": 1, "type": "select", "tables": [{"alias": "x", "table": "x"}],
        "predicates": [], "joins": [], "order_by": [], "group_by": [], "select": {},
        "limit": false, "writes": null, "or_groups": 0}));
    let mut txn = win(4, eq_shape(A), 100, 150.0, 200.0);
    txn.kind = FpKind::Txn;
    let q = Run::new().go(vec![
        SourcedWindow::client(win(1, unsarg, 80 * 24, 700.0, 900.0)),
        SourcedWindow::client(win(2, set, 100, 150.0, 200.0)),
        SourcedWindow::client(win(3, no_obj, 100, 150.0, 200.0)),
        SourcedWindow::client(txn),
    ]);
    assert_eq!(
        miss_of(&q, 1),
        &Miss::Unsargable {
            reasons: vec!["function:LOWER".into()]
        }
    );
    assert_eq!(miss_of(&q, 1).code(), "function:LOWER");
    assert_eq!(miss_of(&q, 2), &Miss::NotIndexable);
    assert_eq!(miss_of(&q, 3), &Miss::NoObjects);
    assert_eq!(miss_of(&q, 4), &Miss::NotIndexable);
}

#[test]
fn ranking_leaves_out_transactions_and_logic_invocations() {
    let mut w = BTreeMap::new();
    let mut txn = win(1, eq_shape(A), 1000, 1000.0, 1000.0);
    txn.kind = FpKind::Txn;
    w.insert(Fingerprint(1), txn);
    w.insert(Fingerprint(2), win(2, eq_shape(A), 10, 1.0, 1.0));
    w.insert(Fingerprint(3), win(3, eq_shape(A), 10, 2.0, 2.0));
    let r = ranks(&w);
    assert_eq!(r.get(&Fingerprint(1)), None);
    assert_eq!(r[&Fingerprint(3)], 1);
    assert_eq!(r[&Fingerprint(2)], 2);
}

#[tokio::test]
async fn recorder_source_reads_open_buckets_and_watches() {
    use sprig_common::time::ManualClock;
    let rec = Arc::new(Recorder::new(
        sprig_workload::recorder::RecorderConfig::default(),
        Arc::new(ManualClock::new()),
    ));
    let src = RecorderSource(Arc::clone(&rec));
    let now = SystemTime::now();
    assert!(src.window(TENANT, now, now).await.unwrap().is_empty());
    src.watch(TENANT, Fingerprint(5));
}
