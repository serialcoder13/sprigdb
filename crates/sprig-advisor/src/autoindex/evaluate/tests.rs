#![allow(
    clippy::unwrap_used,
    clippy::float_cmp,
    clippy::too_many_lines,
    clippy::needless_pass_by_value,
    clippy::cast_precision_loss
)]

use std::sync::Arc;
use std::time::Duration;

use sprig_common::ids::DcId;
use sprig_common::time::ManualClock;
use sprig_settings::{Level, MemSettings, Value};
use sprig_workload::fingerprint::FpKind;

use super::mem::{MemDecisions, MemStats, MemStore};
use super::synth::{TENANT, directory, from_input, probed, registry, scans, win};
use super::*;
use crate::autoindex::fixtures::{
    DOC_ORDERS, EVENTS, INVOICES, ORDERS, doc_orders, events, events_fps, events_writes,
    fp_example1, fp_example4, invoices, invoices_fps, orders, orders_other_reads, orders_writes,
    shape,
};
use crate::autoindex::model::{AutoIndexId, IndexDef};
use crate::autoindex::qualify::{Miss, SourcedWindow, StaticSource, Trigger};

struct Env {
    settings: MemSettings<'static>,
    dc: DcId,
    windows: Vec<SourcedWindow>,
    dir: BTreeMap<ObjectId, ObjectMeta>,
    stats: MemStats,
    store: MemStore,
    decisions: MemDecisions,
    clock: Arc<ManualClock>,
    evaluator: Evaluator,
}

impl Env {
    fn new(tables: Vec<(ObjectId, TableStats)>) -> Self {
        let clock = Arc::new(ManualClock::new());
        let dir = directory(&tables.iter().map(|(o, _)| (*o, None)).collect::<Vec<_>>());
        Self {
            settings: MemSettings::new(registry()),
            dc: DcId::new("mum1").unwrap(),
            windows: Vec::new(),
            dir,
            stats: MemStats::new(tables),
            store: MemStore::new(),
            decisions: MemDecisions::default(),
            evaluator: Evaluator::new(clock.clone()),
            clock,
        }
    }

    fn add(&mut self, f: &FpInput, limit_slot: Option<u32>) {
        self.windows
            .push(SourcedWindow::client(from_input(f, limit_slot)));
    }

    fn set(&mut self, level: Level, object: Option<ObjectId>, key: &str, v: Value) {
        let mut ctx = SettingCtx::process(&self.dc).for_tenant(TENANT, &[]);
        if let Some(o) = object {
            ctx = ctx.with_db(super::synth::DB).with_object(o);
        }
        self.settings.set_value(level, &ctx, key, &v).unwrap();
    }

    fn cx<'a>(&'a self, src: &'a dyn WorkloadSource) -> EvalCx<'a> {
        EvalCx {
            tenant: TENANT,
            settings: &self.settings,
            ctx: SettingCtx::process(&self.dc).for_tenant(TENANT, &[]),
            workload: src,
            objects: &self.dir,
            stats: &self.stats,
            store: &self.store,
            decisions: &self.decisions,
        }
    }

    async fn run(&self) -> Evaluation {
        let src = StaticSource {
            windows: self.windows.clone(),
            ..StaticSource::default()
        };
        self.evaluator.evaluate(&self.cx(&src)).await.unwrap()
    }
}

fn cols(c: &Candidate) -> Vec<&str> {
    c.def.columns().collect()
}

fn table(ev: &Evaluation, o: ObjectId) -> &TableOutcome {
    ev.tables.iter().find(|t| t.object == o).unwrap()
}

fn outcome<'a>(t: &'a TableOutcome, c: &[&str]) -> &'a CandidateOutcome {
    t.candidates
        .iter()
        .find(|x| cols(&x.candidate) == c)
        .unwrap_or_else(|| panic!("no {c:?} in {:#?}", summary(t)))
}

fn summary(t: &TableOutcome) -> Vec<(Vec<&str>, CandidateStatus, Option<&str>)> {
    t.candidates
        .iter()
        .map(|x| (cols(&x.candidate), x.status, x.reason.as_deref()))
        .collect()
}

fn example1() -> Env {
    let mut env = Env::new(vec![(ORDERS, orders())]);
    env.add(&fp_example1(), Some(2));
    for f in orders_other_reads().iter().chain(&orders_writes()) {
        env.add(f, None);
    }
    env
}

/// Worked example 1: B `(customer_id, created_at)` is chosen over A
/// `(customer_id, status, created_at)`, which pays the status updates.
#[tokio::test]
async fn worked_example_1_chooses_the_variant_that_writes_less() {
    let env = example1();
    let ev = env.run().await;
    let q = ev
        .qualified
        .iter()
        .find(|q| q.fp == Fingerprint(0x9c41_d2e0_7b3a_5f18))
        .unwrap();
    // slow_p95, also top_total_time (1,811 examined per returned).
    assert_eq!(q.fired, [Trigger::SlowP95, Trigger::TopTotalTime]);
    assert!(
        (q.signals.examined_per_returned.unwrap() - 32_600.0 / 18.0).abs() < 1.0,
        "{:?}",
        q.signals
    );
    assert_eq!(ev.qualified.len(), 1, "{:?}", ev.qualified);
    let t = table(&ev, ORDERS);
    assert!(t.error.is_none(), "{:?}", t.error);
    assert_eq!(t.queued.len(), 1, "{:#?}", summary(t));
    let b = &t.queued[0];
    assert_eq!(cols(&b.candidate), ["customer_id", "created_at"]);
    assert_eq!(b.route, Route::ValidateThenRollOut);
    // The chapter's numbers: B 190,500 ms/h, Wc 7,200, 2.24 GB.
    assert!(
        (b.estimate.benefit_ms_h - 190_500.0).abs() < 200.0,
        "{}",
        b.estimate.benefit_ms_h
    );
    assert!((b.estimate.write_ms_h - 7_200.0).abs() < 1.0);
    assert!((b.estimate.bytes / 1e9 - 2.24).abs() < 0.01);
    assert_eq!(b.predictions.len(), 1);
    assert!((b.predictions[0].t_after_ms - 9.3).abs() < 0.2);
    assert_eq!(b.target.table, "orders");
    // A is left for later: B serves its fingerprint better per byte.
    let a = outcome(t, &["customer_id", "status", "created_at"]);
    assert_ne!(a.status, CandidateStatus::Selected);
    // The validation queue: one row in `validating`, named for B.
    let rows = env.store.indexes();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].state, IndexState::Validating);
    assert_eq!(
        rows[0].name,
        IndexDef::btree(ORDERS, &["customer_id", "created_at"]).name()
    );
    assert_eq!(rows[0].candidate_id, b.candidate_id);
    assert_eq!(rows[0].baseline.len(), 1);
    // Candidate rows carry the route and selectivities only.
    let cands = env.store.candidates();
    let row = cands
        .iter()
        .find(|c| c.candidate_id == b.candidate_id)
        .unwrap();
    assert_eq!(row.status, CandidateStatus::Selected);
    assert!(
        row.evidence
            .notes
            .contains(&"route:validate_then_roll_out".to_owned())
    );
    assert!((row.evidence.selectivity["customer_id"] - 1.0 / 120_000.0).abs() < 1e-9);
    // Decisions: one create, with evidence and no sampled values.
    let ds = env.decisions.all();
    let create = ds.iter().find(|d| d.kind == "index.create").unwrap();
    assert_eq!(create.reason_code, "slow_p95");
    assert_eq!(
        rows[0].decision_id,
        Some(1 + ds.iter().position(|d| d.kind == "index.create").unwrap() as u64)
    );
    let text = serde_json::to_string(&ds.iter().map(|d| &d.evidence).collect::<Vec<_>>()).unwrap();
    assert!(
        !text.contains("\"hold\"") && !text.contains("\"open\""),
        "{text}"
    );
    // customer_id has no histogram: analyzed first.
    assert_eq!(
        t.analyzed.as_ref().map(|p| p.columns.clone()),
        Some(vec!["customer_id".to_owned()])
    );
}

fn example2() -> Env {
    let mut env = Env::new(vec![(EVENTS, events())]);
    for f in events_fps().iter().chain(&events_writes()) {
        env.add(f, None);
    }
    env
}

/// Worked example 2: F2 merges into F1's index; F3 keeps primary-key
/// order and is deferred; F4 is unsargable and gets an explanation.
#[tokio::test]
async fn worked_example_2_merges_keeps_order_and_explains_the_unsargable() {
    let env = example2();
    let ev = env.run().await;
    let f4 = ev
        .not_qualified
        .iter()
        .find(|n| n.fp == Fingerprint(0xF4))
        .unwrap();
    assert_eq!(f4.why.code(), "function:LOWER");
    // ... recorded as a skipped candidate, which `sprigctl explain query`
    // reads.
    let skipped: Vec<CandidateRow> = env
        .store
        .candidates()
        .into_iter()
        .filter(|c| c.is_skip() && c.fingerprints.iter().any(|(f, _)| *f == Fingerprint(0xF4)))
        .collect();
    assert_eq!(skipped.len(), 1, "{skipped:?}");
    assert_eq!(skipped[0].reason.as_deref(), Some("function:LOWER"));
    let t = table(&ev, EVENTS);
    assert_eq!(t.queued.len(), 1, "{:#?}", summary(t));
    let m = &t.queued[0];
    assert_eq!(cols(&m.candidate), ["account_id", "kind", "created_at"]);
    assert!(
        m.candidate
            .serves
            .contains(&(Fingerprint(0xF1), Role::Primary))
    );
    assert!(
        m.candidate
            .serves
            .contains(&(Fingerprint(0xF2), Role::Merged))
    );
    // Wc = 800,000 inserts × 0.05 ms.
    assert!((m.estimate.write_ms_h - 40_000.0).abs() < 1.0);
    let f3 = outcome(t, &["account_id"]);
    assert_eq!(f3.status, CandidateStatus::Deferred);
    assert_eq!(f3.reason.as_deref(), Some("marginal_below_min_net"));
    assert!(f3.candidate.evidence.pk_order);
    // F3's deferral starts its cooldown: the next evaluation doesn't
    // re-decide it, nor F1 and F2 (selected).
    env.clock.advance(Duration::from_secs(3600));
    let ev2 = env.run().await;
    for fp in [0xF1, 0xF2, 0xF3] {
        assert!(
            ev2.not_qualified
                .iter()
                .any(|n| n.fp == Fingerprint(fp) && matches!(n.why, Miss::Cooldown { .. })),
            "{fp:x}: {:?}",
            ev2.not_qualified
        );
    }
    assert!(ev2.tables.is_empty());
}

/// Worked example 3: C1 `(state, due_date)` goes to validation (where it
/// is rejected for F6's regression and retried covering: chapter 13's
/// validation, not evaluation).
#[tokio::test]
async fn worked_example_3_proposes_c1_for_validation() {
    let mut env = Env::new(vec![(INVOICES, invoices())]);
    for f in &invoices_fps() {
        env.add(f, None);
    }
    let ev = env.run().await;
    let t = table(&ev, INVOICES);
    let c1 = t
        .queued
        .iter()
        .find(|q| cols(&q.candidate) == ["state", "due_date"])
        .unwrap_or_else(|| panic!("{:#?}", summary(t)));
    assert_eq!(c1.route, Route::ValidateThenRollOut);
    assert!(c1.candidate.serves_fp(Fingerprint(0xF5)));
    // F6 (a range on due_date, GROUP BY state, 40 runs of 1.8 s) is
    // among the top 10 by total time and examines 450,000 rows per row
    // returned, so it qualifies too; its grouping candidate `(state)` is a
    // prefix of C1 and merges into it. Validation replays it either way.
    assert!(
        c1.candidate
            .serves
            .contains(&(Fingerprint(0xF6), Role::Merged)),
        "{:?}",
        c1.candidate.serves
    );
    assert_eq!(
        c1.columns_read[&Fingerprint(0xF5)],
        ["amount", "due_date", "id", "state"]
    );
    assert!(
        (c1.candidate.evidence.selectivity["state"] - 0.005).abs() < 1e-9,
        "overdue at 0.5%"
    );
}

/// Worked example 4: a MongoDB shape's field columns get
/// `(_sprig_p_customer_id, _sprig_p_createdat)`.
#[tokio::test]
async fn worked_example_4_field_columns() {
    let mut env = Env::new(vec![(DOC_ORDERS, doc_orders())]);
    let mut w = from_input(&fp_example4(), None);
    w.kind = FpKind::Mongo;
    env.windows.push(SourcedWindow::client(w));
    let ev = env.run().await;
    let t = table(&ev, DOC_ORDERS);
    assert_eq!(t.queued.len(), 1, "{:#?}", summary(t));
    assert_eq!(
        cols(&t.queued[0].candidate),
        ["_sprig_p_customer_id", "_sprig_p_createdat"]
    );
}

#[tokio::test]
async fn recommend_mode_validates_where_it_can_else_recommends() {
    let mut env = example1();
    env.set(
        Level::Tenant,
        None,
        "auto_index.mode",
        Value::from("recommend"),
    );
    let ev = env.run().await;
    let q = &table(&ev, ORDERS).queued[0];
    assert_eq!(q.route, Route::ValidateThenRecommend);
    assert_eq!(env.store.indexes()[0].state, IndexState::Validating);
    // The loop stops it at `recommended` once accepted.
    let observed = env.store.indexes()[0].observed.clone().unwrap();
    assert_eq!(observed["route"], serde_json::json!("recommend"));
    // The validation plan's inputs travel with the row.
    let plan = super::PlanInputs::of(Some(&observed)).unwrap();
    assert!(!plan.predictions.is_empty());
    assert!(plan.target.is_some());
    let d = env.decisions.all();
    let rec = d.iter().find(|d| d.kind == "index.recommend").unwrap();
    assert_eq!(rec.mode, "recommended");

    // No validation node: straight to `recommended`.
    let mut env = example1();
    env.set(
        Level::Tenant,
        None,
        "auto_index.mode",
        Value::from("recommend"),
    );
    env.dir.get_mut(&ORDERS).unwrap().validation_node = false;
    let ev = env.run().await;
    let q = &table(&ev, ORDERS).queued[0];
    assert_eq!(q.route, Route::Recommend);
    assert_eq!(env.store.indexes()[0].state, IndexState::Recommended);
    assert!(
        env.store
            .candidates()
            .iter()
            .any(|c| c.status == CandidateStatus::Recommended)
    );
}

#[tokio::test]
async fn mode_off_evaluates_nothing() {
    let mut env = example1();
    env.set(Level::Tenant, None, "auto_index.mode", Value::from("off"));
    let ev = env.run().await;
    assert_eq!(ev.mode, Mode::Off);
    assert!(ev.run_id.is_none() && ev.tables.is_empty() && ev.qualified.is_empty());
    assert!(env.store.candidates().is_empty());
    // Off on the table only.
    let mut env = example1();
    env.set(
        Level::Object,
        Some(ORDERS),
        "auto_index.mode",
        Value::from("off"),
    );
    let ev = env.run().await;
    assert!(ev.tables.is_empty());
    assert!(ev.not_qualified.iter().any(|n| n.why == Miss::ModeOff));
}

#[tokio::test]
async fn studio_only_runs_do_not_qualify() {
    let mut env = Env::new(vec![(ORDERS, orders())]);
    env.windows
        .push(SourcedWindow::studio(from_input(&fp_example1(), Some(2))));
    let ev = env.run().await;
    assert!(ev.qualified.is_empty());
    assert_eq!(ev.not_qualified[0].why, Miss::ExcludedSource);
    // Included by the setting on the table.
    env.set(
        Level::Object,
        Some(ORDERS),
        "auto_index.include_studio",
        Value::Bool(true),
    );
    let ev = env.run().await;
    assert_eq!(ev.qualified.len(), 1);
}

/// Example 1's fingerprint as a few large scans from `sessions` sessions,
/// with ten busier statements on another table so only the fast trigger
/// fires.
fn scan_env(table_bytes: Option<u64>, sessions: u64) -> Env {
    let mut env = Env::new(vec![(ORDERS, orders())]);
    let mut f = fp_example1();
    f.runs = 5;
    f.examined_per_run = 2_000_000.0;
    f.mean_ms = 2_000.0;
    f.p95_ms = 2_400.0;
    let w = from_input(&f, Some(2));
    env.windows.push(SourcedWindow::client(scans(
        probed(w, 5, 2e6, 18.0),
        5,
        sessions,
    )));
    let other = ObjectId(99);
    for i in 0..10 {
        let busy = probed(
            win(0x100 + i, eq_on(other), 100_000, 1.0, 1.0),
            100,
            1.0,
            1.0,
        );
        env.windows.push(SourcedWindow::client(busy));
    }
    env.dir.get_mut(&ORDERS).unwrap().data_bytes = table_bytes;
    env
}

fn eq_on(o: ObjectId) -> Arc<sprig_workload::shape::Shape> {
    shape(&serde_json::json!({
        "v": 1, "type": "select",
        "tables": [{"alias": "x", "object_id": o.0, "table": "x"}],
        "predicates": [{"t": "x", "col": "id", "op": "eq", "args": [0]}],
        "joins": [], "order_by": [], "group_by": [], "select": {"x": ["*"]},
        "limit": false, "writes": null, "or_groups": 0}))
}

#[tokio::test]
async fn fast_trigger_builds_on_a_10_gb_table_and_recommends_on_150_gb() {
    let env = scan_env(Some(10_000_000_000), 2);
    let ev = env.run().await;
    let q = ev.qualified.iter().find(|q| q.object == ORDERS).unwrap();
    assert_eq!(q.trigger, Trigger::LargeScan);
    let t = table(&ev, ORDERS);
    assert_eq!(t.queued.len(), 1, "{:#?}", summary(t));
    assert_eq!(t.queued[0].route, Route::ValidateThenRollOut);

    let env = scan_env(Some(150_000_000_000), 2);
    let ev = env.run().await;
    let t = table(&ev, ORDERS);
    assert_eq!(t.queued[0].route, Route::ValidateThenRecommend);
    assert!(t.queued[0].advise);

    // Size unknown to the catalog: the statistics decide (31.5 GB here,
    // so it builds; above the limit it recommends).
    let env = scan_env(None, 2);
    assert_eq!(
        table(&env.run().await, ORDERS).queued[0].route,
        Route::ValidateThenRollOut
    );
    let mut env = scan_env(None, 2);
    env.set(
        Level::Object,
        Some(ORDERS),
        "auto_index.scan_recommend_above_gb",
        Value::Int(20),
    );
    assert_eq!(
        table(&env.run().await, ORDERS).queued[0].route,
        Route::ValidateThenRecommend
    );

    // One session: nothing.
    let env = scan_env(Some(10_000_000_000), 1);
    let ev = env.run().await;
    assert!(ev.qualified.iter().all(|q| q.object != ORDERS));
}

fn auto_row(def: &IndexDef, state: IndexState, value_ms_h: f64, at: SystemTime) -> AutoIndexRow {
    AutoIndexRow {
        index_id: AutoIndexId(0),
        object: def.object,
        name: def.name(),
        model: DataModel::Sql,
        kind: StoredKind::BTree,
        definition: Definition::of(def),
        state,
        prior_state: None,
        candidate_id: 0,
        validation_id: None,
        decision_id: None,
        est_benefit_ms_h: value_ms_h,
        est_write_ms_h: 0.0,
        est_bytes: 1_000_000_000,
        actual_bytes: None,
        shards: BTreeMap::new(),
        baseline: BTreeMap::new(),
        observed: None,
        last_used_at: None,
        drop_reason: None,
        created_at: at,
        state_changed_at: at,
    }
}

#[tokio::test]
async fn fast_trigger_never_displaces_an_active_automatic_index() {
    let mut env = scan_env(Some(10_000_000_000), 2);
    env.set(
        Level::Object,
        Some(ORDERS),
        "auto_index.max_per_table",
        Value::Int(1),
    );
    let old = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    // A low-value active automatic index fills the table's budget.
    let def = IndexDef::btree(ORDERS, &["total"]);
    env.store
        .add_index(auto_row(&def, IndexState::Active, 1.0, old));
    let ev = env.run().await;
    let t = table(&ev, ORDERS);
    assert!(t.queued.is_empty(), "{:#?}", summary(t));
    assert!(
        t.candidates
            .iter()
            .any(|c| c.reason.as_deref() == Some("fast_trigger_no_displace")),
        "{:#?}",
        summary(t)
    );
    // The slow trigger's candidate does replace it.
    let mut env = example1();
    env.set(
        Level::Object,
        Some(ORDERS),
        "auto_index.max_per_table",
        Value::Int(1),
    );
    env.store
        .add_index(auto_row(&def, IndexState::Active, 1.0, old));
    let ev = env.run().await;
    let t = table(&ev, ORDERS);
    assert_eq!(t.queued.len(), 1, "{:#?}", summary(t));
    assert_eq!(t.queued[0].replaces.as_deref(), Some(def.name().as_str()));
}

#[tokio::test]
async fn existing_indexes_cover_and_live_rows_are_not_proposed_twice() {
    // A user index equal to B (and one serving A) prevents creation.
    let mut t = orders();
    for (name, c) in [
        ("idx_cust_created", vec!["customer_id", "created_at"]),
        (
            "idx_cust_status",
            vec!["status", "customer_id", "created_at"],
        ),
    ] {
        t.indexes.push(IndexInfo {
            name: name.into(),
            parts: c
                .iter()
                .map(|c| crate::autoindex::model::KeyPart::col(c))
                .collect(),
            unique: false,
            origin: Origin::User,
            ignored: false,
            usable: true,
        });
    }
    let mut env = Env::new(vec![(ORDERS, t)]);
    env.add(&fp_example1(), Some(2));
    let ev = env.run().await;
    let t = table(&ev, ORDERS);
    for (c, by) in [
        (
            &["customer_id", "created_at"][..],
            "covered_by:idx_cust_created",
        ),
        (
            &["customer_id", "status", "created_at"][..],
            "covered_by:idx_cust_status",
        ),
    ] {
        let x = outcome(t, c);
        assert_eq!(
            (x.status, x.reason.as_deref()),
            (CandidateStatus::Covered, Some(by))
        );
    }
    assert!(
        t.queued.iter().all(|q| q.candidate.covering),
        "{:#?}",
        summary(t)
    );
    // B already validating: covered by its own row, not proposed again.
    let env = example1();
    let def = IndexDef::btree(ORDERS, &["customer_id", "created_at"]);
    env.store.add_index(auto_row(
        &def,
        IndexState::Validating,
        0.0,
        env.clock.wall(),
    ));
    let ev = env.run().await;
    let t = table(&ev, ORDERS);
    let b = outcome(t, &["customer_id", "created_at"]);
    assert_eq!(b.reason.as_deref(), Some("exists:validating"));
    assert!(
        t.queued
            .iter()
            .all(|q| cols(&q.candidate) != ["customer_id", "created_at"])
    );
}

#[tokio::test]
async fn old_rows_block_or_are_reused() {
    let def = IndexDef::btree(ORDERS, &["customer_id", "created_at"]);
    // Dropped for no gain 3 days ago: blocked for recreate_block_days.
    let env = example1();
    let mut r = auto_row(
        &def,
        IndexState::Dropped,
        0.0,
        env.clock.wall() - Duration::from_secs(3 * 86_400),
    );
    r.drop_reason = Some(DropReason::NoGain);
    env.store.add_index(r);
    let ev = env.run().await;
    let b = outcome(table(&ev, ORDERS), &["customer_id", "created_at"]);
    assert_eq!(b.status, CandidateStatus::Rejected);
    assert_eq!(b.reason.as_deref(), Some("recreate_blocked:no_gain"));
    // Rejected by validation 3 days ago: blocked too.
    let env = example1();
    env.store.add_index(auto_row(
        &def,
        IndexState::Rejected,
        0.0,
        env.clock.wall() - Duration::from_secs(3 * 86_400),
    ));
    let ev = env.run().await;
    let b = outcome(table(&ev, ORDERS), &["customer_id", "created_at"]);
    assert_eq!(b.reason.as_deref(), Some("recreate_blocked:rejected"));
    // Dropped as unused: the row is reused for the new proposal.
    let env = example1();
    let mut r = auto_row(
        &def,
        IndexState::Dropped,
        0.0,
        env.clock.wall() - Duration::from_secs(86_400),
    );
    r.drop_reason = Some(DropReason::Unused);
    let id = env.store.add_index(r);
    let ev = env.run().await;
    let q = &table(&ev, ORDERS).queued[0];
    assert_eq!(q.index_id, id);
    let rows = env.store.indexes();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].state, IndexState::Validating);
    assert!(rows[0].drop_reason.is_none());
}

#[tokio::test]
async fn the_day_cap_defers_builds_but_not_recommendations() {
    let mut env = example1();
    env.set(
        Level::Tenant,
        None,
        "auto_index.max_new_per_day",
        Value::Int(1),
    );
    // One index already created today.
    let def = IndexDef::btree(ORDERS, &["total"]);
    env.store.add_index(auto_row(
        &def,
        IndexState::RollingOut,
        0.0,
        env.clock.wall(),
    ));
    let ev = env.run().await;
    let t = table(&ev, ORDERS);
    assert!(t.queued.is_empty());
    let b = outcome(t, &["customer_id", "created_at"]);
    assert_eq!(
        (b.status, b.reason.as_deref()),
        (CandidateStatus::Deferred, Some("day_cap"))
    );
}

#[tokio::test]
async fn fingerprints_without_probes_wait() {
    let mut env = Env::new(vec![(ORDERS, orders())]);
    let mut w = from_input(&fp_example1(), Some(2));
    w.stats.examined_samples = 0;
    w.stats.rows_examined = 0;
    w.stats.returned_sampled = 0;
    env.windows.push(SourcedWindow::client(w));
    let ev = env.run().await;
    let t = table(&ev, ORDERS);
    assert!(t.candidates.is_empty());
    assert_eq!(t.skipped[0].reason, "no_probes");
    // A skipped candidate row records it, without starting a cooldown: the
    // next evaluation looks again (and records it once per window).
    let rows = env.store.candidates();
    assert!(
        rows.iter()
            .any(|c| c.is_skip() && c.reason.as_deref() == Some("no_probes"))
    );
    let ev2 = env.run().await;
    assert!(
        !ev2.not_qualified
            .iter()
            .any(|n| matches!(n.why, Miss::Cooldown { .. }))
    );
    assert_eq!(env.store.candidates().len(), rows.len());
    // The decision log says why.
    assert!(
        env.decisions
            .all()
            .iter()
            .any(|d| d.outcome == "no_candidate" && d.reason_code == "no_probes")
    );
}

#[tokio::test]
async fn evaluations_are_paced_by_the_interval() {
    let env = example1();
    let src = StaticSource {
        windows: env.windows.clone(),
        ..StaticSource::default()
    };
    let cx = env.cx(&src);
    assert!(env.evaluator.evaluate_if_due(&cx).await.unwrap().is_some());
    assert!(env.evaluator.evaluate_if_due(&cx).await.unwrap().is_none());
    env.clock.advance(Duration::from_secs(59 * 60));
    assert!(env.evaluator.evaluate_if_due(&cx).await.unwrap().is_none());
    env.clock.advance(Duration::from_secs(60));
    let ev = env.evaluator.evaluate_if_due(&cx).await.unwrap().unwrap();
    // The qualified fingerprint was watched, and is in its cooldown now.
    assert!(
        src.watched
            .lock()
            .unwrap()
            .contains(&(TENANT, Fingerprint(0x9c41_d2e0_7b3a_5f18)))
    );
    assert!(ev.qualified.is_empty());
}

#[test]
fn sample_values_map_to_params() {
    use sprig_workload::fingerprint::LiteralKind as K;
    let v = |kind, t: &str| SampleValue {
        kind,
        text: t.as_bytes().to_vec(),
    };
    assert_eq!(param(&v(K::Number, "-42")), Param::Num(-42.0));
    assert_eq!(param(&v(K::String, "'it''s'")), Param::Text("it's".into()));
    assert_eq!(
        param(&v(K::Temporal, "DATE '2026-11-01'")),
        Param::Text("2026-11-01".into())
    );
    assert_eq!(param(&v(K::Bool, "TRUE")), Param::Num(1.0));
    assert_eq!(param(&v(K::Hex, "0x0A")), Param::Null);
    assert_eq!(param(&v(K::Param, "?")), Param::Null);
    let s = SampledLiterals {
        slots: vec![
            SampledSlot::One(v(K::Number, "7")),
            SampledSlot::List(vec![v(K::String, "'a'"), v(K::String, "'b'")]),
        ],
    };
    assert_eq!(
        literals(&s),
        [
            Literal::Value(Param::Num(7.0)),
            Literal::List(vec![Param::Text("a".into()), Param::Text("b".into())])
        ]
    );
    // The mapping of the workload agent's table.
    let w = from_input(&fp_example1(), Some(2));
    let f = fp_input(&w, false);
    assert_eq!(f.runs, 18_240);
    assert!((f.mean_ms - 260.0).abs() < 1e-6);
    assert!(f.p95_ms >= 412.0 && f.p95_ms <= 412.0 * 1.19);
    assert!((f.examined_per_run - 32_600.0).abs() < 1e-6);
    assert!((f.returned_per_run - 18.0).abs() < 1e-6);
    assert_eq!(f.limit, Some(20));
    assert_eq!(f.in_list_max, Some(2));
    assert_eq!(f.samples, fp_example1().samples);
}

/// Windows by `from`: the full window before `cut`, only `after` from it.
struct SplitSource {
    cut: SystemTime,
    before: Vec<SourcedWindow>,
    after: Vec<SourcedWindow>,
}

#[async_trait::async_trait]
impl WorkloadSource for SplitSource {
    async fn window(
        &self,
        _tenant: TenantId,
        from: SystemTime,
        _to: SystemTime,
    ) -> Result<Vec<SourcedWindow>> {
        Ok(if from < self.cut {
            self.before.clone()
        } else {
            self.after.clone()
        })
    }
    fn watch(&self, _tenant: TenantId, _fp: Fingerprint) {}
}

/// M0.4 follow-up: an index built inside the window that serves example
/// 1's fingerprint is judged on the runs since its build, so the slow runs
/// from before it don't propose redundant variants beside it.
#[tokio::test]
async fn a_served_fingerprint_counts_only_runs_since_its_index() {
    use sprig_common::time::Clock;
    let env = example1();
    let now = env.clock.wall();
    let built = now - Duration::from_secs(600);
    let fp = fp_example1().id;
    let b = IndexDef::btree(ORDERS, &["customer_id", "created_at"]);
    let mut row = auto_row(&b, IndexState::Active, 500.0, built);
    row.baseline.insert(
        fp.to_string(),
        Baseline {
            runs: 1000,
            p95_ms: 400.0,
            mean_ms: 260.0,
        },
    );
    env.store.add_index(row.clone());
    // Since the build the fingerprint hasn't run slow: no runs at all here.
    let after: Vec<SourcedWindow> = env
        .windows
        .iter()
        .filter(|w| w.window.id != fp)
        .cloned()
        .collect();
    let src = SplitSource {
        cut: built,
        before: env.windows.clone(),
        after,
    };
    let ev = env.evaluator.evaluate(&env.cx(&src)).await.unwrap();
    assert!(
        ev.qualified.iter().all(|q| q.fp != fp),
        "qualified on runs from before its index: {:?}",
        ev.qualified
    );
    assert!(ev.tables.iter().all(|t| t.queued.is_empty()));
    // An index built before the window began changes nothing: the window
    // is the fingerprint's runs.
    let env = example1();
    let mut old = row;
    old.state_changed_at = now - Duration::from_secs(48 * 3600);
    old.created_at = old.state_changed_at;
    env.store.add_index(old);
    let src = SplitSource {
        cut: now,
        before: env.windows.clone(),
        after: Vec::new(),
    };
    let ev = env.evaluator.evaluate(&env.cx(&src)).await.unwrap();
    assert!(ev.qualified.iter().any(|q| q.fp == fp));
}
