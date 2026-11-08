//! Live tests against the Compose MariaDB (11.8).
//!
//! `SPRIG_TEST_MARIADB_SINGLE` (for example
//! `mysql://root:sprig-dev-root@127.0.0.1:13301`) names the server; without
//! it every case is skipped with a message. Each case works in its own
//! schema `__sprig_test_autoindex_<pid>_<nanos>_<n>`, dropped afterwards,
//! also when the case fails.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::float_cmp,
    clippy::many_single_char_names
)]

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::FutureExt;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, Pool};
use sprig_catalog::migrate::MigrateOpts;
use sprig_catalog::{Migrator, Store};
use sprig_common::ids::{Fingerprint, ObjectId};
use sprig_common::names::quote;
use sprig_common::time::SystemClock;

use crate::autoindex::candidates::explain::{coverage_status, explain_uses};
use crate::autoindex::candidates::{
    FpInput, Generator, Literal, access, covered_by, weigh_samples,
};
use crate::autoindex::model::{AutoIndexId, Evidence, IndexDef, Origin, Role};
use crate::autoindex::stats::analyze::AnalyzeReason;
use crate::autoindex::stats::{
    AnalyzeLimits, AnalyzeThrottle, Key, KeyClass, Param, SelSource, StatsReader, analyze,
    plan_analyze, weigh,
};
use crate::autoindex::store::{
    AUTOINDEX_MODULE, AutoIndexRow, AutoIndexStore, Baseline, CandidateRow, CandidateStatus,
    Definition, DropReason, IndexState, Model, OriginRow, ShardBuild, StateChange, StoredKind,
    ValidationMethod, ValidationRow, Verdict,
};

const MARIADB_ENV: &str = "SPRIG_TEST_MARIADB_SINGLE";

fn unique() -> u64 {
    static N: AtomicU64 = AtomicU64::new(0);
    N.fetch_add(1, Ordering::Relaxed)
}

/// Runs `body` with a connection whose default database is a fresh schema;
/// drops the schema afterwards. Skips without `SPRIG_TEST_MARIADB_SINGLE`.
async fn with_schema<F, Fut>(case: &str, body: F)
where
    F: FnOnce(Conn, String) -> Fut,
    Fut: Future<Output = ()>,
{
    let Some(url) = std::env::var(MARIADB_ENV)
        .ok()
        .filter(|v| !v.trim().is_empty())
    else {
        eprintln!("skipped {case}: set {MARIADB_ENV} to run it");
        return;
    };
    let pool = Pool::new(Opts::from_url(&url).unwrap());
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let schema = format!(
        "__sprig_test_autoindex_{}_{nanos:x}_{}",
        std::process::id(),
        unique()
    );
    let mut conn = pool.get_conn().await.unwrap();
    conn.query_drop(format!("CREATE DATABASE {}", quote(&schema)))
        .await
        .unwrap();
    conn.query_drop(format!("USE {}", quote(&schema)))
        .await
        .unwrap();
    let r = AssertUnwindSafe(body(conn, schema.clone()))
        .catch_unwind()
        .await;
    let mut c = pool.get_conn().await.unwrap();
    c.query_drop(format!("DROP DATABASE IF EXISTS {}", quote(&schema)))
        .await
        .unwrap();
    drop(c);
    pool.disconnect().await.unwrap();
    if let Err(p) = r {
        std::panic::resume_unwind(p);
    }
}

const TABLE: &str = "CREATE TABLE t (
  id          INT PRIMARY KEY AUTO_INCREMENT,
  status      VARCHAR(16) COLLATE utf8mb4_uca1400_ai_ci,
  customer_id INT,
  created_at  DATETIME,
  amount      DECIMAL(10,2),
  v           VARCHAR(8),
  note        TEXT,
  KEY k_cust (customer_id, status),
  KEY k_status (status)
) ENGINE=InnoDB";

/// 20,000 rows: status `paid` 95%, `open` 4%, `hold` 1%; 500 customers;
/// one row a minute from 2026-01-01; `v` NULL in half the rows and `a` in
/// 10% of all rows.
const FILL: &str = "INSERT INTO t (status, customer_id, created_at, amount, v, note)
  SELECT ELT(1 + (seq % 20 = 0) + (seq % 100 = 0), 'paid', 'open', 'hold'), seq % 500,
         '2026-01-01' + INTERVAL seq MINUTE, seq / 10,
         IF(seq % 2 = 0, NULL, IF(seq % 10 = 1, 'a', 'b')), 'x'
  FROM seq_1_to_20000";

fn close(a: f64, b: f64, tol: f64) -> bool {
    (a - b).abs() <= tol
}

#[tokio::test]
async fn json_hb_format_on_11_8() {
    with_schema("json_hb_format_on_11_8", |mut c, schema| async move {
        c.query_drop(TABLE).await.unwrap();
        c.query_drop(FILL).await.unwrap();
        let version: String = c.query_first("SELECT VERSION()").await.unwrap().unwrap();
        assert!(version.starts_with("11.8"), "{version}");

        let reader = StatsReader::default();
        let before = reader.read(&mut c, &schema, "t").await.unwrap();
        assert!(before.eits);
        assert!(before.column_stats.is_empty());
        assert_eq!(before.primary_key, ["id"]);
        assert_eq!(before.indexes.len(), 3);
        assert_eq!(before.indexes[0].origin, Origin::System);
        assert!(before.columns["note"].lob);
        assert_eq!(before.columns["status"].class, Some(KeyClass::Text));

        // The plan asks for exactly the needed columns; TEXT is skipped.
        let limits = AnalyzeLimits::default();
        let plan = plan_analyze(
            &before,
            &["customer_id", "note"],
            &["status", "created_at", "v"],
            &limits,
        )
        .unwrap();
        assert_eq!(plan.columns, ["created_at", "customer_id", "status", "v"]);
        let throttle = AnalyzeThrottle::new(SystemClock, limits);
        assert!(throttle.admit(1, &plan));
        assert!(!throttle.admit(1, &plan));
        analyze(&mut c, &plan).await.unwrap();
        // analyze_sample_percentage is per session (open question 2).
        let pct: f64 = c
            .query_first("SELECT @@session.analyze_sample_percentage")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pct, 0.0);

        let t = reader.read(&mut c, &schema, "t").await.unwrap();
        assert_eq!(t.cardinality, Some(20_000));
        assert!(t.n_rows.is_some());
        assert!(!t.is_stale(0.2));
        // Only the planned columns have statistics.
        assert!(!t.column_stats.contains_key("amount"));
        let status = &t.column_stats["status"];
        let h = status.histogram.as_ref().unwrap();
        // One bucket per value; the last has `end`; collected_at is set.
        assert_eq!(h.len(), 3);
        assert_eq!(h.buckets[0].start_text, "hold");
        assert_eq!(h.end_text, "paid");
        assert!(h.collected_at.is_some());
        // Equality, IN, and case-insensitive values through WEIGHT_STRING.
        let weights = weigh(
            &mut c,
            "utf8mb4",
            "utf8mb4_uca1400_ai_ci",
            &["open", "Open", "hold"],
        )
        .await
        .unwrap();
        assert_eq!(weights[0], weights[1]);
        let open = Key::Bytes(weights[1].clone());
        let hold = Key::Bytes(weights[2].clone());
        let s = t.sel_eq("status", std::slice::from_ref(&open));
        assert_eq!(s.source, SelSource::Histogram);
        assert!(close(s.value, 0.04, 1e-9), "{s:?}");
        let s = t.sel_in("status", &[open, hold], 2);
        assert!(close(s.value, 0.05, 1e-9), "{s:?}");
        // avg_frequency: 40 rows per customer.
        let s = t.sel_eq("customer_id", &[]);
        assert_eq!(s.source, SelSource::AvgFrequency);
        assert!(close(s.value, 40.0 / 20_000.0, 1e-9));
        // A numeric point from the histogram: about 40 rows of 20,000.
        let s = t.sel_eq("customer_id", &[Key::Num(7.0)]);
        assert_eq!(s.source, SelSource::Histogram);
        assert!(close(s.value, 0.002, 0.0015), "{s:?}");
        // A temporal range: the first 10,000 minutes are half the rows.
        let hi = t
            .key("created_at", &Param::Text("2026-01-07 22:40:00".into()))
            .unwrap();
        let s = t.sel_range("created_at", None, Some(&hi));
        assert!(close(s.value, 0.5, 0.01), "{s:?}");
        // NULLs: `v` is half NULL; `a` is 10% of all rows (20% of non-NULL).
        assert!(close(t.column_stats["v"].nulls_ratio, 0.5, 1e-9));
        let a = Key::Bytes(
            weigh(
                &mut c,
                "utf8mb4",
                t.columns["v"].collation.as_deref().unwrap(),
                &["a"],
            )
            .await
            .unwrap()
            .remove(0),
        );
        assert!(close(
            t.sel_eq("v", std::slice::from_ref(&a)).value,
            0.1,
            1e-9
        ));
        assert!(close(t.sel_eq("v", &[]).value, 0.25, 1e-9));
        // `INDEXES ()` collected no index statistics; a plan naming k_cust
        // does.
        assert!(t.index_stats.is_empty());
        let mut ip = plan.clone();
        ip.columns.clear();
        ip.indexes = vec!["k_cust".into()];
        analyze(&mut c, &ip).await.unwrap();
        let t = reader.read(&mut c, &schema, "t").await.unwrap();
        // index_stats: k_cust's prefixes, and the prefix rule.
        let ks = &t.index_stats["k_cust"];
        assert!(close(ks[0], 40.0, 1e-9));
        let s = t.sel_prefix(&["status", "customer_id"], &[0.5, 0.5]);
        assert_eq!(s.source, SelSource::IndexStats);
        // The table grows by half: stale.
        c.query_drop(
            "INSERT INTO t (status, customer_id, created_at) \
             SELECT 'paid', seq, NOW() FROM seq_1_to_15000",
        )
        .await
        .unwrap();
        c.query_drop(format!("ANALYZE TABLE {}.`t`", quote(&schema)))
            .await
            .unwrap();
        let t2 = reader.read(&mut c, &schema, "t").await.unwrap();
        assert!(t2.is_stale(0.2), "{:?} {:?}", t2.cardinality, t2.n_rows);
        let p = plan_analyze(&t2, &["status"], &[], &limits).unwrap();
        assert_eq!(p.reason, AnalyzeReason::Stale);
    })
    .await;
}

fn fp_on_t(preds: &serde_json::Value, select: &serde_json::Value) -> FpInput {
    FpInput {
        id: Fingerprint(0xabc),
        shape: std::sync::Arc::new(
            sprig_workload::shape::Shape::from_json(
                &serde_json::json!({
                    "v": 1, "type": "select",
                    "tables": [{"alias": "t", "object_id": 7, "table": "t"}],
                    "predicates": preds, "joins": [], "order_by": [], "group_by": [],
                    "select": {"t": select}, "limit": false, "writes": null, "or_groups": 0})
                .to_string(),
            )
            .unwrap(),
        ),
        runs: 1000,
        mean_ms: 50.0,
        p95_ms: 120.0,
        examined_per_run: 20_000.0,
        returned_per_run: 40.0,
        affected_per_run: 0.0,
        limit: None,
        in_list_max: None,
        samples: vec![vec![
            Literal::Value(Param::Num(7.0)),
            Literal::List(vec![Param::Text("open".into()), Param::Text("hold".into())]),
        ]],
        fast_only: false,
    }
}

#[tokio::test]
async fn explain_coverage_check_on_11_8() {
    with_schema(
        "explain_coverage_check_on_11_8",
        |mut c, schema| async move {
            c.query_drop(TABLE).await.unwrap();
            c.query_drop(FILL).await.unwrap();
            c.query_drop("ANALYZE TABLE t PERSISTENT FOR ALL")
                .await
                .unwrap();
            let t = StatsReader::default()
                .read(&mut c, &schema, "t")
                .await
                .unwrap();
            // customer_id = ? AND status IN (...): k_cust covers the candidate
            // on paper, and the optimizer uses it.
            let f = fp_on_t(
                &serde_json::json!([
                {"t": "t", "col": "customer_id", "op": "eq", "args": [0]},
                {"t": "t", "col": "status", "op": "in", "list": true, "args": [1]}]),
                &serde_json::json!(["id"]),
            );
            // Text values compare as the column's weight strings, like the
            // reweighed histogram bounds.
            let mut f = f;
            weigh_samples(&mut c, "t", &t, &mut f).await.unwrap();
            let acc = access(&f, "t", ObjectId(7), &t);
            let s = acc.preds.iter().find(|p| p.column == "status").unwrap();
            assert!(close(s.sel, 0.05, 1e-9), "{s:?}");
            let cands = Generator::default().for_table(&acc, &t);
            let cand = &cands[0];
            assert_eq!(
                cand.def.columns().collect::<Vec<_>>(),
                ["customer_id", "status"]
            );
            let e = covered_by(cand, &t.indexes).unwrap();
            assert_eq!(e.name, "k_cust");
            let o = explain_uses(
                &mut c,
                "SELECT id FROM t WHERE customer_id = 7 AND status IN ('open','hold')",
                &e.name,
            )
            .await
            .unwrap();
            assert!(o.uses);
            assert_eq!(
                coverage_status(&e.name, &[o], false).0,
                CandidateStatus::Covered
            );

            // status = 'paid' is 95% of the rows: k_status covers the
            // candidate on paper but the optimizer scans the table instead.
            let f = fp_on_t(
                &serde_json::json!([{"t": "t", "col": "status", "op": "eq", "args": [0]}]),
                &serde_json::json!(["*"]),
            );
            let cand = crate::autoindex::model::Candidate {
                def: IndexDef::btree(ObjectId(7), &["status"]),
                covering: false,
                serves: vec![(f.id, Role::Primary)],
                evidence: Evidence {
                    parts: vec![crate::autoindex::model::PartSource::Eq],
                    ..Evidence::default()
                },
            };
            let e = covered_by(&cand, &t.indexes).unwrap();
            assert_eq!(e.name, "k_status");
            let o = explain_uses(&mut c, "SELECT * FROM t WHERE status = 'paid'", &e.name)
                .await
                .unwrap();
            assert!(!o.uses, "the optimizer avoids k_status for 95% of rows");
            assert_eq!(
                coverage_status(&e.name, std::slice::from_ref(&o), false),
                (CandidateStatus::Analyze, "analyze_for:k_status".into())
            );
            assert_eq!(
                coverage_status(&e.name, &[o], true).1,
                "optimizer_avoids:k_status"
            );
            // An IGNORED index doesn't count.
            c.query_drop("ALTER TABLE t ALTER INDEX k_cust IGNORED")
                .await
                .unwrap();
            let t = StatsReader::default()
                .read(&mut c, &schema, "t")
                .await
                .unwrap();
            let k = t.indexes.iter().find(|i| i.name == "k_cust").unwrap();
            assert!(k.ignored && !k.counts());
        },
    )
    .await;
}

#[tokio::test]
async fn store_round_trip() {
    with_schema("store_round_trip", |mut c, schema| async move {
        Migrator::new(&[AUTOINDEX_MODULE])
            .unwrap()
            .run(&mut c, &schema, &MigrateOpts::default())
            .await
            .unwrap();
        // Idempotent.
        Migrator::new(&[AUTOINDEX_MODULE])
            .unwrap()
            .run(&mut c, &schema, &MigrateOpts::default())
            .await
            .unwrap();
        let core = Store::new(schema.clone());
        let s = AutoIndexStore::of(&core);
        let run1 = s.next_run_id(&core, &mut c).await.unwrap();
        let run2 = s.next_run_id(&core, &mut c).await.unwrap();
        assert_eq!(run2, run1 + 1);

        let now = SystemTime::UNIX_EPOCH + Duration::from_micros(1_762_600_000_123_456);
        let def = IndexDef::btree(ObjectId(1201), &["customer_id", "created_at"]);
        let mut cand = CandidateRow {
            candidate_id: 0,
            run_id: run1,
            object: def.object,
            def_hash: def.def_hash(),
            kind: StoredKind::BTree,
            definition: Definition::of(&def),
            covering: false,
            fingerprints: vec![
                (Fingerprint(0x9c41_d2e0_7b3a_5f18), Role::Primary),
                (Fingerprint(1), Role::Merged),
            ],
            status: CandidateStatus::Proposed,
            reason: None,
            benefit_ms_h: None,
            write_ms_h: None,
            est_bytes: None,
            build_seconds: None,
            evidence: Evidence {
                notes: vec!["variant:eq_order_limit_in_as_filter".into()],
                ..Evidence::default()
            },
            created_at: now,
        };
        let cid = s.insert_candidate(&mut c, &cand).await.unwrap();
        cand.candidate_id = cid;
        assert_eq!(s.candidate(&mut c, cid).await.unwrap().unwrap(), cand);
        cand.status = CandidateStatus::Selected;
        cand.benefit_ms_h = Some(190_500.0);
        cand.write_ms_h = Some(7_200.0);
        cand.est_bytes = Some(2_240_000_000);
        cand.build_seconds = Some(112);
        assert!(s.set_candidate(&mut c, &cand).await.unwrap());
        assert_eq!(
            s.candidates_of_run(&mut c, run1).await.unwrap(),
            [cand.clone()]
        );
        assert_eq!(
            s.candidates_for(&mut c, def.object, def.def_hash(), now)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(s.decided_since(&mut c, now).await.unwrap().len(), 1);

        let mut row = AutoIndexRow {
            index_id: AutoIndexId(0),
            object: def.object,
            name: def.name(),
            model: Model::Sql,
            kind: StoredKind::BTree,
            definition: Definition::of(&def),
            state: IndexState::Validating,
            prior_state: None,
            candidate_id: cid,
            validation_id: None,
            decision_id: None,
            est_benefit_ms_h: 190_500.0,
            est_write_ms_h: 7_200.0,
            est_bytes: 2_240_000_000,
            actual_bytes: None,
            shards: [("1".to_owned(), ShardBuild::Pending)].into(),
            baseline: [(
                "9c41d2e07b3a5f18".to_owned(),
                Baseline {
                    runs: 18_240,
                    p95_ms: 412.0,
                    mean_ms: 260.0,
                },
            )]
            .into(),
            observed: None,
            last_used_at: None,
            drop_reason: None,
            created_at: now,
            state_changed_at: now,
        };
        let id = s.insert_index(&mut c, &row).await.unwrap();
        row.index_id = id;
        assert_eq!(s.index(&mut c, id).await.unwrap().unwrap(), row);
        assert_eq!(
            s.index_by_name(&mut c, def.object, &def.name())
                .await
                .unwrap()
                .unwrap(),
            row
        );
        // The same definition twice is 1062.
        let dup = s.insert_index(&mut c, &row).await.unwrap_err();
        assert_eq!(crate::sql::errno(&dup), Some(1062));

        // Transitions: a stale `from` loses; a missing edge is refused.
        let at = now + Duration::from_secs(60);
        let go = |from, to| StateChange {
            from,
            to,
            at,
            drop_reason: None,
        };
        assert!(
            s.set_state(
                &mut c,
                id,
                &go(IndexState::Validating, IndexState::RollingOut)
            )
            .await
            .unwrap()
        );
        assert!(
            !s.set_state(
                &mut c,
                id,
                &go(IndexState::Validating, IndexState::RollingOut)
            )
            .await
            .unwrap()
        );
        assert!(
            s.set_state(&mut c, id, &go(IndexState::RollingOut, IndexState::Active))
                .await
                .is_err()
        );
        assert!(
            s.set_shard(&mut c, id, "1", ShardBuild::Built)
                .await
                .unwrap()
        );
        assert!(
            s.set_shard(&mut c, id, "2", ShardBuild::Pending)
                .await
                .unwrap()
        );
        // Freeze and unfreeze restore the state.
        assert!(
            s.set_state(&mut c, id, &go(IndexState::RollingOut, IndexState::Frozen))
                .await
                .unwrap()
        );
        let r = s.index(&mut c, id).await.unwrap().unwrap();
        assert_eq!(r.prior_state, Some(IndexState::RollingOut));
        assert_eq!(r.shards["1"], ShardBuild::Built);
        assert_eq!(r.shards["2"], ShardBuild::Pending);
        assert_eq!(r.state_changed_at, at);
        assert_eq!(
            s.unfreeze(&mut c, id, at).await.unwrap(),
            Some(IndexState::RollingOut)
        );
        assert_eq!(s.unfreeze(&mut c, id, at).await.unwrap(), None);
        s.touch_used(&mut c, id, at).await.unwrap();
        for (a, b) in [
            (IndexState::RollingOut, IndexState::Observing),
            (IndexState::Observing, IndexState::Active),
            (IndexState::Active, IndexState::Retiring),
            (IndexState::Retiring, IndexState::Dropping),
            (IndexState::Dropping, IndexState::Dropped),
        ] {
            let mut ch = go(a, b);
            if b == IndexState::Retiring {
                ch.drop_reason = Some(DropReason::NoGain);
            }
            assert!(s.set_state(&mut c, id, &ch).await.unwrap(), "{a} -> {b}");
        }
        let r = s.index(&mut c, id).await.unwrap().unwrap();
        assert_eq!(r.state, IndexState::Dropped);
        assert_eq!(r.drop_reason, Some(DropReason::NoGain));
        assert_eq!(
            r.last_used_at,
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_762_600_060))
        );
        assert_eq!(
            s.dropped_since(&mut c, def.object, now)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            s.indexes_in(&mut c, &[IndexState::Dropped])
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(s.indexes_on(&mut c, def.object).await.unwrap().len(), 1);
        assert_eq!(s.created_since(&mut c, now).await.unwrap().len(), 1);

        // Validations.
        let mut v = ValidationRow {
            validation_id: 0,
            candidate_id: cid,
            method: ValidationMethod::Replica,
            cluster_id: "mum1-c7".into(),
            node: "mum1-c7-n2".into(),
            shard_id: 17,
            started_at: now,
            finished_at: None,
            build_seconds: None,
            statements: 0,
            noisy: 0,
            target_p95_before: None,
            target_p95_after: None,
            worst_regression: None,
            worst_fp: None,
            recall_at_k: None,
            verdict: Verdict::Running,
            reason: None,
            details: serde_json::json!({}),
        };
        let vid = s.insert_validation(&mut c, &v).await.unwrap();
        v.validation_id = vid;
        assert_eq!(s.validation(&mut c, vid).await.unwrap().unwrap(), v);
        v.finished_at = Some(at);
        v.build_seconds = Some(120);
        v.statements = 260;
        v.target_p95_before = Some(405.0);
        v.target_p95_after = Some(11.2);
        v.worst_regression = Some(3.1);
        v.worst_fp = Some(Fingerprint(u64::MAX));
        v.verdict = Verdict::Accepted;
        v.details = serde_json::json!({"samples": [1, 2]});
        assert!(s.finish_validation(&mut c, &v).await.unwrap());
        assert!(!s.finish_validation(&mut c, &v).await.unwrap());
        assert_eq!(s.validation(&mut c, vid).await.unwrap().unwrap(), v);

        // Origins.
        s.put_origin(
            &mut c,
            &OriginRow {
                object: def.object,
                index_name: "idx_created".into(),
                origin: Origin::Migrated,
                source: "mysqldump".into(),
                recorded_at: now,
            },
        )
        .await
        .unwrap();
        let o = s.origins(&mut c, def.object).await.unwrap();
        assert_eq!(o["idx_created"], Origin::Migrated);
        assert!(
            s.delete_origin(&mut c, def.object, "idx_created")
                .await
                .unwrap()
        );

        // Retention: everything here is older than `now + 1 day`.
        let later = now + Duration::from_secs(86_400);
        assert_eq!(s.purge(&mut c, later, later).await.unwrap(), 3);
    })
    .await;
}
