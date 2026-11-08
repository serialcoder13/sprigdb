//! Live validation tests on the development Compose topologies
//! (`deploy/compose`), chapter 13 build steps 6 to 9. Each test is skipped
//! unless its variable is set to a comma-separated list of node URLs, the
//! writer first (the same variables as `sprig-cluster`'s compose tests):
//!
//! ```sh
//! export SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301
//! export SPRIG_TEST_MARIADB_REPLICATION=mysql://root:sprig-dev-root@127.0.0.1:13311,mysql://root:sprig-dev-root@127.0.0.1:13312,mysql://root:sprig-dev-root@127.0.0.1:13313
//! export SPRIG_TEST_MARIADB_GALERA=mysql://root:sprig-dev-root@127.0.0.1:13321,mysql://root:sprig-dev-root@127.0.0.1:13322,mysql://root:sprig-dev-root@127.0.0.1:13323
//! cargo test -p sprig-advisor --test replay_live -- --nocapture --test-threads=1
//! ```
//!
//! Each test creates `__sprig_test_replay_<topology>` on the writer with a
//! 100,000-row table, validates three candidates (a good one, one that
//! makes another statement slower, one the optimizer ignores), checks that
//! every node is exactly as before, and drops the schema.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_advisor::autoindex::model::{Candidate, Evidence, IndexDef, PartSource, Role};
use sprig_advisor::autoindex::replay::{
    ReplaySettings, ReplayStatement, SessionContext, StatementRole, VerdictKind,
};
use sprig_advisor::autoindex::validate::canary::CanaryValidator;
use sprig_advisor::autoindex::validate::galera::RsuValidator;
use sprig_advisor::autoindex::validate::replica::ReplicaValidator;
use sprig_advisor::autoindex::validate::{
    IndexTarget, NoMarker, Prediction, ValidationPlan, ValidationReport, ValidationSettings,
    Validator, validate_with_covering_retry,
};
use sprig_cluster::{NodeEndpoint, ProbeSession};
use sprig_common::ids::{Fingerprint, NodeId, ObjectId};

fn endpoints(var: &str) -> Option<Vec<NodeEndpoint>> {
    let urls = std::env::var(var).ok().filter(|v| !v.trim().is_empty())?;
    Some(
        urls.split(',')
            .enumerate()
            .map(|(i, u)| {
                NodeEndpoint::from_url(NodeId::new(format!("n{}", i + 1)).unwrap(), u.trim())
                    .unwrap()
            })
            .collect(),
    )
}

macro_rules! topology_or_skip {
    ($var:literal) => {
        match endpoints($var) {
            Some(eps) => eps,
            None => {
                eprintln!("skipped: {} is not set", $var);
                return;
            }
        }
    };
}

async fn conn(ep: &NodeEndpoint) -> ProbeSession {
    ProbeSession::connect(ep, Duration::from_secs(120))
        .await
        .unwrap()
}

async fn scalar(ep: &NodeEndpoint, sql: &str) -> Option<String> {
    let mut c = conn(ep).await;
    let v = c.scalar(sql).await.unwrap();
    c.close().await;
    v
}

const ROWS: u32 = 100_000;

/// `t`: `a` a permutation of 0..99,999 (no index), `b` three values, `c`
/// ascending, `d` = 1 only on the last 1,000 rows, a 150-byte pad.
async fn create_fixture(writer: &NodeEndpoint, schema: &str) {
    let mut c = conn(writer).await;
    c.exec(&format!("DROP DATABASE IF EXISTS `{schema}`"))
        .await
        .unwrap();
    c.exec(&format!("CREATE DATABASE `{schema}`"))
        .await
        .unwrap();
    c.exec(&format!(
        "CREATE TABLE `{schema}`.t (id INT PRIMARY KEY, a INT NOT NULL, b TINYINT NOT NULL, \
         c INT NOT NULL, d TINYINT NOT NULL, pad VARCHAR(200) NOT NULL) ENGINE=InnoDB"
    ))
    .await
    .unwrap();
    // 25,000 rows per transaction keeps Galera write sets small.
    for lo in (1..=ROWS).step_by(25_000) {
        let hi = lo + 24_999;
        c.exec(&format!(
            "INSERT INTO `{schema}`.t SELECT seq, (seq * 7919) % 100000, seq % 3, seq, \
             IF(seq > 99000, 1, 0), REPEAT('x', 150) FROM `{schema}`.seq_{lo}_to_{hi}"
        ))
        .await
        .unwrap();
    }
    c.close().await;
}

async fn drop_fixture(writer: &NodeEndpoint, schema: &str) {
    let mut c = conn(writer).await;
    c.exec(&format!("DROP DATABASE IF EXISTS `{schema}`"))
        .await
        .unwrap();
    c.close().await;
}

/// Waits until every node has all rows (or no schema), and with `gtid`
/// the writer's GTID position (replicas).
async fn wait_caught_up(writer: &NodeEndpoint, others: &[NodeEndpoint], schema: &str, gtid: bool) {
    let pos = scalar(writer, "SELECT @@gtid_binlog_pos")
        .await
        .unwrap_or_default();
    for o in others {
        let started = Instant::now();
        loop {
            // Every row there, or the schema gone (after the drop).
            let ok = {
                let mut c = conn(o).await;
                let v = match c
                    .scalar(&format!("SELECT COUNT(*) = {ROWS} FROM `{schema}`.t"))
                    .await
                {
                    Ok(v) => v,
                    Err(e) if matches!(e.errno, Some(1146 | 1049)) => Some("1".into()),
                    Err(e) => panic!("{e}"),
                };
                c.close().await;
                v
            };
            let gtid_ok = if gtid {
                scalar(o, &format!("SELECT MASTER_GTID_WAIT('{pos}', 1)")).await
            } else {
                Some("0".into())
            };
            if ok.as_deref() == Some("1") && gtid_ok.as_deref() == Some("0") {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(120),
                "{} not caught up",
                o.node
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

fn ctx(schema: &str) -> SessionContext {
    SessionContext {
        database: Some(schema.into()),
        sql_mode: Some(
            "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"
                .into(),
        ),
        time_zone: Some("+00:00".into()),
        charset: Some("utf8mb4".into()),
        collation: None,
    }
}

/// A tiny deterministic generator for sample values.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % n
    }
}

const FP_A_RANGE: Fingerprint = Fingerprint(0xa1);
const FP_B_COUNT: Fingerprint = Fingerprint(0xb2);
const FP_UPDATE_A: Fingerprint = Fingerprint(0xc3);
const FP_C_RANGE: Fingerprint = Fingerprint(0xd4);
const FP_D_ORDER_C: Fingerprint = Fingerprint(0xe5);
const FP_B_SCAN: Fingerprint = Fingerprint(0xf6);

fn stmt(
    fp: Fingerprint,
    id: u64,
    role: StatementRole,
    sql: String,
    schema: &str,
) -> ReplayStatement {
    ReplayStatement {
        fingerprint: fp,
        sample_id: id,
        role,
        sql,
        context: ctx(schema),
    }
}

/// The table's workload: targets of each candidate and the other
/// fingerprints on `t`.
fn workload(schema: &str, samples: u64) -> BTreeMap<&'static str, Vec<ReplayStatement>> {
    let mut g = Lcg(42);
    let mut w: BTreeMap<&'static str, Vec<ReplayStatement>> = BTreeMap::new();
    for i in 0..samples {
        let lo = g.next(99_800);
        w.entry("a_range").or_default().push(stmt(
            FP_A_RANGE,
            i,
            StatementRole::Target,
            format!(
                "SELECT id, pad FROM t WHERE a BETWEEN {lo} AND {}",
                lo + 100
            ),
            schema,
        ));
        let lo = g.next(99_800);
        w.entry("c_range").or_default().push(stmt(
            FP_C_RANGE,
            100 + i,
            StatementRole::Target,
            format!(
                "SELECT id, pad FROM `{schema}`.t AS x WHERE x.c BETWEEN {lo} AND {}",
                lo + 100
            ),
            schema,
        ));
        w.entry("b_scan").or_default().push(stmt(
            FP_B_SCAN,
            200 + i,
            StatementRole::Target,
            format!("SELECT COUNT(*), MAX(pad) FROM t WHERE b = {}", g.next(3)),
            schema,
        ));
    }
    for i in 0..samples.min(5) {
        w.entry("other").or_default().push(stmt(
            FP_B_COUNT,
            300 + i,
            StatementRole::Regression,
            format!("SELECT COUNT(*) FROM t WHERE b = {}", g.next(3)),
            schema,
        ));
        w.entry("other").or_default().push(stmt(
            FP_UPDATE_A,
            400 + i,
            StatementRole::Regression,
            format!("UPDATE t SET pad = 'y' WHERE a = {}", g.next(100_000)),
            schema,
        ));
        w.entry("other").or_default().push(stmt(
            FP_D_ORDER_C,
            500 + i,
            StatementRole::Regression,
            "SELECT id FROM t WHERE d = 1 ORDER BY c LIMIT 10".into(),
            schema,
        ));
    }
    w
}

fn plan(
    schema: &str,
    object: u64,
    cols: &[&str],
    target: Fingerprint,
    targets: Vec<ReplayStatement>,
    others: &[ReplayStatement],
) -> ValidationPlan {
    let def = IndexDef::btree(ObjectId(object), cols);
    let mut statements = targets;
    statements.extend(others.iter().filter(|s| s.fingerprint != target).cloned());
    let mut columns_read = BTreeMap::new();
    columns_read.insert(FP_D_ORDER_C, vec!["id".into(), "d".into(), "c".into()]);
    ValidationPlan {
        candidate_id: object,
        candidate: Candidate {
            evidence: Evidence {
                parts: vec![PartSource::Range; def.parts.len()],
                ..Evidence::default()
            },
            def,
            covering: false,
            serves: vec![(target, Role::Primary)],
        },
        target: IndexTarget {
            schema: schema.into(),
            table: "t".into(),
            pk: vec!["id".into()],
        },
        statements,
        predictions: vec![Prediction {
            fingerprint: target,
            t_before_ms: 12.0,
            t_after_ms: 1.0,
            rows_assumed: f64::from(ROWS),
        }],
        columns_read,
        keep_if_accepted: false,
        settings: ValidationSettings {
            replay: ReplaySettings {
                samples_per_fingerprint: 10,
                ..ReplaySettings::default()
            },
            ..ValidationSettings::default()
        },
    }
}

fn show(label: &str, r: &ValidationReport) {
    eprintln!(
        "[{label}] {} on {} via {}: {} ({}) | total {} ms, build {:?} ms, analyze {:?} ms, \
         resync {:?} ms, replay {:?} ms, drop {:?} ms | statements {}, noisy {} | target p95 \
         {:?} -> {:?} ms | worst regression {:.1}% {:?} | restored {}",
        r.index,
        r.node,
        r.method.as_str(),
        r.verdict.kind.as_str(),
        r.verdict.reason,
        r.elapsed_ms(),
        r.build_ms,
        r.analyze_ms,
        r.resync_ms,
        r.replay_ms,
        r.drop_ms,
        r.verdict.statements,
        r.verdict.noisy,
        r.verdict.target_p95_before,
        r.verdict.target_p95_after,
        r.verdict.worst_regression_pct,
        r.verdict.worst_fp,
        r.node_check.restored
    );
    for f in &r.verdict.fingerprints {
        eprintln!(
            "    {} {:?}: {} samples, p95 {:.3} -> {:.3} ms ({:+.1}%), uses index {}",
            f.fingerprint,
            f.role,
            f.samples,
            f.p95_before,
            f.p95_after,
            -f.improvement_pct,
            f.uses_index
        );
    }
    for n in &r.node_check.notes {
        eprintln!("    check: {n}");
    }
}

/// The three candidates against one validator.
async fn three_candidates(
    v: &dyn Validator,
    schema: &str,
    label: &str,
) -> Vec<(String, Vec<ValidationReport>)> {
    let w = workload(schema, 10);
    let others = w["other"].clone();
    let mut out = Vec::new();

    // A good index: a range on an unindexed column of a 100k-row table.
    let good = plan(
        schema,
        9001,
        &["a"],
        FP_A_RANGE,
        w["a_range"].clone(),
        &others,
    );
    let r = v.validate(&good).await.unwrap();
    show(&format!("{label} good"), &r);
    out.push(("good".to_owned(), vec![r]));

    // A trap: (c) helps the range on c, but sends `d = 1 ORDER BY c LIMIT
    // 10` through the whole index with primary-key lookups.
    let trap = plan(
        schema,
        9002,
        &["c"],
        FP_C_RANGE,
        w["c_range"].clone(),
        &others,
    );
    let rs = validate_with_covering_retry(v, &trap).await.unwrap();
    for (i, r) in rs.iter().enumerate() {
        show(
            &format!(
                "{label} trap{}",
                if i == 0 { "" } else { " covering retry" }
            ),
            r,
        );
    }
    out.push(("trap".to_owned(), rs));

    // Useless: three values in b; the optimizer keeps scanning.
    let useless = plan(
        schema,
        9003,
        &["b"],
        FP_B_SCAN,
        w["b_scan"].clone(),
        &others,
    );
    let r = v.validate(&useless).await.unwrap();
    show(&format!("{label} useless"), &r);
    out.push(("useless".to_owned(), vec![r]));
    out
}

fn assert_verdicts(results: &[(String, Vec<ValidationReport>)]) {
    for (name, rs) in results {
        for r in rs {
            assert!(
                r.node_check.restored,
                "{name}: node not restored: {:?}",
                r.node_check
            );
            assert!(!r.index_kept, "{name}: index kept");
        }
        let first = &rs[0];
        match name.as_str() {
            "good" => assert_eq!(
                first.verdict.kind,
                VerdictKind::Accepted,
                "{}",
                first.verdict.reason
            ),
            "trap" => {
                assert_eq!(
                    first.verdict.kind,
                    VerdictKind::Rejected,
                    "{}",
                    first.verdict.reason
                );
                assert!(
                    first.verdict.reason.starts_with("regression"),
                    "{}",
                    first.verdict.reason
                );
                assert_eq!(first.verdict.worst_fp, Some(FP_D_ORDER_C));
            }
            "useless" => {
                assert_eq!(
                    first.verdict.kind,
                    VerdictKind::Rejected,
                    "{}",
                    first.verdict.reason
                );
            }
            _ => {}
        }
    }
}

async fn indexes_named_auto(ep: &NodeEndpoint, schema: &str) -> String {
    scalar(
        ep,
        &format!(
            "SELECT IFNULL(GROUP_CONCAT(DISTINCT INDEX_NAME), '') FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = '{schema}' AND INDEX_NAME LIKE 'sprig\\_auto\\_%'"
        ),
    )
    .await
    .unwrap_or_default()
}

async fn checksum(ep: &NodeEndpoint, schema: &str) -> String {
    let mut c = conn(ep).await;
    let rows = c
        .rows(&format!("CHECKSUM TABLE `{schema}`.t"))
        .await
        .unwrap();
    c.close().await;
    rows[0]
        .iter()
        .find(|(n, _)| n == "Checksum")
        .and_then(|(_, v)| v.clone())
        .unwrap_or_default()
}

#[tokio::test]
async fn replica_build_validation() {
    let eps = topology_or_skip!("SPRIG_TEST_MARIADB_REPLICATION");
    let (primary, replicas) = (&eps[0], &eps[1..]);
    let node = replicas.last().unwrap().clone();
    let schema = "__sprig_test_replay_replica";
    let server_ids: Vec<String> = {
        let mut v = Vec::new();
        for r in replicas {
            v.push(scalar(r, "SELECT @@server_id").await.unwrap());
        }
        v
    };
    create_fixture(primary, schema).await;
    wait_caught_up(primary, replicas, schema, true).await;

    let v = ReplicaValidator::new(node.clone(), Arc::new(NoMarker));
    let started = Instant::now();
    let results = three_candidates(&v, schema, "replica").await;
    eprintln!(
        "replica: three validations in {} ms",
        started.elapsed().as_millis()
    );

    // The replica fails mid-validation: its SQL thread stops.
    let w = workload(schema, 10);
    let mid = plan(
        schema,
        9004,
        &["a"],
        FP_A_RANGE,
        w["a_range"].clone(),
        &w["other"],
    );
    let stopper = {
        let node = node.clone();
        let name = mid.index_name();
        let schema = schema.to_owned();
        tokio::spawn(async move {
            // As soon as the index exists, stop the SQL thread.
            for _ in 0..600 {
                let n = scalar(
                    &node,
                    &format!(
                        "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = \
                         '{schema}' AND INDEX_NAME = '{name}'"
                    ),
                )
                .await;
                if n.as_deref() != Some("0") {
                    let mut c = conn(&node).await;
                    c.exec("STOP SLAVE SQL_THREAD").await.unwrap();
                    c.close().await;
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            false
        })
    };
    let aborted = v.validate(&mid).await.unwrap();
    let sql_thread_stopped = stopper.await.unwrap();
    // Restart replication whatever happened.
    {
        let mut c = conn(&node).await;
        c.exec("START SLAVE SQL_THREAD").await.unwrap();
        c.close().await;
    }
    show("replica SQL thread stopped mid-validation", &aborted);
    wait_caught_up(primary, replicas, schema, true).await;

    // User DDL on the table during the validation wins: the primary adds a
    // column, it replicates to the validation node, the validation aborts.
    let ddl_plan = plan(
        schema,
        9006,
        &["a"],
        FP_A_RANGE,
        w["a_range"].clone(),
        &w["other"],
    );
    let user_ddl = {
        let (node, primary) = (node.clone(), primary.clone());
        let name = ddl_plan.index_name();
        let schema = schema.to_owned();
        tokio::spawn(async move {
            for _ in 0..600 {
                let n = scalar(
                    &node,
                    &format!(
                        "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = \
                         '{schema}' AND INDEX_NAME = '{name}'"
                    ),
                )
                .await;
                if n.as_deref() != Some("0") {
                    let mut c = conn(&primary).await;
                    c.exec(&format!("ALTER TABLE `{schema}`.t ADD COLUMN e INT NULL"))
                        .await
                        .unwrap();
                    c.close().await;
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            false
        })
    };
    let ddl_aborted = v.validate(&ddl_plan).await.unwrap();
    let altered = user_ddl.await.unwrap();
    show("replica user DDL mid-validation", &ddl_aborted);
    wait_caught_up(primary, replicas, schema, true).await;

    // Every node as before: no index, same data, no errant GTIDs,
    // replication running.
    let mut leftovers = Vec::new();
    let mut sums = Vec::new();
    for ep in &eps {
        leftovers.push(indexes_named_auto(ep, schema).await);
        sums.push(checksum(ep, schema).await);
    }
    let mut repl = Vec::new();
    for (r, sid) in replicas.iter().zip(&server_ids) {
        let mut c = conn(r).await;
        let st = c.rows("SHOW SLAVE STATUS").await.unwrap();
        let pos = c
            .scalar("SELECT @@gtid_binlog_pos")
            .await
            .unwrap()
            .unwrap_or_default();
        c.close().await;
        let get = |k: &str| {
            st[0]
                .iter()
                .find(|(n, _)| n == k)
                .and_then(|(_, v)| v.clone())
                .unwrap_or_default()
        };
        repl.push((
            r.node.to_string(),
            get("Slave_IO_Running"),
            get("Slave_SQL_Running"),
            pos.contains(&format!("-{sid}-")),
        ));
    }
    eprintln!("replica: leftovers {leftovers:?}, checksums {sums:?}, replication {repl:?}");
    drop_fixture(primary, schema).await;
    wait_caught_up(primary, replicas, schema, true).await;

    assert_verdicts(&results);
    assert!(sql_thread_stopped, "the SQL thread was never stopped");
    assert_eq!(
        aborted.verdict.kind,
        VerdictKind::Aborted,
        "{}",
        aborted.verdict.reason
    );
    assert!(
        aborted.verdict.reason.contains("SQL thread"),
        "{}",
        aborted.verdict.reason
    );
    // Cleaning up after an abort still drops the index, but the
    // replication check fails while the thread is stopped: that is the
    // point of the check.
    assert!(
        aborted
            .node_check
            .notes
            .iter()
            .any(|n| n.ends_with("absent"))
    );
    assert!(altered, "the user DDL never ran");
    assert_eq!(
        ddl_aborted.verdict.kind,
        VerdictKind::Aborted,
        "{}",
        ddl_aborted.verdict.reason
    );
    assert!(
        ddl_aborted.verdict.reason.starts_with("user_ddl"),
        "{}",
        ddl_aborted.verdict.reason
    );
    assert!(
        ddl_aborted.node_check.restored,
        "{:?}",
        ddl_aborted.node_check
    );
    assert!(leftovers.iter().all(String::is_empty), "{leftovers:?}");
    assert!(sums.windows(2).all(|w| w[0] == w[1]), "{sums:?}");
    for (n, io, sql, errant) in repl {
        assert_eq!((io.as_str(), sql.as_str()), ("Yes", "Yes"), "{n}");
        assert!(!errant, "{n} has GTIDs from its own server id");
    }
}

#[tokio::test]
async fn galera_rsu_validation() {
    let eps = topology_or_skip!("SPRIG_TEST_MARIADB_GALERA");
    let writer = &eps[0];
    let node = eps.last().unwrap().clone();
    let peers: Vec<NodeEndpoint> = eps[..eps.len() - 1].to_vec();
    let schema = "__sprig_test_replay_galera";
    create_fixture(writer, schema).await;
    wait_caught_up(writer, &eps[1..], schema, false).await;

    let mut binlog_before = Vec::new();
    for ep in &eps {
        binlog_before.push(
            scalar(ep, "SELECT @@gtid_binlog_pos")
                .await
                .unwrap_or_default(),
        );
    }
    let v = RsuValidator::new(node.clone(), peers, Arc::new(NoMarker));
    let started = Instant::now();
    let results = three_candidates(&v, schema, "galera").await;
    eprintln!(
        "galera: three validations in {} ms",
        started.elapsed().as_millis()
    );

    let mut binlog_after = Vec::new();
    let mut leftovers = Vec::new();
    let mut sums = Vec::new();
    let mut states = Vec::new();
    for ep in &eps {
        binlog_after.push(
            scalar(ep, "SELECT @@gtid_binlog_pos")
                .await
                .unwrap_or_default(),
        );
        leftovers.push(indexes_named_auto(ep, schema).await);
        sums.push(checksum(ep, schema).await);
        let mut c = conn(ep).await;
        let s = c
            .rows(
                "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME \
                 IN ('WSREP_LOCAL_STATE_COMMENT','WSREP_CLUSTER_SIZE') ORDER BY VARIABLE_NAME",
            )
            .await
            .unwrap();
        let g = c
            .scalar("SELECT CONCAT_WS(',', @@global.wsrep_OSU_method, @@global.wsrep_desync)")
            .await
            .unwrap()
            .unwrap_or_default();
        c.close().await;
        states.push((
            s.iter()
                .filter_map(|r| r[0].1.clone())
                .collect::<Vec<_>>()
                .join("/"),
            g,
        ));
    }
    eprintln!(
        "galera: binlog before {binlog_before:?} after {binlog_after:?}, leftovers {leftovers:?}, \
         checksums {sums:?}, states {states:?}"
    );
    drop_fixture(writer, schema).await;

    assert_verdicts(&results);
    // No node wrote a binlog event of its own: the positions moved only
    // through the writer's statements (the same on every node).
    assert!(
        binlog_after.windows(2).all(|w| w[0] == w[1]),
        "{binlog_after:?}"
    );
    assert!(leftovers.iter().all(String::is_empty), "{leftovers:?}");
    assert!(sums.windows(2).all(|w| w[0] == w[1]), "{sums:?}");
    for (s, g) in states {
        assert_eq!(s, "3/Synced");
        assert_eq!(g, "TOI,OFF");
    }
}

#[tokio::test]
async fn estimate_and_canary_validation() {
    let eps = topology_or_skip!("SPRIG_TEST_MARIADB_SINGLE");
    let node = eps[0].clone();
    let schema = "__sprig_test_replay_single";
    create_fixture(&node, schema).await;

    let v = CanaryValidator::new(node.clone(), true);
    let started = Instant::now();
    // Short canaries for the test: keep_if_accepted doesn't apply to the
    // canary; a kept canary is dropped below, as a rejected judgement would.
    let mut results = three_candidates(&v, schema, "single").await;
    eprintln!(
        "single: three validations in {} ms",
        started.elapsed().as_millis()
    );

    // A prediction under the 45% bar is rejected before anything is built.
    let w = workload(schema, 3);
    let mut weak = plan(
        schema,
        9005,
        &["a"],
        FP_A_RANGE,
        w["a_range"].clone(),
        &w["other"],
    );
    weak.predictions[0].t_after_ms = 8.0;
    let r = v.validate(&weak).await.unwrap();
    show("single weak prediction", &r);
    assert_eq!(r.verdict.kind, VerdictKind::Rejected);
    assert!(r.verdict.reason.starts_with("prediction_below_margin"));
    assert_eq!(r.build_ms, None);

    // The accepted canary stays, observing; drop it as the observer would
    // after a rejected judgement.
    let good = &results[0].1[0];
    assert!(good.index_kept && good.canary.is_some(), "{good:?}");
    let good_plan = plan(schema, 9001, &["a"], FP_A_RANGE, vec![], &[]);
    v.drop_canary(&good_plan).await.unwrap();
    results[0].1[0].index_kept = false;
    // Other kept canaries (the trap's covering retry) go the same way.
    let mut kept = Vec::new();
    for (_, rs) in &mut results {
        for r in rs.iter_mut().filter(|r| r.index_kept) {
            kept.push(r.index.clone());
            let mut c = conn(&node).await;
            c.exec(&format!(
                "ALTER TABLE `{schema}`.t DROP INDEX IF EXISTS `{}`",
                r.index
            ))
            .await
            .unwrap();
            c.close().await;
            r.index_kept = false;
        }
    }
    eprintln!("single: canaries dropped after the test: {kept:?}");

    let leftovers = indexes_named_auto(&node, schema).await;
    drop_fixture(&node, schema).await;
    assert_verdicts(&results);
    assert!(leftovers.is_empty(), "{leftovers}");
}
