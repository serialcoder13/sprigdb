//! The A/B runner against a scripted connection.

#![allow(clippy::unwrap_used, clippy::float_cmp)]

use std::collections::VecDeque;

use super::*;

/// A connection that answers `ANALYZE` with synthetic plans: A (with
/// `IGNORE INDEX`) and B times come from scripts, and B reads the index
/// when `b_uses` is set.
struct Fake {
    a_times: VecDeque<f64>,
    b_times: VecDeque<f64>,
    b_uses: bool,
    a_uses: bool,
    log: Vec<String>,
    fail_on: Option<(&'static str, Option<u16>)>,
}

impl Fake {
    fn new(a: &[f64], b: &[f64]) -> Self {
        Self {
            a_times: a.iter().copied().collect(),
            b_times: b.iter().copied().collect(),
            b_uses: true,
            a_uses: false,
            log: Vec::new(),
            fail_on: None,
        }
    }
}

fn plan(ms: f64, key: Option<&str>) -> String {
    let key = key.map_or(String::new(), |k| format!(r#""key": "{k}", "#));
    format!(
        r#"{{"query_optimization": {{"r_total_time_ms": 0}}, "query_block": {{"r_total_time_ms": {ms},
        "nested_loop": [{{"table": {{"table_name": "t", {key}"access_type": "ALL", "loops": 1,
        "r_loops": 1, "rows": 100, "r_rows": 100}}}}]}}}}"#
    )
}

#[async_trait::async_trait]
impl ReplayConn for Fake {
    async fn exec(&mut self, sql: &str) -> Result<(), ProbeError> {
        self.log.push(sql.to_owned());
        Ok(())
    }
    async fn first_value(&mut self, sql: &str) -> Result<Option<String>, ProbeError> {
        self.log.push(sql.to_owned());
        if let Some((needle, errno)) = self.fail_on
            && sql.contains(needle)
        {
            return Err(ProbeError {
                errno,
                message: "scripted".into(),
                timeout: false,
            });
        }
        let is_a = sql.contains("IGNORE INDEX");
        let (q, uses) = if is_a {
            (&mut self.a_times, self.a_uses)
        } else {
            (&mut self.b_times, self.b_uses)
        };
        let ms = q.pop_front().unwrap_or(1.0);
        if ms < 0.0 {
            return Err(ProbeError {
                errno: Some(1969),
                message: "max_statement_time exceeded".into(),
                timeout: false,
            });
        }
        Ok(Some(plan(ms, uses.then_some("sprig_auto_1"))))
    }
}

fn target() -> ReplayTarget {
    ReplayTarget {
        schema: "s1".into(),
        table: "t".into(),
        index: "sprig_auto_1".into(),
        pk: vec!["id".into()],
    }
}

fn st(fp: u64, sample: u64, role: StatementRole, sql: &str) -> ReplayStatement {
    ReplayStatement {
        fingerprint: Fingerprint(fp),
        sample_id: sample,
        role,
        sql: sql.into(),
        context: SessionContext {
            database: Some("s1".into()),
            sql_mode: Some("STRICT_TRANS_TABLES".into()),
            time_zone: Some("+00:00".into()),
            charset: Some("utf8mb4".into()),
            collation: None,
        },
    }
}

fn no_warm() -> ReplaySettings {
    ReplaySettings {
        warm_up: false,
        ..ReplaySettings::default()
    }
}

#[tokio::test]
async fn rounds_alternate_and_context_is_set_once() {
    let mut c = Fake::new(&[10.0, 10.2, 10.1], &[0.3, 0.2, 0.25]);
    let stmts = vec![st(
        1,
        7,
        StatementRole::Target,
        "SELECT id FROM t WHERE a = 5",
    )];
    let r = replay(&mut c, &mut NoGuard, &target(), &stmts, &no_warm()).await;
    assert_eq!(r.aborted, None);
    let s = &r.statements[0];
    assert_eq!(s.outcome, Outcome::Measured);
    assert_eq!(
        s.a.iter().map(|r| r.ms).collect::<Vec<_>>(),
        vec![10.0, 10.2, 10.1]
    );
    assert!(s.b.iter().all(|r| r.uses_index));
    assert!(!s.extra_rounds);
    // Context, then A B, B A, A B.
    let sides: Vec<char> = c
        .log
        .iter()
        .filter(|l| l.starts_with("ANALYZE"))
        .map(|l| if l.contains("IGNORE INDEX") { 'A' } else { 'B' })
        .collect();
    assert_eq!(sides, vec!['A', 'B', 'B', 'A', 'A', 'B']);
    assert_eq!(c.log[0], "USE `s1`");
    assert_eq!(c.log[1], "SET SESSION sql_mode = 'STRICT_TRANS_TABLES'");
    assert_eq!(c.log[2], "SET SESSION time_zone = '+00:00'");
    assert_eq!(c.log[3], "SET NAMES utf8mb4");
    assert_eq!(
        c.log[4],
        "ANALYZE FORMAT=JSON SELECT id FROM t IGNORE INDEX (`sprig_auto_1`) WHERE a = 5"
    );
    let v = decide(&r.statements, &ReplaySettings::default().thresholds);
    assert_eq!(v.kind, VerdictKind::Accepted, "{}", v.reason);
}

#[tokio::test]
async fn warm_up_runs_both_sides_unrecorded() {
    let mut c = Fake::new(&[99.0, 10.0, 10.0, 10.0], &[99.0, 1.0, 1.0, 1.0]);
    let stmts = vec![st(
        1,
        7,
        StatementRole::Target,
        "SELECT id FROM t WHERE a = 5",
    )];
    let r = replay(
        &mut c,
        &mut NoGuard,
        &target(),
        &stmts,
        &ReplaySettings::default(),
    )
    .await;
    let s = &r.statements[0];
    assert_eq!(s.a.len(), 3);
    assert!(s.a.iter().all(|r| r.ms == 10.0));
    assert!(s.b.iter().all(|r| r.ms == 1.0));
}

#[tokio::test]
async fn noisy_statements_get_two_more_rounds() {
    let mut c = Fake::new(&[10.0, 20.0, 10.0, 10.1, 10.2], &[1.0; 5]);
    let stmts = vec![st(
        1,
        7,
        StatementRole::Target,
        "SELECT id FROM t WHERE a = 5",
    )];
    let r = replay(&mut c, &mut NoGuard, &target(), &stmts, &no_warm()).await;
    let s = &r.statements[0];
    assert!(s.extra_rounds);
    assert_eq!(s.a.len(), 5);
    assert_eq!(s.b.len(), 5);
    // The 20 ms outlier is trimmed: not noisy in the end.
    let e = verdict::evidence(s, &Thresholds::default());
    assert!(!e.noisy);
}

#[tokio::test]
async fn writes_are_replayed_as_selects_and_inserts_skipped() {
    let mut c = Fake::new(&[], &[]);
    let stmts = vec![
        st(
            1,
            1,
            StatementRole::Target,
            "UPDATE t SET b = 1 WHERE a = 5",
        ),
        st(
            2,
            2,
            StatementRole::Regression,
            "INSERT INTO t VALUES (1, 2)",
        ),
        st(
            3,
            3,
            StatementRole::Regression,
            "SELECT 1 FROM u WHERE x = 1",
        ),
    ];
    let r = replay(&mut c, &mut NoGuard, &target(), &stmts, &no_warm()).await;
    assert!(
        c.log.iter().any(|l| l
            == "ANALYZE FORMAT=JSON SELECT `id` FROM t IGNORE INDEX (`sprig_auto_1`) WHERE a = 5")
    );
    assert!(
        !c.log
            .iter()
            .any(|l| l.contains("UPDATE") || l.contains("INSERT"))
    );
    assert!(matches!(r.statements[1].outcome, Outcome::Unreplayable(_)));
    // A statement that doesn't touch t can't get the hint.
    assert!(matches!(r.statements[2].outcome, Outcome::Unreplayable(_)));
}

#[tokio::test]
async fn a_side_using_the_index_fails_the_statement() {
    let mut c = Fake::new(&[1.0; 3], &[1.0; 3]);
    c.a_uses = true;
    let stmts = vec![st(
        1,
        1,
        StatementRole::Target,
        "SELECT id FROM t WHERE a = 5",
    )];
    let r = replay(&mut c, &mut NoGuard, &target(), &stmts, &no_warm()).await;
    assert_eq!(
        r.statements[0].outcome,
        Outcome::Failed("hint_not_applied".into())
    );
}

#[tokio::test]
async fn timeouts_are_runs_and_errors_fail_one_statement() {
    let mut c = Fake::new(&[-1.0, -1.0, -1.0], &[5.0; 3]);
    let stmts = vec![
        st(1, 1, StatementRole::Target, "SELECT id FROM t WHERE a = 5"),
        st(
            2,
            2,
            StatementRole::Regression,
            "SELECT id FROM t WHERE missing = 1",
        ),
    ];
    c.fail_on = Some(("missing", Some(1054)));
    let r = replay(&mut c, &mut NoGuard, &target(), &stmts, &no_warm()).await;
    let s = &r.statements[0];
    assert!(s.a.iter().all(|r| r.timed_out && r.ms == 10_000.0));
    assert_eq!(
        r.statements[1].outcome,
        Outcome::Failed("error 1054".into())
    );
    assert_eq!(r.aborted, None);
}

#[tokio::test]
async fn a_broken_connection_aborts() {
    let mut c = Fake::new(&[1.0; 3], &[1.0; 3]);
    c.fail_on = Some(("ANALYZE", None));
    let stmts = vec![
        st(1, 1, StatementRole::Target, "SELECT id FROM t WHERE a = 5"),
        st(1, 2, StatementRole::Target, "SELECT id FROM t WHERE a = 6"),
    ];
    let r = replay(&mut c, &mut NoGuard, &target(), &stmts, &no_warm()).await;
    assert!(r.aborted.unwrap().starts_with("connection"));
    assert!(r.statements.is_empty());
}

struct StopAfter(usize);

#[async_trait::async_trait]
impl ReplayGuard for StopAfter {
    async fn check(&mut self) -> Result<(), String> {
        if self.0 == 0 {
            return Err("replica lag 400 s".into());
        }
        self.0 -= 1;
        Ok(())
    }
}

#[tokio::test]
async fn the_guard_stops_the_replay() {
    let mut c = Fake::new(&[], &[]);
    let stmts: Vec<_> = (0..5)
        .map(|i| st(1, i, StatementRole::Target, "SELECT id FROM t WHERE a = 5"))
        .collect();
    let r = replay(&mut c, &mut StopAfter(2), &target(), &stmts, &no_warm()).await;
    assert_eq!(r.statements.len(), 2);
    assert_eq!(r.aborted.as_deref(), Some("replica lag 400 s"));
}

#[tokio::test]
async fn unsafe_context_values_are_refused() {
    let mut c = Fake::new(&[], &[]);
    let mut s = st(1, 1, StatementRole::Target, "SELECT id FROM t WHERE a = 5");
    s.context.time_zone = Some("+00:00'; DROP TABLE x; --".into());
    let r = replay(&mut c, &mut NoGuard, &target(), &[s], &no_warm()).await;
    assert!(matches!(&r.statements[0].outcome, Outcome::Failed(w) if w.contains("unsafe")));
    assert!(!c.log.iter().any(|l| l.contains("DROP")));
}

#[test]
fn selection_caps_samples_and_regression_fingerprints() {
    let mut all = Vec::new();
    for i in 0..30 {
        all.push(st(1, i, StatementRole::Target, "SELECT 1"));
    }
    for fp in 10..40 {
        all.push(st(fp, 0, StatementRole::Regression, "SELECT 1"));
        all.push(st(fp, 1, StatementRole::Regression, "SELECT 1"));
    }
    let s = ReplaySettings {
        samples_per_fingerprint: 20,
        regression_fingerprints: 3,
        ..ReplaySettings::default()
    };
    let out = select_statements(&all, &s);
    assert_eq!(
        out.iter()
            .filter(|x| x.fingerprint == Fingerprint(1))
            .count(),
        20
    );
    let reg: std::collections::BTreeSet<u64> = out
        .iter()
        .filter(|x| x.role == StatementRole::Regression)
        .map(|x| x.fingerprint.0)
        .collect();
    assert_eq!(reg.into_iter().collect::<Vec<_>>(), vec![10, 11, 12]);
    assert_eq!(out.len(), 26);
}

#[test]
fn binding_respects_no_backslash_escapes() {
    let ctx = SessionContext {
        sql_mode: Some("STRICT_ALL_TABLES,NO_BACKSLASH_ESCAPES".into()),
        ..SessionContext::default()
    };
    assert!(ctx.no_backslash_escapes());
    let s = ReplayStatement::bind(
        Fingerprint(1),
        1,
        StatementRole::Target,
        "SELECT id FROM t WHERE p = ?",
        &[SampleValue::Str("a\\b".into())],
        ctx,
    )
    .unwrap();
    assert_eq!(s.sql, "SELECT id FROM t WHERE p = 'a\\b'");
    // Debug never shows values.
    assert!(!format!("{s:?}").contains("a\\"));
}
