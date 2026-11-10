//! Cases measured on MySQL 8.4.11 (9 November 2026): each statement with
//! the error MySQL gave, or none.

use std::collections::HashMap;
use std::sync::Arc;

use super::*;

struct Fake(HashMap<(String, String), Arc<TableMeta>>);

impl Meta for Fake {
    fn table(&self, db: &str, table: &str) -> Option<Option<Arc<TableMeta>>> {
        Some(self.0.get(&(db.to_owned(), table.to_owned())).cloned())
    }
}

fn table(cols: &[(&str, bool)], keys: &[&[usize]], view: bool) -> Arc<TableMeta> {
    Arc::new(TableMeta {
        columns: cols
            .iter()
            .map(|(n, nn)| ColumnMeta {
                name: (*n).to_owned(),
                not_null: *nn,
                visible: true,
            })
            .collect(),
        keys: keys.iter().map(|k| k.to_vec()).collect(),
        view,
    })
}

fn fake() -> Fake {
    let mut m = HashMap::new();
    let k = |t: &str| ("gbp".to_owned(), t.to_owned());
    // t (id PRIMARY KEY, a, b, c NOT NULL, u NOT NULL UNIQUE, n UNIQUE)
    m.insert(
        k("t"),
        table(
            &[
                ("id", true),
                ("a", false),
                ("b", false),
                ("c", true),
                ("u", true),
                ("n", false),
            ],
            &[&[0], &[4], &[5]],
            false,
        ),
    );
    m.insert(
        k("s"),
        table(
            &[("id", true), ("tid", false), ("x", false)],
            &[&[0]],
            false,
        ),
    );
    m.insert(k("ins"), table(&[("a", false), ("b", false)], &[], false));
    m.insert(k("v2"), table(&[("id", true), ("b", false)], &[], true));
    m.insert(
        k("v3"),
        table(&[("id", true), ("a", false), ("b", false)], &[], true),
    );
    Fake(m)
}

fn run(sql: &str, ofgb: bool) -> Outcome {
    let f = fake();
    let c = Check {
        current_db: Some("gbp"),
        only_full_group_by: ofgb,
        meta: &f,
    };
    check(sql.as_bytes(), &LexOptions::mysql84(), &c)
}

/// `None`: accepted. `Some((code, column or expression detail))`.
fn expect(sql: &str, want: Option<(u16, &str)>) {
    let got = run(sql, true);
    match (want, &got) {
        (None, Outcome::Accept) => {}
        (Some((code, frag)), Outcome::Refuse(r)) if r.code == code && r.message.contains(frag) => {}
        _ => panic!("{sql}\n  want {want:?}\n  got  {got:?}"),
    }
}

const NGB: u16 = 1055;

#[test]
#[allow(clippy::too_many_lines)] // one table of cases
fn mysql_84_measured_cases() {
    let cases: &[(&str, Option<(u16, &str)>)] = &[
        (
            "SELECT a, b FROM t GROUP BY a",
            Some((
                NGB,
                "Expression #2 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'gbp.t.b' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by",
            )),
        ),
        (
            "SELECT a, COUNT(*) FROM t",
            Some((
                1140,
                "In aggregated query without GROUP BY, expression #1 of SELECT list contains nonaggregated column 'gbp.t.a'; this is incompatible with sql_mode=only_full_group_by",
            )),
        ),
        ("SELECT id, a FROM t GROUP BY id", None),
        ("SELECT u, a FROM t GROUP BY u", None),
        (
            "SELECT n, a FROM t GROUP BY n",
            Some((
                NGB,
                "#2 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'gbp.t.a'",
            )),
        ),
        (
            "SELECT t.a, s.x FROM t JOIN s ON s.tid = t.id GROUP BY t.id",
            Some((
                NGB,
                "#2 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'gbp.s.x'",
            )),
        ),
        (
            "SELECT t.a, s.x FROM t JOIN s ON s.tid = t.id GROUP BY s.id",
            None,
        ),
        (
            "SELECT t.a, s.x FROM t LEFT JOIN s ON s.tid = t.id GROUP BY t.id",
            Some((NGB, "'gbp.s.x'")),
        ),
        (
            "SELECT t.a, s.x FROM t LEFT JOIN s ON s.id = t.id GROUP BY t.id",
            None,
        ),
        (
            "SELECT t.a, s.x FROM s LEFT JOIN t ON s.tid = t.id GROUP BY s.id",
            None,
        ),
        ("SELECT a, b FROM t WHERE b = 3 GROUP BY a", None),
        ("SELECT a, b FROM t WHERE b = a GROUP BY a", None),
        ("SELECT b, COUNT(*) FROM t WHERE b = 3", None),
        ("SELECT a+1, COUNT(*) FROM t GROUP BY a", None),
        ("SELECT a+b, COUNT(*) FROM t GROUP BY a+b", None),
        ("SELECT a+b AS z FROM t GROUP BY z", None),
        (
            "SELECT b FROM t GROUP BY a ORDER BY b",
            Some((NGB, "Expression #1 of SELECT list")),
        ),
        (
            "SELECT a FROM t GROUP BY a ORDER BY b",
            Some((
                NGB,
                "Expression #1 of ORDER BY clause is not in GROUP BY clause and contains nonaggregated column 'gbp.t.b'",
            )),
        ),
        ("SELECT a FROM t GROUP BY a HAVING MAX(b) > 0", None),
        (
            "SELECT x.a, x.b FROM t AS x GROUP BY x.a",
            Some((NGB, "'gbp.x.b'")),
        ),
        ("SELECT ANY_VALUE(b), a FROM t GROUP BY a", None),
        (
            "SELECT a, (SELECT MAX(x) FROM s WHERE s.tid = t.b) FROM t GROUP BY a",
            Some((
                NGB,
                "#2 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'gbp.t.b'",
            )),
        ),
        (
            "SELECT a, (SELECT MAX(x) FROM s WHERE s.tid = t.a) FROM t GROUP BY a",
            None,
        ),
        (
            "SELECT a, ROW_NUMBER() OVER (ORDER BY b) FROM t GROUP BY a",
            Some((
                NGB,
                "Expression #1 of PARTITION BY or ORDER BY clause of window '<unnamed window>' is not in GROUP BY clause and contains nonaggregated column 'gbp.t.b'",
            )),
        ),
        ("SELECT a, SUM(SUM(b)) OVER () FROM t GROUP BY a", None),
        (
            "SELECT dt.a, dt.b FROM (SELECT a, b FROM t) dt GROUP BY dt.a",
            Some((
                NGB,
                "#2 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'dt.b'",
            )),
        ),
        (
            "SELECT dt.id, dt.b FROM (SELECT id, b FROM t) dt GROUP BY dt.id",
            None,
        ),
        (
            "SELECT dt.a, dt.m FROM (SELECT a, MAX(b) m FROM t GROUP BY a) dt GROUP BY dt.a",
            None,
        ),
        (
            "SELECT id, a FROM t GROUP BY id WITH ROLLUP",
            Some((
                NGB,
                "#2 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'gbp.t.a'",
            )),
        ),
        (
            "SELECT a, b, COUNT(*) FROM t GROUP BY a, b WITH ROLLUP",
            None,
        ),
        (
            "SELECT DISTINCT a FROM t ORDER BY b",
            Some((
                3065,
                "Expression #1 of ORDER BY clause is not in SELECT list, references column 'gbp.t.b' which is not in SELECT list; this is incompatible with DISTINCT",
            )),
        ),
        ("SELECT 1, COUNT(*) FROM t", None),
        ("SELECT a FROM t HAVING a > 0", None),
        ("SELECT MAX(a) FROM t ORDER BY b", None),
        (
            "SELECT a FROM t ORDER BY MAX(b)",
            Some((
                3029,
                "Expression #1 of ORDER BY contains aggregate function and applies to the result of a non-aggregated query",
            )),
        ),
        ("SELECT t.*, COUNT(*) FROM t GROUP BY id", None),
        (
            "SELECT s.*, COUNT(*) FROM t JOIN s ON s.tid=t.id GROUP BY t.id",
            Some((
                NGB,
                "#1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'gbp.s.id'",
            )),
        ),
        (
            "SELECT c, a FROM t GROUP BY c",
            Some((NGB, "#2 of SELECT list")),
        ),
        ("SELECT a, b FROM t WHERE id = 1 GROUP BY a", None),
        ("SELECT b FROM t WHERE id = 1 GROUP BY a", None),
        ("CREATE VIEW v AS SELECT a, b FROM t GROUP BY a", None),
        (
            "SELECT b, ROW_NUMBER() OVER (ORDER BY b) FROM t GROUP BY a",
            Some((NGB, "Expression #1 of SELECT list")),
        ),
        (
            "SELECT a, ROW_NUMBER() OVER w FROM t GROUP BY a WINDOW w AS (ORDER BY b)",
            Some((
                NGB,
                "Expression #1 of PARTITION BY or ORDER BY clause of window 'w'",
            )),
        ),
        (
            "SELECT a FROM t HAVING COUNT(*) > 0",
            Some((
                1140,
                "expression #1 of SELECT list contains nonaggregated column 'gbp.t.a'",
            )),
        ),
        ("SELECT a FROM t GROUP BY a ORDER BY COUNT(*)", None),
        (
            "SELECT (SELECT t.a), COUNT(*) FROM t",
            Some((
                1140,
                "expression #1 of SELECT list contains nonaggregated column 'gbp.t.a'",
            )),
        ),
        (
            "SELECT (SELECT MAX(s.x) FROM s WHERE s.tid = t.id) AS m, t.b FROM t GROUP BY t.id",
            None,
        ),
        (
            "SELECT b, a FROM t GROUP BY 2",
            Some((
                NGB,
                "Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'gbp.t.b'",
            )),
        ),
        ("SELECT t.a FROM t JOIN s USING (id) GROUP BY id", None),
        ("SELECT a FROM t NATURAL JOIN s GROUP BY id", None),
        ("SELECT a, COUNT(*) FROM t GROUP BY a HAVING a > 1", None),
        (
            "SELECT a*2, b FROM t GROUP BY a*2",
            Some((NGB, "#2 of SELECT list")),
        ),
        (
            "SELECT CONCAT(a, b) FROM t GROUP BY a",
            Some((
                NGB,
                "#1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'gbp.t.b'",
            )),
        ),
        (
            "SELECT a, b FROM t GROUP BY a UNION SELECT 1, 2",
            Some((NGB, "#2 of SELECT list")),
        ),
        (
            "INSERT INTO ins SELECT a, b FROM t GROUP BY a",
            Some((NGB, "'gbp.t.b'")),
        ),
        (
            "WITH c AS (SELECT a, b FROM t) SELECT a, b FROM c GROUP BY a",
            Some((
                NGB,
                "#2 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'c.b'",
            )),
        ),
        ("SELECT A, b FROM t GROUP BY a, B", None),
        ("SELECT RAND() > 2, a FROM t GROUP BY a", None),
        (
            "SELECT a, GROUP_CONCAT(b ORDER BY c) FROM t GROUP BY a",
            None,
        ),
        (
            "SELECT a, b FROM t GROUP BY a, b+0",
            Some((NGB, "#2 of SELECT list")),
        ),
        (
            "SELECT u2.a FROM t u1 JOIN t u2 ON u2.id = u1.a GROUP BY u1.a",
            None,
        ),
        (
            "SELECT a FROM t WHERE b=1 OR c=2 GROUP BY b",
            Some((NGB, "#1 of SELECT list")),
        ),
        (
            "SELECT a, COUNT(*) AS n FROM t GROUP BY a HAVING n > 1",
            None,
        ),
        (
            "SELECT a FROM t GROUP BY b HAVING a > 1",
            Some((NGB, "#1 of SELECT list")),
        ),
        ("SELECT COUNT(*) FROM t ORDER BY a", None),
        (
            "SELECT a, COUNT(*) FROM t ORDER BY a",
            Some((1140, "expression #1 of SELECT list")),
        ),
        (
            "SELECT a FROM t GROUP BY a ORDER BY a+b",
            Some((NGB, "Expression #1 of ORDER BY clause")),
        ),
        ("SELECT a AS b FROM t GROUP BY a ORDER BY b", None),
        (
            "SELECT x.b FROM t x JOIN t y ON x.id = y.id GROUP BY y.id",
            None,
        ),
        (
            "SELECT t.b FROM t WHERE t.id = (SELECT MAX(id) FROM s) GROUP BY t.a",
            Some((NGB, "'gbp.t.b'")),
        ),
        ("SELECT b FROM t WHERE id IN (1) GROUP BY a", None),
        ("SELECT b FROM t WHERE id = 1 AND a = 2 GROUP BY c", None),
        ("SELECT b FROM t WHERE 1 = id GROUP BY c", None),
        (
            "SELECT b FROM t WHERE id <=> 1 GROUP BY c",
            Some((NGB, "'gbp.t.b'")),
        ),
        (
            "SELECT b FROM t WHERE id = a+1 GROUP BY a",
            Some((NGB, "'gbp.t.b'")),
        ),
        ("SELECT b FROM t WHERE id = a GROUP BY a", None),
        ("SELECT b FROM t WHERE (id = a) GROUP BY a", None),
        ("SELECT b FROM t WHERE NOT (id <> a) GROUP BY a", None),
        (
            "SELECT a, (SELECT b) FROM t GROUP BY a",
            Some((NGB, "#2 of SELECT list")),
        ),
        ("SELECT a, (SELECT COUNT(b)) FROM t GROUP BY a", None),
        (
            "SELECT a, EXISTS (SELECT 1 FROM s WHERE s.x = t.b) FROM t GROUP BY a",
            Some((NGB, "#2 of SELECT list")),
        ),
        (
            "SELECT MAX(a) AS m, b FROM t",
            Some((
                1140,
                "expression #2 of SELECT list contains nonaggregated column 'gbp.t.b'",
            )),
        ),
        (
            "SELECT MAX(a) FROM t GROUP BY b ORDER BY a",
            Some((
                NGB,
                "Expression #1 of ORDER BY clause is not in GROUP BY clause and contains nonaggregated column 'gbp.t.a'",
            )),
        ),
        ("SELECT SUM(a) FROM t HAVING SUM(b) > 0 ORDER BY c", None),
        ("SELECT * FROM t GROUP BY id", None),
        (
            "SELECT * FROM t GROUP BY a",
            Some((
                NGB,
                "Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'gbp.t.id'",
            )),
        ),
        (
            "SELECT x.* FROM t x GROUP BY x.a",
            Some((NGB, "'gbp.x.id'")),
        ),
        (
            "SELECT a, SUM(b) OVER () FROM t GROUP BY a",
            Some((NGB, "Expression #2 of SELECT list")),
        ),
        (
            "SELECT a FROM t GROUP BY a ORDER BY (SELECT MAX(x) FROM s WHERE s.tid = t.b)",
            Some((NGB, "Expression #1 of ORDER BY clause")),
        ),
        ("SELECT b FROM t WHERE id = 1+1 GROUP BY c", None),
        ("SELECT b FROM t WHERE id = ? GROUP BY c", None),
        (
            "SELECT b FROM t WHERE id = @v GROUP BY c",
            Some((NGB, "'gbp.t.b'")),
        ),
        (
            "SELECT b FROM t WHERE id = NOW() GROUP BY c",
            Some((NGB, "'gbp.t.b'")),
        ),
        (
            "SELECT b FROM t WHERE id = CAST('1' AS SIGNED) GROUP BY c",
            None,
        ),
        ("SELECT b FROM t WHERE id = '1' GROUP BY c", None),
        ("SELECT b FROM t WHERE id = -1 GROUP BY c", None),
        (
            "SELECT b FROM t WHERE id IN (1,2) GROUP BY c",
            Some((NGB, "'gbp.t.b'")),
        ),
        (
            "SELECT t.b FROM t LEFT JOIN s ON s.id = t.id GROUP BY s.id",
            Some((NGB, "'gbp.t.b'")),
        ),
        (
            "SELECT s.x FROM t LEFT JOIN s ON s.id = t.a GROUP BY t.a",
            None,
        ),
        (
            "SELECT s.x FROM t LEFT JOIN s ON s.id = t.a AND s.tid = 3 GROUP BY t.a",
            None,
        ),
        (
            "SELECT s.x FROM t LEFT JOIN s ON s.id = t.a WHERE s.tid = 3 GROUP BY t.a",
            None,
        ),
        (
            "SELECT s.x FROM t LEFT JOIN s ON s.id = 5 GROUP BY t.a",
            None,
        ),
        (
            "SELECT s.x FROM t RIGHT JOIN s ON s.id = t.a GROUP BY t.a",
            Some((NGB, "'gbp.s.x'")),
        ),
        (
            "SELECT s.x FROM t RIGHT JOIN s ON s.id = t.a GROUP BY s.id",
            None,
        ),
        ("SELECT t.a FROM t LEFT JOIN s USING (id) GROUP BY id", None),
        ("SELECT s.x FROM t LEFT JOIN s USING (id) GROUP BY id", None),
        ("SELECT x FROM t JOIN s USING (id) GROUP BY id", None),
        (
            "SELECT dt.b FROM (SELECT id, b FROM t WHERE a = 1) dt GROUP BY dt.id",
            None,
        ),
        (
            "SELECT dt.b FROM (SELECT a, b FROM t WHERE b = 7) dt GROUP BY dt.a",
            None,
        ),
        (
            "SELECT dt.b FROM (SELECT DISTINCT a, b FROM t) dt GROUP BY dt.a",
            Some((NGB, "'dt.b'")),
        ),
        (
            "SELECT dt.b FROM (SELECT id, b FROM t LIMIT 5) dt GROUP BY dt.id",
            None,
        ),
        (
            "SELECT dt.b FROM (SELECT id, b FROM t UNION SELECT 1, 2) dt GROUP BY dt.id",
            Some((NGB, "'dt.b'")),
        ),
        (
            "SELECT dt.m FROM (SELECT a, b, MAX(c) m FROM t GROUP BY a, b) dt GROUP BY dt.a",
            Some((NGB, "'dt.m'")),
        ),
        (
            "SELECT dt.x FROM (SELECT id+1 AS x, b FROM t) dt GROUP BY dt.b",
            Some((NGB, "'dt.x'")),
        ),
        (
            "SELECT dt.x FROM (SELECT id+1 AS x, a FROM t) dt GROUP BY dt.a",
            Some((NGB, "'dt.x'")),
        ),
        (
            "WITH c AS (SELECT id, b FROM t) SELECT b FROM c GROUP BY id",
            None,
        ),
        (
            "SELECT a FROM t GROUP BY a, b WITH ROLLUP HAVING b > 0",
            None,
        ),
        ("SELECT c, a FROM t WHERE u = 3 GROUP BY c", None),
        (
            "SELECT t.a FROM t JOIN s ON t.id = s.tid GROUP BY s.tid",
            None,
        ),
        (
            "SELECT t.a FROM t JOIN s ON t.id = s.tid WHERE s.id = 1 GROUP BY s.x",
            None,
        ),
        (
            "SELECT b FROM t GROUP BY a ORDER BY NULL",
            Some((NGB, "#1 of SELECT list")),
        ),
        ("SELECT a FROM t GROUP BY a ORDER BY 1", None),
        (
            "SELECT a, b FROM t GROUP BY a ORDER BY 2",
            Some((NGB, "#2 of SELECT list")),
        ),
        ("SELECT GROUPING(a), a FROM t GROUP BY a WITH ROLLUP", None),
        (
            "SELECT a AS z, b FROM t GROUP BY z",
            Some((
                NGB,
                "#2 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'gbp.t.b'",
            )),
        ),
        (
            "SELECT a AS b FROM t GROUP BY b",
            Some((
                NGB,
                "#1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'gbp.t.a'",
            )),
        ),
        (
            "SELECT t.b AS a FROM t GROUP BY a",
            Some((NGB, "'gbp.t.b'")),
        ),
        ("SELECT b FROM t GROUP BY t.a + 0", Some((NGB, "'gbp.t.b'"))),
        ("SELECT `b` FROM t GROUP BY `a`", Some((NGB, "'gbp.t.b'"))),
        ("SELECT gbp.t.b FROM t GROUP BY a", Some((NGB, "'gbp.t.b'"))),
        ("SELECT b FROM gbp.t GROUP BY gbp.t.b", None),
    ];
    let mut failures = Vec::new();
    for (sql, want) in cases {
        if std::panic::catch_unwind(|| expect(sql, *want)).is_err() {
            failures.push(*sql);
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} differ: {failures:#?}",
        failures.len(),
        cases.len()
    );
}

#[test]
fn views_are_never_refused() {
    // MySQL merges views (`SELECT id, b FROM v2 GROUP BY id` is accepted
    // through t's key); the gateway doesn't model them, so it accepts.
    expect("SELECT id, b FROM v2 GROUP BY id", None);
    expect("SELECT b FROM v3 GROUP BY a", None);
}

#[test]
fn off_without_the_mode_except_3029() {
    assert_eq!(run("SELECT a, b FROM t GROUP BY a", false), Outcome::Accept);
    assert!(
        matches!(run("SELECT a FROM t ORDER BY MAX(b)", false), Outcome::Refuse(r) if r.code == 3029)
    );
    // 3065 follows the mode too (MySQL 8.4 with sql_mode='' runs it).
    assert_eq!(
        run("SELECT DISTINCT a FROM t ORDER BY b", false),
        Outcome::Accept
    );
}

#[test]
fn the_first_pass_needs_no_definitions() {
    let c = Check {
        current_db: Some("gbp"),
        only_full_group_by: true,
        meta: &NoMeta,
    };
    let opts = LexOptions::mysql84();
    for sql in [
        "SELECT a, COUNT(*) FROM t GROUP BY a",
        "SELECT t.a, s.x, COUNT(*) FROM t JOIN s ON s.tid = t.id GROUP BY t.a, s.x",
        "SELECT COUNT(*), MAX(b) FROM t WHERE a = 1",
        "SELECT a + 1 AS k, SUM(b) FROM t GROUP BY a ORDER BY k",
        "SELECT * FROM t WHERE id = 1",
        "SELECT DISTINCT a FROM t ORDER BY a",
    ] {
        assert_eq!(check(sql.as_bytes(), &opts, &c), Outcome::Accept, "{sql}");
    }
    assert_eq!(
        check(b"SELECT id, a FROM t GROUP BY id", &opts, &c),
        Outcome::Need(vec![("gbp".to_owned(), "t".to_owned())])
    );
    assert!(!may_need(b"SELECT * FROM t WHERE id = 1"));
    assert!(may_need(b"SELECT a FROM t GROUP BY a"));
}

#[test]
fn unknown_columns_are_left_to_mariadb() {
    // MySQL resolves names first: 1054, which MariaDB gives as well.
    expect("SELECT b FROM t GROUP BY nocol", None);
    expect("SELECT b, nocol FROM t GROUP BY a", None);
    expect("SELECT b FROM t WHERE nocol = 1 GROUP BY a", None);
    expect("SELECT b FROM t GROUP BY t.nocol", None);
}
