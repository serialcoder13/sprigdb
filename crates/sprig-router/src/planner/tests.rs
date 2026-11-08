#![allow(clippy::unwrap_used, clippy::panic)]

use super::*;
use crate::topology::TopologyFile;

const DEF: &str = "t1_d1_g1_s1";

fn topo() -> crate::topology::StaticTopology {
    let f: TopologyFile = serde_json::from_str(
        r#"{"tenants": {"a": {
          "groups": [
            {"id": 9, "key_class": "int", "shards": [
              {"shard": 1, "schema": "t1_d1_g9_s1", "cluster": "c1"},
              {"shard": 2, "schema": "t1_d1_g9_s2", "cluster": "c1"},
              {"shard": 3, "schema": "t1_d1_g9_s3", "cluster": "c1"},
              {"shard": 4, "schema": "t1_d1_g9_s4", "cluster": "c1"}]},
            {"id": 8, "key_class": "int", "shards": [
              {"shard": 1, "schema": "t1_d1_g8_s1", "cluster": "c1"},
              {"shard": 2, "schema": "t1_d1_g8_s2", "cluster": "c1"}]}],
          "tables": [
            {"schema": "t1_d1_g1_s1", "table": "orders", "group": 9, "key": "cid", "key_ordinal": 1},
            {"schema": "t1_d1_g1_s1", "table": "lines", "group": 9, "key": "cid", "key_ordinal": 0, "upsert_safe": true},
            {"schema": "t1_d1_g1_s1", "table": "products", "group": 8, "key": "pid", "key_ordinal": 0},
            {"schema": "t1_d1_g1_s1", "table": "countries", "group": 9, "reference": "t1_d1_ref"}]}}}"#,
    )
    .unwrap();
    f.tenants["a"].build().unwrap()
}

fn p(sql: &str) -> Plan {
    let t = topo();
    let cx = PlanCtx {
        topo: &t,
        default_schema: Some(DEF),
        session_cluster: "c1",
        max_route_values: 1024,
        lookup_batch: 256,
    };
    plan(sql, &cx)
}

fn reason(sql: &str) -> Reason {
    match p(sql) {
        Plan::Reject(r) => r.reason,
        other => panic!("{sql}: expected a rejection, got {other:?}"),
    }
}

fn pass(sql: &str) -> Target {
    match p(sql) {
        Plan::Pass(t) => t,
        other => panic!("{sql}: expected Pass, got {other:?}"),
    }
}

fn read(sql: &str) -> ReadPlan {
    match p(sql) {
        Plan::Read(r) => r,
        other => panic!("{sql}: expected Read, got {other:?}"),
    }
}

#[test]
fn unsharded_statements_are_not_routed() {
    assert_eq!(
        p("SELECT * FROM t1_d1_g1_s1.users WHERE id = 1"),
        Plan::NotRouted
    );
    assert_eq!(p("SET @x = 1"), Plan::NotRouted);
}

#[test]
fn key_equality_passes_through_to_one_shard() {
    let t = pass("SELECT * FROM orders WHERE cid = 42");
    assert!(t.sql.contains("`t1_d1_g9_s"), "{}", t.sql);
    assert_eq!(t.cluster, "c1");
    // Same shard for the string form below 2^53 and for 42.0.
    assert_eq!(pass("SELECT * FROM orders WHERE cid = '42'").shard, t.shard);
    assert_eq!(pass("SELECT * FROM orders WHERE cid = 42.0").shard, t.shard);
    assert_eq!(
        pass("SELECT * FROM t1_d1_g1_s1.orders o WHERE o.cid = 42 AND o.x > 3").shard,
        t.shard
    );
}

#[test]
fn inexact_predicates_scatter() {
    for sql in [
        "SELECT * FROM orders WHERE cid = '042x'",
        "SELECT * FROM orders WHERE cid = 42.5",
        "SELECT * FROM orders WHERE cid > 42",
        "SELECT * FROM orders WHERE cid = 42 OR x = 1",
        "SELECT * FROM orders WHERE cid = '9007199254740993'",
    ] {
        let r = read(sql);
        assert_eq!(r.kind, ReadKind::Scatter, "{sql}");
        assert_eq!(r.targets.len(), 4, "{sql}");
    }
}

#[test]
fn in_lists_and_ors_route_to_their_shards() {
    let r = match p("SELECT * FROM orders WHERE cid IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)") {
        Plan::Read(r) => r,
        Plan::Pass(_) => return,
        other => panic!("{other:?}"),
    };
    assert!(r.targets.len() <= 4);
    let one = pass("SELECT * FROM orders WHERE cid = 7 OR cid = 7");
    assert!(one.sql.contains("t1_d1_g9_s"));
}

#[test]
fn colocated_join_and_reference_tables_stay_on_the_shard() {
    let t = pass(
        "SELECT * FROM orders o JOIN lines l ON l.cid = o.cid JOIN countries c ON c.code = o.cc WHERE o.cid = 5",
    );
    assert!(t.sql.contains("t1_d1_ref"), "{}", t.sql);
    let n = t.sql.matches("t1_d1_g9_s").count();
    assert_eq!(n, 2, "{}", t.sql);
    let r = read("SELECT o.id, l.n FROM orders o JOIN lines l ON l.cid = o.cid");
    assert_eq!(r.targets.len(), 4);
    assert!(
        r.targets[0].sql.contains("t1_d1_g9_s1`.lines")
            || r.targets[0].sql.contains("`t1_d1_g9_s1`.`lines`")
            || r.targets[0].sql.contains("`t1_d1_g9_s1`.lines"),
        "{}",
        r.targets[0].sql
    );
}

#[test]
fn ordered_merge_adds_weight_columns_and_pushes_limit() {
    let r = read("SELECT id, name FROM orders ORDER BY name DESC, id LIMIT 20 OFFSET 40");
    assert_eq!(r.kind, ReadKind::Merge);
    assert_eq!(r.order.len(), 2);
    assert!(r.order[0].desc && !r.order[1].desc);
    assert_eq!(r.offset, 40);
    assert_eq!(r.limit, Some(20));
    assert_eq!(r.pushed_limit, Some(60));
    let sql = &r.targets[0].sql;
    assert!(
        sql.contains("WEIGHT_STRING(name LEVEL 1) AS __sprig_h1"),
        "{sql}"
    );
    assert!(sql.contains("WEIGHT_STRING(name LEVEL 3)"), "{sql}");
    assert!(sql.contains("LIMIT 60"), "{sql}");
    assert!(!sql.contains("OFFSET"), "{sql}");
    assert_eq!(r.order[0].cols.value, ColRef::Visible(1));
}

#[test]
fn order_by_hidden_expression_and_position() {
    let r = read("SELECT id FROM orders ORDER BY total * 2, 1");
    assert_eq!(r.order[0].cols.value, ColRef::Hidden(0));
    assert_eq!(r.order[1].cols.value, ColRef::Visible(0));
    assert!(
        r.targets[0].sql.contains("total * 2 AS __sprig_h1"),
        "{}",
        r.targets[0].sql
    );
    assert_eq!(reason("SELECT * FROM orders ORDER BY 2"), Reason::Ordering);
    assert_eq!(
        reason("SELECT id FROM orders ORDER BY RAND()"),
        Reason::Ordering
    );
}

#[test]
fn split_aggregates() {
    let r = read(
        "SELECT region, COUNT(*), SUM(total), AVG(total) AS a, MIN(name) FROM orders GROUP BY region HAVING COUNT(*) > 1 ORDER BY a DESC LIMIT 3",
    );
    assert_eq!(r.kind, ReadKind::Aggregate);
    let a = r.agg.as_ref().unwrap();
    assert_eq!(a.groups.len(), 1);
    assert_eq!(a.aggs.len(), 4);
    assert!(a.having.is_some());
    assert_eq!(a.order, vec![FinalSort::Agg(2, true)]);
    let sql = &r.targets[0].sql;
    assert!(sql.contains("SUM(total) AS __sprig_h"), "{sql}");
    assert!(sql.contains("COUNT(total) AS __sprig_h"), "{sql}");
    assert!(!sql.contains("HAVING"), "{sql}");
    assert!(!sql.contains("LIMIT"), "{sql}");
    assert!(sql.contains("GROUP BY region"), "{sql}");
}

#[test]
fn groups_with_the_key_push_down() {
    let r = read(
        "SELECT cid, COUNT(*) FROM orders GROUP BY cid HAVING COUNT(*) > 2 ORDER BY 2 DESC LIMIT 5",
    );
    assert_eq!(r.kind, ReadKind::Merge);
    assert!(r.targets[0].sql.contains("HAVING"), "{}", r.targets[0].sql);
    assert!(r.targets[0].sql.contains("LIMIT 5"), "{}", r.targets[0].sql);
    let r = read("SELECT DISTINCT cid, x FROM orders");
    assert_eq!(r.kind, ReadKind::Scatter);
}

#[test]
fn rejections_name_their_reasons() {
    assert_eq!(
        reason("SELECT COUNT(DISTINCT x) FROM orders"),
        Reason::Aggregate
    );
    assert_eq!(reason("SELECT SUM(x) + 1 FROM orders"), Reason::Aggregate);
    assert_eq!(
        reason("SELECT x, COUNT(*) FROM orders GROUP BY x WITH ROLLUP"),
        Reason::Aggregate
    );
    assert_eq!(
        reason("SELECT ROW_NUMBER() OVER (ORDER BY id) FROM orders"),
        Reason::WindowFunction
    );
    assert_eq!(
        reason("SELECT id FROM orders UNION SELECT id FROM lines"),
        Reason::SetOperation
    );
    assert_eq!(
        reason("SELECT * FROM users WHERE id IN (SELECT uid FROM orders)"),
        Reason::Subquery
    );
    assert_eq!(
        reason("UPDATE orders SET x = 1 ORDER BY id LIMIT 1"),
        Reason::LimitedDml
    );
    assert_eq!(
        reason("UPDATE orders SET cid = 3 WHERE id = 1"),
        Reason::KeyChange
    );
    assert_eq!(
        reason("INSERT INTO orders (id, x) VALUES (1, 2)"),
        Reason::AutoIncrement
    );
    assert_eq!(
        reason("INSERT INTO orders (id, cid) VALUES (1, NULL)"),
        Reason::AutoIncrement
    );
    assert_eq!(
        reason("INSERT INTO orders (id, cid) VALUES (1, 2) ON DUPLICATE KEY UPDATE x = 1"),
        Reason::UpsertUnique
    );
    assert_eq!(
        reason("INSERT INTO orders (id, cid) SELECT 1, 2"),
        Reason::InsertSelect
    );
    assert_eq!(
        reason("SELECT SQL_CALC_FOUND_ROWS * FROM orders"),
        Reason::FoundRows
    );
    assert_eq!(
        reason("SELECT * FROM countries c LEFT JOIN orders o ON o.cc = c.code"),
        Reason::JoinCondition
    );
}

#[test]
fn inserts_split_by_shard() {
    match p(
        "INSERT INTO orders (id, cid, x) VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c'), (4, 4, 'd'), (5, 5, 'e')",
    ) {
        Plan::Write(w) => {
            assert_eq!(w.kind, WriteKind::Insert);
            assert_eq!(w.rows, 5);
            let total: usize = w.targets.iter().map(|t| t.sql.matches("('").count()).sum();
            assert_eq!(total, 0);
            let rows: usize = w
                .targets
                .iter()
                .map(|t| t.sql.matches("), (").count() + 1)
                .sum();
            assert_eq!(rows, 5);
        }
        Plan::Pass(_) => {}
        other => panic!("{other:?}"),
    }
    let t = pass("INSERT INTO orders (id, cid) VALUES (9, 42)");
    assert_eq!(t.shard, pass("SELECT * FROM orders WHERE cid = 42").shard);
    // No column list: the key's ordinal.
    assert_eq!(
        pass("INSERT INTO orders VALUES (9, 42, 'x')").shard,
        t.shard
    );
    // Upsert on a table whose unique keys hold the key.
    assert!(matches!(
        p("INSERT INTO lines VALUES (42, 1) ON DUPLICATE KEY UPDATE n = n + 1"),
        Plan::Pass(_)
    ));
}

#[test]
fn updates_and_deletes() {
    assert!(matches!(
        p("UPDATE orders SET x = 1 WHERE cid = 3"),
        Plan::Pass(_)
    ));
    match p("DELETE FROM orders WHERE x = 1") {
        Plan::Write(w) => assert_eq!(w.targets.len(), 4),
        other => panic!("{other:?}"),
    }
    match p("UPDATE orders SET x = x + 1 WHERE cid IN (1, 2, 3, 4, 5, 6, 7, 8)") {
        Plan::Write(w) => assert!(w.targets.len() > 1),
        Plan::Pass(_) => {}
        other => panic!("{other:?}"),
    }
}

#[test]
fn lookup_join_between_groups() {
    match p(
        "SELECT o.id, p.name FROM orders o JOIN products p ON p.pid = o.pid WHERE o.cid = 7 ORDER BY o.id",
    ) {
        Plan::LookupJoin(j) => {
            assert!(j.inner_by_key);
            assert!(!j.left);
            assert_eq!(j.outputs, vec![JoinCol::Outer(0), JoinCol::Inner(0)]);
            assert!(
                j.inner_template.contains("p.pid IN (\u{1}K)"),
                "{}",
                j.inner_template
            );
            assert_eq!(j.outer.targets.len(), 1);
            assert!(
                j.outer.targets[0].sql.contains("__sprig_xw0"),
                "{}",
                j.outer.targets[0].sql
            );
        }
        other => panic!("{other:?}"),
    }
    match p(
        "SELECT o.id, p.name FROM orders o LEFT JOIN products p ON p.pid = o.pid AND p.active = 1 ORDER BY o.id LIMIT 10",
    ) {
        Plan::LookupJoin(j) => {
            assert!(j.left);
            assert_eq!(j.outer.limit, Some(10));
            assert!(
                j.inner_template.contains("AND (p.active = 1)"),
                "{}",
                j.inner_template
            );
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        reason("SELECT o.id FROM orders o JOIN products p ON p.pid > o.pid"),
        Reason::JoinCondition
    );
    assert_eq!(
        reason(
            "SELECT o.id, p.name FROM orders o JOIN products p ON p.pid = o.pid ORDER BY p.name"
        ),
        Reason::JoinCondition
    );
}

#[test]
fn explain_sprig_wraps_the_plan() {
    assert!(matches!(
        p("EXPLAIN FORMAT=SPRIG SELECT * FROM orders ORDER BY id"),
        Plan::Explain(_)
    ));
    let t = pass("EXPLAIN SELECT * FROM orders ORDER BY id");
    assert!(t.sql.starts_with("EXPLAIN SELECT"), "{}", t.sql);
}

#[test]
fn string_literals_keep_their_escapes() {
    let t = pass(r"SELECT 'a\'b', 'c\\d' FROM orders WHERE cid = 1");
    assert!(t.sql.contains(r"'a\'b'"), "{}", t.sql);
    assert!(t.sql.contains(r"'c\\d'"), "{}", t.sql);
}

#[test]
fn lock_clauses_render_in_mariadb_syntax() {
    let t = pass("SELECT * FROM orders WHERE cid = 1 LOCK IN SHARE MODE");
    assert!(!t.sql.contains("FOR SHARE"), "{}", t.sql);
    let t = pass("SELECT * FROM orders WHERE cid = 1 FOR UPDATE");
    assert!(t.sql.ends_with("FOR UPDATE"), "{}", t.sql);
}

#[test]
fn share_lock_survives_a_scatter() {
    let r = read("select id from orders order by id lock  in share   mode;");
    assert!(
        r.targets
            .iter()
            .all(|t| t.sql.ends_with(" LOCK IN SHARE MODE")),
        "{:?}",
        r.targets[0].sql
    );
}
