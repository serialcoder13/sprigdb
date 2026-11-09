#![allow(clippy::unwrap_used, clippy::panic)]

use crate::gather::GatherPlan;
use crate::plan::Plan;
use crate::planner::{IdMode, ParamLit, PlanCtx, parse_template, plan, plan_prepared};
use crate::reject::Reason;
use crate::topology::TopologyFile;

const DEF: &str = "t1_d1";

fn topo() -> crate::topology::StaticTopology {
    let f: TopologyFile = serde_json::from_str(
        r#"{"tenants": {"a": {
          "groups": [
            {"id": 9, "key_class": "int", "shards": [
              {"shard": 1, "schema": "t1_d1_g9_s1", "cluster": "c1"},
              {"shard": 2, "schema": "t1_d1_g9_s2", "cluster": "c2"},
              {"shard": 3, "schema": "t1_d1_g9_s3", "cluster": "c1"},
              {"shard": 4, "schema": "t1_d1_g9_s4", "cluster": "c2"}]},
            {"id": 8, "key_class": "int", "shards": [
              {"shard": 1, "schema": "t1_d1_g8_s1", "cluster": "c1"},
              {"shard": 2, "schema": "t1_d1_g8_s2", "cluster": "c2"}]},
            {"id": 7, "key_class": "int", "shards": [
              {"shard": 1, "schema": "t1_d1_g7_s1", "cluster": "c2"},
              {"shard": 2, "schema": "t1_d1_g7_s2", "cluster": "c2"}]},
            {"id": 6, "key_class": "int", "shards": [
              {"shard": 1, "schema": "t1_d1_ref", "cluster": "c1"},
              {"shard": 2, "schema": "t1_d1_ref", "cluster": "c2"}]}],
          "tables": [
            {"schema": "t1_d1", "table": "orders", "group": 9, "key": "cid", "key_ordinal": 1},
            {"schema": "t1_d1", "table": "customers", "group": 9, "key": "cid", "key_ordinal": 0},
            {"schema": "t1_d1", "table": "products", "group": 8, "key": "pid", "key_ordinal": 0},
            {"schema": "t1_d1", "table": "stock", "group": 7, "key": "pid", "key_ordinal": 0},
            {"schema": "t1_d1", "table": "countries", "group": 6, "reference": "t1_d1_ref"}]}}}"#,
    )
    .unwrap();
    f.tenants["a"].build().unwrap()
}

fn cx(t: &crate::topology::StaticTopology) -> PlanCtx<'_> {
    PlanCtx {
        topo: t,
        default_schema: Some(DEF),
        session_cluster: "c1",
        max_route_values: 1024,
        lookup_batch: 256,
        ids: IdMode::default(),
    }
}

fn g(sql: &str) -> GatherPlan {
    let t = topo();
    match plan(sql, &cx(&t)) {
        Plan::Gather(g) => *g,
        other => panic!("{sql}: expected Gather, got {other:?}"),
    }
}

fn refused(sql: &str) -> Reason {
    let t = topo();
    match plan(sql, &cx(&t)) {
        Plan::Reject(r) => r.reason,
        other => panic!("{sql}: expected a rejection, got {other:?}"),
    }
}

#[test]
fn what_r1_refused_is_gathered() {
    for (sql, why) in [
        (
            "SELECT cid, ROW_NUMBER() OVER (ORDER BY cid) FROM customers",
            Reason::WindowFunction,
        ),
        (
            "SELECT cid FROM customers UNION SELECT cid FROM orders",
            Reason::SetOperation,
        ),
        (
            "SELECT cid FROM customers WHERE cid IN (SELECT cid FROM orders WHERE oid > 15)",
            Reason::Subquery,
        ),
        (
            "SELECT o.oid, p.pname FROM orders o JOIN products p ON p.pid > o.pid",
            Reason::JoinCondition,
        ),
        (
            "SELECT COUNT(DISTINCT name) FROM customers",
            Reason::Aggregate,
        ),
        (
            "WITH x AS (SELECT cid FROM orders) SELECT COUNT(*) FROM x",
            Reason::DerivedTable,
        ),
        (
            "SELECT o.oid, p.pid, s.qty FROM orders o JOIN products p ON p.pid = o.pid JOIN stock s ON s.pid = p.pid",
            Reason::JoinDepth,
        ),
    ] {
        let p = g(sql);
        assert_eq!(p.fallback.reason, why, "{sql}");
        assert_eq!(p.sql, sql);
        assert!(!p.reads_unsharded, "{sql}");
    }
}

#[test]
fn each_table_once_with_every_shard_or_its_copy() {
    let p = g(
        "SELECT c.cid, k.cname, (SELECT COUNT(*) FROM orders o2 WHERE o2.cid = c.cid) FROM customers c JOIN countries k ON k.code = c.cc JOIN orders o ON o.cid = c.cid WHERE o.oid > 1 ORDER BY 1 LIMIT 3",
    );
    let mut names: Vec<&str> = p.tables.iter().map(|t| t.name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, ["countries", "customers", "orders"]);
    let by = |n: &str| p.tables.iter().find(|t| t.name == n).unwrap();
    let orders = by("orders");
    // Named twice: every shard, every row.
    assert_eq!(orders.sources.len(), 4);
    assert_eq!(orders.filter, None);
    // The reference table: the session cluster's copy.
    let k = by("countries");
    assert_eq!(k.sources.len(), 1);
    assert_eq!(k.sources[0].cluster, "c1");
    assert_eq!(k.sources[0].at.schema, "t1_d1_ref");
    assert_eq!(k.schema, "t1_d1");
}

#[test]
fn key_equalities_and_simple_conjuncts_go_to_the_shards() {
    let p = g(
        "SELECT o.oid, ROW_NUMBER() OVER (ORDER BY o.oid) FROM orders o JOIN products p ON p.pid > o.pid WHERE o.cid = 7 AND o.oid BETWEEN 1 AND 9 AND p.pname LIKE 'a%' AND o.note = p.pname AND UPPER(p.pname) = 'X'",
    );
    let o = &p.tables[0];
    assert_eq!(o.name, "orders");
    assert_eq!(o.sources.len(), 1, "{:?}", o.sources);
    assert_eq!(o.alias, "o");
    assert_eq!(
        o.filter.as_deref(),
        Some("(o.cid = 7) AND (o.oid BETWEEN 1 AND 9)")
    );
    let pr = &p.tables[1];
    assert_eq!(pr.sources.len(), 2);
    // `UPPER()` and the join condition stay in the statement.
    assert_eq!(pr.filter.as_deref(), Some("(p.pname LIKE 'a%')"));
}

#[test]
fn outer_join_null_sides_and_odd_literals_keep_every_row() {
    let p = g(
        "SELECT c.cid, p.pid FROM customers c LEFT JOIN products p ON p.pid > c.cid WHERE p.pid IS NULL AND c.name = 'it\\'s' AND c.cid > 3",
    );
    let c = &p.tables[0];
    let pr = &p.tables[1];
    assert_eq!(pr.filter, None, "the null side keeps its rows");
    assert_eq!(c.filter.as_deref(), Some("(c.cid > 3)"));
}

#[test]
fn the_guard_refuses_what_copies_cant_answer() {
    assert_eq!(
        refused("SELECT cid, ROW_NUMBER() OVER () FROM customers FOR UPDATE"),
        Reason::WindowFunction
    );
    assert_eq!(
        refused("SELECT cid, ROW_NUMBER() OVER () FROM customers LOCK IN SHARE MODE"),
        Reason::WindowFunction
    );
    assert_eq!(
        refused("SELECT cid, LAST_INSERT_ID(), ROW_NUMBER() OVER () FROM customers"),
        Reason::Gather
    );
    assert_eq!(
        refused("SELECT cid, my_fn(cid), ROW_NUMBER() OVER () FROM customers"),
        Reason::Routine
    );
    assert_eq!(
        refused("SELECT cid, ROW_NUMBER() OVER () FROM customers WHERE MATCH(name) AGAINST ('x')"),
        Reason::FulltextRelevance
    );
    // (`SELECT … INTO @a` doesn't parse: refused with `parse`.)
    // Not a SELECT: not gathered.
    assert_eq!(
        refused("UPDATE customers SET name = 'x' ORDER BY cid LIMIT 1"),
        Reason::LimitedDml
    );
    // Plain EXPLAIN of a gathered read is refused as before.
    assert_eq!(
        refused("EXPLAIN SELECT cid, ROW_NUMBER() OVER () FROM customers"),
        Reason::WindowFunction
    );
}

#[test]
fn unsharded_tables_are_read_where_they_are() {
    let p = g("SELECT u.name, ROW_NUMBER() OVER () FROM users u JOIN customers c ON c.cid = u.id");
    assert!(p.reads_unsharded);
    assert_eq!(p.tables.len(), 1);
}

#[test]
fn explain_sprig_shows_the_gather() {
    let t = topo();
    match plan(
        "EXPLAIN FORMAT=SPRIG SELECT cid, ROW_NUMBER() OVER () FROM customers",
        &cx(&t),
    ) {
        Plan::Explain(p) => assert!(matches!(*p, Plan::Gather(_))),
        other => panic!("{other:?}"),
    }
}

#[test]
fn prepared_statements_are_gathered_with_their_values() {
    let t = topo();
    let tpl = parse_template(
        "SELECT cid, RANK() OVER (ORDER BY cid) FROM customers WHERE name = ? AND cid > ?",
        &t,
    )
    .unwrap()
    .unwrap();
    let pp = plan_prepared(
        &tpl,
        &[
            ParamLit::Str {
                bytes: b"a'b".to_vec(),
                charset: "utf8mb4".into(),
            },
            ParamLit::Int(5),
        ],
        &cx(&t),
    );
    let Plan::Gather(g) = pp.plan else {
        panic!("{:?}", pp.plan)
    };
    assert_eq!(
        g.sql,
        "SELECT cid, RANK() OVER (ORDER BY cid) FROM customers WHERE name = _utf8mb4 X'612762' AND cid > 5"
    );
}
