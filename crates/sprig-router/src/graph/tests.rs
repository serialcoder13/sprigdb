#![allow(clippy::unwrap_used, clippy::panic)]

use super::*;
use crate::plan::{Plan, ReadKind};
use crate::planner::{IdMode, PlanCtx, parse_template, plan, plan_graph};
use crate::topology::{StaticTopology, Topology, TopologyFile};

const DEF: &str = "t1_d1_g1_s1";

/// Orders on four shards over two clusters, a reference table on both,
/// and a graph's group (id 30) without tables.
fn topo() -> StaticTopology {
    let f: TopologyFile = serde_json::from_str(
        r#"{"tenants": {"a": {
          "groups": [
            {"id": 9, "key_class": "int", "shards": [
              {"shard": 1, "schema": "t1_d1_g9_s1", "cluster": "c1"},
              {"shard": 2, "schema": "t1_d1_g9_s2", "cluster": "c1"},
              {"shard": 3, "schema": "t1_d1_g9_s3", "cluster": "c2"},
              {"shard": 4, "schema": "t1_d1_g9_s4", "cluster": "c2"}]},
            {"id": 7, "key_class": "string:utf8mb4_bin", "shards": [
              {"shard": 1, "schema": "t1_d1_g7_s1", "cluster": "c1"},
              {"shard": 2, "schema": "t1_d1_g7_s2", "cluster": "c2"}]}],
          "tables": [
            {"schema": "t1_d1_g1_s1", "table": "orders", "group": 9, "key": "cid", "key_ordinal": 1},
            {"schema": "t1_d1_g1_s1", "table": "people", "group": 7, "key": "email", "key_ordinal": 0},
            {"schema": "t1_d1_g1_s1", "table": "countries", "group": 9, "reference": "t1_d1_ref"}]}}}"#,
    )
    .unwrap();
    let mut t = f.tenants["a"].build().unwrap();
    let orders = t.table(DEF, "orders").unwrap().group.clone();
    let mut g = (*orders).clone();
    g.id = 30;
    for s in &mut g.shards {
        s.schema = s.schema.replace("_g9_", "_g30_");
    }
    t.insert_group(Arc::new(g));
    t
}

fn cx(t: &StaticTopology) -> PlanCtx<'_> {
    PlanCtx {
        topo: t,
        default_schema: Some(DEF),
        session_cluster: "c1",
        max_route_values: 1024,
        lookup_batch: 256,
        ids: IdMode::default(),
    }
}

const CHAPTER: &str = "SELECT g.name, SUM(g.total) AS spent
FROM GRAPH_TABLE (shop
       MATCH (c IS Person)-[o IS BOUGHT]->(p IS Product)
       WHERE c.email = 'alice@example.com'
       COLUMNS (p.name AS name, o.qty * p.price AS total)) AS g
GROUP BY g.name";

#[test]
fn finds_and_translates_the_chapters_example() {
    let v = find(CHAPTER).expect("ok");
    assert_eq!(v.len(), 1);
    let g = &v[0];
    assert_eq!(g.graph, "shop");
    assert_eq!(g.columns, ["name", "total"]);
    assert_eq!(
        g.cypher,
        "MATCH (c:Person)-[o:BOUGHT]->(p:Product) WHERE c.email = 'alice@example.com' RETURN p.name AS `name`, o.qty * p.price AS `total`"
    );
    assert!(CHAPTER[g.span.0..g.span.1].starts_with("GRAPH_TABLE"));
    assert!(CHAPTER[g.span.1..].trim_start().starts_with("AS g"));
    // The keyword inside a string is not a pattern word.
    assert_eq!(
        pattern_to_cypher("(a IS L {n: 'x IS y'})"),
        "(a:L {n: 'x IS y'})"
    );
    assert!(find("SELECT 1").expect("ok").is_empty());
    assert!(find("SELECT * FROM GRAPH_TABLE (g COLUMNS (a))").is_err());
    assert!(mentions(CHAPTER) && !mentions("SELECT 1"));
}

#[test]
fn placeholders_survive_a_rewrite_and_are_found_again() {
    let calls = find(CHAPTER).unwrap();
    let ph = with_placeholders(CHAPTER, &calls, 77);
    assert!(!ph.contains("GRAPH_TABLE"));
    assert!(ph.contains("sprig_graph_table_pending(77, 0)"), "{ph}");
    // Chapter 07's rewrite changes text before the placeholder.
    let rewritten = ph.replace("SELECT g.name", "SELECT `g`.`name`");
    let s = slots(&rewritten, 77);
    assert_eq!(s.len(), 1);
    let (call, (a, b)) = s[0];
    assert_eq!(call, 0);
    assert!(
        rewritten[a..b].starts_with("(SELECT NULL AS `name`"),
        "{rewritten}"
    );
    assert!(rewritten[b..].trim_start().starts_with("AS g"));
    // Another nonce's placeholder is not ours.
    assert!(slots(&rewritten, 78).is_empty());
}

#[test]
fn rows_are_one_flat_value_list() {
    let cols = vec!["a".to_owned(), "b".to_owned()];
    let s = rows_sql(
        &cols,
        &[
            vec![GraphLit::Int(1), GraphLit::Str("it's".into())],
            vec![GraphLit::Null, GraphLit::Other("2.5e0".into())],
            vec![GraphLit::Int(-3), GraphLit::Str("a\\b".into())],
        ],
    );
    assert_eq!(
        s,
        "SELECT 1 AS `a`, 'it''s' AS `b` UNION ALL VALUES (NULL, 2.5e0), (-3, 'a\\\\b')"
    );
    assert_eq!(
        rows_sql(&cols, &[]),
        "SELECT NULL AS `a`, NULL AS `b` FROM DUAL WHERE FALSE"
    );
    // The router's parser reads it, however many rows (no set operation
    // per row, so no deep nesting).
    let many: Vec<Vec<GraphLit>> = (0..5000).map(|i| vec![GraphLit::Int(i)]).collect();
    let big = format!("SELECT * FROM ({}) g", rows_sql(&["a".to_owned()], &many));
    let t = topo();
    assert_eq!(plan(&big, &cx(&t)), Plan::NotRouted);
    let q = format!(
        "SELECT o.oid FROM ({}) g JOIN orders o ON o.cid = g.a",
        rows_sql(&["a".to_owned()], &many)
    );
    assert!(matches!(plan(&q, &cx(&t)), Plan::Read(_)));
}

/// Before round 6 a derived table joined with a multi-shard table was
/// gathered (copied to the session's cluster); literal rows now join
/// inside each shard, like a reference table.
#[test]
fn literal_rows_join_inside_each_shard() {
    let t = topo();
    let q = "SELECT g.a, o.oid FROM (SELECT 1 AS a UNION ALL VALUES (2), (3)) g JOIN orders o ON o.cid = g.a ORDER BY o.oid";
    match plan(q, &cx(&t)) {
        Plan::Read(r) => {
            assert_eq!(r.kind, ReadKind::Merge);
            assert_eq!(r.targets.len(), 4);
            assert!(
                r.targets.iter().all(|x| x.sql.contains("VALUES (2), (3)")),
                "{:?}",
                r.targets
            );
        }
        other => panic!("expected a read, got {other:?}"),
    }
    // Aggregates split as for any read of one group.
    let q = "SELECT g.a, COUNT(*) FROM (SELECT 1 AS a UNION ALL SELECT 2) g JOIN orders o ON o.cid = g.a GROUP BY g.a";
    assert!(
        matches!(plan(q, &cx(&t)), Plan::Read(r) if r.kind == ReadKind::Aggregate),
        "{q}"
    );
    // One key on one shard: the statement passes there whole.
    let q = "SELECT * FROM (SELECT 5 AS a) g JOIN orders o ON o.cid = g.a WHERE o.cid = 5";
    assert!(matches!(plan(q, &cx(&t)), Plan::Pass(_)), "{q}");
    // Literal rows on the preserved side of an outer join would repeat on
    // every shard: still gathered.
    let q = "SELECT * FROM (SELECT 1 AS a) g LEFT JOIN orders o ON o.cid = g.a";
    assert!(matches!(plan(q, &cx(&t)), Plan::Gather(_)), "{q}");
    // A derived table reading a table is not literal rows.
    let q = "SELECT * FROM (SELECT cid AS a FROM orders) g JOIN orders o ON o.cid = g.a";
    assert!(matches!(plan(q, &cx(&t)), Plan::Gather(_)), "{q}");
}

fn graph_plan(sql: &str, nonce: u64, steps: Vec<GraphStep>) -> (Vec<GraphCall>, Plan) {
    let t = topo();
    let calls = find(sql).unwrap();
    let ph = with_placeholders(sql, &calls, nonce);
    let p = plan_graph(&ph, nonce, &calls, steps, &cx(&t));
    (calls, p)
}

#[test]
fn graph_plans_bind_rows_and_narrow_to_the_keys_shards() {
    let sql = "SELECT o.oid, g.name FROM GRAPH_TABLE (shop MATCH (c IS Person) COLUMNS (c.id AS cid, c.name AS name)) AS g JOIN orders o ON o.cid = g.cid ORDER BY o.oid";
    let step = GraphStep {
        call: 0,
        graph: "d.shop".into(),
        group: Some(30),
        shards: Vec::new(),
        tiers: vec!["Match: start c then 0 hop(s)".into()],
    };
    let (calls, p) = graph_plan(sql, 5, vec![step]);
    let Plan::Graph(g) = p else {
        panic!("expected a graph plan, got {p:?}")
    };
    assert_eq!(g.calls, calls);
    // The graph's shards come from the topology's group.
    assert_eq!(g.steps[0].shards.len(), 4);
    assert!(g.steps[0].shards[2].schema.contains("_g30_"));
    assert_eq!(g.steps[0].shards[2].cluster, "c2");
    // The outer statement with empty rows reads every shard.
    assert!(
        matches!(&*g.outer_plan, Plan::Read(r) if r.targets.len() == 4),
        "{:?}",
        g.outer_plan
    );
    assert_eq!(g.keys.len(), 1);
    assert_eq!((g.keys[0].call, g.keys[0].column), (0, 0));
    let t = topo();
    let orders = t.table(DEF, "orders").unwrap().group.clone();
    let key = |i: i64| {
        orders.shards[orders.route(orders.hash_of(&KeyValue::Int(i.into())).unwrap())].shard
    };
    // Two values: only their shards.
    let rows = vec![CallRows {
        columns: calls[0].columns.clone(),
        rows: vec![
            vec![GraphLit::Int(11), GraphLit::Str("x".into())],
            vec![GraphLit::Int(12), GraphLit::Null],
            vec![GraphLit::Null, GraphLit::Str("y".into())],
        ],
    }];
    let bound = g.bind(&rows);
    assert!(
        bound.contains(
            "(SELECT 11 AS `cid`, 'x' AS `name` UNION ALL VALUES (12, NULL), (NULL, 'y')) AS g"
        ),
        "{bound}"
    );
    let Plan::Read(r) = g.narrow(plan(&bound, &cx(&t)), &rows) else {
        panic!("expected a read")
    };
    let want: BTreeSet<u32> = [key(11), key(12)].into_iter().collect();
    let got: BTreeSet<u32> = r.targets.iter().map(|x| x.shard).collect();
    assert_eq!(got, want);
    // A float doesn't route exactly: every shard.
    let rows = vec![CallRows {
        columns: calls[0].columns.clone(),
        rows: vec![vec![GraphLit::Other("1.5e0".into()), GraphLit::Null]],
    }];
    let Plan::Read(r) = g.narrow(plan(&g.bind(&rows), &cx(&t)), &rows) else {
        panic!("expected a read")
    };
    assert_eq!(r.targets.len(), 4);
    // No rows: one shard still answers (the columns).
    let rows = vec![CallRows {
        columns: calls[0].columns.clone(),
        rows: Vec::new(),
    }];
    let Plan::Read(r) = g.narrow(plan(&g.bind(&rows), &cx(&t)), &rows) else {
        panic!("expected a read")
    };
    assert_eq!(r.targets.len(), 1);
}

#[test]
fn string_keys_narrow_and_outer_joins_dont() {
    let sql = "SELECT p.email FROM GRAPH_TABLE (g MATCH (c IS P) COLUMNS (c.email AS e)) AS x JOIN people p ON x.e = p.email";
    let (_, p) = graph_plan(sql, 9, Vec::new());
    let Plan::Graph(g) = p else { panic!() };
    assert_eq!(g.keys.len(), 1);
    let sql = "SELECT p.email FROM GRAPH_TABLE (g MATCH (c IS P) COLUMNS (c.email AS e)) AS x LEFT JOIN people p ON x.e = p.email";
    let (_, p) = graph_plan(sql, 9, Vec::new());
    let Plan::Graph(g) = p else { panic!() };
    assert!(g.keys.is_empty());
    // In WHERE, a LEFT JOIN's table must match: it narrows.
    let sql = "SELECT p.email FROM GRAPH_TABLE (g MATCH (c IS P) COLUMNS (c.email AS e)) AS x, people p WHERE p.email = x.e";
    let (_, p) = graph_plan(sql, 9, Vec::new());
    let Plan::Graph(g) = p else { panic!() };
    assert_eq!(g.keys.len(), 1);
}

#[test]
fn explain_shows_the_graph_and_templates_refuse_it() {
    let sql = format!("EXPLAIN FORMAT=SPRIG {CHAPTER}");
    let (_, p) = graph_plan(&sql, 3, Vec::new());
    assert!(
        matches!(&p, Plan::Explain(x) if matches!(&**x, Plan::Graph(_))),
        "{p:?}"
    );
    assert_eq!(p.kind(), "explain");
    let t = topo();
    assert!(parse_template(CHAPTER, &t).is_err());
    // A placeholder that a rewrite lost: refused, never run without rows.
    let calls = find(CHAPTER).unwrap();
    let p = plan_graph("SELECT 1", 3, &calls, Vec::new(), &cx(&t));
    assert!(matches!(p, Plan::Reject(_)), "{p:?}");
}

/// R8 round 7 (G61): integer filters on a call's columns in the top
/// `SELECT`'s `WHERE` and inner joins' `ON` go into its Cypher, keeping
/// every row that isn't a number (MariaDB converts strings to compare);
/// strings, expressions and other blocks' filters stay in SQL.
#[test]
fn integer_filters_on_graph_columns_go_into_the_cypher() {
    let sql = "SELECT g.name FROM GRAPH_TABLE (shop MATCH (c IS Person) WHERE c.age > 1 \
               COLUMNS (c.id AS cid, c.name AS name, c.age + 1 AS older)) AS g \
               JOIN orders o ON o.cid = g.cid AND g.cid <= 40 \
               WHERE 5 < g.cid AND g.cid IN (6, 7, -8) AND g.name = 'x' AND g.older = 3 \
               AND g.cid BETWEEN 1 AND 100 AND g.name IS NOT NULL AND (g.cid = 1 OR g.cid = 2)";
    let (_, p) = graph_plan(sql, 5, Vec::new());
    let Plan::Graph(g) = p else { panic!("{p:?}") };
    let f = &g.filters[0];
    assert_eq!(f.len(), 4 + 1, "{f:?}");
    let n = |t: &str| {
        format!("(c.id IS NOT NULL AND (NOT valueType(c.id) IN ['INTEGER', 'FLOAT'] OR {t}))")
    };
    assert!(f.contains(&n("c.id > 5")), "{f:?}");
    assert!(
        f.contains(&n("(c.id = 6 OR c.id = 7 OR c.id = -8)")),
        "{f:?}"
    );
    assert!(f.contains(&n("(c.id >= 1 AND c.id <= 100)")), "{f:?}");
    assert!(f.contains(&n("c.id <= 40")), "{f:?}");
    assert!(f.contains(&"c.name IS NOT NULL".to_owned()), "{f:?}");
    let cypher = g.cypher(0);
    assert!(
        cypher.starts_with("MATCH (c:Person) WHERE (c.age > 1) AND (c.id IS NOT NULL"),
        "{cypher}"
    );
    assert!(
        cypher.ends_with("RETURN c.id AS `cid`, c.name AS `name`, c.age + 1 AS `older`"),
        "{cypher}"
    );
    // A LEFT JOIN's ON, a subquery's WHERE and a float aren't pushed.
    let sql = "SELECT o.oid FROM orders o LEFT JOIN GRAPH_TABLE (shop MATCH (c) COLUMNS (c.id AS cid)) AS g \
               ON g.cid = 3 WHERE o.cid IN (SELECT 1 FROM DUAL WHERE 2 > 1) AND g.cid > 1.5";
    let (calls, p) = graph_plan(sql, 6, Vec::new());
    let Plan::Graph(g) = p else { panic!("{p:?}") };
    assert!(g.filters[0].is_empty(), "{:?}", g.filters);
    assert_eq!(g.cypher(0), calls[0].cypher);
}

/// R8 round 7 (G63): a prepared statement's values go in as SQL literals
/// outside the calls and as Cypher literals inside them.
#[test]
fn parameters_bind_as_sql_or_cypher_literals() {
    let sql = "SELECT x.a, '?' FROM GRAPH_TABLE (g MATCH (p IS P) WHERE p.id < ? AND p.n = ? \
               COLUMNS (p.id AS a)) AS x JOIN t ON t.k = x.a WHERE t.v > ? -- ?\n AND t.w = ? /* ? */";
    assert_eq!(param_spans(sql).len(), 4);
    let sql_l = [
        "1".to_owned(),
        "'s'".into(),
        "3".into(),
        "_utf8mb4'x'".into(),
    ];
    let cy_l = ["1".to_owned(), "'s\\\\'".into(), "3".into(), "'x'".into()];
    let b = bind_params(sql, &sql_l, &cy_l).unwrap();
    assert!(
        b.contains("WHERE p.id < 1 AND p.n = 's\\\\' COLUMNS"),
        "{b}"
    );
    assert!(
        b.contains("t.v > 3 -- ?\n AND t.w = _utf8mb4'x' /* ? */"),
        "{b}"
    );
    assert!(b.contains("SELECT x.a, '?' FROM"), "{b}");
    assert!(bind_params(sql, &sql_l[..3], &cy_l[..3]).is_none());
    let found = find(&b).unwrap();
    assert_eq!(
        found[0].cypher,
        "MATCH (p:P) WHERE p.id < 1 AND p.n = 's\\\\' RETURN p.id AS `a`"
    );
}

/// R8 round 7 (G62): rows past the inline limit become a temporary
/// table typed from one representative row per kind of value.
#[test]
fn temp_tables_take_rows_typed_like_their_derived_table() {
    let cols = vec!["a".to_owned(), "b".to_owned()];
    let rows = vec![
        vec![GraphLit::Int(5), GraphLit::Str("xy".into())],
        vec![GraphLit::Int(-12345), GraphLit::Str("longest".into())],
        vec![GraphLit::Null, GraphLit::Other("2.5e0".into())],
        vec![GraphLit::Int(7), GraphLit::Str("it's".into())],
    ];
    let (create, inserts) = temp_table_sql("`s`.`t`", &cols, &rows, 40);
    assert_eq!(
        create,
        "CREATE TEMPORARY TABLE `s`.`t` AS SELECT * FROM (SELECT -12345 AS `a`, 'longest' AS `b` \
         UNION ALL VALUES (NULL, 2.5e0)) AS r LIMIT 0"
    );
    assert_eq!(
        inserts,
        [
            "INSERT INTO `s`.`t` VALUES (5, 'xy')",
            "INSERT INTO `s`.`t` VALUES (-12345, 'longest')",
            "INSERT INTO `s`.`t` VALUES (NULL, 2.5e0)",
            "INSERT INTO `s`.`t` VALUES (7, 'it''s')",
        ]
    );
    let (_, one) = temp_table_sql("t", &cols, &rows, 1 << 20);
    assert_eq!(one.len(), 1);
}
