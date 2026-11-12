//! Executor tests over the in-memory store, on 1 shard and on 4: both must
//! give the same answers (chapter 43, "Testing").

use std::collections::BTreeMap;

use sprig_common::SprigCode;

use crate::eval::Params;
use crate::exec::{Budgets, Output};
use crate::mem::MemStore;
use crate::store::GraphStore;
use crate::value::Value;

async fn q(s: &mut MemStore, text: &str) -> Output {
    crate::execute(s, text, &Params::new(), &Budgets::default())
        .await
        .unwrap_or_else(|e| panic!("{text}: {e}"))
}

async fn qp(s: &mut MemStore, text: &str, params: &[(&str, Value)]) -> Output {
    let p: Params = params
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect();
    crate::execute(s, text, &p, &Budgets::default())
        .await
        .unwrap_or_else(|e| panic!("{text}: {e}"))
}

/// Rows as display strings, for compact assertions.
fn show(o: &Output) -> Vec<String> {
    o.rows
        .iter()
        .map(|r| r.iter().map(Value::display).collect::<Vec<_>>().join("|"))
        .collect()
}

/// A small social graph: a chain and a shop.
async fn social(shards: usize) -> MemStore {
    let mut s = MemStore::new(shards);
    q(
        &mut s,
        "CREATE (a:Person {name: 'Alice', email: 'alice@example.com', age: 30}),
                (b:Person {name: 'Bob', age: 25}),
                (c:Person {name: 'Carol', age: 35}),
                (d:Person {name: 'Dave', age: 40}),
                (e:Person {name: 'Eve'}),
                (p1:Product {name: 'Lamp', price: 10.5}),
                (p2:Product {name: 'Desk', price: 100}),
                (p3:Product {name: 'Pen', price: 1}),
                (a)-[:KNOWS {since: 2010}]->(b),
                (b)-[:KNOWS]->(c),
                (c)-[:KNOWS]->(d),
                (d)-[:KNOWS]->(a),
                (a)-[:BOUGHT]->(p1),
                (b)-[:BOUGHT]->(p2),
                (c)-[:BOUGHT]->(p2),
                (c)-[:BOUGHT]->(p3),
                (d)-[:BOUGHT]->(p3)",
    )
    .await;
    s
}

#[tokio::test]
async fn create_and_match_basics() {
    for shards in [1, 4] {
        let mut s = social(shards).await;
        assert_eq!(s.node_count(), 8);
        assert_eq!(s.edge_count(), 9);
        let o = q(
            &mut s,
            "MATCH (p:Person) WHERE p.age > 28 RETURN p.name AS n ORDER BY n",
        )
        .await;
        assert_eq!(show(&o), ["Alice", "Carol", "Dave"]);
        let o = q(
            &mut s,
            "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b) RETURN b.name, r.since",
        )
        .await;
        assert_eq!(o.columns, ["b.name", "r.since"]);
        assert_eq!(show(&o), ["Bob|2010"]);
        let o = q(
            &mut s,
            "MATCH (a)<-[:KNOWS]-(b {name: 'Bob'}) RETURN a.name",
        )
        .await;
        assert_eq!(show(&o), ["Carol"]);
        let o = q(
            &mut s,
            "MATCH (a {name: 'Alice'})-[:KNOWS]-(x) RETURN x.name ORDER BY x.name",
        )
        .await;
        assert_eq!(show(&o), ["Bob", "Dave"]);
        let o = q(&mut s, "MATCH (n) RETURN count(*) AS c, count(n.age) AS a").await;
        assert_eq!(show(&o), ["8|4"]);
        let o = q(&mut s, "MATCH (n:Nope) RETURN count(*) AS c").await;
        assert_eq!(show(&o), ["0"]);
        // Labels(), type(), id and element ids.
        let o = q(&mut s, "MATCH (a {name: 'Alice'})-[r]->(p:Product) RETURN labels(p), type(r), elementId(a) STARTS WITH 'n:'").await;
        assert_eq!(show(&o), ["[\"Product\"]|BOUGHT|true"]);
        let o = q(
            &mut s,
            "MATCH (a {name: 'Alice'}) WITH id(a) AS i MATCH (n) WHERE id(n) = i RETURN n.name",
        )
        .await;
        assert_eq!(show(&o), ["Alice"]);
    }
}

#[tokio::test]
async fn the_worked_example_on_one_and_four_shards() {
    let text = "MATCH (a:Person {email: $email})-[:KNOWS*1..2]->(f:Person)-[:BOUGHT]->(p:Product)
                WHERE NOT (a)-[:BOUGHT]->(p) AND f <> a
                RETURN p.name AS product, count(DISTINCT f) AS buyers
                ORDER BY buyers DESC, product LIMIT 5";
    let mut results = Vec::new();
    for shards in [1, 4] {
        let mut s = social(shards).await;
        let o = qp(&mut s, text, &[("email", Value::str("alice@example.com"))]).await;
        results.push(show(&o));
        // Endpoint mode: the variable-length hop ran as CTE bursts.
        assert!(s.stats().cte_queries >= 1, "{shards} shards");
    }
    // Alice knows Bob (1 hop) and Carol (2 hops): Bob bought Desk, Carol
    // bought Desk and Pen.
    assert_eq!(results[0], ["Desk|2", "Pen|1"]);
    assert_eq!(results[0], results[1]);
}

#[tokio::test]
async fn variable_length_paths_endpoint_and_path_modes_agree() {
    for shards in [1, 4] {
        let mut s = social(shards).await;
        // Endpoint mode (DISTINCT).
        let o = q(
            &mut s,
            "MATCH (a {name: 'Alice'})-[:KNOWS*1..3]->(x) RETURN DISTINCT x.name AS n ORDER BY n",
        )
        .await;
        assert_eq!(show(&o), ["Bob", "Carol", "Dave"]);
        // Path mode (paths are counted).
        let o = q(&mut s, "MATCH p = (a {name: 'Alice'})-[:KNOWS*1..4]->(x) RETURN x.name, length(p) ORDER BY length(p)").await;
        assert_eq!(show(&o), ["Bob|1", "Carol|2", "Dave|3", "Alice|4"]);
        // Zero-length.
        let o = q(
            &mut s,
            "MATCH (a {name: 'Alice'})-[:KNOWS*0..1]->(x) RETURN x.name ORDER BY x.name",
        )
        .await;
        assert_eq!(show(&o), ["Alice", "Bob"]);
        // Unbounded in endpoint mode: the cycle ends it.
        let o = q(
            &mut s,
            "MATCH (a {name: 'Alice'})-[:KNOWS*]->(x) RETURN count(DISTINCT x) AS c",
        )
        .await;
        assert_eq!(show(&o), ["4"]);
        // Relationship list variable.
        let o = q(
            &mut s,
            "MATCH (a {name: 'Alice'})-[r:KNOWS*2]->(x) RETURN size(r), x.name",
        )
        .await;
        assert_eq!(show(&o), ["2|Carol"]);
        // Undirected path mode: trails in both directions.
        let o = q(
            &mut s,
            "MATCH (a {name: 'Alice'})-[:KNOWS*2]-(x) RETURN x.name ORDER BY x.name",
        )
        .await;
        assert_eq!(show(&o), ["Carol", "Carol"]);
        // Quantifier syntax.
        let o = q(
            &mut s,
            "MATCH (a {name: 'Alice'})-[:KNOWS]->{2,2}(x) RETURN x.name",
        )
        .await;
        assert_eq!(show(&o), ["Carol"]);
    }
}

#[tokio::test]
async fn shortest_paths() {
    for shards in [1, 4] {
        let mut s = social(shards).await;
        let o = q(&mut s, "MATCH (a {name: 'Alice'}), (d {name: 'Dave'})
                           MATCH p = shortestPath((a)-[:KNOWS*]-(d)) RETURN length(p), [n IN nodes(p) | n.name]").await;
        assert_eq!(show(&o), ["1|[\"Alice\", \"Dave\"]"]);
        let o = q(
            &mut s,
            "MATCH (a {name: 'Alice'}), (d {name: 'Dave'})
                           MATCH p = shortestPath((a)-[:KNOWS*]->(d)) RETURN length(p)",
        )
        .await;
        assert_eq!(show(&o), ["3"]);
        // Two shortest paths from Bob to Product Pen: via Carol only (Dave is
        // farther), and all shortest from Alice to Carol undirected: one
        // each way? Alice-Bob-Carol and Alice-Dave-Carol.
        let o = q(&mut s, "MATCH (a {name: 'Alice'}), (c {name: 'Carol'})
                           MATCH p = allShortestPaths((a)-[:KNOWS*]-(c)) RETURN [n IN nodes(p) | n.name] AS ns ORDER BY ns").await;
        assert_eq!(
            show(&o),
            [
                "[\"Alice\", \"Bob\", \"Carol\"]",
                "[\"Alice\", \"Dave\", \"Carol\"]"
            ]
        );
        let o = q(
            &mut s,
            "MATCH (a {name: 'Alice'}), (e {name: 'Eve'})
                           OPTIONAL MATCH p = shortestPath((a)-[*..5]-(e)) RETURN p",
        )
        .await;
        assert_eq!(show(&o), ["null"]);
    }
}

#[tokio::test]
async fn optional_match_with_unwind_and_aggregation() {
    let mut s = social(1).await;
    let o = q(&mut s, "MATCH (p:Person) OPTIONAL MATCH (p)-[:BOUGHT]->(x) RETURN p.name, count(x) AS c ORDER BY p.name").await;
    assert_eq!(show(&o), ["Alice|1", "Bob|1", "Carol|2", "Dave|1", "Eve|0"]);
    let o = q(&mut s, "MATCH (p:Product) RETURN sum(p.price) AS s, avg(p.price) AS a, min(p.name) AS mn, max(p.price) AS mx, collect(p.name) AS names").await;
    assert_eq!(o.rows[0][0], Value::Float(111.5));
    assert_eq!(o.rows[0][2], Value::str("Desk"));
    assert_eq!(o.rows[0][3], Value::Int(100));
    let o = q(
        &mut s,
        "UNWIND [3, 1, 2, 1] AS x WITH DISTINCT x ORDER BY x DESC RETURN collect(x) AS xs",
    )
    .await;
    assert_eq!(show(&o), ["[3, 2, 1]"]);
    let o = q(
        &mut s,
        "UNWIND range(1, 10) AS x WITH x WHERE x % 3 = 0 RETURN x SKIP 1 LIMIT 1",
    )
    .await;
    assert_eq!(show(&o), ["6"]);
    let o = q(&mut s, "MATCH (p:Person) WITH p.age IS NULL AS missing, count(*) AS c RETURN missing, c ORDER BY missing").await;
    assert_eq!(show(&o), ["false|4", "true|1"]);
    let o = q(
        &mut s,
        "RETURN 1 AS x UNION RETURN 1 AS x UNION ALL RETURN 2 AS x",
    )
    .await;
    assert_eq!(show(&o).len(), 2);
    let o = q(
        &mut s,
        "MATCH (n:Person) WHERE n.name =~ 'A.*' OR n.name IN ['Eve'] RETURN n.name ORDER BY n.name",
    )
    .await;
    assert_eq!(show(&o), ["Alice", "Eve"]);
    let o = q(
        &mut s,
        "CALL db.labels() YIELD label RETURN label ORDER BY label",
    )
    .await;
    assert_eq!(show(&o), ["Person", "Product"]);
    let o = q(&mut s, "CALL db.relationshipTypes").await;
    assert_eq!(o.columns, ["relationshipType"]);
    let o = q(&mut s, "MATCH (a:Person) WHERE exists((a)-[:BOUGHT]->(:Product {name: 'Pen'})) RETURN a.name ORDER BY a.name").await;
    assert_eq!(show(&o), ["Carol", "Dave"]);
    let o = q(
        &mut s,
        "MATCH (a:Person) WHERE NOT EXISTS { (a)-[:BOUGHT]->() } RETURN a.name",
    )
    .await;
    assert_eq!(show(&o), ["Eve"]);
}

#[tokio::test]
async fn relationship_uniqueness_within_a_match() {
    let mut s = MemStore::new(1);
    q(&mut s, "CREATE (a:N {i: 1})-[:R]->(b:N {i: 2})").await;
    // (x)-[r1]-(y)-[r2]-(z) can't reuse the one relationship.
    let o = q(
        &mut s,
        "MATCH (x)-[r1:R]-(y)-[r2:R]-(z) RETURN count(*) AS c",
    )
    .await;
    assert_eq!(show(&o), ["0"]);
    // Two MATCH clauses may.
    let o = q(
        &mut s,
        "MATCH (x)-[r1:R]-(y) MATCH (y)-[r2:R]-(z) RETURN count(*) AS c",
    )
    .await;
    assert_eq!(show(&o), ["2"]);
}

#[tokio::test]
async fn merge_set_remove_delete() {
    let mut s = MemStore::new(4);
    let o = q(&mut s, "MERGE (n:User {id: 1}) ON CREATE SET n.created = true ON MATCH SET n.seen = coalesce(n.seen, 0) + 1 RETURN n.created, n.seen").await;
    assert_eq!(show(&o), ["true|null"]);
    assert_eq!(o.stats["nodes-created"], 1);
    let o = q(&mut s, "MERGE (n:User {id: 1}) ON CREATE SET n.created = true ON MATCH SET n.seen = coalesce(n.seen, 0) + 1 RETURN n.seen").await;
    assert_eq!(show(&o), ["1"]);
    assert!(!o.stats.contains_key("nodes-created"));
    // MERGE of a relationship between existing nodes, twice.
    q(&mut s, "CREATE (:User {id: 2})").await;
    for _ in 0..2 {
        q(
            &mut s,
            "MATCH (a:User {id: 1}), (b:User {id: 2}) MERGE (a)-[:FOLLOWS]->(b)",
        )
        .await;
    }
    let o = q(
        &mut s,
        "MATCH (:User)-[r:FOLLOWS]->(:User) RETURN count(r) AS c",
    )
    .await;
    assert_eq!(show(&o), ["1"]);
    // UNWIND + MERGE: later rows see earlier creations.
    q(&mut s, "UNWIND [3, 3, 4] AS i MERGE (:User {id: i})").await;
    let o = q(&mut s, "MATCH (u:User) RETURN count(u) AS c").await;
    assert_eq!(show(&o), ["4"]);
    // SET items see earlier items; labels; += and =.
    let o = q(&mut s, "MATCH (n:User {id: 2}) SET n.a = 1, n.b = n.a + 1, n:Admin, n += {c: 3} RETURN n.a, n.b, n.c, labels(n)").await;
    assert_eq!(show(&o), ["1|2|3|[\"User\", \"Admin\"]"]);
    let o = q(
        &mut s,
        "MATCH (n:Admin) SET n = {z: 9} REMOVE n:Admin RETURN keys(n), labels(n)",
    )
    .await;
    assert_eq!(show(&o), ["[\"z\"]|[\"User\"]"]);
    let o = q(
        &mut s,
        "MATCH (n:User {id: 1}) REMOVE n.created RETURN n.created",
    )
    .await;
    assert_eq!(show(&o), ["null"]);
    // Maps aren't property values.
    let e = crate::execute(
        &mut s,
        "CREATE (:X {m: {a: 1}})",
        &Params::new(),
        &Budgets::default(),
    )
    .await
    .expect_err("map");
    assert_eq!(e.code(), Some(SprigCode::GraphPropertyType));
    // A plain DELETE of a connected node is recorded for the commit check.
    let o = q(&mut s, "MATCH (n:User {id: 1}) DELETE n").await;
    assert_eq!(o.deleted_nodes.len(), 1);
    assert_eq!(o.qtype, "w");
    // DETACH DELETE removes the relationships.
    let mut s = social(4).await;
    let o = q(&mut s, "MATCH (n:Person {name: 'Carol'}) DETACH DELETE n").await;
    assert_eq!(o.stats["nodes-deleted"], 1);
    assert_eq!(o.stats["relationships-deleted"], 4);
    assert!(o.deleted_nodes.is_empty());
    // Over graph.max_detach_edges.
    let b = Budgets {
        max_detach_edges: 2,
        ..Budgets::default()
    };
    let e = crate::execute(
        &mut s,
        "MATCH (n:Person {name: 'Alice'}) DETACH DELETE n",
        &Params::new(),
        &b,
    )
    .await
    .expect_err("detach");
    assert_eq!(e.code(), Some(SprigCode::GraphDetachTooLarge));
}

/// A long chain: deep and wide traversals are refused with 39903, never
/// truncated.
#[tokio::test]
async fn budgets_refuse_cleanly() {
    let mut s = MemStore::new(4);
    q(&mut s, "UNWIND range(0, 39) AS i CREATE (:C {i: i})").await;
    q(
        &mut s,
        "MATCH (a:C), (b:C) WHERE b.i = a.i + 1 CREATE (a)-[:NEXT]->(b)",
    )
    .await;
    // A star: one hub with 50 leaves.
    q(
        &mut s,
        "CREATE (h:Hub) WITH h UNWIND range(1, 50) AS i CREATE (h)-[:LEAF]->(:Leaf {i: i})",
    )
    .await;
    let p = Params::new();
    let check = |r: sprig_common::Result<Output>, reason: &str| {
        let e = r.expect_err(reason);
        assert_eq!(e.code(), Some(SprigCode::GraphBudgetExceeded), "{e}");
        assert!(e.message.contains(reason), "{e}");
    };
    // Unbounded past graph.max_hops (15): an error, in both modes.
    check(
        crate::execute(
            &mut s,
            "MATCH (a:C {i: 0})-[:NEXT*]->(b) RETURN count(DISTINCT b)",
            &p,
            &Budgets::default(),
        )
        .await,
        "hops",
    );
    check(
        crate::execute(
            &mut s,
            "MATCH (a:C {i: 0})-[:NEXT*]->(b) RETURN b",
            &p,
            &Budgets::default(),
        )
        .await,
        "hops",
    );
    // An explicit bound above it.
    check(
        crate::execute(
            &mut s,
            "MATCH (a:C {i: 0})-[:NEXT*1..20]->(b) RETURN b",
            &p,
            &Budgets::default(),
        )
        .await,
        "hops",
    );
    // With max_hops 50 the chain is exact.
    let wide = Budgets {
        max_hops: 50,
        ..Budgets::default()
    };
    let o = crate::execute(
        &mut s,
        "MATCH (a:C {i: 0})-[:NEXT*]->(b) RETURN count(DISTINCT b) AS c",
        &p,
        &wide,
    )
    .await
    .expect("exact");
    assert_eq!(o.rows[0][0], Value::Int(39));
    // Frontier and rows.
    let narrow = Budgets {
        max_frontier: 10,
        ..Budgets::default()
    };
    check(
        crate::execute(
            &mut s,
            "MATCH (h:Hub)-[:LEAF*1..2]->(x) RETURN x",
            &p,
            &narrow,
        )
        .await,
        "frontier",
    );
    let few = Budgets {
        max_rows: 20,
        ..Budgets::default()
    };
    check(
        crate::execute(&mut s, "MATCH (h:Hub)-[:LEAF]->(x) RETURN x", &p, &few).await,
        "rows",
    );
    let quick = Budgets {
        max_time: std::time::Duration::ZERO,
        ..Budgets::default()
    };
    check(
        crate::execute(&mut s, "MATCH (h:Hub)-[:LEAF]->(x) RETURN x", &p, &quick).await,
        "time",
    );
    let small = Budgets {
        max_memory: 1024,
        ..Budgets::default()
    };
    check(
        crate::execute(&mut s, "MATCH (h:Hub)-[:LEAF]->(x) RETURN x", &p, &small).await,
        "memory",
    );
}

#[tokio::test]
async fn errors_and_explain() {
    let mut s = social(1).await;
    let p = Params::new();
    let b = Budgets::default();
    for (text, code) in [
        ("MATCH (n) RETURN m", SprigCode::GraphSyntax),
        ("RETURN $missing", SprigCode::GraphSyntax),
        ("CREATE (a)-[:R|S]->(b)", SprigCode::GraphSyntax),
        ("CREATE (a)-[:R]-(b)", SprigCode::GraphSyntax),
        ("RETURN 1 + 'a' - 1", SprigCode::GraphPropertyType),
        ("CALL db.nothing()", SprigCode::GraphUnsupported),
        ("RETURN 1 AS a UNION RETURN 1 AS b", SprigCode::GraphSyntax),
    ] {
        let e = crate::execute(&mut s, text, &p, &b).await.expect_err(text);
        assert_eq!(e.code(), Some(code), "{text}: {e}");
    }
    let o = q(&mut s, "EXPLAIN MATCH (a)-[:KNOWS*1..2]->(b) RETURN b").await;
    assert!(o.rows.is_empty());
    assert!(o.plan.expect("plan").iter().any(|l| l.contains("cte")));
    let o = q(
        &mut s,
        "PROFILE MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b) RETURN b.name",
    )
    .await;
    assert_eq!(o.rows.len(), 1);
    assert!(
        o.plan
            .expect("plan")
            .iter()
            .any(|l| l.contains("shard statements"))
    );
}

#[tokio::test]
async fn properties_keep_their_types() {
    let mut s = MemStore::new(1);
    let o = q(&mut s, "CREATE (n:T {i: 1, f: 1.0, s: 'x', b: true, l: [1, 2], fl: [1.5, 2.0]}) RETURN n.i, n.f, n.l, n.fl").await;
    assert_eq!(o.rows[0][0], Value::Int(1));
    assert_eq!(o.rows[0][1], Value::Float(1.0));
    let o = q(
        &mut s,
        "MATCH (n:T) RETURN n.f, n.fl, n.i = 1, n.f = 1, n.s = 1",
    )
    .await;
    assert_eq!(o.rows[0][0], Value::Float(1.0));
    assert_eq!(
        o.rows[0][1],
        Value::List(vec![Value::Float(1.5), Value::Float(2.0)])
    );
    assert_eq!(
        show(&o)[0].split('|').skip(2).collect::<Vec<_>>(),
        ["true", "true", "false"]
    );
    // A pushed-down equality is a superset, rechecked: '1' doesn't match 1.
    let o = q(&mut s, "MATCH (n:T) WHERE n.i = '1' RETURN n").await;
    assert!(o.rows.is_empty());
    let _ = BTreeMap::<String, Value>::new();
}

#[tokio::test]
async fn map_projections_subqueries_and_foreach() {
    for shards in [1, 4] {
        let mut s = social(shards).await;
        let o = q(
            &mut s,
            "MATCH (a:Person {name: 'Alice'}) RETURN a {.name, .age, k: 1, .nope} AS m",
        )
        .await;
        assert_eq!(show(&o), ["{age: 30, k: 1, name: \"Alice\", nope: null}"]);
        let o = q(&mut s, "MATCH (a:Person {name: 'Bob'}) RETURN a {.*} AS m").await;
        assert_eq!(show(&o), ["{age: 25, name: \"Bob\"}"]);
        // CALL { } with an importing WITH, a scope clause, a unit subquery.
        let o = q(
            &mut s,
            "MATCH (p:Person) CALL { WITH p MATCH (p)-[:BOUGHT]->(x) RETURN count(x) AS n } RETURN p.name, n ORDER BY p.name",
        )
        .await;
        assert_eq!(show(&o), ["Alice|1", "Bob|1", "Carol|2", "Dave|1", "Eve|0"]);
        let o = q(
            &mut s,
            "MATCH (p:Person {name: 'Carol'}) CALL (p) { MATCH (p)-[:BOUGHT]->(x) RETURN x.name AS x ORDER BY x } RETURN x",
        )
        .await;
        assert_eq!(show(&o), ["Desk", "Pen"]);
        let o = q(
            &mut s,
            "CALL { RETURN 1 AS x UNION RETURN 2 AS x } RETURN sum(x) AS s",
        )
        .await;
        assert_eq!(show(&o), ["3"]);
        // A non-importing subquery doesn't see the outer variable.
        let o = q(
            &mut s,
            "MATCH (p:Person {name: 'Eve'}) CALL { MATCH (p:Product) RETURN count(p) AS c } RETURN c",
        )
        .await;
        assert_eq!(show(&o), ["3"]);
        // Expression subqueries.
        let o = q(
            &mut s,
            "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:BOUGHT]->(x) WHERE x.price > 50 } RETURN p.name ORDER BY p.name",
        )
        .await;
        assert_eq!(show(&o), ["Bob", "Carol"]);
        let o = q(
            &mut s,
            "MATCH (p:Person) RETURN p.name, COUNT { (p)-[:BOUGHT]->() } AS n ORDER BY p.name",
        )
        .await;
        assert_eq!(show(&o), ["Alice|1", "Bob|1", "Carol|2", "Dave|1", "Eve|0"]);
        let o = q(
            &mut s,
            "MATCH (p:Person {name: 'Carol'}) RETURN COLLECT { MATCH (p)-[:BOUGHT]->(x) RETURN x.name ORDER BY x.name } AS xs",
        )
        .await;
        assert_eq!(show(&o), ["[\"Desk\", \"Pen\"]"]);
        // FOREACH.
        let o = q(
            &mut s,
            "MATCH (a:Person {name: 'Alice'}) FOREACH (i IN range(1, 3) | CREATE (a)-[:HAS]->(:Item {i: i}))",
        )
        .await;
        assert_eq!(o.stats.get("nodes-created"), Some(&3));
        let o = q(
            &mut s,
            "MATCH (:Person {name: 'Alice'})-[:HAS]->(i) RETURN sum(i.i) AS s",
        )
        .await;
        assert_eq!(show(&o), ["6"]);
        q(
            &mut s,
            "MATCH (i:Item) FOREACH (x IN CASE WHEN i.i > 1 THEN [1] ELSE [] END | SET i.big = true)",
        )
        .await;
        let o = q(&mut s, "MATCH (i:Item) WHERE i.big RETURN count(*) AS c").await;
        assert_eq!(show(&o), ["2"]);
    }
}

#[tokio::test]
async fn indexes_and_uniqueness_constraints() {
    for shards in [1, 4] {
        let mut s = social(shards).await;
        let o = q(
            &mut s,
            "CREATE CONSTRAINT person_email FOR (p:Person) REQUIRE p.email IS UNIQUE",
        )
        .await;
        assert_eq!(o.qtype, "s");
        assert_eq!(o.stats.get("constraints-added"), Some(&1));
        q(&mut s, "CREATE INDEX FOR (p:Product) ON (p.name)").await;
        q(
            &mut s,
            "CREATE INDEX IF NOT EXISTS FOR (p:Product) ON (p.name)",
        )
        .await;
        let o = q(
            &mut s,
            "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties, owningConstraint RETURN * ORDER BY name",
        )
        .await;
        assert_eq!(o.rows.len(), 4, "{:?}", show(&o));
        let o = q(
            &mut s,
            "SHOW CONSTRAINTS YIELD name, type RETURN name, type",
        )
        .await;
        assert_eq!(show(&o), ["person_email|UNIQUENESS"]);
        // Violations.
        let e = crate::execute(
            &mut s,
            "CREATE (:Person {email: 'alice@example.com'})",
            &Params::new(),
            &Budgets::default(),
        )
        .await
        .expect_err("duplicate");
        assert!(
            matches!(e.kind, sprig_common::ErrorKind::Backend { errno: 1062, .. }),
            "{e}"
        );
        let e = crate::execute(
            &mut s,
            "MATCH (b:Person {name: 'Bob'}) SET b.email = 'alice@example.com'",
            &Params::new(),
            &Budgets::default(),
        )
        .await
        .expect_err("duplicate set");
        assert!(e.to_string().contains("already exists"), "{e}");
        // MERGE finds rather than duplicates.
        let o = q(
            &mut s,
            "MERGE (p:Person {email: 'alice@example.com'}) ON MATCH SET p.seen = true RETURN p.name",
        )
        .await;
        assert_eq!(show(&o), ["Alice"]);
        // Name clashes and drops.
        let e = crate::execute(
            &mut s,
            "CREATE INDEX person_email FOR (p:Person) ON (p.name)",
            &Params::new(),
            &Budgets::default(),
        )
        .await
        .expect_err("name");
        assert_eq!(e.code(), Some(SprigCode::GraphDefinitionInvalid));
        let e = crate::execute(
            &mut s,
            "DROP INDEX person_email",
            &Params::new(),
            &Budgets::default(),
        )
        .await
        .expect_err("owned");
        assert!(e.message.contains("IndexDropFailed"), "{e}");
        let o = q(&mut s, "DROP CONSTRAINT person_email").await;
        assert_eq!(o.stats.get("constraints-removed"), Some(&1));
        q(&mut s, "DROP CONSTRAINT person_email IF EXISTS").await;
        q(&mut s, "CREATE (:Person {email: 'alice@example.com'})").await;
        let e = crate::execute(
            &mut s,
            "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.email IS UNIQUE",
            &Params::new(),
            &Budgets::default(),
        )
        .await
        .expect_err("existing duplicates");
        assert!(e.message.contains("ConstraintCreationFailed"), "{e}");
        // Relationship uniqueness.
        q(
            &mut s,
            "CREATE CONSTRAINT FOR ()-[r:KNOWS]-() REQUIRE r.since IS UNIQUE",
        )
        .await;
        let e = crate::execute(
            &mut s,
            "MATCH (a {name: 'Bob'}), (b {name: 'Eve'}) CREATE (a)-[:KNOWS {since: 2010}]->(b)",
            &Params::new(),
            &Budgets::default(),
        )
        .await
        .expect_err("rel duplicate");
        assert!(e.to_string().contains("Relationship"), "{e}");
    }
}

/// G19 (round 3): `1` and `1.0` are one value for uniqueness and index
/// lookups, as Neo4j's equality says; `'1'` is another.
#[tokio::test]
async fn numeric_keys_are_equal_across_integer_and_float() {
    for shards in [1, 4] {
        let mut s = MemStore::new(shards);
        q(&mut s, "CREATE CONSTRAINT FOR (n:N) REQUIRE n.k IS UNIQUE").await;
        q(&mut s, "CREATE (:N {k: 1, name: 'int'})").await;
        let e = crate::execute(
            &mut s,
            "CREATE (:N {k: 1.0})",
            &Params::new(),
            &Budgets::default(),
        )
        .await
        .expect_err("1.0 duplicates 1");
        assert!(e.to_string().contains("already exists"), "{e}");
        // A string '1' is another value.
        q(&mut s, "CREATE (:N {k: '1', name: 'str'})").await;
        // Lookups through the index find the integer with a float.
        let o = q(&mut s, "MATCH (n:N {k: 1.0}) RETURN n.name").await;
        assert_eq!(show(&o), ["int"]);
        let o = q(&mut s, "MATCH (n:N) WHERE n.k IN [1.0, 2] RETURN n.name").await;
        assert_eq!(show(&o), ["int"]);
        // MERGE on 1.0 matches the node stored with 1.
        let o = q(
            &mut s,
            "MERGE (n:N {k: 1.0}) ON MATCH SET n.seen = true RETURN n.name, n.seen",
        )
        .await;
        assert_eq!(show(&o), ["int|true"]);
        let o = q(&mut s, "MATCH (n:N) RETURN count(n)").await;
        assert_eq!(show(&o), ["2"]);
        // Floats that aren't integers stay apart from their neighbours.
        q(&mut s, "CREATE (:N {k: 1.5})").await;
        q(&mut s, "CREATE (:N {k: 2})").await;
    }
}

#[tokio::test]
async fn temporal_and_spatial_values() {
    let mut s = MemStore::new(2);
    let o = q(
        &mut s,
        "RETURN toString(date('2020-01-31') + duration('P1M')) AS a, \
         date('2020-02-29').dayOfWeek AS b, \
         toString(duration.between(date('2020-01-01'), date('2021-03-04'))) AS c, \
         datetime('2020-01-05T10:00:00+01:00').epochSeconds AS d, \
         toString(datetime({year: 2020, month: 1, day: 1, timezone: '+05:30'})) AS e, \
         localtime('12:00') < localtime('13:00') AS f, \
         toString(point({x: 1, y: 2})) AS g, \
         point.distance(point({x: 0, y: 0}), point({x: 3, y: 4})) AS h, \
         point({latitude: 1, longitude: 2}).srid AS i, \
         duration('PT90M').minutes AS j",
    )
    .await;
    assert_eq!(
        show(&o),
        [
            "2020-02-29|6|P1Y2M3D|1578214800|2020-01-01T00:00:00+05:30|true|point({x: 1.0, y: 2.0, crs: 'cartesian'})|5.0|4326|90"
        ]
    );
    // Stored and read back as themselves; compared and ordered.
    q(
        &mut s,
        "CREATE (:E {at: date('2021-05-01'), t: localtime('09:30:00.25'), d: duration('P2D'), p: point({x: 1.5, y: 2}), ds: [date('2020-01-01'), date('2020-01-02')]}), \
                (:E {at: date('2020-05-01'), t: localtime('10:00'), d: duration('P1D'), p: point({x: 0, y: 0})})",
    )
    .await;
    let o = q(
        &mut s,
        "MATCH (e:E) RETURN toString(e.at) AS at, toString(e.t) AS t, toString(e.d) AS d, e.p.x AS x, size(e.ds) AS n ORDER BY e.at",
    )
    .await;
    assert_eq!(
        show(&o),
        [
            "2020-05-01|10:00:00|P1D|0.0|null",
            "2021-05-01|09:30:00.25|P2D|1.5|2"
        ]
    );
    let o = q(
        &mut s,
        "MATCH (e:E) WHERE e.at > date('2021-01-01') RETURN e.at.year AS y",
    )
    .await;
    assert_eq!(show(&o), ["2021"]);
    let o = q(
        &mut s,
        "MATCH (e:E) RETURN max(e.at) AS m, count(DISTINCT e.p) AS n",
    )
    .await;
    assert_eq!(show(&o), ["2021-05-01|2"]);
}

/// Round 3: GQL's forms (labels with `IS`, `FILTER`, `LET`, `FOR`,
/// `INSERT`, `NEXT`, `OFFSET`, `TRAIL`) answer as their Cypher.
#[tokio::test]
async fn gql_forms_answer_as_their_cypher() {
    for shards in [1, 4] {
        let mut s = social(shards).await;
        let pairs = [
            (
                "MATCH (a IS Person)-[IS KNOWS]->(b IS Person) RETURN a.name, b.name ORDER BY a.name, b.name",
                "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name ORDER BY a.name, b.name",
            ),
            (
                "MATCH (p IS Person) FILTER p.age > 28 LET older = p.age + 1 RETURN p.name, older ORDER BY p.name",
                "MATCH (p:Person) WITH * WHERE p.age > 28 WITH *, p.age + 1 AS older RETURN p.name, older ORDER BY p.name",
            ),
            (
                "FOR x IN [3, 1, 2] RETURN x ORDER BY x OFFSET 1",
                "UNWIND [3, 1, 2] AS x RETURN x ORDER BY x SKIP 1",
            ),
            (
                "MATCH (p IS Person) RETURN p.name AS n NEXT RETURN count(n) AS c",
                "MATCH (p:Person) WITH p.name AS n RETURN count(n) AS c",
            ),
            (
                "MATCH TRAIL path = (a IS Person {name: 'Alice'})-[IS KNOWS]->{1,3}(b) RETURN length(path) AS l ORDER BY l",
                "MATCH path = (a:Person {name: 'Alice'})-[:KNOWS]->{1,3}(b) RETURN length(path) AS l ORDER BY l",
            ),
        ];
        for (g, c) in pairs {
            assert_eq!(show(&q(&mut s, g).await), show(&q(&mut s, c).await), "{g}");
        }
        let o = q(&mut s, "INSERT (:Tag {name: 'gql'}) RETURN 1").await;
        assert_eq!(o.stats.get("nodes-created"), Some(&1));
    }
}

/// Round 4 (G44): one search from a start node to many ends gives the same
/// lengths as one bidirectional search per pair, on 1 and 4 shards.
#[tokio::test]
async fn shortest_paths_to_many_ends_match_pairwise_searches() {
    for shards in [1, 4] {
        let mut s = MemStore::new(shards);
        q(&mut s, "UNWIND range(0, 79) AS i CREATE (:P {i: i})").await;
        q(
            &mut s,
            "MATCH (a:P), (b:P) WHERE b.i = (a.i * 7 + 3) % 80 OR b.i = (a.i + 11) % 80 CREATE (a)-[:K]->(b)",
        )
        .await;
        let batch = q(
            &mut s,
            "MATCH (a:P {i: 5}), (b:P) WHERE b <> a MATCH p = shortestPath((a)-[:K*1..3]-(b)) RETURN b.i, length(p) ORDER BY b.i",
        )
        .await;
        let mut pairwise = Vec::new();
        for j in 0..80 {
            if j == 5 {
                continue;
            }
            let o = qp(
                &mut s,
                "MATCH (a:P {i: 5}), (b:P {i: $j}) MATCH p = shortestPath((a)-[:K*1..3]-(b)) RETURN b.i, length(p)",
                &[("j", Value::Int(j))],
            )
            .await;
            pairwise.extend(show(&o));
        }
        assert_eq!(show(&batch), pairwise, "{shards} shards");
        assert!(pairwise.len() > 20, "{}", pairwise.len());
        // Directed paths run from the start to each end.
        let o = q(
            &mut s,
            "MATCH (a:P {i: 0}), (b:P) WHERE b <> a MATCH p = shortestPath((a)-[:K*1..4]->(b)) RETURN nodes(p)[0] = a AND last(nodes(p)) = b AND size(nodes(p)) = length(p) + 1 AS ok, count(*) AS n",
        )
        .await;
        assert_eq!(
            show(&o)[0].split('|').next(),
            Some("true"),
            "{:?}",
            show(&o)
        );
    }
}

/// `CALL tx.getMetaData()` returns the Bolt transaction's `tx_metadata`
/// (the drivers' testkit, R8), an empty map without one.
#[tokio::test]
async fn tx_get_metadata_returns_the_transaction_metadata() {
    let mut s = MemStore::new(1);
    let o = q(&mut s, "CALL tx.getMetaData").await;
    assert_eq!(o.columns, ["metadata"]);
    assert_eq!(o.rows, vec![vec![Value::Map(BTreeMap::new())]]);
    let meta: BTreeMap<String, Value> = [
        ("foo".to_owned(), Value::Float(1.5)),
        ("bar".to_owned(), Value::String("baz".into())),
    ]
    .into();
    let b = Budgets {
        tx_metadata: Some(meta.clone()),
        ..Budgets::default()
    };
    for text in [
        "CALL tx.getMetaData",
        "CALL tx.getMetaData() YIELD metadata RETURN metadata",
    ] {
        let o = crate::execute(&mut s, text, &Params::new(), &b)
            .await
            .unwrap_or_else(|e| panic!("{text}: {e}"));
        assert_eq!(o.rows, vec![vec![Value::Map(meta.clone())]], "{text}");
    }
}

/// Neo4j's `CartesianProduct` notification: disconnected patterns of one
/// `MATCH`, never connected ones (the drivers' testkit, R8).
#[test]
fn cartesian_product_notifications() {
    let notes = |text: &str| {
        let q = crate::parser::parse(text).unwrap_or_else(|e| panic!("{text}: {e}"));
        crate::exec::notifications(&q)
    };
    let n = notes("EXPLAIN MATCH (n), (m) RETURN n, m");
    assert_eq!(n.len(), 1);
    assert_eq!(
        n[0].code,
        "Neo.ClientNotification.Statement.CartesianProduct"
    );
    assert!(
        n[0].description.ends_with("(identifier is: (m))"),
        "{}",
        n[0].description
    );
    assert_eq!(notes("MATCH (a)-[:R]->(b), (c) RETURN *").len(), 1);
    for connected in [
        "CREATE (n) RETURN n",
        "MATCH (n) RETURN n",
        "MATCH (a)-[:R]->(b), (b)-[:S]->(c) RETURN a, c",
        "MATCH (a) WITH a MATCH (a)-->(b), (a)-->(c) RETURN b, c",
        "UNWIND [1, 2] AS x MATCH (n {k: x}) RETURN n",
    ] {
        assert!(notes(connected).is_empty(), "{connected}");
    }
}

/// `EXPLAIN` and `PROFILE` say which they are, for the Bolt summary's
/// `plan` and `profile`.
#[tokio::test]
async fn explain_and_profile_plans() {
    let mut s = social(1).await;
    let e = q(&mut s, "EXPLAIN MATCH (n:Person) RETURN n").await;
    assert!(e.plan.is_some() && !e.profiled && e.rows.is_empty());
    let p = q(&mut s, "PROFILE MATCH (n:Person) RETURN n.name").await;
    assert!(p.plan.is_some() && p.profiled);
    assert_eq!(p.rows.len(), 5);
    let plain = q(&mut s, "MATCH (n:Person) RETURN n.name").await;
    assert!(plain.plan.is_none() && !plain.profiled && plain.notifications.is_empty());
}

/// R8 round 8: `CALL { } IN TRANSACTIONS`, reads and writes, with
/// Neo4j 5.26's answers (checked on the reference): batches commit on their
/// own; a failed batch rolls back and `ON ERROR` decides what follows.
#[tokio::test]
async fn call_in_transactions_batches_and_errors() {
    for shards in [1, 4] {
        let mut s = MemStore::new(shards);
        // A read, one row per batch, errors become nulls.
        let o = q(
            &mut s,
            "UNWIND [1, 0, 2] AS x CALL (x) { RETURN 10 / x AS y } IN TRANSACTIONS OF 1 ROW ON ERROR CONTINUE RETURN x, y",
        )
        .await;
        assert_eq!(show(&o), ["1|10", "0|null", "2|5"]);
        let o = q(
            &mut s,
            "UNWIND [1, 0, 2] AS x CALL { WITH x RETURN 10 / x AS y } IN TRANSACTIONS OF 1 ROW ON ERROR BREAK RETURN x, y",
        )
        .await;
        assert_eq!(show(&o), ["1|10", "0|null", "2|null"]);
        // Writes with a status report: the failed batch is rolled back.
        let o = q(
            &mut s,
            "UNWIND [1, 0, 2] AS x CALL (x) { CREATE (:T {x: 10 / x}) } IN TRANSACTIONS OF 1 ROW ON ERROR CONTINUE REPORT STATUS AS s RETURN x, s.committed AS c, s.errorMessage IS NULL AS ok",
        )
        .await;
        assert_eq!(show(&o), ["1|true|true", "0|false|false", "2|true|true"]);
        assert_eq!(o.stats.get("nodes-created"), Some(&2));
        let o = q(&mut s, "MATCH (t:T) RETURN t.x ORDER BY t.x").await;
        assert_eq!(show(&o), ["5", "10"]);
        // ON ERROR FAIL (the default): earlier batches stay.
        let e = crate::execute(
            &mut s,
            "UNWIND [1, 0, 2] AS x CALL (x) { CREATE (:U {x: 10 / x}) } IN TRANSACTIONS OF 1 ROW",
            &Params::new(),
            &Budgets::default(),
        )
        .await
        .expect_err("division by zero");
        assert!(e.message.ends_with("(Transactions committed: 1)"), "{e}");
        let o = q(&mut s, "MATCH (u:U) RETURN count(*)").await;
        assert_eq!(show(&o), ["1"]);
        // Batches of two rows.
        let o = q(
            &mut s,
            "UNWIND range(1, 5) AS x CALL (x) { CREATE (:V {x: x}) } IN TRANSACTIONS OF 2 ROWS",
        )
        .await;
        assert_eq!(o.stats.get("nodes-created"), Some(&5));
        // Grammar: REPORT STATUS needs CONTINUE or BREAK; RETRY is refused.
        for bad in [
            "UNWIND [1] AS x CALL (x) { RETURN 1 AS y } IN TRANSACTIONS REPORT STATUS AS s RETURN s",
            "UNWIND [1] AS x CALL (x) { RETURN 1 AS y } IN TRANSACTIONS ON ERROR RETRY RETURN y",
        ] {
            assert!(crate::parser::parse(bad).is_err(), "{bad}");
        }
        assert!(
            crate::parser::parse(
                "UNWIND [1] AS x CALL (x) { RETURN 1 AS y } IN 2 CONCURRENT TRANSACTIONS OF 10 ROWS ON ERROR BREAK REPORT STATUS AS s RETURN y, s"
            )
            .is_ok()
        );
    }
}

/// R8 round 8: a read's top `RETURN` evaluates rows as Neo4j's lazy
/// results do. `run_lazy` answers the rows before a row's error with the
/// error after them; rows past `LIMIT` are never evaluated; `run` still
/// fails the statement.
#[tokio::test]
async fn row_errors_come_after_the_rows_before_them() {
    let mut s = MemStore::new(1);
    let qy = crate::parser::parse("UNWIND [1, 0, 2] AS x RETURN 10 / x AS y").expect("parse");
    let o = crate::exec::run_lazy(&mut s, &qy, &Params::new(), &Budgets::default())
        .await
        .expect("lazy");
    assert_eq!(show(&o), ["10"]);
    let e = o.late_error.expect("late error");
    assert_eq!(
        crate::neo4j::status_code(&e),
        "Neo.ClientError.Statement.ArithmeticError"
    );
    assert!(
        crate::exec::run(&mut s, &qy, &Params::new(), &Budgets::default())
            .await
            .is_err()
    );
    // Past LIMIT nothing is evaluated (Neo4j answers 10).
    let o = q(&mut s, "UNWIND [1, 0] AS x RETURN 10 / x AS y LIMIT 1").await;
    assert_eq!(show(&o), ["10"]);
    // ORDER BY needs every row: the error is the statement's.
    let qy =
        crate::parser::parse("UNWIND [1, 0] AS x RETURN 10 / x AS y ORDER BY y").expect("parse");
    assert!(
        crate::exec::run_lazy(&mut s, &qy, &Params::new(), &Budgets::default())
            .await
            .is_err()
    );
}

/// R8 round 8: byte arrays are values, properties (stored with a `bytes`
/// type entry) and comparable, as in Neo4j.
#[tokio::test]
async fn byte_arrays_are_values_and_properties() {
    let mut s = MemStore::new(4);
    let b = Value::Bytes(vec![1, 2, 255]);
    let o = qp(
        &mut s,
        "CREATE (x:B {b: $b}) RETURN x.b AS b, size($b) AS n, valueType($b) AS t, $b = $b AS eq",
        &[("b", b.clone())],
    )
    .await;
    assert_eq!(o.rows[0][0], b);
    assert_eq!(o.rows[0][1], Value::Int(3));
    assert_eq!(
        o.rows[0][2],
        Value::String("LIST<INTEGER NOT NULL> NOT NULL".into())
    );
    assert_eq!(o.rows[0][3], Value::Bool(true));
    let o = qp(
        &mut s,
        "MATCH (x:B) WHERE x.b = $b RETURN count(*)",
        &[("b", b.clone())],
    )
    .await;
    assert_eq!(show(&o), ["1"]);
    let (props, types) =
        crate::value::props_to_json(&[("b".to_owned(), b.clone())].into_iter().collect());
    assert_eq!(props["b"], serde_json::json!("AQL/"));
    assert_eq!(
        crate::value::props_from_json(&props, types.as_ref()).get("b"),
        Some(&b)
    );
}

/// R8 round 8 (the drivers' testkit): a temporal function takes another
/// temporal value and keeps the parts it needs, as Neo4j 5.26 answers.
#[tokio::test]
async fn temporal_functions_take_other_temporal_values() {
    let mut s = MemStore::new(1);
    let o = q(
        &mut s,
        "WITH datetime('2020-03-04T05:06:07.123456789[Europe/Berlin]') AS d, localdatetime('2021-01-02T03:04:05') AS l \
         RETURN toString(localdatetime(d)), toString(date(d)), toString(localtime(d)), toString(time(d)), \
                toString(date(l)), toString(localtime(l)), toString(datetime(l)), toString(time(l))",
    )
    .await;
    assert_eq!(
        show(&o),
        [
            "2020-03-04T05:06:07.123456789|2020-03-04|05:06:07.123456789|05:06:07.123456789+01:00|2021-01-02|03:04:05|2021-01-02T03:04:05Z|03:04:05Z"
        ]
    );
    assert!(
        crate::execute(
            &mut s,
            "RETURN localdatetime(date('2022-02-03'))",
            &Params::new(),
            &Budgets::default()
        )
        .await
        .is_err()
    );
}
