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
    let p: Params = params.iter().map(|(k, v)| ((*k).to_owned(), v.clone())).collect();
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
        let o = q(&mut s, "MATCH (p:Person) WHERE p.age > 28 RETURN p.name AS n ORDER BY n").await;
        assert_eq!(show(&o), ["Alice", "Carol", "Dave"]);
        let o = q(&mut s, "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b) RETURN b.name, r.since").await;
        assert_eq!(o.columns, ["b.name", "r.since"]);
        assert_eq!(show(&o), ["Bob|2010"]);
        let o = q(&mut s, "MATCH (a)<-[:KNOWS]-(b {name: 'Bob'}) RETURN a.name").await;
        assert_eq!(show(&o), ["Carol"]);
        let o = q(&mut s, "MATCH (a {name: 'Alice'})-[:KNOWS]-(x) RETURN x.name ORDER BY x.name").await;
        assert_eq!(show(&o), ["Bob", "Dave"]);
        let o = q(&mut s, "MATCH (n) RETURN count(*) AS c, count(n.age) AS a").await;
        assert_eq!(show(&o), ["8|4"]);
        let o = q(&mut s, "MATCH (n:Nope) RETURN count(*) AS c").await;
        assert_eq!(show(&o), ["0"]);
        // Labels(), type(), id and element ids.
        let o = q(&mut s, "MATCH (a {name: 'Alice'})-[r]->(p:Product) RETURN labels(p), type(r), elementId(a) STARTS WITH 'n:'").await;
        assert_eq!(show(&o), ["[\"Product\"]|BOUGHT|true"]);
        let o = q(&mut s, "MATCH (a {name: 'Alice'}) WITH id(a) AS i MATCH (n) WHERE id(n) = i RETURN n.name").await;
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
        let o = q(&mut s, "MATCH (a {name: 'Alice'})-[:KNOWS*1..3]->(x) RETURN DISTINCT x.name AS n ORDER BY n").await;
        assert_eq!(show(&o), ["Bob", "Carol", "Dave"]);
        // Path mode (paths are counted).
        let o = q(&mut s, "MATCH p = (a {name: 'Alice'})-[:KNOWS*1..4]->(x) RETURN x.name, length(p) ORDER BY length(p)").await;
        assert_eq!(show(&o), ["Bob|1", "Carol|2", "Dave|3", "Alice|4"]);
        // Zero-length.
        let o = q(&mut s, "MATCH (a {name: 'Alice'})-[:KNOWS*0..1]->(x) RETURN x.name ORDER BY x.name").await;
        assert_eq!(show(&o), ["Alice", "Bob"]);
        // Unbounded in endpoint mode: the cycle ends it.
        let o = q(&mut s, "MATCH (a {name: 'Alice'})-[:KNOWS*]->(x) RETURN count(DISTINCT x) AS c").await;
        assert_eq!(show(&o), ["4"]);
        // Relationship list variable.
        let o = q(&mut s, "MATCH (a {name: 'Alice'})-[r:KNOWS*2]->(x) RETURN size(r), x.name").await;
        assert_eq!(show(&o), ["2|Carol"]);
        // Undirected path mode: trails in both directions.
        let o = q(&mut s, "MATCH (a {name: 'Alice'})-[:KNOWS*2]-(x) RETURN x.name ORDER BY x.name").await;
        assert_eq!(show(&o), ["Carol", "Carol"]);
        // Quantifier syntax.
        let o = q(&mut s, "MATCH (a {name: 'Alice'})-[:KNOWS]->{2,2}(x) RETURN x.name").await;
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
        let o = q(&mut s, "MATCH (a {name: 'Alice'}), (d {name: 'Dave'})
                           MATCH p = shortestPath((a)-[:KNOWS*]->(d)) RETURN length(p)").await;
        assert_eq!(show(&o), ["3"]);
        // Two shortest paths from Bob to Product Pen: via Carol only (Dave is
        // farther), and all shortest from Alice to Carol undirected: one
        // each way? Alice-Bob-Carol and Alice-Dave-Carol.
        let o = q(&mut s, "MATCH (a {name: 'Alice'}), (c {name: 'Carol'})
                           MATCH p = allShortestPaths((a)-[:KNOWS*]-(c)) RETURN [n IN nodes(p) | n.name] AS ns ORDER BY ns").await;
        assert_eq!(show(&o), ["[\"Alice\", \"Bob\", \"Carol\"]", "[\"Alice\", \"Dave\", \"Carol\"]"]);
        let o = q(&mut s, "MATCH (a {name: 'Alice'}), (e {name: 'Eve'})
                           OPTIONAL MATCH p = shortestPath((a)-[*..5]-(e)) RETURN p").await;
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
    let o = q(&mut s, "UNWIND [3, 1, 2, 1] AS x WITH DISTINCT x ORDER BY x DESC RETURN collect(x) AS xs").await;
    assert_eq!(show(&o), ["[3, 2, 1]"]);
    let o = q(&mut s, "UNWIND range(1, 10) AS x WITH x WHERE x % 3 = 0 RETURN x SKIP 1 LIMIT 1").await;
    assert_eq!(show(&o), ["6"]);
    let o = q(&mut s, "MATCH (p:Person) WITH p.age IS NULL AS missing, count(*) AS c RETURN missing, c ORDER BY missing").await;
    assert_eq!(show(&o), ["false|4", "true|1"]);
    let o = q(&mut s, "RETURN 1 AS x UNION RETURN 1 AS x UNION ALL RETURN 2 AS x").await;
    assert_eq!(show(&o).len(), 2);
    let o = q(&mut s, "MATCH (n:Person) WHERE n.name =~ 'A.*' OR n.name IN ['Eve'] RETURN n.name ORDER BY n.name").await;
    assert_eq!(show(&o), ["Alice", "Eve"]);
    let o = q(&mut s, "CALL db.labels() YIELD label RETURN label ORDER BY label").await;
    assert_eq!(show(&o), ["Person", "Product"]);
    let o = q(&mut s, "CALL db.relationshipTypes").await;
    assert_eq!(o.columns, ["relationshipType"]);
    let o = q(&mut s, "MATCH (a:Person) WHERE exists((a)-[:BOUGHT]->(:Product {name: 'Pen'})) RETURN a.name ORDER BY a.name").await;
    assert_eq!(show(&o), ["Carol", "Dave"]);
    let o = q(&mut s, "MATCH (a:Person) WHERE NOT EXISTS { (a)-[:BOUGHT]->() } RETURN a.name").await;
    assert_eq!(show(&o), ["Eve"]);
}

#[tokio::test]
async fn relationship_uniqueness_within_a_match() {
    let mut s = MemStore::new(1);
    q(&mut s, "CREATE (a:N {i: 1})-[:R]->(b:N {i: 2})").await;
    // (x)-[r1]-(y)-[r2]-(z) can't reuse the one relationship.
    let o = q(&mut s, "MATCH (x)-[r1:R]-(y)-[r2:R]-(z) RETURN count(*) AS c").await;
    assert_eq!(show(&o), ["0"]);
    // Two MATCH clauses may.
    let o = q(&mut s, "MATCH (x)-[r1:R]-(y) MATCH (y)-[r2:R]-(z) RETURN count(*) AS c").await;
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
        q(&mut s, "MATCH (a:User {id: 1}), (b:User {id: 2}) MERGE (a)-[:FOLLOWS]->(b)").await;
    }
    let o = q(&mut s, "MATCH (:User)-[r:FOLLOWS]->(:User) RETURN count(r) AS c").await;
    assert_eq!(show(&o), ["1"]);
    // UNWIND + MERGE: later rows see earlier creations.
    q(&mut s, "UNWIND [3, 3, 4] AS i MERGE (:User {id: i})").await;
    let o = q(&mut s, "MATCH (u:User) RETURN count(u) AS c").await;
    assert_eq!(show(&o), ["4"]);
    // SET items see earlier items; labels; += and =.
    let o = q(&mut s, "MATCH (n:User {id: 2}) SET n.a = 1, n.b = n.a + 1, n:Admin, n += {c: 3} RETURN n.a, n.b, n.c, labels(n)").await;
    assert_eq!(show(&o), ["1|2|3|[\"User\", \"Admin\"]"]);
    let o = q(&mut s, "MATCH (n:Admin) SET n = {z: 9} REMOVE n:Admin RETURN keys(n), labels(n)").await;
    assert_eq!(show(&o), ["[\"z\"]|[\"User\"]"]);
    let o = q(&mut s, "MATCH (n:User {id: 1}) REMOVE n.created RETURN n.created").await;
    assert_eq!(show(&o), ["null"]);
    // Maps aren't property values.
    let e = crate::execute(&mut s, "CREATE (:X {m: {a: 1}})", &Params::new(), &Budgets::default())
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
    let e = crate::execute(&mut s, "MATCH (n:Person {name: 'Alice'}) DETACH DELETE n", &Params::new(), &b)
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
    q(&mut s, "MATCH (a:C), (b:C) WHERE b.i = a.i + 1 CREATE (a)-[:NEXT]->(b)").await;
    // A star: one hub with 50 leaves.
    q(&mut s, "CREATE (h:Hub) WITH h UNWIND range(1, 50) AS i CREATE (h)-[:LEAF]->(:Leaf {i: i})").await;
    let p = Params::new();
    let check = |r: sprig_common::Result<Output>, reason: &str| {
        let e = r.expect_err(reason);
        assert_eq!(e.code(), Some(SprigCode::GraphBudgetExceeded), "{e}");
        assert!(e.message.contains(reason), "{e}");
    };
    // Unbounded past graph.max_hops (15): an error, in both modes.
    check(
        crate::execute(&mut s, "MATCH (a:C {i: 0})-[:NEXT*]->(b) RETURN count(DISTINCT b)", &p, &Budgets::default()).await,
        "hops",
    );
    check(
        crate::execute(&mut s, "MATCH (a:C {i: 0})-[:NEXT*]->(b) RETURN b", &p, &Budgets::default()).await,
        "hops",
    );
    // An explicit bound above it.
    check(
        crate::execute(&mut s, "MATCH (a:C {i: 0})-[:NEXT*1..20]->(b) RETURN b", &p, &Budgets::default()).await,
        "hops",
    );
    // With max_hops 50 the chain is exact.
    let wide = Budgets {
        max_hops: 50,
        ..Budgets::default()
    };
    let o = crate::execute(&mut s, "MATCH (a:C {i: 0})-[:NEXT*]->(b) RETURN count(DISTINCT b) AS c", &p, &wide)
        .await
        .expect("exact");
    assert_eq!(o.rows[0][0], Value::Int(39));
    // Frontier and rows.
    let narrow = Budgets {
        max_frontier: 10,
        ..Budgets::default()
    };
    check(
        crate::execute(&mut s, "MATCH (h:Hub)-[:LEAF*1..2]->(x) RETURN x", &p, &narrow).await,
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
    let o = q(&mut s, "PROFILE MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b) RETURN b.name").await;
    assert_eq!(o.rows.len(), 1);
    assert!(o.plan.expect("plan").iter().any(|l| l.contains("shard statements")));
}

#[tokio::test]
async fn properties_keep_their_types() {
    let mut s = MemStore::new(1);
    let o = q(&mut s, "CREATE (n:T {i: 1, f: 1.0, s: 'x', b: true, l: [1, 2], fl: [1.5, 2.0]}) RETURN n.i, n.f, n.l, n.fl").await;
    assert_eq!(o.rows[0][0], Value::Int(1));
    assert_eq!(o.rows[0][1], Value::Float(1.0));
    let o = q(&mut s, "MATCH (n:T) RETURN n.f, n.fl, n.i = 1, n.f = 1, n.s = 1").await;
    assert_eq!(o.rows[0][0], Value::Float(1.0));
    assert_eq!(o.rows[0][1], Value::List(vec![Value::Float(1.5), Value::Float(2.0)]));
    assert_eq!(show(&o)[0].split('|').skip(2).collect::<Vec<_>>(), ["true", "true", "false"]);
    // A pushed-down equality is a superset, rechecked: '1' doesn't match 1.
    let o = q(&mut s, "MATCH (n:T) WHERE n.i = '1' RETURN n").await;
    assert!(o.rows.is_empty());
    let _ = BTreeMap::<String, Value>::new();
}
