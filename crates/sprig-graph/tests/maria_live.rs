//! Live tests of the MariaDB store:
//! the same statements on MariaDB and in memory give the same answers, the
//! recursive CTE runs, budgets hold, and the commit check refuses dangling
//! relationships. Skipped unless `SPRIG_TEST_MARIADB_SINGLE` names a
//! server (`mysql://root:pw@127.0.0.1:13301`). Uses only the schemas
//! `r8graphs3_live_*` (one shard) and `r8graphs3_live4_*` (four shard
//! schemas with mirrors), dropped at the end.

#![allow(clippy::many_single_char_names, clippy::too_many_lines)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use sprig_common::{Result, SprigCode};
use sprig_graph::catalog::{GraphCatalog, Tables};
use sprig_graph::eval::Params;
use sprig_graph::maria::{
    CounterIds, Dictionary, GraphShards, MariaStore, OneConn, ShardLoc, check_deleted,
};
use sprig_graph::mem::MemStore;
use sprig_graph::store::IndexDef;
use sprig_graph::value::Value;
use sprig_graph::{Budgets, Output, execute};

const GRAPH: u64 = 901;

struct Dict {
    cat: String,
    pool: mysql_async::Pool,
    labels: Mutex<BTreeMap<u32, String>>,
    types: Mutex<BTreeMap<u32, String>>,
}

#[async_trait]
impl Dictionary for Dict {
    async fn create(&self, label: bool, name: &str) -> Result<u32> {
        let mut c = self
            .pool
            .get_conn()
            .await
            .map_err(sprig_graph::catalog::backend_error)?;
        let id = GraphCatalog::new(&self.cat)
            .add_source(&mut c, GRAPH, label, name)
            .await?;
        let m = if label { &self.labels } else { &self.types };
        m.lock().expect("lock").insert(id, name.to_owned());
        Ok(id)
    }
}

struct Live {
    pool: mysql_async::Pool,
    dict: Arc<Dict>,
    ids: Arc<CounterIds>,
    layout: Arc<GraphShards>,
    indexes: Mutex<Vec<IndexDef>>,
    /// Every schema the test made.
    schemas: Vec<String>,
}

impl Live {
    async fn run_with(&self, text: &str, b: &Budgets) -> Result<Output> {
        self.run_gated(text, b, None).await
    }

    /// [`Self::run_with`] with a mirror gate (read statements under async
    /// mirrors).
    async fn run_gated(
        &self,
        text: &str,
        b: &Budgets,
        gate: Option<Arc<dyn sprig_graph::mirror::MirrorGate>>,
    ) -> Result<Output> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(sprig_graph::catalog::backend_error)?;
        conn.query_drop("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .await
            .map_err(sprig_graph::catalog::backend_error)?;
        conn.query_drop("START TRANSACTION")
            .await
            .map_err(sprig_graph::catalog::backend_error)?;
        let labels = self.dict.labels.lock().expect("lock").clone();
        let types = self.dict.types.lock().expect("lock").clone();
        let r = {
            let mut one = OneConn(&mut conn);
            let r = {
                let mut store = MariaStore::new(
                    &mut one,
                    Arc::clone(&self.layout),
                    labels,
                    types,
                    self.dict.clone(),
                    self.ids.clone(),
                )
                .with_schema(self.indexes.lock().expect("lock").clone(), None)
                .with_gate(gate);
                execute(&mut store, text, &Params::new(), b).await
            };
            match r {
                Ok(o) => match check_deleted(&mut one, &self.layout, &o.deleted_nodes).await {
                    Ok(()) => Ok(o),
                    Err(e) => Err(e),
                },
                Err(e) => Err(e),
            }
        };
        let end = if r.is_ok() { "COMMIT" } else { "ROLLBACK" };
        conn.query_drop(end)
            .await
            .map_err(sprig_graph::catalog::backend_error)?;
        r
    }

    async fn cleanup(self) {
        let mut c = self.pool.get_conn().await.expect("conn");
        for s in &self.schemas {
            c.query_drop(format!("DROP DATABASE IF EXISTS {s}"))
                .await
                .expect("drop");
        }
        drop(c);
        self.pool.disconnect().await.expect("disconnect");
    }

    async fn run(&self, text: &str) -> Output {
        self.run_with(text, &Budgets::default())
            .await
            .unwrap_or_else(|e| panic!("{text}: {e}"))
    }
}

fn show(o: &Output) -> Vec<String> {
    o.rows
        .iter()
        .map(|r| r.iter().map(Value::display).collect::<Vec<_>>().join("|"))
        .collect()
}

/// A graph in schemas `<prefix>_s1..` (`shards` of them, with mirrors
/// when more than one) and its catalog in `<prefix>_cat`.
async fn setup(prefix: &str, shards: u32) -> Option<Live> {
    let url = std::env::var("SPRIG_TEST_MARIADB_SINGLE").ok()?;
    let opts = mysql_async::Opts::from_url(&url).expect("url");
    let pool = mysql_async::Pool::new(
        mysql_async::OptsBuilder::from_opts(opts).pool_opts(
            mysql_async::PoolOpts::new()
                .with_constraints(mysql_async::PoolConstraints::new(0, 16).expect("pool"))
                .with_inactive_connection_ttl(Duration::from_secs(30)),
        ),
    );
    let cat = format!("{prefix}_cat");
    let data: Vec<String> = (1..=shards).map(|i| format!("{prefix}_s{i}")).collect();
    let mut schemas = data.clone();
    schemas.push(cat.clone());
    let mut c = pool.get_conn().await.expect("conn");
    for s in &schemas {
        c.query_drop(format!("DROP DATABASE IF EXISTS {s}"))
            .await
            .expect("drop");
        c.query_drop(format!("CREATE DATABASE {s}"))
            .await
            .expect("create");
    }
    GraphCatalog::new(&cat)
        .migrate(&mut c)
        .await
        .expect("migrate");
    GraphCatalog::new(&cat)
        .insert_native(&mut c, GRAPH, 1)
        .await
        .expect("graph row");
    let layout = if shards == 1 {
        GraphShards::single(GRAPH, 1, &data[0], "c1")
    } else {
        let mut l = GraphShards::single(GRAPH, 1, &data[0], "c1");
        l.group = 7;
        l.mirrors = true;
        l.shards = (0..shards)
            .map(|i| ShardLoc {
                shard: i + 1,
                lo: u64::try_from((u128::from(i) << 64) / u128::from(shards)).expect("lo"),
                epoch: 1,
                cluster: "c1".into(),
                schema: data[i as usize].clone(),
                tables: Tables::of(&data[i as usize], GRAPH),
            })
            .collect();
        l
    };
    for sh in &layout.shards {
        for s in sh.tables.shard_ddl() {
            c.query_drop(s).await.expect("ddl");
        }
    }
    let first = Tables::of(&data[0], GRAPH);
    for s in first.seq_ddl() {
        c.query_drop(s).await.expect("seq");
    }
    drop(c);
    Some(Live {
        dict: Arc::new(Dict {
            cat,
            pool: pool.clone(),
            labels: Mutex::new(BTreeMap::new()),
            types: Mutex::new(BTreeMap::new()),
        }),
        ids: Arc::new(CounterIds::new(pool.clone(), &first, 64)),
        pool,
        layout: Arc::new(layout),
        indexes: Mutex::new(Vec::new()),
        schemas,
    })
}

const CORPUS_SETUP: &str = "CREATE (a:Person {name: 'Alice', email: 'alice@example.com', age: 30}),
    (b:Person {name: 'Bob', age: 25}), (c:Person {name: 'Carol', age: 35}),
    (d:Person {name: 'Dave', age: 40}), (e:Person {name: 'Eve'}),
    (p1:Product {name: 'Lamp', price: 10.5}), (p2:Product {name: 'Desk', price: 100}),
    (p3:Product {name: 'Pen', price: 1}),
    (a)-[:KNOWS {since: 2010}]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(a),
    (a)-[:BOUGHT]->(p1), (b)-[:BOUGHT]->(p2), (c)-[:BOUGHT]->(p2), (c)-[:BOUGHT]->(p3),
    (d)-[:BOUGHT]->(p3)";

const CORPUS: &[&str] = &[
    "MATCH (p:Person) WHERE p.age > 28 RETURN p.name AS n ORDER BY n",
    "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b) RETURN b.name, r.since",
    "MATCH (a {name: 'Alice'})-[:KNOWS]-(x) RETURN x.name ORDER BY x.name",
    "MATCH (n) RETURN count(*) AS c, count(n.age) AS a",
    "MATCH (a:Person {email: 'alice@example.com'})-[:KNOWS*1..2]->(f:Person)-[:BOUGHT]->(p:Product) WHERE NOT (a)-[:BOUGHT]->(p) AND f <> a RETURN p.name AS product, count(DISTINCT f) AS buyers ORDER BY buyers DESC, product LIMIT 5",
    "MATCH (a {name: 'Alice'})-[:KNOWS*1..3]->(x) RETURN DISTINCT x.name AS n ORDER BY n",
    "MATCH p = (a {name: 'Alice'})-[:KNOWS*1..4]->(x) RETURN x.name, length(p) ORDER BY length(p)",
    "MATCH (a {name: 'Alice'})-[:KNOWS*]->(x) RETURN count(DISTINCT x) AS c",
    "MATCH (a {name: 'Alice'})<-[:KNOWS*1..2]-(x) RETURN DISTINCT x.name AS n ORDER BY n",
    "MATCH (a {name: 'Alice'}), (c {name: 'Carol'}) MATCH p = allShortestPaths((a)-[:KNOWS*]-(c)) RETURN [n IN nodes(p) | n.name] AS ns ORDER BY ns",
    "MATCH (p:Person) OPTIONAL MATCH (p)-[:BOUGHT]->(x) RETURN p.name, count(x) AS c ORDER BY p.name",
    "MATCH (p:Product) RETURN sum(p.price) AS s, min(p.name) AS mn, max(p.price) AS mx",
    "MATCH (a:Person) WHERE exists((a)-[:BOUGHT]->(:Product {name: 'Pen'})) RETURN a.name ORDER BY a.name",
    "MATCH (n:Product) WHERE n.price = 100 RETURN n.name",
    "MATCH (n:Product) WHERE n.price = '100' RETURN n.name",
    "MATCH (n:Product {price: 10.5}) RETURN n.name",
];

#[tokio::test]
async fn maria_and_memory_agree() {
    let Some(live) = setup("r8graphs3_live", 1).await else {
        eprintln!("skipped: SPRIG_TEST_MARIADB_SINGLE is not set");
        return;
    };
    let mut mem = MemStore::new(4);
    live.run(CORPUS_SETUP).await;
    execute(&mut mem, CORPUS_SETUP, &Params::new(), &Budgets::default())
        .await
        .expect("mem setup");
    let mut agree = 0;
    for q in CORPUS {
        let a = show(&live.run(q).await);
        let b = show(
            &execute(&mut mem, q, &Params::new(), &Budgets::default())
                .await
                .expect(q),
        );
        assert_eq!(a, b, "{q}");
        agree += 1;
    }
    eprintln!(
        "maria vs memory: {agree} of {} statements agree",
        CORPUS.len()
    );

    // Writes and the commit check.
    let o = live
        .run("MATCH (n:Person {name: 'Eve'}) SET n.age = 22, n:Admin RETURN n.age, labels(n)")
        .await;
    assert_eq!(show(&o), ["22|[\"Person\", \"Admin\"]"]);
    let o = live.run("MATCH (n:Admin) RETURN n.name").await;
    assert_eq!(show(&o), ["Eve"]);
    let e = live
        .run_with(
            "MATCH (n:Person {name: 'Bob'}) DELETE n",
            &Budgets::default(),
        )
        .await
        .expect_err("dangling");
    assert_eq!(e.code(), Some(SprigCode::GraphNodeHasRelationships));
    // Rolled back: Bob is still there.
    assert_eq!(
        show(&live.run("MATCH (n {name: 'Bob'}) RETURN count(n)").await),
        ["1"]
    );
    // DELETE of the node and its relationships in one statement is fine.
    let o = live
        .run("MATCH (n:Person {name: 'Bob'})-[r]-() DELETE r, n")
        .await;
    assert_eq!(o.stats["nodes-deleted"], 1);
    let o = live
        .run("MATCH (n:Person {name: 'Carol'}) DETACH DELETE n")
        .await;
    assert_eq!(o.stats["relationships-deleted"], 3);
    let o = live
        .run("MERGE (n:Person {name: 'Zed'}) ON CREATE SET n.new = true RETURN n.new")
        .await;
    assert_eq!(show(&o), ["true"]);
    // A pushed-down boolean (not indexed) finds the node.
    let o = live.run("MATCH (n:Person {new: true}) RETURN n.name").await;
    assert_eq!(show(&o), ["Zed"]);
    let o = live
        .run("MATCH (n:Person {new: false}) RETURN n.name")
        .await;
    assert_eq!(show(&o), Vec::<String>::new());

    // A chain of 40 for the budgets: deep traversals are refused with
    // 39903, exact below the limit.
    live.run("UNWIND range(0, 39) AS i CREATE (:C {i: i})")
        .await;
    live.run("MATCH (a:C), (b:C) WHERE b.i = a.i + 1 CREATE (a)-[:NEXT]->(b)")
        .await;
    let e = live
        .run_with(
            "MATCH (a:C {i: 0})-[:NEXT*]->(b) RETURN count(DISTINCT b)",
            &Budgets::default(),
        )
        .await
        .expect_err("deep");
    assert_eq!(e.code(), Some(SprigCode::GraphBudgetExceeded));
    assert!(e.message.contains("hops"), "{e}");
    let wide = Budgets {
        max_hops: 50,
        ..Budgets::default()
    };
    let o = live
        .run_with(
            "MATCH (a:C {i: 0})-[:NEXT*]->(b) RETURN count(DISTINCT b) AS c",
            &wide,
        )
        .await
        .expect("exact");
    assert_eq!(show(&o), ["39"]);
    let o = live
        .run_with(
            "PROFILE MATCH (a:C {i: 0})-[:NEXT*1..12]->(b) RETURN count(DISTINCT b) AS c",
            &wide,
        )
        .await
        .expect("profile");
    assert_eq!(show(&o), ["12"]);
    let plan = o.plan.expect("plan").join("\n");
    assert!(plan.contains("recursive CTEs: 3"), "{plan}");

    numeric_keys(&live).await;

    live.cleanup().await;
}

/// MariaDB's index column computes the same normalized
/// number as `schema::index_value`, so a unique constraint treats `1` and
/// `1.0` as one value, and lookups by either find both.
async fn numeric_keys(live: &Live) {
    use sprig_graph::ast::{Entity, IndexKind};
    let label = live.dict.create(true, "Num").await.expect("label");
    let def = IndexDef {
        id: 77,
        name: "num_k".into(),
        unique: true,
        kind: IndexKind::Range,
        entity: Entity::Node,
        token: "Num".into(),
        token_id: label,
        props: vec!["k".into()],
        state: "online".into(),
    };
    let mut c = live.pool.get_conn().await.expect("conn");
    for sh in &live.layout.shards {
        c.query_drop(sh.tables.index_ddl(&def).expect("ddl"))
            .await
            .expect("alter");
    }
    drop(c);
    live.indexes.lock().expect("lock").push(def.clone());
    let values = [
        "1",
        "1.5",
        "-0.0",
        "1e16",
        "-7",
        "123456789012",
        "0.1",
        "1e300",
        "2.5e-8",
        "9007199254740993",
        "'1'",
        "true",
        "false",
    ];
    for v in values {
        live.run(&format!("CREATE (:Num {{k: {v}}})")).await;
    }
    // The column, as MariaDB computes it, against Rust's text.
    let mut c = live.pool.get_conn().await.expect("conn");
    let mut rows: Vec<(String, Option<String>)> = Vec::new();
    for sh in &live.layout.shards {
        let r: Vec<(String, Option<String>)> = c
            .query(format!(
                "SELECT CAST(props AS CHAR), `{}` FROM {} WHERE `{}` IS NOT NULL",
                Tables::index_column(&def, 0),
                sh.tables.nodes,
                Tables::index_column(&def, 0)
            ))
            .await
            .expect("columns");
        rows.extend(r);
    }
    drop(c);
    assert_eq!(rows.len(), values.len(), "{rows:?}");
    for (props, col) in &rows {
        let j: serde_json::Value = serde_json::from_str(props).expect("json");
        let want = sprig_graph::schema::index_value(&j["k"], IndexKind::Range);
        assert_eq!(col.as_deref(), want.as_deref(), "{props}");
    }
    // Integral floats collide with their integers.
    for dup in ["1.0", "10000000000000000", "-7.0", "0"] {
        let e = live
            .run_with(&format!("CREATE (:Num {{k: {dup}}})"), &Budgets::default())
            .await
            .expect_err(dup);
        assert!(
            matches!(e.kind, sprig_common::ErrorKind::Backend { errno: 1062, .. }),
            "{dup}: {e}"
        );
    }
    // 2^53 + 1 and its nearest float are two values, as in Neo4j.
    live.run("CREATE (:Num {k: 9007199254740992.0})").await;
    let o = live.run("MATCH (n:Num {k: 1.0}) RETURN n.k").await;
    assert_eq!(show(&o), ["1"]);
    let o = live
        .run("MATCH (n:Num) WHERE n.k IN [1e16, 1.5, 9007199254740992] RETURN count(n)")
        .await;
    assert_eq!(show(&o), ["3"]);
    eprintln!(
        "numeric keys: {} values agree with MariaDB's column",
        rows.len()
    );
}

/// A graph over four shard schemas with mirrors gives the same
/// answers as memory; incoming hops and incoming variable-length searches
/// go through the mirrors; writes keep one mirror per edge; the commit
/// check sees edges on other shards.
#[tokio::test]
async fn four_shards_with_mirrors_agree_with_memory() {
    let Some(live) = setup("r8graphs3_live4", 4).await else {
        eprintln!("skipped: SPRIG_TEST_MARIADB_SINGLE is not set");
        return;
    };
    let mut mem = MemStore::new(4);
    live.run(CORPUS_SETUP).await;
    execute(&mut mem, CORPUS_SETUP, &Params::new(), &Budgets::default())
        .await
        .expect("mem setup");
    // A ring of 60 with chords, so paths cross shards many times.
    let ring = "UNWIND range(0, 59) AS i CREATE (:R {i: i})";
    let link = "MATCH (a:R), (b:R) WHERE b.i = (a.i + 1) % 60 OR b.i = (a.i * 7) % 60 CREATE (a)-[:NEXT {w: a.i}]->(b)";
    for q in [ring, link] {
        live.run(q).await;
        execute(&mut mem, q, &Params::new(), &Budgets::default())
            .await
            .expect(q);
    }
    let extra = [
        "MATCH (a:R {i: 0})<-[:NEXT]-(x) RETURN x.i ORDER BY x.i",
        "MATCH (a:R {i: 5})<-[:NEXT*1..3]-(x) RETURN count(DISTINCT x) AS c",
        "MATCH (a:R {i: 5})-[:NEXT*1..4]->(x) RETURN count(DISTINCT x) AS c",
        "MATCH (a:R {i: 5})<-[r:NEXT*2]-(x) RETURN x.i, [e IN r | e.w] AS ws ORDER BY x.i, ws",
        "MATCH (a:R {i: 1}), (b:R {i: 31}) MATCH p = shortestPath((a)-[:NEXT*]->(b)) RETURN length(p)",
        "MATCH (a:R {i: 3})-[:NEXT]-(x) RETURN x.i ORDER BY x.i",
        "MATCH (x:R)<-[:NEXT]-(y:R) WHERE x.i < 4 RETURN x.i, count(y) ORDER BY x.i",
    ];
    let mut agree = 0;
    for q in CORPUS.iter().chain(extra.iter()) {
        let a = show(&live.run(q).await);
        let b = show(
            &execute(&mut mem, q, &Params::new(), &Budgets::default())
                .await
                .expect(q),
        );
        assert_eq!(a, b, "{q}");
        agree += 1;
    }
    let shards_hit = {
        let o = live
            .run("PROFILE MATCH (a:R {i: 5})<-[:NEXT*1..3]-(x) RETURN count(DISTINCT x) AS c")
            .await;
        o.plan.expect("plan").join("\n")
    };
    eprintln!("4 shards vs memory: {agree} statements agree\n{shards_hit}");

    // One mirror per edge, each on its destination's shard.
    let mut c = live.pool.get_conn().await.expect("conn");
    let mut edges = 0u64;
    let mut mirrors = 0u64;
    for (i, sh) in live.layout.shards.iter().enumerate() {
        let e: Option<u64> = c
            .query_first(format!("SELECT COUNT(*) FROM {}", sh.tables.edges))
            .await
            .expect("edges");
        let m: Option<u64> = c
            .query_first(format!("SELECT COUNT(*) FROM {}", sh.tables.mirrors))
            .await
            .expect("mirrors");
        edges += e.unwrap_or(0);
        mirrors += m.unwrap_or(0);
        // Every row is in its own shard's range.
        for (t, col) in [
            (&sh.tables.nodes, "_sprig_rk"),
            (&sh.tables.mirrors, "_sprig_rk"),
        ] {
            let lo = sh.lo;
            let hi = live
                .layout
                .hi(i)
                .map_or(String::new(), |h| format!(" OR _sprig_rk >= {h}"));
            let bad: Option<u64> = c
                .query_first(format!("SELECT COUNT(*) FROM {t} WHERE {col} < {lo}{hi}"))
                .await
                .expect("range");
            assert_eq!(bad, Some(0), "{t}");
        }
        assert!(e.unwrap_or(0) > 0, "shard {} has no edges", sh.shard);
    }
    drop(c);
    assert_eq!(edges, mirrors);

    // The commit check sees an incoming edge from another shard.
    let e = live
        .run_with("MATCH (n:R {i: 0}) DELETE n", &Budgets::default())
        .await
        .expect_err("dangling");
    assert_eq!(e.code(), Some(SprigCode::GraphNodeHasRelationships));
    // DETACH DELETE removes incoming edges on other shards and the mirrors.
    let q = "MATCH (n:R {i: 0}) DETACH DELETE n";
    let o = live.run(q).await;
    let m = execute(&mut mem, q, &Params::new(), &Budgets::default())
        .await
        .expect(q);
    assert_eq!(
        o.stats["relationships-deleted"],
        m.stats["relationships-deleted"]
    );
    let q = "MATCH (a:R)-[r:NEXT]->(b:R) WHERE a.i = 7 DELETE r";
    live.run(q).await;
    execute(&mut mem, q, &Params::new(), &Budgets::default())
        .await
        .expect(q);
    for q in extra
        .iter()
        .chain(["MATCH ()-[r]->() RETURN count(r)"].iter())
    {
        let a = show(&live.run(q).await);
        let b = show(
            &execute(&mut mem, q, &Params::new(), &Budgets::default())
                .await
                .expect(q),
        );
        assert_eq!(a, b, "after deletes: {q}");
    }
    let mut c = live.pool.get_conn().await.expect("conn");
    let (mut edges, mut mirrors) = (0u64, 0u64);
    for sh in &live.layout.shards {
        let e: Option<u64> = c
            .query_first(format!("SELECT COUNT(*) FROM {}", sh.tables.edges))
            .await
            .expect("edges");
        let m: Option<u64> = c
            .query_first(format!("SELECT COUNT(*) FROM {}", sh.tables.mirrors))
            .await
            .expect("mirrors");
        edges += e.unwrap_or(0);
        mirrors += m.unwrap_or(0);
    }
    drop(c);
    assert_eq!(edges, mirrors, "mirrors follow deletes");
    live.cleanup().await;
}

/// On a graph over four shard schemas, a
/// uniqueness constraint holds across shards through the claim table:
/// duplicates on other shards are refused, `MERGE` matches the holder,
/// `SET` and `DELETE` release keys, a relationship constraint works the
/// same way, and concurrent `MERGE`s on the same keys from separate
/// transactions leave exactly one node per key.
#[tokio::test]
async fn four_shards_uniqueness_across_shards() {
    use sprig_graph::ast::{Entity, IndexKind};
    let Some(live) = setup("r8four_live4u", 4).await else {
        eprintln!("skipped: SPRIG_TEST_MARIADB_SINGLE is not set");
        return;
    };
    let label = live.dict.create(true, "U").await.expect("label");
    let rtype = live.dict.create(false, "OWNS").await.expect("type");
    let node_def = IndexDef {
        id: 41,
        name: "u_k".into(),
        unique: true,
        kind: IndexKind::Range,
        entity: Entity::Node,
        token: "U".into(),
        token_id: label,
        props: vec!["k".into()],
        state: "online".into(),
    };
    let rel_def = IndexDef {
        id: 42,
        name: "owns_serial".into(),
        unique: true,
        kind: IndexKind::Range,
        entity: Entity::Relationship,
        token: "OWNS".into(),
        token_id: rtype,
        props: vec!["serial".into(), "kind".into()],
        state: "online".into(),
    };
    let mut c = live.pool.get_conn().await.expect("conn");
    for sh in &live.layout.shards {
        for d in [&node_def, &rel_def] {
            c.query_drop(sh.tables.index_ddl(d).expect("ddl"))
                .await
                .expect("alter");
        }
    }
    drop(c);
    live.indexes
        .lock()
        .expect("lock")
        .extend([node_def.clone(), rel_def.clone()]);

    // 40 keys, then each again: most second copies would land on another
    // shard than the first (ids hash over four shards).
    live.run("UNWIND range(1, 40) AS i CREATE (:U {k: i})")
        .await;
    let mut refused = 0;
    let mut other_shard = 0;
    for i in 1..=40 {
        let q = format!("CREATE (n:U {{k: {i}.0}}) RETURN id(n)");
        let e = live
            .run_with(&q, &Budgets::default())
            .await
            .expect_err("duplicate across shards");
        assert!(
            matches!(e.kind, sprig_common::ErrorKind::Backend { errno: 1062, .. }),
            "{q}: {e}"
        );
        assert!(e.message.contains("already exists with label `U`"), "{e}");
        refused += 1;
        // Where a new node would have gone: the next id's shard.
        let o = live
            .run("CREATE (n:Tmp) WITH n, id(n) AS i DELETE n RETURN i")
            .await;
        let next: u64 = show(&o)[0].parse().expect("id");
        let holder = live
            .run(&format!("MATCH (n:U {{k: {i}}}) RETURN id(n)"))
            .await;
        let h: u64 = show(&holder)[0].parse().expect("id");
        if live.layout.index_of(sprig_graph::store::rk_of(next))
            != live.layout.index_of(sprig_graph::store::rk_of(h))
        {
            other_shard += 1;
        }
    }
    assert_eq!(refused, 40);
    assert!(
        other_shard > 10,
        "only {other_shard} of 40 on another shard"
    );
    // MERGE on a held key matches the holder, wherever it is.
    let o = live
        .run("UNWIND range(1, 40) AS i MERGE (n:U {k: i}) ON MATCH SET n.seen = true RETURN count(*)")
        .await;
    assert_eq!(show(&o), ["40"]);
    let o = live.run("MATCH (n:U) RETURN count(n), count(n.seen)").await;
    assert_eq!(show(&o), ["40|40"]);
    // SET moves a key: the old one is free, the new one is held.
    live.run("MATCH (n:U {k: 1}) SET n.k = 1001").await;
    live.run("CREATE (:U {k: 1})").await;
    assert!(
        live.run_with("CREATE (:U {k: 1001})", &Budgets::default())
            .await
            .is_err()
    );
    // Removing the label frees the key too; DELETE frees it.
    live.run("MATCH (n:U {k: 2}) REMOVE n:U").await;
    live.run("CREATE (:U {k: 2})").await;
    live.run("MATCH (n:U {k: 3}) DELETE n").await;
    live.run("CREATE (:U {k: 3})").await;
    assert!(
        live.run_with("CREATE (:U {k: 3})", &Budgets::default())
            .await
            .is_err()
    );
    // A node without the property, or with another type, claims nothing.
    live.run("CREATE (:U), (:U), (:U {k: '3'})").await;
    // Relationship constraints over two properties.
    live.run("MATCH (a:U {k: 4}), (b:U {k: 5}) CREATE (a)-[:OWNS {serial: 7, kind: 'x'}]->(b)")
        .await;
    for (a, b) in [(6, 7), (8, 9), (10, 11)] {
        let q = format!(
            "MATCH (a:U {{k: {a}}}), (b:U {{k: {b}}}) CREATE (a)-[:OWNS {{serial: 7, kind: 'x'}}]->(b)"
        );
        assert!(live.run_with(&q, &Budgets::default()).await.is_err(), "{q}");
    }
    live.run("MATCH (a:U {k: 6}), (b:U {k: 7}) CREATE (a)-[:OWNS {serial: 7, kind: 'y'}]->(b)")
        .await;
    live.run("MATCH ()-[r:OWNS {kind: 'x'}]->() SET r.serial = 8")
        .await;
    live.run("MATCH (a:U {k: 8}), (b:U {k: 9}) CREATE (a)-[:OWNS {serial: 7, kind: 'x'}]->(b)")
        .await;
    live.run("MATCH ()-[r:OWNS {serial: 8}]->() DELETE r").await;
    live.run("MATCH (a:U {k: 10}), (b:U {k: 11}) CREATE (a)-[:OWNS {serial: 8, kind: 'x'}]->(b)")
        .await;
    let o = live
        .run("MATCH ()-[r:OWNS]->() RETURN r.serial, r.kind ORDER BY r.serial, r.kind")
        .await;
    assert_eq!(show(&o), ["7|x", "7|y", "8|x"]);

    // Concurrent MERGEs: 8 writers, each its own transaction, MERGE the
    // same 25 new keys in different orders.
    let live = Arc::new(live);
    let mut tasks = Vec::new();
    for w in 0..8u64 {
        let live = Arc::clone(&live);
        tasks.push(tokio::spawn(async move {
            let (mut ok, mut retried) = (0, 0);
            for j in 0..25u64 {
                let k = 500 + (j * 7 + w * 3) % 25;
                let q = format!(
                    "MERGE (n:U {{k: {k}}}) ON CREATE SET n.by = {w} ON MATCH SET n.seen = coalesce(n.seen, 0) + 1 RETURN n.k"
                );
                for _ in 0..20 {
                    match live.run_with(&q, &Budgets::default()).await {
                        Ok(_) => {
                            ok += 1;
                            break;
                        }
                        // Deadlocks and lock waits are retryable.
                        Err(e)
                            if matches!(
                                e.kind,
                                sprig_common::ErrorKind::Backend {
                                    errno: 1213 | 1205,
                                    ..
                                }
                            ) =>
                        {
                            retried += 1;
                        }
                        Err(e) => panic!("{q}: {e}"),
                    }
                }
            }
            (ok, retried)
        }));
    }
    let (mut merged, mut retried) = (0, 0);
    for t in tasks {
        let (m, r) = t.await.expect("task");
        merged += m;
        retried += r;
    }
    assert_eq!(merged, 8 * 25);
    let o = live
        .run("MATCH (n:U) WHERE n.k >= 500 AND n.k < 525 RETURN count(n), count(DISTINCT n.k)")
        .await;
    assert_eq!(show(&o), ["25|25"]);
    // Every claim names a live holder with that key.
    let mut c = live.pool.get_conn().await.expect("conn");
    let mut claims = 0u64;
    for sh in &live.layout.shards {
        let n: Option<u64> = c
            .query_first(format!(
                "SELECT COUNT(*) FROM {} WHERE idx = 41",
                sh.tables.claims
            ))
            .await
            .expect("claims");
        claims += n.unwrap_or(0);
    }
    drop(c);
    let o = live
        .run("MATCH (n:U) WHERE n.k IS NOT NULL RETURN count(n)")
        .await;
    assert_eq!(show(&o), [claims.to_string()]);
    eprintln!(
        "uniqueness across 4 shards: {refused} duplicates refused ({other_shard} on another shard), {merged} concurrent MERGEs ({retried} deadlock retries), {claims} claims"
    );
    let Ok(live) = Arc::try_unwrap(live) else {
        panic!("live still shared");
    };
    live.cleanup().await;
}

/// A gate with a fixed answer.
struct Gate(bool);

#[async_trait]
impl sprig_graph::mirror::MirrorGate for Gate {
    async fn fresh(&self, _layout: &GraphShards) -> bool {
        self.0
    }
}

/// The applier's connections: the test's one server.
struct Conns(mysql_async::Pool);

#[async_trait]
impl sprig_graph::mirror::ApplyConns for Conns {
    async fn connect(&mut self, _cluster: &str) -> Result<mysql_async::Conn> {
        self.0
            .get_conn()
            .await
            .map_err(sprig_graph::catalog::backend_error)
    }
}

/// Every edge and every mirror of the graph: `(id, src, dst, type)`.
async fn edges_and_mirrors(live: &Live) -> (Vec<(u64, u64, u64, u32)>, Vec<(u64, u64, u64, u32)>) {
    let mut c = live.pool.get_conn().await.expect("conn");
    let (mut e, mut m) = (Vec::new(), Vec::new());
    for (i, sh) in live.layout.shards.iter().enumerate() {
        let rows: Vec<(u64, u64, u64, u32)> = c
            .query(format!(
                "SELECT id, src, dst, type FROM {}",
                sh.tables.edges
            ))
            .await
            .expect("edges");
        e.extend(rows);
        let rows: Vec<(u64, u64, u64, u32, u64)> = c
            .query(format!(
                "SELECT id, src, dst, type, _sprig_rk FROM {}",
                sh.tables.mirrors
            ))
            .await
            .expect("mirrors");
        for r in rows {
            assert_eq!(live.layout.index_of(r.4), i, "mirror {} off its shard", r.0);
            m.push((r.0, r.1, r.2, r.3));
        }
    }
    e.sort_unstable();
    m.sort_unstable();
    (e, m)
}

/// The edges as the binlog would show their inserts.
async fn insert_events(live: &Live) -> Vec<sprig_graph::mirror::EdgeChange> {
    let mut c = live.pool.get_conn().await.expect("conn");
    let mut out = Vec::new();
    for sh in &live.layout.shards {
        let rows: Vec<(u64, u64, u64, u32, u64, u64)> = c
            .query(format!(
                "SELECT id, src, dst, type, _sprig_rk, dst_rk FROM {}",
                sh.tables.edges
            ))
            .await
            .expect("edges");
        out.extend(
            rows.into_iter()
                .map(
                    |(id, src, dst, rtype, src_rk, dst_rk)| sprig_graph::mirror::EdgeChange {
                        id,
                        src,
                        dst,
                        rtype,
                        src_rk,
                        dst_rk,
                        live: true,
                    },
                ),
        );
    }
    out
}

/// `graph.mirror = async`: writers on four shard schemas keep no
/// mirrors; reads without a fresh gate scatter and still equal memory; the
/// applier's batches (also replayed, and with deletes seen before their
/// inserts, as two clusters' streams can be) leave exactly one mirror per
/// edge; reads through the applied mirrors equal memory; and writers
/// racing the applier never leave a mirror without its edge.
#[tokio::test]
async fn async_mirrors_converge() {
    use sprig_graph::mirror::{EdgeChange, apply_batch};
    let Some(mut live) = setup("r8five_live4a", 4).await else {
        eprintln!("skipped: SPRIG_TEST_MARIADB_SINGLE is not set");
        return;
    };
    {
        let mut l = (*live.layout).clone();
        l.mirror_async = true;
        live.layout = Arc::new(l);
    }
    let b = Budgets::default();
    let mut mem = MemStore::new(4);
    let ring = "UNWIND range(0, 59) AS i CREATE (:R {i: i})";
    let link = "MATCH (a:R), (b:R) WHERE b.i = (a.i + 1) % 60 OR b.i = (a.i * 7) % 60 CREATE (a)-[:NEXT {w: a.i}]->(b)";
    for q in [CORPUS_SETUP, ring, link] {
        live.run(q).await;
        execute(&mut mem, q, &Params::new(), &b).await.expect(q);
    }
    let (edges, mirrors) = edges_and_mirrors(&live).await;
    assert!(!edges.is_empty());
    assert!(mirrors.is_empty(), "writers keep no asynchronous mirrors");
    let extra = [
        "MATCH (a:R {i: 0})<-[:NEXT]-(x) RETURN x.i ORDER BY x.i",
        "MATCH (a:R {i: 5})<-[:NEXT*1..3]-(x) RETURN count(DISTINCT x) AS c",
        "MATCH (a:R {i: 5})<-[r:NEXT*2]-(x) RETURN x.i, [e IN r | e.w] AS ws ORDER BY x.i, ws",
        "MATCH (a:R {i: 3})-[:NEXT]-(x) RETURN x.i ORDER BY x.i",
        "MATCH (x:R)<-[:NEXT]-(y:R) WHERE x.i < 4 RETURN x.i, count(y) ORDER BY x.i",
        "MATCH (a {name: 'Alice'})<-[:KNOWS*1..2]-(x) RETURN DISTINCT x.name AS n ORDER BY n",
    ];
    let qs: Vec<&str> = CORPUS.iter().chain(extra.iter()).copied().collect();
    // Lagging mirrors: incoming hops scatter.
    compare_gated(&live, &mut mem, &qs, false, "scatter").await;
    // The applier: every insert, twice (a replay after a restart).
    let mut pool = Conns(live.pool.clone());
    let ins = insert_events(&live).await;
    let a = apply_batch(&mut pool, &live.layout, &ins, 1)
        .await
        .expect("apply");
    assert_eq!(a.upserts, ins.len() as u64);
    apply_batch(&mut pool, &live.layout, &ins, 2)
        .await
        .expect("replay");
    let (edges, mirrors) = edges_and_mirrors(&live).await;
    assert_eq!(edges, mirrors, "one mirror per edge");
    compare_gated(&live, &mut mem, &qs, true, "through mirrors").await;
    // Deletes: a node with incoming edges from other shards, and one edge.
    let gone_before: std::collections::BTreeSet<u64> = edges.iter().map(|e| e.0).collect();
    for q in [
        "MATCH (n:R {i: 0}) DETACH DELETE n",
        "MATCH (a:R)-[r:NEXT]->(b:R) WHERE a.i = 7 DELETE r",
    ] {
        live.run(q).await;
        execute(&mut mem, q, &Params::new(), &b).await.expect(q);
    }
    let (edges, _) = edges_and_mirrors(&live).await;
    let now: std::collections::BTreeSet<u64> = edges.iter().map(|e| e.0).collect();
    let deleted: Vec<EdgeChange> = ins
        .iter()
        .filter(|e| gone_before.contains(&e.id) && !now.contains(&e.id))
        .map(|e| EdgeChange { live: false, ..*e })
        .collect();
    assert!(deleted.len() >= 3, "{} deleted", deleted.len());
    // The deletes' stream first, then a late replay of the inserts (as a
    // lagging cluster's stream would deliver them): no stale mirror.
    apply_batch(&mut pool, &live.layout, &deleted, 3)
        .await
        .expect("deletes");
    apply_batch(&mut pool, &live.layout, &ins, 4)
        .await
        .expect("late inserts");
    let (edges, mirrors) = edges_and_mirrors(&live).await;
    assert_eq!(
        edges, mirrors,
        "late inserts of deleted edges leave no mirror"
    );
    compare_gated(&live, &mut mem, &qs, true, "after deletes").await;
    // Writers racing the applier: 6 writers (one after another) create
    // and delete edges while the applier replays what it saw, deletes
    // first.
    let writers = async {
        for t in 0..6i64 {
            for k in 0..15i64 {
                live.run(&format!(
                    "MATCH (a:R {{i: {}}}), (b:R {{i: {}}}) CREATE (a)-[:RACE {{t: {t}, k: {k}}}]->(b)",
                    1 + (t * 7 + k) % 59,
                    1 + (t * 13 + k * 5) % 59
                ))
                .await;
                if k % 2 == 0 {
                    live.run(&format!(
                        "MATCH ()-[r:RACE {{t: {t}, k: {k}}}]->() DELETE r"
                    ))
                    .await;
                }
            }
        }
    };
    let applier = {
        let pool = live.pool.clone();
        let layout = Arc::clone(&live.layout);
        let live = &live;
        async move {
            let mut conns = Conns(pool);
            let mut seen: Vec<EdgeChange> = Vec::new();
            for round in 0..20u64 {
                let now = insert_events(live).await;
                let ids: std::collections::BTreeSet<u64> = now.iter().map(|e| e.id).collect();
                // Edges seen before and gone now: their deletes go first.
                let mut batch: Vec<EdgeChange> = seen
                    .iter()
                    .filter(|e| !ids.contains(&e.id))
                    .map(|e| EdgeChange { live: false, ..*e })
                    .collect();
                batch.extend(seen.iter().copied());
                batch.extend(now.iter().copied());
                apply_batch(&mut conns, &layout, &batch, 10 + round)
                    .await
                    .expect("racing apply");
                for e in now {
                    if !seen.iter().any(|x| x.id == e.id) {
                        seen.push(e);
                    }
                }
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
            seen
        }
    };
    let (seen, ()) = tokio::join!(applier, writers);
    // The final stream: what the applier hadn't seen yet, then every
    // delete.
    let now = insert_events(&live).await;
    let ids: std::collections::BTreeSet<u64> = now.iter().map(|e| e.id).collect();
    let mut last: Vec<EdgeChange> = now.clone();
    last.extend(
        seen.iter()
            .filter(|e| !ids.contains(&e.id))
            .map(|e| EdgeChange { live: false, ..*e }),
    );
    apply_batch(&mut Conns(live.pool.clone()), &live.layout, &last, 99)
        .await
        .expect("final apply");
    let (edges, mirrors) = edges_and_mirrors(&live).await;
    assert_eq!(edges, mirrors, "mirrors equal edges after the race");
    eprintln!(
        "async mirrors: {} edges, {} mirrors, {} edges seen by the racing applier",
        edges.len(),
        mirrors.len(),
        seen.len()
    );
    live.cleanup().await;
}

/// `qs` on `live` with a gate answering `gate`, against memory.
async fn compare_gated(live: &Live, mem: &mut MemStore, qs: &[&str], gate: bool, label: &str) {
    for q in qs {
        let a = show(
            &live
                .run_gated(q, &Budgets::default(), Some(Arc::new(Gate(gate))))
                .await
                .unwrap_or_else(|e| panic!("{label}: {q}: {e}")),
        );
        let want = show(
            &execute(mem, q, &Params::new(), &Budgets::default())
                .await
                .expect(q),
        );
        assert_eq!(a, want, "{label}: {q}");
    }
}

/// The official LDBC run's 20th index failed with MariaDB 1117 ("Table
/// definition is too large"). Forty indexes on one node
/// table, on the same few properties of many labels, plus a text index
/// and a uniqueness constraint, all created and all used.
#[tokio::test]
async fn many_indexes_fit_one_table() {
    use sprig_graph::ast::{Entity, IndexKind};
    let Some(live) = setup("r8ten_live_ix", 1).await else {
        eprintln!("skipped: SPRIG_TEST_MARIADB_SINGLE is not set");
        return;
    };
    let mut c = live.pool.get_conn().await.expect("conn");
    let mut defs = Vec::new();
    for i in 0..40u32 {
        let name = format!("L{i}");
        let label = live.dict.create(true, &name).await.expect("label");
        let (prop, kind, unique) = match i {
            0 => ("email", IndexKind::Range, true),
            1 => ("name", IndexKind::Text, false),
            _ if i % 3 == 0 => ("creationDate", IndexKind::Range, false),
            _ if i % 3 == 1 => ("name", IndexKind::Range, false),
            _ => ("id", IndexKind::Range, false),
        };
        let def = IndexDef {
            id: 100 + i,
            name: format!("ix_{name}_{prop}"),
            unique,
            kind,
            entity: Entity::Node,
            token: name.clone(),
            token_id: label,
            props: vec![prop.into()],
            state: "online".into(),
        };
        for sh in &live.layout.shards {
            c.query_drop(sh.tables.index_ddl(&def).expect("ddl"))
                .await
                .unwrap_or_else(|e| panic!("index {i} ({name}.{prop}): {e}"));
        }
        defs.push(def);
    }
    drop(c);
    *live.indexes.lock().expect("lock") = defs;
    live.run(
        "CREATE (:L0 {email: 'a@x', id: 1}), (:L1 {name: 'Ann'}), (:L1 {name: 7}),
                (:L2 {id: 5, name: 'x'}), (:L2 {id: 6}), (:L3 {creationDate: 1289908800123}),
                (:L4 {name: 'Bo'}), (:L38 {id: 5}), (:L39 {creationDate: 3})",
    )
    .await;
    for (text, want) in [
        ("MATCH (n:L2 {id: 5}) RETURN n.name", vec!["x"]),
        ("MATCH (n:L38) WHERE n.id = 5 RETURN count(n)", vec!["1"]),
        (
            "MATCH (n:L3) WHERE n.creationDate = 1289908800123 RETURN count(n)",
            vec!["1"],
        ),
        ("MATCH (n:L4 {name: 'Bo'}) RETURN count(n)", vec!["1"]),
        ("MATCH (n:L1 {name: 'Ann'}) RETURN count(n)", vec!["1"]),
        ("MATCH (n:L0 {email: 'a@x'}) RETURN n.id", vec!["1"]),
    ] {
        assert_eq!(show(&live.run(text).await), want, "{text}");
    }
    // The constraint holds; the text index holds strings only.
    let e = live
        .run_with("CREATE (:L0 {email: 'a@x'})", &Budgets::default())
        .await
        .expect_err("duplicate");
    assert!(
        matches!(e.kind, sprig_common::ErrorKind::Backend { errno: 1062, .. }),
        "{e}"
    );
    let mut c = live.pool.get_conn().await.expect("conn");
    let sh = &live.layout.shards[0];
    let texts: Vec<Option<String>> = c
        .query(format!(
            "SELECT `_sprig_i101_0` FROM {} WHERE JSON_CONTAINS(labels, '{}') ORDER BY id",
            sh.tables.nodes,
            live.indexes.lock().expect("lock")[1].token_id
        ))
        .await
        .expect("text column");
    assert_eq!(texts, [Some("sAnn".to_owned()), None]);
    let n: Option<u64> = c
        .query_first(format!(
            "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '{}' \
             AND TABLE_NAME = '{}' AND INDEX_NAME LIKE '\\_sprig\\_i%'",
            sh.schema,
            sh.tables
                .nodes
                .rsplit('.')
                .next()
                .unwrap_or_default()
                .trim_matches('`')
        ))
        .await
        .expect("statistics");
    drop(c);
    assert_eq!(n, Some(40), "indexes on the node table");
    live.cleanup().await;
}
