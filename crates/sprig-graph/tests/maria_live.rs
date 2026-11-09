//! Live tests of the MariaDB store (chapter 43 build steps 3 and 11):
//! the same statements on MariaDB and in memory give the same answers, the
//! recursive CTE runs, budgets hold, and the commit check refuses dangling
//! relationships. Skipped unless `SPRIG_TEST_MARIADB_SINGLE` names a
//! server (`mysql://root:pw@127.0.0.1:13301`). Uses only the schemas
//! `r8graphs_live_data` and `r8graphs_live_cat`, dropped at the end.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use sprig_common::{Result, SprigCode};
use sprig_graph::catalog::{GraphCatalog, Tables};
use sprig_graph::eval::Params;
use sprig_graph::maria::{CounterIds, Dictionary, MariaStore, check_deleted};
use sprig_graph::mem::MemStore;
use sprig_graph::value::Value;
use sprig_graph::{Budgets, Output, execute};

const DATA: &str = "r8graphs_live_data";
const CAT: &str = "r8graphs_live_cat";
const GRAPH: u64 = 901;

struct Dict {
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
        let id = GraphCatalog::new(CAT)
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
    tables: Tables,
}

impl Live {
    async fn run_with(&self, text: &str, b: &Budgets) -> Result<Output> {
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
            let mut store = MariaStore::new(
                &mut conn,
                GRAPH,
                self.tables.clone(),
                labels,
                types,
                self.dict.clone(),
                self.ids.clone(),
            );
            execute(&mut store, text, &Params::new(), b).await
        };
        let r = match r {
            Ok(o) => match check_deleted(&mut conn, &self.tables, &o.deleted_nodes).await {
                Ok(()) => Ok(o),
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        };
        let end = if r.is_ok() { "COMMIT" } else { "ROLLBACK" };
        conn.query_drop(end)
            .await
            .map_err(sprig_graph::catalog::backend_error)?;
        r
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

async fn setup() -> Option<Live> {
    let url = std::env::var("SPRIG_TEST_MARIADB_SINGLE").ok()?;
    let opts = mysql_async::Opts::from_url(&url).expect("url");
    let pool = mysql_async::Pool::new(
        mysql_async::OptsBuilder::from_opts(opts).pool_opts(
            mysql_async::PoolOpts::new()
                .with_constraints(mysql_async::PoolConstraints::new(0, 4).expect("pool"))
                .with_inactive_connection_ttl(Duration::from_secs(30)),
        ),
    );
    let mut c = pool.get_conn().await.expect("conn");
    for s in [DATA, CAT] {
        c.query_drop(format!("DROP DATABASE IF EXISTS {s}"))
            .await
            .expect("drop");
        c.query_drop(format!("CREATE DATABASE {s}"))
            .await
            .expect("create");
    }
    GraphCatalog::new(CAT)
        .migrate(&mut c)
        .await
        .expect("migrate");
    GraphCatalog::new(CAT)
        .insert_native(&mut c, GRAPH, 1)
        .await
        .expect("graph row");
    let tables = Tables::of(DATA, GRAPH);
    for s in tables.create_ddl() {
        c.query_drop(s).await.expect("ddl");
    }
    drop(c);
    Some(Live {
        dict: Arc::new(Dict {
            pool: pool.clone(),
            labels: Mutex::new(BTreeMap::new()),
            types: Mutex::new(BTreeMap::new()),
        }),
        ids: Arc::new(CounterIds::new(pool.clone(), &tables, 64)),
        pool,
        tables,
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
    let Some(live) = setup().await else {
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

    let mut c = live.pool.get_conn().await.expect("conn");
    for s in [DATA, CAT] {
        c.query_drop(format!("DROP DATABASE IF EXISTS {s}"))
            .await
            .expect("drop");
    }
    drop(c);
    live.pool.disconnect().await.expect("disconnect");
}
