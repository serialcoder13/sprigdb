//! Sprig 2PC under graph writes (chapter 18's held branches; chapter 43,
//! "Writes and transactions"; R8 round 5, decision G49), live.
//!
//! A graph over four shard schemas on two clusters that are treated as
//! having no XA (as Galera), with synchronous mirrors, so every edge
//! between the two clusters writes both in one transaction; policy
//! `sprig_2pc`. The store reports each write it runs through
//! `ShardConns::wrote` (values in the text), which the held branch
//! records for redo. Chapter 18's crash simulations follow: a coordinator
//! that dies after prepare (the resolver aborts), after the commit point
//! (the resolver replays the held branch), and at a random step in many
//! concurrent transactions; after recovery every edge has its mirror and
//! every mirror its edge.
//!
//! ```sh
//! SPRIG_TEST_GRAPH_2PC=mysql://root:sprig-dev-root@127.0.0.1:13301,mysql://root:sprig-dev-root@127.0.0.1:13321 \
//! cargo test -p sprig-graph --test sprig2pc_live -- --nocapture
//! ```
//!
//! Uses only the schemas `t921306099999_d1_g7_s1..4` and `r8five_2pc_cat`,
//! dropped at the end.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::missing_panics_doc,
    clippy::many_single_char_names
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, Pool};
use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId};
use sprig_common::{Result, SprigError};
use sprig_graph::catalog::{GraphCatalog, Tables};
use sprig_graph::eval::Params;
use sprig_graph::maria::{
    CounterIds, Dictionary, GraphShards, MariaStore, ShardConns, ShardLoc, check_deleted,
};
use sprig_graph::{Budgets, execute};
use sprig_txn::decision::create_table_sql;
use sprig_txn::mysql::{MyBackend, MyCluster};
use sprig_txn::{
    BeginOptions, ClusterCaps, Dtid, DtidGen, Intent, NonXaPolicy, Point, Resolver, ResolverConfig,
    ShardRef, StaticLocator, TxnBackend, TxnConfig, TxnCoordinator, TxnHooks, TxnMode,
};

const TENANT: TenantId = TenantId(921_306_099_999);
const GRAPH: u64 = 77;

struct Env {
    urls: Vec<String>,
    ids: Vec<ClusterId>,
    layout: Arc<GraphShards>,
    pool0: Pool,
    dict: Arc<Dict>,
    counter: Arc<CounterIds>,
    backend: MyBackend,
    resolver: Resolver,
}

struct Dict {
    pool: Pool,
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
        let id = GraphCatalog::new("r8five_2pc_cat")
            .add_source(&mut c, GRAPH, label, name)
            .await?;
        let m = if label { &self.labels } else { &self.types };
        m.lock().unwrap().insert(id, name.to_owned());
        Ok(id)
    }
}

/// The coordinator's branches as the store's connections, reporting
/// writes to held branches.
struct Coord<'a> {
    t: &'a mut TxnCoordinator<MyBackend>,
}

fn shard_ref(at: &ShardLoc) -> ShardRef {
    ShardRef {
        db: DbId(1),
        group: GroupId(7),
        shard: ShardId(at.shard),
        hash_point: at.lo,
        schema: at.schema.clone(),
    }
}

#[async_trait]
impl ShardConns for Coord<'_> {
    async fn conn(&mut self, at: &ShardLoc, write: bool) -> Result<&mut Conn> {
        let id = ClusterId::new(&at.cluster).unwrap();
        let intent = if write { Intent::Write } else { Intent::Read };
        let b = self.t.branch(&id, &shard_ref(at), intent).await?;
        b.conn_mut()
            .raw()
            .map_err(|e| SprigError::internal(e.to_string()))
    }

    fn records(&self) -> bool {
        true
    }

    fn wrote(&mut self, at: &ShardLoc, sql: &str) {
        let id = ClusterId::new(&at.cluster).unwrap();
        for (_, c) in self.t.conns_mut(&[id]) {
            c.note_write(sql);
        }
    }
}

fn pool(url: &str) -> Pool {
    Pool::new(
        mysql_async::OptsBuilder::from_opts(Opts::from_url(url).unwrap()).pool_opts(
            mysql_async::PoolOpts::default()
                .with_constraints(mysql_async::PoolConstraints::new(0, 8).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(30)),
        ),
    )
}

async fn q(url: &str, sql: &str) -> Vec<mysql_async::Row> {
    let p = pool(url);
    let mut c = p.get_conn().await.unwrap();
    let r = c.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    drop(c);
    p.disconnect().await.unwrap();
    r
}

fn schema(n: u32) -> String {
    format!("t{TENANT}_d1_g7_s{n}")
}

async fn setup() -> Option<Env> {
    let v = std::env::var("SPRIG_TEST_GRAPH_2PC").ok()?;
    let urls: Vec<String> = v.split(',').map(|s| s.trim().to_owned()).collect();
    assert!(urls.len() >= 2, "two cluster URLs");
    let urls = urls[..2].to_vec();
    let ids: Vec<ClusterId> = (0..2)
        .map(|i| ClusterId::new(format!("r8five-g{}", i + 1)).unwrap())
        .collect();
    // Shards 1, 2 on the first cluster, 3, 4 on the second.
    let shards: Vec<ShardLoc> = (1..=4u32)
        .map(|n| {
            let c = usize::from(n > 2);
            ShardLoc {
                shard: n,
                lo: u64::try_from((u128::from(n - 1) << 64) / 4).unwrap(),
                epoch: 1,
                cluster: ids[c].to_string(),
                schema: schema(n),
                tables: Tables::of(&schema(n), GRAPH),
            }
        })
        .collect();
    for (n, sh) in shards.iter().enumerate() {
        let url = &urls[usize::from(n >= 2)];
        q(url, &format!("DROP DATABASE IF EXISTS `{}`", sh.schema)).await;
        q(url, &format!("CREATE DATABASE `{}`", sh.schema)).await;
        for s in sh.tables.shard_ddl() {
            q(url, &s).await;
        }
        q(url, &create_table_sql(&sh.schema)).await;
    }
    q(&urls[0], "DROP DATABASE IF EXISTS r8five_2pc_cat").await;
    q(&urls[0], "CREATE DATABASE r8five_2pc_cat").await;
    let pool0 = pool(&urls[0]);
    {
        let mut c = pool0.get_conn().await.unwrap();
        GraphCatalog::new("r8five_2pc_cat")
            .migrate(&mut c)
            .await
            .unwrap();
        GraphCatalog::new("r8five_2pc_cat")
            .insert_native(&mut c, GRAPH, 1)
            .await
            .unwrap();
        for s in shards[0].tables.seq_ddl() {
            c.query_drop(s).await.unwrap();
        }
    }
    let mut layout = GraphShards::single(GRAPH, 1, &schema(1), ids[0].as_str());
    layout.group = 7;
    layout.mirrors = true;
    layout.shards = shards;
    let mut m = BTreeMap::new();
    for (i, u) in urls.iter().enumerate() {
        let opts = Opts::from_url(u).unwrap();
        m.insert(
            ids[i].clone(),
            MyCluster {
                opts: opts.clone(),
                admin_opts: opts,
                // Treated as Galera: no XA, so two writers need Sprig 2PC.
                caps: ClusterCaps { xa: false },
            },
        );
    }
    let backend = MyBackend::new(m);
    let locator = Arc::new(StaticLocator::new());
    for sh in &layout.shards {
        locator.insert(
            TENANT,
            shard_ref(sh).anchor(),
            ClusterId::new(&sh.cluster).unwrap(),
            &sh.schema,
        );
    }
    let resolver = Resolver::new(
        backend.admin(),
        locator,
        ResolverConfig {
            interval: Duration::from_secs(1),
            min_age: Duration::from_secs(1),
            abort_retention: Duration::from_secs(24 * 3600),
            id: "r8five-resolver".into(),
        },
    );
    Some(Env {
        counter: Arc::new(CounterIds::new(
            pool0.clone(),
            &Tables::of(&schema(1), GRAPH),
            64,
        )),
        dict: Arc::new(Dict {
            pool: pool0.clone(),
            labels: Mutex::new(BTreeMap::new()),
            types: Mutex::new(BTreeMap::new()),
        }),
        urls,
        ids,
        layout: Arc::new(layout),
        pool0,
        backend,
        resolver,
    })
}

fn cfg() -> TxnConfig {
    TxnConfig {
        policy: NonXaPolicy::Sprig2pc,
        coordinator_id: "r8five-2pc".into(),
        ..TxnConfig::default()
    }
}

/// Holds the coordinator at `point` (once), never releasing: a crash.
struct Hold(Point, std::sync::atomic::AtomicBool);

#[async_trait]
impl TxnHooks for Hold {
    async fn at(&self, _dtid: Option<Dtid>, p: Point) {
        if p == self.0 && self.1.swap(false, std::sync::atomic::Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
    }
}

impl Env {
    /// Runs `text` in one coordinated transaction; with `crash`, the
    /// coordinator stops at that step and is dropped (a dead gateway).
    /// `Ok(Some(mode))` committed, `Ok(None)` crashed.
    async fn run(
        &self,
        dtids: &Arc<DtidGen>,
        text: &str,
        crash: Option<Point>,
    ) -> Result<Option<TxnMode>> {
        let mut t =
            TxnCoordinator::new(Arc::new(self.backend.clone()), dtids.clone(), TENANT, cfg());
        if let Some(p) = crash {
            t.set_hooks(Arc::new(Hold(p, std::sync::atomic::AtomicBool::new(true))));
        }
        t.begin(BeginOptions::default())?;
        let r = {
            let mut cc = Coord { t: &mut t };
            let labels = self.dict.labels.lock().unwrap().clone();
            let types = self.dict.types.lock().unwrap().clone();
            let r = {
                let mut store = MariaStore::new(
                    &mut cc,
                    Arc::clone(&self.layout),
                    labels,
                    types,
                    self.dict.clone(),
                    self.counter.clone(),
                );
                execute(&mut store, text, &Params::new(), &Budgets::default()).await
            };
            match r {
                Ok(o) => check_deleted(&mut cc, &self.layout, &o.deleted_nodes).await,
                Err(e) => Err(e),
            }
        };
        if let Err(e) = r {
            let _ = t.rollback().await;
            return Err(e);
        }
        match tokio::time::timeout(Duration::from_secs(5), t.commit()).await {
            Ok(r) => Ok(Some(r?.mode)),
            Err(_) => Ok(None),
        }
    }

    /// Every edge id and every mirror id, over both clusters.
    async fn edges_and_mirrors(&self) -> (BTreeSet<u64>, BTreeSet<u64>) {
        let (mut e, mut m) = (BTreeSet::new(), BTreeSet::new());
        for (n, sh) in self.layout.shards.iter().enumerate() {
            let url = &self.urls[usize::from(n >= 2)];
            for r in q(url, &format!("SELECT id FROM {}", sh.tables.edges)).await {
                e.insert(r.get::<u64, _>(0).unwrap());
            }
            for r in q(url, &format!("SELECT id FROM {}", sh.tables.mirrors)).await {
                m.insert(r.get::<u64, _>(0).unwrap());
            }
        }
        (e, m)
    }

    /// Redo statement rows (or `done` markers) still left.
    async fn redo_left(&self) -> usize {
        let mut n = 0;
        for c in &self.ids {
            n += self
                .backend
                .admin()
                .redo_branches(c)
                .await
                .unwrap()
                .iter()
                .filter(|r| r.statements > 0 || r.mark == Some(sprig_txn::redo::RedoMark::Done))
                .count();
        }
        n
    }

    /// The resolver's redo pass until nothing is left (20 s).
    async fn recover(&self) -> Duration {
        let t = Instant::now();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        while t.elapsed() < Duration::from_secs(20) {
            for c in &self.ids {
                let _ = self.resolver.scan_redo(c).await;
            }
            if self.redo_left().await == 0 {
                return t.elapsed();
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        panic!("redo rows still left after 20 s");
    }

    async fn teardown(self) {
        for (n, sh) in self.layout.shards.iter().enumerate() {
            q(
                &self.urls[usize::from(n >= 2)],
                &format!("DROP DATABASE IF EXISTS `{}`", sh.schema),
            )
            .await;
        }
        q(&self.urls[0], "DROP DATABASE IF EXISTS r8five_2pc_cat").await;
        self.pool0.disconnect().await.unwrap();
    }
}

/// `(a:P {k: x})`'s shard: which cluster an edge from it is written on.
fn link(x: i64, y: i64, n: i64) -> String {
    format!("MATCH (a:P {{k: {x}}}), (b:P {{k: {y}}}) CREATE (a)-[:L {{n: {n}}}]->(b)")
}

#[tokio::test(flavor = "multi_thread")]
async fn graph_writes_under_sprig_2pc() {
    let Some(env) = setup().await else {
        eprintln!("skipped: SPRIG_TEST_GRAPH_2PC is not set");
        return;
    };
    let dtids = Arc::new(DtidGen::new(5577));
    // Nodes 0..39, spread over the four shards (by id).
    env.run(&dtids, "UNWIND range(0, 39) AS k CREATE (:P {k: k})", None)
        .await
        .unwrap();
    // Which nodes live on which cluster.
    let mut on: BTreeMap<i64, usize> = BTreeMap::new();
    for (n, sh) in env.layout.shards.iter().enumerate() {
        let url = &env.urls[usize::from(n >= 2)];
        for r in q(
            url,
            &format!(
                "SELECT CAST(JSON_VALUE(props, '$.k') AS SIGNED) FROM {}",
                sh.tables.nodes
            ),
        )
        .await
        {
            on.insert(r.get::<i64, _>(0).unwrap(), usize::from(n >= 2));
        }
    }
    let a: Vec<i64> = on.iter().filter(|x| *x.1 == 0).map(|x| *x.0).collect();
    let b: Vec<i64> = on.iter().filter(|x| *x.1 == 1).map(|x| *x.0).collect();
    assert!(a.len() >= 5 && b.len() >= 5, "{a:?} {b:?}");
    // 1. A plain commit across the clusters: edge and mirror, no redo.
    let t0 = Instant::now();
    let mode = env.run(&dtids, &link(a[0], b[0], 1), None).await.unwrap();
    let commit_ms = t0.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(mode, Some(TxnMode::Sprig2pc));
    let (e, m) = env.edges_and_mirrors().await;
    assert_eq!((e.len(), m.len()), (1, 1));
    assert_eq!(e, m);
    assert_eq!(env.redo_left().await, 0);
    // 2. The coordinator dies after prepare: the resolver aborts.
    let r = env
        .run(&dtids, &link(a[1], b[1], 2), Some(Point::Prepared))
        .await
        .unwrap();
    assert_eq!(r, None, "held at Prepared");
    let took_abort = env.recover().await;
    let (e, m) = env.edges_and_mirrors().await;
    assert_eq!((e.len(), m.len()), (1, 1), "no partial commit");
    // 3. The coordinator dies after the commit point: the last resource
    //    committed, the held branch is replayed from its redo rows.
    let r = env
        .run(&dtids, &link(a[2], b[2], 3), Some(Point::CommitPoint))
        .await
        .unwrap();
    assert_eq!(r, None, "held at CommitPoint");
    let (e, m) = env.edges_and_mirrors().await;
    assert_eq!(e.len() + m.len(), 3, "one side committed: {e:?} {m:?}");
    let took_replay = env.recover().await;
    let (e, m) = env.edges_and_mirrors().await;
    assert_eq!(e, m, "replayed");
    assert_eq!(e.len(), 2);
    // 4. Four sessions, edges both ways between the clusters, one in five
    //    coordinators dying at a random step; then recovery.
    let points = [
        Point::BeforePrepare,
        Point::Prepared,
        Point::BeforeCommitPoint,
        Point::CommitPoint,
        Point::Phase2Branch,
    ];
    let env = Arc::new(env);
    let mut tasks = Vec::new();
    for s in 0..4i64 {
        let env = Arc::clone(&env);
        let dtids = dtids.clone();
        let (a, b) = (a.clone(), b.clone());
        tasks.push(tokio::spawn(async move {
            let (mut ok, mut crashed, mut failed) = (0, 0, 0);
            for i in 0..30i64 {
                let (x, y) = if (s + i) % 2 == 0 {
                    (
                        a[usize::try_from(i).unwrap() % a.len()],
                        b[usize::try_from(s).unwrap() % b.len()],
                    )
                } else {
                    (
                        b[usize::try_from(i).unwrap() % b.len()],
                        a[usize::try_from(s).unwrap() % a.len()],
                    )
                };
                let crash = ((s * 1000 + i) % 5 == 0)
                    .then(|| points[usize::try_from((s + i) % 5).unwrap()]);
                match env.run(&dtids, &link(x, y, 100 + s * 100 + i), crash).await {
                    Ok(Some(_)) => ok += 1,
                    Ok(None) => crashed += 1,
                    Err(e) => {
                        eprintln!("session {s} txn {i}: {e}");
                        failed += 1;
                    }
                }
            }
            (ok, crashed, failed)
        }));
    }
    let (mut ok, mut crashed, mut failed) = (0, 0, 0);
    for t in tasks {
        let (a, c, f) = t.await.unwrap();
        ok += a;
        crashed += c;
        failed += f;
    }
    let took = env.recover().await;
    let (e, m) = env.edges_and_mirrors().await;
    println!(
        "graph Sprig 2PC: plain cross-cluster CREATE {commit_ms:.1} ms; crash after prepare aborted in \
         {took_abort:?}; crash after the commit point replayed in {took_replay:?}; 120 concurrent: {ok} \
         committed, {crashed} coordinator crashes, {failed} failed; recovered in {took:?}; {} edges, {} \
         mirrors",
        e.len(),
        m.len()
    );
    assert_eq!(e, m, "every edge has its mirror and every mirror its edge");
    assert!(ok >= 80, "{ok} committed");
    let env = Arc::try_unwrap(env).ok().unwrap();
    for c in &env.ids {
        let _ = env.resolver.sweep(c).await;
    }
    env.teardown().await;
}
