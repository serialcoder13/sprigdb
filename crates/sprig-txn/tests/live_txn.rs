//! Live tests of distributed transactions on the dev clusters.
//!
//! ```sh
//! SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301 \
//! SPRIG_TEST_MARIADB_REPLICATION=mysql://root:sprig-dev-root@127.0.0.1:13311,mysql://root:sprig-dev-root@127.0.0.1:13312,mysql://root:sprig-dev-root@127.0.0.1:13313 \
//! SPRIG_TEST_MARIADB_GALERA=mysql://root:sprig-dev-root@127.0.0.1:13321 \
//! SPRIG_TEST_MARIADB_GALERA2=mysql://root:sprig-dev-root@127.0.0.1:13391 \
//! cargo test -p sprig-txn --test live_txn -- --nocapture --test-threads=1
//! ```
//!
//! `SPRIG_TEST_MARIADB_GALERA2` is a second, separate Galera cluster (the
//! dev Compose file has one): a throwaway single-node Galera container.
//!
//! Crash tests run the coordinator in a child process (this binary, test
//! `child_coordinator`), which prints `SPRIG_AT <point> <dtid>` when it
//! reaches the chosen step and waits; the parent kills it with SIGKILL,
//! so the backend connections drop as in a real crash, and then runs the
//! resolver.
//!
//! Each test creates schemas named `r1txn_<test>_<n>` and drops exactly
//! those names at the start and the end.

#![allow(
    clippy::many_single_char_names,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::missing_panics_doc
)]

use std::collections::BTreeMap;
use std::io::{BufRead, Write as _};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Opts, Pool};
use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId};
use sprig_txn::decision::create_table_sql;
use sprig_txn::mysql::{MyBackend, MyCluster};
use sprig_txn::{
    BeginOptions, ClusterCaps, Decision, Dtid, DtidGen, Intent, NonXaPolicy, Point, Resolver,
    ResolverConfig, ShardRef, StaticLocator, TxnBackend, TxnConfig, TxnCoordinator, TxnHooks,
    TxnMode,
};

const TENANT: TenantId = TenantId(9_180_001);
const GALERA2: &str = "SPRIG_TEST_MARIADB_GALERA2";
const CHILD: &str = "SPRIG_TXN_CHILD";

/// One test cluster: an id, its writer, its replicas, and the schema the
/// test uses there.
#[derive(Clone)]
struct Live {
    id: ClusterId,
    writer: String,
    replicas: Vec<String>,
    schema: String,
    shard: u32,
}

impl Live {
    fn shard_ref(&self) -> ShardRef {
        ShardRef {
            db: DbId(1),
            group: GroupId(1),
            shard: ShardId(self.shard),
            hash_point: u64::from(self.shard) << 40,
            schema: self.schema.clone(),
        }
    }
}

fn urls(var: &str) -> Option<Vec<String>> {
    let v = std::env::var(var).ok()?;
    let u: Vec<String> = v
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    (!u.is_empty()).then_some(u)
}

/// The clusters a test names: `single`, `repl`, `galera`, `galera2`.
fn clusters(test: &str, names: &[&str]) -> Option<Vec<Live>> {
    let mut out = Vec::new();
    for (i, n) in names.iter().enumerate() {
        let var = match *n {
            "single" => "SPRIG_TEST_MARIADB_SINGLE",
            "repl" => "SPRIG_TEST_MARIADB_REPLICATION",
            "galera" => "SPRIG_TEST_MARIADB_GALERA",
            "galera2" => GALERA2,
            _ => unreachable!(),
        };
        let Some(u) = urls(var) else {
            eprintln!("skipped {test}: set {var} to run it");
            return None;
        };
        out.push(Live {
            id: ClusterId::new(format!("dev-{n}")).unwrap(),
            writer: u[0].clone(),
            replicas: if *n == "repl" {
                u[1..].to_vec()
            } else {
                Vec::new()
            },
            schema: format!("r1txn_{test}_{}", i + 1),
            shard: u32::try_from(i).unwrap() + 1,
        });
    }
    Some(out)
}

fn pool(url: &str) -> Pool {
    let opts = mysql_async::OptsBuilder::from_opts(Opts::from_url(url).unwrap()).pool_opts(
        mysql_async::PoolOpts::default()
            .with_constraints(mysql_async::PoolConstraints::new(0, 2).unwrap())
            .with_inactive_connection_ttl(Duration::from_secs(30)),
    );
    Pool::new(opts)
}

async fn q(url: &str, sql: &str) -> Vec<mysql_async::Row> {
    let p = pool(url);
    let mut c = p.get_conn().await.unwrap();
    let r = c.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    drop(c);
    p.disconnect().await.unwrap();
    r
}

/// The capability probe's XA check, reduced: `XA START` works (1235 on Galera).
async fn probe_xa(url: &str) -> bool {
    let mut c = mysql_async::Conn::new(Opts::from_url(url).unwrap())
        .await
        .unwrap();
    let ok = match c.query_drop("XA START 'r1txn-probe'").await {
        Ok(()) => {
            c.query_drop("XA END 'r1txn-probe'; XA ROLLBACK 'r1txn-probe'")
                .await
                .unwrap();
            true
        }
        Err(mysql_async::Error::Server(e)) if e.code == 1235 => false,
        Err(e) => panic!("XA probe on {url}: {e}"),
    };
    c.disconnect().await.unwrap();
    ok
}

/// Drops and creates the test's schemas (exact names), with `t` and
/// `__sprig_dtx`. Prepared branches this test's tenant left behind (an
/// interrupted earlier run) are drained first: they hold locks a `DROP`
/// would wait for.
async fn setup(cs: &[Live]) {
    let b = backend(cs).await;
    let r = resolver(&b, cs);
    for c in cs {
        let d = tokio::time::Instant::now() + Duration::from_secs(10);
        let rep = r.drain_prepared(&c.id, TENANT, d).await.unwrap();
        if rep.committed + rep.rolled_back > 0 {
            println!("drained leftovers on {}: {rep:?}", c.id);
        }
    }
    for c in cs {
        q(
            &c.writer,
            &format!("DROP DATABASE IF EXISTS `{}`", c.schema),
        )
        .await;
        q(&c.writer, &format!("CREATE DATABASE `{}`", c.schema)).await;
        q(
            &c.writer,
            &format!(
                "CREATE TABLE `{}`.t (k BIGINT PRIMARY KEY, v VARCHAR(64)) ENGINE=InnoDB",
                c.schema
            ),
        )
        .await;
        q(&c.writer, &create_table_sql(&c.schema)).await;
    }
}

async fn clear(cs: &[Live]) {
    for c in cs {
        q(&c.writer, &format!("DELETE FROM `{}`.t", c.schema)).await;
    }
}

async fn teardown(cs: &[Live]) {
    for c in cs {
        q(
            &c.writer,
            &format!("DROP DATABASE IF EXISTS `{}`", c.schema),
        )
        .await;
    }
}

async fn backend(cs: &[Live]) -> MyBackend {
    let mut m = BTreeMap::new();
    for c in cs {
        let opts = Opts::from_url(&c.writer).unwrap();
        m.insert(
            c.id.clone(),
            MyCluster {
                opts: opts.clone(),
                admin_opts: opts,
                caps: ClusterCaps {
                    xa: probe_xa(&c.writer).await,
                },
            },
        );
    }
    MyBackend::new(m)
}

fn locator(cs: &[Live]) -> Arc<StaticLocator> {
    let l = Arc::new(StaticLocator::new());
    for c in cs {
        l.insert(TENANT, c.shard_ref().anchor(), c.id.clone(), &c.schema);
    }
    l
}

async fn rows(url: &str, schema: &str) -> Vec<(i64, String)> {
    q(url, &format!("SELECT k, v FROM `{schema}`.t ORDER BY k"))
        .await
        .into_iter()
        .map(|r| (r.get(0).unwrap(), r.get(1).unwrap()))
        .collect()
}

fn cfg(policy: NonXaPolicy, all_xa: bool) -> TxnConfig {
    TxnConfig {
        policy,
        tenant_all_xa: all_xa,
        coordinator_id: "r1txn-live".into(),
        ..TxnConfig::default()
    }
}

/// Writes `(k, v)` in every cluster of `cs`, in order, inside `txn`.
async fn write_all(txn: &mut TxnCoordinator<MyBackend>, cs: &[Live], k: i64, v: &str) {
    for c in cs {
        let b = txn
            .branch(&c.id, &c.shard_ref(), Intent::Write)
            .await
            .unwrap();
        b.conn_mut()
            .exec(&format!(
                "INSERT INTO `{}`.t (k, v) VALUES ({k}, '{v}')",
                c.schema
            ))
            .await
            .unwrap();
    }
}

fn resolver(b: &MyBackend, cs: &[Live]) -> Resolver {
    Resolver::new(
        b.admin(),
        locator(cs),
        ResolverConfig {
            interval: Duration::from_secs(1),
            min_age: Duration::from_secs(1),
            abort_retention: Duration::from_secs(24 * 3600),
            id: "resolver".into(),
        },
    )
}

/// Waits until `f` holds, up to 15 s.
async fn eventually<F, Fut>(what: &str, f: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(15) {
        if f().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("timed out waiting for {what}");
}

// ----------------------------------------------------------------------
// The child coordinator.

struct StopAt(Point);

#[async_trait::async_trait]
impl TxnHooks for StopAt {
    async fn at(&self, dtid: Option<Dtid>, p: Point) {
        if p == self.0 {
            println!(
                "SPRIG_AT {p:?} {}",
                dtid.map(|d| d.to_string()).unwrap_or_default()
            );
            std::io::stdout().flush().unwrap();
            std::future::pending::<()>().await;
        }
    }
}

fn point_of(s: &str) -> Point {
    match s {
        "Prepared" => Point::Prepared,
        "PendingRecorded" => Point::PendingRecorded,
        "CommitPoint" => Point::CommitPoint,
        "BeforeCommitPoint" => Point::BeforeCommitPoint,
        _ => panic!("unknown point {s}"),
    }
}

/// Runs only as the child: `SPRIG_TXN_CHILD=<test>:<names>:<all_xa>:<point>`.
#[tokio::test(flavor = "multi_thread")]
async fn child_coordinator() {
    let Ok(spec) = std::env::var(CHILD) else {
        return;
    };
    let p: Vec<&str> = spec.split(':').collect();
    let names: Vec<&str> = p[1].split(',').collect();
    let cs = clusters(p[0], &names).unwrap();
    let b = Arc::new(backend(&cs).await);
    let mut txn = TxnCoordinator::new(
        b,
        Arc::new(DtidGen::new(u64::from(std::process::id()))),
        TENANT,
        cfg(NonXaPolicy::Reject, p[2] == "1"),
    );
    txn.set_hooks(Arc::new(StopAt(point_of(p[3]))));
    txn.begin(BeginOptions::default()).unwrap();
    write_all(&mut txn, &cs, 1, "child").await;
    let r = txn.commit().await;
    panic!("the child should have been killed before commit returned: {r:?}");
}

/// Runs the child until it reaches `point`, kills it with SIGKILL, and
/// returns the dtid and how long the kill took.
fn run_child_and_kill(test: &str, names: &str, all_xa: bool, point: &str) -> Dtid {
    let exe = std::env::current_exe().unwrap();
    let mut child = std::process::Command::new(exe)
        .args([
            "--exact",
            "child_coordinator",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(
            CHILD,
            format!("{test}:{names}:{}:{point}", u8::from(all_xa)),
        )
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .unwrap();
    let out = child.stdout.take().unwrap();
    let mut dtid = None;
    for line in std::io::BufReader::new(out).lines() {
        let line = line.unwrap();
        // libtest prints "test child_coordinator ... " before it.
        if let Some((_, rest)) = line.split_once("SPRIG_AT ") {
            let (_, d) = rest.split_once(' ').unwrap();
            dtid = Some(d.parse::<Dtid>().unwrap());
            break;
        }
    }
    child.kill().unwrap(); // SIGKILL
    let status = child.wait().unwrap();
    println!("child coordinator killed at {point} ({status})");
    dtid.expect("the child never reached the point")
}

async fn prepared_on(b: &MyBackend, c: &Live, dtid: Dtid) -> bool {
    b.admin()
        .xa_recover(&c.id)
        .await
        .unwrap()
        .iter()
        .any(|x| x.gtrid.dtid == dtid)
}

/// Runs resolver scans until no branch of `dtid` is prepared on `cs`;
/// returns how long it took.
async fn resolve(b: &MyBackend, r: &Resolver, cs: &[Live], dtid: Dtid) -> Duration {
    let t = Instant::now();
    loop {
        for c in cs {
            let rep = r.scan(&c.id).await.unwrap();
            if rep.committed + rep.rolled_back > 0 {
                println!("resolver on {}: {rep:?}", c.id);
            }
        }
        let mut left = false;
        for c in cs {
            left |= prepared_on(b, c, dtid).await;
        }
        if !left {
            return t.elapsed();
        }
        assert!(
            t.elapsed() < Duration::from_secs(30),
            "resolver did not finish {dtid}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

async fn replicas_have(c: &Live, want: usize) {
    for r in &c.replicas {
        let schema = c.schema.clone();
        let r2 = r.clone();
        eventually(&format!("replica {r} to show {want} rows"), move || {
            let (r, s) = (r2.clone(), schema.clone());
            async move { rows(&r, &s).await.len() == want }
        })
        .await;
        let left = q(r, "XA RECOVER FORMAT='SQL'").await;
        let ours = left
            .iter()
            .filter(|row| row.get::<i64, _>(0) == Some(1_397_772_871))
            .count();
        assert_eq!(
            ours, 0,
            "replica {r} still holds a prepared coordinator branch"
        );
    }
}

// ----------------------------------------------------------------------
// The tests.

/// XA across a single node and a primary with two semi-sync replicas,
/// both XA-capable: the coordinator is killed after the commit point (the
/// home committed, the primary's branch is prepared), and the resolver
/// commits the prepared branch; then killed after the prepare (before the
/// commit point), and the resolver rolls it back.
#[tokio::test(flavor = "multi_thread")]
async fn xa_across_two_clusters_resolver_finishes_after_coordinator_kill() {
    let test = "xakill";
    let Some(cs) = clusters(test, &["single", "repl"]) else {
        return;
    };
    setup(&cs).await;
    let b = backend(&cs).await;
    assert!(b.caps(&cs[0].id).xa && b.caps(&cs[1].id).xa);
    let r = resolver(&b, &cs);

    // 1. Killed after the commit point: the resolver must commit.
    let dtid = run_child_and_kill(test, "single,repl", true, "CommitPoint");
    assert_eq!(
        rows(&cs[0].writer, &cs[0].schema).await.len(),
        1,
        "home committed"
    );
    assert!(
        prepared_on(&b, &cs[1], dtid).await,
        "the primary's branch is in doubt"
    );
    assert!(rows(&cs[1].writer, &cs[1].schema).await.is_empty());
    let d = b
        .admin()
        .read_decision(&cs[0].id, &cs[0].schema, dtid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.outcome, Decision::Commit);
    let took = resolve(&b, &r, &cs, dtid).await;
    println!("resolver committed the in-doubt branch {took:?} after the kill was seen");
    assert_eq!(
        rows(&cs[1].writer, &cs[1].schema).await,
        vec![(1, "child".to_owned())]
    );
    replicas_have(&cs[1], 1).await;
    // The commit row goes once its participants finished.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let sw = r.sweep(&cs[0].id).await.unwrap();
    assert_eq!(sw.commit_deleted, 1, "{sw:?}");

    // 2. Killed after the prepare, before the commit point: roll back.
    clear(&cs).await;
    let dtid = run_child_and_kill(test, "single,repl", true, "Prepared");
    assert!(prepared_on(&b, &cs[1], dtid).await);
    let took = resolve(&b, &r, &cs, dtid).await;
    println!("resolver rolled back the in-doubt branch in {took:?}");
    assert!(rows(&cs[0].writer, &cs[0].schema).await.is_empty());
    assert!(rows(&cs[1].writer, &cs[1].schema).await.is_empty());
    let d = b
        .admin()
        .read_decision(&cs[0].id, &cs[0].schema, dtid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.outcome, Decision::Abort);
    assert_eq!(d.coordinator, "resolver");
    replicas_have(&cs[1], 0).await;

    // 3. A normal commit, for the timing.
    let mut txn = TxnCoordinator::new(
        Arc::new(b.clone()),
        Arc::new(DtidGen::new(7)),
        TENANT,
        cfg(NonXaPolicy::Reject, true),
    );
    let t = Instant::now();
    txn.begin(BeginOptions::default()).unwrap();
    write_all(&mut txn, &cs, 2, "live").await;
    let o = txn.commit().await.unwrap();
    println!(
        "XA 2PC commit across single + semi-sync primary: {:?}, {:?}",
        o,
        t.elapsed()
    );
    assert_eq!(o.mode, TxnMode::Xa2pc);
    assert_eq!(o.stragglers, 0);
    teardown(&cs).await;
}

/// `reject` with two separate Galera clusters (no XA): the second
/// cluster's write fails with 39440 before it runs; `check_writes` refuses
/// an autocommit statement over both; `best_effort` commits both, and
/// reports 39442 when the second commit fails after the first.
#[tokio::test(flavor = "multi_thread")]
async fn reject_refuses_writes_spanning_two_galera_clusters() {
    let test = "reject";
    let Some(cs) = clusters(test, &["galera", "galera2"]) else {
        return;
    };
    setup(&cs).await;
    let b = Arc::new(backend(&cs).await);
    assert!(
        !b.caps(&cs[0].id).xa && !b.caps(&cs[1].id).xa,
        "Galera has no XA"
    );
    let ids: Vec<ClusterId> = cs.iter().map(|c| c.id.clone()).collect();

    let mut txn = TxnCoordinator::new(
        b.clone(),
        Arc::new(DtidGen::new(11)),
        TENANT,
        cfg(NonXaPolicy::Reject, false),
    );
    let e = txn.check_writes(&ids).unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39440, "{e}");
    println!("autocommit over both: {e}");
    txn.begin(BeginOptions::default()).unwrap();
    write_all(&mut txn, &cs[..1], 1, "first").await;
    let e = txn
        .branch(&cs[1].id, &cs[1].shard_ref(), Intent::Write)
        .await
        .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39440);
    println!("second Galera write: {e}");
    assert_eq!(
        txn.branches().len(),
        1,
        "no branch opened on the second cluster"
    );
    // Reads of the second cluster are still allowed.
    txn.branch(&cs[1].id, &cs[1].shard_ref(), Intent::Read)
        .await
        .unwrap();
    txn.rollback().await.unwrap();
    assert!(rows(&cs[0].writer, &cs[0].schema).await.is_empty());
    assert!(rows(&cs[1].writer, &cs[1].schema).await.is_empty());

    // best_effort: both commit, one at a time.
    let mut txn = TxnCoordinator::new(
        b.clone(),
        Arc::new(DtidGen::new(12)),
        TENANT,
        cfg(NonXaPolicy::BestEffort, false),
    );
    txn.begin(BeginOptions::default()).unwrap();
    write_all(&mut txn, &cs, 2, "both").await;
    let o = txn.commit().await.unwrap();
    assert_eq!(o.mode, TxnMode::BestEffort);
    assert_eq!(rows(&cs[0].writer, &cs[0].schema).await.len(), 1);
    assert_eq!(rows(&cs[1].writer, &cs[1].schema).await.len(), 1);

    // best_effort, second commit fails: 39442 names the first cluster.
    txn.begin(BeginOptions::default()).unwrap();
    write_all(&mut txn, &cs, 3, "partial").await;
    let thread = sprig_txn::BranchConn::thread_id(
        txn.conns_mut(std::slice::from_ref(&cs[1].id))
            .pop()
            .unwrap()
            .1,
    );
    b.admin().kill(&cs[1].id, thread).await.unwrap();
    let e = txn.commit().await.unwrap_err();
    println!("best effort with a failed second commit: {e}");
    assert_eq!(e.protocol().mysql_errno, 39442);
    assert!(e.message.contains("only clusters dev-galera ("), "{e}");
    assert!(e.message.contains("on dev-galera2"), "{e}");
    assert_eq!(rows(&cs[0].writer, &cs[0].schema).await.len(), 2);
    assert_eq!(rows(&cs[1].writer, &cs[1].schema).await.len(), 1);
    teardown(&cs).await;
}

/// Last-resource commit: a single node and a replicated primary (XA) with
/// one Galera cluster (no XA) as the last resource. Commits atomically;
/// the decision row is in the Galera schema and the home holds a pending
/// pointer. Then the coordinator is killed after the pending row (before
/// Galera commits): the resolver follows the pointer, records abort in the
/// Galera schema and rolls back both XA branches. Then killed after the
/// commit point: the resolver commits them.
#[tokio::test(flavor = "multi_thread")]
async fn last_resource_commit_with_one_galera_participant() {
    let test = "lrc";
    let names = "single,galera,repl";
    let Some(cs) = clusters(test, &["single", "galera", "repl"]) else {
        return;
    };
    setup(&cs).await;
    let b = Arc::new(backend(&cs).await);
    assert!(b.caps(&cs[0].id).xa && !b.caps(&cs[1].id).xa && b.caps(&cs[2].id).xa);
    let r = resolver(&b, &cs);

    let mut txn = TxnCoordinator::new(
        b.clone(),
        Arc::new(DtidGen::new(21)),
        TENANT,
        cfg(NonXaPolicy::Reject, false),
    );
    let t = Instant::now();
    txn.begin(BeginOptions::default()).unwrap();
    write_all(&mut txn, &cs, 1, "lrc").await;
    let dtid = txn.dtid().unwrap();
    let o = txn.commit().await.unwrap();
    println!(
        "last-resource commit (Galera last): {o:?} in {:?}",
        t.elapsed()
    );
    assert_eq!(o.mode, TxnMode::Xa2pc);
    for c in &cs {
        assert_eq!(
            rows(&c.writer, &c.schema).await,
            vec![(1, "lrc".to_owned())]
        );
    }
    let lr = b
        .admin()
        .read_decision(&cs[1].id, &cs[1].schema, dtid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lr.outcome, Decision::Commit);
    assert_eq!(lr.participants.len(), 3);
    let home = b
        .admin()
        .read_decision(&cs[0].id, &cs[0].schema, dtid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(home.outcome, Decision::Pending);
    assert_eq!(home.decider, Some(cs[1].shard_ref().anchor()));
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let s0 = r.sweep(&cs[0].id).await.unwrap();
    let s1 = r.sweep(&cs[1].id).await.unwrap();
    assert_eq!((s0.settled, s1.commit_deleted), (1, 1), "{s0:?} {s1:?}");

    // Killed after the pending row, before Galera's commit: abort.
    clear(&cs).await;
    let dtid = run_child_and_kill(test, names, false, "PendingRecorded");
    assert!(prepared_on(&b, &cs[0], dtid).await && prepared_on(&b, &cs[2], dtid).await);
    let took = resolve(&b, &r, &cs, dtid).await;
    println!("resolver rolled back after a kill before the Galera commit, in {took:?}");
    for c in &cs {
        assert!(
            rows(&c.writer, &c.schema).await.is_empty(),
            "{} rolled back",
            c.id
        );
    }
    let lr = b
        .admin()
        .read_decision(&cs[1].id, &cs[1].schema, dtid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        lr.outcome,
        Decision::Abort,
        "abort recorded where Galera would commit"
    );
    let home = b
        .admin()
        .read_decision(&cs[0].id, &cs[0].schema, dtid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(home.outcome, Decision::Abort, "pending pointer settled");

    // Killed after Galera committed: the resolver commits the XA branches.
    let dtid = run_child_and_kill(test, names, false, "CommitPoint");
    assert_eq!(
        rows(&cs[1].writer, &cs[1].schema).await.len(),
        1,
        "Galera committed"
    );
    let took = resolve(&b, &r, &cs, dtid).await;
    println!("resolver committed after a kill after the Galera commit, in {took:?}");
    for c in &cs {
        assert_eq!(
            rows(&c.writer, &c.schema).await,
            vec![(1, "child".to_owned())]
        );
    }
    replicas_have(&cs[2], 1).await;
    teardown(&cs).await;
}

/// Lookup sessions on a distributed transaction: Pre on the
/// Galera cluster commits first, Main on the single node and the primary
/// commits atomically through the coordinator, Post runs after.
#[tokio::test(flavor = "multi_thread")]
async fn lookup_sessions_commit_main_through_the_coordinator() {
    use sprig_shard::lookup::{LookupSessions, Target};
    use sprig_txn::lookup::DistTxn;

    let test = "lookup";
    let Some(cs) = clusters(test, &["single", "repl", "galera"]) else {
        return;
    };
    setup(&cs).await;
    q(
        &cs[0].writer,
        &format!("INSERT INTO `{}`.t VALUES (99, 'old')", cs[0].schema),
    )
    .await;
    let b = Arc::new(backend(&cs).await);
    let by_schema: BTreeMap<String, ShardRef> = cs
        .iter()
        .map(|c| (c.schema.clone(), c.shard_ref()))
        .collect();
    let anchor_of: sprig_txn::lookup::AnchorOf =
        Arc::new(move |t: &Target| by_schema[&t.schema].clone());
    let txn = TxnCoordinator::new(
        b.clone(),
        Arc::new(DtidGen::new(31)),
        TENANT,
        cfg(NonXaPolicy::Reject, false),
    );
    let mut d = DistTxn::new(txn, b.clone(), anchor_of, 5);
    let target = |c: &Live| Target {
        cluster: c.id.clone(),
        schema: c.schema.clone(),
    };
    d.pre(
        &target(&cs[2]),
        &format!("INSERT INTO `{}`.t VALUES (?, ?)", cs[2].schema),
        (100, "pre").into(),
    )
    .await
    .unwrap();
    for c in &cs[..2] {
        d.main(
            &target(c),
            &format!("INSERT INTO `{}`.t VALUES (?, ?)", c.schema),
            (1, "main").into(),
        )
        .await
        .unwrap();
    }
    d.post(
        &target(&cs[0]),
        format!("DELETE FROM `{}`.t WHERE k = ?", cs[0].schema),
        (99,).into(),
    );
    let (o, failures) = d.commit().await.unwrap();
    println!("lookup sessions on DistTxn: {o:?}, post failures {failures}");
    assert_eq!(o.mode, TxnMode::Xa2pc);
    assert_eq!(
        rows(&cs[0].writer, &cs[0].schema).await,
        vec![(1, "main".to_owned())]
    );
    assert_eq!(
        rows(&cs[1].writer, &cs[1].schema).await,
        vec![(1, "main".to_owned())]
    );
    assert_eq!(
        rows(&cs[2].writer, &cs[2].schema).await,
        vec![(100, "pre".to_owned())]
    );
    teardown(&cs).await;
}

/// Operators and the mover on in-doubt transactions: after
/// a coordinator is killed past the commit point, `indoubt::list` and
/// `show` report the prepared branch with its `commit` decision, and the
/// mover's `drain_prepared` commits it at once (no `min_age` wait). After
/// a kill before the commit point, the list reports no decision, the
/// operator's `resolve` rolls it back through the insert race, and the
/// transaction is no longer in doubt (39447).
#[tokio::test(flavor = "multi_thread")]
async fn operator_lists_and_resolves_and_the_mover_drains() {
    use sprig_common::SprigCode;
    use sprig_txn::indoubt;
    let test = "r1closeop";
    let Some(cs) = clusters(test, &["single", "repl"]) else {
        return;
    };
    setup(&cs).await;
    let b = backend(&cs).await;
    let r = resolver(&b, &cs);
    let loc = locator(&cs);
    let ids: Vec<ClusterId> = cs.iter().map(|c| c.id.clone()).collect();

    // 1. Past the commit point: listed as `commit`; the drain commits it.
    let dtid = run_child_and_kill(test, "single,repl", true, "CommitPoint");
    let l = indoubt::list(&*b.admin(), &*loc, &ids, Some(TENANT)).await;
    let t = l
        .txns
        .iter()
        .find(|t| t.gtrid.dtid == dtid)
        .expect("listed");
    assert_eq!(t.decision.as_str(), "commit", "{t:?}");
    assert_eq!(t.branches.len(), 1);
    assert_eq!(t.branches[0].0, cs[1].id);
    let shown = indoubt::show(&*b.admin(), &*loc, &ids, dtid).await.unwrap();
    assert_eq!(
        shown.home_at,
        Some((cs[0].id.clone(), cs[0].schema.clone()))
    );
    let started = Instant::now();
    let rep = r
        .drain_prepared(
            &cs[1].id,
            TENANT,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    println!("drain: {rep:?} in {:?}", started.elapsed());
    assert_eq!((rep.committed, rep.remaining), (1, 0));
    assert_eq!(
        rows(&cs[1].writer, &cs[1].schema).await,
        vec![(1, "child".to_owned())]
    );

    // 2. Before the commit point: no decision; `resolve` rolls it back.
    clear(&cs).await;
    let dtid = run_child_and_kill(test, "single,repl", true, "Prepared");
    let shown = indoubt::show(&*b.admin(), &*loc, &ids, dtid).await.unwrap();
    assert_eq!(shown.decision.as_str(), "none", "{shown:?}");
    let done = r.resolve(&ids, dtid).await.unwrap();
    println!("resolve: {done:?}");
    assert_eq!(done.rolled_back.len(), 1);
    assert!(done.committed.is_empty() && done.unresolved.is_empty());
    assert!(rows(&cs[1].writer, &cs[1].schema).await.is_empty());
    let e = indoubt::show(&*b.admin(), &*loc, &ids, dtid)
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::TxnNotInDoubt));
    teardown(&cs).await;
}
