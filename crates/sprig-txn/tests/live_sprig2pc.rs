//! Sprig 2PC live: two separate single-node Galera
//! clusters with `wsrep_on` (MariaDB refuses `XA START` there with 1235),
//! policy `sprig_2pc`.
//!
//! ```sh
//! SPRIG_TEST_SPRIG2PC=mysql://root:sprig-dev-root@127.0.0.1:47511,mysql://root:sprig-dev-root@127.0.0.1:47512 \
//! SPRIG_TEST_SPRIG2PC_RESTART=r3b-g2 \
//! cargo test -p sprig-txn --test live_sprig2pc -- --nocapture --test-threads=1
//! ```
//!
//! `SPRIG_TEST_SPRIG2PC_RESTART` names the Docker container of the second
//! cluster: the participant-crash case restarts it between the commit
//! point and phase 2. A coordinator crash is the coordinator dropped while
//! a hook holds it at a step: its connections close, so MariaDB rolls its
//! open transactions back, as for a killed gateway. Each test uses schemas
//! `t9180077_d1_g1_s<n>` and drops exactly those.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::missing_panics_doc,
    clippy::many_single_char_names
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Opts, Pool};
use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId};
use sprig_txn::decision::create_table_sql;
use sprig_txn::mysql::{MyBackend, MyCluster};
use sprig_txn::{
    BeginOptions, ClusterCaps, Dtid, DtidGen, Intent, NonXaPolicy, Point, Resolver, ResolverConfig,
    ShardRef, StaticLocator, TxnBackend, TxnConfig, TxnCoordinator, TxnHooks, TxnMode,
};
use tokio::sync::Notify;

const TENANT: TenantId = TenantId(9_180_077);

#[derive(Clone)]
struct Live {
    id: ClusterId,
    url: String,
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

fn clusters() -> Option<Vec<Live>> {
    let v = std::env::var("SPRIG_TEST_SPRIG2PC").ok()?;
    let urls: Vec<&str> = v.split(',').map(str::trim).collect();
    if urls.len() < 2 {
        return None;
    }
    Some(
        urls.iter()
            .enumerate()
            .map(|(i, u)| Live {
                id: ClusterId::new(format!("r3b-g{}", i + 1)).unwrap(),
                url: (*u).to_owned(),
                schema: format!("t{TENANT}_d1_g1_s{}", i + 1),
                shard: u32::try_from(i + 1).unwrap(),
            })
            .collect(),
    )
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

async fn xa_refused(url: &str) -> bool {
    let mut c = mysql_async::Conn::new(Opts::from_url(url).unwrap())
        .await
        .unwrap();
    let r = c.query_drop("XA START 'r3b-probe'").await;
    let refused = matches!(&r, Err(mysql_async::Error::Server(e)) if e.code == 1235);
    if r.is_ok() {
        let _ = c
            .query_drop("XA END 'r3b-probe'; XA ROLLBACK 'r3b-probe'")
            .await;
    }
    c.disconnect().await.unwrap();
    refused
}

async fn setup(cs: &[Live]) {
    for c in cs {
        assert!(xa_refused(&c.url).await, "{} must refuse XA (Galera)", c.id);
        q(&c.url, &format!("DROP DATABASE IF EXISTS `{}`", c.schema)).await;
        q(&c.url, &format!("CREATE DATABASE `{}`", c.schema)).await;
        q(
            &c.url,
            &format!(
                "CREATE TABLE `{}`.t (k BIGINT PRIMARY KEY, v VARCHAR(64), \
                 at DATETIME(6) NULL) ENGINE=InnoDB",
                c.schema
            ),
        )
        .await;
        q(
            &c.url,
            &format!(
                "CREATE TABLE `{}`.acct (id INT PRIMARY KEY, bal BIGINT NOT NULL) ENGINE=InnoDB",
                c.schema
            ),
        )
        .await;
        q(
            &c.url,
            &format!("INSERT INTO `{}`.acct VALUES (1, 1000000)", c.schema),
        )
        .await;
        q(&c.url, &create_table_sql(&c.schema)).await;
    }
}

async fn teardown(cs: &[Live]) {
    for c in cs {
        q(&c.url, &format!("DROP DATABASE IF EXISTS `{}`", c.schema)).await;
    }
}

fn backend(cs: &[Live]) -> MyBackend {
    let mut m = BTreeMap::new();
    for c in cs {
        let opts = Opts::from_url(&c.url).unwrap();
        m.insert(
            c.id.clone(),
            MyCluster {
                opts: opts.clone(),
                admin_opts: opts,
                caps: ClusterCaps { xa: false },
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

fn cfg(policy: NonXaPolicy) -> TxnConfig {
    TxnConfig {
        policy,
        coordinator_id: "r3b-live".into(),
        ..TxnConfig::default()
    }
}

/// Holds the coordinator at `point` (once) until released.
struct Hold {
    point: Point,
    reached: Notify,
    release: Notify,
    armed: std::sync::atomic::AtomicBool,
}

impl Hold {
    fn new(point: Point) -> Arc<Self> {
        Arc::new(Self {
            point,
            reached: Notify::new(),
            release: Notify::new(),
            armed: std::sync::atomic::AtomicBool::new(true),
        })
    }
}

#[async_trait::async_trait]
impl TxnHooks for Hold {
    async fn at(&self, _dtid: Option<Dtid>, p: Point) {
        if p == self.point && self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
            self.reached.notify_one();
            self.release.notified().await;
        }
    }
}

async fn keys(c: &Live) -> Vec<i64> {
    q(
        &c.url,
        &format!("SELECT k FROM `{}`.t ORDER BY k", c.schema),
    )
    .await
    .into_iter()
    .map(|r| r.get(0).unwrap())
    .collect()
}

async fn redo_left(b: &MyBackend, cs: &[Live]) -> usize {
    let mut n = 0;
    for c in cs {
        n += b
            .admin()
            .redo_branches(&c.id)
            .await
            .unwrap()
            .iter()
            .filter(|r| r.statements > 0 || r.mark == Some(sprig_txn::redo::RedoMark::Done))
            .count();
    }
    n
}

/// Runs the resolver's redo pass until no statement rows are left (15 s).
async fn recover(b: &MyBackend, r: &Resolver, cs: &[Live]) -> Duration {
    let t = Instant::now();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    while t.elapsed() < Duration::from_secs(15) {
        for c in cs {
            let _ = r.scan_redo(&c.id).await;
        }
        if redo_left(b, cs).await == 0 {
            return t.elapsed();
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    panic!("redo rows still left after 15 s");
}

/// One transaction writing `(k, v)` on every cluster, in order.
async fn write_all(txn: &mut TxnCoordinator<MyBackend>, cs: &[Live], k: i64, v: &str) {
    for c in cs {
        let b = txn
            .branch(&c.id, &c.shard_ref(), Intent::Write)
            .await
            .unwrap();
        b.conn_mut()
            .exec(&format!(
                "INSERT INTO `{}`.t (k, v, at) VALUES ({k}, '{v}', NOW(6))",
                c.schema
            ))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sprig_2pc_on_two_galera_clusters() {
    let Some(cs) = clusters() else {
        eprintln!("live_sprig2pc: set SPRIG_TEST_SPRIG2PC to run");
        return;
    };
    setup(&cs).await;
    let b = backend(&cs);
    let r = resolver(&b, &cs);
    let dtids = Arc::new(DtidGen::new(77));
    let new_txn =
        |policy| TxnCoordinator::new(Arc::new(b.clone()), dtids.clone(), TENANT, cfg(policy));

    // 1. Policies: reject refuses the second Galera writer; best effort
    //    commits one after the other; sprig_2pc holds the second branch.
    let mut t = new_txn(NonXaPolicy::Reject);
    t.begin(BeginOptions::default()).unwrap();
    t.branch(&cs[0].id, &cs[0].shard_ref(), Intent::Write)
        .await
        .unwrap();
    let e = t
        .branch(&cs[1].id, &cs[1].shard_ref(), Intent::Write)
        .await
        .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39440, "{e}");
    t.rollback().await.unwrap();
    let mut t = new_txn(NonXaPolicy::BestEffort);
    t.begin(BeginOptions::default()).unwrap();
    write_all(&mut t, &cs, 1, "best").await;
    assert_eq!(t.commit().await.unwrap().mode, TxnMode::BestEffort);

    // 2. A plain commit: both rows, the branch's clock fixed (NOW() is the
    //    same on replay), no redo left.
    let started = Instant::now();
    let mut t = new_txn(NonXaPolicy::Sprig2pc);
    t.begin(BeginOptions::default()).unwrap();
    write_all(&mut t, &cs, 2, "ok").await;
    let o = t.commit().await.unwrap();
    assert_eq!(o.mode, TxnMode::Sprig2pc);
    assert_eq!(o.stragglers, 0);
    let commit_ms = started.elapsed().as_secs_f64() * 1000.0;
    for c in &cs {
        assert!(keys(c).await.contains(&2), "{}", c.id);
    }
    assert_eq!(redo_left(&b, &cs).await, 0);

    // 3. Coordinator crash after prepare (redo written, no decision): the
    //    resolver decides abort and drops the redo; no row anywhere.
    let mut t = new_txn(NonXaPolicy::Sprig2pc);
    let hold = Hold::new(Point::Prepared);
    t.set_hooks(hold.clone());
    t.begin(BeginOptions::default()).unwrap();
    write_all(&mut t, &cs, 3, "crash-prepared").await;
    let done = tokio::time::timeout(Duration::from_secs(3), t.commit()).await;
    assert!(done.is_err(), "held at Prepared");
    drop(t);
    let took = recover(&b, &r, &cs).await;
    for c in &cs {
        assert!(!keys(c).await.contains(&3), "{}: no partial commit", c.id);
    }
    println!("crash after prepare: aborted by the resolver in {took:?}");

    // 4. Coordinator crash after the commit point: the last resource
    //    committed, the held branch's transaction died with the
    //    coordinator; the resolver replays it from its redo rows.
    let mut t = new_txn(NonXaPolicy::Sprig2pc);
    let hold = Hold::new(Point::CommitPoint);
    t.set_hooks(hold.clone());
    t.begin(BeginOptions::default()).unwrap();
    write_all(&mut t, &cs, 4, "crash-commit").await;
    let done = tokio::time::timeout(Duration::from_secs(3), t.commit()).await;
    assert!(done.is_err(), "held at CommitPoint");
    drop(t);
    assert!(
        keys(&cs[0]).await.contains(&4),
        "the last resource committed"
    );
    assert!(!keys(&cs[1]).await.contains(&4), "the held branch was lost");
    let took = recover(&b, &r, &cs).await;
    for c in &cs {
        assert!(keys(c).await.contains(&4), "{}: replayed", c.id);
    }
    let at: Vec<Option<String>> = {
        let mut v = Vec::new();
        for c in &cs {
            let row = q(
                &c.url,
                &format!("SELECT CAST(at AS CHAR) FROM `{}`.t WHERE k = 4", c.schema),
            )
            .await;
            v.push(row[0].get(0).unwrap());
        }
        v
    };
    println!("crash after the commit point: replayed in {took:?}; NOW() {at:?}");

    // 5. Participant crash: the held branch's node restarts between the
    //    commit point and phase 2; phase 2 fails, the commit still
    //    answers OK (the decision is commit), and the resolver replays.
    if let Ok(container) = std::env::var("SPRIG_TEST_SPRIG2PC_RESTART") {
        let mut t = new_txn(NonXaPolicy::Sprig2pc);
        let hold = Hold::new(Point::CommitPoint);
        t.set_hooks(hold.clone());
        t.begin(BeginOptions::default()).unwrap();
        write_all(&mut t, &cs, 5, "node-restart").await;
        let h2 = hold.clone();
        let commit = tokio::spawn(async move {
            let r = t.commit().await;
            drop(t);
            r
        });
        h2.reached.notified().await;
        let st = std::process::Command::new("docker")
            .args(["restart", "-t", "0", &container])
            .status()
            .unwrap();
        assert!(st.success());
        // Back up.
        let t0 = Instant::now();
        loop {
            if let Ok(mut c) = mysql_async::Conn::new(Opts::from_url(&cs[1].url).unwrap()).await
                && c.query_drop("SELECT 1").await.is_ok()
            {
                let _ = c.disconnect().await;
                break;
            }
            assert!(
                t0.elapsed() < Duration::from_secs(60),
                "the node didn't come back"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        hold.release.notify_one();
        let o = commit.await.unwrap().unwrap();
        assert_eq!(o.stragglers, 1, "phase 2 on the restarted node failed");
        let took = recover(&b, &r, &cs).await;
        for c in &cs {
            assert!(
                keys(c).await.contains(&5),
                "{}: replayed after the restart",
                c.id
            );
        }
        println!("participant restart between commit point and phase 2: replayed in {took:?}");
    }

    // 6. A statement that doesn't replay is refused at commit (39446).
    let mut t = new_txn(NonXaPolicy::Sprig2pc);
    t.begin(BeginOptions::default()).unwrap();
    write_all(&mut t, &cs[..1], 6, "x").await;
    let br = t
        .branch(&cs[1].id, &cs[1].shard_ref(), Intent::Write)
        .await
        .unwrap();
    br.conn_mut()
        .exec(&format!(
            "INSERT INTO `{}`.t (k, v) VALUES (6, UUID())",
            cs[1].schema
        ))
        .await
        .unwrap();
    let e = t.commit().await.unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39446, "{e}");
    for c in &cs {
        assert!(!keys(c).await.contains(&6));
    }

    // 7. Many transfers between the clusters' accounts from four sessions,
    //    a fifth of them crashing at a random step; after recovery every
    //    transaction is whole: the balances still sum to the start.
    let points = [
        Point::BeforePrepare,
        Point::Prepared,
        Point::BeforeCommitPoint,
        Point::CommitPoint,
        Point::Phase2Branch,
    ];
    let mut tasks = Vec::new();
    let n_per = 40;
    for s in 0..4u64 {
        let b2 = b.clone();
        let cs2 = cs.clone();
        let dt = dtids.clone();
        tasks.push(tokio::spawn(async move {
            let (mut ok, mut crashed, mut failed) = (0, 0, 0);
            for i in 0..n_per {
                let mut t = TxnCoordinator::new(
                    Arc::new(b2.clone()),
                    dt.clone(),
                    TENANT,
                    cfg(NonXaPolicy::Sprig2pc),
                );
                let crash = (s * 1000 + i) % 5 == 0;
                if crash {
                    t.set_hooks(Hold::new(points[usize::try_from((s + i) % 5).unwrap()]));
                }
                t.begin(BeginOptions::default()).unwrap();
                let amt = 1 + (s * 7 + i) % 13;
                let mut err = None;
                for (j, c) in cs2.iter().enumerate() {
                    let sign = if j == 0 { "-" } else { "+" };
                    let br = match t.branch(&c.id, &c.shard_ref(), Intent::Write).await {
                        Ok(b) => b,
                        Err(e) => {
                            err = Some(e.to_string());
                            break;
                        }
                    };
                    if let Err(e) = br
                        .conn_mut()
                        .exec(&format!(
                            "UPDATE `{}`.acct SET bal = bal {sign} {amt} WHERE id = 1",
                            c.schema
                        ))
                        .await
                    {
                        let _ = t.statement_failed(&c.id, &e).await;
                        err = Some(e.to_string());
                        break;
                    }
                }
                if err.is_some() {
                    if t.in_txn() {
                        t.rollback().await.unwrap();
                    }
                    failed += 1;
                    continue;
                }
                match tokio::time::timeout(Duration::from_secs(5), t.commit()).await {
                    Ok(Ok(_)) => ok += 1,
                    Ok(Err(_)) => failed += 1,
                    Err(_) => crashed += 1,
                }
                drop(t);
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
    let took = recover(&b, &r, &cs).await;
    let mut sum = 0i64;
    for c in &cs {
        let row = q(
            &c.url,
            &format!("SELECT bal FROM `{}`.acct WHERE id = 1", c.schema),
        )
        .await;
        let v: i64 = row[0].get(0).unwrap();
        sum += v;
    }
    println!(
        "transfers: {ok} committed, {crashed} coordinator crashes, {failed} failed or \
         rolled back; recovered in {took:?}; balances sum {sum} (start 2000000)"
    );
    assert_eq!(sum, 2_000_000, "a transfer committed on one cluster only");
    // The sweep leaves no commit row behind once its redo is gone.
    for c in &cs {
        let _ = r.sweep(&c.id).await;
    }
    println!("plain Sprig 2PC commit of two inserts: {commit_ms:.1} ms");
    teardown(&cs).await;
}
