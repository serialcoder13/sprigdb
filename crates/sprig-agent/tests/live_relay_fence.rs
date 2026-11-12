//! Sprig 2PC's relay fence, live (chapter 18, "The relay fence"; decisions
//! Q28, Q36 and Q37): two single-node Galera clusters (`wsrep_on`, no XA),
//! the agent's relay (`sprig_agent::relay`) in front of the second, and
//! the coordinator writing both under `sprig_2pc`.
//!
//! ```sh
//! SPRIG_TEST_RELAY_FENCE=mysql://root:sprig-dev-root@127.0.0.1:47611,mysql://root:sprig-dev-root@127.0.0.1:47612 \
//! cargo test -p sprig-agent --test live_relay_fence -- --nocapture --test-threads=1
//! ```
//!
//! A gateway's connection is a `mysql_async` connection through a shim
//! that writes the relay's preamble first, as the gateway's client does.
//! A gateway crash is the coordinator dropped while a hook holds it after
//! the commit point; a relay crash is the relay's stop token cancelled,
//! which closes every relayed socket at once. Each test uses schemas
//! `t47611000000_d1_g1_s<n>` and drops exactly those.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::missing_panics_doc,
    clippy::similar_names,
    clippy::many_single_char_names
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Pool};
use sprig_agent::relay::{Relay, RelayRegistry, preamble};
use sprig_common::CancellationToken;
use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId};
use sprig_txn::decision::create_table_sql;
use sprig_txn::fence::RelayFence;
use sprig_txn::mysql::{MyAdmin, MyBackend, MyCluster, MyConn};
use sprig_txn::{
    BeginOptions, ClusterAdmin, ClusterCaps, DbResult, Dtid, DtidGen, Gtrid, Intent, NonXaPolicy,
    Point, RedoBranch, RedoStmt, Replay, Resolver, ResolverConfig, ShardRef, StaticLocator,
    TxnBackend, TxnConfig, TxnCoordinator, TxnHooks, Xid,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

const TENANT: TenantId = TenantId(47_611_000_000);

#[derive(Clone)]
struct Live {
    id: ClusterId,
    url: String,
    addr: String,
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

    fn t(&self) -> String {
        format!("`{}`.t", self.schema)
    }
}

fn clusters() -> Option<Vec<Live>> {
    let v = std::env::var("SPRIG_TEST_RELAY_FENCE").ok()?;
    let urls: Vec<&str> = v.split(',').map(str::trim).collect();
    if urls.len() < 2 {
        return None;
    }
    Some(
        urls.iter()
            .enumerate()
            .map(|(i, u)| {
                let o = Opts::from_url(u).unwrap();
                Live {
                    id: ClusterId::new(format!("r3final-g{}", i + 1)).unwrap(),
                    url: (*u).to_owned(),
                    addr: format!("{}:{}", o.ip_or_hostname(), o.tcp_port()),
                    schema: format!("t{}_d1_g1_s{}", TENANT.0, i + 1),
                    shard: u32::try_from(i + 1).unwrap(),
                }
            })
            .collect(),
    )
}

fn pool(url: &str) -> Pool {
    let opts = OptsBuilder::from_opts(Opts::from_url(url).unwrap()).pool_opts(
        mysql_async::PoolOpts::default()
            .with_constraints(mysql_async::PoolConstraints::new(0, 4).unwrap())
            .with_inactive_connection_ttl(Duration::from_secs(30)),
    );
    Pool::new(opts)
}

async fn q(url: &str, sql: &str) -> Vec<mysql_async::Row> {
    let mut c = mysql_async::Conn::new(Opts::from_url(url).unwrap())
        .await
        .unwrap();
    let r = c.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    c.disconnect().await.unwrap();
    r
}

async fn hot(c: &Live, k: i64) -> String {
    let r = q(&c.url, &format!("SELECT v FROM {} WHERE k = {k}", c.t())).await;
    r.first().map(|r| r.get(0).unwrap()).unwrap_or_default()
}

async fn setup(cs: &[Live]) {
    for c in cs {
        q(&c.url, &format!("DROP DATABASE IF EXISTS `{}`", c.schema)).await;
        q(&c.url, &format!("CREATE DATABASE `{}`", c.schema)).await;
        q(
            &c.url,
            &format!(
                "CREATE TABLE {} (k BIGINT PRIMARY KEY, v VARCHAR(4000)) ENGINE=InnoDB",
                c.t()
            ),
        )
        .await;
        q(
            &c.url,
            &format!("INSERT INTO {} VALUES (0, '0'), (1, '1')", c.t()),
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

/// The gateway's side of the wire: writes the preamble, then copies.
/// Cancelling the token cuts every connection at once, as a gateway's
/// death does (no `COM_QUIT`).
async fn shim(relay: String) -> (String, CancellationToken) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let cut = CancellationToken::new();
    let c2 = cut.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut c, _)) = l.accept().await else {
                return;
            };
            let relay = relay.clone();
            let cut = c2.clone();
            tokio::spawn(async move {
                let Ok(mut r) = TcpStream::connect(&relay).await else {
                    return;
                };
                if tokio::io::AsyncWriteExt::write_all(&mut r, &preamble(TENANT))
                    .await
                    .is_err()
                {
                    return;
                }
                tokio::select! {
                    () = cut.cancelled() => {}
                    _ = tokio::io::copy_bidirectional(&mut c, &mut r) => {}
                }
            });
        }
    });
    (addr, cut)
}

/// Fails a test that runs longer than two minutes.
async fn within<F: std::future::Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(120), f)
        .await
        .expect("the test took more than two minutes")
}

fn url_at(url: &str, addr: &str) -> String {
    let o = Opts::from_url(url).unwrap();
    format!(
        "mysql://{}:{}@{addr}",
        o.user().unwrap_or("root"),
        o.pass().unwrap_or("")
    )
}

fn rcfg() -> ResolverConfig {
    ResolverConfig {
        interval: Duration::from_millis(200),
        min_age: Duration::from_secs(1),
        abort_retention: Duration::from_secs(24 * 3600),
        id: "agent".into(),
    }
}

fn locator(cs: &[Live]) -> Arc<StaticLocator> {
    let l = Arc::new(StaticLocator::new());
    for c in cs {
        l.insert(TENANT, c.shard_ref().anchor(), c.id.clone(), &c.schema);
    }
    l
}

fn direct_admin(cs: &[Live]) -> Arc<MyAdmin> {
    let mut m = BTreeMap::new();
    for c in cs {
        let o = Opts::from_url(&c.url).unwrap();
        m.insert(
            c.id.clone(),
            MyCluster {
                opts: o.clone(),
                admin_opts: o,
                caps: ClusterCaps { xa: false },
            },
        );
    }
    MyBackend::new(m).my_admin()
}

/// One relay in front of `cs[1]`.
struct RelayRun {
    relay: Arc<Relay>,
    addr: String,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

async fn start_relay(cs: &[Live], addr: Option<String>) -> RelayRun {
    let g2 = &cs[1];
    let registry = Arc::new(RelayRegistry::new(pool(&g2.url)));
    let fence = Arc::new(RelayFence::new(
        g2.id.clone(),
        registry.clone(),
        direct_admin(cs),
        locator(cs),
        rcfg(),
    ));
    let relay = Arc::new(Relay::new(fence, registry, g2.addr.clone()));
    let l = TcpListener::bind(addr.as_deref().unwrap_or("127.0.0.1:0"))
        .await
        .unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let stop = CancellationToken::new();
    let task = tokio::spawn(Arc::clone(&relay).serve(l, stop.clone()));
    // The start-up recovery runs first.
    let t = Instant::now();
    while !relay.fence().ready() {
        assert!(t.elapsed() < Duration::from_secs(20), "relay never ready");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    RelayRun {
        relay,
        addr,
        stop,
        task,
    }
}

/// The gateway's admin: held-branch calls for the fenced cluster go to
/// its agent's fence (in process here; the agent's internal service in a
/// deployment).
struct FenceAdmin {
    inner: Arc<MyAdmin>,
    fenced: ClusterId,
    relay: Arc<Relay>,
}

#[async_trait::async_trait]
impl ClusterAdmin for FenceAdmin {
    async fn xa_recover(&self, c: &ClusterId) -> DbResult<Vec<Xid>> {
        self.inner.xa_recover(c).await
    }
    async fn xa_finish(&self, c: &ClusterId, x: &Xid, commit: bool) -> DbResult<()> {
        self.inner.xa_finish(c, x, commit).await
    }
    async fn kill(&self, c: &ClusterId, t: u64) -> DbResult<()> {
        self.inner.kill(c, t).await
    }
    async fn insert_decision(
        &self,
        c: &ClusterId,
        s: &str,
        r: &sprig_txn::DecisionRow,
    ) -> DbResult<()> {
        self.inner.insert_decision(c, s, r).await
    }
    async fn read_decision(
        &self,
        c: &ClusterId,
        s: &str,
        d: Dtid,
    ) -> DbResult<Option<sprig_txn::DecisionRow>> {
        self.inner.read_decision(c, s, d).await
    }
    async fn settle_pending(
        &self,
        c: &ClusterId,
        s: &str,
        d: Dtid,
        o: sprig_txn::Decision,
    ) -> DbResult<()> {
        self.inner.settle_pending(c, s, d, o).await
    }
    async fn decision_rows(
        &self,
        c: &ClusterId,
    ) -> DbResult<Vec<(String, sprig_txn::DecisionRow)>> {
        self.inner.decision_rows(c).await
    }
    async fn delete_decision(&self, c: &ClusterId, s: &str, d: Dtid) -> DbResult<()> {
        self.inner.delete_decision(c, s, d).await
    }
    async fn insert_redo(
        &self,
        c: &ClusterId,
        s: &str,
        d: Dtid,
        g: &Gtrid,
        st: &[RedoStmt],
    ) -> DbResult<()> {
        self.inner.insert_redo(c, s, d, g, st).await
    }
    async fn insert_redo_held(
        &self,
        c: &ClusterId,
        s: &str,
        d: Dtid,
        g: &Gtrid,
        st: &[RedoStmt],
        holder: u64,
    ) -> DbResult<()> {
        if *c == self.fenced {
            return self
                .relay
                .fence()
                .prepare(TENANT, holder, s, d, g, st)
                .await;
        }
        self.inner.insert_redo(c, s, d, g, st).await
    }
    async fn ensure_redo(&self, c: &ClusterId, s: &str) -> DbResult<()> {
        self.inner.ensure_redo(c, s).await
    }
    async fn redo_branches(&self, c: &ClusterId) -> DbResult<Vec<RedoBranch>> {
        self.inner.redo_branches(c).await
    }
    async fn replay_redo(&self, c: &ClusterId, s: &str, d: Dtid) -> DbResult<Replay> {
        self.inner.replay_redo(c, s, d).await
    }
    async fn delete_redo(&self, c: &ClusterId, s: &str, d: Dtid) -> DbResult<()> {
        if *c == self.fenced {
            return self.relay.fence().finished(s, d).await;
        }
        self.inner.delete_redo(c, s, d).await
    }
    async fn abandon_held(&self, c: &ClusterId, _s: &str, d: Dtid, holder: u64) -> DbResult<()> {
        if *c == self.fenced {
            self.relay.fence().abandon(d, holder);
        }
        Ok(())
    }
}

/// The gateway's backend: `cs[1]` through the relay (via the shim).
struct GwBackend {
    inner: MyBackend,
    admin: Arc<dyn ClusterAdmin>,
    fenced: Option<ClusterId>,
}

#[async_trait::async_trait]
impl TxnBackend for GwBackend {
    type Conn = MyConn;
    fn caps(&self, c: &ClusterId) -> ClusterCaps {
        self.inner.caps(c)
    }
    async fn checkout(&self, c: &ClusterId) -> DbResult<MyConn> {
        self.inner.checkout(c).await
    }
    fn admin(&self) -> Arc<dyn ClusterAdmin> {
        self.admin.clone()
    }
    fn relay_fenced(&self, c: &ClusterId) -> bool {
        self.fenced.as_ref() == Some(c)
    }
}

/// `relay`: the relayed address of `cs[1]` and its relay; `None`: direct
/// (Sprig 2PC as built in R3, no fence).
fn gateway(cs: &[Live], relay: Option<(&str, &Arc<Relay>)>) -> Arc<GwBackend> {
    let mut m = BTreeMap::new();
    for (i, c) in cs.iter().enumerate() {
        let direct = Opts::from_url(&c.url).unwrap();
        let opts = match (i, relay) {
            (1, Some((addr, _))) => Opts::from_url(&url_at(&c.url, addr)).unwrap(),
            _ => direct.clone(),
        };
        m.insert(
            c.id.clone(),
            MyCluster {
                opts,
                admin_opts: direct,
                caps: ClusterCaps { xa: false },
            },
        );
    }
    let inner = MyBackend::new(m);
    let admin: Arc<dyn ClusterAdmin> = match relay {
        Some((_, r)) => Arc::new(FenceAdmin {
            inner: inner.my_admin(),
            fenced: cs[1].id.clone(),
            relay: Arc::clone(r),
        }),
        None => inner.my_admin(),
    };
    Arc::new(GwBackend {
        inner,
        admin,
        fenced: relay.map(|_| cs[1].id.clone()),
    })
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

fn txn_cfg() -> TxnConfig {
    TxnConfig {
        policy: NonXaPolicy::Sprig2pc,
        coordinator_id: "r3final-live".into(),
        ..TxnConfig::default()
    }
}

/// Starts a transaction that writes row `k` on cs[0] (the home, the last
/// resource) and appends `tag` to hot row 0 of cs[1] (held, read then
/// written back as a literal), and holds it at `point`. Returns its task.
fn held_txn(
    b: Arc<GwBackend>,
    cs: Vec<Live>,
    k: i64,
    tag: &'static str,
    point: Point,
    seed: u64,
) -> (Arc<Hold>, tokio::task::JoinHandle<()>) {
    let hold = Hold::new(point);
    let h = hold.clone();
    let task = tokio::spawn(async move {
        let mut t = TxnCoordinator::new(b, Arc::new(DtidGen::new(seed)), TENANT, txn_cfg());
        t.set_hooks(h);
        t.begin(BeginOptions::default()).unwrap();
        let b0 = t
            .branch(&cs[0].id, &cs[0].shard_ref(), Intent::Write)
            .await
            .unwrap();
        b0.conn_mut()
            .exec(&format!("INSERT INTO {} VALUES ({k}, '{tag}')", cs[0].t()))
            .await
            .unwrap();
        let b1 = t
            .branch(&cs[1].id, &cs[1].shard_ref(), Intent::Write)
            .await
            .unwrap();
        let rows = b1
            .conn_mut()
            .query(&format!(
                "SELECT v FROM {} WHERE k = 0 FOR UPDATE",
                cs[1].t()
            ))
            .await
            .unwrap();
        let v: String = rows[0].get(0).unwrap();
        b1.conn_mut()
            .exec(&format!(
                "UPDATE {} SET v = '{v},{tag}' WHERE k = 0",
                cs[1].t()
            ))
            .await
            .unwrap();
        let _ = t.commit().await;
    });
    (hold, task)
}

/// Another writer's append to hot row 0 of `c`, read-modify-write in a
/// transaction, retried until it commits (or 30 s). Returns (attempts,
/// refusals with 39448).
async fn append_until_done(url: &str, c: &Live, tag: &str) -> (u32, u32) {
    let t0 = Instant::now();
    let (mut tries, mut fenced) = (0, 0);
    loop {
        tries += 1;
        let r: Result<(), mysql_async::Error> = async {
            let mut conn = mysql_async::Conn::new(Opts::from_url(url).unwrap()).await?;
            conn.query_drop("BEGIN").await?;
            let v: Option<String> = conn
                .query_first(format!("SELECT v FROM {} WHERE k = 0 FOR UPDATE", c.t()))
                .await?;
            conn.query_drop(format!(
                "UPDATE {} SET v = '{},{tag}' WHERE k = 0",
                c.t(),
                v.unwrap_or_default()
            ))
            .await?;
            conn.query_drop("COMMIT").await?;
            conn.disconnect().await?;
            Ok(())
        }
        .await;
        match r {
            Ok(()) => return (tries, fenced),
            Err(mysql_async::Error::Server(e)) if e.code == 39448 => fenced += 1,
            Err(_) => {}
        }
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "{tag} never committed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Without the fence (Sprig 2PC as built in R3): a gateway that dies after
/// the commit point releases its held rows; another writer's append lands
/// first; the resolver's replay then writes the held branch's value over
/// it. The other writer's committed append is lost: decision Q12's window.
#[tokio::test(flavor = "multi_thread")]
async fn without_the_fence_a_replay_overwrites_a_later_write() {
    let Some(cs) = clusters() else {
        eprintln!("live_relay_fence: set SPRIG_TEST_RELAY_FENCE to run");
        return;
    };
    setup(&cs).await;
    let gw = gateway(&cs, None);
    let (hold, task) = held_txn(gw, cs.clone(), 100, "T", Point::CommitPoint, 9101);
    within(hold.reached.notified()).await;
    // W waits for T's row lock.
    let w = {
        let (u, c) = (cs[1].url.clone(), cs[1].clone());
        tokio::spawn(async move { append_until_done(&u, &c, "W").await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    // The gateway dies: its connections close, the held branch rolls back.
    task.abort();
    let _ = task.await;
    within(w).await.unwrap();
    // The resolver replays T.
    let r = Resolver::new(direct_admin(&cs), locator(&cs), rcfg());
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let rep = r.scan_redo(&cs[1].id).await.unwrap();
    let v = hot(&cs[1], 0).await;
    println!("no fence: replayed {}, hot row = {v:?}", rep.replayed);
    teardown(&cs).await;
    assert_eq!(rep.replayed, 1, "{rep:?}");
    assert!(v.contains(",T"), "{v}");
    assert!(
        !v.contains(",W"),
        "expected the window: W's committed append overwritten by the replay; got {v}"
    );
}

/// With the fence: the holder's backend stays open when its gateway dies,
/// the relay ends the waiting writer, the agent replays the held branch,
/// and only then admits the tenant again: both appends survive.
#[tokio::test(flavor = "multi_thread")]
async fn the_fence_keeps_held_rows_until_they_are_replayed() {
    let Some(cs) = clusters() else {
        eprintln!("live_relay_fence: set SPRIG_TEST_RELAY_FENCE to run");
        return;
    };
    setup(&cs).await;
    let run = start_relay(&cs, None).await;
    let (shim_addr, gw_cut) = shim(run.addr.clone()).await;
    let (w_addr, w_cut) = shim(run.addr.clone()).await;
    let gw = gateway(&cs, Some((&shim_addr, &run.relay)));
    let w_url = url_at(&cs[1].url, &w_addr);
    let (hold, task) = held_txn(gw, cs.clone(), 200, "T", Point::CommitPoint, 9201);
    within(hold.reached.notified()).await;
    let w = {
        let (u, c) = (w_url.clone(), cs[1].clone());
        tokio::spawn(async move { append_until_done(&u, &c, "W").await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    // The gateway dies: its sockets are cut, then its tasks end.
    let died = Instant::now();
    gw_cut.cancel();
    task.abort();
    let _ = task.await;
    let (tries, refused) = within(w).await.unwrap();
    let reopened = died.elapsed();
    let st = run.relay.fence().stats();
    let v = hot(&cs[1], 0).await;
    let redo = direct_admin(&cs).redo_branches(&cs[1].id).await.unwrap();
    println!(
        "fence: {st:?}; W committed after {tries} attempts ({refused} refused with 39448), \
         {reopened:?} after the gateway died; hot row = {v:?}"
    );
    run.stop.cancel();
    let _ = run.task.await;
    w_cut.cancel();
    teardown(&cs).await;
    assert_eq!(st.fenced, 1, "{st:?}");
    assert_eq!(st.replayed, 1, "{st:?}");
    assert!(st.ended >= 2, "the waiter and the holder end: {st:?}");
    assert!(v.ends_with(",T,W"), "T replayed first, W after it: {v}");
    assert!(redo.iter().all(|b| b.statements == 0), "{redo:?}");
}

/// The relay itself dies (the agent process): every relayed socket closes,
/// so the held branch rolls back. A new relay recovers the node before it
/// admits anyone. Also measures what MariaDB does with an autocommit
/// statement that was waiting for the held rows when its client vanished
/// (decision Q36's residual): it runs on and commits.
#[tokio::test(flavor = "multi_thread")]
async fn a_relay_crash_recovers_before_admitting() {
    let Some(cs) = clusters() else {
        eprintln!("live_relay_fence: set SPRIG_TEST_RELAY_FENCE to run");
        return;
    };
    setup(&cs).await;
    let run = start_relay(&cs, None).await;
    let (shim_addr, shim_cut) = shim(run.addr.clone()).await;
    let gw = gateway(&cs, Some((&shim_addr, &run.relay)));
    let fenced_url = url_at(&cs[1].url, &shim_addr);
    let (hold, task) = held_txn(gw, cs.clone(), 300, "T", Point::CommitPoint, 9301);
    within(hold.reached.notified()).await;
    // An autocommit two-row write through the relay, waiting for row 0.
    let a = {
        let (u, t) = (fenced_url.clone(), cs[1].t());
        tokio::spawn(async move {
            let mut c = mysql_async::Conn::new(Opts::from_url(&u).unwrap())
                .await
                .unwrap();
            c.query_drop(format!(
                "UPDATE {t} SET v = CONCAT(v, ',A') WHERE k IN (1, 0) ORDER BY k DESC"
            ))
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    // The agent dies.
    let addr = run.addr.clone();
    run.stop.cancel();
    let _ = run.task.await;
    let a_result = within(a).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let row1_before = hot(&cs[1], 1).await;
    // It restarts on the same port: start-up recovery, then admission.
    let run2 = start_relay(&cs, Some(addr)).await;
    let st = run2.relay.fence().stats();
    let v0 = hot(&cs[1], 0).await;
    let (tries, refused) = within(append_until_done(&fenced_url, &cs[1], "W")).await;
    let v0_after = hot(&cs[1], 0).await;
    println!(
        "relay crash: the waiting autocommit write got {:?}; row 1 = {row1_before:?} \
         (',A' there: it committed after its client vanished); start-up recovery {st:?}; \
         hot row after recovery = {v0:?}; W after {tries} attempts ({refused} refused) -> {v0_after:?}",
        a_result.as_ref().map_err(ToString::to_string)
    );
    run2.stop.cancel();
    let _ = run2.task.await;
    shim_cut.cancel();
    task.abort();
    let _ = task.await;
    teardown(&cs).await;
    assert_eq!(st.replayed, 1, "{st:?}");
    assert!(v0.contains(",T"), "{v0}");
    assert!(v0_after.ends_with(",W"), "{v0_after}");
}

/// Decision Q37: phase 2 outruns `txn.phase2_wait_ms` (the held branch's
/// `COMMIT` waits behind `FLUSH TABLES WITH READ LOCK`); `COMMIT` answers
/// OK with a straggler. A plain read misses the row; the session's next
/// statement on that cluster settles the straggler first and sees it.
#[tokio::test(flavor = "multi_thread")]
async fn a_phase2_straggler_is_settled_before_the_sessions_next_read() {
    let Some(cs) = clusters() else {
        eprintln!("live_relay_fence: set SPRIG_TEST_RELAY_FENCE to run");
        return;
    };
    setup(&cs).await;
    let gw = gateway(&cs, None);
    let mut t = TxnCoordinator::new(
        gw,
        Arc::new(DtidGen::new(9401)),
        TENANT,
        TxnConfig {
            phase2_wait: Duration::from_millis(200),
            ..txn_cfg()
        },
    );
    let hold = Hold::new(Point::CommitPoint);
    t.set_hooks(hold.clone());
    // FTWRL on the held cluster once the decision is in.
    let locker = {
        let (hold, url) = (hold.clone(), cs[1].url.clone());
        tokio::spawn(async move {
            hold.reached.notified().await;
            let mut c = mysql_async::Conn::new(Opts::from_url(&url).unwrap())
                .await
                .unwrap();
            c.query_drop("FLUSH TABLES WITH READ LOCK").await.unwrap();
            hold.release.notify_one();
            tokio::time::sleep(Duration::from_millis(1500)).await;
            c.query_drop("UNLOCK TABLES").await.unwrap();
            c.disconnect().await.unwrap();
        })
    };
    t.begin(BeginOptions::default()).unwrap();
    for (i, c) in cs.iter().enumerate() {
        let b = t
            .branch(&c.id, &c.shard_ref(), Intent::Write)
            .await
            .unwrap();
        b.conn_mut()
            .exec(&format!("INSERT INTO {} VALUES ({}, 'S')", c.t(), 400 + i))
            .await
            .unwrap();
    }
    let started = Instant::now();
    let o = within(t.commit()).await.unwrap();
    let commit_took = started.elapsed();
    let plain = q(
        &cs[1].url,
        &format!("SELECT COUNT(*) FROM {} WHERE k = 401", cs[1].t()),
    )
    .await;
    let plain_seen: i64 = plain[0].get(0).unwrap();
    let settle_start = Instant::now();
    t.begin(BeginOptions::default()).unwrap();
    let b = t
        .branch(&cs[1].id, &cs[1].shard_ref(), Intent::Read)
        .await
        .unwrap();
    let rows = b
        .conn_mut()
        .query(&format!("SELECT COUNT(*) FROM {} WHERE k = 401", cs[1].t()))
        .await
        .unwrap();
    let settled_seen: i64 = rows[0].get(0).unwrap();
    let settle_took = settle_start.elapsed();
    t.commit().await.unwrap();
    locker.await.unwrap();
    println!(
        "phase 2: COMMIT answered in {commit_took:?} with {} straggler(s); a plain read saw \
         {plain_seen} row(s); the session's next read settled in {settle_took:?} and saw \
         {settled_seen}",
        o.stragglers
    );
    teardown(&cs).await;
    assert_eq!(o.stragglers, 1);
    assert_eq!(
        plain_seen, 0,
        "the window: a read that doesn't settle misses it"
    );
    assert_eq!(settled_seen, 1);
    assert_eq!(t.stragglers(), 0);
}
