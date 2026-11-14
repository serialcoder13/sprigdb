//! Distributed transactions under deterministic simulation: XA
//! transactions survive coordinator and participant crashes, with no
//! partial commits after recovery.
//!
//! The real coordinator and resolver run against the MariaDB model: a
//! single node `a` and a semi-sync primary `b` with one replica (both with
//! XA), and two Galera clusters `g1` and `g2` (no XA: the model refuses
//! `XA START` with 1235, as MariaDB does). Two gateway sessions run
//! transactions that insert one row per chosen cluster, sometimes also
//! update shared hot rows (lock waits and cross-cluster deadlocks). The
//! gateway is crashed at one coordinator step chosen by the seed (before
//! prepare, after prepare, after the pending row, before and after the
//! commit point, in phase 2, ...), and a random fault plan kills,
//! pauses, partitions and slows the gateway, the controller (resolver) and
//! every MariaDB writer. Then everything heals and the resolver runs.
//!
//! Invariants, checked on every seed:
//!
//! 1. Atomicity: every transaction's rows are on all of its clusters or on
//!    none (best effort may be partial only when the client got 39442, or
//!    no answer).
//! 2. The client's answer is true: OK means every row is there; a
//!    rollback error (39443, 1213, 1205, 39440, a client rollback) means
//!    none is.
//! 3. `reject`: no committed transaction wrote both Galera clusters.
//! 4. Nothing stays prepared: `XA RECOVER` is empty on every server once
//!    the resolver has run for `min_age` plus a few intervals.
//! 5. Decisions: no dtid has both a commit and an abort row; a commit row
//!    means the rows are there, an abort row that they are not.
//! 6. Cleanup: no commit row is left once its participants finished.
//!
//! Sprig 2PC: the same world with policy `sprig_2pc`,
//! mostly on three Galera clusters and nothing else (no XA anywhere), the
//! rest mixed with the XA clusters. Held branches record their writes,
//! prepare by writing redo rows, and commit by deleting them inside the
//! branch; the resolver replays committed branches whose transaction was
//! lost and drops aborted ones. The invariants above hold, and no redo
//! row is left after recovery.
//!
//! The relay fence: the same
//! Sprig 2PC world with an agent per Galera writer node (`g1a`, `g2a`,
//! `g3a`) whose relay carries every gateway connection to that node and
//! runs `sprig_txn::fence::RelayFence`, a second gateway `gw2` writing the
//! same hot rows, and statements that keep running after their client
//! went away (MariaDB's behaviour, `Model::set_orphans_run`). Hot rows are
//! read-modify-write list appends, so a held branch replayed over another
//! transaction's later write loses that write, which invariant 7 sees:
//!
//! 7. Isolation: every hot row's final list holds each committed append
//!    exactly once and nothing else.
//! 8. Read your writes: after `COMMIT` answered OK, the
//!    session's next read of each cluster it wrote sees its row.
//!
//! The `#[ignore]`d tests run without the fence, and with reads that skip
//! settling the session's phase-2 stragglers: they fail, which shows what
//! the fence and the settling fix.
//!
//! `SPRIG_SIM_SEEDS=10000 cargo test -p sprig-txn --test sim_txn` runs
//! 10,000 seeds; CI runs the default.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::missing_panics_doc,
    clippy::many_single_char_names,
    clippy::struct_excessive_bools
)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId, TxnId};
use sprig_common::time::Clock;
use sprig_testkit::faults::Node;
use sprig_testkit::sim::mariadb::{
    CLIENT_TIMEOUT, ConnId, Outcome, ServerConfig, SimConn, SimMariaDb, Stmt,
};
use sprig_testkit::sim::{FaultPlan, NodeCtx, Sim, SimNet, SimRng, World};
use sprig_txn::decision::{decode_participants, encode_participants};
use sprig_txn::fence::{Admission, ClientGone, FENCED_ERRNO, RelayFence, RelayLinks};
use sprig_txn::xid::{hex, unhex};
use sprig_txn::{
    AnchorRef, BeginOptions, BranchConn, ClusterAdmin, ClusterCaps, Ctl, DbError, DbResult,
    Decision, DecisionRow, Dtid, DtidGen, Intent, NonXaPolicy, Point, Resolver, ResolverConfig,
    ShardRef, StaticLocator, TxnBackend, TxnConfig, TxnCoordinator, TxnHooks, Xid,
};

const TENANT: TenantId = TenantId(1);
const DTX: &str = "__sprig_dtx";
const REDO: &str = "__sprig_redo";
/// The model keys rows by one integer, so the markers (`seq` = MARK in
/// MariaDB) live in a table of their own.
const REDO_MARK: &str = "__sprig_redo_mark";
const T: &str = "t";
const HOT: i64 = 1_000_000;
const HORIZON: Duration = Duration::from_secs(30);

/// Totals over every seed of the process, printed at the end.
#[derive(Default)]
struct Stats {
    runs: AtomicU64,
    txns: AtomicU64,
    committed: AtomicU64,
    xa_2pc: AtomicU64,
    best_effort: AtomicU64,
    rejected: AtomicU64,
    rolled_back: AtomicU64,
    unknown: AtomicU64,
    partial: AtomicU64,
    no_answer: AtomicU64,
    crashes: AtomicU64,
    resolver_commits: AtomicU64,
    resolver_rollbacks: AtomicU64,
    sprig_2pc: AtomicU64,
    redo_replays: AtomicU64,
    redo_drops: AtomicU64,
    agent_replays: AtomicU64,
    agent_drops: AtomicU64,
    fences: AtomicU64,
    fence_ended: AtomicU64,
    fence_refused: AtomicU64,
    stragglers: AtomicU64,
    ryw_reads: AtomicU64,
    settle_refused: AtomicU64,
    appends: AtomicU64,
}

static STATS: std::sync::LazyLock<Stats> = std::sync::LazyLock::new(Stats::default);

fn bump(c: &AtomicU64) {
    c.fetch_add(1, Ordering::Relaxed);
}

struct ClusterDef {
    id: &'static str,
    writer: &'static str,
    schema: &'static str,
    xa: bool,
}

const CLUSTERS: [ClusterDef; 5] = [
    ClusterDef {
        id: "a",
        writer: "a1",
        schema: "t1_d1_g1_s1",
        xa: true,
    },
    ClusterDef {
        id: "b",
        writer: "b1",
        schema: "t1_d1_g1_s2",
        xa: true,
    },
    ClusterDef {
        id: "g1",
        writer: "g1n1",
        schema: "t1_d1_g1_s3",
        xa: false,
    },
    ClusterDef {
        id: "g2",
        writer: "g2n1",
        schema: "t1_d1_g1_s4",
        xa: false,
    },
    ClusterDef {
        id: "g3",
        writer: "g3n1",
        schema: "t1_d1_g1_s5",
        xa: false,
    },
];

fn def(c: &ClusterId) -> &'static ClusterDef {
    CLUSTERS.iter().find(|d| d.id == c.as_str()).unwrap()
}

fn cid(i: usize) -> ClusterId {
    ClusterId::new(CLUSTERS[i].id).unwrap()
}

fn shard_ref(i: usize) -> ShardRef {
    ShardRef {
        db: DbId(1),
        group: GroupId(1),
        shard: ShardId(u32::try_from(i).unwrap() + 1),
        hash_point: (i as u64) << 60,
        schema: CLUSTERS[i].schema.to_owned(),
    }
}

/// The model's tables have `BIGINT` keys: incarnation and counter.
fn dkey(d: Dtid) -> i64 {
    let hi = i64::try_from(d.0 >> 64).unwrap();
    let lo = i64::try_from(d.0 & 0xffff_ffff).unwrap();
    (hi << 32) | lo
}

fn dtid_of(k: i64) -> Dtid {
    let k = u128::try_from(k).unwrap();
    TxnId(((k >> 32) << 64) | (k & 0xffff_ffff))
}

fn encode_row(row: &DecisionRow, now_ms: u64) -> String {
    format!(
        "{}|{}|{}|{}|{now_ms}",
        row.outcome.as_str(),
        row.decider.map(|a| hex(&a.to_bytes())).unwrap_or_default(),
        row.coordinator,
        hex(&encode_participants(&row.participants)),
    )
}

fn decode_row(dtid: Dtid, v: &str, now_ms: u64) -> Option<DecisionRow> {
    let p: Vec<&str> = v.split('|').collect();
    if p.len() != 5 {
        return None;
    }
    let created: u64 = p[4].parse().ok()?;
    Some(DecisionRow {
        dtid,
        outcome: Decision::parse(p[0])?,
        decider: if p[1].is_empty() {
            None
        } else {
            AnchorRef::from_bytes(&unhex(p[1])?)
        },
        coordinator: p[2].to_owned(),
        participants: decode_participants(&unhex(p[3])?),
        age_seconds: Some(now_ms.saturating_sub(created) / 1000),
    })
}

/// What every simulated component shares.
struct Env {
    db: SimMariaDb,
    origin: tokio::time::Instant,
    /// The relay of each cluster's writer node, on relay-fenced clusters.
    relays: Vec<Option<Arc<SimRelay>>>,
}

impl Env {
    fn relay(&self, c: &ClusterId) -> Option<&Arc<SimRelay>> {
        let i = CLUSTERS.iter().position(|d| d.id == c.as_str())?;
        self.relays[i].as_ref()
    }
}

// ----------------------------------------------------------------------
// The node agent's relay.

/// One relayed connection, as the agent holds it.
struct Link {
    conn: Arc<SimConn>,
    /// Its client (the gateway) is still connected.
    client: bool,
}

/// The relay's side of `RelayLinks`: its connections to the local node.
struct SimLinks {
    db: SimMariaDb,
    agent: Node,
    idx: usize,
    conns: Mutex<BTreeMap<u64, Link>>,
}

#[async_trait::async_trait]
impl RelayLinks for SimLinks {
    async fn end(&self, thread: u64) -> bool {
        let d = &CLUSTERS[self.idx];
        // KILL as sprig_ctl, on the node; then both sides close.
        if let Ok(c) = self.db.connect(&self.agent, d.writer, None, d.schema).await {
            let _ = c.exec(&Stmt::KillConnection(ConnId(thread))).await;
        }
        let link = self.conns.lock().unwrap().remove(&thread);
        drop(link);
        let until = tokio::time::Instant::now() + sprig_txn::fence::END_WAIT;
        loop {
            if !self.db.with_model(|m, _| m.is_alive(ConnId(thread))) {
                return true;
            }
            if tokio::time::Instant::now() >= until {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn backend_alive(&self, thread: u64) -> bool {
        self.db.with_model(|m, _| m.is_alive(ConnId(thread)))
    }
}

#[derive(Clone)]
struct RelayLive {
    inc: u64,
    fence: Arc<RelayFence<SimLinks>>,
    links: Arc<SimLinks>,
    /// Cancelled when this incarnation stops: work it was doing for a
    /// gateway dies with it.
    dead: sprig_common::CancellationToken,
}

/// A Galera writer node's agent relay: every gateway connection to the
/// node passes it, so it dies with the agent.
struct SimRelay {
    idx: usize,
    agent: Node,
    db: SimMariaDb,
    net: SimNet,
    world: World,
    live: Mutex<Option<RelayLive>>,
}

/// A gateway's connection through a relay.
struct RelayedConn {
    relay: Arc<SimRelay>,
    inc: u64,
    thread: u64,
    gw: Node,
}

impl Drop for RelayedConn {
    fn drop(&mut self) {
        self.relay.client_gone(self.inc, self.thread, &self.gw);
    }
}

/// Clears an agent incarnation's relay when it stops (a crash): every
/// relayed connection closes with it.
struct LiveGuard {
    relay: Arc<SimRelay>,
    inc: u64,
}

impl Drop for LiveGuard {
    fn drop(&mut self) {
        let gone = {
            let mut l = self.relay.live.lock().unwrap();
            if l.as_ref().is_some_and(|x| x.inc == self.inc) {
                l.take()
            } else {
                None
            }
        };
        if let Some(l) = gone {
            l.dead.cancel();
            let st = l.fence.stats();
            STATS.fences.fetch_add(st.fenced, Ordering::Relaxed);
            STATS.fence_ended.fetch_add(st.ended, Ordering::Relaxed);
            let conns = std::mem::take(&mut *l.links.conns.lock().unwrap());
            drop(conns);
        }
    }
}

impl SimRelay {
    fn current(&self) -> Option<RelayLive> {
        self.live.lock().unwrap().clone()
    }

    fn lost(started: tokio::time::Instant) -> impl std::future::Future<Output = ()> {
        tokio::time::sleep(CLIENT_TIMEOUT.saturating_sub(started.elapsed()))
    }

    async fn open(self: &Arc<Self>, gw: &Node) -> DbResult<RelayedConn> {
        let started = tokio::time::Instant::now();
        if self.net.transit(gw, &self.agent).await.is_err() {
            Self::lost(started).await;
            return Err(DbError::lost("relay unreachable"));
        }
        let Some(l) = self.current() else {
            return Err(DbError::lost("connection refused: the agent is down"));
        };
        if l.fence.admit(TENANT) == Admission::Fenced {
            bump(&STATS.fence_refused);
            return Err(DbError::new(FENCED_ERRNO, "fenced"));
        }
        let d = &CLUSTERS[self.idx];
        let conn = self
            .db
            .connect(&self.agent, d.writer, None, d.schema)
            .await
            .map_err(|e| DbError::new(e, "connect"))?;
        let thread = conn.id().0;
        if self.current().is_none_or(|x| x.inc != l.inc) {
            return Err(DbError::lost("the agent restarted"));
        }
        if !l.fence.opened(TENANT, thread) {
            bump(&STATS.fence_refused);
            return Err(DbError::new(FENCED_ERRNO, "fenced"));
        }
        l.links.conns.lock().unwrap().insert(
            thread,
            Link {
                conn: Arc::new(conn),
                client: true,
            },
        );
        let rc = RelayedConn {
            relay: self.clone(),
            inc: l.inc,
            thread,
            gw: gw.clone(),
        };
        if self.net.transit(&self.agent, gw).await.is_err() {
            drop(rc);
            Self::lost(started).await;
            return Err(DbError::lost("relay unreachable"));
        }
        Ok(rc)
    }

    async fn exec(&self, rc: &RelayedConn, s: &Stmt) -> Outcome {
        let started = tokio::time::Instant::now();
        if self.net.transit(&rc.gw, &self.agent).await.is_err() {
            Self::lost(started).await;
            return Outcome::Err(2013);
        }
        let conn = match self.current() {
            Some(l) if l.inc == rc.inc => l
                .links
                .conns
                .lock()
                .unwrap()
                .get(&rc.thread)
                .filter(|x| x.client)
                .map(|x| x.conn.clone()),
            _ => None,
        };
        // The relay died or ended the connection: reset.
        let Some(conn) = conn else {
            return Outcome::Err(2013);
        };
        let o = conn.exec(s).await;
        drop(conn);
        let alive = self.current().is_some_and(|l| {
            l.inc == rc.inc
                && l.links
                    .conns
                    .lock()
                    .unwrap()
                    .get(&rc.thread)
                    .is_some_and(|x| x.client)
        });
        if !alive {
            return Outcome::Err(2013);
        }
        if self.net.transit(&self.agent, &rc.gw).await.is_err() {
            Self::lost(started).await;
            return Outcome::Err(2013);
        }
        o
    }

    /// The gateway's side closed (its process ended, or it dropped the
    /// connection): the relay hears at once when the link is up, else
    /// after a keepalive period.
    fn client_gone(self: &Arc<Self>, inc: u64, thread: u64, gw: &Node) {
        if self.net.reachable(gw, &self.agent) {
            self.deliver_gone(inc, thread);
        } else {
            let me = self.clone();
            self.world.spawn("relay keepalive", async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                me.deliver_gone(inc, thread);
            });
        }
    }

    fn deliver_gone(&self, inc: u64, thread: u64) {
        let Some(l) = self.current() else { return };
        if l.inc != inc {
            return;
        }
        match l.fence.client_gone(TENANT, thread) {
            ClientGone::Close => {
                let link = l.links.conns.lock().unwrap().remove(&thread);
                drop(link);
            }
            ClientGone::Keep => {
                if let Some(x) = l.links.conns.lock().unwrap().get_mut(&thread) {
                    x.client = false;
                }
            }
        }
    }

    /// A call from a gateway to the agent (its admin's held-branch calls):
    /// the agent runs it in a task of its own, which dies with the agent
    /// but not with the gateway.
    async fn call<T: Send + 'static>(
        &self,
        gw: &Node,
        dead: sprig_common::CancellationToken,
        f: impl std::future::Future<Output = DbResult<T>> + Send + 'static,
    ) -> DbResult<T> {
        let started = tokio::time::Instant::now();
        if self.net.transit(gw, &self.agent).await.is_err() {
            Self::lost(started).await;
            return Err(DbError::lost("agent unreachable"));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.world.spawn("agent call", async move {
            let r = tokio::select! {
                biased;
                () = dead.cancelled() => None,
                r = f => Some(r),
            };
            let _ = tx.send(r);
        });
        let Ok(Some(r)) = rx.await else {
            Self::lost(started).await;
            return Err(DbError::lost("the agent died"));
        };
        if self.net.transit(&self.agent, gw).await.is_err() {
            Self::lost(started).await;
            return Err(DbError::lost("agent unreachable"));
        }
        r
    }
}

impl Env {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_millis()).unwrap()
    }
}

fn err(o: &Outcome) -> Option<DbError> {
    o.errno().map(|e| DbError::new(e, format!("error {e}")))
}

/// A gateway's backend connection: straight to the server, or through
/// the node agent's relay.
enum Wire {
    Direct(SimConn),
    Relayed(RelayedConn),
}

impl Wire {
    async fn exec(&self, s: &Stmt) -> Outcome {
        match self {
            Self::Direct(c) => c.exec(s).await,
            Self::Relayed(r) => r.relay.exec(r, s).await,
        }
    }

    fn thread(&self) -> u64 {
        match self {
            Self::Direct(c) => c.id().0,
            Self::Relayed(r) => r.thread,
        }
    }
}

/// A branch on a model connection.
struct SimBranch {
    conn: Wire,
    env: Arc<Env>,
    schema: &'static str,
    /// Sprig 2PC: the writes recorded for a held branch.
    redo: Option<Vec<sprig_txn::RedoStmt>>,
}

/// A model write as a redo statement: `I|U|D` `|table|key|value`.
fn redo_of(s: &Stmt) -> Option<String> {
    match s {
        Stmt::Insert { t, k, v } => Some(format!("I|{t}|{k}|{v}")),
        Stmt::Update { t, k, v } => Some(format!("U|{t}|{k}|{v}")),
        Stmt::Delete { t, k } => Some(format!("D|{t}|{k}|")),
        _ => None,
    }
}

fn stmt_of(r: &str) -> Option<Stmt> {
    let mut p = r.splitn(4, '|');
    let (op, t, k, v) = (p.next()?, p.next()?, p.next()?.parse().ok()?, p.next()?);
    let t = t.to_owned();
    Some(match op {
        "I" => Stmt::Insert {
            t,
            k,
            v: v.to_owned(),
        },
        "U" => Stmt::Update {
            t,
            k,
            v: v.to_owned(),
        },
        "D" => Stmt::Delete { t, k },
        _ => return None,
    })
}

impl SimBranch {
    async fn exec(&mut self, s: &Stmt) -> Outcome {
        let o = self.conn.exec(s).await;
        if let (Some(log), None, Some(sql)) = (self.redo.as_mut(), o.errno(), redo_of(s)) {
            log.push(sprig_txn::RedoStmt {
                sql,
                ..sprig_txn::RedoStmt::default()
            });
        }
        o
    }
}

#[async_trait::async_trait]
impl BranchConn for SimBranch {
    fn thread_id(&self) -> u64 {
        self.conn.thread()
    }

    fn start_redo(&mut self, _timestamp_us: u64) -> bool {
        self.redo = Some(Vec::new());
        true
    }

    fn take_redo(&mut self) -> Result<Vec<sprig_txn::RedoStmt>, String> {
        Ok(self.redo.take().unwrap_or_default())
    }

    async fn control(&mut self, ctl: &[Ctl]) -> DbResult<()> {
        for c in ctl {
            let s = match c {
                Ctl::Begin { snapshot: true, .. } => Stmt::BeginSnapshot,
                Ctl::Begin { .. } => Stmt::Begin,
                Ctl::Commit => Stmt::Commit,
                Ctl::Rollback => Stmt::Rollback,
                Ctl::XaStart(x) => Stmt::XaStart(x.sql()),
                Ctl::XaEnd(x) => Stmt::XaEnd(x.sql()),
                Ctl::XaPrepare(x) => Stmt::XaPrepare(x.sql()),
                Ctl::XaCommitOnePhase(x) => Stmt::XaCommitOnePhase(x.sql()),
                Ctl::XaCommit(x) => Stmt::XaCommit(x.sql()),
                Ctl::XaRollback(x) => Stmt::XaRollback(x.sql()),
                Ctl::LockWaitTimeout(n) => Stmt::SetLockWaitTimeout(*n),
                Ctl::InsertDecision { schema, row } => {
                    assert_eq!(schema, self.schema, "one schema per cluster in the sim");
                    Stmt::Insert {
                        t: DTX.to_owned(),
                        k: dkey(row.dtid),
                        v: encode_row(row, self.env.now_ms()),
                    }
                }
                Ctl::Savepoint(_) | Ctl::RollbackTo(_) => {
                    return Err(DbError::new(1235, "savepoints are not modelled"));
                }
                // The model has no clock to fix.
                Ctl::SetTimestamp(_) => continue,
                Ctl::RedoDone { schema, dtid, .. } => {
                    assert_eq!(schema, self.schema, "one schema per cluster in the sim");
                    Stmt::Insert {
                        t: REDO_MARK.to_owned(),
                        k: dkey(*dtid),
                        v: format!("done#{}", self.env.now_ms()),
                    }
                }
            };
            if let Some(e) = err(&self.conn.exec(&s).await) {
                return Err(e);
            }
        }
        Ok(())
    }
}

/// The admin: a fresh model connection per call, from `from`.
struct SimAdmin {
    env: Arc<Env>,
    from: Node,
    /// A gateway's admin: held-branch calls on relay-fenced clusters go to
    /// the node's agent.
    via_relay: bool,
}

impl SimAdmin {
    fn new(env: Arc<Env>, from: Node) -> Self {
        Self {
            env,
            from,
            via_relay: false,
        }
    }

    fn relay(&self, cluster: &ClusterId) -> Option<Arc<SimRelay>> {
        if self.via_relay {
            self.env.relay(cluster).cloned()
        } else {
            None
        }
    }

    fn is_agent(&self) -> bool {
        self.env
            .relays
            .iter()
            .flatten()
            .any(|r| r.agent == self.from)
    }
}

impl SimAdmin {
    async fn conn(&self, cluster: &ClusterId) -> DbResult<SimConn> {
        let d = def(cluster);
        self.env
            .db
            .connect(&self.from, d.writer, None, d.schema)
            .await
            .map_err(|e| DbError::new(e, "connect"))
    }

    async fn one(&self, cluster: &ClusterId, s: &Stmt) -> DbResult<Outcome> {
        let c = self.conn(cluster).await?;
        let o = c.exec(s).await;
        match err(&o) {
            Some(e) => Err(e),
            None => Ok(o),
        }
    }
}

#[async_trait::async_trait]
impl ClusterAdmin for SimAdmin {
    async fn xa_recover(&self, cluster: &ClusterId) -> DbResult<Vec<Xid>> {
        let o = self.one(cluster, &Stmt::XaRecover).await?;
        let Outcome::Ok { rows, .. } = o else {
            unreachable!()
        };
        Ok(rows
            .iter()
            .filter_map(|r| Xid::parse_sql(r.first()?.as_deref()?))
            .collect())
    }

    async fn xa_finish(&self, cluster: &ClusterId, xid: &Xid, commit: bool) -> DbResult<()> {
        let s = if commit {
            Stmt::XaCommit(xid.sql())
        } else {
            Stmt::XaRollback(xid.sql())
        };
        let r = self.one(cluster, &s).await.map(|_| ());
        if r.is_ok() && self.from.as_str() == "ctl" {
            bump(if commit {
                &STATS.resolver_commits
            } else {
                &STATS.resolver_rollbacks
            });
        }
        r
    }

    async fn kill(&self, cluster: &ClusterId, thread: u64) -> DbResult<()> {
        match self
            .one(cluster, &Stmt::KillConnection(ConnId(thread)))
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.errno == 1094 => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn insert_decision(
        &self,
        cluster: &ClusterId,
        schema: &str,
        row: &DecisionRow,
    ) -> DbResult<()> {
        assert_eq!(schema, def(cluster).schema);
        let c = self.conn(cluster).await?;
        if let Some(e) = err(&c.exec(&Stmt::SetLockWaitTimeout(2)).await) {
            return Err(e);
        }
        let o = c
            .exec(&Stmt::Insert {
                t: DTX.to_owned(),
                k: dkey(row.dtid),
                v: encode_row(row, self.env.now_ms()),
            })
            .await;
        err(&o).map_or(Ok(()), Err)
    }

    async fn read_decision(
        &self,
        cluster: &ClusterId,
        schema: &str,
        dtid: Dtid,
    ) -> DbResult<Option<DecisionRow>> {
        assert_eq!(schema, def(cluster).schema);
        let o = self
            .one(
                cluster,
                &Stmt::Get {
                    t: DTX.to_owned(),
                    k: dkey(dtid),
                },
            )
            .await?;
        let Outcome::Ok { rows, .. } = o else {
            unreachable!()
        };
        let v = rows.first().and_then(|r| r.get(1).cloned().flatten());
        Ok(v.and_then(|v| decode_row(dtid, &v, self.env.now_ms())))
    }

    async fn settle_pending(
        &self,
        cluster: &ClusterId,
        schema: &str,
        dtid: Dtid,
        outcome: Decision,
    ) -> DbResult<()> {
        let Some(mut row) = self.read_decision(cluster, schema, dtid).await? else {
            return Ok(());
        };
        if row.outcome != Decision::Pending {
            return Ok(());
        }
        row.outcome = outcome;
        let created = self.env.now_ms() - row.age_seconds.unwrap_or(0) * 1000;
        self.one(
            cluster,
            &Stmt::Update {
                t: DTX.to_owned(),
                k: dkey(dtid),
                v: encode_row(&row, created),
            },
        )
        .await
        .map(|_| ())
    }

    async fn decision_rows(&self, cluster: &ClusterId) -> DbResult<Vec<(String, DecisionRow)>> {
        let o = self.one(cluster, &Stmt::Scan(DTX.to_owned())).await?;
        let Outcome::Ok { rows, .. } = o else {
            unreachable!()
        };
        let now = self.env.now_ms();
        let schema = def(cluster).schema.to_owned();
        Ok(rows
            .iter()
            .filter_map(|r| {
                let k: i64 = r.first()?.as_deref()?.parse().ok()?;
                let v = r.get(1)?.as_deref()?;
                Some((schema.clone(), decode_row(dtid_of(k), v, now)?))
            })
            .collect())
    }

    async fn delete_decision(
        &self,
        cluster: &ClusterId,
        _schema: &str,
        dtid: Dtid,
    ) -> DbResult<()> {
        self.one(
            cluster,
            &Stmt::Delete {
                t: DTX.to_owned(),
                k: dkey(dtid),
            },
        )
        .await
        .map(|_| ())
    }

    async fn insert_redo(
        &self,
        cluster: &ClusterId,
        schema: &str,
        dtid: Dtid,
        gtrid: &sprig_txn::Gtrid,
        stmts: &[sprig_txn::RedoStmt],
    ) -> DbResult<()> {
        assert_eq!(schema, def(cluster).schema);
        // One row per branch in the model (its key is the dtid).
        let body: Vec<&str> = stmts.iter().map(|s| s.sql.as_str()).collect();
        self.one(
            cluster,
            &Stmt::Insert {
                t: REDO.to_owned(),
                k: dkey(dtid),
                v: format!(
                    "{}#{}#{}",
                    hex(&gtrid.to_bytes()),
                    self.env.now_ms(),
                    body.join(";")
                ),
            },
        )
        .await
        .map(|_| ())
    }

    async fn redo_branches(&self, cluster: &ClusterId) -> DbResult<Vec<sprig_txn::RedoBranch>> {
        let Outcome::Ok { rows, .. } = self.one(cluster, &Stmt::Scan(REDO.to_owned())).await?
        else {
            unreachable!()
        };
        let Outcome::Ok { rows: marks, .. } =
            self.one(cluster, &Stmt::Scan(REDO_MARK.to_owned())).await?
        else {
            unreachable!()
        };
        let now = self.env.now_ms();
        let mark_of = |k: i64| -> Option<(sprig_txn::redo::RedoMark, u64)> {
            marks.iter().find_map(|r| {
                let mk: i64 = r.first()?.as_deref()?.parse().ok()?;
                if mk != k {
                    return None;
                }
                let v = r.get(1)?.as_deref()?;
                let (who, at) = v.split_once('#')?;
                let m = if who == "done" {
                    sprig_txn::redo::RedoMark::Done
                } else {
                    sprig_txn::redo::RedoMark::Replayed
                };
                Some((m, at.parse().ok()?))
            })
        };
        let mut out: Vec<sprig_txn::RedoBranch> = rows
            .iter()
            .filter_map(|r| {
                let k: i64 = r.first()?.as_deref()?.parse().ok()?;
                let v = r.get(1)?.as_deref()?;
                let mut p = v.splitn(3, '#');
                let gtrid = sprig_txn::Gtrid::from_bytes(&unhex(p.next()?)?)?;
                let created: u64 = p.next()?.parse().ok()?;
                Some(sprig_txn::RedoBranch {
                    schema: def(cluster).schema.to_owned(),
                    dtid: dtid_of(k),
                    gtrid,
                    age_seconds: now.saturating_sub(created) / 1000,
                    statements: 1,
                    mark: mark_of(k).map(|(m, _)| m),
                })
            })
            .collect();
        // Markers without statements (a replay's, kept for the retention).
        for r in &marks {
            let Some(k) = r
                .first()
                .and_then(|x| x.as_deref())
                .and_then(|x| x.parse::<i64>().ok())
            else {
                continue;
            };
            if out.iter().any(|b| b.dtid == dtid_of(k)) {
                continue;
            }
            let Some((m, at)) = mark_of(k) else { continue };
            out.push(sprig_txn::RedoBranch {
                schema: def(cluster).schema.to_owned(),
                dtid: dtid_of(k),
                gtrid: sprig_txn::Gtrid {
                    tenant: TENANT,
                    dtid: dtid_of(k),
                    start_ms: 0,
                    home: shard_ref(0).anchor(),
                },
                age_seconds: now.saturating_sub(at) / 1000,
                statements: 0,
                mark: Some(m),
            });
        }
        Ok(out)
    }

    async fn replay_redo(
        &self,
        cluster: &ClusterId,
        schema: &str,
        dtid: Dtid,
    ) -> DbResult<sprig_txn::Replay> {
        assert_eq!(schema, def(cluster).schema);
        let c = self.conn(cluster).await?;
        let k = dkey(dtid);
        for s in [Stmt::SetLockWaitTimeout(2), Stmt::Begin] {
            if let Some(e) = err(&c.exec(&s).await) {
                return Err(e);
            }
        }
        // The marker first: a holder still finishing holds it (this waits,
        // then finds it), or committed it (a duplicate).
        match c
            .exec(&Stmt::Insert {
                t: REDO_MARK.to_owned(),
                k,
                v: format!("replayed#{}", self.env.now_ms()),
            })
            .await
        {
            Outcome::Ok { .. } => {}
            Outcome::Err(1062) => {
                let _ = c.exec(&Stmt::Rollback).await;
                return Ok(sprig_txn::Replay::Gone);
            }
            Outcome::Err(e) => {
                let _ = c.exec(&Stmt::Rollback).await;
                return Err(DbError::new(e, "redo marker"));
            }
        }
        let o = c
            .exec(&Stmt::GetForUpdate {
                t: REDO.to_owned(),
                k,
            })
            .await;
        let row = match o {
            Outcome::Ok { rows, .. } => rows.first().and_then(|r| r.get(1).cloned().flatten()),
            Outcome::Err(e) => {
                let _ = c.exec(&Stmt::Rollback).await;
                return Err(DbError::new(e, "redo read"));
            }
        };
        let Some(v) = row else {
            let _ = c.exec(&Stmt::Rollback).await;
            return Ok(sprig_txn::Replay::Gone);
        };
        let body = v.splitn(3, '#').nth(2).unwrap_or("");
        for r in body.split(';').filter(|x| !x.is_empty()) {
            let s = stmt_of(r).expect("a redo statement the sim wrote");
            if let Some(e) = err(&c.exec(&s).await) {
                let _ = c.exec(&Stmt::Rollback).await;
                if e.errno == 1062 {
                    return Ok(sprig_txn::Replay::Hazard(format!("{r}: duplicate key")));
                }
                return Err(e);
            }
        }
        for s in [
            Stmt::Delete {
                t: REDO.to_owned(),
                k,
            },
            Stmt::Commit,
        ] {
            if let Some(e) = err(&c.exec(&s).await) {
                let _ = c.exec(&Stmt::Rollback).await;
                return Err(e);
            }
        }
        if self.from.as_str() == "ctl" {
            bump(&STATS.redo_replays);
        } else if self.is_agent() {
            bump(&STATS.agent_replays);
        }
        Ok(sprig_txn::Replay::Applied)
    }

    async fn insert_redo_held(
        &self,
        cluster: &ClusterId,
        schema: &str,
        dtid: Dtid,
        gtrid: &sprig_txn::Gtrid,
        stmts: &[sprig_txn::RedoStmt],
        holder: u64,
    ) -> DbResult<()> {
        if let Some(r) = self.relay(cluster) {
            let Some(l) = r.current() else {
                return Err(DbError::lost("the agent is down"));
            };
            let (fence, schema, gtrid, stmts) =
                (l.fence.clone(), schema.to_owned(), *gtrid, stmts.to_vec());
            return r
                .call(&self.from, l.dead.clone(), async move {
                    fence
                        .prepare(TENANT, holder, &schema, dtid, &gtrid, &stmts)
                        .await
                })
                .await;
        }
        self.insert_redo(cluster, schema, dtid, gtrid, stmts).await
    }

    async fn abandon_held(
        &self,
        cluster: &ClusterId,
        _schema: &str,
        dtid: Dtid,
        holder: u64,
    ) -> DbResult<()> {
        if let Some(r) = self.relay(cluster) {
            let Some(l) = r.current() else {
                return Err(DbError::lost("the agent is down"));
            };
            return r
                .call(&self.from, l.dead.clone(), async move {
                    l.fence.abandon(dtid, holder);
                    Ok(())
                })
                .await;
        }
        Ok(())
    }

    async fn delete_redo(&self, cluster: &ClusterId, schema: &str, dtid: Dtid) -> DbResult<()> {
        assert_eq!(schema, def(cluster).schema);
        if let Some(r) = self.relay(cluster) {
            let Some(l) = r.current() else {
                return Err(DbError::lost("the agent is down"));
            };
            let (fence, schema) = (l.fence.clone(), schema.to_owned());
            return r
                .call(&self.from, l.dead.clone(), async move {
                    fence.finished(&schema, dtid).await
                })
                .await;
        }
        for t in [REDO, REDO_MARK] {
            self.one(
                cluster,
                &Stmt::Delete {
                    t: t.to_owned(),
                    k: dkey(dtid),
                },
            )
            .await?;
        }
        if self.from.as_str() == "ctl" {
            bump(&STATS.redo_drops);
        } else if self.is_agent() {
            bump(&STATS.agent_drops);
        }
        Ok(())
    }
}

struct SimBackend {
    env: Arc<Env>,
    from: Node,
    admin: Arc<SimAdmin>,
}

#[async_trait::async_trait]
impl TxnBackend for SimBackend {
    type Conn = SimBranch;

    fn caps(&self, cluster: &ClusterId) -> ClusterCaps {
        ClusterCaps {
            xa: def(cluster).xa,
        }
    }

    async fn checkout(&self, cluster: &ClusterId) -> DbResult<SimBranch> {
        let d = def(cluster);
        let conn = if let Some(r) = self.env.relay(cluster) {
            Wire::Relayed(r.open(&self.from).await?)
        } else {
            Wire::Direct(
                self.env
                    .db
                    .connect(&self.from, d.writer, None, d.schema)
                    .await
                    .map_err(|e| DbError::new(e, "connect"))?,
            )
        };
        Ok(SimBranch {
            conn,
            env: self.env.clone(),
            schema: d.schema,
            redo: None,
        })
    }

    fn admin(&self) -> Arc<dyn ClusterAdmin> {
        self.admin.clone()
    }

    fn relay_fenced(&self, cluster: &ClusterId) -> bool {
        self.env.relay(cluster).is_some()
    }
}

/// Crashes the gateway at the seed's coordinator step.
struct CrashHooks {
    world: World,
    gw: Node,
    target: Option<(Point, u32)>,
    seen: Mutex<HashMap<Point, u32>>,
    fired: AtomicBool,
    restart_after: Duration,
}

#[async_trait::async_trait]
impl TxnHooks for CrashHooks {
    async fn at(&self, dtid: Option<Dtid>, p: Point) {
        let Some((tp, n)) = self.target else {
            return;
        };
        if tp != p || self.fired.load(Ordering::SeqCst) {
            return;
        }
        let hit = {
            let mut seen = self.seen.lock().unwrap();
            let c = seen.entry(p).or_insert(0);
            *c += 1;
            *c == n
        };
        if !hit {
            return;
        }
        self.fired.store(true, Ordering::SeqCst);
        bump(&STATS.crashes);
        self.world
            .record(format!("CRASH gateway at {p:?} of {dtid:?}"));
        let net = self.world.net().clone();
        let gw = self.gw.clone();
        let after = self.restart_after;
        net.crash(&gw);
        self.world.spawn("restart gateway", async move {
            tokio::time::sleep(after).await;
            net.restart(&gw);
        });
        std::future::pending::<()>().await;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Res {
    Committed,
    Failed(u16),
    ClientRollback,
}

#[derive(Debug, Default)]
struct Entry {
    clusters: Vec<usize>,
    dtid: Option<Dtid>,
    result: Option<Res>,
    /// Hot rows it appended to (cluster, key), read-modify-write.
    hot: Vec<(usize, i64)>,
}

#[derive(Default)]
struct Log {
    next: AtomicU64,
    txns: Mutex<BTreeMap<i64, Entry>>,
    /// Reads after an OK commit that missed the session's own row.
    ryw_misses: Mutex<Vec<String>>,
}

impl Log {
    fn start(&self, clusters: Vec<usize>) -> i64 {
        let tag = i64::try_from(self.next.fetch_add(1, Ordering::SeqCst) + 1).unwrap();
        self.txns.lock().unwrap().insert(
            tag,
            Entry {
                clusters,
                ..Entry::default()
            },
        );
        tag
    }

    fn set_dtid(&self, tag: i64, d: Option<Dtid>) {
        self.txns.lock().unwrap().get_mut(&tag).unwrap().dtid = d;
    }

    fn finish(&self, tag: i64, r: Res) {
        self.txns.lock().unwrap().get_mut(&tag).unwrap().result = Some(r);
    }

    fn hot(&self, tag: i64, cluster: usize, key: i64) {
        self.txns
            .lock()
            .unwrap()
            .get_mut(&tag)
            .unwrap()
            .hot
            .push((cluster, key));
    }
}

#[derive(Clone)]
struct Scenario {
    policy: NonXaPolicy,
    all_xa: bool,
    /// Sprig 2PC on Galera only.
    all_galera: bool,
    crash: Option<(Point, u32)>,
    hot: f64,
    /// Hot rows are read-modify-write list appends (invariant 7).
    rmw: bool,
    /// After an OK commit, read the row back on each cluster: `Some(true)`
    /// through the coordinator (which settles stragglers first),
    /// `Some(false)` on a plain connection that doesn't (invariant 8).
    ryw: Option<bool>,
    /// Galera writer nodes' agents relay every connection.
    fence: bool,
    /// A second gateway writes the same rows.
    gw2: bool,
}

impl Scenario {
    /// Sprig 2PC: mostly three Galera clusters and nothing else.
    fn sprig(rng: &SimRng) -> Self {
        let mut sc = Self::random(rng);
        sc.policy = NonXaPolicy::Sprig2pc;
        sc.all_xa = false;
        sc.all_galera = rng.chance(0.8);
        sc
    }

    fn random(rng: &SimRng) -> Self {
        const POINTS: [Point; 7] = [
            Point::BranchOpened,
            Point::BeforePrepare,
            Point::Prepared,
            Point::PendingRecorded,
            Point::BeforeCommitPoint,
            Point::CommitPoint,
            Point::Phase2Branch,
        ];
        let crash = if rng.chance(0.9) {
            let p = *rng.pick(&POINTS);
            Some((p, u32::try_from(1 + rng.below(8)).unwrap()))
        } else {
            None
        };
        Self {
            policy: if rng.chance(0.2) {
                NonXaPolicy::BestEffort
            } else {
                NonXaPolicy::Reject
            },
            all_xa: rng.chance(0.2),
            all_galera: false,
            crash,
            hot: if rng.chance(0.5) { 0.4 } else { 0.0 },
            rmw: false,
            ryw: None,
            fence: false,
            gw2: false,
        }
    }

    /// Sprig 2PC with read-modify-write hot rows, a second gateway, and
    /// statements that outlive their client; `fence` puts the relays in.
    fn isolation(rng: &SimRng, fence: bool, ryw: Option<bool>) -> Self {
        let mut sc = Self::sprig(rng);
        sc.hot = if rng.chance(0.8) { 0.6 } else { 0.3 };
        sc.rmw = true;
        sc.ryw = ryw;
        sc.fence = fence;
        sc.gw2 = true;
        sc
    }

    fn clusters(&self) -> Vec<usize> {
        if self.all_xa {
            vec![0, 1]
        } else if self.all_galera {
            vec![2, 3, 4]
        } else if self.policy == NonXaPolicy::Sprig2pc {
            vec![0, 1, 2, 3, 4]
        } else {
            vec![0, 1, 2, 3]
        }
    }
}

fn code_of(e: &sprig_common::SprigError) -> u16 {
    e.protocol().mysql_errno
}

/// Reads the row of `tag` on cluster `i` after its commit: `Some(seen)`,
/// or `None` when the read itself failed.
async fn read_back(
    txn: &mut TxnCoordinator<SimBackend>,
    backend: &SimBackend,
    settled: bool,
    i: usize,
    tag: i64,
) -> Option<bool> {
    let get = Stmt::Get {
        t: T.to_owned(),
        k: tag,
    };
    let seen = |o: Outcome| match o {
        Outcome::Ok { rows, .. } => Some(
            rows.first()
                .and_then(|r| r.get(1).cloned().flatten())
                .is_some(),
        ),
        Outcome::Err(_) => None,
    };
    if !settled {
        // A plain connection, as a read outside the coordinator once was,
        // without settling the session's phase-2 stragglers.
        let d = &CLUSTERS[i];
        let c = backend
            .env
            .db
            .connect(&backend.from, d.writer, None, d.schema)
            .await
            .ok()?;
        return seen(c.exec(&get).await);
    }
    txn.begin(BeginOptions::default()).unwrap();
    let r = match txn.branch(&cid(i), &shard_ref(i), Intent::Read).await {
        Ok(b) => seen(b.conn_mut().exec(&get).await),
        Err(e) => {
            if code_of(&e) == 39449 {
                bump(&STATS.settle_refused);
            }
            None
        }
    };
    if txn.in_txn() {
        let _ = txn.commit().await;
    }
    r
}

/// A hot row's read-modify-write append: read it locked, write it back
/// with `tag` added.
async fn append(b: &mut SimBranch, key: i64, tag: i64) -> Option<DbError> {
    let o = b
        .exec(&Stmt::GetForUpdate {
            t: T.to_owned(),
            k: key,
        })
        .await;
    let v = match o {
        Outcome::Ok { rows, .. } => rows
            .first()
            .and_then(|r| r.get(1).cloned().flatten())
            .unwrap_or_default(),
        Outcome::Err(e) => return Some(DbError::new(e, format!("error {e}"))),
    };
    err(&b
        .exec(&Stmt::Update {
            t: T.to_owned(),
            k: key,
            v: format!("{v},{tag}"),
        })
        .await)
}

#[allow(clippy::too_many_arguments)]
async fn session(
    world: World,
    backend: Arc<SimBackend>,
    dtids: Arc<DtidGen>,
    clock: Arc<dyn Clock>,
    log: Arc<Log>,
    hooks: Arc<CrashHooks>,
    sc: Scenario,
    cfg: TxnConfig,
) {
    let mut txn = TxnCoordinator::new(backend.clone(), dtids, TENANT, cfg);
    txn.set_hooks(hooks);
    txn.set_clock(clock);
    let rng = world.rng();
    while world.elapsed() < HORIZON {
        let mut pool = sc.clusters();
        rng.shuffle(&mut pool);
        let n = 1 + rng.index(3.min(pool.len()));
        pool.truncate(n);
        let tag = log.start(pool.clone());
        txn.begin(BeginOptions::default()).unwrap();
        let mut result = None;
        'stmts: for &i in &pool {
            let c = cid(i);
            let b = match txn.branch(&c, &shard_ref(i), Intent::Write).await {
                Ok(b) => b,
                Err(e) => {
                    result = Some(Res::Failed(code_of(&e)));
                    break;
                }
            };
            let mut stmts = vec![Stmt::Insert {
                t: T.to_owned(),
                k: tag,
                v: tag.to_string(),
            }];
            let mut rmw = None;
            if rng.chance(sc.hot) {
                let key = HOT + i64::try_from(rng.below(2)).unwrap();
                if sc.rmw {
                    rmw = Some(key);
                } else {
                    stmts.push(Stmt::Update {
                        t: T.to_owned(),
                        k: key,
                        v: tag.to_string(),
                    });
                }
            }
            let mut failed = None;
            for s in stmts {
                if let Some(e) = err(&b.conn_mut().exec(&s).await) {
                    failed = Some(e);
                    break;
                }
            }
            if failed.is_none()
                && let Some(key) = rmw
            {
                log.hot(tag, i, key);
                bump(&STATS.appends);
                failed = append(b.conn_mut(), key, tag).await;
            }
            if let Some(e) = failed {
                result = Some(match txn.statement_failed(&c, &e).await {
                    Err(se) => Res::Failed(code_of(&se)),
                    Ok(()) => Res::ClientRollback,
                });
                break 'stmts;
            }
            world.chaos_yield().await;
        }
        if let Some(r) = result {
            if txn.in_txn() {
                txn.rollback().await.unwrap();
            }
            log.finish(tag, r);
        } else {
            log.set_dtid(tag, txn.dtid());
            let r = match txn.commit().await {
                Ok(o) => {
                    if o.stragglers > 0 {
                        STATS
                            .stragglers
                            .fetch_add(o.stragglers as u64, Ordering::Relaxed);
                    }
                    match o.mode {
                        sprig_txn::TxnMode::Xa2pc => bump(&STATS.xa_2pc),
                        sprig_txn::TxnMode::Sprig2pc => bump(&STATS.sprig_2pc),
                        sprig_txn::TxnMode::BestEffort => bump(&STATS.best_effort),
                        _ => {}
                    }
                    Res::Committed
                }
                Err(e) => {
                    world.record(format!("txn {tag} commit failed: {e}"));
                    Res::Failed(code_of(&e))
                }
            };
            let ok = r == Res::Committed;
            log.finish(tag, r);
            if ok && let Some(settled) = sc.ryw {
                for &i in &pool {
                    bump(&STATS.ryw_reads);
                    if read_back(&mut txn, &backend, settled, i, tag).await == Some(false) {
                        log.ryw_misses.lock().unwrap().push(format!(
                            "txn {tag}: committed (client got OK), but the session's next read \
                             of cluster {} missed its row",
                            CLUSTERS[i].id
                        ));
                    }
                }
            }
        }
        world.sleep(Duration::from_millis(rng.range(1..200))).await;
    }
}

fn model(world: &World) -> SimMariaDb {
    SimMariaDb::new(world, |m| {
        m.add_server(ServerConfig::new("a1", 101, 1));
        let mut p = ServerConfig::new("b1", 201, 2);
        p.semisync_master = true;
        m.add_server(p);
        m.add_replica(
            ServerConfig::new("b2", 202, 2),
            "b1",
            Duration::from_millis(30),
        );
        m.add_galera("g1", vec![ServerConfig::new("g1n1", 301, 3)], 3);
        m.add_galera("g2", vec![ServerConfig::new("g2n1", 401, 4)], 4);
        m.add_galera("g3", vec![ServerConfig::new("g3n1", 501, 5)], 5);
    })
}

async fn setup(env: &Env) {
    let me = Node::from("setup");
    for d in &CLUSTERS {
        let c = env.db.connect(&me, d.writer, None, d.schema).await.unwrap();
        for t in [T, DTX, REDO, REDO_MARK] {
            assert!(
                c.exec(&Stmt::CreateTable(t.to_owned()))
                    .await
                    .errno()
                    .is_none()
            );
        }
        for k in [HOT, HOT + 1] {
            let o = c
                .exec(&Stmt::Insert {
                    t: T.to_owned(),
                    k,
                    v: "0".into(),
                })
                .await;
            assert!(o.errno().is_none(), "{o}");
        }
    }
}

/// Which world a test runs.
#[derive(Debug, Clone, Copy)]
enum Kind {
    /// XA and best effort.
    Xa,
    /// Sprig 2PC without the relay.
    Sprig,
    /// Sprig 2PC with read-modify-write hot rows, a second gateway and
    /// orphaned statements; `fence`: the relays; `ryw`:
    /// read each row back after an OK commit.
    Isolation { fence: bool, ryw: Option<bool> },
}

const GALERA: [usize; 3] = [2, 3, 4];

#[allow(clippy::too_many_arguments)]
fn spawn_gateway(
    world: &World,
    gw: &Node,
    env: &Arc<Env>,
    log: &Arc<Log>,
    sc: &Scenario,
    hooks: &Arc<CrashHooks>,
    dtid_base: u64,
) {
    let (env, log, sc, hooks) = (env.clone(), log.clone(), sc.clone(), hooks.clone());
    world.spawn_node(gw, move |ctx: NodeCtx| {
        let (env, log, sc, hooks) = (env.clone(), log.clone(), sc.clone(), hooks.clone());
        async move {
            let admin = Arc::new(SimAdmin {
                env: env.clone(),
                from: ctx.node.clone(),
                via_relay: sc.fence,
            });
            let backend = Arc::new(SimBackend {
                env,
                from: ctx.node.clone(),
                admin,
            });
            let dtids = Arc::new(DtidGen::new(dtid_base + ctx.incarnation + 1));
            let cfg = TxnConfig {
                policy: sc.policy,
                tenant_all_xa: sc.all_xa,
                distributed_lock_wait_timeout_s: 2,
                // Below the resolver's min_age (5 s here), as in production
                // (5 s against 30 s): an unknown commit is raced only while
                // its commit row can't have been swept.
                commit_resolve_timeout: Duration::from_secs(3),
                coordinator_id: format!("{}-{}", ctx.node.as_str(), ctx.incarnation),
                ..TxnConfig::default()
            };
            let clock: Arc<dyn Clock> = ctx.clock();
            let s = (0..2).map(|_| {
                session(
                    ctx.world.clone(),
                    backend.clone(),
                    dtids.clone(),
                    clock.clone(),
                    log.clone(),
                    hooks.clone(),
                    sc.clone(),
                    cfg.clone(),
                )
            });
            futures::future::join_all(s).await;
        }
    });
}

#[allow(clippy::too_many_lines)]
async fn run(world: World, plan: FaultPlan, kind: Kind) -> Result<(), String> {
    let db = model(&world);
    let sc = match kind {
        Kind::Xa => Scenario::random(world.rng()),
        Kind::Sprig => Scenario::sprig(world.rng()),
        Kind::Isolation { fence, ryw } => Scenario::isolation(world.rng(), fence, ryw),
    };
    if sc.rmw {
        // MariaDB runs a statement on after its client went away.
        db.with_model(|m, _| m.set_orphans_run(true));
    }
    let relays: Vec<Option<Arc<SimRelay>>> = (0..CLUSTERS.len())
        .map(|i| {
            (sc.fence && GALERA.contains(&i)).then(|| {
                Arc::new(SimRelay {
                    idx: i,
                    agent: Node::from(format!("{}a", CLUSTERS[i].id).as_str()),
                    db: db.clone(),
                    net: world.net().clone(),
                    world: world.clone(),
                    live: Mutex::new(None),
                })
            })
        })
        .collect();
    let env = Arc::new(Env {
        db: db.clone(),
        origin: tokio::time::Instant::now(),
        relays,
    });
    setup(&env).await;
    world.record(format!(
        "scenario policy={:?} all_xa={} crash={:?} hot={} rmw={} ryw={:?} fence={} gw2={}",
        sc.policy, sc.all_xa, sc.crash, sc.hot, sc.rmw, sc.ryw, sc.fence, sc.gw2
    ));
    let locator = Arc::new(StaticLocator::new());
    for i in 0..CLUSTERS.len() {
        let s = shard_ref(i);
        locator.insert(TENANT, s.anchor(), cid(i), &s.schema);
    }
    let log = Arc::new(Log::default());
    let gw = Node::from("gw");
    let ctl = Node::from("ctl");
    let hooks = Arc::new(CrashHooks {
        world: world.clone(),
        gw: gw.clone(),
        target: sc.crash,
        seen: Mutex::new(HashMap::new()),
        fired: AtomicBool::new(false),
        restart_after: Duration::from_millis(world.rng().range(200..5000)),
    });
    let rcfg = ResolverConfig {
        interval: Duration::from_secs(2),
        min_age: Duration::from_secs(5),
        abort_retention: Duration::from_secs(3600),
        id: "resolver".into(),
    };
    // The agents first: the relays admit nobody until their start-up
    // recovery is done.
    for r in env.relays.iter().flatten() {
        let (env, locator, rcfg, relay) = (env.clone(), locator.clone(), rcfg.clone(), r.clone());
        world.spawn_node(&r.agent.clone(), move |ctx: NodeCtx| {
            let (env, locator, rcfg, relay) =
                (env.clone(), locator.clone(), rcfg.clone(), relay.clone());
            async move {
                let links = Arc::new(SimLinks {
                    db: env.db.clone(),
                    agent: ctx.node.clone(),
                    idx: relay.idx,
                    conns: Mutex::new(BTreeMap::new()),
                });
                let admin = Arc::new(SimAdmin::new(env.clone(), ctx.node.clone()));
                let fence = Arc::new(RelayFence::new(
                    cid(relay.idx),
                    links.clone(),
                    admin,
                    locator,
                    ResolverConfig {
                        id: format!("agent-{}", ctx.node.as_str()),
                        ..rcfg
                    },
                ));
                let inc = ctx.incarnation;
                *relay.live.lock().unwrap() = Some(RelayLive {
                    inc,
                    fence: fence.clone(),
                    links,
                    dead: sprig_common::CancellationToken::new(),
                });
                let _guard = LiveGuard {
                    relay: relay.clone(),
                    inc,
                };
                while fence.start().await.is_err() {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                fence.run(sprig_common::CancellationToken::new()).await;
            }
        });
    }
    spawn_gateway(&world, &gw, &env, &log, &sc, &hooks, 0);
    if sc.gw2 {
        let quiet = Arc::new(CrashHooks {
            world: world.clone(),
            gw: Node::from("gw2"),
            target: None,
            seen: Mutex::new(HashMap::new()),
            fired: AtomicBool::new(false),
            restart_after: Duration::ZERO,
        });
        spawn_gateway(&world, &Node::from("gw2"), &env, &log, &sc, &quiet, 1 << 20);
    }
    {
        let (env, locator, rcfg) = (env.clone(), locator.clone(), rcfg.clone());
        let fenced: Vec<ClusterId> = GALERA
            .iter()
            .filter(|&&i| env.relays[i].is_some())
            .map(|&i| cid(i))
            .collect();
        world.spawn_node(&ctl, move |ctx: NodeCtx| {
            let (env, locator, rcfg, fenced) =
                (env.clone(), locator.clone(), rcfg.clone(), fenced.clone());
            async move {
                let admin = Arc::new(SimAdmin::new(env, ctx.node.clone()));
                let r = Resolver::new(admin, locator, rcfg);
                r.set_fenced(fenced);
                let all: Vec<ClusterId> = (0..CLUSTERS.len()).map(cid).collect();
                r.run(move || all.clone(), sprig_common::CancellationToken::new())
                    .await;
            }
        });
    }
    world.run_plan(&plan).await;
    let left = HORIZON.saturating_sub(world.elapsed());
    world.sleep(left).await;
    world.heal_all().await;
    // In-flight commits finish (at most a few client timeouts), then the
    // resolver gets min_age plus several intervals.
    world.sleep(Duration::from_secs(45)).await;
    world.heal_all().await;
    world.sleep(rcfg.min_age + rcfg.interval * 6).await;
    let r = check(&world, &env, &log, &sc).await;
    // Break the relays' reference cycles (relay -> fence -> admin -> env).
    for rl in env.relays.iter().flatten() {
        if let Some(l) = rl.live.lock().unwrap().take() {
            let st = l.fence.stats();
            STATS.fences.fetch_add(st.fenced, Ordering::Relaxed);
            STATS.fence_ended.fetch_add(st.ended, Ordering::Relaxed);
            l.links.conns.lock().unwrap().clear();
        }
    }
    r
}

/// Invariant 7: every hot row's final list holds each committed append
/// exactly once, and nothing else.
fn check_isolation(
    env: &Arc<Env>,
    txns: &BTreeMap<i64, Entry>,
    present: &[BTreeSet<i64>],
) -> Result<(), String> {
    for (i, d) in CLUSTERS.iter().enumerate() {
        let rows = env.db.with_model(|m, _| m.rows(d.writer, d.schema, T));
        for key in [HOT, HOT + 1] {
            let v = rows
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.clone())
                .unwrap_or_default();
            let mut seen: BTreeMap<i64, usize> = BTreeMap::new();
            for t in v.split(',').skip(1) {
                let tag: i64 = t
                    .parse()
                    .map_err(|_| format!("isolation: hot row {key} on {}: {v:?}", d.id))?;
                *seen.entry(tag).or_default() += 1;
            }
            for (tag, n) in &seen {
                if *n > 1 {
                    return Err(format!(
                        "isolation: txn {tag}'s append to hot row {key} on {} applied {n} times",
                        d.id
                    ));
                }
                if !present[i].contains(tag) {
                    return Err(format!(
                        "isolation: hot row {key} on {} holds txn {tag}'s append, but txn {tag} \
                         didn't commit there",
                        d.id
                    ));
                }
            }
            for (tag, e) in txns {
                if present[i].contains(tag) && e.hot.contains(&(i, key)) && !seen.contains_key(tag)
                {
                    return Err(format!(
                        "isolation: LOST UPDATE: txn {tag} ({:?}, client got {:?}) committed its \
                         append to hot row {key} on {}, but the row's final value {v:?} lacks it \
                         (a later write overwrote it, or it was replayed over one)",
                        e.dtid, e.result, d.id
                    ));
                }
            }
        }
    }
    Ok(())
}

async fn check(world: &World, env: &Arc<Env>, log: &Log, sc: &Scenario) -> Result<(), String> {
    // Rows present per cluster.
    let present: Vec<BTreeSet<i64>> = CLUSTERS
        .iter()
        .map(|d| {
            env.db.with_model(|m, _| {
                m.rows(d.writer, d.schema, T)
                    .into_iter()
                    .map(|(k, _)| k)
                    .filter(|k| *k < HOT)
                    .collect()
            })
        })
        .collect();
    let txns: BTreeMap<i64, Entry> = log
        .txns
        .lock()
        .unwrap()
        .iter()
        .map(|(k, e)| {
            (
                *k,
                Entry {
                    clusters: e.clusters.clone(),
                    dtid: e.dtid,
                    result: e.result.clone(),
                    hot: e.hot.clone(),
                },
            )
        })
        .collect();
    let mut committed = 0usize;
    for (tag, e) in &txns {
        let on: BTreeSet<usize> = (0..CLUSTERS.len())
            .filter(|&i| present[i].contains(tag))
            .collect();
        let want: BTreeSet<usize> = e.clusters.iter().copied().collect();
        if !on.is_subset(&want) {
            return Err(format!("txn {tag}: rows on {on:?}, wrote only {want:?}"));
        }
        let partial = !on.is_empty() && on != want;
        let unknown = matches!(e.result, None | Some(Res::Failed(39441)));
        if partial {
            let allowed = sc.policy == NonXaPolicy::BestEffort
                && (unknown || e.result == Some(Res::Failed(39442)));
            if !allowed {
                return Err(format!(
                    "txn {tag} ({:?}): PARTIAL commit, rows on {on:?} of {want:?}, client got {:?}",
                    e.dtid, e.result
                ));
            }
        }
        match &e.result {
            Some(Res::Committed) => {
                committed += 1;
                if on != want {
                    return Err(format!(
                        "txn {tag} ({:?}): client got OK but rows are on {on:?} of {want:?}",
                        e.dtid
                    ));
                }
            }
            Some(Res::Failed(c)) if *c != 39441 && *c != 39442 => {
                if !on.is_empty() {
                    return Err(format!(
                        "txn {tag} ({:?}): client got error {c} but rows are on {on:?}",
                        e.dtid
                    ));
                }
            }
            Some(Res::ClientRollback) => {
                if !on.is_empty() {
                    return Err(format!("txn {tag}: rolled back but rows on {on:?}"));
                }
            }
            _ => {}
        }
        if sc.policy == NonXaPolicy::Reject
            && [2, 3, 4].iter().filter(|g| on.contains(g)).count() > 1
        {
            return Err(format!(
                "txn {tag}: reject policy, but both Galera clusters committed"
            ));
        }
    }
    if sc.rmw {
        check_isolation(env, &txns, &present)?;
    }
    if let Some(m) = log.ryw_misses.lock().unwrap().first() {
        return Err(format!("read-your-writes: {m}"));
    }
    // No redo row left: every held branch was finished or replayed.
    for d in &CLUSTERS {
        let a = SimAdmin::new(env.clone(), Node::from("checker"));
        let left: Vec<sprig_txn::RedoBranch> = a
            .redo_branches(&ClusterId::new(d.id).unwrap())
            .await
            .map_err(|e| format!("redo rows on {}: {e}", d.id))?
            .into_iter()
            .filter(|b| b.statements > 0 || b.mark == Some(sprig_txn::redo::RedoMark::Done))
            .collect();
        if !left.is_empty() {
            return Err(format!(
                "cluster {}: redo rows left after recovery: {left:?}",
                d.id
            ));
        }
    }
    // Nothing prepared.
    for d in &CLUSTERS {
        let a = SimAdmin::new(env.clone(), Node::from("checker"));
        let left = a
            .xa_recover(&ClusterId::new(d.id).unwrap())
            .await
            .map_err(|e| format!("XA RECOVER on {}: {e}", d.id))?;
        if !left.is_empty() {
            return Err(format!(
                "cluster {}: still prepared after recovery: {left:?}",
                d.id
            ));
        }
    }
    // Decision rows.
    let mut rows: BTreeMap<Dtid, Vec<(usize, DecisionRow)>> = BTreeMap::new();
    for (i, d) in CLUSTERS.iter().enumerate() {
        let a = SimAdmin::new(env.clone(), Node::from("checker"));
        for (_, r) in a
            .decision_rows(&ClusterId::new(d.id).unwrap())
            .await
            .map_err(|e| e.to_string())?
        {
            rows.entry(r.dtid).or_default().push((i, r));
        }
    }
    let by_dtid: HashMap<Dtid, (&i64, &Entry)> = txns
        .iter()
        .filter_map(|(t, e)| e.dtid.map(|d| (d, (t, e))))
        .collect();
    for (dtid, rs) in &rows {
        let commit = rs.iter().any(|(_, r)| r.outcome == Decision::Commit);
        let abort = rs.iter().any(|(_, r)| r.outcome == Decision::Abort);
        if commit && abort {
            return Err(format!("dtid {dtid}: both commit and abort rows: {rs:?}"));
        }
        if commit || rs.iter().any(|(_, r)| r.outcome == Decision::Pending) {
            return Err(format!(
                "dtid {dtid}: commit or pending row left after its participants finished: {rs:?}"
            ));
        }
        if abort && let Some((tag, e)) = by_dtid.get(dtid) {
            let on: Vec<usize> = e
                .clusters
                .iter()
                .copied()
                .filter(|&i| present[i].contains(tag))
                .collect();
            if !on.is_empty() && sc.policy != NonXaPolicy::BestEffort {
                return Err(format!("dtid {dtid}: abort recorded but rows on {on:?}"));
            }
        }
    }
    bump(&STATS.runs);
    for e in txns.values() {
        bump(&STATS.txns);
        match &e.result {
            Some(Res::Committed) => bump(&STATS.committed),
            Some(Res::Failed(39440)) => bump(&STATS.rejected),
            Some(Res::Failed(39441)) => bump(&STATS.unknown),
            Some(Res::Failed(39442)) => bump(&STATS.partial),
            Some(_) => bump(&STATS.rolled_back),
            None => bump(&STATS.no_answer),
        }
    }
    world.record(format!(
        "checked {} txns, {committed} committed, {} decision rows left",
        txns.len(),
        rows.len()
    ));
    Ok(())
}

#[test]
fn xa_transactions_survive_coordinator_and_participant_crashes() {
    let nodes: Vec<Node> = ["gw", "ctl", "a1", "b1", "b2", "g1n1", "g2n1"]
        .into_iter()
        .map(Node::from)
        .collect();
    Sim::new("txn")
        .seeds(200)
        .max_time(Duration::from_secs(3600))
        .run_with_faults(
            move |rng| {
                let n = rng.index(6);
                FaultPlan::random(rng, &nodes, HORIZON, n)
            },
            |w, plan| async move { run(w, plan, Kind::Xa).await.map_err(Into::into) },
        );
    print_totals("xa");
}

/// Sprig 2PC passes the XA crash simulations on
/// all-Galera (coordinator crashes at every step, participant kills,
/// pauses and partitions), with no partial commit after recovery.
#[test]
fn sprig_2pc_survives_coordinator_and_participant_crashes_on_galera() {
    let nodes: Vec<Node> = ["gw", "ctl", "a1", "b1", "b2", "g1n1", "g2n1", "g3n1"]
        .into_iter()
        .map(Node::from)
        .collect();
    Sim::new("txn_sprig2pc")
        .seeds(200)
        .max_time(Duration::from_secs(3600))
        .run_with_faults(
            move |rng| {
                let n = rng.index(6);
                FaultPlan::random(rng, &nodes, HORIZON, n)
            },
            |w, plan| async move { run(w, plan, Kind::Sprig).await.map_err(Into::into) },
        );
    print_totals("sprig_2pc");
}

const FENCED_NODES: [&str; 11] = [
    "gw", "gw2", "ctl", "a1", "b1", "b2", "g1n1", "g2n1", "g3n1", "g1a", "g2a",
];

fn isolation_plan(rng: &SimRng) -> FaultPlan {
    let mut nodes: Vec<Node> = FENCED_NODES.into_iter().map(Node::from).collect();
    nodes.push(Node::from("g3a"));
    let n = rng.index(6);
    FaultPlan::random(rng, &nodes, HORIZON, n)
}

/// With the relay fence, a held branch's rows stay
/// out of reach from its holder's loss until it is recovered: no write
/// is lost to a replay, under gateway crashes at every coordinator step,
/// a second gateway on the same rows, agent and node kills, pauses,
/// partitions and slow links. And after an OK commit the session's next
/// read sees its rows.
#[test]
fn sprig_2pc_relay_fence_keeps_held_rows_isolated() {
    Sim::new("txn_sprig2pc_fence")
        .seeds(200)
        .max_time(Duration::from_secs(3600))
        .run_with_faults(isolation_plan, |w, plan| async move {
            run(
                w,
                plan,
                Kind::Isolation {
                    fence: true,
                    ryw: Some(true),
                },
            )
            .await
            .map_err(Into::into)
        });
    print_totals("sprig_2pc_fence");
}

/// The same world without the relay fence (Sprig 2PC with held
/// branches on the gateway's own connection): a held branch whose
/// gateway died is replayed after another transaction already changed
/// its rows, and that transaction's committed append is lost. Run with `--ignored` to see
/// the simulation catch it.
#[test]
#[ignore = "shows the window the relay fence closes"]
#[should_panic(expected = "isolation")]
fn without_the_relay_fence_a_replay_overwrites_a_later_write() {
    Sim::new("txn_sprig2pc_nofence")
        .seeds(200)
        .max_time(Duration::from_secs(3600))
        .run_with_faults(isolation_plan, |w, plan| async move {
            run(
                w,
                plan,
                Kind::Isolation {
                    fence: false,
                    ryw: None,
                },
            )
            .await
            .map_err(Into::into)
        });
}

/// Without settling the session's phase-2 stragglers (a read on a plain
/// connection, as reads outside the coordinator once were), a read
/// right after an OK commit misses the session's own row when phase 2
/// outran `txn.phase2_wait_ms`. Run with `--ignored`.
#[test]
#[ignore = "shows the window that settling stragglers closes"]
#[should_panic(expected = "read-your-writes")]
fn without_settling_a_straggler_the_next_read_misses_it() {
    Sim::new("txn_sprig2pc_nosettle")
        .seeds(200)
        .max_time(Duration::from_secs(3600))
        .run_with_faults(isolation_plan, |w, plan| async move {
            run(
                w,
                plan,
                Kind::Isolation {
                    fence: true,
                    ryw: Some(false),
                },
            )
            .await
            .map_err(Into::into)
        });
}

fn print_totals(what: &str) {
    let g = |c: &AtomicU64| c.load(Ordering::Relaxed);
    let s = &*STATS;
    println!(
        "sim txn totals ({what}; both tests share the counters when run in one process): \
         runs {} (determinism reruns included), txns {}, committed {} \
         (xa_2pc {}, sprig_2pc {}, best_effort {}), rejected 39440 {}, rolled back {}, outcome unknown \
         39441 {}, partial 39442 {}, no answer (gateway crashed) {}; gateway crashes at a \
         coordinator step {}; resolver XA COMMIT {}, XA ROLLBACK {}, redo replays {}, redo drops {}; \
         relay fence: tenants fenced {}, connections ended {}, connections refused {}, agent \
         replays {}, agent redo deletes (finished and dropped) {}; hot-row appends {}; phase-2 stragglers {}, read-backs {}, \
         settles refused (39449) {}",
        g(&s.runs),
        g(&s.txns),
        g(&s.committed),
        g(&s.xa_2pc),
        g(&s.sprig_2pc),
        g(&s.best_effort),
        g(&s.rejected),
        g(&s.rolled_back),
        g(&s.unknown),
        g(&s.partial),
        g(&s.no_answer),
        g(&s.crashes),
        g(&s.resolver_commits),
        g(&s.resolver_rollbacks),
        g(&s.redo_replays),
        g(&s.redo_drops),
        g(&s.fences),
        g(&s.fence_ended),
        g(&s.fence_refused),
        g(&s.agent_replays),
        g(&s.agent_drops),
        g(&s.appends),
        g(&s.stragglers),
        g(&s.ryw_reads),
        g(&s.settle_refused),
    );
}
