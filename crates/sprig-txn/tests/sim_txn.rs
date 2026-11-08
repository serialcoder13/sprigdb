//! Distributed transactions under deterministic simulation (chapter 18,
//! "Testing"; chapter 46; build step 9; the R1 done-when item "XA
//! transactions survive coordinator and participant crashes in simulation,
//! with no partial commits after recovery").
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
//! `SPRIG_SIM_SEEDS=10000 cargo test -p sprig-txn --test sim_txn` runs
//! the milestone's 10,000 seeds; CI runs the default.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::missing_panics_doc
)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId, TxnId};
use sprig_common::time::Clock;
use sprig_testkit::faults::Node;
use sprig_testkit::sim::mariadb::{ConnId, Outcome, ServerConfig, SimConn, SimMariaDb, Stmt};
use sprig_testkit::sim::{FaultPlan, NodeCtx, Sim, SimRng, World};
use sprig_txn::decision::{decode_participants, encode_participants};
use sprig_txn::xid::{hex, unhex};
use sprig_txn::{
    AnchorRef, BeginOptions, BranchConn, ClusterAdmin, ClusterCaps, Ctl, DbError, DbResult,
    Decision, DecisionRow, Dtid, DtidGen, Intent, NonXaPolicy, Point, Resolver, ResolverConfig,
    ShardRef, StaticLocator, TxnBackend, TxnConfig, TxnCoordinator, TxnHooks, Xid,
};

const TENANT: TenantId = TenantId(1);
const DTX: &str = "__sprig_dtx";
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

const CLUSTERS: [ClusterDef; 4] = [
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
        hash_point: (i as u64) << 62,
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
}

impl Env {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_millis()).unwrap()
    }
}

fn err(o: &Outcome) -> Option<DbError> {
    o.errno().map(|e| DbError::new(e, format!("error {e}")))
}

/// A branch on a model connection.
struct SimBranch {
    conn: SimConn,
    env: Arc<Env>,
    schema: &'static str,
}

impl SimBranch {
    async fn exec(&self, s: &Stmt) -> Outcome {
        self.conn.exec(s).await
    }
}

#[async_trait::async_trait]
impl BranchConn for SimBranch {
    fn thread_id(&self) -> u64 {
        self.conn.id().0
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
        let conn = self
            .env
            .db
            .connect(&self.from, d.writer, None, d.schema)
            .await
            .map_err(|e| DbError::new(e, "connect"))?;
        Ok(SimBranch {
            conn,
            env: self.env.clone(),
            schema: d.schema,
        })
    }

    fn admin(&self) -> Arc<dyn ClusterAdmin> {
        self.admin.clone()
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
}

#[derive(Default)]
struct Log {
    next: AtomicU64,
    txns: Mutex<BTreeMap<i64, Entry>>,
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
}

#[derive(Clone)]
struct Scenario {
    policy: NonXaPolicy,
    all_xa: bool,
    crash: Option<(Point, u32)>,
    hot: f64,
}

impl Scenario {
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
            crash,
            hot: if rng.chance(0.5) { 0.4 } else { 0.0 },
        }
    }

    fn clusters(&self) -> Vec<usize> {
        if self.all_xa {
            vec![0, 1]
        } else {
            vec![0, 1, 2, 3]
        }
    }
}

fn code_of(e: &sprig_common::SprigError) -> u16 {
    e.protocol().mysql_errno
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
    let mut txn = TxnCoordinator::new(backend, dtids, TENANT, cfg);
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
            if rng.chance(sc.hot) {
                stmts.push(Stmt::Update {
                    t: T.to_owned(),
                    k: HOT + i64::try_from(rng.below(2)).unwrap(),
                    v: tag.to_string(),
                });
            }
            let mut failed = None;
            for s in stmts {
                if let Some(e) = err(&b.conn_mut().exec(&s).await) {
                    failed = Some(e);
                    break;
                }
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
                    match o.mode {
                        sprig_txn::TxnMode::Xa2pc => bump(&STATS.xa_2pc),
                        sprig_txn::TxnMode::BestEffort => bump(&STATS.best_effort),
                        _ => {}
                    }
                    Res::Committed
                }
                Err(e) => Res::Failed(code_of(&e)),
            };
            log.finish(tag, r);
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
    })
}

async fn setup(env: &Env) {
    let me = Node::from("setup");
    for d in &CLUSTERS {
        let c = env.db.connect(&me, d.writer, None, d.schema).await.unwrap();
        for t in [T, DTX] {
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

async fn run(world: World, plan: FaultPlan) -> Result<(), String> {
    let db = model(&world);
    let env = Arc::new(Env {
        db: db.clone(),
        origin: tokio::time::Instant::now(),
    });
    setup(&env).await;
    let sc = Scenario::random(world.rng());
    world.record(format!(
        "scenario policy={:?} all_xa={} crash={:?} hot={}",
        sc.policy, sc.all_xa, sc.crash, sc.hot
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
    {
        let (env, log, sc, hooks) = (env.clone(), log.clone(), sc.clone(), hooks.clone());
        world.spawn_node(&gw, move |ctx: NodeCtx| {
            let (env, log, sc, hooks) = (env.clone(), log.clone(), sc.clone(), hooks.clone());
            async move {
                let admin = Arc::new(SimAdmin {
                    env: env.clone(),
                    from: ctx.node.clone(),
                });
                let backend = Arc::new(SimBackend {
                    env,
                    from: ctx.node.clone(),
                    admin,
                });
                let dtids = Arc::new(DtidGen::new(ctx.incarnation + 1));
                let cfg = TxnConfig {
                    policy: sc.policy,
                    tenant_all_xa: sc.all_xa,
                    distributed_lock_wait_timeout_s: 2,
                    coordinator_id: format!("gw-{}", ctx.incarnation),
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
    {
        let (env, locator, rcfg) = (env.clone(), locator.clone(), rcfg.clone());
        world.spawn_node(&ctl, move |ctx: NodeCtx| {
            let (env, locator, rcfg) = (env.clone(), locator.clone(), rcfg.clone());
            async move {
                let admin = Arc::new(SimAdmin {
                    env,
                    from: ctx.node.clone(),
                });
                let r = Resolver::new(admin, locator, rcfg);
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
    check(&world, &env, &log, &sc).await
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
        if sc.policy == NonXaPolicy::Reject && on.contains(&2) && on.contains(&3) {
            return Err(format!(
                "txn {tag}: reject policy, but both Galera clusters committed"
            ));
        }
    }
    // Nothing prepared.
    for d in &CLUSTERS {
        let a = SimAdmin {
            env: env.clone(),
            from: Node::from("checker"),
        };
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
        let a = SimAdmin {
            env: env.clone(),
            from: Node::from("checker"),
        };
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
            if !on.is_empty() && sc.policy == NonXaPolicy::Reject {
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
            |w, plan| async move { run(w, plan).await.map_err(Into::into) },
        );
    let g = |c: &AtomicU64| c.load(Ordering::Relaxed);
    let s = &*STATS;
    println!(
        "sim txn totals: runs {} (determinism reruns included), txns {}, committed {} \
         (xa_2pc {}, best_effort {}), rejected 39440 {}, rolled back {}, outcome unknown \
         39441 {}, partial 39442 {}, no answer (gateway crashed) {}; gateway crashes at a \
         coordinator step {}; resolver XA COMMIT {}, XA ROLLBACK {}",
        g(&s.runs),
        g(&s.txns),
        g(&s.committed),
        g(&s.xa_2pc),
        g(&s.best_effort),
        g(&s.rejected),
        g(&s.rolled_back),
        g(&s.unknown),
        g(&s.partial),
        g(&s.no_answer),
        g(&s.crashes),
        g(&s.resolver_commits),
        g(&s.resolver_rollbacks),
    );
}
