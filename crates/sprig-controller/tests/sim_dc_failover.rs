//! DC failover under partitions, process pauses, kills and clock skew, in
//! deterministic simulation: no split-brain writes, automatic DC failover
//! only when its conditions hold, and a returning data center stays
//! fenced with its diverged transactions reported.
//!
//! Three sites: `fra1` (home of tenants 1 and 2), `ams1` (their standby
//! DC) and the witness `wit1`, plus two global-leader candidates. The real
//! code runs: every controller keeps the DC lease with `DcLease` against
//! the simulated global etcd; gateways and the writer's agent hold
//! `PermitHolder`s renewed from their DC's controllers over the simulated
//! network; the elected global leader runs `GlobalFailover` with guarded
//! writes, asking observers that run the real `observe()`; promotions run
//! the real `promote()`; the returning DC runs `ReturningDc`.
//!
//! MariaDB is a model: one database node per DC holding a list per
//! tenant, a `read_only` flag the agent sets from its permit, and `ams1`
//! pulling `fra1`'s lists every 200 ms (asynchronous replication).
//! Gateways append to the home DC's list and acknowledge only while their
//! permit is still valid when the reply arrives.
//!
//! Checked per seed:
//!
//! 1. **No split-brain**: every write acknowledged by a DC was acknowledged
//!    before the commit point that moved the tenant away from it, and none
//!    by the new home before it.
//! 2. **Conditions**: every promotion committed at least
//!    `clock_drift_margin` after the old home's DC lease vanished, after at
//!    least `min_sites - 1` valid `down`/`unsafe` votes and no `alive`, with
//!    a standby lag within the RPO.
//! 3. **Return**: after healing, the old home is `active` again with the
//!    moved tenants fenced and their maps quarantined; the transactions
//!    only it holds (acknowledged or not) are exactly the diverged range
//!    the report computes, and every acknowledged write missing from the
//!    new home is in it.
//! 4. **Residency** (a pinned tenant never leaves its jurisdiction, under
//!    any failure): tenant 3's residency
//!    allows only `fra1`, yet it has a standby in `ams1` (one recorded
//!    before its policy tightened, which the residency loop would drop);
//!    it is never moved. Tenant 4 starts unrestricted, and in the random
//!    scenario an operator restricts it to `fra1` at a seed-chosen moment,
//!    possibly while its failover is under way: no home change of tenant 4
//!    to `ams1` commits after that write.

// Test code: long scenario functions, short names for record fields, and
// locks held across awaits on purpose (a one-at-a-time guard; the
// single-threaded simulation's shared state).
#![allow(
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::needless_pass_by_value,
    clippy::collapsible_if,
    clippy::single_match_else,
    clippy::cast_possible_truncation,
    clippy::nonminimal_bool,
    clippy::await_holding_lock,
    clippy::redundant_closure_for_method_calls,
    clippy::semicolon_if_nothing_returned
)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::StreamExt;
use serde_json::{Map, json};
use sprig_catalog::placement::{MapRole, ShardMap};
use sprig_cluster::failover::primary_replica::diverged;
use sprig_common::ids::{ClusterId, DcId, TenantId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigError};
use sprig_controller::failover::global::{GlobalFailover, Outcome};
use sprig_controller::failover::observe::{DcProbe, PeerPing, ProbeRound, WriterTarget, observe};
use sprig_controller::failover::promote::{Guard, PromoteHooks, Promotion};
use sprig_controller::failover::record::{
    FailoverRecord, FailoverState, StandbyLag, Verdict, VerdictKind,
};
use sprig_controller::failover::recovery::{CopyFence, ReturningDc};
use sprig_controller::failover::{FailoverSettings, ObserverClient};
use sprig_meta::bootstrap::DcRecord;
use sprig_meta::dclease::{DcLease, dc_key};
use sprig_meta::permit::{Permit, PermitHolder, PermitRequest, PermitSource};
use sprig_meta::{
    Consistency, Election, Elector, LeaderRecord, ListOpts, MetaStore, Op, Record, SessionOpts,
    Tier, TierView, Txn, WatchEvent,
};
use sprig_testkit::faults::Node;
use sprig_testkit::sim::{
    FaultKind, FaultPlan, NodeCtx, PlannedFault, Sim, SimResult, SimRng, World,
};

const TENANTS: [u64; 4] = [1, 2, 3, 4];
/// Residency allows only fra1 from the start.
const PINNED: u64 = 3;
/// Restricted to fra1 during the random scenario.
const LATE_PIN: u64 = 4;
const FRA1_SERVER: u32 = 101;
const AMS1_SERVER: u32 = 201;
const PULL: Duration = Duration::from_millis(200);

fn dc(s: &str) -> DcId {
    DcId::new(s).expect("dc")
}

fn n(s: &str) -> Node {
    Node::from(s)
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One tenant's copy in a database node.
#[derive(Debug, Clone, Default)]
struct Copy {
    /// (server, value) per committed transaction; the index + 1 is the
    /// GTID sequence number in domain = tenant id.
    list: Vec<(u32, u64)>,
    writable: bool,
    fenced: bool,
    replicating: bool,
    /// Standby lag bookkeeping: true time of the last pull that saw the
    /// source and of the last one that left nothing to apply.
    source_seen_at: Duration,
    applied_through: Duration,
}

#[derive(Debug, Default)]
struct Db {
    server: u32,
    read_only: bool,
    copies: BTreeMap<u64, Copy>,
}

/// An acknowledged write.
#[derive(Debug, Clone)]
struct Ack {
    at: Duration,
    dc: String,
    tenant: u64,
    value: u64,
}

#[derive(Default)]
struct Shared {
    dbs: BTreeMap<String, Db>,
    leases: BTreeMap<String, Arc<DcLease>>,
    acks: Vec<Ack>,
    /// (true time, tenant, new home, epoch) of each directory home change.
    moves: Vec<(Duration, u64, String, u64)>,
    /// (true time, dc) of each DC-lease deletion.
    lease_gone: Vec<(Duration, String)>,
    /// Records written, last version per id.
    records: BTreeMap<String, FailoverRecord>,
    /// True time the directory first showed tenant 4's residency.
    late_pin_at: Option<Duration>,
    next_value: u64,
}

type S = Arc<Mutex<Shared>>;

fn settings() -> FailoverSettings {
    FailoverSettings {
        observer_probe_interval: Duration::from_millis(500),
        decision_timeout: Duration::from_secs(60),
        ..FailoverSettings::default()
    }
}

fn controllers(d: &str) -> Vec<Node> {
    match d {
        "wit1" => vec![n("wit1-c1")],
        _ => vec![n(&format!("{d}-c1")), n(&format!("{d}-c2"))],
    }
}

fn group(d: &str) -> Vec<Node> {
    let mut v = controllers(d);
    if d != "wit1" {
        v.push(n(&format!("{d}-gw")));
        v.push(n(&format!("{d}-agent")));
    }
    v
}

// ---- seeding --------------------------------------------------------------

async fn put(store: &dyn MetaStore, key: sprig_meta::Key, value: Vec<u8>) {
    store
        .txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key,
                value,
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .expect("seed");
}

async fn seed(world: &World, s: &S) {
    let g: Arc<dyn MetaStore> = Arc::new(world.etcd().store().clone());
    for (name, kind) in [("fra1", "full"), ("ams1", "full"), ("wit1", "witness")] {
        let rec = Record::new(DcRecord {
            dc_id: dc(name),
            kind: kind.into(),
            state: "active".into(),
            dc_epoch: 0,
            tags: Map::new(),
            global_voter: true,
        });
        put(&*g, dc_key(&dc(name)).expect("k"), rec.encode().expect("e")).await;
    }
    for t in TENANTS {
        let mut e = json!({"v": 1, "name": format!("t{t}"), "state": "active", "home_dc": "fra1",
                       "epoch": 1, "standby_dcs": ["ams1"]});
        if t == PINNED {
            e["residency"] = json!({"policy": "dc = \"fra1\"", "allowed_dcs": ["fra1"],
                                    "state": "converging"});
        }
        put(
            &*g,
            Tier::Global.key(&format!("tenants/{t}")).expect("k"),
            serde_json::to_vec(&e).expect("j"),
        )
        .await;
        for (d, role) in [("fra1", MapRole::Home), ("ams1", MapRole::Standby)] {
            let tier = TierView::over(&g, Tier::Dc(dc(d)));
            let mut m = ShardMap::catalog(
                TenantId(t),
                ClusterId::new(format!("{d}-c1")).expect("c"),
                1,
            );
            m.role = role;
            put(
                &*tier,
                ShardMap::catalog_key(tier.tier(), TenantId(t)).expect("k"),
                Record::new(m).encode().expect("e"),
            )
            .await;
        }
    }
    let mut sh = lock(s);
    for (d, server) in [("fra1", FRA1_SERVER), ("ams1", AMS1_SERVER)] {
        let mut db = Db {
            server,
            read_only: true,
            copies: BTreeMap::new(),
        };
        for t in TENANTS {
            db.copies.insert(
                t,
                Copy {
                    writable: d == "fra1",
                    replicating: d == "ams1",
                    ..Copy::default()
                },
            );
        }
        sh.dbs.insert(format!("{d}-db"), db);
    }
}

/// Watches the directory and the DC leases directly on the store (no
/// network, no faults): the ground truth for the checks.
fn watch_truth(world: &World, s: &S) {
    let store: Arc<dyn MetaStore> = Arc::new(world.etcd().store().clone());
    let (w, s) = (world.clone(), Arc::clone(s));
    world.spawn("truth", async move {
        let mut st = store.watch(&Tier::Global.root(), 0).await.expect("watch");
        while let Some(Ok(ev)) = st.next().await {
            let at = w.elapsed();
            match ev {
                WatchEvent::Put(kv) => {
                    let k = kv.key.as_str().to_owned();
                    if let Some(t) = k.strip_prefix("/sprig/g/tenants/") {
                        let v: serde_json::Value =
                            serde_json::from_slice(&kv.value).unwrap_or_default();
                        let home = v["home_dc"].as_str().unwrap_or_default().to_owned();
                        let epoch = v["epoch"].as_u64().unwrap_or(0);
                        let t: u64 = t.parse().unwrap_or(0);
                        let mut sh = lock(&s);
                        if t == LATE_PIN && !v["residency"].is_null() && sh.late_pin_at.is_none() {
                            w.record(format!("TRUTH tenant {t} restricted to fra1 (home {home})"));
                            sh.late_pin_at = Some(at);
                        }
                        let last = sh
                            .moves
                            .iter()
                            .rev()
                            .find(|m| m.1 == t)
                            .map(|m| m.2.clone());
                        if last.as_deref().unwrap_or("fra1") != home {
                            w.record(format!("TRUTH tenant {t} home {home} epoch {epoch}"));
                            sh.moves.push((at, t, home, epoch));
                        }
                    } else if k.starts_with("/sprig/g/failovers/") {
                        if let Ok(r) = Record::<FailoverRecord>::decode(&kv.value) {
                            lock(&s).records.insert(r.body.failover_id.clone(), r.body);
                        }
                    }
                }
                WatchEvent::Delete { key, .. } => {
                    if let Some(d) = key.as_str().strip_prefix("/sprig/g/dc-leases/") {
                        w.record(format!("TRUTH lease of {d} gone"));
                        lock(&s).lease_gone.push((at, d.to_owned()));
                    }
                }
                _ => {}
            }
        }
    });
}

// ---- the database model ---------------------------------------------------

async fn hop(world: &World, a: &Node, b: &Node) -> Result<()> {
    world
        .net()
        .transit(a, b)
        .await
        .map_err(|e| SprigError::internal(format!("{a}->{b}: {e}")))
}

async fn rpc<T>(world: &World, from: &Node, to: &Node, f: impl FnOnce() -> T) -> Result<T> {
    hop(world, from, to).await?;
    world.net().wait_running(to).await;
    let v = f();
    hop(world, to, from).await?;
    Ok(v)
}

/// `ams1-db` pulls `fra1-db` while replicating.
fn replicator(world: &World, s: &S) {
    let (w, s) = (world.clone(), Arc::clone(s));
    let (me, src) = (n("ams1-db"), n("fra1-db"));
    world.spawn("ams1-repl", async move {
        loop {
            tokio::time::sleep(PULL).await;
            let wanted: Vec<u64> = lock(&s).dbs["ams1-db"]
                .copies
                .iter()
                .filter(|(_, c)| c.replicating)
                .map(|(t, _)| *t)
                .collect();
            if wanted.is_empty() {
                continue;
            }
            let s2 = Arc::clone(&s);
            let Ok(lists) = rpc(&w, &me, &src, move || {
                let sh = lock(&s2);
                wanted
                    .iter()
                    .map(|t| (*t, sh.dbs["fra1-db"].copies[t].list.clone()))
                    .collect::<Vec<_>>()
            })
            .await
            else {
                continue;
            };
            let now = w.elapsed();
            let mut sh = lock(&s);
            let db = sh.dbs.get_mut("ams1-db").expect("db");
            for (t, src_list) in lists {
                let c = db.copies.get_mut(&t).expect("copy");
                if !c.replicating {
                    continue;
                }
                if src_list.len() >= c.list.len() && src_list[..c.list.len()] == c.list[..] {
                    c.list = src_list;
                }
                c.source_seen_at = now;
                c.applied_through = now;
            }
        }
    });
}

fn standby_lags(s: &S) -> Vec<StandbyLag> {
    let sh = lock(s);
    sh.dbs["ams1-db"]
        .copies
        .iter()
        .filter(|(_, c)| c.replicating)
        .map(|(t, c)| StandbyLag {
            tenant_id: *t,
            lag_ms: u32::try_from((c.source_seen_at.saturating_sub(c.applied_through)).as_millis())
                .unwrap_or(u32::MAX),
        })
        .collect()
}

// ---- permits, probes, observers over the simulated network ---------------

struct SimPermits {
    world: World,
    from: Node,
    to: Node,
    s: S,
}

#[async_trait::async_trait]
impl PermitSource for SimPermits {
    async fn renew(&self, _req: &PermitRequest) -> Result<Permit> {
        let s = Arc::clone(&self.s);
        let to = self.to.to_string();
        rpc(&self.world, &self.from, &self.to, move || {
            lock(&s)
                .leases
                .get(&to)
                .map_or(Permit::fenced(0), |l| l.permit())
        })
        .await
    }
}

fn permit_sources(world: &World, from: &Node, d: &str, s: &S) -> Vec<Arc<dyn PermitSource>> {
    controllers(d)
        .into_iter()
        .map(|c| {
            Arc::new(SimPermits {
                world: world.clone(),
                from: from.clone(),
                to: c,
                s: Arc::clone(s),
            }) as Arc<dyn PermitSource>
        })
        .collect()
}

struct SimProbe {
    world: World,
    from: Node,
    s: S,
}

#[async_trait::async_trait]
impl DcProbe for SimProbe {
    async fn probe(&self, x: &DcId) -> ProbeRound {
        let mut r = ProbeRound::default();
        for c in controllers(x.as_str()) {
            let s = Arc::clone(&self.s);
            let name = c.to_string();
            let x2 = x.clone();
            if let Ok(Some(p)) = rpc(&self.world, &self.from, &c, move || {
                lock(&s).leases.get(&name).map(|l| {
                    let p = l.permit();
                    PeerPing {
                        dc_id: x2,
                        dc_state: l.dc_state(),
                        dc_epoch: p.dc_epoch,
                        permits_valid: !p.fenced,
                        fenced_writers: vec![],
                    }
                })
            })
            .await
            {
                r.pings.push(p);
            }
        }
        if x.as_str() != "wit1" {
            let db = n(&format!("{x}-db"));
            let up = rpc(&self.world, &self.from, &db, || ()).await.is_ok();
            r.writers.push((
                WriterTarget {
                    cluster: format!("{x}-c1"),
                    addr: format!("{db}:3306"),
                    self_fences: true,
                },
                up,
            ));
        }
        r
    }
}

struct SimObservers {
    world: World,
    from: Node,
    s: S,
}

#[async_trait::async_trait]
impl ObserverClient for SimObservers {
    async fn observe(&self, site: &DcId, x: &DcId, _id: &str) -> Result<Verdict> {
        let mut last = SprigError::internal("no controller");
        for c in controllers(site.as_str()) {
            if hop(&self.world, &self.from, &c).await.is_err() {
                continue;
            }
            self.world.net().wait_running(&c).await;
            let lease = lock(&self.s).leases.get(c.as_str()).cloned();
            let Some(lease) = lease else { continue };
            let probe = SimProbe {
                world: self.world.clone(),
                from: c.clone(),
                s: Arc::clone(&self.s),
            };
            let lags = if site.as_str() == "ams1" {
                standby_lags(&self.s)
            } else {
                vec![]
            };
            let st = settings();
            let v = observe(
                site,
                lease.lease_valid(),
                x,
                &probe,
                st.observer_probes,
                st.observer_probe_interval,
                lags,
            )
            .await;
            match hop(&self.world, &c, &self.from).await {
                Ok(()) => return Ok(v),
                Err(e) => last = e,
            }
        }
        Err(last)
    }
}

struct SimHooks {
    world: World,
    from: Node,
    s: S,
}

#[async_trait::async_trait]
impl PromoteHooks for SimHooks {
    async fn stop_standby(&self, t: TenantId, to: &DcId) -> Result<BTreeMap<String, String>> {
        let db = format!("{to}-db");
        let s = Arc::clone(&self.s);
        rpc(&self.world, &self.from, &n(&db), move || {
            let mut sh = lock(&s);
            let c = sh
                .dbs
                .get_mut(&db)
                .expect("db")
                .copies
                .get_mut(&t.0)
                .expect("copy");
            c.replicating = false;
            let applied = format!("{}-{FRA1_SERVER}-{}", t.0, c.list.len());
            BTreeMap::from([("fra1-c1".to_owned(), applied)])
        })
        .await
    }

    async fn make_writable(&self, t: TenantId, to: &DcId) -> Result<()> {
        let db = format!("{to}-db");
        let s = Arc::clone(&self.s);
        rpc(&self.world, &self.from, &n(&db), move || {
            let mut sh = lock(&s);
            let d = sh.dbs.get_mut(&db).expect("db");
            d.read_only = false;
            d.copies.get_mut(&t.0).expect("copy").writable = true;
        })
        .await
    }
}

struct SimFence {
    world: World,
    from: Node,
    s: S,
}

#[async_trait::async_trait]
impl CopyFence for SimFence {
    async fn fence(&self, t: TenantId) -> Result<()> {
        let s = Arc::clone(&self.s);
        rpc(&self.world, &self.from, &n("fra1-db"), move || {
            let mut sh = lock(&s);
            let c = sh
                .dbs
                .get_mut("fra1-db")
                .expect("db")
                .copies
                .get_mut(&t.0)
                .expect("copy");
            c.fenced = true;
            c.writable = false;
        })
        .await
    }
}

// ---- the processes ---------------------------------------------------------

fn controller(world: &World, node: Node, d: &'static str, s: &S) {
    let s = Arc::clone(s);
    world.spawn_node(&node, move |ctx: NodeCtx| {
        let s = Arc::clone(&s);
        async move {
            let store = ctx.meta(Tier::Global);
            let clock: Arc<dyn Clock> = ctx.clock();
            let lease = Arc::new(DcLease::new(
                dc(d),
                ctx.node.to_string(),
                Arc::clone(&store),
                clock,
                settings().lease_opts(),
            ));
            lock(&s)
                .leases
                .insert(ctx.node.to_string(), Arc::clone(&lease));
            let keep = lease.run(|| true);
            // The returning DC's leader side (fra1's first controller).
            let ret = async {
                if ctx.node.as_str() != "fra1-c1" {
                    return std::future::pending::<()>().await;
                }
                let r = ReturningDc {
                    dc: dc(d),
                    global: Arc::clone(&store),
                    tier: ctx.meta(Tier::Dc(dc(d))),
                    fence: Arc::new(SimFence {
                        world: ctx.world.clone(),
                        from: ctx.node.clone(),
                        s: Arc::clone(&s),
                    }),
                };
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if let Ok(
                        step @ sprig_controller::failover::recovery::ReturnStep::Unfenced { .. },
                    ) = r.step().await
                    {
                        ctx.world.record(format!("fra1 return step {step:?}"));
                    }
                }
            };
            tokio::join!(keep, ret);
        }
    });
}

fn gateway(world: &World, d: &'static str, s: &S) {
    let s = Arc::clone(s);
    let node = n(&format!("{d}-gw"));
    world.spawn_node(&node, move |ctx: NodeCtx| {
        let s = Arc::clone(&s);
        async move {
            let clock: Arc<dyn Clock> = ctx.clock();
            let holder = PermitHolder::new(ctx.node.to_string(), "gateway", clock);
            let sources = permit_sources(&ctx.world, &ctx.node, d, &s);
            let renew = holder.run(sources, Duration::from_secs(1));
            let store = ctx.meta(Tier::Global);
            let db = n(&format!("{d}-db"));
            let work = async {
                let mut homes: BTreeMap<u64, String> = BTreeMap::new();
                let mut tick = 0u64;
                loop {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    tick += 1;
                    if tick % 5 == 1 {
                        for t in TENANTS {
                            let k = Tier::Global.key(&format!("tenants/{t}")).expect("k");
                            if let Ok(Some(kv)) = store.get(&k, Consistency::Serializable).await {
                                let v: serde_json::Value =
                                    serde_json::from_slice(&kv.value).unwrap_or_default();
                                homes.insert(
                                    t,
                                    v["home_dc"].as_str().unwrap_or_default().to_owned(),
                                );
                            }
                        }
                    }
                    for t in TENANTS {
                        if homes.get(&t).map(String::as_str) != Some(d) || !holder.writable() {
                            continue;
                        }
                        let value = {
                            let mut sh = lock(&s);
                            sh.next_value += 1;
                            sh.next_value
                        };
                        let s2 = Arc::clone(&s);
                        let dbn = db.to_string();
                        let r = rpc(&ctx.world, &ctx.node, &db, move || {
                            let mut sh = lock(&s2);
                            let dbm = sh.dbs.get_mut(&dbn).expect("db");
                            let server = dbm.server;
                            let ro = dbm.read_only;
                            let c = dbm.copies.get_mut(&t).expect("copy");
                            if ro || !c.writable || c.fenced {
                                return false;
                            }
                            c.list.push((server, value));
                            true
                        })
                        .await;
                        // Acknowledge only while the permit still holds.
                        if matches!(r, Ok(true)) && holder.writable() {
                            let at = ctx.world.elapsed();
                            lock(&s).acks.push(Ack {
                                at,
                                dc: d.to_owned(),
                                tenant: t,
                                value,
                            });
                        }
                    }
                }
            };
            tokio::join!(renew, work);
        }
    });
}

fn agent(world: &World, d: &'static str, s: &S) {
    let s = Arc::clone(s);
    let node = n(&format!("{d}-agent"));
    world.spawn_node(&node, move |ctx: NodeCtx| {
        let s = Arc::clone(&s);
        async move {
            let clock: Arc<dyn Clock> = ctx.clock();
            let holder = PermitHolder::new(ctx.node.to_string(), "agent", clock);
            let sources = permit_sources(&ctx.world, &ctx.node, d, &s);
            let renew = holder.run(sources, Duration::from_secs(1));
            let db = n(&format!("{d}-db"));
            let fence = async {
                loop {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    let ro = !holder.writable();
                    let s2 = Arc::clone(&s);
                    let dbn = db.to_string();
                    let _ = rpc(&ctx.world, &ctx.node, &db, move || {
                        lock(&s2).dbs.get_mut(&dbn).expect("db").read_only = ro;
                    })
                    .await;
                }
            };
            tokio::join!(renew, fence);
        }
    });
}

fn global_leader(world: &World, node: Node, s: &S) {
    let s = Arc::clone(s);
    world.spawn_node(&node, move |ctx: NodeCtx| {
        let s = Arc::clone(&s);
        async move {
            let store = ctx.meta(Tier::Global);
            let clock: Arc<dyn Clock> = ctx.clock();
            let election = Election::new(
                Arc::clone(&store),
                Arc::clone(&clock),
                SessionOpts::new(Duration::from_secs(15)),
            );
            let key = Tier::Global.key("leader").expect("k");
            let me = LeaderRecord {
                holder: ctx.node.to_string(),
                dc: None,
                build: "sim".into(),
            };
            loop {
                let Ok(l) = election.campaign(&key, &me).await else {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                };
                ctx.world.record(format!("{} leads globally", ctx.node));
                let promotion = Promotion {
                    global: Arc::clone(&store),
                    guard: Guard::Leader(l.clone()),
                    dc_tier: {
                        let st = Arc::clone(&store);
                        Arc::new(move |d: &DcId| TierView::over(&st, Tier::Dc(d.clone())))
                    },
                    hooks: Arc::new(SimHooks {
                        world: ctx.world.clone(),
                        from: ctx.node.clone(),
                        s: Arc::clone(&s),
                    }),
                };
                let g = GlobalFailover::new(
                    Arc::clone(&store),
                    Arc::clone(&clock),
                    Arc::new(|_: &DcId| settings()),
                    Arc::new(SimObservers {
                        world: ctx.world.clone(),
                        from: ctx.node.clone(),
                        s: Arc::clone(&s),
                    }),
                    promotion,
                );
                while l.is_leading() {
                    match g.tick().await {
                        Ok(out) => {
                            for o in out {
                                if let Outcome::Ended(f) = o {
                                    ctx.world.record(format!(
                                        "failover {} {:?}",
                                        f.failover_id, f.state
                                    ));
                                }
                            }
                        }
                        Err(e) => ctx.world.record(format!("global tick: {e}")),
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    });
}

// ---- faults ----------------------------------------------------------------

fn all_nodes() -> Vec<Node> {
    let mut v = group("fra1");
    v.extend(group("ams1"));
    v.extend(group("wit1"));
    v.extend([n("g1"), n("g2"), n("fra1-db"), n("ams1-db")]);
    v
}

/// Faults aimed at DC failover: the home DC cut from the global tier or
/// from everything, frozen (every process paused at once), single
/// processes paused or killed, observers and leaders disturbed, and clock
/// rate errors within the drift budget.
fn plan(rng: &SimRng) -> FaultPlan {
    let horizon = 120_000u64;
    let mut faults = Vec::new();
    let fra1 = group("fra1");
    let mut fra1_db = fra1.clone();
    fra1_db.push(n("fra1-db"));
    let others: Vec<Node> = all_nodes()
        .into_iter()
        .filter(|x| !fra1_db.contains(x))
        .collect();
    for _ in 0..rng.range(2..6) {
        let at_ms = rng.below(horizon);
        let for_ms = rng.range(3_000..40_000);
        let pick = |v: &[&str]| n(v[rng.index(v.len())]);
        let fault = match rng.below(10) {
            0 | 1 => FaultKind::Partition {
                a: fra1.clone(),
                b: vec![n("etcd")],
                for_ms,
            },
            2 => FaultKind::Partition {
                a: fra1_db.clone(),
                b: others.clone(),
                for_ms,
            },
            3 => {
                // The whole DC frozen: one pause per process.
                for p in &fra1 {
                    faults.push(PlannedFault {
                        at_ms,
                        fault: FaultKind::Pause {
                            node: p.clone(),
                            for_ms,
                        },
                    });
                }
                continue;
            }
            4 => FaultKind::Pause {
                node: pick(&["fra1-gw", "fra1-agent", "fra1-c1", "fra1-c2"]),
                for_ms,
            },
            5 => FaultKind::Kill {
                node: pick(&[
                    "fra1-c1", "fra1-c2", "fra1-gw", "g1", "g2", "ams1-c1", "wit1-c1",
                ]),
            },
            6 => FaultKind::Pause {
                node: pick(&["g1", "g2", "ams1-c1", "ams1-c2", "wit1-c1"]),
                for_ms,
            },
            7 => FaultKind::Partition {
                a: vec![pick(&["ams1-c1", "wit1-c1", "g1", "g2"])],
                b: vec![n("etcd")],
                for_ms,
            },
            8 => FaultKind::Skew {
                node: pick(&["fra1-c1", "fra1-c2", "fra1-gw", "fra1-agent", "g1", "g2"]),
                offset_ms: i64::try_from(rng.below(5000)).unwrap_or(0) - 2500,
                rate_ppm: i64::try_from(rng.below(4000)).unwrap_or(0) - 2000,
                for_ms,
            },
            _ => FaultKind::Partition {
                a: vec![n("fra1-gw")],
                b: controllers("fra1"),
                for_ms,
            },
        };
        faults.push(PlannedFault { at_ms, fault });
    }
    faults.sort_by_key(|f| f.at_ms);
    FaultPlan { faults }
}

// ---- the scenario and its checks -------------------------------------------

fn start(world: &World, s: &S) {
    watch_truth(world, s);
    replicator(world, s);
    for d in ["fra1", "ams1", "wit1"] {
        for c in controllers(d) {
            controller(world, c, d, s);
        }
    }
    for d in ["fra1", "ams1"] {
        gateway(world, d, s);
        agent(world, d, s);
    }
    global_leader(world, n("g1"), s);
    global_leader(world, n("g2"), s);
}

async fn scenario(world: World, plan: FaultPlan) -> SimResult {
    let s: S = Arc::new(Mutex::new(Shared::default()));
    seed(&world, &s).await;
    start(&world, &s);
    // Let leases and permits settle before the faults.
    world.sleep(Duration::from_secs(15)).await;
    // An operator restricts tenant 4 to fra1 at a seed-chosen moment of
    // the faults (the write itself isn't faulted: it is the operator's).
    let pin_after = Duration::from_millis(2_000 + world.seed().wrapping_mul(7919) % 150_000);
    let store: Arc<dyn MetaStore> = Arc::new(world.etcd().store().clone());
    let w2 = world.clone();
    world.spawn("late-pin", async move {
        w2.sleep(pin_after).await;
        let g = TierView::over(&store, Tier::Global);
        let r = sprig_shard::home::update_home(&*g, TenantId(LATE_PIN), |e| {
            e.raw.insert(
                "residency".into(),
                json!({"policy": "dc = \"fra1\"", "allowed_dcs": ["fra1"], "state": "converging"}),
            );
            Ok(true)
        })
        .await;
        w2.record(format!(
            "late pin of tenant {LATE_PIN} written: {}",
            r.is_ok()
        ));
    });
    world.run_plan(&plan).await;
    world.heal_all().await;
    // Recovery: the DC returns, fences, steps up again.
    world.sleep(Duration::from_secs(60)).await;
    check(&world, &s).await
}

#[allow(clippy::too_many_lines)] // one check per property
async fn check(world: &World, s: &S) -> SimResult {
    let store: Arc<dyn MetaStore> = Arc::new(world.etcd().store().clone());
    let sh = lock(s);
    let st = settings();
    // 1. No split-brain.
    for t in TENANTS {
        let moves: Vec<&(Duration, u64, String, u64)> =
            sh.moves.iter().filter(|m| m.1 == t).collect();
        for a in sh.acks.iter().filter(|a| a.tenant == t) {
            // The home at the ack's time, from the directory's history.
            let home = moves
                .iter()
                .rev()
                .find(|m| m.0 <= a.at)
                .map_or("fra1", |m| m.2.as_str());
            if home != a.dc {
                return Err(format!(
                    "split brain: tenant {t} value {} acknowledged by {} at {:?}, home was {home} (moves {moves:?})",
                    a.value, a.dc, a.at
                )
                .into());
            }
        }
    }
    // 2. Conditions of every promotion.
    for m in &sh.moves {
        let (at, t, to, _) = m;
        let gone = sh
            .lease_gone
            .iter()
            .rev()
            .find(|(g, d)| d == "fra1" && g <= at);
        let Some((gone_at, _)) = gone else {
            return Err(format!(
                "tenant {t} moved to {to} at {at:?} while fra1's lease never vanished"
            )
            .into());
        };
        if *at < *gone_at + st.clock_drift_margin {
            return Err(format!("tenant {t} moved at {at:?}, under the drift margin after the lease vanished at {gone_at:?}").into());
        }
        let rec = sh.records.values().find(|r| {
            r.tenants
                .iter()
                .any(|x| x.tenant_id == *t && x.to.as_ref().map(DcId::as_str) == Some(to.as_str()))
        });
        let Some(rec) = rec else {
            return Err(format!("tenant {t} moved without a failover record").into());
        };
        let downs = rec
            .votes
            .iter()
            .filter(|v| matches!(v.verdict, VerdictKind::Down | VerdictKind::Unsafe))
            .count();
        if downs + 1 < st.min_sites as usize
            || rec.votes.iter().any(|v| v.verdict == VerdictKind::Alive)
        {
            return Err(format!("tenant {t} promoted without agreement: {:?}", rec.votes).into());
        }
        let line = rec
            .tenants
            .iter()
            .find(|x| x.tenant_id == *t)
            .expect("line");
        if u128::from(line.lag_ms.unwrap_or(u32::MAX)) > st.rpo.as_millis() {
            return Err(format!(
                "tenant {t} promoted with lag {:?} over the RPO",
                line.lag_ms
            )
            .into());
        }
    }
    // 4. Residency.
    for (at, t, to, _) in &sh.moves {
        if *t == PINNED {
            return Err(
                format!("tenant {PINNED}, allowed only fra1, moved to {to} at {at:?}").into(),
            );
        }
        if *t == LATE_PIN
            && to != "fra1"
            && let Some(p) = sh.late_pin_at
            && *at > p
        {
            return Err(format!(
                "tenant {LATE_PIN} moved to {to} at {at:?}, after it was restricted to fra1 at {p:?}"
            )
            .into());
        }
    }
    for r in sh.records.values() {
        if let Some(x) = r.tenants.iter().find(|x| x.tenant_id == PINNED)
            && x.to.is_some()
        {
            return Err(format!("the failover record promotes tenant {PINNED}: {x:?}").into());
        }
    }
    // 3. The return.
    let moved: Vec<u64> = TENANTS
        .iter()
        .copied()
        .filter(|t| sh.moves.iter().any(|m| m.1 == *t))
        .collect();
    if !moved.is_empty() {
        let kv = store
            .get(&dc_key(&dc("fra1")).expect("k"), Consistency::Linearizable)
            .await?
            .expect("dc");
        let state = Record::<DcRecord>::decode(&kv.value)?.body.state;
        if state != "active" {
            return Err(format!("fra1 is {state} 60 s after healing, not active again").into());
        }
        let fra1 = TierView::over(&store, Tier::Dc(dc("fra1")));
        for t in &moved {
            let c = &sh.dbs["fra1-db"].copies[t];
            if !c.fenced {
                return Err(format!("tenant {t} moved but fra1's copy isn't fenced").into());
            }
            let page = fra1
                .list(
                    &fra1.tier().key(&format!("shardmaps/{t}/"))?,
                    ListOpts::default(),
                )
                .await?;
            for kv in page.kvs {
                if Record::<ShardMap>::decode(&kv.value)?.body.role != MapRole::Quarantined {
                    return Err(format!("tenant {t}'s map in fra1 isn't quarantined").into());
                }
            }
            // Diverged transactions: what fra1 holds past what ams1 applied.
            let rec = sh.records.values().find(|r| {
                r.tenants
                    .iter()
                    .any(|x| x.tenant_id == *t && x.outcome == "promoted")
            });
            let Some(rec) = rec else { continue };
            let line = rec
                .tenants
                .iter()
                .find(|x| x.tenant_id == *t)
                .expect("line");
            let applied = line.applied.get("fra1-c1").cloned().unwrap_or_default();
            let pos = format!("{t}-{FRA1_SERVER}-{}", c.list.len());
            let d = diverged(&pos, &applied, u32::try_from(*t).unwrap_or(0));
            let applied_len: usize = applied
                .rsplit('-')
                .next()
                .and_then(|x| x.parse().ok())
                .unwrap_or(0);
            let extra = c.list.len().saturating_sub(applied_len);
            let count = d.as_ref().map_or(0, |d| d.count());
            if count != extra as u64 {
                return Err(
                    format!("tenant {t}: report counts {count}, fra1 holds {extra} extra").into(),
                );
            }
            let ams_vals: Vec<u64> = sh.dbs["ams1-db"].copies[t]
                .list
                .iter()
                .map(|(_, v)| *v)
                .collect();
            let diverged_vals: Vec<u64> = c.list[applied_len.min(c.list.len())..]
                .iter()
                .map(|(_, v)| *v)
                .collect();
            for a in sh.acks.iter().filter(|a| a.tenant == *t && a.dc == "fra1") {
                if !ams_vals.contains(&a.value) && !diverged_vals.contains(&a.value) {
                    return Err(format!(
                        "tenant {t}: acknowledged value {} is lost and not reported",
                        a.value
                    )
                    .into());
                }
            }
            world.record(format!("tenant {t}: {extra} diverged, reported"));
        }
    }
    world.record(format!(
        "checked {} acks, {} moves, {} records",
        sh.acks.len(),
        sh.moves.len(),
        sh.records.len()
    ));
    if sh.acks.is_empty() {
        return Err("no write was ever acknowledged".into());
    }
    Ok(())
}

#[test]
fn no_split_brain_under_partitions_pauses_and_skew() {
    Sim::new("dc_failover")
        .seeds(40)
        .max_time(Duration::from_secs(600))
        .run_with_faults(plan, scenario);
}

/// The plain case, every time: fra1 cut from the global tier for 40 s.
/// The tenants fail over to ams1, fra1 fences itself first, returns,
/// fences the moved tenants and comes back active; the report counts what
/// only fra1 holds.
#[test]
fn a_dc_cut_from_the_global_tier_fails_over_and_returns_fenced() {
    Sim::new("dc_failover_plain")
        .seeds(5)
        .max_time(Duration::from_secs(600))
        .run(|world| async move {
            let s: S = Arc::new(Mutex::new(Shared::default()));
            seed(&world, &s).await;
            start(&world, &s);
            world.sleep(Duration::from_secs(15)).await;
            let cut = FaultPlan {
                faults: vec![PlannedFault {
                    at_ms: 0,
                    fault: FaultKind::Partition {
                        a: group("fra1"),
                        b: vec![n("etcd")],
                        for_ms: 40_000,
                    },
                }],
            };
            world.run_plan(&cut).await;
            world.heal_all().await;
            world.sleep(Duration::from_secs(60)).await;
            check(&world, &s).await?;
            let sh = lock(&s);
            // Every tenant but the pinned one fails over (tenant 4 isn't
            // restricted in this test).
            if sh.moves.iter().filter(|m| m.2 == "ams1").count() != TENANTS.len() - 1
                || sh.moves.iter().any(|m| m.1 == PINNED)
            {
                return Err(format!(
                    "expected every tenant but {PINNED} to fail over: {:?}",
                    sh.moves
                )
                .into());
            }
            let skipped = sh.records.values().any(|r| {
                r.tenants.iter().any(|x| {
                    x.tenant_id == PINNED
                        && x.to.is_none()
                        && x.reason.as_deref().is_some_and(|w| w.contains("residency"))
                })
            });
            if !skipped {
                return Err(format!(
                    "tenant {PINNED} isn't skipped for residency in the record: {:?}",
                    sh.records
                )
                .into());
            }
            let done = sh.records.values().any(|r| r.state == FailoverState::Done);
            if !done {
                return Err(format!("no finished failover: {:?}", sh.records).into());
            }
            // ams1 took writes after the move.
            if !sh.acks.iter().any(|a| a.dc == "ams1") {
                return Err("the new home never acknowledged a write".into());
            }
            Ok(())
        });
}

/// Two sites only (the witness gone): never automatic.
#[test]
fn with_two_sites_there_is_no_automatic_failover() {
    Sim::new("dc_failover_two_sites")
        .seeds(3)
        .max_time(Duration::from_secs(600))
        .run(|world| async move {
            let s: S = Arc::new(Mutex::new(Shared::default()));
            seed(&world, &s).await;
            start(&world, &s);
            world.faults().crash(&n("wit1-c1"));
            world.sleep(Duration::from_secs(15)).await;
            let cut = FaultPlan {
                faults: vec![PlannedFault {
                    at_ms: 0,
                    fault: FaultKind::Partition {
                        a: group("fra1"),
                        b: vec![n("etcd")],
                        for_ms: 40_000,
                    },
                }],
            };
            world.run_plan(&cut).await;
            let sh = lock(&s);
            if !sh.moves.is_empty() {
                return Err(format!("moved with two sites: {:?}", sh.moves).into());
            }
            if !sh
                .records
                .values()
                .any(|r| r.state == FailoverState::ManualOnly)
            {
                return Err(format!("expected manual_only: {:?}", sh.records).into());
            }
            Ok(())
        });
}
