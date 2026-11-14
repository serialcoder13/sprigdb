//! DC failover with real processes, a real etcd and real MariaDB,
//! Jepsen-style. Set
//! `SPRIG_TEST_DOCKER=1` (and `SPRIG_TEST_TOXIPROXY`, the dev toxiproxy
//! API) to run; `SPRIG_TEST_ELLE=1` adds elle-cli's check of the merged
//! history.
//!
//! Three simulated DCs run as three child processes of this test binary
//! (`fra1`, `ams1`, the witness `wit1`). Each has a controller keeping its
//! DC lease in the test's own etcd container, reached through its own
//! toxiproxy route, and serving permits, `Peer.Ping` and `ObserveDc`;
//! `fra1` and `ams1` also run an agent (the writer's `read_only` from the
//! permit) and gateway clients running list-append transactions against
//! their DC's MariaDB with an application account `read_only` binds,
//! acknowledging only while their permit is still valid. `fra1-db` is the
//! primary of tenant 2830600001 (standby in `ams1`, semi-sync AFTER_SYNC)
//! and of tenant 2830600002 (pinned to `fra1`). The parent runs the global
//! leader's `GlobalFailover`.
//!
//! - [`dc_cut_from_the_global_tier`]: `fra1`'s route to etcd is disabled
//!   for 30 s;
//! - [`dc_frozen_by_a_process_pause`]: the whole `fra1` process is
//!   `SIGSTOP`ped for 30 s (its agent can't fence; in-flight writes resume
//!   after the promotion and must not be acknowledged).
//!
//! Checked: the first tenant moves to `ams1` only after the lease vanished,
//! with two `down` votes; the pinned tenant stays; no write is acknowledged
//! by a DC after the commit point moved its tenant away; every
//! acknowledged append is in the tenant's final home; `fra1` returns
//! fenced (the moved tenant's grants revoked), its diverged transactions
//! are reported and hold no acknowledged write; its gateways then admit a
//! 3,000-client reconnect at the stepped rate with 39531 hints of 1-5 s;
//! elle finds the merged history read-committed.

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
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mysql_async::prelude::Queryable;
use serde_json::{Map, Value, json};
use sprig_catalog::placement::{MapRole, ShardMap};
use sprig_cluster::failover::primary_replica::{diverged, diverged_events, fence_node};
use sprig_cluster::sql::NodeEndpoint;
use sprig_common::ids::{ClusterId, DcId, NodeId, TenantId};
use sprig_common::time::{Clock, SystemClock};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_controller::failover::FailoverSettings;
use sprig_controller::failover::client::{HttpDcProbe, HttpObserverClient, HttpPermitSource};
use sprig_controller::failover::global::{GlobalFailover, Outcome};
use sprig_controller::failover::observe::{ObserveTargets, WriterTarget, targets_key};
use sprig_controller::failover::promote::{Guard, PromoteHooks, Promotion};
use sprig_controller::failover::record::{FailoverRecord, FailoverState, StandbyLag, VerdictKind};
use sprig_controller::failover::recovery::{
    Admission, CopyFence, ReturnStep, ReturningDc, TrafficSteps, errant_report,
};
use sprig_controller::failover::service::{Internal, serve};
use sprig_meta::bootstrap::DcRecord;
use sprig_meta::dclease::{DcLease, dc_key};
use sprig_meta::permit::{PermitHolder, PermitSource};
use sprig_meta::{Consistency, EtcdConfig, EtcdStore, MetaStore, Op, Record, Tier, TierView, Txn};
use sprig_testkit::faults::compose::{OwnContainer, docker};
use sprig_testkit::faults::elle::{ElleCli, ElleModel};
use sprig_testkit::faults::history::{OpKind, OpRecord, read_jsonl, write_jsonl};
use sprig_testkit::faults::toxiproxy::Toxiproxy;
use sprig_testkit::skip;

const ROOT_PW: &str = "sprig-sim-root";
const REPL_PW: &str = "sprig-sim-repl";
const APP_PW: &str = "sprig-sim-app";
/// This test's tenant ids start at 2,830,600,000.
const T_MOVES: u64 = 2_830_600_001;
const T_PINNED: u64 = 2_830_600_002;
const KEYS: u64 = 6;
/// Controllers' internal ports (this test's 28316-28337 range).
const PORTS: [(&str, u16); 3] = [("fra1", 28316), ("ams1", 28317), ("wit1", 28318)];

fn schema(t: u64) -> &'static str {
    if t == T_MOVES {
        "r2failover_t1"
    } else {
        "r2failover_t2"
    }
}

fn key_base(t: u64) -> u64 {
    if t == T_MOVES { 0 } else { 100 }
}

fn port(dc: &str) -> u16 {
    PORTS.iter().find(|(d, _)| *d == dc).map_or(0, |(_, p)| *p)
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(0))
}

fn env(k: &str) -> String {
    std::env::var(k).unwrap_or_default()
}

fn opts(url: &str) -> mysql_async::Opts {
    let o = mysql_async::Opts::from_url(url).expect("url");
    mysql_async::OptsBuilder::from_opts(o)
        .prefer_socket(false)
        .into()
}

async fn exec(url: &str, sqls: &[String]) -> std::result::Result<(), mysql_async::Error> {
    let mut c = mysql_async::Conn::new(opts(url)).await?;
    for s in sqls {
        c.query_drop(s.as_str()).await?;
    }
    let _ = c.disconnect().await;
    Ok(())
}

async fn scalar(url: &str, sql: &str) -> Option<String> {
    let mut c = tokio::time::timeout(Duration::from_secs(2), mysql_async::Conn::new(opts(url)))
        .await
        .ok()?
        .ok()?;
    let v: Option<Option<String>> = c.query_first(sql).await.ok();
    let _ = c.disconnect().await;
    v.flatten()
}

async fn put(store: &dyn MetaStore, key: sprig_meta::Key, value: Vec<u8>) -> Result<()> {
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
        .map(drop)
}

async fn connect_etcd(endpoint: &str) -> Arc<dyn MetaStore> {
    let mut cfg = EtcdConfig::new(vec![endpoint.to_owned()]);
    cfg.request_timeout = Duration::from_secs(1);
    cfg.connect_timeout = Duration::from_secs(1);
    loop {
        match EtcdStore::connect(Tier::Global, &cfg).await {
            Ok(s) => return Arc::new(s),
            Err(_) => tokio::time::sleep(Duration::from_millis(300)).await,
        }
    }
}

// ============================================================================
// The DC process (a child of this test binary).
// ============================================================================

/// Revokes the moved tenant's write grants on the returning DC's copy.
struct RevokeFence {
    root: String,
}

#[async_trait::async_trait]
impl CopyFence for RevokeFence {
    async fn fence(&self, t: TenantId) -> Result<()> {
        exec(
            &self.root,
            &[
                "SET SESSION sql_log_bin = 0".into(),
                format!(
                    "REVOKE INSERT, UPDATE, DELETE ON {}.* FROM 'r2fo_app'@'%'",
                    schema(t.0)
                ),
            ],
        )
        .await
        .or_else(|e| {
            if e.to_string().contains("1141") {
                Ok(())
            } else {
                Err(SprigError::internal(e.to_string()))
            }
        })
    }
}

async fn home_of(store: &dyn MetaStore, t: u64) -> Option<String> {
    let k = Tier::Global.key(&format!("tenants/{t}")).ok()?;
    let kv = tokio::time::timeout(
        Duration::from_secs(1),
        store.get(&k, Consistency::Linearizable),
    )
    .await
    .ok()?
    .ok()??;
    let v: Value = serde_json::from_slice(&kv.value).ok()?;
    // Writes wait while a promotion finishes (`failing_over`).
    if v["state"] != "active" {
        return Some("(moving)".to_owned());
    }
    v["home_dc"].as_str().map(str::to_owned)
}

#[test]
fn child_dc() {
    if env("R2FO_ROLE") != "dc" {
        return;
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("rt");
    rt.block_on(run_dc());
}

#[allow(clippy::too_many_lines)] // one process, every role
async fn run_dc() {
    let me = env("R2FO_DC");
    let d = DcId::new(&me).expect("dc");
    let work = PathBuf::from(env("R2FO_WORK"));
    let root = env("R2FO_DB");
    let app = env("R2FO_APP");
    let db_addr = env("R2FO_DB_ADDR");
    let store = connect_etcd(&env("R2FO_ETCD")).await;
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let lease = Arc::new(DcLease::new(
        d.clone(),
        format!("{me}-c1"),
        Arc::clone(&store),
        Arc::clone(&clock),
        FailoverSettings::default().lease_opts(),
    ));
    let lag = Arc::new(AtomicU32::new(u32::MAX));
    let steps: Arc<Mutex<Option<TrafficSteps>>> = Arc::new(Mutex::new(None));
    let settings = FailoverSettings {
        observer_probe_interval: Duration::from_millis(500),
        ..FailoverSettings::default()
    };
    let (lag2, steps2) = (Arc::clone(&lag), Arc::clone(&steps));
    let me2 = me.clone();
    let internal = Arc::new(Internal {
        lease: Arc::clone(&lease),
        probe: Arc::new(HttpDcProbe::new(Arc::clone(&store), Duration::from_secs(1))),
        settings,
        lags: Arc::new(move || {
            if me2 == "ams1" {
                vec![StandbyLag {
                    tenant_id: T_MOVES,
                    lag_ms: lag2.load(Ordering::SeqCst),
                }]
            } else {
                vec![]
            }
        }),
        admit_per_second: Arc::new(move || {
            let mut s = steps2.lock().expect("lock");
            match s.as_mut() {
                Some(st) => {
                    st.tick(Instant::now(), true);
                    st.admit_per_second(200)
                }
                None => 0,
            }
        }),
        fenced_writers: Arc::new(Vec::new),
    });
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port(&me)))
        .await
        .expect("bind");
    let _svc = serve(listener, Arc::clone(&internal));
    let base = format!("http://127.0.0.1:{}", port(&me));
    let targets = ObserveTargets {
        controllers: vec![base.clone()],
        writers: if db_addr.is_empty() {
            vec![]
        } else {
            vec![WriterTarget {
                cluster: format!("{me}-c1"),
                addr: db_addr.clone(),
                self_fences: true,
            }]
        },
    };
    let stop = Arc::new(AtomicBool::new(false));
    let records: Arc<Mutex<Vec<OpRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let log = |s: String| {
        eprintln!("[{me} {}] {s}", now_ns() / 1_000_000 % 1_000_000);
    };

    // Controller: the lease, the observe targets, the return.
    let l2 = Arc::clone(&lease);
    let keep = tokio::spawn(async move { l2.run(|| true).await });
    let (st2, t2) = (Arc::clone(&store), targets.clone());
    let me3 = me.clone();
    let publish = tokio::spawn(async move {
        loop {
            if let (Ok(k), Ok(v)) = (
                targets_key(&DcId::new(&me3).expect("dc")),
                Record::new(t2.clone()).encode(),
            ) {
                let _ = put(&*st2, k, v).await;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
    let mut tasks = vec![keep, publish];
    if !root.is_empty() {
        let ret = ReturningDc {
            dc: d.clone(),
            global: Arc::clone(&store),
            tier: TierView::over(&store, Tier::Dc(d.clone())),
            fence: Arc::new(RevokeFence { root: root.clone() }),
        };
        let steps3 = Arc::clone(&steps);
        let me4 = me.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                match ret.step().await {
                    Ok(ReturnStep::Unfenced { fenced }) => {
                        eprintln!("[{me4}] returned: fenced {fenced:?}; traffic returns in steps");
                        *steps3.lock().expect("lock") = Some(TrafficSteps::new(
                            vec![10, 25, 50, 100],
                            Duration::from_secs(3),
                            Instant::now(),
                        ));
                    }
                    Ok(ReturnStep::NotReturning) => {}
                    Err(e) => eprintln!("[{me4}] return step: {e}"),
                }
            }
        }));
    }
    // Standby lag (ams1): from the replica's status.
    if me == "ams1" {
        let (root2, lag3) = (root.clone(), Arc::clone(&lag));
        tasks.push(tokio::spawn(async move {
            loop {
                let l = match scalar(&root2, "SELECT IF((SELECT COUNT(*) FROM information_schema.SLAVE_STATUS WHERE Slave_SQL_Running='Yes' AND Slave_IO_Running='Yes') = 1, 0, 4294967295)").await {
                    Some(v) => v.parse::<u64>().unwrap_or(u64::from(u32::MAX)),
                    None => u64::from(u32::MAX),
                };
                lag3.store(u32::try_from(l).unwrap_or(u32::MAX), Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }));
    }

    let tenants: Vec<u64> = if root.is_empty() {
        vec![]
    } else {
        vec![T_MOVES, T_PINNED]
    };
    let homes: Arc<RwLock<BTreeMap<u64, String>>> = Arc::new(RwLock::new(
        tenants.iter().map(|t| (*t, "fra1".to_owned())).collect(),
    ));
    if !root.is_empty() {
        // Directory cache, as a gateway follows its tenants.
        let (st3, h2, ts) = (Arc::clone(&store), Arc::clone(&homes), tenants.clone());
        tasks.push(tokio::spawn(async move {
            loop {
                for t in &ts {
                    if let Some(h) = home_of(&*st3, *t).await {
                        h2.write().expect("lock").insert(*t, h);
                    }
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        }));
        // Agent: read_only follows the permit and whether a tenant is homed here.
        let agent = Arc::new(PermitHolder::new(
            format!("{me}-agent"),
            "agent",
            Arc::clone(&clock),
        ));
        let src: Vec<Arc<dyn PermitSource>> = vec![Arc::new(HttpPermitSource::new(
            &base,
            Duration::from_millis(800),
        ))];
        let a2 = Arc::clone(&agent);
        tasks.push(tokio::spawn(async move {
            a2.run(src, Duration::from_secs(1)).await
        }));
        let (h3, root3, me5) = (Arc::clone(&homes), root.clone(), me.clone());
        let ep = NodeEndpoint::from_url(NodeId::new(format!("{me}-db")).expect("id"), &root)
            .expect("ep");
        tasks.push(tokio::spawn(async move {
            let mut fenced: Option<bool> = None;
            loop {
                let serve =
                    agent.writable() && h3.read().expect("lock").values().any(|h| *h == me5);
                if fenced != Some(!serve) || !serve {
                    let r = if serve {
                        exec(&root3, &["SET GLOBAL read_only = OFF".into()])
                            .await
                            .map_err(|e| e.to_string())
                    } else {
                        fence_node(&ep).await.map(drop).map_err(|e| e.to_string())
                    };
                    if r.is_ok() && fenced != Some(!serve) {
                        eprintln!("[{me5}] agent: read_only {}", !serve);
                        fenced = Some(!serve);
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }));
        // Gateway: clients per tenant.
        let gw = Arc::new(PermitHolder::new(
            format!("{me}-gw"),
            "gateway",
            Arc::clone(&clock),
        ));
        let src: Vec<Arc<dyn PermitSource>> = vec![Arc::new(HttpPermitSource::new(
            &base,
            Duration::from_millis(800),
        ))];
        let g2 = Arc::clone(&gw);
        tasks.push(tokio::spawn(async move {
            g2.run(src, Duration::from_secs(1)).await
        }));
        let base_proc: u32 = env("R2FO_PROC").parse().unwrap_or(0);
        let next = Arc::new(AtomicU64::new(u64::from(base_proc) * 1_000_000 + 1));
        let mut clients = Vec::new();
        for (i, t) in tenants.iter().copied().enumerate() {
            for c in 0..3u32 {
                let proc_id = base_proc + u32::try_from(i).unwrap_or(0) * 10 + c;
                let (gw, homes, stop, records, next, app, me6) = (
                    Arc::clone(&gw),
                    Arc::clone(&homes),
                    Arc::clone(&stop),
                    Arc::clone(&records),
                    Arc::clone(&next),
                    app.clone(),
                    me.clone(),
                );
                clients.push(tokio::spawn(async move {
                    client(proc_id, t, &me6, &app, &gw, &homes, &stop, &records, &next).await;
                }));
            }
        }
        // The reconnect storm, once the DC returned.
        let (gw3, steps4, work2, me7) = (
            Arc::clone(&gw),
            Arc::clone(&steps),
            work.clone(),
            me.clone(),
        );
        tasks.push(tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(200)).await;
                if steps4.lock().expect("lock").is_none() || gw3.admit_per_second() == 0 {
                    continue;
                }
                let rate = gw3.admit_per_second();
                let mut adm = Admission::new(Instant::now(), 42);
                let (mut ok, mut refused, mut hints) = (0u32, 0u32, Vec::new());
                let t0 = Instant::now();
                for i in 0..3000u32 {
                    // 3,000 clients within one second.
                    let at = t0 + Duration::from_micros(u64::from(i) * 333);
                    match adm.admit(at, rate) {
                        Ok(()) => ok += 1,
                        Err(e) => {
                            refused += 1;
                            assert_eq!(e.code(), Some(SprigCode::AdmissionBackoff));
                            if let Some(ms) = e
                                .message
                                .rsplit("retry in ")
                                .next()
                                .and_then(|s| s.trim_end_matches(" ms").parse::<u64>().ok())
                            {
                                hints.push(ms);
                            }
                        }
                    }
                }
                let out = json!({"rate": rate, "admitted": ok, "refused": refused,
                    "hint_min_ms": hints.iter().min(), "hint_max_ms": hints.iter().max()});
                eprintln!("[{me7}] reconnect storm: {out}");
                let _ = std::fs::write(work2.join(format!("{me7}-storm.json")), out.to_string());
                return;
            }
        }));
        let _ = clients;
    }
    // Run until the parent says stop.
    while !work.join("stop").exists() {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    stop.store(true, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(5)).await;
    let recs = records.lock().expect("lock").clone();
    write_jsonl(&work.join(format!("{me}.jsonl")), &recs).expect("history");
    log(format!("wrote {} records", recs.len()));
    for t in tasks {
        t.abort();
    }
}

#[allow(clippy::too_many_arguments)]
async fn client(
    p: u32,
    t: u64,
    me: &str,
    app: &str,
    gw: &PermitHolder,
    homes: &RwLock<BTreeMap<u64, String>>,
    stop: &AtomicBool,
    records: &Mutex<Vec<OpRecord>>,
    next: &AtomicU64,
) {
    let rec = |kind: OpKind, v: Value| {
        records.lock().expect("lock").push(OpRecord {
            process: p,
            kind,
            f: "txn".into(),
            value: v,
            time_ns: now_ns(),
        });
    };
    let mut conn: Option<mysql_async::Conn> = None;
    let mut seed = u64::from(p) * 7919 + 17;
    let mut rnd = move |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    while !stop.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let home = homes
            .read()
            .expect("lock")
            .get(&t)
            .cloned()
            .unwrap_or_default();
        if home != me || !gw.writable() {
            // 39501 for writes homed here without a permit; elsewhere the
            // gateway would forward.
            continue;
        }
        if conn.is_none() {
            conn = tokio::time::timeout(Duration::from_secs(2), mysql_async::Conn::new(opts(app)))
                .await
                .ok()
                .and_then(std::result::Result::ok);
            let Some(c) = conn.as_mut() else { continue };
            let _ = c
                .query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                .await;
        }
        let n = 1 + rnd(3);
        let mops: Vec<(bool, u64, u64)> = (0..n)
            .map(|_| {
                let k = key_base(t) + rnd(KEYS);
                if rnd(2) == 0 {
                    (true, k, next.fetch_add(1, Ordering::SeqCst))
                } else {
                    (false, k, 0)
                }
            })
            .collect();
        let invoke: Vec<Value> = mops
            .iter()
            .map(|(a, k, v)| {
                if *a {
                    json!(["append", k, v])
                } else {
                    json!(["r", k, null])
                }
            })
            .collect();
        rec(OpKind::Invoke, Value::Array(invoke.clone()));
        let Some(c) = conn.as_mut() else { continue };
        let r = tokio::time::timeout(Duration::from_secs(30), txn(c, schema(t), &mops)).await;
        match r {
            // Acknowledge only while the permit still holds.
            Ok(Ok(done)) if gw.writable() => rec(OpKind::Ok, Value::Array(done)),
            Ok(Ok(_)) => {
                eprintln!("[{me}] committed after the permit lapsed: not acknowledged");
                rec(OpKind::Info, Value::Array(invoke));
            }
            Ok(Err(mysql_async::Error::Server(e)))
                if matches!(e.code, 1020 | 1205 | 1213 | 1290 | 1142) =>
            {
                let _ = c.query_drop("ROLLBACK").await;
                rec(OpKind::Fail, Value::Array(invoke));
                if e.code == 1290 || e.code == 1142 {
                    conn = None;
                }
            }
            _ => {
                rec(OpKind::Info, Value::Array(invoke));
                conn = None;
            }
        }
    }
}

async fn txn(
    c: &mut mysql_async::Conn,
    schema: &str,
    mops: &[(bool, u64, u64)],
) -> std::result::Result<Vec<Value>, mysql_async::Error> {
    c.query_drop("BEGIN").await?;
    let mut done = Vec::new();
    for (append, k, v) in mops {
        if *append {
            c.exec_drop(format!("INSERT INTO {schema}.la (k, v) VALUES (?, ?) ON DUPLICATE KEY UPDATE v = CONCAT(v, ',', VALUES(v))"), (k, v.to_string())).await?;
            done.push(json!(["append", k, v]));
        } else {
            let cur: Option<String> = c
                .exec_first(format!("SELECT v FROM {schema}.la WHERE k = ?"), (k,))
                .await?;
            let list: Vec<u64> = cur
                .unwrap_or_default()
                .split(',')
                .filter_map(|x| x.parse().ok())
                .collect();
            done.push(json!(["r", k, list]));
        }
    }
    c.query_drop("COMMIT").await?;
    Ok(done)
}

// ============================================================================
// The parent: containers, etcd, the global leader, faults, checks.
// ============================================================================

struct Net(String);

impl Drop for Net {
    fn drop(&mut self) {
        let _ = docker(&["network", "rm", &self.0]);
    }
}

struct Children(Vec<(String, Child)>);

impl Drop for Children {
    fn drop(&mut self) {
        for (_, c) in &mut self.0 {
            let _ = Command::new("kill")
                .args(["-CONT", &c.id().to_string()])
                .status();
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .expect("port")
}

fn mariadb(net: &str, name: &str, sid: u32, master: Option<&str>) -> (OwnContainer, u16) {
    let dir = sprig_testkit::compose::compose_dir().join("mariadb");
    let binlog = format!(
        "{}:/etc/mysql/conf.d/10-binlog.cnf:ro",
        dir.join("binlog.cnf").display()
    );
    let semi = format!(
        "{}:/etc/mysql/conf.d/20-semi-sync.cnf:ro",
        dir.join("semi-sync.cnf").display()
    );
    let p = free_port();
    let publish = format!("127.0.0.1:{p}:3306");
    let root = format!("MARIADB_ROOT_PASSWORD={ROOT_PW}");
    let rpw = format!("MARIADB_REPLICATION_PASSWORD={REPL_PW}");
    let sid = format!("--server-id={sid}");
    let m = master.map(|m| format!("MARIADB_MASTER_HOST={m}"));
    let mut args: Vec<&str> = vec![
        "--memory",
        "768m",
        "--network",
        net,
        "--network-alias",
        name,
        "-p",
        &publish,
        "-e",
        &root,
        "-e",
        "MARIADB_REPLICATION_USER=sprig_repl",
        "-e",
        &rpw,
        "-v",
        &binlog,
        "-v",
        &semi,
    ];
    if let Some(m) = &m {
        args.extend(["-e", m]);
    }
    args.extend([
        "mariadb:11.8",
        &sid,
        "--gtid-domain-id=1",
        "--innodb-buffer-pool-size=64M",
        "--read-only=ON",
    ]);
    if master.is_none() {
        args.push("--rpl-semi-sync-master-enabled=ON");
    }
    (
        OwnContainer::run(&format!("r2fo-{name}"), &args).expect("mariadb"),
        p,
    )
}

async fn wait_for(url: &str, sql: &str, want: &str, within: Duration) {
    let t0 = Instant::now();
    while t0.elapsed() < within {
        if scalar(url, sql).await.as_deref() == Some(want) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("{url}: {sql} never gave {want}");
}

/// Promotion's data-path side for the standby in `ams1`.
struct LiveHooks {
    ams1_root: String,
}

#[async_trait::async_trait]
impl PromoteHooks for LiveHooks {
    async fn stop_standby(&self, _t: TenantId, _to: &DcId) -> Result<BTreeMap<String, String>> {
        let e = |e: mysql_async::Error| SprigError::internal(e.to_string());
        let mut c = mysql_async::Conn::new(opts(&self.ams1_root))
            .await
            .map_err(e)?;
        c.query_drop("STOP SLAVE IO_THREAD").await.map_err(e)?;
        let io: Option<String> = c
            .query_first("SELECT Gtid_IO_Pos FROM information_schema.SLAVE_STATUS")
            .await
            .map_err(e)?;
        if let Some(io) = io.filter(|s| !s.is_empty()) {
            let _: Option<i64> = c
                .query_first(format!("SELECT MASTER_GTID_WAIT('{io}', 30)"))
                .await
                .map_err(e)?;
        }
        c.query_drop("STOP SLAVE").await.map_err(e)?;
        let pos: Option<String> = c.query_first("SELECT @@gtid_slave_pos").await.map_err(e)?;
        let _ = c.disconnect().await;
        Ok(BTreeMap::from([(
            "fra1-c1".to_owned(),
            pos.unwrap_or_default(),
        )]))
    }

    async fn make_writable(&self, _t: TenantId, _to: &DcId) -> Result<()> {
        exec(
            &self.ams1_root,
            &[
                "RESET SLAVE ALL".into(),
                "SET GLOBAL rpl_semi_sync_master_enabled = ON".into(),
            ],
        )
        .await
        .map_err(|e| SprigError::internal(e.to_string()))
    }
}

async fn seed(store: &Arc<dyn MetaStore>) {
    for (name, kind) in [("fra1", "full"), ("ams1", "full"), ("wit1", "witness")] {
        let rec = Record::new(DcRecord {
            dc_id: DcId::new(name).expect("dc"),
            kind: kind.into(),
            state: "active".into(),
            dc_epoch: 0,
            tags: Map::new(),
            global_voter: true,
        });
        put(
            &**store,
            dc_key(&DcId::new(name).expect("dc")).expect("k"),
            rec.encode().expect("e"),
        )
        .await
        .expect("put");
    }
    for (t, extra) in [
        (T_MOVES, json!({"standby_dcs": ["ams1"]})),
        (T_PINNED, json!({"pinned_dc": "fra1"})),
    ] {
        let mut e = json!({"v": 1, "name": format!("r2failover-t{t}"), "state": "active", "home_dc": "fra1", "epoch": 1});
        e.as_object_mut()
            .expect("obj")
            .extend(extra.as_object().expect("obj").clone());
        put(
            &**store,
            Tier::Global.key(&format!("tenants/{t}")).expect("k"),
            serde_json::to_vec(&e).expect("j"),
        )
        .await
        .expect("put");
        for (d, role) in [("fra1", MapRole::Home), ("ams1", MapRole::Standby)] {
            if t == T_PINNED && d == "ams1" {
                continue;
            }
            let tier = TierView::over(store, Tier::Dc(DcId::new(d).expect("dc")));
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
            .await
            .expect("put");
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    CutFromGlobal,
    Freeze,
}

async fn final_lists(url: &str, t: u64) -> BTreeMap<u64, Vec<u64>> {
    let mut c = mysql_async::Conn::new(opts(url)).await.expect("connect");
    let rows: Vec<(u64, String)> = c
        .query(format!("SELECT k, v FROM {}.la", schema(t)))
        .await
        .expect("read");
    let _ = c.disconnect().await;
    rows.into_iter()
        .map(|(k, v)| (k, v.split(',').filter_map(|x| x.parse().ok()).collect()))
        .collect()
}

fn write_marker(p: &Path) {
    let _ = std::fs::File::create(p).and_then(|mut f| f.write_all(b"stop"));
}

#[allow(clippy::too_many_lines)] // one scenario, start to end
/// The scenarios share the controllers' fixed ports: one at a time.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

async fn scenario(fault: Fault) {
    let _one = ONE_AT_A_TIME
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(tox_url) = skip::var(skip::TOXIPROXY) else {
        skip::report_missing(skip::TOXIPROXY, "live_dc_failover");
        return;
    };
    let tox = Toxiproxy::new(&tox_url).expect("toxiproxy");
    let tox_net = docker(&[
        "inspect",
        "-f",
        "{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}",
        "sprigdb-dev-toxiproxy-1",
    ])
    .expect("net");
    let tox_net = tox_net
        .split_whitespace()
        .next()
        .expect("toxiproxy network")
        .to_owned();
    let work = std::env::temp_dir().join(format!("r2fo-{}-{:?}", std::process::id(), fault));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).expect("work dir");

    // etcd, on toxiproxy's network so each DC reaches it through a route.
    let ep = free_port();
    let etcd_pub = format!("127.0.0.1:{ep}:2379");
    let etcd = OwnContainer::run(
        "r2fo-etcd",
        &[
            "--memory",
            "256m",
            "--network",
            &tox_net,
            "-p",
            &etcd_pub,
            "quay.io/coreos/etcd:v3.6.5",
            "etcd",
            "--listen-client-urls=http://0.0.0.0:2379",
            "--advertise-client-urls=http://127.0.0.1:2379",
        ],
    )
    .expect("etcd");
    let direct = format!("http://127.0.0.1:{ep}");
    let store = connect_etcd(&direct).await;
    seed(&store).await;
    let mut routes = BTreeMap::new();
    for d in ["fra1", "ams1", "wit1"] {
        let p = tox
            .own_proxy(&format!("r2fo-{d}"), &format!("{}:2379", etcd.name()))
            .await
            .expect("route");
        routes.insert(d, p);
    }

    // MariaDB: fra1-db primary, ams1-db its semi-sync replica.
    let net = Net(format!("sprig-sim-r2fo-dc-{}-{:?}", std::process::id(), fault).to_lowercase());
    docker(&["network", "create", &net.0]).expect("network");
    let (fra1_db, fra1_port) = mariadb(&net.0, "fra1db", 101, None);
    let fra1_root = format!("mysql://root:{ROOT_PW}@127.0.0.1:{fra1_port}");
    wait_for(&fra1_root, "SELECT 1", "1", Duration::from_secs(120)).await;
    let mut setup = vec![
        "SET GLOBAL read_only = OFF".to_owned(),
        format!("CREATE USER 'r2fo_app'@'%' IDENTIFIED BY '{APP_PW}'"),
    ];
    for t in [T_MOVES, T_PINNED] {
        setup.push(format!("CREATE DATABASE {}", schema(t)));
        setup.push(format!(
            "CREATE TABLE {}.la (k INT PRIMARY KEY, v TEXT NOT NULL) ENGINE=InnoDB",
            schema(t)
        ));
        setup.push(format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON {}.* TO 'r2fo_app'@'%'",
            schema(t)
        ));
    }
    exec(&fra1_root, &setup).await.expect("setup");
    let (ams1_db, ams1_port) = mariadb(&net.0, "ams1db", 201, Some("fra1db"));
    let ams1_root = format!("mysql://root:{ROOT_PW}@127.0.0.1:{ams1_port}");
    wait_for(
        &ams1_root,
        "SELECT COUNT(*) FROM mysql.user WHERE user='r2fo_app'",
        "1",
        Duration::from_secs(120),
    )
    .await;
    wait_for(&fra1_root, "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME='Rpl_semi_sync_master_clients'", "1", Duration::from_secs(60)).await;
    exec(&fra1_root, &["SET GLOBAL read_only = ON".into()])
        .await
        .expect("fence until the agent unfences");
    eprintln!("etcd, routes and MariaDB up");

    // The three DC processes.
    let exe = std::env::current_exe().expect("exe");
    let mut kids = Children(Vec::new());
    for (d, root, db_port, proc_base) in [
        ("fra1", fra1_root.clone(), fra1_port, 100u32),
        ("ams1", ams1_root.clone(), ams1_port, 200),
        ("wit1", String::new(), 0, 300),
    ] {
        let app = if db_port == 0 {
            String::new()
        } else {
            format!("mysql://r2fo_app:{APP_PW}@127.0.0.1:{db_port}")
        };
        let child = Command::new(&exe)
            .args(["child_dc", "--exact", "--nocapture", "--test-threads=1"])
            .env("R2FO_ROLE", "dc")
            .env("R2FO_DC", d)
            .env("R2FO_WORK", &work)
            .env("R2FO_ETCD", format!("http://{}", routes[d].host_addr()))
            .env("R2FO_DB", &root)
            .env("R2FO_APP", app)
            .env(
                "R2FO_DB_ADDR",
                if db_port == 0 {
                    String::new()
                } else {
                    format!("127.0.0.1:{db_port}")
                },
            )
            .env("R2FO_PROC", proc_base.to_string())
            .spawn()
            .expect("spawn");
        kids.0.push((d.to_owned(), child));
    }

    // The global leader.
    let promotion = Promotion::single_etcd(
        &store,
        Guard::Store(Arc::clone(&store)),
        Arc::new(LiveHooks {
            ams1_root: ams1_root.clone(),
        }),
    );
    let settings = FailoverSettings {
        observer_probe_interval: Duration::from_millis(500),
        ..FailoverSettings::default()
    };
    let g = Arc::new(GlobalFailover::new(
        Arc::clone(&store),
        Arc::new(SystemClock),
        Arc::new(move |_: &DcId| settings.clone()),
        Arc::new(HttpObserverClient::new(
            Arc::clone(&store),
            Duration::from_secs(8),
        )),
        promotion,
    ));
    let gstop = Arc::new(AtomicBool::new(false));
    let (g2, gs) = (Arc::clone(&g), Arc::clone(&gstop));
    let leader = tokio::spawn(async move {
        while !gs.load(Ordering::SeqCst) {
            match g2.tick().await {
                Ok(out) => {
                    for o in out {
                        if let Outcome::Ended(f) = o {
                            eprintln!(
                                "[global] failover {} {:?} votes {:?}",
                                f.failover_id, f.state, f.votes
                            );
                        }
                    }
                }
                Err(e) => eprintln!("[global] tick: {e}"),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
    // Watch the truth: directory moves and lease deletions, at wall time.
    let moves: Arc<Mutex<Vec<(u64, u64, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let gone: Arc<Mutex<Vec<(u64, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let (m2, gn2, st2) = (Arc::clone(&moves), Arc::clone(&gone), Arc::clone(&store));
    let truth = tokio::spawn(async move {
        use futures::StreamExt;
        let mut w = st2.watch(&Tier::Global.root(), 0).await.expect("watch");
        while let Some(Ok(ev)) = w.next().await {
            match ev {
                sprig_meta::WatchEvent::Put(kv) => {
                    if let Some(t) = kv.key.as_str().strip_prefix("/sprig/g/tenants/") {
                        let v: Value = serde_json::from_slice(&kv.value).unwrap_or_default();
                        let h = v["home_dc"].as_str().unwrap_or_default().to_owned();
                        let t: u64 = t.parse().unwrap_or(0);
                        let mut m = m2.lock().expect("lock");
                        if m.iter()
                            .rev()
                            .find(|x| x.1 == t)
                            .map_or("fra1", |x| x.2.as_str())
                            != h
                        {
                            m.push((now_ns(), t, h));
                        }
                    }
                }
                sprig_meta::WatchEvent::Delete { key, .. } => {
                    if let Some(d) = key.as_str().strip_prefix("/sprig/g/dc-leases/") {
                        gn2.lock().expect("lock").push((now_ns(), d.to_owned()));
                    }
                }
                _ => {}
            }
        }
    });

    tokio::time::sleep(Duration::from_secs(15)).await;
    let fault_at = now_ns();
    match fault {
        Fault::CutFromGlobal => {
            eprintln!("--- fra1 cut from etcd for 30 s");
            tox.set_enabled(routes["fra1"].name(), false)
                .await
                .expect("cut");
            tokio::time::sleep(Duration::from_secs(30)).await;
            tox.set_enabled(routes["fra1"].name(), true)
                .await
                .expect("heal");
        }
        Fault::Freeze => {
            let pid = kids.0[0].1.id().to_string();
            eprintln!("--- fra1 frozen (SIGSTOP) for 30 s");
            Command::new("kill")
                .args(["-STOP", &pid])
                .status()
                .expect("stop");
            tokio::time::sleep(Duration::from_secs(30)).await;
            Command::new("kill")
                .args(["-CONT", &pid])
                .status()
                .expect("cont");
        }
    }
    eprintln!("--- healed; waiting for the return");
    // Return: fra1 active again, steps run.
    let t0 = Instant::now();
    let mut active = false;
    while t0.elapsed() < Duration::from_secs(60) {
        if let Ok(Some(kv)) = store
            .get(
                &dc_key(&DcId::new("fra1").expect("dc")).expect("k"),
                Consistency::Linearizable,
            )
            .await
            && Record::<DcRecord>::decode(&kv.value).expect("d").body.state == "active"
            && !moves.lock().expect("lock").is_empty()
        {
            active = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(active, "fra1 must fail over and come back active");
    tokio::time::sleep(Duration::from_secs(15)).await;
    write_marker(&work.join("stop"));
    gstop.store(true, Ordering::SeqCst);
    let _ = leader.await;
    for (_, c) in &mut kids.0 {
        let _ = c.wait();
    }
    truth.abort();

    // ---- checks ----
    let moves = moves.lock().expect("lock").clone();
    let gone = gone.lock().expect("lock").clone();
    eprintln!("moves {moves:?}; lease deletions {gone:?}");
    assert_eq!(moves.len(), 1, "exactly one tenant moved: {moves:?}");
    let (moved_at, moved_t, to) = moves[0].clone();
    assert_eq!(
        (moved_t, to.as_str()),
        (T_MOVES, "ams1"),
        "the pinned tenant stays"
    );
    let lost = gone
        .iter()
        .find(|(_, d)| d == "fra1")
        .expect("fra1's lease vanished")
        .0;
    assert!(lost > fault_at, "the lease vanished after the fault");
    assert!(
        moved_at >= lost + 500_000_000,
        "promotion waited the drift margin after the lease vanished"
    );
    // The record: two down votes.
    let page = store
        .list(
            &Tier::Global.key("failovers/").expect("k"),
            sprig_meta::ListOpts::default(),
        )
        .await
        .expect("list");
    let recs: Vec<FailoverRecord> = page
        .kvs
        .iter()
        .map(|kv| Record::<FailoverRecord>::decode(&kv.value).expect("d").body)
        .collect();
    let done = recs
        .iter()
        .find(|r| r.state == FailoverState::Done)
        .expect("a finished failover");
    assert!(
        done.votes
            .iter()
            .filter(|v| v.verdict == VerdictKind::Down)
            .count()
            >= 2,
        "{:?}",
        done.votes
    );
    eprintln!(
        "failover record: {}",
        serde_json::to_string(done).unwrap_or_default()
    );

    let mut all: Vec<OpRecord> = Vec::new();
    for d in ["fra1", "ams1"] {
        all.extend(read_jsonl(&work.join(format!("{d}.jsonl"))).expect("history"));
    }
    all.sort_by_key(|r| r.time_ns);
    // Ack attribution: process ids 100.. are fra1's, 200.. ams1's.
    let dc_of = |p: u32| if p < 200 { "fra1" } else { "ams1" };
    let mut acked: Vec<(u64, u64, u64, &str)> = Vec::new(); // (tenant, key, value, dc)
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for r in &all {
        *counts
            .entry(format!("{}:{:?}", dc_of(r.process), r.kind))
            .or_default() += 1;
        if r.kind != OpKind::Ok {
            continue;
        }
        for m in r.value.as_array().into_iter().flatten() {
            if m[0] == "append" {
                let k = m[1].as_u64().unwrap_or(0);
                let t = if k >= 100 { T_PINNED } else { T_MOVES };
                acked.push((t, k, m[2].as_u64().unwrap_or(0), dc_of(r.process)));
                // No split brain: a DC acknowledges a tenant's write only
                // while it is the tenant's home.
                let home = if t == T_MOVES && r.time_ns >= moved_at {
                    "ams1"
                } else {
                    "fra1"
                };
                assert_eq!(
                    dc_of(r.process),
                    home,
                    "split brain: {r:?} acknowledged by the wrong home"
                );
            }
        }
    }
    eprintln!("history {counts:?}; {} acknowledged appends", acked.len());
    // Every acknowledged append is in the tenant's final home.
    let l1 = final_lists(&ams1_root, T_MOVES).await;
    let l2 = final_lists(&fra1_root, T_PINNED).await;
    for (t, k, v, _) in &acked {
        let l = if *t == T_MOVES { &l1 } else { &l2 };
        assert!(
            l.get(k).is_some_and(|x| x.contains(v)),
            "acknowledged append {k}:{v} of tenant {t} lost"
        );
    }
    // The returning copy: fenced, diverged transactions reported.
    let grants = scalar(&fra1_root, &format!("SELECT GROUP_CONCAT(PRIVILEGE_TYPE) FROM information_schema.SCHEMA_PRIVILEGES WHERE GRANTEE=\"'r2fo_app'@'%'\" AND TABLE_SCHEMA='{}'", schema(T_MOVES))).await;
    assert_eq!(
        grants.as_deref(),
        Some("SELECT"),
        "the moved tenant's copy in fra1 keeps only read grants"
    );
    let applied = done
        .tenants
        .iter()
        .find(|x| x.tenant_id == T_MOVES)
        .and_then(|x| x.applied.get("fra1-c1").cloned())
        .unwrap_or_default();
    let fra1_pos = scalar(&fra1_root, "SELECT @@gtid_binlog_pos")
        .await
        .unwrap_or_default();
    let ep = NodeEndpoint::from_url(NodeId::new("fra1-db").expect("id"), &fra1_root).expect("ep");
    match diverged(&fra1_pos, &applied, 1) {
        Some(range) => {
            let txns = diverged_events(&ep, &range).await.expect("events");
            let report = errant_report(&range, &txns, &[schema(T_MOVES)]);
            eprintln!("diverged in fra1 (reported, never merged): {report:?}");
            for (t, k, v, _) in &acked {
                if *t == T_MOVES {
                    assert!(
                        !report
                            .statements
                            .iter()
                            .any(|s| s.contains(&format!("({k}, '{v}')"))),
                        "acknowledged {k}:{v} is diverged"
                    );
                }
            }
        }
        None => eprintln!("nothing diverged in fra1 (applied {applied}, fra1 at {fra1_pos})"),
    }
    let storm = std::fs::read_to_string(work.join("fra1-storm.json"))
        .expect("fra1 measured the reconnect storm");
    let s: Value = serde_json::from_str(&storm).expect("json");
    eprintln!("reconnect storm after the return: {s}");
    let (rate, admitted) = (
        s["rate"].as_u64().unwrap_or(0),
        s["admitted"].as_u64().unwrap_or(0),
    );
    assert!(
        rate > 0 && admitted <= rate + 1,
        "admission held to the step's rate"
    );
    assert!(
        s["hint_min_ms"].as_u64().unwrap_or(0) >= 1000
            && s["hint_max_ms"].as_u64().unwrap_or(0) <= 5000
    );
    // T_PINNED wrote again in fra1 after the return.
    let after: usize = all
        .iter()
        .filter(|r| r.kind == OpKind::Ok && r.process < 200 && r.time_ns > moved_at)
        .count();
    assert!(
        after > 0,
        "fra1 serves its pinned tenant again after returning"
    );
    if let Some(cli) = ElleCli::from_env() {
        let v = cli
            .check(&all, ElleModel::ListAppend, &["read-committed"])
            .expect("elle");
        eprintln!(
            "elle over {} records: valid {:?} anomalies {:?}",
            all.len(),
            v.valid,
            v.anomalies
        );
        assert_eq!(v.valid, Some(true), "{}", v.detail);
    }
    drop((fra1_db, ams1_db, etcd, routes, net));
    let _ = std::fs::remove_dir_all(&work);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dc_cut_from_the_global_tier() {
    if env("R2FO_ROLE") == "dc" || !skip::flag(skip::DOCKER) {
        return;
    }
    scenario(Fault::CutFromGlobal).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dc_frozen_by_a_process_pause() {
    if env("R2FO_ROLE") == "dc" || !skip::flag(skip::DOCKER) {
        return;
    }
    scenario(Fault::Freeze).await;
}
