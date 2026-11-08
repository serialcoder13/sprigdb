//! Cluster failover against real MariaDB, Jepsen-style (chapter 25,
//! "Testing"). Set `SPRIG_TEST_DOCKER=1` to run; `SPRIG_TEST_ELLE=1` adds
//! elle-cli's list-append check of the whole history.
//!
//! - [`primary_replicas_promote_with_after_sync`]: three throwaway
//!   MariaDB containers (768 MB each) on their own network, primary + 2
//!   replicas with semi-sync AFTER_SYNC. A list-append workload follows
//!   the writer. The primary is `kill -9`ed: detection needs two sources
//!   (refused probes and the agent's view), the damper allows it, the
//!   drift margin passes, the relay logs drain, the most advanced replica
//!   is promoted within the RPO and the other repointed. Then the new
//!   primary is frozen with `docker pause` for 12 s: probes only time out,
//!   so the damper waits and nothing fails over. The old primary returns
//!   `read_only`, its diverged transactions (if any) are reported and
//!   checked to hold no acknowledged write, and it rejoins. Checked: every
//!   acknowledged append is on the final primary; elle finds the history
//!   serializable.
//! - [`galera_repicks_writers_and_heals`]: the shared dev Galera
//!   (`SPRIG_TEST_MARIADB_GALERA`, three URLs). One node is cut from the
//!   other two with faultd for 15 s; it leaves the primary component, its
//!   shards' writers are re-picked among the two left (rendezvous), a write
//!   sent to it gets 1047; the cut heals and the test waits for Synced 3/3.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use sprig_cluster::driver::galera::choose_writer;
use sprig_cluster::failover::damper::{Damp, DownSources, FlapDamper};
use sprig_cluster::failover::galera::{GaleraAction, GaleraQuorum, probe_node};
use sprig_cluster::failover::primary_replica::{
    self as pr, Choice, ReplSource, binlog_pos, check_rpo, choose, diverged, diverged_events,
    drain_relay, expected_loss, promote, rejoin, repoint, semisync_on,
};
use sprig_cluster::record::WriterWeight;
use sprig_cluster::sql::NodeEndpoint;
use sprig_common::ids::{NodeId, ShardId};
use sprig_controller::failover::recovery::errant_report;
use sprig_testkit::faults::compose::{OwnContainer, docker};
use sprig_testkit::faults::elle::{ElleCli, ElleModel};
use sprig_testkit::faults::history::{History, OpKind};
use sprig_testkit::skip;

const ROOT_PW: &str = "sprig-sim-root";
const REPL_PW: &str = "sprig-sim-repl";
const SCHEMA: &str = "r2failover_la";
const KEYS: u64 = 8;

fn enabled(test: &str) -> bool {
    if skip::flag(skip::DOCKER) {
        return true;
    }
    skip::report_missing(skip::DOCKER, test);
    false
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .expect("free port")
}

fn ms(d: Duration) -> u128 {
    d.as_millis()
}

// ---- the workload ---------------------------------------------------------

/// Where each key's transactions go.
type Route = Arc<dyn Fn(u64) -> String + Send + Sync>;

struct Workload {
    history: Arc<History>,
    stop: Arc<AtomicBool>,
    acked: Arc<std::sync::Mutex<Vec<(u64, u64)>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

fn opts(url: &str) -> mysql_async::Opts {
    let o = mysql_async::Opts::from_url(url).expect("url");
    mysql_async::OptsBuilder::from_opts(o)
        .prefer_socket(false)
        .conn_ttl(None)
        .into()
}

/// List-append clients: each transaction appends to or reads 1-3 keys,
/// on the node `route` names for its first key, at `isolation`.
fn start_workload(route: Route, processes: u32, isolation: &'static str, galera: bool) -> Workload {
    let history = Arc::new(History::new());
    let stop = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(std::sync::Mutex::new(Vec::new()));
    let next = Arc::new(AtomicU64::new(1));
    let mut tasks = Vec::new();
    for p in 0..processes {
        let (history, stop, acked, next, route) = (
            Arc::clone(&history),
            Arc::clone(&stop),
            Arc::clone(&acked),
            Arc::clone(&next),
            Arc::clone(&route),
        );
        tasks.push(tokio::spawn(async move {
            let mut conn: Option<(String, mysql_async::Conn)> = None;
            let mut seed = u64::from(p) * 7919 + 13;
            let mut rnd = move |n: u64| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed % n
            };
            while !stop.load(Ordering::SeqCst) {
                let n = 1 + rnd(3);
                let mops: Vec<(bool, u64, u64)> = (0..n)
                    .map(|_| {
                        let k = rnd(KEYS);
                        if rnd(2) == 0 {
                            (true, k, next.fetch_add(1, Ordering::SeqCst))
                        } else {
                            (false, k, 0)
                        }
                    })
                    .collect();
                let invoke: Vec<Value> = mops
                    .iter()
                    .map(|(a, k, v)| if *a { json!(["append", k, v]) } else { json!(["r", k, null]) })
                    .collect();
                let target = route(mops[0].1);
                if conn.as_ref().is_some_and(|(u, _)| *u != target) {
                    if let Some((_, c)) = conn.take() {
                        let _ = tokio::time::timeout(Duration::from_secs(1), c.disconnect()).await;
                    }
                }
                if conn.is_none() {
                    let c = tokio::time::timeout(Duration::from_secs(2), mysql_async::Conn::new(opts(&target))).await;
                    match c {
                        Ok(Ok(mut c)) => {
                            let _ = c.query_drop(format!("SET SESSION TRANSACTION ISOLATION LEVEL {isolation}")).await;
                            if galera {
                                let _ = c.query_drop("SET SESSION wsrep_sync_wait = 1").await;
                            }
                            conn = Some((target, c));
                        }
                        _ => {
                            // Never invoked: nothing to record.
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    }
                }
                history.record(p, OpKind::Invoke, "txn", Value::Array(invoke.clone()));
                let Some((_, c)) = conn.as_mut() else { continue };
                let r = tokio::time::timeout(Duration::from_secs(4), txn(c, &mops)).await;
                match r {
                    Ok(Ok(done)) => {
                        let mut a = acked.lock().expect("lock");
                        for (app, k, v) in &mops {
                            if *app {
                                a.push((*k, *v));
                            }
                        }
                        history.record(p, OpKind::Ok, "txn", Value::Array(done));
                    }
                    Ok(Err(mysql_async::Error::Server(e))) if matches!(e.code, 1020 | 1205 | 1213 | 1290 | 1047 | 1180) => {
                        // Refused before commit (1020: snapshot isolation conflict; 1180: Galera's commit
                        // refusal is a rollback too).
                        let _ = c.query_drop("ROLLBACK").await;
                        history.record(p, OpKind::Fail, "txn", Value::Array(invoke));
                        if e.code == 1290 || e.code == 1047 {
                            conn = None;
                        }
                    }
                    other => {
                        let why = match other {
                            Ok(Err(e)) => e.to_string().chars().take(60).collect::<String>(),
                            Err(_) => "timed out".to_owned(),
                            Ok(Ok(_)) => String::new(),
                        };
                        if p == 0 {
                            eprintln!("indeterminate: {why}");
                        }
                        history.record(p, OpKind::Info, "txn", Value::Array(invoke));
                        conn = None;
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            if let Some((_, c)) = conn {
                let _ = tokio::time::timeout(Duration::from_secs(1), c.disconnect()).await;
            }
        }));
    }
    Workload { history, stop, acked, tasks }
}

async fn txn(c: &mut mysql_async::Conn, mops: &[(bool, u64, u64)]) -> Result<Vec<Value>, mysql_async::Error> {
    c.query_drop("BEGIN").await?;
    let mut done = Vec::new();
    for (append, k, v) in mops {
        if *append {
            c.exec_drop(
                format!("INSERT INTO `{SCHEMA}`.la (k, v) VALUES (?, ?) ON DUPLICATE KEY UPDATE v = CONCAT(v, ',', VALUES(v))"),
                (k, v.to_string()),
            )
            .await?;
            done.push(json!(["append", k, v]));
        } else {
            let cur: Option<String> = c.exec_first(format!("SELECT v FROM `{SCHEMA}`.la WHERE k = ?"), (k,)).await?;
            let list: Vec<u64> = cur.unwrap_or_default().split(',').filter_map(|x| x.parse().ok()).collect();
            done.push(json!(["r", k, list]));
        }
    }
    c.query_drop("COMMIT").await?;
    Ok(done)
}

impl Workload {
    async fn stop(self) -> (Arc<History>, Vec<(u64, u64)>) {
        self.stop.store(true, Ordering::SeqCst);
        for t in self.tasks {
            let _ = t.await;
        }
        let acked = self.acked.lock().expect("lock").clone();
        (self.history, acked)
    }
}

async fn final_lists(url: &str) -> BTreeMap<u64, Vec<u64>> {
    let mut c = mysql_async::Conn::new(opts(url)).await.expect("connect");
    let rows: Vec<(u64, String)> = c.query(format!("SELECT k, v FROM `{SCHEMA}`.la")).await.expect("read");
    let _ = c.disconnect().await;
    rows.into_iter()
        .map(|(k, v)| (k, v.split(',').filter_map(|x| x.parse().ok()).collect()))
        .collect()
}

fn check_acked(lists: &BTreeMap<u64, Vec<u64>>, acked: &[(u64, u64)]) {
    let mut lost = Vec::new();
    for (k, v) in acked {
        if !lists.get(k).is_some_and(|l| l.contains(v)) {
            lost.push((k, v));
        }
    }
    assert!(lost.is_empty(), "acknowledged appends lost: {lost:?}");
    for (k, l) in lists {
        let mut seen = std::collections::BTreeSet::new();
        assert!(l.iter().all(|v| seen.insert(*v)), "key {k} holds a value twice: {l:?}");
    }
}

fn elle_check(history: &History, consistency: &[&str]) {
    let Some(cli) = ElleCli::from_env() else {
        eprintln!("SPRIG_TEST_ELLE unset: skipping the elle check");
        return;
    };
    let recs = history.records();
    let v = cli.check(&recs, ElleModel::ListAppend, consistency).expect("elle runs");
    eprintln!("elle over {} records: valid {:?} anomalies {:?}", recs.len(), v.valid, v.anomalies);
    assert_eq!(v.valid, Some(true), "elle: {}", v.detail);
}

fn summarize(h: &History) -> String {
    let mut c: BTreeMap<String, usize> = BTreeMap::new();
    for r in h.records() {
        *c.entry(format!("{:?}", r.kind)).or_default() += 1;
    }
    format!("{c:?}")
}

// ---- primary + replicas ---------------------------------------------------

struct Net(String);

impl Drop for Net {
    fn drop(&mut self) {
        let _ = docker(&["network", "rm", &self.0]);
    }
}

struct Node {
    name: &'static str,
    port: u16,
    container: OwnContainer,
}

impl Node {
    fn url(&self) -> String {
        format!("mysql://root:{ROOT_PW}@127.0.0.1:{}", self.port)
    }
    fn ep(&self) -> NodeEndpoint {
        NodeEndpoint::from_url(NodeId::new(self.name).expect("id"), &self.url()).expect("ep")
    }
    fn repl(&self) -> ReplSource {
        ReplSource { host: self.name.to_owned(), port: 3306, user: "sprig_repl".into(), password: REPL_PW.into() }
    }
}

fn start_node(net: &str, name: &'static str, server_id: u32, master: Option<&str>) -> Node {
    let dir = sprig_testkit::compose::compose_dir().join("mariadb");
    let binlog = format!("{}:/etc/mysql/conf.d/10-binlog.cnf:ro", dir.join("binlog.cnf").display());
    let semi = format!("{}:/etc/mysql/conf.d/20-semi-sync.cnf:ro", dir.join("semi-sync.cnf").display());
    let port = free_port();
    let publish = format!("127.0.0.1:{port}:3306");
    let (root, repl_user, repl_pw) = (
        format!("MARIADB_ROOT_PASSWORD={ROOT_PW}"),
        "MARIADB_REPLICATION_USER=sprig_repl".to_owned(),
        format!("MARIADB_REPLICATION_PASSWORD={REPL_PW}"),
    );
    let sid = format!("--server-id={server_id}");
    let mhost = master.map(|m| format!("MARIADB_MASTER_HOST={m}"));
    let mut args: Vec<&str> = vec![
        "--memory", "768m", "--network", net, "--network-alias", name, "-p", &publish,
        "-e", &root, "-e", &repl_user, "-e", &repl_pw, "-v", &binlog, "-v", &semi,
    ];
    if let Some(m) = &mhost {
        args.extend(["-e", m]);
    }
    args.extend(["mariadb:11.8", &sid, "--gtid-domain-id=1", "--innodb-buffer-pool-size=64M", "--read-only=ON"]);
    if master.is_none() {
        args.push("--rpl-semi-sync-master-enabled=ON");
    }
    let container = OwnContainer::run(&format!("r2fo-{name}"), &args).expect("container");
    Node { name, port, container }
}

async fn wait_sql(url: &str, sql: &str, want: impl Fn(&str) -> bool, within: Duration) -> String {
    let t0 = Instant::now();
    let mut last = String::new();
    while t0.elapsed() < within {
        if let Ok(Ok(mut c)) = tokio::time::timeout(Duration::from_secs(2), mysql_async::Conn::new(opts(url))).await {
            let v: Option<Option<String>> = c.query_first(sql).await.ok();
            let _ = c.disconnect().await;
            last = v.flatten().unwrap_or_default();
            if want(&last) {
                return last;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("{url}: `{sql}` never satisfied; last {last:?}");
}

async fn exec(url: &str, sqls: &[&str]) {
    let mut c = mysql_async::Conn::new(opts(url)).await.expect("connect");
    for s in sqls {
        c.query_drop(*s).await.unwrap_or_else(|e| panic!("{s}: {e}"));
    }
    let _ = c.disconnect().await;
}

/// What the probe of one node saw.
async fn probe(url: &str) -> (bool, bool) {
    // (refused, timed out)
    match tokio::time::timeout(Duration::from_secs(1), mysql_async::Conn::new(opts(url))).await {
        Ok(Ok(c)) => {
            let _ = tokio::time::timeout(Duration::from_millis(200), c.disconnect()).await;
            (false, false)
        }
        Ok(Err(_)) => (true, false),
        Err(_) => (false, true),
    }
}

/// The controller's loop for one primary until it decides: returns the
/// damper's verdict when a failover may proceed, or `None` after `watch`.
async fn detect(primary: &Node, damper: &FlapDamper, detect_for: Duration, watch: Duration) -> Option<(Instant, DownSources)> {
    let t0 = Instant::now();
    let mut bad_since: Option<Instant> = None;
    let mut waits = 0;
    while t0.elapsed() < watch {
        let (refused, timed_out) = probe(&primary.url()).await;
        if !refused && !timed_out {
            bad_since = None;
        } else {
            let since = *bad_since.get_or_insert_with(Instant::now);
            if since.elapsed() >= detect_for {
                let s = DownSources {
                    probes_refused: refused,
                    probes_timed_out: timed_out,
                    // The agent's view: mysqld is not running (a paused
                    // process is still "running" to its agent).
                    agent: docker(&["inspect", "-f", "{{.State.Running}}", primary.container.name()]).is_ok_and(|s| s == "false"),
                    gateways: false,
                    hard: false,
                };
                match damper.may_fail_over(&s, Instant::now()) {
                    Damp::Proceed => return Some((Instant::now(), s)),
                    Damp::Wait(w) | Damp::Suppress(w) => {
                        waits += 1;
                        if waits % 4 == 1 {
                            eprintln!("damper: {w}");
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)] // one scenario, start to end
async fn primary_replicas_promote_with_after_sync() {
    if !enabled("primary_replicas_promote_with_after_sync") {
        return;
    }
    let net = Net(format!("sprig-sim-r2fo-net-{}", std::process::id()));
    docker(&["network", "create", &net.0]).expect("network");
    let db1 = start_node(&net.0, "db1", 101, None);
    wait_sql(&db1.url(), "SELECT 1", |v| v == "1", Duration::from_secs(120)).await;
    let db2 = start_node(&net.0, "db2", 102, Some("db1"));
    let db3 = start_node(&net.0, "db3", 103, Some("db1"));
    for r in [&db2, &db3] {
        wait_sql(&r.url(), "SELECT 1", |v| v == "1", Duration::from_secs(120)).await;
        // The replication account exists on every node, so any can be promoted.
        exec(&r.url(), &[
            "SET SESSION sql_log_bin = 0",
            &format!("CREATE USER IF NOT EXISTS 'sprig_repl'@'%' IDENTIFIED BY '{REPL_PW}'"),
            "GRANT REPLICATION SLAVE ON *.* TO 'sprig_repl'@'%'",
        ])
        .await;
    }
    // The agent unfences the primary (fenced boot: every node starts read_only).
    exec(&db1.url(), &["SET GLOBAL read_only = OFF", &format!("CREATE DATABASE IF NOT EXISTS {SCHEMA}"),
        &format!("CREATE TABLE IF NOT EXISTS {SCHEMA}.la (k INT PRIMARY KEY, v TEXT NOT NULL) ENGINE=InnoDB")]).await;
    wait_sql(&db1.url(), "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME='Rpl_semi_sync_master_clients'",
        |v| v == "2", Duration::from_secs(60)).await;
    eprintln!("cluster up: db1 primary, db2 and db3 semi-sync replicas");

    let writer = Arc::new(RwLock::new(db1.url()));
    let w2 = Arc::clone(&writer);
    let route: Route = Arc::new(move |_k| w2.read().expect("lock").clone());
    let wl = start_workload(route, 6, "SERIALIZABLE", false);
    tokio::time::sleep(Duration::from_secs(6)).await;

    // The per-second semi-sync sample, just before the fault.
    let semisync = semisync_on(&db1.ep()).await.expect("sample");
    assert!(semisync, "semi-sync must be on before the kill");
    let killed_at = Instant::now();
    docker(&["kill", "-s", "KILL", db1.container.name()]).expect("kill");
    eprintln!("db1 killed (-9)");

    let mut damper = FlapDamper::new(2, Duration::from_secs(30 * 60));
    let (decided_at, sources) = detect(&db1, &damper, Duration::from_secs(3), Duration::from_secs(30))
        .await
        .expect("the failover is allowed");
    eprintln!("detected after {} ms with {sources:?}", ms(decided_at - killed_at));
    // Step 2: the drift margin after the writer lease would have lapsed.
    tokio::time::sleep(Duration::from_millis(500)).await;
    // Steps 3-6.
    let states = vec![drain_relay(&db2.ep()).await.expect("drain db2"), drain_relay(&db3.ep()).await.expect("drain db3")];
    let Choice::Promote(mut cand) = choose(&states, 1) else { panic!("blocked: {states:?}") };
    let best_lag = states.iter().find(|s| s.node == cand.node).and_then(|s| s.lag_seconds);
    cand.expected_loss = expected_loss(semisync, best_lag);
    check_rpo(cand.expected_loss, Duration::from_secs(5), false).expect("within the RPO");
    let (best, other) = if cand.node.as_str() == "db2" { (&db2, &db3) } else { (&db3, &db2) };
    // Steps 8-10.
    let applied = promote(&best.ep()).await.expect("promote");
    repoint(&other.ep(), &best.repl()).await.expect("repoint");
    *writer.write().expect("lock") = best.url();
    damper.record(Instant::now());
    let promoted_at = Instant::now();
    eprintln!(
        "promoted {} ({}); applied {applied}; RTO {} ms; loss estimate {:?}",
        cand.node, cand.why, ms(promoted_at - killed_at), cand.expected_loss
    );
    wait_sql(&best.url(), "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME='Rpl_semi_sync_master_clients'",
        |v| v == "1", Duration::from_secs(30)).await;
    tokio::time::sleep(Duration::from_secs(6)).await;

    // A process pause of the new primary: timeouts alone are "slow".
    docker(&["pause", best.container.name()]).expect("pause");
    let paused_at = Instant::now();
    let d = detect(best, &damper, Duration::from_secs(3), Duration::from_secs(12)).await;
    docker(&["unpause", best.container.name()]).expect("unpause");
    assert!(d.is_none(), "a paused (slow) primary must not fail over: {d:?}");
    eprintln!("paused {} ms: no failover", ms(paused_at.elapsed()));
    tokio::time::sleep(Duration::from_secs(5)).await;
    let (history, acked) = wl.stop().await;
    eprintln!("history {}; {} acknowledged appends", summarize(&history), acked.len());

    // The old primary returns: fenced (read_only), diverged reported.
    docker(&["start", db1.container.name()]).expect("start");
    wait_sql(&db1.url(), "SELECT @@read_only", |v| v == "1", Duration::from_secs(120)).await;
    let old_pos = binlog_pos(&db1.ep()).await.expect("pos");
    let lists = final_lists(&best.url()).await;
    match diverged(&old_pos, &applied, 1) {
        Some(range) => {
            let txns = diverged_events(&db1.ep(), &range).await.expect("events");
            let report = errant_report(&range, &txns, &[]);
            eprintln!("old primary diverged: {report:?}");
            assert_eq!(report.count as usize, txns.len(), "every diverged GTID found in the binlog");
            // No acknowledged write may be among them.
            for (k, v) in &acked {
                assert!(
                    !report.statements.iter().any(|s| s.contains(&format!("({k},'{v}')")) || s.contains(&format!("'{v}'"))),
                    "acknowledged append {k}:{v} is in the diverged set"
                );
            }
        }
        None => {
            eprintln!("old primary at {old_pos}, nothing diverged; rejoining");
            rejoin(&db1.ep(), &best.repl()).await.expect("rejoin");
            let target = binlog_pos(&best.ep()).await.expect("pos");
            wait_sql(&db1.url(), "SELECT @@gtid_slave_pos", |v| v == target, Duration::from_secs(60)).await;
            assert_eq!(
                wait_sql(&db1.url(), "SELECT @@read_only", |_| true, Duration::from_secs(5)).await,
                "1",
                "a rejoined node stays read_only"
            );
        }
    }
    check_acked(&lists, &acked);
    elle_check(&history, &["serializable"]);
    let _ = pr::STEP_TIMEOUT;
    drop((db1, db2, db3));
}

// ---- Galera ---------------------------------------------------------------

const GALERA: [&str; 3] = ["sprigdb-dev-galera-1-1", "sprigdb-dev-galera-2-1", "sprigdb-dev-galera-3-1"];
const FAULTD: &str = "sprigdb-dev-faultd-1";

/// Heals faultd's rules in a dev Galera node, also when the test panics.
struct Cut(&'static str);

fn faultd(args: &[&str]) -> sprig_common::Result<String> {
    let mut a = vec!["exec", FAULTD, "/bin/sh", "/usr/local/bin/faultd.sh"];
    a.extend_from_slice(args);
    docker(&a)
}

fn pid(container: &str) -> String {
    docker(&["inspect", "-f", "{{.State.Pid}}", container]).expect("pid")
}

fn ip(container: &str) -> String {
    docker(&["inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}", container])
        .expect("ip")
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_owned()
}

impl Cut {
    fn new(target: &'static str, peers: &[&str]) -> Self {
        let p = pid(target);
        let cut = Self(target);
        for peer in peers {
            faultd(&["partition", &p, &ip(peer)]).expect("partition");
        }
        cut
    }
}

impl Drop for Cut {
    fn drop(&mut self) {
        let _ = faultd(&["heal", &pid(self.0)]);
    }
}

async fn synced_3(urls: &[String]) -> bool {
    for u in urls {
        let Ok(Ok(mut c)) = tokio::time::timeout(Duration::from_secs(2), mysql_async::Conn::new(opts(u))).await else {
            return false;
        };
        let rows: Vec<(String, String)> = c
            .query("SHOW GLOBAL STATUS WHERE Variable_name IN ('wsrep_cluster_size','wsrep_local_state_comment','wsrep_cluster_status')")
            .await
            .unwrap_or_default();
        let _ = c.disconnect().await;
        let m: BTreeMap<String, String> = rows.into_iter().collect();
        if m.get("wsrep_cluster_size").map(String::as_str) != Some("3")
            || m.get("wsrep_local_state_comment").map(String::as_str) != Some("Synced")
            || m.get("wsrep_cluster_status").map(String::as_str) != Some("Primary")
        {
            return false;
        }
    }
    true
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)] // one scenario, start to end
async fn galera_repicks_writers_and_heals() {
    if !enabled("galera_repicks_writers_and_heals") {
        return;
    }
    let Some(urls) = skip::var(skip::MARIADB_GALERA).map(|v| skip::urls(&v)) else {
        skip::report_missing(skip::MARIADB_GALERA, "galera_repicks_writers_and_heals");
        return;
    };
    assert_eq!(urls.len(), 3, "three Galera URLs");
    assert!(synced_3(&urls).await, "the dev Galera must start Synced 3/3");
    let ids: Vec<NodeId> = (1..=3).map(|i| NodeId::new(format!("g{i}")).expect("id")).collect();
    let eps: Vec<NodeEndpoint> = ids.iter().zip(&urls).map(|(id, u)| NodeEndpoint::from_url(id.clone(), u).expect("ep")).collect();
    // Only this test's own schema, by its exact name (a leftover of an
    // interrupted run).
    exec(&urls[0], &[&format!("DROP DATABASE IF EXISTS {SCHEMA}"), &format!("CREATE DATABASE {SCHEMA}"),
        &format!("CREATE TABLE IF NOT EXISTS {SCHEMA}.la (k INT PRIMARY KEY, v TEXT NOT NULL) ENGINE=InnoDB")]).await;

    let writers: Arc<RwLock<Vec<WriterWeight>>> =
        Arc::new(RwLock::new(ids.iter().map(|n| WriterWeight { node: n.clone(), weight: 1.0 }).collect()));
    let url_of: BTreeMap<NodeId, String> = ids.iter().cloned().zip(urls.iter().cloned()).collect();
    let (w2, u2) = (Arc::clone(&writers), url_of.clone());
    let route: Route = Arc::new(move |k| {
        let ws = w2.read().expect("lock");
        let w = choose_writer(ShardId(u32::try_from(k).unwrap_or(0) + 1), &ws).expect("writer");
        u2[&w.node].clone()
    });
    let shard_writer = |k: u64, ws: &[WriterWeight]| choose_writer(ShardId(u32::try_from(k).unwrap_or(0) + 1), ws).map(|w| w.node.clone());
    let before: Vec<Option<NodeId>> = (0..KEYS).map(|k| shard_writer(k, &writers.read().expect("lock"))).collect();
    eprintln!("writers before: {before:?}");

    // The watch: every 500 ms, as the DC leader's galera_watch.
    let stop = Arc::new(AtomicBool::new(false));
    let (ws, st, eps2) = (Arc::clone(&writers), Arc::clone(&stop), eps.clone());
    let changes = Arc::new(std::sync::Mutex::new(Vec::<(Instant, Vec<NodeId>)>::new()));
    let ch2 = Arc::clone(&changes);
    let watch = tokio::spawn(async move {
        let mut q = GaleraQuorum::new();
        while !st.load(Ordering::SeqCst) {
            let mut statuses = Vec::new();
            for e in &eps2 {
                statuses.push(probe_node(e, Duration::from_secs(1)).await);
            }
            if let GaleraAction::Primary(p) = q.observe(&statuses, Instant::now(), Duration::from_secs(30), true) {
                let mut cur = ws.write().expect("lock");
                let now: Vec<NodeId> = cur.iter().map(|w| w.node.clone()).collect();
                if now != p {
                    *cur = p.iter().map(|n| WriterWeight { node: n.clone(), weight: 1.0 }).collect();
                    ch2.lock().expect("lock").push((Instant::now(), p));
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });

    let wl = start_workload(route, 6, "REPEATABLE READ", true);
    tokio::time::sleep(Duration::from_secs(5)).await;
    let cut_at = Instant::now();
    let cut = Cut::new(GALERA[2], &GALERA[..2]);
    eprintln!("galera-3 cut from galera-1 and galera-2");
    // The isolated node refuses work once it leaves the primary component.
    let mut refused = None;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        if let Ok(Ok(mut c)) = tokio::time::timeout(Duration::from_secs(2), mysql_async::Conn::new(opts(&urls[2]))).await {
            let r = c.query_drop(format!("INSERT INTO {SCHEMA}.la (k, v) VALUES (999, 'x') ON DUPLICATE KEY UPDATE v = v")).await;
            let _ = c.disconnect().await;
            // 1180 (a commit refused while the cut takes effect) comes
            // first; once out of the primary component every statement
            // gets 1047.
            if let Err(mysql_async::Error::Server(e)) = r {
                refused = Some(e.code);
                if e.code == 1047 {
                    break;
                }
            }
        }
    }
    assert_eq!(refused, Some(1047), "the node outside the primary component refuses writes with 1047");
    tokio::time::sleep(Duration::from_secs(10)).await;
    let repicked = changes.lock().expect("lock").first().map(|(t, p)| (ms(*t - cut_at), p.clone()));
    eprintln!("writer set re-picked: {repicked:?}");
    let (_, set) = repicked.clone().expect("writers re-picked");
    assert_eq!(set, ids[..2].to_vec());
    let during: Vec<Option<NodeId>> = (0..KEYS).map(|k| shard_writer(k, &writers.read().expect("lock"))).collect();
    for (k, (b, d)) in before.iter().zip(&during).enumerate() {
        if b.as_ref() != Some(&ids[2]) {
            assert_eq!(b, d, "key {k}: writers off the lost node don't move");
        } else {
            assert_ne!(d.as_ref(), Some(&ids[2]), "key {k} moved off the lost node");
        }
    }
    drop(cut);
    let healed_at = Instant::now();
    eprintln!("cut healed; waiting for Synced 3/3");
    let mut ok = false;
    for _ in 0..240 {
        if synced_3(&urls).await {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(ok, "the dev Galera must be Synced 3/3 again");
    eprintln!("Synced 3/3 {} ms after healing", ms(healed_at.elapsed()));
    tokio::time::sleep(Duration::from_secs(3)).await;
    let (history, acked) = wl.stop().await;
    stop.store(true, Ordering::SeqCst);
    let _ = watch.await;
    eprintln!("history {}; {} acknowledged appends; writer changes {:?}", summarize(&history), acked.len(),
        changes.lock().expect("lock").iter().map(|(t, p)| (ms(*t - cut_at), p.clone())).collect::<Vec<_>>());
    let lists = final_lists(&urls[0]).await;
    check_acked(&lists, &acked);
    elle_check(&history, &["read-committed"]);
    exec(&urls[0], &[&format!("DROP DATABASE IF EXISTS {SCHEMA}")]).await;
    assert!(synced_3(&urls).await, "Synced 3/3 at the end");
}
