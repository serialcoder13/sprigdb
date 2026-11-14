//! Relay-log safe drains in a primary + replicas cluster driven by
//! SprigDB, live.
//! Set `SPRIG_TEST_DOCKER=1` to run. Each test builds its own primary and
//! two replicas from `mariadb:11.8` (containers `r2drain-*`, 768 MB each,
//! semi-sync `AFTER_SYNC`, `gtid_strict_mode`, `log_slave_updates`,
//! `read_only` on the replicas) on a network of its own, and removes them.
//!
//! The replica holding the last acknowledged commits is made to lag on
//! applying them, the primary is `kill -9`ed, and SprigDB's promotion
//! steps run as the controller's `cluster_failover` loop runs them (steps
//! 6-8: `drain_relay` on every replica, `choose`, `promote_fenced`,
//! `repoint`; the agent's `read_only = OFF` last). Then every write the
//! primary acknowledged (with semi-sync on and no commit falling back to
//! async) must be on the promoted node.
//!
//! - [`sql_thread_stopped_on_the_most_advanced_replica`]: `db2` received
//!   everything but its SQL thread is stopped; `db3` stopped receiving
//!   earlier. The promotion must pick `db2` and apply its relay log.
//! - [`both_threads_stopped_blocks_the_promotion`]: `db2` holds the last
//!   acknowledged commits only in its relay log with both threads stopped.
//!   The promotion must stop (blocked) instead of promoting either node;
//!   the test then shows MariaDB's hazard: `START SLAVE SQL_THREAD` with
//!   the IO thread stopped discards that relay log.

#![allow(clippy::too_many_lines, clippy::doc_markdown)]

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use sprig_cluster::failover::primary_replica::{
    Choice, ReplSource, choose, drain_relay, promote_fenced, repoint,
};
use sprig_cluster::sql::NodeEndpoint;
use sprig_common::ids::NodeId;
use sprig_testkit::faults::compose::docker;
use sprig_testkit::skip;

const ROOT_PW: &str = "r2drain-root";
const REPL_PW: &str = "r2drain-repl";
const SCHEMA: &str = "r2drain_db";
const IMAGE: &str = "mariadb:11.8";

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

fn opts(url: &str) -> mysql_async::Opts {
    let o = mysql_async::Opts::from_url(url).expect("url");
    mysql_async::OptsBuilder::from_opts(o)
        .prefer_socket(false)
        .conn_ttl(None)
        .into()
}

/// A container and network removed on drop, by exact name.
struct Cluster {
    net: String,
    nodes: Vec<Node>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for n in &self.nodes {
            let _ = docker(&["rm", "-f", "-v", &n.container]);
        }
        let _ = docker(&["network", "rm", &self.net]);
    }
}

struct Node {
    name: &'static str,
    container: String,
    port: u16,
}

impl Node {
    fn url(&self) -> String {
        format!("mysql://root:{ROOT_PW}@127.0.0.1:{}", self.port)
    }
    fn ep(&self) -> NodeEndpoint {
        NodeEndpoint::from_url(NodeId::new(self.name).expect("id"), &self.url()).expect("ep")
    }
    fn repl(&self) -> ReplSource {
        ReplSource {
            host: self.name.to_owned(),
            port: 3306,
            user: "sprig_repl".into(),
            password: REPL_PW.into(),
        }
    }
}

fn start_node(net: &str, tag: &str, name: &'static str, server_id: u32, master: bool) -> Node {
    let dir = sprig_testkit::compose::compose_dir().join("mariadb");
    let binlog = format!(
        "{}:/etc/mysql/conf.d/10-binlog.cnf:ro",
        dir.join("binlog.cnf").display()
    );
    let semi = format!(
        "{}:/etc/mysql/conf.d/20-semi-sync.cnf:ro",
        dir.join("semi-sync.cnf").display()
    );
    let port = free_port();
    let container = format!("r2drain-{name}-{tag}");
    let publish = format!("127.0.0.1:{port}:3306");
    let root = format!("MARIADB_ROOT_PASSWORD={ROOT_PW}");
    let repl_pw = format!("MARIADB_REPLICATION_PASSWORD={REPL_PW}");
    let sid = format!("--server-id={server_id}");
    let mut args: Vec<&str> = vec![
        "run",
        "-d",
        "--name",
        &container,
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
        &repl_pw,
        "-v",
        &binlog,
        "-v",
        &semi,
    ];
    if !master {
        args.extend(["-e", "MARIADB_MASTER_HOST=db1"]);
    }
    args.extend([
        IMAGE,
        &sid,
        "--gtid-domain-id=1",
        "--innodb-buffer-pool-size=64M",
    ]);
    if master {
        // A long timeout: no commit falls back to async during the test
        // (checked with Rpl_semi_sync_master_no_tx).
        args.extend([
            "--rpl-semi-sync-master-enabled=ON",
            "--rpl-semi-sync-master-timeout=60000",
        ]);
    } else {
        args.push("--read-only=ON");
    }
    docker(&args).expect("docker run");
    Node {
        name,
        container,
        port,
    }
}

async fn wait_sql(url: &str, sql: &str, want: impl Fn(&str) -> bool, within: Duration) -> String {
    let t0 = Instant::now();
    let mut last = String::new();
    while t0.elapsed() < within {
        if let Ok(Ok(mut c)) =
            tokio::time::timeout(Duration::from_secs(2), mysql_async::Conn::new(opts(url))).await
        {
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
        c.query_drop(*s)
            .await
            .unwrap_or_else(|e| panic!("{s}: {e}"));
    }
    let _ = c.disconnect().await;
}

async fn scalar(url: &str, sql: &str) -> String {
    let mut c = mysql_async::Conn::new(opts(url)).await.expect("connect");
    let v: Option<Option<String>> = c.query_first(sql).await.expect(sql);
    let _ = c.disconnect().await;
    v.flatten().unwrap_or_default()
}

async fn status_var(url: &str, name: &str) -> String {
    scalar(
        url,
        &format!(
            "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS \
             WHERE VARIABLE_NAME = '{name}'"
        ),
    )
    .await
}

async fn slave_status(url: &str) -> String {
    let mut c = mysql_async::Conn::new(opts(url)).await.expect("connect");
    let row: Option<mysql_async::Row> = c.query_first("SHOW SLAVE STATUS").await.expect("status");
    let _ = c.disconnect().await;
    let Some(r) = row else {
        return "not a replica".into();
    };
    let get = |n: &str| -> String {
        r.columns_ref()
            .iter()
            .position(|c| c.name_str() == n)
            .and_then(|i| r.as_ref(i))
            .map(|v| match v {
                mysql_async::Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
                other => format!("{other:?}"),
            })
            .unwrap_or_default()
    };
    let err = get("Last_SQL_Error");
    format!(
        "IO {} SQL {} Gtid_IO_Pos {} Relay_Log_Space {}{}",
        get("Slave_IO_Running"),
        get("Slave_SQL_Running"),
        get("Gtid_IO_Pos"),
        get("Relay_Log_Space"),
        if err.is_empty() {
            String::new()
        } else {
            format!(" Last_SQL_Error {err}")
        }
    )
}

/// Three nodes: `db1` primary, `db2` and `db3` semi-sync replicas.
async fn cluster(tag: &str) -> Cluster {
    let net = format!("r2drain-net-{tag}");
    docker(&["network", "create", &net]).expect("network");
    let mut c = Cluster {
        net,
        nodes: Vec::new(),
    };
    c.nodes.push(start_node(&c.net, tag, "db1", 201, true));
    wait_sql(
        &c.nodes[0].url(),
        "SELECT 1",
        |v| v == "1",
        Duration::from_secs(120),
    )
    .await;
    c.nodes.push(start_node(&c.net, tag, "db2", 202, false));
    c.nodes.push(start_node(&c.net, tag, "db3", 203, false));
    for r in &c.nodes[1..] {
        wait_sql(&r.url(), "SELECT 1", |v| v == "1", Duration::from_secs(120)).await;
    }
    exec(
        &c.nodes[0].url(),
        &[
            &format!("CREATE DATABASE IF NOT EXISTS {SCHEMA}"),
            &format!(
                "CREATE TABLE IF NOT EXISTS {SCHEMA}.t (id BIGINT PRIMARY KEY, phase INT NOT NULL, \
                 pad VARCHAR(2000) NOT NULL) ENGINE=InnoDB"
            ),
        ],
    )
    .await;
    wait_sql(
        &c.nodes[0].url(),
        "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS \
         WHERE VARIABLE_NAME = 'Rpl_semi_sync_master_clients'",
        |v| v == "2",
        Duration::from_secs(60),
    )
    .await;
    for r in &c.nodes[1..] {
        let sql = format!(
            "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{SCHEMA}'"
        );
        let t0 = Instant::now();
        while scalar(&r.url(), &sql).await != "1" {
            assert!(
                t0.elapsed() < Duration::from_secs(90),
                "[{tag}] {} never got the table: {}",
                r.name,
                slave_status(&r.url()).await
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    // Any replica may be promoted: the replication account everywhere.
    // Only now, once replication has applied the primary's own setup (its
    // CREATE USER replicates and would collide with one made here first).
    for r in &c.nodes[1..] {
        exec(
            &r.url(),
            &[
                "SET SESSION sql_log_bin = 0",
                &format!("CREATE USER IF NOT EXISTS 'sprig_repl'@'%' IDENTIFIED BY '{REPL_PW}'"),
                "GRANT REPLICATION SLAVE ON *.* TO 'sprig_repl'@'%'",
            ],
        )
        .await;
    }
    eprintln!("[{tag}] cluster up: db1 primary, db2 and db3 semi-sync AFTER_SYNC replicas");
    c
}

/// Writers inserting 1 KB rows into `t`; each acknowledged id is kept.
struct Load {
    stop: Arc<AtomicBool>,
    phase: Arc<AtomicU64>,
    acked: Arc<Mutex<Vec<(u64, u64)>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

fn start_load(url: &str, writers: u64) -> Load {
    let stop = Arc::new(AtomicBool::new(false));
    let phase = Arc::new(AtomicU64::new(1));
    let acked = Arc::new(Mutex::new(Vec::new()));
    let next = Arc::new(AtomicU64::new(1));
    let mut tasks = Vec::new();
    for _ in 0..writers {
        let (stop, phase, acked, next, url) = (
            Arc::clone(&stop),
            Arc::clone(&phase),
            Arc::clone(&acked),
            Arc::clone(&next),
            url.to_owned(),
        );
        tasks.push(tokio::spawn(async move {
            let mut conn: Option<mysql_async::Conn> = None;
            while !stop.load(Ordering::SeqCst) {
                if conn.is_none() {
                    if let Ok(Ok(c)) = tokio::time::timeout(
                        Duration::from_secs(2),
                        mysql_async::Conn::new(opts(&url)),
                    )
                    .await
                    {
                        conn = Some(c);
                    } else {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                }
                let Some(c) = conn.as_mut() else { continue };
                let id = next.fetch_add(1, Ordering::SeqCst);
                let ph = phase.load(Ordering::SeqCst);
                let r = tokio::time::timeout(
                    Duration::from_secs(70),
                    c.exec_drop(
                        format!("INSERT INTO {SCHEMA}.t (id, phase, pad) VALUES (?, ?, REPEAT('x', 1000))"),
                        (id, ph),
                    ),
                )
                .await;
                match r {
                    Ok(Ok(())) => acked.lock().expect("lock").push((id, ph)),
                    _ => conn = None,
                }
            }
            if let Some(c) = conn {
                let _ = tokio::time::timeout(Duration::from_secs(1), c.disconnect()).await;
            }
        }));
    }
    Load {
        stop,
        phase,
        acked,
        tasks,
    }
}

impl Load {
    fn count(&self) -> usize {
        self.acked.lock().expect("lock").len()
    }
    async fn stop(self) -> Vec<(u64, u64)> {
        self.stop.store(true, Ordering::SeqCst);
        for t in self.tasks {
            let _ = tokio::time::timeout(Duration::from_secs(5), t).await;
        }
        self.acked.lock().expect("lock").clone()
    }
}

async fn ids_on(url: &str) -> BTreeSet<u64> {
    let mut c = mysql_async::Conn::new(opts(url)).await.expect("connect");
    let v: Vec<u64> = c
        .query(format!("SELECT id FROM {SCHEMA}.t"))
        .await
        .expect("ids");
    let _ = c.disconnect().await;
    v.into_iter().collect()
}

fn lost(acked: &[(u64, u64)], on: &BTreeSet<u64>) -> Vec<(u64, u64)> {
    acked
        .iter()
        .filter(|(id, _)| !on.contains(id))
        .copied()
        .collect()
}

fn per_phase(v: &[(u64, u64)]) -> String {
    let mut c = std::collections::BTreeMap::<u64, usize>::new();
    for (_, p) in v {
        *c.entry(*p).or_default() += 1;
    }
    format!("{c:?}")
}

/// Semi-sync was on throughout: every commit waited for an acknowledgement.
async fn assert_all_semisync(primary: &Node, tag: &str) {
    let yes = status_var(&primary.url(), "Rpl_semi_sync_master_yes_tx").await;
    let no = status_var(&primary.url(), "Rpl_semi_sync_master_no_tx").await;
    let on = status_var(&primary.url(), "Rpl_semi_sync_master_status").await;
    eprintln!("[{tag}] semi-sync before the kill: status {on}, yes_tx {yes}, no_tx {no}");
    assert_eq!(on, "ON");
    assert_eq!(no, "0", "a commit fell back to async");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_thread_stopped_on_the_most_advanced_replica() {
    if !enabled("sql_thread_stopped_on_the_most_advanced_replica") {
        return;
    }
    let tag = format!("a{}", std::process::id());
    let c = cluster(&tag).await;
    let (db1, db2, db3) = (&c.nodes[0], &c.nodes[1], &c.nodes[2]);

    let load = start_load(&db1.url(), 4);
    // Phase 1: both replicas receive and apply.
    tokio::time::sleep(Duration::from_secs(3)).await;
    // Phase 2: db2 stops applying (receives and acknowledges still).
    exec(&db2.url(), &["STOP SLAVE SQL_THREAD"]).await;
    load.phase.store(2, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(3)).await;
    // Phase 3: db3 stops receiving; only db2 acknowledges, and applies
    // nothing.
    exec(&db3.url(), &["STOP SLAVE IO_THREAD"]).await;
    load.phase.store(3, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(6)).await;
    eprintln!("[{tag}] {} writes acknowledged so far", load.count());
    assert_all_semisync(db1, &tag).await;
    eprintln!("[{tag}] db2: {}", slave_status(&db2.url()).await);
    eprintln!("[{tag}] db3: {}", slave_status(&db3.url()).await);

    docker(&["kill", "-s", "KILL", &db1.container]).expect("kill");
    let killed = Instant::now();
    let acked = load.stop().await;
    eprintln!(
        "[{tag}] db1 killed; {} acknowledged writes, per phase {}",
        acked.len(),
        per_phase(&acked)
    );
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Steps 6-8, as the cluster_failover loop runs them.
    let t0 = Instant::now();
    let states = vec![
        drain_relay(&db2.ep()).await.expect("drain db2"),
        drain_relay(&db3.ep()).await.expect("drain db3"),
    ];
    eprintln!(
        "[{tag}] drained in {} ms: {states:#?}",
        t0.elapsed().as_millis()
    );
    let cand = match choose(&states, 1) {
        Choice::Promote(cand) => cand,
        Choice::Blocked(why) => panic!("blocked: {why}"),
    };
    eprintln!("[{tag}] chosen {} ({})", cand.node, cand.why);
    let (best, other) = if cand.node.as_str() == "db2" {
        (db2, db3)
    } else {
        (db3, db2)
    };
    let applied = promote_fenced(&best.ep(), true).await.expect("promote");
    repoint(&other.ep(), &best.repl()).await.expect("repoint");
    exec(&best.url(), &["SET GLOBAL read_only = OFF"]).await;
    eprintln!(
        "[{tag}] {} promoted at {applied} {} ms after the kill",
        best.name,
        killed.elapsed().as_millis()
    );

    let on_best = ids_on(&best.url()).await;
    let missing = lost(&acked, &on_best);
    eprintln!(
        "[{tag}] RESULT promoted {}: {} of {} acknowledged writes on it, {} lost (per phase {})",
        best.name,
        acked.len() - missing.len(),
        acked.len(),
        missing.len(),
        per_phase(&missing)
    );
    assert!(
        missing.is_empty(),
        "acknowledged writes lost: {}",
        missing.len()
    );
    assert_eq!(best.name, "db2", "db2 holds every acknowledged write");
    // The repointed replica catches up from the new primary.
    let want = on_best.len().to_string();
    wait_sql(
        &other.url(),
        &format!("SELECT COUNT(*) FROM {SCHEMA}.t"),
        |v| v == want,
        Duration::from_secs(60),
    )
    .await;
    eprintln!("[{tag}] {} caught up: {want} rows", other.name);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_threads_stopped_blocks_the_promotion() {
    if !enabled("both_threads_stopped_blocks_the_promotion") {
        return;
    }
    let tag = format!("b{}", std::process::id());
    let c = cluster(&tag).await;
    let (db1, db2, db3) = (&c.nodes[0], &c.nodes[1], &c.nodes[2]);

    let load = start_load(&db1.url(), 4);
    tokio::time::sleep(Duration::from_secs(3)).await;
    // Phase 2: db3 stops receiving; db2 alone acknowledges and applies.
    exec(&db3.url(), &["STOP SLAVE IO_THREAD"]).await;
    load.phase.store(2, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(3)).await;
    // Phase 3: db2 stops applying; what it acknowledges now is only in its
    // relay log.
    exec(&db2.url(), &["STOP SLAVE SQL_THREAD"]).await;
    load.phase.store(3, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(5)).await;
    let acked = load.stop().await;
    assert_all_semisync(db1, &tag).await;
    // db2's IO thread stops too (its host lost the link, say): both
    // threads stopped, the relay log unapplied.
    exec(&db2.url(), &["STOP SLAVE IO_THREAD"]).await;
    eprintln!("[{tag}] db2: {}", slave_status(&db2.url()).await);
    eprintln!("[{tag}] db3: {}", slave_status(&db3.url()).await);
    docker(&["kill", "-s", "KILL", &db1.container]).expect("kill");
    eprintln!(
        "[{tag}] db1 killed; {} acknowledged writes, per phase {}",
        acked.len(),
        per_phase(&acked)
    );
    tokio::time::sleep(Duration::from_secs(1)).await;

    let states = vec![
        drain_relay(&db2.ep()).await.expect("drain db2"),
        drain_relay(&db3.ep()).await.expect("drain db3"),
    ];
    eprintln!("[{tag}] drained: {states:#?}");
    match choose(&states, 1) {
        Choice::Blocked(why) => {
            eprintln!("[{tag}] RESULT blocked: {why}");
            assert!(why.contains("db2"), "{why}");
        }
        Choice::Promote(cand) => {
            // What the promotion would lose (without a relay-log safe drain).
            let best = if cand.node.as_str() == "db2" {
                db2
            } else {
                db3
            };
            promote_fenced(&best.ep(), true).await.expect("promote");
            exec(&best.url(), &["SET GLOBAL read_only = OFF"]).await;
            let missing = lost(&acked, &ids_on(&best.url()).await);
            eprintln!(
                "[{tag}] RESULT promoted {} ({}): {} of {} acknowledged writes lost (per phase {})",
                best.name,
                cand.why,
                missing.len(),
                acked.len(),
                per_phase(&missing)
            );
            panic!("promoted instead of blocking; {} lost", missing.len());
        }
    }

    // The hazard itself: starting the SQL thread while the IO thread is
    // stopped. Nothing of SprigDB runs here; this is MariaDB's behaviour.
    let before_rows = ids_on(&db2.url()).await.len();
    exec(&db2.url(), &["START SLAVE SQL_THREAD"]).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let after = ids_on(&db2.url()).await;
    let missing = lost(&acked, &after);
    eprintln!(
        "[{tag}] HAZARD START SLAVE SQL_THREAD with the IO thread stopped: rows {before_rows} -> {}, \
         {} acknowledged writes missing on db2 (per phase {}); {}",
        after.len(),
        missing.len(),
        per_phase(&missing),
        slave_status(&db2.url()).await
    );
}
