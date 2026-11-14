//! Failover inside a cluster with the real binaries: the controller's own `cluster_failover` loop, not the
//! test, decides and promotes.
//!
//! ```sh
//! cargo build -p sprigdb -p sprigctl
//! SPRIG_TEST_DOCKER=1 [SPRIG_TEST_ELLE=1] \
//!   cargo test -p sprig-controller --test live_cluster_failover_bins -- --nocapture --test-threads=1
//! ```
//!
//! One DC (`fra1`) on a private etcd that advertises an unresolvable name
//! (so no process's endpoint discovery ever adds the shared dev etcd at
//! 127.0.0.1:2379): `sprigdb controller` (admin 40326), `sprigdb gateway`
//! (MySQL 40306, MariaDB 40307) and one `sprigdb agent` per MariaDB node,
//! set up with `sprigctl`. Tenant ids start at 4,030,600,000 (the MySQL
//! port × 100,000).
//! Containers are capped at 768 MB and removed at the end.
//!
//! - [`bins_primary_replica_kill9`][]: `db1` (primary, semi-sync AFTER_SYNC)
//!   and its replicas `db2`, `db3`, each with an agent. First the agent's
//!   semi-sync guard: with both replicas stopped, `db1` is fenced and
//!   unfenced again (its agent restarted); its first commit doesn't wait
//!   out `rpl_semi_sync_master_timeout`, and semi-sync comes back on once
//!   the replicas reattach. Then a list-append workload runs through the
//!   gateway and `db1` is `kill -9`ed: its agent reports mysqld down and
//!   hands the writer lease back, the controller drains the replicas,
//!   claims the lease for the most advanced one, promotes it and repoints
//!   the other; that node's agent takes the lease over and unfences it.
//!   The new primary is then frozen with `docker pause` for 12 s: probes
//!   only time out and its agent keeps the lease, so nothing fails over.
//!   `db1` is started again: it stays `read_only`, and the controller
//!   rejoins it as a replica (or reports what diverged). Checked: every
//!   acknowledged append is on the final writer; elle finds the history
//!   serializable.
//! - [`bins_galera_node_loss`][]: a throwaway three-node Galera (768 MB
//!   each). One node is `kill -9`ed under a list-append workload through
//!   the gateway: the controller's registry loop re-picks the writer set,
//!   the `cluster_failover` loop records the node outside the primary
//!   component (no bootstrap: quorum held), writes go on; the node is
//!   started again and the cluster ends Synced 3/3. Checked: every
//!   acknowledged append survives; elle finds the history read-committed.

#![allow(
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::doc_markdown,
    clippy::similar_names
)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, Pool, PoolConstraints, PoolOpts, SslOpts};
use serde_json::{Value, json};
use sprig_meta::{Consistency, EtcdConfig, EtcdStore, MetaStore, Op, Tier, Txn};
use sprig_testkit::faults::compose::{OwnContainer, docker};
use sprig_testkit::faults::elle::{ElleCli, ElleModel};
use sprig_testkit::faults::history::{OpKind, OpRecord};
use sprig_testkit::skip;

const ROOT_PW: &str = "sprig-r2next-root";
const REPL_PW: &str = "sprig-r2next-repl";
const APP_PW: &str = "sprig-r2next-app";
const DEPLOYMENT: &str = "sprigdb-r2next";
/// A private deployment's tenant ids: its MySQL port 40306 x 100,000.
const TENANT_BASE: u64 = 4_030_600_000;
const KEYS: u64 = 8;
const DC: &str = "fra1";

// This test's ports: 40306, 40307, 40316-40337.
const GW_MYSQL: u16 = 40306;
const GW_MARIADB: u16 = 40307;
const GW_HTTP: u16 = 40318;
const GW_HTTPS: u16 = 40319;
const GW_PEER: u16 = 40320;
const GW_TEL: u16 = 40321;
const CTL_ADMIN: u16 = 40326;
const CTL_INTERNAL: u16 = 40327;
const CTL_TEL: u16 = 40328;
const AGENT_TEL: [u16; 3] = [40330, 40331, 40332];

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(0))
}

fn bins() -> Option<(PathBuf, PathBuf)> {
    let target = std::env::var("CARGO_TARGET_DIR").map_or_else(
        |_| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../target")),
        PathBuf::from,
    );
    let d = target.join("debug");
    let (s, c) = (d.join("sprigdb"), d.join("sprigctl"));
    (s.exists() && c.exists()).then_some((s, c))
}

// ============================================================================
// Processes
// ============================================================================

struct Procs {
    bin: PathBuf,
    work: PathBuf,
    kids: Vec<(String, Child)>,
}

impl Procs {
    fn start(&mut self, name: &str, role: &str, toml: &str) {
        let cfg = self.work.join(format!("{name}.toml"));
        std::fs::write(&cfg, toml).expect("config");
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.work.join(format!("{name}.log")))
            .expect("log");
        let child = Command::new(&self.bin)
            .args(["--config", cfg.to_str().expect("path"), role])
            .env(
                "RUST_LOG",
                "info,sprig_controller::failover=debug,sprig_agent=debug",
            )
            .stdout(Stdio::from(log.try_clone().expect("log")))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("spawn sprigdb");
        self.kids.push((name.to_owned(), child));
    }

    /// Stops `name` with SIGTERM (it fences its node on the way out).
    fn stop(&mut self, name: &str) {
        if let Some(i) = self.kids.iter().position(|(n, _)| n == name) {
            let (_, mut c) = self.kids.remove(i);
            let _ = Command::new("kill")
                .args(["-TERM", &c.id().to_string()])
                .status();
            let t0 = Instant::now();
            while t0.elapsed() < Duration::from_secs(10) {
                if c.try_wait().ok().flatten().is_some() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

impl Drop for Procs {
    fn drop(&mut self) {
        for (_, c) in &mut self.kids {
            let _ = Command::new("kill")
                .args(["-TERM", &c.id().to_string()])
                .status();
        }
        let t0 = Instant::now();
        for (_, c) in &mut self.kids {
            while t0.elapsed() < Duration::from_secs(10) {
                if c.try_wait().ok().flatten().is_some() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

async fn ready(port: u16, within: Duration) -> bool {
    let t0 = Instant::now();
    while t0.elapsed() < within {
        if let Ok(mut s) = tokio::net::TcpStream::connect(("127.0.0.1", port)).await {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let _ = s
                .write_all(b"GET /readyz HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n")
                .await;
            let mut buf = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut buf)).await;
            if buf.starts_with(b"HTTP/1.1 200") {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    false
}

// ============================================================================
// sprigctl, etcd, MariaDB
// ============================================================================

fn ctl(sprigctl: &Path, args: &[&str]) -> std::result::Result<Value, String> {
    let out = Command::new(sprigctl)
        .args([
            "--endpoint",
            &format!("http://127.0.0.1:{CTL_ADMIN}"),
            "-o",
            "json",
        ])
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    if !out.status.success() {
        return Err(format!(
            "sprigctl {args:?}: {text}{}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
}

async fn ctl_retry(sprigctl: &Path, args: &[&str], within: Duration) -> Value {
    let t0 = Instant::now();
    loop {
        match ctl(sprigctl, args) {
            Ok(v) => return v,
            Err(e) if t0.elapsed() < within => {
                eprintln!("retrying: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => panic!("{e}"),
        }
    }
}

fn key(path: &str) -> sprig_meta::Key {
    if let Some(rel) = path.strip_prefix("/sprig/g/") {
        return Tier::Global.key(rel).expect("key");
    }
    let rest = path.strip_prefix("/sprig/dc/").expect("a tier key");
    let (dc, rel) = rest.split_once('/').expect("dc/rel");
    Tier::Dc(sprig_common::ids::DcId::new(dc).expect("dc"))
        .key(rel)
        .expect("key")
}

async fn put(store: &dyn MetaStore, path: &str, value: Value) {
    store
        .txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key: key(path),
                value: serde_json::to_vec(&value).expect("json"),
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .expect("put");
}

async fn get(store: &dyn MetaStore, path: &str) -> Option<Value> {
    let kv = store
        .get(&key(path), Consistency::Linearizable)
        .await
        .ok()??;
    serde_json::from_slice(&kv.value).ok()
}

fn opts(url: &str) -> mysql_async::Opts {
    let o = mysql_async::Opts::from_url(url).expect("url");
    OptsBuilder::from_opts(o).prefer_socket(false).into()
}

async fn scalar(url: &str, sql: &str) -> Option<String> {
    let mut c = tokio::time::timeout(Duration::from_secs(3), mysql_async::Conn::new(opts(url)))
        .await
        .ok()?
        .ok()?;
    let v: Option<Option<String>> = c.query_first(sql).await.ok();
    let _ = c.disconnect().await;
    v.flatten()
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

async fn master_host(url: &str) -> Option<String> {
    let mut c = tokio::time::timeout(Duration::from_secs(3), mysql_async::Conn::new(opts(url)))
        .await
        .ok()?
        .ok()?;
    let row: Option<mysql_async::Row> = c.query_first("SHOW SLAVE STATUS").await.ok()?;
    let _ = c.disconnect().await;
    row?.get_opt::<String, _>("Master_Host")?.ok()
}

async fn wait_for(url: &str, sql: &str, want: &str, within: Duration) {
    let t0 = Instant::now();
    let mut last = None;
    while t0.elapsed() < within {
        last = scalar(url, sql).await;
        if last.as_deref() == Some(want) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("{url}: {sql} never gave {want} (last {last:?})");
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .expect("port")
}

struct Net(String);

impl Drop for Net {
    fn drop(&mut self) {
        let _ = docker(&["network", "rm", &self.0]);
    }
}

struct Db {
    name: &'static str,
    port: u16,
    c: OwnContainer,
}

impl Db {
    fn root(&self) -> String {
        format!("mysql://root:{ROOT_PW}@127.0.0.1:{}", self.port)
    }
}

/// A primary + replicas node: fenced boot, its own GTID domain per
/// cluster, `report_host` set so a promoted node's address for replicas
/// is known.
fn mariadb(net: &str, name: &'static str, sid: u32, master: Option<&str>) -> Db {
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
    let publish = format!("127.0.0.1:{port}:3306");
    let root = format!("MARIADB_ROOT_PASSWORD={ROOT_PW}");
    let rpw = format!("MARIADB_REPLICATION_PASSWORD={REPL_PW}");
    let sid = format!("--server-id={sid}");
    let report = format!("--report-host={name}");
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
        &report,
        "--report-port=3306",
        "--innodb-buffer-pool-size=64M",
        "--max-connections=300",
        // A node boots fenced; its agent unfences it.
        "--read-only=ON",
    ]);
    if master.is_none() {
        args.push("--rpl-semi-sync-master-enabled=ON");
    }
    Db {
        name,
        port,
        c: OwnContainer::run(&format!("r2next-{name}"), &args).expect("mariadb"),
    }
}

/// A Galera node of a throwaway three-node cluster.
fn galera(net: &str, i: u32) -> Db {
    let name: &'static str = ["g1", "g2", "g3"][i as usize - 1];
    let dir = sprig_testkit::compose::compose_dir().join("mariadb");
    let binlog = format!(
        "{}:/etc/mysql/conf.d/10-binlog.cnf:ro",
        dir.join("binlog.cnf").display()
    );
    // Server options go in a file: the SST script hands command-line
    // options to mariadb-backup, which refuses most of them.
    let cnf = std::env::temp_dir().join(format!("r2next-galera-{}.cnf", std::process::id()));
    std::fs::write(
        &cnf,
        "[mariadbd]\nwsrep_on = ON\nwsrep_provider = /usr/lib/galera/libgalera_smm.so\n\
         wsrep_cluster_name = r2next-galera\nwsrep_cluster_address = gcomm://g1,g2,g3\n\
         wsrep_sst_method = mariabackup\nwsrep_gtid_mode = ON\n\
         wsrep_provider_options = \"gcache.size=64M\"\ninnodb_autoinc_lock_mode = 2\n\
         innodb_buffer_pool_size = 64M\nmax_connections = 300\n",
    )
    .expect("galera cnf");
    let galera_cnf = format!("{}:/etc/mysql/conf.d/20-galera.cnf:ro", cnf.display());
    let port = free_port();
    let publish = format!("127.0.0.1:{port}:3306");
    let root = format!("MARIADB_ROOT_PASSWORD={ROOT_PW}");
    let sid = format!("--server-id={}", 300 + i);
    let dom = format!("--gtid-domain-id={}", 300 + i);
    let node = format!("--wsrep-node-address={name}");
    let auth = format!("--wsrep-sst-auth=root:{ROOT_PW}");
    let mut args: Vec<&str> = vec![
        "--memory",
        "768m",
        "--network",
        net,
        "--network-alias",
        name,
        "--hostname",
        name,
        "-p",
        &publish,
        "-e",
        &root,
        "-v",
        &binlog,
        "-v",
        &galera_cnf,
        "mariadb:11.8",
    ];
    if i == 1 {
        args.push("--wsrep-new-cluster");
    }
    args.extend(["--wsrep-gtid-domain-id=3", &sid, &dom, &node, &auth]);
    Db {
        name,
        port,
        c: OwnContainer::run(&format!("r2next-{name}"), &args).expect("galera"),
    }
}

async fn synced(db: &Db, size: &str) -> bool {
    let Ok(Ok(mut c)) = tokio::time::timeout(
        Duration::from_secs(2),
        mysql_async::Conn::new(opts(&db.root())),
    )
    .await
    else {
        return false;
    };
    let rows: Vec<(String, String)> = c
        .query("SHOW GLOBAL STATUS WHERE Variable_name IN ('wsrep_cluster_size','wsrep_local_state_comment','wsrep_cluster_status')")
        .await
        .unwrap_or_default();
    let _ = c.disconnect().await;
    let m: BTreeMap<String, String> = rows.into_iter().collect();
    m.get("wsrep_cluster_size").map(String::as_str) == Some(size)
        && m.get("wsrep_local_state_comment").map(String::as_str) == Some("Synced")
        && m.get("wsrep_cluster_status").map(String::as_str) == Some("Primary")
}

// ============================================================================
// Configurations
// ============================================================================

fn controller_toml(seed: &str, work: &Path) -> String {
    format!(
        r#"[node]
dc = "{DC}"
id = "ctl-{DC}"
[meta]
dc_seeds = ["{seed}"]
global_seeds = []
deployment_id = "{DEPLOYMENT}"
[controller]
bootstrap = true
global_candidate = true
admin_addr = "127.0.0.1:{CTL_ADMIN}"
internal_addr = "127.0.0.1:{CTL_INTERNAL}"
[telemetry]
bootstrap_log_level = "info"
log_format = "pretty"
bootstrap_addr = "127.0.0.1:{CTL_TEL}"
[audit]
spool_dir = "{w}/audit"
sink_dir = "{w}/audit-sink"
"#,
        w = work.display()
    )
}

fn gateway_toml(seed: &str, backends: &Path) -> String {
    format!(
        r#"[node]
dc = "{DC}"
id = "gw-{DC}"
[meta]
dc_seeds = ["{seed}"]
global_seeds = []
deployment_id = "{DEPLOYMENT}"
[gateway]
login_mode = "catalog"
backends_file = "{b}"
advertise_addr = "127.0.0.1"
[gateway.listen_override]
mysql = "127.0.0.1:{GW_MYSQL}"
mariadb = "127.0.0.1:{GW_MARIADB}"
http = "127.0.0.1:{GW_HTTP}"
https = "127.0.0.1:{GW_HTTPS}"
peer = "127.0.0.1:{GW_PEER}"
[telemetry]
bootstrap_log_level = "info"
log_format = "pretty"
bootstrap_addr = "127.0.0.1:{GW_TEL}"
"#,
        b = backends.display(),
    )
}

fn agent_toml(seed: &str, cluster: &str, node: &str, tel: u16) -> String {
    format!(
        r#"[node]
dc = "{DC}"
id = "agent-{node}"
[meta]
dc_seeds = ["{seed}"]
global_seeds = []
deployment_id = "{DEPLOYMENT}"
[agent]
cluster = "{cluster}"
node = "{node}"
[telemetry]
bootstrap_log_level = "info"
log_format = "pretty"
bootstrap_addr = "127.0.0.1:{tel}"
"#
    )
}

// ============================================================================
// The workload (list-append through the gateway)
// ============================================================================

fn pool(tenant: &str) -> Pool {
    Pool::new(
        OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(GW_MYSQL)
            .user(Some(format!("{tenant}.app")))
            .pass(Some(APP_PW))
            .ssl_opts(
                SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true),
            )
            .db_name(Some("la"))
            .pool_opts(
                PoolOpts::new()
                    .with_constraints(PoolConstraints::new(0, 4).expect("constraints"))
                    .with_inactive_connection_ttl(Duration::from_secs(60)),
            ),
    )
}

struct Workload {
    stop: Arc<AtomicBool>,
    records: Arc<Mutex<Vec<OpRecord>>>,
    errors: Arc<Mutex<BTreeMap<String, u64>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    pool: Pool,
}

fn start_workload(tenant: &str, clients: u32) -> Workload {
    let stop = Arc::new(AtomicBool::new(false));
    let records = Arc::new(Mutex::new(Vec::new()));
    let errors = Arc::new(Mutex::new(BTreeMap::new()));
    let next = Arc::new(AtomicU64::new(1));
    let pl = pool(tenant);
    let tasks = (0..clients)
        .map(|p| {
            tokio::spawn(client(
                p,
                pl.clone(),
                Arc::clone(&stop),
                Arc::clone(&records),
                Arc::clone(&next),
                Arc::clone(&errors),
            ))
        })
        .collect();
    Workload {
        stop,
        records,
        errors,
        tasks,
        pool: pl,
    }
}

impl Workload {
    fn acks(&self) -> usize {
        self.records
            .lock()
            .expect("lock")
            .iter()
            .filter(|r| r.kind == OpKind::Ok)
            .count()
    }

    /// Stops; returns the history and the acknowledged appends.
    async fn stop(self) -> (Vec<OpRecord>, Vec<(u64, u64)>, BTreeMap<String, u64>) {
        self.stop.store(true, Ordering::SeqCst);
        for t in self.tasks {
            let _ = t.await;
        }
        let _ = self.pool.disconnect().await;
        let all = self.records.lock().expect("lock").clone();
        let mut acked = Vec::new();
        for r in &all {
            if r.kind != OpKind::Ok {
                continue;
            }
            for m in r.value.as_array().into_iter().flatten() {
                if m[0] == "append" {
                    acked.push((m[1].as_u64().unwrap_or(0), m[2].as_u64().unwrap_or(0)));
                }
            }
        }
        let errors = self.errors.lock().expect("lock").clone();
        (all, acked, errors)
    }
}

async fn client(
    p: u32,
    pool: Pool,
    stop: Arc<AtomicBool>,
    records: Arc<Mutex<Vec<OpRecord>>>,
    next: Arc<AtomicU64>,
    errors: Arc<Mutex<BTreeMap<String, u64>>>,
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
    let mut seed = u64::from(p) * 7919 + 17;
    let mut rnd = move |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    while !stop.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(20)).await;
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
            .map(|(a, k, v)| {
                if *a {
                    json!(["append", k, v])
                } else {
                    json!(["r", k, null])
                }
            })
            .collect();
        let mut c = match tokio::time::timeout(Duration::from_secs(3), pool.get_conn()).await {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                *errors.lock().expect("lock").entry(code(&e)).or_default() += 1;
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            Err(_) => continue,
        };
        rec(OpKind::Invoke, Value::Array(invoke.clone()));
        match tokio::time::timeout(Duration::from_secs(30), txn(&mut c, &mops)).await {
            Ok(Ok(done)) => rec(OpKind::Ok, Value::Array(done)),
            Ok(Err(e)) => {
                *errors.lock().expect("lock").entry(code(&e)).or_default() += 1;
                // Refused before it could commit: failed. Anything else:
                // indeterminate.
                let definite = match &e {
                    mysql_async::Error::Server(s) => {
                        matches!(
                            s.code,
                            39501 | 39530 | 1290 | 1142 | 1213 | 1205 | 1020 | 1047 | 1180
                        ) && !s.message.contains("outcome is unknown")
                    }
                    _ => false,
                };
                if definite {
                    let _ = c.query_drop("ROLLBACK").await;
                    rec(OpKind::Fail, Value::Array(invoke));
                } else {
                    rec(OpKind::Info, Value::Array(invoke));
                    drop(c);
                }
            }
            Err(_) => rec(OpKind::Info, Value::Array(invoke)),
        }
    }
}

fn code(e: &mysql_async::Error) -> String {
    match e {
        mysql_async::Error::Server(s) => s.code.to_string(),
        other => format!(
            "io: {}",
            other.to_string().chars().take(50).collect::<String>()
        ),
    }
}

async fn txn(
    c: &mut mysql_async::Conn,
    mops: &[(bool, u64, u64)],
) -> std::result::Result<Vec<Value>, mysql_async::Error> {
    // The gateway passes the isolation level through; SERIALIZABLE so the
    // history can be checked as such (REPEATABLE READ's update-in-place
    // reads show up as elle's `internal` and `lost-update`).
    c.query_drop("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .await?;
    c.query_drop("BEGIN").await?;
    let mut done = Vec::new();
    for (append, k, v) in mops {
        if *append {
            c.exec_drop(
                "INSERT INTO la (k, v) VALUES (?, ?) ON DUPLICATE KEY UPDATE v = CONCAT(v, ',', VALUES(v))",
                (k, v.to_string()),
            )
            .await?;
            done.push(json!(["append", k, v]));
        } else {
            let cur: Option<String> = c.exec_first("SELECT v FROM la WHERE k = ?", (k,)).await?;
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

async fn final_lists(url: &str, schema: &str) -> BTreeMap<u64, Vec<u64>> {
    let mut c = mysql_async::Conn::new(opts(url)).await.expect("connect");
    let rows: Vec<(u64, String)> = c
        .query(format!("SELECT k, v FROM `{schema}`.la"))
        .await
        .expect("read");
    let _ = c.disconnect().await;
    rows.into_iter()
        .map(|(k, v)| (k, v.split(',').filter_map(|x| x.parse().ok()).collect()))
        .collect()
}

fn check_acked(lists: &BTreeMap<u64, Vec<u64>>, acked: &[(u64, u64)]) {
    let lost: Vec<_> = acked
        .iter()
        .filter(|(k, v)| !lists.get(k).is_some_and(|l| l.contains(v)))
        .collect();
    assert!(lost.is_empty(), "acknowledged appends lost: {lost:?}");
    for (k, l) in lists {
        let mut seen = std::collections::BTreeSet::new();
        assert!(
            l.iter().all(|v| seen.insert(*v)),
            "key {k} holds a value twice: {l:?}"
        );
    }
}

fn elle(history: &[OpRecord], model: &str) {
    let Some(cli) = ElleCli::from_env() else {
        eprintln!("SPRIG_TEST_ELLE unset: no elle check");
        return;
    };
    let mut h = history.to_vec();
    h.sort_by_key(|r| r.time_ns);
    let v = cli
        .check(&h, ElleModel::ListAppend, &[model])
        .expect("elle runs");
    eprintln!(
        "elle over {} records: valid {:?} anomalies {:?}",
        h.len(),
        v.valid,
        v.anomalies
    );
    assert_eq!(v.valid, Some(true), "elle: {}", v.detail);
}

// ============================================================================
// The shared set-up
// ============================================================================

struct Deployment {
    store: Arc<dyn MetaStore>,
    seed: String,
    procs: Procs,
    sprigctl: PathBuf,
    work: PathBuf,
    backends: PathBuf,
    creds: PathBuf,
    _etcd: OwnContainer,
}

static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn deploy(label: &str) -> Option<Deployment> {
    deploy_via(label, None).await.map(|(d, _)| d)
}

/// The deployment; with `tox`, every process reaches etcd through a
/// toxiproxy route of its own (returned), on toxiproxy's network.
async fn deploy_via(
    label: &str,
    tox: Option<&sprig_testkit::faults::toxiproxy::Toxiproxy>,
) -> Option<(Deployment, Option<sprig_testkit::faults::toxiproxy::Proxy>)> {
    let Some((sprigdb, sprigctl)) = bins() else {
        eprintln!("build sprigdb and sprigctl first (cargo build -p sprigdb -p sprigctl)");
        return None;
    };
    let work = std::env::temp_dir().join(format!("r2next-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).expect("work dir");
    let ep = free_port();
    let etcd_pub = format!("127.0.0.1:{ep}:2379");
    let tox_net = tox.map(|_| {
        docker(&[
            "inspect",
            "-f",
            "{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}",
            "sprigdb-dev-toxiproxy-1",
        ])
        .expect("toxiproxy network")
        .split_whitespace()
        .next()
        .expect("network")
        .to_owned()
    });
    let mut etcd_args: Vec<&str> = vec!["--memory", "256m"];
    if let Some(n) = &tox_net {
        etcd_args.extend(["--network", n.as_str()]);
    }
    etcd_args.extend(["-p", &etcd_pub]);
    let etcd = OwnContainer::run(
        "r2next-etcd",
        &[
            etcd_args.as_slice(),
            &[
                "quay.io/coreos/etcd:v3.6.5",
                "etcd",
                "--listen-client-urls=http://0.0.0.0:2379",
                // Not a host address: endpoint discovery must never add it
                // (127.0.0.1:2379 is the shared dev etcd).
                "--advertise-client-urls=http://r2next-etcd.invalid:2379",
            ],
        ]
        .concat(),
    )
    .expect("etcd");
    let route = match tox {
        Some(t) => Some(
            t.own_proxy("r2next-etcd", &format!("{}:2379", etcd.name()))
                .await
                .expect("route"),
        ),
        None => None,
    };
    let direct = format!("http://127.0.0.1:{ep}");
    let seed = route
        .as_ref()
        .map_or_else(|| direct.clone(), |r| format!("http://{}", r.host_addr()));
    let mut cfg = EtcdConfig::new(vec![direct.clone()]);
    cfg.request_timeout = Duration::from_secs(2);
    let store: Arc<dyn MetaStore> = loop {
        match EtcdStore::connect(Tier::Global, &cfg).await {
            Ok(s) => break Arc::new(s),
            Err(_) => tokio::time::sleep(Duration::from_millis(300)).await,
        }
    };
    let certs = sprig_testkit::compose::compose_dir().join("gateway-certs");
    for (k, v) in [
        (
            "tls.bundles.default.cert_path",
            json!(certs.join("server.pem").display().to_string()),
        ),
        (
            "tls.bundles.default.key_path",
            json!(certs.join("server-key.pem").display().to_string()),
        ),
        ("listener.mysql.tls", json!("optional")),
        ("listener.mariadb.tls", json!("optional")),
        ("listener.http.tls", json!("off")),
        ("listener.https.tls", json!("required")),
    ] {
        put(
            &*store,
            &format!("/sprig/dc/{DC}/settings/{k}"),
            json!({"v": 1, "value": v, "set_by": "op:r2next", "reason": "live_cluster_failover_bins"}),
        )
        .await;
    }
    let creds = work.join("creds.toml");
    std::fs::write(
        &creds,
        format!(
            "[sprig_ctl]\nuser = \"root\"\npassword = \"{ROOT_PW}\"\ntls = \"skip_verify\"\n\
             [sprig_repl]\nuser = \"root\"\npassword = \"{ROOT_PW}\"\ntls = \"skip_verify\"\n"
        ),
    )
    .expect("creds");
    let backends = work.join("backends.toml");
    std::fs::write(
        &backends,
        "[per_user]\nsecret = \"file:backends.toml#backend_secret\"\ntls = \"skip_verify\"\n\
         [backend_secret]\npassword = \"r2next-per-user-secret\"\n",
    )
    .expect("backends");
    let mut procs = Procs {
        bin: sprigdb,
        work: work.clone(),
        kids: Vec::new(),
    };
    procs.start("ctl", "controller", &controller_toml(&seed, &work));
    assert!(
        ready(CTL_TEL, Duration::from_secs(120)).await,
        "the controller is ready"
    );
    let next = get(&*store, "/sprig/g/ids/tenant")
        .await
        .unwrap_or_default();
    if next["next"].as_u64().unwrap_or(0) < TENANT_BASE {
        put(
            &*store,
            "/sprig/g/ids/tenant",
            json!({"v": 1, "next": TENANT_BASE}),
        )
        .await;
    }
    Some((
        Deployment {
            store,
            seed,
            procs,
            sprigctl,
            work,
            backends,
            creds,
            _etcd: etcd,
        },
        route,
    ))
}

impl Deployment {
    async fn register(&self, name: &str, topology: &str, dbs: &[&Db]) -> String {
        let cred = format!("file:{}", self.creds.display());
        let nodes: Vec<String> = dbs
            .iter()
            .map(|d| format!("{}=127.0.0.1:{}", d.name, d.port))
            .collect();
        let mut args = vec!["cluster", "register", name, "--topology", topology];
        for n in &nodes {
            args.extend(["--node", n.as_str()]);
        }
        args.extend(["--credentials", &cred]);
        let v = ctl_retry(&self.sprigctl, &args, Duration::from_secs(60)).await;
        let id = v["cluster"]["cluster_id"]
            .as_str()
            .expect("cluster id")
            .to_owned();
        let t0 = Instant::now();
        loop {
            let v = ctl(&self.sprigctl, &["cluster", "list"]).unwrap_or_default();
            let c = v["clusters"]
                .as_array()
                .and_then(|a| a.iter().find(|x| x["cluster_id"] == id.as_str()))
                .cloned()
                .unwrap_or_default();
            if c["admin_state"] == "active" {
                break;
            }
            assert!(
                t0.elapsed() < Duration::from_secs(90),
                "{id} never active: {c}"
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        id
    }

    fn start_gateway(&mut self) {
        let toml = gateway_toml(&self.seed, &self.backends);
        self.procs.start("gw", "gateway", &toml);
    }

    /// Creates the tenant, its user and `la.la`; returns its id and schema.
    async fn tenant(&self, name: &str, cluster: &str, root: &str) -> (u64, String) {
        let v = ctl_retry(
            &self.sprigctl,
            &["tenant", "create", name, "--cluster", cluster],
            Duration::from_secs(60),
        )
        .await;
        let id = v["tenant"]["tenant_id"].as_u64().expect("tenant id");
        assert!(id >= TENANT_BASE, "tenant id range: {id}");
        ctl_retry(
            &self.sprigctl,
            &[
                "tenant",
                "user",
                "add",
                name,
                "app",
                "--password",
                APP_PW,
                "--native",
                "--if-not-exists",
            ],
            Duration::from_secs(30),
        )
        .await;
        let _ = ctl(
            &self.sprigctl,
            &[
                "settings",
                "set",
                "auth.mysql_native_password",
                "true",
                "--scope",
                &format!("tenant:{name}"),
                "--reason",
                "r2next",
            ],
        );
        let url = format!("mysql://{name}.app:{APP_PW}@127.0.0.1:{GW_MYSQL}");
        let o: mysql_async::Opts = OptsBuilder::from_opts(opts(&url))
            .ssl_opts(Some(
                SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true),
            ))
            .into();
        let t0 = Instant::now();
        loop {
            let r = async {
                let mut c = mysql_async::Conn::new(o.clone()).await?;
                c.query_drop("CREATE DATABASE IF NOT EXISTS la").await?;
                c.query_drop("CREATE TABLE IF NOT EXISTS la.la (k INT PRIMARY KEY, v TEXT NOT NULL) ENGINE=InnoDB").await?;
                c.disconnect().await
            };
            match r.await {
                Ok(()) => break,
                Err(e) if t0.elapsed() < Duration::from_secs(90) => {
                    eprintln!("schema for {name}: {e}; retrying");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(e) => panic!("schema for {name}: {e}"),
            }
        }
        let schema = scalar(
            root,
            &format!(
                "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME='la' AND TABLE_SCHEMA LIKE 't{id}\\_%'"
            ),
        )
        .await
        .expect("the tenant's schema");
        (id, schema)
    }

    async fn failover_record(&self, cluster: &str) -> Value {
        get(
            &*self.store,
            &format!("/sprig/dc/{DC}/cluster-failover/{cluster}"),
        )
        .await
        .unwrap_or_default()
    }

    fn done(self) {
        let work = self.work.clone();
        drop(self);
        if std::env::var_os("R2NEXT_KEEP_WORK").is_some() {
            eprintln!("logs kept in {}", work.display());
        } else {
            let _ = std::fs::remove_dir_all(&work);
        }
    }
}

// ============================================================================
// Primary + replicas
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn bins_primary_replica_kill9() {
    if !skip::flag(skip::DOCKER) {
        skip::report_missing(skip::DOCKER, "bins_primary_replica_kill9");
        return;
    }
    let _one = ONE_AT_A_TIME.lock().await;
    let Some(mut d) = deploy("pr").await else {
        return;
    };
    let net = Net(format!("sprig-sim-r2next-pr-{}", std::process::id()));
    docker(&["network", "create", &net.0]).expect("network");
    let db1 = mariadb(&net.0, "db1", 101, None);
    wait_for(&db1.root(), "SELECT 1", "1", Duration::from_secs(120)).await;
    let db2 = mariadb(&net.0, "db2", 102, Some("db1"));
    let db3 = mariadb(&net.0, "db3", 103, Some("db1"));
    for r in [&db2, &db3] {
        wait_for(&r.root(), "SELECT 1", "1", Duration::from_secs(120)).await;
    }
    wait_for(&db1.root(), "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME='Rpl_semi_sync_master_clients'", "2", Duration::from_secs(60)).await;
    let dbs = [&db1, &db2, &db3];
    let by_name: BTreeMap<&str, &Db> = dbs.iter().map(|x| (x.name, *x)).collect();
    // Registration's precheck wants one writable primary, and nodes boot
    // fenced: the operator unfences db1 once for it; its agent keeps it so.
    exec(&db1.root(), &["SET GLOBAL read_only = OFF"]).await;
    let cluster = d.register("r2next-pr", "primary_replica", &dbs).await;
    eprintln!("cluster {cluster}: db1 primary, db2 and db3 semi-sync replicas");
    for (i, db) in dbs.iter().enumerate() {
        let toml = agent_toml(&d.seed, &cluster, db.name, AGENT_TEL[i]);
        d.procs.start(&format!("agent-{}", db.name), "agent", &toml);
        assert!(
            ready(AGENT_TEL[i], Duration::from_secs(60)).await,
            "agent-{} ready",
            db.name
        );
    }
    wait_for(
        &db1.root(),
        "SELECT @@read_only",
        "0",
        Duration::from_secs(30),
    )
    .await;
    for r in [&db2, &db3] {
        assert_eq!(
            scalar(&r.root(), "SELECT @@read_only").await.as_deref(),
            Some("1"),
            "replicas stay read-only"
        );
    }
    eprintln!("agents: db1 writable, db2 and db3 read-only");

    // ---- the semi-sync guard: a primary unfenced with no replica attached
    exec(&db1.root(), &["CREATE DATABASE IF NOT EXISTS r2next_probe",
        "CREATE TABLE IF NOT EXISTS r2next_probe.t (id INT AUTO_INCREMENT PRIMARY KEY, at BIGINT)"]).await;
    for r in [&db2, &db3] {
        exec(&r.root(), &["STOP SLAVE"]).await;
    }
    wait_for(
        &db1.root(),
        "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%'",
        "0",
        Duration::from_secs(30),
    )
    .await;
    d.procs.stop("agent-db1");
    assert_eq!(
        scalar(&db1.root(), "SELECT @@read_only").await.as_deref(),
        Some("1"),
        "a stopped agent fences its node"
    );
    assert_eq!(
        scalar(&db1.root(), "SELECT @@rpl_semi_sync_master_enabled")
            .await
            .as_deref(),
        Some("1")
    );
    let toml = agent_toml(&d.seed, &cluster, "db1", AGENT_TEL[0]);
    d.procs.start("agent-db1", "agent", &toml);
    wait_for(
        &db1.root(),
        "SELECT @@read_only",
        "0",
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(
        scalar(&db1.root(), "SELECT @@rpl_semi_sync_master_enabled")
            .await
            .as_deref(),
        Some("0"),
        "the agent switched semi-sync off: no replica attached"
    );
    let t0 = Instant::now();
    exec(
        &db1.root(),
        &["INSERT INTO r2next_probe.t (at) VALUES (UNIX_TIMESTAMP())"],
    )
    .await;
    let first_commit = t0.elapsed();
    eprintln!("first commit with no replica attached: {first_commit:?}");
    assert!(
        first_commit < Duration::from_secs(3),
        "no wait for rpl_semi_sync_master_timeout ({first_commit:?})"
    );
    for r in [&db2, &db3] {
        exec(&r.root(), &["START SLAVE"]).await;
    }
    wait_for(
        &db1.root(),
        "SELECT @@rpl_semi_sync_master_enabled",
        "1",
        Duration::from_secs(30),
    )
    .await;
    wait_for(&db1.root(), "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME='Rpl_semi_sync_master_clients'", "2", Duration::from_secs(60)).await;
    exec(&db1.root(), &["DROP DATABASE r2next_probe"]).await;
    eprintln!("semi-sync back on with both replicas attached");

    // ---- the tenant and the workload ----
    d.start_gateway();
    assert!(
        ready(GW_TEL, Duration::from_secs(120)).await,
        "gateway ready"
    );
    let tenant = "r2next-pr";
    let (tid, schema) = d.tenant(tenant, &cluster, &db1.root()).await;
    eprintln!("tenant {tenant} ({tid}), schema {schema}");
    let wl = start_workload(tenant, 6);
    tokio::time::sleep(Duration::from_secs(8)).await;
    let before = wl.acks();
    assert!(before > 50, "the workload runs ({before} acks)");
    let rec = d.failover_record(&cluster).await;
    assert_eq!(rec["writer"], "db1", "the loop watches db1: {rec}");

    // ---- kill -9 the primary ----
    let killed = Instant::now();
    docker(&["kill", "-s", "KILL", db1.c.name()]).expect("kill");
    eprintln!("--- db1 killed (-9)");
    let t0 = Instant::now();
    let rec = loop {
        let r = d.failover_record(&cluster).await;
        if r["history"].as_array().is_some_and(|h| !h.is_empty()) {
            break r;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(60),
            "no failover within 60 s: {r}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let promoted_after = killed.elapsed();
    let ev = rec["history"][0].clone();
    let new = ev["to"].as_str().expect("to").to_owned();
    eprintln!(
        "controller failed over after {promoted_after:?}: {ev}; record state {}",
        rec["state"]
    );
    assert_eq!(ev["from"], "db1");
    assert!(new == "db2" || new == "db3");
    let other = if new == "db2" { "db3" } else { "db2" };
    let newdb = by_name[new.as_str()];
    wait_for(
        &newdb.root(),
        "SELECT @@read_only",
        "0",
        Duration::from_secs(30),
    )
    .await;
    let writable_after = killed.elapsed();
    eprintln!("{new} writable {writable_after:?} after the kill (its agent took the lease over)");
    let lease = get(
        &*d.store,
        &format!("/sprig/dc/{DC}/leases/writer/{cluster}"),
    )
    .await
    .unwrap_or_default();
    assert_eq!(lease["node_id"], new.as_str(), "lease: {lease}");
    assert_eq!(
        lease["agent"],
        format!("agent-{new}"),
        "taken over: {lease}"
    );
    let t0 = Instant::now();
    while master_host(&by_name[other].root()).await.as_deref() != Some(new.as_str()) {
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "{other} repointed at {new}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    eprintln!("{other} replicates from {new}");
    tokio::time::sleep(Duration::from_secs(6)).await;
    let after = wl.acks();
    assert!(after > before + 20, "writes resume ({before} -> {after})");

    // ---- a paused (slow) new primary doesn't fail over ----
    docker(&["pause", newdb.c.name()]).expect("pause");
    eprintln!("--- {new} paused for 12 s");
    tokio::time::sleep(Duration::from_secs(12)).await;
    docker(&["unpause", newdb.c.name()]).expect("unpause");
    let r = d.failover_record(&cluster).await;
    assert_eq!(
        r["history"].as_array().map_or(0, Vec::len),
        1,
        "no second failover: {r}"
    );
    assert_eq!(r["writer"], new.as_str());
    eprintln!("pause: no failover (state {})", r["state"]);
    tokio::time::sleep(Duration::from_secs(6)).await;
    let (history, acked, errors) = wl.stop().await;
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for r in &history {
        *counts.entry(format!("{:?}", r.kind)).or_default() += 1;
    }
    eprintln!(
        "history {counts:?}; {} acknowledged appends; errors {errors:?}",
        acked.len()
    );

    // ---- db1 returns: fenced, then rejoined or reported ----
    docker(&["start", db1.c.name()]).expect("start");
    // The host port is fixed (`-p 127.0.0.1:<port>:3306`): the same
    // after a restart.
    let db1_root = db1.root();
    wait_for(
        &db1_root,
        "SELECT @@read_only",
        "1",
        Duration::from_secs(120),
    )
    .await;
    let t0 = Instant::now();
    let former = loop {
        let r = d.failover_record(&cluster).await;
        let f = r["history"][0]["former"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        if f == "rejoined" || f == "diverged" {
            break r["history"][0].clone();
        }
        assert!(
            t0.elapsed() < Duration::from_secs(90),
            "the former writer was neither rejoined nor reported: {r}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    eprintln!("the former writer: {former}");
    assert_eq!(
        scalar(&db1_root, "SELECT @@read_only").await.as_deref(),
        Some("1"),
        "the former writer stays read-only"
    );
    if let Some(stmts) = former["errant"]["statements"].as_array() {
        for (k, v) in &acked {
            let needle = format!("'{v}'");
            assert!(
                !stmts.iter().any(|s| s
                    .as_str()
                    .is_some_and(|s| s.contains(&needle) && s.contains(&format!("{k}")))),
                "acknowledged {k}:{v} is among the diverged"
            );
        }
    }
    let lists = final_lists(&newdb.root(), &schema).await;
    check_acked(&lists, &acked);
    eprintln!(
        "RTO: lease to {new} {promoted_after:?}, writable {writable_after:?}; record rto_ms {} detect_ms {} loss {}",
        ev["rto_ms"], ev["detect_ms"], ev["loss_estimate_ms"]
    );
    elle(&history, "serializable");
    drop(net_after(net, [db1, db2, db3]));
    d.done();
}

/// Removes the containers before their network.
fn net_after<const N: usize>(net: Net, dbs: [Db; N]) -> Net {
    drop(dbs);
    net
}

// ============================================================================
// Galera
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn bins_galera_node_loss() {
    if !skip::flag(skip::DOCKER) {
        skip::report_missing(skip::DOCKER, "bins_galera_node_loss");
        return;
    }
    let _one = ONE_AT_A_TIME.lock().await;
    let Some(mut d) = deploy("galera").await else {
        return;
    };
    let net = Net(format!("sprig-sim-r2next-galera-{}", std::process::id()));
    docker(&["network", "create", &net.0]).expect("network");
    let g1 = galera(&net.0, 1);
    let t0 = Instant::now();
    while !synced(&g1, "1").await {
        assert!(t0.elapsed() < Duration::from_secs(120), "g1 bootstrapped");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let g2 = galera(&net.0, 2);
    let t0 = Instant::now();
    while !synced(&g2, "2").await {
        assert!(t0.elapsed() < Duration::from_secs(180), "g2 joined");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let g3 = galera(&net.0, 3);
    let t0 = Instant::now();
    while !synced(&g3, "3").await {
        assert!(t0.elapsed() < Duration::from_secs(180), "g3 joined");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    eprintln!("Galera Synced 3/3");
    let dbs = [&g1, &g2, &g3];
    let cluster = d.register("r2next-galera", "galera", &dbs).await;
    d.start_gateway();
    assert!(
        ready(GW_TEL, Duration::from_secs(120)).await,
        "gateway ready"
    );
    let tenant = "r2next-galera";
    let (tid, schema) = d.tenant(tenant, &cluster, &g1.root()).await;
    eprintln!("cluster {cluster}; tenant {tenant} ({tid}), schema {schema}");
    let wl = start_workload(tenant, 6);
    tokio::time::sleep(Duration::from_secs(8)).await;
    let before = wl.acks();
    assert!(before > 50, "the workload runs ({before} acks)");

    let killed = Instant::now();
    docker(&["kill", "-s", "KILL", g3.c.name()]).expect("kill");
    eprintln!("--- g3 killed (-9)");
    // The registry loop drops g3 from the writer set; the failover loop
    // records it outside the primary component.
    let t0 = Instant::now();
    let mut repicked = None;
    let mut recorded = None;
    while t0.elapsed() < Duration::from_secs(60) && (repicked.is_none() || recorded.is_none()) {
        let c = get(&*d.store, &format!("/sprig/dc/{DC}/clusters/{cluster}"))
            .await
            .unwrap_or_default();
        let ws: Vec<String> = c["galera"]["writers"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|w| w["node"].as_str().map(str::to_owned))
            .collect();
        if repicked.is_none() && !ws.is_empty() && !ws.iter().any(|n| n == "g3") {
            repicked = Some((killed.elapsed(), ws));
        }
        let r = d.failover_record(&cluster).await;
        if recorded.is_none() && r["reason"].as_str().is_some_and(|s| s.contains("g3")) {
            recorded = Some((killed.elapsed(), r["reason"].clone()));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    eprintln!("writer set re-picked: {repicked:?}; failover loop: {recorded:?}");
    assert!(repicked.is_some(), "the writer set dropped g3");
    assert!(recorded.is_some(), "the failover loop saw g3 outside");
    let r = d.failover_record(&cluster).await;
    assert_eq!(r["state"], "healthy", "quorum held: no bootstrap: {r}");
    tokio::time::sleep(Duration::from_secs(8)).await;
    let mid = wl.acks();
    assert!(mid > before + 20, "writes go on ({before} -> {mid})");

    docker(&["start", g3.c.name()]).expect("start");
    let healed = Instant::now();
    let g3b = g3;
    let t0 = Instant::now();
    loop {
        if synced(&g1, "3").await && synced(&g2, "3").await && synced(&g3b, "3").await {
            break;
        }
        assert!(t0.elapsed() < Duration::from_secs(240), "Synced 3/3 again");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    eprintln!("Synced 3/3 {:?} after the restart", healed.elapsed());
    tokio::time::sleep(Duration::from_secs(5)).await;
    let (history, acked, errors) = wl.stop().await;
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for r in &history {
        *counts.entry(format!("{:?}", r.kind)).or_default() += 1;
    }
    eprintln!(
        "history {counts:?}; {} acknowledged appends; errors {errors:?}",
        acked.len()
    );
    let lists = final_lists(&g1.root(), &schema).await;
    check_acked(&lists, &acked);
    elle(&history, "read-committed");
    assert!(
        synced(&g1, "3").await && synced(&g2, "3").await && synced(&g3b, "3").await,
        "Synced 3/3 at the end"
    );
    drop(net_after(net, [g1, g2, g3b]));
    d.done();
}

// ============================================================================
// The DC lease with a slow etcd
// ============================================================================

/// Appends acknowledged by the gateway, and 39501 answers, with times.
fn tally(history: &[OpRecord], errors: &BTreeMap<String, u64>) -> (usize, u64) {
    (
        history.iter().filter(|r| r.kind == OpKind::Ok).count(),
        errors.get("39501").copied().unwrap_or(0),
    )
}

/// A slow but reachable etcd must not fence the DC; a cut-off one must,
/// on time (the lead's priority after the 72-hour soak). Every process
/// reaches a private etcd through toxiproxy; 6 clients write through the
/// gateway. 60 s with 300-1,500 ms of added round trip (downstream
/// 900 +- 600 ms): no 39501. Then 25 s cut off: 39501 within the permits'
/// 7 s, and no acknowledgement after that until the heal.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn bins_dc_lease_survives_a_slow_etcd() {
    if !skip::flag(skip::DOCKER) {
        skip::report_missing(skip::DOCKER, "bins_dc_lease_survives_a_slow_etcd");
        return;
    }
    let Some(tox_url) = skip::var(skip::TOXIPROXY) else {
        skip::report_missing(skip::TOXIPROXY, "bins_dc_lease_survives_a_slow_etcd");
        return;
    };
    let _one = ONE_AT_A_TIME.lock().await;
    let tox = sprig_testkit::faults::toxiproxy::Toxiproxy::new(&tox_url).expect("toxiproxy");
    let Some((mut d, route)) = deploy_via("slow", Some(&tox)).await else {
        return;
    };
    let route = route.expect("route");
    let net = Net(format!("sprig-sim-r2next-slow-{}", std::process::id()));
    docker(&["network", "create", &net.0]).expect("network");
    let db = mariadb(&net.0, "db1", 101, None);
    wait_for(&db.root(), "SELECT 1", "1", Duration::from_secs(120)).await;
    // One node, no agent: unfenced by hand, no semi-sync.
    exec(
        &db.root(),
        &[
            "SET GLOBAL rpl_semi_sync_master_enabled = OFF",
            "SET GLOBAL read_only = OFF",
        ],
    )
    .await;
    let cluster = d.register("r2next-slow", "single", &[&db]).await;
    d.start_gateway();
    assert!(
        ready(GW_TEL, Duration::from_secs(120)).await,
        "gateway ready"
    );
    let tenant = "r2next-slow";
    let (_, schema) = d.tenant(tenant, &cluster, &db.root()).await;
    let wl = start_workload(tenant, 6);
    tokio::time::sleep(Duration::from_secs(10)).await;
    let base = wl.acks();
    assert!(base > 50, "the workload runs ({base} acks)");
    let fenced_before = wl
        .errors
        .lock()
        .expect("lock")
        .get("39501")
        .copied()
        .unwrap_or(0);

    // ---- slow: 300-1,500 ms more per round trip, for 60 s ----
    let down = tox
        .add_toxic(
            route.name(),
            &sprig_testkit::faults::toxiproxy::Toxic::latency(900, 600),
        )
        .await
        .expect("latency");
    eprintln!("--- etcd slow (900 +- 600 ms) for 60 s");
    let slow_from = Instant::now();
    tokio::time::sleep(Duration::from_secs(60)).await;
    tox.remove_toxic(route.name(), &down).await.expect("remove");
    let slow_acks = wl.acks() - base;
    let fenced_slow = wl
        .errors
        .lock()
        .expect("lock")
        .get("39501")
        .copied()
        .unwrap_or(0)
        - fenced_before;
    eprintln!(
        "slow etcd for {:?}: {slow_acks} transactions acknowledged, {fenced_slow} answered 39501",
        slow_from.elapsed()
    );
    tokio::time::sleep(Duration::from_secs(10)).await;

    // ---- cut off: the DC must fence itself on time ----
    let before_cut = wl
        .errors
        .lock()
        .expect("lock")
        .get("39501")
        .copied()
        .unwrap_or(0);
    let cut_ns = now_ns();
    tox.set_enabled(route.name(), false).await.expect("cut");
    eprintln!("--- etcd cut off for 25 s");
    let mut first_fence = None;
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(25) {
        let n = wl
            .errors
            .lock()
            .expect("lock")
            .get("39501")
            .copied()
            .unwrap_or(0);
        if first_fence.is_none() && n > before_cut {
            first_fence = Some(t0.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let heal_ns = now_ns();
    tox.set_enabled(route.name(), true).await.expect("heal");
    eprintln!("cut: first 39501 after {first_fence:?}");
    tokio::time::sleep(Duration::from_secs(20)).await;
    let (history, acked, errors) = wl.stop().await;
    let (oks, fenced) = tally(&history, &errors);
    // Acknowledged while cut off, past the permits' 7 s and a second of
    // slack: none allowed.
    let late: Vec<&OpRecord> = history
        .iter()
        .filter(|r| {
            r.kind == OpKind::Ok && r.time_ns > cut_ns + 8_000_000_000 && r.time_ns < heal_ns
        })
        .collect();
    eprintln!(
        "history: {oks} ok, {fenced} x 39501 in all; errors {errors:?}; acknowledged after the fence {}",
        late.len()
    );
    let lists = final_lists(&db.root(), &schema).await;
    check_acked(&lists, &acked);
    assert_eq!(fenced_slow, 0, "no 39501 while etcd was only slow");
    assert!(slow_acks > 100, "writes went on while etcd was slow");
    let f = first_fence.expect("the DC fenced itself when cut off");
    assert!(f <= Duration::from_secs(9), "fenced on time: {f:?}");
    assert!(late.is_empty(), "nothing acknowledged after the fence");
    drop(route);
    drop(net_after(net, [db]));
    d.done();
}
