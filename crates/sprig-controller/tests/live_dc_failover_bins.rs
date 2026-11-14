//! DC failover with the real binaries: `live_dc_failover.rs`'s twin, with `sprigdb controller`,
//! `sprigdb gateway` and `sprigdb agent` processes in place of the test's
//! stand-ins, set up with `sprigctl` as an operator would.
//!
//! ```sh
//! cargo build -p sprigdb -p sprigctl
//! SPRIG_TEST_DOCKER=1 SPRIG_TEST_TOXIPROXY=http://127.0.0.1:8474 \
//!   cargo test -p sprig-controller --test live_dc_failover_bins -- --nocapture --test-threads=1
//! ```
//!
//! Three simulated DCs share one private etcd (single-etcd mode), each
//! reaching it through its own toxiproxy route: `fra1` (controller,
//! gateway on 34306/34307, agent), `ams1` (controller, gateway on
//! 34316/34317, agent) and the witness `wit1` (controller, the only global
//! candidate). `fra1-db` is the primary of both tenants' cluster; `ams1-db`
//! is its semi-sync replica, recorded as the standby of `r2wire-moves`
//! (`sprigctl tenant standby`); `r2wire-pinned` is pinned to `fra1`.
//! Clients in both DCs run list-append transactions on both tenants
//! through their own DC's gateway (tenant ids from 3,430,600,000, rule
//! 8b).
//!
//! - [`bins_dc_cut_from_the_global_tier`]: `fra1`'s route to etcd is
//!   disabled for 30 s;
//! - [`bins_dc_frozen_by_a_process_pause`]: `fra1`'s three processes are
//!   `SIGSTOP`ped for 30 s.
//!
//! Checked: `ams1` and `wit1` were activated by the global leader's
//! `dc_registry` loop once their leases and latency rows existed; the
//! moving tenant failed over to `ams1` only after `fra1`'s lease vanished,
//! with two `down` votes, and the pinned one stayed; `fra1`'s gateway
//! acknowledged nothing while fenced; every acknowledged append is in its
//! tenant's final home; `fra1` returned with the moved tenant's copy
//! fenced (no write privileges) and its diverged transactions in the
//! failover record, none of them acknowledged; a 3,000-client reconnect
//! to `fra1`'s gateway was admitted at the step's rate with 39531 hints of
//! 1-5 s; `fra1` served its pinned tenant again; `sprigctl tenant
//! failover` refuses a pinned tenant and a healthy home; elle (with
//! `SPRIG_TEST_ELLE=1`) finds the merged history read-committed.

#![allow(
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::doc_markdown,
    clippy::similar_names,
    clippy::too_many_arguments
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
use sprig_testkit::faults::toxiproxy::Toxiproxy;
use sprig_testkit::skip;

const ROOT_PW: &str = "sprig-r2wire-root";
const REPL_PW: &str = "sprig-r2wire-repl";
const APP_PW: &str = "sprig-r2wire-app";
const DEPLOYMENT: &str = "sprigdb-r2wire";
/// A private deployment's tenant ids: its MySQL port 34306 x 100,000.
const TENANT_BASE: u64 = 3_430_600_000;
const T_MOVES: &str = "r2wire-moves";
const T_PINNED: &str = "r2wire-pinned";
const KEYS: u64 = 6;
const DCS: [&str; 3] = ["fra1", "ams1", "wit1"];

/// This test's ports (34306, 34307, 34316-34337).
struct Ports {
    mysql: u16,
    http: u16,
    https: u16,
    peer: u16,
    gw_tel: u16,
    admin: u16,
    internal: u16,
    ctl_tel: u16,
    agent_tel: u16,
}

fn ports(dc: &str) -> Ports {
    match dc {
        "fra1" => Ports {
            mysql: 34306,
            http: 34318,
            https: 34319,
            peer: 34320,
            gw_tel: 34321,
            admin: 34326,
            internal: 34327,
            ctl_tel: 34328,
            agent_tel: 34335,
        },
        "ams1" => Ports {
            mysql: 34316,
            http: 34322,
            https: 34323,
            peer: 34324,
            gw_tel: 34325,
            admin: 34329,
            internal: 34330,
            ctl_tel: 34331,
            agent_tel: 34336,
        },
        _ => Ports {
            mysql: 0,
            http: 0,
            https: 0,
            peer: 0,
            gw_tel: 0,
            admin: 34332,
            internal: 34333,
            ctl_tel: 34334,
            agent_tel: 0,
        },
    }
}

fn mariadb_port(dc: &str) -> u16 {
    // The MariaDB persona, one above the MySQL one.
    ports(dc).mysql + 1
}

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
        let log = std::fs::File::create(self.work.join(format!("{name}.log"))).expect("log");
        let child = Command::new(&self.bin)
            .args(["--config", cfg.to_str().expect("path"), role])
            .env("RUST_LOG", "info,sprig_controller::failover=debug")
            .stdout(Stdio::from(log.try_clone().expect("log")))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("spawn sprigdb");
        self.kids.push((name.to_owned(), child));
    }

    fn pids(&self, prefix: &str) -> Vec<String> {
        self.kids
            .iter()
            .filter(|(n, _)| n.ends_with(prefix))
            .map(|(_, c)| c.id().to_string())
            .collect()
    }

    fn signal(&self, dc: &str, sig: &str) {
        for pid in self.pids(dc) {
            let _ = Command::new("kill").args([sig, &pid]).status();
        }
    }

    /// The processes that exited.
    fn exited(&mut self) -> Vec<String> {
        self.kids
            .iter_mut()
            .filter_map(|(n, c)| c.try_wait().ok().flatten().map(|_| n.clone()))
            .collect()
    }
}

impl Drop for Procs {
    fn drop(&mut self) {
        for (_, c) in &mut self.kids {
            let _ = Command::new("kill")
                .args(["-CONT", &c.id().to_string()])
                .status();
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

async fn metric(port: u16, name: &str) -> Option<f64> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .ok()?;
    s.write_all(b"GET /metrics HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n")
        .await
        .ok()?;
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), s.read_to_end(&mut buf))
        .await
        .ok()?
        .ok()?;
    String::from_utf8_lossy(&buf)
        .lines()
        .find(|l| l.starts_with(name) && !l.starts_with('#'))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
}

// ============================================================================
// sprigctl, etcd, MariaDB
// ============================================================================

fn ctl(sprigctl: &Path, dc: &str, args: &[&str]) -> std::result::Result<Value, String> {
    let out = Command::new(sprigctl)
        .args([
            "--endpoint",
            &format!("http://127.0.0.1:{}", ports(dc).admin),
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

async fn ctl_retry(sprigctl: &Path, dc: &str, args: &[&str], within: Duration) -> Value {
    let t0 = Instant::now();
    loop {
        match ctl(sprigctl, dc, args) {
            Ok(v) => return v,
            Err(e) if t0.elapsed() < within => {
                eprintln!("retrying: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => panic!("{e}"),
        }
    }
}

/// `/sprig/g/<rel>` or `/sprig/dc/<dc>/<rel>`.
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

async fn wait_for(url: &str, sql: &str, want: &str, within: Duration) {
    let t0 = Instant::now();
    while t0.elapsed() < within {
        if scalar(url, sql).await.as_deref() == Some(want) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("{sql} never gave {want}");
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
    // Each cluster logs its own writes in its own GTID domain, and the
    // standby doesn't replicate SprigDB's per-cluster schema (each
    // cluster's controller writes its own heartbeats there).
    let domain = if master.is_none() {
        "--gtid-domain-id=1"
    } else {
        "--gtid-domain-id=2"
    };
    args.extend([
        "mariadb:11.8",
        &sid,
        domain,
        "--replicate-ignore-db=__sprig_sys",
        "--replicate-wild-ignore-table=__sprig_sys.%",
        "--innodb-buffer-pool-size=64M",
        "--max-connections=400",
        // A node boots fenced; its agent unfences it.
        "--read-only=ON",
        // Above the failover's duration: a commit waiting for a standby
        // that stopped replicating never falls back to asynchronous and is
        // acknowledged before the old primary fences (the default 10 s
        // fallback would trade that for the RPO).
        "--rpl-semi-sync-master-timeout=45000",
    ]);
    if master.is_none() {
        args.push("--rpl-semi-sync-master-enabled=ON");
    }
    (
        OwnContainer::run(&format!("r2wire-{name}"), &args).expect("mariadb"),
        p,
    )
}

// ============================================================================
// Configurations
// ============================================================================

fn controller_toml(dc: &str, seed: &str, work: &Path) -> String {
    let p = ports(dc);
    format!(
        r#"[node]
dc = "{dc}"
id = "ctl-{dc}"
[meta]
dc_seeds = ["{seed}"]
global_seeds = []
deployment_id = "{DEPLOYMENT}"
[controller]
bootstrap = {boot}
global_candidate = {cand}
admin_addr = "127.0.0.1:{admin}"
internal_addr = "127.0.0.1:{internal}"
[telemetry]
bootstrap_log_level = "info"
log_format = "pretty"
bootstrap_addr = "127.0.0.1:{tel}"
[audit]
spool_dir = "{w}/audit-{dc}"
sink_dir = "{w}/audit-sink-{dc}"
"#,
        boot = dc == "fra1",
        cand = dc == "wit1",
        admin = p.admin,
        internal = p.internal,
        tel = p.ctl_tel,
        w = work.display()
    )
}

fn gateway_toml(dc: &str, seed: &str, backends: &Path) -> String {
    let p = ports(dc);
    format!(
        r#"[node]
dc = "{dc}"
id = "gw-{dc}"
[meta]
dc_seeds = ["{seed}"]
global_seeds = []
deployment_id = "{DEPLOYMENT}"
[gateway]
login_mode = "catalog"
backends_file = "{b}"
advertise_addr = "127.0.0.1"
[gateway.listen_override]
mysql = "127.0.0.1:{my}"
mariadb = "127.0.0.1:{ma}"
http = "127.0.0.1:{ht}"
https = "127.0.0.1:{hs}"
peer = "127.0.0.1:{pe}"
[telemetry]
bootstrap_log_level = "info"
log_format = "pretty"
bootstrap_addr = "127.0.0.1:{tel}"
"#,
        b = backends.display(),
        my = p.mysql,
        ma = mariadb_port(dc),
        ht = p.http,
        hs = p.https,
        pe = p.peer,
        tel = p.gw_tel
    )
}

fn agent_toml(dc: &str, seed: &str, cluster: &str) -> String {
    format!(
        r#"[node]
dc = "{dc}"
id = "agent-{dc}"
[meta]
dc_seeds = ["{seed}"]
global_seeds = []
deployment_id = "{DEPLOYMENT}"
[agent]
cluster = "{cluster}"
[telemetry]
bootstrap_log_level = "info"
log_format = "pretty"
bootstrap_addr = "127.0.0.1:{tel}"
"#,
        tel = ports(dc).agent_tel
    )
}

// ============================================================================
// The workload
// ============================================================================

fn pool(dc: &str, tenant: &str) -> Pool {
    Pool::new(
        OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(ports(dc).mysql)
            .user(Some(format!("{tenant}.app")))
            .pass(Some(APP_PW))
            .ssl_opts(
                SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true),
            )
            .db_name(Some("la"))
            .connect_attribute("sprig_dc", dc)
            .pool_opts(
                PoolOpts::new()
                    .with_constraints(PoolConstraints::new(0, 4).expect("constraints"))
                    .with_inactive_connection_ttl(Duration::from_secs(60)),
            ),
    )
}

/// One client's list-append transactions until `stop`.
async fn client(
    p: u32,
    tenant: &'static str,
    pool: Pool,
    stop: Arc<AtomicBool>,
    records: Arc<Mutex<Vec<OpRecord>>>,
    next: Arc<AtomicU64>,
    errors: Arc<Mutex<BTreeMap<String, u64>>>,
) {
    let key_base = if tenant == T_MOVES { 0 } else { 100 };
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
        tokio::time::sleep(Duration::from_millis(30)).await;
        let n = 1 + rnd(3);
        let mops: Vec<(bool, u64, u64)> = (0..n)
            .map(|_| {
                let k = key_base + rnd(KEYS);
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
        match tokio::time::timeout(Duration::from_secs(60), txn(&mut c, &mops)).await {
            Ok(Ok(done)) => rec(OpKind::Ok, Value::Array(done)),
            Ok(Err(e)) => {
                let k = code(&e);
                *errors.lock().expect("lock").entry(k.clone()).or_default() += 1;
                // Refused before running (39501, 39530, read-only, denied,
                // deadlock, timeout): failed. Anything else, and a 39501
                // that says the outcome is unknown: indeterminate.
                let definite = match &e {
                    mysql_async::Error::Server(s) => {
                        matches!(s.code, 39501 | 39530 | 1290 | 1142 | 1213 | 1205 | 1020)
                            && !s.message.contains("outcome is unknown")
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

/// 3,000 MySQL connections within a second: admitted (a greeting) or
/// refused with 39531 (its retry hint, ms).
async fn storm(port: u16) -> (u32, u32, Vec<u64>) {
    use tokio::io::AsyncReadExt;
    let t0 = Instant::now();
    let mut tasks = Vec::new();
    for i in 0..3000u64 {
        tasks.push(tokio::spawn(async move {
            tokio::time::sleep_until(tokio::time::Instant::from_std(
                t0 + Duration::from_micros(i * 333),
            ))
            .await;
            let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .ok()?;
            let mut buf = vec![0u8; 512];
            let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut buf))
                .await
                .ok()?
                .ok()?;
            let p = buf.get(4..n)?;
            if p.first() == Some(&0xff) {
                let code = u16::from_le_bytes([p[1], p[2]]);
                let msg = String::from_utf8_lossy(&p[9..]).to_string();
                let ms = msg
                    .rsplit("retry in ")
                    .next()
                    .and_then(|s| s.trim_end_matches(" ms").parse::<u64>().ok());
                Some((false, code, ms))
            } else {
                Some((true, 0, None))
            }
        }));
    }
    let (mut ok, mut refused, mut hints) = (0, 0, Vec::new());
    for t in tasks {
        match t.await.ok().flatten() {
            Some((true, _, _)) => ok += 1,
            Some((false, 39531, Some(ms))) => {
                refused += 1;
                hints.push(ms);
            }
            Some((false, c, _)) => panic!("refused with {c}, not 39531"),
            None => {}
        }
    }
    (ok, refused, hints)
}

// ============================================================================
// The scenario
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    CutFromGlobal,
    Freeze,
}

struct Net(String);

impl Drop for Net {
    fn drop(&mut self) {
        let _ = docker(&["network", "rm", &self.0]);
    }
}

static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

async fn scenario(fault: Fault) {
    let _one = ONE_AT_A_TIME.lock().await;
    let Some(tox_url) = skip::var(skip::TOXIPROXY) else {
        skip::report_missing(skip::TOXIPROXY, "live_dc_failover_bins");
        return;
    };
    let Some((sprigdb, sprigctl)) = bins() else {
        eprintln!(
            "live_dc_failover_bins: build sprigdb and sprigctl first (cargo build -p sprigdb -p sprigctl)"
        );
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
    let work = std::env::temp_dir().join(format!("r2wire-bins-{}-{fault:?}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).expect("work dir");

    // ---- etcd and the DCs' routes to it ----
    let ep = free_port();
    let etcd_pub = format!("127.0.0.1:{ep}:2379");
    let etcd = OwnContainer::run(
        "r2wire-etcd",
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
            // Not a host address: the processes' endpoint discovery must
            // never add it (127.0.0.1:2379 is the shared dev etcd).
            "--advertise-client-urls=http://r2wire-etcd.invalid:2379",
        ],
    )
    .expect("etcd");
    let mut cfg = EtcdConfig::new(vec![format!("http://127.0.0.1:{ep}")]);
    cfg.request_timeout = Duration::from_secs(2);
    let store: Arc<dyn MetaStore> = loop {
        match EtcdStore::connect(Tier::Global, &cfg).await {
            Ok(s) => break Arc::new(s),
            Err(_) => tokio::time::sleep(Duration::from_millis(300)).await,
        }
    };
    let mut routes = BTreeMap::new();
    for d in DCS {
        let p = tox
            .own_proxy(&format!("r2wire-{d}"), &format!("{}:2379", etcd.name()))
            .await
            .expect("route");
        routes.insert(d, p);
    }
    let seed = |d: &str| format!("http://{}", routes[d].host_addr());

    // ---- MariaDB: fra1-db primary, ams1-db its semi-sync replica ----
    let net = Net(format!("sprig-sim-r2wire-{}-{fault:?}", std::process::id()).to_lowercase());
    docker(&["network", "create", &net.0]).expect("network");
    let (fra1_db, fra1_port) = mariadb(&net.0, "fra1db", 101, None);
    let fra1_root = format!("mysql://root:{ROOT_PW}@127.0.0.1:{fra1_port}");
    wait_for(&fra1_root, "SELECT 1", "1", Duration::from_secs(120)).await;
    let (ams1_db, ams1_port) = mariadb(&net.0, "ams1db", 201, Some("fra1db"));
    let ams1_root = format!("mysql://root:{ROOT_PW}@127.0.0.1:{ams1_port}");
    wait_for(&ams1_root, "SELECT 1", "1", Duration::from_secs(120)).await;
    wait_for(&fra1_root, "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME='Rpl_semi_sync_master_clients'", "1", Duration::from_secs(60)).await;
    eprintln!("etcd, routes and MariaDB up (fra1-db {fra1_port}, ams1-db {ams1_port})");

    // ---- deployment settings, written as an operator's bootstrap does ----
    let certs = sprig_testkit::compose::compose_dir().join("gateway-certs");
    for d in DCS {
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
                &format!("/sprig/dc/{d}/settings/{k}"),
                json!({"v": 1, "value": v, "set_by": "op:r2wire", "reason": "live_dc_failover_bins"}),
            )
            .await;
        }
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
         [backend_secret]\npassword = \"r2wire-per-user-secret\"\n",
    )
    .expect("backends");

    let mut procs = Procs {
        bin: sprigdb.clone(),
        work: work.clone(),
        kids: Vec::new(),
    };
    // ---- controllers: fra1 bootstraps; ams1 and wit1 register ----
    procs.start(
        "ctl-fra1",
        "controller",
        &controller_toml("fra1", &seed("fra1"), &work),
    );
    assert!(
        ready(ports("fra1").ctl_tel, Duration::from_secs(120)).await,
        "ctl-fra1 ready"
    );
    // This deployment's tenant ids start at 34306 x 100,000.
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
    // fra1 runs a global voter too (three voter sites: automatic failover).
    let mut fra1 = get(&*store, "/sprig/g/dcs/fra1").await.expect("dcs/fra1");
    fra1["global_voter"] = json!(true);
    put(&*store, "/sprig/g/dcs/fra1", fra1).await;
    ctl_retry(
        &sprigctl,
        "fra1",
        &["dc", "register", "ams1", "--global-voter"],
        Duration::from_secs(30),
    )
    .await;
    ctl_retry(
        &sprigctl,
        "fra1",
        &[
            "dc",
            "register",
            "wit1",
            "--kind",
            "witness",
            "--global-voter",
        ],
        Duration::from_secs(30),
    )
    .await;
    procs.start(
        "ctl-ams1",
        "controller",
        &controller_toml("ams1", &seed("ams1"), &work),
    );
    procs.start(
        "ctl-wit1",
        "controller",
        &controller_toml("wit1", &seed("wit1"), &work),
    );
    for d in ["ams1", "wit1"] {
        assert!(
            ready(ports(d).ctl_tel, Duration::from_secs(120)).await,
            "ctl-{d} ready"
        );
    }
    // Joining DCs become active once they hold their
    // lease and latency rows exist both ways.
    let t0 = Instant::now();
    loop {
        let states: Vec<String> = futures::future::join_all(DCS.iter().map(|d| {
            let s = Arc::clone(&store);
            async move {
                get(&*s, &format!("/sprig/g/dcs/{d}"))
                    .await
                    .map(|v| v["state"].as_str().unwrap_or_default().to_owned())
                    .unwrap_or_default()
            }
        }))
        .await;
        if states.iter().all(|s| s == "active") {
            eprintln!(
                "all three DCs active after {:?} (dc_registry)",
                t0.elapsed()
            );
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(120),
            "DCs never activated: {states:?}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let lat = ctl(&sprigctl, "fra1", &["dc", "latency"]).expect("latency");
    eprintln!("latency matrix: {lat}");
    assert!(
        lat["rows"].as_array().is_some_and(|r| r.len() >= 6),
        "rows both ways between three DCs"
    );

    // ---- clusters, agents ----
    let cred = format!("file:{}", creds.display());
    let reg = |dc: &str, port: u16| {
        let v = ctl(
            &sprigctl,
            dc,
            &[
                "cluster",
                "register",
                &format!("{dc}-db"),
                "--topology",
                "single",
                "--node",
                &format!("127.0.0.1:{port}"),
                "--credentials",
                &cred,
            ],
        )
        .expect("register cluster");
        v["cluster"]["cluster_id"]
            .as_str()
            .expect("cluster id")
            .to_owned()
    };
    let fra1_c = reg("fra1", fra1_port);
    let ams1_c = reg("ams1", ams1_port);
    eprintln!("clusters {fra1_c} and {ams1_c}");
    for (dc, c) in [("fra1", &fra1_c), ("ams1", &ams1_c)] {
        let t0 = Instant::now();
        loop {
            let v = ctl(&sprigctl, dc, &["cluster", "list"]).unwrap_or_default();
            let st = v["clusters"]
                .as_array()
                .and_then(|a| a.iter().find(|x| x["cluster_id"] == c.as_str()))
                .map(|x| x["admin_state"].as_str().unwrap_or_default().to_owned());
            if st.as_deref() == Some("active") {
                break;
            }
            assert!(
                t0.elapsed() < Duration::from_secs(90),
                "{c} never active: {st:?}"
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        procs.start(
            &format!("agent-{dc}"),
            "agent",
            &agent_toml(dc, &seed(dc), c),
        );
        assert!(
            ready(ports(dc).agent_tel, Duration::from_secs(60)).await,
            "agent-{dc} ready"
        );
    }
    wait_for(
        &fra1_root,
        "SELECT @@read_only",
        "0",
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(
        scalar(&ams1_root, "SELECT @@read_only").await.as_deref(),
        Some("1"),
        "a standby stays read-only"
    );
    eprintln!("agents: fra1-db writable, ams1-db (replicating) read-only");

    // ---- gateways ----
    for dc in ["fra1", "ams1"] {
        procs.start(
            &format!("gw-{dc}"),
            "gateway",
            &gateway_toml(dc, &seed(dc), &backends),
        );
        assert!(
            ready(ports(dc).gw_tel, Duration::from_secs(120)).await,
            "gw-{dc} ready"
        );
    }

    // ---- tenants, users, the pin, the standby, the tables ----
    let mut ids = BTreeMap::new();
    for t in [T_MOVES, T_PINNED] {
        let v = ctl_retry(
            &sprigctl,
            "fra1",
            &["tenant", "create", t, "--cluster", &fra1_c],
            Duration::from_secs(60),
        )
        .await;
        let id = v["tenant"]["tenant_id"].as_u64().expect("tenant id");
        assert!(id >= TENANT_BASE, "tenant id range: {id}");
        ids.insert(t, id);
        ctl_retry(
            &sprigctl,
            "fra1",
            &[
                "tenant",
                "user",
                "add",
                t,
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
            &sprigctl,
            "fra1",
            &[
                "settings",
                "set",
                "auth.mysql_native_password",
                "true",
                "--scope",
                &format!("tenant:{t}"),
                "--reason",
                "r2wire",
            ],
        );
    }
    ctl_retry(
        &sprigctl,
        "fra1",
        &["tenant", "pin", T_PINNED, "--reason", "r2wire: pinned"],
        Duration::from_secs(30),
    )
    .await;
    let sb = ctl_retry(
        &sprigctl,
        "fra1",
        &[
            "tenant",
            "standby",
            T_MOVES,
            "--dc",
            "ams1",
            "--cluster",
            &ams1_c,
        ],
        Duration::from_secs(30),
    )
    .await;
    eprintln!("tenants {ids:?}; standby {sb}");
    for t in [T_MOVES, T_PINNED] {
        let url = format!("mysql://{t}.app:{APP_PW}@127.0.0.1:{}", ports("fra1").mysql);
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
                Err(e) if t0.elapsed() < Duration::from_secs(60) => {
                    eprintln!("schema for {t}: {e}; retrying");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(e) => panic!("schema for {t}: {e}"),
            }
        }
    }
    // The standby has everything and reports its lag.
    let moves_schema = scalar(
        &fra1_root,
        &format!(
            "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME='la' AND TABLE_SCHEMA LIKE 't{}\\_%'",
            ids[T_MOVES]
        ),
    )
    .await
    .expect("the moving tenant's schema");
    let pinned_schema = scalar(
        &fra1_root,
        &format!(
            "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME='la' AND TABLE_SCHEMA LIKE 't{}\\_%'",
            ids[T_PINNED]
        ),
    )
    .await
    .expect("the pinned tenant's schema");
    wait_for(
        &ams1_root,
        &format!(
            "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='{moves_schema}'"
        ),
        "1",
        Duration::from_secs(30),
    )
    .await;
    eprintln!("schemas {moves_schema} and {pinned_schema}; the standby has them");

    // ---- the workload ----
    let stop = Arc::new(AtomicBool::new(false));
    let records: Arc<Mutex<Vec<OpRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let errors: Arc<Mutex<BTreeMap<String, u64>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let next = Arc::new(AtomicU64::new(1));
    let mut clients = Vec::new();
    let mut pools = Vec::new();
    for (di, dc) in ["fra1", "ams1"].iter().enumerate() {
        for (ti, t) in [T_MOVES, T_PINNED].iter().enumerate() {
            let pl = pool(dc, t);
            pools.push(pl.clone());
            for c in 0..3u32 {
                let p = 100 * (u32::try_from(di).unwrap_or(0) + 1)
                    + u32::try_from(ti).unwrap_or(0) * 10
                    + c;
                clients.push(tokio::spawn(client(
                    p,
                    t,
                    pl.clone(),
                    Arc::clone(&stop),
                    Arc::clone(&records),
                    Arc::clone(&next),
                    Arc::clone(&errors),
                )));
            }
        }
    }

    // The truth: directory moves and lease deletions, at wall time.
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
    let acked_before = records
        .lock()
        .expect("lock")
        .iter()
        .filter(|r| r.kind == OpKind::Ok)
        .count();
    assert!(
        acked_before > 50,
        "the workload runs through both gateways ({acked_before} acks)"
    );
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
            eprintln!("--- fra1's controller, gateway and agent frozen (SIGSTOP) for 30 s");
            procs.signal("fra1", "-STOP");
            tokio::time::sleep(Duration::from_secs(30)).await;
            procs.signal("fra1", "-CONT");
        }
    }
    let healed_at = now_ns();
    eprintln!("--- healed; waiting for the return");
    let t0 = Instant::now();
    let mut returned_at = 0;
    while t0.elapsed() < Duration::from_secs(90) {
        let st = get(&*store, "/sprig/g/dcs/fra1").await.unwrap_or_default();
        if st["state"] == "active" && !moves.lock().expect("lock").is_empty() {
            returned_at = now_ns();
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert!(returned_at > 0, "fra1 must fail over and come back active");
    eprintln!(
        "fra1 active again {:.1} s after the heal",
        (returned_at - healed_at) as f64 / 1e9
    );

    // ---- the reconnect storm, once fra1's gateway admits in steps ----
    let t0 = Instant::now();
    let mut rate = 0.0;
    while t0.elapsed() < Duration::from_secs(30) {
        rate = metric(ports("fra1").gw_tel, "sprig_gateway_admission_rate")
            .await
            .unwrap_or(0.0);
        if rate > 0.0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        rate > 0.0,
        "fra1's gateway admits in steps after the return"
    );
    let (admitted, refused, hints) = storm(ports("fra1").mysql).await;
    eprintln!(
        "reconnect storm: rate {rate}/s, admitted {admitted}, refused {refused} (39531), hints {:?}..{:?} ms",
        hints.iter().min(),
        hints.iter().max()
    );

    // ---- the diverged report, written by fra1's leader ----
    let t0 = Instant::now();
    let report = loop {
        let v = ctl(
            &sprigctl,
            "ams1",
            &["tenant", "failover", T_MOVES, "--errant"],
        )
        .unwrap_or_default();
        if let Some(r) = v["failovers"]
            .as_array()
            .and_then(|a| a.iter().find(|f| f["outcome"] == "promoted"))
            .filter(|f| !f["errant"].is_null())
        {
            break r.clone();
        }
        assert!(
            t0.elapsed() < Duration::from_secs(60),
            "no diverged report: {v}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    eprintln!("failover with diverged report: {report}");

    // ---- manual failover's refusals ----
    let pinned = ctl(
        &sprigctl,
        "ams1",
        &["--dry-run", "tenant", "failover", T_PINNED, "--to", "ams1"],
    );
    assert!(
        pinned
            .as_ref()
            .is_err_and(|e| e.contains("39505") || e.contains("pinned")),
        "{pinned:?}"
    );
    let healthy = ctl(
        &sprigctl,
        "fra1",
        &["--dry-run", "tenant", "failover", T_MOVES, "--to", "fra1"],
    );
    assert!(healthy.is_err(), "{healthy:?}");

    // Let fra1 serve its pinned tenant again, then stop.
    let t0 = Instant::now();
    loop {
        let after = records
            .lock()
            .expect("lock")
            .iter()
            .filter(|r| {
                r.kind == OpKind::Ok && (110..120).contains(&r.process) && r.time_ns > returned_at
            })
            .count();
        if after > 0 {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(90),
            "fra1 never served its pinned tenant again"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    stop.store(true, Ordering::SeqCst);
    for c in clients {
        let _ = tokio::time::timeout(Duration::from_secs(70), c).await;
    }
    for p in pools {
        let _ = p.disconnect().await;
    }
    truth.abort();
    let dead = procs.exited();
    assert!(
        dead.is_empty(),
        "processes exited: {dead:?} (see {})",
        work.display()
    );

    // ---- checks ----
    let moves = moves.lock().expect("lock").clone();
    let gone = gone.lock().expect("lock").clone();
    eprintln!("moves {moves:?}; lease deletions {gone:?}");
    assert_eq!(moves.len(), 1, "exactly one tenant moved: {moves:?}");
    let (moved_at, moved_t, to) = moves[0].clone();
    assert_eq!(
        (moved_t, to.as_str()),
        (ids[T_MOVES], "ams1"),
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
        "promotion waited the drift margin"
    );
    eprintln!(
        "promotion {:.2} s after the lease vanished ({:.2} s after the fault)",
        (moved_at - lost) as f64 / 1e9,
        (moved_at - fault_at) as f64 / 1e9
    );
    let recs = ctl(
        &sprigctl,
        "ams1",
        &["tenant", "failover", T_MOVES, "--errant"],
    )
    .expect("records");
    let page = store
        .list(
            &Tier::Global.key("failovers/").expect("k"),
            sprig_meta::ListOpts::default(),
        )
        .await
        .expect("list");
    let done: Vec<Value> = page
        .kvs
        .iter()
        .filter_map(|kv| serde_json::from_slice::<Value>(&kv.value).ok())
        .filter(|r| r["state"] == "done")
        .collect();
    assert_eq!(done.len(), 1, "one finished failover: {recs}");
    let downs = done[0]["votes"]
        .as_array()
        .map_or(0, |v| v.iter().filter(|x| x["verdict"] == "down").count());
    assert!(downs >= 2, "two down votes: {}", done[0]["votes"]);

    let all = records.lock().expect("lock").clone();
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut acked: Vec<(u64, u64, u32, u64)> = Vec::new(); // (key, value, process, time)
    for r in &all {
        let dc = if r.process < 200 { "fra1" } else { "ams1" };
        *counts.entry(format!("{dc}:{:?}", r.kind)).or_default() += 1;
        if r.kind != OpKind::Ok {
            continue;
        }
        for m in r.value.as_array().into_iter().flatten() {
            if m[0] == "append" {
                acked.push((
                    m[1].as_u64().unwrap_or(0),
                    m[2].as_u64().unwrap_or(0),
                    r.process,
                    r.time_ns,
                ));
            }
        }
    }
    eprintln!(
        "history {counts:?}; {} acknowledged appends; errors {:?}",
        acked.len(),
        errors.lock().expect("lock")
    );
    // fra1's gateway acknowledges no write of a tenant homed in fra1 while
    // fra1 is fenced: from its permits' end (7 s after the fault, plus a
    // second of slack) to the return. Once the moving tenant is homed in
    // ams1, fra1's gateway may route it there (it does after a freeze,
    // when it sees the quarantined map): those acknowledgements are
    // ams1's, and the final-data check below covers them.
    let fence_from = fault_at + 8_000_000_000;
    let during: Vec<&(u64, u64, u32, u64)> = acked
        .iter()
        .filter(|(k, _, p, t)| {
            let homed_in_fra1 = *k >= 100 || *t < moved_at;
            *p < 200 && homed_in_fra1 && *t > fence_from && *t < returned_at
        })
        .collect();
    assert!(
        during.is_empty(),
        "fra1 acknowledged writes while fenced: {during:?}"
    );
    // Every acknowledged append is in its tenant's final home.
    let l_moves = final_lists(&ams1_root, &moves_schema).await;
    let l_pinned = final_lists(&fra1_root, &pinned_schema).await;
    for (k, v, p, _) in &acked {
        let l = if *k >= 100 { &l_pinned } else { &l_moves };
        assert!(
            l.get(k).is_some_and(|x| x.contains(v)),
            "acknowledged append {k}:{v} (process {p}) lost"
        );
    }
    // The returning copy is fenced: no write privilege on its schemas.
    let privs = scalar(
        &fra1_root,
        &format!(
            "SELECT IFNULL(GROUP_CONCAT(DISTINCT PRIVILEGE_TYPE), '') FROM information_schema.SCHEMA_PRIVILEGES \
             WHERE TABLE_SCHEMA = '{}' AND PRIVILEGE_TYPE IN ('INSERT','UPDATE','DELETE')",
            moves_schema.replace('_', "\\\\_")
        ),
    )
    .await
    .unwrap_or_default();
    let privs_like = scalar(
        &fra1_root,
        &format!(
            "SELECT COUNT(*) FROM mysql.db WHERE Db LIKE 't{}%' AND (Insert_priv='Y' OR Update_priv='Y' OR Delete_priv='Y')",
            ids[T_MOVES]
        ),
    )
    .await;
    eprintln!(
        "fra1's copy of {T_MOVES}: write privileges '{privs}', rows with write privileges {privs_like:?}"
    );
    assert_eq!(
        privs_like.as_deref(),
        Some("0"),
        "the moved tenant's copy in fra1 keeps no write privilege"
    );
    // The diverged report holds no acknowledged write.
    let errant = &report["errant"];
    for (k, v, _, _) in &acked {
        if *k < 100 {
            let needle = format!("({k}, '{v}')");
            assert!(
                !errant["statements"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|s| s.as_str().is_some_and(|s| s.contains(&needle))),
                "acknowledged {k}:{v} is diverged"
            );
        }
    }
    eprintln!(
        "diverged in fra1 (reported, never merged): {} transaction(s)",
        errant["count"]
    );
    assert!(
        rate > 0.0 && f64::from(admitted) <= rate * 1.5 + 5.0,
        "admission held to the step's rate"
    );
    assert!(refused > 0, "the storm was refused in part");
    assert!(
        hints.iter().min().copied().unwrap_or(0) >= 1000
            && hints.iter().max().copied().unwrap_or(0) <= 5000,
        "hints of 1-5 s"
    );
    if let Some(cli) = ElleCli::from_env() {
        let mut hist = all.clone();
        hist.sort_by_key(|r| r.time_ns);
        let v = cli
            .check(&hist, ElleModel::ListAppend, &["read-committed"])
            .expect("elle");
        eprintln!(
            "elle over {} records: valid {:?} anomalies {:?}",
            hist.len(),
            v.valid,
            v.anomalies
        );
        assert_eq!(v.valid, Some(true), "{}", v.detail);
    }
    drop(procs);
    drop((fra1_db, ams1_db, etcd, routes, net));
    let _ = std::fs::remove_dir_all(&work);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn bins_dc_cut_from_the_global_tier() {
    if !skip::flag(skip::DOCKER) {
        return;
    }
    Box::pin(scenario(Fault::CutFromGlobal)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn bins_dc_frozen_by_a_process_pause() {
    if !skip::flag(skip::DOCKER) {
        return;
    }
    Box::pin(scenario(Fault::Freeze)).await;
}
