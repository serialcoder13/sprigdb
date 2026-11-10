//! R2 hardening with the real binaries (r2harden): writes held while a
//! DC failover drains the new home (RL13), a mover's fence never reaching
//! clients as 1146 or 1049 (RL14), a promoted standby carrying the
//! tenant's triggers, events, views and routines (RL12), and tenant-scope
//! settings in the sharding loop (RL3).
//!
//! ```sh
//! cargo build -p sprigdb -p sprigctl
//! R2HARDEN_TENANT_BASE=<above every t<id> on the dev clusters> SPRIG_TEST_DOCKER=1 \
//!   cargo test -p sprig-controller --test live_r2harden_bins -- --nocapture --test-threads=1
//! ```
//!
//! Three DCs on one private etcd, as `live_r2last_bins` (fra1, ams1, sin1,
//! each with its own controller, gateway, worker, agent and 768 MB
//! `mariadb:11.8`; delay proxies make fra1-ams1 30 ms and sin1 120 ms):
//!
//! 1. RL12: S has a trigger, a view, a procedure and an event at fra1; its
//!    standby streams in ams1; a second trigger and a row are added at
//!    home: the copy gets the row, no trigger, and its event disabled.
//! 2. RL14 and RL3: T (readers and writers through all three gateways,
//!    prepared statements) moves fra1 -> ams1 -> sin1 -> ams1 while G (a
//!    tenant whose own `sharding.min_shards = 2` and `advisor.mode = off`
//!    shard it) splits twice under load: no client sees 1146 or 1049,
//!    nothing acknowledged is lost.
//! 3. RL13 and RL12: fra1 frozen, S promoted to ams1 under writes from
//!    fra1, ams1 and sin1: no client sees 1142, the gateways held
//!    statements, and on ams1's binlog every client write comes after the
//!    job's last; the promoted copy has both triggers, the event enabled,
//!    the view and the procedure, and its triggers fire.

#![allow(
    clippy::too_many_lines,
    clippy::too_many_arguments,
    clippy::many_single_char_names,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::doc_markdown,
    clippy::similar_names
)]

use std::collections::{BTreeMap, BTreeSet};
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
use sprig_testkit::skip;

const ROOT_PW: &str = "sprig-r2harden-root";
const REPL_PW: &str = "sprig-r2harden-repl";
const APP_PW: &str = "sprig-r2harden-app";
const DEPLOYMENT: &str = "sprigdb-r2harden";
/// Rules 8b and 8f: the tenant-id base, `R2HARDEN_TENANT_BASE`, chosen
/// at the moment of the run above every `t<id>` on the dev clusters
/// (13301, 13311, 13321) and checked there by the caller; only this
/// test's own containers hold these schemas.
fn tenant_base() -> Option<u64> {
    std::env::var("R2HARDEN_TENANT_BASE").ok()?.parse().ok()
}
const KEYS: u64 = 8;
const DCS: [&str; 3] = ["fra1", "ams1", "sin1"];
/// Delay each DC's controllers' internal service adds to a round trip
/// (a proxy in front of it), so the measured latency matrix is
/// fra1-ams1 30 ms, fra1-sin1 and ams1-sin1 120 ms.
fn delay_ms(dc: &str) -> u64 {
    if dc == "sin1" { 120 } else { 30 }
}

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
    worker_tel: u16,
    internal_proxy: u16,
}

fn ports(dc: &str) -> Ports {
    let b = match dc {
        "fra1" => 28506,
        "ams1" => 28526,
        _ => 28546,
    };
    Ports {
        mysql: b,
        http: b + 4,
        https: b + 5,
        peer: b + 6,
        gw_tel: b + 7,
        admin: b + 8,
        internal: b + 9,
        ctl_tel: b + 10,
        agent_tel: b + 11,
        worker_tel: b + 12,
        internal_proxy: b + 13,
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(0))
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
            .env(
                "RUST_LOG",
                "info,sprig_controller::standby=debug,sprig_controller::tenant_scopes=debug,sprig_gateway::exec::follow=debug,sprig_gateway::exec::fence_retry=debug",
            )
            .stdout(Stdio::from(log.try_clone().expect("log")))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("spawn sprigdb");
        self.kids.push((name.to_owned(), child));
    }

    fn signal(&self, dc: &str, sig: &str) {
        for (n, c) in &self.kids {
            if n.ends_with(dc) {
                let _ = Command::new("kill")
                    .args([sig, &c.id().to_string()])
                    .status();
            }
        }
    }
}

impl Drop for Procs {
    fn drop(&mut self) {
        for (_, c) in &mut self.kids {
            let id = c.id().to_string();
            let _ = Command::new("kill").args(["-CONT", &id]).status();
            let _ = Command::new("kill").args(["-TERM", &id]).status();
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

fn ctl(sprigctl: &Path, dc: &str, args: &[&str]) -> std::result::Result<Value, String> {
    let out = Command::new(sprigctl)
        .args([
            "--endpoint",
            &format!("http://127.0.0.1:{}", ports(dc).admin),
            "-o",
            "json",
        ])
        .args(args)
        .env("SPRIGCTL_ACTOR", "r2harden")
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

/// A MariaDB 11.8 node, 768 MB: `domain` is its GTID domain; `master` makes
/// it a replica.
fn mariadb(
    net: &str,
    name: &str,
    sid: u32,
    domain: u32,
    master: Option<&str>,
) -> (OwnContainer, u16) {
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
    let dom = format!("--gtid-domain-id={domain}");
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
        &dom,
        "--replicate-ignore-db=__sprig_sys",
        "--replicate-wild-ignore-table=__sprig_sys.%",
        "--innodb-buffer-pool-size=64M",
        "--max-connections=400",
        "--read-only=ON",
    ]);
    (
        OwnContainer::run(&format!("r2harden-{name}"), &args).expect("mariadb"),
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
internal_advertise = "127.0.0.1:{proxy}"
[telemetry]
bootstrap_log_level = "info"
log_format = "pretty"
bootstrap_addr = "127.0.0.1:{tel}"
[audit]
spool_dir = "{w}/audit-{dc}"
sink_dir = "{w}/audit-sink-{dc}"
"#,
        boot = dc == "fra1",
        // The global leader stays out of the DC that is frozen in part 6.
        cand = dc == "sin1",
        admin = p.admin,
        internal = p.internal,
        proxy = p.internal_proxy,
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
        ma = p.mysql + 1,
        ht = p.http,
        hs = p.https,
        pe = p.peer,
        tel = p.gw_tel
    )
}

fn worker_toml(dc: &str, seed: &str, work: &Path) -> String {
    format!(
        r#"[node]
dc = "{dc}"
id = "worker-{dc}"
[meta]
dc_seeds = ["{seed}"]
global_seeds = []
deployment_id = "{DEPLOYMENT}"
[recovery]
extract_dir = "{w}/extracts-{dc}"
extract_key_file = "{w}/extract.keys"
[telemetry]
bootstrap_log_level = "info"
log_format = "pretty"
bootstrap_addr = "127.0.0.1:{tel}"
"#,
        w = work.display(),
        tel = ports(dc).worker_tel
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
// The workload: list appends, one acknowledged value per append
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

#[derive(Default)]
struct Tally {
    acked: BTreeMap<u64, Vec<u64>>,
    errors: BTreeMap<String, u64>,
    unknown: u64,
    /// (acknowledged at, ms; took, ms) per acknowledged write.
    ack_ms: Vec<(u64, u64)>,
    /// (when, ms; code) per failed write.
    err_ms: Vec<(u64, String)>,
}

fn code(e: &mysql_async::Error) -> String {
    match e {
        mysql_async::Error::Server(s) => s.code.to_string(),
        other => format!(
            "io: {}",
            other.to_string().chars().take(40).collect::<String>()
        ),
    }
}

async fn final_lists(url: &str, schema: &str) -> BTreeMap<u64, BTreeSet<u64>> {
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

struct Net(String);

impl Drop for Net {
    fn drop(&mut self) {
        let _ = docker(&["network", "rm", &self.0]);
    }
}

// ============================================================================
// Helpers on the deployment
// ============================================================================

async fn entry(store: &dyn MetaStore, tenant: u64) -> Value {
    get(store, &format!("/sprig/g/tenants/{tenant}"))
        .await
        .unwrap_or_default()
}

async fn setting(sprigctl: &Path, dc: &str, k: &str, v: &str) {
    ctl_retry(
        sprigctl,
        dc,
        &[
            "settings", "set", k, v, "--scope", "dc", "--dc", dc, "--reason", "r2harden", "--yes",
        ],
        Duration::from_secs(30),
    )
    .await;
}

async fn job_state(store: &dyn MetaStore, dc: &str, job: &str) -> Value {
    get(store, &format!("/sprig/dc/{dc}/mover/jobs/{job}"))
        .await
        .unwrap_or_default()
}
// ============================================================================
// r2last: delay proxies, paced clients, settings
// ============================================================================

/// A TCP proxy on `listen` to `upstream` that holds every chunk `half`
/// before passing it on, both ways: a round trip through it costs about
/// twice `half`.
async fn delay_proxy(listen: u16, upstream: u16, half: Duration) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let l = tokio::net::TcpListener::bind(("127.0.0.1", listen))
        .await
        .expect("proxy bind");
    tokio::spawn(async move {
        loop {
            let Ok((inbound, _)) = l.accept().await else {
                continue;
            };
            tokio::spawn(async move {
                let Ok(out) = tokio::net::TcpStream::connect(("127.0.0.1", upstream)).await else {
                    return;
                };
                let (mut ri, mut wi) = inbound.into_split();
                let (mut ro, mut wo) = out.into_split();
                let up = async move {
                    let mut buf = vec![0u8; 16384];
                    while let Ok(n) = ri.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        tokio::time::sleep(half).await;
                        if wo.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                    let _ = wo.shutdown().await;
                };
                let down = async move {
                    let mut buf = vec![0u8; 16384];
                    while let Ok(n) = ro.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        tokio::time::sleep(half).await;
                        if wi.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                    let _ = wi.shutdown().await;
                };
                tokio::join!(up, down);
            });
        }
    });
}

async fn tenant_setting(sprigctl: &Path, dc: &str, tenant: &str, k: &str, v: &str) {
    ctl_retry(
        sprigctl,
        dc,
        &[
            "settings",
            "set",
            k,
            v,
            "--scope",
            &format!("tenant:{tenant}"),
            "--reason",
            "r2harden",
            "--yes",
        ],
        Duration::from_secs(30),
    )
    .await;
}

/// One paced client: every `every` it either appends `v` to key `k`
/// (autocommit, a write) or reads a key (a read).
async fn paced(
    p: u64,
    pool: Pool,
    write: bool,
    every: Duration,
    stop: Arc<AtomicBool>,
    next: Arc<AtomicU64>,
    tally: Arc<Mutex<Tally>>,
) {
    let mut seed = p * 7919 + 17;
    let mut rnd = move |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    while !stop.load(Ordering::SeqCst) {
        tick.tick().await;
        let k = rnd(KEYS);
        let mut c = match tokio::time::timeout(Duration::from_secs(3), pool.get_conn()).await {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                *tally
                    .lock()
                    .expect("lock")
                    .errors
                    .entry(code(&e))
                    .or_default() += 1;
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            Err(_) => continue,
        };
        if !write {
            let r = tokio::time::timeout(
                Duration::from_secs(10),
                c.exec_drop("SELECT v FROM la WHERE k = ?", (k,)),
            )
            .await;
            if let Ok(Err(e)) = r {
                *tally
                    .lock()
                    .expect("lock")
                    .errors
                    .entry(format!("read {}", code(&e)))
                    .or_default() += 1;
            }
            continue;
        }
        let v = next.fetch_add(1, Ordering::SeqCst);
        let t0 = Instant::now();
        let r = tokio::time::timeout(
            Duration::from_secs(20),
            c.exec_drop(
                "INSERT INTO la (k, v) VALUES (?, ?) ON DUPLICATE KEY UPDATE v = CONCAT(v, ',', VALUES(v))",
                (k, v.to_string()),
            ),
        )
        .await;
        let took = t0.elapsed();
        let mut t = tally.lock().expect("lock");
        match r {
            Ok(Ok(())) => {
                t.acked.entry(k).or_default().push(v);
                t.ack_ms.push((now_ms(), took.as_millis() as u64));
            }
            Ok(Err(e)) => {
                *t.errors.entry(code(&e)).or_default() += 1;
                t.err_ms.push((now_ms(), code(&e)));
                t.unknown += 1;
            }
            Err(_) => t.unknown += 1,
        }
    }
}

/// Starts `readers` and `writers` paced clients of `tenant` through `dc`'s
/// gateway, each at one statement per `every`.
fn workload(
    dc: &str,
    tenant: &str,
    readers: u64,
    writers: u64,
    every: Duration,
    stop: &Arc<AtomicBool>,
    next: &Arc<AtomicU64>,
    tally: &Arc<Mutex<Tally>>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let pl = pool(dc, tenant);
    let mut out = Vec::new();
    for i in 0..readers + writers {
        out.push(tokio::spawn(paced(
            i + 100 * u64::from(dc.as_bytes()[0]),
            pl.clone(),
            i >= readers,
            every,
            Arc::clone(stop),
            Arc::clone(next),
            Arc::clone(tally),
        )));
    }
    out
}

/// Every acknowledged append of `tally` is in `tenant`'s `la` on `url`;
/// returns (acknowledged, lost).
async fn check_appends(url: &str, tenant: u64, tally: &Tally) -> (usize, Vec<(u64, u64)>) {
    let schema = scalar(
        url,
        &format!(
            "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME='la' AND TABLE_SCHEMA LIKE 't{tenant}\\_%'"
        ),
    )
    .await
    .expect("the tenant's copy");
    let have = final_lists(url, &schema).await;
    let acked: usize = tally.acked.values().map(Vec::len).sum();
    let mut lost = Vec::new();
    for (k, vs) in &tally.acked {
        for v in vs {
            if !have.get(k).is_some_and(|s| s.contains(v)) {
                lost.push((*k, *v));
            }
        }
    }
    (acked, lost)
}

/// The longest gap between acknowledged writes, and the longest write,
/// both ms, in `[from, to]`.
fn gaps(t: &Tally, from: u64, to: u64) -> (u64, u64) {
    let mut at: Vec<(u64, u64)> = t
        .ack_ms
        .iter()
        .copied()
        .filter(|(a, _)| *a >= from && *a <= to)
        .collect();
    at.sort_unstable();
    let gap = at.windows(2).map(|w| w[1].0 - w[0].0).max().unwrap_or(0);
    let longest = at.iter().map(|(_, d)| *d).max().unwrap_or(0);
    (gap, longest)
}

async fn user_and_schema(sprigctl: &Path, dc: &str, t: &str) {
    ctl_retry(
        sprigctl,
        dc,
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
        sprigctl,
        dc,
        &[
            "settings",
            "set",
            "auth.mysql_native_password",
            "true",
            "--scope",
            &format!("tenant:{t}"),
            "--reason",
            "r2harden",
            "--yes",
        ],
    );
    let url = format!("mysql://{t}.app:{APP_PW}@127.0.0.1:{}", ports(dc).mysql);
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
            c.query_drop("INSERT IGNORE INTO la.la (k, v) VALUES (100, '0')")
                .await?;
            c.disconnect().await
        };
        match r.await {
            Ok(()) => break,
            Err(e) if t0.elapsed() < Duration::from_secs(90) => {
                eprintln!("schema for {t}: {e}; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => panic!("schema for {t}: {e}"),
        }
    }
}

// ============================================================================
// r2harden: metrics, binlog order, stored programs
// ============================================================================

/// `GET path` on `port`, the body.
async fn http_get(port: u16, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let Ok(mut s) = tokio::net::TcpStream::connect(("127.0.0.1", port)).await else {
        return String::new();
    };
    let _ = s
        .write_all(
            format!("GET {path} HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n").as_bytes(),
        )
        .await;
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut buf)).await;
    String::from_utf8_lossy(&buf).into_owned()
}

/// The sum of every sample of `name` (any labels) on a process's
/// telemetry port.
async fn metric(port: u16, name: &str) -> f64 {
    http_get(port, "/metrics")
        .await
        .lines()
        .filter(|l| !l.starts_with('#'))
        .filter(|l| l.split(['{', ' ']).next() == Some(name))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

/// Every sample line of the metrics named `prefix*`.
async fn metric_lines(port: u16, prefix: &str) -> Vec<String> {
    http_get(port, "/metrics")
        .await
        .lines()
        .filter(|l| l.starts_with(prefix))
        .map(str::to_owned)
        .collect()
}

/// RL13's proof on the promoted copy: in its binlog, where the last
/// transaction the standby job applied to `schema`.la is (the job writes
/// its position row, `__sprig_mover_pos`, in each) and where the first
/// transaction writing it without that row is (a client's, through a
/// gateway). Returns (last job transaction, first client transaction) as
/// (binlog file, position of the transaction's GTID event).
async fn binlog_order(url: &str, schema: &str) -> (Option<(String, u64)>, Option<(String, u64)>) {
    let mut c = mysql_async::Conn::new(opts(url)).await.expect("root");
    let logs: Vec<mysql_async::Row> = c.query("SHOW BINARY LOGS").await.expect("logs");
    let la = format!("({schema}.la)");
    let mut last_job = None;
    let mut first_client = None;
    for l in logs {
        let file: String = l.get(0).expect("file");
        let events: Vec<mysql_async::Row> = c
            .query(format!("SHOW BINLOG EVENTS IN '{file}'"))
            .await
            .expect("events");
        let (mut at, mut job, mut touches) = (0u64, false, false);
        for e in events {
            let pos: u64 = e.get(1).unwrap_or(0);
            let kind: String = e.get(2).unwrap_or_default();
            let info: String = e.get::<Option<String>, _>(5).flatten().unwrap_or_default();
            match kind.as_str() {
                "Gtid" => (at, job, touches) = (pos, false, false),
                "Table_map" => {
                    job |= info.contains("__sprig_mover_pos");
                    touches |= info.contains(&la);
                }
                "Xid" if touches => {
                    if job {
                        last_job = Some((file.clone(), at));
                    } else if first_client.is_none() {
                        first_client = Some((file.clone(), at));
                    }
                }
                _ => {}
            }
        }
    }
    let _ = c.disconnect().await;
    (last_job, first_client)
}

/// Count of rows of `sql` on `url`.
async fn count(url: &str, sql: &str) -> u64 {
    scalar(url, sql)
        .await
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// Waits until `tenant` is homed in `to` and active; returns how long.
async fn wait_home(store: &dyn MetaStore, id: u64, to: &str, within: Duration) -> Duration {
    let t0 = Instant::now();
    loop {
        let e = entry(store, id).await;
        if e["home_dc"] == to && e["state"] == "active" {
            return t0.elapsed();
        }
        assert!(t0.elapsed() < within, "tenant {id} never reached {to}: {e}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Waits for mover job `id` (in `dc`'s tier) to commit.
async fn wait_job(store: &dyn MetaStore, dc: &str, id: &str, within: Duration) -> Value {
    let t0 = Instant::now();
    loop {
        let j = job_state(store, dc, id).await;
        let st = j["state"].as_str().unwrap_or_default().to_owned();
        if ["post_cutover", "retaining", "cleaning", "done"].contains(&st.as_str()) {
            return j;
        }
        assert!(
            !["rolling_back", "rolled_back", "failed", "cancelled"].contains(&st.as_str()),
            "job {id}: {j}"
        );
        assert!(t0.elapsed() < within, "job {id} stuck: {j}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// `(group, [(shard, cluster)])` of `tenant`'s first table group.
fn shards_of(sprigctl: &Path, dc: &str, tenant: &str) -> Option<(u64, Vec<u64>)> {
    let l = ctl(sprigctl, dc, &["shard", "list", tenant]).ok()?;
    let groups = l.as_array()?;
    // The group `cust` roots (the advisor names groups after their root).
    let g = groups
        .iter()
        .find(|g| g["name"].as_str().is_some_and(|n| n.contains("cust")))
        .or_else(|| groups.iter().find(|g| g["group"].as_u64() != Some(0)))?;
    Some((
        g["group"].as_u64()?,
        g["shards"]
            .as_array()?
            .iter()
            .filter_map(|s| s["shard"].as_u64())
            .collect(),
    ))
}

/// One paced client of the sharded tenant: appends to `cust` by id
/// (prepared, through the router) or reads one.
async fn paced_g(
    p: u64,
    pool: Pool,
    write: bool,
    every: Duration,
    stop: Arc<AtomicBool>,
    next: Arc<AtomicU64>,
    tally: Arc<Mutex<Tally>>,
) {
    let mut seed = p * 7919 + 31;
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    while !stop.load(Ordering::SeqCst) {
        tick.tick().await;
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let k = seed % 64;
        let Ok(Ok(mut c)) = tokio::time::timeout(Duration::from_secs(3), pool.get_conn()).await
        else {
            continue;
        };
        if !write {
            let r = tokio::time::timeout(
                Duration::from_secs(10),
                c.exec_drop("SELECT v FROM cust WHERE id = ?", (k,)),
            )
            .await;
            if let Ok(Err(e)) = r {
                *tally
                    .lock()
                    .expect("lock")
                    .errors
                    .entry(format!("read {}", code(&e)))
                    .or_default() += 1;
            }
            continue;
        }
        let v = next.fetch_add(1, Ordering::SeqCst);
        let r = tokio::time::timeout(
            Duration::from_secs(20),
            c.exec_drop(
                "INSERT INTO cust (id, v) VALUES (?, ?) ON DUPLICATE KEY UPDATE v = CONCAT(v, ',', VALUES(v))",
                (k, v.to_string()),
            ),
        )
        .await;
        let mut t = tally.lock().expect("lock");
        match r {
            Ok(Ok(())) => {
                t.acked.entry(k).or_default().push(v);
                t.ack_ms.push((now_ms(), 0));
            }
            Ok(Err(e)) => {
                *t.errors.entry(code(&e)).or_default() += 1;
                t.err_ms.push((now_ms(), code(&e)));
                t.unknown += 1;
            }
            Err(_) => t.unknown += 1,
        }
    }
}

/// Errors that must never reach a client (RL13, RL14).
fn forbidden(t: &Tally) -> Vec<(String, u64)> {
    t.errors
        .iter()
        .filter(|(k, _)| {
            ["1142", "1146", "1049"]
                .iter()
                .any(|c| k.as_str() == *c || k.as_str() == format!("read {c}"))
        })
        .map(|(k, v)| (k.clone(), *v))
        .collect()
}

static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn bins_r2harden_holds_fences_and_stored_programs() {
    if !skip::flag(skip::DOCKER) {
        skip::report_missing(
            skip::DOCKER,
            "bins_r2harden_holds_fences_and_stored_programs",
        );
        return;
    }
    let _one = ONE_AT_A_TIME.lock().await;
    let Some((sprigdb, sprigctl)) = bins() else {
        eprintln!("build sprigdb and sprigctl first (cargo build -p sprigdb -p sprigctl)");
        return;
    };
    let Some(base) = tenant_base() else {
        eprintln!("set R2HARDEN_TENANT_BASE (rule 8f) first");
        return;
    };
    let work = std::env::temp_dir().join(format!("r2harden-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).expect("work dir");
    let t_all = Instant::now();

    // ---- etcd ----
    let ep = free_port();
    let etcd_pub = format!("127.0.0.1:{ep}:2379");
    let etcd = OwnContainer::run(
        "r2harden-etcd",
        &[
            "--memory",
            "256m",
            "-p",
            &etcd_pub,
            "quay.io/coreos/etcd:v3.6.5",
            "etcd",
            "--listen-client-urls=http://0.0.0.0:2379",
            "--advertise-client-urls=http://r2harden-etcd.invalid:2379",
        ],
    )
    .expect("etcd");
    let seed = format!("http://127.0.0.1:{ep}");
    let mut cfg = EtcdConfig::new(vec![seed.clone()]);
    cfg.request_timeout = Duration::from_secs(2);
    let store: Arc<dyn MetaStore> = loop {
        match EtcdStore::connect(Tier::Global, &cfg).await {
            Ok(s) => break Arc::new(s),
            Err(_) => tokio::time::sleep(Duration::from_millis(300)).await,
        }
    };

    // ---- MariaDB: one primary per DC ----
    let net = Net(format!("sprig-sim-r2harden-{}", std::process::id()));
    docker(&["network", "create", &net.0]).expect("network");
    let (fra1_db, fra1_port) = mariadb(&net.0, "fra1db", 101, 1, None);
    let (ams1_db, ams1_port) = mariadb(&net.0, "ams1db", 201, 2, None);
    let (sin1_db, sin1_port) = mariadb(&net.0, "sin1db", 301, 3, None);
    let root_url = |p: u16| format!("mysql://root:{ROOT_PW}@127.0.0.1:{p}");
    let db_port: BTreeMap<&str, u16> = [
        ("fra1", fra1_port),
        ("ams1", ams1_port),
        ("sin1", sin1_port),
    ]
    .into_iter()
    .collect();
    for p in db_port.values() {
        wait_for(&root_url(*p), "SELECT 1", "1", Duration::from_secs(180)).await;
    }
    eprintln!("MariaDB up: {db_port:?} ({:?})", t_all.elapsed());
    for d in DCS {
        delay_proxy(
            ports(d).internal_proxy,
            ports(d).internal,
            Duration::from_millis(delay_ms(d) / 2),
        )
        .await;
    }

    // ---- deployment settings ----
    let certs = sprig_testkit::compose::compose_dir().join("gateway-certs");
    let global_settings: Vec<(&str, Value)> = vec![
        // No automatic moves: this test moves tenants itself.
        ("migration.mode", json!("recommend")),
        ("recovery.stability_minutes", json!(0)),
        ("balancer.interval_seconds", json!(10)),
        ("control.latency_probe_seconds", json!(2)),
        ("mover.ready_sustain_seconds", json!(3)),
        ("mover.source_retain_hours", json!(0)),
        ("sharding.check_interval_seconds", json!(5)),
    ];
    for (k, v) in &global_settings {
        put(
            &*store,
            &format!("/sprig/g/settings/{k}"),
            json!({"v": 1, "value": v, "set_by": "op:r2harden", "reason": "live_r2harden_bins"}),
        )
        .await;
    }
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
            ("failover.standby_dcs", json!(0)),
        ] {
            put(
                &*store,
                &format!("/sprig/dc/{d}/settings/{k}"),
                json!({"v": 1, "value": v, "set_by": "op:r2harden", "reason": "live_r2harden_bins"}),
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
         [backend_secret]\npassword = \"r2harden-per-user-secret\"\n",
    )
    .expect("backends");
    std::fs::write(
        work.join("extract.keys"),
        "# r2harden test key\n1 7f1c0d2e3a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5\n",
    )
    .expect("keys");

    // ---- controllers ----
    let mut procs = Procs {
        bin: sprigdb.clone(),
        work: work.clone(),
        kids: Vec::new(),
    };
    procs.start(
        "ctl-fra1",
        "controller",
        &controller_toml("fra1", &seed, &work),
    );
    assert!(
        ready(ports("fra1").ctl_tel, Duration::from_secs(120)).await,
        "ctl-fra1 ready"
    );
    let next_id = get(&*store, "/sprig/g/ids/tenant")
        .await
        .unwrap_or_default();
    if next_id["next"].as_u64().unwrap_or(0) < base {
        put(
            &*store,
            "/sprig/g/ids/tenant",
            json!({"v": 1, "next": base}),
        )
        .await;
    }
    let mut fra1 = get(&*store, "/sprig/g/dcs/fra1").await.expect("dcs/fra1");
    fra1["global_voter"] = json!(true);
    fra1["tags"] = json!({"jurisdiction": "EU"});
    put(&*store, "/sprig/g/dcs/fra1", fra1).await;
    for (d, j) in [("ams1", "EU"), ("sin1", "SG")] {
        ctl_retry(
            &sprigctl,
            "fra1",
            &[
                "dc",
                "register",
                d,
                "--global-voter",
                "--tags",
                &format!("jurisdiction={j}"),
            ],
            Duration::from_secs(30),
        )
        .await;
    }
    for d in ["ams1", "sin1"] {
        procs.start(
            &format!("ctl-{d}"),
            "controller",
            &controller_toml(d, &seed, &work),
        );
    }
    for d in ["ams1", "sin1"] {
        assert!(
            ready(ports(d).ctl_tel, Duration::from_secs(120)).await,
            "ctl-{d} ready"
        );
    }
    let t0 = Instant::now();
    loop {
        let mut all = true;
        for d in DCS {
            let v = get(&*store, &format!("/sprig/g/dcs/{d}"))
                .await
                .unwrap_or_default();
            all &= v["state"] == "active";
        }
        if all {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(180),
            "DCs never activated"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // ---- clusters, agents, gateways, workers ----
    let cred = format!("file:{}", creds.display());
    let mut cluster = BTreeMap::new();
    for d in DCS {
        let v = ctl_retry(
            &sprigctl,
            d,
            &[
                "cluster",
                "register",
                &format!("{d}-db"),
                "--topology",
                "single",
                "--node",
                &format!("127.0.0.1:{}", db_port[d]),
                "--credentials",
                &cred,
                "--storage-gb",
                "1",
            ],
            Duration::from_secs(30),
        )
        .await;
        let c = v["cluster"]["cluster_id"]
            .as_str()
            .expect("cluster id")
            .to_owned();
        let t0 = Instant::now();
        loop {
            let v = ctl(&sprigctl, d, &["cluster", "list"]).unwrap_or_default();
            let st = v["clusters"]
                .as_array()
                .and_then(|a| a.iter().find(|x| x["cluster_id"] == c.as_str()))
                .map(|x| x["admin_state"].as_str().unwrap_or_default().to_owned());
            if st.as_deref() == Some("active") {
                break;
            }
            assert!(t0.elapsed() < Duration::from_secs(120), "{c} never active");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        procs.start(&format!("agent-{d}"), "agent", &agent_toml(d, &seed, &c));
        assert!(
            ready(ports(d).agent_tel, Duration::from_secs(60)).await,
            "agent-{d}"
        );
        cluster.insert(d, c);
    }
    for p in db_port.values() {
        wait_for(
            &root_url(*p),
            "SELECT @@read_only",
            "0",
            Duration::from_secs(60),
        )
        .await;
    }
    for d in DCS {
        procs.start(
            &format!("gw-{d}"),
            "gateway",
            &gateway_toml(d, &seed, &backends),
        );
        procs.start(
            &format!("worker-{d}"),
            "worker",
            &worker_toml(d, &seed, &work),
        );
    }
    for d in DCS {
        assert!(
            ready(ports(d).gw_tel, Duration::from_secs(180)).await,
            "gw-{d}"
        );
        assert!(
            ready(ports(d).worker_tel, Duration::from_secs(180)).await,
            "worker-{d}"
        );
    }
    eprintln!(
        "clusters {cluster:?}; all processes ready ({:?})",
        t_all.elapsed()
    );

    // ---- tenants ----
    let mut ids: BTreeMap<String, u64> = BTreeMap::new();
    for t in ["r2h-s", "r2h-t", "r2h-g"] {
        let v = ctl_retry(
            &sprigctl,
            "fra1",
            &["tenant", "create", t, "--cluster", &cluster["fra1"]],
            Duration::from_secs(60),
        )
        .await;
        let id = v["tenant"]["tenant_id"].as_u64().expect("tenant id");
        assert!(
            (base..base + 100_000).contains(&id),
            "rule 8f: {id} outside this deployment's range"
        );
        ids.insert(t.to_owned(), id);
        user_and_schema(&sprigctl, "fra1", t).await;
    }
    eprintln!("tenants {ids:?} ({:?})", t_all.elapsed());
    let (s_id, t_id, g_id) = (ids["r2h-s"], ids["r2h-t"], ids["r2h-g"]);
    for t in ["r2h-t", "r2h-g"] {
        tenant_setting(&sprigctl, "fra1", t, "failover.standby_dcs", "0").await;
    }

    // =====================================================================
    // 1. RL12: S's stored programs, then its standby in ams1
    // =====================================================================
    let s_schema = scalar(
        &root_url(fra1_port),
        &format!("SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME='la' AND TABLE_SCHEMA LIKE 't{s_id}\\_%'"),
    )
    .await
    .expect("S's schema");
    let run_root = |p: u16, qs: Vec<String>| {
        let url = root_url(p);
        async move {
            let mut c = mysql_async::Conn::new(opts(&url)).await.expect("root");
            for q in qs {
                c.query_drop(&q)
                    .await
                    .unwrap_or_else(|e| panic!("{q}: {e}"));
            }
            let _ = c.disconnect().await;
        }
    };
    run_root(
        fra1_port,
        vec![
            format!("USE `{s_schema}`"),
            "CREATE TABLE tr (id INT PRIMARY KEY, tag VARCHAR(20) NOT NULL DEFAULT '')".into(),
            "CREATE TRIGGER tr_bi BEFORE INSERT ON tr FOR EACH ROW SET NEW.tag = 'trig1'".into(),
            "CREATE VIEW v_la AS SELECT k FROM la".into(),
            "CREATE PROCEDURE p_one() SELECT 1".into(),
            "CREATE EVENT ev_one ON SCHEDULE EVERY 1 HOUR DO DELETE FROM tr WHERE id < 0".into(),
        ],
    )
    .await;
    let raised = Instant::now();
    setting(&sprigctl, "fra1", "failover.standby_dcs", "1").await;
    let streaming = |e: &Value, dc: &str| e["standby_jobs"][dc]["job"].as_str().map(str::to_owned);
    let s_job = loop {
        let e = entry(&*store, s_id).await;
        if let Some(j) = streaming(&e, "ams1")
            && job_state(&*store, "ams1", &j).await["state"] == "streaming"
        {
            break j;
        }
        assert!(
            raised.elapsed() < Duration::from_secs(300),
            "S got no standby: {e}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    eprintln!(
        "1. S's standby streaming in ams1 {:?} after failover.standby_dcs = 1",
        raised.elapsed()
    );
    // A second trigger and a row while the copy streams: the copy gets the
    // row (with the tag the home's trigger set) and no trigger of its own.
    run_root(
        fra1_port,
        vec![
            format!("USE `{s_schema}`"),
            "CREATE TRIGGER tr_bu BEFORE UPDATE ON tr FOR EACH ROW SET NEW.tag = 'trig2'".into(),
            "INSERT INTO tr (id) VALUES (1)".into(),
        ],
    )
    .await;
    let ams1_url = root_url(ams1_port);
    wait_for(
        &ams1_url,
        &format!("SELECT tag FROM `{s_schema}`.tr WHERE id = 1"),
        "trig1",
        Duration::from_secs(60),
    )
    .await;
    let copy_triggers = count(
        &ams1_url,
        &format!(
            "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = '{s_schema}'"
        ),
    )
    .await;
    let copy_event = scalar(
        &ams1_url,
        &format!("SELECT STATUS FROM information_schema.EVENTS WHERE EVENT_SCHEMA = '{s_schema}' AND EVENT_NAME = 'ev_one'"),
    )
    .await;
    eprintln!(
        "1. while streaming, the copy has {copy_triggers} triggers and ev_one {copy_event:?}"
    );
    assert_eq!(
        copy_triggers, 0,
        "no trigger on a copy that rows are applied to"
    );
    assert_ne!(
        copy_event.as_deref(),
        Some("ENABLED"),
        "the copy's event stays disabled"
    );

    // =====================================================================
    // 2. RL14 and RL3: moves of T and splits of G under load
    // =====================================================================
    // G: two shards from the start, by its own settings (RL3: the sharding
    // loop reads tenant-scope sharding.* and advisor.*).
    tenant_setting(&sprigctl, "fra1", "r2h-g", "advisor.mode", "off").await;
    // An empty table has no distinct values to judge a key by.
    tenant_setting(&sprigctl, "fra1", "r2h-g", "advisor.min_ndv_per_shard", "0").await;
    {
        let pl = pool("fra1", "r2h-g");
        let mut c = pl.get_conn().await.expect("g conn");
        for q in [
            "CREATE TABLE IF NOT EXISTS cust (id BIGINT PRIMARY KEY, v TEXT NOT NULL) ENGINE=InnoDB",
            "CREATE TABLE IF NOT EXISTS ord (id BIGINT PRIMARY KEY, cust_id BIGINT NOT NULL, v TEXT, FOREIGN KEY (cust_id) REFERENCES cust(id)) ENGINE=InnoDB",
        ] {
            c.query_drop(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        }
        drop(c);
        pl.disconnect().await.ok();
    }
    let set_at = Instant::now();
    tenant_setting(&sprigctl, "fra1", "r2h-g", "sharding.min_shards", "2").await;
    let (gid, first) = loop {
        if let Some((g, s)) = shards_of(&sprigctl, "fra1", "r2h-g")
            && s.len() >= 2
        {
            break (g, s);
        }
        assert!(
            set_at.elapsed() < Duration::from_secs(300),
            "RL3: G's tenant-scope sharding.min_shards = 2 never split it: {:?}",
            ctl(&sprigctl, "fra1", &["shard", "list", "r2h-g"])
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    };
    eprintln!(
        "2. RL3: G's tenant-scope sharding.min_shards = 2, advisor.mode = off and advisor.min_ndv_per_shard = 0: group {gid} has shards {first:?} {:?} after the set",
        set_at.elapsed()
    );
    let stop = Arc::new(AtomicBool::new(false));
    let next = Arc::new(AtomicU64::new(1));
    let t_tally = Arc::new(Mutex::new(Tally::default()));
    let g_tally = Arc::new(Mutex::new(Tally::default()));
    let mut clients = Vec::new();
    for d in DCS {
        clients.extend(workload(
            d,
            "r2h-t",
            2,
            2,
            Duration::from_millis(25),
            &stop,
            &next,
            &t_tally,
        ));
        let pl = pool(d, "r2h-g");
        for i in 0..4u64 {
            clients.push(tokio::spawn(paced_g(
                i + 100 * u64::from(d.as_bytes()[0]),
                pl.clone(),
                i >= 2,
                Duration::from_millis(25),
                Arc::clone(&stop),
                Arc::clone(&next),
                Arc::clone(&g_tally),
            )));
        }
    }
    tokio::time::sleep(Duration::from_secs(10)).await;
    let mut steps: Vec<(String, Duration)> = Vec::new();
    // Three moves of T between DCs and two splits of G, interleaved.
    let mut home = "fra1".to_owned();
    for (i, to) in ["ams1", "sin1", "ams1"].iter().enumerate() {
        let t0 = Instant::now();
        ctl_retry(
            &sprigctl,
            &home,
            &["tenant", "move", "r2h-t", "--to", to],
            Duration::from_secs(60),
        )
        .await;
        let took = wait_home(&*store, t_id, to, Duration::from_secs(600)).await;
        steps.push((format!("move T {home} -> {to}"), t0.elapsed().max(took)));
        home = (*to).to_owned();
        if i < 2 {
            let (_, shards) = shards_of(&sprigctl, "fra1", "r2h-g").expect("shards");
            let s = shards.last().copied().expect("a shard").to_string();
            let t0 = Instant::now();
            let j = ctl_retry(
                &sprigctl,
                "fra1",
                &["shard", "split", "r2h-g", &gid.to_string(), &s],
                Duration::from_secs(60),
            )
            .await;
            let id = j["id"].as_str().expect("job id").to_owned();
            wait_job(&*store, "fra1", &id, Duration::from_secs(600)).await;
            steps.push((format!("split G shard {s}"), t0.elapsed()));
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    stop.store(true, Ordering::SeqCst);
    for c in clients.drain(..) {
        let _ = c.await;
    }
    let tt = std::mem::take(&mut *t_tally.lock().expect("lock"));
    let gt = std::mem::take(&mut *g_tally.lock().expect("lock"));
    let (t_acked, t_lost) = check_appends(&root_url(ams1_port), t_id, &tt).await;
    let fence_lines: Vec<String> = {
        let mut v = Vec::new();
        for d in DCS {
            for l in metric_lines(ports(d).gw_tel, "sprig_gateway_fence_errors_total").await {
                v.push(format!("{d}: {l}"));
            }
        }
        v
    };
    eprintln!(
        "2. steps {steps:?}; shards now {:?}",
        shards_of(&sprigctl, "fra1", "r2h-g")
    );
    eprintln!(
        "2. T: {t_acked} acknowledged appends, {} lost, {} unknown; errors {:?}",
        t_lost.len(),
        tt.unknown,
        tt.errors
    );
    eprintln!(
        "2. G: {} acknowledged appends, {} unknown; errors {:?}",
        gt.acked.values().map(Vec::len).sum::<usize>(),
        gt.unknown,
        gt.errors
    );
    eprintln!("2. gateways' fence handling: {fence_lines:?}");
    assert!(
        t_lost.is_empty(),
        "T: acknowledged appends lost: {t_lost:?}"
    );
    assert!(
        forbidden(&tt).is_empty(),
        "RL14: T's clients saw {:?}",
        forbidden(&tt)
    );
    assert!(
        forbidden(&gt).is_empty(),
        "RL14: G's clients saw {:?}",
        forbidden(&gt)
    );
    // G's acknowledged appends: each on the shard schema its row is in.
    {
        let mut c = mysql_async::Conn::new(opts(&root_url(fra1_port)))
            .await
            .expect("root");
        let schemas: Vec<String> = c
            .query(format!(
                "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME = 'cust' AND TABLE_SCHEMA LIKE 't{g_id}\\_%'"
            ))
            .await
            .expect("g schemas");
        let mut have: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();
        for s in &schemas {
            let rows: Vec<(u64, String)> = c
                .query(format!("SELECT id, v FROM `{s}`.cust"))
                .await
                .expect("g rows");
            for (k, v) in rows {
                have.entry(k)
                    .or_default()
                    .extend(v.split(',').filter_map(|x| x.parse::<u64>().ok()));
            }
        }
        let _ = c.disconnect().await;
        let lost: Vec<(u64, u64)> = gt
            .acked
            .iter()
            .flat_map(|(k, vs)| vs.iter().map(move |v| (*k, *v)))
            .filter(|(k, v)| !have.get(k).is_some_and(|s| s.contains(v)))
            .collect();
        eprintln!(
            "2. G's rows on {} shard schemas; {} acknowledged appends lost",
            schemas.len(),
            lost.len()
        );
        assert!(lost.is_empty(), "G: acknowledged appends lost: {lost:?}");
    }

    // =====================================================================
    // 3. RL13 and RL12: fra1 frozen, S promoted to ams1
    // =====================================================================
    let recorded = count(
        &ams1_url,
        &format!(
            "SELECT COUNT(*) FROM __sprig_sys.__sprig_standby_objects WHERE tenant_id = {s_id}"
        ),
    )
    .await;
    eprintln!("3. RL12: {recorded} statements recorded on the copy for S's promotion");
    let s_stop = Arc::new(AtomicBool::new(false));
    let s_tally = Arc::new(Mutex::new(Tally::default()));
    let mut s_clients = workload(
        "fra1",
        "r2h-s",
        0,
        3,
        Duration::from_millis(20),
        &s_stop,
        &next,
        &s_tally,
    );
    s_clients.extend(workload(
        "ams1",
        "r2h-s",
        2,
        3,
        Duration::from_millis(20),
        &s_stop,
        &next,
        &s_tally,
    ));
    s_clients.extend(workload(
        "sin1",
        "r2h-s",
        1,
        2,
        Duration::from_millis(20),
        &s_stop,
        &next,
        &s_tally,
    ));
    tokio::time::sleep(Duration::from_secs(15)).await;
    let holds_before = metric(ports("ams1").gw_tel, "sprig_gateway_promotion_holds_total").await
        + metric(ports("sin1").gw_tel, "sprig_gateway_promotion_holds_total").await;
    let frozen = Instant::now();
    let frozen_ms = now_ms();
    eprintln!("--- fra1's controller, gateway, agent and worker frozen (SIGSTOP)");
    procs.signal("fra1", "-STOP");
    let mut committed_after = None;
    let mut promoted_after = None;
    while frozen.elapsed() < Duration::from_secs(60) && promoted_after.is_none() {
        let e = entry(&*store, s_id).await;
        if committed_after.is_none() && e["home_dc"] == "ams1" {
            committed_after = Some(frozen.elapsed());
        }
        if e["home_dc"] == "ams1" && e["state"] == "active" {
            promoted_after = Some(frozen.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let promoted_after = promoted_after.expect("S must be promoted");
    eprintln!(
        "3. S: failover committed {committed_after:?}, active {promoted_after:?} after the freeze"
    );
    tokio::time::sleep(Duration::from_secs(10)).await;
    procs.signal("fra1", "-CONT");
    tokio::time::sleep(Duration::from_secs(10)).await;
    s_stop.store(true, Ordering::SeqCst);
    for c in s_clients.drain(..) {
        let _ = c.await;
    }
    assert_eq!(
        job_state(&*store, "ams1", &s_job).await["state"],
        "done",
        "S's standby job stopped"
    );
    let st = std::mem::take(&mut *s_tally.lock().expect("lock"));
    let (acked, lost) = check_appends(&ams1_url, s_id, &st).await;
    let mut holds = 0.0;
    let mut hold_lines = Vec::new();
    for d in ["ams1", "sin1"] {
        holds += metric(ports(d).gw_tel, "sprig_gateway_promotion_holds_total").await;
        hold_lines.extend(metric_lines(ports(d).gw_tel, "sprig_gateway_promotion_hold").await);
    }
    // The longest acknowledged write around the promotion, and the gap.
    let window = (
        frozen_ms,
        frozen_ms + promoted_after.as_millis() as u64 + 10_000,
    );
    let (gap, longest) = gaps(&st, window.0, window.1);
    let (last_job, first_client) = binlog_order(&ams1_url, &s_schema).await;
    eprintln!(
        "3. RL13: {acked} acknowledged appends, {} lost, {} unknown; errors {:?}; statements held by the promotion {} ({hold_lines:?}); longest gap between acknowledged writes {gap} ms, longest write {longest} ms",
        lost.len(),
        st.unknown,
        st.errors,
        holds - holds_before
    );
    eprintln!(
        "3. RL13: on ams1's binlog, the job's last write at {last_job:?}, the first client write at {first_client:?}"
    );
    assert!(lost.is_empty(), "acknowledged appends lost: {lost:?}");
    assert!(
        forbidden(&st).is_empty(),
        "RL13: S's clients saw {:?}",
        forbidden(&st)
    );
    assert!(
        holds - holds_before > 0.0,
        "RL13: the promotion held statements"
    );
    if let (Some(j), Some(c)) = (&last_job, &first_client) {
        assert!(
            c > j,
            "RL13: a client write reached the copy before the job's last write"
        );
    } else {
        panic!("binlog order not found: {last_job:?} {first_client:?}");
    }
    // RL12: the promoted copy carries S's triggers, enabled event, view
    // and routine; a trigger fires on a write through ams1's gateway.
    let trig = count(
        &ams1_url,
        &format!(
            "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = '{s_schema}'"
        ),
    )
    .await;
    let ev = scalar(
        &ams1_url,
        &format!("SELECT STATUS FROM information_schema.EVENTS WHERE EVENT_SCHEMA = '{s_schema}' AND EVENT_NAME = 'ev_one'"),
    )
    .await;
    let view = count(
        &ams1_url,
        &format!("SELECT COUNT(*) FROM information_schema.VIEWS WHERE TABLE_SCHEMA = '{s_schema}' AND TABLE_NAME = 'v_la'"),
    )
    .await;
    let proc_n = count(
        &ams1_url,
        &format!("SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = '{s_schema}' AND ROUTINE_NAME = 'p_one'"),
    )
    .await;
    run_root(
        ams1_port,
        vec![
            format!("USE `{s_schema}`"),
            "INSERT INTO tr (id) VALUES (2)".into(),
            "UPDATE tr SET id = 3 WHERE id = 2".into(),
        ],
    )
    .await;
    let tag = scalar(
        &ams1_url,
        &format!("SELECT tag FROM `{s_schema}`.tr WHERE id = 3"),
    )
    .await;
    eprintln!(
        "3. RL12: on the promoted copy: {trig} triggers, ev_one {ev:?}, view {view}, procedure {proc_n}; a new row's tag after insert and update: {tag:?}"
    );
    assert_eq!(trig, 2, "both triggers on the promoted copy");
    assert_eq!(
        ev.as_deref(),
        Some("ENABLED"),
        "the event enabled as at home"
    );
    assert_eq!((view, proc_n), (1, 1), "view and procedure");
    assert_eq!(
        tag.as_deref(),
        Some("trig2"),
        "the triggers fire on the new home"
    );
    eprintln!("summary: all parts passed in {:?}", t_all.elapsed());

    drop(procs);
    drop([fra1_db, ams1_db, sin1_db]);
    drop(net);
    drop(etcd);
    if std::env::var_os("R2HARDEN_KEEP_WORK").is_some() {
        eprintln!("logs kept in {}", work.display());
    } else {
        let _ = std::fs::remove_dir_all(&work);
    }
}
