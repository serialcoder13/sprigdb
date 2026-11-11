//! R2's last round with the real binaries (r2last): read-only standby
//! copies (decision RF10), tenant-scope settings in controllers and workers
//! (RF3), and tenants that follow their traffic between three DCs with
//! least-busy placement of new tenants (chapter 24, build steps 7 and 8).
//!
//! ```sh
//! cargo build -p sprigdb -p sprigctl
//! SPRIG_TEST_DOCKER=1 cargo test -p sprig-controller --test live_r2last_bins -- --nocapture --test-threads=1
//! ```
//!
//! Three DCs on one private etcd (single-etcd mode), all global voters:
//! `fra1`, `ams1` (both tagged `jurisdiction=EU`) and `sin1` (`SG`), each
//! with `sprigdb controller`, `gateway`, `worker` and `agent` and one
//! `mariadb:11.8` primary (768 MB, 1 GB declared storage). Each
//! controller's internal service sits behind a delay proxy, so the
//! measured latency matrix is fra1-ams1 30 ms and 120 ms to sin1. Tenant
//! ids start at 58,840,600,000 (rules 8b and 8f).
//!
//! 1. RF3: with fra1's dc-scope `failover.standby_dcs = 1`, a tenant whose
//!    own value is 0 gets no standby; raising its own value starts one.
//! 2. RF10: a direct write by a tenant account to the standby copy is
//!    refused (1142); reads work.
//! 3. Step 7: `r2last-t` reads from fra1 (150/s) and writes from ams1
//!    (100/s): writes weigh 3, so it moves to ams1 by itself, within its
//!    own tenant-scope write-pause budget (RF3). `r2last-r` (residency EU)
//!    gets its traffic from sin1 and stays, with ResidencyRemoteTraffic.
//! 4. Step 8: a new tenant without a hint lands in the least busy DC
//!    (sin1); a child of the EU tenant lands in the least busy EU DC.
//! 5. T's traffic moves to sin1: the cooldown holds it, then it moves.
//! 6. RF10: fra1 frozen, `r2last-s` promoted to ams1; the promotion gives
//!    the tenant's accounts their writes back; no acknowledged write lost.

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
use sprig_meta::{Consistency, EtcdConfig, EtcdStore, ListOpts, MetaStore, Op, Tier, Txn};
use sprig_testkit::faults::compose::{OwnContainer, docker};
use sprig_testkit::skip;

const ROOT_PW: &str = "sprig-r2last-root";
const REPL_PW: &str = "sprig-r2last-repl";
const APP_PW: &str = "sprig-r2last-app";
const DEPLOYMENT: &str = "sprigdb-r2last";
/// Rules 8b and 8f: port 28406 x 100,000, plus 56 x 10^9 to stay above
/// every id on the dev clusters (53,130,600,001 on 10 Nov, 13:30 IST).
/// Only this test's own containers hold these schemas.
const TENANT_BASE: u64 = 58_840_600_000;
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
        "fra1" => 28406,
        "ams1" => 28426,
        _ => 28446,
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
                "info,sprig_controller::standby=debug,sprig_controller::dc_migrate=debug,sprig_controller::tenant_scopes=debug",
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
        .env("SPRIGCTL_ACTOR", "r2last")
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

async fn list(store: &dyn MetaStore, path: &str) -> Vec<Value> {
    store
        .list(&key(path), ListOpts::default())
        .await
        .map(|p| {
            p.kvs
                .iter()
                .filter_map(|kv| serde_json::from_slice(&kv.value).ok())
                .collect()
        })
        .unwrap_or_default()
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
        OwnContainer::run(&format!("r2last-{name}"), &args).expect("mariadb"),
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
            "settings", "set", k, v, "--scope", "dc", "--dc", dc, "--reason", "r2last", "--yes",
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
            "r2last",
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

fn traffic(sprigctl: &Path, dc: &str, tenant: &str) -> Value {
    ctl(sprigctl, dc, &["tenant", "traffic", tenant]).unwrap_or_default()
}

/// Waits until `tenant` (id `id`) is homed in `to` and active, printing
/// what the automatic decision did on the way (`sprigctl tenant traffic`'s
/// `auto`). Returns (when it started migrating, when it committed), as
/// instants, and the decision records seen.
async fn follow(
    store: &dyn MetaStore,
    sprigctl: &Path,
    tenant: &str,
    id: u64,
    to: &str,
    within: Duration,
) -> (Option<Instant>, Instant, Vec<String>) {
    let t0 = Instant::now();
    let mut started = None;
    let mut seen: Vec<String> = Vec::new();
    loop {
        let e = entry(store, id).await;
        let home = e["home_dc"].as_str().unwrap_or_default().to_owned();
        if started.is_none() && e["state"] == "migrating" {
            started = Some(Instant::now());
            eprintln!(
                "  {tenant} migrating {home} -> {} after {:?} (cause {}, max_pause_ms {})",
                e["migration"]["to"],
                t0.elapsed(),
                e["migration"]["cause"],
                e["migration"]["max_pause_ms"]
            );
        }
        if home == to && e["state"] == "active" {
            return (started, Instant::now(), seen);
        }
        if e["state"] == "active" {
            let v = traffic(sprigctl, &home, tenant);
            let a = &v["auto"];
            let line = format!(
                "{} {}: {}",
                a["action"].as_str().unwrap_or("-"),
                a["evaluation"]["decision"].as_str().unwrap_or("-"),
                a["why"].as_str().unwrap_or("-")
            );
            let key = format!(
                "{} {}",
                a["action"].as_str().unwrap_or("-"),
                a["evaluation"]["decision"].as_str().unwrap_or("-")
            );
            if seen
                .last()
                .is_none_or(|s| s.split(" @").next() != Some(&key))
            {
                eprintln!(
                    "  [{:>5.1}s] {tenant} at {home}: {line}; W {} L {}",
                    t0.elapsed().as_secs_f64(),
                    v["weighted_total"],
                    a["evaluation"]["latency_ms"]
                );
                seen.push(format!("{key} @{:.1}s: {line}", t0.elapsed().as_secs_f64()));
            }
        }
        assert!(t0.elapsed() < within, "{tenant} never moved to {to}: {e}");
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }
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
            "r2last",
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

static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn bins_r2last_readonly_standbys_tenant_scopes_and_traffic_following() {
    if !skip::flag(skip::DOCKER) {
        skip::report_missing(
            skip::DOCKER,
            "bins_r2last_readonly_standbys_tenant_scopes_and_traffic_following",
        );
        return;
    }
    let _one = ONE_AT_A_TIME.lock().await;
    let Some((sprigdb, sprigctl)) = bins() else {
        eprintln!("build sprigdb and sprigctl first (cargo build -p sprigdb -p sprigctl)");
        return;
    };
    let work = std::env::temp_dir().join(format!("r2last-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).expect("work dir");
    let t_all = Instant::now();

    // ---- etcd ----
    let ep = free_port();
    let etcd_pub = format!("127.0.0.1:{ep}:2379");
    let etcd = OwnContainer::run(
        "r2last-etcd",
        &[
            "--memory",
            "256m",
            "-p",
            &etcd_pub,
            "quay.io/coreos/etcd:v3.6.5",
            "etcd",
            "--listen-client-urls=http://0.0.0.0:2379",
            "--advertise-client-urls=http://r2last-etcd.invalid:2379",
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
    let net = Net(format!("sprig-sim-r2last-{}", std::process::id()));
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

    // ---- delay proxies in front of each controller's internal service ----
    for d in DCS {
        delay_proxy(
            ports(d).internal_proxy,
            ports(d).internal,
            Duration::from_millis(delay_ms(d) / 2),
        )
        .await;
    }

    // ---- deployment settings (global, dc) ----
    let certs = sprig_testkit::compose::compose_dir().join("gateway-certs");
    let global_settings: Vec<(&str, Value)> = vec![
        // Chapter 24's decision, shortened for a test.
        ("migration.evaluate_seconds", json!(5)),
        ("migration.window_minutes", json!(2)),
        ("migration.min_signal_requests", json!(300)),
        ("migration.sustain_minutes", json!(0.5)),
        ("migration.cooldown_hours", json!(0.025)),
        ("recovery.stability_minutes", json!(0)),
        (
            "placement.least_busy_weights",
            json!("storage=1,cpu=0,connections=0"),
        ),
        ("balancer.interval_seconds", json!(10)),
        ("control.latency_probe_seconds", json!(2)),
        ("routing.traffic_report_seconds", json!(5)),
        // Moves and standbys, shortened.
        ("mover.ready_sustain_seconds", json!(3)),
        ("mover.source_retain_hours", json!(0)),
    ];
    for (k, v) in &global_settings {
        put(
            &*store,
            &format!("/sprig/g/settings/{k}"),
            json!({"v": 1, "value": v, "set_by": "op:r2last", "reason": "live_r2last_bins"}),
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
            // Standbys only for fra1's tenants, raised in part 1.
            ("failover.standby_dcs", json!(0)),
        ] {
            put(
                &*store,
                &format!("/sprig/dc/{d}/settings/{k}"),
                json!({"v": 1, "value": v, "set_by": "op:r2last", "reason": "live_r2last_bins"}),
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
         [backend_secret]\npassword = \"r2last-per-user-secret\"\n",
    )
    .expect("backends");
    std::fs::write(
        work.join("extract.keys"),
        "# r2last test key\n1 7f1c0d2e3a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5\n",
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
    if next_id["next"].as_u64().unwrap_or(0) < TENANT_BASE {
        put(
            &*store,
            "/sprig/g/ids/tenant",
            json!({"v": 1, "next": TENANT_BASE}),
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
            eprintln!("three DCs active after {:?}", t0.elapsed());
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(180),
            "DCs never activated"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // ---- clusters (1 GB declared storage each), agents, gateways, workers ----
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
    let mut home_of: BTreeMap<String, String> = BTreeMap::new();
    let mk = |t: &str, dc: &str| (t.to_owned(), dc.to_owned());
    let created = [
        mk("r2last-t", "fra1"),  // follows its traffic
        mk("r2last-r", "fra1"),  // residency EU
        mk("r2last-s", "fra1"),  // read-only standby, promotion
        mk("r2last-ga", "ams1"), // filler: makes ams1 busy
        mk("r2last-gf", "fra1"), // filler: makes fra1 a little busy
    ];
    for (t, dc) in &created {
        let v = ctl_retry(
            &sprigctl,
            dc,
            &["tenant", "create", t, "--cluster", &cluster[dc.as_str()]],
            Duration::from_secs(60),
        )
        .await;
        let id = v["tenant"]["tenant_id"].as_u64().expect("tenant id");
        assert!(
            (TENANT_BASE..TENANT_BASE + 100_000).contains(&id),
            "rule 8f: {id} outside this deployment's range"
        );
        ids.insert(t.clone(), id);
        home_of.insert(t.clone(), dc.clone());
    }
    for (t, dc) in &created {
        user_and_schema(&sprigctl, dc, t).await;
    }
    eprintln!("tenants {ids:?} ({:?})", t_all.elapsed());

    // Fillers: ~120 MB in ams1, ~60 MB in fra1 (storage spare 0.88, 0.94;
    // sin1 stays empty), through the gateways, then fresh statistics.
    for (t, dc, doublings) in [("r2last-ga", "ams1", 17u32), ("r2last-gf", "fra1", 16)] {
        let pl = pool(dc, t);
        let mut c = pl.get_conn().await.expect("filler conn");
        c.query_drop("CREATE TABLE IF NOT EXISTS fill (id BIGINT AUTO_INCREMENT PRIMARY KEY, p VARCHAR(1000) NOT NULL) ENGINE=InnoDB").await.expect("fill table");
        c.query_drop("INSERT INTO fill (p) VALUES (REPEAT('x', 900))")
            .await
            .expect("fill");
        for _ in 0..doublings {
            c.query_drop("INSERT INTO fill (p) SELECT p FROM fill")
                .await
                .expect("double");
        }
        drop(c);
        pl.disconnect().await.ok();
        let id = ids[t];
        let schema = scalar(
            &root_url(db_port[dc]),
            &format!("SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME='fill' AND TABLE_SCHEMA LIKE 't{id}\\_%'"),
        )
        .await
        .expect("filler schema");
        let mut c = mysql_async::Conn::new(opts(&root_url(db_port[dc])))
            .await
            .expect("root");
        c.query_drop(format!("ANALYZE TABLE `{schema}`.fill"))
            .await
            .expect("analyze");
        let _ = c.disconnect().await;
    }
    eprintln!("fillers written ({:?})", t_all.elapsed());

    // =====================================================================
    // 1. RF3: a tenant's own failover.standby_dcs wins over its DC's
    // =====================================================================
    let s_id = ids["r2last-s"];
    let t_id = ids["r2last-t"];
    let r_id = ids["r2last-r"];
    // R's residency: the EU DCs.
    ctl_retry(
        &sprigctl,
        "fra1",
        &[
            "tenant",
            "residency",
            "r2last-r",
            "--policy",
            r#"jurisdiction = "EU""#,
            "--apply",
        ],
        Duration::from_secs(30),
    )
    .await;
    for t in ["r2last-s", "r2last-gf"] {
        tenant_setting(&sprigctl, "fra1", t, "failover.standby_dcs", "0").await;
    }
    // The direct account RF10 checks: the tenant's own name pattern, so the
    // standby copies it with its grants.
    let direct = format!("t{s_id}_direct");
    for q in [
        format!("CREATE USER IF NOT EXISTS '{direct}'@'%' IDENTIFIED BY 'direct-pw'"),
        format!("GRANT SELECT, INSERT, UPDATE, DELETE ON `t{s_id}\\_%`.* TO '{direct}'@'%'"),
    ] {
        let mut c = mysql_async::Conn::new(opts(&root_url(fra1_port)))
            .await
            .expect("root");
        c.query_drop(q).await.expect("direct account");
        let _ = c.disconnect().await;
    }
    let raised = Instant::now();
    setting(&sprigctl, "fra1", "failover.standby_dcs", "1").await;
    let streaming = |e: &Value, dc: &str| e["standby_jobs"][dc]["job"].as_str().map(str::to_owned);
    // T and R get standbys (ams1: the nearest); S, whose own value is 0,
    // doesn't.
    let t0 = Instant::now();
    loop {
        let (et, er) = (entry(&*store, t_id).await, entry(&*store, r_id).await);
        let mut ok = true;
        for e in [&et, &er] {
            ok &= match streaming(e, "ams1") {
                Some(j) => job_state(&*store, "ams1", &j).await["state"] == "streaming",
                None => false,
            };
        }
        if ok {
            eprintln!(
                "1. T and R streaming standbys in ams1 {:?} after dc-scope failover.standby_dcs = 1",
                raised.elapsed()
            );
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(240),
            "no standbys: {et} {er}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let es = entry(&*store, s_id).await;
    assert!(
        es["standby_jobs"]
            .as_object()
            .is_none_or(serde_json::Map::is_empty),
        "RF3: S's tenant-scope 0 must win over fra1's 1: {es}"
    );
    eprintln!("1. S (tenant-scope failover.standby_dcs = 0) has no standby while fra1's is 1");
    let raised = Instant::now();
    tenant_setting(&sprigctl, "fra1", "r2last-s", "failover.standby_dcs", "1").await;
    let s_job = loop {
        let e = entry(&*store, s_id).await;
        if let Some(j) = streaming(&e, "ams1")
            && job_state(&*store, "ams1", &j).await["state"] == "streaming"
        {
            eprintln!(
                "1. S's tenant-scope failover.standby_dcs = 1: standby streaming {:?} later (its job recorded {:?} ms after the set)",
                raised.elapsed(),
                e["standby_jobs"]["ams1"]["created_ms"]
                    .as_u64()
                    .map(|c| c.saturating_sub(now_ms() - raised.elapsed().as_millis() as u64))
            );
            break j;
        }
        assert!(
            raised.elapsed() < Duration::from_secs(240),
            "S got no standby: {e}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };

    // =====================================================================
    // 2. RF10: the standby copy is read-only for the tenant's accounts
    // =====================================================================
    let s_schema = scalar(
        &root_url(ams1_port),
        &format!("SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME='la' AND TABLE_SCHEMA LIKE 't{s_id}\\_%'"),
    )
    .await
    .expect("S's copy in ams1");
    let direct_url = |p: u16| format!("mysql://{direct}:direct-pw@127.0.0.1:{p}/{s_schema}");
    let try_write = |p: u16| {
        let url = direct_url(p);
        async move {
            let mut c = mysql_async::Conn::new(opts(&url))
                .await
                .map_err(|e| e.to_string())?;
            let r = c
                .query_drop("INSERT INTO la (k, v) VALUES (9001, 'direct') ON DUPLICATE KEY UPDATE v = CONCAT(v, ',direct')")
                .await
                .map_err(|e| code(&e));
            let read: Option<u64> = c
                .query_first("SELECT COUNT(*) FROM la")
                .await
                .ok()
                .flatten();
            let _ = c.disconnect().await;
            Ok::<_, String>((r, read))
        }
    };
    let (w, read) = try_write(ams1_port)
        .await
        .expect("the direct account logs in to the copy");
    assert_eq!(
        w,
        Err("1142".to_owned()),
        "RF10: a direct write to the standby copy must be refused"
    );
    assert!(read.is_some(), "reads of the copy still work");
    let recorded = scalar(
        &root_url(ams1_port),
        &format!(
            "SELECT COUNT(*) FROM __sprig_sys.__sprig_standby_grants WHERE tenant_id = {s_id}"
        ),
    )
    .await;
    eprintln!(
        "2. RF10: a direct INSERT on S's standby copy in ams1 got 1142; SELECT works ({read:?} rows); {recorded:?} grants recorded for the promotion"
    );
    // The home still takes direct writes (the account is the tenant's).
    let (w, _) = try_write(fra1_port).await.expect("home login");
    assert_eq!(w, Ok(()), "the home copy is writable");
    // Undo that direct write at home so the appends check stays exact.
    {
        let mut c = mysql_async::Conn::new(opts(&root_url(fra1_port)))
            .await
            .expect("root");
        c.query_drop(format!("DELETE FROM `{s_schema}`.la WHERE k = 9001"))
            .await
            .expect("undo");
        let _ = c.disconnect().await;
    }

    // =====================================================================
    // 3. Chapter 24 step 7: T follows its traffic, fra1 -> ams1
    // =====================================================================
    // RF3 again: T's own write-pause budget.
    tenant_setting(
        &sprigctl,
        "fra1",
        "r2last-t",
        "migration.write_pause_target_ms",
        "1500",
    )
    .await;
    let stop = Arc::new(AtomicBool::new(false));
    let next = Arc::new(AtomicU64::new(1));
    let tally = Arc::new(Mutex::new(Tally::default()));
    // fra1 reads 150/s, ams1 writes 100/s: more reads, but writes weigh 3.
    let mut clients = workload(
        "fra1",
        "r2last-t",
        3,
        0,
        Duration::from_millis(20),
        &stop,
        &next,
        &tally,
    );
    clients.extend(workload(
        "ams1",
        "r2last-t",
        0,
        2,
        Duration::from_millis(20),
        &stop,
        &next,
        &tally,
    ));
    // R: all its traffic from sin1, outside its residency.
    let r_stop = Arc::new(AtomicBool::new(false));
    let r_tally = Arc::new(Mutex::new(Tally::default()));
    let mut r_clients = workload(
        "sin1",
        "r2last-r",
        2,
        1,
        Duration::from_millis(20),
        &r_stop,
        &next,
        &r_tally,
    );
    let shift1 = Instant::now();
    let shift1_ms = now_ms();
    let (mig1, commit1, seen1) = follow(
        &*store,
        &sprigctl,
        "r2last-t",
        t_id,
        "ams1",
        Duration::from_secs(600),
    )
    .await;
    let e = entry(&*store, t_id).await;
    let lm = e["last_move"].clone();
    let job1 = lm["job"].as_str().unwrap_or_default().to_owned();
    let j1 = job_state(&*store, "ams1", &job1).await;
    let hold1 = j1["progress"]["holds_ms"]
        .as_array()
        .and_then(|a| a.iter().filter_map(Value::as_u64).max());
    tokio::time::sleep(Duration::from_secs(5)).await;
    let (gap1, longest1) = gaps(&tally.lock().expect("lock"), shift1_ms, now_ms());
    eprintln!(
        "3. T fra1 -> ams1: decided {:?} after the traffic started, committed {:?} after the start ({:?} moving); write pause (hold) {hold1:?} ms, budget {} (tenant scope); longest gap between acknowledged writes {gap1} ms, longest write {longest1} ms; cause {}",
        mig1.map(|m| m - shift1),
        commit1 - shift1,
        mig1.map(|m| commit1 - m),
        lm["migration"]["max_pause_ms"],
        lm["migration"]["cause"],
    );
    assert_eq!(
        lm["migration"]["cause"], "traffic",
        "an automatic move: {lm}"
    );
    assert_eq!(
        lm["migration"]["max_pause_ms"], 1500,
        "RF3: T's tenant-scope budget: {lm}"
    );
    assert!(
        seen1
            .iter()
            .any(|s| s.contains("dropping_standby") || s.contains("wait")),
        "hysteresis or the standby drop were seen: {seen1:?}"
    );
    // R never moved: its best allowed DC is its home, and the alert fired.
    let er = entry(&*store, r_id).await;
    assert_eq!(er["home_dc"], "fra1", "R stays inside its residency: {er}");
    let rv = traffic(&sprigctl, "fra1", "r2last-r");
    eprintln!(
        "3. R (residency EU, traffic from sin1): {} / {}: remote best {}",
        rv["auto"]["action"], rv["auto"]["why"], rv["auto"]["evaluation"]["remote_best"]
    );
    assert_eq!(
        rv["auto"]["evaluation"]["remote_best"], "sin1",
        "ResidencyRemoteTraffic: {rv}"
    );

    // =====================================================================
    // 4. Chapter 24 step 8: new tenants land in the least busy allowed DC
    // =====================================================================
    let loads = list(&*store, "/sprig/g/dc-load/").await;
    let spare: BTreeMap<String, f64> = loads
        .iter()
        .filter_map(|l| {
            Some((
                l["dc_id"].as_str()?.to_owned(),
                l["storage_spare"].as_f64()?,
            ))
        })
        .collect();
    eprintln!("4. dc-load storage spare: {spare:?}");
    let v = ctl_retry(
        &sprigctl,
        "fra1",
        &["tenant", "create", "r2last-n"],
        Duration::from_secs(120),
    )
    .await;
    let n_id = v["tenant"]["tenant_id"].as_u64().expect("n id");
    assert!(
        (TENANT_BASE..TENANT_BASE + 100_000).contains(&n_id),
        "rule 8f: {n_id}"
    );
    ids.insert("r2last-n".into(), n_id);
    let n_home = v["tenant"]["home_dc"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    eprintln!(
        "4. r2last-n created at fra1 without a hint: homed in {n_home} ({})",
        v["placement"]
    );
    assert_eq!(n_home, "sin1", "the least busy DC: {spare:?}");
    let v = ctl_retry(
        &sprigctl,
        "sin1",
        &["tenant", "create", "r2last-rc", "--under", "r2last-r"],
        Duration::from_secs(120),
    )
    .await;
    let rc_id = v["tenant"]["tenant_id"].as_u64().expect("rc id");
    assert!(
        (TENANT_BASE..TENANT_BASE + 100_000).contains(&rc_id),
        "rule 8f: {rc_id}"
    );
    ids.insert("r2last-rc".into(), rc_id);
    let rc_home = v["tenant"]["home_dc"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    eprintln!(
        "4. r2last-rc (child of the EU tenant) created at sin1 without a hint: homed in {rc_home}"
    );
    assert_eq!(
        rc_home, "fra1",
        "residency first, then the least busy allowed: {spare:?}"
    );

    // =====================================================================
    // 5. T's traffic moves to sin1: cooldown, hysteresis, then the move
    // =====================================================================
    stop.store(true, Ordering::SeqCst);
    for c in clients.drain(..) {
        let _ = c.await;
    }
    stop.store(false, Ordering::SeqCst);
    let first = std::mem::take(&mut *tally.lock().expect("lock"));
    let (acked, lost) = check_appends(&root_url(ams1_port), t_id, &first).await;
    eprintln!(
        "3. T's first move: {acked} acknowledged appends, {} lost; errors {:?}",
        lost.len(),
        first.errors
    );
    assert!(lost.is_empty(), "acknowledged appends lost: {lost:?}");
    let mut clients = workload(
        "sin1",
        "r2last-t",
        3,
        2,
        Duration::from_millis(20),
        &stop,
        &next,
        &tally,
    );
    let shift2 = Instant::now();
    let shift2_ms = now_ms();
    let (mig2, commit2, seen2) = follow(
        &*store,
        &sprigctl,
        "r2last-t",
        t_id,
        "sin1",
        Duration::from_secs(600),
    )
    .await;
    let e = entry(&*store, t_id).await;
    let lm = e["last_move"].clone();
    let j2 = job_state(&*store, "sin1", lm["job"].as_str().unwrap_or_default()).await;
    let hold2 = j2["progress"]["holds_ms"]
        .as_array()
        .and_then(|a| a.iter().filter_map(Value::as_u64).max());
    tokio::time::sleep(Duration::from_secs(5)).await;
    stop.store(true, Ordering::SeqCst);
    for c in clients.drain(..) {
        let _ = c.await;
    }
    let (gap2, longest2) = gaps(&tally.lock().expect("lock"), shift2_ms, now_ms());
    eprintln!(
        "5. T ams1 -> sin1: decided {:?} after the traffic moved (cooldown 90 s from the last commit, sustain 30 s), committed {:?} after ({:?} moving); hold {hold2:?} ms; longest gap {gap2} ms, longest write {longest2} ms",
        mig2.map(|m| m - shift2),
        commit2 - shift2,
        mig2.map(|m| commit2 - m),
    );
    assert!(
        seen2.iter().any(|s| s.contains("cooldown")),
        "the cooldown held the second move: {seen2:?}"
    );
    let second = std::mem::take(&mut *tally.lock().expect("lock"));
    let (acked, lost) = check_appends(&root_url(sin1_port), t_id, &second).await;
    eprintln!(
        "5. T's second move: {acked} acknowledged appends, {} lost; errors {:?}",
        lost.len(),
        second.errors
    );
    assert!(lost.is_empty(), "acknowledged appends lost: {lost:?}");
    r_stop.store(true, Ordering::SeqCst);
    for c in r_clients.drain(..) {
        let _ = c.await;
    }

    // =====================================================================
    // 6. RF10: promotion restores writes; no acknowledged write lost
    // =====================================================================
    let s_stop = Arc::new(AtomicBool::new(false));
    let s_tally = Arc::new(Mutex::new(Tally::default()));
    let mut s_clients = workload(
        "fra1",
        "r2last-s",
        0,
        3,
        Duration::from_millis(20),
        &s_stop,
        &next,
        &s_tally,
    );
    s_clients.extend(workload(
        "ams1",
        "r2last-s",
        0,
        3,
        Duration::from_millis(20),
        &s_stop,
        &next,
        &s_tally,
    ));
    tokio::time::sleep(Duration::from_secs(15)).await;
    let frozen = Instant::now();
    eprintln!("--- fra1's controller, gateway, agent and worker frozen (SIGSTOP)");
    procs.signal("fra1", "-STOP");
    let mut promoted_after = None;
    while frozen.elapsed() < Duration::from_secs(45) {
        let e = entry(&*store, s_id).await;
        if promoted_after.is_none() && e["home_dc"] == "ams1" && e["state"] == "active" {
            promoted_after = Some(frozen.elapsed());
            eprintln!("S promoted to ams1 {:?} after the freeze", frozen.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let promoted_after = promoted_after.expect("S must be promoted");
    let frozen_ms = now_ms() - frozen.elapsed().as_millis() as u64;
    tokio::time::sleep(Duration::from_secs(5)).await;
    procs.signal("fra1", "-CONT");
    eprintln!("--- fra1 resumed after {:?}", frozen.elapsed());
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
    let left = scalar(
        &root_url(ams1_port),
        &format!(
            "SELECT COUNT(*) FROM __sprig_sys.__sprig_standby_grants WHERE tenant_id = {s_id}"
        ),
    )
    .await;
    let (w, _) = try_write(ams1_port)
        .await
        .expect("login to the promoted copy");
    eprintln!(
        "6. after the promotion: a direct INSERT on ams1's copy gives {w:?}; recorded grants left {left:?}"
    );
    assert_eq!(
        w,
        Ok(()),
        "RF10: the promotion restores the tenant's write privileges"
    );
    {
        let mut c = mysql_async::Conn::new(opts(&root_url(ams1_port)))
            .await
            .expect("root");
        c.query_drop(format!("DELETE FROM `{s_schema}`.la WHERE k = 9001"))
            .await
            .expect("undo");
        let _ = c.disconnect().await;
    }
    let st = std::mem::take(&mut *s_tally.lock().expect("lock"));
    // Writes refused by a revoked grant (1142): when, relative to the
    // freeze and to the promotion.
    let denied: Vec<u64> = st
        .err_ms
        .iter()
        .filter(|(_, c)| c == "1142")
        .map(|(at, _)| at.saturating_sub(frozen_ms))
        .collect();
    eprintln!(
        "6. writes refused with 1142: {} (first {:?} ms, last {:?} ms after the freeze; promotion at {} ms)",
        denied.len(),
        denied.first(),
        denied.last(),
        promoted_after.as_millis()
    );
    let (acked, lost) = check_appends(&root_url(ams1_port), s_id, &st).await;
    eprintln!(
        "6. S: promoted {promoted_after:?} after the freeze; {acked} acknowledged appends, {} lost; {} outcomes unknown; errors {:?}",
        lost.len(),
        st.unknown,
        st.errors
    );
    assert!(lost.is_empty(), "acknowledged appends lost: {lost:?}");
    eprintln!("summary: all parts passed in {:?}", t_all.elapsed());

    drop(procs);
    drop([fra1_db, ams1_db, sin1_db]);
    drop(net);
    drop(etcd);
    if std::env::var_os("R2LAST_KEEP_WORK").is_some() {
        eprintln!("logs kept in {}", work.display());
    } else {
        let _ = std::fs::remove_dir_all(&work);
    }
}

// ============================================================================
// Chapter 52 (approvals2): a DC move held at tenant level, escalated
// tenant -> ancestor -> platform, approved over gRPC, then applied
// ============================================================================

/// Rule 8f for the approvals test: above every tenant id on the dev
/// clusters (921,306,000,001 on 10 Nov 2026, 23:00 IST): port 24025 x 10^8.
/// Only this test's own containers hold these schemas.
const AP2_BASE: u64 = 2_402_500_000_000;

/// `method path` on a controller's Admin API (no token: the platform
/// operator); the status and the JSON body.
async fn admin_rest(dc: &str, method: &str, path: &str, body: &Value) -> (u16, Value) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let addr = format!("127.0.0.1:{}", ports(dc).admin);
    let mut s = tokio::net::TcpStream::connect(&addr).await.expect("admin");
    let b = if method == "GET" {
        String::new()
    } else {
        body.to_string()
    };
    let req = format!(
        "{method} {path} HTTP/1.1\r\nhost: {addr}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{b}",
        b.len()
    );
    s.write_all(req.as_bytes()).await.expect("write");
    let mut out = Vec::new();
    s.read_to_end(&mut out).await.expect("read");
    let text = String::from_utf8_lossy(&out).to_string();
    let status = text
        .split(' ')
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = text.split_once("\r\n\r\n").map_or("", |x| x.1);
    // Chunked bodies: the JSON is the part between the first '{' and the
    // last '}'.
    let json = match (body.find('{'), body.rfind('}')) {
        (Some(a), Some(z)) => serde_json::from_str(&body[a..=z]).unwrap_or(Value::Null),
        _ => Value::Null,
    };
    (status, json)
}

fn b64(s: &str) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for c in s.as_bytes().chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        for i in 0..4 {
            out.push(if i <= c.len() {
                T[((n >> (18 - 6 * i)) & 63) as usize] as char
            } else {
                '='
            });
        }
    }
    out
}

/// The kid's open `dc.move` item, as fra1's controller lists it.
async fn dc_move_item(tenant: &str) -> Option<Value> {
    let (_, v) = admin_rest(
        "fra1",
        "GET",
        &format!("/admin/v1/recommendations?tenant={tenant}&kind=dc.move&all=true"),
        &Value::Null,
    )
    .await;
    v["items"].as_array()?.first().cloned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn bins_approvals_dc_move_escalates_tenant_ancestor_platform() {
    use sprig_api::proto::admin::recommendations_client::RecommendationsClient;
    use sprig_api::proto::admin::{ListRequest, NameRequest};
    if !skip::flag(skip::DOCKER) {
        skip::report_missing(
            skip::DOCKER,
            "bins_approvals_dc_move_escalates_tenant_ancestor_platform",
        );
        return;
    }
    let _one = ONE_AT_A_TIME.lock().await;
    let Some((sprigdb, sprigctl)) = bins() else {
        eprintln!("build sprigdb and sprigctl first (cargo build -p sprigdb -p sprigctl)");
        return;
    };
    let work = std::env::temp_dir().join(format!("approvals2-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).expect("work dir");
    let t_all = Instant::now();

    // ---- etcd and one MariaDB primary per DC (own containers) ----
    let ep = free_port();
    let etcd_pub = format!("127.0.0.1:{ep}:2379");
    let etcd = OwnContainer::run(
        "r2last-ap2-etcd",
        &[
            "--memory",
            "256m",
            "-p",
            &etcd_pub,
            "quay.io/coreos/etcd:v3.6.5",
            "etcd",
            "--listen-client-urls=http://0.0.0.0:2379",
            "--advertise-client-urls=http://r2last-ap2-etcd.invalid:2379",
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
    let net = Net(format!("sprig-sim-ap2-{}", std::process::id()));
    docker(&["network", "create", &net.0]).expect("network");
    let (fra1_db, fra1_port) = mariadb(&net.0, "ap2fra1db", 101, 1, None);
    let (ams1_db, ams1_port) = mariadb(&net.0, "ap2ams1db", 201, 2, None);
    let (sin1_db, sin1_port) = mariadb(&net.0, "ap2sin1db", 301, 3, None);
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
    for d in DCS {
        delay_proxy(
            ports(d).internal_proxy,
            ports(d).internal,
            Duration::from_millis(delay_ms(d) / 2),
        )
        .await;
    }
    let certs = sprig_testkit::compose::compose_dir().join("gateway-certs");
    for (k, v) in [
        ("migration.evaluate_seconds", json!(5)),
        ("migration.window_minutes", json!(2)),
        ("migration.min_signal_requests", json!(300)),
        ("migration.sustain_minutes", json!(0.5)),
        ("migration.cooldown_hours", json!(0.025)),
        ("recovery.stability_minutes", json!(0)),
        ("control.latency_probe_seconds", json!(2)),
        ("routing.traffic_report_seconds", json!(5)),
        ("mover.ready_sustain_seconds", json!(3)),
        ("mover.source_retain_hours", json!(0)),
        ("approvals.tick_seconds", json!(5)),
    ] {
        put(
            &*store,
            &format!("/sprig/g/settings/{k}"),
            json!({"v": 1, "value": v, "set_by": "op:approvals2", "reason": "chapter 52 live test"}),
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
                json!({"v": 1, "value": v, "set_by": "op:approvals2", "reason": "chapter 52 live test"}),
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
         [backend_secret]\npassword = \"ap2-per-user-secret\"\n",
    )
    .expect("backends");
    std::fs::write(
        work.join("extract.keys"),
        "# approvals2 test key\n1 7f1c0d2e3a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5\n",
    )
    .expect("keys");

    // ---- controllers, DCs, clusters, agents, gateways, workers ----
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
    put(
        &*store,
        "/sprig/g/ids/tenant",
        json!({"v": 1, "next": AP2_BASE}),
    )
    .await;
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
    eprintln!("all processes ready ({:?})", t_all.elapsed());

    // ---- tenants: ap2-kid under ap2-parent, both homed in fra1 ----
    let parent = ctl_retry(
        &sprigctl,
        "fra1",
        &[
            "tenant",
            "create",
            "ap2-parent",
            "--cluster",
            &cluster["fra1"],
        ],
        Duration::from_secs(60),
    )
    .await["tenant"]["tenant_id"]
        .as_u64()
        .expect("parent id");
    let kid = ctl_retry(
        &sprigctl,
        "fra1",
        &[
            "tenant",
            "create",
            "ap2-kid",
            "--under",
            "ap2-parent",
            "--cluster",
            &cluster["fra1"],
        ],
        Duration::from_secs(60),
    )
    .await["tenant"]["tenant_id"]
        .as_u64()
        .expect("kid id");
    for id in [parent, kid] {
        assert!(
            (AP2_BASE..AP2_BASE + 100_000).contains(&id),
            "rule 8f: {id} outside this test's range"
        );
    }
    user_and_schema(&sprigctl, "fra1", "ap2-kid").await;
    // DC moves of the kid wait for an approval at tenant level, and climb
    // a level every minute nobody acts.
    tenant_setting(&sprigctl, "fra1", "ap2-kid", "migration.mode", "approve").await;
    tenant_setting(
        &sprigctl,
        "fra1",
        "ap2-kid",
        "approvals.escalate_after",
        "dc.move=1m",
    )
    .await;
    eprintln!("tenants parent {parent}, kid {kid} ({:?})", t_all.elapsed());

    // ---- the kid's traffic comes from ams1 ----
    let stop = Arc::new(AtomicBool::new(false));
    let next = Arc::new(AtomicU64::new(1));
    let tally = Arc::new(Mutex::new(Tally::default()));
    let mut clients = workload(
        "fra1",
        "ap2-kid",
        1,
        0,
        Duration::from_millis(50),
        &stop,
        &next,
        &tally,
    );
    clients.extend(workload(
        "ams1",
        "ap2-kid",
        0,
        2,
        Duration::from_millis(20),
        &stop,
        &next,
        &tally,
    ));
    let traffic_at = Instant::now();

    // 1. Held at tenant level: an open dc.move item, the tenant stays.
    let item = loop {
        if let Some(i) = dc_move_item("ap2-kid").await
            && i["state"] == "open"
        {
            break i;
        }
        assert!(
            traffic_at.elapsed() < Duration::from_secs(420),
            "no dc.move item: {}",
            traffic(&sprigctl, "fra1", "ap2-kid")
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    };
    let id = item["id"].as_str().expect("id").to_owned();
    eprintln!(
        "1. {id} raised {:?} after the traffic started: level {}, notifies {}: {}",
        traffic_at.elapsed(),
        item["level"],
        item["notify_level"],
        item["summary"]
    );
    assert_eq!(item["level"], "tenant", "{item}");
    assert_eq!(item["notify_level"], "tenant", "{item}");
    assert_eq!(item["action"]["machine"]["to"], "ams1", "{item}");
    let raised = Instant::now();

    // 2. Escalated to the ancestor, then the platform; never moved meanwhile.
    let mut seen = vec!["tenant".to_owned()];
    loop {
        let i = dc_move_item("ap2-kid").await.expect("item");
        let nl = i["notify_level"].as_str().unwrap_or_default().to_owned();
        if seen.last() != Some(&nl) {
            eprintln!("2. {id} notifies {nl} after {:?}", raised.elapsed());
            seen.push(nl.clone());
        }
        let e = entry(&*store, kid).await;
        assert_eq!(e["home_dc"], "fra1", "held: the kid must not move: {e}");
        assert_eq!(i["state"], "open", "{i}");
        if nl == "platform" {
            break;
        }
        assert!(
            raised.elapsed() < Duration::from_secs(300),
            "no escalation to the platform: {seen:?}"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    assert_eq!(
        seen,
        vec![
            "tenant".to_owned(),
            format!("ancestor:{parent}"),
            "platform".to_owned()
        ]
    );
    let auto = traffic(&sprigctl, "fra1", "ap2-kid");
    eprintln!(
        "2. while held the decider says {} ({})",
        auto["auto"]["action"], auto["auto"]["why"]
    );
    assert_eq!(auto["auto"]["action"], "awaiting_approval", "{auto}");

    // 3. Who may act: a kid user without rights is refused (39974); the
    //    parent's administrator may act on the descendant's item.
    let (s, v) = admin_rest(
        "fra1",
        "POST",
        &format!("/admin/v1/recommendations/{id}:approve"),
        &json!({"reason": "x", "caller": {"name": "studio:ap2-kid.nobody", "tenant": kid}}),
    )
    .await;
    eprintln!("3. a kid user without rights: {s} {}", v["code"]);
    assert_eq!(s, 403, "{v}");
    let claim = json!({"name": "studio:ap2-parent.admin", "tenant": parent, "tenant_admin": true})
        .to_string();
    let enc: String = claim
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect();
    let (_, v) = admin_rest(
        "fra1",
        "GET",
        &format!("/admin/v1/recommendations/{id}?caller={enc}"),
        &Value::Null,
    )
    .await;
    assert_eq!(v["may_act"], true, "the parent's administrator: {v}");

    // 4. gRPC through fra1's gateway: the kid's login lists its item with
    //    the same claims as REST; the platform approves.
    let ch =
        tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{}", ports("fra1").http))
            .expect("endpoint")
            .connect()
            .await
            .expect("grpc");
    let mut rc = RecommendationsClient::new(ch);
    let mut req = tonic::Request::new(ListRequest {
        query: "kind=dc.move".into(),
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Basic {}", b64(&format!("ap2-kid.app:{APP_PW}")))
            .parse()
            .expect("header"),
    );
    let listed: Value = serde_json::from_str(
        &rc.list_recommendations(req)
            .await
            .expect("list")
            .into_inner()
            .json,
    )
    .expect("json");
    let mine = listed["items"]
        .as_array()
        .and_then(|a| a.iter().find(|i| i["id"] == id.as_str()))
        .cloned()
        .expect("the kid's login sees its item over gRPC");
    eprintln!(
        "4. gRPC ListRecommendations as ap2-kid.app: {} item(s), {id} may_act {}",
        listed["items"].as_array().map_or(0, Vec::len),
        mine["may_act"]
    );
    let approve_at = Instant::now();
    let got = rc
        .act_on_recommendation(NameRequest {
            name: id.clone(),
            query: String::new(),
            body_json:
                json!({"action": "approve", "reason": "platform: move it", "actor": "approvals2"})
                    .to_string(),
        })
        .await
        .expect("approve over gRPC")
        .into_inner();
    let got: Value = serde_json::from_str(&got.json).expect("json");
    eprintln!(
        "4. gRPC ActOnRecommendation approve (operator): {}",
        got["state"]
    );
    assert_eq!(got["state"], "approved", "{got}");

    // 5. The decider's next cycle still qualifies the move: it runs, and
    //    the item is applied once the kid's home is ams1.
    let (mig, commit, _) = follow(
        &*store,
        &sprigctl,
        "ap2-kid",
        kid,
        "ams1",
        Duration::from_secs(420),
    )
    .await;
    eprintln!(
        "5. moved fra1 -> ams1: started {:?} and committed {:?} after the approval",
        mig.map(|m| m - approve_at),
        commit - approve_at
    );
    let t0 = Instant::now();
    let done = loop {
        let (_, v) = admin_rest(
            "ams1",
            "GET",
            &format!("/admin/v1/recommendations/{id}"),
            &Value::Null,
        )
        .await;
        if v["state"] == "applied" {
            break v;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(120),
            "the item never became applied: {v}"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    };
    eprintln!(
        "5. {id}: {} by {} ({})",
        done["state"], done["decided_by"], done["result"]
    );
    let lm = entry(&*store, kid).await["last_move"].clone();
    assert_eq!(lm["migration"]["cause"], "traffic", "{lm}");

    stop.store(true, Ordering::SeqCst);
    for c in clients {
        let _ = c.await;
    }
    let (acked, lost) =
        check_appends(&root_url(ams1_port), kid, &tally.lock().expect("lock")).await;
    eprintln!("5. {acked} acknowledged appends, {} lost", lost.len());
    assert!(lost.is_empty(), "lost {lost:?}");
    eprintln!("done in {:?}", t_all.elapsed());
    drop(procs);
    drop([fra1_db, ams1_db, sin1_db]);
    drop(net);
    drop(etcd);
    let _ = std::fs::remove_dir_all(&work);
}
