//! Job standbys and live settings with the real binaries (chapter 25,
//! "Standbys"; chapter 08, "Live application"; r2final).
//!
//! ```sh
//! cargo build -p sprigdb -p sprigctl
//! SPRIG_TEST_DOCKER=1 cargo test -p sprig-controller --test live_standby_bins -- --nocapture --test-threads=1
//! ```
//!
//! Three simulated DCs on one private etcd (single-etcd mode): `fra1` and
//! `ams1` each run `sprigdb controller`, `gateway`, `worker` and one
//! `agent`; `wit1` is a witness controller. `fra1-db` is a primary with a
//! semi-sync replica (`AFTER_SYNC`, its acknowledgements only); `ams1-db`
//! is a separate primary. Tenant ids start at 26,730,600,000 (rules 8b
//! and 8f).
//!
//! 1. **Settings reach running workers.** `r2final-mv` moves to `ams1`
//!    after `mover.source_retain_hours = 0` is set at `ams1`'s dc scope:
//!    the move's retention ends at once. It moves back to `fra1` with the
//!    default 24 h; while its job retains, `fra1`'s dc scope is set to 0:
//!    the running worker ends the retention within seconds.
//! 2. **The standby loop.** With `failover.standby_dcs` raised from 0 to 1
//!    (a controller setting, live), `fra1`'s leader gives `r2final-sb` a
//!    standby in `ams1`: a `Continuous` move job in `ams1`'s tier, standby
//!    maps, `standby_jobs` in the directory entry. Its lag is measured
//!    under a list-append workload through both DCs' gateways.
//! 3. **Promotion.** `fra1`'s four processes are frozen (`SIGSTOP`, 40 s);
//!    the global leader fails `r2final-sb` over to `ams1`; the promotion
//!    drains the job to `fra1-db`'s position, stops it and makes the copy
//!    home. Checked: every acknowledged append is on `ams1-db`; no new
//!    standby is made in `fra1` while it holds the quarantined copy.

#![allow(
    clippy::too_many_lines,
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

const ROOT_PW: &str = "sprig-r2final-root";
const REPL_PW: &str = "sprig-r2final-repl";
const APP_PW: &str = "sprig-r2final-app";
const DEPLOYMENT: &str = "sprigdb-r2final";
/// Rules 8b and 8f: port 37306 x 100,000, plus 23 x 10^9 to stay above
/// every id on the dev clusters (25,530,600,001 on 10 Nov). Only this
/// test's own containers hold these schemas.
const TENANT_BASE: u64 = 26_730_600_000;
const T_SB: &str = "r2final-sb";
const T_MV: &str = "r2final-mv";
const KEYS: u64 = 8;
const DCS: [&str; 3] = ["fra1", "ams1", "wit1"];

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
}

fn ports(dc: &str) -> Ports {
    let b = match dc {
        "fra1" => 37306,
        "ams1" => 37326,
        _ => 37346,
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
                "info,sprig_controller::failover=debug,sprig_controller::standby=debug",
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
        .env("SPRIGCTL_ACTOR", "r2final")
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
/// it a semi-sync replica.
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
        // Above the freeze: an acknowledged commit always reached the
        // replica (AFTER_SYNC), never fell back to asynchronous.
        "--rpl-semi-sync-master-timeout=60000",
    ]);
    if master.is_none() && domain == 1 {
        args.push("--rpl-semi-sync-master-enabled=ON");
    }
    (
        OwnContainer::run(&format!("r2final-{name}"), &args).expect("mariadb"),
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
}

async fn client(
    p: u64,
    pool: Pool,
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
    while !stop.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let k = rnd(KEYS);
        let v = next.fetch_add(1, Ordering::SeqCst);
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
        let r = tokio::time::timeout(Duration::from_secs(20), async {
            c.query_drop("BEGIN").await?;
            c.exec_drop(
                "INSERT INTO la (k, v) VALUES (?, ?) ON DUPLICATE KEY UPDATE v = CONCAT(v, ',', VALUES(v))",
                (k, v.to_string()),
            )
            .await?;
            c.query_drop("COMMIT").await
        })
        .await;
        let mut t = tally.lock().expect("lock");
        match r {
            Ok(Ok(())) => t.acked.entry(k).or_default().push(v),
            Ok(Err(e)) => {
                *t.errors.entry(code(&e)).or_default() += 1;
                t.unknown += 1;
            }
            Err(_) => t.unknown += 1,
        }
    }
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

/// The mover jobs of `tenant` in `dc`'s tier.
async fn jobs_of(store: &dyn MetaStore, dc: &str, tenant: u64) -> Vec<Value> {
    list(store, &format!("/sprig/dc/{dc}/mover/jobs/"))
        .await
        .into_iter()
        .filter(|j| j["tenant_id"].as_u64() == Some(tenant))
        .collect()
}

async fn entry(store: &dyn MetaStore, tenant: u64) -> Value {
    get(store, &format!("/sprig/g/tenants/{tenant}"))
        .await
        .unwrap_or_default()
}

async fn schemas_of(url: &str, tenant: u64) -> u64 {
    scalar(
        url,
        &format!(
            "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE 't{tenant}\\_%'"
        ),
    )
    .await
    .and_then(|v| v.parse().ok())
    .unwrap_or(0)
}

async fn setting(sprigctl: &Path, dc: &str, k: &str, v: &str) {
    ctl_retry(
        sprigctl,
        dc,
        &[
            "settings", "set", k, v, "--scope", "dc", "--dc", dc, "--reason", "r2final", "--yes",
        ],
        Duration::from_secs(30),
    )
    .await;
}

/// Moves `tenant` to `to` (asking its home `from`) and waits for the move
/// job to commit; returns the job's id in `to`'s tier.
async fn move_tenant(
    store: &dyn MetaStore,
    sprigctl: &Path,
    tenant: &str,
    id: u64,
    from: &str,
    to: &str,
) -> String {
    let before: BTreeSet<String> = jobs_of(store, to, id)
        .await
        .iter()
        .filter_map(|j| j["job_id"].as_str().map(str::to_owned))
        .collect();
    ctl_retry(
        sprigctl,
        from,
        &["tenant", "move", tenant, "--to", to, "--yes"],
        Duration::from_secs(60),
    )
    .await;
    let t0 = Instant::now();
    loop {
        let e = entry(store, id).await;
        if e["home_dc"] == to && e["state"] == "active" {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(240),
            "{tenant} never moved to {to}: {e}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    jobs_of(store, to, id)
        .await
        .iter()
        .filter_map(|j| j["job_id"].as_str().map(str::to_owned))
        .find(|j| !before.contains(j))
        .expect("the move's job")
}

async fn job_state(store: &dyn MetaStore, dc: &str, job: &str) -> Value {
    get(store, &format!("/sprig/dc/{dc}/mover/jobs/{job}"))
        .await
        .unwrap_or_default()
}

static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn bins_job_standby_settings_and_promotion() {
    if !skip::flag(skip::DOCKER) {
        skip::report_missing(skip::DOCKER, "bins_job_standby_settings_and_promotion");
        return;
    }
    let _one = ONE_AT_A_TIME.lock().await;
    let Some((sprigdb, sprigctl)) = bins() else {
        eprintln!("build sprigdb and sprigctl first (cargo build -p sprigdb -p sprigctl)");
        return;
    };
    let work = std::env::temp_dir().join(format!("r2final-standby-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).expect("work dir");

    // ---- etcd ----
    let ep = free_port();
    let etcd_pub = format!("127.0.0.1:{ep}:2379");
    let etcd = OwnContainer::run(
        "r2final-etcd",
        &[
            "--memory",
            "256m",
            "-p",
            &etcd_pub,
            "quay.io/coreos/etcd:v3.6.5",
            "etcd",
            "--listen-client-urls=http://0.0.0.0:2379",
            "--advertise-client-urls=http://r2final-etcd.invalid:2379",
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

    // ---- MariaDB ----
    let net = Net(format!("sprig-sim-r2final-{}", std::process::id()));
    docker(&["network", "create", &net.0]).expect("network");
    let (fra1_db, fra1_port) = mariadb(&net.0, "fra1db", 101, 1, None);
    let fra1_root = format!("mysql://root:{ROOT_PW}@127.0.0.1:{fra1_port}");
    wait_for(&fra1_root, "SELECT 1", "1", Duration::from_secs(120)).await;
    let (fra1_rep, fra1_rep_port) = mariadb(&net.0, "fra1rep", 102, 1, Some("fra1db"));
    let rep_root = format!("mysql://root:{ROOT_PW}@127.0.0.1:{fra1_rep_port}");
    let (ams1_db, ams1_port) = mariadb(&net.0, "ams1db", 201, 2, None);
    let ams1_root = format!("mysql://root:{ROOT_PW}@127.0.0.1:{ams1_port}");
    for u in [&rep_root, &ams1_root] {
        wait_for(u, "SELECT 1", "1", Duration::from_secs(120)).await;
    }
    wait_for(&fra1_root, "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME='Rpl_semi_sync_master_clients'", "1", Duration::from_secs(60)).await;
    assert_eq!(
        scalar(&fra1_root, "SELECT @@rpl_semi_sync_master_wait_point")
            .await
            .as_deref(),
        Some("AFTER_SYNC")
    );
    eprintln!(
        "MariaDB up: fra1-db {fra1_port} (semi-sync AFTER_SYNC to {fra1_rep_port}), ams1-db {ams1_port}"
    );

    // ---- deployment settings ----
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
            // Standbys start once the settings test is done (part 2).
            ("failover.standby_dcs", json!(0)),
        ] {
            put(
                &*store,
                &format!("/sprig/dc/{d}/settings/{k}"),
                json!({"v": 1, "value": v, "set_by": "op:r2final", "reason": "live_standby_bins"}),
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
         [backend_secret]\npassword = \"r2final-per-user-secret\"\n",
    )
    .expect("backends");
    std::fs::write(
        work.join("extract.keys"),
        "# r2final test key\n1 7f1c0d2e3a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5\n",
    )
    .expect("keys");

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
    for d in ["ams1", "wit1"] {
        procs.start(
            &format!("ctl-{d}"),
            "controller",
            &controller_toml(d, &seed, &work),
        );
    }
    for d in ["ams1", "wit1"] {
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
            t0.elapsed() < Duration::from_secs(120),
            "DCs never activated"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // ---- clusters, agents, gateways, workers ----
    let cred = format!("file:{}", creds.display());
    let mut cluster = BTreeMap::new();
    for (dc, port) in [("fra1", fra1_port), ("ams1", ams1_port)] {
        let v = ctl_retry(
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
            Duration::from_secs(30),
        )
        .await;
        let c = v["cluster"]["cluster_id"]
            .as_str()
            .expect("cluster id")
            .to_owned();
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
            assert!(t0.elapsed() < Duration::from_secs(90), "{c} never active");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        procs.start(&format!("agent-{dc}"), "agent", &agent_toml(dc, &seed, &c));
        assert!(
            ready(ports(dc).agent_tel, Duration::from_secs(60)).await,
            "agent-{dc}"
        );
        cluster.insert(dc, c);
    }
    for u in [&fra1_root, &ams1_root] {
        wait_for(u, "SELECT @@read_only", "0", Duration::from_secs(30)).await;
    }
    for dc in ["fra1", "ams1"] {
        procs.start(
            &format!("gw-{dc}"),
            "gateway",
            &gateway_toml(dc, &seed, &backends),
        );
        procs.start(
            &format!("worker-{dc}"),
            "worker",
            &worker_toml(dc, &seed, &work),
        );
    }
    for dc in ["fra1", "ams1"] {
        assert!(
            ready(ports(dc).gw_tel, Duration::from_secs(120)).await,
            "gw-{dc}"
        );
        assert!(
            ready(ports(dc).worker_tel, Duration::from_secs(120)).await,
            "worker-{dc}"
        );
    }
    eprintln!("clusters {cluster:?}; agents, gateways and workers ready");

    // ---- tenants ----
    let mut ids = BTreeMap::new();
    for t in [T_MV, T_SB] {
        let v = ctl_retry(
            &sprigctl,
            "fra1",
            &["tenant", "create", t, "--cluster", &cluster["fra1"]],
            Duration::from_secs(60),
        )
        .await;
        let id = v["tenant"]["tenant_id"].as_u64().expect("tenant id");
        assert!(
            (TENANT_BASE..TENANT_BASE + 100_000).contains(&id),
            "rule 8f: {id} outside this deployment's range"
        );
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
                "r2final",
                "--yes",
            ],
        );
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
                c.query_drop("INSERT IGNORE INTO la.la (k, v) VALUES (100, '0')")
                    .await?;
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
    eprintln!("tenants {ids:?}");

    // =====================================================================
    // 1. Settings reach running workers (the mover.source_retain_hours bug)
    // =====================================================================
    let mv = ids[T_MV];
    setting(&sprigctl, "ams1", "mover.source_retain_hours", "0").await;
    let set_at = Instant::now();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let job = move_tenant(&*store, &sprigctl, T_MV, mv, "fra1", "ams1").await;
    let moved = set_at.elapsed();
    let t0 = Instant::now();
    loop {
        let j = job_state(&*store, "ams1", &job).await;
        if j["state"] == "done" && schemas_of(&fra1_root, mv).await == 0 {
            eprintln!(
                "1a. move to ams1 committed {moved:?} after ams1's dc-scope mover.source_retain_hours = 0; \
                 retention ended and fra1's copy dropped {:?} after the commit (retain_until {} vs from {})",
                t0.elapsed(),
                j["retain_until_ms"],
                j["retain_from_ms"]
            );
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "the retention didn't follow the dc-scope setting: {j}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // Back to fra1 with fra1's default (24 h): the job retains...
    let job = move_tenant(&*store, &sprigctl, T_MV, mv, "ams1", "fra1").await;
    let t0 = Instant::now();
    loop {
        let j = job_state(&*store, "fra1", &job).await;
        if j["state"] == "retaining" {
            let left = j["retain_until_ms"]
                .as_u64()
                .unwrap_or(0)
                .saturating_sub(now_ms());
            assert!(left > 23 * 3_600_000, "24 h retention expected: {j}");
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "never retaining: {j}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        schemas_of(&ams1_root, mv).await > 0,
        "ams1 keeps the retained source"
    );
    // ...until fra1's dc scope says 0: the running worker applies it.
    setting(&sprigctl, "fra1", "mover.source_retain_hours", "0").await;
    let t0 = Instant::now();
    let (mut done_after, mut dropped_after) = (None, None);
    loop {
        let j = job_state(&*store, "fra1", &job).await;
        if done_after.is_none() && j["state"] == "done" {
            done_after = Some(t0.elapsed());
        }
        if dropped_after.is_none() && schemas_of(&ams1_root, mv).await == 0 {
            dropped_after = Some(t0.elapsed());
        }
        if let (Some(d), Some(x)) = (done_after, dropped_after) {
            eprintln!(
                "1b. fra1's dc-scope mover.source_retain_hours = 0 reached the retaining job: done {d:?} \
                 and ams1's copy dropped {x:?} after the set"
            );
            assert!(
                d < Duration::from_secs(10),
                "chapter 10: live, within seconds"
            );
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(60),
            "the running job ignored the setting: {j}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // =====================================================================
    // 2. The standby loop
    // =====================================================================
    let sb = ids[T_SB];
    let raised = Instant::now();
    setting(&sprigctl, "fra1", "failover.standby_dcs", "1").await;
    let sb_job = loop {
        let e = entry(&*store, sb).await;
        if let Some(j) = e["standby_jobs"]["ams1"]["job"].as_str() {
            let st = job_state(&*store, "ams1", j).await;
            if st["state"] == "streaming" {
                eprintln!(
                    "2. standby of {T_SB} in ams1 streaming {:?} after failover.standby_dcs = 1 \
                     (job {j}, cluster {}, standby_dcs {})",
                    raised.elapsed(),
                    e["standby_jobs"]["ams1"]["cluster"],
                    e["standby_dcs"]
                );
                break j.to_owned();
            }
        }
        assert!(
            raised.elapsed() < Duration::from_secs(180),
            "no streaming standby: {e}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    // The moved-and-back tenant gets one too: ams1 holds only the move's
    // forwarding record (a map naming fra1's cluster), not a copy.
    let t0 = Instant::now();
    loop {
        let e = entry(&*store, mv).await;
        let st = match e["standby_jobs"]["ams1"]["job"].as_str() {
            Some(j) => job_state(&*store, "ams1", j).await["state"].clone(),
            None => Value::Null,
        };
        if st == "streaming" {
            eprintln!(
                "2b. {T_MV} (moved to ams1 and back) has a streaming standby in ams1 too, {:?} later",
                t0.elapsed()
            );
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(240),
            "{T_MV} got no standby: {e}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let maps = list(&*store, &format!("/sprig/dc/ams1/shardmaps/{sb}/")).await;
    assert!(
        !maps.is_empty() && maps.iter().all(|m| m["role"] == "standby"),
        "standby maps in ams1: {maps:?}"
    );

    // ---- the workload, through both DCs' gateways ----
    let stop = Arc::new(AtomicBool::new(false));
    let next = Arc::new(AtomicU64::new(1));
    let tally = Arc::new(Mutex::new(Tally::default()));
    let mut clients = Vec::new();
    for (i, dc) in ["fra1", "ams1"].iter().enumerate() {
        let pl = pool(dc, T_SB);
        for c in 0..3u64 {
            clients.push(tokio::spawn(client(
                (i as u64) * 10 + c,
                pl.clone(),
                Arc::clone(&stop),
                Arc::clone(&next),
                Arc::clone(&tally),
            )));
        }
    }
    let mut lags = Vec::new();
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(20) {
        let j = job_state(&*store, "ams1", &sb_job).await;
        if let (Some(seen), Some(lag)) = (
            j["progress"]["source_seen_ms"].as_u64(),
            j["progress"]["lag_ms"].as_u64(),
        ) {
            lags.push(now_ms().saturating_sub(seen) + lag);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    lags.sort_unstable();
    let acked_before: usize = tally
        .lock()
        .expect("lock")
        .acked
        .values()
        .map(Vec::len)
        .sum();
    eprintln!(
        "standby lag under load (now - source_seen_ms + lag_ms, saved every 2 s): min {:?} median {:?} max {:?} ms; {acked_before} acknowledged appends",
        lags.first(),
        lags.get(lags.len() / 2),
        lags.last()
    );
    assert!(acked_before > 50, "the workload runs ({acked_before})");

    // =====================================================================
    // 3. fra1 frozen; the job standby is promoted
    // =====================================================================
    let frozen = Instant::now();
    eprintln!("--- fra1's controller, gateway, agent and worker frozen (SIGSTOP)");
    procs.signal("fra1", "-STOP");
    let mut promoted_after = None;
    while frozen.elapsed() < Duration::from_secs(40) {
        let e = entry(&*store, sb).await;
        if promoted_after.is_none() && e["home_dc"] == "ams1" && e["state"] == "active" {
            promoted_after = Some(frozen.elapsed());
            eprintln!(
                "{T_SB} promoted to ams1 {:?} after the freeze",
                frozen.elapsed()
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let promoted_after = promoted_after.expect("the job standby must be promoted");
    // Keep writing through ams1 a little after the promotion, then stop.
    tokio::time::sleep(Duration::from_secs(5)).await;
    procs.signal("fra1", "-CONT");
    eprintln!("--- fra1 resumed after {:?}", frozen.elapsed());
    tokio::time::sleep(Duration::from_secs(10)).await;
    stop.store(true, Ordering::SeqCst);
    for c in clients {
        let _ = c.await;
    }
    let j = job_state(&*store, "ams1", &sb_job).await;
    assert_eq!(j["state"], "done", "the standby job stopped: {j}");
    let fo = list(&*store, "/sprig/g/failovers/").await;
    let outcomes: Vec<(u64, String)> = fo
        .iter()
        .flat_map(|r| r["tenants"].as_array().cloned().unwrap_or_default())
        .map(|t| {
            (
                t["tenant_id"].as_u64().unwrap_or(0),
                t["outcome"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect();
    for id in [sb, mv] {
        assert!(
            outcomes.iter().any(|(t, o)| *t == id && o == "promoted"),
            "tenant {id} not promoted: {outcomes:?}"
        );
    }
    eprintln!(
        "failover records: {}",
        serde_json::to_string(&fo).unwrap_or_default()
    );
    let maps = list(&*store, &format!("/sprig/dc/ams1/shardmaps/{sb}/")).await;
    assert!(
        !maps.is_empty() && maps.iter().all(|m| m["role"] == "home"),
        "ams1's maps are home: {maps:?}"
    );

    // ---- zero acknowledged-write loss ----
    let schema = scalar(
        &ams1_root,
        &format!(
            "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME='la' AND TABLE_SCHEMA LIKE 't{sb}\\_%'"
        ),
    )
    .await
    .expect("the promoted copy");
    let have = final_lists(&ams1_root, &schema).await;
    let t = std::mem::take(&mut *tally.lock().expect("lock"));
    let acked: usize = t.acked.values().map(Vec::len).sum();
    let mut lost = Vec::new();
    for (k, vs) in &t.acked {
        for v in vs {
            if !have.get(k).is_some_and(|s| s.contains(v)) {
                lost.push((*k, *v));
            }
        }
    }
    eprintln!(
        "{acked} acknowledged appends, {} lost; {} outcomes unknown; errors {:?}",
        lost.len(),
        t.unknown,
        t.errors
    );
    assert!(lost.is_empty(), "acknowledged appends lost: {lost:?}");

    // ---- after the return: no new standby in fra1 while it holds the copy ----
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(60) {
        if get(&*store, "/sprig/g/dcs/fra1").await.unwrap_or_default()["state"] == "active" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    tokio::time::sleep(Duration::from_secs(15)).await;
    let e = entry(&*store, sb).await;
    eprintln!(
        "after the return: home {}, standby_jobs {}, fra1 maps {:?}",
        e["home_dc"],
        e["standby_jobs"],
        list(&*store, &format!("/sprig/dc/fra1/shardmaps/{sb}/"))
            .await
            .iter()
            .map(|m| m["role"].clone())
            .collect::<Vec<_>>()
    );
    assert!(
        e["standby_jobs"]["fra1"].is_null(),
        "no job standby on the quarantined copy's DC"
    );
    eprintln!(
        "summary: promoted {promoted_after:?} after the freeze; {acked} acknowledged appends, 0 lost"
    );

    drop(procs);
    drop([fra1_db, fra1_rep, ams1_db]);
    drop(net);
    drop(etcd);
    if std::env::var_os("R2FINAL_KEEP_WORK").is_some() {
        eprintln!("logs kept in {}", work.display());
    } else {
        let _ = std::fs::remove_dir_all(&work);
    }
}
