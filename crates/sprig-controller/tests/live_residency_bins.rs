//! Residency with the real binaries (chapter 26; R2's done-when "a pinned
//! tenant never leaves its jurisdiction"): `sprigdb controller` and
//! `sprigdb gateway` in two simulated DCs over one private etcd, set up
//! with `sprigctl` as an operator would.
//!
//! ```sh
//! cargo build -p sprigdb -p sprigctl
//! SPRIG_TEST_DOCKER=1 cargo test -p sprig-controller --test live_residency_bins -- --nocapture
//! ```
//!
//! `fra1` (tagged `jurisdiction=DE`) holds the tenant's cluster, a private
//! `mariadb:11.8` container; `ams1` (`jurisdiction=NL`) has a controller
//! and a gateway only. Ports are agent r2prov's (42306-42337); tenant ids
//! start at 4,230,600,000 (rule 8b). Nothing touches the shared stack.
//!
//! Checked, in order:
//! 1. before any policy, a login at `ams1`'s gateway is decided there;
//! 2. `sprigctl tenant residency --policy 'jurisdiction = "DE"'` prints a
//!    plan and changes nothing; with `--apply` the directory holds the
//!    block with `allowed_dcs = [fra1]`;
//! 3. a login at `ams1`'s gateway is handed to `fra1`'s over
//!    `GatewayPeer.HandoffLogin` and accepted (a wrong password gets
//!    `fra1`'s 1045); `ams1` counts the handoffs, `fra1` audits them;
//! 4. with `deny_remote_access` `ams1` refuses with 39302 while `fra1`
//!    still accepts;
//! 5. a move, a standby and a manual failover to `ams1` are refused
//!    (39550, 39505, 39505).

#![allow(
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::similar_names,
    clippy::cast_possible_truncation
)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, SslOpts};
use serde_json::{Value, json};
use sprig_meta::{Consistency, EtcdConfig, EtcdStore, MetaStore, Op, Tier, Txn};
use sprig_testkit::faults::compose::OwnContainer;
use sprig_testkit::skip;

const ROOT_PW: &str = "sprig-r2prov-root";
const APP_PW: &str = "sprig-r2prov-app";
const DEPLOYMENT: &str = "sprigdb-r2prov";
/// Rule 8b: port 42306 x 100,000.
const TENANT_BASE: u64 = 4_230_600_000;
const TENANT: &str = "r2prov-eu";

struct Ports {
    mysql: u16,
    mariadb: u16,
    http: u16,
    https: u16,
    peer: u16,
    gw_tel: u16,
    admin: u16,
    internal: u16,
    ctl_tel: u16,
}

fn ports(dc: &str) -> Ports {
    if dc == "fra1" {
        Ports {
            mysql: 42306,
            mariadb: 42307,
            http: 42318,
            https: 42319,
            peer: 42320,
            gw_tel: 42321,
            admin: 42322,
            internal: 42323,
            ctl_tel: 42324,
        }
    } else {
        Ports {
            mysql: 42316,
            mariadb: 42317,
            http: 42325,
            https: 42326,
            peer: 42327,
            gw_tel: 42328,
            admin: 42329,
            internal: 42330,
            ctl_tel: 42331,
        }
    }
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

struct Procs {
    bin: PathBuf,
    work: PathBuf,
    kids: Vec<Child>,
}

impl Procs {
    fn start(&mut self, name: &str, role: &str, toml: &str) {
        let cfg = self.work.join(format!("{name}.toml"));
        std::fs::write(&cfg, toml).expect("config");
        let log = std::fs::File::create(self.work.join(format!("{name}.log"))).expect("log");
        let child = Command::new(&self.bin)
            .args(["--config", cfg.to_str().expect("path"), role])
            .env("RUST_LOG", "info")
            .stdout(Stdio::from(log.try_clone().expect("log")))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("spawn sprigdb");
        self.kids.push(child);
    }
}

impl Drop for Procs {
    fn drop(&mut self) {
        for c in &mut self.kids {
            let _ = Command::new("kill")
                .args(["-TERM", &c.id().to_string()])
                .status();
        }
        let t0 = Instant::now();
        for c in &mut self.kids {
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
        if let Some(b) = http_get(port, "/readyz").await
            && b.starts_with("HTTP/1.1 200")
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    false
}

async fn http_get(port: u16, path: &str) -> Option<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .ok()?;
    s.write_all(format!("GET {path} HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n").as_bytes())
        .await
        .ok()?;
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), s.read_to_end(&mut buf))
        .await
        .ok()?
        .ok()?;
    Some(String::from_utf8_lossy(&buf).into_owned())
}

async fn metric(port: u16, line_prefix: &str) -> f64 {
    http_get(port, "/metrics")
        .await
        .unwrap_or_default()
        .lines()
        .filter(|l| l.starts_with(line_prefix))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

fn ctl(sprigctl: &Path, dc: &str, args: &[&str]) -> Result<Value, String> {
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

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .expect("port")
}

async fn scalar(url: &str, sql: &str) -> Option<String> {
    let o: mysql_async::Opts = OptsBuilder::from_opts(mysql_async::Opts::from_url(url).ok()?)
        .prefer_socket(false)
        .into();
    let mut c = tokio::time::timeout(Duration::from_secs(3), mysql_async::Conn::new(o))
        .await
        .ok()?
        .ok()?;
    let v: Option<Option<String>> = c.query_first(sql).await.ok();
    let _ = c.disconnect().await;
    v.flatten()
}

/// A login of `user` at `dc`'s gateway over TLS: `Ok(CURRENT_USER())`, or
/// the server's error code.
async fn login(dc: &str, user: &str, pw: &str) -> Result<String, u16> {
    let o: mysql_async::Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(ports(dc).mysql)
        .user(Some(user))
        .pass(Some(pw))
        .prefer_socket(false)
        .ssl_opts(
            SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        )
        .into();
    match tokio::time::timeout(Duration::from_secs(10), mysql_async::Conn::new(o)).await {
        Ok(Ok(mut c)) => {
            let u: Option<String> = c.query_first("SELECT CURRENT_USER()").await.unwrap_or(None);
            let _ = c.disconnect().await;
            Ok(u.unwrap_or_default())
        }
        Ok(Err(mysql_async::Error::Server(e))) => Err(e.code),
        Ok(Err(e)) => panic!("login {user} at {dc}: {e}"),
        Err(elapsed) => panic!("login {user} at {dc}: {elapsed}"),
    }
}

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
global_candidate = {boot}
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
        ma = p.mariadb,
        ht = p.http,
        hs = p.https,
        pe = p.peer,
        tel = p.gw_tel
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bins_a_pinned_tenant_stays_in_its_jurisdiction() {
    if skip::var(skip::DOCKER).is_none() {
        skip::report_missing(skip::DOCKER, "live_residency_bins");
        return;
    }
    let Some((sprigdb, sprigctl)) = bins() else {
        eprintln!("live_residency_bins: build sprigdb and sprigctl first");
        return;
    };
    let work = std::env::temp_dir().join(format!("r2prov-residency-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).expect("work dir");

    // ---- etcd and one MariaDB, both private ----
    let ep = free_port();
    let etcd_pub = format!("127.0.0.1:{ep}:2379");
    let _etcd = OwnContainer::run(
        "r2prov-etcd",
        &[
            "--memory",
            "256m",
            "-p",
            &etcd_pub,
            "quay.io/coreos/etcd:v3.6.5",
            "etcd",
            "--listen-client-urls=http://0.0.0.0:2379",
            "--advertise-client-urls=http://r2prov-etcd.invalid:2379",
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
    let binlog = format!(
        "{}:/etc/mysql/conf.d/10-binlog.cnf:ro",
        sprig_testkit::compose::compose_dir()
            .join("mariadb/binlog.cnf")
            .display()
    );
    let db_port = free_port();
    let db_pub = format!("127.0.0.1:{db_port}:3306");
    let root_env = format!("MARIADB_ROOT_PASSWORD={ROOT_PW}");
    let _db = OwnContainer::run(
        "r2prov-fra1db",
        &[
            "--memory",
            "512m",
            "-p",
            &db_pub,
            "-e",
            &root_env,
            "-v",
            &binlog,
            "mariadb:11.8",
            "--server-id=101",
            "--innodb-buffer-pool-size=64M",
            "--max-connections=200",
        ],
    )
    .expect("mariadb");
    let root = format!("mysql://root:{ROOT_PW}@127.0.0.1:{db_port}");
    let t0 = Instant::now();
    while scalar(&root, "SELECT 1").await.as_deref() != Some("1") {
        assert!(t0.elapsed() < Duration::from_secs(120), "mariadb never up");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // ---- settings an operator's bootstrap writes ----
    let certs = sprig_testkit::compose::compose_dir().join("gateway-certs");
    for d in ["fra1", "ams1"] {
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
                json!({"v": 1, "value": v, "set_by": "op:r2prov", "reason": "live_residency_bins"}),
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
         [backend_secret]\npassword = \"r2prov-per-user-secret\"\n",
    )
    .expect("backends");

    let mut procs = Procs {
        bin: sprigdb,
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
    fra1["tags"] = json!({"jurisdiction": "DE"});
    put(&*store, "/sprig/g/dcs/fra1", fra1).await;
    ctl_retry(
        &sprigctl,
        "fra1",
        &["dc", "register", "ams1", "--tags", "jurisdiction=NL"],
        Duration::from_secs(30),
    )
    .await;
    procs.start(
        "ctl-ams1",
        "controller",
        &controller_toml("ams1", &seed, &work),
    );
    assert!(
        ready(ports("ams1").ctl_tel, Duration::from_secs(120)).await,
        "ctl-ams1 ready"
    );
    let cred = format!("file:{}", creds.display());
    let v = ctl_retry(
        &sprigctl,
        "fra1",
        &[
            "cluster",
            "register",
            "fra1-db",
            "--topology",
            "single",
            "--node",
            &format!("127.0.0.1:{db_port}"),
            "--credentials",
            &cred,
        ],
        Duration::from_secs(60),
    )
    .await;
    let cluster = v["cluster"]["cluster_id"]
        .as_str()
        .expect("cluster id")
        .to_owned();
    for dc in ["fra1", "ams1"] {
        procs.start(
            &format!("gw-{dc}"),
            "gateway",
            &gateway_toml(dc, &seed, &backends),
        );
    }
    for dc in ["fra1", "ams1"] {
        assert!(
            ready(ports(dc).gw_tel, Duration::from_secs(120)).await,
            "gw-{dc} ready"
        );
    }
    let v = ctl_retry(
        &sprigctl,
        "fra1",
        &["tenant", "create", TENANT, "--cluster", &cluster],
        Duration::from_secs(60),
    )
    .await;
    let id = v["tenant"]["tenant_id"].as_u64().expect("tenant id");
    assert!(id >= TENANT_BASE, "rule 8b ids: {id}");
    ctl_retry(
        &sprigctl,
        "fra1",
        &[
            "tenant",
            "user",
            "add",
            TENANT,
            "app",
            "--password",
            APP_PW,
            "--if-not-exists",
        ],
        Duration::from_secs(30),
    )
    .await;
    let user = format!("{TENANT}.app");
    eprintln!("tenant {TENANT} ({id}) on {cluster}");

    // 1. No policy: ams1 decides its own logins.
    let t0 = Instant::now();
    loop {
        match login("ams1", &user, APP_PW).await {
            Ok(u) => {
                eprintln!("1. before the policy, ams1 accepted {u}");
                break;
            }
            Err(c) if t0.elapsed() < Duration::from_secs(60) => {
                eprintln!("waiting for the tenant at ams1 ({c})");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(c) => panic!("ams1 login before the policy: {c}"),
        }
    }
    let handoffs = r#"sprig_auth_handoffs_total{outcome="decided"}"#;
    let before = metric(ports("ams1").gw_tel, handoffs).await;

    // 2. The plan, then the policy.
    let policy = r#"jurisdiction = "DE""#;
    let plan = ctl(
        &sprigctl,
        "ams1",
        &["tenant", "residency", TENANT, "--policy", policy],
    )
    .expect("dry run");
    eprintln!("2. dry run: {plan}");
    assert_eq!(plan["applied"], false);
    assert_eq!(plan["allowed_dcs"], json!(["fra1"]));
    let dir = get(&*store, &format!("/sprig/g/tenants/{id}"))
        .await
        .expect("entry");
    assert!(dir["residency"].is_null(), "a dry run writes nothing");
    let applied = ctl(
        &sprigctl,
        "ams1",
        &["tenant", "residency", TENANT, "--policy", policy, "--apply"],
    )
    .expect("apply");
    assert_eq!(applied["applied"], true);
    assert_eq!(applied["state"], "compliant", "{applied}");
    let dir = get(&*store, &format!("/sprig/g/tenants/{id}"))
        .await
        .expect("entry");
    assert_eq!(dir["residency"]["allowed_dcs"], json!(["fra1"]), "{dir}");
    eprintln!("2. applied: {}", dir["residency"]);

    // 3. Handed off from ams1 to fra1 (the directory cache is 1 s).
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let t = Instant::now();
    let u = login("ams1", &user, APP_PW).await.expect("relayed login");
    let relayed_ms = t.elapsed().as_millis();
    assert!(u.starts_with("app@"), "{u}");
    assert_eq!(login("ams1", &user, "wrong").await, Err(1045));
    let after = metric(ports("ams1").gw_tel, handoffs).await;
    assert!(after >= before + 2.0, "handoffs {before} -> {after}");
    let fra1_log = std::fs::read_to_string(work.join("gw-fra1.log")).unwrap_or_default();
    let audited = fra1_log.matches("auth.handoff").count();
    assert!(audited >= 2, "fra1 audits the handoffs ({audited})");
    eprintln!(
        "3. ams1 handed the login to fra1: accepted as {u} in {relayed_ms} ms; a wrong password got 1045; \
         ams1 decided-handoff counter {before} -> {after}; fra1 audited {audited}"
    );

    // 4. deny_remote_access.
    ctl(
        &sprigctl,
        "fra1",
        &[
            "tenant",
            "residency",
            TENANT,
            "--deny-remote-access",
            "true",
            "--apply",
        ],
    )
    .expect("deny");
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(login("ams1", &user, APP_PW).await, Err(39302));
    assert!(
        login("fra1", &user, APP_PW).await.is_ok(),
        "fra1 still accepts"
    );
    eprintln!("4. deny_remote_access: ams1 refuses with 39302; fra1 accepts");

    // 5. Nothing moves the tenant to ams1.
    let e = ctl(
        &sprigctl,
        "fra1",
        &["tenant", "move", TENANT, "--to", "ams1"],
    )
    .unwrap_err();
    assert!(e.contains("39550") && e.contains("residency"), "{e}");
    let e = ctl(
        &sprigctl,
        "fra1",
        &[
            "tenant",
            "standby",
            TENANT,
            "--dc",
            "ams1",
            "--cluster",
            "ams1-c1",
        ],
    )
    .unwrap_err();
    assert!(e.contains("39505") && e.contains("residency"), "{e}");
    let e = ctl(
        &sprigctl,
        "ams1",
        &[
            "tenant",
            "failover",
            TENANT,
            "--to",
            "ams1",
            "--old-home-fenced",
            "--accept-data-loss",
        ],
    )
    .unwrap_err();
    assert!(e.contains("39505") && e.contains("residency"), "{e}");
    let dir = get(&*store, &format!("/sprig/g/tenants/{id}"))
        .await
        .expect("entry");
    assert_eq!(dir["home_dc"], "fra1");
    eprintln!("5. move (39550), standby (39505) and failover (39505) to ams1 refused; home fra1");
    drop(procs);
    let _ = std::fs::remove_dir_all(&work);
}
