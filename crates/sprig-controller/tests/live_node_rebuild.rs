//! `errant_extract` and `node_rebuild` with the real binaries (chapter 25,
//! "Recovery after an outage" steps 2-3; r2close).
//!
//! ```sh
//! cargo build -p sprigdb -p sprigctl
//! SPRIG_TEST_DOCKER=1 cargo test -p sprig-controller --test live_node_rebuild -- --nocapture --test-threads=1
//! ```
//!
//! One DC (`fra1`) on a private etcd that advertises an unresolvable name:
//! `sprigdb controller` (admin 41426), `sprigdb worker` (with `[recovery]`
//! extract_dir and key file) and one `sprigdb agent` per MariaDB node with
//! its internal service (41430-41432) and `[agent.rebuild]` commands that
//! reach the node's container with `docker exec`. The nodes run under a
//! small supervisor loop (`/run/sprig-hold` stops it restarting mysqld),
//! so an agent can stop and start mysqld without the container ending, as
//! systemd does on a VM. Containers are capped at 768 MB and removed at
//! the end.
//!
//! [`bins_diverged_writer_is_extracted_and_rebuilt`]: `db1` (primary,
//! semi-sync) and its replicas `db2`, `db3`. The replicas stop receiving;
//! `db1` commits seven row changes in a tenant schema that nobody else
//! has, and is `kill -9`ed. The controller fails over to `db2`. `db1` is
//! started again: the controller reports it diverged, submits
//! `errant_extract` (the worker writes and verifies the sealed extract),
//! then `node_rebuild` (the worker has `db1`'s agent stream a backup from
//! `db3`, restore it and start mysqld; then attaches `db1` to `db2`), and
//! records `rebuilt`. Checked: the extract opens with the key file and
//! holds exactly the seven changes with their SQL; `db1` is read-only,
//! replicates from `db2`, holds none of the diverged rows, and follows
//! writes made during and after the rebuild. Then an operator's
//! `sprigctl cluster rebuild` of the healthy replica `db3` from `db1`,
//! and the refusal to rebuild the writer.
//!
//! With `SPRIG_TEST_INTERNAL_TLS=1` (r2final, decision RC2) every process
//! gets a `[tls]` section from a test CA (etcd runs with client
//! certificates too): the agents' internal services are mutual TLS, the
//! backups stream over it, and the test checks that a plain-HTTP request
//! and a client certificate from another CA get nothing.

#![allow(
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::doc_markdown
)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mysql_async::OptsBuilder;
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use sprig_meta::{Consistency, EtcdConfig, EtcdStore, MetaStore, Op, Tier, Txn};
use sprig_testkit::faults::compose::{OwnContainer, docker};
use sprig_testkit::skip;

const ROOT_PW: &str = "sprig-r2close-root";
const REPL_PW: &str = "sprig-r2close-repl";
const DEPLOYMENT: &str = "sprigdb-r2close";
const DC: &str = "fra1";
/// A tenant-shaped schema (rule 8b's range for port 41406), only on this
/// test's own containers.
const SCHEMA: &str = "t4140600001_r2close";

// This agent's ports: 41426-41442.
const CTL_ADMIN: u16 = 41426;
const CTL_INTERNAL: u16 = 41427;
const CTL_TEL: u16 = 41428;
const WORKER_TEL: u16 = 41429;
const AGENT_SVC: [u16; 3] = [41430, 41431, 41432];
const AGENT_TEL: [u16; 3] = [41440, 41441, 41442];
/// The rebuild stream's cap for the test, MB/s.
const RATE_MB: u64 = 20;

/// `SPRIG_TEST_INTERNAL_TLS=1`: every process with `[tls]` (RC2).
fn internal_tls() -> bool {
    std::env::var_os("SPRIG_TEST_INTERNAL_TLS").is_some_and(|v| v == "1")
}

/// The test PKI in `dir`: `ca.pem`, `etcd(-key).pem` and `node(-key).pem`
/// (every process's identity: server and client, for 127.0.0.1), and a
/// `rogue(-key).pem` from another CA.
fn write_pki(dir: &Path) {
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose,
        IsCa, KeyPair,
    };
    std::fs::create_dir_all(dir).expect("pki dir");
    let ca = |name: &str| {
        let mut p = CertificateParams::new(Vec::<String>::new()).expect("ca");
        p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        p.distinguished_name.push(DnType::CommonName, name);
        CertifiedIssuer::self_signed(p, KeyPair::generate().expect("key")).expect("ca")
    };
    let leaf = |issuer: &CertifiedIssuer<'static, KeyPair>, cn: &str, file: &str| {
        let mut p = CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
            .expect("leaf");
        p.distinguished_name.push(DnType::CommonName, cn);
        p.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let key = KeyPair::generate().expect("key");
        let cert = p.signed_by(&key, issuer).expect("sign");
        std::fs::write(dir.join(format!("{file}.pem")), cert.pem()).expect("pem");
        std::fs::write(dir.join(format!("{file}-key.pem")), key.serialize_pem()).expect("pem");
    };
    let main = ca("r2final internal CA");
    std::fs::write(dir.join("ca.pem"), main.pem()).expect("ca");
    leaf(&main, "etcd", "etcd");
    leaf(&main, "sprig-node", "node");
    let rogue = ca("someone else");
    std::fs::write(dir.join("rogue-ca.pem"), rogue.pem()).expect("ca");
    leaf(&rogue, "stranger", "rogue");
}

/// `[tls]` for a process, when the test runs with TLS.
fn tls_section(work: &Path) -> String {
    if !internal_tls() {
        return String::new();
    }
    let d = work.join("pki");
    format!(
        "[tls]\nca = \"{}\"\ncert = \"{}\"\nkey = \"{}\"\n",
        d.join("ca.pem").display(),
        d.join("node.pem").display(),
        d.join("node-key.pem").display()
    )
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
                "info,sprig_controller::failover=debug,sprig_agent=debug,sprig_worker::recovery=debug",
            )
            .stdout(Stdio::from(log.try_clone().expect("log")))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("spawn sprigdb");
        self.kids.push((name.to_owned(), child));
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
    Ok(serde_json::from_str(&text).unwrap_or(Value::String(text)))
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

/// A primary + replicas node under a supervisor loop: mysqld restarts
/// unless `/run/sprig-hold` exists (an agent's stop and start commands).
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
        "--entrypoint",
        "bash",
    ];
    if let Some(m) = &m {
        args.extend(["-e", m]);
    }
    args.extend([
        "mariadb:11.8",
        "-c",
        "while true; do if [ -e /run/sprig-hold ]; then sleep 0.3; continue; fi; \
         docker-entrypoint.sh \"$@\"; sleep 0.5; done",
        "sprig-run",
        "mariadbd",
        &sid,
        "--gtid-domain-id=1",
        &report,
        "--report-port=3306",
        "--innodb-buffer-pool-size=64M",
        "--max-connections=300",
        "--read-only=ON",
    ]);
    if master.is_none() {
        args.push("--rpl-semi-sync-master-enabled=ON");
    }
    Db {
        name,
        port,
        c: OwnContainer::run(&format!("r2close-{name}"), &args).expect("mariadb"),
    }
}

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
{tls}"#,
        w = work.display(),
        tls = tls_section(work)
    )
}

fn worker_toml(seed: &str, work: &Path) -> String {
    format!(
        r#"[node]
dc = "{DC}"
id = "worker-{DC}"
[meta]
dc_seeds = ["{seed}"]
global_seeds = []
deployment_id = "{DEPLOYMENT}"
[recovery]
extract_dir = "{w}/extracts"
extract_key_file = "{w}/extract.keys"
[telemetry]
bootstrap_log_level = "info"
log_format = "pretty"
bootstrap_addr = "127.0.0.1:{WORKER_TEL}"
{tls}"#,
        w = work.display(),
        tls = tls_section(work)
    )
}

fn agent_toml(seed: &str, cluster: &str, db: &Db, i: usize, work: &Path) -> String {
    let container = db.c.name();
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
internal_addr = "127.0.0.1:{svc}"
[agent.rebuild]
exec_prefix = ["docker", "exec", "-i", "{container}"]
stop_command = ["sh", "-c", "touch /run/sprig-hold && mariadb-admin -uroot -p{ROOT_PW} shutdown"]
start_command = ["rm", "-f", "/run/sprig-hold"]
staging_dir = "/var/lib/sprig-rebuild"
[telemetry]
bootstrap_log_level = "info"
log_format = "pretty"
bootstrap_addr = "127.0.0.1:{tel}"
{tls}"#,
        node = db.name,
        svc = AGENT_SVC[i],
        tel = AGENT_TEL[i],
        tls = tls_section(work),
    )
}

struct Deployment {
    store: Arc<dyn MetaStore>,
    seed: String,
    procs: Procs,
    sprigctl: PathBuf,
    work: PathBuf,
    creds: PathBuf,
    _etcd: OwnContainer,
}

async fn deploy() -> Option<Deployment> {
    let Some((sprigdb, sprigctl)) = bins() else {
        eprintln!("build sprigdb and sprigctl first (cargo build -p sprigdb -p sprigctl)");
        return None;
    };
    let work = std::env::temp_dir().join(format!("r2close-rebuild-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).expect("work dir");
    let ep = free_port();
    let etcd_pub = format!("127.0.0.1:{ep}:2379");
    let tls = internal_tls();
    let pki = work.join("pki");
    if tls {
        write_pki(&pki);
    }
    let mount = format!("{}:/pki:ro", pki.display());
    let mut eargs: Vec<String> = vec!["--memory".into(), "256m".into(), "-p".into(), etcd_pub];
    if tls {
        eargs.extend(["-v".into(), mount]);
    }
    eargs.extend(["quay.io/coreos/etcd:v3.6.5".into(), "etcd".into()]);
    if tls {
        eargs.extend([
            "--listen-client-urls=https://0.0.0.0:2379".into(),
            "--advertise-client-urls=https://r2close-etcd.invalid:2379".into(),
            "--client-cert-auth".into(),
            "--trusted-ca-file=/pki/ca.pem".into(),
            "--cert-file=/pki/etcd.pem".into(),
            "--key-file=/pki/etcd-key.pem".into(),
        ]);
    } else {
        eargs.extend([
            "--listen-client-urls=http://0.0.0.0:2379".into(),
            "--advertise-client-urls=http://r2close-etcd.invalid:2379".into(),
        ]);
    }
    let eargs: Vec<&str> = eargs.iter().map(String::as_str).collect();
    let etcd = OwnContainer::run("r2close-etcd", &eargs).expect("etcd");
    let seed = format!("{}://127.0.0.1:{ep}", if tls { "https" } else { "http" });
    let mut cfg = EtcdConfig::new(vec![seed.clone()]);
    if tls {
        cfg.tls = Some(
            sprig_meta::EtcdTls::from_files(&sprig_common::config::TlsConfig {
                ca: pki.join("ca.pem"),
                cert: pki.join("node.pem"),
                key: pki.join("node-key.pem"),
            })
            .expect("etcd tls"),
        );
    }
    cfg.request_timeout = Duration::from_secs(2);
    let store: Arc<dyn MetaStore> = loop {
        match EtcdStore::connect(Tier::Global, &cfg).await {
            Ok(s) => break Arc::new(s),
            Err(_) => tokio::time::sleep(Duration::from_millis(300)).await,
        }
    };
    let creds = work.join("creds.toml");
    std::fs::write(
        &creds,
        format!(
            "[sprig_ctl]\nuser = \"root\"\npassword = \"{ROOT_PW}\"\ntls = \"skip_verify\"\n\
             [sprig_repl]\nuser = \"root\"\npassword = \"{ROOT_PW}\"\ntls = \"skip_verify\"\n"
        ),
    )
    .expect("creds");
    // The workers' extract keyring (chapter 12's format).
    std::fs::write(
        work.join("extract.keys"),
        "# r2close test key\n1 7f1c0d2e3a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5\n",
    )
    .expect("keys");
    // The stream's cap, small enough to see.
    put(
        &*store,
        &format!("/sprig/dc/{DC}/settings/recovery.rebuild_max_mb_per_second"),
        json!({"v": 1, "value": RATE_MB, "set_by": "op:r2close", "reason": "live_node_rebuild"}),
    )
    .await;
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
    procs.start("worker", "worker", &worker_toml(&seed, &work));
    assert!(
        ready(WORKER_TEL, Duration::from_secs(120)).await,
        "the worker is ready"
    );
    Some(Deployment {
        store,
        seed,
        procs,
        sprigctl,
        work,
        creds,
        _etcd: etcd,
    })
}

impl Deployment {
    async fn register(&self, name: &str, dbs: &[&Db]) -> String {
        let cred = format!("file:{}", self.creds.display());
        let nodes: Vec<String> = dbs
            .iter()
            .map(|d| format!("{}=127.0.0.1:{}", d.name, d.port))
            .collect();
        let mut args = vec!["cluster", "register", name, "--topology", "primary_replica"];
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

    async fn failover_record(&self, cluster: &str) -> Value {
        get(
            &*self.store,
            &format!("/sprig/dc/{DC}/cluster-failover/{cluster}"),
        )
        .await
        .unwrap_or_default()
    }

    async fn job(&self, id: &str) -> Value {
        get(&*self.store, &format!("/sprig/dc/{DC}/jobs/{id}"))
            .await
            .unwrap_or_default()
    }

    async fn wait_job(&self, id: &str, within: Duration) -> Value {
        let t0 = Instant::now();
        let mut last_phase = String::new();
        loop {
            let j = self.job(id).await;
            let state = j["state"].as_str().unwrap_or_default().to_owned();
            if matches!(state.as_str(), "succeeded" | "failed" | "cancelled") {
                return j;
            }
            let phase = j["progress"]["phase"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            if phase != last_phase {
                eprintln!(
                    "  job {id}: {state} {phase} ({} bytes) at {:?}",
                    j["progress"]["bytes"],
                    t0.elapsed()
                );
                last_phase = phase;
            }
            assert!(t0.elapsed() < within, "job {id} didn't end: {j}");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    fn done(self) {
        let work = self.work.clone();
        drop(self);
        if std::env::var_os("R2CLOSE_KEEP_WORK").is_some() {
            eprintln!("logs kept in {}", work.display());
        } else {
            let _ = std::fs::remove_dir_all(&work);
        }
    }
}

/// A writer that inserts into `SCHEMA.kv` on `url` every few ms, counting
/// acknowledged rows; it reconnects after errors.
fn writer(
    url: String,
    from: u64,
    stop: Arc<AtomicBool>,
    acked: Arc<AtomicU64>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut k = from;
        let mut conn: Option<mysql_async::Conn> = None;
        while !stop.load(Ordering::SeqCst) {
            if conn.is_none() {
                conn = mysql_async::Conn::new(opts(&url)).await.ok();
            }
            if let Some(c) = conn.as_mut() {
                let r = c
                    .exec_drop(
                        format!("INSERT INTO {SCHEMA}.kv (k, v) VALUES (?, ?)"),
                        (k, format!("live-{k}")),
                    )
                    .await;
                if r.is_ok() {
                    acked.fetch_add(1, Ordering::SeqCst);
                    k += 1;
                } else {
                    conn = None;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if let Some(c) = conn {
            let _ = c.disconnect().await;
        }
    })
}

static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn bins_diverged_writer_is_extracted_and_rebuilt() {
    if !skip::flag(skip::DOCKER) {
        skip::report_missing(
            skip::DOCKER,
            "bins_diverged_writer_is_extracted_and_rebuilt",
        );
        return;
    }
    let _one = ONE_AT_A_TIME.lock().await;
    let Some(mut d) = deploy().await else {
        return;
    };
    let net = Net(format!("sprig-sim-r2close-{}", std::process::id()));
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
    exec(&db1.root(), &["SET GLOBAL read_only = OFF"]).await;
    let cluster = d.register("r2close-pr", &dbs).await;
    eprintln!("cluster {cluster}: db1 primary, db2 and db3 semi-sync replicas");
    for (i, db) in dbs.iter().enumerate() {
        let toml = agent_toml(&d.seed, &cluster, db, i, &d.work);
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

    // ---- data: ~40 MB so the stream's rate cap shows ----
    exec(
        &db1.root(),
        &[
            &format!("CREATE DATABASE {SCHEMA}"),
            &format!("CREATE TABLE {SCHEMA}.kv (k BIGINT PRIMARY KEY, v VARCHAR(64) NOT NULL) ENGINE=InnoDB"),
            &format!("CREATE TABLE {SCHEMA}.blob_t (id INT AUTO_INCREMENT PRIMARY KEY, b LONGBLOB) ENGINE=InnoDB"),
        ],
    )
    .await;
    {
        let mut c = mysql_async::Conn::new(opts(&db1.root()))
            .await
            .expect("connect");
        for _ in 0..40 {
            c.query_drop(format!(
                "INSERT INTO {SCHEMA}.blob_t (b) SELECT RANDOM_BYTES(1024) FROM {SCHEMA}.seq_1_to_1000"
            ))
            .await
            .expect("blob rows");
        }
        c.query_drop(format!(
            "INSERT INTO {SCHEMA}.kv (k, v) SELECT seq, CONCAT('base-', seq) FROM {SCHEMA}.seq_1_to_2000"
        ))
        .await
        .expect("base rows");
        let _ = c.disconnect().await;
    }
    for r in [&db2, &db3] {
        wait_for(
            &r.root(),
            &format!("SELECT COUNT(*) FROM {SCHEMA}.kv"),
            "2000",
            Duration::from_secs(60),
        )
        .await;
    }
    eprintln!("base data replicated");

    // ---- diverge db1: the replicas stop receiving; db1 commits alone ----
    for r in [&db2, &db3] {
        exec(&r.root(), &["STOP SLAVE IO_THREAD"]).await;
    }
    exec(
        &db1.root(),
        &["SET GLOBAL rpl_semi_sync_master_timeout = 300"],
    )
    .await;
    let errant = [
        format!("INSERT INTO {SCHEMA}.kv (k, v) VALUES (900001, 'errant-1')"),
        format!("INSERT INTO {SCHEMA}.kv (k, v) VALUES (900002, 'errant-2'), (900003, 'errant-3')"),
        format!("UPDATE {SCHEMA}.kv SET v = 'errant-update' WHERE k = 7"),
        format!("DELETE FROM {SCHEMA}.kv WHERE k = 8"),
        format!(
            "INSERT INTO {SCHEMA}.kv (k, v) VALUES (900004, 'errant-4'), (900005, 'it''s errant')"
        ),
    ];
    let errant_refs: Vec<&str> = errant.iter().map(String::as_str).collect();
    exec(&db1.root(), &errant_refs).await;
    let db1_pos = scalar(&db1.root(), "SELECT @@gtid_binlog_pos")
        .await
        .unwrap_or_default();
    eprintln!("db1 diverged: 5 transactions, 7 row changes, at {db1_pos}");
    docker(&["kill", "-s", "KILL", db1.c.name()]).expect("kill");
    eprintln!("--- db1 killed (-9)");
    let t0 = Instant::now();
    let rec = loop {
        let r = d.failover_record(&cluster).await;
        if r["history"].as_array().is_some_and(|h| !h.is_empty()) {
            break r;
        }
        assert!(t0.elapsed() < Duration::from_secs(90), "no failover: {r}");
        tokio::time::sleep(Duration::from_millis(300)).await;
    };
    let new = rec["history"][0]["to"].as_str().expect("to").to_owned();
    eprintln!("failed over to {new} after {:?}", t0.elapsed());
    let newdb = by_name[new.as_str()];
    wait_for(
        &newdb.root(),
        "SELECT @@read_only",
        "0",
        Duration::from_secs(30),
    )
    .await;
    let source = if new == "db2" { "db3" } else { "db2" };
    // Writes go on at the new writer through the whole recovery.
    let stop = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(AtomicU64::new(0));
    let wtask = writer(
        newdb.root(),
        1_000_000,
        Arc::clone(&stop),
        Arc::clone(&acked),
    );

    // ---- db1 returns: diverged -> extracted -> rebuilt ----
    let back = Instant::now();
    docker(&["start", db1.c.name()]).expect("start");
    let t0 = Instant::now();
    let mut seen = String::new();
    let ev = loop {
        let r = d.failover_record(&cluster).await;
        let ev = r["history"][0].clone();
        let former = ev["former"].as_str().unwrap_or_default().to_owned();
        if former != seen {
            eprintln!(
                "  db1 {former} at {:?} (extract job {}, rebuild job {}, error {})",
                back.elapsed(),
                ev["extract_job"],
                ev["rebuild_job"],
                ev["recovery_error"]
            );
            seen = former.clone();
        }
        if former == "rebuilt" {
            break ev;
        }
        assert!(
            former != "rejoined",
            "db1 was rejoined though it diverged: {ev}"
        );
        assert!(
            t0.elapsed() < Duration::from_secs(600),
            "db1 not rebuilt in 10 min: {r}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    let rebuilt_after = back.elapsed();
    eprintln!("db1 rebuilt {rebuilt_after:?} after it came back: {ev}");
    // The extract.
    let x = ev["errant"]["extract"].clone();
    assert_eq!(x["verified"], true, "{x}");
    // The monitor's heartbeats on the then-writable db1 may have diverged
    // too (tenant 0's file); the tenant's own are exactly the five.
    assert!(x["transactions"].as_u64().unwrap_or(0) >= 5, "{x}");
    let tenant_file = x["files"]
        .as_array()
        .and_then(|f| f.iter().find(|f| f["tenant"] == 4_140_600_001_u64))
        .cloned()
        .expect("a file for the tenant schema");
    assert_eq!(tenant_file["rows"], 7, "{tenant_file}");
    assert_eq!(tenant_file["transactions"], 5, "{tenant_file}");
    assert_eq!(x["tables"][format!("{SCHEMA}.kv")], 7, "{x}");
    let opened = ctl(
        &d.sprigctl,
        &[
            "cluster",
            "extract-open",
            tenant_file["location"].as_str().expect("location"),
            "--key-file",
            d.work.join("extract.keys").to_str().expect("path"),
        ],
    )
    .expect("extract-open");
    let text = opened
        .as_str()
        .map_or_else(|| opened.to_string(), str::to_owned);
    let lines: Vec<Value> = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("a JSON line"))
        .collect();
    eprintln!("extract: {} lines, e.g. {}", lines.len(), lines[0]);
    assert_eq!(lines.len(), 7);
    let sqls: Vec<&str> = lines.iter().filter_map(|l| l["sql"].as_str()).collect();
    assert!(
        sqls.iter()
            .any(|s| s.contains("VALUES (900001, 'errant-1')")),
        "{sqls:?}"
    );
    assert!(
        sqls.iter().any(|s| s.contains("'it''s errant'")),
        "{sqls:?}"
    );
    assert!(
        sqls.iter()
            .any(|s| s.starts_with("UPDATE") && s.contains("'errant-update'")),
        "{sqls:?}"
    );
    assert!(
        sqls.iter()
            .any(|s| s.starts_with("DELETE") && s.contains("`k` = 8")),
        "{sqls:?}"
    );
    // A wrong key doesn't open it.
    let bad_keys = d.work.join("bad.keys");
    std::fs::write(
        &bad_keys,
        "1 0000000000000000000000000000000000000000000000000000000000000001\n",
    )
    .expect("bad keys");
    assert!(
        ctl(
            &d.sprigctl,
            &[
                "cluster",
                "extract-open",
                tenant_file["location"].as_str().expect("loc"),
                "--key-file",
                bad_keys.to_str().expect("p")
            ]
        )
        .is_err(),
        "a wrong key opens nothing"
    );
    // The rebuild.
    let rj = d
        .job(ev["rebuild_job"].as_str().expect("rebuild job"))
        .await;
    eprintln!("node_rebuild: {}", rj["progress"]);
    assert_eq!(
        rj["progress"]["source"], source,
        "streamed from the other replica, never the writer"
    );
    let bytes = rj["progress"]["bytes"].as_u64().unwrap_or(0);
    let secs = rj["progress"]["seconds"].as_f64().unwrap_or(0.0);
    assert!(
        bytes > 30 * 1024 * 1024,
        "the backup carries the data ({bytes} bytes)"
    );
    let mbps = bytes as f64 / 1_048_576.0 / secs;
    assert!(
        mbps <= RATE_MB as f64 * 1.1,
        "the rate cap holds ({mbps:.1} MB/s)"
    );
    wait_for(
        &db1.root(),
        "SELECT @@read_only",
        "1",
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(
        master_host(&db1.root()).await.as_deref(),
        Some(new.as_str())
    );
    assert_eq!(
        scalar(
            &db1.root(),
            &format!("SELECT COUNT(*) FROM {SCHEMA}.kv WHERE k BETWEEN 900001 AND 900005")
        )
        .await
        .as_deref(),
        Some("0"),
        "the diverged rows are gone from db1"
    );
    assert_eq!(
        scalar(
            &db1.root(),
            &format!("SELECT v FROM {SCHEMA}.kv WHERE k = 7")
        )
        .await
        .as_deref(),
        Some("base-7")
    );
    // db1 follows the writer: the live rows written during the rebuild and after.
    tokio::time::sleep(Duration::from_secs(3)).await;
    stop.store(true, Ordering::SeqCst);
    let _ = wtask.await;
    let n = acked.load(Ordering::SeqCst);
    let want = scalar(
        &newdb.root(),
        &format!("SELECT COUNT(*) FROM {SCHEMA}.kv WHERE k >= 1000000"),
    )
    .await
    .expect("count");
    assert_eq!(
        want,
        n.to_string(),
        "every acknowledged live write is on the writer"
    );
    wait_for(
        &db1.root(),
        &format!("SELECT COUNT(*) FROM {SCHEMA}.kv WHERE k >= 1000000"),
        &want,
        Duration::from_secs(60),
    )
    .await;
    eprintln!("db1 holds all {n} rows written at {new} during and after the rebuild");
    // The old data directory went, since the extract was verified.
    let left = docker(&[
        "exec",
        db1.c.name(),
        "sh",
        "-c",
        "ls -d /var/lib/mysql.pre-rebuild-* 2>/dev/null | wc -l",
    ])
    .unwrap_or_default();
    assert_eq!(left.trim(), "0", "old data directory removed");
    // The ticket is gone.
    assert!(
        get(
            &*d.store,
            &format!(
                "/sprig/dc/{DC}/rebuilds/{}",
                ev["rebuild_job"].as_str().unwrap_or_default()
            )
        )
        .await
        .is_none()
    );

    // ---- an operator's rebuild of a healthy replica, and refusals ----
    let other = by_name[source];
    let t0 = Instant::now();
    let v = ctl(
        &d.sprigctl,
        &["cluster", "rebuild", &cluster, source, "--from", "db1"],
    )
    .expect("rebuild");
    let jid = v["job_id"].as_str().expect("job id").to_owned();
    let j = d.wait_job(&jid, Duration::from_secs(600)).await;
    assert_eq!(j["state"], "succeeded", "{j}");
    eprintln!(
        "operator rebuild of {source} from db1: {:?}, {}",
        t0.elapsed(),
        j["progress"]
    );
    assert_eq!(j["progress"]["source"], "db1");
    assert_eq!(
        master_host(&other.root()).await.as_deref(),
        Some(new.as_str())
    );
    wait_for(
        &other.root(),
        &format!("SELECT COUNT(*) FROM {SCHEMA}.kv WHERE k >= 1000000"),
        &want,
        Duration::from_secs(60),
    )
    .await;
    let v = ctl(&d.sprigctl, &["cluster", "rebuild", &cluster, &new]).expect("submitted");
    let j = d
        .wait_job(v["job_id"].as_str().expect("id"), Duration::from_secs(60))
        .await;
    assert_eq!(j["state"], "failed");
    assert!(
        j["error"]
            .as_str()
            .unwrap_or_default()
            .contains("is the writer"),
        "{j}"
    );
    eprintln!("rebuilding the writer is refused: {}", j["error"]);

    if internal_tls() {
        tls_checks(&d, &cluster).await;
    }

    drop([db1, db2, db3]);
    drop(net);
    d.done();
}

/// RC2: the agents publish `https://` services, plain HTTP gets nothing,
/// a client certificate from another CA gets nothing, and one of the
/// internal CA does (a refused token: 403, never a backup).
async fn tls_checks(d: &Deployment, cluster: &str) {
    use sprig_cluster::failover::internal_tls::{InternalTls, split_url};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let pki = d.work.join("pki");
    let read = |f: &str| std::fs::read(pki.join(f)).expect("pki");
    let mine = InternalTls::from_pems(&read("ca.pem"), &read("node.pem"), &read("node-key.pem"))
        .expect("tls");
    let stranger =
        InternalTls::from_pems(&read("ca.pem"), &read("rogue.pem"), &read("rogue-key.pem"))
            .expect("tls");
    let agents = d.store.list(&key(&format!("/sprig/dc/{DC}/agents/")), sprig_meta::ListOpts::default()).await.expect("agents");
    let mut urls = Vec::new();
    for kv in agents.kvs {
        let v: Value = serde_json::from_slice(&kv.value).unwrap_or_default();
        let body = if v.get("body").is_some() { v["body"].clone() } else { v };
        if body["cluster_id"] == cluster || body["cluster"] == cluster {
            if let Some(u) = body["internal_url"].as_str() {
                urls.push(u.to_owned());
            }
        }
    }
    assert!(!urls.is_empty(), "no agent registrations with a service");
    for url in &urls {
        assert!(url.starts_with("https://"), "{url} isn't TLS");
        let (_, host, port) = split_url(url).expect("url");
        let req = format!(
            "GET /internal/v1/backup?job=x&token=y HTTP/1.1\r\nhost: {host}\r\nconnection: close\r\n\r\n"
        );
        // Plain HTTP: the TLS server answers no HTTP.
        let mut plain = tokio::net::TcpStream::connect((host.as_str(), port)).await.expect("tcp");
        let _ = plain.write_all(req.as_bytes()).await;
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), plain.read_to_end(&mut buf)).await;
        assert!(!buf.starts_with(b"HTTP/1.1"), "plain HTTP got an answer from {url}");
        // A certificate from another CA: refused at the handshake.
        let r = match stranger.connect(&host, port).await {
            Ok(mut t) => {
                let _ = t.write_all(req.as_bytes()).await;
                let mut b = Vec::new();
                let _ = tokio::time::timeout(Duration::from_secs(5), t.read_to_end(&mut b)).await;
                b
            }
            Err(_) => Vec::new(),
        };
        assert!(!r.starts_with(b"HTTP/1.1"), "a foreign certificate got an answer from {url}");
        // The internal CA's: in, and the token still guards the stream.
        let mut t = mine.connect(&host, port).await.expect("mutual TLS");
        t.write_all(req.as_bytes()).await.expect("write");
        let mut b = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), t.read_to_end(&mut b)).await;
        let head = String::from_utf8_lossy(&b[..b.len().min(40)]).to_string();
        assert!(head.starts_with("HTTP/1.1 4"), "{url}: {head}");
        eprintln!("{url}: plain HTTP and a foreign certificate refused; the CA's certificate in, a bad token answered {}", &head[..12.min(head.len())]);
    }
}
