//! Deleting a tenant leaves nothing behind, against a running deployment: its controller, a worker and a gateway.
//!
//! ```sh
//! SPRIG_TEST_TENANTDEL_SPRIGCTL=target/debug/sprigctl \
//! SPRIG_TEST_TENANTDEL_ADMIN=http://127.0.0.1:26180 \
//! SPRIG_TEST_TENANTDEL_GATEWAY=127.0.0.1:26106 \
//! SPRIG_TEST_TENANTDEL_BACKENDS=mysql://root:<pw>@127.0.0.1:13301,mysql://root:<pw>@127.0.0.1:13311,mysql://root:<pw>@127.0.0.1:13321 \
//! SPRIG_TEST_TENANTDEL_ETCD=http://127.0.0.1:26179 \
//! SPRIG_TEST_TENANTDEL_ID_BASE=4710600000 \
//!   cargo test -p sprig-controller --test live_tenant_delete -- --nocapture
//! ```
//!
//! The deployment's clusters `dc-a-c1` and `dc-a-c2` must be the first two
//! backends. The tenant `tenantdel-<tag>` gets twelve users (so ids 1 and
//! 10..12 exist), a database with a table, a view, a procedure, a function
//! and an event, table, column and routine grants, and a grant kept by name
//! after its table is dropped (`auth.grants_on_drop = keep`). It is then
//! moved from `dc-a-c1` to `dc-a-c2` (the old copy is retained), used on
//! `dc-a-c2`, and deleted with one database still live. Afterwards every
//! backend is checked by exact names for accounts, schemas, grants in
//! `mysql.*`, routines, events, `__sprig_names` rows and the move's
//! `__sprig_sys` rows, and the DC tier for the tenant's keys and job
//! records.
//!
//! The second case is the one `gateway-dev.sh down` left half done: a tenant moved twice
//! (`dc-a-c1` to `dc-a-c2`, whose retention the test then ends in the job
//! record, then to `dc-a-c3`, the old copy kept; it needs `ETCD`) whose database also has shard schemas on the two clusters it
//! left (made here directly, as a sharded group's shards would be). With
//! its databases dropped, the deletion must finish within 20 seconds (it
//! took 30 before: the cleanup waited as a blocked key while the move job
//! dropped the old copy) and leave nothing on any of the three backends
//! (`dc-a-c3` must be the third) or in etcd. `tenant show` failures seen
//! meanwhile are reported: a failed call never means the tenant is gone.

#![allow(
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::match_same_arms
)]

use std::process::Command;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, SslOpts};
use serde_json::Value;
use sprig_common::ids::JobId;
use sprig_common::ids::TenantId;
use sprig_common::names::{AccountName, SchemaName};
use sprig_common::time::SystemClock;
use sprig_meta::{EtcdConfig, EtcdStore, ListOpts, MetaStore, Tier};
use sprig_mover::JobStore;
use std::sync::Arc;

const PREFIX: &str = "SPRIG_TEST_TENANTDEL_";
const APP_PW: &str = "sprig-tenantdel-app";

fn env(name: &str) -> Option<String> {
    std::env::var(format!("{PREFIX}{name}"))
        .ok()
        .filter(|v| !v.trim().is_empty())
}

struct Ctl {
    bin: String,
    admin: String,
}

impl Ctl {
    fn run(&self, args: &[&str]) -> Result<Value, String> {
        let out = Command::new(&self.bin)
            .args(["--endpoint", &self.admin, "-o", "json"])
            .args(args)
            .output()
            .map_err(|e| format!("sprigctl: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "sprigctl {args:?}: {}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ));
        }
        let text = String::from_utf8_lossy(&out.stdout);
        Ok(serde_json::from_str(&text).unwrap_or(Value::String(text.into_owned())))
    }
}

async fn gateway(addr: &str, login: &str) -> Conn {
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    let o: Opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().expect("port"))
        .user(Some(login))
        .pass(Some(APP_PW))
        .ssl_opts(
            SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        )
        .into();
    Conn::new(o).await.expect("gateway login")
}

/// Exact names of tenant `t` found on a backend.
#[derive(Debug, Default)]
struct Leftovers {
    accounts: Vec<String>,
    schemas: Vec<String>,
    names: Vec<String>,
    grants: Vec<String>,
    routines: Vec<String>,
    job_rows: u64,
}

impl Leftovers {
    fn is_empty(&self) -> bool {
        self.accounts.is_empty()
            && self.schemas.is_empty()
            && self.names.is_empty()
            && self.grants.is_empty()
            && self.routines.is_empty()
            && self.job_rows == 0
    }
}

async fn leftovers(url: &str, t: TenantId, jobs: &[String]) -> Leftovers {
    let mut c = Conn::new(Opts::from_url(url).expect("url"))
        .await
        .expect("backend");
    let like = format!("t{t}\\_%");
    let mine_acct = |u: &String| u.parse::<AccountName>().is_ok_and(|a| a.tenant() == t);
    let mine_schema = |s: &String| s.parse::<SchemaName>().is_ok_and(|n| n.tenant() == t);
    let users: Vec<String> = c
        .exec(
            "SELECT User FROM mysql.global_priv WHERE User LIKE ?",
            (&like,),
        )
        .await
        .unwrap();
    let schemas: Vec<String> = c
        .exec(
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE ?",
            (&like,),
        )
        .await
        .unwrap();
    let names: Vec<String> = c
        .exec(
            "SELECT name FROM __sprig_sys.__sprig_names WHERE name LIKE ?",
            (&like,),
        )
        .await
        .unwrap_or_default();
    let mut grants = Vec::new();
    for table in ["db", "tables_priv", "columns_priv", "procs_priv"] {
        // Small tables: read whole, matched exactly here.
        let rows: Vec<(String, String)> = c
            .query(format!("SELECT User, Db FROM mysql.{table}"))
            .await
            .unwrap();
        for (u, d) in rows {
            let wildcard = d == format!("t{t}\\_d%");
            if mine_acct(&u) || mine_schema(&d) || wildcard {
                grants.push(format!("{table}: {u} on {d}"));
            }
        }
    }
    let routines: Vec<(String, String, String)> = c
        .exec(
            "SELECT 'proc', db, definer FROM mysql.proc WHERE db LIKE ? OR definer LIKE ? \
             UNION ALL SELECT 'event', db, definer FROM mysql.event WHERE db LIKE ? OR definer LIKE ?",
            (&like, &like, &like, &like),
        )
        .await
        .unwrap();
    let mut job_rows = 0;
    for j in jobs {
        let hex = j.replace('-', "");
        for table in ["__sprig_mover_pos", "__sprig_watermark", "__sprig_mover_xa"] {
            let n: Option<u64> = c
                .query_first(format!(
                    "SELECT COUNT(*) FROM __sprig_sys.{table} WHERE job_id = X'{hex}'"
                ))
                .await
                .unwrap_or(Some(0));
            job_rows += n.unwrap_or(0);
        }
    }
    let _ = c.disconnect().await;
    Leftovers {
        accounts: users.into_iter().filter(|u| mine_acct(u)).collect(),
        schemas: schemas.into_iter().filter(|s| mine_schema(s)).collect(),
        names: names
            .into_iter()
            .filter(|n| mine_acct(n) || mine_schema(n))
            .collect(),
        grants,
        routines: routines
            .into_iter()
            .filter(|(_, d, def)| {
                mine_schema(d)
                    || def
                        .split('@')
                        .next()
                        .is_some_and(|u| mine_acct(&u.to_owned()))
            })
            .map(|(k, d, def)| format!("{k} in {d} by {def}"))
            .collect(),
        job_rows,
    }
}

/// The tenant's keys left in `tier`.
async fn keys_left(store: &dyn MetaStore, t: TenantId, name: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut opts = ListOpts::default();
    let root = store.tier().root();
    loop {
        let page = store.list(&root, opts.clone()).await.unwrap();
        for kv in &page.kvs {
            let k = kv.key.as_str();
            let rel = k.strip_prefix(root.as_str()).unwrap_or(k);
            let parts: Vec<&str> = rel.split('/').collect();
            let id = t.to_string();
            let mine = match parts.as_slice() {
                ["tenants" | "pins" | "promotions", x] => *x == id,
                ["tenant-names", n] => *n == name,
                ["shardmaps", x, ..]
                | ["feed", _, x, ..]
                | ["logic", "schedules", x, ..]
                | ["mover", "shard-locks", x, ..] => *x == id,
                ["mover", "jobs", _] => {
                    serde_json::from_slice::<Value>(&kv.value)
                        .ok()
                        .and_then(|v| v["tenant_id"].as_u64())
                        == Some(t.0)
                }
                _ => false,
            };
            if mine {
                out.push(k.to_owned());
            }
        }
        if !page.more {
            return out;
        }
        opts.at = Some(page.revision);
        opts.after = page.kvs.last().map(|kv| kv.key.clone());
    }
}

#[tokio::test]
async fn deleting_a_tenant_leaves_nothing_behind() {
    let (Some(bin), Some(admin), Some(gw), Some(backends)) = (
        env("SPRIGCTL"),
        env("ADMIN"),
        env("GATEWAY"),
        env("BACKENDS"),
    ) else {
        eprintln!("skipped: set {PREFIX}SPRIGCTL, ADMIN, GATEWAY and BACKENDS to run it");
        return;
    };
    let backends: Vec<String> = backends.split(',').map(str::to_owned).collect();
    let ctl = Ctl { bin, admin };
    let tag = format!(
        "{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            & 0xff_ffff
    );
    let name = format!("tenantdel-{tag}");
    let created = ctl
        .run(&["tenant", "create", &name, "--cluster", "dc-a-c1"])
        .unwrap();
    let t = TenantId(created["tenant"]["tenant_id"].as_u64().expect("tenant id"));
    eprintln!("tenant {name} is {t}");
    // A private deployment's ids stay in its own range. The
    // controller raises ids past every tenant schema on the clusters, so a
    // range below another deployment's would be left: undo and stop.
    if let Some(base) = env("ID_BASE").and_then(|b| b.parse::<u64>().ok())
        && !(base..base + 100_000).contains(&t.0)
    {
        let _ = ctl.run(&["tenant", "delete", &name, "--yes"]);
        panic!(
            "tenant id {t} is outside {base}..{}: pick a higher base",
            base + 100_000
        );
    }
    ctl.run(&["tenant", "user", "add", &name, "app", "--password", APP_PW])
        .unwrap();
    let login = format!("{name}.app");
    let mut c = gateway(&gw, &login).await;
    for n in 2..=12 {
        c.query_drop(format!("CREATE USER u{n} IDENTIFIED BY '{APP_PW}'"))
            .await
            .unwrap();
    }
    for s in [
        "CREATE DATABASE shop",
        "CREATE DATABASE kept",
        "CREATE TABLE shop.t (id INT PRIMARY KEY, v INT)",
        "INSERT INTO shop.t VALUES (1, 1), (2, 2)",
        "CREATE TABLE kept.k (id INT PRIMARY KEY)",
        "CREATE VIEW shop.v AS SELECT id FROM shop.t",
        "CREATE PROCEDURE shop.p() SELECT COUNT(*) FROM shop.t",
        "CREATE FUNCTION shop.f(x INT) RETURNS INT DETERMINISTIC RETURN x + 1",
        "CREATE EVENT shop.e ON SCHEDULE EVERY 1 DAY DISABLE DO DELETE FROM shop.t WHERE id < 0",
        "GRANT SELECT ON shop.t TO u2",
        "GRANT SELECT (v) ON shop.t TO u10",
        "GRANT EXECUTE ON PROCEDURE shop.p TO u11",
        "GRANT ALL ON kept.* TO u12",
        "CREATE TABLE shop.gone (id INT PRIMARY KEY)",
        "GRANT CREATE, SELECT ON shop.gone TO u3",
        "DROP TABLE shop.gone",
    ] {
        c.query_drop(s).await.unwrap_or_else(|e| panic!("{s}: {e}"));
    }
    let _ = c.disconnect().await;
    // A move: the old copy stays on dc-a-c1 (mover.source_retain_hours).
    // Polled here rather than with --wait, which gives up on the first
    // slow etcd read.
    let moved = ctl
        .run(&["shard", "move", &name, "--to", "dc-a-c2"])
        .unwrap();
    eprintln!("move: {moved}");
    let started = Instant::now();
    let job_ids: Vec<String> = loop {
        if let Ok(jobs) = ctl.run(&["job", "list", "--tenant", &name]) {
            let list = jobs["jobs"].as_array().cloned().unwrap_or_default();
            if list.iter().any(|j| j["state"] == "retaining") {
                eprintln!("jobs: {jobs}");
                break list
                    .iter()
                    .filter_map(|j| j["id"].as_str().map(str::to_owned))
                    .collect();
            }
            assert!(
                !list.iter().any(|j| j["state"] == "rolled_back"),
                "the move rolled back: {jobs}"
            );
        }
        assert!(
            started.elapsed() < Duration::from_secs(300),
            "no committed move after 5 minutes"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    };
    // Used on dc-a-c2, so its accounts are made there too.
    let mut c = gateway(&gw, &login).await;
    let n: Option<u64> = c.query_first("SELECT COUNT(*) FROM shop.t").await.unwrap();
    assert_eq!(n, Some(2));
    c.query_drop("DROP DATABASE shop").await.unwrap();
    let _ = c.disconnect().await;
    let before: Vec<Leftovers> = futures_join(&backends, t, &job_ids).await;
    for (b, l) in backends.iter().zip(&before) {
        eprintln!(
            "before, {}: {} accounts, {} schemas, {} name rows, {} grant rows, {} routines",
            b.rsplit('@').next().unwrap_or(b),
            l.accounts.len(),
            l.schemas.len(),
            l.names.len(),
            l.grants.len(),
            l.routines.len()
        );
    }
    assert!(before[0].accounts.len() >= 12, "{:?}", before[0].accounts);
    // On dc-a-c2 only the users holding grants on a live schema have one.
    assert!(!before[1].accounts.is_empty(), "{:?}", before[1].accounts);
    assert!(
        before[0].schemas.contains(&format!("t{t}_sprig")),
        "the retained copy is on dc-a-c1: {:?}",
        before[0].schemas
    );
    // Delete with `kept` still live: the catalog reconciler drops it.
    let started = Instant::now();
    ctl.run(&["tenant", "delete", &name, "--yes"]).unwrap();
    loop {
        // Gone from the list (a failed call proves nothing).
        if let Ok(list) = ctl.run(&["tenant", "list"])
            && !list["tenants"]
                .as_array()
                .is_some_and(|a| a.iter().any(|x| x["name"] == name.as_str()))
        {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(600),
            "tenant {name} still there after 10 minutes"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    eprintln!("deleted in {:?}", started.elapsed());
    let after = futures_join(&backends, t, &job_ids).await;
    for (b, l) in backends.iter().zip(&after) {
        assert!(l.is_empty(), "left on {b}: {l:?}");
    }
    if let Some(etcd) = env("ETCD") {
        let cfg = EtcdConfig::new(vec![etcd]);
        for tier in [Tier::Global, Tier::Dc("dc-a".parse().unwrap())] {
            let store = EtcdStore::connect(tier.clone(), &cfg).await.unwrap();
            let left = keys_left(&store, t, &name).await;
            assert!(left.is_empty(), "keys left in {tier:?}: {left:?}");
        }
    }
    eprintln!(
        "nothing of {name} ({t}) is left on {} backends",
        backends.len()
    );
}

async fn futures_join(backends: &[String], t: TenantId, jobs: &[String]) -> Vec<Leftovers> {
    let mut out = Vec::new();
    for b in backends {
        out.push(leftovers(b, t, jobs).await);
    }
    out
}

/// Polls `job list --tenant` until a job of `name` is in `want` (or
/// fails the test after 5 minutes, or if one rolled back).
async fn wait_job(ctl: &Ctl, name: &str, want: &str) -> Vec<String> {
    let started = Instant::now();
    loop {
        if let Ok(jobs) = ctl.run(&["job", "list", "--tenant", name]) {
            let list = jobs["jobs"].as_array().cloned().unwrap_or_default();
            assert!(
                !list.iter().any(|j| j["state"] == "rolled_back"),
                "a move rolled back: {jobs}"
            );
            if list.iter().any(|j| j["state"] == want) {
                eprintln!("jobs: {jobs}");
                return list
                    .iter()
                    .filter_map(|j| j["id"].as_str().map(str::to_owned))
                    .collect();
            }
        }
        assert!(
            started.elapsed() < Duration::from_secs(300),
            "no {want} job after 5 minutes"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[tokio::test]
async fn a_tenant_moved_twice_and_spread_over_clusters_is_deleted_within_seconds() {
    let (Some(bin), Some(admin), Some(gw), Some(backends)) = (
        env("SPRIGCTL"),
        env("ADMIN"),
        env("GATEWAY"),
        env("BACKENDS"),
    ) else {
        eprintln!("skipped: set {PREFIX}SPRIGCTL, ADMIN, GATEWAY and BACKENDS to run it");
        return;
    };
    let backends: Vec<String> = backends.split(',').map(str::to_owned).collect();
    assert!(backends.len() >= 3, "needs dc-a-c1, dc-a-c2 and dc-a-c3");
    let ctl = Ctl { bin, admin };
    let tag = format!(
        "{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            & 0xff_ffff
    );
    let name = format!("tenantdel2-{tag}");
    let created = ctl
        .run(&["tenant", "create", &name, "--cluster", "dc-a-c1"])
        .unwrap();
    let t = TenantId(created["tenant"]["tenant_id"].as_u64().expect("tenant id"));
    eprintln!("tenant {name} is {t}");
    if let Some(base) = env("ID_BASE").and_then(|b| b.parse::<u64>().ok())
        && !(base..base + 100_000).contains(&t.0)
    {
        let _ = ctl.run(&["tenant", "delete", &name, "--yes"]);
        panic!(
            "tenant id {t} is outside {base}..{}: pick a higher base",
            base + 100_000
        );
    }
    ctl.run(&["tenant", "user", "add", &name, "app", "--password", APP_PW])
        .unwrap();
    let login = format!("{name}.app");
    let mut c = gateway(&gw, &login).await;
    for s in [
        "CREATE USER u2 IDENTIFIED BY 'sprig-tenantdel-app'",
        "CREATE USER u3 IDENTIFIED BY 'sprig-tenantdel-app'",
        "CREATE DATABASE shop",
        "CREATE TABLE shop.t (id INT PRIMARY KEY, v INT)",
        "INSERT INTO shop.t VALUES (1, 1), (2, 2), (3, 3)",
        "GRANT SELECT ON shop.t TO u2",
    ] {
        c.query_drop(s).await.unwrap_or_else(|e| panic!("{s}: {e}"));
    }
    let _ = c.disconnect().await;
    // First move; its retention is then ended in the job record (as if
    // 24 hours had passed), so the job drops the old copy and finishes.
    // The tenant's accounts stay on dc-a-c1.
    let etcd = env("ETCD").expect("this case needs SPRIG_TEST_TENANTDEL_ETCD");
    let cfg = EtcdConfig::new(vec![etcd]);
    let dc: Arc<dyn MetaStore> = Arc::new(
        EtcdStore::connect(Tier::Dc("dc-a".parse().unwrap()), &cfg)
            .await
            .unwrap(),
    );
    ctl.run(&["shard", "move", &name, "--to", "dc-a-c2"])
        .unwrap();
    let first = wait_job(&ctl, &name, "retaining").await;
    let store = JobStore::new(Arc::clone(&dc), Arc::new(SystemClock));
    let id: JobId = serde_json::from_value(Value::String(first[0].clone())).unwrap();
    loop {
        let (mut rec, rev) = store.get(&id).await.unwrap();
        rec.retain_until_ms = Some(store.now_ms());
        if store.put_if(&rec, rev).await.unwrap().is_some() {
            break;
        }
    }
    wait_job(&ctl, &name, "done").await;
    // Used on dc-a-c2, then moved on to dc-a-c3, the old copy kept.
    let mut c = gateway(&gw, &login).await;
    let n: Option<u64> = c.query_first("SELECT COUNT(*) FROM shop.t").await.unwrap();
    assert_eq!(n, Some(3));
    let _ = c.disconnect().await;
    ctl.run(&["shard", "move", &name, "--to", "dc-a-c3"])
        .unwrap();
    let jobs = wait_job(&ctl, &name, "retaining").await;
    let mut c = gateway(&gw, &login).await;
    let n: Option<u64> = c.query_first("SELECT COUNT(*) FROM shop.t").await.unwrap();
    assert_eq!(n, Some(3));
    let _ = c.disconnect().await;
    // Shards of shop on the clusters it left, as a sharded group's.
    let db: u32 = {
        let mut r = Conn::new(Opts::from_url(&backends[2]).unwrap())
            .await
            .unwrap();
        let id: Option<u32> = r
            .query_first(format!(
                "SELECT db_id FROM `t{t}_sprig`.`databases` WHERE name = 'shop'"
            ))
            .await
            .unwrap();
        let _ = r.disconnect().await;
        id.expect("shop's id")
    };
    for (i, url) in backends[..2].iter().enumerate() {
        let mut r = Conn::new(Opts::from_url(url).unwrap()).await.unwrap();
        let schema = format!("t{t}_d{db}_g90{}_s{}", i + 1, i + 1);
        r.query_drop(format!("CREATE DATABASE `{schema}`"))
            .await
            .unwrap();
        r.query_drop(format!(
            "CREATE TABLE `{schema}`.t (id INT PRIMARY KEY, v INT)"
        ))
        .await
        .unwrap();
        let _ = r.disconnect().await;
    }
    let mut c = gateway(&gw, &login).await;
    c.query_drop("DROP DATABASE shop").await.unwrap();
    let _ = c.disconnect().await;
    let before = futures_join(&backends, t, &jobs).await;
    for (b, l) in backends.iter().zip(&before) {
        eprintln!(
            "before, {}: {} accounts, schemas {:?}, {} name rows",
            b.rsplit('@').next().unwrap_or(b),
            l.accounts.len(),
            l.schemas,
            l.names.len()
        );
    }
    assert!(!before[0].accounts.is_empty(), "first move's accounts");
    assert!(
        before[1].schemas.contains(&format!("t{t}_sprig")),
        "the retained copy is on dc-a-c2: {:?}",
        before[1].schemas
    );
    assert!(before[2].schemas.contains(&format!("t{t}_sprig")));
    let started = Instant::now();
    ctl.run(&["tenant", "delete", &name, "--yes"]).unwrap();
    let mut show_failures = 0;
    let took = loop {
        if ctl.run(&["tenant", "show", &name]).is_err() {
            show_failures += 1;
        }
        if let Ok(list) = ctl.run(&["tenant", "list"])
            && !list["tenants"]
                .as_array()
                .is_some_and(|a| a.iter().any(|x| x["name"] == name.as_str()))
        {
            break started.elapsed();
        }
        assert!(
            started.elapsed() < Duration::from_secs(300),
            "tenant {name} still there after 5 minutes"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    eprintln!("deleted in {took:?} ({show_failures} tenant show calls failed meanwhile)");
    let after = futures_join(&backends, t, &jobs).await;
    for (b, l) in backends.iter().zip(&after) {
        assert!(l.is_empty(), "left on {b}: {l:?}");
    }
    for tier in [Tier::Global, Tier::Dc("dc-a".parse().unwrap())] {
        let store = EtcdStore::connect(tier.clone(), &cfg).await.unwrap();
        let left = keys_left(&store, t, &name).await;
        assert!(left.is_empty(), "keys left in {tier:?}: {left:?}");
    }
    assert!(
        took < Duration::from_secs(20),
        "the deletion took {took:?}: longer than gateway-dev.sh down used to wait"
    );
    eprintln!(
        "nothing of {name} ({t}) is left on {} backends",
        backends.len()
    );
}
