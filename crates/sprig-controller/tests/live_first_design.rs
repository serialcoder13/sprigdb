//! Regression: a tenant the sharding loop's own first design sharded
//! answers through the gateway.
//!
//! The loop's first design (its regroup) moves every table of a
//! group out of its database's first shard schema into the group's own
//! schemas. Text statements were routed there, but `COM_STMT_PREPARE` went
//! to the database's first schema, where the table no longer is, so every
//! prepared statement of the tenant got 1146 (`Table 'la.cust' doesn't
//! exist`). Runs against a deployment `deploy/compose/gateway-dev.sh up`
//! started (its own etcd, its own tenant-id range):
//!
//! ```sh
//! SPRIG_TEST_FIRST_DESIGN=<gateway mysql port>,<admin port> \
//! SPRIG_TEST_FIRST_DESIGN_TENANT=<a new tenant name> \
//!   cargo test -p sprig-controller --test live_first_design -- --nocapture
//! ```
//!
//! The test creates the tenant on `dc-a-c1`, gives it three tables in one
//! database (`cust`, `ord` with a foreign key to it, `la`), sets its own
//! `sharding.min_shards = 2`, `advisor.mode = off` and
//! `advisor.min_ndv_per_shard = 0` (as `live_r2harden_bins.rs`'s tenant G), waits for the
//! loop's design and its split, then runs prepared and text statements on
//! fresh connections: none may fail. It deletes the tenant at the end.

#![allow(clippy::too_many_lines, clippy::doc_markdown)]

use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, Pool, PoolConstraints, PoolOpts, SslOpts};
use serde_json::Value;

const APP_PW: &str = "sprig-first-design-app";

struct Env {
    mysql: u16,
    admin: u16,
    tenant: String,
    sprigctl: PathBuf,
}

fn env() -> Option<Env> {
    let v = std::env::var("SPRIG_TEST_FIRST_DESIGN").ok()?;
    let (m, a) = v.split_once(',')?;
    let target = std::env::var("CARGO_TARGET_DIR").map_or_else(
        |_| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../target")),
        PathBuf::from,
    );
    let sprigctl = target.join("debug").join("sprigctl");
    if !sprigctl.exists() {
        eprintln!("skipped: build sprigctl first ({})", sprigctl.display());
        return None;
    }
    Some(Env {
        mysql: m.trim().parse().ok()?,
        admin: a.trim().parse().ok()?,
        tenant: std::env::var("SPRIG_TEST_FIRST_DESIGN_TENANT")
            .unwrap_or_else(|_| "firstdesign".to_owned()),
        sprigctl,
    })
}

fn ctl(e: &Env, args: &[&str]) -> Result<Value, String> {
    let out = Command::new(&e.sprigctl)
        .args([
            "--endpoint",
            &format!("http://127.0.0.1:{}", e.admin),
            "-o",
            "json",
        ])
        .args(args)
        .env("SPRIGCTL_ACTOR", "live_first_design")
        .output()
        .map_err(|x| x.to_string())?;
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    if !out.status.success() {
        return Err(format!(
            "sprigctl {args:?}: {text}{}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
}

async fn ctl_retry(e: &Env, args: &[&str]) -> Value {
    let t0 = Instant::now();
    loop {
        match ctl(e, args) {
            Ok(v) => return v,
            Err(x) if t0.elapsed() < Duration::from_secs(60) => {
                eprintln!("retrying: {x}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(x) => panic!("{x}"),
        }
    }
}

fn setting(e: &Env, k: &str, v: &str) -> Vec<String> {
    vec![
        "settings".into(),
        "set".into(),
        k.into(),
        v.into(),
        "--scope".into(),
        format!("tenant:{}", e.tenant),
        "--reason".into(),
        "live_first_design".into(),
        "--yes".into(),
    ]
}

fn pool(e: &Env, db: Option<&str>) -> Pool {
    Pool::new(
        OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(e.mysql)
            .user(Some(format!("{}.app", e.tenant)))
            .pass(Some(APP_PW))
            .ssl_opts(
                SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true),
            )
            .db_name(db)
            .pool_opts(
                PoolOpts::new()
                    .with_constraints(PoolConstraints::new(0, 2).expect("constraints"))
                    .with_inactive_connection_ttl(Duration::from_secs(60)),
            ),
    )
}

/// The first `tenant_id` anywhere in `v` (the created tenant's).
fn tenant_id(v: &Value) -> Value {
    match v {
        Value::Object(m) => m.get("tenant_id").cloned().unwrap_or_else(|| {
            m.values()
                .map(tenant_id)
                .find(|x| !x.is_null())
                .unwrap_or(Value::Null)
        }),
        _ => Value::Null,
    }
}

/// The `cust` group's id and shards, from `sprigctl shard list`.
fn shards(e: &Env) -> Option<(u64, Vec<u64>)> {
    let l = ctl(e, &["shard", "list", &e.tenant]).ok()?;
    let g = l
        .as_array()?
        .iter()
        .find(|g| g["name"].as_str().is_some_and(|n| n.contains("cust")))?;
    Some((
        g["group"].as_u64()?,
        g["shards"]
            .as_array()?
            .iter()
            .filter_map(|s| s["shard"].as_u64())
            .collect(),
    ))
}

#[tokio::test]
async fn first_design_tables_answer_prepared_and_text_statements() {
    let Some(e) = env() else {
        eprintln!("skipped: set SPRIG_TEST_FIRST_DESIGN=<gateway mysql port>,<admin port>");
        return;
    };
    let created = ctl_retry(&e, &["tenant", "create", &e.tenant, "--cluster", "dc-a-c1"]).await;
    eprintln!("tenant {} created: id {}", e.tenant, tenant_id(&created));
    ctl_retry(
        &e,
        &[
            "tenant",
            "user",
            "add",
            &e.tenant,
            "app",
            "--password",
            APP_PW,
            "--if-not-exists",
        ],
    )
    .await;
    // The schema, with rows, before the settings that shard it.
    let t0 = Instant::now();
    loop {
        let pl = pool(&e, None);
        let r = async {
            let mut c = pl.get_conn().await?;
            for q in [
                "CREATE DATABASE IF NOT EXISTS la",
                "CREATE TABLE IF NOT EXISTS la.la (k INT PRIMARY KEY, v TEXT NOT NULL) ENGINE=InnoDB",
                "CREATE TABLE IF NOT EXISTS la.cust (id BIGINT PRIMARY KEY, v TEXT NOT NULL) ENGINE=InnoDB",
                "CREATE TABLE IF NOT EXISTS la.ord (id BIGINT PRIMARY KEY, cust_id BIGINT NOT NULL, v TEXT, \
                 FOREIGN KEY (cust_id) REFERENCES cust(id)) ENGINE=InnoDB",
                "INSERT IGNORE INTO la.la (k, v) VALUES (100, '0')",
                "INSERT IGNORE INTO la.cust (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                "INSERT IGNORE INTO la.ord (id, cust_id, v) VALUES (10, 1, 'x'), (20, 2, 'y')",
            ] {
                c.query_drop(q).await?;
            }
            drop(c);
            Ok::<_, mysql_async::Error>(())
        }
        .await;
        pl.disconnect().await.ok();
        match r {
            Ok(()) => break,
            Err(x) if t0.elapsed() < Duration::from_secs(60) => {
                eprintln!("schema: {x}; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(x) => panic!("schema: {x}"),
        }
    }
    for (k, v) in [
        ("advisor.mode", "off"),
        ("advisor.min_ndv_per_shard", "0"),
        ("sharding.min_shards", "2"),
    ] {
        let a = setting(&e, k, v);
        let a: Vec<&str> = a.iter().map(String::as_str).collect();
        ctl_retry(&e, &a).await;
    }
    let set_at = Instant::now();
    let (gid, sh) = loop {
        if let Some((g, s)) = shards(&e)
            && s.len() >= 2
        {
            break (g, s);
        }
        assert!(
            set_at.elapsed() < Duration::from_secs(300),
            "the loop never sharded the tenant: {:?}",
            ctl(&e, &["shard", "list", &e.tenant])
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    };
    eprintln!(
        "group {gid} has shards {sh:?} {:?} after the settings",
        set_at.elapsed()
    );
    // Fresh connections, prepared (binary protocol) and text statements.
    let pl = pool(&e, Some("la"));
    let mut errors: Vec<String> = Vec::new();
    let mut c = pl.get_conn().await.expect("conn");
    for id in 1000..1040u64 {
        let v = format!("v{id}");
        if let Err(x) = c
            .exec_drop(
                "INSERT INTO cust (id, v) VALUES (?, ?) ON DUPLICATE KEY UPDATE v = CONCAT(v, ',', VALUES(v))",
                (id, &v),
            )
            .await
        {
            errors.push(format!("prepared insert {id}: {x}"));
            continue;
        }
        match c
            .exec_first::<String, _, _>("SELECT v FROM cust WHERE id = ?", (id,))
            .await
        {
            Ok(Some(got)) if got == v => {}
            other => errors.push(format!("prepared select {id}: {other:?}")),
        }
        if let Err(x) = c
            .exec_drop(
                "INSERT INTO ord (id, cust_id, v) VALUES (?, ?, ?)",
                (id * 10, id, "o"),
            )
            .await
        {
            errors.push(format!("prepared child insert {id}: {x}"));
        }
    }
    match c
        .exec_first::<u64, _, _>(
            "SELECT COUNT(*) FROM cust c JOIN ord o ON o.cust_id = c.id WHERE c.id = ?",
            (1000u64,),
        )
        .await
    {
        Ok(Some(1)) => {}
        other => errors.push(format!("prepared join: {other:?}")),
    }
    match c.query_first::<u64, _>("SELECT COUNT(*) FROM cust").await {
        Ok(Some(43)) => {}
        other => errors.push(format!("text count: {other:?}")),
    }
    match c
        .exec_first::<u64, _, _>("SELECT k FROM la.la WHERE k = ?", (100u64,))
        .await
    {
        Ok(Some(100)) => {}
        other => errors.push(format!("prepared qualified select: {other:?}")),
    }
    drop(c);
    pl.disconnect().await.ok();
    eprintln!("errors: {} {errors:?}", errors.len());
    // Cleanup: the tenant this test created, by name.
    {
        let pl = pool(&e, None);
        if let Ok(mut c) = pl.get_conn().await {
            let _ = c.query_drop("DROP DATABASE IF EXISTS la").await;
        }
        pl.disconnect().await.ok();
    }
    if let Err(x) = ctl(&e, &["tenant", "delete", &e.tenant, "--yes"]) {
        eprintln!("tenant delete: {x}");
    }
    assert!(errors.is_empty(), "{errors:?}");
}
