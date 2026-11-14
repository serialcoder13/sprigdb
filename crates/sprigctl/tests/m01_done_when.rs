//! Cluster registration and capability profiles, end to end against the Compose
//! stack (`deploy/compose`, `docker compose --profile all up -d`):
//!
//! 1. `sprigctl cluster register` accepts the Galera cluster, the primary
//!    with replicas and the single node;
//! 2. each gets a correct capability profile;
//! 3. the profile changes when semi-sync is switched off on the primary,
//!    and back when it is switched on again.
//!
//! A controller runs in this process against the Compose etcd, in a DC of
//! its own (`m01-<hex>`), serving the Admin API on a free loopback port;
//! the `sprigctl` binary talks to it. Settings are written to etcd while it
//! runs: `cluster.health_interval_seconds = 1` before it starts and
//! `cluster.capability_upgrade_sustain_seconds = 3` after, which also
//! checks that settings apply live.
//!
//! Semi-sync is restored to its original value whatever happens (a failed
//! assertion included); the etcd keys this test wrote are deleted. Run it
//! with:
//!
//! ```sh
//! cargo test -p sprigctl --test m01_done_when -- --ignored --nocapture
//! ```
//!
//! Variables: `SPRIG_TEST_ETCD` (default `http://127.0.0.1:2379`),
//! `MARIADB_ROOT_PASSWORD` (default `sprig-dev-root`) and
//! `SPRIG_TEST_HOST` (default `127.0.0.1`); the ports are Compose's.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    missing_docs
)]

use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mysql_async::prelude::Queryable;
use serde_json::Value;
use sprig_common::ids::DcId;
use sprig_common::telemetry::ProcessState;
use sprig_common::time::SystemClock;
use sprig_common::{CancellationToken, Health, ProcessConfig, RoleContext};
use sprig_controller::runtime::Stores;
use sprig_meta::{Consistency, EtcdConfig, EtcdStore, ListOpts, MetaStore, Op, Tier, Txn};

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

/// Runs `sprigctl` on a blocking thread (never on a runtime worker, which
/// would stall the in-process controller) and returns its exit status and
/// stdout.
async fn sprigctl(endpoint: &str, args: &[&str]) -> (bool, String) {
    let endpoint = endpoint.to_owned();
    let args: Vec<String> = args.iter().map(|a| (*a).to_owned()).collect();
    tokio::task::spawn_blocking(move || {
        let out = Command::new(env!("CARGO_BIN_EXE_sprigctl"))
            .arg("--endpoint")
            .arg(&endpoint)
            .args(&args)
            .output()
            .expect("sprigctl runs");
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&out.stderr);
        if !stderr.is_empty() {
            eprintln!("sprigctl {}: {stderr}", args.join(" "));
        }
        (out.status.success(), stdout)
    })
    .await
    .expect("sprigctl thread")
}

async fn sprigctl_json(endpoint: &str, args: &[&str]) -> Value {
    let mut a = vec!["-o", "json"];
    a.extend_from_slice(args);
    let (ok, out) = sprigctl(endpoint, &a).await;
    assert!(ok, "sprigctl {} failed: {out}", args.join(" "));
    serde_json::from_str(&out).expect("JSON output")
}

async fn put(store: &dyn MetaStore, rel: &str, json: &str) {
    store
        .txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key: store.tier().key(rel).unwrap(),
                value: json.as_bytes().to_vec(),
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .unwrap();
}

/// Deletes every key under `rel` (relative to the store's tier; empty for
/// the whole tier) whose value `del` accepts.
async fn delete_prefix(store: &dyn MetaStore, rel: &str, del: impl Fn(&[u8]) -> bool) -> usize {
    let prefix = if rel.is_empty() {
        store.tier().root()
    } else {
        store.tier().key(rel).unwrap()
    };
    let mut keys = Vec::new();
    let mut opts = ListOpts::default();
    loop {
        let page = store.list(&prefix, opts.clone()).await.unwrap();
        opts.at = Some(page.revision);
        opts.after = page.kvs.last().map(|kv| kv.key.clone());
        keys.extend(
            page.kvs
                .into_iter()
                .filter(|kv| del(&kv.value))
                .map(|kv| kv.key),
        );
        if !page.more {
            break;
        }
    }
    for k in &keys {
        store
            .txn(Txn {
                when: vec![],
                then: vec![Op::Delete(k.clone())],
                otherwise: vec![],
            })
            .await
            .unwrap();
    }
    keys.len()
}

/// `rpl_semi_sync_master_enabled` on the primary.
async fn semi_sync(url: &str) -> bool {
    let mut c = mysql_async::Conn::new(mysql_async::Opts::from_url(url).unwrap())
        .await
        .unwrap();
    let v: Option<String> = c
        .query_first("SELECT @@GLOBAL.rpl_semi_sync_master_enabled")
        .await
        .unwrap();
    let _ = c.disconnect().await;
    v.as_deref() == Some("1")
}

async fn set_semi_sync(url: &str, on: bool) -> Result<(), String> {
    let mut c =
        mysql_async::Conn::new(mysql_async::Opts::from_url(url).map_err(|e| e.to_string())?)
            .await
            .map_err(|e| e.to_string())?;
    let r = c
        .query_drop(format!(
            "SET GLOBAL rpl_semi_sync_master_enabled = {}",
            if on { "ON" } else { "OFF" }
        ))
        .await
        .map_err(|e| e.to_string());
    let _ = c.disconnect().await;
    r
}

/// Polls `show` until `pred` holds; returns the cluster's JSON.
async fn wait_profile(
    endpoint: &str,
    id: &str,
    what: &str,
    secs: u64,
    pred: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let v = sprigctl_json(endpoint, &["cluster", "show", id]).await;
        if pred(&v) {
            return v;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}: {v:#}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

fn summary(v: &Value) -> String {
    let p = &v["profile"];
    let d = &p["derived"];
    format!(
        "{} {} v{}: health {}, version {}, galera {}, semi_sync {{enabled {}, active {}, wait {}, clients {}}}, \
         xa {}, zero_loss_failover {}, expected_loss {}, ryw {}, mover {}/{}, feed {} ({} s), \
         fence {}, validation {}, online_index_build {}, enforce_json_schema {}, compiled_logic {}",
        v["cluster_id"].as_str().unwrap_or("?"),
        v["topology"].as_str().unwrap_or("?"),
        v["profile_version"],
        v["health"]["state"],
        p["server"]["version_text"],
        if p["galera"].is_null() {
            "-".to_owned()
        } else {
            format!("{}/{} synced", p["galera"]["synced"], p["galera"]["size"])
        },
        p["semi_sync"]["enabled"],
        p["semi_sync"]["active"],
        p["semi_sync"]["wait_point"],
        p["semi_sync"]["clients"],
        d["xa"],
        d["zero_loss_failover"],
        d["expected_loss"],
        d["ryw"],
        d["mover_source"],
        d["mover_target"],
        d["feed_level"],
        d["feed_retention_secs"],
        d["fence_methods"],
        d["validation"],
        d["online_index_build"],
        d["enforce_json_schema"],
        d["compiled_logic"],
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the Compose stack: docker compose --profile all up -d"]
async fn m01_done_when() {
    let host = env("SPRIG_TEST_HOST", "127.0.0.1");
    let password = env("MARIADB_ROOT_PASSWORD", "sprig-dev-root");
    let primary_url = format!("mysql://root:{password}@{host}:13311");
    let etcd = env("SPRIG_TEST_ETCD", "http://127.0.0.1:2379");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let tag = format!("{:06x}", (nanos ^ std::process::id()) & 0xff_ffff);
    let dc = DcId::new(format!("m01-{tag}")).unwrap();
    let dir = std::env::temp_dir().join(format!("sprig-m01-{tag}"));
    std::fs::create_dir_all(&dir).unwrap();
    let creds = dir.join("creds.toml");
    std::fs::write(
        &creds,
        format!(
            "[sprig_ctl]\nuser = \"root\"\npassword = \"{password}\"\ntls = \"skip_verify\"\n\
             [sprig_repl]\nuser = \"root\"\npassword = \"{password}\"\ntls = \"skip_verify\"\n"
        ),
    )
    .unwrap();

    let dc_store = Arc::new(
        EtcdStore::connect(Tier::Dc(dc.clone()), &EtcdConfig::new(vec![etcd.clone()]))
            .await
            .expect("etcd is up"),
    );
    let global = Arc::new(dc_store.with_tier(Tier::Global));
    // Join an existing deployment, or bootstrap one of our own.
    let dep_key = Tier::Global.key("settings/system.deployment_id").unwrap();
    let existing = global
        .get(&dep_key, Consistency::Linearizable)
        .await
        .unwrap()
        .map(|kv| {
            serde_json::from_slice::<Value>(&kv.value).unwrap()["value"]
                .as_str()
                .unwrap()
                .to_owned()
        });
    let (deployment, bootstrap) = match existing {
        Some(d) => (d, false),
        None => (format!("m01-test-{tag}"), true),
    };
    let originally_on = semi_sync(&primary_url).await;
    println!(
        "dc {dc}, deployment {deployment} (bootstrap {bootstrap}), semi-sync on: {originally_on}"
    );

    // Settings before the controller starts: probe every second.
    put(
        &*dc_store,
        "settings/cluster.health_interval_seconds",
        r#"{"v":1,"value":1,"set_by":"m01-test","reason":"fast probes"}"#,
    )
    .await;

    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let endpoint = format!("http://127.0.0.1:{port}");
    let cfg = ProcessConfig::from_toml_and_env(
        &format!(
            "[node]\ndc = \"{dc}\"\nid = \"m01-ctl-{tag}\"\n[meta]\ndeployment_id = \"{deployment}\"\n\
             [controller]\nbootstrap = {bootstrap}\nadmin_addr = \"127.0.0.1:{port}\"\n"
        ),
        Vec::new(),
    )
    .unwrap();
    let ctx = RoleContext {
        config: cfg,
        health: Health::default(),
        shutdown: CancellationToken::new(),
    };
    let (stop, health) = (ctx.shutdown.clone(), ctx.health.clone());
    let stores = Stores {
        dc: dc_store.clone(),
        global: global.clone(),
    };
    let controller = tokio::spawn(sprig_controller::serve(ctx, stores, Arc::new(SystemClock)));

    let body = {
        let endpoint = endpoint.clone();
        let creds = format!("file:{}", creds.display());
        let dc = dc.to_string();
        let dc_store = dc_store.clone();
        let primary_url = primary_url.clone();
        tokio::spawn(async move {
            let t0 = Instant::now();
            while health.state() != ProcessState::Ready {
                assert!(
                    t0.elapsed() < Duration::from_secs(30),
                    "controller not ready"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            // The DC record exists after a bootstrap; register it otherwise.
            let (ok, _) = sprigctl(&endpoint, &["dc", "show", &dc]).await;
            if !ok {
                let (ok, out) = sprigctl(
                    &endpoint,
                    &["dc", "register", &dc, "--tags", "purpose=m01-test"],
                )
                .await;
                assert!(ok, "dc register: {out}");
            }
            // Wait for leadership: register needs the DC leader.
            loop {
                let v = sprigctl_json(&endpoint, &["dc", "show", &dc]).await;
                if !v["leader"].is_null() {
                    break;
                }
                assert!(t0.elapsed() < Duration::from_secs(30), "no leader");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }

            let clusters = [
                ("single", "single", vec!["13301"]),
                (
                    "replication",
                    "primary_replica",
                    vec!["13311", "13312", "13313"],
                ),
                ("galera", "galera", vec!["13321", "13322", "13323"]),
            ];
            let mut ids = Vec::new();
            for (name, topology, ports) in &clusters {
                let name = format!("{dc}-{name}");
                let mut args = vec![
                    "cluster",
                    "register",
                    name.as_str(),
                    "--topology",
                    topology,
                    "--credentials",
                    creds.as_str(),
                ];
                let nodes: Vec<String> = ports
                    .iter()
                    .map(|p| format!("{host}:{p}", host = "127.0.0.1"))
                    .collect();
                for n in &nodes {
                    args.push("--node");
                    args.push(n);
                }
                let started = Instant::now();
                let v = sprigctl_json(&endpoint, &args).await;
                assert!(
                    v["precheck"]["blocking"].as_array().unwrap().is_empty(),
                    "{v:#}"
                );
                let c = &v["cluster"];
                assert_eq!(c["admin_state"], "active", "{v:#}");
                println!(
                    "registered {} in {:?}; advisories: {}",
                    c["cluster_id"],
                    started.elapsed(),
                    v["precheck"]["advisories"]
                );
                ids.push(c["cluster_id"].as_str().unwrap().to_owned());
            }
            let (ok, list) = sprigctl(&endpoint, &["cluster", "list"]).await;
            assert!(ok);
            println!("{list}");

            // Health: the loop's heartbeats give the replicas a fresh lag.
            for id in &ids {
                wait_profile(&endpoint, id, "a healthy cluster", 30, |v| {
                    v["health"]["state"] == "healthy"
                })
                .await;
            }

            // Profiles.
            let single = sprigctl_json(&endpoint, &["cluster", "show", &ids[0]]).await;
            let repl = sprigctl_json(&endpoint, &["cluster", "show", &ids[1]]).await;
            let galera = sprigctl_json(&endpoint, &["cluster", "show", &ids[2]]).await;
            for v in [&single, &repl, &galera] {
                println!("{}", summary(v));
                let p = &v["profile"];
                assert!(p["server"]["version"]["major"].as_u64().unwrap() >= 11);
                assert_eq!(p["binlog"]["log_bin"], true);
                assert_eq!(p["derived"]["mover_source"], true);
                assert_eq!(p["derived"]["mover_target"], true);
                assert_eq!(p["derived"]["feed_source"], true);
                assert_eq!(p["derived"]["enforce_json_schema"], true);
                assert_eq!(
                    p["derived"]["fence_methods"],
                    serde_json::json!(["revoke_kill", "rename"])
                );
            }
            let d = |v: &Value, k: &str| v["profile"]["derived"][k].clone();
            assert_eq!(d(&single, "xa"), true);
            assert_eq!(d(&single, "zero_loss_failover"), false);
            assert_eq!(d(&single, "expected_loss"), "no_replica");
            assert_eq!(d(&single, "ryw"), "writer_only");
            assert_eq!(d(&repl, "xa"), true);
            assert_eq!(d(&repl, "zero_loss_failover"), true);
            assert_eq!(d(&repl, "expected_loss"), "zero");
            assert_eq!(d(&repl, "ryw"), "gtid_wait");
            assert_eq!(d(&repl, "validation"), "replica_build");
            assert_eq!(repl["profile"]["semi_sync"]["wait_point"], "AFTER_SYNC");
            assert_eq!(d(&galera, "xa"), false);
            assert_eq!(d(&galera, "zero_loss_failover"), true);
            assert_eq!(d(&galera, "ryw"), "wsrep_sync_wait");
            assert_eq!(d(&galera, "validation"), "galera_rsu");
            assert_eq!(d(&galera, "online_index_build"), false);
            assert_eq!(galera["profile"]["galera"]["synced"], 3);
            let ws = wait_profile(&endpoint, &ids[2], "the Galera writer set", 20, |v| {
                v["galera"]["writers"]
                    .as_array()
                    .is_some_and(|w| w.len() == 3)
            })
            .await;
            println!("galera writers: {}", ws["galera"]);

            // Upgrades are held 3 s from now on: a live dc setting.
            put(
                &*dc_store,
                "settings/cluster.capability_upgrade_sustain_seconds",
                r#"{"v":1,"value":3,"set_by":"m01-test","reason":"short hold"}"#,
            )
            .await;

            // Semi-sync off: a downgrade, published within a light probe.
            let before = repl["profile_version"].as_u64().unwrap();
            set_semi_sync(&primary_url, false).await.unwrap();
            let t = Instant::now();
            let off = wait_profile(&endpoint, &ids[1], "the downgrade", 30, |v| {
                v["profile"]["derived"]["zero_loss_failover"] == false
            })
            .await;
            println!("semi-sync off, after {:?}: {}", t.elapsed(), summary(&off));
            assert_eq!(off["profile"]["semi_sync"]["enabled"], false);
            assert_eq!(off["profile"]["derived"]["expected_loss"], "async");
            assert!(off["profile_version"].as_u64().unwrap() > before);

            // Semi-sync on again: the upgrade waits for the replicas to
            // reconnect and for the 3 s hold.
            set_semi_sync(&primary_url, true).await.unwrap();
            let t = Instant::now();
            let on = wait_profile(&endpoint, &ids[1], "the upgrade", 90, |v| {
                v["profile"]["derived"]["zero_loss_failover"] == true
            })
            .await;
            println!("semi-sync on, after {:?}: {}", t.elapsed(), summary(&on));
            assert!(
                t.elapsed() >= Duration::from_secs(3),
                "the upgrade was held"
            );
            assert_eq!(on["profile"]["semi_sync"]["enabled"], true);

            // A probe on demand, through the API.
            let (ok, out) = sprigctl(&endpoint, &["cluster", "probe", &ids[0], "--full"]).await;
            assert!(ok);
            println!("{out}");
            ids
        })
    };
    let result = body.await;

    // Always: semi-sync back as it was, then the controller and our keys.
    let mut restored = false;
    for _ in 0..10 {
        if set_semi_sync(&primary_url, originally_on).await.is_ok()
            && semi_sync(&primary_url).await == originally_on
        {
            restored = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    println!("semi-sync restored to {originally_on}: {restored}");
    stop.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(15), controller).await;
    let ours = dc.to_string();
    let n_dc = delete_prefix(&*dc_store, "", |_| true).await;
    let n_ids = delete_prefix(&*global, "cluster-ids/", |v| {
        serde_json::from_slice::<Value>(v).is_ok_and(|j| j["dc_id"] == ours.as_str())
    })
    .await;
    let n_dcs = delete_prefix(&*global, &format!("dcs/{dc}"), |_| true).await;
    let mut n_boot = 0;
    if bootstrap {
        for k in ["settings/system.deployment_id", "ids/tenant"] {
            n_boot += delete_prefix(&*global, k, |_| true).await;
        }
    }
    println!(
        "deleted etcd keys: dc tier {n_dc}, cluster ids {n_ids}, dcs {n_dcs}, bootstrap {n_boot}"
    );
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        restored,
        "semi-sync could not be restored; set rpl_semi_sync_master_enabled by hand"
    );
    match result {
        Ok(ids) => println!("done when: passed for {ids:?}"),
        Err(e) => std::panic::resume_unwind(e.into_panic()),
    }
}
