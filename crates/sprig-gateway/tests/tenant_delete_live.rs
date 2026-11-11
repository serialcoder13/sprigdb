//! bugfix-27 and bugfix-29 (decisions/bugfix-interim.md): deleting a
//! tenant that still has databases, streams, queues, sinks and inherited
//! instances finishes on its own, also when the worker is killed in the
//! middle, and the gateway's backend connections of the deleted tenants
//! close.
//!
//! - A declarer with `FOR EACH TENANT` streams and queues, a member with
//!   an instance database, and a plain tenant with a database, a table, a
//!   stream, a change sink and a stream sink, an AMQP queue with a message,
//!   and two users who ran statements. Nobody drops a database.
//! - The tenants are deleted (member first) while the worker is killed and
//!   restarted (`SPRIG_TEST_TENANT_DELETE_KILL`, a shell command).
//! - Each must be gone from `tenant list` within
//!   `SPRIG_TEST_TENANT_DELETE_SECS` (default 120). Before, cleanup refused
//!   while the catalog listed databases, and only the catalog reconciler's
//!   next pass (every 5 minutes) dropped them; an instance database held
//!   by an unfinished creation was never dropped.
//! - No `t<id>_` schema of them is left on the node, and the node has no
//!   connection of their accounts within 15 s of the deletion (before, the
//!   gateway kept an idle connection per user until
//!   `backend.idle_timeout_seconds`, 300 s).
//!
//! ```sh
//! SPRIG_TEST_TENANT_DELETE=<mysql port>,<admin port>,<amqp port> \
//! SPRIG_TEST_TENANT_DELETE_PREFIX=<unique tenant name prefix> \
//! SPRIG_TEST_TENANT_DELETE_SPRIGCTL=<path to sprigctl> \
//! SPRIG_TEST_TENANT_DELETE_NODE=13301 \
//! SPRIG_TEST_TENANT_DELETE_KILL='kill -9 $(cat .../worker.pid); sleep 5; .../start-worker' \
//!   cargo test -p sprig-gateway --test tenant_delete_live -- --nocapture
//! ```

#![allow(
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs
)]

use std::collections::BTreeMap;
use std::process::Command;
use std::time::{Duration, Instant};

use lapin::options::{BasicPublishOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use serde_json::Value;

const PW: &str = "Bf7-delete-pw1";

struct Env {
    mysql: u16,
    admin: u16,
    amqp: u16,
    prefix: String,
    sprigctl: String,
    node: u16,
    kill: Option<String>,
    secs: u64,
}

fn env() -> Option<Env> {
    let v = std::env::var("SPRIG_TEST_TENANT_DELETE").ok()?;
    let p: Vec<u16> = v.split(',').filter_map(|x| x.trim().parse().ok()).collect();
    let [mysql, admin, amqp] = p.as_slice() else {
        return None;
    };
    Some(Env {
        mysql: *mysql,
        admin: *admin,
        amqp: *amqp,
        prefix: std::env::var("SPRIG_TEST_TENANT_DELETE_PREFIX")
            .unwrap_or_else(|_| "tdel".to_owned()),
        sprigctl: std::env::var("SPRIG_TEST_TENANT_DELETE_SPRIGCTL").ok()?,
        node: std::env::var("SPRIG_TEST_TENANT_DELETE_NODE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(13301),
        kill: std::env::var("SPRIG_TEST_TENANT_DELETE_KILL").ok(),
        secs: std::env::var("SPRIG_TEST_TENANT_DELETE_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(120),
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
        .env("SPRIGCTL_ACTOR", "tenant_delete_live")
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

async fn login(e: &Env, tenant: &str, user: &str, db: Option<&str>) -> Conn {
    let opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(e.mysql)
        .user(Some(format!("{tenant}.{user}")))
        .pass(Some(PW))
        .db_name(db)
        .ssl_opts(
            SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        );
    let t0 = Instant::now();
    loop {
        match Conn::new(opts.clone()).await {
            Ok(c) => return c,
            Err(x) if t0.elapsed() < Duration::from_secs(60) => {
                eprintln!("{tenant}.{user}: {x}; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(x) => panic!("{tenant}.{user}: {x}"),
        }
    }
}

async fn ok(c: &mut Conn, q: &str) {
    if let Err(x) = c.query_drop(q).await {
        panic!("{q}: {x}");
    }
}

fn root_opts(port: u16) -> OptsBuilder {
    let pw =
        std::env::var("SPRIG_TEST_ROOT_PASSWORD").unwrap_or_else(|_| "sprig-dev-root".to_owned());
    OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some(pw))
}

/// The node's connections and schemas of tenant `id`.
async fn left_on_node(e: &Env, id: u64) -> (u64, Vec<String>) {
    let mut c = Conn::new(root_opts(e.node)).await.expect("root login");
    let like = format!("t{id}\\_%");
    let conns: Option<u64> = c
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE USER LIKE ?",
            (&like,),
        )
        .await
        .expect("processlist");
    let schemas: Vec<String> = c
        .exec(
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE ?",
            (&like,),
        )
        .await
        .expect("schemata");
    c.disconnect().await.ok();
    (conns.unwrap_or(0), schemas)
}

fn tenant_id(e: &Env, name: &str) -> u64 {
    ctl(e, &["tenant", "show", name])
        .ok()
        .and_then(|v| v["tenant_id"].as_u64())
        .unwrap_or(0)
}

#[tokio::test]
async fn deleted_tenants_finish_and_close_their_connections() {
    let Some(e) = env() else {
        eprintln!("skipped: set SPRIG_TEST_TENANT_DELETE and SPRIG_TEST_TENANT_DELETE_SPRIGCTL");
        return;
    };
    let decl = format!("{}-decl", e.prefix);
    let mem = format!("{}-mem", e.prefix);
    let plain = format!("{}-plain", e.prefix);
    ctl(&e, &["tenant", "create", &decl, "--cluster", "dc-a-c1"]).expect("declarer");
    ctl(
        &e,
        &[
            "tenant",
            "create",
            &mem,
            "--under",
            &decl,
            "--cluster",
            "dc-a-c1",
        ],
    )
    .expect("member");
    ctl(&e, &["tenant", "create", &plain, "--cluster", "dc-a-c1"]).expect("plain");
    let mut ids = BTreeMap::new();
    for t in [&decl, &mem, &plain] {
        for u in ["app", "two"] {
            ctl(
                &e,
                &[
                    "tenant",
                    "user",
                    "add",
                    t,
                    u,
                    "--password",
                    PW,
                    "--if-not-exists",
                ],
            )
            .expect("user");
        }
        let id = tenant_id(&e, t);
        eprintln!("tenant {t}: id {id}");
        ids.insert(t.clone(), id);
    }

    // The declarer's inherited stream and queue; the member's instance.
    let mut d = login(&e, &decl, "app", None).await;
    ok(&mut d, "CREATE DATABASE ix").await;
    ok(&mut d, "CREATE STREAM ix.ev FOR EACH TENANT PARTITIONS 1").await;
    ok(&mut d, "CREATE QUEUE ix.q FOR EACH TENANT").await;
    ok(
        &mut d,
        &format!("INSERT INTO ix.ev FOR TENANT `{mem}` (key, value) VALUES ('k', 'v')"),
    )
    .await;
    ok(&mut d, "GRANT ALL ON *.* TO two").await;
    let mut d2 = login(&e, &decl, "two", None).await;
    ok(&mut d2, "SELECT 1 FROM DUAL").await;
    let mut m = login(&e, &mem, "app", None).await;
    let shown: Vec<mysql_async::Row> = m.query("SHOW INHERITED INSTANCES").await.unwrap();
    eprintln!("member instances: {}", shown.len());
    ok(&mut m, "GRANT ALL ON *.* TO two").await;
    let mut m2 = login(&e, &mem, "two", None).await;
    ok(&mut m2, "SHOW DATABASES").await;

    // The plain tenant: a table, a stream, sinks, a queue with a message.
    let mut p = login(&e, &plain, "app", None).await;
    for q in [
        "GRANT SPRIG_SINK_ADMIN ON *.* TO app",
        "GRANT ALL ON *.* TO two",
        "CREATE DATABASE dd",
        "SPRIG SET (sink.destination_allowlist = '[{\"host\": \"127.0.0.1:9\"}]')",
    ] {
        ok(&mut p, q).await;
    }
    drop(p);
    let mut p = login(&e, &plain, "app", Some("dd")).await;
    for q in [
        "CREATE TABLE orders (id INT PRIMARY KEY, v TEXT)",
        "ALTER TABLE orders SPRIG SET (stream.change_topic = on)",
        "CREATE STREAM ev PARTITIONS 1",
        "CREATE CONNECTION hook TYPE webhook WITH (url = 'http://127.0.0.1:9/h')",
        "CREATE SINK s1 FROM STREAM ev TO WEBHOOK CONNECTION hook",
        "CREATE SINK s2 FROM CHANGES OF orders TO WEBHOOK CONNECTION hook",
        "INSERT INTO orders VALUES (1, 'a'), (2, 'b')",
    ] {
        ok(&mut p, q).await;
    }
    let mut p2 = login(&e, &plain, "two", Some("dd")).await;
    ok(&mut p2, "SELECT COUNT(*) FROM orders").await;
    {
        let c = Connection::connect(
            &format!("amqp://{plain}.app:{PW}@127.0.0.1:{}/dd", e.amqp),
            ConnectionProperties::default(),
        )
        .await
        .expect("amqp");
        let ch = c.create_channel().await.expect("channel");
        ch.queue_declare(
            "jobs".into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("declare");
        ch.basic_publish(
            "".into(),
            "jobs".into(),
            BasicPublishOptions::default(),
            b"hello",
            BasicProperties::default(),
        )
        .await
        .expect("publish")
        .await
        .expect("confirm");
        c.close(200, "bye".into()).await.ok();
    }
    for c in [d, d2, m, m2, p, p2] {
        c.disconnect().await.ok();
    }
    for (t, id) in &ids {
        let (conns, schemas) = left_on_node(&e, *id).await;
        eprintln!("{t} before deletion: {conns} connections, schemas {schemas:?}");
    }

    // Delete, member first, with the worker killed and restarted.
    let t0 = Instant::now();
    ctl(&e, &["tenant", "delete", &mem, "--yes"]).expect("delete member");
    let killer = e.kill.clone().map(|k| {
        std::thread::spawn(move || {
            let s = Command::new("bash").args(["-c", &k]).status();
            eprintln!("worker kill and restart: {s:?}");
        })
    });
    let mut failed = Vec::new();
    let wait_gone = |name: String| {
        let e = &e;
        async move {
            let t = Instant::now();
            loop {
                let listed = ctl(e, &["tenant", "list"]).ok().is_some_and(|v| {
                    v["tenants"]
                        .as_array()
                        .is_some_and(|a| a.iter().any(|x| x["name"] == name.as_str()))
                });
                if !listed {
                    return Some(t.elapsed());
                }
                if t.elapsed() > Duration::from_secs(e.secs) {
                    return None;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    };
    // Within 10 s of a tenant's going, the node has no connection of its
    // accounts (the gateway's idle timeout is 300 s).
    let closed = |name: String| {
        let e = &e;
        let id = ids[&name];
        async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            let (conns, _) = left_on_node(e, id).await;
            eprintln!("{name}: {conns} connections 10 s after it went");
            (conns > 0).then(|| format!("{name}: {conns} connections 10 s after its deletion"))
        }
    };
    match wait_gone(mem.clone()).await {
        Some(d) => {
            eprintln!("{mem} gone after {d:?}");
            failed.extend(closed(mem.clone()).await);
        }
        None => failed.push(format!("{mem} still listed after {} s", e.secs)),
    }
    for t in [&decl, &plain] {
        ctl(&e, &["tenant", "delete", t, "--yes"]).expect("delete");
    }
    for t in [&plain, &decl] {
        match wait_gone(t.clone()).await {
            Some(d) => {
                eprintln!("{t} gone after {d:?}");
                failed.extend(closed(t.clone()).await);
            }
            None => failed.push(format!("{t} still listed after {} s", e.secs)),
        }
    }
    if let Some(k) = killer {
        let _ = k.join();
    }
    eprintln!("all deletions: {:?}", t0.elapsed());

    // Nothing of them on the node; their connections closed.
    tokio::time::sleep(Duration::from_secs(15)).await;
    for (t, id) in &ids {
        let (conns, schemas) = left_on_node(&e, *id).await;
        eprintln!("{t} after deletion: {conns} connections, schemas {schemas:?}");
        if conns > 0 {
            failed.push(format!("{t}: {conns} connections of its accounts left"));
        }
        if !schemas.is_empty() {
            failed.push(format!("{t}: schemas left {schemas:?}"));
        }
    }
    assert!(failed.is_empty(), "{failed:#?}");
}
