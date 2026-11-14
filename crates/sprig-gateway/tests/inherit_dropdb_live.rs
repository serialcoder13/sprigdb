//! `FOR EACH TENANT` definitions
//! and `DROP DATABASE`, on the declaring side and on a member's.
//!
//! 1. `INCLUDING SELF`: the declarer has its own instance of the
//!    definition, under the same name. Before, `DROP STREAM` in the
//!    declarer named that instance and was refused (39897), so the
//!    definition could never be dropped, and the 39895 guard refused
//!    `DROP DATABASE` of its database for ever. Now `DROP STREAM` drops the
//!    definition, after which `DROP DATABASE` goes through; at the end of
//!    the drop grace (0 hours here) every instance goes, the declarer's
//!    own included, and the names can be used again.
//! 2. A member drops its own database holding its instance of a live
//!    definition. Before, every publish to it failed with 39899 ("isn't
//!    loaded yet") until the definition was dropped. Now the instance goes
//!    with the database and the next publish creates it again.
//!
//! ```sh
//! SPRIG_TEST_INHDROP_GATEWAY=127.0.0.1:52306 \
//! SPRIG_TEST_INHDROP_SYSTEM=bf10sys SPRIG_TEST_INHDROP_MEMBERS=bf10ca,bf10cb \
//!   cargo test -p sprig-gateway --test inherit_dropdb_live -- --nocapture --test-threads 1
//! ```
//!
//! The members are children of the system tenant; every tenant has the
//! user `app` (password `sprig-dev-app`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    missing_docs
)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};

struct Env {
    addr: String,
    system: String,
    members: Vec<String>,
}

fn env() -> Option<Env> {
    let get = |k: &str| std::env::var(k).ok().filter(|s| !s.is_empty());
    Some(Env {
        addr: get("SPRIG_TEST_INHDROP_GATEWAY")?,
        system: get("SPRIG_TEST_INHDROP_SYSTEM")?,
        members: get("SPRIG_TEST_INHDROP_MEMBERS")?
            .split(',')
            .map(str::to_owned)
            .collect(),
    })
}

async fn conn(addr: &str, tenant: &str) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{tenant}.app")))
        .pass(Some("sprig-dev-app"))
        .ssl_opts(Some(
            SslOpts::default().with_danger_accept_invalid_certs(true),
        ));
    Conn::new(opts).await.expect("login")
}

/// A statement's rows as text, or its error number and message.
async fn sql(c: &mut Conn, q: &str) -> Result<Vec<Vec<Option<String>>>, (u16, String)> {
    match c.query::<mysql_async::Row, _>(q).await {
        Ok(rows) => Ok(rows
            .into_iter()
            .map(|r| {
                r.unwrap()
                    .into_iter()
                    .map(|v| match v {
                        mysql_async::Value::NULL => None,
                        mysql_async::Value::Bytes(b) => Some(String::from_utf8_lossy(&b).into()),
                        other => Some(other.as_sql(true)),
                    })
                    .collect()
            })
            .collect()),
        Err(mysql_async::Error::Server(e)) => Err((e.code, e.message)),
        Err(e) => panic!("{q}: {e}"),
    }
}

async fn ok(c: &mut Conn, q: &str) -> Vec<Vec<Option<String>>> {
    sql(c, q).await.unwrap_or_else(|e| panic!("{q}: {e:?}"))
}

/// Waits until `SELECT COUNT(*)` `q` reaches `n` (errors count as 0).
async fn wait_count(c: &mut Conn, q: &str, n: u64, secs: u64) -> u64 {
    let end = Instant::now() + Duration::from_secs(secs);
    loop {
        let got = sql(c, q)
            .await
            .ok()
            .and_then(|r| r.first()?.first()?.clone())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if got >= n || Instant::now() > end {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Whether `SHOW INHERITED DEFINITIONS` lists `db.name`.
async fn declared(c: &mut Conn, db: &str, name: &str) -> bool {
    ok(c, "SHOW INHERITED DEFINITIONS")
        .await
        .iter()
        .any(|r| r[1].as_deref() == Some(db) && r[2].as_deref() == Some(name))
}

/// Drops what an earlier run left: the definition now, then the databases.
async fn clean(e: &Env, sys: &mut Conn, db: &str, stream: &str) {
    let _ = sql(sys, &format!("DROP STREAM {db}.{stream} IMMEDIATE")).await;
    let _ = sql(sys, &format!("DROP DATABASE IF EXISTS {db}")).await;
    for m in &e.members {
        let mut c = conn(&e.addr, m).await;
        let _ = sql(&mut c, &format!("DROP DATABASE IF EXISTS {db}")).await;
    }
}

#[tokio::test]
async fn including_self_definitions_can_be_dropped_with_their_database() {
    let Some(e) = env() else {
        eprintln!("skipped: set SPRIG_TEST_INHDROP_GATEWAY, _SYSTEM and _MEMBERS");
        return;
    };
    let (db, s) = ("bf10self", "selfev");
    let mut sys = conn(&e.addr, &e.system).await;
    clean(&e, &mut sys, db, s).await;
    ok(&mut sys, &format!("CREATE DATABASE {db}")).await;
    ok(
        &mut sys,
        &format!("CREATE STREAM {db}.{s} FOR EACH TENANT INCLUDING SELF PARTITIONS 1"),
    )
    .await;
    ok(
        &mut sys,
        &format!("INSERT INTO {db}.{s} (key, value) VALUES ('k', 'self')"),
    )
    .await;
    let m0 = e.members[0].clone();
    ok(
        &mut sys,
        &format!("INSERT INTO {db}.{s} FOR TENANT {m0} (key, value) VALUES ('k', 'v')"),
    )
    .await;
    let q = format!("SELECT COUNT(*) FROM {db}.{s}");
    let mut c0 = conn(&e.addr, &m0).await;
    assert_eq!(wait_count(&mut c0, &q, 1, 60).await, 1);
    assert_eq!(wait_count(&mut sys, &q, 1, 60).await, 1);

    // The guard: a live definition keeps its database.
    let r = sql(&mut sys, &format!("DROP DATABASE {db}")).await;
    assert_eq!(r.map(|_| ()).unwrap_err().0, 39895);

    // The declarer drops its definition (before: 39897, its own instance).
    ok(
        &mut sys,
        "ALTER TENANT SPRIG SET (inherit.drop_grace_hours = 0)",
    )
    .await;
    let r = sql(&mut sys, &format!("DROP STREAM {db}.{s}")).await;
    println!("DROP STREAM in the declarer: {r:?}");
    r.expect("the declarer drops its INCLUDING SELF definition");
    ok(
        &mut sys,
        "ALTER TENANT SPRIG RESET (inherit.drop_grace_hours)",
    )
    .await;
    ok(&mut sys, &format!("DROP DATABASE {db}")).await;

    // The grace's end (every 30 s): the definition and every instance go.
    let t = Instant::now();
    while declared(&mut sys, db, s).await && t.elapsed() < Duration::from_secs(120) {
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    println!("grace end after {:?}", t.elapsed());
    assert!(
        !declared(&mut sys, db, s).await,
        "the definition is still there"
    );
    let r = sql(&mut c0, &q).await;
    assert!(r.is_err(), "the member's instance went: {r:?}");

    // The names are free again.
    ok(&mut sys, &format!("CREATE DATABASE {db}")).await;
    ok(
        &mut sys,
        &format!("CREATE STREAM {db}.{s} FOR EACH TENANT INCLUDING SELF PARTITIONS 1"),
    )
    .await;
    ok(
        &mut sys,
        &format!("INSERT INTO {db}.{s} FOR TENANT {m0} (key, value) VALUES ('k', 'again')"),
    )
    .await;
    assert_eq!(wait_count(&mut c0, &q, 1, 60).await, 1);
    ok(&mut sys, &format!("DROP STREAM {db}.{s} IMMEDIATE")).await;
    ok(&mut sys, &format!("DROP DATABASE {db}")).await;
    let _ = sql(&mut c0, &format!("DROP DATABASE IF EXISTS {db}")).await;
}

#[tokio::test]
async fn a_member_dropping_its_instance_database_gets_the_instance_back() {
    let Some(e) = env() else {
        eprintln!("skipped: set SPRIG_TEST_INHDROP_GATEWAY, _SYSTEM and _MEMBERS");
        return;
    };
    let (db, s) = ("bf10mem", "memev");
    let mut sys = conn(&e.addr, &e.system).await;
    clean(&e, &mut sys, db, s).await;
    ok(&mut sys, &format!("CREATE DATABASE {db}")).await;
    ok(
        &mut sys,
        &format!("CREATE STREAM {db}.{s} FOR EACH TENANT PARTITIONS 1"),
    )
    .await;
    let m0 = e.members[0].clone();
    let publish = format!("INSERT INTO {db}.{s} FOR TENANT {m0} (key, value) VALUES ('k', 'v')");
    ok(&mut sys, &publish).await;
    let q = format!("SELECT COUNT(*) FROM {db}.{s}");
    let mut c0 = conn(&e.addr, &m0).await;
    assert_eq!(wait_count(&mut c0, &q, 1, 60).await, 1);

    // The member drops its own database, instance and all.
    ok(&mut c0, &format!("DROP DATABASE {db}")).await;

    // The next publish creates the instance again (before: 39899 each
    // time, for as long as the definition lived).
    let t = Instant::now();
    let r = loop {
        let r = sql(&mut sys, &publish).await;
        if r.is_ok() || t.elapsed() > Duration::from_secs(20) {
            break r;
        }
        println!("publish after the member's DROP DATABASE: {r:?}");
        tokio::time::sleep(Duration::from_secs(2)).await;
    };
    r.expect("a publish to the member works again");
    assert_eq!(
        wait_count(&mut c0, &q, 1, 60).await,
        1,
        "the new instance has the publish"
    );

    ok(&mut sys, &format!("DROP STREAM {db}.{s} IMMEDIATE")).await;
    ok(&mut sys, &format!("DROP DATABASE {db}")).await;
    let _ = sql(&mut c0, &format!("DROP DATABASE IF EXISTS {db}")).await;
}
