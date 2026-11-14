//! A DDL statement whose client
//! hangs up mid-way no longer holds its table for 15 minutes. Before, the
//! gateway dropped the statement after phase 1 of the DDL engine had
//! marked the table, so every later DDL on it waited `catalog.ddl_wait_ms`
//! and failed with 39071 until the reconciler cleared the job
//! (`catalog.stuck_state_minutes`, 15), as Sequelize's "supports schemas"
//! tests did to `work.Users`.
//!
//! One session holds a metadata lock on the table (an open transaction
//! that read it); a second runs `ALTER TABLE` on it, which waits for the
//! lock, and its client hangs up. Then:
//! 1. with nothing else touching the table, the job is gone within a few
//!    seconds (the engine's janitor), read from the tenant's catalog;
//! 2. again, and this time another `ALTER TABLE` comes at once: it doesn't
//!    fail with 39071 (it resolves the job itself, or the janitor did).
//!
//! ```sh
//! SPRIG_TEST_DDLABANDON_SQL=127.0.0.1:52306 \
//! SPRIG_TEST_DDLABANDON_TENANT=acme SPRIG_TEST_DDLABANDON_TENANT_ID=52306000000 \
//! SPRIG_TEST_DDLABANDON_NODE=127.0.0.1:13301 \
//!   cargo test -p sprig-gateway --test ddl_abandon_live -- --nocapture
//! ```
//!
//! The node is the tenant's catalog cluster, read as `root`.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};

const DB: &str = "bf10abandon";

struct Env {
    sql: (String, u16),
    tenant: String,
    tenant_id: u64,
    node: (String, u16),
}

fn hostport(v: &str) -> (String, u16) {
    let (h, p) = v.rsplit_once(':').expect("host:port");
    (h.to_owned(), p.parse().expect("port"))
}

fn env() -> Option<Env> {
    Some(Env {
        sql: hostport(&std::env::var("SPRIG_TEST_DDLABANDON_SQL").ok()?),
        tenant: std::env::var("SPRIG_TEST_DDLABANDON_TENANT").ok()?,
        tenant_id: std::env::var("SPRIG_TEST_DDLABANDON_TENANT_ID")
            .ok()?
            .parse()
            .ok()?,
        node: hostport(&std::env::var("SPRIG_TEST_DDLABANDON_NODE").ok()?),
    })
}

async fn login(e: &Env, db: Option<&str>) -> Conn {
    let opts = OptsBuilder::default()
        .ip_or_hostname(e.sql.0.clone())
        .tcp_port(e.sql.1)
        .user(Some(format!("{}.app", e.tenant)))
        .pass(Some("sprig-dev-app"))
        .db_name(db)
        .ssl_opts(Some(
            SslOpts::default().with_danger_accept_invalid_certs(true),
        ));
    Conn::new(opts).await.expect("login")
}

/// The tenant's DDL jobs in flight (intent rows), read on the node.
async fn jobs(e: &Env) -> Vec<String> {
    let opts = OptsBuilder::default()
        .ip_or_hostname(e.node.0.clone())
        .tcp_port(e.node.1)
        .user(Some("root"))
        .pass(Some("sprig-dev-root"));
    let mut c = Conn::new(opts).await.expect("root login");
    let rows: Vec<(String, String)> = c
        .query(format!(
            "SELECT setting_key, JSON_UNQUOTE(JSON_EXTRACT(value, '$.ddl')) \
             FROM `t{}_sprig`.settings WHERE setting_key LIKE 'sprig.ddl.%'",
            e.tenant_id
        ))
        .await
        .unwrap();
    c.disconnect().await.unwrap();
    rows.into_iter().map(|(k, d)| format!("{k}: {d}")).collect()
}

/// Starts `ALTER TABLE t ADD <col>` while a transaction holds the table,
/// and hangs up its client while it waits. Returns the holder, still in
/// its transaction.
async fn abandon(e: &Env, col: &str) -> Conn {
    let mut holder = login(e, Some(DB)).await;
    holder.query_drop("BEGIN").await.unwrap();
    let _: Vec<u32> = holder.query("SELECT id FROM t LIMIT 1").await.unwrap();
    let mut alter = login(e, Some(DB)).await;
    let sql = format!("ALTER TABLE t ADD COLUMN {col} INT");
    let run = tokio::spawn(async move {
        // Never answered: the client goes before the lock is free.
        let _ = tokio::time::timeout(Duration::from_secs(2), alter.query_drop(sql)).await;
        // Dropping the connection mid-statement closes the socket.
        drop(alter);
    });
    run.await.unwrap();
    // The gateway sees the hang-up and drops the statement.
    tokio::time::sleep(Duration::from_millis(300)).await;
    holder
}

#[tokio::test]
async fn a_ddl_whose_client_hangs_up_does_not_hold_its_table() {
    let Some(e) = env() else {
        eprintln!("skipped: set SPRIG_TEST_DDLABANDON_SQL, _TENANT, _TENANT_ID and _NODE");
        return;
    };
    let mut c = login(&e, None).await;
    c.query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await
        .unwrap();
    c.query_drop(format!("CREATE DATABASE {DB}")).await.unwrap();
    c.query_drop(format!("USE {DB}")).await.unwrap();
    c.query_drop("CREATE TABLE t (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO t VALUES (1)").await.unwrap();
    assert!(jobs(&e).await.is_empty(), "no job before");

    // 1. Nobody else touches the table: the job still goes.
    let mut holder = abandon(&e, "a1").await;
    let left = jobs(&e).await;
    println!("after the hang-up: {left:?}");
    assert_eq!(left.len(), 1, "the hung-up ALTER left its job: {left:?}");
    holder.query_drop("COMMIT").await.unwrap();
    let t = Instant::now();
    let gone = loop {
        if jobs(&e).await.is_empty() {
            break true;
        }
        if t.elapsed() > Duration::from_secs(20) {
            break false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    println!("job gone after {:?} with no other DDL", t.elapsed());
    assert!(
        gone,
        "the abandoned job is still there: {:?}",
        jobs(&e).await
    );
    drop(holder);

    // 2. The next ALTER comes at once.
    let mut holder = abandon(&e, "a2").await;
    holder.query_drop("COMMIT").await.unwrap();
    let t = Instant::now();
    let r = c.query_drop("ALTER TABLE t ADD COLUMN b2 INT").await;
    println!("the next ALTER: {r:?} after {:?}", t.elapsed());
    r.expect("the next ALTER doesn't fail with 39071");
    assert!(
        t.elapsed() < Duration::from_secs(8),
        "it didn't wait out catalog.ddl_wait_ms: {:?}",
        t.elapsed()
    );
    assert!(jobs(&e).await.is_empty());
    let cols: Vec<String> = c
        .query(format!(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = '{DB}' AND table_name = 't' ORDER BY ordinal_position"
        ))
        .await
        .unwrap();
    println!("columns: {cols:?}");
    assert!(cols.contains(&"b2".to_owned()));
    drop(holder);
    c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
}
