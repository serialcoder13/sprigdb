//! Milestone M0.3 through a running gateway in catalog mode: two tenants on
//! one cluster, logical names, DDL through the catalog and settings in SQL.
//! Start the gateway first (catalog mode creates the tenants acme and
//! globex, users acme.app and globex.app):
//!
//! ```sh
//! deploy/compose/gateway-dev.sh up
//! export SPRIG_TEST_M03_GATEWAY=127.0.0.1:13306,127.0.0.1:13307   # mysql, mariadb persona
//! cargo test -p sprig-gateway --test m03_live -- --nocapture --test-threads 1
//! ```
//!
//! Without the variable every test reports what it needs and passes (or
//! fails under `SPRIG_TEST_STRICT=1`). DDL crash safety is the catalog's own
//! tests' (`sprig-catalog::ddl`); here are the happy paths and refusals.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    missing_docs
)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Row, SslOpts};
use sprig_testkit::{skip, unique_ident};

const PASSWORD: &str = "sprig-dev-app";
const GATEWAY: &str = "SPRIG_TEST_M03_GATEWAY";
const A: &str = "acme.app";
const B: &str = "globex.app";

fn endpoints(test: &str) -> Option<Vec<String>> {
    let Some(v) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
        return None;
    };
    Some(skip::urls(&v))
}

async fn conn(addr: &str, user: &str, db: Option<&str>) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(user))
        .pass(Some(PASSWORD))
        .db_name(db)
        .prefer_socket(false)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true));
    Conn::new(opts)
        .await
        .unwrap_or_else(|e| panic!("connect {user}@{addr}: {e}"))
}

fn errno(e: &mysql_async::Error) -> u16 {
    match e {
        mysql_async::Error::Server(s) => s.code,
        other => panic!("not a server error: {other}"),
    }
}

/// Whether `text` holds a physical schema name (`t<digits>_...`).
fn physical(text: &str) -> bool {
    sprig_sql::names::physical_name_leaks(text.as_bytes())
        .into_iter()
        .next()
        .is_some()
}

async fn strings(c: &mut Conn, sql: &str) -> Vec<String> {
    let rows: Vec<Row> = c.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.into_iter()
        .map(|r| {
            r.unwrap()
                .into_iter()
                .map(|v| match v {
                    mysql_async::Value::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
                    mysql_async::Value::NULL => "NULL".to_owned(),
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[tokio::test]
async fn two_tenants_same_names_own_rows() {
    let Some(eps) = endpoints("two_tenants_same_names_own_rows") else {
        return;
    };
    for addr in &eps {
        let db = unique_ident("m03live");
        let mut a = conn(addr, A, None).await;
        let mut b = conn(addr, B, None).await;
        for (c, tag) in [(&mut a, "acme"), (&mut b, "globex")] {
            c.query_drop(format!("CREATE DATABASE {db}")).await.unwrap();
            c.query_drop(format!("USE {db}")).await.unwrap();
            c.query_drop("CREATE TABLE t (id INT PRIMARY KEY, who VARCHAR(20))")
                .await
                .unwrap();
            c.query_drop(format!("INSERT INTO t VALUES (1, '{tag}')"))
                .await
                .unwrap();
            // Read your own DDL: create then select in the same session.
            assert_eq!(strings(c, "SELECT who FROM t").await, [tag]);
            assert_eq!(
                strings(c, "SELECT DATABASE()").await,
                std::slice::from_ref(&db)
            );
        }
        // Each sees only its own rows, under the same names.
        assert_eq!(
            strings(&mut a, &format!("SELECT who FROM {db}.t")).await,
            ["acme"]
        );
        assert_eq!(
            strings(&mut b, &format!("SELECT who FROM {db}.t")).await,
            ["globex"]
        );
        // SHOW and information_schema hold logical names only.
        for c in [&mut a, &mut b] {
            let dbs = strings(c, "SHOW DATABASES").await;
            assert!(dbs.contains(&db), "{dbs:?}");
            assert!(dbs.iter().all(|d| !physical(d)), "{dbs:?}");
            assert_eq!(strings(c, "SHOW TABLES").await, ["t"]);
            let is = strings(
                c,
                &format!(
                    "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES \
                     WHERE TABLE_SCHEMA = '{db}'"
                ),
            )
            .await;
            assert_eq!(is, [format!("{db}|t")]);
            let users = strings(c, "SELECT CURRENT_USER()").await;
            assert_eq!(users, ["app@%"]);
            // Errors name logical databases.
            let e = c
                .query_drop(format!("SELECT * FROM {db}.nope"))
                .await
                .unwrap_err();
            assert_eq!(errno(&e), 1146);
            assert!(e.to_string().contains(&format!("{db}.nope")), "{e}");
            assert!(!physical(&e.to_string()), "{e}");
        }
        for c in [&mut a, &mut b] {
            c.query_drop(format!("DROP DATABASE {db}")).await.unwrap();
        }
    }
}

#[tokio::test]
async fn physical_and_foreign_names_are_unknown() {
    let Some(eps) = endpoints("physical_and_foreign_names_are_unknown") else {
        return;
    };
    let addr = &eps[0];
    let db = unique_ident("m03vault");
    let mut a = conn(addr, A, None).await;
    let mut b = conn(addr, B, None).await;
    a.query_drop(format!("CREATE DATABASE {db}")).await.unwrap();
    // The other tenant can't see or use it.
    let e = b.query_drop(format!("USE {db}")).await.unwrap_err();
    assert_eq!(errno(&e), 1049);
    // A physical name typed by hand is unknown, and not echoed.
    let e = a.query_drop("USE t1_d1_g1_s1").await.unwrap_err();
    assert_eq!(errno(&e), 1049);
    assert!(!physical(&e.to_string()), "{e}");
    a.query_drop(format!("DROP DATABASE {db}")).await.unwrap();
}

#[tokio::test]
async fn ddl_happy_paths_and_refusals() {
    let Some(eps) = endpoints("ddl_happy_paths_and_refusals") else {
        return;
    };
    let addr = &eps[0];
    let db = unique_ident("m03ddl");
    let mut a = conn(addr, A, None).await;
    a.query_drop(format!("CREATE DATABASE {db}")).await.unwrap();
    a.query_drop(format!("USE {db}")).await.unwrap();
    for sql in [
        "CREATE TABLE p (id INT PRIMARY KEY)",
        "CREATE TABLE c (id INT PRIMARY KEY, p INT, FOREIGN KEY (p) REFERENCES p (id))",
        "CREATE INDEX ix ON c (p)",
        "ALTER TABLE c ADD note VARCHAR(10), RENAME TO c2",
        "RENAME TABLE c2 TO c",
        "CREATE VIEW v AS SELECT * FROM c",
        "CREATE PROCEDURE pr() SELECT COUNT(*) FROM c",
        "CREATE FUNCTION f(x INT) RETURNS INT DETERMINISTIC RETURN x + 1",
        "CREATE TABLE l LIKE p",
        "TRUNCATE TABLE c",
        "DROP VIEW v",
    ] {
        a.query_drop(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_eq!(strings(&mut a, "SELECT f(1)").await, ["2"]);
    // Notes for IF EXISTS / IF NOT EXISTS, from the catalog.
    a.query_drop("DROP TABLE IF EXISTS nope").await.unwrap();
    let w = strings(&mut a, "SHOW WARNINGS").await;
    assert_eq!(w.len(), 1, "{w:?}");
    assert!(w[0].starts_with("Note|1051|"), "{w:?}");
    a.query_drop("ALTER TABLE IF EXISTS nope ADD x INT")
        .await
        .unwrap();
    // Refusals.
    let e = a.query_drop("CREATE TABLE p (id INT)").await.unwrap_err();
    assert_eq!(errno(&e), 1050);
    let e = a
        .query_drop("CREATE TABLE __sprig_x (a INT)")
        .await
        .unwrap_err();
    assert_eq!(errno(&e), 39050);
    let e = a
        .query_drop("CREATE DATABASE t9_d1_g1_s1")
        .await
        .unwrap_err();
    assert_eq!(errno(&e), 39050);
    let e = a.query_drop("CREATE USER bob").await.unwrap_err();
    assert_eq!(errno(&e), 1227);
    // Temporary tables stay on the session's connection.
    a.query_drop("CREATE TEMPORARY TABLE tmp (a INT)")
        .await
        .unwrap();
    a.query_drop("INSERT INTO tmp VALUES (1)").await.unwrap();
    a.query_drop("ALTER TABLE tmp ADD b INT").await.unwrap();
    assert_eq!(strings(&mut a, "SELECT COUNT(*) FROM tmp").await, ["1"]);
    a.query_drop("DROP TEMPORARY TABLE tmp").await.unwrap();
    // A prepared statement of a forwarded query maps its metadata.
    let st = a.prep("SELECT id FROM p WHERE id > ?").await.unwrap();
    assert!(st.columns().iter().all(|c| c.schema_str() == db));
    a.close(st).await.unwrap();
    a.query_drop(format!("DROP DATABASE {db}")).await.unwrap();
    // The session's database is gone with it.
    assert_eq!(strings(&mut a, "SELECT DATABASE()").await, ["NULL"]);
}

#[tokio::test]
async fn settings_at_object_scope_apply_live_and_bounds_hold() {
    let Some(eps) = endpoints("settings_at_object_scope_apply_live_and_bounds_hold") else {
        return;
    };
    let addr = &eps[0];
    let db = unique_ident("m03set");
    let key = "reads.max_replica_lag_ms";
    let mut a = conn(addr, A, None).await;
    let mut observer = conn(addr, A, None).await;
    a.query_drop(format!("CREATE DATABASE {db}")).await.unwrap();
    a.query_drop(format!("CREATE TABLE {db}.hot (a INT)"))
        .await
        .unwrap();
    // The observer sees the new table within a catalog load (another
    // session's DDL is eventual; only a session's own is immediate).
    let read = format!(
        "SELECT VALUE, SOURCE_SCOPE FROM information_schema.SPRIG_SETTINGS \
         WHERE TABLE_SCHEMA = '{db}' AND TABLE_NAME = 'hot' AND SETTING_NAME = '{key}'"
    );
    let start = Instant::now();
    while strings(&mut observer, &read).await != ["1000|default"] {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the table didn't reach the observer"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    a.query_drop(format!("ALTER TABLE {db}.hot SPRIG SET ({key} = 4321)"))
        .await
        .unwrap();
    let start = Instant::now();
    loop {
        if strings(&mut observer, &read).await == ["4321|object"] {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the change didn't reach the second session"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let show = strings(
        &mut observer,
        &format!("SHOW SPRIG SETTINGS LIKE '{key}' FOR TABLE {db}.hot"),
    )
    .await;
    assert_eq!(show.len(), 1);
    assert!(show[0].contains("|4321|object|"), "{show:?}");
    // Outside the hard bounds: 39080, the value stays.
    let e = a
        .query_drop(format!("ALTER TABLE {db}.hot SPRIG SET ({key} = 86400001)"))
        .await
        .unwrap_err();
    assert_eq!(errno(&e), 39080);
    // Database bounds bind the table.
    a.query_drop(format!(
        "ALTER DATABASE {db} SPRIG SET BOUNDS ({key} MAX 5000)"
    ))
    .await
    .unwrap();
    let e = a
        .query_drop(format!("ALTER TABLE {db}.hot SPRIG SET ({key} = 6000)"))
        .await
        .unwrap_err();
    assert_eq!(errno(&e), 39080);
    a.query_drop(format!("ALTER DATABASE {db} SPRIG RESET BOUNDS ({key})"))
        .await
        .unwrap();
    // Unknown keys and operator-only keys.
    let e = a.query_drop("SPRIG SET nope.key = 1").await.unwrap_err();
    assert_eq!(errno(&e), 39081);
    let e = a
        .query_drop("SPRIG SET names.lower_case_table_names = 1")
        .await
        .unwrap_err();
    assert_eq!(errno(&e), 1227);
    a.query_drop(format!("DROP DATABASE {db}")).await.unwrap();
}
