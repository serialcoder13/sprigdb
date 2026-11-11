//! `TEST CONNECTION` and `TEST SINK` through a live private deployment
//! (chapter 51, "Testing connections and sinks"; R9, r9tests).
//!
//! ```sh
//! export SPRIG_TEST_SINKTEST_GATEWAY=127.0.0.1:38306   # MySQL wire
//! export SPRIG_TEST_SINKTEST_TENANT=r9tqc              # user app holds SPRIG_SINK_ADMIN
//! export SPRIG_TEST_SINKTEST_AMQP=127.0.0.1:38682      # the gateway's AMQP listener
//! export SPRIG_TEST_SINKTEST_PG=127.0.0.1:38432        # a PostgreSQL (user postgres)
//! export SPRIG_TEST_SINKTEST_PG_PASSWORD=...
//! export SPRIG_TEST_SINKTEST_KAFKA=127.0.0.1:38092     # a Kafka broker
//! export SPRIG_TEST_SINKTEST_S3=127.0.0.1:38900        # a MinIO (path style)
//! export SPRIG_TEST_SINKTEST_S3_KEY=... SPRIG_TEST_SINKTEST_S3_SECRET=...
//! export SPRIG_TEST_SINKTEST_HOOK_PORT=38881           # a webhook receiver the test runs
//! export SPRIG_TEST_SINKTEST_CLOSED=127.0.0.1:38999    # allowed, nothing listening
//! cargo test -p sprig-gateway --test sink_test_live -- --nocapture --test-threads 1
//! ```
//!
//! Every address must be on the tenant's `sink.destination_allowlist`.
//! Each test works in a database of its own (`st_<time>`) and drops it.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    missing_docs
)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_SINKTEST_GATEWAY";

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|s| !s.is_empty())
}

fn run_id() -> String {
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("st_{}", ms % 100_000_000)
}

async fn conn(addr: &str, user: &str, password: &str) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(user))
        .pass(Some(password))
        .ssl_opts(Some(
            SslOpts::default().with_danger_accept_invalid_certs(true),
        ));
    Conn::new(opts).await.unwrap_or_else(|e| panic!("login as {user}: {e}"))
}

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

/// `TEST CONNECTION`'s rows as (step, result, detail).
async fn steps(c: &mut Conn, name: &str) -> Vec<(String, String, String)> {
    let rows = ok(c, &format!("TEST CONNECTION {name}")).await;
    println!("TEST CONNECTION {name}:");
    for r in &rows {
        println!("  {:?}", r);
    }
    rows.into_iter()
        .map(|r| {
            (
                r[0].clone().unwrap_or_default(),
                r[1].clone().unwrap_or_default(),
                r[2].clone().unwrap_or_default(),
            )
        })
        .collect()
}

fn step<'a>(s: &'a [(String, String, String)], name: &str) -> &'a (String, String, String) {
    s.iter()
        .find(|x| x.0 == name)
        .unwrap_or_else(|| panic!("no step {name} in {s:?}"))
}

/// A webhook receiver answering 204 to everything; records each request's
/// method.
async fn hook(port: u16) -> Arc<Mutex<Vec<String>>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let seen = Arc::new(Mutex::new(Vec::new()));
    let l = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .unwrap_or_else(|e| panic!("hook port {port}: {e}"));
    let s2 = Arc::clone(&seen);
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = l.accept().await else {
                return;
            };
            let seen = Arc::clone(&s2);
            tokio::spawn(async move {
                let mut buf = vec![0u8; 16 << 10];
                loop {
                    let n = match s.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    let head = String::from_utf8_lossy(&buf[..n]).to_string();
                    let method = head.split(' ').next().unwrap_or("").to_owned();
                    seen.lock().unwrap().push(method);
                    if s
                        .write_all(b"HTTP/1.1 204 No Content\r\ncontent-length: 0\r\n\r\n")
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            });
        }
    });
    seen
}

#[tokio::test]
async fn test_connection_reports_each_step() {
    let Some(addr) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, "test_connection_reports_each_step");
        return;
    };
    let tenant = var("SPRIG_TEST_SINKTEST_TENANT").unwrap_or_else(|| "r9tqc".into());
    let mut c = conn(&addr, &format!("{tenant}.app"), "sprig-dev-app").await;
    let db = run_id();
    ok(&mut c, &format!("CREATE DATABASE {db}")).await;
    ok(&mut c, &format!("USE {db}")).await;
    if let (Some(pg), Some(pw)) = (
        var("SPRIG_TEST_SINKTEST_PG"),
        var("SPRIG_TEST_SINKTEST_PG_PASSWORD"),
    ) {
        let (h, p) = pg.rsplit_once(':').unwrap();
        let mk = |name: &str, pw: &str| {
            format!(
                "CREATE CONNECTION {name} TYPE POSTGRESQL WITH (host = '{h}', port = {p}, \
                 database = 'postgres', tls = 'off') CREDENTIALS (username = 'postgres', password = '{pw}')"
            )
        };
        ok(&mut c, &mk("pg_ok", &pw)).await;
        ok(&mut c, &mk("pg_badpw", "not-the-password")).await;
        let s = steps(&mut c, "pg_ok").await;
        assert!(s.iter().all(|x| x.1 == "ok"), "{s:?}");
        assert_eq!(step(&s, "query").2, "logged in; SELECT 1 answered");
        let s = steps(&mut c, "pg_badpw").await;
        assert_eq!(step(&s, "credentials").1, "ok");
        assert_eq!(step(&s, "query").1, "failed", "{s:?}");
        assert!(
            step(&s, "query").2.contains("password"),
            "the reason names the password: {s:?}"
        );
    }
    if let Some(closed) = var("SPRIG_TEST_SINKTEST_CLOSED") {
        let (h, p) = closed.rsplit_once(':').unwrap();
        ok(
            &mut c,
            &format!(
                "CREATE CONNECTION my_closed TYPE MYSQL WITH (host = '{h}', port = {p}, tls = 'off') \
                 CREDENTIALS (username = 'x', password = 'y')"
            ),
        )
        .await;
        let s = steps(&mut c, "my_closed").await;
        assert_eq!(step(&s, "query").1, "failed", "{s:?}");
    }
    if let Some(k) = var("SPRIG_TEST_SINKTEST_KAFKA") {
        ok(
            &mut c,
            &format!("CREATE CONNECTION k_ok TYPE KAFKA WITH (bootstrap = '{k}')"),
        )
        .await;
        let s = steps(&mut c, "k_ok").await;
        assert!(s.iter().all(|x| x.1 == "ok"), "{s:?}");
        assert!(step(&s, "metadata").2.contains("broker"), "{s:?}");
    }
    if let (Some(s3), Some(key), Some(secret)) = (
        var("SPRIG_TEST_SINKTEST_S3"),
        var("SPRIG_TEST_SINKTEST_S3_KEY"),
        var("SPRIG_TEST_SINKTEST_S3_SECRET"),
    ) {
        let mk = |name: &str, bucket: &str, secret: &str| {
            format!(
                "CREATE CONNECTION {name} TYPE S3 WITH (endpoint = 'http://{s3}', bucket = '{bucket}', \
                 region = 'us-east-1', path_style = 'on') \
                 CREDENTIALS (access_key_id = '{key}', secret_access_key = '{secret}')"
            )
        };
        ok(&mut c, &mk("s3_missing", &format!("{}-none", db.replace('_', "-")), &secret)).await;
        ok(&mut c, &mk("s3_badkey", "qcr", "wrong-secret")).await;
        let s = steps(&mut c, "s3_missing").await;
        assert_eq!(step(&s, "head_bucket").1, "failed", "{s:?}");
        assert!(step(&s, "head_bucket").2.contains("404"), "{s:?}");
        let s = steps(&mut c, "s3_badkey").await;
        assert_eq!(step(&s, "head_bucket").1, "failed", "{s:?}");
        assert!(step(&s, "head_bucket").2.contains("403"), "{s:?}");
    }
    if let Some(port) = var("SPRIG_TEST_SINKTEST_HOOK_PORT").and_then(|p| p.parse::<u16>().ok()) {
        let seen = hook(port).await;
        ok(
            &mut c,
            &format!(
                "CREATE CONNECTION hook TYPE WEBHOOK WITH (url = 'http://127.0.0.1:{port}/in', auth = 'bearer') \
                 CREDENTIALS (token = 't0k3n')"
            ),
        )
        .await;
        let s = steps(&mut c, "hook").await;
        assert!(s.iter().all(|x| x.1 == "ok"), "{s:?}");
        assert!(step(&s, "request").2.contains("204"), "{s:?}");
        let methods = seen.lock().unwrap().clone();
        assert_eq!(methods, ["OPTIONS"], "a test sends OPTIONS only, never a POST");
    }
    // Rights: a user without USAGE on the connection is refused.
    ok(&mut c, "CREATE USER IF NOT EXISTS st_nouse IDENTIFIED BY 'sprig-dev-nouse'").await;
    ok(&mut c, &format!("GRANT SELECT ON {db}.* TO st_nouse")).await;
    let mut u = conn(&addr, &format!("{tenant}.st_nouse"), "sprig-dev-nouse").await;
    let r = sql(&mut u, &format!("TEST CONNECTION {db}.k_none")).await;
    assert!(r.is_err(), "unknown connection refused: {r:?}");
    if var("SPRIG_TEST_SINKTEST_KAFKA").is_some() {
        let r = sql(&mut u, &format!("TEST CONNECTION {db}.k_ok")).await;
        assert_eq!(r.unwrap_err().0, 39212, "needs USAGE");
        ok(&mut c, &format!("GRANT USAGE ON CONNECTION {db}.k_ok TO st_nouse")).await;
        let r = sql(&mut u, &format!("TEST CONNECTION {db}.k_ok")).await;
        assert!(r.is_ok(), "with USAGE: {r:?}");
    }
    drop(u);
    ok(&mut c, "DROP USER st_nouse").await;
    ok(&mut c, &format!("DROP DATABASE {db}")).await;
}

#[tokio::test]
async fn test_sink_shows_records_and_delivers_nothing() {
    let Some(addr) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, "test_sink_shows_records_and_delivers_nothing");
        return;
    };
    let Some(port) = var("SPRIG_TEST_SINKTEST_HOOK_PORT").and_then(|p| p.parse::<u16>().ok()) else {
        eprintln!("no SPRIG_TEST_SINKTEST_HOOK_PORT: skipped");
        return;
    };
    let tenant = var("SPRIG_TEST_SINKTEST_TENANT").unwrap_or_else(|| "r9tqc".into());
    let mut c = conn(&addr, &format!("{tenant}.app"), "sprig-dev-app").await;
    let db = run_id();
    ok(&mut c, &format!("CREATE DATABASE {db}")).await;
    ok(&mut c, &format!("USE {db}")).await;
    // The receiver answers, but a paused sink and a test send nothing.
    let seen = hook(port).await;
    ok(
        &mut c,
        &format!("CREATE CONNECTION hook TYPE WEBHOOK WITH (url = 'http://127.0.0.1:{port}/in')"),
    )
    .await;
    // A stream sink with WHERE, paused before anything is published.
    ok(&mut c, "CREATE STREAM ev PARTITIONS 2").await;
    ok(
        &mut c,
        "CREATE SINK s FROM STREAM ev WHERE value->>'$.kind' = 'a' TO WEBHOOK CONNECTION hook",
    )
    .await;
    ok(&mut c, "ALTER SINK s PAUSE").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    for i in 0..6 {
        let kind = if i % 2 == 0 { "a" } else { "b" };
        ok(
            &mut c,
            &format!(
                "INSERT INTO ev (key, value) VALUES ('k{i}', JSON_OBJECT('kind', '{kind}', 'n', {i}))"
            ),
        )
        .await;
    }
    let rows = ok(&mut c, "TEST SINK s WITH SAMPLE 10").await;
    println!("TEST SINK s:");
    for r in &rows {
        println!("  {r:?}");
    }
    assert_eq!(rows.len(), 6, "every published record shown");
    let deliver: Vec<&Vec<Option<String>>> =
        rows.iter().filter(|r| r[1].as_deref() == Some("deliver")).collect();
    let filtered = rows
        .iter()
        .filter(|r| r[1].as_deref() == Some("filtered"))
        .count();
    assert_eq!((deliver.len(), filtered), (3, 3), "WHERE kept the three 'a's");
    for r in &deliver {
        let v: serde_json::Value = serde_json::from_str(r[3].as_deref().unwrap()).unwrap();
        assert_eq!(v["kind"], "a");
        assert!(r[2].as_deref().unwrap().starts_with('k'));
    }
    let r = sql(&mut c, "TEST SINK s WITH SAMPLE 2").await.unwrap();
    assert!(r.len() <= 2, "the sample is capped: {r:?}");
    // Nothing delivered, no position moved.
    let status = ok(&mut c, "SHOW SINK STATUS s").await;
    for t in status.iter().skip(1) {
        assert_eq!(t[4].as_deref(), Some("0"), "nothing delivered: {status:?}");
    }
    // A queue sink: the sample is browsed, not taken.
    if let Some(amqp) = var("SPRIG_TEST_SINKTEST_AMQP") {
        use lapin::options::{BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions};
        let url = format!("amqp://{tenant}.app:sprig-dev-app@{amqp}/{db}");
        let a = lapin::Connection::connect(&url, lapin::ConnectionProperties::default())
            .await
            .unwrap();
        let ch = a.create_channel().await.unwrap();
        ch.queue_declare(
            "jobs".into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            lapin::types::FieldTable::default(),
        )
        .await
        .unwrap();
        ok(&mut c, "CREATE SINK sq FROM QUEUE jobs TO WEBHOOK CONNECTION hook").await;
        ok(&mut c, "ALTER SINK sq PAUSE").await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        ch.confirm_select(ConfirmSelectOptions::default())
            .await
            .unwrap();
        for i in 0..4 {
            ch.basic_publish(
                "".into(),
                "jobs".into(),
                BasicPublishOptions::default(),
                format!("{{\"job\": {i}}}").as_bytes(),
                lapin::BasicProperties::default().with_delivery_mode(2),
            )
            .await
            .unwrap()
            .await
            .unwrap();
        }
        let rows = ok(&mut c, "TEST SINK sq").await;
        println!("TEST SINK sq:");
        for r in &rows {
            println!("  {r:?}");
        }
        assert_eq!(rows.len(), 4);
        assert!(rows.iter().all(|r| r[1].as_deref() == Some("deliver")));
        assert_eq!(rows[0][3].as_deref(), Some("{\"job\": 0}"));
        assert_eq!(rows[0][2].as_deref(), Some("jobs"), "the routing key is the key");
        let info = ok(&mut c, &format!("CALL sprig.mq_info('jobs', '{db}')")).await;
        assert_eq!(info[0][0].as_deref(), Some("4"), "still ready: {info:?}");
        assert_eq!(info[0][1].as_deref(), Some("0"), "none taken: {info:?}");
        let _ = a.close(200, "bye".into()).await;
        ok(&mut c, "DROP SINK sq").await;
    }
    assert!(
        seen.lock().unwrap().is_empty(),
        "the destination saw nothing: {:?}",
        seen.lock().unwrap()
    );
    // Rights: a user who isn't a sink administrator or the owner.
    ok(&mut c, "CREATE USER IF NOT EXISTS st_plain IDENTIFIED BY 'sprig-dev-plain'").await;
    ok(&mut c, &format!("GRANT SELECT ON {db}.* TO st_plain")).await;
    let mut u = conn(&addr, &format!("{tenant}.st_plain"), "sprig-dev-plain").await;
    let r = sql(&mut u, &format!("TEST SINK {db}.s")).await;
    assert_eq!(r.unwrap_err().0, 39212, "needs SPRIG_SINK_ADMIN or ownership");
    drop(u);
    ok(&mut c, "DROP USER st_plain").await;
    ok(&mut c, "DROP SINK s").await;
    ok(&mut c, "DROP STREAM ev IMMEDIATE").await;
    ok(&mut c, &format!("DROP DATABASE {db}")).await;
}
