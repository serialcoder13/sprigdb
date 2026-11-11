//! bugfix-24 (decisions/bugfix-interim.md, Studio's ST48): a statement
//! that carries a password or a connection's credentials never shows
//! them in another session's `SHOW PROCESSLIST` or
//! `information_schema.PROCESSLIST`; they read `<secret>`, as on MySQL
//! 8.4.
//!
//! One session sends a packet that holds the secret and then sleeps (so
//! its `Info` stays up for a few seconds); a second session of the same
//! tenant polls both process lists.
//!
//! ```sh
//! export SPRIG_TEST_PROCESSLIST_SECRETS=127.0.0.1:13306          # mysql listener
//! export SPRIG_TEST_PROCESSLIST_USER=acme.app SPRIG_TEST_PROCESSLIST_PASSWORD=sprig-dev-app
//! # the login needs CREATE USER and SPRIG_SINK_ADMIN, and the tenant
//! # sink.destination_allowlist = ["127.0.0.1:9"] for the connection case
//! cargo test -p sprig-gateway --test processlist_secrets_live -- --nocapture --test-threads 1
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::many_single_char_names,
    missing_docs
)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use sprig_testkit::skip;

const ADDR: &str = "SPRIG_TEST_PROCESSLIST_SECRETS";

async fn conn(addr: &str) -> Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let user = std::env::var("SPRIG_TEST_PROCESSLIST_USER").unwrap_or_else(|_| "acme.app".into());
    let pw =
        std::env::var("SPRIG_TEST_PROCESSLIST_PASSWORD").unwrap_or_else(|_| "sprig-dev-app".into());
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(user))
        .pass(Some(pw))
        .prefer_socket(false)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true));
    Conn::new(opts).await.unwrap()
}

/// Every `Info` the two process lists show within `within`.
async fn infos(c: &mut Conn, within: Duration) -> Vec<String> {
    let t0 = Instant::now();
    let mut all = Vec::new();
    while t0.elapsed() < within {
        let rows: Vec<mysql_async::Row> = c.query("SHOW FULL PROCESSLIST").await.unwrap();
        for r in rows {
            if let Some(Some(v)) = r.get::<Option<String>, _>("Info") {
                all.push(v);
            }
        }
        let rows: Vec<Option<String>> = c
            .query("SELECT INFO FROM information_schema.PROCESSLIST")
            .await
            .unwrap();
        all.extend(rows.into_iter().flatten());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    all
}

#[tokio::test]
async fn secrets_never_show_in_the_process_list() {
    let Some(addr) = skip::var(ADDR) else {
        skip::report_missing(ADDR, "secrets_never_show_in_the_process_list");
        return;
    };
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let cases = [
        (
            "create user",
            format!(
                "CREATE USER IF NOT EXISTS 'bf6_pl'@'%' IDENTIFIED BY 'BF6PL{n}A'; SELECT SLEEP(3)"
            ),
        ),
        (
            "alter user",
            format!("ALTER USER 'bf6_pl'@'%' IDENTIFIED BY 'BF6PL{n}B'; SELECT SLEEP(3)"),
        ),
        (
            "create connection",
            format!(
                "CREATE CONNECTION IF NOT EXISTS bf6_pl_c TYPE webhook WITH (url = 'http://127.0.0.1:9/h') \
                 CREDENTIALS (token = 'BF6PL{n}C', signing_secret = 'BF6PL{n}D'); SELECT SLEEP(3)"
            ),
        ),
        (
            "set credentials",
            format!(
                "ALTER CONNECTION bf6_pl_c SET CREDENTIALS (token = 'BF6PL{n}E'); SELECT SLEEP(3)"
            ),
        ),
    ];
    let mut failed = Vec::new();
    let mut a = conn(&addr).await;
    let mut b = conn(&addr).await;
    a.query_drop("CREATE DATABASE IF NOT EXISTS bf6_pl")
        .await
        .unwrap();
    a.query_drop("USE bf6_pl").await.unwrap();
    for (what, sql) in &cases {
        let run = tokio::spawn({
            let sql = sql.clone();
            let mut a = std::mem::replace(&mut a, conn(&addr).await);
            async move {
                let r = a.query_iter(sql.as_str()).await;
                let r = match r {
                    Ok(mut q) => loop {
                        match q.collect::<mysql_async::Row>().await {
                            Ok(_) if !q.is_empty() => {}
                            Ok(_) => break Ok(()),
                            Err(e) => break Err(e),
                        }
                    },
                    Err(e) => Err(e),
                };
                (a, r)
            }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        let seen = infos(&mut b, Duration::from_millis(2000)).await;
        let (back, r) = run.await.unwrap();
        a = back;
        a.query_drop("USE bf6_pl").await.ok();
        let leaked: Vec<&String> = seen.iter().filter(|i| i.contains("BF6PL")).collect();
        let shown = seen.iter().any(|i| i.contains("<secret>"));
        eprintln!(
            "{what}: statement {:?}; {} Info values seen, leaked {}, <secret> shown {shown}",
            r.as_ref().map(|()| "ok").map_err(ToString::to_string),
            seen.len(),
            leaked.len()
        );
        if !leaked.is_empty() {
            failed.push(format!("{what}: {:?}", leaked.first()));
        }
        if !shown {
            failed.push(format!("{what}: the statement never showed in the list"));
        }
    }
    a.query_drop("DROP CONNECTION IF EXISTS bf6_pl.bf6_pl_c")
        .await
        .ok();
    a.query_drop("DROP USER IF EXISTS 'bf6_pl'@'%'").await.ok();
    a.query_drop("DROP DATABASE IF EXISTS bf6_pl").await.ok();
    assert!(failed.is_empty(), "{failed:#?}");
}
