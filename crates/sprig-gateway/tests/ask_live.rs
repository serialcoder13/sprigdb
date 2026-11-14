//! `ASK` over the MySQL protocol, as a tenant's user, against a gateway
//! whose data assistant is a stub model (no request leaves the machine).
//!
//! Needs a gateway with this build, a tenant user who is the tenant's
//! administrator, a provider record pointing at an OpenAI-compatible stub
//! that answers every question with one `propose_sql` call holding
//! `SELECT id, total FROM orders WHERE total > 100; DELETE FROM orders
//! WHERE total = 0` (and in words when the question says "words"), and
//! `ai.model.data` naming it:
//!
//! ```sh
//! export SPRIG_TEST_ASK_GATEWAY=127.0.0.1:13306
//! cargo test -p sprig-gateway --test ask_live -- --nocapture --test-threads 1
//! ```
//!
//! Login `acme.app` / `sprig-dev-app` unless `SPRIG_TEST_ASK_USER` and
//! `SPRIG_TEST_ASK_PASSWORD` say otherwise. The tests create and drop the
//! database `ask_live`, and leave `ai.enabled` on.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Row, SslOpts};
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_ASK_GATEWAY";

async fn conn(test: &str) -> Option<Conn> {
    let Some(addr) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
        return None;
    };
    let (host, port) = addr.rsplit_once(':').unwrap();
    let c = Conn::new(
        OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().unwrap())
            .user(Some(
                skip::var("SPRIG_TEST_ASK_USER").unwrap_or_else(|| "acme.app".into()),
            ))
            .pass(Some(
                skip::var("SPRIG_TEST_ASK_PASSWORD").unwrap_or_else(|| "sprig-dev-app".into()),
            ))
            .prefer_socket(false)
            .ssl_opts(
                SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true),
            ),
    )
    .await
    .unwrap();
    Some(c)
}

fn code(e: &mysql_async::Error) -> u16 {
    match e {
        mysql_async::Error::Server(s) => s.code,
        e => panic!("not a server error: {e}"),
    }
}

async fn setup(c: &mut Conn) {
    c.query_drop("ALTER TENANT SPRIG SET (ai.enabled = TRUE)")
        .await
        .unwrap();
    c.query_drop("DROP DATABASE IF EXISTS ask_live")
        .await
        .unwrap();
    c.query_drop("CREATE DATABASE ask_live").await.unwrap();
    c.query_drop("USE ask_live").await.unwrap();
    c.query_drop("CREATE TABLE orders (id INT PRIMARY KEY, total INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO orders VALUES (1, 0), (2, 150), (3, 0)")
        .await
        .unwrap();
}

#[tokio::test]
async fn ask_proposes_and_runs_nothing() {
    let Some(mut c) = conn("ask_proposes_and_runs_nothing").await else {
        return;
    };
    setup(&mut c).await;
    let rows: Vec<Row> = c.query("ASK 'big orders, and clean up'").await.unwrap();
    let cols: Vec<String> = rows[0]
        .columns_ref()
        .iter()
        .map(|c| c.name_str().into_owned())
        .collect();
    assert_eq!(
        cols,
        [
            "query",
            "database",
            "explanation",
            "notes",
            "sources",
            "conversation_id"
        ]
    );
    let q: Vec<Option<String>> = rows.iter().map(|r| r.get(0).unwrap()).collect();
    assert_eq!(
        q,
        [
            Some("SELECT id, total FROM orders WHERE total > 100".to_owned()),
            Some("DELETE FROM orders WHERE total = 0".to_owned()),
        ]
    );
    let db: Option<String> = rows[0].get(1).unwrap();
    assert_eq!(db.as_deref(), Some("ask_live"));
    let notes: String = rows[1].get(3).unwrap();
    assert!(notes.contains("review it"), "{notes}");
    let conv: u64 = rows[0].get(5).unwrap();
    assert!(conv > 0);
    // Nothing ran.
    let n: Option<u64> = c.query_first("SELECT COUNT(*) FROM orders").await.unwrap();
    assert_eq!(n, Some(3));
    // In words: one row without a query.
    let rows: Vec<Row> = c
        .query("ASK 'answer in words' ROLE data IN DATABASE ask_live")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let q: Option<String> = rows[0].get(0).unwrap();
    assert_eq!(q, None);
    c.query_drop("DROP DATABASE ask_live").await.unwrap();
}

#[tokio::test]
async fn names_called_ask_still_work_and_mistakes_are_errors() {
    let Some(mut c) = conn("names_called_ask_still_work_and_mistakes_are_errors").await else {
        return;
    };
    setup(&mut c).await;
    c.query_drop("CREATE TABLE ask (ask INT)").await.unwrap();
    c.query_drop("INSERT INTO ask (ask) VALUES (7)")
        .await
        .unwrap();
    let v: Option<i64> = c.query_first("SELECT ask FROM ask").await.unwrap();
    assert_eq!(v, Some(7));
    c.query_drop("SET @ask = 'x'").await.unwrap();
    // `ASK` without a literal is the backend's syntax error.
    assert_eq!(code(&c.query_drop("ASK ask").await.unwrap_err()), 1064);
    // A mistake after the question is ours, also 1064.
    assert_eq!(
        code(&c.query_drop("ASK 'x' LIMIT 3").await.unwrap_err()),
        1064
    );
    // A role ASK doesn't offer.
    assert_eq!(
        code(&c.query_drop("ASK 'x' ROLE admin").await.unwrap_err()),
        39792
    );
    // Off: 39991.
    c.query_drop("ALTER TENANT SPRIG SET (ai.enabled = FALSE)")
        .await
        .unwrap();
    assert_eq!(code(&c.query_drop("ASK 'x'").await.unwrap_err()), 39991);
    c.query_drop("ALTER TENANT SPRIG SET (ai.enabled = TRUE)")
        .await
        .unwrap();
    c.query_drop("DROP DATABASE ask_live").await.unwrap();
}
