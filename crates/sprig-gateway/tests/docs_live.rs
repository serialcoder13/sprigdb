//! Docs search through a gateway: `POST /v1/docs:search` and the read-only
//! `sprig_docs` database, as a tenant's user.
//!
//! Needs a gateway with this build's docs module, the docs pack loaded on
//! its clusters (`cargo test -p sprig-controller --test docs_loader_live`
//! loads it), and a tenant user, such as deploy/compose/gateway-dev.sh's
//! `acme.app`:
//!
//! ```sh
//! export SPRIG_TEST_DOCS_GATEWAY=127.0.0.1:13306,127.0.0.1:25080
//! cargo test -p sprig-gateway --test docs_live -- --nocapture --test-threads 1
//! ```
//!
//! The variable lists the MySQL listener and the HTTP listener. Login
//! `acme.app` / `sprig-dev-app` unless `SPRIG_TEST_DOCS_USER` and
//! `SPRIG_TEST_DOCS_PASSWORD` say otherwise. `SPRIG_TEST_DOCS_ROUNDS`
//! (20) is how many searches the latency line averages.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    missing_docs
)]

use std::time::Instant;

use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use serde_json::{Value, json};
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_DOCS_GATEWAY";

struct Env {
    mysql: String,
    http: String,
    user: String,
    password: String,
}

fn env(test: &str) -> Option<Env> {
    let Some(v) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
        return None;
    };
    let urls = skip::urls(&v);
    assert_eq!(urls.len(), 2, "{GATEWAY}: mysql,http");
    Some(Env {
        mysql: urls[0].clone(),
        http: urls[1].clone(),
        user: skip::var("SPRIG_TEST_DOCS_USER").unwrap_or_else(|| "acme.app".into()),
        password: skip::var("SPRIG_TEST_DOCS_PASSWORD").unwrap_or_else(|| "sprig-dev-app".into()),
    })
}

fn basic(e: &Env) -> String {
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", e.user, e.password))
    )
}

async fn post(addr: &str, auth: Option<&str>, path: &str, body: &Value) -> (u16, Value) {
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut req = hyper::Request::builder()
        .method("POST")
        .uri(path)
        .header("host", addr)
        .header("content-type", "application/json");
    if let Some(a) = auth {
        req = req.header("authorization", a);
    }
    let req = req
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap();
    let (mut tx, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
        .await
        .unwrap();
    tokio::spawn(conn);
    let resp = tx.send_request(req).await.unwrap();
    let status = resp.status().as_u16();
    let b = resp.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&b).unwrap_or(Value::Null))
}

async fn mysql(e: &Env) -> Conn {
    let (host, port) = e.mysql.rsplit_once(':').unwrap();
    Conn::new(
        OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().unwrap())
            .user(Some(e.user.clone()))
            .pass(Some(e.password.clone()))
            .prefer_socket(false)
            .ssl_opts(
                SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true),
            ),
    )
    .await
    .unwrap()
}

fn code(e: &mysql_async::Error) -> u16 {
    match e {
        mysql_async::Error::Server(s) => s.code,
        e => panic!("not a server error: {e}"),
    }
}

#[tokio::test]
async fn http_search_finds_the_docs() {
    let Some(e) = env("http_search_finds_the_docs") else {
        return;
    };
    let auth = basic(&e);
    let path = "/v1/docs:search";

    // Signed-in users only.
    let (s, _) = post(&e.http, None, path, &json!({"query": "replica lag"})).await;
    assert_eq!(s, 401);
    let (s, v) = post(&e.http, Some(&auth), path, &json!({"query": "  "})).await;
    assert_eq!(s, 400, "{v}");

    let (s, v) = post(
        &e.http,
        Some(&auth),
        path,
        &json!({"query": "how do I see replica lag?", "limit": 6}),
    )
    .await;
    assert_eq!(s, 200, "{v}");
    let mode = v["mode"].as_str().unwrap().to_owned();
    let hits = v["hits"].as_array().unwrap();
    eprintln!(
        "mode {mode}, version {}, {} hits; first: {} > {}",
        v["version"],
        hits.len(),
        hits[0]["page"],
        hits[0]["heading_path"]
    );
    assert!(!hits.is_empty() && hits.len() <= 6);
    let mut per_page = std::collections::HashMap::new();
    for h in hits {
        assert!(h["text"].as_str().is_some_and(|t| !t.is_empty()));
        assert!(h["score"].as_f64().unwrap() > 0.0);
        assert!(h["bm25_rank"].is_number() || h["vector_rank"].is_number());
        *per_page.entry(h["page"].as_str().unwrap().to_owned()).or_insert(0) += 1;
    }
    assert!(per_page.values().all(|n| *n <= 2), "{per_page:?}");
    if mode == "hybrid" {
        assert!(hits.iter().any(|h| h["vector_rank"].is_number()));
    }

    // An identifier: an error code's name lands on its reference entry.
    let (s, v) = post(
        &e.http,
        Some(&auth),
        path,
        &json!({"query": "ER_SPRIG_CLUSTER_UNAVAILABLE", "limit": 3}),
    )
    .await;
    assert_eq!(s, 200, "{v}");
    let top = &v["hits"][0];
    assert!(
        top["text"].as_str().unwrap().contains("ER_SPRIG_CLUSTER_UNAVAILABLE")
            || top["text"].as_str().unwrap().contains("39100"),
        "{top}"
    );
    // A setting key.
    let (s, v) = post(
        &e.http,
        Some(&auth),
        path,
        &json!({"query": "what does auto_index.mode do", "limit": 3}),
    )
    .await;
    assert_eq!(s, 200, "{v}");
    assert!(
        v["hits"]
            .as_array()
            .unwrap()
            .iter()
            .any(|h| h["text"].as_str().unwrap().contains("auto_index.mode")),
        "{v}"
    );
    // The audience filter.
    let (s, v) = post(
        &e.http,
        Some(&auth),
        path,
        &json!({"query": "backup and restore", "audience": "operator"}),
    )
    .await;
    assert_eq!(s, 200, "{v}");
    for h in v["hits"].as_array().unwrap() {
        assert!(
            h["audience"]
                .as_array()
                .unwrap()
                .iter()
                .any(|a| a == "operator"),
            "{h}"
        );
    }
    // An unknown release.
    let (s, v) = post(
        &e.http,
        Some(&auth),
        path,
        &json!({"query": "lag", "version": "0.0.0-none"}),
    )
    .await;
    assert_eq!(s, 503, "{v}");

    // Latency over a few questions.
    let rounds: usize = skip::var("SPRIG_TEST_DOCS_ROUNDS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);
    let questions = [
        "why is replication slow",
        "how do I move a shard to another cluster",
        "what does 39412 mean",
        "turn automatic indexing off for one table",
        "rotate the TLS certificates",
    ];
    let mut times = Vec::new();
    for i in 0..rounds {
        let q = questions[i % questions.len()];
        let t = Instant::now();
        let (s, _) = post(&e.http, Some(&auth), path, &json!({"query": q})).await;
        times.push(t.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(s, 200);
    }
    times.sort_by(f64::total_cmp);
    eprintln!(
        "latency ({mode}, {rounds} searches, HTTP round trip): p50 {:.1} ms, p90 {:.1} ms, max {:.1} ms",
        times[times.len() / 2],
        times[times.len() * 9 / 10],
        times[times.len() - 1]
    );
}

#[tokio::test]
async fn sql_reads_and_refuses_writes() {
    let Some(e) = env("sql_reads_and_refuses_writes") else {
        return;
    };
    let mut c = mysql(&e).await;

    let rows: Vec<(String, String, String)> = c
        .query(
            "SELECT page, heading_path, text FROM sprig_docs.chunks \
             WHERE MATCH(text) AGAINST ('replica lag') LIMIT 5",
        )
        .await
        .unwrap();
    assert!(!rows.is_empty());
    eprintln!("MATCH: {} rows, first {} > {}", rows.len(), rows[0].0, rows[0].1);
    let n: u64 = c
        .query_first("SELECT COUNT(*) FROM sprig_docs.chunks")
        .await
        .unwrap()
        .unwrap();
    assert!(n > 100, "{n}");
    // Aliases, qualified columns, the identifiers' column.
    let pages: Vec<String> = c
        .query(
            "SELECT d.page FROM sprig_docs.chunks AS d \
             WHERE MATCH(d.text, d.heading_path, d.terms) AGAINST ('auto_index.mode') LIMIT 3",
        )
        .await
        .unwrap();
    assert!(!pages.is_empty());
    let cols: Vec<(String, String, String, String, Option<String>, String)> =
        c.query("DESCRIBE sprig_docs.chunks").await.unwrap();
    assert!(cols.iter().any(|r| r.0 == "embedding"), "{cols:?}");

    // The embedding function, when the gateway has its model.
    match c
        .query::<String, _>(
            "SELECT page FROM sprig_docs.chunks ORDER BY \
             VEC_DISTANCE_COSINE(embedding, sprig_docs.embed('why is replication slow')) LIMIT 3",
        )
        .await
    {
        Ok(p) => {
            assert_eq!(p.len(), 3);
            eprintln!("embed: {p:?}");
        }
        Err(err) => {
            // No model on this gateway: 39825 (docs search unavailable).
            assert_eq!(code(&err), 39825, "{err}");
            eprintln!("embed: unavailable here ({err})");
        }
    }

    // Writes are refused, as on information_schema: 1044.
    for w in [
        "UPDATE sprig_docs.chunks SET text = 'x'",
        "INSERT INTO sprig_docs.chunks (page) VALUES ('x')",
        "DELETE FROM sprig_docs.chunks",
        "DROP TABLE sprig_docs.chunks",
        "CREATE TABLE sprig_docs.x (a INT)",
    ] {
        let err = c.query_drop(w).await.expect_err(w);
        assert_eq!(code(&err), 1044, "{w}: {err}");
    }
    // The name can't be taken, and the system schema can't be named.
    let err = c
        .query_drop("CREATE DATABASE sprig_docs")
        .await
        .expect_err("create");
    eprintln!("CREATE DATABASE sprig_docs: {err}");
    let err = c
        .query_drop("SELECT * FROM __sprig_docs.chunks LIMIT 1")
        .await
        .expect_err("__sprig_docs");
    eprintln!("__sprig_docs: {err}");
    let err = c.query_drop("USE sprig_docs").await.expect_err("use");
    eprintln!("USE sprig_docs: {err}");
    // Other tables can't be read through the docs account.
    c.query_drop("CREATE DATABASE IF NOT EXISTS docs_live_t")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE IF NOT EXISTS docs_live_t.t (a INT)")
        .await
        .unwrap();
    let err = c
        .query_drop("SELECT * FROM sprig_docs.chunks JOIN docs_live_t.t LIMIT 1")
        .await
        .expect_err("join");
    assert_eq!(code(&err), 1142, "{err}");
    c.query_drop("DROP DATABASE docs_live_t").await.unwrap();
    // The session goes on.
    let one: u64 = c.query_first("SELECT 1").await.unwrap().unwrap();
    assert_eq!(one, 1);
    c.disconnect().await.ok();
}
