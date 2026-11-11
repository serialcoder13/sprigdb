//! A REST or gRPC client that goes away while a statement on a sharded
//! table runs leaves no per-shard statement behind (studio2's ST31; the
//! single-connection case is `disconnect_live.rs`). The router runs one
//! statement per shard (a scatter's streams, a merge, partial aggregates);
//! when the client leaves before the first row, every one of them is
//! stopped with `KILL QUERY`, not only the session's own connection's.
//!
//! Each case starts a statement that sleeps on every shard, waits until
//! MariaDB runs it on all of them, disconnects, and polls the backends'
//! `information_schema.PROCESSLIST` until no shard statement is left.
//!
//! ```sh
//! deploy/compose/gateway-dev.sh up     # plus listener.http.bind and listener.http.enabled
//! # a table split into two or more shards, for example:
//! #   CREATE DATABASE bugfix5_sh; CREATE TABLE bugfix5_sh.customers (id INT PRIMARY KEY, ...);
//! #   SPRIG PIN customers SHARD KEY (id); sprigctl advisor run/apply; sprigctl shard split
//! export SPRIG_TEST_SHARD_DISCONNECT=127.0.0.1:13306,127.0.0.1:8080       # mysql, http
//! export SPRIG_TEST_SHARD_TABLE=bugfix5_sh.customers
//! export SPRIG_TEST_SHARD_BACKENDS=mysql://root:sprig-dev-root@127.0.0.1:13301   # every cluster with a shard
//! cargo test -p sprig-gateway --test shard_disconnect_live -- --nocapture --test-threads 1
//! ```
//!
//! Login `acme.app` / `sprig-dev-app` unless `SPRIG_TEST_HTTP_USER` and
//! `SPRIG_TEST_HTTP_PASSWORD` say otherwise.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::time::{Duration, Instant};

use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, Pool};
use sprig_api::proto::v1::QueryRequest;
use sprig_api::proto::v1::sql_client::SqlClient;
use sprig_testkit::skip;
use tonic::transport::Endpoint;

const GATEWAY: &str = "SPRIG_TEST_SHARD_DISCONNECT";
const TABLE: &str = "SPRIG_TEST_SHARD_TABLE";
const BACKENDS: &str = "SPRIG_TEST_SHARD_BACKENDS";
/// How long a shard statement may outlive its client.
const LIMIT: Duration = Duration::from_secs(1);

struct Env {
    http: String,
    db: String,
    table: String,
    user: String,
    password: String,
    backends: Vec<Pool>,
}

fn env(test: &str) -> Option<Env> {
    let mut vals = Vec::new();
    for v in [GATEWAY, TABLE, BACKENDS] {
        let Some(x) = skip::var(v) else {
            skip::report_missing(v, test);
            return None;
        };
        vals.push(x);
    }
    let urls = skip::urls(&vals[0]);
    assert_eq!(urls.len(), 2, "{GATEWAY}: mysql,http");
    let (db, table) = vals[1].split_once('.').expect("SPRIG_TEST_SHARD_TABLE: db.table");
    let backends = vals[2]
        .split(',')
        .map(|u| {
            let opts = mysql_async::Opts::from_url(u.trim()).unwrap();
            Pool::new(
                OptsBuilder::from_opts(opts).pool_opts(
                    mysql_async::PoolOpts::default()
                        .with_constraints(mysql_async::PoolConstraints::new(0, 2).unwrap())
                        .with_inactive_connection_ttl(Duration::from_secs(30)),
                ),
            )
        })
        .collect();
    Some(Env {
        http: urls[1].clone(),
        db: db.to_owned(),
        table: table.to_owned(),
        user: std::env::var("SPRIG_TEST_HTTP_USER").unwrap_or_else(|_| "acme.app".into()),
        password: std::env::var("SPRIG_TEST_HTTP_PASSWORD")
            .unwrap_or_else(|_| "sprig-dev-app".into()),
        backends,
    })
}

fn basic(e: &Env) -> String {
    let token =
        base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", e.user, e.password));
    format!("Basic {token}")
}

fn marker(case: &str) -> String {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("bf5_{case}_{n}")
}

/// Statements that sleep on every shard: a scatter whose rows stream, one
/// whose shards' rows are merged in order, and partial aggregates.
fn statements(e: &Env, m: &str) -> Vec<(&'static str, String)> {
    let t = &e.table;
    vec![
        ("scatter", format!("SELECT SLEEP(30) AS {m} FROM {t} LIMIT 1")),
        (
            "merge",
            format!("SELECT id, SLEEP(30) AS {m} FROM {t} ORDER BY id LIMIT 1"),
        ),
        (
            "aggregate",
            format!("SELECT MAX(SLEEP(30 + id * 0)) AS {m} FROM {t} WHERE id < 30"),
        ),
    ]
}

/// Shard statements running on the backends whose text has `marker`.
async fn running(e: &Env, marker: &str) -> u64 {
    let mut n = 0;
    for b in &e.backends {
        let mut c = b.get_conn().await.unwrap();
        let sql = format!(
            "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE INFO LIKE '%{marker}%' AND INFO NOT LIKE '%PROCESSLIST%'"
        );
        n += c.query_first::<u64, _>(sql).await.unwrap().unwrap_or(0);
    }
    n
}

/// Waits until the statement runs on two shards or more (at most 10 s).
async fn wait_started<T: std::fmt::Debug>(
    e: &Env,
    marker: &str,
    call: &mut tokio::task::JoinHandle<T>,
) -> u64 {
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        let n = running(e, marker).await;
        if n >= 2 {
            return n;
        }
        assert!(
            !call.is_finished(),
            "{marker}: the call ended first: {:?}",
            call.await
        );
        assert!(
            Instant::now() < end,
            "{marker}: the statement never ran on two shards ({n})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// After the client left at `left`: the time until no shard statement is
/// left, or the number still running three seconds later.
async fn gone_after(e: &Env, marker: &str, left: Instant) -> Result<Duration, u64> {
    let end = left + Duration::from_secs(3);
    loop {
        let n = running(e, marker).await;
        if n == 0 {
            return Ok(left.elapsed());
        }
        if Instant::now() > end {
            return Err(n);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn check(case: &str, shards: u64, took: Result<Duration, u64>) {
    match took {
        Ok(t) => {
            eprintln!(
                "{case}: {shards} shard statements gone {} ms after the client left",
                t.as_millis()
            );
            assert!(t <= LIMIT, "{case}: still running {} ms after", t.as_millis());
        }
        Err(n) => panic!("{case}: {n} of {shards} shard statements still run 3 s after the client left"),
    }
}

async fn whole(
    r: std::result::Result<hyper::Response<hyper::body::Incoming>, hyper::Error>,
) -> std::result::Result<(u16, String), hyper::Error> {
    let r = r?;
    let status = r.status().as_u16();
    let b = r.into_body().collect().await?.to_bytes();
    Ok((status, String::from_utf8_lossy(&b).into_owned()))
}

/// REST (HTTP/1.1, NDJSON and buffered): the client's socket closes
/// before the first row.
#[tokio::test]
async fn rest_client_gone_stops_every_shard() {
    let Some(e) = env("rest_client_gone_stops_every_shard") else {
        return;
    };
    for ndjson in [true, false] {
        let m = marker(if ndjson { "ndjson" } else { "json" });
        for (case, sql) in statements(&e, &m) {
            let m = format!("{m}_{case}");
            let sql = sql.replace(&marker_of(&sql), &m);
            let body = serde_json::json!({ "sql": sql }).to_string();
            let tcp = tokio::net::TcpStream::connect(&e.http).await.unwrap();
            let (mut tx, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
                .await
                .unwrap();
            let conn = tokio::spawn(conn);
            let mut b = hyper::Request::builder()
                .method("POST")
                .uri(format!("http://{}/v1/databases/{}/sql:query", e.http, e.db))
                .header("host", e.http.as_str())
                .header("authorization", basic(&e))
                .header("content-type", "application/json");
            if ndjson {
                b = b.header("accept", "application/x-ndjson");
            }
            let req = b.body(Full::new(Bytes::from(body))).unwrap();
            let mut call = tokio::spawn(async move { whole(tx.send_request(req).await).await });
            let shards = wait_started(&e, &m, &mut call).await;
            call.abort();
            conn.abort();
            let left = Instant::now();
            check(
                &format!("rest {} {case}", if ndjson { "ndjson" } else { "json" }),
                shards,
                gone_after(&e, &m, left).await,
            );
        }
    }
}

/// gRPC `Sql.Query`: the caller drops the stream before the first row.
#[tokio::test]
async fn grpc_client_gone_stops_every_shard() {
    let Some(e) = env("grpc_client_gone_stops_every_shard") else {
        return;
    };
    let ch = Endpoint::from_shared(format!("http://{}", e.http))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let base = marker("grpc");
    for (case, sql) in statements(&e, &base) {
        let m = format!("{base}_{case}");
        let sql = sql.replace(&base, &m);
        let mut client = SqlClient::new(ch.clone());
        let mut req = tonic::Request::new(QueryRequest {
            database: e.db.clone(),
            sql,
            ..QueryRequest::default()
        });
        req.metadata_mut()
            .insert("authorization", basic(&e).parse().unwrap());
        let mut call = tokio::spawn(async move {
            let mut s = client.query(req).await?.into_inner();
            while s.message().await?.is_some() {}
            Ok::<_, tonic::Status>(())
        });
        let shards = wait_started(&e, &m, &mut call).await;
        call.abort();
        let left = Instant::now();
        check(&format!("grpc {case}"), shards, gone_after(&e, &m, left).await);
    }
}

/// The marker a statement of [`statements`] carries (the word after `AS`).
fn marker_of(sql: &str) -> String {
    sql.split_whitespace()
        .skip_while(|w| *w != "AS")
        .nth(1)
        .unwrap()
        .to_owned()
}
