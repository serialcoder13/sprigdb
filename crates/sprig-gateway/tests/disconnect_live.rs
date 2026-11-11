//! A REST or gRPC client that goes away while its statement runs leaves no
//! statement behind (chapter 37, "Streaming and pagination"; tui decision
//! T3): the gateway notices the dropped connection or stream while the
//! statement sends nothing (`SELECT SLEEP(30)`), kills the backend
//! statement (`KILL QUERY` on its connection) and releases the session
//! within a second. Inside a transaction handle only the statement stops:
//! the transaction and its earlier writes survive for the next call.
//!
//! Each case starts `SELECT SLEEP(30) AS <marker>`, waits until MariaDB
//! runs it, disconnects the way that client would (HTTP/1.1 socket closed,
//! HTTP/2 stream reset with the connection kept, gRPC call dropped), and
//! then polls the backend's `information_schema.PROCESSLIST` and the
//! gateway's `SHOW PROCESSLIST` until the marker is gone.
//!
//! ```sh
//! deploy/compose/gateway-dev.sh up            # catalog mode, tenant acme on dc-a-c1
//! # plus listener.http.bind and listener.http.enabled (plain HTTP)
//! export SPRIG_TEST_HTTP_DISCONNECT=127.0.0.1:13306,127.0.0.1:8080   # mysql, http
//! export SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301
//! cargo test -p sprig-gateway --test disconnect_live -- --nocapture --test-threads 1
//! ```
//!
//! Login `acme.app` / `sprig-dev-app` unless `SPRIG_TEST_HTTP_USER` and
//! `SPRIG_TEST_HTTP_PASSWORD` say otherwise.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::many_single_char_names,
    missing_docs
)]

use std::time::{Duration, Instant};

use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Pool, SslOpts};
use sprig_api::proto::v1::QueryRequest;
use sprig_api::proto::v1::sql_client::SqlClient;
use sprig_testkit::skip;
use tonic::transport::Endpoint;

const GATEWAY: &str = "SPRIG_TEST_HTTP_DISCONNECT";
const BACKEND: &str = "SPRIG_TEST_MARIADB_SINGLE";
const DB: &str = "bugfix4_disc";
/// How long a statement may outlive its client.
const LIMIT: Duration = Duration::from_secs(1);

struct Env {
    mysql: String,
    http: String,
    user: String,
    password: String,
    backend: Pool,
}

fn env(test: &str) -> Option<Env> {
    let Some(v) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
        return None;
    };
    let Some(b) = skip::var(BACKEND) else {
        skip::report_missing(BACKEND, test);
        return None;
    };
    let urls = skip::urls(&v);
    assert_eq!(urls.len(), 2, "{GATEWAY}: mysql,http");
    let opts = mysql_async::Opts::from_url(&b).unwrap();
    let backend = Pool::new(
        OptsBuilder::from_opts(opts).pool_opts(
            mysql_async::PoolOpts::default()
                .with_constraints(mysql_async::PoolConstraints::new(0, 2).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(30)),
        ),
    );
    Some(Env {
        mysql: urls[0].clone(),
        http: urls[1].clone(),
        user: std::env::var("SPRIG_TEST_HTTP_USER").unwrap_or_else(|_| "acme.app".into()),
        password: std::env::var("SPRIG_TEST_HTTP_PASSWORD")
            .unwrap_or_else(|_| "sprig-dev-app".into()),
        backend,
    })
}

fn basic(e: &Env) -> String {
    let token =
        base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", e.user, e.password));
    format!("Basic {token}")
}

async fn wire_conn(e: &Env) -> Conn {
    let (host, port) = e.mysql.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(e.user.clone()))
        .pass(Some(e.password.clone()))
        .prefer_socket(false)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true));
    Conn::new(opts).await.expect("wire connect")
}

fn marker(case: &str) -> String {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("bf4_{case}_{n}")
}

fn sleep_sql(marker: &str) -> String {
    format!("SELECT SLEEP(30) AS {marker}")
}

/// Statements running on the backend whose text has `marker`.
async fn backend_running(e: &Env, marker: &str) -> u64 {
    let mut c = e.backend.get_conn().await.unwrap();
    let sql = format!(
        "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE INFO LIKE '%{marker}%' AND INFO NOT LIKE '%PROCESSLIST%'"
    );
    c.query_first::<u64, _>(sql).await.unwrap().unwrap_or(0)
}

/// The gateway's sessions (the user's own, any protocol) showing `marker`.
async fn gateway_running(wire: &mut Conn, marker: &str) -> u64 {
    let rows: Vec<mysql_async::Row> = wire.query("SHOW FULL PROCESSLIST").await.unwrap();
    rows.iter()
        .filter(|r| {
            r.get::<Option<String>, _>("Info")
                .flatten()
                .is_some_and(|i| i.contains(marker) && !i.contains("PROCESSLIST"))
        })
        .count() as u64
}

/// Waits until the backend runs the statement (at most 10 s); `call` is
/// the client's request, which mustn't end first.
async fn wait_started<T: std::fmt::Debug>(
    e: &Env,
    marker: &str,
    call: &mut tokio::task::JoinHandle<T>,
) {
    let end = Instant::now() + Duration::from_secs(10);
    while backend_running(e, marker).await == 0 {
        assert!(
            !call.is_finished(),
            "{marker}: the call ended first: {:?}",
            call.await
        );
        assert!(
            Instant::now() < end,
            "{marker}: the statement never started"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// An HTTP/1.1 or HTTP/2 response read to its end: status and body.
async fn whole(
    r: std::result::Result<hyper::Response<hyper::body::Incoming>, hyper::Error>,
) -> std::result::Result<(u16, String), hyper::Error> {
    let r = r?;
    let status = r.status().as_u16();
    let b = r.into_body().collect().await?.to_bytes();
    Ok((status, String::from_utf8_lossy(&b).into_owned()))
}

/// After the client left at `left`: the time until neither the backend nor
/// the gateway shows the statement, or `None` past three seconds.
async fn gone_after(e: &Env, wire: &mut Conn, marker: &str, left: Instant) -> Option<Duration> {
    let end = left + Duration::from_secs(3);
    loop {
        if backend_running(e, marker).await == 0 && gateway_running(wire, marker).await == 0 {
            return Some(left.elapsed());
        }
        if Instant::now() > end {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn check(case: &str, took: Option<Duration>) {
    match took {
        Some(t) => {
            eprintln!(
                "{case}: statement gone {} ms after the client left",
                t.as_millis()
            );
            assert!(
                t <= LIMIT,
                "{case}: still running {} ms after the client left",
                t.as_millis()
            );
        }
        None => panic!("{case}: the statement still runs 3 s after the client left"),
    }
}

fn request(e: &Env, path: &str, body: &str, ndjson: bool) -> hyper::Request<Full<Bytes>> {
    let mut b = hyper::Request::builder()
        .method("POST")
        .uri(format!("http://{}{path}", e.http))
        .header("host", e.http.as_str())
        .header("authorization", basic(e))
        .header("content-type", "application/json");
    if ndjson {
        b = b.header("accept", "application/x-ndjson");
    }
    b.body(Full::new(Bytes::from(body.to_owned()))).unwrap()
}

async fn setup(e: &Env) -> Conn {
    let mut w = wire_conn(e).await;
    w.query_drop(format!("CREATE DATABASE IF NOT EXISTS {DB}"))
        .await
        .unwrap();
    w.query_drop(format!(
        "CREATE TABLE IF NOT EXISTS {DB}.t (id INT PRIMARY KEY, v INT)"
    ))
    .await
    .unwrap();
    w
}

/// HTTP/1.1: the client closes its socket while the response (NDJSON
/// streamed, or buffered `sql:execute`) waits on the statement.
#[tokio::test]
async fn http1_client_gone() {
    let Some(e) = env("http1_client_gone") else {
        return;
    };
    let mut wire = setup(&e).await;
    for (case, path, ndjson) in [
        ("h1_query_ndjson", "sql:query", true),
        ("h1_query_array", "sql:query", false),
        ("h1_execute", "sql:execute", false),
    ] {
        let m = marker(case);
        let body = serde_json::json!({ "sql": sleep_sql(&m) }).to_string();
        let tcp = tokio::net::TcpStream::connect(&e.http).await.unwrap();
        let (mut tx, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
            .await
            .unwrap();
        let conn = tokio::spawn(conn);
        let req = request(&e, &format!("/v1/databases/{DB}/{path}"), &body, ndjson);
        let mut call = tokio::spawn(async move { whole(tx.send_request(req).await).await });
        wait_started(&e, &m, &mut call).await;
        // The client process goes away: its socket closes.
        call.abort();
        conn.abort();
        let left = Instant::now();
        check(case, gone_after(&e, &mut wire, &m, left).await);
    }
}

/// HTTP/2 (h2c): the client resets the stream and keeps the connection, as
/// a browser or HTTP/2 client cancelling one request does; then the whole
/// connection goes.
#[tokio::test]
async fn http2_client_gone() {
    let Some(e) = env("http2_client_gone") else {
        return;
    };
    let mut wire = setup(&e).await;
    let tcp = tokio::net::TcpStream::connect(&e.http).await.unwrap();
    let (tx, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(tcp))
        .await
        .unwrap();
    let conn = tokio::spawn(conn);
    for (case, path, ndjson) in [
        ("h2_query_ndjson", "sql:query", true),
        ("h2_execute", "sql:execute", false),
    ] {
        let m = marker(case);
        let body = serde_json::json!({ "sql": sleep_sql(&m) }).to_string();
        let mut tx2 = tx.clone();
        let req = request(&e, &format!("/v1/databases/{DB}/{path}"), &body, ndjson);
        // Holds the body open (the NDJSON stream) until aborted.
        let mut call = tokio::spawn(async move { whole(tx2.send_request(req).await).await });
        wait_started(&e, &m, &mut call).await;
        // Dropping the request's future and body resets the stream.
        call.abort();
        let left = Instant::now();
        check(case, gone_after(&e, &mut wire, &m, left).await);
    }
    // The connection itself goes away mid-statement.
    let m = marker("h2_conn_closed");
    let body = serde_json::json!({ "sql": sleep_sql(&m) }).to_string();
    let mut tx2 = tx.clone();
    let req = request(&e, &format!("/v1/databases/{DB}/sql:query"), &body, true);
    let mut call = tokio::spawn(async move { whole(tx2.send_request(req).await).await });
    wait_started(&e, &m, &mut call).await;
    call.abort();
    conn.abort();
    drop(tx);
    let left = Instant::now();
    check("h2_conn_closed", gone_after(&e, &mut wire, &m, left).await);
}

/// gRPC: the caller drops a `Sql.Query` stream (`RST_STREAM`, channel kept)
/// and a unary `Sql.Execute` call.
#[tokio::test]
async fn grpc_client_gone() {
    let Some(e) = env("grpc_client_gone") else {
        return;
    };
    let mut wire = setup(&e).await;
    let ch = Endpoint::from_shared(format!("http://{}", e.http))
        .unwrap()
        .connect()
        .await
        .unwrap();
    for case in ["grpc_query", "grpc_execute"] {
        let m = marker(case);
        let mut client = SqlClient::new(ch.clone());
        let mut req = tonic::Request::new(QueryRequest {
            database: DB.into(),
            sql: sleep_sql(&m),
            ..QueryRequest::default()
        });
        req.metadata_mut()
            .insert("authorization", basic(&e).parse().unwrap());
        let mut call = if case == "grpc_query" {
            tokio::spawn(async move {
                let mut s = client.query(req).await?.into_inner();
                while s.message().await?.is_some() {}
                Ok::<_, tonic::Status>(())
            })
        } else {
            tokio::spawn(async move { client.execute(req).await.map(|_| ()) })
        };
        wait_started(&e, &m, &mut call).await;
        call.abort();
        let left = Instant::now();
        check(case, gone_after(&e, &mut wire, &m, left).await);
    }
    // The channel still works.
    let mut client = SqlClient::new(ch);
    let mut req = tonic::Request::new(QueryRequest {
        database: DB.into(),
        sql: "SELECT 1".into(),
        ..QueryRequest::default()
    });
    req.metadata_mut()
        .insert("authorization", basic(&e).parse().unwrap());
    client.execute(req).await.unwrap();
}

async fn rest_json(e: &Env, path: &str, body: &serde_json::Value) -> (u16, serde_json::Value) {
    let tcp = tokio::net::TcpStream::connect(&e.http).await.unwrap();
    let (mut tx, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
        .await
        .unwrap();
    tokio::spawn(conn);
    let resp = tx
        .send_request(request(e, path, &body.to_string(), false))
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let b = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&b).unwrap_or(serde_json::Value::Null),
    )
}

/// Inside a transaction handle: the statement stops when its client goes,
/// and the transaction, with its earlier write, goes on.
#[tokio::test]
async fn transaction_survives_a_gone_client() {
    let Some(e) = env("transaction_survives_a_gone_client") else {
        return;
    };
    let mut wire = setup(&e).await;
    wire.query_drop(format!("DELETE FROM {DB}.t"))
        .await
        .unwrap();
    let (s, t) = rest_json(
        &e,
        &format!("/v1/databases/{DB}/transactions"),
        &serde_json::json!({}),
    )
    .await;
    assert_eq!(s, 200, "{t}");
    let handle = t["name"].as_str().unwrap().to_owned();
    let (s, r) = rest_json(
        &e,
        &format!("/v1/databases/{DB}/sql:execute"),
        &serde_json::json!({"sql": "INSERT INTO t VALUES (1, 1)", "transaction": handle}),
    )
    .await;
    assert_eq!(s, 200, "{r}");
    let m = marker("txn");
    let body = serde_json::json!({ "sql": sleep_sql(&m), "transaction": handle }).to_string();
    let tcp = tokio::net::TcpStream::connect(&e.http).await.unwrap();
    let (mut tx, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
        .await
        .unwrap();
    let conn = tokio::spawn(conn);
    let req = request(&e, &format!("/v1/databases/{DB}/sql:query"), &body, true);
    let mut call = tokio::spawn(async move { whole(tx.send_request(req).await).await });
    wait_started(&e, &m, &mut call).await;
    call.abort();
    conn.abort();
    let left = Instant::now();
    check("txn", gone_after(&e, &mut wire, &m, left).await);
    // The handle still holds the transaction: a second write, a commit,
    // and both rows are there.
    let (s, r) = rest_json(
        &e,
        &format!("/v1/databases/{DB}/sql:execute"),
        &serde_json::json!({"sql": "INSERT INTO t VALUES (2, 2)", "transaction": handle}),
    )
    .await;
    assert_eq!(s, 200, "the handle after its client left: {r}");
    let (s, r) = rest_json(&e, &format!("/v1/{handle}:commit"), &serde_json::json!({})).await;
    assert_eq!(s, 200, "commit: {r}");
    let n: u64 = wire
        .query_first(format!("SELECT COUNT(*) FROM {DB}.t WHERE id IN (1, 2)"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 2, "both writes of the transaction committed");
}
