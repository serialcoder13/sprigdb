//! The no-restart soak (milestone M0.7; chapter 04 step 11, chapter 23
//! step 9 "No restarts", chapter 37 "listener rebinding under load with no
//! failed request"): load over the MySQL wire, REST (HTTP and HTTPS) and
//! gRPC while certificates rotate, TLS modes and minimum versions change
//! and live settings flip, on a running gateway. It counts every request,
//! every reconnect (a connection the gateway closed at a safe point, which
//! a pool replaces; for gRPC also a request the gateway never processed
//! because its connection was closing, see `grpc_retryable`) and every
//! failure (a request that failed even after its one retry on a new
//! connection), and samples latency and the gateway's
//! memory once a minute, so a long run shows leaks or drift.
//!
//! ```sh
//! deploy/compose/m07/stack.sh up
//! export SPRIG_TEST_M7_SOAK=127.0.0.1:19306,127.0.0.1:19080,127.0.0.1:19443
//! export SPRIG_SOAK_CERTS=$PWD/deploy/compose/m07/.state/certs
//! export SPRIG_SOAK_PID=$(cat deploy/compose/m07/.state/gateway-1.pid)
//! export SPRIG_SOAK_SECONDS=600        # 259200 for the 72-hour soak
//! cargo test --release -p sprig-gateway --test soak_live -- --nocapture
//! ```
//!
//! The changes go through the Admin API on the gateway (dc scope of the
//! gateway's DC: use a DC of its own, as the m07 stack does). Login
//! `m7a.app` / `sprig-dev-app`. Heavy: take the shared lock first.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::many_single_char_names,
    missing_docs
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use rustls::pki_types::pem::PemObject as _;
use sprig_api::proto::v1::QueryRequest;
use sprig_api::proto::v1::sql_client::SqlClient;
use sprig_testkit::skip;
use tokio::sync::Mutex;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

const ENV: &str = "SPRIG_TEST_M7_SOAK";
const USER: &str = "m7a.app";
const PASSWORD: &str = "sprig-dev-app";
const DB: &str = "m7_soak";
const ROWS: u32 = 1000;
const CA: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../deploy/compose/gateway-certs/ca.pem"
);

/// Counters of one protocol.
#[derive(Default)]
struct Stats {
    ok: AtomicU64,
    reconnects: AtomicU64,
    failures: AtomicU64,
    lat: std::sync::Mutex<Vec<u32>>,
    last_error: std::sync::Mutex<Option<String>>,
}

impl Stats {
    fn record(&self, started: Instant) {
        self.ok.fetch_add(1, Ordering::Relaxed);
        let us = started.elapsed().as_micros().min(u128::from(u32::MAX)) as u32;
        self.lat.lock().unwrap().push(us);
    }

    fn fail(&self, what: String) {
        self.failures.fetch_add(1, Ordering::Relaxed);
        eprintln!("FAILURE: {what}");
        *self.last_error.lock().unwrap() = Some(what);
    }

    /// `(p50, p99)` in ms of the window, emptying it.
    fn window(&self) -> (f64, f64) {
        let mut v = std::mem::take(&mut *self.lat.lock().unwrap());
        if v.is_empty() {
            return (0.0, 0.0);
        }
        v.sort_unstable();
        let p = |q: f64| f64::from(v[((v.len() as f64 - 1.0) * q) as usize]) / 1000.0;
        (p(0.5), p(0.99))
    }
}

struct Soak {
    mysql: String,
    http: String,
    https: String,
    stop: AtomicBool,
    wire: Stats,
    rest: Stats,
    rests: Stats,
    grpc: Stats,
    changes: AtomicU64,
    admin_retries: AtomicU64,
}

fn auth() -> String {
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{USER}:{PASSWORD}"))
    )
}

async fn wire_conn(addr: &str) -> mysql_async::Result<Conn> {
    let (host, port) = addr.rsplit_once(':').unwrap();
    Conn::new(
        OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().unwrap())
            .user(Some(USER))
            .pass(Some(PASSWORD))
            .db_name(Some(DB))
            .prefer_socket(false)
            .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true)),
    )
    .await
}

/// A wire error that means "this connection is gone": the next statement
/// goes to a new connection (what every pool does).
fn connection_gone(e: &mysql_async::Error) -> bool {
    match e {
        mysql_async::Error::Server(s) => matches!(s.code, 39000 | 39004 | 2006 | 2013),
        mysql_async::Error::Io(_) | mysql_async::Error::Driver(_) => true,
        _ => false,
    }
}

fn sql_for(i: u64) -> String {
    let id = (i % u64::from(ROWS)) + 1;
    if i.is_multiple_of(10) {
        format!("UPDATE kv SET v = v + 1 WHERE id = {id}")
    } else {
        format!("SELECT id, v FROM kv WHERE id = {id}")
    }
}

async fn wire_worker(s: Arc<Soak>, n: u64) {
    let mut conn: Option<Conn> = None;
    let mut i = n * 7919;
    while !s.stop.load(Ordering::Relaxed) {
        i += 1;
        let sql = sql_for(i);
        let started = Instant::now();
        let mut tries = 0;
        loop {
            tries += 1;
            if conn.is_none() {
                match wire_conn(&s.mysql).await {
                    Ok(c) => conn = Some(c),
                    Err(e) => {
                        if tries > 1 {
                            s.wire.fail(format!("connect: {e}"));
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }
            }
            let c = conn.as_mut().unwrap();
            match c.query_drop(&sql).await {
                Ok(()) => {
                    s.wire.record(started);
                    break;
                }
                Err(e) if connection_gone(&e) && tries == 1 => {
                    s.wire.reconnects.fetch_add(1, Ordering::Relaxed);
                    conn = None;
                }
                Err(e) => {
                    s.wire.fail(format!("{sql}: {e}"));
                    conn = None;
                    break;
                }
            }
        }
    }
}

async fn rest_worker(s: Arc<Soak>, n: u64) {
    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();
    let url = format!("http://{}/v1/databases/{DB}/sql:execute", s.http);
    let mut i = n * 104_729;
    while !s.stop.load(Ordering::Relaxed) {
        i += 1;
        let body = serde_json::json!({ "sql": sql_for(i) }).to_string();
        let started = Instant::now();
        let mut tries = 0;
        loop {
            tries += 1;
            let req = hyper::Request::post(&url)
                .header("authorization", auth())
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(body.clone())))
                .unwrap();
            match client.request(req).await {
                Ok(r) if r.status().is_success() => {
                    let _ = r.into_body().collect().await;
                    s.rest.record(started);
                    break;
                }
                Ok(r) => {
                    let st = r.status();
                    let b = r
                        .into_body()
                        .collect()
                        .await
                        .map(http_body_util::Collected::to_bytes)
                        .unwrap_or_default();
                    s.rest
                        .fail(format!("{st}: {}", String::from_utf8_lossy(&b)));
                    break;
                }
                Err(e) if tries == 1 => {
                    s.rest.reconnects.fetch_add(1, Ordering::Relaxed);
                    let _ = e;
                }
                Err(e) => {
                    s.rest.fail(format!("http: {e}"));
                    break;
                }
            }
        }
    }
}

fn tls_config() -> Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    let pem = std::fs::read(CA).unwrap();
    for c in rustls::pki_types::CertificateDer::pem_slice_iter(&pem) {
        roots.add(c.unwrap()).unwrap();
    }
    let mut cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Arc::new(cfg)
}

/// HTTPS: a new TLS connection per request, so every rotation and mode
/// change is seen by new handshakes.
async fn rests_worker(s: Arc<Soak>, n: u64) {
    let tls = tokio_rustls::TlsConnector::from(tls_config());
    let mut i = n * 1_299_709;
    while !s.stop.load(Ordering::Relaxed) {
        i += 1;
        let body = serde_json::json!({ "sql": sql_for(i) }).to_string();
        let started = Instant::now();
        let r = async {
            let tcp = tokio::net::TcpStream::connect(&s.https)
                .await
                .map_err(|e| e.to_string())?;
            let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
            let stream = tls.connect(name, tcp).await.map_err(|e| e.to_string())?;
            let (mut tx, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
                .await
                .map_err(|e| e.to_string())?;
            tokio::spawn(conn);
            let req = hyper::Request::post(format!("/v1/databases/{DB}/sql:execute"))
                .header("host", &s.https)
                .header("authorization", auth())
                .body(Full::new(Bytes::from(body.clone())))
                .unwrap();
            let resp = tx.send_request(req).await.map_err(|e| e.to_string())?;
            let st = resp.status();
            let b = resp
                .into_body()
                .collect()
                .await
                .map_err(|e| e.to_string())?
                .to_bytes();
            if st.is_success() {
                Ok(())
            } else {
                Err(format!("{st}: {}", String::from_utf8_lossy(&b)))
            }
        }
        .await;
        match r {
            Ok(()) => s.rests.record(started),
            Err(e) => s.rests.fail(format!("https: {e}")),
        }
    }
}

/// A gRPC failure a well-behaved client retries on a new connection:
/// `Unavailable`, or a request the gateway never processed because its
/// connection was closing (gwdrain). tonic reports a request still queued
/// in hyper when the `GOAWAY` arrived as `Cancelled` (hyper's "connection
/// closed", the request handed back unsent), and a stream above the
/// `GOAWAY`'s last stream id as `Internal` (h2's remote `GOAWAY` with
/// `NO_ERROR`); grpc-go and grpc-java retry both on their own.
fn grpc_retryable(e: &tonic::Status) -> bool {
    use std::error::Error as _;
    if e.code() == tonic::Code::Unavailable {
        return true;
    }
    let mut src = e.source();
    while let Some(x) = src {
        if let Some(h) = x.downcast_ref::<hyper::Error>()
            && h.is_canceled()
        {
            return true;
        }
        if let Some(h) = x.downcast_ref::<h2::Error>() {
            return h.is_go_away() && h.is_remote() && h.reason() == Some(h2::Reason::NO_ERROR);
        }
        src = x.source();
    }
    false
}

async fn grpc_worker(s: Arc<Soak>, n: u64, tls: bool) {
    let mut ep = Endpoint::from_shared(format!(
        "{}://{}",
        if tls { "https" } else { "http" },
        if tls { &s.https } else { &s.http }
    ))
    .unwrap();
    if tls {
        ep = ep
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(std::fs::read(CA).unwrap()))
                    .domain_name("localhost"),
            )
            .unwrap();
    }
    let ch: Channel = ep.connect_lazy();
    let mut c = SqlClient::new(ch);
    let mut i = n * 15_485_863;
    while !s.stop.load(Ordering::Relaxed) {
        i += 1;
        let started = Instant::now();
        let mut tries = 0;
        loop {
            tries += 1;
            let mut req = tonic::Request::new(QueryRequest {
                database: DB.into(),
                sql: sql_for(i),
                transaction: String::new(),
                multi_statements: false,
                ..Default::default()
            });
            req.metadata_mut()
                .insert("authorization", auth().parse().unwrap());
            match c.execute(req).await {
                Ok(_) => {
                    s.grpc.record(started);
                    break;
                }
                Err(e) if tries == 1 && grpc_retryable(&e) => {
                    s.grpc.reconnects.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    s.grpc.fail(format!("grpc: {e}"));
                    break;
                }
            }
        }
    }
}

/// Sets `key` at dc scope through the gateway's Admin API. 39504 (a
/// leader change, retryable by contract) and transport errors are retried
/// for up to 30 s; the retries are counted.
async fn admin_set(s: &Soak, key: &str, value: &serde_json::Value) {
    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();
    let body =
        serde_json::json!({"scope": "dc", "value": value, "reason": "soak", "actor": "soak"});
    let mut last = String::new();
    for _ in 0..60 {
        let req = hyper::Request::put(format!("http://{}/admin/v1/settings/{key}", s.http))
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body.to_string())))
            .unwrap();
        match client.request(req).await {
            Ok(r) => {
                let st = r.status();
                let b = r.into_body().collect().await.unwrap().to_bytes();
                if st.is_success() {
                    return;
                }
                last = format!("{st} {}", String::from_utf8_lossy(&b));
                assert_eq!(st.as_u16(), 503, "set {key}: {last}");
            }
            Err(e) => last = e.to_string(),
        }
        s.admin_retries.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("set {key}: {last}");
}

/// One change every `period`, round robin.
async fn changer(s: Arc<Soak>, certs: Option<String>, period: Duration) {
    let mut step = 0u64;
    let mut cert_b = false;
    while !s.stop.load(Ordering::Relaxed) {
        tokio::time::sleep(period).await;
        if s.stop.load(Ordering::Relaxed) {
            break;
        }
        let what = match step % 6 {
            0 => {
                if let Some(dir) = &certs {
                    cert_b = !cert_b;
                    let name = if cert_b { "server-b" } else { "server-a" };
                    std::fs::copy(
                        format!("{dir}/{name}-key.pem"),
                        format!("{dir}/server-key.pem.tmp"),
                    )
                    .unwrap();
                    std::fs::copy(format!("{dir}/{name}.pem"), format!("{dir}/server.pem.tmp"))
                        .unwrap();
                    std::fs::rename(
                        format!("{dir}/server-key.pem.tmp"),
                        format!("{dir}/server-key.pem"),
                    )
                    .unwrap();
                    std::fs::rename(format!("{dir}/server.pem.tmp"), format!("{dir}/server.pem"))
                        .unwrap();
                    format!("certificate rotated to {name}")
                } else {
                    "certificate rotation skipped (no SPRIG_SOAK_CERTS)".into()
                }
            }
            1 => {
                let v = if step % 12 == 1 { 512 } else { 256 };
                admin_set(&s, "gateway.relay_buffer_kb", &serde_json::json!(v)).await;
                format!("gateway.relay_buffer_kb = {v}")
            }
            2 => {
                let v = if step % 12 == 2 {
                    "required"
                } else {
                    "optional"
                };
                admin_set(&s, "listener.mysql.tls", &serde_json::json!(v)).await;
                format!("listener.mysql.tls = {v}")
            }
            3 => {
                let v = if step % 12 == 3 {
                    "optional"
                } else {
                    "required"
                };
                admin_set(&s, "listener.https.tls", &serde_json::json!(v)).await;
                format!("listener.https.tls = {v}")
            }
            4 => {
                let v = if step % 12 == 4 { 100 } else { 500 };
                admin_set(&s, "api.stream_batch_rows", &serde_json::json!(v)).await;
                format!("api.stream_batch_rows = {v}")
            }
            _ => {
                let v = if step % 12 == 5 { "1.3" } else { "1.2" };
                admin_set(&s, "tls.min_version", &serde_json::json!(v)).await;
                format!("tls.min_version = {v}")
            }
        };
        s.changes.fetch_add(1, Ordering::Relaxed);
        println!("change {step}: {what}");
        step += 1;
    }
}

fn rss_kb(pid: Option<&str>) -> Option<u64> {
    let pid = pid?;
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", pid])
        .output()
        .ok()?;
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn listeners_certificates_and_settings_change_under_load() {
    let Some(v) = skip::var(ENV) else {
        skip::report_missing(ENV, "listeners_certificates_and_settings_change_under_load");
        return;
    };
    let urls = skip::urls(&v);
    let seconds: u64 = std::env::var("SPRIG_SOAK_SECONDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(600);
    let certs = std::env::var("SPRIG_SOAK_CERTS").ok();
    let pid = std::env::var("SPRIG_SOAK_PID").ok();
    let s = Arc::new(Soak {
        mysql: urls[0].clone(),
        http: urls[1].clone(),
        https: urls[2].clone(),
        stop: AtomicBool::new(false),
        wire: Stats::default(),
        rest: Stats::default(),
        rests: Stats::default(),
        grpc: Stats::default(),
        changes: AtomicU64::new(0),
        admin_retries: AtomicU64::new(0),
    });
    // The table.
    {
        let (host, port) = s.mysql.rsplit_once(':').unwrap();
        let mut c = Conn::new(
            OptsBuilder::default()
                .ip_or_hostname(host)
                .tcp_port(port.parse().unwrap())
                .user(Some(USER))
                .pass(Some(PASSWORD))
                .prefer_socket(false)
                .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true)),
        )
        .await
        .unwrap();
        c.query_drop(format!("CREATE DATABASE IF NOT EXISTS {DB}"))
            .await
            .unwrap();
        c.query_drop(format!(
            "CREATE TABLE IF NOT EXISTS {DB}.kv (id INT PRIMARY KEY, v BIGINT NOT NULL)"
        ))
        .await
        .unwrap();
        let values: Vec<String> = (1..=ROWS).map(|i| format!("({i}, 0)")).collect();
        c.query_drop(format!(
            "INSERT IGNORE INTO {DB}.kv VALUES {}",
            values.join(",")
        ))
        .await
        .unwrap();
    }
    let mut tasks = Vec::new();
    for n in 0..8 {
        tasks.push(tokio::spawn(wire_worker(Arc::clone(&s), n)));
    }
    for n in 0..4 {
        tasks.push(tokio::spawn(rest_worker(Arc::clone(&s), n)));
    }
    for n in 0..2 {
        tasks.push(tokio::spawn(rests_worker(Arc::clone(&s), n)));
    }
    for n in 0..2 {
        tasks.push(tokio::spawn(grpc_worker(Arc::clone(&s), n, n == 1)));
    }
    let change_period = Duration::from_secs(
        std::env::var("SPRIG_SOAK_CHANGE_SECONDS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(20),
    );
    let changes = tokio::spawn(changer(Arc::clone(&s), certs, change_period));
    let start = Instant::now();
    let rss0 = rss_kb(pid.as_deref());
    println!(
        "minute | wire ok/rec/fail p50/p99 ms | rest | https | grpc | changes | gateway RSS MB"
    );
    let mut minute = 0;
    let sample = Duration::from_secs(60.min(seconds));
    let lock = Mutex::new(());
    while start.elapsed() < Duration::from_secs(seconds) {
        tokio::time::sleep(
            sample.min(Duration::from_secs(seconds).saturating_sub(start.elapsed())),
        )
        .await;
        let _g = lock.lock().await;
        minute += 1;
        let col = |st: &Stats| {
            let (p50, p99) = st.window();
            format!(
                "{}/{}/{} {p50:.2}/{p99:.2}",
                st.ok.load(Ordering::Relaxed),
                st.reconnects.load(Ordering::Relaxed),
                st.failures.load(Ordering::Relaxed)
            )
        };
        println!(
            "{minute:>6} | {} | {} | {} | {} | {} | {}",
            col(&s.wire),
            col(&s.rest),
            col(&s.rests),
            col(&s.grpc),
            s.changes.load(Ordering::Relaxed),
            rss_kb(pid.as_deref())
                .map_or_else(|| "-".into(), |k| format!("{:.1}", k as f64 / 1024.0)),
        );
    }
    s.stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = t.await;
    }
    let _ = changes.await;
    // Put the changed settings back to the m07 stack's values.
    for (k, v) in [
        ("gateway.relay_buffer_kb", serde_json::json!(256)),
        ("listener.mysql.tls", serde_json::json!("optional")),
        ("listener.https.tls", serde_json::json!("required")),
        ("api.stream_batch_rows", serde_json::json!(500)),
        ("tls.min_version", serde_json::json!("1.2")),
    ] {
        admin_set(&s, k, &v).await;
    }
    let total = |st: &Stats| st.ok.load(Ordering::Relaxed);
    let fails = |st: &Stats| st.failures.load(Ordering::Relaxed);
    let rss1 = rss_kb(pid.as_deref());
    println!(
        "soak: {seconds} s, {} changes ({} admin retries); requests wire {} rest {} https {} grpc {}; reconnects wire {} rest {} grpc {}; failures {}; RSS {:?} -> {:?} KB",
        s.changes.load(Ordering::Relaxed),
        s.admin_retries.load(Ordering::Relaxed),
        total(&s.wire),
        total(&s.rest),
        total(&s.rests),
        total(&s.grpc),
        s.wire.reconnects.load(Ordering::Relaxed),
        s.rest.reconnects.load(Ordering::Relaxed),
        s.grpc.reconnects.load(Ordering::Relaxed),
        fails(&s.wire) + fails(&s.rest) + fails(&s.rests) + fails(&s.grpc),
        rss0,
        rss1,
    );
    for (name, st) in [
        ("wire", &s.wire),
        ("rest", &s.rest),
        ("https", &s.rests),
        ("grpc", &s.grpc),
    ] {
        assert_eq!(
            fails(st),
            0,
            "{name}: {} failed requests; last: {:?}",
            fails(st),
            st.last_error.lock().unwrap()
        );
        assert!(total(st) > 0, "{name}: no requests");
    }
}
