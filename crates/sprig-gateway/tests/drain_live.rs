//! Drain (milestone M0.7; chapter 04 "Drain", build step 8): a gateway
//! asked to drain through another gateway's Admin API (the peer service
//! carries the request) finishes its in-flight work, while new requests go
//! to the other gateway.
//!
//! ```sh
//! deploy/compose/m07/stack.sh up      # gateways m7-gw-1 and m7-gw-2
//! export SPRIG_TEST_M7_DRAIN=127.0.0.1:19306,127.0.0.1:19080,127.0.0.1:19190,127.0.0.1:19316,127.0.0.1:19090,127.0.0.1:19191
//! cargo test -p sprig-gateway --test drain_live -- --nocapture
//! deploy/compose/m07/stack.sh gateway 1 start    # the drained gateway exits
//! ```
//!
//! The variable lists gateway 1's MySQL, HTTP and telemetry addresses, then
//! gateway 2's. The client plays a load balancer: it sends each request to
//! a gateway whose `/readyz` answered 200 in the last check.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines, missing_docs)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder};
use sprig_testkit::skip;

const ENV: &str = "SPRIG_TEST_M7_DRAIN";
const USER: &str = "m7a.app";
const PASSWORD: &str = "sprig-dev-app";

type Http = Client<HttpConnector, Full<Bytes>>;

fn auth() -> String {
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{USER}:{PASSWORD}"))
    )
}

async fn call(c: &Http, method: &str, url: &str, body: &str) -> (u16, serde_json::Value) {
    let req = hyper::Request::builder()
        .method(method)
        .uri(url)
        .header("authorization", auth())
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_owned())))
        .unwrap();
    match c.request(req).await {
        Ok(r) => {
            let st = r.status().as_u16();
            let b = r.into_body().collect().await.unwrap().to_bytes();
            (st, serde_json::from_slice(&b).unwrap_or(serde_json::Value::Null))
        }
        Err(e) => (0, serde_json::Value::String(e.to_string())),
    }
}

async fn ready(c: &Http, telemetry: &str) -> bool {
    let req = hyper::Request::get(format!("http://{telemetry}/readyz"))
        .body(Full::new(Bytes::new()))
        .unwrap();
    matches!(c.request(req).await, Ok(r) if r.status().is_success())
}

async fn wire(addr: &str) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    Conn::new(
        OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().unwrap())
            .user(Some(USER))
            .pass(Some(PASSWORD))
            .prefer_socket(false),
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_draining_gateway_finishes_its_work_and_new_requests_go_elsewhere() {
    let Some(v) = skip::var(ENV) else {
        skip::report_missing(ENV, "a_draining_gateway_finishes_its_work");
        return;
    };
    let u = skip::urls(&v);
    let (g1_mysql, g1_http, g1_tel) = (u[0].clone(), u[1].clone(), u[2].clone());
    let (_g2_mysql, g2_http, g2_tel) = (u[3].clone(), u[4].clone(), u[5].clone());
    let c: Http = Client::builder(TokioExecutor::new()).build_http();
    assert!(ready(&c, &g1_tel).await && ready(&c, &g2_tel).await, "both gateways ready");
    let (st, list) = call(&c, "GET", &format!("http://{g2_http}/admin/v1/gateways"), "").await;
    assert_eq!(st, 200, "{list}");
    let ids: Vec<String> = list["gateways"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| g["gateway_id"].as_str().unwrap().to_owned())
        .collect();
    println!("registered gateways: {ids:?}");
    let g1_id = list["gateways"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["listeners"]["http"].as_u64().map(|p| p.to_string()) == g1_http.rsplit_once(':').map(|(_, p)| p.to_owned()))
        .map(|g| g["gateway_id"].as_str().unwrap().to_owned())
        .expect("gateway 1's record");

    // In-flight work on gateway 1: a long statement on the wire, and an open
    // HTTP transaction.
    let mut long = wire(&g1_mysql).await;
    let long_task = tokio::spawn(async move {
        let started = Instant::now();
        let r: Option<i64> = long.query_first("SELECT SLEEP(4)").await.unwrap();
        (r, started.elapsed())
    });
    let (st, t) = call(&c, "POST", &format!("http://{g1_http}/v1/databases/-/transactions"), "{}").await;
    assert_eq!(st, 200, "{t}");
    let handle = t["name"].as_str().unwrap().to_owned();

    // The load balancer's client: round robin over the gateways that are
    // ready, re-checked every 200 ms.
    let stop = Arc::new(AtomicBool::new(false));
    let ok = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let to_g1_after = Arc::new(AtomicU64::new(0));
    let drained_at = Arc::new(std::sync::Mutex::new(None::<Instant>));
    let lb = {
        let (stop, ok, failed, to_g1_after, drained_at) =
            (stop.clone(), ok.clone(), failed.clone(), to_g1_after.clone(), drained_at.clone());
        let c = c.clone();
        let targets = [(g1_http.clone(), g1_tel.clone()), (g2_http.clone(), g2_tel.clone())];
        tokio::spawn(async move {
            let mut up = vec![true, true];
            let mut checked = Instant::now();
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                if checked.elapsed() > Duration::from_millis(200) {
                    for (k, (_, tel)) in targets.iter().enumerate() {
                        up[k] = ready(&c, tel).await;
                    }
                    checked = Instant::now();
                }
                i += 1;
                let Some(k) = (0..2).map(|d| (i + d) % 2).find(|k| up[*k]) else {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                };
                if k == 0 && drained_at.lock().unwrap().is_some_and(|t| t.elapsed() > Duration::from_millis(500)) {
                    to_g1_after.fetch_add(1, Ordering::Relaxed);
                }
                let url = format!("http://{}/v1/databases/-/sql:execute", targets[k].0);
                let (st, b) = call(&c, "POST", &url, "{\"sql\":\"SELECT 1\"}").await;
                if st == 200 {
                    ok.fetch_add(1, Ordering::Relaxed);
                } else {
                    failed.fetch_add(1, Ordering::Relaxed);
                    eprintln!("LB request to {} failed: {st} {b}", targets[k].0);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Drain gateway 1 through gateway 2 (the peer service carries it).
    let (st, r) = call(&c, "POST", &format!("http://{g2_http}/admin/v1/gateways/{g1_id}:drain"), "").await;
    assert_eq!(st, 200, "{r}");
    assert_eq!(r["started"], true);
    *drained_at.lock().unwrap() = Some(Instant::now());
    let t0 = Instant::now();
    // Readiness goes off at once.
    let mut off = None;
    while t0.elapsed() < Duration::from_secs(3) {
        if !ready(&c, &g1_tel).await {
            off = Some(t0.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let off = off.expect("gateway 1's /readyz never went to 503");
    println!("readyz 503 after {off:?}");
    // The open transaction finishes on gateway 1, through gateway 2: the
    // call is forwarded to the handle's owner.
    let body = serde_json::json!({"sql": "SELECT 42", "transaction": handle}).to_string();
    let (st, r) = call(&c, "POST", &format!("http://{g2_http}/v1/databases/-/sql:execute"), &body).await;
    assert_eq!(st, 200, "forwarded call during the drain: {r}");
    let (st, r) = call(&c, "POST", &format!("http://{g2_http}/v1/{handle}:commit"), "{}").await;
    assert_eq!(st, 200, "forwarded commit: {r}");
    // The long statement finishes.
    let (v, took) = long_task.await.unwrap();
    assert_eq!(v, Some(0));
    println!("in-flight SLEEP(4) finished after {took:?}");
    // New connections to gateway 1 are refused once its listeners close.
    let mut refused = None;
    while t0.elapsed() < Duration::from_secs(20) {
        if tokio::net::TcpStream::connect(&g1_mysql).await.is_err() {
            refused = Some(t0.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!("gateway 1 refuses connections after {refused:?}");
    assert!(refused.is_some(), "gateway 1 still accepts connections");
    // Its registration goes when it exits.
    let mut gone = None;
    while t0.elapsed() < Duration::from_secs(60) {
        let (_, list) = call(&c, "GET", &format!("http://{g2_http}/admin/v1/gateways"), "").await;
        let present = list["gateways"]
            .as_array()
            .is_some_and(|a| a.iter().any(|g| g["gateway_id"] == g1_id.as_str()));
        if !present {
            gone = Some(t0.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    println!("gateway 1 deregistered after {gone:?}");
    tokio::time::sleep(Duration::from_secs(1)).await;
    stop.store(true, Ordering::Relaxed);
    lb.await.unwrap();
    println!(
        "LB client: {} ok, {} failed, {} sent to gateway 1 more than 0.5 s after the drain",
        ok.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed),
        to_g1_after.load(Ordering::Relaxed)
    );
    assert!(gone.is_some(), "gateway 1 never deregistered");
    assert_eq!(failed.load(Ordering::Relaxed), 0, "requests failed during the drain");
    assert_eq!(to_g1_after.load(Ordering::Relaxed), 0, "new requests reached the draining gateway");
}
