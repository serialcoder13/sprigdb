//! Graceful close of HTTP connections on listener changes (gwdrain):
//! gRPC and REST clients in tight loops while listeners change TLS mode,
//! minimum TLS version and certificate, and while connections are closed
//! with `GOAWAY`. Each failure is classified by what the client saw.
//!
//! The two measured runs are `#[ignore]`d (they take a while); run them
//! with
//!
//! ```sh
//! SPRIG_GWDRAIN_CHANGES=120 cargo test -p sprig-gateway --lib \
//!     http::drain_tests -- --ignored --nocapture
//! ```

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::error::Error as _;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use futures::Stream;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Request, Response};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use sprig_api::proto::health::health_check_response::ServingStatus;
use sprig_api::proto::health::health_client::HealthClient;
use sprig_api::proto::health::health_server::{Health, HealthServer};
use sprig_api::proto::health::{HealthCheckRequest, HealthCheckResponse};
use sprig_common::CancellationToken;
use sprig_common::time::SystemClock;
use tonic::body::Body;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

use crate::frontend::{ClientConn, Frontend, GatewayContext, InProtocolTls, MaybeTls, Protocol};
use crate::listener::{Frontends, ListenerManager, ListenerSpec, TlsMode, TlsVersion};
use crate::testutil::{TempDir, TestCa, resolution, spec};

use crate::tls::{CertStore, PollOutcome};

const FAR: (i32, u8, u8) = (2045, 1, 1);
/// How long `/slow` takes.
const SLOW: Duration = Duration::from_millis(400);

struct TestHealth;

type HealthStream =
    Pin<Box<dyn Stream<Item = std::result::Result<HealthCheckResponse, tonic::Status>> + Send>>;

#[tonic::async_trait]
impl Health for TestHealth {
    async fn check(
        &self,
        _req: tonic::Request<HealthCheckRequest>,
    ) -> std::result::Result<tonic::Response<HealthCheckResponse>, tonic::Status> {
        Ok(tonic::Response::new(HealthCheckResponse {
            status: ServingStatus::Serving as i32,
        }))
    }

    type WatchStream = HealthStream;

    async fn watch(
        &self,
        _req: tonic::Request<HealthCheckRequest>,
    ) -> std::result::Result<tonic::Response<HealthStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("watch"))
    }
}

/// gRPC health and a REST `200 ok`, through the real connection code
/// ([`super::serve_conn`]). Counts the connections it served.
struct TestHttp {
    grpc: tonic::service::Routes,
    conns: Arc<AtomicU64>,
}

impl TestHttp {
    fn new() -> Self {
        Self {
            grpc: tonic::service::Routes::new(HealthServer::new(TestHealth)),
            conns: Arc::new(AtomicU64::new(0)),
        }
    }
}

fn test_service(
    grpc: tonic::service::Routes,
) -> impl hyper::service::Service<
    Request<Incoming>,
    Response = Response<Body>,
    Error = Infallible,
    Future = impl Send + 'static,
> + Send
+ 'static {
    hyper::service::service_fn(move |req: Request<Incoming>| {
        let mut grpc = grpc.clone();
        async move {
            let resp = if super::is_grpc(&req) {
                match tonic::codegen::Service::call(&mut grpc, req).await {
                    Ok(r) => r,
                    Err(never) => match never {},
                }
            } else {
                if req.uri().path() == "/slow" {
                    tokio::time::sleep(SLOW).await;
                }
                Response::new(Body::new(Full::new(Bytes::from_static(b"ok"))))
            };
            Ok::<_, Infallible>(resp)
        }
    })
}

#[async_trait]
impl Frontend for TestHttp {
    fn protocol(&self) -> Protocol {
        Protocol::Http
    }

    async fn serve(&self, conn: ClientConn, _ctx: Arc<GatewayContext>) -> sprig_common::Result<()> {
        self.conns.fetch_add(1, Ordering::Relaxed);
        super::serve_conn(conn, test_service(self.grpc.clone()), 100).await
    }
}

/// What the clients saw.
#[derive(Default)]
struct Tally {
    ok: AtomicU64,
    /// Failed once, then succeeded on the retry the client's policy allows.
    retried: AtomicU64,
    /// Failures the client's policy does not retry (or whose retry failed
    /// too), by kind.
    failed: Mutex<BTreeMap<String, u64>>,
    /// Every first failure, by kind, retried or not.
    seen: Mutex<BTreeMap<String, u64>>,
}

impl Tally {
    fn add(map: &Mutex<BTreeMap<String, u64>>, k: String) {
        *map.lock().unwrap().entry(k).or_default() += 1;
    }

    fn failures(&self) -> u64 {
        self.failed.lock().unwrap().values().sum()
    }

    fn line(&self, name: &str) -> String {
        format!(
            "{name}: ok {} retried {} failed {} {:?} seen {:?}",
            self.ok.load(Ordering::Relaxed),
            self.retried.load(Ordering::Relaxed),
            self.failures(),
            self.failed.lock().unwrap(),
            self.seen.lock().unwrap(),
        )
    }
}

/// The innermost few errors of a source chain, for the failure kinds.
fn chain(mut e: Option<&(dyn std::error::Error + 'static)>) -> String {
    let mut parts = Vec::new();
    while let Some(x) = e {
        let s = x.to_string();
        if parts.last() != Some(&s) {
            parts.push(s);
        }
        e = x.source();
    }
    parts.join(" / ")
}

/// A gRPC failure in a few words: the code, whether the server never
/// processed the request, and the error chain.
fn grpc_kind(s: &tonic::Status) -> String {
    let sent = if never_sent(s) {
        " (not sent)"
    } else if refused_by_goaway(s) {
        " (refused by GOAWAY)"
    } else {
        ""
    };
    format!("{:?}{sent}: {}", s.code(), chain(s.source()))
}

/// A stream the server's `GOAWAY` refused: its id is above the last
/// stream id the server accepted, so the server never processed it and
/// HTTP/2 says the client may retry it (RFC 9113 section 6.8). h2 reports
/// it as a remote `GOAWAY` with `NO_ERROR`, which tonic maps to `Internal`.
fn refused_by_goaway(s: &tonic::Status) -> bool {
    let mut src = s.source();
    while let Some(e) = src {
        if let Some(h) = e.downcast_ref::<h2::Error>() {
            return h.is_go_away() && h.is_remote() && h.reason() == Some(h2::Reason::NO_ERROR);
        }
        src = e.source();
    }
    false
}

/// A request that never reached the server: safe to retry whatever it
/// does. tonic reports hyper's "connection closed" for a request that was
/// still queued in the client when the connection closed as `Cancelled`.
fn never_sent(s: &tonic::Status) -> bool {
    let mut src = s.source();
    while let Some(e) = src {
        if let Some(h) = e.downcast_ref::<hyper::Error>() {
            return h.is_canceled();
        }
        src = e.source();
    }
    false
}

/// Client retry policies.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Policy {
    /// Retry once on `Unavailable` (the soak client before gwdrain).
    Unavailable,
    /// Also retry once a request the server never processed: one that never
    /// left the client, or a stream the server's `GOAWAY` refused (what
    /// grpc-go and grpc-java retry on their own).
    NotSent,
}

fn pem(der: &[u8]) -> String {
    let b64 = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = String::from("-----BEGIN CERTIFICATE-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).unwrap());
        out.push('\n');
    }
    out.push_str("-----END CERTIFICATE-----\n");
    out
}

fn channel(addr: SocketAddr, ca: Option<&TestCa>) -> Channel {
    match ca {
        Some(ca) => Endpoint::from_shared(format!("https://localhost:{}", addr.port()))
            .unwrap()
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(pem(&ca.der)))
                    .domain_name("localhost"),
            )
            .unwrap()
            .connect_lazy(),
        None => Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect_lazy(),
    }
}

/// gRPC health checks in a loop on a channel from `make`; with `churn`,
/// the channel is replaced that often, so connections keep arriving under
/// whatever spec the listener has at the time.
async fn grpc_loop(
    make: impl Fn() -> Channel,
    churn: Option<Duration>,
    policy: Policy,
    stop: Arc<AtomicBool>,
    t: Arc<Tally>,
) {
    let mut c = HealthClient::new(make());
    let mut since = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        if churn.is_some_and(|d| since.elapsed() >= d) {
            c = HealthClient::new(make());
            since = Instant::now();
        }
        let mut tries = 0;
        loop {
            tries += 1;
            match c.check(HealthCheckRequest::default()).await {
                Ok(_) => {
                    t.ok.fetch_add(1, Ordering::Relaxed);
                    if tries > 1 {
                        t.retried.fetch_add(1, Ordering::Relaxed);
                    }
                    break;
                }
                Err(e) => {
                    if tries == 1 {
                        Tally::add(&t.seen, grpc_kind(&e));
                    }
                    let retry = e.code() == tonic::Code::Unavailable
                        || (policy == Policy::NotSent && (never_sent(&e) || refused_by_goaway(&e)));
                    if tries == 1 && retry {
                        continue;
                    }
                    Tally::add(&t.failed, grpc_kind(&e));
                    break;
                }
            }
        }
    }
}

/// REST over HTTP/1.1 keep-alive with hyper's pooled client: retry once on
/// a request the client says it never sent (hyper retries those itself on
/// reused connections), else count the failure.
async fn rest_loop(
    addr: SocketAddr,
    churn: Option<Duration>,
    stop: Arc<AtomicBool>,
    t: Arc<Tally>,
) {
    let make = || -> Client<HttpConnector, Full<Bytes>> {
        Client::builder(TokioExecutor::new()).build_http()
    };
    let mut client = make();
    let mut since = Instant::now();
    let url = format!("http://{addr}/ok");
    while !stop.load(Ordering::Relaxed) {
        if churn.is_some_and(|d| since.elapsed() >= d) {
            client = make();
            since = Instant::now();
        }
        let r = client
            .request(
                hyper::Request::get(&url)
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await;
        match r {
            Ok(resp) => {
                let _ = resp.into_body().collect().await;
                t.ok.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                let kind = chain(Some(&e));
                Tally::add(&t.seen, kind.clone());
                Tally::add(&t.failed, kind);
            }
        }
    }
}

/// The listener changes of the M0.7 soak against a gateway of our own:
/// certificate rotation, `listener.https.tls` optional/required and
/// `tls.min_version` 1.2/1.3, every `period`, with clients in tight loops:
/// gRPC over TLS on `https` (as the soak's TLS gRPC worker), gRPC over h2c
/// and REST over HTTP/1.1 keep-alive on the plaintext `http` listener.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measured reproduction; takes about a minute"]
#[allow(clippy::too_many_lines)] // one scenario, start to end
async fn soak_changes_measured() {
    let changes: u64 = std::env::var("SPRIG_GWDRAIN_CHANGES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let drain = Duration::from_millis(150);
    let period = Duration::from_millis(400);
    let (ca, dir) = (TestCa::new(), TempDir::new());
    let certs = CertStore::new(Arc::new(SystemClock));
    let (c, k) = dir.write_leaf(&ca.leaf("a", FAR));
    let bundle = certs.bundle("default", &c, &k).unwrap();
    let fe = Arc::new(TestHttp::new());
    let conns = Arc::clone(&fe.conns);
    let mgr = ListenerManager::new(
        Frontends::new().with(fe),
        Arc::new(GatewayContext::new(Arc::new(SystemClock))),
    );
    let free = || {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
    };
    let (ha, pa) = (free(), free());
    let mut https = ListenerSpec {
        tls: TlsMode::Optional,
        cert: Some(bundle.clone()),
        ..ListenerSpec::new("https", Protocol::Http, ha)
    };
    let mut http = ListenerSpec {
        cert: Some(bundle),
        ..ListenerSpec::new("http", Protocol::Http, pa)
    };
    let r = mgr
        .apply(resolution(vec![https.clone(), http.clone()], drain))
        .await;
    assert!(r.rejected.is_empty(), "{:?}", r.rejected);

    let stop = Arc::new(AtomicBool::new(false));
    let tallies: Vec<(String, Arc<Tally>)> = [
        "grpc tls, retry Unavailable",
        "grpc tls, retry Unavailable + not sent",
        "grpc h2c, retry Unavailable",
        "grpc h2c, retry Unavailable + not sent",
        "rest http/1.1 keep-alive",
    ]
    .iter()
    .map(|n| ((*n).to_owned(), Arc::new(Tally::default())))
    .collect();
    // Clients replace their connections every 250 ms, so some are always
    // accepted under the looser spec a stricter change applies to.
    let churn = Some(Duration::from_millis(250));
    let ca = Arc::new(ca);
    let mut tasks = Vec::new();
    for _ in 0..3 {
        for (i, policy) in [Policy::Unavailable, Policy::NotSent]
            .into_iter()
            .enumerate()
        {
            let tls_ca = Arc::clone(&ca);
            tasks.push(tokio::spawn(grpc_loop(
                move || channel(ha, Some(&tls_ca)),
                churn,
                policy,
                Arc::clone(&stop),
                Arc::clone(&tallies[i].1),
            )));
            tasks.push(tokio::spawn(grpc_loop(
                move || channel(pa, None),
                churn,
                policy,
                Arc::clone(&stop),
                Arc::clone(&tallies[2 + i].1),
            )));
        }
        tasks.push(tokio::spawn(rest_loop(
            pa,
            churn,
            Arc::clone(&stop),
            Arc::clone(&tallies[4].1),
        )));
    }

    let started = Instant::now();
    let mut cert_b = false;
    let mut stricter = 0;
    for step in 0..changes {
        tokio::time::sleep(period).await;
        match step % 3 {
            0 => {
                cert_b = !cert_b;
                dir.write_leaf(&ca.leaf(if cert_b { "b" } else { "a" }, FAR));
                certs.poll();
                assert!(matches!(certs.poll()[0].1, PollOutcome::Rotated));
            }
            1 => {
                https.tls = if https.tls == TlsMode::Optional {
                    stricter += 1;
                    TlsMode::Required
                } else {
                    TlsMode::Optional
                };
            }
            _ => {
                let v = if https.min_tls == TlsVersion::V1_2 {
                    stricter += 1;
                    TlsVersion::V1_3
                } else {
                    TlsVersion::V1_2
                };
                https.min_tls = v;
                http.min_tls = v;
            }
        }
        let r = mgr
            .apply(resolution(vec![https.clone(), http.clone()], drain))
            .await;
        assert!(r.rejected.is_empty(), "{:?}", r.rejected);
        if std::env::var_os("SPRIG_GWDRAIN_TRACE").is_some() {
            println!(
                "step {step}: {:?} https {:?}/{:?} conns {}",
                r.applied,
                https.tls,
                https.min_tls,
                conns.load(Ordering::Relaxed)
            );
        }
    }
    tokio::time::sleep(drain * 3).await;
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = t.await;
    }
    println!(
        "soak changes: {changes} changes ({stricter} stricter) in {:.1} s, drain {} ms; {} connections served",
        started.elapsed().as_secs_f64(),
        drain.as_millis(),
        conns.load(Ordering::Relaxed),
    );
    for (n, t) in &tallies {
        println!("  {}", t.line(n));
    }
    mgr.shutdown(Duration::ZERO).await;
}

/// The close itself: a plain accept loop serves every connection with
/// [`super::serve_conn`] and asks each to close (`close_soon`, as a stricter
/// listener change does at `listener.drain_seconds`) `cycles` times, then
/// forces it closed after `force` (`retired`) if it is still open. Clients
/// reconnect, so every failure is the close's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measured reproduction; takes about a minute"]
async fn goaway_close_measured() {
    let cycles: u64 = std::env::var("SPRIG_GWDRAIN_CHANGES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let r = close_race(
        cycles,
        Duration::from_millis(300),
        Duration::from_millis(600),
    )
    .await;
    println!("goaway close: {cycles} closes of every open connection");
    for (n, t) in &r {
        println!("  {}", t.line(n));
    }
}

/// Runs [`goaway_close_measured`]'s scenario; returns the tallies.
async fn close_race(cycles: u64, every: Duration, force: Duration) -> Vec<(String, Arc<Tally>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let spec = Arc::new(spec("http", Protocol::Http));
    // The current "generation": its tokens close every connection it has.
    let gen_tokens: Arc<Mutex<(CancellationToken, CancellationToken)>> = Arc::new(Mutex::new((
        CancellationToken::new(),
        CancellationToken::new(),
    )));
    let grpc = tonic::service::Routes::new(HealthServer::new(TestHealth));
    let accept = {
        let gen_tokens = Arc::clone(&gen_tokens);
        tokio::spawn(async move {
            loop {
                let Ok((sock, peer)) = listener.accept().await else {
                    return;
                };
                let _ = sock.set_nodelay(true);
                let (soft, hard) = gen_tokens.lock().unwrap().clone();
                let conn = ClientConn {
                    stream: MaybeTls::Plain(sock),
                    peer,
                    listener: Arc::clone(&spec),
                    generation: 1,
                    in_protocol_tls: InProtocolTls::NotOffered,
                    retired: hard.clone(),
                    close_soon: soft,
                    transport: Arc::default(),
                    acceptor: None,
                };
                let svc = test_service(grpc.clone());
                tokio::spawn(async move {
                    tokio::select! {
                        biased;
                        () = hard.cancelled() => {}
                        _ = super::serve_conn(conn, svc, 100) => {}
                    }
                });
            }
        })
    };
    let stop = Arc::new(AtomicBool::new(false));
    let tallies: Vec<(String, Arc<Tally>)> = [
        "grpc h2c, retry Unavailable",
        "grpc h2c, retry Unavailable + not sent",
        "rest http/1.1 keep-alive",
    ]
    .iter()
    .map(|n| ((*n).to_owned(), Arc::new(Tally::default())))
    .collect();
    let mut tasks = Vec::new();
    for _ in 0..3 {
        for (i, policy) in [Policy::Unavailable, Policy::NotSent]
            .into_iter()
            .enumerate()
        {
            tasks.push(tokio::spawn(grpc_loop(
                move || channel(addr, None),
                None,
                policy,
                Arc::clone(&stop),
                Arc::clone(&tallies[i].1),
            )));
        }
        tasks.push(tokio::spawn(rest_loop(
            addr,
            None,
            Arc::clone(&stop),
            Arc::clone(&tallies[2].1),
        )));
    }
    for _ in 0..cycles {
        tokio::time::sleep(every).await;
        let (soft, hard) = {
            let mut g = gen_tokens.lock().unwrap();
            std::mem::replace(
                &mut *g,
                (CancellationToken::new(), CancellationToken::new()),
            )
        };
        soft.cancel();
        tokio::spawn(async move {
            tokio::time::sleep(force).await;
            hard.cancel();
        });
    }
    tokio::time::sleep(force).await;
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = t.await;
    }
    accept.abort();
    tallies
}

/// One accepted plaintext connection served by [`super::serve_conn`]; the
/// token asks it to close. The handle ends when the connection does.
async fn one_conn() -> (
    tokio::net::TcpStream,
    CancellationToken,
    tokio::task::JoinHandle<sprig_common::Result<()>>,
) {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = tokio::net::TcpStream::connect(l.local_addr().unwrap())
        .await
        .unwrap();
    let (sock, peer) = l.accept().await.unwrap();
    let soft = CancellationToken::new();
    let conn = ClientConn {
        stream: MaybeTls::Plain(sock),
        peer,
        listener: Arc::new(spec("http", Protocol::Http)),
        generation: 1,
        in_protocol_tls: InProtocolTls::NotOffered,
        retired: CancellationToken::new(),
        close_soon: soft.clone(),
        transport: Arc::default(),
        acceptor: None,
    };
    let grpc = tonic::service::Routes::new(HealthServer::new(TestHealth));
    let h = tokio::spawn(super::serve_conn(conn, test_service(grpc), 100));
    (client, soft, h)
}

/// Sends `GET path` over HTTP/1.1 and reads the response head and body.
async fn h1_get(c: &mut tokio::net::TcpStream, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    c.write_all(format!("GET {path} HTTP/1.1\r\nhost: t\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut buf = Vec::new();
    let mut b = [0u8; 1024];
    while !buf.ends_with(b"ok") {
        let n = tokio::time::timeout(Duration::from_secs(5), c.read(&mut b))
            .await
            .unwrap()
            .unwrap();
        assert!(
            n > 0,
            "closed before the response: {:?}",
            String::from_utf8_lossy(&buf)
        );
        buf.extend_from_slice(&b[..n]);
    }
    String::from_utf8(buf).unwrap().to_ascii_lowercase()
}

async fn eof_within(c: &mut tokio::net::TcpStream, d: Duration) -> bool {
    use tokio::io::AsyncReadExt;
    let mut b = [0u8; 64];
    matches!(
        tokio::time::timeout(d, c.read(&mut b)).await,
        Ok(Ok(0) | Err(_))
    )
}

#[tokio::test]
async fn http1_close_rides_on_the_next_response() {
    let (mut c, soft, h) = one_conn().await;
    assert!(!h1_get(&mut c, "/ok").await.contains("connection: close"));
    soft.cancel();
    // The client's next request is served and told to close; the server
    // closes after it.
    let started = Instant::now();
    assert!(h1_get(&mut c, "/ok").await.contains("connection: close"));
    assert!(eof_within(&mut c, Duration::from_secs(2)).await);
    assert!(started.elapsed() < super::H1_IDLE_GRACE);
    h.await.unwrap().unwrap();
}

#[tokio::test]
async fn an_idle_http1_connection_closes_after_the_grace() {
    let (mut c, soft, h) = one_conn().await;
    assert!(h1_get(&mut c, "/ok").await.contains("200"));
    let started = Instant::now();
    soft.cancel();
    assert!(eof_within(&mut c, Duration::from_secs(5)).await);
    let took = started.elapsed();
    assert!(
        took + Duration::from_millis(50) >= super::H1_IDLE_GRACE,
        "closed after {took:?}"
    );
    h.await.unwrap().unwrap();
}

#[tokio::test]
async fn an_http1_request_in_flight_finishes() {
    let (mut c, soft, h) = one_conn().await;
    let req = tokio::spawn(async move {
        let r = h1_get(&mut c, "/slow").await;
        (r, c)
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    soft.cancel();
    let (r, mut c) = req.await.unwrap();
    assert!(r.contains("200") && r.contains("connection: close"), "{r}");
    assert!(eof_within(&mut c, Duration::from_secs(2)).await);
    h.await.unwrap().unwrap();
}

#[tokio::test]
async fn http2_goaway_lets_streams_in_flight_finish() {
    use hyper_util::rt::TokioIo;
    let (c, soft, h) = one_conn().await;
    let (mut send, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(c))
            .await
            .unwrap();
    let client_conn = tokio::spawn(conn);
    let get = |send: &mut hyper::client::conn::http2::SendRequest<Full<Bytes>>, path: &str| {
        send.send_request(
            hyper::Request::get(format!("http://t{path}"))
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
    };
    assert_eq!(get(&mut send, "/ok").await.unwrap().status(), 200);
    let slow = get(&mut send, "/slow");
    let slow = tokio::spawn(async move {
        let r = slow.await.unwrap();
        let status = r.status();
        let body = r.into_body().collect().await.unwrap().to_bytes();
        (status, body)
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let started = Instant::now();
    soft.cancel();
    // The stream in flight completes after the GOAWAY...
    let (status, body) = slow.await.unwrap();
    assert_eq!((status.as_u16(), &body[..]), (200, &b"ok"[..]));
    // ...and nothing new is accepted: the client sees the connection go.
    tokio::time::timeout(Duration::from_secs(2), client_conn)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(send.ready().await.is_err());
    h.await.unwrap().unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
}
