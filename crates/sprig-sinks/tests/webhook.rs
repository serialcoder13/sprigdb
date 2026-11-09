//! The webhook target against an in-process HTTP server (chapter 51,
//! "Testing": a server that refuses, stalls, resets mid-request, returns
//! 5xx, 429 with `Retry-After`, 413 and partial failures).
//!
//! The server checks every request's signature and authentication, records
//! the idempotency keys it accepted, and injects the faults. The target
//! runs under `runtime::run_task` with an in-memory stream source, as the
//! simulation does, on the real clock and real sockets.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::missing_panics_doc,
    clippy::similar_names,
    clippy::needless_pass_by_value,
    clippy::fn_params_excessive_bools
)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode, Version};
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde_json::{Map, Value, json};
use sprig_common::{ErrorKind, Result, SprigCode};
use sprig_sinks::model::{
    Batch, BatchOutcome, ConnKind, ConnRef, ConnectionId, Guarantee, Outcome, Position, SinkDef,
    SinkId, SinkRecord, SinkState, SourceSpec, StartAt, TargetSpec, TaskEpoch, stream_key,
};
use sprig_sinks::policy::Destinations;
use sprig_sinks::runtime::{
    Cancel, Control, DeadInfo, DeadLetters, Directive, Event, PassAll, RunSettings, SourceReader,
    Target, TaskEnd, TaskInfo, TaskParts, run_task,
};
use sprig_sinks::target::webhook::{sprig_signature, standard_signature};
use sprig_sinks::target::{Conn, MemSecrets, Opts, TargetCtx};
use tokio::net::TcpListener;

const SECRET: &str = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw";

// ---------------------------------------------------------------- server

#[derive(Default)]
struct Plan {
    /// Percent of requests answered 503, 429 (Retry-After: 1), reset, or
    /// stalled past the client's timeout.
    p503: u64,
    p429: u64,
    preset: u64,
    pstall: u64,
    /// Percent of records answered "retry" in partial results.
    pretry: u64,
    /// More records than this in one request: 413.
    max_records: usize,
    /// Answer every request with this status (redirects, 404, ...).
    fixed: Option<u16>,
    /// Retry-After seconds on 429 and 503.
    retry_after: Option<u64>,
}

#[derive(Default)]
struct Srv {
    plan: Mutex<Plan>,
    poison: HashSet<String>,
    signature: &'static str,
    /// `bearer:<token>`, `basic:<user>:<pass>`, `header:<name>:<value>`,
    /// `oauth`, or empty.
    auth: String,
    partial: bool,
    down_until: Mutex<Option<Instant>>,
    accepted: Mutex<Vec<String>>,
    requests: AtomicU64,
    bad: Mutex<Vec<String>>,
    tokens: AtomicU64,
    reject_token_once: AtomicBool,
    h2_seen: AtomicBool,
    rng: Mutex<u64>,
    first_429_at: Mutex<Option<Instant>>,
    after_429_at: Mutex<Option<Instant>>,
}

impl Srv {
    fn roll(&self) -> u64 {
        let mut s = self.rng.lock().unwrap();
        *s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *s;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        (z ^ (z >> 31)) % 100
    }

    fn bad(&self, why: String) {
        self.bad.lock().unwrap().push(why);
    }
}

fn reply(code: u16, headers: &[(&str, String)], body: impl Into<Bytes>) -> Response<Full<Bytes>> {
    let mut r = Response::new(Full::new(body.into()));
    *r.status_mut() = StatusCode::from_u16(code).unwrap();
    for (k, v) in headers {
        r.headers_mut().insert(
            hyper::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            v.parse().unwrap(),
        );
    }
    r
}

/// The records' idempotency keys in a body or `ce-id` header.
fn keys_of(req_headers: &hyper::HeaderMap, body: &[u8]) -> Vec<String> {
    if let Some(id) = req_headers.get("ce-id") {
        return vec![id.to_str().unwrap().to_owned()];
    }
    let ct = req_headers
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or_default();
    let items: Vec<Value> = if ct == "application/x-ndjson" {
        body.split(|b| *b == b'\n')
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_slice(l).unwrap())
            .collect()
    } else {
        serde_json::from_slice::<Vec<Value>>(body).unwrap()
    };
    items
        .iter()
        .map(|v| {
            v.get("idempotencyKey")
                .or_else(|| v.get("id"))
                .and_then(Value::as_str)
                .unwrap()
                .to_owned()
        })
        .collect()
}

async fn handle(
    st: Arc<Srv>,
    req: Request<Incoming>,
) -> std::result::Result<Response<Full<Bytes>>, std::io::Error> {
    if req.version() == Version::HTTP_2 {
        st.h2_seen.store(true, Ordering::SeqCst);
    }
    let (parts, body) = req.into_parts();
    let body = body.collect().await.unwrap().to_bytes();
    let h = &parts.headers;
    let hdr = |n: &str| h.get(n).map(|v| v.to_str().unwrap().to_owned());
    if parts.uri.path() == "/token" {
        let want = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode("sprig-acme:s3cr3t")
        );
        if hdr("authorization") != Some(want) {
            st.bad("token request without client credentials".into());
            return Ok(reply(401, &[], ""));
        }
        let form = String::from_utf8_lossy(&body).into_owned();
        if !form.contains("grant_type=client_credentials") || !form.contains("scope=events.write") {
            st.bad(format!("token request body {form}"));
        }
        let n = st.tokens.fetch_add(1, Ordering::SeqCst) + 1;
        return Ok(reply(
            200,
            &[("content-type", "application/json".into())],
            json!({"access_token": format!("tok-{n}"), "expires_in": 3600, "token_type": "Bearer"})
                .to_string(),
        ));
    }
    st.requests.fetch_add(1, Ordering::SeqCst);
    if parts.uri.path() != "/hooks/orders" {
        st.bad(format!("path {}", parts.uri.path()));
    }
    // Authentication.
    let a = st.auth.clone();
    let authz = hdr("authorization");
    if a == "oauth" {
        let ok = authz
            .as_deref()
            .is_some_and(|v| v.starts_with("Bearer tok-"));
        if !ok {
            st.bad(format!("oauth header {authz:?}"));
            return Ok(reply(401, &[], ""));
        }
        if st.reject_token_once.swap(false, Ordering::SeqCst) {
            return Ok(reply(401, &[], "expired"));
        }
    } else if let Some(t) = a.strip_prefix("bearer:") {
        if authz.as_deref() != Some(&format!("Bearer {t}")) {
            st.bad(format!("bearer {authz:?}"));
            return Ok(reply(401, &[], ""));
        }
    } else if let Some(up) = a.strip_prefix("basic:") {
        let want = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(up)
        );
        if authz != Some(want) {
            st.bad(format!("basic {authz:?}"));
            return Ok(reply(401, &[], ""));
        }
    } else if let Some(nv) = a.strip_prefix("header:") {
        let (n, v) = nv.split_once(':').unwrap();
        if hdr(n).as_deref() != Some(v) {
            st.bad(format!("header {n}"));
            return Ok(reply(403, &[], ""));
        }
    }
    // Signature.
    match st.signature {
        "sprig" => {
            let v = hdr("sprig-signature").unwrap_or_default();
            let t: u64 = v
                .strip_prefix("t=")
                .and_then(|r| r.split(',').next())
                .and_then(|t| t.parse().ok())
                .unwrap_or(0);
            if v != sprig_signature(SECRET.as_bytes(), t, &body) {
                st.bad(format!("sprig signature {v}"));
            }
        }
        "standard" => {
            let id = hdr("webhook-id").unwrap_or_default();
            let ts: u64 = hdr("webhook-timestamp")
                .and_then(|t| t.parse().ok())
                .unwrap_or(0);
            let v = hdr("webhook-signature").unwrap_or_default();
            if v != standard_signature(SECRET, &id, ts, &body)
                || id != hdr("sprig-idempotency-key").unwrap_or_default()
            {
                st.bad(format!("standard signature {v}"));
            }
        }
        _ => {}
    }
    for n in [
        "sprig-idempotency-key",
        "sprig-sink",
        "sprig-source",
        "traceparent",
    ] {
        if hdr(n).is_none() {
            st.bad(format!("missing {n}"));
        }
    }
    // Faults.
    let down = *st.down_until.lock().unwrap();
    if down.is_some_and(|t| Instant::now() < t) {
        return Ok(reply(503, &[], "down"));
    }
    let (fixed, p503, p429, preset, pstall, pretry, max, ra) = {
        let p = st.plan.lock().unwrap();
        (
            p.fixed,
            p.p503,
            p.p429,
            p.preset,
            p.pstall,
            p.pretry,
            p.max_records,
            p.retry_after,
        )
    };
    if let Some(code) = fixed {
        let mut hs = vec![];
        if (300..400).contains(&code) {
            hs.push(("location", "http://example.org/elsewhere".to_owned()));
        }
        return Ok(reply(code, &hs, "fixed"));
    }
    let ra_hdr: Vec<(&str, String)> = ra
        .map(|s| ("retry-after", s.to_string()))
        .into_iter()
        .collect();
    let roll = st.roll();
    if roll < p503 {
        return Ok(reply(503, &ra_hdr, "busy"));
    }
    if roll < p503 + p429 {
        let mut f = st.first_429_at.lock().unwrap();
        if f.is_none() {
            *f = Some(Instant::now());
        }
        return Ok(reply(
            429,
            &[("retry-after", ra.unwrap_or(1).to_string())],
            "slow down",
        ));
    }
    if roll < p503 + p429 + preset {
        return Err(std::io::Error::other("reset mid-request"));
    }
    if roll < p503 + p429 + preset + pstall {
        tokio::time::sleep(Duration::from_secs(3)).await;
        return Ok(reply(200, &[], ""));
    }
    {
        let f = st.first_429_at.lock().unwrap();
        let mut a = st.after_429_at.lock().unwrap();
        if f.is_some() && a.is_none() {
            *a = Some(Instant::now());
        }
    }
    let keys = keys_of(h, &body);
    if max > 0 && keys.len() > max {
        return Ok(reply(413, &[], "too large"));
    }
    let has_poison = keys.iter().any(|k| st.poison.contains(k));
    if st.partial {
        let mut results = Vec::new();
        let mut acc = st.accepted.lock().unwrap();
        let mut held: HashSet<String> = HashSet::new();
        for k in &keys {
            if st.poison.contains(k) {
                results.push(json!({"status": "fail", "reason": "invalid order"}));
                continue;
            }
            // Keeps per-key order: the record's partition stands in for
            // its key here.
            let part = k.split('/').nth(2).unwrap().to_owned();
            if held.contains(&part) || st.roll() < pretry {
                held.insert(part);
                results.push(json!({"status": "retry", "reason": "busy shard"}));
                continue;
            }
            acc.push(k.clone());
            results.push(json!({"status": "ok"}));
        }
        return Ok(reply(
            207,
            &[("content-type", "application/json".into())],
            json!({ "results": results }).to_string(),
        ));
    }
    if has_poison {
        return Ok(reply(422, &[], "invalid order"));
    }
    st.accepted.lock().unwrap().extend(keys);
    Ok(reply(200, &[], ""))
}

/// Serves `st` on a fresh 127.0.0.1 port, plain or TLS (ALPN h2 and
/// HTTP/1.1), HTTP/1.1 and h2 (h2c by prior knowledge) on one port.
async fn serve(st: Arc<Srv>, tls: Option<Arc<rustls::ServerConfig>>) -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((s, _)) = l.accept().await else {
                return;
            };
            let st = Arc::clone(&st);
            let tls = tls.clone();
            tokio::spawn(async move {
                let svc = hyper::service::service_fn(move |r| handle(Arc::clone(&st), r));
                let b = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
                match tls {
                    Some(cfg) => {
                        let Ok(s) = tokio_rustls::TlsAcceptor::from(cfg).accept(s).await else {
                            return;
                        };
                        let _ = b.serve_connection(TokioIo::new(s), svc).await;
                    }
                    None => {
                        let _ = b.serve_connection(TokioIo::new(s), svc).await;
                    }
                }
            });
        }
    });
    addr
}

/// A self-signed certificate for 127.0.0.1: the server's config and the
/// client's `ca_pem`.
fn tls_pair() -> (Arc<rustls::ServerConfig>, String) {
    let ck =
        rcgen::generate_simple_self_signed(vec!["127.0.0.1".into(), "localhost".into()]).unwrap();
    let pem = ck.cert.pem();
    let chain = vec![ck.cert.der().clone()];
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(
        ck.signing_key.serialize_der(),
    ));
    let mut cfg = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(chain, key)
    .unwrap();
    cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    (Arc::new(cfg), pem)
}

// ---------------------------------------------------------------- the task side

const PARTS: u32 = 3;

fn rec(partition: u32, offset: u64) -> SinkRecord {
    SinkRecord {
        key: Some(format!("k{partition}").into_bytes()),
        value: Some(format!("{{\"order\":{offset},\"p\":{partition}}}").into_bytes()),
        headers: vec![("type".into(), b"order.paid".to_vec())],
        op: None,
        before: None,
        after: None,
        ts_ms: 1_762_000_000_000,
        idem_key: stream_key(1, partition, offset, 0),
        pos: Position::Stream {
            stream: 1,
            partition,
            offset,
            epoch: 0,
        },
    }
}

struct Stream {
    len: u64,
    committed: Mutex<HashMap<u32, u64>>,
}

struct Reader {
    s: Arc<Stream>,
    at: HashMap<u32, u64>,
}

#[async_trait]
impl SourceReader for Reader {
    async fn next_batch(
        &mut self,
        max_records: usize,
        _max_bytes: u64,
        wait: Duration,
    ) -> Result<Vec<SinkRecord>> {
        let mut out = Vec::new();
        for p in 0..PARTS {
            let at = self.at.entry(p).or_insert(0);
            while out.len() < max_records && *at < self.s.len {
                out.push(rec(p, *at));
                *at += 1;
            }
        }
        if out.is_empty() {
            tokio::time::sleep(wait).await;
        }
        Ok(out)
    }

    async fn settle(&mut self, done: &[Position]) -> Result<()> {
        let mut c = self.s.committed.lock().unwrap();
        for p in done {
            if let Position::Stream {
                partition, offset, ..
            } = p
            {
                let e = c.entry(*partition).or_insert(0);
                *e = (*e).max(offset + 1);
            }
        }
        Ok(())
    }

    async fn seek_after(&mut self, _pos: &Position) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct Dead(Mutex<Vec<(String, String, String)>>);

#[async_trait]
impl DeadLetters for Dead {
    async fn write(&self, r: &SinkRecord, info: &DeadInfo) -> Result<()> {
        self.0.lock().unwrap().push((
            r.idem_key.clone(),
            info.class.to_owned(),
            info.reason.clone(),
        ));
        Ok(())
    }
}

#[derive(Default)]
struct Ctl(Mutex<Vec<Event>>);

#[async_trait]
impl Control for Ctl {
    async fn poll(&self) -> Result<Directive> {
        Ok(Directive::Run)
    }
    async fn report(&self, ev: Event) -> Result<()> {
        if !matches!(ev, Event::Delivered(_)) {
            self.0.lock().unwrap().push(ev);
        }
        Ok(())
    }
    async fn check_epoch(&self) -> Result<()> {
        Ok(())
    }
}

fn sink() -> SinkDef {
    SinkDef {
        id: SinkId(7),
        db_id: 1,
        name: "orders_to_billing".into(),
        source: SourceSpec::Stream {
            id: 1,
            name: "order_events".into(),
            partitions: PARTS,
        },
        filter_sql: None,
        projection: None,
        transform: None,
        target: TargetSpec::Webhook {
            path: Some("/hooks/orders".into()),
        },
        connections: vec![ConnRef::Tenant {
            id: ConnectionId(1),
            name: "billing_api".into(),
        }],
        guarantee: Guarantee::AtLeastOnce,
        dead_letter: None,
        start_at: StartAt::Latest,
        owner_user_id: 1,
        state: SinkState::Running,
        state_reason: None,
        definition_ver: 1,
        options: BTreeMap::new(),
    }
}

struct Setup<'a> {
    url: String,
    allow: Vec<String>,
    conn_opts: Vec<(&'a str, String)>,
    secrets: Vec<(&'a str, &'a str)>,
    opts: Vec<(&'a str, &'a str)>,
}

fn ctx(s: &Setup<'_>) -> TargetCtx {
    let mut options = Map::new();
    options.insert("url".into(), Value::String(s.url.clone()));
    for (k, v) in &s.conn_opts {
        options.insert((*k).into(), Value::String(v.clone()));
    }
    let allow: Vec<&str> = s.allow.iter().map(String::as_str).collect();
    TargetCtx {
        task: TaskInfo {
            tenant: 1,
            sink: 7,
            task_no: 0,
            epoch: TaskEpoch(1),
        },
        sink: Arc::new(sink()),
        conn: Some(Conn {
            name: "billing_api".into(),
            kind: ConnKind::Webhook,
            options,
            secrets: Arc::new(MemSecrets::of(&s.secrets)),
            dest: Arc::new(Destinations::allow_list(&allow)),
        }),
        extra: Vec::new(),
        opts: Opts(
            s.opts
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        ),
        db_name: "shop".into(),
    }
}

fn settings(lanes: u16, batch: usize) -> RunSettings {
    RunSettings {
        lanes,
        batch_max_records: batch,
        batch_max_wait: Duration::from_millis(20),
        retry_initial: Duration::from_millis(20),
        retry_max: Duration::from_millis(2000),
        record_max_attempts: 50,
        delivery_timeout: Duration::from_secs(10),
        ..RunSettings::default()
    }
}

struct Run {
    dead: Vec<(String, String, String)>,
    events: Vec<Event>,
    secs: f64,
}

/// Runs a task over `n` records per partition until all are settled.
async fn run(target: Box<dyn Target>, n: u64, s: RunSettings, limit: Duration) -> Run {
    let stream = Arc::new(Stream {
        len: n,
        committed: Mutex::new(HashMap::new()),
    });
    let dead = Arc::new(Dead::default());
    let ctl = Arc::new(Ctl::default());
    let parts = TaskParts {
        info: TaskInfo {
            tenant: 1,
            sink: 7,
            task_no: 0,
            epoch: TaskEpoch(1),
        },
        source: Box::new(Reader {
            s: Arc::clone(&stream),
            at: HashMap::new(),
        }),
        target,
        dead: Arc::clone(&dead) as Arc<dyn DeadLetters>,
        control: Arc::clone(&ctl) as Arc<dyn Control>,
        prepare: Arc::new(PassAll),
        settings: s,
        seed: Some(11),
        reject_via_source: false,
    };
    let cancel = Cancel::new();
    let started = Instant::now();
    let h = tokio::spawn(run_task(parts, cancel.clone()));
    loop {
        let done = {
            let c = stream.committed.lock().unwrap();
            (0..PARTS).all(|p| c.get(&p).copied().unwrap_or(0) >= n)
        };
        if done {
            break;
        }
        assert!(
            started.elapsed() < limit,
            "no progress: committed {:?}, events {:?}, dead {}",
            stream.committed.lock().unwrap(),
            ctl.0.lock().unwrap(),
            dead.0.lock().unwrap().len()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let secs = started.elapsed().as_secs_f64();
    cancel.cancel();
    assert_eq!(h.await.unwrap().unwrap(), TaskEnd::Cancelled);
    Run {
        dead: dead.0.lock().unwrap().clone(),
        events: ctl.0.lock().unwrap().clone(),
        secs,
    }
}

/// Every record accepted at least once or dead-lettered as poison;
/// returns (accepted, duplicates).
fn check_delivery(st: &Srv, n: u64, r: &Run) -> (usize, usize) {
    assert!(
        st.bad.lock().unwrap().is_empty(),
        "{:?}",
        st.bad.lock().unwrap()
    );
    let acc = st.accepted.lock().unwrap();
    let set: HashSet<&String> = acc.iter().collect();
    for p in 0..PARTS {
        for o in 0..n {
            let k = stream_key(1, p, o, 0);
            if st.poison.contains(&k) {
                assert!(!set.contains(&k), "poison {k} accepted");
                assert!(
                    r.dead.iter().any(|d| d.0 == k),
                    "poison {k} not dead-lettered"
                );
            } else {
                assert!(set.contains(&k), "{k} never accepted");
            }
        }
    }
    // Only poison records are dead letters.
    for d in &r.dead {
        assert!(st.poison.contains(&d.0), "{} dead-lettered: {}", d.0, d.2);
    }
    // Per-key order of first acceptance.
    let mut seen = HashSet::new();
    let mut last: HashMap<String, u64> = HashMap::new();
    for k in acc.iter() {
        if !seen.insert(k.clone()) {
            continue;
        }
        let p = k.split('/').nth(2).unwrap().to_owned();
        let o: u64 = k.split('/').nth(3).unwrap().parse().unwrap();
        if let Some(prev) = last.get(&p) {
            assert!(o > *prev, "partition {p}: {o} after {prev}");
        }
        last.insert(p, o);
    }
    (acc.len(), acc.len() - set.len())
}

fn poison_set(n: u64) -> HashSet<String> {
    (0..PARTS)
        .flat_map(|p| [n / 3, 2 * n / 3].map(|o| stream_key(1, p, o + u64::from(p), 0)))
        .collect()
}

// ---------------------------------------------------------------- tests

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn faults_over_http1_json_with_sprig_signatures() {
    let n = 700;
    let st = Arc::new(Srv {
        plan: Mutex::new(Plan {
            p503: 4,
            p429: 2,
            preset: 2,
            pstall: 1,
            max_records: 40,
            retry_after: Some(0),
            ..Plan::default()
        }),
        poison: poison_set(n),
        signature: "sprig",
        auth: "bearer:t0ken".into(),
        rng: Mutex::new(5),
        ..Srv::default()
    });
    let addr = serve(Arc::clone(&st), None).await;
    let c = ctx(&Setup {
        url: format!("http://127.0.0.1:{}/", addr.port()),
        allow: vec![format!("127.0.0.1:{}", addr.port())],
        conn_opts: vec![("auth", "bearer".into())],
        secrets: vec![("token", "t0ken"), ("signing_secret", SECRET)],
        opts: vec![
            ("format", "json"),
            ("http2", "off"),
            ("http_timeout_ms", "1000"),
        ],
    });
    let t = sprig_sinks::target::webhook::build(&c).unwrap();
    let r = run(t, n, settings(4, 64), Duration::from_secs(120)).await;
    let (acc, dups) = check_delivery(&st, n, &r);
    assert_eq!(r.dead.len(), 6, "{:?}", r.dead);
    eprintln!(
        "http1 json with faults: {} records, {acc} accepted ({dups} duplicates), {} requests, 6 poison dead-lettered alone, {:.2} s",
        n * u64::from(PARTS),
        st.requests.load(Ordering::SeqCst),
        r.secs
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2_over_tls_with_oauth2_standard_signatures_and_partial_results() {
    let n = 400;
    let (scfg, pem) = tls_pair();
    let st = Arc::new(Srv {
        plan: Mutex::new(Plan {
            p503: 3,
            pretry: 5,
            ..Plan::default()
        }),
        poison: poison_set(n),
        signature: "standard",
        auth: "oauth".into(),
        partial: true,
        reject_token_once: AtomicBool::new(true),
        rng: Mutex::new(9),
        ..Srv::default()
    });
    let addr = serve(Arc::clone(&st), Some(scfg)).await;
    let c = ctx(&Setup {
        url: format!("https://127.0.0.1:{}", addr.port()),
        allow: vec![format!("127.0.0.1:{}", addr.port())],
        conn_opts: vec![
            ("auth", "oauth2_client_credentials".into()),
            (
                "token_url",
                format!("https://127.0.0.1:{}/token", addr.port()),
            ),
            ("scope", "events.write".into()),
            ("ca_pem", pem),
        ],
        secrets: vec![
            ("client_id", "sprig-acme"),
            ("client_secret", "s3cr3t"),
            ("signing_secret", SECRET),
        ],
        opts: vec![
            ("format", "ndjson"),
            ("signature", "standard"),
            ("partial_results", "on"),
        ],
    });
    let t = sprig_sinks::target::webhook::build(&c).unwrap();
    let r = run(t, n, settings(4, 50), Duration::from_secs(120)).await;
    let (acc, dups) = check_delivery(&st, n, &r);
    assert!(st.h2_seen.load(Ordering::SeqCst), "ALPN chose HTTP/2");
    assert_eq!(
        st.tokens.load(Ordering::SeqCst),
        2,
        "one token, refreshed once after a 401"
    );
    assert_eq!(r.dead.len(), 6);
    eprintln!(
        "h2 over TLS, oauth2, standard signatures, partial results: {acc} accepted ({dups} duplicates), {:.2} s",
        r.secs
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2c_prior_knowledge_cloudevents_and_basic_auth() {
    let n = 200;
    let st = Arc::new(Srv {
        signature: "none",
        auth: "basic:sprig:pa:ss".into(),
        rng: Mutex::new(3),
        ..Srv::default()
    });
    let addr = serve(Arc::clone(&st), None).await;
    let c = ctx(&Setup {
        url: format!("http://127.0.0.1:{}", addr.port()),
        allow: vec![format!("127.0.0.1:{}", addr.port())],
        conn_opts: vec![("auth", "basic".into())],
        secrets: vec![("username", "sprig"), ("password", "pa:ss")],
        opts: vec![
            ("format", "cloudevents"),
            ("signature", "none"),
            ("http2", "prior_knowledge"),
        ],
    });
    let t = sprig_sinks::target::webhook::build(&c).unwrap();
    let r = run(t, n, settings(4, 100), Duration::from_secs(60)).await;
    check_delivery(&st, n, &r);
    assert!(st.h2_seen.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binary_cloudevents_one_request_per_record_with_a_header_key() {
    let n = 60;
    let st = Arc::new(Srv {
        plan: Mutex::new(Plan {
            p503: 5,
            ..Plan::default()
        }),
        poison: poison_set(n),
        signature: "sprig",
        auth: "header:x-api-key:k-123".into(),
        rng: Mutex::new(4),
        ..Srv::default()
    });
    let addr = serve(Arc::clone(&st), None).await;
    let c = ctx(&Setup {
        url: format!("http://127.0.0.1:{}", addr.port()),
        allow: vec![format!("127.0.0.1:{}", addr.port())],
        conn_opts: vec![
            ("auth", "header".into()),
            ("auth_header", "X-Api-Key".into()),
        ],
        secrets: vec![("header_value", "k-123"), ("signing_secret", SECRET)],
        opts: vec![("format", "cloudevents_binary")],
    });
    let t = sprig_sinks::target::webhook::build(&c).unwrap();
    let r = run(t, n, settings(2, 20), Duration::from_secs(60)).await;
    check_delivery(&st, n, &r);
    assert_eq!(r.dead.len(), 6);
    assert!(st.requests.load(Ordering::SeqCst) >= n * u64::from(PARTS));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_after_is_honoured() {
    let st = Arc::new(Srv {
        plan: Mutex::new(Plan {
            p429: 100,
            retry_after: Some(2),
            ..Plan::default()
        }),
        signature: "none",
        rng: Mutex::new(1),
        ..Srv::default()
    });
    let addr = serve(Arc::clone(&st), None).await;
    let st2 = Arc::clone(&st);
    // The first answer is 429 with Retry-After: 2; then the server takes
    // everything.
    tokio::spawn(async move {
        loop {
            if st2.first_429_at.lock().unwrap().is_some() {
                st2.plan.lock().unwrap().p429 = 0;
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });
    let c = ctx(&Setup {
        url: format!("http://127.0.0.1:{}", addr.port()),
        allow: vec![format!("127.0.0.1:{}", addr.port())],
        conn_opts: vec![],
        secrets: vec![],
        opts: vec![("signature", "none")],
    });
    let t = sprig_sinks::target::webhook::build(&c).unwrap();
    let r = run(t, 5, settings(1, 100), Duration::from_secs(30)).await;
    check_delivery(&st, 5, &r);
    let first = st.first_429_at.lock().unwrap().unwrap();
    let next = st.after_429_at.lock().unwrap().unwrap();
    let waited = next.duration_since(first);
    assert!(
        waited >= Duration::from_millis(1990),
        "waited only {waited:?} after Retry-After: 2"
    );
    eprintln!("Retry-After: 2 → the next request came {waited:?} later (backoff alone: ≤ 20 ms)");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_outage_pauses_probes_and_resumes() {
    let st = Arc::new(Srv {
        signature: "none",
        rng: Mutex::new(2),
        ..Srv::default()
    });
    *st.down_until.lock().unwrap() = Some(Instant::now() + Duration::from_millis(1500));
    let addr = serve(Arc::clone(&st), None).await;
    let c = ctx(&Setup {
        url: format!("http://127.0.0.1:{}", addr.port()),
        allow: vec![format!("127.0.0.1:{}", addr.port())],
        conn_opts: vec![],
        secrets: vec![],
        opts: vec![("signature", "none")],
    });
    let t = sprig_sinks::target::webhook::build(&c).unwrap();
    let mut s = settings(4, 50);
    s.pause_after = Duration::from_millis(300);
    s.probe_interval = Duration::from_millis(250);
    s.retry_max = Duration::from_millis(100);
    let r = run(t, 50, s, Duration::from_secs(30)).await;
    check_delivery(&st, 50, &r);
    assert!(
        matches!(r.events.first(), Some(Event::Failing(_))),
        "{:?}",
        r.events
    );
    assert!(
        r.events
            .iter()
            .any(|e| matches!(e, Event::PausedByFailure(_))),
        "{:?}",
        r.events
    );
    assert!(
        matches!(r.events.last(), Some(Event::Recovered)),
        "{:?}",
        r.events
    );
}

fn batch_of(n: u64) -> Batch {
    Batch::new((0..n).map(|o| rec(0, o)).collect(), 0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn statuses_are_classified() {
    let st = Arc::new(Srv {
        signature: "none",
        auth: "bearer:right".into(),
        ..Srv::default()
    });
    let addr = serve(Arc::clone(&st), None).await;
    let mk = |token: &'static str| {
        sprig_sinks::target::webhook::build(&ctx(&Setup {
            url: format!("http://127.0.0.1:{}", addr.port()),
            allow: vec![format!("127.0.0.1:{}", addr.port())],
            conn_opts: vec![("auth", "bearer".into())],
            secrets: vec![("token", token)],
            opts: vec![("signature", "none")],
        }))
        .unwrap()
    };
    let t = mk("right");
    let cases: [(u16, &str); 6] = [
        (302, "fail"),
        (404, "fail"),
        (422, "fail"),
        (413, "too_large"),
        (503, "retry"),
        (408, "retry"),
    ];
    for (code, want) in cases {
        st.plan.lock().unwrap().fixed = Some(code);
        let o = t.deliver(&batch_of(3)).await.unwrap();
        let got = match &o {
            BatchOutcome::All(Outcome::Fail { .. }) => "fail",
            BatchOutcome::All(Outcome::Retry { .. }) => "retry",
            BatchOutcome::TooLarge => "too_large",
            _ => "other",
        };
        assert_eq!(got, want, "HTTP {code}: {o:?}");
    }
    st.plan.lock().unwrap().fixed = None;
    // Rejected credentials are destination-wide: the sink pauses.
    let wrong = mk("wrong");
    assert!(matches!(
        wrong.deliver(&batch_of(2)).await.unwrap(),
        BatchOutcome::All(Outcome::Retry { .. })
    ));
    st.bad.lock().unwrap().clear();
    assert_eq!(
        t.deliver(&batch_of(2)).await.unwrap(),
        BatchOutcome::All(Outcome::Ok)
    );
    // Nothing listening: a retryable destination-wide error.
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    let gone = sprig_sinks::target::webhook::build(&ctx(&Setup {
        url: format!("http://127.0.0.1:{port}"),
        allow: vec![format!("127.0.0.1:{port}")],
        conn_opts: vec![],
        secrets: vec![],
        opts: vec![("signature", "none")],
    }))
    .unwrap();
    let e = gone.deliver(&batch_of(1)).await.unwrap_err();
    assert_eq!(e.kind, ErrorKind::Sprig(SprigCode::SinkDeliveryFailed));
    assert!(e.retryable);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn addresses_are_checked() {
    let st = Arc::new(Srv {
        signature: "none",
        ..Srv::default()
    });
    let addr = serve(Arc::clone(&st), None).await;
    let p = addr.port();
    let build = |url: String, allow: Vec<String>| {
        sprig_sinks::target::webhook::build(&ctx(&Setup {
            url,
            allow,
            conn_opts: vec![],
            secrets: vec![],
            opts: vec![("signature", "none")],
        }))
        .unwrap()
    };
    // Not on the allowlist.
    let t = build(
        format!("http://127.0.0.1:{p}"),
        vec!["billing.example.com".into()],
    );
    let e = t.deliver(&batch_of(1)).await.unwrap_err();
    assert_eq!(
        e.kind,
        ErrorKind::Sprig(SprigCode::SinkDestinationNotAllowed),
        "{e}"
    );
    // Listed by name, but the name resolves to a loopback address.
    let t = build(
        format!("http://localhost:{p}"),
        vec![format!("localhost:{p}")],
    );
    let e = t.deliver(&batch_of(1)).await.unwrap_err();
    assert_eq!(
        e.kind,
        ErrorKind::Sprig(SprigCode::SinkDestinationNotAllowed),
        "{e}"
    );
    assert!(e.message.contains("internal address"), "{e}");
    // The address itself listed: allowed.
    let t = build(
        format!("http://127.0.0.1:{p}"),
        vec![format!("127.0.0.1:{p}")],
    );
    assert_eq!(
        t.deliver(&batch_of(1)).await.unwrap(),
        BatchOutcome::All(Outcome::Ok)
    );
    // Debug output never shows a credential.
    let c = ctx(&Setup {
        url: format!("http://127.0.0.1:{p}"),
        allow: vec![],
        conn_opts: vec![("auth", "bearer".into())],
        secrets: vec![("token", "very-secret-token")],
        opts: vec![("signature", "none")],
    });
    let t = sprig_sinks::target::webhook::WebhookTarget::new(&c).unwrap();
    assert!(!format!("{t:?} {c:?}").contains("very-secret-token"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn throughput() {
    for (label, tls, h2) in [("HTTP/1.1", false, "off"), ("h2 over TLS", true, "auto")] {
        let n = 10_000;
        let st = Arc::new(Srv {
            signature: "sprig",
            ..Srv::default()
        });
        let (scfg, pem) = tls_pair();
        let addr = serve(Arc::clone(&st), tls.then_some(scfg)).await;
        let scheme = if tls { "https" } else { "http" };
        let c = ctx(&Setup {
            url: format!("{scheme}://127.0.0.1:{}", addr.port()),
            allow: vec![format!("127.0.0.1:{}", addr.port())],
            conn_opts: vec![("ca_pem", pem)],
            secrets: vec![("signing_secret", SECRET)],
            opts: vec![("http2", h2)],
        });
        let t = sprig_sinks::target::webhook::build(&c).unwrap();
        let r = run(t, n, settings(8, 500), Duration::from_secs(120)).await;
        let (acc, _) = check_delivery(&st, n, &r);
        eprintln!(
            "throughput, {label}: {acc} records in {:.2} s = {:.0} records/s ({} requests)",
            r.secs,
            acc as f64 / r.secs,
            st.requests.load(Ordering::SeqCst)
        );
    }
}
