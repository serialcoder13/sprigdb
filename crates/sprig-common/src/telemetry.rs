//! Tracing setup, the Prometheus recorder and the telemetry endpoints
//! `/metrics`, `/healthz` and `/readyz` (chapters 03, 04 and 47).
//!
//! Chapter 47's traces are in [`trace`]: spans with target
//! [`trace::TARGET`] become W3C traces, tail-sampled in process, kept in a
//! ring served as `/debug/traces` and exported over OTLP. `/readyz?verbose`
//! lists the process identity (pid and start time, so a check can prove no
//! restart happened), every named check ([`Health::set_check`]) and whether
//! `sprigdb.toml` still matches the running configuration
//! ([`Health::watch_config`], chapter 47 "No restarts"). Not here yet: OTLP
//! export of metrics and logs, and log redaction (step 1).

pub mod trace;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::Uri;
use axum::http::StatusCode;
use axum::routing::get;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use crate::config::{LogFormat, TelemetryConfig};
use crate::error::{Result, SprigCode, SprigError};
use crate::time::{Clock, SystemClock};

/// `/healthz` turns 503 when the watchdog hasn't ticked for this long.
pub const WATCHDOG_STALE: Duration = Duration::from_secs(5);
const WATCHDOG_PERIOD: Duration = Duration::from_secs(1);

/// Installs the global `tracing` subscriber: JSON lines or pretty output on
/// standard output, filtered by `bootstrap_log_level`.
///
/// # Errors
/// `ConfigInvalid` for a bad filter; `Internal` if a subscriber is already
/// installed.
pub fn init_tracing(cfg: &TelemetryConfig) -> Result<()> {
    let filter = EnvFilter::try_new(&cfg.bootstrap_log_level).map_err(|e| {
        SprigError::new(
            SprigCode::ConfigInvalid,
            format!(
                "telemetry.bootstrap_log_level {:?}: {e}",
                cfg.bootstrap_log_level
            ),
        )
    })?;
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::layer::SubscriberExt as _;
    use tracing_subscriber::util::SubscriberInitExt as _;
    // Each layer has its own filter: the log filter never hides trace
    // spans, and trace spans pass only to the trace layer and as context.
    let logs = match cfg.log_format {
        LogFormat::Json => tracing_subscriber::fmt::layer()
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .with_span_list(false)
            .with_filter(filter)
            .boxed(),
        LogFormat::Pretty => tracing_subscriber::fmt::layer()
            .pretty()
            .with_filter(filter)
            .boxed(),
    };
    tracing_subscriber::registry()
        .with(logs)
        .with(trace::layer())
        .try_init()
        .map_err(|e| SprigError::internal(format!("tracing already initialized: {e}")))
}

/// Installs the global Prometheus recorder for the `metrics` macros.
///
/// # Errors
/// `Internal` if a recorder is already installed.
pub fn install_metrics() -> Result<PrometheusHandle> {
    builder()?
        .install_recorder()
        .map_err(|e| SprigError::internal(format!("metrics recorder: {e}")))
}

/// Bucket bounds of the SLI histograms, in seconds: they include 0.002
/// (the gateway-overhead SLO) and the cutover-pause range.
pub const SLI_BUCKETS: &[f64] = &[
    0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Histograms the SLOs and alerts read (chapter 47): exported as
/// Prometheus histograms with [`SLI_BUCKETS`], so burn rates can count
/// the share under a threshold. Every other histogram is a summary.
pub const SLI_HISTOGRAMS: &[&str] = &[
    "sprig_gateway_query_duration_seconds",
    "sprig_gateway_overhead_seconds",
    "sprig_gateway_admission_wait_seconds",
    "sprig_backend_pool_wait_seconds",
    "sprig_mover_cutover_hold_seconds",
];

fn builder() -> Result<PrometheusBuilder> {
    use metrics_exporter_prometheus::Matcher;
    let mut b = PrometheusBuilder::new();
    for name in SLI_HISTOGRAMS {
        b = b
            .set_buckets_for_metric(Matcher::Full((*name).to_owned()), SLI_BUCKETS)
            .map_err(|e| SprigError::internal(format!("metrics buckets: {e}")))?;
    }
    Ok(b)
}

/// A process's lifecycle state, as `/readyz` reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ProcessState {
    /// Not yet serving: `/readyz` is 503.
    Starting = 0,
    /// Serving: `/readyz` is 200.
    Ready = 1,
    /// Shutting down: `/readyz` is 503 so load balancers stop sending work.
    Draining = 2,
}

impl ProcessState {
    /// Lowercase name, as reported in `/readyz` and logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            ProcessState::Starting => "starting",
            ProcessState::Ready => "ready",
            ProcessState::Draining => "draining",
        }
    }
}

/// Liveness and readiness, shared between a role and its telemetry server.
#[derive(Clone)]
pub struct Health {
    inner: Arc<HealthInner>,
}

struct HealthInner {
    state: AtomicU8,
    clock: Arc<dyn Clock>,
    origin: std::time::Instant,
    /// Milliseconds since `origin` at the last watchdog tick.
    last_tick_ms: AtomicU64,
    /// When the process started, ms since the Unix epoch.
    started_unix_ms: u64,
    /// Named checks for `/readyz?verbose`.
    checks: Mutex<BTreeMap<String, Check>>,
    /// The configuration file and its text at start.
    config: Mutex<Option<(PathBuf, String)>>,
}

/// One named check of `/readyz?verbose`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Check {
    /// Passing.
    pub ok: bool,
    /// What it saw.
    pub detail: String,
}

impl std::fmt::Debug for Health {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Health")
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

impl Default for Health {
    fn default() -> Self {
        Self::new(Arc::new(SystemClock))
    }
}

impl Health {
    /// A health handle in state `Starting`, with a fresh watchdog tick.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        let origin = clock.now();
        Self {
            inner: Arc::new(HealthInner {
                state: AtomicU8::new(ProcessState::Starting as u8),
                clock,
                origin,
                last_tick_ms: AtomicU64::new(0),
                started_unix_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
                checks: Mutex::new(BTreeMap::new()),
                config: Mutex::new(None),
            }),
        }
    }

    /// The current state.
    #[must_use]
    pub fn state(&self) -> ProcessState {
        match self.inner.state.load(Ordering::Acquire) {
            0 => ProcessState::Starting,
            1 => ProcessState::Ready,
            _ => ProcessState::Draining,
        }
    }

    /// Marks the process ready. Ignored once draining: a draining process
    /// never becomes ready again.
    pub fn set_ready(&self) {
        let _ = self.inner.state.compare_exchange(
            ProcessState::Starting as u8,
            ProcessState::Ready as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// Marks the process draining.
    pub fn set_draining(&self) {
        self.inner
            .state
            .store(ProcessState::Draining as u8, Ordering::Release);
    }

    /// Records a watchdog tick.
    pub fn tick(&self) {
        self.inner
            .last_tick_ms
            .store(self.elapsed_ms(), Ordering::Release);
    }

    /// `true` while the watchdog ticked within [`WATCHDOG_STALE`].
    #[must_use]
    pub fn is_live(&self) -> bool {
        let last = self.inner.last_tick_ms.load(Ordering::Acquire);
        let stale_ms = u64::try_from(WATCHDOG_STALE.as_millis()).unwrap_or(u64::MAX);
        self.elapsed_ms().saturating_sub(last) < stale_ms
    }

    /// Sets the named check `name` (shown by `/readyz?verbose`; it does
    /// not change readiness, which the role's state decides).
    pub fn set_check(&self, name: impl Into<String>, ok: bool, detail: impl Into<String>) {
        self.inner
            .checks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                name.into(),
                Check {
                    ok,
                    detail: detail.into(),
                },
            );
    }

    /// Remembers the configuration file the process started from, so
    /// `/readyz?verbose` reports when the file on disk no longer matches
    /// (the change applies at the next rolling restart).
    pub fn watch_config(&self, path: impl Into<PathBuf>, loaded_text: impl Into<String>) {
        *self
            .inner
            .config
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some((path.into(), loaded_text.into()));
    }

    /// When the process started, ms since the Unix epoch.
    #[must_use]
    pub fn started_unix_ms(&self) -> u64 {
        self.inner.started_unix_ms
    }

    /// The `/readyz?verbose` report: the state, then one line per check,
    /// `[+]` passing and `[-]` failing.
    #[must_use]
    pub fn verbose(&self) -> String {
        use std::fmt::Write as _;
        let mut out = format!("{}\n", self.state().as_str());
        let _ = writeln!(
            out,
            "[+]process pid={} started_unix_ms={} version={}",
            std::process::id(),
            self.inner.started_unix_ms,
            env!("CARGO_PKG_VERSION")
        );
        let _ = writeln!(
            out,
            "[{}]watchdog {}",
            if self.is_live() { '+' } else { '-' },
            if self.is_live() { "ticking" } else { "stalled" }
        );
        let config = self
            .inner
            .config
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some((path, loaded)) = config {
            match std::fs::read_to_string(&path) {
                Ok(now) if now == loaded => {
                    let _ = writeln!(out, "[+]config {} matches the running configuration", path.display());
                }
                Ok(_) => {
                    let _ = writeln!(
                        out,
                        "[-]config drift: {} differs from the running configuration; it applies at the next rolling restart",
                        path.display()
                    );
                }
                Err(e) => {
                    let _ = writeln!(out, "[-]config drift: {} unreadable: {e}", path.display());
                }
            }
        }
        for (name, c) in self
            .inner
            .checks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
        {
            let _ = writeln!(out, "[{}]{name} {}", if c.ok { '+' } else { '-' }, c.detail);
        }
        out
    }

    fn elapsed_ms(&self) -> u64 {
        let d = self
            .inner
            .clock
            .now()
            .saturating_duration_since(self.inner.origin);
        u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
    }
}

#[derive(Clone)]
struct ServerState {
    health: Health,
    metrics: PrometheusHandle,
}

/// The telemetry router: `/metrics`, `/healthz` and `/readyz`.
pub fn router(health: Health, metrics: PrometheusHandle) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_text))
        .route("/debug/traces", get(debug_traces))
        .with_state(ServerState { health, metrics })
}

async fn healthz(State(s): State<ServerState>) -> (StatusCode, &'static str) {
    if s.health.is_live() {
        (StatusCode::OK, "ok\n")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "watchdog stalled\n")
    }
}

async fn readyz(
    State(s): State<ServerState>,
    uri: Uri,
) -> (StatusCode, String) {
    let q = query_params(&uri);
    let state = s.health.state();
    let code = if state == ProcessState::Ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    if q.contains_key("verbose") {
        return (code, s.health.verbose());
    }
    (code, format!("{}\n", state.as_str()))
}

/// The query string's parameters (no percent-decoding: values here are
/// plain words).
fn query_params(uri: &Uri) -> std::collections::HashMap<String, String> {
    uri.query()
        .unwrap_or_default()
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (k.to_owned(), v.to_owned()),
            None => (p.to_owned(), String::new()),
        })
        .collect()
}

/// `/debug/traces`: the most recent kept traces as JSON, newest last;
/// `?attr=key=value` keeps those whose root has that attribute.
async fn debug_traces(uri: Uri) -> ([(axum::http::HeaderName, &'static str); 1], String) {
    let q = query_params(&uri);
    let mut traces = trace::tracer().recent();
    if let Some((k, v)) = q.get("attr").and_then(|a| a.split_once('=')) {
        traces.retain(|t| {
            t.root()
                .is_some_and(|r| r.attributes.iter().any(|(a, b)| a == k && b == v))
        });
    }
    let traces: Vec<&trace::Trace> = traces.iter().map(AsRef::as_ref).collect();
    let body = serde_json::to_string_pretty(&traces).unwrap_or_else(|_| "[]".into());
    ([(axum::http::header::CONTENT_TYPE, "application/json")], body)
}

async fn metrics_text(State(s): State<ServerState>) -> String {
    s.metrics.run_upkeep();
    s.metrics.render()
}

/// Serves the telemetry endpoints on `listener` and ticks the watchdog
/// until `stop` is cancelled. The caller (a binary or role crate) owns the
/// task that runs this.
///
/// # Errors
/// `Internal` if the server fails.
pub async fn serve(
    listener: TcpListener,
    health: Health,
    metrics: PrometheusHandle,
    stop: CancellationToken,
) -> Result<()> {
    let watchdog = {
        let health = health.clone();
        let stop = stop.clone();
        let upkeep = metrics.clone();
        async move {
            let mut every = tokio::time::interval(WATCHDOG_PERIOD);
            loop {
                tokio::select! {
                    () = stop.cancelled() => break,
                    _ = every.tick() => {
                        health.tick();
                        // `install_recorder` runs no upkeep: histogram samples
                        // pile up until something drains them, so a process
                        // nobody scrapes grows without bound (found in the
                        // M0.7 soak: about 12 MB a minute under load).
                        upkeep.run_upkeep();
                    }
                }
            }
        }
    };
    let server = axum::serve(listener, router(health, metrics))
        .with_graceful_shutdown(stop.cancelled_owned());
    let (outcome, ()) = tokio::join!(server, watchdog);
    outcome.map_err(|e| SprigError::internal("telemetry server failed").with_source(e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::ManualClock;

    #[test]
    fn readiness_follows_the_lifecycle() {
        let h = Health::default();
        assert_eq!(h.state(), ProcessState::Starting);
        h.set_ready();
        assert_eq!(h.state(), ProcessState::Ready);
        h.set_draining();
        h.set_ready();
        assert_eq!(h.state(), ProcessState::Draining);
    }

    #[test]
    fn liveness_needs_a_recent_tick() {
        let clock = Arc::new(ManualClock::new());
        let h = Health::new(clock.clone());
        assert!(h.is_live());
        clock.advance(Duration::from_secs(6));
        assert!(!h.is_live());
        h.tick();
        assert!(h.is_live());
    }

    #[tokio::test]
    async fn endpoints_report_health_readiness_and_metrics() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let health = Health::default();
        let handle = PrometheusBuilder::new().build_recorder().handle();
        let stop = CancellationToken::new();
        let server = tokio::spawn(serve(listener, health.clone(), handle, stop.clone()));

        assert_eq!(http_get(addr, "/healthz").await.0, 200);
        assert_eq!(
            http_get(addr, "/readyz").await,
            (503, "starting\n".to_owned())
        );
        health.set_ready();
        assert_eq!(http_get(addr, "/readyz").await.0, 200);
        assert_eq!(http_get(addr, "/metrics").await.0, 200);
        health.set_draining();
        assert_eq!(
            http_get(addr, "/readyz").await,
            (503, "draining\n".to_owned())
        );

        stop.cancel();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn verbose_readiness_shows_identity_checks_and_config_drift() {
        let dir = std::env::temp_dir().join(format!("sprig-readyz-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = dir.join("sprigdb.toml");
        std::fs::write(&cfg, "[node]\ndc = \"dc-a\"\n").unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let health = Health::default();
        health.watch_config(&cfg, "[node]\ndc = \"dc-a\"\n");
        health.set_check("listeners", true, "mysql 127.0.0.1:3306");
        health.set_ready();
        let handle = PrometheusBuilder::new().build_recorder().handle();
        let stop = CancellationToken::new();
        let server = tokio::spawn(serve(listener, health.clone(), handle, stop.clone()));
        let (code, body) = http_get(addr, "/readyz?verbose").await;
        assert_eq!(code, 200);
        assert!(body.starts_with("ready\n"), "{body}");
        assert!(body.contains(&format!("[+]process pid={} ", std::process::id())), "{body}");
        assert!(body.contains("[+]config "), "{body}");
        assert!(body.contains("[+]listeners mysql 127.0.0.1:3306"), "{body}");
        std::fs::write(&cfg, "[node]\ndc = \"dc-b\"\n").unwrap();
        let (code, body) = http_get(addr, "/readyz?verbose").await;
        assert_eq!(code, 200, "drift is reported, not fatal");
        assert!(body.contains("[-]config drift: "), "{body}");
        assert_eq!(http_get(addr, "/readyz").await.1, "ready\n");
        assert_eq!(http_get(addr, "/debug/traces").await.0, 200);
        stop.cancel();
        server.await.unwrap().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A minimal HTTP/1.1 GET, to avoid an HTTP client dependency.
    async fn http_get(addr: std::net::SocketAddr, path: &str) -> (u16, String) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = String::new();
        stream.read_to_string(&mut buf).await.unwrap();
        let status = buf[9..12].parse().unwrap();
        let body = buf
            .split_once("\r\n\r\n")
            .map(|(_, b)| b.to_owned())
            .unwrap_or_default();
        (status, body)
    }
}
