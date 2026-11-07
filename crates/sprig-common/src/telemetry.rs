//! Tracing setup, the Prometheus recorder and the telemetry endpoints
//! `/metrics`, `/healthz` and `/readyz` (chapters 03, 04 and 47).
//!
//! The full telemetry of chapter 47 (redaction, OTLP export, the audit
//! sink) is not implemented yet.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
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
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    let installed = match cfg.log_format {
        LogFormat::Json => builder
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .with_span_list(false)
            .try_init(),
        LogFormat::Pretty => builder.pretty().try_init(),
    };
    installed.map_err(|e| SprigError::internal(format!("tracing already initialized: {e}")))
}

/// Installs the global Prometheus recorder for the `metrics` macros.
///
/// # Errors
/// `Internal` if a recorder is already installed.
pub fn install_metrics() -> Result<PrometheusHandle> {
    PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| SprigError::internal(format!("metrics recorder: {e}")))
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
        .with_state(ServerState { health, metrics })
}

async fn healthz(State(s): State<ServerState>) -> (StatusCode, &'static str) {
    if s.health.is_live() {
        (StatusCode::OK, "ok\n")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "watchdog stalled\n")
    }
}

async fn readyz(State(s): State<ServerState>) -> (StatusCode, String) {
    let state = s.health.state();
    let code = if state == ProcessState::Ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, format!("{}\n", state.as_str()))
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
        async move {
            let mut every = tokio::time::interval(WATCHDOG_PERIOD);
            loop {
                tokio::select! {
                    () = stop.cancelled() => break,
                    _ = every.tick() => health.tick(),
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
