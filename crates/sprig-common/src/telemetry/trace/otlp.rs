//! OTLP export of kept traces: OTLP over HTTP with JSON encoding
//! (`POST <telemetry.otlp_endpoint>/v1/traces`), which every OpenTelemetry
//! collector accepts on port 4318.
//!
//! Traces queue in memory (at most [`QUEUE`]); a background task sends
//! them once a second. When the collector is down a batch is retried for
//! 30 s and then dropped and counted in `sprig_telemetry_spans_dropped_total`
//! (chapter 47, "Failure handling": `/metrics` is pulled, so unaffected).
//! The endpoint changes live ([`OtlpExporter::set_endpoint`]); `None` stops
//! sending and drops what is queued. Plain HTTP only in R0: a collector in
//! the same DC.

use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::Full;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{Trace, TraceExporter, dropped};

/// Traces queued before new ones are dropped.
pub const QUEUE: usize = 1024;
const PERIOD: Duration = Duration::from_secs(1);
const RETRY_FOR: Duration = Duration::from_secs(30);
const BATCH: usize = 64;

/// Sends kept traces to an OTLP/HTTP collector.
pub struct OtlpExporter {
    tx: mpsc::Sender<Arc<Trace>>,
    endpoint: Arc<RwLock<Option<String>>>,
}

impl std::fmt::Debug for OtlpExporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtlpExporter")
            .field("endpoint", &*self.endpoint.read().unwrap_or_else(PoisonError::into_inner))
            .finish_non_exhaustive()
    }
}

impl OtlpExporter {
    /// Starts the sender task; it stops with `stop`. `resource` holds the
    /// resource attributes (`service.name`, `sprig.dc`, `sprig.node`).
    #[must_use]
    pub fn spawn(
        endpoint: Option<String>,
        resource: Vec<(String, String)>,
        stop: CancellationToken,
    ) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(QUEUE);
        let endpoint = Arc::new(RwLock::new(endpoint));
        tokio::spawn(send_loop(rx, Arc::clone(&endpoint), resource, stop));
        Arc::new(Self { tx, endpoint })
    }

    /// Changes the collector (`None`: stop exporting).
    pub fn set_endpoint(&self, endpoint: Option<String>) {
        *self
            .endpoint
            .write()
            .unwrap_or_else(PoisonError::into_inner) = endpoint.filter(|e| !e.is_empty());
    }
}

impl TraceExporter for OtlpExporter {
    fn export(&self, trace: &Arc<Trace>) {
        if self
            .endpoint
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none()
        {
            return;
        }
        if self.tx.try_send(Arc::clone(trace)).is_err() {
            dropped(trace.spans.len() as u64);
        }
    }
}

async fn send_loop(
    mut rx: mpsc::Receiver<Arc<Trace>>,
    endpoint: Arc<RwLock<Option<String>>>,
    resource: Vec<(String, String)>,
    stop: CancellationToken,
) {
    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();
    let failing_since: Mutex<Option<Instant>> = Mutex::new(None);
    let mut batch: Vec<Arc<Trace>> = Vec::new();
    let mut every = tokio::time::interval(PERIOD);
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = every.tick() => {}
        }
        while batch.len() < BATCH {
            match rx.try_recv() {
                Ok(t) => batch.push(t),
                Err(_) => break,
            }
        }
        if batch.is_empty() {
            continue;
        }
        let target = endpoint
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let Some(target) = target else {
            drop_batch(&mut batch);
            continue;
        };
        let body = Bytes::from(encode(&batch, &resource).to_string());
        let url = format!("{}/v1/traces", target.trim_end_matches('/'));
        let sent = match hyper::Request::post(&url)
            .header("content-type", "application/json")
            .body(Full::new(body))
        {
            Ok(req) => match tokio::time::timeout(Duration::from_secs(5), client.request(req)).await
            {
                Ok(Ok(resp)) => resp.status().is_success(),
                Ok(Err(e)) => {
                    tracing::debug!(error = %e, "OTLP export failed");
                    false
                }
                Err(_) => false,
            },
            Err(e) => {
                tracing::warn!(error = %e, endpoint = %target, "bad OTLP endpoint");
                false
            }
        };
        let mut since = failing_since.lock().unwrap_or_else(PoisonError::into_inner);
        if sent {
            batch.clear();
            *since = None;
        } else {
            let first = *since.get_or_insert_with(Instant::now);
            if first.elapsed() >= RETRY_FOR {
                drop_batch(&mut batch);
                *since = None;
            }
        }
    }
}

static GLOBAL: std::sync::OnceLock<Arc<OtlpExporter>> = std::sync::OnceLock::new();

/// Makes `e` the process's exporter and adds it to the process's tracer.
/// Only the first call wins.
pub fn install(e: Arc<OtlpExporter>) {
    if GLOBAL.set(Arc::clone(&e)).is_ok() {
        super::tracer().add_exporter(e);
    }
}

/// The process's exporter, if one is installed.
#[must_use]
pub fn global() -> Option<&'static Arc<OtlpExporter>> {
    GLOBAL.get()
}

fn drop_batch(batch: &mut Vec<Arc<Trace>>) {
    let n: usize = batch.iter().map(|t| t.spans.len()).sum();
    dropped(n as u64);
    batch.clear();
}

fn kv(k: &str, v: &str) -> Value {
    json!({"key": k, "value": {"stringValue": v}})
}

/// The OTLP/JSON `ExportTraceServiceRequest` for `traces`.
#[must_use]
pub fn encode(traces: &[Arc<Trace>], resource: &[(String, String)]) -> Value {
    let mut spans = Vec::new();
    for t in traces {
        for s in &t.spans {
            let mut span = json!({
                "traceId": format!("{:032x}", t.trace_id),
                "spanId": format!("{:016x}", s.span_id),
                "name": s.name,
                // SPAN_KIND_SERVER for roots, INTERNAL for the rest.
                "kind": if std::ptr::eq(s, t.spans.last().unwrap_or(s)) { 2 } else { 1 },
                "startTimeUnixNano": s.start_unix_nanos.to_string(),
                "endTimeUnixNano": s.start_unix_nanos.saturating_add(s.duration_nanos).to_string(),
                "attributes": s.attributes.iter().map(|(k, v)| kv(k, v)).collect::<Vec<_>>(),
                // STATUS_CODE_ERROR or UNSET.
                "status": {"code": if s.error { 2 } else { 0 }},
            });
            if let Some(p) = s.parent_id {
                span["parentSpanId"] = Value::String(format!("{p:016x}"));
            }
            spans.push(span);
        }
    }
    json!({
        "resourceSpans": [{
            "resource": {"attributes": resource.iter().map(|(k, v)| kv(k, v)).collect::<Vec<_>>()},
            "scopeSpans": [{"scope": {"name": "sprigdb", "version": env!("CARGO_PKG_VERSION")}, "spans": spans}],
        }]
    })
}
