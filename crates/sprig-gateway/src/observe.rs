//! Statement SLIs and the root trace span (chapter 47, "Metric catalogue",
//! "SLOs and burn-rate alerts" and "Traces and redaction"; build steps 2
//! and 3).
//!
//! [`observed`] wraps the gateway's executor, outermost, so every request
//! of every front end (MySQL, MariaDB, HTTP) passes it:
//!
//! - **SLIs.** `sprig_gateway_statements_total` (`protocol`, `result`),
//!   `sprig_gateway_query_duration_seconds` (`protocol`: admission, the
//!   gateway and MariaDB) and `sprig_gateway_overhead_seconds`
//!   (`protocol`: the same minus the time spent waiting on MariaDB, which
//!   the backend layer adds up through [`backend_time`]). `result` is `ok`,
//!   `client` (errors the application caused), `sprig` (errors SprigDB
//!   caused) or `backend` (MariaDB unreachable or failed mid-statement);
//!   see [`classify`]. Silent requests (`COM_STMT_SEND_LONG_DATA`,
//!   `COM_STMT_CLOSE`) and requests cancelled by `KILL` or a hang-up are not
//!   counted.
//! - **Traces.** Each request runs inside a `gateway.request` span (target
//!   [`TARGET`]) carrying the tenant id, protocol and request kind, and an
//!   incoming W3C `traceparent` query attribute; the backend layer's
//!   `backend.query` spans are its children. The span records `result`
//!   and `error = true` on failure, so the tracer keeps every failed
//!   statement (tail sampling).
//!
//! No SQL text and no values go into spans or labels.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use sprig_common::telemetry::trace::TARGET;
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{ColumnDef, Value};
use tracing::Instrument as _;

use crate::pipeline::{
    Abandon, EndOfRows, ExecCtx, ExecSummary, Executor, LoginInfo, Request, ResultSink, RowFormat,
    RowValues, SessionExec, SharedExecutor, Wire,
};

tokio::task_local! {
    /// Nanoseconds the current request spent waiting on MariaDB.
    static BACKEND_NANOS: Arc<AtomicU64>;
}

/// Adds `d` to the current request's time waiting on MariaDB (the backend
/// layer calls it around each round trip). Outside a request, nothing.
pub fn backend_time(d: Duration) {
    let _ = BACKEND_NANOS.try_with(|n| {
        n.fetch_add(
            u64::try_from(d.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    });
}

/// The `result` label of an error (chapter 47, "Metric catalogue").
#[must_use]
pub fn classify(e: &SprigError) -> &'static str {
    match e.kind {
        ErrorKind::Backend { errno, .. } => match errno {
            // Server shutting down, too many connections, gone away, lost
            // mid-query, connection killed by the server: MariaDB failed.
            1040 | 1053 | 1077 | 1152 | 1158..=1161 | 1927 | 2006 | 2013 => "backend",
            _ => "client",
        },
        ErrorKind::Standard { .. } => "client",
        ErrorKind::Sprig(code) => match code {
            SprigCode::BackendUnavailable
            | SprigCode::BackendPoolTimeout
            | SprigCode::BackendLost
            | SprigCode::BackendProtocol
            | SprigCode::ClusterUnavailable => "backend",
            _ if e.protocol().http_status < 500 => "client",
            _ => "sprig",
        },
    }
}

/// `exec` with SLI metrics and the root span in front (chapter 47).
#[must_use]
pub fn observed(exec: SharedExecutor) -> SharedExecutor {
    Arc::new(Observed { inner: exec })
}

struct Observed {
    inner: SharedExecutor,
}

impl Executor for Observed {
    fn open(&self, login: &LoginInfo) -> Box<dyn SessionExec> {
        Box::new(ObservedSession {
            inner: self.inner.open(login),
            tenant: login.principal.tenant.map(|t| t.0),
        })
    }
}

struct ObservedSession {
    inner: Box<dyn SessionExec>,
    tenant: Option<u64>,
}

fn traceparent(req: &Request) -> Option<String> {
    let Request::Query { attrs, .. } = req else {
        return None;
    };
    match attrs.get(b"traceparent")? {
        Value::Bytes(b) => std::str::from_utf8(b).ok().map(str::to_owned),
        _ => None,
    }
}

#[async_trait]
impl SessionExec for ObservedSession {
    async fn execute(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: Request,
        out: &mut dyn ResultSink,
    ) -> Result<ExecSummary> {
        if req.is_silent() {
            return self.inner.execute(cx, req, out).await;
        }
        let protocol = cx.session.protocol().as_str();
        let span = tracing::info_span!(
            target: TARGET,
            "gateway.request",
            protocol,
            request = req.name(),
            tenant = self.tenant,
            gateway_session = cx.session.id().0,
            traceparent = traceparent(&req),
            result = tracing::field::Empty,
            error = tracing::field::Empty,
        );
        let waited = Arc::new(AtomicU64::new(0));
        let started = Instant::now();
        let mut sink = Watch {
            inner: out,
            result: None,
        };
        let r = BACKEND_NANOS
            .scope(
                Arc::clone(&waited),
                self.inner.execute(cx, req, &mut sink).instrument(span.clone()),
            )
            .await;
        let total = started.elapsed();
        let result = match &r {
            Ok(_) => sink.result.unwrap_or("ok"),
            Err(e) => classify(e),
        };
        span.record("result", result);
        if result != "ok" {
            span.record("error", true);
        }
        let backend = Duration::from_nanos(waited.load(Ordering::Relaxed));
        metrics::counter!("sprig_gateway_statements_total", "protocol" => protocol, "result" => result)
            .increment(1);
        metrics::histogram!("sprig_gateway_query_duration_seconds", "protocol" => protocol)
            .record(total.as_secs_f64());
        metrics::histogram!("sprig_gateway_overhead_seconds", "protocol" => protocol)
            .record(total.saturating_sub(backend).as_secs_f64());
        r
    }

    async fn init_db(&mut self, cx: &mut ExecCtx<'_>, schema: &str) -> Result<()> {
        self.inner.init_db(cx, schema).await
    }

    async fn reset(&mut self, cx: &mut ExecCtx<'_>) -> Result<()> {
        self.inner.reset(cx).await
    }

    async fn abandoned(&mut self, cx: &mut ExecCtx<'_>, why: Abandon) {
        self.inner.abandoned(cx, why).await;
    }

    async fn close(&mut self) {
        self.inner.close().await;
    }

    fn attach_capture(&mut self, capture: crate::workload::SessionCapture) {
        self.inner.attach_capture(capture);
    }
}

/// A sink that notes the class of an error relayed through it (a backend
/// ERR packet reaches the client as `Ok` from the executor).
struct Watch<'a> {
    inner: &'a mut dyn ResultSink,
    result: Option<&'static str>,
}

#[async_trait]
impl ResultSink for Watch<'_> {
    fn wire(&self) -> Wire {
        self.inner.wire()
    }
    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()> {
        self.inner.begin_rows(columns, format).await
    }
    async fn begin_rows_raw(&mut self, defs: &[Bytes], format: RowFormat) -> Result<()> {
        self.inner.begin_rows_raw(defs, format).await
    }
    async fn cursor_opened(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.inner.cursor_opened(columns, end).await
    }
    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        self.inner.row(values).await
    }
    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        self.inner.raw_row(payload).await
    }
    async fn end_rows(&mut self, end: EndOfRows) -> Result<()> {
        self.inner.end_rows(end).await
    }
    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.inner.ok(ok).await
    }
    async fn error(&mut self, err: &SprigError) -> Result<()> {
        // The first error of a multi-statement decides.
        self.result.get_or_insert(classify(err));
        self.inner.error(err).await
    }
    async fn column_list(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.inner.column_list(columns, end).await
    }
    async fn local_infile_request(&mut self, file_name: &[u8]) -> Result<()> {
        self.inner.local_infile_request(file_name).await
    }
    async fn local_infile_read(&mut self) -> Result<Bytes> {
        self.inner.local_infile_read().await
    }
    async fn flush(&mut self) -> Result<()> {
        self.inner.flush().await
    }
    fn finished(&self) -> bool {
        self.inner.finished()
    }
}

#[cfg(test)]
mod tests;
