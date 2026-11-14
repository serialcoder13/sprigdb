//! Statement SLIs and the root trace span.
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
//!   counted. A tenant whose statements reached
//!   `telemetry.tenant_series_min_rps` over its last full minute gets its
//!   own series: the `tenant` label (its id) on
//!   `sprig_gateway_statements_total` and
//!   `sprig_gateway_query_duration_seconds`; every other statement has
//!   `tenant=""`, which `PromQL` treats as no label, so the series count
//!   stays bounded by the busy tenants.
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

/// The `result` label of an error.
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

const RATE_SHARDS: usize = 16;
const RATE_WINDOW: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy)]
struct Rate {
    since: Instant,
    count: u64,
    busy: bool,
}

/// Per-tenant statement rates, sharded to keep the lock short.
struct TenantRates {
    shards: [std::sync::Mutex<std::collections::HashMap<u64, Rate>>; RATE_SHARDS],
}

impl TenantRates {
    fn get() -> &'static TenantRates {
        static RATES: std::sync::OnceLock<TenantRates> = std::sync::OnceLock::new();
        RATES.get_or_init(|| TenantRates {
            shards: std::array::from_fn(
                |_| std::sync::Mutex::new(std::collections::HashMap::new()),
            ),
        })
    }

    /// Counts one statement of `tenant`; `true` while the tenant was at
    /// `telemetry.tenant_series_min_rps` over its last full window.
    fn busy(&self, tenant: u64, now: Instant) -> bool {
        let shard = usize::try_from(tenant % RATE_SHARDS as u64).unwrap_or(0);
        let mut m = self.shards[shard]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let r = m.entry(tenant).or_insert(Rate {
            since: now,
            count: 0,
            busy: false,
        });
        r.count += 1;
        let elapsed = now.saturating_duration_since(r.since);
        if elapsed >= RATE_WINDOW {
            let min = sprig_common::telemetry::tenant_series_min_rps();
            r.busy = r.count >= min.saturating_mul(elapsed.as_secs());
            r.since = now;
            r.count = 0;
        }
        r.busy
    }
}

/// `exec` with SLI metrics and the root span in front.
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
            sli: Sli::default(),
            internal: false,
        })
    }
}

struct ObservedSession {
    inner: Box<dyn SessionExec>,
    tenant: Option<u64>,
    sli: Sli,
    /// The session runs a protocol engine's own statements
    /// ([`SessionExec::set_internal`]).
    internal: bool,
}

/// Whether the innermost entered span is a traced request already (a
/// MongoDB command's `gateway.request`): an engine's
/// statements inside it record no root span of their own.
fn in_traced_request() -> bool {
    tracing::Span::current()
        .metadata()
        .is_some_and(|m| m.target() == TARGET)
}

/// A session's SLI metric handles, registered on first use (registering
/// them with their labels on every statement cost about 2% of a MongoDB
/// point read's gateway CPU). A session's protocol and tenant
/// don't change; `result` and whether the tenant has its own series do.
#[derive(Default)]
struct Sli {
    /// `sprig_gateway_statements_total` by (result, tenant series).
    statements: Vec<(&'static str, bool, metrics::Counter)>,
    /// `sprig_gateway_query_duration_seconds` without and with the tenant
    /// series.
    duration: [Option<metrics::Histogram>; 2],
    overhead: Option<metrics::Histogram>,
}

impl Sli {
    fn record(
        &mut self,
        protocol: &'static str,
        tenant: Option<u64>,
        busy: bool,
        result: &'static str,
        total: Duration,
        overhead: Duration,
    ) {
        let label = || match tenant {
            Some(t) if busy => t.to_string(),
            _ => String::new(),
        };
        let i = if let Some(i) = self
            .statements
            .iter()
            .position(|(r, b, _)| *r == result && *b == busy)
        {
            i
        } else {
            let c = metrics::counter!("sprig_gateway_statements_total", "protocol" => protocol, "result" => result, "tenant" => label());
            self.statements.push((result, busy, c));
            self.statements.len() - 1
        };
        self.statements[i].2.increment(1);
        self.duration[usize::from(busy)]
            .get_or_insert_with(|| metrics::histogram!("sprig_gateway_query_duration_seconds", "protocol" => protocol, "tenant" => label()))
            .record(total.as_secs_f64());
        self.overhead
            .get_or_insert_with(
                || metrics::histogram!("sprig_gateway_overhead_seconds", "protocol" => protocol),
            )
            .record(overhead.as_secs_f64());
    }
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
    /// A point read is always an engine's statement inside its traced
    /// command: its SLIs, no span of its own.
    async fn point_read(
        &mut self,
        cx: &mut ExecCtx<'_>,
        stmt: crate::pipeline::StmtRef,
        params: &sprig_proto_mysql::command::ExecuteParams,
    ) -> Result<crate::pipeline::PointRead> {
        let protocol = cx.session.protocol.as_str();
        let waited = Arc::new(AtomicU64::new(0));
        let started = Instant::now();
        let run = Box::pin(self.inner.point_read(cx, stmt, params));
        let r = BACKEND_NANOS.scope(Arc::clone(&waited), run).await;
        // A fallback is counted by the execute that follows it.
        if matches!(r, Ok(crate::pipeline::PointRead::Fallback)) {
            return r;
        }
        let total = started.elapsed();
        let result = match &r {
            Ok(_) => "ok",
            Err(e) => classify(e),
        };
        let backend = Duration::from_nanos(waited.load(Ordering::Relaxed));
        let busy = self
            .tenant
            .is_some_and(|t| TenantRates::get().busy(t, Instant::now()));
        self.sli.record(
            protocol,
            self.tenant,
            busy,
            result,
            total,
            total.saturating_sub(backend),
        );
        r
    }

    async fn execute(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: Request,
        out: &mut dyn ResultSink,
    ) -> Result<ExecSummary> {
        if req.is_silent() {
            return self.inner.execute(cx, req, out).await;
        }
        let protocol = cx.session.protocol.as_str();
        // One span per request: an engine statement inside a
        // traced command (MongoDB's) is part of that command's span.
        let span = if self.internal && in_traced_request() {
            None
        } else {
            Some(tracing::info_span!(
                target: TARGET,
                "gateway.request",
                protocol,
                request = req.name(),
                tenant = self.tenant,
                gateway_session = cx.session.id.0,
                traceparent = traceparent(&req),
                result = tracing::field::Empty,
                error = tracing::field::Empty,
            ))
        };
        let waited = Arc::new(AtomicU64::new(0));
        let started = Instant::now();
        let mut sink = Watch {
            inner: out,
            result: None,
        };
        let run = self.inner.execute(cx, req, &mut sink);
        let r = match &span {
            Some(sp) => {
                BACKEND_NANOS
                    .scope(Arc::clone(&waited), run.instrument(sp.clone()))
                    .await
            }
            None => BACKEND_NANOS.scope(Arc::clone(&waited), run).await,
        };
        let total = started.elapsed();
        let result = match &r {
            Ok(_) => sink.result.unwrap_or("ok"),
            Err(e) => classify(e),
        };
        if let Some(span) = &span {
            span.record("result", result);
            if result != "ok" {
                span.record("error", true);
            }
        }
        let backend = Duration::from_nanos(waited.load(Ordering::Relaxed));
        let busy = self
            .tenant
            .is_some_and(|t| TenantRates::get().busy(t, Instant::now()));
        self.sli.record(
            protocol,
            self.tenant,
            busy,
            result,
            total,
            total.saturating_sub(backend),
        );
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

    fn workload(&self, db: &str, table: &str) -> Option<crate::workload::FrontCapture> {
        self.inner.workload(db, table)
    }

    fn idle_timeout(&self) -> Option<std::time::Duration> {
        self.inner.idle_timeout()
    }

    fn set_internal(&mut self, on: bool) {
        self.internal = on;
        self.inner.set_internal(on);
    }

    fn row_policy_binds(&self, object: u64) -> Option<bool> {
        self.inner.row_policy_binds(object)
    }

    fn row_policy_docs(&self) -> Option<std::sync::Arc<crate::tenant::rls::DocViews>> {
        self.inner.row_policy_docs()
    }

    fn holds_tenant_privilege(&self, action: sprig_auth::authz::Action) -> Option<bool> {
        self.inner.holds_tenant_privilege(action)
    }

    fn set_claims(&mut self, claims: Option<std::sync::Arc<serde_json::Value>>) {
        self.inner.set_claims(claims);
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

#[cfg(test)]
mod rate_tests {
    use super::*;

    #[test]
    fn a_tenant_gets_series_after_a_busy_minute() {
        sprig_common::telemetry::set_tenant_series_min_rps(10);
        let r = TenantRates::get();
        let t0 = Instant::now();
        let tenant = 9_000_001;
        assert!(!r.busy(tenant, t0));
        for _ in 0..700 {
            r.busy(tenant, t0 + Duration::from_secs(1));
        }
        // The window closes: 701 statements in 60 s is above 10/s.
        assert!(r.busy(tenant, t0 + Duration::from_secs(60)));
        // A quiet minute drops the series again.
        assert!(!r.busy(tenant, t0 + Duration::from_secs(121)));
    }
}
