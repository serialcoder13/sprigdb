//! Traces (chapter 47, "Traces and redaction"; build step 3).
//!
//! A trace is a tree of `tracing` spans with target [`TARGET`]. The root is
//! one statement or request on any protocol (`gateway.request`); its
//! children are the layers it passes (`backend.query`, later
//! `router.plan`, `txn.commit`, ...). [`TraceLayer`] gives each such span
//! a W3C trace context, keeps its timing and attributes, and when the root
//! closes decides whether to keep the whole trace:
//!
//! - **head sampling**: a root is sampled with probability
//!   `telemetry.trace_sample_ratio`, or when an incoming `traceparent` says
//!   it was sampled upstream;
//! - **tail sampling**: every failed root (field `error = true`) and every
//!   root slower than `telemetry.trace_slow_ms` is kept too. All of a
//!   statement's spans live in one process, so this needs no collector.
//!
//! Kept traces go to the [`Tracer`]'s ring of recent traces (served as
//! `/debug/traces` on the telemetry port) and to every registered
//! [`TraceExporter`], such as [`otlp::OtlpExporter`] (OTLP over HTTP with
//! JSON, to `telemetry.otlp_endpoint`).
//!
//! Spans carry ids and fingerprints, never SQL text or values (chapter 47,
//! "redact"): callers record only those, so no redaction pass runs here.
//!
//! **Propagation.** A root span may carry a `traceparent` field (from an
//! HTTP header or a MySQL query attribute); [`current`] gives the context
//! of the innermost traced span, which the gateway sends to MariaDB as a
//! `/* traceparent='...' */` comment on sampled statements.

pub mod otlp;

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Dispatch, Subscriber};
use tracing_subscriber::Registry;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

/// The `tracing` target of every traced span.
pub const TARGET: &str = "sprig_trace";

/// Kept traces held for `/debug/traces`.
const RECENT: usize = 64;
/// Spans kept per pending trace; more are dropped and counted.
const MAX_SPANS_PER_TRACE: usize = 512;
/// Pending traces (roots still open); more are dropped and counted.
const MAX_PENDING: usize = 4096;
/// Spans whose root never closed are swept after this long.
const PENDING_TTL: Duration = Duration::from_secs(60);

/// A W3C trace context (`traceparent`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceContext {
    /// The 128-bit trace id.
    pub trace_id: u128,
    /// The span's 64-bit id.
    pub span_id: u64,
    /// Sampled at the head (the `01` flag).
    pub sampled: bool,
}

impl TraceContext {
    /// Parses `00-<32 hex>-<16 hex>-<2 hex>`. Ids of all zeros are invalid.
    #[must_use]
    pub fn parse(traceparent: &str) -> Option<Self> {
        let mut parts = traceparent.trim().split('-');
        let version = parts.next()?;
        let trace = parts.next()?;
        let span = parts.next()?;
        let flags = parts.next()?;
        if version.len() != 2 || version == "ff" || trace.len() != 32 || span.len() != 16 {
            return None;
        }
        if flags.len() != 2 || (version == "00" && parts.next().is_some()) {
            return None;
        }
        let trace_id = u128::from_str_radix(trace, 16).ok()?;
        let span_id = u64::from_str_radix(span, 16).ok()?;
        let flags = u8::from_str_radix(flags, 16).ok()?;
        (trace_id != 0 && span_id != 0).then_some(Self {
            trace_id,
            span_id,
            sampled: flags & 1 == 1,
        })
    }

    /// The `traceparent` text.
    #[must_use]
    pub fn traceparent(&self) -> String {
        format!(
            "00-{:032x}-{:016x}-{:02x}",
            self.trace_id,
            self.span_id,
            u8::from(self.sampled)
        )
    }
}

/// One finished span of a kept trace.
#[derive(Debug, Clone, Serialize)]
pub struct SpanRecord {
    /// The span's name (`gateway.request`, `backend.query`).
    pub name: String,
    /// Its id, 16 hex digits.
    #[serde(serialize_with = "hex64")]
    pub span_id: u64,
    /// Its parent's id; for a root, the remote parent's when a
    /// `traceparent` came in.
    #[serde(serialize_with = "opt_hex64", skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<u64>,
    /// Start, nanoseconds since the Unix epoch.
    pub start_unix_nanos: u64,
    /// How long the span was open.
    pub duration_nanos: u64,
    /// Its fields, as text.
    pub attributes: Vec<(String, String)>,
    /// The span recorded `error = true`.
    pub error: bool,
}

/// Why a trace was kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KeptBecause {
    /// Head sampling (or an upstream sampled flag).
    Sampled,
    /// The root failed.
    Error,
    /// The root was slower than `telemetry.trace_slow_ms`.
    Slow,
}

/// A kept trace: the root last.
#[derive(Debug, Clone, Serialize)]
pub struct Trace {
    /// The trace id, 32 hex digits.
    #[serde(serialize_with = "hex128")]
    pub trace_id: u128,
    /// Why it was kept.
    pub kept_because: KeptBecause,
    /// Its spans, in the order they closed (the root last).
    pub spans: Vec<SpanRecord>,
}

impl Trace {
    /// The root span.
    #[must_use]
    pub fn root(&self) -> Option<&SpanRecord> {
        self.spans.last()
    }

    /// The spans whose parent is `id`.
    pub fn children_of(&self, id: u64) -> impl Iterator<Item = &SpanRecord> {
        self.spans.iter().filter(move |s| s.parent_id == Some(id))
    }
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde's signature
fn hex64<S: serde::Serializer>(v: &u64, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&format!("{v:016x}"))
}

#[allow(clippy::ref_option)] // serde's signature
fn opt_hex64<S: serde::Serializer>(v: &Option<u64>, s: S) -> Result<S::Ok, S::Error> {
    match v {
        Some(v) => s.serialize_str(&format!("{v:016x}")),
        None => s.serialize_none(),
    }
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde's signature
fn hex128<S: serde::Serializer>(v: &u128, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&format!("{v:032x}"))
}

/// Where kept traces go besides the ring of recent ones.
pub trait TraceExporter: Send + Sync + 'static {
    /// Takes one kept trace. Must not block: queue it or drop and count.
    fn export(&self, trace: &Arc<Trace>);
}

/// Sampling, the recent ring and the exporters; one per process
/// ([`tracer`]).
pub struct Tracer {
    /// `telemetry.trace_sample_ratio` as `f64` bits.
    ratio_bits: AtomicU64,
    /// `telemetry.trace_slow_ms`.
    slow_ms: AtomicU64,
    recent: Mutex<VecDeque<Arc<Trace>>>,
    exporters: RwLock<Vec<Arc<dyn TraceExporter>>>,
    pending: Mutex<HashMap<u128, Pending>>,
}

struct Pending {
    since: Instant,
    spans: Vec<SpanRecord>,
}

impl fmt::Debug for Tracer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tracer")
            .field("ratio", &self.ratio())
            .field("slow_ms", &self.slow_ms.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Default for Tracer {
    fn default() -> Self {
        Self::new(0.01, 500)
    }
}

impl Tracer {
    /// A tracer with head sampling `ratio` (0 to 1) and slow threshold
    /// `slow_ms`.
    #[must_use]
    pub fn new(ratio: f64, slow_ms: u64) -> Self {
        let t = Self {
            ratio_bits: AtomicU64::new(0),
            slow_ms: AtomicU64::new(slow_ms),
            recent: Mutex::new(VecDeque::with_capacity(RECENT)),
            exporters: RwLock::new(Vec::new()),
            pending: Mutex::new(HashMap::new()),
        };
        t.set_policy(ratio, slow_ms);
        t
    }

    /// Changes the sampling policy (live: the settings watcher calls it).
    pub fn set_policy(&self, ratio: f64, slow_ms: u64) {
        let r = if ratio.is_nan() {
            0.0
        } else {
            ratio.clamp(0.0, 1.0)
        };
        self.ratio_bits.store(r.to_bits(), Ordering::Relaxed);
        self.slow_ms.store(slow_ms, Ordering::Relaxed);
    }

    /// The head sampling ratio.
    #[must_use]
    pub fn ratio(&self) -> f64 {
        f64::from_bits(self.ratio_bits.load(Ordering::Relaxed))
    }

    /// The slow threshold.
    #[must_use]
    pub fn slow(&self) -> Duration {
        Duration::from_millis(self.slow_ms.load(Ordering::Relaxed))
    }

    /// Adds an exporter.
    pub fn add_exporter(&self, e: Arc<dyn TraceExporter>) {
        self.exporters
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .push(e);
    }

    /// The most recent kept traces, newest last.
    #[must_use]
    pub fn recent(&self) -> Vec<Arc<Trace>> {
        lock(&self.recent).iter().cloned().collect()
    }

    /// The newest kept trace whose root has attribute `key` = `value`.
    #[must_use]
    pub fn find(&self, key: &str, value: &str) -> Option<Arc<Trace>> {
        lock(&self.recent)
            .iter()
            .rev()
            .find(|t| {
                t.root().is_some_and(|r| {
                    r.attributes
                        .iter()
                        .any(|(k, v)| k.as_str() == key && v.as_str() == value)
                })
            })
            .cloned()
    }

    fn head_sample(&self) -> bool {
        let ratio = self.ratio();
        if ratio <= 0.0 {
            return false;
        }
        if ratio >= 1.0 {
            return true;
        }
        // 53 random bits as a uniform value in [0, 1).
        #[allow(clippy::cast_precision_loss)]
        let u = (random_u64() >> 11) as f64 / (1u64 << 53) as f64;
        u < ratio
    }

    fn child_closed(&self, trace_id: u128, span: SpanRecord) {
        let mut pending = lock(&self.pending);
        let full = pending.len() >= MAX_PENDING && !pending.contains_key(&trace_id);
        if full {
            drop(pending);
            dropped(1);
            return;
        }
        let p = pending.entry(trace_id).or_insert_with(|| Pending {
            since: Instant::now(),
            spans: Vec::new(),
        });
        if p.spans.len() >= MAX_SPANS_PER_TRACE {
            drop(pending);
            dropped(1);
            return;
        }
        p.spans.push(span);
    }

    fn root_closed(&self, trace_id: u128, root: SpanRecord, sampled: bool) {
        let children = {
            let mut pending = lock(&self.pending);
            let mine = pending.remove(&trace_id).map(|p| p.spans);
            let now = Instant::now();
            let before = pending.len();
            pending.retain(|_, p| now.saturating_duration_since(p.since) < PENDING_TTL);
            let swept = before - pending.len();
            if swept > 0 {
                dropped(swept as u64);
            }
            mine.unwrap_or_default()
        };
        let kept_because = if sampled {
            KeptBecause::Sampled
        } else if root.error {
            KeptBecause::Error
        } else if Duration::from_nanos(root.duration_nanos) >= self.slow() {
            KeptBecause::Slow
        } else {
            return;
        };
        let mut spans = children;
        spans.push(root);
        let trace = Arc::new(Trace {
            trace_id,
            kept_because,
            spans,
        });
        {
            let mut recent = lock(&self.recent);
            if recent.len() >= RECENT {
                recent.pop_front();
            }
            recent.push_back(Arc::clone(&trace));
        }
        metrics::counter!("sprig_telemetry_traces_kept_total", "reason" => match kept_because {
            KeptBecause::Sampled => "sampled",
            KeptBecause::Error => "error",
            KeptBecause::Slow => "slow",
        })
        .increment(1);
        for e in self
            .exporters
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
        {
            e.export(&trace);
        }
    }
}

/// Counts spans lost to a full buffer or an exporter that can't keep up.
pub fn dropped(n: u64) {
    metrics::counter!("sprig_telemetry_spans_dropped_total").increment(n);
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The process's tracer.
pub fn tracer() -> &'static Arc<Tracer> {
    static GLOBAL: OnceLock<Arc<Tracer>> = OnceLock::new();
    GLOBAL.get_or_init(|| Arc::new(Tracer::default()))
}

/// A random non-zero `u64`.
fn random_u64() -> u64 {
    // Span and trace ids and the head sample need uniqueness, not secrecy:
    // a per-thread SplitMix64 seeded once from the OS (a `getentropy`
    // system call per id cost about 3% of a gateway's CPU per statement;
    // R5 round 4).
    thread_local! {
        static STATE: std::cell::Cell<u64> = std::cell::Cell::new(seed());
    }
    loop {
        let v = STATE.with(|st| {
            let x = st.get().wrapping_add(0x9E37_79B9_7F4A_7C15);
            st.set(x);
            let mut z = x;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        });
        if v != 0 {
            return v;
        }
    }
}

/// A thread's seed: OS randomness, else the clock and the thread.
fn seed() -> u64 {
    getrandom::u64().unwrap_or_else(|_| {
        // No OS randomness: the clock is good enough for ids.
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(1, |d| d.as_nanos());
        #[allow(clippy::cast_possible_truncation)]
        let v = (t as u64) ^ (t >> 64) as u64;
        let local = 0u8;
        (v ^ std::ptr::from_ref(&local).addr() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
    })
}

fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

/// What the layer keeps per traced span (a registry extension).
#[derive(Debug)]
struct SpanState {
    ctx: TraceContext,
    parent_id: Option<u64>,
    root: bool,
    name: &'static str,
    start: Instant,
    start_unix_nanos: u64,
    attributes: Vec<(String, String)>,
    error: bool,
}

/// Fields of a span, as text.
#[derive(Default)]
struct Fields {
    attributes: Vec<(String, String)>,
    traceparent: Option<String>,
    error: Option<bool>,
}

impl Fields {
    fn put(&mut self, field: &Field, value: String) {
        let name = field.name();
        if name == "traceparent" {
            self.traceparent = Some(value);
            return;
        }
        if let Some(slot) = self.attributes.iter_mut().find(|(k, _)| k == name) {
            slot.1 = value;
        } else {
            self.attributes.push((name.to_owned(), value));
        }
    }
}

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.put(field, format!("{value:?}"));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, value.to_owned());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == "error" {
            self.error = Some(value);
        }
        self.put(field, value.to_string());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.put(field, value.to_string());
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.put(field, value.to_string());
    }
}

/// The `tracing` layer that builds traces from spans with target
/// [`TARGET`], for the process's [`tracer`]. Install it with a filter
/// that passes only that target ([`layer`] does).
#[derive(Debug, Clone)]
pub struct TraceLayer {
    tracer: Arc<Tracer>,
}

impl TraceLayer {
    /// A layer feeding `tracer`.
    #[must_use]
    pub fn new(tracer: Arc<Tracer>) -> Self {
        Self { tracer }
    }
}

/// The trace layer for the process's [`tracer`], filtered to [`TARGET`].
#[must_use]
pub fn layer<S>() -> impl tracing_subscriber::Layer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    use tracing_subscriber::Layer as _;
    TraceLayer::new(Arc::clone(tracer())).with_filter(tracing_subscriber::filter::filter_fn(|m| {
        m.target() == TARGET
    }))
}

impl<S> tracing_subscriber::Layer<S> for TraceLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        let parent = span
            .scope()
            .skip(1)
            .find_map(|p| p.extensions().get::<SpanState>().map(|s| s.ctx));
        let (ctx, parent_id, root) = if let Some(p) = parent {
            (
                TraceContext {
                    trace_id: p.trace_id,
                    span_id: random_u64(),
                    sampled: p.sampled,
                },
                Some(p.span_id),
                false,
            )
        } else {
            let remote = fields.traceparent.as_deref().and_then(TraceContext::parse);
            let trace_id = remote.map_or_else(
                || (u128::from(random_u64()) << 64) | u128::from(random_u64()),
                |r| r.trace_id,
            );
            let sampled = remote.is_some_and(|r| r.sampled) || self.tracer.head_sample();
            (
                TraceContext {
                    trace_id,
                    span_id: random_u64(),
                    sampled,
                },
                remote.map(|r| r.span_id),
                true,
            )
        };
        span.extensions_mut().insert(SpanState {
            ctx,
            parent_id,
            root,
            name: attrs.metadata().name(),
            start: Instant::now(),
            start_unix_nanos: unix_nanos(),
            attributes: fields.attributes,
            error: fields.error.unwrap_or(false),
        });
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        let mut ext = span.extensions_mut();
        let Some(state) = ext.get_mut::<SpanState>() else {
            return;
        };
        let mut fields = Fields {
            attributes: std::mem::take(&mut state.attributes),
            ..Fields::default()
        };
        values.record(&mut fields);
        state.attributes = fields.attributes;
        if let Some(e) = fields.error {
            state.error = e;
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else {
            return;
        };
        let Some(state) = span.extensions_mut().remove::<SpanState>() else {
            return;
        };
        let record = SpanRecord {
            name: state.name.to_owned(),
            span_id: state.ctx.span_id,
            parent_id: state.parent_id,
            start_unix_nanos: state.start_unix_nanos,
            duration_nanos: u64::try_from(state.start.elapsed().as_nanos()).unwrap_or(u64::MAX),
            attributes: state.attributes,
            error: state.error,
        };
        if state.root {
            self.tracer
                .root_closed(state.ctx.trace_id, record, state.ctx.sampled);
        } else {
            self.tracer.child_closed(state.ctx.trace_id, record);
        }
    }
}

/// The trace context of the innermost traced span around the caller, if
/// any (for propagation to MariaDB and to peers).
#[must_use]
pub fn current() -> Option<TraceContext> {
    tracing::Span::current()
        .with_subscriber(|(id, dispatch)| context_of(id, dispatch))
        .flatten()
}

fn context_of(id: &Id, dispatch: &Dispatch) -> Option<TraceContext> {
    let registry = dispatch.downcast_ref::<Registry>()?;
    let span = registry.span(id)?;
    span.scope()
        .find_map(|s| s.extensions().get::<SpanState>().map(|st| st.ctx))
}

#[cfg(test)]
mod tests;
