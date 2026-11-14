//! The audit pipeline.
//!
//! ```text
//! emit(event):
//! 1. append to the process's spool; fsync every 100 ms or 64 events; seq increases
//! 2. spool above audit.spool_max_mb: admin API mutations and account statements
//!    fail closed with 39970 ER_SPRIG_AUDIT_UNAVAILABLE ([`check_available`]);
//!    logins continue, their events kept in memory up to 10,000, then dropped
//!    and counted
//! ship, every second:
//! 3. from the last acknowledged seq, tenant events go to the tenant sink,
//!    platform events to the platform sink; AuditSink::write is durable before Ok
//! 4. trim the spool; sinks deduplicate on (instance, seq)
//! ```
//!
//! Producers build typed events in their own crates (login and account
//! events in `sprig-auth`, `settings.changed` in the
//! controller) and hand them to the process's [`Pipeline`] as an event
//! name and a JSON body ([`record`]). The log form stays: `sprig-auth`
//! still writes each event as a JSON line with target `sprig_audit`.
//!
//! **Spool.** Segment files `spool-<first seq>.jsonl` in
//! `[audit] spool_dir`, one [`AuditRecord`] per line, and `acked` holding
//! the last sequence number every sink acknowledged. A restart reloads
//! what was spooled but not acknowledged, so nothing is lost; a sink may
//! see a record twice and deduplicates on `(instance, seq)`.
//!
//! **Sinks.** [`DirSink`] writes the audit object-storage layout
//! (`audit/<stream>/<date>/<instance>-<first>-<last>.jsonl`) under a
//! directory: the first sink, and the layout an S3 sink (with residency
//! routing per DC) keeps. [`MemorySink`] is for tests.

use std::collections::VecDeque;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::error::{Result, SprigCode, SprigError};
use crate::ids::{DcId, TenantId};

/// Spool fsync period.
pub const FLUSH_PERIOD: Duration = Duration::from_millis(100);
/// Events that trigger a flush before the period ends.
pub const FLUSH_EVENTS: usize = 64;
/// Ship period.
pub const SHIP_PERIOD: Duration = Duration::from_secs(1);
/// Events kept in memory only (the spool is full) before they are dropped.
pub const MEMORY_LIMIT: usize = 10_000;
/// A spool segment is closed past this size.
const SEGMENT_BYTES: u64 = 4 << 20;
/// Records per ship.
const SHIP_BATCH: usize = 5_000;
/// `audit.spool_max_mb`'s default.
pub const DEFAULT_SPOOL_MAX_MB: u64 = 512;

/// Where an event is kept: a tenant's stream or the platform's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AuditStream {
    /// A tenant's stream (residency follows the tenant).
    Tenant(TenantId),
    /// The platform's stream (operators, settings, decisions).
    Platform,
}

impl AuditStream {
    /// `tenant` or `platform`, the metric label.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            AuditStream::Tenant(_) => "tenant",
            AuditStream::Platform => "platform",
        }
    }
}

impl fmt::Display for AuditStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuditStream::Tenant(t) => write!(f, "tenant-{}", t.0),
            AuditStream::Platform => f.write_str("platform"),
        }
    }
}

impl Serialize for AuditStream {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for AuditStream {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        if s == "platform" {
            return Ok(AuditStream::Platform);
        }
        s.strip_prefix("tenant-")
            .and_then(|n| n.parse().ok())
            .map(|n| AuditStream::Tenant(TenantId(n)))
            .ok_or_else(|| serde::de::Error::custom(format!("bad audit stream {s:?}")))
    }
}

/// One audit event as the pipeline carries it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditRecord {
    /// The emitting process (its node id).
    pub instance: String,
    /// Increasing per instance; with `instance`, the deduplication key.
    pub seq: u64,
    /// The stream.
    pub stream: AuditStream,
    /// When, ms since the Unix epoch.
    pub at_ms: u64,
    /// The event name (`auth.login_failed`, `settings.changed`, ...).
    pub event: String,
    /// The event's own fields, never a secret.
    pub body: serde_json::Value,
}

/// A durable destination for audit records.
#[async_trait::async_trait]
pub trait AuditSink: Send + Sync {
    /// Durable write of one batch of one stream; the shipper trims its
    /// spool only after `Ok`.
    async fn write(&self, stream: &AuditStream, batch: &[AuditRecord]) -> Result<()>;
    /// Where the data lands, for residency checks.
    fn dc(&self) -> &DcId;
}

/// The error of [`check_available`].
#[must_use]
pub fn unavailable(detail: &str) -> SprigError {
    SprigError::new(
        SprigCode::AuditUnavailable,
        format!("the audit pipeline can't record this change: {detail}"),
    )
}

#[derive(Default)]
struct State {
    next_seq: u64,
    acked: u64,
    /// Emitted, not yet in the spool.
    queued: Vec<AuditRecord>,
    /// In the spool (or memory-only), not yet acknowledged, by seq.
    unshipped: VecDeque<AuditRecord>,
    /// Of `unshipped`, how many were never spooled.
    memory_only: usize,
    segments: VecDeque<Segment>,
    spool_bytes: u64,
}

#[derive(Debug)]
struct Segment {
    path: PathBuf,
    last_seq: u64,
    bytes: u64,
}

struct Inner {
    instance: String,
    dir: PathBuf,
    max_bytes: AtomicU64,
    state: Mutex<State>,
    /// Serializes spool file writes (flush runs on a blocking thread).
    file: Mutex<Option<File>>,
    wake: Notify,
    tenant_sink: Arc<dyn AuditSink>,
    platform_sink: Arc<dyn AuditSink>,
}

/// The process's audit spool and shipper.
#[derive(Clone)]
pub struct Pipeline {
    inner: Arc<Inner>,
}

impl fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pipeline")
            .field("instance", &self.inner.instance)
            .field("dir", &self.inner.dir)
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn io_error(what: &str, path: &Path, e: std::io::Error) -> SprigError {
    unavailable(&format!("{what} {}", path.display())).with_source(e)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

impl Pipeline {
    /// Opens (or recovers) the spool in `dir` for `instance`, shipping
    /// tenant streams to `tenant_sink` and the platform stream to
    /// `platform_sink` (often the same sink). Call [`spawn`](Self::spawn)
    /// to start flushing and shipping.
    ///
    /// # Errors
    /// 39970 if the directory can't be created or read.
    pub fn open(
        dir: impl Into<PathBuf>,
        instance: impl Into<String>,
        max_spool_mb: u64,
        tenant_sink: Arc<dyn AuditSink>,
        platform_sink: Arc<dyn AuditSink>,
    ) -> Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).map_err(|e| io_error("can't create", &dir, e))?;
        let mut state = State::default();
        let acked_path = dir.join("acked");
        state.acked = fs::read_to_string(&acked_path)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0);
        let mut names: Vec<(u64, PathBuf)> = fs::read_dir(&dir)
            .map_err(|e| io_error("can't read", &dir, e))?
            .filter_map(std::result::Result::ok)
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                let first = name
                    .strip_prefix("spool-")?
                    .strip_suffix(".jsonl")?
                    .parse()
                    .ok()?;
                Some((first, e.path()))
            })
            .collect();
        names.sort();
        let mut max_seq = state.acked;
        for (_, path) in names {
            let file = File::open(&path).map_err(|e| io_error("can't read", &path, e))?;
            let bytes = file.metadata().map_or(0, |m| m.len());
            let mut last = 0;
            for line in BufReader::new(file).lines() {
                let Ok(line) = line else { break };
                // A torn last line (crash mid-write) is skipped.
                let Ok(r) = serde_json::from_str::<AuditRecord>(&line) else {
                    continue;
                };
                last = last.max(r.seq);
                max_seq = max_seq.max(r.seq);
                if r.seq > state.acked {
                    state.unshipped.push_back(r);
                }
            }
            state.spool_bytes += bytes;
            state.segments.push_back(Segment {
                path,
                last_seq: last,
                bytes,
            });
        }
        state.next_seq = max_seq + 1;
        let p = Self {
            inner: Arc::new(Inner {
                instance: instance.into(),
                dir,
                max_bytes: AtomicU64::new(max_spool_mb.saturating_mul(1 << 20)),
                state: Mutex::new(state),
                file: Mutex::new(None),
                wake: Notify::new(),
                tenant_sink,
                platform_sink,
            }),
        };
        p.gauge();
        Ok(p)
    }

    /// Changes `audit.spool_max_mb` (live).
    pub fn set_max_spool_mb(&self, mb: u64) {
        self.inner
            .max_bytes
            .store(mb.saturating_mul(1 << 20), Ordering::Relaxed);
        self.gauge();
    }

    /// The instance name records carry.
    #[must_use]
    pub fn instance(&self) -> &str {
        &self.inner.instance
    }

    /// Bytes in the spool.
    #[must_use]
    pub fn spool_bytes(&self) -> u64 {
        lock(&self.inner.state).spool_bytes
    }

    /// Events emitted but not yet acknowledged by a sink.
    #[must_use]
    pub fn backlog(&self) -> usize {
        let s = lock(&self.inner.state);
        s.queued.len() + s.unshipped.len()
    }

    fn full(&self, s: &State) -> bool {
        s.spool_bytes >= self.inner.max_bytes.load(Ordering::Relaxed)
    }

    /// `Err(39970)` while the spool is at `audit.spool_max_mb`: admin API
    /// mutations and account statements call this first and fail closed.
    ///
    /// # Errors
    /// 39970 when the spool is full.
    pub fn check(&self) -> Result<()> {
        let s = lock(&self.inner.state);
        if self.full(&s) {
            return Err(unavailable(&format!(
                "the spool holds {} MB, at audit.spool_max_mb; the sink is behind",
                s.spool_bytes >> 20
            )));
        }
        Ok(())
    }

    /// Records one event. Never blocks and never fails: when the spool is
    /// full the event is kept in memory (up to [`MEMORY_LIMIT`]), then
    /// dropped and counted.
    pub fn emit(&self, stream: AuditStream, event: &str, body: serde_json::Value) {
        let mut s = lock(&self.inner.state);
        let record = AuditRecord {
            instance: self.inner.instance.clone(),
            seq: s.next_seq,
            stream,
            at_ms: now_ms(),
            event: event.to_owned(),
            body,
        };
        if self.full(&s) {
            if s.memory_only >= MEMORY_LIMIT {
                drop(s);
                metrics::counter!("sprig_audit_dropped_total", "stream" => stream.kind())
                    .increment(1);
                tracing::error!(target: "sprig_audit", event, %stream, "audit spool full and memory buffer full: event dropped");
                return;
            }
            s.next_seq += 1;
            s.memory_only += 1;
            s.unshipped.push_back(record);
        } else {
            s.next_seq += 1;
            s.queued.push(record);
        }
        let wake = s.queued.len() >= FLUSH_EVENTS;
        drop(s);
        metrics::counter!("sprig_audit_events_total", "stream" => stream.kind()).increment(1);
        if wake {
            self.inner.wake.notify_one();
        }
    }

    /// Writes queued events to the spool and fsyncs it.
    ///
    /// # Errors
    /// 39970 if the spool can't be written; the events stay queued.
    pub fn flush(&self) -> Result<()> {
        let mut file = lock(&self.inner.file);
        let batch = std::mem::take(&mut lock(&self.inner.state).queued);
        if batch.is_empty() {
            return Ok(());
        }
        let restore = |batch: Vec<AuditRecord>| {
            let mut s = lock(&self.inner.state);
            let mut b = batch;
            b.append(&mut s.queued);
            s.queued = b;
        };
        let mut text = Vec::with_capacity(batch.len() * 256);
        for r in &batch {
            if serde_json::to_writer(&mut text, r).is_ok() {
                text.push(b'\n');
            }
        }
        // Open a new segment when there is none or the last is full.
        let need_new = {
            let s = lock(&self.inner.state);
            file.is_none() || s.segments.back().is_none_or(|g| g.bytes >= SEGMENT_BYTES)
        };
        if need_new {
            let first = batch.first().map_or(0, |r| r.seq);
            let path = self.inner.dir.join(format!("spool-{first:020}.jsonl"));
            match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(f) => {
                    *file = Some(f);
                    lock(&self.inner.state).segments.push_back(Segment {
                        path,
                        last_seq: 0,
                        bytes: 0,
                    });
                }
                Err(e) => {
                    restore(batch);
                    return Err(io_error("can't open", &path, e));
                }
            }
        }
        let Some(f) = file.as_mut() else {
            restore(batch);
            return Err(unavailable("no spool segment"));
        };
        if let Err(e) = f.write_all(&text).and_then(|()| f.sync_data()) {
            *file = None;
            restore(batch);
            return Err(unavailable("spool write failed").with_source(e));
        }
        {
            let mut s = lock(&self.inner.state);
            let n = text.len() as u64;
            s.spool_bytes += n;
            let last = batch.last().map_or(0, |r| r.seq);
            if let Some(g) = s.segments.back_mut() {
                g.bytes += n;
                g.last_seq = g.last_seq.max(last);
            }
            s.unshipped.extend(batch);
        }
        drop(file);
        self.gauge();
        Ok(())
    }

    /// Ships unacknowledged records to the sinks, then trims the spool.
    /// Returns how many were acknowledged.
    ///
    /// # Errors
    /// The sink's error; nothing is acknowledged and the next ship retries.
    pub async fn ship(&self) -> Result<usize> {
        let batch: Vec<AuditRecord> = {
            let s = lock(&self.inner.state);
            s.unshipped.iter().take(SHIP_BATCH).cloned().collect()
        };
        if batch.is_empty() {
            return Ok(0);
        }
        let mut streams: Vec<AuditStream> = batch.iter().map(|r| r.stream).collect();
        streams.sort();
        streams.dedup();
        for stream in &streams {
            let part: Vec<AuditRecord> = batch
                .iter()
                .filter(|r| r.stream == *stream)
                .cloned()
                .collect();
            let sink = match stream {
                AuditStream::Tenant(_) => &self.inner.tenant_sink,
                AuditStream::Platform => &self.inner.platform_sink,
            };
            if let Err(e) = sink.write(stream, &part).await {
                metrics::counter!("sprig_audit_ship_failures_total").increment(1);
                return Err(e);
            }
        }
        let last = batch.last().map_or(0, |r| r.seq);
        let n = batch.len();
        self.ack(last)?;
        Ok(n)
    }

    fn ack(&self, last: u64) -> Result<()> {
        let path = self.inner.dir.join("acked");
        let tmp = self.inner.dir.join("acked.tmp");
        fs::write(&tmp, format!("{last}\n"))
            .and_then(|()| File::open(&tmp).and_then(|f| f.sync_all()))
            .and_then(|()| fs::rename(&tmp, &path))
            .map_err(|e| io_error("can't write", &path, e))?;
        let mut done = Vec::new();
        {
            let mut s = lock(&self.inner.state);
            s.acked = last;
            while s.unshipped.front().is_some_and(|r| r.seq <= last) {
                s.unshipped.pop_front();
            }
            s.memory_only = s.memory_only.min(s.unshipped.len());
            // Every segment but the open one, once fully acknowledged.
            while s.segments.len() > 1 && s.segments.front().is_some_and(|g| g.last_seq <= last) {
                if let Some(g) = s.segments.pop_front() {
                    s.spool_bytes = s.spool_bytes.saturating_sub(g.bytes);
                    done.push(g.path);
                }
            }
        }
        for p in done {
            let _ = fs::remove_file(p);
        }
        // The open segment too, when everything in it is acknowledged:
        // the next flush starts a new one.
        {
            let mut file = lock(&self.inner.file);
            let mut s = lock(&self.inner.state);
            if s.queued.is_empty()
                && s.segments.len() == 1
                && s.segments.front().is_some_and(|g| g.last_seq <= last)
                && let Some(g) = s.segments.pop_front()
            {
                s.spool_bytes = s.spool_bytes.saturating_sub(g.bytes);
                *file = None;
                let _ = fs::remove_file(g.path);
            }
        }
        self.gauge();
        Ok(())
    }

    fn gauge(&self) {
        #[allow(clippy::cast_precision_loss)]
        {
            metrics::gauge!("sprig_audit_spool_bytes").set(self.spool_bytes() as f64);
            metrics::gauge!("sprig_audit_spool_max_bytes")
                .set(self.inner.max_bytes.load(Ordering::Relaxed) as f64);
        }
    }

    /// Starts the flusher (every [`FLUSH_PERIOD`] or [`FLUSH_EVENTS`]) and
    /// the shipper (every [`SHIP_PERIOD`]); both flush and ship once more
    /// when `stop` is cancelled.
    #[must_use = "the task flushes and ships until stop is cancelled; await it to know it finished"]
    pub fn spawn(&self, stop: CancellationToken) -> tokio::task::JoinHandle<()> {
        let p = self.clone();
        tokio::spawn(async move {
            let mut flush_every = tokio::time::interval(FLUSH_PERIOD);
            let mut ship_every = tokio::time::interval(SHIP_PERIOD);
            loop {
                tokio::select! {
                    () = stop.cancelled() => break,
                    _ = flush_every.tick() => p.flush_logged().await,
                    () = p.inner.wake.notified() => p.flush_logged().await,
                    _ = ship_every.tick() => {
                        p.flush_logged().await;
                        if let Err(e) = p.ship().await {
                            tracing::warn!(error = %e, backlog = p.backlog(), "audit ship failed; retrying");
                        }
                    }
                }
            }
            p.flush_logged().await;
            let _ = p.ship().await;
        })
    }

    async fn flush_logged(&self) {
        let p = self.clone();
        let r = tokio::task::spawn_blocking(move || p.flush()).await;
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "audit spool flush failed"),
            Err(e) => tracing::error!(error = %e, "audit spool flush panicked"),
        }
    }
}

static GLOBAL: OnceLock<Pipeline> = OnceLock::new();

/// Makes `p` the process's pipeline. Only the first call wins.
///
/// # Errors
/// `Internal` if one is already installed.
pub fn install(p: Pipeline) -> Result<()> {
    GLOBAL
        .set(p)
        .map_err(|_| SprigError::internal("an audit pipeline is already installed"))
}

/// The process's pipeline, if one is installed (`[audit] spool_dir` set).
#[must_use]
pub fn global() -> Option<&'static Pipeline> {
    GLOBAL.get()
}

/// Records an event in the process's pipeline; without one, nothing (the
/// producer's log line is then the only record).
pub fn record(stream: AuditStream, event: &str, body: serde_json::Value) {
    if let Some(p) = global() {
        p.emit(stream, event, body);
    }
}

/// `Err(39970)` while the process's pipeline can't take events durably;
/// `Ok` without a pipeline.
///
/// # Errors
/// 39970 when the spool is full.
pub fn check_available() -> Result<()> {
    global().map_or(Ok(()), Pipeline::check)
}

/// The directory sink: the audit object-storage layout under a directory,
/// `audit/<stream>/<YYYY-MM-DD>/<instance>-<first seq>-<last seq>.jsonl`.
/// A file is written to a temporary name, fsynced and renamed, so a write
/// is durable before `Ok`; writing the same records again replaces the
/// same file (deduplication on `(instance, seq)`).
#[derive(Debug, Clone)]
pub struct DirSink {
    root: PathBuf,
    dc: DcId,
}

impl DirSink {
    /// A sink under `root`, in `dc`.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>, dc: DcId) -> Self {
        Self {
            root: root.into(),
            dc,
        }
    }

    /// Every record under `root`, deduplicated on `(instance, seq)` and
    /// sorted by time.
    ///
    /// # Errors
    /// 39970 if the directory can't be read.
    pub fn read_all(root: &Path) -> Result<Vec<AuditRecord>> {
        let mut out = Vec::new();
        let base = root.join("audit");
        if !base.exists() {
            return Ok(out);
        }
        let mut stack = vec![base];
        while let Some(d) = stack.pop() {
            for e in fs::read_dir(&d).map_err(|e| io_error("can't read", &d, e))? {
                let e = e.map_err(|e| io_error("can't read", &d, e))?;
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().is_some_and(|x| x == "jsonl") {
                    let f = File::open(&p).map_err(|e| io_error("can't read", &p, e))?;
                    for line in BufReader::new(f).lines().map_while(std::result::Result::ok) {
                        if let Ok(r) = serde_json::from_str::<AuditRecord>(&line) {
                            out.push(r);
                        }
                    }
                }
            }
        }
        out.sort_by(|a, b| (a.at_ms, &a.instance, a.seq).cmp(&(b.at_ms, &b.instance, b.seq)));
        out.dedup_by(|a, b| a.instance == b.instance && a.seq == b.seq);
        Ok(out)
    }

    fn write_blocking(root: &Path, stream: AuditStream, batch: &[AuditRecord]) -> Result<()> {
        // One file per day of the batch.
        let mut days: Vec<(String, Vec<&AuditRecord>)> = Vec::new();
        for r in batch {
            let day = civil_date(r.at_ms);
            match days.iter_mut().find(|(d, _)| *d == day) {
                Some((_, v)) => v.push(r),
                None => days.push((day, vec![r])),
            }
        }
        for (day, recs) in days {
            let dir = root.join("audit").join(stream.to_string()).join(&day);
            fs::create_dir_all(&dir).map_err(|e| io_error("can't create", &dir, e))?;
            let (first, last) = (
                recs.first().map_or(0, |r| r.seq),
                recs.last().map_or(0, |r| r.seq),
            );
            let instance = recs.first().map_or("x", |r| r.instance.as_str());
            let name = format!("{instance}-{first:020}-{last:020}.jsonl");
            let path = dir.join(&name);
            let tmp = dir.join(format!(".{name}.tmp"));
            let mut text = Vec::new();
            for r in recs {
                if serde_json::to_writer(&mut text, r).is_ok() {
                    text.push(b'\n');
                }
            }
            let mut f = File::create(&tmp).map_err(|e| io_error("can't create", &tmp, e))?;
            f.write_all(&text)
                .and_then(|()| f.sync_all())
                .and_then(|()| fs::rename(&tmp, &path))
                .map_err(|e| io_error("can't write", &path, e))?;
            if let Ok(d) = File::open(&dir) {
                let _ = d.sync_all();
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl AuditSink for DirSink {
    async fn write(&self, stream: &AuditStream, batch: &[AuditRecord]) -> Result<()> {
        let root = self.root.clone();
        let stream = *stream;
        let batch = batch.to_vec();
        tokio::task::spawn_blocking(move || Self::write_blocking(&root, stream, &batch))
            .await
            .map_err(|e| SprigError::internal("audit sink task failed").with_source(e))?
    }

    fn dc(&self) -> &DcId {
        &self.dc
    }
}

/// `audit.retention_days` as the process resolves it (global and DC
/// scope): 0 until set, and no retention pass deletes anything then.
static RETENTION_DAYS: AtomicU64 = AtomicU64::new(0);

/// Sets the retention the process's [`DirSink`] pass keeps
/// (`sprig-settings::ops` applies `audit.retention_days` here, live).
pub fn set_retention_days(days: u64) {
    RETENTION_DAYS.store(days, Ordering::Relaxed);
}

/// The retention the pass keeps now; 0 when unset.
#[must_use]
pub fn retention_days() -> u64 {
    RETENTION_DAYS.load(Ordering::Relaxed)
}

/// Day directories a retention pass removes at most before it yields.
pub const RETENTION_BATCH: usize = 100;

/// How often the retention pass runs, and how soon it runs again after a
/// full batch.
pub const RETENTION_PERIOD: Duration = Duration::from_secs(3600);
const RETENTION_AGAIN: Duration = Duration::from_secs(10);

/// What one retention pass removed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PruneReport {
    /// Day directories removed, as `<stream>/<day>`.
    pub removed: Vec<String>,
    /// The pass stopped at its batch limit: more are due.
    pub more: bool,
}

impl DirSink {
    /// The retention pass (`audit.retention_days`): removes the day
    /// directories
    /// `audit/<stream>/<YYYY-MM-DD>` under `root` older than `days_for`
    /// the stream (by name, `platform` or `tenant-<id>`; `None` keeps the
    /// stream), oldest first, at most `max` of them; `now_ms` is today.
    /// A day is removed only once all of it is past retention (the cutoff
    /// is today minus the days, so a record is kept at least that long).
    ///
    /// # Errors
    /// 39970 if the directory can't be read; a removal that fails is
    /// logged and skipped.
    pub fn prune(
        root: &Path,
        now_ms: u64,
        days_for: &dyn Fn(&str) -> Option<u64>,
        max: usize,
    ) -> Result<PruneReport> {
        let mut report = PruneReport::default();
        let base = root.join("audit");
        if !base.exists() {
            return Ok(report);
        }
        let mut due: Vec<(String, String, PathBuf)> = Vec::new();
        for st in fs::read_dir(&base).map_err(|e| io_error("can't read", &base, e))? {
            let st = st.map_err(|e| io_error("can't read", &base, e))?;
            let stream = st.file_name().to_string_lossy().into_owned();
            let Some(days) = days_for(&stream).filter(|d| *d > 0) else {
                continue;
            };
            let cutoff = civil_date(now_ms.saturating_sub(days.saturating_mul(86_400_000)));
            let dir = st.path();
            if !dir.is_dir() {
                continue;
            }
            for d in fs::read_dir(&dir).map_err(|e| io_error("can't read", &dir, e))? {
                let d = d.map_err(|e| io_error("can't read", &dir, e))?;
                let day = d.file_name().to_string_lossy().into_owned();
                let is_day = day.len() == 10
                    && day.bytes().enumerate().all(|(i, b)| {
                        if i == 4 || i == 7 {
                            b == b'-'
                        } else {
                            b.is_ascii_digit()
                        }
                    });
                if is_day && day < cutoff && d.path().is_dir() {
                    due.push((day, stream.clone(), d.path()));
                }
            }
        }
        due.sort();
        for (day, stream, path) in due {
            if report.removed.len() >= max {
                report.more = true;
                break;
            }
            match fs::remove_dir_all(&path) {
                Ok(()) => report.removed.push(format!("{stream}/{day}")),
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "audit retention: can't remove");
                }
            }
        }
        Ok(report)
    }

    /// Runs [`prune`](Self::prune) under this sink's root every
    /// [`RETENTION_PERIOD`] (and again soon while a batch was full), with
    /// [`retention_days`] for every stream, until `stop`. Each pass is one
    /// blocking task of at most [`RETENTION_BATCH`] removals, so the pass
    /// never competes with the sink's writes for long.
    #[must_use]
    pub fn spawn_retention(&self, stop: CancellationToken) -> tokio::task::JoinHandle<()> {
        let root = self.root.clone();
        tokio::spawn(async move {
            // The first pass a minute after start, off the startup path.
            let mut wait = Duration::from_secs(60);
            loop {
                tokio::select! {
                    () = stop.cancelled() => return,
                    () = tokio::time::sleep(wait) => {}
                }
                wait = RETENTION_PERIOD;
                let days = retention_days();
                if days == 0 {
                    continue;
                }
                let root = root.clone();
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
                let r = tokio::task::spawn_blocking(move || {
                    Self::prune(&root, now, &|_| Some(days), RETENTION_BATCH)
                })
                .await;
                match r {
                    Ok(Ok(rep)) => {
                        if !rep.removed.is_empty() {
                            metrics::counter!("sprig_audit_retention_removed_total")
                                .increment(rep.removed.len() as u64);
                            tracing::info!(
                                removed = rep.removed.len(),
                                days,
                                more = rep.more,
                                "audit retention removed old days"
                            );
                        }
                        if rep.more {
                            wait = RETENTION_AGAIN;
                        }
                    }
                    Ok(Err(e)) => tracing::warn!(error = %e, "audit retention pass failed"),
                    Err(e) => tracing::warn!(error = %e, "audit retention pass panicked"),
                }
            }
        })
    }
}

/// Keeps records in memory, deduplicated on `(instance, seq)`; can be
/// made to fail. For tests.
#[derive(Debug)]
pub struct MemorySink {
    dc: DcId,
    records: Mutex<Vec<AuditRecord>>,
    failing: std::sync::atomic::AtomicBool,
}

impl MemorySink {
    /// An empty sink in `dc`.
    #[must_use]
    pub fn new(dc: DcId) -> Arc<Self> {
        Arc::new(Self {
            dc,
            records: Mutex::new(Vec::new()),
            failing: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Makes every write fail (`true`) or succeed.
    pub fn set_failing(&self, failing: bool) {
        self.failing.store(failing, Ordering::Relaxed);
    }

    /// The records so far.
    #[must_use]
    pub fn records(&self) -> Vec<AuditRecord> {
        lock(&self.records).clone()
    }
}

#[async_trait::async_trait]
impl AuditSink for MemorySink {
    async fn write(&self, _stream: &AuditStream, batch: &[AuditRecord]) -> Result<()> {
        if self.failing.load(Ordering::Relaxed) {
            return Err(unavailable("memory sink set to fail"));
        }
        let mut v = lock(&self.records);
        for r in batch {
            if !v.iter().any(|x| x.instance == r.instance && x.seq == r.seq) {
                v.push(r.clone());
            }
        }
        Ok(())
    }

    fn dc(&self) -> &DcId {
        &self.dc
    }
}

/// `YYYY-MM-DD` (UTC) of a Unix time in ms.
fn civil_date(ms: u64) -> String {
    let days = i64::try_from(ms / 86_400_000).unwrap_or(0);
    // Howard Hinnant's days-to-civil algorithm.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

#[cfg(test)]
mod tests;
