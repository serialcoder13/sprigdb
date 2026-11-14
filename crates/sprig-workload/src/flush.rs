//! The flusher: a background task per gateway that writes the
//! recorder's first sightings, open buckets, closed buckets and sealed
//! samples to each tenant's catalog, off the statement path.
//!
//! ```text
//! every workload.flush_seconds (900), and at each bucket's end + 0-60 s jitter:
//!   new     := recorder.drain_new()        INSERT IGNORE workload_fingerprints
//!   open    := recorder.snapshot()         REPLACE workload_stats (cumulative)
//!   closed  := recorder.drain_closed()     REPLACE workload_stats (final)
//!   samples := outbox.drain()              REPLACE workload_samples
//!   per tenant, in that order; a tenant that fails gets its closed rows,
//!   first sightings and samples back for the next flush
//! ```
//!
//! A flush is idempotent: every stats row is this gateway's cumulative
//! value under its own `(gateway_id, part)` key. **Late closed rows**
//! *(choice)*: a closed bucket that the rollup may already have merged
//! (it ended more than `workload.rollup_delay_minutes` ago, so the catalog
//! was unreachable for that long) is dropped and counted in
//! `sprig_workload_flush_late_dropped_total` rather than written: written
//! after a rollup, its partial values would be counted twice.
//!
//! [`CatalogTarget`] writes through a `sprig-catalog` locator and migrates
//! the `workload` module once per tenant first.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, UNIX_EPOCH};

use async_trait::async_trait;
use sprig_catalog::migrate::MigrateOpts;
use sprig_catalog::{CatalogLocator, Migrator};
use sprig_common::ids::TenantId;
use sprig_common::time::Clock;
use sprig_common::{CancellationToken, Result, SprigError};

use crate::recorder::{FlushRow, NewFingerprint, Recorder};
use crate::samples::{SampleOutbox, SealedSample};
use crate::store::{WORKLOAD_MODULE, WorkloadTables, map_err};

/// What one tenant gets in one flush.
#[derive(Debug, Default)]
pub struct TenantBatch {
    /// First sightings.
    pub fingerprints: Vec<NewFingerprint>,
    /// Open buckets' cumulative values and closed buckets.
    pub stats: Vec<FlushRow>,
    /// Sealed samples.
    pub samples: Vec<SealedSample>,
}

impl TenantBatch {
    fn is_empty(&self) -> bool {
        self.fingerprints.is_empty() && self.stats.is_empty() && self.samples.is_empty()
    }
}

/// Where flushes go.
#[async_trait]
pub trait FlushTarget: Send + Sync + 'static {
    /// Writes one tenant's batch: fingerprints, then stats, then samples.
    ///
    /// # Errors
    /// The catalog's errors; the flusher keeps what it can for later.
    async fn write(&self, tenant: TenantId, gateway_id: u32, batch: &TenantBatch) -> Result<()>;
}

/// What a flush did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FlushReport {
    /// Tenants written.
    pub tenants: u64,
    /// Tenants whose write failed (kept for later).
    pub failed: u64,
    /// Fingerprint rows sent.
    pub fingerprints: u64,
    /// Stats rows sent.
    pub stats: u64,
    /// Samples sent.
    pub samples: u64,
    /// Closed rows dropped as late (module docs).
    pub late_dropped: u64,
}

/// Settings the flusher reads on every run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushSettings {
    /// `workload.flush_seconds` (900).
    pub flush_seconds: u32,
    /// `workload.rollup_delay_minutes` (10).
    pub rollup_delay_minutes: u32,
}

impl Default for FlushSettings {
    fn default() -> Self {
        Self {
            flush_seconds: 900,
            rollup_delay_minutes: 10,
        }
    }
}

/// The per-gateway flusher.
pub struct Flusher {
    recorder: Arc<Recorder>,
    samples: Arc<SampleOutbox>,
    target: Arc<dyn FlushTarget>,
    gateway_id: u32,
    clock: Arc<dyn Clock>,
    settings: Arc<dyn Fn() -> FlushSettings + Send + Sync>,
    pending_new: Mutex<Vec<NewFingerprint>>,
    /// One flush at a time.
    running: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for Flusher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Flusher")
            .field("gateway_id", &self.gateway_id)
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Flusher {
    /// A flusher of `recorder` and `samples` into `target`, as gateway
    /// `gateway_id`; `settings` is read on every run.
    #[must_use]
    pub fn new(
        recorder: Arc<Recorder>,
        samples: Arc<SampleOutbox>,
        target: Arc<dyn FlushTarget>,
        gateway_id: u32,
        clock: Arc<dyn Clock>,
        settings: Arc<dyn Fn() -> FlushSettings + Send + Sync>,
    ) -> Self {
        Self {
            recorder,
            samples,
            target,
            gateway_id,
            clock,
            settings,
            pending_new: Mutex::new(Vec::new()),
            running: tokio::sync::Mutex::new(()),
        }
    }

    fn now_secs(&self) -> u64 {
        self.clock
            .wall()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs())
    }

    fn bucket_secs(&self) -> u64 {
        u64::from(self.recorder.config().bucket_minutes.max(1)) * 60
    }

    /// One flush (module docs).
    pub async fn flush(&self) -> FlushReport {
        let _one = self.running.lock().await;
        let started = Instant::now();
        let st = (self.settings)();
        let now = self.now_secs();
        let bucket = self.bucket_secs();
        let current = now - now % bucket;
        let mut rep = FlushReport::default();

        let mut new = std::mem::take(&mut *lock(&self.pending_new));
        new.extend(self.recorder.drain_new());
        let open = self.recorder.snapshot();
        let mut closed = self.recorder.drain_closed();
        // Closed buckets the rollup may have merged already: dropped.
        let late_before = now.saturating_sub(bucket + u64::from(st.rollup_delay_minutes) * 60);
        let before = closed.len();
        closed.retain(|r| r.bucket_start >= late_before);
        rep.late_dropped = (before - closed.len()) as u64;
        let samples = self.samples.drain(current);

        let mut by: BTreeMap<TenantId, (TenantBatch, usize)> = BTreeMap::new();
        for f in new {
            by.entry(f.tenant).or_default().0.fingerprints.push(f);
        }
        for r in open {
            by.entry(r.tenant).or_default().0.stats.push(r);
        }
        for r in closed {
            let e = by.entry(r.tenant).or_default();
            e.0.stats.push(r);
            e.1 += 1;
        }
        for (t, s) in samples {
            by.entry(t).or_default().0.samples.push(s);
        }
        for (tenant, (batch, n_closed)) in by {
            if batch.is_empty() {
                continue;
            }
            match self.target.write(tenant, self.gateway_id, &batch).await {
                Ok(()) => {
                    rep.tenants += 1;
                    rep.fingerprints += batch.fingerprints.len() as u64;
                    rep.stats += batch.stats.len() as u64;
                    rep.samples += batch.samples.len() as u64;
                }
                Err(e) => {
                    rep.failed += 1;
                    tracing::warn!(tenant = %tenant, error = %e,
                        "workload flush failed; keeping the rows for the next flush");
                    let TenantBatch {
                        fingerprints,
                        mut stats,
                        samples,
                    } = batch;
                    lock(&self.pending_new).extend(fingerprints);
                    // Open rows (first) are cumulative: the next snapshot
                    // has them. Closed rows (last) are kept.
                    let closed: Vec<FlushRow> = stats.split_off(stats.len() - n_closed);
                    self.recorder.requeue_closed(closed);
                    self.samples
                        .requeue(samples.into_iter().map(|s| (tenant, s)).collect());
                }
            }
        }
        let m = self.recorder.metrics();
        metrics::histogram!("sprig_workload_flush_duration_seconds")
            .record(started.elapsed().as_secs_f64());
        metrics::counter!("sprig_workload_flush_rows_total")
            .increment(rep.stats + rep.fingerprints + rep.samples);
        metrics::counter!("sprig_workload_flush_late_dropped_total").increment(rep.late_dropped);
        #[allow(clippy::cast_precision_loss)]
        {
            metrics::gauge!("sprig_workload_fingerprints_active").set(m.fingerprints_active as f64);
            metrics::gauge!("sprig_workload_memory_bytes").set(m.memory_bytes as f64);
        }
        metrics::counter!("sprig_workload_other_total").absolute(m.other_total);
        metrics::counter!("sprig_workload_probes_total").absolute(m.probes_total);
        metrics::counter!("sprig_workload_samples_total").absolute(m.samples_total);
        metrics::counter!("sprig_workload_flush_deferred_total").absolute(m.dropped_rows_total);
        rep
    }

    /// How long until the next flush: the period, or the current bucket's
    /// end plus `jitter`, whichever comes first.
    #[must_use]
    pub fn next_delay(&self, jitter: Duration) -> Duration {
        let period = Duration::from_secs(u64::from((self.settings)().flush_seconds.max(1)));
        let now = self.now_secs();
        let bucket = self.bucket_secs();
        let to_end = Duration::from_secs(bucket - now % bucket) + jitter;
        period.min(to_end)
    }

    /// Flushes until `shutdown`, then once more (as the gateway drains).
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        let mut seed = self.now_secs() ^ u64::from(self.gateway_id).rotate_left(32);
        loop {
            seed = crate::recorder::mix(seed.wrapping_add(1));
            let jitter = Duration::from_secs(seed % 61);
            tokio::select! {
                () = shutdown.cancelled() => break,
                () = tokio::time::sleep(self.next_delay(jitter)) => {}
            }
            let r = self.flush().await;
            tracing::debug!(?r, "workload flush");
        }
        let r = self.flush().await;
        tracing::info!(?r, "final workload flush");
    }
}

/// Writes flushes to each tenant's catalog writer.
pub struct CatalogTarget {
    locator: Arc<dyn CatalogLocator>,
    migrated: Mutex<HashSet<TenantId>>,
    applied_by: String,
}

impl std::fmt::Debug for CatalogTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogTarget").finish_non_exhaustive()
    }
}

impl CatalogTarget {
    /// A target over `locator`; `applied_by` names this process in the
    /// migration history.
    #[must_use]
    pub fn new(locator: Arc<dyn CatalogLocator>, applied_by: impl Into<String>) -> Self {
        Self {
            locator,
            migrated: Mutex::new(HashSet::new()),
            applied_by: applied_by.into(),
        }
    }
}

/// Migrates the `workload` module on `conn`'s catalog `schema`.
///
/// # Errors
/// The migrator's errors.
pub async fn migrate(conn: &mut mysql_async::Conn, schema: &str, applied_by: &str) -> Result<()> {
    Migrator::new(&[WORKLOAD_MODULE])?
        .run(
            conn,
            schema,
            &MigrateOpts {
                applied_by: applied_by.to_owned(),
                ..MigrateOpts::default()
            },
        )
        .await?;
    Ok(())
}

#[async_trait]
impl FlushTarget for CatalogTarget {
    async fn write(&self, tenant: TenantId, gateway_id: u32, batch: &TenantBatch) -> Result<()> {
        let ep = self.locator.writer(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
        let schema = ep.store.schema().to_owned();
        if !lock(&self.migrated).contains(&tenant) {
            migrate(&mut conn, &schema, &self.applied_by).await?;
            lock(&self.migrated).insert(tenant);
        }
        let t = WorkloadTables::new(schema);
        t.insert_fingerprints(&mut conn, &batch.fingerprints)
            .await?;
        t.replace_stats(&mut conn, gateway_id, &batch.stats).await?;
        t.replace_samples(&mut conn, &batch.samples).await?;
        Ok(())
    }
}

/// A target that keeps every batch in memory (tests).
#[derive(Debug, Default)]
pub struct MemoryTarget {
    /// (tenant, gateway, batch) in write order.
    pub batches: Mutex<Vec<(TenantId, u32, TenantBatch)>>,
    /// Tenants whose writes fail.
    pub failing: Mutex<HashSet<TenantId>>,
}

#[async_trait]
impl FlushTarget for MemoryTarget {
    async fn write(&self, tenant: TenantId, gateway_id: u32, batch: &TenantBatch) -> Result<()> {
        if lock(&self.failing).contains(&tenant) {
            return Err(SprigError::new(
                sprig_common::SprigCode::CatalogUnavailable,
                "test: catalog down",
            ));
        }
        lock(&self.batches).push((
            tenant,
            gateway_id,
            TenantBatch {
                fingerprints: batch.fingerprints.clone(),
                stats: batch.stats.clone(),
                samples: batch.samples.clone(),
            },
        ));
        Ok(())
    }
}

#[cfg(test)]
mod tests;
