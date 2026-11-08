//! The workload store over tenant catalogs: chapter 12's `WorkloadStore`
//! for its readers (chapter 13's trigger evaluation and validation), and
//! the rollup and retention run for one tenant.

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use sprig_catalog::CatalogLocator;
use sprig_common::Result;
use sprig_common::ids::TenantId;

use super::{MaintenanceReport, Retention, WorkloadTables, map_err};
use crate::fingerprint::FingerprintId;
use crate::keyring::TenantKeyring;
use crate::samples::{OpenedSample, SealedSample, open};
use crate::window::FpWindowStats;

/// Chapter 12's `WorkloadStore`: what the advisor, placement and
/// `sprigctl explain query` read.
#[async_trait]
pub trait WorkloadStore: Send + Sync {
    /// Every fingerprint of `tenant` with runs in buckets starting in
    /// `[from, to)`, merged over the window (sums, maxima, merged
    /// histograms, so [`FpWindowStats::p95_ms`] is the window's p95, and
    /// the union of scan sessions). Samples are not filled in.
    ///
    /// # Errors
    /// The catalog's errors.
    async fn window(
        &self,
        tenant: TenantId,
        from: SystemTime,
        to: SystemTime,
    ) -> Result<Vec<FpWindowStats>>;

    /// Up to `max` sealed samples of a fingerprint, newest first. They
    /// stay sealed: only the replay harness opens them
    /// ([`WorkloadStore::open_samples`]).
    ///
    /// # Errors
    /// The catalog's errors.
    async fn samples(
        &self,
        tenant: TenantId,
        fp: FingerprintId,
        max: usize,
    ) -> Result<Vec<SealedSample>>;

    /// The replay harness's decrypt API: up to `max` samples of a
    /// fingerprint, opened in memory with `keyring`. Samples that don't
    /// open are skipped (and counted in
    /// `sprig_workload_samples_unopened_total`). Never log what this
    /// returns.
    ///
    /// # Errors
    /// The catalog's errors.
    async fn open_samples(
        &self,
        keyring: &dyn TenantKeyring,
        tenant: TenantId,
        fp: FingerprintId,
        max: usize,
    ) -> Result<Vec<OpenedSample>> {
        let sealed = self.samples(tenant, fp, max).await?;
        Ok(sealed
            .iter()
            .filter_map(|s| match open(keyring, tenant, s) {
                Ok(o) => Some(o),
                Err(e) => {
                    metrics::counter!("sprig_workload_samples_unopened_total").increment(1);
                    tracing::debug!(fingerprint = %fp, sample = s.sample_id, error = %e,
                        "a workload sample didn't open; skipped");
                    None
                }
            })
            .collect())
    }
}

/// [`WorkloadStore`] over each tenant's catalog, found by a
/// `sprig-catalog` locator: reads go to the catalog's reader (a healthy
/// local replica, else the writer). A catalog without the workload tables
/// (never migrated: no traffic yet) reads as empty.
#[derive(Clone)]
pub struct CatalogWorkloadStore {
    locator: Arc<dyn CatalogLocator>,
}

impl std::fmt::Debug for CatalogWorkloadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogWorkloadStore")
            .finish_non_exhaustive()
    }
}

impl CatalogWorkloadStore {
    /// A store over `locator`.
    #[must_use]
    pub fn new(locator: Arc<dyn CatalogLocator>) -> Self {
        Self { locator }
    }
}

/// 1146 (no such table) reads as empty.
fn missing_table(e: &sprig_common::SprigError) -> bool {
    matches!(e.kind, sprig_common::ErrorKind::Backend { errno: 1146, .. })
}

#[async_trait]
impl WorkloadStore for CatalogWorkloadStore {
    async fn window(
        &self,
        tenant: TenantId,
        from: SystemTime,
        to: SystemTime,
    ) -> Result<Vec<FpWindowStats>> {
        let ep = self.locator.reader(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
        let t = WorkloadTables::new(ep.store.schema());
        match t.window(&mut conn, tenant, from, to).await {
            Err(e) if missing_table(&e) => Ok(Vec::new()),
            r => r,
        }
    }

    async fn samples(
        &self,
        tenant: TenantId,
        fp: FingerprintId,
        max: usize,
    ) -> Result<Vec<SealedSample>> {
        let ep = self.locator.reader(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
        let t = WorkloadTables::new(ep.store.schema());
        match t.samples(&mut conn, fp, max).await {
            Err(e) if missing_table(&e) => Ok(Vec::new()),
            r => r,
        }
    }
}

/// The rollup and retention job for one tenant (chapter 12, step 4 of
/// "In-memory aggregation and flushing"): migrates the module if needed,
/// merges closed buckets, then applies retention. `now` is the wall
/// clock. Idempotent: run it from a controller loop every few minutes.
///
/// # Errors
/// The catalog's errors.
pub async fn maintain(
    locator: &dyn CatalogLocator,
    tenant: TenantId,
    now: SystemTime,
    r: &Retention,
    applied_by: &str,
) -> Result<MaintenanceReport> {
    let ep = locator.writer(tenant).await?;
    let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
    let schema = ep.store.schema().to_owned();
    crate::flush::migrate(&mut conn, &schema, applied_by).await?;
    let t = WorkloadTables::new(schema);
    let secs = now
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let mut rep = t.rollup(&mut conn, secs, r).await?;
    let ret = t.retain(&mut conn, secs, r).await?;
    rep.expired_stats = ret.expired_stats;
    rep.expired_samples = ret.expired_samples;
    rep.expired_fingerprints = ret.expired_fingerprints;
    Ok(rep)
}
