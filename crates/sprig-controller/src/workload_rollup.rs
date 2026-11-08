//! Chapter 12's rollup and retention job (build step 4), as a DC-leader
//! loop over the tenants homed here.
//!
//! Every [`INTERVAL`] each active tenant's workload tables are maintained
//! with `sprig_workload::store::maintain`: closed buckets (ended at least
//! `workload.rollup_delay_minutes` ago) are merged into one row per
//! fingerprint, rolled-up hours older than 7 days become day rows, and
//! retention deletes stats past `workload.retention_days`, expired
//! samples, samples past `workload.max_samples_per_fingerprint` and
//! fingerprints not seen within the retention. Each batch is one
//! transaction, so a failed run is rerun from where it stopped.
//!
//! Where this differs from the book: chapter 12 runs it as a worker job;
//! there is no worker job runtime yet, so the DC leader runs it, like
//! chapter 13's builds. Settings are resolved at global and dc scope;
//! tenant-scope values of the tenant keys wait for the job to read each
//! catalog's settings.

use std::sync::Arc;
use std::time::Duration;

use sprig_catalog::CatalogLocator;
use sprig_common::Result;
use sprig_common::ids::{DcId, TenantId};
use sprig_common::time::Clock;
use sprig_settings::{LiveSettings, SettingCtx};
use sprig_workload::settings as ws;
use sprig_workload::store::{Retention, maintain};

use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};
use crate::tree::{Directory, DirectoryEntry, TenantState};

/// How often each tenant is maintained.
pub const INTERVAL: Duration = Duration::from_secs(300);

/// The rollup and retention loop (module docs).
pub struct WorkloadRollup {
    dir: Directory,
    locator: Arc<dyn CatalogLocator>,
    dc: DcId,
    live: Option<LiveSettings>,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for WorkloadRollup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkloadRollup")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

impl WorkloadRollup {
    /// The loop over the tenants homed in `dc`.
    #[must_use]
    pub fn new(
        dir: Directory,
        locator: Arc<dyn CatalogLocator>,
        dc: DcId,
        live: Option<LiveSettings>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            dir,
            locator,
            dc,
            live,
            clock,
        }
    }

    fn ours(&self, e: &DirectoryEntry) -> bool {
        e.home_dc == self.dc && e.state == TenantState::Active
    }

    fn retention(&self) -> Retention {
        let Some(live) = &self.live else {
            return Retention::default();
        };
        let ctx = SettingCtx::process(&self.dc);
        Retention {
            bucket_minutes: live.get(&ws::BUCKET_MINUTES, &ctx),
            rollup_delay_minutes: live.get(&ws::ROLLUP_DELAY_MINUTES, &ctx),
            retention_days: live.get(&ws::RETENTION_DAYS, &ctx),
            max_samples_per_fingerprint: live.get(&ws::MAX_SAMPLES_PER_FINGERPRINT, &ctx),
        }
    }
}

#[async_trait::async_trait]
impl Reconciler for WorkloadRollup {
    type Key = TenantId;

    fn name(&self) -> &'static str {
        "workload_rollup"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        vec![WatchSpec::global("tenants/")]
    }

    fn keys_for(&self, ev: &MetaEvent) -> Vec<TenantId> {
        self.dir.tenant_of(&ev.key).into_iter().collect()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<TenantId>> {
        Ok(self
            .dir
            .list()
            .await?
            .iter()
            .map(crate::tree::StoredEntry::body)
            .filter(|e| self.ours(e))
            .map(|e| e.tenant_id)
            .collect())
    }

    async fn reconcile(&self, _cx: &LoopCx, key: &TenantId) -> Result<Outcome> {
        let Some(entry) = self.dir.get(*key).await? else {
            return Ok(Outcome::Done);
        };
        if !self.ours(entry.body()) {
            return Ok(Outcome::Done);
        }
        let started = std::time::Instant::now();
        let rep = maintain(
            &*self.locator,
            *key,
            self.clock.wall(),
            &self.retention(),
            "controller:workload_rollup",
        )
        .await?;
        metrics::histogram!("sprig_workload_rollup_duration_seconds")
            .record(started.elapsed().as_secs_f64());
        metrics::counter!("sprig_workload_rollup_merged_total").increment(rep.merged);
        if rep != sprig_workload::store::MaintenanceReport::default() {
            tracing::debug!(tenant = %key, ?rep, "workload rollup and retention");
        }
        Ok(Outcome::RequeueAfter(INTERVAL))
    }
}
