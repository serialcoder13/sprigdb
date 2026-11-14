//! The DC failover Admin API calls: manual failover, standbys and failover
//! records (see [`crate::failover::manual`]).
//!
//! ```text
//! POST /admin/v1/tenants/{name}:failover   {"to": "ams1", "accept_data_loss": false,
//!                                           "old_home_fenced": false, "dry_run": false}
//! POST /admin/v1/failovers:all             {"from": "fra1", "to": "ams1", ...}
//! POST /admin/v1/tenants/{name}:standby    {"dc": "ams1", "cluster": "ams1-c1"}
//! GET  /admin/v1/tenants/{name}/failovers  the tenant's failovers and diverged reports
//! GET  /admin/v1/failovers                 every failover record, newest first
//! ```
//!
//! Writes are answered by the DC leader of the controller asked (39504
//! elsewhere): point `--endpoint` at a controller of the new home, since
//! the old one may be gone.

use std::sync::Arc;

use sprig_common::Result;

use super::{AdminService, invalid};
use crate::failover::manual::{
    ErrantView, FailoverAllRequest, FailoverRequest, FailoverView, StandbyRequest,
};
use crate::failover::record::FailoverRecord;
use crate::failover::role::FailoverRole;

impl AdminService {
    /// The same service with the failover calls.
    #[must_use]
    pub fn with_failover(mut self, f: Arc<FailoverRole>) -> Self {
        self.failover = Some(f);
        self
    }

    fn failover(&self) -> Result<&Arc<FailoverRole>> {
        self.failover
            .as_ref()
            .ok_or_else(|| invalid("this controller doesn't serve failovers"))
    }

    /// `Tenants.FailoverTenant`.
    ///
    /// # Errors
    /// 39504 off the leader (unless a dry run); as
    /// [`FailoverRole::failover_tenant`].
    pub async fn failover_tenant(&self, name: &str, req: FailoverRequest) -> Result<FailoverView> {
        let f = self.failover()?;
        if !req.dry_run {
            self.leader_guard().await?;
        }
        f.failover_tenant(name, &req).await
    }

    /// Every tenant of a DC.
    ///
    /// # Errors
    /// As [`Self::failover_tenant`].
    pub async fn failover_all(&self, req: FailoverAllRequest) -> Result<Vec<FailoverView>> {
        let f = self.failover()?;
        if !req.dry_run {
            self.leader_guard().await?;
        }
        f.failover_all(&req).await
    }

    /// `Tenants.AddStandby`.
    ///
    /// # Errors
    /// 39504 off the leader; as [`FailoverRole::add_standby`].
    pub async fn add_standby(&self, name: &str, req: StandbyRequest) -> Result<serde_json::Value> {
        let f = self.failover()?;
        self.leader_guard().await?;
        f.add_standby(name, &req).await
    }

    /// A tenant's failovers and diverged-transaction reports.
    ///
    /// # Errors
    /// As [`FailoverRole::errant`].
    pub async fn tenant_failovers(&self, name: &str) -> Result<ErrantView> {
        self.failover()?.errant(name).await
    }

    /// `DataCenters.DrainDc` and `UndrainDc`.
    ///
    /// # Errors
    /// 39503 for an unknown DC; a DC in another state; the store's errors.
    pub async fn set_dc_draining(
        &self,
        dc: &str,
        drain: bool,
    ) -> Result<sprig_meta::bootstrap::DcRecord> {
        let dc = sprig_common::ids::DcId::new(dc).map_err(|e| invalid(e.to_string()))?;
        crate::latency::set_draining(&*self.stores.global, &dc, drain).await
    }

    /// The measured latency matrix.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn latency_matrix(&self) -> Result<serde_json::Value> {
        let m = crate::latency::Matrix::read(&*self.stores.global).await?;
        let rows: Vec<serde_json::Value> = m
            .rows()
            .iter()
            .map(|((a, b), r)| {
                serde_json::json!({"from": a.as_str(), "to": b.as_str(), "p50_ms": r.p50_ms,
                    "p90_ms": r.p90_ms, "p99_ms": r.p99_ms, "reachable": r.reachable,
                    "samples": r.samples, "at": r.at})
            })
            .collect();
        Ok(serde_json::json!({"rows": rows}))
    }

    /// Every failover record.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn failovers(&self) -> Result<Vec<FailoverRecord>> {
        self.failover()?.failovers().await
    }
}
