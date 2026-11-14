//! The Admin API calls for moving a tenant to another data center
//! and its traffic window (see [`crate::migrate`] and
//! [`crate::traffic`]).
//!
//! ```text
//! POST /admin/v1/tenants/{name}:move     {"to": "fra1", "max_pause_ms": 5000, "dry_run": false}
//! GET  /admin/v1/tenants/{name}/move     the move in progress, or the last one
//! POST /admin/v1/tenants/{name}:pin      pinned_dc = the home DC (and :unpin);
//!                                        `sprigctl tenant pin` calls it
//! POST /admin/v1/tenants/{name}:residency {"policy": "...", "deny_remote_access": true,
//!                                        "apply": false}: the residency plan, or apply
//! GET  /admin/v1/tenants/{name}/residency the policy, allowed DCs and state
//! GET  /admin/v1/tenants/{name}/traffic  the window and the decision
//! POST /admin/v1/traffic/reports         gateways' counts (no bearer token:
//!                                        counts only, on the private Admin port)
//! ```
//!
//! A move is answered by the leader of the tenant's home DC (39504
//! elsewhere in that DC; 39550 naming the home DC from another DC). A
//! tenant pinned with `sprigctl tenant pin` (the balancer's pin, the one
//! pin for both placement and DC) doesn't move.

use std::sync::Arc;

use sprig_common::Result;

use super::{AdminService, invalid};
use crate::migrate::{DcMoves, MoveTenantRequest, MoveView};
use crate::traffic::{
    MIN_IMPROVEMENT_PCT, MIN_SIGNAL_REQUESTS, TrafficBatch, TrafficView, WINDOW_MINUTES,
    WRITE_WEIGHT, unix_minute,
};

impl AdminService {
    /// The same service with the DC move calls.
    #[must_use]
    pub fn with_moves(mut self, moves: Arc<DcMoves>) -> Self {
        self.moves = Some(moves);
        self
    }

    fn moves(&self) -> Result<&Arc<DcMoves>> {
        self.moves
            .as_ref()
            .ok_or_else(|| invalid("this controller doesn't serve tenant moves"))
    }

    /// `Tenants.MoveTenant`.
    ///
    /// # Errors
    /// 39504 off the leader; as [`DcMoves::start`].
    pub async fn move_tenant(&self, name: &str, req: MoveTenantRequest) -> Result<MoveView> {
        let moves = self.moves()?;
        let t = self.get_tenant(name).await?;
        if t.home_dc == *self.dc() && !req.dry_run {
            self.leader_guard().await?;
        }
        moves.start(t.tenant_id, name, &req).await
    }

    /// The move in progress or the last one.
    ///
    /// # Errors
    /// The tenant lookup's errors.
    pub async fn tenant_move(&self, name: &str) -> Result<MoveView> {
        let t = self.get_tenant(name).await?;
        self.moves()?.status(t.tenant_id, name).await
    }

    /// The same service with the residency calls.
    #[must_use]
    pub fn with_residency(mut self, r: Arc<crate::residency::Residency>) -> Self {
        self.residency = Some(r);
        self
    }

    fn residency(&self) -> Result<&Arc<crate::residency::Residency>> {
        self.residency
            .as_ref()
            .ok_or_else(|| invalid("this controller doesn't serve residency"))
    }

    /// `Tenants.SetResidency` (`:residency`): the plan, and with `apply`
    /// the policy.
    ///
    /// # Errors
    /// As [`crate::residency::Residency::set`].
    pub async fn set_residency(
        &self,
        name: &str,
        req: crate::residency::ResidencyRequest,
    ) -> Result<crate::residency::ResidencyView> {
        self.residency()?.set(name, &req).await
    }

    /// `Tenants.GetResidency`.
    ///
    /// # Errors
    /// As [`crate::residency::Residency::show`].
    pub async fn get_residency(&self, name: &str) -> Result<crate::residency::ResidencyView> {
        self.residency()?.show(name).await
    }

    /// Pins `name` to its home DC, or unpins it (`:pin`, `:unpin`).
    ///
    /// # Errors
    /// As [`DcMoves::pin`].
    pub async fn pin_tenant_dc(&self, name: &str, pin: bool) -> Result<MoveView> {
        let t = self.get_tenant(name).await?;
        self.moves()?.pin(t.tenant_id, name, pin).await
    }

    /// Adds a gateway's traffic report.
    ///
    /// # Errors
    /// None in practice.
    pub fn traffic_report(&self, batch: &TrafficBatch) -> Result<serde_json::Value> {
        let m = self.moves()?;
        m.traffic.add(batch, m.setting(&WINDOW_MINUTES));
        metrics::counter!("sprig_controller_traffic_reports_total").increment(1);
        Ok(serde_json::json!({"rows": batch.rows.len()}))
    }

    /// A tenant's traffic window and the decision it would show.
    ///
    /// # Errors
    /// The tenant lookup's errors.
    pub async fn tenant_traffic(&self, name: &str) -> Result<TrafficView> {
        let m = self.moves()?;
        let t = self.get_tenant(name).await?;
        // Weigh by the measured latency matrix when it has rows.
        let matrix = crate::latency::Matrix::read(&*self.stores.global)
            .await
            .unwrap_or_default();
        let rtt = |a: &str, b: &str| matrix.rtt(a, b);
        let mut v = m.traffic.view_with_rtt(
            t.tenant_id,
            name,
            &t.home_dc,
            unix_minute(),
            m.setting(&WINDOW_MINUTES),
            m.setting(&WRITE_WEIGHT),
            m.setting(&MIN_SIGNAL_REQUESTS),
            m.setting(&MIN_IMPROVEMENT_PCT),
            (!matrix.is_empty()).then_some(&rtt as &crate::traffic::RttFn<'_>),
        );
        // What the automatic decision did last.
        v.auto = m.decisions.last(t.tenant_id);
        Ok(v)
    }
}
