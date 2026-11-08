//! Chapter 24's Admin API calls: moving a tenant to another data center
//! and its traffic window (see [`crate::migrate`] and
//! [`crate::traffic`]).
//!
//! ```text
//! POST /admin/v1/tenants/{name}:move     {"to": "fra1", "max_pause_ms": 5000, "dry_run": false}
//! GET  /admin/v1/tenants/{name}/move     the move in progress, or the last one
//! GET  /admin/v1/tenants/{name}/traffic  the window and the R0 decision
//! POST /admin/v1/traffic/reports         gateways' counts (no bearer token:
//!                                        counts only, on the private Admin port)
//! ```
//!
//! A move is answered by the leader of the tenant's home DC (39504
//! elsewhere in that DC; 39550 naming the home DC from another DC). A
//! tenant pinned with `sprigctl tenant pin` (chapter 11's pin, which
//! chapter 47 makes the one pin for placement and DC) doesn't move.

use std::sync::Arc;

use sprig_common::Result;

use super::{AdminService, invalid};
use crate::migrate::{DcMoves, MoveTenantRequest, MoveView};
use crate::traffic::{
    MIN_IMPROVEMENT_PCT, MIN_SIGNAL_REQUESTS, TrafficBatch, TrafficView, WINDOW_MINUTES,
    WRITE_WEIGHT, unix_minute,
};

impl AdminService {
    /// The same service with chapter 24's calls.
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

    /// A tenant's traffic window and the decision R0 would show.
    ///
    /// # Errors
    /// The tenant lookup's errors.
    pub async fn tenant_traffic(&self, name: &str) -> Result<TrafficView> {
        let m = self.moves()?;
        let t = self.get_tenant(name).await?;
        Ok(m.traffic.view(
            t.tenant_id,
            name,
            &t.home_dc,
            unix_minute(),
            m.setting(&WINDOW_MINUTES),
            m.setting(&WRITE_WEIGHT),
            m.setting(&MIN_SIGNAL_REQUESTS),
            m.setting(&MIN_IMPROVEMENT_PCT),
        ))
    }
}
