//! `sprig-controller`: the controller role, reconcilers, leader election and the decision log.
//!
//! Layer 3 (roles). Design: [chapter 23](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/23-control-plane.md) (Control plane) of the SprigDB low-level design.
//!
//! **Skeleton.** [`run`] starts, reports ready, waits for shutdown and
//! drains; it does no controller work: no `MetaStore` connection, election or reconcilers yet (chapter 23 steps 3-7).

use sprig_common::{Result, Role, RoleContext};

/// Runs the controller role until `ctx.shutdown` is cancelled.
///
/// The `sprigdb` binary has already bound the telemetry port, so
/// `/healthz` answers and `/readyz` reports `starting` until this marks the
/// process ready.
///
/// # Errors
/// None yet; later, failures that stop the role.
pub async fn run(ctx: RoleContext) -> Result<()> {
    let role = Role::Controller;
    tracing::warn!(%role, "skeleton only: no MetaStore connection, election or reconcilers yet (chapter 23 steps 3-7)");
    ctx.health.set_ready();
    tracing::info!(%role, "ready");

    ctx.shutdown.cancelled().await;

    ctx.health.set_draining();
    tracing::info!(%role, "draining: nothing to drain yet");
    Ok(())
}
