//! `sprig-operator`: the operator role, the Kubernetes operator.
//!
//! Layer 3 (roles). Design: [chapter 27](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/27-provisioning.md) (Provisioning) of the SprigDB low-level design.
//!
//! **Skeleton.** [`run`] starts, reports ready, waits for shutdown and
//! drains; it does no operator work: no CRDs or reconcilers yet (chapter 27 step 10).

use sprig_common::{Result, Role, RoleContext};

/// Runs the operator role until `ctx.shutdown` is cancelled.
///
/// The `sprigdb` binary has already bound the telemetry port, so
/// `/healthz` answers and `/readyz` reports `starting` until this marks the
/// process ready.
///
/// # Errors
/// None yet; later, failures that stop the role.
pub async fn run(ctx: RoleContext) -> Result<()> {
    let role = Role::Operator;
    tracing::warn!(%role, "skeleton only: no CRDs or reconcilers yet (chapter 27 step 10)");
    ctx.health.set_ready();
    tracing::info!(%role, "ready");

    ctx.shutdown.cancelled().await;

    ctx.health.set_draining();
    tracing::info!(%role, "draining: nothing to drain yet");
    Ok(())
}
