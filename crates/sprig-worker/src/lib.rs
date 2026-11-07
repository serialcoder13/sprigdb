//! `sprig-worker`: the worker role, the job runtime.
//!
//! Layer 3 (roles). Design: [chapter 10](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/10-mover.md) (The mover) and [chapter 13](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/13-auto-indexing.md) (Automatic indexing) of the SprigDB low-level design.
//!
//! **Skeleton.** [`run`] starts, reports ready, waits for shutdown and
//! drains; it does no worker work: no job runtime yet (chapter 23 step 6, chapters 10 and 13).

use sprig_common::{Result, Role, RoleContext};

/// Runs the worker role until `ctx.shutdown` is cancelled.
///
/// The `sprigdb` binary has already bound the telemetry port, so
/// `/healthz` answers and `/readyz` reports `starting` until this marks the
/// process ready.
///
/// # Errors
/// None yet; later, failures that stop the role.
pub async fn run(ctx: RoleContext) -> Result<()> {
    let role = Role::Worker;
    tracing::warn!(%role, "skeleton only: no job runtime yet (chapter 23 step 6, chapters 10 and 13)");
    ctx.health.set_ready();
    tracing::info!(%role, "ready");

    ctx.shutdown.cancelled().await;

    ctx.health.set_draining();
    tracing::info!(%role, "draining: nothing to drain yet");
    Ok(())
}
