//! `sprig-gateway`: the gateway role, listeners, sessions, front-end glue, backend pools and read routing.
//!
//! Layer 3 (roles). Design: [chapter 04](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/04-gateway.md) (Gateway) and [chapter 06](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/06-backend.md) (Backend) of the SprigDB low-level design.
//!
//! What exists:
//! - [`listener`]: the listener manager (build step 2): specs from
//!   `listener.<name>.*`, accept loops, PROXY v1/v2, TLS modes with
//!   first-byte detection, live re-binding with generations and draining;
//! - [`tls`]: certificate bundles (build step 3): polled PEM files, validation,
//!   atomic resolver swap, `sprig_gateway_cert_expiry_seconds`;
//! - [`frontend`]: the [`Frontend`](frontend::Frontend) trait the protocol
//!   front ends plug into, and the [`ClientConn`](frontend::ClientConn)
//!   they receive.
//!
//! Not yet: front ends (chapters 05, 30, 37, 40), sessions, limits,
//! registration, the `MetaStore` connection and settings watches (steps 4-10).
//! Until settings come from the control plane, [`run`] applies the built-in
//! defaults once; with no front ends compiled in, every listener is reported
//! as not applied.

pub mod frontend;
pub mod listener;
pub mod tls;

use std::sync::Arc;
use std::time::Duration;

use sprig_common::time::{Clock, SystemClock};
use sprig_common::{Result, Role, RoleContext};
use sprig_settings::{MemSettings, Registry, SettingCtx};

use crate::frontend::GatewayContext;
use crate::listener::{Frontends, ListenerManager};
use crate::tls::CertStore;

/// Runs the gateway role until `ctx.shutdown` is cancelled.
///
/// The `sprigdb` binary has already bound the telemetry port, so
/// `/healthz` answers and `/readyz` reports `starting` until this marks the
/// process ready. Startup follows chapter 04 steps 5 and 6: apply the
/// listeners, then report ready; shutdown closes the listeners and waits up
/// to `gateway.drain_timeout_seconds` for open connections.
///
/// # Errors
/// `Internal` if the setting registry is invalid (a bug).
pub async fn run(ctx: RoleContext) -> Result<()> {
    let role = Role::Gateway;
    let registry = Registry::global()?;
    // Settings rows arrive from the control plane in chapter 23 step 8; until
    // then every key resolves to its default.
    let settings = MemSettings::new(registry);
    let dc = ctx.config.node.dc.clone();
    let sctx = SettingCtx::process(&dc);

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let certs = Arc::new(CertStore::new(clock.clone()));
    let watcher = certs.spawn_watcher(tls::POLL_INTERVAL, ctx.shutdown.clone());
    let listeners = ListenerManager::new(Frontends::new(), Arc::new(GatewayContext::new(clock)));
    let report = listeners
        .apply(listener::resolve(&settings, &sctx, &certs))
        .await;
    tracing::info!(%role, applied = ?report.applied, not_applied = report.rejected.len(), "listeners applied");

    metrics::gauge!("sprig_gateway_draining").set(0.0);
    ctx.health.set_ready();
    tracing::info!(%role, "ready");

    ctx.shutdown.cancelled().await;

    ctx.health.set_draining();
    metrics::gauge!("sprig_gateway_draining").set(1.0);
    let drain =
        Duration::from_secs(settings.get(&listener::settings::DRAIN_TIMEOUT_SECONDS, &sctx));
    tracing::info!(%role, timeout_s = drain.as_secs(), "draining: closing listeners");
    listeners.shutdown(drain).await;
    let _ = watcher.await;
    Ok(())
}

#[cfg(test)]
mod testutil;
