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
//! Listener settings come from the control plane: [`run`] mirrors the
//! `global`, `dc` and `cluster` settings scopes from etcd
//! ([`LiveSettings`]) and re-applies the listeners whenever their keys
//! change ([`listener::live`]). Without `[meta] dc_seeds` (development
//! without etcd) it runs on the built-in defaults, which then never change.
//! With no front ends compiled in, every listener is reported as not
//! applied.
//!
//! Not yet: front ends (chapters 05, 30, 37, 40), sessions, limits,
//! registration and the other `MetaStore` caches (steps 4-10).

pub mod frontend;
pub mod listener;
pub mod tls;

use std::sync::Arc;
use std::time::Duration;

use sprig_common::time::{Clock, SystemClock};
use sprig_common::{Result, Role, RoleContext, SprigError};
use sprig_meta::etcd::{DEFAULT_REQUEST_TIMEOUT, EtcdTls, connect_tiers};
use sprig_meta::{MemStore, Tier};
use sprig_settings::{LiveSettings, Registry, SettingCtx, SettingsStores};

use crate::frontend::GatewayContext;
use crate::listener::{Frontends, ListenerManager};
use crate::tls::CertStore;

/// Runs the gateway role until `ctx.shutdown` is cancelled.
///
/// The `sprigdb` binary has already bound the telemetry port, so
/// `/healthz` answers and `/readyz` reports `starting` until this marks the
/// process ready. Startup follows chapter 04 steps 5 and 6: load the
/// settings mirrors, apply the listeners, then report ready; listener
/// settings then apply live. Shutdown closes the listeners and waits up to
/// `gateway.drain_timeout_seconds` for open connections.
///
/// # Errors
/// `Internal` if the setting registry is invalid (a bug); 39998 for
/// unreadable `[tls]` files; 39500 if no etcd seed answers at start-up
/// (chapter 23: refuse to start).
pub async fn run(ctx: RoleContext) -> Result<()> {
    let role = Role::Gateway;
    let registry = Registry::global()?;
    let dc = ctx.config.node.dc.clone();
    let sctx = SettingCtx::process(&dc);

    let live = LiveSettings::start(registry, settings_stores(&ctx).await?)?;
    tokio::select! {
        () = ctx.shutdown.cancelled() => return Ok(()),
        () = live.ready() => {}
    }

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let certs = Arc::new(CertStore::new(clock.clone()));
    let watcher = certs.spawn_watcher(tls::POLL_INTERVAL, ctx.shutdown.clone());
    let listeners = Arc::new(ListenerManager::new(
        Frontends::new(),
        Arc::new(GatewayContext::new(clock)),
    ));
    // Subscribe before the first apply, so no change falls in between.
    let changes = live.subscribe(listener::live::LISTENER_KEYS);
    let report = listener::live::apply_current(&live, &dc, &certs, &listeners).await;
    tracing::info!(%role, applied = ?report.applied, not_applied = report.rejected.len(), "listeners applied");
    let follower = tokio::spawn(listener::live::follow(
        live.clone(),
        dc.clone(),
        Arc::clone(&certs),
        Arc::clone(&listeners),
        changes,
        listener::live::rejections(&report),
        ctx.shutdown.clone(),
    ));

    metrics::gauge!("sprig_gateway_draining").set(0.0);
    ctx.health.set_ready();
    tracing::info!(%role, "ready");

    ctx.shutdown.cancelled().await;

    ctx.health.set_draining();
    metrics::gauge!("sprig_gateway_draining").set(1.0);
    let drain = Duration::from_secs(live.get(&listener::settings::DRAIN_TIMEOUT_SECONDS, &sctx));
    tracing::info!(%role, timeout_s = drain.as_secs(), "draining: closing listeners");
    let _ = follower.await;
    listeners.shutdown(drain).await;
    let _ = watcher.await;
    Ok(())
}

/// The stores the settings mirrors read: the etcd tiers named by `[meta]`
/// (single-etcd mode when the global seeds are empty or the same), or, with
/// no seeds, an empty in-memory store, so every key keeps its default.
async fn settings_stores(ctx: &RoleContext) -> Result<SettingsStores> {
    let dc = ctx.config.node.dc.clone();
    let meta = &ctx.config.meta;
    if meta.dc_seeds.is_empty() {
        tracing::warn!(
            "no [meta] dc_seeds: settings are the built-in defaults and won't change live"
        );
        let mem = MemStore::new(Tier::Dc(dc));
        return SettingsStores::new(Arc::new(mem.with_tier(Tier::Global)), Arc::new(mem));
    }
    let tls = ctx
        .config
        .tls
        .as_ref()
        .map(EtcdTls::from_files)
        .transpose()?;
    // Until the relay (chapter 23 step 11), the gateway reads the global
    // settings straight from the global tier.
    let tiers = connect_tiers(dc, meta, tls, DEFAULT_REQUEST_TIMEOUT, true).await?;
    let global = tiers
        .global
        .ok_or_else(|| SprigError::internal("connect_tiers returned no global store"))?;
    SettingsStores::new(Arc::new(global), Arc::new(tiers.dc))
}

#[cfg(test)]
mod testutil;
