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
//!   they receive;
//! - [`session`]: sessions (build step 5): `Session`, the `SessionTable`
//!   with connection ids, `SHOW PROCESSLIST` and `KILL`, the connection
//!   task wrapper with panic isolation, and disconnect detection;
//! - [`mysql`]: the MySQL 8.4 and MariaDB front ends (chapter 05 steps 3,
//!   4, 6, 10 and 12): handshake and TLS upgrade, the auth plugins over the
//!   interim accounts file, command dispatch, compression and live
//!   listener hooks;
//! - [`pipeline`]: the `Request`, `Executor` and `ResultSink` boundary
//!   between front ends and the execution layer (step 9's interfaces), a
//!   `NullExecutor` and a canned executor for tests.
//!
//! Listener settings come from the control plane: [`run`] mirrors the
//! `global`, `dc` and `cluster` settings scopes from etcd
//! ([`LiveSettings`]) and re-applies the listeners whenever their keys
//! change ([`listener::live`]). Without `[meta] dc_seeds` (development
//! without etcd) it runs on the built-in defaults, which then never change.
//! Listeners without a front end in this build (HTTP, MongoDB, Redis, ...)
//! are reported as not applied.
//!
//! - [`exec`]: the pass-through executor (decision D3) and the relay
//!   engine (chapter 05 steps 7-9): statements to the login's cluster,
//!   prepared statements across pooled connections, multi-statements,
//!   `LOCAL INFILE`; requests reach it through the workload hook.
//!
//! Not yet: the other front ends (chapters 30, 37, 40), limits,
//! registration and the other `MetaStore` caches (steps 4-10).

pub mod backend;
pub mod exec;
pub mod frontend;
pub mod listener;
pub mod mysql;
pub mod pipeline;
pub mod reads;
pub mod session;
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
use crate::session::SessionTable;
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
    // Backend pools and read routing (chapter 06): clusters from the DC
    // tier's registry, settings from the live mirror.
    let stores = live.stores();
    let backends = backend::Backends::new(
        dc.clone(),
        backend::RegistryClusters::new(
            sprig_cluster::registry::ClusterRegistry::new(
                Arc::clone(&stores.global),
                Arc::clone(&stores.dc),
            )?,
            Duration::from_secs(2),
        ),
        backend::LiveBackendSettings::new(live.clone(), dc.clone()),
    );
    let _reaper = backends.start();
    // The MySQL and MariaDB front ends (chapter 05): requests pass the
    // workload hook (chapter 12, a no-op until M0.4) into the pass-through
    // executor (decision D3).
    let mysql = mysql::MySqlStack::start(
        mysql::StackConfig {
            accounts_file: ctx.config.gateway.accounts_file.clone(),
            live: live.clone(),
            dc: dc.clone(),
            hostname: ctx.config.node.id(),
            clock: clock.clone(),
            executor: pipeline::with_workload(
                Arc::new(exec::PassThrough::new(
                    Arc::clone(&backends),
                    exec::ExecSettingsSource::Live {
                        live: live.clone(),
                        dc: dc.clone(),
                    },
                )),
                Arc::new(pipeline::NoWorkload),
            ),
        },
        ctx.shutdown.clone(),
    )?;
    let listeners = Arc::new(ListenerManager::new(
        mysql.register(Frontends::new()),
        Arc::new(
            GatewayContext::with_sessions(clock.clone(), Arc::new(SessionTable::new(clock)))
                .with_backends(Arc::clone(&backends)),
        ),
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
    backends.shutdown();
    let _ = watcher.await;
    mysql.join().await;
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
