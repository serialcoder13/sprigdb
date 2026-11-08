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
//!   4, 6, 10 and 12): handshake and TLS upgrade, the auth plugins over
//!   chapter 14's `LoginService` (tenant catalogs, or the interim accounts
//!   file with `[gateway] login_mode = "accounts_file"`), command dispatch,
//!   compression and live listener hooks;
//! - [`tenant`]: tenants (M0.3): the catalog cache with the auth module,
//!   name virtualization (chapter 07) per statement, DDL through the
//!   catalog (chapter 08 step 10), settings in SQL (step 8) and
//!   `information_schema.SPRIG_SETTINGS`, the interim backend accounts;
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
//!   `LOCAL INFILE`; requests reach it through the workload hook. For a
//!   catalog login it plans every statement with chapter 07's rewrite and
//!   maps results, errors and session tracking back to logical names.
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
pub mod tenant;
pub mod tls;
pub mod workload;

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
#[allow(clippy::too_many_lines)] // start-up and drain, step by step
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
    let sessions = Arc::new(SessionTable::new(clock.clone()));
    // Logins (chapter 14) and, for catalog logins, the tenant services
    // (chapters 07 and 08).
    let (login, tenants, login_tasks) = logins(
        &ctx,
        &live,
        Arc::clone(&backends),
        Arc::clone(&sessions),
        clock.clone(),
    )?;
    let mut executor = exec::PassThrough::new(
        Arc::clone(&backends),
        exec::ExecSettingsSource::Live {
            live: live.clone(),
            dc: dc.clone(),
        },
    );
    if let Some(t) = &tenants {
        executor = executor.with_tenants(Arc::clone(t));
    }
    // Workload capture (chapter 12, M0.4): the hook and its flusher.
    let (workload, flusher) = workload::start(
        &ctx.config,
        &live,
        tenants.as_ref(),
        clock.clone(),
        ctx.shutdown.clone(),
    )?;
    // The MySQL and MariaDB front ends (chapter 05): requests pass the
    // workload hook (chapter 12) into the executor.
    let mysql = mysql::MySqlStack::start(
        mysql::StackConfig {
            login,
            live: live.clone(),
            dc: dc.clone(),
            hostname: ctx.config.node.id(),
            clock: clock.clone(),
            executor: pipeline::with_workload(Arc::new(executor), workload),
        },
        ctx.shutdown.clone(),
    )?;
    let binds = bind_overrides(&ctx)?;
    let listeners = Arc::new(
        ListenerManager::new(
            mysql.register(Frontends::new()),
            Arc::new(
                GatewayContext::with_sessions(clock.clone(), sessions)
                    .with_backends(Arc::clone(&backends)),
            ),
        )
        .with_bind_overrides(binds),
    );
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
    for t in login_tasks {
        let _ = t.await;
    }
    // Drain step 5: the flusher's last flush, before the catalog pools close.
    if let Some(f) = flusher {
        let _ = f.await;
    }
    if let Some(t) = tenants {
        t.locator.close().await;
    }
    Ok(())
}

/// `[gateway] listen_override`: this process's own listener addresses.
fn bind_overrides(
    ctx: &RoleContext,
) -> Result<std::collections::HashMap<String, std::net::SocketAddr>> {
    let mut binds = std::collections::HashMap::new();
    for (name, addr) in &ctx.config.gateway.listen_override {
        let a = addr.parse().map_err(|e| {
            SprigError::new(
                sprig_common::SprigCode::ConfigInvalid,
                format!("[gateway] listen_override.{name} = {addr:?}: {e}"),
            )
        })?;
        tracing::info!(listener = %name, bind = %a, "listener address overridden for this process");
        binds.insert(name.clone(), a);
    }
    Ok(binds)
}

/// The process's logins by `[gateway] login_mode`: tenant catalogs (the
/// default, chapter 14) with the tenant services, or the interim accounts
/// file (decision D1) with its watcher.
type Logins = (
    Arc<mysql::login::LoginService>,
    Option<Arc<tenant::TenantServices>>,
    Vec<tokio::task::JoinHandle<()>>,
);

fn logins(
    ctx: &RoleContext,
    live: &LiveSettings,
    backends: Arc<backend::Backends>,
    sessions: Arc<SessionTable>,
    clock: Arc<dyn Clock>,
) -> Result<Logins> {
    use sprig_auth::{CatalogLogins, FastCache, LoginService, LoginSource, MetaTenants};
    use sprig_common::config::LoginModeConfig;
    let fast_cache = Arc::new(FastCache::new(
        clock.clone(),
        sprig_auth::fast_cache::DEFAULT_TTL,
    ));
    let cfg = &ctx.config.gateway;
    let audit: Arc<dyn sprig_auth::audit::AuditSink> =
        Arc::new(sprig_auth::audit::LogAudit::new(ctx.config.node.id()));
    match cfg.effective_login_mode() {
        LoginModeConfig::AccountsFile => {
            let mut tasks = Vec::new();
            let accounts: Arc<dyn sprig_auth::AccountStore> = if let Some(path) = &cfg.accounts_file
            {
                let file = sprig_auth::FileAccounts::open(path)?;
                tasks.push(
                    file.spawn_watcher(sprig_auth::accounts::POLL_INTERVAL, ctx.shutdown.clone()),
                );
                file
            } else {
                tracing::warn!("no [gateway] accounts_file: every MySQL login is refused (1045)");
                Arc::new(sprig_auth::StaticAccounts::default())
            };
            tracing::info!(
                "logins from the accounts file (login_mode = accounts_file): names pass through"
            );
            let svc = LoginService::new(LoginSource::File(accounts), fast_cache)
                .with_clock(clock)
                .with_audit(audit);
            Ok((Arc::new(svc), None, tasks))
        }
        LoginModeConfig::Catalog => {
            let tenants = tenant::TenantServices::start(live, backends, sessions, clock.clone())?;
            let resolver = if let Some(p) = &cfg.backends_file {
                tenant::backends::TenantBackends::open(p, Arc::clone(&tenants.locator))?
            } else {
                tracing::warn!(
                    "no [gateway] backends_file: catalog logins fail with 39998 until one names a backend account"
                );
                tenant::backends::TenantBackends::none(Arc::clone(&tenants.locator))
            };
            let catalogs = CatalogLogins::new(
                Arc::new(MetaTenants::new(
                    Arc::clone(&tenants.global),
                    sprig_auth::account::META_TENANTS_TTL,
                )),
                Arc::new(tenants.cache.clone()),
                Arc::new(resolver),
                Arc::new(sprig_auth::SettingsPolicy::new(
                    tenants.settings.clone(),
                    tenants.dc.clone(),
                )),
            );
            tracing::info!("logins from tenant catalogs (login_mode = catalog)");
            // Chapter 14: failure counters and temporary locks in the
            // tenants' catalogs, audit events in the log (target
            // sprig_audit) until chapter 47's pipeline.
            let state = sprig_auth::policy::CatalogLoginState::new(tenants.catalog.clone());
            let svc = LoginService::new(LoginSource::Catalog(Arc::new(catalogs)), fast_cache)
                .with_clock(clock)
                .with_login_state(Arc::new(state))
                .with_audit(audit);
            Ok((Arc::new(svc), Some(tenants), Vec::new()))
        }
    }
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
