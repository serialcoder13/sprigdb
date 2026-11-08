//! `sprig-controller`: the controller role, the reconciler runtime, leader
//! election and the job scheduler.
//!
//! Layer 3 (roles). Design: [chapter 23](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/23-control-plane.md)
//! (Control plane) of the SprigDB low-level design.
//!
//! What exists:
//! - [`runtime`]: the [`Reconciler`](runtime::Reconciler) trait and the
//!   runtime that hosts loops by tier, with DC (and optionally global)
//!   leader election, fencing tokens and step-down (step 5);
//! - [`dispatch`]: registrations, jobs, the dispatch and `gc` loops, and
//!   the worker side's claims and progress writes (step 6);
//! - [`dc`]: the DC records behind `sprigctl dc register|list|show`
//!   (step 7);
//! - [`run`]: the role: connect (single-etcd mode when the DC and global
//!   seeds are the same), bootstrap or verify the deployment, mirror the
//!   settings, then run the loops and the Admin API until shutdown (step 7);
//! - [`settings`]: the `control.*` keys and the slot limits; [`live`]: the
//!   etcd mirrors they are resolved from (chapter 23 step 8);
//! - [`monitor`]: the `registry` loop, which probes every cluster and keeps
//!   its capability profile and health current (chapter 09);
//! - [`admin`]: the Admin API's `Clusters`, `DataCenters` and `Tenants`
//!   services (chapter 09 step 9, chapter 23 step 7, chapter 08 step 9,
//!   chapter 37), served as REST on `[controller] admin_addr`;
//! - [`tree`]: the tenant tree in the directory: entries, names, ids,
//!   create under a parent and the delete guard (chapter 23 step 16, the
//!   parts bootstrap needs);
//! - [`tree_settings`]: the `tree_settings` loop that copies ancestors'
//!   settings into descendants' catalogs (chapter 08 step 6);
//! - [`tenants`]: tenant bootstrap and deletion, and the
//!   `tenant_bootstrap` loop that finishes or fails them (chapter 08 step
//!   9); [`catalogs`]: how the controller reaches tenant catalogs.
//!
//! Not yet: the relay and peer services (R2), the latency prober, the
//! global loops (`directory`, `tree_moves`, `dc_registry`), subtree moves,
//! the decision log (chapter 11), and the Admin API's other services.

pub mod admin;
pub mod catalogs;
pub mod dc;
pub mod dispatch;
pub mod live;
pub mod monitor;
pub mod runtime;
pub mod settings;
pub mod tenants;
pub mod tree;
pub mod tree_settings;

use std::sync::Arc;
use std::time::Duration;

use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::DcId;
use sprig_common::time::{Clock, SystemClock};
use sprig_common::{CancellationToken, Result, Role, RoleContext, SprigCode, SprigError};
use sprig_meta::MetaStore;
use sprig_meta::bootstrap::{self, Bootstrap};
use sprig_meta::elect::LeaderRecord;
use sprig_meta::etcd::{EtcdTls, connect_tiers};

use sprig_catalog::{DirectoryAllocator, MariaBackend, Versioner};

use crate::admin::{AdminService, http};
use crate::catalogs::{ClusterPools, RegistryPools, ShardMapLocator, default_naming};
use crate::monitor::{ClusterMonitor, RegistryLoop};
use crate::runtime::{Loop, Runtime, Stores};
use crate::settings::ControlSettings;
use crate::tenants::{NoHooks, TenantLifecycle, TenantService};
use crate::tree::Directory;
use crate::tree_settings::{TreeSettings, TreeSync};

/// Wait between checks while another controller bootstraps the deployment.
const BOOTSTRAP_WAIT: Duration = Duration::from_secs(1);

/// Runs the controller role until `ctx.shutdown` is cancelled.
///
/// The `sprigdb` binary has already bound the telemetry port, so
/// `/healthz` answers and `/readyz` reports `starting` until the stores
/// are connected and the deployment is verified.
///
/// # Errors
/// 39998 `ER_SPRIG_CONFIG_INVALID` for a missing `[meta] deployment_id` or
/// seeds, unreadable TLS files, or a deployment id that doesn't match;
/// 39500 `ER_SPRIG_META_UNAVAILABLE` if no etcd seed answers at start-up.
pub async fn run(ctx: RoleContext) -> Result<()> {
    let cfg = &ctx.config;
    let settings = ControlSettings::default();
    let tls = cfg.tls.as_ref().map(EtcdTls::from_files).transpose()?;
    let tiers = connect_tiers(
        cfg.node.dc.clone(),
        &cfg.meta,
        tls,
        settings.request_timeout,
        true,
    )
    .await?;
    let global = tiers
        .global
        .ok_or_else(|| SprigError::internal("controllers always connect the global tier"))?;
    let stores = Stores {
        dc: Arc::new(tiers.dc),
        global: Arc::new(global),
    };
    serve(ctx, stores, Arc::new(SystemClock)).await
}

/// The role over given stores: what [`run`] does after connecting. Tests
/// call it with `MemStore`s.
///
/// # Errors
/// As [`run`], after connecting.
pub async fn serve(ctx: RoleContext, stores: Stores, clock: Arc<dyn Clock>) -> Result<()> {
    let role = Role::Controller;
    let cfg = &ctx.config;
    let dc = cfg.node.dc.clone();
    let deployment = cfg.meta.deployment_id.as_str();
    if deployment.is_empty() {
        return Err(SprigError::new(
            SprigCode::ConfigInvalid,
            "[meta] deployment_id is required for the controller",
        ));
    }
    let ready = prepare_deployment(
        &*stores.global,
        &*clock,
        deployment,
        &dc,
        cfg.controller.bootstrap,
        &ctx.shutdown,
    )
    .await?;
    if !ready {
        return Ok(());
    }
    let Some(settings) = live::start(&stores, &dc, &ctx.shutdown).await? else {
        return Ok(());
    };
    let me = LeaderRecord {
        holder: cfg.node.id(),
        dc: Some(dc.to_string()),
        build: env!("CARGO_PKG_VERSION").to_owned(),
    };
    let registry = ClusterRegistry::new(Arc::clone(&stores.global), Arc::clone(&stores.dc))?;
    let monitor = Arc::new(ClusterMonitor::new(
        registry.clone(),
        settings.live.clone(),
        Arc::clone(&clock),
    ));
    // Tenant catalogs: found through their shard maps, changed with
    // versions allocated in the directory (chapter 08).
    let pools: Arc<dyn ClusterPools> =
        Arc::new(RegistryPools::new(registry, Some(Arc::clone(&monitor))));
    let locator = ShardMapLocator::new(Arc::clone(&stores.dc), pools, default_naming());
    let versioner = Versioner::new(
        MariaBackend::new(Arc::new(locator.clone())).with_clock(Arc::clone(&clock)),
        Arc::new(DirectoryAllocator::new(Arc::clone(&stores.global))),
    );
    let tree = Arc::new(TreeSync::new(
        versioner,
        Directory::new(Arc::clone(&stores.global)),
        Arc::clone(&clock),
    ));
    let tenants = Arc::new(TenantService::new(
        Arc::clone(&stores.dc),
        locator,
        Arc::clone(&tree),
        Arc::new(NoHooks),
        Arc::clone(&clock),
        Some(settings.live.clone()),
    )?);
    let tree_loop = TreeSettings::new(tree, dc.clone(), Some(settings.live.clone()))?;
    let rt = Runtime::new(stores.clone(), Arc::clone(&clock), me)
        .with_settings(settings.control.clone())
        .with_loop(Loop::new(dispatch::Dispatch))
        .with_loop(Loop::new(dispatch::JobGc))
        .with_loop(Loop::new(RegistryLoop::new(Arc::clone(&monitor))))
        .with_loop(Loop::new(tree_loop))
        .with_loop(Loop::new(TenantLifecycle::new(Arc::clone(&tenants))))
        .global_candidate(cfg.controller.global_candidate);

    let token = cfg
        .controller
        .admin_token_file
        .as_deref()
        .map(http::load_token)
        .transpose()?;
    let listener = http::bind(cfg.controller.admin_addr, token.as_deref()).await?;
    let admin_addr = listener
        .local_addr()
        .map_err(|e| SprigError::internal("admin listener address").with_source(e))?;
    let svc = Arc::new(
        AdminService::new(stores, monitor, rt.status(), Arc::clone(&clock)).with_tenants(tenants),
    );
    let admin = tokio::spawn(http::serve(
        listener,
        http::router(svc, token),
        ctx.shutdown.clone(),
    ));

    ctx.health.set_ready();
    tracing::info!(%role, %dc, admin = %admin_addr, "ready");
    rt.run(ctx.shutdown.clone()).await;
    ctx.health.set_draining();
    match admin.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(error = %e, "the Admin API stopped with an error"),
        Err(e) => tracing::warn!(error = %e, "the Admin API task failed"),
    }
    tracing::info!(%role, "stopped every loop");
    Ok(())
}

/// Bootstraps the first DC (`[controller] bootstrap = true`) or waits for
/// the deployment to exist, then checks its id. `false` if shutdown came
/// first.
async fn prepare_deployment(
    global: &dyn MetaStore,
    clock: &dyn Clock,
    deployment: &str,
    dc: &DcId,
    bootstrap: bool,
    shutdown: &CancellationToken,
) -> Result<bool> {
    let key =
        sprig_meta::Tier::Global.key(&format!("settings/{}", bootstrap::DEPLOYMENT_ID_SETTING))?;
    let mut logged = false;
    loop {
        let r = if bootstrap {
            bootstrap::bootstrap_first_dc(global, clock, deployment, dc)
                .await
                .map(|b| {
                    if b == Bootstrap::Created {
                        tracing::info!(%dc, deployment, "bootstrapped the deployment");
                    }
                    true
                })
        } else {
            match global
                .get(&key, sprig_meta::Consistency::Linearizable)
                .await
            {
                // Not bootstrapped yet: another controller will do it.
                Ok(None) => Ok(false),
                Ok(Some(_)) => bootstrap::verify_deployment_id(global, deployment)
                    .await
                    .map(|()| true),
                Err(e) => Err(e),
            }
        };
        match r {
            Ok(true) => return Ok(true),
            Ok(false) => {
                if !logged {
                    tracing::info!(deployment, "waiting for the deployment to be bootstrapped");
                    logged = true;
                }
            }
            Err(e) if e.retryable => {
                tracing::warn!(error = %e, "deployment check failed; retrying");
            }
            Err(e) => return Err(e),
        }
        tokio::select! {
            () = shutdown.cancelled() => return Ok(false),
            () = tokio::time::sleep(BOOTSTRAP_WAIT) => {}
        }
    }
}

#[cfg(test)]
mod tests;
