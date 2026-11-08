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
//! - [`settings_admin`]: the Admin API's `Settings` service: list and
//!   describe keys, resolve one with its chain and provenance, and set,
//!   reset, bound, lock and unlock it at every scope (chapter 08 step 7,
//!   chapter 23 step 8, chapter 37, chapter 47);
//! - [`tenants`]: tenant bootstrap and deletion, and the
//!   `tenant_bootstrap` loop that finishes or fails them (chapter 08 step
//!   9); [`catalogs`]: how the controller reaches tenant catalogs;
//! - [`autoindex_admin`]: the Admin API's automatic-indexing calls behind
//!   `sprigctl index` and `sprigctl explain query` (chapter 13 step 13);
//! - [`autoindex_loop`]: the `auto_index` loop, evaluate → validate → roll
//!   out → observe per tenant (chapter 13 steps 10 and 11), with
//!   [`autoindex_env`], its view of catalogs, objects, settings and
//!   clusters;
//! - [`catalog_reconciler`]: the `catalog_reconciler` loop (chapter 08 step
//!   11): stuck DDL jobs finished or rolled back, missing and orphan
//!   schemas, outside objects adopted, tombstones purged by age, clamped
//!   setting values counted.
//!
//! Not yet: the relay and peer services (R2), the latency prober, the
//! global loops (`directory`, `tree_moves`, `dc_registry`), subtree moves,
//! the decision log (chapter 11), and the Admin API's other services.

pub mod admin;
pub mod autoindex_admin;
pub mod autoindex_env;
pub mod autoindex_loop;
pub mod autoindex_validate;
pub mod autoindex_workload;
pub mod balancer_loop;
pub mod catalog_reconciler;
pub mod catalogs;
pub mod dc;
pub mod dispatch;
pub mod live;
pub mod migrate;
pub mod monitor;
pub mod registration;
pub mod runtime;
pub mod settings;
pub mod settings_admin;
pub mod tenants;
pub mod traffic;
pub mod tree;
pub mod tree_settings;
pub mod users;
pub mod workload_rollup;

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

use sprig_catalog::ddl::DdlEngine;
use sprig_catalog::{DirectoryAllocator, MariaBackend, Versioner};

use crate::admin::{AdminService, http};
use crate::catalog_reconciler::{CatalogReconciler, LogDecisions, PoolExecutors};
use crate::catalogs::{ClusterPools, RegistryPools, ShardMapLocator, default_naming};
use crate::monitor::{ClusterMonitor, RegistryLoop};
use crate::runtime::{Loop, Runtime, Stores};
use crate::settings::ControlSettings;
use crate::settings_admin::SettingsService;
use crate::tenants::{TenantLifecycle, TenantService};
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
    // Endpoint discovery (chapter 23 step 9): new etcd members join the
    // client without a restart. One etcd in single-etcd mode: one follower.
    let discovery = Arc::new(sprig_meta::discovery::Discovery::new(&tiers.dc));
    let period = Duration::from_secs(settings::ETCD_MEMBER_REFRESH_SECONDS.default);
    let _discovery = discovery.spawn(move || period, ctx.shutdown.clone());
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
#[allow(clippy::too_many_lines)] // the role's wiring, one loop after another
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
    // Chapter 47: trace sampling, the OTLP endpoint and the audit spool
    // limit follow their settings.
    let _telemetry_settings = sprig_settings::ops::apply_live(&settings.live, &dc);
    let me = LeaderRecord {
        holder: cfg.node.id(),
        dc: Some(dc.to_string()),
        build: env!("CARGO_PKG_VERSION").to_owned(),
    };
    let registry = ClusterRegistry::new(Arc::clone(&stores.global), Arc::clone(&stores.dc))?;
    let registry_for_balancer = registry.clone();
    let monitor = Arc::new(ClusterMonitor::new(
        registry.clone(),
        settings.live.clone(),
        Arc::clone(&clock),
    ));
    // Tenant catalogs: found through their shard maps, changed with
    // versions allocated in the directory (chapter 08).
    let pools: Arc<dyn ClusterPools> = Arc::new(RegistryPools::new(
        registry.clone(),
        Some(Arc::clone(&monitor)),
    ));
    let locator = ShardMapLocator::new(Arc::clone(&stores.dc), pools, default_naming());
    let versioner = Versioner::new(
        MariaBackend::new(Arc::new(locator.clone())).with_clock(Arc::clone(&clock)),
        Arc::new(DirectoryAllocator::new(Arc::clone(&stores.global))),
    );
    let reconciler = CatalogReconciler::new(
        DdlEngine::new(versioner.clone(), Arc::clone(&clock)),
        Directory::new(Arc::clone(&stores.global)),
        Arc::new(PoolExecutors::new(locator.clone())),
        Arc::new(LogDecisions),
        dc.clone(),
        Some(settings.live.clone()),
        Arc::clone(&clock),
    );
    let settings_admin = Arc::new(SettingsService::new(
        settings.live.clone(),
        versioner.clone(),
        Directory::new(Arc::clone(&stores.global)),
        Arc::clone(&clock),
    ));
    let tree = Arc::new(TreeSync::new(
        versioner.clone(),
        Directory::new(Arc::clone(&stores.global)),
        Arc::clone(&clock),
    ));
    let indexes = Arc::new(
        autoindex_admin::AutoIndexService::new(
            Directory::new(Arc::clone(&stores.global)),
            Arc::new(locator.clone()),
            versioner.clone(),
            settings.live.clone(),
            Arc::clone(&clock),
        )
        // `sprigctl index restore` runs its statements on the shards
        // (decision O1).
        .with_env(Arc::new(autoindex_env::ControllerEnv::new(
            dc.clone(),
            locator.clone(),
            registry.clone(),
            Some(Arc::clone(&monitor)),
            Some(settings.live.clone()),
            autoindex_loop::JobShardLocks::new(Arc::clone(&stores.dc)),
        ))),
    );
    let users = Arc::new(users::TenantUsers::new(
        Directory::new(Arc::clone(&stores.global)),
        Arc::new(locator.clone()),
        versioner.clone(),
        Arc::clone(&clock),
    ));
    // Chapter 13's automatic indexing: evaluation, validation, builds and
    // observation on the DC leader (no worker job runtime yet), reading
    // chapter 12's workload store in the tenants' catalogs.
    let jobs = autoindex_loop::JobShardLocks::new(Arc::clone(&stores.dc));
    let catalog_locator: Arc<dyn sprig_catalog::CatalogLocator> = Arc::new(locator.clone());
    let workload_store: Arc<dyn sprig_workload::store::WorkloadStore> = Arc::new(
        sprig_workload::store::CatalogWorkloadStore::new(Arc::clone(&catalog_locator)),
    );
    let keyring: Option<Arc<dyn sprig_workload::keyring::TenantKeyring>> =
        if let Some(p) = &cfg.controller.workload_key_file {
            let k = sprig_workload::keyring::MasterKeyring::from_file(p)?;
            tracing::info!(versions = ?k.versions(), "workload sample keys loaded");
            Some(Arc::new(k))
        } else {
            tracing::info!(
                "no [controller] workload_key_file: automatic-index validation uses \
                 estimates and a canary only"
            );
            None
        };
    let ai_clock: Arc<dyn Clock> = match &cfg.controller.autoindex_test_clock_file {
        Some(f) => {
            tracing::warn!(file = %f.display(), "auto_index runs on a test clock");
            Arc::new(autoindex_env::SkewClock::new(Arc::clone(&clock), f.clone()))
        }
        None => Arc::clone(&clock),
    };
    let ai_env = Arc::new(autoindex_env::ControllerEnv::new(
        dc.clone(),
        locator.clone(),
        registry.clone(),
        Some(Arc::clone(&monitor)),
        Some(settings.live.clone()),
        jobs.clone(),
    ));
    let ai_eval = autoindex_env::EvalShim::new(
        Arc::clone(&ai_env),
        Arc::clone(&ai_clock),
        Arc::new(autoindex_workload::StoreSource::new(Arc::clone(
            &workload_store,
        ))),
        Arc::new(sprig_advisor::autoindex::store::LogDecisions),
    );
    let marker: Arc<dyn sprig_advisor::autoindex::validate::NodeMarker> =
        Arc::new(autoindex_validate::LogMarker);
    let auto_index = autoindex_loop::AutoIndexLoop::new(
        ai_env,
        autoindex_loop::AutoIndexHooks {
            candidates: Arc::new(ai_eval),
            validation: Arc::new(autoindex_validate::SampleValidation::new(
                Arc::clone(&catalog_locator),
                registry,
                keyring.clone(),
                Arc::clone(&marker),
            )),
            workload: Arc::new(autoindex_workload::ObserveSource::new(
                workload_store,
                catalog_locator,
                keyring,
                Arc::clone(&ai_clock),
            )),
            locks: Arc::new(jobs),
            mover: None,
            marker,
            decisions: Arc::new(sprig_advisor::autoindex::store::LogDecisions),
        },
        Directory::new(Arc::clone(&stores.global)),
        dc.clone(),
        Arc::clone(&ai_clock),
        // Passes every minute at most; evaluation is paced by
        // auto_index.eval_interval_minutes (resolved per tenant).
        autoindex_loop::Cadence {
            eval_every: Duration::from_secs(60),
            max_requeue: Duration::from_secs(60),
            ..autoindex_loop::Cadence::default()
        },
    );
    // Chapter 12's rollup and retention of the workload tables.
    let workload_rollup = workload_rollup::WorkloadRollup::new(
        Directory::new(Arc::clone(&stores.global)),
        Arc::new(locator.clone()),
        dc.clone(),
        Some(settings.live.clone()),
        Arc::clone(&clock),
    );
    // Chapter 11's balancer: plans and decisions; in `auto` mode its moves
    // become chapter 10 jobs in the DC tier, which workers (`sprigdb
    // worker`) claim and run. `recommend` and `dry_run` only record.
    let move_exec: Arc<dyn sprig_balancer::MoveExecutor> = Arc::new(
        sprig_balancer::mover::MoverExecutor::new(Arc::new(sprig_mover::MetaMover::new(
            sprig_mover::JobStore::new(Arc::clone(&stores.dc), Arc::clone(&clock)),
        ))),
    );
    let balancer = Arc::new(balancer_loop::BalancerLoop::new(
        dc.clone(),
        ClusterRegistry::new(Arc::clone(&stores.global), Arc::clone(&stores.dc))?,
        Arc::clone(&stores.dc),
        Arc::new(RegistryPools::new(
            registry_for_balancer,
            Some(Arc::clone(&monitor)),
        )),
        Arc::new(locator.clone()),
        Directory::new(Arc::clone(&stores.global)),
        Some(settings.live.clone()),
        Arc::clone(&clock),
        Some(move_exec),
        cfg.node.id().clone(),
    ));
    let tenants = Arc::new(TenantService::new(
        Arc::clone(&stores.dc),
        locator,
        Arc::clone(&tree),
        // Chapter 14's verifier tables and chapter 13's automatic-indexing
        // tables with the core catalog.
        // Chapter 11's decision log too.
        Arc::new(balancer_loop::WithDecisions(autoindex_loop::WithAutoIndex(
            users::AuthHooks,
        ))),
        Arc::clone(&clock),
        Some(settings.live.clone()),
    )?);
    let tree_loop = TreeSettings::new(tree, dc.clone(), Some(settings.live.clone()))?;
    // Chapter 24: moves between DCs and traffic windows.
    let moves = Arc::new(migrate::DcMoves::new(
        dc.clone(),
        Arc::clone(&stores.dc),
        Arc::clone(&stores.global),
        Arc::clone(&clock),
        Some(settings.live.clone()),
    ));
    let rt = Runtime::new(stores.clone(), Arc::clone(&clock), me)
        .with_settings(settings.control.clone())
        .with_loop(Loop::new(dispatch::Dispatch))
        .with_loop(Loop::new(dispatch::JobGc))
        .with_loop(Loop::new(RegistryLoop::new(Arc::clone(&monitor))))
        .with_loop(Loop::new(tree_loop))
        .with_loop(Loop::new(TenantLifecycle::new(Arc::clone(&tenants))))
        .with_loop(Loop::new(reconciler))
        .with_loop(Loop::new(auto_index))
        .with_loop(Loop::new(workload_rollup))
        .with_loop(Loop::new(Arc::clone(&balancer)))
        .with_loop(Loop::new(migrate::MigrationLoop::new(Arc::clone(&moves))))
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
    let reg_store = Arc::clone(&stores.dc);
    let svc = Arc::new(
        AdminService::new(stores, monitor, rt.status(), Arc::clone(&clock))
            .with_tenants(tenants)
            .with_users(users)
            .with_settings(settings_admin)
            .with_indexes(indexes)
            .with_placement(balancer)
            .with_moves(moves),
    );
    let admin = tokio::spawn(http::serve(
        listener,
        http::router(svc, token),
        ctx.shutdown.clone(),
    ));
    // Gateways find the leader's Admin API here (chapter 37 step 4).
    let registered = tokio::spawn(registration::keep(
        Arc::clone(&reg_store),
        Arc::clone(&clock),
        cfg.node.id(),
        registration::ControllerRecord {
            admin_addr: registration::advertised(admin_addr),
            version: env!("CARGO_PKG_VERSION").to_owned(),
        },
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
    let _ = registered.await;
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
