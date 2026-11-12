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
//! - [`failover`]: chapter 25's DC failover: permits and observers
//!   served from the DC lease, the global leader's decision, promotion
//!   with its commit point, and recovery of a returning DC (R2).
//! - [`residency`]: chapter 26's policies, their resolution through the
//!   tenant tree, compliance plans and the `residency` loop (R2).
//!
//! Not yet: the relay and peer services (R2), the latency prober, the
//! global loops (`directory`, `tree_moves`, `dc_registry`), subtree moves,
//! the decision log (chapter 11), and the Admin API's other services.

pub mod admin;
// Chapter 52: approvals and recommendations for every automatic decision.
pub mod approvals;
pub mod autoindex_admin;
pub mod autoindex_doc;
pub mod autoindex_env;
pub mod autoindex_loop;
pub mod autoindex_validate;
pub mod autoindex_workload;
pub mod balancer_loop;
pub mod catalog_reconciler;
pub mod catalogs;
pub mod dc;
// Chapter 24 steps 7 and 9 (r2last): automatic moves between DCs.
pub mod dc_migrate;
pub mod dispatch;
pub mod drain;
pub mod failover;
pub mod inherit_loop;
pub mod latency;
// Chapter 24 step 8 (r2last): least-busy DC placement.
pub mod least_busy;
pub mod live;
pub mod logic_loop;
// Chapter 45 (R9): one sequencer job per stream.
pub mod migrate;
pub mod monitor;
// Chapter 14 (operators): named platform and DC operators.
pub mod operators;
pub mod registration;
pub mod residency;
pub mod runtime;
pub mod settings;
pub mod settings_admin;
pub mod sharding;
pub mod sharding_runtime;
// Chapter 25 (R2, r2final): job standbys kept current by the mover.
pub mod standby;
pub mod streams_loop;
// Chapter 43 (R8 round 5): the graph mirror applier jobs.
pub mod graph_mirror_loop;
// RLS round 5 (RL52): `queue.change_exchange` forwarders.
pub mod change_exchange_loop;
// Chapter 39 (R5 round 6): search copies on workers.
pub mod search_loop;
// Chapter 50 (R9, r9fwd): the feed-driven queue outbox forwarder jobs.
pub mod queue_forwarder_loop;
// Chapter 33 (R6): the key-value sweeper job.
pub mod kv_sweep_loop;
// Chapter 51 (R9): sink tasks.
pub mod sinks_loop;
pub mod tenant_scopes;
pub mod tenants;
pub mod traffic;
pub mod tree;
pub mod tree_settings;
pub mod txn_resolver;
pub mod users;
pub mod workload_rollup;
pub mod write_load;

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
    let registry_for_sharding = registry.clone();
    let registry_for_scopes = registry.clone();
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
    let reconciler = Arc::new(
        CatalogReconciler::new(
            DdlEngine::new(versioner.clone(), Arc::clone(&clock)),
            Directory::new(Arc::clone(&stores.global)),
            Arc::new(PoolExecutors::new(locator.clone())),
            Arc::new(LogDecisions),
            dc.clone(),
            Some(settings.live.clone()),
            Arc::clone(&clock),
        )
        .with_group_maps(Arc::new(sprig_shard::ShardMapStore::new(Arc::clone(
            &stores.dc,
        ))))
        // bugfix-45: a crashed gateway's DDL job goes within about a minute.
        .with_owners(Arc::new(sprig_catalog::ddl::ProcessOwners::new(
            Arc::clone(&stores.dc),
        ))),
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
    // Automatic indexing holds a catalog connection for a whole pass (and
    // its Admin API for a request) while it reads snapshots, samples and
    // decisions: those long-held connections come from their own budget,
    // so they can never starve the short calls of every other loop
    // (`catalogs` module docs; the R0 catalog-connection deadlock).
    let ai_sessions = ShardMapLocator::new(
        Arc::clone(&stores.dc),
        Arc::new(
            RegistryPools::new(registry.clone(), Some(Arc::clone(&monitor)))
                .budget("autoindex", autoindex_loop::SESSIONS),
        ),
        default_naming(),
    );
    let indexes = Arc::new(
        autoindex_admin::AutoIndexService::new(
            Directory::new(Arc::clone(&stores.global)),
            Arc::new(ai_sessions.clone()),
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
    let logic_locator = Arc::clone(&catalog_locator);
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
    let ai_env = Arc::new(
        autoindex_env::ControllerEnv::new(
            dc.clone(),
            locator.clone(),
            registry.clone(),
            Some(Arc::clone(&monitor)),
            Some(settings.live.clone()),
            jobs.clone(),
        )
        .with_sessions(ai_sessions),
    );
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
            docs: Some(Arc::new(autoindex_doc::DocRegistry::new(versioner.clone()))),
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
    // Decision RF3 (r2last): tenant, database and object scopes from the
    // tenants' catalogs, for the loops below that decide per tenant (the balancer and
    // the sharding loop too: RL3, r2harden).
    let tenant_scopes = tenant_scopes::TenantScopes::new(
        Arc::clone(&stores.global),
        Arc::clone(&stores.dc),
        Arc::new(
            RegistryPools::new(registry_for_scopes, Some(Arc::clone(&monitor)))
                .budget("tenant_settings", 4),
        ),
    )?;
    let balancer = Arc::new(
        balancer_loop::BalancerLoop::new(
            dc.clone(),
            ClusterRegistry::new(Arc::clone(&stores.global), Arc::clone(&stores.dc))?,
            Arc::clone(&stores.dc),
            Arc::new(
                RegistryPools::new(registry_for_balancer, Some(Arc::clone(&monitor)))
                    .budget("balancer", catalogs::POOL_MAX),
            ),
            Arc::new(locator.clone()),
            Directory::new(Arc::clone(&stores.global)),
            Some(settings.live.clone()),
            Arc::clone(&clock),
            Some(move_exec),
            cfg.node.id().clone(),
        )
        .with_tenant_scopes(Arc::clone(&tenant_scopes)),
    );
    // Chapters 16 and 20 (R1): the advisor's first design and automatic
    // split, merge and move, per tenant homed here.
    let sharding = Arc::new(
        sharding_runtime::ShardingRuntime::new(
            dc.clone(),
            Arc::clone(&stores.dc),
            Arc::clone(&stores.global),
            Arc::clone(&clock),
            Arc::new(
                RegistryPools::new(registry_for_sharding, Some(Arc::clone(&monitor)))
                    .budget("sharding", sharding_runtime::POOL_MAX),
            ),
            Arc::new(sprig_balancer::decisions::CatalogDecisionLog::new(
                Arc::new(locator.clone()),
            )),
            Some(settings.live.clone()),
            &format!("controller:{}", cfg.node.id()),
        )
        .with_publish(versioner.clone())
        .with_tenant_scopes(Arc::clone(&tenant_scopes)),
    );
    // Chapter 52: approvals and recommendations. Items about a tenant live
    // in its catalog; cluster failovers in this DC's tier, DC failovers in
    // the global tier.
    let read_secret = |p: Option<&std::path::Path>| -> Result<Option<String>> {
        p.map(|p| {
            std::fs::read_to_string(p)
                .map(|s| s.trim().to_owned())
                .map_err(|e| {
                    SprigError::new(SprigCode::ConfigInvalid, format!("{}: {e}", p.display()))
                })
        })
        .transpose()
    };
    let notifier = Arc::new(approvals::notify::Notifier::new(
        Arc::new(locator.clone()),
        approvals::notify::Secrets {
            smtp_password: read_secret(cfg.controller.smtp_password_file.as_deref())?,
            webhook_secret: read_secret(cfg.controller.approvals_webhook_secret_file.as_deref())?,
            smtp_tls: sprig_approvals::smtp::TlsFiles {
                ca_pem: read_secret(cfg.controller.smtp_ca_file.as_deref())?,
                client_cert_pem: read_secret(cfg.controller.smtp_client_cert_file.as_deref())?,
                client_key_pem: read_secret(cfg.controller.smtp_client_key_file.as_deref())?,
            },
        },
    ));
    // Webhooks through chapter 51's webhook target: the platform URL and
    // tenants' own connections (their credentials open with the master
    // key file, as in sink workers).
    notifier.set_webhooks(Arc::new(approvals::webhook::Webhooks::new(
        Arc::new(locator.clone()),
        Arc::clone(&stores.dc),
        Arc::clone(&stores.global),
        cfg.controller.workload_key_file.as_deref(),
        Some(settings.live.clone()),
        dc.clone(),
        read_secret(cfg.controller.approvals_webhook_secret_file.as_deref())?,
    )));
    let approvals = {
        let stores_ = sprig_approvals::LocatorStores::new(Arc::new(locator.clone()))
            .with_dc(dc.clone(), Arc::clone(&stores.dc))
            .with_global(Arc::clone(&stores.global));
        let hub = sprig_approvals::Approvals::new(Arc::new(stores_), Arc::clone(&clock))
            .with_hook(Arc::new(approvals::DecisionHook::new(
                Arc::new(sprig_balancer::decisions::CatalogDecisionLog::new(
                    Arc::new(locator.clone()),
                )),
                dc.clone(),
            )))
            .with_hook(Arc::clone(&notifier) as Arc<dyn sprig_approvals::Hook>);
        let a = Arc::new(
            approvals::ControllerApprovals::new(
                Arc::new(hub),
                dc.clone(),
                Directory::new(Arc::clone(&stores.global)),
                Some(settings.live.clone()),
            )
            .with_tenant_scopes(Arc::clone(&tenant_scopes))
            .with_locator(Arc::new(locator.clone())),
        );
        let ix = Arc::new(approvals::index::IndexApprovals::new(Arc::clone(&indexes)));
        a.register_producer(ix.clone());
        a.register(sprig_approvals::Kind::IndexCreate, ix);
        a.register(
            sprig_approvals::Kind::DcMove,
            Arc::new(approvals::DcMoveProgress::new(Directory::new(Arc::clone(
                &stores.global,
            )))),
        );
        a
    };
    let _ = sharding.sharding().approvals.set(Arc::clone(&approvals));
    let _ = balancer.approvals.set(Arc::clone(&approvals));
    approvals::install(Arc::clone(&approvals));
    notifier.bind(Arc::clone(&approvals));
    // Chapter 20: load splits need placement's hot clusters.
    sharding
        .write_load()
        .set_placement(Arc::clone(&balancer) as Arc<dyn write_load::HotClusters>);
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
    // bugfix-27: cleanup drops a deleted tenant's databases itself.
    tenants.set_database_dropper(Arc::clone(&reconciler) as Arc<dyn tenants::DeletedDatabases>);
    let tree_loop = TreeSettings::new(tree, dc.clone(), Some(settings.live.clone()))?;
    // Chapter 24: moves between DCs and traffic windows.
    let moves = Arc::new(
        migrate::DcMoves::new(
            dc.clone(),
            Arc::clone(&stores.dc),
            Arc::clone(&stores.global),
            Arc::clone(&clock),
            Some(settings.live.clone()),
        )
        .with_tenant_scopes(Arc::clone(&tenant_scopes)),
    );
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
        // Chapter 24 steps 7-9 (r2last): tenants follow their traffic.
        .with_loop(Loop::new(
            dc_migrate::MigrationDecider::new(Arc::clone(&moves), Some(Arc::clone(&balancer)))
                .with_approvals(Arc::clone(&approvals)),
        ))
        .with_loop(Loop::from_arc(Arc::clone(&sharding)))
        // Chapter 18: in-doubt distributed transactions and decision rows.
        .with_loop(Loop::new(txn_resolver::ResolverLoop::for_controller(
            &dc,
            &stores,
            Arc::clone(&monitor),
            settings.live.clone(),
        )?))
        // Chapter 41 (R7 order 5): after-commit trigger jobs and schedules.
        .with_loop(Loop::new(logic_loop::LogicLoop::new(logic_locator)))
        .with_loop(Loop::new(inherit_loop::InheritLoop))
        .with_loop(Loop::new(streams_loop::StreamsLoop))
        .with_loop(Loop::new(graph_mirror_loop::GraphMirrorLoop))
        .with_loop(Loop::new(change_exchange_loop::ChangeExchangeLoop))
        .with_loop(Loop::new(search_loop::SearchPlacementLoop))
        .with_loop(Loop::new(queue_forwarder_loop::QueueForwarderLoop))
        .with_loop(Loop::new(kv_sweep_loop::KvSweepLoop::new(Some((
            settings.live.clone(),
            dc.clone(),
        )))))
        .with_loop(Loop::new(sinks_loop::SinksLoop::default()))
        // Chapter 52: escalation, auto-apply, appliers, expiry, gauges.
        .with_loop(Loop::new(
            approvals::ApprovalsLoop::new(Arc::clone(&approvals)).with_notifier(Arc::clone(&notifier)),
        ))
        // Chapter 52: the same for DC failover items in the global tier.
        .with_loop(Loop::new(approvals::GlobalApprovalsLoop::new(Arc::clone(&approvals))))
        .global_candidate(cfg.controller.global_candidate);
    // Chapter 25 (r2final): job standbys, in single-etcd mode (the loop
    // writes the standby DC's tier through the one etcd).
    let rt = if single_etcd(&cfg.meta) {
        rt.with_loop(Loop::new(
            standby::StandbyLoop::new(
                dc.clone(),
                Arc::clone(&stores.dc),
                Arc::clone(&stores.global),
                Arc::clone(&clock),
                Some(settings.live.clone()),
            )
            .with_tenant_scopes(Arc::clone(&tenant_scopes))
            .with_move_intents(Arc::clone(&moves.decisions))
            .with_approvals(Arc::clone(&approvals)),
        ))
    } else {
        rt
    };
    // Chapter 25 (R2): the DC lease, permits, observations, the return of
    // a lost DC and the global failover decision (failover::role).
    let failover = failover::role::FailoverRole::new(
        dc.clone(),
        cfg.node.id(),
        stores.clone(),
        Arc::clone(&clock),
        Some(settings.live.clone()),
        single_etcd(&cfg.meta),
    );
    let rt = failover.loops().into_iter().fold(rt, Runtime::with_loop);
    // Chapter 26 (R2): residency policies, their convergence loop and the
    // Admin calls.
    let residency = Arc::new(residency::Residency::new(
        dc.clone(),
        Arc::clone(&stores.global),
        failover.dc_tiers(),
        Arc::clone(&clock),
        Some(Arc::clone(&moves)),
    ));
    let rt = rt.with_loop(Loop::new(residency::ResidencyLoop(Arc::clone(&residency))));
    // Chapter 23 steps 12 and 13 (R2): the latency matrix and the DC
    // lifecycle.
    let latency = latency::LatencyProber::new(
        dc.clone(),
        Arc::clone(&stores.global),
        Some(settings.live.clone()),
    );
    let rt = latency.loops().into_iter().fold(rt, Runtime::with_loop);
    let (internal, internal_addr) = failover::role::bind(
        cfg.controller.internal_addr,
        cfg.controller.internal_advertise.as_deref(),
    )
    .await?;
    // Hardening round 3: mutual TLS on the internal port with the process
    // identity, when `[controller] internal_tls` is on.
    let internal_tls = internal_tls_config(cfg)?;
    let internal_https = internal_tls.is_some();
    let internal_task = failover.serve_internal(
        internal,
        &internal_addr,
        axum::Router::new(),
        internal_tls,
        ctx.shutdown.clone(),
    );
    let failover_tasks = failover.spawn(&rt.status(), &ctx.shutdown);

    let token = cfg
        .controller
        .admin_token_file
        .as_deref()
        .map(http::load_token)
        .transpose()?;
    let gateway_token = cfg
        .controller
        .gateway_token_file
        .as_deref()
        .map(http::load_token)
        .transpose()?;
    // Chapter 14 (operators): the operator directory in the global tier,
    // and the guard in front of the whole Admin API.
    let operators = Arc::new(operators::Operators::new(
        Arc::clone(&stores.global),
        Arc::clone(&clock),
        match &cfg.controller.workload_key_file {
            Some(p) => Some(Arc::new(operators::keys::OperatorKeys(
                sprig_workload::keyring::MasterKeyring::from_file(p)?,
            )) as Arc<dyn sprig_auth::mfa::FactorKeys>),
            None => None,
        },
        Some(settings.live.clone()),
        dc.clone(),
    ));
    let mut services: Vec<(String, String)> = Vec::new();
    if let Some(g) = &gateway_token {
        services.push(("gateway".to_owned(), g.clone()));
    }
    for (name, path) in &cfg.controller.service_token_files {
        services.push((name.clone(), http::load_token(path)?));
    }
    let named = operators.switched().await.unwrap_or(false);
    // Chapter 37, "Admin API over TLS": HTTPS when a certificate is set.
    let admin_tls = http::tls_config(&cfg.controller)?;
    let admin_https = admin_tls.is_some();
    let listener = http::bind(
        cfg.controller.admin_addr,
        if named {
            Some("operators")
        } else {
            token.as_deref()
        },
    )
    .await?;
    let admin_addr = listener
        .local_addr()
        .map_err(|e| SprigError::internal("admin listener address").with_source(e))?;
    let reg_store = Arc::clone(&stores.dc);
    let svc = Arc::new(
        AdminService::new(stores, monitor, rt.status(), Arc::clone(&clock))
            .with_tenants(tenants)
            .with_users(users)
            .with_settings(Arc::clone(&settings_admin) as Arc<dyn settings_admin::SettingsAdmin>)
            .with_indexes(indexes)
            .with_placement(balancer)
            .with_moves(moves)
            .with_sharding(sharding.sharding())
            .with_failover(Arc::clone(&failover))
            .with_residency(residency)
            .with_search_catalog(versioner.clone()),
    );
    // Chapter 52: the `Recommendations` service beside the Admin API.
    let dc_token = cfg
        .controller
        .dc_operator_token_file
        .as_deref()
        .map(http::load_token)
        .transpose()?;
    let recs = approvals::http::router(Arc::new(approvals::http::RecApp::new(
        Arc::new(approvals::service::RecommendationsService::new(
            Arc::clone(&approvals),
            Some(Arc::clone(&settings_admin) as Arc<dyn settings_admin::SettingsAdmin>),
        )),
        Some(Arc::clone(&svc)),
        None,
        None,
        None,
        dc.clone(),
    )));
    let guard = Arc::new(operators::guard::Guard::new(
        Arc::clone(&operators),
        token,
        dc_token,
        services,
        cfg.controller.admin_token_break_glass,
    ));
    let admin = tokio::spawn(http::serve_tls(
        listener,
        http::router_guarded(svc)
            .merge(recs)
            .merge(operators::http::router(operators))
            .layer(axum::middleware::from_fn_with_state(
                guard,
                operators::guard::layer,
            )),
        admin_tls,
        ctx.shutdown.clone(),
    ));
    // Gateways find the leader's Admin API here (chapter 37 step 4).
    let registered = tokio::spawn(registration::keep(
        Arc::clone(&reg_store),
        Arc::clone(&clock),
        cfg.node.id(),
        registration::ControllerRecord {
            admin_addr: cfg
                .controller
                .admin_advertise
                .clone()
                .unwrap_or_else(|| registration::advertised(admin_addr)),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            internal_addr: Some(internal_addr.clone()),
            admin_tls: admin_https,
            internal_tls: internal_https,
        },
        ctx.shutdown.clone(),
    ));

    ctx.health.set_ready();
    tracing::info!(%role, %dc, admin = %admin_addr, admin_tls = admin_https, internal = %internal_addr, internal_tls = internal_https, "ready");
    rt.run(ctx.shutdown.clone()).await;
    ctx.health.set_draining();
    match admin.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(error = %e, "the Admin API stopped with an error"),
        Err(e) => tracing::warn!(error = %e, "the Admin API task failed"),
    }
    let _ = registered.await;
    for t in failover_tasks {
        let _ = t.await;
    }
    let _ = internal_task.await;
    tracing::info!(%role, "stopped every loop");
    Ok(())
}

/// Single-etcd mode: one etcd serves both tiers (no global seeds, or the
/// same seeds as the DC's).
/// The internal port's server configuration when `[controller]
/// internal_tls` is on: `[tls] cert` and `key` (re-read when they change),
/// and client certificates signed by `[tls] ca` required.
///
/// # Errors
/// 39998 when `internal_tls` is on without `[tls]`, or a file doesn't load.
fn internal_tls_config(
    cfg: &sprig_common::ProcessConfig,
) -> Result<Option<Arc<sprig_common::https::ServerConfig>>> {
    if !cfg.controller.internal_tls {
        return Ok(None);
    }
    let Some(t) = &cfg.tls else {
        return Err(sprig_common::SprigError::new(
            sprig_common::SprigCode::ConfigInvalid,
            "[controller] internal_tls needs [tls] (ca, cert, key)",
        ));
    };
    sprig_common::https::server_config_named(
        &t.cert,
        &t.key,
        Some(&t.ca),
        sprig_common::https::INTERNAL_NAMES,
    )
    .map(Some)
}

fn single_etcd(meta: &sprig_common::config::MetaConfig) -> bool {
    let norm = |v: &[String]| {
        let mut v: Vec<String> = v
            .iter()
            .map(|s| s.trim_end_matches('/').to_owned())
            .collect();
        v.sort();
        v
    };
    meta.global_seeds.is_empty() || norm(&meta.global_seeds) == norm(&meta.dc_seeds)
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
