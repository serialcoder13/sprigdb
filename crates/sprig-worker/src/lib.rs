//! `sprig-worker`: the worker role, the job runtime.
//!
//! Layer 3 (roles).
//!
//! [`run`] connects to the DC and global tiers and runs the mover's job
//! runtime (`sprig_mover::JobRuntime`): it claims runnable mover jobs in
//! its DC, runs each to its end, and gives them up when its lease is lost.
//! Every job resolves its settings from the worker's live mirrors of the
//! etcd scopes when it uses them ([`settings`]). Automatic index builds
//! don't run here yet.

// The asynchronous graph mirror applier and the `queue.change_exchange`
// forwarders.
pub mod change_exchange;
pub mod graph_mirror;
pub mod inherit;
// The key-value expiry and old-generation sweeper.
pub mod kv_sweep;
// The TTL monitor.
pub mod doc_ttl;
pub mod logic;
// Triggers on streams and queues.
pub mod msg_triggers;
pub mod permit;
// The feed-driven queue outbox forwarder.
pub mod queue_forwarder;
// Recovery: errant_extract and node_rebuild.
pub mod recovery;
pub mod runtime;
// The search role.
pub mod search;
// Live settings for every job.
pub mod settings;
// Stream sequencers.
pub mod streams;
// Sink tasks.
pub mod sinks;
pub mod xa_drain;

use std::sync::Arc;
use std::time::Duration;

use sprig_cluster::registry::ClusterRegistry;
use sprig_common::time::SystemClock;
use sprig_common::{Result, Role, RoleContext, SprigError};
use sprig_meta::MetaStore;
use sprig_meta::etcd::{EtcdTls, connect_tiers};
use sprig_mover::mysql::MysqlEnvFactory;
use sprig_mover::{JobRuntime, JobStore, MoverSettings, WorkerOpts};
use sprig_shard::TenantMaps;

/// Runs the worker role until `ctx.shutdown` is cancelled.
///
/// The `sprigdb` binary has already bound the telemetry port, so
/// `/healthz` answers and `/readyz` reports `starting` until this marks the
/// process ready.
///
/// # Errors
/// etcd can't be reached at start.
#[allow(clippy::too_many_lines)] // one wiring sequence
pub async fn run(ctx: RoleContext) -> Result<()> {
    let role = Role::Worker;
    let cfg = &ctx.config;
    let tls = cfg.tls.as_ref().map(EtcdTls::from_files).transpose()?;
    let tiers = connect_tiers(
        cfg.node.dc.clone(),
        &cfg.meta,
        tls,
        Duration::from_secs(5),
        true,
    )
    .await?;
    let dc: Arc<dyn MetaStore> = Arc::new(tiers.dc);
    let global: Arc<dyn MetaStore> = Arc::new(
        tiers
            .global
            .ok_or_else(|| SprigError::internal("workers connect the global tier"))?,
    );
    let registry = ClusterRegistry::new(Arc::clone(&global), dc.clone())?;
    let registry_logic = registry.clone();
    let registry_search = registry.clone();
    let sweep_registry = registry.clone();
    let ttl_registry = registry.clone();
    let ttl_dc = dc.clone();
    let recovery_env = recovery::RecoveryEnv {
        dc: dc.clone(),
        registry: registry.clone(),
    };
    let errant_extract = recovery::ErrantExtract::from_config(recovery_env.clone(), &cfg.recovery)?;
    let dc_logic = dc.clone();
    // Live settings: the mirrors of the global, dc and
    // cluster scopes, read by every job when it uses a key.
    let Some(live) = settings::WorkerSettings::start(
        Arc::clone(&global),
        dc.clone(),
        cfg.node.dc.clone(),
        &ctx.shutdown,
    )
    .await?
    else {
        return Ok(());
    };
    let settings = live.live().map_or_else(MoverSettings::default, |l| {
        MoverSettings::resolve(
            &l.snapshot(),
            &sprig_settings::SettingCtx::process(&cfg.node.dc),
        )
    });
    // The XA resolver finishes the tenant's prepared branches at each
    // cutover, once before the source is fenced and again after it.
    let drain_id = cfg
        .node
        .id
        .clone()
        .unwrap_or_else(|| format!("worker-{}", std::process::id()));
    let drain = Arc::new(xa_drain::ResolverDrain::new(
        &dc,
        registry.clone(),
        &drain_id,
    ));
    // Moves between DCs commit on the one etcd of the single-etcd
    // mode: this DC's tier reaches the others.
    let mut factory = MysqlEnvFactory::new(registry, TenantMaps::new(dc.clone()), settings.clone())
        .with_home_changes(dc.clone())
        // Splits, merges and shard moves cut over on the
        // table groups' maps in this DC's tier.
        .with_group_maps(dc.clone())
        .with_drain(drain);
    // Each job's `mover.*` from the live settings, followed while it runs.
    if let Some(l) = live.live() {
        factory = factory.with_live_settings(l.clone(), cfg.node.dc.clone());
    }
    // ...and from its tenant's own catalog rows.
    let tenant_scopes = sprig_controller::tenant_scopes::TenantScopes::new(
        Arc::clone(&global),
        dc.clone(),
        Arc::new(
            sprig_controller::catalogs::RegistryPools::new(registry_logic.clone(), None)
                .budget("tenant_settings", 2),
        ),
    )?;
    factory = factory.with_tenant_rows(Arc::new(sprig_controller::tenant_scopes::MoverRows(
        Arc::clone(&tenant_scopes),
    )));
    let live = live.with_tenant_scopes(Arc::clone(&tenant_scopes));
    let factory = Arc::new(factory);
    let store = JobStore::new(dc, Arc::new(SystemClock));
    let worker = cfg
        .node
        .id
        .clone()
        .unwrap_or_else(|| format!("worker-{}", std::process::id()));
    let mut opts = WorkerOpts::new(&worker);
    opts.lease = settings.job_lease;
    // Mover jobs (cutovers, moves between DCs) run only while the worker
    // holds a valid DC permit.
    let (keeper, watchdog) = permit::keep(
        &worker,
        dc_logic.clone(),
        Duration::from_millis(1000),
        ctx.shutdown.clone(),
    );
    // The permit loop stops with the shutdown, and its heartbeat with it:
    // the watchdog goes first, so it doesn't abort the drain.
    let sd = ctx.shutdown.clone();
    tokio::spawn(async move {
        sd.cancelled().await;
        std::thread::spawn(move || drop(watchdog));
    });
    let gate_factory: Arc<dyn sprig_mover::EnvFactory> = factory;
    let (gate_store, gate_opts) = (store, opts);
    // The job runtime with the logic jobs: after-commit triggers and
    // scheduled runs.
    let pools: Arc<dyn sprig_controller::catalogs::ClusterPools> = Arc::new(
        sprig_controller::catalogs::RegistryPools::new(registry_logic.clone(), None)
            .budget("logic", 8),
    );
    // Shards get their own connection budget: a trigger holds a shard's
    // connection while it runs, and must never wait for a catalog one
    // from the same budget.
    let shard_pools: Arc<dyn sprig_controller::catalogs::ClusterPools> = Arc::new(
        sprig_controller::catalogs::RegistryPools::new(registry_logic.clone(), None)
            .budget("logic_shards", 16),
    );
    let registry_clusters = Arc::new(logic::RegistryClusters::new(
        registry_logic.clone(),
        Arc::clone(&pools),
    ));
    // One shared binlog reader per cluster for this
    // worker's feed consumers: stream sequencers, the queue forwarder,
    // after-commit triggers and search copies.
    let shared_feed = sprig_feed::shared::SharedFeed::new(
        Arc::clone(&registry_clusters) as Arc<dyn sprig_feed::Clusters>,
        sprig_feed::shared::SharedOpts::default(),
    );
    let env = logic::LogicEnv {
        dc: dc_logic.clone(),
        registry: registry_logic,
        pools: shard_pools,
        locator: Arc::new(sprig_controller::catalogs::ShardMapLocator::new(
            dc_logic.clone(),
            Arc::clone(&pools),
            sprig_controller::catalogs::default_naming(),
        )),
        clusters: registry_clusters,
        feed: shared_feed.view(sprig_feed::shared::Kept::All),
        opts: live.run_opts(),
        settings: live.clone(),
        // Set below, once the gateways are known.
        gateways: None,
    };
    // The copy worker: one job per tenant tree.
    let inherit_env = Arc::new(inherit::LiveEnv::new(
        dc_logic.clone(),
        Arc::clone(&global),
        Arc::clone(&env.locator),
        Arc::clone(&env.pools),
        Arc::clone(&env.clusters),
    )?);
    // The graph mirror applier: one job per tenant.
    let graph_mirror_env = graph_mirror::GraphMirrorEnv {
        dc: dc_logic.clone(),
        pools: Arc::clone(&env.pools),
        locator: Arc::clone(&env.locator),
        clusters: Arc::clone(&env.clusters) as Arc<dyn sprig_feed::Clusters>,
    };
    // Stream sequencers, on the logic jobs' pools and the
    // shared reader.
    let streams_env = streams::StreamsEnv::from_logic(&env, &shared_feed);
    // Sink tasks, on the same pools, with the global tier
    // (platform connections) and the master key file (credentials).
    let sinks_env = sinks::SinksEnv::new(
        &env,
        Arc::clone(&global),
        cfg.gateway
            .workload_key_file
            .as_deref()
            .or(cfg.controller.workload_key_file.as_deref()),
        &worker,
    );
    // Triggers on streams and queues, run in gateways.
    let peer_pems = cfg
        .tls
        .as_ref()
        .map(msg_triggers::PeerPems::from_config)
        .transpose()?;
    let msg_env = msg_triggers::MsgEnv {
        dc: dc_logic.clone(),
        locator: Arc::clone(&env.locator),
        pools: Arc::clone(&env.pools),
        gateways: Arc::new(msg_triggers::Gateways::new(dc_logic.clone(), peer_pems)),
        opts: env.opts.clone(),
        settings: live.clone(),
    };
    // Sink tasks reach the gateways too.
    let sinks_env = sinks_env.with_peer(Arc::clone(&msg_env.gateways) as Arc<_>);
    // The feed-driven queue outbox forwarder, through the
    // gateways too.
    let forwarder_env = queue_forwarder::ForwarderEnv {
        dc: dc_logic.clone(),
        locator: Arc::clone(&env.locator),
        // The shared reader's view of every
        // tenant's queue outbox rows.
        clusters: queue_forwarder::shared_view(&shared_feed),
        peer: Arc::clone(&msg_env.gateways) as Arc<dyn sprig_sinks::peer::SinkPeer>,
    };
    // The search role, when SPRIG_SEARCH_LISTEN
    // names this worker's search address.
    let search_role = search::SearchRole::from_env(
        search::SearchDeps {
            worker_id: worker.clone(),
            dc_id: cfg.node.dc.clone(),
            dc: dc_logic.clone(),
            registry: registry_search,
            // Search copies read through the shared reader.
            clusters: Arc::clone(&env.feed),
            locator: Arc::clone(&env.locator),
            gateways: Arc::clone(&msg_env.gateways),
            tls: cfg
                .tls
                .as_ref()
                .map(msg_triggers::PeerPems::from_config)
                .transpose()?,
            live: live.live().cloned(),
        },
        ctx.shutdown.clone(),
    )?;
    if let Some(r) = &search_role {
        r.serve().await?;
    }
    // Agents' internal services over mutual TLS with `[tls]`.
    let mut rebuild_handler = recovery::NodeRebuild::new(recovery_env);
    if let Some(t) = cfg.tls.as_ref() {
        rebuild_handler = rebuild_handler
            .with_tls(sprig_cluster::failover::internal_tls::InternalTls::from_config(t)?);
    }
    let mut jobs = runtime::WorkerRuntime::new(dc_logic, Arc::new(SystemClock), worker.clone())
        .with_handler(Arc::new(logic::LogicTrigger(logic::LogicEnv {
            // After-commit triggers check a bound
            // definer's events on the gateways.
            gateways: Some((
                Arc::clone(&msg_env.gateways) as Arc<dyn sprig_sinks::peer::SinkPeer>,
                Arc::clone(&global),
            )),
            ..env.clone()
        })))
        .with_handler(Arc::new(change_exchange::ChangeExchange(logic::LogicEnv {
            gateways: Some((
                Arc::clone(&msg_env.gateways) as Arc<dyn sprig_sinks::peer::SinkPeer>,
                Arc::clone(&global),
            )),
            ..env.clone()
        })))
        .with_handler(Arc::new(logic::LogicSchedule(env)))
        .with_handler(Arc::new(inherit::InheritCopy(inherit_env)))
        .with_handler(Arc::new(graph_mirror::GraphMirror(graph_mirror_env)))
        .with_handler(Arc::new(streams::StreamSequencer(streams_env)))
        .with_handler(Arc::new(sinks::SinkTask(sinks_env)))
        .with_handler(Arc::new(queue_forwarder::QueueForwarder(forwarder_env)))
        .with_handler(Arc::new(msg_triggers::StreamTrigger(msg_env.clone())))
        .with_handler(Arc::new(msg_triggers::QueueTrigger(msg_env)))
        .with_handler(Arc::new(kv_sweep::KvSweep {
            pools: Arc::new(
                sprig_controller::catalogs::RegistryPools::new(sweep_registry, None)
                    .budget("kv_sweep", 2),
            ),
            settings: Some(live.clone()),
        }))
        .with_handler(Arc::new(doc_ttl::DocTtl(doc_ttl::DocTtlEnv {
            dc: ttl_dc,
            pools: Arc::new(
                sprig_controller::catalogs::RegistryPools::new(ttl_registry, None)
                    .budget("doc_ttl", 2),
            ),
            settings: live.clone(),
        })))
        .with_handler(Arc::new(errant_extract))
        .with_handler(Arc::new(rebuild_handler));
    if let Some(r) = search_role {
        jobs = jobs
            .with_handler(Arc::new(search::SearchCopyJob(r)))
            .with_extra_slots(&[search::SearchRole::slots()]);
    }
    ctx.health.set_ready();
    tracing::info!(%role, %worker, "ready: running mover and logic jobs");
    let shutdown = ctx.shutdown.clone();
    let jobs_task = tokio::spawn(async move { jobs.run(shutdown).await });
    let k = Arc::clone(&keeper);
    let r = permit::gated(
        move || k.may_write(),
        ctx.shutdown.clone(),
        move |stop| {
            let runtime = JobRuntime::new(
                gate_store.clone(),
                Arc::clone(&gate_factory),
                gate_opts.clone(),
            );
            async move { runtime.run(stop).await }
        },
    )
    .await;
    let _ = jobs_task.await;
    ctx.health.set_draining();
    tracing::info!(%role, "stopped");
    r
}
