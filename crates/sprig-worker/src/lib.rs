//! `sprig-worker`: the worker role, the job runtime.
//!
//! Layer 3 (roles). Design: [chapter 10](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/10-mover.md) (The mover) and [chapter 13](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/13-auto-indexing.md) (Automatic indexing) of the SprigDB low-level design.
//!
//! [`run`] connects to the DC and global tiers and runs the mover's job
//! runtime (`sprig_mover::JobRuntime`): it claims runnable mover jobs in
//! its DC, runs each to its end, and gives them up when its lease is lost.
//! Settings are the `mover.*` defaults (live resolution per job is a later
//! step). Index builds (chapter 13) don't run here yet.

pub mod inherit;
pub mod logic;
// Chapter 41 (R9): triggers on streams and queues.
pub mod msg_triggers;
pub mod permit;
pub mod runtime;
// Chapter 45 (R9): stream sequencers.
pub mod streams;
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
    let dc_logic = dc.clone();
    let settings = MoverSettings::default();
    // Chapter 18's resolver finishes the tenant's prepared branches at
    // each cutover (chapter 10, steps 2b and 3b).
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
    // Moves between DCs (chapter 24) commit on the one etcd of R0's
    // single-etcd mode: this DC's tier reaches the others.
    let factory = Arc::new(
        MysqlEnvFactory::new(registry, TenantMaps::new(dc.clone()), settings.clone())
            .with_home_changes(dc.clone())
            // Splits, merges and shard moves (chapter 20) cut over on the
            // table groups' maps in this DC's tier.
            .with_group_maps(dc.clone())
            .with_drain(drain),
    );
    let store = JobStore::new(dc, Arc::new(SystemClock));
    let worker = cfg
        .node
        .id
        .clone()
        .unwrap_or_else(|| format!("worker-{}", std::process::id()));
    let mut opts = WorkerOpts::new(&worker);
    opts.lease = settings.job_lease;
    // Chapter 25: mover jobs (cutovers, moves between DCs) run only while
    // the worker holds a valid DC permit (r2next).
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
    // Chapter 23's job runtime with chapter 41's logic jobs: after-commit
    // triggers and scheduled runs (R7 order 5).
    let pools: Arc<dyn sprig_controller::catalogs::ClusterPools> = Arc::new(
        sprig_controller::catalogs::RegistryPools::new(registry_logic.clone(), None)
            .budget("logic", 8),
    );
    // Shards get their own connection budget: a trigger holds a shard's
    // connection while it runs, and must never wait for a catalog one
    // from the same budget (chapter 13's rule for catalog pools).
    let shard_pools: Arc<dyn sprig_controller::catalogs::ClusterPools> = Arc::new(
        sprig_controller::catalogs::RegistryPools::new(registry_logic.clone(), None)
            .budget("logic_shards", 16),
    );
    let env = logic::LogicEnv {
        dc: dc_logic.clone(),
        registry: registry_logic.clone(),
        pools: shard_pools,
        locator: Arc::new(sprig_controller::catalogs::ShardMapLocator::new(
            dc_logic.clone(),
            Arc::clone(&pools),
            sprig_controller::catalogs::default_naming(),
        )),
        clusters: Arc::new(logic::RegistryClusters::new(
            registry_logic,
            Arc::clone(&pools),
        )),
        opts: logic::run_opts_from_env(),
        trigger_budget: Duration::from_millis(30_000),
        job_budget: Duration::from_millis(300_000),
    };
    // Chapter 42's copy worker: one job per tenant tree.
    let inherit_env = Arc::new(inherit::LiveEnv::new(
        dc_logic.clone(),
        Arc::clone(&global),
        Arc::clone(&env.locator),
        Arc::clone(&env.pools),
        Arc::clone(&env.clusters),
    )?);
    // Chapter 45's stream sequencers, on the logic jobs' pools and feeds.
    let streams_env = streams::StreamsEnv::from_logic(&env);
    // Chapter 41 (R9): triggers on streams and queues, run in gateways.
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
        budget: env.trigger_budget,
    };
    let jobs = runtime::WorkerRuntime::new(dc_logic, Arc::new(SystemClock), worker.clone())
        .with_handler(Arc::new(logic::LogicTrigger(env.clone())))
        .with_handler(Arc::new(logic::LogicSchedule(env)))
        .with_handler(Arc::new(inherit::InheritCopy(inherit_env)))
        .with_handler(Arc::new(streams::StreamSequencer(streams_env)))
        .with_handler(Arc::new(msg_triggers::StreamTrigger(msg_env.clone())))
        .with_handler(Arc::new(msg_triggers::QueueTrigger(msg_env)));
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
