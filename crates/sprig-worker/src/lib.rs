//! `sprig-worker`: the worker role, the job runtime.
//!
//! Layer 3 (roles). Design: [chapter 10](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/10-mover.md) (The mover) and [chapter 13](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/13-auto-indexing.md) (Automatic indexing) of the SprigDB low-level design.
//!
//! [`run`] connects to the DC and global tiers and runs the mover's job
//! runtime (`sprig_mover::JobRuntime`): it claims runnable mover jobs in
//! its DC, runs each to its end, and gives them up when its lease is lost.
//! Settings are the `mover.*` defaults (live resolution per job is a later
//! step). Index builds (chapter 13) don't run here yet.

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
    let registry = ClusterRegistry::new(global, dc.clone())?;
    let settings = MoverSettings::default();
    let factory = Arc::new(MysqlEnvFactory::new(
        registry,
        TenantMaps::new(dc.clone()),
        settings.clone(),
    ));
    let store = JobStore::new(dc, Arc::new(SystemClock));
    let worker = cfg
        .node
        .id
        .clone()
        .unwrap_or_else(|| format!("worker-{}", std::process::id()));
    let mut opts = WorkerOpts::new(&worker);
    opts.lease = settings.job_lease;
    let runtime = JobRuntime::new(store, factory, opts);
    ctx.health.set_ready();
    tracing::info!(%role, %worker, "ready: running mover jobs");
    let r = runtime.run(ctx.shutdown.clone()).await;
    ctx.health.set_draining();
    tracing::info!(%role, "stopped");
    r
}
