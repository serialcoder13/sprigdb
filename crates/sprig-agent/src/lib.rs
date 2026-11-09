//! `sprig-agent`: the agent role, the node agent for VMs: MariaDB install,
//! settings, health, backups.
//!
//! Layer 3 (roles). Design: [chapter 27](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/27-provisioning.md) (Provisioning) and
//! [chapter 25](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/25-failover.md) (Failover) of the SprigDB low-level design.
//!
//! What exists: [`fence`], chapter 25's self-fence of the agent's MariaDB
//! node (R2): the DC permit, the writer lease, `read_only` and session
//! kills, and the watchdog. It runs when `[agent] cluster` names the
//! node's cluster. Not yet: chapter 27's plan engine and MariaDB
//! supervision (steps 4-6).

pub mod fence;
pub mod rebuild;

use std::sync::Arc;
use std::time::Duration;

use sprig_common::{Result, Role, RoleContext};
use sprig_meta::MetaStore;
use sprig_meta::etcd::{DEFAULT_REQUEST_TIMEOUT, EtcdTls, connect_tiers};

/// Runs the agent role until `ctx.shutdown` is cancelled.
///
/// The `sprigdb` binary has already bound the telemetry port, so
/// `/healthz` answers and `/readyz` reports `starting` until this marks the
/// process ready.
///
/// # Errors
/// 39998 for unreadable TLS files or an `[agent]` that names no node of
/// its cluster; 39500 if no etcd seed answers at start-up.
pub async fn run(ctx: RoleContext) -> Result<()> {
    let role = Role::Agent;
    let cfg = &ctx.config;
    if cfg.agent.cluster.is_none() {
        tracing::warn!(%role, "no [agent] cluster: no node to fence; no plan engine yet (chapter 27 steps 4-6)");
        ctx.health.set_ready();
        ctx.shutdown.cancelled().await;
        ctx.health.set_draining();
        return Ok(());
    }
    let tls = cfg.tls.as_ref().map(EtcdTls::from_files).transpose()?;
    let tiers = connect_tiers(
        cfg.node.dc.clone(),
        &cfg.meta,
        tls,
        DEFAULT_REQUEST_TIMEOUT,
        false,
    )
    .await?;
    let store: Arc<dyn MetaStore> = Arc::new(tiers.dc);
    // The cluster may be registered after the agent starts.
    let target = loop {
        match fence::target(&*store, &cfg.agent).await {
            Ok(t) => break t,
            Err(e) => {
                tracing::warn!(error = %e, "can't find the node to fence yet; retrying");
                tokio::select! {
                    () = ctx.shutdown.cancelled() => return Ok(()),
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
        }
    };
    let mut agent = fence::AgentFence::new(
        cfg.node.id(),
        cfg.node.dc.clone(),
        Arc::clone(&store),
        target.clone(),
    );
    // Chapter 25's node_rebuild: the internal service (backup streams and
    // rebuilds), when `[agent] internal_addr` is set.
    let mut service_task = None;
    if let Some(addr) = cfg.agent.internal_addr {
        let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
            sprig_common::SprigError::new(
                sprig_common::SprigCode::ConfigInvalid,
                format!("[agent] internal_addr {addr}: {e}"),
            )
        })?;
        let bound = listener.local_addr().map_err(|e| {
            sprig_common::SprigError::internal(format!("[agent] internal_addr: {e}"))
        })?;
        let url = cfg
            .agent
            .internal_advertise
            .clone()
            .unwrap_or_else(|| format!("http://{bound}"));
        let service = Arc::new(rebuild::RebuildService::new(
            cfg.agent.rebuild.clone(),
            store,
            target.cluster.clone(),
            target.node.clone(),
            target.endpoint.clone(),
            agent.hold(),
        ));
        service_task = Some(rebuild::serve(
            listener,
            Arc::clone(&service),
            ctx.shutdown.clone(),
        ));
        tracing::info!(%url, "agent internal service (node_rebuild) listening");
        agent = agent.with_rebuild(url, service);
    }
    ctx.health.set_ready();
    tracing::info!(%role, "ready");
    let r = agent
        .run(Duration::from_millis(1000), ctx.shutdown.clone())
        .await;
    if let Some(t) = service_task {
        let _ = t.await;
    }
    ctx.health.set_draining();
    r
}
