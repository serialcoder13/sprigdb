//! The `queue_forwarder` loop (publishing inside a database transaction),
//! on the DC leader, every two seconds:
//!
//! ```text
//! if any feed subscription of kind `queue_forwarder` is active (one per
//!   tenant, registered by a gateway when it first opens a queue store or
//!   makes a cross-tenant outbox for the tenant):
//!   each cluster of this DC without a live `queue_forwarder` job → submit
//!   one (spec: cluster); a job whose worker died is requeued by dispatch,
//!   and a failed or finished one is replaced here, so each cluster has
//!   one forwarder, which serves every subscribed tenant the cluster holds
//!   from one binlog dump
//! ```
//!
//! Jobs of the older per-tenant shape (spec: tenant) end themselves on a
//! worker of this build.
//!
//! The worker's job (`sprig_worker::queue_forwarder`) and the gateways
//! (`sprig_gateway::outbox_feed`) use these names.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use sprig_cluster::ClusterRegistry;
use sprig_common::Result;
use sprig_feed::subs::Subscription;
use sprig_meta::{ListOpts, Record};

use crate::dispatch::{JobClass, NewJob, list_jobs, submit};
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler};

/// The subscription kind (and name) the gateways register.
pub const SUB_NAME: &str = "queue_forwarder";
/// The forwarder's job kind.
pub const JOB_KIND: &str = "queue_forwarder";

/// The loop's one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueueForwarderTick;

impl fmt::Display for QueueForwarderTick {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("tick")
    }
}

/// See the module docs.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueueForwarderLoop;

#[async_trait::async_trait]
impl Reconciler for QueueForwarderLoop {
    type Key = QueueForwarderTick;

    fn name(&self) -> &'static str {
        "queue_forwarder"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<QueueForwarderTick> {
        Vec::new()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<QueueForwarderTick>> {
        Ok(vec![QueueForwarderTick])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &QueueForwarderTick) -> Result<Outcome> {
        let l = cx.leader()?;
        let mut live = HashSet::new();
        for j in &list_jobs(cx.dc.as_ref()).await? {
            let job = j.job();
            if job.state.is_final() || job.kind != JOB_KIND {
                continue;
            }
            if let Some(c) = job.spec["cluster"].as_str() {
                live.insert(c.to_owned());
            }
        }
        let prefix = cx.dc.tier().key("feed/subs/")?;
        let page = cx
            .dc
            .list(
                &prefix,
                ListOpts {
                    limit: 100_000,
                    ..ListOpts::default()
                },
            )
            .await?;
        let any = page.kvs.iter().any(|kv| {
            Record::<Subscription>::decode(&kv.value)
                .is_ok_and(|r| r.body.kind == SUB_NAME && r.body.state == "active")
        });
        if !any {
            return Ok(Outcome::RequeueAfter(Duration::from_secs(2)));
        }
        let registry = ClusterRegistry::new(Arc::clone(&cx.global), Arc::clone(&cx.dc))?;
        for c in registry.list().await? {
            let id = c.body().cluster_id.to_string();
            if live.contains(&id) {
                continue;
            }
            let job = submit(
                l,
                cx.clock.as_ref(),
                NewJob {
                    kind: JOB_KIND.into(),
                    // The consumers' pool: one job per cluster, for as
                    // long as any tenant has queue or cross-tenant outboxes.
                    class: JobClass::Consumer,
                    tenant_id: None,
                    clusters: Vec::new(),
                    priority: 0,
                    cause: "queue_forwarder".into(),
                    zone: None,
                    spec: json!({"cluster": id}),
                },
            )
            .await?;
            tracing::info!(cluster = %id, job = %job, "queue forwarder job submitted");
            live.insert(id);
        }
        Ok(Outcome::RequeueAfter(Duration::from_secs(2)))
    }
}
