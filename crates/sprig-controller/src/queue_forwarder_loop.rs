//! The `queue_forwarder` loop (chapter 50, "Publishing inside a database
//! transaction"; R9, r9fwd), on the DC leader, every two seconds:
//!
//! ```text
//! each active feed subscription of kind `queue_forwarder` (one per tenant,
//!   registered by a gateway when it first opens a queue store or makes a
//!   cross-tenant outbox for the tenant) without a live `queue_forwarder`
//!   job → submit one (spec: tenant); a job whose worker died is requeued
//!   by dispatch, and a failed or finished one is replaced here, so each
//!   tenant has one forwarder
//! ```
//!
//! The worker's job (`sprig_worker::queue_forwarder`) and the gateways
//! (`sprig_gateway::outbox_feed`) use these names.

use std::collections::HashSet;
use std::fmt;
use std::time::Duration;

use serde_json::json;
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
            if let Some(t) = job.spec["tenant"].as_u64() {
                live.insert(t);
            }
        }
        let prefix = cx.dc.tier().key("feed/subs/")?;
        let page = cx
            .dc
            .list(
                &prefix,
                ListOpts {
                    limit: 10_000,
                    ..ListOpts::default()
                },
            )
            .await?;
        for kv in &page.kvs {
            let Ok(r) = Record::<Subscription>::decode(&kv.value) else {
                continue;
            };
            let s = r.body;
            if s.kind != SUB_NAME || s.state != "active" || live.contains(&s.tenant.0) {
                continue;
            }
            let id = submit(
                l,
                cx.clock.as_ref(),
                NewJob {
                    kind: JOB_KIND.into(),
                    // The consumers' pool: one job per tenant, for as
                    // long as it has queue or cross-tenant outboxes.
                    class: JobClass::Consumer,
                    tenant_id: None,
                    clusters: Vec::new(),
                    priority: 0,
                    cause: "queue_forwarder".into(),
                    zone: None,
                    spec: json!({"tenant": s.tenant.0}),
                },
            )
            .await?;
            live.insert(s.tenant.0);
            tracing::info!(tenant = %s.tenant, job = %id, "queue forwarder job submitted");
        }
        Ok(Outcome::RequeueAfter(Duration::from_secs(2)))
    }
}
