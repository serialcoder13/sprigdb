//! The `graph_mirror` loop (graph mirrors and their freshness), on the DC
//! leader, every two seconds:
//!
//! ```text
//! each active feed subscription of kind `graph_mirror` (one per tenant,
//!   registered by a gateway when a graph of the tenant wants `async`
//!   mirrors) without a live `graph_mirror` job → submit one (spec:
//!   tenant); a job whose worker died is requeued by dispatch, and a
//!   failed or finished one is replaced here, so each tenant has one
//!   applier
//! ```
//!
//! The names match `sprig_graph::mirror::{SUB_NAME, JOB_KIND}` (the
//! controller doesn't link the graph crate).

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
pub const SUB_KIND: &str = "graph_mirror";
/// The applier's job kind.
pub const JOB_KIND: &str = "graph_mirror";

/// The loop's one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GraphMirrorTick;

impl fmt::Display for GraphMirrorTick {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("tick")
    }
}

/// See the module docs.
#[derive(Debug, Clone, Copy, Default)]
pub struct GraphMirrorLoop;

#[async_trait::async_trait]
impl Reconciler for GraphMirrorLoop {
    type Key = GraphMirrorTick;

    fn name(&self) -> &'static str {
        "graph_mirror"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<GraphMirrorTick> {
        Vec::new()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<GraphMirrorTick>> {
        Ok(vec![GraphMirrorTick])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &GraphMirrorTick) -> Result<Outcome> {
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
            if s.kind != SUB_KIND || s.state != "active" || live.contains(&s.tenant.0) {
                continue;
            }
            let id = submit(
                l,
                cx.clock.as_ref(),
                NewJob {
                    kind: JOB_KIND.into(),
                    // Their own pool: one job per tenant, for as long as it
                    // has asynchronous mirrors.
                    class: JobClass::Consumer,
                    tenant_id: None,
                    clusters: Vec::new(),
                    priority: 0,
                    cause: "graph_mirror".into(),
                    zone: None,
                    spec: json!({"tenant": s.tenant.0}),
                },
            )
            .await?;
            live.insert(s.tenant.0);
            tracing::info!(tenant = %s.tenant, job = %id, "graph mirror applier job submitted");
        }
        Ok(Outcome::RequeueAfter(Duration::from_secs(2)))
    }
}
