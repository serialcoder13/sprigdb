//! The `change_exchange` loop (a change feed to an exchange), on the DC
//! leader, every two seconds:
//!
//! ```text
//! each active feed subscription of kind `change_exchange` (one per table
//!   with queue.change_exchange set, registered by the gateway that ran the
//!   setting) without a live `change_exchange` job → submit one (spec:
//!   tenant, subscription); a job whose worker died is requeued by
//!   dispatch, and a failed or finished one is replaced here
//! ```
//!
//! The names match the gateway's `exec::change_exchange::KIND` and the
//! worker's handler.

use std::collections::HashSet;
use std::fmt;
use std::time::Duration;

use serde_json::json;
use sprig_common::Result;
use sprig_feed::subs::Subscription;
use sprig_meta::{ListOpts, Record};

use crate::dispatch::{JobClass, NewJob, list_jobs, submit};
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler};

/// The subscription kind the gateways register.
pub const SUB_KIND: &str = "change_exchange";
/// The forwarder's job kind.
pub const JOB_KIND: &str = "change_exchange";

/// The loop's one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChangeExchangeTick;

impl fmt::Display for ChangeExchangeTick {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("tick")
    }
}

/// See the module docs.
#[derive(Debug, Clone, Copy, Default)]
pub struct ChangeExchangeLoop;

#[async_trait::async_trait]
impl Reconciler for ChangeExchangeLoop {
    type Key = ChangeExchangeTick;

    fn name(&self) -> &'static str {
        "change_exchange"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<ChangeExchangeTick> {
        Vec::new()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<ChangeExchangeTick>> {
        Ok(vec![ChangeExchangeTick])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &ChangeExchangeTick) -> Result<Outcome> {
        let l = cx.leader()?;
        let mut live = HashSet::new();
        for j in &list_jobs(cx.dc.as_ref()).await? {
            let job = j.job();
            if job.state.is_final() || job.kind != JOB_KIND {
                continue;
            }
            if let (Some(t), Some(n)) = (job.spec["tenant"].as_u64(), job.spec["name"].as_str()) {
                live.insert((t, n.to_owned()));
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
            let k = (s.tenant.0, s.name.clone());
            if s.kind != SUB_KIND || s.state != "active" || live.contains(&k) {
                continue;
            }
            let id = submit(
                l,
                cx.clock.as_ref(),
                NewJob {
                    kind: JOB_KIND.into(),
                    // One long-running job per table with an exchange.
                    class: JobClass::Consumer,
                    tenant_id: None,
                    clusters: Vec::new(),
                    priority: 0,
                    cause: "change_exchange".into(),
                    zone: None,
                    spec: json!({"tenant": s.tenant.0, "name": s.name}),
                },
            )
            .await?;
            tracing::info!(tenant = %s.tenant, sub = %s.name, job = %id, "change exchange forwarder job submitted");
            live.insert(k);
        }
        Ok(Outcome::RequeueAfter(Duration::from_secs(2)))
    }
}
