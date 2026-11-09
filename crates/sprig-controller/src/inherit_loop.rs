//! The `inherit` loop (chapter 42, "Materialized copies"), on the DC
//! leader, every second:
//!
//! ```text
//! each active feed subscription of kind `inherit` (inherit:f<root>, one per
//!   tenant of a tree that has copies or cross-tenant shares)
//!   without a live `inherit_copy` job for that root → submit one
//!   (spec: root); a job whose worker died is requeued by dispatch, and a
//!   failed one is replaced here, so each tree has one copy worker
//! ```
//!
//! Like logic jobs, the job carries its tree in the spec, not in
//! `tenant_id`, so a long-running worker doesn't count as a tenant's
//! running job for dispatch's per-tenant rules.

use std::collections::HashSet;
use std::fmt;
use std::time::Duration;

use serde_json::json;
use sprig_common::Result;
use sprig_feed::subs::Subscription;
use sprig_inherit::subs::{JOB_KIND, KIND, root_of};
use sprig_meta::{ListOpts, Record};

use crate::dispatch::{JobClass, NewJob, list_jobs, submit};
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler};

/// The loop's one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InheritTick;

impl fmt::Display for InheritTick {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("tick")
    }
}

/// See the module docs.
#[derive(Debug, Default)]
pub struct InheritLoop;

/// The trees that want a copy worker, from the subscriptions' names.
#[must_use]
pub fn wanted(subs: &[Subscription]) -> Vec<u64> {
    let mut out: Vec<u64> = subs
        .iter()
        .filter(|s| s.kind == KIND && s.state == "active")
        .filter_map(|s| root_of(&s.name).map(|r| r.0))
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

#[async_trait::async_trait]
impl Reconciler for InheritLoop {
    type Key = InheritTick;

    fn name(&self) -> &'static str {
        "inherit"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<InheritTick> {
        Vec::new()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<InheritTick>> {
        Ok(vec![InheritTick])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &InheritTick) -> Result<Outcome> {
        let live: HashSet<u64> = list_jobs(cx.dc.as_ref())
            .await?
            .iter()
            .map(crate::dispatch::StoredJob::job)
            .filter(|j| !j.state.is_final() && j.kind == JOB_KIND)
            .filter_map(|j| j.spec["root"].as_u64())
            .collect();
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
        let subs: Vec<Subscription> = page
            .kvs
            .iter()
            .filter_map(|kv| Record::<Subscription>::decode(&kv.value).ok())
            .map(|r| r.body)
            .collect();
        let l = cx.leader()?;
        for root in wanted(&subs) {
            if live.contains(&root) {
                continue;
            }
            let id = submit(
                l,
                cx.clock.as_ref(),
                NewJob {
                    kind: JOB_KIND.into(),
                    class: JobClass::Other,
                    tenant_id: None,
                    clusters: Vec::new(),
                    priority: 0,
                    cause: "inherit".into(),
                    zone: None,
                    spec: json!({ "root": root }),
                },
            )
            .await?;
            tracing::info!(root, job = %id, "copy worker job submitted");
        }
        Ok(Outcome::RequeueAfter(Duration::from_secs(1)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_common::ids::TenantId;

    fn sub(t: u64, name: &str, kind: &str) -> Subscription {
        Subscription {
            tenant: TenantId(t),
            name: name.into(),
            kind: kind.into(),
            ordering: "merged".into(),
            owner_ref: None,
            state: "active".into(),
            created_at_ms: 0,
        }
    }

    #[test]
    fn one_job_per_tree() {
        let subs = vec![
            sub(1, "inherit:f1", "inherit"),
            sub(2, "inherit:f1", "inherit"),
            sub(9, "inherit:f9", "inherit"),
            sub(3, "logic:b4", "logic"),
        ];
        assert_eq!(wanted(&subs), vec![1, 9]);
    }
}
