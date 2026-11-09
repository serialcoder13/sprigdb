//! The `streams` loop (chapter 45, "Sequencing and offsets"; R9), on the DC
//! leader, every second:
//!
//! ```text
//! each active feed subscription of kind `stream` (stream:<id>) without a live
//!   `stream_sequencer` job → submit one (spec: tenant, stream); a job whose
//!   worker died is requeued by dispatch, and a failed or finished one is
//!   replaced here, so each stream has one sequencer
//! ```
//!
//! The leader epoch in the stream's checkpoint fences a sequencer that
//! hasn't noticed it lost its job (`sprig_streams::sequencer`). Jobs carry
//! their tenant in the spec, as logic jobs do, so a long-running sequencer
//! doesn't count as the tenant's running job for dispatch's per-tenant
//! rules.

use std::collections::HashSet;
use std::fmt;
use std::time::Duration;

use serde_json::json;
use sprig_common::Result;
use sprig_feed::subs::Subscription;
use sprig_meta::{ListOpts, Record};
use sprig_streams::runtime::{JOB_KIND, SUB_KIND, stream_of};

use crate::dispatch::{JobClass, NewJob, list_jobs, submit};
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler};

/// The loop's one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamsTick;

impl fmt::Display for StreamsTick {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("tick")
    }
}

/// See the module docs.
#[derive(Debug, Clone, Copy, Default)]
pub struct StreamsLoop;

#[async_trait::async_trait]
impl Reconciler for StreamsLoop {
    type Key = StreamsTick;

    fn name(&self) -> &'static str {
        "streams"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<StreamsTick> {
        Vec::new()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<StreamsTick>> {
        Ok(vec![StreamsTick])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &StreamsTick) -> Result<Outcome> {
        let l = cx.leader()?;
        let mut live = HashSet::new();
        for j in &list_jobs(cx.dc.as_ref()).await? {
            let job = j.job();
            if job.state.is_final() || job.kind != JOB_KIND {
                continue;
            }
            if let (Some(t), Some(s)) = (job.spec["tenant"].as_u64(), job.spec["stream"].as_u64()) {
                live.insert((t, s));
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
            if s.kind != SUB_KIND || s.state != "active" {
                continue;
            }
            let Some(stream) = stream_of(&s.name) else {
                continue;
            };
            if live.contains(&(s.tenant.0, stream.0)) {
                continue;
            }
            let id = submit(
                l,
                cx.clock.as_ref(),
                NewJob {
                    kind: JOB_KIND.into(),
                    // Their own pool: one job per stream, for as long as
                    // it exists (r9triggers).
                    class: JobClass::Consumer,
                    tenant_id: None,
                    clusters: Vec::new(),
                    priority: 0,
                    cause: "streams".into(),
                    zone: None,
                    spec: json!({"tenant": s.tenant.0, "stream": stream.0}),
                },
            )
            .await?;
            tracing::info!(tenant = %s.tenant, stream = stream.0, job = %id, "stream sequencer job submitted");
        }
        Ok(Outcome::RequeueAfter(Duration::from_secs(1)))
    }
}
