//! The `sinks` loop (chapter 51, "Task placement"; R9), on the DC leader,
//! every second:
//!
//! ```text
//! each sink in the DC's index (sinks/<tenant>/<sink>, written by the
//!   gateway at CREATE and ALTER SINK) and each of its tasks 0..n without a
//!   live `sink_task` job → submit one (spec: tenant, sink, task); a job
//!   whose worker died is requeued by dispatch, and a finished one (the
//!   definition changed, the sink paused or failed) is replaced here
//! ```
//!
//! Dispatch's lease decides which worker runs a task (the chapter's
//! rendezvous claiming is replaced by the job runtime, as for stream
//! sequencers); the task epoch in `sink_tasks` fences a worker that lost
//! it. A task whose job ended less than 5 s ago waits, so a sink that
//! stops at once (failed, paused by a user) isn't resubmitted every second.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::json;
use sprig_common::Result;
use sprig_sinks::meta::{JOB_KIND, index_list};

use crate::dispatch::{JobClass, NewJob, list_jobs, submit};
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler};

/// The loop's one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SinksTick;

impl fmt::Display for SinksTick {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("tick")
    }
}

/// See the module docs.
#[derive(Debug, Default)]
pub struct SinksLoop {
    ended: Mutex<HashMap<(u64, u64, u64), Instant>>,
}

#[async_trait::async_trait]
impl Reconciler for SinksLoop {
    type Key = SinksTick;

    fn name(&self) -> &'static str {
        "sinks"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<SinksTick> {
        Vec::new()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<SinksTick>> {
        Ok(vec![SinksTick])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &SinksTick) -> Result<Outcome> {
        let l = cx.leader()?;
        let index = index_list(&cx.dc).await?;
        if index.is_empty() {
            return Ok(Outcome::RequeueAfter(Duration::from_secs(1)));
        }
        let mut live = HashSet::new();
        let now = Instant::now();
        let mut ended = self
            .ended
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for j in &list_jobs(cx.dc.as_ref()).await? {
            let job = j.job();
            if job.kind != JOB_KIND {
                continue;
            }
            let (Some(t), Some(s), Some(n)) = (
                job.spec["tenant"].as_u64(),
                job.spec["sink"].as_u64(),
                job.spec["task"].as_u64(),
            ) else {
                continue;
            };
            if job.state.is_final() {
                ended.entry((t, s, n)).or_insert(now);
            } else {
                live.insert((t, s, n));
                ended.remove(&(t, s, n));
            }
        }
        for ix in &index {
            for task in 0..u64::from(ix.tasks) {
                let k = (ix.tenant, ix.sink, task);
                if live.contains(&k) {
                    continue;
                }
                if ended
                    .get(&k)
                    .is_some_and(|t| now.duration_since(*t) < Duration::from_secs(5))
                {
                    continue;
                }
                let id = submit(
                    l,
                    cx.clock.as_ref(),
                    NewJob {
                        kind: JOB_KIND.into(),
                        // Long-running and mostly waiting, like triggers
                        // on streams: the consumer pool, not the 8 slots
                        // of other jobs (R9, r9queue).
                        class: JobClass::Consumer,
                        tenant_id: None,
                        clusters: Vec::new(),
                        priority: 0,
                        cause: "sinks".into(),
                        zone: None,
                        spec: json!({"tenant": ix.tenant, "sink": ix.sink, "task": task}),
                    },
                )
                .await?;
                ended.remove(&k);
                tracing::info!(tenant = ix.tenant, sink = ix.sink, task, job = %id, "sink task job submitted");
            }
        }
        *self
            .ended
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = ended;
        Ok(Outcome::RequeueAfter(Duration::from_secs(1)))
    }
}
