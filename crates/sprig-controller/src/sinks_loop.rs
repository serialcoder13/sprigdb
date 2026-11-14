//! The `sinks` loop (task placement), on the DC leader,
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
//! Fan-in sinks: an index entry
//! with `fan_in` gets its task count from its member instances, counted
//! in the DC's `inherit-instances/` index at most every 10 s (at once for
//! a new definition version): one task per `per_task` instance
//! partitions. A change is written to the entry first; the tasks of the
//! old count (their job spec's `of`) stop at their next poll, and the new
//! ones are submitted only once none of them is live, so two tasks never
//! read one member at a time.
//!
//! Dispatch's lease decides which worker runs a task (rendezvous
//! claiming is replaced by the job runtime, as for stream
//! sequencers); the task epoch in `sink_tasks` fences a worker that lost
//! it. A task whose job ended less than 5 s ago waits, so a sink that
//! stops at once (failed, paused by a user) isn't resubmitted every second.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::json;
use sprig_common::Result;
use sprig_sinks::meta::{
    JOB_KIND, SinkIndex, fanin_regroup, fanin_tasks, index_list, index_set_tasks,
};

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
    /// Fan-in sinks counted: (tenant, sink) → (definition version, when).
    counted: Mutex<HashMap<(u64, u64), (u64, Instant)>>,
}

/// How often a fan-in sink's member instances are counted again.
const FANIN_RECOUNT: Duration = Duration::from_secs(10);

/// Member instances per definition `(tenant, object, kind)` in the DC's
/// `inherit-instances/` index.
async fn instance_counts(
    dc: &std::sync::Arc<dyn sprig_meta::MetaStore>,
) -> Result<HashMap<(u64, u64, String), u64>> {
    let prefix = dc.tier().key("inherit-instances/")?;
    let page = dc
        .list(
            &prefix,
            sprig_meta::ListOpts {
                limit: 1_000_000,
                ..sprig_meta::ListOpts::default()
            },
        )
        .await?;
    let mut out: HashMap<(u64, u64, String), u64> = HashMap::new();
    for kv in &page.kvs {
        let Ok(v) = serde_json::from_slice::<serde_json::Value>(&kv.value) else {
            continue;
        };
        let (Some(t), Some(o), Some(k)) = (
            v.pointer("/def/tenant").and_then(serde_json::Value::as_u64),
            v.pointer("/def/object").and_then(serde_json::Value::as_u64),
            v.get("kind").and_then(serde_json::Value::as_str),
        ) else {
            continue;
        };
        *out.entry((t, o, k.to_owned())).or_default() += 1;
    }
    Ok(out)
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
        let index = self.regroup(cx, index, Instant::now()).await?;
        let mut live = HashSet::new();
        // A fan-in sink's live tasks' counts (`of`), to hold new ones back
        // while tasks of another count still run.
        let mut live_of: HashMap<(u64, u64), HashSet<u64>> = HashMap::new();
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
                live_of
                    .entry((t, s))
                    .or_default()
                    .insert(job.spec["of"].as_u64().unwrap_or(1));
            }
        }
        for ix in &index {
            let of = u64::from(ix.tasks);
            if ix.fan_in.is_some()
                && live_of
                    .get(&(ix.tenant, ix.sink))
                    .is_some_and(|o| o.iter().any(|n| *n != of))
            {
                // Regrouping: the old tasks end first.
                continue;
            }
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
                        // of other jobs.
                        class: JobClass::Consumer,
                        tenant_id: None,
                        clusters: Vec::new(),
                        priority: 0,
                        cause: "sinks".into(),
                        zone: None,
                        spec: if ix.fan_in.is_some() {
                            json!({"tenant": ix.tenant, "sink": ix.sink, "task": task, "of": of})
                        } else {
                            json!({"tenant": ix.tenant, "sink": ix.sink, "task": task})
                        },
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

impl SinksLoop {
    /// Fan-in sinks' task counts (see the module docs): `index` with each
    /// fan-in entry's `tasks` as counted, changed entries written back.
    /// A fan-in sink not counted yet for its definition version is left
    /// out (no task starts before its count is known).
    async fn regroup(
        &self,
        cx: &LoopCx,
        index: Vec<SinkIndex>,
        now: Instant,
    ) -> Result<Vec<SinkIndex>> {
        let mut counted = self
            .counted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        counted.retain(|k, _| index.iter().any(|ix| (ix.tenant, ix.sink) == *k));
        let due = index.iter().any(|ix| {
            ix.fan_in.is_some()
                && counted.get(&(ix.tenant, ix.sink)).is_none_or(|(v, at)| {
                    *v != ix.definition_ver || now.duration_since(*at) >= FANIN_RECOUNT
                })
        });
        let counts = if due {
            match instance_counts(&cx.dc).await {
                Ok(c) => Some(c),
                Err(e) => {
                    tracing::warn!(error = %e, "sinks: fan-in instances not counted");
                    None
                }
            }
        } else {
            None
        };
        let mut out = Vec::with_capacity(index.len());
        for mut ix in index {
            let Some(g) = ix.fan_in.clone() else {
                out.push(ix);
                continue;
            };
            let key = (ix.tenant, ix.sink);
            if let Some(counts) = &counts
                && counted.get(&key).is_none_or(|(v, at)| {
                    *v != ix.definition_ver || now.duration_since(*at) >= FANIN_RECOUNT
                })
            {
                let n = counts
                    .get(&(g.def_tenant, g.def_object, g.kind.clone()))
                    .copied()
                    .unwrap_or(0);
                let wanted = fanin_tasks(n, g.partitions, g.per_task);
                if wanted != ix.tasks && fanin_regroup(ix.tasks, wanted) {
                    tracing::info!(
                        tenant = ix.tenant,
                        sink = ix.sink,
                        instances = n,
                        from = ix.tasks,
                        to = wanted,
                        "fan-in sink regrouped"
                    );
                    metrics::counter!("sprig_sink_fanin_regroups_total").increment(1);
                    if !index_set_tasks(&cx.dc, ix.tenant, ix.sink, ix.definition_ver, wanted)
                        .await?
                    {
                        // Changed meanwhile (ALTER SINK): counted again
                        // at the next tick, with the new entry.
                        continue;
                    }
                    ix.tasks = wanted;
                }
                counted.insert(key, (ix.definition_ver, now));
            }
            if counted
                .get(&key)
                .is_some_and(|(v, _)| *v == ix.definition_ver)
            {
                out.push(ix);
            }
        }
        *self
            .counted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = counted;
        Ok(out)
    }
}
