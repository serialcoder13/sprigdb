//! The `logic` loop (chapter 41, "After-commit triggers" and "Scheduled
//! jobs"; R7 order 5), on the DC leader, every second:
//!
//! ```text
//! triggers: each active feed subscription of kind `logic` (logic:b<binding>)
//!   without a live `logic_trigger` job → submit one (spec: tenant, binding);
//!   a job whose worker died is requeued by dispatch, and a failed one is
//!   replaced here, so each binding has one consumer
//! schedules: each index entry logic/schedules/<tenant>/<module> due now →
//!   sprig_logic::schedule::tick on the tenant's catalog (the
//!   compare-and-set); Fire(run) → submit `logic_schedule` (spec: tenant,
//!   module, schema, run); the index follows next_fire_at
//! ```
//!
//! triggers on streams and queues (R9): each index entry
//!   logic/msg/<tenant>/<binding> without a live `logic_stream_trigger` or
//!   `logic_queue_trigger` job → submit one (spec: tenant, binding), in the
//!   `consumer` job class; the job removes the entry when its stream or
//!   queue is gone
//!
//! Logic jobs carry their tenant in the spec, not in `tenant_id`, so a
//! long-running trigger doesn't count as the tenant's running job for
//! dispatch's per-tenant rules (exclusive kinds, `control.tenant_jobs_per_hour`).

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use sprig_catalog::CatalogLocator;
use sprig_common::Result;
use sprig_feed::subs::Subscription;
use sprig_logic::LogicStore;
use sprig_logic::schedule::{self, Tick};
use sprig_logic::triggers;
use sprig_meta::{ListOpts, Record};

use crate::dispatch::{JobClass, NewJob, list_jobs, submit};
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler};

/// The trigger job kind.
pub const TRIGGER_KIND: &str = "logic_trigger";

/// The loop's one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LogicTick;

impl fmt::Display for LogicTick {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("tick")
    }
}

/// See the module docs.
pub struct LogicLoop {
    locator: Arc<dyn CatalogLocator>,
    /// `logic.schedule_misfire_seconds`.
    pub misfire: Duration,
}

impl fmt::Debug for LogicLoop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogicLoop").finish_non_exhaustive()
    }
}

impl LogicLoop {
    /// A loop reaching tenant catalogs through `locator`.
    #[must_use]
    pub fn new(locator: Arc<dyn CatalogLocator>) -> Self {
        Self {
            locator,
            misfire: Duration::from_secs(300),
        }
    }

    async fn triggers(&self, cx: &LoopCx, live: &HashSet<(u64, u64)>) -> Result<()> {
        let l = cx.leader()?;
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
            if s.kind != "logic" || s.state != "active" {
                continue;
            }
            let Some(binding) = triggers::binding_of(&s.name) else {
                continue;
            };
            if live.contains(&(s.tenant.0, binding)) {
                continue;
            }
            let id = submit(
                l,
                cx.clock.as_ref(),
                NewJob {
                    kind: TRIGGER_KIND.into(),
                    class: JobClass::Other,
                    tenant_id: None,
                    clusters: Vec::new(),
                    priority: 0,
                    cause: "logic".into(),
                    zone: None,
                    spec: json!({"tenant": s.tenant.0, "binding": binding}),
                },
            )
            .await?;
            tracing::info!(tenant = %s.tenant, binding, job = %id, "after-commit trigger job submitted");
        }
        Ok(())
    }

    /// Triggers on streams and queues (see the module docs).
    async fn message_triggers(&self, cx: &LoopCx, live: &HashSet<(u64, u64)>) -> Result<()> {
        let l = cx.leader()?;
        for e in sprig_logic::messages::list_index(cx.dc.as_ref()).await? {
            if live.contains(&(e.tenant.0, e.binding)) {
                continue;
            }
            let id = submit(
                l,
                cx.clock.as_ref(),
                NewJob {
                    kind: e.job_kind().into(),
                    class: JobClass::Consumer,
                    tenant_id: None,
                    clusters: Vec::new(),
                    priority: 0,
                    cause: "logic".into(),
                    zone: None,
                    spec: json!({"tenant": e.tenant.0, "binding": e.binding}),
                },
            )
            .await?;
            tracing::info!(tenant = %e.tenant, binding = e.binding, kind = e.job_kind(), job = %id, "trigger on a stream or queue: job submitted");
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)] // one pass over the index, each case inline
    async fn schedules(&self, cx: &LoopCx, running: &HashSet<String>) -> Result<()> {
        let l = cx.leader()?;
        let now = u64::try_from(
            cx.clock
                .wall()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX);
        for e in schedule::list_index(cx.dc.as_ref()).await? {
            if e.next_fire_ms > now {
                continue;
            }
            let ep = match self.locator.writer(e.tenant).await {
                Ok(ep) => ep,
                Err(err) => {
                    tracing::warn!(tenant = %e.tenant, error = %err, "schedule's catalog unreachable");
                    continue;
                }
            };
            let Ok(mut c) = ep.pool.get_conn().await else {
                continue;
            };
            let store = LogicStore::new(ep.store.schema());
            let (tick, next) = match schedule::tick(
                &mut c,
                &store,
                e.module_id,
                now,
                self.misfire,
                |hex| running.contains(hex),
            )
            .await
            {
                Ok(t) => t,
                Err(err) => {
                    tracing::warn!(tenant = %e.tenant, module = e.module_id, error = %err, "schedule tick failed");
                    continue;
                }
            };
            match &tick {
                Tick::Fire(run) => {
                    let r = submit(
                        l,
                        cx.clock.as_ref(),
                        NewJob {
                            kind: schedule::JOB_KIND.into(),
                            class: JobClass::Other,
                            tenant_id: None,
                            clusters: Vec::new(),
                            priority: 10,
                            cause: "logic".into(),
                            zone: None,
                            spec: json!({
                                "tenant": e.tenant.0, "module": e.module_id,
                                "schema": e.schema, "run": run.to_string(),
                            }),
                        },
                    )
                    .await;
                    match r {
                        Ok(job) => {
                            tracing::info!(tenant = %e.tenant, module = e.module_id, %run, %job, "schedule fired");
                        }
                        Err(err) => {
                            let _ = store
                                .run_ended(&mut c, e.module_id, &run.to_bytes(), "error")
                                .await;
                            return Err(err);
                        }
                    }
                }
                Tick::Skipped | Tick::Misfired => {
                    tracing::info!(tenant = %e.tenant, module = e.module_id, ?tick, "schedule passed");
                    let _ = store
                        .log(
                            &mut c,
                            e.module_id,
                            None,
                            "warn",
                            "schedule",
                            &format!("fire time passed: {tick:?}"),
                        )
                        .await;
                }
                Tick::Nothing | Tick::Running => {}
            }
            match next {
                Some(n) if n != e.next_fire_ms => {
                    let mut e2 = e.clone();
                    e2.next_fire_ms = n;
                    schedule::put_index(cx.dc.as_ref(), &e2).await?;
                }
                // Gone from the catalog: drop the entry by moving it far out.
                None => {
                    let mut e2 = e.clone();
                    e2.next_fire_ms = u64::MAX / 2;
                    schedule::put_index(cx.dc.as_ref(), &e2).await?;
                }
                Some(_) => {}
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Reconciler for LogicLoop {
    type Key = LogicTick;

    fn name(&self) -> &'static str {
        "logic"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<LogicTick> {
        Vec::new()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<LogicTick>> {
        Ok(vec![LogicTick])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &LogicTick) -> Result<Outcome> {
        let jobs = list_jobs(cx.dc.as_ref()).await?;
        let mut live_triggers = HashSet::new();
        let mut live_msg = HashSet::new();
        let mut running = HashSet::new();
        for j in &jobs {
            let job = j.job();
            if job.state.is_final() {
                continue;
            }
            let spec = &job.spec;
            if job.kind == TRIGGER_KIND
                && let (Some(t), Some(b)) = (spec["tenant"].as_u64(), spec["binding"].as_u64())
            {
                live_triggers.insert((t, b));
            }
            if (job.kind == sprig_logic::messages::STREAM_JOB_KIND
                || job.kind == sprig_logic::messages::QUEUE_JOB_KIND)
                && let (Some(t), Some(b)) = (spec["tenant"].as_u64(), spec["binding"].as_u64())
            {
                live_msg.insert((t, b));
            }
            if job.kind == schedule::JOB_KIND
                && let Some(r) = spec["run"].as_str()
            {
                running.insert(r.to_owned());
            }
        }
        if let Err(e) = self.triggers(cx, &live_triggers).await {
            tracing::warn!(error = %e, "logic loop: triggers");
        }
        if let Err(e) = self.message_triggers(cx, &live_msg).await {
            tracing::warn!(error = %e, "logic loop: triggers on streams and queues");
        }
        if let Err(e) = self.schedules(cx, &running).await {
            tracing::warn!(error = %e, "logic loop: schedules");
        }
        Ok(Outcome::RequeueAfter(Duration::from_secs(1)))
    }
}
