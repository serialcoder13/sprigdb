//! The job runtime (chapter 23 step 6, the worker's half of "One job
//! scheduler for every loop").
//!
//! ```text
//! run():
//!   session = lease (gateway-style keep-alive); register workers/<id> with slots
//!   every second and on each jobs/ change: jobs assigned to me and not claimed
//!     claim (assigned -> running, compared on the revision seen)
//!     run the kind's handler in its own task with a cancel token
//!   handler.progress(v): write compared on the job's revision; Lost -> cancel
//!   handler returns: finish (succeeded with progress, or failed with the reason)
//!   session lost: cancel every running job; register again
//! ```
//!
//! A job of a kind this worker has no handler for is failed at once with
//! the reason, so it never waits for a worker that can't run it.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use sprig_common::ids::JobId;
use sprig_common::time::Clock;
use sprig_common::{CancellationToken, Result};
use sprig_controller::dispatch::{
    self, Claimed, JobClass, Ownership, Registration, RegistrationKind,
};
use sprig_meta::session::SessionOpts;
use sprig_meta::{MetaStore, Session};
use tokio::sync::Mutex as AsyncMutex;

/// How often assigned jobs are looked for.
pub const POLL: Duration = Duration::from_secs(1);
/// The registration lease.
pub const SESSION_TTL: Duration = Duration::from_secs(10);

/// What a running job sees.
pub struct JobCtx {
    store: Arc<dyn MetaStore>,
    claimed: Arc<AsyncMutex<Claimed>>,
    /// Fired when the job must stop: it lost ownership, was cancelled, or
    /// the worker's session ended. Handlers check it between steps.
    pub cancel: CancellationToken,
    /// The job's spec.
    pub spec: Value,
    /// Its progress when claimed (a resumed job's checkpoint).
    pub progress: Value,
    /// The job id.
    pub job_id: JobId,
}

impl std::fmt::Debug for JobCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobCtx")
            .field("job_id", &self.job_id)
            .finish_non_exhaustive()
    }
}

impl JobCtx {
    /// The job's attempt (its owner's `attempt`, 0 if unassigned): it
    /// rises with every reassignment, so it fences a worker that lost the
    /// job but still runs (chapter 42's copy worker uses it as its epoch).
    pub async fn attempt(&self) -> u32 {
        self.claimed
            .lock()
            .await
            .job
            .job()
            .owner
            .as_ref()
            .map_or(0, |o| o.attempt)
    }

    /// Writes progress, compared on the job's revision. `false` (and the
    /// cancel token fired) when the job is no longer this worker's.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn progress(&self, v: Value) -> Result<bool> {
        let mut c = self.claimed.lock().await;
        match dispatch::report_progress(self.store.as_ref(), &mut c, v).await? {
            Ownership::Held => Ok(true),
            Ownership::Lost => {
                self.cancel.cancel();
                Ok(false)
            }
        }
    }
}

/// Runs the jobs of one kind.
#[async_trait]
pub trait JobHandler: Send + Sync + 'static {
    /// The kind (`copy`, `index_build`, ...).
    fn kind(&self) -> &'static str;
    /// The slot class it runs in.
    fn class(&self) -> JobClass;
    /// Runs the job; `Ok(progress)` succeeds, `Err(reason)` fails. Must
    /// return soon after `cx.cancel` fires.
    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String>;
}

/// `noop`: sleeps `spec.sleep_ms` in steps of 100 ms, reporting progress;
/// for tests and smoke checks of the scheduler.
#[derive(Debug, Clone, Copy, Default)]
pub struct Noop;

#[async_trait]
impl JobHandler for Noop {
    fn kind(&self) -> &'static str {
        "noop"
    }

    fn class(&self) -> JobClass {
        JobClass::Other
    }

    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let total = cx.spec.get("sleep_ms").and_then(Value::as_u64).unwrap_or(0);
        let mut done = 0;
        while done < total {
            let step = (total - done).min(100);
            tokio::select! {
                () = cx.cancel.cancelled() => return Err("cancelled".into()),
                () = tokio::time::sleep(Duration::from_millis(step)) => {}
            }
            done += step;
            if !cx
                .progress(json!({ "slept_ms": done }))
                .await
                .map_err(|e| e.to_string())?
            {
                return Err("lost the job".into());
            }
        }
        if cx
            .spec
            .get("fail")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Err("asked to fail".into());
        }
        Ok(json!({ "slept_ms": done }))
    }
}

/// One worker's runtime.
pub struct WorkerRuntime {
    store: Arc<dyn MetaStore>,
    clock: Arc<dyn Clock>,
    id: String,
    zone: String,
    handlers: HashMap<&'static str, Arc<dyn JobHandler>>,
    slots: BTreeMap<JobClass, u32>,
    running: Arc<Mutex<HashSet<JobId>>>,
}

impl std::fmt::Debug for WorkerRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerRuntime")
            .field("id", &self.id)
            .field("kinds", &self.handlers.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl WorkerRuntime {
    /// A runtime for worker `id` in `store`'s DC tier, with no handlers.
    #[must_use]
    pub fn new(store: Arc<dyn MetaStore>, clock: Arc<dyn Clock>, id: String) -> Self {
        Self {
            store,
            clock,
            id,
            zone: String::new(),
            handlers: HashMap::new(),
            slots: BTreeMap::from([
                (JobClass::Copy, 2),
                (JobClass::Build, 2),
                (JobClass::Recovery, 1),
                (JobClass::Other, 8),
                (JobClass::Consumer, dispatch::CONSUMER_SLOTS),
            ]),
            running: Arc::default(),
        }
    }

    /// Adds the handler of one kind.
    #[must_use]
    pub fn with_handler(mut self, h: Arc<dyn JobHandler>) -> Self {
        self.handlers.insert(h.kind(), h);
        self
    }

    /// Sets the zone the dispatcher prefers for this worker.
    #[must_use]
    pub fn with_zone(mut self, zone: &str) -> Self {
        zone.clone_into(&mut self.zone);
        self
    }

    /// Adds slots of classes the defaults leave out (the search role's).
    #[must_use]
    pub fn with_extra_slots(mut self, slots: &[(JobClass, u32)]) -> Self {
        for (c, n) in slots {
            self.slots.insert(*c, *n);
        }
        self
    }

    /// Sets the slots per class this worker offers.
    #[must_use]
    pub fn with_slots(mut self, slots: BTreeMap<JobClass, u32>) -> Self {
        self.slots = slots;
        self
    }

    /// Jobs running now.
    #[must_use]
    pub fn running(&self) -> usize {
        self.running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Registers, then claims and runs jobs until `stop`, registering again
    /// after a lost session.
    ///
    /// # Errors
    /// None: store failures are logged and retried.
    pub async fn run(&self, stop: CancellationToken) -> Result<()> {
        loop {
            let session = tokio::select! {
                () = stop.cancelled() => return Ok(()),
                r = self.register() => r,
            };
            let session = match session {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = %e, "worker registration failed; retrying");
                    tokio::select! {
                        () = stop.cancelled() => return Ok(()),
                        () = tokio::time::sleep(POLL) => continue,
                    }
                }
            };
            let term = stop.child_token();
            let lost = self.serve(&session, &term).await;
            term.cancel();
            // Running jobs see `term` and stop; wait for them.
            while self.running() > 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            if !lost {
                let _ = session.close().await;
                return Ok(());
            }
            tracing::warn!(worker = %self.id, "worker session lost: running jobs stopped; registering again");
        }
    }

    async fn register(&self) -> Result<Session> {
        let session = Session::open(
            Arc::clone(&self.store),
            Arc::clone(&self.clock),
            SessionOpts::new(SESSION_TTL),
        )
        .await?;
        let reg = Registration {
            address: gethostname::gethostname().to_string_lossy().into_owned(),
            zone: self.zone.clone(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            load: json!({ "running": self.running() }),
            slots: self.slots.clone(),
        };
        dispatch::register(&session, RegistrationKind::Worker, &self.id, &reg).await?;
        tracing::info!(worker = %self.id, "worker registered");
        Ok(session)
    }

    /// Claims and runs until the session is lost (`true`) or `term` fires.
    async fn serve(&self, session: &Session, term: &CancellationToken) -> bool {
        let mut tick = tokio::time::interval(POLL);
        loop {
            tokio::select! {
                () = term.cancelled() => return false,
                () = session.lost() => return true,
                _ = tick.tick() => {}
            }
            // Jobs that are `running` under this worker's id but not running
            // here: a worker restarted under its old id before its lease ran
            // out, or a progress write that timed out but committed (the
            // next write then fails its revision check and the job stops
            // here, though the record still names this worker). Dispatch
            // sees their owner alive and never requeues them, so they resume
            // here, from their checkpoint.
            match dispatch::list_jobs(self.store.as_ref()).await {
                Ok(jobs) => {
                    for j in jobs {
                        let mine = j.job().state == dispatch::JobState::Running
                            && j.job()
                                .owner
                                .as_ref()
                                .is_some_and(|o| o.worker_id == self.id);
                        let local = self
                            .running
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .contains(&j.job().job_id);
                        if mine && !local {
                            tracing::info!(job = %j.job().job_id, kind = %j.job().kind, "resuming a job this worker owns but isn't running");
                            self.start(Claimed { job: j }, term.child_token());
                        }
                    }
                }
                Err(e) => tracing::warn!(error = %e, "can't list jobs to resume"),
            }
            let assigned = match dispatch::assigned_to(self.store.as_ref(), &self.id).await {
                Ok(a) => a,
                Err(e) => {
                    tracing::warn!(error = %e, "can't list assigned jobs");
                    continue;
                }
            };
            for seen in assigned {
                let id = seen.job().job_id;
                if self
                    .running
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .contains(&id)
                {
                    continue;
                }
                let claimed = match dispatch::claim(self.store.as_ref(), &self.id, seen).await {
                    Ok(Some(c)) => c,
                    Ok(None) => continue,
                    Err(e) => {
                        tracing::warn!(job = %id, error = %e, "claim failed");
                        continue;
                    }
                };
                self.start(claimed, term.child_token());
            }
        }
    }

    fn start(&self, claimed: Claimed, cancel: CancellationToken) {
        let job = claimed.job.job().clone();
        let handler = self.handlers.get(job.kind.as_str()).cloned();
        let store = Arc::clone(&self.store);
        let clock = Arc::clone(&self.clock);
        let running = Arc::clone(&self.running);
        running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(job.job_id);
        tokio::spawn(async move {
            let claimed = Arc::new(AsyncMutex::new(claimed));
            let result = match handler {
                Some(h) => {
                    let cx = JobCtx {
                        store: Arc::clone(&store),
                        claimed: Arc::clone(&claimed),
                        cancel: cancel.clone(),
                        spec: job.spec.clone(),
                        progress: job.progress.clone(),
                        job_id: job.job_id,
                    };
                    tracing::info!(job = %job.job_id, kind = %job.kind, "job started");
                    h.run(&cx).await
                }
                None => Err(format!("no worker handler for job kind {:?}", job.kind)),
            };
            if !cancel.is_cancelled() {
                let c = claimed.lock().await.clone();
                match dispatch::finish(store.as_ref(), clock.as_ref(), c, result).await {
                    Ok(Ownership::Held) => tracing::info!(job = %job.job_id, "job finished"),
                    Ok(Ownership::Lost) => {
                        tracing::info!(job = %job.job_id, "job changed before it finished; result dropped");
                    }
                    Err(e) => {
                        tracing::warn!(job = %job.job_id, error = %e, "can't record the job's end");
                    }
                }
            }
            running
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&job.job_id);
        });
    }
}

#[cfg(test)]
mod tests;
