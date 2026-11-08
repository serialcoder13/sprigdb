//! The job runtime a worker runs (chapter 10, "Jobs and workers"; build
//! step 3): scan the DC tier's jobs, claim runnable ones nobody owns, run
//! each in its own task, and stop them all if the worker's lease is lost.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sprig_common::ids::JobId;
use sprig_common::time::Clock;
use sprig_common::{CancellationToken, ErrorKind, Result, SprigCode};
use sprig_meta::{Session, SessionOpts};
use tokio::task::JoinHandle;

use crate::engine::Engine;
use crate::io::JobEnv;
use crate::record::{JobRecord, JobStore};
use crate::settings::MoverSettings;

/// Builds a job's environment: connections to its source and target, and
/// its tenant's map.
#[async_trait::async_trait]
pub trait EnvFactory: Send + Sync {
    /// The environment for `rec`.
    async fn env(&self, rec: &JobRecord) -> Result<JobEnv>;
    /// The settings for `rec` (its tenant and source cluster).
    fn settings(&self, rec: &JobRecord) -> MoverSettings;
}

/// Engine tasks, aborted when dropped.
struct Running(HashMap<JobId, JoinHandle<()>>);

impl Drop for Running {
    fn drop(&mut self) {
        for h in self.0.values() {
            h.abort();
        }
    }
}

/// How a runtime behaves.
#[derive(Debug, Clone)]
pub struct WorkerOpts {
    /// This worker's id, recorded as the owner.
    pub worker_id: String,
    /// Jobs run at once.
    pub max_jobs: usize,
    /// How often jobs are scanned.
    pub scan_interval: Duration,
    /// `mover.job_lease_seconds`.
    pub lease: Duration,
}

impl WorkerOpts {
    /// Defaults for `worker_id`.
    #[must_use]
    pub fn new(worker_id: &str) -> Self {
        Self {
            worker_id: worker_id.to_owned(),
            max_jobs: 4,
            scan_interval: Duration::from_secs(1),
            lease: MoverSettings::default().job_lease,
        }
    }
}

/// The runtime.
pub struct JobRuntime {
    store: JobStore,
    factory: Arc<dyn EnvFactory>,
    opts: WorkerOpts,
}

impl std::fmt::Debug for JobRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobRuntime")
            .field("worker", &self.opts.worker_id)
            .finish_non_exhaustive()
    }
}

impl JobRuntime {
    /// A runtime over `store`.
    #[must_use]
    pub fn new(store: JobStore, factory: Arc<dyn EnvFactory>, opts: WorkerOpts) -> Self {
        Self {
            store,
            factory,
            opts,
        }
    }

    fn clock(&self) -> Arc<dyn Clock> {
        self.store.clock().clone()
    }

    /// Runs until `shutdown`; reopens the lease when it is lost.
    ///
    /// # Errors
    /// None in practice: store failures are retried.
    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            let session = match Session::open(
                self.store.meta().clone(),
                self.clock(),
                SessionOpts::new(self.opts.lease),
            )
            .await
            {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = %e, "worker can't open its lease; retrying");
                    tokio::select! {
                        biased;
                        () = shutdown.cancelled() => return Ok(()),
                        () = tokio::time::sleep(Duration::from_secs(1)) => continue,
                    }
                }
            };
            self.serve(&session, &shutdown).await;
            let _ = session.close().await;
        }
    }

    /// Claims and runs jobs while the session lives. Every engine stops
    /// when the lease is lost, at shutdown, and when this future is dropped
    /// (a crash takes the jobs with it).
    async fn serve(&self, session: &Session, shutdown: &CancellationToken) {
        let mut running = Running(HashMap::new());
        loop {
            running.0.retain(|_, h| !h.is_finished());
            if session.is_lost() || shutdown.is_cancelled() {
                break;
            }
            if running.0.len() < self.opts.max_jobs
                && let Err(e) = self.claim_some(session, &mut running.0).await
            {
                tracing::warn!(error = %e, "job scan failed");
            }
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                () = session.lost() => {
                    tracing::warn!(worker = %self.opts.worker_id, "lease lost; stopping jobs");
                    break;
                }
                () = tokio::time::sleep(self.opts.scan_interval) => {}
            }
        }
    }

    async fn claim_some(
        &self,
        session: &Session,
        running: &mut HashMap<JobId, JoinHandle<()>>,
    ) -> Result<()> {
        for (rec, rev) in self.store.all().await? {
            if running.len() >= self.opts.max_jobs {
                break;
            }
            if !rec.state.is_runnable() || running.contains_key(&rec.job_id) {
                continue;
            }
            if self.store.owner(&rec.job_id).await?.is_some() {
                continue;
            }
            let Some(claim) = self
                .store
                .claim(&rec, rev, &self.opts.worker_id, session.lease())
                .await?
            else {
                continue;
            };
            let claim = claim.with_session(session.clone());
            let id = rec.job_id;
            tracing::info!(job = %id.0, token = claim.token(), worker = %self.opts.worker_id, "job claimed");
            let factory = self.factory.clone();
            let clock = self.clock();
            let h = tokio::spawn(async move {
                let settings = factory.settings(&claim.rec);
                let env = match factory.env(&claim.rec).await {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!(job = %id.0, error = %e, "can't reach the job's clusters");
                        let _ = claim.release().await;
                        return;
                    }
                };
                let releaser = claim.clone();
                match Engine::new(claim, env, clock, settings).run().await {
                    Ok(state) => tracing::info!(job = %id.0, %state, "job stopped"),
                    Err(e) if matches!(e.kind, ErrorKind::Sprig(SprigCode::JobLeaseLost)) => {
                        tracing::warn!(job = %id.0, "job lost to another worker");
                    }
                    Err(e) => {
                        tracing::warn!(job = %id.0, error = %e, "job failed; giving it back");
                        let _ = releaser.release().await;
                    }
                }
            });
            running.insert(id, h);
        }
        Ok(())
    }
}
