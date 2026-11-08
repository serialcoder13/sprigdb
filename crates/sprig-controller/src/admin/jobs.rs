//! The `Jobs` service (chapter 37; chapter 23, "One job scheduler for every
//! loop"): `GET /admin/v1/jobs`, `GET /admin/v1/jobs/{job_id}` and
//! `POST /admin/v1/jobs/{job_id}:cancel`. Reads are answered by any
//! controller; a cancel only by the DC leader, under its fencing compare.

use serde::Serialize;
use sprig_common::ids::JobId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Cmp, Op, Revision};

use super::{AdminService, invalid};
use crate::dispatch::{self, Job, JobState};

/// One job as the Admin API shows it.
#[derive(Debug, Clone, Serialize)]
pub struct JobView {
    /// The record.
    #[serde(flatten)]
    pub job: Job,
    /// Its mod revision.
    pub mod_revision: Revision,
}

/// `GET /admin/v1/jobs`.
#[derive(Debug, Clone, Serialize)]
pub struct ListJobsResponse {
    /// Every job of the DC, newest first.
    pub jobs: Vec<JobView>,
}

/// `POST /admin/v1/jobs/{job_id}:cancel`.
#[derive(Debug, Clone, Serialize)]
pub struct CancelJobResponse {
    /// The job after the call.
    pub job: JobView,
    /// The call cancelled it (false: it had already ended).
    pub cancelled: bool,
}

fn parse(id: &str) -> Result<JobId> {
    id.parse::<JobId>()
        .map_err(|e| invalid(format!("job id {id:?}: {e}")))
}

fn not_found(id: JobId) -> SprigError {
    SprigError::new(SprigCode::AdminRequestInvalid, format!("no job {id} in this DC"))
}

impl AdminService {
    /// `Jobs.ListJobs`: optional `state` and `kind` filters.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn list_jobs(&self, state: Option<&str>, kind: Option<&str>) -> Result<ListJobsResponse> {
        let mut jobs: Vec<JobView> = dispatch::list_jobs(&*self.stores.dc)
            .await?
            .into_iter()
            .filter(|j| state.is_none_or(|s| j.job().state.to_string() == s))
            .filter(|j| kind.is_none_or(|k| j.job().kind == k))
            .map(|j| JobView {
                mod_revision: j.mod_revision,
                job: j.record.body,
            })
            .collect();
        jobs.sort_by(|a, b| b.job.job_id.to_string().cmp(&a.job.job_id.to_string()));
        Ok(ListJobsResponse { jobs })
    }

    /// `Jobs.GetJob`.
    ///
    /// # Errors
    /// 39792 for a malformed or unknown id.
    pub async fn get_job(&self, id: &str) -> Result<JobView> {
        let id = parse(id)?;
        let j = dispatch::get_job(&*self.stores.dc, id)
            .await?
            .ok_or_else(|| not_found(id))?;
        Ok(JobView {
            mod_revision: j.mod_revision,
            job: j.record.body,
        })
    }

    /// `Jobs.CancelJob`: a job not yet final becomes `cancelled`; its
    /// worker sees the owner change at its next progress write and stops.
    ///
    /// # Errors
    /// 39504 off the leader; 39792 for a malformed or unknown id.
    pub async fn cancel_job(&self, id: &str) -> Result<CancelJobResponse> {
        let id = parse(id)?;
        for _ in 0..5 {
            let guard = self.leader_guard().await?;
            let mut j = dispatch::get_job(&*self.stores.dc, id)
                .await?
                .ok_or_else(|| not_found(id))?;
            if j.job().state.is_final() {
                return Ok(CancelJobResponse {
                    job: JobView {
                        mod_revision: j.mod_revision,
                        job: j.record.body,
                    },
                    cancelled: false,
                });
            }
            let key = dispatch::job_key(&*self.stores.dc, id)?;
            let body = &mut j.record.body;
            body.state = JobState::Cancelled;
            body.owner = None;
            body.finished_ms = Some(
                self.clock
                    .wall()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
            );
            let mut when = guard;
            when.push(Cmp::ModRev(key.clone(), j.mod_revision));
            let r = self
                .stores
                .dc
                .txn(sprig_meta::Txn {
                    when,
                    then: vec![Op::Put {
                        key,
                        value: j.record.encode()?,
                        lease: None,
                    }],
                    otherwise: vec![],
                })
                .await?;
            if r.succeeded {
                tracing::info!(job = %id, event = "job.cancelled", "job cancelled by an operator");
                let view = self.get_job(&id.to_string()).await?;
                return Ok(CancelJobResponse {
                    job: view,
                    cancelled: true,
                });
            }
            // A concurrent change, or lost leadership: the guard re-checks.
        }
        Err(SprigError::new(
            SprigCode::CatalogConflict,
            format!("job {id} kept changing; try again"),
        ))
    }
}
