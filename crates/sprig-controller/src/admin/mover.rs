//! The mover's Admin API (chapter 10, `sprigctl shard move` and
//! `sprigctl job`): submit a tenant move, list, show, cancel, and let a
//! manual move cut over. Jobs are records in the DC tier
//! (`sprig_mover::MetaMover`); workers run them. Any controller answers.
//!
//! ```text
//! POST /admin/v1/mover/jobs            {"tenant": "acme", "to": "dc-a-c2"}
//! GET  /admin/v1/mover/jobs?tenant=acme
//! GET  /admin/v1/mover/jobs/{id}
//! POST /admin/v1/mover/jobs/{id}:cancel
//! POST /admin/v1/mover/jobs/{id}:cutover
//! ```

use serde::{Deserialize, Serialize};
use sprig_common::ids::{ClusterId, JobId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_mover::{
    CutoverMode, JobFilter, JobSpec, JobStatus, JobStore, MetaMover, Mover, VerifyMode,
};
use sprig_shard::{Placement, TenantMaps};

use super::{AdminService, invalid};

/// `POST /admin/v1/mover/jobs`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MoveRequest {
    /// The tenant, by name.
    pub tenant: String,
    /// The target cluster.
    pub to: String,
    /// `auto` (default) or `manual` (waits in `ready` for `:cutover`).
    #[serde(default)]
    pub cutover: Option<String>,
    /// `full` (default), `off`, or `sample:<pct>`.
    #[serde(default)]
    pub verify: Option<String>,
}

/// `GET /admin/v1/mover/jobs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MoveJobs {
    /// Newest first.
    pub jobs: Vec<JobStatus>,
}

fn parse_id(id: &str) -> Result<JobId> {
    id.parse::<JobId>()
        .map_err(|e| invalid(format!("job id {id:?}: {e}")))
}

impl AdminService {
    fn mover(&self) -> MetaMover {
        MetaMover::new(JobStore::new(self.stores.dc.clone(), self.clock.clone()))
    }

    /// Starts moving `tenant` to another cluster of this DC.
    ///
    /// # Errors
    /// 39792 for a bad request or a tenant already there; 39131 if a job
    /// holds the tenant; the tenant lookup's errors.
    pub async fn submit_move(&self, req: MoveRequest) -> Result<JobStatus> {
        let tenant = self.get_tenant(&req.tenant).await?.tenant_id;
        let to = ClusterId::new(req.to.as_str()).map_err(|e| invalid(e.to_string()))?;
        if self.monitor.registry().get(&to).await?.is_none() {
            return Err(SprigError::new(
                SprigCode::ClusterNotFound,
                format!("no cluster {to} in this DC"),
            ));
        }
        let (map, _) = TenantMaps::new(self.stores.dc.clone()).load(tenant).await?;
        let from = Placement::of(&map)?.cluster;
        if from == to {
            return Err(invalid(format!("tenant {} is already on {to}", req.tenant)));
        }
        let cutover = match req.cutover.as_deref() {
            None | Some("auto") => CutoverMode::Auto,
            Some("manual") => CutoverMode::Manual,
            Some(o) => return Err(invalid(format!("cutover {o:?}: auto or manual"))),
        };
        let verify = match req.verify.as_deref() {
            None | Some("full") => VerifyMode::Full,
            Some("off") => VerifyMode::Off,
            Some(s) => match s.strip_prefix("sample:").and_then(|p| p.parse::<u8>().ok()) {
                Some(pct) => VerifyMode::Sample { pct },
                None => return Err(invalid(format!("verify {s:?}: full, off or sample:<pct>"))),
            },
        };
        let mut spec = JobSpec::move_tenant(tenant, from, to)
            .with_cutover(cutover)
            .with_verify(verify);
        spec.requested_by = Some("operator".to_owned());
        let m = self.mover();
        let id = m.submit(spec).await?;
        let st = m.status(&id).await?;
        // Chapter 11: an operator move is a decision too.
        self.record_operator_move(&st).await;
        Ok(st)
    }

    /// Mover jobs, newest first; `tenant` filters by name.
    ///
    /// # Errors
    /// The store's errors; the tenant lookup's.
    pub async fn list_moves(&self, tenant: Option<&str>) -> Result<MoveJobs> {
        let tenant = match tenant {
            Some(t) => Some(self.get_tenant(t).await?.tenant_id),
            None => None,
        };
        let jobs = self
            .mover()
            .list(&JobFilter {
                tenant,
                active_only: false,
            })
            .await?;
        Ok(MoveJobs { jobs })
    }

    /// One mover job.
    ///
    /// # Errors
    /// 39130 if unknown.
    pub async fn get_move(&self, id: &str) -> Result<JobStatus> {
        self.mover().status(&parse_id(id)?).await
    }

    /// Cancels a mover job (before its commit).
    ///
    /// # Errors
    /// 39130 if unknown; 39132 once committed.
    pub async fn cancel_move(&self, id: &str) -> Result<JobStatus> {
        let id = parse_id(id)?;
        let m = self.mover();
        m.cancel(&id, "operator").await?;
        m.status(&id).await
    }

    /// Lets a manual mover job cut over.
    ///
    /// # Errors
    /// 39130 if unknown; 39132 for an automatic job.
    pub async fn cutover_move(&self, id: &str) -> Result<JobStatus> {
        let id = parse_id(id)?;
        let m = self.mover();
        m.cut_over(&id).await?;
        m.status(&id).await
    }
}
