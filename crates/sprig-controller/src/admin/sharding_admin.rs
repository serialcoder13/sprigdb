//! The Admin API's sharding calls (chapter 20's `sprigctl shard list|split|
//! merge|move`, chapter 16's `sprigctl advisor run|show|apply`), over the
//! controller's [`ShardingLoop`](crate::sharding::ShardingLoop).
//!
//! ```text
//! GET  /admin/v1/tenants/{name}/shards                  groups and their maps
//! POST /admin/v1/tenants/{name}/shards                  {"group":3,"kind":"split","shard":4,"ways":2}
//!                                                       {"group":3,"kind":"merge","left":6,"right":7}
//!                                                       {"group":3,"kind":"move","shard":4,"to":"dc-a-c2"}
//! GET  /admin/v1/tenants/{name}/advisor[?run=N]         the newest (or run N) advisor run
//! POST /admin/v1/tenants/{name}/advisor:run             a manual advisor run
//! POST /admin/v1/tenants/{name}/advisor:apply           {"run":N}
//! ```

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_mover::{JobStatus, JobStore, MetaMover, Mover as _};

use super::AdminService;
use crate::sharding::{GroupView, ManualJob, ShardingLoop, TickReport};

/// `POST /admin/v1/tenants/{name}/shards`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardJobRequest {
    /// The table group.
    pub group: u32,
    /// What to do.
    #[serde(flatten)]
    pub job: ManualJob,
}

/// `POST /admin/v1/tenants/{name}/advisor:apply`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvisorApplyRequest {
    /// The run.
    pub run: u64,
}

fn report_json(r: &TickReport) -> Value {
    json!({
        "run": r.advisor.map(|a| a.0),
        "verdict": r.advisor.map(|a| a.1),
        "applied_hold_ms": r.applied_hold_ms,
        "waiting": r.waiting.iter().map(|(s, w)| json!({"subject": s, "blocked_by": w})).collect::<Vec<_>>(),
    })
}

impl AdminService {
    fn sharding(&self) -> Result<&Arc<ShardingLoop>> {
        self.sharding.as_ref().ok_or_else(|| {
            SprigError::new(
                SprigCode::AdminRequestInvalid,
                "this controller doesn't run the sharding loop",
            )
        })
    }

    /// The same service with the sharding calls.
    #[must_use]
    pub fn with_sharding(mut self, s: Arc<ShardingLoop>) -> Self {
        self.sharding = Some(s);
        self
    }

    /// `sprigctl shard list <tenant>`.
    ///
    /// # Errors
    /// The tenant lookup's and the loop's errors.
    pub async fn list_shards(&self, tenant: &str) -> Result<Vec<GroupView>> {
        let t = self.get_tenant(tenant).await?.tenant_id;
        self.sharding()?.shards(t).await
    }

    /// `sprigctl graph list <tenant>` (chapter 43).
    ///
    /// # Errors
    /// The tenant lookup's and the loop's errors.
    pub async fn list_graphs(&self, tenant: &str) -> Result<Vec<crate::sharding::GraphRow>> {
        let t = self.get_tenant(tenant).await?.tenant_id;
        self.sharding()?.graphs(t).await
    }

    /// `sprigctl graph show <tenant> <db.graph>`.
    ///
    /// # Errors
    /// 39902 and the loop's errors.
    pub async fn show_graph(&self, tenant: &str, graph: &str) -> Result<crate::sharding::GraphView> {
        let t = self.get_tenant(tenant).await?.tenant_id;
        self.sharding()?.graph_show(t, graph).await
    }

    /// `sprigctl graph partition <tenant> <db.graph>`.
    ///
    /// # Errors
    /// 39902, 39904 and the loop's errors.
    pub async fn graph_partition(
        &self,
        tenant: &str,
        graph: &str,
    ) -> Result<crate::sharding::GraphPartition> {
        let t = self.get_tenant(tenant).await?.tenant_id;
        self.sharding()?.graph_partition(t, graph).await
    }

    /// `sprigctl shard split|merge|move`: the new mover job.
    ///
    /// # Errors
    /// 39470, 39471 and the loop's errors.
    pub async fn submit_shard_job(&self, tenant: &str, req: ShardJobRequest) -> Result<JobStatus> {
        let t = self.get_tenant(tenant).await?.tenant_id;
        let id = self
            .sharding()?
            .manual(t, req.group, req.job, "operator")
            .await?;
        MetaMover::new(JobStore::new(self.stores.dc.clone(), self.clock.clone()))
            .status(&id)
            .await
    }

    /// `sprigctl advisor show <tenant> [run]`.
    ///
    /// # Errors
    /// The loop's errors; 39792 when there is no run.
    pub async fn advisor_show(&self, tenant: &str, run: Option<u64>) -> Result<Value> {
        let t = self.get_tenant(tenant).await?.tenant_id;
        self.sharding()?.advisor_show(t, run).await?.ok_or_else(|| {
            SprigError::new(
                SprigCode::AdminRequestInvalid,
                format!(
                    "tenant {tenant} has no advisor run{}",
                    run.map_or(String::new(), |r| format!(" {r}"))
                ),
            )
        })
    }

    /// `sprigctl advisor run <tenant>`.
    ///
    /// # Errors
    /// The loop's errors.
    pub async fn advisor_run(&self, tenant: &str) -> Result<Value> {
        let t = self.get_tenant(tenant).await?.tenant_id;
        Ok(report_json(&self.sharding()?.advisor_run(t).await?))
    }

    /// `sprigctl advisor apply <tenant> <run>`.
    ///
    /// # Errors
    /// The loop's errors.
    pub async fn advisor_apply(&self, tenant: &str, req: AdvisorApplyRequest) -> Result<Value> {
        let t = self.get_tenant(tenant).await?.tenant_id;
        Ok(report_json(
            &self.sharding()?.advisor_apply(t, req.run).await?,
        ))
    }
}
