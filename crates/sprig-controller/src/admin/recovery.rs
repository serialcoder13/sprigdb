//! An operator's recovery jobs (chapter 25, "Recovery after an outage"
//! steps 2-3; r2close): `POST /admin/v1/clusters/{id}:extract` and
//! `POST /admin/v1/clusters/{id}:rebuild`, behind `sprigctl cluster
//! extract|rebuild`. Both submit a job (cause `operator`) on the DC
//! leader; the `cluster_failover` loop follows it in the cluster's record.

use serde::{Deserialize, Serialize};
use sprig_cluster::failover::primary_replica::{binlog_pos, diverged};
use sprig_cluster::failover::{extract, rebuild};
use sprig_common::{Result, SprigError};
use sprig_meta::{Consistency, Record};
use sprig_settings::SettingCtx;

use super::{AdminService, invalid};
use crate::dispatch::{self, JobClass, NewJob};
use crate::failover::cluster::{ClusterFailoverRecord, FormerWriter, record_key};
use crate::failover::nodes::ctl_endpoints;
use crate::failover::settings as fs;

/// `POST /admin/v1/clusters/{id}:extract`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtractNodeRequest {
    /// The diverged node.
    pub node: String,
}

/// `POST /admin/v1/clusters/{id}:rebuild`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebuildNodeRequest {
    /// The node to rebuild.
    pub node: String,
    /// The replica to stream from (default: the healthiest one).
    #[serde(default)]
    pub source: Option<String>,
    /// Rebuild a diverged node without a verified extract; its old data
    /// directory is then kept beside the new one.
    #[serde(default)]
    pub no_extract: bool,
}

/// What both calls answer.
#[derive(Debug, Clone, Serialize)]
pub struct RecoveryJobResponse {
    /// The job.
    pub job_id: String,
    /// `errant_extract` or `node_rebuild`.
    pub kind: String,
    /// Its spec.
    pub spec: serde_json::Value,
}

impl AdminService {
    async fn failover_record(
        &self,
        id: &sprig_common::ids::ClusterId,
    ) -> Result<Option<ClusterFailoverRecord>> {
        let key = record_key(self.stores.dc.tier(), id)?;
        Ok(
            match self.stores.dc.get(&key, Consistency::Linearizable).await? {
                Some(kv) => Some(Record::<ClusterFailoverRecord>::decode(&kv.value)?.body),
                None => None,
            },
        )
    }

    async fn submit_recovery(
        &self,
        id: &sprig_common::ids::ClusterId,
        kind: &str,
        spec: serde_json::Value,
    ) -> Result<RecoveryJobResponse> {
        let guard = self.leader_guard().await?;
        let job = dispatch::submit_guarded(
            &*self.stores.dc,
            guard,
            &*self.clock,
            NewJob {
                kind: kind.into(),
                class: JobClass::Recovery,
                tenant_id: None,
                clusters: vec![id.clone()],
                priority: 20,
                cause: dispatch::OPERATOR.into(),
                zone: None,
                spec: spec.clone(),
            },
        )
        .await?;
        tracing::warn!(cluster = %id, %kind, job = %job, "recovery job submitted by an operator");
        Ok(RecoveryJobResponse {
            job_id: job.to_string(),
            kind: kind.into(),
            spec,
        })
    }

    /// `Clusters.ExtractNode`: an `errant_extract` job for the diverged
    /// transactions of `req.node`, the former writer of the cluster's last
    /// failover.
    ///
    /// # Errors
    /// 39504 off the leader; 39104 unknown cluster; 39792 when the node
    /// isn't a former writer or nothing diverged.
    pub async fn extract_node(
        &self,
        id: &str,
        req: ExtractNodeRequest,
    ) -> Result<RecoveryJobResponse> {
        let id = Self::parse_id(id)?;
        let rec = self
            .registry()
            .get(&id)
            .await?
            .ok_or_else(|| super::not_found(&id, self.dc()))?
            .body()
            .clone();
        let fo = self.failover_record(&id).await?.unwrap_or_default();
        let ev = fo
            .history
            .iter()
            .rev()
            .find(|e| e.kind == "promote" && e.from == req.node)
            .ok_or_else(|| invalid(format!("{} isn't a former writer of {id}", req.node)))?;
        let ep = ctl_endpoints(&rec)?
            .into_iter()
            .find(|e| e.node.as_str() == req.node)
            .ok_or_else(|| invalid(format!("{} isn't a node of {id}", req.node)))?;
        let pos = binlog_pos(&ep)
            .await
            .map_err(|e| invalid(format!("{} can't be read: {e}", req.node)))?;
        let range = diverged(&pos, &ev.applied, ev.domain)
            .ok_or_else(|| invalid(format!("{} has nothing diverged ({pos})", req.node)))?;
        let spec = serde_json::to_value(extract::ErrantExtractSpec {
            cluster: id.clone(),
            node: req.node,
            range,
            failover_at_unix_ms: ev.at_unix_ms,
        })
        .map_err(|e| SprigError::internal(e.to_string()))?;
        self.submit_recovery(&id, extract::KIND, spec).await
    }

    /// `Clusters.RebuildNode`: a `node_rebuild` job for `req.node`. The
    /// worker refuses the writer, Galera nodes, and a diverged node without
    /// a verified extract unless `no_extract`.
    ///
    /// # Errors
    /// 39504 off the leader; 39104 unknown cluster; 39792 for a node that
    /// isn't the cluster's.
    pub async fn rebuild_node(
        &self,
        id: &str,
        req: RebuildNodeRequest,
    ) -> Result<RecoveryJobResponse> {
        let id = Self::parse_id(id)?;
        let rec = self
            .registry()
            .get(&id)
            .await?
            .ok_or_else(|| super::not_found(&id, self.dc()))?
            .body()
            .clone();
        if !rec.nodes.iter().any(|n| n.node.as_str() == req.node) {
            return Err(invalid(format!("{} isn't a node of {id}", req.node)));
        }
        let rate: u64 = self.monitor.live().snapshot().get(
            &fs::REBUILD_MAX_MB_PER_SECOND,
            &SettingCtx::process(self.dc()).with_cluster(&id),
        );
        let fo = self.failover_record(&id).await?.unwrap_or_default();
        let diverged_node = fo.history.iter().any(|e| {
            e.from == req.node
                && matches!(
                    e.former,
                    Some(FormerWriter::Diverged | FormerWriter::Rebuilding)
                )
        });
        let mut spec = serde_json::to_value(rebuild::NodeRebuildSpec {
            cluster: id.clone(),
            node: req.node,
            source: req.source,
            require_extract: !req.no_extract,
        })
        .map_err(|e| SprigError::internal(e.to_string()))?;
        spec["rate_mb_per_second"] = serde_json::json!(rate);
        spec["diverged"] = serde_json::json!(diverged_node);
        self.submit_recovery(&id, rebuild::KIND, spec).await
    }
}
