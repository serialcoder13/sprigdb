//! Backups on Kubernetes, exported for the `BackupMissed` alert.
//!
//! On Kubernetes, mariadb-operator takes each cluster's backups: the
//! `PhysicalBackup` the operator makes for `spec.backup`
//! ([`crate::mapping::backups`]) runs one `batch/v1` `Job` per scheduled
//! backup, owned by that `PhysicalBackup`. The leading operator lists those
//! Jobs every [`PERIOD`] and exports
//! `sprig_backup_last_success_timestamp_seconds{cluster, kind="full"}`:
//! the newest successful Job's `status.completionTime`, under the
//! cluster's SprigDB id (`status.clusterId`), the same series the
//! controllers export from the Compose bundle's backup records.
//!
//! Only Kubernetes' own Job fields are read (`ownerReferences`,
//! `status.succeeded`, `status.completionTime`), not mariadb-operator's
//! status. The gauge never moves back: a Job removed by mariadb-operator's
//! history limit doesn't lower it. After an operator restart it starts
//! from the Jobs still listed.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use sprig_common::Result;

use crate::crd;
use crate::kube::{ApiResource, KubeApi};
use crate::lease::parse_utc;
use crate::mapping::{PHYSICAL_BACKUP, backup_name};

/// `batch/v1` `Job`.
pub const JOB: ApiResource = ApiResource {
    group: "batch",
    version: "v1",
    kind: "Job",
    plural: "jobs",
};

/// How often the leading operator reads the Jobs.
pub const PERIOD: Duration = Duration::from_secs(60);

/// The gauge's name (the alert's metric).
pub const METRIC: &str = "sprig_backup_last_success_timestamp_seconds";

/// The `kind` label: a `PhysicalBackup` is a full backup.
pub const KIND: &str = "full";

/// One cluster's newest successful backup.
#[derive(Debug, Clone, PartialEq)]
pub struct LastBackup {
    /// The SprigDB cluster id (`status.clusterId`).
    pub cluster: String,
    /// When the backup Job completed, in Unix seconds.
    pub finished_at: f64,
}

/// The name of the `PhysicalBackup` that owns `job`, if one does.
fn owner(job: &Value) -> Option<&str> {
    job["metadata"]["ownerReferences"]
        .as_array()?
        .iter()
        .find(|r| r["kind"].as_str() == Some(PHYSICAL_BACKUP.kind))?["name"]
        .as_str()
}

/// When `job` completed successfully, in Unix seconds; `None` while it
/// runs, after it failed, or without a readable `completionTime`.
fn completed(job: &Value) -> Option<i64> {
    let st = &job["status"];
    if st["succeeded"].as_u64().unwrap_or(0) == 0 {
        return None;
    }
    parse_utc(st["completionTime"].as_str()?)
}

/// The newest successful backup of each registered `SprigCluster` in
/// `clusters`, from `jobs`. Clusters without a `status.clusterId` (not
/// registered yet) and Jobs that no cluster's `PhysicalBackup` owns are
/// skipped.
#[must_use]
pub fn newest(clusters: &[Value], jobs: &[Value]) -> Vec<LastBackup> {
    // PhysicalBackup name -> cluster id.
    let ids: BTreeMap<String, &str> = clusters
        .iter()
        .filter_map(|c| {
            let name = c["metadata"]["name"].as_str()?;
            let id = c["status"]["clusterId"]
                .as_str()
                .filter(|s| !s.is_empty())?;
            Some((backup_name(name), id))
        })
        .collect();
    let mut best: BTreeMap<&str, i64> = BTreeMap::new();
    for j in jobs {
        let (Some(pb), Some(t)) = (owner(j), completed(j)) else {
            continue;
        };
        if let Some(id) = ids.get(pb) {
            let e = best.entry(id).or_insert(t);
            *e = (*e).max(t);
        }
    }
    best.into_iter()
        .map(|(cluster, t)| LastBackup {
            cluster: cluster.to_owned(),
            #[allow(clippy::cast_precision_loss)] // Unix seconds fit in f64
            finished_at: t as f64,
        })
        .collect()
}

/// Exports the newest successful backups and remembers the highest value
/// set per cluster, so the gauge never moves back.
#[derive(Default)]
pub struct BackupExporter {
    seen: BTreeMap<String, f64>,
}

impl BackupExporter {
    /// A new exporter, with nothing exported yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Lists the `SprigCluster`s and Jobs in `ns` and sets the gauge for
    /// each cluster with a successful backup; returns what it set.
    ///
    /// # Errors
    /// A failed list.
    pub async fn export_once(
        &mut self,
        kube: &Arc<dyn KubeApi>,
        ns: &str,
    ) -> Result<Vec<LastBackup>> {
        let clusters = kube.list(&crd::SPRIG_CLUSTER, ns, None).await?;
        let jobs = kube.list(&JOB, ns, None).await?;
        let mut out = newest(&clusters, &jobs);
        for b in &mut out {
            let e = self.seen.entry(b.cluster.clone()).or_insert(b.finished_at);
            *e = e.max(b.finished_at);
            b.finished_at = *e;
            metrics::gauge!(METRIC, "cluster" => b.cluster.clone(), "kind" => KIND)
                .set(b.finished_at);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cluster(name: &str, id: Option<&str>) -> Value {
        let mut c = json!({"metadata": {"name": name}, "status": {}});
        if let Some(id) = id {
            c["status"]["clusterId"] = json!(id);
        }
        c
    }

    fn job(owner_kind: &str, owner: &str, succeeded: u64, done: Option<&str>) -> Value {
        let mut j = json!({
            "metadata": {
                "name": format!("{owner}-x"),
                "ownerReferences": [{"kind": owner_kind, "name": owner}],
            },
            "status": {"succeeded": succeeded},
        });
        if let Some(t) = done {
            j["status"]["completionTime"] = json!(t);
        }
        j
    }

    #[test]
    fn newest_successful_job_per_registered_cluster() {
        let clusters = [
            cluster("orders", Some("fra1-c1")),
            cluster("users", Some("fra1-c2")),
            cluster("fresh", None),
        ];
        let jobs = [
            job(
                "PhysicalBackup",
                "orders-backup",
                1,
                Some("2026-11-12T03:00:40Z"),
            ),
            job(
                "PhysicalBackup",
                "orders-backup",
                1,
                Some("2026-11-13T03:00:10Z"),
            ),
            // Running, failed, or another owner: not a success of orders.
            job("PhysicalBackup", "orders-backup", 0, None),
            job(
                "PhysicalBackup",
                "orders-backup",
                0,
                Some("2026-11-14T03:00:00Z"),
            ),
            job("CronJob", "orders-backup", 1, Some("2026-11-15T03:00:00Z")),
            job(
                "PhysicalBackup",
                "users-backup",
                1,
                Some("2026-11-13T04:00:00Z"),
            ),
            // Not registered yet, and an unknown PhysicalBackup.
            job(
                "PhysicalBackup",
                "fresh-backup",
                1,
                Some("2026-11-13T05:00:00Z"),
            ),
            job(
                "PhysicalBackup",
                "other-backup",
                1,
                Some("2026-11-13T06:00:00Z"),
            ),
            // An unreadable time.
            job("PhysicalBackup", "users-backup", 1, Some("yesterday")),
        ];
        assert_eq!(
            newest(&clusters, &jobs),
            vec![
                LastBackup {
                    cluster: "fra1-c1".into(),
                    finished_at: 1_794_538_810.0,
                },
                LastBackup {
                    cluster: "fra1-c2".into(),
                    finished_at: 1_794_542_400.0,
                },
            ]
        );
    }

    #[test]
    fn no_jobs_no_series() {
        assert!(newest(&[cluster("orders", Some("fra1-c1"))], &[]).is_empty());
    }
}
