//! The backup gauge on Kubernetes ([`sprig_operator::backups`]): the
//! leading operator reads mariadb-operator's backup Jobs through the REST
//! client, against the in-process fake API server.

#![allow(clippy::unwrap_used)]

mod support;

use std::sync::Arc;

use serde_json::{Value, json};
use sprig_operator::backups::{BackupExporter, LastBackup};
use sprig_operator::kube::{HttpKube, KubeApi, KubeConfig};
use support::FakeApi;

const NS: &str = "sprigdb";
const CLUSTERS: &str = "/apis/sprigdb.io/v1alpha1/namespaces/sprigdb/sprigclusters";
const JOBS: &str = "/apis/batch/v1/namespaces/sprigdb/jobs";

fn backup_job(name: &str, owner: &str, done: Option<&str>) -> Value {
    let mut j = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": NS,
            "ownerReferences": [{
                "apiVersion": "k8s.mariadb.com/v1alpha1",
                "kind": "PhysicalBackup",
                "name": owner,
                "uid": "pb-uid",
                "controller": true,
            }],
        },
        "status": {},
    });
    if let Some(t) = done {
        j["status"] = json!({"succeeded": 1, "completionTime": t});
    } else {
        j["status"] = json!({"active": 1});
    }
    j
}

#[tokio::test]
async fn the_newest_completed_backup_job_of_each_cluster_is_exported_and_never_moves_back() {
    let api = FakeApi::start().await;
    let kube: Arc<dyn KubeApi> = Arc::new(
        HttpKube::new(KubeConfig {
            base: api.base(),
            token_file: None,
            ca_pem: None,
            namespace: NS.into(),
        })
        .unwrap(),
    );
    api.put(
        CLUSTERS,
        json!({
            "apiVersion": "sprigdb.io/v1alpha1",
            "kind": "SprigCluster",
            "metadata": {"name": "orders", "namespace": NS},
            "spec": {"topology": "replication", "nodes": 3, "storage": "10Gi"},
            "status": {"clusterId": "fra1-c4", "phase": "Active"},
        }),
    );
    api.put(
        JOBS,
        backup_job(
            "orders-backup-20261112030000",
            "orders-backup",
            Some("2026-11-12T03:02:00Z"),
        ),
    );
    api.put(
        JOBS,
        backup_job(
            "orders-backup-20261113030000",
            "orders-backup",
            Some("2026-11-13T03:01:30Z"),
        ),
    );
    // Today's is still running.
    api.put(
        JOBS,
        backup_job("orders-backup-20261114030000", "orders-backup", None),
    );
    let mut exp = BackupExporter::new();
    let first = exp.export_once(&kube, NS).await.unwrap();
    assert_eq!(
        first,
        vec![LastBackup {
            cluster: "fra1-c4".into(),
            finished_at: 1_794_538_890.0,
        }]
    );
    assert_eq!(api.count("GET", JOBS), 1);

    // mariadb-operator's history limit removes the newest Job's record
    // (here: replaced by an older one): the gauge keeps the newest seen.
    api.put(
        JOBS,
        backup_job(
            "orders-backup-20261113030000",
            "orders-backup",
            Some("2026-11-11T03:00:00Z"),
        ),
    );
    let second = exp.export_once(&kube, NS).await.unwrap();
    assert_eq!(second, first);
}
