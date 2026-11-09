//! The Kubernetes provisioner (chapter 27, the `Provisioner` trait's
//! second implementation). It lives here, beside its client, rather than in
//! `sprig-provision::kubernetes`: `sprig-provision` would otherwise depend
//! on the operator's REST client.
//!
//! `create` adds a `SprigCluster` to the spec's pool (within
//! `maxClusters`; the reconcile loop does the rest) or, without a pool,
//! creates one from the spec; `retire` deletes the `SprigCluster`, and
//! owner references delete its `MariaDB`, backups and Services; `retire`
//! deletes what owner references don't reach: the volume claims,
//! mariadb-operator's generated password Secrets and the cluster's entry
//! in the credentials Secret. A cluster
//! is retired only after the controller drained it (chapter 11).

use std::sync::Arc;

use serde_json::Value;
use sprig_common::ids::{ClusterId, JobId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_provision::{ClusterSpec, ProvisionOp, Provisioner, ProvisionerKind, Topology};

use crate::crd::{
    self, BackupSpec, FailoverDriver, MariaDbImage, SPRIG_CLUSTER, SPRIG_CLUSTER_POOL,
    SprigCluster, SprigClusterPool, SprigClusterSpec, StorageSpec,
};
use crate::kube::{ApiResource, KubeError, SECRET};
use crate::reconcile::Reconciler;

/// `v1` `PersistentVolumeClaim`.
pub const PVC: ApiResource = ApiResource {
    group: "",
    version: "v1",
    kind: "PersistentVolumeClaim",
    plural: "persistentvolumeclaims",
};

/// [`Provisioner`] over the operator's custom resources.
pub struct KubernetesProvisioner {
    rec: Arc<Reconciler>,
}

impl std::fmt::Debug for KubernetesProvisioner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubernetesProvisioner")
            .finish_non_exhaustive()
    }
}

/// A `SprigCluster` spec for a `ClusterSpec`.
#[must_use]
pub fn cluster_spec(spec: &ClusterSpec) -> SprigClusterSpec {
    let storage_gb = spec.resources.storage_gb.max(10);
    let mut resources = serde_json::json!({});
    if spec.resources.cpu_millis > 0 {
        resources["requests"]["cpu"] = format!("{}m", spec.resources.cpu_millis).into();
    }
    if spec.resources.memory_gb > 0 {
        let m = format!("{}Gi", spec.resources.memory_gb);
        resources["requests"]["memory"] = m.clone().into();
        resources["limits"]["memory"] = m.into();
    }
    SprigClusterSpec {
        topology: match spec.topology {
            Topology::Galera => crd::Topology::Galera,
            Topology::PrimaryReplica => crd::Topology::Replication,
            Topology::Single => crd::Topology::Single,
        },
        nodes: spec.nodes,
        mariadb: MariaDbImage {
            image: format!("mariadb:{}", spec.version),
        },
        storage: StorageSpec {
            size: format!("{storage_gb}Gi"),
            storage_class_name: None,
        },
        failover_driver: FailoverDriver::Sprig,
        backup: (!spec.backup_target.is_empty()).then(|| BackupSpec {
            schedule: "0 2 * * *".into(),
            target: spec.backup_target.clone(),
            max_retention: None,
            endpoint: None,
            credentials_secret: None,
        }),
        zones: spec.zones.clone(),
        resources: (resources != serde_json::json!({})).then_some(resources),
    }
}

impl KubernetesProvisioner {
    /// A provisioner writing through `rec`'s client.
    #[must_use]
    pub fn new(rec: Arc<Reconciler>) -> Self {
        Self { rec }
    }
}

fn kerr(e: &crate::kube::KubeError) -> SprigError {
    SprigError::new(SprigCode::Internal, format!("kubernetes API: {e}"))
}

#[async_trait::async_trait]
impl Provisioner for KubernetesProvisioner {
    fn kind(&self) -> ProvisionerKind {
        ProvisionerKind::Kubernetes
    }

    fn supports(&self, op: ProvisionOp) -> bool {
        matches!(op, ProvisionOp::Create | ProvisionOp::Retire)
    }

    async fn create(&self, spec: ClusterSpec) -> Result<JobId> {
        let ns = self.rec.config().namespace.clone();
        let job = JobId::new_v7();
        if let Some(pool) = &spec.pool {
            let raw = self
                .rec
                .kube()
                .get(&SPRIG_CLUSTER_POOL, &ns, pool)
                .await
                .map_err(|e| kerr(&e))?
                .ok_or_else(|| {
                    SprigError::new(
                        SprigCode::ProvisionerUnsupported,
                        format!("no SprigClusterPool {pool} in namespace {ns}"),
                    )
                })?;
            let pool: SprigClusterPool = serde_json::from_value(raw)
                .map_err(|e| SprigError::internal(format!("bad pool: {e}")))?;
            return match self.rec.grow(&pool, Some(&job.to_string())).await? {
                Some(_) => Ok(job),
                None => Err(SprigError::new(
                    SprigCode::ProvisionNoHosts,
                    format!(
                        "pool {} is at maxClusters {}",
                        pool.metadata.name, pool.spec.max_clusters
                    ),
                )),
            };
        }
        let suffix = uuid::Uuid::now_v7().simple().to_string();
        let name = format!("{}-{}", spec.dc, &suffix[suffix.len() - 6..]);
        let mut sc = SprigCluster::new("SprigCluster", &ns, &name, cluster_spec(&spec));
        sc.metadata
            .labels
            .insert(crd::MANAGED_BY.0.into(), crd::MANAGED_BY.1.into());
        sc.metadata
            .annotations
            .insert("sprigdb.io/job-id".into(), job.to_string());
        let mut v = serde_json::to_value(&sc).map_err(|e| SprigError::internal(e.to_string()))?;
        if let Some(o) = v.as_object_mut() {
            o.remove("status");
        }
        self.rec
            .kube()
            .create(&SPRIG_CLUSTER, &ns, &v)
            .await
            .map_err(|e| kerr(&e))?;
        Ok(job)
    }

    async fn retire(&self, c: &ClusterId) -> Result<JobId> {
        let ns = self.rec.config().namespace.clone();
        let all = self
            .rec
            .kube()
            .list(&SPRIG_CLUSTER, &ns, None)
            .await
            .map_err(|e| kerr(&e))?;
        let found = all
            .iter()
            .find(|v: &&Value| v["status"]["clusterId"].as_str() == Some(c.as_str()));
        let Some(v) = found else {
            return Err(SprigError::new(
                SprigCode::ProvisionerUnsupported,
                format!("no SprigCluster registered as {c} in namespace {ns}"),
            ));
        };
        let name = v["metadata"]["name"].as_str().unwrap_or_default();
        self.rec
            .kube()
            .delete(&SPRIG_CLUSTER, &ns, name)
            .await
            .map_err(|e| kerr(&e))?;
        // The StatefulSet's volume claims outlive it (Kubernetes keeps
        // them by default; found on kind): a retired cluster's go too.
        // They are deleted once their pods are gone.
        for pvc in self
            .rec
            .kube()
            .list(
                &PVC,
                &ns,
                Some(&format!("app.kubernetes.io/instance={name}")),
            )
            .await
            .map_err(|e| kerr(&e))?
        {
            if let Some(n) = pvc["metadata"]["name"].as_str() {
                self.rec
                    .kube()
                    .delete(&PVC, &ns, n)
                    .await
                    .map_err(|e| kerr(&e))?;
            }
        }
        // mariadb-operator's generated passwords aren't owned by the
        // `MariaDB` and stay behind, and so would the system accounts'
        // passwords in the credentials Secret (found on kind).
        for secret in [format!("{name}-root"), format!("{name}-repl-password")] {
            self.rec
                .kube()
                .delete(&SECRET, &ns, &secret)
                .await
                .map_err(|e| kerr(&e))?;
        }
        let creds = &self.rec.config().credentials_secret;
        match self
            .rec
            .kube()
            .merge_patch(
                &SECRET,
                &ns,
                creds,
                &serde_json::json!({"data": {format!("{name}.toml"): null}}),
            )
            .await
        {
            Ok(_) | Err(KubeError::NotFound) => {}
            Err(e) => return Err(kerr(&e)),
        }
        Ok(JobId::new_v7())
    }
}
