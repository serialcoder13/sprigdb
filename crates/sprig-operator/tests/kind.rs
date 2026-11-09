//! The Kubernetes provisioner against a real cluster (chapter 27,
//! "Testing"): `create` and `retire` through [`KubernetesProvisioner`] and
//! the real REST client, on a kind cluster with mariadb-operator.
//!
//! Skipped unless `SPRIG_KIND_API` is set. The run in
//! `decisions/r2-failover.md` ("Kubernetes on kind (r2kind)") set:
//!
//! - `SPRIG_KIND_API`: the API server, `https://127.0.0.1:<port>`;
//! - `SPRIG_KIND_CA`: its CA bundle (PEM file);
//! - `SPRIG_KIND_TOKEN`: a file with the operator ServiceAccount's token
//!   (`kubectl create token sprigdb-operator`), so the calls run under the
//!   chart's Role;
//! - `SPRIG_KIND_NS`: the namespace (default `sprigdb`);
//! - `SPRIG_KIND_CREATE`: `<pool>,<pool>,...`: one `create` per pool, each
//!   a `ClusterSpec` naming that `SprigClusterPool`;
//! - `SPRIG_KIND_RETIRE`: `<cluster_id>,...`: one `retire` each.
//!
//! The operator itself runs in the cluster; this test only drives the
//! provisioner's two operations and prints what they did.

#![allow(clippy::unwrap_used, clippy::print_stdout)]

use std::path::PathBuf;
use std::sync::Arc;

use sprig_common::SprigCode;
use sprig_common::ids::{ClusterId, DcId};
use sprig_common::time::SystemClock;
use sprig_operator::OperatorConfig;
use sprig_operator::accounts::MysqlAccounts;
use sprig_operator::crd::SPRIG_CLUSTER;
use sprig_operator::kube::{HttpKube, KubeApi, KubeConfig};
use sprig_operator::provisioner::KubernetesProvisioner;
use sprig_operator::reconcile::Reconciler;
use sprig_operator::register::AdminRegistrar;
use sprig_provision::{ClusterSpec, MariaDbVersion, Provisioner, Resources, Topology};

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

fn provisioner() -> Option<(KubernetesProvisioner, Arc<dyn KubeApi>, String)> {
    let base = env("SPRIG_KIND_API")?;
    let ns = env("SPRIG_KIND_NS").unwrap_or_else(|| "sprigdb".into());
    let cfg = KubeConfig {
        base,
        token_file: env("SPRIG_KIND_TOKEN").map(PathBuf::from),
        ca_pem: env("SPRIG_KIND_CA").map(|p| std::fs::read(p).unwrap()),
        namespace: ns.clone(),
    };
    let kube: Arc<dyn KubeApi> = Arc::new(HttpKube::new(cfg).unwrap());
    let rec = Reconciler::new(
        Arc::clone(&kube),
        Arc::new(MysqlAccounts::new(None)),
        Arc::new(AdminRegistrar::new("http://127.0.0.1:1", None).unwrap()),
        OperatorConfig::new(&ns),
        Arc::new(SystemClock),
    );
    Some((KubernetesProvisioner::new(Arc::new(rec)), kube, ns))
}

#[tokio::test]
async fn kind_create_and_retire() {
    let Some((prov, kube, ns)) = provisioner() else {
        println!("SPRIG_KIND_API not set: skipped");
        return;
    };
    for pool in env("SPRIG_KIND_CREATE").unwrap_or_default().split(',') {
        if pool.is_empty() {
            continue;
        }
        let spec = ClusterSpec {
            dc: DcId::new("kind1").unwrap(),
            topology: Topology::Single,
            nodes: 1,
            version: MariaDbVersion {
                major: 11,
                minor: 8,
            },
            resources: Resources::default(),
            zones: Vec::new(),
            backup_target: String::new(),
            pool: Some(pool.to_owned()),
        };
        match prov.create(spec).await {
            Ok(job) => {
                let made = kube.list(&SPRIG_CLUSTER, &ns, None).await.unwrap();
                let name = made
                    .iter()
                    .find(|c| {
                        c["metadata"]["annotations"]["sprigdb.io/job-id"].as_str()
                            == Some(job.to_string().as_str())
                    })
                    .map(|c| c["metadata"]["name"].as_str().unwrap_or_default().to_owned());
                println!("create pool={pool} job={job} cluster={name:?}");
                assert!(name.is_some(), "no SprigCluster carries job {job}");
            }
            Err(e) => {
                println!("create pool={pool} refused: {e}");
                assert_eq!(e.code(), Some(SprigCode::ProvisionNoHosts), "{e}");
            }
        }
    }
    for id in env("SPRIG_KIND_RETIRE").unwrap_or_default().split(',') {
        if id.is_empty() {
            continue;
        }
        let job = prov.retire(&ClusterId::new(id).unwrap()).await.unwrap();
        println!("retire cluster={id} job={job}");
    }
}
