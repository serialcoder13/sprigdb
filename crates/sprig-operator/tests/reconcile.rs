//! The operator's reconcile loop against an in-process fake Kubernetes API
//! server, through the real HTTP client (chapter 27, "The Kubernetes
//! operator"; no real cluster is involved).

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

mod support;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use serde_json::{Value, json};
use sprig_common::time::{Clock, ManualClock};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_operator::accounts::{BootstrapTarget, SystemAccounts};
use sprig_operator::crd::{self, Phase};
use sprig_operator::kube::{HttpKube, KubeApi, KubeConfig, KubeError, LEASE};
use sprig_operator::lease::LeaseElector;
use sprig_operator::provisioner::KubernetesProvisioner;
use sprig_operator::reconcile::Reconciler;
use sprig_operator::register::{ClusterRegistrar, ClusterRegistration, Registered};
use sprig_operator::{OperatorConfig, pass};
use sprig_provision::{ClusterSpec, MariaDbVersion, Provisioner, Resources, Topology};
use support::FakeApi;

const NS: &str = "sprigdb";
const SC: &str = "/apis/sprigdb.io/v1alpha1/namespaces/sprigdb/sprigclusters";
const POOLS: &str = "/apis/sprigdb.io/v1alpha1/namespaces/sprigdb/sprigclusterpools";
const MDB: &str = "/apis/k8s.mariadb.com/v1alpha1/namespaces/sprigdb/mariadbs";
const PB: &str = "/apis/k8s.mariadb.com/v1alpha1/namespaces/sprigdb/physicalbackups";
const SVC: &str = "/api/v1/namespaces/sprigdb/services";
const SECRETS: &str = "/api/v1/namespaces/sprigdb/secrets";
const EVENTS: &str = "/api/v1/namespaces/sprigdb/events";
const PVCS: &str = "/api/v1/namespaces/sprigdb/persistentvolumeclaims";

#[derive(Default)]
struct FakeAccounts {
    ensured: Mutex<Vec<BootstrapTarget>>,
    domains: Mutex<HashMap<String, u32>>,
    fail: Mutex<bool>,
}

#[async_trait]
impl SystemAccounts for FakeAccounts {
    async fn ensure(&self, t: &BootstrapTarget) -> Result<()> {
        if *self.fail.lock().unwrap() {
            return Err(SprigError::new(
                SprigCode::ClusterUnavailable,
                "TLS handshake failed",
            ));
        }
        self.ensured.lock().unwrap().push(t.clone());
        Ok(())
    }
    async fn ensure_domain(&self, t: &BootstrapTarget, domain: u32) -> Result<usize> {
        let mut d = self.domains.lock().unwrap();
        let changed = d.insert(t.cluster.clone(), domain) != Some(domain);
        Ok(if changed { t.nodes.len() } else { 0 })
    }
}

#[derive(Default)]
struct FakeRegistrar {
    calls: Mutex<Vec<ClusterRegistration>>,
    refuse: Mutex<bool>,
    node_updates: Mutex<Vec<(String, usize)>>,
    retired: Mutex<Vec<String>>,
}

#[async_trait]
impl ClusterRegistrar for FakeRegistrar {
    async fn register(&self, r: &ClusterRegistration) -> Result<Registered> {
        if *self.refuse.lock().unwrap() {
            return Err(SprigError::new(
                SprigCode::ClusterPrecheckFailed,
                "cluster failed 1 registration precheck(s)",
            ));
        }
        let mut c = self.calls.lock().unwrap();
        c.push(r.clone());
        Ok(Registered {
            cluster_id: format!("fra1-c{}", c.len() + 6),
            cluster_number: u32::try_from(c.len()).unwrap() + 6,
        })
    }
    async fn set_nodes(
        &self,
        cluster_id: &str,
        nodes: &[sprig_cluster::record::NodeSpec],
    ) -> Result<()> {
        self.node_updates
            .lock()
            .unwrap()
            .push((cluster_id.to_owned(), nodes.len()));
        Ok(())
    }
    async fn retire(&self, cluster_id: &str) -> Result<()> {
        self.retired.lock().unwrap().push(cluster_id.to_owned());
        Ok(())
    }
}

struct World {
    api: FakeApi,
    kube: Arc<dyn KubeApi>,
    accounts: Arc<FakeAccounts>,
    registrar: Arc<FakeRegistrar>,
    clock: Arc<ManualClock>,
    rec: Arc<Reconciler>,
}

async fn world() -> World {
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
    let accounts = Arc::new(FakeAccounts::default());
    let registrar = Arc::new(FakeRegistrar::default());
    let clock = Arc::new(ManualClock::new());
    let mut cfg = OperatorConfig::new(NS);
    cfg.agent_image = Some("ghcr.io/sprigdb/sprigdb:0.9.0".into());
    let rec = Arc::new(Reconciler::new(
        Arc::clone(&kube),
        accounts.clone(),
        registrar.clone(),
        cfg,
        clock.clone(),
    ));
    World {
        api,
        kube,
        accounts,
        registrar,
        clock,
        rec,
    }
}

fn sprig_cluster(name: &str, topology: &str, nodes: u8) -> Value {
    json!({
        "apiVersion": "sprigdb.io/v1alpha1",
        "kind": "SprigCluster",
        "metadata": {"name": name, "namespace": NS},
        "spec": {
            "topology": topology,
            "nodes": nodes,
            "mariadb": {"image": "mariadb:12.3"},
            "storage": {"size": "500Gi", "storageClassName": "fast-ssd"},
            "failoverDriver": "operator",
            "backup": {"schedule": "0 2 * * *", "target": "s3://sprig-fra1-backups"},
        },
    })
}

impl World {
    async fn reconcile(&self, name: &str) -> sprig_operator::reconcile::Outcome {
        let raw = self
            .kube
            .get(&crd::SPRIG_CLUSTER, NS, name)
            .await
            .unwrap()
            .unwrap();
        self.rec.reconcile(&raw).await.unwrap()
    }

    fn status(&self, name: &str) -> Value {
        self.api.get(SC, name).unwrap()["status"].clone()
    }

    /// What mariadb-operator, the cloud's load balancer and the root
    /// Secret would do.
    fn bring_up(&self, name: &str, nodes: u8) {
        self.api.mutate(
            MDB,
            name,
            &json!({"status": {"conditions": [{"type": "Ready", "status": "True"}]}}),
        );
        for i in 0..nodes {
            self.api.mutate(
                SVC,
                &format!("{name}-node-{i}"),
                &json!({"status": {"loadBalancer": {"ingress": [{"ip": format!("203.0.113.{}", 10 + i)}]}}}),
            );
        }
        self.api.put(
            SECRETS,
            json!({"apiVersion": "v1", "kind": "Secret",
                   "metadata": {"name": format!("{name}-root"), "namespace": NS},
                   "data": {"password": base64::engine::general_purpose::STANDARD.encode("rootpw")}}),
        );
    }
}

#[tokio::test]
async fn a_replication_cluster_goes_from_pending_to_active() {
    let w = world().await;
    w.api.put(SC, sprig_cluster("fra1-c7", "replication", 3));

    // Pass 1: the MariaDB, the backups and three node Services exist; it
    // isn't Ready.
    let o = w.reconcile("fra1-c7").await;
    assert_eq!(o.phase, Phase::Provisioning);
    let mdb = w.api.get(MDB, "fra1-c7").unwrap();
    assert_eq!(mdb["spec"]["replicas"], 3);
    assert_eq!(mdb["spec"]["replication"]["semiSyncWaitPoint"], "AfterSync");
    assert_eq!(
        mdb["metadata"]["ownerReferences"][0]["kind"],
        "SprigCluster"
    );
    assert_eq!(w.api.names(PB), ["fra1-c7-backup"]);
    assert_eq!(
        w.api.names(SVC),
        ["fra1-c7-node-0", "fra1-c7-node-1", "fra1-c7-node-2"]
    );
    let st = w.status("fra1-c7");
    assert_eq!(st["phase"], "Provisioning");
    assert!(st["message"].as_str().unwrap().contains("Ready"));
    assert_eq!(w.api.names(EVENTS).len(), 1, "one Event per phase change");

    // Pass 2: Ready, addresses, the root Secret: accounts, registration.
    w.bring_up("fra1-c7", 3);
    let o = w.reconcile("fra1-c7").await;
    assert_eq!(o.phase, Phase::Probing);
    let t = w.accounts.ensured.lock().unwrap()[0].clone();
    assert_eq!(t.root_password, "rootpw");
    assert_eq!(t.writer, "fra1-c7-primary.sprigdb.svc.cluster.local:3306");
    assert_eq!(t.nodes.len(), 3);
    let reg = w.registrar.calls.lock().unwrap()[0].clone();
    assert_eq!(
        reg.nodes
            .iter()
            .map(|n| n.addr.as_str())
            .collect::<Vec<_>>(),
        [
            "203.0.113.10:3306",
            "203.0.113.11:3306",
            "203.0.113.12:3306"
        ]
    );
    assert_eq!(
        reg.secrets.ctl,
        "file:/etc/sprigdb/creds/fra1-c7.toml#sprig_ctl"
    );
    // The credentials Secret holds the passwords the accounts got.
    let creds = w.api.get(SECRETS, "sprigdb-cluster-credentials").unwrap();
    let toml = String::from_utf8(
        base64::engine::general_purpose::STANDARD
            .decode(creds["data"]["fra1-c7.toml"].as_str().unwrap())
            .unwrap(),
    )
    .unwrap();
    assert!(toml.contains(&t.ctl_password) && toml.contains(&t.repl_password));
    assert_eq!(w.accounts.domains.lock().unwrap()["fra1-c7"], 7);
    let st = w.status("fra1-c7");
    assert_eq!(st["clusterId"], "fra1-c7");
    assert_eq!(st["clusterNumber"], 7);

    // Pass 3: Active. Pass 4: steady, nothing written.
    assert_eq!(w.reconcile("fra1-c7").await.phase, Phase::Active);
    w.api.clear_log();
    assert_eq!(w.reconcile("fra1-c7").await.phase, Phase::Active);
    assert_eq!(w.api.count("PATCH", "/"), 0, "no patch in a steady state");
    assert_eq!(
        w.api.count("PUT", "/"),
        0,
        "no status write in a steady state"
    );
    assert_eq!(w.api.count("POST", "/"), 0);
    assert_eq!(
        w.registrar.calls.lock().unwrap().len(),
        1,
        "registered once"
    );
    assert_eq!(w.accounts.ensured.lock().unwrap().len(), 1);
    // Provisioning, Probing, Active.
    assert_eq!(w.api.names(EVENTS).len(), 3);
}

#[tokio::test]
async fn scaling_patches_replicas_but_never_the_primary_or_my_cnf() {
    let w = world().await;
    w.api.put(SC, sprig_cluster("fra1-c8", "replication", 3));
    w.reconcile("fra1-c8").await;
    w.bring_up("fra1-c8", 3);
    w.reconcile("fra1-c8").await;
    // mariadb-operator failed the primary over to pod 2; someone edited
    // myCnf by hand.
    w.api.mutate(
        MDB,
        "fra1-c8",
        &json!({"spec": {"replication": {"primary": {"podIndex": 2}}, "myCnf": "[mariadbd]\n"}}),
    );
    w.api.mutate(
        SC,
        "fra1-c8",
        &json!({"spec": {"nodes": 5}, "metadata": {"generation": 2}}),
    );
    w.reconcile("fra1-c8").await;
    let mdb = w.api.get(MDB, "fra1-c8").unwrap();
    assert_eq!(mdb["spec"]["replicas"], 5);
    assert_eq!(mdb["spec"]["replication"]["primary"]["podIndex"], 2);
    assert_eq!(mdb["spec"]["myCnf"], "[mariadbd]\n");
    assert_eq!(w.api.names(SVC).len(), 5);
    // Back to 3: the extra Services go.
    w.api.mutate(
        SC,
        "fra1-c8",
        &json!({"spec": {"nodes": 3}, "metadata": {"generation": 3}}),
    );
    w.reconcile("fra1-c8").await;
    assert_eq!(
        w.api.names(SVC),
        ["fra1-c8-node-0", "fra1-c8-node-1", "fra1-c8-node-2"]
    );
    assert_eq!(w.api.get(MDB, "fra1-c8").unwrap()["spec"]["replicas"], 3);
}

#[tokio::test]
async fn not_ready_in_thirty_minutes_is_failed_and_a_spec_change_retries() {
    let w = world().await;
    w.api.put(SC, sprig_cluster("fra1-c9", "galera", 3));
    assert_eq!(w.reconcile("fra1-c9").await.phase, Phase::Provisioning);
    w.clock.advance(Duration::from_secs(29 * 60));
    assert_eq!(w.reconcile("fra1-c9").await.phase, Phase::Provisioning);
    w.clock.advance(Duration::from_secs(2 * 60));
    assert_eq!(w.reconcile("fra1-c9").await.phase, Phase::Failed);
    let st = w.status("fra1-c9");
    assert!(st["message"].as_str().unwrap().contains("30 min"), "{st}");
    let ev = w
        .api
        .names(EVENTS)
        .iter()
        .map(|n| w.api.get(EVENTS, n).unwrap())
        .find(|e| e["reason"] == "Failed")
        .unwrap();
    assert_eq!(ev["type"], "Warning");
    // A new generation starts a new attempt.
    w.api.mutate(
        SC,
        "fra1-c9",
        &json!({"spec": {"nodes": 3, "zones": ["a"]}, "metadata": {"generation": 2}}),
    );
    assert_eq!(w.reconcile("fra1-c9").await.phase, Phase::Provisioning);
}

#[tokio::test]
async fn failed_accounts_and_refused_registration_stay_bootstrapping() {
    let w = world().await;
    w.api.put(SC, sprig_cluster("solo", "single", 1));
    w.reconcile("solo").await;
    w.bring_up("solo", 1);
    *w.accounts.fail.lock().unwrap() = true;
    let o = w.reconcile("solo").await;
    assert_eq!(o.phase, Phase::Bootstrapping);
    assert!(
        w.status("solo")["message"]
            .as_str()
            .unwrap()
            .contains("TLS")
    );
    *w.accounts.fail.lock().unwrap() = false;
    *w.registrar.refuse.lock().unwrap() = true;
    let o = w.reconcile("solo").await;
    assert_eq!(o.phase, Phase::Bootstrapping);
    assert!(
        w.status("solo")["message"]
            .as_str()
            .unwrap()
            .contains("39511")
    );
    assert_eq!(
        w.accounts.ensured.lock().unwrap()[0].writer,
        "solo.sprigdb.svc.cluster.local:3306"
    );
    *w.registrar.refuse.lock().unwrap() = false;
    assert_eq!(w.reconcile("solo").await.phase, Phase::Probing);
}

#[tokio::test]
async fn pools_keep_their_minimum_and_provision_up_to_the_maximum() {
    let w = world().await;
    w.api.put(
        POOLS,
        json!({
            "apiVersion": "sprigdb.io/v1alpha1", "kind": "SprigClusterPool",
            "metadata": {"name": "default", "namespace": NS},
            "spec": {
                "template": {"topology": "replication", "nodes": 3,
                             "mariadb": {"image": "mariadb:12.3"}, "storage": {"size": "100Gi"}},
                "minClusters": 2, "maxClusters": 3,
            },
        }),
    );
    let mut due = HashMap::new();
    pass(&w.rec, &mut due).await.unwrap();
    assert_eq!(w.api.names(SC), ["default-1", "default-2"]);
    let c = w.api.get(SC, "default-1").unwrap();
    assert_eq!(c["metadata"]["labels"]["sprigdb.io/pool"], "default");
    assert_eq!(
        c["metadata"]["ownerReferences"][0]["kind"],
        "SprigClusterPool"
    );
    // The same pass reconciled them: their MariaDBs exist.
    assert_eq!(w.api.names(MDB), ["default-1", "default-2"]);
    assert_eq!(
        w.api.get(POOLS, "default").unwrap()["status"]["clusters"],
        2
    );

    // A cluster_provision job through the Provisioner trait.
    let prov = KubernetesProvisioner::new(Arc::clone(&w.rec));
    let spec = ClusterSpec {
        dc: sprig_common::ids::DcId::new("fra1").unwrap(),
        topology: Topology::PrimaryReplica,
        nodes: 3,
        version: MariaDbVersion {
            major: 12,
            minor: 3,
        },
        resources: Resources::default(),
        zones: vec![],
        backup_target: String::new(),
        pool: Some("default".into()),
    };
    let job = prov.create(spec.clone()).await.unwrap();
    let third = w.api.get(SC, "default-3").unwrap();
    assert_eq!(
        third["metadata"]["annotations"]["sprigdb.io/job-id"],
        job.to_string()
    );
    let e = prov.create(spec.clone()).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ProvisionNoHosts));
    assert!(e.to_string().contains("maxClusters 3"));

    // Without a pool: a standalone cluster from the spec.
    let mut solo = spec;
    solo.pool = None;
    solo.backup_target = "s3://b".into();
    prov.create(solo).await.unwrap();
    let names = w.api.names(SC);
    assert_eq!(names.len(), 4);
    let standalone = names.iter().find(|n| n.starts_with("fra1-")).unwrap();
    let s = w.api.get(SC, standalone).unwrap();
    assert_eq!(s["spec"]["mariadb"]["image"], "mariadb:12.3");
    assert_eq!(s["spec"]["storage"]["size"], "10Gi");

    // Retire: the SprigCluster goes, and its MariaDB and Services with it.
    w.bring_up("default-1", 3);
    w.reconcile("default-1").await;
    let id = w.status("default-1")["clusterId"]
        .as_str()
        .unwrap()
        .to_owned();
    // The StatefulSet's volume claims, which Kubernetes keeps.
    for (n, inst) in [
        ("storage-default-1-0", "default-1"),
        ("storage-default-1-1", "default-1"),
        ("storage-default-2-0", "default-2"),
    ] {
        w.api.put(
            PVCS,
            json!({"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                "metadata": {"name": n, "namespace": NS,
                    "labels": {"app.kubernetes.io/instance": inst}}}),
        );
    }
    prov.retire(&sprig_common::ids::ClusterId::new(id).unwrap())
        .await
        .unwrap();
    // The record was retired first (r2opfix).
    assert_eq!(w.registrar.retired.lock().unwrap().len(), 1);
    assert!(w.api.get(SC, "default-1").is_none());
    assert!(w.api.get(MDB, "default-1").is_none());
    assert!(!w.api.names(SVC).iter().any(|n| n.starts_with("default-1-")));
    assert_eq!(w.api.names(PVCS), vec!["storage-default-2-0".to_owned()]);
    assert!(w.api.get(SECRETS, "default-1-root").is_none());
    let creds = w.api.get(SECRETS, "sprigdb-cluster-credentials").unwrap();
    assert!(creds["data"]["default-1.toml"].is_null());
}

#[tokio::test]
async fn one_operator_leads_and_a_lapsed_lease_moves() {
    let w = world().await;
    let a = LeaseElector::new(
        Arc::clone(&w.kube),
        NS,
        "sprigdb-operator",
        "op-a",
        Duration::from_secs(15),
        w.clock.clone(),
    );
    let b = LeaseElector::new(
        Arc::clone(&w.kube),
        NS,
        "sprigdb-operator",
        "op-b",
        Duration::from_secs(15),
        w.clock.clone(),
    );
    assert!(a.try_lead().await.unwrap());
    assert!(!b.try_lead().await.unwrap());
    w.clock.advance(Duration::from_secs(10));
    assert!(a.try_lead().await.unwrap(), "renewed");
    w.clock.advance(Duration::from_secs(10));
    assert!(!b.try_lead().await.unwrap(), "renewed 10 s ago");
    w.clock.advance(Duration::from_secs(16));
    assert!(b.try_lead().await.unwrap(), "a stopped renewing");
    assert!(!a.try_lead().await.unwrap());
    let l = w
        .kube
        .get(&LEASE, NS, "sprigdb-operator")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(l["spec"]["holderIdentity"], "op-b");
    assert_eq!(l["spec"]["leaseTransitions"], 1);
    assert!(l["spec"]["renewTime"].as_str().unwrap().contains('.'));
}

#[tokio::test]
async fn a_stale_resource_version_is_a_conflict() {
    let w = world().await;
    w.api.put(SC, sprig_cluster("x", "single", 1));
    let raw = w
        .kube
        .get(&crd::SPRIG_CLUSTER, NS, "x")
        .await
        .unwrap()
        .unwrap();
    w.api
        .mutate(SC, "x", &json!({"metadata": {"labels": {"a": "b"}}}));
    let mut stale = raw.clone();
    stale["status"] = json!({"phase": "Active"});
    let e = w
        .kube
        .replace_status(&crd::SPRIG_CLUSTER, NS, "x", &stale)
        .await
        .unwrap_err();
    assert!(matches!(e, KubeError::Conflict(_)), "{e}");
    // The reconcile of a stale read fails and is retried by the loop.
    assert!(w.rec.reconcile(&raw).await.is_err());
    assert_eq!(w.reconcile("x").await.phase, Phase::Provisioning);
    let _ = w.clock.now();
}

/// r2close (r2kind's leftover): a pass the API refuses (mariadb-operator's
/// webhook) leaves its reason in the status and one `ReconcileError` Event,
/// instead of an empty phase; the next good pass clears it.
#[tokio::test]
async fn a_refused_pass_shows_its_reason_in_the_status() {
    let w = world().await;
    w.api.put(SC, sprig_cluster("fra1-c9", "replication", 3));
    w.api.state.lock().unwrap().refuse_create.insert(
        MDB.to_owned(),
        "spec.replication.replica.recovery needs bootstrapFrom".into(),
    );
    let raw = w
        .kube
        .get(&crd::SPRIG_CLUSTER, NS, "fra1-c9")
        .await
        .unwrap()
        .unwrap();
    assert!(w.rec.reconcile(&raw).await.is_err());
    let st = w.status("fra1-c9");
    assert_eq!(st["phase"], "Pending", "{st}");
    let msg = st["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("reconcile error") && msg.contains("bootstrapFrom"),
        "{st}"
    );
    let events = || {
        w.api
            .names(EVENTS)
            .iter()
            .filter_map(|n| w.api.get(EVENTS, n))
            .filter(|e| e["reason"] == "ReconcileError")
            .count()
    };
    assert_eq!(events(), 1);
    // The same refusal again: no new Event, no status write.
    let raw = w
        .kube
        .get(&crd::SPRIG_CLUSTER, NS, "fra1-c9")
        .await
        .unwrap()
        .unwrap();
    assert!(w.rec.reconcile(&raw).await.is_err());
    assert_eq!(events(), 1);
    // The webhook is fixed: the next pass proceeds and clears the message.
    w.api.state.lock().unwrap().refuse_create.clear();
    let o = w.reconcile("fra1-c9").await;
    assert_eq!(o.phase, Phase::Provisioning);
    let st = w.status("fra1-c9");
    assert!(
        !st["message"]
            .as_str()
            .unwrap_or_default()
            .contains("reconcile error"),
        "{st}"
    );
}
