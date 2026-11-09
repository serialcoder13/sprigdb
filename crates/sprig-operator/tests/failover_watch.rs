//! The failover watch (r2opfix) against the fake Kubernetes API server and
//! a fake replication seam: SprigDB promotes the most advanced replica, not
//! the next pod index; it waits for a second source; it stops when no
//! replica may be promoted; and it repairs, or marks for rebuild, replicas
//! a failover left broken.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

mod support;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use serde_json::{Value, json};
use sprig_common::time::ManualClock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_operator::OperatorConfig;
use sprig_operator::accounts::{BootstrapTarget, SystemAccounts};
use sprig_operator::crd;
use sprig_operator::failover::{ReplView, ReplicationOps};
use sprig_operator::kube::{HttpKube, KubeApi, KubeConfig};
use sprig_operator::reconcile::{Reconciler, WatchOutcome};
use sprig_operator::register::{ClusterRegistrar, ClusterRegistration, Registered};
use support::FakeApi;

const NS: &str = "sprigdb";
const SC: &str = "/apis/sprigdb.io/v1alpha1/namespaces/sprigdb/sprigclusters";
const MDB: &str = "/apis/k8s.mariadb.com/v1alpha1/namespaces/sprigdb/mariadbs";
const SECRETS: &str = "/api/v1/namespaces/sprigdb/secrets";
const PODS: &str = "/api/v1/namespaces/sprigdb/pods";
const EVENTS: &str = "/api/v1/namespaces/sprigdb/events";

struct NoAccounts;
#[async_trait]
impl SystemAccounts for NoAccounts {
    async fn ensure(&self, _: &BootstrapTarget) -> Result<()> {
        Ok(())
    }
    async fn ensure_domain(&self, _: &BootstrapTarget, _: u32) -> Result<usize> {
        Ok(0)
    }
}
struct NoRegistrar;
#[async_trait]
impl ClusterRegistrar for NoRegistrar {
    async fn register(&self, _: &ClusterRegistration) -> Result<Registered> {
        Err(SprigError::internal("not used"))
    }
}

/// Pods by address: their state, whether they answer, what a drain does.
#[derive(Default)]
struct FakeRepl {
    views: Mutex<HashMap<String, ReplView>>,
    down: Mutex<HashSet<String>>,
    /// Pods whose SQL thread can't catch up within the drain.
    stuck: Mutex<HashSet<String>>,
    drained: Mutex<Vec<String>>,
    resumed: Mutex<Vec<String>>,
    repaired: Mutex<Vec<String>>,
    in_history: Mutex<bool>,
}

fn addr(i: u8) -> String {
    format!("kc-{i}.kc-internal.sprigdb.svc.cluster.local:3306")
}

impl FakeRepl {
    fn set(&self, i: u8, v: ReplView) {
        self.views.lock().unwrap().insert(addr(i), v);
    }
    fn answer(&self, a: &str) -> Result<ReplView> {
        if self.down.lock().unwrap().contains(a) {
            return Err(SprigError::new(
                SprigCode::ClusterUnavailable,
                "connect timed out",
            ));
        }
        Ok(self
            .views
            .lock()
            .unwrap()
            .get(a)
            .cloned()
            .unwrap_or_default())
    }
}

#[async_trait]
impl ReplicationOps for FakeRepl {
    async fn view(&self, a: &str, _: &str, _: u8) -> Result<ReplView> {
        self.answer(a)
    }
    async fn drain(&self, a: &str, _: &str, _: u8, _: Duration) -> Result<ReplView> {
        let mut v = self.answer(a)?;
        self.drained.lock().unwrap().push(a.to_owned());
        v.io_running = false;
        v.sql_running = true;
        if !self.stuck.lock().unwrap().contains(a) {
            v.slave_pos = v.received().to_string();
        }
        self.views.lock().unwrap().insert(a.to_owned(), v.clone());
        Ok(v)
    }
    async fn resume(&self, a: &str, _: &str) -> Result<()> {
        self.resumed.lock().unwrap().push(a.to_owned());
        Ok(())
    }
    async fn repair(&self, a: &str, _: &str) -> Result<()> {
        self.repaired.lock().unwrap().push(a.to_owned());
        let mut views = self.views.lock().unwrap();
        if let Some(v) = views.get_mut(a) {
            v.slave_pos = v.binlog_pos.clone();
            v.sql_running = true;
            v.io_running = true;
            v.sql_errno = 0;
        }
        Ok(())
    }
    async fn in_history(&self, _: &str, _: &str, _: &str) -> Result<bool> {
        Ok(*self.in_history.lock().unwrap())
    }
}

fn replica(index: u8, io: &str, applied: &str) -> ReplView {
    ReplView {
        index,
        master_host: "kc-0.kc-internal".into(),
        io_running: true,
        sql_running: true,
        io_pos: io.into(),
        slave_pos: applied.into(),
        binlog_pos: applied.into(),
        read_only: true,
        ..ReplView::default()
    }
}

struct World {
    api: FakeApi,
    kube: Arc<dyn KubeApi>,
    repl: Arc<FakeRepl>,
    rec: Reconciler,
}

async fn world(detect: Duration) -> World {
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
    let mut cfg = OperatorConfig::new(NS);
    cfg.failover_detect = detect;
    let repl = Arc::new(FakeRepl::default());
    *repl.in_history.lock().unwrap() = true;
    let rec = Reconciler::new(
        Arc::clone(&kube),
        Arc::new(NoAccounts),
        Arc::new(NoRegistrar),
        cfg,
        Arc::new(ManualClock::new()),
    )
    .with_replication(repl.clone());
    api.put(
        SC,
        json!({
            "apiVersion": "sprigdb.io/v1alpha1", "kind": "SprigCluster",
            "metadata": {"name": "kc", "namespace": NS},
            "spec": {"topology": "replication", "nodes": 3, "mariadb": {"image": "mariadb:11.8"},
                     "storage": {"size": "1Gi"}},
            "status": {"clusterId": "kind1-c2", "clusterNumber": 2, "phase": "Active"},
        }),
    );
    api.put(
        MDB,
        json!({
            "apiVersion": "k8s.mariadb.com/v1alpha1", "kind": "MariaDB",
            "metadata": {"name": "kc", "namespace": NS},
            "spec": {"replication": {"primary": {"podIndex": 0, "autoFailover": false}}},
            "status": {"currentPrimaryPodIndex": 0},
        }),
    );
    api.put(
        SECRETS,
        json!({"apiVersion": "v1", "kind": "Secret",
               "metadata": {"name": "kc-root", "namespace": NS},
               "data": {"password": base64::engine::general_purpose::STANDARD.encode("rootpw")}}),
    );
    for i in 0..3u8 {
        api.put(PODS, pod(i, true));
    }
    World {
        api,
        kube,
        repl,
        rec,
    }
}

fn pod(i: u8, ready: bool) -> Value {
    json!({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": format!("kc-{i}"), "namespace": NS},
           "status": {"conditions": [{"type": "Ready", "status": if ready { "True" } else { "False" }}]}})
}

impl World {
    async fn watch(&self) -> WatchOutcome {
        let raw = self
            .kube
            .get(&crd::SPRIG_CLUSTER, NS, "kc")
            .await
            .unwrap()
            .unwrap();
        self.rec.watch(&raw).await.unwrap()
    }
    fn pod_index(&self) -> Value {
        self.api.get(MDB, "kc").unwrap()["spec"]["replication"]["primary"]["podIndex"].clone()
    }
    fn events(&self, reason: &str) -> Vec<String> {
        self.api
            .names(EVENTS)
            .iter()
            .filter_map(|n| self.api.get(EVENTS, n))
            .filter(|e| e["reason"] == reason)
            .map(|e| e["message"].as_str().unwrap_or_default().to_owned())
            .collect()
    }
}

#[tokio::test]
async fn the_most_advanced_replica_is_promoted_not_the_next_index() {
    let w = world(Duration::ZERO).await;
    // Pod 1 (the next index, which mariadb-operator would take on a tie and
    // which applied everything it got) received less than pod 2, whose SQL
    // thread lags: mariadb-operator would skip pod 2 for its relay log.
    w.repl.set(
        0,
        ReplView {
            index: 0,
            binlog_pos: "2-10-130".into(),
            ..ReplView::default()
        },
    );
    w.repl.set(1, replica(1, "2-10-100", "2-10-100"));
    w.repl.set(2, replica(2, "2-10-130", "2-10-118"));
    assert_eq!(w.watch().await, WatchOutcome::Healthy { repaired: vec![] });

    // The primary stops answering, but its pod still looks Ready: one
    // source isn't enough.
    w.repl.down.lock().unwrap().insert(addr(0));
    assert_eq!(w.watch().await, WatchOutcome::Suspect);
    assert_eq!(w.pod_index(), json!(0));
    // The pod goes (deleted): both sources agree.
    w.api.put(PODS, pod(0, false));
    assert_eq!(w.watch().await, WatchOutcome::FailedOver(2));
    assert_eq!(w.pod_index(), json!(2));
    // Both replicas applied what they received before the choice.
    assert_eq!(w.repl.drained.lock().unwrap().len(), 2);
    let ev = w.events("Failover");
    assert_eq!(ev.len(), 1);
    assert!(
        ev[0].contains("pod 2 holds every replica's transactions (2-10-130"),
        "{}",
        ev[0]
    );
    // While mariadb-operator switches, the watch waits.
    assert_eq!(w.watch().await, WatchOutcome::Switching);
}

#[tokio::test]
async fn detection_waits_and_a_returning_primary_keeps_its_role() {
    let w = world(Duration::from_secs(3600)).await;
    w.repl.set(1, replica(1, "2-10-5", "2-10-5"));
    w.repl.set(2, replica(2, "2-10-5", "2-10-5"));
    w.repl.down.lock().unwrap().insert(addr(0));
    w.api.put(PODS, pod(0, false));
    // Not down for an hour yet.
    assert_eq!(w.watch().await, WatchOutcome::Suspect);
    assert_eq!(w.pod_index(), json!(0));
    // It answers again: healthy, nothing drained.
    w.repl.down.lock().unwrap().clear();
    assert!(matches!(w.watch().await, WatchOutcome::Healthy { .. }));
    assert!(w.repl.drained.lock().unwrap().is_empty());
}

#[tokio::test]
async fn no_safe_candidate_blocks_and_resumes_the_replicas() {
    let w = world(Duration::ZERO).await;
    // Pod 2 received more than it can apply in the drain: promoting pod 1
    // loses pod 2's acknowledged commits; promoting pod 2 drops its relay
    // log. Stop, report once, and let the replicas receive again.
    w.repl.set(1, replica(1, "2-10-100", "2-10-100"));
    w.repl.set(2, replica(2, "2-10-130", "2-10-118"));
    w.repl.stuck.lock().unwrap().insert(addr(2));
    w.repl.down.lock().unwrap().insert(addr(0));
    w.api.put(PODS, pod(0, false));
    let o = w.watch().await;
    assert!(
        matches!(&o, WatchOutcome::Blocked(why) if why.contains("applied only 2-10-118")),
        "{o:?}"
    );
    assert_eq!(w.pod_index(), json!(0));
    assert_eq!(w.repl.resumed.lock().unwrap().len(), 2);
    let _ = w.watch().await;
    assert_eq!(w.events("FailoverBlocked").len(), 1, "reported once");
}

#[tokio::test]
async fn broken_replicas_are_repaired_only_when_provably_safe() {
    let w = world(Duration::ZERO).await;
    // After a failover to pod 2: pod 1 replayed pod 2's binlog from its
    // start (gtid_slave_pos reset) and stopped with 1950; pod 0, the old
    // primary, holds an unacknowledged tail pod 2 never got.
    w.api.mutate(
        MDB,
        "kc",
        &json!({"spec": {"replication": {"primary": {"podIndex": 2}}},
                "status": {"currentPrimaryPodIndex": 2}}),
    );
    w.repl.set(
        2,
        ReplView {
            index: 2,
            binlog_pos: "0-10-4,2-12-140".into(),
            ..ReplView::default()
        },
    );
    let mut one = replica(1, "", "");
    one.binlog_pos = "0-10-4,2-10-100".into();
    one.sql_running = false;
    one.sql_errno = 1950;
    w.repl.set(1, one);
    let mut zero = replica(0, "", "");
    zero.binlog_pos = "0-10-4,2-10-141".into();
    zero.io_running = false;
    zero.io_errno = 1236;
    w.repl.set(0, zero);

    assert_eq!(w.watch().await, WatchOutcome::Healthy { repaired: vec![1] });
    assert_eq!(*w.repl.repaired.lock().unwrap(), vec![addr(1)]);
    assert_eq!(w.events("ReplicaRepaired").len(), 1);
    let rebuild = w.events("NodeNeedsRebuild");
    assert_eq!(rebuild.len(), 1);
    assert!(
        rebuild[0].contains("pod 0") && rebuild[0].contains("past the primary"),
        "{}",
        rebuild[0]
    );
    // Marked once; never repaired.
    assert_eq!(w.watch().await, WatchOutcome::Healthy { repaired: vec![] });
    assert_eq!(w.events("NodeNeedsRebuild").len(), 1);
    assert_eq!(w.repl.repaired.lock().unwrap().len(), 1);

    // Within by numbers but the primary's binlog refuses the position:
    // rebuild, not repair.
    *w.repl.in_history.lock().unwrap() = false;
    let mut one = replica(1, "", "");
    one.binlog_pos = "0-10-4,2-10-100".into();
    one.sql_running = false;
    one.sql_errno = 1950;
    w.repl.set(1, one);
    assert_eq!(w.watch().await, WatchOutcome::Healthy { repaired: vec![] });
    assert!(
        w.events("NodeNeedsRebuild")
            .iter()
            .any(|m| m.contains("pod 1"))
    );
}

#[tokio::test]
async fn operator_driven_and_unregistered_clusters_are_not_watched() {
    let w = world(Duration::ZERO).await;
    w.api
        .mutate(SC, "kc", &json!({"spec": {"failoverDriver": "operator"}}));
    w.repl.down.lock().unwrap().insert(addr(0));
    assert_eq!(w.watch().await, WatchOutcome::Skipped);
}
