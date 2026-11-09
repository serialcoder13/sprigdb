//! `reconcile(SprigCluster)` and the pool loop (chapter 27, "The
//! Kubernetes operator").
//!
//! ```text
//! reconcile(SprigCluster sc):
//! 1. build the MariaDB resource from the mapping table; create or patch it with an owner
//!    reference to sc; never touch replication.primary.podIndex after creation
//! 2. ensure the PhysicalBackup and PointInTimeRecovery resources for sc.backup
//! 3. ensure one Service per pod (values.nodeServices.type) and read its advertised address
//! 4. when the MariaDB resource is Ready: create sprig_ctl and sprig_repl over TLS, using
//!    the root credentials in mariadb-operator's Secret
//! 5. register or update the cluster record through the DC controller (nodes, addresses,
//!    provisioner = kubernetes, failover_driver); probes start
//! 6. status.phase = Active
//! pools: create SprigClusters from the pool template within maxClusters; minClusters are
//! created at install
//! ```
//!
//! Phases: `Pending` → `Provisioning` (the `MariaDB` isn't Ready, or a node
//! Service has no address yet) → `Bootstrapping` (accounts) → `Probing`
//! (registered) → `Active`; `Failed` when the `MariaDB` isn't Ready within
//! `ready_timeout` (30 minutes) of the attempt's start. A failed cluster is
//! retried: the next pass starts a new attempt when the spec changes
//! (`observedGeneration`). Every phase change is a Kubernetes Event on the
//! `SprigCluster`.
//!
//! Credentials: the passwords of `sprig_ctl` and `sprig_repl` are generated
//! once and kept in one Secret (`credentialsSecret`, key `<cluster>.toml`),
//! mounted at `/etc/sprigdb/creds` on controllers and workers, so the
//! record's references are `file:/etc/sprigdb/creds/<cluster>.toml#sprig_ctl`
//! and `#sprig_repl` (resolved at every connection, as for external
//! clusters).

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, UNIX_EPOCH};

use base64::Engine as _;
use serde_json::{Value, json};
use sprig_cluster::record::{NodeSpec, SecretRefs, TopologyKind};
use sprig_common::ids::NodeId;
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};

use crate::OperatorConfig;
use crate::accounts::{self, BootstrapTarget, SystemAccounts};
use crate::crd::{
    self, FailoverDriver, FailoverRecord, NodeAddress, POOL_LABEL, Phase, SPRIG_CLUSTER,
    SprigCluster, SprigClusterPool, SprigClusterStatus, Topology,
};
use crate::failover::{self, Repair, ReplView, ReplicationOps};
use crate::kube::{self, ApiResource, EVENT, KubeApi, KubeError, POD, SECRET, SERVICE};
use crate::mapping::{self, MARIADB};
use crate::register::{ClusterRegistrar, ClusterRegistration};

/// What one pass decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    /// The phase written.
    pub phase: Phase,
    /// When to look again.
    pub requeue: Duration,
}

/// Runs reconcile passes.
pub struct Reconciler {
    kube: Arc<dyn KubeApi>,
    accounts: Arc<dyn SystemAccounts>,
    registrar: Arc<dyn ClusterRegistrar>,
    cfg: OperatorConfig,
    clock: Arc<dyn Clock>,
    /// The failover watch's SQL; `None`: no watch ([`Self::watch`]).
    repl: Option<Arc<dyn ReplicationOps>>,
    watches: Mutex<HashMap<String, ClusterWatch>>,
}

/// What the failover watch remembers about one cluster.
#[derive(Debug, Default, Clone)]
struct ClusterWatch {
    /// Since when the primary hasn't answered.
    down_since: Option<Instant>,
    /// The last `FailoverBlocked` reason reported (reported once).
    blocked: Option<String>,
    /// Nodes marked for rebuild, by pod index.
    rebuild: BTreeSet<u8>,
    /// The last failover, for the status.
    last: Option<FailoverRecord>,
}

/// What one watch pass did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchOutcome {
    /// Not watched (not a registered `sprig`-driven replication cluster).
    Skipped,
    /// The primary answers; `repaired` replicas were repaired.
    Healthy {
        /// Replicas repaired in this pass.
        repaired: Vec<u8>,
    },
    /// The primary doesn't answer, not yet for long enough (or its pod
    /// still looks Ready).
    Suspect,
    /// mariadb-operator is switching the primary.
    Switching,
    /// SprigDB set `podIndex` to this pod.
    FailedOver(u8),
    /// No replica may be promoted (the reason is in an Event).
    Blocked(String),
}

impl std::fmt::Debug for Reconciler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reconciler")
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

fn unix(clock: &dyn Clock) -> u64 {
    clock
        .wall()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn kerr(what: &str, e: &KubeError) -> SprigError {
    SprigError::new(SprigCode::Internal, format!("kubernetes API, {what}: {e}"))
}

/// The record's topology.
#[must_use]
pub fn topology_kind(t: Topology) -> TopologyKind {
    match t {
        Topology::Galera => TopologyKind::Galera,
        Topology::Replication => TopologyKind::PrimaryReplica,
        Topology::Single => TopologyKind::SingleNode,
    }
}

impl Reconciler {
    /// A reconciler over `kube`.
    #[must_use]
    pub fn new(
        kube: Arc<dyn KubeApi>,
        accounts: Arc<dyn SystemAccounts>,
        registrar: Arc<dyn ClusterRegistrar>,
        cfg: OperatorConfig,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            kube,
            accounts,
            registrar,
            cfg,
            clock,
            repl: None,
            watches: Mutex::new(HashMap::new()),
        }
    }

    /// With the failover watch over `repl` ([`Self::watch`]).
    #[must_use]
    pub fn with_replication(mut self, repl: Arc<dyn ReplicationOps>) -> Self {
        self.repl = Some(repl);
        self
    }

    /// The configuration.
    #[must_use]
    pub fn config(&self) -> &OperatorConfig {
        &self.cfg
    }

    /// The Kubernetes client.
    #[must_use]
    pub fn kube(&self) -> &Arc<dyn KubeApi> {
        &self.kube
    }

    /// The DC controller's registration calls.
    #[must_use]
    pub fn registrar(&self) -> &Arc<dyn ClusterRegistrar> {
        &self.registrar
    }

    /// Creates `obj` if missing; else merge-patches `patch` (default: the
    /// whole object's labels and spec) when the live object lacks it.
    /// Returns the live object.
    async fn ensure(
        &self,
        r: &ApiResource,
        ns: &str,
        obj: &Value,
        patch: Option<Value>,
    ) -> Result<Value> {
        let name = obj["metadata"]["name"].as_str().unwrap_or_default();
        match self.kube.get(r, ns, name).await {
            Ok(None) => match self.kube.create(r, ns, obj).await {
                Ok(v) => {
                    metrics::counter!("sprig_operator_objects_total", "kind" => r.kind, "op" => "create").increment(1);
                    Ok(v)
                }
                Err(KubeError::AlreadyExists) => self
                    .kube
                    .get(r, ns, name)
                    .await
                    .map_err(|e| kerr("get", &e))?
                    .ok_or_else(|| SprigError::internal(format!("{} {name} vanished", r.kind))),
                Err(e) => Err(kerr(&format!("create {} {name}", r.kind), &e)),
            },
            Ok(Some(live)) => {
                let patch = patch.unwrap_or_else(|| {
                    json!({"metadata": {"labels": obj["metadata"]["labels"]}, "spec": obj["spec"]})
                });
                if kube::contains(&live, &patch) {
                    return Ok(live);
                }
                metrics::counter!("sprig_operator_objects_total", "kind" => r.kind, "op" => "patch").increment(1);
                self.kube
                    .merge_patch(r, ns, name, &patch)
                    .await
                    .map_err(|e| kerr(&format!("patch {} {name}", r.kind), &e))
            }
            Err(e) => Err(kerr(&format!("get {} {name}", r.kind), &e)),
        }
    }

    async fn secret_value(&self, ns: &str, name: &str, key: &str) -> Result<Option<String>> {
        let Some(s) = self
            .kube
            .get(&SECRET, ns, name)
            .await
            .map_err(|e| kerr("get secret", &e))?
        else {
            return Ok(None);
        };
        Ok(s["data"][key]
            .as_str()
            .and_then(|b| base64::engine::general_purpose::STANDARD.decode(b).ok())
            .and_then(|b| String::from_utf8(b).ok()))
    }

    /// The cluster's passwords: read from the credentials Secret, or
    /// generated and written there.
    async fn credentials(&self, ns: &str, cluster: &str) -> Result<(String, String)> {
        let key = format!("{cluster}.toml");
        if let Some(t) = self
            .secret_value(ns, &self.cfg.credentials_secret, &key)
            .await?
        {
            let v: toml::Value = toml::from_str(&t)
                .map_err(|e| SprigError::internal(format!("credentials {key}: {e}")))?;
            let pw = |s: &str| v[s]["password"].as_str().map(str::to_owned);
            if let (Some(c), Some(r)) = (pw("sprig_ctl"), pw("sprig_repl")) {
                return Ok((c, r));
            }
        }
        let (c, r) = (
            accounts::generate_password()?,
            accounts::generate_password()?,
        );
        let toml = accounts::credentials_toml(&c, &r, self.cfg.mariadb_ca_file.as_deref());
        let encoded = base64::engine::general_purpose::STANDARD.encode(toml);
        let secret = json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": self.cfg.credentials_secret,
                "namespace": ns,
                "labels": {crd::MANAGED_BY.0: crd::MANAGED_BY.1},
            },
            "type": "Opaque",
            "data": {key.clone(): encoded.clone()},
        });
        match self.kube.create(&SECRET, ns, &secret).await {
            Ok(_) => {}
            Err(KubeError::AlreadyExists) => {
                self.kube
                    .merge_patch(
                        &SECRET,
                        ns,
                        &self.cfg.credentials_secret,
                        &json!({"data": {key: encoded}}),
                    )
                    .await
                    .map_err(|e| kerr("patch credentials", &e))?;
            }
            Err(e) => return Err(kerr("create credentials", &e)),
        }
        Ok((c, r))
    }

    fn writer_addr(&self, sc: &SprigCluster, ns: &str) -> String {
        let name = &sc.metadata.name;
        let svc = if sc.spec.topology == Topology::Single {
            name.clone()
        } else {
            format!("{name}-primary")
        };
        format!("{svc}.{ns}.svc.{}:3306", self.cfg.cluster_domain)
    }

    fn pod_addrs(&self, sc: &SprigCluster, ns: &str) -> Vec<String> {
        let name = &sc.metadata.name;
        (0..sc.spec.replicas())
            .map(|i| {
                format!(
                    "{name}-{i}.{name}-internal.{ns}.svc.{}:3306",
                    self.cfg.cluster_domain
                )
            })
            .collect()
    }

    async fn event(&self, sc: &SprigCluster, ns: &str, reason: &str, message: &str) {
        let now = sprig_cluster::record::rfc3339(self.clock.wall());
        let ev = json!({
            "apiVersion": "v1",
            "kind": "Event",
            "metadata": {"generateName": format!("{}.", sc.metadata.name), "namespace": ns},
            "involvedObject": {
                "apiVersion": crd::API_VERSION, "kind": "SprigCluster",
                "name": sc.metadata.name, "namespace": ns,
                "uid": sc.metadata.uid.clone().unwrap_or_default(),
            },
            "reason": reason,
            "message": message,
            "type": if reason == "Failed" { "Warning" } else { "Normal" },
            "firstTimestamp": now, "lastTimestamp": now, "count": 1,
            "source": {"component": "sprigdb-operator"},
        });
        if let Err(e) = self.kube.create(&EVENT, ns, &ev).await {
            tracing::debug!(error = %e, "event not recorded");
        }
    }

    async fn write_status(
        &self,
        raw: &Value,
        sc: &SprigCluster,
        ns: &str,
        mut status: SprigClusterStatus,
    ) -> Result<()> {
        let old = sc.status.clone().unwrap_or_default();
        status.observed_generation = sc.metadata.generation;
        if status.phase == old.phase {
            status
                .last_transition_time
                .clone_from(&old.last_transition_time);
        } else {
            status.last_transition_time = Some(sprig_cluster::record::rfc3339(self.clock.wall()));
            let msg = status
                .message
                .clone()
                .unwrap_or_else(|| format!("phase {}", status.phase.as_str()));
            self.event(sc, ns, status.phase.as_str(), &msg).await;
            metrics::counter!("sprig_operator_phase_changes_total", "phase" => status.phase.as_str()).increment(1);
        }
        if status == old {
            return Ok(());
        }
        let mut obj = raw.clone();
        obj["status"] = serde_json::to_value(&status)
            .map_err(|e| SprigError::internal(format!("status: {e}")))?;
        self.kube
            .replace_status(&SPRIG_CLUSTER, ns, &sc.metadata.name, &obj)
            .await
            .map_err(|e| kerr("status", &e))?;
        Ok(())
    }

    /// One pass over the `SprigCluster` `raw` (see the module docs).
    ///
    /// # Errors
    /// API failures; the caller requeues.
    pub async fn reconcile(&self, raw: &Value) -> Result<Outcome> {
        let r = self.pass(raw).await;
        let result = match &r {
            Ok(o) if o.phase == Phase::Failed => "failed",
            Ok(_) => "ok",
            Err(_) => "error",
        };
        metrics::counter!("sprig_operator_reconcile_total", "kind" => "SprigCluster", "result" => result).increment(1);
        r
    }

    #[allow(clippy::too_many_lines)] // the six steps, in order
    async fn pass(&self, raw: &Value) -> Result<Outcome> {
        let sc: SprigCluster = serde_json::from_value(raw.clone())
            .map_err(|e| SprigError::internal(format!("bad SprigCluster: {e}")))?;
        let ns = sc
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| self.cfg.namespace.clone());
        let name = sc.metadata.name.clone();
        let old = sc.status.clone().unwrap_or_default();
        if sc.metadata.deletion_timestamp.is_some() {
            // Owner references delete everything it owns.
            return Ok(Outcome {
                phase: old.phase,
                requeue: Duration::from_secs(60),
            });
        }
        let now = unix(&*self.clock);
        let mut status = old.clone();
        status.message = None;
        let new_attempt = old.started_unix.is_none()
            || (old.phase == Phase::Failed && old.observed_generation != sc.metadata.generation);
        if new_attempt {
            status.started_unix = Some(now);
        }
        let started = status.started_unix.unwrap_or(now);

        // 1. The MariaDB resource.
        let desired = mapping::mariadb(&sc, &self.cfg);
        let mdb = self
            .ensure(
                &MARIADB,
                &ns,
                &desired,
                Some(mapping::update_patch(&desired)),
            )
            .await?;
        // The domain ConfigMap the pods mount (r2opfix), before the pods
        // start; the domain once the cluster has a number.
        let cm = mapping::domain_config_map(&sc, old.cluster_number.filter(|n| *n > 0));
        self.ensure(
            &mapping::CONFIG_MAP,
            &ns,
            &cm,
            Some(json!({"data": cm["data"]})),
        )
        .await?;
        // 2. Backups and the binlog archive.
        for (r, obj) in mapping::backups(&sc) {
            self.ensure(&r, &ns, &obj, None).await?;
        }
        // 3. One Service per pod; the ones past `nodes` go.
        let mut addrs = Vec::new();
        for svc in mapping::node_services(&sc, &self.cfg) {
            let live = self.ensure(&SERVICE, &ns, &svc, None).await?;
            addrs.push(mapping::advertised_for(&live, &self.cfg));
        }
        let keep: Vec<String> = (0..sc.spec.replicas())
            .map(|i| mapping::node_service_name(&name, i))
            .collect();
        for s in self
            .kube
            .list(
                &SERVICE,
                &ns,
                Some(&format!("{}={name}", crd::CLUSTER_LABEL)),
            )
            .await
            .map_err(|e| kerr("list services", &e))?
        {
            let n = s["metadata"]["name"].as_str().unwrap_or_default();
            if s["metadata"]["labels"]["sprigdb.io/pod-index"].is_string()
                && !keep.iter().any(|k| k == n)
            {
                let _ = self.kube.delete(&SERVICE, &ns, n).await;
            }
        }
        // 4. Ready?
        let waiting = if !mapping::is_ready(&mdb) {
            Some("waiting for the MariaDB resource to be Ready".to_owned())
        } else if addrs.iter().any(Option::is_none) {
            Some("waiting for the node Services' addresses".to_owned())
        } else {
            None
        };
        if let Some(why) = waiting {
            let timeout = self.cfg.ready_timeout.as_secs();
            if now.saturating_sub(started) >= timeout && old.cluster_id.is_none() {
                status.phase = Phase::Failed;
                status.message = Some(format!("{why} for {} min", timeout / 60));
            } else {
                status.phase = if old.cluster_id.is_some() {
                    // A registered cluster that lost readiness keeps its
                    // record; the controller's probes see the rest.
                    old.phase
                } else {
                    Phase::Provisioning
                };
                status.message = Some(why);
                // The record follows the nodes even while not Ready (a
                // broken replica can keep the MariaDB from being Ready).
                if old.cluster_id.is_some() && addrs.iter().all(Option::is_some) {
                    let nodes: Vec<NodeAddress> = addrs
                        .iter()
                        .flatten()
                        .enumerate()
                        .map(|(i, a)| NodeAddress {
                            node: format!("n{i}"),
                            addr: a.clone(),
                        })
                        .collect();
                    if status.nodes != nodes {
                        let msg = status.message.take();
                        self.sync_nodes(&name, &mut status, &nodes).await;
                        status.message = status.message.take().or(msg);
                    }
                }
                // A pod restarted after a failover boots in domain 0, and the
                // cluster may stay not Ready for long (a broken replica):
                // keep the domain on every node that answers (kind, r2kind).
                if let Some(n) = old.cluster_number.filter(|n| *n > 0)
                    && old.cluster_id.is_some()
                {
                    self.domain_while_waiting(&sc, &ns, n).await;
                }
            }
            self.write_status(raw, &sc, &ns, status.clone()).await?;
            return Ok(Outcome {
                phase: status.phase,
                requeue: Duration::from_secs(10),
            });
        }
        let addrs: Vec<String> = addrs.into_iter().flatten().collect();
        // 5 (the book's 4). System accounts.
        let Some(root) = self
            .secret_value(&ns, &format!("{name}-root"), "password")
            .await?
        else {
            status.phase = Phase::Bootstrapping;
            status.message = Some(format!("waiting for the root Secret {name}-root"));
            self.write_status(raw, &sc, &ns, status).await?;
            return Ok(Outcome {
                phase: Phase::Bootstrapping,
                requeue: Duration::from_secs(10),
            });
        };
        let (ctl, repl) = self.credentials(&ns, &name).await?;
        let target = BootstrapTarget {
            cluster: name.clone(),
            writer: self.writer_addr(&sc, &ns),
            nodes: self.pod_addrs(&sc, &ns),
            root_password: root,
            ctl_password: ctl,
            repl_password: repl,
        };
        if old.cluster_id.is_none()
            && let Err(e) = self.accounts.ensure(&target).await
        {
            status.phase = Phase::Bootstrapping;
            status.message = Some(e.to_string());
            self.write_status(raw, &sc, &ns, status).await?;
            return Ok(Outcome {
                phase: Phase::Bootstrapping,
                requeue: Duration::from_secs(15),
            });
        }
        // 6 (the book's 5). Registration.
        let nodes: Vec<NodeAddress> = addrs
            .iter()
            .enumerate()
            .map(|(i, a)| NodeAddress {
                node: format!("n{i}"),
                addr: a.clone(),
            })
            .collect();
        if status.cluster_id.is_none() {
            let refs = format!("file:{}/{name}.toml", self.cfg.creds_dir);
            let reg = ClusterRegistration {
                name: name.clone(),
                topology: topology_kind(sc.spec.topology),
                nodes: nodes
                    .iter()
                    .map(|n| {
                        Ok(NodeSpec {
                            node: NodeId::new(n.node.clone())
                                .map_err(|e| SprigError::internal(e.to_string()))?,
                            addr: n.addr.clone(),
                            zone: None,
                        })
                    })
                    .collect::<Result<_>>()?,
                secrets: SecretRefs {
                    ctl: format!("{refs}#sprig_ctl"),
                    repl: format!("{refs}#sprig_repl"),
                },
                failover_driver: sc.spec.failover_driver,
            };
            match self.registrar.register(&reg).await {
                Ok(r) => {
                    status.cluster_id = Some(r.cluster_id);
                    status.cluster_number = Some(r.cluster_number);
                    status.nodes.clone_from(&nodes);
                    if r.cluster_number > 0 {
                        let cm = mapping::domain_config_map(&sc, Some(r.cluster_number));
                        self.ensure(
                            &mapping::CONFIG_MAP,
                            &ns,
                            &cm,
                            Some(json!({"data": cm["data"]})),
                        )
                        .await?;
                    }
                }
                Err(e) => {
                    status.phase = Phase::Bootstrapping;
                    status.message = Some(format!("registration: {e}"));
                    self.write_status(raw, &sc, &ns, status).await?;
                    return Ok(Outcome {
                        phase: Phase::Bootstrapping,
                        requeue: Duration::from_secs(30),
                    });
                }
            }
        } else if status.nodes != nodes {
            self.sync_nodes(&name, &mut status, &nodes).await;
        }
        // The cluster number is the GTID domain, set live on every node.
        let mut requeue = Duration::from_secs(30);
        if let Some(n) = status.cluster_number.filter(|n| *n > 0) {
            match self.accounts.ensure_domain(&target, n).await {
                Ok(changed) if changed > 0 => {
                    tracing::info!(cluster = %name, changed, domain = n, "gtid_domain_id set live");
                }
                Ok(_) => {}
                Err(e) => {
                    status.message = Some(format!("gtid_domain_id: {e}"));
                    requeue = Duration::from_secs(10);
                }
            }
        }
        // What the failover watch found.
        if let Some(w) = self
            .watches
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&name)
        {
            status.rebuild = w.rebuild.iter().map(|i| format!("n{i}")).collect();
            if w.last.is_some() {
                status.last_failover.clone_from(&w.last);
            }
        }
        // 7 (the book's 6).
        status.phase = if old.phase == Phase::Active || old.phase == Phase::Probing {
            Phase::Active
        } else {
            Phase::Probing
        };
        let phase = status.phase;
        self.write_status(raw, &sc, &ns, status).await?;
        Ok(Outcome {
            phase,
            requeue: if phase == Phase::Probing {
                Duration::from_secs(1)
            } else {
                requeue
            },
        })
    }

    /// Best effort: `gtid_domain_id` on the nodes of a registered cluster
    /// whose `MariaDB` isn't Ready.
    async fn domain_while_waiting(&self, sc: &SprigCluster, ns: &str, domain: u32) {
        let name = &sc.metadata.name;
        let Ok(Some(root)) = self
            .secret_value(ns, &format!("{name}-root"), "password")
            .await
        else {
            return;
        };
        let target = BootstrapTarget {
            cluster: name.clone(),
            writer: self.writer_addr(sc, ns),
            nodes: self.pod_addrs(sc, ns),
            root_password: root,
            ctl_password: String::new(),
            repl_password: String::new(),
        };
        match self.accounts.ensure_domain(&target, domain).await {
            Ok(changed) if changed > 0 => {
                tracing::info!(cluster = %name, changed, domain, "gtid_domain_id set live while not Ready");
            }
            Ok(_) => {}
            Err(e) => {
                tracing::debug!(cluster = %name, error = %e, "gtid_domain_id while not Ready");
            }
        }
    }

    /// A scale or a replaced Service: the record follows (r2opfix), so a
    /// freed address can't keep pointing at another cluster's node; the
    /// probes also refuse a node whose identity isn't the pinned one.
    async fn sync_nodes(&self, name: &str, status: &mut SprigClusterStatus, nodes: &[NodeAddress]) {
        let specs: Vec<NodeSpec> = nodes
            .iter()
            .filter_map(|n| {
                Some(NodeSpec {
                    node: NodeId::new(n.node.clone()).ok()?,
                    addr: n.addr.clone(),
                    zone: None,
                })
            })
            .collect();
        let id = status.cluster_id.clone().unwrap_or_default();
        match self.registrar.set_nodes(&id, &specs).await {
            Ok(()) => {
                tracing::info!(cluster = %name, nodes = specs.len(), "record's nodes updated");
                status.nodes = nodes.to_vec();
            }
            Err(e) => {
                status.message = Some(format!(
                    "node addresses changed to {}; updating the record failed: {e}",
                    nodes
                        .iter()
                        .map(|n| n.addr.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
        }
    }

    fn pod_addr(&self, sc: &SprigCluster, ns: &str, i: u8) -> String {
        let name = &sc.metadata.name;
        format!(
            "{name}-{i}.{name}-internal.{ns}.svc.{}:3306",
            self.cfg.cluster_domain
        )
    }

    fn note<T>(&self, cluster: &str, f: impl FnOnce(&mut ClusterWatch) -> T) -> T {
        let mut w = self.watches.lock().unwrap_or_else(PoisonError::into_inner);
        f(w.entry(cluster.to_owned()).or_default())
    }

    /// Whether the pod is gone, terminating, or not Ready: the second
    /// source a failover needs (the first is the primary not answering).
    async fn pod_down(&self, ns: &str, pod: &str) -> bool {
        match self.kube.get(&POD, ns, pod).await {
            Ok(None) => true,
            Ok(Some(p)) => {
                p["metadata"]["deletionTimestamp"].is_string()
                    || !p["status"]["conditions"].as_array().is_some_and(|cs| {
                        cs.iter()
                            .any(|c| c["type"] == "Ready" && c["status"] == "True")
                    })
            }
            // The API is unreachable: no second source.
            Err(_) => false,
        }
    }

    /// One pass of the failover watch over the `SprigCluster` `raw` (see
    /// [`crate::failover`]): a registered primary + replica cluster with
    /// `failoverDriver: sprig`. Run every second.
    ///
    /// # Errors
    /// API failures; the next pass tries again.
    #[allow(clippy::too_many_lines)] // the steps, in order
    pub async fn watch(&self, raw: &Value) -> Result<WatchOutcome> {
        let Some(repl) = self.repl.clone() else {
            return Ok(WatchOutcome::Skipped);
        };
        let sc: SprigCluster = serde_json::from_value(raw.clone())
            .map_err(|e| SprigError::internal(format!("bad SprigCluster: {e}")))?;
        let registered = sc.status.as_ref().is_some_and(|s| s.cluster_id.is_some());
        if sc.spec.topology != Topology::Replication
            || sc.spec.failover_driver != FailoverDriver::Sprig
            || !registered
            || sc.metadata.deletion_timestamp.is_some()
        {
            return Ok(WatchOutcome::Skipped);
        }
        let ns = sc
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| self.cfg.namespace.clone());
        let name = sc.metadata.name.clone();
        let Some(mdb) = self
            .kube
            .get(&MARIADB, &ns, &name)
            .await
            .map_err(|e| kerr("get MariaDB", &e))?
        else {
            return Ok(WatchOutcome::Skipped);
        };
        let idx = |v: &Value| v.as_u64().and_then(|n| u8::try_from(n).ok());
        let (Some(cur), want) = (
            idx(&mdb["status"]["currentPrimaryPodIndex"]),
            idx(&mdb["spec"]["replication"]["primary"]["podIndex"]),
        ) else {
            return Ok(WatchOutcome::Skipped);
        };
        if want.is_some_and(|w| w != cur) {
            self.note(&name, |w| w.down_since = None);
            return Ok(WatchOutcome::Switching);
        }
        let Some(root) = self
            .secret_value(&ns, &format!("{name}-root"), "password")
            .await?
        else {
            return Ok(WatchOutcome::Skipped);
        };
        let replicas = sc.spec.replicas();
        let primary_addr = self.pod_addr(&sc, &ns, cur);
        match repl.view(&primary_addr, &root, cur).await {
            Ok(pv) => {
                self.note(&name, |w| {
                    w.down_since = None;
                    w.blocked = None;
                });
                let repaired = self
                    .repair_replicas(&sc, &ns, &root, cur, &pv, repl.as_ref())
                    .await;
                Ok(WatchOutcome::Healthy { repaired })
            }
            Err(e) => {
                let now = Instant::now();
                let since = self.note(&name, |w| *w.down_since.get_or_insert(now));
                let detect = self.cfg.failover_detect;
                let pod = format!("{name}-{cur}");
                if now.duration_since(since) < detect || !self.pod_down(&ns, &pod).await {
                    tracing::debug!(cluster = %name, error = %e, "primary not answering");
                    return Ok(WatchOutcome::Suspect);
                }
                self.fail_over(&sc, &ns, &root, cur, replicas, since, repl.as_ref())
                    .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn fail_over(
        &self,
        sc: &SprigCluster,
        ns: &str,
        root: &str,
        cur: u8,
        replicas: u8,
        since: Instant,
        repl: &dyn ReplicationOps,
    ) -> Result<WatchOutcome> {
        let name = sc.metadata.name.clone();
        let started = Instant::now();
        // a. Every other pod applies what it received.
        let mut views: Vec<ReplView> = Vec::new();
        for i in (0..replicas).filter(|i| *i != cur) {
            let addr = self.pod_addr(sc, ns, i);
            match repl.drain(&addr, root, i, failover::DRAIN_WAIT).await {
                Ok(v) => views.push(v),
                Err(e) => {
                    tracing::warn!(cluster = %name, pod = i, error = %e, "replica unreachable during failover");
                }
            }
        }
        // b. The most advanced replica.
        let best = match failover::choose(&views) {
            Ok(b) => b,
            Err(why) => {
                let first = self.note(&name, |w| {
                    let first = w.blocked.as_deref() != Some(why.as_str());
                    w.blocked = Some(why.clone());
                    first
                });
                if first {
                    self.event(sc, ns, "FailoverBlocked", &why).await;
                }
                metrics::counter!("sprig_operator_failovers_total", "result" => "blocked")
                    .increment(1);
                tracing::warn!(cluster = %name, %why, "failover blocked");
                return Ok(WatchOutcome::Blocked(why));
            }
        };
        // c. Still down? A primary back in the meantime keeps its role.
        if repl
            .view(&self.pod_addr(sc, ns, cur), root, cur)
            .await
            .is_ok()
        {
            tracing::info!(cluster = %name, "primary answered again; failover abandoned");
            self.note(&name, |w| w.down_since = None);
            return Ok(WatchOutcome::Healthy { repaired: vec![] });
        }
        let chosen = views
            .iter()
            .find(|v| v.index == best)
            .cloned()
            .unwrap_or_default();
        let others: Vec<String> = views
            .iter()
            .filter(|v| v.index != best)
            .map(|v| format!("pod {} {}", v.index, v.received()))
            .collect();
        let reason = format!(
            "pod {cur} unreachable for {:.1}s; pod {best} holds every replica's transactions \
             ({}; others: {})",
            since.elapsed().as_secs_f64(),
            chosen.received(),
            if others.is_empty() {
                "none".to_owned()
            } else {
                others.join(", ")
            }
        );
        self.kube
            .merge_patch(
                &MARIADB,
                ns,
                &name,
                &json!({"spec": {"replication": {"primary": {"podIndex": best}}}}),
            )
            .await
            .map_err(|e| kerr("patch podIndex", &e))?;
        metrics::counter!("sprig_operator_failovers_total", "result" => "promoted").increment(1);
        tracing::info!(cluster = %name, from = cur, to = best, took_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX), %reason, "failover: podIndex set");
        self.event(sc, ns, "Failover", &reason).await;
        let rec = FailoverRecord {
            from: cur,
            to: best,
            time: sprig_cluster::record::rfc3339(self.clock.wall()),
            reason,
        };
        self.note(&name, |w| {
            w.down_since = None;
            w.blocked = None;
            w.last = Some(rec);
        });
        Ok(WatchOutcome::FailedOver(best))
    }

    /// Repairs, or marks for rebuild, replicas a failover left broken.
    async fn repair_replicas(
        &self,
        sc: &SprigCluster,
        ns: &str,
        root: &str,
        cur: u8,
        primary: &ReplView,
        repl: &dyn ReplicationOps,
    ) -> Vec<u8> {
        let name = sc.metadata.name.clone();
        let primary_addr = self.pod_addr(sc, ns, cur);
        let mut repaired = Vec::new();
        for i in (0..sc.spec.replicas()).filter(|i| *i != cur) {
            let addr = self.pod_addr(sc, ns, i);
            let Ok(v) = repl.view(&addr, root, i).await else {
                continue;
            };
            if !v.broken() {
                if v.sql_running && v.io_running {
                    self.note(&name, |w| w.rebuild.remove(&i));
                }
                continue;
            }
            if self.note(&name, |w| w.rebuild.contains(&i)) {
                continue;
            }
            let history = repl.in_history(&primary_addr, root, &v.binlog_pos).await;
            match failover::repair_plan(&v.binlog_pos, &primary.binlog_pos, history) {
                Repair::Apply => match repl.repair(&addr, root).await {
                    Ok(()) => {
                        let msg = format!(
                            "pod {i} stopped (SQL {}, IO {}) after a failover; its binlog {} is in \
                             pod {cur}'s history ({}): gtid_slave_pos set to its gtid_binlog_pos",
                            v.sql_errno, v.io_errno, v.binlog_pos, primary.binlog_pos
                        );
                        tracing::info!(cluster = %name, pod = i, "{msg}");
                        self.event(sc, ns, "ReplicaRepaired", &msg).await;
                        metrics::counter!("sprig_operator_replica_repairs_total", "result" => "repaired").increment(1);
                        repaired.push(i);
                    }
                    Err(e) => tracing::warn!(cluster = %name, pod = i, error = %e, "repair failed"),
                },
                Repair::Rebuild(why) => {
                    let msg = format!("pod {i} needs a rebuild: {why}");
                    tracing::warn!(cluster = %name, pod = i, "{msg}");
                    self.event(sc, ns, "NodeNeedsRebuild", &msg).await;
                    metrics::counter!("sprig_operator_replica_repairs_total", "result" => "rebuild").increment(1);
                    self.note(&name, |w| w.rebuild.insert(i));
                }
            }
        }
        repaired
    }

    /// The `SprigCluster`s of pool `pool`.
    ///
    /// # Errors
    /// API failures.
    pub async fn pool_members(&self, ns: &str, pool: &str) -> Result<Vec<SprigCluster>> {
        self.kube
            .list(&SPRIG_CLUSTER, ns, Some(&format!("{POOL_LABEL}={pool}")))
            .await
            .map_err(|e| kerr("list clusters", &e))?
            .into_iter()
            .map(|v| {
                serde_json::from_value(v)
                    .map_err(|e| SprigError::internal(format!("bad SprigCluster: {e}")))
            })
            .collect()
    }

    /// Adds one cluster to `pool` (named `<pool>-<n>`, the lowest free
    /// `n` from 1) when it has fewer than `maxClusters`; `None` at the
    /// limit. `job` is recorded as the annotation `sprigdb.io/job-id`.
    ///
    /// # Errors
    /// API failures.
    pub async fn grow(&self, pool: &SprigClusterPool, job: Option<&str>) -> Result<Option<String>> {
        let ns = pool
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| self.cfg.namespace.clone());
        let members = self.pool_members(&ns, &pool.metadata.name).await?;
        if members.len() >= pool.spec.max_clusters as usize {
            return Ok(None);
        }
        let taken: Vec<&str> = members.iter().map(|m| m.metadata.name.as_str()).collect();
        let name = (1..=pool.spec.max_clusters)
            .map(|n| format!("{}-{n}", pool.metadata.name))
            .find(|n| !taken.contains(&n.as_str()))
            .unwrap_or_default();
        let mut sc = SprigCluster::new("SprigCluster", &ns, &name, pool.spec.template.clone());
        sc.metadata
            .labels
            .insert(POOL_LABEL.into(), pool.metadata.name.clone());
        sc.metadata
            .labels
            .insert(crd::MANAGED_BY.0.into(), crd::MANAGED_BY.1.into());
        if let Some(j) = job {
            sc.metadata
                .annotations
                .insert("sprigdb.io/job-id".into(), j.to_owned());
        }
        let mut v = serde_json::to_value(&sc).map_err(|e| SprigError::internal(e.to_string()))?;
        v["metadata"]["ownerReferences"] = json!([pool.owner_reference()]);
        if let Some(o) = v.as_object_mut() {
            o.remove("status");
        }
        match self.kube.create(&SPRIG_CLUSTER, &ns, &v).await {
            Ok(_) | Err(KubeError::AlreadyExists) => Ok(Some(name)),
            Err(e) => Err(kerr("create SprigCluster", &e)),
        }
    }

    /// One pass over a pool: at least `minClusters`, and the status counts.
    ///
    /// # Errors
    /// API failures.
    pub async fn reconcile_pool(&self, raw: &Value) -> Result<Duration> {
        let pool: SprigClusterPool = serde_json::from_value(raw.clone())
            .map_err(|e| SprigError::internal(format!("bad SprigClusterPool: {e}")))?;
        let ns = pool
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| self.cfg.namespace.clone());
        let min = pool.spec.min_clusters.min(pool.spec.max_clusters) as usize;
        let mut members = self.pool_members(&ns, &pool.metadata.name).await?;
        while members.len() < min {
            if self.grow(&pool, None).await?.is_none() {
                break;
            }
            members = self.pool_members(&ns, &pool.metadata.name).await?;
        }
        let status = crd::SprigClusterPoolStatus {
            clusters: u32::try_from(members.len()).unwrap_or(u32::MAX),
            active: u32::try_from(
                members
                    .iter()
                    .filter(|m| m.status.as_ref().is_some_and(|s| s.phase == Phase::Active))
                    .count(),
            )
            .unwrap_or(u32::MAX),
        };
        if pool.status.as_ref() != Some(&status) {
            let mut obj = raw.clone();
            obj["status"] =
                serde_json::to_value(&status).map_err(|e| SprigError::internal(e.to_string()))?;
            self.kube
                .replace_status(&crd::SPRIG_CLUSTER_POOL, &ns, &pool.metadata.name, &obj)
                .await
                .map_err(|e| kerr("pool status", &e))?;
        }
        metrics::counter!("sprig_operator_reconcile_total", "kind" => "SprigClusterPool", "result" => "ok").increment(1);
        Ok(Duration::from_secs(30))
    }
}
