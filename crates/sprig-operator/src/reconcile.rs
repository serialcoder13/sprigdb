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

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use base64::Engine as _;
use serde_json::{Value, json};
use sprig_cluster::record::{NodeSpec, SecretRefs, TopologyKind};
use sprig_common::ids::NodeId;
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};

use crate::OperatorConfig;
use crate::accounts::{self, BootstrapTarget, SystemAccounts};
use crate::crd::{
    self, NodeAddress, POOL_LABEL, Phase, SPRIG_CLUSTER, SprigCluster, SprigClusterPool,
    SprigClusterStatus, Topology,
};
use crate::kube::{self, ApiResource, EVENT, KubeApi, KubeError, SECRET, SERVICE};
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
        }
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
            // The controller has no call to change a record's addresses
            // yet: say so, and keep the registered ones.
            status.message = Some(format!(
                "node addresses changed to {}; the cluster record keeps the registered ones",
                nodes
                    .iter()
                    .map(|n| n.addr.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
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
