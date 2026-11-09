//! The VM provisioner (chapter 27, "VM packaging", "The agent on VMs";
//! build steps 8 and 9): the free-host pool, cluster plans, systemd units
//! and install steps.
//!
//! ```text
//! create(spec) on VMs, a cluster_provision job:
//! 1. take spec.nodes free hosts in spec.dc across zones (txn on their records)
//! 2. server_id = cluster_number * 100 + index; gtid_domain_id = cluster_number
//! 3. Apply(install, configure) on every agent
//! 4. primary + replicas: node 1 starts fenced, creates sprig_ctl, sprig_repl and sprig_gw,
//!    becomes primary when its agent takes the writer lease; each replica is seeded
//!    by StreamBackup from node 1, then CHANGE MASTER ... MASTER_USE_GTID = slave_pos
//! 5. Galera: galera_new_cluster on node 1, wait for Synced; then each other node
//! 6. single: start node 1
//! 7. write the cluster record (provisioner = vm, failover_driver = sprig)
//! ```
//!
//! **As built (R2, r2prov).** Steps 1 and 2 run here: [`VmProvisioner::create`]
//! claims hosts from `vm-hosts/` in the DC tier with one compare-and-set
//! transaction, reserves the cluster number, renders every node's settings
//! and steps, and stores the whole plan as `provision-jobs/<job>` (state
//! `planned`). Running a plan (the agent's `Apply`, steps 3 to 7) is the
//! agent's plan engine, which isn't built; [`VmProvisioner::register_planned`]
//! is step 7. Hosts join the pool with [`HostPool::offer`] (the agent, or
//! the Ansible role through it, at first start).

pub mod install;
pub mod units;

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_cluster::record::{Capacity, NodeSpec, ProvisionerKind, SecretRefs, TopologyKind};
use sprig_cluster::registry::{ClusterRegistry, Registration, StoredCluster};
use sprig_common::ids::{ClusterId, JobId, NodeId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Cmp, ListOpts, MetaStore, Op, Record, Txn, Versioned};

use crate::mycnf::{self, GaleraSettings, NodeRole, NodeSettings, Platform, TlsFiles};
use crate::{ClusterSpec, ProvisionOp, Provisioner, ProvisionerKind as Kind, Topology};
use install::{HostOs, Step};

/// A host handed over for databases (`vm-hosts/<host_id>`, DC tier).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostRecord {
    /// The host's id (its agent's id).
    pub host_id: String,
    /// The address MariaDB will listen on (port 3306 implied).
    pub addr: String,
    /// Its zone.
    #[serde(default)]
    pub zone: String,
    /// Memory, bytes.
    pub memory_bytes: u64,
    /// Its operating system.
    pub os: HostOs,
    /// `free`, `claimed` or `broken`.
    pub state: HostState,
    /// The cluster it serves, once claimed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
    /// The job that claimed it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<String>,
}

impl Versioned for HostRecord {
    const V: u32 = 1;
}

/// A host's state in the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostState {
    /// Ready for a cluster.
    Free,
    /// Part of a cluster, or of a plan for one.
    Claimed,
    /// A step failed three times; an operator replaces it.
    Broken,
}

/// The free-host pool of one DC.
#[derive(Clone)]
pub struct HostPool {
    dc: Arc<dyn MetaStore>,
}

impl std::fmt::Debug for HostPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostPool")
            .field("tier", self.dc.tier())
            .finish()
    }
}

fn key(dc: &dyn MetaStore, rel: &str) -> Result<sprig_meta::Key> {
    dc.tier().key(rel)
}

impl HostPool {
    /// The pool in the DC tier `dc`.
    #[must_use]
    pub fn new(dc: Arc<dyn MetaStore>) -> Self {
        Self { dc }
    }

    /// Adds (or refreshes) a free host. A claimed host keeps its claim.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn offer(&self, mut host: HostRecord) -> Result<()> {
        let k = key(&*self.dc, &format!("vm-hosts/{}", host.host_id))?;
        if let Some(kv) = self
            .dc
            .get(&k, sprig_meta::Consistency::Linearizable)
            .await?
        {
            let cur = Record::<HostRecord>::decode(&kv.value)?.body;
            if cur.state != HostState::Free {
                return Ok(());
            }
        }
        host.state = HostState::Free;
        host.cluster = None;
        host.job = None;
        self.dc
            .txn(Txn {
                when: vec![],
                then: vec![Op::Put {
                    key: k,
                    value: Record::new(host).encode()?,
                    lease: None,
                }],
                otherwise: vec![],
            })
            .await?;
        Ok(())
    }

    /// Every host, with its revision.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn list(&self) -> Result<Vec<(HostRecord, sprig_meta::Revision)>> {
        let prefix = key(&*self.dc, "vm-hosts/")?;
        let mut out = Vec::new();
        let mut after = None;
        loop {
            let page = self
                .dc
                .list(
                    &prefix,
                    ListOpts {
                        after: after.clone(),
                        ..ListOpts::default()
                    },
                )
                .await?;
            for kv in &page.kvs {
                out.push((
                    Record::<HostRecord>::decode(&kv.value)?.body,
                    kv.mod_revision,
                ));
            }
            if !page.more {
                break;
            }
            after = page.kvs.last().map(|kv| kv.key.clone());
        }
        Ok(out)
    }

    /// Claims `hosts` for `cluster` and `job` in one transaction: every
    /// host must still be free at the revision it was read at.
    ///
    /// # Errors
    /// `false` when another claim won the race; the store's errors.
    pub async fn claim(
        &self,
        hosts: &[(HostRecord, sprig_meta::Revision)],
        cluster: &str,
        job: &str,
    ) -> Result<bool> {
        let mut when = Vec::new();
        let mut then = Vec::new();
        for (h, rev) in hosts {
            let k = key(&*self.dc, &format!("vm-hosts/{}", h.host_id))?;
            when.push(Cmp::ModRev(k.clone(), *rev));
            let mut c = h.clone();
            c.state = HostState::Claimed;
            c.cluster = Some(cluster.to_owned());
            c.job = Some(job.to_owned());
            then.push(Op::Put {
                key: k,
                value: Record::new(c).encode()?,
                lease: None,
            });
        }
        Ok(self
            .dc
            .txn(Txn {
                when,
                then,
                otherwise: vec![],
            })
            .await?
            .succeeded)
    }
}

/// Step 1's choice: `n` free hosts, spread over zones (`zones`, or every
/// zone when empty) one per zone in turn, the zone with the most free hosts
/// first; host ids break ties. `None` when fewer than `n` qualify.
#[must_use]
pub fn choose_hosts(
    hosts: &[(HostRecord, sprig_meta::Revision)],
    zones: &[String],
    n: usize,
) -> Option<Vec<(HostRecord, sprig_meta::Revision)>> {
    let mut by_zone: BTreeMap<&str, Vec<&(HostRecord, sprig_meta::Revision)>> = BTreeMap::new();
    for h in hosts {
        if h.0.state == HostState::Free && (zones.is_empty() || zones.contains(&h.0.zone)) {
            by_zone.entry(h.0.zone.as_str()).or_default().push(h);
        }
    }
    for v in by_zone.values_mut() {
        v.sort_by(|a, b| a.0.host_id.cmp(&b.0.host_id));
        v.reverse(); // pop() takes the smallest id
    }
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        let mut order: Vec<&str> = by_zone
            .iter()
            .filter(|(_, v)| !v.is_empty())
            .map(|(z, _)| *z)
            .collect();
        if order.is_empty() {
            return None;
        }
        order.sort_by(|a, b| by_zone[b].len().cmp(&by_zone[a].len()).then(a.cmp(b)));
        for z in order {
            if out.len() == n {
                break;
            }
            if let Some(h) = by_zone.get_mut(z).and_then(Vec::pop) {
                out.push(h.clone());
            }
        }
    }
    Some(out)
}

/// One node of a plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedNode {
    /// `n1`, `n2`, ...
    pub node: String,
    /// The host.
    pub host_id: String,
    /// `host:3306`.
    pub addr: String,
    /// The zone.
    pub zone: String,
    /// `server_id`.
    pub server_id: u32,
    /// `primary`, `replica`, `galera` or `single`.
    pub role: String,
    /// Install, then configure.
    pub steps: Vec<Step>,
}

/// The cluster-wide sequence after every node is configured (steps 4 to 7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "step", rename_all = "snake_case")]
pub enum BootstrapStep {
    /// Start MariaDB fenced (`read_only = ON`).
    StartFenced {
        /// The node.
        node: String,
    },
    /// Create `sprig_ctl`, `sprig_repl` and `sprig_gw` (`sprig_provision::gw_account`) on the node.
    CreateSystemAccounts {
        /// The node.
        node: String,
    },
    /// The node's agent takes the writer lease and unfences.
    TakeWriterLease {
        /// The node.
        node: String,
    },
    /// Seed `node` from `from` with a streamed `mariadb-backup`, prepare,
    /// copy back, set `gtid_slave_pos`, `CHANGE MASTER TO` with
    /// `MASTER_USE_GTID = slave_pos` and `MASTER_SSL = 1`, `START SLAVE`.
    SeedReplica {
        /// The new replica.
        node: String,
        /// The primary.
        from: String,
    },
    /// `galera_new_cluster` on the first node.
    GaleraNewCluster {
        /// The node.
        node: String,
    },
    /// Start a Galera node (SST by mariabackup) and wait for `Synced`.
    GaleraJoin {
        /// The node.
        node: String,
    },
    /// Write the cluster record (`provisioner = vm`, `failover_driver = sprig`).
    Register,
}

/// A `cluster_provision` plan (`provision-jobs/<job>`, DC tier).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvisionPlan {
    /// The job.
    pub job: String,
    /// `create`, `add_node` or `remove_node`.
    pub op: String,
    /// The cluster's name.
    pub name: String,
    /// Its reserved number.
    pub cluster_number: u32,
    /// Its id.
    pub cluster_id: String,
    /// The topology.
    pub topology: Topology,
    /// `planned`, then the agent's states (`running`, `done`, `failed`).
    pub state: String,
    /// Every node.
    pub nodes: Vec<PlannedNode>,
    /// The cluster-wide sequence.
    pub bootstrap: Vec<BootstrapStep>,
}

impl Versioned for ProvisionPlan {
    const V: u32 = 1;
}

/// The TLS file locations every managed node uses.
#[must_use]
pub fn tls_files() -> TlsFiles {
    TlsFiles {
        ca: "/etc/sprigdb/tls/mariadb/ca.pem".into(),
        cert: "/etc/sprigdb/tls/mariadb/server.pem".into(),
        key: "/etc/sprigdb/tls/mariadb/server-key.pem".into(),
    }
}

fn role_of(topology: Topology, index: u32) -> NodeRole {
    match topology {
        Topology::Galera => NodeRole::Galera,
        Topology::Single => NodeRole::Single,
        Topology::PrimaryReplica if index == 1 => NodeRole::Primary,
        Topology::PrimaryReplica => NodeRole::Replica,
    }
}

fn role_name(r: NodeRole) -> &'static str {
    match r {
        NodeRole::Primary => "primary",
        NodeRole::Replica => "replica",
        NodeRole::Galera => "galera",
        NodeRole::Single => "single",
    }
}

fn host_of(addr: &str) -> &str {
    addr.rsplit_once(':').map_or(addr, |(h, _)| h)
}

/// Settings and steps for every chosen host (steps 2 and 3), and the
/// bootstrap sequence (steps 4 to 7).
#[must_use]
pub fn plan(
    spec: &ClusterSpec,
    name: &str,
    cluster_number: u32,
    cluster_id: &ClusterId,
    hosts: &[HostRecord],
    job: &str,
    gcache_size: &str,
) -> ProvisionPlan {
    let members: Vec<String> = hosts.iter().map(|h| host_of(&h.addr).to_owned()).collect();
    let mut nodes = Vec::new();
    for (i, h) in hosts.iter().enumerate() {
        let index = u32::try_from(i + 1).unwrap_or(u32::MAX);
        let role = role_of(spec.topology, index);
        let mut n = NodeSettings::new(Platform::Vm, role, cluster_number, index, h.memory_bytes);
        n.report_host = Some(host_of(&h.addr).to_owned());
        n.tls = Some(tls_files());
        if role == NodeRole::Galera {
            n.galera = Some(GaleraSettings {
                cluster_name: name.to_owned(),
                members: members.clone(),
                node_address: host_of(&h.addr).to_owned(),
                gcache_size: gcache_size.to_owned(),
                provider: h.os.distro.galera_provider().to_owned(),
            });
        }
        let mut steps = install::install_steps(&h.os, spec.version, spec.topology);
        // The TLS files come from the deployment's CA at run time; the
        // plan names where they go.
        steps.extend(install::configure_steps(h.os.distro, &n, None));
        nodes.push(PlannedNode {
            node: format!("n{index}"),
            host_id: h.host_id.clone(),
            addr: if h.addr.contains(':') {
                h.addr.clone()
            } else {
                format!("{}:3306", h.addr)
            },
            zone: h.zone.clone(),
            server_id: n.server_id(),
            role: role_name(role).to_owned(),
            steps,
        });
    }
    let first = nodes.first().map(|n| n.node.clone()).unwrap_or_default();
    let mut bootstrap = Vec::new();
    match spec.topology {
        Topology::PrimaryReplica => {
            bootstrap.push(BootstrapStep::StartFenced {
                node: first.clone(),
            });
            bootstrap.push(BootstrapStep::CreateSystemAccounts {
                node: first.clone(),
            });
            bootstrap.push(BootstrapStep::TakeWriterLease {
                node: first.clone(),
            });
            for n in nodes.iter().skip(1) {
                bootstrap.push(BootstrapStep::SeedReplica {
                    node: n.node.clone(),
                    from: first.clone(),
                });
            }
        }
        Topology::Galera => {
            bootstrap.push(BootstrapStep::GaleraNewCluster {
                node: first.clone(),
            });
            bootstrap.push(BootstrapStep::CreateSystemAccounts {
                node: first.clone(),
            });
            for n in nodes.iter().skip(1) {
                bootstrap.push(BootstrapStep::GaleraJoin {
                    node: n.node.clone(),
                });
            }
        }
        Topology::Single => {
            bootstrap.push(BootstrapStep::StartFenced {
                node: first.clone(),
            });
            bootstrap.push(BootstrapStep::CreateSystemAccounts {
                node: first.clone(),
            });
            bootstrap.push(BootstrapStep::TakeWriterLease { node: first });
        }
    }
    bootstrap.push(BootstrapStep::Register);
    ProvisionPlan {
        job: job.to_owned(),
        op: "create".into(),
        name: name.to_owned(),
        cluster_number,
        cluster_id: cluster_id.to_string(),
        topology: spec.topology,
        state: "planned".into(),
        nodes,
        bootstrap,
    }
}

/// The VM provisioner of one DC.
#[derive(Clone)]
pub struct VmProvisioner {
    registry: ClusterRegistry,
    dc: Arc<dyn MetaStore>,
    pool: HostPool,
    gcache_size: String,
    creds_dir: String,
}

impl std::fmt::Debug for VmProvisioner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VmProvisioner")
            .field("dc", self.registry.dc())
            .finish_non_exhaustive()
    }
}

fn topology_kind(t: Topology) -> TopologyKind {
    match t {
        Topology::Galera => TopologyKind::Galera,
        Topology::PrimaryReplica => TopologyKind::PrimaryReplica,
        Topology::Single => TopologyKind::SingleNode,
    }
}

impl VmProvisioner {
    /// A provisioner over `registry` and its DC tier `dc`.
    #[must_use]
    pub fn new(registry: ClusterRegistry, dc: Arc<dyn MetaStore>) -> Self {
        Self {
            registry,
            pool: HostPool::new(Arc::clone(&dc)),
            dc,
            gcache_size: "2G".into(),
            creds_dir: "/etc/sprigdb/creds".into(),
        }
    }

    /// Uses `provision.galera_gcache_size` = `size`.
    #[must_use]
    pub fn with_gcache_size(mut self, size: &str) -> Self {
        size.clone_into(&mut self.gcache_size);
        self
    }

    /// The host pool.
    #[must_use]
    pub fn pool(&self) -> &HostPool {
        &self.pool
    }

    fn plan_key(&self, job: &str) -> Result<sprig_meta::Key> {
        key(&*self.dc, &format!("provision-jobs/{job}"))
    }

    /// A stored plan.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn plan(&self, job: &JobId) -> Result<Option<ProvisionPlan>> {
        let k = self.plan_key(&job.to_string())?;
        match self
            .dc
            .get(&k, sprig_meta::Consistency::Linearizable)
            .await?
        {
            Some(kv) => Ok(Some(Record::<ProvisionPlan>::decode(&kv.value)?.body)),
            None => Ok(None),
        }
    }

    /// Plans a cluster called `name` (see the module docs).
    ///
    /// # Errors
    /// 39513 with too few free hosts; 39510 for a spec in another DC; the
    /// stores' errors.
    pub async fn create_named(&self, name: &str, spec: &ClusterSpec) -> Result<JobId> {
        if &spec.dc != self.registry.dc() {
            return Err(SprigError::new(
                SprigCode::ProvisionerUnsupported,
                format!(
                    "this provisioner serves dc {}, not {}",
                    self.registry.dc(),
                    spec.dc
                ),
            ));
        }
        let want = match spec.topology {
            Topology::Single => 1,
            _ => usize::from(spec.nodes.max(1)),
        };
        let job = JobId::new_v7();
        for _ in 0..5 {
            let hosts = self.pool.list().await?;
            let free = hosts
                .iter()
                .filter(|h| h.0.state == HostState::Free)
                .count();
            let Some(chosen) = choose_hosts(&hosts, &spec.zones, want) else {
                return Err(SprigError::new(
                    SprigCode::ProvisionNoHosts,
                    format!(
                        "{want} free hosts needed in {}{}, {free} free",
                        spec.dc,
                        if spec.zones.is_empty() {
                            String::new()
                        } else {
                            format!(" (zones {})", spec.zones.join(", "))
                        }
                    ),
                ));
            };
            if !self.pool.claim(&chosen, name, &job.to_string()).await? {
                continue;
            }
            let (number, cluster_id) = self.registry.allocate_id(name).await?;
            let records: Vec<HostRecord> = chosen.into_iter().map(|h| h.0).collect();
            let p = plan(
                spec,
                name,
                number,
                &cluster_id,
                &records,
                &job.to_string(),
                &self.gcache_size,
            );
            self.dc
                .txn(Txn {
                    when: vec![Cmp::Missing(self.plan_key(&p.job)?)],
                    then: vec![Op::Put {
                        key: self.plan_key(&p.job)?,
                        value: Record::new(p).encode()?,
                        lease: None,
                    }],
                    otherwise: vec![],
                })
                .await?;
            metrics::counter!("sprig_provision_jobs_total", "provisioner" => "vm", "op" => "create", "result" => "planned").increment(1);
            return Ok(job);
        }
        Err(SprigError::new(
            SprigCode::ProvisionNoHosts,
            "lost the race for free hosts five times; retry",
        ))
    }

    /// Step 7 of a plan whose nodes are bootstrapped: the cluster record,
    /// with `provisioner = vm`.
    ///
    /// # Errors
    /// Internal for an unknown job; the registry's errors.
    pub async fn register_planned(&self, job: &JobId) -> Result<StoredCluster> {
        let p = self
            .plan(job)
            .await?
            .ok_or_else(|| SprigError::internal(format!("no provision job {job}")))?;
        let nodes = p
            .nodes
            .iter()
            .map(|n| {
                Ok(NodeSpec {
                    node: NodeId::new(n.node.clone())
                        .map_err(|e| SprigError::internal(e.to_string()))?,
                    addr: n.addr.clone(),
                    zone: (!n.zone.is_empty()).then(|| n.zone.clone()),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let refs = format!("file:{}/{}.toml", self.creds_dir, p.name);
        let cluster_id = ClusterId::new(p.cluster_id.clone())
            .map_err(|e| SprigError::internal(e.to_string()))?;
        self.registry
            .register_numbered(
                Registration {
                    name: p.name.clone(),
                    topology: topology_kind(p.topology),
                    provisioner: ProvisionerKind::Vm,
                    nodes,
                    secrets: SecretRefs {
                        ctl: format!("{refs}#sprig_ctl"),
                        repl: format!("{refs}#sprig_repl"),
                    },
                    capacity: Capacity::default(),
                    declared: None,
                },
                p.cluster_number,
                cluster_id,
            )
            .await
    }
}

#[async_trait::async_trait]
impl Provisioner for VmProvisioner {
    fn kind(&self) -> Kind {
        Kind::Vm
    }

    fn supports(&self, op: ProvisionOp) -> bool {
        matches!(op, ProvisionOp::Create)
    }

    /// Plans a cluster named after its pool (or `vm`) and the job.
    async fn create(&self, spec: ClusterSpec) -> Result<sprig_common::ids::JobId> {
        let base = spec.pool.clone().unwrap_or_else(|| "vm".into());
        let suffix = uuid::Uuid::now_v7().simple().to_string();
        let name = format!("{base}-{}", &suffix[suffix.len() - 6..]);
        self.create_named(&name, &spec).await
    }
}

/// The files the packages install: every unit, the drop-in directory's
/// limits file, and an example configuration.
#[must_use]
pub fn package_files() -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = units::units()
        .into_iter()
        .map(|(n, c)| (format!("systemd/{n}"), c))
        .collect();
    out.push((
        "systemd/mariadb.service.d/10-sprigdb-limits.conf".into(),
        units::mariadb_limits_dropin(mycnf::OPEN_FILES_LIMIT),
    ));
    out
}

#[cfg(test)]
mod tests;
