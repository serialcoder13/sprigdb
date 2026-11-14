//! `sprig-provision`: the `Provisioner` trait, with external, Kubernetes and
//! VM implementations.
//!
//! Layer: 3 roles.
//!
//! What exists:
//! - [`Provisioner`], the operations a provisioner may support, and the
//!   specs they take;
//! - [`credentials`]: secret references for the system accounts and how
//!   controllers resolve them;
//! - [`external`]: the external provisioner, which registers MariaDB that
//!   already exists after prechecking its TLS, grants, settings (the change
//!   feed's binlog settings included), accounts, a binlog read as
//!   `sprig_repl` (the binlog dump probe) and capability profile.
//!
//! - [`mycnf`]: the baseline server settings of managed clusters, for VMs
//!   and for Kubernetes' `spec.myCnf`;
//! - [`vm`]: the VM provisioner's free-host pool, cluster plans, systemd
//!   units and install steps.
//!
//! Not yet: the object-store check. The Kubernetes provisioner
//! lives in `sprig-operator`, next to its client.

pub mod credentials;
// The docs-reader account.
pub mod docs_account;
pub mod external;
pub mod gw_account;
pub mod mycnf;
pub mod vm;

use std::fmt;

use serde::{Deserialize, Serialize};
use sprig_cluster::health::ClusterHealth;
use sprig_cluster::probe::NodeProbe;
use sprig_cluster::profile::CapabilityProfile;
use sprig_cluster::record::{Capacity, Declared, NodeSpec, SecretRefs, TopologyKind};
use sprig_common::ids::{ClusterId, DcId, JobId, NodeId};
use sprig_common::{Result, SprigCode, SprigError};

pub use external::ExternalProvisioner;
pub use sprig_cluster::record::ProvisionerKind;

/// An operation a provisioner may support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProvisionOp {
    /// Register a cluster that exists.
    Register,
    /// Create a cluster.
    Create,
    /// Add a node.
    AddNode,
    /// Remove a node.
    RemoveNode,
    /// Upgrade MariaDB.
    Upgrade,
    /// Take a backup now.
    BackupNow,
    /// Restore a backup.
    Restore,
    /// Retire a drained cluster.
    Retire,
}

impl fmt::Display for ProvisionOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Register => "register",
            Self::Create => "create",
            Self::AddNode => "add_node",
            Self::RemoveNode => "remove_node",
            Self::Upgrade => "upgrade",
            Self::BackupNow => "backup_now",
            Self::Restore => "restore",
            Self::Retire => "retire",
        })
    }
}

/// A topology SprigDB can create (external clusters are registered, never
/// created).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Topology {
    /// Galera.
    Galera,
    /// A primary with replicas, semi-sync `AFTER_SYNC`.
    PrimaryReplica,
    /// One node.
    Single,
}

/// A MariaDB release, `major.minor`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MariaDbVersion {
    /// Major.
    pub major: u16,
    /// Minor.
    pub minor: u16,
}

impl fmt::Display for MariaDbVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// Resources of each node of a new cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Resources {
    /// CPU, millicores.
    pub cpu_millis: u32,
    /// Memory, GB.
    pub memory_gb: u64,
    /// Storage, GB.
    pub storage_gb: u64,
}

/// A cluster to create (Kubernetes and VMs).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterSpec {
    /// The data center.
    pub dc: DcId,
    /// The topology.
    pub topology: Topology,
    /// Nodes.
    pub nodes: u8,
    /// MariaDB version (`provision.mariadb_version`).
    pub version: MariaDbVersion,
    /// Per-node resources.
    pub resources: Resources,
    /// Zones to spread over.
    pub zones: Vec<String>,
    /// Where backups go.
    pub backup_target: String,
    /// The pool it belongs to.
    pub pool: Option<String>,
}

/// A restore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreSpec {
    /// The cluster to restore into.
    pub cluster: ClusterId,
    /// The backup to restore.
    pub backup: String,
    /// Point in time, RFC 3339, if not the backup's own.
    pub point_in_time: Option<String>,
}

/// What `sprigctl cluster register` asks for.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegisterCluster {
    /// The data center; it must be the registering controller's.
    pub dc: DcId,
    /// A name unique within the data center.
    pub name: String,
    /// The topology the operator says it is; probes must agree.
    pub topology: TopologyKind,
    /// The nodes.
    pub nodes: Vec<NodeSpec>,
    /// References to the `sprig_ctl` and `sprig_repl` credentials.
    pub secrets: SecretRefs,
    /// Declared capacity.
    #[serde(default)]
    pub capacity: Capacity,
    /// `[declared]` facts probes can't see.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared: Option<Declared>,
}

/// One node as the prechecks saw it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeReport {
    /// The node.
    pub node: NodeId,
    /// `host:port`.
    pub addr: String,
    /// The TLS cipher of the `sprig_ctl` session, if it connected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_cipher: Option<String>,
    /// `@@version`, if it answered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// `writer`, `replica` or `galera`, as probed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// Why it couldn't be probed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// The prechecks' findings.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PrecheckReport {
    /// Items that refuse the registration, each with its fix where there
    /// is one.
    pub blocking: Vec<String>,
    /// Items that disable only what needs them, or are recommendations.
    pub advisories: Vec<String>,
    /// Each node.
    pub nodes: Vec<NodeReport>,
    /// The capability profile the probes derived, when a node answered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<CapabilityProfile>,
    /// The health the probes saw.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health: Option<ClusterHealth>,
    /// The probes themselves, for whoever keeps probing next.
    #[serde(skip)]
    pub probes: Vec<NodeProbe>,
}

impl PrecheckReport {
    /// Whether registration may go ahead.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.blocking.is_empty()
    }

    /// 39511 `ER_SPRIG_CLUSTER_PRECHECK_FAILED`, listing every blocking
    /// item, one per line.
    #[must_use]
    pub fn error(&self, name: &str) -> SprigError {
        let mut msg = format!(
            "cluster {name:?} failed {} registration precheck(s):",
            self.blocking.len()
        );
        for b in &self.blocking {
            msg.push_str("\n- ");
            msg.push_str(b);
        }
        SprigError::new(SprigCode::ClusterPrecheckFailed, msg)
    }
}

/// 39510 `ER_SPRIG_PROVISIONER_UNSUPPORTED`.
#[must_use]
pub fn unsupported(kind: ProvisionerKind, op: ProvisionOp) -> SprigError {
    let kind = match kind {
        ProvisionerKind::External => "external",
        ProvisionerKind::Kubernetes => "kubernetes",
        ProvisionerKind::Vm => "vm",
    };
    SprigError::new(
        SprigCode::ProvisionerUnsupported,
        format!("the {kind} provisioner doesn't support {op}"),
    )
}

/// Creates, changes and retires the clusters SprigDB uses. Every operation a provisioner doesn't support fails with
/// 39510; the defaults below say so. Long operations return the id of a
/// `cluster_provision` job.
///
/// `precheck` backs `sprigctl cluster register --dry-run`: it runs the prechecks without writing.
#[async_trait::async_trait]
pub trait Provisioner: Send + Sync {
    /// Which provisioner this is.
    fn kind(&self) -> ProvisionerKind;
    /// Whether it supports `op`.
    fn supports(&self, op: ProvisionOp) -> bool;
    /// Runs the registration prechecks without writing anything.
    ///
    /// # Errors
    /// 39510 where registration isn't supported; otherwise the findings
    /// come back in the report, not as errors.
    async fn precheck(&self, req: &RegisterCluster) -> Result<PrecheckReport> {
        let _ = req;
        Err(unsupported(self.kind(), ProvisionOp::Register))
    }
    /// Registers a cluster that exists.
    ///
    /// # Errors
    /// 39511 when a precheck fails, 39105 for a name in use, 39510 where
    /// unsupported.
    async fn register(&self, req: RegisterCluster) -> Result<ClusterId> {
        let _ = req;
        Err(unsupported(self.kind(), ProvisionOp::Register))
    }
    /// Creates a cluster.
    ///
    /// # Errors
    /// 39510 where unsupported.
    async fn create(&self, spec: ClusterSpec) -> Result<JobId> {
        let _ = spec;
        Err(unsupported(self.kind(), ProvisionOp::Create))
    }
    /// Adds a node.
    ///
    /// # Errors
    /// 39510 where unsupported.
    async fn add_node(&self, c: &ClusterId, n: NodeSpec) -> Result<JobId> {
        let _ = (c, n);
        Err(unsupported(self.kind(), ProvisionOp::AddNode))
    }
    /// Removes a node.
    ///
    /// # Errors
    /// 39510 where unsupported.
    async fn remove_node(&self, c: &ClusterId, n: &NodeId) -> Result<JobId> {
        let _ = (c, n);
        Err(unsupported(self.kind(), ProvisionOp::RemoveNode))
    }
    /// Upgrades MariaDB.
    ///
    /// # Errors
    /// 39510 where unsupported.
    async fn upgrade(&self, c: &ClusterId, v: MariaDbVersion) -> Result<JobId> {
        let _ = (c, v);
        Err(unsupported(self.kind(), ProvisionOp::Upgrade))
    }
    /// Takes a backup now.
    ///
    /// # Errors
    /// 39510 where unsupported.
    async fn backup_now(&self, c: &ClusterId) -> Result<JobId> {
        let _ = c;
        Err(unsupported(self.kind(), ProvisionOp::BackupNow))
    }
    /// Restores a backup.
    ///
    /// # Errors
    /// 39510 where unsupported.
    async fn restore(&self, r: RestoreSpec) -> Result<JobId> {
        let _ = r;
        Err(unsupported(self.kind(), ProvisionOp::Restore))
    }
    /// Retires a drained cluster.
    ///
    /// # Errors
    /// 39510 where unsupported.
    async fn retire(&self, c: &ClusterId) -> Result<JobId> {
        let _ = c;
        Err(unsupported(self.kind(), ProvisionOp::Retire))
    }
}
