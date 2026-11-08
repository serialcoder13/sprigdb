//! The Admin API's messages (`sprig.admin.v1.Clusters` and `DataCenters`,
//! chapter 37), as the REST face carries them: JSON bodies under
//! `/admin/v1/`. `sprigctl` decodes the same types.
//!
//! Until the protobuf definitions exist (chapter 37 steps 1-4), these
//! serde types are the contract; records appear as they are stored in etcd
//! (64-bit integers as JSON numbers, not proto3's strings).

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sprig_cluster::health::HealthReport;
use sprig_cluster::probe::ProbeDepth;
use sprig_cluster::profile::FieldChange;
use sprig_cluster::record::{
    Capacity, ClusterRecord, Declared, NodeSpec, ProvisionerKind, SecretRefs, TopologyKind,
};
use sprig_common::ids::DcId;
use sprig_meta::Revision;
use sprig_meta::bootstrap::DcRecord;
use sprig_provision::PrecheckReport;

use crate::dc::DcView;

/// `POST /admin/v1/clusters`: `Clusters.RegisterCluster`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegisterClusterRequest {
    /// The data center; defaults to the serving controller's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dc: Option<DcId>,
    /// A name unique within the data center.
    pub name: String,
    /// The topology.
    pub topology: TopologyKind,
    /// Who made the nodes; only `external` registers through this call.
    #[serde(default = "external")]
    pub provisioner: ProvisionerKind,
    /// The nodes.
    pub nodes: Vec<NodeSpec>,
    /// References to the system-account credentials.
    pub secrets: SecretRefs,
    /// Declared capacity.
    #[serde(default)]
    pub capacity: Capacity,
    /// `[declared]` facts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared: Option<Declared>,
    /// Run the prechecks only.
    #[serde(default)]
    pub dry_run: bool,
}

fn external() -> ProvisionerKind {
    ProvisionerKind::External
}

/// A cluster record with its revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterView {
    /// The record.
    #[serde(flatten)]
    pub record: ClusterRecord,
    /// The record's mod revision.
    pub mod_revision: Revision,
    /// The latest health assessment per node, from the DC leader's last
    /// pass (absent elsewhere).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_report: Option<HealthReport>,
    /// The tuning plan as `SET GLOBAL` statements (chapter 09); empty until
    /// tuning exists (step 10).
    #[serde(default)]
    pub tuning_plan: Vec<String>,
}

/// The answer to `RegisterCluster`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegisterClusterResponse {
    /// The registered cluster; absent for a dry run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<ClusterView>,
    /// What the prechecks found.
    pub precheck: PrecheckReport,
}

/// `GET /admin/v1/clusters`: `Clusters.ListClusters`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListClustersResponse {
    /// The data center listed.
    pub dc: DcId,
    /// Its clusters, by id.
    pub clusters: Vec<ClusterView>,
}

/// `POST /admin/v1/clusters/{id}:probe`: `Clusters.ProbeCluster`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeClusterRequest {
    /// Also run the active probes: XA, features, routines (`--full`).
    #[serde(default)]
    pub full: bool,
}

/// The answer to `ProbeCluster`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProbeClusterResponse {
    /// The record after the probe.
    pub cluster: ClusterView,
    /// How deep it probed.
    pub depth: ProbeDepth,
    /// What the published profile changed.
    pub changes: Vec<FieldChange>,
    /// Upgrades seen but held for `cluster.capability_upgrade_sustain_seconds`.
    pub held: Vec<String>,
}

/// `POST /admin/v1/dcs`: `DataCenters.RegisterDc`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegisterDcRequest {
    /// The DC.
    pub dc: DcId,
    /// `full` or `witness`.
    #[serde(default = "full")]
    pub kind: String,
    /// Tags.
    #[serde(default)]
    pub tags: Map<String, Value>,
    /// Whether a global etcd voter runs there.
    #[serde(default)]
    pub global_voter: bool,
}

fn full() -> String {
    "full".to_owned()
}

/// The answer to `RegisterDc`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegisterDcResponse {
    /// Whether this call wrote the record.
    pub created: bool,
    /// The record.
    pub record: DcRecord,
}

/// `GET /admin/v1/dcs`: `DataCenters.ListDcs`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListDcsResponse {
    /// Every DC.
    pub dcs: Vec<DcRecord>,
}

/// `GET /admin/v1/dcs/{dc}`: `DataCenters.GetDc`.
pub type GetDcResponse = DcView;

/// An error, as chapter 37 renders it for REST.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    /// The SprigDB (or MariaDB) error number.
    pub code: u16,
    /// `ER_SPRIG_<NAME>`.
    pub name: String,
    /// Client-safe text.
    pub message: String,
    /// The request's id in the logs.
    pub trace_id: String,
}
