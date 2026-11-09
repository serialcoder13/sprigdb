//! The cluster record (chapter 09, "Cluster record"), stored at
//! `/sprig/dc/<dc_id>/clusters/<cluster_id>`.

use std::collections::BTreeMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sprig_common::ids::{ClusterId, DcId, NodeId};
use sprig_meta::Versioned;

use crate::health::ClusterHealth;
use crate::profile::CapabilityProfile;

/// A cluster's replication topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyKind {
    /// Galera: every node writable, certification-based replication.
    Galera,
    /// One primary with asynchronous or semi-synchronous replicas.
    PrimaryReplica,
    /// One node.
    SingleNode,
    /// A managed service: what probes can't see is declared.
    External,
}

impl TopologyKind {
    /// The record's spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Galera => "galera",
            Self::PrimaryReplica => "primary_replica",
            Self::SingleNode => "single_node",
            Self::External => "external",
        }
    }
}

impl fmt::Display for TopologyKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Who created the nodes (chapter 27).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProvisionerKind {
    /// Registered by an operator.
    External,
    /// The Kubernetes operator.
    Kubernetes,
    /// The VM agent.
    Vm,
}

/// The registration state machine (chapter 09, "State machines").
///
/// ```text
///  register ─► probing ─gates pass─► active ─drain─► draining ─empty─► drained ─► retired
///                 │                     ▲
///                 └─gate fails─► rejected ─re-probe passes
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminState {
    /// Registered; the first probe hasn't decided yet.
    Probing,
    /// In service.
    Active,
    /// A registration gate failed.
    Rejected,
    /// No new placements; shards are moving away.
    Draining,
    /// Empty.
    Drained,
    /// Gone for good.
    Retired,
}

impl AdminState {
    /// Whether the state machine allows `self -> to`.
    #[must_use]
    pub fn can_become(self, to: AdminState) -> bool {
        use AdminState::{Active, Drained, Draining, Probing, Rejected, Retired};
        matches!(
            (self, to),
            (Probing, Active | Rejected)
                | (Rejected, Active)
                | (Active, Draining)
                | (Draining, Drained)
                | (Drained, Retired)
        )
    }

    /// The record's spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Probing => "probing",
            Self::Active => "active",
            Self::Rejected => "rejected",
            Self::Draining => "draining",
            Self::Drained => "drained",
            Self::Retired => "retired",
        }
    }
}

impl fmt::Display for AdminState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One node of the record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSpec {
    /// The node id, unique within the cluster.
    pub node: NodeId,
    /// `host:port`.
    pub addr: String,
    /// Availability zone, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
}

/// References to the system-account secrets; never the secrets themselves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretRefs {
    /// `sprig_ctl`.
    pub ctl: String,
    /// `sprig_repl`.
    pub repl: String,
}

/// Declared capacity, for placement and tuning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Capacity {
    /// Storage, GB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_gb: Option<u64>,
    /// Sustainable row writes per second.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_rows_per_sec: Option<u64>,
    /// Busy milliseconds per second across the cluster.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub busy_ms_per_sec: Option<u64>,
    /// Connections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connections: Option<u64>,
    /// Server memory, GB; without it tuning only recommends.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_gb: Option<u64>,
}

/// One Galera writer and its weight.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WriterWeight {
    /// The node.
    pub node: NodeId,
    /// Rendezvous weight.
    pub weight: f64,
}

/// The Galera writer set (chapter 09, "Galera writer choice").
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GaleraWriters {
    /// Raised on every change of the set.
    pub writer_epoch: u64,
    /// The set.
    pub writers: Vec<WriterWeight>,
}

/// How much a declaration is trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Trust {
    /// The operator vouches for it.
    Asserted,
    /// Informational only.
    Unverified,
}

/// Who fails an external cluster over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeclaredFailover {
    /// The managed service.
    Provider,
    /// Nobody.
    None,
}

/// What an operator declares about an external cluster (`[declared]`).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Declared {
    /// The writer endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_endpoint: Option<String>,
    /// Reader endpoints.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reader_endpoints: Vec<String>,
    /// Who fails it over.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failover: Option<DeclaredFailover>,
    /// Recovery point objective, seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpo_seconds: Option<u64>,
    /// How far to trust the declaration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trust: Option<Trust>,
    /// Data centers holding replicas, for residency.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replica_dcs: Vec<DcId>,
    /// Storage, GB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_gb: Option<u64>,
    /// Memory, GB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_gb: Option<u64>,
}

/// The published health.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthSummary {
    /// The state.
    pub state: ClusterHealth,
    /// Since when, RFC 3339 UTC.
    pub since: String,
}

/// Server tuning state (chapter 09, "Server tuning"; step 10).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TuningState {
    /// Variables applied, by name.
    #[serde(default)]
    pub applied: BTreeMap<String, u64>,
    /// Variables waiting for the next restart.
    #[serde(default)]
    pub pending_restart: BTreeMap<String, u64>,
}

/// The cluster record.
///
/// `cluster_id` is the textual id of chapter 03 (`<dc>-c<number>`); the
/// number is allocated globally and is what chapter 09's JSON calls
/// `cluster_id`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterRecord {
    /// The id, such as `mum1-c7`.
    pub cluster_id: ClusterId,
    /// The globally allocated number, such as 7.
    pub cluster_number: u32,
    /// The operator's name, unique within the data center.
    pub name: String,
    /// The data center.
    pub dc_id: DcId,
    /// The topology.
    pub topology: TopologyKind,
    /// Who made the nodes.
    pub provisioner: ProvisionerKind,
    /// Registration state.
    pub admin_state: AdminState,
    /// The nodes.
    pub nodes: Vec<NodeSpec>,
    /// Secret references.
    pub secrets: SecretRefs,
    /// Declared capacity.
    #[serde(default)]
    pub capacity: Capacity,
    /// Galera writer set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub galera: Option<GaleraWriters>,
    /// Operator declarations (external clusters).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared: Option<Declared>,
    /// Raised on every published profile.
    #[serde(default)]
    pub profile_version: u64,
    /// The published capability profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<CapabilityProfile>,
    /// The published health.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health: Option<HealthSummary>,
    /// Server tuning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tuning: Option<TuningState>,
    /// Why the cluster is `rejected`, each with the fix where there is one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gaps: Vec<String>,
    /// Each node's identity (`<hostname>#<server_id>`), pinned by the first probe that reached
    /// it at its recorded address; a probe that finds another server there
    /// refuses the node ([`crate::probe::check_identity`]). Changing a
    /// node's address forgets its pin (r2opfix).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub node_identities: BTreeMap<NodeId, String>,
}

impl Versioned for ClusterRecord {
    const V: u32 = 1;
}

/// The global id entry at `/sprig/g/cluster-ids/<number>`: lets any gateway
/// find a cluster's data center.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterIdEntry {
    /// The data center.
    pub dc_id: DcId,
    /// The operator's name.
    pub name: String,
    /// The textual id.
    pub cluster_id: ClusterId,
}

impl Versioned for ClusterIdEntry {
    const V: u32 = 1;
}

/// RFC 3339 UTC with seconds, such as `2026-10-31T09:12:03Z`.
#[must_use]
#[allow(clippy::many_single_char_names)] // the published algorithm's names
pub fn rfc3339(t: SystemTime) -> String {
    let secs = t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let days = i64::try_from(secs / 86_400).unwrap_or(0);
    let rem = secs % 86_400;
    // Howard Hinnant's civil-from-days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sprig_meta::Record;

    use super::*;

    #[test]
    fn admin_states_follow_the_diagram() {
        use AdminState::*;
        assert!(Probing.can_become(Active));
        assert!(Probing.can_become(Rejected));
        assert!(Rejected.can_become(Active));
        assert!(Active.can_become(Draining));
        assert!(Draining.can_become(Drained));
        assert!(Drained.can_become(Retired));
        assert!(!Active.can_become(Probing));
        assert!(!Retired.can_become(Active));
        assert!(!Draining.can_become(Active));
        assert!(!Probing.can_become(Draining));
    }

    #[test]
    fn rfc3339_formats_utc() {
        assert_eq!(rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        let t = UNIX_EPOCH + Duration::from_secs(1_793_438_723);
        assert_eq!(rfc3339(t), "2026-10-31T09:25:23Z");
        let leap = UNIX_EPOCH + Duration::from_secs(951_782_400); // 2000-02-29
        assert_eq!(rfc3339(leap), "2000-02-29T00:00:00Z");
    }

    #[test]
    fn record_round_trips_with_unknown_fields() {
        let stored = br#"{"v":1,"cluster_id":"mum1-c7","cluster_number":7,"name":"mum1-g1",
            "dc_id":"mum1","topology":"galera","provisioner":"external","admin_state":"active",
            "nodes":[{"node":"n1","addr":"db1.mum1.internal:3306","zone":"a"}],
            "secrets":{"ctl":"vault:kv/sprig/mum1/c7/ctl","repl":"vault:kv/sprig/mum1/c7/repl"},
            "capacity":{"storage_gb":4000,"memory_gb":256},
            "galera":{"writer_epoch":4,"writers":[{"node":"n1","weight":1.0}]},
            "profile_version":12,
            "health":{"state":"healthy","since":"2026-10-31T09:12:03Z"},
            "tuning":{"applied":{"table_open_cache":60000},"pending_restart":{"open_files_limit":130000}},
            "from_a_newer_build":true}"#;
        let r = Record::<ClusterRecord>::decode(stored).unwrap();
        assert_eq!(r.body.cluster_number, 7);
        assert_eq!(r.body.topology, TopologyKind::Galera);
        assert_eq!(
            r.body.health.as_ref().unwrap().state,
            ClusterHealth::Healthy
        );
        let again: serde_json::Value = serde_json::from_slice(&r.encode().unwrap()).unwrap();
        assert_eq!(again["from_a_newer_build"], true);
        assert_eq!(
            again["tuning"]["pending_restart"]["open_files_limit"],
            130_000
        );
    }
}
