//! The Admin API's messages (`sprig.admin.v1.Clusters`, `DataCenters`,
//! `Tenants` and `Settings`, chapter 37), as the REST face carries them: JSON bodies under
//! `/admin/v1/`. `sprigctl` decodes the same types.
//!
//! Until the protobuf definitions exist (chapter 37 steps 1-4), these
//! serde types are the contract; records appear as they are stored in etcd
//! (64-bit integers as JSON numbers, not proto3's strings).

use serde::{Deserialize, Serialize};
use serde_json::Map;
use sprig_cluster::health::HealthReport;
use sprig_cluster::probe::ProbeDepth;
use sprig_cluster::profile::FieldChange;
use sprig_cluster::record::{
    Capacity, ClusterRecord, Declared, NodeSpec, ProvisionerKind, SecretRefs, TopologyKind,
};
use sprig_common::ids::{ClusterId, DcId, TenantId};
use sprig_meta::Revision;
use sprig_meta::bootstrap::DcRecord;
use sprig_provision::PrecheckReport;
pub use sprig_settings::{Bounds, Value};

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

/// `POST /admin/v1/clusters/{id}:setNodes`: `Clusters.SetClusterNodes`
/// (r2opfix): the provisioner's current nodes, after a scale or a
/// replacement. A node whose address changes, and a removed node, lose
/// their pinned identity; the next probe pins the server now there.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetClusterNodesRequest {
    /// Every node, in order.
    pub nodes: Vec<NodeSpec>,
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
    pub tags: Map<String, serde_json::Value>,
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

// ---- Tenants (chapter 08 step 9, chapter 23 step 16) ----

/// `POST /admin/v1/tenants`: create a tenant, optionally under a parent,
/// and bootstrap its catalog.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTenantRequest {
    /// The name, a lowercase DNS label.
    pub name: String,
    /// The parent's name; a root tenant without it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    /// The cluster for the catalog; the active cluster with the fewest
    /// tenants without it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<ClusterId>,
}

/// A tenant as the Admin API shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantView {
    /// The id.
    pub tenant_id: TenantId,
    /// The name.
    pub name: String,
    /// The directory state.
    pub state: String,
    /// The home DC.
    pub home_dc: DcId,
    /// The parent's id; 0 for a root.
    pub parent_tenant_id: u64,
    /// The parent's name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    /// Ids root first, self included.
    pub path: String,
    /// Root 0.
    pub depth: u32,
    /// Children.
    pub child_count: u64,
    /// The last allocated catalog version.
    pub catalog_version: u64,
    /// The last allocated auth version.
    pub auth_version: u64,
    /// The catalog version of the last tenant-scope settings change.
    pub settings_version: u64,
    /// The cluster holding the catalog, once placed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<ClusterId>,
    /// Suspended by an administrator (its own flag; an ancestor's covers
    /// it too).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub suspended: bool,
    /// Why, while suspended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspended_reason: Option<String>,
    /// Ancestors' names, root first (`show` only).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ancestry: Vec<String>,
    /// When it was created.
    #[serde(default)]
    pub created_at: String,
    /// The directory entry's mod revision.
    pub mod_revision: Revision,
}

/// The answer to `POST /admin/v1/tenants`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTenantResponse {
    /// `false` when the tenant existed (a re-run continues its bootstrap).
    pub created: bool,
    /// The tenant after the call.
    pub tenant: TenantView,
    /// Why its catalog went where it did: `requested` or `fewest_tenants`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<String>,
}

/// `GET /admin/v1/tenants`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListTenantsResponse {
    /// Every tenant, by id.
    pub tenants: Vec<TenantView>,
}

// ---- Settings (chapter 08 "Setting a value", chapter 23 "Settings: writes,
// bounds and watches", chapter 37 `Settings`, chapter 47 "Explaining
// settings") ----

/// Where a setting row lives, written as chapter 47's `--scope`:
/// `global`, `dc` (the serving controller's DC) or `dc:<id>`,
/// `cluster:<id>`, `tenant:<name>`, `db:<tenant>.<db>` and
/// `object:<tenant>.<db>.<object>`. On the wire it is that string.
///
/// Tenant names hold no dots, and the database is the text up to the next
/// dot; the object name is the rest.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScopeRef {
    /// Every data center.
    Global,
    /// One data center; `None` is the serving controller's.
    Dc(Option<DcId>),
    /// One cluster.
    Cluster(ClusterId),
    /// A tenant.
    Tenant(String),
    /// A database of a tenant.
    Database {
        /// The tenant's name.
        tenant: String,
        /// The database's name.
        db: String,
    },
    /// An object (table, collection, ...) of a tenant.
    Object {
        /// The tenant's name.
        tenant: String,
        /// The database's name.
        db: String,
        /// The object's name.
        object: String,
    },
}

impl ScopeRef {
    /// The scope kind: `global`, `dc`, `cluster`, `tenant`, `database` or
    /// `object`.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            ScopeRef::Global => "global",
            ScopeRef::Dc(_) => "dc",
            ScopeRef::Cluster(_) => "cluster",
            ScopeRef::Tenant(_) => "tenant",
            ScopeRef::Database { .. } => "database",
            ScopeRef::Object { .. } => "object",
        }
    }

    /// Kept in etcd (`global`, `dc`, `cluster`, chapter 23) rather than in
    /// a tenant's catalog (chapter 08).
    #[must_use]
    pub fn is_etcd(&self) -> bool {
        matches!(
            self,
            ScopeRef::Global | ScopeRef::Dc(_) | ScopeRef::Cluster(_)
        )
    }

    /// The tenant of a catalog scope.
    #[must_use]
    pub fn tenant(&self) -> Option<&str> {
        match self {
            ScopeRef::Tenant(t) | ScopeRef::Database { tenant: t, .. } => Some(t),
            ScopeRef::Object { tenant, .. } => Some(tenant),
            _ => None,
        }
    }
}

impl std::fmt::Display for ScopeRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScopeRef::Global => f.write_str("global"),
            ScopeRef::Dc(None) => f.write_str("dc"),
            ScopeRef::Dc(Some(d)) => write!(f, "dc:{d}"),
            ScopeRef::Cluster(c) => write!(f, "cluster:{c}"),
            ScopeRef::Tenant(t) => write!(f, "tenant:{t}"),
            ScopeRef::Database { tenant, db } => write!(f, "db:{tenant}.{db}"),
            ScopeRef::Object { tenant, db, object } => write!(f, "object:{tenant}.{db}.{object}"),
        }
    }
}

impl std::str::FromStr for ScopeRef {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let s = s.trim();
        let (kind, target) = match s.split_once(':') {
            Some((k, t)) => (k, Some(t)),
            None => (s, None),
        };
        let need = |what: &str| {
            target
                .filter(|t| !t.is_empty())
                .ok_or_else(|| format!("scope {s:?}: {kind} needs {what}, as {kind}:<{what}>"))
        };
        let dotted = |t: &str, parts: usize| -> Result<Vec<String>, String> {
            let v: Vec<String> = t.splitn(parts, '.').map(str::to_owned).collect();
            if v.len() == parts && v.iter().all(|p| !p.is_empty()) {
                Ok(v)
            } else if parts == 2 {
                Err(format!("scope {s:?}: expected db:<tenant>.<db>"))
            } else {
                Err(format!(
                    "scope {s:?}: expected object:<tenant>.<db>.<object>"
                ))
            }
        };
        match kind {
            "global" if target.is_none() => Ok(ScopeRef::Global),
            "dc" => match target {
                None | Some("") => Ok(ScopeRef::Dc(None)),
                Some(d) => DcId::new(d)
                    .map(|d| ScopeRef::Dc(Some(d)))
                    .map_err(|e| format!("scope {s:?}: {e}")),
            },
            "cluster" => ClusterId::new(need("id")?)
                .map(ScopeRef::Cluster)
                .map_err(|e| format!("scope {s:?}: {e}")),
            "tenant" => Ok(ScopeRef::Tenant(need("name")?.to_owned())),
            "db" | "database" => {
                let mut v = dotted(need("tenant>.<db")?, 2)?.into_iter();
                Ok(ScopeRef::Database {
                    tenant: v.next().unwrap_or_default(),
                    db: v.next().unwrap_or_default(),
                })
            }
            "object" => {
                let mut v = dotted(need("tenant>.<db>.<object")?, 3)?.into_iter();
                Ok(ScopeRef::Object {
                    tenant: v.next().unwrap_or_default(),
                    db: v.next().unwrap_or_default(),
                    object: v.next().unwrap_or_default(),
                })
            }
            _ => Err(format!(
                "scope {s:?}: expected global, dc[:<id>], cluster:<id>, tenant:<name>, \
                 db:<tenant>.<db> or object:<tenant>.<db>.<object>"
            )),
        }
    }
}

impl Serialize for ScopeRef {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ScopeRef {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// `GET /admin/v1/settings`: which keys to list.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListSettingKeysQuery {
    /// Keys starting with this text, such as `catalog.`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// `true`: only keys only operators may set (operator-only, or
    /// accepting no tenant, database or object scope); `false`: only keys
    /// tenants may set.
    /// A query string carries it as text.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "bool_or_text"
    )]
    pub operator_only: Option<bool>,
    /// Only keys accepting this scope kind (`global`, `dc`, `cluster`,
    /// `tenant`, `database`, `object`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

/// `true`, `false` or their text, as a query string carries them.
fn bool_or_text<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<bool>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum B {
        Bool(bool),
        Text(String),
    }
    match Option::<B>::deserialize(d)? {
        None => Ok(None),
        Some(B::Bool(b)) => Ok(Some(b)),
        Some(B::Text(t)) => match t.as_str() {
            "true" | "1" | "yes" => Ok(Some(true)),
            "false" | "0" | "no" => Ok(Some(false)),
            _ => Err(serde::de::Error::custom(format!(
                "expected true or false, not {t:?}"
            ))),
        },
    }
}

/// One declared key, as the registry holds it (chapter 03, "Declaring a
/// key").
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettingKeyView {
    /// The key.
    pub key: String,
    /// `bool`, `int`, `float` or `string`.
    #[serde(rename = "type")]
    pub kind: String,
    /// The built-in default.
    pub default: Value,
    /// The scopes that may set it, widest first.
    pub scopes: Vec<String>,
    /// Bounds no scope can go outside of.
    pub hard_bounds: Bounds,
    /// `live`, `new_sessions` or `rebind`.
    pub apply: String,
    /// Tenants may not set it.
    pub operator_only: bool,
    /// Tenants may set it: not operator-only, and it accepts the tenant,
    /// database or object scope.
    #[serde(default)]
    pub tenant_settable: bool,
    /// The chapter that owns it.
    pub owner: u8,
    /// One line of documentation.
    pub doc: String,
}

/// The answer to `GET /admin/v1/settings`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListSettingKeysResponse {
    /// The matching keys, by key.
    pub keys: Vec<SettingKeyView>,
}

/// `GET /admin/v1/settings/{key}`: the context to resolve in. Every part
/// is optional; without any, the key resolves for the serving controller's
/// DC (dc and global).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettingContextQuery {
    /// A tenant's name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// A database of the tenant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db: Option<String>,
    /// An object of the database (or `<db>.<object>`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    /// A cluster, for work on one cluster.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
    /// The data center; the serving controller's by default (the only one
    /// it mirrors in R0).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dc: Option<String>,
}

/// The context a key was resolved in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettingContextView {
    /// The data center.
    pub dc: DcId,
    /// The cluster.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<ClusterId>,
    /// The tenant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// The database.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db: Option<String>,
    /// The object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
}

/// One scope of a resolution chain.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettingStepView {
    /// `default`, `global`, `dc`, `cluster`, `ancestor`, `tenant`,
    /// `database` or `object`.
    pub scope: String,
    /// Which one: the DC or cluster id, the (ancestor) tenant's name,
    /// `<tenant>.<db>` or `<tenant>.<db>.<object>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Whether the key accepts this scope.
    pub accepted: bool,
    /// The value stored here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    /// The bounds this scope sets on narrower ones.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bounds: Option<Bounds>,
    /// Hard bounds and every wider scope's bounds: what a value here must
    /// fit.
    pub bounds_in_force: Bounds,
    /// `effective`, `overridden`, `unset`, `bounds_only`, `not_accepted`,
    /// `ignored_locked`, `ignored_not_allowed` or `ignored_wrong_type`.
    pub outcome: String,
    /// The value after clamping, when clamping changed it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clamped_to: Option<Value>,
    /// Who last changed the row (`op:<name>`, `user:<id>`, `system`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub set_by: Option<String>,
    /// When, RFC 3339 UTC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub set_at: Option<String>,
    /// Why (etcd scopes record a reason).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The catalog version of the row's last change (catalog scopes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
}

/// The answer to `GET /admin/v1/settings/{key}` (and `{key}:explain`):
/// the declaration, the effective value in the context, where it came
/// from and the whole chain (chapter 47, "Explaining settings").
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettingExplanation {
    /// The key.
    pub key: String,
    /// Its declaration.
    pub declaration: SettingKeyView,
    /// The context.
    pub context: SettingContextView,
    /// The effective value.
    pub value: Value,
    /// The scope it came from (as in [`SettingStepView::scope`]).
    pub source: String,
    /// Which one (as in [`SettingStepView::target`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_target: Option<String>,
    /// The value was moved into the bounds in force at its scope.
    pub clamped: bool,
    /// The bounds in force after every scope of the context.
    pub bounds: Bounds,
    /// The default first, then every scope from the widest to the narrowest.
    pub chain: Vec<SettingStepView>,
    /// The tenant's catalog version the rows were read at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_version: Option<u64>,
}

/// `PUT /admin/v1/settings/{key}`: `Settings.SetSetting`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SetSettingRequest {
    /// Where.
    pub scope: Option<ScopeRef>,
    /// The value as typed JSON (`8`, `true`, `"on"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    /// The value as text, parsed with the key's type (`8`, `true`, `on`);
    /// what `sprigctl` sends.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_text: Option<String>,
    /// Why (audited).
    #[serde(default)]
    pub reason: String,
    /// Who, recorded as `op:<actor>` (the bearer token doesn't name a
    /// person in R0); `admin` without it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

/// `POST /admin/v1/settings/{key}:reset` (and `DELETE
/// /admin/v1/settings/{key}?scope=...`, which resets the value).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetSettingRequest {
    /// Where.
    pub scope: Option<ScopeRef>,
    /// Reset the bounds instead of the value.
    #[serde(default)]
    pub bounds: bool,
    /// Reset both the value and the bounds.
    #[serde(default)]
    pub all: bool,
    /// Why.
    #[serde(default)]
    pub reason: String,
    /// Who.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

/// `POST /admin/v1/settings/{key}:bounds`: set parts of the bounds a scope
/// puts on narrower scopes; parts not named are kept.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SetBoundsRequest {
    /// Where.
    pub scope: Option<ScopeRef>,
    /// The smallest value narrower scopes may set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    /// The largest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    /// The only values narrower scopes may set, as typed JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed: Option<Vec<Value>>,
    /// The same as text, parsed with the key's type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_text: Option<Vec<String>>,
    /// Parts to clear: `min`, `max`, `allowed`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub clear: Vec<String>,
    /// Why.
    #[serde(default)]
    pub reason: String,
    /// Who.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

/// `POST /admin/v1/settings/{key}:lock` and `{key}:unlock`: lock or unlock
/// the key for narrower scopes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockSettingRequest {
    /// Where.
    pub scope: Option<ScopeRef>,
    /// Why.
    #[serde(default)]
    pub reason: String,
    /// Who.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

/// What a write changes, as the service receives it from any of the write
/// routes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum SettingOp {
    /// Set the value.
    Set {
        /// Typed JSON.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value: Option<Value>,
        /// Text, parsed with the key's type.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_text: Option<String>,
    },
    /// Reset the value, the bounds, or both.
    Reset {
        /// The value.
        value: bool,
        /// The bounds.
        bounds: bool,
    },
    /// Change parts of the bounds.
    Bounds {
        /// New minimum.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min: Option<f64>,
        /// New maximum.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max: Option<f64>,
        /// New allowed list, typed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        allowed: Option<Vec<Value>>,
        /// New allowed list, as text.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        allowed_text: Option<Vec<String>>,
        /// Parts to clear: `min`, `max`, `allowed`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        clear: Vec<String>,
    },
    /// Lock (`true`) or unlock the key for narrower scopes.
    Lock {
        /// Lock or unlock.
        locked: bool,
    },
}

impl SettingOp {
    /// `set`, `reset`, `bounds`, `lock` or `unlock`.
    #[must_use]
    pub fn action(&self) -> &'static str {
        match self {
            SettingOp::Set { .. } => "set",
            SettingOp::Reset { .. } => "reset",
            SettingOp::Bounds { .. } => "bounds",
            SettingOp::Lock { locked: true } => "lock",
            SettingOp::Lock { locked: false } => "unlock",
        }
    }
}

/// One write, from any of the write routes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettingWrite {
    /// Where.
    pub scope: ScopeRef,
    /// What.
    pub op: SettingOp,
    /// Who (without `op:`).
    pub actor: String,
    /// Why.
    pub reason: String,
}

/// One stored row, before or after a write.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SettingRowView {
    /// The value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    /// The bounds on narrower scopes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bounds: Option<Bounds>,
    /// Who changed it last.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub set_by: Option<String>,
    /// When.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub set_at: Option<String>,
    /// Why (etcd scopes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// A narrower value that new bounds clamp or ignore at read time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettingWarningView {
    /// Where the value is stored, in [`ScopeRef`] syntax.
    pub scope: String,
    /// The stored value.
    pub value: Value,
    /// What happens to it.
    pub message: String,
}

/// The answer to every write route.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettingWriteResponse {
    /// The key.
    pub key: String,
    /// Where.
    pub scope: ScopeRef,
    /// `set`, `reset`, `bounds`, `lock` or `unlock`.
    pub action: String,
    /// `false` when there was nothing to change.
    pub changed: bool,
    /// The catalog version of a catalog-scope write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    /// The row before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous: Option<SettingRowView>,
    /// The row after; absent when nothing is left of it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current: Option<SettingRowView>,
    /// Narrower values the new bounds clamp or ignore.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<SettingWarningView>,
    /// Chapter 08's summary of the warnings ("2 values will be clamped").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    /// Who it was recorded as.
    pub set_by: String,
}

/// One row of a tenant's catalog `settings` table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TenantSettingRowView {
    /// `tenant`, `database`, `object` or `ancestor` (a read-only copy of
    /// an ancestor's tenant-scope row).
    pub scope: String,
    /// The tenant, `<tenant>.<db>`, `<tenant>.<db>.<object>` or the
    /// ancestor's name.
    pub target: String,
    /// The key.
    pub key: String,
    /// The value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    /// The bounds on narrower scopes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bounds: Option<Bounds>,
    /// Who changed it last.
    pub set_by: String,
    /// When.
    pub set_at: String,
    /// The catalog version of its last change.
    pub version: u64,
}

/// The answer to `GET /admin/v1/tenants/{name}/settings`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TenantSettingsResponse {
    /// The tenant.
    pub tenant: String,
    /// Its id.
    pub tenant_id: TenantId,
    /// The catalog version read.
    pub catalog_version: u64,
    /// The rows with a value or bounds: the tenant's own, then the copies
    /// of its ancestors' rows. Internal `sprig.` rows are left out.
    pub rows: Vec<TenantSettingRowView>,
}
