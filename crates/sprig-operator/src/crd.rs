//! The operator's two custom resources, `SprigCluster` and
//! `SprigClusterPool` in `sprigdb.io/v1alpha1` (chapter 27, "Kubernetes:
//! CRDs and their mapping"), and their `CustomResourceDefinition`s.
//!
//! ```yaml
//! apiVersion: sprigdb.io/v1alpha1
//! kind: SprigCluster
//! metadata: { name: fra1-c7, namespace: sprigdb }
//! spec:
//!   topology: replication                   # galera | replication | single
//!   nodes: 3
//!   mariadb: { image: "mariadb:12.3" }
//!   storage: { size: 500Gi, storageClassName: fast-ssd }
//!   failoverDriver: sprig                   # sprig (default) | operator
//!   backup: { schedule: "0 2 * * *", target: "s3://sprig-fra1-backups" }
//! status: { clusterId: fra1-c7, phase: Active }
//! ```
//!
//! The CRD manifests ([`crds`]) carry `OpenAPI` v3 schemas written here, and
//! the Helm chart's `charts/sprigdb/crds/` holds them as generated YAML (a
//! test keeps the two equal).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::kube::ApiResource;

/// The API group.
pub const GROUP: &str = "sprigdb.io";
/// The version.
pub const VERSION: &str = "v1alpha1";
/// `apiVersion` of both kinds.
pub const API_VERSION: &str = "sprigdb.io/v1alpha1";

/// `SprigCluster`.
pub const SPRIG_CLUSTER: ApiResource = ApiResource {
    group: GROUP,
    version: VERSION,
    kind: "SprigCluster",
    plural: "sprigclusters",
};

/// `SprigClusterPool`.
pub const SPRIG_CLUSTER_POOL: ApiResource = ApiResource {
    group: GROUP,
    version: VERSION,
    kind: "SprigClusterPool",
    plural: "sprigclusterpools",
};

/// The label naming a cluster's pool.
pub const POOL_LABEL: &str = "sprigdb.io/pool";
/// The label every object the operator creates carries.
pub const MANAGED_BY: (&str, &str) = ("app.kubernetes.io/managed-by", "sprigdb-operator");
/// The label naming the `SprigCluster` an object belongs to.
pub const CLUSTER_LABEL: &str = "sprigdb.io/cluster";

/// `spec.topology`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Topology {
    /// Galera.
    Galera,
    /// A primary with replicas, semi-sync `AFTER_SYNC`.
    Replication,
    /// One node.
    Single,
}

/// `spec.failoverDriver`: who promotes a replica when the primary fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailoverDriver {
    /// mariadb-operator alone (`replication.primary.autoFailover: true`):
    /// it promotes the replica with the highest applied position, skipping
    /// any with relay log left, so zero loss isn't guaranteed (r2kind).
    Operator,
    /// SprigDB chooses (the default): `autoFailover: false`, and the SprigDB
    /// operator picks the most advanced replica and sets
    /// `replication.primary.podIndex`, so mariadb-operator's switchover
    /// promotes it ([`crate::failover`]). Chapter 25's controller loop
    /// leaves Kubernetes clusters alone either way.
    #[default]
    Sprig,
}

/// `spec.mariadb`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MariaDbImage {
    /// The container image, such as `mariadb:12.3`.
    pub image: String,
}

/// `spec.storage`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageSpec {
    /// A Kubernetes quantity, such as `500Gi`.
    pub size: String,
    /// The storage class; the cluster default without it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class_name: Option<String>,
}

/// `spec.backup`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupSpec {
    /// Cron schedule of full backups.
    pub schedule: String,
    /// `s3://bucket[/prefix]`: the DC's object store (`storage.object_url`).
    pub target: String,
    /// How long backups are kept, such as `720h`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retention: Option<String>,
    /// The S3 endpoint, when not AWS's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// The Secret holding `access-key-id` and `secret-access-key`
    /// (`storage.object_credentials_ref`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials_secret: Option<String>,
}

/// `SprigCluster.spec`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SprigClusterSpec {
    /// The topology.
    pub topology: Topology,
    /// Nodes (pods); 1 for `single`.
    #[serde(default = "one")]
    pub nodes: u8,
    /// The MariaDB image.
    pub mariadb: MariaDbImage,
    /// Per-pod storage.
    pub storage: StorageSpec,
    /// Who fails the primary over.
    #[serde(default)]
    pub failover_driver: FailoverDriver,
    /// Backups and the binlog archive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup: Option<BackupSpec>,
    /// Zones to spread the pods over (`topology.kubernetes.io/zone`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub zones: Vec<String>,
    /// Container resources (`requests`, `limits`), passed through.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Value>,
}

fn one() -> u8 {
    1
}

impl SprigClusterSpec {
    /// The pods the cluster has: `nodes`, or 1 for `single`.
    #[must_use]
    pub fn replicas(&self) -> u8 {
        if self.topology == Topology::Single {
            1
        } else {
            self.nodes.max(1)
        }
    }
}

/// `status.phase` (chapter 27, "State machines"; `requested` is a
/// `SprigCluster` without a status yet).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum Phase {
    /// Not reconciled yet.
    #[default]
    Pending,
    /// The `MariaDB` resource exists and isn't Ready.
    Provisioning,
    /// Ready: system accounts are being created.
    Bootstrapping,
    /// Registered; the controller's probes run.
    Probing,
    /// In use.
    Active,
    /// Not Ready within the timeout, or a step failed past its retries.
    Failed,
}

impl Phase {
    /// The status spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "Pending",
            Self::Provisioning => "Provisioning",
            Self::Bootstrapping => "Bootstrapping",
            Self::Probing => "Probing",
            Self::Active => "Active",
            Self::Failed => "Failed",
        }
    }
}

/// One node's advertised address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeAddress {
    /// `n0`, `n1`, ...: the pod index.
    pub node: String,
    /// `host:port` as other DCs reach it.
    pub addr: String,
}

/// `SprigCluster.status`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SprigClusterStatus {
    /// The cluster record's id, once registered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,
    /// The record's number (the GTID domain), once registered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_number: Option<u32>,
    /// The phase.
    #[serde(default)]
    pub phase: Phase,
    /// Why the phase is what it is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The nodes' addresses, as registered.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<NodeAddress>,
    /// When the current attempt to reach `Active` started, Unix seconds:
    /// the 30-minute Ready timeout counts from here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_unix: Option<u64>,
    /// The last phase change, RFC 3339.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
    /// `metadata.generation` the status describes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// Nodes (`n<i>`) a failover left broken whose repair isn't provably
    /// safe: they need a rebuild ([`crate::failover`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rebuild: Vec<String>,
    /// The last failover SprigDB drove.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failover: Option<FailoverRecord>,
}

/// `status.lastFailover`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FailoverRecord {
    /// The pod index that failed.
    pub from: u8,
    /// The pod index promoted.
    pub to: u8,
    /// When, RFC 3339.
    pub time: String,
    /// Why that replica.
    pub reason: String,
}

/// `SprigClusterPool.spec`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SprigClusterPoolSpec {
    /// Every cluster of the pool is made from this.
    pub template: SprigClusterSpec,
    /// Created at install and kept.
    #[serde(default)]
    pub min_clusters: u32,
    /// The most clusters `cluster_provision` jobs may create.
    pub max_clusters: u32,
}

/// `SprigClusterPool.status`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SprigClusterPoolStatus {
    /// Clusters in the pool.
    #[serde(default)]
    pub clusters: u32,
    /// Of which `Active`.
    #[serde(default)]
    pub active: u32,
}

/// The `metadata` fields the operator reads and writes; the rest is kept.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMeta {
    /// The name.
    #[serde(default)]
    pub name: String,
    /// The namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// The server's id of the object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// For optimistic concurrency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
    /// Raised on every spec change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,
    /// Labels.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    /// Annotations.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
    /// Set when the object is being deleted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deletion_timestamp: Option<String>,
    /// Everything else.
    #[serde(flatten)]
    pub other: Map<String, Value>,
}

/// A custom resource.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Object<S, T> {
    /// `sprigdb.io/v1alpha1`.
    pub api_version: String,
    /// The kind.
    pub kind: String,
    /// The metadata.
    pub metadata: ObjectMeta,
    /// The spec.
    pub spec: S,
    /// The status.
    #[serde(default)]
    pub status: Option<T>,
}

/// A `SprigCluster`.
pub type SprigCluster = Object<SprigClusterSpec, SprigClusterStatus>;
/// A `SprigClusterPool`.
pub type SprigClusterPool = Object<SprigClusterPoolSpec, SprigClusterPoolStatus>;

impl<S, T> Object<S, T> {
    /// A new object named `name` in `namespace`.
    pub fn new(kind: &str, namespace: &str, name: &str, spec: S) -> Self {
        Self {
            api_version: API_VERSION.to_owned(),
            kind: kind.to_owned(),
            metadata: ObjectMeta {
                name: name.to_owned(),
                namespace: Some(namespace.to_owned()),
                ..ObjectMeta::default()
            },
            spec,
            status: None,
        }
    }

    /// An owner reference to this object, for the objects it owns
    /// (garbage-collected with it).
    #[must_use]
    pub fn owner_reference(&self) -> Value {
        json!({
            "apiVersion": self.api_version,
            "kind": self.kind,
            "name": self.metadata.name,
            "uid": self.metadata.uid.clone().unwrap_or_default(),
            "controller": true,
            "blockOwnerDeletion": true,
        })
    }
}

// ---- the CRDs ----

fn quantity() -> Value {
    json!({"type": "string", "pattern": "^[0-9]+(\\.[0-9]+)?(Ki|Mi|Gi|Ti|Pi|k|M|G|T|P)?$"})
}

/// The `OpenAPI` v3 schema of [`SprigClusterSpec`].
#[must_use]
pub fn cluster_spec_schema() -> Value {
    json!({
        "type": "object",
        "required": ["topology", "mariadb", "storage"],
        "properties": {
            "topology": {"type": "string", "enum": ["galera", "replication", "single"]},
            "nodes": {"type": "integer", "minimum": 1, "maximum": 9, "default": 1},
            "mariadb": {
                "type": "object",
                "required": ["image"],
                "properties": {"image": {"type": "string", "minLength": 1}},
            },
            "storage": {
                "type": "object",
                "required": ["size"],
                "properties": {
                    "size": quantity(),
                    "storageClassName": {"type": "string"},
                },
            },
            "failoverDriver": {"type": "string", "enum": ["operator", "sprig"], "default": "sprig"},
            "backup": {
                "type": "object",
                "required": ["schedule", "target"],
                "properties": {
                    "schedule": {"type": "string"},
                    "target": {"type": "string", "pattern": "^s3://"},
                    "maxRetention": {"type": "string"},
                    "endpoint": {"type": "string"},
                    "credentialsSecret": {"type": "string"},
                },
            },
            "zones": {"type": "array", "items": {"type": "string"}},
            "resources": {"type": "object", "x-kubernetes-preserve-unknown-fields": true},
        },
    })
}

fn cluster_status_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "clusterId": {"type": "string"},
            "clusterNumber": {"type": "integer"},
            "phase": {"type": "string", "enum": ["Pending", "Provisioning", "Bootstrapping", "Probing", "Active", "Failed"]},
            "message": {"type": "string"},
            "nodes": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["node", "addr"],
                    "properties": {"node": {"type": "string"}, "addr": {"type": "string"}},
                },
            },
            "startedUnix": {"type": "integer"},
            "lastTransitionTime": {"type": "string", "format": "date-time"},
            "observedGeneration": {"type": "integer"},
            "rebuild": {"type": "array", "items": {"type": "string"}},
            "lastFailover": {
                "type": "object",
                "properties": {
                    "from": {"type": "integer"},
                    "to": {"type": "integer"},
                    "time": {"type": "string", "format": "date-time"},
                    "reason": {"type": "string"},
                },
            },
        },
    })
}

#[allow(clippy::needless_pass_by_value)] // built in place by the caller
fn crd(r: &ApiResource, short: &str, spec: Value, status: Value, columns: Value) -> Value {
    let subresources = json!({"status": {}});
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {
            "name": format!("{}.{}", r.plural, r.group),
            "labels": {MANAGED_BY.0: MANAGED_BY.1},
        },
        "spec": {
            "group": r.group,
            "scope": "Namespaced",
            "names": {
                "kind": r.kind,
                "listKind": format!("{}List", r.kind),
                "plural": r.plural,
                "singular": r.kind.to_ascii_lowercase(),
                "shortNames": [short],
                "categories": ["sprigdb"],
            },
            "versions": [{
                "name": r.version,
                "served": true,
                "storage": true,
                "subresources": subresources,
                "additionalPrinterColumns": columns,
                "schema": {"openAPIV3Schema": {
                    "type": "object",
                    "required": ["spec"],
                    "properties": {
                        "apiVersion": {"type": "string"},
                        "kind": {"type": "string"},
                        "metadata": {"type": "object"},
                        "spec": spec,
                        "status": status,
                    },
                }},
            }],
        },
    })
}

/// The two `CustomResourceDefinition`s, `SprigCluster` first.
#[must_use]
pub fn crds() -> Vec<Value> {
    let cluster = crd(
        &SPRIG_CLUSTER,
        "sc",
        cluster_spec_schema(),
        cluster_status_schema(),
        json!([
            {"name": "Topology", "type": "string", "jsonPath": ".spec.topology"},
            {"name": "Nodes", "type": "integer", "jsonPath": ".spec.nodes"},
            {"name": "Phase", "type": "string", "jsonPath": ".status.phase"},
            {"name": "Cluster", "type": "string", "jsonPath": ".status.clusterId"},
            {"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp"},
        ]),
    );
    let pool = crd(
        &SPRIG_CLUSTER_POOL,
        "scp",
        json!({
            "type": "object",
            "required": ["template", "maxClusters"],
            "properties": {
                "template": cluster_spec_schema(),
                "minClusters": {"type": "integer", "minimum": 0, "default": 0},
                "maxClusters": {"type": "integer", "minimum": 1},
            },
            "x-kubernetes-validations": [{
                "rule": "self.minClusters <= self.maxClusters",
                "message": "minClusters must not exceed maxClusters",
            }],
        }),
        json!({
            "type": "object",
            "properties": {
                "clusters": {"type": "integer"},
                "active": {"type": "integer"},
            },
        }),
        json!([
            {"name": "Min", "type": "integer", "jsonPath": ".spec.minClusters"},
            {"name": "Max", "type": "integer", "jsonPath": ".spec.maxClusters"},
            {"name": "Clusters", "type": "integer", "jsonPath": ".status.clusters"},
            {"name": "Active", "type": "integer", "jsonPath": ".status.active"},
        ]),
    );
    vec![cluster, pool]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_books_example_parses() {
        let sc: SprigCluster = serde_json::from_value(json!({
            "apiVersion": "sprigdb.io/v1alpha1",
            "kind": "SprigCluster",
            "metadata": {"name": "fra1-c7", "namespace": "sprigdb"},
            "spec": {
                "topology": "replication",
                "nodes": 3,
                "mariadb": {"image": "mariadb:12.3"},
                "storage": {"size": "500Gi", "storageClassName": "fast-ssd"},
                "failoverDriver": "operator",
                "backup": {"schedule": "0 2 * * *", "target": "s3://sprig-fra1-backups"},
            },
            "status": {"clusterId": "fra1-c7", "phase": "Active"},
        }))
        .expect("parses");
        assert_eq!(sc.spec.topology, Topology::Replication);
        assert_eq!(sc.spec.replicas(), 3);
        assert_eq!(sc.status.as_ref().map(|s| s.phase), Some(Phase::Active));
        let single = SprigClusterSpec {
            topology: Topology::Single,
            nodes: 3,
            ..sc.spec.clone()
        };
        assert_eq!(single.replicas(), 1, "single is always one pod");
        // Round trip keeps unknown metadata.
        let mut v = serde_json::to_value(&sc).expect("ser");
        v["metadata"]["creationTimestamp"] = json!("2026-11-09T00:00:00Z");
        let back: SprigCluster = serde_json::from_value(v).expect("de");
        assert_eq!(
            back.metadata.other["creationTimestamp"],
            json!("2026-11-09T00:00:00Z")
        );
    }

    #[test]
    fn crds_name_their_group_and_status_subresource() {
        let c = crds();
        assert_eq!(c.len(), 2);
        assert_eq!(c[0]["metadata"]["name"], "sprigclusters.sprigdb.io");
        assert_eq!(c[1]["metadata"]["name"], "sprigclusterpools.sprigdb.io");
        for d in &c {
            let v = &d["spec"]["versions"][0];
            assert_eq!(v["name"], "v1alpha1");
            assert!(v["subresources"]["status"].is_object());
            assert_eq!(v["schema"]["openAPIV3Schema"]["required"], json!(["spec"]));
        }
        let topo = &c[0]["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["topology"]["enum"];
        assert_eq!(topo, &json!(["galera", "replication", "single"]));
    }
}
