//! `SprigCluster` to mariadb-operator resources (chapter 27, the mapping
//! table), in `k8s.mariadb.com/v1alpha1`. mariadb-operator is MIT; only the
//! fields used here are written, from its API reference (chapter 27, open
//! question 1: verify against the pinned release).
//!
//! | `SprigCluster` | `MariaDB` |
//! |---|---|
//! | `topology: galera` | `galera.enabled`, `sst: mariabackup`, `recovery.enabled` |
//! | `topology: replication` | `replication.enabled`, `semiSyncEnabled`, `semiSyncWaitPoint: AfterSync`, `semiSyncBootAsReplica`, `gtidStrictMode`, `syncBinlog: 1` |
//! | `failoverDriver` | `replication.primary.autoFailover` (`true` only for `operator`; `sprig`, the default, has SprigDB choose the replica: [`crate::failover`]) |
//! | `topology: single`, `nodes` | `replicas` |
//! | baseline settings | `myCnf`, at creation only |
//! | `storage` | `storage.size`, `storageClassName` |
//! | TLS | `tls.enabled`, `required`, `serverCertIssuerRef`, `clientCertIssuerRef` |
//! | fence agent | `sidecarContainers` |
//! | spread | `topologySpreadConstraints`, `podDisruptionBudget` |
//! | `backup` | `PhysicalBackup`, `PointInTimeRecovery` |
//! | new and broken replicas (with a backup) | `replica.bootstrapFrom.physicalBackupTemplateRef`, `replica.recovery.enabled` |
//!
//! [`update_patch`] is the merge patch for an existing `MariaDB`: the
//! desired object without `spec.myCnf` (a change would roll every pod; live
//! settings use `SET GLOBAL`), without the immutable `gtidDomainId`, and
//! without `replication.primary.podIndex` (only a failover moves it: the
//! failover watch, or mariadb-operator with `failoverDriver: operator`).

use serde_json::{Value, json};

use crate::OperatorConfig;
use crate::crd::{
    CLUSTER_LABEL, FailoverDriver, MANAGED_BY, SprigCluster, SprigClusterSpec, Topology,
};
use crate::kube::ApiResource;
use sprig_provision::mycnf::{self, NodeRole, NodeSettings, Platform};

/// `MariaDB`.
pub const MARIADB: ApiResource = ApiResource {
    group: "k8s.mariadb.com",
    version: "v1alpha1",
    kind: "MariaDB",
    plural: "mariadbs",
};
/// `PhysicalBackup`.
pub const PHYSICAL_BACKUP: ApiResource = ApiResource {
    group: "k8s.mariadb.com",
    version: "v1alpha1",
    kind: "PhysicalBackup",
    plural: "physicalbackups",
};
/// `PointInTimeRecovery`.
pub const POINT_IN_TIME_RECOVERY: ApiResource = ApiResource {
    group: "k8s.mariadb.com",
    version: "v1alpha1",
    kind: "PointInTimeRecovery",
    plural: "pointintimerecoveries",
};

/// The memory the baseline sizes the buffer pool from when the spec sets
/// no memory limit.
pub const DEFAULT_MEMORY_BYTES: u64 = 4 << 30;

/// The labels every owned object carries.
#[must_use]
pub fn labels(sc: &SprigCluster) -> Value {
    json!({
        MANAGED_BY.0: MANAGED_BY.1,
        CLUSTER_LABEL: sc.metadata.name,
        "app.kubernetes.io/name": "mariadb",
        "app.kubernetes.io/instance": sc.metadata.name,
    })
}

fn meta(sc: &SprigCluster, name: &str) -> Value {
    json!({
        "name": name,
        "namespace": sc.metadata.namespace,
        "labels": labels(sc),
        "ownerReferences": [sc.owner_reference()],
    })
}

/// Parses a Kubernetes memory quantity (`8Gi`, `512Mi`, `2G`, bytes).
#[must_use]
pub fn quantity_bytes(q: &str) -> Option<u64> {
    let q = q.trim();
    let split = q
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(q.len());
    let (num, unit) = q.split_at(split);
    let n: f64 = num.parse().ok()?;
    let mult: f64 = match unit {
        "" => 1.0,
        "Ki" => 1024.0,
        "Mi" => 1024.0 * 1024.0,
        "Gi" => 1024.0 * 1024.0 * 1024.0,
        "Ti" => 1024.0_f64.powi(4),
        "k" => 1e3,
        "M" => 1e6,
        "G" => 1e9,
        "T" => 1e12,
        _ => return None,
    };
    let v = n * mult;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    (v.is_finite() && v >= 0.0).then_some(v as u64)
}

/// The pod memory the buffer pool is sized from: `resources.limits.memory`,
/// else `requests.memory`, else [`DEFAULT_MEMORY_BYTES`].
#[must_use]
pub fn memory_of(spec: &SprigClusterSpec) -> u64 {
    let r = spec.resources.as_ref();
    ["limits", "requests"]
        .iter()
        .find_map(|k| {
            r.and_then(|r| r.get(*k))
                .and_then(|l| l.get("memory"))
                .and_then(Value::as_str)
                .and_then(quantity_bytes)
        })
        .unwrap_or(DEFAULT_MEMORY_BYTES)
}

/// The baseline `myCnf` (what mariadb-operator owns left out). The
/// cluster number isn't known before registration, so `gtid_domain_id` is
/// left out too and the reconciler sets it live ([`crate::accounts`]).
#[must_use]
pub fn my_cnf(spec: &SprigClusterSpec) -> String {
    let role = match spec.topology {
        Topology::Galera => NodeRole::Galera,
        Topology::Replication => NodeRole::Primary,
        Topology::Single => NodeRole::Single,
    };
    let n = NodeSettings::new(Platform::Kubernetes, role, 0, 1, memory_of(spec));
    mycnf::render(&n)
}

/// The `MariaDB` resource for `sc`.
#[must_use]
#[allow(clippy::too_many_lines)] // the mapping table, row by row
pub fn mariadb(sc: &SprigCluster, cfg: &OperatorConfig) -> Value {
    let name = &sc.metadata.name;
    let spec = &sc.spec;
    let replicas = spec.replicas();
    let mut s = json!({
        "image": spec.mariadb.image,
        "replicas": replicas,
        "port": 3306,
        "rootPasswordSecretKeyRef": {"name": format!("{name}-root"), "key": "password", "generate": true},
        "storage": {"size": spec.storage.size},
        "myCnf": my_cnf(spec),
        "tls": {"enabled": true, "required": true},
        "podDisruptionBudget": {"maxUnavailable": 1},
        "topologySpreadConstraints": [{
            "maxSkew": 1,
            "topologyKey": "topology.kubernetes.io/zone",
            "whenUnsatisfiable": if spec.zones.is_empty() { "ScheduleAnyway" } else { "DoNotSchedule" },
            "labelSelector": {"matchLabels": {"app.kubernetes.io/instance": name}},
        }],
    });
    if let Some(c) = &spec.storage.storage_class_name {
        s["storage"]["storageClassName"] = json!(c);
    }
    if let Some(r) = &spec.resources {
        s["resources"] = r.clone();
    }
    if let Some(i) = &cfg.server_cert_issuer {
        s["tls"]["serverCertIssuerRef"] = json!({"name": i, "kind": "ClusterIssuer"});
    }
    if let Some(i) = &cfg.client_cert_issuer {
        s["tls"]["clientCertIssuerRef"] = json!({"name": i, "kind": "ClusterIssuer"});
    }
    if !spec.zones.is_empty() {
        s["affinity"] = json!({"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [{"matchExpressions": [{
                "key": "topology.kubernetes.io/zone", "operator": "In", "values": spec.zones,
            }]}],
        }}});
    }
    if replicas == 1 {
        s.as_object_mut().map(|o| o.remove("podDisruptionBudget"));
    }
    match spec.topology {
        Topology::Galera => {
            s["galera"] = json!({
                "enabled": true,
                "sst": "mariabackup",
                "recovery": {"enabled": true},
            });
        }
        Topology::Replication => {
            let mut r = json!({
                "enabled": true,
                "semiSyncEnabled": true,
                "semiSyncWaitPoint": "AfterSync",
                "semiSyncBootAsReplica": true,
                "gtidStrictMode": true,
                "syncBinlog": 1,
                "primary": {
                    "podIndex": 0,
                    "autoFailover": spec.failover_driver == FailoverDriver::Operator,
                },
            });
            // mariadb-operator's webhook refuses `replica.recovery` without
            // `bootstrapFrom` (found on kind, mariadb-operator 26.10.2), so
            // both come with a backup.
            if spec.backup.is_some() {
                r["replica"] = json!({
                    "bootstrapFrom": {"physicalBackupTemplateRef": {"name": backup_name(name)}},
                    "recovery": {"enabled": true},
                });
            }
            s["replication"] = r;
        }
        Topology::Single => {}
    }
    if let Some(img) = &cfg.agent_image {
        // The fence-only agent (chapter 25): DC permit and read_only. Its
        // `[agent] cluster` is the record's id, known only after
        // registration, so the sidecar starts without it (and fences
        // nothing) until the agent can find its cluster by address.
        s["sidecarContainers"] = json!([{
            "name": "sprig-agent",
            "image": img,
            "args": ["agent", "--config", "/etc/sprigdb/sprigdb.toml"],
            "env": [
                {"name": "SPRIGDB_NODE_ID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
                {"name": "SPRIGDB_AGENT_MYSQL_ADDR", "value": "127.0.0.1:3306"},
            ],
            "volumeMounts": [{"name": "sprigdb-agent-config", "mountPath": "/etc/sprigdb"}],
        }]);
        s["volumes"] = json!([{
            "name": "sprigdb-agent-config",
            "configMap": {"name": cfg.agent_config_map},
        }]);
    }
    json!({
        "apiVersion": MARIADB.api_version(),
        "kind": MARIADB.kind,
        "metadata": meta(sc, name),
        "spec": s,
    })
}

/// The merge patch that brings an existing `MariaDB` to `desired`
/// (see the module docs for what it leaves alone).
#[must_use]
pub fn update_patch(desired: &Value) -> Value {
    let mut p = json!({
        "metadata": {"labels": desired["metadata"]["labels"]},
        "spec": desired["spec"],
    });
    if let Some(spec) = p["spec"].as_object_mut() {
        spec.remove("myCnf");
        spec.remove("rootPasswordSecretKeyRef");
    }
    if let Some(primary) = p["spec"]["replication"]["primary"].as_object_mut() {
        primary.remove("podIndex");
    }
    p
}

/// The `PhysicalBackup` name.
#[must_use]
pub fn backup_name(cluster: &str) -> String {
    format!("{cluster}-backup")
}

fn s3(cluster: &str, b: &crate::crd::BackupSpec) -> Value {
    let rest = b.target.trim_start_matches("s3://");
    let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
    let prefix = if prefix.is_empty() {
        format!("clusters/{cluster}")
    } else {
        format!("{}/clusters/{cluster}", prefix.trim_end_matches('/'))
    };
    let mut s = json!({"bucket": bucket, "prefix": prefix});
    if let Some(e) = &b.endpoint {
        s["endpoint"] = json!(e);
    }
    if let Some(sec) = &b.credentials_secret {
        s["accessKeyIdSecretKeyRef"] = json!({"name": sec, "key": "access-key-id"});
        s["secretAccessKeySecretKeyRef"] = json!({"name": sec, "key": "secret-access-key"});
    }
    s
}

/// The `PhysicalBackup` and `PointInTimeRecovery` for `sc.spec.backup`
/// (empty without one). Backups go under `clusters/<cluster>/backups/`
/// and archived binlogs under `clusters/<cluster>/binlog/` (chapter 27,
/// "Object storage").
#[must_use]
pub fn backups(sc: &SprigCluster) -> Vec<(ApiResource, Value)> {
    let Some(b) = &sc.spec.backup else {
        return Vec::new();
    };
    let name = &sc.metadata.name;
    let mut store = s3(name, b);
    let base = store["prefix"].as_str().unwrap_or_default().to_owned();
    store["prefix"] = json!(format!("{base}/backups"));
    let mut pb = json!({
        "mariaDbRef": {"name": name},
        "schedule": {"cron": b.schedule},
        "storage": {"s3": store},
    });
    if let Some(r) = &b.max_retention {
        pb["maxRetention"] = json!(r);
    }
    let mut archive = s3(name, b);
    archive["prefix"] = json!(format!("{base}/binlog"));
    vec![
        (
            PHYSICAL_BACKUP,
            json!({
                "apiVersion": PHYSICAL_BACKUP.api_version(),
                "kind": PHYSICAL_BACKUP.kind,
                "metadata": meta(sc, &backup_name(name)),
                "spec": pb,
            }),
        ),
        (
            POINT_IN_TIME_RECOVERY,
            json!({
                "apiVersion": POINT_IN_TIME_RECOVERY.api_version(),
                "kind": POINT_IN_TIME_RECOVERY.kind,
                "metadata": meta(sc, &format!("{name}-pitr")),
                "spec": {
                    "physicalBackupRef": {"name": backup_name(name)},
                    "storage": {"s3": archive},
                },
            }),
        ),
    ]
}

/// The name of pod `i`'s Service.
#[must_use]
pub fn node_service_name(cluster: &str, i: u8) -> String {
    format!("{cluster}-node-{i}")
}

/// One Service per pod (`nodeServices.type`), so other DCs reach every
/// node: the selector is the `StatefulSet`'s pod-name label.
#[must_use]
pub fn node_services(sc: &SprigCluster, cfg: &OperatorConfig) -> Vec<Value> {
    let name = &sc.metadata.name;
    (0..sc.spec.replicas())
        .map(|i| {
            let mut m = meta(sc, &node_service_name(name, i));
            m["labels"]["sprigdb.io/pod-index"] = json!(i.to_string());
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": m,
                "spec": {
                    "type": cfg.node_service_type,
                    "publishNotReadyAddresses": true,
                    "selector": {"statefulset.kubernetes.io/pod-name": format!("{name}-{i}")},
                    "ports": [{"name": "mariadb", "port": 3306, "targetPort": 3306, "protocol": "TCP"}],
                },
            })
        })
        .collect()
}

/// The address a node Service advertises under `cfg`: for a `NodePort`
/// Service with [`OperatorConfig::node_port_host`] set, that host and the
/// Service's node port (a single-host cluster such as kind, whose node
/// ports are published on the host); otherwise [`advertised`].
#[must_use]
pub fn advertised_for(svc: &Value, cfg: &OperatorConfig) -> Option<String> {
    if svc["spec"]["type"] == "NodePort"
        && let Some(host) = &cfg.node_port_host
    {
        let port = svc["spec"]["ports"]
            .as_array()?
            .iter()
            .find(|p| p["port"] == 3306)?["nodePort"]
            .as_u64()?;
        return Some(format!("{host}:{port}"));
    }
    advertised(svc)
}

/// The address a node Service advertises: the load balancer's ingress, or
/// the cluster IP for `ClusterIP` (and `NodePort`) Services. `None` until
/// assigned.
#[must_use]
pub fn advertised(svc: &Value) -> Option<String> {
    let host = if svc["spec"]["type"] == "LoadBalancer" {
        let ing = svc["status"]["loadBalancer"]["ingress"].get(0)?;
        ing["hostname"]
            .as_str()
            .or_else(|| ing["ip"].as_str())?
            .to_owned()
    } else {
        svc["spec"]["clusterIP"]
            .as_str()
            .filter(|s| !s.is_empty() && *s != "None")?
            .to_owned()
    };
    Some(format!("{host}:3306"))
}

/// Whether mariadb-operator reports the `MariaDB` Ready.
#[must_use]
pub fn is_ready(mdb: &Value) -> bool {
    mdb["status"]["conditions"].as_array().is_some_and(|cs| {
        cs.iter()
            .any(|c| c["type"] == "Ready" && c["status"] == "True")
    })
}

#[cfg(test)]
mod tests;
