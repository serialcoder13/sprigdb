#![allow(clippy::unwrap_used)]

use serde_json::json;

use super::*;
use crate::crd::{BackupSpec, MariaDbImage, StorageSpec};

pub(crate) fn sc(topology: Topology, nodes: u8) -> SprigCluster {
    let mut s = SprigCluster::new(
        "SprigCluster",
        "sprigdb",
        "fra1-c7",
        SprigClusterSpec {
            topology,
            nodes,
            mariadb: MariaDbImage {
                image: "mariadb:12.3".into(),
            },
            storage: StorageSpec {
                size: "500Gi".into(),
                storage_class_name: Some("fast-ssd".into()),
            },
            failover_driver: FailoverDriver::Operator,
            backup: Some(BackupSpec {
                schedule: "0 2 * * *".into(),
                target: "s3://sprig-fra1-backups".into(),
                max_retention: Some("720h".into()),
                endpoint: Some("https://s3.fra1.example.net".into()),
                credentials_secret: Some("sprig-fra1-s3".into()),
            }),
            zones: vec!["fra1-a".into(), "fra1-b".into()],
            resources: Some(json!({"limits": {"memory": "8Gi"}})),
        },
    );
    s.metadata.uid = Some("u-1".into());
    s
}

fn cfg() -> OperatorConfig {
    let mut c = OperatorConfig::new("sprigdb");
    c.server_cert_issuer = Some("sprig-fra1-ca".into());
    c.client_cert_issuer = Some("sprig-fra1-ca".into());
    c.agent_image = Some("ghcr.io/sprigdb/sprigdb:0.9.0".into());
    c
}

#[test]
fn replication_follows_the_table() {
    let m = mariadb(&sc(Topology::Replication, 3), &cfg());
    assert_eq!(m["apiVersion"], "k8s.mariadb.com/v1alpha1");
    assert_eq!(m["kind"], "MariaDB");
    let s = &m["spec"];
    assert_eq!(s["replicas"], 3);
    assert_eq!(s["image"], "mariadb:12.3");
    let r = &s["replication"];
    assert_eq!(r["enabled"], true);
    assert_eq!(r["semiSyncEnabled"], true);
    assert_eq!(r["semiSyncWaitPoint"], "AfterSync");
    assert_eq!(r["semiSyncBootAsReplica"], true);
    assert_eq!(r["gtidStrictMode"], true);
    assert_eq!(r["syncBinlog"], 1);
    assert_eq!(r["primary"]["autoFailover"], true);
    assert_eq!(r["primary"]["podIndex"], 0);
    assert_eq!(r["replica"]["recovery"]["enabled"], true);
    assert_eq!(
        r["replica"]["bootstrapFrom"]["physicalBackupTemplateRef"]["name"],
        "fra1-c7-backup"
    );
    assert_eq!(
        s["storage"],
        json!({"size": "500Gi", "storageClassName": "fast-ssd"})
    );
    assert_eq!(s["tls"]["enabled"], true);
    assert_eq!(s["tls"]["required"], true);
    assert_eq!(s["tls"]["serverCertIssuerRef"]["kind"], "ClusterIssuer");
    assert_eq!(s["sidecarContainers"][0]["name"], "sprig-agent");
    assert_eq!(s["podDisruptionBudget"]["maxUnavailable"], 1);
    assert_eq!(
        s["topologySpreadConstraints"][0]["whenUnsatisfiable"],
        "DoNotSchedule"
    );
    assert_eq!(m["metadata"]["ownerReferences"][0]["uid"], "u-1");
    assert_eq!(m["metadata"]["ownerReferences"][0]["controller"], true);
    // The baseline: 8 GiB x 60%, no server_id, no read_only.
    let cnf = s["myCnf"].as_str().unwrap();
    assert!(cnf.contains("innodb_buffer_pool_size"));
    assert!(cnf.contains("4915M"), "{cnf}");
    assert!(!cnf.contains("server_id"));
    assert!(!cnf.contains("gtid_domain_id"));
    // Sprig drives failover: the operator's is off.
    let mut sprig = sc(Topology::Replication, 3);
    sprig.spec.failover_driver = FailoverDriver::Sprig;
    assert_eq!(
        mariadb(&sprig, &cfg())["spec"]["replication"]["primary"]["autoFailover"],
        false
    );
}

#[test]
fn galera_and_single() {
    let g = mariadb(&sc(Topology::Galera, 3), &cfg());
    assert_eq!(
        g["spec"]["galera"],
        json!({"enabled": true, "sst": "mariabackup", "recovery": {"enabled": true}})
    );
    assert!(g["spec"]["replication"].is_null());
    let mut one = sc(Topology::Single, 3);
    one.spec.zones.clear();
    let s = mariadb(&one, &OperatorConfig::new("sprigdb"));
    assert_eq!(s["spec"]["replicas"], 1);
    assert!(s["spec"]["podDisruptionBudget"].is_null());
    assert!(s["spec"]["sidecarContainers"].is_null(), "no agent image");
    assert!(s["spec"]["affinity"].is_null());
    assert_eq!(
        s["spec"]["topologySpreadConstraints"][0]["whenUnsatisfiable"],
        "ScheduleAnyway"
    );
}

#[test]
fn the_update_patch_never_touches_pod_index_or_my_cnf() {
    let m = mariadb(&sc(Topology::Replication, 3), &cfg());
    let p = update_patch(&m);
    assert!(p["spec"]["myCnf"].is_null());
    assert!(p["spec"]["rootPasswordSecretKeyRef"].is_null());
    assert!(p["spec"]["replication"]["primary"]["podIndex"].is_null());
    assert_eq!(p["spec"]["replication"]["primary"]["autoFailover"], true);
    // A live object whose operator moved the primary to pod 2 already
    // matches the patch: nothing to send.
    let mut live = m.clone();
    live["spec"]["replication"]["primary"]["podIndex"] = json!(2);
    live["spec"]["myCnf"] = json!("edited by hand");
    assert!(crate::kube::contains(&live, &p));
    live["spec"]["replicas"] = json!(5);
    assert!(!crate::kube::contains(&live, &p));
}

#[test]
fn backups_land_under_the_cluster_prefixes() {
    let b = backups(&sc(Topology::Replication, 3));
    assert_eq!(b.len(), 2);
    let (r, pb) = &b[0];
    assert_eq!(r.kind, "PhysicalBackup");
    assert_eq!(pb["spec"]["mariaDbRef"]["name"], "fra1-c7");
    assert_eq!(pb["spec"]["schedule"]["cron"], "0 2 * * *");
    assert_eq!(pb["spec"]["maxRetention"], "720h");
    let s3 = &pb["spec"]["storage"]["s3"];
    assert_eq!(s3["bucket"], "sprig-fra1-backups");
    assert_eq!(s3["prefix"], "clusters/fra1-c7/backups");
    assert_eq!(s3["accessKeyIdSecretKeyRef"]["key"], "access-key-id");
    let (r, pitr) = &b[1];
    assert_eq!(r.kind, "PointInTimeRecovery");
    assert_eq!(pitr["spec"]["physicalBackupRef"]["name"], "fra1-c7-backup");
    assert_eq!(
        pitr["spec"]["storage"]["s3"]["prefix"],
        "clusters/fra1-c7/binlog"
    );
    let mut none = sc(Topology::Single, 1);
    none.spec.backup = None;
    assert!(backups(&none).is_empty());
}

#[test]
fn one_service_per_pod_and_their_addresses() {
    let svcs = node_services(&sc(Topology::Galera, 3), &cfg());
    assert_eq!(svcs.len(), 3);
    assert_eq!(svcs[2]["metadata"]["name"], "fra1-c7-node-2");
    assert_eq!(
        svcs[2]["spec"]["selector"]["statefulset.kubernetes.io/pod-name"],
        "fra1-c7-2"
    );
    assert_eq!(svcs[0]["spec"]["type"], "LoadBalancer");
    let mut live = svcs[0].clone();
    assert_eq!(advertised(&live), None);
    live["status"] = json!({"loadBalancer": {"ingress": [{"ip": "203.0.113.7"}]}});
    assert_eq!(advertised(&live).as_deref(), Some("203.0.113.7:3306"));
    live["status"] = json!({"loadBalancer": {"ingress": [{"hostname": "a.elb.example"}]}});
    assert_eq!(advertised(&live).as_deref(), Some("a.elb.example:3306"));
    let cip = json!({"spec": {"type": "ClusterIP", "clusterIP": "10.96.4.4"}});
    assert_eq!(advertised(&cip).as_deref(), Some("10.96.4.4:3306"));
    // NodePort: the node-port host when one is configured (kind).
    let np = json!({"spec": {"type": "NodePort", "clusterIP": "10.96.4.5",
        "ports": [{"port": 3306, "nodePort": 31104}]}});
    assert_eq!(advertised_for(&np, &cfg()).as_deref(), Some("10.96.4.5:3306"));
    let mut c = cfg();
    c.node_port_host = Some("127.0.0.1".into());
    assert_eq!(advertised_for(&np, &c).as_deref(), Some("127.0.0.1:31104"));
    assert_eq!(advertised_for(&cip, &c).as_deref(), Some("10.96.4.4:3306"));
}

#[test]
fn quantities() {
    assert_eq!(quantity_bytes("8Gi"), Some(8 << 30));
    assert_eq!(quantity_bytes("512Mi"), Some(512 << 20));
    assert_eq!(quantity_bytes("2G"), Some(2_000_000_000));
    assert_eq!(quantity_bytes("1.5Gi"), Some(3 << 29));
    assert_eq!(quantity_bytes("12x"), None);
    assert!(is_ready(
        &json!({"status": {"conditions": [{"type": "Ready", "status": "True"}]}})
    ));
    assert!(!is_ready(
        &json!({"status": {"conditions": [{"type": "Ready", "status": "False"}]}})
    ));
}
