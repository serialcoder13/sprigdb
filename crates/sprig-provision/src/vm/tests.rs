#![allow(clippy::unwrap_used)]

use std::path::PathBuf;

use sprig_meta::{MemStore, Tier};

use super::install::Distro;
use super::*;
use crate::{MariaDbVersion, Resources};
use sprig_common::ids::DcId;

fn dc() -> DcId {
    DcId::new("fra1").unwrap()
}

fn host(id: &str, zone: &str) -> HostRecord {
    HostRecord {
        host_id: id.into(),
        addr: format!("10.8.1.{}", id.trim_start_matches('h')),
        zone: zone.into(),
        memory_bytes: 16 << 30,
        os: HostOs {
            distro: Distro::Apt,
            id: "debian".into(),
            codename: "bookworm".into(),
        },
        state: HostState::Free,
        cluster: None,
        job: None,
    }
}

fn spec(topology: Topology, nodes: u8) -> ClusterSpec {
    ClusterSpec {
        dc: dc(),
        topology,
        nodes,
        version: MariaDbVersion {
            major: 11,
            minor: 8,
        },
        resources: Resources::default(),
        zones: vec![],
        backup_target: "s3://sprig-fra1".into(),
        pool: Some("default".into()),
    }
}

fn with_rev(hs: Vec<HostRecord>) -> Vec<(HostRecord, sprig_meta::Revision)> {
    hs.into_iter().map(|h| (h, 1)).collect()
}

#[test]
fn hosts_spread_over_zones() {
    let hs = with_rev(vec![
        host("h1", "a"),
        host("h2", "a"),
        host("h3", "a"),
        host("h4", "b"),
        host("h5", "c"),
    ]);
    let ids = |v: Vec<(HostRecord, sprig_meta::Revision)>| {
        v.into_iter().map(|h| h.0.host_id).collect::<Vec<_>>()
    };
    assert_eq!(ids(choose_hosts(&hs, &[], 3).unwrap()), ["h1", "h4", "h5"]);
    assert_eq!(
        ids(choose_hosts(&hs, &[], 5).unwrap()),
        ["h1", "h4", "h5", "h2", "h3"]
    );
    assert_eq!(
        ids(choose_hosts(&hs, &["a".into(), "b".into()], 2).unwrap()),
        ["h1", "h4"]
    );
    assert!(choose_hosts(&hs, &["b".into()], 2).is_none());
    let mut taken = hs.clone();
    taken[3].0.state = HostState::Claimed;
    taken[4].0.state = HostState::Broken;
    assert_eq!(
        ids(choose_hosts(&taken, &[], 3).unwrap()),
        ["h1", "h2", "h3"]
    );
}

#[test]
fn a_primary_replica_plan() {
    let id = ClusterRegistry::cluster_id(&dc(), 7).unwrap();
    let hs = vec![host("h1", "a"), host("h2", "b"), host("h3", "c")];
    let p = plan(
        &spec(Topology::PrimaryReplica, 3),
        "fra1-c7",
        7,
        &id,
        &hs,
        "j1",
        "2G",
    );
    assert_eq!(
        p.nodes.iter().map(|n| n.server_id).collect::<Vec<_>>(),
        [701, 702, 703]
    );
    assert_eq!(p.nodes[0].role, "primary");
    assert_eq!(p.nodes[1].role, "replica");
    assert_eq!(p.nodes[0].addr, "10.8.1.1:3306");
    let script = install::script(&p.nodes[1].steps);
    assert!(script.contains("dlm.mariadb.com/repo/mariadb-server/11.8/repo/debian"));
    assert!(script.contains(
        "'apt-get' 'install' '-y' '--no-install-recommends' 'mariadb-server' 'mariadb-backup'"
    ));
    assert!(
        script.contains("exit 101"),
        "the package doesn't start MariaDB"
    );
    let set = |k: &str, v: &str| {
        script.lines().any(|l| {
            l.split('#')
                .next()
                .unwrap()
                .split('=')
                .map(str::trim)
                .eq([k, v])
        })
    };
    assert!(set("rpl_semi_sync_master_enabled", "OFF"));
    assert!(script.contains("--init-rpl-role=SLAVE"));
    assert!(script.contains("LimitNOFILE=524288"));
    assert!(set("report_host", "10.8.1.2"));
    assert!(set("server_id", "702"));
    assert_eq!(
        p.bootstrap,
        vec![
            BootstrapStep::StartFenced { node: "n1".into() },
            BootstrapStep::CreateSystemAccounts { node: "n1".into() },
            BootstrapStep::TakeWriterLease { node: "n1".into() },
            BootstrapStep::SeedReplica {
                node: "n2".into(),
                from: "n1".into()
            },
            BootstrapStep::SeedReplica {
                node: "n3".into(),
                from: "n1".into()
            },
            BootstrapStep::Register,
        ]
    );
    // The plan is a record: it round-trips.
    let back: ProvisionPlan = serde_json::from_value(serde_json::to_value(&p).unwrap()).unwrap();
    assert_eq!(back, p);
}

#[test]
fn a_galera_plan_on_dnf_hosts() {
    let id = ClusterRegistry::cluster_id(&dc(), 3).unwrap();
    let mut hs = vec![host("h1", "a"), host("h2", "b"), host("h3", "c")];
    for h in &mut hs {
        h.os = HostOs {
            distro: Distro::Dnf,
            id: "rocky".into(),
            codename: String::new(),
        };
    }
    let p = plan(&spec(Topology::Galera, 3), "g", 3, &id, &hs, "j", "1G");
    let s = install::script(&p.nodes[2].steps);
    assert!(s.contains("'dnf' 'install' '-y' 'MariaDB-server' 'MariaDB-backup' 'galera-4'"));
    assert!(s.contains("/etc/my.cnf.d/60-sprigdb.cnf"));
    assert!(s.contains("gcomm://10.8.1.1,10.8.1.2,10.8.1.3"));
    assert!(s.contains("/usr/lib64/galera-4/libgalera_smm.so"));
    assert!(s.contains("gcache.size=1G"));
    assert!(matches!(
        p.bootstrap[0],
        BootstrapStep::GaleraNewCluster { .. }
    ));
    assert_eq!(p.bootstrap.len(), 5);
}

fn provisioner() -> (VmProvisioner, Arc<MemStore>) {
    let global: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Global));
    let dcs = Arc::new(MemStore::new(Tier::Dc(dc())));
    let dcm: Arc<dyn MetaStore> = dcs.clone();
    let reg = ClusterRegistry::new(global, Arc::clone(&dcm)).unwrap();
    (VmProvisioner::new(reg, dcm), dcs)
}

#[tokio::test]
async fn create_claims_hosts_and_stores_the_plan() {
    let (vm, _) = provisioner();
    assert!(vm.supports(ProvisionOp::Create));
    assert!(!vm.supports(ProvisionOp::Upgrade));
    for (id, z) in [("h1", "a"), ("h2", "b")] {
        vm.pool().offer(host(id, z)).await.unwrap();
    }
    // Two free hosts, three wanted: 39513, nothing claimed.
    let e = vm
        .create_named("c1", &spec(Topology::PrimaryReplica, 3))
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ProvisionNoHosts));
    assert!(
        e.to_string()
            .contains("3 free hosts needed in fra1, 2 free"),
        "{e}"
    );
    vm.pool().offer(host("h3", "c")).await.unwrap();
    let job = vm
        .create_named("c1", &spec(Topology::PrimaryReplica, 3))
        .await
        .unwrap();
    let p = vm.plan(&job).await.unwrap().unwrap();
    assert_eq!(p.state, "planned");
    assert_eq!(p.cluster_number, 1);
    assert_eq!(p.nodes.len(), 3);
    let hosts = vm.pool().list().await.unwrap();
    assert!(hosts.iter().all(|(h, _)| h.state == HostState::Claimed
        && h.cluster.as_deref() == Some("c1")
        && h.job.as_deref() == Some(job.to_string().as_str())));
    // Offering a claimed host again doesn't free it.
    vm.pool().offer(host("h1", "a")).await.unwrap();
    assert_eq!(
        vm.pool().list().await.unwrap()[0].0.state,
        HostState::Claimed
    );
    // A second cluster finds no hosts.
    let e = vm.create(spec(Topology::Single, 1)).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ProvisionNoHosts));
    // Step 7: the record takes the reserved number.
    let rec = vm.register_planned(&job).await.unwrap();
    assert_eq!(rec.body().cluster_number, 1);
    assert_eq!(rec.body().provisioner, ProvisionerKind::Vm);
    assert_eq!(rec.body().nodes.len(), 3);
    assert_eq!(
        rec.body().secrets.ctl,
        "file:/etc/sprigdb/creds/c1.toml#sprig_ctl"
    );
    // Another DC's spec is refused.
    let mut other = spec(Topology::Single, 1);
    other.dc = DcId::new("ams1").unwrap();
    assert_eq!(
        vm.create_named("x", &other).await.unwrap_err().code(),
        Some(SprigCode::ProvisionerUnsupported)
    );
}

#[test]
fn units_follow_the_book() {
    let agent = units::unit(units::Unit::Agent);
    for l in [
        "Type=notify",
        "ExecStart=/usr/bin/sprigdb agent --config /etc/sprigdb/sprigdb.toml",
        "WatchdogSec=3s",
        "Restart=always",
        "RestartSec=1s",
        "Before=mariadb.service",
        "WantedBy=multi-user.target",
    ] {
        assert!(agent.lines().any(|x| x == l), "agent unit lacks {l}");
    }
    assert!(!agent.contains("User="), "the agent runs as root");
    let gw = units::unit(units::Unit::Gateway);
    assert!(gw.contains("User=sprigdb") && gw.contains("AmbientCapabilities=CAP_NET_BIND_SERVICE"));
    // mimalloc ignores glibc's arena cap, which doubled a point read's CPU.
    assert!(!gw.contains("MALLOC_ARENA_MAX"));
    assert!(units::unit(units::Unit::EtcdGlobal).contains("/etc/sprigdb/etcd-global.yaml"));
    assert_eq!(units::units().len(), 6);
    for (_, u) in units::units() {
        // Every unit has the three sections, in order.
        let a = u.find("[Unit]").unwrap();
        let b = u.find("[Service]").unwrap();
        let c = u.find("[Install]").unwrap();
        assert!(a < b && b < c);
    }
}

/// `deploy/vm/` holds the generated files; `SPRIG_PROVISION_REGEN=1`
/// rewrites them.
#[test]
fn checked_in_units_are_current() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../deploy/vm");
    let regen = std::env::var_os("SPRIG_PROVISION_REGEN").is_some();
    let mut stale = Vec::new();
    for (rel, content) in package_files() {
        let path = root.join(&rel);
        if regen {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, &content).unwrap();
        } else if std::fs::read_to_string(&path).ok().as_deref() != Some(content.as_str()) {
            stale.push(rel);
        }
    }
    assert!(
        stale.is_empty(),
        "deploy/vm is stale for {stale:?}: run SPRIG_PROVISION_REGEN=1 cargo test -p sprig-provision checked_in_units"
    );
}

#[test]
fn the_example_configuration_loads() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy/packaging/sprigdb.example.toml");
    let text = std::fs::read_to_string(path).unwrap();
    let c = sprig_common::ProcessConfig::from_toml_and_env(
        &text,
        std::iter::empty::<(String, String)>(),
    )
    .unwrap();
    assert_eq!(c.node.dc.as_str(), "fra1");
}

/// Writes a primary's settings, install script and the account SQL for
/// the container check (`SPRIG_PROVISION_LIVE_DIR=<dir>`); a no-op
/// otherwise.
#[test]
fn write_live_fixture() {
    let Some(dir) = std::env::var_os("SPRIG_PROVISION_LIVE_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut n = NodeSettings::new(Platform::Vm, NodeRole::Primary, 42, 1, 512 << 20);
    n.report_host = Some("10.8.1.11".into());
    n.tls = Some(tls_files());
    std::fs::write(dir.join("60-sprigdb.cnf"), mycnf::render(&n)).unwrap();
    let mut r = NodeSettings::new(Platform::Vm, NodeRole::Replica, 42, 2, 512 << 20);
    r.tls = Some(tls_files());
    std::fs::write(dir.join("60-sprigdb-replica.cnf"), mycnf::render(&r)).unwrap();
}
