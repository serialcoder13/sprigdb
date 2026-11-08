//! Command parsing, golden output, and the client against an in-process
//! Admin API over `MemStore`.
//!
//! Golden files live in `crates/sprigctl/tests/golden/`. After an intended
//! change to the output, rewrite them with
//! `SPRIG_UPDATE_GOLDEN=1 cargo test -p sprigctl` and review the diff.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::path::PathBuf;
use std::sync::Arc;

use sprig_cluster::health::{ClusterHealth, HealthTracker};
use sprig_cluster::probe::{
    ActiveProbes, FeatureFlags, NodeFacts, NodeProbe, ProbeDepth, RawCapture,
};
use sprig_cluster::profile::{DeriveInput, Direction, FieldChange, derive};
use sprig_cluster::record::{
    AdminState, Capacity, ClusterRecord, GaleraWriters, HealthSummary, NodeSpec, ProvisionerKind,
    SecretRefs, TopologyKind, WriterWeight,
};
use sprig_cluster::registry::ClusterRegistry;
use sprig_cluster::settings::ClusterSettings;
use sprig_common::ids::{ClusterId, DcId, NodeId};
use sprig_common::time::SystemClock;
use sprig_controller::admin::types::{ClusterView, ProbeClusterResponse};
use sprig_controller::admin::{AdminService, http};
use sprig_controller::dc::{DcView, LeaderView};
use sprig_controller::monitor::ClusterMonitor;
use sprig_controller::runtime::{Status, Stores};
use sprig_meta::bootstrap::DcRecord;
use sprig_meta::{MemStore, Tier};
use sprig_provision::{NodeReport, PrecheckReport};
use sprig_settings::{LiveSettings, Registry, SettingsStores};

use super::*;

#[test]
fn every_command_parses() {
    cli().debug_assert();
    for &(group, _, subs) in GROUPS {
        for &sub in subs {
            let m = cli()
                .try_get_matches_from(["sprigctl", "-o", "json", group, sub, "x", "--flag"])
                .unwrap();
            let (g, s) = m.subcommand().unwrap();
            assert_eq!((g, s.subcommand_name()), (group, Some(sub)));
        }
    }
    let scoped =
        cli().try_get_matches_from(["sprigctl", "config", "get", "--scope", "dc:mum1", "x"]);
    assert!(scoped.is_ok());
    assert!(cli().try_get_matches_from(["sprigctl", "nope"]).is_err());
}

#[test]
fn dc_commands_parse() {
    let m = cli()
        .try_get_matches_from([
            "sprigctl",
            "--endpoint",
            "http://127.0.0.1:7480",
            "dc",
            "register",
            "ams1",
            "--tags",
            "jurisdiction=EU,country=NL",
            "--tags",
            "tier=gold",
            "--global-voter",
        ])
        .unwrap();
    let (_, dc) = m.subcommand().unwrap();
    let (cmd, sub) = dc.subcommand().unwrap();
    assert_eq!(cmd, "register");
    assert_eq!(sub.get_one::<String>("id").unwrap(), "ams1");
    assert_eq!(sub.get_many::<String>("tags").unwrap().count(), 3);
    assert!(sub.get_flag("global-voter"));
    for args in [&["dc", "list"][..], &["dc", "show", "ams1"]] {
        let mut full = vec!["sprigctl"];
        full.extend_from_slice(args);
        assert!(cli().try_get_matches_from(full).is_ok(), "{args:?}");
    }
    assert!(
        cli()
            .try_get_matches_from(["sprigctl", "dc", "show"])
            .is_err()
    );
    assert!(
        cli()
            .try_get_matches_from(["sprigctl", "dc", "register", "x", "--kind", "edge"])
            .is_err()
    );
}

#[test]
fn cluster_commands_parse() {
    let m = cli()
        .try_get_matches_from([
            "sprigctl",
            "--dc",
            "dc-a",
            "--dry-run",
            "cluster",
            "register",
            "dc-a-galera",
            "--topology",
            "galera",
            "--node",
            "127.0.0.1:13321",
            "--node",
            "n2=127.0.0.1:13322",
            "--credentials",
            "file:/tmp/c.toml",
            "--memory-gb",
            "16",
        ])
        .unwrap();
    let (_, g) = m.subcommand().unwrap();
    let (_, sub) = g.subcommand().unwrap();
    let flags = register_flags(&m, sub).unwrap();
    assert_eq!(flags.dc.as_deref(), Some("dc-a"));
    assert_eq!(flags.nodes.len(), 2);
    assert!(flags.dry_run);
    let req = cluster::build(&flags).unwrap();
    assert_eq!(req.capacity.memory_gb, Some(16));
    for args in [
        &["cluster", "list"][..],
        &["cluster", "show", "dc-a-c1"],
        &["cluster", "probe", "dc-a-c1", "--full"],
        &["cluster", "drain", "dc-a-c1", "--yes"],
    ] {
        let mut full = vec!["sprigctl"];
        full.extend_from_slice(args);
        assert!(cli().try_get_matches_from(full).is_ok(), "{args:?}");
    }
    assert!(
        cli()
            .try_get_matches_from(["sprigctl", "cluster", "register", "--topology", "mesh"])
            .is_err()
    );
    assert!(
        cli()
            .try_get_matches_from(["sprigctl", "cluster", "show"])
            .is_err()
    );
}

// --- Golden output ---------------------------------------------------------

fn raw(name: &str) -> RawCapture {
    let text = match name {
        "single" => include_str!("../../sprig-cluster/fixtures/11.8-single.json"),
        "primary" => include_str!("../../sprig-cluster/fixtures/11.8-primary.json"),
        "replica" => include_str!("../../sprig-cluster/fixtures/11.8-replica.json"),
        "galera" => include_str!("../../sprig-cluster/fixtures/11.8-galera.json"),
        other => panic!("no fixture {other}"),
    };
    serde_json::from_str(text).unwrap()
}

fn active(xa: bool) -> ActiveProbes {
    ActiveProbes {
        sys_schema: true,
        xa,
        xa_detail: None,
        features: FeatureFlags {
            ignored_indexes: true,
            instant_ignored_toggle: true,
            vector: true,
            json_schema_valid: true,
            json_table: true,
            vcol_index: true,
            vcol_index_online: true,
            oracle_routines: true,
            oracle_routines_any_mode: true,
            trigger_create: true,
            ..FeatureFlags::default()
        },
        sql_log_bin_settable: true,
        osu_method_settable: None,
        binlog_dump: None,
    }
}

fn probes(names: &[&str], xa: bool) -> Vec<NodeProbe> {
    let mut v: Vec<NodeProbe> = names
        .iter()
        .enumerate()
        .map(|(i, n)| NodeProbe {
            node: NodeId::new(format!("n{}", i + 1)).unwrap(),
            depth: ProbeDepth::Deep,
            at_unix_ms: 0,
            took_ms: 1,
            result: Ok(NodeFacts::parse(&raw(n))),
            active: None,
        })
        .collect();
    v[0].active = Some(active(xa));
    v
}

fn view(
    number: u32,
    name: &str,
    topology: TopologyKind,
    ports: &[u16],
    probes: &[NodeProbe],
) -> ClusterView {
    let mut profile = derive(DeriveInput {
        topology,
        probes,
        declared: None,
        previous: None,
    });
    profile.version = 3;
    let report = HealthTracker::new().observe(topology, probes, &ClusterSettings::default());
    ClusterView {
        record: ClusterRecord {
            cluster_id: ClusterId::new(format!("dc-a-c{number}")).unwrap(),
            cluster_number: number,
            name: name.into(),
            dc_id: DcId::new("dc-a").unwrap(),
            topology,
            provisioner: ProvisionerKind::External,
            admin_state: AdminState::Active,
            nodes: ports
                .iter()
                .enumerate()
                .map(|(i, p)| NodeSpec {
                    node: NodeId::new(format!("n{}", i + 1)).unwrap(),
                    addr: format!("127.0.0.1:{p}"),
                    zone: None,
                })
                .collect(),
            secrets: SecretRefs {
                ctl: "file:/etc/sprigdb/creds/dev.toml#sprig_ctl".into(),
                repl: "file:/etc/sprigdb/creds/dev.toml#sprig_repl".into(),
            },
            capacity: Capacity::default(),
            galera: (topology == TopologyKind::Galera).then(|| GaleraWriters {
                writer_epoch: 1,
                writers: (1..=3)
                    .map(|i| WriterWeight {
                        node: NodeId::new(format!("n{i}")).unwrap(),
                        weight: 1.0,
                    })
                    .collect(),
            }),
            declared: None,
            profile_version: 3,
            profile: Some(profile),
            health: Some(HealthSummary {
                state: report.health,
                since: "2026-11-07T10:00:00Z".into(),
            }),
            tuning: None,
            gaps: Vec::new(),
        },
        mod_revision: 42,
        health_report: Some(report),
        tuning_plan: Vec::new(),
    }
}

fn fleet() -> Vec<ClusterView> {
    let mut single = view(
        1,
        "dc-a-single",
        TopologyKind::SingleNode,
        &[13301],
        &probes(&["single"], true),
    );
    single.record.gaps = vec![
        "node n1: require_secure_transport is OFF: SET GLOBAL require_secure_transport = ON".into(),
    ];
    vec![
        single,
        view(
            2,
            "dc-a-replication",
            TopologyKind::PrimaryReplica,
            &[13311, 13312, 13313],
            &probes(&["primary", "replica", "replica"], true),
        ),
        view(
            3,
            "dc-a-galera",
            TopologyKind::Galera,
            &[13321, 13322, 13323],
            &probes(&["galera", "galera", "galera"], false),
        ),
    ]
}

fn golden(name: &str, got: &str) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name);
    if std::env::var_os("SPRIG_UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, got).unwrap();
        return;
    }
    let want = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{}: {e}; run with SPRIG_UPDATE_GOLDEN=1", path.display()));
    assert_eq!(got, want, "{name} differs from its golden file");
}

#[test]
fn golden_cluster_output() {
    let f = fleet();
    assert_eq!(
        f.iter()
            .map(|c| c.record.health.as_ref().unwrap().state)
            .collect::<Vec<_>>(),
        [ClusterHealth::Healthy; 3]
    );
    golden("cluster-list.txt", &render::cluster_list(&f));
    golden("cluster-show-single.txt", &render::cluster_show(&f[0]));
    golden("cluster-show-replication.txt", &render::cluster_show(&f[1]));
    golden("cluster-show-galera.txt", &render::cluster_show(&f[2]));

    let mut off = f[1].clone();
    let p = off.record.profile.as_mut().unwrap();
    p.semi_sync.enabled = false;
    p.derived.zero_loss_failover = false;
    p.derived.expected_loss = sprig_cluster::profile::LossClass::Async;
    off.record.profile_version = 4;
    let probe = ProbeClusterResponse {
        cluster: off,
        depth: ProbeDepth::Full,
        changes: vec![
            FieldChange {
                field: "derived.zero_loss_failover".into(),
                direction: Direction::Downgrade,
                old: "true".into(),
                new: "false".into(),
            },
            FieldChange {
                field: "semi_sync.enabled".into(),
                direction: Direction::Downgrade,
                old: "true".into(),
                new: "false".into(),
            },
        ],
        held: vec!["derived.xa".into()],
    };
    golden("cluster-probe.txt", &render::probe(&probe));
    // The JSON face of show is the API's JSON.
    let json = serde_json::to_value(&f[0]).unwrap();
    assert_eq!(json["cluster_id"], "dc-a-c1");
    assert_eq!(json["profile"]["derived"]["ryw"], "writer_only");
}

#[test]
fn golden_precheck_and_dc_output() {
    let failed = PrecheckReport {
        blocking: vec![
            "node n2: binlog_row_image is MINIMAL, needs FULL: SET GLOBAL binlog_row_image = 'FULL'".into(),
            "sprig_repl can't read binlogs: GRANT REPLICATION SLAVE, BINLOG MONITOR ON *.* TO sprig_repl".into(),
        ],
        advisories: vec![
            "node n1: require_secure_transport is OFF: SET GLOBAL require_secure_transport = ON".into(),
        ],
        nodes: vec![
            NodeReport {
                node: NodeId::new("n1").unwrap(),
                addr: "127.0.0.1:13311".into(),
                tls_cipher: Some("TLS_AES_256_GCM_SHA384".into()),
                version: Some("11.8.9-MariaDB-ubu2404-log".into()),
                role: Some("writer".into()),
                error: None,
            },
            NodeReport {
                node: NodeId::new("n2").unwrap(),
                addr: "127.0.0.1:13312".into(),
                tls_cipher: None,
                version: None,
                role: None,
                error: Some("timed out".into()),
            },
        ],
        ..PrecheckReport::default()
    };
    golden("precheck-failed.txt", &render::precheck(&failed));
    let dcs = vec![
        DcRecord {
            dc_id: DcId::new("dc-a").unwrap(),
            kind: "full".into(),
            state: "active".into(),
            dc_epoch: 0,
            tags: serde_json::Map::new(),
            global_voter: true,
        },
        DcRecord {
            dc_id: DcId::new("ams1").unwrap(),
            kind: "witness".into(),
            state: "joining".into(),
            dc_epoch: 0,
            tags: sprig_controller::dc::parse_tags(["jurisdiction=EU", "country=NL"]).unwrap(),
            global_voter: false,
        },
    ];
    golden("dc-list.txt", &render::dc_list(&dcs));
    golden(
        "dc-show.txt",
        &render::dc_show(&DcView {
            record: dcs[0].clone(),
            mod_revision: 7,
            leader: Some(LeaderView {
                holder: "ctl-a".into(),
                build: "0.0.1".into(),
                token: 12,
            }),
        }),
    );
}

// --- The client against an in-process Admin API ----------------------------

#[tokio::test]
async fn the_client_against_the_admin_api() {
    let store = MemStore::new(Tier::Dc(DcId::new("ctl-t").unwrap()));
    let global = store.with_tier(Tier::Global);
    let live = LiveSettings::start(
        Registry::global().unwrap(),
        SettingsStores::new(Arc::new(global.clone()), Arc::new(store.clone())).unwrap(),
    )
    .unwrap();
    live.ready().await;
    let registry = ClusterRegistry::new(Arc::new(global.clone()), Arc::new(store.clone())).unwrap();
    let monitor = Arc::new(ClusterMonitor::new(registry, live, Arc::new(SystemClock)));
    let (_status, rx) = tokio::sync::watch::channel(Status::default());
    let svc = Arc::new(AdminService::new(
        Stores {
            dc: Arc::new(store.clone()),
            global: Arc::new(global.clone()),
        },
        monitor,
        rx,
        Arc::new(SystemClock),
    ));
    let l = http::bind("127.0.0.1:0".parse().unwrap(), Some("tok"))
        .await
        .unwrap();
    let addr = l.local_addr().unwrap();
    let stop = sprig_common::CancellationToken::new();
    let server = tokio::spawn(http::serve(
        l,
        http::router(svc, Some("tok".into())),
        stop.clone(),
    ));
    let endpoint = format!("http://{addr}");
    let c = AdminClient::new(&endpoint, Some("tok".into())).unwrap();

    let r: RegisterDcResponse = c
        .post(
            "/admin/v1/dcs",
            &RegisterDcRequest {
                dc: DcId::new("ctl-t").unwrap(),
                kind: "full".into(),
                tags: serde_json::Map::new(),
                global_voter: false,
            },
        )
        .await
        .unwrap();
    assert!(r.created);
    let l: ListDcsResponse = c.get("/admin/v1/dcs").await.unwrap();
    assert_eq!(render::dc_list(&l.dcs).lines().count(), 2);
    let v: GetDcResponse = c.get("/admin/v1/dcs/ctl-t").await.unwrap();
    assert!(render::dc_show(&v).contains("leader:        none"));
    let empty: ListClustersResponse = c.get("/admin/v1/clusters").await.unwrap();
    assert!(empty.clusters.is_empty());

    let req = cluster::build(&RegisterFlags {
        name: Some("x1".into()),
        topology: Some("single".into()),
        nodes: vec!["127.0.0.1:1".into()],
        credentials: Some("env:SPRIGCTL_TEST_NO_SUCH_VAR".into()),
        ..RegisterFlags::default()
    })
    .unwrap();
    let e = c
        .post::<_, RegisterClusterResponse>("/admin/v1/clusters", &req)
        .await
        .unwrap_err();
    assert_eq!(
        e.to_string(),
        "39504 ER_SPRIG_NOT_LEADER: dc ctl-t has no leader yet; try again"
    );
    let e = c
        .get::<ClusterView>("/admin/v1/clusters/ctl-t-c5")
        .await
        .unwrap_err();
    assert!(matches!(&e, CallError::Api(b) if b.code == 39104), "{e}");

    let anon = AdminClient::new(&endpoint, None).unwrap();
    let e = anon
        .get::<ListDcsResponse>("/admin/v1/dcs")
        .await
        .unwrap_err();
    assert!(
        e.to_string()
            .starts_with("39793 ER_SPRIG_ADMIN_UNAUTHENTICATED"),
        "{e}"
    );
    assert!(AdminClient::new("https://127.0.0.1:8443", None).is_err());
    assert!(AdminClient::new("127.0.0.1:7480", None).is_err());
    stop.cancel();
    server.await.unwrap().unwrap();
    let down = AdminClient::new(&endpoint, None).unwrap();
    assert!(matches!(
        down.get::<ListDcsResponse>("/admin/v1/dcs")
            .await
            .unwrap_err(),
        CallError::Transport(_)
    ));
}
