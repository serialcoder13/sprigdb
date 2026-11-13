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
            node_identities: std::collections::BTreeMap::new(),
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

// --- HTTPS (chapter 37, "Admin API over TLS") --------------------------------

#[tokio::test]
async fn the_client_over_https_with_the_deployment_ca() {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
    let dir = std::env::temp_dir().join(format!("sprigctl-https-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut cap = CertificateParams::new(Vec::<String>::new()).unwrap();
    cap.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let cak = KeyPair::generate().unwrap();
    let ca_cert = cap.self_signed(&cak).unwrap();
    let issuer = Issuer::new(cap, cak);
    let lk = KeyPair::generate().unwrap();
    let leaf = CertificateParams::new(vec!["localhost".to_owned()])
        .unwrap()
        .signed_by(&lk, &issuer)
        .unwrap();
    let (ca, cert, key) = (dir.join("ca.pem"), dir.join("c.pem"), dir.join("k.pem"));
    std::fs::write(&ca, ca_cert.pem()).unwrap();
    std::fs::write(&cert, leaf.pem()).unwrap();
    std::fs::write(&key, lk.serialize_pem()).unwrap();

    let tls = sprig_common::https::server_config(&cert, &key, None).unwrap();
    let l = http::bind("127.0.0.1:0".parse().unwrap(), Some("tok"))
        .await
        .unwrap();
    let port = l.local_addr().unwrap().port();
    let stop = sprig_common::CancellationToken::new();
    let app = axum::Router::new().route(
        "/admin/v1/ping",
        axum::routing::get(|h: axum::http::HeaderMap| async move {
            serde_json::json!({
                "auth": h.get("authorization").and_then(|v| v.to_str().ok()),
            })
            .to_string()
        }),
    );
    let server = tokio::spawn(http::serve_tls(l, app, Some(tls), stop.clone()));
    let endpoint = format!("https://localhost:{port}");

    // Without --ca-file: refused before any call.
    let e = AdminClient::new(&endpoint, None).unwrap_err();
    assert!(e.to_string().contains("--ca-file"), "{e}");
    // With it: the call goes through, the token with it.
    let files = client::TlsFiles {
        ca_file: Some(ca.clone()),
        client: None,
    };
    let c = AdminClient::with_tls(&endpoint, Some("tok".into()), &files).unwrap();
    let v: serde_json::Value = c.get("/admin/v1/ping").await.unwrap();
    assert_eq!(v["auth"], "Bearer tok");
    // Plain HTTP to the HTTPS port fails.
    let plain = AdminClient::new(&format!("http://127.0.0.1:{port}"), None).unwrap();
    assert!(
        plain
            .get::<serde_json::Value>("/admin/v1/ping")
            .await
            .is_err()
    );
    // A CA that didn't sign the certificate: refused.
    let other = KeyPair::generate().unwrap();
    let mut op = CertificateParams::new(Vec::<String>::new()).unwrap();
    op.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let other_ca = dir.join("other.pem");
    std::fs::write(&other_ca, op.self_signed(&other).unwrap().pem()).unwrap();
    let bad = AdminClient::with_tls(
        &endpoint,
        None,
        &client::TlsFiles {
            ca_file: Some(other_ca),
            client: None,
        },
    )
    .unwrap();
    let e = bad
        .get::<serde_json::Value>("/admin/v1/ping")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("TLS"), "{e}");
    stop.cancel();
    server.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
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

// --- Tenants (chapter 08 step 9) --------------------------------------------

#[test]
fn tenant_commands_parse() {
    let m = cli()
        .try_get_matches_from([
            "sprigctl",
            "tenant",
            "create",
            "pune",
            "--under",
            "acme",
            "--cluster",
            "mum1-c7",
        ])
        .unwrap();
    let (_, t) = m.subcommand().unwrap();
    let (cmd, sub) = t.subcommand().unwrap();
    assert_eq!(cmd, "create");
    assert_eq!(sub.get_one::<String>("name").unwrap(), "pune");
    assert_eq!(sub.get_one::<String>("under").unwrap(), "acme");
    assert_eq!(sub.get_one::<String>("cluster").unwrap(), "mum1-c7");
    for args in [
        &["tenant", "list"][..],
        &["tenant", "show", "pune"],
        &["--yes", "tenant", "delete", "pune"],
        &["tenant", "move", "pune", "--to", "x"],
    ] {
        let mut full = vec!["sprigctl"];
        full.extend_from_slice(args);
        assert!(cli().try_get_matches_from(full).is_ok(), "{args:?}");
    }
    for bad in [
        &["tenant", "create"][..],
        &["tenant", "show"],
        &["tenant", "delete"],
    ] {
        let mut full = vec!["sprigctl"];
        full.extend_from_slice(bad);
        assert!(cli().try_get_matches_from(full).is_err(), "{bad:?}");
    }
    // Chapter 24's `tenant move` and chapter 25's `tenant failover` are
    // implemented.
    assert!(!TENANT_LATER.contains(&"move"));
    assert!(!TENANT_LATER.contains(&"failover"));
    for good in [
        &["tenant", "failover", "acme", "--to", "ams1"][..],
        &[
            "tenant",
            "failover",
            "acme",
            "--to",
            "ams1",
            "--accept-data-loss",
            "--old-home-fenced",
        ],
        &["tenant", "failover", "--all-from", "fra1", "--to", "ams1"],
        &["tenant", "failover", "acme", "--errant"],
        &[
            "tenant",
            "standby",
            "acme",
            "--dc",
            "ams1",
            "--cluster",
            "ams1-c1",
        ],
    ] {
        let mut full = vec!["sprigctl"];
        full.extend_from_slice(good);
        assert!(cli().try_get_matches_from(full).is_ok(), "{good:?}");
    }
    for bad in [
        &["tenant", "failover", "acme"][..],
        &["tenant", "failover", "acme", "--to", "ams1", "--errant"],
    ] {
        let mut full = vec!["sprigctl"];
        full.extend_from_slice(bad);
        assert!(cli().try_get_matches_from(full).is_err(), "{bad:?}");
    }
    for good in [
        &["tenant", "move", "acme", "--to", "fra1"][..],
        &[
            "tenant",
            "move",
            "acme",
            "--to",
            "fra1",
            "--max-pause-ms",
            "3000",
            "--wait",
        ],
        &["tenant", "move", "acme", "--status"],
        &["tenant", "traffic", "acme"],
    ] {
        let mut full = vec!["sprigctl"];
        full.extend_from_slice(good);
        assert!(cli().try_get_matches_from(full).is_ok(), "{good:?}");
    }
    let mut full = vec!["sprigctl"];
    full.extend_from_slice(&["tenant", "move", "acme"]);
    assert!(
        cli().try_get_matches_from(full).is_err(),
        "move needs --to or --status"
    );
}

fn tenant_view(id: u64, name: &str, parent: Option<&str>) -> TenantView {
    TenantView {
        tenant_id: sprig_common::ids::TenantId(id),
        name: name.into(),
        state: "active".into(),
        home_dc: DcId::new("mum1").unwrap(),
        parent_tenant_id: u64::from(parent.is_some()),
        parent: parent.map(Into::into),
        path: if parent.is_some() {
            format!("/1/{id}/")
        } else {
            format!("/{id}/")
        },
        depth: u32::from(parent.is_some()),
        child_count: 0,
        catalog_version: 4,
        auth_version: 0,
        settings_version: 3,
        cluster: Some(ClusterId::new("mum1-c1").unwrap()),
        suspended: false,
        suspended_reason: None,
        ancestry: parent.into_iter().map(Into::into).collect(),
        created_at: "2026-11-08T09:00:00Z".into(),
        mod_revision: 9,
    }
}

#[test]
fn tenant_output() {
    let list = render::tenant_list(&[
        tenant_view(1, "acme", None),
        tenant_view(2, "pune", Some("acme")),
    ]);
    let lines: Vec<&str> = list.lines().collect();
    assert_eq!(lines.len(), 3);
    assert!(
        lines[0].starts_with("ID  NAME  STATE   PARENT  HOME"),
        "{list}"
    );
    assert!(lines[2].contains("pune  active  acme    mum1"), "{list}");
    assert!(lines[2].ends_with("4/3"), "{list}");
    let show = render::tenant_show(&tenant_view(2, "pune", Some("acme")));
    for want in [
        "tenant:            pune (2)",
        "ancestry:          acme",
        "catalog cluster:   mum1-c1",
        "settings version:  3",
    ] {
        assert!(show.contains(want), "{want}: {show}");
    }
    assert!(render::tenant_show(&tenant_view(1, "acme", None)).contains("- (a root tenant)"));
}

/// Answers tenant calls from memory, for the HTTP face.
struct FakeTenants;

#[async_trait::async_trait]
impl sprig_controller::tenants::TenantAdmin for FakeTenants {
    async fn create(
        &self,
        req: CreateTenantRequest,
        guard: Vec<sprig_meta::Cmp>,
    ) -> sprig_common::Result<CreateTenantResponse> {
        assert_eq!(guard.len(), 1, "the leader's guard");
        Ok(CreateTenantResponse {
            created: true,
            tenant: tenant_view(2, &req.name, req.parent.as_deref()),
            placement: Some(
                if req.cluster.is_some() {
                    "requested"
                } else {
                    "fewest_tenants"
                }
                .into(),
            ),
        })
    }
    async fn list(&self) -> sprig_common::Result<ListTenantsResponse> {
        Ok(ListTenantsResponse {
            tenants: vec![tenant_view(1, "acme", None)],
        })
    }
    async fn show(&self, name: &str) -> sprig_common::Result<TenantView> {
        if name == "acme" {
            Ok(tenant_view(1, "acme", None))
        } else {
            Err(sprig_common::SprigError::new(
                sprig_common::SprigCode::TenantNotReady,
                format!("no tenant named {name:?}"),
            ))
        }
    }
    async fn delete(
        &self,
        name: &str,
        _: Vec<sprig_meta::Cmp>,
    ) -> sprig_common::Result<TenantView> {
        let mut v = tenant_view(1, name, None);
        v.state = "deleting".into();
        Ok(v)
    }
}

#[tokio::test]
async fn tenant_calls_over_the_admin_api() {
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
    let (status_tx, rx) = tokio::sync::watch::channel(Status::default());
    let stores = Stores {
        dc: Arc::new(store.clone()),
        global: Arc::new(global.clone()),
    };
    let bare = Arc::new(AdminService::new(
        stores.clone(),
        Arc::clone(&monitor),
        rx.clone(),
        Arc::new(SystemClock),
    ));
    let svc = Arc::new(
        AdminService::new(stores, monitor, rx, Arc::new(SystemClock))
            .with_tenants(Arc::new(FakeTenants)),
    );
    let mut ends = Vec::new();
    let stop = sprig_common::CancellationToken::new();
    for s in [svc, bare] {
        let l = http::bind("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        ends.push(format!("http://{}", l.local_addr().unwrap()));
        tokio::spawn(http::serve(l, http::router(s, None), stop.clone()));
    }
    let c = AdminClient::new(&ends[0], None).unwrap();
    let req = CreateTenantRequest {
        name: "pune".into(),
        parent: Some("acme".into()),
        cluster: None,
    };
    // Off the leader: refused.
    let e = c
        .post::<_, CreateTenantResponse>("/admin/v1/tenants", &req)
        .await
        .unwrap_err();
    assert!(e.to_string().starts_with("39504"), "{e}");
    // As the leader.
    status_tx.send_modify(|s| s.dc_leader = Some(1));
    let r: CreateTenantResponse = c.post("/admin/v1/tenants", &req).await.unwrap();
    assert!(r.created);
    assert_eq!(r.tenant.parent.as_deref(), Some("acme"));
    assert_eq!(r.placement.as_deref(), Some("fewest_tenants"));
    let l: ListTenantsResponse = c.get("/admin/v1/tenants").await.unwrap();
    assert_eq!(l.tenants[0].name, "acme");
    let v: TenantView = c.get("/admin/v1/tenants/acme").await.unwrap();
    assert_eq!(v.tenant_id.0, 1);
    let e = c
        .get::<TenantView>("/admin/v1/tenants/nobody")
        .await
        .unwrap_err();
    assert!(matches!(&e, CallError::Api(b) if b.code == 39072), "{e}");
    let d: TenantView = c.delete("/admin/v1/tenants/acme").await.unwrap();
    assert_eq!(d.state, "deleting");
    // A controller without tenant management says so.
    let e = AdminClient::new(&ends[1], None)
        .unwrap()
        .get::<ListTenantsResponse>("/admin/v1/tenants")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("doesn't manage tenants"), "{e}");
    stop.cancel();
}

// --- Settings (chapter 08 steps 7-8, chapter 23 step 8, chapter 47) --------

mod index_cli;
mod settings_cli;
