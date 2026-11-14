//! Classification rules on probe captures from MariaDB 11.8 (sprig-cluster's
//! fixtures), and the registration flow against Compose when
//! `SPRIG_TEST_MARIADB_SINGLE` names the single node's root URL.

#![allow(clippy::unwrap_used)]

use sprig_cluster::probe::{ActiveProbes, FeedLevel, NodeFacts, RawCapture};
use sprig_cluster::record::{Capacity, SecretRefs};
use sprig_cluster::sql::ProbeError;
use sprig_common::ids::{DcId, NodeId};
use sprig_meta::{MemStore, Tier};

use super::*;

fn raw(name: &str) -> RawCapture {
    let text = match name {
        "primary" => include_str!("../../../sprig-cluster/fixtures/11.8-primary.json"),
        "replica" => include_str!("../../../sprig-cluster/fixtures/11.8-replica.json"),
        "galera" => include_str!("../../../sprig-cluster/fixtures/11.8-galera.json"),
        "single" => include_str!("../../../sprig-cluster/fixtures/11.8-single.json"),
        other => panic!("no fixture {other}"),
    };
    serde_json::from_str(text).unwrap()
}

fn probe(id: &str, raw: &RawCapture) -> NodeProbe {
    NodeProbe {
        node: NodeId::new(id).unwrap(),
        depth: ProbeDepth::Deep,
        at_unix_ms: 0,
        took_ms: 1,
        result: Ok(NodeFacts::parse(raw)),
        active: None,
    }
}

fn active() -> ActiveProbes {
    ActiveProbes {
        sys_schema: true,
        xa: true,
        sql_log_bin_settable: true,
        ..ActiveProbes::default()
    }
}

fn replication() -> Vec<NodeProbe> {
    let mut v = vec![
        probe("n1", &raw("primary")),
        probe("n2", &raw("replica")),
        probe("n3", &raw("replica")),
    ];
    v[0].active = Some(active());
    v
}

fn tls(n: usize) -> Vec<Option<SessionCheck>> {
    vec![
        Some(SessionCheck {
            cipher: Some("TLS_AES_256_GCM_SHA384".into()),
            require_secure_transport: Some(true),
            privileges: None,
        });
        n
    ]
}

fn run(
    topology: TopologyKind,
    probes: &[NodeProbe],
    sessions: &[Option<SessionCheck>],
) -> (Vec<String>, Vec<String>) {
    let profile = derive(DeriveInput {
        topology,
        probes,
        declared: None,
        previous: None,
    });
    classify(&Classify {
        topology,
        probes,
        profile: &profile,
        sessions,
        fleet_lower_case_table_names: 0,
        retention_hours: 168,
    })
}

#[test]
fn captured_topologies_pass() {
    let (b, a) = run(TopologyKind::PrimaryReplica, &replication(), &tls(3));
    assert!(b.is_empty(), "{b:#?}");
    assert!(a.is_empty(), "{a:#?}");
    let g = raw("galera");
    let galera = vec![probe("n1", &g), probe("n2", &g), probe("n3", &g)];
    let (b, _) = run(TopologyKind::Galera, &galera, &tls(3));
    assert!(b.is_empty(), "{b:#?}");
    let (b, _) = run(
        TopologyKind::SingleNode,
        &[probe("n1", &raw("single"))],
        &tls(1),
    );
    assert!(b.is_empty(), "{b:#?}");
}

#[test]
fn plain_text_sessions_and_open_transport_are_reported() {
    let mut s = tls(3);
    s[1] = Some(SessionCheck {
        cipher: Some(String::new()),
        require_secure_transport: Some(false),
        privileges: None,
    });
    let (b, a) = run(TopologyKind::PrimaryReplica, &replication(), &s);
    assert_eq!(
        b,
        [
            "node n2: the sprig_ctl session isn't encrypted; SprigDB connects over TLS only \
          (configure ssl_cert and ssl_key on the server)"
        ]
    );
    assert_eq!(
        a,
        ["node n2: require_secure_transport is OFF: SET GLOBAL require_secure_transport = ON"]
    );
}

#[test]
fn binlog_settings_block_with_their_fixes() {
    let mut p = replication();
    {
        let f = p[1].result.as_mut().unwrap();
        f.binlog.row_image = Some(sprig_cluster::probe::RowImage::Minimal);
        f.binlog.log_slave_updates = false;
        f.binlog.expire_logs_seconds = 3600;
    }
    let (b, a) = run(TopologyKind::PrimaryReplica, &p, &tls(3));
    assert_eq!(
        b,
        [
            "node n2: binlog_row_image is MINIMAL, needs FULL: SET GLOBAL binlog_row_image = 'FULL'",
            "node n2: log_slave_updates is OFF, needs ON: set log_slave_updates in the server configuration and restart",
            "node n2: binlog_expire_logs_seconds is 3600, needs at least 86400: SET GLOBAL binlog_expire_logs_seconds = 604800",
        ]
    );
    assert_eq!(
        a,
        [
            "node n2: binlog_expire_logs_seconds is 3600, needs at least 604800: SET GLOBAL binlog_expire_logs_seconds = 604800"
        ]
    );
}

#[test]
fn topology_must_match_what_probes_see() {
    let (b, _) = run(TopologyKind::Galera, &replication(), &tls(3));
    assert!(
        b.iter().any(|l| l
            == "node n1 isn't a Galera node (wsrep_on is OFF), but the topology given is galera"),
        "{b:#?}"
    );
    let g = raw("galera");
    let (b, _) = run(TopologyKind::PrimaryReplica, &[probe("n1", &g)], &tls(1));
    assert!(
        b.iter()
            .any(|l| l == "node n1 runs Galera, but the topology given is primary_replica"),
        "{b:#?}"
    );
    let (b, _) = run(
        TopologyKind::PrimaryReplica,
        &[probe("n1", &raw("single"))],
        &tls(1),
    );
    assert!(
        b.contains(
            &"no node replicates: a primary_replica cluster needs at least one replica".to_owned()
        ),
        "{b:#?}"
    );
    let mut gtid_off = probe("n1", &g);
    gtid_off
        .result
        .as_mut()
        .unwrap()
        .galera
        .as_mut()
        .unwrap()
        .gtid_mode = false;
    let (b, _) = run(TopologyKind::Galera, &[gtid_off], &tls(1));
    assert!(
        b.iter()
            .any(|l| l.starts_with("node n1: wsrep_gtid_mode is OFF")),
        "{b:#?}"
    );
}

#[test]
fn missing_grants_split_into_blocking_and_advisories() {
    let mut p = replication();
    for x in &mut p {
        x.result.as_mut().unwrap().privileges = Some(PrivilegeInfo {
            sys_schema_all: true,
            create_user: true,
            ..PrivilegeInfo::default()
        });
    }
    let (b, a) = run(TopologyKind::PrimaryReplica, &p, &tls(3));
    assert_eq!(
        b,
        [
            "sprig_ctl can't manage tenant schemas: GRANT ALL ON `t%`.* TO sprig_ctl WITH GRANT OPTION"
        ]
    );
    assert_eq!(a.len(), 3, "{a:#?}");
    // The docs schema is an advisory: only docs search needs it.
    assert!(
        a[0].starts_with("sprig_ctl can't manage __sprig_docs; docs search is off"),
        "{a:#?}"
    );
    assert!(
        a[0].ends_with("GRANT ALL ON `\\_\\_sprig\\_docs`.* TO sprig_ctl WITH GRANT OPTION"),
        "{a:#?}"
    );
    assert!(a[1].starts_with("sprig_ctl lacks privileges; what needs them is disabled: GRANT PROCESS, CONNECTION ADMIN"), "{a:#?}");
    // With the grant, the advisory goes.
    for x in &mut p {
        let pr = x.result.as_mut().unwrap().privileges.as_mut().unwrap();
        pr.docs_schema_all = true;
    }
    let (_, a) = run(TopologyKind::PrimaryReplica, &p, &tls(3));
    assert!(!a.iter().any(|l| l.contains("__sprig_docs")), "{a:#?}");
}

#[test]
fn unreachable_nodes_block() {
    let mut p = replication();
    p[2].result = Err(ProbeError {
        errno: Some(1045),
        message: "Access denied".into(),
        timeout: false,
    });
    let (b, _) = run(TopologyKind::PrimaryReplica, &p, &tls(3));
    assert!(
        b[0].starts_with("node n3: no sprig_ctl login: error 1045"),
        "{b:#?}"
    );
}

fn request(dc: &str, secrets: SecretRefs, nodes: Vec<NodeSpec>) -> RegisterCluster {
    RegisterCluster {
        dc: DcId::new(dc).unwrap(),
        name: "dc-a-single".into(),
        topology: TopologyKind::SingleNode,
        nodes,
        secrets,
        capacity: Capacity::default(),
        declared: None,
    }
}

fn provisioner(dc: &str) -> ExternalProvisioner {
    let reg = ClusterRegistry::new(
        Arc::new(MemStore::new(Tier::Global)),
        Arc::new(MemStore::new(Tier::Dc(DcId::new(dc).unwrap()))),
    )
    .unwrap();
    ExternalProvisioner::new(reg, ClusterSettings::default())
}

/// The dump probe's result in a precheck report.
fn dump_of(r: &PrecheckReport) -> sprig_cluster::probe::DumpProbe {
    r.probes
        .iter()
        .find_map(|p| p.active.as_ref().and_then(|a| a.binlog_dump.clone()))
        .expect("the dump probe ran")
}

fn spec(addr: &str) -> NodeSpec {
    NodeSpec {
        node: NodeId::new("n1").unwrap(),
        addr: addr.into(),
        zone: None,
    }
}

#[tokio::test]
async fn input_and_credentials_are_checked_before_probing() {
    let p = provisioner("dc-a");
    let refs = SecretRefs {
        ctl: "env:SPRIG_TEST_UNSET_CTL_7f3a".into(),
        repl: "env:SPRIG_TEST_UNSET_REPL_7f3a".into(),
    };
    let mut req = request("dc-b", refs, vec![spec("127.0.0.1:1"), spec("127.0.0.1:1")]);
    req.name = "bad name".into();
    let r = p.precheck_report(&req).await;
    assert_eq!(
        r.blocking,
        [
            "the cluster is for dc dc-b, but this controller registers clusters of dc-a",
            "name \"bad name\": 1 to 64 letters, digits, '-' or '_'",
            "a single_node cluster has one node, not 2",
            "node n1 (127.0.0.1:1) is listed twice",
            "node n1: sprig_ctl credential: secret env:SPRIG_TEST_UNSET_CTL_7f3a: the variable isn't set",
            "node n1: sprig_ctl credential: secret env:SPRIG_TEST_UNSET_CTL_7f3a: the variable isn't set",
        ]
    );
    let e = p.register(req).await.unwrap_err();
    assert_eq!(
        e.code(),
        Some(sprig_common::SprigCode::ClusterPrecheckFailed)
    );
    assert!(
        e.message
            .contains("failed 6 registration precheck(s):\n- the cluster is for dc dc-b")
    );
    assert!(p.registry().list().await.unwrap().is_empty());
    assert!(!p.supports(ProvisionOp::Create));
    let job = p
        .retire(&ClusterId::new("dc-a-c1").unwrap())
        .await
        .unwrap_err();
    assert_eq!(
        job.code(),
        Some(sprig_common::SprigCode::ProvisionerUnsupported)
    );
}

/// The whole flow against the Compose single node, with root as both
/// accounts (development): prechecks pass, the record is active with a
/// profile; a powerless account fails with the exact missing items.
#[tokio::test]
async fn registers_the_compose_single_node() {
    let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: SPRIG_TEST_MARIADB_SINGLE is not set");
        return;
    };
    let opts = mysql_async::Opts::from_url(&url).unwrap();
    let addr = format!("{}:{}", opts.ip_or_hostname(), opts.tcp_port());
    let dir = std::env::temp_dir().join(format!("sprig-prov-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let creds = dir.join("creds.toml");
    std::fs::write(
        &creds,
        format!(
            "[sprig_ctl]\nuser = \"{u}\"\npassword = \"{p}\"\ntls = \"skip_verify\"\n\
             [sprig_repl]\nuser = \"{u}\"\npassword = \"{p}\"\ntls = \"skip_verify\"\n",
            u = opts.user().unwrap(),
            p = opts.pass().unwrap()
        ),
    )
    .unwrap();
    let refs = crate::credentials::refs_for(&format!("file:{}", creds.display())).unwrap();
    let p = provisioner("dc-a");
    let (stored, report) = p
        .register_report(request("dc-a", refs.clone(), vec![spec(&addr)]))
        .await
        .unwrap();
    assert!(report.passed(), "{:#?}", report.blocking);
    assert!(
        report.nodes[0]
            .tls_cipher
            .as_deref()
            .is_some_and(|c| !c.is_empty())
    );
    let rec = stored.body();
    assert_eq!(rec.admin_state, AdminState::Active);
    assert_eq!(rec.provisioner, ProvisionerKind::External);
    assert!(rec.profile.as_ref().unwrap().derived.xa);
    // Step 5 read the binlog as sprig_repl.
    let dump = dump_of(&report);
    assert!(dump.passed() && rec.profile.as_ref().unwrap().derived.feed_source);

    // An account with no privileges: refused, nothing written.
    let tag = p.tag();
    let user = format!("sprigprov_{tag}");
    let mut root = ProbeSession::connect(
        &NodeEndpoint::from_url(NodeId::new("n1").unwrap(), &url).unwrap(),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    root.exec(&format!("CREATE USER '{user}'@'%' IDENTIFIED BY 'x-{tag}'"))
        .await
        .unwrap();
    let weak = dir.join("weak.toml");
    std::fs::write(
        &weak,
        format!(
            "[sprig_ctl]\nuser = \"{user}\"\npassword = \"x-{tag}\"\ntls = \"skip_verify\"\n\
             [sprig_repl]\nuser = \"{user}\"\npassword = \"x-{tag}\"\ntls = \"skip_verify\"\n"
        ),
    )
    .unwrap();
    let weak_refs = crate::credentials::refs_for(&format!("file:{}", weak.display())).unwrap();
    let mut req = request("dc-a", weak_refs, vec![spec(&addr)]);
    req.name = "weak".into();
    let r = p.precheck_report(&req).await;

    root.exec(&format!("DROP USER IF EXISTS '{user}'@'%'"))
        .await
        .unwrap();
    root.close().await;
    std::fs::remove_dir_all(&dir).unwrap();

    // sprig_repl's missing grants are an advisory now (the dump test below).
    assert!(!r.passed());
    for want in [
        "sprig_ctl can't manage tenant schemas: GRANT ALL ON `t%`.* TO sprig_ctl WITH GRANT OPTION",
        "sprig_ctl can't manage __sprig_sys: GRANT ALL ON `\\_\\_sprig\\_sys`.* TO sprig_ctl WITH GRANT OPTION",
    ] {
        assert!(
            r.blocking.iter().any(|b| b == want),
            "missing {want:?} in {:#?}",
            r.blocking
        );
    }
    assert!(
        r.blocking
            .iter()
            .any(|b| b
                .starts_with("node n1: sprig_ctl lacks a privilege the probes need: error 1227")),
        "{:#?}",
        r.blocking
    );
    assert!(
        r.blocking
            .iter()
            .any(|b| b.starts_with("node n1: sprig_ctl can't create accounts")),
        "{:#?}",
        r.blocking
    );
    assert_eq!(p.registry().list().await.unwrap().len(), 1);
}

/// Root as `sprig_ctl`, an account without `REPLICATION SLAVE` as
/// `sprig_repl`, on the Compose single node: the dump probe fails with
/// 1227, registration still passes, the feed and the mover are
/// `unsupported`, and the advisory names the grant.
#[tokio::test]
async fn a_sprig_repl_without_replication_slave_leaves_the_feed_unsupported() {
    let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: SPRIG_TEST_MARIADB_SINGLE is not set");
        return;
    };
    let opts = mysql_async::Opts::from_url(&url).unwrap();
    let addr = format!("{}:{}", opts.ip_or_hostname(), opts.tcp_port());
    let p = provisioner("dc-a");
    let tag = p.tag();
    let user = format!("sprignorepl_{tag}");
    let dir = std::env::temp_dir().join(format!("sprig-prov-norepl-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut root = ProbeSession::connect(
        &NodeEndpoint::from_url(NodeId::new("n1").unwrap(), &url).unwrap(),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    root.exec(&format!("CREATE USER '{user}'@'%' IDENTIFIED BY 'x-{tag}'"))
        .await
        .unwrap();
    let mixed = dir.join("mixed.toml");
    std::fs::write(
        &mixed,
        format!(
            "[sprig_ctl]\nuser = \"{u}\"\npassword = \"{pw}\"\ntls = \"skip_verify\"\n\
             [sprig_repl]\nuser = \"{user}\"\npassword = \"x-{tag}\"\ntls = \"skip_verify\"\n",
            u = opts.user().unwrap(),
            pw = opts.pass().unwrap()
        ),
    )
    .unwrap();
    let mixed_refs = crate::credentials::refs_for(&format!("file:{}", mixed.display())).unwrap();
    let mut req = request("dc-a", mixed_refs, vec![spec(&addr)]);
    req.name = "no-repl".into();
    let m = p.precheck_report(&req).await;

    root.exec(&format!("DROP USER IF EXISTS '{user}'@'%'"))
        .await
        .unwrap();
    root.close().await;
    std::fs::remove_dir_all(&dir).unwrap();

    assert!(m.passed(), "{:#?}", m.blocking);
    let profile = m.profile.as_ref().unwrap();
    assert_eq!(profile.derived.feed_level, FeedLevel::Unsupported);
    assert!(!profile.derived.mover_source && !profile.derived.feed_source);
    let dump = dump_of(&m);
    let f = dump.failure.as_ref().expect("the dump failed");
    assert_eq!(
        (f.kind, f.errno),
        (DumpFailureKind::Privilege, Some(1227)),
        "{f:?}"
    );
    assert!(
        m.advisories.iter().any(|a| a
            == "sprig_repl can't read binlogs, so the change feed and the mover are off: \
                GRANT REPLICATION SLAVE, BINLOG MONITOR ON *.* TO sprig_repl"),
        "{:#?}",
        m.advisories
    );
}
