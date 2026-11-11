use super::*;
use crate::testutil::{failed, node, probe, raw};

#[test]
fn single_11_8() {
    let f = NodeFacts::parse(&raw("11.8-single"));
    assert_eq!(f.server.version, Some(ServerVersion::new(11, 8, 9)));
    assert_eq!(f.server.read_only, Some(ReadOnly::Off));
    assert!(f.writable() && !f.is_replica() && f.galera.is_none());
    assert_eq!(f.server.default_storage_engine, "InnoDB");
    // Not in 11.8: the XA probe decides alone.
    assert_eq!(f.server.legacy_xa_rollback_at_disconnect, None);
    assert_eq!(f.server.binlog_alter_two_phase, Some(false));
    let b = &f.binlog;
    assert!(b.log_bin && b.gtid_strict_mode && b.log_slave_updates && b.in_files());
    assert_eq!(b.format, Some(BinlogFormat::Row));
    assert_eq!(b.row_image, Some(RowImage::Full));
    assert_eq!(b.row_metadata, Some(RowMetadata::Full));
    assert_eq!(b.expire_logs_seconds, 604_800);
    assert!(b.files.is_some_and(|n| n >= 1) && b.oldest_file.is_some());
    assert!(!f.semi_sync.master_enabled);
    assert_eq!(f.routines.stored_program_cache, 256);
    assert_eq!(f.tables.table_definition_cache, 400);
    let p = f.privileges.expect("grants captured");
    assert!(p.create_user && p.connection_admin && p.set_user && p.tenant_schemas_grant_option);
}

#[test]
fn primary_11_8_runs_semi_sync_after_sync() {
    let f = NodeFacts::parse(&raw("11.8-primary"));
    let s = f.semi_sync;
    assert!(s.master_enabled && s.master_active);
    assert!(s.master_clients >= 1, "{s:?}");
    assert_eq!(s.wait_point, Some(WaitPoint::AfterSync));
    assert_eq!(s.timeout_ms, 10_000);
    assert!(!f.is_replica() && f.writable());
    assert_eq!(f.binlog.gtid_domain_id, 2);
}

#[test]
fn replica_11_8_has_one_running_channel() {
    let f = NodeFacts::parse(&raw("11.8-replica"));
    assert!(f.is_replica());
    let c = &f.channels[0];
    assert!(c.running(), "{c:?}");
    assert_eq!(c.master_host, "primary");
    assert_eq!(c.master_port, 3306);
    assert_eq!(c.master_server_id, 201);
    assert_eq!(c.using_gtid, "Slave_Pos");
    assert_eq!(c.seconds_behind, Some(0));
    assert!(f.semi_sync.slave_enabled && f.semi_sync.slave_active);
    let beats = f.heartbeats.expect("heartbeat table exists");
    assert!(beats.iter().any(|h| h.writer_node == "n1"));
}

#[test]
fn galera_11_8_node_is_usable() {
    let f = NodeFacts::parse(&raw("11.8-galera"));
    let g = f.galera.clone().expect("wsrep on");
    assert!(g.usable());
    assert_eq!(g.local_state, GaleraLocalState::Synced);
    assert_eq!(g.cluster_size, 3);
    assert!(g.gtid_mode);
    assert_eq!(g.gtid_domain_id, Some(3));
    assert_eq!(g.osu_method, "TOI");
    assert!(g.conf_id >= 1);
    assert!(!f.is_replica());
    // The fixture predates the state UUID: unknown, not lost.
    assert_eq!(g.state_uuid, None);
    assert!(!g.lost_identity());
}

#[test]
fn galera_state_uuid_is_read() {
    let mut r = raw("11.8-galera");
    r.status.insert(
        "wsrep_cluster_state_uuid".into(),
        "00000000-0000-0000-0000-000000000000".into(),
    );
    let g = NodeFacts::parse(&r).galera.expect("wsrep on");
    assert!(g.lost_identity());
    assert!(g.usable(), "it still serves");

    r.status.insert(
        "wsrep_cluster_state_uuid".into(),
        "f01a6c83-8353-410e-bd55-31ce16ff1426".into(),
    );
    let g = NodeFacts::parse(&r).galera.expect("wsrep on");
    assert_eq!(
        g.state_uuid.as_deref(),
        Some("f01a6c83-8353-410e-bd55-31ce16ff1426")
    );
    assert!(!g.lost_identity());
}

#[test]
fn mariadb_12_3() {
    let f = NodeFacts::parse(&raw("12.3-single"));
    assert_eq!(f.server.version, Some(ServerVersion::new(12, 3, 3)));
    // 12.3 has binlog_storage_engine, empty: the binlog is in files.
    assert_eq!(f.binlog.storage_engine, None);
    assert!(f.binlog.in_files());
    // Semi-sync variables keep their names on 12.3 (open question 1).
    assert!(f.semi_sync.master_enabled && f.semi_sync.master_active);
    assert_eq!(f.semi_sync.wait_point, Some(WaitPoint::AfterCommit));
    // Galera is off: no wsrep facts even though wsrep_* status rows exist.
    assert!(f.galera.is_none());

    let ro = NodeFacts::parse(&raw("12.3-read-only-no-lock"));
    assert_eq!(ro.server.read_only, Some(ReadOnly::NoLock));
    assert!(!ro.writable());
}

#[test]
fn innodb_binlog_is_not_in_files() {
    let mut r = raw("12.3-single");
    r.variables
        .insert("binlog_storage_engine".into(), "innodb".into());
    assert!(!NodeFacts::parse(&r).binlog.in_files());
}

#[test]
fn active_target_is_a_writer() {
    let probes = crate::testutil::replication();
    assert_eq!(
        active_target(TopologyKind::PrimaryReplica, &probes),
        Some(0)
    );
    let mut swapped = vec![probes[1].clone(), probes[0].clone()];
    assert_eq!(
        active_target(TopologyKind::PrimaryReplica, &swapped),
        Some(1)
    );
    swapped[1] = failed("n1", None);
    assert_eq!(active_target(TopologyKind::PrimaryReplica, &swapped), None);
    let g = crate::testutil::galera();
    assert_eq!(active_target(TopologyKind::Galera, &g), Some(0));
    // A non-Galera node never stands in for a Galera cluster.
    let single = vec![probe("n1", &raw("11.8-single"))];
    assert_eq!(active_target(TopologyKind::Galera, &single), None);
}

#[test]
fn depth_orders_and_helpers() {
    assert!(ProbeDepth::Light < ProbeDepth::Full && ProbeDepth::Full < ProbeDepth::Deep);
    assert_eq!(
        decimal_to_us("1762519022.252233"),
        Some(1_762_519_022_252_233)
    );
    assert_eq!(decimal_to_us("5.1"), Some(5_100_000));
    assert_eq!(decimal_to_us("7"), Some(7_000_000));
    assert_eq!(decimal_to_us("x"), None);
    assert_eq!(in_list(&["a", "b'c"]), "'a', 'b''c'");
    // Every name is lowercase: the capture map is keyed in lowercase.
    for n in VARIABLES.iter().chain(STATUS) {
        assert_eq!(*n, n.to_ascii_lowercase());
    }
}

#[test]
fn fixtures_hold_no_password_hashes() {
    for name in [
        "11.8-single",
        "11.8-primary",
        "11.8-replica",
        "11.8-galera",
        "12.3-single",
    ] {
        let r = raw(name);
        for g in r.grants.unwrap_or_default() {
            assert!(!g.contains("IDENTIFIED"), "{name}: {g}");
        }
    }
}

/// r2opfix: a probe that reaches another server than the pinned one is
/// refused (the node becomes `broken`); a node without a pin gets one.
#[test]
fn identity_is_pinned_and_checked() {
    use std::collections::BTreeMap;
    let with_uuid = |host: &str| {
        let mut r = raw("11.8-replica");
        r.variables.insert("hostname".into(), host.into());
        r
    };
    let mut probes = vec![
        probe("n1", &with_uuid("db-a-1")),
        probe("n2", &with_uuid("db-b-2")),
        probe("n3", &with_uuid("db-c-3")),
        failed("n4", None),
    ];
    let mut pinned = BTreeMap::new();
    pinned.insert(node("n1"), probes[0].facts().unwrap().server.identity());
    pinned.insert(node("n2"), "other-server".to_owned());
    let new = check_identity(&pinned, &mut probes);
    assert_eq!(new.len(), 1);
    assert_eq!(new[0].0, node("n3"));
    assert!(new[0].1.starts_with("db-c-3#"), "{}", new[0].1);
    assert!(probes[0].result.is_ok());
    let e = probes[1].result.as_ref().unwrap_err();
    assert!(
        e.is_auth_failure(),
        "a wrong node is broken, not unreachable"
    );
    assert!(e.message.contains("db-b-2#") && e.message.contains("other-server"));
    assert!(probes[2].result.is_ok());
    assert!(probes[3].result.is_err());
}
