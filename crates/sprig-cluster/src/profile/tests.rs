use super::*;
use crate::probe::{GaleraLocalState, RowImage};
use crate::record::DeclaredFailover;
use crate::testutil::{facts_mut, failed, galera, probe, raw, replication, single};

fn derive_for(topology: TopologyKind, probes: &[NodeProbe]) -> CapabilityProfile {
    derive(DeriveInput {
        topology,
        probes,
        declared: None,
        previous: None,
    })
}

#[test]
fn single_node() {
    let p = derive_for(TopologyKind::SingleNode, &single());
    let d = &p.derived;
    assert!(d.xa && d.mover_source && d.feed_source && d.mover_target);
    assert!(!d.zero_loss_failover);
    assert_eq!(d.expected_loss, LossClass::NoReplica);
    assert_eq!(d.ryw, RywMethod::WriterOnly);
    assert_eq!(d.validation, None);
    assert!(d.online_index_build && d.enforce_json_schema && d.compiled_logic);
    assert_eq!(
        d.fence_methods,
        [FenceMethod::RevokeKill, FenceMethod::Rename]
    );
    assert_eq!(d.feed_retention_secs, 604_800);
    assert_eq!(p.server.version, Some(ServerVersion::new(11, 8, 9)));
    assert!(p.features.alter_two_phase, "binlog_alter_two_phase exists");
    assert_eq!(p.privileges.sql_log_bin_settable, Some(true));
}

#[test]
fn primary_with_semi_sync_replicas() {
    let p = derive_for(TopologyKind::PrimaryReplica, &replication());
    let d = &p.derived;
    assert!(d.zero_loss_failover);
    assert_eq!(d.expected_loss, LossClass::Zero);
    assert_eq!(d.ryw, RywMethod::GtidWait);
    assert_eq!(d.validation, Some(ValidationMethod::ReplicaBuild));
    assert!(d.xa && d.mover_source && d.online_index_build);
    assert_eq!(p.semi_sync.wait_point, Some(WaitPoint::AfterSync));
}

/// Each case changes the primary's facts; `zero_loss_failover` must drop.
#[test]
fn semi_sync_zero_loss_needs_every_condition() {
    type Tweak = fn(&mut NodeFacts);
    let cases: [(&str, Tweak); 5] = [
        ("disabled", |f| f.semi_sync.master_enabled = false),
        ("fell back", |f| f.semi_sync.master_active = false),
        ("no clients", |f| f.semi_sync.master_clients = 0),
        ("after commit", |f| {
            f.semi_sync.wait_point = Some(WaitPoint::AfterCommit);
        }),
        ("innodb binlog", |f| {
            f.binlog.storage_engine = Some("innodb".into());
        }),
    ];
    for (name, tweak) in cases {
        let mut probes = replication();
        tweak(facts_mut(&mut probes[0]));
        let p = derive_for(TopologyKind::PrimaryReplica, &probes);
        assert!(!p.derived.zero_loss_failover, "{name}");
        assert_eq!(p.derived.expected_loss, LossClass::Async, "{name}");
    }
}

#[test]
fn galera_cluster() {
    let p = derive_for(TopologyKind::Galera, &galera());
    let d = &p.derived;
    assert!(d.zero_loss_failover, "all synced");
    assert!(!d.xa, "XA START fails on Galera");
    assert_eq!(d.ryw, RywMethod::WsrepSyncWait);
    assert_eq!(d.validation, Some(ValidationMethod::GaleraRsu));
    assert!(!d.online_index_build);
    let g = p.galera.unwrap();
    assert_eq!((g.size, g.synced), (3, 3));
    assert!(g.gtid_mode && g.shared_gtid_domain);
    assert_eq!(p.privileges.osu_method_settable, Some(true));

    // One donor: not all synced.
    let mut probes = galera();
    facts_mut(&mut probes[2])
        .galera
        .as_mut()
        .unwrap()
        .local_state = GaleraLocalState::DonorDesynced;
    let p = derive_for(TopologyKind::Galera, &probes);
    assert!(!p.derived.zero_loss_failover);
    assert_eq!(p.galera.unwrap().synced, 2);

    // One unreachable: not all synced either.
    let mut probes = galera();
    probes[1] = failed("n2", None);
    assert!(
        !derive_for(TopologyKind::Galera, &probes)
            .derived
            .zero_loss_failover
    );

    // A different GTID domain on one node: readers can't switch nodes.
    let mut probes = galera();
    facts_mut(&mut probes[1])
        .galera
        .as_mut()
        .unwrap()
        .gtid_domain_id = Some(9);
    assert!(
        !derive_for(TopologyKind::Galera, &probes)
            .galera
            .unwrap()
            .shared_gtid_domain
    );

    // Two nodes: no RSU validation.
    let mut probes = galera();
    probes.pop();
    for p in &mut probes {
        facts_mut(p).galera.as_mut().unwrap().cluster_size = 2;
    }
    assert_eq!(
        derive_for(TopologyKind::Galera, &probes).derived.validation,
        None
    );
}

#[test]
fn minimal_row_image_clears_mover_and_feed_source() {
    let mut probes = single();
    facts_mut(&mut probes[0]).binlog.row_image = Some(RowImage::Minimal);
    let p = derive_for(TopologyKind::SingleNode, &probes);
    assert!(!p.derived.mover_source && !p.derived.feed_source);
    // A replica without log_slave_updates is no source either, but the
    // primary still is.
    let mut probes = replication();
    facts_mut(&mut probes[1]).binlog.log_slave_updates = false;
    assert!(
        derive_for(TopologyKind::PrimaryReplica, &probes)
            .derived
            .mover_source
    );
    // A failed dump probe clears it.
    let mut probes = single();
    probes[0].active.as_mut().unwrap().binlog_dump = Some(false);
    assert!(
        !derive_for(TopologyKind::SingleNode, &probes)
            .derived
            .mover_source
    );
}

#[test]
fn privileges_gate_what_needs_them() {
    let mut probes = single();
    facts_mut(&mut probes[0]).privileges = Some(PrivilegeInfo {
        tenant_schemas_all: true,
        tenant_schemas_grant_option: true,
        sys_schema_all: true,
        create_user: true,
        ..PrivilegeInfo::default()
    });
    let p = derive_for(TopologyKind::SingleNode, &probes);
    assert_eq!(
        p.derived.fence_methods,
        [FenceMethod::Rename],
        "no CONNECTION ADMIN"
    );
    assert!(!p.derived.compiled_logic, "no SET USER");
    assert!(p.derived.mover_target);

    let mut probes = replication();
    facts_mut(&mut probes[0])
        .privileges
        .as_mut()
        .unwrap()
        .read_only_admin = false;
    let p = derive_for(TopologyKind::PrimaryReplica, &probes);
    assert_eq!(p.derived.validation, None, "no READ_ONLY ADMIN");
}

#[test]
fn legacy_xa_rollback_disables_xa() {
    let mut probes = single();
    facts_mut(&mut probes[0])
        .server
        .legacy_xa_rollback_at_disconnect = Some(true);
    assert!(!derive_for(TopologyKind::SingleNode, &probes).derived.xa);
    facts_mut(&mut probes[0])
        .server
        .legacy_xa_rollback_at_disconnect = Some(false);
    assert!(derive_for(TopologyKind::SingleNode, &probes).derived.xa);
}

#[test]
fn external_declared_rpo() {
    let probes = vec![probe("n1", &raw("11.8-single"))];
    let mut declared = Declared {
        failover: Some(DeclaredFailover::Provider),
        rpo_seconds: Some(0),
        trust: Some(Trust::Asserted),
        ..Declared::default()
    };
    let with = |d: &Declared| {
        derive(DeriveInput {
            topology: TopologyKind::External,
            probes: &probes,
            declared: Some(d),
            previous: None,
        })
    };
    assert!(with(&declared).derived.zero_loss_failover);
    assert_eq!(with(&declared).derived.expected_loss, LossClass::Zero);
    declared.trust = Some(Trust::Unverified);
    assert!(!with(&declared).derived.zero_loss_failover);
    assert_eq!(with(&declared).derived.expected_loss, LossClass::Declared);
}

#[test]
fn light_probes_carry_deep_facts_over() {
    let deep = derive_for(TopologyKind::SingleNode, &single());
    // A light probe: no grants, no active probes.
    let mut light = vec![probe("n1", &raw("11.8-single"))];
    facts_mut(&mut light[0]).privileges = None;
    let p = derive(DeriveInput {
        topology: TopologyKind::SingleNode,
        probes: &light,
        declared: None,
        previous: Some(&deep),
    });
    assert!(p.derived.xa && p.derived.compiled_logic && p.derived.enforce_json_schema);
    assert_eq!(p.features.oracle_routines, deep.features.oracle_routines);
    assert_eq!(p.privileges, deep.privileges);
    assert!(diff(&deep, &p).is_empty(), "{:?}", diff(&deep, &p));
}

#[test]
fn nothing_answered() {
    let p = derive_for(TopologyKind::SingleNode, &[failed("n1", None)]);
    assert!(p.derived.fence_methods.is_empty());
    assert!(!p.derived.xa && !p.derived.mover_source);
    assert_eq!(p.derived.expected_loss, LossClass::Unknown);
}

#[test]
fn registration_gates() {
    let probes = single();
    let p = derive_for(TopologyKind::SingleNode, &probes);
    let g = registration_gaps(&probes, &p, 0);
    assert!(g.blocking.is_empty() && g.advisories.is_empty(), "{g:?}");

    let g = registration_gaps(&probes, &p, 1);
    assert!(g.blocking[0].contains("lower_case_table_names"), "{g:?}");

    let mut old = single();
    facts_mut(&mut old[0]).server.version = Some(ServerVersion::new(11, 4, 5));
    facts_mut(&mut old[0]).server.default_storage_engine = "MyISAM".into();
    facts_mut(&mut old[0]).privileges = Some(PrivilegeInfo::default());
    let p = derive_for(TopologyKind::SingleNode, &old);
    let g = registration_gaps(&old, &p, 0);
    assert_eq!(g.blocking.len(), 2, "{g:?}");
    assert!(g.blocking.iter().any(|b| b.contains("11.4.5")));
    assert!(g.blocking.iter().any(|b| b.contains("InnoDB")));
    assert!(g.advisories.iter().any(|a| a.contains("CONNECTION ADMIN")));

    let down = [failed("n1", Some(1045))];
    let p = derive_for(TopologyKind::SingleNode, &down);
    let g = registration_gaps(&down, &p, 0);
    assert_eq!(g.blocking.len(), 1);
    assert!(g.blocking[0].contains("no sprig_ctl login"));
}

#[test]
fn diff_reports_directions() {
    let a = derive_for(TopologyKind::PrimaryReplica, &replication());
    let mut b = a.clone();
    b.derived.zero_loss_failover = false;
    b.derived.expected_loss = LossClass::Async;
    b.features.vector = false;
    b.derived.fence_methods = vec![FenceMethod::Rename];
    b.tables.opened_tables += 1000; // counters are not capabilities
    let changes = diff(&a, &b);
    let by = |f: &str| changes.iter().find(|c| c.field == f).map(|c| c.direction);
    assert_eq!(by("derived.zero_loss_failover"), Some(Direction::Downgrade));
    assert_eq!(by("features.vector"), Some(Direction::Downgrade));
    assert_eq!(
        by("derived.fence_methods.revoke_kill"),
        Some(Direction::Downgrade)
    );
    assert_eq!(by("derived.expected_loss"), Some(Direction::Changed));
    assert_eq!(changes.len(), 4, "{changes:?}");
    let back = diff(&b, &a);
    assert!(back.iter().all(|c| c.direction != Direction::Downgrade));
}

/// Hysteresis under a simulated clock.
#[test]
fn downgrades_publish_at_once_upgrades_wait() {
    let sustain = Duration::from_secs(60);
    let t0 = Instant::now();
    let good = derive_for(TopologyKind::PrimaryReplica, &replication());
    let mut fallen = replication();
    facts_mut(&mut fallen[0]).semi_sync.master_active = false;
    let bad = derive_for(TopologyKind::PrimaryReplica, &fallen);

    let mut pubr = CapabilityPublisher::new(None, sustain);
    let first = pubr.observe(good.clone(), t0).unwrap();
    assert_eq!(first.profile.version, 1);
    assert!(first.changes.is_empty());
    assert!(
        pubr.observe(good.clone(), t0).is_none(),
        "no change, no publication"
    );

    // Downgrade: at once.
    let down = pubr
        .observe(bad.clone(), t0 + Duration::from_secs(5))
        .unwrap();
    assert_eq!(down.profile.version, 2);
    assert!(!down.profile.derived.zero_loss_failover);
    assert_eq!(down.profile.derived.expected_loss, LossClass::Async);

    // Upgrade: held, including the values that follow it.
    let t1 = t0 + Duration::from_secs(10);
    assert!(pubr.observe(good.clone(), t1).is_none());
    assert!(pubr.pending().contains(&"derived.zero_loss_failover"));
    assert!(
        pubr.observe(good.clone(), t1 + Duration::from_secs(59))
            .is_none()
    );

    // Flapping restarts the hold.
    assert!(
        pubr.observe(bad.clone(), t1 + Duration::from_secs(59))
            .is_none()
    );
    assert!(pubr.pending().is_empty());
    let t2 = t1 + Duration::from_secs(70);
    assert!(pubr.observe(good.clone(), t2).is_none());
    assert!(
        pubr.observe(good.clone(), t2 + Duration::from_secs(30))
            .is_none()
    );
    let up = pubr.observe(good.clone(), t2 + sustain).unwrap();
    assert_eq!(up.profile.version, 3);
    assert!(up.profile.derived.zero_loss_failover);
    assert_eq!(up.profile.derived.expected_loss, LossClass::Zero);
    assert!(
        up.changes
            .iter()
            .any(|c| c.field == "derived.zero_loss_failover" && c.direction == Direction::Upgrade)
    );
    assert_eq!(pubr.published().unwrap().version, 3);
}

#[test]
fn zero_sustain_publishes_upgrades_at_once() {
    let t0 = Instant::now();
    let mut fallen = replication();
    facts_mut(&mut fallen[0]).semi_sync.master_active = false;
    let mut pubr = CapabilityPublisher::new(
        Some(derive_for(TopologyKind::PrimaryReplica, &fallen)),
        Duration::from_secs(60),
    );
    pubr.set_sustain(Duration::ZERO);
    let up = pubr
        .observe(derive_for(TopologyKind::PrimaryReplica, &replication()), t0)
        .unwrap();
    assert!(up.profile.derived.zero_loss_failover);
}

#[test]
fn triggers_for_a_full_probe() {
    let before = galera();
    assert!(full_probe_triggers(&before, &before).is_empty());
    let mut after = galera();
    facts_mut(&mut after[0]).galera.as_mut().unwrap().conf_id += 1;
    facts_mut(&mut after[1]).server.version_text = "11.8.10-MariaDB".into();
    facts_mut(&mut after[2]).server.server_id = 999;
    let t = full_probe_triggers(&before, &after);
    assert_eq!(t.len(), 3, "{t:?}");
    // A node that didn't answer triggers nothing.
    after[0] = failed("n1", None);
    assert_eq!(full_probe_triggers(&before, &after).len(), 2);
}
