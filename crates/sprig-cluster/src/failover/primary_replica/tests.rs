use super::*;

fn st(node: &str, pos: &str, lag: Option<u64>) -> ReplicaState {
    ReplicaState {
        node: NodeId::new(node).expect("node"),
        slave_pos: pos.into(),
        received: pos.into(),
        lag_seconds: lag,
        drained: true,
        stuck: None,
    }
}

/// Received `received`, applied `applied`; `stuck` when not drained.
fn lagging(node: &str, received: &str, applied: &str, stuck: Option<&str>) -> ReplicaState {
    ReplicaState {
        node: NodeId::new(node).expect("node"),
        slave_pos: applied.into(),
        received: received.into(),
        lag_seconds: Some(0),
        drained: stuck.is_none(),
        stuck: stuck.map(Into::into),
    }
}

#[test]
fn the_most_advanced_replica_is_chosen() {
    let states = [
        st("r2", "1-101-90", Some(0)),
        st("r3", "1-101-95", Some(0)),
        st("r4", "1-101-93", Some(1)),
    ];
    let Choice::Promote(c) = choose(&states, 1) else {
        panic!("blocked");
    };
    assert_eq!(c.node.as_str(), "r3");
    assert_eq!(c.applied, "1-101-95");
    // Containment in every domain: r3 lacks r4's domain-0 transactions.
    let states = [st("r3", "1-101-95", None), st("r4", "0-1-5,1-101-93", None)];
    assert!(
        matches!(choose(&states, 1), Choice::Blocked(w) if w.contains("no replica holds")),
        "neither holds every other's"
    );
}

#[test]
fn the_choice_is_by_received_not_applied() {
    // r2 applied more, but r3 received (acknowledged) more and applied it
    // all in the drain: r3. Before the relay-log safe drain the applied
    // position chose r2 and lost 1-101-96..100.
    let states = [
        st("r2", "1-101-95", Some(0)),
        lagging("r3", "1-101-100", "1-101-100", None),
    ];
    let Choice::Promote(c) = choose(&states, 1) else {
        panic!("blocked");
    };
    assert_eq!(c.node.as_str(), "r3");
    // r3 couldn't apply it (both threads stopped): promoting it would
    // discard its relay log, promoting r2 would lose what only r3 has.
    let states = [
        st("r2", "1-101-95", Some(0)),
        lagging(
            "r3",
            "1-101-100",
            "1-101-80",
            Some("both replication threads are stopped"),
        ),
    ];
    let Choice::Blocked(why) = choose(&states, 1) else {
        panic!("promoted");
    };
    assert!(
        why.contains("r3 holds the most") && why.contains("both"),
        "{why}"
    );
    // Equal received positions: the drained one, not the stuck one.
    let states = [
        lagging("r2", "1-101-100", "1-101-80", Some("stuck")),
        st("r3", "1-101-100", Some(0)),
    ];
    let Choice::Promote(c) = choose(&states, 1) else {
        panic!("blocked");
    };
    assert_eq!(c.node.as_str(), "r3");
    // A replica that never connected falls back to its applied position.
    let states = [
        lagging("r2", "", "1-101-7", None),
        st("r3", "1-101-5", None),
    ];
    let Choice::Promote(c) = choose(&states, 1) else {
        panic!("blocked");
    };
    assert_eq!(c.node.as_str(), "r2");
}

#[test]
fn equal_sequence_numbers_from_different_servers_diverge() {
    let states = [st("r2", "1-101-95", None), st("r3", "1-102-95", None)];
    assert!(
        matches!(choose(&states, 1), Choice::Blocked(w) if w.contains("diverge") || w.contains("no replica holds"))
    );
    assert!(matches!(choose(&[], 1), Choice::Blocked(_)));
}

#[test]
fn loss_comes_from_semi_sync_or_the_lag() {
    assert_eq!(expected_loss(true, Some(30)), Some(Duration::from_secs(1)));
    assert_eq!(expected_loss(false, Some(3)), Some(Duration::from_secs(3)));
    assert_eq!(expected_loss(false, None), None);
    let rpo = Duration::from_secs(5);
    assert!(check_rpo(Some(Duration::from_secs(1)), rpo, false).is_ok());
    assert!(check_rpo(Some(Duration::from_secs(9)), rpo, false).is_err());
    assert!(check_rpo(None, rpo, false).is_err(), "unknown loss stops");
    assert!(
        check_rpo(Some(Duration::ZERO), rpo, true).is_err(),
        "manual stops"
    );
}

#[test]
fn diverged_transactions_are_past_the_applied_position() {
    let d = diverged("1-101-120", "1-101-117", 1).expect("diverged");
    assert_eq!(d.count(), 3);
    assert_eq!((d.server, d.after_seq, d.through_seq), (101, 117, 120));
    assert!(d.contains(&"1-101-118".parse().expect("gtid")));
    assert!(!d.contains(&"1-101-117".parse().expect("gtid")));
    assert!(!d.contains(&"1-102-118".parse().expect("gtid")));
    assert!(diverged("1-101-117", "1-101-117", 1).is_none());
    assert!(
        diverged("1-101-110", "1-101-117", 1).is_none(),
        "behind, not diverged"
    );
    assert!(diverged("", "1-101-117", 1).is_none());
}

#[test]
fn binlog_event_infos_parse() {
    assert_eq!(
        gtid_of("BEGIN GTID 1-101-7").map(|g| g.to_string()),
        Some("1-101-7".into())
    );
    assert_eq!(gtid_of("GTID 1-101-8").map(|g| g.seq), Some(8));
    assert_eq!(gtid_of("COMMIT"), None);
    assert_eq!(
        table_of("table_id: 33 (r2_db.items)"),
        Some("r2_db.items".into())
    );
    assert_eq!(table_of("nothing"), None);
}

#[test]
fn repointing_keeps_tls_verified_against_the_nodes_ca() {
    assert_eq!(
        ssl_clause(Some("/run/sprig/tls/ca.pem")),
        ", MASTER_SSL = 1, MASTER_SSL_CA = '/run/sprig/tls/ca.pem'"
    );
    assert_eq!(ssl_clause(Some("")), "");
    assert_eq!(ssl_clause(None), "");
}
