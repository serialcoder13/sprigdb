use std::time::Duration;

use super::*;

fn s() -> ClusterSettings {
    ClusterSettings {
        detect: Duration::from_secs(3),
        ..ClusterSettings::defaults()
    }
}

fn obs(probe: Probe, failing: u64, lease: LeaseView, agent: AgentView) -> Observation {
    Observation {
        probe,
        failing_for: Duration::from_secs(failing),
        lease,
        agent,
        since_last: None,
    }
}

const UP: AgentView = AgentView::Registered(MysqldState::Up);
const DOWN: AgentView = AgentView::Registered(MysqldState::Down);

#[test]
fn a_writer_that_answers_is_healthy() {
    let o = obs(Probe::Up { semisync: true }, 0, LeaseView::Writer, UP);
    assert_eq!(decide(&o, &s()), Step::Healthy);
}

#[test]
fn nothing_happens_inside_the_detect_window() {
    let o = obs(
        Probe::Refused,
        2,
        LeaseView::Gone(Duration::from_secs(1)),
        DOWN,
    );
    assert!(matches!(decide(&o, &s()), Step::Wait(w) if w.contains("cluster_detect_seconds")));
}

#[test]
fn a_killed_mysqld_fails_over_once_its_agent_hands_the_lease_back() {
    // Refused probes and the agent's "down": two sources, but the lease is
    // still held: the old writer isn't fenced yet.
    let held = obs(Probe::Refused, 4, LeaseView::Writer, DOWN);
    assert!(matches!(decide(&held, &s()), Step::Wait(w) if w.contains("still holds")));
    // Handed back, inside the drift margin.
    let fresh = obs(
        Probe::Refused,
        4,
        LeaseView::Gone(Duration::from_millis(100)),
        DOWN,
    );
    assert!(matches!(decide(&fresh, &s()), Step::Wait(w) if w.contains("drift")));
    // After it.
    let gone = obs(
        Probe::Refused,
        4,
        LeaseView::Gone(Duration::from_secs(1)),
        DOWN,
    );
    let Step::FailOver { sources } = decide(&gone, &s()) else {
        panic!("fails over")
    };
    assert!(sources.contains(&"probes".to_owned()) && sources.contains(&"agent".to_owned()));
}

#[test]
fn a_paused_writer_is_slow_not_down() {
    // Probes time out, its agent keeps the lease (its mysqld isn't
    // refusing): no failover however long it lasts.
    let o = obs(Probe::TimedOut, 60, LeaseView::Writer, UP);
    assert!(matches!(decide(&o, &s()), Step::Wait(w) if w.contains("slow")));
}

#[test]
fn a_lost_host_is_a_hard_failure() {
    // Agent and mysqld gone together: the lease ended with the agent's
    // session, probes only time out. Hard: proceeds on its own.
    let o = obs(
        Probe::TimedOut,
        12,
        LeaseView::Gone(Duration::from_secs(2)),
        AgentView::Missing,
    );
    assert!(
        matches!(decide(&o, &s()), Step::FailOver { sources } if sources.contains(&"hard".to_owned()))
    );
}

#[test]
fn a_partitioned_controller_alone_never_fails_over() {
    // Only this controller's probes fail; the agent sees mysqld up and
    // keeps the lease.
    let o = obs(Probe::Refused, 30, LeaseView::Writer, UP);
    assert!(matches!(decide(&o, &s()), Step::Wait(w) if w.contains("1 of 2")));
}

#[test]
fn the_interval_suppresses_all_but_hard_failures() {
    let mut o = obs(
        Probe::Refused,
        4,
        LeaseView::Gone(Duration::from_secs(1)),
        DOWN,
    );
    o.since_last = Some(Duration::from_secs(600));
    // Lease gone and mysqld down: hard.
    assert!(matches!(decide(&o, &s()), Step::FailOver { .. }));
    // An agent still registered and saying "up" whose lease vanished (its
    // node started replicating, say): not hard, suppressed.
    o.agent = UP;
    assert!(matches!(decide(&o, &s()), Step::Suppress(_)));
    o.since_last = Some(Duration::from_secs(31 * 60));
    assert!(matches!(decide(&o, &s()), Step::FailOver { .. }));
}

#[test]
fn manual_mode_and_a_foreign_lease_block() {
    let o = obs(
        Probe::Refused,
        4,
        LeaseView::Gone(Duration::from_secs(1)),
        DOWN,
    );
    let manual = ClusterSettings {
        manual: true,
        ..s()
    };
    assert!(matches!(decide(&o, &manual), Step::Blocked(w) if w.contains("manual")));
    let other = obs(Probe::Refused, 4, LeaseView::Other("db3".into()), DOWN);
    assert!(matches!(decide(&other, &s()), Step::Blocked(w) if w.contains("db3")));
}

#[test]
fn records_round_trip_and_keep_unknown_defaults() {
    let r = ClusterFailoverRecord {
        cluster_id: "fra1-c1".into(),
        state: "healthy".into(),
        writer: Some("db2".into()),
        writer_epoch: 2,
        ..ClusterFailoverRecord::default()
    };
    let enc = Record::new(r.clone()).encode().expect("encode");
    let back = Record::<ClusterFailoverRecord>::decode(&enc).expect("decode");
    assert_eq!(back.body, r);
}
