use super::*;

fn node(id: &str, status: &str, state: &str, seq: u64) -> GaleraNodeStatus {
    GaleraNodeStatus {
        node: NodeId::new(id).expect("node"),
        reachable: true,
        cluster_status: status.into(),
        local_state: state.into(),
        ready: status == "Primary",
        last_committed: seq,
        confirmed_down: false,
    }
}

const WAIT: Duration = Duration::from_secs(30);

#[test]
fn a_primary_component_names_the_writers() {
    let mut q = GaleraQuorum::new();
    let t = Instant::now();
    let nodes = [
        node("g1", "Primary", "Synced", 10),
        node("g2", "Primary", "Synced", 10),
        node("g3", "non-Primary", "Initialized", 8),
    ];
    assert_eq!(
        q.observe(&nodes, t, WAIT, false),
        GaleraAction::Primary(vec![
            NodeId::new("g1").expect("node"),
            NodeId::new("g2").expect("node")
        ])
    );
    assert_eq!(q.highest_primary(), 10);
}

#[test]
fn lost_quorum_waits_then_bootstraps_only_the_most_advanced_confirmed_node() {
    let mut q = GaleraQuorum::new();
    let t0 = Instant::now();
    let all = [
        node("g1", "Primary", "Synced", 50),
        node("g2", "Primary", "Synced", 50),
        node("g3", "Primary", "Synced", 50),
    ];
    assert!(matches!(
        q.observe(&all, t0, WAIT, false),
        GaleraAction::Primary(_)
    ));
    let mut lost = [
        node("g1", "non-Primary", "Initialized", 50),
        GaleraNodeStatus::unreachable(NodeId::new("g2").expect("node")),
        GaleraNodeStatus::unreachable(NodeId::new("g3").expect("node")),
    ];
    let t1 = t0 + Duration::from_secs(5);
    assert!(matches!(
        q.observe(&lost, t1, WAIT, false),
        GaleraAction::WaitQuorum { .. }
    ));
    let t2 = t1 + WAIT;
    assert!(
        matches!(q.observe(&lost, t2, WAIT, false), GaleraAction::Blocked(w) if w.contains("not confirmed down")),
        "unreachable is not confirmed down"
    );
    lost[1].confirmed_down = true;
    lost[2].confirmed_down = true;
    assert!(
        matches!(q.observe(&lost, t2, WAIT, true), GaleraAction::Blocked(w) if w.contains("manual"))
    );
    assert_eq!(
        q.observe(&lost, t2, WAIT, false),
        GaleraAction::Bootstrap(NodeId::new("g1").expect("node"))
    );
}

#[test]
fn a_node_that_left_earlier_is_never_bootstrapped() {
    let mut q = GaleraQuorum::new();
    let t0 = Instant::now();
    let all = [
        node("g1", "Primary", "Synced", 70),
        node("g2", "Primary", "Synced", 70),
        node("g3", "non-Primary", "Initialized", 40),
    ];
    let _ = q.observe(&all, t0, WAIT, false);
    let mut lost = [
        GaleraNodeStatus::unreachable(NodeId::new("g1").expect("node")),
        GaleraNodeStatus::unreachable(NodeId::new("g2").expect("node")),
        node("g3", "non-Primary", "Initialized", 40),
    ];
    lost[0].confirmed_down = true;
    lost[1].confirmed_down = true;
    let _ = q.observe(&lost, t0, WAIT, false);
    let a = q.observe(&lost, t0 + WAIT, WAIT, false);
    assert!(
        matches!(&a, GaleraAction::Blocked(w) if w.contains("below 70")),
        "{a:?}"
    );
}
