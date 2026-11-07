//! Fixtures for unit tests: probe captures from MariaDB 11.8 and 12.3,
//! taken from the Compose topologies with the `capture_fixtures` test.

use sprig_common::ids::NodeId;

use crate::probe::{ActiveProbes, FeatureFlags, NodeFacts, NodeProbe, ProbeDepth, RawCapture};
use crate::sql::ProbeError;

/// A captured node, by fixture name.
pub(crate) fn raw(name: &str) -> RawCapture {
    let text = match name {
        "11.8-single" => include_str!("../fixtures/11.8-single.json"),
        "11.8-primary" => include_str!("../fixtures/11.8-primary.json"),
        "11.8-replica" => include_str!("../fixtures/11.8-replica.json"),
        "11.8-galera" => include_str!("../fixtures/11.8-galera.json"),
        "12.3-single" => include_str!("../fixtures/12.3-single.json"),
        "12.3-read-only-no-lock" => include_str!("../fixtures/12.3-read-only-no-lock.json"),
        other => panic!("no fixture {other}"),
    };
    serde_json::from_str(text).expect("fixture parses")
}

pub(crate) fn node(id: &str) -> NodeId {
    NodeId::new(id).expect("node id")
}

/// A successful full probe of `raw`.
pub(crate) fn probe(id: &str, raw: &RawCapture) -> NodeProbe {
    NodeProbe {
        node: node(id),
        depth: ProbeDepth::Full,
        at_unix_ms: 0,
        took_ms: 1,
        result: Ok(NodeFacts::parse(raw)),
        active: None,
    }
}

/// A failed probe.
pub(crate) fn failed(id: &str, errno: Option<u16>) -> NodeProbe {
    NodeProbe {
        node: node(id),
        depth: ProbeDepth::Light,
        at_unix_ms: 0,
        took_ms: 2000,
        result: Err(ProbeError {
            errno,
            message: "no answer".into(),
            timeout: errno.is_none(),
        }),
        active: None,
    }
}

/// Active probes where everything passed.
pub(crate) fn all_passed() -> ActiveProbes {
    ActiveProbes {
        sys_schema: true,
        xa: true,
        xa_detail: None,
        features: FeatureFlags {
            ignored_indexes: true,
            instant_ignored_toggle: true,
            vector: true,
            online_spatial_index: false,
            alter_two_phase: false,
            performance_schema: false,
            json_schema_valid: true,
            json_table: true,
            vcol_index: true,
            vcol_index_online: true,
            oracle_routines: true,
            oracle_routines_any_mode: true,
        },
        sql_log_bin_settable: true,
        osu_method_settable: None,
        binlog_dump: None,
    }
}

/// `facts` of a probe, mutably.
pub(crate) fn facts_mut(p: &mut NodeProbe) -> &mut NodeFacts {
    p.result.as_mut().expect("successful probe")
}

/// The three-node replication topology: primary n1, replicas n2 and n3.
pub(crate) fn replication() -> Vec<NodeProbe> {
    let mut v = vec![
        probe("n1", &raw("11.8-primary")),
        probe("n2", &raw("11.8-replica")),
        probe("n3", &raw("11.8-replica")),
    ];
    v[0].active = Some(all_passed());
    v
}

/// A three-node Galera cluster built from one captured node.
pub(crate) fn galera() -> Vec<NodeProbe> {
    let g = raw("11.8-galera");
    let mut v = vec![probe("n1", &g), probe("n2", &g), probe("n3", &g)];
    let mut a = all_passed();
    a.xa = false;
    a.osu_method_settable = Some(true);
    v[0].active = Some(a);
    v
}

/// A single node with every active probe passed.
pub(crate) fn single() -> Vec<NodeProbe> {
    let mut v = vec![probe("n1", &raw("11.8-single"))];
    v[0].active = Some(all_passed());
    v
}
