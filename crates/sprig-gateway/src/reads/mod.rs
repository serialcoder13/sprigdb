//! Read routing and read-your-writes (chapter 06, "Read routing and
//! read-your-writes"; build step 6).
//!
//! A plain read may leave the writer. Its **causal target** is the
//! session's last commit ([`GtidSet`], from session tracking's `last_gtid`)
//! plus, under `reads.causal_scope = tenant_gateway`, the [`Watermarks`] of
//! every commit by the same tenant through this gateway (in M0.2, the same
//! cluster and backend account: decision D1). [`choose`] then picks:
//!
//! - **Galera** (decisions D13 and D14): like replicas. Nodes more than
//!   `reads.max_galera_lag_ms` behind the most advanced node (marked
//!   behind) get no reads. A read with an empty target goes to the
//!   least-loaded remaining node. One that must see a write goes to the
//!   least-loaded node whose polled position already covers the target
//!   (the shard's writer always qualifies), so it needs no wait. When no
//!   node but the writer is known to have it, `reads.galera_unconfirmed`
//!   decides: `check` (the default) picks the least-loaded node with a
//!   known lag under the limit (the least lag among equals, the writer
//!   counting as none); `writer` keeps the read on the writer. Off the
//!   writer, [`GALERA_SYNC_CHECK`] runs first, bounded by
//!   `reads.galera_sync_wait_timeout_ms`: Galera nodes apply within
//!   milliseconds, so it normally returns at once. A check that fails or
//!   times out reruns the statement on the writer. Galera's own
//!   causal-read timeout (30 s by default) is never reached.
//! - **Primary and replicas:** a healthy replica within
//!   `reads.max_replica_lag_ms` whose polled position covers the target, or
//!   else one expected to catch up within `reads.gtid_wait_timeout_ms`,
//!   read with the [`wait_prefix`] (`MASTER_GTID_WAIT`, failing with 1242 on
//!   timeout so the statement reruns on the writer); else the writer.
//! - **Single node, external:** the writer.
//!
//! Positions come from the backend monitor, which polls replicas'
//! `@@gtid_slave_pos`, and writers' and Galera nodes' `@@gtid_binlog_pos`,
//! every `reads.replica_position_poll_ms`.

pub mod gtid;
pub mod settings;

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

pub use gtid::{GtidParseError, GtidSet};
use sprig_cluster::profile::RywMethod;
use sprig_cluster::record::TopologyKind;
use sprig_common::ids::{ClusterId, NodeId};

use crate::backend::cluster::{ClusterView, NodeKey, NodeRef};
use crate::backend::health::{NodeHealth, NodeRole, NodeTable};
use crate::backend::settings::{BackendSettings, GaleraUnconfirmed, Routing};

/// Where a read goes (chapter 06, "Interfaces").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadTarget {
    /// The writer.
    Writer,
    /// A replica; `wait_for` is the target to wait for, `None` when its
    /// position already covers it.
    Replica {
        /// The replica.
        node: NodeRef,
        /// What to wait for with the wait-or-fail prefix.
        wait_for: Option<GtidSet>,
    },
    /// A Galera node: the writer, one whose position covers the target,
    /// or (decision D14) one not yet known to have it.
    GaleraNode {
        /// The node.
        node: NodeRef,
        /// Run [`GALERA_SYNC_CHECK`] first (a non-empty target off the
        /// writer).
        sync_wait: bool,
        /// The node is the writer, or its polled position covers the
        /// target (always, for an empty target). When `false`, only the
        /// check makes the read see the write.
        confirmed: bool,
    },
}

impl ReadTarget {
    /// The metric label.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            ReadTarget::Writer => "writer",
            ReadTarget::Replica { .. } => "replica",
            ReadTarget::GaleraNode {
                confirmed: false, ..
            } => "galera_unconfirmed",
            ReadTarget::GaleraNode { .. } => "galera",
        }
    }
}

/// Every commit by a tenant through this gateway, per cluster and backend
/// account (chapter 06: `reads.causal_scope = tenant_gateway`).
#[derive(Debug, Default)]
pub struct Watermarks {
    map: Mutex<HashMap<(ClusterId, String), GtidSet>>,
}

impl Watermarks {
    /// No commits yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a commit.
    pub fn advance(&self, cluster: &ClusterId, account: &str, gtid: &GtidSet) {
        self.map
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry((cluster.clone(), account.to_owned()))
            .or_default()
            .union(gtid);
    }

    /// The watermark of a tenant's account on a cluster.
    #[must_use]
    pub fn get(&self, cluster: &ClusterId, account: &str) -> GtidSet {
        self.map
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(cluster.clone(), account.to_owned()))
            .cloned()
            .unwrap_or_default()
    }
}

/// Picks the node for a plain read (see the module docs). `writer` is the
/// shard's writer (`None` when no node qualifies right now), `load` the
/// leased connections per node, `exclude` nodes that already failed this
/// statement, and `spread` a counter that rotates among equally loaded
/// Galera nodes so reads spread over the cluster.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn choose(
    view: &ClusterView,
    writer: Option<&NodeRef>,
    nodes: &NodeTable,
    load: &dyn Fn(&NodeKey) -> u32,
    target: &GtidSet,
    settings: &BackendSettings,
    exclude: &[NodeId],
    spread: u64,
) -> ReadTarget {
    if settings.routing == Routing::Writer {
        return ReadTarget::Writer;
    }
    match view.topology {
        TopologyKind::Galera => {
            choose_galera(view, writer, nodes, load, target, settings, exclude, spread)
        }
        TopologyKind::PrimaryReplica => {
            if view.ryw == RywMethod::WriterOnly && !target.is_empty() {
                return ReadTarget::Writer;
            }
            let mut cands: Vec<(&NodeRef, Option<GtidSet>, Duration)> = view
                .nodes
                .iter()
                .filter(|n| Some(&n.node) != writer.map(|w| &w.node) && !exclude.contains(&n.node))
                .filter_map(|n| {
                    let s = nodes.status(&view.key(&n.node));
                    let ok = s.usable()
                        && s.health == NodeHealth::Up
                        && matches!(s.role(), NodeRole::Replica)
                        && s.lag.is_some_and(|l| l <= settings.max_replica_lag);
                    ok.then(|| (n, s.slave_pos.clone(), s.lag.unwrap_or_default()))
                })
                .collect();
            cands.sort_by_key(|(n, _, _)| load(&view.key(&n.node)));
            if let Some((n, _, _)) = cands
                .iter()
                .find(|(_, pos, _)| pos.as_ref().is_some_and(|p| p.covers(target)))
            {
                return ReadTarget::Replica {
                    node: (*n).clone(),
                    wait_for: None,
                };
            }
            if view.ryw == RywMethod::GtidWait
                && let Some((n, _, _)) = cands
                    .iter()
                    .find(|(_, _, lag)| *lag < settings.gtid_wait_timeout)
            {
                return ReadTarget::Replica {
                    node: (*n).clone(),
                    wait_for: Some(target.clone()),
                };
            }
            ReadTarget::Writer
        }
        TopologyKind::SingleNode | TopologyKind::External => ReadTarget::Writer,
    }
}

/// The Galera rule of [`choose`] (decisions D13 and D14).
#[allow(clippy::too_many_arguments)]
fn choose_galera(
    view: &ClusterView,
    writer: Option<&NodeRef>,
    nodes: &NodeTable,
    load: &dyn Fn(&NodeKey) -> u32,
    target: &GtidSet,
    settings: &BackendSettings,
    exclude: &[NodeId],
    spread: u64,
) -> ReadTarget {
    let writer_id = writer.map(|w| &w.node);
    let causal = !target.is_empty();
    // Synced nodes that aren't behind: the node, whether it is the writer,
    // whether its polled position covers the target, and its lag.
    let fresh: Vec<(&NodeRef, bool, bool, Option<Duration>)> = view
        .nodes
        .iter()
        .filter(|n| !exclude.contains(&n.node))
        .filter_map(|n| {
            let s = nodes.status(&view.key(&n.node));
            let ok = s.usable()
                && s.health == NodeHealth::Up
                && !s.behind
                && s.lag.is_none_or(|l| l <= settings.max_galera_lag);
            let is_writer = Some(&n.node) == writer_id;
            let covers =
                !causal || is_writer || s.binlog_pos.as_ref().is_some_and(|p| p.covers(target));
            ok.then_some((n, is_writer, covers, s.lag))
        })
        .collect();
    let confirmed_elsewhere = fresh.iter().any(|&(_, w, c, _)| c && !w);
    let unconfirmed =
        causal && !confirmed_elsewhere && settings.galera_unconfirmed == GaleraUnconfirmed::Check;
    // Without a read off the writer known to have the write (decision D14),
    // any node with a known lag under the limit may serve it after the
    // check; among equally loaded nodes, the one with the least lag.
    let cands: Vec<(&NodeRef, bool, bool, Duration)> = fresh
        .into_iter()
        .filter_map(|(n, w, c, lag)| {
            let lag = if w { Some(Duration::ZERO) } else { lag };
            if unconfirmed {
                lag.map(|l| (n, w, c, l))
            } else {
                c.then_some((n, w, c, Duration::ZERO))
            }
        })
        .collect();
    let best_key = cands
        .iter()
        .map(|&(n, _, _, lag)| (load(&view.key(&n.node)), lag))
        .min();
    let ties: Vec<_> = cands
        .into_iter()
        .filter(|&(n, _, _, lag)| Some((load(&view.key(&n.node)), lag)) == best_key)
        .collect();
    if ties.is_empty() {
        return ReadTarget::Writer;
    }
    let (n, is_writer, covers, _) = ties[usize::try_from(spread).unwrap_or(0) % ties.len()];
    ReadTarget::GaleraNode {
        sync_wait: causal && !is_writer,
        confirmed: covers,
        node: n.clone(),
    }
}

/// `MASTER_GTID_WAIT`'s timeout argument, in seconds.
fn wait_seconds(timeout: Duration) -> String {
    format!("{:.3}", timeout.as_secs_f64())
}

/// The wait-or-fail prefix for text statements: it waits for `target` and,
/// on timeout, fails with 1242 before the client's statement runs.
#[must_use]
pub fn wait_prefix(target: &GtidSet, timeout: Duration) -> String {
    format!(
        "SET @__sprig_w = (SELECT CASE WHEN MASTER_GTID_WAIT('{target}', {}) = 0 \
         THEN 1 ELSE (SELECT 1 FROM information_schema.ENGINES) END)",
        wait_seconds(timeout)
    )
}

/// The separate round trip prepared statements wait with: returns 0 once
/// applied, -1 on timeout.
#[must_use]
pub fn wait_query(target: &GtidSet, timeout: Duration) -> String {
    format!(
        "SELECT MASTER_GTID_WAIT('{target}', {})",
        wait_seconds(timeout)
    )
}

/// The error MariaDB raises when the wait-or-fail prefix times out.
pub const WAIT_TIMEOUT_ERRNO: u16 = 1242;

/// The Galera safety net (decision D13): a statement that waits until the
/// node has applied everything the cluster had committed when it started
/// (`wsrep_sync_wait = 1`), so a read that follows it sees the session's
/// writes. It runs before the client's statement, which keeps
/// `wsrep_sync_wait = 0`. MariaDB's `max_statement_time` doesn't end this
/// wait, so the gateway bounds it itself with
/// `reads.galera_sync_wait_timeout_ms` and drops the connection on timeout.
pub const GALERA_SYNC_CHECK: &str = "SET STATEMENT wsrep_sync_wait = 1 FOR SELECT 1";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::health::Observation;

    fn view(topology: TopologyKind) -> ClusterView {
        let nodes = ["n1", "n2", "n3"]
            .iter()
            .map(|n| NodeRef {
                node: NodeId::new(*n).unwrap(),
                addr: format!("{n}:3306"),
            })
            .collect();
        ClusterView::new(ClusterId::new("dc-a-c1").unwrap(), topology, nodes)
    }

    fn replica(t: &NodeTable, v: &ClusterView, n: &str, pos: &str) {
        let k = v.key(&NodeId::new(n).unwrap());
        t.record_pass(
            &k,
            Observation {
                role: NodeRole::Replica,
                ..Observation::default()
            },
        );
        t.record_positions(&k, Some(GtidSet::parse(pos).unwrap()), None, false);
    }

    #[test]
    fn primary_replica_routing() {
        let v = view(TopologyKind::PrimaryReplica);
        let t = NodeTable::new();
        let w = v.nodes[0].clone();
        let s = BackendSettings::default();
        let none = |_: &NodeKey| 0;
        // No replica observed yet: the writer.
        assert_eq!(
            choose(&v, Some(&w), &t, &none, &GtidSet::new(), &s, &[], 0),
            ReadTarget::Writer
        );
        // The writer's history makes lag 0 for caught-up replicas.
        t.record_positions(
            &v.key(&w.node),
            None,
            Some(GtidSet::parse("2-1-10").unwrap()),
            true,
        );
        replica(&t, &v, "n2", "2-1-10");
        replica(&t, &v, "n3", "2-1-9");
        let target = GtidSet::parse("2-1-10").unwrap();
        match choose(&v, Some(&w), &t, &none, &target, &s, &[], 0) {
            ReadTarget::Replica { node, wait_for } => {
                assert_eq!(node.node.as_str(), "n2");
                assert_eq!(wait_for, None);
            }
            other => panic!("{other:?}"),
        }
        // n2 excluded: n3 lags a little, so wait on it.
        let n2 = NodeId::new("n2").unwrap();
        match choose(&v, Some(&w), &t, &none, &target, &s, &[n2], 0) {
            ReadTarget::Replica { node, wait_for } => {
                assert_eq!(node.node.as_str(), "n3");
                assert_eq!(wait_for, Some(target.clone()));
            }
            other => panic!("{other:?}"),
        }
        let writer_only = BackendSettings {
            routing: Routing::Writer,
            ..BackendSettings::default()
        };
        assert_eq!(
            choose(&v, Some(&w), &t, &none, &target, &writer_only, &[], 0),
            ReadTarget::Writer
        );
        // A Down replica isn't chosen.
        let k2 = v.key(&NodeId::new("n2").unwrap());
        let k3 = v.key(&NodeId::new("n3").unwrap());
        for _ in 0..3 {
            t.record_failure(&k2, 3);
            t.record_failure(&k3, 3);
        }
        assert_eq!(
            choose(&v, Some(&w), &t, &none, &target, &s, &[], 0),
            ReadTarget::Writer
        );
    }

    fn galera_node(t: &NodeTable, v: &ClusterView, n: &str) {
        let k = v.key(&NodeId::new(n).unwrap());
        t.record_pass(
            &k,
            Observation {
                role: NodeRole::Galera,
                galera_usable: Some(true),
                ..Observation::default()
            },
        );
    }

    fn galera_round(t: &NodeTable, v: &ClusterView, pos: [&str; 3]) {
        let polled: Vec<_> = ["n1", "n2", "n3"]
            .iter()
            .zip(pos)
            .map(|(n, p)| {
                (
                    v.key(&NodeId::new(*n).unwrap()),
                    Some(GtidSet::parse(p).unwrap()),
                )
            })
            .collect();
        t.record_galera_positions(&v.id, &polled, Some(3));
    }

    /// Decision D13, kept by `reads.galera_unconfirmed = writer`.
    #[test]
    fn galera_routing_and_sync_wait() {
        let v = view(TopologyKind::Galera);
        let t = NodeTable::new();
        for n in ["n1", "n2", "n3"] {
            galera_node(&t, &v, n);
        }
        let w = v.nodes[1].clone();
        let s = BackendSettings {
            galera_unconfirmed: GaleraUnconfirmed::Writer,
            ..BackendSettings::default()
        };
        let load = |k: &NodeKey| if k.node.as_str() == "n1" { 5 } else { 0 };
        // No target: the least-loaded node, no check.
        match choose(&v, Some(&w), &t, &load, &GtidSet::new(), &s, &[], 0) {
            ReadTarget::GaleraNode {
                node, sync_wait, ..
            } => {
                assert_eq!(node.node.as_str(), "n2", "least loaded");
                assert!(!sync_wait);
            }
            other => panic!("{other:?}"),
        }
        // A target no polled position covers yet: the writer.
        let target = GtidSet::parse("3-1-5").unwrap();
        let busy_writer = |k: &NodeKey| if k.node.as_str() == "n3" { 0 } else { 5 };
        match choose(&v, Some(&w), &t, &busy_writer, &target, &s, &[], 0) {
            ReadTarget::GaleraNode {
                node, sync_wait, ..
            } => {
                assert_eq!(node.node.as_str(), "n2", "only the writer has it");
                assert!(!sync_wait, "no check on the writer");
            }
            other => panic!("{other:?}"),
        }
        // n3 has applied it, n1 hasn't: n3, with the safety-net check.
        galera_round(&t, &v, ["3-1-4", "3-1-5", "3-1-5"]);
        match choose(&v, Some(&w), &t, &busy_writer, &target, &s, &[], 0) {
            ReadTarget::GaleraNode {
                node, sync_wait, ..
            } => {
                assert_eq!(node.node.as_str(), "n3");
                assert!(sync_wait, "off the writer, target set");
            }
            other => panic!("{other:?}"),
        }
        let only_n1 = |k: &NodeKey| if k.node.as_str() == "n1" { 0 } else { 5 };
        match choose(&v, Some(&w), &t, &only_n1, &target, &s, &[], 0) {
            ReadTarget::GaleraNode { node, .. } => {
                assert_ne!(node.node.as_str(), "n1", "n1 lacks the write");
            }
            other => panic!("{other:?}"),
        }
        // Without a writer and no node covering the target: the writer
        // fallback (the caller waits for one).
        assert_eq!(
            choose(
                &v,
                None,
                &t,
                &load,
                &GtidSet::parse("3-1-9").unwrap(),
                &s,
                &[],
                0
            ),
            ReadTarget::Writer
        );
        // A node far behind gets no reads at all, causal or not.
        let tight = BackendSettings {
            max_galera_lag: Duration::from_millis(10),
            ..BackendSettings::default()
        };
        std::thread::sleep(Duration::from_millis(30));
        galera_round(&t, &v, ["3-1-4", "3-1-5", "3-1-5"]);
        for _ in 0..6 {
            match choose(&v, Some(&w), &t, &only_n1, &GtidSet::new(), &tight, &[], 0) {
                ReadTarget::GaleraNode { node, .. } => assert_ne!(node.node.as_str(), "n1"),
                other => panic!("{other:?}"),
            }
        }
        // Caught up: it is a candidate again.
        galera_round(&t, &v, ["3-1-5", "3-1-5", "3-1-5"]);
        match choose(&v, Some(&w), &t, &only_n1, &target, &tight, &[], 0) {
            ReadTarget::GaleraNode {
                node, sync_wait, ..
            } => {
                assert_eq!(node.node.as_str(), "n1");
                assert!(sync_wait);
            }
            other => panic!("{other:?}"),
        }
    }

    fn pick(
        v: &ClusterView,
        w: Option<&NodeRef>,
        t: &NodeTable,
        load: &dyn Fn(&NodeKey) -> u32,
        target: &str,
        s: &BackendSettings,
        spread: u64,
    ) -> (String, bool, bool) {
        let target = GtidSet::parse(target).unwrap();
        match choose(v, w, t, load, &target, s, &[], spread) {
            ReadTarget::GaleraNode {
                node,
                sync_wait,
                confirmed,
            } => (node.node.as_str().to_owned(), sync_wait, confirmed),
            ReadTarget::Writer => ("writer".to_owned(), false, true),
            other @ ReadTarget::Replica { .. } => panic!("{other:?}"),
        }
    }

    /// Decision D14: a read that must see a write no polled position shows
    /// off the writer, in both modes of `reads.galera_unconfirmed`.
    #[test]
    fn galera_unconfirmed_reads_in_both_modes() {
        let v = view(TopologyKind::Galera);
        let t = NodeTable::new();
        for n in ["n1", "n2", "n3"] {
            galera_node(&t, &v, n);
        }
        let w = v.nodes[1].clone();
        let check = BackendSettings::default();
        assert_eq!(check.galera_unconfirmed, GaleraUnconfirmed::Check);
        let strict = BackendSettings {
            galera_unconfirmed: GaleraUnconfirmed::Writer,
            ..BackendSettings::default()
        };
        let busy_writer = |k: &NodeKey| u32::from(k.node.as_str() == "n2") * 5;
        let idle = |_: &NodeKey| 0;

        // Before any poll no node's lag is known: only the writer, in
        // both modes.
        for s in [&check, &strict] {
            assert_eq!(
                pick(&v, Some(&w), &t, &busy_writer, "3-1-5", s, 0),
                ("n2".to_owned(), false, true)
            );
        }

        // The writer has 3-1-5; n1 and n3 don't show it yet (lag 0).
        galera_round(&t, &v, ["3-1-4", "3-1-5", "3-1-4"]);
        let mut seen = std::collections::BTreeSet::new();
        for spread in 0..4 {
            let (n, sync_wait, confirmed) =
                pick(&v, Some(&w), &t, &busy_writer, "3-1-5", &check, spread);
            assert!(sync_wait && !confirmed, "{n}: the bounded check first");
            seen.insert(n);
            // Strict: the writer, no check.
            assert_eq!(
                pick(&v, Some(&w), &t, &busy_writer, "3-1-5", &strict, spread),
                ("n2".to_owned(), false, true)
            );
        }
        assert_eq!(seen, ["n1", "n3"].map(str::to_owned).into(), "spread");
        // Equally loaded, the writer is one of the choices (no check there).
        let mut writer_too = false;
        for spread in 0..3 {
            let (n, sync_wait, confirmed) = pick(&v, Some(&w), &t, &idle, "3-1-5", &check, spread);
            assert_eq!(sync_wait, n != "n2");
            assert_eq!(confirmed, n == "n2");
            writer_too |= n == "n2";
        }
        assert!(writer_too);
        // An empty target needs no check, in either mode.
        for s in [&check, &strict] {
            let (_, sync_wait, confirmed) = pick(&v, Some(&w), &t, &busy_writer, "", s, 0);
            assert!(!sync_wait && confirmed);
        }

        // n1 has missed commits for a while, n3 only the newest one: among
        // equally loaded nodes, the least lag wins.
        std::thread::sleep(Duration::from_millis(30));
        galera_round(&t, &v, ["3-1-4", "3-1-6", "3-1-5"]);
        for spread in 0..4 {
            assert_eq!(
                pick(&v, Some(&w), &t, &busy_writer, "3-1-6", &check, spread),
                ("n3".to_owned(), true, false)
            );
        }
        // Load comes first: n3 busy, n1 (lagging, but under the limit).
        let n3_busy = |k: &NodeKey| u32::from(k.node.as_str() != "n1") * 5;
        assert_eq!(
            pick(&v, Some(&w), &t, &n3_busy, "3-1-6", &check, 0),
            ("n1".to_owned(), true, false)
        );
        // A node that already shows the write is preferred, with the check
        // as a safety net; n1, which lacks it, is never chosen.
        for spread in 0..4 {
            let (n, _, confirmed) = pick(&v, Some(&w), &t, &n3_busy, "3-1-5", &check, spread);
            assert!(confirmed && n != "n1", "{n}");
        }

        // A node marked behind is never chosen, even when idle.
        let k1 = v.key(&NodeId::new("n1").unwrap());
        let k3 = v.key(&NodeId::new("n3").unwrap());
        t.set_behind(&k3, true);
        let only_n3_idle = |k: &NodeKey| u32::from(k.node.as_str() != "n3") * 5;
        for spread in 0..4 {
            let (n, _, _) = pick(&v, Some(&w), &t, &only_n3_idle, "3-1-6", &check, spread);
            assert_ne!(n, "n3");
            let (n, _, _) = pick(&v, Some(&w), &t, &only_n3_idle, "", &check, spread);
            assert_ne!(n, "n3");
        }
        // Over reads.max_galera_lag_ms counts as behind too: n1 is out, so
        // only the writer is left.
        let tight = BackendSettings {
            max_galera_lag: Duration::from_millis(10),
            ..check.clone()
        };
        assert_eq!(
            pick(&v, Some(&w), &t, &idle, "3-1-6", &tight, 0),
            ("n2".to_owned(), false, true)
        );
        // An unknown lag (the poll failed) isn't "not behind".
        t.set_behind(&k3, false);
        t.clear_positions(&k1);
        t.clear_positions(&k3);
        assert_eq!(
            pick(&v, Some(&w), &t, &busy_writer, "3-1-6", &check, 0),
            ("n2".to_owned(), false, true)
        );

        // No writer right now: check mode still serves the read from a
        // node that isn't behind; strict mode waits for the writer.
        galera_round(&t, &v, ["3-1-6", "3-1-6", "3-1-6"]);
        assert_eq!(pick(&v, None, &t, &idle, "3-1-7", &strict, 0).0, "writer");
        let (n, sync_wait, confirmed) = pick(&v, None, &t, &idle, "3-1-7", &check, 0);
        assert!(n.starts_with('n') && sync_wait && !confirmed, "{n}");
    }

    #[test]
    fn prefixes() {
        let g = GtidSet::parse("0-11-5402,1-12-77").unwrap();
        assert_eq!(
            wait_prefix(&g, Duration::from_millis(100)),
            "SET @__sprig_w = (SELECT CASE WHEN MASTER_GTID_WAIT('0-11-5402,1-12-77', 0.100) = 0 \
             THEN 1 ELSE (SELECT 1 FROM information_schema.ENGINES) END)"
        );
        assert_eq!(
            wait_query(&g, Duration::from_millis(2500)),
            "SELECT MASTER_GTID_WAIT('0-11-5402,1-12-77', 2.500)"
        );
        let w = Watermarks::new();
        let c = ClusterId::new("dc-a-c1").unwrap();
        w.advance(&c, "app", &g);
        w.advance(&c, "app", &GtidSet::parse("0-11-5403").unwrap());
        assert_eq!(w.get(&c, "app").to_string(), "0-11-5403,1-12-77");
        assert!(w.get(&c, "other").is_empty());
    }
}
