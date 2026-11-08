//! Node states (chapter 06, "Health checks"; build step 7): Up, Suspect
//! and Down from active checks and from errors on real traffic, plus what
//! the checks and the position poller observed (role, Galera state, GTID
//! positions, estimated lag) and whether the node is behind: lagging more
//! than `reads.max_replica_lag_ms` (a replica) or `reads.max_galera_lag_ms`
//! (a Galera node), so it gets no reads until it catches up.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_common::ids::ClusterId;

use super::cluster::NodeKey;
use crate::reads::GtidSet;

/// A node's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NodeHealth {
    /// Usable.
    #[default]
    Up,
    /// A check or real traffic failed; still usable.
    Suspect,
    /// `backend.health_failures_to_down` failures: no new leases, idle
    /// connections closed, readers chosen elsewhere.
    Down,
}

impl NodeHealth {
    /// The log and metric name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            NodeHealth::Up => "up",
            NodeHealth::Suspect => "suspect",
            NodeHealth::Down => "down",
        }
    }
}

/// The replication role a check saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NodeRole {
    /// Not checked yet.
    #[default]
    Unknown,
    /// Writable and replicating from nobody.
    Writer,
    /// Replicates from another node, or is `read_only`.
    Replica,
    /// A Galera node.
    Galera,
}

/// What one check saw.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Observation {
    /// The role.
    pub role: NodeRole,
    /// `@@read_only`.
    pub read_only: bool,
    /// Galera: Synced (`wsrep_local_state` 4) and `wsrep_ready = ON`.
    pub galera_usable: Option<bool>,
}

/// Everything known about one node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeStatus {
    /// Up, Suspect or Down.
    pub health: NodeHealth,
    /// Consecutive failures.
    pub failures: u32,
    /// Consecutive passes.
    pub passes: u32,
    /// The last check's observation.
    pub observed: Option<Observation>,
    /// `@@gtid_slave_pos` at the last poll.
    pub slave_pos: Option<GtidSet>,
    /// `@@gtid_binlog_pos` at the last poll. On a Galera node this is what
    /// it has applied (`None` when its GTIDs aren't comparable across the
    /// cluster: `wsrep_gtid_mode` off).
    pub binlog_pos: Option<GtidSet>,
    /// Estimated lag at the last poll: for a replica, behind its writer;
    /// for a Galera node, behind the most advanced node of its cluster.
    pub lag: Option<Duration>,
    /// Lagging more than its threshold at the last poll (no reads).
    pub behind: bool,
}

impl NodeStatus {
    /// Whether the node may get new leases.
    #[must_use]
    pub fn usable(&self) -> bool {
        self.health != NodeHealth::Down
            && self
                .observed
                .as_ref()
                .and_then(|o| o.galera_usable)
                .unwrap_or(true)
    }

    /// The role seen, or `Unknown`.
    #[must_use]
    pub fn role(&self) -> NodeRole {
        self.observed.as_ref().map_or(NodeRole::Unknown, |o| o.role)
    }
}

/// How long a cluster's position history is kept for lag estimates.
const HISTORY: Duration = Duration::from_secs(120);

/// The state of every node this gateway uses.
#[derive(Debug, Default)]
pub struct NodeTable {
    nodes: Mutex<HashMap<NodeKey, NodeStatus>>,
    /// Per cluster, when its newest position first appeared: the
    /// primary's binlog position, or the most advanced Galera node's.
    head_history: Mutex<HashMap<ClusterId, VecDeque<(Instant, GtidSet)>>>,
}

impl NodeTable {
    /// An empty table: unknown nodes are Up.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn with<T>(&self, k: &NodeKey, f: impl FnOnce(&mut NodeStatus) -> T) -> T {
        let mut g = self.nodes.lock().unwrap_or_else(PoisonError::into_inner);
        f(g.entry(k.clone()).or_default())
    }

    /// A node's status.
    #[must_use]
    pub fn status(&self, k: &NodeKey) -> NodeStatus {
        self.nodes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(k)
            .cloned()
            .unwrap_or_default()
    }

    /// A passed check: Suspect and Down nodes return to Up after two passes
    /// in a row. Returns the new state if it changed.
    pub fn record_pass(&self, k: &NodeKey, obs: Observation) -> Option<NodeHealth> {
        self.with(k, |s| {
            s.failures = 0;
            s.passes = s.passes.saturating_add(1);
            s.observed = Some(obs);
            (s.health != NodeHealth::Up && s.passes >= 2).then(|| {
                s.health = NodeHealth::Up;
                NodeHealth::Up
            })
        })
    }

    /// A failed check, or a connect or I/O error on real traffic: Suspect,
    /// then Down at `to_down` failures in a row. Returns the new state if it
    /// changed.
    pub fn record_failure(&self, k: &NodeKey, to_down: u32) -> Option<NodeHealth> {
        self.with(k, |s| {
            s.passes = 0;
            s.failures = s.failures.saturating_add(1);
            let next = if s.failures >= to_down {
                NodeHealth::Down
            } else {
                NodeHealth::Suspect
            };
            (next != s.health && !(s.health == NodeHealth::Down && next == NodeHealth::Suspect))
                .then(|| {
                    s.health = next;
                    next
                })
        })
    }

    /// Records a position poll of a node. A writer's binlog position also
    /// goes into its cluster's history; a replica's lag is estimated as now
    /// minus the time the writer first had a commit the replica lacks.
    pub fn record_positions(
        &self,
        k: &NodeKey,
        slave: Option<GtidSet>,
        binlog: Option<GtidSet>,
        is_writer: bool,
    ) {
        let now = Instant::now();
        if is_writer && let Some(b) = &binlog {
            self.push_history(&k.cluster, b.clone(), now);
        }
        let lag = if is_writer {
            Some(Duration::ZERO)
        } else {
            slave
                .as_ref()
                .and_then(|p| self.estimate_lag(&k.cluster, p, now))
        };
        self.with(k, |s| {
            s.slave_pos = slave;
            s.binlog_pos = binlog;
            s.lag = lag;
        });
    }

    /// Records one poll round of a Galera cluster: each node's
    /// `@@gtid_binlog_pos`, and the cluster's `wsrep_gtid_domain_id` when
    /// `wsrep_gtid_mode` is on (`None` otherwise: positions then aren't
    /// comparable, so none is kept and causal reads go to the writer). The
    /// most advanced position in that domain goes into the cluster's
    /// history; a node's lag is now minus the time the cluster first had a
    /// commit the node lacks, as for replicas.
    pub fn record_galera_positions(
        &self,
        cluster: &ClusterId,
        polled: &[(NodeKey, Option<GtidSet>)],
        domain: Option<u32>,
    ) {
        let now = Instant::now();
        let Some(domain) = domain else {
            for (k, _) in polled {
                self.with(k, |s| {
                    s.slave_pos = None;
                    s.binlog_pos = None;
                    s.lag = None;
                });
            }
            return;
        };
        let mut head = GtidSet::new();
        for p in polled.iter().filter_map(|(_, p)| p.as_ref()) {
            head.union(&p.only(domain));
        }
        if !head.is_empty() {
            self.push_history(cluster, head, now);
        }
        for (k, pos) in polled {
            let lag = pos
                .as_ref()
                .and_then(|p| self.estimate_lag(cluster, &p.only(domain), now));
            self.with(k, |s| {
                s.slave_pos = None;
                s.binlog_pos.clone_from(pos);
                s.lag = lag;
            });
        }
    }

    /// Sets whether a node is behind; returns the new value if it changed.
    pub fn set_behind(&self, k: &NodeKey, behind: bool) -> Option<bool> {
        self.with(k, |s| {
            (s.behind != behind).then(|| {
                s.behind = behind;
                behind
            })
        })
    }

    /// Forgets a node's positions (its poll failed).
    pub fn clear_positions(&self, k: &NodeKey) {
        self.with(k, |s| {
            s.slave_pos = None;
            s.binlog_pos = None;
            s.lag = None;
        });
    }

    /// Adds a cluster's newest position to its history, keeping
    /// [`HISTORY`] of it.
    fn push_history(&self, cluster: &ClusterId, pos: GtidSet, now: Instant) {
        let mut h = self
            .head_history
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let q = h.entry(cluster.clone()).or_default();
        if q.back().is_none_or(|(_, last)| *last != pos) {
            q.push_back((now, pos));
        }
        while q
            .front()
            .is_some_and(|(t, _)| now.duration_since(*t) > HISTORY)
            && q.len() > 1
        {
            q.pop_front();
        }
    }

    fn estimate_lag(&self, cluster: &ClusterId, pos: &GtidSet, now: Instant) -> Option<Duration> {
        let h = self
            .head_history
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let q = h.get(cluster)?;
        let (_, latest) = q.back()?;
        if pos.covers(latest) {
            return Some(Duration::ZERO);
        }
        // The first writer position this replica hasn't applied.
        let first_missing = q.iter().find(|(_, w)| !pos.covers(w)).map(|(t, _)| *t)?;
        Some(now.duration_since(first_missing))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_common::ids::NodeId;

    fn key(n: &str) -> NodeKey {
        NodeKey {
            cluster: ClusterId::new("dc-a-c1").unwrap(),
            node: NodeId::new(n).unwrap(),
        }
    }

    #[test]
    fn up_suspect_down_and_back() {
        let t = NodeTable::new();
        let k = key("n1");
        assert_eq!(t.status(&k).health, NodeHealth::Up);
        assert_eq!(t.record_failure(&k, 3), Some(NodeHealth::Suspect));
        assert_eq!(t.record_failure(&k, 3), None);
        assert_eq!(t.record_failure(&k, 3), Some(NodeHealth::Down));
        assert!(!t.status(&k).usable());
        assert_eq!(t.record_failure(&k, 3), None);
        assert_eq!(t.record_pass(&k, Observation::default()), None);
        assert_eq!(
            t.record_pass(&k, Observation::default()),
            Some(NodeHealth::Up)
        );
        assert!(t.status(&k).usable());
        t.record_pass(
            &k,
            Observation {
                role: NodeRole::Galera,
                galera_usable: Some(false),
                ..Observation::default()
            },
        );
        assert!(
            !t.status(&k).usable(),
            "a desynced Galera node is not usable"
        );
    }

    #[test]
    #[allow(clippy::many_single_char_names)] // two nodes, a table, a cluster
    fn galera_lag_is_measured_against_the_most_advanced_node() {
        let t = NodeTable::new();
        let c = ClusterId::new("dc-a-c1").unwrap();
        let (a, b) = (key("a"), key("b"));
        let p = |s: &str| Some(GtidSet::parse(s).unwrap());
        // Each node's own domain (301, 302) never counts: only domain 3.
        t.record_galera_positions(
            &c,
            &[
                (a.clone(), p("3-301-10,301-301-4")),
                (b.clone(), p("3-301-10,302-302-9")),
            ],
            Some(3),
        );
        assert_eq!(t.status(&a).lag, Some(Duration::ZERO));
        assert_eq!(t.status(&b).lag, Some(Duration::ZERO));
        std::thread::sleep(Duration::from_millis(20));
        t.record_galera_positions(
            &c,
            &[(a.clone(), p("3-301-12")), (b.clone(), p("3-301-10"))],
            Some(3),
        );
        std::thread::sleep(Duration::from_millis(20));
        t.record_galera_positions(
            &c,
            &[(a.clone(), p("3-301-12")), (b.clone(), p("3-301-10"))],
            Some(3),
        );
        assert_eq!(t.status(&a).lag, Some(Duration::ZERO));
        let lag = t.status(&b).lag.unwrap();
        assert!(lag >= Duration::from_millis(20), "{lag:?}");
        assert_eq!(t.status(&b).binlog_pos, p("3-301-10"));
        assert_eq!(t.set_behind(&b, true), Some(true));
        assert_eq!(t.set_behind(&b, true), None);
        // Caught up.
        t.record_galera_positions(
            &c,
            &[(a.clone(), p("3-301-12")), (b.clone(), p("3-301-12"))],
            Some(3),
        );
        assert_eq!(t.status(&b).lag, Some(Duration::ZERO));
        // wsrep_gtid_mode off: nothing comparable is kept.
        t.record_galera_positions(&c, &[(b.clone(), p("3-301-13"))], None);
        assert_eq!(t.status(&b).binlog_pos, None);
        assert_eq!(t.status(&b).lag, None);
    }

    #[test]
    fn lag_is_the_age_of_the_oldest_missing_commit() {
        let t = NodeTable::new();
        let w = key("w");
        let r = key("r");
        let p = |s: &str| GtidSet::parse(s).unwrap();
        t.record_positions(&w, None, Some(p("2-1-10")), true);
        t.record_positions(&r, Some(p("2-1-10")), None, false);
        assert_eq!(t.status(&r).lag, Some(Duration::ZERO));
        std::thread::sleep(Duration::from_millis(20));
        t.record_positions(&w, None, Some(p("2-1-11")), true);
        std::thread::sleep(Duration::from_millis(20));
        t.record_positions(&r, Some(p("2-1-10")), None, false);
        let lag = t.status(&r).lag.unwrap();
        assert!(
            lag >= Duration::from_millis(20) && lag < Duration::from_secs(5),
            "{lag:?}"
        );
    }
}
