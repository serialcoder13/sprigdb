//! The writer of clusters someone else fails over (r2opfix): Kubernetes
//! clusters (mariadb-operator, with or without SprigDB choosing the
//! replica) and external clusters that declare `failover = provider`.
//!
//! Gateways follow only the writer the controller publishes
//! (`cluster-failover/<cluster>`, read by `sprig-gateway`'s
//! `read_published`). The `cluster_failover` loop never wrote it for these
//! clusters, so after mariadb-operator promoted a replica, gateways kept
//! the registered first node and refused every write. Now the DC leader
//! observes the writer and publishes a change, with a new `writer_epoch`
//! (gateways drop the old writer at once), only when all of these hold:
//!
//! ```text
//! 1. exactly one node is writable: read_only OFF and replicating from nobody
//!    (two writable nodes: publish nothing, state "diverged", alert)
//! 2. the published writer is unreachable, or read_only and replicating from
//!    the new one
//! 3. the same observation has held for failover.observe_settle_ms (2000)
//! ```
//!
//! The record says where the writer came from: `source` is `observed:
//! mariadb-operator` (Kubernetes) or `observed: provider` (external),
//! versus `sprig failover` for SprigDB's own.

use std::time::Duration;

use sprig_cluster::record::{ClusterRecord, DeclaredFailover, ProvisionerKind, TopologyKind};
use sprig_cluster::sql::{NodeEndpoint, ProbeSession, TextRow};

/// One probe's timeout.
pub const PROBE_TIMEOUT: Duration = Duration::from_millis(1500);

/// One node as the observation saw it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NodeObs {
    /// The record's node id.
    pub node: String,
    /// It answered.
    pub reachable: bool,
    /// `@@read_only`.
    pub read_only: bool,
    /// `Master_Host` when it is configured as a replica (whatever its
    /// threads' state); `None` when it replicates from nobody.
    pub replicating_from: Option<String>,
    /// `@@hostname` (a pod's name on Kubernetes).
    pub hostname: String,
    /// The host part of the record's address.
    pub addr_host: String,
}

impl NodeObs {
    /// Writable: answers, `read_only` off, replicating from nobody.
    #[must_use]
    pub fn writable(&self) -> bool {
        self.reachable && !self.read_only && self.replicating_from.is_none()
    }

    /// Whether a replica's `Master_Host` names this node: its host name,
    /// a fully qualified name starting with it, or its address's host.
    #[must_use]
    pub fn is_named_by(&self, master_host: &str) -> bool {
        let m = master_host.trim().to_ascii_lowercase();
        let h = self.hostname.to_ascii_lowercase();
        (!h.is_empty() && (m == h || m.starts_with(&format!("{h}."))))
            || (!self.addr_host.is_empty() && m == self.addr_host.to_ascii_lowercase())
    }
}

/// What the observation decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observed {
    /// The published writer is still the writer.
    Keep(String),
    /// A different node is the writer (or the first one seen); it is
    /// published once it has held for the settle time.
    Candidate(String),
    /// No node may be published; the reason.
    NoWriter(String),
    /// More than one node is writable: publish nothing, alert.
    Diverged(Vec<String>),
}

/// The rules in the module docs (1 and 2; the settle time is the
/// caller's).
#[must_use]
pub fn decide(published: Option<&str>, obs: &[NodeObs]) -> Observed {
    let writable: Vec<&NodeObs> = obs.iter().filter(|o| o.writable()).collect();
    match writable.as_slice() {
        [] => Observed::NoWriter(format!(
            "no node is writable and replicating from nobody ({} of {} answered)",
            obs.iter().filter(|o| o.reachable).count(),
            obs.len()
        )),
        [x] => {
            if published == Some(x.node.as_str()) {
                return Observed::Keep(x.node.clone());
            }
            let Some(old) = published.and_then(|p| obs.iter().find(|o| o.node == p)) else {
                // Nothing published yet (or the old writer left the record).
                return Observed::Candidate(x.node.clone());
            };
            if !old.reachable {
                return Observed::Candidate(x.node.clone());
            }
            match &old.replicating_from {
                Some(m) if old.read_only && x.is_named_by(m) => Observed::Candidate(x.node.clone()),
                Some(m) => Observed::NoWriter(format!(
                    "{} answers, read_only {}, replicating from {m}, not from {}",
                    old.node, old.read_only, x.node
                )),
                None => Observed::NoWriter(format!(
                    "{} answers and replicates from nobody (read_only {}); {} isn't published yet",
                    old.node, old.read_only, x.node
                )),
            }
        }
        many => Observed::Diverged(many.iter().map(|o| o.node.clone()).collect()),
    }
}

/// Whether the cluster's writer is observed rather than chosen by
/// SprigDB: Kubernetes clusters and external ones with provider failover,
/// primary + replicas.
#[must_use]
pub fn observed_source(rec: &ClusterRecord) -> Option<&'static str> {
    if rec.topology != TopologyKind::PrimaryReplica {
        return None;
    }
    match rec.provisioner {
        ProvisionerKind::Kubernetes => Some("observed: mariadb-operator"),
        ProvisionerKind::External
            if rec
                .declared
                .as_ref()
                .and_then(|d| d.failover)
                .is_some_and(|f| f == DeclaredFailover::Provider) =>
        {
            Some("observed: provider")
        }
        _ => None,
    }
}

fn col<'a>(row: &'a TextRow, name: &str) -> Option<&'a str> {
    row.iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(name))
        .and_then(|(_, v)| v.as_deref())
}

/// Observes one node as `sprig_ctl`.
pub async fn observe(ep: &NodeEndpoint) -> NodeObs {
    let mut o = NodeObs {
        node: ep.node.to_string(),
        addr_host: ep.opts.ip_or_hostname().to_owned(),
        ..NodeObs::default()
    };
    let Ok(mut s) = ProbeSession::connect(ep, PROBE_TIMEOUT).await else {
        return o;
    };
    let vars = s
        .rows("SELECT @@GLOBAL.read_only AS ro, @@hostname AS h")
        .await;
    let slave = s.rows("SHOW SLAVE STATUS").await;
    s.close().await;
    let (Ok(vars), Ok(slave)) = (vars, slave) else {
        return o;
    };
    let Some(v) = vars.first() else { return o };
    o.reachable = true;
    o.read_only = col(v, "ro").is_some_and(|r| r != "0" && !r.eq_ignore_ascii_case("off"));
    col(v, "h").unwrap_or_default().clone_into(&mut o.hostname);
    o.replicating_from = slave
        .first()
        .and_then(|r| col(r, "Master_Host"))
        .filter(|m| !m.is_empty())
        .map(str::to_owned);
    o
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(node: &str, host: &str, ro: bool, from: Option<&str>) -> NodeObs {
        NodeObs {
            node: node.into(),
            reachable: true,
            read_only: ro,
            replicating_from: from.map(str::to_owned),
            hostname: host.into(),
            addr_host: "127.0.0.1".into(),
        }
    }

    #[test]
    fn two_writable_nodes_publish_nothing() {
        let obs = [
            n("n0", "db-0", false, None),
            n(
                "n1",
                "db-1",
                true,
                Some("db-0.db-internal.ns.svc.cluster.local"),
            ),
            n("n2", "db-2", false, None),
        ];
        assert_eq!(
            decide(Some("n0"), &obs),
            Observed::Diverged(vec!["n0".into(), "n2".into()])
        );
        assert_eq!(
            decide(None, &obs),
            Observed::Diverged(vec!["n0".into(), "n2".into()])
        );
    }

    #[test]
    fn a_promoted_replica_is_published_when_the_old_writer_follows_it_or_is_gone() {
        // mariadb-operator promoted pod 2; pod 0 is down.
        let mut down = n("n0", "db-0", false, None);
        down.reachable = false;
        let obs = [
            down.clone(),
            n(
                "n1",
                "db-1",
                true,
                Some("db-2.db-internal.ns.svc.cluster.local"),
            ),
            n("n2", "db-2", false, None),
        ];
        assert_eq!(decide(Some("n0"), &obs), Observed::Candidate("n2".into()));
        // Pod 0 came back as a replica of pod 2.
        let obs = [
            n(
                "n0",
                "db-0",
                true,
                Some("db-2.db-internal.ns.svc.cluster.local"),
            ),
            obs[1].clone(),
            obs[2].clone(),
        ];
        assert_eq!(decide(Some("n0"), &obs), Observed::Candidate("n2".into()));
        // Published already.
        assert_eq!(decide(Some("n2"), &obs), Observed::Keep("n2".into()));
    }

    #[test]
    fn an_old_writer_that_follows_someone_else_or_nobody_holds_the_change() {
        // Read-only, but replicating from pod 1, not the candidate.
        let obs = [
            n("n0", "db-0", true, Some("db-1")),
            n("n2", "db-2", false, None),
        ];
        assert!(
            matches!(decide(Some("n0"), &obs), Observed::NoWriter(w) if w.contains("not from n2"))
        );
        // Read-only (mid-switch) and replicating from nobody.
        let obs = [n("n0", "db-0", true, None), n("n2", "db-2", false, None)];
        assert!(matches!(decide(Some("n0"), &obs), Observed::NoWriter(_)));
        // Nobody writable.
        let obs = [n("n0", "db-0", true, None), n("n2", "db-2", true, None)];
        assert!(matches!(decide(Some("n0"), &obs), Observed::NoWriter(w) if w.contains("no node")));
        // First publication.
        let obs = [
            n("n0", "db-0", false, None),
            n("n1", "db-1", true, Some("db-0")),
        ];
        assert_eq!(decide(None, &obs), Observed::Candidate("n0".into()));
    }

    #[test]
    fn master_hosts_match_by_name_or_address() {
        let x = n("n2", "r2o-c-2", false, None);
        assert!(x.is_named_by("r2o-c-2.r2o-c-internal.sprigdb.svc.cluster.local"));
        assert!(x.is_named_by("R2O-C-2"));
        assert!(x.is_named_by("127.0.0.1"));
        assert!(!x.is_named_by("r2o-c-20.r2o-c-internal"));
        assert!(!x.is_named_by("r2o-c-1.r2o-c-internal"));
    }
}
