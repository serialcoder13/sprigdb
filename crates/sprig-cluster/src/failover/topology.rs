//! Single nodes and external clusters.
//!
//! - **Single node.** A restart is not a failover. A lost node's tenants
//!   are promoted from standbys (the DC-level promotion, per tenant), or
//!   restored from backup and archived binlogs when
//!   `backup.binlog_archive_seconds` fits the RPO.
//! - **External.** The provider fails over; SprigDB follows the endpoint,
//!   re-probes, and uses the declared RPO.
//! - **mariadb-operator** promotes on its own; SprigDB sees the new primary
//!   through probes, like an external cluster.

use std::time::Duration;

/// What to do about a single node that stopped answering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SingleNodePlan {
    /// The node is restarting (its agent says so) or hasn't been down for
    /// `failover.cluster_detect_seconds`: wait.
    Wait,
    /// Promote each tenant's standby copy in another DC: its lag fits the
    /// RPO.
    PromoteStandbys,
    /// Restore from backup and archived binlogs: the archive lag fits the
    /// RPO.
    Restore,
    /// Neither fits the RPO (or the mode is manual): an operator decides.
    Blocked(String),
}

/// What a lost single node needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SingleNodeLoss {
    /// The agent reports a restart in progress.
    pub restarting: bool,
    /// How long the node has been down.
    pub down_for: Duration,
    /// `failover.cluster_detect_seconds`.
    pub detect: Duration,
    /// The worst standby lag over the node's tenants; `None` without
    /// standbys.
    pub standby_lag: Option<Duration>,
    /// `backup.binlog_archive_seconds`, when archiving is on.
    pub binlog_archive: Option<Duration>,
    /// `failover.rpo_seconds`.
    pub rpo: Duration,
    /// `failover.mode = manual`.
    pub manual: bool,
}

/// The plan for a lost single node.
#[must_use]
pub fn single_node_plan(l: &SingleNodeLoss) -> SingleNodePlan {
    if l.restarting || l.down_for < l.detect {
        return SingleNodePlan::Wait;
    }
    if l.manual {
        return SingleNodePlan::Blocked("failover.mode is manual".into());
    }
    match (l.standby_lag, l.binlog_archive) {
        (Some(lag), _) if lag <= l.rpo => SingleNodePlan::PromoteStandbys,
        (_, Some(a)) if a <= l.rpo => SingleNodePlan::Restore,
        (Some(lag), _) => SingleNodePlan::Blocked(format!(
            "standby lag {} s is over the RPO {} s and no archive fits",
            lag.as_secs(),
            l.rpo.as_secs()
        )),
        (None, _) => {
            SingleNodePlan::Blocked("no standby and no binlog archive fits the RPO".into())
        }
    }
}

/// What a probe pass shows about an external (or operator-driven)
/// cluster's writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalWriter {
    /// Nothing changed.
    Same,
    /// The provider promoted another node: record it, raise
    /// `cluster_epoch`, log the estimated loss (the declared RPO).
    Moved {
        /// The previous writer.
        from: String,
        /// The new one.
        to: String,
    },
    /// No node is writable: the provider is failing over; wait.
    None,
}

/// Compares the recorded writer with the one probes found writable.
#[must_use]
pub fn follow_external(recorded: &str, writable: Option<&str>) -> ExternalWriter {
    match writable {
        None => ExternalWriter::None,
        Some(w) if w == recorded => ExternalWriter::Same,
        Some(w) => ExternalWriter::Moved {
            from: recorded.to_owned(),
            to: w.to_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loss() -> SingleNodeLoss {
        SingleNodeLoss {
            restarting: false,
            down_for: Duration::from_secs(10),
            detect: Duration::from_secs(5),
            standby_lag: Some(Duration::from_secs(1)),
            binlog_archive: Some(Duration::from_secs(60)),
            rpo: Duration::from_secs(5),
            manual: false,
        }
    }

    #[test]
    fn a_restart_is_not_a_failover() {
        let l = SingleNodeLoss {
            restarting: true,
            ..loss()
        };
        assert_eq!(single_node_plan(&l), SingleNodePlan::Wait);
        let l = SingleNodeLoss {
            down_for: Duration::from_secs(2),
            ..loss()
        };
        assert_eq!(single_node_plan(&l), SingleNodePlan::Wait);
    }

    #[test]
    fn standbys_within_the_rpo_are_promoted_else_a_fitting_restore() {
        assert_eq!(single_node_plan(&loss()), SingleNodePlan::PromoteStandbys);
        let l = SingleNodeLoss {
            standby_lag: Some(Duration::from_secs(9)),
            binlog_archive: Some(Duration::from_secs(3)),
            ..loss()
        };
        assert_eq!(single_node_plan(&l), SingleNodePlan::Restore);
        let l = SingleNodeLoss {
            standby_lag: Some(Duration::from_secs(9)),
            ..loss()
        };
        assert!(matches!(single_node_plan(&l), SingleNodePlan::Blocked(_)));
        let l = SingleNodeLoss {
            manual: true,
            ..loss()
        };
        assert!(matches!(single_node_plan(&l), SingleNodePlan::Blocked(_)));
    }

    #[test]
    fn external_writers_are_followed() {
        assert_eq!(
            follow_external("a:3306", Some("a:3306")),
            ExternalWriter::Same
        );
        assert_eq!(follow_external("a:3306", None), ExternalWriter::None);
        assert_eq!(
            follow_external("a:3306", Some("b:3306")),
            ExternalWriter::Moved {
                from: "a:3306".into(),
                to: "b:3306".into()
            }
        );
    }
}
