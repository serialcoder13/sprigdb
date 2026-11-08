//! Reference tables: copy states, freshness and where reads go (chapter
//! 15, "Reference tables").
//!
//! The primary copy lives on the tenant's catalog cluster and takes every
//! write; a `ref_follow` mover job per other cluster seeds a copy and
//! applies the primary's changes from the change feed (chapter 38),
//! recording each applied source GTID in `__sprig_ref_pos`. This module
//! holds the parts the router and the controller share: the states, the
//! freshness rule and the read decision.

use std::time::Duration;

/// The table, in each reference schema, recording the last applied source
/// position.
pub const REF_POS_TABLE: &str = "__sprig_ref_pos";

/// `CREATE TABLE` of [`REF_POS_TABLE`]: one row per source domain.
pub const DDL_REF_POS: &str = "CREATE TABLE IF NOT EXISTS __sprig_ref_pos (
  domain_id  INT UNSIGNED NOT NULL,
  server_id  INT UNSIGNED NOT NULL,
  seq_no     BIGINT UNSIGNED NOT NULL,
  applied_at DATETIME(6) NOT NULL,
  PRIMARY KEY (domain_id)
) ENGINE=InnoDB";

/// A copy's state (`reference_tables.state`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyState {
    /// The chunked copy runs.
    Seeding,
    /// Applying changes, not yet caught up.
    Following,
    /// Caught up within `sharding.reference_max_lag_ms`.
    Serving,
    /// Lagging; reads go to the primary.
    Stale,
    /// Being removed.
    Dropping,
}

/// `sharding.reference_reads`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceReads {
    /// Join against the local copy when it is fresh enough.
    Local,
    /// Every read goes to the primary.
    Primary,
}

/// A GTID position in one replication domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct GtidPos {
    /// The domain.
    pub domain: u32,
    /// The sequence number.
    pub seq_no: u64,
}

/// Where a read of a reference table goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadTarget {
    /// The copy on the cluster running the rest of the statement.
    Local,
    /// The primary copy.
    Primary,
    /// The local copy, once `__sprig_ref_pos` shows this position applied
    /// (the session wrote a reference table); else the primary.
    LocalIfApplied(GtidPos),
}

/// The read decision: the local copy only when it is `serving` and the
/// setting allows it; if the session wrote a reference table, only after
/// that write has been applied there.
#[must_use]
pub fn read_target(
    reads: ReferenceReads,
    local: Option<CopyState>,
    session_wrote: Option<GtidPos>,
) -> ReadTarget {
    match (reads, local) {
        (ReferenceReads::Local, Some(CopyState::Serving)) => match session_wrote {
            Some(p) => ReadTarget::LocalIfApplied(p),
            None => ReadTarget::Local,
        },
        _ => ReadTarget::Primary,
    }
}

/// The controller's freshness rule: `stale` above the limit, `serving`
/// again below half of it; other states don't change here.
#[must_use]
pub fn next_state(state: CopyState, lag: Duration, max_lag: Duration) -> CopyState {
    match state {
        CopyState::Serving | CopyState::Following if lag > max_lag => CopyState::Stale,
        CopyState::Stale | CopyState::Following if lag < max_lag / 2 => CopyState::Serving,
        s => s,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_and_freshness() {
        let p = GtidPos {
            domain: 0,
            seq_no: 907,
        };
        assert_eq!(
            read_target(ReferenceReads::Local, Some(CopyState::Serving), None),
            ReadTarget::Local
        );
        assert_eq!(
            read_target(ReferenceReads::Local, Some(CopyState::Serving), Some(p)),
            ReadTarget::LocalIfApplied(p)
        );
        assert_eq!(
            read_target(ReferenceReads::Local, Some(CopyState::Stale), None),
            ReadTarget::Primary
        );
        assert_eq!(
            read_target(ReferenceReads::Primary, Some(CopyState::Serving), None),
            ReadTarget::Primary
        );
        assert_eq!(
            read_target(ReferenceReads::Local, None, None),
            ReadTarget::Primary
        );
        let max = Duration::from_millis(2000);
        let ms = Duration::from_millis;
        assert_eq!(
            next_state(CopyState::Serving, ms(2500), max),
            CopyState::Stale
        );
        assert_eq!(
            next_state(CopyState::Stale, ms(1500), max),
            CopyState::Stale
        );
        assert_eq!(
            next_state(CopyState::Stale, ms(900), max),
            CopyState::Serving
        );
        assert_eq!(
            next_state(CopyState::Following, ms(10), max),
            CopyState::Serving
        );
        assert_eq!(
            next_state(CopyState::Seeding, ms(10), max),
            CopyState::Seeding
        );
    }
}
