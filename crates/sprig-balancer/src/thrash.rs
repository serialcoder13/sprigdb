//! Ping-pong detection (chapter 11, "Thrash: ping-pong and flapping"): a
//! shard moved back to a cluster it left within 7 days counts in
//! `sprig_balancer_moves_reverted_total`, and doubles that shard's
//! cooldown, capped at `migration.max_cooldown_days`.

use serde::{Deserialize, Serialize};
use sprig_common::ids::ClusterId;

use crate::model::{Timestamp, Visit};
use crate::plan::MOVE_BACK_WINDOW_SECS;

/// Clusters a shard remembers.
pub const HISTORY_LEN: usize = 4;

/// The thrash counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThrashCounters {
    /// Moves back within 7 days.
    pub reverted: u64,
    /// Of those, moves back to a cluster that returned from an outage
    /// within the window (label `after_recovery`).
    pub reverted_after_recovery: u64,
}

/// Whether moving to `to` at `now` is a move back.
#[must_use]
pub fn is_move_back(history: &[Visit], to: &ClusterId, now: Timestamp) -> bool {
    history
        .iter()
        .any(|v| &v.cluster == to && v.left_at + MOVE_BACK_WINDOW_SECS > now)
}

/// Remembers that the shard left `from` at `now`, keeping the last
/// [`HISTORY_LEN`].
pub fn remember(history: &mut Vec<Visit>, from: ClusterId, now: Timestamp) {
    history.push(Visit {
        cluster: from,
        left_at: now,
    });
    if history.len() > HISTORY_LEN {
        let extra = history.len() - HISTORY_LEN;
        history.drain(..extra);
    }
}

/// The cooldown after a move: the base, doubled per move back, capped.
#[must_use]
pub fn cooldown_secs(base: u64, moves_back: u32, cap: u64) -> u64 {
    let factor = 1u64.checked_shl(moves_back.min(32)).unwrap_or(u64::MAX);
    base.saturating_mul(factor).min(cap.max(base))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn move_back_window_and_history() {
        let a = ClusterId::new("mum1-c1").unwrap();
        let b = ClusterId::new("mum1-c2").unwrap();
        let mut h = Vec::new();
        remember(&mut h, a.clone(), 100);
        assert!(is_move_back(&h, &a, 100 + 3600));
        assert!(!is_move_back(&h, &b, 100 + 3600));
        assert!(!is_move_back(&h, &a, 100 + MOVE_BACK_WINDOW_SECS));
        for i in 0..6 {
            remember(&mut h, b.clone(), 200 + i);
        }
        assert_eq!(h.len(), HISTORY_LEN);
        assert!(!is_move_back(&h, &a, 300));
    }

    #[test]
    fn cooldown_doubles_and_caps() {
        assert_eq!(cooldown_secs(86_400, 0, 7 * 86_400), 86_400);
        assert_eq!(cooldown_secs(86_400, 1, 7 * 86_400), 2 * 86_400);
        assert_eq!(cooldown_secs(86_400, 2, 7 * 86_400), 4 * 86_400);
        assert_eq!(cooldown_secs(86_400, 3, 7 * 86_400), 7 * 86_400);
        assert_eq!(cooldown_secs(86_400, 60, 7 * 86_400), 7 * 86_400);
        // A cap below the base never shortens the base.
        assert_eq!(cooldown_secs(86_400, 1, 3600), 86_400);
    }
}
