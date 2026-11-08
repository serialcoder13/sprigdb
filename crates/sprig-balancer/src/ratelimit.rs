//! The schema-creation rate limit (chapter 11, "Placing new shards"; build
//! step 9): new shard schemas are created on one cluster at most
//! `placement.max_schema_creates_per_minute` at a time. Pre-splits, tenant
//! bootstraps and rebalancing waves queue behind it, because mass DDL
//! leaves `InnoDB` flushing for minutes (the month-1 spike saw reads on the
//! whole server fall from about 15,000 to under 1,000 per second).
//!
//! A window per cluster per minute: the first request in a minute opens
//! the window; at most the limit is admitted in it.

use std::collections::BTreeMap;

use sprig_common::ids::ClusterId;

use crate::model::Timestamp;

/// Per-cluster creation windows.
#[derive(Debug, Clone, Default)]
pub struct SchemaCreateLimiter {
    windows: BTreeMap<ClusterId, (Timestamp, u32)>,
}

impl SchemaCreateLimiter {
    /// An empty limiter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Asks to create `n` schemas on `c` at `now` with `limit` per minute.
    /// Returns how many may be created now (0 to `n`) and, when fewer than
    /// `n`, when to ask again.
    pub fn admit(
        &mut self,
        c: &ClusterId,
        n: u32,
        limit: u32,
        now: Timestamp,
    ) -> (u32, Option<Timestamp>) {
        let limit = limit.max(1);
        let w = self.windows.entry(c.clone()).or_insert((now, 0));
        if now >= w.0 + 60 {
            *w = (now, 0);
        }
        let free = limit.saturating_sub(w.1);
        let take = n.min(free);
        w.1 += take;
        if take < n {
            (take, Some(w.0 + 60))
        } else {
            (take, None)
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn a_40_shard_presplit_at_10_per_minute_takes_4_minutes() {
        let c = ClusterId::new("mum1-c1").unwrap();
        let mut l = SchemaCreateLimiter::new();
        let mut now = 1000;
        let mut left = 40;
        let mut admitted_at = Vec::new();
        while left > 0 {
            let (n, again) = l.admit(&c, left, 10, now);
            if n > 0 {
                admitted_at.push((now, n));
            }
            left -= n;
            if let Some(t) = again {
                now = t;
            }
        }
        assert_eq!(
            admitted_at,
            vec![(1000, 10), (1060, 10), (1120, 10), (1180, 10)]
        );
        // The last batch starts 3 minutes after the first: 4 minute
        // windows, and no more.
        assert_eq!(admitted_at.last().unwrap().0 - 1000, 180);
    }

    #[test]
    fn clusters_have_their_own_windows() {
        let (a, b) = (
            ClusterId::new("mum1-c1").unwrap(),
            ClusterId::new("mum1-c2").unwrap(),
        );
        let mut l = SchemaCreateLimiter::new();
        assert_eq!(l.admit(&a, 300, 300, 0), (300, None));
        assert_eq!(l.admit(&a, 1, 300, 59), (0, Some(60)));
        assert_eq!(l.admit(&b, 5, 300, 59), (5, None));
        assert_eq!(l.admit(&a, 1, 300, 60), (1, None));
    }
}
