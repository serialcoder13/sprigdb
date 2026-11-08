//! Tenant sessions follow their tenant's map (chapter 10, "Cutover";
//! chapter 04's side of the hold): while a mover cutover holds the map, a
//! statement that would start new work waits (until the hold clears or its
//! deadline passes); once the map points at another cluster, the session's
//! backend moves there. A transaction left open on the old cluster is lost
//! with its connection and reported as 1213, like any lost connection.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::PassSession;

/// The longest one statement waits for a hold, whatever its deadline says.
const MAX_WAIT: Duration = Duration::from_secs(10);

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

impl PassSession {
    /// Waits out a hold and follows a move; see the module docs.
    pub(super) async fn follow_tenant(&mut self) {
        let Some(ts) = self.tenant.as_deref() else {
            return;
        };
        let tenant = ts.tenant;
        let locator = ts.services().locator.clone();
        let mut changes = locator.placement_changes();
        let started = std::time::Instant::now();
        loop {
            let Some(p) = locator.placement(tenant) else {
                return; // no map copy yet: route as before
            };
            let in_trx = self.bs.as_ref().is_some_and(|b| b.pins().in_transaction());
            let now = now_ms();
            if let Some(h) = &p.hold
                && now < h.until_ms
                && !in_trx
                && started.elapsed() < MAX_WAIT
            {
                // New work waits for the cutover; open transactions run on
                // (the mover gives them `mover.txn_grace_ms`).
                let wait = Duration::from_millis(h.until_ms - now).min(MAX_WAIT);
                let _ = tokio::time::timeout(wait, changes.changed()).await;
                metrics::counter!("sprig_gateway_hold_waits_total").increment(1);
                continue;
            }
            if p.cluster != self.target.cluster {
                self.target.cluster = p.cluster.clone();
                if let Some(bs) = self.bs.as_mut() {
                    bs.retarget(p.cluster);
                }
            }
            return;
        }
    }
}
