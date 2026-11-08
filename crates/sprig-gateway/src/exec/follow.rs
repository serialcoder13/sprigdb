//! Tenant sessions follow their tenant's map (chapter 10, "Cutover";
//! chapter 04's side of the hold; chapter 24, "Sessions during cutover"):
//! while a mover cutover holds the map, a statement that would start new
//! work waits (until the hold clears or its deadline passes); once the map
//! points at another cluster, the session's backend moves there. A
//! transaction left open on the old cluster is lost with its connection
//! and reported as 1213, like any lost connection.
//!
//! The map followed is the one in the tenant's home DC, wherever this
//! gateway is (chapter 24's remote homes; see
//! [`GatewayLocator`](crate::tenant::locator::GatewayLocator)). For a move
//! between DCs:
//!
//! - a statement still held when the hold's deadline passes gets 1205
//!   (lock wait timeout, retryable): the cutover neither committed nor
//!   released in time, and the old home may be fenced;
//! - a session holding state that can't follow (temporary tables,
//!   `GET_LOCK` locks, `LOCK TABLES`, XA) gets 39502
//!   `ER_SPRIG_SESSION_MOVED` (SQLSTATE 08S01) and must reconnect;
//! - every other session moves transparently: `USE`, variables and
//!   prepared statements are replayed on the new home's connections.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sprig_common::{Result, SprigError};

use super::PassSession;

/// The longest one statement waits for a hold, whatever its deadline says.
const MAX_WAIT: Duration = Duration::from_secs(10);

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// 1205 for a statement held past the cutover's deadline (chapter 24).
fn held_too_long() -> SprigError {
    SprigError::standard(
        1205,
        *b"HY000",
        "Lock wait timeout exceeded; try restarting transaction (the tenant's move held \
         writes past its deadline)",
    )
}

impl PassSession {
    /// Waits out a hold and follows a move; see the module docs.
    pub(super) async fn follow_tenant(&mut self) -> Result<()> {
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(());
        };
        let tenant = ts.tenant;
        let locator = ts.services().locator.clone();
        let mut changes = locator.placement_changes_for(tenant);
        let started = std::time::Instant::now();
        let mut waited = false;
        loop {
            let Some(p) = locator.placement_followed(tenant).await else {
                return Ok(()); // no map copy yet: route as before
            };
            let in_trx = self.bs.as_ref().is_some_and(|b| b.pins().in_transaction());
            if let Some(h) = &p.hold
                && !in_trx
            {
                let now = now_ms();
                if now < h.until_ms && started.elapsed() < MAX_WAIT {
                    // New work waits for the cutover; open transactions run
                    // on (the mover gives them `mover.txn_grace_ms`).
                    let wait = Duration::from_millis(h.until_ms - now)
                        .min(MAX_WAIT.saturating_sub(started.elapsed()));
                    let _ = tokio::time::timeout(wait, changes.changed()).await;
                    waited = true;
                    metrics::counter!("sprig_gateway_hold_waits_total").increment(1);
                    // The tenant's home may have moved: follow that map.
                    changes = locator.placement_changes_for(tenant);
                    continue;
                }
                // A hold whose deadline had passed before this statement
                // came is stale (its worker died): route by the map.
                if waited || now < h.until_ms {
                    metrics::counter!("sprig_gateway_hold_timeouts_total").increment(1);
                    return Err(held_too_long());
                }
            }
            if p.cluster != self.target.cluster {
                self.target.cluster = p.cluster.clone();
                if let Some(bs) = self.bs.as_mut() {
                    bs.retarget(p.cluster);
                }
            }
            let home = locator.home_of(tenant);
            if &home != locator.routing().dc() {
                metrics::counter!("sprig_gateway_remote_home_statements_total", "home_dc" => home.to_string())
                    .increment(1);
            }
            return Ok(());
        }
    }
}
