//! Tenant sessions follow their tenant's map: while a mover cutover holds
//! the map, a statement that would start new work waits (until the hold
//! clears or its deadline passes); once the map
//! points at another cluster, the session's backend moves there. A
//! transaction left open on the old cluster is lost with its connection
//! and reported as 1213, like any lost connection.
//!
//! The map followed is the one in the tenant's home DC, wherever this
//! gateway is (remote homes; see
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
//!
//! **A DC failover holds too.** From the promotion's
//! commit point the directory names the new home, whose map stays
//! `standby` until the promotion has drained the copy (its job reached
//! the old home's last position), given the tenant's accounts their
//! write privileges back and turned the map `home`. While the followed
//! map is `standby`, every statement of the tenant waits (prepares and
//! reads too: a `PREPARE` of an `INSERT` is checked against the copy's
//! revoked grants, and a read would see a copy that isn't caught up), up
//! to [`PROMOTION_HOLD`]; past it the statement gets 1205, never the copy.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sprig_catalog::placement::MapRole;
use sprig_common::{Result, SprigError};

use super::PassSession;

/// The longest one statement waits for a hold, whatever its deadline says.
const MAX_WAIT: Duration = Duration::from_secs(10);

/// The longest one statement waits for a promotion to make the new home
/// writable.
pub(super) const PROMOTION_HOLD: Duration = MAX_WAIT;

/// Placement checks a session keeps per map generation: a
/// statement whose tenant's map mirror is at the generation the last
/// check saw skips the work that check did. Every change to a map (a hold,
/// a cutover, a split, a promotion) bumps the generation, and the home DC
/// is part of the key.
#[derive(Debug, Default)]
pub(super) struct PlacementMemo {
    /// `follow_tenant` found nothing to wait for and routed to this
    /// cluster at (home, generation).
    followed: Option<(sprig_common::ids::DcId, u64, sprig_common::ids::ClusterId)>,
    /// `maps_token` at (home, generation): its revision sum and held flag.
    token: Option<(sprig_common::ids::DcId, u64, u64, bool)>,
}

impl PlacementMemo {
    /// [`GatewayLocator::maps_token`](crate::tenant::locator::GatewayLocator::maps_token)
    /// for `tenant`, kept per generation.
    pub(super) fn maps_token(
        &mut self,
        locator: &crate::tenant::locator::GatewayLocator,
        tenant: sprig_common::ids::TenantId,
    ) -> (sprig_common::ids::DcId, u64, bool) {
        let (dc, g) = locator.maps_generation(tenant);
        if let Some((d, mg, sum, held)) = &self.token
            && *d == dc
            && *mg == g
        {
            return (dc, *sum, *held);
        }
        let t = locator.maps_token(tenant);
        if t.0 == dc {
            self.token = Some((dc, g, t.1, t.2));
        }
        t
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// 1205 for a statement held past the cutover's deadline.
fn held_too_long() -> SprigError {
    SprigError::standard(
        1205,
        *b"HY000",
        "Lock wait timeout exceeded; try restarting transaction (the tenant's move held \
         writes past its deadline)",
    )
}

/// 1205 for a statement held past [`PROMOTION_HOLD`]: the tenant's
/// new home is still catching up; nothing was sent to it.
fn promotion_held_too_long() -> SprigError {
    SprigError::standard(
        1205,
        *b"HY000",
        "Lock wait timeout exceeded; try restarting transaction (the tenant's failover to \
         another data center is still catching its new home up)",
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
        // Nothing changed since the last statement found nothing to
        // wait for.
        let (home0, gen0) = locator.maps_generation(tenant);
        if let Some((d, g, c)) = &self.placement_memo.followed
            && *d == home0
            && *g == gen0
            && *c == self.target.cluster
        {
            return Ok(());
        }
        self.placement_memo.followed = None;
        let mut changes = locator.placement_changes_for(tenant);
        let started = std::time::Instant::now();
        let mut waited = false;
        let mut promotion: Option<std::time::Instant> = None;
        loop {
            let Some(p) = locator.placement_followed(tenant).await else {
                return Ok(()); // no map copy yet: route as before
            };
            // A promotion is still draining the new home's copy. A
            // `quarantined` map is a home left behind (a move between DCs,
            // a failover) until the directory names the new one (read at
            // most once a second): wait for it too rather than answer
            // 39530.
            if matches!(
                locator.followed_role(tenant),
                Some(MapRole::Standby | MapRole::Quarantined)
            ) {
                if promotion.is_none() {
                    promotion = Some(std::time::Instant::now());
                    metrics::counter!("sprig_gateway_promotion_holds_total").increment(1);
                }
                if started.elapsed() < PROMOTION_HOLD {
                    let _ =
                        tokio::time::timeout(Duration::from_millis(200), changes.changed()).await;
                    changes = locator.placement_changes_for(tenant);
                    continue;
                }
                metrics::counter!("sprig_gateway_promotion_hold_timeouts_total").increment(1);
                return Err(promotion_held_too_long());
            }
            if let Some(t) = promotion.take() {
                metrics::histogram!("sprig_gateway_promotion_hold_seconds")
                    .record(t.elapsed().as_secs_f64());
                tracing::debug!(%tenant, held_ms = t.elapsed().as_millis(), "statement released after the tenant's promotion");
            }
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
            } else if p.hold.is_none()
                && home == home0
                && matches!(locator.followed_role(tenant), Some(MapRole::Home))
            {
                // Nothing here depends on the clock or the transaction:
                // the same maps give the same answer.
                self.placement_memo.followed = Some((home0, gen0, self.target.cluster.clone()));
            }
            return Ok(());
        }
    }
}
