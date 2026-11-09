//! Chapter 25 in the executor: the DC permit gates writes of tenants
//! homed in this DC, and a fenced copy refuses writes with 39530 (see
//! [`crate::failover`]). While a promotion finishes (the map followed in
//! the new home is still `standby`), writes wait up to
//! [`PROMOTION_WAIT`], then get 39530 too (decision F6).

use std::time::{Duration, Instant};

use sprig_catalog::placement::MapRole;
use sprig_common::Result;

use super::PassSession;
use crate::backend::classify::StmtClass;
use crate::dc;
use crate::failover::{self, GatewayFence};
use crate::pipeline::Request;

/// How long a write waits for a promotion to finish.
pub(super) const PROMOTION_WAIT: Duration = Duration::from_secs(5);

/// `COMMIT` (the acknowledgement of a transaction's writes).
fn is_commit(sql: &[u8]) -> bool {
    let s = sql.trim_ascii_start();
    s.len() >= 6 && s[..6].eq_ignore_ascii_case(b"commit")
}

impl PassSession {
    /// Checks a request of a tenant session: `Ok(Some(fence))` when it is
    /// a write (or `COMMIT`) of a tenant homed in this DC, whose answer
    /// must then pass the fence too; `Ok(None)` for anything else.
    ///
    /// # Errors
    /// 39501 without a valid permit; 39530 when the map followed for the
    /// tenant is `quarantined`.
    pub(super) async fn dc_write_gate(
        &mut self,
        req: &Request,
    ) -> Result<Option<&'static GatewayFence>> {
        let Some(f) = failover::get() else {
            return Ok(None);
        };
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(None);
        };
        let tenant = ts.tenant;
        let locator = ts.services().locator.clone();
        let write = match req {
            Request::Query { sql, .. } => dc::is_write(sql) || is_commit(sql),
            Request::Execute { stmt, .. } | Request::BulkExecute { stmt, .. } => self
                .stmts
                .get_mut(*stmt)
                .is_ok_and(|s| s.info.class == StmtClass::Write),
            _ => false,
        };
        if !write {
            return Ok(None);
        }
        let started = Instant::now();
        while locator.followed_role(tenant) == Some(MapRole::Standby)
            && started.elapsed() < PROMOTION_WAIT
        {
            // The tenant's promotion hasn't made the new home's map `home`
            // yet: wait for it rather than write to a replicating copy.
            let mut changes = locator.placement_changes_for(tenant);
            let _ = tokio::time::timeout(Duration::from_millis(200), changes.changed()).await;
            let _ = locator.placement_followed(tenant).await;
        }
        if matches!(
            locator.followed_role(tenant),
            Some(MapRole::Quarantined | MapRole::Standby)
        ) {
            metrics::counter!("sprig_gateway_recovery_fenced_writes_total").increment(1);
            return Err(failover::recovery_fenced(tenant));
        }
        if locator.home_of(tenant) != *f.dc() {
            // Homed elsewhere: that DC's writer fences itself.
            return Ok(None);
        }
        f.check_write()?;
        Ok(Some(f))
    }
}

#[cfg(test)]
mod tests {
    use super::is_commit;

    #[test]
    fn commit_is_recognised() {
        assert!(is_commit(b"COMMIT"));
        assert!(is_commit(b"  commit work"));
        assert!(!is_commit(b"ROLLBACK"));
        assert!(!is_commit(b"com"));
    }
}
