//! Daily token budgets, per tenant and per user: hard stops.
//!
//! A turn checks both before every model call, counting what the tenant
//! and the user used today (UTC) plus what the turn itself used so far.
//! When either is spent the turn ends with 39992 before the call is made;
//! a call's output is also capped at what is left. The tokens counted are
//! [`Usage::billable`](crate::provider::Usage::billable): input read or
//! written uncached, and output.

use serde::{Deserialize, Serialize};
use sprig_common::Result;

use crate::error::budget_exceeded;

/// The limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Budget {
    /// `ai.budget.tenant_tokens_day`; 0 allows none.
    pub tenant_tokens_day: u64,
    /// `ai.budget.user_tokens_day`; 0 allows none.
    pub user_tokens_day: u64,
}

/// Tokens used today.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageToday {
    /// By the whole tenant.
    pub tenant: u64,
    /// By the user.
    pub user: u64,
}

impl Budget {
    /// Tokens left today: the smaller of the tenant's and the user's.
    #[must_use]
    pub fn remaining(&self, used: UsageToday, this_turn: u64) -> u64 {
        let t = self
            .tenant_tokens_day
            .saturating_sub(used.tenant.saturating_add(this_turn));
        let u = self
            .user_tokens_day
            .saturating_sub(used.user.saturating_add(this_turn));
        t.min(u)
    }

    /// `Ok` while both budgets have tokens left.
    ///
    /// # Errors
    /// 39992 naming the budget that is spent.
    pub fn check(&self, used: UsageToday, this_turn: u64) -> Result<()> {
        if used.tenant.saturating_add(this_turn) >= self.tenant_tokens_day {
            crate::metrics::budget_stop();
            return Err(budget_exceeded("tenant", self.tenant_tokens_day));
        }
        if used.user.saturating_add(this_turn) >= self.user_tokens_day {
            crate::metrics::budget_stop();
            return Err(budget_exceeded("user", self.user_tokens_day));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use sprig_common::SprigCode;

    use super::*;

    #[test]
    fn budgets_stop_hard() {
        let b = Budget {
            tenant_tokens_day: 1000,
            user_tokens_day: 100,
        };
        let used = UsageToday {
            tenant: 500,
            user: 40,
        };
        assert!(b.check(used, 0).is_ok());
        assert_eq!(b.remaining(used, 10), 50);
        let e = b.check(used, 60).unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::AiBudgetExceeded));
        assert!(e.message.contains("user"));
        let e = b
            .check(
                UsageToday {
                    tenant: 1000,
                    user: 0,
                },
                0,
            )
            .unwrap_err();
        assert!(e.message.contains("tenant"));
        let none = Budget {
            tenant_tokens_day: 0,
            user_tokens_day: 0,
        };
        assert!(none.check(UsageToday::default(), 0).is_err());
    }
}
