//! `DEFINER` naming another account (MySQL 8.4's `SET_ANY_DEFINER`).
//!
//! ```text
//! CREATE | ALTER ... DEFINER = 'bob'@'%' ... (a view, routine, trigger or event)
//!   bob is the session's own account, CURRENT_USER or CURRENT_ROLE → as before
//!   no SET_ANY_DEFINER                         → 1227 (SUPER or SET_ANY_DEFINER)
//!   bob isn't a live user of the tenant        → 1227 (SUPER or ALLOW_NONEXISTENT_DEFINER)
//!   else: bob's backend account t<T>_u<bob>@% is the definer MariaDB records
//!         (stored programs and views both run as sprig_ctl with it, then the
//!         recorded definer is checked: 39367 otherwise)
//! ```
//!
//! As on MySQL, the code then runs with bob's rights (`SQL SECURITY
//! DEFINER`, triggers, events) and, under row-level security, bob's
//! policies: both read the definer MariaDB recorded.

use sprig_common::ids::UserId;
use sprig_common::{Result, SprigError};
use sprig_sql::lex::LexOptions;

use super::PassSession;

impl PassSession {
    /// The user a `CREATE` or `ALTER` names as `DEFINER` other than the
    /// session's own account (see the module docs); `None` when it names
    /// none.
    ///
    /// # Errors
    /// 1227 without `SET_ANY_DEFINER`, or for an account the tenant has no
    /// live user for.
    pub(super) fn named_definer(&self, text: &[u8], lex: LexOptions) -> Result<Option<UserId>> {
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(None);
        };
        let p = &ts.principal;
        let named = crate::tenant::ddl::named_definers(text, lex);
        let Some((user, host)) = named
            .into_iter()
            .rfind(|(u, h)| *u != p.user || !h.eq_ignore_ascii_case(&p.host))
        else {
            return Ok(None);
        };
        let holder = crate::pipeline::SessionExec::holds_tenant_privilege(
            self,
            sprig_auth::authz::Action::SetAnyDefiner,
        )
        .unwrap_or(false);
        if !holder {
            return Err(SprigError::standard(
                1227,
                *b"42000",
                "Access denied; you need (at least one of) the SUPER or SET_ANY_DEFINER \
                 privilege(s) for this operation",
            ));
        }
        let uid = ts
            .catalog()
            .and_then(|cat| sprig_auth::TenantAuth::of(cat))
            .and_then(|auth| {
                auth.accounts(&user)
                    .iter()
                    .find(|u| u.row.host.eq_ignore_ascii_case(&host))
                    .map(|u| u.row.user_id)
            });
        uid.map(Some).ok_or_else(|| {
            SprigError::standard(
                1227,
                *b"42000",
                "Access denied; you need (at least one of) the SUPER or \
                 ALLOW_NONEXISTENT_DEFINER privilege(s) for this operation",
            )
        })
    }
}
