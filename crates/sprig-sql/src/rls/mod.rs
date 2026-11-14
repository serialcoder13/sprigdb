//! Row-level security for SQL, the gateway's side:
//!
//! - [`stmt`]: `CREATE`, `ALTER`, `DROP` and `SHOW [CREATE] POLICY`,
//!   `ALTER TABLE ... ROW LEVEL SECURITY`;
//! - [`expr`]: policy expressions, validated when a policy is created and
//!   rendered for each table reference with the session's values bound;
//! - [`inject`]: the statement rewrite that puts every protected table's
//!   predicate into the statement before it is planned and routed.

pub mod definer;
pub mod expr;
pub mod inject;
pub mod stmt;
pub mod subtree;
pub(crate) mod text;
pub mod triggers;
pub mod views;

pub use inject::{Cmd, Injected, Resolver, inject};
pub use text::{quote, sql_string};

use sprig_common::{SprigCode, SprigError};

/// 39366 `ER_SPRIG_RLS_UNSUPPORTED`: a statement over a protected table
/// that can't be policed exactly; refused rather than run unfiltered.
#[must_use]
pub fn unsupported(what: &str) -> SprigError {
    SprigError::new(
        SprigCode::RlsUnsupported,
        format!("ER_SPRIG_RLS_UNSUPPORTED: {what} isn't supported under row-level security"),
    )
}
