//! Checks of a value against the running system before it is written:
//! for keys whose valid values depend on more than their declaration.
//! Chapter 29's `auth.authorizer` is the first: a new engine is accepted
//! only when every live gateway knows it
//! (`sprig_auth::authz::engine_guard`).
//!
//! Both writers call the guard they were given for every `Set`, after the
//! key, scope and type checks and before anything is written:
//! [`SettingsWriter`](crate::SettingsWriter) for `global`, `dc` and
//! `cluster`, and the catalog's `TenantSettingsWriter` for `tenant`,
//! `database` and `object`. Resets and bounds aren't checked: a reset
//! falls back to a value that was checked when it was set.

use std::future::Future;
use std::pin::Pin;

use sprig_common::Result;

use crate::Value;

/// What [`ValueGuard::check`] returns.
pub type GuardFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

/// A check of values before they are written (see the module docs).
pub trait ValueGuard: Send + Sync + std::fmt::Debug {
    /// `Ok` if `value` may be written for `key`; keys the guard doesn't
    /// look at are `Ok`.
    ///
    /// # Errors
    /// Why the value is refused.
    fn check<'a>(&'a self, key: &'a str, value: &'a Value) -> GuardFuture<'a>;
}
