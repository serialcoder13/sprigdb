//! `sprig-settings`: the setting registry, scopes, bounds and resolution.
//!
//! Layer 1 (foundations).
//!
//! What exists:
//! - [`Setting`] declarations and the [`Registry`] they are collected into
//!   ([`register!`]);
//! - [`MemSettings`]: rows of every scope in memory, with
//!   resolution (nearest value wins, every wider scope's bounds apply,
//!   clamping at read time) and its write checks (39080-39084);
//! - [`etcd`]: the `global`, `dc` and `cluster` records in etcd and
//!   [`SettingsWriter`], which writes them through the scope's leader with
//!   the write checks;
//! - [`watch`]: [`LiveSettings`], mirrors of those scopes with a snapshot
//!   for resolution, key watches and apply callbacks, and
//!   `SettingNotApplied`.
//!
//! - [`resolve`]: six-scope resolution: a tenant's
//!   rows at one catalog version ([`TenantSettings`], built by the catalog
//!   cache) layered over the etcd snapshot, with bounds and clamping,
//!   memoized per (tenant snapshot version, etcd snapshot); [`Settings`],
//!   the process-wide entry point with key watches across all six scopes
//!   and [`ResolvedWatch`]es on resolved values; clamp reports for the
//!   warnings of bound-tightening writes.
//!
//! - [`explain`]: a resolution with its chain, one step per scope with
//!   its row, the bounds in force there and what happened to its value
//!   (`sprigctl settings get`);
//!   [`SettingsWriter::set_guarded`] and [`BoundsPatch`]: the Admin API's
//!   writes under the serving leader's fencing compare, with bounds
//!   patched inside the guarded write.
//! - [`guard`]: [`ValueGuard`], checks of a value against the running
//!   system before either writer stores it (such as
//!   `auth.authorizer`).
//!
//! The tenant-scope side lives elsewhere: the ancestor-copy loop that
//! fills ancestor rows is `sprig-controller::tree_settings` and
//! tenant, database and object writes are `sprig-catalog::settings`, which reuses
//! [`normalize_change`]. Not yet: privilege checks.

pub mod etcd;
pub mod explain;
pub mod guard;
pub mod ops;
pub mod registry;
pub mod resolve;
pub mod scope;
pub mod value;
pub mod watch;

pub use etcd::{
    BoundsPatch, RecordOutcome, SettingChange, SettingEdit, SettingRecord, SettingsStores,
    SettingsWriter, Warning, normalize_change, show_bounds, show_value,
};
pub use explain::{ExplainStep, Explanation, StepOutcome, explain_layered};
pub use guard::{AllGuards, GuardFuture, ValueGuard};
#[doc(hidden)]
pub use linkme;
pub use registry::{AnySetting, Apply, HardBounds, Mark, Registry, Scopes, Setting};
pub use resolve::{
    Bounds, ClampedValue, Entry, Ignored, MemSettings, OwnedCtx, Resolved, ResolvedWatch, Settings,
    TenantNotify, TenantScope, TenantSettings, TenantSettingsBuilder, clamp_summary,
    resolve_layered,
};
pub use scope::{Level, Scope, ScopeKind, SettingCtx};
pub use value::{SettingValue, Value, ValueKind};
pub use watch::{LiveSettings, Registration, Snapshot, not_applied};

#[cfg(test)]
mod live_tests;
#[cfg(test)]
mod tests;
