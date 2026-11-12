//! `sprig-settings`: the setting registry, scopes, bounds and resolution.
//!
//! Layer 1 (foundations). Design: chapter 08 (Catalog, "Setting
//! resolution") and chapter 23 (Control plane, "Settings: writes, bounds and
//! watches") of the SprigDB low-level design; key declaration in chapter 03.
//!
//! What exists:
//! - [`Setting`] declarations and the [`Registry`] they are collected into
//!   ([`register!`]);
//! - [`MemSettings`]: rows of every scope in memory, with chapter 08's
//!   resolution (nearest value wins, every wider scope's bounds apply,
//!   clamping at read time) and its write checks (39080-39084);
//! - [`etcd`]: the `global`, `dc` and `cluster` records in etcd and
//!   [`SettingsWriter`], which writes them through the scope's leader with
//!   chapter 23's checks (chapter 23 step 8);
//! - [`watch`]: [`LiveSettings`], mirrors of those scopes with a snapshot
//!   for resolution, key watches and apply callbacks, and
//!   `SettingNotApplied` (chapter 23 step 8).
//!
//! - [`resolve`]: six-scope resolution (chapter 08 step 5): a tenant's
//!   rows at one catalog version ([`TenantSettings`], built by the catalog
//!   cache) layered over the etcd snapshot, with bounds and clamping,
//!   memoized per (tenant snapshot version, etcd snapshot); [`Settings`],
//!   the process-wide entry point with key watches across all six scopes
//!   and [`ResolvedWatch`]es on resolved values; clamp reports for the
//!   warnings of bound-tightening writes.
//!
//! - [`explain`]: a resolution with its chain, one step per scope with
//!   its row, the bounds in force there and what happened to its value
//!   (`sprigctl settings get`, chapter 47 "Explaining settings");
//!   [`SettingsWriter::set_guarded`] and [`BoundsPatch`]: the Admin API's
//!   writes under the serving leader's fencing compare, with bounds
//!   patched inside the guarded write.
//! - [`guard`]: [`ValueGuard`], checks of a value against the running
//!   system before either writer stores it (chapter 29's
//!   `auth.authorizer`).
//!
//! The tenant-scope side lives elsewhere: the ancestor-copy loop that
//! fills ancestor rows is `sprig-controller::tree_settings` (chapter 08
//! step 6) and tenant, database and object writes are
//! `sprig-catalog::settings` (step 7), which reuses
//! [`normalize_change`]. Not yet: privilege checks (chapter 29).

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
pub use registry::{AnySetting, Apply, HardBounds, Registry, Scopes, Setting};
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
