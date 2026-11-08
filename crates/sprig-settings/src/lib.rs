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
//! Not yet: the ancestor-copy loop that fills ancestor rows (chapter 08
//! step 6; resolution already walks them), tenant-scope writes (step 7),
//! privileges and `operator_only` checks (chapter 29).

pub mod etcd;
pub mod registry;
pub mod resolve;
pub mod scope;
pub mod value;
pub mod watch;

pub use etcd::{
    SettingChange, SettingRecord, SettingsStores, SettingsWriter, Warning, normalize_change,
};
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
