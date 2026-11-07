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
//!   clamping at read time) and its write checks (39080-39084).
//!
//! Not yet: the etcd mirror of global, dc and cluster rows, the catalog
//! snapshot of tenant rows, memoization, watches and apply callbacks
//! (chapter 23 step 8), privileges and `operator_only` checks (chapter 29).

pub mod registry;
pub mod resolve;
pub mod scope;
pub mod value;

#[doc(hidden)]
pub use linkme;
pub use registry::{AnySetting, Apply, HardBounds, Registry, Scopes, Setting};
pub use resolve::{Bounds, Entry, Ignored, MemSettings, Resolved};
pub use scope::{Level, Scope, ScopeKind, SettingCtx};
pub use value::{SettingValue, Value, ValueKind};

#[cfg(test)]
mod tests;
