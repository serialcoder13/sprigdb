//! `sprig-catalog`: the tenant catalog model, storage, caching and versioning.
//!
//! Layer: 2 data layer. Design: [chapter 08](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/08-catalog.md)
//! (The catalog) of the SprigDB low-level design, with the tables in
//! Appendix C and the global tier's tenant directory in chapter 23.
//!
//! Each tenant has one catalog, the schema `t<tenant>_sprig`. This crate
//! owns its four core tables (`databases`, `objects`, `settings`,
//! `schema_versions`) and the per-tenant catalog and auth versions that are
//! allocated in the global tier, so caches invalidate reliably.
//!
//! What exists (chapter 08, build steps 1-3):
//! - [`migrate`]: the catalog DDL and the migration framework: ordered,
//!   idempotent, expand and contract migrations per module, recorded in the
//!   catalog's internal keys, serialized across processes with `GET_LOCK`,
//!   and the format check (39073) that upgrade orchestration gates on
//!   (step 1);
//! - [`store`]: typed rows of the four tables, id counters, tombstones and
//!   purging, and the consistent, incremental snapshot read a cache loader
//!   uses (step 2);
//! - [`version`]: version allocation in the global tier and
//!   [`change`](version::Versioner::change), the protocol that commits a
//!   versioned change, with failpoints for crash tests (step 3).
//!
//! Not yet: the gateway cache (step 4), [`CatalogExtension`] loading,
//! setting writes (step 7), bootstrap (step 9), the DDL write path (step 10),
//! the reconciler (step 11) and upgrade orchestration (step 12).
//!
//! [`CatalogExtension`]: https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/08-catalog.md#interfaces

pub mod migrate;
mod sql;
pub mod store;
pub mod version;

pub use migrate::{FormatState, Migration, Migrator, Module, ModuleFormat, Phase};
pub use store::{
    CatalogEndpoint, CatalogLocator, Counter, DatabaseRow, DbState, Namespace, ObjectFlags,
    ObjectKind, ObjectRow, ObjectState, SchemaVersionRow, SettingRow, SettingScope, Snapshot,
    StaticLocator, Store, Versions,
};
pub use version::{
    CatalogStore, CatalogTx, Change, ChangeOpts, DirectoryAllocator, DirectoryVersions,
    MariaBackend, MariaCatalog, MariaOpts, MariaTx, Step, VersionAllocator, VersionBackend,
    VersionKind, Versioner,
};

#[cfg(test)]
mod live_tests;
