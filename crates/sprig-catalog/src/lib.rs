//! `sprig-catalog`: the tenant catalog model, storage, caching and versioning.
//!
//! Layer: 2 data layer.
//!
//! Each tenant has one catalog, the schema `t<tenant>_sprig`. This crate
//! owns its four core tables (`databases`, `objects`, `settings`,
//! `schema_versions`) and the per-tenant catalog and auth versions that are
//! allocated in the global tier, so caches invalidate reliably.
//!
//! What exists:
//! - [`migrate`]: the catalog DDL and the migration framework: ordered,
//!   idempotent, expand and contract migrations per module, recorded in the
//!   catalog's internal keys, serialized across processes with `GET_LOCK`,
//!   and the format check (39073) that upgrade orchestration gates on;
//! - [`store`]: typed rows of the four tables, id counters, tombstones and
//!   purging, and the consistent, incremental snapshot read a cache loader
//!   uses;
//! - [`version`]: version allocation in the global tier and
//!   [`change`](version::Versioner::change), the protocol that commits a
//!   versioned change, with failpoints for crash tests.
//!
//! - [`cache`]: the gateway cache: immutable [`TenantCatalog`]
//!   snapshots, incremental loads, invalidation from the directory's
//!   version keys, single-flight loads, read-your-own-DDL freshness,
//!   bounded memory with LRU eviction, and [`CatalogExtension`]s through
//!   which other crates' modules load their state into snapshots.
//!
//! Six-scope setting resolution over a snapshot is in
//! `sprig-settings` ([`sprig_settings::Settings`]); each snapshot carries
//! its tenant's rows as a [`sprig_settings::TenantSettings`].
//!
//! - [`ancestry`]: the catalog side of the ancestry copy: `sprig.ancestry`,
//!   `sprig.ancestor_versions` and the read-only copies of every
//!   ancestor's tenant-scope rows, replaced in one versioned change (the
//!   `tree_settings` loop that drives it is in `sprig-controller`);
//! - [`settings`]: setting writes at tenant, database and object scope:
//!   registry, scope, `operator_only`, privilege hook, type,
//!   bounds of every wider scope (etcd scopes and ancestors' copies
//!   included), locks and allowed lists, clamp warnings and decisions;
//! - [`bootstrap`] and [`placement`]: the catalog side of tenant
//!   bootstrap: the schema and its migrations, `sprig.bootstrap` progress,
//!   and the catalog's shard map `shardmaps/<tenant>/0`;
//! - [`rows`]: the settings-row access the three share, over MariaDB and
//!   the in-memory catalog.
//!
//! - [`ddl`]: the single-shard DDL write path: prepare with
//!   unversioned marks and an intent row, the physical statement through a
//!   [`DdlExecutor`](ddl::DdlExecutor), one versioned change to publish;
//!   and the repairs the reconciler makes with it (finish, roll
//!   back or roll forward stuck jobs, adopt outside objects). The
//!   reconciler loop itself is `sprig-controller::catalog_reconciler`.
//!
//! Not yet: upgrade orchestration, and ancestry modes other than `copy`.

pub mod ancestry;
pub mod bootstrap;
pub mod cache;
pub mod ddl;
pub mod infoschema;
pub mod migrate;
pub mod placement;
pub mod rows;
pub mod settings;
mod sql;
pub mod store;
pub mod version;

pub use ancestry::{AncestorRows, AncestryReport, AncestryState};
pub use cache::{
    CacheConfig, CatalogExtension, CatalogLoaded, CatalogSource, DirectoryFeed, DirectoryWatch,
    ExtState, ExtensionStates, Freshness, MariaSource, Node, SnapshotTx, TenantCache,
    TenantCatalog, TenantPin, TenantRef,
};
pub use migrate::{FormatState, Migration, Migrator, Module, ModuleFormat, Phase};
pub use rows::{SettingsBackend, SettingsTx, TenantScopeRows};
pub use settings::{
    AllowAll, DecisionSink, EtcdView, FixedEtcd, LogDecisions, Principal, Privilege, SetOutcome,
    SetRequest, SettingAuthorizer, SettingDecision, Target, TenantSettingsWriter,
};
pub use store::{
    CatalogEndpoint, CatalogLocator, Counter, DatabaseRow, DbState, Namespace, ObjectFlags,
    ObjectKind, ObjectRow, ObjectState, SchemaVersionRow, SettingRow, SettingScope, Snapshot,
    StaticLocator, Store, Versions,
};
pub use version::{
    CatalogStore, CatalogTx, Change, ChangeOpts, DirectoryAllocator, DirectoryVersions, Locked,
    MariaBackend, MariaCatalog, MariaOpts, MariaTx, Step, VersionAllocator, VersionBackend,
    VersionKind, Versioner,
};

#[cfg(test)]
mod live_tests;
