//! An in-memory catalog that is both a [`VersionBackend`] and a
//! [`CatalogSource`], so unit and property tests run changes through the
//! real [`Versioner`](crate::version::Versioner) protocol and load them
//! through the real cache without a server.
//!
//! It models what the cache relies on from MariaDB: a change's rows and
//! its version row commit atomically, under the version row's lock; a
//! snapshot reads whole commits, with [`Store::snapshot`]'s rules (rows
//! changed after `since`; a request from before `sprig.purged_version`
//! becomes a full read); [`MemSource::purge`] follows [`Store::purge`].
//! For tests of the loader it can freeze the replica (a lagging replica),
//! fail loads, delay them, and counts them per node.
//!
//! [`Store::snapshot`]: crate::store::Store::snapshot
//! [`Store::purge`]: crate::store::Store::purge

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};

use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use tokio::sync::OwnedMutexGuard;

use super::snapshot::{Loaded, TenantCatalog};
use super::source::{CatalogSource, Node};
use crate::store::{
    DatabaseRow, DbState, ObjectRow, ObjectState, SchemaVersionRow, SettingRow, SettingScope,
    Snapshot, Versions,
};
use crate::version::{VersionBackend, VersionKind};

/// A tenant's committed tables.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MemTables {
    /// The version rows.
    pub versions: Versions,
    /// `databases`.
    pub databases: BTreeMap<DbId, DatabaseRow>,
    /// `objects`.
    pub objects: BTreeMap<ObjectId, ObjectRow>,
    /// `settings`, by scope and key (internal rows are not modelled,
    /// except those a test or the DDL path writes).
    pub settings: BTreeMap<(SettingScope, String), SettingRow>,
    /// The id counters `sprig.next_*_id`, by key: the next id to hand
    /// out (a missing counter starts at 1).
    pub counters: BTreeMap<String, u64>,
    /// `schema_versions`, by object and version.
    pub schema_versions: BTreeMap<(ObjectId, u64), SchemaVersionRow>,
    /// When the catalog version row was last written (`set_at`); `None`
    /// before the first commit.
    pub version_set_at: Option<SystemTime>,
}

impl MemTables {
    /// A snapshot with [`Store::snapshot`](crate::store::Store::snapshot)'s
    /// rules.
    #[must_use]
    pub fn snapshot(&self, since: Option<u64>) -> Snapshot {
        let since = since.filter(|&s| s >= self.versions.purged);
        let after = |v: u64| since.is_none_or(|s| v > s);
        Snapshot {
            versions: self.versions,
            since,
            databases: self
                .databases
                .values()
                .filter(|r| after(r.changed_version))
                .cloned()
                .collect(),
            objects: self
                .objects
                .values()
                .filter(|r| after(r.changed_version))
                .cloned()
                .collect(),
            settings: self
                .settings
                .values()
                .filter(|r| after(r.version))
                .cloned()
                .collect(),
        }
    }

    fn write(&mut self, w: MemWrite) {
        match w {
            MemWrite::Database(r) => {
                self.databases.insert(r.db_id, r);
            }
            MemWrite::Object(r) => {
                self.objects.insert(r.object_id, r);
            }
            MemWrite::Setting(r) => {
                self.settings.insert((r.scope, r.key.clone()), r);
            }
            MemWrite::DeleteDatabase(db) => {
                self.databases.remove(&db);
                self.settings
                    .retain(|(s, _), _| *s != SettingScope::Database(db));
            }
            MemWrite::DeleteObject(o) => {
                self.objects.remove(&o);
                self.settings
                    .retain(|(s, _), _| *s != SettingScope::Object(o));
                self.schema_versions.retain(|(id, _), _| *id != o);
            }
            MemWrite::DeleteSetting(scope, key) => {
                self.settings.remove(&(scope, key));
            }
            MemWrite::Counter(key, next) => {
                self.counters.insert(key, next);
            }
            MemWrite::SchemaVersion(r) => {
                self.schema_versions.insert((r.object_id, r.version), r);
            }
            MemWrite::Reset {
                scope,
                key,
                value,
                bounds,
                version,
            } => {
                if let Some(r) = self.settings.get_mut(&(scope, key)) {
                    if value {
                        r.value = None;
                    }
                    if bounds {
                        r.bounds = None;
                    }
                    r.version = version;
                    r.set_at = SystemTime::UNIX_EPOCH;
                }
            }
        }
    }

    /// [`Store::purge`](crate::store::Store::purge)'s deletions up to
    /// `up_to`, then raises the purged version.
    fn purge(&mut self, up_to: u64) {
        let dead_objects: Vec<ObjectId> = self
            .objects
            .values()
            .filter(|o| o.state == ObjectState::Dropped && o.changed_version <= up_to)
            .map(|o| o.object_id)
            .collect();
        let dead_dbs: Vec<DbId> = self
            .databases
            .values()
            .filter(|d| d.state == DbState::Dropped && d.changed_version <= up_to)
            .map(|d| d.db_id)
            .collect();
        self.settings.retain(|(s, _), r| {
            let dead_scope = match s {
                SettingScope::Object(o) => dead_objects.contains(o),
                SettingScope::Database(d) => dead_dbs.contains(d),
                _ => false,
            };
            !(dead_scope || (r.is_empty() && r.version <= up_to))
        });
        self.schema_versions
            .retain(|(o, _), _| !dead_objects.contains(o));
        for o in dead_objects {
            self.objects.remove(&o);
        }
        for d in dead_dbs {
            self.databases.remove(&d);
        }
        self.versions.purged = self.versions.purged.max(up_to);
    }
}

/// A write staged in a [`MemSourceTx`].
#[derive(Debug, Clone)]
enum MemWrite {
    Database(DatabaseRow),
    Object(ObjectRow),
    Setting(SettingRow),
    DeleteDatabase(DbId),
    DeleteObject(ObjectId),
    DeleteSetting(SettingScope, String),
    Counter(String, u64),
    SchemaVersion(SchemaVersionRow),
    Reset {
        scope: SettingScope,
        key: String,
        value: bool,
        bounds: bool,
        version: u64,
    },
}

#[derive(Debug)]
struct MemTenant {
    locks: [Arc<tokio::sync::Mutex<()>>; 2],
    tables: Mutex<MemTables>,
    /// A frozen replica: replica loads read this copy.
    replica: Mutex<Option<MemTables>>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// An open change: holds the version row's lock and the staged writes.
/// Dropping it rolls back.
#[derive(Debug)]
pub struct MemSourceTx {
    _guard: OwnedMutexGuard<()>,
    tenant: Arc<MemTenant>,
    staged: Vec<MemWrite>,
}

impl MemSourceTx {
    /// Inserts or replaces a database row.
    pub fn put_database(&mut self, r: DatabaseRow) {
        self.staged.push(MemWrite::Database(r));
    }

    /// Inserts or replaces an object row.
    pub fn put_object(&mut self, r: ObjectRow) {
        self.staged.push(MemWrite::Object(r));
    }

    /// Inserts or replaces a settings row.
    pub fn put_setting(&mut self, r: SettingRow) {
        self.staged.push(MemWrite::Setting(r));
    }

    /// Clears a row's value and/or bounds, leaving it (empty) with
    /// `version`, as [`Store::reset_setting`](crate::store::Store::reset_setting).
    pub fn reset_setting(
        &mut self,
        scope: SettingScope,
        key: &str,
        value: bool,
        bounds: bool,
        version: u64,
    ) {
        self.staged.push(MemWrite::Reset {
            scope,
            key: key.to_owned(),
            value,
            bounds,
            version,
        });
    }

    /// Deletes a database row with its settings rows.
    pub fn delete_database(&mut self, db: DbId) {
        self.staged.push(MemWrite::DeleteDatabase(db));
    }

    /// Deletes an object row with its settings rows and schema versions.
    pub fn delete_object(&mut self, object: ObjectId) {
        self.staged.push(MemWrite::DeleteObject(object));
    }

    /// Deletes a settings row.
    pub fn delete_setting(&mut self, scope: SettingScope, key: &str) {
        self.staged
            .push(MemWrite::DeleteSetting(scope, key.to_owned()));
    }

    /// Sets a counter's next id.
    pub fn set_counter(&mut self, key: &str, next: u64) {
        self.staged.push(MemWrite::Counter(key.to_owned(), next));
    }

    /// Inserts or replaces a schema version.
    pub fn put_schema_version(&mut self, r: SchemaVersionRow) {
        self.staged.push(MemWrite::SchemaVersion(r));
    }

    /// The tables as committed (not including staged writes).
    #[must_use]
    pub fn committed(&self) -> MemTables {
        lock(&self.tenant.tables).clone()
    }

    /// Applies the staged writes to `tables` (a copy), as the commit will.
    pub(crate) fn apply_staged(&self, tables: &mut MemTables) {
        for w in &self.staged {
            tables.write(w.clone());
        }
    }
}

/// The in-memory catalog (see the module docs).
#[derive(Debug, Default)]
pub struct MemSource {
    tenants: Mutex<HashMap<TenantId, Arc<MemTenant>>>,
    failing: AtomicBool,
    delay_ms: AtomicU64,
    replica_loads: AtomicU64,
    writer_loads: AtomicU64,
}

impl MemSource {
    /// No tenants.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates `tenant`'s catalog at version 0.
    pub fn bootstrap(&self, tenant: TenantId) {
        lock(&self.tenants).entry(tenant).or_insert_with(|| {
            Arc::new(MemTenant {
                locks: [
                    Arc::new(tokio::sync::Mutex::new(())),
                    Arc::new(tokio::sync::Mutex::new(())),
                ],
                tables: Mutex::new(MemTables::default()),
                replica: Mutex::new(None),
            })
        });
    }

    /// `tenant`'s committed tables.
    #[must_use]
    pub fn tables(&self, tenant: TenantId) -> Option<MemTables> {
        let t = lock(&self.tenants).get(&tenant).cloned()?;
        let tables = lock(&t.tables).clone();
        Some(tables)
    }

    /// Purges tombstones and empty rows changed at or before `up_to`
    /// (clamped to the committed catalog version), holding the version
    /// row's lock, as [`MariaCatalog::purge`](crate::MariaCatalog::purge).
    ///
    /// # Errors
    /// 39072 if the tenant isn't bootstrapped.
    pub async fn purge(&self, tenant: TenantId, up_to: u64) -> Result<()> {
        let t = self.tenant(tenant)?;
        let _g = Arc::clone(&t.locks[0]).lock_owned().await;
        let mut tables = lock(&t.tables);
        let up_to = up_to.min(tables.versions.catalog);
        tables.purge(up_to);
        Ok(())
    }

    /// Sets when `tenant`'s catalog version row was last written, for
    /// tests of purging by age.
    ///
    /// # Errors
    /// 39072 if the tenant isn't bootstrapped.
    pub fn set_version_time(&self, tenant: TenantId, at: SystemTime) -> Result<()> {
        let t = self.tenant(tenant)?;
        lock(&t.tables).version_set_at = Some(at);
        Ok(())
    }

    /// Freezes (`true`) or thaws `tenant`'s replica: while frozen, replica
    /// loads read the tables as they were when frozen.
    ///
    /// # Errors
    /// 39072 if the tenant isn't bootstrapped.
    pub fn freeze_replica(&self, tenant: TenantId, frozen: bool) -> Result<()> {
        let t = self.tenant(tenant)?;
        let copy = frozen.then(|| lock(&t.tables).clone());
        *lock(&t.replica) = copy;
        Ok(())
    }

    /// Makes every load fail with 39070 (`true`) or succeed.
    pub fn set_failing(&self, failing: bool) {
        self.failing.store(failing, Ordering::SeqCst);
    }

    /// Delays every load.
    pub fn set_delay(&self, delay: Duration) {
        self.delay_ms.store(
            u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            Ordering::SeqCst,
        );
    }

    /// Loads served, per node: (replica, writer).
    #[must_use]
    pub fn loads(&self) -> (u64, u64) {
        (
            self.replica_loads.load(Ordering::SeqCst),
            self.writer_loads.load(Ordering::SeqCst),
        )
    }

    fn tenant(&self, tenant: TenantId) -> Result<Arc<MemTenant>> {
        lock(&self.tenants).get(&tenant).cloned().ok_or_else(|| {
            SprigError::new(
                SprigCode::TenantNotReady,
                format!("no catalog for tenant {tenant}"),
            )
        })
    }
}

const fn slot(kind: VersionKind) -> usize {
    match kind {
        VersionKind::Catalog => 0,
        VersionKind::Auth => 1,
    }
}

#[async_trait::async_trait]
impl VersionBackend for MemSource {
    type Tx = MemSourceTx;

    async fn begin_locked(
        &self,
        tenant: TenantId,
        kind: VersionKind,
    ) -> Result<(MemSourceTx, u64)> {
        let t = self.tenant(tenant)?;
        let guard = Arc::clone(&t.locks[slot(kind)]).lock_owned().await;
        let v = lock(&t.tables).versions.get(kind);
        Ok((
            MemSourceTx {
                _guard: guard,
                tenant: t,
                staged: Vec::new(),
            },
            v,
        ))
    }

    async fn commit(&self, tx: MemSourceTx, kind: VersionKind, version: u64) -> Result<()> {
        let mut tables = lock(&tx.tenant.tables);
        for w in tx.staged {
            tables.write(w);
        }
        match kind {
            VersionKind::Catalog => {
                tables.versions.catalog = version;
                tables.version_set_at = Some(SystemTime::now());
            }
            VersionKind::Auth => tables.versions.auth = version,
        }
        Ok(())
    }

    async fn rollback(&self, tx: MemSourceTx) -> Result<()> {
        drop(tx);
        Ok(())
    }
}

#[async_trait::async_trait]
impl CatalogSource for MemSource {
    async fn load(
        &self,
        tenant: TenantId,
        prev: Option<&TenantCatalog>,
        node: Node,
    ) -> Result<Loaded> {
        match node {
            Node::Replica => self.replica_loads.fetch_add(1, Ordering::SeqCst),
            Node::Writer => self.writer_loads.fetch_add(1, Ordering::SeqCst),
        };
        let delay = self.delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        if self.failing.load(Ordering::SeqCst) {
            return Err(SprigError::new(
                SprigCode::CatalogUnavailable,
                "catalog unavailable (test)",
            ));
        }
        let t = self.tenant(tenant)?;
        let since = prev.map(|p| p.version);
        let frozen = match node {
            Node::Replica => lock(&t.replica).clone(),
            Node::Writer => None,
        };
        let snap = match frozen {
            Some(r) => r.snapshot(since),
            None => lock(&t.tables).snapshot(since),
        };
        Ok(Loaded::core(snap))
    }
}
