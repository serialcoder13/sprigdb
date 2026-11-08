//! Catalog extensions: other chapters' modules in the catalog (chapter 08,
//! "Tables owned by other chapters").
//!
//! Each owning crate (`sprig-auth`, search, logic, graphs, ...) registers a
//! [`CatalogExtension`]: its migrations, whether it is versioned, and how
//! to load its state into a snapshot. `sprig-auth` depends on
//! `sprig-catalog`, never the reverse. The cache loads every versioned
//! module in the same consistent transaction as the core rows
//! ([`SnapshotTx`]), so a snapshot never mixes versions; modules marked
//! `None` (large or often-written tables) are read on demand and never
//! loaded here.
//!
//! **Incremental loads.** A module's [`load`](CatalogExtension::load)
//! gets the previous state and `since`: the snapshot version of the
//! module's kind ([`versioned`](CatalogExtension::versioned)) the previous
//! state was loaded at. It returns the whole new state, normally the
//! previous one with the rows changed after `since` applied. `since` is
//! `None` for a full load: the first load, a catalog purged past the
//! cached version, or a previous snapshot without this module.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use mysql_async::Transaction;
use sprig_common::Result;

use crate::migrate::{Migration, Module};
use crate::store::{Store, Versions};
use crate::version::VersionKind;

/// A module's state in a snapshot: immutable, shared through `Arc`.
pub trait ExtState: Any + Send + Sync + fmt::Debug {
    /// Estimated memory, counted in the cache's bound.
    fn bytes(&self) -> usize {
        0
    }
}

/// The transaction a snapshot reads in: consistent, read-only, at the
/// versions the core rows were read at.
pub struct SnapshotTx<'a> {
    tx: &'a mut Transaction<'static>,
    store: &'a Store,
    versions: Versions,
}

impl fmt::Debug for SnapshotTx<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotTx")
            .field("schema", &self.store.schema())
            .field("versions", &self.versions)
            .finish_non_exhaustive()
    }
}

impl<'a> SnapshotTx<'a> {
    pub(crate) fn new(
        tx: &'a mut Transaction<'static>,
        store: &'a Store,
        versions: Versions,
    ) -> Self {
        Self {
            tx,
            store,
            versions,
        }
    }

    /// The connection, inside the snapshot.
    pub fn conn(&mut self) -> &mut Transaction<'static> {
        self.tx
    }

    /// The catalog schema (`t<tenant>_sprig`), for qualifying tables.
    #[must_use]
    pub fn schema(&self) -> &str {
        self.store.schema()
    }

    /// The core tables' store.
    #[must_use]
    pub fn store(&self) -> &Store {
        self.store
    }

    /// The versions in the snapshot.
    #[must_use]
    pub fn versions(&self) -> Versions {
        self.versions
    }
}

/// A module owning catalog tables (chapter 08's interface).
#[async_trait::async_trait]
pub trait CatalogExtension: Send + Sync + 'static {
    /// Module name: `auth`, `search`, `logic`, ... (`[a-z0-9_]+`).
    fn module(&self) -> &'static str;

    /// Its migrations, expand and contract, in format order.
    fn migrations(&self) -> &'static [Migration];

    /// Which version its changes bump; `None`: read on demand only, never
    /// loaded into snapshots.
    fn versioned(&self) -> Option<VersionKind>;

    /// Loads the module's state in the snapshot: from nothing when `since`
    /// is `None`, else `prev` with the rows changed after `since` applied
    /// (see the module docs).
    ///
    /// # Errors
    /// The module's; the load fails and the cache keeps its previous
    /// snapshot.
    async fn load(
        &self,
        tx: &mut SnapshotTx<'_>,
        since: Option<u64>,
        prev: Option<&Arc<dyn ExtState>>,
    ) -> Result<Arc<dyn ExtState>>;
}

/// The migration module of an extension, for [`Migrator::new`](crate::Migrator::new).
#[must_use]
pub fn module_of(ext: &dyn CatalogExtension) -> Module {
    Module {
        name: ext.module(),
        migrations: ext.migrations(),
    }
}

/// The modules' states in one snapshot.
#[derive(Clone, Default)]
pub struct ExtensionStates {
    states: Arc<HashMap<&'static str, Arc<dyn ExtState>>>,
}

impl fmt::Debug for ExtensionStates {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.states.iter()).finish()
    }
}

impl ExtensionStates {
    /// The state of `module`, if loaded.
    #[must_use]
    pub fn raw(&self, module: &str) -> Option<&Arc<dyn ExtState>> {
        self.states.get(module)
    }

    /// The state of `module` as its type.
    #[must_use]
    pub fn get<T: ExtState>(&self, module: &str) -> Option<&T> {
        let s: &dyn Any = &**self.states.get(module)?;
        s.downcast_ref::<T>()
    }

    /// Modules loaded.
    pub fn modules(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.states.keys().copied()
    }

    /// Number of modules loaded.
    #[must_use]
    pub fn len(&self) -> usize {
        self.states.len()
    }

    /// Whether no module is loaded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    /// Estimated memory of every state.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.states.values().map(|s| s.bytes()).sum()
    }

    /// These states with `loaded` replacing their modules.
    #[must_use]
    pub fn with(&self, loaded: Vec<(&'static str, Arc<dyn ExtState>)>) -> Self {
        if loaded.is_empty() {
            return self.clone();
        }
        let mut states = (*self.states).clone();
        states.extend(loaded);
        Self {
            states: Arc::new(states),
        }
    }
}
