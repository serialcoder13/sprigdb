//! [`TenantCatalog`]: one tenant's catalog at one version, immutable and
//! shared through `Arc`.
//!
//! A snapshot is built from a full load ([`TenantCatalog::build`]) or
//! derived from the previous one by an incremental load
//! ([`TenantCatalog::apply`]), which shares every index shard and settings
//! scope the change didn't touch. Both paths normalize the same way, so an
//! incremental load equals a full load at the same version:
//!
//! - dropped databases and objects (tombstones) are left out, and their
//!   settings rows with them (purge deletes those rows later, so a full
//!   load after a purge agrees);
//! - empty settings rows (resets kept until purge) are left out;
//! - internal `sprig.` rows are not settings; `sprig.ancestry` sets the
//!   ancestry (the ancestry copy writes it).
//!
//! Rows committed without a version (transient states such as `creating`,
//! "Intermediate states commit without a version") are seen by an
//! incremental load only once a versioned change touches them; a full load
//! at the same version may already show them. Readers treat transient
//! states as not yet (or still) there.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_common::ids::{DbId, DcId, ObjectId, TenantId};
use sprig_settings::{
    ClampedValue, MemSettings, Registry, SettingCtx, TenantScope, TenantSettings,
};

use super::cow::CowMap;
use super::ext::{ExtState, ExtensionStates};
use crate::store::{
    DatabaseRow, DbState, Namespace, ObjectRow, ObjectState, SettingScope, Snapshot,
};

/// The internal key holding the ancestry.
pub const ANCESTRY_KEY: &str = "sprig.ancestry";

/// Estimated per-entry overhead of an index, for the memory bound.
const ENTRY_BYTES: usize = 96;

/// An ancestor tenant, as `sprig.ancestry` lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantRef {
    /// The ancestor's id.
    pub id: TenantId,
    /// Its name.
    #[serde(default)]
    pub name: String,
}

/// `sprig.ancestry`: root first, either `[{"id": 1, "name": "platform"},
/// ...]` or bare ids.
#[derive(Deserialize)]
#[serde(untagged)]
enum AncestryItem {
    Ref(TenantRef),
    Id(TenantId),
}

impl From<SettingScope> for TenantScope {
    fn from(s: SettingScope) -> Self {
        match s {
            SettingScope::Tenant => TenantScope::Tenant,
            SettingScope::Database(d) => TenantScope::Database(d),
            SettingScope::Object(o) => TenantScope::Object(o),
            SettingScope::Ancestor(t) => TenantScope::Ancestor(t),
        }
    }
}

/// A snapshot's databases, by id and by name key.
#[derive(Clone, Debug, Default)]
pub struct DatabaseIndex {
    by_id: CowMap<DbId, Arc<DatabaseRow>>,
    by_name: CowMap<Arc<str>, DbId>,
}

impl DatabaseIndex {
    /// The database `id`.
    #[must_use]
    pub fn get(&self, id: DbId) -> Option<&Arc<DatabaseRow>> {
        self.by_id.get(&id)
    }

    /// The database whose name key is `name_key`.
    #[must_use]
    pub fn by_name(&self, name_key: &str) -> Option<&Arc<DatabaseRow>> {
        self.get(*self.by_name.get(name_key)?)
    }

    /// Every database, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = &Arc<DatabaseRow>> {
        self.by_id.values()
    }

    /// Number of databases.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// Whether there are none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

type NameMap = CowMap<Arc<str>, ObjectId>;

/// A snapshot's objects, by id and by `(database, namespace, name key)`.
#[derive(Clone, Debug, Default)]
pub struct ObjectIndex {
    pub(crate) by_id: CowMap<ObjectId, Arc<ObjectRow>>,
    by_name: Arc<HashMap<(DbId, Namespace), NameMap>>,
}

impl ObjectIndex {
    /// The object `id`.
    #[must_use]
    pub fn get(&self, id: ObjectId) -> Option<&Arc<ObjectRow>> {
        self.by_id.get(&id)
    }

    /// The object named `name_key` in `db` and `namespace`.
    #[must_use]
    pub fn by_name(
        &self,
        db: DbId,
        namespace: Namespace,
        name_key: &str,
    ) -> Option<&Arc<ObjectRow>> {
        let id = self.by_name.get(&(db, namespace))?.get(name_key)?;
        self.get(*id)
    }

    /// The names in `db` and `namespace`, with their ids, in no particular
    /// order.
    pub fn names(&self, db: DbId, namespace: Namespace) -> impl Iterator<Item = (&str, ObjectId)> {
        self.by_name
            .get(&(db, namespace))
            .into_iter()
            .flat_map(|m| m.iter().map(|(k, v)| (&**k, *v)))
    }

    /// Every object, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = &Arc<ObjectRow>> {
        self.by_id.values()
    }

    /// Number of objects.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// Whether there are none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

/// What a source read for one load: the core rows and every versioned
/// module's state.
#[derive(Debug, Clone)]
pub struct Loaded {
    /// The core rows, full or changed after `snapshot.since`.
    pub snapshot: Snapshot,
    /// Modules' states, replacing the previous snapshot's.
    pub ext: Vec<(&'static str, Arc<dyn ExtState>)>,
}

impl Loaded {
    /// Core rows only.
    #[must_use]
    pub fn core(snapshot: Snapshot) -> Self {
        Self {
            snapshot,
            ext: Vec::new(),
        }
    }
}

/// One tenant's catalog at one version: immutable, shared through `Arc`.
/// A statement keeps the snapshot it started with.
#[derive(Clone)]
pub struct TenantCatalog {
    /// The tenant.
    pub tenant: TenantId,
    /// The catalog version (`sprig.catalog_version`).
    pub version: u64,
    /// The auth version (`sprig.auth_version`).
    pub auth_version: u64,
    /// `sprig.purged_version` when loaded.
    pub purged_version: u64,
    /// Databases.
    pub databases: DatabaseIndex,
    /// Objects of every kind and namespace.
    pub objects: ObjectIndex,
    /// Tenant, database, object and ancestor setting rows.
    pub settings: Arc<TenantSettings>,
    /// Ancestors, root first, parent last (`sprig.ancestry`).
    pub ancestry: Arc<[TenantRef]>,
    /// Other crates' modules.
    pub ext: ExtensionStates,
    core_bytes: usize,
}

impl fmt::Debug for TenantCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TenantCatalog")
            .field("tenant", &self.tenant)
            .field("version", &self.version)
            .field("auth_version", &self.auth_version)
            .field("databases", &self.databases.len())
            .field("objects", &self.objects.len())
            .field("settings", &self.settings.len())
            .field("ext", &self.ext)
            .finish_non_exhaustive()
    }
}

/// Equal versions, rows, names, settings and ancestry; module states are
/// not compared.
impl PartialEq for TenantCatalog {
    fn eq(&self, other: &Self) -> bool {
        let dbs = |c: &TenantCatalog| {
            let mut v: Vec<DatabaseRow> = c.databases.iter().map(|r| (**r).clone()).collect();
            v.sort_by_key(|r| r.db_id);
            v
        };
        let objs = |c: &TenantCatalog| {
            let mut v: Vec<ObjectRow> = c.objects.iter().map(|r| (**r).clone()).collect();
            v.sort_by_key(|r| r.object_id);
            v
        };
        let names = |c: &TenantCatalog| {
            let mut v: Vec<(DbId, Namespace, String, ObjectId)> = c
                .objects
                .by_name
                .iter()
                .flat_map(|(&(d, n), m)| m.iter().map(move |(k, &id)| (d, n, k.to_string(), id)))
                .collect();
            v.sort();
            let mut d: Vec<(String, DbId)> = c
                .databases
                .by_name
                .iter()
                .map(|(k, &id)| (k.to_string(), id))
                .collect();
            d.sort();
            (v, d)
        };
        self.tenant == other.tenant
            && self.version == other.version
            && self.auth_version == other.auth_version
            && self.ancestry == other.ancestry
            && *self.settings == *other.settings
            && dbs(self) == dbs(other)
            && objs(self) == objs(other)
            && names(self) == names(other)
            && self.core_bytes == other.core_bytes
    }
}

fn db_bytes(r: &DatabaseRow) -> usize {
    std::mem::size_of::<DatabaseRow>()
        + ENTRY_BYTES * 2
        + r.name.len()
        + r.name_key.len() * 2
        + r.default_charset.len()
        + r.default_collation.len()
        + serde_json::to_vec(&r.options).map_or(0, |v| v.len())
}

fn object_bytes(r: &ObjectRow) -> usize {
    std::mem::size_of::<ObjectRow>() + ENTRY_BYTES * 2 + r.name.len() + r.name_key.len() * 2
}

impl TenantCatalog {
    /// A catalog with nothing in it, at version 0.
    #[must_use]
    pub fn empty(registry: &'static Registry, tenant: TenantId) -> Self {
        Self {
            tenant,
            version: 0,
            auth_version: 0,
            purged_version: 0,
            databases: DatabaseIndex::default(),
            objects: ObjectIndex::default(),
            settings: Arc::new(TenantSettings::empty(registry, tenant)),
            ancestry: Arc::from(Vec::new()),
            ext: ExtensionStates::default(),
            core_bytes: 0,
        }
    }

    /// A snapshot from a full load (`loaded.snapshot.since` is ignored).
    #[must_use]
    pub fn build(registry: &'static Registry, tenant: TenantId, loaded: Loaded) -> Self {
        Self::empty(registry, tenant).absorb(loaded, true)
    }

    /// The next snapshot: `loaded` applied to this one when it is an
    /// incremental load (`since` set), else a fresh build from it.
    #[must_use]
    pub fn apply(&self, loaded: Loaded) -> Self {
        if loaded.snapshot.since.is_none() {
            return Self::build(self.settings.registry(), self.tenant, loaded);
        }
        self.absorb(loaded, false)
    }

    /// Estimated memory: rows, indexes, settings and module states.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.core_bytes + self.settings.bytes() + self.ext.bytes()
    }

    /// The database named `name_key`.
    #[must_use]
    pub fn database(&self, name_key: &str) -> Option<&Arc<DatabaseRow>> {
        self.databases.by_name(name_key)
    }

    /// The object named `name_key` in database `db` and `namespace`.
    #[must_use]
    pub fn object(
        &self,
        db: DbId,
        namespace: Namespace,
        name_key: &str,
    ) -> Option<&Arc<ObjectRow>> {
        self.objects.by_name(db, namespace, name_key)
    }

    /// A settings context for the tenant in `dc` (resolve with
    /// [`sprig_settings::Settings::resolve`] and `Some(&self.settings)`).
    #[must_use]
    pub fn setting_ctx<'a>(&'a self, dc: &'a DcId) -> SettingCtx<'a> {
        self.settings.ctx(dc)
    }

    /// A settings context for `object` in `dc`, with its database.
    #[must_use]
    pub fn object_ctx<'a>(&'a self, dc: &'a DcId, object: ObjectId) -> SettingCtx<'a> {
        let ctx = self.settings.ctx(dc);
        match self.objects.get(object) {
            Some(o) => ctx.with_db(o.db_id).with_object(object),
            None => ctx.with_object(object),
        }
    }

    /// Stored values resolution clamps or ignores against `etcd`
    /// ([`TenantSettings::clamped`]), with objects' databases filled in.
    #[must_use]
    pub fn clamped(&self, etcd: &MemSettings<'_>, dc: &DcId) -> Vec<ClampedValue> {
        self.settings
            .clamped(etcd, dc, &|o| self.objects.get(o).map(|r| r.db_id))
    }

    fn absorb(&self, loaded: Loaded, full: bool) -> Self {
        let Loaded {
            snapshot: snap,
            ext,
        } = loaded;
        let mut dbs = self.databases.clone();
        let mut objs = self.objects.clone();
        let mut bytes = self.core_bytes;
        let mut st = if full {
            TenantSettings::builder(self.settings.registry(), self.tenant, snap.versions.catalog)
        } else {
            self.settings.edit(snap.versions.catalog)
        };
        let mut ancestry = Arc::clone(&self.ancestry);
        let mut dropped: HashSet<TenantScope> = HashSet::new();

        for d in snap.databases {
            if let Some(old) = dbs.by_id.remove(&d.db_id) {
                bytes = bytes.saturating_sub(db_bytes(&old));
                if dbs.by_name.get(old.name_key.as_str()) == Some(&old.db_id) {
                    dbs.by_name.remove(old.name_key.as_str());
                }
            }
            if d.state == DbState::Dropped {
                dropped.insert(TenantScope::Database(d.db_id));
                st.remove_scope(TenantScope::Database(d.db_id));
                continue;
            }
            bytes += db_bytes(&d);
            dbs.by_name.insert(Arc::from(d.name_key.as_str()), d.db_id);
            dbs.by_id.insert(d.db_id, Arc::new(d));
        }

        let names = Arc::make_mut(&mut objs.by_name);
        for o in snap.objects {
            if let Some(old) = objs.by_id.remove(&o.object_id) {
                bytes = bytes.saturating_sub(object_bytes(&old));
                let k = (old.db_id, old.namespace);
                if let Some(m) = names.get_mut(&k) {
                    if m.get(old.name_key.as_str()) == Some(&old.object_id) {
                        m.remove(old.name_key.as_str());
                    }
                    if m.is_empty() {
                        names.remove(&k);
                    }
                }
            }
            if o.state == ObjectState::Dropped {
                dropped.insert(TenantScope::Object(o.object_id));
                st.remove_scope(TenantScope::Object(o.object_id));
                continue;
            }
            bytes += object_bytes(&o);
            names
                .entry((o.db_id, o.namespace))
                .or_default()
                .insert(Arc::from(o.name_key.as_str()), o.object_id);
            objs.by_id.insert(o.object_id, Arc::new(o));
        }

        for s in snap.settings {
            if s.is_internal() {
                if s.key == ANCESTRY_KEY && s.scope == SettingScope::Tenant {
                    ancestry = parse_ancestry(self.tenant, s.value.as_ref());
                }
                continue;
            }
            let scope = TenantScope::from(s.scope);
            if dropped.contains(&scope) {
                continue;
            }
            match s.entry() {
                Ok(e) => st.put(scope, &s.key, e),
                Err(e) => {
                    tracing::debug!(tenant = %self.tenant, key = %s.key, error = %e, "setting row ignored");
                    st.put_invalid(scope, &s.key);
                }
            }
        }
        st.set_ancestry(ancestry.iter().map(|r| r.id).collect());

        dbs.by_id.rebalance();
        dbs.by_name.rebalance();
        objs.by_id.rebalance();
        for m in Arc::make_mut(&mut objs.by_name).values_mut() {
            m.rebalance();
        }
        Self {
            tenant: self.tenant,
            version: snap.versions.catalog,
            auth_version: snap.versions.auth,
            purged_version: snap.versions.purged,
            databases: dbs,
            objects: objs,
            settings: Arc::new(st.build()),
            ancestry,
            ext: self.ext.with(ext),
            core_bytes: bytes,
        }
    }
}

pub(crate) fn parse_ancestry(tenant: TenantId, v: Option<&serde_json::Value>) -> Arc<[TenantRef]> {
    let Some(v) = v else {
        return Arc::from(Vec::new());
    };
    match serde_json::from_value::<Vec<AncestryItem>>(v.clone()) {
        Ok(items) => items
            .into_iter()
            .map(|i| match i {
                AncestryItem::Ref(r) => r,
                AncestryItem::Id(id) => TenantRef {
                    id,
                    name: String::new(),
                },
            })
            .collect(),
        Err(e) => {
            tracing::warn!(%tenant, error = %e, "bad {ANCESTRY_KEY}; resolving without ancestors");
            Arc::from(Vec::new())
        }
    }
}
