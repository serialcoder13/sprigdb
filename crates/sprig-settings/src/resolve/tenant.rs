//! One tenant's setting rows at one catalog version (chapter 08, "Setting
//! resolution"; build step 5).
//!
//! The catalog cache builds a [`TenantSettings`] with every snapshot of a
//! tenant's catalog: the tenant, database, object and ancestor rows of the
//! `settings` table, indexed by `(scope, key)` with keys interned through
//! the registry. It is immutable; an incremental load derives the next one
//! with [`TenantSettings::edit`], which shares the rows of every scope the
//! change didn't touch.
//!
//! Rows are kept as stored: clamping happens at read time
//! ([`resolve`](TenantSettings::resolve)), so loosening a bound restores
//! the stored value. Rows the registry doesn't know (written by a newer
//! binary) and rows whose value isn't a setting scalar are ignored and
//! counted ([`ignored_rows`](TenantSettings::ignored_rows)); internal
//! `sprig.` keys are not settings and are skipped.
//!
//! **Ancestors (step 6 hook).** Ancestor rows (`scope_kind = 'ancestor'`)
//! load like any other, and [`ancestry`](TenantSettings::ancestry) (root
//! first, from `sprig.ancestry`, set by the catalog cache) puts them in the
//! chain. Until chapter 08 step 6's `tree_settings` loop writes them, a
//! catalog has neither, and the chain is tenant, database, object.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;

use sprig_common::ids::{DbId, DcId, ObjectId, TenantId};
use sprig_common::{Result, SprigError};

use super::memo::Memo;
use super::{Entry, Ignored, MemSettings, Resolved, walk};
use crate::etcd::Warning;
use crate::registry::Registry;
use crate::scope::{Scope, ScopeKind, SettingCtx};
use crate::value::Value;

/// The scope of a tenant catalog's `settings` row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TenantScope {
    /// The tenant itself (`scope_id` 0).
    Tenant,
    /// A database of the tenant.
    Database(DbId),
    /// An object of the tenant.
    Object(ObjectId),
    /// A read-only copy of an ancestor's tenant-scope row.
    Ancestor(TenantId),
}

impl TenantScope {
    /// The row's address among every scope, for `tenant`'s catalog.
    #[must_use]
    pub const fn scope(self, tenant: TenantId) -> Scope {
        match self {
            TenantScope::Tenant => Scope::Tenant(tenant),
            TenantScope::Database(d) => Scope::Database(tenant, d),
            TenantScope::Object(o) => Scope::Object(tenant, o),
            TenantScope::Ancestor(a) => Scope::Tenant(a),
        }
    }

    /// Where a value from this row shows up in [`Resolved::source`].
    #[must_use]
    pub const fn kind(self) -> ScopeKind {
        match self {
            TenantScope::Tenant => ScopeKind::Tenant,
            TenantScope::Database(_) => ScopeKind::Database,
            TenantScope::Object(_) => ScopeKind::Object,
            TenantScope::Ancestor(a) => ScopeKind::Ancestor(a),
        }
    }
}

impl fmt::Display for TenantScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TenantScope::Tenant => f.write_str("tenant"),
            TenantScope::Database(d) => write!(f, "database {d}"),
            TenantScope::Object(o) => write!(f, "object {o}"),
            TenantScope::Ancestor(a) => write!(f, "ancestor {a}"),
        }
    }
}

type ScopeRows = HashMap<&'static str, Entry>;

/// Estimated bytes of one row and of one scope's table, for the catalog
/// cache's memory bound.
const ROW_BYTES: usize = 96;
const SCOPE_BYTES: usize = 80;

/// A tenant's setting rows at one catalog version. Immutable; shared
/// through `Arc` by the catalog snapshot that holds it.
pub struct TenantSettings {
    registry: &'static Registry,
    tenant: TenantId,
    version: u64,
    ancestry: Arc<[TenantId]>,
    scopes: HashMap<TenantScope, Arc<ScopeRows>>,
    ignored: Arc<BTreeSet<(TenantScope, String)>>,
    rows: usize,
    bytes: usize,
    memo: Memo,
}

impl fmt::Debug for TenantSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TenantSettings")
            .field("tenant", &self.tenant)
            .field("version", &self.version)
            .field("ancestry", &self.ancestry)
            .field("rows", &self.rows)
            .field("ignored", &self.ignored.len())
            .finish_non_exhaustive()
    }
}

/// Equal rows, ancestry and version; the memo is not compared.
impl PartialEq for TenantSettings {
    fn eq(&self, other: &Self) -> bool {
        self.tenant == other.tenant
            && self.version == other.version
            && self.ancestry == other.ancestry
            && self.ignored == other.ignored
            && self.scopes.len() == other.scopes.len()
            && self
                .scopes
                .iter()
                .all(|(s, rows)| other.scopes.get(s).is_some_and(|o| **o == **rows))
    }
}

impl TenantSettings {
    /// No rows: a tenant at version 0.
    #[must_use]
    pub fn empty(registry: &'static Registry, tenant: TenantId) -> Self {
        TenantSettings::builder(registry, tenant, 0).build()
    }

    /// Starts a snapshot with no rows (a full load).
    #[must_use]
    pub fn builder(
        registry: &'static Registry,
        tenant: TenantId,
        version: u64,
    ) -> TenantSettingsBuilder {
        TenantSettingsBuilder {
            registry,
            tenant,
            version,
            ancestry: Arc::from(Vec::new()),
            scopes: HashMap::new(),
            ignored: Arc::default(),
        }
    }

    /// Starts the next snapshot from this one (an incremental load): every
    /// row is kept until the builder changes it.
    #[must_use]
    pub fn edit(&self, version: u64) -> TenantSettingsBuilder {
        TenantSettingsBuilder {
            registry: self.registry,
            tenant: self.tenant,
            version,
            ancestry: Arc::clone(&self.ancestry),
            scopes: self.scopes.clone(),
            ignored: Arc::clone(&self.ignored),
        }
    }

    /// The registry keys are interned through.
    #[must_use]
    pub fn registry(&self) -> &'static Registry {
        self.registry
    }

    /// The tenant.
    #[must_use]
    pub fn tenant(&self) -> TenantId {
        self.tenant
    }

    /// The catalog version the rows are at.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// The tenant's ancestors, root first, not including the tenant.
    #[must_use]
    pub fn ancestry(&self) -> &[TenantId] {
        &self.ancestry
    }

    /// The row of `key` at `scope`.
    #[must_use]
    pub fn entry(&self, scope: TenantScope, key: &str) -> Option<&Entry> {
        self.scopes.get(&scope)?.get(key)
    }

    /// Every row, in no particular order.
    pub fn rows(&self) -> impl Iterator<Item = (TenantScope, &'static str, &Entry)> + '_ {
        self.scopes
            .iter()
            .flat_map(|(&s, rows)| rows.iter().map(move |(&k, e)| (s, k, e)))
    }

    /// Number of rows kept.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows
    }

    /// Whether no row is kept.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Rows ignored at load: keys this binary doesn't know, and values that
    /// aren't setting scalars ("Stored key unknown to this binary: row
    /// ignored and counted").
    #[must_use]
    pub fn ignored_rows(&self) -> usize {
        self.ignored.len()
    }

    /// Estimated memory, for the catalog cache's bound.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// A context for the tenant, at tenant scope, in `dc`; add a database,
    /// object or cluster with [`SettingCtx`]'s `with_*` methods.
    #[must_use]
    pub fn ctx<'a>(&'a self, dc: &'a DcId) -> SettingCtx<'a> {
        SettingCtx::process(dc).for_tenant(self.tenant, &self.ancestry)
    }

    /// The keys whose rows differ between `old` and `self`, at any scope.
    /// Scopes whose rows are shared (untouched by the change) cost
    /// nothing.
    #[must_use]
    pub fn changed_keys(&self, old: &TenantSettings) -> BTreeSet<&'static str> {
        let mut out = BTreeSet::new();
        let empty = ScopeRows::new();
        for (s, rows) in &self.scopes {
            match old.scopes.get(s) {
                Some(o) if Arc::ptr_eq(o, rows) => {}
                Some(o) => diff_rows(o, rows, &mut out),
                None => diff_rows(&empty, rows, &mut out),
            }
        }
        for (s, o) in &old.scopes {
            if !self.scopes.contains_key(s) {
                diff_rows(o, &empty, &mut out);
            }
        }
        if self.ancestry != old.ancestry {
            for (s, rows) in self.scopes.iter().chain(old.scopes.iter()) {
                if matches!(s, TenantScope::Ancestor(_)) {
                    out.extend(rows.keys().copied());
                }
            }
        }
        out
    }

    /// Resolves `key` in `ctx` over these rows and `etcd` (see
    /// [`resolve_layered`](super::resolve_layered)), memoized per
    /// (context, etcd snapshot): this snapshot is one catalog version, so
    /// a result is reused until either side changes.
    ///
    /// # Errors
    /// 39081 if the key is unknown; `Internal` if `ctx` names another
    /// tenant.
    pub fn resolve(
        &self,
        etcd: &MemSettings<'_>,
        key: &str,
        ctx: &SettingCtx<'_>,
    ) -> Result<Resolved> {
        let def = super::def(self.registry, key)?;
        let bound = self.bind(ctx)?;
        let stamp = etcd.stamp();
        if let Some(r) = self.memo.get(def.key(), &bound, stamp) {
            return Ok(r);
        }
        let r = walk(def, &bound, |s| {
            self.lookup(s, def.key())
                .or_else(|| etcd.rows.get(&(s.clone(), def.key())))
        });
        self.memo.put(def.key(), &bound, stamp, &r);
        Ok(r)
    }

    /// Stored values that resolution clamps or ignores, at the scope they
    /// are stored at: what the reconciler counts and `SPRIG_SETTINGS`
    /// flags, and what a write that tightens bounds warns about (chapter
    /// 08: "bounds tightened: warn 'N values will be clamped'"). Ancestor
    /// rows are skipped: they are the ancestors' values, not the tenant's.
    /// `object_db` names an object's database, so its rows apply.
    #[must_use]
    pub fn clamped(
        &self,
        etcd: &MemSettings<'_>,
        dc: &DcId,
        object_db: &dyn Fn(ObjectId) -> Option<DbId>,
    ) -> Vec<ClampedValue> {
        let mut out = Vec::new();
        for (scope, key, entry) in self.rows() {
            let Some(stored) = &entry.value else {
                continue;
            };
            let ctx = self.ctx(dc);
            let ctx = match scope {
                TenantScope::Ancestor(_) => continue,
                TenantScope::Tenant => ctx,
                TenantScope::Database(d) => ctx.with_db(d),
                TenantScope::Object(o) => match object_db(o) {
                    Some(d) => ctx.with_db(d).with_object(o),
                    None => ctx.with_object(o),
                },
            };
            let Ok(r) = super::resolve_layered(etcd, Some(self), key, &ctx) else {
                continue;
            };
            let own = scope.kind();
            if let Some((_, why)) = r.ignored.iter().find(|(k, _)| *k == own) {
                out.push(ClampedValue {
                    scope,
                    key,
                    stored: stored.clone(),
                    effective: None,
                    why: Some(*why),
                });
            } else if r.source == own && r.clamped {
                out.push(ClampedValue {
                    scope,
                    key,
                    stored: stored.clone(),
                    effective: Some(r.value),
                    why: None,
                });
            }
        }
        out.sort_by(|a, b| (a.key, a.scope).cmp(&(b.key, b.scope)));
        out
    }

    /// `ctx` with this tenant and its ancestry.
    pub(crate) fn bind<'a>(&'a self, ctx: &SettingCtx<'a>) -> Result<SettingCtx<'a>> {
        match ctx.tenant {
            Some(t) if t != self.tenant => Err(SprigError::internal(format!(
                "settings context names tenant {t}, the rows are tenant {}'s",
                self.tenant
            ))),
            _ => Ok(SettingCtx {
                tenant: Some(self.tenant),
                ancestry: &self.ancestry,
                ..*ctx
            }),
        }
    }

    /// The row of `key` at `scope` if `scope` is one of this tenant's.
    pub(crate) fn lookup(&self, scope: &Scope, key: &'static str) -> Option<&Entry> {
        let s = match scope {
            Scope::Tenant(t) if *t == self.tenant => TenantScope::Tenant,
            Scope::Tenant(a) => TenantScope::Ancestor(*a),
            Scope::Database(t, d) if *t == self.tenant => TenantScope::Database(*d),
            Scope::Object(t, o) if *t == self.tenant => TenantScope::Object(*o),
            _ => return None,
        };
        self.entry(s, key)
    }

    /// Number of memoized results.
    #[cfg(test)]
    pub(crate) fn memoized(&self) -> usize {
        self.memo.len()
    }
}

fn diff_rows(old: &ScopeRows, new: &ScopeRows, out: &mut BTreeSet<&'static str>) {
    for (k, e) in new {
        if old.get(k) != Some(e) {
            out.insert(k);
        }
    }
    for k in old.keys() {
        if !new.contains_key(k) {
            out.insert(k);
        }
    }
}

/// Builds a [`TenantSettings`]: from nothing ([`TenantSettings::builder`])
/// or from the previous snapshot ([`TenantSettings::edit`]).
pub struct TenantSettingsBuilder {
    registry: &'static Registry,
    tenant: TenantId,
    version: u64,
    ancestry: Arc<[TenantId]>,
    scopes: HashMap<TenantScope, Arc<ScopeRows>>,
    ignored: Arc<BTreeSet<(TenantScope, String)>>,
}

impl fmt::Debug for TenantSettingsBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TenantSettingsBuilder")
            .field("tenant", &self.tenant)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

impl TenantSettingsBuilder {
    /// Applies a row as loaded: an empty entry (a reset, kept until purge)
    /// removes the row; a key the registry doesn't know is ignored and
    /// counted; internal `sprig.` keys are skipped.
    pub fn put(&mut self, scope: TenantScope, key: &str, entry: Entry) {
        if key.starts_with("sprig.") {
            return;
        }
        self.unignore(scope, key);
        let Some(def) = self.registry.get(key) else {
            if entry != Entry::default() {
                Arc::make_mut(&mut self.ignored).insert((scope, key.to_owned()));
            }
            return;
        };
        if entry == Entry::default() {
            self.remove(scope, def.key());
        } else {
            Arc::make_mut(self.scopes.entry(scope).or_default()).insert(def.key(), entry);
        }
    }

    /// Records a row whose value can't be read (not a setting scalar): it
    /// is ignored and counted, and replaces any earlier value of the row.
    pub fn put_invalid(&mut self, scope: TenantScope, key: &str) {
        if key.starts_with("sprig.") {
            return;
        }
        if let Some(def) = self.registry.get(key) {
            self.remove(scope, def.key());
        }
        Arc::make_mut(&mut self.ignored).insert((scope, key.to_owned()));
    }

    /// Removes every row of `scope`: its database or object was dropped.
    pub fn remove_scope(&mut self, scope: TenantScope) {
        self.scopes.remove(&scope);
        if self.ignored.iter().any(|(s, _)| *s == scope) {
            Arc::make_mut(&mut self.ignored).retain(|(s, _)| *s != scope);
        }
    }

    /// Sets the ancestry, root first (from `sprig.ancestry`).
    pub fn set_ancestry(&mut self, ancestry: Vec<TenantId>) {
        if *self.ancestry != *ancestry {
            self.ancestry = Arc::from(ancestry);
        }
    }

    /// The snapshot.
    #[must_use]
    pub fn build(self) -> TenantSettings {
        let mut rows = 0;
        let mut bytes = 0;
        for r in self.scopes.values() {
            rows += r.len();
            bytes += SCOPE_BYTES;
            for e in r.values() {
                bytes += ROW_BYTES + entry_bytes(e);
            }
        }
        bytes += self
            .ignored
            .iter()
            .map(|(_, k)| ROW_BYTES + k.len())
            .sum::<usize>();
        TenantSettings {
            registry: self.registry,
            tenant: self.tenant,
            version: self.version,
            ancestry: self.ancestry,
            scopes: self.scopes,
            ignored: self.ignored,
            rows,
            bytes,
            memo: Memo::default(),
        }
    }

    fn remove(&mut self, scope: TenantScope, key: &'static str) {
        if let Some(rows) = self.scopes.get_mut(&scope) {
            if rows.contains_key(key) {
                Arc::make_mut(rows).remove(key);
            }
            if rows.is_empty() {
                self.scopes.remove(&scope);
            }
        }
    }

    fn unignore(&mut self, scope: TenantScope, key: &str) {
        if self.ignored.is_empty() {
            return;
        }
        let k = (scope, key.to_owned());
        if self.ignored.contains(&k) {
            Arc::make_mut(&mut self.ignored).remove(&k);
        }
    }
}

fn value_bytes(v: &Value) -> usize {
    match v {
        Value::Str(s) => s.len(),
        Value::Bool(_) | Value::Int(_) | Value::Float(_) => 0,
    }
}

fn entry_bytes(e: &Entry) -> usize {
    e.value.as_ref().map_or(0, value_bytes)
        + e.bounds.as_ref().map_or(0, |b| {
            32 + b
                .allowed
                .iter()
                .flatten()
                .map(|v| 24 + value_bytes(v))
                .sum::<usize>()
        })
}

/// A stored value that resolution clamps or ignores
/// ([`TenantSettings::clamped`]).
#[derive(Debug, Clone, PartialEq)]
pub struct ClampedValue {
    /// Where the value is stored.
    pub scope: TenantScope,
    /// The key.
    pub key: &'static str,
    /// The stored value.
    pub stored: Value,
    /// The value read instead when clamped; `None` when ignored.
    pub effective: Option<Value>,
    /// Why it is ignored; `None` when clamped.
    pub why: Option<Ignored>,
}

impl ClampedValue {
    /// The value as a write warning of `tenant` (the form etcd-scope
    /// writes return, [`Warning`]).
    #[must_use]
    pub fn warning(&self, tenant: TenantId) -> Warning {
        let (key, scope) = (self.key, self.scope);
        let message = match (self.why, &self.effective) {
            (Some(Ignored::Locked), _) => {
                format!("{key} is locked by a wider scope: the value at {scope} is ignored")
            }
            (Some(Ignored::NotAllowed), _) => format!(
                "{key}: the value at {scope} is not among the allowed values and is ignored"
            ),
            (Some(Ignored::WrongType), _) => {
                format!("{key}: the value at {scope} has the wrong type and is ignored")
            }
            (None, Some(v)) => format!(
                "{key}: the value at {scope} is outside the bounds in force and is read as {v:?}"
            ),
            (None, None) => format!("{key}: the value at {scope} is clamped"),
        };
        Warning {
            scope: scope.scope(tenant),
            value: self.stored.clone(),
            message,
        }
    }
}

/// Chapter 08's warning for a write that tightens bounds: "N values will be
/// clamped" (with the ignored ones counted apart), or `None` if no value is
/// affected.
#[must_use]
pub fn clamp_summary(values: &[ClampedValue]) -> Option<String> {
    let ignored = values.iter().filter(|v| v.why.is_some()).count();
    let clamped = values.len() - ignored;
    let plural = |n: usize| if n == 1 { "value" } else { "values" };
    match (clamped, ignored) {
        (0, 0) => None,
        (c, 0) => Some(format!("{c} {} will be clamped", plural(c))),
        (0, i) => Some(format!("{i} {} will be ignored", plural(i))),
        (c, i) => Some(format!("{c} {} will be clamped and {i} ignored", plural(c))),
    }
}
