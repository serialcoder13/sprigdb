//! Bounds, resolution and an in-memory store of setting rows (chapter 08,
//! "Setting resolution" and "Setting a value").
//!
//! [`MemSettings`] keeps every scope's rows in memory. The etcd mirror
//! ([`LiveSettings`](crate::watch::LiveSettings): global, dc, cluster)
//! builds one as its snapshot.
//!
//! Six-scope resolution (chapter 08, build step 5) layers a tenant's rows
//! over it: [`TenantSettings`] holds one tenant's tenant, database, object
//! and ancestor rows at one catalog version (built by the catalog cache),
//! and [`resolve_layered`] walks global, dc and cluster from the etcd
//! snapshot, then ancestors, tenant, database and object from the tenant's
//! rows, with one algorithm for both. [`Settings`] is the process-wide
//! entry point: memoized resolution per (tenant snapshot version, etcd
//! snapshot), key watches across all six scopes, and watches on resolved
//! values ([`ResolvedWatch`]).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use sprig_common::{Result, SprigCode, SprigError};

use crate::registry::{AnySetting, HardBounds, Registry, Setting};
use crate::scope::{Level, Scope, ScopeKind, SettingCtx};
use crate::value::{SettingValue, Value};

mod live;
mod memo;
mod tenant;
#[cfg(test)]
mod tests;

pub use live::{OwnedCtx, ResolvedWatch, Settings, TenantNotify};
pub use tenant::{ClampedValue, TenantScope, TenantSettings, TenantSettingsBuilder, clamp_summary};

/// Stamps of [`MemSettings`] contents: every store and every change to one
/// gets a new stamp, so a memoized result can name exactly the rows it was
/// computed from.
static STAMPS: AtomicU64 = AtomicU64::new(1);

fn next_stamp() -> u64 {
    STAMPS.fetch_add(1, Ordering::Relaxed)
}

/// Bounds a scope sets on narrower scopes.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Bounds {
    /// Smallest numeric value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    /// Largest numeric value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    /// The only values accepted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed: Option<Vec<Value>>,
    /// Narrower scopes can't set the key.
    #[serde(default)]
    pub locked: bool,
}

impl Bounds {
    /// At most `max`.
    #[must_use]
    pub fn max(max: f64) -> Self {
        Self {
            max: Some(max),
            ..Self::default()
        }
    }

    /// Only the listed values.
    #[must_use]
    pub fn allowed(values: impl IntoIterator<Item = Value>) -> Self {
        Self {
            allowed: Some(values.into_iter().collect()),
            ..Self::default()
        }
    }

    /// Narrower scopes can't set the key.
    #[must_use]
    pub fn locked() -> Self {
        Self {
            locked: true,
            ..Self::default()
        }
    }

    /// `self ∩ other`: the larger minimum, the smaller maximum, the common
    /// allowed values; locked if either is.
    #[must_use]
    pub fn intersect(&self, other: &Bounds) -> Bounds {
        let pick = |a: Option<f64>, b: Option<f64>, f: fn(f64, f64) -> f64| match (a, b) {
            (Some(a), Some(b)) => Some(f(a, b)),
            (a, b) => a.or(b),
        };
        let allowed = match (&self.allowed, &other.allowed) {
            (Some(a), Some(b)) => Some(a.iter().filter(|v| b.contains(v)).cloned().collect()),
            (a, b) => a.clone().or_else(|| b.clone()),
        };
        Bounds {
            min: pick(self.min, other.min, f64::max),
            max: pick(self.max, other.max, f64::min),
            allowed,
            locked: self.locked || other.locked,
        }
    }

    /// Whether `v` is inside these bounds.
    #[must_use]
    pub fn admits(&self, v: &Value) -> bool {
        self.allowed_contains(v) && self.clamp(v) == *v
    }

    fn allowed_contains(&self, v: &Value) -> bool {
        self.allowed.as_ref().is_none_or(|a| a.contains(v))
    }

    /// `v` moved into `[min, max]`; non-numeric values are returned as is.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // f64 bounds clamp i64 values (chapter 08)
    pub fn clamp(&self, v: &Value) -> Value {
        let lo = self.min.unwrap_or(f64::NEG_INFINITY);
        let hi = self.max.unwrap_or(f64::INFINITY);
        match v {
            Value::Int(i) => {
                let x = v.as_f64().unwrap_or_default();
                if x < lo {
                    Value::Int(lo.ceil() as i64)
                } else if x > hi {
                    Value::Int(hi.floor() as i64)
                } else {
                    Value::Int(*i)
                }
            }
            Value::Float(f) => Value::Float(f.clamp(lo, hi.max(lo))),
            other => other.clone(),
        }
    }
}

impl From<HardBounds> for Bounds {
    fn from(h: HardBounds) -> Self {
        Bounds {
            min: h.min,
            max: h.max,
            allowed: h
                .allowed
                .map(|a| a.iter().map(|&s| Value::from(s)).collect()),
            locked: false,
        }
    }
}

/// One scope's row for one key: a value, bounds on narrower scopes, or both.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    /// The value set at this scope.
    pub value: Option<Value>,
    /// Bounds this scope sets on narrower scopes.
    pub bounds: Option<Bounds>,
}

/// Why a stored value was not used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ignored {
    /// A wider scope locked the key.
    Locked,
    /// Not in a wider scope's allowed list.
    NotAllowed,
    /// The stored value doesn't fit the key's type (written by another
    /// binary version).
    WrongType,
}

/// A resolved key, with where its value came from.
#[derive(Debug, Clone, PartialEq)]
pub struct Resolved {
    /// The effective value.
    pub value: Value,
    /// The scope that supplied it.
    pub source: ScopeKind,
    /// The bounds in force after every scope of the context.
    pub bounds: Bounds,
    /// The value was moved into the bounds in force at its scope.
    pub clamped: bool,
    /// Values that were skipped, and why.
    pub ignored: Vec<(ScopeKind, Ignored)>,
}

/// Setting rows of every scope, in memory.
#[derive(Debug)]
pub struct MemSettings<'r> {
    registry: &'r Registry,
    rows: HashMap<(Scope, &'static str), Entry>,
    stamp: u64,
}

impl<'r> MemSettings<'r> {
    /// An empty store over `registry`.
    #[must_use]
    pub fn new(registry: &'r Registry) -> Self {
        Self {
            registry,
            rows: HashMap::new(),
            stamp: next_stamp(),
        }
    }

    /// Identifies these rows: unique per store, and changed by every write,
    /// so results memoized against one stamp are never served for other
    /// rows.
    #[must_use]
    pub fn stamp(&self) -> u64 {
        self.stamp
    }

    /// The row of `key` at `scope`, if any.
    #[must_use]
    pub fn entry(&self, scope: &Scope, key: &str) -> Option<&Entry> {
        let def = self.registry.get(key)?;
        self.rows.get(&(scope.clone(), def.key()))
    }

    /// The registry the rows are checked against.
    #[must_use]
    pub fn registry(&self) -> &'r Registry {
        self.registry
    }

    /// Stores a row as read from a store, without the write checks: rows in
    /// etcd were checked when written, and resolution skips values that no
    /// longer fit. Keys missing from the registry are dropped.
    pub(crate) fn insert_row(&mut self, scope: Scope, key: &str, entry: Entry) {
        if let Some(def) = self.registry.get(key) {
            self.rows.insert((scope, def.key()), entry);
            self.stamp = next_stamp();
        }
    }

    /// Every row.
    pub(crate) fn rows(&self) -> &HashMap<(Scope, &'static str), Entry> {
        &self.rows
    }

    /// Resolves `key` in `ctx`, following chapter 08's algorithm: walk the
    /// chain from the widest scope to the narrowest, so the nearest value
    /// wins and the bounds of every wider scope apply to it.
    ///
    /// # Errors
    /// 39081 if the key is unknown.
    pub fn resolve(&self, key: &str, ctx: &SettingCtx<'_>) -> Result<Resolved> {
        let def = self.def(key)?;
        Ok(walk(def, ctx, |scope| {
            self.rows.get(&(scope.clone(), def.key()))
        }))
    }

    /// The typed value of `setting` in `ctx`, or its default if resolution
    /// fails (a key missing from this registry).
    #[must_use]
    pub fn get<T: SettingValue>(&self, setting: &Setting<T>, ctx: &SettingCtx<'_>) -> T {
        self.resolve(setting.key, ctx)
            .ok()
            .and_then(|r| T::from_value(&r.value))
            .unwrap_or_else(|| setting.default.clone())
    }

    /// Sets `key` to `value` at `level` of `ctx`.
    ///
    /// # Errors
    /// 39081 unknown key, 39082 scope not accepted, 39084 wrong type,
    /// 39083 locked by a wider scope, 39080 outside the bounds in force.
    pub fn set_value(
        &mut self,
        level: Level,
        ctx: &SettingCtx<'_>,
        key: &str,
        value: &Value,
    ) -> Result<()> {
        let (def, scope) = self.target(level, ctx, key)?;
        let v = def.normalize(value).ok_or_else(|| {
            SprigError::new(
                SprigCode::SettingBadValue,
                format!("{key}: {value:?} is not a {:?}", def.kind()),
            )
        })?;
        let wider = self.wider_bounds(def, ctx, level);
        if wider.locked {
            return Err(locked(key));
        }
        if !wider.admits(&v) {
            return Err(SprigError::new(
                SprigCode::SettingOutOfBounds,
                format!("{key}: {v:?} is outside the bounds in force {wider:?}"),
            ));
        }
        self.rows.entry((scope, def.key())).or_default().value = Some(v);
        self.stamp = next_stamp();
        Ok(())
    }

    /// Sets bounds on `key` for scopes narrower than `level`.
    ///
    /// # Errors
    /// 39081, 39082 and 39083 as for [`set_value`](Self::set_value); 39084
    /// if an allowed value doesn't fit the key's type.
    pub fn set_bounds(
        &mut self,
        level: Level,
        ctx: &SettingCtx<'_>,
        key: &str,
        bounds: Bounds,
    ) -> Result<()> {
        let (def, scope) = self.target(level, ctx, key)?;
        if [bounds.min, bounds.max]
            .iter()
            .flatten()
            .any(|x| x.is_nan())
        {
            return Err(SprigError::new(
                SprigCode::SettingBadValue,
                format!("{key}: NaN bound"),
            ));
        }
        if let Some(bad) = bounds
            .allowed
            .iter()
            .flatten()
            .find(|v| def.normalize(v).is_none())
        {
            return Err(SprigError::new(
                SprigCode::SettingBadValue,
                format!("{key}: allowed value {bad:?} is not a {:?}", def.kind()),
            ));
        }
        if self.wider_bounds(def, ctx, level).locked {
            return Err(locked(key));
        }
        self.rows.entry((scope, def.key())).or_default().bounds = Some(bounds);
        self.stamp = next_stamp();
        Ok(())
    }

    /// Clears the value at `level` (`RESET`). An empty row is deleted.
    ///
    /// # Errors
    /// 39081 and 39082 as for [`set_value`](Self::set_value).
    pub fn reset_value(&mut self, level: Level, ctx: &SettingCtx<'_>, key: &str) -> Result<()> {
        self.clear(level, ctx, key, |e| e.value = None)
    }

    /// Clears the bounds at `level` (`RESET BOUNDS`). An empty row is deleted.
    ///
    /// # Errors
    /// 39081 and 39082 as for [`set_value`](Self::set_value).
    pub fn reset_bounds(&mut self, level: Level, ctx: &SettingCtx<'_>, key: &str) -> Result<()> {
        self.clear(level, ctx, key, |e| e.bounds = None)
    }

    fn clear(
        &mut self,
        level: Level,
        ctx: &SettingCtx<'_>,
        key: &str,
        f: fn(&mut Entry),
    ) -> Result<()> {
        let (def, scope) = self.target(level, ctx, key)?;
        let k = (scope, def.key());
        if let Some(e) = self.rows.get_mut(&k) {
            f(e);
            if *e == Entry::default() {
                self.rows.remove(&k);
            }
            self.stamp = next_stamp();
        }
        Ok(())
    }

    fn def(&self, key: &str) -> Result<&'static dyn AnySetting> {
        def(self.registry, key)
    }

    /// Checks the key and the level, and returns the row address.
    fn target(
        &self,
        level: Level,
        ctx: &SettingCtx<'_>,
        key: &str,
    ) -> Result<(&'static dyn AnySetting, Scope)> {
        let def = self.def(key)?;
        let not_allowed = || {
            SprigError::new(
                SprigCode::SettingScopeNotAllowed,
                format!("{key} can't be set at {} scope", level.kind()),
            )
        };
        if !def.scopes().contains(level.kind()) {
            return Err(not_allowed());
        }
        let scope = ctx
            .chain()
            .into_iter()
            .find(|(k, _)| *k == level.kind())
            .map(|(_, s)| s)
            .ok_or_else(not_allowed)?;
        Ok((def, scope))
    }

    /// The bounds of every scope wider than `level` in `ctx`, hard bounds
    /// included.
    fn wider_bounds(&self, def: &dyn AnySetting, ctx: &SettingCtx<'_>, level: Level) -> Bounds {
        let mut b: Bounds = def.hard().into();
        for (kind, scope) in chain(def, ctx) {
            if kind == level.kind() {
                break;
            }
            if let Some(eb) = self
                .rows
                .get(&(scope, def.key()))
                .and_then(|e| e.bounds.as_ref())
            {
                b = b.intersect(eb);
            }
        }
        b
    }
}

/// The declaration of `key`: 39081 if unknown or internal (`sprig.`).
pub(crate) fn def(registry: &Registry, key: &str) -> Result<&'static dyn AnySetting> {
    registry
        .get(key)
        .filter(|_| !key.starts_with("sprig."))
        .ok_or_else(|| SprigError::new(SprigCode::SettingUnknown, format!("unknown setting {key}")))
}

/// Chapter 08's resolution of `def` in `ctx`, reading each scope's row
/// through `entry`: walk the chain from the widest scope to the narrowest,
/// so the nearest value wins and the bounds of every wider scope apply to
/// it.
///
/// ```text
/// value, source = def.default, default;  B = def.hard_bounds
/// for s in chain:
///   e = entry(s, key);  if none: continue
///   if B.locked: record ignored(s, locked) if e.value; continue
///   if e.value:
///     if B.allowed and e.value not in B.allowed: record ignored(s, not_allowed)
///     else: v = clamp(e.value, B.min, B.max); clamped = v != e.value
///           value, source = v, s
///   if e.bounds: B = B ∩ e.bounds
/// if source = default: value = clamp(def.default, B.min, B.max)
/// ```
pub(crate) fn walk<'e>(
    def: &dyn AnySetting,
    ctx: &SettingCtx<'_>,
    entry: impl Fn(&Scope) -> Option<&'e Entry>,
) -> Resolved {
    let mut r = Resolved {
        value: def.default_value(),
        source: ScopeKind::Default,
        bounds: def.hard().into(),
        clamped: false,
        ignored: Vec::new(),
    };
    for (kind, scope) in chain(def, ctx) {
        let Some(e) = entry(&scope) else {
            continue;
        };
        if r.bounds.locked {
            if e.value.is_some() {
                r.ignored.push((kind, Ignored::Locked));
            }
            continue;
        }
        if let Some(v) = &e.value {
            match def.normalize(v) {
                None => r.ignored.push((kind, Ignored::WrongType)),
                Some(v) if !r.bounds.allowed_contains(&v) => {
                    r.ignored.push((kind, Ignored::NotAllowed));
                }
                Some(v) => {
                    let c = r.bounds.clamp(&v);
                    r.clamped = c != v;
                    r.value = c;
                    r.source = kind;
                }
            }
        }
        if let Some(b) = &e.bounds {
            r.bounds = r.bounds.intersect(b);
        }
    }
    // A built-in default that falls outside bounds tightened by some scope
    // is clamped like any other value (chapter 08).
    if r.source == ScopeKind::Default {
        let c = r.bounds.clamp(&r.value);
        r.clamped = c != r.value;
        r.value = c;
    }
    r
}

/// Resolves `key` in `ctx` over two layers: global, dc and cluster rows
/// from `etcd` (the process's mirror snapshot), and the tenant, database,
/// object and ancestor rows of `tenant` (its catalog snapshot). Without
/// `tenant`, or for scopes of another tenant, every row comes from `etcd`.
/// Not memoized: [`Settings::resolve`] and [`TenantSettings::resolve`] are.
///
/// When `tenant` is given, the context's tenant must be `None` or the same
/// tenant, and its ancestry is the tenant's ([`TenantSettings::ancestry`]).
///
/// # Errors
/// 39081 if the key is unknown; `Internal` if `ctx` names another tenant.
pub fn resolve_layered(
    etcd: &MemSettings<'_>,
    tenant: Option<&TenantSettings>,
    key: &str,
    ctx: &SettingCtx<'_>,
) -> Result<Resolved> {
    let def = def(etcd.registry, key)?;
    let Some(ts) = tenant else {
        return Ok(walk(def, ctx, |s| etcd.rows.get(&(s.clone(), def.key()))));
    };
    let ctx = ts.bind(ctx)?;
    Ok(walk(def, &ctx, |s| {
        ts.lookup(s, def.key())
            .or_else(|| etcd.rows.get(&(s.clone(), def.key())))
    }))
}

/// The chain of `ctx`, keeping the scopes `def` accepts.
fn chain(def: &dyn AnySetting, ctx: &SettingCtx<'_>) -> Vec<(ScopeKind, Scope)> {
    let scopes = def.scopes();
    ctx.chain()
        .into_iter()
        .filter(|(k, _)| scopes.contains(*k))
        .collect()
}

fn locked(key: &str) -> SprigError {
    SprigError::new(
        SprigCode::SettingLocked,
        format!("{key} is locked by a wider scope"),
    )
}
