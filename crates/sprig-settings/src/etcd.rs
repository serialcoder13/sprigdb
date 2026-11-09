//! Settings at the `global`, `dc` and `cluster` scopes in etcd (chapter 23,
//! "Setting records" and "Settings: writes, bounds and watches"; build step
//! 8).
//!
//! | Scope | Key | Written by |
//! |---|---|---|
//! | `global` | `/sprig/g/settings/<key>` | global leader |
//! | `dc` | `/sprig/dc/<dc_id>/settings/<key>` | that DC's leader |
//! | `cluster` | `/sprig/dc/<dc_id>/cluster-settings/<cluster_id>/<key>` | that DC's leader |
//!
//! Each value is one versioned JSON record ([`SettingRecord`]):
//!
//! ```json
//! {"v":1, "value":2000, "bounds":{"min":500, "max":20000, "locked":false},
//!  "set_by":"op:kiran", "set_at":"2027-02-11T09:30:00Z", "reason":"larger caches"}
//! ```
//!
//! [`SettingsWriter::set`] follows chapter 23's write algorithm:
//!
//! ```text
//! set(scope, key, value or bounds, principal):
//! 1. reg = registry[key]; unknown -> 39081
//! 2. scope not in reg.scopes -> 39082
//! 3. parse value with reg.type (39084); check bounds of every higher scope
//!    (global, then dc for cluster keys): locked -> 39083, outside -> 39080
//! 4. txn guarded by the leader's token and ModRev(record) (Missing when new)
//! 5. bounds tightened: list lower values now outside them; they are clamped at
//!    read time, and returned as warnings
//! ```
//!
//! The stores are passed in, and every key is built under the given store's
//! tier root, so a test can stand a private DC tier in for the global one.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_common::ids::{ClusterId, DcId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::bootstrap::rfc3339;
use sprig_meta::{
    Cmp, Consistency, Key, Leadership, ListOpts, MetaStore, Op, Record, Revision, Tier, Txn,
    Versioned,
};

use crate::registry::{AnySetting, Registry};
use crate::resolve::{Bounds, Entry};
use crate::scope::{Scope, ScopeKind};
use crate::value::Value;

/// Attempts at a write whose record changed between the read and the
/// guarded transaction.
const WRITE_ATTEMPTS: usize = 10;

/// One stored setting row in etcd (chapter 23, "Setting records").
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SettingRecord {
    /// The value; absent when the record only sets bounds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    /// Bounds on every lower scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bounds: Option<Bounds>,
    /// Who set it, such as `op:kiran`.
    #[serde(default)]
    pub set_by: String,
    /// When, RFC 3339 UTC; for people only.
    #[serde(default)]
    pub set_at: String,
    /// Why.
    #[serde(default)]
    pub reason: String,
}

impl Versioned for SettingRecord {
    const V: u32 = 1;
}

impl SettingRecord {
    /// The row resolution reads.
    #[must_use]
    pub fn entry(&self) -> Entry {
        Entry {
            value: self.value.clone(),
            bounds: self.bounds.clone(),
        }
    }

    /// Decodes a stored record into its row.
    ///
    /// # Errors
    /// Internal if the value isn't a setting record.
    pub fn decode_entry(bytes: &[u8]) -> Result<Entry> {
        Ok(Record::<SettingRecord>::decode(bytes)?.body.entry())
    }
}

/// The stores the etcd scopes live in: the global tier and this process's
/// DC tier. In single-etcd mode both are views of one connection.
#[derive(Clone)]
pub struct SettingsStores {
    /// The global tier (`/sprig/g/`).
    pub global: Arc<dyn MetaStore>,
    /// This DC's tier (`/sprig/dc/<dc_id>/`).
    pub dc: Arc<dyn MetaStore>,
    dc_id: DcId,
}

impl std::fmt::Debug for SettingsStores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SettingsStores")
            .field("global", self.global.tier())
            .field("dc", self.dc.tier())
            .finish_non_exhaustive()
    }
}

impl SettingsStores {
    /// The stores of a process in the DC that `dc`'s tier names.
    ///
    /// # Errors
    /// Internal if `dc` is not a DC tier.
    pub fn new(global: Arc<dyn MetaStore>, dc: Arc<dyn MetaStore>) -> Result<Self> {
        let Tier::Dc(dc_id) = dc.tier().clone() else {
            return Err(SprigError::internal("settings need a DC tier store"));
        };
        Ok(Self { global, dc, dc_id })
    }

    /// This process's DC.
    #[must_use]
    pub fn dc_id(&self) -> &DcId {
        &self.dc_id
    }

    /// `/sprig/g/settings/`.
    ///
    /// # Errors
    /// Internal if the key can't be built (a bug).
    pub fn global_prefix(&self) -> Result<Key> {
        self.global.tier().key("settings/")
    }

    /// `/sprig/dc/<dc_id>/settings/`.
    ///
    /// # Errors
    /// Internal if the key can't be built (a bug).
    pub fn dc_prefix(&self) -> Result<Key> {
        self.dc.tier().key("settings/")
    }

    /// `/sprig/dc/<dc_id>/cluster-settings/`.
    ///
    /// # Errors
    /// Internal if the key can't be built (a bug).
    pub fn cluster_prefix(&self) -> Result<Key> {
        self.dc.tier().key("cluster-settings/")
    }

    /// The etcd key of `key` at `scope`.
    ///
    /// # Errors
    /// Internal for a tenant scope (those live in the catalog), a DC other
    /// than this process's, or a cluster outside it: only that DC's leader
    /// writes them.
    pub fn record_key(&self, scope: &Scope, key: &str) -> Result<Key> {
        match scope {
            Scope::Global => self.global.tier().key(&format!("settings/{key}")),
            Scope::Dc(dc) => {
                self.check_dc(dc)?;
                self.dc.tier().key(&format!("settings/{key}"))
            }
            Scope::Cluster(c) => {
                self.check_cluster(c)?;
                self.dc.tier().key(&format!("cluster-settings/{c}/{key}"))
            }
            Scope::Tenant(_) | Scope::Database(..) | Scope::Object(..) => Err(not_etcd(scope)),
        }
    }

    fn check_dc(&self, dc: &DcId) -> Result<()> {
        if *dc == self.dc_id {
            return Ok(());
        }
        Err(SprigError::internal(format!(
            "dc scope of {dc} is kept by {dc}'s leader, not in {}",
            self.dc_id
        )))
    }

    /// Cluster ids are `<dc_id>-c<n>` (chapter 23, "Encoding rules").
    fn check_cluster(&self, c: &ClusterId) -> Result<()> {
        let ours = c
            .as_str()
            .strip_prefix(self.dc_id.as_str())
            .is_some_and(|rest| rest.starts_with('-'));
        if ours {
            return Ok(());
        }
        Err(SprigError::internal(format!(
            "cluster {c} is not in {}: its settings are kept by its DC's leader",
            self.dc_id
        )))
    }

    fn store(&self, scope: &Scope) -> &Arc<dyn MetaStore> {
        match scope {
            Scope::Global => &self.global,
            _ => &self.dc,
        }
    }

    /// The leader key whose holder writes `scope`.
    fn leader_key(&self, scope: &Scope) -> Result<Key> {
        self.store(scope).tier().key("leader")
    }
}

fn not_etcd(scope: &Scope) -> SprigError {
    SprigError::internal(format!(
        "{scope:?} is a catalog scope: written through the tenant catalog (chapter 08)"
    ))
}

fn scope_kind(scope: &Scope) -> ScopeKind {
    match scope {
        Scope::Global => ScopeKind::Global,
        Scope::Dc(_) => ScopeKind::Dc,
        Scope::Cluster(_) => ScopeKind::Cluster,
        Scope::Tenant(_) => ScopeKind::Tenant,
        Scope::Database(..) => ScopeKind::Database,
        Scope::Object(..) => ScopeKind::Object,
    }
}

/// What a write changes at one scope.
#[derive(Debug, Clone, PartialEq)]
pub enum SettingChange {
    /// Sets the value (`sprigctl config set`).
    Set(Value),
    /// Clears the value (`RESET`).
    Reset,
    /// Sets bounds on every lower scope (`SET BOUNDS`).
    SetBounds(Bounds),
    /// Clears the bounds (`RESET BOUNDS`).
    ResetBounds,
}

/// A lower-scope value that new bounds will clamp or ignore at read time.
#[derive(Debug, Clone, PartialEq)]
pub struct Warning {
    /// Where the value is set.
    pub scope: Scope,
    /// The stored value.
    pub value: Value,
    /// What happens to it.
    pub message: String,
}

/// Writes `global`, `dc` and `cluster` setting records through the scope's
/// leader.
pub struct SettingsWriter {
    registry: &'static Registry,
    stores: SettingsStores,
    clock: Arc<dyn Clock>,
    guard: Option<Arc<dyn crate::ValueGuard>>,
}

impl std::fmt::Debug for SettingsWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SettingsWriter")
            .field("stores", &self.stores)
            .finish_non_exhaustive()
    }
}

impl SettingsWriter {
    /// A writer checking keys against `registry`; `clock` stamps `set_at`.
    #[must_use]
    pub fn new(registry: &'static Registry, stores: SettingsStores, clock: Arc<dyn Clock>) -> Self {
        Self {
            registry,
            stores,
            clock,
            guard: None,
        }
    }

    /// Checks every `Set` with `guard` before writing it
    /// ([`crate::guard`]).
    #[must_use]
    pub fn with_guard(mut self, guard: Arc<dyn crate::ValueGuard>) -> Self {
        self.guard = Some(guard);
        self
    }

    /// Runs the guard on `change`, if it is a `Set`.
    async fn guarded(&self, key: &str, change: &SettingChange) -> Result<()> {
        match (&self.guard, change) {
            (Some(g), SettingChange::Set(v)) => g.check(key, v).await,
            _ => Ok(()),
        }
    }

    /// The stores written to.
    #[must_use]
    pub fn stores(&self) -> &SettingsStores {
        &self.stores
    }

    /// Reads the record of `key` at `scope`, linearizably, with its mod
    /// revision.
    ///
    /// # Errors
    /// As [`SettingsStores::record_key`]; the store's errors; internal for a
    /// record that doesn't decode.
    pub async fn read(
        &self,
        scope: &Scope,
        key: &str,
    ) -> Result<Option<(Record<SettingRecord>, Revision)>> {
        let k = self.stores.record_key(scope, key)?;
        let Some(kv) = self
            .stores
            .store(scope)
            .get(&k, Consistency::Linearizable)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some((Record::decode(&kv.value)?, kv.mod_revision)))
    }

    /// Applies `change` to `key` at `scope`, as `who` for `reason`, through
    /// `leader`: the global leader for `global`, this DC's leader for `dc`
    /// and `cluster`. Returns the lower values that new bounds will clamp.
    ///
    /// # Errors
    /// 39081 unknown key, 39082 scope not accepted, 39084 wrong type, 39083
    /// locked by a wider scope, 39080 outside the wider bounds; internal for
    /// the wrong leader or a scope this process doesn't keep; the leader's
    /// error when it no longer leads; 39500 (retryable) when the record kept
    /// changing under the write, or the store's errors; the guard's refusal
    /// ([`with_guard`](Self::with_guard)).
    pub async fn set(
        &self,
        leader: &Leadership,
        scope: &Scope,
        key: &str,
        change: SettingChange,
        who: &str,
        reason: &str,
    ) -> Result<Vec<Warning>> {
        let def = self.target(scope, key)?;
        let want = self.stores.leader_key(scope)?;
        if leader.key() != &want {
            return Err(SprigError::internal(format!(
                "{} settings are written by the leader at {want}, not {}",
                scope_kind(scope),
                leader.key()
            )));
        }
        let change = check_change(def, change)?;
        self.guarded(key, &change).await?;
        let edit = SettingEdit::Change(change);
        Ok(self
            .apply(Commit::Leader(leader), def, scope, &edit, who, reason)
            .await?
            .warnings)
    }

    /// Applies `edit` to `key` at `scope` like [`set`](Self::set), but
    /// guarded by the compares in `guard` instead of a [`Leadership`] held
    /// by this process: the Admin API passes the fencing compare of the
    /// leader it runs on (`CreateRev(leader key, token)`). A bounds patch is
    /// applied to the bounds read inside the guarded retry loop, so two
    /// concurrent patches don't lose each other's parts.
    ///
    /// R0 (single-etcd mode, chapter 23): `global` records may be written
    /// under the DC leader's guard, as the DC leader writes the tenant
    /// directory; from R2 the guard is the global leader's.
    ///
    /// # Errors
    /// As [`set`](Self::set); 39504 `ER_SPRIG_NOT_LEADER` when a compare in
    /// `guard` no longer holds.
    pub async fn set_guarded(
        &self,
        guard: &[Cmp],
        scope: &Scope,
        key: &str,
        edit: SettingEdit,
        who: &str,
        reason: &str,
    ) -> Result<RecordOutcome> {
        let def = self.target(scope, key)?;
        let edit = match edit {
            SettingEdit::Change(c) => {
                let c = check_change(def, c)?;
                self.guarded(key, &c).await?;
                SettingEdit::Change(c)
            }
            p @ SettingEdit::PatchBounds(_) => p,
        };
        self.apply(Commit::Guard(guard), def, scope, &edit, who, reason)
            .await
    }

    /// The declaration of `key`, checked against `scope`, whose record key
    /// this process can build.
    fn target(&self, scope: &Scope, key: &str) -> Result<&'static dyn AnySetting> {
        let def = self.def(key)?;
        let kind = scope_kind(scope);
        if !def.scopes().contains(kind) {
            return Err(SprigError::new(
                SprigCode::SettingScopeNotAllowed,
                format!("{key} can't be set at {kind} scope"),
            ));
        }
        self.stores.record_key(scope, key)?;
        Ok(def)
    }

    /// The write itself: read, check against the wider bounds, guarded
    /// transaction, retried while the record changes under it.
    async fn apply(
        &self,
        commit: Commit<'_>,
        def: &'static dyn AnySetting,
        scope: &Scope,
        edit: &SettingEdit,
        who: &str,
        reason: &str,
    ) -> Result<RecordOutcome> {
        let key = def.key();
        let kind = scope_kind(scope);
        let record_key = self.stores.record_key(scope, key)?;
        for _ in 0..WRITE_ATTEMPTS {
            let wider = self.wider_bounds(def, scope).await?;
            let current = self.read(scope, key).await?;
            let change = match edit {
                SettingEdit::Change(c) => c.clone(),
                SettingEdit::PatchBounds(p) => {
                    let stored = current.as_ref().and_then(|(r, _)| r.body.bounds.as_ref());
                    match p.apply(stored) {
                        Some(b) => check_change(def, SettingChange::SetBounds(b))?,
                        None => SettingChange::ResetBounds,
                    }
                }
            };
            if let SettingChange::Set(v) = &change {
                if wider.locked {
                    return Err(locked(key));
                }
                if !wider.admits(v) {
                    return Err(SprigError::new(
                        SprigCode::SettingOutOfBounds,
                        format!(
                            "{key}: {} is outside the bounds in force ({})",
                            show_value(v),
                            show_bounds(&wider)
                        ),
                    ));
                }
            }
            if matches!(change, SettingChange::SetBounds(_)) && wider.locked {
                return Err(locked(key));
            }
            let previous = current.as_ref().map(|(r, _)| r.body.clone());
            let (mut rec, guard) = match current {
                Some((rec, rev)) => (rec, Cmp::ModRev(record_key.clone(), rev)),
                None => (
                    Record::new(SettingRecord::default()),
                    Cmp::Missing(record_key.clone()),
                ),
            };
            if !rec.writable() {
                return Err(SprigError::internal(format!(
                    "{record_key} has schema version {}: this build won't rewrite it",
                    rec.schema_version()
                )));
            }
            match &change {
                SettingChange::Set(v) => rec.body.value = Some(v.clone()),
                SettingChange::Reset => rec.body.value = None,
                SettingChange::SetBounds(b) => rec.body.bounds = Some(b.clone()),
                SettingChange::ResetBounds => rec.body.bounds = None,
            }
            rec.body.set_by = who.to_owned();
            rec.body.set_at = rfc3339(self.clock.wall());
            rec.body.reason = reason.to_owned();
            let emptied = rec.body.value.is_none() && rec.body.bounds.is_none();
            let op = if emptied {
                Op::Delete(record_key.clone())
            } else {
                Op::Put {
                    key: record_key.clone(),
                    value: rec.encode()?,
                    lease: None,
                }
            };
            // Nothing to delete: done.
            if emptied && matches!(guard, Cmp::Missing(_)) {
                return Ok(RecordOutcome::default());
            }
            let committed = match commit {
                Commit::Leader(l) => l.commit_if(vec![guard], vec![op]).await?.succeeded,
                Commit::Guard(g) => self.commit_guarded(scope, g, guard, op).await?,
            };
            if !committed {
                tracing::debug!(%record_key, "setting record changed under the write; retrying");
                continue;
            }
            tracing::info!(event = "settings.set", scope = %kind, key, who, reason, change = ?change, "setting changed");
            let warnings = match &change {
                SettingChange::SetBounds(b) => {
                    self.lower_outside(def, scope, &wider.intersect(b)).await?
                }
                _ => Vec::new(),
            };
            return Ok(RecordOutcome {
                previous,
                current: (!emptied).then_some(rec.body),
                warnings,
                changed: true,
            });
        }
        Err(SprigError::new(
            SprigCode::MetaUnavailable,
            format!("{record_key} kept changing; try again"),
        ))
    }

    /// One guarded transaction: `true` if it committed, `false` if the
    /// record changed (retry); 39504 if `guard` no longer holds.
    async fn commit_guarded(
        &self,
        scope: &Scope,
        guard: &[Cmp],
        record: Cmp,
        op: Op,
    ) -> Result<bool> {
        let store = self.stores.store(scope);
        let mut when = guard.to_vec();
        when.push(record);
        let r = store
            .txn(Txn {
                when,
                then: vec![op],
                otherwise: Vec::new(),
            })
            .await?;
        if r.succeeded {
            return Ok(true);
        }
        if guard.is_empty() {
            return Ok(false);
        }
        let still = store
            .txn(Txn {
                when: guard.to_vec(),
                then: Vec::new(),
                otherwise: Vec::new(),
            })
            .await?;
        if still.succeeded {
            return Ok(false);
        }
        Err(SprigError::new(
            SprigCode::NotLeader,
            "this controller no longer leads; send the request to the current leader",
        ))
    }

    fn def(&self, key: &str) -> Result<&'static dyn AnySetting> {
        self.registry
            .get(key)
            .filter(|_| !key.starts_with("sprig."))
            .ok_or_else(|| {
                SprigError::new(SprigCode::SettingUnknown, format!("unknown setting {key}"))
            })
    }

    async fn bounds_at(&self, scope: &Scope, key: &str) -> Result<Option<Bounds>> {
        Ok(self
            .read(scope, key)
            .await?
            .and_then(|(rec, _)| rec.body.bounds))
    }

    /// Hard bounds and the bounds of every scope wider than `scope`.
    async fn wider_bounds(&self, def: &dyn AnySetting, scope: &Scope) -> Result<Bounds> {
        let mut b: Bounds = def.hard().into();
        let key = def.key();
        let mut wider = Vec::new();
        match scope {
            Scope::Global => {}
            Scope::Dc(_) => wider.push(Scope::Global),
            Scope::Cluster(_) => {
                wider.push(Scope::Global);
                wider.push(Scope::Dc(self.stores.dc_id.clone()));
            }
            other => return Err(not_etcd(other)),
        }
        for s in wider {
            if !def.scopes().contains(scope_kind(&s)) {
                continue;
            }
            if let Some(sb) = self.bounds_at(&s, key).await? {
                b = b.intersect(&sb);
            }
        }
        Ok(b)
    }

    /// Lower values in this DC that `bounds` (now in force below `scope`)
    /// will clamp or ignore.
    async fn lower_outside(
        &self,
        def: &dyn AnySetting,
        scope: &Scope,
        bounds: &Bounds,
    ) -> Result<Vec<Warning>> {
        let key = def.key();
        let mut lower: Vec<(Scope, Option<Value>)> = Vec::new();
        if matches!(scope, Scope::Global) && def.scopes().contains(ScopeKind::Dc) {
            let dc = Scope::Dc(self.stores.dc_id.clone());
            let v = self.read(&dc, key).await?.and_then(|(r, _)| r.body.value);
            lower.push((dc, v));
        }
        if matches!(scope, Scope::Global | Scope::Dc(_))
            && def.scopes().contains(ScopeKind::Cluster)
        {
            lower.extend(self.cluster_values(key).await?);
        }
        let mut out = Vec::new();
        for (s, v) in lower {
            let Some(v) = v.and_then(|v| def.normalize(&v)) else {
                continue;
            };
            let message = if bounds.locked {
                format!("{key} is now locked: the value at {s:?} will be ignored")
            } else if !bounds.admits(&v) {
                format!("{key}: the value at {s:?} is outside the new bounds and will be clamped")
            } else {
                continue;
            };
            out.push(Warning {
                scope: s,
                value: v,
                message,
            });
        }
        Ok(out)
    }

    /// Every cluster value of `key` in this DC.
    async fn cluster_values(&self, key: &str) -> Result<Vec<(Scope, Option<Value>)>> {
        let prefix = self.stores.cluster_prefix()?;
        let suffix = format!("/{key}");
        let mut out = Vec::new();
        let mut opts = ListOpts::default();
        loop {
            let page = self.stores.dc.list(&prefix, opts.clone()).await?;
            for kv in &page.kvs {
                let Some(rest) = kv.key.as_str().strip_prefix(prefix.as_str()) else {
                    continue;
                };
                let Some(cluster) = rest.strip_suffix(&suffix) else {
                    continue;
                };
                let Ok(c) = ClusterId::new(cluster) else {
                    continue;
                };
                match Record::<SettingRecord>::decode(&kv.value) {
                    Ok(r) => out.push((Scope::Cluster(c), r.body.value)),
                    Err(e) => tracing::warn!(key = %kv.key, error = %e, "bad setting record"),
                }
            }
            if !page.more {
                return Ok(out);
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
    }
}

/// `change` checked against `def`'s type: a value in the key's canonical
/// form, or bounds whose numbers aren't NaN and whose allowed values fit
/// the type. The tenant-scope writer (`sprig-catalog::settings`) uses the
/// same check as [`SettingsWriter::set`].
///
/// # Errors
/// 39084 `ER_SPRIG_SETTING_BAD_VALUE`.
pub fn normalize_change(def: &dyn AnySetting, change: SettingChange) -> Result<SettingChange> {
    check_change(def, change)
}

/// Normalizes a value or checks bounds against the key's type: 39084.
fn check_change(def: &dyn AnySetting, change: SettingChange) -> Result<SettingChange> {
    let key = def.key();
    match change {
        SettingChange::Set(v) => def.normalize(&v).map(SettingChange::Set).ok_or_else(|| {
            SprigError::new(
                SprigCode::SettingBadValue,
                format!("{key}: {v:?} is not a {:?}", def.kind()),
            )
        }),
        SettingChange::SetBounds(b) => {
            if [b.min, b.max].iter().flatten().any(|x| x.is_nan()) {
                return Err(SprigError::new(
                    SprigCode::SettingBadValue,
                    format!("{key}: NaN bound"),
                ));
            }
            if let Some(bad) = b
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
            Ok(SettingChange::SetBounds(b))
        }
        other => Ok(other),
    }
}

fn locked(key: &str) -> SprigError {
    SprigError::new(
        SprigCode::SettingLocked,
        format!("{key} is locked by a wider scope"),
    )
}

/// How [`SettingsWriter`] commits: through a leadership this process
/// holds, or under compares the caller supplies.
#[derive(Clone, Copy)]
enum Commit<'a> {
    Leader(&'a Leadership),
    Guard(&'a [Cmp]),
}

/// A change of one scope's bounds that keeps what it doesn't name
/// (`sprigctl settings bounds|lock|unlock`). Each field that is `Some`
/// replaces that part of the stored bounds: `min: Some(None)` clears the
/// minimum, `min: Some(Some(3.0))` sets it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BoundsPatch {
    /// The new minimum, or `Some(None)` to clear it.
    pub min: Option<Option<f64>>,
    /// The new maximum, or `Some(None)` to clear it.
    pub max: Option<Option<f64>>,
    /// The new allowed list, or `Some(None)` to clear it.
    pub allowed: Option<Option<Vec<Value>>>,
    /// Lock or unlock narrower scopes.
    pub locked: Option<bool>,
}

impl BoundsPatch {
    /// Whether the patch changes nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.min.is_none() && self.max.is_none() && self.allowed.is_none() && self.locked.is_none()
    }

    /// The bounds after applying the patch to `current`; `None` when
    /// nothing is left (no minimum, maximum or allowed list, and unlocked),
    /// which is a reset of the bounds.
    #[must_use]
    pub fn apply(&self, current: Option<&Bounds>) -> Option<Bounds> {
        let mut b = current.cloned().unwrap_or_default();
        if let Some(m) = self.min {
            b.min = m;
        }
        if let Some(m) = self.max {
            b.max = m;
        }
        if let Some(a) = &self.allowed {
            b.allowed.clone_from(a);
        }
        if let Some(l) = self.locked {
            b.locked = l;
        }
        (b != Bounds::default()).then_some(b)
    }
}

/// What [`SettingsWriter::set_guarded`] changes.
#[derive(Debug, Clone, PartialEq)]
pub enum SettingEdit {
    /// A whole change, as [`SettingsWriter::set`] takes.
    Change(SettingChange),
    /// A partial change of the bounds, applied to the stored bounds inside
    /// the guarded write.
    PatchBounds(BoundsPatch),
}

impl From<SettingChange> for SettingEdit {
    fn from(c: SettingChange) -> Self {
        SettingEdit::Change(c)
    }
}

/// What [`SettingsWriter::set_guarded`] did.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RecordOutcome {
    /// The record before the write.
    pub previous: Option<SettingRecord>,
    /// The record after it; `None` when it was deleted (nothing left).
    pub current: Option<SettingRecord>,
    /// Lower values new bounds will clamp or ignore.
    pub warnings: Vec<Warning>,
    /// `false` when there was nothing to change (a reset of a record that
    /// doesn't exist).
    pub changed: bool,
}

/// A value as an operator writes it: `8`, `true`, `'on'`.
#[must_use]
pub fn show_value(v: &Value) -> String {
    match v {
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Str(s) => format!("'{s}'"),
    }
}

/// Bounds as an operator reads them: `min 1, max 8, allowed ('on',
/// 'off'), locked`, or `none`.
#[must_use]
pub fn show_bounds(b: &Bounds) -> String {
    let mut parts = Vec::new();
    if let Some(m) = b.min {
        parts.push(format!("min {m}"));
    }
    if let Some(m) = b.max {
        parts.push(format!("max {m}"));
    }
    if let Some(a) = &b.allowed {
        parts.push(format!(
            "allowed ({})",
            a.iter().map(show_value).collect::<Vec<_>>().join(", ")
        ));
    }
    if b.locked {
        parts.push("locked".to_owned());
    }
    if parts.is_empty() {
        "none".to_owned()
    } else {
        parts.join(", ")
    }
}
