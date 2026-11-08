//! Setting writes at tenant, database and object scope (chapter 08,
//! "Setting a value"; build step 7).
//!
//! ```text
//! set(scope, key, change, principal):           // SQL, MongoDB, admin API, sprigctl
//!   1. def = registry[key]; unknown or 'sprig.' → 39081
//!   2. scope not in def.scopes → 39082; operator_only key and a tenant principal →
//!      access denied (chapter 29)
//!   3. privileges: object → ALTER on it; database → ALTER on it; tenant scope or any
//!      bounds → SPRIG_SETTINGS_ADMIN, or SPRIG_TENANT_ADMIN on an ancestor (chapter 29);
//!      ancestor rows → never
//!   4. parse with def.type → 39084
//!   5. bounds of every wider scope for the setter's context: locked → 39083,
//!      outside → 39080
//!   6. tenant scopes: change(tenant, Catalog), upsert or delete the row
//!   7. bounds tightened: warn "N values will be clamped"; record a decision
//! ```
//!
//! [`TenantSettingsWriter::set`] runs these steps for the three catalog
//! scopes; the etcd scopes are `sprig_settings::SettingsWriter`'s.
//!
//! - **Privileges** are chapter 29's (M0.5): the writer asks a
//!   [`SettingAuthorizer`] the caller passes, with the [`Privilege`] the
//!   chapter names. [`AllowAll`] allows everything until then. The
//!   `operator_only` check needs no privilege tables and is made here.
//! - **Bounds.** Step 5 reads, under the catalog version row's lock, the
//!   rows of every wider scope: hard bounds, `global` and `dc` from the
//!   process's etcd mirror ([`EtcdView`]), then the copies of every
//!   ancestor's tenant-scope rows (root first), the tenant, and the
//!   object's database. Holding the lock means no other change can move
//!   them meanwhile, and a refused write releases the lock before any
//!   version is allocated, so it leaves no gap. "Writes check the copy":
//!   a value accepted against a stale ancestor copy is clamped at read time
//!   once the newer bound arrives.
//! - **The row.** A value and bounds share one row; `RESET` clears the
//!   value, `RESET BOUNDS` the bounds, and a row left with neither stays as
//!   an empty row with the change's version so incremental loads see it. A
//!   tenant-scope change moves the directory's `settings_version`
//!   (`ChangeOpts { tenant_settings: true }`), which descendants copy.
//! - **Warnings and decisions.** After a bounds change, stored values at
//!   narrower scopes of this tenant that resolution will clamp or ignore
//!   come back as [`Warning`]s with chapter 08's summary
//!   ([`clamp_summary`]), and the write is recorded as a decision through
//!   a [`DecisionSink`] (chapter 11's `decisions` table doesn't exist yet;
//!   [`LogDecisions`] logs it). Values in descendants are clamped once the
//!   tree loop copies the bound; their reconcilers count them (step 11).
//!   Every change is audited as a `settings.set` event with key, scope, old
//!   and new value, who and why.
//!
//! Not yet: `catalog.max_setting_rows` (no error code is assigned for it).

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use sprig_common::ids::{DbId, DcId, ObjectId, TenantId, UserId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_settings::{
    AnySetting, Apply, Bounds, ClampedValue, Entry, HardBounds, LiveSettings, MemSettings,
    Registry, Scope, ScopeKind, Scopes, Setting, SettingChange, TenantScope, TenantSettings, Value,
    Warning, clamp_summary, normalize_change,
};

use crate::ancestry::AncestryState;
use crate::rows::{SettingsBackend, SettingsTx};
use crate::store::{SettingRow, SettingScope};
use crate::version::{ChangeOpts, VersionKind, Versioner};

// ---- keys (chapter 08, "Configuration") ----

/// Tenant (the top of a subtree) and global.
const TENANT_GLOBAL: Scopes = Scopes(0b10_0100);

/// `tenant.ancestry_mode`: how descendants hold ancestors' settings. Only
/// `copy` is implemented; `auto` behaves as `copy` (chapter 08 step 18).
pub static ANCESTRY_MODE: Setting<Cow<'static, str>> = Setting {
    key: "tenant.ancestry_mode",
    default: Cow::Borrowed("auto"),
    scopes: TENANT_GLOBAL,
    hard: HardBounds::one_of(&["auto", "copy", "shared", "reference"]),
    apply: Apply::Live,
    operator_only: true,
    owner: 8,
    doc: "auto, copy, shared or reference: how descendants hold ancestors' settings",
};
sprig_settings::register!(ANCESTRY_MODE);

/// `tenant.copy_max_descendants`.
pub static COPY_MAX_DESCENDANTS: Setting<u64> = Setting {
    key: "tenant.copy_max_descendants",
    default: 10_000,
    scopes: TENANT_GLOBAL,
    hard: HardBounds::range(1, 100_000_000),
    apply: Apply::Live,
    operator_only: true,
    owner: 8,
    doc: "auto switches from copy to shared above this subtree size",
};
sprig_settings::register!(COPY_MAX_DESCENDANTS);

/// `tenant.ancestry_cache_seconds`.
pub static ANCESTRY_CACHE_SECONDS: Setting<u64> = Setting {
    key: "tenant.ancestry_cache_seconds",
    default: 60,
    scopes: TENANT_GLOBAL,
    hard: HardBounds::range(1, 86_400),
    apply: Apply::Live,
    operator_only: true,
    owner: 8,
    doc: "Cache lifetime of ancestors' settings in reference mode",
};
sprig_settings::register!(ANCESTRY_CACHE_SECONDS);

/// `catalog.tree_sync_concurrency`.
pub static TREE_SYNC_CONCURRENCY: Setting<u32> = Setting {
    key: "catalog.tree_sync_concurrency",
    default: 16,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(1, 1024),
    apply: Apply::Live,
    operator_only: true,
    owner: 8,
    doc: "Descendant catalogs updated at once per DC after an ancestor's settings change",
};
sprig_settings::register!(TREE_SYNC_CONCURRENCY);

/// `catalog.bootstrap_timeout_minutes`.
pub static BOOTSTRAP_TIMEOUT_MINUTES: Setting<u64> = Setting {
    key: "catalog.bootstrap_timeout_minutes",
    default: 30,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(1, 10_080),
    apply: Apply::Live,
    operator_only: true,
    owner: 8,
    doc: "Bootstrap deadline: a tenant still creating after this long becomes failed",
};
sprig_settings::register!(BOOTSTRAP_TIMEOUT_MINUTES);

// ---- who writes, and what they need ----

/// Who changes a setting; recorded as the row's `set_by`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Principal {
    /// A user of the tenant (`user:<id>`).
    User(UserId),
    /// A platform operator (`op:<subject>`).
    Operator(String),
    /// SprigDB itself (`system`).
    System,
}

impl Principal {
    /// The `set_by` text: `user:7`, `op:<subject>` or `system`.
    #[must_use]
    pub fn set_by(&self) -> String {
        match self {
            Principal::User(u) => format!("user:{u}"),
            Principal::Operator(s) => format!("op:{s}"),
            Principal::System => "system".to_owned(),
        }
    }

    /// A tenant principal: may not touch `operator_only` keys.
    #[must_use]
    pub fn is_tenant(&self) -> bool {
        matches!(self, Principal::User(_))
    }
}

/// The catalog scope a write targets. Ancestor rows are never a target:
/// only the tree loop writes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Target {
    /// The tenant.
    Tenant,
    /// A database of the tenant.
    Database(DbId),
    /// An object of the tenant.
    Object(ObjectId),
}

impl Target {
    /// The row's scope.
    #[must_use]
    pub const fn scope(self) -> SettingScope {
        match self {
            Target::Tenant => SettingScope::Tenant,
            Target::Database(d) => SettingScope::Database(d),
            Target::Object(o) => SettingScope::Object(o),
        }
    }

    /// The scope kind, as the registry's accepted scopes name it.
    #[must_use]
    pub const fn kind(self) -> ScopeKind {
        match self {
            Target::Tenant => ScopeKind::Tenant,
            Target::Database(_) => ScopeKind::Database,
            Target::Object(_) => ScopeKind::Object,
        }
    }
}

impl std::fmt::Display for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Target::Tenant => f.write_str("tenant"),
            Target::Database(d) => write!(f, "database {d}"),
            Target::Object(o) => write!(f, "object {o}"),
        }
    }
}

/// The privilege a write needs (chapter 08 step 3; chapter 29 decides
/// how a principal holds it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Privilege {
    /// `ALTER` on the object.
    AlterObject(ObjectId),
    /// `ALTER` on the database.
    AlterDatabase(DbId),
    /// `SPRIG_SETTINGS_ADMIN` on the tenant, or `SPRIG_TENANT_ADMIN` on an
    /// ancestor: tenant-scope values and any bounds.
    SettingsAdmin,
}

/// Chapter 29's privilege check, passed in by the caller.
#[async_trait::async_trait]
pub trait SettingAuthorizer: Send + Sync {
    /// `Ok` if `principal` holds `need` in `tenant`.
    ///
    /// # Errors
    /// Access denied (1227 or chapter 29's code).
    async fn authorize(
        &self,
        tenant: TenantId,
        principal: &Principal,
        need: Privilege,
    ) -> Result<()>;
}

/// Allows every write: the privilege tables arrive with chapter 29 (M0.5).
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

#[async_trait::async_trait]
impl SettingAuthorizer for AllowAll {
    async fn authorize(&self, _: TenantId, _: &Principal, _: Privilege) -> Result<()> {
        Ok(())
    }
}

/// The process's view of the `global`, `dc` and `cluster` scopes.
pub trait EtcdView: Send + Sync {
    /// The current rows.
    fn snapshot(&self) -> Arc<MemSettings<'static>>;
    /// The setter's DC, whose `dc` scope applies.
    fn dc(&self) -> &DcId;
}

impl EtcdView for LiveSettings {
    fn snapshot(&self) -> Arc<MemSettings<'static>> {
        LiveSettings::snapshot(self)
    }
    fn dc(&self) -> &DcId {
        self.stores().dc_id()
    }
}

/// Fixed etcd rows, for tests and tools.
#[derive(Debug, Clone)]
pub struct FixedEtcd {
    /// The rows.
    pub rows: Arc<MemSettings<'static>>,
    /// The DC.
    pub dc: DcId,
}

impl EtcdView for FixedEtcd {
    fn snapshot(&self) -> Arc<MemSettings<'static>> {
        Arc::clone(&self.rows)
    }
    fn dc(&self) -> &DcId {
        &self.dc
    }
}

/// A decision a setting write records (chapter 08 step 7; chapter 11's
/// decision log).
#[derive(Debug, Clone, PartialEq)]
pub struct SettingDecision {
    /// The tenant.
    pub tenant: TenantId,
    /// Where the bounds were set.
    pub target: Target,
    /// The key.
    pub key: String,
    /// `bounds_tightened`.
    pub reason_code: &'static str,
    /// The warning summary, such as "3 values will be clamped".
    pub summary: String,
    /// Values clamped.
    pub clamped: usize,
    /// Values ignored.
    pub ignored: usize,
    /// `set_by` of the write.
    pub actor: String,
    /// The writer's reason.
    pub reason: String,
    /// The catalog version of the write.
    pub version: u64,
}

/// Where setting decisions go: chapter 11's `decisions` table once
/// `sprig-balancer` has it.
#[async_trait::async_trait]
pub trait DecisionSink: Send + Sync {
    /// Records `d`.
    ///
    /// # Errors
    /// The log's error; the write has already committed, so the caller
    /// logs it and carries on.
    async fn record(&self, d: &SettingDecision) -> Result<()>;
}

/// Logs decisions as `decision` events until the decision log exists.
#[derive(Debug, Clone, Copy, Default)]
pub struct LogDecisions;

#[async_trait::async_trait]
impl DecisionSink for LogDecisions {
    async fn record(&self, d: &SettingDecision) -> Result<()> {
        tracing::info!(
            event = "decision",
            component = "catalog",
            kind = "settings.bounds",
            tenant = %d.tenant,
            subject = %d.target,
            key = %d.key,
            reason_code = d.reason_code,
            clamped = d.clamped,
            ignored = d.ignored,
            actor = %d.actor,
            reason = %d.reason,
            version = d.version,
            "{}",
            d.summary
        );
        Ok(())
    }
}

// ---- the write ----

/// One setting write.
#[derive(Debug, Clone, PartialEq)]
pub struct SetRequest {
    /// The tenant whose catalog holds the row.
    pub tenant: TenantId,
    /// The scope.
    pub target: Target,
    /// The key.
    pub key: String,
    /// What changes.
    pub change: SettingChange,
    /// Who.
    pub principal: Principal,
    /// Why (audited; recorded with decisions).
    pub reason: String,
}

/// What a write did.
#[derive(Debug, Clone, PartialEq)]
pub struct SetOutcome {
    /// The catalog version it committed at; `None` when there was nothing
    /// to change (a reset of a row that wasn't set).
    pub version: Option<u64>,
    /// The row before.
    pub previous: Option<Entry>,
    /// The row after (`None` when left empty).
    pub current: Option<Entry>,
    /// Stored values at narrower scopes that new bounds clamp or ignore.
    pub warnings: Vec<Warning>,
    /// Chapter 08's summary of the warnings ("2 values will be clamped").
    pub summary: Option<String>,
}

/// Writes setting rows of the tenant, database and object scopes.
pub struct TenantSettingsWriter<B: SettingsBackend>
where
    B::Tx: SettingsTx,
{
    versioner: Versioner<B>,
    registry: &'static Registry,
    etcd: Arc<dyn EtcdView>,
    auth: Arc<dyn SettingAuthorizer>,
    decisions: Arc<dyn DecisionSink>,
    clock: Arc<dyn Clock>,
}

impl<B: SettingsBackend> std::fmt::Debug for TenantSettingsWriter<B>
where
    B::Tx: SettingsTx,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantSettingsWriter")
            .field("dc", self.etcd.dc())
            .finish_non_exhaustive()
    }
}

/// 39081 for an unknown or internal key.
fn def_of(registry: &Registry, key: &str) -> Result<&'static dyn AnySetting> {
    registry
        .get(key)
        .filter(|_| !key.starts_with("sprig."))
        .ok_or_else(|| SprigError::new(SprigCode::SettingUnknown, format!("unknown setting {key}")))
}

fn access_denied(key: &str) -> SprigError {
    SprigError::standard(
        1227,
        *b"42000",
        format!("Access denied; {key} is an operator-only setting"),
    )
}

fn row_entry(r: &SettingRow) -> Option<Entry> {
    r.entry().ok().filter(|e| *e != Entry::default())
}

fn to_json(v: &Value) -> Result<serde_json::Value> {
    serde_json::to_value(v).map_err(|e| SprigError::internal(format!("encoding a value: {e}")))
}

impl<B: SettingsBackend> TenantSettingsWriter<B>
where
    B::Tx: SettingsTx,
{
    /// A writer committing through `versioner`, checking keys against
    /// `registry` and wider bounds against `etcd`, privileges with `auth`,
    /// recording decisions in `decisions`; `clock` stamps `set_at`.
    pub fn new(
        versioner: Versioner<B>,
        registry: &'static Registry,
        etcd: Arc<dyn EtcdView>,
        auth: Arc<dyn SettingAuthorizer>,
        decisions: Arc<dyn DecisionSink>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            versioner,
            registry,
            etcd,
            auth,
            decisions,
            clock,
        }
    }

    /// The versioner.
    #[must_use]
    pub fn versioner(&self) -> &Versioner<B> {
        &self.versioner
    }

    /// Applies `req` (see the module docs).
    ///
    /// # Errors
    /// - 39081 `ER_SPRIG_SETTING_UNKNOWN` (also for `sprig.` keys);
    /// - 39082 `ER_SPRIG_SETTING_SCOPE_NOT_ALLOWED`;
    /// - 1227 for an `operator_only` key and a tenant principal, or the
    ///   authorizer's refusal;
    /// - 39084 `ER_SPRIG_SETTING_BAD_VALUE`;
    /// - 39083 `ER_SPRIG_SETTING_LOCKED`, 39080
    ///   `ER_SPRIG_SETTING_OUT_OF_BOUNDS`;
    /// - 1049 or 1146 for a database or object that doesn't exist;
    /// - [`Versioner::change`]'s errors (39070, 39071, 39072, 39504).
    pub async fn set(&self, req: &SetRequest) -> Result<SetOutcome> {
        let key = req.key.as_str();
        let (def, change) = self.precheck(req).await?;
        let etcd = self.etcd.snapshot();

        let mut locked = self
            .versioner
            .lock(req.tenant, VersionKind::Catalog)
            .await?;
        let checked = self.check(locked.tx(), req, def, &change, &etcd).await;
        let (previous, ancestry) = match checked {
            Ok(Some(c)) => c,
            Ok(None) => {
                locked.release().await?;
                return Ok(SetOutcome {
                    version: None,
                    previous: None,
                    current: None,
                    warnings: Vec::new(),
                    summary: None,
                });
            }
            Err(e) => {
                let _ = locked.release().await;
                return Err(e);
            }
        };
        let mut c = locked
            .allocate(ChangeOpts {
                tenant_settings: req.target == Target::Tenant,
            })
            .await?;
        let version = c.version();
        let now = self.clock.wall();
        let set_by = req.principal.set_by();
        let scope = req.target.scope();
        let written = async {
            let tx = c.tx();
            write_row(
                tx,
                scope,
                key,
                &change,
                previous.as_ref(),
                &set_by,
                now,
                version,
            )
            .await?;
            let current = tx.setting(scope, key).await?.as_ref().and_then(row_entry);
            let clamped = if matches!(change, SettingChange::SetBounds(_)) {
                self.clamped_below(tx, req, &ancestry, &etcd, version)
                    .await?
            } else {
                Vec::new()
            };
            Ok::<_, SprigError>((current, clamped))
        }
        .await;
        let (current, clamped) = match written {
            Ok(w) => w,
            Err(e) => {
                let _ = c.rollback().await;
                return Err(e);
            }
        };
        let version = c.commit().await?;
        Ok(self
            .finish(
                req,
                previous.as_ref().and_then(row_entry),
                current,
                &clamped,
                set_by,
                version,
            )
            .await)
    }

    /// Audits the committed write, records a decision when bounds clamp
    /// values, and builds the outcome.
    async fn finish(
        &self,
        req: &SetRequest,
        previous: Option<Entry>,
        current: Option<Entry>,
        clamped: &[ClampedValue],
        set_by: String,
        version: u64,
    ) -> SetOutcome {
        let key = req.key.as_str();
        tracing::info!(
            event = "settings.set",
            tenant = %req.tenant,
            scope = %req.target,
            key,
            old = ?previous,
            new = ?current,
            who = %set_by,
            reason = %req.reason,
            version,
            "setting changed"
        );
        let summary = clamp_summary(clamped);
        let warnings: Vec<Warning> = clamped.iter().map(|c| c.warning(req.tenant)).collect();
        if let Some(s) = &summary {
            let ignored = clamped.iter().filter(|c| c.why.is_some()).count();
            let d = SettingDecision {
                tenant: req.tenant,
                target: req.target,
                key: key.to_owned(),
                reason_code: "bounds_tightened",
                summary: s.clone(),
                clamped: clamped.len() - ignored,
                ignored,
                actor: set_by,
                reason: req.reason.clone(),
                version,
            };
            if let Err(e) = self.decisions.record(&d).await {
                tracing::warn!(tenant = %req.tenant, key, error = %e, "recording a setting decision failed");
            }
        }
        SetOutcome {
            version: Some(version),
            previous,
            current,
            warnings,
            summary,
        }
    }

    /// Steps 1 to 4: the key, the scope, `operator_only`, the privilege
    /// and the type. Nothing is read from the catalog yet.
    async fn precheck(&self, req: &SetRequest) -> Result<(&'static dyn AnySetting, SettingChange)> {
        let key = req.key.as_str();
        let def = def_of(self.registry, key)?;
        let kind = req.target.kind();
        if !def.scopes().contains(kind) {
            return Err(SprigError::new(
                SprigCode::SettingScopeNotAllowed,
                format!("{key} can't be set at {kind} scope"),
            ));
        }
        if def.operator_only() && req.principal.is_tenant() {
            return Err(access_denied(key));
        }
        let bounds_change = matches!(
            req.change,
            SettingChange::SetBounds(_) | SettingChange::ResetBounds
        );
        let need = match req.target {
            _ if bounds_change => Privilege::SettingsAdmin,
            Target::Tenant => Privilege::SettingsAdmin,
            Target::Database(d) => Privilege::AlterDatabase(d),
            Target::Object(o) => Privilege::AlterObject(o),
        };
        self.auth
            .authorize(req.tenant, &req.principal, need)
            .await?;
        Ok((def, normalize_change(def, req.change.clone())?))
    }

    /// Steps 5 and the target's existence, under the version row's lock.
    /// `None` when there is nothing to change.
    async fn check(
        &self,
        tx: &mut B::Tx,
        req: &SetRequest,
        def: &'static dyn AnySetting,
        change: &SettingChange,
        etcd: &MemSettings<'static>,
    ) -> Result<Option<(Option<SettingRow>, Vec<TenantId>)>> {
        let key = def.key();
        let object_db = match req.target {
            Target::Tenant => None,
            Target::Database(d) => {
                if !tx.database_live(d).await? {
                    return Err(SprigError::standard(
                        1049,
                        *b"42000",
                        format!("Unknown database {d}"),
                    ));
                }
                None
            }
            Target::Object(o) => Some(tx.object_db(o).await?.ok_or_else(|| {
                SprigError::standard(1146, *b"42S02", format!("Object {o} doesn't exist"))
            })?),
        };
        let ancestry = AncestryState::read(tx, req.tenant).await?.ids();
        let wider = self
            .wider_bounds(tx, def, req.target, &ancestry, object_db, etcd)
            .await?;
        match change {
            SettingChange::Set(v) => {
                if wider.locked {
                    return Err(locked(key));
                }
                if !wider.admits(v) {
                    return Err(SprigError::new(
                        SprigCode::SettingOutOfBounds,
                        format!(
                            "{key}: {} is outside the bounds in force ({})",
                            show(v),
                            show_bounds(&wider)
                        ),
                    ));
                }
            }
            SettingChange::SetBounds(_) if wider.locked => return Err(locked(key)),
            _ => {}
        }
        let previous = tx.setting(req.target.scope(), key).await?;
        let nothing = match change {
            SettingChange::Reset => previous.as_ref().is_none_or(|p| p.value.is_none()),
            SettingChange::ResetBounds => previous.as_ref().is_none_or(|p| p.bounds.is_none()),
            _ => false,
        };
        if nothing {
            return Ok(None);
        }
        Ok(Some((previous, ancestry)))
    }

    /// Hard bounds and the bounds of every scope wider than `target`: global
    /// and dc (etcd), ancestors root first, the tenant, and the object's
    /// database, keeping the scopes the key accepts.
    async fn wider_bounds(
        &self,
        tx: &mut B::Tx,
        def: &'static dyn AnySetting,
        target: Target,
        ancestry: &[TenantId],
        object_db: Option<DbId>,
        etcd: &MemSettings<'static>,
    ) -> Result<Bounds> {
        let key = def.key();
        let scopes = def.scopes();
        let mut b: Bounds = def.hard().into();
        let mut add = |e: Option<&Entry>| {
            if let Some(eb) = e.and_then(|e| e.bounds.as_ref()) {
                b = b.intersect(eb);
            }
        };
        if scopes.contains(ScopeKind::Global) {
            add(etcd.entry(&Scope::Global, key));
        }
        if scopes.contains(ScopeKind::Dc) {
            add(etcd.entry(&Scope::Dc(self.etcd.dc().clone()), key));
        }
        let mut catalog: Vec<SettingScope> = Vec::new();
        if scopes.contains(ScopeKind::Tenant) {
            catalog.extend(ancestry.iter().map(|&a| SettingScope::Ancestor(a)));
            if target != Target::Tenant {
                catalog.push(SettingScope::Tenant);
            }
        }
        if let (Target::Object(_), Some(d)) = (target, object_db)
            && scopes.contains(ScopeKind::Database)
        {
            catalog.push(SettingScope::Database(d));
        }
        for s in catalog {
            let e = tx.setting(s, key).await?.and_then(|r| r.entry().ok());
            add(e.as_ref());
        }
        Ok(b)
    }

    /// Stored values of `req.key` at scopes narrower than `req.target`
    /// that resolution clamps or ignores after the write (the write's own
    /// row is already in `tx`).
    async fn clamped_below(
        &self,
        tx: &mut B::Tx,
        req: &SetRequest,
        ancestry: &[TenantId],
        etcd: &MemSettings<'static>,
        version: u64,
    ) -> Result<Vec<ClampedValue>> {
        let rows = tx.key_rows(&req.key).await?;
        let mut b = TenantSettings::builder(self.registry, req.tenant, version);
        b.set_ancestry(ancestry.to_vec());
        let mut dbs: HashMap<ObjectId, DbId> = HashMap::new();
        for r in &rows {
            if let SettingScope::Object(o) = r.scope
                && let Some(d) = tx.object_db(o).await?
            {
                dbs.insert(o, d);
            }
            if let Ok(e) = r.entry() {
                b.put(TenantScope::from(r.scope), &r.key, e);
            }
        }
        let ts = b.build();
        let narrower = |s: TenantScope| match (req.target, s) {
            (Target::Tenant, TenantScope::Database(_) | TenantScope::Object(_)) => true,
            (Target::Database(d), TenantScope::Object(o)) => dbs.get(&o) == Some(&d),
            _ => false,
        };
        let clamped = ts.clamped(etcd, self.etcd.dc(), &|o| dbs.get(&o).copied());
        Ok(clamped
            .into_iter()
            .filter(|c| c.key == req.key && narrower(c.scope))
            .collect())
    }
}

/// Step 6's row write: a value or bounds upserted into the row (keeping
/// the other half), or one half cleared.
#[allow(clippy::too_many_arguments)] // one row's address and its change
async fn write_row<T: SettingsTx + ?Sized>(
    tx: &mut T,
    scope: SettingScope,
    key: &str,
    change: &SettingChange,
    previous: Option<&SettingRow>,
    set_by: &str,
    now: std::time::SystemTime,
    version: u64,
) -> Result<()> {
    let row = |value, bounds| SettingRow {
        scope,
        key: key.to_owned(),
        value,
        bounds,
        set_by: set_by.to_owned(),
        set_at: now,
        version,
    };
    match change {
        SettingChange::Set(v) => {
            let bounds = previous.and_then(|p| p.bounds.clone());
            tx.put_setting(&row(Some(to_json(v)?), bounds)).await
        }
        SettingChange::SetBounds(b) => {
            let value = previous.and_then(|p| p.value.clone());
            tx.put_setting(&row(value, Some(b.clone()))).await
        }
        SettingChange::Reset => tx
            .reset_setting(scope, key, true, false, set_by, now, version)
            .await
            .map(|_| ()),
        SettingChange::ResetBounds => tx
            .reset_setting(scope, key, false, true, set_by, now, version)
            .await
            .map(|_| ()),
    }
}

fn locked(key: &str) -> SprigError {
    SprigError::new(
        SprigCode::SettingLocked,
        format!("{key} is locked by a wider scope"),
    )
}

fn show(v: &Value) -> String {
    match v {
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Str(s) => format!("'{s}'"),
    }
}

fn show_bounds(b: &Bounds) -> String {
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
            a.iter().map(show).collect::<Vec<_>>().join(", ")
        ));
    }
    if parts.is_empty() {
        "none".to_owned()
    } else {
        parts.join(", ")
    }
}

#[cfg(test)]
mod tests;
