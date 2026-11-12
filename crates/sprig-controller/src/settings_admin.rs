//! The Admin API's `Settings` service (chapter 37): list and describe
//! keys, resolve one with its chain, and change values, bounds and locks
//! at every scope (chapter 08 "Setting a value", chapter 23 "Settings:
//! writes, bounds and watches", chapter 47 "Explaining settings").
//!
//! | Call | Route |
//! |---|---|
//! | `Settings.ListKeys` | `GET /admin/v1/settings?prefix=&operator_only=&scope=` |
//! | `Settings.GetSetting` | `GET /admin/v1/settings/{key}?tenant=&db=&object=&cluster=&dc=` |
//! | `Settings.ExplainSetting` | `GET /admin/v1/settings/{key}:explain` (the same answer) |
//! | `Settings.SetSetting` | `PUT /admin/v1/settings/{key}` |
//! | `Settings.UnsetSetting` | `DELETE /admin/v1/settings/{key}?scope=` |
//! | reset value, bounds or both | `POST /admin/v1/settings/{key}:reset` |
//! | set parts of the bounds | `POST /admin/v1/settings/{key}:bounds` |
//! | lock, unlock | `POST /admin/v1/settings/{key}:lock`, `{key}:unlock` |
//! | a tenant's rows | `GET /admin/v1/tenants/{name}/settings` |
//!
//! [`SettingsService`] is the implementation over a controller's stores:
//!
//! - **Reads** resolve against this controller's etcd mirror
//!   ([`LiveSettings`], what every process here resolves with) and, for a
//!   tenant context, a fresh full read of the tenant's catalog (its
//!   writer), with chapter 08's algorithm ([`explain_layered`]). Each step
//!   of the chain carries who last changed its row and when: the etcd
//!   record's `set_by`, `set_at` and `reason`, the catalog row's `set_by`,
//!   `set_at` and version.
//! - **`global`, `dc`, `cluster`** are written with
//!   [`SettingsWriter::set_guarded`] under the fencing compare of the
//!   leader the call is served by ([`AdminService`](crate::admin::AdminService)
//!   passes it): chapter 23's checks (scope, type, every wider scope's
//!   bounds and locks), the record's `set_by`/`set_at`/`reason`, and
//!   warnings for this DC's narrower values the new bounds clamp. In R0
//!   (single-etcd mode) a DC leader writes `global` records, as it writes
//!   the tenant directory; a controller that leads the global tier uses
//!   that leadership instead.
//! - **`tenant`, `database`, `object`** go through
//!   [`TenantSettingsWriter`] with an operator principal (`op:<actor>`),
//!   so they are the same checked, versioned catalog writes as SQL's
//!   `SPRIG SET`. Like SQL writes from any gateway, they need no leader.
//! - **Bounds** calls change only the parts they name (`--max 8` keeps a
//!   stored minimum and the lock). In etcd the merge happens inside the
//!   guarded write; for catalog scopes it is computed from the catalog
//!   read just before the write (a concurrent bounds change of the same
//!   row in between is overwritten, as with SQL).
//! - **Audit.** Every change is logged as a `settings.changed` event
//!   (chapter 47) with key, scope, action, old and new row, who and why;
//!   the writers add their own `settings.set` events. Tightened bounds
//!   come back as warnings and, for catalog scopes, are recorded as a
//!   `bounds_tightened` decision (chapter 08 step 7).
//!
//! Refusals: 39081 unknown key, 39082 scope not accepted, 39084 wrong
//! type, 39080 outside the bounds in force, 39083 locked by a wider scope,
//! 1049 or 1146 for a database or object that doesn't exist, 39072 for an
//! unknown or inactive tenant, 39504 off the leader (etcd scopes), 39792
//! for a malformed request, 39368 for an `auth.authorizer` engine some
//! live gateway doesn't know (chapter 29).

use std::collections::HashMap;
use std::sync::Arc;

use sprig_catalog::cache::{Loaded, TenantCatalog};
use sprig_catalog::ddl::backend::{DdlBackend, DdlTx};
use sprig_catalog::store::{DbState, Namespace, ObjectState, SettingRow, SettingScope};
use sprig_catalog::{
    AllowAll, LogDecisions, Principal, SetRequest, SettingsBackend, SettingsTx, Target,
    TenantSettingsWriter, Versioner,
};
use sprig_common::ids::{ClusterId, DbId, DcId, ObjectId, TenantId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::Cmp;
use sprig_meta::bootstrap::rfc3339;
use sprig_settings::{
    AnySetting, Apply, BoundsPatch, Explanation, Ignored, LiveSettings, RecordOutcome, Registry,
    Scope, ScopeKind, SettingChange, SettingCtx, SettingEdit, SettingRecord, SettingsWriter,
    StepOutcome, Value, ValueKind, Warning, explain_layered,
};

use crate::admin::invalid;
use crate::admin::types::{
    ListSettingKeysQuery, ListSettingKeysResponse, ScopeRef, SettingContextQuery,
    SettingContextView, SettingExplanation, SettingKeyView, SettingOp, SettingRowView,
    SettingStepView, SettingWarningView, SettingWrite, SettingWriteResponse, TenantSettingRowView,
    TenantSettingsResponse,
};
use crate::tree::{Directory, TenantState};

/// The Admin API's settings calls.
#[async_trait::async_trait]
pub trait SettingsAdmin: Send + Sync {
    /// `Settings.ListKeys`.
    ///
    /// # Errors
    /// 39792 for an unknown scope kind.
    async fn list_keys(&self, q: &ListSettingKeysQuery) -> Result<ListSettingKeysResponse>;

    /// `Settings.GetSetting` / `ExplainSetting`: `key` resolved in `q`'s
    /// context, with its chain.
    ///
    /// # Errors
    /// 39081 unknown key; 39072 unknown tenant; 1049, 1146 unknown
    /// database or object; 39792 for a malformed context.
    async fn explain(&self, key: &str, q: &SettingContextQuery) -> Result<SettingExplanation>;

    /// One write. `guard` is the serving leader's fencing compare for the
    /// etcd scopes (empty for catalog scopes).
    ///
    /// # Errors
    /// See the module docs.
    async fn write(
        &self,
        key: &str,
        w: &SettingWrite,
        guard: Vec<Cmp>,
    ) -> Result<SettingWriteResponse>;

    /// The rows of tenant `name`'s catalog `settings` table.
    ///
    /// # Errors
    /// 39072 for an unknown or inactive tenant; catalog errors.
    async fn tenant_settings(&self, name: &str) -> Result<TenantSettingsResponse>;
}

// ---- pure helpers, shared with fakes and sprigctl's tests ----

/// The declaration of `key`: 39081 if unknown or internal.
///
/// # Errors
/// 39081 `ER_SPRIG_SETTING_UNKNOWN`.
pub fn def_of(registry: &Registry, key: &str) -> Result<&'static dyn AnySetting> {
    registry
        .get(key)
        .filter(|_| !key.starts_with("sprig."))
        .ok_or_else(|| SprigError::new(SprigCode::SettingUnknown, format!("unknown setting {key}")))
}

const SCOPE_ORDER: [(ScopeKind, &str); 6] = [
    (ScopeKind::Global, "global"),
    (ScopeKind::Dc, "dc"),
    (ScopeKind::Cluster, "cluster"),
    (ScopeKind::Tenant, "tenant"),
    (ScopeKind::Database, "database"),
    (ScopeKind::Object, "object"),
];

fn scope_kind_named(name: &str) -> Result<ScopeKind> {
    let name = if name == "db" { "database" } else { name };
    SCOPE_ORDER
        .iter()
        .find(|(_, n)| *n == name)
        .map(|(k, _)| *k)
        .ok_or_else(|| {
            invalid(format!(
                "scope {name:?}: expected global, dc, cluster, tenant, database or object"
            ))
        })
}

fn type_name(k: ValueKind) -> &'static str {
    match k {
        ValueKind::Bool => "bool",
        ValueKind::Int => "int",
        ValueKind::Float => "float",
        ValueKind::Str => "string",
    }
}

/// Tenants may set it: not operator-only, and some catalog scope accepts
/// it.
fn tenant_settable(def: &dyn AnySetting) -> bool {
    !def.operator_only()
        && [ScopeKind::Tenant, ScopeKind::Database, ScopeKind::Object]
            .iter()
            .any(|k| def.scopes().contains(*k))
}

/// A declaration as the API shows it.
#[must_use]
pub fn key_view(def: &dyn AnySetting) -> SettingKeyView {
    SettingKeyView {
        key: def.key().to_owned(),
        kind: type_name(def.kind()).to_owned(),
        default: def.default_value(),
        scopes: SCOPE_ORDER
            .iter()
            .filter(|(k, _)| def.scopes().contains(*k))
            .map(|(_, n)| (*n).to_owned())
            .collect(),
        hard_bounds: def.hard().into(),
        apply: match def.apply() {
            Apply::Live => "live",
            Apply::NewSessions => "new_sessions",
            Apply::Rebind => "rebind",
        }
        .to_owned(),
        operator_only: def.operator_only(),
        tenant_settable: tenant_settable(def),
        owner: def.owner(),
        doc: def.doc().to_owned(),
    }
}

/// `Settings.ListKeys` over `registry`.
///
/// # Errors
/// 39792 for an unknown scope kind.
pub fn list_keys(registry: &Registry, q: &ListSettingKeysQuery) -> Result<ListSettingKeysResponse> {
    let scope = q.scope.as_deref().map(scope_kind_named).transpose()?;
    let keys = registry
        .iter()
        .filter(|d| q.prefix.as_deref().is_none_or(|p| d.key().starts_with(p)))
        .filter(|d| q.operator_only.is_none_or(|o| tenant_settable(*d) != o))
        .filter(|d| scope.is_none_or(|s| d.scopes().contains(s)))
        .map(key_view)
        .collect();
    Ok(ListSettingKeysResponse { keys })
}

/// A value written as text (`8`, `true`, `on`), parsed with `def`'s type.
///
/// # Errors
/// 39084 `ER_SPRIG_SETTING_BAD_VALUE` if it isn't one, or doesn't fit
/// the key's range of integers.
pub fn parse_text(def: &dyn AnySetting, text: &str) -> Result<Value> {
    let t = text.trim();
    let v = match def.kind() {
        ValueKind::Bool => match t.to_ascii_lowercase().as_str() {
            "true" | "on" | "yes" | "1" => Some(Value::Bool(true)),
            "false" | "off" | "no" | "0" => Some(Value::Bool(false)),
            _ => None,
        },
        ValueKind::Int => t.replace('_', "").parse::<i64>().ok().map(Value::Int),
        ValueKind::Float => t
            .parse::<f64>()
            .ok()
            .filter(|f| f.is_finite())
            .map(Value::Float),
        ValueKind::Str => Some(Value::Str(text.to_owned())),
    };
    v.and_then(|v| def.normalize(&v)).ok_or_else(|| {
        SprigError::new(
            SprigCode::SettingBadValue,
            format!(
                "{}: {text:?} is not a valid {}",
                def.key(),
                type_name(def.kind())
            ),
        )
    })
}

/// The value of a `set`: typed JSON or text, exactly one.
fn value_of(def: &dyn AnySetting, value: Option<&Value>, text: Option<&str>) -> Result<Value> {
    match (value, text) {
        (Some(v), None) => Ok(v.clone()),
        (None, Some(t)) => parse_text(def, t),
        (None, None) => Err(invalid("a value is required (value or value_text)")),
        (Some(_), Some(_)) => Err(invalid("give either value or value_text, not both")),
    }
}

/// The bounds change of a `bounds` or `lock` op.
fn patch_of(def: &dyn AnySetting, op: &SettingOp) -> Result<BoundsPatch> {
    match op {
        SettingOp::Lock { locked } => Ok(BoundsPatch {
            locked: Some(*locked),
            ..BoundsPatch::default()
        }),
        SettingOp::Bounds {
            min,
            max,
            allowed,
            allowed_text,
            clear,
        } => {
            let mut p = BoundsPatch::default();
            if (min.is_some() || max.is_some())
                && !matches!(def.kind(), ValueKind::Int | ValueKind::Float)
            {
                return Err(SprigError::new(
                    SprigCode::SettingBadValue,
                    format!(
                        "{} is a {}: min and max apply to numbers; use allowed",
                        def.key(),
                        type_name(def.kind())
                    ),
                ));
            }
            if let (Some(a), Some(b)) = (min, max)
                && a > b
            {
                return Err(invalid(format!("min {a} is greater than max {b}")));
            }
            p.min = min.map(Some);
            p.max = max.map(Some);
            p.allowed = match (allowed, allowed_text) {
                (Some(_), Some(_)) => {
                    return Err(invalid("give either allowed or allowed_text, not both"));
                }
                (Some(a), None) => Some(Some(a.clone())),
                (None, Some(t)) => Some(Some(
                    t.iter()
                        .map(|x| parse_text(def, x))
                        .collect::<Result<Vec<_>>>()?,
                )),
                (None, None) => None,
            };
            for c in clear {
                match c.as_str() {
                    "min" if p.min.is_none() => p.min = Some(None),
                    "max" if p.max.is_none() => p.max = Some(None),
                    "allowed" if p.allowed.is_none() => p.allowed = Some(None),
                    "min" | "max" | "allowed" => {
                        return Err(invalid(format!("{c} is both set and cleared")));
                    }
                    other => {
                        return Err(invalid(format!(
                            "clear {other:?}: expected min, max or allowed"
                        )));
                    }
                }
            }
            if p.is_empty() {
                return Err(invalid(
                    "nothing to change: give min, max, allowed or clear",
                ));
            }
            Ok(p)
        }
        _ => Err(SprigError::internal("not a bounds change")),
    }
}

/// The edits one write makes, in order.
fn edits_of(def: &dyn AnySetting, op: &SettingOp) -> Result<Vec<SettingEdit>> {
    Ok(match op {
        SettingOp::Set { value, value_text } => vec![SettingEdit::Change(SettingChange::Set(
            value_of(def, value.as_ref(), value_text.as_deref())?,
        ))],
        SettingOp::Reset { value, bounds } => {
            let mut v = Vec::new();
            if *value || !*bounds {
                v.push(SettingEdit::Change(SettingChange::Reset));
            }
            if *bounds {
                v.push(SettingEdit::Change(SettingChange::ResetBounds));
            }
            v
        }
        SettingOp::Bounds { .. } | SettingOp::Lock { .. } => {
            vec![SettingEdit::PatchBounds(patch_of(def, op)?)]
        }
    })
}

fn kind_of(scope: &ScopeRef) -> ScopeKind {
    match scope {
        ScopeRef::Global => ScopeKind::Global,
        ScopeRef::Dc(_) => ScopeKind::Dc,
        ScopeRef::Cluster(_) => ScopeKind::Cluster,
        ScopeRef::Tenant(_) => ScopeKind::Tenant,
        ScopeRef::Database { .. } => ScopeKind::Database,
        ScopeRef::Object { .. } => ScopeKind::Object,
    }
}

/// Chapter 08's checks 1 and 2, before anything is read: the key exists
/// and accepts the scope.
///
/// # Errors
/// 39081, 39082.
pub fn check_key_and_scope(
    registry: &Registry,
    key: &str,
    scope: &ScopeRef,
) -> Result<&'static dyn AnySetting> {
    let def = def_of(registry, key)?;
    let kind = kind_of(scope);
    if !def.scopes().contains(kind) {
        let accepted = key_view(def).scopes.join(", ");
        return Err(SprigError::new(
            SprigCode::SettingScopeNotAllowed,
            format!("{key} can't be set at {kind} scope (it accepts {accepted})"),
        ));
    }
    Ok(def)
}

fn outcome_name(o: StepOutcome) -> &'static str {
    match o {
        StepOutcome::NotAccepted => "not_accepted",
        StepOutcome::Unset => "unset",
        StepOutcome::BoundsOnly => "bounds_only",
        StepOutcome::Effective => "effective",
        StepOutcome::Overridden => "overridden",
        StepOutcome::Ignored(Ignored::Locked) => "ignored_locked",
        StepOutcome::Ignored(Ignored::NotAllowed) => "ignored_not_allowed",
        StepOutcome::Ignored(Ignored::WrongType) => "ignored_wrong_type",
    }
}

fn scope_name(k: ScopeKind) -> &'static str {
    match k {
        ScopeKind::Object => "object",
        ScopeKind::Database => "database",
        ScopeKind::Tenant => "tenant",
        ScopeKind::Ancestor(_) => "ancestor",
        ScopeKind::Cluster => "cluster",
        ScopeKind::Dc => "dc",
        ScopeKind::Global => "global",
        ScopeKind::Default => "default",
    }
}

fn record_view(r: &SettingRecord) -> SettingRowView {
    let opt = |s: &str| (!s.is_empty()).then(|| s.to_owned());
    SettingRowView {
        value: r.value.clone(),
        bounds: r.bounds.clone(),
        set_by: opt(&r.set_by),
        set_at: opt(&r.set_at),
        reason: opt(&r.reason),
    }
}

fn audit(key: &str, w: &SettingWrite, r: &SettingWriteResponse) {
    tracing::info!(
        event = "settings.changed",
        key,
        scope = %w.scope,
        action = w.op.action(),
        old = ?r.previous,
        new = ?r.current,
        changed = r.changed,
        who = %r.set_by,
        reason = %w.reason,
        "setting changed through the Admin API"
    );
    // Chapter 47's pipeline (platform stream: an operator's change through
    // the Admin API, whatever its scope).
    sprig_common::audit::record(
        sprig_common::audit::AuditStream::Platform,
        "settings.changed",
        serde_json::json!({
            "key": key,
            "scope": w.scope.to_string(),
            "action": w.op.action(),
            "old": r.previous,
            "new": r.current,
            "changed": r.changed,
            "who": r.set_by,
            "reason": w.reason,
        }),
    );
}

fn summary(warnings: &[SettingWarningView]) -> Option<String> {
    match warnings.len() {
        0 => None,
        1 => Some("1 narrower value will be clamped or ignored".to_owned()),
        n => Some(format!("{n} narrower values will be clamped or ignored")),
    }
}

// ---- the implementation over a controller's stores ----

/// One tenant's catalog, read for a call.
struct TenantRead {
    id: TenantId,
    name: String,
    cat: TenantCatalog,
    rows: HashMap<(SettingScope, String), SettingRow>,
}

impl TenantRead {
    fn db(&self, name: &str) -> Result<DbId> {
        let lower = name.to_lowercase();
        let live = || {
            self.cat
                .databases
                .iter()
                .filter(|r| r.state == DbState::Active)
        };
        live()
            .find(|r| r.name == name)
            .or_else(|| live().find(|r| r.name_key == name || r.name_key == lower))
            .map(|r| r.db_id)
            .ok_or_else(|| {
                SprigError::standard(
                    1049,
                    *b"42000",
                    format!("Unknown database '{name}' in tenant {}", self.name),
                )
            })
    }

    fn object(&self, db: DbId, db_name: &str, name: &str) -> Result<ObjectId> {
        let lower = name.to_lowercase();
        let mut found: Vec<_> = self
            .cat
            .objects
            .iter()
            .filter(|o| {
                o.db_id == db
                    && matches!(o.state, ObjectState::Active | ObjectState::Altering)
                    && (o.name == name || o.name_key == name || o.name_key == lower)
            })
            .collect();
        if found.iter().any(|o| o.name == name) {
            found.retain(|o| o.name == name);
        }
        if found.iter().any(|o| o.namespace == Namespace::Relation) {
            found.retain(|o| o.namespace == Namespace::Relation);
        }
        match found.as_slice() {
            [o] => Ok(o.object_id),
            [] => Err(SprigError::standard(
                1146,
                *b"42S02",
                format!(
                    "Object '{db_name}.{name}' doesn't exist in tenant {}",
                    self.name
                ),
            )),
            _ => Err(invalid(format!(
                "{db_name}.{name} names several objects (namespaces {}); none is a table",
                found
                    .iter()
                    .map(|o| o.namespace.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        }
    }

    fn db_name(&self, d: DbId) -> String {
        self.cat
            .databases
            .get(d)
            .map_or_else(|| format!("#{d}"), |r| r.name.clone())
    }

    fn object_name(&self, o: ObjectId) -> String {
        self.cat.objects.get(o).map_or_else(
            || format!("{}.#{o}", self.name),
            |r| format!("{}.{}.{}", self.name, self.db_name(r.db_id), r.name),
        )
    }

    fn ancestor_name(&self, a: TenantId) -> String {
        self.cat
            .ancestry
            .iter()
            .find(|r| r.id == a && !r.name.is_empty())
            .map_or_else(|| format!("#{a}"), |r| r.name.clone())
    }

    /// `scope` in [`ScopeRef`] syntax, with names.
    fn scope_text(&self, scope: &Scope) -> String {
        match scope {
            Scope::Tenant(t) if *t == self.id => format!("tenant:{}", self.name),
            Scope::Tenant(a) => format!("tenant:{}", self.ancestor_name(*a)),
            Scope::Database(_, d) => format!("db:{}.{}", self.name, self.db_name(*d)),
            Scope::Object(_, o) => format!("object:{}", self.object_name(*o)),
            other => etcd_scope_text(other),
        }
    }

    /// A catalog write's warning, with names instead of ids.
    fn warning_view(&self, x: &Warning) -> SettingWarningView {
        let named = self.scope_text(&x.scope);
        let raw = match &x.scope {
            Scope::Database(_, d) => format!("at database {d}"),
            Scope::Object(_, o) => format!("at object {o}"),
            _ => String::new(),
        };
        SettingWarningView {
            message: if raw.is_empty() {
                x.message.clone()
            } else {
                x.message.replace(&raw, &format!("at {named}"))
            },
            scope: named,
            value: x.value.clone(),
        }
    }

    fn row(&self, scope: SettingScope, key: &str) -> Option<&SettingRow> {
        self.rows.get(&(scope, key.to_owned()))
    }

    fn setting_scope(&self, scope: &Scope) -> Option<SettingScope> {
        match scope {
            Scope::Tenant(t) if *t == self.id => Some(SettingScope::Tenant),
            Scope::Tenant(a) => Some(SettingScope::Ancestor(*a)),
            Scope::Database(_, d) => Some(SettingScope::Database(*d)),
            Scope::Object(_, o) => Some(SettingScope::Object(*o)),
            _ => None,
        }
    }
}

fn etcd_scope_text(scope: &Scope) -> String {
    match scope {
        Scope::Global => "global".to_owned(),
        Scope::Dc(d) => format!("dc:{d}"),
        Scope::Cluster(c) => format!("cluster:{c}"),
        other => format!("{other:?}"),
    }
}

/// The Admin API's settings calls over one controller's etcd mirror and
/// tenant catalogs.
pub struct SettingsService<B>
where
    B: SettingsBackend + DdlBackend,
    B::Tx: SettingsTx + DdlTx,
{
    live: LiveSettings,
    etcd: SettingsWriter,
    catalog: TenantSettingsWriter<B>,
    directory: Directory,
    clock: Arc<dyn Clock>,
}

impl<B> std::fmt::Debug for SettingsService<B>
where
    B: SettingsBackend + DdlBackend,
    B::Tx: SettingsTx + DdlTx,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SettingsService")
            .field("dc", self.dc())
            .finish_non_exhaustive()
    }
}

impl<B> SettingsService<B>
where
    B: SettingsBackend + DdlBackend,
    B::Tx: SettingsTx + DdlTx,
{
    /// The service of a controller: `live` is its etcd mirror (registry,
    /// stores and DC included), `versioner` reaches tenant catalogs,
    /// `directory` names tenants; `clock` stamps `set_at`.
    #[must_use]
    pub fn new(
        live: LiveSettings,
        versioner: Versioner<B>,
        directory: Directory,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let registry = live.registry();
        // Chapter 29: auth.authorizer only names an engine every live
        // gateway knows.
        // bugfix-62 (Q39): txn.commit_resolve_timeout_ms stays below
        // txn.resolver_min_age_seconds.
        let guard: Arc<dyn sprig_settings::ValueGuard> = Arc::new(sprig_settings::AllGuards(vec![
            Arc::new(sprig_auth::authz::engine_guard::EngineGuard::new(
                Arc::clone(&live.stores().global),
                Arc::clone(&live.stores().dc),
            )),
            Arc::new(sprig_txn::settings::ResolveWindowGuard::new(
                live.clone(),
                live.stores().dc_id().clone(),
            )),
        ]));
        let etcd = SettingsWriter::new(registry, live.stores().clone(), Arc::clone(&clock))
            .with_guard(Arc::clone(&guard));
        let catalog = TenantSettingsWriter::new(
            versioner,
            registry,
            Arc::new(live.clone()),
            Arc::new(AllowAll),
            Arc::new(LogDecisions),
            Arc::clone(&clock),
        )
        .with_guard(guard);
        Self {
            live,
            etcd,
            catalog,
            directory,
            clock,
        }
    }

    fn registry(&self) -> &'static Registry {
        self.live.registry()
    }

    fn dc(&self) -> &DcId {
        self.live.stores().dc_id()
    }

    /// A tenant homed here, active, with its catalog read from the writer.
    async fn tenant(&self, name: &str) -> Result<TenantRead> {
        let e = self.directory.require(name).await?;
        let b = e.body();
        if b.home_dc != *self.dc() {
            return Err(invalid(format!(
                "tenant {name} is homed in dc {}; ask that DC's controller",
                b.home_dc
            )));
        }
        if b.state != TenantState::Active {
            return Err(SprigError::new(
                SprigCode::TenantNotReady,
                format!("tenant {name} is {}", b.state.as_str()),
            ));
        }
        let id = b.tenant_id;
        let snap = self.catalog.versioner().backend().full_snapshot(id).await?;
        let rows = snap
            .settings
            .iter()
            .map(|r| ((r.scope, r.key.clone()), r.clone()))
            .collect();
        let cat = TenantCatalog::build(self.registry(), id, Loaded::core(snap));
        Ok(TenantRead {
            id,
            name: name.to_owned(),
            cat,
            rows,
        })
    }

    fn check_dc(&self, d: &DcId) -> Result<()> {
        if d == self.dc() {
            return Ok(());
        }
        Err(invalid(format!(
            "this controller keeps dc {}'s settings; ask dc {d}'s controller",
            self.dc()
        )))
    }

    fn check_cluster(&self, c: &ClusterId) -> Result<()> {
        let ours = c
            .as_str()
            .strip_prefix(self.dc().as_str())
            .is_some_and(|r| r.starts_with('-'));
        if ours {
            return Ok(());
        }
        Err(invalid(format!(
            "cluster {c} isn't in dc {}; ask its DC's controller",
            self.dc()
        )))
    }

    async fn chain_view(
        &self,
        def: &dyn AnySetting,
        e: &Explanation,
        t: Option<&TenantRead>,
    ) -> Result<Vec<SettingStepView>> {
        let mut out = Vec::new();
        for s in &e.steps {
            let stored = s.entry.as_ref().and_then(|x| x.value.clone());
            let mut v = SettingStepView {
                scope: scope_name(s.kind).to_owned(),
                target: None,
                accepted: s.accepted,
                value: match s.kind {
                    ScopeKind::Default => Some(def.default_value()),
                    _ => stored,
                },
                bounds: s.entry.as_ref().and_then(|x| x.bounds.clone()),
                bounds_in_force: s.bounds_in_force.clone(),
                outcome: outcome_name(s.outcome).to_owned(),
                clamped_to: s.clamped_to.clone(),
                set_by: None,
                set_at: None,
                reason: None,
                version: None,
            };
            match &s.scope {
                None => {}
                Some(sc @ (Scope::Global | Scope::Dc(_) | Scope::Cluster(_))) => {
                    v.target = match sc {
                        Scope::Dc(d) => Some(d.to_string()),
                        Scope::Cluster(c) => Some(c.to_string()),
                        _ => None,
                    };
                    if s.entry.is_some()
                        && let Some((rec, _)) = self.etcd.read(sc, e.key).await?
                    {
                        let r = record_view(&rec.body);
                        (v.set_by, v.set_at, v.reason) = (r.set_by, r.set_at, r.reason);
                    }
                }
                Some(sc) => {
                    if let Some(t) = t {
                        let text = t.scope_text(sc);
                        v.target = text.split_once(':').map(|(_, x)| x.to_owned());
                        if let Some(row) = t.setting_scope(sc).and_then(|ss| t.row(ss, e.key)) {
                            v.set_by = Some(row.set_by.clone()).filter(|x| !x.is_empty());
                            v.set_at = Some(rfc3339(row.set_at));
                            v.version = Some(row.version);
                        }
                    }
                }
            }
            out.push(v);
        }
        Ok(out)
    }

    async fn write_etcd(
        &self,
        def: &'static dyn AnySetting,
        w: &SettingWrite,
        guard: &[Cmp],
        set_by: &str,
    ) -> Result<SettingWriteResponse> {
        let scope = match &w.scope {
            ScopeRef::Global => Scope::Global,
            ScopeRef::Dc(d) => {
                if let Some(d) = d {
                    self.check_dc(d)?;
                }
                Scope::Dc(self.dc().clone())
            }
            ScopeRef::Cluster(c) => {
                self.check_cluster(c)?;
                Scope::Cluster(c.clone())
            }
            _ => return Err(SprigError::internal("not an etcd scope")),
        };
        let mut done: Vec<RecordOutcome> = Vec::new();
        for edit in edits_of(def, &w.op)? {
            done.push(
                self.etcd
                    .set_guarded(guard, &scope, def.key(), edit, set_by, &w.reason)
                    .await?,
            );
        }
        let previous = done
            .iter()
            .find(|o| o.changed)
            .and_then(|o| o.previous.as_ref())
            .map(record_view);
        let changed = done.iter().any(|o| o.changed);
        let current = if changed {
            done.iter()
                .rev()
                .find(|o| o.changed)
                .and_then(|o| o.current.as_ref())
                .map(record_view)
        } else {
            // Nothing changed: show the record as it stands.
            self.etcd
                .read(&scope, def.key())
                .await?
                .map(|(r, _)| record_view(&r.body))
        };
        let warnings: Vec<SettingWarningView> = done
            .iter()
            .flat_map(|o| &o.warnings)
            .map(|x: &Warning| SettingWarningView {
                scope: etcd_scope_text(&x.scope),
                value: x.value.clone(),
                message: x.message.clone(),
            })
            .collect();
        Ok(SettingWriteResponse {
            key: def.key().to_owned(),
            scope: w.scope.clone(),
            action: w.op.action().to_owned(),
            changed,
            version: None,
            previous,
            current,
            summary: summary(&warnings),
            warnings,
            set_by: set_by.to_owned(),
        })
    }

    async fn write_catalog(
        &self,
        def: &'static dyn AnySetting,
        w: &SettingWrite,
        actor: &str,
    ) -> Result<SettingWriteResponse> {
        let name = w.scope.tenant().unwrap_or_default();
        let t = self.tenant(name).await?;
        let target = match &w.scope {
            ScopeRef::Tenant(_) => Target::Tenant,
            ScopeRef::Database { db, .. } => Target::Database(t.db(db)?),
            ScopeRef::Object { db, object, .. } => {
                let d = t.db(db)?;
                Target::Object(t.object(d, db, object)?)
            }
            _ => return Err(SprigError::internal("not a catalog scope")),
        };
        let row_scope = target.scope();
        let stored = t.row(row_scope, def.key());
        let stored_bounds = stored.and_then(|r| r.bounds.clone());
        let mut changes = Vec::new();
        for edit in edits_of(def, &w.op)? {
            changes.push(match edit {
                SettingEdit::Change(c) => c,
                SettingEdit::PatchBounds(p) => match p.apply(stored_bounds.as_ref()) {
                    Some(b) => SettingChange::SetBounds(b),
                    None => SettingChange::ResetBounds,
                },
            });
        }
        let principal = Principal::Operator(actor.to_owned());
        let set_by = principal.set_by();
        let mut version = None;
        let mut current = None;
        let mut warnings = Vec::new();
        let mut summaries = Vec::new();
        for change in changes {
            let o = self
                .catalog
                .set(&SetRequest {
                    tenant: t.id,
                    target,
                    key: def.key().to_owned(),
                    change,
                    principal: principal.clone(),
                    reason: w.reason.clone(),
                })
                .await?;
            if o.version.is_some() {
                version = o.version;
                current = Some(o.current.clone());
            }
            warnings.extend(o.warnings.iter().map(|x| t.warning_view(x)));
            summaries.extend(o.summary);
        }
        let entry_view = |e: Option<sprig_settings::Entry>| {
            e.filter(|e| e.value.is_some() || e.bounds.is_some())
                .map(|e| SettingRowView {
                    value: e.value,
                    bounds: e.bounds,
                    ..SettingRowView::default()
                })
        };
        let previous = stored
            .and_then(|r| r.entry().ok())
            .and_then(|e| entry_view(Some(e)))
            .map(|mut v| {
                v.set_by = stored.map(|r| r.set_by.clone());
                v.set_at = stored.map(|r| rfc3339(r.set_at));
                v
            });
        let current = match current {
            Some(c) => entry_view(c).map(|mut v| {
                v.set_by = Some(set_by.clone());
                v.set_at = Some(rfc3339(self.clock.wall()));
                v
            }),
            None => previous.clone(),
        };
        let summary = if summaries.is_empty() {
            None
        } else {
            Some(summaries.join("; "))
        };
        Ok(SettingWriteResponse {
            key: def.key().to_owned(),
            scope: w.scope.clone(),
            action: w.op.action().to_owned(),
            changed: version.is_some(),
            version,
            previous,
            current,
            warnings,
            summary,
            set_by,
        })
    }
}

/// `op:<actor>`'s actor: trimmed, `admin` when empty.
fn actor_of(w: &SettingWrite) -> Result<String> {
    let a = w.actor.trim();
    if a.len() > 200 || a.chars().any(char::is_control) {
        return Err(invalid("actor: at most 200 printable characters"));
    }
    Ok(if a.is_empty() {
        "admin".to_owned()
    } else {
        a.to_owned()
    })
}

#[async_trait::async_trait]
impl<B> SettingsAdmin for SettingsService<B>
where
    B: SettingsBackend + DdlBackend,
    B::Tx: SettingsTx + DdlTx,
{
    async fn list_keys(&self, q: &ListSettingKeysQuery) -> Result<ListSettingKeysResponse> {
        list_keys(self.registry(), q)
    }

    async fn explain(&self, key: &str, q: &SettingContextQuery) -> Result<SettingExplanation> {
        let def = def_of(self.registry(), key)?;
        let dc = match &q.dc {
            Some(d) => DcId::new(d.as_str()).map_err(|e| invalid(format!("dc {d:?}: {e}")))?,
            None => self.dc().clone(),
        };
        self.check_dc(&dc)?;
        let cluster = q
            .cluster
            .as_deref()
            .map(|c| ClusterId::new(c).map_err(|e| invalid(format!("cluster {c:?}: {e}"))))
            .transpose()?;
        if let Some(c) = &cluster {
            self.check_cluster(c)?;
        }
        let (db, object) = match (q.db.as_deref(), q.object.as_deref()) {
            (None, Some(o)) => match o.split_once('.') {
                Some((d, o)) => (Some(d), Some(o)),
                None => return Err(invalid("an object needs its database: db=<db>")),
            },
            other => other,
        };
        if q.tenant.is_none() && db.is_some() {
            return Err(invalid(
                "a database or object needs its tenant: tenant=<name>",
            ));
        }
        let tenant = match &q.tenant {
            Some(n) => Some(self.tenant(n).await?),
            None => None,
        };
        let etcd = self.live.snapshot();
        let mut ids = (None, None);
        if let (Some(t), Some(d)) = (&tenant, db) {
            let db_id = t.db(d)?;
            ids.0 = Some(db_id);
            if let Some(o) = object {
                ids.1 = Some(t.object(db_id, d, o)?);
            }
        }
        let e = {
            let base = match &tenant {
                Some(t) => t.cat.setting_ctx(&dc),
                None => SettingCtx::process(&dc),
            };
            let mut ctx = base;
            if let Some(c) = &cluster {
                ctx = ctx.with_cluster(c);
            }
            if let Some(d) = ids.0 {
                ctx = ctx.with_db(d);
            }
            if let Some(o) = ids.1 {
                ctx = ctx.with_object(o);
            }
            explain_layered(&etcd, tenant.as_ref().map(|t| &*t.cat.settings), key, &ctx)?
        };
        let chain = self.chain_view(def, &e, tenant.as_ref()).await?;
        let src = chain.iter().find(|s| s.outcome == "effective").map_or_else(
            || ("default".to_owned(), None),
            |s| (s.scope.clone(), s.target.clone()),
        );
        Ok(SettingExplanation {
            key: def.key().to_owned(),
            declaration: key_view(def),
            context: SettingContextView {
                dc,
                cluster,
                tenant: tenant.as_ref().map(|t| t.name.clone()),
                db: db.map(str::to_owned),
                object: object.map(str::to_owned),
            },
            value: e.resolved.value.clone(),
            source: src.0,
            source_target: src.1,
            clamped: e.resolved.clamped,
            bounds: e.resolved.bounds.clone(),
            chain,
            catalog_version: tenant.as_ref().map(|t| t.cat.version),
        })
    }

    async fn write(
        &self,
        key: &str,
        w: &SettingWrite,
        guard: Vec<Cmp>,
    ) -> Result<SettingWriteResponse> {
        let def = check_key_and_scope(self.registry(), key, &w.scope)?;
        let actor = actor_of(w)?;
        // Chapter 47: Admin API mutations fail closed while the audit
        // spool is full.
        sprig_common::audit::check_available()?;
        let r = if w.scope.is_etcd() {
            self.write_etcd(def, w, &guard, &format!("op:{actor}"))
                .await?
        } else {
            self.write_catalog(def, w, &actor).await?
        };
        audit(def.key(), w, &r);
        Ok(r)
    }

    async fn tenant_settings(&self, name: &str) -> Result<TenantSettingsResponse> {
        let t = self.tenant(name).await?;
        let mut rows: Vec<(u8, TenantSettingRowView)> = t
            .rows
            .values()
            .filter(|r| !r.is_internal() && !r.is_empty())
            .map(|r| {
                let (rank, scope, target) = match r.scope {
                    SettingScope::Tenant => (0, "tenant", t.name.clone()),
                    SettingScope::Database(d) => {
                        (1, "database", format!("{}.{}", t.name, t.db_name(d)))
                    }
                    SettingScope::Object(o) => (2, "object", t.object_name(o)),
                    SettingScope::Ancestor(a) => (3, "ancestor", t.ancestor_name(a)),
                };
                let e = r.entry().ok();
                (
                    rank,
                    TenantSettingRowView {
                        scope: scope.to_owned(),
                        target,
                        key: r.key.clone(),
                        value: e.as_ref().and_then(|e| e.value.clone()),
                        bounds: e.and_then(|e| e.bounds),
                        set_by: r.set_by.clone(),
                        set_at: rfc3339(r.set_at),
                        version: r.version,
                    },
                )
            })
            .collect();
        rows.sort_by(|a, b| (a.0, &a.1.target, &a.1.key).cmp(&(b.0, &b.1.target, &b.1.key)));
        Ok(TenantSettingsResponse {
            tenant: t.name.clone(),
            tenant_id: t.id,
            catalog_version: t.cat.version,
            rows: rows.into_iter().map(|(_, r)| r).collect(),
        })
    }
}

#[cfg(test)]
mod live_tests;
#[cfg(test)]
mod tests;
