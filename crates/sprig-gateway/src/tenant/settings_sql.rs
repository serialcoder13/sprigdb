//! Settings in SQL, executed (chapter 08, "Setting a value"; build step
//! 8): [`sprig_sql::settings_stmt`] parses, this resolves names against the
//! session's snapshot and writes through the catalog's
//! [`TenantSettingsWriter`](sprig_catalog::TenantSettingsWriter) as the
//! session's user.
//!
//! - **Targets.** `ALTER TABLE t` and `ALTER DATABASE d` resolve against
//!   the session's snapshot (1146, 1049, 1046 as MySQL words them; the
//!   writer checks again under the catalog lock). `ALTER TENANT` without a
//!   name, `SPRIG SET` and `SPRIG RESET` are the session's tenant. `ALTER
//!   TENANT <name>` writes a descendant's catalog: the named tenant's
//!   directory path must contain the session's tenant, else 1227 (also for
//!   a name that doesn't exist, so the answer doesn't reveal other
//!   tenants).
//! - **Values** are parsed with each key's type (39084); a statement with
//!   several keys writes them one by one, in order, and stops at the first
//!   refusal (each write is its own catalog change).
//! - **Bounds.** `SET BOUNDS` changes only the parts it names, merged with
//!   the row in the session's snapshot (chapter 08, "Bounds changes").
//! - **Privileges** are chapter 29's; until then the writer's allow-all
//!   hook passes every write, and `operator_only` keys are refused with
//!   1227 (decision N3).
//! - **Warnings.** A bounds change that will clamp or ignore narrower
//!   values answers OK with one warning per value (code 39080), shown by
//!   `SHOW WARNINGS`.
//! - **`SHOW SPRIG SETTINGS`** explains every key accepted at the target's
//!   scope (filtered by `LIKE`), with [`explain_layered`] over the
//!   process's etcd scopes and the snapshot's rows: the effective value,
//!   where it came from, the bounds in force and whether it was clamped.

use std::fmt::Write as _;
use std::sync::Arc;

use bytes::Bytes;
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::{DbState, Namespace, ObjectKind, ObjectState};
use sprig_catalog::{Principal, SetRequest, Target, TenantCatalog};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Consistency, Record, Versioned};
use sprig_proto_mysql::ColumnDef;
use sprig_settings::{
    AnySetting, BoundsPatch, ScopeKind, SettingChange, TenantScope, Value, explain_layered,
    show_bounds,
};
use sprig_sql::names::LocalAnswer;
use sprig_sql::settings_stmt::{
    BoundsSpec, SettingsOp, SettingsStmt, SettingsTarget, like_matches, parse_value,
};

use super::TenantServices;
use super::session::{LocalWarning, TenantSession};
use crate::pipeline::text_column;

/// What a settings statement answers.
#[derive(Debug)]
pub enum SettingsAnswer {
    /// OK, with the gateway's warnings.
    Ok {
        /// Warnings for `SHOW WARNINGS`.
        warnings: Vec<LocalWarning>,
    },
    /// A resultset (`SHOW SPRIG SETTINGS`).
    Rows(LocalAnswer),
}

fn access_denied(what: &str) -> SprigError {
    SprigError::standard(1227, *b"42000", format!("Access denied; {what}"))
}

fn no_database() -> SprigError {
    SprigError::standard(1046, *b"3D000", "No database selected")
}

/// A resolved target: the tenant whose catalog holds the row, and where.
struct Resolved {
    tenant: TenantId,
    target: Target,
    /// The snapshot to read rows from (the descendant's, for `ALTER
    /// TENANT <name>`).
    cat: Arc<TenantCatalog>,
    schema: Option<String>,
    table: Option<String>,
}

/// The fields of a directory entry the descendant check reads.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Entry {
    #[serde(default)]
    path: String,
}

impl Versioned for Entry {
    const V: u32 = 1;
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct NameEntry {
    tenant_id: u64,
}

impl Versioned for NameEntry {
    const V: u32 = 1;
}

/// The id of tenant `name` when it is a descendant of `me`.
async fn descendant(svc: &TenantServices, me: TenantId, name: &str) -> Result<Option<TenantId>> {
    let denied = || Ok(None);
    let key = svc.global.tier().key(&format!("tenant-names/{name}"))?;
    let Some(kv) = svc.global.get(&key, Consistency::Serializable).await? else {
        return denied();
    };
    let id = match std::str::from_utf8(&kv.value)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
    {
        Some(id) => id,
        None => Record::<NameEntry>::decode(&kv.value)?.body.tenant_id,
    };
    let key = svc.global.tier().key(&format!("tenants/{id}"))?;
    let Some(kv) = svc.global.get(&key, Consistency::Serializable).await? else {
        return denied();
    };
    let e = Record::<Entry>::decode(&kv.value)?.body;
    if id != me.0 && e.path.contains(&format!("/{me}/")) {
        Ok(Some(TenantId(id)))
    } else {
        denied()
    }
}

async fn resolve(
    ts: &TenantSession,
    cat: &Arc<TenantCatalog>,
    current: Option<&str>,
    target: &SettingsTarget,
) -> Result<Resolved> {
    let lctn = ts.lower_case_table_names();
    let own = |target| Resolved {
        tenant: ts.tenant,
        target,
        cat: Arc::clone(cat),
        schema: None,
        table: None,
    };
    match target {
        SettingsTarget::Tenant(None) => Ok(own(Target::Tenant)),
        SettingsTarget::Tenant(Some(name))
            if ts.principal.tenant_name.as_deref() == Some(name.as_str()) =>
        {
            Ok(own(Target::Tenant))
        }
        SettingsTarget::Tenant(Some(name)) => {
            let svc = ts.services();
            let Some(id) = descendant(svc, ts.tenant, name).await? else {
                return Err(access_denied(&format!(
                    "you need the SPRIG_TENANT_ADMIN privilege on tenant '{name}'"
                )));
            };
            let dcat = svc.cache.get(id, sprig_catalog::Freshness::Latest).await?;
            Ok(Resolved {
                tenant: id,
                target: Target::Tenant,
                cat: dcat,
                schema: None,
                table: None,
            })
        }
        SettingsTarget::Database(name) => {
            let name = name.as_deref().or(current).ok_or_else(no_database)?;
            let db = cat
                .database(&database_key(name, lctn))
                .filter(|d| d.state == DbState::Active)
                .ok_or_else(|| {
                    SprigError::standard(1049, *b"42000", format!("Unknown database '{name}'"))
                })?;
            Ok(Resolved {
                schema: Some(db.name.clone()),
                ..own(Target::Database(db.db_id))
            })
        }
        SettingsTarget::Table { db, name } => {
            let dbn = db.as_deref().or(current).ok_or_else(no_database)?;
            let missing = || {
                SprigError::standard(
                    1146,
                    *b"42S02",
                    format!("Table '{dbn}.{name}' doesn't exist"),
                )
            };
            let d = cat
                .database(&database_key(dbn, lctn))
                .filter(|d| d.state == DbState::Active)
                .ok_or_else(|| {
                    SprigError::standard(1049, *b"42000", format!("Unknown database '{dbn}'"))
                })?;
            let o = cat
                .object(
                    d.db_id,
                    Namespace::Relation,
                    &object_key(ObjectKind::Table, name, lctn),
                )
                .filter(|o| o.state == ObjectState::Active)
                .ok_or_else(missing)?;
            Ok(Resolved {
                schema: Some(d.name.clone()),
                table: Some(o.name.clone()),
                ..own(Target::Object(o.object_id))
            })
        }
    }
}

fn def(svc: &TenantServices, key: &str) -> Result<&'static dyn AnySetting> {
    svc.settings
        .registry()
        .get(key)
        .filter(|_| !key.starts_with("sprig."))
        .ok_or_else(|| SprigError::new(SprigCode::SettingUnknown, format!("unknown setting {key}")))
}

fn bounds_change(
    def: &dyn AnySetting,
    spec: &BoundsSpec,
    current: Option<&sprig_settings::Bounds>,
) -> Result<SettingChange> {
    let allowed = spec
        .allowed
        .as_ref()
        .map(|vs| {
            vs.iter()
                .map(|v| parse_value(def, v))
                .collect::<Result<Vec<Value>>>()
        })
        .transpose()?;
    let patch = BoundsPatch {
        min: spec.min.map(Some),
        max: spec.max.map(Some),
        allowed: allowed.map(Some),
        locked: spec.locked,
    };
    Ok(match patch.apply(current) {
        Some(b) => SettingChange::SetBounds(b),
        None => SettingChange::ResetBounds,
    })
}

fn tenant_scope(t: Target) -> TenantScope {
    match t {
        Target::Tenant => TenantScope::Tenant,
        Target::Database(d) => TenantScope::Database(d),
        Target::Object(o) => TenantScope::Object(o),
    }
}

/// Runs `stmt` for the session. `cat` is the snapshot the statement reads;
/// `current` the session's logical database.
///
/// # Errors
/// As the module docs list: 1046, 1049, 1146, 1227, 39080-39084, and the
/// catalog's (39070, 39071, 39072).
pub async fn execute(
    ts: &mut TenantSession,
    cat: &Arc<TenantCatalog>,
    current: Option<&str>,
    stmt: &SettingsStmt,
    charset: u16,
) -> Result<SettingsAnswer> {
    match stmt {
        SettingsStmt::Show { like, target } => {
            let r = resolve(ts, cat, current, target).await?;
            Ok(SettingsAnswer::Rows(show(
                ts.services(),
                &r,
                like.as_deref(),
                charset,
            )?))
        }
        SettingsStmt::Alter { target, op } => {
            let r = resolve(ts, cat, current, target).await?;
            let svc = Arc::clone(ts.services());
            let principal = match ts.principal.user_id {
                Some(u) => Principal::User(u),
                None => return Err(access_denied("settings need a catalog login")),
            };
            let mut changes: Vec<(String, SettingChange)> = Vec::new();
            let scope = tenant_scope(r.target);
            match op {
                SettingsOp::Set(items) => {
                    for (k, v) in items {
                        let d = def(&svc, k)?;
                        changes.push((k.clone(), SettingChange::Set(parse_value(d, v)?)));
                    }
                }
                SettingsOp::Reset(keys) => {
                    changes.extend(keys.iter().map(|k| (k.clone(), SettingChange::Reset)));
                }
                SettingsOp::SetBounds(items) => {
                    for (k, spec) in items {
                        let d = def(&svc, k)?;
                        let current = r
                            .cat
                            .settings
                            .entry(scope, d.key())
                            .and_then(|e| e.bounds.as_ref());
                        changes.push((k.clone(), bounds_change(d, spec, current)?));
                    }
                }
                SettingsOp::ResetBounds(keys) => {
                    changes.extend(keys.iter().map(|k| (k.clone(), SettingChange::ResetBounds)));
                }
            }
            let mut warnings = Vec::new();
            for (key, change) in changes {
                let out = svc
                    .writer
                    .set(&SetRequest {
                        tenant: r.tenant,
                        target: r.target,
                        key,
                        change,
                        principal: principal.clone(),
                        reason: "SQL".to_owned(),
                    })
                    .await?;
                if r.tenant == ts.tenant {
                    ts.changed(out.version);
                }
                warnings.extend(out.warnings.into_iter().map(|w| LocalWarning {
                    level: "Warning",
                    code: SprigCode::SettingOutOfBounds.number(),
                    message: w.message,
                }));
            }
            Ok(SettingsAnswer::Ok { warnings })
        }
    }
}

/// A value as `SHOW SPRIG SETTINGS` prints it: strings bare.
fn shown(v: &Value) -> String {
    match v {
        Value::Str(s) => s.clone(),
        other => sprig_settings::show_value(other),
    }
}

fn scope_name(k: ScopeKind, cat: &TenantCatalog) -> String {
    match k {
        ScopeKind::Ancestor(t) => match cat.ancestry.iter().find(|a| a.id == t) {
            Some(a) if !a.name.is_empty() => format!("ancestor {}", a.name),
            _ => k.to_string(),
        },
        other => other.to_string(),
    }
}

/// The columns of `SHOW SPRIG SETTINGS`, as `information_schema.SPRIG_SETTINGS`
/// names them (chapter 07).
#[must_use]
pub fn show_columns(charset: u16) -> Vec<ColumnDef> {
    [
        "SETTING_SCOPE",
        "TABLE_SCHEMA",
        "TABLE_NAME",
        "SETTING_NAME",
        "VALUE",
        "SOURCE_SCOPE",
        "BOUNDS",
        "CLAMPED",
    ]
    .iter()
    .map(|n| text_column(n, charset))
    .collect()
}

fn show(
    svc: &TenantServices,
    r: &Resolved,
    like: Option<&str>,
    charset: u16,
) -> Result<LocalAnswer> {
    let etcd = svc.settings.live().snapshot();
    let ancestry: Vec<TenantId> = r.cat.ancestry.iter().map(|a| a.id).collect();
    let mut ctx = sprig_settings::SettingCtx::process(&svc.dc).for_tenant(r.tenant, &ancestry);
    let kind = match r.target {
        Target::Tenant => ScopeKind::Tenant,
        Target::Database(d) => {
            ctx = ctx.with_db(d);
            ScopeKind::Database
        }
        Target::Object(o) => {
            if let Some(obj) = r.cat.objects.get(o) {
                ctx = ctx.with_db(obj.db_id);
            }
            ctx = ctx.with_object(o);
            ScopeKind::Object
        }
    };
    let mut rows = Vec::new();
    rows_for(svc, &etcd, r, &ctx, kind, like, &mut rows)?;
    Ok(LocalAnswer {
        columns: show_columns(charset),
        rows,
    })
}

/// One row per key accepted at `kind`, explained in `ctx`.
fn rows_for(
    svc: &TenantServices,
    etcd: &sprig_settings::MemSettings<'_>,
    r: &Resolved,
    ctx: &sprig_settings::SettingCtx<'_>,
    kind: ScopeKind,
    like: Option<&str>,
    rows: &mut Vec<Vec<Option<Bytes>>>,
) -> Result<()> {
    let mut keys: Vec<&'static dyn AnySetting> = svc
        .settings
        .registry()
        .iter()
        .filter(|d| d.scopes().contains(kind))
        .filter(|d| like.is_none_or(|p| like_matches(p, d.key())))
        .collect();
    keys.sort_by_key(|d| d.key());
    let text = |s: &str| Some(Bytes::copy_from_slice(s.as_bytes()));
    for d in keys {
        let e = explain_layered(etcd, Some(&r.cat.settings), d.key(), ctx)?;
        let res = &e.resolved;
        rows.push(vec![
            text(&kind.to_string()),
            r.schema.as_deref().and_then(text),
            r.table.as_deref().and_then(text),
            text(d.key()),
            text(&shown(&res.value)),
            text(&scope_name(res.source, &r.cat)),
            text(&show_bounds(&res.bounds)),
            text(if res.clamped { "YES" } else { "NO" }),
        ]);
    }
    Ok(())
}

/// Most rows `information_schema.SPRIG_SETTINGS` inlines
/// (`names.local_view_max_rows`'s default; the key isn't registered yet).
pub const VIEW_MAX_ROWS: usize = 10_000;

/// `information_schema.SPRIG_SETTINGS` (chapter 07, "SprigDB views";
/// build step 11, settings only): every key's effective value for the
/// tenant, each live database and each live table, as a derived table of
/// literal rows the rest of the query runs over in MariaDB. More than
/// [`VIEW_MAX_ROWS`] rows fail with 39052.
///
/// # Errors
/// 39052; 39081 never in practice (keys come from the registry).
#[allow(clippy::too_many_lines)] // the rows, then the SQL
pub fn view_sql(ts: &TenantSession, cat: &Arc<TenantCatalog>, nbe: bool) -> Result<String> {
    let svc = ts.services();
    let etcd = svc.settings.live().snapshot();
    let ancestry: Vec<TenantId> = cat.ancestry.iter().map(|a| a.id).collect();
    let base = sprig_settings::SettingCtx::process(&svc.dc).for_tenant(ts.tenant, &ancestry);
    let mut rows = Vec::new();
    let own = |target, schema: Option<String>, table: Option<String>| Resolved {
        tenant: ts.tenant,
        target,
        cat: Arc::clone(cat),
        schema,
        table,
    };
    rows_for(
        svc,
        &etcd,
        &own(Target::Tenant, None, None),
        &base,
        ScopeKind::Tenant,
        None,
        &mut rows,
    )?;
    let mut dbs: Vec<_> = cat
        .databases
        .iter()
        .filter(|d| d.state == DbState::Active)
        .collect();
    dbs.sort_by_key(|d| d.db_id);
    for d in &dbs {
        let r = own(Target::Database(d.db_id), Some(d.name.clone()), None);
        rows_for(
            svc,
            &etcd,
            &r,
            &base.with_db(d.db_id),
            ScopeKind::Database,
            None,
            &mut rows,
        )?;
        if rows.len() > VIEW_MAX_ROWS {
            return Err(too_large());
        }
    }
    let mut objs: Vec<_> = cat
        .objects
        .iter()
        .filter(|o| o.state == ObjectState::Active && o.namespace == Namespace::Relation)
        .filter(|o| !sprig_common::names::is_internal_object(&o.name))
        .collect();
    objs.sort_by_key(|o| o.object_id);
    for o in objs {
        let Some(d) = dbs.iter().find(|d| d.db_id == o.db_id) else {
            continue;
        };
        let r = own(
            Target::Object(o.object_id),
            Some(d.name.clone()),
            Some(o.name.clone()),
        );
        let ctx = base.with_db(o.db_id).with_object(o.object_id);
        rows_for(svc, &etcd, &r, &ctx, ScopeKind::Object, None, &mut rows)?;
        if rows.len() > VIEW_MAX_ROWS {
            return Err(too_large());
        }
    }
    let names = [
        "SETTING_SCOPE",
        "TABLE_SCHEMA",
        "TABLE_NAME",
        "SETTING_NAME",
        "VALUE",
        "SOURCE_SCOPE",
        "BOUNDS",
        "CLAMPED",
    ];
    let lit = |v: &Option<Bytes>| match v {
        Some(b) => sprig_sql::names::rewrite::sql_string(&String::from_utf8_lossy(b), nbe),
        None => "NULL".to_owned(),
    };
    let mut sql = String::from("(");
    if rows.is_empty() {
        let cols: Vec<String> = names
            .iter()
            .map(|n| format!("CAST(NULL AS CHAR) AS `{n}`"))
            .collect();
        let _ = write!(sql, "SELECT {} FROM DUAL WHERE 0", cols.join(", "));
    }
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            sql.push_str(" UNION ALL ");
        }
        sql.push_str("SELECT ");
        for (j, (v, n)) in row.iter().zip(names).enumerate() {
            if j > 0 {
                sql.push_str(", ");
            }
            if i == 0 {
                // CHAR so a NULL first value doesn't type the column.
                let _ = write!(sql, "CAST({} AS CHAR) AS `{n}`", lit(v));
            } else {
                sql.push_str(&lit(v));
            }
        }
    }
    sql.push(')');
    Ok(sql)
}

fn too_large() -> SprigError {
    SprigError::new(
        SprigCode::MetadataTooLarge,
        format!(
            "information_schema.SPRIG_SETTINGS would hold more than {VIEW_MAX_ROWS} rows; \
             use SHOW SPRIG SETTINGS ... FOR TABLE or FOR DATABASE"
        ),
    )
}

/// The statements a text referencing `information_schema.SPRIG_SETTINGS`
/// may hold: ones that run now and keep no text. First words, upper case.
const RUNS_NOW: &[&str] = &[
    "SELECT", "WITH", "TABLE", "VALUES", "(", "INSERT", "REPLACE", "UPDATE", "DELETE", "DO", "SET",
    "EXPLAIN", "DESCRIBE", "DESC",
];

/// Chapter 07 rule 9: the view is literal rows, so inlining it where
/// MariaDB keeps the text (a view, routine, trigger or event body, `CREATE
/// TABLE ... SELECT`, a compound statement's body) would bake a snapshot
/// of the tenant's settings into a stored object. A text that references
/// the view is inlined only when each of its statements starts with a
/// [`RUNS_NOW`] word (`SET STATEMENT ... FOR` excepted), else 39051, as
/// `names::plan` answers for the other mapped views. A `PREPARE` string
/// is never inlined (a string literal isn't scanned here), and `plan`
/// refuses it.
fn refuse_stored(text: &[u8], sig: &[&sprig_sql::lex::Token]) -> Option<SprigError> {
    use sprig_sql::lex::{TokenKind, ident_value};
    let word = |t: &sprig_sql::lex::Token| -> String {
        if t.kind == TokenKind::Word {
            String::from_utf8_lossy(&ident_value(t.bytes(text))).to_ascii_uppercase()
        } else {
            String::from_utf8_lossy(t.bytes(text)).into_owned()
        }
    };
    let mut start = true;
    for (i, t) in sig.iter().enumerate() {
        if t.bytes(text) == b";" {
            start = true;
            continue;
        }
        if !start {
            continue;
        }
        start = false;
        let w = word(t);
        let set_statement = w == "SET" && sig.get(i + 1).is_some_and(|n| word(n) == "STATEMENT");
        if !RUNS_NOW.contains(&w.as_str()) || set_statement {
            return Some(SprigError::new(
                SprigCode::NameRewriteUnsupported,
                "name rewrite unsupported: information_schema.SPRIG_SETTINGS in a prepared \
                 statement, view or stored program; read it with a statement of its own",
            ));
        }
    }
    None
}

/// Replaces each reference to `information_schema.SPRIG_SETTINGS` in
/// `text` with the derived table `view` builds (aliased `SPRIG_SETTINGS`
/// unless the statement gives its own alias). `None` when the text has
/// none.
///
/// # Errors
/// 39051 where the rows would be stored ([`refuse_stored`]); `view`'s.
pub fn inline_view(
    text: &[u8],
    lex: &sprig_sql::lex::LexOptions,
    view: impl FnOnce() -> Result<String>,
) -> Option<Result<Vec<u8>>> {
    use sprig_sql::lex::{TokenKind, ident_value};
    let tokens = sprig_sql::lex::lex(text, lex).ok()?;
    let sig: Vec<&sprig_sql::lex::Token> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    let is = |t: &sprig_sql::lex::Token, w: &str| {
        matches!(t.kind, TokenKind::Word | TokenKind::QuotedIdent)
            && ident_value(t.bytes(text)).eq_ignore_ascii_case(w.as_bytes())
    };
    let mut spans = Vec::new();
    for i in 0..sig.len().saturating_sub(2) {
        if is(sig[i], "information_schema")
            && sig[i + 1].bytes(text) == b"."
            && is(sig[i + 2], "SPRIG_SETTINGS")
        {
            const NOT_ALIAS: &[&str] = &[
                "WHERE",
                "JOIN",
                "LEFT",
                "RIGHT",
                "INNER",
                "CROSS",
                "ON",
                "GROUP",
                "ORDER",
                "LIMIT",
                "UNION",
                "HAVING",
                "NATURAL",
                "STRAIGHT_JOIN",
                "USING",
                "WINDOW",
                "FOR",
                "LOCK",
                "INTO",
                "EXCEPT",
                "INTERSECT",
                "PROCEDURE",
                "FULL",
                "OUTER",
            ];
            let aliased = sig.get(i + 3).is_some_and(|t| {
                is(t, "AS")
                    || (matches!(t.kind, TokenKind::Word | TokenKind::QuotedIdent)
                        && !NOT_ALIAS.iter().any(|w| is(t, w)))
            });
            spans.push((sig[i].start as usize, sig[i + 2].end as usize, aliased));
        }
    }
    if spans.is_empty() {
        return None;
    }
    if let Some(e) = refuse_stored(text, &sig) {
        return Some(Err(e));
    }
    let derived = match view() {
        Ok(v) => v,
        Err(e) => return Some(Err(e)),
    };
    let mut out = Vec::with_capacity(text.len() + derived.len());
    let mut at = 0;
    for (s, e, aliased) in spans {
        out.extend_from_slice(&text[at..s]);
        out.extend_from_slice(derived.as_bytes());
        if !aliased {
            out.extend_from_slice(b" AS `SPRIG_SETTINGS`");
        }
        at = e;
    }
    out.extend_from_slice(&text[at..]);
    Some(Ok(out))
}

#[cfg(test)]
mod tests;
