//! Row-level security in tenant sessions: the gateway's side.
//!
//! ```text
//! CREATE | ALTER | DROP POLICY, ALTER TABLE ... ROW LEVEL SECURITY:
//!   ALTER on the table (or SPRIG_TENANT_ADMIN); expressions validated
//!   (39364), subquery tables qualified with the policy's database and
//!   resolved in the tenant, cycles refused (39365); one auth change; the
//!   MariaDB write checks redeployed (exec::rls)
//! SHOW POLICIES, SHOW CREATE POLICY: tables the session may see
//! every other statement (exec::rls):
//!   @sprig_rls named by the client          → 39050
//!   the session's protected tables          → sprig_sql::rls::inject
//!   the covered tables                      → the privilege check lets them pass;
//!                                             any other filtered table is 39366
//! ```

#![allow(
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::trivially_copy_pass_by_ref,
    clippy::match_same_arms,
    clippy::assigning_clones,
    clippy::type_complexity,
    clippy::too_many_arguments,
    clippy::format_push_string,
    clippy::too_many_lines
)] // pedantic style: names follow the chapter's pseudocode

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use sprig_auth::TenantAuth;
use sprig_auth::authz::rls::{Clause, NewPolicy, RlsState, RlsStore, SessionRls, TableRls, cmd};
use sprig_auth::authz::{Action, AuthzState, Grantee, SessionGrants, Target};
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::{DbState, Namespace, ObjectKind, ObjectState};
use sprig_catalog::{CatalogLocator, ChangeOpts, Freshness, TenantCatalog, VersionKind};
use sprig_common::ids::{DbId, ObjectId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_sql::lex::LexOptions;
use sprig_sql::names::LocalAnswer;
use sprig_sql::rls::expr::{self, LiteralBinder};
use sprig_sql::rls::stmt::{
    Commands, PolicyGrantee, PolicyMode, PolicyStmt, SecuritySwitch, TableName, create_sql,
};
use sprig_sql::rls::{Cmd, Resolver};

use super::session::TenantSession;

/// Per-session row-level security state.
#[derive(Debug, Default)]
pub struct RlsSession {
    /// Verified claims of the session's token (`SPRIG_CLAIM`), or the
    /// simulated ones of Studio's "view as".
    pub claims: Option<Arc<serde_json::Value>>,
    /// The protected tables the last rewritten statement covers: the
    /// privilege check lets these pass.
    pub covered: Vec<ObjectId>,
}

fn invalid(why: impl std::fmt::Display) -> SprigError {
    expr::invalid(why)
}

fn no_table(db: &str, t: &str) -> SprigError {
    SprigError::standard(1146, *b"42S02", format!("Table '{db}.{t}' doesn't exist"))
}

/// 1305, as MySQL words a missing stored object.
fn no_policy(name: &str, t: &str) -> SprigError {
    SprigError::standard(
        1305,
        *b"42000",
        format!("POLICY {name} ON {t} does not exist"),
    )
}

/// 1304, as MySQL words an existing stored object.
fn policy_exists(name: &str, t: &str) -> SprigError {
    SprigError::standard(
        1304,
        *b"42000",
        format!("POLICY {name} ON {t} already exists"),
    )
}

/// The live table a statement names, with its database's name key.
fn table_of(
    cat: &TenantCatalog,
    t: &TableName,
    current: Option<&str>,
    lctn: bool,
) -> Result<(DbId, String, ObjectId, String)> {
    let db =
        t.db.as_deref().or(current).ok_or_else(|| {
            SprigError::standard(1046, *b"3D000", "No database selected".to_owned())
        })?;
    let key = database_key(db, lctn);
    let row = cat
        .database(&key)
        .filter(|d| d.state == DbState::Active)
        .ok_or_else(|| SprigError::standard(1049, *b"42000", format!("Unknown database '{db}'")))?;
    let okey = object_key(ObjectKind::Table, &t.name, lctn);
    let obj = cat
        .object(row.db_id, Namespace::Relation, &okey)
        .filter(|o| o.state == ObjectState::Active && o.kind == ObjectKind::Table)
        .ok_or_else(|| no_table(db, &t.name))?;
    Ok((row.db_id, key, obj.object_id, obj.name.clone()))
}

/// What a policy statement answers.
#[derive(Debug)]
pub enum PolicyAnswer {
    /// OK; the auth version and the table whose write checks must follow.
    Changed {
        /// The auth version of the change.
        version: u64,
        /// The table.
        object: ObjectId,
    },
    /// Nothing changed (`DROP POLICY IF EXISTS` of a missing one).
    Unchanged,
    /// Rows.
    Rows(LocalAnswer),
}

/// The account text of a grantee, for `SHOW`.
fn grantee_text(cat: &TenantCatalog, g: &Grantee) -> String {
    match g {
        Grantee::User(u) => TenantAuth::of(cat).and_then(|a| a.user(*u)).map_or_else(
            || format!("<user {}>", u.0),
            |u| {
                format!(
                    "`{}`@`{}`",
                    u.row.name.replace('`', "``"),
                    u.row.host.replace('`', "``")
                )
            },
        ),
        Grantee::Role(r) => AuthzState::of(cat).and_then(|s| s.role(*r)).map_or_else(
            || format!("<role {}>", r.0),
            |r| {
                format!(
                    "`{}`@`{}`",
                    r.name.replace('`', "``"),
                    r.host.replace('`', "``")
                )
            },
        ),
    }
}

fn resolve_grantees(
    ts: &TenantSession,
    cat: &TenantCatalog,
    to: &[PolicyGrantee],
) -> Result<Vec<Grantee>> {
    let state = AuthzState::of(cat).cloned().unwrap_or_default();
    let mut out = Vec::new();
    for g in to {
        let gr = match g {
            PolicyGrantee::Public => continue,
            PolicyGrantee::CurrentUser => {
                let u = ts
                    .principal
                    .user_id
                    .ok_or_else(|| invalid("CURRENT_USER has no account here"))?;
                Grantee::User(u)
            }
            PolicyGrantee::Account(a) => {
                let user = TenantAuth::of(cat).and_then(|au| {
                    au.accounts(&a.name)
                        .iter()
                        .find(|u| u.row.host.eq_ignore_ascii_case(&a.host))
                        .map(|u| u.row.user_id)
                });
                match user {
                    Some(u) => Grantee::User(u),
                    None => Grantee::Role(state.role_named(&a.name, &a.host).ok_or_else(|| {
                        SprigError::standard(
                            3523,
                            *b"HY000",
                            format!("Unknown authorization ID `{}`@`{}`", a.name, a.host),
                        )
                    })?),
                }
            }
        };
        if !out.contains(&gr) {
            out.push(gr);
        }
    }
    Ok(out)
}

/// Validates an expression and qualifies its subquery tables with `db`;
/// the tables it reads.
fn checked_expr(
    cat: &TenantCatalog,
    text: &str,
    lex: &LexOptions,
    db: &str,
    lctn: bool,
) -> Result<(String, Vec<ObjectId>)> {
    let info = expr::validate(text, lex)?;
    let q = expr::qualify_tables(text, lex, db)?;
    let mut tables = Vec::new();
    for t in &info.tables {
        let (_, _, o, _) = table_of(cat, t, Some(db), lctn)
            .map_err(|_| invalid(format!("table '{}' doesn't exist", t.name)))?;
        tables.push(o);
    }
    Ok((q, tables))
}

/// Tables `object`'s policies read through subqueries (now, with `extra`
/// replacing its own).
fn reads(
    cat: &TenantCatalog,
    state: &RlsState,
    object: ObjectId,
    lex: &LexOptions,
) -> Vec<ObjectId> {
    let mut out = Vec::new();
    for p in state.policies_of(object) {
        for e in [p.using_expr.as_deref(), p.check_expr.as_deref()]
            .into_iter()
            .flatten()
        {
            if let Ok(info) = expr::validate(e, lex) {
                let db = cat
                    .objects
                    .get(object)
                    .and_then(|o| cat.databases.get(o.db_id))
                    .map(|d| d.name.clone())
                    .unwrap_or_default();
                for t in &info.tables {
                    if let Ok((_, _, o, _)) = table_of(cat, t, Some(&db), false) {
                        out.push(o);
                    }
                }
            }
        }
    }
    out
}

/// 39365 if `object`'s policies reading `new_reads` would close a cycle.
fn check_cycle(
    cat: &TenantCatalog,
    state: &RlsState,
    object: ObjectId,
    new_reads: &[ObjectId],
    lex: &LexOptions,
) -> Result<()> {
    let mut seen = HashSet::new();
    let mut stack: Vec<ObjectId> = new_reads.to_vec();
    while let Some(o) = stack.pop() {
        if o == object {
            return Err(SprigError::new(
                SprigCode::RlsPolicyCycle,
                "ER_SPRIG_RLS_POLICY_CYCLE: policy subqueries would read the table back",
            ));
        }
        if seen.insert(o) {
            stack.extend(reads(cat, state, o, lex));
        }
    }
    Ok(())
}

/// Runs a policy statement (see the module docs). The caller deploys the
/// write checks of [`PolicyAnswer::Changed`]'s table.
///
/// # Errors
/// 1142 without `ALTER`; 1146, 39364, 39365; the catalog's.
#[allow(clippy::too_many_lines)] // one arm per statement
pub async fn statement(
    ts: &mut TenantSession,
    cat: &Arc<TenantCatalog>,
    stmt: &PolicyStmt,
    current: Option<&str>,
    lex: &LexOptions,
    charset: u16,
) -> Result<PolicyAnswer> {
    let lctn = ts.lower_case_table_names();
    let p = super::authz::privileges(ts, cat);
    let state = RlsState::of(cat).cloned().unwrap_or_default();
    let table = match stmt {
        PolicyStmt::Create { table, .. }
        | PolicyStmt::Rename { table, .. }
        | PolicyStmt::Alter { table, .. }
        | PolicyStmt::Drop { table, .. }
        | PolicyStmt::Security { table, .. }
        | PolicyStmt::ShowCreate { table, .. } => Some(table),
        PolicyStmt::Show { table, .. } => table.as_ref(),
    };
    let resolved = table.map(|t| table_of(cat, t, current, lctn)).transpose()?;
    let manage = |db_key: &str, db: DbId, object: ObjectId| -> Result<()> {
        let target = Target::Object { db_key, db, object };
        if p.unrestricted() || p.allows(Action::Alter, target) {
            return Ok(());
        }
        Err(sprig_auth::authz::account_sql::table_denied(
            sprig_proto_mysql::Persona::MariaDb,
            "ALTER",
            &ts.principal.user,
            &ts.principal.client_ip.to_string(),
            current.unwrap_or_default(),
            table.map_or("", |t| t.name.as_str()),
        ))
    };
    if let PolicyStmt::Show { db, .. } = stmt {
        return Ok(PolicyAnswer::Rows(show(
            ts,
            cat,
            &*p,
            &state,
            db.as_deref().or(current),
            resolved.as_ref(),
            lctn,
            charset,
        )));
    }
    let (db, db_key, object, tname) =
        resolved.ok_or_else(|| SprigError::internal("a policy statement without a table"))?;
    let db_name = cat
        .databases
        .get(db)
        .map(|d| d.name.clone())
        .unwrap_or_default();
    if let PolicyStmt::ShowCreate { name, .. } = stmt {
        let see = p.unrestricted()
            || p.allows(
                Action::Select,
                Target::Object {
                    db_key: &db_key,
                    db,
                    object,
                },
            )
            || p.allows(
                Action::Alter,
                Target::Object {
                    db_key: &db_key,
                    db,
                    object,
                },
            );
        let pol = state
            .policies_of(object)
            .find(|x| x.name == *name)
            .filter(|_| see)
            .ok_or_else(|| no_policy(name, &tname))?;
        let to: Vec<String> = pol.grantees.iter().map(|g| grantee_text(cat, g)).collect();
        let sql = create_sql(
            &pol.name,
            &db_name,
            &tname,
            if pol.restrictive {
                PolicyMode::Restrictive
            } else {
                PolicyMode::Permissive
            },
            Commands(pol.commands),
            &to,
            pol.using_expr.as_deref(),
            pol.check_expr.as_deref(),
        );
        return Ok(PolicyAnswer::Rows(LocalAnswer {
            columns: vec![
                crate::pipeline::text_column("Policy", charset),
                crate::pipeline::text_column("Create Policy", charset),
            ],
            rows: vec![vec![
                Some(bytes::Bytes::from(pol.name.clone())),
                Some(bytes::Bytes::from(sql)),
            ]],
        }));
    }
    manage(&db_key, db, object)?;
    let by = ts
        .principal
        .user_id
        .ok_or_else(|| invalid("an internal session can't manage policies"))?;
    let svc = Arc::clone(ts.services());
    // The module's tables, created on first use (DDL: outside the change).
    {
        let ep = svc.locator.writer(ts.tenant).await?;
        let store = RlsStore::new(ep.store.schema());
        let mut conn = ep.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        if !store.migrated(&mut conn).await? {
            store.migrate(&mut conn).await?;
        }
    }
    let mut change = svc
        .catalog
        .change_with(ts.tenant, VersionKind::Auth, ChangeOpts::default())
        .await?;
    let version = change.version();
    let r: Result<bool> = async {
        let (core, q) = change.tx().parts();
        let store = RlsStore::new(core.schema());
        match stmt {
            PolicyStmt::Create {
                name,
                mode,
                commands,
                to,
                using,
                check,
                mongo_using,
                mongo_check,
                ..
            } => {
                if name.len() > 64 {
                    return Err(invalid("a policy name is at most 64 characters"));
                }
                // A MongoDB form must be exactly what its SQL says.
                for (m, sql) in [(mongo_using, using), (mongo_check, check)] {
                    if let Some(m) = m {
                        let same = mongo_filter(m)
                            .and_then(|d| sprig_doc::policy::to_sql(&d).ok())
                            .is_some_and(|t| sql.as_deref() == Some(t.as_str()));
                        if !same {
                            return Err(invalid(
                                "the MongoDB form of a policy must be the filter its SQL was made from",
                            ));
                        }
                    }
                }
                let grantees = resolve_grantees(ts, cat, to)?;
                let mut new_reads = Vec::new();
                let using = match using {
                    Some(u) => {
                        let (t, r) = checked_expr(cat, u, lex, &db_name, lctn)?;
                        new_reads.extend(r);
                        Some(t)
                    }
                    None => None,
                };
                let check = match check {
                    Some(u) => {
                        let (t, r) = checked_expr(cat, u, lex, &db_name, lctn)?;
                        new_reads.extend(r);
                        Some(t)
                    }
                    None => None,
                };
                check_cycle(cat, &state, object, &new_reads, lex)?;
                let ok = store
                    .insert(
                        q,
                        &NewPolicy {
                            object,
                            db,
                            name,
                            restrictive: *mode == PolicyMode::Restrictive,
                            commands: commands.0,
                            grantees: &grantees,
                            using_expr: using.as_deref(),
                            check_expr: check.as_deref(),
                            using_mongo: mongo_using.as_deref(),
                            check_mongo: mongo_check.as_deref(),
                            by,
                        },
                        version,
                    )
                    .await?;
                if !ok {
                    return Err(policy_exists(name, &tname));
                }
                store.touch(q, object, db, version, None).await?;
                Ok(true)
            }
            PolicyStmt::Alter {
                name,
                to,
                using,
                check,
                ..
            } => {
                let grantees = to
                    .as_ref()
                    .map(|t| resolve_grantees(ts, cat, t))
                    .transpose()?;
                let mut new_reads = Vec::new();
                let using = match using {
                    Some(u) => {
                        let (t, r) = checked_expr(cat, u, lex, &db_name, lctn)?;
                        new_reads.extend(r);
                        Some(t)
                    }
                    None => None,
                };
                let check = match check {
                    Some(u) => {
                        let (t, r) = checked_expr(cat, u, lex, &db_name, lctn)?;
                        new_reads.extend(r);
                        Some(t)
                    }
                    None => None,
                };
                let existing = state
                    .policies_of(object)
                    .find(|x| x.name == *name)
                    .ok_or_else(|| no_policy(name, &tname))?;
                if using.is_some() && existing.commands == cmd::INSERT {
                    return Err(invalid("only WITH CHECK expression allowed for INSERT"));
                }
                if check.is_some()
                    && (existing.commands == cmd::SELECT || existing.commands == cmd::DELETE)
                {
                    return Err(invalid("WITH CHECK cannot be applied to SELECT or DELETE"));
                }
                check_cycle(cat, &state, object, &new_reads, lex)?;
                if !store
                    .alter(
                        q,
                        object,
                        name,
                        grantees.as_deref(),
                        using.as_deref(),
                        check.as_deref(),
                        version,
                    )
                    .await?
                {
                    return Err(no_policy(name, &tname));
                }
                store.touch(q, object, db, version, None).await?;
                Ok(true)
            }
            PolicyStmt::Rename { name, to, .. } => {
                match store.rename(q, object, name, to, version).await? {
                    Some(true) => {
                        store.touch(q, object, db, version, None).await?;
                        Ok(true)
                    }
                    Some(false) => Err(policy_exists(to, &tname)),
                    None => Err(no_policy(name, &tname)),
                }
            }
            PolicyStmt::Drop {
                name, if_exists, ..
            } => {
                if store.delete(q, object, name).await? {
                    store.touch(q, object, db, version, None).await?;
                    Ok(true)
                } else if *if_exists {
                    Ok(false)
                } else {
                    Err(no_policy(name, &tname))
                }
            }
            PolicyStmt::Security { switch, .. } => {
                let s = match switch {
                    SecuritySwitch::Enable => (Some(true), None),
                    SecuritySwitch::Disable => (Some(false), None),
                    SecuritySwitch::Force => (None, Some(true)),
                    SecuritySwitch::NoForce => (None, Some(false)),
                };
                store.touch(q, object, db, version, Some(s)).await?;
                Ok(true)
            }
            PolicyStmt::Show { .. } | PolicyStmt::ShowCreate { .. } => Ok(false),
        }
    }
    .await;
    match r {
        Ok(true) => {
            let v = change.commit().await?;
            await_auth(ts, v).await;
            Ok(PolicyAnswer::Changed { version: v, object })
        }
        Ok(false) => {
            let _ = change.rollback().await;
            let _ = svc.catalog.settle(ts.tenant, VersionKind::Auth).await;
            Ok(PolicyAnswer::Unchanged)
        }
        Err(e) => {
            let _ = change.rollback().await;
            let _ = svc.catalog.settle(ts.tenant, VersionKind::Auth).await;
            Err(e)
        }
    }
}

/// Waits (up to 5 s) until the session sees auth version `v`.
pub async fn await_auth(ts: &mut TenantSession, v: u64) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(c) = ts.services().cache.get(ts.tenant, Freshness::Latest).await
            && c.auth_version >= v
        {
            let _ = ts.refresh().await;
            return;
        }
        if std::time::Instant::now() > deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[allow(clippy::too_many_arguments)]
fn show(
    ts: &TenantSession,
    cat: &TenantCatalog,
    p: &dyn SessionGrants,
    state: &RlsState,
    db: Option<&str>,
    table: Option<&(DbId, String, ObjectId, String)>,
    lctn: bool,
    charset: u16,
) -> LocalAnswer {
    let cols = [
        "Database",
        "Table",
        "Policy",
        "Mode",
        "Commands",
        "Roles",
        "Using",
        "With_check",
        "Enabled",
        "Forced",
    ];
    let mut rows = Vec::new();
    let db_key = db.map(|d| database_key(d, lctn));
    let mut objects: Vec<ObjectId> = state.rows().security.iter().map(|s| s.object_id).collect();
    objects.extend(state.rows().policies.iter().map(|p| p.object_id));
    objects.sort_unstable();
    objects.dedup();
    let _ = ts;
    for o in objects {
        if table.is_some_and(|t| t.2 != o) {
            continue;
        }
        let Some(obj) = cat.objects.get(o) else {
            continue;
        };
        let Some(d) = cat.databases.get(obj.db_id) else {
            continue;
        };
        if table.is_none() && db_key.as_deref().is_some_and(|k| k != d.name_key) {
            continue;
        }
        let target = Target::Object {
            db_key: &d.name_key,
            db: d.db_id,
            object: o,
        };
        if !(p.unrestricted()
            || p.allows(Action::Select, target)
            || p.allows(Action::Alter, target))
        {
            continue;
        }
        let sec = state.security(o);
        let flag = |b: bool| Some(bytes::Bytes::from(if b { "YES" } else { "NO" }));
        let pols: Vec<_> = state.policies_of(o).collect();
        if pols.is_empty() {
            rows.push(vec![
                Some(bytes::Bytes::from(d.name.clone())),
                Some(bytes::Bytes::from(obj.name.clone())),
                None,
                None,
                None,
                None,
                None,
                None,
                flag(sec.is_some_and(|s| s.enabled)),
                flag(sec.is_some_and(|s| s.forced)),
            ]);
        }
        for pol in pols {
            let to: Vec<String> = pol.grantees.iter().map(|g| grantee_text(cat, g)).collect();
            rows.push(vec![
                Some(bytes::Bytes::from(d.name.clone())),
                Some(bytes::Bytes::from(obj.name.clone())),
                Some(bytes::Bytes::from(pol.name.clone())),
                Some(bytes::Bytes::from(if pol.restrictive {
                    "RESTRICTIVE"
                } else {
                    "PERMISSIVE"
                })),
                Some(bytes::Bytes::from(Commands(pol.commands).sql())),
                Some(bytes::Bytes::from(if to.is_empty() {
                    "PUBLIC".to_owned()
                } else {
                    to.join(", ")
                })),
                pol.using_expr.clone().map(bytes::Bytes::from),
                pol.check_expr.clone().map(bytes::Bytes::from),
                flag(sec.is_some_and(|s| s.enabled)),
                flag(sec.is_some_and(|s| s.forced)),
            ]);
        }
    }
    LocalAnswer {
        columns: cols
            .iter()
            .map(|c| crate::pipeline::text_column(c, charset))
            .collect(),
        rows,
    }
}

/// Studio's "view as" with simulated session values: claims and
/// attributes that replace the user's own for the statements run in
/// [`SIMULATED`]'s scope, and the policy predicates the rewrite rendered,
/// by table, for the response.
#[derive(Debug, Default)]
pub struct Simulated {
    /// `SPRIG_CLAIM` values (an object), instead of the token's.
    pub claims: Option<Arc<serde_json::Value>>,
    /// `SPRIG_USER_ATTR` values (an object), over the user's own.
    pub attrs: Option<serde_json::Value>,
    /// (table, policy, mode, predicate with the session's values bound).
    pub seen: std::sync::Mutex<Vec<(String, String, &'static str, String)>>,
}

tokio::task_local! {
    /// Set by Studio's tester around the statement it runs as a user.
    pub static SIMULATED: Arc<Simulated>;
}

fn simulated() -> Option<Arc<Simulated>> {
    SIMULATED.try_with(Arc::clone).ok()
}

/// The values a statement's predicates bind (the session functions).
pub struct SessionValues {
    current_user: String,
    user_id: u64,
    tenant: u64,
    home: u64,
    space: Option<u64>,
    claims: Option<Arc<serde_json::Value>>,
    attrs: Option<serde_json::Value>,
    /// `SPRIG_TENANT_IN_SUBTREE`: the home tenant and its descendants,
    /// when a policy asks.
    pub subtree: Option<Arc<Vec<u64>>>,
    /// `SPRIG_TENANT_MANAGED`: the tenants the user administers.
    pub managed: Option<Arc<Vec<u64>>>,
    /// The subtree is above `rls.inline_subtree_max`: it is read from
    /// `__sprig_sys.__sprig_subtree_v`.
    pub subtree_table: bool,
    /// Likewise the managed tenants (an administrator's subtree).
    pub managed_table: bool,
}

impl SessionValues {
    /// The session's values.
    #[must_use]
    pub fn of(ts: &TenantSession, cat: &TenantCatalog) -> Self {
        let sim = simulated();
        let attrs = ts
            .principal
            .user_id
            .and_then(|u| TenantAuth::of(cat).and_then(|a| a.user(u)))
            .and_then(|u| u.row.attributes.clone());
        Self {
            current_user: format!("{}@{}", ts.principal.user, ts.principal.host),
            user_id: ts.principal.user_id.map_or(0, |u| u64::from(u.0)),
            tenant: ts.tenant.0,
            home: ts.tenant.0,
            space: ts.inherit.space.map(|s| s.0),
            claims: sim
                .as_ref()
                .and_then(|x| x.claims.clone())
                .or_else(|| ts.rls.claims.clone()),
            attrs: match (attrs, sim.as_ref().and_then(|x| x.attrs.clone())) {
                (Some(serde_json::Value::Object(mut a)), Some(serde_json::Value::Object(o))) => {
                    a.extend(o);
                    Some(serde_json::Value::Object(a))
                }
                (a, None) => a,
                (_, o) => o,
            },
            subtree: None,
            managed: None,
            subtree_table: false,
            managed_table: false,
        }
    }

    /// A session value as a MongoDB policy filter names it (`USER`,
    /// `USER_ID`, `CLAIM.<k>`, ...; `sprig_doc::policy`), `None` when it
    /// has none.
    #[must_use]
    pub fn mongo_value(&self, key: &str) -> Option<bson::Bson> {
        let json = |v: &serde_json::Value| -> Option<bson::Bson> {
            match v {
                serde_json::Value::Null => None,
                v => bson::Bson::try_from(v.clone()).ok(),
            }
        };
        let id = |n: u64| i64::try_from(n).ok().map(bson::Bson::Int64);
        match key {
            "USER" => Some(bson::Bson::String(self.current_user.clone())),
            "USER_ID" => id(self.user_id),
            "TENANT_ID" => id(self.tenant),
            "HOME_TENANT_ID" => id(self.home),
            "SESSION_SPACE" => self.space.and_then(id),
            k => {
                if let Some(c) = k.strip_prefix("CLAIM.") {
                    json(self.claims.as_deref()?.get(c)?)
                } else if let Some(a) = k.strip_prefix("ATTR.") {
                    json(self.attrs.as_ref()?.get(a)?)
                } else {
                    None
                }
            }
        }
    }

    /// The `@sprig_rls` context for MariaDB's triggers.
    #[must_use]
    pub fn context(&self, rls: &SessionRls) -> String {
        let v = serde_json::json!({
            "b": u8::from(rls.bypass),
            "x": rls.exempt.iter().map(|o| o.0).collect::<Vec<_>>(),
            "p": rls.policy_ids,
            "s": {
                "user": self.current_user,
                "uid": self.user_id,
                "tid": self.tenant,
                "htid": self.home,
                "space": self.space,
                "claims": self.claims.as_deref().cloned().unwrap_or(serde_json::Value::Null),
                "attrs": self.attrs.clone().unwrap_or(serde_json::Value::Null),
                "sub": self.subtree.as_deref().cloned().unwrap_or_default(),
                "man": self.managed.as_deref().cloned().unwrap_or_default(),
                "subt": u8::from(self.subtree_table),
                "mant": u8::from(self.managed_table),
            }
        });
        v.to_string()
    }
}

/// A JSON value as a SQL literal.
fn literal(v: &serde_json::Value, nbe: bool) -> String {
    match v {
        serde_json::Value::Null => "NULL".to_owned(),
        serde_json::Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_owned(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => sprig_sql::rls::sql_string(s, nbe),
        other => sprig_sql::rls::sql_string(&other.to_string(), nbe),
    }
}

/// The rewrite's view of the session.
pub struct Resolve<'a> {
    cat: &'a TenantCatalog,
    rls: &'a SessionRls,
    current: Option<&'a str>,
    lctn: bool,
    temp: &'a dyn Fn(Option<&str>, &str) -> bool,
    values: &'a SessionValues,
    lex: LexOptions,
    names: HashSet<String>,
    tainted: &'a HashSet<String>,
    /// key → protected table.
    pub keys: RefCell<Vec<ObjectId>>,
    /// Keys that are a key-value namespace's element or sorted-set table
    /// (`__sprig_ke_<ns>`, `__sprig_kz_<ns>`), filtered by the namespace's
    /// key policies, with the key table's database as written and its
    /// name.
    elements: RefCell<HashMap<usize, (String, String)>>,
    cache: RefCell<HashMap<(usize, Vec<Cmd>, String), String>>,
}

/// The prefixes of a key-value namespace's element and sorted-set tables.
const KV_ELEMENT_PREFIXES: [&str; 2] = sprig_sql::rls::triggers::KV_ELEMENT_PREFIXES;

/// `rendered` (a policy predicate rendered with qualifier `` `__sprig_kq` ``)
/// with `qual` in its place, when it reads no column but the key `k`: an
/// element row then shows exactly when its key's row would. `None` when it
/// reads another column.
fn key_only(rendered: &str, qual: &str) -> Option<String> {
    const Q: &str = "`__sprig_kq`.";
    let mut rest = rendered;
    while let Some(at) = rest.find(Q) {
        let after = &rest[at + Q.len()..];
        let col = if let Some(r) = after.strip_prefix('`') {
            r.split('`').next().unwrap_or_default().to_owned()
        } else {
            after
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '$')
                .collect()
        };
        if !col.eq_ignore_ascii_case("k") {
            return None;
        }
        rest = after;
    }
    Some(rendered.replace("`__sprig_kq`", qual))
}

impl<'a> Resolve<'a> {
    /// A resolver for one statement.
    #[must_use]
    pub fn new(
        cat: &'a TenantCatalog,
        rls: &'a SessionRls,
        current: Option<&'a str>,
        lctn: bool,
        temp: &'a dyn Fn(Option<&str>, &str) -> bool,
        values: &'a SessionValues,
        lex: LexOptions,
        tainted: &'a HashSet<String>,
    ) -> Self {
        let names = rls
            .tables
            .keys()
            .filter_map(|o| cat.objects.get(*o))
            .map(|o| o.name.to_lowercase())
            .flat_map(|n| {
                let e: Vec<String> = KV_ELEMENT_PREFIXES
                    .iter()
                    .map(|p| format!("{p}{n}"))
                    .collect();
                std::iter::once(n).chain(e)
            })
            .collect();
        Self {
            cat,
            rls,
            current,
            lctn,
            temp,
            values,
            lex,
            names,
            tainted,
            keys: RefCell::new(Vec::new()),
            elements: RefCell::new(HashMap::new()),
            cache: RefCell::new(HashMap::new()),
        }
    }

    fn table(&self, key: usize) -> Option<Arc<TableRls>> {
        let o = *self.keys.borrow().get(key)?;
        self.rls.tables.get(&o).cloned()
    }

    fn render_one(&self, t: &TableRls, bit: u8, qual: &str, table_name: &str) -> Result<String> {
        let c = t.combination(bit, Clause::Using);
        let nbe = self.lex.no_backslash_escapes;
        let claim = |k: &str| {
            self.values
                .claims
                .as_deref()
                .and_then(|c| c.get(k))
                .map(|v| literal(v, nbe))
        };
        let attr = |k: &str| {
            self.values
                .attrs
                .as_ref()
                .and_then(|c| c.get(k))
                .map(|v| literal(v, nbe))
        };
        let b = LiteralBinder {
            current_user: &self.values.current_user,
            user_id: self.values.user_id,
            tenant_id: self.values.tenant,
            home_tenant_id: self.values.home,
            space: self.values.space,
            claim: &claim,
            attr: &attr,
            subtree: self.values.subtree.as_deref().map(Vec::as_slice),
            managed: self.values.managed.as_deref().map(Vec::as_slice),
            subtree_table: self.values.subtree_table,
            managed_table: self.values.managed_table,
            no_backslash_escapes: nbe,
        };
        let sim = simulated();
        let note = |term: &sprig_auth::authz::rls::Term<'_>, r: &str| {
            if let Some(sim) = &sim {
                sim.seen
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push((
                        table_name.to_owned(),
                        term.policy.name.clone(),
                        if term.policy.restrictive {
                            "restrictive"
                        } else {
                            "permissive"
                        },
                        r.to_owned(),
                    ));
            }
        };
        let mut perm = Vec::new();
        for term in &c.permissive {
            let r = expr::render(term.expr, &self.lex, table_name, qual, &b)?;
            note(term, &r);
            perm.push(format!("({r})"));
        }
        let mut s = if perm.is_empty() {
            "FALSE".to_owned()
        } else if perm.len() == 1 {
            perm.remove(0)
        } else {
            format!("({})", perm.join(" OR "))
        };
        for term in &c.restrictive {
            let r = expr::render(term.expr, &self.lex, table_name, qual, &b)?;
            note(term, &r);
            s.push_str(&format!(" AND ({r})"));
        }
        Ok(s)
    }
}

impl Resolver for Resolve<'_> {
    fn mentions(&self, word: &str) -> bool {
        self.names.contains(word) || self.tainted.contains(word)
    }

    fn refused_relation(&self, _db: Option<&str>, name: &str) -> bool {
        self.tainted.contains(&name.to_lowercase())
    }

    fn refused_function(&self, name: &str) -> bool {
        self.tainted.contains(&name.to_lowercase())
    }

    fn protected(&self, db: Option<&str>, table: &str) -> Option<usize> {
        let d = db.or(self.current)?;
        if (self.temp)(Some(d), table) {
            return None;
        }
        let key = database_key(d, self.lctn);
        let row = self
            .cat
            .database(&key)
            .filter(|r| r.state == DbState::Active)?;
        // A key-value namespace's element tables follow its key
        // table's policies.
        let lower = table.to_ascii_lowercase();
        let element = KV_ELEMENT_PREFIXES
            .iter()
            .find_map(|p| lower.strip_prefix(p).map(|_| &table[p.len()..]));
        let okey = object_key(ObjectKind::Table, element.unwrap_or(table), self.lctn);
        let obj = self
            .cat
            .object(row.db_id, Namespace::Relation, &okey)
            .filter(|o| o.state == ObjectState::Active)?;
        if !self.rls.tables.contains_key(&obj.object_id) {
            return None;
        }
        if element.is_some() {
            let mut keys = self.keys.borrow_mut();
            keys.push(obj.object_id);
            let i = keys.len() - 1;
            self.elements
                .borrow_mut()
                .insert(i, (d.to_owned(), obj.name.clone()));
            return Some(i);
        }
        let mut keys = self.keys.borrow_mut();
        if let Some(i) = keys.iter().position(|o| *o == obj.object_id) {
            return Some(i);
        }
        keys.push(obj.object_id);
        Some(keys.len() - 1)
    }

    fn predicate(&self, key: usize, cmds: &[Cmd], qual: &str) -> Result<String> {
        let ck = (key, cmds.to_vec(), qual.to_owned());
        if let Some(s) = self.cache.borrow().get(&ck) {
            return Ok(s.clone());
        }
        let t = self
            .table(key)
            .ok_or_else(|| SprigError::internal("an unknown protected table"))?;
        let name = self
            .cat
            .objects
            .get(t.object)
            .map(|o| o.name.clone())
            .unwrap_or_default();
        let mut parts = Vec::new();
        for c in cmds {
            let bit = match c {
                Cmd::Select => cmd::SELECT,
                Cmd::Update => cmd::UPDATE,
                Cmd::Delete => cmd::DELETE,
            };
            if let Some((db, key_table)) = self.elements.borrow().get(&key) {
                // A policy reading only the key filters the element row
                // itself; any other reads the element's key row (its
                // primary key) and applies the key table's policies there.
                let r = self.render_one(&t, bit, "`__sprig_kq`", &name)?;
                parts.push(key_only(&r, qual).unwrap_or_else(|| {
                    format!(
                        "EXISTS (SELECT 1 FROM {}.{} AS `__sprig_kq` WHERE `__sprig_kq`.`k` = \
                         {qual}.`k` AND ({r}))",
                        sprig_sql::rls::quote(db),
                        sprig_sql::rls::quote(key_table),
                    )
                }));
                continue;
            }
            parts.push(self.render_one(&t, bit, qual, &name)?);
        }
        let s = if parts.len() == 1 {
            parts.remove(0)
        } else {
            parts
                .iter()
                .map(|p| format!("({p})"))
                .collect::<Vec<_>>()
                .join(" AND ")
        };
        self.cache.borrow_mut().insert(ck, s.clone());
        Ok(s)
    }
}

/// Tables the policies of `objects` read through subqueries (they run
/// unfiltered, with MariaDB checking the session's own grants).
#[must_use]
pub fn policy_reads(cat: &TenantCatalog, objects: &[ObjectId], lex: &LexOptions) -> Vec<ObjectId> {
    let Some(state) = RlsState::of(cat) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for o in objects {
        out.extend(reads(cat, state, *o, lex));
    }
    out
}

/// What this process last wrote to `__sprig_sys.__sprig_subtree`, by
/// (tenant, cluster): a hash of the subtree.
type SubtreeApplied = std::sync::Mutex<HashMap<(u64, String), u64>>;

fn subtree_applied() -> &'static SubtreeApplied {
    static C: std::sync::OnceLock<SubtreeApplied> = std::sync::OnceLock::new();
    C.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Brings tenant `tenant`'s rows of `__sprig_sys.__sprig_subtree` on
/// `cluster` to `tree`, when they differ from what this process last wrote:
/// creates the table and its `PUBLIC` view if needed, then deletes the rows
/// no longer in the tree and inserts the new ones, in one transaction.
/// Idempotent, so gateways may sync the same tenant at once.
///
/// # Errors
/// The cluster's.
pub async fn subtree_sync(
    svc: &super::TenantServices,
    tenant: u64,
    tree: &[u64],
    cluster: &sprig_common::ids::ClusterId,
) -> Result<()> {
    use mysql_async::prelude::Queryable;
    use std::hash::{Hash, Hasher};
    let h = {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        tree.hash(&mut h);
        h.finish()
    };
    let key = (tenant, cluster.to_string());
    if subtree_applied()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&key)
        == Some(&h)
    {
        return Ok(());
    }
    let pool = svc.locator.ctl_pool(cluster).await?;
    let mut c = pool.get_conn().await.map_err(|e| {
        SprigError::new(
            SprigCode::BackendUnavailable,
            format!("sprig_ctl on {cluster}: {e}"),
        )
    })?;
    let fail = |e: mysql_async::Error| {
        SprigError::new(
            SprigCode::BackendUnavailable,
            format!("tenant subtree on {cluster}: {e}"),
        )
    };
    for q in sprig_sql::rls::subtree::setup_sql() {
        c.query_drop(q).await.map_err(fail)?;
    }
    let t = format!(
        "`{}`.`{}`",
        sprig_sql::rls::subtree::SYS_SCHEMA,
        sprig_sql::rls::subtree::TABLE
    );
    let have: Vec<u64> = c
        .exec(
            format!("SELECT `descendant` FROM {t} WHERE `ancestor` = ?"),
            (tenant,),
        )
        .await
        .map_err(fail)?;
    let have: std::collections::BTreeSet<u64> = have.into_iter().collect();
    let want: std::collections::BTreeSet<u64> = tree.iter().copied().collect();
    let gone: Vec<u64> = have.difference(&want).copied().collect();
    let new: Vec<u64> = want.difference(&have).copied().collect();
    let mut tx = c
        .start_transaction(mysql_async::TxOpts::default())
        .await
        .map_err(fail)?;
    for chunk in gone.chunks(1000) {
        let ids: Vec<String> = chunk.iter().map(u64::to_string).collect();
        tx.query_drop(format!(
            "DELETE FROM {t} WHERE `ancestor` = {tenant} AND `descendant` IN ({})",
            ids.join(", ")
        ))
        .await
        .map_err(fail)?;
    }
    for chunk in new.chunks(1000) {
        let rows: Vec<String> = chunk.iter().map(|d| format!("({tenant}, {d})")).collect();
        tx.query_drop(format!(
            "INSERT IGNORE INTO {t} (`ancestor`, `descendant`) VALUES {}",
            rows.join(", ")
        ))
        .await
        .map_err(fail)?;
    }
    tx.commit().await.map_err(fail)?;
    tracing::info!(tenant, %cluster, tenants = tree.len(), added = new.len(), removed = gone.len(), "tenant subtree written for row-level security");
    subtree_applied()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(key, h);
    Ok(())
}

/// Subtrees by root tenant, with when they were read.
type SubtreeCache = std::sync::Mutex<HashMap<u64, (std::time::Instant, Arc<Vec<u64>>)>>;

fn subtree_cache() -> &'static SubtreeCache {
    static C: std::sync::OnceLock<SubtreeCache> = std::sync::OnceLock::new();
    C.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Tenant `root` and every tenant below it, from the directory's
/// `tenants/<id>` records (their `path` lists the ancestors), cached for
/// five seconds.
///
/// # Errors
/// The directory's.
pub async fn subtree(
    svc: &super::TenantServices,
    root: sprig_common::ids::TenantId,
) -> Result<Arc<Vec<u64>>> {
    use sprig_meta::{ListOpts, Record, Versioned};
    #[derive(serde::Deserialize, serde::Serialize)]
    struct Entry {
        #[serde(default)]
        path: String,
    }
    impl Versioned for Entry {
        const V: u32 = 1;
    }
    if let Some((at, v)) = subtree_cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&root.0)
        && at.elapsed() < Duration::from_secs(5)
    {
        return Ok(Arc::clone(v));
    }
    let prefix = svc.global.tier().key("tenants/")?;
    let mark = format!("/{}/", root.0);
    let mut out = vec![root.0];
    let mut opts = ListOpts::default();
    loop {
        let page = svc.global.list(&prefix, opts.clone()).await?;
        for kv in &page.kvs {
            let key = kv.key.to_string();
            let Some(id) = key.rsplit('/').next().and_then(|x| x.parse::<u64>().ok()) else {
                continue;
            };
            if id == root.0 {
                continue;
            }
            if let Ok(r) = Record::<Entry>::decode(&kv.value)
                && r.body.path.contains(&mark)
            {
                out.push(id);
            }
        }
        if !page.more {
            break;
        }
        opts.at = Some(page.revision);
        opts.after = page.kvs.last().map(|kv| kv.key.clone());
    }
    out.sort_unstable();
    out.dedup();
    let v = Arc::new(out);
    subtree_cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(root.0, (std::time::Instant::now(), Arc::clone(&v)));
    Ok(v)
}

/// Fills `values`' subtree and managed sets when a policy applying to the
/// session uses `SPRIG_TENANT_IN_SUBTREE` or `SPRIG_TENANT_MANAGED`; 39366
/// above `rls.inline_subtree_max` tenants.
///
/// # Errors
/// The directory's; 39366.
pub async fn bind_tree(
    ts: &TenantSession,
    cat: &TenantCatalog,
    rls: &SessionRls,
    values: &mut SessionValues,
) -> Result<()> {
    let uses = rls.tables.values().any(|t| {
        t.policies.iter().any(|p| {
            [p.using_expr.as_deref(), p.check_expr.as_deref()]
                .into_iter()
                .flatten()
                .any(|e| e.to_ascii_uppercase().contains("SPRIG_TENANT_"))
        })
    });
    if !uses {
        return Ok(());
    }
    let svc = ts.services();
    let tree = subtree(svc, ts.tenant).await?;
    let max = svc.settings.get(
        &sprig_auth::authz::settings::RLS_INLINE_SUBTREE_MAX,
        &cat.setting_ctx(&svc.dc),
        Some(&cat.settings),
    );
    let p = super::authz::privileges(ts, cat);
    let admin = p.unrestricted() || p.allows(Action::TenantAdmin, Target::Tenant);
    if tree.len() as u64 > max {
        // A large subtree goes to `__sprig_sys.__sprig_subtree` on the
        // tenant's clusters, and the statement reads it there.
        let mut clusters = crate::exec::router::placement_digest(svc, cat)
            .map(|p| p.1)
            .unwrap_or_default();
        if !clusters.contains(&ts.principal.backend.cluster) {
            clusters.push(ts.principal.backend.cluster.clone());
        }
        if clusters.is_empty() {
            return Err(sprig_sql::rls::unsupported(&format!(
                "a subtree of {} tenants (rls.inline_subtree_max is {max}) before the tenant's \
                 clusters are known",
                tree.len()
            )));
        }
        for c in &clusters {
            subtree_sync(svc, ts.tenant.0, &tree, c).await?;
        }
        values.subtree = None;
        values.subtree_table = true;
        values.managed_table = admin;
        values.managed = (!admin).then(|| Arc::new(Vec::new()));
        return Ok(());
    }
    values.managed = Some(if admin {
        Arc::clone(&tree)
    } else {
        Arc::new(Vec::new())
    });
    values.subtree = Some(tree);
    Ok(())
}

// Change sinks over a table are not paused when its policies come to bind
// their owners: their workers filter each batch for the owner
// (sprig_sinks::rls_filter).

// ---------------------------------------------------------------------
// Policy views.

/// The policies on a collection's table that bind a session, as MongoDB
/// filters with the session's values bound: what a change stream shows of
/// each event's documents.
#[derive(Debug, Clone)]
pub enum DocPolicies {
    /// No policy binds the session there.
    Unbound,
    /// Bound, and an applying policy has no MongoDB form (it was written in
    /// SQL) or a session value it needs is missing: no document shows.
    Hidden,
    /// A document shows when one of `permissive` and all of `restrictive`
    /// match it (none permissive: none shows).
    Filters {
        /// OR-ed.
        permissive: Vec<sprig_doc::filter::Pred>,
        /// AND-ed.
        restrictive: Vec<sprig_doc::filter::Pred>,
    },
}

impl DocPolicies {
    /// Whether document `d` shows.
    #[must_use]
    pub fn visible(&self, d: &bson::Document) -> bool {
        match self {
            Self::Unbound => true,
            Self::Hidden => false,
            Self::Filters {
                permissive,
                restrictive,
            } => {
                permissive.iter().any(|p| p.matches(d)) && restrictive.iter().all(|p| p.matches(d))
            }
        }
    }
}

/// [`DocPolicies`] of table `object` for session `ts`.
#[must_use]
pub fn doc_policies(
    ts: &TenantSession,
    cat: &TenantCatalog,
    rls: &SessionRls,
    object: ObjectId,
) -> DocPolicies {
    let Some(t) = rls.tables.get(&object) else {
        return DocPolicies::Unbound;
    };
    let values = SessionValues::of(ts, cat);
    let lookup = |k: &str| values.mongo_value(k);
    let comb = t.combination(cmd::SELECT, Clause::Using);
    let compile =
        |terms: &[sprig_auth::authz::rls::Term<'_>]| -> Option<Vec<sprig_doc::filter::Pred>> {
            terms
                .iter()
                .map(|term| {
                    let f = mongo_filter(term.policy.using_mongo.as_deref()?)?;
                    let bound = sprig_doc::policy::bind(&f, &lookup)?;
                    sprig_doc::filter::parse(&bound).ok()
                })
                .collect()
        };
    match (compile(&comb.permissive), compile(&comb.restrictive)) {
        (Some(permissive), Some(restrictive)) => DocPolicies::Filters {
            permissive,
            restrictive,
        },
        _ => DocPolicies::Hidden,
    }
}

/// Every table whose policies bind a session, by lower-case (database,
/// table), as [`DocPolicies`]; tables not here don't bind it.
#[derive(Debug, Clone, Default)]
pub struct DocViews {
    /// (database, table) → the policies.
    pub tables: HashMap<(String, String), DocPolicies>,
    /// (database, table) the session may read but not `WATCH`:
    /// [`DocPolicies::Hidden`] in `tables` too; a stream over
    /// one of them alone is refused.
    pub unwatched: std::collections::HashSet<(String, String)>,
}

impl DocViews {
    /// A digest of what the views show (tables, policies, bound values):
    /// a change stream compares it at each `getMore` with the one it
    /// opened with.
    #[must_use]
    pub fn fingerprint(views: Option<&Self>) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        if let Some(v) = views {
            let mut keys: Vec<_> = v.tables.iter().collect();
            keys.sort_by(|a, b| a.0.cmp(b.0));
            for (k, p) in keys {
                k.hash(&mut h);
                format!("{p:?}").hash(&mut h);
            }
        } else {
            u64::MAX.hash(&mut h);
        }
        h.finish()
    }

    /// Whether document `d` of `db`.`table` shows.
    #[must_use]
    pub fn visible(&self, db: &str, table: &str, d: &bson::Document) -> bool {
        self.tables
            .get(&(db.to_lowercase(), table.to_lowercase()))
            .is_none_or(|p| p.visible(d))
    }
}

/// [`DocViews`] for a session with compiled policies `rls` (none: empty).
/// Tables the session may not `SELECT`, or may not `WATCH` (an application
/// stream needs `WATCH` on each object), are [`DocPolicies::Hidden`] too: a
/// database or deployment change stream leaves their events out, as MongoDB
/// asks `find` and `changeStream` on every collection of what it watches.
#[must_use]
pub fn doc_views(ts: &TenantSession, cat: &TenantCatalog, rls: Option<&SessionRls>) -> DocViews {
    let mut out = DocViews::default();
    let grants = super::authz::privileges(ts, cat);
    if !grants.unrestricted() {
        let lctn = ts.lower_case_table_names();
        for obj in cat.objects.iter() {
            if obj.namespace != Namespace::Relation || obj.state != ObjectState::Active {
                continue;
            }
            let Some(db) = cat.databases.get(obj.db_id) else {
                continue;
            };
            let db_key = database_key(&db.name, lctn);
            let on = Target::Object {
                db_key: &db_key,
                db: obj.db_id,
                object: obj.object_id,
            };
            let key = (db.name.to_lowercase(), obj.name.to_lowercase());
            if !grants.allows(Action::Select, on) {
                out.tables.insert(key, DocPolicies::Hidden);
            } else if !grants.allows(Action::Watch, on) {
                out.tables.insert(key.clone(), DocPolicies::Hidden);
                out.unwatched.insert(key);
            }
        }
    }
    let Some(rls) = rls else {
        return out;
    };
    for o in rls.tables.keys() {
        let Some(obj) = cat.objects.get(*o) else {
            continue;
        };
        let Some(db) = cat.databases.get(obj.db_id) else {
            continue;
        };
        // A table the session may not read stays hidden.
        out.tables
            .entry((db.name.to_lowercase(), obj.name.to_lowercase()))
            .or_insert_with(|| doc_policies(ts, cat, rls, *o));
    }
    out
}

/// A policy's MongoDB form (relaxed extended JSON) as a filter document.
#[must_use]
pub fn mongo_filter(text: &str) -> Option<bson::Document> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;
    match bson::Bson::try_from(v).ok()? {
        bson::Bson::Document(d) => Some(d),
        _ => None,
    }
}

/// Protected tables' invisible columns (lower case), by (tenant, object),
/// as the last check of their shards found them: their policy views come in
/// two, and a statement naming one of these reads the all-columns view.
type Hidden = std::sync::Mutex<HashMap<(u64, u64), Arc<Vec<String>>>>;

fn hidden() -> &'static Hidden {
    static V: std::sync::OnceLock<Hidden> = std::sync::OnceLock::new();
    V.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Records table `object`'s invisible columns (none: forgets it).
pub fn hidden_set(tenant: u64, object: u64, columns: Vec<String>) {
    let mut v = hidden()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if columns.is_empty() {
        v.remove(&(tenant, object));
    } else {
        v.insert((tenant, object), Arc::new(columns));
    }
}

/// Table `object`'s invisible columns, if it has any (see
/// [`hidden_set`]).
pub fn hidden_of(tenant: u64, object: u64) -> Option<Arc<Vec<String>>> {
    hidden()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&(tenant, object))
        .cloned()
}

/// Key-value element tables with policy views, by (tenant, key table's
/// object): each element table's name and its invisible columns (lower
/// case), as the last check of the key table's shards found them.
type Elements = std::sync::Mutex<HashMap<(u64, u64), ElementViews>>;

/// [`elements_of`]: (element table, its invisible columns).
pub type ElementViews = Arc<Vec<(String, Vec<String>)>>;

fn elements() -> &'static Elements {
    static V: std::sync::OnceLock<Elements> = std::sync::OnceLock::new();
    V.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Records key table `object`'s element tables that have policy views
/// (none: an empty list, so [`elements_of`] tells a table checked from
/// one not checked yet).
pub fn elements_set(tenant: u64, object: u64, tables: Vec<(String, Vec<String>)>) {
    elements()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert((tenant, object), Arc::new(tables));
}

/// Key table `object`'s element tables with policy views (see
/// [`elements_set`]); `None` until a check of its shards on this gateway.
pub fn elements_of(tenant: u64, object: u64) -> Option<ElementViews> {
    elements()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&(tenant, object))
        .cloned()
}

/// Accounts (`t42_u7`) defining wrapped definer routines, by tenant: the
/// catalog version they were last looked up at, and the accounts (those
/// repoint wrapped since are added at once).
type Definers = std::sync::Mutex<HashMap<u64, (u64, std::collections::BTreeSet<String>)>>;

fn definers() -> &'static Definers {
    static V: std::sync::OnceLock<Definers> = std::sync::OnceLock::new();
    V.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Notes that `account` defines a wrapped definer routine of `tenant`.
pub fn definer_note(tenant: u64, account: &str) {
    definers()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .entry(tenant)
        .or_insert((0, std::collections::BTreeSet::new()))
        .1
        .insert(account.to_owned());
}

/// The accounts defining `tenant`'s wrapped definer routines, looked up
/// on `cluster` once per catalog version `version`.
///
/// # Errors
/// The cluster's.
pub async fn definers_of(
    svc: &super::TenantServices,
    tenant: u64,
    version: u64,
    cluster: &sprig_common::ids::ClusterId,
) -> Result<std::collections::BTreeSet<String>> {
    use mysql_async::prelude::Queryable;
    if let Some((v, set)) = definers()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&tenant)
        && *v == version
    {
        return Ok(set.clone());
    }
    let pool = svc.locator.ctl_pool(cluster).await?;
    let mut conn = pool.get_conn().await.map_err(|e| {
        SprigError::new(
            SprigCode::BackendUnavailable,
            format!("sprig_ctl on {cluster}: {e}"),
        )
    })?;
    let like = format!("t{tenant}\\_d%");
    let found: Vec<String> = conn
        .exec(
            "SELECT DISTINCT SUBSTRING_INDEX(DEFINER, '@', 1) FROM information_schema.ROUTINES \
             WHERE ROUTINE_SCHEMA LIKE ? AND SECURITY_TYPE = 'DEFINER' \
             AND ROUTINE_DEFINITION LIKE '%sprig:rls-definer(%'",
            (like.as_str(),),
        )
        .await
        .map_err(|e| SprigError::new(SprigCode::BackendUnavailable, format!("{cluster}: {e}")))?;
    drop(conn);
    let mut g = definers()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let e = g
        .entry(tenant)
        .or_insert((0, std::collections::BTreeSet::new()));
    e.0 = version;
    e.1.extend(found);
    Ok(e.1.clone())
}

/// The `@sprig_rls` contexts of `accounts`, as the session's `"d"`: each
/// definer's policies at the catalog's auth version, compiled with its
/// default roles (a `SECURITY DEFINER` function runs with its definer's
/// default roles), with its name, ids, attributes and space; claims are
/// absent and the subtree sets empty (as for worker-run code). Accounts of
/// other tenants or of gone users are left out (their routines keep the
/// caller's context).
#[must_use]
pub fn definer_contexts(
    cat: &TenantCatalog,
    accounts: &std::collections::BTreeSet<String>,
) -> Option<serde_json::Value> {
    use sprig_auth::authz::CompiledPrivileges;
    let (Some(state), Some(auth)) = (AuthzState::of(cat), TenantAuth::of(cat)) else {
        return None;
    };
    let prefix = format!("t{}_u", cat.tenant);
    let mut d = serde_json::Map::new();
    for a in accounts {
        let Some(uid) = a.strip_prefix(&prefix).and_then(|x| x.parse::<u32>().ok()) else {
            continue;
        };
        let u = sprig_common::ids::UserId(uid);
        let Some(row) = auth.user(u).map(|x| x.row.clone()) else {
            continue;
        };
        let c = CompiledPrivileges::compile(state, u, &state.default_roles(u));
        let r = sprig_auth::authz::rls::compile(cat, &c, u);
        d.insert(
            a.clone(),
            serde_json::json!({
                "b": u8::from(r.bypass || c.everything),
                "x": r.exempt.iter().map(|o| o.0).collect::<Vec<_>>(),
                "p": r.policy_ids,
                "s": {
                    "user": format!("{}@{}", row.name, row.host),
                    "uid": uid,
                    "tid": cat.tenant.0,
                    "htid": cat.tenant.0,
                    "space": (row.home_space_id != 0).then_some(row.home_space_id),
                    "claims": serde_json::Value::Null,
                    "attrs": row.attributes.clone().unwrap_or(serde_json::Value::Null),
                    "sub": Vec::<u64>::new(),
                    "man": Vec::<u64>::new(),
                }
            }),
        );
    }
    (!d.is_empty()).then_some(serde_json::Value::Object(d))
}

/// `rls.backend_enforcement` at database `db`: `views`, `triggers` or
/// `off`.
#[must_use]
pub fn enforcement(svc: &super::TenantServices, cat: &TenantCatalog, db: DbId) -> String {
    svc.settings
        .get(
            &sprig_auth::authz::settings::RLS_BACKEND_ENFORCEMENT,
            &cat.setting_ctx(&svc.dc).with_db(db),
            Some(&cat.settings),
        )
        .into_owned()
}

/// A hash of `rls.backend_enforcement` at every database with a protected
/// table (0 without row-level security): the backend reconcile's key, so
/// a change narrows or widens grants at once.
#[must_use]
pub fn enforcement_digest(svc: &super::TenantServices, cat: &TenantCatalog) -> u64 {
    use std::hash::{Hash, Hasher};
    let Some(state) = RlsState::of(cat).filter(|s| s.any_enabled()) else {
        return 0;
    };
    let mut dbs: Vec<DbId> = state
        .rows()
        .security
        .iter()
        .filter(|s| s.enabled)
        .map(|s| s.db_id)
        .collect();
    dbs.sort_unstable();
    dbs.dedup();
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for d in dbs {
        (d.0, enforcement(svc, cat, d)).hash(&mut h);
    }
    h.finish()
}

/// Whether table `object`'s reads go through its policy view.
#[must_use]
pub fn views_on(svc: &super::TenantServices, cat: &TenantCatalog, object: ObjectId) -> bool {
    cat.objects
        .get(object)
        .is_some_and(|o| enforcement(svc, cat, o.db_id) == "views")
}

/// The user whose account defines `object`'s policy view: it must read
/// and write the table itself (unbound by its policies) and call the
/// context function. `prefer` (the user changing the policies) when it
/// qualifies, else the live user with the lowest id that does, else
/// `prefer` (reads through the view then fail closed, 1142).
#[must_use]
pub fn view_definer(
    cat: &TenantCatalog,
    object: ObjectId,
    prefer: sprig_common::ids::UserId,
) -> sprig_common::ids::UserId {
    use sprig_auth::authz::CompiledPrivileges;
    let (Some(state), Some(auth)) = (AuthzState::of(cat), TenantAuth::of(cat)) else {
        return prefer;
    };
    let Some(obj) = cat.objects.get(object) else {
        return prefer;
    };
    let Some(db) = cat.databases.get(obj.db_id) else {
        return prefer;
    };
    let fits = |u: sprig_common::ids::UserId| {
        let roles = state.granted_roles(u);
        let c = CompiledPrivileges::compile(state, u, &roles);
        if c.everything {
            return true;
        }
        let t = Target::Object {
            db_key: &db.name_key,
            db: obj.db_id,
            object,
        };
        let r = sprig_auth::authz::rls::compile(cat, &c, u);
        [
            Action::Select,
            Action::Insert,
            Action::Update,
            Action::Delete,
        ]
        .iter()
        .all(|a| c.allows(*a, t))
            && !r.tables.contains_key(&object)
    };
    if fits(prefer) {
        return prefer;
    }
    let mut ids: Vec<_> = auth.users().map(|u| u.row.user_id).collect();
    ids.sort_unstable();
    ids.into_iter().find(|u| fits(*u)).unwrap_or(prefer)
}

/// The views a bound session's statements read instead of its protected
/// tables: tables that bind it, whose checks (triggers and views) are
/// deployed, with `rls.backend_enforcement = views`. `None` when there
/// are none.
#[must_use]
pub fn view_swap(
    ts: &TenantSession,
    cat: &TenantCatalog,
    rls: &SessionRls,
) -> Option<Arc<sprig_sql::rls::views::ViewSwap>> {
    let svc = ts.services();
    let mut v = sprig_sql::rls::views::ViewSwap {
        tenant: ts.tenant.0,
        ..Default::default()
    };
    for t in rls.tables.values() {
        if !t.checks_deployed || !views_on(svc, cat, t.object) {
            continue;
        }
        let Some(o) = cat.objects.get(t.object) else {
            continue;
        };
        let view = sprig_sql::rls::views::view_name(&o.name, t.object.0);
        if let Some(cols) = hidden_of(ts.tenant.0, t.object.0) {
            v.hidden.insert(
                view.clone(),
                (
                    sprig_sql::rls::views::hidden_view_name(&o.name, t.object.0),
                    cols.to_vec(),
                ),
            );
        }
        v.tables
            .entry(o.db_id.0)
            .or_default()
            .insert(o.name.to_lowercase(), view);
        // A key-value namespace's element tables read their own
        // policy views, which apply the key table's policies.
        for (e, cols) in elements_of(ts.tenant.0, t.object.0)
            .iter()
            .flat_map(|x| x.iter())
        {
            let ev = sprig_sql::rls::views::view_name(e, t.object.0);
            if !cols.is_empty() {
                v.hidden.insert(
                    ev.clone(),
                    (
                        sprig_sql::rls::views::hidden_view_name(e, t.object.0),
                        cols.clone(),
                    ),
                );
            }
            v.tables
                .entry(o.db_id.0)
                .or_default()
                .insert(e.to_lowercase(), ev);
        }
    }
    if v.tables.is_empty() {
        return None;
    }
    v.temporary = ts.temp.iter().map(|(_, n)| n.clone()).collect();
    Some(Arc::new(v))
}

/// Narrows the tenant's wanted backend grants on `cluster` for policy
/// views (`sprig_auth::authz::rls::narrow`): reads the relations of the
/// databases with protected tables there, as `sprig_ctl`.
///
/// # Errors
/// The cluster's.
pub async fn narrow_backend(
    svc: &super::TenantServices,
    cat: &TenantCatalog,
    cluster: &sprig_common::ids::ClusterId,
    want: &mut sprig_auth::authz::backend::BackendState,
) -> Result<()> {
    use mysql_async::prelude::Queryable;
    let Some(state) = RlsState::of(cat).filter(|s| s.any_enabled()) else {
        return Ok(());
    };
    let (Some(authz), Some(auth)) = (AuthzState::of(cat), TenantAuth::of(cat)) else {
        return Ok(());
    };
    // Databases with a protected table whose enforcement is `views`.
    let mut dbs: Vec<DbId> = state
        .rows()
        .security
        .iter()
        .filter(|s| s.enabled && enforcement(svc, cat, s.db_id) == "views")
        .map(|s| s.db_id)
        .collect();
    dbs.sort_unstable();
    dbs.dedup();
    if dbs.is_empty() {
        return Ok(());
    }
    let pool = svc.locator.ctl_pool(cluster).await?;
    let mut conn = pool.get_conn().await.map_err(|e| {
        SprigError::new(
            SprigCode::BackendUnavailable,
            format!("sprig_ctl on {cluster}: {e}"),
        )
    })?;
    let mut physical: Vec<(String, String)> = Vec::new();
    for d in &dbs {
        let like = format!("t{}\\_d{}\\_%", cat.tenant, d.0);
        let rows: Vec<(String, String)> = conn
            .exec(
                "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA LIKE ? UNION ALL SELECT ROUTINE_SCHEMA, ROUTINE_NAME \
                 FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA LIKE ? AND ROUTINE_NAME = ?",
                (
                    like.as_str(),
                    like.as_str(),
                    sprig_sql::rls::views::CTX_FUNCTION,
                ),
            )
            .await
            .map_err(|e| {
                SprigError::new(SprigCode::BackendUnavailable, format!("{cluster}: {e}"))
            })?;
        physical.extend(rows);
    }
    // Policy views' definers (`user@host`): they keep the table.
    let mut definers = HashSet::new();
    for d in &dbs {
        let like = format!("t{}\\_d{}\\_%", cat.tenant, d.0);
        let rows: Vec<(String, String)> = conn
            .exec(
                "SELECT TABLE_NAME, DEFINER FROM information_schema.VIEWS \
                 WHERE TABLE_SCHEMA LIKE ? AND TABLE_NAME LIKE '\\_\\_sprig\\_rls%'",
                (like.as_str(),),
            )
            .await
            .map_err(|e| {
                SprigError::new(SprigCode::BackendUnavailable, format!("{cluster}: {e}"))
            })?;
        for (view, definer) in rows {
            let user = definer.split('@').next().unwrap_or_default().to_owned();
            definers.insert((user, view.to_lowercase()));
        }
    }
    drop(conn);
    let bound = sprig_auth::authz::rls::backend_bound(cat, authz, auth);
    sprig_auth::authz::rls::narrow(want, cat, &bound, &physical, &definers);
    Ok(())
}

/// DDL texts of views and routines by (tenant, object, schema version).
type DdlCache = std::sync::Mutex<HashMap<(u64, u64, u64), Arc<str>>>;

fn ddl_cache() -> &'static DdlCache {
    static C: std::sync::OnceLock<DdlCache> = std::sync::OnceLock::new();
    C.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// A view, routine, trigger or event whose text names a protected table
/// (or another such object), or runs dynamic SQL.
#[derive(Debug, Clone)]
pub struct TaintedCode {
    /// The object.
    pub object: ObjectId,
    /// What it is.
    pub kind: ObjectKind,
    /// Its name.
    pub name: String,
    /// Its database.
    pub db: DbId,
    /// Its catalog schema version.
    pub schema_version: u64,
    /// Whether its body runs dynamic SQL (`PREPARE`, `EXECUTE
    /// IMMEDIATE`): what it reads can't be known.
    pub dynamic: bool,
}

/// What [`tainted`] finds.
#[derive(Debug, Clone, Default)]
pub struct Tainted {
    /// Lower-case names of code (and of tables whose triggers are such
    /// code) a bound session can't use (39366).
    pub names: HashSet<String>,
    /// The code objects among them.
    pub code: Vec<TaintedCode>,
    words: Vec<(String, Option<HashSet<String>>)>,
    triggers: Vec<(String, Option<String>)>,
}

impl Tainted {
    /// The names again, after the code in `safe` was pointed at the policy
    /// views: only what still reaches a protected table unfiltered (code
    /// not in `safe`, and what names it) stays.
    pub fn without(&mut self, safe: &HashSet<ObjectId>) {
        let seeds: HashSet<String> = self
            .code
            .iter()
            .filter(|c| !safe.contains(&c.object))
            .map(|c| c.name.to_lowercase())
            .collect();
        let mut out = closure(&self.words, seeds);
        for (t, parent) in &self.triggers {
            if out.contains(t)
                && let Some(p) = parent
            {
                out.insert(p.clone());
            }
        }
        self.names = out;
    }
}

/// The names in `bodies` that reach `bad` (transitively).
fn closure(
    bodies: &[(String, Option<HashSet<String>>)],
    mut bad: HashSet<String>,
) -> HashSet<String> {
    let mut out: HashSet<String> = bad
        .iter()
        .filter(|b| bodies.iter().any(|(n, _)| n == *b))
        .cloned()
        .collect();
    loop {
        let mut grew = false;
        for (n, body) in bodies {
            if out.contains(n) {
                continue;
            }
            // Unknown text: refused, to fail closed.
            let hit = body
                .as_ref()
                .is_none_or(|b| b.iter().any(|w| bad.contains(w) && w != n));
            if hit {
                out.insert(n.clone());
                bad.insert(n.clone());
                grew = true;
            }
        }
        if !grew {
            break;
        }
    }
    out
}

/// The tenant's views and stored code whose text names a protected table
/// (or another such object), or that run dynamic SQL: MariaDB runs them
/// with their definer's rights, unfiltered, so sessions bound by policies
/// can't use them (39366) unless they read the policy views.
///
/// # Errors
/// The catalog's.
pub async fn tainted(
    ts: &TenantSession,
    cat: &TenantCatalog,
    protected: &[String],
) -> Result<Tainted> {
    use mysql_async::prelude::Queryable;
    let code: Vec<_> = cat
        .objects
        .iter()
        .filter(|o| {
            o.state == ObjectState::Active
                && matches!(
                    o.kind,
                    ObjectKind::View
                        | ObjectKind::Procedure
                        | ObjectKind::Function
                        | ObjectKind::Trigger
                        | ObjectKind::Event
                )
        })
        .cloned()
        .collect();
    if code.is_empty() {
        return Ok(Tainted::default());
    }
    let tenant = ts.tenant.0;
    let missing: Vec<_> = {
        let c = ddl_cache()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        code.iter()
            .filter(|o| !c.contains_key(&(tenant, o.object_id.0, o.schema_version)))
            .map(|o| o.object_id.0)
            .collect()
    };
    if !missing.is_empty() {
        let ep = ts.services().locator.writer(ts.tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        let list = missing
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let rows: Vec<(u64, u64, String)> = conn
            .query(format!(
                "SELECT object_id, version, ddl FROM {}.schema_versions WHERE object_id IN ({list})",
                sprig_sql::rls::quote(ep.store.schema())
            ))
            .await
            .map_err(|e| SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e))?;
        let mut c = ddl_cache()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if c.len() > 100_000 {
            c.clear();
        }
        for (o, v, ddl) in rows {
            c.insert((tenant, o, v), Arc::from(ddl.as_str()));
        }
    }
    let texts: Vec<(String, Option<Arc<str>>)> = {
        let c = ddl_cache()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        code.iter()
            .map(|o| {
                (
                    o.name.to_lowercase(),
                    c.get(&(tenant, o.object_id.0, o.schema_version)).cloned(),
                )
            })
            .collect()
    };
    let lex = LexOptions::mariadb();
    let words = |t: &str| -> HashSet<String> {
        sprig_sql::lex::lex(t.as_bytes(), &lex)
            .map(|all| {
                all.iter()
                    .filter(|k| k.kind.is_ident_like())
                    .map(|k| {
                        String::from_utf8_lossy(&sprig_sql::lex::ident_value(k.bytes(t.as_bytes())))
                            .to_lowercase()
                    })
                    .collect()
            })
            .unwrap_or_default()
    };
    let bodies: Vec<(String, Option<HashSet<String>>)> = texts
        .into_iter()
        .map(|(n, t)| (n, t.map(|t| words(&t))))
        .collect();
    let dynamic = |o: &sprig_catalog::store::ObjectRow, body: Option<&HashSet<String>>| {
        o.kind != ObjectKind::View
            && body.is_some_and(|b| b.contains("prepare") || b.contains("immediate"))
    };
    let mut seeds: HashSet<String> = protected.iter().cloned().collect();
    for (o, (_, b)) in code.iter().zip(&bodies) {
        if dynamic(o, b.as_ref()) {
            seeds.insert(o.name.to_lowercase());
        }
    }
    let mut out = closure(&bodies, seeds);
    // A table whose trigger is tainted: writing it would run that code.
    let triggers: Vec<(String, Option<String>)> = code
        .iter()
        .filter(|o| o.kind == ObjectKind::Trigger)
        .map(|o| {
            (
                o.name.to_lowercase(),
                o.parent_id
                    .and_then(|p| cat.objects.get(p))
                    .map(|p| p.name.to_lowercase()),
            )
        })
        .collect();
    for (t, parent) in &triggers {
        if out.contains(t)
            && let Some(p) = parent
        {
            out.insert(p.clone());
        }
    }
    let tainted_code = code
        .iter()
        .zip(&bodies)
        .filter(|(o, _)| out.contains(&o.name.to_lowercase()))
        .map(|(o, (_, b))| TaintedCode {
            object: o.object_id,
            kind: o.kind,
            name: o.name.clone(),
            db: o.db_id,
            schema_version: o.schema_version,
            dynamic: dynamic(o, b.as_ref()),
        })
        .collect();
    Ok(Tainted {
        names: out,
        code: tainted_code,
        words: bodies,
        triggers,
    })
}

/// The columns of `information_schema.SPRIG_ROW_POLICIES`.
const VIEW_COLUMNS: &[&str] = &[
    "TABLE_SCHEMA",
    "TABLE_NAME",
    "POLICY_NAME",
    "POLICY_MODE",
    "COMMANDS",
    "ROLES",
    "USING_EXPR",
    "CHECK_EXPR",
    "RLS_ENABLED",
    "RLS_FORCED",
];

/// `information_schema.SPRIG_ROW_POLICIES` in `text` replaced by a derived
/// table of the rows `SHOW POLICIES` gives the session (every database it
/// may see); `None` when the text doesn't name the view.
#[must_use]
pub fn inline_view(ts: &TenantSession, text: &[u8], lex: &LexOptions) -> Option<Vec<u8>> {
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
            && is(sig[i + 2], "SPRIG_ROW_POLICIES")
        {
            let aliased = sig.get(i + 3).is_some_and(|t| {
                is(t, "AS")
                    || (matches!(t.kind, TokenKind::Word | TokenKind::QuotedIdent)
                        && ![
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
                        ]
                        .iter()
                        .any(|w| is(t, w)))
            });
            spans.push((sig[i].start as usize, sig[i + 2].end as usize, aliased));
        }
    }
    if spans.is_empty() {
        return None;
    }
    let cat = ts.catalog()?.clone();
    let p = super::authz::privileges(ts, &cat);
    let state = RlsState::of(&cat).cloned().unwrap_or_default();
    let ans = show(
        ts,
        &cat,
        &*p,
        &state,
        None,
        None,
        ts.lower_case_table_names(),
        45,
    );
    let lit = |v: &Option<bytes::Bytes>| match v {
        None => "NULL".to_owned(),
        Some(b) => {
            sprig_sql::rls::sql_string(&String::from_utf8_lossy(b), lex.no_backslash_escapes)
        }
    };
    let mut selects: Vec<String> = ans
        .rows
        .iter()
        .map(|r| {
            let cols: Vec<String> = r
                .iter()
                .zip(VIEW_COLUMNS)
                .map(|(v, c)| format!("{} AS `{c}`", lit(v)))
                .collect();
            format!("SELECT {}", cols.join(", "))
        })
        .collect();
    if selects.is_empty() {
        let cols: Vec<String> = VIEW_COLUMNS
            .iter()
            .map(|c| format!("CAST(NULL AS CHAR(64)) AS `{c}`"))
            .collect();
        selects.push(format!("SELECT {} FROM DUAL WHERE FALSE", cols.join(", ")));
    }
    let derived = format!("({})", selects.join(" UNION ALL "));
    let mut out = Vec::with_capacity(text.len() + derived.len());
    let mut at = 0;
    for (s, e, aliased) in spans {
        out.extend_from_slice(&text[at..s]);
        out.extend_from_slice(derived.as_bytes());
        if !aliased {
            out.extend_from_slice(b" AS `SPRIG_ROW_POLICIES`");
        }
        at = e;
    }
    out.extend_from_slice(&text[at..]);
    Some(out)
}

/// Metadata columns that count or measure rows: hidden for protected
/// tables from sessions their policies bind.
const MASKED: &[(&str, &[&str])] = &[
    (
        "TABLES",
        &[
            "TABLE_ROWS",
            "AVG_ROW_LENGTH",
            "DATA_LENGTH",
            "MAX_DATA_LENGTH",
            "INDEX_LENGTH",
            "DATA_FREE",
            "AUTO_INCREMENT",
            "CHECKSUM",
            "UPDATE_TIME",
        ],
    ),
    (
        "PARTITIONS",
        &[
            "TABLE_ROWS",
            "AVG_ROW_LENGTH",
            "DATA_LENGTH",
            "MAX_DATA_LENGTH",
            "INDEX_LENGTH",
            "DATA_FREE",
            "CHECKSUM",
            "UPDATE_TIME",
        ],
    ),
    ("STATISTICS", &["CARDINALITY"]),
];

/// The 3306 persona's columns of the views [`mask_metadata`] wraps, where
/// they differ from MariaDB's (MySQL 8.4 has no `MAX_INDEX_LENGTH`,
/// `TEMPORARY` or `IGNORED`, and has `IS_VISIBLE` and `EXPRESSION`).
#[must_use]
pub fn mysql84_columns(view: &str) -> Option<Vec<String>> {
    let list = match view.to_ascii_uppercase().as_str() {
        "TABLES" => {
            "TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,ENGINE,VERSION,ROW_FORMAT,\
             TABLE_ROWS,AVG_ROW_LENGTH,DATA_LENGTH,MAX_DATA_LENGTH,INDEX_LENGTH,DATA_FREE,\
             AUTO_INCREMENT,CREATE_TIME,UPDATE_TIME,CHECK_TIME,TABLE_COLLATION,CHECKSUM,\
             CREATE_OPTIONS,TABLE_COMMENT"
        }
        "STATISTICS" => {
            "TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,NON_UNIQUE,INDEX_SCHEMA,INDEX_NAME,\
             SEQ_IN_INDEX,COLUMN_NAME,COLLATION,CARDINALITY,SUB_PART,PACKED,NULLABLE,\
             INDEX_TYPE,COMMENT,INDEX_COMMENT,IS_VISIBLE,EXPRESSION"
        }
        _ => return None,
    };
    Some(list.split(',').map(str::to_owned).collect())
}

/// `information_schema.TABLES`, `PARTITIONS` and `STATISTICS` in `text`
/// wrapped so their row counts, sizes, `AUTO_INCREMENT` and cardinalities
/// are `NULL` for `protected` (logical database, table) pairs; `None` when
/// the text names none of them.
#[must_use]
pub fn mask_metadata(
    text: &[u8],
    lex: &LexOptions,
    columns: &dyn Fn(&str) -> Option<Vec<String>>,
    protected: &[(String, String)],
) -> Option<Vec<u8>> {
    use sprig_sql::lex::{TokenKind, ident_value};
    if protected.is_empty() {
        return None;
    }
    let tokens = sprig_sql::lex::lex(text, lex).ok()?;
    let sig: Vec<&sprig_sql::lex::Token> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    let is = |t: &sprig_sql::lex::Token, w: &str| {
        matches!(t.kind, TokenKind::Word | TokenKind::QuotedIdent)
            && ident_value(t.bytes(text)).eq_ignore_ascii_case(w.as_bytes())
    };
    let nbe = lex.no_backslash_escapes;
    let pairs = protected
        .iter()
        .map(|(d, t)| {
            format!(
                "({}, {})",
                sprig_sql::rls::sql_string(d, nbe),
                sprig_sql::rls::sql_string(t, nbe)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut spans = Vec::new();
    for i in 0..sig.len().saturating_sub(2) {
        if !(is(sig[i], "information_schema") && sig[i + 1].bytes(text) == b".") {
            continue;
        }
        let Some((view, masked)) = MASKED.iter().find(|(v, _)| is(sig[i + 2], v)) else {
            continue;
        };
        let cols = columns(view)?;
        let list: Vec<String> = cols
            .iter()
            .map(|c| {
                if masked.iter().any(|m| m.eq_ignore_ascii_case(c)) {
                    format!("IF((`TABLE_SCHEMA`, `TABLE_NAME`) IN ({pairs}), NULL, `{c}`) AS `{c}`")
                } else {
                    format!("`{c}`")
                }
            })
            .collect();
        let aliased = sig.get(i + 3).is_some_and(|t| {
            is(t, "AS")
                || (matches!(t.kind, TokenKind::Word | TokenKind::QuotedIdent)
                    && ![
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
                    ]
                    .iter()
                    .any(|w| is(t, w)))
        });
        let inner = String::from_utf8_lossy(&text[sig[i].start as usize..sig[i + 2].end as usize])
            .into_owned();
        let derived = format!("(SELECT {} FROM {inner})", list.join(", "));
        spans.push((
            sig[i].start as usize,
            sig[i + 2].end as usize,
            derived,
            (!aliased).then(|| format!(" AS `{view}`")),
        ));
    }
    if spans.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(text.len() + 256);
    let mut at = 0;
    for (s, e, derived, alias) in spans {
        out.extend_from_slice(&text[at..s]);
        out.extend_from_slice(derived.as_bytes());
        if let Some(a) = alias {
            out.extend_from_slice(a.as_bytes());
        }
        at = e;
    }
    out.extend_from_slice(&text[at..]);
    Some(out)
}

/// Gateway sessions the gateway itself runs for work that must read whole
/// tables: the search seeds. Not reachable by clients: only internal code
/// registers a session, for its life.
fn exempt_sessions() -> &'static std::sync::Mutex<HashSet<u32>> {
    static S: std::sync::OnceLock<std::sync::Mutex<HashSet<u32>>> = std::sync::OnceLock::new();
    S.get_or_init(|| std::sync::Mutex::new(HashSet::new()))
}

/// While held, the gateway session `id` reads protected tables without
/// row-level security (an internal, audited exemption).
#[derive(Debug)]
pub struct ExemptGuard(u32);

impl Drop for ExemptGuard {
    fn drop(&mut self) {
        exempt_sessions()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.0);
    }
}

/// Exempts internal session `id` from row-level security for `purpose`
/// (audited as `rls.internal_exempt`).
#[must_use]
pub fn exempt_internal(
    id: sprig_common::ids::ConnId,
    tenant: u64,
    purpose: &'static str,
    object: &str,
) -> ExemptGuard {
    tracing::info!(target: "sprig_audit", event = "rls.internal_exempt", tenant, purpose,
        object, session = id.0, "an internal read without row-level security");
    exempt_sessions()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(id.0);
    ExemptGuard(id.0)
}

/// Whether gateway session `id` is exempt (see [`exempt_internal`]).
#[must_use]
pub fn is_exempt(id: sprig_common::ids::ConnId) -> bool {
    exempt_sessions()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .contains(&id.0)
}

/// The note code of [`definer_default_note`].
pub const DEFINER_DEFAULT_NOTE: u16 = 39369;

/// The protected tables a `CREATE PROCEDURE` or `CREATE FUNCTION` text
/// names when it has no `SQL SECURITY` clause (MariaDB's default is
/// `DEFINER`, so the routine applies its definer's policies, not its
/// caller's), as `db.table`, in order of first mention; `None` for any
/// other statement, an explicit clause, or no protected table. `protected`
/// holds `(db, table)` pairs in lower case; `db` is the database an
/// unqualified name means (the routine's, else the current).
#[must_use]
pub fn definer_default_tables(
    text: &[u8],
    lex_opts: &LexOptions,
    db: Option<&str>,
    protected: &[(String, String)],
) -> Option<Vec<String>> {
    use sprig_sql::lex::{self, TokenKind, ident_value};
    let toks: Vec<(TokenKind, String)> = lex::lex(text, lex_opts)
        .ok()?
        .iter()
        .filter(|t| !t.kind.is_trivia())
        .map(|t| {
            let v = if t.kind.is_ident_like() {
                String::from_utf8_lossy(&ident_value(t.bytes(text))).into_owned()
            } else {
                String::from_utf8_lossy(t.bytes(text)).into_owned()
            };
            (t.kind, v)
        })
        .collect();
    let word = |i: usize, w: &str| {
        toks.get(i)
            .is_some_and(|(k, v)| *k == TokenKind::Word && v.eq_ignore_ascii_case(w))
    };
    if !word(0, "CREATE") {
        return None;
    }
    let kw = toks.iter().position(|(k, v)| {
        *k == TokenKind::Word
            && (v.eq_ignore_ascii_case("PROCEDURE") || v.eq_ignore_ascii_case("FUNCTION"))
    })?;
    // `CREATE [OR REPLACE] [DEFINER = ...] [AGGREGATE] FUNCTION|PROCEDURE`:
    // nothing else may come first (a trigger or event body may name a
    // function).
    if toks[..kw].iter().any(|(k, v)| {
        *k == TokenKind::Word
            && ["TRIGGER", "EVENT", "VIEW", "TABLE"]
                .iter()
                .any(|x| v.eq_ignore_ascii_case(x))
    }) {
        return None;
    }
    if (kw..toks.len()).any(|i| word(i, "SQL") && word(i + 1, "SECURITY")) {
        return None;
    }
    let ident = |i: usize| {
        toks.get(i)
            .filter(|(k, _)| k.is_ident_like())
            .map(|(_, v)| v.to_lowercase())
    };
    let dot = |i: usize| toks.get(i).is_some_and(|(_, v)| v == ".");
    // The routine's own database, when its name is qualified.
    let own_db = (ident(kw + 1).is_some() && dot(kw + 2))
        .then(|| ident(kw + 1))
        .flatten()
        .or_else(|| db.map(str::to_lowercase));
    let mut found: Vec<String> = Vec::new();
    let mut i = kw + 1;
    while i < toks.len() {
        let Some(a) = ident(i) else {
            i += 1;
            continue;
        };
        let (d, t, next) = match ident(i + 2) {
            Some(b) if dot(i + 1) => (Some(a), b, i + 3),
            _ => (own_db.clone(), a, i + 1),
        };
        if let Some(d) = d
            && protected.iter().any(|(pd, pt)| *pd == d && *pt == t)
        {
            let n = format!("{d}.{t}");
            if !found.contains(&n) {
                found.push(n);
            }
        }
        i = next;
    }
    (!found.is_empty()).then_some(found)
}

/// A note for a `CREATE PROCEDURE` or `CREATE FUNCTION` without
/// `SQL SECURITY` over policy-protected tables of `ts`'s tenant, naming
/// them (see [`definer_default_tables`]).
#[must_use]
pub fn definer_default_note(
    ts: &TenantSession,
    text: &[u8],
    lex_opts: &LexOptions,
    current_db: Option<&str>,
) -> Option<super::session::LocalWarning> {
    let cat = ts.catalog()?;
    let state = RlsState::of(cat)?;
    let protected: Vec<(String, String)> = state
        .rows()
        .security
        .iter()
        .filter(|s| s.enabled)
        .filter_map(|s| {
            let o = cat.objects.get(s.object_id)?;
            let d = cat.databases.get(o.db_id)?;
            Some((d.name.to_lowercase(), o.name.to_lowercase()))
        })
        .collect();
    if protected.is_empty() {
        return None;
    }
    let tables = definer_default_tables(text, lex_opts, current_db, &protected)?;
    Some(super::session::LocalWarning {
        level: "Note",
        code: DEFINER_DEFAULT_NOTE,
        message: format!(
            "ER_SPRIG_RLS_DEFINER_DEFAULT: this routine has no SQL SECURITY clause, so it runs \
             as SQL SECURITY DEFINER and applies its definer's row-level security policies, \
             not its caller's, to {}; add SQL SECURITY INVOKER to keep the caller's",
            tables.join(", ")
        ),
    })
}

#[cfg(test)]
mod definer_default_tests {
    use super::*;

    fn t(text: &str, db: Option<&str>) -> Option<Vec<String>> {
        let protected = vec![
            ("app".to_owned(), "docs".to_owned()),
            ("other".to_owned(), "secrets".to_owned()),
        ];
        definer_default_tables(text.as_bytes(), &LexOptions::mariadb(), db, &protected)
    }

    #[test]
    fn routines_without_sql_security_over_protected_tables_are_noted() {
        assert_eq!(
            t("CREATE PROCEDURE app.p() SELECT COUNT(*) FROM docs", None),
            Some(vec!["app.docs".to_owned()])
        );
        assert_eq!(
            t(
                "CREATE DEFINER=CURRENT_USER FUNCTION f() RETURNS INT READS SQL DATA \
                 RETURN (SELECT COUNT(*) FROM `Docs` JOIN other.secrets USING (id))",
                Some("app")
            ),
            Some(vec!["app.docs".to_owned(), "other.secrets".to_owned()])
        );
        // An explicit clause, either way: no note.
        assert_eq!(
            t(
                "CREATE PROCEDURE app.p() SQL SECURITY DEFINER SELECT * FROM docs",
                None
            ),
            None
        );
        assert_eq!(
            t(
                "create procedure app.p() sql security invoker select * from docs",
                None
            ),
            None
        );
        // No protected table, or another database's table of that name.
        assert_eq!(
            t("CREATE PROCEDURE app.p() SELECT * FROM notes", None),
            None
        );
        assert_eq!(
            t("CREATE PROCEDURE p() SELECT * FROM docs", Some("x")),
            None
        );
        // Not a routine.
        assert_eq!(t("CREATE VIEW app.v AS SELECT * FROM docs", None), None);
        assert_eq!(
            t(
                "CREATE TRIGGER app.tr BEFORE INSERT ON docs FOR EACH ROW SET @x = 1",
                None
            ),
            None
        );
    }
}
