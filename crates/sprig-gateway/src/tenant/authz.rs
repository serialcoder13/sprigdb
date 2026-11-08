//! Chapter 29 in the gateway (M0.5): privilege checks on every tenant
//! statement, the account statements, and the per-user backend accounts.
//!
//! **Checks.** Each session compiles its privileges from the snapshot it
//! reads ([`AuthzState`] in the catalog cache) and compiles again when the
//! snapshot's auth or catalog version, or its active roles, change. Every
//! gateway's cache reloads a tenant when its directory entry's auth version
//! moves (the watch settings changes use), so a `REVOKE` holds from the
//! next statement on every gateway once its cache has the version. A
//! statement is checked before it is forwarded:
//!
//! | Statement | Check | Refused with |
//! |---|---|---|
//! | `SELECT`, `INSERT`, `REPLACE`, `UPDATE`, `DELETE` | each relation ([`sprig_sql::access`]) | 1142 |
//! | DDL | `CREATE` on the database, the action on the object | 1142, 1044 (databases) |
//! | `CALL` | `EXECUTE` on the procedure | 1370 |
//! | `USE` | any privilege in the database | 1044 |
//! | `SHOW DATABASES` | rows filtered to reachable databases | |
//!
//! What the gateway can't model (statements sqlparser-rs doesn't parse,
//! column lists, view and routine bodies) MariaDB checks through the
//! user's own backend account.
//!
//! **Backend accounts.** With per-user accounts configured (`[per_user]`
//! in the backends file), [`AuthzServices::ensure`] brings the tenant's
//! accounts on its cluster to what the snapshot says before a session uses
//! them: after every auth or catalog change this gateway sees, compare the
//! wanted grants' hash with the last applied one and reconcile when it
//! moved. Accounts whose grants changed get their pooled connections
//! retired (MariaDB keeps a connection's database privileges until its
//! next `USE`).
//!
//! **Migration.** A tenant whose catalog predates the `authz` module is
//! migrated by the first session that reads it (every existing user keeps
//! `ALL` and `SPRIG_TENANT_ADMIN`; see `sprig_auth::authz::store`).

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use sprig_auth::TenantAuth;
use sprig_auth::authz::account_sql::{
    self, AccountCtx, Outcome, db_denied, routine_denied, table_denied,
};
use sprig_auth::authz::backend::{self, BackendKey};
use sprig_auth::authz::definers;
use sprig_auth::authz::settings::{ACTIVATE_ALL_ROLES_ON_LOGIN, UNSAFE_PRIVILEGE_ACTION};
use sprig_auth::authz::stmt::AccountStmt;
use sprig_auth::authz::{Action, ActionSet, AuthzState, CompiledPrivileges, ObjectType, Target};
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::{DbState, Namespace, ObjectKind, ObjectState};
use sprig_catalog::{Freshness, TenantCatalog};
use sprig_common::ids::{ClusterId, RoleId, TenantId};
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::Persona;
use sprig_sql::access::{self, Need};
use sprig_sql::lex::LexOptions;
use sprig_sql::names::{LocalAnswer, StmtKind, StmtPlan, SystemSchema, UseTarget};

use super::TenantServices;
use super::backends::PerUser;
use super::session::TenantSession;

/// Serializes reconciles of one tenant on one cluster.
type Gate = Arc<tokio::sync::Mutex<()>>;

/// A compiled set and what it was compiled at: auth and catalog
/// versions, active roles.
type Compiled = (u64, u64, Vec<RoleId>, Arc<CompiledPrivileges>);

/// The process-wide half: per-user account settings and what was applied.
#[derive(Default)]
pub struct AuthzServices {
    per_user: ArcSwapOption<PerUser>,
    applied: Mutex<HashMap<(TenantId, ClusterId), u64>>,
    /// The snapshot versions last compared, per tenant and cluster.
    checked: Mutex<HashMap<(TenantId, ClusterId), (u64, u64)>>,
    gates: Mutex<HashMap<(TenantId, ClusterId), Gate>>,
    migrating: Mutex<HashSet<TenantId>>,
}

impl std::fmt::Debug for AuthzServices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthzServices")
            .field("per_user", &self.per_user.load().is_some())
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl AuthzServices {
    /// Turns per-user backend accounts on (`[per_user]` in the backends
    /// file).
    pub fn configure(&self, per_user: Option<PerUser>) {
        self.per_user.store(per_user.map(Arc::new));
    }

    /// Whether sessions run as per-user backend accounts.
    #[must_use]
    pub fn per_user(&self) -> bool {
        self.per_user.load().is_some()
    }

    /// The key per-user passwords derive from.
    #[must_use]
    pub fn key(&self) -> Option<BackendKey> {
        self.per_user.load().as_ref().map(|p| p.key.clone())
    }

    /// Migrates `cat`'s tenant to the `authz` module if it predates it (once
    /// per process and tenant at a time; a failure is logged and retried
    /// by a later session).
    async fn migrate_if_needed(&self, svc: &TenantServices, cat: &TenantCatalog) {
        let migrated = AuthzState::of(cat).is_some_and(AuthzState::is_migrated);
        if migrated || !lock(&self.migrating).insert(cat.tenant) {
            return;
        }
        let r = sprig_auth::authz::migrate(&*svc.locator, &svc.catalog, cat.tenant).await;
        match r {
            Ok(v) => {
                tracing::info!(tenant = %cat.tenant, auth_version = v, "authz module migrated: existing users keep every privilege");
            }
            Err(e) => {
                tracing::warn!(tenant = %cat.tenant, error = %e, "authz migration failed; retried later");
            }
        }
        lock(&self.migrating).remove(&cat.tenant);
    }

    /// Brings the tenant's per-user backend accounts on `cluster` to what
    /// `cat` says (see the module docs). A no-op without per-user
    /// accounts, or when nothing changed since the last reconcile.
    ///
    /// # Errors
    /// The cluster's (no `sprig_ctl` pool, a failed statement).
    pub async fn ensure(
        &self,
        svc: &TenantServices,
        cat: &TenantCatalog,
        cluster: &ClusterId,
    ) -> Result<()> {
        self.migrate_if_needed(svc, cat).await;
        let Some(per_user) = self.per_user.load_full() else {
            return Ok(());
        };
        let key = (cat.tenant, cluster.clone());
        let versions = (cat.version, cat.auth_version);
        if lock(&self.checked).get(&key) == Some(&versions) {
            return Ok(());
        }
        let Some(auth) = TenantAuth::of(cat) else {
            return Ok(());
        };
        let state = AuthzState::of(cat).cloned().unwrap_or_default();
        let want = backend::desired(cat, &state, auth);
        let hash = {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            want.hash(&mut h);
            h.finish()
        };
        if lock(&self.applied).get(&key) == Some(&hash) {
            lock(&self.checked).insert(key, versions);
            return Ok(());
        }
        let gate = Arc::clone(lock(&self.gates).entry(key.clone()).or_default());
        let _g = gate.lock().await;
        let full = match lock(&self.applied).get(&key) {
            Some(h) if *h == hash => {
                lock(&self.checked).insert(key, versions);
                return Ok(());
            }
            Some(_) => false,
            None => true,
        };
        let pool = svc.locator.ctl_pool(cluster).await?;
        let mut conn = pool.get_conn().await.map_err(|e| {
            SprigError::new(
                sprig_common::SprigCode::BackendUnavailable,
                format!("sprig_ctl on {cluster}: {e}"),
            )
        })?;
        let started = std::time::Instant::now();
        let rep = backend::reconcile(&mut conn, cat.tenant, &want, &per_user.key, full).await?;
        if full {
            // Stored code created as sprig_ctl before definers were the
            // session's account (chapter 29, "Definers of stored code").
            let to = definers::wanted(cat, &state, auth);
            match definers::migrate(&mut conn, cat.tenant, &to).await {
                Ok(r) => {
                    for o in &r.moved {
                        tracing::warn!(tenant = %cat.tenant, %cluster, kind = o.kind.keyword(), schema = %o.schema, name = %o.name, from = %o.definer, "stored code moved off a non-tenant definer");
                    }
                    for (o, e) in &r.failed {
                        tracing::error!(tenant = %cat.tenant, %cluster, kind = o.kind.keyword(), schema = %o.schema, name = %o.name, definer = %o.definer, error = %e, "stored code still has a non-tenant definer");
                    }
                }
                Err(e) => {
                    tracing::error!(tenant = %cat.tenant, %cluster, error = %e, "checking stored code definers failed");
                }
            }
        }
        drop(conn);
        for acct in &rep.changed {
            svc.locator.retire_account(acct);
        }
        metrics::histogram!("sprig_auth_reconcile_duration_seconds", "cluster" => cluster.to_string())
            .record(started.elapsed().as_secs_f64());
        if rep.statements > 0 {
            tracing::info!(tenant = %cat.tenant, %cluster, statements = rep.statements, created = rep.created, dropped = rep.dropped, changed = rep.changed.len(), "backend grants reconciled");
        }
        lock(&self.applied).insert(key.clone(), hash);
        lock(&self.checked).insert(key, versions);
        Ok(())
    }
}

/// One session's authorization state.
#[derive(Debug, Default)]
pub struct SessionAuthz {
    /// The active roles; `None` until the first compile (then the default
    /// roles, or all with `auth.activate_all_roles_on_login`).
    active: Mutex<Option<Vec<RoleId>>>,
    compiled: Mutex<Option<Compiled>>,
}

impl SessionAuthz {
    /// Forgets the compiled set (after `SET ROLE`).
    pub fn invalidate(&self) {
        *lock(&self.compiled) = None;
    }

    /// Sets the active roles (`SET ROLE`).
    pub fn set_active(&self, roles: Vec<RoleId>) {
        *lock(&self.active) = Some(roles);
        self.invalidate();
    }

    /// The active roles (empty before the first compile).
    #[must_use]
    pub fn active(&self) -> Vec<RoleId> {
        lock(&self.active).clone().unwrap_or_default()
    }
}

/// The session's privileges at `cat` (compiled once per snapshot and
/// active-role set). Active roles no longer granted drop out.
#[must_use]
pub fn privileges(ts: &TenantSession, cat: &TenantCatalog) -> Arc<CompiledPrivileges> {
    let Some(user) = ts.principal.user_id else {
        return Arc::new(CompiledPrivileges::everything());
    };
    let state = AuthzState::of(cat).cloned().unwrap_or_default();
    let active = {
        let mut a = lock(&ts.authz.active);
        if a.is_none() {
            let all = ts.services().settings.get(
                &ACTIVATE_ALL_ROLES_ON_LOGIN,
                &cat.setting_ctx(&ts.services().dc),
                Some(&cat.settings),
            );
            *a = Some(if all {
                state.granted_roles(user)
            } else {
                state.default_roles(user)
            });
        }
        let granted = state.granted_roles(user);
        a.iter()
            .flatten()
            .copied()
            .filter(|r| granted.contains(r))
            .collect::<Vec<RoleId>>()
    };
    let mut g = lock(&ts.authz.compiled);
    if let Some((a, c, roles, p)) = &*g
        && *a == cat.auth_version
        && *c == cat.version
        && *roles == active
    {
        return Arc::clone(p);
    }
    metrics::counter!("sprig_auth_compiles_total").increment(1);
    let p = Arc::new(CompiledPrivileges::compile(&state, user, &active));
    *g = Some((cat.auth_version, cat.version, active, Arc::clone(&p)));
    p
}

/// What [`account_statement`] answers.
#[derive(Debug)]
pub enum AccountAnswer {
    /// OK with notes.
    Ok(Vec<super::session::LocalWarning>),
    /// `SHOW GRANTS`.
    Rows(LocalAnswer),
}

/// Runs an account statement for the session; waits until this gateway's
/// snapshot has the change and its backend grants are applied, so the
/// next statement (and MariaDB) sees it.
///
/// # Errors
/// The statement's MySQL error, or the catalog's.
pub async fn account_statement(
    ts: &mut TenantSession,
    cat: &Arc<TenantCatalog>,
    stmt: &AccountStmt,
    persona: Persona,
    current: Option<&str>,
    charset: u16,
) -> Result<AccountAnswer> {
    let Some(user) = ts.principal.user_id else {
        return Err(account_sql::need_privilege("CREATE USER"));
    };
    let compiled = privileges(ts, cat);
    let svc = Arc::clone(ts.services());
    let ctx = cat.setting_ctx(&svc.dc);
    let hashing = sprig_auth::admin::Hashing {
        caching_sha2_rounds: u32::try_from(svc.settings.get(
            &sprig_auth::settings::CACHING_SHA2_ROUNDS,
            &ctx,
            Some(&cat.settings),
        ))
        .unwrap_or(5000),
        native: svc.settings.get(
            &sprig_auth::settings::MYSQL_NATIVE_PASSWORD,
            &ctx,
            Some(&cat.settings),
        ),
    };
    let unsafe_error = svc
        .settings
        .get(&UNSAFE_PRIVILEGE_ACTION, &ctx, Some(&cat.settings))
        == "error";
    let client_host = ts.principal.client_ip.to_string();
    let lctn = ts.lower_case_table_names();
    let active = ts.authz.active();
    let policy = sprig_auth::SettingsPolicy::new(svc.settings.clone(), svc.dc.clone());
    let audit = sprig_auth::audit::LogAudit::default();
    let tenant_name = ts.principal.tenant_name.clone();
    let outcome = {
        let actx = AccountCtx {
            tenant_name: tenant_name.as_deref(),
            policy: &policy,
            audit: &audit,
            catalog: &svc.catalog,
            snapshot: cat,
            tenant: ts.tenant,
            user,
            user_name: &ts.principal.user,
            user_host: &ts.principal.host,
            client_host: &client_host,
            persona,
            current_db: current,
            lower_case_table_names: lctn,
            privileges: &compiled,
            active_roles: &active,
            hashing,
            unsafe_error,
            clock: &*svc.clock,
        };
        account_sql::execute(&actx, stmt).await?
    };
    match outcome {
        Outcome::Ok {
            notes,
            auth_version,
        } => {
            if let Some(v) = auth_version {
                let fresh = await_auth(&svc, ts.tenant, v).await;
                if let Some(c) = fresh {
                    let cluster = ts.principal.backend.cluster.clone();
                    if let Err(e) = svc.authz.ensure(&svc, &c, &cluster).await {
                        tracing::warn!(tenant = %ts.tenant, error = %e, "backend grants not applied yet; the gateway enforces the change");
                        return Ok(AccountAnswer::Ok(vec![super::session::LocalWarning {
                            level: "Warning",
                            code: 39351,
                            message: "ER_SPRIG_AUTH_PROPAGATION_PENDING: the change holds in the \
                                      gateway; MariaDB's grants follow"
                                .to_owned(),
                        }]));
                    }
                }
            }
            Ok(AccountAnswer::Ok(
                notes
                    .into_iter()
                    .map(|n| super::session::LocalWarning {
                        level: n.level,
                        code: n.code,
                        message: n.message,
                    })
                    .collect(),
            ))
        }
        Outcome::SetRole(roles) => {
            ts.authz.set_active(roles);
            Ok(AccountAnswer::Ok(Vec::new()))
        }
        Outcome::Grants { header, lines } => Ok(AccountAnswer::Rows(LocalAnswer {
            columns: vec![crate::pipeline::text_column(&header, charset)],
            rows: lines
                .into_iter()
                .map(|l| vec![Some(bytes::Bytes::from(l))])
                .collect(),
        })),
    }
}

/// Waits (up to 5 s) until this gateway's cache holds `tenant` at auth
/// version `v` or later; the snapshot, or `None` on timeout.
async fn await_auth(svc: &TenantServices, tenant: TenantId, v: u64) -> Option<Arc<TenantCatalog>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(c) = svc.cache.get(tenant, Freshness::Latest).await
            && c.auth_version >= v
        {
            return Some(c);
        }
        if std::time::Instant::now() > deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Facts a check needs about the session.
pub struct CheckCtx<'a> {
    /// The snapshot.
    pub cat: &'a TenantCatalog,
    /// The session's privileges.
    pub p: &'a CompiledPrivileges,
    /// The persona.
    pub persona: Persona,
    /// The user name.
    pub user: &'a str,
    /// The account host.
    pub host: &'a str,
    /// The client address.
    pub client_host: &'a str,
    /// The current logical database.
    pub current: Option<&'a str>,
    /// `lower_case_table_names`.
    pub lctn: bool,
    /// The session's temporary tables.
    pub temp: &'a dyn Fn(Option<&str>, &str) -> bool,
}

impl CheckCtx<'_> {
    fn db(&self, db: Option<&str>) -> Option<String> {
        db.or(self.current).map(str::to_owned)
    }

    fn table(&self, db: Option<&str>, name: &str, any: ActionSet) -> Result<()> {
        let Some(db) = self.db(db) else {
            return Ok(());
        };
        if SystemSchema::parse(&db).is_some() || (self.temp)(Some(&db), name) {
            return Ok(());
        }
        let key = database_key(&db, self.lctn);
        let Some(row) = self
            .cat
            .database(&key)
            .filter(|d| d.state == DbState::Active)
        else {
            return Ok(());
        };
        let okey = object_key(ObjectKind::Table, name, self.lctn);
        let target = match self
            .cat
            .object(row.db_id, Namespace::Relation, &okey)
            .filter(|o| o.state == ObjectState::Active)
        {
            Some(o) => Target::Object {
                db_key: &key,
                db: row.db_id,
                object: o.object_id,
            },
            None => Target::Database { db_key: &key },
        };
        if any.iter().any(|a| self.p.allows(a, target)) {
            return Ok(());
        }
        let cmd = any.iter().next().map_or("SELECT", Action::command);
        Err(table_denied(
            self.persona,
            cmd,
            self.user,
            self.client_host,
            &db,
            name,
        ))
    }

    fn database(&self, db: Option<&str>, action: Action) -> Result<()> {
        let Some(db) = self.db(db) else {
            return Ok(());
        };
        if SystemSchema::parse(&db).is_some() {
            return Ok(());
        }
        let key = database_key(&db, self.lctn);
        if self.p.allows(action, Target::Database { db_key: &key }) {
            return Ok(());
        }
        Err(db_denied(self.user, self.host, &db))
    }

    fn routine(
        &self,
        db: Option<&str>,
        name: &str,
        kind: ObjectType,
        action: Action,
    ) -> Result<()> {
        let Some(db) = self.db(db) else {
            return Ok(());
        };
        if SystemSchema::parse(&db).is_some() {
            return Ok(());
        }
        let key = database_key(&db, self.lctn);
        let Some(row) = self
            .cat
            .database(&key)
            .filter(|d| d.state == DbState::Active)
        else {
            return Ok(());
        };
        let (ns, ok) = match kind {
            ObjectType::Function => (Namespace::Function, ObjectKind::Function),
            _ => (Namespace::Procedure, ObjectKind::Procedure),
        };
        let target = match self
            .cat
            .object(row.db_id, ns, &object_key(ok, name, self.lctn))
            .filter(|o| o.state == ObjectState::Active)
        {
            Some(o) => Target::Object {
                db_key: &key,
                db: row.db_id,
                object: o.object_id,
            },
            None => Target::Database { db_key: &key },
        };
        if self.p.allows(action, target) {
            return Ok(());
        }
        Err(routine_denied(
            action.command(),
            self.user,
            self.client_host,
            &db,
            name,
        ))
    }

    fn need(&self, n: &Need) -> Result<()> {
        match n {
            Need::Table { db, name, any } => self.table(db.as_deref(), name, *any),
            Need::Database { db, action, object } => match object {
                // Creating an object is refused as MySQL words it: for
                // the new table's name.
                Some(o) => {
                    let Some(d) = self.db(db.as_deref()) else {
                        return Ok(());
                    };
                    if SystemSchema::parse(&d).is_some() {
                        return Ok(());
                    }
                    let key = database_key(&d, self.lctn);
                    if self.p.allows(*action, Target::Database { db_key: &key }) {
                        return Ok(());
                    }
                    Err(table_denied(
                        self.persona,
                        action.command(),
                        self.user,
                        self.client_host,
                        &d,
                        o,
                    ))
                }
                None => self.database(db.as_deref(), *action),
            },
            Need::Routine {
                db,
                name,
                kind,
                action,
            } => self.routine(db.as_deref(), name, *kind, *action),
        }
    }
}

/// `CALL [db.]proc`: the procedure's name.
fn call_target(text: &[u8], lex: LexOptions) -> Option<(Option<String>, String)> {
    use sprig_sql::lex::{TokenKind, ident_value};
    let tokens = sprig_sql::lex::lex(text, &lex).ok()?;
    let t: Vec<_> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    let name = |i: usize| -> Option<String> {
        let tk = t.get(i)?;
        tk.kind
            .is_ident_like()
            .then(|| String::from_utf8_lossy(&ident_value(tk.bytes(text))).into_owned())
    };
    if !t.first()?.bytes(text).eq_ignore_ascii_case(b"CALL") {
        return None;
    }
    let first = name(1)?;
    if t.get(2)
        .is_some_and(|d| d.kind == TokenKind::Punct && d.bytes(text) == b".")
    {
        return Some((Some(first), name(3)?));
    }
    Some((None, first))
}

/// Checks one planned statement (see the module docs).
///
/// # Errors
/// The MySQL error the statement is refused with.
pub fn check(cx: &CheckCtx<'_>, sp: &StmtPlan, original: &[u8], lex: &LexOptions) -> Result<()> {
    if cx.p.everything {
        return Ok(());
    }
    check_text(cx, sp.info.kind, original, *lex)?;
    if sp.info.kind == StmtKind::Use
        && let Some(UseTarget::Database { logical, .. }) = &sp.info.use_target
    {
        let key = database_key(logical, cx.lctn);
        let id = cx.cat.database(&key).map(|d| d.db_id);
        if !cx.p.any_in_database(&key, id) {
            return Err(db_denied(cx.user, cx.host, logical));
        }
    }
    if let Some(t) = &sp.info.ddl {
        for n in access::ddl_requirements(t) {
            cx.need(&n)?;
        }
    }
    Ok(())
}

/// The checks of [`check`] that need only the statement's text: the
/// relations of a `SELECT`, `INSERT`, `REPLACE`, `UPDATE` or `DELETE`, and
/// the procedure of a `CALL`.
fn check_text(cx: &CheckCtx<'_>, kind: StmtKind, original: &[u8], lex: LexOptions) -> Result<()> {
    if cx.p.everything {
        return Ok(());
    }
    match kind {
        StmtKind::Select
        | StmtKind::Insert
        | StmtKind::Replace
        | StmtKind::Update
        | StmtKind::Delete => {
            if let Some(needs) = access::requirements(original, &lex) {
                for n in &needs {
                    cx.need(n)?;
                }
            }
        }
        StmtKind::Call => {
            if let Some((db, name)) = call_target(original, lex) {
                cx.routine(db.as_deref(), &name, ObjectType::Procedure, Action::Execute)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// What a prepared statement was checked with at `COM_STMT_PREPARE`, so
/// `COM_STMT_EXECUTE` can check it again when the session's privileges
/// changed since (MySQL checks at every execute: a `REVOKE` between
/// prepare and execute refuses the execute).
#[derive(Debug, Clone)]
pub struct Recheck {
    kind: StmtKind,
    text: bytes::Bytes,
    lex: LexOptions,
    current: Option<String>,
    compiled: Arc<CompiledPrivileges>,
}

impl Recheck {
    /// The facts of a statement just checked at prepare; `None` for a
    /// session without a snapshot.
    #[must_use]
    pub fn new(
        ts: &TenantSession,
        kind: StmtKind,
        text: &[u8],
        lex: LexOptions,
        current: Option<&str>,
    ) -> Option<Self> {
        let cat = ts.catalog().cloned()?;
        Some(Self {
            kind,
            text: bytes::Bytes::copy_from_slice(text),
            lex,
            current: current.map(str::to_owned),
            compiled: privileges(ts, &cat),
        })
    }
}

/// Checks a prepared statement again at execute when the session's
/// compiled privileges are no longer the ones it was prepared under. The
/// names bind as at prepare (its default database then).
///
/// # Errors
/// As [`check`].
pub fn recheck(ts: &TenantSession, persona: Persona, rc: &Recheck) -> Result<()> {
    let Some(cat) = ts.catalog().cloned() else {
        return Ok(());
    };
    let p = privileges(ts, &cat);
    if Arc::ptr_eq(&p, &rc.compiled) {
        return Ok(());
    }
    let client_host = ts.principal.client_ip.to_string();
    let lctn = ts.lower_case_table_names();
    let temp = |db: Option<&str>, name: &str| ts.is_temp(db, name);
    let cx = CheckCtx {
        cat: &cat,
        p: &p,
        persona,
        user: &ts.principal.user,
        host: &ts.principal.host,
        client_host: &client_host,
        current: rc.current.as_deref(),
        lctn,
        temp: &temp,
    };
    let r = check_text(&cx, rc.kind, &rc.text, rc.lex);
    if r.is_err() {
        metrics::counter!("sprig_auth_checks_total", "protocol" => "mysql", "result" => "deny")
            .increment(1);
    }
    r
}

/// `SHOW DATABASES` without the tenant-scoped `SHOW DATABASES` privilege:
/// only databases the user holds any privilege in (system schemas stay).
pub fn filter_databases(
    ans: &mut LocalAnswer,
    cat: &TenantCatalog,
    p: &CompiledPrivileges,
    lctn: bool,
) {
    if p.everything || p.tenant.has(Action::ShowDatabases) {
        return;
    }
    ans.rows.retain(|r| {
        let Some(Some(name)) = r.first() else {
            return true;
        };
        let name = String::from_utf8_lossy(name);
        if SystemSchema::parse(&name).is_some() {
            return true;
        }
        let key = database_key(&name, lctn);
        let id = cat.database(&key).map(|d| d.db_id);
        p.any_in_database(&key, id)
    });
}

/// [`check`] for a session at its installed snapshot.
///
/// # Errors
/// As [`check`].
pub fn check_session(
    ts: &TenantSession,
    persona: Persona,
    sp: &StmtPlan,
    original: &[u8],
    lex: &LexOptions,
    current: Option<&str>,
) -> Result<()> {
    let Some(cat) = ts.catalog().cloned() else {
        return Ok(());
    };
    let p = privileges(ts, &cat);
    let client_host = ts.principal.client_ip.to_string();
    let lctn = ts.lower_case_table_names();
    let temp = |db: Option<&str>, name: &str| ts.is_temp(db, name);
    let cx = CheckCtx {
        cat: &cat,
        p: &p,
        persona,
        user: &ts.principal.user,
        host: &ts.principal.host,
        client_host: &client_host,
        current,
        lctn,
        temp: &temp,
    };
    let r = check(&cx, sp, original, lex);
    if r.is_err() {
        metrics::counter!("sprig_auth_checks_total", "protocol" => "mysql", "result" => "deny")
            .increment(1);
    }
    r
}

/// The routines a `DROP PROCEDURE` or `DROP FUNCTION` names that hold
/// privilege rows. MySQL 8.4 and MariaDB 11.8 revoke a routine's
/// privileges when it is dropped, but keep table, column and database
/// privileges through `DROP TABLE` and `DROP DATABASE` (measured on both),
/// so only routines are cleaned up (chapter 29, "Privileges of dropped
/// objects").
#[must_use]
pub fn dropped_routines(
    ts: &TenantSession,
    stmt: &sprig_catalog::ddl::DdlStatement,
    current: Option<&str>,
) -> Vec<(sprig_common::ids::DbId, sprig_common::ids::ObjectId)> {
    let sprig_catalog::ddl::DdlStatement::DropObjects { kind, names, .. } = stmt else {
        return Vec::new();
    };
    let ns = match kind {
        ObjectKind::Procedure => Namespace::Procedure,
        ObjectKind::Function => Namespace::Function,
        _ => return Vec::new(),
    };
    let Some(cat) = ts.catalog().cloned() else {
        return Vec::new();
    };
    let Some(state) = AuthzState::of(&cat) else {
        return Vec::new();
    };
    let lctn = ts.lower_case_table_names();
    let mut out = Vec::new();
    for n in names {
        let Some(db) = n.db.as_deref().or(current) else {
            continue;
        };
        let Some(d) = cat.database(&database_key(db, lctn)) else {
            continue;
        };
        let Some(o) = cat.object(d.db_id, ns, &object_key(*kind, &n.name, lctn)) else {
            continue;
        };
        let held = state.rows().privileges.iter().any(|p| {
            matches!(p.target, sprig_auth::authz::PrivTarget::Object { db, object }
                if db == d.db_id && object == o.object_id)
        });
        if held {
            out.push((d.db_id, o.object_id));
        }
    }
    out
}

/// Deletes the privilege rows of routines a `DROP` removed (see
/// [`dropped_routines`]); the new auth version, if any. Failures are
/// logged: rows of a dropped routine grant nothing.
pub async fn forget_dropped(
    ts: &TenantSession,
    ids: &[(sprig_common::ids::DbId, sprig_common::ids::ObjectId)],
) -> Option<u64> {
    let mut version = None;
    for (db, object) in ids {
        match sprig_auth::authz::account_sql::forget_object(
            &ts.services().catalog,
            ts.tenant,
            *db,
            *object,
        )
        .await
        {
            Ok(v) => version = v.or(version),
            Err(e) => {
                tracing::warn!(tenant = %ts.tenant, %object, error = %e, "privileges of a dropped routine not removed");
            }
        }
    }
    version
}

/// Which databases a session may see in `SHOW DATABASES` (all its forms)
/// and `information_schema`: those it holds any privilege in, or all with
/// the tenant-scoped `SHOW DATABASES` privilege.
#[derive(Debug, Clone)]
pub struct DbVisibility(Option<(Arc<CompiledPrivileges>, bool)>);

impl DbVisibility {
    /// The session's, at its installed snapshot.
    #[must_use]
    pub fn of(ts: &TenantSession) -> Self {
        let Some(cat) = ts.catalog().cloned() else {
            return Self(None);
        };
        let p = privileges(ts, &cat);
        if p.everything || p.tenant.has(Action::ShowDatabases) {
            return Self(None);
        }
        Self(Some((p, ts.lower_case_table_names())))
    }

    /// Whether `db` is listed.
    #[must_use]
    pub fn allows(&self, db: &sprig_sql::names::LogicalDb) -> bool {
        match &self.0 {
            None => true,
            Some((p, lctn)) => p.any_in_database(&database_key(&db.name, *lctn), Some(db.id)),
        }
    }
}

/// [`filter_databases`] for a session.
pub fn filter_session_databases(ts: &TenantSession, ans: &mut LocalAnswer) {
    let Some(cat) = ts.catalog().cloned() else {
        return;
    };
    let p = privileges(ts, &cat);
    let lctn = ts.lower_case_table_names();
    filter_databases(ans, &cat, &p, lctn);
}

/// Chapter 29's settings privileges (build step 4):
///
/// | Operation | Needs | Refused with |
/// |---|---|---|
/// | values on a table | `ALTER` on it | 1142 |
/// | values on a database | `ALTER` on it | 1044 |
/// | tenant values, any bounds | `SPRIG_SETTINGS_ADMIN` on `*.*` | 1227 |
/// | a descendant tenant | `SPRIG_TENANT_ADMIN` | 1227 |
/// | `SHOW SPRIG SETTINGS FOR` a table or database | any privilege on it | 1142, 1044 |
///
/// `db` and `table` are the logical names of a database or table target.
///
/// # Errors
/// As the table says.
#[allow(clippy::too_many_arguments)] // one flag per kind of statement
pub fn settings_check(
    ts: &TenantSession,
    target: sprig_catalog::Target,
    descendant: bool,
    bounds: bool,
    show: bool,
    db: Option<&str>,
    table: Option<&str>,
    persona: Persona,
) -> Result<()> {
    let Some(cat) = ts.catalog().cloned() else {
        return Ok(());
    };
    let p = privileges(ts, &cat);
    if p.everything || p.tenant_admin() {
        return Ok(());
    }
    if descendant {
        return Err(account_sql::need_privilege("SPRIG_TENANT_ADMIN"));
    }
    let lctn = ts.lower_case_table_names();
    let admin = p.tenant.has(Action::SettingsAdmin);
    match target {
        sprig_catalog::Target::Tenant => {
            if show || admin {
                Ok(())
            } else {
                Err(account_sql::need_privilege("SPRIG_SETTINGS_ADMIN"))
            }
        }
        _ if bounds && !show => {
            if admin {
                Ok(())
            } else {
                Err(account_sql::need_privilege("SPRIG_SETTINGS_ADMIN"))
            }
        }
        sprig_catalog::Target::Database(id) => {
            let name = db.unwrap_or_default();
            let key = database_key(name, lctn);
            let ok = if show {
                p.any_in_database(&key, Some(id))
            } else {
                p.allows(Action::Alter, Target::Database { db_key: &key })
            };
            if ok {
                Ok(())
            } else {
                Err(db_denied(&ts.principal.user, &ts.principal.host, name))
            }
        }
        sprig_catalog::Target::Object(o) => {
            let name = db.unwrap_or_default();
            let key = database_key(name, lctn);
            let dbid = cat
                .objects
                .get(o)
                .map_or(sprig_common::ids::DbId(0), |r| r.db_id);
            let ok = if show {
                p.any_on_object(&key, dbid, o)
            } else {
                p.allows(
                    Action::Alter,
                    Target::Object {
                        db_key: &key,
                        db: dbid,
                        object: o,
                    },
                )
            };
            if ok {
                Ok(())
            } else {
                Err(table_denied(
                    persona,
                    if show { "SELECT" } else { "ALTER" },
                    &ts.principal.user,
                    &ts.principal.client_ip.to_string(),
                    name,
                    table.unwrap_or_default(),
                ))
            }
        }
    }
}

/// Whether `information_schema.SPRIG_SETTINGS` shows database `db` and
/// object `object` rows to the session (any privilege on them).
#[must_use]
pub fn settings_visible(
    ts: &TenantSession,
    cat: &TenantCatalog,
    db: &str,
    db_id: sprig_common::ids::DbId,
    object: Option<sprig_common::ids::ObjectId>,
) -> bool {
    let p = privileges(ts, cat);
    let key = database_key(db, ts.lower_case_table_names());
    match object {
        None => p.any_in_database(&key, Some(db_id)),
        Some(o) => p.any_on_object(&key, db_id, o),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_targets() {
        let lex = LexOptions::for_persona(Persona::MariaDb);
        assert_eq!(
            call_target(b"CALL shop.p_count(@n)", lex),
            Some((Some("shop".into()), "p_count".into()))
        );
        assert_eq!(call_target(b"call `p` ()", lex), Some((None, "p".into())));
        assert_eq!(call_target(b"SELECT 1", lex), None);
    }
}
