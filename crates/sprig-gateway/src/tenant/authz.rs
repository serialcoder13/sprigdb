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
//! **Engines.** Every decision goes through the tenant's authorization
//! engine (`auth.authorizer`, [`sprig_auth::authz::Authorizer`]): a
//! session picks it up when it first compiles and keeps it. An engine this
//! build doesn't know allows nothing.
//!
//! **Dropped objects.** A `DROP` of tables, views, routines or a database
//! hands what it dropped, with `auth.grants_on_drop` resolved at each
//! object's database, to the engine ([`objects_dropped`]), then waits for
//! the change like an account statement does.
//!
//! **Renamed tables.** Before a `RENAME TABLE` or `ALTER TABLE ... RENAME`
//! runs, the tables it moves go to the engine ([`objects_renamed`]): their
//! grants stay with their old names, as MySQL's do. After it, MariaDB's
//! grants on the session's cluster follow the new snapshot before the
//! client gets its `OK` ([`after_rename`]).
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
use sprig_auth::authz::authorizer::{
    self as engines, Dropped, Renamed, SettingsDenied, SettingsOp, Subject, row_filter_unsupported,
};
use sprig_auth::authz::backend::{self, BackendKey};
use sprig_auth::authz::definers;
use sprig_auth::authz::settings::{
    ACTIVATE_ALL_ROLES_ON_LOGIN, AUTHORIZER, GRANTS_ON_DROP, UNSAFE_PRIVILEGE_ACTION,
};
use sprig_auth::authz::stmt::AccountStmt;
use sprig_auth::authz::{
    Action, ActionSet, Authorizer, AuthzState, CompiledPrivileges, ObjectType, SessionGrants,
    Target,
};
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::{DbState, Namespace, ObjectKind, ObjectState};
use sprig_catalog::{Freshness, TenantCatalog};
use sprig_common::ids::{ClusterId, DbId, RoleId, TenantId};
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

/// What a reconcile was checked at: catalog version, auth version, and
/// the shard placement's digest (`exec::router::placement_digest`).
type Checked = (u64, u64, u64);

/// A compiled set and what it was compiled at: auth and catalog
/// versions, active roles.
type Compiled = (u64, u64, Vec<RoleId>, Arc<dyn SessionGrants>);

/// The engine `auth.authorizer` names for `cat`'s tenant; `None` for a
/// name this build doesn't know (logged; such a tenant gets nothing).
fn engine_for(svc: &TenantServices, cat: &TenantCatalog) -> Option<Arc<dyn Authorizer>> {
    let name = svc
        .settings
        .get(&AUTHORIZER, &cat.setting_ctx(&svc.dc), Some(&cat.settings));
    let e = engines::by_name(&name);
    if e.is_none() {
        tracing::error!(tenant = %cat.tenant, authorizer = %name, "unknown authorization engine: the tenant's sessions are refused everything");
    }
    e
}

/// The process-wide half: per-user account settings and what was applied.
#[derive(Default)]
pub struct AuthzServices {
    per_user: ArcSwapOption<PerUser>,
    applied: Mutex<HashMap<(TenantId, ClusterId), u64>>,
    /// Per tenant, cluster and backend account: a hash of the account's
    /// database-level grants this gateway last applied (bugfix-64).
    db_grants: Mutex<HashMap<(TenantId, ClusterId), HashMap<String, u64>>>,
    /// The snapshot versions and shard placement last compared, per
    /// tenant and cluster, and when.
    checked: Mutex<HashMap<(TenantId, ClusterId), (Checked, std::time::Instant)>>,
    /// Per tenant: what its clusters were last reconciled at (bugfix-25).
    placed: Mutex<HashMap<TenantId, Checked>>,
    gates: Mutex<HashMap<(TenantId, ClusterId), Gate>>,
    migrating: Mutex<HashSet<TenantId>>,
    /// Tenants being deleted or deleted (decision bugfix-32): their
    /// backend accounts are never made again here.
    gone: Mutex<HashSet<TenantId>>,
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

    /// `tenant` is being deleted (or is gone): from now on [`Self::ensure`]
    /// refuses it, so no session that still runs a statement, and no new
    /// login, makes its backend accounts again after the tenant's cleanup
    /// dropped them (decision bugfix-32).
    pub fn deleting(&self, tenant: TenantId) {
        lock(&self.gone).insert(tenant);
    }

    fn refuse_gone(&self, tenant: TenantId) -> Result<()> {
        if lock(&self.gone).contains(&tenant) {
            return Err(SprigError::new(
                sprig_common::SprigCode::TenantNotReady,
                format!("tenant {tenant} is being deleted"),
            ));
        }
        Ok(())
    }

    /// Forgets what was reconciled for `tenant` (it was deleted).
    pub fn forget(&self, tenant: TenantId) {
        lock(&self.applied).retain(|(t, _), _| *t != tenant);
        lock(&self.checked).retain(|(t, _), _| *t != tenant);
        lock(&self.gates).retain(|(t, _), _| *t != tenant);
        lock(&self.placed).remove(&tenant);
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
    /// Table, column and routine grants name every schema holding the
    /// object on the cluster (bugfix-25), so the shard placement is part of
    /// what is compared: when a split, merge, move, re-key or regroup
    /// changes it (or the snapshot's catalog or auth version moves), the
    /// next statement of any session of the tenant here reconciles this
    /// cluster and every other cluster of the tenant this gateway
    /// reconciled or the layout names. Every
    /// `auth.reconcile_interval_seconds` a cluster is compared with
    /// MariaDB's grants again even when nothing changed (the drift sweep).
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
        if self.per_user.load().is_none() {
            return Ok(());
        }
        self.refuse_gone(cat.tenant)?;
        let placement = crate::exec::router::placement_digest(svc, cat);
        // Chapter 29 (RL30): a changed `rls.backend_enforcement` (a global
        // value changes no catalog version) narrows or widens grants now.
        let digest =
            placement.as_ref().map_or(0, |p| p.0) ^ super::rls::enforcement_digest(svc, cat);
        let versions = (cat.version, cat.auth_version, digest);
        let every = Duration::from_secs(svc.settings.get(
            &sprig_auth::authz::settings::RECONCILE_INTERVAL_SECONDS,
            &cat.setting_ctx(&svc.dc),
            Some(&cat.settings),
        ));
        let key = (cat.tenant, cluster.clone());
        let sweep = match lock(&self.checked).get(&key) {
            Some((v, at)) if *v == versions => {
                if at.elapsed() < every {
                    return Ok(());
                }
                true
            }
            _ => false,
        };
        self.ensure_one(svc, cat, cluster, versions, sweep).await?;
        // The placement, the grants or the catalog moved: the tenant's
        // other clusters follow now, so a session already using one finds
        // the new shards' grants there (and a REVOKE reaches them).
        let moved = lock(&self.placed).insert(cat.tenant, versions) != Some(versions);
        if moved {
            let mut others: Vec<ClusterId> = lock(&self.applied)
                .keys()
                .filter(|(t, c)| *t == cat.tenant && c != cluster)
                .map(|(_, c)| c.clone())
                .collect();
            for c in placement.map(|p| p.1).unwrap_or_default() {
                if &c != cluster && !others.contains(&c) {
                    others.push(c);
                }
            }
            for c in others {
                if let Err(e) = self.ensure_one(svc, cat, &c, versions, false).await {
                    tracing::warn!(tenant = %cat.tenant, cluster = %c, error = %e, "backend grants after a placement change not reconciled yet");
                }
            }
        }
        Ok(())
    }

    /// One cluster of [`Self::ensure`]; `sweep` compares with MariaDB even
    /// when the wanted grants didn't change.
    #[allow(clippy::too_many_lines)] // derive, place, narrow, apply, definers
    async fn ensure_one(
        &self,
        svc: &TenantServices,
        cat: &TenantCatalog,
        cluster: &ClusterId,
        versions: Checked,
        sweep: bool,
    ) -> Result<()> {
        let Some(per_user) = self.per_user.load_full() else {
            return Ok(());
        };
        let key = (cat.tenant, cluster.clone());
        let now = std::time::Instant::now;
        let Some(auth) = TenantAuth::of(cat) else {
            return Ok(());
        };
        // Chapter 07 step 12: the name pairs stored code looks up.
        super::stored_names::sync_logged(svc, cat, cluster).await;
        let Some(engine) = engine_for(svc, cat) else {
            return Ok(());
        };
        let mut want = engine.backend_grants(cat, auth);
        // bugfix-25: table, column and routine grants on every schema of
        // this cluster that holds the object.
        let names = backend::object_names(&want);
        if !names.is_empty() {
            match placement_on(svc, cat.tenant, cluster, &names).await {
                Ok(here) => want = engine.backend_grants_on(cat, auth, &here),
                Err(e) => {
                    tracing::warn!(tenant = %cat.tenant, %cluster, error = %e, "where the tenant's tables are wasn't read; table grants name the default schemas");
                }
            }
        }
        // Chapter 29 (RLS round 2, RL18): accounts bound by a table's
        // policies read it only through its policy view.
        if let Err(e) = super::rls::narrow_backend(svc, cat, cluster, &mut want).await {
            tracing::warn!(tenant = %cat.tenant, %cluster, error = %e, "row-level security grants not narrowed; MariaDB doesn't filter bound users' reads until the next reconcile");
        }
        let hash = {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            want.hash(&mut h);
            h.finish()
        };
        if !sweep && lock(&self.applied).get(&key) == Some(&hash) {
            lock(&self.checked).insert(key, (versions, now()));
            return Ok(());
        }
        let gate = Arc::clone(lock(&self.gates).entry(key.clone()).or_default());
        let _g = gate.lock().await;
        let full = match lock(&self.applied).get(&key) {
            Some(h) if *h == hash && !sweep => {
                lock(&self.checked).insert(key, (versions, now()));
                return Ok(());
            }
            Some(_) => false,
            None => true,
        };
        // A deletion that began while this reconcile waited.
        self.refuse_gone(cat.tenant)?;
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
            let state = AuthzState::of(cat).cloned().unwrap_or_default();
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
        // bugfix-64: an account whose database-level grants changed here is
        // retired even when another gateway's reconcile already applied
        // them (`rep.changed` is then empty), so this gateway's sessions
        // pinned to its connections send `USE` again.
        for acct in db_grants_moved(&mut lock(&self.db_grants), &key, &want) {
            if !rep.changed.contains(&acct) {
                svc.locator.retire_account(&acct);
            }
        }
        metrics::histogram!("sprig_auth_reconcile_duration_seconds", "cluster" => cluster.to_string())
            .record(started.elapsed().as_secs_f64());
        if rep.statements > 0 {
            tracing::info!(tenant = %cat.tenant, %cluster, statements = rep.statements, created = rep.created, dropped = rep.dropped, changed = rep.changed.len(), "backend grants reconciled");
        }
        if sweep && rep.statements > 0 {
            metrics::counter!("sprig_auth_drift_corrections_total", "cluster" => cluster.to_string())
                .increment(rep.statements as u64);
        }
        lock(&self.applied).insert(key.clone(), hash);
        lock(&self.checked).insert(key, (versions, now()));
        Ok(())
    }
}

/// The accounts of `want` whose database-level grants (`mysql.db`)
/// differ from what `seen` last recorded for `key`, and records `want`'s
/// (bugfix-64). The first call for a key records only.
fn db_grants_moved(
    seen: &mut HashMap<(TenantId, ClusterId), HashMap<String, u64>>,
    key: &(TenantId, ClusterId),
    want: &sprig_auth::authz::backend::BackendState,
) -> Vec<String> {
    let now: HashMap<String, u64> = want
        .iter()
        .map(|(acct, grants)| {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            for (t, set) in grants {
                if matches!(t, sprig_auth::authz::backend::BackendTarget::Db(_)) {
                    (t, set).hash(&mut h);
                }
            }
            (acct.clone(), h.finish())
        })
        .collect();
    let moved = match seen.get(key) {
        Some(before) => now
            .iter()
            .filter(|(a, h)| before.get(*a).is_some_and(|b| b != *h))
            .map(|(a, _)| a.clone())
            .collect(),
        None => Vec::new(),
    };
    seen.insert(key.clone(), now);
    moved
}

/// Where `tenant`'s tables, views and routines named in `names` are on
/// `cluster`, and its schemas there (bugfix-25), read as `sprig_ctl` from
/// `information_schema`: what MariaDB's table, column and routine grants
/// must name.
///
/// # Errors
/// The cluster's.
async fn placement_on(
    svc: &TenantServices,
    tenant: TenantId,
    cluster: &ClusterId,
    names: &std::collections::BTreeSet<String>,
) -> Result<backend::Placement> {
    use mysql_async::prelude::Queryable;
    let pool = svc.locator.ctl_pool(cluster).await?;
    let mut conn = pool.get_conn().await.map_err(|e| {
        SprigError::new(
            sprig_common::SprigCode::BackendUnavailable,
            format!("sprig_ctl on {cluster}: {e}"),
        )
    })?;
    let like = format!("t{tenant}\\_d%");
    let unavailable = |e: String| {
        SprigError::new(
            sprig_common::SprigCode::BackendUnavailable,
            format!("{cluster}: {e}"),
        )
    };
    let marks = vec!["?"; names.len()].join(", ");
    let mut params: Vec<mysql_async::Value> = vec![like.clone().into()];
    params.extend(names.iter().map(|n| mysql_async::Value::from(n.as_str())));
    let mut rows: Vec<(String, String, String)> = conn
        .exec(
            format!(
                "SELECT ROUTINE_TYPE, ROUTINE_SCHEMA, ROUTINE_NAME \
                 FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA LIKE ? \
                 AND ROUTINE_NAME IN ({marks})"
            ),
            params,
        )
        .await
        .map_err(|e| unavailable(e.to_string()))?;
    // R9 (r9scale): the schemas from a directory listing, then the tables
    // one schema at a time; a LIKE on TABLE_SCHEMA lists every table of
    // every tenant on the server (`sprig_catalog::infoschema`).
    let schemas = sprig_catalog::infoschema::schemas_like(&mut conn, &like).await?;
    let in_names = names
        .iter()
        .map(|n| sprig_catalog::infoschema::literal(n))
        .collect::<Vec<_>>()
        .join(", ");
    rows.extend(
        sprig_catalog::infoschema::read_per_schema::<(String, String, String)>(
            &mut conn,
            "SELECT 'T', TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES",
            "TABLE_SCHEMA",
            &schemas,
            &format!(" AND TABLE_NAME IN ({in_names})"),
        )
        .await?,
    );
    rows.extend(schemas.into_iter().map(|s| ("S".to_owned(), s, String::new())));
    let mut here = backend::Placement::default();
    for (kind, schema, name) in rows {
        match kind.as_str() {
            "T" => here.add_relation(&schema, &name),
            "S" => here.add_schema(&schema),
            k => here.add_routine(&schema, &name, k),
        }
    }
    Ok(here)
}

/// One session's authorization state.
#[derive(Debug, Default)]
pub struct SessionAuthz {
    /// The active roles; `None` until the first compile (then the default
    /// roles, or all with `auth.activate_all_roles_on_login`).
    active: Mutex<Option<Vec<RoleId>>>,
    compiled: Mutex<Option<Compiled>>,
    /// The engine, from the first compile on (`None` before, and for an
    /// unknown `auth.authorizer`).
    engine: std::sync::OnceLock<Option<Arc<dyn Authorizer>>>,
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

/// `CURRENT_ROLE()` for the session (chapter 29, R4): the active roles,
/// as MySQL 8.4 prints them (`` `r1`@`%`,`r2`@`%` ``, or `NONE`) or as
/// MariaDB does (the role's name, or `NULL`: `None`). The backend's own
/// answer would be the per-user account's, which never has a role.
#[must_use]
pub fn current_role(ts: &TenantSession, persona: Persona) -> Option<String> {
    let cat = ts.catalog()?;
    let _ = privileges(ts, cat); // the login's default roles, once
    let state = AuthzState::of(cat)?;
    let mut names: Vec<(String, String)> = ts
        .authz
        .active()
        .into_iter()
        .filter_map(|r| {
            state
                .role(r)
                .map(|row| (row.name.clone(), row.host.clone()))
        })
        .collect();
    names.sort();
    let q = |s: &str| format!("`{}`", s.replace('`', "``"));
    match persona {
        Persona::MySql84 if names.is_empty() => Some("NONE".to_owned()),
        Persona::MySql84 => Some(
            names
                .iter()
                .map(|(n, h)| format!("{}@{}", q(n), q(h)))
                .collect::<Vec<_>>()
                .join(","),
        ),
        Persona::MariaDb => names.first().map(|(n, _)| n.clone()),
    }
}

/// The session's engine (read from `auth.authorizer` once).
fn session_engine(ts: &TenantSession, cat: &TenantCatalog) -> Option<Arc<dyn Authorizer>> {
    ts.authz
        .engine
        .get_or_init(|| engine_for(ts.services(), cat))
        .clone()
}

/// Nothing at all: the rights of a session whose engine is unknown.
fn nothing() -> Arc<dyn SessionGrants> {
    Arc::new(CompiledPrivileges::default())
}

/// The session's privileges at `cat` (compiled once per snapshot and
/// active-role set). Active roles no longer granted drop out.
#[must_use]
pub fn privileges(ts: &TenantSession, cat: &TenantCatalog) -> Arc<dyn SessionGrants> {
    let Some(user) = ts.principal.user_id else {
        return Arc::new(CompiledPrivileges::everything());
    };
    let Some(engine) = session_engine(ts, cat) else {
        return nothing();
    };
    let active = {
        let mut a = lock(&ts.authz.active);
        let all = || {
            ts.services().settings.get(
                &ACTIVATE_ALL_ROLES_ON_LOGIN,
                &cat.setting_ctx(&ts.services().dc),
                Some(&cat.settings),
            )
        };
        if a.is_none() {
            *a = Some(engine.active_roles(cat, user, None, all()));
        }
        engine.active_roles(cat, user, a.as_deref(), false)
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
    let p = engine.session(
        cat,
        &Subject {
            user,
            roles: &active,
        },
    );
    *g = Some((cat.auth_version, cat.version, active, Arc::clone(&p)));
    p
}

/// The privileges of `user` at `cat` with the roles active at login, for
/// front ends without a SQL session (the Kafka listener, chapter 45): every
/// right for a principal without a user id (operators), none when the
/// engine is unknown. Not cached; callers cache by `(cat.version,
/// cat.auth_version)`.
#[must_use]
pub fn grants_of(
    svc: &TenantServices,
    cat: &TenantCatalog,
    user: Option<sprig_common::ids::UserId>,
) -> Arc<dyn SessionGrants> {
    let Some(user) = user else {
        return Arc::new(CompiledPrivileges::everything());
    };
    let Some(engine) = engine_for(svc, cat) else {
        return nothing();
    };
    let all = svc.settings.get(
        &ACTIVATE_ALL_ROLES_ON_LOGIN,
        &cat.setting_ctx(&svc.dc),
        Some(&cat.settings),
    );
    let roles = engine.active_roles(cat, user, None, all);
    engine.session(
        cat,
        &Subject {
            user,
            roles: &roles,
        },
    )
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
#[allow(clippy::too_many_lines)] // the context, the statement, the wait
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
    let engine =
        session_engine(ts, cat).ok_or_else(|| account_sql::need_privilege("CREATE USER"))?;
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
        // Chapter 14: every password also gets SCRAM verifiers.
        scram_sha256_iterations: u32::try_from(svc.settings.get(
            &sprig_auth::settings::SCRAM_SHA256_ITERATIONS,
            &ctx,
            Some(&cat.settings),
        ))
        .unwrap_or(sprig_auth::scram::DEFAULT_SCRAM_ITERATIONS),
        scram_sha512_iterations: u32::try_from(svc.settings.get(
            &sprig_auth::settings::SCRAM_SHA512_ITERATIONS,
            &ctx,
            Some(&cat.settings),
        ))
        .unwrap_or(sprig_auth::scram::DEFAULT_SCRAM_ITERATIONS),
    };
    let unsafe_error = svc
        .settings
        .get(&UNSAFE_PRIVILEGE_ACTION, &ctx, Some(&cat.settings))
        == "error";
    let partial_revokes = svc.settings.get(
        &sprig_auth::authz::settings::PARTIAL_REVOKES,
        &ctx,
        Some(&cat.settings),
    );
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
            privileges: &*compiled,
            active_roles: &active,
            hashing,
            unsafe_error,
            partial_revokes,
            clock: &*svc.clock,
        };
        engine.account_statement(&actx, stmt).await?
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
    pub p: &'a dyn SessionGrants,
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
    /// Chapter 29: protected tables the statement's row-level security
    /// rewrite covered (their policies are in the text, or MariaDB's
    /// triggers check its writes).
    pub covered: &'a [sprig_common::ids::ObjectId],
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
        // Chapter 30's retryable-write records (`decisions/r5-interim.md`
        // D12): the document engine writes them in the write's own
        // transaction for any user of the database. Only the engine's
        // internal statements can name a `__sprig_` table (chapter 07),
        // and the writes they record are checked on their collections.
        if name.eq_ignore_ascii_case(sprig_auth::authz::backend::DOC_SESSIONS_TABLE)
            && self.p.sees_database(&key, Some(row.db_id))
        {
            return Ok(());
        }
        let okey = object_key(ObjectKind::Table, name, self.lctn);
        let target = self.relation(&key, row.db_id, &okey);
        if let Some(a) = any.iter().find(|a| self.p.allows(*a, target)) {
            // The row-level hook: no engine filters rows yet, and a
            // filter this gateway can't inject must not be skipped.
            if self.p.row_filter(target, a).is_some()
                && !matches!(target, Target::Object { object, .. } if self.covered.contains(&object))
            {
                return Err(row_filter_unsupported(name));
            }
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

    /// Table or view `name_key` of database `db`: the live object, else
    /// its name (grants held by name count).
    fn relation<'k>(&self, db_key: &'k str, db: DbId, name_key: &'k str) -> Target<'k> {
        match self
            .cat
            .object(db, Namespace::Relation, name_key)
            .filter(|o| o.state == ObjectState::Active)
        {
            Some(o) => Target::Object {
                db_key,
                db,
                object: o.object_id,
            },
            None => Target::Named { db_key, name_key },
        }
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
                    // The records table (see `table`), created on a
                    // database's first retryable write.
                    if o.eq_ignore_ascii_case(sprig_auth::authz::backend::DOC_SESSIONS_TABLE)
                        && self
                            .p
                            .sees_database(&key, self.cat.database(&key).map(|r| r.db_id))
                    {
                        return Ok(());
                    }
                    // MySQL accepts a table-level grant for the table
                    // being created (CREATE on `db.t`), including one
                    // kept from a dropped table of that name.
                    let okey = object_key(ObjectKind::Table, o, self.lctn);
                    if let Some(row) = self
                        .cat
                        .database(&key)
                        .filter(|r| r.state == DbState::Active)
                        && self
                            .p
                            .allows(*action, self.relation(&key, row.db_id, &okey))
                    {
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
    if cx.p.unrestricted() {
        return Ok(());
    }
    check_text(cx, sp.info.kind, original, *lex)?;
    if sp.info.kind == StmtKind::Use
        && let Some(UseTarget::Database { logical, .. }) = &sp.info.use_target
    {
        let key = database_key(logical, cx.lctn);
        let id = cx.cat.database(&key).map(|d| d.db_id);
        if !cx.p.sees_database(&key, id) {
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
    if cx.p.unrestricted() {
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
    compiled: Arc<dyn SessionGrants>,
    /// Chapter 29: the tables the prepare-time rewrite covered.
    covered: Vec<sprig_common::ids::ObjectId>,
    /// Chapter 29: the row-level security context the text's predicates
    /// were bound with, when it covers protected tables.
    rls_context: Option<String>,
}

impl Recheck {
    /// The default database the statement was prepared in.
    #[must_use]
    pub fn current(&self) -> Option<&str> {
        self.current.as_deref()
    }

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
            rls_context: rls_context(ts, &cat),
            compiled: privileges(ts, &cat),
            covered: ts.rls.covered.clone(),
        })
    }
}

/// The session's row-level security context when its last rewrite
/// covered protected tables (what a prepared text's predicates depend on).
fn rls_context(ts: &TenantSession, cat: &TenantCatalog) -> Option<String> {
    if ts.rls.covered.is_empty() {
        return None;
    }
    let rls = privileges(ts, cat).row_policies()?;
    Some(super::rls::SessionValues::of(ts, cat).context(&rls))
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
    // Chapter 29: the text's policy predicates were bound at prepare; a
    // new role set or policy change needs a new text (MySQL's 1615, which
    // connectors answer by preparing again).
    if rc.rls_context.is_some() && rls_context(ts, &cat) != rc.rls_context {
        return Err(SprigError::standard(
            1615,
            *b"HY000",
            "Prepared statement needs to be re-prepared".to_owned(),
        ));
    }
    let client_host = ts.principal.client_ip.to_string();
    let lctn = ts.lower_case_table_names();
    let temp = |db: Option<&str>, name: &str| ts.is_temp(db, name);
    let cx = CheckCtx {
        cat: &cat,
        p: &*p,
        persona,
        user: &ts.principal.user,
        host: &ts.principal.host,
        client_host: &client_host,
        current: rc.current.as_deref(),
        lctn,
        temp: &temp,
        covered: &rc.covered,
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
    p: &dyn SessionGrants,
    lctn: bool,
) {
    if p.sees_all_databases() {
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
        p.sees_database(&key, id)
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
        p: &*p,
        persona,
        user: &ts.principal.user,
        host: &ts.principal.host,
        client_host: &client_host,
        current,
        lctn,
        temp: &temp,
        covered: &ts.rls.covered,
    };
    let r = check(&cx, sp, original, lex);
    if r.is_err() {
        metrics::counter!("sprig_auth_checks_total", "protocol" => "mysql", "result" => "deny")
            .increment(1);
    }
    r
}

/// What a `DROP` statement removes, with `auth.grants_on_drop` resolved
/// at each object's database (chapter 29, "Privileges of dropped
/// objects"), read from the session's snapshot before the statement runs.
/// Objects that don't exist (`IF EXISTS`) and temporary tables aren't
/// listed.
#[must_use]
pub fn dropped_objects(
    ts: &TenantSession,
    stmt: &sprig_catalog::ddl::DdlStatement,
    current: Option<&str>,
) -> Vec<Dropped> {
    use sprig_catalog::ddl::DdlStatement;
    let Some(cat) = ts.catalog().cloned() else {
        return Vec::new();
    };
    let svc = ts.services();
    let lctn = ts.lower_case_table_names();
    let keep = |db: sprig_common::ids::DbId| {
        svc.settings.get(
            &GRANTS_ON_DROP,
            &cat.setting_ctx(&svc.dc).with_db(db),
            Some(&cat.settings),
        ) != "drop"
    };
    let live_db = |name: &str| {
        cat.database(&database_key(name, lctn))
            .filter(|d| d.state == DbState::Active)
            .cloned()
    };
    match stmt {
        DdlStatement::DropDatabase { name, .. } => live_db(name)
            .map(|d| Dropped::Database {
                db: d.db_id,
                db_key: d.name_key.clone(),
                keep: keep(d.db_id),
            })
            .into_iter()
            .collect(),
        DdlStatement::DropObjects { kind, names, .. } => names
            .iter()
            .filter(|n| !ts.is_temp(n.db.as_deref().or(current), &n.name))
            .filter_map(|n| {
                let d = live_db(n.db.as_deref().or(current)?)?;
                let o = cat
                    .object(d.db_id, kind.namespace(), &object_key(*kind, &n.name, lctn))
                    .filter(|o| o.state == ObjectState::Active)?;
                Some(Dropped::Object {
                    db: d.db_id,
                    db_key: d.name_key.clone(),
                    object: o.object_id,
                    name_key: o.name_key.clone(),
                    namespace: o.namespace,
                    keep: keep(d.db_id),
                })
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Hands what a successful `DROP` removed to the tenant's engine (see
/// [`dropped_objects`]); if it changed grants, waits until this gateway's
/// snapshot has the change and MariaDB's grants follow, so the session's
/// next statement sees it. Failures are logged: the objects are gone.
pub async fn objects_dropped(ts: &TenantSession, dropped: &[Dropped]) {
    if dropped.is_empty() {
        return;
    }
    let Some(cat) = ts.catalog().cloned() else {
        return;
    };
    let Some(engine) = session_engine(ts, &cat) else {
        return;
    };
    let svc = Arc::clone(ts.services());
    match engine.objects_dropped(&svc.catalog, &cat, dropped).await {
        Ok(Some(v)) => {
            if let Some(c) = await_auth(&svc, ts.tenant, v).await {
                let cluster = ts.principal.backend.cluster.clone();
                if let Err(e) = svc.authz.ensure(&svc, &c, &cluster).await {
                    tracing::warn!(tenant = %ts.tenant, error = %e, "backend grants after a DROP not applied yet");
                }
            }
        }
        Ok(None) => {}
        Err(e) => {
            tracing::warn!(tenant = %ts.tenant, error = %e, "grants of dropped objects not updated");
        }
    }
}

/// The live tables and views a `RENAME TABLE` or `ALTER TABLE ...
/// RENAME` moves away from their names, read from the session's snapshot
/// before the statement runs (chapter 29, "Renamed tables"): each once,
/// under the name it has now. The moves are followed in order, as MariaDB
/// applies them, so `RENAME TABLE a TO tmp, b TO a, tmp TO b` lists `a`
/// and `b`. Sources that don't exist and temporary tables aren't listed.
#[must_use]
pub fn renamed_objects(
    ts: &TenantSession,
    stmt: &sprig_catalog::ddl::DdlStatement,
    current: Option<&str>,
) -> Vec<Renamed> {
    use sprig_catalog::ddl::{DdlStatement, ObjectName};
    let Some(cat) = ts.catalog().cloned() else {
        return Vec::new();
    };
    let lctn = ts.lower_case_table_names();
    // RENAME TABLE resolves an unqualified new name in the current
    // database; ALTER TABLE ... RENAME in the table's own.
    let (pairs, alter): (Vec<(&ObjectName, &ObjectName)>, bool) = match stmt {
        DdlStatement::RenameTables { pairs, .. } => {
            (pairs.iter().map(|(a, b)| (a, b)).collect(), false)
        }
        DdlStatement::AlterObject {
            kind,
            name,
            rename_to: Some(to),
            ..
        } if kind.namespace() == Namespace::Relation => (vec![(name, to)], true),
        _ => return Vec::new(),
    };
    let live_db = |name: &str| {
        cat.database(&database_key(name, lctn))
            .filter(|d| d.state == DbState::Active)
            .cloned()
    };
    // Names the earlier moves took (Some(object)) or freed (None).
    let mut overlay: HashMap<(String, String), Option<sprig_common::ids::ObjectId>> =
        HashMap::new();
    let mut out: Vec<Renamed> = Vec::new();
    for (from, to) in pairs {
        let Some(fdb) = from.db.as_deref().or(current) else {
            continue;
        };
        let tdb_name = to.db.as_deref().or(if alter { Some(fdb) } else { current });
        let fkey = (
            database_key(fdb, lctn),
            object_key(ObjectKind::Table, &from.name, lctn),
        );
        let source = match overlay.get(&fkey) {
            Some(id) => *id,
            None if ts.is_temp(Some(fdb), &from.name) => None,
            None => {
                let found = live_db(fdb).and_then(|d| {
                    cat.object(d.db_id, Namespace::Relation, &fkey.1)
                        .filter(|o| o.state == ObjectState::Active)
                        .map(|o| (d, o.clone()))
                });
                found.map(|(d, o)| {
                    if !out.iter().any(|r| r.object == o.object_id) {
                        out.push(Renamed {
                            db: d.db_id,
                            db_key: d.name_key.clone(),
                            object: o.object_id,
                            name_key: o.name_key.clone(),
                        });
                    }
                    o.object_id
                })
            }
        };
        let Some(id) = source else {
            continue;
        };
        overlay.insert(fkey, None);
        if let Some(t) = tdb_name {
            overlay.insert(
                (
                    database_key(t, lctn),
                    object_key(ObjectKind::Table, &to.name, lctn),
                ),
                Some(id),
            );
        }
    }
    out
}

/// Hands the tables and views a rename is about to move (see
/// [`renamed_objects`]) to the tenant's engine before the statement runs:
/// with `mysql_grants` their grants become grants held by their current
/// names, which give the same rights until the rename and stay with the
/// names after it, as MySQL's do. Failures are logged and the rename goes
/// ahead (its grants then follow the table, the behaviour before).
pub async fn objects_renamed(ts: &TenantSession, renamed: &[Renamed]) {
    if renamed.is_empty() {
        return;
    }
    let Some(cat) = ts.catalog().cloned() else {
        return;
    };
    let Some(engine) = session_engine(ts, &cat) else {
        return;
    };
    let svc = Arc::clone(ts.services());
    match engine.objects_renamed(&svc.catalog, &cat, renamed).await {
        Ok(Some(v)) => {
            // This gateway's snapshot has it before the rename commits.
            let _ = await_auth(&svc, ts.tenant, v).await;
        }
        Ok(None) => {}
        Err(e) => {
            tracing::warn!(tenant = %ts.tenant, error = %e, "grants of renamed tables not moved to their names");
        }
    }
}

/// Whether `stmt` renames tables or views (`RENAME TABLE`, `ALTER TABLE
/// ... RENAME`).
#[must_use]
pub fn renames_relations(stmt: &sprig_catalog::ddl::DdlStatement) -> bool {
    use sprig_catalog::ddl::DdlStatement;
    match stmt {
        DdlStatement::RenameTables { .. } => true,
        DdlStatement::AlterObject {
            kind,
            rename_to: Some(_),
            ..
        } => kind.namespace() == Namespace::Relation,
        _ => false,
    }
}

/// After a rename committed at catalog `version`: brings MariaDB's grants
/// on the session's cluster to that snapshot before the client gets its
/// `OK` (grants held by the new names now apply to the renamed tables,
/// those of names left empty don't), as a `DROP` does. Without this, a
/// session whose snapshot still predates the rename could reach MariaDB
/// before any reconcile at the new snapshot. Failures are logged.
pub async fn after_rename(ts: &TenantSession, version: u64) {
    let svc = Arc::clone(ts.services());
    let cat = match svc
        .cache
        .get(
            ts.tenant,
            Freshness::Cached {
                min_version: version,
            },
        )
        .await
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(tenant = %ts.tenant, error = %e, "backend grants after a rename not applied yet");
            return;
        }
    };
    let cluster = ts.principal.backend.cluster.clone();
    if let Err(e) = svc.authz.ensure(&svc, &cat, &cluster).await {
        tracing::warn!(tenant = %ts.tenant, error = %e, "backend grants after a rename not applied yet");
    }
}

/// Which databases a session may see in `SHOW DATABASES` (all its forms)
/// and `information_schema`: those it holds any privilege in, or all with
/// the tenant-scoped `SHOW DATABASES` privilege.
#[derive(Debug, Clone)]
pub struct DbVisibility(Option<(Arc<dyn SessionGrants>, bool)>);

impl DbVisibility {
    /// The session's, at its installed snapshot.
    #[must_use]
    pub fn of(ts: &TenantSession) -> Self {
        let Some(cat) = ts.catalog().cloned() else {
            return Self(None);
        };
        let p = privileges(ts, &cat);
        if p.sees_all_databases() {
            return Self(None);
        }
        Self(Some((p, ts.lower_case_table_names())))
    }

    /// Whether `db` is listed.
    #[must_use]
    pub fn allows(&self, db: &sprig_sql::names::LogicalDb) -> bool {
        match &self.0 {
            None => true,
            Some((p, lctn)) => p.sees_database(&database_key(&db.name, *lctn), Some(db.id)),
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
    filter_databases(ans, &cat, &*p, lctn);
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
    let lctn = ts.lower_case_table_names();
    let name = db.unwrap_or_default();
    let key = database_key(name, lctn);
    let op = match target {
        _ if descendant => SettingsOp::Descendant,
        sprig_catalog::Target::Tenant => SettingsOp::Tenant { show },
        _ if bounds && !show => SettingsOp::Bounds,
        sprig_catalog::Target::Database(id) => SettingsOp::Database {
            db_key: &key,
            db: id,
            show,
        },
        sprig_catalog::Target::Object(o) => SettingsOp::Object {
            db_key: &key,
            db: cat
                .objects
                .get(o)
                .map_or(sprig_common::ids::DbId(0), |r| r.db_id),
            object: o,
            show,
        },
    };
    match p.settings(op) {
        Ok(()) => Ok(()),
        Err(SettingsDenied::TenantAdmin) => Err(account_sql::need_privilege("SPRIG_TENANT_ADMIN")),
        Err(SettingsDenied::SettingsAdmin) => {
            Err(account_sql::need_privilege("SPRIG_SETTINGS_ADMIN"))
        }
        Err(SettingsDenied::Target) => match op {
            SettingsOp::Object { .. } => Err(table_denied(
                persona,
                if show { "SELECT" } else { "ALTER" },
                &ts.principal.user,
                &ts.principal.client_ip.to_string(),
                name,
                table.unwrap_or_default(),
            )),
            _ => Err(db_denied(&ts.principal.user, &ts.principal.host, name)),
        },
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
    let op = match object {
        None => SettingsOp::Database {
            db_key: &key,
            db: db_id,
            show: true,
        },
        Some(o) => SettingsOp::Object {
            db_key: &key,
            db: db_id,
            object: o,
            show: true,
        },
    };
    p.settings(op).is_ok()
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

    #[test]
    fn a_tenant_being_deleted_gets_no_account_made_again() {
        let a = AuthzServices::default();
        assert!(a.refuse_gone(TenantId(3)).is_ok());
        a.deleting(TenantId(3));
        let e = a.refuse_gone(TenantId(3)).unwrap_err();
        assert_eq!(e.code(), Some(sprig_common::SprigCode::TenantNotReady));
        // Only that tenant: 30 isn't 3.
        assert!(a.refuse_gone(TenantId(30)).is_ok());
        a.forget(TenantId(3));
        assert!(a.refuse_gone(TenantId(3)).is_err());
    }
}
