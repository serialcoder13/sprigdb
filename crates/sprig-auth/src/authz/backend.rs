//! Backend accounts and grants per shard schema (chapter 29, "Backend
//! accounts and grants" and "Reconciler"; build step 5), the R0 form.
//!
//! Every live user of a tenant has its own MariaDB account
//! `t<tenant>_u<user>@'%'`, granted only on the tenant's own shard
//! schemas, so MariaDB enforces isolation and the user's grants a second
//! time, whatever the gateway does:
//!
//! | Sprig privilege | MariaDB grant |
//! |---|---|
//! | `*.*` | `` ON `t42\_d%`.* ``: every shard and reference schema of the tenant, now and later; never `t42_sprig` |
//! | `db.*` | `` ON `t42\_d1\_%`.* ``: every group, shard and reference schema of the database, now and later, with the `*.*` actions too (MariaDB uses the most specific matching database row only) |
//! | `db.t` | `` ON `t42_d1_g1_s1`.`t` `` per schema |
//! | `db.t (c)` | the column, per schema |
//! | `db.t` held by name (a dropped table's, `auth.grants_on_drop = keep`) | as `db.t` while a table of that name exists; before that only if it includes `CREATE` (MariaDB refuses others on a missing table), so the user can create it again |
//! | `db.f` (routine) | `` ON FUNCTION `t42_d1_g1_s1`.`f` `` per schema |
//!
//! Every database row also carries `CREATE TEMPORARY TABLES`, for the
//! gateway's own temporary tables (gathered and analytic reads); the
//! gateway checks the privilege itself for users' statements.
//!
//! Only data and object actions reach MariaDB (`SELECT` ... `TRIGGER`,
//! `EXECUTE`, `CREATE ROUTINE`, `EVENT`, `LOCK TABLES`, temporary tables);
//! management actions (`GRANT OPTION`, `CREATE USER`, settings, ...) stay
//! in the gateway. Object DDL runs on the session's own backend account in
//! R0, so `CREATE`, `ALTER`, `DROP` and `INDEX` are mirrored as well.
//!
//! **Roles (R0).** A user's account gets its own privileges plus those of
//! every role granted to it, active or not; the gateway enforces which
//! roles are active. MariaDB roles and combined roles (one active role per
//! MariaDB session) arrive with build step 7.
//!
//! **Credentials (R0).** Accounts have a password derived from a secret
//! only gateways hold, `HMAC-SHA256(secret, account)`, and `REQUIRE SSL`,
//! instead of the chapter's certificate subject: the dev and test clusters
//! don't verify client certificates. Nothing stores the passwords.
//!
//! ```text
//! reconcile(tenant T, cluster C, want = desired(snapshot)):
//!   have := mysql.global_priv, db, tables_priv, columns_priv, procs_priv
//!           for accounts LIKE 't<T>\_u%' at host '%'     (as sprig_ctl)
//!   CREATE USER IF NOT EXISTS for new accounts (ALTER USER ... IDENTIFIED
//!     BY on a full pass, so a new secret takes effect)
//!   per target: GRANT want - have; REVOKE have - want
//!   DROP USER for accounts of users that are gone
//! ```
//!
//! Every statement is idempotent or its "nothing to do" error is ignored,
//! so gateways may reconcile the same tenant at once.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};
use sha2::{Digest, Sha256};
use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::shard_schemas;
use sprig_catalog::store::{DbState, Namespace, ObjectKind, ObjectState};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::names::{AccountName, SchemaName};
use sprig_common::{Result, SprigError};
use zeroize::Zeroizing;

use super::privilege::{Action, ActionSet};
use super::state::AuthzState;
use super::store::{Grantee, PrivTarget};
use crate::cache::TenantAuth;

/// The backend account of `user` of `tenant` (user part; host `%`).
#[must_use]
pub fn account(tenant: TenantId, user: UserId) -> String {
    AccountName::User { tenant, user }.to_string()
}

/// The secret backend passwords derive from.
#[derive(Clone)]
pub struct BackendKey(Zeroizing<Vec<u8>>);

impl std::fmt::Debug for BackendKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BackendKey(..)")
    }
}

fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    const B: usize = 64;
    let mut k = [0u8; B];
    if key.len() > B {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; B];
    let mut opad = [0x5cu8; B];
    for i in 0..B {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let inner = Sha256::new()
        .chain_update(ipad)
        .chain_update(msg)
        .finalize();
    Sha256::new()
        .chain_update(opad)
        .chain_update(inner)
        .finalize()
        .into()
}

impl BackendKey {
    /// A key from secret bytes (at least 16).
    ///
    /// # Errors
    /// 39998 for a secret shorter than 16 bytes.
    pub fn new(secret: &[u8]) -> Result<Self> {
        if secret.len() < 16 {
            return Err(SprigError::new(
                sprig_common::SprigCode::ConfigInvalid,
                "the backend account secret must be at least 16 bytes",
            ));
        }
        Ok(Self(Zeroizing::new(secret.to_vec())))
    }

    /// The password of backend account `account`: 40 hex digits of
    /// `HMAC-SHA256(secret, account)`.
    #[must_use]
    pub fn password(&self, account: &str) -> Zeroizing<String> {
        let mac = hmac_sha256(&self.0, account.as_bytes());
        let mut s = Zeroizing::new(String::with_capacity(40));
        for b in &mac[..20] {
            let _ = write!(s, "{b:02x}");
        }
        s
    }
}

/// MariaDB privilege names of the actions that reach the backend.
const BACKEND: &[(Action, &str)] = &[
    (Action::Select, "SELECT"),
    (Action::Insert, "INSERT"),
    (Action::Update, "UPDATE"),
    (Action::Delete, "DELETE"),
    (Action::Create, "CREATE"),
    (Action::Drop, "DROP"),
    (Action::References, "REFERENCES"),
    (Action::Index, "INDEX"),
    (Action::Alter, "ALTER"),
    (Action::CreateTemporaryTables, "CREATE TEMPORARY TABLES"),
    (Action::LockTables, "LOCK TABLES"),
    (Action::Execute, "EXECUTE"),
    (Action::CreateView, "CREATE VIEW"),
    (Action::ShowView, "SHOW VIEW"),
    (Action::CreateRoutine, "CREATE ROUTINE"),
    (Action::AlterRoutine, "ALTER ROUTINE"),
    (Action::Event, "EVENT"),
    (Action::Trigger, "TRIGGER"),
];

/// What MariaDB accepts per level.
const DB_LEVEL: ActionSet = ActionSet::of(&[
    Action::Select,
    Action::Insert,
    Action::Update,
    Action::Delete,
    Action::Create,
    Action::Drop,
    Action::References,
    Action::Index,
    Action::Alter,
    Action::CreateTemporaryTables,
    Action::LockTables,
    Action::Execute,
    Action::CreateView,
    Action::ShowView,
    Action::CreateRoutine,
    Action::AlterRoutine,
    Action::Event,
    Action::Trigger,
]);
const TABLE_LEVEL: ActionSet = ActionSet::of(&[
    Action::Select,
    Action::Insert,
    Action::Update,
    Action::Delete,
    Action::Create,
    Action::Drop,
    Action::References,
    Action::Index,
    Action::Alter,
    Action::CreateView,
    Action::ShowView,
    Action::Trigger,
]);
const COLUMN_LEVEL: ActionSet = ActionSet::of(&[
    Action::Select,
    Action::Insert,
    Action::Update,
    Action::References,
]);
const ROUTINE_LEVEL: ActionSet = ActionSet::of(&[Action::Execute, Action::AlterRoutine]);

fn names(set: ActionSet) -> String {
    BACKEND
        .iter()
        .filter(|(a, _)| set.has(*a))
        .map(|(_, n)| *n)
        .collect::<Vec<_>>()
        .join(", ")
}

/// `_` and `%` escaped for a database-level grant.
fn escape_db(name: &str) -> String {
    name.replace('\\', "\\\\")
        .replace('_', "\\_")
        .replace('%', "\\%")
}

fn q(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// Where a backend grant sits.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum BackendTarget {
    /// `mysql.db`: a database pattern, as stored (escaped).
    Db(String),
    /// `mysql.tables_priv`.
    Table(String, String),
    /// `mysql.columns_priv`: schema, table, column (lower case).
    Column(String, String, String),
    /// `mysql.procs_priv`: schema, routine (lower case), `FUNCTION` or
    /// `PROCEDURE`.
    Routine(String, String, &'static str),
}

impl BackendTarget {
    fn on(&self) -> String {
        match self {
            BackendTarget::Db(p) => format!("{}.*", q(p)),
            BackendTarget::Table(s, t) | BackendTarget::Column(s, t, _) => {
                format!("{}.{}", q(s), q(t))
            }
            BackendTarget::Routine(s, r, kind) => format!("{kind} {}.{}", q(s), q(r)),
        }
    }

    fn privileges(&self, set: ActionSet) -> String {
        match self {
            BackendTarget::Column(_, _, c) => BACKEND
                .iter()
                .filter(|(a, _)| set.has(*a))
                .map(|(_, n)| format!("{n} ({})", q(c)))
                .collect::<Vec<_>>()
                .join(", "),
            _ => names(set),
        }
    }

    fn accepts(&self) -> ActionSet {
        match self {
            BackendTarget::Db(_) => DB_LEVEL,
            BackendTarget::Table(..) => TABLE_LEVEL,
            BackendTarget::Column(..) => COLUMN_LEVEL,
            BackendTarget::Routine(..) => ROUTINE_LEVEL,
        }
    }
}

/// Grants per backend account: what one side (wanted or found) holds.
pub type BackendState = BTreeMap<String, BTreeMap<BackendTarget, ActionSet>>;

/// The grants every live user's account should hold (see the module
/// docs). Accounts of users without any privilege are present, empty.
#[must_use]
#[allow(clippy::too_many_lines)] // one arm per privilege level
pub fn desired(cat: &TenantCatalog, state: &AuthzState, auth: &TenantAuth) -> BackendState {
    let tenant = cat.tenant;
    let mut out = BackendState::new();
    let tenant_pattern = format!("t{tenant}\\_d%");
    for u in auth.users() {
        let uid = u.row.user_id;
        let mut grants: BTreeMap<BackendTarget, ActionSet> = BTreeMap::new();
        let mut add = |t: BackendTarget, a: ActionSet| {
            let a = a.intersect(t.accepts());
            if !a.is_empty() {
                let e = grants.entry(t).or_default();
                *e = e.union(a);
            }
        };
        let compiled_tenant;
        if state.is_migrated() {
            let roles = state.closure(&state.granted_roles(uid));
            let grantees: Vec<Grantee> = std::iter::once(Grantee::User(uid))
                .chain(roles.into_iter().map(Grantee::Role))
                .collect();
            let mut t_bits = ActionSet::EMPTY;
            let mut db_bits: BTreeMap<String, ActionSet> = BTreeMap::new();
            let mut rest = Vec::new();
            let mut per_grantee = Vec::new();
            for g in &grantees {
                let mut own = (ActionSet::EMPTY, BTreeMap::new());
                for p in state.privileges_of(*g) {
                    match &p.target {
                        PrivTarget::Tenant => {
                            t_bits = t_bits.union(p.actions);
                            own.0 = own.0.union(p.actions);
                        }
                        PrivTarget::Database(n) => {
                            let e = db_bits.entry(n.clone()).or_default();
                            *e = e.union(p.actions);
                        }
                        PrivTarget::Restriction(n) => {
                            let e: &mut ActionSet = own.1.entry(n.clone()).or_default();
                            *e = e.union(p.actions);
                        }
                        _ => rest.push(p.clone()),
                    }
                }
                per_grantee.push(own);
            }
            compiled_tenant = t_bits;
            // Partial revokes (R4): a restricted database gets its own
            // rows without the cut actions, which win over the tenant
            // pattern row as the more specific match.
            let cut = super::compile::restrictions(&per_grantee);
            for name in cut.keys() {
                db_bits.entry(name.clone()).or_default();
            }
            for (name, bits) in db_bits {
                let Some(d) = cat.database(&name).filter(|d| d.state == DbState::Active) else {
                    continue;
                };
                let tenant_part = t_bits.minus(cut.get(&name).copied().unwrap_or_default());
                // One pattern row per database, not one row per schema:
                // it covers every group, shard and reference schema the
                // database has now or gets later (regroups, splits, moves,
                // merges, re-keys), with no reconcile in between.
                add(
                    BackendTarget::Db(db_pattern(tenant, d.db_id)),
                    bits.union(tenant_part),
                );
            }
            for p in rest {
                let (d, o, column) = match &p.target {
                    PrivTarget::Object { db, object } => {
                        (cat.databases.get(*db), cat.objects.get(*object), None)
                    }
                    PrivTarget::Column { db, object, column } => (
                        cat.databases.get(*db),
                        cat.objects.get(*object),
                        Some(column),
                    ),
                    // Held by name: the relation with that name, if any.
                    PrivTarget::Named {
                        db_key,
                        name_key,
                        column,
                    } => {
                        let d = cat.database(db_key).filter(|d| d.state == DbState::Active);
                        let o = d.and_then(|d| {
                            cat.object(d.db_id, Namespace::Relation, name_key)
                                .filter(|o| o.state == ObjectState::Active)
                        });
                        if o.is_none() {
                            // No such table yet: MariaDB keeps a table
                            // grant without the table only when it
                            // includes CREATE (1146 otherwise), which is
                            // what lets the user create it again.
                            if let Some(d) = d
                                && column.is_none()
                                && p.actions.has(Action::Create)
                            {
                                for s in shard_schemas(tenant, d) {
                                    add(
                                        BackendTarget::Table(s.to_string(), name_key.clone()),
                                        p.actions,
                                    );
                                }
                            }
                            continue;
                        }
                        (d, o, column.as_ref())
                    }
                    _ => continue,
                };
                let (Some(d), Some(o)) = (d, o) else {
                    continue;
                };
                if d.state != DbState::Active || o.state != ObjectState::Active {
                    continue;
                }
                for s in shard_schemas(tenant, d) {
                    let schema = s.to_string();
                    let t = match (o.kind, column) {
                        (ObjectKind::Function, _) => {
                            BackendTarget::Routine(schema, o.name.to_lowercase(), "FUNCTION")
                        }
                        (ObjectKind::Procedure, _) => {
                            BackendTarget::Routine(schema, o.name.to_lowercase(), "PROCEDURE")
                        }
                        (_, Some(c)) => {
                            BackendTarget::Column(schema, o.name.clone(), c.to_lowercase())
                        }
                        (_, None) => BackendTarget::Table(schema, o.name.clone()),
                    };
                    add(t, p.actions);
                }
            }
        } else {
            compiled_tenant = ActionSet(u64::MAX);
        }
        add(BackendTarget::Db(tenant_pattern.clone()), compiled_tenant);
        redo_marker_grants(&mut grants, &tenant_pattern);
        doc_session_grants(&mut grants, &tenant_pattern);
        scratch_grants(&mut grants, &tenant_pattern);
        out.insert(account(tenant, uid), grants);
    }
    out
}

/// The `mysql.db` pattern of every schema of database `db`:
/// `` `t42\_d1\_%` `` matches `t42_d1_g<group>_s<shard>` and `t42_d1_ref`,
/// never another database's (`t42_d10_…` has no `_` after `d1`). As a
/// longer literal prefix than the tenant's `` `t42\_d%` ``, MariaDB picks it
/// first for the database's schemas.
#[must_use]
pub fn db_pattern(tenant: TenantId, db: sprig_common::ids::DbId) -> String {
    format!("{}\\_%", escape_db(&format!("t{tenant}_d{db}")))
}

/// Whether the MariaDB `mysql.db` pattern `pattern` (escaped `\_`, `\%`
/// and `\\`, wildcards `_` and `%`) matches schema name `name`.
fn db_matches(pattern: &str, name: &str) -> bool {
    fn go(p: &[char], n: &[char]) -> bool {
        match p.split_first() {
            None => n.is_empty(),
            Some(('%', rest)) => (0..=n.len()).any(|i| go(rest, &n[i..])),
            Some(('_', rest)) => !n.is_empty() && go(rest, &n[1..]),
            Some(('\\', rest)) if !rest.is_empty() => {
                n.first() == Some(&rest[0]) && go(&rest[1..], &n[1..])
            }
            Some((c, rest)) => n.first() == Some(c) && go(rest, &n[1..]),
        }
    }
    let p: Vec<char> = pattern.chars().collect();
    let n: Vec<char> = name.chars().collect();
    go(&p, &n)
}

/// The `mysql.db` row MariaDB uses for schema `schema`: the matching
/// pattern with the longest literal prefix (a name without wildcards
/// first), as its ACL sort does. MariaDB never adds two matching rows.
pub(crate) fn db_row_for(
    grants: &BTreeMap<BackendTarget, ActionSet>,
    schema: &str,
) -> Option<ActionSet> {
    fn prefix(p: &str) -> usize {
        let mut chars = p.chars();
        let mut i = 0;
        while let Some(c) = chars.next() {
            match c {
                '%' | '_' => return i,
                '\\' => {
                    chars.next();
                    i += 2;
                }
                _ => i += 1,
            }
        }
        usize::MAX
    }
    grants
        .iter()
        .filter_map(|(t, a)| match t {
            BackendTarget::Db(p) if db_matches(p, schema) => Some((prefix(p), *a)),
            _ => None,
        })
        .max_by_key(|(len, _)| *len)
        .map(|(_, a)| a)
}

/// The gateway's own temporary tables (chapter 17, "Gathered reads"; the
/// analytic reads' column probes): they are created by a connection of
/// the session's backend account in the schema the statement names, so
/// every database row of the account carries `CREATE TEMPORARY TABLES`,
/// and the tenant's pattern row exists for schemas no other row covers.
/// A temporary table holds only rows the account read with its own
/// `SELECT`, and only that connection sees it, so this widens nothing a
/// user can read. Users never create one without the privilege: the
/// gateway checks `CREATE TEMPORARY TABLES` itself before a statement
/// reaches MariaDB.
fn scratch_grants(grants: &mut BTreeMap<BackendTarget, ActionSet>, tenant_pattern: &str) {
    let ctt = ActionSet::of(&[Action::CreateTemporaryTables]);
    grants
        .entry(BackendTarget::Db(tenant_pattern.to_owned()))
        .or_default();
    for (t, a) in grants.iter_mut() {
        if matches!(t, BackendTarget::Db(_)) {
            *a = a.union(ctt);
        }
    }
}

/// Chapter 30's retryable-write records (R5, `decisions/r5-interim.md`
/// D12): a MongoDB write and its record in the database's
/// `__sprig_doc_sessions` commit in one MariaDB transaction on the
/// session's own account, so a user who may write some table of a schema
/// but whose schema-level grants don't cover the records table gets
/// `SELECT`, `INSERT`, `UPDATE` and `DELETE` on it there (with `CREATE`,
/// so the grant holds before the table exists and the engine may create
/// it). Clients never reach the table: chapter 07 refuses `__sprig_`
/// names outside the gateway's internal statements.
pub const DOC_SESSIONS_TABLE: &str = "__sprig_doc_sessions";

fn doc_session_grants(grants: &mut BTreeMap<BackendTarget, ActionSet>, tenant_pattern: &str) {
    let need = ActionSet::of(&[
        Action::Select,
        Action::Insert,
        Action::Update,
        Action::Delete,
    ]);
    let covered = |grants: &BTreeMap<BackendTarget, ActionSet>, pattern: &str| {
        grants.iter().any(|(t, a)| {
            matches!(t, BackendTarget::Db(p) if p == pattern) && a.intersect(need) == need
        })
    };
    if covered(grants, tenant_pattern) {
        return;
    }
    let writes = ActionSet::of(&[Action::Insert, Action::Update, Action::Delete]);
    let schemas: BTreeSet<String> = grants
        .iter()
        .filter(|(_, a)| !a.intersect(writes).is_empty())
        .filter_map(|(t, _)| match t {
            BackendTarget::Table(s, n) | BackendTarget::Column(s, n, _)
                if n != DOC_SESSIONS_TABLE && n != "__sprig_redo" =>
            {
                Some(s.clone())
            }
            _ => None,
        })
        .collect();
    for s in schemas {
        if db_row_for(grants, &s).is_some_and(|a| a.intersect(need) == need) {
            continue;
        }
        let t = BackendTarget::Table(s, DOC_SESSIONS_TABLE.into());
        let a = need.with(Action::Create).intersect(t.accepts());
        let e = grants.entry(t).or_default();
        *e = e.union(a);
    }
}

/// Chapter 18's Sprig 2PC (R3): a held branch commits with a marker row in
/// its schema's `__sprig_redo` (`sprig_txn::redo::REDO_TABLE`), inserted by
/// the session's own account inside the branch's transaction. A user whose
/// grants in a schema are table-level only, without `INSERT` on the
/// schema, and who may write there, gets `INSERT` on that table there (with `CREATE`: MariaDB keeps
/// a table grant on a table that doesn't exist yet only then).
fn redo_marker_grants(grants: &mut BTreeMap<BackendTarget, ActionSet>, tenant_pattern: &str) {
    let inserts_in = |grants: &BTreeMap<BackendTarget, ActionSet>, pattern: &str| {
        grants.iter().any(|(t, a)| {
            matches!(t, BackendTarget::Db(p) if p == pattern) && a.has(Action::Insert)
        })
    };
    if inserts_in(grants, tenant_pattern) {
        return;
    }
    // Only where the user can write: a branch that writes nothing is
    // never held.
    let writes = ActionSet::of(&[Action::Insert, Action::Update, Action::Delete]);
    let schemas: BTreeSet<String> = grants
        .iter()
        .filter(|(_, a)| !a.intersect(writes).is_empty())
        .filter_map(|(t, _)| match t {
            BackendTarget::Table(s, _) | BackendTarget::Column(s, _, _) => Some(s.clone()),
            _ => None,
        })
        .collect();
    for s in schemas {
        if db_row_for(grants, &s).is_some_and(|a| a.has(Action::Insert)) {
            continue;
        }
        let t = BackendTarget::Table(s, "__sprig_redo".into());
        let a = ActionSet::of(&[Action::Insert, Action::Create]).intersect(t.accepts());
        let e = grants.entry(t).or_default();
        *e = e.union(a);
    }
}

/// The tenant's per-user accounts on the cluster and their grants, read
/// as `sprig_ctl`.
///
/// # Errors
/// A backend error.
pub async fn read_have(conn: &mut Conn, tenant: TenantId) -> Result<BackendState> {
    let like = format!("t{tenant}\\_u%");
    let mut have = BackendState::new();
    let users: Vec<String> = conn
        .exec(
            "SELECT User FROM mysql.global_priv WHERE User LIKE ? AND Host = '%'",
            (&like,),
        )
        .await
        .map_err(crate::store::map_err)?;
    for u in users {
        have.entry(u).or_default();
    }
    let cols = BACKEND_DB_COLUMNS.join(", ");
    let rows: Vec<Row> = conn
        .exec(
            format!("SELECT User, Db, {cols} FROM mysql.db WHERE User LIKE ? AND Host = '%'"),
            (&like,),
        )
        .await
        .map_err(crate::store::map_err)?;
    for r in rows {
        let user: String = r.get(0).unwrap_or_default();
        let db: String = r.get(1).unwrap_or_default();
        let mut set = ActionSet::EMPTY;
        for (i, (a, _)) in DB_COLUMN_ACTIONS.iter().enumerate() {
            let v: String = r.get(i + 2).unwrap_or_default();
            if v == "Y" {
                set = set.with(*a);
            }
        }
        have.entry(user)
            .or_default()
            .insert(BackendTarget::Db(db), set);
    }
    let rows: Vec<(String, String, String, String)> = conn
        .exec(
            "SELECT User, Db, Table_name, Table_priv FROM mysql.tables_priv \
             WHERE User LIKE ? AND Host = '%'",
            (&like,),
        )
        .await
        .map_err(crate::store::map_err)?;
    for (user, db, table, privs) in rows {
        let set = parse_set(&privs);
        if !set.is_empty() {
            have.entry(user)
                .or_default()
                .insert(BackendTarget::Table(db, table), set);
        }
    }
    let rows: Vec<(String, String, String, String, String)> = conn
        .exec(
            "SELECT User, Db, Table_name, Column_name, Column_priv FROM mysql.columns_priv \
             WHERE User LIKE ? AND Host = '%'",
            (&like,),
        )
        .await
        .map_err(crate::store::map_err)?;
    for (user, db, table, column, privs) in rows {
        let set = parse_set(&privs);
        if !set.is_empty() {
            have.entry(user)
                .or_default()
                .insert(BackendTarget::Column(db, table, column.to_lowercase()), set);
        }
    }
    let rows: Vec<(String, String, String, String, String)> = conn
        .exec(
            "SELECT User, Db, Routine_name, Routine_type, Proc_priv FROM mysql.procs_priv \
             WHERE User LIKE ? AND Host = '%'",
            (&like,),
        )
        .await
        .map_err(crate::store::map_err)?;
    for (user, db, routine, kind, privs) in rows {
        let set = parse_set(&privs);
        let kind = if kind.eq_ignore_ascii_case("FUNCTION") {
            "FUNCTION"
        } else {
            "PROCEDURE"
        };
        if !set.is_empty() {
            have.entry(user).or_default().insert(
                BackendTarget::Routine(db, routine.to_lowercase(), kind),
                set,
            );
        }
    }
    Ok(have)
}

/// `mysql.db` privilege columns, in [`DB_COLUMN_ACTIONS`] order.
const BACKEND_DB_COLUMNS: &[&str] = &[
    "Select_priv",
    "Insert_priv",
    "Update_priv",
    "Delete_priv",
    "Create_priv",
    "Drop_priv",
    "References_priv",
    "Index_priv",
    "Alter_priv",
    "Create_tmp_table_priv",
    "Lock_tables_priv",
    "Execute_priv",
    "Create_view_priv",
    "Show_view_priv",
    "Create_routine_priv",
    "Alter_routine_priv",
    "Event_priv",
    "Trigger_priv",
];
const DB_COLUMN_ACTIONS: &[(Action, &str)] = BACKEND;

/// A `tables_priv`, `columns_priv` or `procs_priv` `SET` value.
fn parse_set(s: &str) -> ActionSet {
    let mut set = ActionSet::EMPTY;
    for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        if let Some((a, _)) = BACKEND.iter().find(|(_, n)| n.eq_ignore_ascii_case(part)) {
            set = set.with(*a);
        }
    }
    set
}

/// What a reconcile did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Statements run.
    pub statements: usize,
    /// Accounts whose grants changed (their pooled connections should be
    /// replaced: MariaDB keeps a connection's database privileges until
    /// its next `USE`).
    pub changed: BTreeSet<String>,
    /// Accounts created.
    pub created: usize,
    /// Accounts dropped.
    pub dropped: usize,
}

/// The statements turning `have` into `want`, and the accounts they touch.
/// `full`: also set every wanted account's password (a new secret).
#[must_use]
pub fn plan(
    want: &BackendState,
    have: &BackendState,
    key: &BackendKey,
    full: bool,
) -> (Vec<Zeroizing<String>>, ReconcileReport) {
    let mut out = Vec::new();
    let mut rep = ReconcileReport::default();
    for (acct, grants) in want {
        let who = format!("'{acct}'@'%'");
        let found = have.get(acct);
        if found.is_none() {
            out.push(Zeroizing::new(format!(
                "CREATE USER IF NOT EXISTS {who} IDENTIFIED BY '{}' REQUIRE SSL",
                key.password(acct).as_str()
            )));
            rep.created += 1;
        } else if full {
            out.push(Zeroizing::new(format!(
                "ALTER USER {who} IDENTIFIED BY '{}' REQUIRE SSL",
                key.password(acct).as_str()
            )));
        }
        let empty = BTreeMap::new();
        let found = found.unwrap_or(&empty);
        let mut touched = false;
        for (t, set) in grants {
            let had = found.get(t).copied().unwrap_or_default();
            let mut add = set.minus(had);
            let del = had.minus(*set);
            // MariaDB refuses a table grant on a missing table (1146)
            // unless the list includes CREATE; repeating CREATE is harmless.
            if !add.is_empty() && matches!(t, BackendTarget::Table(..)) && set.has(Action::Create) {
                add = add.with(Action::Create);
            }
            if !add.is_empty() {
                out.push(Zeroizing::new(format!(
                    "GRANT {} ON {} TO {who}",
                    t.privileges(add),
                    t.on()
                )));
                touched = true;
            }
            if !del.is_empty() {
                out.push(Zeroizing::new(format!(
                    "REVOKE {} ON {} FROM {who}",
                    t.privileges(del),
                    t.on()
                )));
                touched = true;
            }
        }
        for (t, had) in found {
            if grants.contains_key(t) || had.is_empty() {
                continue;
            }
            out.push(Zeroizing::new(format!(
                "REVOKE {} ON {} FROM {who}",
                t.privileges(*had),
                t.on()
            )));
            touched = true;
        }
        if touched {
            rep.changed.insert(acct.clone());
        }
    }
    for acct in have.keys() {
        if !want.contains_key(acct) {
            out.push(Zeroizing::new(format!("DROP USER IF EXISTS '{acct}'@'%'")));
            rep.dropped += 1;
            rep.changed.insert(acct.clone());
        }
    }
    rep.statements = out.len();
    (out, rep)
}

/// Errors a concurrent reconcile can cause, meaning "already done".
fn benign(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(s) if matches!(s.code, 1141 | 1147 | 1396 | 1403))
}

/// Brings the tenant's accounts on the cluster behind `conn` (a
/// `sprig_ctl` connection to its writer) to `want`.
///
/// # Errors
/// A backend error other than the "already done" ones.
pub async fn reconcile(
    conn: &mut Conn,
    tenant: TenantId,
    want: &BackendState,
    key: &BackendKey,
    full: bool,
) -> Result<ReconcileReport> {
    let have = read_have(conn, tenant).await?;
    let (stmts, rep) = plan(want, &have, key, full);
    for s in stmts {
        if let Err(e) = conn.query_drop(s.as_str()).await {
            if benign(&e) {
                continue;
            }
            // Never echo the statement: it may hold a password.
            return Err(crate::store::map_err(e));
        }
    }
    if rep.statements > 0 {
        metrics::counter!("sprig_auth_reconcile_statements_total").increment(rep.statements as u64);
    }
    Ok(rep)
}

/// The physical schemas a tenant's backend accounts may reach: every
/// shard schema of every live database (for checks and tests).
#[must_use]
pub fn tenant_schemas(cat: &TenantCatalog) -> Vec<SchemaName> {
    cat.databases
        .iter()
        .filter(|d| d.state == DbState::Active)
        .flat_map(|d| shard_schemas(cat.tenant, d))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_matches_rfc4231_case_2() {
        let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        let hex = mac.iter().fold(String::new(), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        });
        assert_eq!(
            hex,
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn passwords_differ_per_account() {
        let k = BackendKey::new(b"0123456789abcdef").unwrap_or_else(|_| unreachable!());
        assert_ne!(*k.password("t1_u1"), *k.password("t1_u2"));
        assert_eq!(k.password("t1_u1").len(), 40);
        assert!(BackendKey::new(b"short").is_err());
    }

    #[test]
    fn plan_grants_revokes_and_drops() {
        let k = BackendKey::new(b"0123456789abcdef").unwrap_or_else(|_| unreachable!());
        let db = BackendTarget::Db("t1\\_d%".into());
        let t = BackendTarget::Table("t1_d1_g1_s1".into(), "orders".into());
        let mut want = BackendState::new();
        want.entry("t1_u1".into())
            .or_default()
            .insert(db.clone(), ActionSet::of(&[Action::Select, Action::Insert]));
        want.entry("t1_u2".into()).or_default();
        let mut have = BackendState::new();
        have.entry("t1_u1".into())
            .or_default()
            .insert(db.clone(), ActionSet::of(&[Action::Select, Action::Delete]));
        have.entry("t1_u1".into())
            .or_default()
            .insert(t, ActionSet::of(&[Action::Select]));
        have.entry("t1_u9".into()).or_default();
        let (stmts, rep) = plan(&want, &have, &k, false);
        let text: Vec<&str> = stmts.iter().map(|s| s.as_str()).collect();
        assert!(
            text.contains(&"GRANT INSERT ON `t1\\_d%`.* TO 't1_u1'@'%'"),
            "{text:?}"
        );
        assert!(text.contains(&"REVOKE DELETE ON `t1\\_d%`.* FROM 't1_u1'@'%'"));
        assert!(text.contains(&"REVOKE SELECT ON `t1_d1_g1_s1`.`orders` FROM 't1_u1'@'%'"));
        assert!(
            text.iter()
                .any(|s| s.starts_with("CREATE USER IF NOT EXISTS 't1_u2'@'%'"))
        );
        assert!(text.contains(&"DROP USER IF EXISTS 't1_u9'@'%'"));
        assert_eq!(rep.created, 1);
        assert_eq!(rep.dropped, 1);
        assert!(rep.changed.contains("t1_u1"));
        // Converged: nothing to do.
        let (none, _) = plan(&want, &want, &k, false);
        assert!(none.is_empty());
    }

    #[test]
    fn table_grants_repeat_create() {
        // MariaDB grants on a missing table only with CREATE in the list.
        let k = BackendKey::new(b"0123456789abcdef").unwrap_or_else(|_| unreachable!());
        let t = BackendTarget::Table("t1_d1_g1_s1".into(), "orders".into());
        let mut want = BackendState::new();
        want.entry("t1_u1".into())
            .or_default()
            .insert(t.clone(), ActionSet::of(&[Action::Select, Action::Create]));
        let mut have = BackendState::new();
        have.entry("t1_u1".into())
            .or_default()
            .insert(t, ActionSet::of(&[Action::Create]));
        let (stmts, _) = plan(&want, &have, &k, false);
        let text: Vec<&str> = stmts.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            text,
            vec!["GRANT SELECT, CREATE ON `t1_d1_g1_s1`.`orders` TO 't1_u1'@'%'"]
        );
    }

    #[test]
    fn table_level_users_can_insert_the_redo_marker() {
        let t = BackendTarget::Table("t1_d1_g1_s1".into(), "orders".into());
        let mut g = BTreeMap::new();
        g.insert(t, ActionSet::of(&[Action::Select, Action::Insert]));
        redo_marker_grants(&mut g, "t1\\_d%");
        let redo = BackendTarget::Table("t1_d1_g1_s1".into(), "__sprig_redo".into());
        assert_eq!(
            g.get(&redo).copied(),
            Some(ActionSet::of(&[Action::Insert, Action::Create]))
        );
        // A schema-level INSERT covers it already.
        let mut g = BTreeMap::new();
        g.insert(
            BackendTarget::Table("t1_d1_g1_s1".into(), "orders".into()),
            ActionSet::of(&[Action::Select]),
        );
        g.insert(
            BackendTarget::Db("t1\\_d%".into()),
            ActionSet::of(&[Action::Insert]),
        );
        redo_marker_grants(&mut g, "t1\\_d%");
        assert_eq!(g.len(), 2);
    }

    #[test]
    fn table_level_writers_reach_the_doc_session_records() {
        let mut g = BTreeMap::new();
        g.insert(
            BackendTarget::Table("t1_d1_g1_s1".into(), "orders".into()),
            ActionSet::of(&[Action::Select, Action::Update]),
        );
        g.insert(
            BackendTarget::Table("t1_d2_g1_s1".into(), "audit".into()),
            ActionSet::of(&[Action::Select]),
        );
        doc_session_grants(&mut g, "t1\\_d%");
        let rec = BackendTarget::Table("t1_d1_g1_s1".into(), DOC_SESSIONS_TABLE.into());
        assert_eq!(
            g.get(&rec).copied(),
            Some(ActionSet::of(&[
                Action::Select,
                Action::Insert,
                Action::Update,
                Action::Delete,
                Action::Create
            ]))
        );
        // Read-only grants write no records.
        let ro = BackendTarget::Table("t1_d2_g1_s1".into(), DOC_SESSIONS_TABLE.into());
        assert!(!g.contains_key(&ro));
        // Schema-level DML covers the table already.
        let mut g = BTreeMap::new();
        g.insert(
            BackendTarget::Table("t1_d1_g1_s1".into(), "orders".into()),
            ActionSet::of(&[Action::Insert]),
        );
        g.insert(
            BackendTarget::Db("t1\\_d1\\_g1\\_s1".into()),
            ActionSet::of(&[
                Action::Select,
                Action::Insert,
                Action::Update,
                Action::Delete,
            ]),
        );
        doc_session_grants(&mut g, "t1\\_d%");
        assert_eq!(g.len(), 2);
    }

    #[test]
    fn database_patterns_match_only_their_databases_schemas() {
        let p = db_pattern(TenantId(42), sprig_common::ids::DbId(1));
        assert_eq!(p, "t42\\_d1\\_%");
        for yes in ["t42_d1_g1_s1", "t42_d1_g7_s3", "t42_d1_g90001_s12", "t42_d1_ref"] {
            assert!(db_matches(&p, yes), "{yes}");
        }
        for no in ["t42_d10_g1_s1", "t42_d2_g1_s1", "t420_d1_g1_s1", "t42_sprig", "t42xd1_g1_s1"] {
            assert!(!db_matches(&p, no), "{no}");
        }
        assert!(db_matches("t42\\_d%", "t42_d10_g1_s1"));
        assert!(!db_matches("t42\\_d%", "t42_sprig"));
    }

    #[test]
    fn the_most_specific_database_row_wins() {
        let mut g = BTreeMap::new();
        g.insert(
            BackendTarget::Db("t1\\_d%".into()),
            ActionSet::of(&[Action::Select, Action::Insert]),
        );
        g.insert(
            BackendTarget::Db("t1\\_d1\\_%".into()),
            ActionSet::of(&[Action::Select]),
        );
        assert_eq!(
            db_row_for(&g, "t1_d1_g3_s2"),
            Some(ActionSet::of(&[Action::Select]))
        );
        assert_eq!(
            db_row_for(&g, "t1_d2_g1_s1"),
            Some(ActionSet::of(&[Action::Select, Action::Insert]))
        );
        // A table-level writer of a database whose row lacks INSERT gets
        // the redo marker; one whose database row has it doesn't.
        g.insert(
            BackendTarget::Table("t1_d1_g3_s2".into(), "orders".into()),
            ActionSet::of(&[Action::Insert]),
        );
        redo_marker_grants(&mut g, "t9\\_d%");
        assert!(g.contains_key(&BackendTarget::Table(
            "t1_d1_g3_s2".into(),
            "__sprig_redo".into()
        )));
    }

    #[test]
    fn every_database_row_can_create_temporary_tables() {
        let ctt = ActionSet::of(&[Action::CreateTemporaryTables]);
        // Table grants only: the tenant row appears, with CTT alone.
        let mut g = BTreeMap::new();
        g.insert(
            BackendTarget::Table("t1_d1_g1_s1".into(), "orders".into()),
            ActionSet::of(&[Action::Select]),
        );
        scratch_grants(&mut g, "t1\\_d%");
        assert_eq!(g.get(&BackendTarget::Db("t1\\_d%".into())).copied(), Some(ctt));
        // A database row keeps its own actions and gets CTT too, so the
        // more specific row still covers temporary tables.
        let mut g = BTreeMap::new();
        g.insert(
            BackendTarget::Db("t1\\_d1\\_%".into()),
            ActionSet::of(&[Action::Select]),
        );
        scratch_grants(&mut g, "t1\\_d%");
        assert_eq!(
            db_row_for(&g, "t1_d1_g4_s1"),
            Some(ActionSet::of(&[Action::Select]).union(ctt))
        );
    }

    #[test]
    fn database_names_are_escaped() {
        assert_eq!(escape_db("t1_d1_g1_s1"), "t1\\_d1\\_g1\\_s1");
        assert_eq!(
            BackendTarget::Column("s".into(), "t".into(), "c".into())
                .privileges(ActionSet::of(&[Action::Select, Action::Update])),
            "SELECT (`c`), UPDATE (`c`)"
        );
        assert_eq!(
            parse_set("Select,Show view,Grant"),
            ActionSet::of(&[Action::Select, Action::ShowView])
        );
    }
}
