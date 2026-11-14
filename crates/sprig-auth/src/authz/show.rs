//! Rendering `SHOW GRANTS`.
//!
//! ```text
//! "GRANT <tenant static privileges | USAGE> ON *.* TO a [WITH GRANT OPTION]"
//! MySQL: "GRANT <dynamic privileges> ON *.* TO a" when any
//! per database: "GRANT <privileges> ON `db`.* TO a"
//! per object:   "GRANT <privileges>[, PRIV (`c1`, `c2`)] ON `db`.`t` TO a"
//!               routines "ON FUNCTION" / "ON PROCEDURE"
//! roles: MySQL "GRANT `r1`@`%`,`r2`@`%` TO a"; MariaDB one line per role,
//!        then "SET DEFAULT ROLE `r1` FOR a"
//! ```
//!
//! Names are always logical; an object's database is its current name.
//! Grants held by name (a dropped table's, `auth.grants_on_drop = keep`)
//! are shown under that name whether or not the table exists, as MySQL
//! shows `tables_priv` rows; rows of other objects that no longer exist
//! aren't shown.

use std::collections::BTreeMap;

use sprig_catalog::TenantCatalog;
use sprig_catalog::store::{DbState, Namespace, ObjectKind, ObjectState};
use sprig_common::ids::{ObjectId, RoleId};
use sprig_proto_mysql::Persona;

use super::privilege::{ALL_TENANT, Action, ActionSet, Level, ObjectType, render_static};
use super::state::AuthzState;
use super::stmt::AccountRef;
use super::store::{Grantee, PrivTarget};

fn q(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

fn account(a: &AccountRef, role: bool, persona: Persona) -> String {
    if role && persona != Persona::MySql84 {
        q(&a.name)
    } else {
        format!("{}@{}", q(&a.name), q(&a.host))
    }
}

/// `names` with the extension `WATCH` after the static names, when `set`
/// holds it and isn't `ALL PRIVILEGES` (database level).
fn with_watch(mut names: Vec<&'static str>, set: ActionSet) -> Vec<&'static str> {
    if set.has(Action::Watch) && names != ["ALL PRIVILEGES"] {
        names.push("WATCH");
    }
    names
}

fn with_option(set: ActionSet) -> &'static str {
    if set.has(Action::GrantOption) {
        " WITH GRANT OPTION"
    } else {
        ""
    }
}

/// An object as `SHOW GRANTS` names it: database, name, type.
type ObjKey = (String, String, ObjectType);

/// A live object's names; `None` once it is gone.
fn live_key(cat: &TenantCatalog, id: ObjectId) -> Option<ObjKey> {
    let o = cat
        .objects
        .get(id)
        .filter(|o| o.state == ObjectState::Active)?;
    let d = cat.databases.get(o.db_id)?;
    let kind = match o.kind {
        ObjectKind::Function => ObjectType::Function,
        ObjectKind::Procedure => ObjectType::Procedure,
        _ => ObjectType::Table,
    };
    Some((d.name.clone(), o.name.clone(), kind))
}

/// A grant held by name: the live relation's names when one exists, else
/// the keys it was kept under.
fn named_key(cat: &TenantCatalog, db_key: &str, name_key: &str) -> ObjKey {
    cat.database(db_key)
        .filter(|d| d.state == DbState::Active)
        .and_then(|d| cat.object(d.db_id, Namespace::Relation, name_key))
        .and_then(|o| live_key(cat, o.object_id))
        .unwrap_or_else(|| (db_key.to_owned(), name_key.to_owned(), ObjectType::Table))
}

/// The lines of `SHOW GRANTS FOR acc [USING extra]`. On the MariaDB
/// persona a role's lines are followed by those of every role granted to
/// it, depth first, as MariaDB 11.8 prints them (measured).
#[must_use]
pub fn render(
    state: &AuthzState,
    cat: &TenantCatalog,
    grantee: Grantee,
    acc: &AccountRef,
    extra: &[RoleId],
    persona: Persona,
) -> Vec<String> {
    let mut out = render_one(state, cat, grantee, acc, extra, persona);
    if persona == Persona::MariaDb
        && let Grantee::Role(top) = grantee
    {
        let mut seen = vec![top];
        let mut stack: Vec<RoleId> = state
            .edges_of(grantee)
            .iter()
            .rev()
            .map(|e| e.role)
            .collect();
        while let Some(r) = stack.pop() {
            if seen.contains(&r) {
                continue;
            }
            seen.push(r);
            if let Some(row) = state.role(r) {
                let a = AccountRef {
                    name: row.name.clone(),
                    host: row.host.clone(),
                };
                out.extend(render_one(state, cat, Grantee::Role(r), &a, &[], persona));
            }
            stack.extend(
                state
                    .edges_of(Grantee::Role(r))
                    .iter()
                    .rev()
                    .map(|e| e.role),
            );
        }
    }
    out
}

/// One grantee's lines.
#[allow(clippy::too_many_lines)] // the levels in rendering order
fn render_one(
    state: &AuthzState,
    cat: &TenantCatalog,
    grantee: Grantee,
    acc: &AccountRef,
    extra: &[RoleId],
    persona: Persona,
) -> Vec<String> {
    let is_role = matches!(grantee, Grantee::Role(_));
    let to = account(acc, is_role, persona);
    let mut tenant = ActionSet::EMPTY;
    let mut dbs: BTreeMap<String, ActionSet> = BTreeMap::new();
    let mut restricted: BTreeMap<String, ActionSet> = BTreeMap::new();
    // Objects by (database, name, type) as shown, so a grant held by name
    // and one on the live table merge into one line.
    let mut objects: BTreeMap<ObjKey, ActionSet> = BTreeMap::new();
    let mut columns: BTreeMap<ObjKey, BTreeMap<String, ActionSet>> = BTreeMap::new();
    // Graph elements by (database, graph, relationship type?, name).
    let mut elements: BTreeMap<(String, String, bool, String), ActionSet> = BTreeMap::new();
    let grantees =
        std::iter::once(grantee).chain(state.closure(extra).into_iter().map(Grantee::Role));
    for g in grantees {
        for p in state.privileges_of(g) {
            let (key, column) = match &p.target {
                PrivTarget::Tenant => {
                    tenant = tenant.union(p.actions);
                    continue;
                }
                PrivTarget::Database(n) => {
                    let e = dbs.entry(n.clone()).or_default();
                    *e = e.union(p.actions);
                    continue;
                }
                // Partial revokes are the account's own: a role's apply
                // only through the role.
                PrivTarget::Restriction(n) if g == grantee => {
                    let e = restricted.entry(n.clone()).or_default();
                    *e = e.union(p.actions);
                    continue;
                }
                PrivTarget::Restriction(_) => continue,
                PrivTarget::GraphElement {
                    graph, rels, name, ..
                } => {
                    if let Some((db, g, _)) = live_key(cat, *graph) {
                        let e = elements.entry((db, g, *rels, name.clone())).or_default();
                        *e = e.union(p.actions);
                    }
                    continue;
                }
                PrivTarget::Object { object, .. } => (live_key(cat, *object), None),
                PrivTarget::Column { object, column, .. } => (live_key(cat, *object), Some(column)),
                PrivTarget::Named {
                    db_key,
                    name_key,
                    column,
                } => (Some(named_key(cat, db_key, name_key)), column.as_ref()),
            };
            let Some(key) = key else {
                continue;
            };
            match column {
                None => {
                    let e = objects.entry(key).or_default();
                    *e = e.union(p.actions);
                }
                Some(c) => {
                    let e = columns
                        .entry(key)
                        .or_default()
                        .entry(c.clone())
                        .or_default();
                    *e = e.union(p.actions);
                }
            }
        }
    }
    let mut out = Vec::new();
    // *.*
    let statics = render_static(tenant, Level::Tenant, ObjectType::Table);
    let all = statics == ["ALL PRIVILEGES"];
    let dynamic: Vec<&str> = {
        let mut d: Vec<&str> = tenant
            .iter()
            .filter(|a| a.is_dynamic())
            .filter(|a| persona == Persona::MySql84 || !all || !ALL_TENANT.has(*a))
            .map(Action::command)
            .collect();
        d.sort_unstable();
        d
    };
    let mut first: Vec<&str> = if statics.is_empty() {
        vec![]
    } else {
        statics.clone()
    };
    if persona != Persona::MySql84 {
        first.extend(dynamic.iter().copied());
    }
    if first.is_empty() {
        first.push("USAGE");
    }
    out.push(format!(
        "GRANT {} ON *.* TO {to}{}",
        first.join(", "),
        with_option(tenant)
    ));
    if persona == Persona::MySql84 && !dynamic.is_empty() {
        out.push(format!(
            "GRANT {} ON *.* TO {to}{}",
            dynamic.join(","),
            with_option(tenant)
        ));
    }
    // db.*
    for (db, set) in &dbs {
        let names = with_watch(
            render_static(*set, Level::Database, ObjectType::Table),
            *set,
        );
        if names.is_empty() && !set.has(Action::GrantOption) {
            continue;
        }
        let names = if names.is_empty() {
            vec!["USAGE"]
        } else {
            names
        };
        out.push(format!(
            "GRANT {} ON {}.* TO {to}{}",
            names.join(", "),
            q(db),
            with_option(*set)
        ));
    }
    // Partial revokes, after the database grants (MySQL 8.4's order).
    for (db, set) in &restricted {
        let names = with_watch(
            render_static(*set, Level::Database, ObjectType::Table),
            *set,
        );
        if !names.is_empty() {
            out.push(format!(
                "REVOKE {} ON {}.* FROM {to}",
                names.join(", "),
                q(db)
            ));
        }
    }
    // db.obj
    let mut objs: Vec<ObjKey> = objects.keys().chain(columns.keys()).cloned().collect();
    objs.sort();
    objs.dedup();
    for key in objs {
        let set = objects.get(&key).copied().unwrap_or_default();
        let cols = columns.get(&key);
        let (db, name, kind) = key;
        let mut parts: Vec<String> = Vec::new();
        let table_names = render_static(set, Level::Object, kind);
        let all_table = table_names == ["ALL PRIVILEGES"];
        if all_table {
            parts.push("ALL PRIVILEGES".to_owned());
        }
        for a in [
            Action::Select,
            Action::Insert,
            Action::Update,
            Action::Delete,
            Action::Create,
            Action::Drop,
            Action::References,
            Action::Index,
            Action::Alter,
            Action::Execute,
            Action::CreateView,
            Action::ShowView,
            Action::AlterRoutine,
            Action::Trigger,
            Action::Watch,
        ] {
            if set.has(a) && !all_table {
                parts.push(a.command().to_owned());
            }
            // MySQL lists a column grant next to the same privilege at the
            // table level (`UPDATE, UPDATE (`b`)`).
            if let Some(cols) = cols {
                let named: Vec<String> = cols
                    .iter()
                    .filter(|(_, s)| s.has(a))
                    .map(|(c, _)| q(c))
                    .collect();
                if !named.is_empty() {
                    parts.push(format!("{} ({})", a.command(), named.join(", ")));
                }
            }
        }
        if parts.is_empty() {
            parts.push("USAGE".to_owned());
        }
        let on = match kind {
            ObjectType::Table => String::new(),
            ObjectType::Function => "FUNCTION ".to_owned(),
            ObjectType::Procedure => "PROCEDURE ".to_owned(),
        };
        out.push(format!(
            "GRANT {} ON {on}{}.{} TO {to}{}",
            parts.join(", "),
            q(&db),
            q(&name),
            with_option(set)
        ));
    }
    // Graph elements: Neo4j's forms, `READ`, `CREATE`, `DELETE`,
    // `SET PROPERTY {*}`.
    for ((db, g, rels, name), set) in &elements {
        let mut privs = Vec::new();
        for (a, n) in [
            (Action::Select, "READ"),
            (Action::Insert, "CREATE"),
            (Action::Delete, "DELETE"),
            (Action::Update, "SET PROPERTY {*}"),
        ] {
            if set.has(a) {
                privs.push(n);
            }
        }
        if privs.is_empty() {
            continue;
        }
        out.push(format!(
            "GRANT {} ON GRAPH {}.{} {} {} TO {to}",
            privs.join(", "),
            q(db),
            q(g),
            if *rels { "RELATIONSHIPS" } else { "NODES" },
            q(name)
        ));
    }
    // Roles.
    let edges = state.edges_of(grantee);
    let role_acc = |r: RoleId| {
        state.role(r).map(|row| AccountRef {
            name: row.name.clone(),
            host: row.host.clone(),
        })
    };
    if persona == Persona::MySql84 {
        {
            let plain: Vec<String> = edges
                .iter()
                .filter(|e| !e.admin_option)
                .filter_map(|e| role_acc(e.role))
                .map(|a| format!("{}@{}", q(&a.name), q(&a.host)))
                .collect();
            if !plain.is_empty() {
                out.push(format!("GRANT {} TO {to}", plain.join(",")));
            }
            let admin: Vec<String> = edges
                .iter()
                .filter(|e| e.admin_option)
                .filter_map(|e| role_acc(e.role))
                .map(|a| format!("{}@{}", q(&a.name), q(&a.host)))
                .collect();
            if !admin.is_empty() {
                out.push(format!(
                    "GRANT {} TO {to} WITH ADMIN OPTION",
                    admin.join(",")
                ));
            }
        }
    } else {
        {
            // MariaDB prints role grants before the privileges.
            let mut roles = Vec::new();
            for e in edges {
                if let Some(a) = role_acc(e.role) {
                    let opt = if e.admin_option {
                        " WITH ADMIN OPTION"
                    } else {
                        ""
                    };
                    roles.push(format!("GRANT {} TO {to}{opt}", q(&a.name)));
                }
            }
            out.splice(0..0, roles);
            if let Some(d) = edges.iter().find(|e| e.is_default)
                && let Some(a) = role_acc(d.role)
            {
                out.push(format!("SET DEFAULT ROLE {} FOR {to}", q(&a.name)));
            }
        }
    }
    out
}

/// A string literal as MySQL and MariaDB print one in `SHOW CREATE USER`.
fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// JSON as MySQL 8.4 prints a stored document: object keys by length,
/// then bytes; `", "` and `": "` between items.
fn mysql_json(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort_by(|a, b| a.len().cmp(&b.len()).then_with(|| a.cmp(b)));
            let items: Vec<String> = keys
                .into_iter()
                .map(|k| {
                    format!(
                        "{}: {}",
                        serde_json::Value::String(k.clone()),
                        mysql_json(&m[k])
                    )
                })
                .collect();
            format!("{{{}}}", items.join(", "))
        }
        serde_json::Value::Array(a) => {
            let items: Vec<String> = a.iter().map(mysql_json).collect();
            format!("[{}]", items.join(", "))
        }
        other => other.to_string(),
    }
}

/// The `REQUIRE` clause: SUBJECT then ISSUER on 3306, ISSUER then
/// SUBJECT on 3307, space-separated on both; 3307 leaves out `NONE`.
fn require_clause(row: &crate::store::UserRow, mysql: bool) -> Option<String> {
    use crate::store::RequireTls;
    let subject = row
        .require_subject
        .as_deref()
        .map(|v| format!("SUBJECT {}", lit(v)));
    let issuer = row
        .require_issuer
        .as_deref()
        .map(|v| format!("ISSUER {}", lit(v)));
    let dn: Vec<String> = if mysql {
        subject.into_iter().chain(issuer).collect()
    } else {
        issuer.into_iter().chain(subject).collect()
    };
    if !dn.is_empty() {
        return Some(format!("REQUIRE {}", dn.join(" ")));
    }
    match row.require_tls {
        RequireTls::X509 => Some("REQUIRE X509".into()),
        RequireTls::Ssl => Some("REQUIRE SSL".into()),
        RequireTls::None if mysql => Some("REQUIRE NONE".into()),
        RequireTls::None => None,
    }
}

/// `WITH MAX_...` for the limits that are set.
fn limits_clause(l: &crate::store::AccountLimits) -> Option<String> {
    let set: Vec<String> = [
        ("MAX_QUERIES_PER_HOUR", l.max_queries_per_hour),
        ("MAX_UPDATES_PER_HOUR", l.max_updates_per_hour),
        ("MAX_CONNECTIONS_PER_HOUR", l.max_connections_per_hour),
        ("MAX_USER_CONNECTIONS", l.max_user_connections),
    ]
    .into_iter()
    .filter(|(_, n)| *n > 0)
    .map(|(k, n)| format!("{k} {n}"))
    .collect();
    (!set.is_empty()).then(|| format!("WITH {}", set.join(" ")))
}

/// 3306's password, lock and attribute clauses after `PASSWORD EXPIRE`.
fn mysql_tail(row: &crate::store::UserRow, s: &mut String) {
    use std::fmt::Write as _;

    use crate::store::RequireCurrent;
    s.push_str(if row.account_locked {
        " ACCOUNT LOCK"
    } else {
        " ACCOUNT UNLOCK"
    });
    match row.password_history {
        Some(n) => {
            let _ = write!(s, " PASSWORD HISTORY {n}");
        }
        None => s.push_str(" PASSWORD HISTORY DEFAULT"),
    }
    match row.password_reuse_days {
        Some(n) => {
            let _ = write!(s, " PASSWORD REUSE INTERVAL {n} DAY");
        }
        None => s.push_str(" PASSWORD REUSE INTERVAL DEFAULT"),
    }
    s.push_str(match row.require_current {
        RequireCurrent::Default => " PASSWORD REQUIRE CURRENT DEFAULT",
        RequireCurrent::Yes => " PASSWORD REQUIRE CURRENT",
        RequireCurrent::Optional => " PASSWORD REQUIRE CURRENT OPTIONAL",
    });
    if let Some(n) = row.failed_login_attempts.filter(|n| *n > 0) {
        let _ = write!(s, " FAILED_LOGIN_ATTEMPTS {n}");
    }
    match row.password_lock_minutes {
        Some(m) if m < 0 => s.push_str(" PASSWORD_LOCK_TIME UNBOUNDED"),
        // MySQL counts in days; the catalog keeps minutes.
        Some(m) if m > 0 => {
            let _ = write!(s, " PASSWORD_LOCK_TIME {}", (m + 1439) / 1440);
        }
        _ => {}
    }
    if let Some(a) = &row.attributes {
        let _ = write!(s, " ATTRIBUTE {}", lit(&mysql_json(a)));
    }
}

/// `SHOW CREATE USER`'s line for `row`, as MySQL 8.4 (3306) or
/// MariaDB 11.8 (3307) prints it, measured against both. No password hash
/// or verifier is ever printed: 3306 prints
/// `IDENTIFIED WITH '<plugin>'` alone, 3307 no `IDENTIFIED` clause.
#[must_use]
pub fn create_user(row: &crate::store::UserRow, persona: Persona) -> String {
    use std::fmt::Write as _;
    let mysql = persona == Persona::MySql84;
    let mut s = format!("CREATE USER {}@{}", q(&row.name), q(&row.host));
    if mysql {
        let _ = write!(s, " IDENTIFIED WITH {}", lit(&row.plugin));
    }
    for c in [require_clause(row, mysql), limits_clause(&row.limits)]
        .into_iter()
        .flatten()
    {
        s.push(' ');
        s.push_str(&c);
    }
    let expire = if row.password_expired {
        Some("PASSWORD EXPIRE".to_owned())
    } else {
        match row.password_lifetime {
            None if mysql => Some("PASSWORD EXPIRE DEFAULT".to_owned()),
            None => None,
            Some(n) if n <= 0 => Some("PASSWORD EXPIRE NEVER".to_owned()),
            Some(n) => Some(format!("PASSWORD EXPIRE INTERVAL {n} DAY")),
        }
    };
    if !mysql && row.account_locked {
        s.push_str(" ACCOUNT LOCK");
    }
    if let Some(e) = expire {
        s.push(' ');
        s.push_str(&e);
    }
    if mysql {
        mysql_tail(row, &mut s);
    }
    s
}

#[cfg(test)]
mod create_user_tests {
    use std::time::UNIX_EPOCH;

    use sprig_common::ids::UserId;

    use super::*;
    use crate::store::{RequireCurrent, RequireTls, UserRow, UserSource};

    fn row(name: &str) -> UserRow {
        UserRow::new(UserId(7), name, "%", UserSource::Sql, UNIX_EPOCH, 1)
    }

    /// Lines measured on MySQL 8.4 and MariaDB 11.8 (mysql:8.4 and
    /// mariadb:11.8 images, 14 Nov 2026), less their password hashes.
    #[test]
    fn as_mysql_and_mariadb_print_it() {
        let tail = "PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT \
                    PASSWORD REUSE INTERVAL DEFAULT PASSWORD REQUIRE CURRENT DEFAULT";
        let mut u1 = row("u1");
        u1.require_tls = RequireTls::X509;
        u1.require_subject = Some("/O=A/CN=b".into());
        u1.require_issuer = Some("/CN=CA".into());
        assert_eq!(
            create_user(&u1, Persona::MySql84),
            format!(
                "CREATE USER `u1`@`%` IDENTIFIED WITH 'caching_sha2_password' \
                 REQUIRE SUBJECT '/O=A/CN=b' ISSUER '/CN=CA' {tail}"
            )
        );
        assert_eq!(
            create_user(&u1, Persona::MariaDb),
            "CREATE USER `u1`@`%` REQUIRE ISSUER '/CN=CA' SUBJECT '/O=A/CN=b'"
        );
        let mut u2 = row("u2");
        u2.require_tls = RequireTls::X509;
        assert_eq!(
            create_user(&u2, Persona::MySql84),
            format!(
                "CREATE USER `u2`@`%` IDENTIFIED WITH 'caching_sha2_password' REQUIRE X509 {tail}"
            )
        );
        assert_eq!(
            create_user(&u2, Persona::MariaDb),
            "CREATE USER `u2`@`%` REQUIRE X509"
        );
        let mut u3 = row("u3");
        u3.require_tls = RequireTls::Ssl;
        u3.attributes = Some(serde_json::json!({"org": "o1"}));
        assert_eq!(
            create_user(&u3, Persona::MySql84),
            format!(
                "CREATE USER `u3`@`%` IDENTIFIED WITH 'caching_sha2_password' REQUIRE SSL {tail} \
                 ATTRIBUTE '{{\"org\": \"o1\"}}'"
            )
        );
        let mut u5 = row("u5");
        u5.require_tls = RequireTls::X509;
        u5.require_issuer = Some("/CN=CA".into());
        u5.failed_login_attempts = Some(3);
        u5.password_lock_minutes = Some(2 * 1440);
        u5.password_lifetime = Some(30);
        u5.account_locked = true;
        assert_eq!(
            create_user(&u5, Persona::MySql84),
            "CREATE USER `u5`@`%` IDENTIFIED WITH 'caching_sha2_password' REQUIRE ISSUER '/CN=CA' \
             PASSWORD EXPIRE INTERVAL 30 DAY ACCOUNT LOCK PASSWORD HISTORY DEFAULT \
             PASSWORD REUSE INTERVAL DEFAULT PASSWORD REQUIRE CURRENT DEFAULT \
             FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 2"
        );
        assert_eq!(
            create_user(&u5, Persona::MariaDb),
            "CREATE USER `u5`@`%` REQUIRE ISSUER '/CN=CA' ACCOUNT LOCK PASSWORD EXPIRE INTERVAL 30 DAY"
        );
        let mut u6 = UserRow::new(UserId(8), "u6", "10.%", UserSource::Sql, UNIX_EPOCH, 1);
        u6.password_history = Some(5);
        u6.password_reuse_days = Some(10);
        u6.require_current = RequireCurrent::Yes;
        assert_eq!(
            create_user(&u6, Persona::MySql84),
            "CREATE USER `u6`@`10.%` IDENTIFIED WITH 'caching_sha2_password' REQUIRE NONE \
             PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY 5 \
             PASSWORD REUSE INTERVAL 10 DAY PASSWORD REQUIRE CURRENT"
        );
        let mut l1 = row("l1");
        l1.limits.max_queries_per_hour = 10;
        l1.limits.max_user_connections = 2;
        assert_eq!(
            create_user(&l1, Persona::MariaDb),
            "CREATE USER `l1`@`%` WITH MAX_QUERIES_PER_HOUR 10 MAX_USER_CONNECTIONS 2"
        );
        let mut l3 = row("l3");
        l3.password_lock_minutes = Some(-1);
        l3.password_lifetime = Some(0);
        l3.require_current = RequireCurrent::Optional;
        assert_eq!(
            create_user(&l3, Persona::MySql84),
            "CREATE USER `l3`@`%` IDENTIFIED WITH 'caching_sha2_password' REQUIRE NONE \
             PASSWORD EXPIRE NEVER ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT \
             PASSWORD REUSE INTERVAL DEFAULT PASSWORD REQUIRE CURRENT OPTIONAL \
             PASSWORD_LOCK_TIME UNBOUNDED"
        );
        let mut l4 = row("l4");
        l4.password_expired = true;
        l4.attributes = Some(serde_json::json!({"zz": 1, "a": [1, 2], "bb": {"y": null}}));
        assert!(create_user(&l4, Persona::MySql84).ends_with(
            "PASSWORD EXPIRE ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL \
             DEFAULT PASSWORD REQUIRE CURRENT DEFAULT ATTRIBUTE '{\"a\": [1, 2], \"bb\": {\"y\": null}, \"zz\": 1}'"
        ));
        assert_eq!(
            create_user(&l4, Persona::MariaDb),
            "CREATE USER `l4`@`%` PASSWORD EXPIRE"
        );
    }
}
