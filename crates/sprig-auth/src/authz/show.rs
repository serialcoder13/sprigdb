//! `SHOW GRANTS` (chapter 29, "Rendering `SHOW GRANTS`"; build step 3's
//! basic form, exact per-persona text is step 9).
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
        let names = render_static(*set, Level::Database, ObjectType::Table);
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
        let names = render_static(*set, Level::Database, ObjectType::Table);
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
