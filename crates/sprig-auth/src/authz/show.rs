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
//! Grants on objects that no longer exist aren't shown.

use std::collections::BTreeMap;

use sprig_catalog::TenantCatalog;
use sprig_catalog::store::{ObjectKind, ObjectState};
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

/// The lines of `SHOW GRANTS FOR acc [USING extra]`.
#[must_use]
#[allow(clippy::too_many_lines)] // the levels in rendering order
pub fn render(
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
    let mut objects: BTreeMap<ObjectId, ActionSet> = BTreeMap::new();
    let mut columns: BTreeMap<ObjectId, BTreeMap<String, ActionSet>> = BTreeMap::new();
    let grantees =
        std::iter::once(grantee).chain(state.closure(extra).into_iter().map(Grantee::Role));
    for g in grantees {
        for p in state.privileges_of(g) {
            match &p.target {
                PrivTarget::Tenant => tenant = tenant.union(p.actions),
                PrivTarget::Database(n) => {
                    let e = dbs.entry(n.clone()).or_default();
                    *e = e.union(p.actions);
                }
                PrivTarget::Object { object, .. } => {
                    let e = objects.entry(*object).or_default();
                    *e = e.union(p.actions);
                }
                PrivTarget::Column { object, column, .. } => {
                    let e = columns
                        .entry(*object)
                        .or_default()
                        .entry(column.clone())
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
    // db.obj
    let mut objs: Vec<(String, String, ObjectType, ObjectId)> = Vec::new();
    for id in objects.keys().chain(columns.keys()) {
        let Some(o) = cat
            .objects
            .get(*id)
            .filter(|o| o.state == ObjectState::Active)
        else {
            continue;
        };
        let Some(d) = cat.databases.get(o.db_id) else {
            continue;
        };
        let kind = match o.kind {
            ObjectKind::Function => ObjectType::Function,
            ObjectKind::Procedure => ObjectType::Procedure,
            _ => ObjectType::Table,
        };
        objs.push((d.name.clone(), o.name.clone(), kind, *id));
    }
    objs.sort();
    objs.dedup();
    for (db, name, kind, id) in objs {
        let set = objects.get(&id).copied().unwrap_or_default();
        let cols = columns.get(&id);
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
            if set.has(a) {
                if !all_table {
                    parts.push(a.command().to_owned());
                }
            } else if let Some(cols) = cols {
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
            for e in edges {
                if let Some(a) = role_acc(e.role) {
                    let opt = if e.admin_option {
                        " WITH ADMIN OPTION"
                    } else {
                        ""
                    };
                    out.push(format!("GRANT {} TO {to}{opt}", q(&a.name)));
                }
            }
            if let Some(d) = edges.iter().find(|e| e.is_default)
                && let Some(a) = role_acc(d.role)
            {
                out.push(format!("SET DEFAULT ROLE {} FOR {to}", q(&a.name)));
            }
        }
    }
    out
}
