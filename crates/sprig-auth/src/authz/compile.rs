//! Compile and check (chapter 29, "Evaluation"; build step 2).
//!
//! ```text
//! compile(user u, active roles A) at one snapshot:
//!   R    := closure of A over role_edges (breadth first)
//!   rows := privileges of u, and of every role in R
//!   fold rows by level: tenant bits, database bits by name key,
//!        object bits by id, column bits by (object, column)
//! check(action a, target t):
//!   b := tenant | database(t.db) | object(t.obj) | any column of t.obj
//!   allow iff a in b
//! ```
//!
//! Grants held by name ([`PrivTarget::Named`], kept from a dropped table)
//! are resolved against the snapshot: onto the table or view with that
//! name when one exists, else kept by name, where they count for creating
//! that table ([`Target::Named`]) and for seeing its database.
//!
//! SQL is checked coarsely here (an object passes if any column grant
//! gives the action); MariaDB applies exact column rules through the
//! user's backend account ([`super::backend`]). A [`CompiledPrivileges`]
//! belongs to one snapshot (auth and catalog version) and active-role set;
//! sessions compile again when either moves, so a `REVOKE` holds from the
//! next statement on every gateway once its cache has loaded the version
//! (the directory watch, typically well under a second).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use sprig_common::ids::{DbId, ObjectId, RoleId, UserId};

use super::privilege::{Action, ActionSet};
use super::state::AuthzState;
use super::store::{Grantee, PrivTarget};

/// Finds the live table or view with a (database key, name key).
pub type Relations<'a> = dyn Fn(&str, &str) -> Option<(DbId, ObjectId)> + 'a;

/// What a check is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target<'a> {
    /// The tenant (`*.*`).
    Tenant,
    /// A logical database by name key (it may not exist).
    Database {
        /// The name key.
        db_key: &'a str,
    },
    /// A table or view that doesn't exist, by name: creating it, or a
    /// statement naming a missing table (grants held by name count).
    Named {
        /// The database's name key.
        db_key: &'a str,
        /// The relation's name key.
        name_key: &'a str,
    },
    /// An object of an existing database.
    Object {
        /// The database's name key.
        db_key: &'a str,
        /// The database.
        db: DbId,
        /// The object.
        object: ObjectId,
    },
}

/// One user's effective privileges at one snapshot and active-role set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompiledPrivileges {
    /// The catalog isn't migrated: everything is allowed.
    pub everything: bool,
    /// `*.*`.
    pub tenant: ActionSet,
    /// `db.*`, by name key.
    pub dbs: HashMap<String, ActionSet>,
    /// `db.obj`.
    pub objects: HashMap<ObjectId, ActionSet>,
    /// The union of every column grant of `db.t`.
    pub col_any: HashMap<ObjectId, ActionSet>,
    /// `db.t (c)`, column lower case.
    pub columns: HashMap<(ObjectId, String), ActionSet>,
    /// Databases (by id) with any object or column grant.
    pub dbs_with_objects: BTreeSet<DbId>,
    /// Grants held by name on relations that don't exist in the snapshot,
    /// by (database key, name key): the table level and every column
    /// merged (the coarse check).
    pub named: HashMap<(String, String), ActionSet>,
    /// The roles the set was compiled for (active, then closed).
    pub roles: Vec<RoleId>,
    /// Partial revokes (R4): the tenant-level actions that don't reach a
    /// database, by name key. An action is cut only when every grantee
    /// that holds it at `*.*` restricts it there (MySQL: a role without
    /// the restriction gives it back).
    pub restricted: HashMap<String, ActionSet>,
    /// Graph element grants (chapter 43, R8), by (graph, relationship
    /// type?, label or type name).
    pub elements: HashMap<(ObjectId, bool, String), ActionSet>,
    /// Row-level security (chapter 29): the protected tables that bind
    /// this session and their policies (`super::rls::compile`).
    pub rls: std::sync::Arc<super::rls::SessionRls>,
}

/// The tenant-level actions cut from each database by partial revokes,
/// given each grantee's `*.*` actions and its restrictions.
#[must_use]
pub fn restrictions(
    per_grantee: &[(ActionSet, BTreeMap<String, ActionSet>)],
) -> HashMap<String, ActionSet> {
    let all = per_grantee
        .iter()
        .fold(ActionSet::EMPTY, |a, (t, _)| a.union(*t));
    let dbs: BTreeSet<&String> = per_grantee.iter().flat_map(|(_, r)| r.keys()).collect();
    let mut out = HashMap::new();
    for d in dbs {
        let reach = per_grantee.iter().fold(ActionSet::EMPTY, |a, (t, r)| {
            a.union(t.minus(r.get(d).copied().unwrap_or_default()))
        });
        let cut = all.minus(reach);
        if !cut.is_empty() {
            out.insert(d.clone(), cut);
        }
    }
    out
}

impl CompiledPrivileges {
    /// Everything (an unmigrated catalog).
    #[must_use]
    pub fn everything() -> Self {
        Self {
            everything: true,
            tenant: ActionSet(u64::MAX),
            ..Self::default()
        }
    }

    /// Compiles `user`'s privileges with `active` roles active; grants held
    /// by name stay by name.
    #[must_use]
    pub fn compile(state: &AuthzState, user: UserId, active: &[RoleId]) -> Self {
        Self::compile_at(state, &|_, _| None, user, active)
    }

    /// [`Self::compile`], resolving grants held by name with `relation`:
    /// the live table or view with a (database key, name key), if any.
    #[must_use]
    pub fn compile_at(
        state: &AuthzState,
        relation: &Relations<'_>,
        user: UserId,
        active: &[RoleId],
    ) -> Self {
        if !state.is_migrated() {
            return Self::everything();
        }
        let roles = state.closure(active);
        let mut c = Self {
            roles: roles.clone(),
            ..Self::default()
        };
        let grantees =
            std::iter::once(Grantee::User(user)).chain(roles.iter().map(|r| Grantee::Role(*r)));
        let mut per_grantee = Vec::new();
        for g in grantees {
            let mut own = (ActionSet::EMPTY, BTreeMap::new());
            for p in state.privileges_of(g) {
                match &p.target {
                    PrivTarget::Tenant => own.0 = own.0.union(p.actions),
                    PrivTarget::Restriction(d) => {
                        let e: &mut ActionSet = own.1.entry(d.clone()).or_default();
                        *e = e.union(p.actions);
                    }
                    _ => {}
                }
                match &p.target {
                    PrivTarget::Named {
                        db_key,
                        name_key,
                        column,
                    } => match (relation(db_key, name_key), column) {
                        (Some((db, object)), None) => {
                            c.add(&PrivTarget::Object { db, object }, p.actions);
                        }
                        (Some((db, object)), Some(column)) => c.add(
                            &PrivTarget::Column {
                                db,
                                object,
                                column: column.clone(),
                            },
                            p.actions,
                        ),
                        (None, _) => {
                            let e = c
                                .named
                                .entry((db_key.clone(), name_key.clone()))
                                .or_default();
                            *e = e.union(p.actions);
                        }
                    },
                    t => c.add(t, p.actions),
                }
            }
            per_grantee.push(own);
        }
        c.restricted = restrictions(&per_grantee);
        c
    }

    fn add(&mut self, t: &PrivTarget, a: ActionSet) {
        match t {
            PrivTarget::Tenant => self.tenant = self.tenant.union(a),
            PrivTarget::Database(n) => {
                let e = self.dbs.entry(n.clone()).or_default();
                *e = e.union(a);
            }
            PrivTarget::Object { db, object } => {
                let e = self.objects.entry(*object).or_default();
                *e = e.union(a);
                self.dbs_with_objects.insert(*db);
            }
            PrivTarget::Column { db, object, column } => {
                let e = self.col_any.entry(*object).or_default();
                *e = e.union(a);
                let e = self
                    .columns
                    .entry((*object, column.to_lowercase()))
                    .or_default();
                *e = e.union(a);
                self.dbs_with_objects.insert(*db);
            }
            PrivTarget::GraphElement {
                db,
                graph,
                rels,
                name,
            } => {
                let e = self
                    .elements
                    .entry((*graph, *rels, name.clone()))
                    .or_default();
                *e = e.union(a);
                self.dbs_with_objects.insert(*db);
            }
            // Resolved by compile_at.
            PrivTarget::Named { .. } | PrivTarget::Restriction(_) => {}
        }
    }

    /// The element grants on `graph`: (relationship type?, name, actions).
    #[must_use]
    pub fn graph_elements(&self, graph: ObjectId) -> Vec<(bool, String, ActionSet)> {
        let mut v: Vec<(bool, String, ActionSet)> = self
            .elements
            .iter()
            .filter(|((g, _, _), _)| *g == graph)
            .map(|((_, r, n), a)| (*r, n.clone(), *a))
            .collect();
        v.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
        v
    }

    /// The actions held on `t`, levels above included (columns excluded).
    #[must_use]
    pub fn actions_on(&self, t: Target<'_>) -> ActionSet {
        match t {
            Target::Tenant => self.tenant,
            Target::Database { db_key } => self
                .tenant
                .minus(self.restricted.get(db_key).copied().unwrap_or_default())
                .union(self.dbs.get(db_key).copied().unwrap_or_default()),
            Target::Named { db_key, name_key } => self
                .actions_on(Target::Database { db_key })
                .union(self.named_on(db_key, name_key)),
            Target::Object { db_key, object, .. } => self
                .actions_on(Target::Database { db_key })
                .union(self.objects.get(&object).copied().unwrap_or_default()),
        }
    }

    /// Whether `a` is allowed on `t`: for an object, a grant on any of its
    /// columns counts (the coarse SQL check).
    #[must_use]
    pub fn allows(&self, a: Action, t: Target<'_>) -> bool {
        if self.actions_on(t).has(a) {
            return true;
        }
        match t {
            Target::Object { object, .. } => self.col_any.get(&object).is_some_and(|c| c.has(a)),
            _ => false,
        }
    }

    fn named_on(&self, db_key: &str, name_key: &str) -> ActionSet {
        self.named
            .get(&(db_key.to_owned(), name_key.to_owned()))
            .copied()
            .unwrap_or_default()
    }

    /// Whether any privilege at all reaches database `db_key` (`USE`,
    /// `SHOW DATABASES`): at the tenant, the database, or any of its
    /// objects or columns, including grants held by name (MySQL lists a
    /// database for a `tables_priv` row of a missing table too).
    #[must_use]
    pub fn any_in_database(&self, db_key: &str, db: Option<DbId>) -> bool {
        if self.everything {
            return true;
        }
        !self.actions_on(Target::Database { db_key }).is_empty()
            || db.is_some_and(|d| self.dbs_with_objects.contains(&d))
            || self.named.keys().any(|(d, _)| d == db_key)
    }

    /// Whether any privilege reaches object `object` (`SHOW CREATE
    /// TABLE`, `DESCRIBE`).
    #[must_use]
    pub fn any_on_object(&self, db_key: &str, db: DbId, object: ObjectId) -> bool {
        !self
            .actions_on(Target::Object { db_key, db, object })
            .is_empty()
            || self.col_any.get(&object).is_some_and(|c| !c.is_empty())
    }

    /// Whether the user holds `SPRIG_TENANT_ADMIN` (here: on its own
    /// tenant; ancestors' administrators arrive with build step 17).
    #[must_use]
    pub fn tenant_admin(&self) -> bool {
        self.tenant.has(Action::TenantAdmin)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authz::store::{AuthzRows, EdgeRow, PrivRow, RoleRow};

    fn state() -> AuthzState {
        let p = |g, target, a: &[Action]| PrivRow {
            priv_id: 0,
            grantee: g,
            target,
            actions: ActionSet::of(a),
        };
        AuthzState::new(AuthzRows {
            roles: vec![
                RoleRow {
                    role_id: RoleId(1),
                    name: "reader".into(),
                    host: "%".into(),
                },
                RoleRow {
                    role_id: RoleId(2),
                    name: "writer".into(),
                    host: "%".into(),
                },
            ],
            edges: vec![
                EdgeRow {
                    grantee: Grantee::User(UserId(7)),
                    role: RoleId(2),
                    admin_option: false,
                    is_default: true,
                },
                EdgeRow {
                    grantee: Grantee::Role(RoleId(2)),
                    role: RoleId(1),
                    admin_option: false,
                    is_default: false,
                },
            ],
            privileges: vec![
                p(
                    Grantee::Role(RoleId(1)),
                    PrivTarget::Database("shop".into()),
                    &[Action::Select],
                ),
                p(
                    Grantee::Role(RoleId(2)),
                    PrivTarget::Object {
                        db: DbId(1),
                        object: ObjectId(10),
                    },
                    &[Action::Insert],
                ),
                p(
                    Grantee::User(UserId(7)),
                    PrivTarget::Column {
                        db: DbId(2),
                        object: ObjectId(20),
                        column: "Price".into(),
                    },
                    &[Action::Update],
                ),
            ],
        })
    }

    #[test]
    fn roles_count_only_when_active() {
        let s = state();
        let none = CompiledPrivileges::compile(&s, UserId(7), &[]);
        let shop = Target::Object {
            db_key: "shop",
            db: DbId(1),
            object: ObjectId(10),
        };
        assert!(!none.allows(Action::Select, shop));
        let c = CompiledPrivileges::compile(&s, UserId(7), &s.default_roles(UserId(7)));
        assert!(c.allows(Action::Select, shop), "through writer -> reader");
        assert!(c.allows(Action::Insert, shop));
        assert!(!c.allows(Action::Delete, shop));
        assert!(c.any_in_database("shop", Some(DbId(1))));
        assert!(!c.any_in_database("crm", Some(DbId(3))));
        // A column grant reaches its database and passes the coarse check.
        let other = Target::Object {
            db_key: "other",
            db: DbId(2),
            object: ObjectId(20),
        };
        assert!(c.any_in_database("other", Some(DbId(2))));
        assert!(c.allows(Action::Update, other));
        assert!(!c.allows(Action::Select, other));
        assert!(c.columns.contains_key(&(ObjectId(20), "price".into())));
    }

    #[test]
    fn cycles_are_detected_and_unmigrated_allows_all() {
        let s = state();
        assert!(s.would_cycle(RoleId(1), RoleId(1)));
        assert!(s.would_cycle(RoleId(2), RoleId(1)));
        assert!(!s.would_cycle(RoleId(1), RoleId(2)));
        let c = CompiledPrivileges::compile(&AuthzState::unmigrated(), UserId(1), &[]);
        assert!(c.allows(Action::Drop, Target::Tenant));
    }

    #[test]
    fn graph_element_grants_compile_by_graph() {
        let row = |rels, name: &str| PrivRow {
            priv_id: 0,
            grantee: Grantee::User(UserId(9)),
            target: PrivTarget::GraphElement {
                db: DbId(2),
                graph: ObjectId(77),
                rels,
                name: name.into(),
            },
            actions: ActionSet::of(&[Action::Select]),
        };
        let st = AuthzState::new(AuthzRows {
            roles: vec![],
            edges: vec![],
            privileges: vec![row(false, "Person"), row(true, "KNOWS")],
        });
        let c = CompiledPrivileges::compile(&st, UserId(9), &[]);
        let e = c.graph_elements(ObjectId(77));
        assert_eq!(e.len(), 2);
        assert!(
            e.iter()
                .any(|(r, n, a)| !r && n == "Person" && a.has(Action::Select))
        );
        assert!(e.iter().any(|(r, n, _)| *r && n == "KNOWS"));
        assert!(c.graph_elements(ObjectId(78)).is_empty());
        // The database becomes visible, the graph itself isn't readable.
        assert!(c.dbs_with_objects.contains(&DbId(2)));
        assert!(!c.allows(
            Action::Select,
            Target::Object {
                db_key: "g",
                db: DbId(2),
                object: ObjectId(77)
            }
        ));
    }
}
