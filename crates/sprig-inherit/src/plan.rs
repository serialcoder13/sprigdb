//! What a statement needs from the session, and the pieces every plan
//! shares: name resolution, a space's [`Chain`] of levels (chapter 42,
//! `view(o, s)`), and the edge from the parent tenant.

use std::sync::Arc;

use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::{database_key, object_key, shard_schemas};
use sprig_catalog::store::{DbState, Namespace, ObjectKind};
use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_sql::lex::LexOptions;

use crate::copy::{CopyKey, CopyStates, UPSTREAM, read_sql};
use crate::grammar::{self, Name};
use crate::model::{InheritState, InheritableObject, MergeMode, ROOT_SPACE, Share, SpaceId};
use crate::store::MODULE;
use crate::view::{Chain, Edge, Level, Top, q};

/// Settings, resolved for the statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// `inherit.parent_delete = follow`.
    pub parent_delete_follow: bool,
    /// `inherit.override = record`.
    pub override_record: bool,
    /// `inherit.max_depth`.
    pub max_depth: usize,
    /// `inherit.read_through_max_depth`.
    pub read_through_max_depth: usize,
    /// `tenant.max_depth`.
    pub tenant_max_depth: usize,
    /// `inherit.merge` is `federated` (not built).
    pub merge_unbuilt: bool,
    /// `inherit.merge`: `None` for `auto`.
    pub merge: Option<MergeMode>,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            parent_delete_follow: false,
            override_record: false,
            max_depth: 16,
            read_through_max_depth: 4,
            tenant_max_depth: 64,
            merge_unbuilt: false,
            merge: None,
        }
    }
}

/// The session's rights the extension statements check (chapter 29's
/// statements are checked by the gateway as usual).
pub trait Privileges: Send + Sync {
    /// May create and drop spaces and set home spaces: `CREATE USER` or
    /// `SPRIG_TENANT_ADMIN` on the tenant.
    fn space_admin(&self) -> bool;
    /// Holds `ALTER` on the object (creating, changing and dropping its
    /// shares; chapter 29's `SHARE` privilege is not built).
    fn alter(&self, db_name: &str, db: DbId, object: ObjectId) -> bool;
}

/// Everything allowed (tests and unrestricted principals).
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

impl Privileges for AllowAll {
    fn space_admin(&self) -> bool {
        true
    }
    fn alter(&self, _: &str, _: DbId, _: ObjectId) -> bool {
        true
    }
}

/// What a statement needs.
pub struct Ctx<'a> {
    /// The tenant's snapshot.
    pub cat: &'a TenantCatalog,
    /// Its inheritance state.
    pub state: &'a InheritState,
    /// The tenant's name (its root space's name).
    pub tenant_name: &'a str,
    /// The ancestor tenants' snapshots, root first, parent last (empty
    /// when not needed).
    pub ancestors: &'a [Arc<TenantCatalog>],
    /// The session's lexing options.
    pub lex: LexOptions,
    /// `lower_case_table_names`.
    pub lctn: bool,
    /// The current logical database.
    pub current_db: Option<String>,
    /// The session's space.
    pub space: SpaceId,
    /// The session's user.
    pub user: Option<u32>,
    /// Settings.
    pub limits: Limits,
    /// Rights.
    pub privileges: &'a dyn Privileges,
    /// Lowercase names of typed and hierarchical tables (chapter 32).
    pub typed: &'a [String],
    /// The materialized copies that are serving ([`crate::copy`]).
    pub copies: &'a CopyStates,
    /// The statement is chapter 32's own (a type plan's): its DDL is the
    /// type layer's business, and its typed tables are planned already.
    pub from_types: bool,
}

/// Where [`Ctx::chain_with`] stops climbing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cut {
    /// At the nearest materialized edge whose copy is serving (reads and
    /// writes).
    Serving,
    /// At the nearest materialized edge, serving or not (the read-through
    /// depth since it).
    Declared,
    /// Never: every level up to the owner (the share chain's length).
    Never,
}

/// A reference to an inheritable object.
#[derive(Debug, Clone)]
pub struct Resolved {
    /// The database.
    pub db: DbId,
    /// Its logical name.
    pub db_name: String,
    /// The object.
    pub obj: Arc<InheritableObject>,
    /// The layer table as the statement should name it.
    pub layer: String,
    /// The database prefix used (`` `shop`. `` or empty).
    pub prefix: String,
    /// Columns its merged view computes ([`Ctx::derived`]).
    pub derived: Vec<(String, String)>,
}

/// The parent tenant's edge into a tenant, for one object.
#[derive(Debug, Clone)]
pub struct UpEdge {
    /// The parent's share.
    pub share: Arc<Share>,
    /// The parent's object.
    pub parent_obj: Arc<InheritableObject>,
    /// The parent tenant.
    pub parent: TenantId,
    /// The columns the share view exposes.
    pub columns: Vec<String>,
    /// The child's narrowing.
    pub take: Option<String>,
}

pub(crate) fn not_found(what: &str) -> SprigError {
    SprigError::new(SprigCode::SpaceNotFound, what.to_owned())
}

/// The inheritance state in a snapshot.
#[must_use]
pub fn state_of(cat: &TenantCatalog) -> Option<&InheritState> {
    cat.ext.get::<InheritState>(MODULE)
}

/// The single physical schema of `db_name` in `cat`.
///
/// # Errors
/// 39889 when the database is sharded (cross-tenant edges need one
/// schema per side in R6); `Internal` when it has no placement.
pub fn physical_schema(cat: &TenantCatalog, db_name: &str, lctn: bool) -> Result<String> {
    let d = cat
        .database(&database_key(db_name, lctn))
        .filter(|d| d.state == DbState::Active)
        .ok_or_else(|| SprigError::internal(format!("no database {db_name}")))?;
    let s = shard_schemas(cat.tenant, d);
    match s.as_slice() {
        [one] => Ok(one.to_string()),
        [] => Err(SprigError::internal(format!(
            "database {db_name} has no placement"
        ))),
        _ => Err(SprigError::new(
            SprigCode::ShareResidencyConflict,
            "a cross-tenant edge needs single-shard databases: materialized edges are not built",
        )),
    }
}

/// The edge carrying the object `db_name.table` from `parent`'s root into
/// `child` (whose snapshot is `child_cat`), if any.
///
/// # Errors
/// 39892 when the two objects' keys differ.
pub fn tenant_edge(
    parent: &TenantCatalog,
    child_cat: &TenantCatalog,
    db_name: &str,
    table: &str,
    lctn: bool,
) -> Result<Option<UpEdge>> {
    let Some(ps) = state_of(parent) else {
        return Ok(None);
    };
    let Some(pd) = parent.database(&database_key(db_name, lctn)) else {
        return Ok(None);
    };
    let Some(po) = ps.object_named(pd.db_id, table, lctn) else {
        return Ok(None);
    };
    let Some(share) = ps.tenant_edge(po.object_id, child_cat.tenant) else {
        return Ok(None);
    };
    let Some(cs) = state_of(child_cat) else {
        return Ok(None);
    };
    let Some(cd) = child_cat.database(&database_key(db_name, lctn)) else {
        return Ok(None);
    };
    let Some(co) = cs.object_named(cd.db_id, table, lctn) else {
        return Ok(None);
    };
    let same_key = po.key.len() == co.key.len()
        && po
            .key
            .iter()
            .zip(&co.key)
            .all(|(a, b)| a.eq_ignore_ascii_case(b));
    if !same_key {
        return Err(SprigError::new(
            SprigCode::InheritSchemaMismatch,
            format!(
                "{db_name}.{table} has key ({}) here but ({}) in the parent tenant",
                co.key.join(", "),
                po.key.join(", ")
            ),
        ));
    }
    let columns = po
        .columns
        .iter()
        .filter(|c| share.shares_column(po, c))
        .cloned()
        .collect();
    Ok(Some(UpEdge {
        share: Arc::clone(share),
        parent_obj: Arc::clone(po),
        parent: parent.tenant,
        columns,
        take: cs.take(share.id).and_then(|t| t.take.clone()),
    }))
}

impl Ctx<'_> {
    /// The tenant's types (chapter 32), if any.
    #[must_use]
    pub fn types(&self) -> Option<&sprig_types::TypesState> {
        self.cat
            .ext
            .get::<sprig_types::TypesState>(sprig_types::store::MODULE)
    }

    /// `table`'s type tree: its root's name and every table of the tree.
    #[must_use]
    pub fn type_tree(&self, db: DbId, table: &str) -> Option<(String, Vec<String>)> {
        sprig_types::type_tree(self.types()?, db, table, self.lctn)
    }

    /// The columns a merged view of `o` computes rather than reads: a
    /// discriminated root's `_type` and `_type_path`, by this tenant's
    /// types ([`sprig_types::derived_columns`]).
    #[must_use]
    pub fn derived(&self, o: &InheritableObject) -> Vec<(String, String)> {
        if o.type_root.is_none() {
            return Vec::new();
        }
        self.types().map_or_else(Vec::new, |t| {
            sprig_types::derived_columns(t, o.db, &o.table, self.lctn, self.lex.no_backslash_escapes)
        })
    }

    /// A database's logical name.
    #[must_use]
    pub fn db_name(&self, db: DbId) -> Option<String> {
        self.cat.databases.get(db).map(|d| d.name.clone())
    }

    /// The database a name refers to.
    #[must_use]
    pub fn db_of(&self, name: &Name) -> Option<(DbId, String)> {
        let n = name.db.clone().or_else(|| self.current_db.clone())?;
        let d = self.cat.database(&database_key(&n, self.lctn))?;
        Some((d.db_id, d.name.clone()))
    }

    /// The catalog object `name` refers to, if it is a table.
    #[must_use]
    pub fn object_id(&self, name: &Name) -> Option<(DbId, String, ObjectId)> {
        let (db, db_name) = self.db_of(name)?;
        let key = object_key(ObjectKind::Table, &name.name, self.lctn);
        let o = self.cat.object(db, Namespace::Relation, &key)?;
        Some((db, db_name, o.object_id))
    }

    /// The inheritable object `name` refers to.
    ///
    /// # Errors
    /// None today; reserved for name errors.
    pub fn resolve(&self, name: &Name) -> Result<Option<Resolved>> {
        let Some((db, db_name)) = self.db_of(name) else {
            return Ok(None);
        };
        let Some(obj) = self.state.object_named(db, &name.name, self.lctn) else {
            return Ok(None);
        };
        let prefix = match &name.db {
            Some(d) => format!("{}.", q(d)),
            None => String::new(),
        };
        Ok(Some(Resolved {
            db,
            db_name,
            layer: format!("{prefix}{}", q(&name.name)),
            prefix,
            derived: self.derived(obj),
            obj: Arc::clone(obj),
        }))
    }

    /// The parent tenant's edge into this tenant for `o`.
    ///
    /// # Errors
    /// 39892.
    pub fn up_edge(&self, o: &InheritableObject) -> Result<Option<UpEdge>> {
        let Some(parent) = self.ancestors.last() else {
            return Ok(None);
        };
        let Some(db_name) = self.db_name(o.db) else {
            return Ok(None);
        };
        tenant_edge(parent, self.cat, &db_name, &o.table, self.lctn)
    }

    /// The name of space `s` (for origins and messages).
    #[must_use]
    pub fn space_name(&self, s: SpaceId) -> String {
        self.state
            .space(s, self.tenant_name)
            .map_or_else(|| s.to_string(), |x| x.name)
    }

    /// The levels of `space`'s merged view of `o` (`prefix`: how the
    /// statement names the object's database), up to the nearest serving
    /// copy.
    ///
    /// # Errors
    /// 39880 for an unknown space; 39892.
    pub fn chain(&self, o: &InheritableObject, space: SpaceId, prefix: &str) -> Result<Chain> {
        self.chain_with(o, space, prefix, Cut::Serving)
    }

    #[allow(clippy::too_many_arguments)] // the edge's parts, as the two callers have them
    fn copy_top(
        &self,
        o: &InheritableObject,
        share: &Share,
        key: u64,
        columns: Vec<String>,
        prefix: &str,
        origin: String,
        cut: Cut,
    ) -> Option<Top> {
        let ok = share.mode == MergeMode::Materialized
            && match cut {
                Cut::Never => false,
                Cut::Declared => true,
                Cut::Serving => self
                    .copies
                    .ready(
                        CopyKey {
                            tenant: self.cat.tenant,
                            object: o.object_id,
                            share: key,
                        },
                        o.version,
                        share.version,
                    )
                    .is_some(),
            };
        ok.then(|| {
            let (from, filter) = read_sql(prefix, o.object_id, key, share.filter.as_deref());
            Top::Upstream {
                from,
                columns,
                take: share.take.clone(),
                origin,
                filter: Some(filter),
            }
        })
    }

    /// [`Self::chain`], stopping as `cut` says.
    ///
    /// # Errors
    /// 39880 for an unknown space; 39892.
    pub fn chain_with(
        &self,
        o: &InheritableObject,
        space: SpaceId,
        prefix: &str,
        cut: Cut,
    ) -> Result<Chain> {
        let mut levels = Vec::new();
        let mut cur = space;
        let mut top = None;
        loop {
            let sp = self
                .state
                .space(cur, self.tenant_name)
                .ok_or_else(|| not_found(&format!("space {cur} doesn't exist")))?;
            let Some(sh) = self.state.edge(o.object_id, cur) else {
                levels.push(Level {
                    space: cur,
                    name: sp.name,
                    edge: None,
                });
                break;
            };
            let parent = sp.parent.unwrap_or(ROOT_SPACE);
            let shared: Vec<String> = o
                .columns
                .iter()
                .filter(|c| sh.shares_column(o, c))
                .cloned()
                .collect();
            if let Some(t) =
                self.copy_top(o, sh, sh.id, shared, prefix, self.space_name(parent), cut)
            {
                levels.push(Level {
                    space: cur,
                    name: sp.name,
                    edge: None,
                });
                top = Some(t);
                break;
            }
            levels.push(Level {
                space: cur,
                name: sp.name,
                edge: Some(Edge {
                    filter: sh.filter.clone(),
                    take: sh.take.clone(),
                    columns: sh.columns.clone(),
                }),
            });
            cur = parent;
            if levels.len() > 4096 {
                return Err(SprigError::internal("a cycle in the space tree"));
            }
        }
        levels.reverse();
        if let Some(top) = top {
            return Ok(Chain { top, levels });
        }
        let top = if levels[0].space == ROOT_SPACE
            && let Some(up) = self.up_edge(o)?
        {
            let origin = self
                .cat
                .ancestry
                .last()
                .map_or_else(|| up.parent.to_string(), |a| a.name.clone());
            // A copy holds the child's columns only.
            let mut shared = up.share.as_ref().clone();
            shared.take.clone_from(&up.take);
            let cols: Vec<String> = up
                .columns
                .iter()
                .filter(|c| o.column(c).is_some())
                .cloned()
                .collect();
            match self.copy_top(o, &shared, UPSTREAM, cols, prefix, origin.clone(), cut) {
                Some(t) => t,
                None => Top::Upstream {
                    from: format!("{prefix}{}", q(&format!("__sprig_share_{}", up.share.id))),
                    columns: up.columns,
                    take: up.take,
                    origin,
                    filter: None,
                },
            }
        } else {
            Top::Base
        };
        Ok(Chain { top, levels })
    }

    /// Edges between the space owning a record and `space`, across
    /// tenants (the share chain `inherit.max_depth` bounds).
    ///
    /// # Errors
    /// 39892.
    pub fn chain_edges(&self, o: &InheritableObject, space: SpaceId) -> Result<usize> {
        let c = self.chain_with(o, space, "", Cut::Never)?;
        let mut n = c.levels.len() - 1;
        if matches!(c.top, Top::Upstream { .. }) {
            // The parent tenant's own chain into its root.
            n += 1;
            let mut lineage: Vec<&TenantCatalog> =
                self.ancestors.iter().map(AsRef::as_ref).collect();
            lineage.push(self.cat);
            let db_name = self.db_name(o.db).unwrap_or_default();
            let mut j = lineage.len() - 1;
            while j >= 2 {
                if tenant_edge(
                    lineage[j - 2],
                    lineage[j - 1],
                    &db_name,
                    &o.table,
                    self.lctn,
                )?
                .is_none()
                {
                    break;
                }
                n += 1;
                j -= 1;
            }
        }
        Ok(n)
    }
}

/// Whether `sql` names an inheritable object of the tenant (by its table
/// name, in any database).
#[must_use]
pub fn touches(sql: &str, ctx: &Ctx<'_>) -> bool {
    let names: Vec<String> = ctx
        .state
        .objects()
        .map(|o| o.table.to_lowercase())
        .collect();
    if names.is_empty() {
        return false;
    }
    grammar::identifiers(sql.as_bytes(), &ctx.lex)
        .iter()
        .any(|i| names.contains(&i.to_lowercase()))
}

/// Whether `sql` names a typed or hierarchical table.
#[must_use]
pub fn touches_typed(sql: &str, ctx: &Ctx<'_>) -> bool {
    !ctx.typed.is_empty()
        && grammar::identifiers(sql.as_bytes(), &ctx.lex)
            .iter()
            .any(|i| ctx.typed.contains(&i.to_lowercase()))
}
