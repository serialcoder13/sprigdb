//! A tenant's layout from the catalog (chapter 15, "Catalog tables") and
//! the shard maps (chapter 15, "Shard maps in etcd"): what the gateway
//! builds the router's [`StaticTopology`] from.
//!
//! The inputs are plain data, so the router depends on neither the catalog
//! cache nor etcd: per table, its logical database, the schema chapter
//! 07's rewrite names it in, and its membership (a column member of a
//! sharded group, a parent member, or a reference table with the clusters
//! holding a copy); per sharded group, its key spec and its current
//! [`ShardMap`]. Each `(group, database)` pair becomes one [`Group`]
//! whose shards are `t<tenant>_d<db>_g<group>_s<shard>` on the map's
//! clusters, with each range's epoch and hold.
//!
//! [`TableShape`] reads a table's `SHOW CREATE TABLE` text (the catalog's
//! `schema_versions.create_sql`): the key column's position among the
//! visible columns (`INSERT` without a column list) and whether every
//! unique key holds the key column (upserts and `REPLACE` per shard).

use std::collections::HashMap;
use std::sync::Arc;

use sprig_common::ids::{DbId, GroupId, TenantId};
use sprig_shard::{KeySpec, ShardMap};

use sprig_shard::lookup::{
    LookupColumn, LookupId, LookupIndex, LookupOrigin, LookupState, MapPlacement,
};

use super::{
    AutoInc, Group, KeyClass, LookupRef, ParentLink, ShardLoc, StaticTopology, TableEntry,
    TableKind, even_ranges,
};

/// A `lookup_indexes` row of a table (chapter 19), with its lookup group's
/// map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogLookup {
    /// `lookup_id`.
    pub id: u64,
    /// The owner table's object id.
    pub owner: u64,
    /// `index_name`.
    pub index_name: String,
    /// `columns`, in key order.
    pub columns: Vec<String>,
    /// `is_unique`.
    pub unique: bool,
    /// `origin`.
    pub origin: LookupOrigin,
    /// `state`.
    pub state: LookupState,
    /// The lookup group's current map (`None` while it isn't cached:
    /// the lookup is then a gap, and writes to the table refuse).
    pub map: Option<Arc<ShardMap>>,
}

/// A sharded group: its key and its current map.
#[derive(Debug, Clone)]
pub struct CatalogGroup {
    /// The group id.
    pub id: u32,
    /// The key spec (`table_groups.key_spec`).
    pub spec: KeySpec,
    /// The current map.
    pub map: Arc<ShardMap>,
}

/// How a table takes part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Membership {
    /// A member of a sharded group.
    Sharded {
        /// The group.
        group: u32,
        /// The key columns (a column member), `None` for a parent member.
        key_columns: Option<Vec<String>>,
        /// `SHOW CREATE TABLE` text, when the catalog has it.
        create_sql: Option<String>,
        /// Its lookup indexes.
        lookups: Vec<CatalogLookup>,
        /// Its `rk_state` is `filling` or later: writes carry `_sprig_rk`
        /// (chapter 15).
        fill_rk: bool,
        /// A parent member's `parent_fk`: the constraint (in `create_sql`)
        /// that reaches its parent.
        parent_fk: Option<String>,
    },
    /// A reference table: a full copy on each listed cluster.
    Reference {
        /// Clusters holding a copy that serves reads (the primary first).
        clusters: Vec<String>,
    },
}

/// One table of the layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogTable {
    /// Its object id (`objects.object_id`): an `AUTO_INCREMENT` column's
    /// `sequences` row has it as `seq_id` (chapter 19). 0 when unknown.
    pub object: u64,
    /// Its logical database.
    pub db: u32,
    /// The schema chapter 07's rewrite names the database by (its default
    /// group's first shard, `t<tenant>_d<db>_g<group>_s<shard>`).
    pub default_schema: String,
    /// The table name.
    pub name: String,
    /// Its membership.
    pub member: Membership,
}

/// What `SHOW CREATE TABLE` says about a table's columns and unique keys.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableShape {
    /// Visible columns, in order, lowercase.
    pub visible: Vec<String>,
    /// The primary key and every unique key, as lowercase column lists.
    pub unique_keys: Vec<Vec<String>>,
    /// The primary key's columns, lowercase.
    pub primary: Vec<String>,
    /// Every column's definition after its name (`varchar(40) COLLATE
    /// utf8mb4_general_ci DEFAULT NULL`), by lowercase name.
    pub types: HashMap<String, String>,
    /// The table's default collation (`COLLATE=` of the table options).
    pub collation: Option<String>,
    /// Its foreign keys.
    pub foreign_keys: Vec<ForeignKeyShape>,
}

/// A `CONSTRAINT ... FOREIGN KEY ... REFERENCES ...` line of `SHOW CREATE
/// TABLE`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ForeignKeyShape {
    /// The constraint's name.
    pub name: String,
    /// The child's columns, lowercase.
    pub columns: Vec<String>,
    /// The referenced table, lowercase (without a schema).
    pub ref_table: String,
    /// The referenced columns, lowercase.
    pub ref_columns: Vec<String>,
}

impl ForeignKeyShape {
    /// Reads `CONSTRAINT `n` FOREIGN KEY (`a`, ...) REFERENCES [`db`.]`t`
    /// (`b`, ...) ...`.
    fn parse(line: &str) -> Option<Self> {
        let rest = line.strip_prefix("CONSTRAINT `")?;
        let (name, rest) = split_ident(rest)?;
        let upper = rest.to_ascii_uppercase();
        let fk = upper.find("FOREIGN KEY")?;
        let refs = upper.find(" REFERENCES ")?;
        let columns = key_columns(&rest[fk..refs])?;
        let mut tail = rest[refs + " REFERENCES ".len()..].trim_start();
        let mut table = String::new();
        while let Some(t) = tail.strip_prefix('`') {
            let (n, after) = split_ident(t)?;
            table = n;
            tail = after;
            match tail.strip_prefix('.') {
                Some(more) => tail = more,
                None => break,
            }
        }
        let ref_columns = key_columns(tail)?;
        (!table.is_empty() && columns.len() == ref_columns.len()).then(|| Self {
            name,
            columns,
            ref_table: table.to_ascii_lowercase(),
            ref_columns,
        })
    }
}

impl TableShape {
    /// Reads `SHOW CREATE TABLE` output (MariaDB's layout: one item per
    /// line, columns first). Lines it doesn't recognize are skipped.
    #[must_use]
    pub fn parse(create_sql: &str) -> Self {
        let mut shape = Self::default();
        for line in create_sql.lines().skip(1) {
            let l = line.trim().trim_end_matches(',');
            if let Some(rest) = l.strip_prefix('`') {
                let Some((name, tail)) = split_ident(rest) else {
                    continue;
                };
                let upper = tail.to_ascii_uppercase();
                if !upper.contains(" INVISIBLE") {
                    shape.visible.push(name.to_ascii_lowercase());
                }
                shape
                    .types
                    .insert(name.to_ascii_lowercase(), tail.trim().to_owned());
                continue;
            }
            let upper = l.to_ascii_uppercase();
            if upper.starts_with("CONSTRAINT ") {
                shape.foreign_keys.extend(ForeignKeyShape::parse(l));
                continue;
            }
            if l.starts_with(')') {
                shape.collation = option_value(l, "COLLATE=")
                    .or_else(|| option_value(l, "DEFAULT COLLATE="))
                    .map(str::to_owned);
                continue;
            }
            let cols = if upper.starts_with("PRIMARY KEY") || upper.starts_with("UNIQUE KEY") {
                key_columns(l)
            } else {
                None
            };
            if let Some(c) = cols {
                if upper.starts_with("PRIMARY KEY") {
                    shape.primary.clone_from(&c);
                }
                shape.unique_keys.push(c);
            }
        }
        shape
    }

    /// The position of `col` among the visible columns.
    #[must_use]
    pub fn ordinal(&self, col: &str) -> Option<usize> {
        let c = col.to_ascii_lowercase();
        self.visible.iter().position(|v| *v == c)
    }

    /// Column `col`'s type and its key class (chapter 15's classes:
    /// integers and decimals, strings under their collation, binary
    /// strings); `None` for other types or an unknown collation.
    #[must_use]
    pub fn column_class(&self, col: &str) -> Option<(String, sprig_shard::KeyClass)> {
        let def = self.types.get(&col.to_ascii_lowercase())?;
        let lower = def.to_ascii_lowercase();
        let base: String = lower
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        let class = match base.as_str() {
            "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "decimal"
            | "numeric" => sprig_shard::KeyClass::Numeric,
            "char" | "varchar" => {
                let coll = option_word(def, " COLLATE ").or(self.collation.as_deref())?;
                sprig_shard::KeyClass::String {
                    collation: sprig_shard::collation::collation_by_name(coll)?.id,
                }
            }
            "binary" | "varbinary" => sprig_shard::KeyClass::Binary,
            _ => return None,
        };
        // The type alone (up to its attributes), for lookup tables' DDL.
        let mut sql_type = def.clone();
        for cut in [
            " NOT NULL",
            " DEFAULT ",
            " NULL",
            " AUTO_INCREMENT",
            " COMMENT ",
        ] {
            if let Some(i) = sql_type.to_ascii_uppercase().find(cut) {
                sql_type.truncate(i);
            }
        }
        Some((sql_type.trim().to_owned(), class))
    }

    /// The `AUTO_INCREMENT` column, lowercase, if the table has one.
    #[must_use]
    pub fn auto_increment(&self) -> Option<String> {
        self.visible
            .iter()
            .chain(self.types.keys().filter(|k| !self.visible.contains(k)))
            .find(|c| {
                self.types.get(*c).is_some_and(|d| {
                    let def = d.to_ascii_uppercase();
                    // The attribute, not a word inside a COMMENT or DEFAULT.
                    let head = def.split(" COMMENT '").next().unwrap_or("");
                    head.split_whitespace().any(|w| w == "AUTO_INCREMENT")
                })
            })
            .cloned()
    }

    /// Whether every unique key holds `col`.
    #[must_use]
    pub fn unique_keys_hold(&self, col: &str) -> bool {
        let c = col.to_ascii_lowercase();
        // Chapter 33's key-value tables: `_sprig_dk` is a function of the
        // key `k` (the hash tag or the first segments), so a unique key
        // holding `k` holds the distribution key too (R6 round 4).
        let dk_of_k = c == "_sprig_dk";
        self.unique_keys
            .iter()
            .all(|k| k.contains(&c) || (dk_of_k && k.iter().any(|x| x == "k")))
    }
}

/// `name` of a backquoted identifier whose opening quote was cut, and the
/// text after its closing quote.
fn split_ident(s: &str) -> Option<(String, &str)> {
    let mut name = String::new();
    let mut chars = s.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if c == '`' {
            if chars.peek().map(|(_, n)| *n) == Some('`') {
                chars.next();
                name.push('`');
            } else {
                return Some((name, &s[i + 1..]));
            }
        } else {
            name.push(c);
        }
    }
    None
}

/// The value of table option `opt` (`COLLATE=`) in the closing line.
fn option_value<'a>(line: &'a str, opt: &str) -> Option<&'a str> {
    let i = line.to_ascii_uppercase().find(opt)?;
    line[i + opt.len()..].split_whitespace().next()
}

/// The word after `kw` (` COLLATE `) in a column definition.
fn option_word<'a>(def: &'a str, kw: &str) -> Option<&'a str> {
    let i = def.to_ascii_uppercase().find(kw)?;
    def[i + kw.len()..]
        .split_whitespace()
        .next()
        .map(|w| w.trim_matches(|c| c == '\'' || c == '`' || c == ','))
}

/// The columns of `PRIMARY KEY (...)` or `UNIQUE KEY `n` (...)`; prefix
/// lengths (`(10)`) are dropped.
fn key_columns(line: &str) -> Option<Vec<String>> {
    let open = line.find(" (").map(|i| i + 2).or_else(|| line.find('('))?;
    let body = &line[open..];
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(i) = rest.find('`') {
        let (name, tail) = split_ident(&rest[i + 1..])?;
        out.push(name.to_ascii_lowercase());
        rest = tail;
        // Skip a prefix length and the separator.
        let t = rest.trim_start();
        if let Some(after) = t.strip_prefix('(') {
            rest = after.split_once(')').map_or("", |(_, r)| r);
        }
        if rest.trim_start().starts_with(')') {
            break;
        }
    }
    (!out.is_empty()).then_some(out)
}

/// The router's key class for a one-part key spec; `None` for keys the
/// router doesn't route by a literal (composite, UUID, temporal): those
/// tables scatter.
fn router_class(spec: &KeySpec) -> Option<KeyClass> {
    match spec.parts.as_slice() {
        [sprig_shard::KeyClass::Numeric] => Some(KeyClass::Int),
        [sprig_shard::KeyClass::String { collation }] => Some(KeyClass::Str {
            collation: collation.0,
        }),
        [sprig_shard::KeyClass::Binary] => Some(KeyClass::Binary),
        _ => None,
    }
}

/// Builds the router's view of tenant `tenant`: sharded tables of `groups`
/// (a member of a group without a map is left out, so the caller can tell
/// the session to wait or fail) and reference tables.
#[must_use]
pub fn build(tenant: u64, groups: &[CatalogGroup], tables: &[CatalogTable]) -> StaticTopology {
    build_with_fks(tenant, groups, tables, &[])
}

/// [`build`], with the tenant's `foreign_keys` rows (chapter 19): the keys
/// the gateway keeps (`gateway` and `reference`, `validating` or
/// `enforced`) between two tables it knows become each database's
/// [`crate::fk::FkGraph`]. A key with an end the router doesn't know (an
/// unsharded table) is left to MariaDB.
#[must_use]
pub fn build_with_fks(
    tenant: u64,
    groups: &[CatalogGroup],
    tables: &[CatalogTable],
    fks: &[sprig_shard::fk::ForeignKey],
) -> StaticTopology {
    let by_id: HashMap<u32, &CatalogGroup> = groups.iter().map(|g| (g.id, g)).collect();
    let mut built: HashMap<(u32, u32), Arc<Group>> = HashMap::new();
    let mut entries: Vec<(String, TableEntry)> = Vec::new();
    // Each entry's unique keys (parents' keys decide co-location).
    let mut uniques: Vec<Vec<Vec<String>>> = Vec::new();
    // Each entry's object id and shape (foreign keys).
    let mut shapes: Vec<(u64, Option<TableShape>)> = Vec::new();
    for t in tables {
        let name = t.name.to_ascii_lowercase();
        let mut lookups = Vec::new();
        let mut lookup_gap = false;
        let mut columns = Vec::new();
        let mut fill = false;
        let mut parent = None;
        let mut unique_keys = Vec::new();
        let mut auto_inc = None;
        let mut primary = Vec::new();
        let mut primary_classes = Vec::new();
        let (kind, group) = match &t.member {
            Membership::Sharded {
                group,
                key_columns,
                create_sql,
                lookups: catalog_lookups,
                fill_rk,
                parent_fk,
            } => {
                let Some(g) = by_id.get(group) else {
                    continue;
                };
                let routable = router_class(&g.spec);
                let key = match (key_columns.as_deref(), routable) {
                    (Some([k]), Some(_)) => k.to_ascii_lowercase(),
                    _ => String::new(),
                };
                let shape = create_sql.as_deref().map(TableShape::parse);
                shapes.push((t.object, shape.clone()));
                if let Some(s) = &shape {
                    columns.clone_from(&s.visible);
                    unique_keys.clone_from(&s.unique_keys);
                    let classes: Option<Vec<crate::fk::ValClass>> = s
                        .primary
                        .iter()
                        .map(|c| {
                            s.column_class(c).map(|(_, k)| match k {
                                sprig_shard::KeyClass::Numeric => crate::fk::ValClass::Num,
                                sprig_shard::KeyClass::String { .. } => crate::fk::ValClass::Str,
                                sprig_shard::KeyClass::Binary => crate::fk::ValClass::Bin,
                                _ => crate::fk::ValClass::Text,
                            })
                        })
                        .collect();
                    if let Some(c) = classes {
                        primary.clone_from(&s.primary);
                        primary_classes = c;
                    }
                }
                fill = *fill_rk;
                // Chapter 19: a sharded member's AUTO_INCREMENT column
                // takes ID blocks, whatever the group's shard count.
                if t.object != 0 {
                    auto_inc = shape.as_ref().and_then(|s| {
                        let column = s.auto_increment()?;
                        Some(AutoInc {
                            ordinal: s.ordinal(&column),
                            column,
                            seq: t.object,
                        })
                    });
                }
                let key_ordinal = shape
                    .as_ref()
                    .and_then(|s| (!key.is_empty()).then(|| s.ordinal(&key)).flatten());
                let upsert_safe =
                    !key.is_empty() && shape.as_ref().is_some_and(|s| s.unique_keys_hold(&key));
                if key.is_empty()
                    && key_columns.is_none()
                    && let (Some(fk), Some(s)) = (parent_fk.as_deref(), shape.as_ref())
                {
                    parent = parent_link(fk, s);
                }
                let group = built
                    .entry((g.id, t.db))
                    .or_insert_with(|| Arc::new(group_of(tenant, t.db, g, routable)))
                    .clone();
                for l in catalog_lookups {
                    match lookup_ref(tenant, t, &name, l, shape.as_ref(), g) {
                        Some(r) => lookups.push(Arc::new(r)),
                        // A maintained lookup the router can't follow:
                        // writes to the table are refused, never left
                        // unmaintained.
                        None if l.state.maintained() => lookup_gap = true,
                        None => {}
                    }
                }
                (
                    TableKind::Sharded {
                        key,
                        key_ordinal,
                        upsert_safe,
                    },
                    group,
                )
            }
            Membership::Reference { clusters } => {
                shapes.push((t.object, None));
                let schema = sprig_shard::reference_schema(TenantId(tenant), DbId(t.db));
                (
                    TableKind::Reference {
                        schema: schema.clone(),
                    },
                    Arc::new(reference_group(t.db, &schema, clusters)),
                )
            }
        };
        uniques.push(unique_keys);
        entries.push((
            t.default_schema.to_ascii_lowercase(),
            TableEntry {
                name,
                kind,
                group,
                lookups,
                lookup_gap,
                columns,
                fill_rk: fill,
                parent,
                auto_inc,
                primary,
                primary_classes,
            },
        ));
    }
    // A parent link holds only when the parent is a sharded member of the
    // same group and database (so a child row and its parent share a
    // shard); otherwise the child's inserts stay refused.
    // The parent's public unique lookup on its referenced column, if any,
    // finds parents without asking every shard.
    let checked: Vec<Checked> = entries
        .iter()
        .map(|(schema, e)| {
            let l = e.parent.as_ref()?;
            let at = entries.iter().position(|(ps, p)| {
                ps == schema
                    && p.name == l.table
                    && !p.is_reference()
                    && p.group.same_layout(&e.group)
            })?;
            let p = &entries[at].1;
            let lookup = p
                .lookups
                .iter()
                .find(|x| x.routes() && l.parent_columns.as_slice() == [x.column.clone()])
                .cloned();
            let unique = uniques[at]
                .iter()
                .any(|k| !k.is_empty() && k.iter().all(|c| l.parent_columns.contains(c)));
            Some((lookup, unique))
        })
        .collect();
    let mut topo = StaticTopology::default();
    let mut done: Vec<(String, TableEntry)> = Vec::new();
    for ((schema, mut e), c) in entries.into_iter().zip(checked) {
        match (c, e.parent.as_mut()) {
            (Some((lookup, unique)), Some(l)) => {
                l.lookup = lookup;
                l.parent_unique = unique;
            }
            _ => e.parent = None,
        }
        done.push((schema, e));
    }
    for (schema, graph) in fk_graphs(fks, &done, &shapes) {
        topo.set_fk_graph(&schema, graph);
    }
    for (schema, e) in done {
        topo.insert(&schema, e);
    }
    topo
}

/// Each database's foreign keys the gateway keeps, from the catalog rows
/// whose two ends are in `entries` (with their object ids and shapes in
/// `shapes`, in the same order).
fn fk_graphs(
    fks: &[sprig_shard::fk::ForeignKey],
    entries: &[(String, TableEntry)],
    shapes: &[(u64, Option<TableShape>)],
) -> Vec<(String, crate::fk::FkGraph)> {
    use crate::fk::{FkGraph, FkRef, ValClass};
    use sprig_shard::fk::FkEnforcement;
    let mut graphs: Vec<(String, FkGraph)> = Vec::new();
    let at = |object: u64| shapes.iter().position(|(o, _)| *o == object && object != 0);
    for f in fks {
        if f.enforcement == FkEnforcement::Native || !f.state.checked() {
            continue;
        }
        let (Some(ci), Some(pi)) = (at(f.child), at(f.parent)) else {
            continue;
        };
        let (cs, child) = &entries[ci];
        let (ps, parent) = &entries[pi];
        if cs != ps || f.child_columns.len() != f.parent_columns.len() {
            continue;
        }
        let child_shape = shapes[ci].1.as_ref();
        let parent_shape = shapes[pi].1.as_ref();
        let class_of = |shape: Option<&TableShape>, col: &str| -> Option<ValClass> {
            let s = shape?;
            if let Some((_, class)) = s.column_class(col) {
                return Some(match class {
                    sprig_shard::KeyClass::Numeric => ValClass::Num,
                    sprig_shard::KeyClass::String { .. } => ValClass::Str,
                    sprig_shard::KeyClass::Binary => ValClass::Bin,
                    _ => ValClass::Text,
                });
            }
            s.types.get(col).map(|_| ValClass::Text)
        };
        let classes: Vec<ValClass> = f
            .parent_columns
            .iter()
            .zip(&f.child_columns)
            .map(|(p, c)| {
                class_of(parent_shape, p)
                    .or_else(|| class_of(child_shape, c))
                    .unwrap_or(ValClass::Text)
            })
            .collect();
        let at = graphs.iter().position(|(s, _)| s == cs);
        let g = if let Some(i) = at {
            &mut graphs[i].1
        } else {
            graphs.push((
                cs.clone(),
                FkGraph {
                    default_schema: cs.clone(),
                    tables: Vec::new(),
                    fks: Vec::new(),
                },
            ));
            &mut graphs.last_mut().expect("just pushed").1
        };
        let mut index = |e: &TableEntry| {
            if let Some(i) = g.tables.iter().position(|t| t.name == e.name) {
                i
            } else {
                g.tables.push(e.clone());
                g.tables.len() - 1
            }
        };
        let (c, p) = (index(child), index(parent));
        g.fks.push(FkRef {
            name: f.name.clone(),
            child: c,
            child_ordinals: f
                .child_columns
                .iter()
                .map(|col| child_shape.and_then(|s| s.ordinal(col)))
                .collect(),
            child_null_default: f
                .child_columns
                .iter()
                .map(|col| {
                    child_shape.and_then(|s| s.types.get(col)).is_none_or(|d| {
                        let u = d.to_ascii_uppercase();
                        !u.contains(" DEFAULT ") || u.contains(" DEFAULT NULL")
                    })
                })
                .collect(),
            child_columns: f.child_columns.clone(),
            parent: p,
            parent_columns: f.parent_columns.clone(),
            classes,
            on_delete: f.on_delete,
            on_update: f.on_update,
        });
    }
    graphs
}

/// A parent link, checked: the parent's lookup and whether its referenced
/// columns are unique; `None` when the link doesn't hold.
type Checked = Option<(Option<Arc<LookupRef>>, bool)>;

/// The parent link of a parent member whose foreign key `fk` is in
/// `shape`; `None` when the constraint isn't there.
fn parent_link(fk: &str, shape: &TableShape) -> Option<ParentLink> {
    let f = shape
        .foreign_keys
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(fk))?;
    Some(ParentLink {
        fk: f.name.clone(),
        ordinals: f.columns.iter().map(|c| shape.ordinal(c)).collect(),
        upsert_safe: shape
            .unique_keys
            .iter()
            .all(|k| f.columns.iter().all(|c| k.contains(c))),
        columns: f.columns.clone(),
        table: f.ref_table.clone(),
        parent_columns: f.ref_columns.clone(),
        lookup: None,
        parent_unique: false,
    })
}

/// The router's view of lookup `l` of table `t` (named `name`): one value
/// column, unique, maintained, with every column's class known; `None`
/// otherwise.
fn lookup_ref(
    tenant: u64,
    t: &CatalogTable,
    name: &str,
    l: &CatalogLookup,
    shape: Option<&TableShape>,
    g: &CatalogGroup,
) -> Option<LookupRef> {
    let shape = shape?;
    let lookup_map = l.map.clone()?;
    let [col] = l.columns.as_slice() else {
        return None;
    };
    if !l.unique || !l.state.maintained() || shape.primary.is_empty() {
        return None;
    }
    let column = |c: &str| -> Option<LookupColumn> {
        let (sql_type, class) = shape.column_class(c)?;
        Some(LookupColumn {
            name: c.to_ascii_lowercase(),
            sql_type,
            class,
        })
    };
    let value = column(col)?;
    let owner_pk = shape
        .primary
        .iter()
        .map(|c| column(c))
        .collect::<Option<Vec<_>>>()?;
    let class = match value.class {
        sprig_shard::KeyClass::Numeric => KeyClass::Int,
        sprig_shard::KeyClass::String { collation } => KeyClass::Str {
            collation: collation.0,
        },
        sprig_shard::KeyClass::Binary => KeyClass::Binary,
        _ => return None,
    };
    let ordinals = std::iter::once(&value.name)
        .chain(shape.primary.iter())
        .map(|c| shape.ordinal(c))
        .collect();
    Some(LookupRef {
        column: value.name.clone(),
        class,
        pk: shape.primary.clone(),
        ordinals,
        index: LookupIndex {
            id: LookupId(l.id),
            owner: sprig_common::ids::ObjectId(l.owner),
            owner_table: name.to_owned(),
            index_name: l.index_name.clone(),
            columns: vec![value],
            owner_pk,
            unique: true,
            origin: l.origin,
            group: GroupId(lookup_map.group.0),
            state: l.state,
            auto_index_id: None,
        },
        placement: Arc::new(MapPlacement {
            tenant: TenantId(tenant),
            db: DbId(t.db),
            lookup_map,
            owner_map: Arc::clone(&g.map),
        }),
    })
}

fn group_of(tenant: u64, db: u32, g: &CatalogGroup, class: Option<KeyClass>) -> Group {
    let shards = g
        .map
        .ranges
        .iter()
        .map(|r| ShardLoc {
            shard: r.shard.0,
            schema: sprig_shard::shard_schema(TenantId(tenant), DbId(db), GroupId(g.id), r.shard),
            cluster: r.cluster.to_string(),
            lo: r.lo,
            epoch: r.epoch,
            held_until_ms: (r.state == sprig_shard::shardmap::RangeState::Held)
                .then_some(r.hold_until_ms.unwrap_or(0)),
        })
        .collect();
    Group {
        id: g.id,
        db,
        version: g.map.version,
        // Unroutable keys still need a class; no literal routes under a
        // binary class the planner never matches (the key column is empty).
        key_class: class.unwrap_or(KeyClass::Binary),
        spec: g.spec.clone(),
        shards,
    }
}

/// A reference table's "group": one entry per cluster holding a copy, so
/// the planner can tell where it joins locally.
fn reference_group(db: u32, schema: &str, clusters: &[String]) -> Group {
    let n = u32::try_from(clusters.len().max(1)).unwrap_or(u32::MAX);
    let los = even_ranges(n);
    let shards = clusters
        .iter()
        .zip(los)
        .map(|(c, lo)| ShardLoc {
            shard: 0,
            schema: schema.to_owned(),
            cluster: c.clone(),
            lo,
            epoch: 0,
            held_until_ms: None,
        })
        .collect();
    Group {
        id: 0,
        db,
        version: 0,
        key_class: KeyClass::Binary,
        spec: KeySpec::new(vec![sprig_shard::KeyClass::Binary]).unwrap_or(KeySpec {
            parts: Vec::new(),
            digest: 0,
        }),
        shards,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::Topology;
    use sprig_common::ids::ClusterId;

    const CREATE: &str = "CREATE TABLE `orders` (\n  `oid` int(11) NOT NULL,\n  `cid` int(11) NOT NULL,\n  `note` varchar(30) DEFAULT NULL,\n  `_sprig_rk` bigint(20) unsigned DEFAULT NULL INVISIBLE,\n  PRIMARY KEY (`cid`,`oid`),\n  UNIQUE KEY `u1` (`note`(10),`cid`),\n  KEY `_sprig_rk` (`_sprig_rk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

    #[test]
    fn shape_reads_visible_columns_and_unique_keys() {
        let s = TableShape::parse(CREATE);
        assert_eq!(s.visible, ["oid", "cid", "note"]);
        assert_eq!(s.unique_keys, [vec!["cid", "oid"], vec!["note", "cid"]]);
        assert_eq!(s.ordinal("CID"), Some(1));
        assert!(s.unique_keys_hold("cid"));
        assert!(!s.unique_keys_hold("oid"));
    }

    #[test]
    fn key_value_unique_keys_hold_the_distribution_key() {
        // Chapter 33's element table: `_sprig_dk` derives from `k`.
        let s = TableShape::parse(
            "CREATE TABLE `__sprig_ke_db0` (\n  `k` varbinary(1024) NOT NULL,\n  `g` bigint(20) unsigned NOT NULL,\n  `o` bigint(20) NOT NULL,\n  `e` varbinary(2024) NOT NULL DEFAULT '',\n  `v` longblob DEFAULT NULL,\n  `_sprig_dk` varbinary(1024) NOT NULL DEFAULT '' INVISIBLE,\n  `_sprig_rk` bigint(20) unsigned DEFAULT NULL INVISIBLE,\n  PRIMARY KEY (`k`,`g`,`o`,`e`),\n  KEY `_sprig_rk` (`_sprig_rk`)\n) ENGINE=InnoDB",
        );
        assert!(s.unique_keys_hold("_sprig_dk"));
        assert!(!s.unique_keys_hold("v"));
        assert!(!TableShape::parse(CREATE).unique_keys_hold("_sprig_dk"));
    }

    #[test]
    fn build_names_shard_schemas_per_database() {
        let spec =
            KeySpec::new(vec![sprig_shard::KeyClass::Numeric]).unwrap_or_else(|e| panic!("{e}"));
        let c1 = ClusterId::new("c1").unwrap_or_else(|e| panic!("{e}"));
        let c2 = ClusterId::new("c2").unwrap_or_else(|e| panic!("{e}"));
        let map = ShardMap::presplit(TenantId(7), GroupId(3), spec.digest, 3, &[c1, c2])
            .unwrap_or_else(|e| panic!("{e}"));
        let g = CatalogGroup {
            id: 3,
            spec,
            map: Arc::new(map),
        };
        let tables = vec![
            CatalogTable {
                object: 0,
                db: 1,
                default_schema: "t7_d1_g1_s1".into(),
                name: "Orders".into(),
                member: Membership::Sharded {
                    group: 3,
                    key_columns: Some(vec!["CID".into()]),
                    create_sql: Some(CREATE.into()),
                    lookups: Vec::new(),
                    fill_rk: false,
                    parent_fk: None,
                },
            },
            CatalogTable {
                object: 0,
                db: 2,
                default_schema: "t7_d2_g2_s1".into(),
                name: "items".into(),
                member: Membership::Sharded {
                    group: 3,
                    key_columns: None,
                    create_sql: None,
                    lookups: Vec::new(),
                    fill_rk: false,
                    parent_fk: None,
                },
            },
            CatalogTable {
                object: 0,
                db: 1,
                default_schema: "t7_d1_g1_s1".into(),
                name: "countries".into(),
                member: Membership::Reference {
                    clusters: vec!["c1".into(), "c2".into()],
                },
            },
        ];
        let t = build(7, &[g], &tables);
        let o = t.table("t7_d1_g1_s1", "orders").unwrap_or_else(|| panic!());
        assert_eq!(o.key(), Some("cid"));
        assert_eq!(o.group.shards.len(), 3);
        assert_eq!(o.group.shards[1].schema, "t7_d1_g3_s2");
        assert_eq!(o.group.shards[1].cluster, "c2");
        assert!(matches!(
            o.kind,
            TableKind::Sharded {
                key_ordinal: Some(1),
                upsert_safe: true,
                ..
            }
        ));
        let i = t.table("t7_d2_g2_s1", "items").unwrap_or_else(|| panic!());
        assert_eq!(i.key(), None);
        assert_eq!(i.group.shards[0].schema, "t7_d2_g3_s1");
        assert!(!i.group.same_layout(&o.group));
        let r = t
            .table("t7_d1_g1_s1", "countries")
            .unwrap_or_else(|| panic!());
        assert!(r.is_reference());
        assert_eq!(r.group.shards.len(), 2);
        assert!(t.mentions("select * from countries"));
    }
}
