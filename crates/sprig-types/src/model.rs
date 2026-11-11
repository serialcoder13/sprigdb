//! The type catalog in memory (chapter 32, "Interfaces" and "In-memory
//! state"): types with their chains, descendants and field owners, and
//! record hierarchies, per logical database.

use std::collections::HashMap;
use std::sync::Arc;

use sprig_common::ids::{DbId, ObjectId};

/// A type's id: the `objects.object_id` of its table.
pub type TypeId = ObjectId;

/// Where a type's rows live.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Storage {
    /// The root table holds the shared columns; each subtype adds an
    /// extension table under the same key.
    Joined,
    /// Every column in the root table; each subtype keeps an empty table
    /// of its name with only the key (names, privileges and dump tools
    /// see it), never read or written.
    SingleTable,
    /// No storage: an entity type for other models.
    None,
}

impl Storage {
    /// The catalog's spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Storage::Joined => "joined",
            Storage::SingleTable => "single_table",
            Storage::None => "none",
        }
    }

    /// Parses the catalog's spelling.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "joined" => Some(Storage::Joined),
            "single_table" => Some(Storage::SingleTable),
            "none" => Some(Storage::None),
            _ => None,
        }
    }
}

/// A type's lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TypeState {
    /// Serving.
    Active,
    /// A rename job is rewriting the type and path columns (not built).
    Renaming,
    /// `DROP TABLE` is deleting the type's rows and tables.
    Dropping,
}

impl TypeState {
    /// The catalog's spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            TypeState::Active => "active",
            TypeState::Renaming => "renaming",
            TypeState::Dropping => "dropping",
        }
    }

    /// Parses the catalog's spelling.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "active" => Some(TypeState::Active),
            "renaming" => Some(TypeState::Renaming),
            "dropping" => Some(TypeState::Dropping),
            _ => None,
        }
    }
}

/// The `default_expr` that marks an `AUTO_INCREMENT` key field.
pub const AUTO_INCREMENT: &str = "AUTO_INCREMENT";

/// A field a type declares itself (a `type_fields` row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDef {
    /// The column name as declared.
    pub name: String,
    /// Position among the type's own fields, from 1.
    pub ordinal: u16,
    /// The column type as MariaDB shows it: `bigint(20) unsigned`,
    /// `varchar(200) COLLATE utf8mb4_bin`.
    pub data_type: String,
    /// Whether the column takes `NULL`.
    pub nullable: bool,
    /// The default, or [`AUTO_INCREMENT`] for an auto-increment key.
    pub default_expr: Option<String>,
    /// The table that holds the column: the type's own table.
    pub physical_table: String,
}

impl FieldDef {
    /// Whether this is an `AUTO_INCREMENT` column.
    #[must_use]
    pub fn auto_increment(&self) -> bool {
        self.default_expr.as_deref() == Some(AUTO_INCREMENT)
    }
}

/// The value of a discriminator column that marks a row as one subtype
/// (`INHERITS (p) WHERE vtype = 'TRUCK'`), as the clause wrote it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DiscValue {
    /// A string literal's value.
    Str(String),
    /// A number's text (`5`, `-1`, `2.5`).
    Num(String),
}

impl DiscValue {
    /// The value as an SQL literal.
    #[must_use]
    pub fn sql(&self, no_backslash_escapes: bool) -> String {
        match self {
            DiscValue::Str(s) => crate::rewrite::quote_str(s, no_backslash_escapes),
            DiscValue::Num(n) => n.clone(),
        }
    }

    /// The catalog's JSON for a subtype's value.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            DiscValue::Str(s) => serde_json::json!({ "value": s }),
            DiscValue::Num(n) => serde_json::json!({ "number": n }),
        }
    }

    /// Whether `other` (a literal a statement wrote) is this value: strings
    /// byte for byte, ignoring trailing spaces as `PAD SPACE` collations do;
    /// numbers by value.
    #[must_use]
    pub fn same(&self, other: &DiscValue) -> bool {
        match (self, other) {
            (DiscValue::Str(a), DiscValue::Str(b)) => {
                a.trim_end_matches(' ') == b.trim_end_matches(' ')
            }
            (DiscValue::Num(a), DiscValue::Num(b)) => {
                a == b
                    || matches!((a.parse::<f64>(), b.parse::<f64>()), (Ok(x), Ok(y)) if (x - y).abs() == 0.0)
            }
            (DiscValue::Num(n), DiscValue::Str(s)) | (DiscValue::Str(s), DiscValue::Num(n)) => {
                matches!((n.parse::<f64>(), s.trim().parse::<f64>()), (Ok(x), Ok(y)) if (x - y).abs() == 0.0)
            }
        }
    }
}

impl std::fmt::Display for DiscValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.sql(false))
    }
}

/// One type (a `types` row with its own fields).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeDef {
    /// The table's object id.
    pub id: TypeId,
    /// The logical database.
    pub db: DbId,
    /// The type's name, which is its table's name.
    pub name: String,
    /// The parent type; `None` for a root.
    pub parent: Option<TypeId>,
    /// The root type (itself for a root).
    pub root: TypeId,
    /// 0 for a root.
    pub depth: u8,
    /// `/asset/vehicle/`.
    pub path: String,
    /// No row has exactly this type.
    pub is_abstract: bool,
    /// Storage mode (the root's).
    pub storage: Storage,
    /// The root's primary key columns (every type repeats them).
    pub key_fields: Vec<String>,
    /// The fields this type declares; a root's include the key.
    pub own_fields: Vec<FieldDef>,
    /// A root's discriminator column: the user column whose value says
    /// which type a row is (`SPRIG_DISCRIMINATOR`, or the column of its
    /// subtypes' `WHERE`). `None` for subtypes and for roots typed by
    /// SprigDB's own `_type`.
    pub discriminator: Option<String>,
    /// A subtype's discriminator value (`WHERE vtype = 'TRUCK'`).
    pub disc_value: Option<DiscValue>,
    /// State.
    pub state: TypeState,
    /// Bumped on every change to the row.
    pub version: u64,
}

impl TypeDef {
    /// Whether this is a root.
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.parent.is_none()
    }

    /// Whether `name` is one of the key columns (case-insensitive, as
    /// MySQL compares column names).
    #[must_use]
    pub fn is_key(&self, name: &str) -> bool {
        self.key_fields.iter().any(|k| k.eq_ignore_ascii_case(name))
    }

    /// Whether the type's own fields include `name`.
    #[must_use]
    pub fn owns(&self, name: &str) -> bool {
        self.own_fields
            .iter()
            .any(|f| f.name.eq_ignore_ascii_case(name))
    }
}

/// The exclusive upper bound of the byte range of paths under `path`: the
/// final `/` (0x2F) replaced by `0` (0x30).
#[must_use]
pub fn upper(path: &[u8]) -> Vec<u8> {
    let mut u = path.to_vec();
    if let Some(last) = u.last_mut()
        && *last == b'/'
    {
        *last = b'0';
    }
    u
}

/// `ON DELETE` of a record hierarchy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OnDelete {
    /// Deleting a record with children fails (39714).
    Restrict,
    /// Deleting a record deletes its subtree.
    Cascade,
}

impl OnDelete {
    /// The catalog's spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            OnDelete::Restrict => "restrict",
            OnDelete::Cascade => "cascade",
        }
    }
}

/// How a key becomes a path segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyClass {
    /// Integers, in decimal.
    Int,
    /// Strings, with `%`, `/` and `,` escaped.
    Str,
    /// Binary strings, in hex.
    Bin,
}

impl KeyClass {
    /// The spelling in `hierarchies.key_fields`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            KeyClass::Int => "int",
            KeyClass::Str => "string",
            KeyClass::Bin => "binary",
        }
    }

    /// Parses [`KeyClass::as_str`].
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "int" => Some(KeyClass::Int),
            "string" => Some(KeyClass::Str),
            "binary" => Some(KeyClass::Bin),
            _ => None,
        }
    }

    /// The class of a column type as MariaDB shows it (`bigint(20)
    /// unsigned`, `varchar(64)`, `binary(16)`); `None` for a type a path
    /// segment can't hold.
    #[must_use]
    pub fn of_type(data_type: &str) -> Option<Self> {
        let t = data_type.trim_start().to_ascii_lowercase();
        let base: String = t.chars().take_while(char::is_ascii_alphabetic).collect();
        match base.as_str() {
            "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" => {
                Some(KeyClass::Int)
            }
            "char" | "varchar" => Some(KeyClass::Str),
            "binary" | "varbinary" => Some(KeyClass::Bin),
            _ => None,
        }
    }
}

/// A record hierarchy's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HierState {
    /// `ADD HIERARCHY` is filling the paths.
    Backfilling,
    /// Serving.
    Active,
    /// The backfill found orphans or cycles.
    Failed,
    /// `DROP HIERARCHY` is removing the columns.
    Dropping,
}

impl HierState {
    /// The catalog's spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            HierState::Backfilling => "backfilling",
            HierState::Active => "active",
            HierState::Failed => "failed",
            HierState::Dropping => "dropping",
        }
    }

    /// Parses the catalog's spelling.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "backfilling" => Some(HierState::Backfilling),
            "active" => Some(HierState::Active),
            "failed" => Some(HierState::Failed),
            "dropping" => Some(HierState::Dropping),
            _ => None,
        }
    }
}

/// A declared record hierarchy (a `hierarchies` row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HierarchyDef {
    /// The table's object id.
    pub object_id: ObjectId,
    /// The logical database.
    pub db: DbId,
    /// The table's name.
    pub table: String,
    /// The key column (one column in this release).
    pub key: String,
    /// How the key becomes a segment.
    pub key_class: KeyClass,
    /// The key column's type, for `_sprig_rk`.
    pub key_type: String,
    /// The parent column.
    pub parent: String,
    /// `ON DELETE`.
    pub on_delete: OnDelete,
    /// `MAX DEPTH`.
    pub max_depth: u16,
    /// State.
    pub state: HierState,
}

/// A field of a type with the type that declares it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedField {
    /// The column name.
    pub name: String,
    /// The type that declares it (the root for key fields).
    pub owner: TypeId,
    /// The table that holds it: the owner's, or the root's in single-table
    /// storage.
    pub owner_table: String,
    /// Whether it is a key column (every table of the chain has it).
    pub is_key: bool,
}

/// One logical database's types and hierarchies.
#[derive(Debug, Clone, Default)]
pub struct DbTypes {
    types: HashMap<TypeId, Arc<TypeDef>>,
    by_lower: HashMap<String, TypeId>,
    children: HashMap<TypeId, Vec<TypeId>>,
    hierarchies: HashMap<String, Arc<HierarchyDef>>,
}

impl DbTypes {
    /// Builds the index from its rows.
    #[must_use]
    pub fn new(types: Vec<TypeDef>, hierarchies: Vec<HierarchyDef>) -> Self {
        let mut db = DbTypes::default();
        for t in types {
            db.by_lower.insert(t.name.to_lowercase(), t.id);
            if let Some(p) = t.parent {
                db.children.entry(p).or_default().push(t.id);
            }
            db.types.insert(t.id, Arc::new(t));
        }
        for kids in db.children.values_mut() {
            kids.sort_unstable();
        }
        for h in hierarchies {
            db.hierarchies.insert(h.table.to_lowercase(), Arc::new(h));
        }
        db
    }

    /// Whether the database has neither types nor hierarchies.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.types.is_empty() && self.hierarchies.is_empty()
    }

    /// The type of table `name`. Table names compare exactly unless
    /// `lower_case_table_names` is on.
    #[must_use]
    pub fn get(&self, name: &str, lctn: bool) -> Option<&Arc<TypeDef>> {
        let t = self.types.get(self.by_lower.get(&name.to_lowercase())?)?;
        (lctn || t.name == name).then_some(t)
    }

    /// The type `id`.
    #[must_use]
    pub fn by_id(&self, id: TypeId) -> Option<&Arc<TypeDef>> {
        self.types.get(&id)
    }

    /// Every type, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = &Arc<TypeDef>> {
        self.types.values()
    }

    /// The hierarchy declared on table `name`.
    #[must_use]
    pub fn hierarchy(&self, name: &str, lctn: bool) -> Option<&Arc<HierarchyDef>> {
        let h = self.hierarchies.get(&name.to_lowercase())?;
        (lctn || h.table == name).then_some(h)
    }

    /// Every hierarchy.
    pub fn hierarchies(&self) -> impl Iterator<Item = &Arc<HierarchyDef>> {
        self.hierarchies.values()
    }

    /// The chain from the root down to `id`, root first.
    #[must_use]
    pub fn chain(&self, id: TypeId) -> Vec<Arc<TypeDef>> {
        let mut out = Vec::new();
        let mut cur = self.types.get(&id);
        while let Some(t) = cur {
            out.push(Arc::clone(t));
            if out.len() > 255 {
                break; // a cycle in a corrupt catalog
            }
            cur = t.parent.and_then(|p| self.types.get(&p));
        }
        out.reverse();
        out
    }

    /// The root of `id`'s hierarchy.
    #[must_use]
    pub fn root_of(&self, id: TypeId) -> Option<Arc<TypeDef>> {
        self.chain(id).into_iter().next()
    }

    /// Whether `id` has subtypes.
    #[must_use]
    pub fn has_subtypes(&self, id: TypeId) -> bool {
        self.children.get(&id).is_some_and(|c| !c.is_empty())
    }

    /// The direct subtypes of `id`.
    #[must_use]
    pub fn children(&self, id: TypeId) -> Vec<Arc<TypeDef>> {
        self.children
            .get(&id)
            .map(|c| {
                c.iter()
                    .filter_map(|i| self.types.get(i).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Every type under `id` (not `id` itself), shallowest first.
    #[must_use]
    pub fn descendants(&self, id: TypeId) -> Vec<Arc<TypeDef>> {
        let mut out = Vec::new();
        let mut queue = std::collections::VecDeque::from([id]);
        while let Some(t) = queue.pop_front() {
            if out.len() > 10_000 {
                break;
            }
            for c in self.children.get(&t).into_iter().flatten() {
                if let Some(def) = self.types.get(c) {
                    out.push(Arc::clone(def));
                    queue.push_back(*c);
                }
            }
        }
        out
    }

    /// Every field of `id`, inherited first: the root's (its key
    /// included), then each level's own.
    #[must_use]
    pub fn all_fields(&self, id: TypeId) -> Vec<ResolvedField> {
        let chain = self.chain(id);
        let Some(root) = chain.first() else {
            return Vec::new();
        };
        let single = root.storage == Storage::SingleTable;
        let mut out = Vec::new();
        for t in &chain {
            for f in &t.own_fields {
                out.push(ResolvedField {
                    name: f.name.clone(),
                    owner: t.id,
                    owner_table: if single {
                        root.name.clone()
                    } else {
                        t.name.clone()
                    },
                    is_key: root.is_key(&f.name),
                });
            }
        }
        out
    }

    /// The field `name` of `id` (case-insensitive).
    #[must_use]
    pub fn field(&self, id: TypeId, name: &str) -> Option<ResolvedField> {
        self.all_fields(id)
            .into_iter()
            .find(|f| f.name.eq_ignore_ascii_case(name))
    }

    /// Whether `name` is a field of `id` or of any of its subtypes: what a
    /// new field of `id` must not be called.
    #[must_use]
    pub fn name_taken(&self, id: TypeId, name: &str) -> bool {
        self.field(id, name).is_some() || self.descendants(id).iter().any(|d| d.owns(name))
    }

    /// The discriminator column of `id`'s hierarchy, if it has one.
    #[must_use]
    pub fn discriminator(&self, id: TypeId) -> Option<String> {
        self.root_of(id)?.discriminator.clone()
    }

    /// The storage of `id`'s hierarchy (the root's).
    #[must_use]
    pub fn storage(&self, id: TypeId) -> Storage {
        self.root_of(id).map_or(Storage::Joined, |r| r.storage)
    }

    /// The type a row of `root`'s hierarchy is when its discriminator
    /// holds `v`: the subtype that declares `v`, else the root (open
    /// values: an undeclared value belongs to the root).
    #[must_use]
    pub fn type_of_value(&self, root: TypeId, v: &DiscValue) -> Option<Arc<TypeDef>> {
        let r = self.types.get(&root)?;
        Some(
            self.descendants(root)
                .into_iter()
                .find(|t| t.disc_value.as_ref().is_some_and(|d| d.same(v)))
                .unwrap_or_else(|| Arc::clone(r)),
        )
    }

    /// The root key field definitions of `id`'s hierarchy, in key order.
    #[must_use]
    pub fn key_defs(&self, id: TypeId) -> Vec<FieldDef> {
        let Some(root) = self.root_of(id) else {
            return Vec::new();
        };
        root.key_fields
            .iter()
            .filter_map(|k| {
                root.own_fields
                    .iter()
                    .find(|f| f.name.eq_ignore_ascii_case(k))
                    .cloned()
            })
            .collect()
    }
}

/// Every database's types: the `types` module's state in a catalog
/// snapshot.
#[derive(Debug, Clone, Default)]
pub struct TypesState {
    /// The module's tables exist.
    pub migrated: bool,
    dbs: HashMap<DbId, Arc<DbTypes>>,
}

impl TypesState {
    /// The state of a catalog without the module's tables.
    #[must_use]
    pub fn unmigrated() -> Self {
        Self::default()
    }

    /// Builds the state from its rows.
    #[must_use]
    pub fn new(types: Vec<TypeDef>, hierarchies: Vec<HierarchyDef>) -> Self {
        let mut by_db: HashMap<DbId, (Vec<TypeDef>, Vec<HierarchyDef>)> = HashMap::new();
        for t in types {
            by_db.entry(t.db).or_default().0.push(t);
        }
        for h in hierarchies {
            by_db.entry(h.db).or_default().1.push(h);
        }
        Self {
            migrated: true,
            dbs: by_db
                .into_iter()
                .map(|(db, (t, h))| (db, Arc::new(DbTypes::new(t, h))))
                .collect(),
        }
    }

    /// The types of database `db`.
    #[must_use]
    pub fn db(&self, db: DbId) -> Option<&Arc<DbTypes>> {
        self.dbs.get(&db)
    }

    /// Whether no database has types or hierarchies.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.dbs.values().all(|d| d.is_empty())
    }

    /// Every database with types or hierarchies.
    pub fn dbs(&self) -> impl Iterator<Item = (DbId, &Arc<DbTypes>)> {
        self.dbs.iter().map(|(k, v)| (*k, v))
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    use super::*;

    pub(crate) fn field(name: &str, table: &str, data_type: &str) -> FieldDef {
        FieldDef {
            name: name.into(),
            ordinal: 0,
            data_type: data_type.into(),
            nullable: true,
            default_expr: None,
            physical_table: table.into(),
        }
    }

    /// `asset (id, name, site_id) > vehicle (vin) > truck (payload_kg)`;
    /// asset > building (floors). Ids 1..4 in database 1.
    pub(crate) fn assets() -> DbTypes {
        let mut id = field("id", "asset", "bigint(20)");
        id.nullable = false;
        let t = |oid: u64, name: &str, parent: Option<u64>, depth: u8, path: &str, own| TypeDef {
            id: ObjectId(oid),
            db: DbId(1),
            name: name.into(),
            parent: parent.map(ObjectId),
            root: ObjectId(1),
            depth,
            path: path.into(),
            is_abstract: false,
            storage: Storage::Joined,
            key_fields: vec!["id".into()],
            own_fields: own,
            discriminator: None,
            disc_value: None,
            state: TypeState::Active,
            version: 1,
        };
        DbTypes::new(
            vec![
                t(
                    1,
                    "asset",
                    None,
                    0,
                    "/asset/",
                    vec![
                        id,
                        field("name", "asset", "varchar(200)"),
                        field("site_id", "asset", "int(11)"),
                    ],
                ),
                t(
                    2,
                    "vehicle",
                    Some(1),
                    1,
                    "/asset/vehicle/",
                    vec![field("vin", "vehicle", "char(17)")],
                ),
                t(
                    3,
                    "truck",
                    Some(2),
                    2,
                    "/asset/vehicle/truck/",
                    vec![field("payload_kg", "truck", "decimal(10,2)")],
                ),
                t(
                    4,
                    "building",
                    Some(1),
                    1,
                    "/asset/building/",
                    vec![field("floors", "building", "int(11)")],
                ),
            ],
            vec![HierarchyDef {
                object_id: ObjectId(10),
                db: DbId(1),
                table: "org_unit".into(),
                key: "id".into(),
                key_class: KeyClass::Int,
                key_type: "int(11)".into(),
                parent: "parent_id".into(),
                on_delete: OnDelete::Restrict,
                max_depth: 64,
                state: HierState::Active,
            }],
        )
    }
}

#[cfg(test)]
pub(crate) mod fleet {
    //! Kiran's example: `vehicle` typed by its `vtype` column.
    use super::fixtures::field;
    use super::*;

    /// `vehicle (id, name, vtype)` with discriminator `vtype` > `truck
    /// (payload_kg)` 'TRUCK' > `smalltruck (cap)` 'SMALLTRUCK', and
    /// `vehicle` > `car (seats)` 'CAR'. Ids 21..24 in database 1.
    pub(crate) fn fleet(storage: Storage) -> DbTypes {
        let single = storage == Storage::SingleTable;
        let table = |own: &str| {
            if single {
                "vehicle".to_owned()
            } else {
                own.to_owned()
            }
        };
        let mut id = field("id", "vehicle", "bigint(20)");
        id.nullable = false;
        let t =
            |oid: u64, name: &str, parent: Option<u64>, depth: u8, path: &str, v, own| TypeDef {
                id: ObjectId(oid),
                db: DbId(1),
                name: name.into(),
                parent: parent.map(ObjectId),
                root: ObjectId(21),
                depth,
                path: path.into(),
                is_abstract: false,
                storage,
                key_fields: vec!["id".into()],
                own_fields: own,
                discriminator: (oid == 21).then(|| "vtype".to_owned()),
                disc_value: v,
                state: TypeState::Active,
                version: 1,
            };
        let s = |v: &str| Some(DiscValue::Str(v.into()));
        DbTypes::new(
            vec![
                t(
                    21,
                    "vehicle",
                    None,
                    0,
                    "/vehicle/",
                    None,
                    vec![
                        id,
                        field("name", "vehicle", "varchar(50)"),
                        field("vtype", "vehicle", "varchar(31)"),
                    ],
                ),
                t(
                    22,
                    "truck",
                    Some(21),
                    1,
                    "/vehicle/truck/",
                    s("TRUCK"),
                    vec![field("payload_kg", &table("truck"), "decimal(10,2)")],
                ),
                t(
                    23,
                    "smalltruck",
                    Some(22),
                    2,
                    "/vehicle/truck/smalltruck/",
                    s("SMALLTRUCK"),
                    vec![field("cap", &table("smalltruck"), "int(11)")],
                ),
                t(
                    24,
                    "car",
                    Some(21),
                    1,
                    "/vehicle/car/",
                    s("CAR"),
                    vec![field("seats", &table("car"), "int(11)")],
                ),
            ],
            Vec::new(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::assets;
    use super::*;

    #[test]
    fn chains_descendants_and_fields() {
        let db = assets();
        let truck = db.get("truck", false).unwrap();
        let chain: Vec<_> = db.chain(truck.id).iter().map(|t| t.name.clone()).collect();
        assert_eq!(chain, ["asset", "vehicle", "truck"]);
        let desc: Vec<_> = db
            .descendants(ObjectId(1))
            .iter()
            .map(|t| t.name.clone())
            .collect();
        assert_eq!(desc, ["vehicle", "building", "truck"]);
        let fields: Vec<_> = db
            .all_fields(truck.id)
            .iter()
            .map(|f| (f.name.clone(), f.owner_table.clone(), f.is_key))
            .collect();
        assert_eq!(
            fields,
            [
                ("id".into(), "asset".into(), true),
                ("name".into(), "asset".into(), false),
                ("site_id".into(), "asset".into(), false),
                ("vin".into(), "vehicle".into(), false),
                ("payload_kg".into(), "truck".into(), false),
            ]
        );
        assert!(db.name_taken(ObjectId(2), "PAYLOAD_KG"));
        assert!(!db.name_taken(ObjectId(2), "floors"));
        assert!(db.get("Truck", false).is_none());
        assert!(db.get("Truck", true).is_some());
        assert!(db.hierarchy("org_unit", false).is_some());
    }

    #[test]
    fn values_name_types_and_undeclared_values_are_the_roots() {
        let db = super::fleet::fleet(Storage::SingleTable);
        let of = |v: DiscValue| db.type_of_value(ObjectId(21), &v).unwrap().name.clone();
        assert_eq!(of(DiscValue::Str("TRUCK".into())), "truck");
        assert_eq!(of(DiscValue::Str("SMALLTRUCK ".into())), "smalltruck");
        assert_eq!(of(DiscValue::Str("MINITRUCK".into())), "vehicle");
        assert_eq!(of(DiscValue::Str("truck".into())), "vehicle");
        assert_eq!(db.discriminator(ObjectId(23)).as_deref(), Some("vtype"));
        let owners: Vec<_> = db
            .all_fields(ObjectId(23))
            .iter()
            .map(|f| f.owner_table.clone())
            .collect();
        assert!(owners.iter().all(|o| o == "vehicle"), "{owners:?}");
        assert!(DiscValue::Num("5".into()).same(&DiscValue::Num("5.0".into())));
        assert!(!DiscValue::Num("5".into()).same(&DiscValue::Num("6".into())));
    }

    #[test]
    fn upper_bounds_and_key_classes() {
        assert_eq!(upper(b"/asset/vehicle/"), b"/asset/vehicle0");
        assert_eq!(upper(b"/1/7/"), b"/1/70");
        assert_eq!(
            KeyClass::of_type("bigint(20) unsigned"),
            Some(KeyClass::Int)
        );
        assert_eq!(KeyClass::of_type("varchar(64)"), Some(KeyClass::Str));
        assert_eq!(KeyClass::of_type("binary(16)"), Some(KeyClass::Bin));
        assert_eq!(KeyClass::of_type("datetime"), None);
    }
}
