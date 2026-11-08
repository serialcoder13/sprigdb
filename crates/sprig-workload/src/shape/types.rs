//! The shape's public types: what the advisor and the indexer read
//! (chapter 12, "Data structures and storage", the `shape` JSON).
//!
//! These types are a stable contract with `sprig-advisor` and with the
//! `workload_fingerprints.shape` column. Fields are only ever added (with
//! `#[serde(default)]`), never renamed or removed, within one
//! [`SHAPE_VERSION`].
//!
//! Columns are referred to by **name only** (chapter 12, "Security notes"):
//! lower case, unquoted. Tables are referred to by the alias the statement
//! uses ([`TableRef::alias`]), and through it by their catalog
//! [`ObjectId`].

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use sprig_common::ids::{DbId, ObjectId};

/// The version of the shape format, stored in `shape_version` and in the
/// JSON as `"v"`.
pub const SHAPE_VERSION: u16 = 1;

/// What a statement does with which columns: the predicates, joins, order,
/// grouping and selected columns that automatic indexing designs indexes
/// from (chapter 13, "Candidate generation").
///
/// One shape is shared by every execution of a fingerprint. Literal
/// values are never in it: a [`Predicate`]'s `args` index the execution's
/// literal list instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)] // the stored JSON's flags
pub struct Shape {
    /// [`SHAPE_VERSION`].
    #[serde(rename = "v")]
    pub version: u16,
    /// The statement type.
    #[serde(rename = "type")]
    pub stmt_type: StmtType,
    /// Every table the statement reads or writes, in the order they appear,
    /// including tables inside subqueries. Aliases are unique within a
    /// shape (see [`TableRef::alias`]).
    pub tables: Vec<TableRef>,
    /// The top-level `AND` conjuncts of every `WHERE` and `ON`, one entry
    /// per column condition, in statement order.
    pub predicates: Vec<Predicate>,
    /// Equality (and range) conditions between columns of two tables, and
    /// `IN (SELECT ...)` / `EXISTS` semi-joins.
    pub joins: Vec<Join>,
    /// `ORDER BY` columns of the outermost query, in order.
    pub order_by: Vec<OrderItem>,
    /// `GROUP BY` columns of the outermost query, in order.
    pub group_by: Vec<ColumnRef>,
    /// Columns read by each table's select list (and, for writes, by
    /// `SET` expressions), keyed by [`TableRef::alias`]; sorted and unique.
    /// `"*"` stands for every column. Columns whose table could not be
    /// determined are under the key `"?"`. Used for covering decisions.
    pub select: BTreeMap<String, Vec<String>>,
    /// Whether the outermost query has a `LIMIT`.
    pub limit: bool,
    /// What a write statement changes; `None` for reads.
    pub writes: Option<Writes>,
    /// How many `OR` groups the conjuncts held that could not be rewritten
    /// to an `in` predicate.
    pub or_groups: u32,
    /// An `ORDER BY` item is not a plain column (an expression or a
    /// position), so no index can deliver the order.
    #[serde(default, skip_serializing_if = "is_false")]
    pub order_by_expr: bool,
    /// A `GROUP BY` item is not a plain column.
    #[serde(default, skip_serializing_if = "is_false")]
    pub group_by_expr: bool,
    /// The shape came from the lexer fallback, not from a full parse: only
    /// [`Shape::tables`] and the simple conjuncts the fallback recognizes
    /// are filled. Consumers should not conclude that a missing predicate
    /// is absent.
    #[serde(default, skip_serializing_if = "is_false")]
    pub partial: bool,
    /// The slot of the outermost `LIMIT`'s row count, when it is a literal
    /// or `?`: a parameter sample gives its value (chapter 13's cost model
    /// uses `LIMIT k`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_arg: Option<u32>,
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde's skip_serializing_if signature
fn is_false(b: &bool) -> bool {
    !*b
}

impl Shape {
    /// An empty shape of a statement type: no tables, no predicates.
    #[must_use]
    pub fn empty(stmt_type: StmtType) -> Self {
        Self {
            version: SHAPE_VERSION,
            stmt_type,
            tables: Vec::new(),
            predicates: Vec::new(),
            joins: Vec::new(),
            order_by: Vec::new(),
            group_by: Vec::new(),
            select: BTreeMap::new(),
            limit: false,
            writes: None,
            or_groups: 0,
            order_by_expr: false,
            group_by_expr: false,
            partial: false,
            limit_arg: None,
        }
    }

    /// The table an alias names.
    #[must_use]
    pub fn table(&self, alias: &str) -> Option<&TableRef> {
        self.tables.iter().find(|t| t.alias == alias)
    }

    /// The fingerprint's **main object** (chapter 12, "Configuration": data
    /// settings resolve on it): the first write target, else the first base
    /// table the catalog knows.
    #[must_use]
    pub fn main_table(&self) -> Option<&TableRef> {
        let target = self
            .writes
            .as_ref()
            .and_then(|w| w.targets.first())
            .and_then(|a| self.table(a))
            .filter(|t| t.object_id.is_some());
        target.or_else(|| self.tables.iter().find(|t| t.object_id.is_some()))
    }

    /// The catalog objects referenced, sorted and unique: the
    /// `workload_fingerprints.object_ids` column.
    #[must_use]
    pub fn object_ids(&self) -> Vec<ObjectId> {
        let mut ids: Vec<ObjectId> = self.tables.iter().filter_map(|t| t.object_id).collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// The predicates on one table alias.
    pub fn predicates_on<'a>(&'a self, alias: &'a str) -> impl Iterator<Item = &'a Predicate> {
        self.predicates
            .iter()
            .filter(move |p| p.column.table.as_deref() == Some(alias))
    }

    /// Whether every predicate is unsargable (chapter 13's trigger records
    /// a reason and makes no candidate). False when there are none.
    #[must_use]
    pub fn all_unsargable(&self) -> bool {
        !self.predicates.is_empty() && self.predicates.iter().all(|p| !p.op.is_sargable())
    }

    /// The shape as the JSON stored in `workload_fingerprints.shape`.
    #[must_use]
    pub fn to_json(&self) -> String {
        // Serializing these plain types can't fail.
        serde_json::to_string(self).unwrap_or_default()
    }

    /// Parses the stored JSON.
    ///
    /// # Errors
    ///
    /// The JSON doesn't describe a shape.
    pub fn from_json(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }
}

/// The statement type, as `"type"` in the JSON and, upper case, in
/// `workload_fingerprints.stmt_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StmtType {
    /// `SELECT`, `WITH ... SELECT`, `TABLE`, `VALUES` and set operations.
    Select,
    /// `INSERT` (including `INSERT ... SELECT` and `ON DUPLICATE KEY
    /// UPDATE`).
    Insert,
    /// `REPLACE`.
    Replace,
    /// `UPDATE`.
    Update,
    /// `DELETE`.
    Delete,
    /// `CALL`.
    Call,
    /// DDL: `CREATE`, `ALTER`, `DROP`, `RENAME`, `TRUNCATE`.
    Ddl,
    /// `SET`.
    Set,
    /// `SHOW`, `DESCRIBE`, `EXPLAIN`.
    Show,
    /// `BEGIN`, `START TRANSACTION`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `XA`.
    Transaction,
    /// Anything else.
    #[default]
    Other,
}

impl StmtType {
    /// The `workload_fingerprints.stmt_type` value: `SELECT`, `UPDATE`, ...
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Replace => "REPLACE",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::Call => "CALL",
            Self::Ddl => "DDL",
            Self::Set => "SET",
            Self::Show => "SHOW",
            Self::Transaction => "TRANSACTION",
            Self::Other => "OTHER",
        }
    }

    /// Whether automatic indexing considers this type at all: reads and
    /// the `WHERE` clauses of writes (chapter 13, "Trigger evaluation").
    #[must_use]
    pub const fn is_indexable(self) -> bool {
        matches!(
            self,
            Self::Select | Self::Insert | Self::Replace | Self::Update | Self::Delete
        )
    }
}

impl fmt::Display for StmtType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One table reference of the statement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRef {
    /// How the statement refers to the table: its alias, else its name as
    /// written (case kept). Unique within the shape: when two scopes use
    /// the same alias, the later ones get `#2`, `#3`, ...
    pub alias: String,
    /// The catalog object, when the resolver knows the table. `None` for
    /// derived tables, CTEs, system schemas and unknown tables.
    pub object_id: Option<ObjectId>,
    /// The logical database: the qualifier as written, else the session's
    /// current database. `None` when neither exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db: Option<String>,
    /// The logical table name (for a derived table or CTE, its alias).
    #[serde(default)]
    pub table: String,
    /// The catalog database id, when resolved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db_id: Option<DbId>,
    /// A derived table (`FROM (SELECT ...) d`) or a CTE: not a base table,
    /// so predicates on it can't be served by an index on it.
    #[serde(default, skip_serializing_if = "is_false")]
    pub derived: bool,
}

/// A column of a table in the shape: `"t"` (the table's alias) and
/// `"col"` in the JSON.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ColumnRef {
    /// The [`TableRef::alias`] of the column's table; `None` when an
    /// unqualified column could belong to several tables and the resolver
    /// couldn't say which.
    #[serde(rename = "t")]
    pub table: Option<String>,
    /// The column name, lower case.
    pub col: String,
}

impl ColumnRef {
    /// A column of a known table.
    #[must_use]
    pub fn new(table: impl Into<String>, col: impl Into<String>) -> Self {
        Self {
            table: Some(table.into()),
            col: col.into(),
        }
    }
}

/// One column condition: a top-level `AND` conjunct of a `WHERE` or `ON`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Predicate {
    /// The column.
    #[serde(flatten)]
    pub column: ColumnRef,
    /// The operator, with its flags.
    #[serde(flatten)]
    pub op: PredOp,
    /// Indexes into the execution's literal list (the `?` slots of the
    /// normalized text, in order): a parameter sample supplies each
    /// value. Empty when the compared value isn't a literal or `?` (another
    /// column, a function of constants, a subquery).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<u32>,
}

/// A predicate's operator (chapter 12, "Extracting predicates and
/// columns"). In the JSON, `"op"` plus the variant's fields.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum PredOp {
    /// `col = v`, `col <=> v`, `v = col`.
    Eq,
    /// `col IN (...)`, or an `OR` of equalities on one column.
    In {
        /// The single arg is a collapsed list slot (`IN (...)` in the
        /// normalized text), whose length varies per execution. False when
        /// the predicate came from an `OR` chain, with one arg per value.
        #[serde(default)]
        list: bool,
    },
    /// `<`, `<=`, `>`, `>=`, `BETWEEN`.
    Range {
        /// Bounded below (`>`, `>=`, `BETWEEN`).
        lo: bool,
        /// Bounded above (`<`, `<=`, `BETWEEN`).
        hi: bool,
    },
    /// `LIKE 'abc%'`: a literal pattern with a fixed prefix.
    Prefix,
    /// `col IS NULL`.
    IsNull,
    /// `col IS NOT NULL`. An index can serve it as a range (`NULL < col`);
    /// its selectivity is `1 - nulls_ratio`.
    NotNull,
    /// `<>`, `!=`, `NOT IN`, `NOT BETWEEN`, `NOT LIKE`.
    Ne,
    /// `LIKE` with a pattern only known at execution (a `?` or a
    /// non-literal): it may or may not have a fixed prefix.
    Like,
    /// `MATCH (...) AGAINST (...)`.
    Fulltext,
    /// A spatial relation (`ST_Contains`, `MBRIntersects`, ...).
    Spatial,
    /// An `ORDER BY VEC_DISTANCE_*(...)` (`VEC_DISTANCE_EUCLIDEAN`,
    /// `VEC_DISTANCE_COSINE`, `VEC_DISTANCE`).
    Vector,
    /// `JSON_VALUE(doc, '$.x') = v` and the like.
    JsonPath {
        /// The JSON path, as written in the statement.
        path: String,
    },
    /// No index on the column can serve the condition.
    Unsargable {
        /// Why, as `sprigctl explain query` prints it.
        why: Unsargable,
    },
}

impl PredOp {
    /// Whether an ordinary B-tree index on the column could serve the
    /// condition: `eq`, `in`, `range`, `prefix`, `is_null` and `not_null`.
    #[must_use]
    pub const fn is_sargable(&self) -> bool {
        matches!(
            self,
            Self::Eq
                | Self::In { .. }
                | Self::Range { .. }
                | Self::Prefix
                | Self::IsNull
                | Self::NotNull
        )
    }

    /// The operator's name in the JSON.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Eq => "eq",
            Self::In { .. } => "in",
            Self::Range { .. } => "range",
            Self::Prefix => "prefix",
            Self::IsNull => "is_null",
            Self::NotNull => "not_null",
            Self::Ne => "ne",
            Self::Like => "like",
            Self::Fulltext => "fulltext",
            Self::Spatial => "spatial",
            Self::Vector => "vector",
            Self::JsonPath { .. } => "json_path",
            Self::Unsargable { .. } => "unsargable",
        }
    }
}

/// Why a condition on a column can't use an index on it. Serialized as a
/// string: `"function:LOWER"`, `"type_conversion"`, `"collation"`,
/// `"leading_wildcard"`, `"expression"`, `"or"`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Unsargable {
    /// The column is inside a function call: `LOWER(email) = ?`. The
    /// function name is upper case.
    Function(String),
    /// A string column compared with a number, so MariaDB converts every
    /// row's value (needs the resolver's column types).
    TypeConversion,
    /// The column is given another collation: `name COLLATE utf8mb4_bin
    /// = ?`.
    Collation,
    /// `LIKE '%word'` or `LIKE '_x'`: no fixed prefix. Chapter 13 turns a
    /// repeated one into a search-index recommendation.
    LeadingWildcard,
    /// The column is part of an arithmetic or other expression: `a + 1 =
    /// ?`, `a = b + c` on one table.
    Expression,
    /// The column is in an `OR` across different columns, which a single
    /// index can't serve.
    Or,
}

impl Unsargable {
    /// The reason as text.
    #[must_use]
    pub fn as_string(&self) -> String {
        match self {
            Self::Function(f) => format!("function:{f}"),
            other => other.simple_name().unwrap_or_default().to_owned(),
        }
    }

    const fn simple_name(&self) -> Option<&'static str> {
        Some(match self {
            Self::Function(_) => return None,
            Self::TypeConversion => "type_conversion",
            Self::Collation => "collation",
            Self::LeadingWildcard => "leading_wildcard",
            Self::Expression => "expression",
            Self::Or => "or",
        })
    }

    /// Parses the text form. Unknown reasons become
    /// [`Unsargable::Expression`], so a newer writer never breaks a reader.
    #[must_use]
    pub fn parse(s: &str) -> Self {
        if let Some(f) = s.strip_prefix("function:") {
            return Self::Function(f.to_owned());
        }
        match s {
            "type_conversion" => Self::TypeConversion,
            "collation" => Self::Collation,
            "leading_wildcard" => Self::LeadingWildcard,
            "or" => Self::Or,
            _ => Self::Expression,
        }
    }
}

impl fmt::Display for Unsargable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_string())
    }
}

impl Serialize for Unsargable {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.as_string())
    }
}

impl<'de> Deserialize<'de> for Unsargable {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Ok(Self::parse(&s))
    }
}

/// A join condition between two tables. In the JSON, `"l"` and `"r"` are
/// `[alias, column]` pairs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Join {
    /// The left column, as `[t, col]`.
    #[serde(with = "pair")]
    pub l: ColumnRef,
    /// The right column, as `[t, col]`. For a semi-join, the subquery's
    /// column.
    #[serde(with = "pair")]
    pub r: ColumnRef,
    /// The comparison.
    pub op: JoinOp,
    /// The join kind.
    pub kind: JoinKind,
}

/// A join comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinOp {
    /// `l = r` or `l <=> r`.
    Eq,
    /// `<`, `<=`, `>`, `>=` or `BETWEEN` between columns.
    Range,
}

/// How two tables are joined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinKind {
    /// `JOIN`, `INNER JOIN`, `STRAIGHT_JOIN`, or a comma join with the
    /// condition in `WHERE`.
    Inner,
    /// `LEFT [OUTER] JOIN`: `r` is the inner (optional) side.
    Left,
    /// `RIGHT [OUTER] JOIN`: `l` is the inner (optional) side.
    Right,
    /// `CROSS JOIN` with a condition in `WHERE`.
    Cross,
    /// `col IN (SELECT ...)` or a correlated `EXISTS`: `r` is in the
    /// subquery.
    Semi,
    /// `NOT IN (SELECT ...)` or a correlated `NOT EXISTS`.
    Anti,
}

/// An `ORDER BY` column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderItem {
    /// The column.
    #[serde(flatten)]
    pub column: ColumnRef,
    /// The direction.
    pub dir: SortDir,
}

/// A sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SortDir {
    /// Ascending (the default).
    #[default]
    Asc,
    /// Descending.
    Desc,
}

/// What a write statement changes, for write cost (chapter 13, "Cost
/// model").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Writes {
    /// The kind of write.
    pub kind: WriteKind,
    /// The aliases of the tables written, in order (several for multi-table
    /// `UPDATE` and `DELETE`).
    pub targets: Vec<String>,
    /// The columns set: `UPDATE ... SET` columns, the column list of an
    /// `INSERT` or `REPLACE`, and `ON DUPLICATE KEY UPDATE` columns. Empty
    /// for `DELETE`, and for an `INSERT` without a column list (every
    /// column: see [`Writes::all_columns`]).
    pub set: Vec<ColumnRef>,
    /// The write sets every column: `INSERT` or `REPLACE` without a column
    /// list, and `DELETE`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub all_columns: bool,
}

/// A kind of write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteKind {
    /// `INSERT`.
    Insert,
    /// `INSERT ... ON DUPLICATE KEY UPDATE`.
    Upsert,
    /// `REPLACE`.
    Replace,
    /// `UPDATE`.
    Update,
    /// `DELETE`.
    Delete,
}

/// Serde for a [`ColumnRef`] as a `[t, col]` pair.
mod pair {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::ColumnRef;

    #[allow(clippy::ref_option)]
    pub(super) fn serialize<S: Serializer>(c: &ColumnRef, s: S) -> Result<S::Ok, S::Error> {
        (&c.table, &c.col).serialize(s)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<ColumnRef, D::Error> {
        let (table, col) = <(Option<String>, String)>::deserialize(d)?;
        Ok(ColumnRef { table, col })
    }
}
