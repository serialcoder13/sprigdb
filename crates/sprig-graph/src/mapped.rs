//! Mapped graphs (chapter 43, "Language surface", SQL/PGQ's `CREATE
//! PROPERTY GRAPH`; build step 4): a graph declared over existing tables,
//! without copying, and a read-only [`GraphStore`] over them.
//!
//! - **Definition.** [`parse_create`] reads
//!   `CREATE [OR REPLACE] PROPERTY GRAPH [IF NOT EXISTS] [db.]g VERTEX TABLES
//!   (...) [EDGE TABLES (...)]`. A vertex table has `KEY (col)`, one
//!   `LABEL` (default: the table name) and `PROPERTIES (expr [AS name],
//!   ...)`, `PROPERTIES ARE ALL COLUMNS` or `NO PROPERTIES`; an edge table
//!   adds `SOURCE KEY (col) REFERENCES v (col)` and `DESTINATION KEY (col)
//!   REFERENCES v (col)`, where `v` names a vertex table (or its alias) and
//!   `col` is that table's key.
//! - **Built:** one integer key column per table (keys below 2^48), labels
//!   and types unique within the graph, tables in the database's first
//!   shard schema (unsharded). Element ids are `source × 2^48 + key`, so
//!   `id()` works and a node's source is its id's top bits. Other keys and
//!   sharded tables answer 39908; writes answer 39904.
//! - **Store.** Each scan or hop is SQL over the user's tables: a node scan
//!   is `SELECT key, properties FROM table`, a hop reads the edge table by
//!   its source (or destination) column with the far table left-joined.
//!   Property equalities push down as `(expr) IN (...)`, a superset the
//!   gateway rechecks with Cypher's rules.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use async_trait::async_trait;
use mysql_async::consts::ColumnType;
use mysql_async::{Row, Value as MyValue};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json};
use sprig_common::{Result, SprigCode, SprigError};

use crate::ast::Dir;
use crate::store::{EdgeKey, EdgeRow, GraphStore, IdKind, NodeRow, Reach, Scan, StoreStats};

/// Keys below this fit an element id with the source in the top bits.
pub const KEY_LIMIT: u64 = 1 << 48;

/// One vertex table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VertexTable {
    /// Source id (1-based, in definition order).
    pub id: u32,
    /// The table.
    pub table: String,
    /// Its alias (the table name without one).
    pub alias: String,
    /// The key column.
    pub key: String,
    /// The label.
    pub label: String,
    /// `(name, SQL expression)`; `None`: every column.
    pub properties: Option<Vec<(String, String)>>,
}

/// One end of an edge table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeEnd {
    /// The edge table's column.
    pub column: String,
    /// The vertex table's source id.
    pub vertex: u32,
}

/// One edge table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeTable {
    /// Source id (1-based).
    pub id: u32,
    /// The table.
    pub table: String,
    /// Its alias.
    pub alias: String,
    /// The key column.
    pub key: String,
    /// The relationship type.
    pub label: String,
    /// The source end.
    pub src: EdgeEnd,
    /// The destination end.
    pub dst: EdgeEnd,
    /// `(name, SQL expression)`; `None`: every column.
    pub properties: Option<Vec<(String, String)>>,
}

/// A mapped graph's definition (stored as `graphs.definition`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MappedDef {
    /// Vertex tables.
    pub vertices: Vec<VertexTable>,
    /// Edge tables.
    pub edges: Vec<EdgeTable>,
}

/// `CREATE PROPERTY GRAPH` as parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePropertyGraph {
    /// The database, if named.
    pub db: Option<String>,
    /// The graph.
    pub name: String,
    /// `OR REPLACE`.
    pub or_replace: bool,
    /// `IF NOT EXISTS`.
    pub if_not_exists: bool,
    /// The definition.
    pub def: MappedDef,
}

fn bad(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::GraphDefinitionInvalid, msg.into())
}

fn syntax(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::GraphSyntax, msg.into())
}

/// A token of the DDL: a word, a quoted name, a string, punctuation, or a
/// parenthesized raw span (property expressions keep their text).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Word(String),
    Punct(char),
}

/// Splits `sql` into words (identifiers, backquoted names, string
/// literals kept with their quotes, numbers) and punctuation.
fn lex(sql: &str) -> Result<Vec<Tok>> {
    let c: Vec<char> = sql.chars().collect();
    let mut i = 0;
    let mut out = Vec::new();
    while i < c.len() {
        let ch = c[i];
        if ch.is_whitespace() {
            i += 1;
        } else if ch == '`' || ch == '\'' || ch == '"' {
            let q = ch;
            let mut s = String::from(q);
            i += 1;
            loop {
                let Some(&x) = c.get(i) else {
                    return Err(syntax("unterminated quote in CREATE PROPERTY GRAPH"));
                };
                s.push(x);
                i += 1;
                if x == q {
                    if c.get(i) == Some(&q) {
                        s.push(q);
                        i += 1;
                        continue;
                    }
                    break;
                }
            }
            out.push(Tok::Word(s));
        } else if ch.is_alphanumeric() || ch == '_' || ch == '$' {
            let mut s = String::new();
            while i < c.len() && (c[i].is_alphanumeric() || c[i] == '_' || c[i] == '$') {
                s.push(c[i]);
                i += 1;
            }
            out.push(Tok::Word(s));
        } else {
            out.push(Tok::Punct(ch));
            i += 1;
        }
    }
    Ok(out)
}

/// A name without its backquotes.
fn ident(w: &str) -> String {
    w.strip_prefix('`')
        .and_then(|x| x.strip_suffix('`'))
        .map_or_else(|| w.to_owned(), |x| x.replace("``", "`"))
}

struct P {
    t: Vec<Tok>,
    i: usize,
}

impl P {
    fn peek_kw(&self, k: &str) -> bool {
        matches!(self.t.get(self.i), Some(Tok::Word(w)) if w.eq_ignore_ascii_case(k))
    }
    fn kw(&mut self, k: &str) -> bool {
        if self.peek_kw(k) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn expect_kw(&mut self, k: &str) -> Result<()> {
        if self.kw(k) {
            Ok(())
        } else {
            Err(syntax(format!("expected {k} in CREATE PROPERTY GRAPH")))
        }
    }
    fn punct(&mut self, p: char) -> bool {
        if self.t.get(self.i) == Some(&Tok::Punct(p)) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn expect(&mut self, p: char) -> Result<()> {
        if self.punct(p) {
            Ok(())
        } else {
            Err(syntax(format!("expected '{p}' in CREATE PROPERTY GRAPH")))
        }
    }
    fn word(&mut self) -> Result<String> {
        match self.t.get(self.i) {
            Some(Tok::Word(w)) => {
                self.i += 1;
                Ok(w.clone())
            }
            _ => Err(syntax("expected a name in CREATE PROPERTY GRAPH")),
        }
    }
    /// `(a, b)`: one column (several answer 39908: only single keys).
    fn one_column(&mut self, what: &str) -> Result<String> {
        self.expect('(')?;
        let c = ident(&self.word()?);
        if self.punct(',') {
            return Err(bad(format!(
                "{what}: only single-column keys are supported for mapped graphs"
            )));
        }
        self.expect(')')?;
        Ok(c)
    }
    /// The raw text of tokens up to a `,` or `)` at depth 0.
    fn expr(&mut self) -> Result<String> {
        let mut depth = 0;
        let mut s = String::new();
        loop {
            match self.t.get(self.i) {
                None => return Err(syntax("unterminated PROPERTIES list")),
                Some(Tok::Punct(',' | ')')) if depth == 0 => break,
                Some(Tok::Word(w)) if depth == 0 && w.eq_ignore_ascii_case("AS") => break,
                Some(Tok::Punct(p)) => {
                    if *p == '(' {
                        depth += 1;
                    } else if *p == ')' {
                        depth -= 1;
                    }
                    s.push(*p);
                }
                Some(Tok::Word(w)) => {
                    if s.ends_with(|c: char| {
                        c.is_alphanumeric() || c == '_' || c == '`' || c == '\''
                    }) {
                        s.push(' ');
                    }
                    s.push_str(w);
                }
            }
            self.i += 1;
        }
        if s.is_empty() {
            return Err(syntax("empty property expression"));
        }
        Ok(s)
    }
    /// `PROPERTIES (...)`, `PROPERTIES ARE ALL COLUMNS`, `NO PROPERTIES`,
    /// or nothing (all columns).
    fn properties(&mut self) -> Result<Option<Vec<(String, String)>>> {
        if self.kw("NO") {
            self.expect_kw("PROPERTIES")?;
            return Ok(Some(Vec::new()));
        }
        if !self.kw("PROPERTIES") {
            return Ok(None);
        }
        if self.kw("ARE") {
            self.expect_kw("ALL")?;
            self.expect_kw("COLUMNS")?;
            return Ok(None);
        }
        self.expect('(')?;
        let mut v = Vec::new();
        loop {
            let e = self.expr()?;
            let name = if self.kw("AS") {
                ident(&self.word()?)
            } else {
                let plain = ident(&e);
                if plain.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    plain
                } else {
                    return Err(bad(format!("property expression '{e}' needs AS name")));
                }
            };
            if name.contains(['"', '\\', '\'', '`']) {
                return Err(bad(format!("property name '{name}' has quotes")));
            }
            v.push((name, e));
            if self.punct(')') {
                break;
            }
            self.expect(',')?;
        }
        Ok(Some(v))
    }
    fn alias(&mut self, table: &str) -> Result<String> {
        if self.kw("AS") {
            return Ok(ident(&self.word()?));
        }
        Ok(table.to_owned())
    }
    fn label(&mut self, table: &str) -> Result<String> {
        if self.kw("DEFAULT") {
            self.expect_kw("LABEL")?;
            return Ok(table.to_owned());
        }
        if self.kw("LABEL") {
            return Ok(ident(&self.word()?));
        }
        Ok(table.to_owned())
    }
}

/// Parses `CREATE [OR REPLACE] PROPERTY GRAPH ...`; `None` when `sql` is
/// another statement.
///
/// # Errors
/// 39900 for bad syntax; 39908 for definitions this build can't map
/// (composite keys, references to non-key columns, duplicate labels).
#[allow(clippy::too_many_lines)] // one grammar, top to bottom
pub fn parse_create(sql: &str) -> Result<Option<CreatePropertyGraph>> {
    let t = lex(sql.trim().trim_end_matches(';'))?;
    let mut p = P { t, i: 0 };
    if !p.kw("CREATE") {
        return Ok(None);
    }
    let or_replace = if p.kw("OR") {
        p.expect_kw("REPLACE")?;
        true
    } else {
        false
    };
    if !(p.kw("PROPERTY") && p.kw("GRAPH")) {
        return Ok(None);
    }
    let if_not_exists = if p.kw("IF") {
        p.expect_kw("NOT")?;
        p.expect_kw("EXISTS")?;
        true
    } else {
        false
    };
    let first = ident(&p.word()?);
    let (db, name) = if p.punct('.') {
        (Some(first), ident(&p.word()?))
    } else {
        (None, first)
    };
    p.expect_kw("VERTEX")?;
    p.expect_kw("TABLES")?;
    p.expect('(')?;
    let mut def = MappedDef::default();
    loop {
        let table = ident(&p.word()?);
        let alias = p.alias(&table)?;
        let mut key = None;
        let mut label = None;
        let mut props = None;
        loop {
            if p.kw("KEY") {
                key = Some(p.one_column("KEY")?);
            } else if p.peek_kw("LABEL") || p.peek_kw("DEFAULT") {
                label = Some(p.label(&table)?);
            } else if p.peek_kw("PROPERTIES") || p.peek_kw("NO") {
                props = Some(p.properties()?);
            } else {
                break;
            }
        }
        let id = u32::try_from(def.vertices.len() + 1).unwrap_or(u32::MAX);
        def.vertices.push(VertexTable {
            id,
            label: label.unwrap_or_else(|| table.clone()),
            key: key.ok_or_else(|| bad(format!("vertex table {table} needs KEY (column)")))?,
            table,
            alias,
            properties: props.unwrap_or(None),
        });
        if p.punct(')') {
            break;
        }
        p.expect(',')?;
    }
    if p.kw("EDGE") {
        p.expect_kw("TABLES")?;
        p.expect('(')?;
        loop {
            let table = ident(&p.word()?);
            let alias = p.alias(&table)?;
            let mut key = None;
            let mut ends: [Option<EdgeEnd>; 2] = [None, None];
            let mut label = None;
            let mut props = None;
            loop {
                if p.kw("KEY") {
                    key = Some(p.one_column("KEY")?);
                } else if p.peek_kw("SOURCE") || p.peek_kw("DESTINATION") {
                    let which = usize::from(p.kw("DESTINATION"));
                    if which == 0 {
                        p.expect_kw("SOURCE")?;
                    }
                    p.expect_kw("KEY")?;
                    let column = p.one_column("SOURCE / DESTINATION KEY")?;
                    p.expect_kw("REFERENCES")?;
                    let target = ident(&p.word()?);
                    let refcol = p.one_column("REFERENCES")?;
                    let v = def
                        .vertices
                        .iter()
                        .find(|v| v.alias == target || v.table == target)
                        .ok_or_else(|| {
                            bad(format!(
                                "{table} references {target}, which isn't a vertex table"
                            ))
                        })?;
                    if v.key != refcol {
                        return Err(bad(format!(
                            "{table} references {target} ({refcol}): only the vertex table's KEY ({}) can be referenced",
                            v.key
                        )));
                    }
                    ends[which] = Some(EdgeEnd {
                        column,
                        vertex: v.id,
                    });
                } else if p.peek_kw("LABEL") || p.peek_kw("DEFAULT") {
                    label = Some(p.label(&table)?);
                } else if p.peek_kw("PROPERTIES") || p.peek_kw("NO") {
                    props = Some(p.properties()?);
                } else {
                    break;
                }
            }
            let [src, dst] = ends;
            let id = u32::try_from(def.edges.len() + 1).unwrap_or(u32::MAX);
            def.edges.push(EdgeTable {
                id,
                label: label.unwrap_or_else(|| table.clone()),
                key: key.ok_or_else(|| bad(format!("edge table {table} needs KEY (column)")))?,
                src: src.ok_or_else(|| bad(format!("edge table {table} needs SOURCE KEY")))?,
                dst: dst.ok_or_else(|| bad(format!("edge table {table} needs DESTINATION KEY")))?,
                table,
                alias,
                properties: props.unwrap_or(None),
            });
            if p.punct(')') {
                break;
            }
            p.expect(',')?;
        }
    }
    if p.i != p.t.len() {
        return Err(syntax("unexpected text after CREATE PROPERTY GRAPH"));
    }
    for (i, v) in def.vertices.iter().enumerate() {
        if def.vertices[..i].iter().any(|w| w.label == v.label) {
            return Err(bad(format!(
                "label {} is on two vertex tables: mapped graphs need one table per label",
                v.label
            )));
        }
    }
    for (i, e) in def.edges.iter().enumerate() {
        if def.edges[..i].iter().any(|w| w.label == e.label) {
            return Err(bad(format!(
                "type {} is on two edge tables: mapped graphs need one table per type",
                e.label
            )));
        }
    }
    Ok(Some(CreatePropertyGraph {
        db,
        name,
        or_replace,
        if_not_exists,
        def,
    }))
}

/// Runs SQL for a mapped store (the gateway's connection, as `sprig_ctl`
/// after chapter 29's checks on each source table).
#[async_trait]
pub trait SqlRunner: Send {
    /// The rows of `sql`.
    async fn rows(&mut self, sql: &str, params: Vec<MyValue>) -> Result<Vec<Row>>;
}

#[async_trait]
impl SqlRunner for mysql_async::Conn {
    async fn rows(&mut self, sql: &str, params: Vec<MyValue>) -> Result<Vec<Row>> {
        use mysql_async::prelude::Queryable;
        self.exec(sql, mysql_async::Params::Positional(params))
            .await
            .map_err(crate::catalog::backend_error)
    }
}

/// A mapped graph over one schema, read-only.
pub struct MappedStore<'c> {
    run: &'c mut dyn SqlRunner,
    graph: u64,
    schema: String,
    def: MappedDef,
    stats: StoreStats,
}

impl std::fmt::Debug for MappedStore<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MappedStore")
            .field("graph", &self.graph)
            .finish_non_exhaustive()
    }
}

fn read_only() -> SprigError {
    SprigError::new(
        SprigCode::GraphReadOnlySource,
        "a mapped graph's tables are read-only through the graph in this build; write them in SQL",
    )
}

fn q(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn ids_text(ids: &[u64]) -> String {
    let mut s = String::new();
    for (i, x) in ids.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        let _ = write!(s, "{x}");
    }
    s
}

/// A column value as JSON, and its `_types` entry.
fn json_of(v: MyValue, ty: ColumnType) -> (Json, Option<&'static str>) {
    match v {
        MyValue::NULL => (Json::Null, None),
        MyValue::Int(i) => (Json::from(i), None),
        MyValue::UInt(u) => (
            i64::try_from(u).map_or_else(|_| Json::from(u.to_string()), Json::from),
            None,
        ),
        MyValue::Float(f) => (Json::from(f64::from(f)), Some("float")),
        MyValue::Double(f) => (Json::from(f), Some("float")),
        MyValue::Date(y, m, d, h, mi, s, us) => {
            if matches!(
                ty,
                ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE
            ) {
                (Json::from(format!("{y:04}-{m:02}-{d:02}")), Some("date"))
            } else {
                let frac = if us > 0 {
                    format!(".{us:06}")
                } else {
                    String::new()
                };
                (
                    Json::from(format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}{frac}")),
                    Some("localdatetime"),
                )
            }
        }
        MyValue::Time(neg, days, h, mi, s, us) => {
            let h = u32::from(h) + days * 24;
            let frac = if us > 0 {
                format!(".{us:06}")
            } else {
                String::new()
            };
            let sign = if neg { "-" } else { "" };
            (
                Json::from(format!("{sign}{h:02}:{mi:02}:{s:02}{frac}")),
                Some("localtime"),
            )
        }
        MyValue::Bytes(b) => {
            let s = String::from_utf8_lossy(&b).into_owned();
            match ty {
                ColumnType::MYSQL_TYPE_NEWDECIMAL | ColumnType::MYSQL_TYPE_DECIMAL => {
                    match s.parse::<f64>() {
                        Ok(f) => (Json::from(f), Some("float")),
                        Err(_) => (Json::from(s), None),
                    }
                }
                ColumnType::MYSQL_TYPE_LONGLONG
                | ColumnType::MYSQL_TYPE_LONG
                | ColumnType::MYSQL_TYPE_INT24
                | ColumnType::MYSQL_TYPE_SHORT
                | ColumnType::MYSQL_TYPE_TINY => s
                    .parse::<i64>()
                    .map_or_else(|_| (Json::from(s), None), |i| (Json::from(i), None)),
                _ => (Json::from(s), None),
            }
        }
    }
}

/// Columns `from..` of `row` as `props` and `_types`, named `names`.
fn props_at(row: &mut Row, from: usize, names: &[String]) -> (Json, Option<Json>) {
    let cols = row.columns();
    let mut props = Map::new();
    let mut types = Map::new();
    for (k, n) in names.iter().enumerate() {
        let i = from + k;
        let ty = cols.get(i).map_or(
            ColumnType::MYSQL_TYPE_NULL,
            mysql_async::Column::column_type,
        );
        let v = row.take::<MyValue, _>(i).unwrap_or(MyValue::NULL);
        let (j, t) = json_of(v, ty);
        if j.is_null() {
            continue;
        }
        if let Some(t) = t {
            types.insert(n.clone(), Json::from(t));
        }
        props.insert(n.clone(), j);
    }
    (
        Json::Object(props),
        (!types.is_empty()).then_some(Json::Object(types)),
    )
}

fn key_of(v: Option<MyValue>) -> Option<u64> {
    match v? {
        MyValue::Int(i) => u64::try_from(i).ok(),
        MyValue::UInt(u) => Some(u),
        MyValue::Bytes(b) => std::str::from_utf8(&b).ok()?.parse().ok(),
        _ => None,
    }
    .filter(|k| *k < KEY_LIMIT)
}

/// The element id of key `k` of source `s`.
#[must_use]
pub fn element(s: u32, k: u64) -> u64 {
    (u64::from(s) << 48) | k
}

/// `(source, key)` of an element id.
#[must_use]
pub fn split(id: u64) -> (u32, u64) {
    (
        u32::try_from(id >> 48).unwrap_or(u32::MAX),
        id & (KEY_LIMIT - 1),
    )
}

impl<'c> MappedStore<'c> {
    /// Graph `graph` defined by `def`, its tables in `schema`, read through
    /// `run`. Properties listed as "every column" are resolved by the
    /// caller first ([`Self::columns_sql`]).
    #[must_use]
    pub fn new(run: &'c mut dyn SqlRunner, graph: u64, schema: &str, def: MappedDef) -> Self {
        Self {
            run,
            graph,
            schema: schema.to_owned(),
            def,
            stats: StoreStats::default(),
        }
    }

    /// The statement listing `table`'s columns in `schema`
    /// (`information_schema`), to resolve "every column".
    #[must_use]
    pub fn columns_sql() -> &'static str {
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
    }

    fn t(&self, table: &str) -> String {
        format!("{}.{}", q(&self.schema), q(table))
    }

    fn vertex(&self, id: u32) -> Option<&VertexTable> {
        self.def.vertices.iter().find(|v| v.id == id)
    }

    async fn rows(&mut self, sql: &str, params: Vec<MyValue>) -> Result<Vec<Row>> {
        self.stats.shard_queries += 1;
        self.run.rows(sql, params).await
    }

    /// `SELECT expr AS p, ...` of a property list (`alias.` before plain
    /// column names).
    fn select_props(props: &[(String, String)], alias: &str) -> (String, Vec<String>) {
        let mut sql = String::new();
        let mut names = Vec::new();
        for (n, e) in props {
            let plain = e.chars().all(|c| c.is_alphanumeric() || c == '_');
            let expr = if plain {
                format!("{alias}.{}", q(e))
            } else {
                e.clone()
            };
            let _ = write!(sql, ", {expr}");
            names.push(n.clone());
        }
        (sql, names)
    }

    async fn vertex_rows(
        &mut self,
        v: &VertexTable,
        keys: Option<&[u64]>,
        eq: &[(String, Vec<Json>)],
        limit: Option<usize>,
        after: Option<u64>,
    ) -> Result<Vec<NodeRow>> {
        let props = v.properties.clone().unwrap_or_default();
        let (cols, names) = Self::select_props(&props, "v");
        let mut sql = format!(
            "SELECT v.{key}{cols} FROM {} v WHERE 1 = 1",
            self.t(&v.table),
            key = q(&v.key)
        );
        let mut params = Vec::new();
        if let Some(k) = keys {
            if k.is_empty() {
                return Ok(Vec::new());
            }
            let _ = write!(sql, " AND v.{} IN ({})", q(&v.key), ids_text(k));
        }
        for (name, vals) in eq {
            let Some((_, e)) = props.iter().find(|(n, _)| n == name) else {
                if !vals.is_empty() {
                    return Ok(Vec::new()); // the property doesn't exist here
                }
                continue;
            };
            let plain = e.chars().all(|c| c.is_alphanumeric() || c == '_');
            let expr = if plain {
                format!("v.{}", q(e))
            } else {
                e.clone()
            };
            let ps: Vec<MyValue> = vals
                .iter()
                .filter_map(|j| match j {
                    Json::String(s) => Some(MyValue::from(s.as_str())),
                    Json::Number(n) => n
                        .as_i64()
                        .map(MyValue::from)
                        .or_else(|| n.as_f64().map(MyValue::from)),
                    Json::Bool(b) => Some(MyValue::from(*b)),
                    _ => None,
                })
                .collect();
            if ps.len() != vals.len() || ps.is_empty() {
                continue;
            }
            let marks = vec!["?"; ps.len()].join(", ");
            let _ = write!(sql, " AND ({expr}) IN ({marks})");
            params.extend(ps);
        }
        if let Some(a) = after {
            let (s, k) = split(a);
            let floor = match s.cmp(&v.id) {
                std::cmp::Ordering::Less => 0,
                std::cmp::Ordering::Greater => return Ok(Vec::new()),
                std::cmp::Ordering::Equal => k + 1,
            };
            let _ = write!(
                sql,
                " AND v.{} >= {floor} ORDER BY v.{}",
                q(&v.key),
                q(&v.key)
            );
        }
        if let Some(l) = limit {
            let _ = write!(sql, " LIMIT {l}");
        }
        let rows = self.rows(&sql, params).await?;
        let mut out = Vec::with_capacity(rows.len());
        for mut r in rows {
            let Some(k) = key_of(r.take(0)) else { continue };
            let (props, types) = props_at(&mut r, 1, &names);
            out.push(NodeRow {
                id: element(v.id, k),
                rk: 0,
                labels: vec![v.id],
                props,
                types,
            });
        }
        Ok(out)
    }

    /// Edges of table `e` whose `near` end is in `keys`, with the far
    /// vertex joined.
    async fn edge_rows(
        &mut self,
        e: &EdgeTable,
        near_out: bool,
        keys: Option<&[u64]>,
        ids: Option<&[u64]>,
    ) -> Result<Vec<(EdgeRow, Option<NodeRow>)>> {
        let (near, far) = if near_out {
            (&e.src, &e.dst)
        } else {
            (&e.dst, &e.src)
        };
        let Some(fv) = self.vertex(far.vertex).cloned() else {
            return Ok(Vec::new());
        };
        let eprops = e.properties.clone().unwrap_or_default();
        let fprops = fv.properties.clone().unwrap_or_default();
        let (ecols, enames) = Self::select_props(&eprops, "e");
        let (fcols, fnames) = Self::select_props(&fprops, "f");
        let mut sql = format!(
            "SELECT e.{ek}, e.{sc}, e.{dc}{ecols}, f.{fk}{fcols} FROM {et} e LEFT JOIN {ft} f ON f.{fk} = e.{fc} WHERE 1 = 1",
            ek = q(&e.key),
            sc = q(&e.src.column),
            dc = q(&e.dst.column),
            fk = q(&fv.key),
            fc = q(&far.column),
            et = self.t(&e.table),
            ft = self.t(&fv.table),
        );
        if let Some(k) = keys {
            if k.is_empty() {
                return Ok(Vec::new());
            }
            let _ = write!(sql, " AND e.{} IN ({})", q(&near.column), ids_text(k));
        }
        if let Some(i) = ids {
            if i.is_empty() {
                return Ok(Vec::new());
            }
            let _ = write!(sql, " AND e.{} IN ({})", q(&e.key), ids_text(i));
        }
        let rows = self.rows(&sql, Vec::new()).await?;
        let mut out = Vec::with_capacity(rows.len());
        for mut r in rows {
            let (Some(k), Some(s), Some(d)) =
                (key_of(r.take(0)), key_of(r.take(1)), key_of(r.take(2)))
            else {
                continue;
            };
            let (props, types) = props_at(&mut r, 3, &enames);
            let fpos = 3 + enames.len();
            let far_node = key_of(r.take(fpos)).map(|fk| {
                let (p, t) = props_at(&mut r, fpos + 1, &fnames);
                NodeRow {
                    id: element(fv.id, fk),
                    rk: 0,
                    labels: vec![fv.id],
                    props: p,
                    types: t,
                }
            });
            out.push((
                EdgeRow {
                    id: element(e.id, k),
                    src: element(e.src.vertex, s),
                    dst: element(e.dst.vertex, d),
                    src_rk: 0,
                    dst_rk: 0,
                    rtype: e.id,
                    props,
                    types,
                },
                far_node,
            ));
        }
        Ok(out)
    }
}

/// Node ids grouped by source: `source -> keys`.
fn by_source(ids: &[u64]) -> BTreeMap<u32, Vec<u64>> {
    let mut m: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
    for &i in ids {
        let (s, k) = split(i);
        m.entry(s).or_default().push(k);
    }
    m
}

#[async_trait]
impl GraphStore for MappedStore<'_> {
    fn graph_id(&self) -> u64 {
        self.graph
    }

    // Read-only: a batch has nothing to commit.
    async fn batches(&mut self) -> Result<()> {
        Ok(())
    }

    async fn end_batch(&mut self, _ok: bool, _deleted: &[u64]) -> Result<()> {
        Ok(())
    }

    fn shards(&self) -> usize {
        1
    }

    fn shard_of(&self, _rk: u64) -> usize {
        0
    }

    fn stats(&self) -> StoreStats {
        self.stats
    }

    async fn scan(&mut self, s: &Scan) -> Result<Vec<NodeRow>> {
        let vs: Vec<VertexTable> = self
            .def
            .vertices
            .iter()
            .filter(|v| s.label.is_none_or(|l| l == v.id))
            .cloned()
            .collect();
        let mut out = Vec::new();
        for v in vs {
            let keys: Option<Vec<u64>> = s.ids.as_ref().map(|ids| {
                ids.iter()
                    .filter_map(|i| {
                        let (src, k) = split(*i);
                        (src == v.id).then_some(k)
                    })
                    .collect()
            });
            let lim = s.limit.map(|l| l.saturating_sub(out.len()));
            if lim == Some(0) {
                break;
            }
            out.extend(
                self.vertex_rows(&v, keys.as_deref(), &s.props_eq, lim, s.after)
                    .await?,
            );
        }
        out.sort_by_key(|n| n.id);
        if let Some(l) = s.limit {
            out.truncate(l);
        }
        Ok(out)
    }

    async fn nodes(&mut self, ids: &[u64]) -> Result<Vec<NodeRow>> {
        let mut out = Vec::new();
        for (s, keys) in by_source(ids) {
            if let Some(v) = self.vertex(s).cloned() {
                out.extend(self.vertex_rows(&v, Some(&keys), &[], None, None).await?);
            }
        }
        Ok(out)
    }

    async fn expand(
        &mut self,
        from: &[u64],
        types: &[u32],
        dir: Dir,
    ) -> Result<Vec<(EdgeRow, Option<NodeRow>)>> {
        let out_dir = match dir {
            Dir::Out => true,
            Dir::In => false,
            Dir::Both => return Err(SprigError::internal("expand takes one direction")),
        };
        let groups = by_source(from);
        let es: Vec<EdgeTable> = self
            .def
            .edges
            .iter()
            .filter(|e| types.is_empty() || types.contains(&e.id))
            .cloned()
            .collect();
        let mut out = Vec::new();
        for e in es {
            let near = if out_dir { e.src.vertex } else { e.dst.vertex };
            if let Some(keys) = groups.get(&near) {
                for batch in keys.chunks(1000) {
                    out.extend(self.edge_rows(&e, out_dir, Some(batch), None).await?);
                }
            }
        }
        Ok(out)
    }

    async fn reach(
        &mut self,
        _shard: usize,
        seeds: &[(u64, u64)],
        types: &[u32],
        dir: Dir,
        hops: u32,
    ) -> Result<Vec<Reach>> {
        // Level by level through `expand`: each node once per origin, at its
        // smallest depth (what the CTE answers on a native graph).
        self.stats.cte_queries += 1;
        let mut best: BTreeMap<(u64, u64), u32> = BTreeMap::new();
        let mut frontier: Vec<(u64, u64)> = seeds.to_vec();
        for d in 1..=hops {
            if frontier.is_empty() {
                break;
            }
            let nodes: Vec<u64> = {
                let mut v: Vec<u64> = frontier.iter().map(|(_, n)| *n).collect();
                v.sort_unstable();
                v.dedup();
                v
            };
            let hop = self.expand(&nodes, types, dir).await?;
            let mut next = Vec::new();
            for (o, n) in &frontier {
                for (e, _) in &hop {
                    let (near, far) = if dir == Dir::Out {
                        (e.src, e.dst)
                    } else {
                        (e.dst, e.src)
                    };
                    if near == *n && !best.contains_key(&(*o, far)) {
                        best.insert((*o, far), d);
                        next.push((*o, far));
                    }
                }
            }
            frontier = next;
        }
        Ok(best
            .into_iter()
            .map(|((origin, node), depth)| Reach {
                origin,
                node,
                rk: 0,
                depth,
            })
            .collect())
    }

    async fn edges(&mut self, ids: &[u64]) -> Result<Vec<EdgeRow>> {
        let mut out = Vec::new();
        for (s, keys) in by_source(ids) {
            if let Some(e) = self.def.edges.iter().find(|e| e.id == s).cloned() {
                out.extend(
                    self.edge_rows(&e, true, None, Some(&keys))
                        .await?
                        .into_iter()
                        .map(|(e, _)| e),
                );
            }
        }
        Ok(out)
    }

    async fn edges_touching(&mut self, nodes: &[u64], limit: usize) -> Result<Vec<EdgeRow>> {
        let mut out: Vec<EdgeRow> = Vec::new();
        for d in [Dir::Out, Dir::In] {
            for (e, _) in self.expand(nodes, &[], d).await? {
                if !out.iter().any(|x| x.id == e.id) {
                    out.push(e);
                }
            }
        }
        out.truncate(limit.saturating_add(1));
        Ok(out)
    }

    async fn lock_nodes(&mut self, ids: &[u64], _exclusive: bool) -> Result<Vec<NodeRow>> {
        self.nodes(ids).await
    }

    async fn alloc_ids(&mut self, _kind: IdKind, _n: usize) -> Result<Vec<u64>> {
        Err(read_only())
    }

    async fn insert_nodes(&mut self, _rows: &[NodeRow]) -> Result<()> {
        Err(read_only())
    }

    async fn insert_edges(&mut self, _rows: &[EdgeRow]) -> Result<()> {
        Err(read_only())
    }

    async fn update_node(&mut self, _row: &NodeRow, _old: &[u32]) -> Result<()> {
        Err(read_only())
    }

    async fn update_edge(&mut self, _row: &EdgeRow) -> Result<()> {
        Err(read_only())
    }

    async fn delete_edges(&mut self, _edges: &[EdgeKey]) -> Result<u64> {
        Err(read_only())
    }

    async fn delete_nodes(&mut self, _ids: &[u64]) -> Result<u64> {
        Err(read_only())
    }

    async fn label_id(&mut self, name: &str, create: bool) -> Result<Option<u32>> {
        match self.def.vertices.iter().find(|v| v.label == name) {
            Some(v) => Ok(Some(v.id)),
            None if create => Err(SprigError::new(
                SprigCode::GraphReadOnlySource,
                format!("a mapped graph's labels are fixed; there is no label {name}"),
            )),
            None => Ok(None),
        }
    }

    async fn type_id(&mut self, name: &str, create: bool) -> Result<Option<u32>> {
        match self.def.edges.iter().find(|e| e.label == name) {
            Some(e) => Ok(Some(e.id)),
            None if create => Err(SprigError::new(
                SprigCode::GraphReadOnlySource,
                format!("a mapped graph's types are fixed; there is no type {name}"),
            )),
            None => Ok(None),
        }
    }

    fn label_name(&self, id: u32) -> Option<String> {
        self.vertex(id).map(|v| v.label.clone())
    }

    fn type_name(&self, id: u32) -> Option<String> {
        self.def
            .edges
            .iter()
            .find(|e| e.id == id)
            .map(|e| e.label.clone())
    }

    fn all_labels(&self) -> Vec<String> {
        self.def.vertices.iter().map(|v| v.label.clone()).collect()
    }

    fn all_types(&self) -> Vec<String> {
        self.def.edges.iter().map(|e| e.label.clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHOP: &str = "CREATE PROPERTY GRAPH shop
      VERTEX TABLES (
        customers KEY (id) LABEL Person PROPERTIES (id, name, email),
        products AS p KEY (sku) LABEL Product PROPERTIES (sku, name, price),
        reviews KEY (rid) LABEL Review PROPERTIES (JSON_VALUE(doc, '$.stars') AS stars))
      EDGE TABLES (
        orders KEY (id)
          SOURCE KEY (customer_id) REFERENCES customers (id)
          DESTINATION KEY (sku) REFERENCES p (sku)
          LABEL BOUGHT PROPERTIES (qty, placed_at));";

    #[test]
    fn parses_the_chapters_example() {
        let c = parse_create(SHOP).expect("ok").expect("some");
        assert_eq!(c.name, "shop");
        assert_eq!(c.def.vertices.len(), 3);
        assert_eq!(c.def.vertices[1].alias, "p");
        assert_eq!(c.def.vertices[1].key, "sku");
        assert_eq!(
            c.def.vertices[2].properties.as_deref(),
            Some(&[("stars".to_owned(), "JSON_VALUE(doc,'$.stars')".to_owned())][..])
        );
        let e = &c.def.edges[0];
        assert_eq!((e.src.vertex, e.dst.vertex), (1, 2));
        assert_eq!(e.label, "BOUGHT");
        assert!(parse_create("CREATE GRAPH x").expect("ok").is_none());
        let e = parse_create("CREATE PROPERTY GRAPH g VERTEX TABLES (a KEY (x, y))")
            .expect_err("composite");
        assert_eq!(e.code(), Some(SprigCode::GraphDefinitionInvalid));
        let e = parse_create(
            "CREATE PROPERTY GRAPH g VERTEX TABLES (a KEY (x)) EDGE TABLES (e KEY (i) SOURCE KEY (s) REFERENCES a (y) DESTINATION KEY (d) REFERENCES a (x))",
        )
        .expect_err("non-key reference");
        assert!(e.message.contains("only the vertex table's KEY"), "{e}");
        let c = parse_create(
            "create or replace property graph if not exists d.g vertex tables (t key (k) properties are all columns)",
        )
        .expect("ok")
        .expect("some");
        assert!(c.or_replace && c.if_not_exists);
        assert_eq!(c.db.as_deref(), Some("d"));
        assert_eq!(c.def.vertices[0].label, "t");
        assert_eq!(c.def.vertices[0].properties, None);
    }

    #[test]
    fn element_ids_carry_the_source() {
        let id = element(3, 12345);
        assert_eq!(split(id), (3, 12345));
    }
}
