//! Planning a batch into row operations, and the SQL of both dialects
//! (chapter 51, "Database target": upsert with the version guard, deletes,
//! append, the offsets and seen-keys tables, schema policies).

// `ops` and `opts` read best side by side.
#![allow(clippy::similar_names)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use serde_json::{Map, Value};
use sprig_common::Result;

use super::{DbErr, DbOpts, DbResult, Param};
use crate::model::{ChangeOp, Position, SinkRecord, bad_option};

/// The destination's SQL dialect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    /// MySQL and MariaDB.
    Mysql,
    /// PostgreSQL.
    Postgres,
}

/// `mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Upsert by the destination's primary key.
    Upsert,
    /// Insert every record as a row.
    Append,
}

impl Mode {
    /// Parses `upsert` or `append`.
    ///
    /// # Errors
    /// 39218.
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "upsert" => Ok(Mode::Upsert),
            "append" => Ok(Mode::Append),
            o => Err(bad_option(format!(
                "mode must be upsert or append, not '{o}'"
            ))),
        }
    }
}

/// `delete_mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteMode {
    /// Delete the row.
    Delete,
    /// Set `_sprig_deleted`.
    Soft,
    /// Ignore deletes.
    Ignore,
}

impl DeleteMode {
    /// Parses `delete`, `soft` or `ignore`.
    ///
    /// # Errors
    /// 39218.
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "delete" => Ok(DeleteMode::Delete),
            "soft" => Ok(DeleteMode::Soft),
            "ignore" => Ok(DeleteMode::Ignore),
            o => Err(bad_option(format!(
                "delete_mode must be delete, soft or ignore, not '{o}'"
            ))),
        }
    }
}

/// `schema_policy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaPolicy {
    /// The table must exist with every mapped column.
    Strict,
    /// New columns are added, nullable.
    AddColumns,
    /// Missing tables are created too.
    Create,
}

impl SchemaPolicy {
    /// Parses `strict`, `add_columns` or `create`.
    ///
    /// # Errors
    /// 39218.
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "strict" => Ok(SchemaPolicy::Strict),
            "add_columns" => Ok(SchemaPolicy::AddColumns),
            "create" => Ok(SchemaPolicy::Create),
            o => Err(bad_option(format!(
                "schema_policy must be strict, add_columns or create, not '{o}'"
            ))),
        }
    }

    /// The option's text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            SchemaPolicy::Strict => "strict",
            SchemaPolicy::AddColumns => "add_columns",
            SchemaPolicy::Create => "create",
        }
    }
}

/// One destination column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Col {
    /// Its name, as the destination spells it.
    pub name: String,
    /// The type values are cast to (PostgreSQL, without a modifier, so a
    /// value too long fails instead of being cut) or the data type
    /// (MySQL).
    pub cast: String,
    /// `JSON` or `JSONB`: values go in as JSON text.
    pub json: bool,
    /// Binary: `{"base64": ...}` values are decoded.
    pub binary: bool,
}

/// A destination table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    /// The name as the sink wrote it.
    pub name: String,
    /// Its columns.
    pub cols: Vec<Col>,
    /// Its primary key's columns, in key order.
    pub pk: Vec<String>,
}

impl TableInfo {
    /// The column `name` (any case).
    #[must_use]
    pub fn col(&self, name: &str) -> Option<&Col> {
        self.cols
            .iter()
            .find(|c| c.name == name)
            .or_else(|| self.cols.iter().find(|c| c.name.eq_ignore_ascii_case(name)))
    }
}

/// What an operation does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpKind {
    /// Insert or update, guarded by `_sprig_ver`.
    Upsert,
    /// Delete, guarded by `_sprig_ver`.
    Delete,
    /// Insert, duplicates of `_sprig_idem` dropped.
    Append,
}

/// One row operation.
#[derive(Debug, Clone, PartialEq)]
pub struct Op {
    /// The record's index in the batch.
    pub rec: usize,
    /// What it does.
    pub kind: OpKind,
    /// The destination table.
    pub table: String,
    /// Mapped columns and values.
    pub cols: Vec<(String, Value)>,
    /// The row before an update, when the record has it (a key change is
    /// a delete of the old key).
    pub old: Option<Vec<(String, Value)>>,
    /// `_sprig_ver`.
    pub ver: [u8; 16],
    /// Soft delete: `_sprig_deleted = true`.
    pub deleted: bool,
    /// `_sprig_op`.
    pub op: &'static str,
    /// `_sprig_ts`.
    pub ts: i64,
    /// `_sprig_idem`.
    pub idem: String,
    /// Key columns the record names (its key as a JSON object), for
    /// `create`.
    pub key_cols: Vec<String>,
}

/// A statement with its parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stmt {
    /// The text.
    pub sql: String,
    /// The values.
    pub params: Vec<Param>,
}

/// The record's `_sprig_ver` (see the module docs of `db`).
#[must_use]
pub fn version_of(r: &SinkRecord) -> [u8; 16] {
    let mut v = [0u8; 16];
    match &r.pos {
        Position::Stream { offset, .. } => v[8..].copy_from_slice(&offset.to_be_bytes()),
        Position::Change { seq, .. } => {
            v[..8].copy_from_slice(&r.ts_ms.max(0).to_be_bytes());
            v[8..].copy_from_slice(&seq.to_be_bytes());
        }
        Position::Queue { msg_id, .. } => {
            v[..8].copy_from_slice(&r.ts_ms.max(0).to_be_bytes());
            let h = ring::digest::digest(&ring::digest::SHA256, msg_id.as_bytes());
            v[8..].copy_from_slice(&h.as_ref()[..8]);
        }
    }
    v
}

/// Quotes an identifier.
#[must_use]
pub fn ident(d: Dialect, name: &str) -> String {
    match d {
        Dialect::Mysql => format!("`{}`", name.replace('`', "``")),
        Dialect::Postgres => format!("\"{}\"", name.replace('"', "\"\"")),
    }
}

/// Quotes a possibly qualified table name (`schema.table`).
#[must_use]
pub fn table_ident(d: Dialect, name: &str) -> String {
    name.split('.')
        .map(|p| ident(d, p))
        .collect::<Vec<_>>()
        .join(".")
}

/// `TABLE '...'` with `{table}`, `{db}` and `{source}` filled in; a change
/// record's table is its `sprig.table` header, else the source's name.
#[must_use]
pub fn render_table(template: &str, r: &SinkRecord, opts: &DbOpts) -> String {
    if !template.contains('{') {
        return template.to_owned();
    }
    let table = r
        .headers
        .iter()
        .find(|(k, _)| k == "sprig.table")
        .map_or_else(
            || opts.source_name.clone(),
            |(_, v)| String::from_utf8_lossy(v).into_owned(),
        );
    template
        .replace("{table}", &table)
        .replace("{db}", &opts.db_name)
        .replace("{source}", &opts.source_name)
}

/// A JSON object's members as columns, renamed by `column_map` and sorted
/// by name (so statements don't depend on the JSON map's order).
fn mapped(obj: &Map<String, Value>, opts: &DbOpts) -> Vec<(String, Value)> {
    let mut v: Vec<(String, Value)> = obj
        .iter()
        .map(|(k, v)| {
            (
                opts.column_map.get(k).cloned().unwrap_or_else(|| k.clone()),
                v.clone(),
            )
        })
        .collect();
    v.sort_by(|a, b| a.0.cmp(&b.0));
    v
}

fn key_object(r: &SinkRecord) -> Option<Map<String, Value>> {
    let k = r.key.as_ref()?;
    match serde_json::from_slice::<Value>(k).ok()? {
        Value::Object(m) => Some(m),
        _ => None,
    }
}

/// The row a stream or queue record carries: its value's JSON object, or
/// `{"key": ..., "value": ...}` for any other value.
fn value_row(r: &SinkRecord) -> Option<Map<String, Value>> {
    match r.value_json() {
        Value::Object(m) => Some(m),
        Value::Null => None,
        v => {
            let mut m = Map::new();
            m.insert(
                "key".into(),
                r.key_text().map_or(Value::Null, Value::String),
            );
            m.insert("value".into(), v);
            Some(m)
        }
    }
}

/// Plans a batch into row operations, in record order.
///
/// # Errors
/// Never for well-formed records; `DbErr::Row` for a change record without
/// the image it needs.
pub fn plan(records: &[SinkRecord], opts: &DbOpts, d: Dialect) -> DbResult<Vec<Op>> {
    let mut out = plan_rows(records, opts, d)?;
    for op in &mut out {
        let Some(m) = records.get(op.rec).and_then(crate::source::record_member) else {
            continue;
        };
        if !op.cols.iter().any(|(c, _)| c == TENANT_COL) {
            op.cols.insert(0, (TENANT_COL.to_owned(), Value::from(m)));
        }
        if let Some(old) = op.old.as_mut()
            && !old.iter().any(|(c, _)| c == TENANT_COL)
        {
            old.insert(0, (TENANT_COL.to_owned(), Value::from(m)));
        }
    }
    Ok(out)
}

/// The column a fan-in sink's database rows name their member in.
pub const TENANT_COL: &str = "_sprig_tenant";

fn plan_rows(records: &[SinkRecord], opts: &DbOpts, _d: Dialect) -> DbResult<Vec<Op>> {
    let mut out = Vec::with_capacity(records.len());
    for (i, r) in records.iter().enumerate() {
        let table = render_table(&opts.table, r, opts);
        let ver = version_of(r);
        let mut key_cols: Vec<String> = key_object(r)
            .map(|m| mapped(&m, opts).into_iter().map(|(c, _)| c).collect())
            .unwrap_or_default();
        // A fan-in sink's record carries its member in `_sprig_tenant`,
        // part of the key: equal keys of two members are two rows (R9).
        let member = crate::source::record_member(r);
        if member.is_some() && !key_cols.is_empty() {
            key_cols.insert(0, TENANT_COL.to_owned());
        }
        let base = |kind, cols, old, deleted, op| Op {
            rec: i,
            kind,
            table: table.clone(),
            cols,
            old,
            ver,
            deleted,
            op,
            ts: r.ts_ms,
            idem: r.idem_key.clone(),
            key_cols: key_cols.clone(),
        };
        let image = |v: &Option<Value>| -> Option<Vec<(String, Value)>> {
            match v {
                Some(Value::Object(m)) => Some(mapped(m, opts)),
                _ => None,
            }
        };
        if opts.mode == super::Mode::Append {
            let (row, op) = match r.op {
                Some(ChangeOp::Delete) => (image(&r.before), "d"),
                Some(op) => (image(&r.after), op.letter()),
                None => (value_row(r).map(|m| mapped(&m, opts)), "c"),
            };
            out.push(base(
                OpKind::Append,
                row.unwrap_or_default(),
                None,
                false,
                op,
            ));
            continue;
        }
        let deletion = |row: Vec<(String, Value)>| match opts.delete_mode {
            super::DeleteMode::Delete => Some(base(OpKind::Delete, row, None, false, "d")),
            super::DeleteMode::Soft => Some(base(OpKind::Upsert, row, None, true, "d")),
            super::DeleteMode::Ignore => None,
        };
        match r.op {
            Some(ChangeOp::Delete) => {
                let row = image(&r.before).or_else(|| key_object(r).map(|m| mapped(&m, opts)));
                let Some(row) = row else {
                    return Err(DbErr::Row(format!(
                        "delete record {} carries no before image or key",
                        r.idem_key
                    )));
                };
                out.extend(deletion(row));
            }
            Some(op) => {
                let Some(row) = image(&r.after) else {
                    return Err(DbErr::Row(format!(
                        "change record {} carries no after image",
                        r.idem_key
                    )));
                };
                let old = if op == ChangeOp::Update {
                    image(&r.before)
                } else {
                    None
                };
                out.push(base(OpKind::Upsert, row, old, false, op.letter()));
            }
            None => match value_row(r) {
                Some(m) => out.push(base(OpKind::Upsert, mapped(&m, opts), None, false, "c")),
                None => {
                    // A tombstone: a null value with a key deletes it.
                    if let Some(k) = key_object(r) {
                        out.extend(deletion(mapped(&k, opts)));
                    }
                }
            },
        }
    }
    Ok(out)
}

/// Every column the operations write, with a sample value (for types),
/// plus the sink's own columns.
#[must_use]
pub fn needed_columns(ops: &[&Op], opts: &DbOpts) -> Vec<(String, Value)> {
    let mut seen: BTreeMap<String, Value> = BTreeMap::new();
    let mut order: Vec<String> = Vec::new();
    for op in ops {
        if op.kind == OpKind::Delete {
            continue;
        }
        for (c, v) in &op.cols {
            match seen.get(c) {
                None => {
                    seen.insert(c.clone(), v.clone());
                    order.push(c.clone());
                }
                Some(Value::Null) if !v.is_null() => {
                    seen.insert(c.clone(), v.clone());
                }
                _ => {}
            }
        }
    }
    let mut out: Vec<(String, Value)> = order
        .into_iter()
        .map(|c| {
            let v = seen.remove(&c).unwrap_or(Value::Null);
            (c, v)
        })
        .collect();
    let own: &[&str] = match opts.mode {
        super::Mode::Upsert if opts.delete_mode == super::DeleteMode::Soft => {
            &["_sprig_deleted", "_sprig_ver"]
        }
        super::Mode::Upsert => &["_sprig_ver"],
        super::Mode::Append => &["_sprig_op", "_sprig_ts", "_sprig_idem"],
    };
    for c in own {
        if !out.iter().any(|(n, _)| n == c) {
            out.push(((*c).to_owned(), Value::Null));
        }
    }
    out
}

/// The primary key a `create` table gets: `_sprig_idem` in append mode,
/// else `primary_key`, else the records' key fields.
#[must_use]
pub fn create_key(ops: &[&Op], opts: &DbOpts) -> Vec<String> {
    if opts.mode == super::Mode::Append {
        return vec!["_sprig_idem".into()];
    }
    if !opts.primary_key.is_empty() {
        let mut k: Vec<String> = opts
            .primary_key
            .iter()
            .map(|c| opts.column_map.get(c).cloned().unwrap_or_else(|| c.clone()))
            .collect();
        // A fan-in sink's rows: equal keys of two members are two rows.
        if !k.iter().any(|c| c == TENANT_COL)
            && ops
                .iter()
                .any(|o| o.cols.iter().any(|(c, _)| c == TENANT_COL))
        {
            k.insert(0, TENANT_COL.to_owned());
        }
        return k;
    }
    ops.iter()
        .find(|o| !o.key_cols.is_empty())
        .map(|o| o.key_cols.clone())
        .unwrap_or_default()
}

/// The type a new column gets.
#[must_use]
pub fn type_for(d: Dialect, name: &str, sample: &Value, key: bool) -> &'static str {
    let my = d == Dialect::Mysql;
    match name {
        "_sprig_ver" => return if my { "VARBINARY(16)" } else { "BYTEA" },
        "_sprig_deleted" => return "BOOLEAN NOT NULL DEFAULT FALSE",
        "_sprig_op" => return "VARCHAR(1)",
        "_sprig_ts" => return "BIGINT",
        "_sprig_idem" => return "VARCHAR(200)",
        TENANT_COL => return "BIGINT",
        _ => {}
    }
    match sample {
        Value::Bool(_) => "BOOLEAN",
        Value::Number(n) if n.is_i64() || n.is_u64() => "BIGINT",
        Value::Number(_) => {
            if my {
                "DOUBLE"
            } else {
                "DOUBLE PRECISION"
            }
        }
        Value::Object(o) if o.len() == 1 && o.contains_key("base64") => {
            if my {
                "LONGBLOB"
            } else {
                "BYTEA"
            }
        }
        Value::Object(_) | Value::Array(_) => {
            if my {
                "JSON"
            } else {
                "JSONB"
            }
        }
        Value::String(_) | Value::Null => {
            if key {
                "VARCHAR(255)"
            } else if my {
                "LONGTEXT"
            } else {
                "TEXT"
            }
        }
    }
}

/// `CREATE TABLE` for `schema_policy = create`.
///
/// # Errors
/// The reason when the table can have no primary key.
pub fn create_table_sql(
    d: Dialect,
    name: &str,
    cols: &[(String, Value)],
    pk: &[String],
    opts: &DbOpts,
) -> std::result::Result<String, String> {
    if pk.is_empty() {
        return Err(format!(
            "table {name} doesn't exist and the records name no key: set primary_key to create it"
        ));
    }
    let mut defs = Vec::new();
    let mut have: BTreeSet<&str> = BTreeSet::new();
    for (c, v) in cols {
        let key = pk.iter().any(|k| k == c);
        let mut t = type_for(d, c, v, key).to_owned();
        if key && !t.contains("NOT NULL") {
            t.push_str(" NOT NULL");
        }
        defs.push(format!("{} {t}", ident(d, c)));
        have.insert(c);
    }
    for k in pk {
        if !have.contains(k.as_str()) {
            defs.push(format!(
                "{} {} NOT NULL",
                ident(d, k),
                type_for(d, k, &Value::Null, true)
            ));
        }
    }
    let _ = opts;
    defs.push(format!(
        "PRIMARY KEY ({})",
        pk.iter()
            .map(|k| ident(d, k))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        table_ident(d, name),
        defs.join(", ")
    ))
}

/// `ALTER TABLE ... ADD COLUMN` for `add_columns` and `create`.
#[must_use]
pub fn add_column_sql(d: Dialect, table: &str, col: &str, sample: &Value) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMN {} {}",
        table_ident(d, table),
        ident(d, col),
        type_for(d, col, sample, false)
    )
}

fn ph(d: Dialect, n: &mut usize, cast: Option<&str>) -> String {
    match d {
        Dialect::Mysql => "?".to_owned(),
        Dialect::Postgres => {
            *n += 1;
            match cast {
                Some(t) => format!("CAST(${n} AS {t})"),
                None => format!("${n}"),
            }
        }
    }
}

/// A value as a parameter for `col`.
#[must_use]
pub fn value_param(d: Dialect, col: &Col, v: &Value) -> Param {
    match v {
        Value::Null => Param::Null,
        _ if col.json => Param::Text(v.to_string()),
        Value::Object(o) if col.binary && o.len() == 1 => match o.get("base64") {
            Some(Value::String(s)) => base64::engine::general_purpose::STANDARD
                .decode(s)
                .map_or_else(|_| Param::Text(v.to_string()), Param::Bytes),
            _ => Param::Text(v.to_string()),
        },
        Value::String(s) => Param::Text(s.clone()),
        Value::Bool(b) => Param::Text(
            match (d, b) {
                (Dialect::Mysql, true) => "1",
                (Dialect::Mysql, false) => "0",
                (Dialect::Postgres, true) => "true",
                (Dialect::Postgres, false) => "false",
            }
            .to_owned(),
        ),
        Value::Number(n) => Param::Text(n.to_string()),
        Value::Object(_) | Value::Array(_) => Param::Text(v.to_string()),
    }
}

fn cast_of(d: Dialect, c: &Col) -> Option<&str> {
    (d == Dialect::Postgres).then_some(c.cast.as_str())
}

/// Most parameters one statement binds (both servers stop at 65,535).
const MAX_PARAMS: usize = 30_000;

fn lookup<'a>(t: &'a TableInfo, c: &str) -> DbResult<&'a Col> {
    t.col(c)
        .ok_or_else(|| DbErr::Schema(format!("table {} has no column {c}", t.name)))
}

fn pk_values<'a>(t: &TableInfo, row: &'a [(String, Value)]) -> Option<Vec<&'a Value>> {
    t.pk.iter()
        .map(|k| {
            row.iter()
                .find(|(c, _)| c == k || c.eq_ignore_ascii_case(k))
                .map(|(_, v)| v)
        })
        .collect()
}

/// The statements of `ops`, in order: consecutive operations of one kind,
/// table and column set share a multi-row statement of at most
/// `db.batch_rows` rows.
///
/// # Errors
/// `DbErr::Schema` for a column the table lacks or a delete without its
/// key.
#[allow(clippy::too_many_lines, clippy::implicit_hasher)] // three statement shapes, two dialects
pub fn statements(
    d: Dialect,
    ops: &[Op],
    tables: &HashMap<String, Arc<TableInfo>>,
    opts: &DbOpts,
) -> DbResult<Vec<Stmt>> {
    // Key changes first become a delete of the old key.
    let mut flat: Vec<Op> = Vec::with_capacity(ops.len());
    for op in ops {
        if let (OpKind::Upsert, Some(old)) = (op.kind, &op.old)
            && let Some(t) = tables.get(&op.table)
        {
            let a = pk_values(t, old);
            let b = pk_values(t, &op.cols);
            if a.is_some() && b.is_some() && a != b && opts.delete_mode != super::DeleteMode::Ignore
            {
                let mut del = op.clone();
                del.cols.clone_from(old);
                del.old = None;
                if opts.delete_mode == super::DeleteMode::Soft {
                    del.deleted = true;
                } else {
                    del.kind = OpKind::Delete;
                }
                flat.push(del);
            }
        }
        let mut o = op.clone();
        o.old = None;
        flat.push(o);
    }
    let mut out = Vec::new();
    let mut i = 0;
    while i < flat.len() {
        let first = &flat[i];
        let t = tables
            .get(&first.table)
            .ok_or_else(|| DbErr::Schema(format!("table {} unknown", first.table)))?;
        let names = |o: &Op| -> Vec<String> { o.cols.iter().map(|(c, _)| c.clone()).collect() };
        let shape = names(first);
        let mut j = i + 1;
        while j < flat.len()
            && flat[j].kind == first.kind
            && flat[j].table == first.table
            && (first.kind == OpKind::Delete || names(&flat[j]) == shape)
        {
            j += 1;
        }
        let mut group: Vec<&Op> = flat[i..j].iter().collect();
        if first.kind == OpKind::Upsert {
            // One row per key in a statement (PostgreSQL refuses a key
            // twice); the later record has the newer version anyway.
            let mut last: HashMap<String, usize> = HashMap::new();
            for (n, o) in group.iter().enumerate() {
                if let Some(k) = pk_values(t, &o.cols) {
                    last.insert(serde_json::to_string(&k).unwrap_or_default(), n);
                }
            }
            group = group
                .iter()
                .enumerate()
                .filter(|(n, o)| {
                    pk_values(t, &o.cols).is_none_or(|k| {
                        last.get(&serde_json::to_string(&k).unwrap_or_default()) == Some(n)
                    })
                })
                .map(|(_, o)| *o)
                .collect();
        }
        let width = shape.len() + 4;
        let per = opts.batch_rows.min((MAX_PARAMS / width).max(1));
        for chunk in group.chunks(per) {
            out.push(match first.kind {
                OpKind::Upsert => upsert_sql(d, t, &shape, chunk, opts)?,
                OpKind::Append => append_sql(d, t, &shape, chunk)?,
                OpKind::Delete => delete_sql(d, t, chunk)?,
            });
        }
        i = j;
    }
    Ok(out)
}

fn upsert_sql(
    d: Dialect,
    t: &TableInfo,
    shape: &[String],
    rows: &[&Op],
    opts: &DbOpts,
) -> DbResult<Stmt> {
    let soft = opts.delete_mode == super::DeleteMode::Soft;
    let mut cols: Vec<&Col> = shape
        .iter()
        .map(|c| lookup(t, c))
        .collect::<DbResult<_>>()?;
    cols.retain(|c| c.name != "_sprig_ver" && c.name != "_sprig_deleted");
    let del_col = if soft {
        Some(lookup(t, "_sprig_deleted")?)
    } else {
        None
    };
    let ver_col = lookup(t, "_sprig_ver")?;
    let mut names: Vec<String> = cols.iter().map(|c| ident(d, &c.name)).collect();
    if let Some(c) = del_col {
        names.push(ident(d, &c.name));
    }
    names.push(ident(d, &ver_col.name));
    let mut n = 0usize;
    let mut params = Vec::new();
    let mut values = Vec::with_capacity(rows.len());
    for o in rows {
        let mut v = Vec::with_capacity(names.len());
        for c in &cols {
            v.push(ph(d, &mut n, cast_of(d, c)));
            let val = o
                .cols
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(&c.name))
                .map_or(&Value::Null, |(_, x)| x);
            params.push(value_param(d, c, val));
        }
        if let Some(c) = del_col {
            v.push(ph(d, &mut n, cast_of(d, c)));
            params.push(value_param(d, c, &Value::Bool(o.deleted)));
        }
        v.push(ph(d, &mut n, None));
        params.push(Param::Bytes(o.ver.to_vec()));
        values.push(format!("({})", v.join(", ")));
    }
    let pk: BTreeSet<String> = t.pk.iter().map(|k| k.to_ascii_lowercase()).collect();
    let mut updated: Vec<&Col> = cols
        .iter()
        .copied()
        .filter(|c| !pk.contains(&c.name.to_ascii_lowercase()))
        .collect();
    if let Some(c) = del_col {
        updated.push(c);
    }
    let table = table_ident(d, &t.name);
    let ver = ident(d, &ver_col.name);
    let sql = match d {
        Dialect::Mysql => {
            // `_sprig_ver` is assigned last, so the other columns compare
            // against the old version.
            let newer = format!("({ver} IS NULL OR VALUES({ver}) > {ver})");
            let mut set: Vec<String> = updated
                .iter()
                .map(|c| {
                    let q = ident(d, &c.name);
                    format!("{q} = IF({newer}, VALUES({q}), {q})")
                })
                .collect();
            set.push(format!("{ver} = IF({newer}, VALUES({ver}), {ver})"));
            format!(
                "INSERT INTO {table} ({}) VALUES {} ON DUPLICATE KEY UPDATE {}",
                names.join(", "),
                values.join(", "),
                set.join(", ")
            )
        }
        Dialect::Postgres => {
            let mut set: Vec<String> = updated
                .iter()
                .map(|c| {
                    let q = ident(d, &c.name);
                    format!("{q} = EXCLUDED.{q}")
                })
                .collect();
            set.push(format!("{ver} = EXCLUDED.{ver}"));
            format!(
                "INSERT INTO {table} AS d ({}) VALUES {} ON CONFLICT ({}) DO UPDATE SET {} \
                 WHERE d.{ver} IS NULL OR d.{ver} < EXCLUDED.{ver}",
                names.join(", "),
                values.join(", "),
                t.pk.iter()
                    .map(|k| ident(d, k))
                    .collect::<Vec<_>>()
                    .join(", "),
                set.join(", ")
            )
        }
    };
    Ok(Stmt { sql, params })
}

fn append_sql(d: Dialect, t: &TableInfo, shape: &[String], rows: &[&Op]) -> DbResult<Stmt> {
    let mut cols: Vec<&Col> = shape
        .iter()
        .map(|c| lookup(t, c))
        .collect::<DbResult<_>>()?;
    cols.retain(|c| !matches!(c.name.as_str(), "_sprig_op" | "_sprig_ts" | "_sprig_idem"));
    let own = [
        lookup(t, "_sprig_op")?,
        lookup(t, "_sprig_ts")?,
        lookup(t, "_sprig_idem")?,
    ];
    let mut names: Vec<String> = cols.iter().map(|c| ident(d, &c.name)).collect();
    names.extend(own.iter().map(|c| ident(d, &c.name)));
    let mut n = 0usize;
    let mut params = Vec::new();
    let mut values = Vec::new();
    for o in rows {
        let mut v = Vec::new();
        for c in &cols {
            v.push(ph(d, &mut n, cast_of(d, c)));
            let val = o
                .cols
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(&c.name))
                .map_or(&Value::Null, |(_, x)| x);
            params.push(value_param(d, c, val));
        }
        v.push(ph(d, &mut n, cast_of(d, own[0])));
        params.push(Param::Text(o.op.to_owned()));
        v.push(ph(d, &mut n, cast_of(d, own[1])));
        params.push(Param::Text(o.ts.to_string()));
        v.push(ph(d, &mut n, cast_of(d, own[2])));
        params.push(Param::Text(o.idem.clone()));
        values.push(format!("({})", v.join(", ")));
    }
    let table = table_ident(d, &t.name);
    let idem = ident(d, &own[2].name);
    let sql = match d {
        Dialect::Mysql => format!(
            "INSERT INTO {table} ({}) VALUES {} ON DUPLICATE KEY UPDATE {idem} = {idem}",
            names.join(", "),
            values.join(", ")
        ),
        Dialect::Postgres => format!(
            "INSERT INTO {table} ({}) VALUES {} ON CONFLICT DO NOTHING",
            names.join(", "),
            values.join(", ")
        ),
    };
    Ok(Stmt { sql, params })
}

fn delete_sql(d: Dialect, t: &TableInfo, rows: &[&Op]) -> DbResult<Stmt> {
    let ver_col = lookup(t, "_sprig_ver")?;
    let ver = ident(d, &ver_col.name);
    let mut n = 0usize;
    let mut params = Vec::new();
    let mut conds = Vec::new();
    for o in rows {
        let Some(vals) = pk_values(t, &o.cols) else {
            return Err(DbErr::Schema(format!(
                "a delete for {} lacks a primary key column of {} ({})",
                o.idem,
                t.name,
                t.pk.join(", ")
            )));
        };
        let mut parts = Vec::new();
        for (k, v) in t.pk.iter().zip(vals) {
            let c = lookup(t, k)?;
            parts.push(format!(
                "{} = {}",
                ident(d, &c.name),
                ph(d, &mut n, cast_of(d, c))
            ));
            params.push(value_param(d, c, v));
        }
        parts.push(format!(
            "({ver} IS NULL OR {ver} <= {})",
            ph(d, &mut n, None)
        ));
        params.push(Param::Bytes(o.ver.to_vec()));
        conds.push(format!("({})", parts.join(" AND ")));
    }
    Ok(Stmt {
        sql: format!(
            "DELETE FROM {} WHERE {}",
            table_ident(d, &t.name),
            conds.join(" OR ")
        ),
        params,
    })
}

/// The offsets table.
#[must_use]
pub fn offsets_ddl(d: Dialect) -> String {
    match d {
        Dialect::Mysql => format!(
            "CREATE TABLE IF NOT EXISTS {} (sink_key VARCHAR(160) NOT NULL, \
             position VARBINARY(2048) NOT NULL, epoch BIGINT UNSIGNED NOT NULL, \
             updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), \
             PRIMARY KEY (sink_key)) ENGINE=InnoDB",
            super::OFFSETS
        ),
        Dialect::Postgres => format!(
            "CREATE TABLE IF NOT EXISTS {} (sink_key VARCHAR(160) NOT NULL PRIMARY KEY, \
             position BYTEA NOT NULL, epoch BIGINT NOT NULL, \
             updated_at TIMESTAMPTZ(3) NOT NULL DEFAULT now())",
            super::OFFSETS
        ),
    }
}

/// The seen-keys table.
#[must_use]
pub fn seen_ddl(d: Dialect) -> String {
    match d {
        Dialect::Mysql => format!(
            "CREATE TABLE IF NOT EXISTS {} (sink_key VARCHAR(160) NOT NULL, \
             idem_key VARCHAR(200) NOT NULL, seen_at TIMESTAMP(3) NOT NULL, \
             PRIMARY KEY (sink_key, idem_key)) ENGINE=InnoDB",
            super::SEEN
        ),
        Dialect::Postgres => format!(
            "CREATE TABLE IF NOT EXISTS {} (sink_key VARCHAR(160) NOT NULL, \
             idem_key VARCHAR(200) NOT NULL, seen_at TIMESTAMPTZ(3) NOT NULL, \
             PRIMARY KEY (sink_key, idem_key))",
            super::SEEN
        ),
    }
}

fn now(d: Dialect) -> &'static str {
    match d {
        Dialect::Mysql => "CURRENT_TIMESTAMP(3)",
        Dialect::Postgres => "now()",
    }
}

fn list(d: Dialect, n: &mut usize, k: usize) -> String {
    (0..k)
        .map(|_| ph(d, n, None))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Locks the offset rows of `keys`.
#[must_use]
pub fn offsets_lock_sql(d: Dialect, keys: &[String]) -> (String, Vec<Param>) {
    let mut n = 0;
    (
        format!(
            "SELECT sink_key, position, epoch FROM {} WHERE sink_key IN ({}) ORDER BY sink_key FOR UPDATE",
            super::OFFSETS,
            list(d, &mut n, keys.len())
        ),
        keys.iter().map(|k| Param::Text(k.clone())).collect(),
    )
}

/// Reads a task's offset rows.
#[must_use]
pub fn offsets_prefix_sql(d: Dialect, prefix: &str) -> (String, Vec<Param>) {
    let mut n = 0;
    (
        format!(
            "SELECT sink_key, position, epoch FROM {} WHERE sink_key LIKE {} ORDER BY sink_key",
            super::OFFSETS,
            ph(d, &mut n, None)
        ),
        vec![Param::Text(format!("{prefix}%"))],
    )
}

/// Raises a task's rows to its epoch, fencing older tasks.
#[must_use]
pub fn offsets_fence_sql(d: Dialect, prefix: &str, epoch: i64) -> (String, Vec<Param>) {
    let mut n = 0;
    let a = ph(d, &mut n, None);
    let b = ph(d, &mut n, None);
    let c = ph(d, &mut n, None);
    (
        format!(
            "UPDATE {} SET epoch = {a} WHERE sink_key LIKE {b} AND epoch < {c}",
            super::OFFSETS
        ),
        vec![
            Param::Int(epoch),
            Param::Text(format!("{prefix}%")),
            Param::Int(epoch),
        ],
    )
}

/// Moves a unit's position, unless a newer epoch holds it.
#[must_use]
pub fn offsets_update_sql(
    d: Dialect,
    key: &str,
    pos: &Position,
    epoch: i64,
) -> (String, Vec<Param>) {
    let mut n = 0;
    let a = ph(d, &mut n, None);
    let b = ph(d, &mut n, None);
    let c = ph(d, &mut n, None);
    let e = ph(d, &mut n, None);
    (
        format!(
            "UPDATE {} SET position = {a}, epoch = {b}, updated_at = {} WHERE sink_key = {c} AND epoch <= {e}",
            super::OFFSETS,
            now(d)
        ),
        vec![
            Param::Bytes(pos.encode()),
            Param::Int(epoch),
            Param::Text(key.to_owned()),
            Param::Int(epoch),
        ],
    )
}

/// A unit's first position.
#[must_use]
pub fn offsets_insert_sql(
    d: Dialect,
    key: &str,
    pos: &Position,
    epoch: i64,
) -> (String, Vec<Param>) {
    let mut n = 0;
    let vals = list(d, &mut n, 3);
    (
        format!(
            "INSERT INTO {} (sink_key, position, epoch, updated_at) VALUES ({vals}, {})",
            super::OFFSETS,
            now(d)
        ),
        vec![
            Param::Text(key.to_owned()),
            Param::Bytes(pos.encode()),
            Param::Int(epoch),
        ],
    )
}

/// Locks the seen rows of `keys`.
#[must_use]
pub fn seen_lock_sql(d: Dialect, sink: &str, keys: &[String]) -> (String, Vec<Param>) {
    let mut n = 0;
    let s = ph(d, &mut n, None);
    let l = list(d, &mut n, keys.len());
    let mut params = vec![Param::Text(sink.to_owned())];
    params.extend(keys.iter().map(|k| Param::Text(k.clone())));
    (
        format!(
            "SELECT idem_key FROM {} WHERE sink_key = {s} AND idem_key IN ({l}) FOR UPDATE",
            super::SEEN
        ),
        params,
    )
}

/// Records `keys` as seen.
#[must_use]
pub fn seen_insert_sql(d: Dialect, sink: &str, keys: &[String]) -> (String, Vec<Param>) {
    let mut n = 0;
    let mut rows = Vec::new();
    let mut params = Vec::new();
    for k in keys {
        let a = ph(d, &mut n, None);
        let b = ph(d, &mut n, None);
        rows.push(format!("({a}, {b}, {})", now(d)));
        params.push(Param::Text(sink.to_owned()));
        params.push(Param::Text(k.clone()));
    }
    (
        format!(
            "INSERT INTO {} (sink_key, idem_key, seen_at) VALUES {}",
            super::SEEN,
            rows.join(", ")
        ),
        params,
    )
}

/// Deletes seen keys older than the retention (`sink` is digits and `/`).
#[must_use]
pub fn purge_seen_sql(d: Dialect, sink: &str, keep: Duration) -> String {
    let sink: String = sink
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '/')
        .collect();
    let secs = keep.as_secs();
    match d {
        Dialect::Mysql => format!(
            "DELETE FROM {} WHERE sink_key = '{sink}' AND seen_at < CURRENT_TIMESTAMP(3) - INTERVAL {secs} SECOND",
            super::SEEN
        ),
        Dialect::Postgres => format!(
            "DELETE FROM {} WHERE sink_key = '{sink}' AND seen_at < now() - interval '{secs} seconds'",
            super::SEEN
        ),
    }
}
