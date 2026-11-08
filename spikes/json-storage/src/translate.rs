//! Chapter 31 filter / sort / update translation, cut down to what the spike needs:
//! scalar-safe paths only (no path registry), field columns where they exist.

use crate::json::{iso_date, Renderer};
use crate::keyenc::{class_of, encode_key};
use anyhow::{bail, Result};
use bson::raw::{RawBsonRef, RawDocument};
use mysql_async::Value;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Class {
    String,
    Integer,
    Number,
    Date,
    ObjectId,
    Bool,
}

impl Class {
    pub fn name(self) -> &'static str {
        match self {
            Class::String => "string",
            Class::Integer => "integer",
            Class::Number => "number",
            Class::Date => "date",
            Class::ObjectId => "objectid",
            Class::Bool => "bool",
        }
    }
    pub fn parse(s: &str) -> Option<Class> {
        Some(match s {
            "string" => Class::String,
            "integer" => Class::Integer,
            "number" => Class::Number,
            "date" => Class::Date,
            "objectid" => Class::ObjectId,
            "bool" => Class::Bool,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug)]
pub struct FieldCol {
    pub path: String,
    pub class: Class,
    pub col: String,
}

pub fn check_path(p: &str) -> Result<()> {
    if p.is_empty() || p.len() > 200 || !p.split('.').all(|s| !s.is_empty() && s.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')) {
        bail!("spike: unsupported field path {p:?}");
    }
    Ok(())
}

pub fn jpath(p: &str) -> String {
    format!("$.{p}")
}
pub fn tpath(p: &str) -> String {
    format!("$.\"{p}\"")
}

/// Generated-column expression and SQL type for a field column (chapter 31 "Field columns").
pub fn column_ddl(fc: &FieldCol) -> String {
    let (j, t) = (jpath(&fc.path), tpath(&fc.path));
    let (ty, expr) = match fc.class {
        Class::String => ("VARCHAR(255) COLLATE utf8mb4_bin", format!(
            "CASE WHEN JSON_TYPE(JSON_EXTRACT(doc,'{j}')) = 'STRING' AND JSON_VALUE(_types,'{t}') IS NULL THEN LEFT(JSON_VALUE(doc,'{j}'),255) END")),
        Class::Integer => ("BIGINT", format!(
            "CASE WHEN JSON_TYPE(JSON_EXTRACT(doc,'{j}')) = 'INTEGER' THEN CAST(JSON_VALUE(doc,'{j}') AS SIGNED) END")),
        Class::Number => ("DOUBLE", format!(
            "CASE WHEN JSON_TYPE(JSON_EXTRACT(doc,'{j}')) IN ('INTEGER','DOUBLE') THEN CAST(JSON_VALUE(doc,'{j}') AS DOUBLE) END")),
        // Chapter 31 says DATETIME(3) via CAST(... AS DATETIME(3)); MariaDB 11.8 refuses to
        // index any string->temporal conversion in a virtual column (ERROR 1901), so the
        // column holds the fixed-width ISO text, which sorts in time order.
        Class::Date => ("CHAR(24) CHARACTER SET ascii COLLATE ascii_bin", format!(
            "CASE WHEN JSON_VALUE(_types,'{t}') = 'date' AND JSON_TYPE(JSON_EXTRACT(doc,'{j}')) = 'STRING' THEN JSON_VALUE(doc,'{j}') END")),
        Class::ObjectId => ("BINARY(12)", format!(
            "CASE WHEN JSON_VALUE(_types,'{t}') = 'objectId' THEN UNHEX(JSON_VALUE(doc,'{j}')) END")),
        Class::Bool => ("TINYINT(1)", format!(
            "CASE WHEN JSON_TYPE(JSON_EXTRACT(doc,'{j}')) = 'BOOLEAN' THEN JSON_EXTRACT(doc,'{j}') = 'true' END")),
    };
    format!("`{}` {ty} AS ({expr}) VIRTUAL INVISIBLE COMMENT 'sprig:{}:{}'", fc.col, fc.class.name(), fc.path)
}

pub fn lit_class(v: RawBsonRef<'_>) -> Option<Class> {
    Some(match v {
        RawBsonRef::String(_) => Class::String,
        RawBsonRef::Int32(_) | RawBsonRef::Int64(_) => Class::Integer,
        RawBsonRef::Double(_) => Class::Number,
        RawBsonRef::DateTime(_) => Class::Date,
        RawBsonRef::ObjectId(_) => Class::ObjectId,
        RawBsonRef::Boolean(_) => Class::Bool,
        _ => return None,
    })
}

fn lit_param(c: Class, v: RawBsonRef<'_>) -> Result<Value> {
    Ok(match (c, v) {
        (Class::String, RawBsonRef::String(s)) => Value::Bytes(s.as_bytes().to_vec()),
        (_, RawBsonRef::Int32(i)) => Value::Int(i as i64),
        (_, RawBsonRef::Int64(i)) => Value::Int(i),
        (_, RawBsonRef::Double(f)) => Value::Double(f),
        (_, RawBsonRef::Boolean(b)) => Value::Int(b as i64),
        (_, RawBsonRef::ObjectId(o)) => Value::Bytes(o.bytes().to_vec()),
        (_, RawBsonRef::DateTime(d)) => match iso_date(d.timestamp_millis()) {
            Some((iso, _)) => Value::Bytes(iso.into_bytes()),
            None => bail!("spike: date out of range"),
        },
        _ => bail!("spike: literal {:?} for {:?}", v.element_type(), c),
    })
}

fn sql_op(op: &str) -> Result<&'static str> {
    Ok(match op {
        "$eq" => "=",
        "$gt" => ">",
        "$gte" => ">=",
        "$lt" => "<",
        "$lte" => "<=",
        _ => bail!("spike: unsupported operator {op}"),
    })
}

pub struct Where {
    pub sql: Vec<String>,
    pub params: Vec<Value>,
}

impl Where {
    pub fn render(&self) -> String {
        if self.sql.is_empty() {
            "TRUE".into()
        } else {
            self.sql.join(" AND ")
        }
    }
}

pub fn filter(f: Option<&RawDocument>, fields: &[FieldCol]) -> Result<Where> {
    let mut w = Where { sql: vec![], params: vec![] };
    if let Some(f) = f {
        filter_doc(f, fields, &mut w)?;
    }
    Ok(w)
}

fn filter_doc(d: &RawDocument, fields: &[FieldCol], w: &mut Where) -> Result<()> {
    for e in d {
        let (k, v) = e?;
        if k == "$and" {
            let RawBsonRef::Array(a) = v else { bail!("$and needs an array") };
            for x in a {
                let RawBsonRef::Document(x) = x? else { bail!("$and element") };
                filter_doc(x, fields, w)?;
            }
            continue;
        }
        if k.starts_with('$') {
            bail!("spike: unsupported top-level operator {k}");
        }
        match v {
            RawBsonRef::Document(ops) if ops.iter().next().transpose()?.is_some_and(|(o, _)| o.starts_with('$')) => {
                for o in ops {
                    let (op, lit) = o?;
                    pred(k, op, lit, fields, w)?;
                }
            }
            _ => pred(k, "$eq", v, fields, w)?,
        }
    }
    Ok(())
}

fn pred(path: &str, op: &str, lit: RawBsonRef<'_>, fields: &[FieldCol], w: &mut Where) -> Result<()> {
    if path == "_id" {
        if op == "$in" {
            let RawBsonRef::Array(a) = lit else { bail!("$in needs an array") };
            let mut qs = vec![];
            for x in a {
                qs.push("?");
                w.params.push(Value::Bytes(encode_key(x?)));
            }
            w.sql.push(format!("_id IN ({})", qs.join(",")));
            return Ok(());
        }
        let o = sql_op(op)?;
        w.sql.push(format!("_id {o} ?"));
        w.params.push(Value::Bytes(encode_key(lit)));
        if o != "=" {
            // type bracketing: stay inside the literal's class
            let c = class_of(lit);
            w.sql.push("_id >= ? AND _id < ?".into());
            w.params.push(Value::Bytes(vec![c]));
            w.params.push(Value::Bytes(vec![c + 1]));
        }
        return Ok(());
    }
    check_path(path)?;
    if op == "$in" {
        let RawBsonRef::Array(a) = lit else { bail!("$in needs an array") };
        let vals: Vec<RawBsonRef> = a.into_iter().collect::<Result<_, _>>()?;
        let c = vals.first().and_then(|v| lit_class(*v));
        if let Some(c) = c {
            if vals.iter().all(|v| lit_class(*v) == Some(c)) {
                if let Some(fc) = find_col(fields, path, c) {
                    let qs: Vec<&str> = vals.iter().map(|_| "?").collect();
                    w.sql.push(format!("`{}` IN ({})", fc.col, qs.join(",")));
                    for v in vals {
                        w.params.push(lit_param(fc.class, v)?);
                    }
                    return Ok(());
                }
            }
        }
        bail!("spike: $in without a field column on {path}");
    }
    let o = sql_op(op)?;
    let c = lit_class(lit);
    if let Some(fc) = c.and_then(|c| find_col(fields, path, c)) {
        w.sql.push(format!("`{}` {o} ?", fc.col));
        w.params.push(lit_param(fc.class, lit)?);
        return Ok(());
    }
    let (j, t) = (jpath(path), tpath(path));
    match lit {
        RawBsonRef::String(s) => {
            w.sql.push(format!("(JSON_VALUE(doc,'{j}') {o} ? AND JSON_TYPE(JSON_EXTRACT(doc,'{j}')) = 'STRING' AND JSON_VALUE(_types,'{t}') IS NULL)"));
            w.params.push(Value::Bytes(s.as_bytes().to_vec()));
        }
        RawBsonRef::Int32(_) | RawBsonRef::Int64(_) => {
            w.sql.push(format!("(JSON_TYPE(JSON_EXTRACT(doc,'{j}')) = 'INTEGER' AND CAST(JSON_VALUE(doc,'{j}') AS SIGNED) {o} ?)"));
            w.params.push(lit_param(Class::Integer, lit)?);
        }
        RawBsonRef::ObjectId(oid) if o == "=" => {
            w.sql.push(format!("(JSON_VALUE(doc,'{j}') = ? AND JSON_VALUE(_types,'{t}') = 'objectId')"));
            w.params.push(Value::Bytes(oid.to_hex().into_bytes()));
        }
        _ => bail!("spike: unsupported literal for {path} without a field column"),
    }
    Ok(())
}

fn find_col<'a>(fields: &'a [FieldCol], path: &str, c: Class) -> Option<&'a FieldCol> {
    fields.iter().find(|f| f.path == path && f.class == c).or_else(|| {
        if c == Class::Integer {
            fields.iter().find(|f| f.path == path && f.class == Class::Number)
        } else {
            None
        }
    })
}

pub fn sort(s: Option<&RawDocument>, fields: &[FieldCol]) -> Result<String> {
    let Some(s) = s else { return Ok(String::new()) };
    let mut parts = vec![];
    let mut has_id = false;
    for e in s {
        let (k, v) = e?;
        let dir = match v {
            RawBsonRef::Int32(i) => i as i64,
            RawBsonRef::Int64(i) => i,
            RawBsonRef::Double(f) => f as i64,
            _ => bail!("spike: sort direction"),
        };
        let d = if dir < 0 { " DESC" } else { "" };
        if k == "_id" {
            has_id = true;
            parts.push(format!("_id{d}"));
        } else {
            let fc = fields.iter().find(|f| f.path == k).ok_or_else(|| anyhow::anyhow!("spike: sort on {k} needs a field column"))?;
            parts.push(format!("`{}`{d}", fc.col));
        }
    }
    if parts.is_empty() {
        return Ok(String::new());
    }
    if !has_id {
        parts.push("_id".into());
    }
    Ok(format!(" ORDER BY {}", parts.join(", ")))
}

/// A pushed-down update (chapter 31 "Writes and updates").
pub struct Update {
    pub set_sql: String, // "doc = ..., _types = ..."
    pub params: Vec<Value>,
    pub guards: Vec<String>,
}

pub fn update(u: &RawDocument) -> Result<Update> {
    let mut doc_args: Vec<String> = vec![];
    let mut doc_params: Vec<Value> = vec![];
    let mut unset: Vec<String> = vec![];
    let mut ty_remove: Vec<String> = vec![];
    let mut ty_set: Vec<String> = vec![];
    let mut ty_params: Vec<Value> = vec![];
    let mut guards: Vec<String> = vec![];
    let no_entries_under = |p: &str| {
        let pat = p.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_");
        format!("(_types IS NULL OR JSON_SEARCH(JSON_KEYS(_types),'one','{pat}.%') IS NULL)")
    };
    for e in u {
        let (op, spec) = e?;
        let RawBsonRef::Document(spec) = spec else { bail!("{op} needs a document") };
        for f in spec {
            let (p, v) = f?;
            if p == "_id" {
                bail!("spike: $-update of _id");
            }
            check_path(p)?;
            let j = jpath(p);
            if let Some((parent, _)) = p.rsplit_once('.') {
                guards.push(format!("JSON_TYPE(JSON_EXTRACT(doc,'{}')) = 'OBJECT'", jpath(parent)));
            }
            match op {
                "$set" => {
                    let (frag, entries) = Renderer::fragment(v, p)?;
                    if frag.starts_with('"') {
                        let s: String = serde_json::from_str(&frag)?;
                        doc_args.push(format!("'{j}', ?"));
                        doc_params.push(Value::Bytes(s.into_bytes()));
                    } else {
                        doc_args.push(format!("'{j}', JSON_EXTRACT(?, '$')"));
                        doc_params.push(Value::Bytes(frag.into_bytes()));
                    }
                    if entries.is_empty() {
                        ty_remove.push(format!("'{}'", tpath(p)));
                    }
                    for (ep, et) in entries {
                        ty_set.push(format!("'{}', ?", tpath(&ep)));
                        ty_params.push(Value::Bytes(et.into_bytes()));
                    }
                    guards.push(no_entries_under(p));
                }
                "$inc" => {
                    let (n, long) = match v {
                        RawBsonRef::Int32(i) => (i as i64, false),
                        RawBsonRef::Int64(i) => (i, true),
                        _ => bail!("spike: $inc by a non-integer"),
                    };
                    doc_args.push(format!("'{j}', IFNULL(CAST(JSON_VALUE(doc,'{j}') AS SIGNED), 0) + ?"));
                    doc_params.push(Value::Int(n));
                    guards.push(format!(
                        "(JSON_EXTRACT(doc,'{j}') IS NULL OR JSON_TYPE(JSON_EXTRACT(doc,'{j}')) = 'INTEGER') AND COALESCE(JSON_VALUE(_types,'{}'),'long') = 'long'",
                        tpath(p)
                    ));
                    if long {
                        ty_set.push(format!("'{}', 'long'", tpath(p)));
                    }
                }
                "$unset" => {
                    unset.push(format!("'{j}'"));
                    ty_remove.push(format!("'{}'", tpath(p)));
                    guards.push(no_entries_under(p));
                }
                _ => bail!("spike: update operator {op} needs read-modify-write"),
            }
        }
    }
    let mut doc_expr = "doc".to_string();
    if !doc_args.is_empty() {
        doc_expr = format!("JSON_SET({doc_expr}, {})", doc_args.join(", "));
    }
    if !unset.is_empty() {
        doc_expr = format!("JSON_REMOVE({doc_expr}, {})", unset.join(", "));
    }
    let mut ty_expr = "_types".to_string();
    if !ty_remove.is_empty() {
        // MariaDB 11.8: JSON_REMOVE of an absent path returns the document unchanged.
        ty_expr = format!("JSON_REMOVE({ty_expr}, {})", ty_remove.join(", "));
    }
    if !ty_set.is_empty() {
        ty_expr = format!("JSON_SET(COALESCE({ty_expr}, '{{}}'), {})", ty_set.join(", "));
    }
    let mut set_sql = format!("doc = {doc_expr}");
    let mut params = doc_params;
    if ty_expr != "_types" {
        set_sql.push_str(&format!(", _types = {ty_expr}"));
        params.extend(ty_params);
    }
    Ok(Update { set_sql, params, guards })
}
