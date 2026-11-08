//! SQL results over HTTP (chapter 37, `Sql`): their proto form and its
//! JSON.
//!
//! A cell is the MySQL text-protocol form of the value, exactly what a
//! MySQL client reads: `"42"`, `"2026-11-08 10:00:00"`, `"1.50"`, or
//! `null`. Columns whose bytes aren't text (collation 63 and not a number
//! or temporal type) set `binary`, and their cells are base64. Rows are
//! `google.protobuf.ListValue`s, so their proto3 JSON is a plain array.
//!
//! JSON follows proto3's mapping: field names as in the protos, `uint64`
//! as decimal strings, default values left out except where a reader needs
//! them (`affected_rows`, `rows`).

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use prost_types::value::Kind;
use prost_types::{ListValue, Value as PValue};
use serde_json::{Map, Value, json};
use sprig_common::{SprigCode, SprigError};

use crate::proto::v1::{
    Column, ExecuteResponse, QueryResponse, Result as SqlResult, ResultEnd, ResultHeader, RowBatch,
    Transaction, query_response,
};

/// The binary collation.
pub const BINARY_CHARSET: u32 = 63;

/// The MySQL type name for a column type code.
#[must_use]
pub fn type_name(mysql_type: u32) -> &'static str {
    match mysql_type {
        0 | 246 => "DECIMAL",
        1 => "TINYINT",
        2 => "SMALLINT",
        3 => "INT",
        4 => "FLOAT",
        5 => "DOUBLE",
        6 => "NULL",
        7 => "TIMESTAMP",
        8 => "BIGINT",
        9 => "MEDIUMINT",
        10 => "DATE",
        11 => "TIME",
        12 => "DATETIME",
        13 => "YEAR",
        15 | 253 => "VARCHAR",
        16 => "BIT",
        245 => "JSON",
        247 => "ENUM",
        248 => "SET",
        249 => "TINYBLOB",
        250 => "MEDIUMBLOB",
        251 => "LONGBLOB",
        252 => "BLOB",
        254 => "CHAR",
        255 => "GEOMETRY",
        _ => "UNKNOWN",
    }
}

/// Whether a column's cells are bytes, not text: binary collation on a
/// string, blob, bit or geometry type.
#[must_use]
pub fn is_binary(mysql_type: u32, charset: u32) -> bool {
    match mysql_type {
        16 | 255 => true,
        15 | 249..=254 => charset == BINARY_CHARSET,
        _ => false,
    }
}

/// One cell: `None` is SQL NULL.
#[must_use]
pub fn cell(bytes: Option<&[u8]>, binary: bool) -> PValue {
    let kind = match bytes {
        None => Kind::NullValue(0),
        Some(b) if binary => Kind::StringValue(STANDARD.encode(b)),
        Some(b) => Kind::StringValue(String::from_utf8_lossy(b).into_owned()),
    };
    PValue { kind: Some(kind) }
}

/// A cell's bytes back (base64-decoded for binary columns).
#[must_use]
pub fn cell_bytes(v: &PValue, binary: bool) -> Option<Vec<u8>> {
    match &v.kind {
        Some(Kind::StringValue(s)) if binary => STANDARD.decode(s).ok(),
        Some(Kind::StringValue(s)) => Some(s.clone().into_bytes()),
        Some(Kind::NumberValue(n)) => Some(n.to_string().into_bytes()),
        Some(Kind::BoolValue(b)) => Some(if *b { b"1".to_vec() } else { b"0".to_vec() }),
        _ => None,
    }
}

fn value_json(v: &PValue) -> Value {
    match &v.kind {
        Some(Kind::StringValue(s)) => Value::String(s.clone()),
        Some(Kind::NumberValue(n)) => json!(n),
        Some(Kind::BoolValue(b)) => Value::Bool(*b),
        _ => Value::Null,
    }
}

fn value_from_json(v: &Value) -> PValue {
    let kind = match v {
        Value::String(s) => Kind::StringValue(s.clone()),
        Value::Bool(b) => Kind::BoolValue(*b),
        Value::Number(n) => Kind::NumberValue(n.as_f64().unwrap_or(0.0)),
        _ => Kind::NullValue(0),
    };
    PValue { kind: Some(kind) }
}

/// A row as a JSON array.
#[must_use]
pub fn row_json(r: &ListValue) -> Value {
    Value::Array(r.values.iter().map(value_json).collect())
}

fn row_from_json(v: &Value) -> ListValue {
    ListValue {
        values: v
            .as_array()
            .map(|a| a.iter().map(value_from_json).collect())
            .unwrap_or_default(),
    }
}

fn put_str(m: &mut Map<String, Value>, k: &str, v: &str) {
    if !v.is_empty() {
        m.insert(k.to_owned(), Value::String(v.to_owned()));
    }
}

fn put_u32(m: &mut Map<String, Value>, k: &str, v: u32) {
    if v != 0 {
        m.insert(k.to_owned(), json!(v));
    }
}

fn s(v: &Value, k: &str) -> String {
    v.get(k).and_then(Value::as_str).unwrap_or("").to_owned()
}

fn u32_of(v: &Value, k: &str) -> u32 {
    v.get(k)
        .and_then(Value::as_u64)
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(0)
}

/// proto3 JSON reads a `uint64` from a string or a number.
fn u64_of(v: &Value, k: &str) -> u64 {
    match v.get(k) {
        Some(Value::String(s)) => s.parse().unwrap_or(0),
        Some(Value::Number(n)) => n.as_u64().unwrap_or(0),
        _ => 0,
    }
}

fn i64_of(v: &Value, k: &str) -> i64 {
    match v.get(k) {
        Some(Value::String(s)) => s.parse().unwrap_or(0),
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
        _ => 0,
    }
}

/// A column as JSON.
#[must_use]
pub fn column_json(c: &Column) -> Value {
    let mut m = Map::new();
    m.insert("name".into(), Value::String(c.name.clone()));
    put_str(&mut m, "org_name", &c.org_name);
    put_str(&mut m, "table", &c.table);
    put_str(&mut m, "org_table", &c.org_table);
    put_str(&mut m, "database", &c.database);
    m.insert("type".into(), Value::String(c.r#type.clone()));
    m.insert("mysql_type".into(), json!(c.mysql_type));
    put_u32(&mut m, "flags", c.flags);
    put_u32(&mut m, "charset", c.charset);
    put_u32(&mut m, "decimals", c.decimals);
    if c.length != 0 {
        m.insert("length".into(), Value::String(c.length.to_string()));
    }
    if c.binary {
        m.insert("binary".into(), Value::Bool(true));
    }
    Value::Object(m)
}

/// A column from JSON.
#[must_use]
pub fn column_from_json(v: &Value) -> Column {
    Column {
        name: s(v, "name"),
        org_name: s(v, "org_name"),
        table: s(v, "table"),
        org_table: s(v, "org_table"),
        database: s(v, "database"),
        r#type: s(v, "type"),
        mysql_type: u32_of(v, "mysql_type"),
        flags: u32_of(v, "flags"),
        charset: u32_of(v, "charset"),
        decimals: u32_of(v, "decimals"),
        length: u64_of(v, "length"),
        binary: v.get("binary").and_then(Value::as_bool).unwrap_or(false),
    }
}

/// One result as JSON.
#[must_use]
pub fn result_json(r: &SqlResult) -> Value {
    let mut m = Map::new();
    if !r.columns.is_empty() {
        m.insert(
            "columns".into(),
            Value::Array(r.columns.iter().map(column_json).collect()),
        );
        m.insert(
            "rows".into(),
            Value::Array(r.rows.iter().map(row_json).collect()),
        );
    }
    m.insert(
        "affected_rows".into(),
        Value::String(r.affected_rows.to_string()),
    );
    if r.last_insert_id != 0 {
        m.insert(
            "last_insert_id".into(),
            Value::String(r.last_insert_id.to_string()),
        );
    }
    put_u32(&mut m, "warnings", r.warnings);
    put_str(&mut m, "info", &r.info);
    Value::Object(m)
}

/// One result from JSON.
#[must_use]
pub fn result_from_json(v: &Value) -> SqlResult {
    SqlResult {
        columns: v
            .get("columns")
            .and_then(Value::as_array)
            .map(|a| a.iter().map(column_from_json).collect())
            .unwrap_or_default(),
        rows: v
            .get("rows")
            .and_then(Value::as_array)
            .map(|a| a.iter().map(row_from_json).collect())
            .unwrap_or_default(),
        affected_rows: u64_of(v, "affected_rows"),
        last_insert_id: u64_of(v, "last_insert_id"),
        warnings: u32_of(v, "warnings"),
        info: s(v, "info"),
    }
}

/// `Sql.Execute`'s reply as JSON.
#[must_use]
pub fn execute_json(r: &ExecuteResponse) -> Value {
    json!({ "results": r.results.iter().map(result_json).collect::<Vec<_>>() })
}

/// `Sql.Execute`'s reply from JSON.
///
/// # Errors
/// 39792 if `v` has no `results` array.
pub fn execute_from_json(v: &Value) -> Result<ExecuteResponse, SprigError> {
    let results = v.get("results").and_then(Value::as_array).ok_or_else(|| {
        SprigError::new(SprigCode::AdminRequestInvalid, "reply without \"results\"")
    })?;
    Ok(ExecuteResponse {
        results: results.iter().map(result_from_json).collect(),
    })
}

/// One `Sql.Query` message as JSON (one NDJSON line).
#[must_use]
pub fn query_part_json(r: &QueryResponse) -> Value {
    match &r.part {
        Some(query_response::Part::Header(h)) => json!({"header": {
            "index": h.index,
            "columns": h.columns.iter().map(column_json).collect::<Vec<_>>(),
        }}),
        Some(query_response::Part::Rows(b)) => json!({"rows": {
            "rows": b.rows.iter().map(row_json).collect::<Vec<_>>(),
        }}),
        Some(query_response::Part::End(e)) => {
            let mut m = Map::new();
            m.insert("index".into(), json!(e.index));
            m.insert(
                "affected_rows".into(),
                Value::String(e.affected_rows.to_string()),
            );
            if e.last_insert_id != 0 {
                m.insert(
                    "last_insert_id".into(),
                    Value::String(e.last_insert_id.to_string()),
                );
            }
            put_u32(&mut m, "warnings", e.warnings);
            put_str(&mut m, "info", &e.info);
            if e.more_results {
                m.insert("more_results".into(), Value::Bool(true));
            }
            json!({ "end": m })
        }
        None => json!({}),
    }
}

/// One `Sql.Query` message from JSON.
#[must_use]
pub fn query_part_from_json(v: &Value) -> QueryResponse {
    let part = if let Some(h) = v.get("header") {
        Some(query_response::Part::Header(ResultHeader {
            index: u32_of(h, "index"),
            columns: h
                .get("columns")
                .and_then(Value::as_array)
                .map(|a| a.iter().map(column_from_json).collect())
                .unwrap_or_default(),
        }))
    } else if let Some(b) = v.get("rows") {
        Some(query_response::Part::Rows(RowBatch {
            rows: b
                .get("rows")
                .and_then(Value::as_array)
                .map(|a| a.iter().map(row_from_json).collect())
                .unwrap_or_default(),
        }))
    } else {
        v.get("end").map(|e| {
            query_response::Part::End(ResultEnd {
                index: u32_of(e, "index"),
                affected_rows: u64_of(e, "affected_rows"),
                last_insert_id: u64_of(e, "last_insert_id"),
                warnings: u32_of(e, "warnings"),
                info: s(e, "info"),
                more_results: e
                    .get("more_results")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            })
        })
    };
    QueryResponse { part }
}

/// Folds a `Sql.Query` stream into `Sql.Execute`'s reply.
#[must_use]
pub fn collect(parts: impl IntoIterator<Item = QueryResponse>) -> ExecuteResponse {
    let mut out = ExecuteResponse::default();
    let mut current: Option<SqlResult> = None;
    for p in parts {
        match p.part {
            Some(query_response::Part::Header(h)) => {
                current = Some(SqlResult {
                    columns: h.columns,
                    ..SqlResult::default()
                });
            }
            Some(query_response::Part::Rows(b)) => {
                current
                    .get_or_insert_with(SqlResult::default)
                    .rows
                    .extend(b.rows);
            }
            Some(query_response::Part::End(e)) => {
                let mut r = current.take().unwrap_or_default();
                r.affected_rows = e.affected_rows;
                r.last_insert_id = e.last_insert_id;
                r.warnings = e.warnings;
                r.info = e.info;
                out.results.push(r);
            }
            None => {}
        }
    }
    out
}

/// A transaction handle reply as JSON.
#[must_use]
pub fn transaction_json(t: &Transaction) -> Value {
    json!({
        "name": t.name,
        "idle_deadline_ms": t.idle_deadline_ms.to_string(),
        "deadline_ms": t.deadline_ms.to_string(),
    })
}

/// A transaction handle reply from JSON.
#[must_use]
pub fn transaction_from_json(v: &Value) -> Transaction {
    Transaction {
        name: s(v, "name"),
        idle_deadline_ms: i64_of(v, "idle_deadline_ms"),
        deadline_ms: i64_of(v, "deadline_ms"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> SqlResult {
        SqlResult {
            columns: vec![
                Column {
                    name: "id".into(),
                    r#type: "BIGINT".into(),
                    mysql_type: 8,
                    flags: 1,
                    charset: 63,
                    length: 20,
                    ..Column::default()
                },
                Column {
                    name: "blob".into(),
                    r#type: "BLOB".into(),
                    mysql_type: 252,
                    charset: 63,
                    binary: true,
                    ..Column::default()
                },
            ],
            rows: vec![ListValue {
                values: vec![cell(Some(b"1"), false), cell(Some(&[0, 255, 7]), true)],
            }],
            affected_rows: 0,
            last_insert_id: 0,
            warnings: 1,
            info: String::new(),
        }
    }

    #[test]
    fn results_round_trip_through_json() {
        let r = ExecuteResponse {
            results: vec![
                sample(),
                SqlResult {
                    affected_rows: u64::MAX,
                    last_insert_id: 9,
                    ..SqlResult::default()
                },
            ],
        };
        let j = execute_json(&r);
        assert_eq!(j["results"][1]["affected_rows"], "18446744073709551615");
        assert_eq!(j["results"][0]["rows"][0][0], "1");
        let back = execute_from_json(&j).unwrap();
        assert_eq!(back, r);
        assert_eq!(
            cell_bytes(&back.results[0].rows[0].values[1], true).unwrap(),
            vec![0, 255, 7]
        );
    }

    #[test]
    fn streams_fold_into_results() {
        let r = sample();
        let parts = vec![
            QueryResponse {
                part: Some(query_response::Part::Header(ResultHeader {
                    index: 0,
                    columns: r.columns.clone(),
                })),
            },
            QueryResponse {
                part: Some(query_response::Part::Rows(RowBatch {
                    rows: r.rows.clone(),
                })),
            },
            QueryResponse {
                part: Some(query_response::Part::End(ResultEnd {
                    index: 0,
                    warnings: 1,
                    ..ResultEnd::default()
                })),
            },
        ];
        let json_parts: Vec<_> = parts.iter().map(query_part_json).collect();
        let back: Vec<_> = json_parts.iter().map(query_part_from_json).collect();
        assert_eq!(back, parts);
        assert_eq!(collect(parts).results, vec![r]);
    }

    #[test]
    fn binary_columns() {
        assert!(is_binary(252, 63));
        assert!(!is_binary(252, 45));
        assert!(!is_binary(8, 63));
        assert!(is_binary(16, 63));
        assert_eq!(type_name(253), "VARCHAR");
    }
}
