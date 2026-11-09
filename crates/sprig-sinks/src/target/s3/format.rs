//! Object formats of the S3 target (chapter 51, "S3 target", Formats):
//! JSON Lines, CSV (RFC 4180) and Parquet, with gzip or zstd for the text
//! formats (Parquet compresses its pages with zstd).
//!
//! Columns (`columns` option) are `name[:type][=json path]`, separated by
//! commas: `id:int64, state:string=$.status.code, amount:double`. Types:
//! `string` (default), `int64`, `int32`, `double`, `bool`, `json` (the
//! value's JSON text), `timestamp_ms`. The path defaults to `$.<name>`
//! and reads the record's row: a change record's after image (before for
//! deletes), or a stream or queue record's JSON value.

use std::io::Write as _;
use std::sync::Arc;

use parquet::basic::{Compression, ConvertedType, Repetition, Type as Phys, ZstdLevel};
use parquet::data_type::{BoolType, ByteArray, ByteArrayType, DoubleType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::Type;
use serde_json::{Map, Value, json};
use sprig_common::{Result, SprigCode};

use crate::model::{ChangeOp, SinkRecord, bad_option, err};

/// `format`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// JSON Lines.
    Jsonl,
    /// CSV with a header row.
    Csv,
    /// Parquet, one row group per object.
    Parquet,
}

/// `compression` of the text formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compress {
    /// None.
    None,
    /// gzip.
    Gzip,
    /// zstd.
    Zstd,
}

impl Format {
    /// Parses `format`.
    ///
    /// # Errors
    /// 39231.
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "jsonl" | "json" | "ndjson" => Ok(Format::Jsonl),
            "csv" => Ok(Format::Csv),
            "parquet" => Ok(Format::Parquet),
            other => Err(err(
                SprigCode::SinkFormatUnsupported,
                format!("S3 format must be jsonl, csv or parquet, not {other}"),
            )),
        }
    }

    /// The object name's extension, with the compression's.
    #[must_use]
    pub fn ext(self, c: Compress) -> &'static str {
        match (self, c) {
            (Format::Parquet, _) => "parquet",
            (Format::Jsonl, Compress::None) => "jsonl",
            (Format::Jsonl, Compress::Gzip) => "jsonl.gz",
            (Format::Jsonl, Compress::Zstd) => "jsonl.zst",
            (Format::Csv, Compress::None) => "csv",
            (Format::Csv, Compress::Gzip) => "csv.gz",
            (Format::Csv, Compress::Zstd) => "csv.zst",
        }
    }

    /// The object's content type.
    #[must_use]
    pub fn content_type(self, c: Compress) -> &'static str {
        match (self, c) {
            (Format::Parquet, _) => "application/vnd.apache.parquet",
            (_, Compress::Gzip) => "application/gzip",
            (_, Compress::Zstd) => "application/zstd",
            (Format::Jsonl, Compress::None) => "application/x-ndjson",
            (Format::Csv, Compress::None) => "text/csv",
        }
    }
}

impl Compress {
    /// Parses `compression`.
    ///
    /// # Errors
    /// 39231.
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "none" | "off" => Ok(Compress::None),
            "gzip" | "gz" => Ok(Compress::Gzip),
            "zstd" => Ok(Compress::Zstd),
            other => Err(err(
                SprigCode::SinkFormatUnsupported,
                format!("S3 compression must be none, gzip or zstd, not {other}"),
            )),
        }
    }

    /// Compresses `b`.
    ///
    /// # Errors
    /// The encoder's (internal).
    pub fn apply(self, b: Vec<u8>) -> Result<Vec<u8>> {
        match self {
            Compress::None => Ok(b),
            Compress::Gzip => {
                let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                e.write_all(&b)
                    .and_then(|()| e.finish())
                    .map_err(|e| sprig_common::SprigError::internal(format!("gzip: {e}")))
            }
            Compress::Zstd => zstd::encode_all(&b[..], 3)
                .map_err(|e| sprig_common::SprigError::internal(format!("zstd: {e}"))),
        }
    }
}

/// A column's type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColType {
    /// UTF-8 text.
    String,
    /// 64-bit integer.
    Int64,
    /// 32-bit integer.
    Int32,
    /// Double.
    Double,
    /// Boolean.
    Bool,
    /// The value's JSON text.
    Json,
    /// Milliseconds since the epoch (Parquet `TIMESTAMP_MILLIS`).
    TimestampMs,
}

/// One output column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// Its name.
    pub name: String,
    /// Its type.
    pub ty: ColType,
    /// The JSON path into the record's row, as members.
    pub path: Vec<String>,
}

/// Parses the `columns` option.
///
/// # Errors
/// 39218 for a malformed list or an unknown type.
pub fn parse_columns(spec: &str) -> Result<Vec<Column>> {
    let mut out: Vec<Column> = Vec::new();
    for item in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (head, path) = match item.split_once('=') {
            Some((h, p)) => (h.trim(), Some(p.trim())),
            None => (item, None),
        };
        let (name, ty) = match head.split_once(':') {
            Some((n, t)) => (n.trim(), t.trim()),
            None => (head, "string"),
        };
        if name.is_empty() || name.starts_with("_sprig") {
            return Err(bad_option(format!("columns: bad column name in '{item}'")));
        }
        let ty = match ty.to_ascii_lowercase().as_str() {
            "string" | "text" | "utf8" => ColType::String,
            "int64" | "bigint" | "long" => ColType::Int64,
            "int32" | "int" | "integer" => ColType::Int32,
            "double" | "float64" | "float" => ColType::Double,
            "bool" | "boolean" => ColType::Bool,
            "json" => ColType::Json,
            "timestamp_ms" | "timestamp" => ColType::TimestampMs,
            other => return Err(bad_option(format!("columns: unknown type {other}"))),
        };
        let path = match path {
            None => vec![name.to_owned()],
            Some(p) => {
                let p = p.strip_prefix("$.").or_else(|| p.strip_prefix('$')).unwrap_or(p);
                let parts: Vec<String> = p
                    .split('.')
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .collect();
                if parts.is_empty() {
                    return Err(bad_option(format!("columns: empty path in '{item}'")));
                }
                parts
            }
        };
        if out.iter().any(|c| c.name == name) {
            return Err(bad_option(format!("columns: {name} twice")));
        }
        out.push(Column {
            name: name.to_owned(),
            ty,
            path,
        });
    }
    if out.is_empty() {
        return Err(bad_option("columns is empty"));
    }
    Ok(out)
}

/// The row a record's columns are read from.
#[must_use]
pub fn row_of(r: &SinkRecord) -> Value {
    if r.op.is_some() {
        if r.op == Some(ChangeOp::Delete) {
            return r.before.clone().unwrap_or(Value::Null);
        }
        return r.after.clone().unwrap_or(Value::Null);
    }
    r.value_json()
}

/// Columns of change records without a `columns` option: every member of
/// their rows, in first-seen order, typed by their values (mixed types
/// become strings, objects and arrays JSON).
#[must_use]
pub fn infer_columns(recs: &[&SinkRecord]) -> Vec<Column> {
    let mut cols: Vec<Column> = Vec::new();
    for r in recs {
        let Value::Object(m) = row_of(r) else {
            continue;
        };
        for (k, v) in m {
            let ty = match &v {
                Value::Null => None,
                Value::Bool(_) => Some(ColType::Bool),
                Value::Number(n) if n.is_i64() || n.is_u64() => Some(ColType::Int64),
                Value::Number(_) => Some(ColType::Double),
                Value::String(_) => Some(ColType::String),
                Value::Array(_) | Value::Object(_) => Some(ColType::Json),
            };
            match cols.iter_mut().find(|c| c.name == k) {
                Some(c) => {
                    if let Some(t) = ty
                        && t != c.ty
                    {
                        c.ty = match (c.ty, t) {
                            (ColType::Int64, ColType::Double) | (ColType::Double, ColType::Int64) => {
                                ColType::Double
                            }
                            _ => ColType::String,
                        };
                    }
                }
                None => cols.push(Column {
                    name: k.clone(),
                    ty: ty.unwrap_or(ColType::String),
                    path: vec![k],
                }),
            }
        }
    }
    cols
}

fn pick<'a>(row: &'a Value, path: &[String]) -> Option<&'a Value> {
    let mut v = row;
    for p in path {
        v = match v {
            Value::Object(m) => m.get(p)?,
            Value::Array(a) => a.get(p.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(v)
}

/// One cell, converted to the column's type (`None` for null).
#[derive(Debug, Clone, PartialEq)]
enum Cell {
    Text(String),
    I64(i64),
    I32(i32),
    F64(f64),
    Bool(bool),
}

fn cell(c: &Column, row: &Value) -> std::result::Result<Option<Cell>, String> {
    let Some(v) = pick(row, &c.path).filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    let bad = || format!("column {}: {v} isn't a {:?}", c.name, c.ty);
    Ok(Some(match c.ty {
        ColType::String => Cell::Text(match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        }),
        ColType::Json => Cell::Text(v.to_string()),
        ColType::Int64 | ColType::TimestampMs => Cell::I64(match v {
            Value::Number(n) => n.as_i64().ok_or_else(bad)?,
            Value::String(s) => s.trim().parse().map_err(|_| bad())?,
            _ => return Err(bad()),
        }),
        ColType::Int32 => Cell::I32(match v {
            Value::Number(n) => n
                .as_i64()
                .and_then(|i| i32::try_from(i).ok())
                .ok_or_else(bad)?,
            Value::String(s) => s.trim().parse().map_err(|_| bad())?,
            _ => return Err(bad()),
        }),
        ColType::Double => Cell::F64(match v {
            Value::Number(n) => n.as_f64().ok_or_else(bad)?,
            Value::String(s) => s.trim().parse().map_err(|_| bad())?,
            _ => return Err(bad()),
        }),
        ColType::Bool => Cell::Bool(match v {
            Value::Bool(b) => *b,
            Value::Number(n) if n.as_i64() == Some(0) => false,
            Value::Number(n) if n.as_i64() == Some(1) => true,
            Value::String(s) if s.eq_ignore_ascii_case("true") => true,
            Value::String(s) if s.eq_ignore_ascii_case("false") => false,
            _ => return Err(bad()),
        }),
    }))
}

/// One JSON Lines line: the row's members (or `{"value": v}` when the row
/// isn't an object) and `_sprig` with `key` (the idempotency key), `op`
/// and `ts`, plus the record key.
#[must_use]
pub fn jsonl_line(r: &SinkRecord) -> Value {
    let mut m = match row_of(r) {
        Value::Object(m) => m,
        other => {
            let mut m = Map::new();
            m.insert("value".into(), other);
            m
        }
    };
    let mut s = Map::new();
    s.insert("key".into(), json!(r.idem_key));
    s.insert(
        "op".into(),
        r.op.map_or(Value::Null, |o| json!(o.letter())),
    );
    s.insert("ts".into(), json!(r.ts_ms));
    if let Some(k) = &r.key {
        s.insert("record_key".into(), crate::model::bytes_json_text(k));
    }
    m.insert("_sprig".into(), Value::Object(s));
    Value::Object(m)
}

fn csv_field(out: &mut String, s: &str) {
    if s.contains([',', '"', '\r', '\n']) {
        out.push('"');
        out.push_str(&s.replace('"', "\"\""));
        out.push('"');
    } else {
        out.push_str(s);
    }
}

/// An encoded object: its bytes, and the records left out (with why).
#[derive(Debug, Default)]
pub struct Encoded {
    /// The object's bytes (compressed).
    pub bytes: Vec<u8>,
    /// Indexes into the input that failed for good, with the reason.
    pub failed: Vec<(usize, String)>,
    /// Records written.
    pub records: usize,
}

/// Encodes `recs` in `format`. `columns` is required for CSV and Parquet
/// (the caller infers them for change records).
///
/// # Errors
/// 39231 for CSV or Parquet without columns; encoder errors.
pub fn encode(
    format: Format,
    compress: Compress,
    columns: Option<&[Column]>,
    recs: &[&SinkRecord],
) -> Result<Encoded> {
    let mut failed = Vec::new();
    let raw = match format {
        Format::Jsonl => {
            let mut b = Vec::new();
            for r in recs {
                serde_json::to_writer(&mut b, &jsonl_line(r))
                    .map_err(|e| sprig_common::SprigError::internal(format!("jsonl: {e}")))?;
                b.push(b'\n');
            }
            b
        }
        Format::Csv => {
            let cols = columns.ok_or_else(|| need_columns("csv"))?;
            let mut s = String::from("_sprig_key,_sprig_op,_sprig_ts");
            for c in cols {
                s.push(',');
                csv_field(&mut s, &c.name);
            }
            s.push_str("\r\n");
            for (i, r) in recs.iter().enumerate() {
                let row = row_of(r);
                let mut line = String::new();
                csv_field(&mut line, &r.idem_key);
                line.push(',');
                line.push_str(r.op.map_or("", ChangeOp::letter));
                line.push(',');
                line.push_str(&r.ts_ms.to_string());
                let mut ok = true;
                for c in cols {
                    line.push(',');
                    match cell(c, &row) {
                        Ok(None) => {}
                        Ok(Some(Cell::Text(t))) => csv_field(&mut line, &t),
                        Ok(Some(Cell::I64(n))) => line.push_str(&n.to_string()),
                        Ok(Some(Cell::I32(n))) => line.push_str(&n.to_string()),
                        Ok(Some(Cell::F64(n))) => line.push_str(&n.to_string()),
                        Ok(Some(Cell::Bool(b))) => line.push_str(if b { "true" } else { "false" }),
                        Err(e) => {
                            failed.push((i, e));
                            ok = false;
                            break;
                        }
                    }
                }
                if ok {
                    s.push_str(&line);
                    s.push_str("\r\n");
                }
            }
            s.into_bytes()
        }
        Format::Parquet => {
            let cols = columns.ok_or_else(|| need_columns("parquet"))?;
            let (b, f) = parquet_bytes(cols, recs)?;
            failed = f;
            return Ok(Encoded {
                records: recs.len() - failed.len(),
                bytes: b,
                failed,
            });
        }
    };
    Ok(Encoded {
        records: recs.len() - failed.len(),
        bytes: compress.apply(raw)?,
        failed,
    })
}

fn need_columns(f: &str) -> sprig_common::SprigError {
    err(
        SprigCode::SinkFormatUnsupported,
        format!("{f} needs the columns option for stream and queue sources"),
    )
}

fn pq(e: parquet::errors::ParquetError) -> sprig_common::SprigError {
    sprig_common::SprigError::internal(format!("parquet: {e}"))
}

/// The Parquet schema: `_sprig_key`, `_sprig_op`, `_sprig_ts`, then the
/// columns, all flat.
///
/// # Errors
/// The crate's (internal).
pub fn parquet_schema(cols: &[Column]) -> Result<Type> {
    let prim = |name: &str, p: Phys, ct: ConvertedType, rep: Repetition| {
        Type::primitive_type_builder(name, p)
            .with_converted_type(ct)
            .with_repetition(rep)
            .build()
            .map(Arc::new)
            .map_err(pq)
    };
    let mut fields = vec![
        prim("_sprig_key", Phys::BYTE_ARRAY, ConvertedType::UTF8, Repetition::REQUIRED)?,
        prim("_sprig_op", Phys::BYTE_ARRAY, ConvertedType::UTF8, Repetition::OPTIONAL)?,
        prim(
            "_sprig_ts",
            Phys::INT64,
            ConvertedType::TIMESTAMP_MILLIS,
            Repetition::REQUIRED,
        )?,
    ];
    for c in cols {
        let (p, ct) = match c.ty {
            ColType::String => (Phys::BYTE_ARRAY, ConvertedType::UTF8),
            ColType::Json => (Phys::BYTE_ARRAY, ConvertedType::JSON),
            ColType::Int64 => (Phys::INT64, ConvertedType::NONE),
            ColType::Int32 => (Phys::INT32, ConvertedType::NONE),
            ColType::Double => (Phys::DOUBLE, ConvertedType::NONE),
            ColType::Bool => (Phys::BOOLEAN, ConvertedType::NONE),
            ColType::TimestampMs => (Phys::INT64, ConvertedType::TIMESTAMP_MILLIS),
        };
        fields.push(prim(&c.name, p, ct, Repetition::OPTIONAL)?);
    }
    Type::group_type_builder("sprig_sink")
        .with_fields(fields)
        .build()
        .map_err(pq)
}

fn parquet_bytes(
    cols: &[Column],
    recs: &[&SinkRecord],
) -> Result<(Vec<u8>, Vec<(usize, String)>)> {
    // Convert every row first, so a bad record is left out of every column.
    let mut failed = Vec::new();
    let mut good: Vec<(&SinkRecord, Vec<Option<Cell>>)> = Vec::new();
    for (i, r) in recs.iter().enumerate() {
        let row = row_of(r);
        match cols.iter().map(|c| cell(c, &row)).collect::<std::result::Result<Vec<_>, _>>() {
            Ok(cells) => good.push((r, cells)),
            Err(e) => failed.push((i, e)),
        }
    }
    let schema = Arc::new(parquet_schema(cols)?);
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_dictionary_enabled(true)
            .set_created_by("sprigdb sink".into())
            .build(),
    );
    let mut w = SerializedFileWriter::new(Vec::new(), schema, props).map_err(pq)?;
    {
        let mut rg = w.next_row_group().map_err(pq)?;
        let mut idx = 0usize;
        while let Some(mut col) = rg.next_column().map_err(pq)? {
            match idx {
                0 => {
                    let v: Vec<ByteArray> = good
                        .iter()
                        .map(|(r, _)| ByteArray::from(r.idem_key.as_bytes().to_vec()))
                        .collect();
                    col.typed::<ByteArrayType>()
                        .write_batch(&v, None, None)
                        .map_err(pq)?;
                }
                1 => {
                    let def: Vec<i16> = good.iter().map(|(r, _)| i16::from(r.op.is_some())).collect();
                    let v: Vec<ByteArray> = good
                        .iter()
                        .filter_map(|(r, _)| r.op.map(|o| ByteArray::from(o.letter())))
                        .collect();
                    col.typed::<ByteArrayType>()
                        .write_batch(&v, Some(&def), None)
                        .map_err(pq)?;
                }
                2 => {
                    let v: Vec<i64> = good.iter().map(|(r, _)| r.ts_ms).collect();
                    col.typed::<Int64Type>()
                        .write_batch(&v, None, None)
                        .map_err(pq)?;
                }
                n => {
                    let k = n - 3;
                    let def: Vec<i16> = good.iter().map(|(_, c)| i16::from(c[k].is_some())).collect();
                    let cells = good.iter().filter_map(|(_, c)| c[k].clone());
                    match cols[k].ty {
                        ColType::String | ColType::Json => {
                            let v: Vec<ByteArray> = cells
                                .map(|c| match c {
                                    Cell::Text(t) => ByteArray::from(t.into_bytes()),
                                    _ => ByteArray::from(Vec::new()),
                                })
                                .collect();
                            col.typed::<ByteArrayType>()
                                .write_batch(&v, Some(&def), None)
                                .map_err(pq)?;
                        }
                        ColType::Int64 | ColType::TimestampMs => {
                            let v: Vec<i64> = cells
                                .map(|c| if let Cell::I64(n) = c { n } else { 0 })
                                .collect();
                            col.typed::<Int64Type>()
                                .write_batch(&v, Some(&def), None)
                                .map_err(pq)?;
                        }
                        ColType::Int32 => {
                            let v: Vec<i32> = cells
                                .map(|c| if let Cell::I32(n) = c { n } else { 0 })
                                .collect();
                            col.typed::<Int32Type>()
                                .write_batch(&v, Some(&def), None)
                                .map_err(pq)?;
                        }
                        ColType::Double => {
                            let v: Vec<f64> = cells
                                .map(|c| if let Cell::F64(n) = c { n } else { 0.0 })
                                .collect();
                            col.typed::<DoubleType>()
                                .write_batch(&v, Some(&def), None)
                                .map_err(pq)?;
                        }
                        ColType::Bool => {
                            let v: Vec<bool> = cells
                                .map(|c| matches!(c, Cell::Bool(true)))
                                .collect();
                            col.typed::<BoolType>()
                                .write_batch(&v, Some(&def), None)
                                .map_err(pq)?;
                        }
                    }
                }
            }
            col.close().map_err(pq)?;
            idx += 1;
        }
        rg.close().map_err(pq)?;
    }
    let bytes = w.into_inner().map_err(pq)?;
    Ok((bytes, failed))
}
