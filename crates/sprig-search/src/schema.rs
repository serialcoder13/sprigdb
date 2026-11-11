//! The Tantivy schema of an index generation, keys and extraction
//! (chapter 39, "Tantivy schema of a generation", "Extraction"; build
//! step 3).
//!
//! | Field | Tantivy | Use |
//! |---|---|---|
//! | `_key` | bytes, indexed, stored | The encoded key: deletes by term, hits |
//! | `f<n>` text kinds | text with positions, not stored | The field's analyzer |
//! | `f<n>` keyword | raw string, fast when filter, sort or facet | Exact match |
//! | `f<n>` number, date, bool | f64, date, bool, indexed, fast when needed | Ranges, sorts |
//! | `_dyn` | JSON object, text | `dynamic` mappings and `$**` |
//!
//! Text is never stored: rows are fetched from MariaDB for output and
//! highlights. A **key** is the row's primary key ([`Key`]): a table's
//! key columns, encoded so the gateway can write them back as SQL
//! literals, or a collection's `_id` bytes as stored.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use sprig_common::{Result, SprigError};
use tantivy::schema::{
    BytesOptions, DateOptions, FacetOptions, Field, IndexRecordOption, JsonObjectOptions,
    NumericOptions, Schema, TextFieldIndexing, TextOptions,
};
use tantivy::{DateTime, TantivyDocument};

use crate::analyze::{Analyzers, CustomAnalyzer};
use crate::def::{FieldDef, FieldKind, IndexDef, Model, Source};

/// One part of a table's key, as read from a row.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum KeyPart {
    /// A signed integer.
    #[serde(rename = "i")]
    Int(i64),
    /// An unsigned integer.
    #[serde(rename = "u")]
    UInt(u64),
    /// A string.
    #[serde(rename = "s")]
    Str(String),
    /// Bytes.
    #[serde(rename = "b")]
    Bin(Vec<u8>),
    /// Anything else, as SQL text (decimals, dates).
    #[serde(rename = "t")]
    Text(String),
}

fn quote_str(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('\'');
    for c in s.chars() {
        match c {
            '\'' => o.push_str("''"),
            '\\' => o.push_str("\\\\"),
            '\0' => o.push_str("\\0"),
            c => o.push(c),
        }
    }
    o.push('\'');
    o
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2 + 3);
    s.push_str("X'");
    for x in b {
        let _ = write!(s, "{x:02X}");
    }
    s.push('\'');
    s
}

impl KeyPart {
    /// The part as a SQL literal.
    #[must_use]
    pub fn sql(&self) -> String {
        match self {
            Self::Int(i) => i.to_string(),
            Self::UInt(u) => u.to_string(),
            Self::Str(s) | Self::Text(s) => quote_str(s),
            Self::Bin(b) => hex(b),
        }
    }
}

/// A key: the bytes in `_key`.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct Key(pub Vec<u8>);

impl Key {
    /// A table key from its parts.
    #[must_use]
    pub fn of_parts(parts: &[KeyPart]) -> Self {
        Self(serde_json::to_vec(parts).unwrap_or_default())
    }

    /// A table key's parts.
    ///
    /// # Errors
    /// Internal for bytes that aren't a table key.
    pub fn parts(&self) -> Result<Vec<KeyPart>> {
        serde_json::from_slice(&self.0)
            .map_err(|e| SprigError::internal(format!("search key is not a table key: {e}")))
    }

    /// The key as a SQL literal: a value, or a row `(a, b)`.
    ///
    /// # Errors
    /// As [`Key::parts`].
    pub fn sql(&self, model: Model) -> Result<String> {
        if model != Model::Table {
            return Ok(hex(&self.0));
        }
        let p = self.parts()?;
        Ok(if p.len() == 1 {
            p[0].sql()
        } else {
            format!(
                "({})",
                p.iter().map(KeyPart::sql).collect::<Vec<_>>().join(", ")
            )
        })
    }
}

/// A column or document value, as extraction reads it.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Val {
    /// SQL `NULL` or JSON `null`.
    Null,
    /// A boolean.
    Bool(bool),
    /// An integer.
    Int(i64),
    /// An unsigned integer.
    UInt(u64),
    /// A float.
    Float(f64),
    /// Text (also decimals and dates as text).
    Str(String),
    /// Bytes.
    Bytes(Vec<u8>),
    /// A JSON value (JSON columns, documents).
    Json(Json),
}

/// A row or document to index.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Record {
    /// A table row: its key and columns (any order, any case).
    Row {
        /// The key.
        key: Key,
        /// Columns.
        columns: Vec<(String, Val)>,
    },
    /// A document: its `_id` bytes and the parsed JSON.
    Doc {
        /// The key.
        key: Key,
        /// The document.
        doc: Json,
    },
}

impl Record {
    /// The key.
    #[must_use]
    pub fn key(&self) -> &Key {
        match self {
            Self::Row { key, .. } | Self::Doc { key, .. } => key,
        }
    }
}

/// A Tantivy schema of one generation, with its fields and analyzers.
#[derive(Debug, Clone)]
pub struct IndexSchema {
    /// The definition.
    pub def: IndexDef,
    /// The schema.
    pub schema: Schema,
    /// `_key`.
    pub key: Field,
    /// `_dyn` (dynamic indexes): every value by path.
    pub dynamic: Option<Field>,
    /// `_all` (dynamic indexes): every string, for queries naming no path.
    pub all: Option<Field>,
    /// Field number → Tantivy field.
    pub fields: HashMap<u16, Field>,
    /// The analyzers.
    pub analyzers: Analyzers,
}

fn text_options(registered: &str, fast: bool) -> TextOptions {
    let o = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(registered)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );
    if fast { o.set_fast("raw") } else { o }
}

impl IndexSchema {
    /// Builds the schema of `def`.
    ///
    /// # Errors
    /// 39828 for analyzers that aren't defined.
    pub fn build(def: &IndexDef, customs: &[CustomAnalyzer]) -> Result<Self> {
        let names = def.analyzer_names();
        let analyzers = Analyzers::resolve(names.iter().map(String::as_str), customs)?;
        let mut b = Schema::builder();
        let key = b.add_bytes_field("_key", BytesOptions::default().set_indexed().set_stored());
        let mut fields = HashMap::new();
        for f in &def.fields {
            let name = format!("f{}", f.field_no);
            let fast = f.fast();
            let field = match f.kind {
                FieldKind::Text | FieldKind::Autocomplete | FieldKind::Substring => {
                    let a = analyzers.get(f.index_analyzer(&def.default_analyzer))?;
                    b.add_text_field(&name, text_options(&a.registered, false))
                }
                FieldKind::Keyword => {
                    let o = TextOptions::default().set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer("raw")
                            .set_index_option(IndexRecordOption::Basic),
                    );
                    b.add_text_field(&name, if fast { o.set_fast("raw") } else { o })
                }
                FieldKind::Number => {
                    let o = NumericOptions::default().set_indexed();
                    b.add_f64_field(&name, if fast { o.set_fast() } else { o })
                }
                FieldKind::Date => {
                    let o = DateOptions::default().set_indexed();
                    b.add_date_field(&name, if fast { o.set_fast() } else { o })
                }
                FieldKind::Bool => {
                    let o = NumericOptions::default().set_indexed();
                    b.add_bool_field(&name, if fast { o.set_fast() } else { o })
                }
                FieldKind::Facet => b.add_facet_field(&name, FacetOptions::default()),
            };
            fields.insert(f.field_no, field);
        }
        let dyn_tok = analyzers
            .get(&def.default_analyzer)
            .map_or_else(|_| "default".to_owned(), |a| a.registered.clone());
        let all = def
            .dynamic
            .then(|| b.add_text_field("_all", text_options(&dyn_tok, false)));
        let dynamic = def.dynamic.then(|| {
            let a = dyn_tok.clone();
            b.add_json_field(
                "_dyn",
                JsonObjectOptions::default().set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer(&a)
                        .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                ),
            )
        });
        Ok(Self {
            def: def.clone(),
            schema: b.build(),
            key,
            dynamic,
            all,
            fields,
            analyzers,
        })
    }

    /// The Tantivy field of `f`.
    ///
    /// # Errors
    /// Internal when the schema doesn't have it (a bug).
    pub fn field(&self, f: &FieldDef) -> Result<Field> {
        self.fields
            .get(&f.field_no)
            .copied()
            .ok_or_else(|| SprigError::internal(format!("no Tantivy field for '{}'", f.name)))
    }

    /// The Tantivy document of `r`; `None` when no field got a value (the
    /// document isn't indexed, only its old version deleted).
    #[must_use]
    pub fn extract(&self, r: &Record) -> Option<TantivyDocument> {
        let mut d = TantivyDocument::new();
        d.add_bytes(self.key, &r.key().0);
        let mut any = false;
        for f in &self.def.fields {
            let Some(field) = self.fields.get(&f.field_no).copied() else {
                continue;
            };
            let mut vals = Vec::new();
            match r {
                Record::Row { columns, .. } => {
                    if let Some((_, v)) = columns
                        .iter()
                        .find(|(n, _)| n.eq_ignore_ascii_case(&f.source_ref))
                    {
                        match v {
                            Val::Json(j) => leaves(j, &mut vals),
                            other => vals.push(other.clone()),
                        }
                    }
                }
                Record::Doc { doc, .. } => {
                    let parts: Vec<&str> = f.source_ref.split('.').collect();
                    path_values(doc, &parts, &mut vals);
                }
            }
            for v in vals {
                any |= add_value(&mut d, field, f.kind, &v);
            }
        }
        if let Some(allf) = self.all {
            let mut vals = Vec::new();
            match r {
                Record::Doc { doc, .. } => {
                    if let Json::Object(m) = doc {
                        for (k, v) in m {
                            if k != "_id" {
                                all_strings(v, &mut vals);
                            }
                        }
                    }
                }
                Record::Row { columns, .. } => {
                    for (_, v) in columns {
                        match v {
                            Val::Str(s) => vals.push(s.clone()),
                            Val::Json(j) => all_strings(j, &mut vals),
                            _ => {}
                        }
                    }
                }
            }
            for v in vals {
                d.add_text(allf, &v);
                any = true;
            }
        }
        if let (Some(dynf), Record::Doc { doc, .. }) = (self.dynamic, r)
            && let Json::Object(m) = doc
        {
            let mut m = m.clone();
            m.remove("_id");
            if !m.is_empty() {
                d.add_object(
                    dynf,
                    m.into_iter()
                        .map(|(k, v)| (k, tantivy::schema::OwnedValue::from(v)))
                        .collect(),
                );
                any = true;
            }
        }
        if let (Some(dynf), Record::Row { columns, .. }) = (self.dynamic, r) {
            let m: Vec<(String, tantivy::schema::OwnedValue)> = columns
                .iter()
                .filter_map(|(k, v)| match v {
                    Val::Str(s) => Some((k.clone(), tantivy::schema::OwnedValue::Str(s.clone()))),
                    _ => None,
                })
                .collect();
            if !m.is_empty() {
                d.add_object(dynf, m.into_iter().collect());
                any = true;
            }
        }
        any.then_some(d)
    }
}

/// Every string under a JSON value.
fn all_strings(j: &Json, out: &mut Vec<String>) {
    match j {
        Json::String(s) => out.push(s.clone()),
        Json::Array(a) => a.iter().for_each(|x| all_strings(x, out)),
        Json::Object(m) => m.values().for_each(|x| all_strings(x, out)),
        _ => {}
    }
}

/// Every scalar under a JSON value (arrays flattened).
fn leaves(j: &Json, out: &mut Vec<Val>) {
    match j {
        Json::Null | Json::Object(_) => {}
        Json::Bool(b) => out.push(Val::Bool(*b)),
        Json::Number(n) => out.push(n.as_i64().map_or_else(
            || {
                n.as_u64()
                    .map_or_else(|| Val::Float(n.as_f64().unwrap_or(0.0)), Val::UInt)
            },
            Val::Int,
        )),
        Json::String(s) => out.push(Val::Str(s.clone())),
        Json::Array(a) => a.iter().for_each(|x| leaves(x, out)),
    }
}

/// The values at a dotted path, through arrays as MongoDB walks them.
fn path_values(j: &Json, parts: &[&str], out: &mut Vec<Val>) {
    let Some((first, rest)) = parts.split_first() else {
        leaves(j, out);
        return;
    };
    match j {
        Json::Object(m) => {
            if let Some(x) = m.get(*first) {
                path_values(x, rest, out);
            }
        }
        Json::Array(a) => {
            if let Ok(i) = first.parse::<usize>()
                && let Some(x) = a.get(i)
            {
                path_values(x, rest, out);
                return;
            }
            for x in a {
                if x.is_object() {
                    path_values(x, parts, out);
                }
            }
        }
        _ => {}
    }
}

fn parse_date(s: &str) -> Option<DateTime> {
    // ISO 8601 or MariaDB's `YYYY-MM-DD[ HH:MM:SS[.ffffff]]`.
    let s = s.trim().trim_end_matches('Z');
    let (date, time) = s.split_once(['T', ' ']).unwrap_or((s, "00:00:00"));
    let mut dp = date.split('-');
    let y: i64 = dp.next()?.parse().ok()?;
    let m: i64 = dp.next()?.parse().ok()?;
    let d: i64 = dp.next()?.parse().ok()?;
    let (hms, frac) = time.split_once('.').unwrap_or((time, ""));
    let mut tp = hms.split(':');
    let hh: i64 = tp.next().unwrap_or("0").parse().ok()?;
    let mm: i64 = tp.next().unwrap_or("0").parse().ok()?;
    let ss: i64 = tp.next().unwrap_or("0").parse().ok()?;
    let micros: i64 = format!("{frac:0<6}").get(..6)?.parse().ok()?;
    // Days from the civil date (Howard Hinnant's algorithm).
    let y2 = if m <= 2 { y - 1 } else { y };
    let era = y2.div_euclid(400);
    let yoe = y2 - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = days * 86_400 + hh * 3600 + mm * 60 + ss;
    Some(DateTime::from_timestamp_micros(secs * 1_000_000 + micros))
}

fn as_str(v: &Val) -> Option<String> {
    Some(match v {
        Val::Str(s) => s.clone(),
        Val::Int(i) => i.to_string(),
        Val::UInt(u) => u.to_string(),
        Val::Float(f) => f.to_string(),
        Val::Bool(b) => b.to_string(),
        Val::Bytes(b) => String::from_utf8(b.clone()).ok()?,
        Val::Json(j) => j.as_str()?.to_owned(),
        Val::Null => return None,
    })
}

fn as_f64(v: &Val) -> Option<f64> {
    #[allow(clippy::cast_precision_loss)]
    match v {
        Val::Int(i) => Some(*i as f64),
        Val::UInt(u) => Some(*u as f64),
        Val::Float(f) => Some(*f),
        Val::Bool(b) => Some(f64::from(u8::from(*b))),
        Val::Str(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// Adds `v` to `d.field`; `false` when it doesn't fit the kind (skipped,
/// counted by `sprig_search_extract_skipped_total`).
fn add_value(d: &mut TantivyDocument, field: Field, kind: FieldKind, v: &Val) -> bool {
    let ok = match kind {
        FieldKind::Text | FieldKind::Autocomplete | FieldKind::Substring | FieldKind::Keyword => {
            match as_str(v) {
                Some(s) if !s.is_empty() || kind == FieldKind::Keyword => {
                    d.add_text(field, &s);
                    true
                }
                _ => false,
            }
        }
        FieldKind::Number => match as_f64(v) {
            Some(f) if f.is_finite() => {
                d.add_f64(field, f);
                true
            }
            _ => false,
        },
        FieldKind::Date => match v {
            Val::Str(s) => parse_date(s).map(|t| d.add_date(field, t)).is_some(),
            Val::Int(ms) => {
                d.add_date(field, DateTime::from_timestamp_millis(*ms));
                true
            }
            _ => false,
        },
        FieldKind::Bool => match v {
            Val::Bool(b) => {
                d.add_bool(field, *b);
                true
            }
            Val::Int(i) => {
                d.add_bool(field, *i != 0);
                true
            }
            Val::UInt(u) => {
                d.add_bool(field, *u != 0);
                true
            }
            _ => false,
        },
        FieldKind::Facet => match as_str(v) {
            Some(s) if s.starts_with('/') => {
                d.add_facet(field, tantivy::schema::Facet::from(s.as_str()));
                true
            }
            _ => false,
        },
    };
    if !ok && !matches!(v, Val::Null) {
        metrics_skip(kind);
    }
    ok
}

fn metrics_skip(kind: FieldKind) {
    tracing::trace!(kind = kind.as_str(), "search extraction skipped a value");
}

/// The source of a field, for callers building records from rows: the
/// columns a table's index reads.
#[must_use]
pub fn columns_read(def: &IndexDef) -> Vec<String> {
    let mut v: Vec<String> = def.key_columns.clone();
    for f in &def.fields {
        if f.source == Source::Column && !v.iter().any(|c| c.eq_ignore_ascii_case(&f.source_ref)) {
            v.push(f.source_ref.clone());
        }
    }
    v
}

/// Parses a date as extraction does (for range queries).
#[must_use]
pub fn date_of(s: &str) -> Option<DateTime> {
    parse_date(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::def::{Flavor, Source};
    use serde_json::json;

    #[test]
    fn keys_render_as_sql() {
        let k = Key::of_parts(&[KeyPart::Int(7), KeyPart::Str("o'k".into())]);
        assert_eq!(k.sql(Model::Table).unwrap(), "(7, 'o''k')");
        assert_eq!(Key(vec![1, 255]).sql(Model::Collection).unwrap(), "X'01FF'");
    }

    #[test]
    fn dates_parse() {
        let t = parse_date("1970-01-02 00:00:01.5").unwrap();
        assert_eq!(t.into_timestamp_micros(), 86_401_500_000);
        assert_eq!(
            parse_date("2024-02-29T12:00:00Z")
                .unwrap()
                .into_timestamp_secs(),
            1_709_208_000
        );
    }

    #[test]
    fn extracts_paths_through_arrays() {
        let mut def = IndexDef::new("i", "d", "c", Model::Collection, Flavor::MongoSearch);
        def.push(FieldDef::new("items.name", Source::Path, FieldKind::Text))
            .unwrap();
        let s = IndexSchema::build(&def, &[]).unwrap();
        let r = Record::Doc {
            key: Key(vec![1]),
            doc: json!({"_id": 1, "items": [{"name": "red shoe"}, {"name": "blue hat"}]}),
        };
        let d = s.extract(&r).unwrap();
        let f = s.fields[&0];
        assert_eq!(d.get_all(f).count(), 2);
        let none = Record::Doc {
            key: Key(vec![2]),
            doc: json!({"_id": 2, "other": 1}),
        };
        assert!(s.extract(&none).is_none());
    }
}
