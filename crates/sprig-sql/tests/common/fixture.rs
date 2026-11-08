//! Captured MariaDB resultsets: column definitions and text rows, stored as
//! JSON under `tests/fixtures/names/`. `names_live.rs` writes them with
//! `SPRIG_GOLDEN_CAPTURE=1`; the offline tests replay them through the
//! result mappers.

use std::path::PathBuf;

use bytes::Bytes;
use serde_json::{Value, json};
use sprig_proto_mysql::ColumnDef;
use sprig_sql::names::TextRow;

/// One captured resultset.
#[derive(Debug, Clone)]
pub struct Captured {
    /// The statement as sent to MariaDB (physical names).
    pub sql: String,
    /// Column definitions.
    pub columns: Vec<ColumnDef>,
    /// Text rows.
    pub rows: Vec<TextRow>,
}

/// The fixture directory.
pub fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/names")
}

fn s(b: &[u8]) -> Value {
    Value::String(String::from_utf8_lossy(b).into_owned())
}

fn b(v: &Value) -> Bytes {
    Bytes::from(v.as_str().unwrap_or_default().to_owned())
}

fn n(v: &Value) -> u64 {
    v.as_u64().unwrap_or_default()
}

impl Captured {
    /// As JSON.
    pub fn to_json(&self) -> Value {
        let cols: Vec<Value> = self
            .columns
            .iter()
            .map(|c| {
                json!({
                    "schema": s(&c.schema), "table": s(&c.table), "org_table": s(&c.org_table),
                    "name": s(&c.name), "org_name": s(&c.org_name), "charset": c.charset,
                    "length": c.length, "type": c.col_type, "flags": c.flags, "decimals": c.decimals,
                })
            })
            .collect();
        let rows: Vec<Value> = self
            .rows
            .iter()
            .map(|r| Value::Array(r.iter().map(|v| v.as_deref().map_or(Value::Null, s)).collect()))
            .collect();
        json!({ "sql": self.sql, "columns": cols, "rows": rows })
    }

    /// From JSON.
    pub fn from_json(v: &Value) -> Self {
        let columns = v["columns"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|c| ColumnDef {
                        schema: b(&c["schema"]),
                        table: b(&c["table"]),
                        org_table: b(&c["org_table"]),
                        name: b(&c["name"]),
                        org_name: b(&c["org_name"]),
                        ext_type: None,
                        charset: u16::try_from(n(&c["charset"])).unwrap_or_default(),
                        length: u32::try_from(n(&c["length"])).unwrap_or_default(),
                        col_type: u8::try_from(n(&c["type"])).unwrap_or_default(),
                        flags: u16::try_from(n(&c["flags"])).unwrap_or_default(),
                        decimals: u8::try_from(n(&c["decimals"])).unwrap_or_default(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let rows = v["rows"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|r| {
                        r.as_array()
                            .map(|cells| {
                                cells
                                    .iter()
                                    .map(|c| (!c.is_null()).then(|| b(c)))
                                    .collect()
                            })
                            .unwrap_or_default()
                    })
                    .collect()
            })
            .unwrap_or_default();
        Self {
            sql: v["sql"].as_str().unwrap_or_default().to_owned(),
            columns,
            rows,
        }
    }

    /// Loads `show/<name>.json`.
    pub fn load(name: &str) -> Self {
        let path = dir().join("show").join(format!("{name}.json"));
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
        let v: Value = serde_json::from_str(&text)
            .unwrap_or_else(|e| panic!("parsing {}: {e}", path.display()));
        Self::from_json(&v)
    }

    /// Writes `show/<name>.json`.
    pub fn save(&self, name: &str) {
        let path = dir().join("show").join(format!("{name}.json"));
        let text = serde_json::to_string_pretty(&self.to_json()).unwrap_or_default();
        std::fs::write(&path, text + "\n")
            .unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
    }

    /// The column names.
    pub fn names(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|c| String::from_utf8_lossy(&c.name).into_owned())
            .collect()
    }
}

/// A row as strings, `NULL` for SQL NULL.
pub fn row_strings(r: &TextRow) -> Vec<String> {
    r.iter()
        .map(|v| {
            v.as_deref()
                .map_or_else(|| "NULL".to_owned(), |b| String::from_utf8_lossy(b).into_owned())
        })
        .collect()
}
