//! Calling an engine-tier version (chapter 41, "Calling code"; build step
//! 15 in part): the same `CALL db.name(…)` a compiled version answers.
//!
//! An engine-tier version's wrapper ([`crate::deploy::engine_wrapper`])
//! lets MariaDB do what it does for any `CALL`: resolve the name in the
//! session's schema, check `EXECUTE` (chapter 29), and convert the
//! arguments to the declared parameter types. It then returns one
//! **marker** row, `__sprig_engine, module, version, args`, and fails with
//! 39876. The gateway sees the marker, drops it and the error, runs the
//! version on the fallback runtime with the caller's session as its `db`,
//! and answers with the row a compiled wrapper returns: `event`, then
//! every event's fields, typed as the wrapper types them.
//!
//! ```text
//! client ─ CALL shop.quote(7, 3) ─► gateway ─► MariaDB: wrapper `quote`
//!   ◄── marker (module 44, version 2, [7, 3]); ERR 39876 ──┘
//! gateway: version 44/2 from the cache (else the catalog), KIRun run,
//!   each Sprig.Db.* statement through the caller's own tenant session
//! client ◄── event | total ; OK
//! ```
//!
//! A gateway without the runtime passes the marker and 39876 to the
//! client, which then sees a clear error.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use serde_json::Value;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic_js::kirun::Prepared;

/// The marker column's name.
pub const MARKER: &str = "__sprig_engine";

/// What a marker row says.
#[derive(Debug, Clone, PartialEq)]
pub struct Marker {
    /// The module.
    pub module_id: u64,
    /// The version.
    pub version: u64,
    /// The arguments, as MariaDB converted them.
    pub args: Vec<Value>,
}

impl Marker {
    /// Reads a marker row: `__sprig_engine`, `module`, `version`, `args`.
    #[must_use]
    pub fn parse(columns: &[String], row: &[Option<String>]) -> Option<Self> {
        if columns.first().map(String::as_str) != Some(MARKER) || row.len() < 4 {
            return None;
        }
        let module_id = row[1].as_deref()?.parse().ok()?;
        let version = row[2].as_deref()?.parse().ok()?;
        let args = match row[3].as_deref() {
            Some(t) => serde_json::from_str::<Vec<Value>>(t).ok()?,
            None => Vec::new(),
        };
        Some(Self {
            module_id,
            version,
            args,
        })
    }
}

/// An output column's type, as the compiled wrapper types it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutTy {
    /// `event`: `VARCHAR(64)`.
    Event,
    /// `i64` and `bool`: `BIGINT`.
    Int,
    /// `f64`: `DOUBLE`.
    Float,
    /// `str`: text.
    Str,
    /// `json` and `null`: JSON text.
    Json,
}

/// One output column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutCol {
    /// Its name.
    pub name: String,
    /// Its type.
    pub ty: OutTy,
}

/// An engine-tier version, ready to call.
#[derive(Debug)]
pub struct EngineVersion {
    /// The module.
    pub module_id: u64,
    /// The version.
    pub version: u64,
    /// The function's name.
    pub name: String,
    /// Its database.
    pub db_id: u32,
    /// `kirun`, `javascript`, ...
    pub runtime: String,
    /// `SQL SECURITY DEFINER`.
    pub definer: bool,
    /// The tier placement chose.
    pub tier: String,
    /// The parsed definition (KIRun).
    pub prepared: Option<Prepared>,
    /// The result row's columns.
    pub columns: Vec<OutCol>,
}

impl EngineVersion {
    /// Checks that this gateway can run the version.
    ///
    /// # Errors
    /// 39876 for a runtime this build lacks (JavaScript) or for
    /// `SQL SECURITY DEFINER` (the fallback tier runs with the caller's
    /// session only, so far).
    pub fn runnable(&self) -> Result<&Prepared> {
        let unavailable = |why: &str| {
            Err(SprigError::new(
                SprigCode::LogicEngineUnavailable,
                format!("{} version {}: {why}", self.name, self.version),
            ))
        };
        if self.definer {
            return unavailable(
                "SQL SECURITY DEFINER isn't supported in the engine tier yet; deploy it SQL SECURITY INVOKER",
            );
        }
        match &self.prepared {
            Some(p) if self.runtime == "kirun" => Ok(p),
            _ => unavailable(&format!("the {} runtime isn't built yet", self.runtime)),
        }
    }

    /// The result row for an event and its fields, as text (`None` is
    /// `NULL`), formatted as MariaDB formats the compiled wrapper's row.
    #[must_use]
    pub fn row(&self, event: &str, result: &Value) -> Vec<Option<String>> {
        self.columns
            .iter()
            .map(|c| match c.ty {
                OutTy::Event => Some(event.to_owned()),
                ty => cell(result.get(&c.name), ty),
            })
            .collect()
    }
}

fn cell(v: Option<&Value>, ty: OutTy) -> Option<String> {
    let v = v?;
    if v.is_null() {
        return None;
    }
    Some(match ty {
        OutTy::Int => match v {
            Value::Bool(b) => i64::from(*b).to_string(),
            Value::Number(n) => n.as_i64().map_or_else(
                || {
                    #[allow(clippy::cast_possible_truncation)]
                    let t = n.as_f64().unwrap_or(0.0).trunc() as i64;
                    t.to_string()
                },
                |i| i.to_string(),
            ),
            Value::String(s) => s.trim().parse::<f64>().map_or_else(
                |_| "0".to_owned(),
                |x| {
                    #[allow(clippy::cast_possible_truncation)]
                    let t = x.trunc() as i64;
                    t.to_string()
                },
            ),
            other => mariadb_json(other),
        },
        OutTy::Float => match v {
            Value::Bool(b) => i64::from(*b).to_string(),
            Value::Number(n) => double_text(n.as_f64().unwrap_or(0.0)),
            Value::String(s) => double_text(s.trim().parse::<f64>().unwrap_or(0.0)),
            other => mariadb_json(other),
        },
        OutTy::Str => match v {
            Value::String(s) => s.clone(),
            other => mariadb_json(other),
        },
        OutTy::Json | OutTy::Event => mariadb_json(v),
    })
}

/// A double as MariaDB's text protocol prints it.
#[allow(clippy::float_cmp)] // whole numbers print without a fraction
fn double_text(x: f64) -> String {
    if x == x.trunc() && x.abs() < 1e15 {
        #[allow(clippy::cast_possible_truncation)]
        let i = x as i64;
        i.to_string()
    } else {
        let s = format!("{x}");
        if x.abs() >= 1e15 || (x != 0.0 && x.abs() < 1e-4) {
            format!("{x:e}")
        } else {
            s
        }
    }
}

/// JSON as MariaDB writes it (`JSON_OBJECT`, `JSON_EXTRACT`): `, ` and
/// `: ` between members.
#[must_use]
pub fn mariadb_json(v: &Value) -> String {
    match v {
        Value::Array(a) => {
            let parts: Vec<String> = a.iter().map(mariadb_json).collect();
            format!("[{}]", parts.join(", "))
        }
        Value::Object(o) => {
            let parts: Vec<String> = o
                .iter()
                .map(|(k, v)| format!("{}: {}", Value::String(k.clone()), mariadb_json(v)))
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
        other => other.to_string(),
    }
}

/// The wrapper's columns from a version's stored signature: `event`, then
/// every event's fields once each, in event order.
#[must_use]
pub fn columns(signature: &Value) -> Vec<OutCol> {
    let mut out = vec![OutCol {
        name: "event".to_owned(),
        ty: OutTy::Event,
    }];
    for e in signature["events"].as_array().into_iter().flatten() {
        for f in e["fields"].as_array().into_iter().flatten() {
            let Some(name) = f["name"].as_str() else {
                continue;
            };
            if name == "event" || out.iter().any(|c| c.name == name) {
                continue;
            }
            let ty = match f["type"].as_str() {
                Some("i64" | "bool") => OutTy::Int,
                Some("f64") => OutTy::Float,
                Some("str") => OutTy::Str,
                _ => OutTy::Json,
            };
            out.push(OutCol {
                name: name.to_owned(),
                ty,
            });
        }
    }
    out
}

type Key = (u64, u64, u64);

/// Versions already loaded, by (tenant, module, version). Versions never
/// change once recorded, so entries never go stale; the cache is bounded
/// by clearing it whole.
static CACHE: LazyLock<Mutex<HashMap<Key, Arc<EngineVersion>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Entries kept before the cache is cleared.
const CACHE_MAX: usize = 1024;

/// A version already loaded.
#[must_use]
pub fn cached(tenant: u64, module_id: u64, version: u64) -> Option<Arc<EngineVersion>> {
    CACHE
        .lock()
        .ok()
        .and_then(|m| m.get(&(tenant, module_id, version)).cloned())
}

/// Loads version `version` of `module_id` from the tenant's logic catalog
/// (`t<tenant>_sprig`), parsing a KIRun definition once.
///
/// # Errors
/// 39860 when there is no such engine-tier version; 39861 for a source
/// that no longer parses; 39070 or a backend error.
pub async fn load(
    c: &mut Conn,
    catalog_schema: &str,
    tenant: u64,
    module_id: u64,
    version: u64,
) -> Result<Arc<EngineVersion>> {
    let key = (tenant, module_id, version);
    if let Some(v) = CACHE.lock().ok().and_then(|m| m.get(&key).cloned()) {
        return Ok(v);
    }
    let q = sprig_common::names::quote(catalog_schema);
    let row: Option<(String, u32, String, String, String, String, String)> = c
        .exec_first(
            format!(
                "SELECT m.name, m.db_id, m.runtime, m.security, v.tier, v.source, CAST(v.signature AS CHAR) \
                 FROM {q}.logic_versions v JOIN {q}.logic_modules m ON m.module_id = v.module_id \
                 WHERE v.module_id = ? AND v.version = ?"
            ),
            (module_id, version),
        )
        .await
        .map_err(|e| {
            SprigError::new(
                SprigCode::CatalogUnavailable,
                "the logic catalog is unreachable",
            )
            .with_source(e)
        })?;
    let Some((name, db_id, runtime, security, tier, source, signature)) = row else {
        return Err(SprigError::new(
            SprigCode::LogicNotFound,
            format!("logic module {module_id} has no version {version}"),
        ));
    };
    if tier != "engine" {
        return Err(SprigError::new(
            SprigCode::LogicNotFound,
            format!("{name} version {version} isn't an engine-tier version"),
        ));
    }
    let prepared = if runtime == "kirun" {
        Some(Prepared::new(&source)?)
    } else {
        None
    };
    let sig: Value = serde_json::from_str(&signature).unwrap_or_default();
    let v = Arc::new(EngineVersion {
        module_id,
        version,
        name,
        db_id,
        runtime,
        definer: security == "definer",
        tier,
        prepared,
        columns: columns(&sig),
    });
    if let Ok(mut m) = CACHE.lock() {
        if m.len() >= CACHE_MAX {
            m.clear();
        }
        m.insert(key, Arc::clone(&v));
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use serde_json::json;

    #[test]
    fn markers_and_rows() {
        let cols: Vec<String> = ["__sprig_engine", "module", "version", "args"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        let m = Marker::parse(
            &cols,
            &[
                Some("sprig".into()),
                Some("44".into()),
                Some("2".into()),
                Some("[7, \"x\", null]".into()),
            ],
        )
        .unwrap();
        assert_eq!(m.module_id, 44);
        assert_eq!(m.args, vec![json!(7), json!("x"), Value::Null]);
        assert!(Marker::parse(&cols[1..], &[]).is_none());

        let sig = json!({"events": [
            {"name": "output", "fields": [{"name": "total", "type": "f64"}, {"name": "n", "type": "i64"}, {"name": "rows", "type": "json"}]},
            {"name": "error", "fields": [{"name": "message", "type": "str"}, {"name": "n", "type": "i64"}]}]});
        let v = EngineVersion {
            module_id: 44,
            version: 2,
            name: "quote".into(),
            db_id: 1,
            runtime: "kirun".into(),
            definer: false,
            tier: "engine".into(),
            prepared: None,
            columns: columns(&sig),
        };
        let names: Vec<&str> = v.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["event", "total", "n", "rows", "message"]);
        let row = v.row(
            "output",
            &json!({"total": 1240.0, "n": true, "rows": [{"a": 1, "b": "x"}]}),
        );
        assert_eq!(
            row,
            vec![
                Some("output".into()),
                Some("1240".into()),
                Some("1".into()),
                Some("[{\"a\": 1, \"b\": \"x\"}]".into()),
                None
            ]
        );
        assert!(v.runnable().is_err());
    }
}
