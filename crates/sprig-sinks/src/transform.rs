//! `TRANSFORM f`: a deployed logic
//! function `f(record) → record | [records] | null`, run on each record
//! after `WHERE` and columns, in the sink's worker on the fallback tier
//! (QuickJS, KIRun or Lua from `sprig-logic-js`), whatever tier the
//! function was placed in for SQL callers.
//!
//! ```text
//! at CREATE SINK: f is a live function of the sink's database whose
//!   runtime is javascript, kirun or lua, and whose text doesn't reach for
//!   db or fetch (39219)
//! at task start: f's active version, compiled once
//! per record: f({key, value, headers, op, before, after, ts, tenant, ...})
//!   under sink.transform_budget_ms, with no db and no outbound calls
//!     null / []            → the record is dropped (settled, not delivered)
//!     an object with key, value or headers → the record with those replaced
//!     any other object      → the record with that as its value
//!     an array of those     → several records, keys <idempotency key>#0, #1, ...
//!     an error, a timeout, a db or fetch call → the record fails for good
//!       (dead letter, class transform)
//! ```

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic_js::Limits;
use sprig_logic_js::bridge::NoDb;
use sprig_logic_js::kirun::Host;
use sprig_logic_js::outbound::NoOutbound;
use sprig_logic_js::program::Program;

use crate::model::SinkRecord;

/// A transform function, compiled.
#[derive(Debug, Clone)]
pub struct Transform {
    /// The function's name.
    pub name: String,
    /// Its version.
    pub version: u64,
    program: Program,
    budget: Duration,
}

fn invalid(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::SinkTransformInvalid, m.into())
}

/// What a transform's text may not use: data and outside calls (a
/// transform runs once per delivery attempt, so it must have no effects).
const EFFECTS: &[&str] = &[
    "ctx.db",
    "db.sql",
    "db.get",
    "db.exec",
    "db.query",
    "db.publish",
    "db.mq",
    "fetch(",
    "sprig.fetch",
    "sprig.db",
    "Sprig.Db.",
    "Sprig.Http.",
    "Sprig.Sink.",
    "Sprig.Mq.",
    "Sprig.Stream.",
];

/// Checks a function for `TRANSFORM` (39219): its runtime runs in workers
/// and its text calls nothing outside the record.
///
/// # Errors
/// 39219.
pub fn check(name: &str, runtime: &str, source: &str) -> Result<()> {
    if !matches!(runtime, "javascript" | "kirun" | "lua") {
        return Err(invalid(format!(
            "TRANSFORM {name}: a {runtime} function can't run in a sink worker; use JavaScript, \
             KIRun or Lua"
        )));
    }
    if let Some(e) = EFFECTS.iter().find(|e| source.contains(*e)) {
        return Err(invalid(format!(
            "TRANSFORM {name}: a transform has no effects, and its text uses {}",
            e.trim_end_matches(['(', '.'])
        )));
    }
    Ok(())
}

/// The live function `name` of database `db_id`: its runtime, active
/// version, source text and signature; `None` when there is none.
///
/// # Errors
/// The catalog's.
pub async fn source_of(
    c: &mut mysql_async::Conn,
    catalog_schema: &str,
    db_id: u32,
    name: &str,
) -> Result<Option<(String, u64, String, Value)>> {
    source_at(c, catalog_schema, db_id, name, None).await
}

/// [`source_of`] for version `version` of the function (`None`: the
/// active one): `transform_version` pins a sink to one.
///
/// # Errors
/// The catalog's.
pub async fn source_at(
    c: &mut mysql_async::Conn,
    catalog_schema: &str,
    db_id: u32,
    name: &str,
    version: Option<u64>,
) -> Result<Option<(String, u64, String, Value)>> {
    use mysql_async::prelude::Queryable;
    let q = sprig_common::names::quote(catalog_schema);
    let which = match version {
        Some(v) => format!("v.version = {v}"),
        None => "v.version = m.active_version".to_owned(),
    };
    let r: std::result::Result<Option<(String, u64, String, String)>, mysql_async::Error> = c
        .exec_first(
            format!(
                "SELECT m.runtime, v.version, v.source, CAST(v.signature AS CHAR) \
                 FROM {q}.logic_modules m JOIN {q}.logic_versions v \
                 ON v.module_id = m.module_id AND {which} \
                 WHERE m.db_id = ? AND m.name = ? AND m.kind = 'function' AND m.state = 'active'"
            ),
            (db_id, name),
        )
        .await;
    match r {
        // No logic tables yet: no function.
        Err(mysql_async::Error::Server(e)) if e.code == 1146 => Ok(None),
        Err(e) => Err(
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e),
        ),
        Ok(r) => Ok(r.map(|(runtime, v, src, sig)| {
            (
                runtime,
                v,
                src,
                serde_json::from_str(&sig).unwrap_or(Value::Null),
            )
        })),
    }
}

/// Whether a version's recorded signature says `DETERMINISTIC`
/// (`CREATE FUNCTION ... DETERMINISTIC` records it).
#[must_use]
pub fn is_deterministic(sig: &Value) -> bool {
    sig.get("deterministic").and_then(Value::as_bool) == Some(true)
}

/// The determinism check under `EXACTLY ONCE` (39219): a transform runs
/// again on every delivery attempt, so a replayed record must come out
/// the same.
///
/// # Errors
/// 39219 for a version not declared `DETERMINISTIC`.
pub fn check_deterministic(name: &str, version: u64, sig: &Value) -> Result<()> {
    if is_deterministic(sig) {
        return Ok(());
    }
    Err(invalid(format!(
        "TRANSFORM {name}: version {version} isn't declared DETERMINISTIC, which EXACTLY ONCE needs \
         (CREATE OR REPLACE FUNCTION ... DETERMINISTIC)"
    )))
}

impl Transform {
    /// Compiles `name`'s active version.
    ///
    /// # Errors
    /// 39219 for a function that is gone or can't be a transform; 39861
    /// for text that doesn't compile.
    pub async fn load(
        c: &mut mysql_async::Conn,
        catalog_schema: &str,
        db_id: u32,
        name: &str,
        budget: Duration,
    ) -> Result<Arc<Self>> {
        Self::load_at(c, catalog_schema, db_id, name, None, false, budget).await
    }

    /// Compiles version `version` of `name` (`None`: the active one),
    /// checking it is `DETERMINISTIC` when `exactly_once`.
    ///
    /// # Errors
    /// As [`Transform::load`]; 39219 for a missing version or one that
    /// isn't deterministic under `EXACTLY ONCE`.
    pub async fn load_at(
        c: &mut mysql_async::Conn,
        catalog_schema: &str,
        db_id: u32,
        name: &str,
        version: Option<u64>,
        exactly_once: bool,
        budget: Duration,
    ) -> Result<Arc<Self>> {
        let Some((runtime, version, source, sig)) =
            source_at(c, catalog_schema, db_id, name, version).await?
        else {
            return Err(invalid(match version {
                Some(v) => format!("TRANSFORM {name}: no version {v} of the function"),
                None => format!("TRANSFORM {name}: no such function"),
            }));
        };
        if exactly_once {
            check_deterministic(name, version, &sig)?;
        }
        Self::compile(name, &runtime, version, &source, &sig, budget).map(Arc::new)
    }

    /// Compiles a version.
    ///
    /// # Errors
    /// As [`Transform::load`].
    pub fn compile(
        name: &str,
        runtime: &str,
        version: u64,
        source: &str,
        sig: &Value,
        budget: Duration,
    ) -> Result<Self> {
        check(name, runtime, source)?;
        let program = Program::new(runtime, name, source, sprig_logic::invoke::script_sig(sig))?;
        Ok(Self {
            name: name.to_owned(),
            version,
            program,
            budget,
        })
    }

    /// Runs the function on `rec`.
    ///
    /// # Errors
    /// The reason the record fails for good.
    pub async fn apply(&self, rec: SinkRecord) -> std::result::Result<Vec<SinkRecord>, String> {
        let host = Host {
            db: Arc::new(NoDb),
            outbound: Arc::new(NoOutbound),
            default_db: None,
            info: serde_json::json!({"function": self.name, "version": self.version}),
        };
        let limits = Limits {
            wall: self.budget,
            ..Limits::default()
        };
        let out = self
            .program
            .run(vec![rec.to_json()], host, limits)
            .await
            .map_err(|e| format!("TRANSFORM {}: {}", self.name, e.message))?;
        let v = sprig_logic::invoke::scalar(&out.event, &out.result)
            .map_err(|e| format!("TRANSFORM {}: {}", self.name, e.message))?;
        Ok(records_of(&rec, v))
    }
}

/// The records a transform's result stands for (see the module docs).
#[must_use]
pub fn records_of(rec: &SinkRecord, v: Value) -> Vec<SinkRecord> {
    let one = |x: Value| -> Option<SinkRecord> {
        let mut r = rec.clone();
        match x {
            Value::Null => return None,
            Value::Object(m)
                if m.contains_key("value")
                    || m.contains_key("key")
                    || m.contains_key("headers") =>
            {
                if let Some(k) = m.get("key") {
                    r.key = match k {
                        Value::Null => None,
                        Value::String(s) => Some(s.clone().into_bytes()),
                        other => Some(other.to_string().into_bytes()),
                    };
                }
                if let Some(val) = m.get("value") {
                    set_value(&mut r, val.clone());
                }
                if let Some(Value::Object(h)) = m.get("headers") {
                    for (k, hv) in h {
                        let b = match hv {
                            Value::String(s) => s.clone().into_bytes(),
                            other => other.to_string().into_bytes(),
                        };
                        match r.headers.iter_mut().find(|(n, _)| n == k) {
                            Some(e) => e.1 = b,
                            None => r.headers.push((k.clone(), b)),
                        }
                    }
                }
            }
            other => set_value(&mut r, other),
        }
        Some(r)
    };
    match v {
        Value::Array(items) => {
            let many = items.len() > 1;
            items
                .into_iter()
                .filter_map(one)
                .enumerate()
                .map(|(i, mut r)| {
                    if many {
                        r.idem_key = format!("{}#{i}", rec.idem_key);
                    }
                    r
                })
                .collect()
        }
        other => one(other).into_iter().collect(),
    }
}

fn set_value(r: &mut SinkRecord, v: Value) {
    if r.op.is_some() {
        // A change record's value is its after image.
        r.after = Some(v);
    } else {
        r.value = Some(match v {
            Value::String(s) => s.into_bytes(),
            other => other.to_string().into_bytes(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Position;
    use serde_json::json;

    #[test]
    fn exactly_once_needs_deterministic() {
        assert!(check_deterministic("f", 3, &json!({"deterministic": true})).is_ok());
        let e = check_deterministic("f", 3, &json!({"params": []})).unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::SinkTransformInvalid));
        assert!(e.message.contains("version 3"), "{}", e.message);
        assert!(!is_deterministic(&Value::Null));
    }

    fn rec() -> SinkRecord {
        SinkRecord {
            key: Some(b"k1".to_vec()),
            value: Some(br#"{"type":"user.added","user_id":7,"email":"a@b.c"}"#.to_vec()),
            headers: vec![("x-sprig-tenant".into(), b"acme".to_vec())],
            op: None,
            before: None,
            after: None,
            ts_ms: 10,
            idem_key: "s/1/0/4/1".into(),
            pos: Position::Stream {
                stream: 1,
                partition: 0,
                offset: 4,
                epoch: 1,
            },
        }
    }

    #[tokio::test]
    async fn javascript_reshapes_drops_and_fans_out() {
        let sig = json!({"params": [{"name": "r", "type": "json"}], "scalar": true});
        let t = Transform::compile(
            "shape",
            "javascript",
            1,
            "if (r.value.type !== 'user.added') return null;
             if (r.value.user_id === 0) return [r.value, r.value];
             return { user: r.value.user_id, tenant: r.tenant };",
            &sig,
            Duration::from_millis(500),
        )
        .unwrap();
        let out = t.apply(rec()).await.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(
            serde_json::from_slice::<Value>(out[0].value.as_deref().unwrap()).unwrap(),
            json!({"user": 7, "tenant": "acme"})
        );
        assert_eq!(out[0].idem_key, "s/1/0/4/1");
        let mut other = rec();
        other.value = Some(br#"{"type":"user.removed","user_id":7}"#.to_vec());
        assert!(t.apply(other).await.unwrap().is_empty());
        let mut zero = rec();
        zero.value = Some(br#"{"type":"user.added","user_id":0}"#.to_vec());
        let two = t.apply(zero).await.unwrap();
        assert_eq!(
            two.iter().map(|r| r.idem_key.as_str()).collect::<Vec<_>>(),
            ["s/1/0/4/1#0", "s/1/0/4/1#1"]
        );
    }

    #[tokio::test]
    async fn errors_and_budgets_fail_the_record() {
        let sig = json!({"params": [{"name": "r", "type": "json"}], "scalar": true});
        let t = Transform::compile(
            "boom",
            "javascript",
            1,
            "throw new Error('bad record');",
            &sig,
            Duration::from_millis(500),
        )
        .unwrap();
        assert!(t.apply(rec()).await.unwrap_err().contains("bad record"));
        let t = Transform::compile(
            "spin",
            "javascript",
            1,
            "while (true) {}",
            &sig,
            Duration::from_millis(100),
        )
        .unwrap();
        assert!(t.apply(rec()).await.is_err());
    }

    #[test]
    fn effects_and_runtimes_are_refused() {
        assert!(check("f", "javascript", "function f(r) { return r; }").is_ok());
        for src in [
            "function f(r) { ctx.db.sql('select 1'); return r; }",
            "function f(r) { fetch('https://x'); return r; }",
        ] {
            let e = check("f", "javascript", src).unwrap_err();
            assert_eq!(e.code(), Some(SprigCode::SinkTransformInvalid), "{src}");
        }
        assert!(check("f", "lua", "local x = sprig.db.query('x')").is_err());
        assert!(check("f", "sql", "SELECT 1").is_err());
    }
}
