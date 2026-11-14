//! The JSON-schema subset tool inputs use, and its checks.
//!
//! Supported: `type` (a name or a list of names), `properties`,
//! `required`, `additionalProperties: false`, `items`, `enum` and
//! `description`. Numeric and length limits aren't used in schemas
//! (providers' strict modes refuse them); tools check them in code.

use serde_json::Value;
use sprig_common::{Result, SprigError};

/// Keywords a tool schema may not use.
const FORBIDDEN: &[&str] = &[
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
    "minLength",
    "maxLength",
    "minItems",
    "maxItems",
    "pattern",
    "$ref",
];

/// Checks a tool's own schema: an object at the top, every object closed
/// with `additionalProperties: false`, no unsupported keyword.
///
/// # Errors
/// Internal, naming the tool: a bad built-in schema is a bug.
pub fn check_spec_schema(tool: &str, s: &Value) -> Result<()> {
    if s.get("type").and_then(Value::as_str) != Some("object") {
        return Err(SprigError::internal(format!(
            "tool {tool}: the input schema must be an object"
        )));
    }
    walk(tool, s)
}

fn walk(tool: &str, s: &Value) -> Result<()> {
    let Some(m) = s.as_object() else {
        return Ok(());
    };
    for k in FORBIDDEN {
        if m.contains_key(*k) {
            return Err(SprigError::internal(format!(
                "tool {tool}: schemas don't use {k}"
            )));
        }
    }
    if types(s).contains(&"object") && m.get("additionalProperties") != Some(&Value::Bool(false)) {
        return Err(SprigError::internal(format!(
            "tool {tool}: every object needs additionalProperties: false"
        )));
    }
    if let Some(p) = m.get("properties").and_then(Value::as_object) {
        for v in p.values() {
            walk(tool, v)?;
        }
    }
    if let Some(i) = m.get("items") {
        walk(tool, i)?;
    }
    Ok(())
}

fn types(s: &Value) -> Vec<&str> {
    match s.get("type") {
        Some(Value::String(t)) => vec![t.as_str()],
        Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    }
}

fn type_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_i64() || n.is_u64() => "integer",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Checks `v` against schema `s`.
///
/// # Errors
/// What doesn't fit, with its path (`$.limit: expected integer`).
pub fn validate(s: &Value, v: &Value) -> std::result::Result<(), String> {
    at(s, v, "$")
}

fn at(s: &Value, v: &Value, path: &str) -> std::result::Result<(), String> {
    let want = types(s);
    let have = type_of(v);
    let fits =
        want.is_empty() || want.contains(&have) || (have == "integer" && want.contains(&"number"));
    if !fits {
        return Err(format!(
            "{path}: expected {}, got {have}",
            want.join(" or ")
        ));
    }
    if let Some(e) = s.get("enum").and_then(Value::as_array)
        && !e.contains(v)
    {
        let opts: Vec<String> = e.iter().map(Value::to_string).collect();
        return Err(format!("{path}: must be one of {}", opts.join(", ")));
    }
    match v {
        Value::Object(o) => {
            let props = s.get("properties").and_then(Value::as_object);
            if let Some(req) = s.get("required").and_then(Value::as_array) {
                for r in req.iter().filter_map(Value::as_str) {
                    if !o.contains_key(r) {
                        return Err(format!("{path}: {r} is required"));
                    }
                }
            }
            for (k, x) in o {
                match props.and_then(|p| p.get(k)) {
                    Some(ps) => at(ps, x, &format!("{path}.{k}"))?,
                    None if s.get("additionalProperties") == Some(&Value::Bool(false)) => {
                        return Err(format!("{path}: unknown field {k}"));
                    }
                    None => {}
                }
            }
        }
        Value::Array(a) => {
            if let Some(items) = s.get("items") {
                for (i, x) in a.iter().enumerate() {
                    at(items, x, &format!("{path}[{i}]"))?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn inputs_are_checked_against_the_subset() {
        let s = json!({
            "type": "object", "additionalProperties": false,
            "properties": {
                "db": {"type": ["string", "null"]},
                "limit": {"type": "integer"},
                "mode": {"type": "string", "enum": ["a", "b"]},
                "cols": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["limit"],
        });
        assert!(validate(&s, &json!({"limit": 3})).is_ok());
        assert!(validate(&s, &json!({"limit": 3, "db": null, "cols": ["x"]})).is_ok());
        assert_eq!(
            validate(&s, &json!({})).unwrap_err(),
            "$: limit is required"
        );
        assert_eq!(
            validate(&s, &json!({"limit": "3"})).unwrap_err(),
            "$.limit: expected integer, got string"
        );
        assert_eq!(
            validate(&s, &json!({"limit": 1, "x": 1})).unwrap_err(),
            "$: unknown field x"
        );
        assert!(validate(&s, &json!({"limit": 1, "mode": "c"})).is_err());
        assert_eq!(
            validate(&s, &json!({"limit": 1, "cols": [1]})).unwrap_err(),
            "$.cols[0]: expected string, got integer"
        );
        assert!(check_spec_schema("t", &s).is_ok());
        let open = json!({"type": "object", "properties": {}});
        assert!(check_spec_schema("t", &open).is_err());
        let limited = json!({"type": "object", "additionalProperties": false,
            "properties": {"n": {"type": "integer", "maximum": 5}}});
        assert!(check_spec_schema("t", &limited).is_err());
    }
}
