//! SQL/PGQ's `GRAPH_TABLE` (chapter 43, "Language surface"; build step
//! 8): the values of a call's rows as SQL literals.
//!
//! Finding the calls in a statement, their Cypher, the placeholders and
//! binding the rows are the router's (`sprig_router::graph`, R8 round 6:
//! `Plan::Graph`); this module only writes graph values as SQL: integers,
//! floats, strings and booleans as themselves, `NULL`, and nodes,
//! relationships, paths, lists, maps and temporal values as their JSON or
//! text form (chapter 43: "Native properties come out as JSON unless
//! cast"). The rows are bounded by [`MAX_INLINE_BYTES`] (39903 past it).

use crate::value::Value;

/// Largest inlined result of one `GRAPH_TABLE`, in bytes of SQL text.
pub const MAX_INLINE_BYTES: usize = 16 << 20;

fn sql_string(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('\'');
    for c in s.chars() {
        match c {
            '\'' => o.push_str("''"),
            '\\' => o.push_str("\\\\"),
            '\0' => o.push_str("\\0"),
            _ => o.push(c),
        }
    }
    o.push('\'');
    o
}

/// One value as a SQL literal.
#[must_use]
pub fn literal(v: &Value) -> String {
    match v {
        Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.into(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) if f.is_finite() => format!("{f:e}"),
        Value::Null | Value::Float(_) => "NULL".into(),
        Value::String(s) => sql_string(s),
        Value::List(_) | Value::Map(_) | Value::Node(_) | Value::Rel(_) | Value::Path(_) => {
            sql_string(&crate::value::to_json(v).to_string())
        }
        other => sql_string(&other.display()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_become_literals() {
        assert_eq!(literal(&Value::Int(1)), "1");
        assert_eq!(literal(&Value::String("it's".into())), "'it''s'");
        assert_eq!(literal(&Value::Null), "NULL");
        assert_eq!(literal(&Value::Float(2.5)), "2.5e0");
        assert_eq!(literal(&Value::Float(f64::NAN)), "NULL");
        assert_eq!(literal(&Value::Bool(true)), "TRUE");
        assert_eq!(literal(&Value::String("a\\b".into())), "'a\\\\b'");
    }
}
