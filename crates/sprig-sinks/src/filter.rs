//! `WHERE`, `COLUMNS` and `EXCEPT COLUMNS` (chapter 51, "Sinks", step 2b of
//! the pipeline). The condition is parsed with sqlparser (MySQL dialect)
//! when the sink is created and evaluated in the worker on each record:
//!
//! - stream and queue records see `key`, `value`, `headers`, `ts`, with
//!   JSON paths into them (`value->>'$.type' = 'order.paid'`,
//!   `headers->>'$.type'`);
//! - change records see the row's columns (the after image, or the before
//!   image for deletes) and `_op` (`c`, `u`, `d`, `r`).
//!
//! The subset: literals, names, `AND`, `OR`, `NOT`, comparisons, `+ - * /`,
//! `->` and `->>` with `$.a.b[0]` paths, `IS [NOT] NULL`, `[NOT] IN (...)`,
//! `[NOT] BETWEEN`, `[NOT] LIKE` and parentheses. Anything else is refused
//! when the sink is created (39218). Comparisons follow SQL: `NULL` makes
//! them unknown, and an unknown condition drops the record.

use serde_json::{Map, Value};
use sprig_common::Result;
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::model::{ChangeOp, Projection, SinkRecord, bad_option, bytes_json};
use crate::runtime::Prepare;

/// A parsed condition.
#[derive(Debug, Clone)]
pub struct Condition(Expr);

/// Parses and checks a `WHERE` condition.
///
/// # Errors
/// 39218 when it doesn't parse or uses something outside the subset.
pub fn parse(text: &str) -> Result<Condition> {
    let mut p = Parser::new(&MySqlDialect {})
        .try_with_sql(text)
        .map_err(|e| bad_option(format!("WHERE: {e}")))?;
    let e = p
        .parse_expr()
        .map_err(|e| bad_option(format!("WHERE: {e}")))?;
    check(&e)?;
    Ok(Condition(e))
}

fn check(e: &Expr) -> Result<()> {
    match e {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Value(_) => Ok(()),
        Expr::Nested(x)
        | Expr::IsNull(x)
        | Expr::IsNotNull(x)
        | Expr::IsTrue(x)
        | Expr::IsFalse(x) => check(x),
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Not | UnaryOperator::Minus | UnaryOperator::Plus => check(expr),
            _ => Err(bad_option(format!(
                "WHERE: the operator {op} isn't supported"
            ))),
        },
        Expr::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::And
                | BinaryOperator::Or
                | BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Arrow
                | BinaryOperator::LongArrow => {}
                other => {
                    return Err(bad_option(format!(
                        "WHERE: the operator {other} isn't supported"
                    )));
                }
            }
            check(left)?;
            check(right)
        }
        Expr::InList { expr, list, .. } => {
            check(expr)?;
            list.iter().try_for_each(check)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            check(expr)?;
            check(low)?;
            check(high)
        }
        Expr::Like {
            expr,
            pattern,
            escape_char: None,
            any: false,
            ..
        } => {
            check(expr)?;
            check(pattern)
        }
        other => Err(bad_option(format!(
            "WHERE: {other} isn't supported in a sink condition"
        ))),
    }
}

/// The names a condition sees for one record.
struct Env<'a> {
    rec: &'a SinkRecord,
    row: Option<&'a Map<String, Value>>,
}

impl Env<'_> {
    fn name(&self, n: &str) -> Value {
        let lower = n.to_ascii_lowercase();
        if let Some(row) = self.row {
            if lower == "_op" {
                return self.rec.op.map_or(Value::Null, |o| Value::from(o.letter()));
            }
            return row
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(n))
                .map_or(Value::Null, |(_, v)| v.clone());
        }
        match lower.as_str() {
            "key" => bytes_json(self.rec.key.as_deref()),
            "value" => self.rec.value_json(),
            "headers" => self.rec.headers_json(),
            "ts" => Value::from(self.rec.ts_ms),
            "_op" => self.rec.op.map_or(Value::Null, |o| Value::from(o.letter())),
            _ => Value::Null,
        }
    }
}

/// `$.a.b[0]` into `v`.
fn json_path(v: &Value, path: &str) -> Value {
    let Some(rest) = path.trim().strip_prefix('$') else {
        return Value::Null;
    };
    let mut cur = v.clone();
    let mut s = rest;
    while !s.is_empty() {
        if let Some(r) = s.strip_prefix('.') {
            let (name, tail) = r.find(['.', '[']).map_or((r, ""), |i| (&r[..i], &r[i..]));
            let name = name.trim_matches('"');
            cur = cur.get(name).cloned().unwrap_or(Value::Null);
            s = tail;
        } else if let Some(r) = s.strip_prefix('[') {
            let Some(end) = r.find(']') else {
                return Value::Null;
            };
            let idx: usize = r[..end].trim().parse().unwrap_or(usize::MAX);
            cur = cur.get(idx).cloned().unwrap_or(Value::Null);
            s = &r[end + 1..];
        } else {
            return Value::Null;
        }
    }
    cur
}

fn num(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn truth(v: &Value) -> Option<bool> {
    match v {
        Value::Null => None,
        Value::Bool(b) => Some(*b),
        Value::Number(n) => Some(n.as_f64().is_some_and(|f| f != 0.0)),
        Value::String(s) => Some(s.trim().parse::<f64>().is_ok_and(|f| f != 0.0)),
        _ => Some(true),
    }
}

/// SQL comparison: numbers as numbers when both sides are numeric, else
/// as text; `None` when either side is `NULL`.
fn cmp(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    if a.is_null() || b.is_null() {
        return None;
    }
    let numeric = |v: &Value| matches!(v, Value::Number(_) | Value::Bool(_));
    if (numeric(a) || numeric(b))
        && let (Some(x), Some(y)) = (num(a), num(b))
    {
        return x.partial_cmp(&y);
    }
    Some(text(a).cmp(&text(b)))
}

fn text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn like(s: &str, pat: &str) -> bool {
    fn go(s: &[char], p: &[char]) -> bool {
        match p.first() {
            None => s.is_empty(),
            Some('%') => (0..=s.len()).any(|i| go(&s[i..], &p[1..])),
            Some('_') => !s.is_empty() && go(&s[1..], &p[1..]),
            Some('\\') if p.len() > 1 => s.first() == Some(&p[1]) && go(&s[1..], &p[2..]),
            Some(c) => s.first().is_some_and(|x| x.eq_ignore_ascii_case(c)) && go(&s[1..], &p[1..]),
        }
    }
    let s: Vec<char> = s.chars().collect();
    let p: Vec<char> = pat.chars().collect();
    go(&s, &p)
}

fn b(v: Option<bool>) -> Value {
    v.map_or(Value::Null, Value::Bool)
}

fn eval(e: &Expr, env: &Env<'_>) -> Value {
    match e {
        Expr::Identifier(i) => env.name(&i.value),
        Expr::CompoundIdentifier(v) => v.last().map_or(Value::Null, |i| env.name(&i.value)),
        Expr::Value(v) => {
            use sqlparser::ast::Value as V;
            match &v.value {
                V::Number(n, _) => n
                    .parse::<i64>()
                    .map(Value::from)
                    .or_else(|_| n.parse::<f64>().map(Value::from))
                    .unwrap_or(Value::Null),
                V::SingleQuotedString(s) | V::DoubleQuotedString(s) => Value::from(s.clone()),
                V::Boolean(x) => Value::Bool(*x),
                _ => Value::Null,
            }
        }
        Expr::Nested(x) => eval(x, env),
        Expr::IsNull(x) => Value::Bool(eval(x, env).is_null()),
        Expr::IsNotNull(x) => Value::Bool(!eval(x, env).is_null()),
        Expr::IsTrue(x) => Value::Bool(truth(&eval(x, env)) == Some(true)),
        Expr::IsFalse(x) => Value::Bool(truth(&eval(x, env)) == Some(false)),
        Expr::UnaryOp { op, expr } => {
            let v = eval(expr, env);
            match op {
                UnaryOperator::Not => b(truth(&v).map(|t| !t)),
                UnaryOperator::Minus => num(&v).map_or(Value::Null, |f| Value::from(-f)),
                _ => v,
            }
        }
        Expr::BinaryOp { left, op, right } => {
            if *op == BinaryOperator::And {
                let (l, r) = (truth(&eval(left, env)), truth(&eval(right, env)));
                return b(match (l, r) {
                    (Some(false), _) | (_, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    _ => None,
                });
            }
            if *op == BinaryOperator::Or {
                let (l, r) = (truth(&eval(left, env)), truth(&eval(right, env)));
                return b(match (l, r) {
                    (Some(true), _) | (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                });
            }
            let (l, r) = (eval(left, env), eval(right, env));
            use std::cmp::Ordering as O;
            match op {
                BinaryOperator::Eq => b(cmp(&l, &r).map(|o| o == O::Equal)),
                BinaryOperator::NotEq => b(cmp(&l, &r).map(|o| o != O::Equal)),
                BinaryOperator::Lt => b(cmp(&l, &r).map(|o| o == O::Less)),
                BinaryOperator::LtEq => b(cmp(&l, &r).map(|o| o != O::Greater)),
                BinaryOperator::Gt => b(cmp(&l, &r).map(|o| o == O::Greater)),
                BinaryOperator::GtEq => b(cmp(&l, &r).map(|o| o != O::Less)),
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide => match (num(&l), num(&r)) {
                    (Some(x), Some(y)) => {
                        let v = match op {
                            BinaryOperator::Plus => x + y,
                            BinaryOperator::Minus => x - y,
                            BinaryOperator::Multiply => x * y,
                            _ if y == 0.0 => return Value::Null,
                            _ => x / y,
                        };
                        serde_json::Number::from_f64(v).map_or(Value::Null, Value::Number)
                    }
                    _ => Value::Null,
                },
                BinaryOperator::Arrow | BinaryOperator::LongArrow => {
                    let base = match &l {
                        Value::String(s) => serde_json::from_str(s).unwrap_or(l.clone()),
                        _ => l.clone(),
                    };
                    let v = json_path(&base, &text(&r));
                    if *op == BinaryOperator::LongArrow {
                        match v {
                            Value::Null => Value::Null,
                            Value::String(s) => Value::String(s),
                            other => Value::String(other.to_string()),
                        }
                    } else {
                        v
                    }
                }
                _ => Value::Null,
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let v = eval(expr, env);
            if v.is_null() {
                return Value::Null;
            }
            let found = list
                .iter()
                .any(|x| cmp(&v, &eval(x, env)) == Some(std::cmp::Ordering::Equal));
            Value::Bool(found != *negated)
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let v = eval(expr, env);
            let r = match (cmp(&v, &eval(low, env)), cmp(&v, &eval(high, env))) {
                (Some(a), Some(c)) => {
                    Some(a != std::cmp::Ordering::Less && c != std::cmp::Ordering::Greater)
                }
                _ => None,
            };
            b(r.map(|x| x != *negated))
        }
        Expr::Like {
            negated,
            expr,
            pattern,
            ..
        } => {
            let (v, p) = (eval(expr, env), eval(pattern, env));
            if v.is_null() || p.is_null() {
                return Value::Null;
            }
            Value::Bool(like(&text(&v), &text(&p)) != *negated)
        }
        _ => Value::Null,
    }
}

impl Condition {
    /// Whether the record passes (an unknown result drops it).
    #[must_use]
    pub fn matches(&self, rec: &SinkRecord) -> bool {
        let row = change_row(rec);
        let env = Env {
            rec,
            row: row.as_ref(),
        };
        truth(&eval(&self.0, &env)) == Some(true)
    }
}

/// A change record's row: the after image, or the before image for a
/// delete.
fn change_row(rec: &SinkRecord) -> Option<Map<String, Value>> {
    rec.op?;
    let img = if rec.op == Some(ChangeOp::Delete) {
        rec.before.as_ref()
    } else {
        rec.after.as_ref().or(rec.before.as_ref())
    };
    img.and_then(Value::as_object).cloned()
}

fn project(img: &mut Option<Value>, p: &Projection) {
    let Some(Value::Object(m)) = img else {
        return;
    };
    match p {
        Projection::Only { columns } => {
            m.retain(|k, _| columns.iter().any(|c| c.eq_ignore_ascii_case(k)));
        }
        Projection::Except { columns } => {
            m.retain(|k, _| !columns.iter().any(|c| c.eq_ignore_ascii_case(k)));
        }
    }
}

/// The preparation step of a sink: its condition, then its projection
/// (change records only). `TRANSFORM` is not built (a sink with one is
/// refused when created).
#[derive(Debug, Clone, Default)]
pub struct SinkPrepare {
    /// `WHERE`.
    pub condition: Option<Condition>,
    /// `COLUMNS` / `EXCEPT COLUMNS`.
    pub projection: Option<Projection>,
    /// `TRANSFORM f` (R9, r9more).
    pub transform: Option<std::sync::Arc<crate::transform::Transform>>,
}

#[async_trait::async_trait]
impl Prepare for SinkPrepare {
    async fn transform(
        &self,
        recs: Vec<SinkRecord>,
    ) -> std::result::Result<Vec<SinkRecord>, String> {
        let Some(t) = &self.transform else {
            return Ok(recs);
        };
        let mut out = Vec::with_capacity(recs.len());
        for r in recs {
            out.extend(t.apply(r).await?);
        }
        Ok(out)
    }

    fn prepare(&self, mut rec: SinkRecord) -> std::result::Result<Vec<SinkRecord>, String> {
        if let Some(c) = &self.condition
            && !c.matches(&rec)
        {
            return Ok(Vec::new());
        }
        if let Some(p) = &self.projection {
            project(&mut rec.after, p);
            project(&mut rec.before, p);
        }
        Ok(vec![rec])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Position;
    use serde_json::json;

    fn rec(value: &str) -> SinkRecord {
        SinkRecord {
            key: Some(b"k1".to_vec()),
            value: Some(value.as_bytes().to_vec()),
            headers: vec![("type".into(), b"order.paid".to_vec())],
            op: None,
            before: None,
            after: None,
            ts_ms: 10,
            idem_key: "s/1/0/0/0".into(),
            pos: Position::Stream {
                stream: 1,
                partition: 0,
                offset: 0,
                epoch: 0,
            },
        }
    }

    #[test]
    fn stream_conditions() {
        let r = rec(r#"{"type": "order.paid", "amount": 12.5, "items": [{"sku": "a"}]}"#);
        for (c, want) in [
            ("value->>'$.type' = 'order.paid'", true),
            ("value->>'$.type' = 'order.new'", false),
            ("value->>'$.amount' > 10 AND key = 'k1'", true),
            ("value->>'$.items[0].sku' IN ('a', 'b')", true),
            ("value->>'$.missing' = 'x'", false),
            ("value->>'$.missing' IS NULL", true),
            ("NOT (value->>'$.amount' BETWEEN 1 AND 5)", true),
            ("headers->>'$.type' LIKE 'order.%'", true),
            ("ts >= 10 OR value->>'$.missing' = 1", true),
        ] {
            assert_eq!(parse(c).unwrap().matches(&r), want, "{c}");
        }
        assert!(parse("value IN (SELECT 1)").is_err());
        assert!(parse("f(x) = 1").is_err());
    }

    #[test]
    fn change_conditions_and_projection() {
        let mut r = rec("");
        r.op = Some(ChangeOp::Update);
        r.after = Some(json!({"id": 7, "status": "paid", "card_token": "x"}));
        r.before = Some(json!({"id": 7, "status": "new", "card_token": "x"}));
        assert!(
            parse("_op IN ('c','u') AND status = 'paid'")
                .unwrap()
                .matches(&r)
        );
        r.op = Some(ChangeOp::Delete);
        assert!(parse("status = 'new'").unwrap().matches(&r));
        let p = SinkPrepare {
            condition: None,
            projection: Some(Projection::Except {
                columns: vec!["card_token".into()],
            }),
            transform: None,
        };
        let out = p.prepare(r).unwrap();
        assert!(out[0].after.as_ref().unwrap().get("card_token").is_none());
        assert!(out[0].before.as_ref().unwrap().get("id").is_some());
    }
}
