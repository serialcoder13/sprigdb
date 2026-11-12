//! The aggregation subset that runs in the gateway (chapter 31,
//! "Aggregation, search and joins"; build step 11): the expression
//! evaluator, projections, sorting, and the stages after the leading
//! `$match`/`$sort`/`$skip`/`$limit` that the engine pushes into SQL.
//!
//! Stages here: `$match`, `$project`, `$addFields`/`$set`, `$unset`,
//! `$replaceRoot`/`$replaceWith`, `$sort`, `$skip`, `$limit`, `$count`,
//! `$group`, `$unwind`, `$sortByCount`, `$facet`, `$sample` (first n).
//! `$lookup` and `$unionWith` need the database and run in
//! [`crate::engine`]. Each blocking stage is limited by
//! `doc.agg_stage_memory_bytes` (292 past it).

use std::cmp::Ordering;
use std::collections::HashMap;

use bson::{Bson, DateTime, Document, doc};
use sprig_proto_mongo::errors::{MongoError, code};

use crate::filter::{self, ExprEval, as_i64, truthy};
use crate::keyenc::{compare, equal};
use crate::update::{arith, get_path, type_name};

// The expressions added in M1 (12 Nov 2026): dates, strings, arrays and
// sets, arithmetic, types, and 8.1–8.3's additions.
mod expr_array;
mod expr_common;
mod expr_date;
mod expr_decimal;
mod expr_math;
mod expr_string;
mod expr_type;

// M2 (12 Nov 2026): accumulators, window functions and stages.
mod accum;
mod accum_num;
mod stages_bucket;
mod stages_fill;
mod stages_misc;
mod window;
// M5 (12 Nov 2026): the stages over sequences that may be on disk.
mod spilled;
pub use spilled::{run_stage_seq, run_stages_seq};
pub use stages_misc::{GraphSearch, GraphSpec, db_level_error, documents, graph_lookup_in_memory};

fn bad(msg: impl Into<String>) -> MongoError {
    MongoError::new(code::BAD_VALUE, msg)
}

fn loc(code: i32, msg: impl Into<String>) -> MongoError {
    MongoError::new(code, msg)
}

/// Variables of an evaluation: `$$ROOT`, `$$CURRENT`, `$$NOW` and those
/// `$map`, `$filter`, `$let` bind.
#[derive(Debug, Clone)]
pub struct Vars {
    vars: HashMap<String, Bson>,
}

impl Vars {
    /// Variables for evaluating against `root`.
    #[must_use]
    pub fn new(root: &Document, now: DateTime) -> Self {
        let mut vars = HashMap::new();
        vars.insert("ROOT".to_owned(), Bson::Document(root.clone()));
        vars.insert("CURRENT".to_owned(), Bson::Document(root.clone()));
        vars.insert("NOW".to_owned(), Bson::DateTime(now));
        // `$$REMOVE`: a missing value.
        vars.insert("REMOVE".to_owned(), Bson::Undefined);
        Self { vars }
    }

    fn with(&self, name: &str, v: Bson) -> Self {
        let mut n = self.clone();
        n.vars.insert(name.to_owned(), v);
        n
    }
}

/// Values at a field path for expressions: arrays of documents give the
/// array of their values (`"$items.sku"`).
fn field_value(v: &Bson, parts: &[&str]) -> Bson {
    if parts.is_empty() {
        return v.clone();
    }
    match v {
        Bson::Document(d) => match d.get(parts[0]) {
            Some(x) => field_value(x, &parts[1..]),
            None => Bson::Undefined,
        },
        Bson::Array(a) => Bson::Array(
            a.iter()
                .map(|e| field_value(e, parts))
                .filter(|x| !matches!(x, Bson::Undefined))
                .collect(),
        ),
        _ => Bson::Undefined,
    }
}

fn missing(v: &Bson) -> bool {
    matches!(v, Bson::Undefined)
}

fn nullish(v: &Bson) -> bool {
    matches!(v, Bson::Null | Bson::Undefined)
}

fn num_f(v: &Bson) -> Option<f64> {
    match v {
        Bson::Int32(i) => Some(f64::from(*i)),
        #[allow(clippy::cast_precision_loss)]
        Bson::Int64(i) => Some(*i as f64),
        Bson::Double(f) => Some(*f),
        Bson::Decimal128(d) => d.to_string().parse().ok(),
        _ => None,
    }
}

fn args(v: &Bson) -> Vec<&Bson> {
    match v {
        Bson::Array(a) => a.iter().collect(),
        other => vec![other],
    }
}

/// Evaluates an aggregation expression.
///
/// # Errors
/// MongoDB's errors for unknown operators and bad arguments.
#[allow(clippy::too_many_lines)]
pub fn eval(e: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    match e {
        Bson::String(s) if s.starts_with("$$") => {
            let rest = &s[2..];
            let (name, path) = rest.split_once('.').unwrap_or((rest, ""));
            let base = vars
                .vars
                .get(name)
                .cloned()
                .ok_or_else(|| loc(17276, format!("Use of undefined variable: {name}")))?;
            let parts: Vec<&str> = if path.is_empty() {
                vec![]
            } else {
                path.split('.').collect()
            };
            Ok(field_value(&base, &parts))
        }
        Bson::String(s) if s.starts_with('$') => {
            let cur = vars.vars.get("CURRENT").cloned().unwrap_or(Bson::Null);
            let parts: Vec<&str> = s[1..].split('.').collect();
            Ok(field_value(&cur, &parts))
        }
        Bson::Array(a) => Ok(Bson::Array(
            a.iter()
                .map(|x| eval(x, vars).map(|v| if missing(&v) { Bson::Null } else { v }))
                .collect::<Result<_, _>>()?,
        )),
        Bson::Document(d) => {
            let Some((op, arg)) = d.iter().next() else {
                return Ok(Bson::Document(Document::new()));
            };
            if !op.starts_with('$') {
                let mut out = Document::new();
                for (k, v) in d {
                    let x = eval(v, vars)?;
                    if !missing(&x) {
                        out.insert(k.clone(), x);
                    }
                }
                return Ok(Bson::Document(out));
            }
            if d.len() > 1 {
                return Err(loc(
                    15983,
                    format!(
                        "An object representing an expression must have exactly one field: {d}"
                    ),
                ));
            }
            // `{$meta: "vectorSearchScore"}` (chapter 34, R6 round 3).
            if op == "$meta" {
                return Ok(crate::engine::models::meta_value(
                    arg,
                    vars.vars.get("ROOT"),
                ));
            }
            op_eval(op, arg, vars)
        }
        other => Ok(other.clone()),
    }
}

fn evals(arg: &Bson, vars: &Vars) -> Result<Vec<Bson>, MongoError> {
    args(arg).into_iter().map(|a| eval(a, vars)).collect()
}

#[allow(clippy::too_many_lines)]
fn op_eval(op: &str, arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    if let Some(r) = expr_common::dispatch(op, arg, vars) {
        return r;
    }
    let cmp2 = |vars: &Vars| -> Result<Ordering, MongoError> {
        let v = evals(arg, vars)?;
        if v.len() != 2 {
            return Err(loc(
                16020,
                format!("Expression {op} takes exactly 2 arguments"),
            ));
        }
        Ok(compare(&v[0], &v[1]))
    };
    Ok(match op {
        "$literal" => arg.clone(),
        // Server-side JavaScript (M4, decision S110).
        "$function" => crate::serverjs::function(arg, vars)?,
        // A uniform double in [0, 1) (R5 round 4).
        "$rand" => {
            use std::hash::{BuildHasher, Hasher};
            let mut h = std::collections::hash_map::RandomState::new().build_hasher();
            h.write_u128(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_nanos()),
            );
            #[allow(clippy::cast_precision_loss)]
            let u = (h.finish() >> 11) as f64 / (1u64 << 53) as f64;
            Bson::Double(u)
        }
        // Fields by literal name, dots and `$` included (MongoDB 5.0+).
        "$getField" | "$setField" | "$unsetField" => {
            let (field, input, value) = match arg {
                Bson::Document(d) if d.contains_key("field") => (
                    eval(d.get("field").unwrap_or(&Bson::Null), vars)?,
                    match d.get("input") {
                        Some(i) => eval(i, vars)?,
                        None if op == "$getField" => {
                            vars.vars.get("CURRENT").cloned().unwrap_or(Bson::Null)
                        }
                        None => return Err(loc(4_161_102, format!("{op} requires 'input'"))),
                    },
                    d.get("value").map(|v| eval(v, vars)).transpose()?,
                ),
                f if op == "$getField" => (
                    eval(f, vars)?,
                    vars.vars.get("CURRENT").cloned().unwrap_or(Bson::Null),
                    None,
                ),
                _ => return Err(loc(4_161_101, format!("{op} requires an object argument"))),
            };
            let Bson::String(field) = field else {
                return Err(loc(
                    5_654_602,
                    format!("{op} requires 'field' to evaluate to a string"),
                ));
            };
            match (op, input) {
                ("$getField", Bson::Document(d)) => {
                    d.get(&field).cloned().unwrap_or(Bson::Undefined)
                }
                ("$getField", _) => Bson::Undefined,
                (_, Bson::Null | Bson::Undefined) => Bson::Null,
                (_, Bson::Document(mut d)) => {
                    match value {
                        Some(v) if op == "$setField" && !missing(&v) => {
                            d.insert(field, v);
                        }
                        _ => {
                            d.remove(&field);
                        }
                    }
                    Bson::Document(d)
                }
                (_, other) => {
                    return Err(loc(
                        4_161_105,
                        format!(
                            "{op} requires 'input' to evaluate to an object, got {}",
                            type_name(&other)
                        ),
                    ));
                }
            }
        }
        "$add" | "$multiply" => {
            let vs = evals(arg, vars)?;
            let mut acc: Option<Bson> = None;
            let mut date: Option<i64> = None;
            for v in vs {
                if nullish(&v) {
                    return Ok(Bson::Null);
                }
                if let (Bson::DateTime(d), "$add") = (&v, op) {
                    date = Some(d.timestamp_millis());
                    continue;
                }
                if num_f(&v).is_none() {
                    return Err(loc(
                        16554,
                        format!(
                            "{op} only supports numeric or date types, not {}",
                            type_name(&v)
                        ),
                    ));
                }
                acc = Some(match acc {
                    None => v,
                    Some(a) => arith(&a, &v, op == "$multiply")?,
                });
            }
            match date {
                #[allow(clippy::cast_possible_truncation)]
                Some(ms) => Bson::DateTime(DateTime::from_millis(
                    ms + acc.as_ref().and_then(num_f).unwrap_or(0.0) as i64,
                )),
                None => acc.unwrap_or(Bson::Int32(i32::from(op != "$add"))),
            }
        }
        "$subtract" => {
            let v = evals(arg, vars)?;
            if v.len() != 2 {
                return Err(loc(16020, "Expression $subtract takes exactly 2 arguments"));
            }
            if nullish(&v[0]) || nullish(&v[1]) {
                return Ok(Bson::Null);
            }
            match (&v[0], &v[1]) {
                (Bson::DateTime(a), Bson::DateTime(b)) => {
                    Bson::Int64(a.timestamp_millis() - b.timestamp_millis())
                }
                #[allow(clippy::cast_possible_truncation)]
                (Bson::DateTime(a), b) => Bson::DateTime(DateTime::from_millis(
                    a.timestamp_millis() - num_f(b).unwrap_or(0.0) as i64,
                )),
                (a, b) => {
                    let neg = arith(b, &Bson::Int32(-1), true)?;
                    arith(a, &neg, false)?
                }
            }
        }
        "$divide" => {
            let v = evals(arg, vars)?;
            if v.iter().any(nullish) {
                return Ok(Bson::Null);
            }
            let (Some(a), Some(b)) = (v.first().and_then(num_f), v.get(1).and_then(num_f)) else {
                return Err(loc(16609, "$divide only supports numeric types"));
            };
            if b == 0.0 {
                return Err(loc(16608, "can't $divide by zero"));
            }
            Bson::Double(a / b)
        }
        "$mod" => {
            let v = evals(arg, vars)?;
            if v.iter().any(nullish) {
                return Ok(Bson::Null);
            }
            match (v.first().and_then(as_i64), v.get(1).and_then(as_i64)) {
                (Some(_), Some(0)) => return Err(loc(16610, "can't $mod by zero")),
                (Some(a), Some(b))
                    if matches!(v[0], Bson::Int32(_)) && matches!(v[1], Bson::Int32(_)) =>
                {
                    Bson::Int32(i32::try_from(a % b).unwrap_or(0))
                }
                (Some(a), Some(b)) => Bson::Int64(a % b),
                _ => {
                    let (a, b) = (
                        num_f(&v[0]).unwrap_or(f64::NAN),
                        num_f(&v[1]).unwrap_or(f64::NAN),
                    );
                    Bson::Double(a % b)
                }
            }
        }
        "$abs" | "$ceil" | "$floor" | "$round" | "$trunc" | "$sqrt" => {
            let v = evals(arg, vars)?;
            let x = v.first().cloned().unwrap_or(Bson::Null);
            if nullish(&x) {
                return Ok(Bson::Null);
            }
            match (&x, op) {
                (Bson::Int32(i), "$abs") => Bson::Int32(i.abs()),
                (Bson::Int64(i), "$abs") => Bson::Int64(i.abs()),
                (Bson::Int32(_) | Bson::Int64(_), "$ceil" | "$floor" | "$round" | "$trunc") => x,
                _ => {
                    let f = num_f(&x)
                        .ok_or_else(|| loc(28765, format!("{op} only supports numeric types")))?;
                    Bson::Double(match op {
                        "$abs" => f.abs(),
                        "$ceil" => f.ceil(),
                        "$floor" => f.floor(),
                        "$trunc" => f.trunc(),
                        "$sqrt" => f.sqrt(),
                        _ => {
                            let places = v.get(1).and_then(as_i64).unwrap_or(0);
                            let m = 10f64.powi(i32::try_from(places).unwrap_or(0));
                            (f * m).round_ties_even() / m
                        }
                    })
                }
            }
        }
        "$concat" => {
            let mut s = String::new();
            for v in evals(arg, vars)? {
                match v {
                    Bson::String(x) => s.push_str(&x),
                    v if nullish(&v) => return Ok(Bson::Null),
                    v => {
                        return Err(loc(
                            16702,
                            format!("$concat only supports strings, not {}", type_name(&v)),
                        ));
                    }
                }
            }
            Bson::String(s)
        }
        "$toUpper" | "$toLower" => {
            let v = evals(arg, vars)?;
            let s = match v.first() {
                Some(Bson::String(s)) => s.clone(),
                Some(x) if nullish(x) => String::new(),
                Some(x) => x.to_string(),
                None => String::new(),
            };
            Bson::String(if op == "$toUpper" {
                s.to_uppercase()
            } else {
                s.to_lowercase()
            })
        }
        "$strLenCP" | "$strLenBytes" => match evals(arg, vars)?.first() {
            Some(Bson::String(s)) => Bson::Int32(
                i32::try_from(if op == "$strLenCP" {
                    s.chars().count()
                } else {
                    s.len()
                })
                .unwrap_or(i32::MAX),
            ),
            _ => return Err(loc(34471, format!("{op} requires a string argument"))),
        },
        "$substr" | "$substrCP" | "$substrBytes" => {
            let v = evals(arg, vars)?;
            let s = match v.first() {
                Some(Bson::String(s)) => s.clone(),
                _ => String::new(),
            };
            let start = usize::try_from(v.get(1).and_then(as_i64).unwrap_or(0).max(0)).unwrap_or(0);
            let len = v.get(2).and_then(as_i64).unwrap_or(-1);
            let chars: Vec<char> = s.chars().collect();
            let end = if len < 0 {
                chars.len()
            } else {
                (start + usize::try_from(len).unwrap_or(0)).min(chars.len())
            };
            Bson::String(
                chars
                    .get(start.min(chars.len())..end.max(start.min(chars.len())))
                    .map(|c| c.iter().collect())
                    .unwrap_or_default(),
            )
        }
        "$split" => {
            let v = evals(arg, vars)?;
            match (v.first(), v.get(1)) {
                (Some(Bson::String(s)), Some(Bson::String(d))) => Bson::Array(
                    s.split(d.as_str())
                        .map(|x| Bson::String(x.to_owned()))
                        .collect(),
                ),
                (Some(x), _) if nullish(x) => Bson::Null,
                _ => return Err(loc(40085, "$split requires strings")),
            }
        }
        "$eq" => Bson::Boolean(cmp2(vars)? == Ordering::Equal),
        "$ne" => Bson::Boolean(cmp2(vars)? != Ordering::Equal),
        "$gt" => Bson::Boolean(cmp2(vars)? == Ordering::Greater),
        "$gte" => Bson::Boolean(cmp2(vars)? != Ordering::Less),
        "$lt" => Bson::Boolean(cmp2(vars)? == Ordering::Less),
        "$lte" => Bson::Boolean(cmp2(vars)? != Ordering::Greater),
        "$cmp" => Bson::Int32(match cmp2(vars)? {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        }),
        "$and" => {
            for a in args(arg) {
                if !value_truthy(&eval(a, vars)?) {
                    return Ok(Bson::Boolean(false));
                }
            }
            Bson::Boolean(true)
        }
        "$or" => {
            for a in args(arg) {
                if value_truthy(&eval(a, vars)?) {
                    return Ok(Bson::Boolean(true));
                }
            }
            Bson::Boolean(false)
        }
        "$not" => Bson::Boolean(!value_truthy(
            &evals(arg, vars)?.first().cloned().unwrap_or(Bson::Null),
        )),
        "$cond" => {
            let (c, t, f) = match arg {
                Bson::Document(d) => (
                    d.get("if").cloned().unwrap_or(Bson::Null),
                    d.get("then").cloned().unwrap_or(Bson::Null),
                    d.get("else").cloned().unwrap_or(Bson::Null),
                ),
                Bson::Array(a) if a.len() == 3 => (a[0].clone(), a[1].clone(), a[2].clone()),
                _ => return Err(loc(16020, "Expression $cond takes exactly 3 arguments")),
            };
            if value_truthy(&eval(&c, vars)?) {
                eval(&t, vars)?
            } else {
                eval(&f, vars)?
            }
        }
        "$switch" => {
            let d = arg
                .as_document()
                .ok_or_else(|| bad("$switch requires an object"))?;
            for b in d
                .get_array("branches")
                .map_err(|_| bad("$switch requires branches"))?
            {
                let b = b.as_document().ok_or_else(|| bad("bad branch"))?;
                if value_truthy(&eval(b.get("case").unwrap_or(&Bson::Null), vars)?) {
                    return eval(b.get("then").unwrap_or(&Bson::Null), vars);
                }
            }
            match d.get("default") {
                Some(x) => eval(x, vars)?,
                None => {
                    return Err(loc(
                        40066,
                        "$switch could not find a matching branch for an input, and no default was specified.",
                    ));
                }
            }
        }
        "$ifNull" => {
            let a = args(arg);
            for (i, x) in a.iter().enumerate() {
                let v = eval(x, vars)?;
                if !nullish(&v) || i == a.len() - 1 {
                    return Ok(if missing(&v) { Bson::Null } else { v });
                }
            }
            Bson::Null
        }
        "$in" => {
            let v = evals(arg, vars)?;
            let Some(Bson::Array(list)) = v.get(1) else {
                return Err(loc(40081, "$in requires an array as a second argument"));
            };
            Bson::Boolean(list.iter().any(|x| equal(x, &v[0])))
        }
        "$size" => match evals(arg, vars)?.first() {
            Some(Bson::Array(a)) => Bson::Int32(i32::try_from(a.len()).unwrap_or(i32::MAX)),
            Some(x) => {
                return Err(loc(
                    17124,
                    format!(
                        "The argument to $size must be an array. Type of the argument: {}",
                        if missing(x) { "missing" } else { type_name(x) }
                    ),
                ));
            }
            None => return Err(loc(17124, "The argument to $size must be an array")),
        },
        "$isArray" => Bson::Boolean(matches!(evals(arg, vars)?.first(), Some(Bson::Array(_)))),
        "$arrayElemAt" => {
            let v = evals(arg, vars)?;
            match (v.first(), v.get(1).and_then(as_i64)) {
                (Some(Bson::Array(a)), Some(i)) => {
                    let len = i64::try_from(a.len()).unwrap_or(0);
                    let k = if i < 0 { len + i } else { i };
                    usize::try_from(k)
                        .ok()
                        .and_then(|k| a.get(k))
                        .cloned()
                        .unwrap_or(Bson::Undefined)
                }
                (Some(x), _) if nullish(x) => Bson::Null,
                _ => return Err(loc(28689, "$arrayElemAt's first argument must be an array")),
            }
        }
        "$first" | "$last" => match eval(arg, vars)? {
            Bson::Array(a) => (if op == "$first" { a.first() } else { a.last() })
                .cloned()
                .unwrap_or(Bson::Undefined),
            x if nullish(&x) => Bson::Null,
            x => {
                return Err(loc(
                    28689,
                    format!("{op}'s argument must be an array, but is {}", type_name(&x)),
                ));
            }
        },
        "$concatArrays" => {
            let mut out = Vec::new();
            for v in evals(arg, vars)? {
                match v {
                    Bson::Array(a) => out.extend(a),
                    v if nullish(&v) => return Ok(Bson::Null),
                    v => {
                        return Err(loc(
                            28664,
                            format!("$concatArrays only supports arrays, not {}", type_name(&v)),
                        ));
                    }
                }
            }
            Bson::Array(out)
        }
        "$reverseArray" => match eval(arg, vars)? {
            Bson::Array(mut a) => {
                a.reverse();
                Bson::Array(a)
            }
            x => x,
        },
        "$sum" | "$avg" | "$max" | "$min" => {
            let vs = evals(arg, vars)?;
            let flat: Vec<Bson> = if vs.len() == 1 {
                match &vs[0] {
                    Bson::Array(a) => a.clone(),
                    x => vec![x.clone()],
                }
            } else {
                vs
            };
            let mut acc = Acc::new(op);
            for v in flat {
                acc.add(v)?;
            }
            acc.result()
        }
        "$type" => {
            let v = evals(arg, vars)?
                .first()
                .cloned()
                .unwrap_or(Bson::Undefined);
            Bson::String(if missing(&v) {
                "missing".into()
            } else {
                type_name(&v).into()
            })
        }
        "$toString" => match evals(arg, vars)?.first() {
            Some(Bson::String(s)) => Bson::String(s.clone()),
            Some(Bson::ObjectId(o)) => Bson::String(o.to_hex()),
            Some(Bson::DateTime(d)) => {
                Bson::String(crate::json::iso_from_ms(d.timestamp_millis()).unwrap_or_default())
            }
            Some(Bson::Double(f)) => Bson::String(format!("{f}")),
            Some(x) if nullish(x) => Bson::Null,
            Some(x) => Bson::String(x.to_string()),
            None => Bson::Null,
        },
        "$toInt" | "$toLong" | "$toDouble" | "$toDecimal" | "$toBool" => {
            let v = evals(arg, vars)?.first().cloned().unwrap_or(Bson::Null);
            if nullish(&v) {
                return Ok(Bson::Null);
            }
            let f = match &v {
                Bson::String(s) => s
                    .trim()
                    .parse::<f64>()
                    .map_err(|_| loc(241, format!("Failed to parse number '{s}' in $convert")))?,
                Bson::Boolean(b) => f64::from(u8::from(*b)),
                x => num_f(x).ok_or_else(|| loc(241, "Unsupported conversion in $convert"))?,
            };
            #[allow(clippy::cast_possible_truncation)]
            match op {
                "$toInt" => Bson::Int32(f as i32),
                "$toLong" => Bson::Int64(f as i64),
                "$toDouble" => Bson::Double(f),
                "$toBool" => Bson::Boolean(f != 0.0 || matches!(v, Bson::String(_))),
                _ => match &v {
                    Bson::String(s) => s.parse().map(Bson::Decimal128).unwrap_or(Bson::Double(f)),
                    _ => format!("{f}")
                        .parse()
                        .map(Bson::Decimal128)
                        .unwrap_or(Bson::Double(f)),
                },
            }
        }
        "$year" | "$month" | "$dayOfMonth" | "$hour" | "$minute" | "$second" | "$millisecond"
        | "$dayOfWeek" | "$dayOfYear" => {
            let v = match arg {
                Bson::Document(d) if d.contains_key("date") => {
                    eval(d.get("date").unwrap_or(&Bson::Null), vars)?
                }
                other => evals(other, vars)?.first().cloned().unwrap_or(Bson::Null),
            };
            let Bson::DateTime(d) = v else {
                if nullish(&v) {
                    return Ok(Bson::Null);
                }
                return Err(loc(
                    16006,
                    format!("can't convert from BSON type {} to Date", type_name(&v)),
                ));
            };
            date_part(op, d.timestamp_millis())
        }
        "$dateToString" => {
            let d = arg
                .as_document()
                .ok_or_else(|| bad("$dateToString needs an object"))?;
            let v = eval(d.get("date").unwrap_or(&Bson::Null), vars)?;
            let Bson::DateTime(dt) = v else {
                return Ok(Bson::Null);
            };
            let fmt = d.get_str("format").unwrap_or("%Y-%m-%dT%H:%M:%S.%LZ");
            Bson::String(date_format(fmt, dt.timestamp_millis()))
        }
        "$mergeObjects" => {
            let mut out = Document::new();
            for v in evals(arg, vars)? {
                let list = match v {
                    Bson::Array(a) => a,
                    x => vec![x],
                };
                for x in list {
                    if let Bson::Document(d) = x {
                        for (k, y) in d {
                            out.insert(k, y);
                        }
                    }
                }
            }
            Bson::Document(out)
        }
        "$objectToArray" => match eval(arg, vars)? {
            Bson::Document(d) => Bson::Array(
                d.into_iter()
                    .map(|(k, v)| Bson::Document(doc! {"k": k, "v": v}))
                    .collect(),
            ),
            x if nullish(&x) => Bson::Null,
            _ => return Err(loc(40390, "$objectToArray requires a document input")),
        },
        "$arrayToObject" => match evals(arg, vars)?.first() {
            Some(Bson::Array(a)) => {
                let mut out = Document::new();
                for e in a {
                    match e {
                        Bson::Document(d) => {
                            if let (Ok(k), Some(v)) = (d.get_str("k"), d.get("v")) {
                                out.insert(k, v.clone());
                            }
                        }
                        Bson::Array(p) if p.len() == 2 => {
                            if let Bson::String(k) = &p[0] {
                                out.insert(k.clone(), p[1].clone());
                            }
                        }
                        _ => {}
                    }
                }
                Bson::Document(out)
            }
            _ => Bson::Null,
        },
        "$map" | "$filter" => {
            let d = arg
                .as_document()
                .ok_or_else(|| bad(format!("{op} needs an object")))?;
            let input = eval(d.get("input").unwrap_or(&Bson::Null), vars)?;
            let name = d.get_str("as").unwrap_or("this");
            let Bson::Array(a) = input else {
                return Ok(Bson::Null);
            };
            let mut out = Vec::new();
            for e in a {
                let v2 = vars.with(name, e.clone());
                if op == "$map" {
                    out.push(eval(d.get("in").unwrap_or(&Bson::Null), &v2)?);
                } else if value_truthy(&eval(d.get("cond").unwrap_or(&Bson::Null), &v2)?) {
                    out.push(e);
                }
            }
            Bson::Array(out)
        }
        "$reduce" => {
            let d = arg
                .as_document()
                .ok_or_else(|| bad("$reduce needs an object"))?;
            let Bson::Array(a) = eval(d.get("input").unwrap_or(&Bson::Null), vars)? else {
                return Ok(Bson::Null);
            };
            let mut acc = eval(d.get("initialValue").unwrap_or(&Bson::Null), vars)?;
            for e in a {
                let v2 = vars.with("this", e).with("value", acc);
                acc = eval(d.get("in").unwrap_or(&Bson::Null), &v2)?;
            }
            acc
        }
        "$let" => {
            let d = arg
                .as_document()
                .ok_or_else(|| bad("$let needs an object"))?;
            let mut v2 = vars.clone();
            if let Ok(vs) = d.get_document("vars") {
                for (k, x) in vs {
                    let val = eval(x, vars)?;
                    v2 = v2.with(k, val);
                }
            }
            eval(d.get("in").unwrap_or(&Bson::Null), &v2)?
        }
        "$setUnion" | "$setIntersection" => {
            let vs = evals(arg, vars)?;
            let mut out: Vec<Bson> = Vec::new();
            for (i, v) in vs.iter().enumerate() {
                let Bson::Array(a) = v else {
                    return Ok(Bson::Null);
                };
                if op == "$setUnion" || i == 0 {
                    for x in a {
                        if !out.iter().any(|y| equal(x, y)) {
                            out.push(x.clone());
                        }
                    }
                } else {
                    out.retain(|y| a.iter().any(|x| equal(x, y)));
                }
            }
            Bson::Array(out)
        }
        "$regexMatch" => {
            let d = arg
                .as_document()
                .ok_or_else(|| bad("$regexMatch needs an object"))?;
            let input = eval(d.get("input").unwrap_or(&Bson::Null), vars)?;
            let (p, o) = match d.get("regex") {
                Some(Bson::String(s)) => (s.clone(), d.get_str("options").unwrap_or("").to_owned()),
                Some(Bson::RegularExpression(r)) => (r.pattern.clone(), r.options.clone()),
                _ => return Err(bad("$regexMatch needs a regex")),
            };
            let re = filter::Re::new(&p, &o)?;
            Bson::Boolean(matches!(input, Bson::String(ref s) if re.re.is_match(s)))
        }
        _ => {
            return Err(loc(168, format!("Unrecognized expression '{op}'")));
        }
    })
}

fn date_part(op: &str, ms: i64) -> Bson {
    let days = ms.div_euclid(86_400_000);
    let rem = ms.rem_euclid(86_400_000);
    let iso = crate::json::iso_from_ms(ms).unwrap_or_default();
    let num =
        |r: std::ops::Range<usize>| iso.get(r).and_then(|s| s.parse::<i32>().ok()).unwrap_or(0);
    #[allow(clippy::cast_possible_truncation)]
    Bson::Int32(match op {
        "$year" => num(0..4),
        "$month" => num(5..7),
        "$dayOfMonth" => num(8..10),
        "$hour" => (rem / 3_600_000) as i32,
        "$minute" => (rem % 3_600_000 / 60_000) as i32,
        "$second" => (rem % 60_000 / 1000) as i32,
        "$millisecond" => (rem % 1000) as i32,
        "$dayOfWeek" => ((days + 4).rem_euclid(7) + 1) as i32,
        _ => {
            let y = num(0..4);
            let jan1 = crate::json::ms_from_iso(&format!("{y:04}-01-01")).unwrap_or(0);
            ((ms - jan1) / 86_400_000 + 1) as i32
        }
    })
}

fn date_format(fmt: &str, ms: i64) -> String {
    let iso = crate::json::iso_from_ms(ms).unwrap_or_default();
    let g = |r: std::ops::Range<usize>| iso.get(r).unwrap_or("").to_owned();
    let mut out = String::new();
    let mut it = fmt.chars();
    while let Some(c) = it.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        match it.next() {
            Some('Y') => out.push_str(&g(0..4)),
            Some('m') => out.push_str(&g(5..7)),
            Some('d') => out.push_str(&g(8..10)),
            Some('H') => out.push_str(&g(11..13)),
            Some('M') => out.push_str(&g(14..16)),
            Some('S') => out.push_str(&g(17..19)),
            Some('L') => out.push_str(&g(20..23)),
            Some(x) => {
                out.push('%');
                out.push(x);
            }
            None => out.push('%'),
        }
    }
    out
}

/// MongoDB truthiness of an expression result.
#[must_use]
pub fn value_truthy(v: &Bson) -> bool {
    match v {
        Bson::Boolean(b) => *b,
        Bson::Null | Bson::Undefined => false,
        Bson::Int32(i) => *i != 0,
        Bson::Int64(i) => *i != 0,
        Bson::Double(f) => *f != 0.0,
        Bson::Decimal128(_) => num_f(v).is_some_and(|f| f != 0.0),
        _ => true,
    }
}

// --- `let` variables (chapter 31, R5 round 4) --------------------------------

/// The values of a command's `let` option: each expression evaluated once
/// (against an empty document, with `$$NOW`), as MongoDB does.
///
/// # Errors
/// 17276 and the expressions' own errors; 9 for a name MongoDB refuses.
pub fn let_values(lets: &Document, now: DateTime) -> Result<Document, MongoError> {
    let empty = Document::new();
    let vars = Vars::new(&empty, now);
    let mut out = Document::new();
    for (k, e) in lets {
        if k.is_empty()
            || !k.starts_with(|c: char| c.is_ascii_lowercase() || !c.is_ascii())
            || !k.chars().all(|c| c.is_alphanumeric() || c == '_')
        {
            return Err(MongoError::new(
                code::FAILED_TO_PARSE,
                format!("'{k}' starts with an invalid character for a user variable name"),
            ));
        }
        out.insert(k.clone(), eval(e, &vars)?);
    }
    Ok(out)
}

/// `expr` with every `$$name` (and `$$name.path`) of `lets` replaced by
/// its value as a `$literal`: the command's `let` variables bound into an
/// aggregation expression.
#[must_use]
pub fn bind_expr(expr: &Bson, lets: &Document) -> Bson {
    match expr {
        Bson::String(s) if s.starts_with("$$") => {
            let rest = &s[2..];
            let (name, path) = rest.split_once('.').unwrap_or((rest, ""));
            match lets.get(name) {
                Some(v) => {
                    let parts: Vec<&str> = if path.is_empty() {
                        Vec::new()
                    } else {
                        path.split('.').collect()
                    };
                    Bson::Document(doc! {"$literal": field_value(v, &parts)})
                }
                None => expr.clone(),
            }
        }
        Bson::Array(a) => Bson::Array(a.iter().map(|x| bind_expr(x, lets)).collect()),
        Bson::Document(d) => {
            // `$literal` stays as written.
            if d.len() == 1 && d.contains_key("$literal") {
                return expr.clone();
            }
            Bson::Document(
                d.iter()
                    .map(|(k, v)| (k.clone(), bind_expr(v, lets)))
                    .collect(),
            )
        }
        other => other.clone(),
    }
}

/// A query filter with `lets` bound inside its `$expr` clauses (the only
/// place a filter reads variables).
#[must_use]
pub fn bind_filter(filter: &Document, lets: &Document) -> Document {
    filter
        .iter()
        .map(|(k, v)| {
            let v = match (k.as_str(), v) {
                ("$expr", e) => bind_expr(e, lets),
                ("$and" | "$or" | "$nor", Bson::Array(a)) => Bson::Array(
                    a.iter()
                        .map(|x| match x {
                            Bson::Document(d) => Bson::Document(bind_filter(d, lets)),
                            o => o.clone(),
                        })
                        .collect(),
                ),
                _ => v.clone(),
            };
            (k.clone(), v)
        })
        .collect()
}

/// An aggregation pipeline (or a pipeline update) with `lets` bound:
/// `$match` stages in their `$expr`, `$lookup` sub-pipelines and their
/// own `let`, every other stage throughout.
#[must_use]
pub fn bind_pipeline(stages: &[Document], lets: &Document) -> Vec<Document> {
    if lets.is_empty() {
        return stages.to_vec();
    }
    stages
        .iter()
        .map(|st| {
            st.iter()
                .map(|(name, arg)| {
                    let v = match (name.as_str(), arg) {
                        ("$match", Bson::Document(f)) => Bson::Document(bind_filter(f, lets)),
                        ("$lookup" | "$unionWith", Bson::Document(l)) => {
                            let mut l = l.clone();
                            if let Ok(p) = l.get_array("pipeline") {
                                let p: Vec<Document> =
                                    p.iter().filter_map(|x| x.as_document().cloned()).collect();
                                l.insert(
                                    "pipeline",
                                    bind_pipeline(&p, lets)
                                        .into_iter()
                                        .map(Bson::Document)
                                        .collect::<Vec<_>>(),
                                );
                            }
                            if let Some(v) = l.get("let").map(|x| bind_expr(x, lets)) {
                                l.insert("let", v);
                            }
                            Bson::Document(l)
                        }
                        // Output stages name collections, not values.
                        ("$out" | "$merge", a) => a.clone(),
                        (_, a) => bind_expr(a, lets),
                    };
                    (name.clone(), v)
                })
                .collect()
        })
        .collect()
}

/// `$expr` support for the matcher.
#[derive(Debug, Clone, Copy)]
pub struct AggExpr {
    /// `$$NOW`.
    pub now: DateTime,
}

impl ExprEval for AggExpr {
    fn truthy(&self, expr: &Bson, doc: &Document) -> Result<bool, MongoError> {
        Ok(value_truthy(&eval(expr, &Vars::new(doc, self.now))?))
    }
}

// --- accumulators --------------------------------------------------------

/// A `$group` accumulator, or the array form of `$sum` and friends.
#[derive(Debug, Clone)]
pub struct Acc {
    op: String,
    sum: Option<Bson>,
    count: i64,
    best: Option<Bson>,
    list: Vec<Bson>,
    any_num: bool,
    /// `$addToSet`: the equality keys already in `list` (linear, not
    /// quadratic, in the group's size).
    seen: std::collections::HashSet<Vec<u8>>,
}

impl Acc {
    /// A new accumulator for `op` (`$sum`, `$avg`, ...).
    #[must_use]
    pub fn new(op: &str) -> Self {
        Self {
            op: op.to_owned(),
            sum: None,
            count: 0,
            best: None,
            list: Vec::new(),
            any_num: false,
            seen: std::collections::HashSet::new(),
        }
    }

    /// Adds one input value.
    ///
    /// # Errors
    /// Arithmetic overflow.
    pub fn add(&mut self, v: Bson) -> Result<(), MongoError> {
        match self.op.as_str() {
            "$sum" | "$avg" => {
                if num_f(&v).is_some() {
                    self.any_num = true;
                    self.count += 1;
                    self.sum = Some(match self.sum.take() {
                        None => v,
                        Some(s) => match arith(&s, &v, false) {
                            Ok(x) => x,
                            Err(_) => {
                                Bson::Double(num_f(&s).unwrap_or(0.0) + num_f(&v).unwrap_or(0.0))
                            }
                        },
                    });
                }
            }
            "$min" | "$max" => {
                if nullish(&v) {
                    return Ok(());
                }
                let replace = match &self.best {
                    None => true,
                    Some(b) => {
                        let o = compare(&v, b);
                        if self.op == "$min" {
                            o == Ordering::Less
                        } else {
                            o == Ordering::Greater
                        }
                    }
                };
                if replace {
                    self.best = Some(v);
                }
            }
            "$first" => {
                if self.count == 0 {
                    self.best = Some(if missing(&v) { Bson::Null } else { v });
                }
                self.count += 1;
            }
            "$last" => self.best = Some(if missing(&v) { Bson::Null } else { v }),
            "$push" => {
                if !missing(&v) {
                    self.list.push(v);
                }
            }
            "$addToSet" => {
                if !missing(&v) && self.seen.insert(crate::keyenc::eq_key(&v)) {
                    self.list.push(v);
                }
            }
            "$count" => self.count += 1,
            _ => {}
        }
        Ok(())
    }

    /// The final value.
    #[must_use]
    pub fn result(&self) -> Bson {
        match self.op.as_str() {
            "$sum" => self.sum.clone().unwrap_or(Bson::Int32(0)),
            #[allow(clippy::cast_precision_loss)]
            "$avg" => {
                if self.any_num {
                    Bson::Double(
                        self.sum.as_ref().and_then(num_f).unwrap_or(0.0) / self.count as f64,
                    )
                } else {
                    Bson::Null
                }
            }
            "$min" | "$max" | "$first" | "$last" => self.best.clone().unwrap_or(Bson::Null),
            "$push" | "$addToSet" => Bson::Array(self.list.clone()),
            "$count" => i32::try_from(self.count).map_or(Bson::Int64(self.count), Bson::Int32),
            _ => Bson::Null,
        }
    }
}

// --- projection and sort ---------------------------------------------------

/// A projection of `find` or `$project`.
#[derive(Debug, Clone)]
pub struct Projection {
    spec: Document,
    inclusion: bool,
    id: bool,
}

fn proj_flag(v: &Bson) -> Option<bool> {
    match v {
        Bson::Boolean(b) => Some(*b),
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => Some(truthy(v)),
        _ => None,
    }
}

impl Projection {
    /// Parses a projection.
    ///
    /// # Errors
    /// 31254 / 31253 for mixing inclusion and exclusion.
    pub fn parse(spec: &Document) -> Result<Self, MongoError> {
        let mut inclusion: Option<bool> = None;
        let mut id = true;
        for (k, v) in spec {
            if k == "_id"
                && let Some(f) = proj_flag(v)
            {
                id = f;
                continue;
            }
            let this = match proj_flag(v) {
                Some(f) => f,
                None => match v {
                    Bson::Document(d)
                        if d.contains_key("$slice") || d.contains_key("$elemMatch") =>
                    {
                        continue;
                    }
                    _ => true,
                },
            };
            match inclusion {
                None => inclusion = Some(this),
                Some(i) if i != this => {
                    return Err(loc(
                        if i { 31253 } else { 31254 },
                        format!(
                            "Cannot do {} on field {k} in {} projection",
                            if this { "inclusion" } else { "exclusion" },
                            if i { "inclusion" } else { "exclusion" }
                        ),
                    ));
                }
                _ => {}
            }
        }
        // `{_id: 1}` alone is an inclusion projection; `{_id: 0}` alone an
        // exclusion one.
        let only_id = spec.contains_key("_id") && id;
        Ok(Self {
            spec: spec.clone(),
            inclusion: inclusion.unwrap_or(only_id),
            id,
        })
    }

    /// Applies the projection.
    ///
    /// # Errors
    /// The expression evaluator's.
    pub fn apply(&self, doc: &Document, now: DateTime) -> Result<Document, MongoError> {
        let vars = Vars::new(doc, now);
        let mut out = if self.inclusion {
            let mut o = Document::new();
            if self.id
                && let Some(id) = doc.get("_id")
            {
                o.insert("_id", id.clone());
            }
            for (k, v) in &self.spec {
                if k == "_id" && proj_flag(v).is_some() {
                    continue;
                }
                match proj_flag(v) {
                    Some(true) => include_path(doc, &mut o, k),
                    Some(false) => {}
                    None => self.special(doc, &mut o, k, v, &vars)?,
                }
            }
            o
        } else {
            let mut o = doc.clone();
            if !self.id {
                o.remove("_id");
            }
            for (k, v) in &self.spec {
                match proj_flag(v) {
                    Some(false) if k != "_id" => exclude_path(&mut o, k),
                    None => self.special(doc, &mut o, k, v, &vars)?,
                    _ => {}
                }
            }
            o
        };
        if !self.id {
            out.remove("_id");
        }
        if self.inclusion {
            out = reorder(&out, doc);
        }
        // MongoDB applies `$elemMatch` projections after the rest, so
        // their fields come last.
        for (k, v) in &self.spec {
            if matches!(v, Bson::Document(d) if d.contains_key("$elemMatch"))
                && let Some(x) = out.remove(k)
            {
                out.insert(k.clone(), x);
            }
        }
        Ok(out)
    }

    fn special(
        &self,
        doc: &Document,
        out: &mut Document,
        k: &str,
        v: &Bson,
        vars: &Vars,
    ) -> Result<(), MongoError> {
        if let Bson::Document(d) = v {
            if let Some(s) = d.get("$slice") {
                if let Some(Bson::Array(a)) = get_path(doc, k) {
                    let sliced = slice(a, s);
                    crate::update::set_path(out, k, Bson::Array(sliced))?;
                } else if self.inclusion
                    && let Some(x) = get_path(doc, k)
                {
                    crate::update::set_path(out, k, x.clone())?;
                }
                return Ok(());
            }
            if let Some(Bson::Document(cond)) = d.get("$elemMatch") {
                let p = filter::parse(cond)?;
                if let Some(Bson::Array(a)) = doc.get(k) {
                    if let Some(e) = a
                        .iter()
                        .find(|e| matches!(e, Bson::Document(x) if p.matches(x)))
                    {
                        out.insert(k, Bson::Array(vec![e.clone()]));
                    } else {
                        out.remove(k);
                    }
                }
                return Ok(());
            }
        }
        let x = eval(v, vars)?;
        if !missing(&x) {
            crate::update::set_path(out, k, x)?;
        }
        Ok(())
    }
}

/// `out`'s fields in `src`'s order (MongoDB keeps the document's order
/// in inclusion projections); fields `src` lacks (computed) come last.
fn reorder(out: &Document, src: &Document) -> Document {
    let mut d = Document::new();
    for (k, sv) in src {
        if let Some(v) = out.get(k) {
            d.insert(k.clone(), reorder_val(v, sv));
        }
    }
    for (k, v) in out {
        if !d.contains_key(k) {
            d.insert(k.clone(), v.clone());
        }
    }
    d
}

fn reorder_val(v: &Bson, src: &Bson) -> Bson {
    match (v, src) {
        (Bson::Document(o), Bson::Document(s)) => Bson::Document(reorder(o, s)),
        (Bson::Array(o), Bson::Array(s)) if o.len() == s.len() => {
            Bson::Array(o.iter().zip(s).map(|(a, b)| reorder_val(a, b)).collect())
        }
        _ => v.clone(),
    }
}

fn slice(a: &[Bson], s: &Bson) -> Vec<Bson> {
    let len = i64::try_from(a.len()).unwrap_or(0);
    let (skip, n) = match s {
        Bson::Array(p) if p.len() == 2 => (as_i64(&p[0]).unwrap_or(0), as_i64(&p[1]).unwrap_or(0)),
        other => {
            let n = as_i64(other).unwrap_or(0);
            if n >= 0 { (0, n) } else { (len + n, -n) }
        }
    };
    let start = if skip < 0 {
        (len + skip).max(0)
    } else {
        skip.min(len)
    };
    let end = (start + n.max(0)).min(len);
    a[usize::try_from(start).unwrap_or(0)..usize::try_from(end).unwrap_or(0)].to_vec()
}

fn include_path(src: &Document, out: &mut Document, path: &str) {
    let (head, rest) = path
        .split_once('.')
        .map_or((path, None), |(h, r)| (h, Some(r)));
    let Some(v) = src.get(head) else { return };
    match rest {
        None => {
            out.insert(head, v.clone());
        }
        Some(r) => {
            let sub = match v {
                Bson::Document(d) => {
                    let mut o = match out.get(head) {
                        Some(Bson::Document(x)) => x.clone(),
                        _ => Document::new(),
                    };
                    include_path(d, &mut o, r);
                    Bson::Document(o)
                }
                Bson::Array(a) => {
                    let prev = match out.get(head) {
                        Some(Bson::Array(x)) => x.clone(),
                        _ => Vec::new(),
                    };
                    Bson::Array(
                        a.iter()
                            .enumerate()
                            .filter_map(|(i, e)| match e {
                                Bson::Document(d) => {
                                    let mut o = match prev.get(i) {
                                        Some(Bson::Document(x)) => x.clone(),
                                        _ => Document::new(),
                                    };
                                    include_path(d, &mut o, r);
                                    Some(Bson::Document(o))
                                }
                                _ => None,
                            })
                            .collect(),
                    )
                }
                _ => return,
            };
            out.insert(head, sub);
        }
    }
}

fn exclude_path(d: &mut Document, path: &str) {
    let (head, rest) = path
        .split_once('.')
        .map_or((path, None), |(h, r)| (h, Some(r)));
    match rest {
        None => {
            d.remove(head);
        }
        Some(r) => match d.get_mut(head) {
            Some(Bson::Document(x)) => exclude_path(x, r),
            Some(Bson::Array(a)) => {
                for e in a {
                    if let Bson::Document(x) = e {
                        exclude_path(x, r);
                    }
                }
            }
            _ => {}
        },
    }
}

/// A sort specification: `(path, direction)`.
pub type SortSpec = Vec<(String, i32)>;

/// Parses `{a: 1, b: -1}`.
///
/// # Errors
/// 15974 for a bad direction.
pub fn parse_sort(d: &Document) -> Result<SortSpec, MongoError> {
    d.iter()
        .map(|(k, v)| match as_i64(v) {
            Some(1) => Ok((k.clone(), 1)),
            Some(-1) => Ok((k.clone(), -1)),
            _ => match v {
                // A `$meta` key sorts by the score the search stage left
                // (chapter 39), whatever the key's name.
                Bson::Document(m) if m.contains_key("$meta") => {
                    Ok((crate::engine::models::SCORE_FIELD.to_owned(), -1))
                }
                _ => Err(loc(
                    15975,
                    "$sort key ordering must be 1 (for ascending) or -1 (for descending)",
                )),
            },
        })
        .collect()
}

/// The sort key of `doc` at `path`: arrays give their smallest element
/// ascending and largest descending; missing is null.
fn sort_key(doc: &Document, path: &str, dir: i32) -> Bson {
    let vs = filter::values_at(doc, path);
    let mut cands: Vec<&Bson> = Vec::new();
    for v in &vs.leaves {
        match v {
            Bson::Array(a) if !a.is_empty() => cands.extend(a.iter()),
            // An empty array sorts before null and missing, as in MongoDB.
            Bson::Array(_) => cands.push(&Bson::Undefined),
            _ => cands.push(*v),
        }
    }
    let pick = if dir > 0 {
        cands.into_iter().min_by(|a, b| compare(a, b))
    } else {
        cands.into_iter().max_by(|a, b| compare(a, b))
    };
    pick.cloned().unwrap_or(Bson::Null)
}

/// Sort keys compare as values do, except that `Undefined` (an empty
/// array's key) sorts before null.
fn sort_compare(a: &Bson, b: &Bson) -> Ordering {
    match (a, b) {
        (Bson::Undefined, Bson::Undefined) => Ordering::Equal,
        (Bson::Undefined, x) => {
            if matches!(x, Bson::MinKey) {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (x, Bson::Undefined) => {
            if matches!(x, Bson::MinKey) {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        _ => compare(a, b),
    }
}

/// Compares two documents by `spec`.
#[must_use]
pub fn cmp_docs(a: &Document, b: &Document, spec: &SortSpec) -> Ordering {
    for (p, dir) in spec {
        let o = sort_compare(&sort_key(a, p, *dir), &sort_key(b, p, *dir));
        if o != Ordering::Equal {
            return if *dir < 0 { o.reverse() } else { o };
        }
    }
    Ordering::Equal
}

/// Sorts documents in place, stably.
pub fn sort_docs(docs: &mut [Document], spec: &SortSpec) {
    docs.sort_by(|a, b| cmp_docs(a, b, spec));
}

// --- stages ---------------------------------------------------------------

/// Limits of in-gateway stages.
#[derive(Debug, Clone, Copy)]
pub struct AggLimits {
    /// `doc.agg_stage_memory_bytes`.
    pub stage_bytes: usize,
    /// `$$NOW`.
    pub now: DateTime,
}

fn size_of(docs: &[Document]) -> usize {
    docs.iter()
        .map(|d| bson::to_vec(d).map_or(0, |v| v.len()))
        .sum()
}

fn over(lim: &AggLimits, docs: &[Document], stage: &str) -> Result<(), MongoError> {
    let n = size_of(docs);
    if n > lim.stage_bytes {
        return Err(MongoError::new(
            code::QUERY_EXCEEDED_MEMORY_LIMIT,
            format!(
                "{stage} exceeded memory limit of {} bytes (doc.agg_stage_memory_bytes)",
                lim.stage_bytes
            ),
        ));
    }
    // MongoDB 9.0's per-operation limit (M3).
    crate::opmem::check(n, stage)
}

/// Runs one in-gateway stage over `docs`.
///
/// # Errors
/// The stage's; 40324 for unknown stages.
#[allow(clippy::too_many_lines)]
pub fn run_stage(
    docs: Vec<Document>,
    stage: &Document,
    lim: &AggLimits,
) -> Result<Vec<Document>, MongoError> {
    let Some((name, arg)) = stage.iter().next() else {
        return Err(loc(
            40323,
            "A pipeline stage specification object must contain exactly one field.",
        ));
    };
    if stage.len() != 1 {
        return Err(loc(
            40323,
            "A pipeline stage specification object must contain exactly one field.",
        ));
    }
    let doc_arg = || {
        arg.as_document()
            .ok_or_else(|| loc(15969, format!("{name} specification must be an object")))
    };
    Ok(match name.as_str() {
        "$match" => {
            let p = filter::parse(doc_arg()?)?;
            let cx = AggExpr { now: lim.now };
            docs.into_iter()
                .filter(|d| p.matches_with(d, &cx))
                .collect()
        }
        "$project" => {
            let p = Projection::parse(doc_arg()?)?;
            docs.iter()
                .map(|d| p.apply(d, lim.now))
                .collect::<Result<_, _>>()?
        }
        "$addFields" | "$set" => {
            let spec = doc_arg()?;
            docs.into_iter()
                .map(|mut d| {
                    let vars = Vars::new(&d, lim.now);
                    for (k, e) in spec {
                        let v = eval(e, &vars)?;
                        if missing(&v) {
                            crate::update::unset_path(&mut d, k);
                        } else {
                            crate::update::set_path(&mut d, k, v)?;
                        }
                    }
                    Ok(d)
                })
                .collect::<Result<_, MongoError>>()?
        }
        "$unset" => {
            let fields: Vec<String> = match arg {
                Bson::String(s) => vec![s.clone()],
                Bson::Array(a) => a
                    .iter()
                    .filter_map(|x| x.as_str().map(str::to_owned))
                    .collect(),
                _ => {
                    return Err(loc(
                        31002,
                        "$unset specification must be a string or an array",
                    ));
                }
            };
            docs.into_iter()
                .map(|mut d| {
                    for f in &fields {
                        exclude_path(&mut d, f);
                    }
                    d
                })
                .collect()
        }
        "$replaceRoot" | "$replaceWith" => {
            let e = if name == "$replaceRoot" {
                doc_arg()?
                    .get("newRoot")
                    .cloned()
                    .ok_or_else(|| loc(40231, "no newRoot specified for the $replaceRoot stage"))?
            } else {
                arg.clone()
            };
            docs.iter()
                .map(|d| match eval(&e, &Vars::new(d, lim.now))? {
                    Bson::Document(n) => Ok(n),
                    other => Err(loc(40228, format!("'newRoot' expression must evaluate to an object, but resulting value was: {other}. Type of resulting value: '{}'", type_name(&other)))),
                })
                .collect::<Result<_, _>>()?
        }
        "$sort" => {
            over(lim, &docs, "$sort")?;
            let spec = parse_sort(doc_arg()?)?;
            let mut docs = docs;
            sort_docs(&mut docs, &spec);
            docs
        }
        "$skip" => {
            let n = usize::try_from(
                as_i64(arg)
                    .filter(|n| *n >= 0)
                    .ok_or_else(|| loc(15956, "invalid argument to $skip stage"))?,
            )
            .unwrap_or(usize::MAX);
            docs.into_iter().skip(n).collect()
        }
        "$limit" => {
            let n = as_i64(arg)
                .filter(|n| *n > 0)
                .ok_or_else(|| loc(15958, "the limit must be positive"))?;
            docs.into_iter()
                .take(usize::try_from(n).unwrap_or(usize::MAX))
                .collect()
        }
        "$sample" => {
            let n = doc_arg()?.get("size").and_then(as_i64).unwrap_or(0);
            docs.into_iter()
                .take(usize::try_from(n.max(0)).unwrap_or(0))
                .collect()
        }
        "$count" => {
            let Bson::String(f) = arg else {
                return Err(loc(40156, "the count field must be a non-empty string"));
            };
            if docs.is_empty() {
                Vec::new()
            } else {
                let n = docs.len();
                vec![
                    doc! {f.clone(): i32::try_from(n).map_or(Bson::Int64(i64::try_from(n).unwrap_or(i64::MAX)), Bson::Int32)},
                ]
            }
        }
        "$group" => accum::group(&docs, doc_arg()?, lim)?,
        "$sortByCount" => {
            let g = accum::group(&docs, &doc! {"_id": arg.clone(), "count": {"$sum": 1}}, lim)?;
            let mut g = g;
            sort_docs(&mut g, &vec![("count".to_owned(), -1)]);
            g
        }
        "$unwind" => {
            let (path, keep_empty, index) = match arg {
                Bson::String(s) => (s.clone(), false, None),
                Bson::Document(d) => (
                    d.get_str("path")
                        .map_err(|_| loc(28812, "no path specified to $unwind stage"))?
                        .to_owned(),
                    d.get_bool("preserveNullAndEmptyArrays").unwrap_or(false),
                    d.get_str("includeArrayIndex").ok().map(str::to_owned),
                ),
                _ => {
                    return Err(loc(
                        15981,
                        "expected either a string or an object as specification for $unwind stage",
                    ));
                }
            };
            let path = path
                .strip_prefix('$')
                .ok_or_else(|| {
                    loc(
                        28818,
                        "path option to $unwind stage should be prefixed with a '$'",
                    )
                })?
                .to_owned();
            let mut out = Vec::new();
            for d in docs {
                match get_path(&d, &path).cloned() {
                    Some(Bson::Array(a)) if !a.is_empty() => {
                        for (i, e) in a.into_iter().enumerate() {
                            let mut n = d.clone();
                            crate::update::set_path(&mut n, &path, e)?;
                            if let Some(ix) = &index {
                                n.insert(ix.clone(), i64::try_from(i).unwrap_or(0));
                            }
                            out.push(n);
                        }
                    }
                    Some(Bson::Array(_) | Bson::Null) | None => {
                        if keep_empty {
                            let mut n = d.clone();
                            if matches!(get_path(&d, &path), Some(Bson::Array(_))) {
                                crate::update::unset_path(&mut n, &path);
                            }
                            if let Some(ix) = &index {
                                n.insert(ix.clone(), Bson::Null);
                            }
                            out.push(n);
                        }
                    }
                    Some(_) => {
                        let mut n = d;
                        if let Some(ix) = &index {
                            n.insert(ix.clone(), Bson::Null);
                        }
                        out.push(n);
                    }
                }
            }
            out
        }
        "$facet" => {
            let mut out = Document::new();
            for (k, p) in doc_arg()? {
                let Bson::Array(stages) = p else {
                    return Err(loc(40170, "arguments to $facet must be arrays"));
                };
                let mut cur = docs.clone();
                for s in stages {
                    let s = s
                        .as_document()
                        .ok_or_else(|| bad("each stage must be an object"))?;
                    cur = run_stage(cur, s, lim)?;
                }
                out.insert(
                    k.clone(),
                    cur.into_iter().map(Bson::Document).collect::<Vec<_>>(),
                );
            }
            vec![out]
        }
        "$setWindowFields" => window::set_window_fields(docs, arg, lim)?,
        "$bucket" => stages_bucket::bucket(&docs, arg, lim)?,
        "$bucketAuto" => stages_bucket::bucket_auto(&docs, arg, lim)?,
        "$densify" => stages_fill::densify(docs, arg, lim)?,
        "$fill" => stages_fill::fill(docs, arg, lim)?,
        "$redact" => stages_misc::redact(docs, arg, lim)?,
        "$documents"
        | "$collStats"
        | "$indexStats"
        | "$planCacheStats"
        | "$currentOp"
        | "$listSessions"
        | "$listLocalSessions"
        | "$listClusterCatalog" => {
            return Err(loc(
                40602,
                format!("{name} is only valid as the first stage in a pipeline"),
            ));
        }
        other => {
            return Err(MongoError::new(
                if matches!(
                    other,
                    "$out"
                        | "$merge"
                        | "$graphLookup"
                        | "$geoNear"
                        | "$search"
                        | "$searchMeta"
                        | "$vectorSearch"
                        | "$changeStream"
                ) {
                    code::SPRIG_DOC_FEATURE_UNSUPPORTED
                } else {
                    40324
                },
                format!(
                    "Unrecognized pipeline stage name: '{other}' (or not supported in this release)"
                ),
            ));
        }
    })
}

/// Runs an update pipeline on one document.
///
/// # Errors
/// The stages'.
pub fn run_update_pipeline(
    doc: Document,
    stages: &[Document],
    now: DateTime,
) -> Result<Document, MongoError> {
    let lim = AggLimits {
        stage_bytes: usize::MAX,
        now,
    };
    let mut cur = vec![doc];
    for s in stages {
        let n = s.keys().next().map_or("", String::as_str);
        if !matches!(
            n,
            "$addFields" | "$set" | "$project" | "$unset" | "$replaceRoot" | "$replaceWith"
        ) {
            return Err(MongoError::new(
                code::BAD_VALUE,
                format!("{n} is not allowed to be used within an update"),
            ));
        }
        cur = run_stage(cur, s, &lim)?;
    }
    cur.pop()
        .ok_or_else(|| bad("the update pipeline produced no document"))
}

#[cfg(test)]
mod tests;
