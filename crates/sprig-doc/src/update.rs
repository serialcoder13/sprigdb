//! Update operators with MongoDB semantics, applied in the gateway (the
//! read-modify-write path).
//!
//! Supported: replacements; `$set`, `$unset`, `$inc`, `$mul`, `$min`,
//! `$max`, `$rename`, `$currentDate`, `$setOnInsert`, `$push` (with
//! `$each`, `$slice`, `$sort`, `$position`), `$addToSet` (with `$each`),
//! `$pop`, `$pull`, `$pullAll`, `$bit`; the positional `$`, `$[]` and
//! `$[<id>]` with `arrayFilters`. Pipeline updates go through
//! [`crate::agg`].

use std::cmp::Ordering;
use std::str::FromStr;

use bson::{Bson, DateTime, Decimal128, Document, Timestamp};
use sprig_proto_mongo::errors::{MongoError, code};

use crate::filter::{self, Pred, as_i64};
use crate::keyenc::compare;

fn bad(msg: impl Into<String>) -> MongoError {
    MongoError::new(code::BAD_VALUE, msg)
}

fn not_viable(msg: impl Into<String>) -> MongoError {
    MongoError::new(28, msg)
}

/// A parsed update.
#[derive(Debug, Clone)]
pub enum UpdateSpec {
    /// A replacement document.
    Replace(Document),
    /// Operators: `(operator, path, argument)` in order.
    Ops(Vec<(String, String, Bson)>),
    /// An aggregation pipeline (`[{$set: ...}, ...]`).
    Pipeline(Vec<Document>),
}

/// The context of applying an update to one document.
#[derive(Debug, Clone, Copy)]
pub struct ApplyCx<'a> {
    /// The query, for the positional `$`.
    pub query: &'a Pred,
    /// `arrayFilters`, parsed: `(identifier, predicate on the element)`.
    pub array_filters: &'a [(String, Pred)],
    /// The document is being inserted by an upsert (`$setOnInsert` runs).
    pub inserting: bool,
    /// The time `$currentDate` sets.
    pub now: DateTime,
}

/// MongoDB's order of update paths: component by component, numeric
/// names numerically, others by their bytes; a prefix first.
fn field_order(a: &str, b: &str) -> std::cmp::Ordering {
    let mut x = a.split('.');
    let mut y = b.split('.');
    loop {
        match (x.next(), y.next()) {
            (None, None) => return std::cmp::Ordering::Equal,
            (None, Some(_)) => return std::cmp::Ordering::Less,
            (Some(_), None) => return std::cmp::Ordering::Greater,
            (Some(p), Some(q)) => {
                let o = match (p.parse::<u64>(), q.parse::<u64>()) {
                    (Ok(m), Ok(n)) if !p.starts_with('0') || p == "0" => {
                        m.cmp(&n).then_with(|| p.cmp(q))
                    }
                    _ => p.cmp(q),
                };
                if o != std::cmp::Ordering::Equal {
                    return o;
                }
            }
        }
    }
}

/// Parses an update document or pipeline.
///
/// # Errors
/// 9 `FailedToParse` for an empty or mixed update; 2 for unknown
/// operators.
pub fn parse(u: &Bson) -> Result<UpdateSpec, MongoError> {
    match u {
        Bson::Array(a) => Ok(UpdateSpec::Pipeline(
            a.iter()
                .map(|s| {
                    s.as_document()
                        .cloned()
                        .ok_or_else(|| bad("each pipeline stage must be an object"))
                })
                .collect::<Result<_, _>>()?,
        )),
        Bson::Document(d) => {
            let ops = d.keys().filter(|k| k.starts_with('$')).count();
            if ops == 0 {
                for k in d.keys() {
                    if k.starts_with('$') {
                        return Err(MongoError::new(
                            code::DOLLAR_PREFIXED_FIELD_NAME,
                            "bad field",
                        ));
                    }
                }
                return Ok(UpdateSpec::Replace(d.clone()));
            }
            if ops != d.len() {
                return Err(MongoError::new(
                    code::FAILED_TO_PARSE,
                    "Unknown modifier: an update mixes operators and fields",
                ));
            }
            let mut out = Vec::new();
            let mut paths: Vec<String> = Vec::new();
            for (op, arg) in d {
                if !matches!(
                    op.as_str(),
                    "$set"
                        | "$unset"
                        | "$inc"
                        | "$mul"
                        | "$min"
                        | "$max"
                        | "$rename"
                        | "$currentDate"
                        | "$setOnInsert"
                        | "$push"
                        | "$addToSet"
                        | "$pop"
                        | "$pull"
                        | "$pullAll"
                        | "$bit"
                ) {
                    return Err(MongoError::new(
                        code::FAILED_TO_PARSE,
                        format!(
                            "Unknown modifier: {op}. Expected a valid update modifier or pipeline-style update specified as an array"
                        ),
                    ));
                }
                let Bson::Document(fields) = arg else {
                    return Err(MongoError::new(
                        code::FAILED_TO_PARSE,
                        format!(
                            "Modifiers operate on fields but we found type {} instead. For example: {{$mod: {{<field>: ...}}}} not {{{op}: {arg}}}",
                            type_name(arg)
                        ),
                    ));
                };
                for (path, v) in fields {
                    if path.is_empty() || path.split('.').any(str::is_empty) {
                        return Err(bad(format!(
                            "The update path '{path}' contains an empty field name, which is not allowed."
                        )));
                    }
                    let mut targets = vec![path.clone()];
                    if op == "$rename" {
                        let Bson::String(to) = v else {
                            return Err(bad("The 'to' field for $rename must be a string"));
                        };
                        targets.push(to.clone());
                    }
                    for t in targets {
                        if let Some(p) = paths.iter().find(|p| conflicts(p, &t)) {
                            return Err(MongoError::new(
                                code::CONFLICTING_UPDATE_OPERATORS,
                                format!("Updating the path '{t}' would create a conflict at '{p}'"),
                            ));
                        }
                        paths.push(t);
                    }
                    out.push((op.clone(), path.clone(), v.clone()));
                }
            }
            if out.is_empty() {
                return Err(MongoError::new(
                    code::FAILED_TO_PARSE,
                    "'$set' is empty. You must specify a field like so: {$set: {<field>: ...}}",
                ));
            }
            // MongoDB 5.0+ applies an update's fields in field-name order,
            // whatever the operator (numeric names numerically), so new
            // fields are appended in that order (M7, found by the `$merge`
            // differential).
            out.sort_by(|a, b| field_order(&a.1, &b.1));
            Ok(UpdateSpec::Ops(out))
        }
        _ => Err(MongoError::new(
            code::FAILED_TO_PARSE,
            "Update argument must be either an object or an array",
        )),
    }
}

fn conflicts(a: &str, b: &str) -> bool {
    // Positional parts stand for any element.
    let norm = |p: &str| -> Vec<String> {
        p.split('.')
            .map(|x| {
                if x.starts_with('$') {
                    "$".to_owned()
                } else {
                    x.to_owned()
                }
            })
            .collect()
    };
    let (a, b) = (norm(a), norm(b));
    let n = a.len().min(b.len());
    a[..n] == b[..n]
}

/// A short BSON type name for messages.
#[must_use]
pub fn type_name(v: &Bson) -> &'static str {
    match v {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Document(_) => "object",
        Bson::Array(_) => "array",
        Bson::Binary(_) => "binData",
        Bson::Undefined => "undefined",
        Bson::ObjectId(_) => "objectId",
        Bson::Boolean(_) => "bool",
        Bson::DateTime(_) => "date",
        Bson::Null => "null",
        Bson::RegularExpression(_) => "regex",
        Bson::Int32(_) => "int",
        Bson::Int64(_) => "long",
        Bson::Decimal128(_) => "decimal",
        Bson::Timestamp(_) => "timestamp",
        _ => "other",
    }
}

// --- paths -------------------------------------------------------------------

/// The value at a dotted path (array positions as numbers), if any.
#[must_use]
pub fn get_path<'a>(doc: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut parts = path.split('.');
    let mut cur = doc.get(parts.next()?)?;
    for p in parts {
        cur = match cur {
            Bson::Document(d) => d.get(p)?,
            Bson::Array(a) => a.get(p.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

fn get_mut<'a>(doc: &'a mut Document, parts: &[&str]) -> Option<&'a mut Bson> {
    let mut cur = doc.get_mut(parts[0])?;
    for p in &parts[1..] {
        cur = match cur {
            Bson::Document(d) => d.get_mut(*p)?,
            Bson::Array(a) => a.get_mut(p.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

/// Sets `path` to `v`, creating documents on the way and padding arrays
/// with nulls.
///
/// # Errors
/// 28 `PathNotViable` through a scalar.
pub fn set_path(doc: &mut Document, path: &str, v: Bson) -> Result<(), MongoError> {
    let parts: Vec<&str> = path.split('.').collect();
    set_in_doc(doc, &parts, v, path)
}

fn set_in_doc(d: &mut Document, parts: &[&str], v: Bson, full: &str) -> Result<(), MongoError> {
    if parts.len() == 1 {
        d.insert(parts[0], v);
        return Ok(());
    }
    if !d.contains_key(parts[0]) {
        d.insert(parts[0], Document::new());
    }
    let child = d.get_mut(parts[0]).ok_or_else(|| bad("path"))?;
    set_in_val(child, &parts[1..], v, full)
}

fn set_in_val(cur: &mut Bson, parts: &[&str], v: Bson, full: &str) -> Result<(), MongoError> {
    match cur {
        Bson::Document(d) => set_in_doc(d, parts, v, full),
        Bson::Array(a) => {
            let Ok(i) = parts[0].parse::<usize>() else {
                return Err(not_viable(format!(
                    "Cannot create field '{}' in element {{{}: [...]}}",
                    parts[0], full
                )));
            };
            while a.len() <= i {
                a.push(Bson::Null);
            }
            if parts.len() == 1 {
                a[i] = v;
                return Ok(());
            }
            if matches!(a[i], Bson::Null) {
                a[i] = Bson::Document(Document::new());
            }
            set_in_val(&mut a[i], &parts[1..], v, full)
        }
        other => Err(not_viable(format!(
            "Cannot create field '{}' in element {{{}}}",
            parts[0], other
        ))),
    }
}

/// Removes `path` (array elements become null, as `$unset` does).
pub fn unset_path(doc: &mut Document, path: &str) {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.len() == 1 {
        doc.remove(parts[0]);
        return;
    }
    let Some(parent) = get_mut(doc, &parts[..parts.len() - 1]) else {
        return;
    };
    let last = parts[parts.len() - 1];
    match parent {
        Bson::Document(d) => {
            d.remove(last);
        }
        Bson::Array(a) => {
            if let Ok(i) = last.parse::<usize>()
                && i < a.len()
            {
                a[i] = Bson::Null;
            }
        }
        _ => {}
    }
}

// --- arithmetic --------------------------------------------------------------

fn is_num(v: &Bson) -> bool {
    matches!(
        v,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_)
    )
}

/// `(coefficient, exponent)` of a finite number, for decimal arithmetic.
fn dec_parts(v: &Bson) -> Option<(i128, i32)> {
    let t = match v {
        Bson::Int32(i) => return Some((i128::from(*i), 0)),
        Bson::Int64(i) => return Some((i128::from(*i), 0)),
        Bson::Double(f) if f.is_finite() => format!("{f:e}"),
        Bson::Decimal128(d) => d.to_string(),
        _ => return None,
    };
    let (neg, rest) = t
        .strip_prefix('-')
        .map_or((false, t.as_str()), |r| (true, r));
    let (mant, e) = match rest.find(['e', 'E']) {
        Some(i) => (&rest[..i], rest[i + 1..].parse::<i32>().ok()?),
        None => (rest, 0),
    };
    let (ip, fp) = mant.split_once('.').unwrap_or((mant, ""));
    let coef: i128 = format!("{ip}{fp}").parse().ok()?;
    let exp = e - i32::try_from(fp.len()).ok()?;
    Some((if neg { -coef } else { coef }, exp))
}

fn dec_from(coef: i128, exp: i32) -> Option<Bson> {
    let s = if exp == 0 {
        coef.to_string()
    } else {
        format!("{coef}E{exp}")
    };
    Decimal128::from_str(&s).ok().map(Bson::Decimal128)
}

fn to_f64(v: &Bson) -> f64 {
    match v {
        Bson::Int32(i) => f64::from(*i),
        #[allow(clippy::cast_precision_loss)]
        Bson::Int64(i) => *i as f64,
        Bson::Double(f) => *f,
        Bson::Decimal128(d) => d.to_string().parse().unwrap_or(f64::NAN),
        _ => f64::NAN,
    }
}

/// `a + b` or `a * b` with MongoDB's type promotion.
///
/// # Errors
/// 2 on int64 overflow.
pub fn arith(a: &Bson, b: &Bson, mul: bool) -> Result<Bson, MongoError> {
    let overflow = || bad("integer overflow");
    Ok(match (a, b) {
        (Bson::Decimal128(_), _) | (_, Bson::Decimal128(_)) => {
            let r = match (dec_parts(a), dec_parts(b)) {
                (Some((ca, ea)), Some((cb, eb))) => {
                    if mul {
                        ca.checked_mul(cb).and_then(|c| dec_from(c, ea + eb))
                    } else {
                        let e = ea.min(eb);
                        let sa = 10i128.checked_pow(u32::try_from(ea - e).unwrap_or(99));
                        let sb = 10i128.checked_pow(u32::try_from(eb - e).unwrap_or(99));
                        match (sa, sb) {
                            (Some(sa), Some(sb)) => ca
                                .checked_mul(sa)
                                .zip(cb.checked_mul(sb))
                                .and_then(|(x, y)| x.checked_add(y))
                                .and_then(|c| dec_from(c, e)),
                            _ => None,
                        }
                    }
                }
                _ => None,
            };
            if let Some(r) = r {
                r
            } else {
                let f = if mul {
                    to_f64(a) * to_f64(b)
                } else {
                    to_f64(a) + to_f64(b)
                };
                Decimal128::from_str(&format!("{f:e}")).map_or(Bson::Double(f), Bson::Decimal128)
            }
        }
        (Bson::Double(_), _) | (_, Bson::Double(_)) => Bson::Double(if mul {
            to_f64(a) * to_f64(b)
        } else {
            to_f64(a) + to_f64(b)
        }),
        (Bson::Int32(x), Bson::Int32(y)) => {
            let r = if mul {
                i64::from(*x) * i64::from(*y)
            } else {
                i64::from(*x) + i64::from(*y)
            };
            i32::try_from(r).map_or(Bson::Int64(r), Bson::Int32)
        }
        _ => {
            let x = as_i64(a).ok_or_else(overflow)?;
            let y = as_i64(b).ok_or_else(overflow)?;
            Bson::Int64(
                if mul {
                    x.checked_mul(y)
                } else {
                    x.checked_add(y)
                }
                .ok_or_else(overflow)?,
            )
        }
    })
}

// --- positional paths --------------------------------------------------------

/// The first array position of `array_path` whose element meets the
/// query's conditions on that array.
fn matched_position(doc: &Document, query: &Pred, array_path: &str) -> Option<usize> {
    let Some(Bson::Array(a)) = get_path(doc, array_path) else {
        return None;
    };
    let mut conds = Vec::new();
    collect_conds(query, array_path, &mut conds);
    if conds.is_empty() {
        return None;
    }
    a.iter().position(|e| {
        let wrap = bson::doc! {"e": e.clone()};
        conds.iter().all(|(sub, op)| {
            let p = if sub.is_empty() {
                "e".to_owned()
            } else {
                format!("e.{sub}")
            };
            op.eval(&filter::values_at(&wrap, &p), &filter::NoExpr, &wrap)
        })
    })
}

fn collect_conds(p: &Pred, array_path: &str, out: &mut Vec<(String, filter::FieldOp)>) {
    match p {
        Pred::Field { path, op } => {
            if path == array_path {
                out.push((String::new(), op.clone()));
            } else if let Some(rest) = path
                .strip_prefix(array_path)
                .and_then(|r| r.strip_prefix('.'))
            {
                out.push((rest.to_owned(), op.clone()));
            }
        }
        Pred::And(k) => {
            for x in k {
                collect_conds(x, array_path, out);
            }
        }
        _ => {}
    }
}

/// Expands positional parts of `path` into concrete paths.
fn expand(doc: &Document, path: &str, cx: &ApplyCx<'_>) -> Result<Vec<String>, MongoError> {
    if !path.split('.').any(|p| p.starts_with('$')) {
        return Ok(vec![path.to_owned()]);
    }
    let parts: Vec<&str> = path.split('.').collect();
    let mut prefixes: Vec<String> = vec![String::new()];
    for (i, p) in parts.iter().enumerate() {
        let mut next = Vec::new();
        for pre in &prefixes {
            let join = |x: &str| {
                if pre.is_empty() {
                    x.to_owned()
                } else {
                    format!("{pre}.{x}")
                }
            };
            if *p == "$" {
                let pos = matched_position(doc, cx.query, pre).ok_or_else(|| {
                    bad("The positional operator did not find the match needed from the query.")
                })?;
                next.push(join(&pos.to_string()));
            } else if *p == "$[]" || (p.starts_with("$[") && p.ends_with(']')) {
                let Some(Bson::Array(a)) = get_path(doc, pre) else {
                    return Err(bad(format!(
                        "The path '{}' must exist in the document in order to apply array updates.",
                        parts[..i].join(".")
                    )));
                };
                let ident = &p[2..p.len() - 1];
                let filt = if ident.is_empty() {
                    None
                } else {
                    Some(
                        cx.array_filters
                            .iter()
                            .find(|(id, _)| id == ident)
                            .map(|(_, f)| f)
                            .ok_or_else(|| {
                                bad(format!(
                                    "No array filter found for identifier '{ident}' in path '{path}'"
                                ))
                            })?,
                    )
                };
                for (k, e) in a.iter().enumerate() {
                    let ok = filt.is_none_or(|f| {
                        let wrap = bson::doc! {ident: e.clone()};
                        f.matches(&wrap)
                    });
                    if ok {
                        next.push(join(&k.to_string()));
                    }
                }
            } else {
                next.push(join(p));
            }
        }
        prefixes = next;
    }
    Ok(prefixes)
}

// --- applying ----------------------------------------------------------------

fn each_args(v: &Bson) -> Option<(Vec<Bson>, &Document)> {
    match v {
        Bson::Document(d) if d.contains_key("$each") => {
            let Ok(each) = d.get_array("$each") else {
                return None;
            };
            Some((each.clone(), d))
        }
        _ => None,
    }
}

fn sort_values(a: &mut [Bson], spec: &Bson) -> Result<(), MongoError> {
    match spec {
        Bson::Document(keys) => {
            let keys: Vec<(String, i64)> = keys
                .iter()
                .map(|(k, v)| (k.clone(), as_i64(v).unwrap_or(1)))
                .collect();
            a.sort_by(|x, y| {
                for (k, dir) in &keys {
                    let gx = x
                        .as_document()
                        .and_then(|d| get_path(d, k))
                        .unwrap_or(&Bson::Null);
                    let gy = y
                        .as_document()
                        .and_then(|d| get_path(d, k))
                        .unwrap_or(&Bson::Null);
                    let o = compare(gx, gy);
                    if o != Ordering::Equal {
                        return if *dir < 0 { o.reverse() } else { o };
                    }
                }
                Ordering::Equal
            });
        }
        other => {
            let dir = as_i64(other).ok_or_else(|| bad("The $sort is invalid"))?;
            a.sort_by(|x, y| {
                if dir < 0 {
                    compare(y, x)
                } else {
                    compare(x, y)
                }
            });
        }
    }
    Ok(())
}

fn pull_matches(e: &Bson, cond: &Bson) -> bool {
    match cond {
        Bson::Document(d) if d.keys().next().is_some_and(|k| k.starts_with('$')) => {
            let wrap = bson::doc! {"e": e.clone()};
            filter::parse(&bson::doc! {"e": d.clone()})
                .map(|p| p.matches(&wrap))
                .unwrap_or(false)
        }
        Bson::Document(d) => match e {
            Bson::Document(ed) => filter::parse(d).map(|p| p.matches(ed)).unwrap_or(false),
            _ => false,
        },
        other => crate::keyenc::equal(e, other),
    }
}

/// Applies `spec` to `doc` in place. Returns whether the document changed.
///
/// # Errors
/// MongoDB's errors for type mismatches, non-viable paths and changes to
/// `_id` (66 `ImmutableField`).
pub fn apply(doc: &mut Document, spec: &UpdateSpec, cx: &ApplyCx<'_>) -> Result<bool, MongoError> {
    let before = bson::to_vec(doc).unwrap_or_default();
    let old_id = doc.get("_id").cloned();
    match spec {
        UpdateSpec::Replace(r) => {
            let mut n = Document::new();
            if let Some(id) = &old_id {
                n.insert("_id", id.clone());
            }
            for (k, v) in r {
                if k == "_id" {
                    if let Some(id) = &old_id
                        && !crate::keyenc::equal(id, v)
                    {
                        return Err(MongoError::new(
                            code::IMMUTABLE_FIELD,
                            "After applying the update, the (immutable) field '_id' was found to have been altered",
                        ));
                    }
                    n.insert("_id", v.clone());
                    continue;
                }
                n.insert(k.clone(), v.clone());
            }
            *doc = n;
        }
        UpdateSpec::Ops(ops) => {
            for (op, path, arg) in ops {
                for p in expand(doc, path, cx)? {
                    apply_one(doc, op, &p, arg, cx)?;
                }
            }
        }
        UpdateSpec::Pipeline(stages) => {
            let out = crate::agg::run_update_pipeline(doc.clone(), stages, cx.now)?;
            *doc = out;
        }
    }
    match (&old_id, doc.get("_id")) {
        (Some(a), Some(b))
            if bson::to_vec(&bson::doc! {"x": a.clone()}).ok()
                != bson::to_vec(&bson::doc! {"x": b.clone()}).ok() =>
        {
            return Err(MongoError::new(
                code::IMMUTABLE_FIELD,
                "Performing an update on the path '_id' would modify the immutable field '_id'",
            ));
        }
        (Some(a), None) => {
            let mut n = bson::doc! {"_id": a.clone()};
            n.extend(std::mem::take(doc));
            *doc = n;
        }
        _ => {}
    }
    Ok(bson::to_vec(doc).unwrap_or_default() != before)
}

#[allow(clippy::too_many_lines)]
fn apply_one(
    doc: &mut Document,
    op: &str,
    path: &str,
    arg: &Bson,
    cx: &ApplyCx<'_>,
) -> Result<(), MongoError> {
    let cur = get_path(doc, path).cloned();
    match op {
        "$set" => set_path(doc, path, arg.clone()),
        "$setOnInsert" => {
            if cx.inserting {
                set_path(doc, path, arg.clone())
            } else {
                Ok(())
            }
        }
        "$unset" => {
            unset_path(doc, path);
            Ok(())
        }
        "$inc" | "$mul" => {
            if !is_num(arg) {
                return Err(MongoError::new(
                    code::TYPE_MISMATCH,
                    format!(
                        "Cannot {} with non-numeric argument: {{{path}: {arg}}}",
                        if op == "$inc" {
                            "increment"
                        } else {
                            "multiply"
                        }
                    ),
                ));
            }
            let v = match &cur {
                None => {
                    if op == "$inc" {
                        arg.clone()
                    } else {
                        arith(&Bson::Int32(0), arg, true)?
                    }
                }
                Some(c) if is_num(c) => arith(c, arg, op == "$mul")?,
                Some(c) => {
                    return Err(MongoError::new(
                        code::TYPE_MISMATCH,
                        format!(
                            "Cannot apply {op} to a value of non-numeric type. {{_id: ...}} has the field '{path}' of non-numeric type {}",
                            type_name(c)
                        ),
                    ));
                }
            };
            set_path(doc, path, v)
        }
        "$min" | "$max" => {
            let replace = match &cur {
                None => true,
                Some(c) => {
                    let o = compare(arg, c);
                    if op == "$min" {
                        o == Ordering::Less
                    } else {
                        o == Ordering::Greater
                    }
                }
            };
            if replace {
                set_path(doc, path, arg.clone())?;
            }
            Ok(())
        }
        "$rename" => {
            let Bson::String(to) = arg else {
                return Err(bad("The 'to' field for $rename must be a string"));
            };
            if let Some(v) = cur {
                unset_path(doc, path);
                set_path(doc, to, v)?;
            }
            Ok(())
        }
        "$currentDate" => {
            let ts = matches!(arg, Bson::Document(d) if d.get_str("$type") == Ok("timestamp"));
            let v = if ts {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let secs = (cx.now.timestamp_millis() / 1000) as u32;
                Bson::Timestamp(Timestamp {
                    time: secs,
                    increment: 1,
                })
            } else {
                Bson::DateTime(cx.now)
            };
            set_path(doc, path, v)
        }
        "$push" | "$addToSet" => {
            let mut arr = match cur {
                None => Vec::new(),
                Some(Bson::Array(a)) => a,
                Some(other) => {
                    return Err(bad(format!(
                        "The field '{path}' must be an array but is of type {} in document {{_id: ...}}",
                        type_name(&other)
                    )));
                }
            };
            let (items, mods) = match each_args(arg) {
                Some((each, d)) => (each, Some(d)),
                None => (vec![arg.clone()], None),
            };
            if op == "$addToSet" {
                for it in items {
                    if !arr.iter().any(|e| crate::keyenc::equal(e, &it)) {
                        arr.push(it);
                    }
                }
            } else {
                let pos = mods.and_then(|m| m.get("$position")).and_then(as_i64);
                match pos {
                    Some(p) => {
                        let len = i64::try_from(arr.len()).unwrap_or(i64::MAX);
                        let at = if p < 0 { (len + p).max(0) } else { p.min(len) };
                        let at = usize::try_from(at).unwrap_or(0);
                        for (k, it) in items.into_iter().enumerate() {
                            arr.insert(at + k, it);
                        }
                    }
                    None => arr.extend(items),
                }
                if let Some(s) = mods.and_then(|m| m.get("$sort")) {
                    sort_values(&mut arr, s)?;
                }
                if let Some(n) = mods.and_then(|m| m.get("$slice")).and_then(as_i64) {
                    let len = arr.len();
                    if n >= 0 {
                        arr.truncate(usize::try_from(n).unwrap_or(len));
                    } else {
                        let keep = usize::try_from(-n).unwrap_or(len).min(len);
                        arr.drain(..len - keep);
                    }
                }
            }
            set_path(doc, path, Bson::Array(arr))
        }
        "$pop" => match cur {
            None => Ok(()),
            Some(Bson::Array(mut a)) => {
                if !a.is_empty() {
                    if as_i64(arg) == Some(-1) {
                        a.remove(0);
                    } else {
                        a.pop();
                    }
                }
                set_path(doc, path, Bson::Array(a))
            }
            Some(other) => Err(MongoError::new(
                code::TYPE_MISMATCH,
                format!(
                    "Path '{path}' contains an element of non-array type '{}'",
                    type_name(&other)
                ),
            )),
        },
        "$pull" | "$pullAll" => match cur {
            None => Ok(()),
            Some(Bson::Array(a)) => {
                let kept: Vec<Bson> = if op == "$pullAll" {
                    let Bson::Array(list) = arg else {
                        return Err(bad("$pullAll requires an array argument"));
                    };
                    a.into_iter()
                        .filter(|e| !list.iter().any(|x| crate::keyenc::equal(e, x)))
                        .collect()
                } else {
                    a.into_iter().filter(|e| !pull_matches(e, arg)).collect()
                };
                set_path(doc, path, Bson::Array(kept))
            }
            Some(_) => Err(bad("Cannot apply $pull to a non-array value")),
        },
        "$bit" => {
            let Bson::Document(b) = arg else {
                return Err(bad("The $bit modifier is not compatible with a non-object"));
            };
            let mut v = cur.unwrap_or(Bson::Int32(0));
            for (k, x) in b {
                v = match (&v, x) {
                    (Bson::Int32(a), Bson::Int32(y)) => Bson::Int32(
                        bitop(k, i64::from(*a), i64::from(*y))
                            .try_into()
                            .unwrap_or(0),
                    ),
                    (a, y) => match (as_i64(a), as_i64(y)) {
                        (Some(a), Some(y)) => Bson::Int64(bitop(k, a, y)),
                        _ => return Err(bad("$bit needs integers")),
                    },
                };
            }
            set_path(doc, path, v)
        }
        _ => Err(bad(format!("unknown modifier {op}"))),
    }
}

fn bitop(k: &str, a: i64, b: i64) -> i64 {
    match k {
        "and" => a & b,
        "or" => a | b,
        _ => a ^ b,
    }
}

/// The document an upsert inserts before the update applies: the
/// query's equality fields.
///
/// # Errors
/// 28 when two equality paths collide.
pub fn upsert_seed(query: &Pred) -> Result<Document, MongoError> {
    let mut fields = Vec::new();
    query.equality_fields(&mut fields);
    let mut d = Document::new();
    for (p, v) in fields {
        if p.split('.').any(|x| x.starts_with('$')) {
            continue;
        }
        set_path(&mut d, &p, v)?;
    }
    Ok(d)
}

#[cfg(test)]
mod tests;
