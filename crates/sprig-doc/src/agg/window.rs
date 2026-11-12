//! `$setWindowFields` (M2, 12 Nov 2026): partitions, document and range
//! windows (with time units), the accumulators as window functions, and
//! `$rank`, `$denseRank`, `$documentNumber`, `$shift`, `$derivative`,
//! `$integral`, `$expMovingAvg`, `$covariancePop`, `$covarianceSamp`,
//! `$linearFill` and `$locf`, with MongoDB 8.0's errors.
//!
//! The stage sorts its input by the partition key and `sortBy` (stably,
//! as MongoDB does), then computes every output over each partition from
//! the documents as they came in. A window whose lower bound is
//! `unbounded` is accumulated incrementally; other windows are summed
//! afresh for each document.

use std::cmp::Ordering;

use bson::{Bson, Document};
use sprig_proto_mongo::errors::{MongoError, code};

use super::accum::{AccSpec, AccState, Ctx, Item, kind_of};
use super::accum_num::{self, Dec, is_num, varith};
use super::{AggLimits, SortSpec, Vars, eval, missing, nullish, sort_compare, sort_key};
use crate::keyenc::compare;
use crate::update::type_name;

fn err(c: i32, msg: impl Into<String>) -> MongoError {
    MongoError::new(c, msg)
}

fn e9(msg: impl Into<String>) -> MongoError {
    err(code::FAILED_TO_PARSE, msg)
}

#[derive(Debug, Clone)]
enum Bound {
    Unbounded,
    Current,
    Off(Bson),
}

#[derive(Debug, Clone)]
enum Window {
    Docs(Bound, Bound),
    Range(Bound, Bound, Option<String>),
}

#[derive(Debug, Clone)]
enum Func {
    Acc(AccSpec),
    Rank(u8),
    Shift { output: Bson, by: i64, default: Bson },
    Derivative { input: Bson, unit: Option<String> },
    Integral { input: Bson, unit: Option<String> },
    Ema { input: Bson, alpha: Dec },
    Cov { pair: Bson, samp: bool },
    LinearFill(Bson),
    Locf(Bson),
}

#[derive(Debug, Clone)]
struct Output {
    path: String,
    func: Func,
    window: Option<Window>,
}

const UNITS: [&str; 9] = [
    "year",
    "quarter",
    "month",
    "week",
    "day",
    "hour",
    "minute",
    "second",
    "millisecond",
];

/// The length of a fixed time unit in milliseconds.
pub(crate) fn unit_ms(u: &str) -> Option<i64> {
    Some(match u {
        "week" => 604_800_000,
        "day" => 86_400_000,
        "hour" => 3_600_000,
        "minute" => 60_000,
        "second" => 1000,
        "millisecond" => 1,
        _ => return None,
    })
}

/// `ms` plus `amount` units, months clamped to the month's last day as
/// `$dateAdd` does.
pub(crate) fn date_add(ms: i64, amount: i64, unit: &str) -> Option<i64> {
    if let Some(u) = unit_ms(unit) {
        return ms.checked_add(amount.checked_mul(u)?);
    }
    let months = match unit {
        "month" => amount,
        "quarter" => amount.checked_mul(3)?,
        "year" => amount.checked_mul(12)?,
        _ => return None,
    };
    let days = ms.div_euclid(86_400_000);
    let rem = ms.rem_euclid(86_400_000);
    let (y, m, d) = civil(days);
    let total = y.checked_mul(12)? + (m - 1) + months;
    let (ny, nm) = (total.div_euclid(12), total.rem_euclid(12) + 1);
    let last = month_len(ny, nm);
    Some(days_from_civil(ny, nm, d.min(last)) * 86_400_000 + rem)
}

fn month_len(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        _ => 28,
    }
}

/// Days since 1970-01-01 to (year, month, day) (Howard Hinnant's method).
pub(crate) fn civil(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

pub(crate) fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn parse_bound(v: &Bson, docs: bool) -> Result<Bound, MongoError> {
    match v {
        Bson::String(s) if s == "unbounded" => Ok(Bound::Unbounded),
        Bson::String(s) if s == "current" => Ok(Bound::Current),
        x if is_num(x) => {
            if docs {
                match accum_num::integral(x) {
                    Some(i) => Ok(Bound::Off(Bson::Int64(i))),
                    None => Err(e9("Numeric document-based bounds must be an integer")),
                }
            } else {
                Ok(Bound::Off(x.clone()))
            }
        }
        _ => Err(e9(
            "Window bounds must be 'unbounded', 'current', or a number.",
        )),
    }
}

/// Compares bounds where `current` is 0.
fn bound_order(a: &Bound, b: &Bound) -> bool {
    let num = |x: &Bound| match x {
        Bound::Unbounded => None,
        Bound::Current => Some(Bson::Int32(0)),
        Bound::Off(v) => Some(v.clone()),
    };
    match (a, b) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
        _ => compare(&num(a).unwrap_or(Bson::Null), &num(b).unwrap_or(Bson::Null)) != Ordering::Greater,
    }
}

fn show_bound(b: &Bound) -> String {
    match b {
        Bound::Unbounded => "\"unbounded\"".to_owned(),
        Bound::Current => "\"current\"".to_owned(),
        Bound::Off(v) => v.to_string(),
    }
}

fn parse_window(v: &Bson) -> Result<Window, MongoError> {
    let only = "'window' field can only contain 'documents' as the only argument or 'range' with an optional 'unit' field";
    let Bson::Document(w) = v else {
        return Err(e9("'window' field must be an object"));
    };
    // `window: {}` is the default window, the whole partition.
    if w.is_empty() {
        return Ok(Window::Docs(Bound::Unbounded, Bound::Unbounded));
    }
    if w.keys().any(|k| !matches!(k.as_str(), "documents" | "range" | "unit")) {
        return Err(e9(only));
    }
    if w.contains_key("documents") && w.contains_key("range") {
        return Err(e9(only));
    }
    if w.contains_key("documents") && w.contains_key("unit") {
        return Err(e9(
            "Window bounds can specify either 'documents' or 'unit', not both.",
        ));
    }
    if !w.contains_key("documents") && !w.contains_key("range") {
        return Err(e9(
            "Window bounds can only specify 'unit' with range-based bounds.",
        ));
    }
    let docs = w.contains_key("documents");
    let key = if docs { "documents" } else { "range" };
    let pair = match w.get(key) {
        Some(Bson::Array(a)) if a.len() == 2 => a,
        Some(x) => {
            return Err(e9(format!(
                "Window bounds must be a 2-element array: {key}: {x}"
            )));
        }
        None => return Err(e9(only)),
    };
    let lo = parse_bound(&pair[0], docs)?;
    let hi = parse_bound(&pair[1], docs)?;
    if !bound_order(&lo, &hi) {
        return Err(err(
            5_339_900,
            format!(
                "Lower bound must not exceed upper bound: [{}, {}]",
                show_bound(&lo),
                show_bound(&hi)
            ),
        ));
    }
    if docs {
        return Ok(Window::Docs(lo, hi));
    }
    let unit = match w.get("unit") {
        None => None,
        Some(Bson::String(u)) if UNITS.contains(&u.as_str()) => Some(u.clone()),
        Some(u) => {
            return Err(err(
                code::BAD_VALUE,
                format!("unknown time unit value: {u}"),
            ));
        }
    };
    Ok(Window::Range(lo, hi, unit))
}

fn obj_arg<'a>(op: &str, arg: &'a Bson) -> Result<&'a Document, MongoError> {
    arg.as_document().ok_or_else(|| {
        e9(format!(
            "{op} expects an object, but got a {}: {op}: {}",
            type_name(arg),
            match arg {
                Bson::String(s) => format!("\"{s}\""),
                o => o.to_string(),
            }
        ))
    })
}

fn is_const(e: &Bson) -> bool {
    match e {
        Bson::String(s) => !s.starts_with('$') || s == "$$NOW",
        Bson::Array(a) => a.iter().all(is_const),
        Bson::Document(d) => d.keys().next().is_some_and(|k| k == "$literal") || d.values().all(is_const),
        _ => true,
    }
}

fn parse_func(
    name: &str,
    op: &str,
    arg: &Bson,
    window: Option<&Bson>,
    sort: Option<&SortSpec>,
) -> Result<Func, MongoError> {
    let no_window = |f: &str| -> Result<(), MongoError> {
        if window.is_some() {
            return Err(e9(format!("'window' field is not allowed in {f}")));
        }
        Ok(())
    };
    let single_sort = sort.is_some_and(|s| s.len() == 1);
    Ok(match op {
        "$rank" | "$denseRank" | "$documentNumber" => {
            if !matches!(arg, Bson::Document(d) if d.is_empty()) {
                return Err(err(
                    5_371_603,
                    format!("{op} must be specified with '{{}}' as the value"),
                ));
            }
            if window.is_some() {
                return Err(err(
                    5_371_601,
                    "Rank style window functions take no other arguments",
                ));
            }
            if !single_sort {
                return Err(err(
                    5_371_602,
                    format!(
                        "{op} must be specified with a top level sortBy expression with exactly one element"
                    ),
                ));
            }
            Func::Rank(match op {
                "$rank" => 0,
                "$denseRank" => 1,
                _ => 2,
            })
        }
        "$shift" => {
            let d = obj_arg(op, arg)?;
            if window.is_some() {
                return Err(e9("$shift does not accept a 'window' field"));
            }
            let output = d
                .get("output")
                .cloned()
                .ok_or_else(|| e9("$shift requires an 'output' expression."))?;
            let by = match d.get("by") {
                Some(b) if is_num(b) => accum_num::integral(b).ok_or_else(|| {
                    e9(format!("'$shift:by' field must be an integer, but found  by: {b}"))
                })?,
                _ => return Err(e9("$shift requires 'by' as an integer value.")),
            };
            let default = match d.get("default") {
                Some(x) if !is_const(x) => {
                    return Err(e9("'$shift:default' expression must yield a constant value."));
                }
                Some(x) => x.clone(),
                None => Bson::Null,
            };
            for k in d.keys() {
                if !matches!(k.as_str(), "output" | "by" | "default") {
                    return Err(e9(format!("Unknown argument in $shift: {k}")));
                }
            }
            if sort.is_none() {
                return Err(e9("'$shift' requires a sortBy"));
            }
            Func::Shift { output, by, default }
        }
        "$derivative" | "$integral" => {
            let d = obj_arg(op, arg)?;
            let input = d
                .get("input")
                .cloned()
                .ok_or_else(|| e9(format!("{op} requires an 'input' expression")))?;
            let unit = match d.get("unit") {
                None => None,
                Some(Bson::String(u)) if UNITS.contains(&u.as_str()) => Some(u.clone()),
                Some(u) => return Err(err(code::BAD_VALUE, format!("unknown time unit value: {u}"))),
            };
            if let Some(u) = &unit
                && unit_ms(u).is_none()
            {
                return Err(err(
                    5_490_704,
                    format!("{op} with unit '{u}' is not supported: use 'week' or smaller"),
                ));
            }
            for k in d.keys() {
                if !matches!(k.as_str(), "input" | "unit") {
                    return Err(e9(format!("{op} got unexpected argument: {k}")));
                }
            }
            if op == "$derivative" {
                if sort.is_none() {
                    return Err(e9("$derivative requires a sortBy"));
                }
                if window.is_none() {
                    return Err(e9("$derivative requires explicit window bounds"));
                }
                Func::Derivative { input, unit }
            } else {
                if !single_sort {
                    return Err(e9("$integral requires a non-compound sortBy"));
                }
                Func::Integral { input, unit }
            }
        }
        "$expMovingAvg" => {
            let Bson::Document(d) = arg else {
                return Err(e9("$expMovingAvg must have exactly one argument that is an object"));
            };
            let two = "$expMovingAvg sub object must have exactly two fields: An 'input' field, and either an 'N' field or an 'alpha' field";
            if d.len() != 2 || !d.contains_key("input") {
                return Err(e9(two));
            }
            let alpha = if let Some(n) = d.get("N") {
                let Some(n) = accum_num::integral(n) else {
                    return Err(e9(format!(
                        "'N' field must be an integer, but found  N: {n}. To use a non-integer, use the 'alpha' argument instead"
                    )));
                };
                if n <= 0 {
                    return Err(e9(format!("'N' must be greater than zero. Got {n}")));
                }
                Dec::from_i128(2).div(&Dec::from_i128(i128::from(n) + 1))
            } else if let Some(a) = d.get("alpha") {
                let f = accum_num::to_f64(a).ok_or_else(|| e9("'alpha' must be a number"))?;
                if f <= 0.0 || f >= 1.0 {
                    return Err(e9(format!(
                        "'alpha' must be between 0 and 1 (exclusive), found alpha: {a}"
                    )));
                }
                Dec::from_bson15(a).unwrap_or(Dec::Nan)
            } else {
                return Err(e9(two));
            };
            if sort.is_none() {
                return Err(e9("$expMovingAvg requires an explicit 'sortBy'"));
            }
            no_window("$expMovingAvg")?;
            Func::Ema {
                input: d.get("input").cloned().unwrap_or(Bson::Null),
                alpha,
            }
        }
        // The argument is one expression; documents where it isn't a pair
        // of numbers are skipped.
        "$covariancePop" | "$covarianceSamp" => Func::Cov {
            pair: arg.clone(),
            samp: op == "$covarianceSamp",
        },
        "$linearFill" => {
            no_window("$linearFill")?;
            if !single_sort {
                return Err(err(
                    605_001,
                    "$linearFill must be specified with a top level sortBy expression with exactly one element",
                ));
            }
            Func::LinearFill(arg.clone())
        }
        "$locf" => {
            no_window("$locf")?;
            Func::Locf(arg.clone())
        }
        _ if kind_of(op).is_some() => Func::Acc(AccSpec::parse(op, arg, Ctx::Window)?),
        _ => {
            let _ = name;
            return Err(e9(format!("Unrecognized window function, {op}")));
        }
    })
}

struct Spec {
    partition: Option<Bson>,
    sort: Option<SortSpec>,
    outputs: Vec<Output>,
}

fn parse(arg: &Bson) -> Result<Spec, MongoError> {
    let Bson::Document(d) = arg else {
        return Err(e9(format!(
            "the $setWindowFields stage specification must be an object, found {}",
            type_name(arg)
        )));
    };
    for k in d.keys() {
        if !matches!(k.as_str(), "partitionBy" | "sortBy" | "output") {
            return Err(err(
                40415,
                format!("BSON field '$setWindowFields.{k}' is an unknown field."),
            ));
        }
    }
    let wrong = |f: &str, v: &Bson| {
        err(
            code::TYPE_MISMATCH,
            format!(
                "BSON field '$setWindowFields.{f}' is the wrong type '{}', expected type 'object'",
                type_name(v)
            ),
        )
    };
    let sort = match d.get("sortBy") {
        None => None,
        Some(Bson::Document(s)) => Some(super::parse_sort(s)?),
        Some(v) => return Err(wrong("sortBy", v)),
    };
    let out = match d.get("output") {
        None => {
            return Err(err(
                40414,
                "BSON field '$setWindowFields.output' is missing but a required field",
            ));
        }
        Some(Bson::Document(o)) => o,
        Some(v) => return Err(wrong("output", v)),
    };
    let mut outputs = Vec::new();
    for (name, f) in out {
        if name.starts_with('$') || name.is_empty() || name.split('.').any(str::is_empty) {
            return Err(err(
                16410,
                format!("FieldPath field names may not start with '$'. Consider using $getField or $setField for a field path with '.' or '$'. :: caused by :: {name}"),
            ));
        }
        let Bson::Document(fd) = f else {
            return Err(e9(format!("The field '{name}' must be an object")));
        };
        let mut func: Option<(&String, &Bson)> = None;
        let mut window = None;
        for (k, v) in fd {
            if k == "window" {
                window = Some(v);
            } else if k.starts_with('$') {
                if func.is_some() {
                    return Err(e9("Cannot specify two functions in window function spec"));
                }
                func = Some((k, v));
            } else {
                return Err(e9(format!("Window function found an unknown argument: {k}")));
            }
        }
        let Some((op, a)) = func else {
            return Err(e9("Expected a $-prefixed window function"));
        };
        let w = window.map(parse_window).transpose()?;
        let func = parse_func(name, op, a, window, sort.as_ref())?;
        if let Some(w) = &w {
            match w {
                Window::Docs(Bound::Unbounded, Bound::Unbounded) => {}
                Window::Docs(..) if sort.is_none() => {
                    return Err(err(5_339_901, "Document-based bounds require a sortBy"));
                }
                Window::Range(..) if sort.as_ref().is_none_or(|s| s.len() != 1) => {
                    return Err(err(
                        5_339_902,
                        "Range-based bounds require sortBy a single field",
                    ));
                }
                _ => {}
            }
        }
        outputs.push(Output {
            path: name.clone(),
            func,
            window: w,
        });
    }
    Ok(Spec {
        partition: d.get("partitionBy").cloned(),
        sort,
        outputs,
    })
}

/// `$setWindowFields`.
///
/// # Errors
/// MongoDB 8.0's parse and run-time errors; 292 past
/// `doc.agg_stage_memory_bytes`.
pub(crate) fn set_window_fields(
    docs: Vec<Document>,
    arg: &Bson,
    lim: &AggLimits,
) -> Result<Vec<Document>, MongoError> {
    let spec = parse(arg)?;
    super::over(lim, &docs, "$setWindowFields")?;
    // Partition keys.
    let mut keyed: Vec<(Bson, Document)> = Vec::with_capacity(docs.len());
    for d in docs {
        let k = match &spec.partition {
            None => Bson::Null,
            Some(p) => {
                let v = eval(p, &Vars::new(&d, lim.now))?;
                if let Bson::Array(_) = v {
                    return Err(err(
                        code::TYPE_MISMATCH,
                        "An expression used to partition cannot evaluate to value of type array",
                    ));
                }
                if missing(&v) { Bson::Null } else { v }
            }
        };
        keyed.push((k, d));
    }
    let sort = spec.sort.clone().unwrap_or_default();
    if spec.partition.is_some() || !sort.is_empty() {
        keyed.sort_by(|a, b| {
            let o = compare(&a.0, &b.0);
            if o != Ordering::Equal {
                return o;
            }
            super::cmp_docs(&a.1, &b.1, &sort)
        });
    }
    let mut out = Vec::with_capacity(keyed.len());
    let mut i = 0;
    while i < keyed.len() {
        let mut j = i + 1;
        while j < keyed.len() && compare(&keyed[i].0, &keyed[j].0) == Ordering::Equal {
            j += 1;
        }
        let part: Vec<Document> = keyed[i..j].iter().map(|x| x.1.clone()).collect();
        let mut results: Vec<Vec<Bson>> = Vec::with_capacity(spec.outputs.len());
        for o in &spec.outputs {
            results.push(run_output(o, &part, &sort, lim)?);
        }
        for (k, mut d) in part.into_iter().enumerate() {
            for (o, r) in spec.outputs.iter().zip(&results) {
                let v = r.get(k).cloned().unwrap_or(Bson::Null);
                let v = if missing(&v) { Bson::Null } else { v };
                crate::update::set_path(&mut d, &o.path, v)?;
            }
            out.push(d);
        }
        i = j;
    }
    Ok(out)
}

/// The sortBy value of each document for range windows and the
/// functions that use positions on the sort axis.
fn axis(part: &[Document], sort: &SortSpec) -> Vec<Bson> {
    let Some((p, dir)) = sort.first() else {
        return vec![Bson::Null; part.len()];
    };
    part.iter().map(|d| sort_key(d, p, *dir)).collect()
}

/// The inclusive index range `[a, b]` of each document's window, `None`
/// for an empty window.
fn windows(
    w: &Window,
    part: &[Document],
    sort: &SortSpec,
) -> Result<Vec<Option<(usize, usize)>>, MongoError> {
    let n = part.len();
    let last = n.saturating_sub(1);
    match w {
        Window::Docs(lo, hi) => Ok((0..n)
            .map(|i| {
                let off = |b: &Bound, unb: i64| -> i64 {
                    match b {
                        Bound::Unbounded => unb,
                        Bound::Current => 0,
                        Bound::Off(v) => accum_num::integral(v).unwrap_or(0),
                    }
                };
                #[allow(clippy::cast_possible_wrap)]
                let ii = i as i64;
                let a = (ii + off(lo, -ii)).max(0);
                #[allow(clippy::cast_possible_wrap)]
                let b = (ii + off(hi, last as i64 - ii)).min(last as i64);
                if a > b || n == 0 {
                    None
                } else {
                    Some((usize::try_from(a).unwrap_or(0), usize::try_from(b).unwrap_or(0)))
                }
            })
            .collect()),
        Window::Range(lo, hi, unit) => {
            let vals = axis(part, sort);
            for v in &vals {
                match (unit, v) {
                    (Some(_), Bson::DateTime(_)) => {}
                    (Some(_), o) => {
                        return Err(err(
                            5_429_513,
                            format!(
                                "Invalid range: Expected the sortBy field to be a Date, but it was {}",
                                type_name(o)
                            ),
                        ));
                    }
                    (None, Bson::DateTime(_)) => {
                        return Err(err(
                            5_429_413,
                            "Invalid range: For windows that involve date or time ranges, a unit must be provided.",
                        ));
                    }
                    (None, o) if !is_num(o) => {
                        return Err(err(
                            5_429_414,
                            format!(
                                "Invalid range: Expected the sortBy field to be a number, but it was {}",
                                if missing(o) { "missing" } else { type_name(o) }
                            ),
                        ));
                    }
                    _ => {}
                }
            }
            let desc = sort.first().is_some_and(|(_, d)| *d < 0);
            let shift = |v: &Bson, b: &Bound| -> Option<Bson> {
                match b {
                    Bound::Unbounded => None,
                    Bound::Current => Some(v.clone()),
                    Bound::Off(o) => match (v, unit) {
                        (Bson::DateTime(t), Some(u)) => {
                            let amt = accum_num::integral(o).unwrap_or(0);
                            date_add(t.timestamp_millis(), amt, u)
                                .map(|ms| Bson::DateTime(bson::DateTime::from_millis(ms)))
                        }
                        _ => varith(v, o, '+'),
                    },
                }
            };
            Ok(vals
                .iter()
                .map(|v| {
                    let lov = shift(v, lo);
                    let hiv = shift(v, hi);
                    let inside = |x: &Bson| {
                        lov.as_ref().is_none_or(|l| compare(x, l) != Ordering::Less)
                            && hiv.as_ref().is_none_or(|h| compare(x, h) != Ordering::Greater)
                    };
                    let mut a = None;
                    let mut b = None;
                    for (k, x) in vals.iter().enumerate() {
                        if inside(x) {
                            if a.is_none() {
                                a = Some(k);
                            }
                            b = Some(k);
                        }
                    }
                    let _ = desc;
                    a.zip(b)
                })
                .collect())
        }
    }
}

fn lower_unbounded(w: &Window) -> bool {
    matches!(
        w,
        Window::Docs(Bound::Unbounded, _) | Window::Range(Bound::Unbounded, _, _)
    )
}

fn mem(lim: &AggLimits, bytes: usize) -> Result<(), MongoError> {
    if bytes > lim.stage_bytes {
        return Err(MongoError::new(
            code::QUERY_EXCEEDED_MEMORY_LIMIT,
            format!(
                "$setWindowFields exceeded memory limit of {} bytes (doc.agg_stage_memory_bytes)",
                lim.stage_bytes
            ),
        ));
    }
    Ok(())
}

fn evals(e: &Bson, part: &[Document], lim: &AggLimits) -> Result<Vec<Bson>, MongoError> {
    part.iter()
        .map(|d| eval(e, &Vars::new(d, lim.now)))
        .collect()
}

#[allow(clippy::too_many_lines)]
fn run_output(
    o: &Output,
    part: &[Document],
    sort: &SortSpec,
    lim: &AggLimits,
) -> Result<Vec<Bson>, MongoError> {
    let n = part.len();
    let whole = Window::Docs(Bound::Unbounded, Bound::Unbounded);
    let w = o.window.as_ref().unwrap_or(&whole);
    match &o.func {
        Func::Acc(spec) => {
            let items: Vec<Item> = part
                .iter()
                .map(|d| spec.item(d, &Vars::new(d, lim.now)))
                .collect::<Result<_, _>>()?;
            let nn = spec.n_for(None, lim)?;
            let wins = windows(w, part, sort)?;
            let narrow = !lower_unbounded(w);
            let mut out = Vec::with_capacity(n);
            if lower_unbounded(w) {
                // Upper bounds only grow: one running state.
                let mut st = spec.state(nn);
                let mut upto = 0usize;
                for win in &wins {
                    if let Some((_, b)) = win {
                        while upto <= *b {
                            st.add(&items[upto])?;
                            upto += 1;
                        }
                    }
                    mem(lim, st.bytes)?;
                    out.push(if win.is_some() {
                        st.result()?
                    } else {
                        spec.state(nn).result()?
                    });
                }
            } else {
                for win in &wins {
                    let mut st: AccState<'_> = spec.state(nn);
                    st.narrow = narrow;
                    if let Some((a, b)) = win {
                        for it in &items[*a..=*b] {
                            st.add(it)?;
                        }
                    }
                    mem(lim, st.bytes)?;
                    out.push(st.result()?);
                }
            }
            Ok(out)
        }
        Func::Rank(kind) => {
            let (p, dir) = sort.first().cloned().unwrap_or_default();
            let keys: Vec<Bson> = part.iter().map(|d| sort_key(d, &p, dir)).collect();
            let mut out = Vec::with_capacity(n);
            let (mut rank, mut dense) = (0i64, 0i64);
            for i in 0..n {
                let same = i > 0 && sort_compare(&keys[i], &keys[i - 1]) == Ordering::Equal;
                if !same {
                    #[allow(clippy::cast_possible_wrap)]
                    {
                        rank = i as i64 + 1;
                    }
                    dense += 1;
                }
                #[allow(clippy::cast_possible_wrap)]
                let v = match kind {
                    0 => rank,
                    1 => dense,
                    _ => i as i64 + 1,
                };
                out.push(i32::try_from(v).map_or(Bson::Int64(v), Bson::Int32));
            }
            Ok(out)
        }
        Func::Shift { output, by, default } => {
            let empty = Document::new();
            let dv = eval(default, &Vars::new(&empty, lim.now))?;
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                #[allow(clippy::cast_possible_wrap)]
                let j = i as i64 + by;
                // A missing value takes the default, as past the ends.
                let v = match usize::try_from(j).ok().filter(|j| *j < n) {
                    Some(j) => eval(output, &Vars::new(&part[j], lim.now))?,
                    None => dv.clone(),
                };
                let v = if missing(&v) { dv.clone() } else { v };
                out.push(if missing(&v) { Bson::Null } else { v });
            }
            Ok(out)
        }
        Func::Derivative { input, unit } => {
            let ys = evals(input, part, lim)?;
            let xs = axis(part, sort);
            let wins = windows(w, part, sort)?;
            let mut out = Vec::with_capacity(n);
            for win in wins {
                let Some((a, b)) = win else {
                    out.push(Bson::Null);
                    continue;
                };
                let x1 = time_axis(&xs[a], unit.as_deref(), "$derivative")?;
                let x2 = time_axis(&xs[b], unit.as_deref(), "$derivative")?;
                for y in [&ys[a], &ys[b]] {
                    if !is_num(y) {
                        return Err(err(
                            5_624_903,
                            "$derivative input must not be null or missing",
                        ));
                    }
                }
                if a == b {
                    out.push(Bson::Null);
                    continue;
                }
                let dy = varith(&ys[b], &ys[a], '-').unwrap_or(Bson::Null);
                let dx = varith(&x2, &x1, '-').unwrap_or(Bson::Null);
                out.push(varith(&dy, &dx, '/').unwrap_or(Bson::Null));
            }
            Ok(out)
        }
        Func::Integral { input, unit } => {
            let ys = evals(input, part, lim)?;
            let xs = axis(part, sort);
            let wins = windows(w, part, sort)?;
            let mut out = Vec::with_capacity(n);
            for win in wins {
                let Some((a, b)) = win else {
                    out.push(Bson::Null);
                    continue;
                };
                let mut pts = Vec::with_capacity(b - a + 1);
                for k in a..=b {
                    let x = time_axis(&xs[k], unit.as_deref(), "$integral")?;
                    if !is_num(&ys[k]) {
                        return Err(err(
                            5_423_900,
                            "The input value of $integral window function must be a vector of 2 value, the first value must be numeric or date type and the second must be numeric.",
                        ));
                    }
                    pts.push((x, ys[k].clone()));
                }
                let mut total = Bson::Int32(0);
                for p in pts.windows(2) {
                    let dx = varith(&p[1].0, &p[0].0, '-').unwrap_or(Bson::Null);
                    let sy = varith(&p[0].1, &p[1].1, '+').unwrap_or(Bson::Null);
                    let area = varith(
                        &varith(&dx, &sy, '*').unwrap_or(Bson::Null),
                        &Bson::Double(2.0),
                        '/',
                    )
                    .unwrap_or(Bson::Null);
                    total = varith(&total, &area, '+').unwrap_or(Bson::Null);
                }
                out.push(total);
            }
            Ok(out)
        }
        // In decimal, as MongoDB computes it; the result is a double
        // until a decimal input is seen.
        Func::Ema { input, alpha } => {
            let ys = evals(input, part, lim)?;
            let one_minus = Dec::from_i128(1).sub(alpha);
            let mut prev: Option<Dec> = None;
            let mut dec = false;
            let mut last = Bson::Null;
            let mut out = Vec::with_capacity(n);
            for y in ys {
                if !is_num(&y) {
                    out.push(last.clone());
                    continue;
                }
                dec |= matches!(y, Bson::Decimal128(_));
                let x = Dec::from_bson15(&y).unwrap_or(Dec::Nan);
                let v = match &prev {
                    None => x,
                    Some(p) => x.mul(alpha).add(&p.mul(&one_minus)),
                };
                last = if dec { v.to_bson() } else { Bson::Double(v.to_f64()) };
                prev = Some(v);
                out.push(last.clone());
            }
            Ok(out)
        }
        Func::Cov { pair, samp } => {
            let pairs = evals(pair, part, lim)?;
            let wins = windows(w, part, sort)?;
            let mut out = Vec::with_capacity(n);
            for win in wins {
                let mut c = Cov::default();
                if let Some((a, b)) = win {
                    for p in &pairs[a..=b] {
                        if let Bson::Array(xy) = p
                            && xy.len() == 2
                        {
                            c.add(&xy[0], &xy[1]);
                        }
                    }
                }
                out.push(c.result(*samp));
            }
            Ok(out)
        }
        Func::LinearFill(e) => {
            let ys = evals(e, part, lim)?;
            let xs = axis(part, sort);
            let mut xv = Vec::with_capacity(n);
            for x in &xs {
                xv.push(match x {
                    Bson::DateTime(t) => Bson::Int64(t.timestamp_millis()),
                    o if is_num(o) => o.clone(),
                    _ => {
                        return Err(err(
                            code::TYPE_MISMATCH,
                            "Value of the sortBy field must be numeric or a date",
                        ));
                    }
                });
            }
            for k in 1..n {
                if compare(&xv[k], &xv[k - 1]) == Ordering::Equal {
                    return Err(err(
                        6_050_106,
                        "There can be no repeated values in the sort field",
                    ));
                }
            }
            for y in &ys {
                if !nullish(y) && !is_num(y) {
                    return Err(err(
                        code::TYPE_MISMATCH,
                        "Value to be filled must be numeric or nullish",
                    ));
                }
            }
            let mut out: Vec<Bson> = ys
                .iter()
                .map(|y| if missing(y) { Bson::Null } else { y.clone() })
                .collect();
            let known: Vec<usize> = (0..n).filter(|k| !nullish(&ys[*k])).collect();
            for pair in known.windows(2) {
                let (a, b) = (pair[0], pair[1]);
                for k in a + 1..b {
                    let dy = varith(&ys[b], &ys[a], '-').unwrap_or(Bson::Null);
                    let dx = varith(&xv[b], &xv[a], '-').unwrap_or(Bson::Null);
                    let off = varith(&xv[k], &xv[a], '-').unwrap_or(Bson::Null);
                    let slope = varith(&dy, &dx, '/').unwrap_or(Bson::Null);
                    let v = varith(&ys[a], &varith(&off, &slope, '*').unwrap_or(Bson::Null), '+');
                    out[k] = v.unwrap_or(Bson::Null);
                }
            }
            Ok(out)
        }
        Func::Locf(e) => {
            let ys = evals(e, part, lim)?;
            let mut last = Bson::Null;
            Ok(ys
                .into_iter()
                .map(|y| {
                    if !nullish(&y) {
                        last = y;
                    }
                    last.clone()
                })
                .collect())
        }
    }
}

/// A sort-axis value as a number: dates in `unit`s when a unit is given.
fn time_axis(x: &Bson, unit: Option<&str>, op: &str) -> Result<Bson, MongoError> {
    let (c_unit, c_date) = if op == "$derivative" {
        (5_624_900, 5_624_901)
    } else {
        (5_423_901, 5_423_902)
    };
    match (x, unit) {
        (Bson::DateTime(t), Some(u)) => {
            #[allow(clippy::cast_precision_loss)]
            let v = t.timestamp_millis() as f64 / unit_ms(u).unwrap_or(1) as f64;
            Ok(Bson::Double(v))
        }
        (Bson::DateTime(_), None) => Err(err(
            c_date,
            format!("{op} where the sortBy is a Date requires an 'unit'"),
        )),
        (_, Some(_)) => Err(err(
            c_unit,
            format!("{op} with 'unit' expects the sortBy field to be a Date"),
        )),
        (o, None) if is_num(o) => Ok(o.clone()),
        (o, None) => Err(err(
            c_date,
            format!(
                "{op} (with no 'unit') expects the sortBy field to be numeric, got {}",
                type_name(o)
            ),
        )),
    }
}

/// Covariance by co-moments (Welford's update) in MongoDB's value
/// arithmetic.
#[derive(Debug)]
struct Cov {
    n: i64,
    mx: Bson,
    my: Bson,
    c: Bson,
}

impl Default for Cov {
    fn default() -> Self {
        Self {
            n: 0,
            mx: Bson::Int32(0),
            my: Bson::Int32(0),
            c: Bson::Int32(0),
        }
    }
}

impl Cov {
    fn add(&mut self, x: &Bson, y: &Bson) {
        if !is_num(x) || !is_num(y) {
            return;
        }
        self.n += 1;
        let k = Bson::Int64(self.n);
        let a = |p: &Bson, q: &Bson, op: char| varith(p, q, op).unwrap_or(Bson::Null);
        let dx = a(x, &self.mx, '-');
        self.mx = a(&self.mx, &a(&dx, &k, '/'), '+');
        let dy = a(y, &self.my, '-');
        self.my = a(&self.my, &a(&dy, &k, '/'), '+');
        self.c = a(&self.c, &a(&dx, &a(y, &self.my, '-'), '*'), '+');
    }

    fn result(&self, samp: bool) -> Bson {
        let d = if samp { self.n - 1 } else { self.n };
        if self.n == 0 || d <= 0 {
            return Bson::Null;
        }
        let r = varith(&self.c, &Bson::Int64(d), '/').unwrap_or(Bson::Null);
        if let Bson::Decimal128(_) = r {
            return r;
        }
        Bson::Double(accum_num::to_f64(&r).unwrap_or(f64::NAN))
    }
}

#[allow(dead_code)]
fn dec_of(v: &Bson) -> Option<Dec> {
    Dec::from_bson(v)
}
