//! Shared pieces of the aggregation expressions added in M1 (12 Nov
//! 2026): argument handling, MongoDB's type names, number formatting and
//! conversions, and the dispatch from [`super::op_eval`].

use bson::{Bson, Decimal128};
use sprig_proto_mongo::errors::MongoError;

use super::{Vars, eval};

/// An error with a code.
pub(super) fn err(code: i32, msg: impl Into<String>) -> MongoError {
    MongoError::new(code, msg)
}

/// Missing (the evaluator's `Undefined`).
pub(super) fn is_missing(v: &Bson) -> bool {
    matches!(v, Bson::Undefined)
}

/// Null or missing.
pub(super) fn is_nullish(v: &Bson) -> bool {
    matches!(v, Bson::Null | Bson::Undefined)
}

/// A number of any BSON numeric type.
pub(super) fn is_num(v: &Bson) -> bool {
    matches!(
        v,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_)
    )
}

/// MongoDB's name of a value's type (`missing` for a missing value).
pub(super) fn tname(v: &Bson) -> &'static str {
    match v {
        Bson::Undefined => "missing",
        Bson::MinKey => "minKey",
        Bson::MaxKey => "maxKey",
        Bson::JavaScriptCode(_) => "javascript",
        Bson::JavaScriptCodeWithScope(_) => "javascriptWithScope",
        Bson::Symbol(_) => "symbol",
        Bson::DbPointer(_) => "dbPointer",
        other => crate::update::type_name(other),
    }
}

/// The argument list of an operator: an array is the list, anything else
/// one argument.
pub(super) fn arg_list(arg: &Bson) -> Vec<&Bson> {
    match arg {
        Bson::Array(a) => a.iter().collect(),
        other => vec![other],
    }
}

/// 16020: exactly `n` arguments.
pub(super) fn exact_args(op: &str, arg: &Bson, n: usize) -> Result<Vec<Bson>, MongoError> {
    let list = arg_list(arg);
    if list.len() != n {
        return Err(err(
            16020,
            format!(
                "Expression {op} takes exactly {n} arguments. {} were passed in.",
                list.len()
            ),
        ));
    }
    Ok(list.into_iter().cloned().collect())
}

/// 28667: between `lo` and `hi` arguments.
pub(super) fn range_args(
    op: &str,
    arg: &Bson,
    lo: usize,
    hi: usize,
) -> Result<Vec<Bson>, MongoError> {
    let list = arg_list(arg);
    if list.len() < lo || list.len() > hi {
        return Err(err(
            28667,
            format!(
                "Expression {op} takes at least {lo} arguments, and at most {hi}, but {} were passed in.",
                list.len()
            ),
        ));
    }
    Ok(list.into_iter().cloned().collect())
}

/// Evaluates every argument.
pub(super) fn eval_all(list: &[Bson], vars: &Vars) -> Result<Vec<Bson>, MongoError> {
    list.iter().map(|a| eval(a, vars)).collect()
}

/// Evaluates the single argument of a one-argument operator.
pub(super) fn one(op: &str, arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let a = exact_args(op, arg, 1)?;
    eval(&a[0], vars)
}

/// A numeric value as `f64`.
pub(super) fn to_f64(v: &Bson) -> f64 {
    match v {
        Bson::Int32(i) => f64::from(*i),
        #[allow(clippy::cast_precision_loss)]
        Bson::Int64(i) => *i as f64,
        Bson::Double(f) => *f,
        Bson::Decimal128(d) => dec_to_f64(d),
        _ => f64::NAN,
    }
}

/// A decimal as the nearest double.
pub(super) fn dec_to_f64(d: &Decimal128) -> f64 {
    let s = d.to_string();
    match s.as_str() {
        "NaN" | "-NaN" => f64::NAN,
        "Infinity" => f64::INFINITY,
        "-Infinity" => f64::NEG_INFINITY,
        _ => s.parse().unwrap_or(f64::NAN),
    }
}

/// A double as a decimal, the way MongoDB converts one: rounded to 15
/// significant digits, trailing zeros kept (`2.5` → `2.50000000000000`).
pub(super) fn dec_from_f64(f: f64) -> Decimal128 {
    let s = if f.is_nan() {
        "NaN".to_owned()
    } else if f.is_infinite() {
        if f > 0.0 { "Infinity" } else { "-Infinity" }.to_owned()
    } else if f == 0.0 {
        if f.is_sign_negative() { "-0" } else { "0" }.to_owned()
    } else {
        // `{:.14e}` gives 15 significant digits: d.dddddddddddddde±x.
        format!("{f:.14e}").replace('e', "E")
    };
    s.parse().unwrap_or_else(|_| dec_parse("NaN"))
}

/// A decimal from its string form.
pub(super) fn dec_parse(s: &str) -> Decimal128 {
    s.parse().unwrap_or_else(|_| {
        "NaN"
            .parse()
            .unwrap_or_else(|_| Decimal128::from_bytes([0; 16]))
    })
}

/// A decimal from an exact integer.
pub(super) fn dec_from_i64(i: i64) -> Decimal128 {
    dec_parse(&i.to_string())
}

/// C's `%.{p}g`.
pub(super) fn fmt_g(f: f64, p: usize) -> String {
    if f.is_nan() {
        return "nan".to_owned();
    }
    if f.is_infinite() {
        return if f > 0.0 { "inf" } else { "-inf" }.to_owned();
    }
    if f == 0.0 {
        return if f.is_sign_negative() { "-0" } else { "0" }.to_owned();
    }
    let p = p.max(1);
    let sci = format!("{:.*e}", p - 1, f);
    let (mant, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let strip = |s: &str| -> String {
        if s.contains('.') {
            s.trim_end_matches('0').trim_end_matches('.').to_owned()
        } else {
            s.to_owned()
        }
    };
    if exp < -4 || exp >= i32::try_from(p).unwrap_or(i32::MAX) {
        format!(
            "{}e{}{:02}",
            strip(mant),
            if exp < 0 { '-' } else { '+' },
            exp.unsigned_abs()
        )
    } else {
        let decimals = usize::try_from(i32::try_from(p).unwrap_or(17) - 1 - exp).unwrap_or(0);
        strip(&format!("{f:.decimals$}"))
    }
}

/// A double the way C++ streams print one by default (`%g`, 6 digits),
/// as MongoDB's error messages do.
pub(super) fn fmt_num6(v: &Bson) -> String {
    match v {
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        Bson::Decimal128(d) => d.to_string(),
        other => fmt_g(to_f64(other), 6),
    }
}

/// A double as `$toString` writes one: the shortest digits that read
/// back the same, in exponent form below 1e-4 and from 1e16 up
/// (`1e-05`, `1000000000000000`, `1e+16`).
pub(super) fn fmt_double(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_owned();
    }
    if f.is_infinite() {
        return if f > 0.0 { "Infinity" } else { "-Infinity" }.to_owned();
    }
    if f == 0.0 {
        return if f.is_sign_negative() { "-0" } else { "0" }.to_owned();
    }
    let sci = format!("{f:e}");
    let (mant, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let neg = mant.starts_with('-');
    let digits: String = mant.chars().filter(char::is_ascii_digit).collect();
    let sign = if neg { "-" } else { "" };
    if !(-4..16).contains(&exp) {
        let m = if digits.len() > 1 {
            format!("{}.{}", &digits[..1], &digits[1..])
        } else {
            digits
        };
        return format!(
            "{sign}{m}e{}{:02}",
            if exp < 0 { '-' } else { '+' },
            exp.unsigned_abs()
        );
    }
    let n = i32::try_from(digits.len()).unwrap_or(0);
    if exp < 0 {
        let zeros = "0".repeat(usize::try_from(-exp - 1).unwrap_or(0));
        format!("{sign}0.{zeros}{digits}")
    } else if exp + 1 >= n {
        let zeros = "0".repeat(usize::try_from(exp + 1 - n).unwrap_or(0));
        format!("{sign}{digits}{zeros}")
    } else {
        let k = usize::try_from(exp + 1).unwrap_or(0);
        format!("{sign}{}.{}", &digits[..k], &digits[k..])
    }
}

/// A value as MongoDB writes it inside an error message (`5`, `"a"`,
/// `null`, `1.5`).
pub(super) fn show(v: &Bson) -> String {
    match v {
        Bson::String(s) => format!("\"{s}\""),
        Bson::Double(f) => fmt_g(*f, 6),
        Bson::Undefined => "missing".to_owned(),
        other => other.to_string(),
    }
}

/// The int64 value of an integral number, if it has one (`2.0` counts).
pub(super) fn integral_i64(v: &Bson) -> Option<i64> {
    match v {
        Bson::Int32(i) => Some(i64::from(*i)),
        Bson::Int64(i) => Some(*i),
        #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
        Bson::Double(f)
            if f.fract() == 0.0
                && *f >= -9.223_372_036_854_776e18
                && *f < 9.223_372_036_854_776e18 =>
        {
            Some(*f as i64)
        }
        Bson::Decimal128(d) => {
            let f = dec_to_f64(d);
            #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
            if f.fract() == 0.0 && f.abs() < 9.2e18 {
                d.to_string().parse::<i64>().ok().or(Some(f as i64))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// The int32 value of an integral number, if it has one.
pub(super) fn integral_i32(v: &Bson) -> Option<i32> {
    integral_i64(v).and_then(|i| i32::try_from(i).ok())
}

/// The operators of this module set, evaluated; `None` for every other
/// operator (the older evaluator in [`super::op_eval`] handles those).
pub(super) fn dispatch(op: &str, arg: &Bson, vars: &Vars) -> Option<Result<Bson, MongoError>> {
    if let Some(r) = super::expr_math::eval_op(op, arg, vars) {
        return Some(r);
    }
    if let Some(r) = super::expr_string::eval_op(op, arg, vars) {
        return Some(r);
    }
    if let Some(r) = super::expr_array::eval_op(op, arg, vars) {
        return Some(r);
    }
    if let Some(r) = super::expr_date::eval_op(op, arg, vars) {
        return Some(r);
    }
    super::expr_type::eval_op(op, arg, vars)
}
