//! Array and set expressions (M1): `$indexOfArray`, `$range`, `$zip`,
//! `$sortArray`, `$firstN`/`$lastN`/`$maxN`/`$minN` as expressions,
//! `$allElementsTrue`, `$anyElementTrue`, `$setDifference`,
//! `$setEquals`, `$setIsSubset`, and `$map`/`$filter`/`$reduce` with
//! MongoDB's error codes and 8.3's `arrayIndexAs` and `$$IDX`.

use std::cmp::Ordering;
use std::collections::HashSet;

use bson::{Bson, Document};
use sprig_proto_mongo::errors::MongoError;

use super::expr_common::{
    err, eval_all, exact_args, integral_i32, integral_i64, is_missing, is_nullish, is_num,
    range_args, show, tname,
};
use super::{Vars, eval, value_truthy};
use crate::keyenc::{compare, eq_key, equal};

fn index_of_array(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let op = "$indexOfArray";
    let a = eval_all(&range_args(op, arg, 2, 4)?, vars)?;
    let arr = match &a[0] {
        Bson::Array(x) => x,
        v if is_nullish(v) => return Ok(Bson::Null),
        v => {
            return Err(err(
                40090,
                format!(
                    "{op} requires an array as a first argument, found: {}",
                    tname(v)
                ),
            ));
        }
    };
    let bound = |i: usize, what: &str| -> Result<Option<usize>, MongoError> {
        let Some(v) = a.get(i) else { return Ok(None) };
        let n = integral_i64(v).ok_or_else(|| {
            err(
                40096,
                format!(
                    "{op}requires an integral {what} index, found a value of type: {}, with value: {}",
                    tname(v),
                    show(v)
                ),
            )
        })?;
        if n < 0 {
            return Err(err(
                40097,
                format!("{op} requires a nonnegative {what} index, found: {n}"),
            ));
        }
        Ok(Some(usize::try_from(n).unwrap_or(usize::MAX)))
    };
    let start = bound(2, "starting")?.unwrap_or(0);
    let end = bound(3, "ending")?.unwrap_or(arr.len()).min(arr.len());
    let found = (start..end).find(|&i| equal(&arr[i], &a[1]));
    Ok(Bson::Int32(
        found.map_or(-1, |i| i32::try_from(i).unwrap_or(i32::MAX)),
    ))
}

fn range(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let a = eval_all(&range_args("$range", arg, 2, 3)?, vars)?;
    let get = |i: usize, what: &str, c_num: i32, c_int: i32| -> Result<i64, MongoError> {
        let v = &a[i];
        if !is_num(v) {
            return Err(err(
                c_num,
                format!(
                    "$range requires a numeric {what} value, found value of type: {}",
                    tname(v)
                ),
            ));
        }
        integral_i32(v).map(i64::from).ok_or_else(|| {
            err(
                c_int,
                format!(
                    "$range requires a{} {what} value that can be represented as a 32-bit integer, found value: {}",
                    if what == "ending" { "n" } else { "" },
                    super::expr_common::fmt_num6(v)
                ),
            )
        })
    };
    let start = get(0, "starting", 34443, 34444)?;
    let end = get(1, "ending", 34445, 34446)?;
    let step = if a.len() == 3 {
        let s = get(2, "step", 34447, 34448)?;
        if s == 0 {
            return Err(err(34449, "$range requires a non-zero step value"));
        }
        s
    } else {
        1
    };
    let mut out = Vec::new();
    let mut i = start;
    while (step > 0 && i < end) || (step < 0 && i > end) {
        #[allow(clippy::cast_possible_truncation)]
        out.push(Bson::Int32(i as i32));
        i += step;
    }
    Ok(Bson::Array(out))
}

fn zip(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let Bson::Document(d) = arg else {
        return Err(err(
            34460,
            format!(
                "$zip only supports an object as an argument, found {}",
                tname(arg)
            ),
        ));
    };
    let mut inputs: Option<&Vec<Bson>> = None;
    let mut longest = false;
    let mut defaults: Option<&Vec<Bson>> = None;
    for (k, v) in d {
        match k.as_str() {
            "inputs" => match v {
                Bson::Array(a) => inputs = Some(a),
                o => {
                    return Err(err(
                        34461,
                        format!("inputs must be an array of expressions, found {}", tname(o)),
                    ));
                }
            },
            "useLongestLength" => match v {
                Bson::Boolean(b) => longest = *b,
                o => {
                    return Err(err(
                        34463,
                        format!("useLongestLength must be a bool, found {}", tname(o)),
                    ));
                }
            },
            "defaults" => match v {
                Bson::Array(a) => defaults = Some(a),
                o => {
                    return Err(err(
                        34462,
                        format!(
                            "defaults must be an array of expressions, found {}",
                            tname(o)
                        ),
                    ));
                }
            },
            _ => return Err(err(34464, format!("$zip found an unknown argument: {k}"))),
        }
    }
    let inputs = inputs
        .filter(|a| !a.is_empty())
        .ok_or_else(|| err(34465, "$zip requires at least one input array"))?;
    if defaults.is_some() && !longest {
        return Err(err(
            34466,
            "cannot specify defaults unless useLongestLength is true",
        ));
    }
    if let Some(dfl) = defaults
        && dfl.len() != inputs.len()
    {
        return Err(err(34467, "defaults and inputs must have the same length"));
    }
    let mut arrays = Vec::new();
    for e in inputs {
        match eval(e, vars)? {
            Bson::Array(a) => arrays.push(a),
            v if is_nullish(&v) => return Ok(Bson::Null),
            v => {
                return Err(err(
                    34468,
                    format!("$zip found a non-array expression in input: {}", show(&v)),
                ));
            }
        }
    }
    let dfl: Vec<Bson> = match defaults {
        Some(d) => d
            .iter()
            .map(|e| eval(e, vars).map(|v| if is_missing(&v) { Bson::Null } else { v }))
            .collect::<Result<_, _>>()?,
        None => vec![Bson::Null; arrays.len()],
    };
    let n = if longest {
        arrays.iter().map(Vec::len).max().unwrap_or(0)
    } else {
        arrays.iter().map(Vec::len).min().unwrap_or(0)
    };
    Ok(Bson::Array(
        (0..n)
            .map(|i| {
                Bson::Array(
                    arrays
                        .iter()
                        .enumerate()
                        .map(|(j, a)| a.get(i).cloned().unwrap_or_else(|| dfl[j].clone()))
                        .collect(),
                )
            })
            .collect(),
    ))
}

fn path_value(v: &Bson, path: &str) -> Bson {
    let mut cur = v;
    for p in path.split('.') {
        match cur {
            Bson::Document(d) => match d.get(p) {
                Some(x) => cur = x,
                None => return Bson::Null,
            },
            _ => return Bson::Null,
        }
    }
    cur.clone()
}

fn sort_dir(v: &Bson) -> Option<i32> {
    match integral_i64(v) {
        Some(1) => Some(1),
        Some(-1) => Some(-1),
        _ => None,
    }
}

fn sort_array(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let Bson::Document(d) = arg else {
        return Err(err(
            2_942_500,
            format!(
                "$sortArray requires an object as an argument, found: {}",
                tname(arg)
            ),
        ));
    };
    for k in d.keys() {
        if k != "input" && k != "sortBy" {
            return Err(err(
                2_942_501,
                format!("$sortArray found an unknown argument: {k}"),
            ));
        }
    }
    let Some(input) = d.get("input") else {
        return Err(err(
            2_942_502,
            "$sortArray requires 'input' to be specified",
        ));
    };
    let Some(sort_by) = d.get("sortBy") else {
        return Err(err(
            2_942_503,
            "$sortArray requires 'sortBy' to be specified",
        ));
    };
    // `None`: the whole element; otherwise field paths.
    let spec: Result<Vec<(String, i32)>, i32> = match sort_by {
        Bson::Document(s) if s.is_empty() => {
            return Err(err(
                2_942_505,
                "BadValue: The sort pattern is empty when it should be a set of fields.",
            ));
        }
        Bson::Document(s) => s
            .iter()
            .map(|(k, v)| {
                sort_dir(v).map(|d| (k.clone(), d)).ok_or_else(|| {
                    err(
                        15975,
                        "$sort key ordering must be 1 (for ascending) or -1 (for descending)",
                    )
                })
            })
            .collect::<Result<_, _>>()
            .map(Ok)?,
        v if is_num(v) => match sort_dir(v) {
            Some(d) => Err(d),
            None => {
                return Err(err(
                    2_942_506,
                    "The $sort element value must be either 1 or -1",
                ));
            }
        },
        _ => {
            return Err(err(
                2_942_507,
                "The $sort is invalid: use 1/-1 to sort the whole element, or {field:1/-1} to sort embedded fields",
            ));
        }
    };
    let mut arr = match eval(input, vars)? {
        Bson::Array(a) => a,
        v if is_nullish(&v) => return Ok(Bson::Null),
        v => {
            return Err(err(
                2_942_504,
                format!(
                    "The input argument to $sortArray must be an array, but was of type: {}",
                    tname(&v)
                ),
            ));
        }
    };
    match spec {
        Err(dir) => arr.sort_by(|a, b| {
            let o = compare(a, b);
            if dir < 0 { o.reverse() } else { o }
        }),
        Ok(fields) => arr.sort_by(|a, b| {
            for (p, dir) in &fields {
                let o = compare(&path_value(a, p), &path_value(b, p));
                let o = if *dir < 0 { o.reverse() } else { o };
                if o != Ordering::Equal {
                    return o;
                }
            }
            Ordering::Equal
        }),
    }
    Ok(Bson::Array(arr))
}

/// MongoDB's checks of the `n` of `$firstN`, `$lastN`, `$maxN`, `$minN`,
/// `$topN` and `$bottomN` (5_787_902, 5_787_903, 5_787_908), as the expression
/// and the accumulator forms share them.
///
/// # Errors
/// The codes above.
pub(crate) fn parse_n(n: &Bson) -> Result<usize, MongoError> {
    if !is_num(n) {
        return Err(err(
            5_787_902,
            format!(
                "Value for 'n' must be of integral type, but found {}",
                if is_missing(n) {
                    "missing".to_owned()
                } else {
                    show(n)
                }
            ),
        ));
    }
    let Some(v) = integral_i64(n) else {
        return Err(err(
            5_787_903,
            format!(
                "Value for 'n' must be of integral type, but found {}",
                super::expr_common::fmt_num6(n)
            ),
        ));
    };
    if v <= 0 {
        return Err(err(
            5_787_908,
            format!("'n' must be greater than 0, found {v}"),
        ));
    }
    Ok(usize::try_from(v).unwrap_or(usize::MAX))
}

fn n_func(op: &str, arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let first_last = op == "$firstN" || op == "$lastN";
    let Bson::Document(d) = arg else {
        return Err(err(
            if first_last { 5_787_801 } else { 5_787_900 },
            format!("specification must be an object; found {op}: {arg}"),
        ));
    };
    for k in d.keys() {
        if k != "n" && k != "input" {
            return Err(err(
                5_787_901,
                format!("Unknown argument for 'n' operator: {k}"),
            ));
        }
    }
    let Some(n) = d.get("n") else {
        return Err(err(5_787_906, "Missing value for 'n'"));
    };
    let Some(input) = d.get("input") else {
        return Err(err(5_787_907, "Missing value for 'input'"));
    };
    let n = parse_n(&eval(n, vars)?)?;
    let Bson::Array(arr) = eval(input, vars)? else {
        return Err(err(5_788_200, "Input must be an array"));
    };
    Ok(Bson::Array(match op {
        "$firstN" => arr.into_iter().take(n).collect(),
        "$lastN" => {
            let skip = arr.len().saturating_sub(n);
            arr.into_iter().skip(skip).collect()
        }
        _ => {
            let mut v: Vec<Bson> = arr.into_iter().filter(|x| !is_nullish(x)).collect();
            if op == "$maxN" {
                v.sort_by(|a, b| compare(b, a));
            } else {
                v.sort_by(compare);
            }
            v.truncate(n);
            v
        }
    }))
}

fn all_any(op: &str, arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let v = eval(&exact_args(op, arg, 1)?[0], vars)?;
    let Bson::Array(a) = v else {
        return Err(err(
            if op == "$allElementsTrue" {
                17040
            } else {
                17041
            },
            format!("{op}'s argument must be an array, but is {}", tname(&v)),
        ));
    };
    Ok(Bson::Boolean(if op == "$allElementsTrue" {
        a.iter().all(value_truthy)
    } else {
        a.iter().any(value_truthy)
    }))
}

fn keys(a: &[Bson]) -> HashSet<Vec<u8>> {
    a.iter().map(eq_key).collect()
}

fn set_difference(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let op = "$setDifference";
    let a = eval_all(&exact_args(op, arg, 2)?, vars)?;
    if is_nullish(&a[0]) || is_nullish(&a[1]) {
        return Ok(Bson::Null);
    }
    let Bson::Array(l) = &a[0] else {
        return Err(err(
            17048,
            format!(
                "both operands of {op} must be arrays. First argument is of type: {}",
                tname(&a[0])
            ),
        ));
    };
    let Bson::Array(r) = &a[1] else {
        return Err(err(
            17049,
            format!(
                "both operands of {op} must be arrays. Second argument is of type: {}",
                tname(&a[1])
            ),
        ));
    };
    let rk = keys(r);
    let mut seen = HashSet::new();
    Ok(Bson::Array(
        l.iter()
            .filter(|x| {
                let k = eq_key(x);
                !rk.contains(&k) && seen.insert(k)
            })
            .cloned()
            .collect(),
    ))
}

fn set_is_subset(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let op = "$setIsSubset";
    let a = eval_all(&exact_args(op, arg, 2)?, vars)?;
    let Bson::Array(l) = &a[0] else {
        return Err(err(
            if is_missing(&a[0]) { 17310 } else { 17046 },
            format!(
                "both operands of {op} must be arrays. First argument is of type: {}",
                tname(&a[0])
            ),
        ));
    };
    let Bson::Array(r) = &a[1] else {
        return Err(err(
            if is_missing(&a[1]) { 17311 } else { 17042 },
            format!(
                "both operands of {op} must be arrays. Second argument is of type: {}",
                tname(&a[1])
            ),
        ));
    };
    let rk = keys(r);
    Ok(Bson::Boolean(l.iter().all(|x| rk.contains(&eq_key(x)))))
}

fn set_equals(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let list = super::expr_common::arg_list(arg);
    if list.len() < 2 {
        return Err(err(
            17045,
            format!(
                "$setEquals needs at least two arguments had: {}",
                list.len()
            ),
        ));
    }
    let evaluated: Vec<Bson> = list
        .into_iter()
        .map(|e| eval(e, vars))
        .collect::<Result<_, _>>()?;
    let mut first: Option<HashSet<Vec<u8>>> = None;
    let mut equal_all = true;
    for (i, v) in evaluated.iter().enumerate() {
        let Bson::Array(a) = v else {
            return Err(err(
                if is_missing(v) { 17044 } else { 5_887_502 },
                format!(
                    "All operands of $setEquals must be arrays. {}-th argument is of type: {}",
                    i + 1,
                    tname(v)
                ),
            ));
        };
        let k = keys(a);
        match &first {
            None => first = Some(k),
            Some(f) => {
                if *f != k {
                    equal_all = false;
                }
            }
        }
    }
    Ok(Bson::Boolean(equal_all))
}

fn obj_arg<'a>(
    op: &str,
    arg: &'a Bson,
    code: i32,
    allowed: &[&str],
    unknown_code: i32,
) -> Result<&'a Document, MongoError> {
    let Bson::Document(d) = arg else {
        return Err(err(
            code,
            format!("{op} only supports an object as its argument"),
        ));
    };
    for k in d.keys() {
        if !allowed.contains(&k.as_str()) {
            return Err(err(
                unknown_code,
                format!("Unrecognized parameter to {op}: {k}"),
            ));
        }
    }
    Ok(d)
}

fn var_name(d: &Document, key: &str, default: &str) -> String {
    d.get_str(key).unwrap_or(default).to_owned()
}

/// `$map`, `$filter` and `$reduce`, each with `arrayIndexAs` (8.3) and
/// `$$IDX`.
fn map_filter_reduce(op: &str, arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let (code, allowed, unknown): (i32, &[&str], i32) = match op {
        "$map" => (16878, &["input", "as", "in", "arrayIndexAs"], 16879),
        "$filter" => (
            28646,
            &["input", "as", "cond", "limit", "arrayIndexAs"],
            28647,
        ),
        _ => (
            40075,
            &["input", "initialValue", "in", "arrayIndexAs"],
            40076,
        ),
    };
    let d = obj_arg(op, arg, code, allowed, unknown)?;
    let Some(input) = d.get("input") else {
        return Err(err(
            match op {
                "$map" => 16880,
                "$filter" => 28648,
                _ => 40077,
            },
            format!("Missing 'input' parameter to {op}"),
        ));
    };
    let body_key = if op == "$filter" { "cond" } else { "in" };
    let Some(body) = d.get(body_key) else {
        return Err(err(
            match op {
                "$map" => 16882,
                "$filter" => 28650,
                _ => 40079,
            },
            format!("Missing '{body_key}' parameter to {op}"),
        ));
    };
    if op == "$reduce" && !d.contains_key("initialValue") {
        return Err(err(40078, "Missing 'initialValue' parameter to $reduce"));
    }
    let name = var_name(d, "as", "this");
    let idx_name = var_name(d, "arrayIndexAs", "IDX");
    let input = eval(input, vars)?;
    let arr = match input {
        Bson::Array(a) => a,
        v if is_nullish(&v) => return Ok(Bson::Null),
        v => {
            return Err(match op {
                "$map" => err(
                    16883,
                    format!("input to $map must be an array not {}", tname(&v)),
                ),
                "$filter" => err(
                    28651,
                    format!("input to $filter must be an array not {}", tname(&v)),
                ),
                _ => err(
                    40080,
                    format!(
                        "$reduce requires that 'input' be an array, found: {}",
                        show(&v)
                    ),
                ),
            });
        }
    };
    let idx = |i: usize| Bson::Int32(i32::try_from(i).unwrap_or(i32::MAX));
    match op {
        "$map" => {
            let mut out = Vec::with_capacity(arr.len());
            for (i, e) in arr.into_iter().enumerate() {
                let v2 = vars.with(&name, e).with(&idx_name, idx(i));
                let r = eval(body, &v2)?;
                out.push(if is_missing(&r) { Bson::Null } else { r });
            }
            Ok(Bson::Array(out))
        }
        "$filter" => {
            let limit = match d.get("limit") {
                None => None,
                Some(l) => match eval(l, vars)? {
                    v if is_nullish(&v) => None,
                    v => {
                        let n = integral_i32(&v).ok_or_else(|| {
                            err(
                                327_391,
                                format!(
                                    "$filter: limit must be represented as a 32-bit integral value: {}",
                                    show(&v)
                                ),
                            )
                        })?;
                        if n <= 0 {
                            return Err(err(
                                327_392,
                                format!("$filter: limit must be greater than 0: {n}"),
                            ));
                        }
                        Some(usize::try_from(n).unwrap_or(usize::MAX))
                    }
                },
            };
            let mut out = Vec::new();
            for (i, e) in arr.into_iter().enumerate() {
                if limit.is_some_and(|l| out.len() >= l) {
                    break;
                }
                let v2 = vars.with(&name, e.clone()).with(&idx_name, idx(i));
                if value_truthy(&eval(body, &v2)?) {
                    out.push(e);
                }
            }
            Ok(Bson::Array(out))
        }
        _ => {
            let mut acc = eval(d.get("initialValue").unwrap_or(&Bson::Null), vars)?;
            for (i, e) in arr.into_iter().enumerate() {
                let v2 = vars
                    .with("this", e)
                    .with("value", acc)
                    .with(&idx_name, idx(i));
                acc = eval(body, &v2)?;
            }
            Ok(acc)
        }
    }
}

/// This module's operators.
pub(super) fn eval_op(op: &str, arg: &Bson, vars: &Vars) -> Option<Result<Bson, MongoError>> {
    Some(match op {
        "$indexOfArray" => index_of_array(arg, vars),
        "$range" => range(arg, vars),
        "$zip" => zip(arg, vars),
        "$sortArray" => sort_array(arg, vars),
        "$firstN" | "$lastN" | "$maxN" | "$minN" => n_func(op, arg, vars),
        "$allElementsTrue" | "$anyElementTrue" => all_any(op, arg, vars),
        "$setDifference" => set_difference(arg, vars),
        "$setIsSubset" => set_is_subset(arg, vars),
        "$setEquals" => set_equals(arg, vars),
        "$map" | "$filter" | "$reduce" => map_filter_reduce(op, arg, vars),
        _ => return None,
    })
}
