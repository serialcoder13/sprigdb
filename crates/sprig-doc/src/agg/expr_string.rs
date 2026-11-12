//! String expressions (M1): `$trim`, `$ltrim`, `$rtrim`, `$regexFind`,
//! `$regexFindAll`, `$regexMatch`, `$replaceOne`, `$replaceAll`,
//! `$indexOfBytes`, `$indexOfCP` and `$strcasecmp`, with MongoDB's error
//! codes and null handling.

use bson::{Bson, Document, doc};
use sprig_proto_mongo::errors::MongoError;

use super::expr_common::{
    err, eval_all, exact_args, fmt_double, integral_i64, is_nullish, range_args, show, tname,
};
use super::{Vars, eval};

const WS: &[char] = &[
    '\0', ' ', '\t', '\n', '\u{0B}', '\u{0C}', '\r', '\u{00A0}', '\u{1680}', '\u{2000}',
    '\u{2001}', '\u{2002}', '\u{2003}', '\u{2004}', '\u{2005}', '\u{2006}', '\u{2007}', '\u{2008}',
    '\u{2009}', '\u{200A}',
];

fn obj<'a>(op: &str, arg: &'a Bson, code: i32, what: &str) -> Result<&'a Document, MongoError> {
    arg.as_document()
        .ok_or_else(|| err(code, format!("{op} {what}{}", tname(arg))))
}

fn trim(op: &str, arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let d = obj(
        op,
        arg,
        50696,
        "only supports an object as an argument, found ",
    )?;
    for k in d.keys() {
        if k != "input" && k != "chars" {
            return Err(err(50694, format!("{op} found an unknown argument: {k}")));
        }
    }
    let Some(input) = d.get("input") else {
        return Err(err(50695, format!("{op} requires an 'input' field")));
    };
    let input = eval(input, vars)?;
    let s = match &input {
        Bson::String(s) => s.clone(),
        v if is_nullish(v) => return Ok(Bson::Null),
        v => {
            return Err(err(
                50699,
                format!(
                    "{op} requires its input to be a string, got {} (of type {}) instead.",
                    show(v),
                    tname(v)
                ),
            ));
        }
    };
    let chars: Vec<char> = match d.get("chars") {
        None => WS.to_vec(),
        Some(c) => match eval(c, vars)? {
            Bson::String(c) => c.chars().collect(),
            v if is_nullish(&v) => return Ok(Bson::Null),
            v => {
                return Err(err(
                    50700,
                    format!(
                        "{op} requires 'chars' to be a string, got {} (of type {}) instead.",
                        show(&v),
                        tname(&v)
                    ),
                ));
            }
        },
    };
    let f = |c: char| chars.contains(&c);
    Ok(Bson::String(
        match op {
            "$trim" => s.trim_matches(f),
            "$ltrim" => s.trim_start_matches(f),
            _ => s.trim_end_matches(f),
        }
        .to_owned(),
    ))
}

/// What a regex operator returns when there is nothing to match.
fn regex_none(op: &str) -> Bson {
    match op {
        "$regexFind" => Bson::Null,
        "$regexFindAll" => Bson::Array(Vec::new()),
        _ => Bson::Boolean(false),
    }
}

fn regex(op: &str, arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let d = obj(
        op,
        arg,
        51103,
        "expects an object of named arguments but found: ",
    )?;
    for k in d.keys() {
        if !matches!(k.as_str(), "input" | "regex" | "options") {
            return Err(err(31024, format!("{op} found an unknown argument: {k}")));
        }
    }
    let Some(input) = d.get("input") else {
        return Err(err(31022, format!("{op} requires 'input' parameter")));
    };
    let Some(re) = d.get("regex") else {
        return Err(err(31023, format!("{op} requires 'regex' parameter")));
    };
    let re = eval(re, vars)?;
    let opts = match d.get("options") {
        None => None,
        Some(o) => match eval(o, vars)? {
            Bson::String(s) => Some(s),
            v if is_nullish(&v) => None,
            _ => {
                return Err(err(
                    51106,
                    format!("{op} needs 'options' to be of type string"),
                ));
            }
        },
    };
    let (pattern, options) = match &re {
        Bson::String(s) => (s.clone(), opts.unwrap_or_default()),
        Bson::RegularExpression(r) => {
            if !r.options.is_empty() && opts.as_ref().is_some_and(|o| !o.is_empty()) {
                return Err(err(
                    51107,
                    format!(
                        "{op}: found regex option(s) specified in both 'regex' and 'option' fields"
                    ),
                ));
            }
            (
                r.pattern.clone(),
                opts.filter(|o| !o.is_empty())
                    .unwrap_or_else(|| r.options.clone()),
            )
        }
        v if is_nullish(v) => (String::new(), String::new()),
        _ => {
            return Err(err(
                51105,
                format!("{op} needs 'regex' to be of type string or regex"),
            ));
        }
    };
    if let Some(c) = options.chars().find(|c| !"imsxu".contains(*c)) {
        return Err(err(
            51108,
            format!("{op} invalid flag in regex options: {c}"),
        ));
    }
    if pattern.contains('\0') {
        return Err(err(
            51109,
            format!("{op} needs 'regex' pattern to be free of null characters"),
        ));
    }
    let input = eval(input, vars)?;
    if is_nullish(&re) {
        if !is_nullish(&input) && !matches!(input, Bson::String(_)) {
            return Err(err(
                51104,
                format!("{op} needs 'input' to be of type string"),
            ));
        }
        return Ok(regex_none(op));
    }
    let s = match &input {
        Bson::String(s) => s.clone(),
        v if is_nullish(v) => return Ok(regex_none(op)),
        _ => {
            return Err(err(
                51104,
                format!("{op} needs 'input' to be of type string"),
            ));
        }
    };
    let compiled = crate::filter::Re::new(&pattern, &options)
        .map_err(|e| err(51111, format!("Invalid Regex in {op}: {}", e.message)))?;
    let re = &compiled.re;
    if op == "$regexMatch" {
        return Ok(Bson::Boolean(re.is_match(&s)));
    }
    let cp_index = |byte: usize| s[..byte].chars().count();
    let one = |c: &regex::Captures<'_>| -> Bson {
        let m = c.get(0).map_or("", |m| m.as_str());
        let start = c.get(0).map_or(0, |m| m.start());
        let caps: Vec<Bson> = (1..c.len())
            .map(|i| {
                c.get(i)
                    .map_or(Bson::Null, |g| Bson::String(g.as_str().to_owned()))
            })
            .collect();
        Bson::Document(doc! {
            "match": m,
            "idx": i32::try_from(cp_index(start)).unwrap_or(i32::MAX),
            "captures": caps,
        })
    };
    if op == "$regexFind" {
        return Ok(re.captures(&s).map_or(Bson::Null, |c| one(&c)));
    }
    let mut out = Vec::new();
    for c in re.captures_iter(&s) {
        let m = c.get(0);
        // MongoDB stops at the end of a non-empty input.
        if m.is_some_and(|m| m.as_str().is_empty() && m.start() == s.len() && !s.is_empty()) {
            break;
        }
        out.push(one(&c));
    }
    Ok(Bson::Array(out))
}

fn replace(op: &str, arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let d = obj(op, arg, 51751, "requires an object as an argument, found: ")?;
    for k in d.keys() {
        if !matches!(k.as_str(), "input" | "find" | "replacement") {
            return Err(err(51750, format!("{op} found an unknown argument: {k}")));
        }
    }
    let mut fields = Vec::new();
    for (name, code) in [("input", 51749), ("find", 51748), ("replacement", 51747)] {
        let Some(e) = d.get(name) else {
            return Err(err(code, format!("{op} requires '{name}' to be specified")));
        };
        fields.push((name, e));
    }
    let mut got = Vec::new();
    for (i, (name, e)) in fields.into_iter().enumerate() {
        let v = eval(e, vars)?;
        if !is_nullish(&v) && !matches!(v, Bson::String(_)) {
            return Err(err(
                [51746, 51745, 51744][i],
                format!(
                    "{op} requires that '{name}' be a string, found: {}",
                    show(&v)
                ),
            ));
        }
        got.push(v);
    }
    let (Bson::String(s), Bson::String(f), Bson::String(r)) = (&got[0], &got[1], &got[2]) else {
        return Ok(Bson::Null);
    };
    Ok(Bson::String(if op == "$replaceOne" {
        s.replacen(f.as_str(), r, 1)
    } else {
        s.replace(f.as_str(), r)
    }))
}

fn index_of(op: &str, arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let a = eval_all(&range_args(op, arg, 2, 4)?, vars)?;
    let cp = op == "$indexOfCP";
    let s = match &a[0] {
        Bson::String(s) => s.clone(),
        v if is_nullish(v) => return Ok(Bson::Null),
        v => {
            return Err(err(
                if cp { 40093 } else { 40091 },
                format!(
                    "{op} requires a string as the first argument, found: {}",
                    tname(v)
                ),
            ));
        }
    };
    let Bson::String(tok) = &a[1] else {
        return Err(err(
            if cp { 40094 } else { 40092 },
            format!(
                "{op} requires a string as the second argument, found: {}",
                tname(&a[1])
            ),
        ));
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
    let end_arg = bound(3, "ending")?;
    let found = if cp {
        let chars: Vec<char> = s.chars().collect();
        let t: Vec<char> = tok.chars().collect();
        let end = end_arg.unwrap_or(chars.len()).min(chars.len());
        (start..end).find(|&i| chars[i..].starts_with(&t))
    } else {
        let b = s.as_bytes();
        let t = tok.as_bytes();
        if start > b.len() {
            None
        } else {
            let end = end_arg.unwrap_or(b.len()).min(b.len());
            (start..=end.saturating_sub(t.len()))
                .find(|&i| i + t.len() <= end && b[i..].starts_with(t))
        }
    };
    Ok(Bson::Int32(
        found.map_or(-1, |i| i32::try_from(i).unwrap_or(i32::MAX)),
    ))
}

/// A value as `$concat`-like coercions write it (`$strcasecmp`).
pub(super) fn coerce_to_string(v: &Bson) -> Result<String, MongoError> {
    Ok(match v {
        Bson::String(s) | Bson::Symbol(s) | Bson::JavaScriptCode(s) => s.clone(),
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        Bson::Double(f) => fmt_double(*f),
        Bson::Decimal128(d) => d.to_string(),
        Bson::DateTime(d) => crate::json::iso_from_ms(d.timestamp_millis()).unwrap_or_default(),
        Bson::Timestamp(t) => format!("{} {}", t.time, t.increment),
        Bson::Null | Bson::Undefined => String::new(),
        other => {
            return Err(err(
                16007,
                format!("can't convert from BSON type {} to String", tname(other)),
            ));
        }
    })
}

fn strcasecmp(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let a = eval_all(&exact_args("$strcasecmp", arg, 2)?, vars)?;
    let x = coerce_to_string(&a[0])?.to_ascii_lowercase();
    let y = coerce_to_string(&a[1])?.to_ascii_lowercase();
    Ok(Bson::Int32(match x.as_bytes().cmp(y.as_bytes()) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }))
}

/// This module's operators.
pub(super) fn eval_op(op: &str, arg: &Bson, vars: &Vars) -> Option<Result<Bson, MongoError>> {
    Some(match op {
        "$trim" | "$ltrim" | "$rtrim" => trim(op, arg, vars),
        "$regexFind" | "$regexFindAll" | "$regexMatch" => regex(op, arg, vars),
        "$replaceOne" | "$replaceAll" => replace(op, arg, vars),
        "$indexOfBytes" | "$indexOfCP" => index_of(op, arg, vars),
        "$strcasecmp" => strcasecmp(arg, vars),
        _ => return None,
    })
}
