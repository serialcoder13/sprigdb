//! `$densify` and `$fill` (M2, 12 Nov 2026), with MongoDB 8.0's errors.
//!
//! `$densify` follows MongoDB's streaming order: `bounds: "full"` sorts by
//! the field alone and each partition's sequence starts at the global
//! minimum; `"partition"` and explicit bounds sort by the partition
//! fields, then the field. Generated values step from the previous one
//! (months clamp to the month's end, as `$dateAdd` does). `$fill` is
//! `$setWindowFields` with `$locf`/`$linearFill`, then `$addFields` with
//! `$ifNull` for constant values, as MongoDB rewrites it.

use std::cmp::Ordering;

use bson::{Bson, DateTime, Document, doc};
use sprig_proto_mongo::errors::{MongoError, code};

use super::accum_num::{is_num, varith};
use super::window::{date_add, set_window_fields};
use super::{AggLimits, run_stage};
use crate::keyenc::compare;
use crate::update::{get_path, set_path, type_name};

fn err(c: i32, msg: impl Into<String>) -> MongoError {
    MongoError::new(c, msg)
}

fn missing_field(f: &str) -> MongoError {
    err(
        40414,
        format!("BSON field '{f}' is missing but a required field"),
    )
}

fn field_path_ok(f: &str) -> Result<(), MongoError> {
    if f.is_empty() || f.starts_with('$') || f.split('.').any(str::is_empty) {
        return Err(err(
            16410,
            format!(
                "FieldPath field names may not start with '$'. Consider using $getField or $setField for a field path with '.' or '$'. :: caused by :: {f}"
            ),
        ));
    }
    Ok(())
}

#[allow(clippy::large_enum_variant)]
enum Bounds {
    Full,
    Partition,
    Range(Bson, Bson),
}

struct Densify {
    field: String,
    parts: Vec<String>,
    step: Bson,
    unit: Option<String>,
    bounds: Bounds,
}

fn parse_densify(arg: &Bson) -> Result<Densify, MongoError> {
    let Bson::Document(d) = arg else {
        return Err(err(
            code::TYPE_MISMATCH,
            "the $densify stage specification must be an object",
        ));
    };
    for k in d.keys() {
        if !matches!(k.as_str(), "field" | "partitionByFields" | "range") {
            return Err(err(
                40415,
                format!("BSON field '$densify.{k}' is an unknown field."),
            ));
        }
    }
    let field = match d.get("field") {
        None => return Err(missing_field("$densify.field")),
        Some(Bson::String(s)) => s.clone(),
        Some(v) => {
            return Err(err(
                code::TYPE_MISMATCH,
                format!(
                    "BSON field '$densify.field' is the wrong type '{}', expected type 'string'",
                    type_name(v)
                ),
            ));
        }
    };
    field_path_ok(&field)?;
    let Some(Bson::Document(r)) = d.get("range") else {
        return Err(missing_field("$densify.range"));
    };
    for k in r.keys() {
        if !matches!(k.as_str(), "step" | "bounds" | "unit") {
            return Err(err(
                40415,
                format!("BSON field '$densify.range.{k}' is an unknown field."),
            ));
        }
    }
    let step = r
        .get("step")
        .cloned()
        .ok_or_else(|| missing_field("$densify.range.step"))?;
    let bounds = r
        .get("bounds")
        .ok_or_else(|| missing_field("$densify.range.bounds"))?;
    if !is_num(&step) || compare(&step, &Bson::Int32(0)) != Ordering::Greater {
        return Err(err(
            5_733_401,
            "The step parameter in a range statement must be a strictly positive numeric value",
        ));
    }
    let unit = match r.get("unit") {
        None => None,
        Some(Bson::String(u)) => Some(u.clone()),
        Some(_) => return Err(err(code::TYPE_MISMATCH, "'unit' must be a string")),
    };
    let bounds = match bounds {
        Bson::String(s) if s == "full" => Bounds::Full,
        Bson::String(s) if s == "partition" => Bounds::Partition,
        Bson::String(_) => {
            return Err(err(
                5_946_802,
                "Bounds string must either be 'full' or 'partition'",
            ));
        }
        Bson::Array(a)
            if a.len() == 2
                && ((is_num(&a[0]) && is_num(&a[1]))
                    || (matches!(a[0], Bson::DateTime(_))
                        && matches!(a[1], Bson::DateTime(_))))
                && compare(&a[0], &a[1]) != Ordering::Greater =>
        {
            if matches!(a[0], Bson::DateTime(_)) != unit.is_some() {
                return Err(err(
                    5_733_409,
                    "A bounding array of dates requires a unit, and numeric bounds can't have one",
                ));
            }
            Bounds::Range(a[0].clone(), a[1].clone())
        }
        _ => {
            return Err(err(
                5_733_402,
                "A bounding array must be an ascending array of either two dates or two numbers",
            ));
        }
    };
    let parts: Vec<String> = match d.get("partitionByFields") {
        None => Vec::new(),
        Some(Bson::Array(a)) => a
            .iter()
            .map(|x| match x {
                Bson::String(s) => {
                    field_path_ok(s)?;
                    Ok(s.clone())
                }
                _ => Err(err(
                    code::TYPE_MISMATCH,
                    "partitionByFields must be strings",
                )),
            })
            .collect::<Result<_, _>>()?,
        Some(_) => {
            return Err(err(
                code::TYPE_MISMATCH,
                "partitionByFields must be an array",
            ));
        }
    };
    for p in &parts {
        if p == &field || field.starts_with(&format!("{p}.")) || p.starts_with(&format!("{field}."))
        {
            return Err(err(
                8_993_000,
                "partitionByFields cannot include field that is being densified",
            ));
        }
    }
    if matches!(bounds, Bounds::Partition) && parts.is_empty() {
        // MongoDB treats "partition" without partition fields as one
        // partition: the whole input.
    }
    Ok(Densify {
        field,
        parts,
        step,
        unit,
        bounds,
    })
}

impl Densify {
    fn next(&self, v: &Bson) -> Option<Bson> {
        match (v, &self.unit) {
            (Bson::DateTime(t), Some(u)) => {
                let amt = super::accum_num::integral(&self.step)?;
                date_add(t.timestamp_millis(), amt, u)
                    .map(|ms| Bson::DateTime(DateTime::from_millis(ms)))
            }
            _ => varith(v, &self.step, '+'),
        }
    }

    fn check(&self, v: &Bson) -> Result<(), MongoError> {
        match (v, &self.unit) {
            (Bson::DateTime(_), Some(_)) => Ok(()),
            (Bson::DateTime(_), None) => Err(err(
                6_053_600,
                "Encountered date densify value in collection when step does not have a date unit.",
            )),
            (x, Some(_)) if is_num(x) => Err(err(
                6_053_600,
                "Encountered numeric densify value in collection when step has a date unit.",
            )),
            (x, None) if is_num(x) => Ok(()),
            (x, _) => Err(err(
                5_733_201,
                format!(
                    "Densify field type must be numeric or a date, but found {}",
                    type_name(x)
                ),
            )),
        }
    }

    /// A generated document: the partition's fields, then the value.
    fn make(&self, part: &Document, v: &Bson) -> Result<Document, MongoError> {
        let mut d = part.clone();
        set_path(&mut d, &self.field, v.clone())?;
        Ok(d)
    }
}

/// The partition fields of `d` as a document (absent fields left out).
fn part_of(d: &Document, parts: &[String]) -> Result<Document, MongoError> {
    let mut out = Document::new();
    for p in parts {
        if let Some(v) = get_path(d, p) {
            set_path(&mut out, p, v.clone())?;
        }
    }
    Ok(out)
}

struct PState {
    part: Document,
    /// The next value to generate; `None` before the partition starts.
    cur: Option<Bson>,
    /// Full bounds: the partition began above the minimum and generates
    /// from it at its next document.
    pending: bool,
}

/// `$densify`.
///
/// # Errors
/// MongoDB 8.0's parse and run-time errors.
#[allow(clippy::too_many_lines)]
pub(crate) fn densify(
    docs: Vec<Document>,
    arg: &Bson,
    lim: &AggLimits,
) -> Result<Vec<Document>, MongoError> {
    let spec = parse_densify(arg)?;
    super::over(lim, &docs, "$densify")?;
    let val = |d: &Document| get_path(d, &spec.field).cloned().unwrap_or(Bson::Null);
    for d in &docs {
        let v = val(d);
        if !matches!(v, Bson::Null) {
            spec.check(&v)?;
        }
    }
    let full = matches!(spec.bounds, Bounds::Full);
    let mut items: Vec<(Document, Document)> = docs
        .into_iter()
        .map(|d| Ok((part_of(&d, &spec.parts)?, d)))
        .collect::<Result<_, MongoError>>()?;
    items.sort_by(|a, b| {
        if !full {
            let o = compare(&Bson::Document(a.0.clone()), &Bson::Document(b.0.clone()));
            if o != Ordering::Equal {
                return o;
            }
        }
        compare(&val(&a.1), &val(&b.1))
    });
    let (gmin, gmax) = {
        let vs: Vec<Bson> = items
            .iter()
            .map(|x| val(&x.1))
            .filter(|v| !matches!(v, Bson::Null))
            .collect();
        (vs.first().cloned(), vs.last().cloned())
    };
    let (lo, hi, hi_inclusive) = match &spec.bounds {
        Bounds::Full => (gmin.clone(), gmax.clone(), true),
        Bounds::Partition => (None, None, true),
        Bounds::Range(a, b) => (Some(a.clone()), Some(b.clone()), false),
    };
    let below_hi = |v: &Bson, hi: &Bson| {
        let o = compare(v, hi);
        o == Ordering::Less || (hi_inclusive && o == Ordering::Equal)
    };
    let mut states: Vec<PState> = Vec::new();
    let mut out: Vec<Document> = Vec::new();
    let limit = lim.stage_bytes / 64;
    for (p, d) in items {
        let v = val(&d);
        if matches!(v, Bson::Null) {
            out.push(d);
            continue;
        }
        let si = if let Some(i) = states.iter().position(|s| s.part == p) {
            i
        } else {
            states.push(PState {
                part: p.clone(),
                cur: None,
                pending: false,
            });
            states.len() - 1
        };
        let st = &mut states[si];
        // Outside explicit bounds: passed through.
        if let (Bounds::Range(a, _), true) = (&spec.bounds, true)
            && compare(&v, a) == Ordering::Less
        {
            out.push(d);
            continue;
        }
        if st.cur.is_none() {
            match &spec.bounds {
                Bounds::Partition => st.cur = Some(v.clone()),
                Bounds::Full => {
                    st.cur.clone_from(&lo);
                    if compare(&v, lo.as_ref().unwrap_or(&v)) != Ordering::Equal {
                        st.pending = true;
                        out.push(d);
                        continue;
                    }
                }
                Bounds::Range(a, _) => st.cur = Some(a.clone()),
            }
        }
        st.pending = false;
        while let Some(c) = st.cur.clone() {
            if compare(&c, &v) != Ordering::Less {
                break;
            }
            if let Some(h) = &hi
                && !below_hi(&c, h)
            {
                break;
            }
            out.push(spec.make(&st.part, &c)?);
            if out.len() > limit.max(1_000_000) {
                return Err(err(5_897_900, "Generated too many documents in $densify"));
            }
            st.cur = spec.next(&c);
        }
        if let Some(c) = &st.cur
            && compare(c, &v) == Ordering::Equal
        {
            st.cur = spec.next(c);
        }
        out.push(d);
    }
    // The rest of each partition's range.
    if let Some(h) = &hi {
        for st in &mut states {
            if st.cur.is_none() {
                continue;
            }
            while let Some(c) = st.cur.clone() {
                if !below_hi(&c, h) {
                    break;
                }
                out.push(spec.make(&st.part, &c)?);
                st.cur = spec.next(&c);
            }
        }
    }
    Ok(out)
}

/// `$fill`.
///
/// # Errors
/// MongoDB 8.0's parse errors; those of the stages it becomes.
pub(crate) fn fill(
    docs: Vec<Document>,
    arg: &Bson,
    lim: &AggLimits,
) -> Result<Vec<Document>, MongoError> {
    let Bson::Document(d) = arg else {
        return Err(err(
            code::TYPE_MISMATCH,
            "the $fill stage specification must be an object",
        ));
    };
    for k in d.keys() {
        if !matches!(
            k.as_str(),
            "sortBy" | "partitionBy" | "partitionByFields" | "output"
        ) {
            return Err(err(
                40415,
                format!("BSON field '$fill.{k}' is an unknown field."),
            ));
        }
    }
    let Some(out) = d.get("output") else {
        return Err(missing_field("$fill.output"));
    };
    let Bson::Document(out) = out else {
        return Err(err(
            code::TYPE_MISMATCH,
            "BSON field '$fill.output' must be an object",
        ));
    };
    if d.contains_key("partitionBy") && d.contains_key("partitionByFields") {
        return Err(err(
            6_050_204,
            "Maximum one of 'partitionBy' and 'partitionByFields can be specified in '$fill'",
        ));
    }
    let mut window = Document::new();
    let mut values = Document::new();
    for (f, spec) in out {
        let Bson::Document(s) = spec else {
            return Err(err(
                code::TYPE_MISMATCH,
                format!("Each fill output must be an object, found {f}"),
            ));
        };
        let (m, v) = (s.get("method"), s.get("value"));
        if (m.is_some() && v.is_some()) || s.len() > 1 {
            return Err(err(
                6_050_201,
                "Each fill output specification must have exactly one of 'method' or 'value' fields, not both or neither",
            ));
        }
        match (m, v) {
            (Some(Bson::String(m)), _) if m == "locf" => {
                window.insert(f.clone(), doc! {"$locf": format!("${f}")});
            }
            (Some(Bson::String(m)), _) if m == "linear" => {
                window.insert(f.clone(), doc! {"$linearFill": format!("${f}")});
            }
            (Some(_), _) => return Err(err(6_050_202, "Method must be either locf or linear")),
            (None, Some(v)) => {
                values.insert(f.clone(), doc! {"$ifNull": [format!("${f}"), v.clone()]});
            }
            (None, None) => {}
        }
    }
    let mut cur = docs;
    if !window.is_empty() {
        let mut w = doc! {};
        if let Some(s) = d.get("sortBy") {
            w.insert("sortBy", s.clone());
        }
        match (d.get("partitionBy"), d.get("partitionByFields")) {
            (Some(p), _) => {
                w.insert("partitionBy", p.clone());
            }
            (None, Some(Bson::Array(fs))) => {
                let mut p = Document::new();
                for x in fs {
                    if let Bson::String(s) = x {
                        field_path_ok(s)?;
                        p.insert(s.clone(), format!("${s}"));
                    }
                }
                w.insert("partitionBy", p);
            }
            _ => {}
        }
        w.insert("output", window);
        cur = set_window_fields(cur, &Bson::Document(w), lim)?;
    }
    if !values.is_empty() {
        cur = run_stage(cur, &doc! {"$addFields": values}, lim)?;
    }
    Ok(cur)
}
