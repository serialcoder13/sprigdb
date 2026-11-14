//! `$densify` and `$fill`, with MongoDB 8.0's errors.
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

use std::sync::Arc;

use super::accum_num::{is_num, varith};
use super::window::{date_add, set_window_fields};
use super::{AggLimits, run_stage};
use crate::keyenc::compare;
use crate::spill::{self, Cmp, DocSeq, SeqBuilder};
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
pub(crate) fn densify(
    docs: Vec<Document>,
    arg: &Bson,
    lim: &AggLimits,
) -> Result<Vec<Document>, MongoError> {
    let spec = parse_densify(arg)?;
    super::over(lim, &docs, "$densify")?;
    for d in &docs {
        let v = spec.val(d);
        if !matches!(v, Bson::Null) {
            spec.check(&v)?;
        }
    }
    let mut items: Vec<(Document, Document)> = docs
        .into_iter()
        .map(|d| Ok((part_of(&d, &spec.parts)?, d)))
        .collect::<Result<_, MongoError>>()?;
    items.sort_by(|a, b| spec.order(&a.0, &a.1, &b.0, &b.1));
    let mut out: Vec<Document> = Vec::new();
    densify_sorted(&spec, items.into_iter().map(Ok), lim, &mut |d| {
        out.push(d);
        Ok(())
    })?;
    Ok(out)
}

/// `$densify` over an input that may be on disk: the
/// input sorted on disk as in memory (by partition, then the field;
/// `bounds: "full"` by the field alone), then the same single pass, its
/// output collected into a builder that spills.
///
/// # Errors
/// As [`densify`]; 292 past the stage's memory without `allowDiskUse`.
pub(crate) fn densify_seq(seq: DocSeq, arg: &Bson, lim: &AggLimits) -> Result<DocSeq, MongoError> {
    let spec = Arc::new(parse_densify(arg)?);
    let s2 = Arc::clone(&spec);
    let wrapped = seq.into_iter_docs()?.map(move |d| {
        let d = d?;
        let v = s2.val(&d);
        if !matches!(v, Bson::Null) {
            s2.check(&v)?;
        }
        Ok(doc! {"p": part_of(&d, &s2.parts)?, "d": d})
    });
    let s3 = Arc::clone(&spec);
    let cmp: Cmp = Arc::new(move |a: &Document, b: &Document| {
        let (pa, da) = split(a);
        let (pb, db) = split(b);
        s3.order(pa, da, pb, db)
    });
    let sorted = spill::sort_seq(wrapped, cmp, None, lim.stage_bytes, "Sort")?;
    let items = sorted.into_iter_docs()?.map(|w| {
        let mut w = w?;
        let p = match w.remove("p") {
            Some(Bson::Document(p)) => p,
            _ => Document::new(),
        };
        let d = match w.remove("d") {
            Some(Bson::Document(d)) => d,
            _ => Document::new(),
        };
        Ok((p, d))
    });
    let mut out = SeqBuilder::new(lim.stage_bytes);
    densify_sorted(&spec, items, lim, &mut |d| out.push(d))?;
    out.finish()
}

fn split(w: &Document) -> (&Document, &Document) {
    static EMPTY: std::sync::LazyLock<Document> = std::sync::LazyLock::new(Document::new);
    (
        w.get_document("p").unwrap_or(&EMPTY),
        w.get_document("d").unwrap_or(&EMPTY),
    )
}

impl Densify {
    fn val(&self, d: &Document) -> Bson {
        get_path(d, &self.field).cloned().unwrap_or(Bson::Null)
    }

    /// MongoDB's streaming order: by partition (except with full
    /// bounds), then by the field.
    fn order(&self, pa: &Document, da: &Document, pb: &Document, db: &Document) -> Ordering {
        if !matches!(self.bounds, Bounds::Full) {
            let o = compare(&Bson::Document(pa.clone()), &Bson::Document(pb.clone()));
            if o != Ordering::Equal {
                return o;
            }
        }
        compare(&self.val(da), &self.val(db))
    }
}

/// The densify pass over `(partition, document)` pairs already in
/// [`Densify::order`]: one state per partition, documents passed through
/// and generated ones emitted in MongoDB's order.
#[allow(clippy::too_many_lines)]
fn densify_sorted(
    spec: &Densify,
    items: impl Iterator<Item = Result<(Document, Document), MongoError>>,
    lim: &AggLimits,
    emit: &mut dyn FnMut(Document) -> Result<(), MongoError>,
) -> Result<(), MongoError> {
    // Full bounds: from the smallest value (the first, values sorted
    // alone) to the largest (the last), which only the tail of each
    // partition needs. Generation inside the input never passes it.
    let mut gmin: Option<Bson> = None;
    let mut gmax: Option<Bson> = None;
    let (fixed_lo, fixed_hi, hi_inclusive) = match &spec.bounds {
        Bounds::Full | Bounds::Partition => (None, None, true),
        Bounds::Range(a, b) => (Some(a.clone()), Some(b.clone()), false),
    };
    let below_hi = |v: &Bson, hi: &Bson| {
        let o = compare(v, hi);
        o == Ordering::Less || (hi_inclusive && o == Ordering::Equal)
    };
    let mut states: Vec<PState> = Vec::new();
    let mut emitted = 0usize;
    let limit = (lim.stage_bytes / 64).max(1_000_000);
    for item in items {
        let (p, d) = item?;
        let v = spec.val(&d);
        if matches!(v, Bson::Null) {
            emit(d)?;
            emitted += 1;
            continue;
        }
        if gmin.is_none() {
            gmin = Some(v.clone());
        }
        gmax = Some(v.clone());
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
        if let Some(a) = &fixed_lo
            && compare(&v, a) == Ordering::Less
        {
            emit(d)?;
            emitted += 1;
            continue;
        }
        if st.cur.is_none() {
            match &spec.bounds {
                Bounds::Partition => st.cur = Some(v.clone()),
                Bounds::Full => {
                    st.cur.clone_from(&gmin);
                    if compare(&v, gmin.as_ref().unwrap_or(&v)) != Ordering::Equal {
                        st.pending = true;
                        emit(d)?;
                        emitted += 1;
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
            if let Some(h) = &fixed_hi
                && !below_hi(&c, h)
            {
                break;
            }
            emit(spec.make(&st.part, &c)?)?;
            emitted += 1;
            if emitted > limit {
                return Err(err(5_897_900, "Generated too many documents in $densify"));
            }
            st.cur = spec.next(&c);
        }
        if let Some(c) = &st.cur
            && compare(c, &v) == Ordering::Equal
        {
            st.cur = spec.next(c);
        }
        emit(d)?;
        emitted += 1;
    }
    // The rest of each partition's range.
    let hi = match &spec.bounds {
        Bounds::Full => gmax,
        _ => fixed_hi,
    };
    if let Some(h) = &hi {
        for st in &mut states {
            if st.cur.is_none() {
                continue;
            }
            while let Some(c) = st.cur.clone() {
                if !below_hi(&c, h) {
                    break;
                }
                emit(spec.make(&st.part, &c)?)?;
                st.cur = spec.next(&c);
            }
        }
    }
    Ok(())
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
