//! `$bucket` and `$bucketAuto` (M2, 12 Nov 2026), with MongoDB 8.0's
//! errors and `$bucketAuto`'s granularities (the Renard and E series,
//! 1-2-5 and powers of two).

use std::cmp::Ordering;

use bson::{Bson, Document, doc};
use sprig_proto_mongo::errors::MongoError;

use super::accum::{self, AccSpec};
use super::accum_num::{self, is_num};
use super::{AggLimits, Vars, eval, missing};
use crate::keyenc::compare;
use crate::update::type_name;

fn err(c: i32, msg: impl Into<String>) -> MongoError {
    MongoError::new(c, msg)
}

/// A group-by expression must be a `$path` or an expression object.
fn group_by_ok(v: &Bson) -> bool {
    matches!(v, Bson::String(s) if s.starts_with('$')) || matches!(v, Bson::Document(_))
}

/// The canonical type family of a boundary (numbers are one family).
fn family(v: &Bson) -> &'static str {
    if is_num(v) { "number" } else { type_name(v) }
}

fn default_output() -> Vec<(String, AccSpec)> {
    vec![(
        "count".to_owned(),
        AccSpec::parse("$sum", &Bson::Int32(1), accum::Ctx::Group)
            .unwrap_or_else(|_| unreachable!()),
    )]
}

fn output_of(
    spec: Option<&Bson>,
    stage: &str,
    code_obj: i32,
) -> Result<Vec<(String, AccSpec)>, MongoError> {
    match spec {
        None => Ok(default_output()),
        Some(Bson::Document(o)) => accum::parse_fields(o, false),
        Some(v) => Err(err(
            code_obj,
            format!(
                "The $bucket 'output' field must be an object, but found type: {}",
                type_name(v)
            )
            .replace("$bucket", stage),
        )),
    }
}

/// `$bucket`.
///
/// # Errors
/// 40192–40202 for the specification; 40066 for a value outside every
/// bucket without `default`; the accumulators'.
pub(crate) fn bucket(
    docs: &[Document],
    arg: &Bson,
    lim: &AggLimits,
) -> Result<Vec<Document>, MongoError> {
    let sp = parse_bucket(arg)?;
    let mut js = crate::serverjs::GroupJs::parse(&sp.js_spec)?;
    let mut keys = Vec::with_capacity(docs.len());
    for doc in docs {
        keys.push(sp.key(doc, lim)?);
    }
    let mut out = accum::group_by(
        docs,
        keys,
        &sp.accs,
        lim,
        "$bucket",
        (!js.is_empty()).then_some(&mut js),
    )?;
    sort_by_id(&mut out);
    Ok(out)
}

/// Sorts `$bucket` output by `_id`.
pub(crate) fn sort_by_id(out: &mut [Document]) {
    out.sort_by(|x, y| {
        compare(
            x.get("_id").unwrap_or(&Bson::Null),
            y.get("_id").unwrap_or(&Bson::Null),
        )
    });
}

/// A parsed `$bucket` (split out for the spilling version, M5).
pub(crate) struct BucketSpec {
    gb: Bson,
    b: Vec<Bson>,
    default: Option<Bson>,
    pub(crate) accs: Vec<(String, AccSpec)>,
    pub(crate) js_spec: Document,
}

/// Parses `$bucket`.
///
/// # Errors
/// 40192–40202.
pub(crate) fn parse_bucket(arg: &Bson) -> Result<BucketSpec, MongoError> {
    let Bson::Document(d) = arg else {
        return Err(err(
            40201,
            format!(
                "Argument to $bucket stage must be an object, but found type: {}",
                type_name(arg)
            ),
        ));
    };
    for k in d.keys() {
        if !matches!(k.as_str(), "groupBy" | "boundaries" | "default" | "output") {
            return Err(err(40197, format!("Unrecognized option to $bucket: {k}.")));
        }
    }
    let (Some(gb), Some(bounds)) = (d.get("groupBy"), d.get("boundaries")) else {
        return Err(err(
            40198,
            "$bucket requires 'groupBy' and 'boundaries' to be specified.",
        ));
    };
    if !group_by_ok(gb) {
        return Err(err(
            40202,
            format!(
                "The $bucket 'groupBy' field must be defined as a $-prefixed path or an expression, but found: {gb}."
            ),
        ));
    }
    let Bson::Array(b) = bounds else {
        return Err(err(
            40200,
            format!(
                "The $bucket 'boundaries' field must be an array, but found type: {}.",
                type_name(bounds)
            ),
        ));
    };
    if b.len() < 2 {
        return Err(err(
            40192,
            format!(
                "The $bucket 'boundaries' field must have at least 2 values, but found {} value(s).",
                b.len()
            ),
        ));
    }
    for w in b.windows(2) {
        if family(&w[0]) != family(&w[1]) {
            return Err(err(
                40193,
                format!(
                    "All values in the the 'boundaries' option to $bucket must have the same type. Found conflicting types {} and {}.",
                    type_name(&w[0]),
                    type_name(&w[1])
                ),
            ));
        }
    }
    for (i, w) in b.windows(2).enumerate() {
        if compare(&w[0], &w[1]) != Ordering::Less {
            return Err(err(
                40194,
                format!(
                    "The 'boundaries' option to $bucket must be sorted, but elements {i} and {} are not in ascending order ({} is not less than {}).",
                    i + 1,
                    w[0],
                    w[1]
                ),
            ));
        }
    }
    let default = d.get("default");
    if let Some(dv) = default
        && family(dv) == family(&b[0])
        && compare(dv, &b[0]) != Ordering::Less
        && compare(dv, &b[b.len() - 1]) == Ordering::Less
    {
        return Err(err(
            40199,
            "The $bucket 'default' field must be less than the lowest boundary or greater than or equal to the highest boundary.",
        ));
    }
    let accs = output_of(d.get("output"), "$bucket", 40196)?;
    Ok(BucketSpec {
        gb: gb.clone(),
        b: b.clone(),
        default: default.cloned(),
        accs,
        js_spec: d.get_document("output").cloned().unwrap_or_default(),
    })
}

impl BucketSpec {
    /// The bucket (its lower boundary, or the default) of `doc`.
    ///
    /// # Errors
    /// 40066 outside every bucket without `default`; the expression's.
    pub(crate) fn key(&self, doc: &Document, lim: &AggLimits) -> Result<Bson, MongoError> {
        let b = &self.b;
        let v = eval(&self.gb, &Vars::new(doc, lim.now))?;
        let pos = b.windows(2).position(|w| {
            !missing(&v)
                && family(&v) == family(&w[0])
                && compare(&v, &w[0]) != Ordering::Less
                && compare(&v, &w[1]) == Ordering::Less
        });
        match (pos, &self.default) {
            (Some(i), _) => Ok(b[i].clone()),
            (None, Some(dv)) => Ok(dv.clone()),
            (None, None) => Err(err(
                40066,
                "$switch could not find a matching branch for an input, and no default was specified.",
            )),
        }
    }
}

// --- granularity ---------------------------------------------------------------------

// The series as MongoDB 8.0 keeps them, times 100 (read from the
// reference server one step at a time: its R40 has no 3.35, and its R80
// is every other value of the standard series, from 1.03).

const R5: &[u16] = &[100, 160, 250, 400, 630];

const R10: &[u16] = &[100, 125, 160, 200, 250, 315, 400, 500, 630, 800];

const R20: &[u16] = &[
    100, 112, 125, 140, 160, 180, 200, 224, 250, 280, 315, 355, 400, 450, 500, 560, 630, 710, 800,
    900,
];

const R40: &[u16] = &[
    100, 106, 112, 118, 125, 132, 140, 150, 160, 170, 180, 190, 200, 212, 224, 236, 250, 265, 280,
    300, 315, 355, 375, 400, 425, 450, 475, 500, 530, 560, 600, 630, 670, 710, 750, 800, 850, 900,
    950,
];

const R80: &[u16] = &[
    103, 109, 115, 122, 128, 136, 145, 155, 165, 175, 185, 195, 206, 218, 230, 243, 258, 272, 290,
    307, 325, 345, 365, 387, 412, 437, 462, 487, 515, 545, 575, 615, 650, 690, 730, 775, 825, 875,
    925, 975,
];

const S125: &[u16] = &[100, 200, 500];

const E6: &[u16] = &[100, 150, 220, 330, 470, 680];

const E12: &[u16] = &[100, 120, 150, 180, 220, 270, 330, 390, 470, 560, 680, 820];

const E24: &[u16] = &[
    100, 110, 120, 130, 150, 160, 180, 200, 220, 240, 270, 300, 330, 360, 390, 430, 470, 510, 560,
    620, 680, 750, 820, 910,
];

const E48: &[u16] = &[
    100, 105, 110, 115, 121, 127, 133, 140, 147, 154, 162, 169, 178, 187, 196, 205, 215, 226, 237,
    249, 261, 274, 287, 301, 316, 332, 348, 365, 383, 402, 422, 442, 464, 487, 511, 536, 562, 590,
    619, 649, 681, 715, 750, 787, 825, 866, 909, 953,
];

const E96: &[u16] = &[
    100, 102, 105, 107, 110, 113, 115, 118, 121, 124, 127, 130, 133, 137, 140, 143, 147, 150, 154,
    158, 162, 165, 169, 174, 178, 182, 187, 191, 196, 200, 205, 210, 215, 221, 226, 232, 237, 243,
    249, 255, 261, 267, 274, 280, 287, 294, 301, 309, 316, 324, 332, 340, 348, 357, 365, 374, 383,
    392, 402, 412, 422, 432, 442, 453, 464, 475, 487, 499, 511, 523, 536, 549, 562, 576, 590, 604,
    619, 634, 649, 665, 681, 698, 715, 732, 750, 768, 787, 806, 825, 845, 866, 887, 909, 931, 953,
    976,
];

const E192: &[u16] = &[
    100, 101, 102, 104, 105, 106, 107, 109, 110, 111, 113, 114, 115, 117, 118, 120, 121, 123, 124,
    126, 127, 129, 130, 132, 133, 135, 137, 138, 140, 142, 143, 145, 147, 149, 150, 152, 154, 156,
    158, 160, 162, 164, 165, 167, 169, 172, 174, 176, 178, 180, 182, 184, 187, 189, 191, 193, 196,
    198, 200, 203, 205, 208, 210, 213, 215, 218, 221, 223, 226, 229, 232, 234, 237, 240, 243, 246,
    249, 252, 255, 258, 261, 264, 267, 271, 274, 277, 280, 284, 287, 291, 294, 298, 301, 305, 309,
    312, 316, 320, 324, 328, 332, 336, 340, 344, 348, 352, 357, 361, 365, 370, 374, 379, 383, 388,
    392, 397, 402, 407, 412, 417, 422, 427, 432, 437, 442, 448, 453, 459, 464, 470, 475, 481, 487,
    493, 499, 505, 511, 517, 523, 530, 536, 542, 549, 556, 562, 569, 576, 583, 590, 597, 604, 612,
    619, 626, 634, 642, 649, 657, 665, 673, 681, 690, 698, 706, 715, 723, 732, 741, 750, 759, 768,
    777, 787, 796, 806, 816, 825, 835, 845, 856, 866, 876, 887, 898, 909, 920, 931, 942, 953, 965,
    976, 988,
];

enum Rounder {
    Series(&'static [u16]),
    Pow2,
}

fn rounder(name: &str) -> Option<Rounder> {
    Some(Rounder::Series(match name {
        "R5" => R5,
        "R10" => R10,
        "R20" => R20,
        "R40" => R40,
        "R80" => R80,
        "1-2-5" => S125,
        "E6" => E6,
        "E12" => E12,
        "E24" => E24,
        "E48" => E48,
        "E96" => E96,
        "E192" => E192,
        "POWERSOF2" => return Some(Rounder::Pow2),
        _ => return None,
    }))
}

/// `x` scaled into [100, 1000) by powers of ten: (scaled, exponent).
fn scale(x: f64) -> (f64, i32) {
    let (mut n, mut k) = (x, 0);
    while n >= 1000.0 {
        n /= 10.0;
        k += 1;
    }
    while n < 100.0 {
        n *= 10.0;
        k -= 1;
    }
    (n, k)
}

/// 10^k as MongoDB's tables scale them.
fn p10(k: i32) -> f64 {
    if k >= 0 {
        10f64.powi(k)
    } else {
        1.0 / 10f64.powi(-k)
    }
}

fn pow2_int(v: i64, up: bool) -> i64 {
    if v == 0 {
        return 0;
    }
    let lz = v.leading_zeros();
    if up {
        1i64.checked_shl(64 - lz).unwrap_or(i64::MAX)
    } else if v.count_ones() == 1 {
        v / 2
    } else {
        1i64 << (63 - lz)
    }
}

impl Rounder {
    fn round(&self, v: &Bson, up: bool) -> Bson {
        match self {
            Rounder::Pow2 => match v {
                Bson::Int32(i) => {
                    let r = pow2_int(i64::from(*i), up);
                    i32::try_from(r).map_or(Bson::Int64(r), Bson::Int32)
                }
                Bson::Int64(i) => Bson::Int64(pow2_int(*i, up)),
                o => {
                    let x = accum_num::to_f64(o).unwrap_or(0.0);
                    if x == 0.0 {
                        return Bson::Double(0.0);
                    }
                    let e = x.log2();
                    let f = if up {
                        if e.fract() == 0.0 { e + 1.0 } else { e.ceil() }
                    } else if e.fract() == 0.0 {
                        e - 1.0
                    } else {
                        e.floor()
                    };
                    Bson::Double(f.exp2())
                }
            },
            Rounder::Series(s) => {
                let x = accum_num::to_f64(v).unwrap_or(0.0);
                if x == 0.0 {
                    return Bson::Double(0.0);
                }
                // The series are kept as three-digit integers (1.27 as
                // 127), as MongoDB keeps them, so 12.7 comes out as
                // 127 * 0.1.
                let (n, k) = scale(x);
                let t = |y: &u16| f64::from(*y);
                let r = if up {
                    s.iter()
                        .map(t)
                        .find(|y| *y > n)
                        .map_or_else(|| t(&s[0]) * p10(k + 1), |y| y * p10(k))
                } else {
                    s.iter()
                        .map(t)
                        .rev()
                        .find(|y| *y < n)
                        .map_or_else(|| t(&s[s.len() - 1]) * p10(k - 1), |y| y * p10(k))
                };
                Bson::Double(r)
            }
        }
    }
}

/// `$bucketAuto`.
///
/// # Errors
/// 40239–40260 for the specification and granularity; the accumulators'.
#[allow(clippy::too_many_lines)]
pub(crate) fn bucket_auto(
    docs: &[Document],
    arg: &Bson,
    lim: &AggLimits,
) -> Result<Vec<Document>, MongoError> {
    let sp = parse_auto(arg)?;
    // Each document's value, sorted (stably).
    let mut vals: Vec<(Bson, usize)> = Vec::with_capacity(docs.len());
    for (i, doc) in docs.iter().enumerate() {
        vals.push((sp.value(doc, lim)?, i));
    }
    super::over(lim, docs, "$bucketAuto")?;
    vals.sort_by(|a, b| compare(&a.0, &b.0));
    let sorted: Vec<Bson> = vals.iter().map(|(v, _)| v.clone()).collect();
    let mut out = Vec::new();
    for (a, b, min, max) in sp.plan(&sorted) {
        // MongoDB 8.0 accumulates a bucket's documents in input order,
        // not in value order (M5: `$push` showed it).
        let mut idx: Vec<usize> = vals[a..b].iter().map(|(_, k)| *k).collect();
        idx.sort_unstable();
        let ds = idx.into_iter().map(|k| Ok(docs[k].clone()));
        if let Some(d) = sp.bucket(ds, min, max, lim)? {
            out.push(d);
        }
    }
    Ok(out)
}

/// A parsed `$bucketAuto` (split out for the spilling version, M5).
pub(crate) struct AutoSpec {
    gb: Bson,
    nbuckets: i32,
    gran: Option<Rounder>,
    accs: Vec<(String, AccSpec)>,
    js_spec: Document,
}

/// Parses `$bucketAuto`.
///
/// # Errors
/// 40239–40261 and the accumulators'.
pub(crate) fn parse_auto(arg: &Bson) -> Result<AutoSpec, MongoError> {
    let Bson::Document(d) = arg else {
        return Err(err(
            40240,
            format!(
                "The argument to $bucketAuto must be an object, but found type: {}",
                type_name(arg)
            ),
        ));
    };
    for k in d.keys() {
        if !matches!(k.as_str(), "groupBy" | "buckets" | "output" | "granularity") {
            return Err(err(
                40245,
                format!("Unrecognized option to $bucketAuto: {k}"),
            ));
        }
    }
    let (Some(gb), Some(nb)) = (d.get("groupBy"), d.get("buckets")) else {
        return Err(err(
            40246,
            "$bucketAuto requires 'groupBy' and 'buckets' to be specified",
        ));
    };
    if !group_by_ok(gb) {
        return Err(err(
            40239,
            format!(
                "The $bucketAuto 'groupBy' field must be defined as a $-prefixed path or an expression object, but found: {gb}"
            ),
        ));
    }
    let nbuckets = match accum_num::integral(nb).and_then(|n| i32::try_from(n).ok()) {
        Some(n) => n,
        None if is_num(nb) => {
            return Err(err(
                40242,
                format!(
                    "The $bucketAuto 'buckets' field must be representable as a 32-bit integer, but found {nb}"
                ),
            ));
        }
        None => {
            return Err(err(
                40241,
                format!(
                    "The $bucketAuto 'buckets' field must be a numeric value, but found type: {}",
                    type_name(nb)
                ),
            ));
        }
    };
    if nbuckets <= 0 {
        return Err(err(
            40243,
            format!(
                "The $bucketAuto 'buckets' field must be greater than 0, but found: {nbuckets}"
            ),
        ));
    }
    let gran = match d.get("granularity") {
        None => None,
        Some(Bson::String(g)) => Some(
            rounder(g).ok_or_else(|| err(40257, format!("Unknown rounding granularity '{g}'")))?,
        ),
        Some(g) => {
            return Err(err(
                40261,
                format!(
                    "The $bucketAuto 'granularity' field must be a string, but found type: {}",
                    type_name(g)
                ),
            ));
        }
    };
    let accs = output_of(d.get("output"), "$bucketAuto", 40244)?;
    let js_spec = d.get_document("output").cloned().unwrap_or_default();
    Ok(AutoSpec {
        gb: gb.clone(),
        nbuckets,
        gran,
        accs,
        js_spec,
    })
}

impl AutoSpec {
    /// The `groupBy` value of `doc`.
    ///
    /// # Errors
    /// 40258 and 40260 with a granularity; the expression's.
    pub(crate) fn value(&self, doc: &Document, lim: &AggLimits) -> Result<Bson, MongoError> {
        let v = eval(&self.gb, &Vars::new(doc, lim.now))?;
        let v = if missing(&v) { Bson::Null } else { v };
        if self.gran.is_some() {
            if !is_num(&v) {
                return Err(err(
                    40258,
                    format!(
                        "$bucketAuto can specify a 'granularity' with numeric boundaries only, but found a value with type: {}",
                        type_name(&v)
                    ),
                ));
            }
            if accum_num::to_f64(&v).is_some_and(|f| f < 0.0) {
                return Err(err(
                    40260,
                    format!(
                        "$bucketAuto can specify a 'granularity' with numeric boundaries only, but found a negative value: {v}"
                    ),
                ));
            }
        }
        Ok(v)
    }

    /// Buckets over the sorted values: index ranges with their bounds.
    pub(crate) fn plan(&self, vals: &[Bson]) -> Vec<(usize, usize, Bson, Bson)> {
        let (nbuckets, gran) = (self.nbuckets, &self.gran);
        let n = vals.len();
        if n == 0 {
            return Vec::new();
        }
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )]
        let size = ((n as f64 / f64::from(nbuckets)).round() as usize).max(1);
        // Buckets as index ranges with their min and max.
        let mut buckets: Vec<(usize, usize, Bson, Bson)> = Vec::new();
        let mut i = 0;
        let mut prev_max: Option<Bson> = None;
        while i < n {
            let start = i;
            let last_bucket =
                buckets.len() + 1 == usize::try_from(nbuckets).unwrap_or(usize::MAX);
            let mut end = if last_bucket { n } else { (i + size).min(n) };
            if gran.is_none() {
                while end < n && compare(&vals[end], &vals[end - 1]) == Ordering::Equal {
                    end += 1;
                }
            }
            let mut max = if end < n {
                vals[end].clone()
            } else {
                vals[n - 1].clone()
            };
            if let Some(r) = gran {
                let mut boundary = r.round(&vals[end - 1], true);
                while end < n && compare(&vals[end], &boundary) == Ordering::Less {
                    end += 1;
                }
                if accum_num::to_f64(&vals[end - 1]) == Some(0.0)
                    && accum_num::to_f64(&boundary) == Some(0.0)
                    && end < n
                {
                    boundary = r.round(&vals[end], false);
                }
                max = boundary;
            }
            let min = match (gran, prev_max.take()) {
                (Some(_), Some(p)) => p,
                (Some(r), None) => r.round(&vals[start], false),
                (None, _) => vals[start].clone(),
            };
            prev_max = Some(max.clone());
            buckets.push((start, end, min, max));
            i = end;
        }
        buckets
    }

    /// One bucket's output document from its documents.
    ///
    /// # Errors
    /// The accumulators'.
    pub(crate) fn bucket(
        &self,
        ds: impl Iterator<Item = Result<Document, MongoError>>,
        min: Bson,
        max: Bson,
        lim: &AggLimits,
    ) -> Result<Option<Document>, MongoError> {
        let mut js = crate::serverjs::GroupJs::parse(&self.js_spec)?;
        let mut g = accum::Grouper::new(
            &self.accs,
            lim,
            "$bucketAuto",
            (!js.is_empty()).then_some(&mut js),
        );
        for d in ds {
            g.push(&d?, Bson::Null)?;
        }
        let mut out = g.finish()?;
        Ok(out.pop().map(|mut first| {
            first.insert("_id", doc! {"min": min, "max": max});
            first
        }))
    }
}
