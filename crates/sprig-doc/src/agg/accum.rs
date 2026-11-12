//! Accumulators of `$group`, `$bucket`, `$bucketAuto` and
//! `$setWindowFields` (M2, 12 Nov 2026): `$sum`, `$avg`, `$min`, `$max`,
//! `$first`, `$last`, `$push`, `$addToSet`, `$count`, `$mergeObjects`,
//! `$stdDevPop`, `$stdDevSamp`, `$top`, `$bottom`, `$topN`, `$bottomN`,
//! `$firstN`, `$lastN`, `$maxN`, `$minN`, `$median`, `$percentile`, and
//! (MongoDB 8.1) `$concatArrays` and `$setUnion`.
//!
//! An accumulator is parsed once ([`AccSpec`]); each document gives an
//! [`Item`] (its input value, and for `$top`/`$bottom` its sort keys),
//! which a [`AccState`] takes in input order.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};

use bson::{Bson, Document, doc};
use sprig_proto_mongo::errors::{MongoError, code};

use super::accum_num::{self, Summer};
use super::{AggLimits, SortSpec, Vars, eval, missing, nullish, sort_compare, sort_key};
use crate::keyenc::compare;
use crate::update::type_name;

fn err(c: i32, msg: impl Into<String>) -> MongoError {
    MongoError::new(c, msg)
}

/// Where an accumulator runs: `n` of a `$group` may read the group's `_id`;
/// in a window it must be a constant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ctx {
    Group,
    Window,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
    Push,
    AddToSet,
    Count,
    MergeObjects,
    StdDevPop,
    StdDevSamp,
    Top,
    Bottom,
    TopN,
    BottomN,
    FirstN,
    LastN,
    MaxN,
    MinN,
    Median,
    Percentile,
    ConcatArrays,
    SetUnion,
}

/// The accumulator names this module knows.
pub(crate) fn kind_of(op: &str) -> Option<Kind> {
    Some(match op {
        "$sum" => Kind::Sum,
        "$avg" => Kind::Avg,
        "$min" => Kind::Min,
        "$max" => Kind::Max,
        "$first" => Kind::First,
        "$last" => Kind::Last,
        "$push" => Kind::Push,
        "$addToSet" => Kind::AddToSet,
        "$count" => Kind::Count,
        "$mergeObjects" => Kind::MergeObjects,
        "$stdDevPop" => Kind::StdDevPop,
        "$stdDevSamp" => Kind::StdDevSamp,
        "$top" => Kind::Top,
        "$bottom" => Kind::Bottom,
        "$topN" => Kind::TopN,
        "$bottomN" => Kind::BottomN,
        "$firstN" => Kind::FirstN,
        "$lastN" => Kind::LastN,
        "$maxN" => Kind::MaxN,
        "$minN" => Kind::MinN,
        "$median" => Kind::Median,
        "$percentile" => Kind::Percentile,
        "$concatArrays" => Kind::ConcatArrays,
        "$setUnion" => Kind::SetUnion,
        _ => return None,
    })
}

/// A parsed accumulator.
#[derive(Debug, Clone)]
pub(crate) struct AccSpec {
    pub(crate) op: String,
    pub(crate) kind: Kind,
    input: Bson,
    n: Option<Bson>,
    sort: SortSpec,
    ps: Vec<f64>,
}

/// One document's contribution.
#[derive(Debug, Clone)]
pub(crate) struct Item {
    pub(crate) val: Bson,
    keys: Vec<Bson>,
}

fn show(v: &Bson) -> String {
    match v {
        Bson::Undefined => "MISSING".to_owned(),
        Bson::String(s) => format!("\"{s}\""),
        o => o.to_string(),
    }
}

/// `n` of `$topN`, `$firstN` and friends (MongoDB 8.0's checks).
pub(crate) fn n_of(v: &Bson) -> Result<usize, MongoError> {
    let Some(n) = accum_num::integral(v) else {
        return Err(err(
            if accum_num::is_num(v) {
                5_787_903
            } else {
                5_787_902
            },
            format!(
                "Value for 'n' must be of integral type, but found {}",
                show(v)
            ),
        ));
    };
    if n <= 0 {
        return Err(err(
            5_787_908,
            format!("'n' must be greater than 0, found {n}"),
        ));
    }
    Ok(usize::try_from(n).unwrap_or(usize::MAX))
}

impl AccSpec {
    /// Parses `{op: arg}`.
    ///
    /// # Errors
    /// MongoDB 8.0's codes for each accumulator's arguments; 15952 for an
    /// unknown name in a `$group`.
    pub(crate) fn parse(op: &str, arg: &Bson, ctx: Ctx) -> Result<Self, MongoError> {
        let Some(kind) = kind_of(op) else {
            return Err(err(15952, format!("unknown group operator '{op}'")));
        };
        let mut spec = Self {
            op: op.to_owned(),
            kind,
            input: Bson::Null,
            n: None,
            sort: Vec::new(),
            ps: Vec::new(),
        };
        let unary = || err(40237, format!("The {op} accumulator is a unary operator"));
        match kind {
            Kind::Top | Kind::Bottom | Kind::TopN | Kind::BottomN => {
                let Bson::Document(d) = arg else {
                    return Err(err(
                        5_788_001,
                        format!("specification must be an object; found {op}: {}", show(arg)),
                    ));
                };
                let with_n = matches!(kind, Kind::TopN | Kind::BottomN);
                let mut sort = None;
                let mut output = None;
                for (k, v) in d {
                    match k.as_str() {
                        "n" if with_n => spec.n = Some(v.clone()),
                        "sortBy" => sort = Some(v),
                        "output" => output = Some(v.clone()),
                        _ => {
                            return Err(err(5_788_002, format!("Unknown argument to {op} '{k}'")));
                        }
                    }
                }
                if with_n && spec.n.is_none() {
                    return Err(err(5_788_003, "Missing value for 'n'"));
                }
                spec.input = output.ok_or_else(|| err(5_788_004, "Missing value for 'output'"))?;
                let sort = sort.ok_or_else(|| err(5_788_005, "Missing value for 'sortBy'"))?;
                let Bson::Document(sd) = sort else {
                    return Err(err(
                        code::TYPE_MISMATCH,
                        format!("expected an object for 'sortBy', found {}", type_name(sort)),
                    ));
                };
                spec.sort = super::parse_sort(sd)?;
            }
            Kind::FirstN | Kind::LastN | Kind::MaxN | Kind::MinN => {
                let d = match arg {
                    Bson::Document(d) => d,
                    Bson::Array(_) => return Err(unary()),
                    o => {
                        return Err(err(
                            5_787_900,
                            format!("specification must be an object; found {op}: {}", show(o)),
                        ));
                    }
                };
                let mut input = None;
                for (k, v) in d {
                    match k.as_str() {
                        "n" => spec.n = Some(v.clone()),
                        "input" => input = Some(v.clone()),
                        _ => {
                            return Err(err(
                                5_787_901,
                                format!("Unknown argument for 'n' operator: {k}"),
                            ));
                        }
                    }
                }
                if spec.n.is_none() {
                    return Err(err(5_787_906, "Missing value for 'n'"));
                }
                spec.input = input.ok_or_else(|| err(5_787_907, "Missing value for 'input'"))?;
            }
            Kind::Median | Kind::Percentile => {
                let Bson::Document(d) = arg else {
                    return Err(err(
                        7_436_100,
                        format!("specification must be an object; found {op}: {}", show(arg)),
                    ));
                };
                let mut fields: HashMap<&str, &Bson> = HashMap::new();
                for (k, v) in d {
                    let ok = matches!(k.as_str(), "input" | "method")
                        || (k == "p" && kind == Kind::Percentile);
                    if !ok {
                        return Err(err(
                            40415,
                            format!("BSON field '{op}.{k}' is an unknown field."),
                        ));
                    }
                    fields.insert(k.as_str(), v);
                }
                let need = if kind == Kind::Percentile {
                    &["input", "p", "method"][..]
                } else {
                    &["input", "method"][..]
                };
                for f in need {
                    if !fields.contains_key(f) {
                        return Err(err(
                            40414,
                            format!("BSON field '{op}.{f}' is missing but a required field"),
                        ));
                    }
                }
                spec.input = fields["input"].clone();
                match fields["method"] {
                    Bson::String(m) if m == "approximate" => {}
                    Bson::String(_) => {
                        return Err(err(
                            code::BAD_VALUE,
                            "Currently only 'approximate' can be used as percentile 'method'.",
                        ));
                    }
                    o => {
                        return Err(err(
                            code::TYPE_MISMATCH,
                            format!(
                                "BSON field '{op}.method' is the wrong type '{}', expected type 'string'",
                                type_name(o)
                            ),
                        ));
                    }
                }
                if kind == Kind::Percentile {
                    let empty = Document::new();
                    let p = eval(fields["p"], &Vars::new(&empty, bson::DateTime::now()))?;
                    let bad = |c: i32, v: &Bson| {
                        err(
                            c,
                            format!(
                                "The $percentile 'p' field must be an array of numbers from [0.0, 1.0], but found: {}",
                                show(v)
                            ),
                        )
                    };
                    let Bson::Array(a) = &p else {
                        return Err(bad(7_750_301, &p));
                    };
                    if a.is_empty() {
                        return Err(bad(7_750_301, &p));
                    }
                    for x in a {
                        let f = accum_num::to_f64(x).ok_or_else(|| bad(7_750_302, x))?;
                        if !(0.0..=1.0).contains(&f) {
                            return Err(bad(7_750_303, x));
                        }
                        spec.ps.push(f);
                    }
                } else {
                    spec.ps.push(0.5);
                }
            }
            Kind::Count => {
                if !matches!(arg, Bson::Document(d) if d.is_empty()) {
                    return Err(err(
                        code::TYPE_MISMATCH,
                        "$count takes no arguments, i.e. $count:{}",
                    ));
                }
            }
            _ => {
                if matches!(arg, Bson::Array(_)) && ctx == Ctx::Group {
                    return Err(unary());
                }
                spec.input = arg.clone();
            }
        }
        Ok(spec)
    }

    /// The accumulator's `n` for a group (evaluated against `{_id: key}`)
    /// or a window (a constant).
    ///
    /// # Errors
    /// 5787902, 5787903, 5787908.
    pub(crate) fn n_for(&self, key: Option<&Bson>, lim: &AggLimits) -> Result<usize, MongoError> {
        let Some(n) = &self.n else { return Ok(1) };
        // MongoDB 8.0 evaluates `n` once, against no document: `$_id`
        // reads as missing even in a `$group`.
        let _ = key;
        let d = Document::new();
        n_of(&eval(n, &Vars::new(&d, lim.now))?)
    }

    /// The contribution of the document `vars` reads.
    ///
    /// # Errors
    /// The input expression's.
    pub(crate) fn item(&self, d: &Document, vars: &Vars) -> Result<Item, MongoError> {
        let val = if self.kind == Kind::Count {
            Bson::Null
        } else {
            eval(&self.input, vars)?
        };
        let keys = self
            .sort
            .iter()
            .map(|(p, dir)| sort_key(d, p, *dir))
            .collect();
        Ok(Item { val, keys })
    }

    /// A new state, with `n` already known.
    #[must_use]
    pub(crate) fn state(&self, n: usize) -> AccState<'_> {
        AccState {
            spec: self,
            n,
            sum: Summer::default(),
            best: None,
            list: VecDeque::new(),
            seen: HashSet::new(),
            keyed: Vec::new(),
            count: 0,
            welford: (0, 0.0, 0.0),
            dsums: None,
            merged: Document::new(),
            nums: Vec::new(),
            narrow: false,
            bytes: 0,
            err: None,
        }
    }
}

/// An accumulator's running state.
#[derive(Debug, Clone)]
pub(crate) struct AccState<'a> {
    spec: &'a AccSpec,
    n: usize,
    sum: Summer,
    best: Option<Bson>,
    list: VecDeque<Bson>,
    seen: HashSet<Vec<u8>>,
    keyed: Vec<(Vec<Bson>, Bson)>,
    count: i64,
    welford: (i64, f64, f64),
    /// Removable windows: exact decimal sums of values and squares, and
    /// the count of non-finite values (MongoDB's removable `$stdDev*`).
    dsums: Option<(accum_num::Dec, accum_num::Dec, i64)>,
    merged: Document,
    nums: Vec<f64>,
    /// Window functions over removable windows: integer sums in the
    /// narrowest type.
    pub(crate) narrow: bool,
    /// Bytes held by list-like accumulators.
    pub(crate) bytes: usize,
    err: Option<MongoError>,
}

fn is_num_v(v: &Bson) -> bool {
    accum_num::is_num(v)
}

fn val_bytes(v: &Bson) -> usize {
    bson::to_vec(&doc! {"v": v.clone()}).map_or(0, |b| b.len())
}

impl AccState<'_> {
    /// Takes one item.
    ///
    /// # Errors
    /// `$mergeObjects`, `$concatArrays` and `$setUnion` refuse other types.
    pub(crate) fn add(&mut self, it: &Item) -> Result<(), MongoError> {
        let v = &it.val;
        match self.spec.kind {
            Kind::Sum | Kind::Avg => {
                self.sum.add(v);
            }
            Kind::Min | Kind::Max => {
                if nullish(v) {
                    return Ok(());
                }
                let replace = self.best.as_ref().is_none_or(|b| {
                    let o = compare(v, b);
                    if self.spec.kind == Kind::Min {
                        o == Ordering::Less
                    } else {
                        o == Ordering::Greater
                    }
                });
                if replace {
                    self.best = Some(v.clone());
                }
            }
            Kind::First => {
                if self.count == 0 {
                    self.best = Some(if missing(v) { Bson::Null } else { v.clone() });
                }
            }
            Kind::Last => self.best = Some(if missing(v) { Bson::Null } else { v.clone() }),
            Kind::Push => {
                if !missing(v) {
                    self.bytes += val_bytes(v);
                    self.list.push_back(v.clone());
                }
            }
            Kind::AddToSet => {
                if !missing(v) && self.seen.insert(crate::keyenc::eq_key(v)) {
                    self.bytes += val_bytes(v);
                    self.list.push_back(v.clone());
                }
            }
            Kind::Count => {}
            Kind::MergeObjects => match v {
                Bson::Document(d) => {
                    for (k, x) in d {
                        self.merged.insert(k.clone(), x.clone());
                    }
                }
                x if nullish(x) => {}
                x => {
                    return Err(err(
                        40400,
                        format!(
                            "$mergeObjects requires object inputs, but input {x} is of type {}",
                            type_name(x)
                        ),
                    ));
                }
            },
            Kind::ConcatArrays | Kind::SetUnion => match v {
                Bson::Array(a) => {
                    for x in a {
                        if self.spec.kind == Kind::ConcatArrays
                            || self.seen.insert(crate::keyenc::eq_key(x))
                        {
                            self.bytes += val_bytes(x);
                            self.list.push_back(x.clone());
                        }
                    }
                }
                x if missing(x) => {}
                x => {
                    return Err(err(
                        if self.spec.kind == Kind::ConcatArrays {
                            28664
                        } else {
                            17044
                        },
                        format!(
                            "{} only supports arrays, not {}",
                            self.spec.op,
                            type_name(x)
                        ),
                    ));
                }
            },
            Kind::StdDevPop | Kind::StdDevSamp if self.narrow => {
                if is_num_v(v) {
                    let d = accum_num::Dec::from_bson(v).unwrap_or(accum_num::Dec::Nan);
                    let (s, q, nonfinite) = self
                        .dsums
                        .get_or_insert_with(|| (accum_num::Dec::zero(), accum_num::Dec::zero(), 0));
                    if matches!(d, accum_num::Dec::Fin { .. }) {
                        *q = q.add(&d.mul(&d));
                        *s = s.add(&d);
                        self.welford.0 += 1;
                    } else {
                        *nonfinite += 1;
                    }
                }
            }
            Kind::StdDevPop | Kind::StdDevSamp => {
                if let Some(x) = accum_num::to_f64(v) {
                    let (c, mean, m2) = &mut self.welford;
                    *c += 1;
                    let delta = x - *mean;
                    if delta != 0.0 {
                        #[allow(clippy::cast_precision_loss)]
                        {
                            *mean += delta / *c as f64;
                        }
                        *m2 += delta * (x - *mean);
                    }
                }
            }
            Kind::Top | Kind::Bottom | Kind::TopN | Kind::BottomN => {
                let out = if missing(v) { Bson::Null } else { v.clone() };
                self.bytes += val_bytes(&out);
                self.keyed.push((it.keys.clone(), out));
            }
            Kind::FirstN => {
                if self.list.len() < self.n {
                    let x = if missing(v) { Bson::Null } else { v.clone() };
                    self.bytes += val_bytes(&x);
                    self.list.push_back(x);
                }
            }
            Kind::LastN => {
                let x = if missing(v) { Bson::Null } else { v.clone() };
                self.bytes += val_bytes(&x);
                self.list.push_back(x);
                if self.list.len() > self.n
                    && let Some(old) = self.list.pop_front()
                {
                    self.bytes = self.bytes.saturating_sub(val_bytes(&old));
                }
            }
            Kind::MaxN | Kind::MinN => {
                if !nullish(v) {
                    self.bytes += val_bytes(v);
                    self.list.push_back(v.clone());
                }
            }
            Kind::Median | Kind::Percentile => {
                if accum_num::is_num(v)
                    && let Some(f) = accum_num::to_f64(v)
                {
                    self.nums.push(f);
                }
            }
        }
        self.count += 1;
        Ok(())
    }

    fn keyed_sorted(&self) -> Vec<&(Vec<Bson>, Bson)> {
        let mut v: Vec<&(Vec<Bson>, Bson)> = self.keyed.iter().collect();
        let sort = &self.spec.sort;
        v.sort_by(|a, b| {
            for (i, (_, dir)) in sort.iter().enumerate() {
                let o = sort_compare(&a.0[i], &b.0[i]);
                if o != Ordering::Equal {
                    return if *dir < 0 { o.reverse() } else { o };
                }
            }
            Ordering::Equal
        });
        v
    }

    /// The value so far.
    ///
    /// # Errors
    /// An error an earlier `add` kept.
    pub(crate) fn result(&self) -> Result<Bson, MongoError> {
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        Ok(match self.spec.kind {
            Kind::Sum => self.sum.sum(self.narrow),
            Kind::Avg => self.sum.avg(),
            Kind::Min | Kind::Max | Kind::First | Kind::Last => {
                self.best.clone().unwrap_or(Bson::Null)
            }
            // Over removable windows MongoDB keeps `$addToSet` in an
            // ordered multiset, so the set comes out sorted.
            Kind::AddToSet if self.narrow => {
                let mut v: Vec<Bson> = self.list.iter().cloned().collect();
                v.sort_by(compare);
                Bson::Array(v)
            }
            Kind::Push
            | Kind::AddToSet
            | Kind::ConcatArrays
            | Kind::SetUnion
            | Kind::FirstN
            | Kind::LastN => Bson::Array(self.list.iter().cloned().collect()),
            Kind::Count => i32::try_from(self.count).map_or(Bson::Int64(self.count), Bson::Int32),
            Kind::MergeObjects => Bson::Document(self.merged.clone()),
            Kind::StdDevPop | Kind::StdDevSamp if self.narrow => {
                let c = self.welford.0;
                let adj = if self.spec.kind == Kind::StdDevSamp {
                    c - 1
                } else {
                    c
                };
                match &self.dsums {
                    Some((_, _, nf)) if *nf > 0 => Bson::Double(f64::NAN),
                    Some((s, q, _)) if adj > 0 => {
                        let n = accum_num::Dec::from_i128(i128::from(c));
                        let var = q
                            .sub(&s.mul(s).div(&n))
                            .div(&accum_num::Dec::from_i128(i128::from(adj)));
                        let f = var.to_f64();
                        // A single value (or equal values): MongoDB answers 0.
                        if f == 0.0 {
                            Bson::Int32(0)
                        } else {
                            Bson::Double(f.sqrt())
                        }
                    }
                    _ => Bson::Null,
                }
            }
            Kind::StdDevPop | Kind::StdDevSamp => {
                let (c, _, m2) = self.welford;
                let adj = if self.spec.kind == Kind::StdDevSamp {
                    c - 1
                } else {
                    c
                };
                if adj <= 0 {
                    Bson::Null
                } else {
                    #[allow(clippy::cast_precision_loss)]
                    Bson::Double((m2 / adj as f64).sqrt())
                }
            }
            Kind::Top | Kind::TopN => {
                let s = self.keyed_sorted();
                if self.spec.kind == Kind::Top {
                    s.first().map_or(Bson::Null, |x| x.1.clone())
                } else {
                    Bson::Array(s.iter().take(self.n).map(|x| x.1.clone()).collect())
                }
            }
            Kind::Bottom | Kind::BottomN => {
                let s = self.keyed_sorted();
                if self.spec.kind == Kind::Bottom {
                    s.last().map_or(Bson::Null, |x| x.1.clone())
                } else {
                    let from = s.len().saturating_sub(self.n);
                    Bson::Array(s[from..].iter().map(|x| x.1.clone()).collect())
                }
            }
            Kind::MaxN | Kind::MinN => {
                let mut v: Vec<&Bson> = self.list.iter().collect();
                if self.spec.kind == Kind::MaxN {
                    v.sort_by(|a, b| compare(b, a));
                } else {
                    v.sort_by(|a, b| compare(a, b));
                }
                Bson::Array(v.into_iter().take(self.n).cloned().collect())
            }
            Kind::Median | Kind::Percentile => {
                let mut v = self.nums.clone();
                v.sort_by(f64::total_cmp);
                let at = |p: f64| -> Bson {
                    if v.is_empty() {
                        return Bson::Null;
                    }
                    #[allow(clippy::cast_precision_loss)]
                    let rank = (p * v.len() as f64).ceil();
                    #[allow(
                        clippy::cast_possible_truncation,
                        clippy::cast_sign_loss,
                        clippy::cast_possible_wrap
                    )]
                    let i = ((rank as i64) - 1).clamp(0, v.len() as i64 - 1) as usize;
                    Bson::Double(v[i])
                };
                if self.spec.kind == Kind::Median {
                    at(0.5)
                } else {
                    Bson::Array(self.spec.ps.iter().map(|p| at(*p)).collect())
                }
            }
        })
    }
}

/// Parses the accumulator fields of a `$group` (or `$bucket` output).
///
/// # Errors
/// 40234 for a field that isn't an accumulator object; the accumulators'.
pub(crate) fn parse_fields(
    spec: &Document,
    skip_id: bool,
) -> Result<Vec<(String, AccSpec)>, MongoError> {
    let mut out = Vec::new();
    for (k, v) in spec {
        if skip_id && k == "_id" {
            continue;
        }
        if k.contains('.') {
            return Err(err(
                40235,
                format!("The field name '{k}' cannot contain '.'"),
            ));
        }
        if k.starts_with('$') {
            return Err(err(
                40236,
                format!("The field '{k}' must not start with '$'"),
            ));
        }
        let Bson::Document(d) = v else {
            return Err(err(
                40234,
                format!("The field '{k}' must be an accumulator object"),
            ));
        };
        if d.len() != 1 {
            return Err(err(
                40238,
                format!("The field '{k}' must specify one accumulator"),
            ));
        }
        let Some((op, e)) = d.iter().next() else {
            return Err(err(
                40234,
                format!("The field '{k}' must be an accumulator object"),
            ));
        };
        // Server-side JavaScript (M4): `crate::serverjs::GroupJs`.
        if op == "$accumulator" {
            continue;
        }
        out.push((k.clone(), AccSpec::parse(op, e, Ctx::Group)?));
    }
    Ok(out)
}

/// Groups `docs` by `keys` (one per document) and accumulates: the groups
/// in first-seen order, each `{_id: key, field: value...}`.
///
/// # Errors
/// The accumulators'; 292 past `doc.agg_stage_memory_bytes`.
pub(crate) fn group_by(
    docs: &[Document],
    keys: Vec<Bson>,
    accs: &[(String, AccSpec)],
    lim: &AggLimits,
    stage: &str,
    mut js: Option<&mut crate::serverjs::GroupJs>,
) -> Result<Vec<Document>, MongoError> {
    struct G<'a> {
        id: Bson,
        states: Vec<AccState<'a>>,
    }
    let mut order: Vec<Vec<u8>> = Vec::new();
    let mut groups: HashMap<Vec<u8>, G<'_>> = HashMap::new();
    let mut bytes = 0usize;
    for (d, id) in docs.iter().zip(keys) {
        let k = crate::keyenc::key(&id);
        if !groups.contains_key(&k) {
            let mut states = Vec::with_capacity(accs.len());
            for (_, a) in accs {
                states.push(a.state(a.n_for(Some(&id), lim)?));
            }
            order.push(k.clone());
            groups.insert(k.clone(), G { id, states });
        }
        let Some(g) = groups.get_mut(&k) else {
            continue;
        };
        let vars = Vars::new(d, lim.now);
        for (i, (_, a)) in accs.iter().enumerate() {
            let it = a.item(d, &vars)?;
            let before = g.states[i].bytes;
            g.states[i].add(&it)?;
            bytes = (bytes + g.states[i].bytes).saturating_sub(before);
        }
        if let Some(j) = js.as_deref_mut() {
            j.add(&k, &g.id, &vars)?;
        }
        if bytes > lim.stage_bytes {
            return Err(MongoError::new(
                code::QUERY_EXCEEDED_MEMORY_LIMIT,
                format!(
                    "{stage} exceeded memory limit of {} bytes (doc.agg_stage_memory_bytes)",
                    lim.stage_bytes
                ),
            ));
        }
        // MongoDB 9.0's per-operation limit (M3).
        crate::opmem::check(bytes, stage)?;
    }
    let mut out = Vec::with_capacity(order.len());
    for k in order {
        let Some(g) = groups.remove(&k) else { continue };
        let mut d = doc! {"_id": g.id};
        for (i, (name, _)) in accs.iter().enumerate() {
            d.insert(name.clone(), g.states[i].result()?);
        }
        if let Some(j) = js.as_deref() {
            j.finish(&k, &mut d)?;
        }
        out.push(d);
    }
    Ok(out)
}

/// `$group`.
///
/// # Errors
/// 15955 without `_id`; the accumulators'.
pub(crate) fn group(
    docs: &[Document],
    spec: &Document,
    lim: &AggLimits,
) -> Result<Vec<Document>, MongoError> {
    let id_expr = spec
        .get("_id")
        .ok_or_else(|| err(15955, "a group specification must include an _id"))?;
    let accs = parse_fields(spec, true)?;
    let mut js = crate::serverjs::GroupJs::parse(spec)?;
    let mut keys = Vec::with_capacity(docs.len());
    for d in docs {
        let id = eval(id_expr, &Vars::new(d, lim.now))?;
        keys.push(if missing(&id) { Bson::Null } else { id });
    }
    group_by(
        docs,
        keys,
        &accs,
        lim,
        "$group",
        (!js.is_empty()).then_some(&mut js),
    )
}

#[cfg(test)]
mod tests {
    use bson::{Bson, DateTime, doc};

    use super::super::{AggLimits, run_stage};

    fn lim() -> AggLimits {
        AggLimits {
            stage_bytes: 1 << 20,
            now: DateTime::now(),
        }
    }

    /// MongoDB 8.1's `$concatArrays` and `$setUnion` accumulators, and
    /// 8.3's `$mergeObjects` window function (the 8.0 reference refuses
    /// them, so the corpus can't check them).
    #[test]
    fn newer_accumulators() {
        let docs = vec![
            doc! {"_id": 1, "g": 1, "a": [1, 2], "o": {"x": 1}},
            doc! {"_id": 2, "g": 1, "a": [2, 3], "o": {"y": 2}},
            doc! {"_id": 3, "g": 1, "o": {"x": 3}},
        ];
        let out = run_stage(
            docs.clone(),
            &doc! {"$group": {"_id": "$g", "c": {"$concatArrays": "$a"}, "u": {"$setUnion": "$a"}}},
            &lim(),
        )
        .unwrap();
        assert_eq!(
            out,
            vec![doc! {"_id": 1, "c": [1, 2, 2, 3], "u": [1, 2, 3]}]
        );
        let e = run_stage(
            vec![doc! {"g": 1, "a": 5}],
            &doc! {"$group": {"_id": "$g", "c": {"$concatArrays": "$a"}}},
            &lim(),
        )
        .unwrap_err();
        assert_eq!(e.code, 28664);
        let w = run_stage(
            docs,
            &doc! {"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"m": {"$mergeObjects": "$o", "window": {"documents": ["unbounded", "current"]}}}}},
            &lim(),
        )
        .unwrap();
        assert_eq!(w[2].get("m"), Some(&Bson::Document(doc! {"x": 3, "y": 2})));
    }
}
