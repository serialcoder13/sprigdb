//! Hybrid search ranking (chapter 39, "Hybrid ranking"; decisions S114
//! and S115): `$rankFusion` (MongoDB 8.0) and `$scoreFusion` (8.3) over
//! input pipelines of `$search`, `$vectorSearch`, `$geoNear`, `$match`,
//! `$sort`, `$skip`, `$limit` (and `$sample`, `$score`).
//!
//! Each input pipeline runs as its own aggregation on the collection; the
//! results are fused by `_id` and ordered by the fused score, highest
//! first, ties by `_id` ascending (as MongoDB 8.0 orders them). The score
//! is `{$meta: "score"}`; with `scoreDetails: true` the details are
//! `{$meta: "scoreDetails"}`, in MongoDB's shape.
//!
//! - **`$rankFusion`**, reciprocal rank fusion: a document's score is the
//!   sum, over the pipelines that output it, of `weight / (60 + rank)`,
//!   with ranks from 1.
//! - **`$scoreFusion`**: each pipeline's scores are normalized (`none`,
//!   `sigmoid`, or `minMaxScaler` over that pipeline's output), weighted,
//!   and combined: `avg` divides the sum by the number of pipelines (a
//!   pipeline that didn't output the document counts 0), `expression`
//!   evaluates `combination.expression` with each pipeline's weighted
//!   score bound as `$$<name>`.

use std::cmp::Ordering;
use std::collections::HashMap;

use bson::{Bson, Document, doc};
use sprig_proto_mongo::errors::{MongoError, code};

use super::{Engine, Ns, SqlRunner};
use crate::agg::{self, AggLimits};
use crate::engine::models::SCORE_FIELD;
use crate::keyenc;

/// The fused score (`{$meta: "score"}`).
pub const FUSION_SCORE: &str = "__sprig_score";
/// The fused score's details (`{$meta: "scoreDetails"}`).
pub const FUSION_DETAILS: &str = "__sprig_score_details";
/// An input pipeline's own score, carried out of its aggregation.
const IN_SCORE: &str = "__sprig_in_score";

/// RRF's constant, as MongoDB fixes it.
pub const RRF_K: f64 = 60.0;

const RRF_DESCRIPTION: &str = "value output by reciprocal rank fusion algorithm, computed as sum of (weight * (1 / (60 + rank))) across input pipelines from which this document is output, from:";
const SCORE_DESCRIPTION: &str = "the value calculated by combining the scores (either normalized or raw) across input pipelines from which this document is output from:";

/// The two stages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// `$rankFusion`.
    Rank,
    /// `$scoreFusion`.
    Score,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Rank => "$rankFusion",
            Self::Score => "$scoreFusion",
        }
    }
}

/// `input.normalization` of `$scoreFusion`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Norm {
    /// Raw scores.
    None,
    /// `1 / (1 + e^-x)`.
    Sigmoid,
    /// `(x - min) / (max - min)` over the pipeline's output.
    MinMax,
}

impl Norm {
    fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Sigmoid => "sigmoid",
            Self::MinMax => "minMaxScaler",
        }
    }
}

/// `combination.method` of `$scoreFusion`.
#[derive(Debug, Clone, PartialEq)]
pub enum Method {
    /// The average over the pipelines.
    Avg,
    /// An expression over `$$<pipeline name>`.
    Expr(Bson),
}

/// A parsed stage.
#[derive(Debug, Clone)]
pub struct Spec {
    /// Which stage.
    pub kind: Kind,
    /// The input pipelines, in the specification's order.
    pub pipelines: Vec<(String, Vec<Document>)>,
    /// Each pipeline's weight, as a double (1 when not given).
    pub weights: Vec<Bson>,
    /// `$scoreFusion`'s normalization.
    pub norm: Norm,
    /// `$scoreFusion`'s method.
    pub method: Method,
    /// `scoreDetails`.
    pub details: bool,
}

/// One input pipeline's output: each document and its own score.
#[derive(Debug, Clone, Default)]
pub struct Input {
    /// In rank order.
    pub docs: Vec<(Document, Option<f64>)>,
}

fn e(code: i32, msg: impl Into<String>) -> MongoError {
    MongoError::new(code, msg)
}

fn type_of(v: &Bson) -> &'static str {
    crate::update::type_name(v)
}

fn unknown(path: &str, k: &str) -> MongoError {
    e(
        40415,
        format!("BSON field '{path}.{k}' is an unknown field."),
    )
}

fn missing(path: &str) -> MongoError {
    e(
        40414,
        format!("BSON field '{path}' is missing but a required field"),
    )
}

fn num(v: &Bson) -> Option<f64> {
    match v {
        Bson::Double(d) => Some(*d),
        Bson::Int32(i) => Some(f64::from(*i)),
        #[allow(clippy::cast_precision_loss)]
        Bson::Int64(i) => Some(*i as f64),
        Bson::Decimal128(d) => d.to_string().parse().ok(),
        _ => None,
    }
}

fn edit_distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut cur = vec![i + 1; b.len() + 1];
        for (j, cb) in b.iter().enumerate() {
            cur[j + 1] = (prev[j] + usize::from(ca != *cb))
                .min(prev[j + 1] + 1)
                .min(cur[j] + 1);
        }
        prev = cur;
    }
    prev[b.len()]
}

/// Checks one input pipeline: selection stages only, and ranked (or
/// scored).
fn check_input(kind: Kind, p: &[Document]) -> Result<(), MongoError> {
    for s in p {
        let name = s.keys().next().map_or("", String::as_str);
        let ok = matches!(
            name,
            "$match" | "$search" | "$vectorSearch" | "$geoNear" | "$sort" | "$skip" | "$limit"
        ) || (kind == Kind::Rank && name == "$sample")
            || (kind == Kind::Score && name == "$score");
        if !ok {
            return Err(e(
                9_191_103,
                format!(
                    "{name} is not a selection stage because it modifies or transforms the input documents. Only stages that retrieve, limit, or order documents are allowed."
                ),
            ));
        }
        if name == "$geoNear"
            && let Ok(g) = s.get_document("$geoNear")
            && (g.contains_key("includeLocs") || g.contains_key("distanceField"))
        {
            return Err(e(
                9_191_103,
                "$geoNear is not a selection stage because it modifies or transforms the input documents (includeLocs, distanceField). Only stages that retrieve, limit, or order documents are allowed.",
            ));
        }
    }
    let first = p
        .first()
        .and_then(|s| s.keys().next())
        .map_or("", String::as_str);
    match kind {
        Kind::Rank => {
            let ranked = matches!(first, "$search" | "$vectorSearch" | "$geoNear")
                || p.iter().any(|s| s.contains_key("$sort"));
            if !ranked {
                return Err(e(
                    9_191_100,
                    "Pipeline did not begin with a ranked stage and did not contain an explicit $sort stage. All subpipelines to the $rankFusion stage must begin with one of $search or $vectorSearch, or have a custom $sort in the pipeline.",
                ));
            }
        }
        Kind::Score => {
            let text = first == "$match"
                && p.first()
                    .and_then(|s| s.get_document("$match").ok())
                    .is_some_and(|m| m.contains_key("$text"));
            let scored = matches!(first, "$search" | "$vectorSearch")
                || text
                || p.iter().any(|s| s.contains_key("$score"));
            if !scored {
                return Err(e(
                    9_402_500,
                    "Pipeline did not begin with a scored stage and did not contain an explicit $score stage. All subpipelines to the $scoreFusion stage must begin with one of $search, $vectorSearch, or $match with $text, or contain a $score stage.",
                ));
            }
        }
    }
    Ok(())
}

/// Parses `{$rankFusion: ...}` or `{$scoreFusion: ...}`.
///
/// # Errors
/// MongoDB's codes for a malformed specification.
pub fn parse(kind: Kind, arg: &Bson) -> Result<Spec, MongoError> {
    let st = kind.name();
    let Bson::Document(d) = arg else {
        return Err(e(
            code::FAILED_TO_PARSE,
            format!("{st} must take a nested object but found: {st}: {arg}"),
        ));
    };
    for k in d.keys() {
        if !matches!(k.as_str(), "input" | "combination" | "scoreDetails") {
            return Err(unknown(st, k));
        }
    }
    let input = match d.get("input") {
        None => return Err(missing(&format!("{st}.input"))),
        Some(Bson::Document(i)) => i,
        Some(v) => {
            return Err(e(
                code::TYPE_MISMATCH,
                format!(
                    "BSON field '{st}.input' is the wrong type '{}', expected type 'object'",
                    type_of(v)
                ),
            ));
        }
    };
    for k in input.keys() {
        let ok = k == "pipelines" || (kind == Kind::Score && k == "normalization");
        if !ok {
            return Err(unknown(&format!("{st}.input"), k));
        }
    }
    let pipes = match input.get("pipelines") {
        None => return Err(missing(&format!("{st}.input.pipelines"))),
        Some(Bson::Document(p)) => p,
        Some(v) => {
            return Err(e(
                code::TYPE_MISMATCH,
                format!(
                    "BSON field '{st}.input.pipelines' is the wrong type '{}', expected type 'object'",
                    type_of(v)
                ),
            ));
        }
    };
    if pipes.is_empty() {
        return Err(e(
            code::BAD_VALUE,
            "A hybrid scoring stage should be run with at least one pipeline.",
        ));
    }
    let mut pipelines = Vec::new();
    for (name, p) in pipes {
        let why = if name.is_empty() {
            Some((
                15_998,
                "FieldPath field names may not be empty strings.".to_owned(),
            ))
        } else if name.starts_with('$') {
            Some((
                16_410,
                format!("FieldPath field names may not start with '$', given '{name}'."),
            ))
        } else if name.contains('.') {
            Some((
                16_412,
                format!("FieldPath field names may not contain '.', given '{name}'."),
            ))
        } else if name.contains('\0') {
            Some((
                16_411,
                "FieldPath field names may not contain '\\0'.".to_owned(),
            ))
        } else {
            None
        };
        if let Some((c, m)) = why {
            return Err(e(
                c,
                format!(
                    "{st} pipeline names must follow the naming rules of field path expressions. :: caused by :: {m}"
                ),
            ));
        }
        let Bson::Array(a) = p else {
            return Err(e(
                code::TYPE_MISMATCH,
                format!(
                    "Error parsing {st}.input.pipelines :: caused by :: A pipeline must be an array of objects"
                ),
            ));
        };
        let mut stages = Vec::with_capacity(a.len());
        for s in a {
            let Bson::Document(s) = s else {
                return Err(e(
                    code::TYPE_MISMATCH,
                    format!(
                        "Error parsing {st}.input.pipelines :: caused by :: A pipeline must be an array of objects"
                    ),
                ));
            };
            stages.push(s.clone());
        }
        check_input(kind, &stages)?;
        pipelines.push((name.clone(), stages));
    }
    let norm = match input.get("normalization") {
        _ if kind == Kind::Rank => Norm::None,
        None => return Err(missing(&format!("{st}.input.normalization"))),
        Some(Bson::String(s)) => match s.as_str() {
            "none" => Norm::None,
            "sigmoid" => Norm::Sigmoid,
            "minMaxScaler" => Norm::MinMax,
            other => {
                return Err(e(
                    code::BAD_VALUE,
                    format!(
                        "Enumeration value '{other}' for field '{st}.input.normalization' is not a valid value."
                    ),
                ));
            }
        },
        Some(v) => {
            return Err(e(
                code::TYPE_MISMATCH,
                format!(
                    "BSON field '{st}.input.normalization' is the wrong type '{}', expected type 'string'",
                    type_of(v)
                ),
            ));
        }
    };
    let mut weights: Vec<Bson> = vec![Bson::Double(1.0); pipelines.len()];
    let mut method = Method::Avg;
    match d.get("combination") {
        None => {}
        Some(Bson::Document(c)) => {
            for k in c.keys() {
                let ok =
                    k == "weights" || (kind == Kind::Score && (k == "method" || k == "expression"));
                if !ok {
                    return Err(unknown(&format!("{st}.combination"), k));
                }
            }
            if let Some(w) = c.get("weights") {
                let Bson::Document(w) = w else {
                    return Err(e(
                        code::TYPE_MISMATCH,
                        format!(
                            "BSON field '{st}.combination.weights' is the wrong type '{}', expected type 'object'",
                            type_of(w)
                        ),
                    ));
                };
                let mut bad: Vec<(String, String)> = Vec::new();
                for (name, v) in w {
                    let Some(n) = num(v) else {
                        return Err(e(
                            13_118,
                            format!(
                                "expected {name} to have a numeric type, but it is a {}",
                                type_of(v)
                            ),
                        ));
                    };
                    if let Some(i) = pipelines.iter().position(|(p, _)| p == name) {
                        if n < 0.0 {
                            return Err(e(
                                9_460_300,
                                format!(
                                    "{} pipeline weight must be non-negative, but given {v} for pipeline '{name}'.",
                                    if kind == Kind::Rank {
                                        "Rank fusion"
                                    } else {
                                        "Score fusion"
                                    }
                                ),
                            ));
                        }
                        // MongoDB reports every weight as a double.
                        weights[i] = Bson::Double(n);
                    } else {
                        let best = pipelines
                            .iter()
                            .min_by_key(|(p, _)| edit_distance(name, p))
                            .map(|(p, _)| p.clone())
                            .unwrap_or_default();
                        bad.push((name.clone(), best));
                    }
                }
                if !bad.is_empty() {
                    let list: Vec<String> = bad
                        .iter()
                        .map(|(p, s)| format!("(provided: '{p}' -> suggested: '{s}')"))
                        .collect();
                    return Err(e(
                        9_967_500,
                        format!(
                            "{st} stage contained ({}) weight(s) in 'combination.weights' that did not reference valid pipeline names. Suggestions for valid pipeline names: {}",
                            bad.len(),
                            list.join(", ")
                        ),
                    ));
                }
            }
            if kind == Kind::Score {
                match c.get("method") {
                    None | Some(Bson::String(_)) => {}
                    Some(v) => {
                        return Err(e(
                            code::TYPE_MISMATCH,
                            format!(
                                "BSON field '{st}.combination.method' is the wrong type '{}', expected type 'string'",
                                type_of(v)
                            ),
                        ));
                    }
                }
                let m = c.get_str("method").unwrap_or("avg");
                method = match (m, c.get("expression")) {
                    ("avg", None) => Method::Avg,
                    ("expression", Some(x)) => {
                        if c.contains_key("weights") {
                            return Err(e(
                                code::BAD_VALUE,
                                "combination.weights and combination.expression cannot both be specified",
                            ));
                        }
                        Method::Expr(x.clone())
                    }
                    ("expression", None) => {
                        return Err(e(
                            code::BAD_VALUE,
                            "combination.expression must be specified when combination.method is 'expression'",
                        ));
                    }
                    ("avg", Some(_)) => {
                        return Err(e(
                            code::BAD_VALUE,
                            "combination.expression can only be specified when combination.method is 'expression'",
                        ));
                    }
                    (other, _) => {
                        return Err(e(
                            code::BAD_VALUE,
                            format!(
                                "Enumeration value '{other}' for field '{st}.combination.method' is not a valid value."
                            ),
                        ));
                    }
                };
            }
        }
        Some(v) => {
            return Err(e(
                code::TYPE_MISMATCH,
                format!(
                    "BSON field '{st}.combination' is the wrong type '{}', expected type 'object'",
                    type_of(v)
                ),
            ));
        }
    }
    let details = match d.get("scoreDetails") {
        None => false,
        Some(Bson::Boolean(b)) => *b,
        Some(v) => {
            return Err(e(
                code::TYPE_MISMATCH,
                format!(
                    "BSON field '{st}.scoreDetails' is the wrong type '{}', expected type 'bool'",
                    type_of(v)
                ),
            ));
        }
    };
    Ok(Spec {
        kind,
        pipelines,
        weights,
        norm,
        method,
        details,
    })
}

/// One fused document as it is built.
struct Fused {
    doc: Document,
    id: Bson,
    /// Per pipeline: rank (from 1) and own score, when it output the
    /// document.
    seen: Vec<Option<(usize, Option<f64>)>>,
}

fn collect(spec: &Spec, inputs: &[Input]) -> Vec<Fused> {
    let mut order: Vec<Fused> = Vec::new();
    let mut at: HashMap<Vec<u8>, usize> = HashMap::new();
    for (p, input) in inputs.iter().enumerate() {
        for (r, (d, s)) in input.docs.iter().enumerate() {
            let id = d.get("_id").cloned().unwrap_or(Bson::Null);
            let k = keyenc::key(&id);
            let i = *at.entry(k).or_insert_with(|| {
                order.push(Fused {
                    doc: d.clone(),
                    id: id.clone(),
                    seen: vec![None; spec.pipelines.len()],
                });
                order.len() - 1
            });
            // A document twice in one pipeline keeps its best rank.
            if order[i].seen[p].is_none() {
                order[i].seen[p] = Some((r + 1, *s));
            }
        }
    }
    order
}

fn finish(mut out: Vec<(Fused, f64, Option<Document>)>) -> Vec<Document> {
    out.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| keyenc::compare(&a.0.id, &b.0.id))
    });
    out.into_iter()
        .map(|(f, score, det)| {
            let mut d = f.doc;
            d.remove(SCORE_FIELD);
            d.remove(IN_SCORE);
            d.insert(FUSION_SCORE, Bson::Double(score));
            if let Some(det) = det {
                d.insert(FUSION_DETAILS, det);
            }
            d
        })
        .collect()
}

/// Reciprocal rank fusion of `inputs` (one per pipeline of `spec`).
#[must_use]
pub fn rank_fusion(spec: &Spec, inputs: &[Input]) -> Vec<Document> {
    let w: Vec<f64> = spec.weights.iter().map(|v| num(v).unwrap_or(1.0)).collect();
    let out = collect(spec, inputs)
        .into_iter()
        .map(|f| {
            #[allow(clippy::cast_precision_loss)]
            let score: f64 = f
                .seen
                .iter()
                .enumerate()
                .filter_map(|(p, s)| s.map(|(r, _)| w[p] * (1.0 / (RRF_K + r as f64))))
                .sum();
            let det = spec.details.then(|| {
                let details: Vec<Bson> = f
                    .seen
                    .iter()
                    .enumerate()
                    .map(|(p, s)| {
                        let mut x = doc! {
                            "inputPipelineName": spec.pipelines[p].0.clone(),
                            "rank": s.map_or(0, |(r, _)| i32::try_from(r).unwrap_or(i32::MAX)),
                            "weight": spec.weights[p].clone(),
                        };
                        if let Some((_, own)) = s {
                            if let Some(v) = own {
                                x.insert("value", *v);
                            }
                            x.insert("details", Bson::Array(Vec::new()));
                        }
                        Bson::Document(x)
                    })
                    .collect();
                doc! {"value": score, "description": RRF_DESCRIPTION, "details": details}
            });
            (f, score, det)
        })
        .collect();
    finish(out)
}

fn sigmoid(x: f64) -> f64 {
    1.0 / (1.0 + (-x).exp())
}

/// Score fusion of `inputs`.
///
/// # Errors
/// The combination expression's.
pub fn score_fusion(
    spec: &Spec,
    inputs: &[Input],
    now: bson::DateTime,
) -> Result<Vec<Document>, MongoError> {
    let w: Vec<f64> = spec.weights.iter().map(|v| num(v).unwrap_or(1.0)).collect();
    // Each pipeline's range, for minMaxScaler.
    let ranges: Vec<(f64, f64)> = inputs
        .iter()
        .map(|i| {
            i.docs
                .iter()
                .filter_map(|(_, s)| *s)
                .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), s| {
                    (lo.min(s), hi.max(s))
                })
        })
        .collect();
    let norm = |p: usize, x: f64| match spec.norm {
        Norm::None => x,
        Norm::Sigmoid => sigmoid(x),
        Norm::MinMax => {
            let (lo, hi) = ranges[p];
            if hi > lo { (x - lo) / (hi - lo) } else { 0.0 }
        }
    };
    let n = spec.pipelines.len();
    let mut out = Vec::new();
    for f in collect(spec, inputs) {
        let normed: Vec<Option<(f64, f64)>> = f
            .seen
            .iter()
            .enumerate()
            .map(|(p, s)| {
                s.map(|(_, own)| {
                    let raw = own.unwrap_or(0.0);
                    (raw, norm(p, raw))
                })
            })
            .collect();
        let weighted: Vec<f64> = normed
            .iter()
            .enumerate()
            .map(|(p, s)| s.map_or(0.0, |(_, v)| v * w[p]))
            .collect();
        let score = match &spec.method {
            #[allow(clippy::cast_precision_loss)]
            Method::Avg => weighted.iter().sum::<f64>() / n as f64,
            Method::Expr(x) => {
                let mut bound = x.clone();
                for (p, (name, _)) in spec.pipelines.iter().enumerate() {
                    bound = bind_var(&bound, name, &Bson::Double(weighted[p]));
                }
                let v = agg::eval(&bound, &agg::Vars::new(&f.doc, now))?;
                num(&v).ok_or_else(|| {
                    e(
                        9_460_301,
                        format!("$scoreFusion's combination.expression must evaluate to a number, but it is a {}", type_of(&v)),
                    )
                })?
            }
        };
        let det = spec.details.then(|| {
            let details: Vec<Bson> = normed
                .iter()
                .enumerate()
                .map(|(p, s)| {
                    let mut x = doc! {
                        "inputPipelineName": spec.pipelines[p].0.clone(),
                        "inputPipelineRawScore": s.map_or(0.0, |(raw, _)| raw),
                        "weight": spec.weights[p].clone(),
                    };
                    if let Some((_, v)) = s {
                        x.insert("value", *v);
                        x.insert("details", Bson::Array(Vec::new()));
                    }
                    Bson::Document(x)
                })
                .collect();
            let combination = match &spec.method {
                Method::Avg => doc! {"method": "average"},
                Method::Expr(x) => {
                    doc! {"method": "custom expression", "expression": x.to_string()}
                }
            };
            doc! {
                "value": score,
                "description": SCORE_DESCRIPTION,
                "normalization": spec.norm.name(),
                "combination": combination,
                "details": details,
            }
        });
        out.push((f, score, det));
    }
    Ok(finish(out))
}

/// `$$name` in `e` replaced by the literal `v`.
fn bind_var(e: &Bson, name: &str, v: &Bson) -> Bson {
    match e {
        Bson::String(s) if s == &format!("$${name}") => doc! {"$literal": v.clone()}.into(),
        Bson::Document(d) => Bson::Document(
            d.iter()
                .map(|(k, x)| (k.clone(), bind_var(x, name, v)))
                .collect(),
        ),
        Bson::Array(a) => Bson::Array(a.iter().map(|x| bind_var(x, name, v)).collect()),
        other => other.clone(),
    }
}

/// `$rankFusion` or `$scoreFusion` inside a `$lookup` or `$unionWith`
/// sub-pipeline: MongoDB refuses both there.
///
/// # Errors
/// 11178500.
pub fn check_nested(stages: &[Bson]) -> Result<(), MongoError> {
    fn walk(stages: &[Bson], nested: bool) -> Result<(), MongoError> {
        for s in stages {
            let Some((name, arg)) = s.as_document().and_then(|d| d.iter().next()) else {
                continue;
            };
            if nested && matches!(name.as_str(), "$rankFusion" | "$scoreFusion") {
                return Err(e(
                    11_178_500,
                    format!("{name} is not allowed within a sub-pipeline"),
                ));
            }
            if matches!(name.as_str(), "$lookup" | "$unionWith")
                && let Some(Bson::Array(p)) = arg.as_document().and_then(|d| d.get("pipeline"))
            {
                walk(p, true)?;
            }
        }
        Ok(())
    }
    walk(stages, false)
}

/// The `$score` stage (MongoDB 8.2) in an input pipeline: `score` is an
/// expression; `normalization` as `$scoreFusion`'s; `weight` multiplies.
///
/// # Errors
/// The expression's; 40415 for unknown fields.
pub fn score_stage(
    docs: Vec<Document>,
    arg: &Bson,
    now: bson::DateTime,
) -> Result<Vec<Document>, MongoError> {
    let Bson::Document(d) = arg else {
        return Err(e(code::FAILED_TO_PARSE, "$score must take a nested object"));
    };
    for k in d.keys() {
        if !matches!(
            k.as_str(),
            "score" | "normalization" | "weight" | "scoreDetails"
        ) {
            return Err(unknown("$score", k));
        }
    }
    let x = d.get("score").ok_or_else(|| missing("$score.score"))?;
    let weight = d.get("weight").and_then(num).unwrap_or(1.0);
    let mut scores = Vec::with_capacity(docs.len());
    for doc in &docs {
        let v = agg::eval(x, &agg::Vars::new(doc, now))?;
        scores.push(num(&v).ok_or_else(|| {
            e(
                10_017_300,
                format!(
                    "$score's score must evaluate to a number, but it is a {}",
                    type_of(&v)
                ),
            )
        })?);
    }
    let (lo, hi) = scores
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(a, b), s| {
            (a.min(*s), b.max(*s))
        });
    let norm = match d.get_str("normalization").unwrap_or("none") {
        "none" => Norm::None,
        "sigmoid" => Norm::Sigmoid,
        "minMaxScaler" => Norm::MinMax,
        other => {
            return Err(e(
                code::BAD_VALUE,
                format!(
                    "Enumeration value '{other}' for field '$score.normalization' is not a valid value."
                ),
            ));
        }
    };
    Ok(docs
        .into_iter()
        .zip(scores)
        .map(|(mut doc, s)| {
            let v = match norm {
                Norm::None => s,
                Norm::Sigmoid => sigmoid(s),
                Norm::MinMax if hi > lo => (s - lo) / (hi - lo),
                Norm::MinMax => 0.0,
            };
            doc.insert(SCORE_FIELD, v * weight);
            doc
        })
        .collect())
}

impl Engine {
    /// `$rankFusion` or `$scoreFusion` as the first stage: the fused
    /// documents; `None` when the pipeline starts with neither.
    ///
    /// # Errors
    /// The specification's; 10170100 when either stage isn't first; the
    /// input pipelines'.
    pub(super) async fn fusion_stage(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        pipeline: &[Document],
        lim: &AggLimits,
    ) -> Result<Option<Vec<Document>>, MongoError> {
        for s in pipeline.iter().skip(1) {
            if let Some(k) = s
                .keys()
                .next()
                .filter(|k| matches!(k.as_str(), "$rankFusion" | "$scoreFusion"))
            {
                return Err(e(
                    10_170_100,
                    format!("{k} can only be the first stage of an aggregation pipeline."),
                ));
            }
        }
        let Some((name, arg)) = pipeline.first().and_then(|s| s.iter().next()) else {
            return Ok(None);
        };
        let kind = match name.as_str() {
            "$rankFusion" => Kind::Rank,
            "$scoreFusion" => Kind::Score,
            _ => return Ok(None),
        };
        let spec = parse(kind, arg)?;
        let mut inputs = Vec::with_capacity(spec.pipelines.len());
        for (_, p) in &spec.pipelines {
            inputs.push(Box::pin(self.input_docs(sql, ns, p, lim)).await?);
        }
        Ok(Some(match kind {
            Kind::Rank => rank_fusion(&spec, &inputs),
            Kind::Score => score_fusion(&spec, &inputs, lim.now)?,
        }))
    }

    /// One input pipeline's documents, each with its own score: the stages
    /// up to the first `$score` run as an aggregation (with the score kept
    /// in a field), the rest in the gateway.
    async fn input_docs(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        p: &[Document],
        lim: &AggLimits,
    ) -> Result<Input, MongoError> {
        let cut = p
            .iter()
            .position(|s| s.contains_key("$score"))
            .unwrap_or(p.len());
        let mut head: Vec<Document> = p[..cut].to_vec();
        let mut add = Document::new();
        add.insert(IN_SCORE, doc! {"$meta": "searchScore"});
        head.push(doc! {"$addFields": add});
        let mut docs = Box::pin(self.aggregate(sql, ns, &head)).await?;
        for d in &mut docs {
            if let Some(s) = d.remove(IN_SCORE).as_ref().and_then(num) {
                d.insert(SCORE_FIELD, s);
            }
        }
        for s in &p[cut..] {
            docs = match s.get("$score") {
                Some(a) => score_stage(docs, a, lim.now)?,
                None => agg::run_stage(docs, s, lim)?,
            };
        }
        Ok(Input {
            docs: docs
                .into_iter()
                .map(|mut d| {
                    let s = d.remove(SCORE_FIELD).as_ref().and_then(num);
                    (d, s)
                })
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests;
