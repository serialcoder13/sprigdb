//! Query filters: parsing into a predicate tree and the MongoDB matcher
//! (chapter 31, "Filter translation" and "Recheck"; build step 7).
//!
//! The same tree feeds the SQL translator ([`crate::translate`]) and the
//! recheck, which runs every candidate MariaDB returns through
//! [`Pred::matches`] with MongoDB's semantics: a path matches against each
//! array element and against the array whole, comparisons stay within a
//! type class (type bracketing), and separate conditions on one array path
//! may be met by different elements.

use std::cmp::Ordering;

use bson::{Bson, Document};
use sprig_proto_mongo::errors::{MongoError, code};

use crate::keyenc::{self, class, cmp_class, compare};

/// A compiled regular expression with its MongoDB source.
#[derive(Debug, Clone)]
pub struct Re {
    /// The pattern as written.
    pub pattern: String,
    /// The options as written (`i`, `m`, `s`, `x`, `u`).
    pub options: String,
    /// The compiled matcher.
    pub re: regex::Regex,
}

impl Re {
    /// Compiles a MongoDB regex.
    ///
    /// # Errors
    /// 2 `BadValue` for an invalid pattern or option.
    pub fn new(pattern: &str, options: &str) -> Result<Self, MongoError> {
        let mut flags = String::new();
        for o in options.chars() {
            match o {
                'i' | 'm' | 's' | 'x' => flags.push(o),
                'u' => {}
                _ => {
                    return Err(MongoError::new(
                        51108,
                        format!("invalid flag in regex options: {o}"),
                    ));
                }
            }
        }
        let src = if flags.is_empty() {
            pattern.to_owned()
        } else {
            format!("(?{flags}){pattern}")
        };
        let re = regex::Regex::new(&src)
            .map_err(|e| MongoError::new(51091, format!("Regular expression is invalid: {e}")))?;
        Ok(Self {
            pattern: pattern.to_owned(),
            options: options.to_owned(),
            re,
        })
    }

    /// A literal prefix the pattern requires (`^abc`), when it has one and
    /// no options change its meaning.
    #[must_use]
    pub fn literal_prefix(&self) -> Option<String> {
        if !self.options.chars().all(|c| c == 's' || c == 'u') {
            return None;
        }
        let rest = self.pattern.strip_prefix('^')?;
        let mut out = String::new();
        let mut chars = rest.chars().peekable();
        for c in chars.by_ref() {
            if "\\.^$|?*+()[]{}".contains(c) {
                // A quantifier makes the previous character optional.
                if matches!(c, '?' | '*' | '{') {
                    out.pop();
                }
                break;
            }
            out.push(c);
        }
        let _ = chars.peek();
        (!out.is_empty()).then_some(out)
    }
}

/// One BSON type selector of `$type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeSel {
    /// A BSON type number (1 double ... 19 decimal, -1 minKey, 127 maxKey).
    Code(i32),
    /// `"number"`.
    Number,
}

/// A condition on the values at one path.
#[derive(Debug, Clone)]
pub enum FieldOp {
    /// `$eq`.
    Eq(Bson),
    /// `$ne`.
    Ne(Bson),
    /// `$gt`.
    Gt(Bson),
    /// `$gte`.
    Gte(Bson),
    /// `$lt`.
    Lt(Bson),
    /// `$lte`.
    Lte(Bson),
    /// `$in` (values and regexes).
    In(Vec<InItem>),
    /// `$nin`.
    Nin(Vec<InItem>),
    /// `$exists`.
    Exists(bool),
    /// `$type`.
    Type(Vec<TypeSel>),
    /// `$size`.
    Size(i64),
    /// `$all`.
    All(Vec<FieldOp>),
    /// `$elemMatch` over a sub-filter (documents) or operators (values).
    ElemMatch(Box<ElemMatch>),
    /// `$regex`, or a regex given as the value.
    Regex(Re),
    /// `$not`: the inner conditions, all of which must fail together.
    Not(Vec<FieldOp>),
    /// `$mod [divisor, remainder]`.
    Mod(i64, i64),
    /// `$geoWithin`, `$geoIntersects`, `$near`, `$nearSphere` (chapter
    /// 36, R6 round 3).
    Geo(Box<crate::geo::GeoOp>),
    /// `$bitsAllSet`, `$bitsAnySet`, `$bitsAllClear`, `$bitsAnyClear`
    /// (M3, decision S95).
    Bits(crate::validate::bits::Bits),
}

/// An `$in` element.
#[derive(Debug, Clone)]
pub enum InItem {
    /// A value, compared with MongoDB equality.
    Value(Bson),
    /// A regex.
    Regex(Re),
}

/// The body of `$elemMatch`.
#[derive(Debug, Clone)]
pub enum ElemMatch {
    /// `{$elemMatch: {a: 1, b: {$gt: 2}}}`: elements are documents
    /// matched against a filter.
    Doc(Pred),
    /// `{$elemMatch: {$gt: 1, $lt: 5}}`: one element meets every
    /// operator.
    Ops(Vec<FieldOp>),
}

/// A predicate tree.
#[derive(Debug, Clone)]
pub enum Pred {
    /// Every child.
    And(Vec<Pred>),
    /// Any child.
    Or(Vec<Pred>),
    /// No child.
    Nor(Vec<Pred>),
    /// A condition on a path.
    Field {
        /// The dotted path.
        path: String,
        /// The condition.
        op: FieldOp,
    },
    /// `$expr`, evaluated by the aggregation evaluator.
    Expr(Bson),
    /// `$jsonSchema` (M3, decision S90), on the whole document.
    JsonSchema(std::sync::Arc<crate::validate::schema::Schema>),
    /// `$where` (M4, decision S110): server-side JavaScript per document.
    Where(crate::serverjs::Where),
    /// Always true (`{}`, `$comment`).
    True,
}

fn bad(msg: impl Into<String>) -> MongoError {
    MongoError::new(code::BAD_VALUE, msg)
}

/// Parses a filter document.
///
/// # Errors
/// 2 `BadValue` for unknown operators or malformed arguments; 39662 for
/// `$where`, `$text` outside its stage and other unsupported operators.
pub fn parse(filter: &Document) -> Result<Pred, MongoError> {
    let mut all = Vec::new();
    for (k, v) in filter {
        if let Some(op) = k.strip_prefix('$') {
            match op {
                "and" | "or" | "nor" => {
                    let Bson::Array(a) = v else {
                        return Err(bad(format!("${op} must be an array")));
                    };
                    if a.is_empty() {
                        return Err(bad("$and/$or/$nor must be a nonempty array"));
                    }
                    let mut kids = Vec::new();
                    for x in a {
                        let Bson::Document(d) = x else {
                            return Err(bad(format!("${op} argument's entries must be objects")));
                        };
                        kids.push(parse(d)?);
                    }
                    all.push(match op {
                        "and" => Pred::And(kids),
                        "or" => Pred::Or(kids),
                        _ => Pred::Nor(kids),
                    });
                }
                "expr" => all.push(Pred::Expr(v.clone())),
                "jsonSchema" => {
                    let Bson::Document(d) = v else {
                        return Err(MongoError::new(
                            code::TYPE_MISMATCH,
                            "$jsonSchema must be an object",
                        ));
                    };
                    all.push(Pred::JsonSchema(std::sync::Arc::new(
                        crate::validate::schema::Schema::parse_root(d)?,
                    )));
                }
                "comment" => {}
                // MongoDB rewrites `$sampleRate: r` as `$expr: {$lt: [{$rand:
                // {}}, r]}` (M3, for M1's list); always rechecked.
                "sampleRate" => {
                    let r = match v {
                        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => {
                            crate::validate::schema::as_number(v)
                        }
                        _ => None,
                    };
                    let Some(r) = r.filter(|r| (0.0..=1.0).contains(r)) else {
                        return Err(bad("numeric argument to $sampleRate must be in [0, 1]"));
                    };
                    all.push(Pred::Expr(Bson::Document(
                        bson::doc! {"$lt": [{"$rand": {}}, r]},
                    )));
                }
                "where" => all.push(Pred::Where(crate::serverjs::Where::parse(v)?)),
                "text" | "geoWithin" | "near" => {
                    return Err(MongoError::new(
                        code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                        format!("${op} is not supported in this release"),
                    ));
                }
                _ => return Err(bad(format!("unknown top level operator: ${op}"))),
            }
            continue;
        }
        for op in parse_field(v)? {
            all.push(Pred::Field {
                path: k.clone(),
                op,
            });
        }
    }
    Ok(match all.len() {
        0 => Pred::True,
        1 => all.pop().unwrap_or(Pred::True),
        _ => Pred::And(all),
    })
}

fn is_op_doc(d: &Document) -> bool {
    d.keys().next().is_some_and(|k| k.starts_with('$'))
}

/// Parses the value of a field condition: a literal (equality), a regex,
/// or an operator document.
fn parse_field(v: &Bson) -> Result<Vec<FieldOp>, MongoError> {
    match v {
        Bson::Document(d) if is_op_doc(d) => parse_ops(d),
        Bson::RegularExpression(r) => Ok(vec![FieldOp::Regex(Re::new(&r.pattern, &r.options)?)]),
        _ => Ok(vec![FieldOp::Eq(v.clone())]),
    }
}

fn parse_ops(d: &Document) -> Result<Vec<FieldOp>, MongoError> {
    let mut ops = Vec::new();
    let options = d.get_str("$options").ok();
    for (k, v) in d {
        let op = match k.as_str() {
            "$eq" => FieldOp::Eq(v.clone()),
            "$ne" => FieldOp::Ne(v.clone()),
            "$gt" => FieldOp::Gt(v.clone()),
            "$gte" => FieldOp::Gte(v.clone()),
            "$lt" => FieldOp::Lt(v.clone()),
            "$lte" => FieldOp::Lte(v.clone()),
            "$in" | "$nin" => {
                let Bson::Array(a) = v else {
                    return Err(bad(format!("{k} needs an array")));
                };
                let items = a
                    .iter()
                    .map(|x| match x {
                        Bson::RegularExpression(r) => {
                            Ok(InItem::Regex(Re::new(&r.pattern, &r.options)?))
                        }
                        Bson::Document(d) if is_op_doc(d) => Err(bad("cannot nest $ under $in")),
                        other => Ok(InItem::Value(other.clone())),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if k == "$in" {
                    FieldOp::In(items)
                } else {
                    FieldOp::Nin(items)
                }
            }
            "$exists" => FieldOp::Exists(truthy(v)),
            "$type" => {
                let list = match v {
                    Bson::Array(a) => a.clone(),
                    other => vec![other.clone()],
                };
                FieldOp::Type(list.iter().map(type_sel).collect::<Result<_, _>>()?)
            }
            "$size" => FieldOp::Size(
                as_i64(v)
                    .filter(|n| *n >= 0)
                    .ok_or_else(|| bad("$size needs a non-negative whole number"))?,
            ),
            "$all" => {
                let Bson::Array(a) = v else {
                    return Err(bad("$all needs an array"));
                };
                let mut inner = Vec::new();
                for x in a {
                    match x {
                        Bson::Document(d) if d.contains_key("$elemMatch") => {
                            inner.extend(parse_ops(d)?);
                        }
                        Bson::RegularExpression(r) => {
                            inner.push(FieldOp::Regex(Re::new(&r.pattern, &r.options)?));
                        }
                        other => inner.push(FieldOp::Eq(other.clone())),
                    }
                }
                FieldOp::All(inner)
            }
            "$elemMatch" => {
                let Bson::Document(e) = v else {
                    return Err(bad("$elemMatch needs an Object"));
                };
                let ops_only = !e.is_empty()
                    && e.keys().all(|k| {
                        k.starts_with('$')
                            && !matches!(k.as_str(), "$and" | "$or" | "$nor" | "$expr")
                    });
                if e.contains_key("$where") {
                    return Err(bad("$where can only be applied to the top-level document"));
                }
                FieldOp::ElemMatch(Box::new(if ops_only {
                    ElemMatch::Ops(parse_ops(e)?)
                } else {
                    ElemMatch::Doc(parse(e)?)
                }))
            }
            "$regex" => match v {
                Bson::String(p) => FieldOp::Regex(Re::new(p, options.unwrap_or(""))?),
                Bson::RegularExpression(r) => {
                    FieldOp::Regex(Re::new(&r.pattern, options.unwrap_or(&r.options))?)
                }
                _ => return Err(bad("$regex has to be a string")),
            },
            "$options" => {
                if !d.contains_key("$regex") {
                    return Err(bad("$options needs a $regex"));
                }
                continue;
            }
            "$not" => match v {
                Bson::Document(n) if is_op_doc(n) => FieldOp::Not(parse_ops(n)?),
                Bson::RegularExpression(r) => {
                    FieldOp::Not(vec![FieldOp::Regex(Re::new(&r.pattern, &r.options)?)])
                }
                _ => return Err(bad("$not needs a regex or a document")),
            },
            "$mod" => {
                let Bson::Array(a) = v else {
                    return Err(bad("malformed mod, needs to be an array"));
                };
                let (Some(dv), Some(rm)) = (a.first().and_then(as_i64), a.get(1).and_then(as_i64))
                else {
                    return Err(bad("malformed mod, not enough elements"));
                };
                if dv == 0 {
                    return Err(bad("divisor cannot be 0"));
                }
                FieldOp::Mod(dv, rm)
            }
            "$comment" => continue,
            // Chapter 36 (R6 round 3); `$maxDistance` and `$minDistance`
            // belong to a sibling `$near`.
            "$geoWithin" | "$within" | "$geoIntersects" | "$near" | "$nearSphere" => {
                FieldOp::Geo(Box::new(crate::geo::parse(k, v, d)?))
            }
            "$maxDistance" | "$minDistance"
                if d.contains_key("$near") || d.contains_key("$nearSphere") =>
            {
                continue;
            }
            "$bitsAllSet" | "$bitsAnySet" | "$bitsAllClear" | "$bitsAnyClear" => {
                FieldOp::Bits(crate::validate::bits::Bits::parse(k, v)?)
            }
            "$where" => return Err(bad("$where cannot be applied to a field")),
            "$text" => {
                return Err(MongoError::new(
                    code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                    format!("{k} is not supported in this release"),
                ));
            }
            _ => return Err(bad(format!("unknown operator: {k}"))),
        };
        ops.push(op);
    }
    Ok(ops)
}

/// MongoDB truthiness of an operator argument.
#[must_use]
pub fn truthy(v: &Bson) -> bool {
    match v {
        Bson::Boolean(b) => *b,
        Bson::Null | Bson::Undefined => false,
        Bson::Int32(i) => *i != 0,
        Bson::Int64(i) => *i != 0,
        Bson::Double(f) => *f != 0.0,
        _ => true,
    }
}

/// A whole number from any numeric value.
#[must_use]
pub fn as_i64(v: &Bson) -> Option<i64> {
    match v {
        Bson::Int32(i) => Some(i64::from(*i)),
        Bson::Int64(i) => Some(*i),
        #[allow(clippy::cast_possible_truncation)]
        Bson::Double(f) if f.fract() == 0.0 && f.is_finite() => Some(*f as i64),
        Bson::Decimal128(d) => d.to_string().parse().ok(),
        _ => None,
    }
}

fn type_sel(v: &Bson) -> Result<TypeSel, MongoError> {
    if let Some(n) = as_i64(v) {
        return i32::try_from(n)
            .map(TypeSel::Code)
            .map_err(|_| bad("invalid $type number"));
    }
    let Bson::String(s) = v else {
        return Err(bad("type must be represented as a number or a string"));
    };
    Ok(match s.as_str() {
        "number" => TypeSel::Number,
        name => TypeSel::Code(
            type_code(name).ok_or_else(|| bad(format!("Unknown type name alias: {name}")))?,
        ),
    })
}

/// The BSON type number of a `$type` alias.
#[must_use]
pub fn type_code(name: &str) -> Option<i32> {
    Some(match name {
        "double" => 1,
        "string" => 2,
        "object" => 3,
        "array" => 4,
        "binData" => 5,
        "undefined" => 6,
        "objectId" => 7,
        "bool" => 8,
        "date" => 9,
        "null" => 10,
        "regex" => 11,
        "dbPointer" => 12,
        "javascript" => 13,
        "symbol" => 14,
        "javascriptWithScope" => 15,
        "int" => 16,
        "timestamp" => 17,
        "long" => 18,
        "decimal" => 19,
        "minKey" => -1,
        "maxKey" => 127,
        _ => return None,
    })
}

/// The BSON type number of a value.
#[must_use]
pub fn bson_type_code(v: &Bson) -> i32 {
    match v {
        Bson::Double(_) => 1,
        Bson::String(_) => 2,
        Bson::Document(_) => 3,
        Bson::Array(_) => 4,
        Bson::Binary(_) => 5,
        Bson::Undefined => 6,
        Bson::ObjectId(_) => 7,
        Bson::Boolean(_) => 8,
        Bson::DateTime(_) => 9,
        Bson::Null => 10,
        Bson::RegularExpression(_) => 11,
        Bson::DbPointer(_) => 12,
        Bson::JavaScriptCode(_) => 13,
        Bson::Symbol(_) => 14,
        Bson::JavaScriptCodeWithScope(_) => 15,
        Bson::Int32(_) => 16,
        Bson::Timestamp(_) => 17,
        Bson::Int64(_) => 18,
        Bson::Decimal128(_) => 19,
        Bson::MinKey => -1,
        Bson::MaxKey => 127,
    }
}

/// The values at a dotted path, as MongoDB sees them.
#[derive(Debug, Default)]
pub struct Values<'a> {
    /// Values found, with leaf arrays' elements after the array.
    pub vals: Vec<&'a Bson>,
    /// Leaf values only, arrays not expanded.
    pub leaves: Vec<&'a Bson>,
    /// Some branch of the path was missing.
    pub missing: bool,
    /// A leaf was named by position (`"a.0"`): MongoDB takes it whole,
    /// arrays included (S136).
    pub positional: bool,
}

/// Collects the values at `path` in `doc`.
#[must_use]
pub fn values_at<'a>(doc: &'a Document, path: &str) -> Values<'a> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut out = Values::default();
    match doc.get(parts[0]) {
        Some(v) => walk(v, &parts[1..], &mut out),
        None => out.missing = true,
    }
    out
}

fn walk<'a>(v: &'a Bson, rest: &[&str], out: &mut Values<'a>) {
    if rest.is_empty() {
        out.leaves.push(v);
        out.vals.push(v);
        if let Bson::Array(a) = v {
            out.vals.extend(a.iter());
        }
        return;
    }
    match v {
        Bson::Document(d) => match d.get(rest[0]) {
            Some(x) => walk(x, &rest[1..], out),
            None => out.missing = true,
        },
        Bson::Array(a) => {
            let mut any = false;
            if let Ok(i) = rest[0].parse::<usize>()
                && let Some(x) = a.get(i)
            {
                if rest.len() == 1 {
                    // A positional path's last part (`"a.0"`) names the
                    // element itself: MongoDB compares an array there
                    // whole, without trying its elements (S93, S136).
                    out.leaves.push(x);
                    out.vals.push(x);
                    out.positional = true;
                } else {
                    walk(x, &rest[1..], out);
                }
                any = true;
            }
            for e in a {
                if let Bson::Document(d) = e {
                    match d.get(rest[0]) {
                        Some(x) => {
                            walk(x, &rest[1..], out);
                            any = true;
                        }
                        None => out.missing = true,
                    }
                }
            }
            if !any && a.is_empty() {
                out.missing = true;
            }
        }
        _ => out.missing = true,
    }
}

fn is_nullish(v: &Bson) -> bool {
    matches!(v, Bson::Null | Bson::Undefined)
}

/// Whether `v` equals the literal `lit` (MongoDB equality).
fn eq_one(v: &Bson, lit: &Bson) -> bool {
    keyenc::equal(v, lit)
}

fn eq_values(vs: &Values<'_>, lit: &Bson) -> bool {
    if is_nullish(lit) {
        return vs.missing || vs.vals.iter().any(|v| is_nullish(v)) || vs.vals.is_empty();
    }
    vs.vals.iter().any(|v| eq_one(v, lit))
}

/// Comparison under type bracketing: only values of `lit`'s class.
fn cmp_values(vs: &Values<'_>, lit: &Bson, ok: impl Fn(Ordering) -> bool) -> bool {
    let lc = cmp_class(lit);
    if lc == class::MIN_KEY || lc == class::MAX_KEY {
        return vs.vals.iter().any(|v| ok(compare(v, lit)));
    }
    if lc == class::NULL {
        // $gte/$lte null behave as equality with null.
        return ok(Ordering::Equal) && eq_values(vs, lit);
    }
    vs.vals.iter().any(|v| {
        cmp_class(v) == lc && {
            if let (Bson::Double(f), _) | (_, Bson::Double(f)) = (*v, lit)
                && f.is_nan()
            {
                // NaN only equals NaN.
                return ok(compare(v, lit)) && compare(v, lit) == Ordering::Equal;
            }
            ok(compare(v, lit))
        }
    })
}

fn regex_one(v: &Bson, re: &Re) -> bool {
    match v {
        Bson::String(s) | Bson::Symbol(s) => re.re.is_match(s),
        Bson::RegularExpression(r) => r.pattern == re.pattern && r.options == re.options,
        _ => false,
    }
}

fn in_values(vs: &Values<'_>, items: &[InItem]) -> bool {
    items.iter().any(|it| match it {
        InItem::Value(lit) => eq_values(vs, lit),
        InItem::Regex(re) => vs.vals.iter().any(|v| regex_one(v, re)),
    })
}

fn type_matches(v: &Bson, sel: &[TypeSel]) -> bool {
    let c = bson_type_code(v);
    sel.iter().any(|s| match s {
        TypeSel::Number => matches!(c, 1 | 16 | 18 | 19),
        TypeSel::Code(n) => *n == c,
    })
}

impl FieldOp {
    /// Whether the values at a path satisfy this condition.
    #[must_use]
    #[allow(clippy::only_used_in_recursion)]
    pub fn eval(&self, vs: &Values<'_>, cx: &dyn ExprEval, root: &Document) -> bool {
        match self {
            Self::Eq(lit) => eq_values(vs, lit),
            Self::Ne(lit) => !eq_values(vs, lit),
            Self::Gt(l) => cmp_values(vs, l, |o| o == Ordering::Greater),
            Self::Gte(l) => cmp_values(vs, l, |o| o != Ordering::Less),
            Self::Lt(l) => cmp_values(vs, l, |o| o == Ordering::Less),
            Self::Lte(l) => cmp_values(vs, l, |o| o != Ordering::Greater),
            Self::In(items) => in_values(vs, items),
            Self::Nin(items) => !in_values(vs, items),
            Self::Exists(want) => vs.leaves.is_empty() != *want,
            Self::Type(sel) => vs.vals.iter().any(|v| type_matches(v, sel)),
            Self::Size(n) => vs
                .leaves
                .iter()
                .any(|v| matches!(v, Bson::Array(a) if i64::try_from(a.len()).ok() == Some(*n))),
            Self::All(inner) => !inner.is_empty() && inner.iter().all(|op| op.eval(vs, cx, root)),
            Self::ElemMatch(em) => vs.leaves.iter().any(|v| {
                let Bson::Array(a) = v else { return false };
                a.iter().any(|e| match em.as_ref() {
                    ElemMatch::Doc(p) => match e {
                        Bson::Document(d) => p.matches_with(d, cx),
                        _ => false,
                    },
                    ElemMatch::Ops(ops) => {
                        let one = Values {
                            vals: vec![e],
                            leaves: vec![e],
                            missing: false,
                            positional: false,
                        };
                        // An array element that is itself an array is
                        // compared whole.
                        ops.iter().all(|op| op.eval(&one, cx, root))
                    }
                })
            }),
            Self::Regex(re) => vs.vals.iter().any(|v| regex_one(v, re)),
            Self::Not(inner) => !inner.iter().all(|op| op.eval(vs, cx, root)),
            Self::Mod(d, r) => vs.vals.iter().any(|v| {
                #[allow(clippy::cast_possible_truncation)]
                let n = match v {
                    Bson::Int32(i) => Some(i64::from(*i)),
                    Bson::Int64(i) => Some(*i),
                    Bson::Double(f) if f.is_finite() => Some(f.trunc() as i64),
                    Bson::Decimal128(_) => as_i64(v),
                    _ => None,
                };
                n.is_some_and(|n| n.wrapping_rem(*d) == *r)
            }),
            Self::Geo(g) => g.eval(&vs.leaves),
            Self::Bits(b) => vs.vals.iter().any(|v| b.matches(v)),
        }
    }
}

/// Evaluates `$expr` against a document (the aggregation evaluator).
pub trait ExprEval {
    /// The value of `expr` for `doc`, as truthiness.
    ///
    /// # Errors
    /// The evaluator's.
    fn truthy(&self, expr: &Bson, doc: &Document) -> Result<bool, MongoError>;
}

/// An evaluator without `$expr` support: every `$expr` is false.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoExpr;

impl ExprEval for NoExpr {
    fn truthy(&self, _: &Bson, _: &Document) -> Result<bool, MongoError> {
        Ok(false)
    }
}

impl Pred {
    /// Whether `doc` matches, with `$expr` through `cx`.
    #[must_use]
    pub fn matches_with(&self, doc: &Document, cx: &dyn ExprEval) -> bool {
        match self {
            Self::True => true,
            Self::And(k) => k.iter().all(|p| p.matches_with(doc, cx)),
            Self::Or(k) => k.iter().any(|p| p.matches_with(doc, cx)),
            Self::Nor(k) => !k.iter().any(|p| p.matches_with(doc, cx)),
            Self::Field { path, op } => op.eval(&values_at(doc, path), cx, doc),
            Self::Expr(e) => cx.truthy(e, doc).unwrap_or(false),
            Self::JsonSchema(s) => s.matches_doc(doc),
            Self::Where(w) => w.matches(doc),
        }
    }

    /// Whether `doc` matches (no `$expr`).
    #[must_use]
    pub fn matches(&self, doc: &Document) -> bool {
        self.matches_with(doc, &NoExpr)
    }

    /// The `_id` values the filter pins: `{_id: v}` or `{_id: {$in: [...]}}`
    /// at the top level (directly or inside `$and`). `None` when it
    /// doesn't pin `_id`.
    #[must_use]
    pub fn id_values(&self) -> Option<Vec<Bson>> {
        match self {
            Self::Field { path, op } if path == "_id" => match op {
                FieldOp::Eq(v) if !matches!(v, Bson::Document(_) | Bson::Array(_)) => {
                    Some(vec![v.clone()])
                }
                FieldOp::In(items) => items
                    .iter()
                    .map(|i| match i {
                        InItem::Value(v) if !matches!(v, Bson::Document(_) | Bson::Array(_)) => {
                            Some(v.clone())
                        }
                        _ => None,
                    })
                    .collect(),
                _ => None,
            },
            Self::And(k) => k.iter().find_map(Self::id_values),
            _ => None,
        }
    }

    /// The equality fields an upsert inserts: `{a: 1, "b.c": 2}` and
    /// those inside `$and`.
    pub fn equality_fields(&self, out: &mut Vec<(String, Bson)>) {
        match self {
            Self::Field {
                path,
                op: FieldOp::Eq(v),
            } => out.push((path.clone(), v.clone())),
            Self::Field {
                path,
                op: FieldOp::In(items),
            } if items.len() == 1 => {
                if let InItem::Value(v) = &items[0] {
                    out.push((path.clone(), v.clone()));
                }
            }
            Self::And(k) => {
                for p in k {
                    p.equality_fields(out);
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests;
