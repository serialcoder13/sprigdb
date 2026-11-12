//! MongoDB's `$jsonSchema` (draft 4 with BSON types): parsing, matching,
//! and the detailed `errInfo` MongoDB 5.0+ reports for a document that
//! fails (decision S90).
//!
//! A schema is parsed once into [`Schema`], a list of keywords in the
//! order MongoDB evaluates and reports them: the string and number
//! keywords, the array keywords, the object keywords, the logical
//! keywords and `enum`, and the type last. Each keyword applies only to
//! values of its kind (`minimum` to numbers, `required` to objects ...),
//! as in JSON Schema; the type keyword restricts the kind.

use bson::{Bson, Document, doc};
use sprig_proto_mongo::errors::{MongoError, code};

use crate::filter::{Re, bson_type_code, type_code};
use crate::keyenc::{compare, equal};

/// A parsed `$jsonSchema` (sub)schema.
#[derive(Debug, Clone, Default)]
pub struct Schema {
    kws: Vec<Kw>,
    /// `title`, reported with the subschema's failures.
    title: Option<String>,
    /// `description`, reported with the subschema's failures.
    description: Option<String>,
}

#[derive(Debug, Clone)]
enum Kw {
    Pattern(Re),
    MaxLength(Bson, u64),
    MinLength(Bson, u64),
    MultipleOf(Bson),
    Maximum(Bson, Option<Bson>),
    Minimum(Bson, Option<Bson>),
    MinItems(Bson, u64),
    MaxItems(Bson, u64),
    UniqueItems,
    Items(Box<Schema>),
    ItemsTuple(Vec<Schema>),
    AdditionalItemsFalse(usize),
    AdditionalItems(usize, Box<Schema>),
    Properties(Vec<(String, Schema)>),
    AdditionalPropertiesFalse(Vec<String>, Vec<Re>),
    AdditionalProperties(Vec<String>, Vec<Re>, Box<Schema>),
    PatternProperties(Vec<(Re, Schema)>),
    Required(Bson, Vec<String>),
    MinProperties(Bson, u64),
    MaxProperties(Bson, u64),
    Dependencies(Vec<(String, Dep)>),
    AllOf(Vec<Schema>),
    AnyOf(Vec<Schema>),
    OneOf(Vec<Schema>),
    Not(Box<Schema>),
    Enum(Bson, Vec<Bson>),
    Type {
        name: &'static str,
        spec: Bson,
        json: bool,
        set: Vec<i32>,
    },
}

#[derive(Debug, Clone)]
enum Dep {
    Props(Vec<String>),
    Schema(Schema),
}

const NUMBER: i32 = 1000;

fn failed_to_parse(msg: impl Into<String>) -> MongoError {
    MongoError::new(code::FAILED_TO_PARSE, msg)
}

fn type_mismatch(msg: impl Into<String>) -> MongoError {
    MongoError::new(code::TYPE_MISMATCH, msg)
}

fn is_number(v: &Bson) -> bool {
    matches!(
        v,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_)
    )
}

/// A number's value as a double.
#[must_use]
pub fn as_number(v: &Bson) -> Option<f64> {
    as_f64(v)
}

fn as_f64(v: &Bson) -> Option<f64> {
    match v {
        Bson::Int32(i) => Some(f64::from(*i)),
        #[allow(clippy::cast_precision_loss)]
        Bson::Int64(i) => Some(*i as f64),
        Bson::Double(f) => Some(*f),
        Bson::Decimal128(d) => d.to_string().parse().ok(),
        _ => None,
    }
}

/// The `$type` alias of a value's BSON type (`consideredType`).
#[must_use]
pub fn type_alias(v: &Bson) -> &'static str {
    match bson_type_code(v) {
        1 => "double",
        2 => "string",
        3 => "object",
        4 => "array",
        5 => "binData",
        6 => "undefined",
        7 => "objectId",
        8 => "bool",
        9 => "date",
        10 => "null",
        11 => "regex",
        12 => "dbPointer",
        13 => "javascript",
        14 => "symbol",
        15 => "javascriptWithScope",
        16 => "int",
        17 => "timestamp",
        18 => "long",
        19 => "decimal",
        -1 => "minKey",
        _ => "maxKey",
    }
}

/// A non-negative whole number argument (`minLength`, `maxItems` ...).
fn count_arg(kw: &str, v: &Bson) -> Result<u64, MongoError> {
    if !is_number(v) {
        return Err(failed_to_parse(format!(
            "Expected a number in: {kw}: {}",
            num_text(v)
        )));
    }
    let f = as_f64(v).unwrap_or(f64::NAN);
    if f.fract() != 0.0 || !f.is_finite() {
        return Err(failed_to_parse(format!(
            "Expected an integer: {kw}: {}",
            num_text(v)
        )));
    }
    if f < 0.0 {
        return Err(failed_to_parse(format!(
            "Expected a non-negative number in: {kw}: {}",
            num_text(v)
        )));
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    Ok(f as u64)
}

fn num_text(v: &Bson) -> String {
    match v {
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        Bson::Double(f) => f.to_string(),
        other => other.to_string(),
    }
}

fn str_list(kw: &str, v: &Bson, what: &str) -> Result<Vec<String>, MongoError> {
    let Bson::Array(a) = v else {
        return Err(type_mismatch(format!(
            "$jsonSchema keyword '{kw}' must be an array"
        )));
    };
    let mut out: Vec<String> = Vec::new();
    for x in a {
        let Bson::String(s) = x else {
            return Err(type_mismatch(format!(
                "$jsonSchema keyword '{kw}' must contain only strings"
            )));
        };
        if out.contains(s) {
            return Err(failed_to_parse(format!(
                "{what} array cannot contain duplicate values"
            )));
        }
        out.push(s.clone());
    }
    if out.is_empty() {
        return Err(failed_to_parse(format!("{what} cannot be an empty array")));
    }
    Ok(out)
}

fn sub(kw: &str, v: &Bson) -> Result<Schema, MongoError> {
    let Bson::Document(d) = v else {
        return Err(type_mismatch(format!(
            "$jsonSchema keyword '{kw}' must be an object"
        )));
    };
    Schema::parse(d)
}

fn sub_list(kw: &str, v: &Bson) -> Result<Vec<Schema>, MongoError> {
    let Bson::Array(a) = v else {
        return Err(type_mismatch(format!(
            "$jsonSchema keyword '{kw}' must be an array"
        )));
    };
    if a.is_empty() {
        return Err(MongoError::new(
            code::BAD_VALUE,
            format!("$jsonSchema keyword '{kw}' must be a non-empty array"),
        ));
    }
    a.iter()
        .map(|x| match x {
            Bson::Document(d) => Schema::parse(d),
            other => Err(type_mismatch(format!(
                "$jsonSchema keyword '{kw}' must be an array of objects, but found an element of type {}",
                type_alias(other)
            ))),
        })
        .collect()
}

fn type_set(kw: &'static str, v: &Bson) -> Result<Vec<i32>, MongoError> {
    let names: Vec<&Bson> = match v {
        Bson::Array(a) => a.iter().collect(),
        other => vec![other],
    };
    if names.is_empty() {
        return Err(failed_to_parse(format!(
            "$jsonSchema keyword '{kw}' must name at least one type"
        )));
    }
    let mut set = Vec::new();
    for n in names {
        let Bson::String(s) = n else {
            return Err(type_mismatch(format!(
                "$jsonSchema keyword '{kw}' must be either a string or an array of strings"
            )));
        };
        if s == "integer" {
            return Err(failed_to_parse(
                "$jsonSchema type 'integer' is not currently supported.",
            ));
        }
        let c = if kw == "type" {
            match s.as_str() {
                "object" => 3,
                "array" => 4,
                "string" => 2,
                "null" => 10,
                "boolean" => 8,
                "number" => NUMBER,
                _ => {
                    return Err(MongoError::new(
                        code::BAD_VALUE,
                        format!("Unknown type name alias: {s}"),
                    ));
                }
            }
        } else if s == "number" {
            NUMBER
        } else {
            type_code(s).ok_or_else(|| {
                MongoError::new(code::BAD_VALUE, format!("Unknown type name alias: {s}"))
            })?
        };
        if set.contains(&c) {
            return Err(failed_to_parse(format!(
                "$jsonSchema keyword '{kw}' has duplicate value: {s}"
            )));
        }
        set.push(c);
    }
    Ok(set)
}

fn type_ok(set: &[i32], v: &Bson) -> bool {
    let c = bson_type_code(v);
    set.iter()
        .any(|t| *t == c || (*t == NUMBER && matches!(c, 1 | 16 | 18 | 19)))
}

/// MongoDB's comparison with documents' fields in any order (`enum`,
/// `uniqueItems`).
fn eq_unordered(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        (Bson::Document(x), Bson::Document(y)) => {
            x.len() == y.len()
                && x
                    .iter()
                    .all(|(k, v)| y.get(k).is_some_and(|w| eq_unordered(v, w)))
        }
        (Bson::Array(x), Bson::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| eq_unordered(p, q))
        }
        _ => equal(a, b),
    }
}

impl Schema {
    /// Parses a `$jsonSchema` (sub)schema.
    ///
    /// # Errors
    /// 9 `FailedToParse`, 14 `TypeMismatch`, 2 `BadValue` and 51091 as
    /// MongoDB 8.0 reports them; 39662 for the encryption keywords.
    pub fn parse(d: &Document) -> Result<Self, MongoError> {
        Self::parse_at(d, false)
    }

    /// Parses the top-level schema, which applies to a document: its
    /// string, number and array keywords never apply (MongoDB drops them,
    /// without compiling a `pattern`).
    ///
    /// # Errors
    /// As [`Self::parse`].
    pub fn parse_root(d: &Document) -> Result<Self, MongoError> {
        Self::parse_at(d, true)
    }

    fn parse_at(d: &Document, root: bool) -> Result<Self, MongoError> {
        let mut s = Self::default();
        for k in d.keys() {
            match k.as_str() {
                "bsonType" | "type" | "pattern" | "maxLength" | "minLength" | "multipleOf"
                | "maximum" | "minimum" | "exclusiveMaximum" | "exclusiveMinimum"
                | "minItems" | "maxItems" | "uniqueItems" | "items" | "additionalItems"
                | "properties" | "additionalProperties" | "patternProperties" | "required"
                | "minProperties" | "maxProperties" | "dependencies" | "allOf" | "anyOf"
                | "oneOf" | "not" | "enum" | "title" | "description" => {}
                "$ref" | "$schema" | "default" | "definitions" | "format" | "id" => {
                    return Err(failed_to_parse(format!(
                        "$jsonSchema keyword '{k}' is not currently supported"
                    )));
                }
                "encrypt" | "encryptMetadata" => {
                    return Err(MongoError::new(
                        code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                        format!("$jsonSchema keyword '{k}' (Queryable Encryption) is not supported in this release"),
                    ));
                }
                other => {
                    return Err(failed_to_parse(format!(
                        "Unknown $jsonSchema keyword: {other}"
                    )));
                }
            }
        }
        for kw in ["title", "description"] {
            match d.get(kw) {
                None => {}
                Some(Bson::String(t)) => {
                    if kw == "title" {
                        s.title = Some(t.clone());
                    } else {
                        s.description = Some(t.clone());
                    }
                }
                Some(_) => {
                    return Err(type_mismatch(format!(
                        "$jsonSchema keyword '{kw}' must be of type string"
                    )));
                }
            }
        }
        if d.contains_key("type") && d.contains_key("bsonType") {
            return Err(failed_to_parse(
                "Cannot specify both $jsonSchema keywords 'type' and 'bsonType'",
            ));
        }
        // A type that excludes a kind of value makes that kind's keywords
        // always true (the type keyword fails those values anyway).
        let set = match (d.get("type"), d.get("bsonType")) {
            (Some(t), _) => Some(type_set("type", t)?),
            (_, Some(t)) => Some(type_set("bsonType", t)?),
            _ => None,
        };
        let allows = |codes: &[i32]| -> bool {
            if root {
                return codes.contains(&3);
            }
            set.as_ref()
                .is_none_or(|set| set.iter().any(|c| codes.contains(c)))
        };
        let strings = allows(&[2]);
        let numbers = allows(&[1, 16, 18, 19, NUMBER]);
        let arrays = allows(&[4]);
        let objects = allows(&[3]);

        // Strings.
        if let Some(v) = d.get("pattern") {
            let Bson::String(p) = v else {
                return Err(type_mismatch(
                    "$jsonSchema keyword 'pattern' must be a string",
                ));
            };
            if strings {
                s.kws.push(Kw::Pattern(Re::new(p, "")?));
            }
        }
        if let Some(v) = d.get("maxLength") {
            s.kws.push(Kw::MaxLength(v.clone(), count_arg("maxLength", v)?));
        }
        if let Some(v) = d.get("minLength") {
            s.kws.push(Kw::MinLength(v.clone(), count_arg("minLength", v)?));
        }
        // Numbers.
        let num = |kw: &str| -> Result<Option<Bson>, MongoError> {
            match d.get(kw) {
                None => Ok(None),
                Some(v) if is_number(v) => Ok(Some(v.clone())),
                Some(_) => Err(type_mismatch(format!(
                    "$jsonSchema keyword '{kw}' must be a number"
                ))),
            }
        };
        let excl = |kw: &str, base: &str| -> Result<Option<Bson>, MongoError> {
            match d.get(kw) {
                None => Ok(None),
                Some(Bson::Boolean(b)) => {
                    if !d.contains_key(base) {
                        return Err(failed_to_parse(format!(
                            "$jsonSchema keyword '{base}' must be a present if {kw} is present"
                        )));
                    }
                    Ok(Some(Bson::Boolean(*b)))
                }
                Some(_) => Err(type_mismatch(format!(
                    "$jsonSchema keyword '{kw}' must be a boolean"
                ))),
            }
        };
        if let Some(m) = num("multipleOf")? {
            if as_f64(&m).is_none_or(|f| f <= 0.0 || f.is_nan()) {
                return Err(failed_to_parse(
                    "$jsonSchema keyword 'multipleOf' must have a positive value",
                ));
            }
            s.kws.push(Kw::MultipleOf(m));
        }
        let ex_max = excl("exclusiveMaximum", "maximum")?;
        if let Some(m) = num("maximum")? {
            s.kws.push(Kw::Maximum(m, ex_max));
        }
        let ex_min = excl("exclusiveMinimum", "minimum")?;
        if let Some(m) = num("minimum")? {
            s.kws.push(Kw::Minimum(m, ex_min));
        }
        // Arrays.
        if let Some(v) = d.get("minItems") {
            s.kws.push(Kw::MinItems(v.clone(), count_arg("minItems", v)?));
        }
        if let Some(v) = d.get("maxItems") {
            s.kws.push(Kw::MaxItems(v.clone(), count_arg("maxItems", v)?));
        }
        match d.get("uniqueItems") {
            None | Some(Bson::Boolean(false)) => {}
            Some(Bson::Boolean(true)) => s.kws.push(Kw::UniqueItems),
            Some(_) => {
                return Err(type_mismatch(
                    "$jsonSchema keyword 'uniqueItems' must be a boolean",
                ));
            }
        }
        let mut tuple = None;
        match d.get("items") {
            None => {}
            Some(Bson::Document(i)) => s.kws.push(Kw::Items(Box::new(Self::parse(i)?))),
            Some(Bson::Array(a)) => {
                let list = a
                    .iter()
                    .map(|x| match x {
                        Bson::Document(i) => Self::parse(i),
                        other => Err(type_mismatch(format!(
                            "$jsonSchema keyword 'items' requires that each element of the array is an object, but found a {}",
                            type_alias(other)
                        ))),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                tuple = Some(list.len());
                s.kws.push(Kw::ItemsTuple(list));
            }
            Some(other) => {
                return Err(type_mismatch(format!(
                    "$jsonSchema keyword 'items' must be an array or an object, not {}",
                    type_alias(other)
                )));
            }
        }
        match (d.get("additionalItems"), tuple) {
            // Without a tuple of items, `additionalItems` has no effect.
            (None, _)
            | (Some(Bson::Boolean(_) | Bson::Document(_)), None)
            | (Some(Bson::Boolean(true)), Some(_)) => {}
            (Some(Bson::Boolean(false)), Some(n)) => s.kws.push(Kw::AdditionalItemsFalse(n)),
            (Some(Bson::Document(a)), Some(n)) => {
                s.kws
                    .push(Kw::AdditionalItems(n, Box::new(Self::parse(a)?)));
            }
            (Some(other), _) => {
                return Err(type_mismatch(format!(
                    "$jsonSchema keyword 'additionalItems' must be either an object or a boolean, but got a(n) {}",
                    type_alias(other)
                )));
            }
        }
        // Objects.
        let mut names = Vec::new();
        if let Some(v) = d.get("properties") {
            let Bson::Document(p) = v else {
                return Err(type_mismatch(
                    "$jsonSchema keyword 'properties' must be an object",
                ));
            };
            let mut list = Vec::new();
            for (k, x) in p {
                let Bson::Document(x) = x else {
                    return Err(type_mismatch(format!(
                        "Nested schema for $jsonSchema property '{k}' must be an object"
                    )));
                };
                names.push(k.clone());
                list.push((k.clone(), Self::parse(x)?));
            }
            s.kws.push(Kw::Properties(list));
        }
        let mut patterns = Vec::new();
        if let Some(v) = d.get("patternProperties") {
            let Bson::Document(p) = v else {
                return Err(type_mismatch(
                    "$jsonSchema keyword 'patternProperties' must be an object",
                ));
            };
            for (k, x) in p {
                let Bson::Document(x) = x else {
                    return Err(type_mismatch(format!(
                        "$jsonSchema keyword 'patternProperties' has property '{k}' which is not an object"
                    )));
                };
                patterns.push((Re::new(k, "")?, Self::parse(x)?));
            }
        }
        let res: Vec<Re> = patterns.iter().map(|(r, _)| r.clone()).collect();
        match d.get("additionalProperties") {
            None | Some(Bson::Boolean(true)) => {}
            Some(Bson::Boolean(false)) => {
                s.kws
                    .push(Kw::AdditionalPropertiesFalse(names.clone(), res.clone()));
            }
            Some(Bson::Document(a)) => s.kws.push(Kw::AdditionalProperties(
                names.clone(),
                res.clone(),
                Box::new(Self::parse(a)?),
            )),
            Some(other) => {
                return Err(type_mismatch(format!(
                    "$jsonSchema keyword 'additionalProperties' must be either an object or a boolean, but got a(n) {}",
                    type_alias(other)
                )));
            }
        }
        if !patterns.is_empty() {
            s.kws.push(Kw::PatternProperties(patterns));
        }
        if let Some(v) = d.get("required") {
            s.kws.push(Kw::Required(
                v.clone(),
                str_list("required", v, "$jsonSchema keyword 'required'")?,
            ));
        }
        if let Some(v) = d.get("minProperties") {
            s.kws
                .push(Kw::MinProperties(v.clone(), count_arg("minProperties", v)?));
        }
        if let Some(v) = d.get("maxProperties") {
            s.kws
                .push(Kw::MaxProperties(v.clone(), count_arg("maxProperties", v)?));
        }
        if let Some(v) = d.get("dependencies") {
            let Bson::Document(p) = v else {
                return Err(type_mismatch(
                    "$jsonSchema keyword 'dependencies' must be an object",
                ));
            };
            let mut list = Vec::new();
            for (k, x) in p {
                let dep = match x {
                    Bson::Document(sd) => Dep::Schema(Self::parse(sd)?),
                    Bson::Array(_) => Dep::Props(str_list(
                        "dependencies",
                        x,
                        &format!("property '{k}' in $jsonSchema keyword 'dependencies'"),
                    )?),
                    _ => {
                        return Err(type_mismatch(format!(
                            "property '{k}' in $jsonSchema keyword 'dependencies' must be either an object or an array"
                        )));
                    }
                };
                list.push((k.clone(), dep));
            }
            s.kws.push(Kw::Dependencies(list));
        }
        s.kws.retain(|k| match k {
            Kw::Pattern(_) | Kw::MaxLength(..) | Kw::MinLength(..) => strings,
            Kw::MultipleOf(_) | Kw::Maximum(..) | Kw::Minimum(..) => numbers,
            Kw::MinItems(..)
            | Kw::MaxItems(..)
            | Kw::UniqueItems
            | Kw::Items(_)
            | Kw::ItemsTuple(_)
            | Kw::AdditionalItemsFalse(_)
            | Kw::AdditionalItems(..) => arrays,
            Kw::Properties(_)
            | Kw::AdditionalPropertiesFalse(..)
            | Kw::AdditionalProperties(..)
            | Kw::PatternProperties(_)
            | Kw::Required(..)
            | Kw::MinProperties(..)
            | Kw::MaxProperties(..)
            | Kw::Dependencies(_) => objects,
            _ => true,
        });
        // Logical keywords, then `enum`.
        if let Some(v) = d.get("allOf") {
            s.kws.push(Kw::AllOf(sub_list("allOf", v)?));
        }
        if let Some(v) = d.get("anyOf") {
            s.kws.push(Kw::AnyOf(sub_list("anyOf", v)?));
        }
        if let Some(v) = d.get("oneOf") {
            s.kws.push(Kw::OneOf(sub_list("oneOf", v)?));
        }
        if let Some(v) = d.get("not") {
            s.kws.push(Kw::Not(Box::new(sub("not", v)?)));
        }
        if let Some(v) = d.get("enum") {
            let Bson::Array(a) = v else {
                return Err(type_mismatch(
                    "$jsonSchema keyword 'enum' must be an array",
                ));
            };
            if a.is_empty() {
                return Err(failed_to_parse(
                    "$jsonSchema keyword 'enum' cannot be an empty array",
                ));
            }
            for (i, x) in a.iter().enumerate() {
                if a[..i].iter().any(|y| eq_unordered(x, y)) {
                    return Err(failed_to_parse(
                        "$jsonSchema keyword 'enum' array cannot contain duplicate values",
                    ));
                }
            }
            s.kws.push(Kw::Enum(v.clone(), a.clone()));
        }
        // The type last.
        for (name, json) in [("type", true), ("bsonType", false)] {
            if let Some(v) = d.get(name) {
                s.kws.push(Kw::Type {
                    name,
                    spec: v.clone(),
                    json,
                    set: type_set(name, v)?,
                });
            }
        }
        Ok(s)
    }

    /// Whether `v` satisfies the schema.
    #[must_use]
    pub fn matches(&self, v: &Bson) -> bool {
        self.fails(v, false).is_empty()
    }

    /// Whether the document satisfies the schema at the top level.
    #[must_use]
    pub fn matches_doc(&self, d: &Document) -> bool {
        self.matches(&Bson::Document(d.clone()))
    }

    /// The rules `v` doesn't satisfy, as MongoDB reports them
    /// (`schemaRulesNotSatisfied`); empty when it satisfies all.
    #[must_use]
    pub fn explain(&self, v: &Bson) -> Vec<Document> {
        self.fails(v, true)
    }

    /// The root's `title` and `description`.
    #[must_use]
    pub fn annotations(&self) -> (Option<&str>, Option<&str>) {
        (self.title.as_deref(), self.description.as_deref())
    }

    fn annotate(&self, d: &mut Document) {
        if let Some(t) = &self.title {
            d.insert("title", t.clone());
        }
        if let Some(t) = &self.description {
            d.insert("description", t.clone());
        }
    }

    /// The failing keywords; with `detail` false, stops at the first one.
    #[allow(clippy::too_many_lines)]
    fn fails(&self, v: &Bson, detail: bool) -> Vec<Document> {
        let mut out = Vec::new();
        macro_rules! fail {
            ($d:expr) => {{
                if !detail {
                    return vec![Document::new()];
                }
                out.push($d);
            }};
        }
        for kw in &self.kws {
            match kw {
                Kw::Pattern(re) => {
                    if let Bson::String(s) = v
                        && !re.re.is_match(s)
                    {
                        fail!(doc! {"operatorName": "pattern", "specifiedAs": {"pattern": re.pattern.clone()}, "reason": "regular expression did not match", "consideredValue": v.clone()});
                    }
                }
                Kw::MaxLength(spec, n) | Kw::MinLength(spec, n) => {
                    if let Bson::String(s) = v {
                        let len = s.chars().count() as u64;
                        let (name, bad) = if matches!(kw, Kw::MaxLength(..)) {
                            ("maxLength", len > *n)
                        } else {
                            ("minLength", len < *n)
                        };
                        if bad {
                            fail!(doc! {"operatorName": name, "specifiedAs": {name: spec.clone()}, "reason": "specified string length was not satisfied", "consideredValue": v.clone()});
                        }
                    }
                }
                Kw::MultipleOf(m) => {
                    if is_number(v) && !multiple_of(v, m) {
                        fail!(doc! {"operatorName": "multipleOf", "specifiedAs": {"multipleOf": m.clone()}, "reason": "considered value is not a multiple of the specified value", "consideredValue": v.clone()});
                    }
                }
                Kw::Maximum(m, ex) | Kw::Minimum(m, ex) => {
                    if is_number(v) {
                        let max = matches!(kw, Kw::Maximum(..));
                        let nan = as_f64(v).is_some_and(f64::is_nan);
                        let strict = matches!(ex, Some(Bson::Boolean(true)));
                        let o = num_cmp(v, m);
                        let ok = match (max, strict) {
                            (true, false) => o != std::cmp::Ordering::Greater,
                            (true, true) => o == std::cmp::Ordering::Less,
                            (false, false) => o != std::cmp::Ordering::Less,
                            (false, true) => o == std::cmp::Ordering::Greater,
                        };
                        if !ok || nan {
                            let name = if max { "maximum" } else { "minimum" };
                            let mut spec = doc! {name: m.clone()};
                            if let Some(e) = ex {
                                spec.insert(
                                    if max {
                                        "exclusiveMaximum"
                                    } else {
                                        "exclusiveMinimum"
                                    },
                                    e.clone(),
                                );
                            }
                            fail!(doc! {"operatorName": name, "specifiedAs": spec, "reason": "comparison failed", "consideredValue": v.clone()});
                        }
                    }
                }
                Kw::MinItems(spec, n) | Kw::MaxItems(spec, n) => {
                    if let Bson::Array(a) = v {
                        let len = a.len() as u64;
                        let (name, bad) = if matches!(kw, Kw::MinItems(..)) {
                            ("minItems", len < *n)
                        } else {
                            ("maxItems", len > *n)
                        };
                        if bad {
                            fail!(doc! {"operatorName": name, "specifiedAs": {name: spec.clone()}, "reason": "array did not match specified length", "consideredValue": v.clone(), "numberOfItems": i32::try_from(len).unwrap_or(i32::MAX)});
                        }
                    }
                }
                Kw::UniqueItems => {
                    if let Bson::Array(a) = v
                        && let Some(dup) = (1..a.len())
                            .find(|j| a[..*j].iter().any(|x| eq_unordered(x, &a[*j])))
                    {
                        fail!(doc! {"operatorName": "uniqueItems", "specifiedAs": {"uniqueItems": true}, "reason": "found a duplicate item", "consideredValue": v.clone(), "duplicatedValue": a[dup].clone()});
                    }
                }
                Kw::Items(s) => {
                    if let Bson::Array(a) = v
                        && let Some((i, e)) = a.iter().enumerate().find(|(_, e)| !s.matches(e))
                    {
                        let mut d = doc! {"operatorName": "items"};
                        s.annotate(&mut d);
                        d.insert("reason", "At least one item did not match the sub-schema");
                        d.insert("itemIndex", i32::try_from(i).unwrap_or(i32::MAX));
                        d.insert("details", s.fails(e, detail));
                        fail!(d);
                    }
                }
                Kw::ItemsTuple(list) => {
                    if let Bson::Array(a) = v {
                        let mut items = Vec::new();
                        for (i, (s, e)) in list.iter().zip(a).enumerate() {
                            let f = s.fails(e, detail);
                            if !f.is_empty() {
                                if !detail {
                                    return f;
                                }
                                let mut d =
                                    doc! {"itemIndex": i64::try_from(i).unwrap_or(i64::MAX)};
                                s.annotate(&mut d);
                                d.insert("details", f);
                                items.push(d);
                            }
                        }
                        if !items.is_empty() {
                            fail!(doc! {"operatorName": "items", "details": items});
                        }
                    }
                }
                Kw::AdditionalItemsFalse(n) => {
                    if let Bson::Array(a) = v
                        && a.len() > *n
                    {
                        fail!(doc! {"operatorName": "additionalItems", "specifiedAs": {"additionalItems": false}, "reason": "found additional items", "additionalItems": a[*n..].to_vec()});
                    }
                }
                Kw::AdditionalItems(n, s) => {
                    if let Bson::Array(a) = v
                        && let Some((i, e)) = a
                            .iter()
                            .enumerate()
                            .skip(*n)
                            .find(|(_, e)| !s.matches(e))
                    {
                        let mut d = doc! {"operatorName": "additionalItems"};
                        s.annotate(&mut d);
                        d.insert(
                            "reason",
                            "At least one additional item did not match the sub-schema",
                        );
                        d.insert("itemIndex", i32::try_from(i).unwrap_or(i32::MAX));
                        d.insert("details", s.fails(e, detail));
                        fail!(d);
                    }
                }
                Kw::Properties(list) => {
                    if let Bson::Document(o) = v {
                        let mut props = Vec::new();
                        for (name, s) in list {
                            if let Some(x) = o.get(name) {
                                let f = s.fails(x, detail);
                                if !f.is_empty() {
                                    if !detail {
                                        return f;
                                    }
                                    let mut d = doc! {"propertyName": name.clone()};
                                    s.annotate(&mut d);
                                    d.insert("details", f);
                                    props.push(d);
                                }
                            }
                        }
                        if !props.is_empty() {
                            fail!(doc! {"operatorName": "properties", "propertiesNotSatisfied": props});
                        }
                    }
                }
                Kw::AdditionalPropertiesFalse(names, res) => {
                    if let Bson::Document(o) = v {
                        let extra: Vec<String> = o
                            .keys()
                            .filter(|k| !is_declared(k, names, res))
                            .cloned()
                            .collect();
                        if !extra.is_empty() {
                            fail!(doc! {"operatorName": "additionalProperties", "specifiedAs": {"additionalProperties": false}, "additionalProperties": extra});
                        }
                    }
                }
                Kw::AdditionalProperties(names, res, s) => {
                    if let Bson::Document(o) = v
                        && let Some((k, x)) = o
                            .iter()
                            .find(|(k, x)| !is_declared(k, names, res) && !s.matches(x))
                    {
                        let mut d = doc! {"operatorName": "additionalProperties"};
                        s.annotate(&mut d);
                        d.insert(
                            "reason",
                            "at least one additional property did not match the subschema",
                        );
                        d.insert("failingProperty", k.clone());
                        d.insert("details", s.fails(x, detail));
                        fail!(d);
                    }
                }
                Kw::PatternProperties(list) => {
                    if let Bson::Document(o) = v {
                        let mut props = Vec::new();
                        for (k, x) in o {
                            for (re, s) in list {
                                if !re.re.is_match(k) {
                                    continue;
                                }
                                let f = s.fails(x, detail);
                                if !f.is_empty() {
                                    if !detail {
                                        return f;
                                    }
                                    let mut d = doc! {"propertyName": k.clone(), "regexMatched": re.pattern.clone()};
                                    s.annotate(&mut d);
                                    d.insert("details", f);
                                    props.push(d);
                                }
                            }
                        }
                        if !props.is_empty() {
                            fail!(doc! {"operatorName": "patternProperties", "details": props});
                        }
                    }
                }
                Kw::Required(spec, names) => {
                    if let Bson::Document(o) = v {
                        let missing: Vec<&String> =
                            names.iter().filter(|n| !o.contains_key(n.as_str())).collect();
                        if !missing.is_empty() {
                            fail!(doc! {"operatorName": "required", "specifiedAs": {"required": spec.clone()}, "missingProperties": missing});
                        }
                    }
                }
                Kw::MinProperties(spec, n) | Kw::MaxProperties(spec, n) => {
                    if let Bson::Document(o) = v {
                        let len = o.len() as u64;
                        let (name, bad) = if matches!(kw, Kw::MinProperties(..)) {
                            ("minProperties", len < *n)
                        } else {
                            ("maxProperties", len > *n)
                        };
                        if bad {
                            fail!(doc! {"operatorName": name, "specifiedAs": {name: spec.clone()}, "reason": "specified number of properties was not satisfied", "numberOfProperties": i32::try_from(len).unwrap_or(i32::MAX)});
                        }
                    }
                }
                Kw::Dependencies(list) => {
                    if let Bson::Document(o) = v {
                        let mut deps = Vec::new();
                        for (k, dep) in list {
                            if !o.contains_key(k) {
                                continue;
                            }
                            match dep {
                                Dep::Props(names) => {
                                    let missing: Vec<&String> = names
                                        .iter()
                                        .filter(|n| !o.contains_key(n.as_str()))
                                        .collect();
                                    if !missing.is_empty() {
                                        if !detail {
                                            return vec![Document::new()];
                                        }
                                        deps.push(doc! {"conditionalProperty": k.clone(), "missingProperties": missing});
                                    }
                                }
                                Dep::Schema(s) => {
                                    let f = s.fails(v, detail);
                                    if !f.is_empty() {
                                        if !detail {
                                            return f;
                                        }
                                        let mut d = doc! {"conditionalProperty": k.clone()};
                                        s.annotate(&mut d);
                                        d.insert("details", f);
                                        deps.push(d);
                                    }
                                }
                            }
                        }
                        if !deps.is_empty() {
                            fail!(doc! {"operatorName": "dependencies", "failingDependencies": deps});
                        }
                    }
                }
                Kw::AllOf(list) | Kw::AnyOf(list) => {
                    let all = matches!(kw, Kw::AllOf(_));
                    let mut failed = Vec::new();
                    let mut any = false;
                    for (i, s) in list.iter().enumerate() {
                        if s.matches(v) {
                            any = true;
                            if !all {
                                break;
                            }
                        } else if detail {
                            let mut d = doc! {"index": i32::try_from(i).unwrap_or(i32::MAX)};
                            s.annotate(&mut d);
                            d.insert("details", s.fails(v, true));
                            failed.push(d);
                        } else if all {
                            return vec![Document::new()];
                        }
                    }
                    let bad = if all { !failed.is_empty() } else { !any };
                    if bad {
                        fail!(doc! {"operatorName": if all {"allOf"} else {"anyOf"}, "schemasNotSatisfied": failed});
                    }
                }
                Kw::OneOf(list) => {
                    let ok: Vec<usize> = list
                        .iter()
                        .enumerate()
                        .filter(|(_, s)| s.matches(v))
                        .map(|(i, _)| i)
                        .collect();
                    if ok.len() > 1 {
                        fail!(doc! {"operatorName": "oneOf", "reason": "more than one subschema matched", "matchingSchemaIndexes": ok.iter().map(|i| i32::try_from(*i).unwrap_or(i32::MAX)).collect::<Vec<_>>()});
                    } else if ok.is_empty() {
                        let failed: Vec<Document> = if detail {
                            list.iter()
                                .enumerate()
                                .map(|(i, s)| {
                                    let mut d =
                                        doc! {"index": i32::try_from(i).unwrap_or(i32::MAX)};
                                    s.annotate(&mut d);
                                    d.insert("details", s.fails(v, true));
                                    d
                                })
                                .collect()
                        } else {
                            Vec::new()
                        };
                        fail!(doc! {"operatorName": "oneOf", "schemasNotSatisfied": failed});
                    }
                }
                Kw::Not(s) => {
                    if s.matches(v) {
                        let mut d = doc! {"operatorName": "not"};
                        s.annotate(&mut d);
                        d.insert("reason", "child expression matched");
                        fail!(d);
                    }
                }
                Kw::Enum(spec, list) => {
                    if !list.iter().any(|x| eq_unordered(x, v)) {
                        fail!(doc! {"operatorName": "enum", "specifiedAs": {"enum": spec.clone()}, "reason": "value was not found in enum", "consideredValue": v.clone()});
                    }
                }
                Kw::Type {
                    name,
                    spec,
                    json,
                    set,
                } => {
                    // JSON Schema's "number" has no BSON type of its own; a
                    // document is an "object" for both keywords.
                    let _ = json;
                    if !type_ok(set, v) {
                        fail!(doc! {"operatorName": *name, "specifiedAs": {*name: spec.clone()}, "reason": "type did not match", "consideredValue": v.clone(), "consideredType": type_alias(v)});
                    }
                }
            }
        }
        out
    }
}

fn is_declared(k: &str, names: &[String], res: &[Re]) -> bool {
    names.iter().any(|n| n == k) || res.iter().any(|r| r.re.is_match(k))
}

fn num_cmp(a: &Bson, b: &Bson) -> std::cmp::Ordering {
    compare(a, b)
}

fn multiple_of(v: &Bson, m: &Bson) -> bool {
    let whole = |x: &Bson| match x {
        Bson::Int32(i) => Some(i64::from(*i)),
        Bson::Int64(i) => Some(*i),
        _ => None,
    };
    if let (Some(a), Some(b)) = (whole(v), whole(m)) {
        return b != 0 && a.wrapping_rem(b) == 0;
    }
    match (as_f64(v), as_f64(m)) {
        (Some(a), Some(b)) if a.is_finite() && b != 0.0 => (a % b) == 0.0,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_and_types() {
        let s = Schema::parse(
            &doc! {"required": ["a"], "properties": {"a": {"bsonType": "int", "minimum": 2}}},
        )
        .unwrap();
        assert!(s.matches(&Bson::Document(doc! {"a": 3})));
        assert!(!s.matches(&Bson::Document(doc! {"a": 1})));
        assert!(!s.matches(&Bson::Document(doc! {"b": 1})));
        let e = s.explain(&Bson::Document(doc! {"a": "x"}));
        assert_eq!(e.len(), 1);
        assert_eq!(e[0].get_str("operatorName").unwrap(), "properties");
    }

    #[test]
    fn unique_items_ignore_field_order() {
        let s = Schema::parse(&doc! {"uniqueItems": true}).unwrap();
        assert!(!s.matches(&Bson::Array(vec![
            Bson::Document(doc! {"x": 1, "y": 2}),
            Bson::Document(doc! {"y": 2, "x": 1}),
        ])));
        assert!(s.matches(&Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)])));
    }

    #[test]
    fn parse_errors() {
        assert_eq!(
            Schema::parse(&doc! {"foo": 1}).unwrap_err().code,
            code::FAILED_TO_PARSE
        );
        assert_eq!(
            Schema::parse(&doc! {"minimum": "x"}).unwrap_err().code,
            code::TYPE_MISMATCH
        );
        assert_eq!(
            Schema::parse(&doc! {"bsonType": "bogus"}).unwrap_err().code,
            code::BAD_VALUE
        );
    }
}
