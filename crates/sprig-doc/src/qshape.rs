//! MongoDB query shapes (M3, decisions S96–S98): the shape of a `find`,
//! `aggregate`, `distinct` or `count`, and of the write commands, as
//! `$queryStats` and query settings key them. Literal values become type
//! placeholders (`"?number"`, `"?string"` ...), the spelling chapter 12
//! uses for MongoDB shapes; field names, operators, sort and projection
//! specs stay.
//!
//! The shape follows MongoDB 8.0's `debugQueryShape` (a filter's clauses
//! as `{path: {$op: placeholder}}`, several under `$and`), and its hash is
//! SHA-256 over the shape's BSON. MongoDB's own hash covers an internal
//! serialization, so the hash strings differ from MongoDB's for the same
//! query (S96); they are stable across SprigDB gateways and versions.

use base64::Engine as _;
use bson::{Bson, Document, doc};
use sha2::{Digest, Sha256};

/// The placeholder of a literal.
#[must_use]
pub fn placeholder(v: &Bson) -> Bson {
    let s = match v {
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => "?number",
        Bson::String(_) | Bson::Symbol(_) => "?string",
        Bson::Boolean(_) => "?bool",
        Bson::Null | Bson::Undefined => "?null",
        Bson::DateTime(_) => "?date",
        Bson::ObjectId(_) => "?objectId",
        Bson::Document(_) => "?object",
        Bson::Binary(_) => "?binData",
        Bson::RegularExpression(_) => "?regex",
        Bson::Timestamp(_) => "?timestamp",
        Bson::JavaScriptCode(_) | Bson::JavaScriptCodeWithScope(_) => "?javascript",
        Bson::MinKey => "?minKey",
        Bson::MaxKey => "?maxKey",
        Bson::DbPointer(_) => "?dbPointer",
        Bson::Array(a) => {
            if a.is_empty() {
                return Bson::Array(Vec::new());
            }
            let kinds: Vec<Bson> = a.iter().map(placeholder).collect();
            let first = &kinds[0];
            return if kinds.iter().all(|k| k == first) {
                match first {
                    Bson::String(s) => Bson::String(format!("?array<{s}>")),
                    _ => Bson::String("?array<>".into()),
                }
            } else {
                Bson::String("?array<>".into())
            };
        }
    };
    Bson::String(s.into())
}

/// A filter's shape.
#[must_use]
pub fn filter_shape(f: &Document) -> Document {
    let mut clauses: Vec<Document> = Vec::new();
    for (k, v) in f {
        match k.as_str() {
            "$and" | "$or" | "$nor" => {
                let kids: Vec<Bson> = match v {
                    Bson::Array(a) => a
                        .iter()
                        .map(|x| match x {
                            Bson::Document(d) => Bson::Document(filter_shape(d)),
                            other => placeholder(other),
                        })
                        .collect(),
                    other => vec![placeholder(other)],
                };
                clauses.push(doc! {k.clone(): kids});
            }
            "$expr" => clauses.push(doc! {"$expr": expr_shape(v)}),
            "$jsonSchema" => clauses.push(doc! {"$jsonSchema": v.clone()}),
            "$comment" => {}
            "$text" => {
                let mut t = Document::new();
                if let Bson::Document(d) = v {
                    for (tk, tv) in d {
                        t.insert(tk.clone(), placeholder(tv));
                    }
                }
                clauses.push(doc! {"$text": t});
            }
            _ => match v {
                Bson::Document(d) if d.keys().next().is_some_and(|x| x.starts_with('$')) => {
                    for (op, arg) in d {
                        if op == "$options" {
                            continue;
                        }
                        let a = match op.as_str() {
                            "$not" => match arg {
                                Bson::Document(n) => {
                                    Bson::Document(filter_shape(&doc! {k.clone(): n.clone()}))
                                }
                                other => placeholder(other),
                            },
                            "$elemMatch" => match arg {
                                Bson::Document(n) => Bson::Document(filter_shape(n)),
                                other => placeholder(other),
                            },
                            "$exists" | "$type" => arg.clone(),
                            _ => placeholder(arg),
                        };
                        clauses.push(doc! {k.clone(): {op.clone(): a}});
                    }
                }
                _ => clauses.push(doc! {k.clone(): {"$eq": placeholder(v)}}),
            },
        }
    }
    match clauses.len() {
        0 => Document::new(),
        1 => clauses.pop().unwrap_or_default(),
        _ => doc! {"$and": clauses},
    }
}

/// An aggregation expression's shape: field paths (`"$a"`), operators and
/// names stay; other literals become placeholders.
#[must_use]
pub fn expr_shape(v: &Bson) -> Bson {
    match v {
        Bson::String(s) if s.starts_with('$') => v.clone(),
        Bson::Document(d) => {
            Bson::Document(d.iter().map(|(k, x)| (k.clone(), expr_shape(x))).collect())
        }
        Bson::Array(a) => Bson::Array(a.iter().map(expr_shape).collect()),
        other => placeholder(other),
    }
}

fn spec_shape(v: &Bson) -> Bson {
    // Sort and projection specs: 1/-1/0/true/false stay, expressions are
    // shaped.
    match v {
        Bson::Document(d) => Bson::Document(
            d.iter()
                .map(|(k, x)| {
                    let y = match x {
                        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Boolean(_) => {
                            x.clone()
                        }
                        other => expr_shape(other),
                    };
                    (k.clone(), y)
                })
                .collect(),
        ),
        other => placeholder(other),
    }
}

fn stage_shape(st: &Document) -> Document {
    let Some((name, arg)) = st.iter().next() else {
        return Document::new();
    };
    let shaped = match name.as_str() {
        "$match" => match arg {
            Bson::Document(f) => Bson::Document(filter_shape(f)),
            other => placeholder(other),
        },
        "$limit" | "$skip" | "$sample" | "$count" | "$out" | "$unwind" | "$sortByCount" => {
            match (name.as_str(), arg) {
                ("$count" | "$out" | "$unwind" | "$sortByCount", _) => expr_shape(arg),
                _ => placeholder(arg),
            }
        }
        "$sort" | "$project" => spec_shape(arg),
        "$lookup" | "$graphLookup" | "$merge" | "$unionWith" | "$facet" => match arg {
            Bson::Document(d) => Bson::Document(
                d.iter()
                    .map(|(k, x)| {
                        let y = match (k.as_str(), x) {
                            ("pipeline", Bson::Array(a)) => Bson::Array(
                                a.iter()
                                    .filter_map(Bson::as_document)
                                    .map(|s| Bson::Document(stage_shape(s)))
                                    .collect(),
                            ),
                            (_, Bson::String(_)) => x.clone(),
                            (_, Bson::Array(a)) if name == "$facet" => Bson::Array(
                                a.iter()
                                    .filter_map(Bson::as_document)
                                    .map(|s| Bson::Document(stage_shape(s)))
                                    .collect(),
                            ),
                            _ => expr_shape(x),
                        };
                        (k.clone(), y)
                    })
                    .collect(),
            ),
            other => expr_shape(other),
        },
        _ => expr_shape(arg),
    };
    doc! {name.clone(): shaped}
}

/// The shape of command `name` (`body`) on database `db`, or `None` for a
/// command without one. Reads: `find`, `aggregate`, `distinct`, `count`;
/// writes (MongoDB 9.0's `$queryStats`): `insert`, `update`, `delete`.
#[must_use]
pub fn shape(name: &str, body: &Document, db: &str) -> Option<Document> {
    let coll = match body.get(name) {
        Some(Bson::String(s)) => s.clone(),
        Some(_) if name == "aggregate" => "$cmd.aggregate".to_owned(),
        _ => return None,
    };
    let mut d = doc! {"cmdNs": {"db": db, "coll": coll}, "command": name};
    match name {
        "find" => {
            if let Ok(f) = body.get_document("filter") {
                d.insert("filter", filter_shape(f));
            } else {
                d.insert("filter", Document::new());
            }
            for k in ["sort", "projection"] {
                if let Some(v) = body
                    .get(k)
                    .filter(|v| v.as_document().is_some_and(|x| !x.is_empty()))
                {
                    d.insert(k, spec_shape(v));
                }
            }
            for k in ["skip", "limit", "batchSize"] {
                if body.contains_key(k) && k != "batchSize" {
                    d.insert(k, "?number");
                }
            }
            if let Some(h) = body.get("hint") {
                d.insert("hint", h.clone());
            }
            if body.get_bool("singleBatch").unwrap_or(false) {
                d.insert("singleBatch", true);
            }
        }
        "aggregate" => {
            let stages: Vec<Bson> = body
                .get_array("pipeline")
                .map(|a| {
                    a.iter()
                        .filter_map(Bson::as_document)
                        .map(|s| Bson::Document(stage_shape(s)))
                        .collect()
                })
                .unwrap_or_default();
            d.insert("pipeline", stages);
            if body.get_bool("explain").unwrap_or(false) {
                d.insert("explain", true);
            }
        }
        "distinct" => {
            d.insert("key", body.get_str("key").unwrap_or_default());
            if let Ok(q) = body.get_document("query") {
                d.insert("query", filter_shape(q));
            }
        }
        "count" => {
            if let Ok(q) = body.get_document("query") {
                d.insert("query", filter_shape(q));
            }
        }
        "insert" => {}
        "update" | "delete" => {
            let key = if name == "update" {
                "updates"
            } else {
                "deletes"
            };
            let list: Vec<Bson> = body
                .get_array(key)
                .map(|a| {
                    a.iter()
                        .filter_map(Bson::as_document)
                        .map(|u| {
                            let mut s = doc! {"q": u.get_document("q").map(filter_shape).unwrap_or_default()};
                            if name == "update" {
                                let um = match u.get("u") {
                                    Some(Bson::Document(m)) if m.keys().next().is_some_and(|k| k.starts_with('$')) => {
                                        Bson::Document(m.iter().map(|(op, f)| {
                                            let fs = match f {
                                                Bson::Document(fd) => Bson::Document(fd.iter().map(|(p, x)| (p.clone(), placeholder(x))).collect()),
                                                other => placeholder(other),
                                            };
                                            (op.clone(), fs)
                                        }).collect())
                                    }
                                    Some(Bson::Array(p)) => Bson::Array(p.iter().filter_map(Bson::as_document).map(|s| Bson::Document(stage_shape(s))).collect()),
                                    Some(_) => Bson::String("?object".into()),
                                    None => Bson::Null,
                                };
                                s.insert("u", um);
                                s.insert("multi", u.get_bool("multi").unwrap_or(false));
                                s.insert("upsert", u.get_bool("upsert").unwrap_or(false));
                            } else {
                                s.insert("limit", u.get("limit").cloned().unwrap_or(Bson::Int32(0)));
                            }
                            Bson::Document(s)
                        })
                        .collect()
                })
                .unwrap_or_default();
            d.insert(key, list);
        }
        _ => return None,
    }
    Some(d)
}

fn digest(d: &Document) -> [u8; 32] {
    let bytes = bson::to_vec(d).unwrap_or_default();
    Sha256::digest(&bytes).into()
}

/// The shape's hash as `queryShapeHash` prints it: 64 upper-case hex
/// digits.
#[must_use]
pub fn shape_hash(shape: &Document) -> String {
    use std::fmt::Write as _;
    digest(shape)
        .iter()
        .fold(String::with_capacity(64), |mut out, b| {
            let _ = write!(out, "{b:02X}");
            out
        })
}

/// A `$queryStats` key's hash (`keyHash`): base64 of its SHA-256.
#[must_use]
pub fn key_hash(key: &Document) -> String {
    base64::engine::general_purpose::STANDARD.encode(digest(key))
}

/// Whether `s` looks like a `queryShapeHash`.
#[must_use]
pub fn is_hash(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_lose_their_values() {
        let a = shape(
            "find",
            &doc! {"find": "c", "filter": {"a": 5, "b": {"$gt": 1}}, "limit": 3},
            "d",
        )
        .unwrap();
        let b = shape(
            "find",
            &doc! {"find": "c", "filter": {"a": 7, "b": {"$gt": 9}}, "limit": 1},
            "d",
        )
        .unwrap();
        assert_eq!(shape_hash(&a), shape_hash(&b));
        assert_eq!(
            a.get_document("filter").unwrap(),
            &doc! {"$and": [{"a": {"$eq": "?number"}}, {"b": {"$gt": "?number"}}]}
        );
        let c = shape("find", &doc! {"find": "c", "filter": {"a": "x"}}, "d").unwrap();
        assert_ne!(shape_hash(&a), shape_hash(&c));
        assert!(is_hash(&shape_hash(&c)));
    }
}
