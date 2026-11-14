//! Row-level security policies on collections in MongoDB's filter form
//! (`sprigCreatePolicy`).
//!
//! A policy written as a MongoDB filter becomes an SQL expression over the
//! collection's table (`doc`, `_types`), stored as an ordinary policy
//! with the filter kept beside it (`using_mongo`, `check_mongo`). Every
//! read and write of the document engine is SQL through the user's
//! session, so the gateway's rewrite, the policy views and the write
//! triggers apply it as they do to tables; change streams evaluate the
//! filter itself ([`Pred::matches`](crate::filter::Pred::matches)).
//!
//! The SQL is never wider than MongoDB's match (a document the filter
//! doesn't match is never visible); where MongoDB's semantics need more
//! than a JSON path can say, it is narrower and the document stays hidden:
//!
//! ```text
//! {f: v}, {f: {$eq: v}}  v a string, number or boolean of the same type at f (not
//!                        ObjectId, date or other typed values), or an element of
//!                        an array at f whose elements are plain JSON
//! {f: {$in: [v, ...]}}   any of them
//! {f: {$gt|$gte|$lt|$lte: v}}  the same type at f, compared (strings binary)
//! {f: {$ne|$nin: v}}     f present, a plain scalar, and not equal (missing and
//!                        arrays: hidden)
//! {f: {$exists: true}}   f present
//! $and, $or              combined; $nor, $not, $expr, $where, $regex, null,
//!                        $exists: false and other operators: refused (39364)
//! "$$USER", "$$USER_ID", "$$TENANT_ID", "$$HOME_TENANT_ID", "$$SESSION_SPACE",
//! "$$CLAIM.<k>", "$$ATTR.<k>"   the session functions (strings, ids as numbers)
//! ```
//!
//! A path through an array (`a.b` where `a` is an array) reads nothing, so
//! such documents stay hidden too.

use bson::{Bson, Document};
use sprig_proto_mongo::errors::MongoError;

use crate::sql::{json_path, str_lit, types_path, valid_path};

/// 39364 `ER_SPRIG_RLS_POLICY_INVALID`.
pub const POLICY_INVALID: i32 = 39364;

fn invalid(why: impl std::fmt::Display) -> MongoError {
    MongoError::new(
        POLICY_INVALID,
        format!("ER_SPRIG_RLS_POLICY_INVALID: {why}"),
    )
}

/// A value a condition compares with.
#[derive(Debug, Clone, PartialEq)]
enum Val {
    /// A string literal or a string session function.
    Str(String),
    /// A number literal or a numeric session function.
    Num(String),
    /// A boolean.
    Bool(bool),
}

fn session(s: &str) -> Option<Val> {
    let rest = s.strip_prefix("$$")?;
    Some(match rest {
        "USER" => Val::Str("CURRENT_USER()".into()),
        "USER_ID" => Val::Num("SPRIG_USER_ID()".into()),
        "TENANT_ID" => Val::Num("SPRIG_TENANT_ID()".into()),
        "HOME_TENANT_ID" => Val::Num("SPRIG_HOME_TENANT_ID()".into()),
        "SESSION_SPACE" => Val::Num("SPRIG_SESSION_SPACE()".into()),
        _ => {
            if let Some(k) = rest.strip_prefix("CLAIM.") {
                Val::Str(format!("SPRIG_CLAIM({})", str_lit(k)))
            } else if let Some(k) = rest.strip_prefix("ATTR.") {
                Val::Str(format!("SPRIG_USER_ATTR({})", str_lit(k)))
            } else {
                return None;
            }
        }
    })
}

fn value(v: &Bson) -> Result<Val, MongoError> {
    Ok(match v {
        Bson::String(s) if s.starts_with("$$") => {
            session(s).ok_or_else(|| invalid(format!("unknown session value '{s}'")))?
        }
        Bson::String(s) => Val::Str(str_lit(s)),
        Bson::Int32(i) => Val::Num(i.to_string()),
        Bson::Int64(i) => Val::Num(i.to_string()),
        Bson::Double(f) if f.is_finite() => Val::Num(crate::sql::f64_lit(*f)),
        Bson::Boolean(b) => Val::Bool(*b),
        Bson::Null => {
            return Err(invalid(
                "null in a policy filter (missing fields can't be told from fields under arrays)",
            ));
        }
        other => {
            return Err(invalid(format!(
                "a {:?} value in a policy filter (strings, numbers, booleans and session values only)",
                other.element_type()
            )));
        }
    })
}

/// SQL pieces for one field path.
struct Path {
    /// `JSON_EXTRACT(doc, '$."a"."b"')`.
    extract: String,
    /// `JSON_VALUE(doc, ...)`.
    value: String,
    /// `JSON_VALUE(_types, '$."a.b"')`.
    typed: String,
    /// The `_types` key prefix of elements under the path (`a.b.`), as a
    /// `LIKE` pattern.
    elements: String,
}

impl Path {
    fn new(path: &str) -> Result<Self, MongoError> {
        if !valid_path(path) {
            return Err(invalid(format!("field path '{path}'")));
        }
        let parts: Vec<&str> = path.split('.').collect();
        let p = str_lit(&json_path(&parts, &[]));
        let t = str_lit(&types_path(path));
        let key: Vec<String> = path.split('.').map(crate::json::escape_name).collect();
        let like = key
            .join(".")
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        Ok(Self {
            extract: format!("JSON_EXTRACT(doc, {p})"),
            value: format!("JSON_VALUE(doc, {p})"),
            typed: format!("JSON_VALUE(_types, {t})"),
            elements: str_lit(&format!("{like}.%")),
        })
    }

    fn jtype(&self) -> String {
        format!("JSON_TYPE({})", self.extract)
    }

    /// The value at the path is a plain string, number or boolean of
    /// `v`'s kind (no `_types` entry beyond numbers' own).
    fn same_kind(&self, v: &Val) -> String {
        match v {
            Val::Str(_) => format!("{} = 'STRING' AND {} IS NULL", self.jtype(), self.typed),
            Val::Num(_) => format!(
                "{} IN ('INTEGER', 'DOUBLE') AND COALESCE({}, 'long') IN ('long', 'double', 'decimal')",
                self.jtype(),
                self.typed
            ),
            Val::Bool(_) => format!("{} = 'BOOLEAN'", self.jtype()),
        }
    }

    fn compare(&self, op: &str, v: &Val) -> String {
        match v {
            Val::Str(s) => format!("{} COLLATE utf8mb4_bin {op} CAST({s} AS CHAR)", self.value),
            Val::Num(n) => format!("CAST({} AS DECIMAL(65, 30)) {op} {n}", self.value),
            Val::Bool(b) => format!("{} {op} '{b}'", self.value),
        }
    }

    /// An array at the path whose elements are plain JSON contains `v`.
    fn array_has(&self, v: &Val) -> String {
        let probe = match v {
            Val::Str(s) => format!("JSON_QUOTE({s})"),
            Val::Num(n) => format!("CAST({n} AS CHAR)"),
            Val::Bool(b) => format!("'{b}'"),
        };
        format!(
            "({} = 'ARRAY' AND (_types IS NULL OR JSON_SEARCH(JSON_KEYS(_types), 'one', {}) IS NULL) \
             AND JSON_CONTAINS({}, {probe}))",
            self.jtype(),
            self.elements,
            self.extract
        )
    }

    fn eq(&self, v: &Val) -> String {
        let scalar = format!("({} AND {})", self.same_kind(v), self.compare("=", v));
        format!("({scalar} OR {})", self.array_has(v))
    }

    /// Present, a plain scalar, and not `v` (narrower than `$ne`).
    fn ne(&self, v: &Val) -> String {
        let plain = format!(
            "{} IN ('STRING', 'INTEGER', 'DOUBLE', 'BOOLEAN') AND (_types IS NULL OR {} IS NULL)",
            self.jtype(),
            self.typed
        );
        format!(
            "({plain} AND NOT ({} AND {}))",
            self.same_kind(v),
            self.compare("=", v)
        )
    }
}

fn field(path: &str, cond: &Bson) -> Result<String, MongoError> {
    let p = Path::new(path)?;
    let ops = match cond {
        Bson::Document(d) if d.keys().next().is_some_and(|k| k.starts_with('$')) => d,
        v => return Ok(p.eq(&value(v)?)),
    };
    let mut out = Vec::new();
    for (op, v) in ops {
        out.push(match op.as_str() {
            "$eq" => p.eq(&value(v)?),
            "$ne" => p.ne(&value(v)?),
            "$gt" | "$gte" | "$lt" | "$lte" => {
                let v = value(v)?;
                if matches!(v, Val::Bool(_)) {
                    return Err(invalid(format!("{op} on a boolean")));
                }
                let sql = match op.as_str() {
                    "$gt" => ">",
                    "$gte" => ">=",
                    "$lt" => "<",
                    _ => "<=",
                };
                format!("({} AND {})", p.same_kind(&v), p.compare(sql, &v))
            }
            "$in" | "$nin" => {
                let Bson::Array(a) = v else {
                    return Err(invalid(format!("{op} needs an array")));
                };
                if a.is_empty() {
                    if op == "$in" {
                        "FALSE".to_owned()
                    } else {
                        return Err(invalid("$nin with an empty array"));
                    }
                } else {
                    let parts = a
                        .iter()
                        .map(|x| {
                            let x = value(x)?;
                            Ok(if op == "$in" { p.eq(&x) } else { p.ne(&x) })
                        })
                        .collect::<Result<Vec<_>, MongoError>>()?;
                    let join = if op == "$in" { " OR " } else { " AND " };
                    format!("({})", parts.join(join))
                }
            }
            "$exists" => match v {
                Bson::Boolean(true) | Bson::Int32(1) | Bson::Int64(1) => {
                    format!("({} IS NOT NULL)", p.extract)
                }
                _ => {
                    return Err(invalid(
                        "$exists: false (a field under an array can't be told from a missing one)",
                    ));
                }
            },
            other => return Err(invalid(format!("{other} in a policy filter"))),
        });
    }
    if out.is_empty() {
        return Err(invalid(format!("an empty condition on '{path}'")));
    }
    Ok(format!("({})", out.join(" AND ")))
}

/// The SQL expression of policy filter `filter` over a collection's table
/// (see the module docs). An empty filter is `TRUE`.
///
/// # Errors
/// 39364 for what a policy filter can't hold.
pub fn to_sql(filter: &Document) -> Result<String, MongoError> {
    let mut out = Vec::new();
    for (k, v) in filter {
        out.push(match k.as_str() {
            "$and" | "$or" => {
                let Bson::Array(a) = v else {
                    return Err(invalid(format!("{k} needs an array")));
                };
                if a.is_empty() {
                    return Err(invalid(format!("{k} with an empty array")));
                }
                let parts = a
                    .iter()
                    .map(|x| match x {
                        Bson::Document(d) => to_sql(d),
                        _ => Err(invalid(format!("{k} needs documents"))),
                    })
                    .collect::<Result<Vec<_>, MongoError>>()?;
                let join = if k == "$and" { " AND " } else { " OR " };
                format!("({})", parts.join(join))
            }
            other if other.starts_with('$') => {
                return Err(invalid(format!("{other} in a policy filter")));
            }
            path => field(path, v)?,
        });
    }
    if out.is_empty() {
        return Ok("TRUE".to_owned());
    }
    Ok(out.join(" AND "))
}

/// Policy filter `filter` with its session values bound, for the
/// MongoDB matcher (change streams): `"$$USER"` and the others become the
/// session's values that `lookup` answers. `None` when one of them has no
/// value (`NULL` in SQL): the policy then shows nothing, which is never
/// wider than the SQL form.
#[must_use]
pub fn bind(filter: &Document, lookup: &dyn Fn(&str) -> Option<Bson>) -> Option<Document> {
    fn walk(v: &Bson, lookup: &dyn Fn(&str) -> Option<Bson>) -> Option<Bson> {
        Some(match v {
            Bson::String(s) if s.starts_with("$$") => lookup(&s[2..])?,
            Bson::Document(d) => Bson::Document(
                d.iter()
                    .map(|(k, x)| Some((k.clone(), walk(x, lookup)?)))
                    .collect::<Option<Document>>()?,
            ),
            Bson::Array(a) => Bson::Array(
                a.iter()
                    .map(|x| walk(x, lookup))
                    .collect::<Option<Vec<_>>>()?,
            ),
            other => other.clone(),
        })
    }
    filter
        .iter()
        .map(|(k, x)| Some((k.clone(), walk(x, lookup)?)))
        .collect()
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::*;

    #[test]
    fn filters_become_sql() {
        let s = to_sql(&doc! {"owner": "$$USER"}).unwrap();
        assert!(
            s.contains("JSON_TYPE(JSON_EXTRACT(doc, '$.\"owner\"')) = 'STRING'"),
            "{s}"
        );
        assert!(
            s.contains("COLLATE utf8mb4_bin = CAST(CURRENT_USER() AS CHAR)"),
            "{s}"
        );
        assert!(
            s.contains(
                "JSON_CONTAINS(JSON_EXTRACT(doc, '$.\"owner\"'), JSON_QUOTE(CURRENT_USER()))"
            ),
            "{s}"
        );
        let s = to_sql(
            &doc! {"$or": [{"team": {"$in": [1, 2]}}, {"public": true}], "level": {"$lt": 5}},
        )
        .unwrap();
        assert!(
            s.starts_with("(((") && s.contains(" OR ") && !s.contains("<= 5"),
            "{s}"
        );
        assert!(
            s.contains("CAST(JSON_VALUE(doc, '$.\"level\"') AS DECIMAL(65, 30)) < 5"),
            "{s}"
        );
        let s = to_sql(&doc! {"tenant": "$$CLAIM.org", "n": {"$ne": "x"}}).unwrap();
        assert!(
            s.contains("SPRIG_CLAIM('org')") && s.contains("NOT ("),
            "{s}"
        );
        assert_eq!(to_sql(&doc! {}).unwrap(), "TRUE");
        for bad in [
            doc! {"a": null},
            doc! {"a": {"$exists": false}},
            doc! {"$nor": [{"a": 1}]},
            doc! {"a": {"$regex": "x"}},
            doc! {"a": "$$NOPE"},
            doc! {"$where": "1"},
            doc! {"a": {"$gt": true}},
            doc! {"a": bson::oid::ObjectId::new()},
        ] {
            assert_eq!(to_sql(&bad).unwrap_err().code, POLICY_INVALID, "{bad}");
        }
    }

    #[test]
    fn session_values_bind_for_the_matcher() {
        let f = doc! {"owner": "$$USER", "n": {"$in": ["$$CLAIM.x", 3]}};
        let b = bind(&f, &|k| match k {
            "USER" => Some(Bson::String("a@%".into())),
            "CLAIM.x" => Some(Bson::Int32(9)),
            _ => None,
        });
        assert_eq!(b, Some(doc! {"owner": "a@%", "n": {"$in": [9, 3]}}));
        assert_eq!(bind(&f, &|_| None), None);
    }
}
