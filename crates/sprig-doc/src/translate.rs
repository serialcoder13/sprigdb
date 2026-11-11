//! Filter translation to SQL (chapter 31, "Filter translation"; build
//! step 7). Each node becomes SQL plus an exactness: `Exact` SQL selects
//! exactly the matching documents; a `Superset` keeps every match and
//! some others, which the recheck drops.
//!
//! The rules of this release:
//!
//! - **`_id`** compares the encoded key, which sorts in MongoDB order:
//!   equality, `$in` and ranges (bracketed to the literal's class) are
//!   exact.
//! - **Paths with field columns** (an index covers them) and no array on
//!   the path: equality on strings, ObjectIds, dates, booleans and null,
//!   and ranges on strings, ObjectIds and dates, are exact over the
//!   columns; numbers use the `DOUBLE` column as a superset.
//! - **Other scalar-safe paths** use `JSON_VALUE` and `JSON_TYPE` with the
//!   type-map entry: the same exact cases, numbers as supersets.
//! - **Paths that have held arrays** use `JSON_CONTAINS` over every `[*]`
//!   variant of the path (a superset); ranges there aren't pushed.
//! - Negations are pushed only over exact children, as `(x) IS NOT TRUE`.
//! - Everything else (`$type`, `$elemMatch`, `$mod`, `$expr`, regexes
//!   other than `^literal`) is `TRUE`: a superset.

use bson::Bson;

use crate::filter::{FieldOp, InItem, Pred};
use crate::json::iso_from_ms;
use crate::keyenc::{self, class};
use crate::meta::{CollMeta, S_CHARS, columns};
use crate::sql::{bin_lit, f64_lit, ident, json_path, str_lit, types_path, valid_path};

/// SQL for a predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlPred {
    /// The condition.
    pub sql: String,
    /// Whether it selects exactly the matching documents.
    pub exact: bool,
}

impl SqlPred {
    fn t() -> Self {
        Self {
            sql: "TRUE".into(),
            exact: false,
        }
    }

    fn exact(sql: String) -> Self {
        Self { sql, exact: true }
    }

    fn sup(sql: String) -> Self {
        Self { sql, exact: false }
    }
}

impl SqlPred {
    /// The key of a point condition (`_id = X'..'` alone), which the engine
    /// runs as a prepared statement (decision S26).
    #[must_use]
    pub fn point_key(&self) -> Option<Vec<u8>> {
        let hex = self.sql.strip_prefix("_id = X'")?.strip_suffix('\'')?;
        if !self.exact || hex.len() % 2 != 0 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
            .collect()
    }
}

/// Translates `p` for collection `m`.
#[must_use]
pub fn translate(p: &Pred, m: &CollMeta) -> SqlPred {
    match p {
        Pred::True => SqlPred::exact("TRUE".into()),
        Pred::And(kids) => {
            let parts: Vec<SqlPred> = kids.iter().map(|k| translate(k, m)).collect();
            let exact = parts.iter().all(|x| x.exact);
            let sql: Vec<String> = parts
                .into_iter()
                .filter(|x| x.sql != "TRUE")
                .map(|x| format!("({})", x.sql))
                .collect();
            SqlPred {
                sql: if sql.is_empty() {
                    "TRUE".into()
                } else {
                    sql.join(" AND ")
                },
                exact,
            }
        }
        Pred::Or(kids) => {
            let parts: Vec<SqlPred> = kids.iter().map(|k| translate(k, m)).collect();
            if parts.iter().any(|x| x.sql == "TRUE") {
                return SqlPred {
                    sql: "TRUE".into(),
                    exact: parts.iter().all(|x| x.exact),
                };
            }
            let exact = parts.iter().all(|x| x.exact);
            SqlPred {
                sql: parts
                    .into_iter()
                    .map(|x| format!("({})", x.sql))
                    .collect::<Vec<_>>()
                    .join(" OR "),
                exact,
            }
        }
        Pred::Nor(kids) => {
            let inner = translate(&Pred::Or(kids.clone()), m);
            negate(&inner)
        }
        Pred::Field { path, op } => field(path, op, m),
        Pred::Expr(_) => SqlPred::t(),
    }
}

fn negate(inner: &SqlPred) -> SqlPred {
    if inner.exact {
        SqlPred::exact(format!("({}) IS NOT TRUE", inner.sql))
    } else {
        SqlPred::t()
    }
}

fn field(path: &str, op: &FieldOp, m: &CollMeta) -> SqlPred {
    // A collation (chapter 31) compares strings as `utf8mb4_bin` doesn't:
    // nothing is pushed down, the gateway rechecks every candidate.
    if !valid_path(path) || crate::collation::active().is_some() {
        return SqlPred::t();
    }
    if path == "_id" {
        return id_op(op);
    }
    // A numeric part may be an array position or a field name; not pushed.
    if path
        .split('.')
        .any(|p| p.bytes().all(|b| b.is_ascii_digit()))
    {
        return SqlPred::t();
    }
    let arrays = m.may_cross_array(path);
    let cols = !arrays && m.has_columns(path);
    let f = Field {
        path,
        parts: path.split('.').collect(),
        cols,
        arrays,
        m,
    };
    match op {
        FieldOp::Eq(v) => f.eq(v),
        FieldOp::Ne(v) => negate(&f.eq(v)),
        FieldOp::Gt(v) | FieldOp::Gte(v) | FieldOp::Lt(v) | FieldOp::Lte(v) => {
            if arrays {
                return SqlPred::t();
            }
            let o = match op {
                FieldOp::Gt(_) => ">",
                FieldOp::Gte(_) => ">=",
                FieldOp::Lt(_) => "<",
                _ => "<=",
            };
            f.range(o, v)
        }
        FieldOp::In(items) => {
            let mut parts = Vec::new();
            let mut exact = true;
            for it in items {
                let x = match it {
                    InItem::Value(v) => f.eq(v),
                    InItem::Regex(_) => return SqlPred::t(),
                };
                exact &= x.exact;
                if x.sql == "TRUE" {
                    return SqlPred::t();
                }
                parts.push(format!("({})", x.sql));
            }
            if parts.is_empty() {
                return SqlPred::exact("FALSE".into());
            }
            SqlPred {
                sql: parts.join(" OR "),
                exact,
            }
        }
        FieldOp::Nin(items) => negate(&field(path, &FieldOp::In(items.clone()), m)),
        FieldOp::Exists(want) => {
            let e = SqlPred::exact(format!(
                "JSON_CONTAINS_PATH(doc, 'one', {})",
                f.variants().join(", ")
            ));
            if *want { e } else { negate(&e) }
        }
        FieldOp::Size(n) => {
            // Exact when no proper prefix has held an array.
            let parent = path.rsplit_once('.').map(|(p, _)| p);
            if parent.is_some_and(|p| m.may_cross_array(p)) {
                return SqlPred::t();
            }
            let x = f.extract();
            SqlPred::exact(format!(
                "JSON_TYPE({x}) = 'ARRAY' AND JSON_LENGTH({x}) = {n}"
            ))
        }
        FieldOp::Regex(re) => {
            if arrays {
                return SqlPred::t();
            }
            let Some(pre) = re.literal_prefix() else {
                return SqlPred::t();
            };
            let like: String = pre
                .chars()
                .flat_map(|c| match c {
                    '%' | '_' | '\\' => vec!['\\', c],
                    c => vec![c],
                })
                .collect();
            let exact = re.pattern == format!("^{pre}");
            let cond = format!(
                "JSON_TYPE({}) = 'STRING' AND {} IS NULL AND {} COLLATE utf8mb4_bin LIKE {}",
                f.extract(),
                f.ty(),
                f.val(),
                str_lit(&format!("{like}%"))
            );
            SqlPred { sql: cond, exact }
        }
        FieldOp::All(inner) => {
            let kids: Vec<Pred> = inner
                .iter()
                .map(|o| Pred::Field {
                    path: path.to_owned(),
                    op: o.clone(),
                })
                .collect();
            if kids.is_empty() {
                return SqlPred::exact("FALSE".into());
            }
            let t = translate(&Pred::And(kids), m);
            // $all matches arrays element-wise; keep it a superset.
            SqlPred::sup(t.sql)
        }
        FieldOp::Not(inner) => {
            let kids: Vec<Pred> = inner
                .iter()
                .map(|o| Pred::Field {
                    path: path.to_owned(),
                    op: o.clone(),
                })
                .collect();
            negate(&translate(&Pred::And(kids), m))
        }
        FieldOp::Type(_) | FieldOp::ElemMatch(_) | FieldOp::Mod(..) => SqlPred::t(),
        // Chapter 36 (R6 round 3): a box on a `2dsphere` index's columns.
        FieldOp::Geo(g) => SqlPred::sup(crate::geo::prefilter(path, g, m)),
    }
}

fn class_upper(c: u8) -> Vec<u8> {
    vec![c + 1]
}

fn id_op(op: &FieldOp) -> SqlPred {
    let key = |v: &Bson| crate::json::id_key(v).ok();
    match op {
        FieldOp::Eq(v) => match key(v) {
            Some(k) => SqlPred::exact(format!("_id = {}", bin_lit(&k))),
            None => SqlPred::exact("FALSE".into()),
        },
        FieldOp::Ne(v) => match key(v) {
            Some(k) => SqlPred::exact(format!("_id <> {}", bin_lit(&k))),
            None => SqlPred::exact("TRUE".into()),
        },
        FieldOp::In(items) => {
            let mut keys = Vec::new();
            for it in items {
                match it {
                    InItem::Value(v) => {
                        if let Some(k) = key(v) {
                            keys.push(bin_lit(&k));
                        }
                    }
                    InItem::Regex(_) => return SqlPred::t(),
                }
            }
            if keys.is_empty() {
                SqlPred::exact("FALSE".into())
            } else {
                SqlPred::exact(format!("_id IN ({})", keys.join(", ")))
            }
        }
        FieldOp::Gt(v) | FieldOp::Gte(v) | FieldOp::Lt(v) | FieldOp::Lte(v) => {
            let c = keyenc::class_of(v);
            if matches!(v, Bson::Double(f) if f.is_nan())
                || c == class::MIN_KEY
                || c == class::MAX_KEY
                || c == class::NULL
            {
                return SqlPred::t();
            }
            let Some(k) = key(v) else {
                return SqlPred::t();
            };
            let lo = bin_lit(&[c]);
            let hi = bin_lit(&class_upper(c));
            let k = bin_lit(&k);
            let sql = match op {
                FieldOp::Gt(_) => format!("_id > {k} AND _id < {hi}"),
                FieldOp::Gte(_) => format!("_id >= {k} AND _id < {hi}"),
                FieldOp::Lt(_) => format!("_id < {k} AND _id >= {lo}"),
                _ => format!("_id <= {k} AND _id >= {lo}"),
            };
            SqlPred::exact(sql)
        }
        FieldOp::Exists(true) => SqlPred::exact("TRUE".into()),
        FieldOp::Exists(false) => SqlPred::exact("FALSE".into()),
        FieldOp::Nin(items) => negate(&id_op(&FieldOp::In(items.clone()))),
        _ => SqlPred::t(),
    }
}

struct Field<'a> {
    path: &'a str,
    parts: Vec<&'a str>,
    cols: bool,
    arrays: bool,
    m: &'a CollMeta,
}

impl Field<'_> {
    fn jpath(&self) -> String {
        str_lit(&json_path(&self.parts, &[]))
    }

    fn extract(&self) -> String {
        format!("JSON_EXTRACT(doc, {})", self.jpath())
    }

    fn val(&self) -> String {
        format!("JSON_VALUE(doc, {})", self.jpath())
    }

    fn ty(&self) -> String {
        format!("JSON_VALUE(_types, {})", str_lit(&types_path(self.path)))
    }

    /// Every `[*]` variant of the path, as SQL string literals.
    fn variants(&self) -> Vec<String> {
        let wild = self.m.array_parts(self.path);
        let idx: Vec<usize> = (0..wild.len()).filter(|i| wild[*i]).collect();
        let n = idx.len().min(4);
        let mut out = Vec::new();
        for mask in 0u32..(1 << n) {
            let mut w = vec![false; wild.len()];
            for (b, i) in idx.iter().take(n).enumerate() {
                w[*i] = mask & (1 << b) != 0;
            }
            out.push(str_lit(&json_path(&self.parts, &w)));
        }
        out
    }

    fn vals(&self) -> String {
        format!("JSON_EXTRACT(doc, {})", self.variants().join(", "))
    }

    fn col(&self) -> (String, String, String) {
        let (c, n, s) = columns(self.path);
        (ident(&c), ident(&n), ident(&s))
    }

    /// `$eq` with a scalar or other literal.
    fn eq(&self, v: &Bson) -> SqlPred {
        if self.arrays {
            return self.eq_arrays(v);
        }
        if self.cols {
            return self.eq_cols(v);
        }
        let (x, val, ty) = (self.extract(), self.val(), self.ty());
        match v {
            Bson::String(s) => SqlPred::exact(format!(
                "JSON_TYPE({x}) = 'STRING' AND ({ty} IS NULL OR {ty} = 'symbol') AND {val} COLLATE utf8mb4_bin = {}",
                str_lit(s)
            )),
            Bson::Boolean(b) => {
                SqlPred::exact(format!("JSON_TYPE({x}) = 'BOOLEAN' AND {val} = '{b}'"))
            }
            Bson::Null | Bson::Undefined => {
                SqlPred::exact(format!("({x} IS NULL OR JSON_TYPE({x}) = 'NULL')"))
            }
            Bson::ObjectId(o) => {
                SqlPred::exact(format!("{ty} = 'objectId' AND {val} = '{}'", o.to_hex()))
            }
            Bson::DateTime(d) => match iso_from_ms(d.timestamp_millis()) {
                Some(iso) => SqlPred::exact(format!("{ty} = 'date' AND {val} = '{iso}'")),
                None => SqlPred::exact(format!(
                    "{ty} = 'date' AND JSON_TYPE({x}) = 'INTEGER' AND {val} = '{}'",
                    d.timestamp_millis()
                )),
            },
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => {
                match num_lit(v) {
                    Some(n) => SqlPred::sup(format!(
                        "JSON_TYPE({x}) IN ('INTEGER','DOUBLE') AND CAST({val} AS DOUBLE) = {n}"
                    )),
                    None => SqlPred::sup(format!("{ty} IN ('double','decimal')")),
                }
            }
            _ => SqlPred::t(),
        }
    }

    fn eq_cols(&self, v: &Bson) -> SqlPred {
        let (c, n, s) = self.col();
        match v {
            Bson::String(st) => {
                if st.chars().count() < S_CHARS {
                    SqlPred::exact(format!("{c} = 40 AND {s} = {}", str_lit(st)))
                } else {
                    let left: String = st.chars().take(S_CHARS).collect();
                    SqlPred::exact(format!(
                        "{c} = 40 AND {s} = {} AND {} COLLATE utf8mb4_bin = {}",
                        str_lit(&left),
                        self.val(),
                        str_lit(st)
                    ))
                }
            }
            Bson::Boolean(b) => SqlPred::exact(format!("{c} = 90 AND {n} = {}", u8::from(*b))),
            Bson::Null | Bson::Undefined => SqlPred::exact(format!("{c} = 20")),
            Bson::ObjectId(o) => SqlPred::exact(format!("{c} = 80 AND {s} = '{}'", o.to_hex())),
            Bson::DateTime(d) => match iso_from_ms(d.timestamp_millis()) {
                Some(iso) => SqlPred::exact(format!("{c} = 100 AND {s} = '{iso}'")),
                None => SqlPred::sup(format!("{c} = 100")),
            },
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => {
                match num_lit(v) {
                    Some(lit) => SqlPred::sup(format!("{c} = 30 AND {n} = {lit}")),
                    None => SqlPred::sup(format!("{c} = 30")),
                }
            }
            _ => SqlPred::t(),
        }
    }

    fn eq_arrays(&self, v: &Bson) -> SqlPred {
        let lit = match v {
            Bson::String(s) | Bson::Symbol(s) => {
                let mut j = String::new();
                crate::json::write_json_string(&mut j, s);
                j
            }
            Bson::Boolean(b) => b.to_string(),
            Bson::ObjectId(o) => format!("\"{}\"", o.to_hex()),
            Bson::DateTime(d) => match iso_from_ms(d.timestamp_millis()) {
                Some(iso) => format!("\"{iso}\""),
                None => return SqlPred::t(),
            },
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => {
                match num_lit(v) {
                    Some(n) => n,
                    None => return SqlPred::t(),
                }
            }
            _ => return SqlPred::t(),
        };
        SqlPred::sup(format!("JSON_CONTAINS({}, {})", self.vals(), str_lit(&lit)))
    }

    fn range(&self, o: &str, v: &Bson) -> SqlPred {
        let lower = o.starts_with('>');
        let incl = |o: &str| if lower { ">=" } else { "<=" }.to_owned() + &o[o.len()..];
        if self.cols {
            let (c, n, s) = self.col();
            return match v {
                Bson::String(st) => {
                    if st.chars().count() < S_CHARS {
                        SqlPred::exact(format!("{c} = 40 AND {s} {o} {}", str_lit(st)))
                    } else {
                        let left: String = st.chars().take(S_CHARS).collect();
                        SqlPred::sup(format!("{c} = 40 AND {s} {} {}", incl(o), str_lit(&left)))
                    }
                }
                Bson::ObjectId(x) => {
                    SqlPred::exact(format!("{c} = 80 AND {s} {o} '{}'", x.to_hex()))
                }
                Bson::DateTime(d) => match iso_from_ms(d.timestamp_millis()) {
                    Some(iso) if lower => {
                        SqlPred::sup(format!("{c} = 100 AND ({s} {o} '{iso}' OR {s} = '')"))
                    }
                    Some(iso) => SqlPred::sup(format!("{c} = 100 AND {s} {o} '{iso}'")),
                    None => SqlPred::sup(format!("{c} = 100")),
                },
                Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => {
                    match num_lit(v) {
                        Some(lit) => SqlPred::sup(format!("{c} = 30 AND {n} {} {lit}", incl(o))),
                        None => SqlPred::sup(format!("{c} = 30")),
                    }
                }
                _ => SqlPred::t(),
            };
        }
        let (x, val, ty) = (self.extract(), self.val(), self.ty());
        match v {
            Bson::String(st) => SqlPred::exact(format!(
                "JSON_TYPE({x}) = 'STRING' AND ({ty} IS NULL OR {ty} = 'symbol') AND {val} COLLATE utf8mb4_bin {o} {}",
                str_lit(st)
            )),
            Bson::ObjectId(oid) => SqlPred::exact(format!(
                "{ty} = 'objectId' AND {val} {o} '{}'",
                oid.to_hex()
            )),
            Bson::DateTime(d) => match iso_from_ms(d.timestamp_millis()) {
                Some(iso) => SqlPred::sup(format!(
                    "{ty} = 'date' AND (JSON_TYPE({x}) = 'INTEGER' OR {val} {o} '{iso}')"
                )),
                None => SqlPred::sup(format!("{ty} = 'date'")),
            },
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => {
                match num_lit(v) {
                    Some(lit) => SqlPred::sup(format!(
                        "(JSON_TYPE({x}) IN ('INTEGER','DOUBLE') AND CAST({val} AS DOUBLE) {} {lit}) OR {ty} IN ('double','decimal')",
                        incl(o)
                    )),
                    None => SqlPred::t(),
                }
            }
            _ => SqlPred::t(),
        }
    }
}

/// A finite number as an SQL double literal.
fn num_lit(v: &Bson) -> Option<String> {
    let f = match v {
        Bson::Int32(i) => f64::from(*i),
        #[allow(clippy::cast_precision_loss)]
        Bson::Int64(i) => *i as f64,
        Bson::Double(f) => *f,
        Bson::Decimal128(d) => d.to_string().parse::<f64>().ok()?,
        _ => return None,
    };
    f.is_finite().then(|| f64_lit(f))
}

#[cfg(test)]
mod tests {
    use bson::{doc, oid::ObjectId};

    use super::*;
    use crate::filter::parse;
    use crate::meta::IndexDef;

    fn meta(arrays: &[&str], indexed: &[&str]) -> CollMeta {
        CollMeta {
            name: "c".into(),
            table: "c".into(),
            uuid: [0; 16],
            array_paths: arrays.iter().map(|s| (*s).to_owned()).collect(),
            arrays_full: false,
            pending_paths: std::collections::BTreeSet::new(),
            indexes: indexed
                .iter()
                .enumerate()
                .map(|(i, p)| IndexDef {
                    id: u32::try_from(i).unwrap() + 1,
                    name: format!("{p}_1"),
                    key: vec![((*p).to_owned(), 1)],
                    unique: false,
                    sparse: false,
                    expire_after: None,
                    origin: "user".into(),
                    extra: doc! {},
                })
                .collect(),
            options: doc! {},
            next_index: 9,
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn tr(f: bson::Document, m: &CollMeta) -> SqlPred {
        translate(&parse(&f).unwrap(), m)
    }

    /// Decision S26: only a lone `_id` equality is a point; the engine's
    /// text fallback writes the same statement.
    #[test]
    fn point_keys_and_bound_text() {
        use crate::engine::{Param, inline};
        let m = meta(&[], &[]);
        let k = tr(doc! {"_id": 5}, &m).point_key().expect("a point");
        assert_eq!(crate::sql::bin_lit(&k), tr(doc! {"_id": 5}, &m).sql[6..]);
        assert!(tr(doc! {"_id": {"$gt": 5}}, &m).point_key().is_none());
        assert!(tr(doc! {"_id": 5, "a": 1}, &m).point_key().is_none());
        assert!(tr(doc! {"_id": {"$in": [1, 2]}}, &m).point_key().is_none());
        let sql = inline(
            "UPDATE `d`.`t?` SET doc = ?, _types = ? WHERE _id = ? AND note = 'what?' AND _v = ?",
            &[
                Param::Text("it's".into()),
                Param::Null,
                Param::Bin(vec![0x01, 0xab]),
                Param::Int(7),
            ],
        );
        assert_eq!(
            sql,
            "UPDATE `d`.`t?` SET doc = 'it''s', _types = NULL WHERE _id = X'01AB' AND note = 'what?' AND _v = 7"
        );
    }

    #[test]
    fn id_and_scalars_exact() {
        let m = meta(&[], &[]);
        let t = tr(doc! {"_id": 5}, &m);
        assert!(t.exact);
        assert!(t.sql.starts_with("_id = X'1E04"), "{}", t.sql);
        assert!(tr(doc! {"status": "paid"}, &m).exact);
        assert!(tr(doc! {"status": {"$gt": "a"}}, &m).exact);
        assert!(tr(doc! {"flag": true, "n": null}, &m).exact);
        assert!(!tr(doc! {"qty": 5}, &m).exact);
        assert!(tr(doc! {"o": ObjectId::new()}, &m).exact);
        assert!(tr(doc! {"s": {"$ne": "x"}}, &m).sql.contains("IS NOT TRUE"));
        assert_eq!(tr(doc! {"q": {"$ne": 5}}, &m).sql, "TRUE");
        let t = tr(doc! {"_id": {"$gt": 5}}, &m);
        assert!(t.exact && t.sql.contains("_id < X'1F'"), "{}", t.sql);
        assert!(tr(doc! {"s": {"$regex": "^ab"}}, &m).exact);
        assert!(!tr(doc! {"s": {"$regex": "^ab.*c"}}, &m).exact);
        assert!(tr(doc! {"a.b": {"$exists": true}}, &m).exact);
    }

    #[test]
    fn arrays_and_columns() {
        let m = meta(&["tags", "items"], &["status"]);
        let t = tr(doc! {"tags": "x"}, &m);
        assert!(!t.exact);
        assert!(
            t.sql
                .starts_with("JSON_CONTAINS(JSON_EXTRACT(doc, '$.\"tags\"', '$.\"tags\"[*]')"),
            "{}",
            t.sql
        );
        let t = tr(doc! {"items.sku": "A"}, &m);
        assert!(t.sql.contains("'$.\"items\"[*].\"sku\"'"), "{}", t.sql);
        assert_eq!(tr(doc! {"items.q": {"$gt": 1}}, &m).sql, "TRUE");
        let t = tr(doc! {"status": "paid"}, &m);
        assert!(t.exact);
        assert!(t.sql.contains("_c` = 40 AND"), "{}", t.sql);
        let t = tr(doc! {"$or": [{"status": "a"}, {"qty": {"$gt": 2}}]}, &m);
        assert!(!t.exact);
        assert!(t.sql.contains(" OR "));
    }
}
