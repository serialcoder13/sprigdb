//! TTL indexes: the checks of `createIndexes` and `collMod`, and the deletes of
//! the TTL monitor (`sprig_worker::doc_ttl`), as MongoDB 8.0 does them.
//!
//! - **Which documents expire.** A document expires when the indexed field
//!   is a date, or an array holding a date, earlier than now minus
//!   `expireAfterSeconds`: exactly the documents `{path: {$lt: cutoff}}`
//!   matches, since `$lt` with a date compares only dates and looks into
//!   one level of arrays. Other values (strings, numbers, timestamps,
//!   null, missing, nested arrays) never expire.
//! - **Deletes.** [`Engine::expire_batch`] reads up to a batch of matching
//!   documents in key order from a position and deletes them by `_id` and
//!   `_v`, so a document changed since the read (a refreshed date) stays.
//!   The statement is a plain `DELETE`, so change streams show ordinary
//!   `delete` events. A capped collection (MongoDB 8.0 accepts TTL indexes
//!   there and the monitor deletes from them) has its totals marked stale,
//!   as any delete there does.
//! - **Checks** (`createIndexes`, `collMod`): MongoDB 8.0's codes and
//!   texts, from the reference server.

use bson::{Bson, DateTime, Document, doc};
use sprig_proto_mongo::errors::{MongoError, code};

use super::{CollMeta, Engine, Ns, SqlRunner, capped, mongo_error};
use crate::filter;
use crate::json;
use crate::meta::IndexDef;
use crate::sql::{bin_lit, ident};
use crate::translate::translate;

/// The largest `expireAfterSeconds` (a 32-bit integer).
pub const MAX_EXPIRE: i64 = i32::MAX as i64;
/// [`MAX_EXPIRE`] as a double, for doubles and decimals.
const MAX_EXPIRE_F: f64 = 2_147_483_647.0;

/// MongoDB's `InvalidIndexSpecificationOption`.
const INVALID_INDEX_SPEC_OPTION: i32 = 197;
/// MongoDB's `IndexNotFound`.
const INDEX_NOT_FOUND: i32 = 27;
/// MongoDB's `IDLUnknownField`.
const IDL_UNKNOWN_FIELD: i32 = 40415;

/// The TTL path and seconds of index `i`: one field, not `_id`, with
/// `expireAfterSeconds`.
#[must_use]
pub fn ttl_of(i: &IndexDef) -> Option<(&str, i64)> {
    match (i.key.as_slice(), i.expire_after) {
        ([(p, _)], Some(e)) if p != "_id" => Some((p.as_str(), e)),
        _ => None,
    }
}

/// A numeric `expireAfterSeconds` as MongoDB keeps it (truncated), or why
/// not: `Err(None)` for a type that isn't numeric, `Err(Some(text))` for
/// a bad number.
fn seconds(v: &Bson) -> Result<i64, Option<&'static str>> {
    #[allow(clippy::cast_possible_truncation)]
    let n = match v {
        Bson::Int32(i) => i64::from(*i),
        Bson::Int64(i) => *i,
        Bson::Double(d) if d.is_nan() => return Err(Some("must not be NaN")),
        Bson::Double(d) => {
            if *d < 0.0 {
                return Err(Some("cannot be less than 0"));
            }
            if *d > MAX_EXPIRE_F {
                return Err(Some("must be within the range of a 32-bit integer"));
            }
            d.trunc() as i64
        }
        Bson::Decimal128(d) => match d.to_string().parse::<f64>() {
            Ok(f) if f.is_nan() => return Err(Some("must not be NaN")),
            Ok(f) if f < 0.0 => return Err(Some("cannot be less than 0")),
            Ok(f) if f > MAX_EXPIRE_F => {
                return Err(Some("must be within the range of a 32-bit integer"));
            }
            Ok(f) => f.trunc() as i64,
            Err(_) => return Err(Some("must not be NaN")),
        },
        _ => return Err(None),
    };
    if n < 0 {
        return Err(Some("cannot be less than 0"));
    }
    if n > MAX_EXPIRE {
        return Err(Some("must be within the range of a 32-bit integer"));
    }
    Ok(n)
}

/// `createIndexes`' `expireAfterSeconds` of `spec` over key `pattern`:
/// `None` without one.
///
/// # Errors
/// 67 for a value that isn't a number in `[0, 2^31)` or a compound key;
/// 197 on `_id`.
pub fn create_expire(
    spec: &Document,
    pattern: &[(String, i32)],
) -> Result<Option<i64>, MongoError> {
    let Some(v) = spec.get("expireAfterSeconds") else {
        return Ok(None);
    };
    if pattern.len() == 1 && pattern[0].0 == "_id" {
        return Err(MongoError::new(
            INVALID_INDEX_SPEC_OPTION,
            format!(
                "The field 'expireAfterSeconds' is not valid for an _id index specification. Specification: {spec}"
            ),
        ));
    }
    if pattern.len() > 1 {
        return Err(MongoError::new(
            code::CANNOT_CREATE_INDEX,
            format!(
                "TTL indexes are single-field indexes, compound indexes do not support TTL. Index spec: {spec}"
            ),
        ));
    }
    match seconds(v) {
        Ok(n) => Ok(Some(n)),
        Err(why) => {
            let why = why.map_or_else(
                || {
                    format!(
                        "TTL index 'expireAfterSeconds' option must be numeric, but received a type of '{}'",
                        crate::update::type_name(v)
                    )
                },
                |w| format!("TTL index 'expireAfterSeconds' option {w}"),
            );
            Err(MongoError::new(
                code::CANNOT_CREATE_INDEX,
                format!(". Index spec: {spec} :: caused by :: {why}"),
            ))
        }
    }
}

fn invalid(m: impl Into<String>) -> MongoError {
    MongoError::new(code::INVALID_OPTIONS, m)
}

/// `collMod`'s `index: {name | keyPattern, expireAfterSeconds}` on `m`:
/// changes the index in place and returns the reply's fields
/// (`expireAfterSeconds_old` when it had one, `expireAfterSeconds_new`).
/// The caller saves `m`.
///
/// # Errors
/// 14 for wrong types; 40415 for an unknown field; 72 for a missing or
/// doubled identifier, nothing to change, a negative value, `_id` or a
/// compound index; 27 for no such index; 39662 for `hidden`, `unique` and
/// `prepareUnique`, which aren't built.
pub fn coll_mod_index(m: &mut CollMeta, ns: &Ns, index: &Bson) -> Result<Document, MongoError> {
    let Bson::Document(spec) = index else {
        return Err(MongoError::new(
            code::TYPE_MISMATCH,
            format!(
                "BSON field 'collMod.index' is the wrong type '{}', expected type 'object'",
                crate::update::type_name(index)
            ),
        ));
    };
    for k in spec.keys() {
        if !matches!(
            k.as_str(),
            "name" | "keyPattern" | "expireAfterSeconds" | "hidden" | "unique" | "prepareUnique"
        ) {
            return Err(MongoError::new(
                IDL_UNKNOWN_FIELD,
                format!("BSON field 'collMod.index.{k}' is an unknown field."),
            ));
        }
    }
    let name = spec.get("name");
    let key = spec.get("keyPattern");
    match (name, key) {
        (None, None) => return Err(invalid("Must specify either index name or key pattern.")),
        (Some(_), Some(_)) => return Err(invalid("Cannot specify both key pattern and name.")),
        _ => {}
    }
    let Some(v) = spec.get("expireAfterSeconds") else {
        if ["hidden", "unique", "prepareUnique"]
            .iter()
            .any(|k| spec.contains_key(*k))
        {
            return Err(MongoError::new(
                code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                "collMod's index options hidden, unique and prepareUnique are not supported in this release",
            ));
        }
        return Err(invalid(
            "no expireAfterSeconds, hidden, unique, or prepareUnique field",
        ));
    };
    let secs = match seconds(v) {
        Ok(n) => n,
        Err(None) => {
            return Err(MongoError::new(
                code::TYPE_MISMATCH,
                format!(
                    "BSON field 'collMod.index.expireAfterSeconds' is the wrong type '{}', expected types '[double, int, decimal, long']",
                    crate::update::type_name(v)
                ),
            ));
        }
        Err(Some(w)) => {
            return Err(invalid(format!(
                "TTL index 'expireAfterSeconds' option {w}"
            )));
        }
    };
    let found = match (name, key) {
        (Some(Bson::String(n)), _) => {
            if n == "_id_" {
                return Err(invalid("the _id field does not support TTL indexes"));
            }
            m.indexes.iter().position(|i| &i.name == n && !i.is_auto())
        }
        (Some(other), _) => {
            return Err(MongoError::new(
                code::TYPE_MISMATCH,
                format!(
                    "BSON field 'collMod.index.name' is the wrong type '{}', expected type 'string'",
                    crate::update::type_name(other)
                ),
            ));
        }
        (None, Some(Bson::Document(k))) => {
            if k.len() == 1 && k.contains_key("_id") {
                return Err(invalid("the _id field does not support TTL indexes"));
            }
            m.indexes
                .iter()
                .position(|i| !i.is_auto() && i.key_doc() == *k)
        }
        (None, Some(other)) => {
            return Err(MongoError::new(
                code::TYPE_MISMATCH,
                format!(
                    "BSON field 'collMod.index.keyPattern' is the wrong type '{}', expected type 'object'",
                    crate::update::type_name(other)
                ),
            ));
        }
        (None, None) => None,
    };
    let Some(pos) = found else {
        let what = match (name, key) {
            (Some(n), _) => n.as_str().unwrap_or_default().to_owned(),
            (_, Some(k)) => k.to_string(),
            _ => String::new(),
        };
        return Err(MongoError::new(
            INDEX_NOT_FOUND,
            format!("cannot find index {what} for ns {ns}"),
        ));
    };
    let idx = &mut m.indexes[pos];
    if idx.key.len() > 1 {
        return Err(invalid(
            "TTL indexes are single-field indexes, compound indexes do not support TTL",
        ));
    }
    let mut reply = Document::new();
    if let Some(old) = idx.expire_after {
        reply.insert("expireAfterSeconds_old", old);
    }
    reply.insert("expireAfterSeconds_new", secs);
    idx.expire_after = Some(secs);
    Ok(reply)
}

/// What one [`Engine::expire_batch`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Expired {
    /// Documents deleted.
    pub deleted: u64,
    /// Where the next batch starts; `None` when the collection has been
    /// read to its end.
    pub next: Option<Vec<u8>>,
}

impl Engine {
    /// Deletes up to `batch` documents of collection `m` (table `m.table`
    /// in schema `ns.db`, which is a physical shard schema when a worker
    /// calls it) whose `path` holds a date before `cutoff_ms`, reading on
    /// from `after` in key order and at most `max(batch, chunk_rows)` rows
    /// past it (see the module docs). A missing table (a fenced source)
    /// deletes nothing and ends.
    ///
    /// # Errors
    /// The SQL's, as MongoDB errors.
    #[allow(clippy::too_many_arguments)] // the batch's position and size
    pub async fn expire_batch(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
        path: &str,
        cutoff_ms: i64,
        after: Option<Vec<u8>>,
        batch: usize,
    ) -> Result<Expired, MongoError> {
        let pred = filter::parse(&doc! {path: {"$lt": DateTime::from_millis(cutoff_ms)}})?;
        let sqlp = translate(&pred, m);
        let order = capped::KeyOrder::of(m);
        let n = if sqlp.exact {
            batch.max(1)
        } else {
            self.cfg.chunk_rows.max(batch).max(1)
        };
        let rows = self
            .read_rows(
                sql,
                ns,
                &m.table,
                &sqlp,
                after.as_deref(),
                false,
                n,
                true,
                order,
            )
            .await?;
        let cx = crate::agg::AggExpr {
            now: super::now_date(),
        };
        let mut keys: Vec<String> = Vec::new();
        // Where the next batch starts: after the row that filled this one,
        // else after the last row read, or nowhere at the end.
        let mut next = if rows.len() < n {
            None
        } else {
            rows.last().map(|(k, _, _)| k.clone())
        };
        for (k, d, v) in &rows {
            if !pred.matches_with(d, &cx) {
                continue;
            }
            let (Some(id), Some(v)) = (d.get("_id"), v.as_deref()) else {
                continue;
            };
            if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) {
                continue;
            }
            let Ok(key) = json::id_key(id) else {
                continue;
            };
            keys.push(format!("({}, {v})", bin_lit(&key)));
            if keys.len() >= batch {
                if rows.last().is_some_and(|(l, _, _)| l != k) || next.is_some() {
                    next = Some(k.clone());
                }
                break;
            }
        }
        if keys.is_empty() {
            return Ok(Expired { deleted: 0, next });
        }
        let stmt = format!(
            "DELETE FROM {}.{} WHERE (_id, _v) IN ({})",
            ident(&ns.db),
            ident(&m.table),
            keys.join(", ")
        );
        let deleted = match sql.exec(&stmt).await {
            Ok(d) => d.affected,
            Err(e) if e.is(1146) => return Ok(Expired::default()),
            Err(e) => return Err(mongo_error(&e)),
        };
        if deleted > 0 && capped::Cap::of(&m.options).is_some() {
            capped::mark_stale(sql, &ns.db, &m.uuid).await?;
        }
        Ok(Expired { deleted, next })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn idx(key: &[(&str, i32)], e: Option<i64>) -> IndexDef {
        IndexDef {
            id: 1,
            name: "at_1".into(),
            key: key.iter().map(|(p, d)| ((*p).to_owned(), *d)).collect(),
            unique: false,
            sparse: false,
            expire_after: e,
            origin: "user".into(),
            extra: Document::new(),
            mariadb: None,
            auto_index_id: None,
        }
    }

    #[test]
    fn ttl_indexes() {
        assert_eq!(ttl_of(&idx(&[("at", 1)], Some(5))), Some(("at", 5)));
        assert_eq!(ttl_of(&idx(&[("at", 1)], None)), None);
        assert_eq!(ttl_of(&idx(&[("a", 1), ("b", 1)], Some(5))), None);
    }

    #[test]
    fn expiry_predicate_uses_the_field_columns() {
        let mut m = CollMeta {
            name: "c".into(),
            table: "c".into(),
            uuid: [0; 16],
            array_paths: std::collections::BTreeSet::new(),
            arrays_full: false,
            pending_paths: std::collections::BTreeSet::new(),
            indexes: vec![idx(&[("at", 1)], Some(0))],
            options: Document::new(),
            next_index: 2,
        };
        let pred =
            filter::parse(&doc! {"at": {"$lt": DateTime::from_millis(1_700_000_000_000)}}).unwrap();
        let p = translate(&pred, &m);
        eprintln!("{p:?}");
        assert!(p.sql.contains("_sprig_p_"), "{p:?}");
        m.array_paths.insert("at".into());
        assert!(!translate(&pred, &m).exact);
    }

    #[test]
    fn create_checks_match_mongodb() {
        let p = vec![("at".to_owned(), 1)];
        let e = |v: Bson| create_expire(&doc! {"expireAfterSeconds": v}, &p);
        assert_eq!(e(Bson::Int32(10)).unwrap(), Some(10));
        assert_eq!(e(Bson::Double(10.7)).unwrap(), Some(10));
        assert_eq!(e(Bson::Int64(MAX_EXPIRE)).unwrap(), Some(MAX_EXPIRE));
        for bad in [
            Bson::Int32(-1),
            Bson::Double(2_147_483_648.0),
            Bson::String("10".into()),
            Bson::Double(f64::NAN),
            Bson::Boolean(true),
        ] {
            assert_eq!(e(bad).unwrap_err().code, code::CANNOT_CREATE_INDEX);
        }
        let two = vec![("a".to_owned(), 1), ("b".to_owned(), 1)];
        assert_eq!(
            create_expire(&doc! {"expireAfterSeconds": 1}, &two)
                .unwrap_err()
                .code,
            code::CANNOT_CREATE_INDEX
        );
        let id = vec![("_id".to_owned(), 1)];
        assert_eq!(
            create_expire(&doc! {"expireAfterSeconds": 1}, &id)
                .unwrap_err()
                .code,
            197
        );
        assert_eq!(create_expire(&doc! {}, &p).unwrap(), None);
    }

    #[test]
    fn coll_mod_checks_match_mongodb() {
        let ns = Ns {
            db: "d".into(),
            coll: "c".into(),
        };
        let mut m = CollMeta {
            name: "c".into(),
            table: "c".into(),
            uuid: [0; 16],
            array_paths: std::collections::BTreeSet::new(),
            arrays_full: false,
            pending_paths: std::collections::BTreeSet::new(),
            indexes: Vec::new(),
            options: Document::new(),
            next_index: 4,
        };
        m.indexes = vec![idx(&[("at", 1)], Some(10))];
        let mut w = idx(&[("w", 1)], None);
        w.name = "w_1".into();
        m.indexes.push(w);
        let mut pq = idx(&[("p", 1), ("q", 1)], None);
        pq.name = "p_1_q_1".into();
        m.indexes.push(pq);
        let r = coll_mod_index(
            &mut m,
            &ns,
            &Bson::Document(doc! {"name": "at_1", "expireAfterSeconds": 30}),
        )
        .unwrap();
        assert_eq!(
            r,
            doc! {"expireAfterSeconds_old": 10_i64, "expireAfterSeconds_new": 30_i64}
        );
        let r = coll_mod_index(
            &mut m,
            &ns,
            &Bson::Document(doc! {"keyPattern": {"w": 1}, "expireAfterSeconds": 7.9}),
        )
        .unwrap();
        assert_eq!(r, doc! {"expireAfterSeconds_new": 7_i64});
        assert_eq!(m.indexes[1].expire_after, Some(7));
        let c = |d: Document| {
            coll_mod_index(&mut m.clone(), &ns, &Bson::Document(d))
                .unwrap_err()
                .code
        };
        assert_eq!(
            c(doc! {"name": "at_1", "expireAfterSeconds": -5}),
            code::INVALID_OPTIONS
        );
        assert_eq!(
            c(doc! {"name": "at_1", "expireAfterSeconds": "5"}),
            code::TYPE_MISMATCH
        );
        assert_eq!(c(doc! {"name": "nope", "expireAfterSeconds": 5}), 27);
        assert_eq!(c(doc! {"expireAfterSeconds": 5}), code::INVALID_OPTIONS);
        assert_eq!(
            c(doc! {"name": "at_1", "keyPattern": {"at": 1}, "expireAfterSeconds": 5}),
            code::INVALID_OPTIONS
        );
        assert_eq!(c(doc! {"name": "at_1"}), code::INVALID_OPTIONS);
        assert_eq!(
            c(doc! {"name": "p_1_q_1", "expireAfterSeconds": 5}),
            code::INVALID_OPTIONS
        );
        assert_eq!(
            c(doc! {"name": "_id_", "expireAfterSeconds": 5}),
            code::INVALID_OPTIONS
        );
        assert_eq!(
            c(doc! {"name": "at_1", "expireAfterSeconds": 5, "foo": 1}),
            40415
        );
        assert_eq!(
            coll_mod_index(&mut m, &ns, &Bson::Int32(5))
                .unwrap_err()
                .code,
            code::TYPE_MISMATCH
        );
    }
}
