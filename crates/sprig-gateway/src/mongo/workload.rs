//! MongoDB commands in the workload store (`Fingerprinter::mongo`).
//!
//! ```text
//! find, aggregate ($match first), count, distinct, update, delete,
//! findAndModify on a collection, outside admin:
//!   before: shape (sprig_doc::qshape, the $queryStats grouping)
//!           + the filter's top-level conditions with their values
//!           → sprig_workload::fingerprint::mongo::fingerprint
//!           plan := recorder.plan(tenant, fp)       probe? sample?
//!   run:    the command's statements run under FRONT_RUN: when the plan
//!           probes, every statement reads Handler_read_* before and after
//!           (the workload probe), and the rows examined are summed
//!   after:  recorder.record(tenant, fp, summary): time, error, documents
//!           returned or changed, rows examined when probed, one shard
//!           [sample] the values by slot, sealed like a SQL sample
//! ```
//!
//! The engine's own SQL statements are still fingerprinted as SQL, as
//! before; the MongoDB fingerprint adds the command's view: its whole time,
//! the documents it returned and the field paths it filters and sorts on,
//! which the automatic-index advisor's document candidates read.

use std::sync::Arc;
use std::time::Instant;

use bson::{Bson, Document, doc};
use sprig_common::ids::{DbId, TenantId};
use sprig_doc::engine::{Engine, Ns};
use sprig_doc::meta::CollMeta;
use sprig_doc::qshape;
use sprig_workload::fingerprint::mongo::{MongoInput, MongoPred, boolean, number, quoted};
use sprig_workload::fingerprint::{Fingerprint, LiteralKind};
use sprig_workload::recorder::{ExecSummary, ProbePlan};
use sprig_workload::samples::{SampleContext, SamplePayload};
use sprig_workload::shape::PredOp;
use sprig_workload::window::{SampleValue, SampledLiterals, SampledSlot};

use super::sql::SqlSession;
use crate::workload::{FRONT_RUN, FrontCapture, FrontRun};

/// The commands fingerprinted.
#[must_use]
pub fn captured(name: &str) -> bool {
    matches!(
        name,
        "find" | "aggregate" | "count" | "distinct" | "update" | "delete" | "findAndModify"
    )
}

/// A command between [`start`] and [`finish`].
pub struct Started {
    cap: FrontCapture,
    tenant: TenantId,
    fp: Fingerprint,
    literals: SampledLiterals,
    plan: ProbePlan,
    run: Arc<FrontRun>,
    at: Instant,
    db: String,
    db_id: DbId,
    session: u64,
}

impl std::fmt::Debug for Started {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Started")
            .field("fingerprint", &self.fp.id)
            .finish_non_exhaustive()
    }
}

impl Started {
    /// The run its statements share ([`FRONT_RUN`]).
    #[must_use]
    pub fn run(&self) -> Arc<FrontRun> {
        Arc::clone(&self.run)
    }
}

/// The filter, sort and limit of a command.
fn parts(name: &str, body: &Document) -> Option<(Document, Document, bool)> {
    let d = |k: &str| body.get_document(k).ok().cloned().unwrap_or_default();
    Some(match name {
        "find" => (d("filter"), d("sort"), body.contains_key("limit")),
        "count" | "distinct" => (d("query"), Document::new(), body.contains_key("limit")),
        "findAndModify" => (d("query"), d("sort"), true),
        "update" | "delete" => {
            let key = if name == "update" {
                "updates"
            } else {
                "deletes"
            };
            let first = body
                .get_array(key)
                .ok()?
                .first()?
                .as_document()?
                .get_document("q")
                .ok()?
                .clone();
            (first, Document::new(), name == "delete")
        }
        "aggregate" => {
            let stages = body.get_array("pipeline").ok()?;
            let mut it = stages.iter().filter_map(Bson::as_document);
            let mut filter = Document::new();
            let mut sort = Document::new();
            let mut limit = false;
            let mut next = it.next();
            if let Some(m) = next.and_then(|s| s.get_document("$match").ok()) {
                filter = m.clone();
                next = it.next();
            }
            if let Some(s) = next.and_then(|s| s.get_document("$sort").ok()) {
                sort = s.clone();
                next = it.next();
            }
            if next.is_some_and(|s| s.contains_key("$limit")) {
                limit = true;
            }
            (filter, sort, limit)
        }
        _ => return None,
    })
}

/// A path the engine can index: dotted names, no array positions, no `$`.
fn indexable_path(p: &str) -> bool {
    !p.is_empty()
        && !p.starts_with('$')
        && p.split('.')
            .all(|x| !x.is_empty() && !x.bytes().all(|b| b.is_ascii_digit()))
}

/// A literal as a sampled value: its class and SQL text.
fn value(v: &Bson) -> Option<SampleValue> {
    Some(match v {
        Bson::Int32(i) => number(&i.to_string()),
        Bson::Int64(i) => number(&i.to_string()),
        Bson::Double(f) if f.is_finite() => number(&format!("{f:?}")),
        Bson::Decimal128(d) => number(&d.to_string()).clone(),
        Bson::String(s) => quoted(LiteralKind::String, s),
        Bson::Boolean(b) => boolean(*b),
        Bson::ObjectId(o) => quoted(LiteralKind::Hex, &o.to_hex()),
        Bson::DateTime(t) => quoted(
            LiteralKind::Temporal,
            &sprig_doc::json::iso_from_ms(t.timestamp_millis())?,
        ),
        _ => return None,
    })
}

/// The filter's top-level conditions (and those of a top-level `$and`).
fn preds(f: &Document, m: &CollMeta, out: &mut Vec<MongoPred>) {
    for (k, v) in f {
        if k == "$and" {
            if let Bson::Array(a) = v {
                for x in a.iter().filter_map(Bson::as_document) {
                    preds(x, m, out);
                }
            }
            continue;
        }
        if !indexable_path(k) {
            continue;
        }
        let multikey = m.may_cross_array(k);
        let mut push = |cmp: PredOp, values: Vec<SampleValue>| {
            out.push(MongoPred {
                path: k.clone(),
                cmp,
                multikey,
                values,
            });
        };
        match v {
            Bson::Document(d) if d.keys().next().is_some_and(|x| x.starts_with('$')) => {
                for (op, arg) in d {
                    match (op.as_str(), arg) {
                        ("$eq", Bson::Null) => push(PredOp::IsNull, Vec::new()),
                        ("$eq", a) => {
                            if let Some(x) = value(a) {
                                push(PredOp::Eq, vec![x]);
                            }
                        }
                        ("$in", Bson::Array(a)) => {
                            let vs: Option<Vec<SampleValue>> = a.iter().map(value).collect();
                            if let Some(vs) = vs.filter(|x| !x.is_empty()) {
                                push(PredOp::In { list: true }, vs);
                            }
                        }
                        ("$gt" | "$gte" | "$lt" | "$lte", a) => {
                            if let Some(x) = value(a) {
                                let lo = op.starts_with("$g");
                                push(PredOp::Range { lo, hi: !lo }, vec![x]);
                            }
                        }
                        ("$ne", a) => {
                            if let Some(x) = value(a) {
                                push(PredOp::Ne, vec![x]);
                            }
                        }
                        _ => {}
                    }
                }
            }
            Bson::Null => push(PredOp::IsNull, Vec::new()),
            other => {
                if let Some(x) = value(other) {
                    push(PredOp::Eq, vec![x]);
                }
            }
        }
    }
}

/// The shape text: the `$queryStats` shape without its namespace and
/// command (they lead the fingerprint text).
fn shape_text(name: &str, body: &Document, db: &str, filter: &Document) -> String {
    let mut s = qshape::shape(name, body, db).unwrap_or_else(|| {
        // findAndModify isn't a `$queryStats` command.
        let mut d = doc! {"query": qshape::filter_shape(filter)};
        if let Ok(so) = body.get_document("sort") {
            d.insert("sort", so.clone());
        }
        if body.contains_key("update") {
            d.insert("update", "?");
        }
        if body.get_bool("remove").unwrap_or(false) {
            d.insert("remove", true);
        }
        d
    });
    s.remove("cmdNs");
    s.remove("command");
    Bson::Document(s).into_relaxed_extjson().to_string()
}

/// Fingerprints a command before it runs; `None` when it isn't captured
/// (not a captured command, admin, no tenant capture, no such collection).
pub async fn start(
    engine: &Engine,
    sql: &mut SqlSession,
    tenant: u64,
    name: &str,
    body: &Document,
    db: &str,
) -> Option<Started> {
    if tenant == 0 || db == "admin" || db == "config" || db == "local" || !captured(name) {
        return None;
    }
    let coll = body.get_str(name).ok()?.to_owned();
    let (filter, sort, limit) = parts(name, body)?;
    let ns = Ns {
        db: db.to_owned(),
        coll: coll.clone(),
    };
    let m = engine.meta(sql, &ns).await.ok()??;
    let cap = sql.workload(db, &m.table)?;
    let tenant = TenantId(tenant);
    let mut ps = Vec::new();
    preds(&filter, &m, &mut ps);
    let sort: Vec<(String, i32)> = sort
        .iter()
        .filter(|(p, _)| indexable_path(p))
        .map(|(p, v)| {
            (
                p.clone(),
                if sprig_doc::filter::as_i64(v).unwrap_or(1) < 0 {
                    -1
                } else {
                    1
                },
            )
        })
        .collect();
    let db_id = cap.table.as_ref().map_or(DbId(0), |t| t.db_id);
    let object_id = cap.table.as_ref().map(|t| t.object_id);
    let max_text_bytes = cap.workload.max_text_bytes();
    // A point find's fingerprint depends only on its key: reused
    // while the key stays the same; its values are this command's.
    let pk = point_key(
        name,
        body,
        &filter,
        &ps,
        db,
        &coll,
        &m.table,
        db_id,
        object_id,
        max_text_bytes,
    );
    let memo = pk.as_ref().and_then(|k| {
        sql.point_fp
            .as_ref()
            .filter(|(mk, _)| mk == k)
            .map(|(_, fp)| fp.clone())
    });
    let (fp, literals) = if let Some(fp) = memo {
        (fp, point_literals(&ps))
    } else {
        let text = shape_text(name, body, db, &filter);
        let r = sprig_workload::fingerprint::mongo::fingerprint(&MongoInput {
            cmd: name,
            db,
            db_id,
            coll: &coll,
            table: &m.table,
            object_id,
            shape_text: &text,
            preds: ps,
            sort,
            limit,
            max_text_bytes,
        });
        if let Some(k) = pk {
            sql.point_fp = Some((k, r.0.clone()));
        }
        r
    };
    let plan = cap.workload.front_plan(tenant, &fp);
    let run = Arc::new(FrontRun {
        probe: plan.examined && !sql.in_txn,
        ..FrontRun::default()
    });
    let session = cap.workload.session_hash(sql.session_id());
    Some(Started {
        cap,
        tenant,
        fp,
        literals,
        plan,
        run,
        at: Instant::now(),
        db: db.to_owned(),
        db_id,
        session,
    })
}

/// What a point find's fingerprint depends on: a `find` that the point-read
/// path answers (filter `{_id: <scalar>}`, no sort, projection, hint or
/// collation), so its shape is the `_id` value's type and which of `skip`,
/// `limit` and `singleBatch` it carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointKey {
    db: String,
    coll: String,
    table: String,
    db_id: DbId,
    object_id: Option<sprig_common::ids::ObjectId>,
    max_text_bytes: usize,
    id_type: u8,
    value_kind: Option<sprig_workload::fingerprint::LiteralKind>,
    skip: bool,
    limit: bool,
    single_batch: bool,
}

#[allow(clippy::too_many_arguments)]
fn point_key(
    name: &str,
    body: &Document,
    filter: &Document,
    ps: &[MongoPred],
    db: &str,
    coll: &str,
    table: &str,
    db_id: DbId,
    object_id: Option<sprig_common::ids::ObjectId>,
    max_text_bytes: usize,
) -> Option<PointKey> {
    if name != "find"
        || !body
            .keys()
            .all(|k| super::commands::POINT_FIND_KEYS.contains(&k.as_str()))
        || body.get_document("sort").is_ok_and(|d| !d.is_empty())
        || body.get_document("projection").is_ok_and(|d| !d.is_empty())
        || body.contains_key("sort") && body.get_document("sort").is_err()
        || body.contains_key("projection") && body.get_document("projection").is_err()
        || filter.len() != 1
    {
        return None;
    }
    let id = filter.get("_id")?;
    if matches!(id, Bson::Document(_) | Bson::Array(_)) {
        return None;
    }
    // One equality with its value, or none (a value without a sample).
    let value_kind = match ps {
        [] => None,
        [p] if matches!(p.cmp, PredOp::Eq) && p.values.len() == 1 => Some(p.values[0].kind),
        _ => return None,
    };
    Some(PointKey {
        db: db.to_owned(),
        coll: coll.to_owned(),
        table: table.to_owned(),
        db_id,
        object_id,
        max_text_bytes,
        id_type: id.element_type() as u8,
        value_kind,
        skip: body.contains_key("skip"),
        limit: body.contains_key("limit"),
        single_batch: body.get_bool("singleBatch").unwrap_or(false),
    })
}

/// A point find's sampled values, as the fingerprint gives them.
fn point_literals(ps: &[MongoPred]) -> SampledLiterals {
    SampledLiterals {
        slots: ps
            .iter()
            .map(|p| match p.values.first() {
                Some(v) => SampledSlot::One(v.clone()),
                None => SampledSlot::List(Vec::new()),
            })
            .collect(),
    }
}

/// Runs `fut` with the command's statements under its [`FrontRun`].
pub async fn scoped<F: std::future::Future>(st: Option<&Started>, fut: F) -> F::Output {
    match st {
        Some(s) => FRONT_RUN.scope(s.run(), fut).await,
        None => fut.await,
    }
}

fn returned(reply: &Document) -> u64 {
    let n = reply
        .get_document("cursor")
        .ok()
        .and_then(|c| {
            c.get_array("firstBatch")
                .or_else(|_| c.get_array("nextBatch"))
                .ok()
        })
        .map(Vec::len)
        .or_else(|| reply.get_array("values").ok().map(Vec::len));
    match n {
        Some(n) => u64::try_from(n).unwrap_or(u64::MAX),
        None => u64::from(
            reply.contains_key("value") && !reply.get("value").is_some_and(|v| *v == Bson::Null),
        ),
    }
}

fn affected(name: &str, reply: &Document) -> u64 {
    let g = |d: &Document, k: &str| {
        d.get(k)
            .and_then(sprig_doc::filter::as_i64)
            .and_then(|x| u64::try_from(x).ok())
            .unwrap_or(0)
    };
    match name {
        "update" => {
            g(reply, "nModified") + reply.get_array("upserted").map_or(0, |a| a.len() as u64)
        }
        "delete" => g(reply, "n"),
        "findAndModify" => reply
            .get_document("lastErrorObject")
            .map_or(0, |l| g(l, "n")),
        _ => 0,
    }
}

/// Records the command after it ran. `raw_n`: the documents of a reply
/// sent as raw bytes (a point read).
pub fn finish(
    st: &Started,
    r: &Result<Document, sprig_proto_mongo::errors::MongoError>,
    raw_n: Option<i64>,
    name: &str,
) {
    let latency_us = u64::try_from(st.at.elapsed().as_micros()).unwrap_or(u64::MAX);
    let (error, rows_returned, rows_affected) = match r {
        Ok(d) => (
            false,
            raw_n
                .and_then(|n| u64::try_from(n).ok())
                .unwrap_or_else(|| returned(d)),
            affected(name, d),
        ),
        Err(_) => (true, 0, 0),
    };
    let probed = st.run.probed.load(std::sync::atomic::Ordering::Relaxed);
    let ex = ExecSummary {
        latency_us,
        error,
        rows_returned,
        rows_affected,
        shards: 1,
        clusters: 1,
        scatter: false,
        in_list_max: st.fp.in_list_max,
        examined: (probed > 0).then(|| st.run.examined.load(std::sync::atomic::Ordering::Relaxed)),
        session: st.session,
    };
    let sample = (st.plan.capture_params && st.cap.samples).then(|| SamplePayload {
        literals: st.literals.clone(),
        context: SampleContext {
            database: Some(st.db.clone()),
            db_id: st.db_id,
            sql_mode: None,
            time_zone: None,
            charset: None,
            collation: None,
        },
    });
    st.cap.workload.front_record(st.tenant, &st.fp, &ex, sample);
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn meta(arrays: &[&str]) -> CollMeta {
        CollMeta {
            name: "c".into(),
            table: "c".into(),
            uuid: [0; 16],
            array_paths: arrays.iter().map(|s| (*s).to_owned()).collect(),
            arrays_full: false,
            pending_paths: std::collections::BTreeSet::new(),
            indexes: Vec::new(),
            options: Document::new(),
            next_index: 1,
        }
    }

    #[test]
    fn conditions_and_values() {
        let f = doc! {
            "status": "open",
            "qty": {"$gt": 5, "$lte": 9.5},
            "$and": [{"tags": {"$in": ["a", "b"]}}],
            "gone": null,
            "a.0": 1,
            "obj": {"x": 1},
        };
        let mut out = Vec::new();
        preds(&f, &meta(&["tags"]), &mut out);
        let got: Vec<(&str, &PredOp, bool, usize)> = out
            .iter()
            .map(|p| (p.path.as_str(), &p.cmp, p.multikey, p.values.len()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("status", &PredOp::Eq, false, 1),
                (
                    "qty",
                    &PredOp::Range {
                        lo: true,
                        hi: false
                    },
                    false,
                    1
                ),
                (
                    "qty",
                    &PredOp::Range {
                        lo: false,
                        hi: true
                    },
                    false,
                    1
                ),
                ("tags", &PredOp::In { list: true }, true, 2),
                ("gone", &PredOp::IsNull, false, 0),
            ]
        );
        assert_eq!(out[0].values[0].text, b"'open'".to_vec());
        assert_eq!(out[2].values[0].text, b"9.5".to_vec());
    }

    #[test]
    #[allow(clippy::many_single_char_names)]
    fn aggregate_parts() {
        let b = doc! {"aggregate": "c", "pipeline": [{"$match": {"a": 1}}, {"$sort": {"b": -1}}, {"$limit": 3}]};
        let (f, s, l) = parts("aggregate", &b).unwrap();
        assert_eq!(f, doc! {"a": 1});
        assert_eq!(s, doc! {"b": -1});
        assert!(l);
        let t = shape_text(
            "find",
            &doc! {"find": "c", "filter": {"a": 1}},
            "d",
            &doc! {},
        );
        assert_eq!(t, r#"{"filter":{"a":{"$eq":"?number"}}}"#);
    }

    /// Point finds with the same key get the same fingerprint when
    /// computed afresh (so reusing it is exact), and the key changes with
    /// anything their shape reads; other finds have no key.
    #[test]
    fn point_finds_share_a_fingerprint_by_key() {
        let m = meta(&[]);
        let fresh = |b: &Document| {
            let (f, _, limit) = parts("find", b).unwrap();
            let mut ps = Vec::new();
            preds(&f, &m, &mut ps);
            let key = point_key("find", b, &f, &ps, "d", "c", "c", DbId(7), None, 4096);
            let text = shape_text("find", b, "d", &f);
            let lits = point_literals(&ps);
            let (fp, sampled) = sprig_workload::fingerprint::mongo::fingerprint(&MongoInput {
                cmd: "find",
                db: "d",
                db_id: DbId(7),
                coll: "c",
                table: "c",
                object_id: None,
                shape_text: &text,
                preds: ps,
                sort: Vec::new(),
                limit,
                max_text_bytes: 4096,
            });
            assert_eq!(lits, sampled, "{b}");
            (key, fp)
        };
        let lsid = doc! {"id": bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: vec![1; 16] }};
        let (k1, f1) = fresh(
            &doc! {"find": "c", "filter": {"_id": "user1"}, "limit": 1, "singleBatch": true, "$db": "d", "lsid": lsid.clone()},
        );
        let (k2, f2) = fresh(
            &doc! {"find": "c", "filter": {"_id": "user99"}, "limit": 1, "singleBatch": true, "$db": "d"},
        );
        assert!(k1.is_some());
        assert_eq!(k1, k2);
        assert_eq!(f1.id, f2.id);
        assert_eq!(f1.text, f2.text);
        assert_eq!(f1.literals, f2.literals);
        assert_eq!(f1.list_slots, f2.list_slots);
        // Each of these changes the shape, and the key.
        for b in [
            doc! {"find": "c", "filter": {"_id": 5}, "limit": 1, "singleBatch": true},
            doc! {"find": "c", "filter": {"_id": "u"}, "singleBatch": true},
            doc! {"find": "c", "filter": {"_id": "u"}, "limit": 1},
            doc! {"find": "c", "filter": {"_id": "u"}, "limit": 1, "singleBatch": true, "skip": 0},
        ] {
            let (k, f) = fresh(&b);
            assert!(k.is_some() && k != k1, "{b}");
            assert!(f.id != f1.id || f.literals != f1.literals, "{b}");
        }
        // No key: not a point find the fast path answers.
        for b in [
            doc! {"find": "c", "filter": {"_id": {"$gt": 1}}},
            doc! {"find": "c", "filter": {"_id": 1, "a": 2}},
            doc! {"find": "c", "filter": {"_id": 1}, "sort": {"a": 1}},
            doc! {"find": "c", "filter": {"_id": 1}, "projection": {"a": 1}},
            doc! {"find": "c", "filter": {"_id": 1}, "hint": {"a": 1}},
            doc! {"find": "c", "filter": {"_id": 1}, "collation": {"locale": "fr"}},
            doc! {"find": "c", "filter": {"_id": [1]}},
            doc! {"find": "c", "filter": {"_id": null}},
        ] {
            assert!(fresh(&b).0.is_none(), "{b}");
        }
    }
}
