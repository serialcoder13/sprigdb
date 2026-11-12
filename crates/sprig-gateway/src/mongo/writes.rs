//! The write commands, statement by statement (chapter 30, "Dispatch" and
//! "Retryable writes, exactly once"): `insert`, `update`, `delete`,
//! `findAndModify` and MongoDB 8.0's `bulkWrite` over `sprig-doc`'s engine.
//!
//! Each runs the statements whose ids (positions in the command) are not
//! in `done` and reports, beside the reply, the result of every statement
//! that took effect. A plain command passes an empty `done`; a retryable
//! write passes the statements its record says already ran, so the reply
//! is built from their stored results and only the others run.

use bson::{Bson, Document, doc};
use sprig_doc::engine::{Engine, Ns, SqlRunner};
use sprig_doc::filter::as_i64;
use sprig_doc::sessions::StmtResults;
use sprig_proto_mongo::errors::{MongoError, code};

use super::commands::{coll_of, docs_of};

/// A write command's reply, and the statements that took effect now.
#[derive(Debug, Default)]
pub struct Outcome {
    /// The reply.
    pub reply: Document,
    /// Statement id to its result, for the statements run now.
    pub executed: StmtResults,
}

/// Codes not in the shared table.
const INVALID_LENGTH: i32 = 16;

fn idx(i: usize) -> i32 {
    i32::try_from(i).unwrap_or(i32::MAX)
}

fn n_of(d: &Document, k: &str) -> u64 {
    d.get(k)
        .and_then(as_i64)
        .and_then(|n| u64::try_from(n).ok())
        .unwrap_or(0)
}

fn count(n: u64) -> i32 {
    i32::try_from(n).unwrap_or(i32::MAX)
}

/// Whether `name` is a write command this module runs.
#[must_use]
pub fn is_write(name: &str) -> bool {
    matches!(
        name,
        "insert" | "update" | "delete" | "findAndModify" | "findandmodify" | "bulkWrite"
    )
}

/// `bulkWrite`'s namespaces, by `nsInfo` index.
///
/// # Errors
/// 73 for a namespace without a database and a collection.
pub fn namespaces(body: &Document) -> Result<Vec<Ns>, MongoError> {
    docs_of(body, "nsInfo")
        .iter()
        .map(|d| {
            let ns = d.get_str("ns").unwrap_or_default();
            match ns.split_once('.') {
                Some((db, coll)) if !db.is_empty() && !coll.is_empty() => Ok(Ns {
                    db: db.to_owned(),
                    coll: coll.to_owned(),
                }),
                _ => Err(MongoError::new(
                    code::INVALID_NAMESPACE,
                    format!("Invalid namespace specified '{ns}'"),
                )),
            }
        })
        .collect()
}

/// The databases a write command writes: the request's, or `bulkWrite`'s
/// (by name, once each).
#[must_use]
pub fn target_dbs(name: &str, body: &Document, db: &str) -> Vec<String> {
    match name {
        "bulkWrite" => namespaces(body)
            .unwrap_or_default()
            .into_iter()
            .map(|n| n.db)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect(),
        n if is_write(n) => vec![db.to_owned()],
        _ => Vec::new(),
    }
}

/// The database whose records table holds a retryable write's record:
/// the request's, or for `bulkWrite` (sent to `admin`) the first
/// namespace's.
#[must_use]
pub fn record_db(name: &str, body: &Document, db: &str) -> String {
    if name == "bulkWrite"
        && let Some(n) = namespaces(body).ok().and_then(|v| v.into_iter().next())
    {
        return n.db;
    }
    db.to_owned()
}

/// The collections a command's statements may create (inserts and
/// upserts), which must exist before a transaction starts.
#[must_use]
pub fn created_namespaces(name: &str, body: &Document, db: &str) -> Vec<Ns> {
    if name == "bulkWrite" {
        let Ok(nss) = namespaces(body) else {
            return Vec::new();
        };
        let mut out: Vec<Ns> = Vec::new();
        for op in docs_of(body, "ops") {
            let idx = if let Some(i) = op.get("insert").and_then(as_i64) {
                Some(i)
            } else if op.get_bool("upsert").unwrap_or(false) {
                op.get("update").and_then(as_i64)
            } else {
                None
            };
            if let Some(ns) = idx
                .and_then(|i| usize::try_from(i).ok())
                .and_then(|i| nss.get(i))
                && !out.contains(ns)
            {
                out.push(ns.clone());
            }
        }
        return out;
    }
    if creates(name, body)
        && let Ok(coll) = coll_of(body, name)
    {
        return vec![Ns {
            db: db.to_owned(),
            coll,
        }];
    }
    Vec::new()
}

/// Runs write command `name` on `db`, skipping the statements in `done`.
///
/// # Errors
/// Errors that stop the whole command; statement errors are in the
/// reply's `writeErrors`.
pub async fn run(
    engine: &Engine,
    sql: &mut dyn SqlRunner,
    name: &str,
    body: &Document,
    db: &str,
    done: &StmtResults,
) -> Result<Outcome, MongoError> {
    // `bypassDocumentValidation` (M3, decision S90): the engine skips the
    // collections' validators for this command's writes.
    let bypass = body.get_bool("bypassDocumentValidation").unwrap_or(false);
    sprig_doc::validate::bypass(
        bypass,
        Box::pin(run_writes(engine, sql, name, body, db, done)),
    )
    .await
}

async fn run_writes(
    engine: &Engine,
    sql: &mut dyn SqlRunner,
    name: &str,
    body: &Document,
    db: &str,
    done: &StmtResults,
) -> Result<Outcome, MongoError> {
    if name == "bulkWrite" {
        return bulk_write(engine, sql, body, done).await;
    }
    let ns = Ns {
        db: db.to_owned(),
        coll: coll_of(body, name)?,
    };
    match name {
        "insert" => insert(engine, sql, &ns, body, done).await,
        "update" => update(engine, sql, &ns, body, done).await,
        "delete" => delete(engine, sql, &ns, body, done).await,
        _ => find_and_modify(engine, sql, &ns, body, done).await,
    }
}

async fn insert(
    engine: &Engine,
    sql: &mut dyn SqlRunner,
    ns: &Ns,
    body: &Document,
    done: &StmtResults,
) -> Result<Outcome, MongoError> {
    let docs = docs_of(body, "documents");
    if docs.is_empty() {
        return Err(MongoError::new(
            INVALID_LENGTH,
            "Write batch sizes must be between 1 and 100000. Got 0 operations.",
        ));
    }
    let ordered = body.get_bool("ordered").unwrap_or(true);
    let total = docs.len();
    let (todo, rest): (Vec<usize>, Vec<Document>) = docs
        .into_iter()
        .enumerate()
        .filter(|(i, _)| !done.contains_key(&idx(*i)))
        .unzip();
    let already = total - todo.len();
    let errs = if rest.is_empty() {
        Vec::new()
    } else {
        engine.insert(sql, ns, rest, ordered).await?.1
    };
    // Which statements took effect: an ordered batch stops at its first
    // error; an unordered one skips only the failed statements.
    let failed: Vec<usize> = errs.iter().map(|(k, _)| *k).collect();
    let stop = if ordered {
        failed.iter().min().copied().unwrap_or(todo.len())
    } else {
        todo.len()
    };
    let mut out = Outcome::default();
    for (k, i) in todo.iter().enumerate().take(stop) {
        if !failed.contains(&k) {
            out.executed.insert(idx(*i), doc! {"n": 1});
        }
    }
    let n = already + out.executed.len();
    let mut d = doc! {"n": count(n as u64)};
    if !errs.is_empty() {
        d.insert(
            "writeErrors",
            errs.iter()
                .map(|(k, e)| Bson::Document(e.write_error(todo[*k])))
                .collect::<Vec<_>>(),
        );
    }
    d.insert("ok", 1.0);
    out.reply = d;
    Ok(out)
}

async fn update(
    engine: &Engine,
    sql: &mut dyn SqlRunner,
    ns: &Ns,
    body: &Document,
    done: &StmtResults,
) -> Result<Outcome, MongoError> {
    let ordered = body.get_bool("ordered").unwrap_or(true);
    let mut n = 0u64;
    let mut modified = 0u64;
    let mut upserted = Vec::new();
    let mut errors = Vec::new();
    let mut out = Outcome::default();
    for (i, u) in docs_of(body, "updates").iter().enumerate() {
        let r = if let Some(r) = done.get(&idx(i)) {
            r.clone()
        } else {
            let q = u.get_document("q").cloned().unwrap_or_default();
            let Some(spec) = u.get("u") else {
                errors.push(Bson::Document(
                    MongoError::new(
                        code::FAILED_TO_PARSE,
                        "BSON field 'update.updates.u' is missing but a required field",
                    )
                    .write_error(i),
                ));
                break;
            };
            let afs = docs_of(u, "arrayFilters");
            // Per statement: its collation, and MongoDB 8.0's `sort` for
            // an update of one document.
            let r = match sprig_doc::collation::Collation::of(u) {
                Ok(coll) => {
                    sprig_doc::collation::scope(
                        coll,
                        engine.update(
                            sql,
                            ns,
                            &q,
                            spec,
                            u.get_bool("multi").unwrap_or(false),
                            u.get_bool("upsert").unwrap_or(false),
                            &afs,
                            u.get_document("sort").ok(),
                        ),
                    )
                    .await
                }
                Err(e) => Err(e),
            };
            match r {
                Ok(o) => {
                    let mut r = doc! {
                        "n": i64::try_from(o.matched).unwrap_or(i64::MAX),
                        "m": i64::try_from(o.modified).unwrap_or(i64::MAX),
                    };
                    if let Some(id) = o.upserted {
                        r.insert("u", id);
                    }
                    out.executed.insert(idx(i), r.clone());
                    r
                }
                Err(e) => {
                    errors.push(Bson::Document(e.write_error(i)));
                    if ordered {
                        break;
                    }
                    continue;
                }
            }
        };
        if let Some(id) = r.get("u") {
            n += 1;
            upserted.push(Bson::Document(doc! {"index": idx(i), "_id": id.clone()}));
        } else {
            n += n_of(&r, "n");
            modified += n_of(&r, "m");
        }
    }
    let mut d = doc! {"n": count(n), "nModified": count(modified)};
    if !upserted.is_empty() {
        d.insert("upserted", upserted);
    }
    if !errors.is_empty() {
        d.insert("writeErrors", errors);
    }
    d.insert("ok", 1.0);
    out.reply = d;
    Ok(out)
}

async fn delete(
    engine: &Engine,
    sql: &mut dyn SqlRunner,
    ns: &Ns,
    body: &Document,
    done: &StmtResults,
) -> Result<Outcome, MongoError> {
    let ordered = body.get_bool("ordered").unwrap_or(true);
    let mut n = 0u64;
    let mut errors = Vec::new();
    let mut out = Outcome::default();
    for (i, d) in docs_of(body, "deletes").iter().enumerate() {
        if let Some(r) = done.get(&idx(i)) {
            n += n_of(r, "n");
            continue;
        }
        let q = d.get_document("q").cloned().unwrap_or_default();
        let one = d.get("limit").and_then(as_i64) == Some(1);
        let r = match sprig_doc::collation::Collation::of(d) {
            Ok(coll) => {
                sprig_doc::collation::scope(coll, engine.delete(sql, ns, &q, one, None)).await
            }
            Err(e) => Err(e),
        };
        match r {
            Ok((k, _)) => {
                n += k;
                out.executed
                    .insert(idx(i), doc! {"n": i64::try_from(k).unwrap_or(i64::MAX)});
            }
            Err(e) => {
                errors.push(Bson::Document(e.write_error(i)));
                if ordered {
                    break;
                }
            }
        }
    }
    let mut d = doc! {"n": count(n)};
    if !errors.is_empty() {
        d.insert("writeErrors", errors);
    }
    d.insert("ok", 1.0);
    out.reply = d;
    Ok(out)
}

async fn find_and_modify(
    engine: &Engine,
    sql: &mut dyn SqlRunner,
    ns: &Ns,
    body: &Document,
    done: &StmtResults,
) -> Result<Outcome, MongoError> {
    // One statement, whose stored result is the whole reply (with the
    // document image it returned).
    if let Some(r) = done.get(&0) {
        return Ok(Outcome {
            reply: r.clone(),
            executed: StmtResults::new(),
        });
    }
    let q = body.get_document("query").cloned().unwrap_or_default();
    let sort = body.get_document("sort").ok();
    let fields = body
        .get_document("fields")
        .ok()
        .filter(|f| !f.is_empty())
        .map(sprig_doc::agg::Projection::parse)
        .transpose()?;
    let project = |d: Option<Document>| -> Result<Bson, MongoError> {
        Ok(match (d, &fields) {
            (Some(d), Some(p)) => Bson::Document(p.apply(&d, bson::DateTime::now())?),
            (Some(d), None) => Bson::Document(d),
            (None, _) => Bson::Null,
        })
    };
    let reply = if body.get_bool("remove").unwrap_or(false) {
        let (n, old) = engine.delete(sql, ns, &q, true, sort).await?;
        doc! {"lastErrorObject": {"n": count(n)}, "value": project(old)?, "ok": 1.0}
    } else {
        let u = body.get("update").ok_or_else(|| {
            MongoError::new(
                code::FAILED_TO_PARSE,
                "Either an update or remove=true must be specified",
            )
        })?;
        let upsert = body.get_bool("upsert").unwrap_or(false);
        let o = engine
            .update(
                sql,
                ns,
                &q,
                u,
                false,
                upsert,
                &docs_of(body, "arrayFilters"),
                sort,
            )
            .await
            .map_err(|e| {
                // MongoDB 8.0's wording for a findAndModify refused by the
                // collection's validator.
                if e.code == sprig_doc::validate::DOCUMENT_VALIDATION_FAILURE {
                    MongoError {
                        message: format!(
                            "Plan executor error during findAndModify :: caused by :: {}",
                            e.message
                        ),
                        ..e
                    }
                } else {
                    e
                }
            })?;
        let new = body.get_bool("new").unwrap_or(false);
        let mut leo = doc! {"n": i32::from(o.matched > 0 || o.upserted.is_some()), "updatedExisting": o.matched > 0};
        if let Some(id) = &o.upserted {
            leo.insert("upserted", id.clone());
        }
        let value = if o.upserted.is_some() {
            if new { o.last_new } else { None }
        } else if new {
            o.last_new
        } else {
            o.last_old
        };
        doc! {"lastErrorObject": leo, "value": project(value)?, "ok": 1.0}
    };
    let mut executed = StmtResults::new();
    executed.insert(0, reply.clone());
    Ok(Outcome { reply, executed })
}

/// One `bulkWrite` result: `ok: 0`, its `idx`, the error's fields, and
/// zero counts, as MongoDB 8.0 sends them.
fn bulk_error(e: &MongoError, i: usize, update: bool) -> Document {
    let mut d = doc! {"ok": 0.0, "idx": idx(i), "code": e.code, "errmsg": e.message.clone()};
    if let Some(x) = &e.extra {
        for (k, v) in x.iter() {
            d.insert(k.clone(), v.clone());
        }
    }
    d.insert("n", 0);
    if update {
        d.insert("nModified", 0);
    }
    d
}

#[allow(clippy::too_many_lines)]
/// MongoDB 8.0's `bulkWrite` (chapter 30, "Dispatch"): `ops` over the
/// collections of `nsInfo`, one statement per op (its index is its
/// statement id), with a cursor of per-op results and the totals.
async fn bulk_write(
    engine: &Engine,
    sql: &mut dyn SqlRunner,
    body: &Document,
    done: &StmtResults,
) -> Result<Outcome, MongoError> {
    let nss = namespaces(body)?;
    let ops = docs_of(body, "ops");
    if ops.is_empty() || ops.len() > 100_000 {
        return Err(MongoError::new(
            INVALID_LENGTH,
            format!(
                "Write batch sizes must be between 1 and 100000. Got {} operations.",
                ops.len()
            ),
        ));
    }
    // Every op names a namespace and a kind before anything runs.
    let mut parsed = Vec::with_capacity(ops.len());
    for op in &ops {
        let (kind, ix) = match op.iter().next() {
            Some((k, v)) if matches!(k.as_str(), "insert" | "update" | "delete") => {
                (k.clone(), as_i64(v))
            }
            _ => {
                return Err(MongoError::new(
                    code::FAILED_TO_PARSE,
                    format!("Unrecognized bulkWrite op {op}"),
                ));
            }
        };
        let Some(ns) = ix
            .and_then(|i| usize::try_from(i).ok())
            .and_then(|i| nss.get(i))
        else {
            return Err(MongoError::new(
                code::BAD_VALUE,
                format!("BulkWrite ops entry {op} has an invalid nsInfo index."),
            ));
        };
        parsed.push((kind, ns.clone()));
    }
    let ordered = body.get_bool("ordered").unwrap_or(true);
    let errors_only = body.get_bool("errorsOnly").unwrap_or(false);
    let (mut ins, mut matched, mut modified, mut ups, mut del, mut errs) =
        (0i64, 0i64, 0i64, 0i64, 0i64, 0i32);
    let mut out = Outcome::default();
    let mut batch = Vec::new();
    for (i, (op, (kind, ns))) in ops.iter().zip(parsed).enumerate() {
        let r: Result<Document, MongoError> = if let Some(r) = done.get(&idx(i)) {
            Ok(r.clone())
        } else {
            let r = match kind.as_str() {
                "insert" => {
                    let d = op.get_document("document").cloned().unwrap_or_default();
                    match engine.insert(sql, &ns, vec![d], true).await {
                        Ok((_, e)) if !e.is_empty() => Err(e[0].1.clone()),
                        Ok(_) => Ok(doc! {"ok": 1.0, "idx": idx(i), "n": 1}),
                        Err(e) => Err(e),
                    }
                }
                "update" => {
                    let q = op.get_document("filter").cloned().unwrap_or_default();
                    match op.get("updateMods") {
                        None => Err(MongoError::new(
                            code::FAILED_TO_PARSE,
                            "BSON field 'bulkWrite.ops.updateMods' is missing but a required field",
                        )),
                        Some(spec) => match sprig_doc::collation::Collation::of(op) {
                            Ok(coll) => {
                                sprig_doc::collation::scope(
                                    coll,
                                    engine.update(
                                        sql,
                                        &ns,
                                        &q,
                                        spec,
                                        op.get_bool("multi").unwrap_or(false),
                                        op.get_bool("upsert").unwrap_or(false),
                                        &docs_of(op, "arrayFilters"),
                                        op.get_document("sort").ok(),
                                    ),
                                )
                                .await
                            }
                            Err(e) => Err(e),
                        }
                        .map(|o| {
                            let mut d = doc! {
                                "ok": 1.0,
                                "idx": idx(i),
                                "n": i32::try_from(o.matched).unwrap_or(i32::MAX)
                                    + i32::from(o.upserted.is_some()),
                                "nModified": i32::try_from(o.modified).unwrap_or(i32::MAX),
                            };
                            if let Some(id) = o.upserted {
                                d.insert("upserted", doc! {"_id": id});
                            }
                            d
                        }),
                    }
                }
                _ => {
                    let q = op.get_document("filter").cloned().unwrap_or_default();
                    let multi = op.get_bool("multi").unwrap_or(false);
                    match sprig_doc::collation::Collation::of(op) {
                        Ok(coll) => {
                            sprig_doc::collation::scope(
                                coll,
                                engine.delete(sql, &ns, &q, !multi, None),
                            )
                            .await
                        }
                        Err(e) => Err(e),
                    }
                    .map(|(n, _)| doc! {"ok": 1.0, "idx": idx(i), "n": i32::try_from(n).unwrap_or(i32::MAX)})
                }
            };
            if let Ok(d) = &r {
                out.executed.insert(idx(i), d.clone());
            }
            r
        };
        match r {
            Ok(d) => {
                let n = d.get("n").and_then(as_i64).unwrap_or(0);
                match kind.as_str() {
                    "insert" => ins += n,
                    "update" if d.contains_key("upserted") => ups += 1,
                    "update" => {
                        matched += n;
                        modified += d.get("nModified").and_then(as_i64).unwrap_or(0);
                    }
                    _ => del += n,
                }
                if !errors_only {
                    batch.push(Bson::Document(d));
                }
            }
            Err(e) => {
                errs += 1;
                batch.push(Bson::Document(bulk_error(&e, i, kind == "update")));
                if ordered {
                    break;
                }
            }
        }
    }
    let c = |n: i64| i32::try_from(n).unwrap_or(i32::MAX);
    out.reply = doc! {
        "cursor": {"id": 0i64, "firstBatch": batch, "ns": "admin.$cmd.bulkWrite"},
        "nErrors": errs,
        "nInserted": c(ins),
        "nMatched": c(matched),
        "nModified": c(modified),
        "nUpserted": c(ups),
        "nDeleted": c(del),
        "ok": 1.0,
    };
    Ok(out)
}

/// Whether the command's statements may create its collection (an insert
/// or an upsert), so it must exist before a transaction starts (MariaDB
/// DDL commits implicitly).
#[must_use]
pub fn creates(name: &str, body: &Document) -> bool {
    match name {
        "insert" => true,
        "update" => docs_of(body, "updates")
            .iter()
            .any(|u| u.get_bool("upsert").unwrap_or(false)),
        "findAndModify" | "findandmodify" => {
            body.get_bool("upsert").unwrap_or(false) && !body.get_bool("remove").unwrap_or(false)
        }
        _ => false,
    }
}
