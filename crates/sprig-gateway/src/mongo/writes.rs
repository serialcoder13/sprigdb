//! The write commands, statement by statement (chapter 30, "Dispatch" and
//! "Retryable writes, exactly once"): `insert`, `update`, `delete` and
//! `findAndModify` over `sprig-doc`'s engine.
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
        "insert" | "update" | "delete" | "findAndModify" | "findandmodify"
    )
}

/// Runs write command `name` on `db`, skipping the statements in `done`.
///
/// # Errors
/// Errors that stop the whole command; statement errors are in the
/// reply's `writeErrors`.
pub async fn run(
    engine: Engine,
    sql: &mut dyn SqlRunner,
    name: &str,
    body: &Document,
    db: &str,
    done: &StmtResults,
) -> Result<Outcome, MongoError> {
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
    engine: Engine,
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
    engine: Engine,
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
            match engine
                .update(
                    sql,
                    ns,
                    &q,
                    spec,
                    u.get_bool("multi").unwrap_or(false),
                    u.get_bool("upsert").unwrap_or(false),
                    &afs,
                    None,
                )
                .await
            {
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
    engine: Engine,
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
        match engine.delete(sql, ns, &q, one, None).await {
            Ok((k, _)) => {
                n += k;
                out.executed.insert(
                    idx(i),
                    doc! {"n": i64::try_from(k).unwrap_or(i64::MAX)},
                );
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
    engine: Engine,
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
            .await?;
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
