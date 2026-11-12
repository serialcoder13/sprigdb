//! MongoDB's `explain` for `find`, `aggregate`, `count`, `distinct`,
//! `update` and `delete` (chapter 31, "Interfaces"; R5 round 8, after
//! decision S25 covered the searches).
//!
//! The command runs through the engine as usual, on a [`Recorder`] around
//! the caller's session: every statement the engine sends to the
//! collection's table is noted, and nothing is written. Reads of the table
//! run for `executionStats` and `allPlansExecution` (so the counts are
//! real) and are answered with no rows for `queryPlanner`, which then shows
//! the first statement of each step without reading. Writes to the table
//! (an `UPDATE`, a `DELETE`) are noted and not sent; an update is planned as
//! the read that finds its documents plus the write the engine would send
//! for each. Each noted statement then goes to MariaDB as `EXPLAIN
//! FORMAT=JSON` through the same session, so chapter 29's privileges and
//! row policies apply: a caller a row policy binds gets MariaDB's plan
//! refused (RL26), as SQL `EXPLAIN` is, and sees the refusal in its place.
//!
//! The reply has MongoDB's shape. `queryPlanner.winningPlan` is a stage tree
//! made from MariaDB's plan of the main read (`EXPRESS_IXSCAN` for a point
//! read by `_id`, `IXSCAN` under `FETCH` for an index, `COLLSCAN` for a
//! scan, `EOF` when there is no collection), with `SORT`, `SKIP`, `LIMIT`,
//! `PROJECTION_DEFAULT`, `COUNT`, `UPDATE` or `DELETE` above it.
//! `queryPlanner.sprig` is SprigDB's own plan: the statements, MariaDB's
//! plan of each, and what the gateway does after MariaDB. `aggregate`
//! answers in MongoDB's `stages` form: a `$cursor` stage with the plan of
//! the collection's read, then the pipeline's other stages, which run in the
//! gateway.

use std::time::Instant;

use async_trait::async_trait;
use bson::{Bson, Document, doc};
use sprig_doc::engine::{Done, Engine, FindSpec, Ns, Param, Rows, SqlError, SqlRunner, inline};
use sprig_doc::filter::as_i64;
use sprig_doc::meta::CollMeta;
use sprig_doc::sql::ident;
use sprig_proto_mongo::errors::MongoError;

use super::sql::SqlSession;

/// At most this many distinct statements are explained by MariaDB.
const MAX_STATEMENTS: usize = 8;

/// Documents an executed `find` or update reads at most for
/// `executionStats`.
const MAX_EXECUTED_DOCS: usize = 1_000_000;

/// Largest batch the explained `find` reads at a time.
const BATCH: usize = 1000;

/// A statement the engine sent to the collection's table.
#[derive(Debug, Clone)]
struct Noted {
    sql: String,
    /// The rows it returned, when it ran.
    rows: Option<usize>,
    /// A write, not sent.
    write: bool,
}

/// The session the engine runs on while a command is explained.
struct Recorder<'a> {
    inner: &'a mut SqlSession,
    /// `` `db`.`table` ``: the collection's table.
    target: String,
    /// Reads of the table run (`executionStats`).
    run_reads: bool,
    noted: Vec<Noted>,
}

/// Whether `sql` only reads: the engine's reads start with `SELECT`.
fn is_read(sql: &str) -> bool {
    let t = sql.trim_start();
    t.get(..6).is_some_and(|h| h.eq_ignore_ascii_case("SELECT"))
        || t.get(..4).is_some_and(|h| h.eq_ignore_ascii_case("WITH"))
}

/// Whether `sql` names `target` (`` `db`.`table` ``) itself; side tables
/// are other names.
fn names(sql: &str, target: &str) -> bool {
    sql.match_indices(target).any(|(i, _)| {
        !sql[i + target.len()..]
            .chars()
            .next()
            .is_some_and(|c| c.is_alphanumeric() || c == '_')
    })
}

#[async_trait]
impl SqlRunner for Recorder<'_> {
    async fn query(&mut self, sql: &str) -> Result<Rows, SqlError> {
        if !names(sql, &self.target) {
            return self.inner.query(sql).await;
        }
        let rows = if self.run_reads && is_read(sql) {
            Some(self.inner.query(sql).await?)
        } else {
            None
        };
        self.noted.push(Noted {
            sql: sql.to_owned(),
            rows: rows.as_ref().map(Vec::len),
            write: !is_read(sql),
        });
        Ok(rows.unwrap_or_default())
    }

    async fn exec(&mut self, sql: &str) -> Result<Done, SqlError> {
        if !names(sql, &self.target) {
            return self.inner.exec(sql).await;
        }
        if is_read(sql) {
            let rows = self.query(sql).await?;
            return Ok(Done {
                affected: rows.len() as u64,
                info: String::new(),
            });
        }
        // A write to the collection: noted, not sent. The engine is told it
        // changed one row, so it goes no further.
        self.noted.push(Noted {
            sql: sql.to_owned(),
            rows: None,
            write: true,
        });
        Ok(Done {
            affected: 1,
            info: "Rows matched: 1  Changed: 1  Warnings: 0".to_owned(),
        })
    }

    fn tenant(&self) -> u64 {
        self.inner.tenant()
    }

    fn principal_hint(&self) -> Option<(String, u32)> {
        self.inner.principal_hint()
    }

    fn row_policy_binds(&self, object: u64) -> Option<bool> {
        self.inner.row_policy_binds(object)
    }

    async fn query_bound(&mut self, sql: &str, params: &[Param]) -> Result<Rows, SqlError> {
        self.query(&inline(sql, params)).await
    }

    async fn exec_bound(&mut self, sql: &str, params: &[Param]) -> Result<Done, SqlError> {
        self.exec(&inline(sql, params)).await
    }
}

/// What the explained command did.
#[derive(Default)]
struct Ran {
    /// Documents returned (or matched, for writes).
    n: u64,
    /// Steps after MariaDB, in the gateway.
    gateway: Vec<String>,
    /// The filter shown as `parsedQuery`.
    parsed: Document,
    /// Stages above the read: outermost last.
    above: Vec<Document>,
    /// The writes the engine would send but didn't plan yet (an update's,
    /// a single delete's).
    planned: Vec<String>,
    /// Extra `executionStats` fields (`nMatched`, `nWouldDelete`).
    stats: Document,
}

/// The explain of `cmd` (the document under `explain`) in `db`; `None` for
/// commands other than the six this module explains, and for `aggregate`
/// on a database.
///
/// # Errors
/// The command's own errors (parsing, privileges on reads that ran).
#[allow(clippy::too_many_lines)] // one pass building the explain document, verbosity by verbosity
pub(super) async fn explain(
    engine: &Engine,
    sql: &mut SqlSession,
    cmd: &Document,
    db: &str,
    verbosity: &str,
) -> Result<Option<Document>, MongoError> {
    let Some((name, target)) = cmd.iter().next() else {
        return Ok(None);
    };
    let name = name.as_str();
    if !matches!(
        name,
        "find" | "aggregate" | "count" | "distinct" | "update" | "delete"
    ) {
        return Ok(None);
    }
    let Bson::String(coll) = target else {
        return Ok(None);
    };
    let ns = Ns {
        db: db.to_owned(),
        coll: coll.clone(),
    };
    let execute = verbosity != "queryPlanner";
    let started = Instant::now();
    let Some(m) = engine.meta(sql, &ns).await? else {
        return Ok(Some(eof(name, &ns, cmd, execute)));
    };
    let mut rec = Recorder {
        inner: sql,
        target: format!("{}.{}", ident(db), ident(&m.table)),
        run_reads: execute,
        noted: Vec::new(),
    };
    let ran = run(engine, &mut rec, name, &ns, &m, cmd, execute).await?;
    let noted = std::mem::take(&mut rec.noted);
    drop(rec);
    let mut ran = ran;
    // Matches of a write, counted without the write.
    if execute {
        match name {
            "update" => {
                ran.stats.insert("nMatched", to_i64(ran.n));
            }
            "delete" => {
                let d = first_of(cmd, "deletes");
                let q = d.get_document("q").cloned().unwrap_or_default();
                let one = d.get("limit").and_then(as_i64) == Some(1);
                let n = engine.count(sql, &ns, &q, 0, u64::from(one)).await?;
                ran.n = n;
                ran.stats.insert("nWouldDelete", to_i64(n));
            }
            _ => {}
        }
    }
    // The statements MariaDB explains: the noted ones, then the planned
    // writes, each text once.
    let mut texts: Vec<(String, bool, Option<usize>)> = Vec::new();
    for n in &noted {
        if !texts.iter().any(|(t, _, _)| *t == n.sql) {
            texts.push((n.sql.clone(), n.write, n.rows));
        }
    }
    for p in &ran.planned {
        if !texts.iter().any(|(t, _, _)| t == p) {
            texts.push((p.clone(), true, None));
        }
    }
    let total = texts.len();
    texts.truncate(MAX_STATEMENTS);
    let tenant = sql.tenant();
    let mut statements = Vec::new();
    let mut main: Option<serde_json::Value> = None;
    for (text, write, _) in &texts {
        let plan = match sql.query(&format!("EXPLAIN FORMAT=JSON {text}")).await {
            Ok(rows) => {
                let j = rows
                    .first()
                    .and_then(|r| r.first())
                    .and_then(Option::as_ref)
                    .map(|b| scrub(&String::from_utf8_lossy(b), tenant, db))
                    .unwrap_or_default();
                let parsed = serde_json::from_str::<serde_json::Value>(&j)
                    .or_else(|_| serde_json::from_str::<serde_json::Value>(&repair_json(&j)));
                match parsed {
                    Ok(v) => {
                        if main.is_none() && !write {
                            main = Some(v.clone());
                        }
                        Bson::Document(match json_bson(v) {
                            Bson::Document(d) => d,
                            other => doc! {"plan": other},
                        })
                    }
                    Err(_) => Bson::String(j),
                }
            }
            Err(e) => Bson::Document(doc! {"refused": scrub(&e.message, tenant, db)}),
        };
        statements.push(doc! {
            "sql": scrub(text, tenant, db),
            "kind": if *write { "write (not sent)" } else { "read" },
            "mariadb": plan,
        });
    }
    let filter_doc = ran.parsed.clone();
    let mut plan = leaf(main.as_ref(), &m, &filter_doc);
    for s in &ran.above {
        let mut s = s.clone();
        s.insert("inputStage", plan);
        plan = s;
    }
    let examined: usize = noted.iter().filter_map(|n| n.rows).sum();
    let mut sprig = doc! {
        "statements": statements,
        "gateway": ran.gateway.clone(),
    };
    if total > MAX_STATEMENTS {
        sprig.insert(
            "statementsNotShown",
            to_i64((total - MAX_STATEMENTS) as u64),
        );
    }
    if !execute {
        sprig.insert(
            "note",
            "queryPlanner: reads of the collection were not run; each step's first statement is shown",
        );
    }
    let planner = doc! {
        "namespace": ns.to_string(),
        "parsedQuery": filter_doc,
        "winningPlan": plan,
        "rejectedPlans": [],
        "sprig": sprig,
    };
    let stats = execute.then(|| {
        let mut s = doc! {
            "executionSuccess": true,
            "nReturned": if matches!(name, "update" | "delete") { 0 } else { to_i64(ran.n) },
            "executionTimeMillis": to_i64(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)),
            "totalKeysExamined": 0i64,
            "totalDocsExamined": to_i64(examined as u64),
            "sprigStatements": to_i64(noted.len() as u64),
        };
        s.extend(ran.stats.clone());
        s
    });
    let mut out = doc! {"explainVersion": "1"};
    if name == "aggregate" {
        let mut cursor = doc! {"queryPlanner": planner};
        if let Some(s) = stats {
            cursor.insert("executionStats", s);
        }
        let mut stages = vec![Bson::Document(doc! {"$cursor": cursor})];
        stages.extend(pipeline_rest(cmd).into_iter().map(Bson::Document));
        out.insert("stages", stages);
    } else {
        out.insert("queryPlanner", planner);
        if let Some(s) = stats {
            out.insert("executionStats", s);
        }
    }
    out.insert("command", cmd.clone());
    out.insert("ok", 1.0);
    Ok(Some(out))
}

/// Runs `name` through the engine on `rec`.
#[allow(clippy::too_many_lines)] // one arm per explained command
async fn run(
    engine: &Engine,
    rec: &mut Recorder<'_>,
    name: &str,
    ns: &Ns,
    m: &CollMeta,
    cmd: &Document,
    execute: bool,
) -> Result<Ran, MongoError> {
    let mut ran = Ran::default();
    let skip_of = |d: &Document| u64::try_from(as_i64_or(d, "skip").max(0)).unwrap_or(0);
    match name {
        "find" => {
            let filter = cmd.get_document("filter").cloned().unwrap_or_default();
            let limit = as_i64_or(cmd, "limit");
            let sort = cmd
                .get_document("sort")
                .ok()
                .cloned()
                .filter(|s| !s.is_empty());
            let projection = cmd
                .get_document("projection")
                .ok()
                .cloned()
                .filter(|p| !p.is_empty());
            let skip = skip_of(cmd);
            ran.n = read_all(
                engine,
                rec,
                ns,
                FindSpec {
                    filter: filter.clone(),
                    sort: sort.clone(),
                    projection: projection.clone(),
                    skip,
                    limit: limit.unsigned_abs(),
                },
                execute,
            )
            .await?;
            ran.gateway
                .push("recheck each document against the filter (decision D4)".into());
            if let Some(s) = &sort
                && !id_sort(s)
            {
                ran.above.push(doc! {"stage": "SORT", "sortPattern": s.clone(), "memLimit": 0i64, "type": "simple"});
                ran.gateway.push("sort in the gateway".into());
            }
            if skip > 0 {
                ran.above
                    .push(doc! {"stage": "SKIP", "skipAmount": to_i64(skip)});
            }
            if limit != 0 {
                ran.above
                    .push(doc! {"stage": "LIMIT", "limitAmount": limit.abs()});
            }
            if let Some(p) = &projection {
                ran.above
                    .push(doc! {"stage": "PROJECTION_DEFAULT", "transformBy": p.clone()});
                ran.gateway.push("projection in the gateway".into());
            }
            ran.parsed = filter;
        }
        "count" => {
            let q = cmd.get_document("query").cloned().unwrap_or_default();
            let limit = as_i64_or(cmd, "limit").unsigned_abs();
            ran.n = engine.count(rec, ns, &q, skip_of(cmd), limit).await?;
            ran.above.push(doc! {"stage": "COUNT"});
            ran.parsed = q;
        }
        "distinct" => {
            let q = cmd.get_document("query").cloned().unwrap_or_default();
            let key = cmd
                .get_str("key")
                .map_err(|_| MongoError::new(code::FAILED_TO_PARSE, "distinct needs a key"))?;
            ran.n = engine.distinct(rec, ns, key, &q).await?.len() as u64;
            ran.above
                .push(doc! {"stage": "PROJECTION_DEFAULT", "transformBy": {key: 1}});
            ran.gateway
                .push(format!("distinct values of '{key}' in the gateway"));
            ran.parsed = q;
        }
        "aggregate" => {
            let pipeline: Vec<Document> = cmd
                .get_array("pipeline")
                .map(|a| a.iter().filter_map(|s| s.as_document().cloned()).collect())
                .unwrap_or_default();
            let first_match = pipeline
                .first()
                .and_then(|s| s.get_document("$match").ok())
                .cloned()
                .unwrap_or_default();
            let writes = pipeline
                .iter()
                .any(|s| s.contains_key("$out") || s.contains_key("$merge"));
            if writes {
                // `$out` and `$merge` write elsewhere: only the read of
                // the collection is run.
                ran.n = read_all(
                    engine,
                    rec,
                    ns,
                    FindSpec {
                        filter: first_match.clone(),
                        ..FindSpec::default()
                    },
                    execute,
                )
                .await?;
                ran.gateway.push("$out / $merge not run by explain".into());
            } else {
                ran.n = engine.aggregate(rec, ns, &pipeline).await?.len() as u64;
            }
            ran.gateway
                .push("the pipeline's stages after the collection's read, in the gateway".into());
            ran.parsed = first_match;
        }
        "update" => {
            let u = first_of(cmd, "updates");
            let q = u.get_document("q").cloned().unwrap_or_default();
            let multi = u.get_bool("multi").unwrap_or(false);
            let upsert = u.get_bool("upsert").unwrap_or(false);
            let sort = u.get_document("sort").ok().cloned();
            // The update's read: the documents it changes (not the
            // update itself, which may create the collection or register
            // array paths in the catalog).
            let (n, key) = read_keys(
                engine,
                rec,
                ns,
                FindSpec {
                    filter: q.clone(),
                    sort,
                    limit: u64::from(!multi),
                    ..FindSpec::default()
                },
                execute,
            )
            .await?;
            ran.n = n;
            let table = format!("{}.{}", ident(&ns.db), ident(&m.table));
            ran.planned.push(inline(
                &format!("UPDATE {table} SET _v = ?, doc = ?, _types = ? WHERE _id = ? AND _v = ?"),
                &[
                    Param::Int(0),
                    Param::Text("{}".into()),
                    Param::Null,
                    Param::Bin(key.unwrap_or_else(|| vec![0])),
                    Param::Int(0),
                ],
            ));
            if upsert {
                ran.planned.push(format!(
                    "INSERT INTO {table} (_id, doc, _types, _v) VALUES (X'00', '{{}}', NULL, 0)"
                ));
            }
            ran.gateway.push("apply the update to each document in the gateway, then write it if its _v is unchanged".into());
            ran.above.push(doc! {"stage": "UPDATE"});
            ran.parsed = q;
        }
        "delete" => {
            let d = first_of(cmd, "deletes");
            let q = d.get_document("q").cloned().unwrap_or_default();
            let one = d.get("limit").and_then(as_i64) == Some(1);
            let before = rec.noted.iter().filter(|n| n.write).count();
            let _ = engine.delete(rec, ns, &q, one, None).await?;
            if rec.noted.iter().filter(|n| n.write).count() == before {
                ran.planned.push(format!(
                    "DELETE FROM {}.{} WHERE _id IN (X'00')",
                    ident(&ns.db),
                    ident(&m.table)
                ));
            }
            ran.above.push(doc! {"stage": "DELETE"});
            ran.parsed = q;
        }
        _ => {}
    }
    Ok(ran)
}

/// Reads a find's documents (all of them when `execute`, else the first
/// step's statement only): how many.
async fn read_all(
    engine: &Engine,
    rec: &mut Recorder<'_>,
    ns: &Ns,
    spec: FindSpec,
    execute: bool,
) -> Result<u64, MongoError> {
    Ok(read_keys(engine, rec, ns, spec, execute).await?.0)
}

/// [`read_all`], with the first document's encoded `_id`.
async fn read_keys(
    engine: &Engine,
    rec: &mut Recorder<'_>,
    ns: &Ns,
    spec: FindSpec,
    execute: bool,
) -> Result<(u64, Option<Vec<u8>>), MongoError> {
    let mut plan = engine.find(rec, ns, spec).await?;
    let mut n = 0usize;
    let mut first = None;
    loop {
        let batch = engine
            .next_batch(rec, &mut plan, BATCH, 16 * 1024 * 1024)
            .await?;
        if first.is_none() {
            first = batch
                .first()
                .and_then(|d| d.get("_id"))
                .and_then(|i| sprig_doc::json::id_key(i).ok());
        }
        n += batch.len();
        if !execute || Engine::plan_done(&plan) || batch.is_empty() || n >= MAX_EXECUTED_DOCS {
            break;
        }
    }
    Ok((n as u64, first))
}

/// The reply for a collection that doesn't exist: MongoDB's `EOF`.
fn eof(name: &str, ns: &Ns, cmd: &Document, execute: bool) -> Document {
    let planner = doc! {
        "namespace": ns.to_string(),
        "parsedQuery": {},
        "winningPlan": {"stage": "EOF"},
        "rejectedPlans": [],
        "sprig": {"statements": [], "gateway": [], "note": "the collection does not exist"},
    };
    let stats = doc! {"executionSuccess": true, "nReturned": 0i64, "executionTimeMillis": 0i64, "totalKeysExamined": 0i64, "totalDocsExamined": 0i64};
    let mut out = doc! {"explainVersion": "1"};
    if name == "aggregate" {
        let mut c = doc! {"queryPlanner": planner};
        if execute {
            c.insert("executionStats", stats);
        }
        out.insert("stages", vec![Bson::Document(doc! {"$cursor": c})]);
    } else {
        out.insert("queryPlanner", planner);
        if execute {
            out.insert("executionStats", stats);
        }
    }
    out.insert("command", cmd.clone());
    out.insert("ok", 1.0);
    out
}

/// The leaf stage from MariaDB's plan of the main read.
fn leaf(plan: Option<&serde_json::Value>, m: &CollMeta, filter: &Document) -> Document {
    let Some(t) = plan.and_then(first_table) else {
        return doc! {"stage": "EOF"};
    };
    let access = t.get("access_type").and_then(|v| v.as_str()).unwrap_or("");
    let key = t.get("key").and_then(|v| v.as_str());
    let filtered = |mut d: Document| {
        if !filter.is_empty() {
            d.insert("filter", filter.clone());
        }
        d
    };
    match key {
        Some("PRIMARY") if matches!(access, "const" | "system" | "eq_ref") => {
            doc! {"stage": "EXPRESS_IXSCAN", "keyPattern": {"_id": 1}, "indexName": "_id_"}
        }
        // A full walk of the primary key (the `_id` order of the engine's
        // reads) is MongoDB's collection scan.
        Some("PRIMARY") if access == "index" => {
            filtered(doc! {"stage": "COLLSCAN", "direction": "forward"})
        }
        Some(k) => {
            let (name, pattern) = if k == "PRIMARY" {
                ("_id_".to_owned(), doc! {"_id": 1})
            } else {
                m.indexes
                    .iter()
                    .find(|i| i.mariadb_name() == k)
                    .map_or_else(
                        || (k.to_owned(), Document::new()),
                        |i| (i.name.clone(), i.key_doc()),
                    )
            };
            filtered(doc! {
                "stage": "FETCH",
                "inputStage": {"stage": "IXSCAN", "keyPattern": pattern, "indexName": name, "direction": "forward", "mariadbAccess": access},
            })
        }
        None => filtered(doc! {"stage": "COLLSCAN", "direction": "forward"}),
    }
}

/// The first table of a MariaDB JSON plan: the first object with an
/// `access_type`, depth first (under `query_block`, `nested_loop`, a
/// `filesort` or `read_sorted_file`).
fn first_table(v: &serde_json::Value) -> Option<&serde_json::Map<String, serde_json::Value>> {
    match v {
        serde_json::Value::Object(o) => {
            if o.contains_key("access_type") {
                return Some(o);
            }
            o.values().find_map(first_table)
        }
        serde_json::Value::Array(a) => a.iter().find_map(first_table),
        _ => None,
    }
}

/// `text` with the tenant's physical schema names (`t<tenant>_...`) shown
/// as the logical database `db`: the engine's statements touch only the
/// command's database.
fn scrub(text: &str, tenant: u64, db: &str) -> String {
    if tenant == 0 {
        return text.to_owned();
    }
    let prefix = format!("t{tenant}_");
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(i) = rest.find(&prefix) {
        // Only at the start of a name.
        let at_start = rest[..i]
            .chars()
            .next_back()
            .is_none_or(|c| !(c.is_alphanumeric() || c == '_'));
        out.push_str(&rest[..i]);
        let after = &rest[i + prefix.len()..];
        let end = after
            .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '$'))
            .unwrap_or(after.len());
        if at_start {
            out.push_str(db);
        } else {
            out.push_str(&prefix);
            out.push_str(&after[..end]);
        }
        rest = &after[end..];
    }
    out.push_str(rest);
    out
}

/// MariaDB's `EXPLAIN FORMAT=JSON` with the `"` inside string values
/// escaped: MariaDB writes conditions such as `json_value(doc,'$."tag"')`
/// into `attached_condition` without escaping them. Its output has one
/// member per line, so each `"name": "..."` line is mended alone.
fn repair_json(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 16);
    for line in text.split_inclusive('\n') {
        let (body, nl) = line.strip_suffix('\n').map_or((line, ""), |b| (b, "\n"));
        let Some(start) = body.find(": \"") else {
            out.push_str(line);
            continue;
        };
        let value_start = start + 3;
        let rest = &body[value_start..];
        let (inner, tail) = if let Some(i) = rest.strip_suffix("\",") {
            (i, "\",")
        } else if let Some(i) = rest.strip_suffix('"') {
            (i, "\"")
        } else {
            out.push_str(line);
            continue;
        };
        out.push_str(&body[..value_start]);
        let mut prev_backslash = false;
        for c in inner.chars() {
            if c == '"' && !prev_backslash {
                out.push('\\');
            }
            prev_backslash = c == '\\' && !prev_backslash;
            out.push(c);
        }
        out.push_str(tail);
        out.push_str(nl);
    }
    out
}

/// A JSON value as BSON (integers as int64 when they fit).
fn json_bson(v: serde_json::Value) -> Bson {
    match v {
        serde_json::Value::Null => Bson::Null,
        serde_json::Value::Bool(b) => Bson::Boolean(b),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(Bson::Int64)
            .or_else(|| n.as_f64().map(Bson::Double))
            .unwrap_or(Bson::Null),
        serde_json::Value::String(s) => Bson::String(s),
        serde_json::Value::Array(a) => Bson::Array(a.into_iter().map(json_bson).collect()),
        serde_json::Value::Object(o) => {
            Bson::Document(o.into_iter().map(|(k, v)| (k, json_bson(v))).collect())
        }
    }
}

/// The pipeline's stages after a leading `$match`.
fn pipeline_rest(cmd: &Document) -> Vec<Document> {
    let stages: Vec<Document> = cmd
        .get_array("pipeline")
        .map(|a| a.iter().filter_map(|s| s.as_document().cloned()).collect())
        .unwrap_or_default();
    let skip = usize::from(stages.first().is_some_and(|s| s.contains_key("$match")));
    stages.into_iter().skip(skip).collect()
}

fn first_of(cmd: &Document, list: &str) -> Document {
    cmd.get_array(list)
        .ok()
        .and_then(|a| a.first())
        .and_then(Bson::as_document)
        .cloned()
        .unwrap_or_default()
}

fn as_i64_or(d: &Document, k: &str) -> i64 {
    d.get(k).and_then(as_i64).unwrap_or(0)
}

/// A sort by `_id` alone, which the keyset read gives.
fn id_sort(s: &Document) -> bool {
    s.len() == 1 && s.contains_key("_id")
}

fn to_i64(n: u64) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// Codes not in the shared table.
mod code {
    pub(crate) use sprig_proto_mongo::errors::code::*;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn physical_schema_names_are_shown_as_the_database() {
        assert_eq!(
            scrub(
                "t5_d1_g1_s1.c._id = X'01' or json_value(t5_d1_g1_s1.c.doc,'$.a')",
                5,
                "shop"
            ),
            "shop.c._id = X'01' or json_value(shop.c.doc,'$.a')"
        );
        // Another tenant's prefix, or the prefix inside a name, stays.
        assert_eq!(scrub("t55_x.c", 5, "shop"), "t55_x.c");
        assert_eq!(scrub("at5_x.c", 5, "shop"), "at5_x.c");
    }

    #[test]
    fn mariadb_json_with_bare_quotes_is_mended() {
        let bad = "{\n  \"t\": {\n    \"attached_condition\": \"json_value(o.doc,'$.\"tag\"') = 'x'\",\n    \"rows\": 2\n  }\n}";
        assert!(serde_json::from_str::<serde_json::Value>(bad).is_err());
        let v: serde_json::Value = serde_json::from_str(&repair_json(bad)).unwrap();
        assert_eq!(
            v["t"]["attached_condition"],
            "json_value(o.doc,'$.\"tag\"') = 'x'"
        );
        assert_eq!(v["t"]["rows"], 2);
    }

    #[test]
    fn statements_on_the_collection_are_told_apart() {
        let t = "`shop`.`orders`";
        assert!(names("SELECT _id FROM `shop`.`orders` WHERE _id = ?", t));
        assert!(!names("SELECT * FROM `shop`.`orders__geo_loc`", t));
        assert!(!names("SELECT * FROM `shop`.`orders2`", t));
        assert!(is_read(" select 1"));
        assert!(!is_read("UPDATE `shop`.`orders` SET doc = '{}'"));
    }

    #[test]
    fn stages_come_from_mariadb_access() {
        let m = CollMeta {
            name: "c".into(),
            table: "c".into(),
            uuid: [0; 16],
            array_paths: std::collections::BTreeSet::default(),
            arrays_full: false,
            pending_paths: std::collections::BTreeSet::default(),
            indexes: Vec::new(),
            options: Document::new(),
            next_index: 1,
        };
        let point: serde_json::Value = serde_json::json!({"query_block": {"select_id": 1, "table": {"table_name": "c", "access_type": "const", "key": "PRIMARY"}}});
        assert_eq!(
            leaf(Some(&point), &m, &doc! {"_id": 1}).get_str("stage"),
            Ok("EXPRESS_IXSCAN")
        );
        let scan: serde_json::Value = serde_json::json!({"query_block": {"nested_loop": [{"table": {"table_name": "c", "access_type": "ALL"}}]}});
        let l = leaf(Some(&scan), &m, &doc! {"a": 1});
        assert_eq!(l.get_str("stage"), Ok("COLLSCAN"));
        assert_eq!(l.get_document("filter"), Ok(&doc! {"a": 1}));
        let range: serde_json::Value = serde_json::json!({"query_block": {"table": {"table_name": "c", "access_type": "range", "key": "PRIMARY"}}});
        let l = leaf(Some(&range), &m, &Document::new());
        assert_eq!(l.get_str("stage"), Ok("FETCH"));
        assert_eq!(
            l.get_document("inputStage").unwrap().get_str("indexName"),
            Ok("_id_")
        );
        assert_eq!(leaf(None, &m, &Document::new()).get_str("stage"), Ok("EOF"));
    }
}
