//! Query settings and query statistics (M3, decisions S96–S99):
//! `setQuerySettings` and `removeQuerySettings` (MongoDB 8.0, with 8.1's
//! `comment` and 9.0's per-shape `maxTimeMS`), the `$querySettings` stage,
//! 9.0's per-command `querySettings` field on `find`, `aggregate` and
//! `distinct`, and the `$queryStats` stage (8.0's shape, with 9.0's write
//! commands and `metrics.writes`).
//!
//! Shapes come from [`sprig_doc::qshape`]. Settings are kept per tenant in
//! the shape's database (hidden table `__sprig_doc_query_settings`) and read
//! through a two-second cache per gateway (S97). Statistics are kept per
//! gateway and tenant in memory (S98).
//!
//! What the settings do here:
//! - `reject: true` refuses the query with 411 `QueryRejectedBySettings`;
//! - `maxTimeMS` (9.0) runs the command's statements under MariaDB's
//!   `max_statement_time` and answers 50 `MaxTimeMSExpired` when one runs
//!   out;
//! - `indexHints`, `queryFramework` and `comment` are kept and listed; the
//!   index hints aren't applied (MariaDB's optimizer picks the index, S99).

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use bson::{Bson, Document, doc};
use sprig_doc::engine::{Engine, SqlRunner, mongo_error};
use sprig_doc::qshape;
use sprig_proto_mongo::errors::{MongoError, code};

/// 411 `QueryRejectedBySettings`.
pub const QUERY_REJECTED_BY_SETTINGS: i32 = 411;
/// The table that keeps a database's query settings.
pub const TABLE: &str = "__sprig_doc_query_settings";
/// How long a gateway trusts its copy of a database's settings.
const CACHE_TTL: Duration = Duration::from_secs(2);
/// Shapes kept per tenant by `$queryStats` (the oldest go first).
const MAX_SHAPES: usize = 5000;

/// Whether this module runs command `name`.
#[must_use]
pub fn handles(name: &str) -> bool {
    matches!(name, "setQuerySettings" | "removeQuerySettings")
}

/// Whether `stage` is one of this module's aggregation stages.
#[must_use]
pub fn handles_stage(stage: &Document) -> bool {
    stage.contains_key("$querySettings") || stage.contains_key("$queryStats")
}

#[derive(Debug, Clone)]
struct Entry {
    hash: String,
    settings: Document,
    representative: Option<Document>,
    shape: Option<Document>,
}

#[derive(Debug, Default, Clone)]
struct Agg {
    sum: i64,
    max: i64,
    min: i64,
    sq: i128,
}

impl Agg {
    fn add(&mut self, v: i64, first: bool) {
        self.sum = self.sum.saturating_add(v);
        if first || v > self.max {
            self.max = v;
        }
        if first || v < self.min {
            self.min = v;
        }
        self.sq = self.sq.saturating_add(i128::from(v) * i128::from(v));
    }

    fn to_doc(&self) -> Document {
        doc! {
            "sum": self.sum,
            "max": self.max,
            "min": self.min,
            "sumOfSquares": self.sq.to_string().parse::<bson::Decimal128>().map_or(Bson::Null, Bson::Decimal128),
        }
    }
}

#[derive(Debug, Clone)]
struct Stat {
    key: Document,
    key_hash: String,
    shape_hash: String,
    count: i64,
    last_micros: i64,
    exec: Agg,
    first_response: Agg,
    returned: Agg,
    writes: Option<[Agg; 5]>,
    first_seen: bson::DateTime,
    last_seen: bson::DateTime,
}

/// A database's settings as read at an instant.
type Cached = (Instant, Arc<Vec<Entry>>);

/// What a settings command names: database, hash, representative query,
/// shape.
type Target = (Option<String>, String, Option<Document>, Option<Document>);

#[derive(Default)]
struct State {
    settings: HashMap<(u64, String), Cached>,
    stats: HashMap<u64, HashMap<String, Stat>>,
    cursors: HashMap<(u64, i64), String>,
}

static STATE: LazyLock<Mutex<State>> = LazyLock::new(Mutex::default);

fn state() -> std::sync::MutexGuard<'static, State> {
    STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn bad(msg: impl Into<String>) -> MongoError {
    MongoError::new(code::BAD_VALUE, msg)
}

fn ident(s: &str) -> String {
    sprig_doc::sql::ident(s)
}

fn lit(s: &str) -> String {
    let mut out = String::from("'");
    for ch in s.chars() {
        match ch {
            '\'' => out.push_str("''"),
            '\\' => out.push_str("\\\\"),
            '\0' => out.push_str("\\0"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

fn to_json(d: &Document) -> String {
    Bson::Document(d.clone())
        .into_canonical_extjson()
        .to_string()
}

fn from_json(s: &str) -> Option<Document> {
    let v: serde_json::Value = serde_json::from_str(s).ok()?;
    match Bson::try_from(v).ok()? {
        Bson::Document(d) => Some(d),
        _ => None,
    }
}

fn text(c: Option<&Vec<u8>>) -> String {
    c.map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default()
}

/// The settings of database `db` (cached).
async fn load(sql: &mut dyn SqlRunner, tenant: u64, db: &str) -> Arc<Vec<Entry>> {
    if let Some((at, v)) = state().settings.get(&(tenant, db.to_owned()))
        && at.elapsed() < CACHE_TTL
    {
        return Arc::clone(v);
    }
    let q = format!(
        "SELECT hash, settings, representative, shape FROM {}.{}",
        ident(db),
        ident(TABLE)
    );
    let list: Vec<Entry> = match sql.query(&q).await {
        Ok(rows) => rows
            .iter()
            .map(|r| Entry {
                hash: text(r[0].as_ref()),
                settings: from_json(&text(r[1].as_ref())).unwrap_or_default(),
                representative: r[2]
                    .as_ref()
                    .and_then(|b| from_json(&String::from_utf8_lossy(b))),
                shape: r[3]
                    .as_ref()
                    .and_then(|b| from_json(&String::from_utf8_lossy(b))),
            })
            .collect(),
        // No table (1146) or no database (1049): no settings.
        Err(_) => Vec::new(),
    };
    let v = Arc::new(list);
    state()
        .settings
        .insert((tenant, db.to_owned()), (Instant::now(), Arc::clone(&v)));
    v
}

fn forget(tenant: u64, db: &str) {
    state().settings.remove(&(tenant, db.to_owned()));
}

/// The query a `setQuerySettings`/`removeQuerySettings` names: its
/// database, shape, hash and representative query, or a hash alone.
fn target(v: &Bson) -> Result<Target, MongoError> {
    match v {
        Bson::String(h) if qshape::is_hash(h) => Ok((None, h.to_ascii_uppercase(), None, None)),
        Bson::String(_) => Err(bad("the query shape hash must be 64 hexadecimal digits")),
        Bson::Document(q) => {
            let db = q
                .get_str("$db")
                .map_err(|_| bad("the representative query must include '$db'"))?
                .to_owned();
            let name = q.keys().next().cloned().unwrap_or_default();
            if !matches!(name.as_str(), "find" | "aggregate" | "distinct") {
                return Err(MongoError::new(
                    7_746_402,
                    format!("query settings apply to find, aggregate and distinct, not '{name}'"),
                ));
            }
            sprig_doc::filter::parse(
                &q.get_document(if name == "distinct" {
                    "query"
                } else {
                    "filter"
                })
                .cloned()
                .unwrap_or_default(),
            )?;
            let shape = qshape::shape(&name, q, &db)
                .ok_or_else(|| bad("the representative query has no collection"))?;
            Ok((
                Some(db),
                qshape::shape_hash(&shape),
                Some(q.clone()),
                Some(shape),
            ))
        }
        _ => Err(MongoError::new(
            code::TYPE_MISMATCH,
            "setQuerySettings takes a query or a query shape hash",
        )),
    }
}

fn check_settings(s: &Document) -> Result<(), MongoError> {
    for (k, v) in s {
        match k.as_str() {
            "reject" => {
                if !matches!(v, Bson::Boolean(_)) {
                    return Err(MongoError::new(
                        code::TYPE_MISMATCH,
                        "'reject' must be a boolean",
                    ));
                }
            }
            "queryFramework" => {
                if !matches!(v.as_str(), Some("classic" | "sbe")) {
                    return Err(bad("'queryFramework' must be 'classic' or 'sbe'"));
                }
            }
            "indexHints" => {
                let list: Vec<&Bson> = match v {
                    Bson::Array(a) => a.iter().collect(),
                    other => vec![other],
                };
                for h in list {
                    let ok = h.as_document().is_some_and(|d| {
                        d.get_document("ns").is_ok() && d.get_array("allowedIndexes").is_ok()
                    });
                    if !ok {
                        return Err(bad(
                            "each index hint needs 'ns' {db, coll} and 'allowedIndexes'",
                        ));
                    }
                }
            }
            "comment" => {}
            "maxTimeMS" => {
                if sprig_doc::filter::as_i64(v).is_none_or(|n| n < 0) {
                    return Err(bad("'maxTimeMS' must be a non-negative whole number"));
                }
            }
            "queryKnobs" => {
                let Bson::Document(d) = v else {
                    return Err(MongoError::new(
                        code::TYPE_MISMATCH,
                        "'queryKnobs' must be an object",
                    ));
                };
                if let Some(k) = d.keys().next() {
                    return Err(MongoError::new(
                        code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                        format!("query knob '{k}' has no SprigDB equivalent in this release"),
                    ));
                }
            }
            other => {
                return Err(MongoError::new(
                    40415,
                    format!("BSON field 'setQuerySettings.settings.{other}' is an unknown field."),
                ));
            }
        }
    }
    Ok(())
}

/// Whether the settings say anything beyond the defaults.
fn meaningful(s: &Document) -> bool {
    s.iter().any(|(k, v)| match k.as_str() {
        "reject" => v.as_bool() == Some(true),
        "comment" => false,
        _ => true,
    })
}

fn entry_doc(e: &Entry, debug: bool) -> Document {
    let mut d = doc! {"queryShapeHash": e.hash.clone(), "settings": e.settings.clone()};
    if let Some(r) = &e.representative {
        d.insert("representativeQuery", r.clone());
    }
    if debug && let Some(s) = &e.shape {
        d.insert("debugQueryShape", s.clone());
    }
    d
}

/// `setQuerySettings` and `removeQuerySettings` (on `admin`).
///
/// # Errors
/// 2, 14 and 40415 for bad arguments; 7746604 for settings with nothing
/// in them; SQL errors.
pub async fn command(
    engine: &Engine,
    sql: &mut dyn SqlRunner,
    tenant: u64,
    name: &str,
    body: &Document,
    db: &str,
) -> Result<Document, MongoError> {
    if db != "admin" {
        return Err(MongoError::new(
            code::UNAUTHORIZED,
            format!("{name} may only be run against the admin database."),
        ));
    }
    let arg = body
        .get(name)
        .ok_or_else(|| bad(format!("{name} needs a query")))?;
    let (qdb, hash, rep, shape) = target(arg)?;
    // A hash alone names settings already kept in some database.
    let qdb = match qdb {
        Some(d) => d,
        None => find_db(engine, sql, tenant, &hash).await?.ok_or_else(|| {
            MongoError::new(
                7_746_401,
                "no query settings have this hash; give a representative query to create them",
            )
        })?,
    };
    let existing = load(sql, tenant, &qdb)
        .await
        .iter()
        .find(|e| e.hash == hash)
        .cloned();
    if name == "removeQuerySettings" {
        if existing.is_some() {
            let q = format!(
                "DELETE FROM {}.{} WHERE hash = {}",
                ident(&qdb),
                ident(TABLE),
                lit(&hash)
            );
            sql.exec(&q).await.map_err(|e| mongo_error(&e))?;
            forget(tenant, &qdb);
        }
        return Ok(doc! {"ok": 1.0});
    }
    let new = body
        .get_document("settings")
        .map_err(|_| MongoError::new(code::TYPE_MISMATCH, "setQuerySettings needs 'settings'"))?;
    if new.is_empty() {
        return Err(MongoError::new(
            8_727_502,
            "settings field in setQuerySettings command cannot be empty",
        ));
    }
    check_settings(new)?;
    // New fields replace old ones; the rest are kept (MongoDB merges).
    let mut merged = existing
        .as_ref()
        .map(|e| e.settings.clone())
        .unwrap_or_default();
    for (k, v) in new {
        merged.insert(k.clone(), v.clone());
    }
    if !meaningful(&merged) {
        return Err(MongoError::new(
            7_746_604,
            "the resulting settings cannot be empty or contain only default values",
        ));
    }
    let rep = rep.or_else(|| existing.as_ref().and_then(|e| e.representative.clone()));
    let shape = shape.or_else(|| existing.as_ref().and_then(|e| e.shape.clone()));
    engine.ensure_db(sql, &qdb).await?;
    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (hash CHAR(64) NOT NULL PRIMARY KEY, settings LONGTEXT NOT NULL, \
         representative LONGTEXT NULL, shape LONGTEXT NULL) ENGINE=InnoDB",
        ident(&qdb),
        ident(TABLE)
    );
    sql.exec(&ddl).await.map_err(|e| mongo_error(&e))?;
    let opt = |d: &Option<Document>| {
        d.as_ref()
            .map_or_else(|| "NULL".to_owned(), |d| lit(&to_json(d)))
    };
    let q = format!(
        "REPLACE INTO {}.{} (hash, settings, representative, shape) VALUES ({}, {}, {}, {})",
        ident(&qdb),
        ident(TABLE),
        lit(&hash),
        lit(&to_json(&merged)),
        opt(&rep),
        opt(&shape),
    );
    sql.exec(&q).await.map_err(|e| mongo_error(&e))?;
    forget(tenant, &qdb);
    let mut out = doc! {"queryShapeHash": hash, "settings": merged};
    if let Some(r) = rep {
        out.insert("representativeQuery", r);
    }
    out.insert("ok", 1.0);
    Ok(out)
}

async fn find_db(
    engine: &Engine,
    sql: &mut dyn SqlRunner,
    tenant: u64,
    hash: &str,
) -> Result<Option<String>, MongoError> {
    for db in engine.list_databases(sql).await? {
        if load(sql, tenant, &db).await.iter().any(|e| e.hash == hash) {
            return Ok(Some(db));
        }
    }
    Ok(None)
}

/// The documents of `$querySettings` or `$queryStats` (the first stage of
/// an `aggregate: 1` on `admin`); the caller runs the stages after it.
///
/// # Errors
/// Bad stage arguments; SQL errors.
pub async fn stage_docs(
    engine: &Engine,
    sql: &mut dyn SqlRunner,
    tenant: u64,
    db: &str,
    stage: &Document,
) -> Result<Vec<Document>, MongoError> {
    if db != "admin" {
        let n = stage.keys().next().cloned().unwrap_or_default();
        return Err(MongoError::new(
            73,
            format!("{n} must be run against the 'admin' database with {{aggregate: 1}}"),
        ));
    }
    if let Ok(a) = stage.get_document("$querySettings") {
        let debug = a.get_bool("showDebugQueryShape").unwrap_or(false);
        let mut out = Vec::new();
        for d in engine.list_databases(sql).await? {
            for e in load(sql, tenant, &d).await.iter() {
                out.push(entry_doc(e, debug));
            }
        }
        return Ok(out);
    }
    let a = stage
        .get_document("$queryStats")
        .map_err(|_| MongoError::new(code::TYPE_MISMATCH, "$queryStats takes an object"))?;
    if a.contains_key("transformIdentifiers") {
        return Err(MongoError::new(
            code::SPRIG_DOC_FEATURE_UNSUPPORTED,
            "$queryStats with transformIdentifiers is not supported in this release",
        ));
    }
    let now = bson::DateTime::now();
    let st = state();
    let Some(map) = st.stats.get(&tenant) else {
        return Ok(Vec::new());
    };
    let mut list: Vec<&Stat> = map.values().collect();
    list.sort_by_key(|s| s.first_seen.timestamp_millis());
    Ok(list
        .into_iter()
        .map(|s| {
            let mut metrics = doc! {
                "lastExecutionMicros": s.last_micros,
                "execCount": s.count,
                "totalExecMicros": s.exec.to_doc(),
                "firstResponseExecMicros": s.first_response.to_doc(),
                "docsReturned": s.returned.to_doc(),
            };
            if let Some(w) = &s.writes {
                metrics.insert(
                    "writes",
                    doc! {
                        "nMatched": w[0].to_doc(),
                        "nModified": w[1].to_doc(),
                        "nUpserted": w[2].to_doc(),
                        "nInserted": w[3].to_doc(),
                        "nDeleted": w[4].to_doc(),
                    },
                );
            }
            metrics.insert("firstSeenTimestamp", s.first_seen);
            metrics.insert("latestSeenTimestamp", s.last_seen);
            doc! {
                "key": s.key.clone(),
                "keyHash": s.key_hash.clone(),
                "queryShapeHash": s.shape_hash.clone(),
                "metrics": metrics,
                "asOf": now,
            }
        })
        .collect())
}

/// What the settings of a query ask for, once it passed `reject`.
#[derive(Debug, Default)]
pub struct Applied {
    /// 9.0's per-shape `maxTimeMS`.
    pub max_time_ms: Option<i64>,
}

/// Looks up the settings of a `find`, `aggregate` or `distinct` (stored,
/// merged with 9.0's per-command `querySettings`, stored first) and
/// refuses a rejected query.
///
/// # Errors
/// 411 for a rejected query; 2 for a bad `querySettings` field.
pub async fn before(
    sql: &mut dyn SqlRunner,
    tenant: u64,
    name: &str,
    body: &Document,
    db: &str,
) -> Result<Applied, MongoError> {
    if !matches!(name, "find" | "aggregate" | "distinct") {
        return Ok(Applied::default());
    }
    let mut settings = match body.get("querySettings") {
        None => Document::new(),
        Some(Bson::Document(d)) => {
            check_settings(d)?;
            d.clone()
        }
        Some(_) => {
            return Err(MongoError::new(
                code::TYPE_MISMATCH,
                "BSON field 'querySettings' is the wrong type, expected type 'object'",
            ));
        }
    };
    // The shape is worked out only when the database has settings.
    let list = if db == "admin" {
        Arc::new(Vec::new())
    } else {
        load(sql, tenant, db).await
    };
    if !list.is_empty()
        && let Some(shape) = qshape::shape(name, body, db)
    {
        let hash = qshape::shape_hash(&shape);
        if let Some(e) = list.iter().find(|e| e.hash == hash) {
            for (k, v) in &e.settings {
                settings.insert(k.clone(), v.clone());
            }
        }
    }
    if settings.get_bool("reject").unwrap_or(false) {
        metrics::counter!("sprig_mongo_query_settings_rejected_total").increment(1);
        return Err(MongoError::new(
            QUERY_REJECTED_BY_SETTINGS,
            "Query rejected by admin query settings",
        ));
    }
    Ok(Applied {
        max_time_ms: settings
            .get("maxTimeMS")
            .and_then(sprig_doc::filter::as_i64)
            .filter(|n| *n > 0),
    })
}

/// A command's own `maxTimeMS` (S134): `find`, `aggregate`, `count`,
/// `distinct`, `update`, `delete` and `findAndModify`; 0 or absent is no
/// limit, as in MongoDB.
#[must_use]
pub fn own_max_time(name: &str, body: &Document) -> Option<i64> {
    if !matches!(
        name,
        "find" | "aggregate" | "count" | "distinct" | "update" | "delete" | "findAndModify"
    ) {
        return None;
    }
    body.get("maxTimeMS")
        .and_then(sprig_doc::filter::as_i64)
        .filter(|n| *n > 0)
}

/// Sets MariaDB's `max_statement_time` for 9.0's per-shape `maxTimeMS`.
///
/// # Errors
/// SQL errors.
pub async fn set_max_time(sql: &mut dyn SqlRunner, ms: i64) -> Result<(), MongoError> {
    #[allow(clippy::cast_precision_loss)]
    let secs = ms as f64 / 1000.0;
    sql.exec(&format!("SET SESSION max_statement_time = {secs:.3}"))
        .await
        .map(|_| ())
        .map_err(|e| mongo_error(&e))
}

/// Clears what [`set_max_time`] set.
pub async fn reset_max_time(sql: &mut dyn SqlRunner) {
    let _ = sql.exec("SET SESSION max_statement_time = 0").await;
}

/// MariaDB's statement timeout (1969) as 50 `MaxTimeMSExpired`.
#[must_use]
pub fn map_timeout(e: MongoError) -> MongoError {
    if e.message.contains("max_statement_time") || e.code == 1969 {
        MongoError::new(code::MAX_TIME_MS_EXPIRED, "operation exceeded time limit")
    } else {
        e
    }
}

fn count_of(reply: &Document) -> i64 {
    reply
        .get_document("cursor")
        .ok()
        .and_then(|c| {
            c.get_array("firstBatch")
                .or_else(|_| c.get_array("nextBatch"))
                .ok()
        })
        .map_or_else(
            || {
                reply
                    .get_array("values")
                    .map_or(0, |v| i64::try_from(v.len()).unwrap_or(i64::MAX))
            },
            |b| i64::try_from(b.len()).unwrap_or(i64::MAX),
        )
}

/// Runs recorded per second per gateway at most (MongoDB 8.0's
/// `internalQueryStatsRateLimit` is 100); the rest aren't counted (S98).
const RATE_LIMIT: u32 = 1000;

static WINDOW: LazyLock<Mutex<(Instant, u32)>> = LazyLock::new(|| Mutex::new((Instant::now(), 0)));

fn admit() -> bool {
    let mut w = WINDOW
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if w.0.elapsed() >= Duration::from_secs(1) {
        *w = (Instant::now(), 0);
    }
    w.1 += 1;
    w.1 <= RATE_LIMIT
}

/// Records one run of a command for `$queryStats`; `raw_n`: the documents
/// of a reply sent as raw bytes.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub fn record(
    tenant: u64,
    name: &str,
    body: &Document,
    db: &str,
    client: Option<&Document>,
    elapsed: Duration,
    reply: &Document,
    raw_n: Option<i64>,
) {
    let micros = i64::try_from(elapsed.as_micros()).unwrap_or(i64::MAX);
    if name == "getMore" {
        let Some(id) = sprig_doc::filter::as_i64(body.get("getMore").unwrap_or(&Bson::Null)) else {
            return;
        };
        let n = count_of(reply);
        let mut st = state();
        let Some(kh) = st.cursors.get(&(tenant, id)).cloned() else {
            return;
        };
        let done = reply
            .get_document("cursor")
            .ok()
            .and_then(|c| c.get("id"))
            .and_then(sprig_doc::filter::as_i64)
            == Some(0);
        if done {
            st.cursors.remove(&(tenant, id));
        }
        if let Some(s) = st.stats.get_mut(&tenant).and_then(|m| m.get_mut(&kh)) {
            s.exec.sum = s.exec.sum.saturating_add(micros);
            s.returned.sum = s.returned.sum.saturating_add(n);
        }
        return;
    }
    if db == "admin" && name == "aggregate" {
        // `$queryStats` and other admin pipelines are left out.
        return;
    }
    if !matches!(
        name,
        "find" | "aggregate" | "distinct" | "count" | "insert" | "update" | "delete"
    ) || !admit()
    {
        return;
    }
    let Some(shape) = qshape::shape(name, body, db) else {
        return;
    };
    let mut key = doc! {"queryShape": shape.clone()};
    if let Some(c) = client {
        key.insert("client", c.clone());
    }
    key.insert("collectionType", "collection");
    let kh = qshape::key_hash(&key);
    let now = bson::DateTime::now();
    let n = raw_n.unwrap_or_else(|| count_of(reply));
    let writes = match name {
        "insert" | "update" | "delete" => {
            let g = |k: &str| {
                reply
                    .get(k)
                    .and_then(sprig_doc::filter::as_i64)
                    .unwrap_or(0)
            };
            let ups = reply
                .get_array("upserted")
                .map_or(0, |a| i64::try_from(a.len()).unwrap_or(0));
            Some(match name {
                "insert" => [0, 0, 0, g("n"), 0],
                "update" => [g("n") - ups, g("nModified"), ups, 0, 0],
                _ => [0, 0, 0, 0, g("n")],
            })
        }
        _ => None,
    };
    let mut st = state();
    let cursor = reply
        .get_document("cursor")
        .ok()
        .and_then(|c| c.get("id"))
        .and_then(sprig_doc::filter::as_i64)
        .filter(|id| *id != 0);
    if let Some(id) = cursor {
        st.cursors.insert((tenant, id), kh.clone());
        if st.cursors.len() > 100_000 {
            st.cursors.clear();
        }
    }
    let map = st.stats.entry(tenant).or_default();
    if !map.contains_key(&kh)
        && map.len() >= MAX_SHAPES
        && let Some(old) = map
            .values()
            .min_by_key(|s| s.last_seen.timestamp_millis())
            .map(|s| s.key_hash.clone())
    {
        map.remove(&old);
    }
    let s = map.entry(kh.clone()).or_insert_with(|| Stat {
        key,
        key_hash: kh,
        shape_hash: qshape::shape_hash(&shape),
        count: 0,
        last_micros: 0,
        exec: Agg::default(),
        first_response: Agg::default(),
        returned: Agg::default(),
        writes: None,
        first_seen: now,
        last_seen: now,
    });
    let first = s.count == 0;
    s.count += 1;
    s.last_micros = micros;
    s.exec.add(micros, first);
    s.first_response.add(micros, first);
    s.returned.add(if writes.is_some() { 0 } else { n }, first);
    if let Some(w) = writes {
        let agg = s.writes.get_or_insert_with(Default::default);
        for (a, v) in agg.iter_mut().zip(w) {
            a.add(v, first);
        }
    }
    s.last_seen = now;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_are_checked() {
        assert!(check_settings(&doc! {"reject": true, "comment": "x"}).is_ok());
        assert!(check_settings(&doc! {"reject": 1}).is_err());
        assert!(check_settings(&doc! {"bogus": 1}).is_err());
        assert!(!meaningful(&doc! {"reject": false, "comment": "x"}));
        assert!(meaningful(&doc! {"maxTimeMS": 5}));
    }

    #[test]
    fn stats_accumulate() {
        let body = doc! {"find": "c", "filter": {"a": 1}};
        let reply = doc! {"cursor": {"firstBatch": [{"a": 1}], "id": 0i64, "ns": "d.c"}, "ok": 1.0};
        record(
            7,
            "find",
            &body,
            "d",
            None,
            Duration::from_micros(10),
            &reply,
            None,
        );
        record(
            7,
            "find",
            &doc! {"find": "c", "filter": {"a": 2}},
            "d",
            None,
            Duration::from_micros(30),
            &reply,
            None,
        );
        let st = state();
        let m = st.stats.get(&7).unwrap();
        assert_eq!(m.len(), 1);
        let s = m.values().next().unwrap();
        assert_eq!(s.count, 2);
        assert_eq!(s.exec.sum, 40);
        assert_eq!(s.exec.min, 10);
        assert_eq!(s.returned.sum, 2);
    }
}
