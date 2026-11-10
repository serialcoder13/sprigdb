//! The document engine's operations over SQL (chapter 31, "Writes and
//! updates", "Planning and sorting"; build steps 4, 5, 8, 9 and 12).
//!
//! The engine never holds a connection: every operation takes a
//! [`SqlRunner`], which the gateway implements over the user's own
//! executor session (logical names, chapter 29's privileges, the router
//! and transactions). A [`CursorPlan`] is resumable between batches
//! without a connection: a keyset position on `_id`, or a buffer of
//! results sorted in the gateway.
//!
//! **Plans.**
//!
//! - *Keyset*: no sort, or a sort on `_id`. Each batch is
//!   `SELECT _id, doc, _types FROM t WHERE <filter> AND _id > ? ORDER BY
//!   _id LIMIT n`; exact filters push `skip + limit` into the `LIMIT`.
//! - *Buffer*: any other sort. Candidates are read in `_id` chunks,
//!   rechecked, sorted in Rust within `doc.max_sort_bytes` (else 292), and
//!   served from the buffer.
//!
//! Every candidate is rechecked with the MongoDB matcher, also on exact
//! plans: exactness decides only whether `skip` and `limit` reach SQL.
//!
//! **Writes.** Inserts register new array paths first (in their own
//! committed statement, see [`crate::meta`]), then insert rows in one
//! multi-row `INSERT`. Updates read, apply the operators in Rust and write
//! back with `WHERE _id = ? AND _v = ?`, retrying on a lost race
//! (read-modify-write for every operator in this release).

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtOrd};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bson::{Bson, DateTime, Document, doc, oid::ObjectId};
use sprig_proto_mongo::errors::{MongoError, code};

use crate::agg::{self, AggExpr, AggLimits, Projection};
use crate::catalog::{DocCatalog, TableCatalog};
use crate::filter::{self, Pred};
use crate::json::{self, RenderOpts};
use crate::meta::{self, CollMeta, IndexDef};
use crate::sql::{bin_lit, ident, str_lit};
use crate::translate::{SqlPred, translate};
use crate::update::{self, ApplyCx, UpdateSpec};

/// An error of one SQL statement.
#[derive(Debug, Clone)]
pub struct SqlError {
    /// The MySQL error number, when MariaDB or the gateway raised one.
    pub errno: Option<u16>,
    /// The message.
    pub message: String,
    /// The error already in MongoDB terms, when the runner knows it.
    pub mongo: Option<MongoError>,
}

impl SqlError {
    fn is(&self, n: u16) -> bool {
        self.errno == Some(n)
    }
}

/// The OK result of a statement.
#[derive(Debug, Clone, Default)]
pub struct Done {
    /// Affected rows.
    pub affected: u64,
    /// The OK packet's info (`Rows matched: 1  Changed: 1 ...`).
    pub info: String,
}

/// Rows: one `Option<bytes>` per column.
pub type Rows = Vec<Vec<Option<Vec<u8>>>>;

/// Runs SQL for the engine, in the user's session.
#[async_trait]
pub trait SqlRunner: Send {
    /// Runs a statement returning rows.
    async fn query(&mut self, sql: &str) -> Result<Rows, SqlError>;
    /// Runs a statement returning an OK packet.
    async fn exec(&mut self, sql: &str) -> Result<Done, SqlError>;
    /// The tenant the statements run for (0 outside the tenant catalog),
    /// for a [`DocCatalog`] that keeps metadata per tenant.
    fn tenant(&self) -> u64 {
        0
    }
}

/// MongoDB's view of a SQL error.
#[must_use]
pub fn mongo_error(e: &SqlError) -> MongoError {
    if let Some(m) = &e.mongo {
        return m.clone();
    }
    let c = match e.errno {
        Some(1062) => code::DUPLICATE_KEY,
        Some(1142 | 1044 | 1045 | 1227 | 1143) => code::UNAUTHORIZED,
        Some(1146 | 1049) => code::NAMESPACE_NOT_FOUND,
        Some(1213 | 1205) => code::WRITE_CONFLICT,
        Some(3024 | 1969) => code::MAX_TIME_MS_EXPIRED,
        Some(4025) => code::DOCUMENT_VALIDATION_FAILURE,
        Some(n) if (39000..40000).contains(&n) => i32::from(n),
        _ => code::INTERNAL_ERROR,
    };
    let mut m = MongoError::new(c, e.message.clone());
    if c == code::WRITE_CONFLICT {
        m = m.label("TransientTransactionError");
    }
    m
}

/// Engine settings (chapter 31, "Configuration").
#[derive(Debug, Clone, Copy)]
pub struct EngineConfig {
    /// `doc.type_map`.
    pub type_map: bool,
    /// `doc.max_sort_bytes`.
    pub max_sort_bytes: usize,
    /// `doc.agg_stage_memory_bytes`.
    pub agg_stage_bytes: usize,
    /// `doc.update_retry_max`.
    pub update_retry_max: u32,
    /// `doc.max_array_paths`.
    pub max_array_paths: usize,
    /// Rows per internal read.
    pub chunk_rows: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            type_map: true,
            max_sort_bytes: 104_857_600,
            agg_stage_bytes: 104_857_600,
            update_retry_max: 10,
            max_array_paths: 1000,
            chunk_rows: 1000,
        }
    }
}

/// A namespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ns {
    /// The logical database.
    pub db: String,
    /// The collection.
    pub coll: String,
}

impl std::fmt::Display for Ns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.db, self.coll)
    }
}

static HLC: AtomicU64 = AtomicU64::new(0);

/// The next HLC stamp for `_v`: wall-clock milliseconds in the high bits
/// and a counter in the low 16, never going backwards.
#[must_use]
pub fn hlc_now() -> u64 {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(0));
    let wall = ms << 16;
    let mut cur = HLC.load(AtOrd::SeqCst);
    loop {
        let next = wall.max(cur + 1);
        match HLC.compare_exchange(cur, next, AtOrd::SeqCst, AtOrd::SeqCst) {
            Ok(_) => return next,
            Err(c) => cur = c,
        }
    }
}

static FEED_HLC: AtomicU64 = AtomicU64::new(0);

/// Chapter 38's statement comment for a whole-document replacement:
/// `/*sprig h=<HLC> o=r*/`, the HLC in the feed's layout (seconds in the
/// high 32 bits, a counter in the low 32), never going backwards.
#[must_use]
pub fn replace_comment() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let wall = secs << 32;
    let mut cur = FEED_HLC.load(AtOrd::SeqCst);
    let h = loop {
        let next = wall.max(cur + 1);
        match FEED_HLC.compare_exchange(cur, next, AtOrd::SeqCst, AtOrd::SeqCst) {
            Ok(_) => break next,
            Err(c) => cur = c,
        }
    };
    format!("/*sprig h={h:016x} o=r*/ ")
}

fn now_date() -> DateTime {
    DateTime::now()
}

fn text(c: Option<&Vec<u8>>) -> String {
    c.as_ref()
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default()
}

/// Validates a collection name (`InvalidNamespace`).
///
/// # Errors
/// 73 for empty names, `$`, NUL and `system.` names.
pub fn check_coll_name(name: &str) -> Result<(), MongoError> {
    if name.is_empty() || name.contains('\0') || name.contains('$') || name.len() > 255 {
        return Err(MongoError::new(
            code::INVALID_NAMESPACE,
            format!("Invalid collection name: {name}"),
        ));
    }
    if name.starts_with("system.") {
        return Err(MongoError::new(
            code::INVALID_NAMESPACE,
            format!("Invalid system namespace: {name}"),
        ));
    }
    Ok(())
}

/// Validates a database name.
///
/// # Errors
/// 73 for names MongoDB refuses.
pub fn check_db_name(name: &str) -> Result<(), MongoError> {
    if name.is_empty() || name.len() > 64 || name.contains(['/', '\\', '.', ' ', '"', '$', '\0']) {
        return Err(MongoError::new(
            code::INVALID_NAMESPACE,
            format!("Invalid database name: '{name}'"),
        ));
    }
    Ok(())
}

/// Every path where `d` holds an array (positions removed).
#[must_use]
pub fn array_paths(d: &Document) -> BTreeSet<String> {
    fn walk(v: &Bson, path: &str, out: &mut BTreeSet<String>) {
        match v {
            Bson::Document(d) => {
                for (k, x) in d {
                    let p = if path.is_empty() {
                        k.clone()
                    } else {
                        format!("{path}.{k}")
                    };
                    walk(x, &p, out);
                }
            }
            Bson::Array(a) => {
                out.insert(path.to_owned());
                for e in a {
                    walk(e, path, out);
                }
            }
            _ => {}
        }
    }
    let mut out = BTreeSet::new();
    for (k, v) in d {
        walk(v, k, &mut out);
    }
    out
}

/// A document rendered for insert, or why it couldn't be.
type Rendered = (usize, Result<(json::Stored, Bson), MongoError>);

/// What a find asks for.
#[derive(Debug, Clone, Default)]
pub struct FindSpec {
    /// The filter.
    pub filter: Document,
    /// The sort.
    pub sort: Option<Document>,
    /// The projection.
    pub projection: Option<Document>,
    /// Documents to skip.
    pub skip: u64,
    /// Documents to return at most; 0 for no limit.
    pub limit: u64,
}

#[derive(Debug)]
enum PlanKind {
    Keyset {
        sql: SqlPred,
        desc: bool,
        last: Option<Vec<u8>>,
        to_skip: u64,
        done: bool,
    },
    Buffer(VecDeque<Document>),
}

/// A resumable plan, held by the cursor between batches.
#[derive(Debug)]
pub struct CursorPlan {
    ns: Ns,
    table: String,
    pred: Pred,
    projection: Option<Projection>,
    remaining: Option<u64>,
    kind: PlanKind,
    /// Post-processing stages of an aggregation, already applied to a
    /// buffer.
    pub exhausted: bool,
}

impl CursorPlan {
    /// The namespace.
    #[must_use]
    pub fn ns(&self) -> &Ns {
        &self.ns
    }

    /// A plan over a ready list (aggregation results, `listCollections`).
    #[must_use]
    pub fn from_docs(ns: Ns, docs: Vec<Document>) -> Self {
        Self {
            ns,
            table: String::new(),
            pred: Pred::True,
            projection: None,
            remaining: None,
            exhausted: docs.is_empty(),
            kind: PlanKind::Buffer(docs.into()),
        }
    }

    /// Bytes held in the buffer (for the cursor memory budget).
    #[must_use]
    pub fn buffered_bytes(&self) -> usize {
        match &self.kind {
            PlanKind::Buffer(b) => b
                .iter()
                .map(|d| bson::to_vec(d).map_or(0, |v| v.len()))
                .sum(),
            PlanKind::Keyset { .. } => 0,
        }
    }
}

/// The document engine: stateless apart from its settings and where it
/// keeps collection metadata.
#[derive(Debug, Clone)]
pub struct Engine {
    /// Settings.
    pub cfg: EngineConfig,
    /// Collection metadata.
    pub catalog: Arc<dyn DocCatalog>,
}

impl Default for Engine {
    fn default() -> Self {
        Self::new(EngineConfig::default())
    }
}

impl Engine {
    /// An engine with `cfg`, keeping metadata in each database's hidden
    /// table ([`TableCatalog`]).
    #[must_use]
    pub fn new(cfg: EngineConfig) -> Self {
        Self::with_catalog(cfg, Arc::new(TableCatalog))
    }

    /// An engine with `cfg` over `catalog`.
    #[must_use]
    pub fn with_catalog(cfg: EngineConfig, catalog: Arc<dyn DocCatalog>) -> Self {
        Self { cfg, catalog }
    }

    fn opts(&self) -> RenderOpts {
        RenderOpts {
            type_map: self.cfg.type_map,
        }
    }

    /// The collection's metadata, `None` when it doesn't exist.
    ///
    /// # Errors
    /// SQL errors other than a missing database or table.
    pub async fn meta(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
    ) -> Result<Option<CollMeta>, MongoError> {
        self.catalog.get(sql, ns).await
    }

    /// Creates the database (if needed) and its metadata table.
    ///
    /// # Errors
    /// SQL errors (privileges).
    pub async fn ensure_db(&self, sql: &mut dyn SqlRunner, db: &str) -> Result<(), MongoError> {
        check_db_name(db)?;
        // Retryable-write records live next to the collections (chapter
        // 30); creating their table also tells whether the database exists.
        match sql.exec(&crate::sessions::table_sql(db)).await {
            Ok(_) => {}
            Err(e) if e.is(1049) => {
                sql.exec(&format!("CREATE DATABASE IF NOT EXISTS {}", ident(db)))
                    .await
                    .map_err(|e| mongo_error(&e))?;
                sql.exec(&crate::sessions::table_sql(db))
                    .await
                    .map_err(|e| mongo_error(&e))?;
            }
            Err(e) => return Err(mongo_error(&e)),
        }
        self.catalog.ensure_db(sql, db).await
    }

    /// Creates a collection; `explicit` makes an existing one an error
    /// (48 `NamespaceExists`).
    ///
    /// # Errors
    /// 48; 73; SQL errors.
    pub async fn create(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        options: Document,
        explicit: bool,
    ) -> Result<CollMeta, MongoError> {
        check_coll_name(&ns.coll)?;
        if let Some(m) = self.meta(sql, ns).await? {
            // MongoDB 8.0 answers ok when the options are the same.
            let same = options
                .iter()
                .filter(|(k, _)| !k.starts_with('$') && k.as_str() != "create")
                .all(|(k, v)| m.options.get(k) == Some(v));
            if explicit && !same {
                return Err(MongoError::new(
                    code::NAMESPACE_EXISTS,
                    format!("Collection {ns} already exists."),
                ));
            }
            return Ok(m);
        }
        for k in options.keys() {
            if matches!(
                k.as_str(),
                "capped"
                    | "timeseries"
                    | "viewOn"
                    | "pipeline"
                    | "clusteredIndex"
                    | "encryptedFields"
            ) {
                if k == "capped" && !matches!(options.get(k), Some(Bson::Boolean(true))) {
                    continue;
                }
                return Err(MongoError::new(
                    code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                    format!("create with '{k}' is not supported in this release"),
                ));
            }
        }
        self.ensure_db(sql, &ns.db).await?;
        let table = meta::table_name(&ns.coll);
        sql.exec(&meta::create_table_sql(&ns.db, &table))
            .await
            .map_err(|e| mongo_error(&e))?;
        let mut uuid = [0u8; 16];
        uuid[..12].copy_from_slice(&ObjectId::new().bytes());
        uuid[12..].copy_from_slice(
            &u32::try_from(meta::fnv64(ns.coll.as_bytes()) & 0xFFFF_FFFF)
                .unwrap_or(0)
                .to_be_bytes(),
        );
        let m = CollMeta {
            name: ns.coll.clone(),
            table,
            uuid,
            array_paths: BTreeSet::new(),
            arrays_full: false,
            pending_paths: BTreeSet::new(),
            indexes: Vec::new(),
            options: options
                .into_iter()
                .filter(|(k, _)| !k.starts_with('$') && k != "create" && k != "lsid")
                .collect(),
            next_index: 1,
        };
        match self.catalog.register(sql, ns, &m).await? {
            None => Ok(m),
            Some(_) if explicit => Err(MongoError::new(
                code::NAMESPACE_EXISTS,
                format!("Collection {ns} already exists."),
            )),
            Some(x) => Ok(x),
        }
    }

    async fn meta_or_create(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
    ) -> Result<CollMeta, MongoError> {
        match self.meta(sql, ns).await? {
            Some(m) => Ok(m),
            None => self.create(sql, ns, Document::new(), false).await,
        }
    }

    /// Drops a collection. Returns whether it existed.
    ///
    /// # Errors
    /// SQL errors.
    pub async fn drop(&self, sql: &mut dyn SqlRunner, ns: &Ns) -> Result<bool, MongoError> {
        let Some(m) = self.meta(sql, ns).await? else {
            return Ok(false);
        };
        sql.exec(&format!(
            "DROP TABLE IF EXISTS {}.{}",
            ident(&ns.db),
            ident(&m.table)
        ))
        .await
        .map_err(|e| mongo_error(&e))?;
        self.catalog.unregister(sql, ns).await?;
        Ok(true)
    }

    /// Drops a database.
    ///
    /// # Errors
    /// SQL errors other than a missing database.
    pub async fn drop_database(&self, sql: &mut dyn SqlRunner, db: &str) -> Result<(), MongoError> {
        match sql
            .exec(&format!("DROP DATABASE IF EXISTS {}", ident(db)))
            .await
        {
            Ok(_) => self.catalog.dropped_db(sql, db).await,
            Err(e) if e.is(1008) || e.is(1049) => Ok(()),
            Err(e) => Err(mongo_error(&e)),
        }
    }

    /// The databases the user sees, with their collection counts.
    ///
    /// # Errors
    /// SQL errors.
    pub async fn list_databases(&self, sql: &mut dyn SqlRunner) -> Result<Vec<String>, MongoError> {
        let rows = sql
            .query("SHOW DATABASES")
            .await
            .map_err(|e| mongo_error(&e))?;
        Ok(rows
            .iter()
            .map(|r| text(r[0].as_ref()))
            .filter(|d| {
                !matches!(
                    d.as_str(),
                    "information_schema" | "performance_schema" | "mysql" | "sys" | "sprig"
                )
            })
            .collect())
    }

    /// `listCollections` entries of `db`.
    ///
    /// # Errors
    /// SQL errors.
    pub async fn list_collections(
        &self,
        sql: &mut dyn SqlRunner,
        db: &str,
        filter_doc: &Document,
        name_only: bool,
    ) -> Result<Vec<Document>, MongoError> {
        let list = self.catalog.list(sql, db).await?;
        let pred = filter::parse(filter_doc)?;
        let mut out = Vec::new();
        for m in list {
            let d = if name_only {
                doc! {"name": m.name.clone(), "type": "collection"}
            } else {
                doc! {
                    "name": m.name.clone(), "type": "collection", "options": m.options.clone(),
                    "info": {"readOnly": false, "uuid": bson::Binary{subtype: bson::spec::BinarySubtype::Uuid, bytes: m.uuid.to_vec()}},
                    "idIndex": {"v": 2, "key": {"_id": 1}, "name": "_id_"},
                }
            };
            if pred.matches(&d) {
                out.push(d);
            }
        }
        Ok(out)
    }

    /// Adds `paths` to the registry before a write that holds them.
    async fn register_arrays(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &mut CollMeta,
        paths: &BTreeSet<String>,
    ) -> Result<(), MongoError> {
        let new: Vec<&String> = paths
            .iter()
            .filter(|p| !m.arrays_full && !m.array_paths.contains(*p))
            .collect();
        // Paths registered by an earlier write whose publication wasn't
        // confirmed yet: this write waits for it too.
        let unconfirmed: Vec<&String> = if paths.is_empty() {
            Vec::new()
        } else if m.arrays_full && m.pending_paths.contains("*") {
            paths.iter().collect()
        } else {
            paths
                .iter()
                .filter(|p| m.pending_paths.contains(*p))
                .collect()
        };
        if new.is_empty() && unconfirmed.is_empty() {
            return Ok(());
        }
        let full = !m.arrays_full && m.array_paths.len() + new.len() > self.cfg.max_array_paths;
        let set: BTreeSet<String> = new
            .iter()
            .chain(unconfirmed.iter())
            .map(|p| (*p).clone())
            .collect();
        self.catalog.add_array_paths(sql, ns, &set, full).await?;
        if full {
            m.arrays_full = true;
        } else {
            m.array_paths.extend(new.into_iter().cloned());
        }
        for p in &set {
            m.pending_paths.remove(p);
        }
        if m.arrays_full {
            m.pending_paths.remove("*");
        }
        Ok(())
    }

    /// Inserts documents (each already checked with
    /// [`json::check_raw`]). Returns the count inserted and the write
    /// errors by index.
    ///
    /// # Errors
    /// Errors that stop the whole command (metadata, privileges).
    pub async fn insert(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        docs: Vec<Document>,
        ordered: bool,
    ) -> Result<(u64, Vec<(usize, MongoError)>), MongoError> {
        check_coll_name(&ns.coll)?;
        let mut m = self.meta_or_create(sql, ns).await?;
        let mut rows: Vec<Rendered> = Vec::new();
        let mut paths = BTreeSet::new();
        for (i, mut d) in docs.into_iter().enumerate() {
            if !d.contains_key("_id") {
                let mut n = doc! {"_id": ObjectId::new()};
                n.extend(d);
                d = n;
            }
            let id = d.get("_id").cloned().unwrap_or(Bson::Null);
            let r = json::to_stored(&d, &self.opts());
            if r.is_ok() {
                paths.extend(array_paths(&d));
            }
            rows.push((i, r.map(|s| (s, id))));
            if ordered && rows.last().is_some_and(|r| r.1.is_err()) {
                break;
            }
        }
        self.register_arrays(sql, ns, &mut m, &paths).await?;
        let mut errors = Vec::new();
        let mut n = 0u64;
        let good: Vec<&(json::Stored, Bson)> =
            rows.iter().filter_map(|(_, r)| r.as_ref().ok()).collect();
        let all_good = good.len() == rows.len();
        // One multi-row INSERT when every document rendered; on a
        // duplicate, row by row to attribute the error.
        if all_good && !good.is_empty() {
            let values: Vec<String> = good.iter().map(|(s, _)| Self::row_values(s)).collect();
            let stmt = format!(
                "INSERT INTO {}.{} (_id, doc, _types, _v) VALUES {}",
                ident(&ns.db),
                ident(&m.table),
                values.join(", ")
            );
            match sql.exec(&stmt).await {
                Ok(d) => return Ok((d.affected, errors)),
                Err(e) if e.is(1062) && good.len() > 1 => {}
                Err(e) if e.is(1062) => {
                    errors.push((0, dup_error(&e, &good[0].1)));
                    return Ok((0, errors));
                }
                Err(e) => return Err(mongo_error(&e)),
            }
        }
        for (i, r) in rows {
            match r {
                Err(e) => {
                    errors.push((i, e));
                    if ordered {
                        break;
                    }
                }
                Ok((s, id)) => {
                    let stmt = format!(
                        "INSERT INTO {}.{} (_id, doc, _types, _v) VALUES {}",
                        ident(&ns.db),
                        ident(&m.table),
                        Self::row_values(&s)
                    );
                    match sql.exec(&stmt).await {
                        Ok(_) => n += 1,
                        Err(e) if e.is(1062) => {
                            errors.push((i, dup_error(&e, &id)));
                            if ordered {
                                break;
                            }
                        }
                        Err(e) => return Err(mongo_error(&e)),
                    }
                }
            }
        }
        Ok((n, errors))
    }

    fn row_values(s: &json::Stored) -> String {
        format!(
            "({}, {}, {}, {})",
            bin_lit(&s.id_key),
            str_lit(&s.doc),
            s.types
                .as_deref()
                .map_or_else(|| "NULL".to_owned(), str_lit),
            hlc_now()
        )
    }

    /// Plans a find.
    ///
    /// # Errors
    /// Parse errors; SQL errors; 292 past `doc.max_sort_bytes`.
    pub async fn find(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        spec: FindSpec,
    ) -> Result<CursorPlan, MongoError> {
        let pred = filter::parse(&spec.filter)?;
        let projection = spec
            .projection
            .as_ref()
            .filter(|p| !p.is_empty())
            .map(Projection::parse)
            .transpose()?;
        let sort = spec
            .sort
            .as_ref()
            .map(agg::parse_sort)
            .transpose()?
            .unwrap_or_default();
        let Some(m) = self.meta(sql, ns).await? else {
            return Ok(CursorPlan::from_docs(ns.clone(), Vec::new()));
        };
        // Chapter 36 (R6 round 3): `$near` sorts by distance.
        models::check_near(&pred, true)?;
        if let Some((path, near)) = models::near_of(&pred) {
            return Box::pin(self.find_near(sql, ns, &spec, &pred, &path, &near, &m)).await;
        }
        let sqlp = translate(&pred, &m);
        let remaining = (spec.limit > 0).then_some(spec.limit);
        let keyset = sort.is_empty() || (sort.len() == 1 && sort[0].0 == "_id");
        if keyset {
            return Ok(CursorPlan {
                ns: ns.clone(),
                table: m.table.clone(),
                pred,
                projection,
                remaining,
                exhausted: false,
                kind: PlanKind::Keyset {
                    sql: sqlp,
                    desc: sort.first().is_some_and(|s| s.1 < 0),
                    last: None,
                    to_skip: spec.skip,
                    done: false,
                },
            });
        }
        let mut docs = self
            .scan(sql, ns, &m.table, &pred, &sqlp, self.cfg.max_sort_bytes)
            .await?;
        crate::heavy::run(docs.len(), || agg::sort_docs(&mut docs, &sort));
        let skip = usize::try_from(spec.skip).unwrap_or(usize::MAX);
        let docs: Vec<Document> = docs
            .into_iter()
            .skip(skip)
            .take(remaining.map_or(usize::MAX, |r| usize::try_from(r).unwrap_or(usize::MAX)))
            .collect();
        let docs = match &projection {
            Some(p) => docs
                .iter()
                .map(|d| p.apply(d, now_date()))
                .collect::<Result<_, _>>()?,
            None => docs,
        };
        let mut plan = CursorPlan::from_docs(ns.clone(), docs);
        plan.table = m.table;
        Ok(plan)
    }

    /// Every matching document, in `_id` order, within `max_bytes`.
    async fn scan(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        table: &str,
        pred: &Pred,
        sqlp: &SqlPred,
        max_bytes: usize,
    ) -> Result<Vec<Document>, MongoError> {
        let mut out = Vec::new();
        let mut last: Option<Vec<u8>> = None;
        let mut bytes = 0usize;
        let cx = AggExpr { now: now_date() };
        loop {
            let rows = self
                .read_chunk(
                    sql,
                    ns,
                    table,
                    sqlp,
                    last.as_deref(),
                    false,
                    self.cfg.chunk_rows,
                )
                .await?;
            let n = rows.len();
            for (k, d) in rows {
                last = Some(k);
                if pred.matches_with(&d, &cx) {
                    bytes += bson::to_vec(&d).map_or(0, |v| v.len());
                    if bytes > max_bytes {
                        return Err(MongoError::new(
                            code::QUERY_EXCEEDED_MEMORY_LIMIT,
                            format!(
                                "Sort exceeded memory limit of {max_bytes} bytes, but did not opt in to external sorting (doc.max_sort_bytes)."
                            ),
                        ));
                    }
                    out.push(d);
                }
            }
            if n < self.cfg.chunk_rows {
                return Ok(out);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_chunk(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        table: &str,
        sqlp: &SqlPred,
        after: Option<&[u8]>,
        desc: bool,
        n: usize,
    ) -> Result<Vec<(Vec<u8>, Document)>, MongoError> {
        let mut cond = sqlp.sql.clone();
        if let Some(k) = after {
            cond = format!(
                "({cond}) AND _id {} {}",
                if desc { "<" } else { ">" },
                bin_lit(k)
            );
        }
        let q = format!(
            "SELECT _id, doc, _types FROM {}.{} WHERE {cond} ORDER BY _id{} LIMIT {n}",
            ident(&ns.db),
            ident(table),
            if desc { " DESC" } else { "" }
        );
        let rows = match sql.query(&q).await {
            Ok(r) => r,
            Err(e) if e.is(1146) => return Ok(Vec::new()),
            Err(e) => return Err(mongo_error(&e)),
        };
        rows.into_iter()
            .map(|r| {
                let key = r.first().cloned().flatten().unwrap_or_default();
                let d = json::to_bson(
                    &text(r[1].as_ref()),
                    r.get(2)
                        .and_then(Option::as_ref)
                        .map(|b| String::from_utf8_lossy(b))
                        .as_deref(),
                )?;
                Ok((key, d))
            })
            .collect()
    }

    /// The next batch of up to `n` documents (and `max_bytes` of BSON),
    /// and whether the plan is exhausted.
    ///
    /// # Errors
    /// SQL errors.
    pub async fn next_batch(
        &self,
        sql: &mut dyn SqlRunner,
        plan: &mut CursorPlan,
        n: usize,
        max_bytes: usize,
    ) -> Result<Vec<Document>, MongoError> {
        let mut want = n.max(1);
        if let Some(r) = plan.remaining {
            want = want.min(usize::try_from(r).unwrap_or(usize::MAX));
        }
        let mut out = Vec::new();
        let mut bytes = 0usize;
        let now = now_date();
        let cx = AggExpr { now };
        match &mut plan.kind {
            PlanKind::Buffer(b) => {
                while out.len() < want {
                    let Some(d) = b.pop_front() else { break };
                    bytes += bson::to_vec(&d).map_or(0, |v| v.len());
                    out.push(d);
                    if bytes >= max_bytes {
                        break;
                    }
                }
                if b.is_empty() {
                    plan.exhausted = true;
                }
            }
            PlanKind::Keyset {
                sql: sqlp,
                desc,
                last,
                to_skip,
                done,
            } => {
                while out.len() < want && !*done {
                    let chunk = if sqlp.exact {
                        (want - out.len()) + usize::try_from(*to_skip).unwrap_or(0)
                    } else {
                        ((want - out.len()) * 2).max(101)
                    }
                    .min(self.cfg.chunk_rows.max(want));
                    let rows = self
                        .read_chunk(
                            sql,
                            &plan.ns,
                            &plan.table,
                            sqlp,
                            last.as_deref(),
                            *desc,
                            chunk,
                        )
                        .await?;
                    let short = rows.len() < chunk;
                    let total = rows.len();
                    let mut used = 0;
                    for (k, d) in rows {
                        used += 1;
                        *last = Some(k);
                        if !plan.pred.matches_with(&d, &cx) {
                            continue;
                        }
                        if *to_skip > 0 {
                            *to_skip -= 1;
                            continue;
                        }
                        let d = match &plan.projection {
                            Some(p) => p.apply(&d, now)?,
                            None => d,
                        };
                        bytes += bson::to_vec(&d).map_or(0, |v| v.len());
                        out.push(d);
                        if out.len() >= want || bytes >= max_bytes {
                            break;
                        }
                    }
                    if short && used == total {
                        *done = true;
                    }
                    if bytes >= max_bytes {
                        break;
                    }
                }
                if *done && out.len() < want {
                    plan.exhausted = true;
                }
            }
        }
        if let Some(r) = plan.remaining.as_mut() {
            *r = r.saturating_sub(out.len() as u64);
            if *r == 0 {
                plan.exhausted = true;
            }
        }
        Ok(out)
    }

    /// Whether a keyset plan can't return more without another read.
    #[must_use]
    pub fn plan_done(plan: &CursorPlan) -> bool {
        plan.exhausted
    }

    /// `count`: documents matching `filter`, after `skip`, up to `limit`.
    ///
    /// # Errors
    /// SQL errors.
    pub async fn count(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        filter_doc: &Document,
        skip: u64,
        limit: u64,
    ) -> Result<u64, MongoError> {
        let pred = filter::parse(filter_doc)?;
        models::check_near(&pred, false)?;
        let Some(m) = self.meta(sql, ns).await? else {
            return Ok(0);
        };
        let sqlp = translate(&pred, &m);
        let total = if sqlp.exact {
            let q = format!(
                "SELECT COUNT(*) FROM {}.{} WHERE {}",
                ident(&ns.db),
                ident(&m.table),
                sqlp.sql
            );
            let rows = sql.query(&q).await.map_err(|e| mongo_error(&e))?;
            rows.first()
                .map(|r| text(r[0].as_ref()))
                .and_then(|t| t.parse().ok())
                .unwrap_or(0)
        } else {
            let mut n = 0u64;
            let mut last = None;
            let cx = AggExpr { now: now_date() };
            loop {
                let rows = self
                    .read_chunk(
                        sql,
                        ns,
                        &m.table,
                        &sqlp,
                        last.as_deref(),
                        false,
                        self.cfg.chunk_rows,
                    )
                    .await?;
                let k = rows.len();
                for (key, d) in rows {
                    last = Some(key);
                    if pred.matches_with(&d, &cx) {
                        n += 1;
                    }
                }
                if k < self.cfg.chunk_rows {
                    break;
                }
            }
            n
        };
        let after = total.saturating_sub(skip);
        Ok(if limit > 0 { after.min(limit) } else { after })
    }

    /// `distinct`.
    ///
    /// # Errors
    /// SQL errors; 292.
    pub async fn distinct(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        key: &str,
        filter_doc: &Document,
    ) -> Result<Vec<Bson>, MongoError> {
        let pred = filter::parse(filter_doc)?;
        let Some(m) = self.meta(sql, ns).await? else {
            return Ok(Vec::new());
        };
        let sqlp = translate(&pred, &m);
        let docs = self
            .scan(sql, ns, &m.table, &pred, &sqlp, self.cfg.max_sort_bytes)
            .await?;
        let mut seen: Vec<(Vec<u8>, Bson)> = Vec::new();
        for d in &docs {
            let vs = filter::values_at(d, key);
            for v in vs.leaves {
                let items: Vec<&Bson> = match v {
                    Bson::Array(a) => a.iter().collect(),
                    other => vec![other],
                };
                for x in items {
                    let k = crate::keyenc::key(x);
                    if !seen.iter().any(|(s, _)| *s == k) {
                        seen.push((k, x.clone()));
                    }
                }
            }
        }
        seen.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(seen.into_iter().map(|(_, v)| v).collect())
    }

    /// One update statement: `(matched, modified, upserted _id)`.
    ///
    /// # Errors
    /// Update errors (per statement); SQL errors.
    #[allow(clippy::too_many_arguments)]
    pub async fn update(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        q: &Document,
        u: &Bson,
        multi: bool,
        upsert: bool,
        array_filters: &[Document],
        sort: Option<&Document>,
    ) -> Result<UpdateOutcome, MongoError> {
        let spec = update::parse(u)?;
        if multi && matches!(spec, UpdateSpec::Replace(_)) {
            return Err(MongoError::new(
                code::FAILED_TO_PARSE,
                "multi update is not supported for replacement-style update",
            ));
        }
        let pred = filter::parse(q)?;
        let afs = parse_array_filters(array_filters)?;
        let mut m = match self.meta(sql, ns).await? {
            Some(m) => m,
            None if upsert => self.create(sql, ns, Document::new(), false).await?,
            None => return Ok(UpdateOutcome::default()),
        };
        let sqlp = translate(&pred, &m);
        let now = now_date();
        let cx = ApplyCx {
            query: &pred,
            array_filters: &afs,
            inserting: false,
            now,
        };
        let mut out = UpdateOutcome::default();
        let targets: Vec<Document> = if multi {
            self.scan(sql, ns, &m.table, &pred, &sqlp, usize::MAX)
                .await?
        } else {
            self.first_match(sql, ns, &m, &pred, &sqlp, sort)
                .await?
                .into_iter()
                .collect()
        };
        for old in targets {
            out.matched += 1;
            let (changed, new_doc, old_doc) = self
                .update_one_rmw(sql, ns, &mut m, old, &spec, &cx, &pred)
                .await?;
            if changed {
                out.modified += 1;
            }
            out.last_new = new_doc;
            out.last_old = old_doc;
        }
        if out.matched == 0 && upsert {
            let mut d = match &spec {
                UpdateSpec::Replace(_) => {
                    let mut s = Document::new();
                    if let Some(ids) = pred.id_values()
                        && ids.len() == 1
                    {
                        s.insert("_id", ids[0].clone());
                    }
                    s
                }
                _ => update::upsert_seed(&pred)?,
            };
            let icx = ApplyCx {
                inserting: true,
                ..cx
            };
            update::apply(&mut d, &spec, &icx)?;
            if !d.contains_key("_id") {
                let mut n = doc! {"_id": ObjectId::new()};
                n.extend(d);
                d = n;
            }
            let id = d.get("_id").cloned().unwrap_or(Bson::Null);
            let (n, errs) = self.insert(sql, ns, vec![d.clone()], true).await?;
            if let Some((_, e)) = errs.into_iter().next() {
                return Err(e);
            }
            if n == 1 {
                out.upserted = Some(id);
                out.last_new = Some(d);
            }
        }
        Ok(out)
    }

    async fn first_match(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
        pred: &Pred,
        sqlp: &SqlPred,
        sort: Option<&Document>,
    ) -> Result<Option<Document>, MongoError> {
        let cx = AggExpr { now: now_date() };
        if let Some(s) = sort.filter(|s| !s.is_empty()) {
            let spec = agg::parse_sort(s)?;
            let mut docs = self
                .scan(sql, ns, &m.table, pred, sqlp, self.cfg.max_sort_bytes)
                .await?;
            crate::heavy::run(docs.len(), || agg::sort_docs(&mut docs, &spec));
            return Ok(docs.into_iter().next());
        }
        let mut last = None;
        loop {
            let chunk = if sqlp.exact { 1 } else { 101 };
            let rows = self
                .read_chunk(sql, ns, &m.table, sqlp, last.as_deref(), false, chunk)
                .await?;
            let k = rows.len();
            for (key, d) in rows {
                last = Some(key);
                if pred.matches_with(&d, &cx) {
                    return Ok(Some(d));
                }
            }
            if k < chunk {
                return Ok(None);
            }
        }
    }

    /// Reads `_v`, applies, writes back with the `_v` check; retries.
    #[allow(clippy::too_many_arguments)]
    async fn update_one_rmw(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &mut CollMeta,
        first: Document,
        spec: &UpdateSpec,
        cx: &ApplyCx<'_>,
        pred: &Pred,
    ) -> Result<(bool, Option<Document>, Option<Document>), MongoError> {
        let id = first.get("_id").cloned().unwrap_or(Bson::Null);
        let key = json::id_key(&id)?;
        for _ in 0..=self.cfg.update_retry_max {
            let q = format!(
                "SELECT doc, _types, _v FROM {}.{} WHERE _id = {}",
                ident(&ns.db),
                ident(&m.table),
                bin_lit(&key)
            );
            let rows = sql.query(&q).await.map_err(|e| mongo_error(&e))?;
            let Some(r) = rows.first() else {
                return Ok((false, None, None));
            };
            let old = json::to_bson(
                &text(r[0].as_ref()),
                r[1].as_ref().map(|b| String::from_utf8_lossy(b)).as_deref(),
            )?;
            if !pred.matches_with(&old, &AggExpr { now: cx.now }) {
                return Ok((false, None, None));
            }
            let v = text(r[2].as_ref());
            let mut new = old.clone();
            let changed = update::apply(&mut new, spec, cx)?;
            if !changed {
                return Ok((false, Some(new), Some(old)));
            }
            let s = json::to_stored(&new, &self.opts())?;
            if bson::to_vec(&new).map_or(0, |b| b.len()) > 16 * 1024 * 1024 {
                return Err(MongoError::new(
                    code::BSON_OBJECT_TOO_LARGE,
                    "Resulting document after update is larger than 16777216",
                ));
            }
            self.register_arrays(sql, ns, m, &array_paths(&new)).await?;
            // A replacement carries chapter 38's statement comment, so
            // change streams report it as `replace` (decisions/r5-interim.md
            // D14); other updates stay plain.
            let hint = if matches!(spec, UpdateSpec::Replace(_)) {
                replace_comment()
            } else {
                String::new()
            };
            let stmt = format!(
                "{hint}UPDATE {}.{} SET _v = {}, doc = {}, _types = {} WHERE _id = {} AND _v = {v}",
                ident(&ns.db),
                ident(&m.table),
                hlc_now(),
                str_lit(&s.doc),
                s.types
                    .as_deref()
                    .map_or_else(|| "NULL".to_owned(), str_lit),
                bin_lit(&key)
            );
            match sql.exec(&stmt).await {
                Ok(d) if d.affected > 0 || d.info.contains("Rows matched: 1") => {
                    return Ok((true, Some(new), Some(old)));
                }
                Ok(_) => {}
                Err(e) if e.is(1062) => return Err(dup_error(&e, &id)),
                Err(e) => return Err(mongo_error(&e)),
            }
        }
        Err(MongoError::new(
            code::WRITE_CONFLICT,
            "the document kept changing (doc.update_retry_max)",
        )
        .label("TransientTransactionError"))
    }

    /// One delete statement; `limit` 1 deletes at most one document.
    ///
    /// # Errors
    /// SQL errors.
    pub async fn delete(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        q: &Document,
        limit_one: bool,
        sort: Option<&Document>,
    ) -> Result<(u64, Option<Document>), MongoError> {
        let pred = filter::parse(q)?;
        let Some(m) = self.meta(sql, ns).await? else {
            return Ok((0, None));
        };
        let sqlp = translate(&pred, &m);
        if !limit_one && sqlp.exact && !matches!(pred, Pred::Expr(_)) {
            let stmt = format!(
                "DELETE FROM {}.{} WHERE {}",
                ident(&ns.db),
                ident(&m.table),
                sqlp.sql
            );
            let d = sql.exec(&stmt).await.map_err(|e| mongo_error(&e))?;
            return Ok((d.affected, None));
        }
        let docs: Vec<Document> = if limit_one {
            self.first_match(sql, ns, &m, &pred, &sqlp, sort)
                .await?
                .into_iter()
                .collect()
        } else {
            self.scan(sql, ns, &m.table, &pred, &sqlp, usize::MAX)
                .await?
        };
        let mut n = 0;
        let mut last = None;
        for chunk in docs.chunks(500) {
            let keys: Vec<String> = chunk
                .iter()
                .filter_map(|d| d.get("_id").and_then(|i| json::id_key(i).ok()))
                .map(|k| bin_lit(&k))
                .collect();
            if keys.is_empty() {
                continue;
            }
            let stmt = format!(
                "DELETE FROM {}.{} WHERE _id IN ({})",
                ident(&ns.db),
                ident(&m.table),
                keys.join(", ")
            );
            n += sql.exec(&stmt).await.map_err(|e| mongo_error(&e))?.affected;
            last = chunk.last().cloned();
        }
        Ok((n, if limit_one { last } else { None }))
    }

    /// `createIndexes`. Returns `(before, after, created_collection)`.
    ///
    /// # Errors
    /// 67, 85, 86 for bad or conflicting specs; 11000 when a unique index
    /// meets duplicates; 39662 for text and geo indexes.
    pub async fn create_indexes(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        specs: &[Document],
        migrated: bool,
    ) -> Result<(usize, usize, bool), MongoError> {
        let existed = self.meta(sql, ns).await?.is_some();
        let mut m = self.meta_or_create(sql, ns).await?;
        let before = m.indexes.len() + 1;
        for s in specs {
            let key = s.get_document("key").map_err(|_| {
                MongoError::new(
                    code::FAILED_TO_PARSE,
                    "The 'key' field is a required property of an index specification",
                )
            })?;
            if key.is_empty() {
                return Err(MongoError::new(
                    code::CANNOT_CREATE_INDEX,
                    "Index keys cannot be empty.",
                ));
            }
            // Chapter 36 (R6 round 3): a single-path `2dsphere` index.
            let geo_path = Self::geo_key(key)?;
            let mut pattern = Vec::new();
            for (p, v) in key {
                let dir = match v {
                    Bson::String(t) if t == "hashed" => 1,
                    Bson::String(t) if t == "2dsphere" => 1,
                    Bson::String(t) => {
                        return Err(MongoError::new(
                            code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                            format!(
                                "'{t}' indexes are not supported in this release (chapter 39 and 36)"
                            ),
                        ));
                    }
                    other => match filter::as_i64(other) {
                        Some(n) if n > 0 => 1,
                        Some(n) if n < 0 => -1,
                        _ => {
                            return Err(MongoError::new(
                                code::CANNOT_CREATE_INDEX,
                                format!("Values in the index key pattern can't be 0: {key}"),
                            ));
                        }
                    },
                };
                if !crate::sql::valid_path(p)
                    || p.split('.').any(|x| x.bytes().all(|b| b.is_ascii_digit()))
                {
                    return Err(MongoError::new(
                        code::CANNOT_CREATE_INDEX,
                        format!("Index key path '{p}' is not supported"),
                    ));
                }
                pattern.push((p.clone(), dir));
            }
            let name = s
                .get_str("name")
                .map_or_else(|_| meta::default_index_name(key), str::to_owned);
            if name == "_id_" || pattern == [("_id".to_owned(), 1)] {
                continue;
            }
            let unique = s.get_bool("unique").unwrap_or(false);
            if let Some(ex) = m.indexes.iter().find(|i| i.name == name) {
                if ex.key == pattern && ex.unique == unique {
                    continue;
                }
                return Err(MongoError::new(
                    code::INDEX_KEY_SPECS_CONFLICT,
                    format!(
                        "An existing index has the same name as the requested index. When index names are not specified, they are auto generated and can cause conflicts. Please refer to our documentation. Requested index: {s}, existing index: {}",
                        ex.spec()
                    ),
                ));
            }
            if let Some(ex) = m.indexes.iter().find(|i| i.key == pattern) {
                return Err(MongoError::new(
                    code::INDEX_OPTIONS_CONFLICT,
                    format!("Index already exists with a different name: {}", ex.name),
                ));
            }
            let extra: Document = s
                .iter()
                .filter(|(k, _)| {
                    !matches!(
                        k.as_str(),
                        "key"
                            | "name"
                            | "unique"
                            | "sparse"
                            | "expireAfterSeconds"
                            | "v"
                            | "background"
                            | "ns"
                            | "sprig"
                    )
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let mut extra = extra;
            if geo_path.is_some() && !extra.contains_key("2dsphereIndexVersion") {
                extra.insert("2dsphereIndexVersion", 3);
            }
            let idx = IndexDef {
                id: m.next_index,
                name,
                key: pattern.clone(),
                unique,
                sparse: s.get_bool("sparse").unwrap_or(false),
                expire_after: s.get("expireAfterSeconds").and_then(filter::as_i64),
                origin: if migrated {
                    "migrated".into()
                } else {
                    "user".into()
                },
                extra,
            };
            if let Some(p) = &geo_path {
                self.create_geo_index(sql, ns, &m, &idx, p).await?;
                m.indexes.push(idx);
                m.next_index += 1;
                self.catalog.save_indexes(sql, ns, &m).await?;
                continue;
            }
            for (p, _) in &pattern {
                if !m.has_columns(p) {
                    sql.exec(&meta::add_columns_sql(&ns.db, &m.table, p))
                        .await
                        .map_err(|e| mongo_error(&e))?;
                }
            }
            if let Err(e) = sql
                .exec(&meta::create_index_sql(&ns.db, &m.table, &idx))
                .await
            {
                if e.is(1062) {
                    return Err(MongoError::new(
                        code::DUPLICATE_KEY,
                        format!(
                            "Index build failed: E11000 duplicate key error collection: {ns} index: {}",
                            idx.name
                        ),
                    ));
                }
                return Err(mongo_error(&e));
            }
            m.indexes.push(idx);
            m.next_index += 1;
            self.catalog.save_indexes(sql, ns, &m).await?;
        }
        Ok((before, m.indexes.len() + 1, !existed))
    }

    /// `listIndexes`; `None` when the collection doesn't exist.
    ///
    /// # Errors
    /// SQL errors.
    pub async fn list_indexes(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
    ) -> Result<Option<Vec<Document>>, MongoError> {
        let Some(m) = self.meta(sql, ns).await? else {
            return Ok(None);
        };
        let mut out = vec![doc! {"v": 2, "key": {"_id": 1}, "name": "_id_"}];
        out.extend(m.indexes.iter().map(IndexDef::spec));
        Ok(Some(out))
    }

    /// `dropIndexes` with a name, a key pattern, a list of names or `*`.
    /// Returns the index count before.
    ///
    /// # Errors
    /// 26 for a missing collection; 27 for a missing index; 72 for `_id_`.
    pub async fn drop_indexes(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        which: &Bson,
    ) -> Result<usize, MongoError> {
        let Some(mut m) = self.meta(sql, ns).await? else {
            return Err(MongoError::new(
                code::NAMESPACE_NOT_FOUND,
                format!("ns not found {ns}"),
            ));
        };
        let before = m.indexes.len() + 1;
        let names: Vec<String> = match which {
            Bson::String(s) if s == "*" => m.indexes.iter().map(|i| i.name.clone()).collect(),
            Bson::String(s) => vec![s.clone()],
            Bson::Array(a) => a
                .iter()
                .filter_map(|x| x.as_str().map(str::to_owned))
                .collect(),
            Bson::Document(k) => {
                let pat: Vec<(String, i32)> = k
                    .iter()
                    .map(|(p, v)| {
                        (
                            p.clone(),
                            if filter::as_i64(v).unwrap_or(1) < 0 {
                                -1
                            } else {
                                1
                            },
                        )
                    })
                    .collect();
                match m.indexes.iter().find(|i| i.key == pat) {
                    Some(i) => vec![i.name.clone()],
                    None if pat == [("_id".to_owned(), 1)] => vec!["_id_".into()],
                    None => {
                        return Err(MongoError::new(
                            code::INDEX_NOT_FOUND,
                            format!("can't find index with key: {k}"),
                        ));
                    }
                }
            }
            _ => return Err(MongoError::new(code::BAD_VALUE, "invalid index name spec")),
        };
        for n in &names {
            if n == "_id_" {
                return Err(MongoError::new(
                    code::INVALID_OPTIONS,
                    "cannot drop _id index",
                ));
            }
            let Some(pos) = m.indexes.iter().position(|i| &i.name == n) else {
                return Err(MongoError::new(
                    code::INDEX_NOT_FOUND,
                    format!("index not found with name [{n}]"),
                ));
            };
            let idx = m.indexes.remove(pos);
            sql.exec(&meta::drop_index_sql(&ns.db, &m.table, &idx))
                .await
                .map_err(|e| mongo_error(&e))?;
            // Chapter 36 (R6 round 3): a `2dsphere` index's columns.
            self.drop_geo_columns(sql, ns, &m, &idx).await?;
            if crate::geo::is_2dsphere(&idx) {
                continue;
            }
            for (p, _) in &idx.key {
                if !m.has_columns(p) {
                    sql.exec(&meta::drop_columns_sql(&ns.db, &m.table, p))
                        .await
                        .map_err(|e| mongo_error(&e))?;
                }
            }
        }
        self.catalog.save_indexes(sql, ns, &m).await?;
        Ok(before)
    }

    /// `renameCollection` within one database.
    ///
    /// # Errors
    /// 26, 48, SQL errors.
    pub async fn rename(
        &self,
        sql: &mut dyn SqlRunner,
        from: &Ns,
        to: &Ns,
        drop_target: bool,
    ) -> Result<(), MongoError> {
        check_coll_name(&to.coll)?;
        let Some(mut m) = self.meta(sql, from).await? else {
            return Err(MongoError::new(
                code::NAMESPACE_NOT_FOUND,
                "source namespace does not exist",
            ));
        };
        if from.db != to.db {
            return Err(MongoError::new(
                code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                "renameCollection across databases is not supported in this release",
            ));
        }
        if self.meta(sql, to).await?.is_some() {
            if !drop_target {
                return Err(MongoError::new(
                    code::NAMESPACE_EXISTS,
                    "target namespace exists",
                ));
            }
            self.drop(sql, to).await?;
        }
        let table = meta::table_name(&to.coll);
        sql.exec(&format!(
            "RENAME TABLE {0}.{1} TO {0}.{2}",
            ident(&from.db),
            ident(&m.table),
            ident(&table)
        ))
        .await
        .map_err(|e| mongo_error(&e))?;
        m.table = table;
        self.catalog.rename(sql, from, to, &m).await?;
        Ok(())
    }

    /// Runs an aggregation: leading `$match` goes to SQL, the rest in the
    /// gateway; `$lookup` reads the foreign collection.
    ///
    /// # Errors
    /// Stage errors; SQL errors.
    pub async fn aggregate(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        pipeline: &[Document],
    ) -> Result<Vec<Document>, MongoError> {
        let now = now_date();
        let lim = AggLimits {
            stage_bytes: self.cfg.agg_stage_bytes,
            now,
        };
        // Chapters 34 and 36 (R6 round 3): `$geoNear`, `$vectorSearch` and
        // `$listSearchIndexes` read the collection themselves.
        let first = if ns.coll.is_empty() {
            None
        } else {
            Box::pin(self.first_stage(sql, ns, pipeline)).await?
        };
        let mut stages = if first.is_some() {
            &pipeline[1..]
        } else {
            pipeline
        };
        let mut conds: Vec<Bson> = Vec::new();
        while first.is_none()
            && let Some(s) = stages.first()
        {
            match s.get_document("$match") {
                Ok(m) if s.len() == 1 && !m.contains_key("$expr") => {
                    if !m.is_empty() {
                        conds.push(Bson::Document(m.clone()));
                    }
                    stages = &stages[1..];
                }
                _ => break,
            }
        }
        let leading = if conds.is_empty() {
            Document::new()
        } else {
            doc! {"$and": conds}
        };
        let mut docs = if let Some(d) = first {
            d
        } else if ns.coll.is_empty() {
            Vec::new()
        } else {
            let pred = filter::parse(&leading)?;
            models::check_near(&pred, false)?;
            match self.meta(sql, ns).await? {
                Some(m) => {
                    let sqlp = translate(&pred, &m);
                    self.scan(
                        sql,
                        ns,
                        &m.table,
                        &pred,
                        &sqlp,
                        self.cfg.agg_stage_bytes.saturating_mul(4),
                    )
                    .await?
                }
                None => Vec::new(),
            }
        };
        for s in stages {
            let name = s.keys().next().map_or("", String::as_str);
            docs = match name {
                "$lookup" => {
                    self.lookup(
                        sql,
                        ns,
                        docs,
                        s.get_document("$lookup").map_err(|_| {
                            MongoError::new(
                                code::FAILED_TO_PARSE,
                                "the $lookup specification must be an Object",
                            )
                        })?,
                        &lim,
                    )
                    .await?
                }
                "$unionWith" => {
                    let (coll, pipe) = match s.get("$unionWith") {
                        Some(Bson::String(c)) => (c.clone(), Vec::new()),
                        Some(Bson::Document(d)) => (
                            d.get_str("coll").unwrap_or("").to_owned(),
                            d.get_array("pipeline")
                                .map(|a| {
                                    a.iter().filter_map(|x| x.as_document().cloned()).collect()
                                })
                                .unwrap_or_default(),
                        ),
                        _ => return Err(MongoError::new(code::FAILED_TO_PARSE, "bad $unionWith")),
                    };
                    let other = Box::pin(self.aggregate(
                        sql,
                        &Ns {
                            db: ns.db.clone(),
                            coll,
                        },
                        &pipe,
                    ))
                    .await?;
                    docs.extend(other);
                    docs
                }
                "$out" | "$merge" => {
                    models::strip_scores(&mut docs);
                    let target = match s.get(name) {
                        Some(Bson::String(c)) => c.clone(),
                        Some(Bson::Document(d)) => d
                            .get_str("coll")
                            .or_else(|_| d.get_str("into"))
                            .unwrap_or("")
                            .to_owned(),
                        _ => String::new(),
                    };
                    let tdb = match s.get(name) {
                        Some(Bson::Document(d)) => match d.get("into") {
                            Some(Bson::Document(i)) => i.get_str("db").ok().map(str::to_owned),
                            _ => d.get_str("db").ok().map(str::to_owned),
                        },
                        _ => None,
                    };
                    let target = match s.get(name) {
                        Some(Bson::Document(d)) => match d.get("into") {
                            Some(Bson::Document(i)) => {
                                i.get_str("coll").unwrap_or_default().to_owned()
                            }
                            _ => target,
                        },
                        _ => target,
                    };
                    let tns = Ns {
                        db: tdb.unwrap_or_else(|| ns.db.clone()),
                        coll: target,
                    };
                    if name == "$out" {
                        self.drop(sql, &tns).await?;
                        let (_, errs) = self.insert(sql, &tns, docs, true).await?;
                        if let Some((_, e)) = errs.into_iter().next() {
                            return Err(e);
                        }
                    } else {
                        let spec = s.get_document(name).cloned().unwrap_or_default();
                        self.merge_into(sql, &tns, docs, &spec).await?;
                    }
                    Vec::new()
                }
                // A large set leaves the worker thread (heavy::HEAVY_DOCS).
                _ => crate::heavy::run(docs.len(), || agg::run_stage(docs, s, &lim))?,
            };
        }
        models::strip_scores(&mut docs);
        Ok(docs)
    }

    /// `$merge` on `_id`: `whenMatched` `merge` (the default), `replace`,
    /// `keepExisting` or `fail`; `whenNotMatched` `insert` (the default),
    /// `discard` or `fail`.
    async fn merge_into(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        docs: Vec<Document>,
        spec: &Document,
    ) -> Result<(), MongoError> {
        let matched = spec.get_str("whenMatched").unwrap_or("merge");
        let not_matched = spec.get_str("whenNotMatched").unwrap_or("insert");
        if spec.get("on").is_some_and(|o| o.as_str() != Some("_id")) {
            return Err(MongoError::new(
                code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                "$merge 'on' fields other than _id are not supported in this release",
            ));
        }
        for d in docs {
            let id = d.get("_id").cloned().unwrap_or(Bson::Null);
            let q = doc! {"_id": id.clone()};
            let exists = self.count(sql, ns, &q, 0, 1).await? > 0;
            match (exists, matched, not_matched) {
                (true, "keepExisting", _) | (false, _, "discard") => {}
                (true, "fail", _) => {
                    return Err(MongoError::new(
                        code::DUPLICATE_KEY,
                        format!("$merge found an existing document with _id {id}"),
                    ));
                }
                (false, _, "fail") => {
                    return Err(MongoError::new(
                        13113,
                        "$merge could not find a matching document in the target collection",
                    ));
                }
                (true, "replace", _) => {
                    self.update(sql, ns, &q, &Bson::Document(d), false, false, &[], None)
                        .await?;
                }
                (true, "merge", _) => {
                    let mut set = d.clone();
                    set.remove("_id");
                    if !set.is_empty() {
                        self.update(
                            sql,
                            ns,
                            &q,
                            &Bson::Document(doc! {"$set": set}),
                            false,
                            false,
                            &[],
                            None,
                        )
                        .await?;
                    }
                }
                (true, other, _) => {
                    return Err(MongoError::new(
                        code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                        format!("$merge whenMatched {other} is not supported in this release"),
                    ));
                }
                (false, _, _) => {
                    let (_, errs) = self.insert(sql, ns, vec![d], true).await?;
                    if let Some((_, e)) = errs.into_iter().next() {
                        return Err(e);
                    }
                }
            }
        }
        Ok(())
    }

    async fn lookup(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        docs: Vec<Document>,
        spec: &Document,
        lim: &AggLimits,
    ) -> Result<Vec<Document>, MongoError> {
        let from = spec.get_str("from").map_err(|_| {
            MongoError::new(
                code::FAILED_TO_PARSE,
                "missing 'from' option to $lookup stage specification",
            )
        })?;
        let as_f = spec.get_str("as").map_err(|_| {
            MongoError::new(
                code::FAILED_TO_PARSE,
                "must specify 'as' field for a $lookup",
            )
        })?;
        let fns = Ns {
            db: ns.db.clone(),
            coll: from.to_owned(),
        };
        let local = spec.get_str("localField").ok();
        let foreign = spec.get_str("foreignField").ok();
        let pipe: Vec<Document> = spec
            .get_array("pipeline")
            .map(|a| a.iter().filter_map(|x| x.as_document().cloned()).collect())
            .unwrap_or_default();
        let mut out = Vec::with_capacity(docs.len());
        let mut bytes = 0usize;
        // Batches of input documents query the foreign side with $in;
        // each batch joins through a hash of the foreign values (equality
        // keys), not a nested loop, and the output counts against
        // `doc.agg_stage_memory_bytes`.
        for chunk in docs.chunks(500) {
            let foreign_docs = match (local, foreign) {
                (Some(l), Some(f)) => {
                    let mut keys: Vec<Bson> = Vec::new();
                    for d in chunk {
                        let vs = filter::values_at(d, l);
                        if vs.vals.is_empty() {
                            keys.push(Bson::Null);
                        }
                        for v in vs.vals {
                            if !matches!(v, Bson::Array(_)) {
                                keys.push(v.clone());
                            }
                        }
                    }
                    let mut p = vec![doc! {"$match": {f: {"$in": keys}}}];
                    p.extend(pipe.iter().cloned());
                    Box::pin(self.aggregate(sql, &fns, &p)).await?
                }
                _ => Box::pin(self.aggregate(sql, &fns, &pipe)).await?,
            };
            let joined = crate::heavy::run(chunk.len() * foreign_docs.len().max(1), || {
                join_chunk(chunk, &foreign_docs, local.zip(foreign), as_f, lim, &mut bytes)
            })?;
            out.extend(joined);
        }
        Ok(out)
    }
}

/// One batch of `$lookup`: each local document with the foreign
/// documents whose `foreign` values equal one of its `local` values
/// (a missing or null local value matches missing or null foreign ones),
/// in foreign order; every foreign document without fields.
fn join_chunk(
    chunk: &[Document],
    foreign_docs: &[Document],
    fields: Option<(&str, &str)>,
    as_f: &str,
    lim: &AggLimits,
    bytes: &mut usize,
) -> Result<Vec<Document>, MongoError> {
    let null_key = crate::keyenc::eq_key(&Bson::Null);
    // Foreign values to the positions of their documents.
    let mut index: std::collections::HashMap<Vec<u8>, Vec<usize>> =
        std::collections::HashMap::new();
    if let Some((_, f)) = fields {
        for (i, fd) in foreign_docs.iter().enumerate() {
            let fv = filter::values_at(fd, f);
            if fv.vals.is_empty() {
                index.entry(null_key.clone()).or_default().push(i);
            }
            for v in fv.vals {
                let e = index.entry(crate::keyenc::eq_key(v)).or_default();
                if e.last() != Some(&i) {
                    e.push(i);
                }
            }
        }
    }
    let mut out = Vec::with_capacity(chunk.len());
    for d in chunk {
        let matched: Vec<Bson> = match fields {
            Some((l, _)) => {
                let lv = filter::values_at(d, l);
                let mut hits: Vec<usize> = Vec::new();
                if lv.vals.is_empty() {
                    if let Some(v) = index.get(&null_key) {
                        hits.extend(v);
                    }
                } else {
                    for v in lv.vals {
                        if let Some(v) = index.get(&crate::keyenc::eq_key(v)) {
                            hits.extend(v);
                        }
                    }
                }
                hits.sort_unstable();
                hits.dedup();
                hits.into_iter()
                    .map(|i| Bson::Document(foreign_docs[i].clone()))
                    .collect()
            }
            None => foreign_docs
                .iter()
                .map(|x| Bson::Document(x.clone()))
                .collect(),
        };
        let mut n = d.clone();
        update::set_path(&mut n, as_f, Bson::Array(matched))?;
        *bytes += bson::to_vec(&n).map_or(0, |v| v.len());
        if *bytes > lim.stage_bytes {
            return Err(MongoError::new(
                code::QUERY_EXCEEDED_MEMORY_LIMIT,
                format!(
                    "$lookup exceeded memory limit of {} bytes (doc.agg_stage_memory_bytes)",
                    lim.stage_bytes
                ),
            ));
        }
        out.push(n);
    }
    Ok(out)
}

/// What one update statement did.
#[derive(Debug, Clone, Default)]
pub struct UpdateOutcome {
    /// Documents matched (`n`).
    pub matched: u64,
    /// Documents changed (`nModified`).
    pub modified: u64,
    /// The `_id` an upsert inserted.
    pub upserted: Option<Bson>,
    /// The last document after the update (`findAndModify`).
    pub last_new: Option<Document>,
    /// The last document before the update.
    pub last_old: Option<Document>,
}

fn parse_array_filters(afs: &[Document]) -> Result<Vec<(String, Pred)>, MongoError> {
    afs.iter()
        .map(|f| {
            let id = f
                .keys()
                .next()
                .and_then(|k| k.split('.').next())
                .ok_or_else(|| {
                    MongoError::new(
                        code::FAILED_TO_PARSE,
                        "Cannot use an expression without a top-level field name in arrayFilters",
                    )
                })?
                .to_owned();
            Ok((id, filter::parse(f)?))
        })
        .collect()
}

fn dup_error(e: &SqlError, id: &Bson) -> MongoError {
    let idx = if e.message.contains("PRIMARY") {
        None
    } else {
        e.message.split('\'').rev().nth(1).map(str::to_owned)
    };
    let (pattern, value) = match idx {
        None => (doc! {"_id": 1}, doc! {"_id": id.clone()}),
        Some(_) => (doc! {}, doc! {}),
    };
    MongoError::new(
        code::DUPLICATE_KEY,
        format!(
            "E11000 duplicate key error collection: index: {} dup key: {value}",
            if pattern.contains_key("_id") {
                "_id_"
            } else {
                "unique"
            }
        ),
    )
    .with_extra(doc! {"keyPattern": pattern, "keyValue": value})
}

#[cfg(test)]
mod tests;

/// `$near`, `$geoNear`, `2dsphere` columns, search indexes and
/// `$vectorSearch` (chapters 34 and 36, R6 round 3).
pub mod models;
