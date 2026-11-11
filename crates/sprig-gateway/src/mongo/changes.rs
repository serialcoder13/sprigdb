//! Change streams (chapter 30 step 9; chapter 38 step 14; chapter 31
//! step 14, "Sharding, SQL reads and change events").
//!
//! An `aggregate` whose first stage is `$changeStream` (what drivers'
//! `watch()` sends) opens a tailable `awaitData` cursor over the tenant's
//! change feed: a [`TenantTail`] (chapter 38, "As built in R1") read by a
//! task of its own, which keeps the stream's events and progress tokens
//! in a bounded queue. `getMore` waits up to its `maxTimeMS` (default
//! 1 s) for events and answers with the batch and `postBatchResumeToken`.
//!
//! - **Rendering.** A row change of a collection's table becomes
//!   MongoDB's change event: `insert`, `update` (with `updateDescription`
//!   from a diff of the before and after documents) and `delete`; the
//!   documents are rebuilt from `doc` and `_types`, so they carry the BSON
//!   types the client wrote. `DROP` and `RENAME` of a watched collection's
//!   table give `drop` or `rename`, then `invalidate`.
//! - **`fullDocument`.** `default` sends it for inserts only;
//!   `updateLookup`, `whenAvailable` and `required` also for updates: the
//!   document as that update left it, read from the row image (MongoDB
//!   reads the current one). `fullDocumentBeforeChange` comes from the
//!   before image (`whenAvailable`, `required`).
//! - **Resume tokens** are the feed's tokens, hex in `_data`. `resumeAfter`
//!   and `startAfter` restart the tail after one, also when the tenant
//!   moved to another cluster in between: the tail follows the moves
//!   through the mover's handoff records, so nothing is lost or repeated.
//! - **Pipeline.** Stages after `$changeStream` that work on one event at
//!   a time (`$match`, `$project`, `$addFields`, `$set`, `$unset`,
//!   `$replaceRoot`, `$replaceWith`) run on each batch.
//!
//! - **Replacements.** The document engine sends a replacement's `UPDATE`
//!   with chapter 38's statement comment (`o=r`), which the feed keeps as
//!   the row's operation hint, so it shows as a `replace` event with the
//!   new document (`decisions/r5-interim.md` D14).
//!
//! - **`showExpandedEvents`** (R5 round 4): `create`, `createIndexes`,
//!   `dropIndexes` and `modify` (a `collMod` sets a table comment) from
//!   the feed's DDL statements, with `collectionUUID` and
//!   `operationDescription`; `rename` carries `to`.
//! - **Pre- and post-images** (R5 round 4): `whenAvailable` and
//!   `required` follow the collection's `changeStreamPreAndPostImages`
//!   option (`null`, or the stream fails with 280, when it is off).
//!
//! Not built: `startAtOperationTime` (the feed can't position by time yet),
//! `shardCollection` events, and the k-way merge across clusters (R1 keeps
//! a tenant on one cluster at a time).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bson::{Bson, DateTime, Document, Timestamp, doc};
use mysql_async::prelude::Queryable;
use mysql_async::{Pool, PoolConstraints, PoolOpts};
use sprig_binlog::GtidPos;
use sprig_catalog::CatalogLocator;
use sprig_cluster::driver::NodeConnector;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_doc::agg::{self, AggLimits};
use sprig_feed::binlog::{BinlogTxns, SERVER_ID_BASE};
use sprig_feed::names::{CatalogPools, CatalogResolver, Resolver};
use sprig_feed::{
    ChangeEvent, Clusters, DdlKind, FeedItem, MetaPlacement, Op, PlacementView, ResumeToken,
    StartAt, TailOptions, TenantTail, TxnSource,
};
use sprig_meta::MetaStore;
use sprig_proto_mongo::errors::{MongoError, code};
use sprig_provision::credentials::CredentialConnector;
use tokio::sync::mpsc;

/// Items a stream's queue holds before its tail waits.
const QUEUE: usize = 1024;

/// The feed's binlog sources for gateways: each cluster's writer, found
/// through the cluster registry, read as `sprig_repl`.
pub struct GatewayClusters {
    registry: ClusterRegistry,
    access: Mutex<HashMap<ClusterId, (sprig_binlog::BinlogSource, Pool)>>,
    next_server_id: AtomicU32,
}

impl std::fmt::Debug for GatewayClusters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayClusters").finish_non_exhaustive()
    }
}

fn unavailable(cluster: &ClusterId, e: impl std::fmt::Display) -> SprigError {
    let mut x = SprigError::new(SprigCode::BackendUnavailable, format!("{cluster}: {e}"));
    x.retryable = true;
    x
}

impl GatewayClusters {
    /// Over the registry.
    #[must_use]
    pub fn new(registry: ClusterRegistry) -> Self {
        // Server ids above the feed's base and the workers' (which add up
        // to 10,000,499 per process), per process.
        let offset = 20_000_000 + std::process::id() % 10_000 * 1_000;
        Self {
            registry,
            access: Mutex::new(HashMap::new()),
            next_server_id: AtomicU32::new(SERVER_ID_BASE + offset),
        }
    }

    async fn access(&self, cluster: &ClusterId) -> Result<(sprig_binlog::BinlogSource, Pool)> {
        if let Some(a) = self
            .access
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(cluster)
            .cloned()
        {
            return Ok(a);
        }
        let rec = self.registry.get(cluster).await?.ok_or_else(|| {
            SprigError::new(SprigCode::ClusterNotFound, format!("no cluster {cluster}"))
        })?;
        let rec = rec.body();
        let ctl = CredentialConnector::ctl(&rec.secrets);
        let repl = CredentialConnector::repl(&rec.secrets);
        let mut writer = None;
        for spec in &rec.nodes {
            let ep = ctl.endpoint(spec)?;
            let Ok(Ok(mut c)) = tokio::time::timeout(
                Duration::from_secs(5),
                mysql_async::Conn::new(ep.opts.clone()),
            )
            .await
            else {
                continue;
            };
            let probe: Option<(String, i64)> = c
                .query_first("SELECT @@wsrep_on, @@read_only")
                .await
                .ok()
                .flatten();
            let replicas: Vec<mysql_async::Row> =
                c.query("SHOW ALL SLAVES STATUS").await.unwrap_or_default();
            let _ = c.disconnect().await;
            let Some((wsrep, read_only)) = probe else {
                continue;
            };
            let galera = wsrep == "1" || wsrep.eq_ignore_ascii_case("ON");
            if galera || (replicas.is_empty() && read_only == 0) {
                writer = Some((spec.clone(), ep));
                break;
            }
        }
        let (spec, ep) = writer.ok_or_else(|| unavailable(cluster, "no reachable writer"))?;
        let src = repl.endpoint(&spec)?.binlog_source().await;
        let pool = Pool::new(
            mysql_async::OptsBuilder::from_opts(ep.opts.clone()).pool_opts(
                PoolOpts::new()
                    .with_constraints(PoolConstraints::new(0, 2).unwrap_or_default())
                    .with_inactive_connection_ttl(Duration::from_secs(60)),
            ),
        );
        self.access
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(cluster.clone(), (src.clone(), pool.clone()));
        Ok((src, pool))
    }

    fn forget(&self, cluster: &ClusterId) {
        self.access
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(cluster);
    }
}

#[async_trait]
impl Clusters for GatewayClusters {
    async fn open(
        &self,
        cluster: &ClusterId,
        tenant: TenantId,
        from: &GtidPos,
    ) -> Result<Box<dyn TxnSource>> {
        let (src, _) = self.access(cluster).await?;
        let id = self.next_server_id.fetch_add(1, Ordering::Relaxed);
        match BinlogTxns::connect(&src, tenant, from, id).await {
            Ok(t) => Ok(Box::new(t)),
            Err(e) => {
                // The writer may have moved: look it up again next time.
                self.forget(cluster);
                Err(e)
            }
        }
    }

    async fn position(&self, cluster: &ClusterId) -> Result<GtidPos> {
        let (_, pool) = self.access(cluster).await?;
        let mut c = pool.get_conn().await.map_err(|e| {
            self.forget(cluster);
            unavailable(cluster, e)
        })?;
        let p: Option<String> = c
            .query_first("SELECT @@gtid_binlog_pos")
            .await
            .map_err(|e| unavailable(cluster, e))?;
        p.unwrap_or_default().parse()
    }
}

/// The tenant catalog's pool, for the feed's name resolver.
struct Catalogs(Arc<dyn CatalogLocator>);

#[async_trait]
impl CatalogPools for Catalogs {
    async fn pool(&self, tenant: TenantId) -> Result<Pool> {
        Ok(self.0.writer(tenant).await?.pool)
    }
}

/// What change streams are built from.
pub struct ChangeFeed {
    clusters: Arc<dyn Clusters>,
    placement: Arc<dyn PlacementView>,
    names: Arc<dyn Resolver>,
}

impl std::fmt::Debug for ChangeFeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeFeed").finish_non_exhaustive()
    }
}

impl ChangeFeed {
    /// Over this DC's tier (shard maps, handoff records), the cluster
    /// registry and the tenant catalogs.
    ///
    /// # Errors
    /// The registry's.
    pub fn new(
        global: Arc<dyn MetaStore>,
        dc: Arc<dyn MetaStore>,
        catalogs: Arc<dyn CatalogLocator>,
    ) -> Result<Self> {
        Ok(Self {
            clusters: Arc::new(GatewayClusters::new(ClusterRegistry::new(
                global,
                Arc::clone(&dc),
            )?)),
            placement: Arc::new(MetaPlacement::new(dc)),
            names: Arc::new(CatalogResolver::new(Arc::new(Catalogs(catalogs)))),
        })
    }
}

/// The feed's parts as [`ChangeFeed::new`] builds them, for other
/// consumers in the gateway (chapter 39's search runtimes).
///
/// # Errors
/// The registry's.
#[allow(clippy::type_complexity)]
pub(crate) fn feed_parts(
    global: Arc<dyn MetaStore>,
    dc: Arc<dyn MetaStore>,
    catalogs: Arc<dyn CatalogLocator>,
) -> Result<(Arc<dyn Clusters>, Arc<dyn PlacementView>, Arc<dyn Resolver>)> {
    let f = ChangeFeed::new(global, dc, catalogs)?;
    Ok((f.clusters, f.placement, f.names))
}

/// `fullDocument` and `fullDocumentBeforeChange`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Images {
    /// `default` / `off`.
    Off,
    /// `updateLookup`: the document as the update left it (the row image;
    /// MongoDB reads the current one).
    On,
    /// `whenAvailable`: the image when the collection keeps them
    /// (`changeStreamPreAndPostImages`), else `null`.
    WhenAvailable,
    /// `required`: the image, or the stream fails when the collection
    /// doesn't keep them.
    Required,
}

impl Images {
    /// Whether the event carries the image at all.
    fn wanted(self) -> bool {
        self != Self::Off
    }

    /// Whether this mode needs the collection's
    /// `changeStreamPreAndPostImages`.
    fn needs_option(self) -> bool {
        matches!(self, Self::WhenAvailable | Self::Required)
    }
}

/// What a stream watches and how it renders.
#[derive(Debug, Clone)]
pub struct StreamSpec {
    /// The tenant.
    pub tenant: TenantId,
    /// The database; `None` for the whole deployment (`admin`,
    /// `allChangesForCluster`).
    pub db: Option<String>,
    /// The collection, by name and table; `None` for a whole database.
    pub coll: Option<(String, String)>,
    /// Collections of the database by table name (database and cluster
    /// streams), refreshed by the gateway's catalog; tables that aren't
    /// collections are left out.
    pub tables: Option<Arc<dyn TableNames>>,
    /// `fullDocument`.
    pub full: Images,
    /// `fullDocumentBeforeChange`.
    pub before: Images,
    /// Stages after `$changeStream`.
    pub pipeline: Vec<Document>,
    /// `showExpandedEvents`: DDL events (`create`, `createIndexes`,
    /// `dropIndexes`, `modify`) and the fields MongoDB 6.0 added
    /// (`collectionUUID`, `operationDescription`).
    pub expanded: bool,
    /// Chapter 29 (RL34): the documents the stream's session may see, by
    /// its row-level security policies when the stream opened. `None`: no
    /// row event shows.
    pub visibility: Option<Arc<crate::tenant::rls::DocViews>>,
}

/// Whether a `$changeStream` stage asks for `showExpandedEvents`.
#[must_use]
pub fn expanded_of(stage: &Document) -> bool {
    stage.get_bool("showExpandedEvents").unwrap_or(false)
}

/// What a DDL event is in MongoDB's terms, from its statement.
fn ddl_event(e: &ChangeEvent) -> Option<&'static str> {
    let text = e.ddl.as_deref().unwrap_or("");
    let words: Vec<String> = text
        .split_ascii_whitespace()
        .take(3)
        .map(str::to_ascii_uppercase)
        .collect();
    let w = |i: usize| words.get(i).map_or("", String::as_str);
    Some(match e.op {
        Op::Ddl(DdlKind::Create) if w(1) == "TABLE" || text.is_empty() => "create",
        Op::Ddl(DdlKind::Create) if w(1) == "INDEX" || (w(1) == "UNIQUE" && w(2) == "INDEX") => {
            "createIndexes"
        }
        Op::Ddl(DdlKind::Drop) if w(1) == "INDEX" => "dropIndexes",
        Op::Ddl(DdlKind::Drop) => "drop",
        Op::Ddl(DdlKind::Rename) => "rename",
        Op::Ddl(DdlKind::Alter) if text.contains(COLLMOD_MARK) => "modify",
        _ => return None,
    })
}

/// The table comment `collMod` sets, so the change feed carries a
/// `modify` event (MongoDB's `collMod` has one).
pub const COLLMOD_MARK: &str = "sprig:collMod";

/// The new name in `RENAME TABLE a TO b` (logical, unquoted).
fn rename_target(e: &ChangeEvent) -> Option<String> {
    let text = e.ddl.as_deref()?;
    let upper = text.to_ascii_uppercase();
    let at = upper.find(" TO ")?;
    let target = text[at + 4..].trim().trim_end_matches(';');
    let name = target.rsplit('.').next()?.trim().trim_matches('`');
    Some(name.to_owned())
}

/// Collection names of tables, for database-wide streams.
#[async_trait]
pub trait TableNames: Send + Sync + std::fmt::Debug {
    /// The collection whose table is `table` in database `db`.
    async fn collection(&self, tenant: TenantId, db: &str, table: &str) -> Option<String>;

    /// Whether the collection of `table` keeps pre- and post-images
    /// (`changeStreamPreAndPostImages: {enabled: true}`).
    async fn images(&self, _tenant: TenantId, _db: &str, _table: &str) -> bool {
        false
    }

    /// The collection's `info.uuid` (`collectionUUID` of expanded events).
    async fn uuid(&self, _tenant: TenantId, _db: &str, _table: &str) -> Option<[u8; 16]> {
        None
    }
}

/// One item of a stream's queue.
#[derive(Debug)]
enum Item {
    Event(Box<Document>, String),
    Progress(String),
    Invalidate(Box<Document>, String),
    Failed(MongoError),
}

/// An open change stream.
#[derive(Debug)]
pub struct ChangeStream {
    rx: mpsc::Receiver<Item>,
    stop: CancellationToken,
    /// The `_data` of `postBatchResumeToken`.
    pub token: String,
    pipeline: Vec<Document>,
    done: bool,
}

impl Drop for ChangeStream {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

/// `{_data: hex}`.
#[must_use]
pub fn token_doc(hex: &str) -> Document {
    doc! {"_data": hex}
}

/// The token of a `resumeAfter` / `startAfter` value.
fn parse_token(v: &Bson, tenant: TenantId) -> std::result::Result<ResumeToken, MongoError> {
    let hex = v
        .as_document()
        .and_then(|d| d.get_str("_data").ok())
        .ok_or_else(|| {
            MongoError::new(code::BAD_VALUE, "resume token must be {_data: <string>}")
        })?;
    let t = ResumeToken::from_hex(hex).map_err(|e| feed_error(&e))?;
    t.check_tenant(tenant).map_err(|e| feed_error(&e))?;
    Ok(t)
}

/// Chapter 38's codes in MongoDB's terms: a lost history is 286
/// `ChangeStreamHistoryLost`, a bad token 260 `InvalidResumeToken`.
fn feed_error(e: &SprigError) -> MongoError {
    match e.kind {
        sprig_common::ErrorKind::Sprig(SprigCode::FeedHistoryLost) => {
            MongoError::new(286, e.message.clone()).label("NonResumableChangeStreamError")
        }
        sprig_common::ErrorKind::Sprig(SprigCode::FeedBadResumeToken) => {
            MongoError::new(260, e.message.clone())
        }
        sprig_common::ErrorKind::Sprig(c) => {
            let m = MongoError::new(i32::from(c.number()), e.message.clone());
            if e.retryable {
                m.label("ResumableChangeStreamError")
            } else {
                m
            }
        }
        _ => MongoError::new(code::INTERNAL_ERROR, e.message.clone()),
    }
}

fn ts_of(clamp: u64) -> (Timestamp, DateTime) {
    let secs = u32::try_from(clamp >> 32).unwrap_or(u32::MAX);
    let inc = u32::try_from(clamp & 0xFFFF_FFFF).unwrap_or(0);
    (
        Timestamp {
            time: secs,
            increment: inc,
        },
        DateTime::from_millis(i64::from(secs) * 1000),
    )
}

fn image_doc(img: Option<&sprig_feed::Image>) -> Option<Document> {
    let img = img?;
    let doc = img.get("doc")?.as_str()?;
    let types = img.get("_types").and_then(|v| v.as_str());
    sprig_doc::json::to_bson(doc, types).ok()
}

/// `updateDescription` from the before and after documents: changed and
/// added fields (nested documents compared field by field, grown arrays
/// by position) and removed fields; `truncatedArrays` stays empty.
#[must_use]
pub fn diff(old: &Document, new: &Document) -> Document {
    fn walk(p: &str, a: &Document, b: &Document, upd: &mut Document, rem: &mut Vec<Bson>) {
        let path = |k: &str| {
            if p.is_empty() {
                k.to_owned()
            } else {
                format!("{p}.{k}")
            }
        };
        for (k, nv) in b {
            match a.get(k) {
                Some(ov) if bson_eq(ov, nv) => {}
                Some(Bson::Document(od)) if matches!(nv, Bson::Document(_)) => {
                    if let Bson::Document(nd) = nv {
                        walk(&path(k), od, nd, upd, rem);
                    }
                }
                // A grown array reports its changed positions; a shorter
                // one is replaced whole, as MongoDB reports `$pop`.
                Some(Bson::Array(oa)) if matches!(nv, Bson::Array(na) if na.len() >= oa.len()) => {
                    let Bson::Array(na) = nv else { continue };
                    for (i, x) in na.iter().enumerate() {
                        if oa.get(i).is_none_or(|y| !bson_eq(x, y)) {
                            upd.insert(format!("{}.{i}", path(k)), x.clone());
                        }
                    }
                }
                _ => {
                    upd.insert(path(k), nv.clone());
                }
            }
        }
        for k in a.keys() {
            if !b.contains_key(k) {
                rem.push(Bson::String(path(k)));
            }
        }
    }
    let (mut upd, mut rem) = (Document::new(), Vec::new());
    walk("", old, new, &mut upd, &mut rem);
    doc! {"updatedFields": upd, "removedFields": rem, "truncatedArrays": []}
}

fn bson_eq(a: &Bson, b: &Bson) -> bool {
    let wrap = |v: &Bson| bson::to_vec(&doc! {"v": v.clone()}).unwrap_or_default();
    wrap(a) == wrap(b)
}

/// The event for `e`, or `None` when the stream doesn't show it.
async fn render(
    spec: &StreamSpec,
    e: &ChangeEvent,
) -> std::result::Result<Option<(Document, bool)>, MongoError> {
    // Pre- and post-images for `whenAvailable` and `required` exist only
    // for collections that keep them (MongoDB 6.0's collection option).
    let kept = if (spec.full.needs_option() || spec.before.needs_option())
        && matches!(e.op, Op::Update | Op::Delete)
    {
        match &spec.tables {
            Some(t) => t.images(spec.tenant, &e.db_name, &e.table).await,
            None => false,
        }
    } else {
        true
    };
    let image = |mode: Images, img: Document| -> std::result::Result<Bson, MongoError> {
        if kept || !mode.needs_option() {
            return Ok(Bson::Document(img));
        }
        if mode == Images::Required {
            return Err(MongoError::new(
                280,
                "Change stream was configured to require a pre- or post-image, but the collection doesn't keep them (changeStreamPreAndPostImages)",
            )
            .label("NonResumableChangeStreamError"));
        }
        Ok(Bson::Null)
    };
    let mut err = None;
    let r = render_inner(spec, e, &image, &mut err).await;
    match err {
        Some(x) => Err(x),
        None => Ok(r),
    }
}

/// Puts `img` under `key` as `mode` allows; records the error of a
/// `required` image that isn't kept.
fn put_image(
    d: &mut Document,
    key: &str,
    mode: Images,
    img: Document,
    image: &(dyn Fn(Images, Document) -> std::result::Result<Bson, MongoError> + Sync),
    err: &mut Option<MongoError>,
) {
    if !mode.wanted() {
        return;
    }
    match image(mode, img) {
        Ok(v) => {
            d.insert(key, v);
        }
        Err(x) => *err = Some(x),
    }
}

#[allow(clippy::too_many_lines)]
async fn render_inner(
    spec: &StreamSpec,
    e: &ChangeEvent,
    image: &(dyn Fn(Images, Document) -> std::result::Result<Bson, MongoError> + Sync),
    err: &mut Option<MongoError>,
) -> Option<(Document, bool)> {
    if let Some(db) = &spec.db
        && &e.db_name != db
    {
        return None;
    }
    let ddl = if matches!(e.op, Op::Ddl(_)) {
        let k = ddl_event(e)?;
        // Without `showExpandedEvents`, only drops and renames show.
        if !spec.expanded && !matches!(k, "drop" | "rename") {
            return None;
        }
        Some(k)
    } else {
        None
    };
    let coll = if let Some((name, table)) = &spec.coll {
        if &e.table != table {
            return None;
        }
        name.clone()
    } else {
        let found = match &spec.tables {
            Some(t) => t.collection(spec.tenant, &e.db_name, &e.table).await,
            None => return None,
        };
        match found {
            Some(c) => c,
            // A collection created a moment ago may not be in this
            // gateway's catalog yet: a plain table name is its own
            // collection name (meta::table_name).
            None if ddl == Some("create") && sprig_doc::meta::table_name(&e.table) == e.table => {
                e.table.clone()
            }
            None => return None,
        }
    };
    let token = e.token.to_hex();
    let (ts, wall) = ts_of(e.hlc);
    let mut d = doc! {
        "_id": token_doc(&token),
        "operationType": "",
        "clusterTime": ts,
        "wallTime": wall,
    };
    let ns = doc! {"db": e.db_name.clone(), "coll": coll.clone()};
    let invalidates = spec.coll.is_some();
    if spec.expanded
        && let Some(t) = &spec.tables
        && let Some(u) = t.uuid(spec.tenant, &e.db_name, &e.table).await
    {
        d.insert(
            "collectionUUID",
            bson::Binary {
                subtype: bson::spec::BinarySubtype::Uuid,
                bytes: u.to_vec(),
            },
        );
    }
    // Chapter 29 (RL34): events of documents the session may not see
    // are dropped; an update that hides a document arrives as a delete,
    // one that shows it as an insert (`rls.stream_visibility_events`).
    let mut op = e.op;
    if matches!(e.op, Op::Insert | Op::Update | Op::Delete) {
        let see = |img: Option<&sprig_feed::Image>| {
            image_doc(img).is_some_and(|d| {
                spec.visibility
                    .as_ref()
                    .is_some_and(|v| v.visible(&e.db_name, &e.table, &d))
            })
        };
        let (b, a) = (see(e.before.as_ref()), see(e.after.as_ref()));
        match e.op {
            Op::Insert if !a => return None,
            Op::Delete if !b => return None,
            Op::Update => match (b, a) {
                (false, false) => return None,
                (true, false) => op = Op::Delete,
                (false, true) => op = Op::Insert,
                (true, true) => {}
            },
            _ => {}
        }
    }
    match op {
        Op::Insert => {
            let after = image_doc(e.after.as_ref())?;
            d.insert("operationType", "insert");
            d.insert("fullDocument", after.clone());
            d.insert("ns", ns);
            d.insert(
                "documentKey",
                doc! {"_id": after.get("_id").cloned().unwrap_or(Bson::Null)},
            );
        }
        Op::Update if e.hint == Some(sprig_binlog::OpHint::Replace) => {
            // A whole-document replacement: MongoDB reports `replace`,
            // always with the new document and no update description.
            let before = image_doc(e.before.as_ref())?;
            let after = image_doc(e.after.as_ref())?;
            d.insert("operationType", "replace");
            d.insert("fullDocument", after.clone());
            d.insert("ns", ns);
            d.insert(
                "documentKey",
                doc! {"_id": after.get("_id").cloned().unwrap_or(Bson::Null)},
            );
            put_image(
                &mut d,
                "fullDocumentBeforeChange",
                spec.before,
                before,
                image,
                err,
            );
        }
        Op::Update => {
            let before = image_doc(e.before.as_ref())?;
            let after = image_doc(e.after.as_ref())?;
            d.insert("operationType", "update");
            put_image(&mut d, "fullDocument", spec.full, after.clone(), image, err);
            d.insert("ns", ns);
            d.insert(
                "documentKey",
                doc! {"_id": after.get("_id").cloned().unwrap_or(Bson::Null)},
            );
            d.insert("updateDescription", diff(&before, &after));
            put_image(
                &mut d,
                "fullDocumentBeforeChange",
                spec.before,
                before,
                image,
                err,
            );
        }
        Op::Delete => {
            let before = image_doc(e.before.as_ref())?;
            d.insert("operationType", "delete");
            d.insert("ns", ns);
            d.insert(
                "documentKey",
                doc! {"_id": before.get("_id").cloned().unwrap_or(Bson::Null)},
            );
            put_image(
                &mut d,
                "fullDocumentBeforeChange",
                spec.before,
                before,
                image,
                err,
            );
        }
        Op::Ddl(_) => {
            let kind = ddl.unwrap_or("drop");
            d.insert("operationType", kind);
            d.insert("ns", ns);
            match kind {
                "drop" => return Some((d, invalidates)),
                "rename" => {
                    if let Some(to) = rename_target(e) {
                        let to = doc! {"db": e.db_name.clone(), "coll": to};
                        d.insert("to", to.clone());
                        if spec.expanded {
                            d.insert("operationDescription", doc! {"to": to, "dropTarget": false});
                        }
                    }
                    return Some((d, invalidates));
                }
                "createIndexes" | "dropIndexes" => {
                    d.insert("operationDescription", doc! {"indexes": []});
                }
                "create" => {
                    d.insert(
                        "operationDescription",
                        doc! {"idIndex": {"v": 2, "key": {"_id": 1}, "name": "_id_"}},
                    );
                }
                _ => {}
            }
        }
        Op::Truncate => return None,
    }
    Some((d, false))
}

impl ChangeFeed {
    /// Opens a stream for `spec`, after `resume` when given.
    ///
    /// # Errors
    /// 260 for a token that isn't the tenant's; 286 when its history is
    /// gone; the feed's errors.
    #[allow(clippy::single_match_else)]
    pub async fn open(
        &self,
        spec: StreamSpec,
        resume: Option<&Bson>,
    ) -> std::result::Result<ChangeStream, MongoError> {
        let tenant = spec.tenant;
        let start = match resume {
            Some(v) => StartAt::After(parse_token(v, tenant)?),
            None => StartAt::Now,
        };
        let first = match &start {
            StartAt::After(t) => Some(t.to_hex()),
            _ => None,
        };
        let tail = TenantTail::start(
            tenant,
            start,
            Arc::clone(&self.clusters),
            Arc::clone(&self.placement),
            Arc::clone(&self.names),
            TailOptions {
                progress_every: Duration::from_millis(500),
                ..TailOptions::default()
            },
        )
        .await
        .map_err(|e| feed_error(&e))?;
        // The high-water mark before any event: where the tail starts.
        let token = match first {
            Some(t) => t,
            None => {
                let segment = self
                    .placement
                    .history(tenant)
                    .await
                    .map_err(|e| feed_error(&e))?
                    .last()
                    .map_or(0, |h| h.map_version);
                ResumeToken {
                    tenant,
                    clamp: 0,
                    cluster: tail.cluster().clone(),
                    segment,
                    pos: tail.position().clone(),
                    until: None,
                    progress: true,
                }
                .to_hex()
            }
        };
        let (tx, rx) = mpsc::channel(QUEUE);
        let stop = CancellationToken::new();
        let pipeline = spec.pipeline.clone();
        tokio::spawn(run(tail, spec, tx, stop.clone()));
        metrics::gauge!("sprig_mongo_cursors_open", "kind" => "change_stream").increment(1.0);
        Ok(ChangeStream {
            rx,
            stop,
            token,
            pipeline,
            done: false,
        })
    }
}

async fn run(
    mut tail: TenantTail,
    spec: StreamSpec,
    tx: mpsc::Sender<Item>,
    stop: CancellationToken,
) {
    loop {
        let item = tokio::select! {
            () = stop.cancelled() => break,
            () = tx.closed() => break,
            i = tail.next() => i,
        };
        let out = match item {
            Ok(FeedItem::Event(e)) => match render(&spec, &e).await {
                Ok(Some((d, false))) => Item::Event(Box::new(d), e.token.to_hex()),
                Ok(Some((d, true))) => Item::Invalidate(Box::new(d), e.token.to_hex()),
                Ok(None) => Item::Progress(e.token.to_hex()),
                Err(x) => Item::Failed(x),
            },
            Ok(FeedItem::TxnEnd { token } | FeedItem::Progress { token }) => {
                Item::Progress(token.to_hex())
            }
            Err(e) => Item::Failed(feed_error(&e)),
        };
        let last = matches!(out, Item::Failed(_) | Item::Invalidate(..));
        // Progress never waits for room: a newer one follows.
        let sent = if matches!(out, Item::Progress(_)) {
            match tx.try_send(out) {
                Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => true,
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        } else {
            tx.send(out).await.is_ok()
        };
        if !sent || last {
            break;
        }
    }
    metrics::gauge!("sprig_mongo_cursors_open", "kind" => "change_stream").decrement(1.0);
}

impl ChangeStream {
    /// Whether the stream ended (invalidated or failed).
    #[must_use]
    pub fn done(&self) -> bool {
        self.done
    }

    /// Up to `n` events, waiting up to `wait` for the first one.
    ///
    /// # Errors
    /// The feed's failure, once.
    pub async fn next_batch(
        &mut self,
        n: usize,
        wait: Duration,
    ) -> std::result::Result<Vec<Document>, MongoError> {
        let mut out = Vec::new();
        if self.done {
            return Ok(out);
        }
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let item = if out.is_empty() {
                match tokio::time::timeout_at(deadline, self.rx.recv()).await {
                    Ok(Some(i)) => i,
                    Ok(None) => {
                        self.done = true;
                        break;
                    }
                    Err(_) => break,
                }
            } else {
                match self.rx.try_recv() {
                    Ok(i) => i,
                    Err(_) => break,
                }
            };
            match item {
                Item::Event(d, t) => {
                    self.token = t;
                    out.push(*d);
                }
                Item::Progress(t) => {
                    self.token = t;
                }
                Item::Invalidate(d, t) => {
                    self.token.clone_from(&t);
                    out.push(*d);
                    out.push(doc! {
                        "_id": token_doc(&t),
                        "operationType": "invalidate",
                        "clusterTime": Timestamp { time: 0, increment: 0 },
                    });
                    self.done = true;
                    break;
                }
                Item::Failed(e) => {
                    self.done = true;
                    return Err(e);
                }
            }
            if out.len() >= n {
                break;
            }
        }
        if self.pipeline.is_empty() || out.is_empty() {
            return Ok(out);
        }
        let lim = AggLimits {
            stage_bytes: 104_857_600,
            now: DateTime::now(),
        };
        for s in &self.pipeline {
            out = agg::run_stage(out, s, &lim)?;
        }
        Ok(out)
    }
}

/// The options of a `$changeStream` stage.
///
/// # Errors
/// 9 for unknown options or values; 39662 for `startAtOperationTime`.
pub fn parse_stage(
    st: &Document,
) -> std::result::Result<(Images, Images, Option<Bson>, bool), MongoError> {
    let mut full = Images::Off;
    let mut before = Images::Off;
    let mut resume = None;
    let mut all = false;
    for (k, v) in st {
        match k.as_str() {
            "fullDocument" => {
                full = match v.as_str() {
                    Some("default") => Images::Off,
                    Some("updateLookup") => Images::On,
                    Some("whenAvailable") => Images::WhenAvailable,
                    Some("required") => Images::Required,
                    _ => {
                        return Err(MongoError::new(
                            code::BAD_VALUE,
                            format!("unknown full document option: {v}"),
                        ));
                    }
                };
            }
            "fullDocumentBeforeChange" => {
                before = match v.as_str() {
                    Some("off") => Images::Off,
                    Some("whenAvailable") => Images::WhenAvailable,
                    Some("required") => Images::Required,
                    _ => {
                        return Err(MongoError::new(
                            code::BAD_VALUE,
                            format!("unknown full document before change option: {v}"),
                        ));
                    }
                };
            }
            "resumeAfter" | "startAfter" => resume = Some(v.clone()),
            "allChangesForCluster" => all = v.as_bool().unwrap_or(false),
            "showExpandedEvents" | "allowToRunOnConfigDB" | "allowToRunOnSystemNS" => {}
            "startAtOperationTime" => {
                return Err(MongoError::new(
                    code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                    "startAtOperationTime is not supported in this release; resume with a token",
                ));
            }
            other => {
                return Err(MongoError::new(
                    code::FAILED_TO_PARSE,
                    format!("unrecognized option to $changeStream: {other}"),
                ));
            }
        }
    }
    Ok((full, before, resume, all))
}

/// Stages allowed after `$changeStream`.
///
/// # Errors
/// 9 for any other stage.
pub fn check_pipeline(stages: &[Document]) -> std::result::Result<(), MongoError> {
    for s in stages {
        let name = s.keys().next().map_or("", String::as_str);
        if !matches!(
            name,
            "$match"
                | "$project"
                | "$addFields"
                | "$set"
                | "$unset"
                | "$replaceRoot"
                | "$replaceWith"
        ) {
            return Err(MongoError::new(
                code::FAILED_TO_PARSE,
                format!("{name} is not permitted in a $changeStream pipeline"),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_description() {
        let a = doc! {"_id": 1, "a": 1, "b": {"c": 1, "d": 2}, "arr": [1, 2, 3], "gone": true, "s": [1, 2]};
        let b = doc! {"_id": 1, "a": 2, "b": {"c": 1, "d": 3, "e": 4}, "arr": [1, 2], "s": [1, 2, 9], "n": "new"};
        let d = diff(&a, &b);
        assert_eq!(
            d.get_document("updatedFields").unwrap(),
            &doc! {"a": 2, "b.d": 3, "b.e": 4, "arr": [1, 2], "s.2": 9, "n": "new"}
        );
        assert_eq!(
            d.get_array("removedFields").unwrap(),
            &vec![Bson::String("gone".into())]
        );
        assert_eq!(
            d.get_document("updatedFields")
                .unwrap()
                .get_array("arr")
                .unwrap(),
            &vec![Bson::Int32(1), Bson::Int32(2)]
        );
    }

    #[test]
    fn stage_options() {
        let (f, b, r, all) =
            parse_stage(&doc! {"fullDocument": "updateLookup", "resumeAfter": {"_data": "00"}})
                .unwrap();
        assert_eq!((f, b, all), (Images::On, Images::Off, false));
        assert!(r.is_some());
        assert_eq!(
            parse_stage(&doc! {"startAtOperationTime": Timestamp { time: 1, increment: 0 }})
                .unwrap_err()
                .code,
            39662
        );
        assert!(check_pipeline(&[doc! {"$match": {"operationType": "insert"}}]).is_ok());
        assert!(check_pipeline(&[doc! {"$group": {"_id": 1}}]).is_err());
    }
}
