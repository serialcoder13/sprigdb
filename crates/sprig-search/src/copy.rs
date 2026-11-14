//! One copy of a search index, kept current from the change feed.
//!
//! The same runtime serves a copy on a worker with the search role (the
//! default placement) and a copy inside a gateway (kept for deployments
//! without search workers). What differs is the
//! [`CopyHost`]: the feed's parts, the clusters to read, and where the
//! seed's rows come from (a gateway session of the index's definer, read
//! without row-level security).
//!
//! ```text
//! copy(index):                          one per (tenant, index, generation)
//!   open the partitions on disk
//!   every partition committed the same feed positions: resume after them
//!   otherwise: empty them; tails := every cluster's feed now; seed: read
//!     every row page by page by key; commit with the tails' positions
//!   loop over the feed: events of the object upsert or delete by key
//!     (each key's newest commit clamp wins), TRUNCATE empties, DROP of the
//!     object stops; every search.commit_interval_ms (sooner when a query
//!     waits, or after a quarter of it when the feed has gone quiet for
//!     50 ms): commit with the positions, then mark them applied
//!
//! wait(targets):  until every cluster's applied position reached its target,
//!   or 39826 after the timeout
//! snapshot(dest): hard links of the committed files (peer seeding and
//!   snapshots), checked against the linked meta.json
//! ```

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sprig_binlog::GtidPos;
use sprig_binlog::value::Value as BinValue;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_feed::names::Resolver;
use sprig_feed::{
    ChangeEvent, Clusters, DdlKind, FeedItem, Image, Op, PlacementView, StartAt, TailOptions,
    TenantTail,
};
use tokio::sync::watch;

use crate::analyze::CustomAnalyzer;
use crate::def::{IndexDef, Model};
use crate::schema::{IndexSchema, Key, KeyPart, Record, Val};
use crate::search::SearchIndex;

/// Rows read per seed page.
pub const SEED_PAGE: usize = 2000;
/// Changes held before a commit regardless of the interval.
const MAX_PENDING: u64 = 10_000;

fn unavailable(msg: impl Into<String>) -> SprigError {
    let mut e = SprigError::new(SprigCode::SearchUnavailable, msg);
    e.retryable = true;
    e
}

#[allow(clippy::needless_pass_by_value)] // used as map_err(join_err)
fn join_err(e: tokio::task::JoinError) -> SprigError {
    SprigError::internal(e.to_string())
}

/// Where a copy is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Phase {
    /// Reading the object from MariaDB.
    Seeding,
    /// Following the feed.
    Ready,
    /// Stopped by an error it can't retry (the message).
    Failed(String),
    /// Stopped.
    Stopped,
}

impl Phase {
    /// The name in records and `SHOW SEARCH INDEXES`.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Seeding => "seeding",
            Self::Ready => "ready",
            Self::Failed(_) => "failed",
            Self::Stopped => "stopped",
        }
    }
}

/// The feed positions whose changes are committed and searchable, per
/// cluster read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Applied {
    /// Each cluster's binlog position.
    pub pos: BTreeMap<ClusterId, GtidPos>,
    /// When it last moved.
    pub at: Option<Instant>,
}

impl Applied {
    /// The positions as text, for replies and records.
    #[must_use]
    pub fn text(&self) -> BTreeMap<String, String> {
        self.pos
            .iter()
            .map(|(c, p)| (c.to_string(), p.to_string()))
            .collect()
    }

    /// Whether every target is reached. A target's domains the copy has
    /// never seen on that cluster are ignored (they hold no tenant rows);
    /// a cluster the copy doesn't read yet is not reached.
    #[must_use]
    pub fn reached(&self, targets: &[(ClusterId, GtidPos)]) -> bool {
        targets.iter().all(|(c, target)| {
            self.pos.get(c).is_some_and(|p| {
                let mut t = target.clone();
                if !p.is_empty() {
                    t.retain_domains(|d| p.get(d).is_some());
                }
                p.reached(&t)
            })
        })
    }
}

/// What a commit stores with the index: each cluster's position.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Payload {
    v: u32,
    pos: BTreeMap<String, String>,
}

impl Payload {
    fn of(pos: &BTreeMap<ClusterId, GtidPos>) -> String {
        serde_json::to_string(&Payload {
            v: 2,
            pos: pos
                .iter()
                .map(|(c, p)| (c.to_string(), p.to_string()))
                .collect(),
        })
        .unwrap_or_default()
    }

    fn parse(s: &str) -> Option<BTreeMap<ClusterId, GtidPos>> {
        let p: Payload = serde_json::from_str(s).ok()?;
        if p.v != 2 {
            return None;
        }
        p.pos
            .into_iter()
            .map(|(c, x)| Some((ClusterId::new(&c).ok()?, x.parse().ok()?)))
            .collect()
    }
}

/// The change feed's parts (as MongoDB change streams build them).
#[derive(Clone)]
pub struct FeedParts {
    /// Clusters by id.
    pub clusters: Arc<dyn Clusters>,
    /// Shard placement and moves.
    pub placement: Arc<dyn PlacementView>,
    /// Physical to logical names.
    pub names: Arc<dyn Resolver>,
}

impl std::fmt::Debug for FeedParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FeedParts").finish_non_exhaustive()
    }
}

/// Values a copy reads while it runs.
#[derive(Debug, Clone, Copy)]
pub struct CopyKnobs {
    /// `search.commit_interval_ms`.
    pub commit_interval: Duration,
    /// `search.writer_memory_mb` in bytes.
    pub writer_bytes: usize,
}

/// Pages of the object's rows for a seed.
#[async_trait]
pub trait Seeder: Send {
    /// The next `limit` rows after `after` (`None`: from the first), in
    /// key order. 39820 when the object is gone.
    async fn page(&mut self, after: Option<&Key>, limit: usize) -> Result<Vec<Record>>;
    /// Ends the seed.
    async fn close(self: Box<Self>);
}

/// What a copy needs from the process it runs in.
#[async_trait]
pub trait CopyHost: Send + Sync + 'static {
    /// The knobs now.
    fn knobs(&self) -> CopyKnobs;
    /// The feed's parts.
    fn feed(&self) -> &FeedParts;
    /// The clusters to read: every registered, unretired cluster of the
    /// DC (a shard of the object can be on any of them).
    async fn clusters(&self) -> Result<Vec<ClusterId>>;
    /// A seed's row source.
    async fn seeder(&self, tenant: TenantId, def: &IndexDef) -> Result<Box<dyn Seeder>>;
    /// Whether the catalog no longer has the index's object.
    async fn object_gone(&self, tenant: TenantId, def: &IndexDef) -> bool;
}

/// One index's copy.
pub struct Copy {
    /// The tenant.
    pub tenant: TenantId,
    /// The definition.
    pub def: Arc<IndexDef>,
    /// The partitions.
    pub index: Arc<SearchIndex>,
    /// Where its files are.
    pub dir: PathBuf,
    phase: watch::Sender<Phase>,
    applied: watch::Sender<Applied>,
    waiters: AtomicUsize,
    stop: CancellationToken,
    /// Held by commits and by snapshots' links.
    commit_gate: Arc<Mutex<()>>,
    /// Documents indexed from the feed and seeds.
    pub docs_indexed: AtomicU64,
    /// Seeds done.
    pub seeds: AtomicU64,
}

impl std::fmt::Debug for Copy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Copy")
            .field("tenant", &self.tenant)
            .field("index", &self.def.name)
            .finish_non_exhaustive()
    }
}

/// A key part from text (a seed, a fetched row), by the column's type.
#[must_use]
pub fn key_part_from_text(text: &[u8], ty: &str) -> KeyPart {
    let t = ty.to_ascii_lowercase();
    if t.contains("int") || t == "year" {
        let s = String::from_utf8_lossy(text);
        if let Ok(i) = s.parse::<i64>() {
            return KeyPart::Int(i);
        }
        if let Ok(u) = s.parse::<u64>() {
            return KeyPart::UInt(u);
        }
    }
    if t.contains("binary") || t.contains("blob") {
        return KeyPart::Bin(text.to_vec());
    }
    if t.contains("char") || t.contains("text") || t == "enum" || t == "set" {
        return KeyPart::Str(String::from_utf8_lossy(text).into_owned());
    }
    KeyPart::Text(String::from_utf8_lossy(text).into_owned())
}

fn uuid_text(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(36);
    for (i, x) in b.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            s.push('-');
        }
        let _ = write!(s, "{x:02x}");
    }
    s
}

/// A key part from the binlog, encoded as [`key_part_from_text`] would.
fn part_from_bin(v: &BinValue, ty: &str) -> KeyPart {
    match v {
        BinValue::Int(i) => KeyPart::Int(*i),
        BinValue::UInt(u) => i64::try_from(*u).map_or(KeyPart::UInt(*u), KeyPart::Int),
        BinValue::Year(y) => KeyPart::Int(i64::from(*y)),
        BinValue::String(b) => KeyPart::Str(String::from_utf8_lossy(b).into_owned()),
        BinValue::Bytes(b) if ty.eq_ignore_ascii_case("uuid") && b.len() == 16 => {
            KeyPart::Text(uuid_text(b))
        }
        BinValue::Bytes(b) => KeyPart::Bin(b.clone()),
        BinValue::Decimal(s) => KeyPart::Text(s.clone()),
        other => KeyPart::Text(bin_text(other)),
    }
}

/// Text of a binlog value as MariaDB's text protocol prints it.
fn bin_text(v: &BinValue) -> String {
    match v {
        BinValue::Date(d) => d.to_string(),
        BinValue::Time(t) => t.to_string(),
        BinValue::DateTime(d) => d.to_string(),
        BinValue::Timestamp(t) => {
            let micros = i64::from(t.secs) * 1_000_000 + i64::from(t.micros);
            let secs = micros.div_euclid(1_000_000);
            let days = secs.div_euclid(86_400);
            let rem = secs.rem_euclid(86_400);
            // Civil date from days (Howard Hinnant's algorithm).
            let z = days + 719_468;
            let era = z.div_euclid(146_097);
            let doe = z - era * 146_097;
            let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            let mp = (5 * doy + 2) / 153;
            let d = doy - (153 * mp + 2) / 5 + 1;
            let m = if mp < 10 { mp + 3 } else { mp - 9 };
            let y = yoe + era * 400 + i64::from(m <= 2);
            format!(
                "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
                rem / 3600,
                (rem % 3600) / 60,
                rem % 60
            )
        }
        BinValue::Enum(n) => n.to_string(),
        BinValue::Set(b) | BinValue::Bit(b) => b.to_string(),
        BinValue::Int(i) => i.to_string(),
        BinValue::UInt(u) => u.to_string(),
        BinValue::Float(f) => f.to_string(),
        BinValue::Double(f) => f.to_string(),
        BinValue::Decimal(s) => s.clone(),
        BinValue::Year(y) => y.to_string(),
        BinValue::String(b) | BinValue::Bytes(b) | BinValue::Geometry(b) => {
            String::from_utf8_lossy(b).into_owned()
        }
        BinValue::Null => String::new(),
    }
}

/// A column value from the binlog.
fn val_from_bin(v: &BinValue) -> Val {
    match v {
        BinValue::Null => Val::Null,
        BinValue::Int(i) => Val::Int(*i),
        BinValue::UInt(u) => Val::UInt(*u),
        BinValue::Float(f) => Val::Float(f64::from(*f)),
        BinValue::Double(f) => Val::Float(*f),
        BinValue::Year(y) => Val::Int(i64::from(*y)),
        BinValue::Bit(b) => Val::UInt(*b),
        BinValue::String(b) => Val::Str(String::from_utf8_lossy(b).into_owned()),
        BinValue::Bytes(b) => Val::Bytes(b.clone()),
        other => Val::Str(bin_text(other)),
    }
}

/// A seed row read as text (`columns` as [`crate::schema::columns_read`]
/// lists them for a table; `_id`, `doc` for a collection) as a record.
#[must_use]
pub fn record_from_text(def: &IndexDef, columns: &[String], row: Vec<Option<Vec<u8>>>) -> Record {
    if def.model == Model::Table {
        let nk = def.key_columns.len().max(1);
        let parts: Vec<KeyPart> = (0..nk)
            .map(|i| {
                let ty = def.key_types.get(i).map_or("", String::as_str);
                key_part_from_text(
                    row.get(i).and_then(Option::as_deref).unwrap_or_default(),
                    ty,
                )
            })
            .collect();
        Record::Row {
            key: Key::of_parts(&parts),
            columns: columns
                .iter()
                .zip(row)
                .map(|(c, v)| {
                    (
                        c.clone(),
                        v.map_or(Val::Null, |b| {
                            Val::Str(String::from_utf8_lossy(&b).into_owned())
                        }),
                    )
                })
                .collect(),
        }
    } else if def.model == Model::Kv {
        let mut it = row.into_iter();
        let key = Key(it.next().flatten().unwrap_or_default());
        let doc = it
            .next()
            .flatten()
            .map_or(serde_json::Value::Null, |b| kv_value(&b));
        Record::Doc { key, doc }
    } else {
        let mut it = row.into_iter();
        let key = Key(it.next().flatten().unwrap_or_default());
        let doc = it
            .next()
            .flatten()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or(serde_json::Value::Null);
        Record::Doc { key, doc }
    }
}

/// A key-value string's value as a document: its JSON when it is JSON,
/// else the text.
#[must_use]
pub fn kv_value(b: &[u8]) -> serde_json::Value {
    serde_json::from_slice(b)
        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(b).into_owned()))
}

/// Whether a binlog `type` value is `string` (an ENUM's index 1, or its
/// text).
fn kv_string(v: &BinValue) -> bool {
    match v {
        BinValue::Enum(n) => *n == 1,
        BinValue::String(b) | BinValue::Bytes(b) => b.as_slice() == b"string",
        BinValue::Int(1) | BinValue::UInt(1) => true,
        _ => false,
    }
}

/// The columns a seed reads, and the `SELECT` of the page after `after`.
///
/// # Errors
/// A key that isn't a table key.
pub fn seed_page_sql(
    def: &IndexDef,
    after: Option<&Key>,
    limit: usize,
) -> Result<(Vec<String>, String)> {
    let q = |s: &str| sprig_common::names::quote(s);
    let table = format!("{}.{}", q(&def.db), q(&def.table));
    let cols: Vec<String> = match def.model {
        Model::Table => crate::schema::columns_read(def),
        Model::Kv => vec!["k".to_owned(), "v".to_owned()],
        Model::Collection => vec!["_id".to_owned(), "doc".to_owned()],
    };
    let nk = def.key_columns.len().max(1);
    let select = cols.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
    let order = cols[..nk.min(cols.len())]
        .iter()
        .map(|c| q(c))
        .collect::<Vec<_>>()
        .join(", ");
    let cond = match (after, def.model) {
        (None, _) => String::new(),
        (Some(k), Model::Table) => {
            let lhs = if nk == 1 {
                order.clone()
            } else {
                format!("({order})")
            };
            format!(" WHERE {lhs} > {}", k.sql(Model::Table)?)
        }
        (Some(k), Model::Kv) => format!(" WHERE `k` > {}", k.sql(def.model)?),
        (Some(k), _) => format!(" WHERE `_id` > {}", k.sql(def.model)?),
    };
    // A namespace's string keys only.
    let cond = match (def.model, cond.is_empty()) {
        (Model::Kv, true) => " WHERE `type` = 'string'".to_owned(),
        (Model::Kv, false) => format!("{cond} AND `type` = 'string'"),
        _ => cond,
    };
    let stmt = format!("SELECT {select} FROM {table}{cond} ORDER BY {order} LIMIT {limit}");
    Ok((cols, stmt))
}

/// A copy's request for one page of its object (`SearchSeed.SeedPage`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SeedRequest {
    /// The tenant.
    pub tenant: u64,
    /// The index.
    pub index: u64,
    /// The copy's generation; the gateway's catalog must agree (39825).
    pub generation: u32,
    /// The last key of the previous page.
    pub after: Option<Key>,
    /// Rows.
    pub limit: usize,
}

/// The page.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SeedReply {
    /// The rows in key order, or the error.
    pub rows: std::result::Result<Vec<Record>, crate::rpc::WireError>,
}

/// The file names a hard-link snapshot leaves out (writer locks).
fn skipped(name: &str) -> bool {
    name.starts_with(".tantivy-")
        && std::path::Path::new(name)
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("lock"))
}

/// Hard links of every file under `src` into `dest` (same relative
/// paths); `meta.json` files last.
fn link_tree(src: &Path, dest: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dest)?;
    let mut metas = Vec::new();
    for e in std::fs::read_dir(src)? {
        let e = e?;
        let name = e.file_name();
        let s = name.to_string_lossy();
        let p = e.path();
        if e.file_type()?.is_dir() {
            link_tree(&p, &dest.join(&name))?;
        } else if s == "meta.json" {
            metas.push(p);
        } else if !skipped(&s) {
            match std::fs::hard_link(&p, dest.join(&name)) {
                Ok(()) => {}
                // Deleted by a merge's clean-up meanwhile: the check sees it.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
    }
    for p in metas {
        std::fs::hard_link(&p, dest.join("meta.json"))?;
    }
    Ok(())
}

/// Whether every partition directory under `dir` (`p<n>`) holds every
/// file its `meta.json` names.
///
/// # Errors
/// I/O and Tantivy's.
pub fn verify_tree(dir: &Path, partitions: u32) -> Result<bool> {
    for i in 0..partitions.max(1) {
        let d = dir.join(format!("p{i}"));
        if !d.join("meta.json").exists() {
            return Ok(false);
        }
        let md = tantivy::directory::MmapDirectory::open(&d)
            .map_err(|e| SprigError::internal(format!("search copy {}: {e}", d.display())))?;
        let Ok(index) = tantivy::Index::open(md) else {
            return Ok(false);
        };
        // Opening a reader opens every segment's files.
        let reader: tantivy::Result<tantivy::IndexReader> = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into();
        if reader.is_err() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Every file under `dir`, relative paths, sorted (snapshots, streams).
///
/// # Errors
/// I/O.
pub fn list_tree(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    fn walk(base: &Path, d: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
        for e in std::fs::read_dir(d)? {
            let e = e?;
            let p = e.path();
            if e.file_type()?.is_dir() {
                walk(base, &p, out)?;
            } else if let Ok(r) = p.strip_prefix(base) {
                out.push(r.to_path_buf());
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out)?;
    out.sort();
    Ok(out)
}

impl Copy {
    /// Opens the copy's partitions in `dir` and starts following the feed;
    /// stops on `stop`.
    ///
    /// # Errors
    /// 39828 for analyzers that aren't defined; Tantivy's.
    pub fn start(
        host: Arc<dyn CopyHost>,
        tenant: TenantId,
        def: Arc<IndexDef>,
        customs: &[CustomAnalyzer],
        dir: PathBuf,
        stop: CancellationToken,
    ) -> Result<Arc<Self>> {
        let schema = IndexSchema::build(&def, customs)?;
        let knobs = host.knobs();
        let index = Arc::new(SearchIndex::open(schema, Some(&dir), knobs.writer_bytes)?);
        let rt = Arc::new(Self {
            tenant,
            def,
            index,
            dir,
            phase: watch::channel(Phase::Seeding).0,
            applied: watch::channel(Applied::default()).0,
            waiters: AtomicUsize::new(0),
            stop,
            commit_gate: Arc::new(Mutex::new(())),
            docs_indexed: AtomicU64::new(0),
            seeds: AtomicU64::new(0),
        });
        let r = Arc::clone(&rt);
        tokio::spawn(async move { r.run(host).await });
        Ok(rt)
    }

    /// Stops following the feed.
    pub fn stop(&self) {
        self.stop.cancel();
        self.phase.send_replace(Phase::Stopped);
    }

    /// Whether it stopped (or failed).
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        matches!(*self.phase.borrow(), Phase::Stopped | Phase::Failed(_))
    }

    /// The phase.
    #[must_use]
    pub fn phase(&self) -> Phase {
        self.phase.borrow().clone()
    }

    /// Phase changes.
    #[must_use]
    pub fn phases(&self) -> watch::Receiver<Phase> {
        self.phase.subscribe()
    }

    /// The committed position.
    #[must_use]
    pub fn applied(&self) -> Applied {
        self.applied.borrow().clone()
    }

    /// Waits until the copy is seeded.
    ///
    /// # Errors
    /// 39824 after `timeout`; 39825 when it failed.
    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        let mut rx = self.phase.subscribe();
        let r = tokio::time::timeout(timeout, async {
            loop {
                match &*rx.borrow_and_update() {
                    Phase::Ready => return Ok(()),
                    Phase::Failed(m) => {
                        return Err(unavailable(format!("search index copy failed: {m}")));
                    }
                    Phase::Stopped => return Err(unavailable("search index copy stopped")),
                    Phase::Seeding => {}
                }
                if rx.changed().await.is_err() {
                    return Err(unavailable("search index copy stopped"));
                }
            }
        })
        .await;
        r.unwrap_or_else(|_| {
            Err(SprigError::new(
                SprigCode::SearchIndexNotReady,
                format!("search index '{}' is still being built", self.def.name),
            ))
        })
    }

    /// Waits until every target position is searchable (the clusters'
    /// positions read when the query arrived).
    ///
    /// # Errors
    /// 39824 or 39826 after `timeout`.
    #[allow(clippy::items_after_statements)] // the guard type next to its use
    pub async fn wait_for(
        &self,
        targets: &[(ClusterId, GtidPos)],
        timeout: Duration,
    ) -> Result<()> {
        let started = Instant::now();
        self.wait_ready(timeout).await?;
        self.waiters.fetch_add(1, Ordering::SeqCst);
        struct Done<'a>(&'a AtomicUsize);
        impl Drop for Done<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let _g = Done(&self.waiters);
        let mut rx = self.applied.subscribe();
        let left = timeout.saturating_sub(started.elapsed());
        let r = tokio::time::timeout(left, async {
            loop {
                if rx.borrow_and_update().reached(targets) {
                    return Ok(());
                }
                if rx.changed().await.is_err() {
                    return Err(unavailable("search index copy stopped"));
                }
            }
        })
        .await;
        r.unwrap_or_else(|_| {
            Err(SprigError::new(
                SprigCode::SearchWaitTimeout,
                format!(
                    "search index '{}' didn't reach the clusters' positions within {} ms",
                    self.def.name,
                    timeout.as_millis()
                ),
            ))
        })
    }

    /// Hard links of the copy's committed files into `dest` (emptied
    /// first), checked against the linked `meta.json` files: a merge that
    /// removed a file meanwhile makes it try again (up to 5 times).
    ///
    /// # Errors
    /// I/O; 39825 when no consistent set was linked.
    pub fn snapshot_into(&self, dest: &Path) -> Result<()> {
        let io = |e: std::io::Error| SprigError::internal(format!("search snapshot: {e}"));
        for _ in 0..5 {
            let _ = std::fs::remove_dir_all(dest);
            {
                let _g = self
                    .commit_gate
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                link_tree(&self.dir, dest).map_err(io)?;
            }
            if verify_tree(dest, self.def.partitions)? {
                return Ok(());
            }
        }
        let _ = std::fs::remove_dir_all(dest);
        Err(unavailable("search snapshot: the files kept changing"))
    }

    fn mark(&self, pos: &BTreeMap<ClusterId, GtidPos>) {
        self.applied.send_replace(Applied {
            pos: pos.clone(),
            at: Some(Instant::now()),
        });
    }

    async fn run(self: Arc<Self>, host: Arc<dyn CopyHost>) {
        let mut backoff = Duration::from_millis(200);
        loop {
            if self.stop.is_cancelled() {
                return;
            }
            match self.serve(host.as_ref()).await {
                Ok(()) => return,
                Err(e) if e.code() == Some(SprigCode::SearchIndexNotFound) => {
                    tracing::info!(
                        index = self.def.name,
                        "search index's object dropped; copy stopped"
                    );
                    self.phase.send_replace(Phase::Stopped);
                    return;
                }
                Err(e) => {
                    tracing::warn!(tenant = %self.tenant, index = self.def.name, error = %e, "search copy interrupted; retrying");
                    if matches!(*self.phase.borrow(), Phase::Seeding)
                        && backoff >= Duration::from_secs(20)
                    {
                        self.phase.send_replace(Phase::Failed(e.message.clone()));
                    }
                    tokio::select! {
                        () = self.stop.cancelled() => return,
                        () = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(10));
                    if matches!(*self.phase.borrow(), Phase::Failed(_)) {
                        self.phase.send_replace(Phase::Seeding);
                    }
                }
            }
        }
    }

    async fn open_tail(
        &self,
        feed: &FeedParts,
        cluster: &ClusterId,
        pos: GtidPos,
    ) -> Result<TenantTail> {
        TenantTail::start(
            self.tenant,
            StartAt::At {
                cluster: cluster.clone(),
                pos,
            },
            Arc::clone(&feed.clusters),
            Arc::clone(&feed.placement),
            Arc::clone(&feed.names),
            TailOptions {
                progress_every: Duration::from_millis(200),
                follow_moves: false,
                progress_on_filtered: true,
                ..TailOptions::default()
            },
        )
        .await
    }

    /// Opens one feed reader per cluster (resuming when every partition
    /// committed the same positions), seeds when it can't resume, then
    /// follows.
    async fn serve(&self, host: &dyn CopyHost) -> Result<()> {
        let payloads = {
            let ix = Arc::clone(&self.index);
            tokio::task::spawn_blocking(move || ix.payloads())
                .await
                .map_err(join_err)??
        };
        let mut resume = match payloads.first() {
            Some(Some(p)) if payloads.iter().all(|x| x.as_deref() == Some(p.as_str())) => {
                Payload::parse(p)
            }
            _ => None,
        };
        let clusters = host.clusters().await?;
        let feed = host.feed();
        let (pos, tails) = loop {
            let mut pos: BTreeMap<ClusterId, GtidPos> = BTreeMap::new();
            for c in &clusters {
                let p = match resume.as_ref().and_then(|r| r.get(c)) {
                    Some(p) => p.clone(),
                    None => feed.clusters.position(c).await?,
                };
                pos.insert(c.clone(), p);
            }
            let mut tails = Vec::new();
            let mut lost = false;
            for (c, p) in &pos {
                match self.open_tail(feed, c, p.clone()).await {
                    Ok(t) => tails.push(t),
                    // Files older than the binlog (a stale snapshot, a copy
                    // offline too long): build again from MariaDB.
                    Err(e) if resume.is_some() && e.code() == Some(SprigCode::FeedHistoryLost) => {
                        tracing::info!(index = self.def.name, cluster = %c, "search copy's position is past the feed's history; building it again");
                        lost = true;
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }
            if !lost {
                break (pos, tails);
            }
            resume = None;
        };
        if resume.is_none() {
            self.phase.send_replace(Phase::Seeding);
            self.seed(host, &pos).await?;
        }
        self.mark(&pos);
        self.phase.send_replace(Phase::Ready);
        self.follow(host, tails, pos).await
    }

    /// Reads every row of the object and commits them at `t0`, the feed
    /// positions taken before the first read: replaying from there
    /// converges, since every change is an upsert by key.
    async fn seed(&self, host: &dyn CopyHost, t0: &BTreeMap<ClusterId, GtidPos>) -> Result<()> {
        let started = Instant::now();
        let def = Arc::clone(&self.def);
        let ix = Arc::clone(&self.index);
        tokio::task::spawn_blocking(move || ix.delete_all())
            .await
            .map_err(join_err)??;
        let mut seeder = host.seeder(self.tenant, &def).await?;
        let r = self.seed_rows(seeder.as_mut()).await;
        seeder.close().await;
        let n = r?;
        self.commit(t0).await?;
        self.seeds.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("sprig_search_seeds_total", "source" => "mariadb").increment(1);
        tracing::info!(tenant = %self.tenant, index = def.name, rows = n, ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX), "search copy seeded");
        Ok(())
    }

    async fn seed_rows(&self, seeder: &mut dyn Seeder) -> Result<u64> {
        let mut after: Option<Key> = None;
        let mut n = 0u64;
        loop {
            let recs = seeder.page(after.as_ref(), SEED_PAGE).await?;
            let got = recs.len();
            after = recs.last().map(|r| r.key().clone());
            let ix = Arc::clone(&self.index);
            tokio::task::spawn_blocking(move || -> Result<()> {
                for r in &recs {
                    ix.upsert(r)?;
                }
                Ok(())
            })
            .await
            .map_err(join_err)??;
            n += got as u64;
            self.docs_indexed.fetch_add(got as u64, Ordering::Relaxed);
            if got < SEED_PAGE || self.stop.is_cancelled() {
                return Ok(n);
            }
        }
    }

    fn ours(&self, e: &ChangeEvent) -> bool {
        let d = &self.def;
        if d.object_id != 0 && e.object == d.object_id {
            return true;
        }
        e.object == 0
            && e.db_name.eq_ignore_ascii_case(&d.db)
            && e.table.eq_ignore_ascii_case(&d.table)
    }

    fn key_of(&self, img: &Image) -> Key {
        let d = &self.def;
        match d.model {
            Model::Table => {
                let parts: Vec<KeyPart> = d
                    .key_columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        let ty = d.key_types.get(i).map_or("", String::as_str);
                        img.0
                            .iter()
                            .find(|(n, _)| n.eq_ignore_ascii_case(c))
                            .map_or(KeyPart::Text(String::new()), |(_, v)| part_from_bin(v, ty))
                    })
                    .collect();
                Key::of_parts(&parts)
            }
            Model::Kv => Key(img
                .get("k")
                .and_then(|v| v.as_bytes().map(<[u8]>::to_vec))
                .unwrap_or_default()),
            Model::Collection => Key(img
                .get("_id")
                .and_then(|v| v.as_bytes().map(<[u8]>::to_vec))
                .unwrap_or_default()),
        }
    }

    fn record(&self, key: Key, after: &Image) -> Record {
        match self.def.model {
            Model::Table => Record::Row {
                key,
                columns: after
                    .0
                    .iter()
                    .map(|(n, v)| (n.clone(), val_from_bin(v)))
                    .collect(),
            },
            // A key-value namespace: string keys' values; a
            // key of another type (or no value) indexes nothing.
            Model::Kv => Record::Doc {
                key,
                doc: match (after.get("type"), after.get("v").and_then(|v| v.as_bytes())) {
                    (Some(t), Some(b)) if kv_string(t) => kv_value(b),
                    (None, Some(b)) => kv_value(b),
                    _ => serde_json::Value::Null,
                },
            },
            Model::Collection => Record::Doc {
                key,
                doc: after
                    .get("doc")
                    .and_then(|v| v.as_bytes())
                    .and_then(|b| serde_json::from_slice(b).ok())
                    .unwrap_or(serde_json::Value::Null),
            },
        }
    }

    /// Applies one event; whether it changed the index.
    fn apply(&self, e: &ChangeEvent) -> Result<bool> {
        match e.op {
            Op::Insert | Op::Update => {
                let key = self.key_of(&e.key);
                match &e.after {
                    Some(a) => self.index.upsert(&self.record(key, a))?,
                    None => self.index.delete(&key),
                }
                Ok(true)
            }
            Op::Delete => {
                self.index.delete(&self.key_of(&e.key));
                Ok(true)
            }
            Op::Truncate => {
                self.index.delete_all()?;
                Ok(true)
            }
            Op::Ddl(DdlKind::Drop) => Err(SprigError::new(
                SprigCode::SearchIndexNotFound,
                format!("the object of search index '{}' was dropped", self.def.name),
            )),
            Op::Ddl(_) => Ok(false),
        }
    }

    async fn commit(&self, pos: &BTreeMap<ClusterId, GtidPos>) -> Result<()> {
        let ix = Arc::clone(&self.index);
        let gate = Arc::clone(&self.commit_gate);
        let p = Payload::of(pos);
        tokio::task::spawn_blocking(move || {
            let _g = gate.lock().unwrap_or_else(PoisonError::into_inner);
            ix.commit(&p)
        })
        .await
        .map_err(join_err)??;
        metrics::counter!("sprig_search_commits_total").increment(1);
        self.mark(pos);
        Ok(())
    }

    fn spawn_reader(
        mut tail: TenantTail,
        cluster: ClusterId,
        tx: tokio::sync::mpsc::Sender<(ClusterId, Result<FeedItem>)>,
        stop: CancellationToken,
    ) {
        tokio::spawn(async move {
            loop {
                let item = tokio::select! {
                    () = stop.cancelled() => return,
                    i = tail.next() => i,
                };
                let failed = item.is_err();
                if tx.send((cluster.clone(), item)).await.is_err() || failed {
                    return;
                }
            }
        });
    }

    /// Applies the readers' items: each key's newest change wins by commit
    /// clamp, so a shard's old cluster lagging behind its new one (a move
    /// between clusters) can't bring back an older version.
    #[allow(clippy::too_many_lines)]
    async fn follow(
        &self,
        host: &dyn CopyHost,
        tails: Vec<TenantTail>,
        mut pos: BTreeMap<ClusterId, GtidPos>,
    ) -> Result<()> {
        // Each tail runs in its own task: `next()` isn't cancel-safe, and
        // the loop below wakes every 50 ms to commit.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<(ClusterId, Result<FeedItem>)>(1024);
        let reader_stop = self.stop.child_token();
        for t in tails {
            let c = t.cluster().clone();
            Self::spawn_reader(t, c, tx.clone(), reader_stop.clone());
        }
        let _reader = reader_stop.clone().drop_guard();
        // This copy's part of `sprig_search_index_lag_seconds`, dropped
        // when it stops following.
        let lag = LagReport::new();
        // Each key's last applied clamp, and each cluster's clamp so far.
        let mut newest: HashMap<Key, u64> = HashMap::new();
        let mut frontier: BTreeMap<ClusterId, u64> = BTreeMap::new();
        let mut pending = 0u64;
        let mut since = Instant::now();
        let mut checked = Instant::now();
        // When the feed last brought something: a quiet feed commits early.
        let mut last_item = Instant::now();
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let interval = host.knobs().commit_interval;
            let item = tokio::select! {
                biased;
                () = self.stop.cancelled() => {
                    if pending > 0 {
                        self.commit(&pos).await?;
                    }
                    return Ok(());
                }
                _ = tick.tick() => None,
                i = rx.recv() => match i {
                    Some((c, i)) => Some((c, i?)),
                    None => return Err(unavailable("the feed readers stopped")),
                },
            };
            let boundary = !matches!(item, Some((_, FeedItem::Event(_))));
            let quiet = item.is_none() && last_item.elapsed() >= Duration::from_millis(50);
            if item.is_some() {
                last_item = Instant::now();
            }
            match item {
                Some((c, FeedItem::Event(e))) => {
                    let mine = match (self.ours(&e), e.op) {
                        (false, _) => false,
                        // A shard schema dropped by a split's or a move's
                        // cleanup isn't the object dropped: the catalog says.
                        (true, Op::Ddl(DdlKind::Drop)) => {
                            if host.object_gone(self.tenant, &self.def).await {
                                return Err(SprigError::new(
                                    SprigCode::SearchIndexNotFound,
                                    format!(
                                        "the object of search index '{}' was dropped",
                                        self.def.name
                                    ),
                                ));
                            }
                            false
                        }
                        (true, Op::Insert | Op::Update | Op::Delete) => {
                            let k = self.key_of(&e.key);
                            if newest.get(&k).is_some_and(|h| *h > e.hlc) {
                                metrics::counter!("sprig_search_docs_indexed_total", "op" => "stale_skipped").increment(1);
                                false
                            } else {
                                newest.insert(k, e.hlc);
                                self.apply(&e)?
                            }
                        }
                        (true, _) => self.apply(&e)?,
                    };
                    if mine {
                        pending += 1;
                        self.docs_indexed.fetch_add(1, Ordering::Relaxed);
                        metrics::counter!("sprig_search_docs_indexed_total", "op" => "feed")
                            .increment(1);
                    }
                    frontier.insert(c.clone(), e.token.clamp.max(e.hlc));
                    pos.insert(c, e.token.pos.clone());
                }
                Some((c, FeedItem::TxnEnd { token } | FeedItem::Progress { token })) => {
                    frontier.insert(c.clone(), token.clamp);
                    pos.insert(c, token.pos);
                }
                None => {}
            }
            // Clamps every reader passed can't come back: forget them.
            if newest.len() > 100_000 || checked.elapsed() >= Duration::from_secs(10) {
                if frontier.len() == pos.len() {
                    let low = frontier.values().copied().min().unwrap_or(0);
                    newest.retain(|_, h| *h >= low);
                }
                if newest.len() > 1_000_000 {
                    tracing::warn!(
                        index = self.def.name,
                        keys = newest.len(),
                        "search copy's per-key order map is large"
                    );
                }
                // A cluster registered since gets a reader.
                if checked.elapsed() >= Duration::from_secs(10) {
                    checked = Instant::now();
                    if let Ok(cs) = host.clusters().await {
                        let feed = host.feed();
                        for c in cs {
                            if pos.contains_key(&c) {
                                continue;
                            }
                            if let Ok(p) = feed.clusters.position(&c).await
                                && let Ok(t) = self.open_tail(feed, &c, p.clone()).await
                            {
                                tracing::info!(index = self.def.name, cluster = %c, "search copy reads a new cluster");
                                pos.insert(c.clone(), p);
                                Self::spawn_reader(t, c, tx.clone(), reader_stop.clone());
                            }
                        }
                    }
                }
            }
            if pending == 0 {
                // Nothing to commit: the positions are applied as read.
                if boundary && self.applied.borrow().pos != pos {
                    self.mark(&pos);
                }
                if boundary {
                    lag.report(&frontier);
                }
                since = Instant::now();
                continue;
            }
            let waiting = self.waiters.load(Ordering::SeqCst) > 0;
            // A feed gone quiet (no item for a tick) commits after a
            // quarter of the interval, so a lone write is searchable
            // sooner; a busy feed still commits once an interval.
            let early = quiet && since.elapsed() >= interval / 4;
            if pending >= MAX_PENDING
                || (boundary && (since.elapsed() >= interval || waiting || early))
            {
                self.commit(&pos).await?;
                lag.report(&frontier);
                pending = 0;
                since = Instant::now();
            }
        }
    }
}

/// `sprig_search_index_lag_seconds` (`cluster`): how far the searchable
/// state of the copies on this process trails each cluster's feed, the
/// wall clock minus the clamp (commit time) a copy last made searchable,
/// the largest over the copies reading the cluster. Each copy reports
/// after a commit, or when a feed boundary leaves nothing to commit.
struct LagReport {
    id: u64,
}

/// Each cluster's lag per copy (by [`LagReport::id`]), in seconds.
type Lags = HashMap<ClusterId, HashMap<u64, f64>>;

fn lags() -> std::sync::MutexGuard<'static, Lags> {
    static LAGS: std::sync::OnceLock<std::sync::Mutex<Lags>> = std::sync::OnceLock::new();
    LAGS.get_or_init(Default::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// Seconds from a feed clamp (seconds in the high 32 bits) to `now`.
#[allow(clippy::cast_precision_loss)]
fn clamp_lag(clamp: u64, now: std::time::SystemTime) -> f64 {
    let now_ms = now
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    let at_ms = u128::from(clamp >> 32) * 1000;
    now_ms.saturating_sub(at_ms) as f64 / 1000.0
}

impl LagReport {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        Self {
            id: NEXT.fetch_add(1, Ordering::Relaxed),
        }
    }

    fn report(&self, frontier: &BTreeMap<ClusterId, u64>) {
        let now = std::time::SystemTime::now();
        let mut all = lags();
        for (c, clamp) in frontier {
            if *clamp == 0 {
                continue;
            }
            let per = all.entry(c.clone()).or_default();
            per.insert(self.id, clamp_lag(*clamp, now));
            let max = per.values().copied().fold(0.0, f64::max);
            metrics::gauge!("sprig_search_index_lag_seconds", "cluster" => c.to_string()).set(max);
        }
    }
}

impl Drop for LagReport {
    fn drop(&mut self) {
        let mut all = lags();
        for (c, per) in all.iter_mut() {
            if per.remove(&self.id).is_some() {
                let max = per.values().copied().fold(0.0, f64::max);
                metrics::gauge!("sprig_search_index_lag_seconds", "cluster" => c.to_string())
                    .set(max);
            }
        }
        all.retain(|_, per| !per.is_empty());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applied_reaches_only_read_clusters() {
        let c1 = ClusterId::new("dc-a-c1").unwrap();
        let c2 = ClusterId::new("dc-a-c2").unwrap();
        let mut a = Applied::default();
        a.pos.insert(c1.clone(), "0-1-10".parse().unwrap());
        assert!(a.reached(&[(c1.clone(), "0-1-10".parse().unwrap())]));
        assert!(!a.reached(&[(c1.clone(), "0-1-11".parse().unwrap())]));
        // A domain the copy never saw on c1 holds no tenant rows.
        assert!(a.reached(&[(c1.clone(), "0-1-9,7-1-3".parse().unwrap())]));
        // A cluster it doesn't read yet isn't reached.
        assert!(!a.reached(&[(c2, "0-2-1".parse().unwrap())]));
    }

    #[test]
    fn lag_is_wall_clock_minus_the_clamp() {
        let now = std::time::UNIX_EPOCH + Duration::from_millis(1_000_500);
        assert!((clamp_lag(990 << 32 | 7, now) - 10.5).abs() < 1e-9);
        // A clamp ahead of this clock (skew) is no lag.
        assert!(clamp_lag(2000 << 32, now).abs() < 1e-9);
        let c = ClusterId::new("dc-a-c9").unwrap();
        let (a, b) = (LagReport::new(), LagReport::new());
        let at = |secs_ago: u64| {
            let n = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            BTreeMap::from([(c.clone(), (n - secs_ago) << 32)])
        };
        a.report(&at(100));
        b.report(&at(5));
        let max = |c: &ClusterId| {
            lags()
                .get(c)
                .map(|p| p.values().copied().fold(0.0, f64::max))
        };
        assert!(max(&c).unwrap() >= 100.0);
        drop(a);
        let m = max(&c).unwrap();
        assert!((5.0..100.0).contains(&m), "{m}");
        drop(b);
        assert!(max(&c).is_none());
    }

    #[test]
    fn payload_round_trips() {
        let mut m = BTreeMap::new();
        m.insert(
            ClusterId::new("dc-a-c1").unwrap(),
            "0-1-10".parse().unwrap(),
        );
        let s = Payload::of(&m);
        assert_eq!(Payload::parse(&s), Some(m));
        assert_eq!(Payload::parse("{\"v\":1,\"pos\":{}}"), None);
    }
}
