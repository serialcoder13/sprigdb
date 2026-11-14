//! Spilling to disk: `allowDiskUse` for the
//! in-gateway stages.
//!
//! - **Where:** spill files live in one directory per gateway process,
//!   `<doc.spill_dir>/sprig-spill-<owner>-<pid>`, held by a lock on its
//!   `lock` file. A starting gateway removes every sibling directory
//!   whose lock it can take (its process is gone), so no spill file
//!   survives a restart.
//! - **Caps:** bytes on disk per tenant (`doc.spill_tenant_max_bytes`) and
//!   per gateway (`doc.spill_max_bytes`); past either a spill fails with
//!   MongoDB's 14031 `OutOfDiskSpace`.
//! - **Lifetime:** a [`SpillFile`] is deleted when its last handle goes:
//!   at the end of the stage that wrote it, or when the cursor reading it
//!   is exhausted, killed, reaped or its connection closes. A cancelled
//!   command drops its futures, and with them its files.
//! - **Context:** the gateway runs a command inside [`scope`] with the
//!   command's `allowDiskUse` and tenant; outside a scope (unit tests,
//!   the engine-direct differential) nothing spills and every stage keeps
//!   its 292 at `doc.agg_stage_memory_bytes`.
//! - **Metrics:** each stage that spills records MongoDB 8.2's
//!   standardized fields (`usedDisk`, `spills`, `spilledBytes`,
//!   `spilledRecords`, `spilledDataStorageSize`) for `explain`.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AO};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use bson::Document;
use sprig_proto_mongo::errors::{MongoError, code};

/// MongoDB's `OutOfDiskSpace`.
pub const OUT_OF_DISK_SPACE: i32 = 14031;

/// Where and how much may be spilled.
#[derive(Debug, Clone)]
pub struct SpillConfig {
    /// `doc.spill_dir`; empty: the system temporary directory's
    /// `sprigdb-spill`.
    pub dir: PathBuf,
    /// `doc.spill_tenant_max_bytes`: bytes on disk per tenant (0: none).
    pub tenant_max_bytes: u64,
    /// `doc.spill_max_bytes`: bytes on disk per gateway (0: none).
    pub gateway_max_bytes: u64,
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            dir: std::env::temp_dir().join("sprigdb-spill"),
            tenant_max_bytes: 4 << 30,
            gateway_max_bytes: 10 << 30,
        }
    }
}

struct Global {
    /// This process's directory and the lock file that holds it.
    dir: Mutex<Option<(PathBuf, File)>>,
    caps: Mutex<(u64, u64)>,
    root: Mutex<PathBuf>,
    used: AtomicU64,
    files: AtomicU64,
    per_tenant: Mutex<HashMap<u64, u64>>,
    seq: AtomicU64,
    /// Totals since start (`serverStatus`).
    total_spills: AtomicU64,
    total_bytes: AtomicU64,
    /// By kind (`serverStatus`'s `metrics.query`): spills, bytes,
    /// records.
    by_kind: [[AtomicU64; 3]; KINDS],
    /// Documents and bytes sorted (`metrics.query.sort`).
    sorted_keys: AtomicU64,
    sorted_bytes: AtomicU64,
    /// Commands run with `allowDiskUse: false`.
    disallowed: AtomicU64,
}

/// What spilled, for `serverStatus`: MongoDB 8.0's `metrics.query`
/// groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// `$sort`, `find`'s sort, `$bucketAuto`, `$densify` (sorts).
    Sort,
    /// `$group`, `$bucket`, `$sortByCount`.
    Group,
    /// `$setWindowFields`.
    Window,
    /// `$graphLookup`.
    Graph,
    /// Anything else: the collection's read, stage outputs, `$facet`.
    Other,
}

const KINDS: usize = 5;

impl Kind {
    /// The kind of pipeline stage `name`.
    #[must_use]
    pub fn of_stage(name: &str) -> Self {
        match name {
            "$sort" | "$bucketAuto" | "$densify" | "$fill" => Self::Sort,
            "$group" | "$bucket" | "$sortByCount" => Self::Group,
            "$setWindowFields" => Self::Window,
            "$graphLookup" => Self::Graph,
            _ => Self::Other,
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Sort => 0,
            Self::Group => 1,
            Self::Window => 2,
            Self::Graph => 3,
            Self::Other => 4,
        }
    }
}

fn global() -> &'static Global {
    static G: OnceLock<Global> = OnceLock::new();
    G.get_or_init(|| {
        let d = SpillConfig::default();
        Global {
            dir: Mutex::new(None),
            caps: Mutex::new((d.tenant_max_bytes, d.gateway_max_bytes)),
            root: Mutex::new(d.dir),
            used: AtomicU64::new(0),
            files: AtomicU64::new(0),
            per_tenant: Mutex::new(HashMap::new()),
            seq: AtomicU64::new(0),
            total_spills: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            by_kind: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            sorted_keys: AtomicU64::new(0),
            sorted_bytes: AtomicU64::new(0),
            disallowed: AtomicU64::new(0),
        }
    })
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn sanitize(owner: &str) -> String {
    owner
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .take(48)
        .collect()
}

/// Sets the spill directory and caps, creates this process's directory,
/// and removes the directories of gateways that are gone. Returns
/// this process's directory.
///
/// # Errors
/// The directory can't be created or locked.
pub fn configure(cfg: &SpillConfig, owner: &str) -> std::io::Result<PathBuf> {
    set_caps(cfg.tenant_max_bytes, cfg.gateway_max_bytes);
    open_dir(&cfg.dir, owner)
}

/// Creates (once) this process's directory under `root`.
fn open_dir(root: &Path, owner: &str) -> std::io::Result<PathBuf> {
    let g = global();
    let root = if root.as_os_str().is_empty() {
        SpillConfig::default().dir
    } else {
        root.to_path_buf()
    };
    fs::create_dir_all(&root)?;
    let mine = root.join(format!(
        "sprig-spill-{}-{}",
        sanitize(owner),
        std::process::id()
    ));
    let mut slot = lock(&g.dir);
    if let Some((p, _)) = slot.as_ref()
        && *p == mine
    {
        return Ok(mine);
    }
    remove_stale(&root, &mine);
    fs::create_dir_all(&mine)?;
    let f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(mine.join("lock"))?;
    f.try_lock().map_err(std::io::Error::other)?;
    // Files left in our own directory (a reused pid) are stale too.
    if let Ok(rd) = fs::read_dir(&mine) {
        for e in rd.flatten() {
            if e.file_name() != "lock" {
                let _ = fs::remove_file(e.path());
            }
        }
    }
    *lock(&g.root) = root;
    *slot = Some((mine.clone(), f));
    Ok(mine)
}

/// Removes every `sprig-spill-*` directory under `root` but `mine` whose
/// lock is free: its gateway is gone.
fn remove_stale(root: &Path, mine: &Path) {
    let Ok(rd) = fs::read_dir(root) else { return };
    for e in rd.flatten() {
        let p = e.path();
        if p == mine
            || !e
                .file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("sprig-spill-"))
        {
            continue;
        }
        let free = match OpenOptions::new().write(true).open(p.join("lock")) {
            Ok(f) => f.try_lock().is_ok(),
            // No lock file: an old or broken directory.
            Err(_) => true,
        };
        if free {
            let _ = fs::remove_dir_all(&p);
        }
    }
}

/// Updates the caps (live settings).
pub fn set_caps(tenant_max_bytes: u64, gateway_max_bytes: u64) {
    *lock(&global().caps) = (tenant_max_bytes, gateway_max_bytes);
}

/// This process's spill directory, created with the defaults when
/// [`configure`] hasn't run.
fn proc_dir() -> Result<PathBuf, MongoError> {
    if let Some((p, _)) = lock(&global().dir).as_ref() {
        return Ok(p.clone());
    }
    let root = lock(&global().root).clone();
    open_dir(&root, "proc").map_err(|e| io_error(&e))
}

fn io_error(e: &std::io::Error) -> MongoError {
    MongoError::new(
        if e.kind() == std::io::ErrorKind::StorageFull {
            OUT_OF_DISK_SPACE
        } else {
            code::INTERNAL_ERROR
        },
        format!("spilling to disk failed: {e}"),
    )
}

/// Bytes and files on disk now: the gateway's, and one tenant's.
#[must_use]
pub fn usage(tenant: Option<u64>) -> (u64, u64, u64) {
    let g = global();
    let t = tenant.map_or(0, |t| lock(&g.per_tenant).get(&t).copied().unwrap_or(0));
    (g.used.load(AO::Relaxed), g.files.load(AO::Relaxed), t)
}

/// Spills and spilled bytes since the process started.
#[must_use]
pub fn totals() -> (u64, u64) {
    let g = global();
    (
        g.total_spills.load(AO::Relaxed),
        g.total_bytes.load(AO::Relaxed),
    )
}

/// Counts a command run with `allowDiskUse: false`
/// (`metrics.query.allowDiskUseFalse`).
pub fn note_disallowed() {
    global().disallowed.fetch_add(1, AO::Relaxed);
}

/// `serverStatus`'s spill counters, in MongoDB 8.0's
/// `metrics.query` shape: `allowDiskUseFalse`, `group`,
/// `setWindowFields` and `sort`, plus SprigDB's `graphLookup` and
/// `sprigSpills` (every spill, including the collection's read).
#[must_use]
pub fn metrics_doc() -> Document {
    let g = global();
    let n = |a: &AtomicU64| i64::try_from(a.load(AO::Relaxed)).unwrap_or(i64::MAX);
    let k = |kind: Kind| &g.by_kind[kind.index()];
    let four = |kind: Kind| {
        let c = k(kind);
        bson::doc! {
            "spilledBytes": n(&c[1]),
            "spilledDataStorageSize": n(&c[1]),
            "spilledRecords": n(&c[2]),
            "spills": n(&c[0]),
        }
    };
    let w = k(Kind::Window);
    let s = k(Kind::Sort);
    bson::doc! {
        "allowDiskUseFalse": n(&g.disallowed),
        "group": four(Kind::Group),
        "setWindowFields": {
            "spilledBytes": n(&w[1]),
            "spilledRecords": n(&w[2]),
            "spills": n(&w[0]),
        },
        "sort": {
            "spillToDisk": n(&s[0]),
            "spillToDiskBytes": n(&s[1]),
            "totalBytesSorted": n(&g.sorted_bytes),
            "totalKeysSorted": n(&g.sorted_keys),
        },
        "graphLookup": four(Kind::Graph),
        "sprigSpills": {
            "spills": n(&g.total_spills),
            "spilledBytes": n(&g.total_bytes),
        },
    }
}

fn charge(tenant: u64, n: u64) -> Result<(), MongoError> {
    let g = global();
    let (tcap, gcap) = *lock(&g.caps);
    let mut pt = lock(&g.per_tenant);
    let t = pt.entry(tenant).or_insert(0);
    let used = g.used.load(AO::Relaxed);
    if (gcap > 0 && used + n > gcap) || (tcap > 0 && *t + n > tcap) {
        let which = if gcap > 0 && used + n > gcap {
            format!("the gateway's spill limit of {gcap} bytes (doc.spill_max_bytes)")
        } else {
            format!("the tenant's spill limit of {tcap} bytes (doc.spill_tenant_max_bytes)")
        };
        return Err(MongoError::new(
            OUT_OF_DISK_SPACE,
            format!("spilling to disk would exceed {which}"),
        ));
    }
    *t += n;
    g.used.fetch_add(n, AO::Relaxed);
    g.total_bytes.fetch_add(n, AO::Relaxed);
    Ok(())
}

fn uncharge(tenant: u64, n: u64) {
    let g = global();
    let mut pt = lock(&g.per_tenant);
    if let Some(t) = pt.get_mut(&tenant) {
        *t = t.saturating_sub(n);
        if *t == 0 {
            pt.remove(&tenant);
        }
    }
    let _ = g
        .used
        .fetch_update(AO::Relaxed, AO::Relaxed, |u| Some(u.saturating_sub(n)));
}

// --- the command's context ----------------------------------------------

/// What one stage spilled (MongoDB 8.2's standardized names).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StageSpill {
    /// The stage's position in the pipeline.
    pub stage: usize,
    /// Times the stage wrote to disk.
    pub spills: u64,
    /// Bytes of documents written (`spilledBytes`).
    pub bytes: u64,
    /// Records written (`spilledRecords`).
    pub records: u64,
    /// Bytes on disk (`spilledDataStorageSize`).
    pub storage: u64,
}

/// A command's spilling context.
#[derive(Debug, Clone)]
pub struct SpillCtx {
    /// `allowDiskUse`.
    pub allow: bool,
    /// The tenant charged for the files.
    pub tenant: u64,
    stage: Arc<AtomicUsize>,
    stats: Arc<Mutex<Vec<StageSpill>>>,
    /// What the running stage is, for the counters by kind.
    kind: Arc<AtomicUsize>,
}

impl SpillCtx {
    /// A context for one command.
    #[must_use]
    pub fn new(allow: bool, tenant: u64) -> Self {
        Self {
            allow,
            tenant,
            stage: Arc::new(AtomicUsize::new(0)),
            stats: Arc::new(Mutex::new(Vec::new())),
            kind: Arc::new(AtomicUsize::new(Kind::Other.index())),
        }
    }

    /// What the command's stages spilled so far.
    #[must_use]
    pub fn stats(&self) -> Vec<StageSpill> {
        lock(&self.stats).clone()
    }
}

tokio::task_local! {
    static CTX: SpillCtx;
}

/// Runs `f` with `ctx`.
pub async fn scope<F: Future>(ctx: SpillCtx, f: F) -> F::Output {
    CTX.scope(ctx, f).await
}

/// Whether the running command may spill.
#[must_use]
pub fn allowed() -> bool {
    CTX.try_with(|c| c.allow).unwrap_or(false)
}

/// Whether the running command has a context (it runs in a gateway).
#[must_use]
pub fn in_scope() -> bool {
    CTX.try_with(|_| ()).is_ok()
}

/// Marks the pipeline stage now running.
pub fn set_stage(i: usize) {
    let _ = CTX.try_with(|c| c.stage.store(i, AO::Relaxed));
}

/// Marks the kind of stage now running (for `serverStatus`'s counters).
pub fn set_kind(k: Kind) {
    let _ = CTX.try_with(|c| c.kind.store(k.index(), AO::Relaxed));
}

fn tenant() -> u64 {
    CTX.try_with(|c| c.tenant).unwrap_or(0)
}

fn record(bytes: u64, records: u64, storage: u64) {
    let g = global();
    g.total_spills.fetch_add(1, AO::Relaxed);
    let ki = CTX
        .try_with(|c| c.kind.load(AO::Relaxed))
        .unwrap_or(Kind::Other.index())
        .min(KINDS - 1);
    let c = &g.by_kind[ki];
    c[0].fetch_add(1, AO::Relaxed);
    c[1].fetch_add(bytes, AO::Relaxed);
    c[2].fetch_add(records, AO::Relaxed);
    let _ = CTX.try_with(|c| {
        let i = c.stage.load(AO::Relaxed);
        let mut s = lock(&c.stats);
        let e = if let Some(e) = s.iter_mut().find(|e| e.stage == i) {
            e
        } else {
            s.push(StageSpill {
                stage: i,
                ..StageSpill::default()
            });
            let n = s.len() - 1;
            &mut s[n]
        };
        e.spills += 1;
        e.bytes += bytes;
        e.records += records;
        e.storage += storage;
    });
}

/// Counts a finished spill file written outside a builder or a sort
/// (a window partition's documents).
pub fn record_file(f: &SpillFile) {
    record(f.bytes, f.count, f.bytes);
}

// --- files ----------------------------------------------------------------

/// A spill file: BSON documents one after another. Deleted, and its bytes
/// given back to the caps, when the last handle goes.
#[derive(Debug)]
pub struct SpillFile {
    path: PathBuf,
    tenant: u64,
    bytes: u64,
    count: u64,
}

impl SpillFile {
    /// Documents in the file.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Bytes on disk.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// A reader from the start.
    ///
    /// # Errors
    /// The file can't be opened.
    pub fn reader(self: &Arc<Self>) -> Result<DocIter, MongoError> {
        let f = File::open(&self.path).map_err(|e| io_error(&e))?;
        Ok(Box::new(FileIter {
            r: BufReader::with_capacity(256 * 1024, f),
            left: self.count,
            _file: Arc::clone(self),
        }))
    }
}

impl Drop for SpillFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        uncharge(self.tenant, self.bytes);
        let _ = global()
            .files
            .fetch_update(AO::Relaxed, AO::Relaxed, |n| Some(n.saturating_sub(1)));
    }
}

/// Writes one spill file.
pub struct SpillWriter {
    w: BufWriter<File>,
    /// Dropped (deleted) with the writer unless [`Self::finish`] ran.
    file: Option<SpillFile>,
    raw: Vec<u8>,
}

impl SpillWriter {
    /// A new file in this process's directory.
    ///
    /// # Errors
    /// The file can't be created.
    pub fn create() -> Result<Self, MongoError> {
        let dir = proc_dir()?;
        let t = tenant();
        let n = global().seq.fetch_add(1, AO::Relaxed);
        let path = dir.join(format!("t{t}-{n}.bson"));
        let f = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(|e| io_error(&e))?;
        global().files.fetch_add(1, AO::Relaxed);
        Ok(Self {
            w: BufWriter::with_capacity(256 * 1024, f),
            file: Some(SpillFile {
                path,
                tenant: t,
                bytes: 0,
                count: 0,
            }),
            raw: Vec::new(),
        })
    }

    /// Appends `d`.
    ///
    /// # Errors
    /// 14031 past a cap; I/O errors.
    pub fn push(&mut self, d: &Document) -> Result<(), MongoError> {
        self.raw.clear();
        d.to_writer(&mut self.raw)
            .map_err(|e| MongoError::new(code::INTERNAL_ERROR, e.to_string()))?;
        let n = self.raw.len() as u64;
        let Some(file) = self.file.as_mut() else {
            return Err(MongoError::new(code::INTERNAL_ERROR, "spill file finished"));
        };
        charge(file.tenant, n)?;
        file.bytes += n;
        file.count += 1;
        self.w.write_all(&self.raw).map_err(|e| io_error(&e))
    }

    /// Ends the file.
    ///
    /// # Errors
    /// I/O errors.
    pub fn finish(mut self) -> Result<Arc<SpillFile>, MongoError> {
        self.w.flush().map_err(|e| io_error(&e))?;
        self.file
            .take()
            .map(Arc::new)
            .ok_or_else(|| MongoError::new(code::INTERNAL_ERROR, "spill file finished"))
    }
}

struct FileIter {
    r: BufReader<File>,
    left: u64,
    _file: Arc<SpillFile>,
}

impl Iterator for FileIter {
    type Item = Result<Document, MongoError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let mut len = [0u8; 4];
        if let Err(e) = self.r.read_exact(&mut len) {
            return Some(Err(io_error(&e)));
        }
        let n = usize::try_from(i32::from_le_bytes(len).max(5)).unwrap_or(5);
        let mut buf = vec![0u8; n];
        buf[..4].copy_from_slice(&len);
        if let Err(e) = self.r.read_exact(&mut buf[4..]) {
            return Some(Err(io_error(&e)));
        }
        Some(
            Document::from_reader(&mut buf.as_slice())
                .map_err(|e| MongoError::new(code::INTERNAL_ERROR, e.to_string())),
        )
    }
}

// --- sequences ----------------------------------------------------------

/// Documents, one at a time.
pub type DocIter = Box<dyn Iterator<Item = Result<Document, MongoError>> + Send>;

/// A stage's output: in memory, in a spill file, or a stream (a merge of
/// sorted runs) read once.
pub enum DocSeq {
    /// In memory.
    Mem(Vec<Document>),
    /// On disk; may be read more than once.
    File(Arc<SpillFile>),
    /// Read once.
    Stream(DocIter),
}

impl std::fmt::Debug for DocSeq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mem(v) => write!(f, "Mem({})", v.len()),
            Self::File(s) => write!(f, "File({}, {} bytes)", s.count, s.bytes),
            Self::Stream(_) => f.write_str("Stream"),
        }
    }
}

impl DocSeq {
    /// The documents, one at a time.
    ///
    /// # Errors
    /// A spill file can't be opened.
    pub fn into_iter_docs(self) -> Result<DocIter, MongoError> {
        Ok(match self {
            Self::Mem(v) => Box::new(v.into_iter().map(Ok)),
            Self::File(f) => f.reader()?,
            Self::Stream(s) => s,
        })
    }

    /// Whether the documents are in memory.
    #[must_use]
    pub fn is_mem(&self) -> bool {
        matches!(self, Self::Mem(_))
    }

    /// A sequence that can be read again: a stream is written out first
    /// (in memory up to `budget` bytes).
    ///
    /// # Errors
    /// The stream's; spilling's.
    pub fn rereadable(self, budget: usize) -> Result<Self, MongoError> {
        match self {
            Self::Stream(s) => {
                let mut b = SeqBuilder::new(budget);
                for d in s {
                    b.push(d?)?;
                }
                b.finish()
            }
            other => Ok(other),
        }
    }

    /// A reader of a sequence [`Self::rereadable`] made (memory is
    /// copied).
    ///
    /// # Errors
    /// A spill file can't be opened.
    pub fn reread(&self) -> Result<DocIter, MongoError> {
        Ok(match self {
            Self::Mem(v) => Box::new(v.clone().into_iter().map(Ok)),
            Self::File(f) => f.reader()?,
            Self::Stream(_) => {
                return Err(MongoError::new(
                    code::INTERNAL_ERROR,
                    "a streamed sequence can't be read twice",
                ));
            }
        })
    }

    /// Every document in memory.
    ///
    /// # Errors
    /// The stream's.
    pub fn into_vec(self) -> Result<Vec<Document>, MongoError> {
        match self {
            Self::Mem(v) => Ok(v),
            other => other.into_iter_docs()?.collect(),
        }
    }

    /// Every document in memory, failing with `err` once they pass
    /// `limit` bytes.
    ///
    /// # Errors
    /// `err()` past the limit; the stream's.
    pub fn into_vec_within(
        self,
        limit: usize,
        err: impl Fn() -> MongoError,
    ) -> Result<Vec<Document>, MongoError> {
        if let Self::Mem(v) = self {
            return Ok(v);
        }
        let mut out = Vec::new();
        let mut bytes = 0usize;
        for d in self.into_iter_docs()? {
            let d = d?;
            bytes += doc_bytes(&d);
            if bytes > limit {
                return Err(err());
            }
            out.push(d);
        }
        Ok(out)
    }
}

/// A document's BSON size.
#[must_use]
pub fn doc_bytes(d: &Document) -> usize {
    bson::to_vec(d).map_or(0, |v| v.len())
}

/// Collects a stage's output: in memory up to `budget` bytes, then all of
/// it in a spill file.
pub struct SeqBuilder {
    budget: usize,
    mem: Vec<Document>,
    bytes: usize,
    w: Option<SpillWriter>,
}

impl SeqBuilder {
    /// A builder keeping up to `budget` bytes in memory.
    #[must_use]
    pub fn new(budget: usize) -> Self {
        Self {
            budget,
            mem: Vec::new(),
            bytes: 0,
            w: None,
        }
    }

    /// Adds `d`.
    ///
    /// # Errors
    /// Spilling's.
    pub fn push(&mut self, d: Document) -> Result<(), MongoError> {
        if let Some(w) = self.w.as_mut() {
            return w.push(&d);
        }
        self.bytes += doc_bytes(&d);
        self.mem.push(d);
        if self.bytes > self.budget {
            let mut w = SpillWriter::create()?;
            for x in self.mem.drain(..) {
                w.push(&x)?;
            }
            self.w = Some(w);
        }
        Ok(())
    }

    /// The sequence.
    ///
    /// # Errors
    /// Spilling's.
    pub fn finish(self) -> Result<DocSeq, MongoError> {
        match self.w {
            None => Ok(DocSeq::Mem(self.mem)),
            Some(w) => {
                let f = w.finish()?;
                record(f.bytes, f.count, f.bytes);
                Ok(DocSeq::File(f))
            }
        }
    }
}

// --- external sort --------------------------------------------------------

/// A document order.
pub type Cmp = Arc<dyn Fn(&Document, &Document) -> Ordering + Send + Sync>;

fn sort_buf(buf: &mut Vec<Document>, cmp: &Cmp, limit: Option<usize>) {
    crate::heavy::run(buf.len(), || buf.sort_by(|a, b| cmp(a, b)));
    if let Some(n) = limit {
        buf.truncate(n);
    }
}

/// Sorts `input` stably by `cmp`, keeping the first `limit` documents:
/// in memory while the documents held fit `budget` bytes, else as sorted
/// runs on disk merged on reading. `stage` names the 292 when the
/// command may not spill.
///
/// # Errors
/// 292 past `budget` without `allowDiskUse`; the input's; spilling's.
pub fn sort_seq(
    input: impl Iterator<Item = Result<Document, MongoError>>,
    cmp: Cmp,
    limit: Option<usize>,
    budget: usize,
    stage: &str,
) -> Result<DocSeq, MongoError> {
    let mut buf: Vec<Document> = Vec::new();
    let mut bytes = 0usize;
    let mut runs: Vec<Arc<SpillFile>> = Vec::new();
    // With a limit, the buffer is trimmed to it whenever it doubles.
    let trim_at = limit.map(|n| n.saturating_mul(2).max(1024));
    for d in input {
        let d = d?;
        let b = doc_bytes(&d);
        bytes += b;
        let g = global();
        g.sorted_keys.fetch_add(1, AO::Relaxed);
        g.sorted_bytes.fetch_add(b as u64, AO::Relaxed);
        buf.push(d);
        if trim_at.is_some_and(|t| buf.len() >= t) || (limit.is_some() && bytes > budget) {
            sort_buf(&mut buf, &cmp, limit);
            bytes = buf.iter().map(doc_bytes).sum();
        }
        if bytes > budget {
            if !allowed() {
                return Err(MongoError::new(
                    code::QUERY_EXCEEDED_MEMORY_LIMIT,
                    format!(
                        "{stage} exceeded memory limit of {budget} bytes, but did not opt in to external sorting (doc.agg_stage_memory_bytes)."
                    ),
                ));
            }
            sort_buf(&mut buf, &cmp, limit);
            let mut w = SpillWriter::create()?;
            for x in &buf {
                w.push(x)?;
            }
            let f = w.finish()?;
            record(f.bytes, f.count, f.bytes);
            runs.push(f);
            buf.clear();
            bytes = 0;
        }
    }
    sort_buf(&mut buf, &cmp, limit);
    if runs.is_empty() {
        return Ok(DocSeq::Mem(buf));
    }
    let mut sources: Vec<DocIter> = Vec::with_capacity(runs.len() + 1);
    for r in &runs {
        sources.push(r.reader()?);
    }
    // The last run stays in memory; it comes last, being the latest input.
    sources.push(Box::new(buf.into_iter().map(Ok)));
    Ok(DocSeq::Stream(Box::new(Merge::new(sources, cmp, limit)?)))
}

/// A k-way merge of sorted runs; ties go to the earlier run, which keeps
/// the sort stable.
struct Merge {
    sources: Vec<DocIter>,
    heads: Vec<Option<Document>>,
    cmp: Cmp,
    left: Option<usize>,
}

impl Merge {
    fn new(mut sources: Vec<DocIter>, cmp: Cmp, limit: Option<usize>) -> Result<Self, MongoError> {
        let mut heads = Vec::with_capacity(sources.len());
        for s in &mut sources {
            heads.push(s.next().transpose()?);
        }
        Ok(Self {
            sources,
            heads,
            cmp,
            left: limit,
        })
    }
}

impl Iterator for Merge {
    type Item = Result<Document, MongoError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == Some(0) {
            return None;
        }
        let mut best: Option<usize> = None;
        for (i, h) in self.heads.iter().enumerate() {
            let Some(h) = h else { continue };
            best = match best {
                Some(b) if (self.cmp)(h, self.heads[b].as_ref().unwrap_or(h)) != Ordering::Less => {
                    Some(b)
                }
                _ => Some(i),
            };
        }
        let b = best?;
        let out = self.heads[b].take();
        match self.sources[b].next().transpose() {
            Ok(n) => self.heads[b] = n,
            Err(e) => return Some(Err(e)),
        }
        if let Some(l) = self.left.as_mut() {
            *l -= 1;
        }
        out.map(Ok)
    }
}

/// Tests that spill share the caps and counters: one at a time.
#[cfg(test)]
pub(crate) async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
    static M: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    M.lock().await
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::*;

    fn by_v() -> Cmp {
        Arc::new(|a: &Document, b: &Document| {
            a.get_i64("v")
                .unwrap_or(0)
                .cmp(&b.get_i64("v").unwrap_or(0))
        })
    }

    #[tokio::test]
    async fn sorts_through_runs_stably_and_deletes_the_files() {
        let _g = serial().await;
        let ctx = SpillCtx::new(true, 7);
        let c2 = ctx.clone();
        scope(ctx, async move {
            let input = (0..5000i64).map(|i| Ok(doc! {"v": (i * 7919) % 100, "i": i}));
            let seq = sort_seq(input, by_v(), None, 20_000, "$sort").unwrap();
            assert!(matches!(seq, DocSeq::Stream(_)));
            let (used, files, t) = usage(Some(7));
            assert!(used > 0 && files > 0 && t > 0);
            let out: Vec<Document> = seq.into_vec().unwrap();
            assert_eq!(out.len(), 5000);
            for w in out.windows(2) {
                let (a, b) = (w[0].get_i64("v").unwrap(), w[1].get_i64("v").unwrap());
                assert!(
                    a < b || (a == b && w[0].get_i64("i").unwrap() < w[1].get_i64("i").unwrap())
                );
            }
            assert_eq!(usage(Some(7)).2, 0, "files are gone with the merge");
        })
        .await;
        let st = c2.stats();
        assert_eq!(st.len(), 1);
        assert!(st[0].spills > 1 && st[0].records > 0 && st[0].bytes > 0);
    }

    #[tokio::test]
    async fn without_allow_disk_use_a_large_sort_is_292() {
        let _g = serial().await;
        let r = scope(SpillCtx::new(false, 8), async {
            let input = (0..5000i64).map(|i| Ok(doc! {"v": i}));
            sort_seq(input, by_v(), None, 20_000, "$sort")
        })
        .await;
        assert_eq!(r.unwrap_err().code, 292);
    }

    #[tokio::test]
    async fn sort_with_a_limit_keeps_the_top() {
        let _g = serial().await;
        let out = scope(SpillCtx::new(true, 9), async {
            let input = (0..50_000i64).rev().map(|i| Ok(doc! {"v": i}));
            sort_seq(input, by_v(), Some(3), 20_000, "$sort")
                .unwrap()
                .into_vec()
                .unwrap()
        })
        .await;
        let v: Vec<i64> = out.iter().map(|d| d.get_i64("v").unwrap()).collect();
        assert_eq!(v, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn the_tenant_cap_is_14031() {
        let _g = serial().await;
        set_caps(10_000, 0);
        let r = scope(SpillCtx::new(true, 10), async {
            let mut b = SeqBuilder::new(1000);
            let mut r = Ok(());
            for i in 0..2000i64 {
                r = b.push(doc! {"v": i, "pad": "xxxxxxxxxxxxxxxxxxxx"});
                if r.is_err() {
                    break;
                }
            }
            r
        })
        .await;
        let d = SpillConfig::default();
        set_caps(d.tenant_max_bytes, d.gateway_max_bytes);
        assert_eq!(r.unwrap_err().code, OUT_OF_DISK_SPACE);
        assert_eq!(usage(Some(10)).2, 0);
    }
}
