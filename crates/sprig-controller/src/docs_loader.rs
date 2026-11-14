//! The `docs_loader` loop: the release's docs pack in `__sprig_docs` on
//! every cluster of the DC, read-only for everyone but the loader.
//!
//! ```text
//! pack := docs.pack_path, or sprig-docs-<version>.pack next to the binary;
//!         its trailing SHA-256 checked (sprig_docs::pack::Pack::from_bytes)
//! per cluster (one key each, on the DC leader; a failure backs off on its own):
//!   1. CREATE the schema and tables IF NOT EXISTS; the docs-reader account
//!      sprig_docs_r with SELECT on __sprig_docs.* only (once per process)
//!   2. packs[version] ready with the pack's sha256 and, the first time this
//!      process looks, its rows hash to the pack's rows: done
//!      rows differ: event docs.pack.tampered, then load
//!   3. load: packs[version] := loading; delete the version's rows; insert in
//!      batches (500 rows or 4 MiB); read back and verify; packs[version] := ready
//!   4. retire: keep docs.keep_versions releases (this one and the newest
//!      others by load time); older ones lose their rows and become retired
//! ```
//!
//! Every connection is `sprig_ctl`'s, through the loop's own connection
//! budget (`docs`, one per cluster), so a slow cluster never holds another
//! loop's connections. Rows are verified once per cluster per process (at
//! start, and after each load); later passes only read the `packs` row.
//!
//! Metrics: `sprig_docs_pack_info{version,sha}`, `sprig_docs_load_seconds`,
//! `sprig_docs_verify_failures_total`. Events: `docs.pack.loaded`,
//! `docs.pack.tampered`, `docs.pack.unavailable`.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Value};
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::{ClusterId, DcId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_docs::pack::{Pack, hex};
use sprig_docs::settings as keys;
use sprig_docs::table::{self, Row};
use sprig_settings::{LiveSettings, SettingCtx};

use crate::catalogs::{ClusterPools, RegistryPools, acquire};
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};

/// Rows per `INSERT`.
pub const BATCH_ROWS: usize = 500;
/// Bytes of row data per `INSERT`, below every server's
/// `max_allowed_packet` default.
pub const BATCH_BYTES: usize = 4 << 20;
/// How often a cluster whose pack is verified is looked at again.
pub const RECHECK: Duration = Duration::from_secs(600);
/// How long a pack that didn't read is left before it is read again.
const PACK_RETRY: Duration = Duration::from_secs(60);
/// The longest one cluster's pass may take.
const PASS_LIMIT: Duration = Duration::from_secs(600);
/// How often a disabled loader looks at `docs.enabled` again.
const DISABLED_RECHECK: Duration = Duration::from_secs(60);

/// A pack's state on one cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackState {
    /// The pack file's SHA-256.
    pub sha256: Vec<u8>,
    /// Its chunk count.
    pub chunks: u32,
    /// `loading`, `ready` or `retired`.
    pub state: String,
}

/// What a cluster's `__sprig_docs` is asked to do. [`MariaDocs`] is the
/// one over a `sprig_ctl` connection; tests use their own.
#[async_trait::async_trait]
pub trait DocsDb: Send {
    /// Creates the schema and tables if missing.
    async fn ensure_schema(&mut self) -> Result<()>;
    /// Creates or re-asserts the docs-reader account with `password`.
    async fn ensure_reader(&mut self, password: &str) -> Result<()>;
    /// `packs[version]`.
    async fn pack_state(&mut self, version: &str) -> Result<Option<PackState>>;
    /// The version's rows in `chunk_id` order.
    async fn read_rows(&mut self, version: &str) -> Result<Vec<Row>>;
    /// `packs[version] := loading` with the pack's hash and chunk count,
    /// and the version's rows deleted.
    async fn begin_load(&mut self, version: &str, sha256: &[u8; 32], chunks: u32) -> Result<()>;
    /// Inserts `rows` with one statement.
    async fn insert(&mut self, rows: &[Row]) -> Result<()>;
    /// `packs[version] := ready`.
    async fn mark_ready(&mut self, version: &str) -> Result<()>;
    /// Every version with its state, newest load first.
    async fn versions(&mut self) -> Result<Vec<(String, String)>>;
    /// Deletes the version's rows and marks it `retired`.
    async fn retire(&mut self, version: &str) -> Result<()>;
}

/// The pack as the loader writes it.
#[derive(Debug, Clone)]
pub struct LoadedPack {
    /// The release.
    pub version: String,
    /// The pack file's SHA-256 (its trailer).
    pub sha256: [u8; 32],
    /// The rows, in `chunk_id` order.
    pub rows: Vec<Row>,
    /// [`table::rows_digest`] of `rows`.
    pub digest: [u8; 32],
}

impl LoadedPack {
    /// Reads and verifies a pack file's bytes.
    ///
    /// # Errors
    /// 39998 when the pack doesn't verify or a chunk doesn't fit the table.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let invalid = |m: String| SprigError::new(SprigCode::ConfigInvalid, m);
        let pack = Pack::from_bytes(bytes).map_err(|e| invalid(format!("docs pack: {e}")))?;
        let sha256 = sprig_docs::pack::pack_sha256(bytes)
            .ok_or_else(|| invalid("docs pack: truncated".into()))?;
        let rows = table::rows_of(&pack.chunks).map_err(|e| invalid(format!("docs pack: {e}")))?;
        if rows.iter().any(|r| r.version != pack.version) {
            return Err(invalid(format!(
                "docs pack: a chunk's version isn't the pack's {}",
                pack.version
            )));
        }
        let digest = table::rows_digest(&rows);
        Ok(Self {
            version: pack.version,
            sha256,
            rows,
            digest,
        })
    }

    fn chunks(&self) -> u32 {
        u32::try_from(self.rows.len()).unwrap_or(u32::MAX)
    }
}

/// What [`sync_cluster`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncReport {
    /// The pack was (re)loaded.
    pub loaded: bool,
    /// Rows of a ready pack didn't match the pack and were reloaded.
    pub tampered: bool,
    /// The rows were read back and checked in this pass.
    pub verified: bool,
    /// Versions retired in this pass.
    pub retired: Vec<String>,
    /// `INSERT` statements run.
    pub batches: usize,
}

/// `rows` cut into runs of at most `max_rows` rows and about `max_bytes`
/// bytes (a row bigger than `max_bytes` is a run of its own).
#[must_use]
pub fn batches(rows: &[Row], max_rows: usize, max_bytes: usize) -> Vec<std::ops::Range<usize>> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut bytes = 0;
    for (i, r) in rows.iter().enumerate() {
        let n = row_bytes(r);
        if i > start && (i - start >= max_rows.max(1) || bytes + n > max_bytes) {
            out.push(start..i);
            start = i;
            bytes = 0;
        }
        bytes += n;
    }
    if start < rows.len() {
        out.push(start..rows.len());
    }
    out
}

fn row_bytes(r: &Row) -> usize {
    16 + r.version.len()
        + r.page.len()
        + r.title.len()
        + r.heading_path.len()
        + r.audience.len()
        + r.text.len()
        + 2
        + r.terms.len()
        + r.embedding.len()
}

/// Brings one cluster's `__sprig_docs` to `pack`: verifies a ready copy
/// (unless `trusted`, a copy this process already verified), loads it
/// when missing, different or edited, and retires all but `keep` versions.
///
/// # Errors
/// The database's errors; 39999 when the rows read back after a load
/// don't match the pack (the next pass loads again).
pub async fn sync_cluster(
    db: &mut dyn DocsDb,
    pack: &LoadedPack,
    keep: usize,
    trusted: bool,
) -> Result<SyncReport> {
    let mut report = SyncReport {
        loaded: false,
        tampered: false,
        verified: false,
        retired: Vec::new(),
        batches: 0,
    };
    let state = db.pack_state(&pack.version).await?;
    let ready = state.as_ref().is_some_and(|s| {
        s.state == "ready" && s.sha256 == pack.sha256 && s.chunks == pack.chunks()
    });
    let mut good = ready && trusted;
    if ready && !trusted {
        let rows = db.read_rows(&pack.version).await?;
        report.verified = true;
        if table::rows_digest(&rows) == pack.digest {
            good = true;
        } else {
            report.tampered = true;
        }
    }
    if !good {
        db.begin_load(&pack.version, &pack.sha256, pack.chunks())
            .await?;
        for b in batches(&pack.rows, BATCH_ROWS, BATCH_BYTES) {
            db.insert(&pack.rows[b]).await?;
            report.batches += 1;
        }
        let back = db.read_rows(&pack.version).await?;
        report.verified = true;
        if table::rows_digest(&back) != pack.digest {
            metrics::counter!("sprig_docs_verify_failures_total").increment(1);
            return Err(SprigError::internal(format!(
                "docs {}: {} rows read back don't match the {} loaded",
                pack.version,
                back.len(),
                pack.rows.len()
            )));
        }
        db.mark_ready(&pack.version).await?;
        report.loaded = true;
    }
    // Retention: this version, then the newest others.
    let mut kept = 1usize;
    for (v, s) in db.versions().await? {
        if v == pack.version || s == "retired" {
            continue;
        }
        if kept < keep.max(1) {
            kept += 1;
        } else {
            db.retire(&v).await?;
            report.retired.push(v);
        }
    }
    Ok(report)
}

/// [`DocsDb`] over a `sprig_ctl` connection.
pub struct MariaDocs {
    conn: Conn,
}

impl std::fmt::Debug for MariaDocs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MariaDocs").finish_non_exhaustive()
    }
}

impl MariaDocs {
    /// Over `conn`.
    #[must_use]
    pub fn new(conn: Conn) -> Self {
        Self { conn }
    }
}

fn db_err(what: &str, e: mysql_async::Error) -> SprigError {
    // 1044, 1142: sprig_ctl has no grant on __sprig_docs.
    let denied = matches!(&e, mysql_async::Error::Server(s) if matches!(s.code, 1044 | 1142 | 1227));
    if denied {
        return SprigError::new(
            SprigCode::ClusterPrecheckFailed,
            format!(
                "docs: {what}: sprig_ctl may not manage __sprig_docs ({e}); run as an \
                 administrator: {}",
                sprig_provision::docs_account::CTL_GRANT
            ),
        );
    }
    SprigError::new(SprigCode::BackendUnavailable, format!("docs: {what}")).with_source(e)
}

fn row_from(mut r: mysql_async::Row) -> Result<Row> {
    let bytes = |r: &mut mysql_async::Row, i: usize| -> Result<Vec<u8>> {
        r.take_opt::<Vec<u8>, _>(i)
            .and_then(std::result::Result::ok)
            .ok_or_else(|| SprigError::internal(format!("docs: column {i} unreadable")))
    };
    let text = |r: &mut mysql_async::Row, i: usize| -> Result<String> {
        String::from_utf8(bytes(r, i)?)
            .map_err(|_| SprigError::internal(format!("docs: column {i} isn't UTF-8")))
    };
    let id = bytes(&mut r, 0)?;
    let chunk_id: [u8; 16] = id
        .try_into()
        .map_err(|_| SprigError::internal("docs: a chunk_id isn't 16 bytes"))?;
    Ok(Row {
        chunk_id,
        version: text(&mut r, 1)?,
        page: text(&mut r, 2)?,
        title: text(&mut r, 3)?,
        heading_path: text(&mut r, 4)?,
        audience: text(&mut r, 5)?,
        text: text(&mut r, 6)?,
        tokens: r
            .take_opt::<u16, _>(7)
            .and_then(std::result::Result::ok)
            .ok_or_else(|| SprigError::internal("docs: tokens unreadable"))?,
        terms: text(&mut r, 8)?,
        embedding: bytes(&mut r, 9)?,
    })
}

#[async_trait::async_trait]
impl DocsDb for MariaDocs {
    async fn ensure_schema(&mut self) -> Result<()> {
        for sql in table::ddl() {
            self.conn
                .query_drop(&sql)
                .await
                .map_err(|e| db_err("creating the schema", e))?;
        }
        Ok(())
    }

    async fn ensure_reader(&mut self, password: &str) -> Result<()> {
        for sql in sprig_provision::docs_account::account_sql(password) {
            self.conn
                .query_drop(&sql)
                .await
                .map_err(|e| db_err("the docs-reader account", e))?;
        }
        Ok(())
    }

    async fn pack_state(&mut self, version: &str) -> Result<Option<PackState>> {
        let sql = format!(
            "SELECT sha256, chunks, state FROM {} WHERE version = ?",
            table::qualified(table::PACKS)
        );
        let r: Option<(Vec<u8>, u32, String)> = self
            .conn
            .exec_first(sql, (version,))
            .await
            .map_err(|e| db_err("reading packs", e))?;
        Ok(r.map(|(sha256, chunks, state)| PackState {
            sha256,
            chunks,
            state,
        }))
    }

    async fn read_rows(&mut self, version: &str) -> Result<Vec<Row>> {
        let sql = format!(
            "SELECT {} FROM {} WHERE version = ? ORDER BY chunk_id",
            "chunk_id, version, page, title, heading_path, audience, text, tokens, terms, embedding",
            table::qualified(table::CHUNKS)
        );
        let rows: Vec<mysql_async::Row> = self
            .conn
            .exec(sql, (version,))
            .await
            .map_err(|e| db_err("reading chunks", e))?;
        rows.into_iter().map(row_from).collect()
    }

    async fn begin_load(&mut self, version: &str, sha256: &[u8; 32], chunks: u32) -> Result<()> {
        let up = format!(
            "INSERT INTO {} (version, sha256, chunks, loaded_at, state) \
             VALUES (?, ?, ?, NOW(3), 'loading') ON DUPLICATE KEY UPDATE \
             sha256 = VALUES(sha256), chunks = VALUES(chunks), loaded_at = VALUES(loaded_at), \
             state = 'loading'",
            table::qualified(table::PACKS)
        );
        self.conn
            .exec_drop(up, (version, sha256.to_vec(), chunks))
            .await
            .map_err(|e| db_err("marking the pack loading", e))?;
        let del = format!(
            "DELETE FROM {} WHERE version = ?",
            table::qualified(table::CHUNKS)
        );
        self.conn
            .exec_drop(del, (version,))
            .await
            .map_err(|e| db_err("deleting old rows", e))
    }

    async fn insert(&mut self, rows: &[Row]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut params: Vec<Value> = Vec::with_capacity(rows.len() * 10);
        for r in rows {
            params.extend([
                Value::from(r.version.as_str()),
                Value::Bytes(r.chunk_id.to_vec()),
                Value::from(r.page.as_str()),
                Value::from(r.title.as_str()),
                Value::from(r.heading_path.as_str()),
                Value::from(r.audience.as_str()),
                Value::from(r.text.as_str()),
                Value::UInt(u64::from(r.tokens)),
                Value::from(r.terms.as_str()),
                Value::Bytes(r.embedding.clone()),
            ]);
        }
        self.conn
            .exec_drop(table::insert_sql(rows.len()), params)
            .await
            .map_err(|e| db_err("inserting chunks", e))
    }

    async fn mark_ready(&mut self, version: &str) -> Result<()> {
        let sql = format!(
            "UPDATE {} SET state = 'ready', loaded_at = NOW(3) WHERE version = ?",
            table::qualified(table::PACKS)
        );
        self.conn
            .exec_drop(sql, (version,))
            .await
            .map_err(|e| db_err("marking the pack ready", e))
    }

    async fn versions(&mut self) -> Result<Vec<(String, String)>> {
        let sql = format!(
            "SELECT version, CAST(state AS CHAR) FROM {} ORDER BY loaded_at DESC, version DESC",
            table::qualified(table::PACKS)
        );
        self.conn
            .query(sql)
            .await
            .map_err(|e| db_err("listing packs", e))
    }

    async fn retire(&mut self, version: &str) -> Result<()> {
        let del = format!(
            "DELETE FROM {} WHERE version = ?",
            table::qualified(table::CHUNKS)
        );
        self.conn
            .exec_drop(del, (version,))
            .await
            .map_err(|e| db_err("retiring a version", e))?;
        let up = format!(
            "UPDATE {} SET state = 'retired' WHERE version = ?",
            table::qualified(table::PACKS)
        );
        self.conn
            .exec_drop(up, (version,))
            .await
            .map_err(|e| db_err("retiring a version", e))
    }
}

/// The clusters the loader serves and how it reaches them.
#[async_trait::async_trait]
pub trait DocsClusters: Send + Sync + 'static {
    /// Every cluster that may hold data: registered, neither rejected nor
    /// retired.
    async fn clusters(&self) -> Result<Vec<ClusterId>>;
    /// A `sprig_ctl` session on the cluster's writer.
    async fn open(&self, cluster: &ClusterId) -> Result<Box<dyn DocsDb>>;
    /// The docs-reader account's password on the cluster.
    async fn reader_password(&self, cluster: &ClusterId) -> Result<String>;
}

/// [`DocsClusters`] over the DC's registry, with its own connection budget.
pub struct RegistryDocs {
    pools: RegistryPools,
    registry: ClusterRegistry,
}

impl std::fmt::Debug for RegistryDocs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryDocs").finish_non_exhaustive()
    }
}

impl RegistryDocs {
    /// Over `registry`, with `pools` (a budget of its own).
    #[must_use]
    pub fn new(registry: ClusterRegistry, pools: RegistryPools) -> Self {
        Self { pools, registry }
    }
}

#[async_trait::async_trait]
impl DocsClusters for RegistryDocs {
    async fn clusters(&self) -> Result<Vec<ClusterId>> {
        self.pools.sweepable().await
    }

    async fn open(&self, cluster: &ClusterId) -> Result<Box<dyn DocsDb>> {
        let pool = self.pools.writer(cluster).await?;
        let conn = acquire(&pool, "docs loader").await?;
        Ok(Box::new(MariaDocs::new(conn)))
    }

    async fn reader_password(&self, cluster: &ClusterId) -> Result<String> {
        let stored = self.registry.get(cluster).await?.ok_or_else(|| {
            SprigError::new(SprigCode::ClusterNotFound, format!("no cluster {cluster}"))
        })?;
        let c = sprig_provision::docs_account::docs_credential(&stored.body().secrets.ctl)?;
        Ok(c.password().to_owned())
    }
}

/// Where the pack comes from: a fixed file (tests), or the settings.
#[derive(Debug, Clone)]
pub enum PackSource {
    /// This file.
    File(PathBuf),
    /// `docs.pack_path`, else `sprig-docs-<version>.pack` next to the
    /// binary.
    Settings,
}

/// The pack read last, keyed by its file's path, length and modification
/// time; or why it couldn't be read, until [`PACK_RETRY`] passes.
enum PackCache {
    Empty,
    Ready {
        key: (PathBuf, u64, Option<SystemTime>),
        pack: Arc<LoadedPack>,
    },
    Failed {
        path: PathBuf,
        at: Instant,
        error: String,
    },
}

/// The loop (see the module docs).
pub struct DocsLoader {
    clusters: Arc<dyn DocsClusters>,
    live: Option<(LiveSettings, DcId)>,
    source: PackSource,
    pack: Mutex<PackCache>,
    /// Clusters whose rows this process verified, with the pack digest.
    verified: Mutex<HashMap<ClusterId, [u8; 32]>>,
    /// Clusters whose reader account this process asserted.
    readers: Mutex<HashSet<ClusterId>>,
}

impl std::fmt::Debug for DocsLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocsLoader")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl DocsLoader {
    /// The loop over `clusters`, reading the `docs.*` settings from `live`
    /// (defaults without).
    #[must_use]
    pub fn new(
        clusters: Arc<dyn DocsClusters>,
        live: Option<(LiveSettings, DcId)>,
        source: PackSource,
    ) -> Self {
        Self {
            clusters,
            live,
            source,
            pack: Mutex::new(PackCache::Empty),
            verified: Mutex::new(HashMap::new()),
            readers: Mutex::new(HashSet::new()),
        }
    }

    fn enabled(&self) -> bool {
        match &self.live {
            Some((l, dc)) => l.get(&keys::ENABLED, &SettingCtx::process(dc)),
            None => keys::ENABLED.default,
        }
    }

    fn keep(&self) -> usize {
        let n = match &self.live {
            Some((l, dc)) => l.get(&keys::KEEP_VERSIONS, &SettingCtx::process(dc)),
            None => keys::KEEP_VERSIONS.default,
        };
        usize::try_from(n).unwrap_or(usize::MAX)
    }

    fn pack_path(&self) -> Option<PathBuf> {
        match &self.source {
            PackSource::File(p) => Some(p.clone()),
            PackSource::Settings => {
                let set = match &self.live {
                    Some((l, dc)) => l.get(&keys::PACK_PATH, &SettingCtx::process(dc)),
                    None => keys::PACK_PATH.default.clone(),
                };
                if set.trim().is_empty() {
                    keys::next_to_binary(&keys::pack_file_name(env!("CARGO_PKG_VERSION")))
                } else {
                    Some(PathBuf::from(set.trim()))
                }
            }
        }
    }

    /// The verified pack, read again when its file changes.
    ///
    /// # Errors
    /// 39998 when the file is missing or doesn't verify (an operator event
    /// once per [`PACK_RETRY`]).
    pub fn pack(&self) -> Result<Arc<LoadedPack>> {
        let Some(path) = self.pack_path() else {
            return Err(SprigError::new(
                SprigCode::ConfigInvalid,
                "docs pack: the binary's directory is unknown; set docs.pack_path",
            ));
        };
        let meta = std::fs::metadata(&path).ok();
        let key = (
            path.clone(),
            meta.as_ref().map_or(0, std::fs::Metadata::len),
            meta.as_ref().and_then(|m| m.modified().ok()),
        );
        {
            let cache = self.pack.lock().unwrap_or_else(PoisonError::into_inner);
            match &*cache {
                PackCache::Ready { key: k, pack } if *k == key => return Ok(Arc::clone(pack)),
                PackCache::Failed { path: p, at, error }
                    if *p == path && at.elapsed() < PACK_RETRY =>
                {
                    return Err(SprigError::new(SprigCode::ConfigInvalid, error.clone()));
                }
                _ => {}
            }
        }
        let r = std::fs::read(&path)
            .map_err(|e| {
                SprigError::new(
                    SprigCode::ConfigInvalid,
                    format!("docs pack {}: {e}", path.display()),
                )
            })
            .and_then(|b| LoadedPack::from_bytes(&b));
        let mut cache = self.pack.lock().unwrap_or_else(PoisonError::into_inner);
        match r {
            Ok(p) => {
                let p = Arc::new(p);
                tracing::info!(version = %p.version, sha256 = %hex(&p.sha256), chunks = p.rows.len(),
                    path = %path.display(), "docs pack verified");
                metrics::gauge!("sprig_docs_pack_info",
                    "version" => p.version.clone(), "sha" => hex(&p.sha256[..8]))
                .set(1.0);
                *cache = PackCache::Ready {
                    key,
                    pack: Arc::clone(&p),
                };
                Ok(p)
            }
            Err(e) => {
                tracing::warn!(event = "docs.pack.unavailable", path = %path.display(), error = %e,
                    "the docs pack is missing or doesn't verify: docs search is unavailable until it does");
                *cache = PackCache::Failed {
                    path,
                    at: Instant::now(),
                    error: e.message.clone(),
                };
                Err(e)
            }
        }
    }

    /// One cluster's pass.
    ///
    /// # Errors
    /// See [`sync_cluster`]; the connection's.
    pub async fn sync(&self, cluster: &ClusterId, pack: &LoadedPack) -> Result<SyncReport> {
        let started = Instant::now();
        let mut db = self.clusters.open(cluster).await?;
        let new_reader = !self
            .readers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(cluster);
        let trusted = self
            .verified
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(cluster)
            == Some(&pack.digest);
        if new_reader || !trusted {
            db.ensure_schema().await?;
        }
        if new_reader {
            let pw = self.clusters.reader_password(cluster).await?;
            db.ensure_reader(&pw).await?;
            self.readers
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(cluster.clone());
        }
        let report = sync_cluster(db.as_mut(), pack, self.keep(), trusted).await?;
        if report.tampered {
            metrics::counter!("sprig_docs_verify_failures_total").increment(1);
            tracing::warn!(event = "docs.pack.tampered", %cluster, version = %pack.version,
                "rows of the docs pack were changed; loading it again");
        }
        if report.loaded {
            let secs = started.elapsed().as_secs_f64();
            metrics::histogram!("sprig_docs_load_seconds").record(secs);
            tracing::info!(event = "docs.pack.loaded", %cluster, version = %pack.version,
                chunks = pack.rows.len(), batches = report.batches, seconds = secs,
                "docs pack loaded");
        }
        if !report.retired.is_empty() {
            tracing::info!(%cluster, retired = ?report.retired, "old docs versions retired");
        }
        self.verified
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(cluster.clone(), pack.digest);
        Ok(report)
    }
}

#[async_trait::async_trait]
impl Reconciler for DocsLoader {
    type Key = ClusterId;

    fn name(&self) -> &'static str {
        "docs_loader"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        vec![WatchSpec::dc("clusters/")]
    }

    fn keys_for(&self, ev: &MetaEvent) -> Vec<ClusterId> {
        if ev.deleted() {
            return Vec::new();
        }
        ev.path()
            .strip_prefix("clusters/")
            .and_then(|s| ClusterId::new(s).ok())
            .into_iter()
            .collect()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<ClusterId>> {
        self.clusters.clusters().await
    }

    async fn reconcile(&self, cx: &LoopCx, key: &ClusterId) -> Result<Outcome> {
        cx.leader()?;
        if !self.enabled() {
            return Ok(Outcome::RequeueAfter(DISABLED_RECHECK));
        }
        // A cluster that left the registry (or was rejected) is skipped.
        if !self.clusters.clusters().await?.contains(key) {
            return Ok(Outcome::Done);
        }
        let Ok(pack) = self.pack() else {
            return Ok(Outcome::Blocked {
                reason: "docs pack unavailable",
            });
        };
        match tokio::time::timeout(PASS_LIMIT, self.sync(key, &pack)).await {
            Ok(r) => r.map(|_| Outcome::RequeueAfter(RECHECK)),
            Err(_) => Err(SprigError::new(
                SprigCode::BackendUnavailable,
                format!(
                    "docs: loading cluster {key} took over {} s",
                    PASS_LIMIT.as_secs()
                ),
            )),
        }
    }

    fn workers(&self) -> usize {
        4
    }
}

#[cfg(test)]
mod tests;
