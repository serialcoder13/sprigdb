//! ID blocks for `AUTO_INCREMENT` and sequences (chapter 19, "ID blocks").
//!
//! While a group has one shard MariaDB's own counter is used. From the
//! first split each `AUTO_INCREMENT` column has a `sequences` row in the
//! tenant catalog, and gateways take blocks of values from it: unique
//! (blocks never overlap), increasing per gateway, and consecutive within
//! a statement. [`BlockAllocator`] caches one block per sequence,
//! prefetches the next at half use, and sizes blocks to last about
//! `ids.block_target_seconds`. Strict sequences fetch every statement's
//! values from the catalog.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Pool, TxOpts};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use tokio::sync::Mutex;

use crate::groups::db_err;

/// `sequences` (chapter 19), in `t<tenant>_sprig`.
pub const DDL_SEQUENCES: &str = "CREATE TABLE IF NOT EXISTS sequences (
  seq_id        BIGINT UNSIGNED NOT NULL,
  object_id     BIGINT UNSIGNED NOT NULL,
  column_name   VARCHAR(64) NULL,
  kind          ENUM('auto_increment','sequence') NOT NULL,
  ordering      ENUM('blocks','strict') NOT NULL DEFAULT 'blocks',
  next_value    DECIMAL(20,0) NOT NULL,
  increment_by  DECIMAL(20,0) NOT NULL DEFAULT 1,
  min_value     DECIMAL(20,0) NOT NULL,
  max_value     DECIMAL(20,0) NOT NULL,
  cycle         TINYINT(1) NOT NULL DEFAULT 0,
  cache_size    BIGINT UNSIGNED NULL,
  updated_at    DATETIME(6) NOT NULL,
  PRIMARY KEY (seq_id),
  UNIQUE KEY uk_object (object_id, column_name)
) ENGINE=InnoDB";

/// A `sequences` row id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SeqId(pub u64);

/// Values handed to one statement: `first`, `first + step`, ... `count`
/// of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdRun {
    /// The first value (what `LAST_INSERT_ID()` reports).
    pub first: i128,
    /// The distance between values (`auto_increment_increment`).
    pub step: i128,
    /// How many.
    pub count: u64,
}

impl IdRun {
    /// The values in order.
    pub fn values(&self) -> impl Iterator<Item = i128> + '_ {
        (0..self.count).map(move |k| self.first + i128::from(k) * self.step)
    }

    /// The last value.
    #[must_use]
    pub fn last(&self) -> i128 {
        self.first + i128::from(self.count.saturating_sub(1)) * self.step
    }
}

/// The session's `auto_increment_increment` and `auto_increment_offset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoIncSession {
    /// `auto_increment_increment`, 1 to 65535.
    pub increment: u64,
    /// `auto_increment_offset`, 1 to 65535 (MySQL ignores it above the
    /// increment).
    pub offset: u64,
}

impl Default for AutoIncSession {
    fn default() -> Self {
        Self {
            increment: 1,
            offset: 1,
        }
    }
}

impl AutoIncSession {
    /// The smallest value `v ≥ from` in the series `offset + k × increment`.
    #[must_use]
    pub fn first_at_or_after(&self, from: i128) -> i128 {
        let inc = i128::from(self.increment.max(1));
        let off = i128::from(if self.offset > self.increment {
            1
        } else {
            self.offset.max(1)
        });
        if from <= off {
            return off;
        }
        let k = (from - off + inc - 1) / inc;
        off + k * inc
    }
}

/// How a sequence hands out values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ordering {
    /// Per-gateway blocks (the default).
    Blocks,
    /// Every statement fetches from the catalog: values increase in
    /// allocation order across gateways.
    Strict,
}

/// Hands out generated values.
#[async_trait::async_trait]
pub trait IdAllocator: Send + Sync {
    /// `count` values for one statement, consecutive in the session's
    /// series.
    async fn take(&self, seq: SeqId, count: u64, s: &AutoIncSession) -> Result<IdRun>;
    /// An explicit value `v` was written: make sure no later value is at
    /// or below it.
    async fn raise(&self, seq: SeqId, at_least: i128) -> Result<()>;
}

/// Where blocks come from: the catalog's `sequences` row.
#[async_trait::async_trait]
pub trait SequenceSource: Send + Sync + 'static {
    /// Reserves `size` values: returns `[start, start + size)`, clamped to
    /// `max_value`.
    async fn fetch(&self, seq: SeqId, size: u128) -> Result<(i128, i128)>;
    /// Raises `next_value` to at least `v + 1`.
    async fn raise(&self, seq: SeqId, v: i128) -> Result<()>;
    /// The sequence's ordering.
    async fn ordering(&self, seq: SeqId) -> Result<Ordering>;
}

/// Block-size limits (`ids.block_size_min`, `ids.block_size_max`,
/// `ids.block_target_seconds`).
#[derive(Debug, Clone, Copy)]
pub struct BlockConfig {
    /// Smallest block.
    pub min: u64,
    /// Largest block.
    pub max: u64,
    /// A block should last about this long.
    pub target: Duration,
}

impl Default for BlockConfig {
    fn default() -> Self {
        Self {
            min: 16,
            max: 65536,
            target: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Default)]
struct BlockState {
    /// The current block `[next, end)`.
    cur: Option<(i128, i128)>,
    /// The size of the current block.
    cur_size: u128,
    /// A prefetched block.
    spare: Option<(i128, i128)>,
    prefetching: bool,
    /// When the current block was installed.
    since: Option<Instant>,
    /// Cached values below this are dropped (`ALTER TABLE ... AUTO_INCREMENT`,
    /// explicit values).
    floor: i128,
    /// The catalog's `next_value` is known to be above this: the highest
    /// value this gateway raised it past.
    raised: i128,
    ordering: Option<Ordering>,
}

struct Inner<S> {
    src: S,
    cfg: BlockConfig,
    seqs: std::sync::Mutex<HashMap<SeqId, Arc<Mutex<BlockState>>>>,
    fetched: std::sync::atomic::AtomicU64,
}

/// A gateway's block cache over a [`SequenceSource`]. Cloning shares it.
pub struct BlockAllocator<S> {
    inner: Arc<Inner<S>>,
}

impl<S> Clone for BlockAllocator<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S> std::fmt::Debug for BlockAllocator<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockAllocator")
            .field("cfg", &self.inner.cfg)
            .finish_non_exhaustive()
    }
}

impl<S: SequenceSource> BlockAllocator<S> {
    /// A cache over `src`.
    #[must_use]
    pub fn new(src: S, cfg: BlockConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                src,
                cfg,
                seqs: std::sync::Mutex::default(),
                fetched: std::sync::atomic::AtomicU64::new(0),
            }),
        }
    }

    /// The source.
    #[must_use]
    pub fn source(&self) -> &S {
        &self.inner.src
    }

    /// Blocks fetched so far (`sprig_ids_blocks_fetched_total`).
    #[must_use]
    pub fn blocks_fetched(&self) -> u64 {
        self.inner
            .fetched
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Drops cached values below `n` (`ALTER TABLE ... AUTO_INCREMENT = n`).
    pub async fn invalidate_below(&self, seq: SeqId, n: i128) {
        let st = self.state(seq);
        let mut b = st.lock().await;
        b.floor = b.floor.max(n);
    }

    /// An explicit value `v` was written (chapter 19, "Explicit values"):
    /// this gateway's later values are above it, and the catalog's
    /// `next_value` is raised past it unless a block this gateway holds
    /// already ends above it (the catalog is then past it too). Raises go
    /// to the catalog once per new maximum.
    ///
    /// # Errors
    /// 39460 when the catalog can't be raised.
    pub async fn explicit(&self, seq: SeqId, v: i128) -> Result<()> {
        let st = self.state(seq);
        let mut b = st.lock().await;
        b.floor = b.floor.max(v.saturating_add(1));
        let held = b
            .cur
            .map(|(_, end)| end)
            .into_iter()
            .chain(b.spare.map(|(_, end)| end))
            .max()
            .unwrap_or(i128::MIN);
        if v < held || v < b.raised {
            return Ok(());
        }
        self.inner.src.raise(seq, v).await?;
        b.raised = b.raised.max(v.saturating_add(1));
        Ok(())
    }

    fn state(&self, seq: SeqId) -> Arc<Mutex<BlockState>> {
        let mut m = self
            .inner
            .seqs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(m.entry(seq).or_default())
    }

    /// The next block's size: the current one doubled when it would last
    /// under half of `ids.block_target_seconds`, halved when over twice
    /// that, within the limits.
    #[allow(clippy::cast_precision_loss)] // an estimate
    fn block_size(&self, b: &BlockState) -> u128 {
        let cfg = self.inner.cfg;
        let (min, max) = (u128::from(cfg.min), u128::from(cfg.max));
        let cur = b.cur_size.clamp(min, max);
        let (Some(t), Some((next, end))) = (b.since, b.cur) else {
            return cur;
        };
        let left = u128::try_from(end - next).unwrap_or(0);
        let used = b.cur_size.saturating_sub(left).max(1);
        // How long the whole block lasts at the rate seen so far.
        let lasts = t.elapsed().as_secs_f64() * (b.cur_size as f64 / used as f64);
        let target = cfg.target.as_secs_f64();
        let next = if lasts < target / 2.0 {
            cur * 2
        } else if lasts > target * 2.0 {
            cur / 2
        } else {
            cur
        };
        next.clamp(min, max)
    }

    /// Takes `count` values of the series out of `[next, end)`, if they fit.
    fn carve(b: &mut BlockState, count: u64, s: &AutoIncSession) -> Option<IdRun> {
        let (next, end) = b.cur?;
        let first = s.first_at_or_after(next.max(b.floor));
        let step = i128::from(s.increment.max(1));
        let last = first + i128::from(count.saturating_sub(1)) * step;
        if last >= end {
            return None;
        }
        b.cur = Some((last + 1, end));
        Some(IdRun { first, step, count })
    }

    fn maybe_prefetch(&self, seq: SeqId, b: &mut BlockState, st: &Arc<Mutex<BlockState>>) {
        let Some((next, end)) = b.cur else { return };
        let left = u128::try_from(end - next).unwrap_or(0);
        if b.spare.is_some() || b.prefetching || left * 2 > b.cur_size {
            return;
        }
        b.prefetching = true;
        let size = self.block_size(b);
        let this = self.clone();
        let st = Arc::clone(st);
        tokio::spawn(async move {
            let got = this.inner.src.fetch(seq, size).await;
            let mut b = st.lock().await;
            b.prefetching = false;
            match got {
                Ok(r) => {
                    this.inner
                        .fetched
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // A block fetched synchronously meanwhile is higher:
                    // keep values increasing per gateway, drop this one.
                    if b.cur.is_none_or(|(_, end)| r.0 >= end) {
                        b.spare = Some(r);
                    }
                }
                Err(e) => tracing::warn!(seq = seq.0, error = %e, "ID block prefetch failed"),
            }
        });
    }
}

#[async_trait::async_trait]
impl<S: SequenceSource> IdAllocator for BlockAllocator<S> {
    async fn take(&self, seq: SeqId, count: u64, s: &AutoIncSession) -> Result<IdRun> {
        if count == 0 {
            return Ok(IdRun {
                first: 0,
                step: 1,
                count: 0,
            });
        }
        let st = self.state(seq);
        let mut b = st.lock().await;
        if b.ordering.is_none() {
            b.ordering = Some(self.inner.src.ordering(seq).await?);
        }
        let step = u128::from(s.increment.max(1));
        let need = u128::from(count) * step + step;
        if b.ordering == Some(Ordering::Strict) {
            let (start, end) = self.inner.src.fetch(seq, need).await?;
            let mut tmp = BlockState {
                cur: Some((start, end)),
                floor: b.floor,
                ..BlockState::default()
            };
            return Self::carve(&mut tmp, count, s).ok_or_else(exhausted);
        }
        if let Some(run) = Self::carve(&mut b, count, s) {
            self.maybe_prefetch(seq, &mut b, &st);
            return Ok(run);
        }
        // The current block can't hold the statement: use the spare, else
        // fetch one big enough. The rest of the old block becomes a gap.
        for _ in 0..3 {
            if let Some(spare) = b.spare.take()
                && b.cur.is_none_or(|(_, end)| spare.0 >= end)
            {
                b.cur = Some(spare);
                b.cur_size = u128::try_from(spare.1 - spare.0).unwrap_or(0);
                b.since = Some(Instant::now());
                if let Some(run) = Self::carve(&mut b, count, s) {
                    self.maybe_prefetch(seq, &mut b, &st);
                    return Ok(run);
                }
            }
            let size = self.block_size(&b).max(need);
            let mut r = self.inner.src.fetch(seq, size).await?;
            if r.1 <= b.floor {
                // The catalog is behind an `AUTO_INCREMENT = n` this
                // gateway saw: raise it, then fetch again.
                self.inner.src.raise(seq, b.floor - 1).await?;
                r = self.inner.src.fetch(seq, size).await?;
            }
            self.inner
                .fetched
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            b.cur = Some(r);
            b.cur_size = size;
            b.since = Some(Instant::now());
            if let Some(run) = Self::carve(&mut b, count, s) {
                self.maybe_prefetch(seq, &mut b, &st);
                return Ok(run);
            }
        }
        Err(exhausted())
    }

    async fn raise(&self, seq: SeqId, at_least: i128) -> Result<()> {
        self.inner.src.raise(seq, at_least).await
    }
}

fn exhausted() -> SprigError {
    // MySQL's ER_AUTOINC_READ_FAILED.
    SprigError::standard(
        1467,
        *b"HY000",
        "Failed to read auto-increment value from storage engine",
    )
}

/// The catalog's `sequences` table as a [`SequenceSource`]: one pooled
/// connection per fetch, a `FOR UPDATE` read and an update in one
/// transaction. On Galera, concurrent fetches through different nodes
/// can fail certification (1213); the fetch retries.
#[derive(Debug, Clone)]
pub struct CatalogSequences {
    pool: Pool,
    schema: String,
}

impl CatalogSequences {
    /// Sequences in `schema` (`t<tenant>_sprig`) through `pool`.
    #[must_use]
    pub fn new(pool: Pool, schema: impl Into<String>) -> Self {
        Self {
            pool,
            schema: schema.into(),
        }
    }

    /// Creates a sequence row (at the first split: `next = MAX(col) + 1`).
    ///
    /// # Errors
    /// MariaDB's errors (1062 if it exists).
    pub async fn create(
        &self,
        seq: SeqId,
        object: u64,
        column: Option<&str>,
        next: i128,
        ordering: Ordering,
    ) -> Result<()> {
        let mut c = self.pool.get_conn().await.map_err(unavailable)?;
        c.exec_drop(
            format!(
                "INSERT INTO {}.sequences (seq_id, object_id, column_name, kind, ordering,
                   next_value, increment_by, min_value, max_value, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, 1, 1, 18446744073709551615, UTC_TIMESTAMP(6))",
                quote(&self.schema)
            ),
            (
                seq.0,
                object,
                column,
                if column.is_some() {
                    "auto_increment"
                } else {
                    "sequence"
                },
                match ordering {
                    Ordering::Blocks => "blocks",
                    Ordering::Strict => "strict",
                },
                next.to_string(),
            ),
        )
        .await
        .map_err(db_err)
    }

    /// Whether sequence `seq` has its row.
    ///
    /// # Errors
    /// 39460 when the catalog can't be reached; MariaDB's errors.
    pub async fn exists(&self, seq: SeqId) -> Result<bool> {
        let mut c = self.pool.get_conn().await.map_err(unavailable)?;
        let n: Option<u64> = c
            .exec_first(
                format!(
                    "SELECT COUNT(*) FROM {}.sequences WHERE seq_id = ?",
                    quote(&self.schema)
                ),
                (seq.0,),
            )
            .await
            .map_err(db_err)?;
        Ok(n.unwrap_or(0) > 0)
    }

    async fn fetch_once(&self, seq: SeqId, size: u128) -> Result<(i128, i128)> {
        let mut c = self.pool.get_conn().await.map_err(unavailable)?;
        let mut tx = c
            .start_transaction(TxOpts::default())
            .await
            .map_err(db_err)?;
        let row: Option<(String, String)> = tx
            .exec_first(
                format!(
                    "SELECT CAST(next_value AS CHAR), CAST(max_value AS CHAR) FROM {}.sequences
                     WHERE seq_id = ? FOR UPDATE",
                    quote(&self.schema)
                ),
                (seq.0,),
            )
            .await
            .map_err(db_err)?;
        let (next, max) = row.ok_or_else(|| {
            SprigError::new(
                SprigCode::IdBlockUnavailable,
                format!("sequence {} doesn't exist", seq.0),
            )
        })?;
        let next: i128 = next
            .parse()
            .map_err(|_| SprigError::internal("bad next_value"))?;
        let max: i128 = max
            .parse()
            .map_err(|_| SprigError::internal("bad max_value"))?;
        if next > max {
            return Err(exhausted());
        }
        let size = i128::try_from(size).unwrap_or(i128::MAX);
        let end = next.saturating_add(size).min(max + 1);
        tx.exec_drop(
            format!(
                "UPDATE {}.sequences SET next_value = ?, updated_at = UTC_TIMESTAMP(6)
                 WHERE seq_id = ?",
                quote(&self.schema)
            ),
            (end.to_string(), seq.0),
        )
        .await
        .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        Ok((next, end))
    }
}

#[allow(clippy::needless_pass_by_value)] // used with map_err
fn unavailable(e: mysql_async::Error) -> SprigError {
    SprigError::new(
        SprigCode::IdBlockUnavailable,
        format!("the catalog can't hand out an ID block: {e}"),
    )
}

/// Retries certification conflicts and deadlocks (Galera) a few times;
/// connection failures become 39460.
async fn retry<T, F, Fut>(mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut last = None;
    for attempt in 0..20u32 {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if e.retryable => {
                last = Some(e);
                // Exponential backoff with jitter, so two gateways that
                // lost the same certification race don't collide again.
                let jitter = u64::from(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |d| d.subsec_nanos())
                        % 1000,
                );
                let base = 1u64 << attempt.min(7);
                tokio::time::sleep(Duration::from_micros(base * 1000 + jitter * base)).await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last.unwrap_or_else(|| SprigError::internal("retry without attempts")))
}

#[async_trait::async_trait]
impl SequenceSource for CatalogSequences {
    async fn fetch(&self, seq: SeqId, size: u128) -> Result<(i128, i128)> {
        let r = retry(|| self.fetch_once(seq, size)).await;
        match r {
            Err(e) if e.code() == Some(SprigCode::BackendUnavailable) => Err(SprigError::new(
                SprigCode::IdBlockUnavailable,
                format!("the catalog can't hand out an ID block: {e}"),
            )),
            r => r,
        }
    }

    async fn raise(&self, seq: SeqId, v: i128) -> Result<()> {
        retry(|| async {
            let mut c = self.pool.get_conn().await.map_err(unavailable)?;
            c.exec_drop(
                format!(
                    "UPDATE {}.sequences SET next_value = GREATEST(next_value, ?),
                       updated_at = UTC_TIMESTAMP(6) WHERE seq_id = ?",
                    quote(&self.schema)
                ),
                ((v + 1).to_string(), seq.0),
            )
            .await
            .map_err(db_err)
        })
        .await
    }

    async fn ordering(&self, seq: SeqId) -> Result<Ordering> {
        let mut c = self.pool.get_conn().await.map_err(unavailable)?;
        let o: Option<String> = c
            .exec_first(
                format!(
                    "SELECT ordering FROM {}.sequences WHERE seq_id = ?",
                    quote(&self.schema)
                ),
                (seq.0,),
            )
            .await
            .map_err(db_err)?;
        Ok(match o.as_deref() {
            Some("strict") => Ordering::Strict,
            Some(_) => Ordering::Blocks,
            None => {
                return Err(SprigError::new(
                    SprigCode::IdBlockUnavailable,
                    format!("sequence {} doesn't exist", seq.0),
                ));
            }
        })
    }
}

#[cfg(test)]
mod tests;
