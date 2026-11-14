//! The stream log in MariaDB: [`MariaLog`], the [`LogStore`] of one
//! (stream, log shard) on the shard's cluster (sequencing and
//! offsets).
//!
//! ```text
//! acquire: BEGIN; SELECT ... FROM __sprig_sl_ckpt WHERE stream_id = s FOR UPDATE
//!          epoch := max(leader_epoch) + 1 → the shard row; COMMIT
//! append:  BEGIN
//!          SELECT leader_epoch ... shard row FOR UPDATE   -- the fence
//!          SELECT next_offset ... touched partitions FOR UPDATE
//!          SELECT source_event FROM log WHERE (partition, source) in the batch
//!          INSERT the new records at next_offset, next_offset + 1, ...
//!          UPDATE the partitions' next_offset; the shard row's resume_token
//!          COMMIT
//! ```
//!
//! Statements are text with hex literals, so batches of any size share no
//! prepared-statement shapes. The pool comes from a [`PoolSource`] on every
//! call, so after a move the log is written on the tenant's new cluster.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;
use std::sync::Arc;

use mysql_async::prelude::Queryable;
use mysql_async::{Pool, TxOpts};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};

use crate::ddl::{civil, day_of_ms, hex_literal};
use crate::model::{
    CKPT_TABLE, LeaderEpoch, NewRecord, Offset, PartitionNo, SHARD_ROW, StreamId, log_table,
    outbox_table,
};
use crate::sequencer::{Acquired, AppendOutcome, Appended, Batch, LogStore, Start};

/// The pool of the cluster holding a schema now.
#[async_trait::async_trait]
pub trait PoolSource: Send + Sync {
    /// The pool.
    ///
    /// # Errors
    /// The cluster is unknown or unreachable (retryable).
    async fn pool(&self) -> Result<Pool>;
}

/// A fixed pool.
#[derive(Debug, Clone)]
pub struct FixedPool(pub Pool);

#[async_trait::async_trait]
impl PoolSource for FixedPool {
    async fn pool(&self) -> Result<Pool> {
        Ok(self.0.clone())
    }
}

/// A backend error as a [`SprigError`], retryable when the server or the
/// connection is the problem (and for 1146: a move's fence renames the
/// schema for up to the hold).
#[must_use]
pub fn map_err(e: mysql_async::Error) -> SprigError {
    if let mysql_async::Error::Server(s) = &e {
        let mut state = *b"HY000";
        if s.state.len() == 5 {
            state.copy_from_slice(s.state.as_bytes());
        }
        let mut out = SprigError::backend(s.code, state, s.message.clone());
        out.retryable = matches!(
            s.code,
            1205 | 1213 | 1146 | 1049 | 1927 | 1053 | 1290 | 1836
        );
        return out.with_source(e);
    }
    let mut out = SprigError::new(SprigCode::BackendUnavailable, format!("stream log: {e}"));
    out.retryable = true;
    out.with_source(e)
}

/// `'YYYY-MM-DD hh:mm:ss.fff'` of a time in ms since the Unix epoch.
#[must_use]
pub fn datetime_literal(ms: i64) -> String {
    let day = day_of_ms(ms);
    let (y, m, d) = civil(day);
    let rest = ms - day * 86_400_000;
    format!(
        "'{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}.{:03}'",
        rest / 3_600_000,
        rest / 60_000 % 60,
        rest / 1000 % 60,
        rest % 1000
    )
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0))
}

/// The log of one (stream, log shard).
pub struct MariaLog {
    pools: Arc<dyn PoolSource>,
    stream: StreamId,
    schema: String,
    outbox_schemas: Vec<String>,
}

impl std::fmt::Debug for MariaLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MariaLog")
            .field("stream", &self.stream)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl MariaLog {
    /// Stream `stream`'s log in `schema`; its outbox in `outbox_schemas`.
    #[must_use]
    pub fn new(
        pools: Arc<dyn PoolSource>,
        stream: StreamId,
        schema: impl Into<String>,
        outbox_schemas: Vec<String>,
    ) -> Self {
        Self {
            pools,
            stream,
            schema: schema.into(),
            outbox_schemas,
        }
    }

    fn ckpt(&self) -> String {
        format!("{}.{}", quote(&self.schema), quote(CKPT_TABLE))
    }

    fn log(&self) -> String {
        format!("{}.{}", quote(&self.schema), quote(&log_table(self.stream)))
    }

    async fn conn(&self) -> Result<mysql_async::Conn> {
        self.pools.pool().await?.get_conn().await.map_err(map_err)
    }

    fn values(out: &mut String, r: &NewRecord, offset: u64, epoch: LeaderEpoch) {
        let day = day_of_ms(r.ts_ms);
        let (y, m, d) = civil(day);
        let _ = write!(
            out,
            "({},{offset},'{y:04}-{m:02}-{d:02}',{},{},{},{},{},{})",
            r.partition.0,
            datetime_literal(r.ts_ms),
            r.key
                .as_deref()
                .map_or_else(|| "NULL".into(), hex_literal_nonempty),
            r.value
                .as_deref()
                .map_or_else(|| "NULL".into(), hex_literal_nonempty),
            r.headers.as_deref().map_or_else(
                || "NULL".into(),
                |h| format!(
                    "CONVERT({} USING utf8mb4)",
                    hex_literal_nonempty(h.as_bytes())
                )
            ),
            hex_literal_nonempty(&r.source.to_bytes()),
            epoch.0
        );
    }
}

/// `X'..'`, and `''` for empty bytes (a present but empty key or value).
fn hex_literal_nonempty(b: &[u8]) -> String {
    hex_literal(b)
}

#[async_trait::async_trait]
impl LogStore for MariaLog {
    async fn acquire(&self) -> Result<Acquired> {
        let mut c = self.conn().await?;
        let mut tx = c
            .start_transaction(TxOpts::default())
            .await
            .map_err(map_err)?;
        let rows: Vec<(u32, u32, Vec<u8>)> = tx
            .exec(
                format!(
                    "SELECT partition_no, leader_epoch, resume_token FROM {} WHERE stream_id = ? FOR UPDATE",
                    self.ckpt()
                ),
                (self.stream.0,),
            )
            .await
            .map_err(map_err)?;
        let Some((_, _, resume)) = rows.iter().find(|(p, _, _)| *p == SHARD_ROW) else {
            let mut e = SprigError::new(
                SprigCode::StreamNotFound,
                format!(
                    "stream {} has no checkpoint in {}",
                    self.stream.0, self.schema
                ),
            );
            e.retryable = true;
            return Err(e);
        };
        let epoch = LeaderEpoch(rows.iter().map(|r| r.1).max().unwrap_or(0) + 1);
        tx.exec_drop(
            format!(
                "UPDATE {} SET leader_epoch = ? WHERE stream_id = ? AND partition_no = ?",
                self.ckpt()
            ),
            (epoch.0, self.stream.0, SHARD_ROW),
        )
        .await
        .map_err(map_err)?;
        let start = Start::decode(resume)?;
        tx.commit().await.map_err(map_err)?;
        Ok(Acquired { epoch, start })
    }

    #[allow(clippy::too_many_lines)]
    async fn append(&self, epoch: LeaderEpoch, batch: &Batch) -> Result<AppendOutcome> {
        let mut c = self.conn().await?;
        let mut tx = c
            .start_transaction(TxOpts::default())
            .await
            .map_err(map_err)?;
        let now: Option<u32> = tx
            .exec_first(
                format!(
                    "SELECT leader_epoch FROM {} WHERE stream_id = ? AND partition_no = ? FOR UPDATE",
                    self.ckpt()
                ),
                (self.stream.0, SHARD_ROW),
            )
            .await
            .map_err(map_err)?;
        let now = LeaderEpoch(now.unwrap_or(0));
        if now != epoch {
            tx.rollback().await.map_err(map_err)?;
            return Ok(AppendOutcome::Fenced { now });
        }
        let mut out = Appended::default();
        if !batch.records.is_empty() {
            let touched: BTreeSet<PartitionNo> =
                batch.records.iter().map(|r| r.partition).collect();
            let list: Vec<String> = touched.iter().map(|p| p.0.to_string()).collect();
            let rows: Vec<(u32, u64)> = tx
                .exec(
                    format!(
                        "SELECT partition_no, next_offset FROM {} WHERE stream_id = ? AND partition_no IN ({}) FOR UPDATE",
                        self.ckpt(),
                        list.join(",")
                    ),
                    (self.stream.0,),
                )
                .await
                .map_err(map_err)?;
            let mut next: BTreeMap<PartitionNo, u64> =
                rows.into_iter().map(|(p, o)| (PartitionNo(p), o)).collect();
            let missing: Vec<PartitionNo> = touched
                .iter()
                .filter(|p| !next.contains_key(p))
                .copied()
                .collect();
            for p in missing {
                // A partition added by ALTER STREAM before its row reached
                // this shard.
                tx.exec_drop(
                    format!(
                        "INSERT IGNORE INTO {} (stream_id, partition_no, next_offset, log_start, leader_epoch, resume_token) VALUES (?, ?, 0, 0, 0, '')",
                        self.ckpt()
                    ),
                    (self.stream.0, p.0),
                )
                .await
                .map_err(map_err)?;
                next.insert(p, 0);
            }
            // uk_source: what a replay would append twice.
            let mut seen: HashMap<(u32, Vec<u8>), ()> = HashMap::new();
            for chunk in batch.records.chunks(500) {
                let srcs: Vec<String> = chunk
                    .iter()
                    .map(|r| hex_literal(&r.source.to_bytes()))
                    .collect();
                let rows: Vec<(u32, Vec<u8>)> = tx
                    .query(format!(
                        "SELECT partition_no, source_event FROM {} WHERE partition_no IN ({}) AND source_event IN ({})",
                        self.log(),
                        list.join(","),
                        srcs.join(",")
                    ))
                    .await
                    .map_err(map_err)?;
                for r in rows {
                    seen.insert(r, ());
                }
            }
            let mut sql = String::new();
            let mut n = 0usize;
            let head = format!(
                "INSERT INTO {} (partition_no, offset_no, day, ts, k, v, headers, source_event, leader_epoch) VALUES ",
                self.log()
            );
            for r in &batch.records {
                let src = r.source.to_bytes();
                if seen.insert((r.partition.0, src), ()).is_some() {
                    out.duplicates += 1;
                    continue;
                }
                let o = next.entry(r.partition).or_insert(0);
                if n == 0 {
                    sql.clone_from(&head);
                } else {
                    sql.push(',');
                }
                Self::values(&mut sql, r, *o, epoch);
                *o += 1;
                n += 1;
                out.records += 1;
                out.high_watermarks.insert(r.partition, Offset(*o));
                if n >= 500 || sql.len() > 4 << 20 {
                    tx.query_drop(std::mem::take(&mut sql))
                        .await
                        .map_err(map_err)?;
                    n = 0;
                }
            }
            if n > 0 {
                tx.query_drop(sql).await.map_err(map_err)?;
            }
            for (p, o) in &out.high_watermarks {
                tx.exec_drop(
                    format!(
                        "UPDATE {} SET next_offset = ?, leader_epoch = ? WHERE stream_id = ? AND partition_no = ?",
                        self.ckpt()
                    ),
                    (o.0, epoch.0, self.stream.0, p.0),
                )
                .await
                .map_err(map_err)?;
            }
        }
        tx.exec_drop(
            format!(
                "UPDATE {} SET resume_token = ? WHERE stream_id = ? AND partition_no = ?",
                self.ckpt()
            ),
            (batch.resume.clone(), self.stream.0, SHARD_ROW),
        )
        .await
        .map_err(map_err)?;
        tx.commit().await.map_err(map_err)?;
        Ok(AppendOutcome::Appended(out))
    }

    async fn cleanup(&self, outbox_ids: &[[u8; 16]]) -> Result<()> {
        if outbox_ids.is_empty() {
            return Ok(());
        }
        let mut c = self.conn().await?;
        for schema in &self.outbox_schemas {
            for chunk in outbox_ids.chunks(1000) {
                let ids: Vec<String> = chunk.iter().map(|i| hex_literal(i)).collect();
                c.query_drop(format!(
                    "DELETE FROM {}.{} WHERE event_id IN ({})",
                    quote(schema),
                    quote(&outbox_table(self.stream)),
                    ids.join(",")
                ))
                .await
                .map_err(map_err)?;
            }
        }
        Ok(())
    }
}

/// Milliseconds since the Unix epoch now (for tests and the sweep).
#[must_use]
pub fn wall_ms() -> i64 {
    now_ms()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals() {
        assert_eq!(datetime_literal(0), "'1970-01-01 00:00:00.000'");
        assert_eq!(
            datetime_literal(1_762_508_100_123),
            "'2025-11-07 09:35:00.123'"
        );
    }
}
