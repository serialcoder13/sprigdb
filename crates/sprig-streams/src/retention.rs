//! Retention and compaction (chapter 45, "Retention and compaction"; build
//! step 7). The sequencer job of each log shard runs [`maintain`] every
//! minute:
//!
//! ```text
//! days ahead:  REORGANIZE PARTITION pmax INTO (p<day>, pmax) until today + 3 exists
//! time:        DROP PARTITION p<day> for days ending before now − retention_hours
//! size:        while the log's bytes > retention_bytes × partitions, drop the
//!              oldest day before today
//! log start:   per partition, the smallest offset left (next_offset when empty)
//! compaction:  CLEANUP compact: delete records older than compaction_lag_minutes
//!              that a later record with the same key supersedes; tombstones
//!              older than tombstone_hours when they are the key's latest
//! outbox:      delete outbox rows older than a minute whose record is in the
//!              log (rows the sequencer's own cleanup missed)
//! ```
//!
//! **As built:** size retention is measured per day partition of the whole
//! log (`information_schema.PARTITIONS`), against `retention_bytes ×
//! partitions`: MariaDB doesn't size one stream partition inside a day.
//! Offsets keep their gaps after compaction, as in Kafka.

use mysql_async::prelude::Queryable;
use sprig_common::Result;
use sprig_common::names::quote;

use crate::ddl::{
    DAYS_AHEAD, day_literal, day_of_ms, day_of_partition, hex_literal, partition_name,
};
use crate::maria::map_err;
use crate::model::{
    CKPT_TABLE, Cleanup, PartitionNo, SHARD_ROW, StreamId, log_table, outbox_table,
};
use crate::partition;
use crate::settings::Retention;

/// What one pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Maintained {
    /// Day partitions added.
    pub added: u32,
    /// Day partitions dropped (time or size).
    pub dropped: u32,
    /// Records removed by compaction.
    pub compacted: u64,
    /// Outbox rows swept.
    pub swept: u64,
}

/// The log's day partitions (day number, name) in order, and whether
/// `pmax` exists.
async fn days<Q: Queryable + Send>(
    q: &mut Q,
    schema: &str,
    table: &str,
) -> Result<Vec<(i64, String, u64)>> {
    let rows: Vec<(String, Option<u64>)> = q
        .exec(
            "SELECT PARTITION_NAME, DATA_LENGTH + INDEX_LENGTH FROM information_schema.PARTITIONS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL \
             ORDER BY PARTITION_ORDINAL_POSITION",
            (schema, table),
        )
        .await
        .map_err(map_err)?;
    Ok(rows
        .into_iter()
        .filter_map(|(n, b)| Some((day_of_partition(&n)?, n, b.unwrap_or(0))))
        .collect())
}

/// One maintenance pass over stream `id`'s log in `schema` (see the module
/// docs). `now_ms` is the wall clock.
///
/// # Errors
/// The backend's.
#[allow(clippy::too_many_arguments)]
pub async fn maintain<Q: Queryable + Send>(
    q: &mut Q,
    schema: &str,
    outbox_schemas: &[String],
    id: StreamId,
    partitions: u32,
    cleanup: Cleanup,
    r: &Retention,
    now_ms: i64,
) -> Result<Maintained> {
    let table = log_table(id);
    let full = format!("{}.{}", quote(schema), quote(&table));
    let mut out = Maintained::default();
    let today = day_of_ms(now_ms);
    // Days ahead.
    let mut have = days(q, schema, &table).await?;
    let last = have.last().map_or(today - 1, |d| d.0);
    for d in (last + 1)..=(today + DAYS_AHEAD) {
        q.query_drop(format!(
            "ALTER TABLE {full} REORGANIZE PARTITION pmax INTO (PARTITION {} VALUES LESS THAN ({}), \
             PARTITION pmax VALUES LESS THAN (MAXVALUE))",
            partition_name(d),
            day_literal(d + 1)
        ))
        .await
        .map_err(map_err)?;
        out.added += 1;
    }
    if out.added > 0 {
        have = days(q, schema, &table).await?;
    }
    // Time retention: a day ends at (d + 1) days.
    let horizon = now_ms - i64::try_from(r.hours.saturating_mul(3_600_000)).unwrap_or(i64::MAX);
    let mut drop: Vec<String> = have
        .iter()
        .filter(|(d, _, _)| (d + 1) * 86_400_000 <= horizon && *d < today)
        .map(|(_, n, _)| n.clone())
        .collect();
    // Size retention.
    if r.bytes > 0 {
        let limit = r.bytes.saturating_mul(u64::from(partitions.max(1)));
        let mut total: u64 = have
            .iter()
            .filter(|(_, n, _)| !drop.contains(n))
            .map(|(_, _, b)| *b)
            .sum();
        for (d, n, b) in &have {
            if total <= limit || *d >= today {
                break;
            }
            if !drop.contains(n) {
                drop.push(n.clone());
                total = total.saturating_sub(*b);
            }
        }
    }
    if !drop.is_empty() {
        q.query_drop(format!(
            "ALTER TABLE {full} DROP PARTITION {}",
            drop.join(", ")
        ))
        .await
        .map_err(map_err)?;
        out.dropped = u32::try_from(drop.len()).unwrap_or(u32::MAX);
        metrics::counter!("sprig_stream_retention_partitions_dropped_total")
            .increment(u64::from(out.dropped));
    }
    // Compaction.
    if cleanup == Cleanup::Compact {
        let lag = now_ms
            - i64::try_from(r.compaction_lag_minutes.saturating_mul(60_000)).unwrap_or(i64::MAX);
        let tomb =
            now_ms - i64::try_from(r.tombstone_hours.saturating_mul(3_600_000)).unwrap_or(i64::MAX);
        for p in 0..partitions {
            let n = compact_partition(q, &full, PartitionNo(p), lag, tomb).await?;
            out.compacted += n;
        }
        if out.compacted > 0 {
            metrics::counter!("sprig_stream_compaction_records_removed_total")
                .increment(out.compacted);
        }
    }
    // Log start.
    if out.dropped > 0 || out.compacted > 0 {
        let ckpt = format!("{}.{}", quote(schema), quote(CKPT_TABLE));
        q.exec_drop(
            format!(
                "UPDATE {ckpt} c SET c.log_start = COALESCE((SELECT MIN(l.offset_no) FROM {full} l \
                 WHERE l.partition_no = c.partition_no), c.next_offset) \
                 WHERE c.stream_id = ? AND c.partition_no <> ?"
            ),
            (id.0, SHARD_ROW),
        )
        .await
        .map_err(map_err)?;
    }
    // Outbox sweep.
    for o in outbox_schemas {
        out.swept += sweep_outbox(q, o, &full, id, partitions, now_ms - 60_000).await?;
    }
    Ok(out)
}

fn ms_literal(ms: i64) -> String {
    crate::maria::datetime_literal(ms)
}

/// Compacts one partition: records older than `lag_ms` superseded by a
/// later record of the same key, and tombstones older than `tomb_ms` that
/// are their key's latest. Returns the records removed.
async fn compact_partition<Q: Queryable + Send>(
    q: &mut Q,
    full: &str,
    p: PartitionNo,
    lag_ms: i64,
    tomb_ms: i64,
) -> Result<u64> {
    let mut removed = 0u64;
    loop {
        let r = q
            .query_iter(format!(
                "DELETE l FROM {full} l JOIN (SELECT k, MAX(offset_no) AS last FROM {full} \
                 WHERE partition_no = {pn} AND k IS NOT NULL GROUP BY k) m \
                 ON l.k = m.k AND l.offset_no < m.last \
                 WHERE l.partition_no = {pn} AND l.ts < {lag}",
                pn = p.0,
                lag = ms_literal(lag_ms)
            ))
            .await
            .map_err(map_err)?;
        let n = r.affected_rows();
        drop(r);
        removed += n;
        if n == 0 {
            break;
        }
    }
    let r = q
        .query_iter(format!(
            "DELETE FROM {full} WHERE partition_no = {} AND v IS NULL AND ts < {}",
            p.0,
            ms_literal(tomb_ms)
        ))
        .await
        .map_err(map_err)?;
    removed += r.affected_rows();
    Ok(removed)
}

/// Deletes outbox rows older than `before_ms` whose record is in the log.
async fn sweep_outbox<Q: Queryable + Send>(
    q: &mut Q,
    schema: &str,
    log: &str,
    id: StreamId,
    partitions: u32,
    before_ms: i64,
) -> Result<u64> {
    let outbox = format!("{}.{}", quote(schema), quote(&outbox_table(id)));
    let rows: Vec<(Vec<u8>, Option<u32>, Option<Vec<u8>>)> = q
        .query(format!(
            "SELECT event_id, partition_no, k FROM {outbox} WHERE ts < {} LIMIT 1000",
            ms_literal(before_ms)
        ))
        .await
        .map_err(map_err)?;
    if rows.is_empty() {
        return Ok(0);
    }
    let mut pairs = Vec::new();
    for (eid, p, k) in &rows {
        let pn = partition::of_record(*p, k.as_deref(), eid, partitions);
        let mut src = vec![b'e'];
        src.extend_from_slice(eid);
        pairs.push(format!("({}, {})", pn.0, hex_literal(&src)));
    }
    let found: Vec<(Vec<u8>,)> = q
        .query(format!(
            "SELECT source_event FROM {log} WHERE (partition_no, source_event) IN ({})",
            pairs.join(",")
        ))
        .await
        .map_err(map_err)?;
    if found.is_empty() {
        return Ok(0);
    }
    let ids: Vec<String> = found
        .iter()
        .filter_map(|(s,)| s.strip_prefix(b"e").map(hex_literal))
        .collect();
    let r = q
        .query_iter(format!(
            "DELETE FROM {outbox} WHERE event_id IN ({})",
            ids.join(",")
        ))
        .await
        .map_err(map_err)?;
    Ok(r.affected_rows())
}
