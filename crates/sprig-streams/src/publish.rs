//! Publishing (chapter 45, "Publishing inside a transaction"): the one
//! interface every front end publishes through. SQL's `INSERT INTO
//! <stream>`, `db.publish` (chapter 41), MongoDB inserts on a stream
//! (chapter 30) and Kafka's Produce share:
//!
//! - [`check_target`]: change topics are written only by SprigDB (39941);
//! - [`check_record`]: keys up to 4096 bytes and records up to
//!   `stream.max_message_bytes` (39944), keys on compacted streams (39955);
//! - [`headers_json`]: record headers as the outbox's `headers` JSON
//!   object, and [`headers_of_json`] back;
//! - the outbox row itself: [`outbox_insert`] for a session's own
//!   transaction (`db.publish` through a [`Publisher`]), and [`append`]
//!   for Kafka, which writes the outbox directly with the idempotent
//!   producer's check.
//!
//! SQL's `INSERT INTO <stream>` keeps its own rewrite (the client's
//! statement with the outbox as its target, so every insert shape works),
//! with [`check_target`] and the size check on its literals.
//!
//! **Idempotent producers** (Kafka's `enable.idempotence`). Kafka keeps,
//! per producer id and partition, the producer's epoch, its last sequence
//! number and its last five batches. Here that state is a row of the
//! stream's producer table, `__sprig_eo_<id>_pid`, next to the outbox in
//! the same shard schema (so the mover copies it with the outbox), and
//! [`append`] locks the row, applies Kafka's rules ([`check_sequence`]),
//! inserts the outbox rows and updates the row in one transaction:
//!
//! - an older epoch: `INVALID_PRODUCER_EPOCH`;
//! - a newer epoch must start at sequence 0;
//! - a batch equal to one of the last five (same first and last sequence)
//!   is a retry: nothing is written, and the original batch's offsets are
//!   returned;
//! - any other sequence than the last plus one: `OUT_OF_ORDER_SEQUENCE_NUMBER`;
//! - a producer the table doesn't know may start at any sequence (KIP-360:
//!   its state may have expired).
//!
//! Producer rows untouched for 7 days are deleted as appends pass by.

use std::fmt::Write as _;

use base64::Engine as _;
use mysql_async::prelude::Queryable;
use mysql_async::{TxOpts, Value};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};

use crate::ddl::{civil, day_of_ms, hex_literal};
use crate::maria::map_err;
use crate::model::{Cleanup, Source, StreamDef, StreamId, StreamKind, log_table, outbox_table};

/// The longest key (the outbox's `k VARBINARY(4096)`).
pub const MAX_KEY_BYTES: usize = 4096;
/// Batches remembered per producer and partition, as Kafka does.
pub const RECENT_BATCHES: usize = 5;
/// Producer rows older than this are deleted.
pub const PRODUCER_EXPIRY_DAYS: u32 = 7;

/// One record to publish.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Publish {
    /// The key.
    pub key: Option<Vec<u8>>,
    /// The value (`None`: a tombstone).
    pub value: Option<Vec<u8>>,
    /// Headers as a JSON object.
    pub headers: Option<String>,
    /// A partition the producer chose.
    pub partition: Option<u32>,
    /// The producer's timestamp (ms since the Unix epoch); `None`: the
    /// server's time.
    pub ts_ms: Option<i64>,
}

/// Change topics are written only by SprigDB.
///
/// # Errors
/// 39941.
pub fn check_target(def: &StreamDef) -> Result<()> {
    if def.kind == StreamKind::Changes {
        return Err(SprigError::new(
            SprigCode::StreamAppendOnly,
            format!(
                "ER_SPRIG_STREAM_APPEND_ONLY: '{}' is a change topic: only SprigDB writes it",
                def.name
            ),
        ));
    }
    Ok(())
}

/// A record's size limits: key bytes, and key + value + `extra` (header)
/// bytes against `max_message_bytes`; a key on a compacted stream.
///
/// # Errors
/// 39944, 39955.
pub fn check_record(
    def: &StreamDef,
    key: Option<&[u8]>,
    value_len: usize,
    extra: usize,
    max_message_bytes: u64,
) -> Result<()> {
    let k = key.map_or(0, <[u8]>::len);
    if k > MAX_KEY_BYTES {
        return Err(SprigError::new(
            SprigCode::StreamRecordTooLarge,
            format!(
                "ER_SPRIG_STREAM_RECORD_TOO_LARGE: a key of {k} bytes is above {MAX_KEY_BYTES}"
            ),
        ));
    }
    let size = (k + value_len + extra) as u64;
    if size > max_message_bytes {
        return Err(SprigError::new(
            SprigCode::StreamRecordTooLarge,
            format!(
                "ER_SPRIG_STREAM_RECORD_TOO_LARGE: a record of {size} bytes is above \
                 stream.max_message_bytes ({max_message_bytes})"
            ),
        ));
    }
    if def.cleanup == Cleanup::Compact && key.is_none() {
        return Err(SprigError::new(
            SprigCode::StreamCompactNeedsKey,
            "ER_SPRIG_STREAM_COMPACT_NEEDS_KEY: a record without a key on a compacted stream",
        ));
    }
    Ok(())
}

/// Record headers as the outbox's `headers` JSON object: a UTF-8 value is
/// a string, a null value `null`, other bytes `{"base64": "..."}`. A key
/// given twice keeps its last value (a JSON object has one value per
/// key). `None` for no headers.
#[must_use]
pub fn headers_json(headers: &[(String, Option<Vec<u8>>)]) -> Option<String> {
    if headers.is_empty() {
        return None;
    }
    let mut m = serde_json::Map::new();
    for (k, v) in headers {
        let j = match v {
            None => serde_json::Value::Null,
            Some(b) => match std::str::from_utf8(b) {
                Ok(s) => serde_json::Value::String(s.to_owned()),
                Err(_) => serde_json::json!({
                    "base64": base64::engine::general_purpose::STANDARD.encode(b)
                }),
            },
        };
        m.insert(k.clone(), j);
    }
    Some(serde_json::Value::Object(m).to_string())
}

/// Headers back from the `headers` JSON: strings as their bytes, `null`
/// as no value, `{"base64": ...}` decoded, anything else as its JSON text
/// (what SQL publishers may store).
#[must_use]
pub fn headers_of_json(json: &str) -> Vec<(String, Option<Vec<u8>>)> {
    let Ok(serde_json::Value::Object(m)) = serde_json::from_str::<serde_json::Value>(json) else {
        return Vec::new();
    };
    m.into_iter()
        .map(|(k, v)| {
            let b = match v {
                serde_json::Value::Null => None,
                serde_json::Value::String(s) => Some(s.into_bytes()),
                other => {
                    let decoded = match &other {
                        serde_json::Value::Object(o) if o.len() == 1 => o
                            .get("base64")
                            .and_then(serde_json::Value::as_str)
                            .and_then(|b| base64::engine::general_purpose::STANDARD.decode(b).ok()),
                        _ => None,
                    };
                    Some(decoded.unwrap_or_else(|| other.to_string().into_bytes()))
                }
            };
            (k, b)
        })
        .collect()
}

/// The outbox insert of `records` into stream `def` (in logical database
/// `db` when given), for a session's own transaction. `None` for a change
/// topic, which only SprigDB writes (39941).
#[must_use]
pub fn outbox_insert(def: &StreamDef, db: Option<&str>, records: &[Publish]) -> Option<String> {
    if check_target(def).is_err() || records.is_empty() {
        return None;
    }
    let table = match db {
        Some(d) => format!("{}.{}", quote(d), quote(&def.outbox_table())),
        None => quote(&def.outbox_table()),
    };
    let lit = |b: &Option<Vec<u8>>| b.as_deref().map_or_else(|| "NULL".to_owned(), hex_literal);
    let with_ts = records.iter().any(|r| r.ts_ms.is_some());
    let rows: Vec<String> = records
        .iter()
        .map(|r| {
            let mut row = format!(
                "({}, {}, {}, {}",
                lit(&r.key),
                lit(&r.value),
                r.headers.as_deref().map_or_else(
                    || "NULL".to_owned(),
                    |h| format!("CONVERT({} USING utf8mb4)", hex_literal(h.as_bytes()))
                ),
                r.partition.map_or_else(
                    || "NULL".to_owned(),
                    |p| (p % def.partitions.max(1)).to_string()
                )
            );
            if with_ts {
                let _ = write!(
                    row,
                    ", {}",
                    r.ts_ms.map_or_else(
                        || "UTC_TIMESTAMP(3)".to_owned(),
                        crate::maria::datetime_literal
                    )
                );
            }
            row.push(')');
            row
        })
        .collect();
    Some(format!(
        "INSERT INTO {table} (k, v, headers, partition_no{}) VALUES {}",
        if with_ts { ", ts" } else { "" },
        rows.join(", ")
    ))
}

/// Publishes inside the caller's transaction (the interface for
/// `db.publish`, MongoDB inserts on a stream, and Kafka produce).
#[async_trait::async_trait]
pub trait Publisher: Send {
    /// Publishes `records` to stream `name` of database `db`; they exist
    /// only if the caller's transaction commits.
    ///
    /// # Errors
    /// 39940 for an unknown stream, 39941 for a change topic, 39944 above
    /// `stream.max_message_bytes`, and the session's errors.
    async fn publish(&mut self, db: &str, name: &str, records: &[Publish]) -> Result<u64>;
}

/// The producer table of stream `id`: `__sprig_eo_<id>_pid` (named under
/// the outbox's prefix, so the mover copies it with the outbox).
#[must_use]
pub fn producer_table(id: StreamId) -> String {
    format!("{}_pid", outbox_table(id))
}

/// The producer table of stream `id` in `schema`.
#[must_use]
pub fn producer_table_sql(schema: &str, id: StreamId) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (
  producer_id    BIGINT UNSIGNED NOT NULL,
  partition_no   INT UNSIGNED NOT NULL,
  producer_epoch SMALLINT UNSIGNED NOT NULL,
  last_seq       INT NOT NULL,
  recent         VARBINARY(255) NOT NULL,
  updated_at     DATETIME(3) NOT NULL,
  PRIMARY KEY (producer_id, partition_no)
) ENGINE=InnoDB",
        quote(schema),
        quote(&producer_table(id))
    )
}

/// The producer table's drop.
#[must_use]
pub fn drop_producer_table_sql(schema: &str, id: StreamId) -> String {
    format!(
        "DROP TABLE IF EXISTS {}.{}",
        quote(schema),
        quote(&producer_table(id))
    )
}

/// An idempotent producer's batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerBatch {
    /// The producer id.
    pub producer_id: i64,
    /// Its epoch.
    pub epoch: i16,
    /// The first record's sequence.
    pub base_sequence: i32,
    /// Records in the batch.
    pub count: i32,
}

impl ProducerBatch {
    /// The last record's sequence (sequences wrap at `i32::MAX`, as in
    /// Kafka).
    #[must_use]
    pub fn last_sequence(&self) -> i32 {
        let last = i64::from(self.base_sequence) + i64::from(self.count) - 1;
        i32::try_from(last % (i64::from(i32::MAX) + 1)).unwrap_or(0)
    }
}

/// A producer's state for one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerState {
    /// The epoch.
    pub epoch: i16,
    /// The last sequence appended.
    pub last_seq: i32,
    /// The last batches: first and last sequence, first event id.
    pub recent: Vec<(i32, i32, [u8; 16])>,
}

impl ProducerState {
    /// The `recent` column.
    #[must_use]
    pub fn recent_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(self.recent.len() * 24);
        for (f, l, id) in &self.recent {
            b.extend_from_slice(&f.to_be_bytes());
            b.extend_from_slice(&l.to_be_bytes());
            b.extend_from_slice(id);
        }
        b
    }

    /// From the `recent` column (malformed entries are skipped).
    #[must_use]
    pub fn parse_recent(b: &[u8]) -> Vec<(i32, i32, [u8; 16])> {
        b.chunks_exact(24)
            .filter_map(|c| {
                Some((
                    i32::from_be_bytes(c[..4].try_into().ok()?),
                    i32::from_be_bytes(c[4..8].try_into().ok()?),
                    c[8..].try_into().ok()?,
                ))
            })
            .collect()
    }

    /// The state after appending `b` with first event `first`.
    #[must_use]
    pub fn after(state: Option<&Self>, b: &ProducerBatch, first: [u8; 16]) -> Self {
        let mut recent = match state {
            Some(s) if s.epoch == b.epoch => s.recent.clone(),
            _ => Vec::new(),
        };
        recent.push((b.base_sequence, b.last_sequence(), first));
        if recent.len() > RECENT_BATCHES {
            recent.remove(0);
        }
        Self {
            epoch: b.epoch,
            last_seq: b.last_sequence(),
            recent,
        }
    }
}

/// What Kafka's rules say about a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceVerdict {
    /// Append it.
    Append,
    /// A retry of a batch already appended: its first event id.
    Duplicate([u8; 16]),
    /// An epoch older than the producer's current one.
    StaleEpoch {
        /// The producer's epoch.
        current: i16,
    },
    /// Not the next sequence.
    OutOfOrder {
        /// The sequence expected.
        expected: i32,
    },
}

/// Kafka's idempotent producer rules (see the module docs).
#[must_use]
pub fn check_sequence(state: Option<&ProducerState>, b: &ProducerBatch) -> SequenceVerdict {
    let Some(s) = state else {
        return SequenceVerdict::Append;
    };
    if b.epoch < s.epoch {
        return SequenceVerdict::StaleEpoch { current: s.epoch };
    }
    if b.epoch > s.epoch {
        return if b.base_sequence == 0 {
            SequenceVerdict::Append
        } else {
            SequenceVerdict::OutOfOrder { expected: 0 }
        };
    }
    let last = b.last_sequence();
    if let Some((_, _, id)) = s
        .recent
        .iter()
        .find(|(f, l, _)| *f == b.base_sequence && *l == last)
    {
        return SequenceVerdict::Duplicate(*id);
    }
    let expected = if s.last_seq == i32::MAX {
        0
    } else {
        s.last_seq + 1
    };
    if b.base_sequence == expected {
        SequenceVerdict::Append
    } else {
        SequenceVerdict::OutOfOrder { expected }
    }
}

/// What [`append`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendOutcome {
    /// Written: the first record's event id.
    Appended([u8; 16]),
    /// A retried batch: the original's first event id; nothing written.
    Duplicate([u8; 16]),
    /// Refused by the sequence rules.
    Refused(SequenceVerdict),
}

/// `DATETIME(3)` of a time in ms since the Unix epoch, as a parameter.
#[must_use]
pub fn datetime_value(ms: i64) -> Value {
    let day = day_of_ms(ms);
    let (y, m, d) = civil(day);
    let rest = ms - day * 86_400_000;
    Value::Date(
        u16::try_from(y).unwrap_or(1970),
        u8::try_from(m).unwrap_or(1),
        u8::try_from(d).unwrap_or(1),
        u8::try_from(rest / 3_600_000).unwrap_or(0),
        u8::try_from(rest / 60_000 % 60).unwrap_or(0),
        u8::try_from(rest / 1000 % 60).unwrap_or(0),
        u32::try_from(rest % 1000 * 1000).unwrap_or(0),
    )
}

/// Rows per outbox insert (9 parameters each).
const ROWS_PER_INSERT: usize = 1000;

fn missing_table(e: &SprigError) -> bool {
    e.protocol().mysql_errno == 1146
}

/// Appends `records` to partition `partition` of stream `def`'s outbox in
/// shard schema `schema`, in one transaction on `conn`; with `producer`,
/// under Kafka's idempotent producer rules (see the module docs). The
/// records get event ids here, so the caller can find their offsets
/// ([`locate`]) once sequenced.
///
/// # Errors
/// 39941 for a change topic; the backend's.
pub async fn append(
    conn: &mut mysql_async::Conn,
    schema: &str,
    def: &StreamDef,
    partition: u32,
    records: &[Publish],
    producer: Option<ProducerBatch>,
) -> Result<AppendOutcome> {
    check_target(def)?;
    if records.is_empty() {
        return Err(SprigError::internal("an empty append"));
    }
    match append_once(conn, schema, def, partition, records, producer).await {
        Err(e) if producer.is_some() && missing_table(&e) => {
            // A stream created before producer tables existed.
            conn.query_drop(producer_table_sql(schema, def.id))
                .await
                .map_err(map_err)?;
            append_once(conn, schema, def, partition, records, producer).await
        }
        r => r,
    }
}

async fn append_once(
    conn: &mut mysql_async::Conn,
    schema: &str,
    def: &StreamDef,
    partition: u32,
    records: &[Publish],
    producer: Option<ProducerBatch>,
) -> Result<AppendOutcome> {
    let mut tx = conn
        .start_transaction(TxOpts::default())
        .await
        .map_err(map_err)?;
    let pid_table = format!("{}.{}", quote(schema), quote(&producer_table(def.id)));
    let mut state = None;
    if let Some(b) = producer {
        let row: Option<(u16, i32, Vec<u8>)> = tx
            .exec_first(
                format!(
                    "SELECT producer_epoch, last_seq, recent FROM {pid_table} \
                     WHERE producer_id = ? AND partition_no = ? FOR UPDATE"
                ),
                (b.producer_id, partition),
            )
            .await
            .map_err(map_err)?;
        state = row.map(|(e, l, r)| ProducerState {
            epoch: i16::try_from(e).unwrap_or(i16::MAX),
            last_seq: l,
            recent: ProducerState::parse_recent(&r),
        });
        match check_sequence(state.as_ref(), &b) {
            SequenceVerdict::Append => {}
            SequenceVerdict::Duplicate(id) => {
                tx.rollback().await.map_err(map_err)?;
                return Ok(AppendOutcome::Duplicate(id));
            }
            other => {
                tx.rollback().await.map_err(map_err)?;
                return Ok(AppendOutcome::Refused(other));
            }
        }
    }
    let ids: Vec<[u8; 16]> = records
        .iter()
        .map(|_| *uuid::Uuid::now_v7().as_bytes())
        .collect();
    let table = format!("{}.{}", quote(schema), quote(&outbox_table(def.id)));
    let mut at = 0usize;
    for chunk in records.chunks(ROWS_PER_INSERT) {
        let mut sql = format!(
            "INSERT INTO {table} (event_id, partition_no, k, v, headers, ts, producer_id, \
             producer_epoch, producer_seq) VALUES "
        );
        let mut params: Vec<Value> = Vec::with_capacity(chunk.len() * 9);
        for (i, r) in chunk.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str("(?, ?, ?, ?, ?, ?, ?, ?, ?)");
            params.push(Value::Bytes(ids[at].to_vec()));
            params.push(Value::UInt(u64::from(partition)));
            params.push(r.key.clone().map_or(Value::NULL, Value::Bytes));
            params.push(r.value.clone().map_or(Value::NULL, Value::Bytes));
            params.push(
                r.headers
                    .clone()
                    .map_or(Value::NULL, |h| Value::Bytes(h.into_bytes())),
            );
            params.push(datetime_value(
                r.ts_ms.unwrap_or_else(crate::maria::wall_ms),
            ));
            match producer {
                Some(b) => {
                    params.push(Value::Int(b.producer_id));
                    params.push(Value::Int(i64::from(b.epoch)));
                    let seq = (i64::from(b.base_sequence) + i64::try_from(at).unwrap_or(0))
                        % (i64::from(i32::MAX) + 1);
                    params.push(Value::Int(seq));
                }
                None => {
                    params.push(Value::NULL);
                    params.push(Value::NULL);
                    params.push(Value::NULL);
                }
            }
            at += 1;
        }
        tx.exec_drop(sql, mysql_async::Params::Positional(params))
            .await
            .map_err(map_err)?;
    }
    if let Some(b) = producer {
        let next = ProducerState::after(state.as_ref(), &b, ids[0]);
        tx.exec_drop(
            format!(
                "INSERT INTO {pid_table} (producer_id, partition_no, producer_epoch, last_seq, \
                 recent, updated_at) VALUES (?, ?, ?, ?, ?, UTC_TIMESTAMP(3)) \
                 ON DUPLICATE KEY UPDATE producer_epoch = VALUES(producer_epoch), \
                 last_seq = VALUES(last_seq), recent = VALUES(recent), updated_at = VALUES(updated_at)"
            ),
            (
                b.producer_id,
                partition,
                next.epoch,
                next.last_seq,
                next.recent_bytes(),
            ),
        )
        .await
        .map_err(map_err)?;
    }
    tx.commit().await.map_err(map_err)?;
    Ok(AppendOutcome::Appended(ids[0]))
}

/// Deletes producer rows untouched for [`PRODUCER_EXPIRY_DAYS`] (a few at
/// a time).
///
/// # Errors
/// The backend's (1146 when the stream has no producer table yet).
pub async fn expire_producers<Q: Queryable + Send>(
    conn: &mut Q,
    schema: &str,
    id: StreamId,
) -> Result<u64> {
    let r = conn
        .query_iter(format!(
            "DELETE FROM {}.{} WHERE updated_at < UTC_TIMESTAMP(3) - INTERVAL {PRODUCER_EXPIRY_DAYS} DAY LIMIT 1000",
            quote(schema),
            quote(&producer_table(id))
        ))
        .await
        .map_err(map_err)?;
    let n = r.affected_rows();
    drop(r);
    Ok(n)
}

/// Where an appended record is now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Located {
    /// In the log, at this offset.
    Offset(u64),
    /// Still in the outbox: not sequenced yet.
    Waiting,
    /// In neither (sequenced, then removed by retention).
    Gone,
}

/// The offset of the record with outbox event id `event` in partition
/// `partition` of stream `id`: `log_schema` holds the log, `outbox_schema`
/// the outbox.
///
/// # Errors
/// The backend's.
pub async fn locate<Q: Queryable + Send>(
    conn: &mut Q,
    log_schema: &str,
    outbox_schema: &str,
    id: StreamId,
    partition: u32,
    event: [u8; 16],
    check_outbox: bool,
) -> Result<Located> {
    let o: Option<u64> = conn
        .exec_first(
            format!(
                "SELECT offset_no FROM {}.{} WHERE partition_no = ? AND source_event = ?",
                quote(log_schema),
                quote(&log_table(id))
            ),
            (partition, Source::Outbox(event).to_bytes()),
        )
        .await
        .map_err(map_err)?;
    if let Some(o) = o {
        return Ok(Located::Offset(o));
    }
    if !check_outbox {
        return Ok(Located::Waiting);
    }
    let n: Option<u64> = conn
        .exec_first(
            format!(
                "SELECT COUNT(*) FROM {}.{} WHERE event_id = ?",
                quote(outbox_schema),
                quote(&outbox_table(id))
            ),
            (event.to_vec(),),
        )
        .await
        .map_err(map_err)?;
    Ok(if n.unwrap_or(0) > 0 {
        Located::Waiting
    } else {
        Located::Gone
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Envelope, StreamState};
    use sprig_common::ids::{DbId, GroupId};

    fn def(cleanup: Cleanup, kind: StreamKind) -> StreamDef {
        StreamDef {
            id: StreamId(41),
            db: DbId(1),
            name: "e".into(),
            kind,
            source_object: None,
            source_name: None,
            partitions: 4,
            key_spec: Vec::new(),
            attached_group: None,
            log_group: GroupId(1),
            cleanup,
            envelope: Envelope::Raw,
            state: StreamState::Active,
            outbox: Vec::new(),
            log: Vec::new(),
            created_at_ms: 0,
        }
    }

    #[test]
    fn statement() {
        let d = def(Cleanup::Delete, StreamKind::Events);
        let sql = outbox_insert(
            &d,
            Some("shop"),
            &[Publish {
                key: Some(b"k".to_vec()),
                partition: Some(6),
                ..Publish::default()
            }],
        )
        .unwrap();
        assert_eq!(
            sql,
            "INSERT INTO `shop`.`__sprig_eo_41` (k, v, headers, partition_no) VALUES (X'6B', NULL, NULL, 2)"
        );
        let sql = outbox_insert(
            &d,
            None,
            &[Publish {
                ts_ms: Some(1_762_508_100_123),
                ..Publish::default()
            }],
        )
        .unwrap();
        assert!(sql.ends_with("(NULL, NULL, NULL, NULL, '2025-11-07 09:35:00.123')"), "{sql}");
        assert_eq!(
            outbox_insert(&def(Cleanup::Delete, StreamKind::Changes), None, &[Publish::default()]),
            None
        );
    }

    #[test]
    fn checks() {
        let d = def(Cleanup::Compact, StreamKind::Events);
        assert!(check_record(&d, Some(b"k"), 10, 0, 100).is_ok());
        assert_eq!(
            check_record(&d, None, 10, 0, 100).unwrap_err().code(),
            Some(SprigCode::StreamCompactNeedsKey)
        );
        assert_eq!(
            check_record(&d, Some(b"k"), 100, 0, 100).unwrap_err().code(),
            Some(SprigCode::StreamRecordTooLarge)
        );
        assert_eq!(
            check_record(&d, Some(&[0; 4097]), 0, 0, 1 << 20)
                .unwrap_err()
                .code(),
            Some(SprigCode::StreamRecordTooLarge)
        );
        assert_eq!(
            check_target(&def(Cleanup::Delete, StreamKind::Changes))
                .unwrap_err()
                .code(),
            Some(SprigCode::StreamAppendOnly)
        );
    }

    #[test]
    fn headers_round_trip() {
        let h = vec![
            ("source".to_owned(), Some(b"checkout".to_vec())),
            ("bin".to_owned(), Some(vec![0xff, 0x00])),
            ("none".to_owned(), None),
        ];
        let j = headers_json(&h).unwrap();
        let mut back = headers_of_json(&j);
        back.sort();
        let mut want = h.clone();
        want.sort();
        assert_eq!(back, want);
        assert_eq!(headers_json(&[]), None);
        // What a SQL publisher stores.
        let mut sql = headers_of_json(r#"{"n": 7, "s": "x"}"#);
        sql.sort();
        assert_eq!(
            sql,
            vec![
                ("n".to_owned(), Some(b"7".to_vec())),
                ("s".to_owned(), Some(b"x".to_vec()))
            ]
        );
    }

    fn batch(epoch: i16, base: i32, count: i32) -> ProducerBatch {
        ProducerBatch {
            producer_id: 9,
            epoch,
            base_sequence: base,
            count,
        }
    }

    #[test]
    fn kafka_sequence_rules() {
        // Unknown producers start anywhere.
        assert_eq!(check_sequence(None, &batch(0, 17, 3)), SequenceVerdict::Append);
        let mut s = None;
        let mut ids = Vec::new();
        for (i, base) in [0, 3, 6, 9, 12, 15].into_iter().enumerate() {
            let b = batch(0, base, 3);
            assert_eq!(check_sequence(s.as_ref(), &b), SequenceVerdict::Append);
            let id = [u8::try_from(i).unwrap(); 16];
            ids.push(id);
            s = Some(ProducerState::after(s.as_ref(), &b, id));
        }
        let st = s.clone().unwrap();
        assert_eq!(st.last_seq, 17);
        assert_eq!(st.recent.len(), RECENT_BATCHES);
        // Retries of the last five are duplicates; older ones are out of
        // order, as are gaps.
        assert_eq!(
            check_sequence(s.as_ref(), &batch(0, 15, 3)),
            SequenceVerdict::Duplicate(ids[5])
        );
        assert_eq!(
            check_sequence(s.as_ref(), &batch(0, 3, 3)),
            SequenceVerdict::Duplicate(ids[1])
        );
        assert_eq!(
            check_sequence(s.as_ref(), &batch(0, 0, 3)),
            SequenceVerdict::OutOfOrder { expected: 18 }
        );
        assert_eq!(
            check_sequence(s.as_ref(), &batch(0, 19, 1)),
            SequenceVerdict::OutOfOrder { expected: 18 }
        );
        // A batch with the same start but another length isn't the same
        // batch.
        assert_eq!(
            check_sequence(s.as_ref(), &batch(0, 15, 2)),
            SequenceVerdict::OutOfOrder { expected: 18 }
        );
        // Epochs.
        let newer = ProducerState::after(s.as_ref(), &batch(1, 0, 1), [9; 16]);
        assert_eq!(
            check_sequence(Some(&newer), &batch(0, 1, 1)),
            SequenceVerdict::StaleEpoch { current: 1 }
        );
        assert_eq!(
            check_sequence(s.as_ref(), &batch(1, 5, 1)),
            SequenceVerdict::OutOfOrder { expected: 0 }
        );
        assert_eq!(newer.recent.len(), 1);
        // Wrapping at i32::MAX.
        let w = ProducerState::after(None, &batch(0, i32::MAX - 1, 2), [1; 16]);
        assert_eq!(w.last_seq, i32::MAX);
        assert_eq!(check_sequence(Some(&w), &batch(0, 0, 1)), SequenceVerdict::Append);
        assert_eq!(batch(0, i32::MAX, 3).last_sequence(), 1);
        // The column round-trips.
        assert_eq!(ProducerState::parse_recent(&st.recent_bytes()), st.recent);
    }

    #[test]
    fn datetime_parameter() {
        assert_eq!(
            datetime_value(1_762_508_100_123),
            Value::Date(2025, 11, 7, 9, 35, 0, 123_000)
        );
    }
}
