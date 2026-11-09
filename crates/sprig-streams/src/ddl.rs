//! Physical tables of a stream (chapter 45, "Outbox rows" and "The stream
//! log"; build steps 3 and 4): the statements that create and drop them in
//! a shard schema.
//!
//! **As built, against the book's tables:**
//!
//! - The outbox fills `event_id` and `ts` itself, with MariaDB's
//!   `UUID_v7()` and `UTC_TIMESTAMP(3)` as column defaults, so any
//!   `INSERT` shape (`VALUES`, `SELECT`, prepared, from a compiled routine)
//!   publishes without the gateway touching the values. Event ids are
//!   UUIDv7 from the writing server, not the gateway.
//! - `partition_no` may be NULL: the sequencer then partitions by key
//!   ([`crate::partition`]). Only producers that choose a partition (Kafka
//!   clients) fill it.
//! - A compacted stream's outbox refuses keyless records with a `CHECK`
//!   (`sprig_sc_needs_key`), which the gateway reports as 39955.
//! - `source_event` is `VARBINARY(64)`: `e` + the outbox event id for
//!   event streams, `c` + the feed's event id (cluster name, GTID, row) for
//!   change topics.
//! - The checkpoint table keeps one extra row per stream and log shard,
//!   `partition_no = 4294967295`: the shard's leader epoch and feed
//!   position, written by every batch; partition rows hold `next_offset`,
//!   `log_start` and the epoch of their last append.

use sprig_common::names::quote;

use crate::model::{CKPT_TABLE, Cleanup, PartitionNo, SHARD_ROW, StreamId, log_table, outbox_table};

/// Days of partitions kept ahead of today.
pub const DAYS_AHEAD: i64 = 3;

/// The outbox of stream `id` in `schema`.
#[must_use]
pub fn outbox_sql(schema: &str, id: StreamId, cleanup: Cleanup) -> String {
    let check = if cleanup == Cleanup::Compact {
        ",\n  CONSTRAINT sprig_sc_needs_key CHECK (k IS NOT NULL)"
    } else {
        ""
    };
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (
  event_id       BINARY(16) NOT NULL DEFAULT (UNHEX(REPLACE(UUID_v7(), '-', ''))),
  partition_no   INT UNSIGNED NULL,
  k              VARBINARY(4096) NULL,
  v              LONGBLOB NULL,
  headers        JSON NULL,
  ts             DATETIME(3) NOT NULL DEFAULT (UTC_TIMESTAMP(3)),
  producer_id    BIGINT UNSIGNED NULL,
  producer_epoch SMALLINT UNSIGNED NULL,
  producer_seq   INT UNSIGNED NULL,
  _sprig_rk      BIGINT UNSIGNED NULL INVISIBLE,
  PRIMARY KEY (event_id),
  UNIQUE KEY uk_producer (producer_id, producer_epoch, partition_no, producer_seq){check}
) ENGINE=InnoDB",
        quote(schema),
        quote(&outbox_table(id))
    )
}

/// `pYYYYMMDD` for day number `d` (days since 1970-01-01).
#[must_use]
pub fn partition_name(day: i64) -> String {
    let (y, m, d) = civil(day);
    format!("p{y:04}{m:02}{d:02}")
}

/// `'YYYY-MM-DD'` of day number `day`.
#[must_use]
pub fn day_literal(day: i64) -> String {
    let (y, m, d) = civil(day);
    format!("'{y:04}-{m:02}-{d:02}'")
}

/// The day number of `name` (`pYYYYMMDD`), if it is a day partition.
#[must_use]
pub fn day_of_partition(name: &str) -> Option<i64> {
    let digits = name.strip_prefix('p')?;
    if digits.len() != 8 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let y: i64 = digits[..4].parse().ok()?;
    let m: u32 = digits[4..6].parse().ok()?;
    let d: u32 = digits[6..].parse().ok()?;
    Some(days_from_civil(y, m, d))
}

/// Days since 1970-01-01 of a civil date (Howard Hinnant's algorithm).
#[must_use]
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = i64::from((m + 9) % 12);
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The civil date of day number `z`.
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub fn civil(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// The day number of a time in ms since the Unix epoch.
#[must_use]
pub fn day_of_ms(ms: i64) -> i64 {
    ms.div_euclid(86_400_000)
}

/// The stream log of stream `id` in `schema`, with day partitions from
/// `today - 1` to `today + DAYS_AHEAD` and `pmax`.
#[must_use]
pub fn log_sql(schema: &str, id: StreamId, today: i64) -> String {
    let mut parts = Vec::new();
    for d in (today - 1)..=(today + DAYS_AHEAD) {
        parts.push(format!(
            "PARTITION {} VALUES LESS THAN ({})",
            partition_name(d),
            day_literal(d + 1)
        ));
    }
    parts.push("PARTITION pmax VALUES LESS THAN (MAXVALUE)".into());
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (
  partition_no  INT UNSIGNED NOT NULL,
  offset_no     BIGINT UNSIGNED NOT NULL,
  day           DATE NOT NULL,
  ts            DATETIME(3) NOT NULL,
  k             VARBINARY(4096) NULL,
  v             LONGBLOB NULL,
  headers       JSON NULL,
  source_event  VARBINARY(64) NOT NULL,
  leader_epoch  INT UNSIGNED NOT NULL,
  PRIMARY KEY (partition_no, offset_no, day),
  UNIQUE KEY uk_source (partition_no, source_event, day),
  KEY k_compact (partition_no, k(255), offset_no)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS (day) ({})",
        quote(schema),
        quote(&log_table(id)),
        parts.join(", ")
    )
}

/// The checkpoint table of a log shard schema.
#[must_use]
pub fn ckpt_sql(schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (
  stream_id     BIGINT UNSIGNED NOT NULL,
  partition_no  INT UNSIGNED NOT NULL,
  next_offset   BIGINT UNSIGNED NOT NULL,
  log_start     BIGINT UNSIGNED NOT NULL,
  leader_epoch  INT UNSIGNED NOT NULL,
  resume_token  VARBINARY(512) NOT NULL,
  PRIMARY KEY (stream_id, partition_no)
) ENGINE=InnoDB",
        quote(schema),
        quote(CKPT_TABLE)
    )
}

/// The checkpoint rows of a new log shard: one per partition at offset 0,
/// and the shard row with the feed position to start from
/// ([`crate::sequencer::Start::encode`]).
#[must_use]
pub fn ckpt_rows_sql(schema: &str, id: StreamId, partitions: &[PartitionNo], start: &[u8]) -> String {
    let mut rows: Vec<String> = partitions
        .iter()
        .map(|p| format!("({}, {}, 0, 0, 0, '')", id.0, p.0))
        .collect();
    rows.push(format!("({}, {SHARD_ROW}, 0, 0, 0, {})", id.0, hex_literal(start)));
    format!(
        "INSERT IGNORE INTO {}.{} (stream_id, partition_no, next_offset, log_start, leader_epoch, resume_token) VALUES {}",
        quote(schema),
        quote(CKPT_TABLE),
        rows.join(", ")
    )
}

/// `X'..'`, or `''` for no bytes.
#[must_use]
pub fn hex_literal(b: &[u8]) -> String {
    if b.is_empty() {
        return "''".into();
    }
    let mut s = String::with_capacity(b.len() * 2 + 3);
    s.push_str("X'");
    for x in b {
        s.push_str(&format!("{x:02X}"));
    }
    s.push('\'');
    s
}

/// What `DROP STREAM` (and turning a change topic off) removes from a log
/// shard: the log and the stream's checkpoint rows.
#[must_use]
pub fn drop_log_sql(schema: &str, id: StreamId) -> Vec<String> {
    vec![
        format!("DROP TABLE IF EXISTS {}.{}", quote(schema), quote(&log_table(id))),
        format!(
            "DELETE FROM {}.{} WHERE stream_id = {}",
            quote(schema),
            quote(CKPT_TABLE),
            id.0
        ),
    ]
}

/// The outbox's drop.
#[must_use]
pub fn drop_outbox_sql(schema: &str, id: StreamId) -> String {
    format!("DROP TABLE IF EXISTS {}.{}", quote(schema), quote(&outbox_table(id)))
}

/// New partitions' checkpoint rows (`ALTER STREAM ... PARTITIONS`).
#[must_use]
pub fn add_partitions_sql(schema: &str, id: StreamId, from: u32, to: u32) -> Option<String> {
    if from >= to {
        return None;
    }
    let rows: Vec<String> = (from..to)
        .map(|p| format!("({}, {p}, 0, 0, 0, '')", id.0))
        .collect();
    Some(format!(
        "INSERT IGNORE INTO {}.{} (stream_id, partition_no, next_offset, log_start, leader_epoch, resume_token) VALUES {}",
        quote(schema),
        quote(CKPT_TABLE),
        rows.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn days() {
        let d = days_from_civil(2026, 11, 7);
        assert_eq!(civil(d), (2026, 11, 7));
        assert_eq!(partition_name(d), "p20261107");
        assert_eq!(day_literal(d + 1), "'2026-11-08'");
        assert_eq!(day_of_partition("p20261107"), Some(d));
        assert_eq!(day_of_partition("pmax"), None);
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(civil(-1), (1969, 12, 31));
        assert_eq!(day_of_ms(1_762_508_100_123), days_from_civil(2025, 11, 7));
    }

    #[test]
    fn statements() {
        let s = log_sql("t1_d1_g1_s1", StreamId(5), days_from_civil(2026, 11, 7));
        assert!(s.contains("PARTITION p20261106 VALUES LESS THAN ('2026-11-07')"));
        assert!(s.contains("PARTITION p20261110 VALUES LESS THAN ('2026-11-11')"));
        assert!(s.contains("`__sprig_sl_5`"));
        assert!(outbox_sql("x", StreamId(5), Cleanup::Compact).contains("sprig_sc_needs_key"));
        assert!(!outbox_sql("x", StreamId(5), Cleanup::Delete).contains("CHECK"));
        let r = ckpt_rows_sql("x", StreamId(5), &[PartitionNo(0), PartitionNo(1)], b"@a");
        assert!(r.contains("(5, 4294967295, 0, 0, 0, X'4061')"));
        assert_eq!(add_partitions_sql("x", StreamId(5), 2, 2), None);
    }
}
