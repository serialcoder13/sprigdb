//! Shard sizes and size histograms on MariaDB (sizes, and index dives
//! for split points).
//!
//! - **Size**: `DATA_LENGTH + INDEX_LENGTH` from `information_schema.TABLES`,
//!   summed over the shard's schemas.
//! - **Histogram**: for each member table (every table with `_sprig_rk`)
//!   and each of 64 equal sub-ranges of the shard, the `rows` estimate of
//!   `EXPLAIN SELECT 1 ... FORCE INDEX (_sprig_rk) WHERE _sprig_rk >= a AND
//!   _sprig_rk < b`, times `AVG_ROW_LENGTH`. `InnoDB` answers with B-tree dives
//!   and reads no rows. Bytes are scaled to the measured size. A bucket
//!   holding half the bytes or more is divided into 64 again, at most
//!   twice; then a time-boxed `GROUP BY _sprig_rk` on the heaviest bucket
//!   names its heavy hashes.

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row, Value};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};

use super::split::{Bucket, HashHistogram};

/// Buckets per dive pass.
pub const BUCKETS: u32 = 64;
/// Refinement passes.
pub const REFINE: usize = 2;
/// The heavy-hash query's time box, in seconds.
pub const HEAVY_SECONDS: f64 = 2.0;
/// Heavy hashes below this share of the shard's rows are ignored.
pub const HEAVY_MIN_SHARE: f64 = 0.01;

fn db_err(what: &str, e: &mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::BackendUnavailable, format!("{what}: {e}"))
}

fn in_list(schemas: &[String]) -> String {
    schemas
        .iter()
        .map(|s| format!("'{}'", s.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `DATA_LENGTH + INDEX_LENGTH` of every table in `schemas`, in bytes.
///
/// # Errors
/// MariaDB's.
pub async fn shard_size(c: &mut Conn, schemas: &[String]) -> Result<f64> {
    if schemas.is_empty() {
        return Ok(0.0);
    }
    let q = format!(
        "SELECT CAST(COALESCE(SUM(DATA_LENGTH + INDEX_LENGTH), 0) AS UNSIGNED) \
         FROM information_schema.TABLES WHERE TABLE_SCHEMA IN ({})",
        in_list(schemas)
    );
    let n: Option<u64> = c
        .query_first(q)
        .await
        .map_err(|e| db_err("shard size", &e))?;
    #[allow(clippy::cast_precision_loss)]
    Ok(n.unwrap_or(0) as f64)
}

/// A member table with its average row length.
#[derive(Debug, Clone)]
struct Member {
    schema: String,
    table: String,
    avg_row: f64,
}

async fn members(c: &mut Conn, schemas: &[String]) -> Result<Vec<Member>> {
    let q = format!(
        "SELECT t.TABLE_SCHEMA, t.TABLE_NAME, COALESCE(t.AVG_ROW_LENGTH, 0) \
         FROM information_schema.TABLES t JOIN information_schema.COLUMNS c \
           ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME \
         WHERE t.TABLE_SCHEMA IN ({}) AND c.COLUMN_NAME = '_sprig_rk' \
         ORDER BY 1, 2",
        in_list(schemas)
    );
    let rows: Vec<(String, String, u64)> =
        c.query(q).await.map_err(|e| db_err("member tables", &e))?;
    #[allow(clippy::cast_precision_loss)]
    Ok(rows
        .into_iter()
        .map(|(schema, table, avg)| Member {
            schema,
            table,
            avg_row: (avg as f64).max(1.0),
        })
        .collect())
}

fn rows_of(r: Row) -> f64 {
    let cols = r.columns_ref().to_vec();
    let vals: Vec<Value> = r.unwrap();
    for (i, c) in cols.iter().enumerate() {
        if c.name_str().eq_ignore_ascii_case("rows") {
            return match vals.get(i) {
                #[allow(clippy::cast_precision_loss)]
                Some(Value::UInt(u)) => *u as f64,
                #[allow(clippy::cast_precision_loss)]
                Some(Value::Int(i)) => *i as f64,
                Some(Value::Bytes(b)) => String::from_utf8_lossy(b).parse().unwrap_or(0.0),
                _ => 0.0,
            };
        }
    }
    0.0
}

fn hi_text(hi: u128) -> String {
    if hi > u128::from(u64::MAX) {
        String::new()
    } else {
        format!(" AND `_sprig_rk` < {hi}")
    }
}

/// The estimated rows of `[a, b)` in one table, by index dive.
async fn dive(c: &mut Conn, m: &Member, a: u64, b: u128) -> Result<f64> {
    let q = format!(
        "EXPLAIN SELECT 1 FROM {}.{} FORCE INDEX (`_sprig_rk`) WHERE `_sprig_rk` >= {a}{}",
        quote(&m.schema),
        quote(&m.table),
        hi_text(b)
    );
    let r: Vec<Row> = c.query(q).await.map_err(|e| db_err("index dive", &e))?;
    Ok(r.into_iter().map(rows_of).sum())
}

/// Fills `h`'s buckets with dived bytes over `members`.
async fn fill(c: &mut Conn, ms: &[Member], h: &mut HashHistogram) -> Result<()> {
    for i in 0..h.buckets.len() {
        let (a, b) = (h.buckets[i].lo, h.bucket_hi(i));
        let mut bytes = 0.0;
        for m in ms {
            bytes += dive(c, m, a, b).await? * m.avg_row;
        }
        h.buckets[i].bytes = bytes;
    }
    Ok(())
}

/// The size histogram of a shard `[lo, hi)` over `schemas`, scaled to
/// `size_bytes`.
///
/// # Errors
/// MariaDB's (the caller then splits at equal widths).
pub async fn size_histogram(
    c: &mut Conn,
    schemas: &[String],
    lo: u64,
    hi: Option<u64>,
    size_bytes: f64,
) -> Result<HashHistogram> {
    let ms = members(c, schemas).await?;
    let mut h = HashHistogram::equal(lo, hi, BUCKETS);
    fill(c, &ms, &mut h).await?;
    for _ in 0..REFINE {
        let total = h.total(super::split::Weight::Bytes);
        let Some((i, _)) = h
            .buckets
            .iter()
            .enumerate()
            .find(|(_, b)| total > 0.0 && b.bytes >= total / 2.0)
        else {
            break;
        };
        let (a, b) = (h.buckets[i].lo, h.bucket_hi(i));
        if b - u128::from(a) < u128::from(BUCKETS) {
            break;
        }
        let mut sub = HashHistogram::equal(a, u64::try_from(b).ok(), BUCKETS);
        fill(c, &ms, &mut sub).await?;
        let mut buckets: Vec<Bucket> = h.buckets[..i].to_vec();
        buckets.extend(sub.buckets);
        buckets.extend_from_slice(&h.buckets[i + 1..]);
        h.buckets = buckets;
    }
    // Scale to the measured size.
    let raw = h.total(super::split::Weight::Bytes);
    let f = if raw > 0.0 && size_bytes > 0.0 {
        size_bytes / raw
    } else {
        1.0
    };
    for b in &mut h.buckets {
        b.bytes *= f;
    }
    // Heavy hashes of the heaviest bucket, time-boxed.
    if let Some((i, _)) = h
        .buckets
        .iter()
        .enumerate()
        .max_by(|a, b| a.1.bytes.total_cmp(&b.1.bytes))
    {
        let (a, b) = (h.buckets[i].lo, h.bucket_hi(i));
        let mut counts: std::collections::BTreeMap<u64, f64> = std::collections::BTreeMap::new();
        for m in &ms {
            let q = format!(
                "SET STATEMENT max_statement_time = {HEAVY_SECONDS} FOR \
                 SELECT `_sprig_rk`, COUNT(*) FROM {}.{} WHERE `_sprig_rk` >= {a}{} \
                 GROUP BY 1 ORDER BY 2 DESC LIMIT 4",
                quote(&m.schema),
                quote(&m.table),
                hi_text(b)
            );
            match c.query::<(u64, u64), _>(q).await {
                Ok(rows) => {
                    for (k, n) in rows {
                        #[allow(clippy::cast_precision_loss)]
                        let w = n as f64 * m.avg_row;
                        *counts.entry(k).or_default() += w;
                    }
                }
                Err(e) => tracing::debug!(error = %e, "heavy-hash query skipped"),
            }
        }
        let total = h.total(super::split::Weight::Bytes);
        h.heavy = counts
            .into_iter()
            .map(|(k, w)| (k, w * f))
            .filter(|(_, w)| total > 0.0 && *w >= total * HEAVY_MIN_SHARE)
            .collect();
    }
    Ok(h)
}
