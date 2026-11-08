//! Small helpers shared by the MariaDB code: error mapping, column
//! decoding, times as microseconds.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mysql_async::Row;
use mysql_async::prelude::{FromValue, Queryable};
use sprig_common::{Result, SprigCode, SprigError};

/// Maps a driver error to a client-safe [`SprigError`], as the catalog
/// does: 1205 and 1213 are 39071 (retryable), other server errors pass
/// through, lost connections and driver errors are 39070. The driver's
/// text, which may name a physical schema, goes into `source` only.
pub(crate) fn map_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => match s.code {
            1205 | 1213 => SprigError::new(SprigCode::CatalogConflict, "catalog row busy; retry"),
            code => {
                let mut state = *b"HY000";
                if s.state.len() == 5 {
                    state.copy_from_slice(s.state.as_bytes());
                }
                SprigError::backend(code, state, format!("statement failed ({code})"))
            }
        }
        .with_source(e),
        _ => SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e),
    }
}

/// The MariaDB error number of `e`, if it is a server error passed through.
pub(crate) fn errno(e: &SprigError) -> Option<u16> {
    match e.kind {
        sprig_common::ErrorKind::Backend { errno, .. } => Some(errno),
        _ => None,
    }
}

/// Takes column `idx` of `row` as `T`.
pub(crate) fn col<T: FromValue>(row: &mut Row, idx: usize, name: &str) -> Result<T> {
    match row.take_opt::<T, usize>(idx) {
        Some(Ok(v)) => Ok(v),
        Some(Err(e)) => Err(SprigError::internal(format!(
            "column {name}: unexpected value: {e}"
        ))),
        None => Err(SprigError::internal(format!(
            "column {name} missing from the result"
        ))),
    }
}

/// Takes a JSON column and parses it as `T`.
pub(crate) fn json_col<T: serde::de::DeserializeOwned>(
    row: &mut Row,
    idx: usize,
    name: &str,
) -> Result<Option<T>> {
    let raw: Option<Vec<u8>> = col(row, idx, name)?;
    raw.map(|b| {
        serde_json::from_slice(&b)
            .map_err(|e| SprigError::internal(format!("column {name}: bad JSON: {e}")))
    })
    .transpose()
}

/// A JSON column that must not be NULL.
pub(crate) fn json_req<T: serde::de::DeserializeOwned>(
    row: &mut Row,
    idx: usize,
    name: &str,
) -> Result<T> {
    json_col(row, idx, name)?.ok_or_else(|| SprigError::internal(format!("column {name} is NULL")))
}

/// Serializes `v` for a JSON column.
pub(crate) fn to_json<T: serde::Serialize>(v: &T) -> Result<String> {
    serde_json::to_string(v).map_err(|e| SprigError::internal(format!("JSON encoding: {e}")))
}

/// Microseconds since the epoch (negative before it).
pub(crate) fn micros(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_micros()).unwrap_or(i64::MAX),
        Err(e) => -i64::try_from(e.duration().as_micros()).unwrap_or(i64::MAX),
    }
}

/// The time `us` microseconds after the epoch.
pub(crate) fn from_micros(us: i64) -> SystemTime {
    if us >= 0 {
        UNIX_EPOCH + Duration::from_micros(us.unsigned_abs())
    } else {
        UNIX_EPOCH - Duration::from_micros(us.unsigned_abs())
    }
}

/// A `DATETIME` column read back as microseconds.
pub(crate) fn time_col(row: &mut Row, idx: usize, name: &str) -> Result<SystemTime> {
    let us: i64 = col(row, idx, name)?;
    Ok(from_micros(us))
}

/// A nullable `DATETIME` column read back as microseconds.
pub(crate) fn opt_time_col(row: &mut Row, idx: usize, name: &str) -> Result<Option<SystemTime>> {
    let us: Option<i64> = col(row, idx, name)?;
    Ok(us.map(from_micros))
}

/// A time parameter: `TIMESTAMPADD(MICROSECOND, ?, '1970-01-01')`. Times
/// travel as microseconds so no calendar arithmetic happens in Rust and
/// the session time zone doesn't matter.
pub(crate) const AT: &str = "TIMESTAMPADD(MICROSECOND, ?, '1970-01-01')";

/// Rows changed by the last statement on `q`'s connection.
pub(crate) async fn row_count<Q: Queryable + Send>(q: &mut Q) -> Result<u64> {
    let n: Option<i64> = q.query_first("SELECT ROW_COUNT()").await.map_err(map_err)?;
    Ok(n.and_then(|n| u64::try_from(n).ok()).unwrap_or(0))
}

/// `LAST_INSERT_ID()` on `q`'s connection.
pub(crate) async fn last_id<Q: Queryable + Send>(q: &mut Q) -> Result<u64> {
    let n: Option<u64> = q
        .query_first("SELECT LAST_INSERT_ID()")
        .await
        .map_err(map_err)?;
    n.ok_or_else(|| SprigError::internal("LAST_INSERT_ID() returned nothing"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn micros_round_trip() {
        let t = UNIX_EPOCH + Duration::from_micros(1_700_000_000_123_456);
        assert_eq!(from_micros(micros(t)), t);
        let before = UNIX_EPOCH - Duration::from_micros(5);
        assert_eq!(micros(before), -5);
        assert_eq!(from_micros(-5), before);
    }
}
