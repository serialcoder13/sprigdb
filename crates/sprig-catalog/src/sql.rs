//! Helpers shared by the MariaDB code: error mapping, timeouts, column
//! decoding and `DATETIME(6)` conversion.

use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mysql_async::prelude::FromValue;
use mysql_async::{Row, Value};
use sprig_common::{Result, SprigCode, SprigError};

/// Maps a driver error to a client-safe [`SprigError`]. The driver's text,
/// which may name the physical schema, goes into `source` only.
///
/// - lock wait timeouts and deadlocks: 39071 `ER_SPRIG_CATALOG_CONFLICT`,
///   retryable;
/// - other server errors: passed through as backend errors (1062 stays
///   1062, so callers can turn a duplicate live name into their own error);
/// - lost connections, I/O and driver errors: 39070
///   `ER_SPRIG_CATALOG_UNAVAILABLE`, retryable.
pub(crate) fn map_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => match s.code {
            1205 | 1213 => SprigError::new(SprigCode::CatalogConflict, "catalog row busy; retry"),
            code => {
                let mut state = *b"HY000";
                if s.state.len() == 5 {
                    state.copy_from_slice(s.state.as_bytes());
                }
                SprigError::backend(code, state, format!("catalog statement failed ({code})"))
            }
        }
        .with_source(e),
        _ => SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e),
    }
}

/// Runs `fut` with a deadline (chapter 03: every network await has one).
/// A timeout is 39070, retryable.
pub(crate) async fn timed<T>(
    limit: Duration,
    what: &'static str,
    fut: impl Future<Output = Result<T>>,
) -> Result<T> {
    match tokio::time::timeout(limit, fut).await {
        Ok(r) => r,
        Err(_) => Err(SprigError::new(
            SprigCode::CatalogUnavailable,
            format!("catalog {what} timed out after {} ms", limit.as_millis()),
        )),
    }
}

/// Takes column `idx` of `row` as `T`.
pub(crate) fn col<T: FromValue>(row: &mut Row, idx: usize, name: &str) -> Result<T> {
    match row.take_opt::<T, usize>(idx) {
        Some(Ok(v)) => Ok(v),
        Some(Err(e)) => Err(SprigError::internal(format!(
            "catalog column {name}: unexpected value: {e}"
        ))),
        None => Err(SprigError::internal(format!(
            "catalog column {name} missing from the result"
        ))),
    }
}

/// Takes a JSON column (`LONGTEXT` in MariaDB) that may be NULL.
pub(crate) fn json_col(row: &mut Row, idx: usize, name: &str) -> Result<Option<serde_json::Value>> {
    let raw: Option<Vec<u8>> = col(row, idx, name)?;
    raw.map(|b| {
        serde_json::from_slice(&b)
            .map_err(|e| SprigError::internal(format!("catalog column {name}: bad JSON: {e}")))
    })
    .transpose()
}

/// Takes a `DATETIME(6)` column, read as UTC.
pub(crate) fn time_col(row: &mut Row, idx: usize, name: &str) -> Result<SystemTime> {
    let v: Value = col(row, idx, name)?;
    from_datetime(&v)
        .ok_or_else(|| SprigError::internal(format!("catalog column {name}: bad DATETIME {v:?}")))
}

/// Takes a `BINARY(n)` column that may be NULL.
pub(crate) fn bytes_col<const N: usize>(
    row: &mut Row,
    idx: usize,
    name: &str,
) -> Result<Option<[u8; N]>> {
    let raw: Option<Vec<u8>> = col(row, idx, name)?;
    raw.map(|b| {
        <[u8; N]>::try_from(b.as_slice()).map_err(|_| {
            SprigError::internal(format!(
                "catalog column {name}: {} bytes, expected {N}",
                b.len()
            ))
        })
    })
    .transpose()
}

/// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's
/// `days_from_civil`).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = i64::from(m);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The date of a day number (Howard Hinnant's `civil_from_days`).
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)] // month and day are tiny
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// Splits a time into UTC calendar fields: year, month, day, hour, minute,
/// second, microsecond. Times before 1970 clamp to the epoch.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)] // fields are bounded
fn fields(t: SystemTime) -> (i64, u32, u32, u32, u32, u32, u32) {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = i64::try_from(d.as_secs()).unwrap_or(i64::MAX / 2);
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let (y, m, day) = civil_from_days(days);
    (
        y,
        m,
        day,
        (rem / 3600) as u32,
        (rem % 3600 / 60) as u32,
        (rem % 60) as u32,
        d.subsec_micros(),
    )
}

/// A time as a `DATETIME(6)` parameter, in UTC.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::many_single_char_names
)] // fields are bounded
pub(crate) fn to_datetime(t: SystemTime) -> Value {
    let (y, m, d, h, mi, s, us) = fields(t);
    Value::Date(
        y.clamp(0, 9999) as u16,
        m as u8,
        d as u8,
        h as u8,
        mi as u8,
        s as u8,
        us,
    )
}

/// A time as RFC 3339 UTC with microseconds, for people (migration
/// history).
#[allow(clippy::many_single_char_names)]
pub(crate) fn rfc3339(t: SystemTime) -> String {
    let (y, m, d, h, mi, s, us) = fields(t);
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{us:06}Z")
}

/// A `DATETIME` value (binary or text protocol) as a UTC time.
#[allow(clippy::many_single_char_names)]
pub(crate) fn from_datetime(v: &Value) -> Option<SystemTime> {
    let (y, mo, d, h, mi, s, us) = match v {
        Value::Date(y, mo, d, h, mi, s, us) => (
            i64::from(*y),
            u32::from(*mo),
            u32::from(*d),
            u64::from(*h),
            u64::from(*mi),
            u64::from(*s),
            *us,
        ),
        Value::Bytes(b) => parse_datetime_text(std::str::from_utf8(b).ok()?)?,
        _ => return None,
    };
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
        return None;
    }
    let days = u64::try_from(days_from_civil(y, mo, d)).ok()?;
    let secs = days * 86_400 + h * 3600 + mi * 60 + s;
    Some(UNIX_EPOCH + Duration::from_secs(secs) + Duration::from_micros(u64::from(us)))
}

/// Parses `YYYY-MM-DD HH:MM:SS[.ffffff]`.
#[allow(clippy::many_single_char_names)]
fn parse_datetime_text(t: &str) -> Option<(i64, u32, u32, u64, u64, u64, u32)> {
    let (date, time) = t.split_once(' ')?;
    let mut dp = date.split('-');
    let y = dp.next()?.parse().ok()?;
    let mo = dp.next()?.parse().ok()?;
    let d = dp.next()?.parse().ok()?;
    let (hms, frac) = time.split_once('.').unwrap_or((time, ""));
    let mut tp = hms.split(':');
    let h = tp.next()?.parse().ok()?;
    let mi = tp.next()?.parse().ok()?;
    let s = tp.next()?.parse().ok()?;
    let us = if frac.is_empty() {
        0
    } else {
        let digits: String = frac.chars().chain(std::iter::repeat('0')).take(6).collect();
        digits.parse().ok()?
    };
    Some((y, mo, d, h, mi, s, us))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn datetimes_round_trip() {
        for secs in [
            0_u64,
            951_782_400,
            1_762_560_000,
            4_102_444_800,
            253_402_300_799,
        ] {
            let t = UNIX_EPOCH + Duration::from_secs(secs) + Duration::from_micros(123_456);
            assert_eq!(from_datetime(&to_datetime(t)), Some(t), "{secs}");
        }
        // 2000-02-29 (a leap day) and the text protocol form.
        let leap = from_datetime(&Value::Bytes(b"2000-02-29 12:34:56.5".to_vec())).unwrap();
        assert_eq!(
            leap,
            UNIX_EPOCH + Duration::from_secs(951_827_696) + Duration::from_micros(500_000)
        );
        assert_eq!(rfc3339(leap), "2000-02-29T12:34:56.500000Z");
        assert!(from_datetime(&Value::Bytes(b"2000-13-01 00:00:00".to_vec())).is_none());
        assert!(from_datetime(&Value::Int(3)).is_none());
    }

    #[test]
    fn server_errors_keep_their_number_and_hide_the_text() {
        let e = map_err(mysql_async::Error::Server(mysql_async::ServerError {
            code: 1062,
            message: "Duplicate entry for key t42_sprig.objects".to_owned(),
            state: "23000".to_owned(),
        }));
        assert_eq!(
            e.kind,
            sprig_common::ErrorKind::Backend {
                errno: 1062,
                sqlstate: *b"23000"
            }
        );
        assert!(!e.message.contains("t42_"));
        let busy = map_err(mysql_async::Error::Server(mysql_async::ServerError {
            code: 1205,
            message: "Lock wait timeout exceeded".to_owned(),
            state: "HY000".to_owned(),
        }));
        assert_eq!(busy.code(), Some(SprigCode::CatalogConflict));
        assert!(busy.retryable);
    }
}
