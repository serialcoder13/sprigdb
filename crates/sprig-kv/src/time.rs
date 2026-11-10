//! Expiry instants. Keys expire by MariaDB's clock (`UTC_TIMESTAMP(3)`),
//! so the engine reads "now" from MariaDB in the same statement as the key
//! row, computes absolute instants in Rust, and writes them as
//! `DATETIME(3)` literals in UTC.

/// The latest instant a `DATETIME(3)` holds: 9999-12-31 23:59:59.999 UTC,
/// in milliseconds since the epoch.
pub const MAX_MS: i64 = 253_402_300_799_999;

/// SQL for "now" in milliseconds since the epoch, by MariaDB's clock.
pub const NOW_MS_SQL: &str =
    "(TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP(3)) DIV 1000)";

/// SQL for a `DATETIME(3)` column `col` in milliseconds since the epoch.
#[must_use]
pub fn ms_sql(col: &str) -> String {
    format!("(TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', {col}) DIV 1000)")
}

/// `'YYYY-MM-DD HH:MM:SS.mmm'` for `ms` since the epoch, clamped to
/// [`MAX_MS`] and to the epoch.
#[must_use]
pub fn datetime_lit(ms: i64) -> String {
    let ms = ms.clamp(0, MAX_MS);
    let days = ms.div_euclid(86_400_000);
    let rem = ms.rem_euclid(86_400_000);
    let (year, month, day) = civil_from_days(days);
    let (hour, minute, second, milli) = (
        rem / 3_600_000,
        rem / 60_000 % 60,
        rem / 1000 % 60,
        rem % 1000,
    );
    format!("'{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{milli:03}'")
}

/// Howard Hinnant's `civil_from_days`.
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datetimes() {
        assert_eq!(datetime_lit(0), "'1970-01-01 00:00:00.000'");
        assert_eq!(datetime_lit(1_762_700_000_123), "'2025-11-09 14:53:20.123'");
        assert_eq!(datetime_lit(i64::MAX), "'9999-12-31 23:59:59.999'");
        assert_eq!(datetime_lit(951_782_400_000), "'2000-02-29 00:00:00.000'");
    }
}
