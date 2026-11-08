//! Column values in row images, decoded from MariaDB's binlog row format
//! (chapter 10, "Decoding").
//!
//! Choices from the book: `ENUM` and `SET` stay numbers; `TIMESTAMP` is
//! epoch seconds (UTC); `DECIMAL` is a string. Strings stay bytes in their
//! column's collation ([`crate::Column::collation`]), because SQL appliers
//! re-insert them with that collation; [`Value::as_str`] gives a `&str`
//! when the bytes are UTF-8.
//!
//! MariaDB-specific types log as one of the string types: `JSON` is
//! `LONGTEXT` (a [`Value::String`]); `UUID`, `INET4`, `INET6` and `VECTOR`
//! are fixed or variable binary strings. [`uuid_text`], [`inet4_text`],
//! [`inet6_text`] and [`vector_values`] turn those bytes into their usual forms.

use std::fmt::{self, Write as _};

use sprig_common::SprigError;

use crate::buf::{Buf, be_uint, unsupported};
use crate::compress::uncompress_column;
use crate::table_map::{Column, ColumnType, bit};

/// One column value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// SQL `NULL`.
    Null,
    /// A signed integer, or an integer whose signedness the binlog didn't
    /// say (`binlog_row_metadata = NO_LOG`).
    Int(i64),
    /// An unsigned integer.
    UInt(u64),
    /// `FLOAT`.
    Float(f32),
    /// `DOUBLE`.
    Double(f64),
    /// `DECIMAL`, as MariaDB prints it: `-12.340`.
    Decimal(String),
    /// `BIT(n)`, right-aligned.
    Bit(u64),
    /// `YEAR`; 0 is `0000`.
    Year(u16),
    /// `DATE`.
    Date(Date),
    /// `TIME`.
    Time(Time),
    /// `DATETIME`.
    DateTime(DateTime),
    /// `TIMESTAMP`, as seconds since the epoch in UTC.
    Timestamp(Timestamp),
    /// `ENUM`, as the 1-based member index (0 is the empty error value).
    Enum(u16),
    /// `SET`, as a bitmap of members (bit 0 is the first member).
    Set(u64),
    /// A character string, in the column's collation (`CHAR`, `VARCHAR`,
    /// `TEXT`, MariaDB `JSON`). Columns logged without collation also land here.
    String(Vec<u8>),
    /// A binary string (`BINARY`, `VARBINARY`, `BLOB`, MariaDB `UUID`,
    /// `INET4`, `INET6`, `VECTOR`).
    Bytes(Vec<u8>),
    /// A spatial value: 4-byte little-endian SRID, then WKB.
    Geometry(Vec<u8>),
}

impl Value {
    /// The bytes of a string, binary or geometry value.
    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::String(b) | Value::Bytes(b) | Value::Geometry(b) => Some(b),
            _ => None,
        }
    }

    /// A string value as `&str`, when its bytes are UTF-8.
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(b) | Value::Bytes(b) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }

    /// Whether this is SQL `NULL`.
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

/// A `DATE`. Zero parts are allowed (`0000-00-00`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date {
    /// Year, 0 to 9999.
    pub year: u16,
    /// Month, 0 to 12.
    pub month: u8,
    /// Day, 0 to 31.
    pub day: u8,
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }
}

/// A `TIME`: a signed duration up to 838:59:59.999999.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Time {
    /// Below zero.
    pub negative: bool,
    /// Hours, 0 to 838.
    pub hours: u16,
    /// Minutes.
    pub minutes: u8,
    /// Seconds.
    pub seconds: u8,
    /// Microseconds.
    pub micros: u32,
    /// Fractional digits of the column, for display.
    pub fsp: u8,
}

impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.negative {
            f.write_str("-")?;
        }
        write!(
            f,
            "{:02}:{:02}:{:02}",
            self.hours, self.minutes, self.seconds
        )?;
        write_frac(f, self.micros, self.fsp)
    }
}

/// A `DATETIME`. Zero parts are allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DateTime {
    /// The date part.
    pub date: Date,
    /// Hour.
    pub hour: u8,
    /// Minute.
    pub minute: u8,
    /// Second.
    pub second: u8,
    /// Microseconds.
    pub micros: u32,
    /// Fractional digits of the column, for display.
    pub fsp: u8,
}

impl fmt::Display for DateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {:02}:{:02}:{:02}",
            self.date, self.hour, self.minute, self.second
        )?;
        write_frac(f, self.micros, self.fsp)
    }
}

/// A `TIMESTAMP`: seconds since 1970-01-01 UTC. Zero is MariaDB's zero
/// timestamp, `0000-00-00 00:00:00`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp {
    /// Seconds since the epoch.
    pub secs: u32,
    /// Microseconds.
    pub micros: u32,
    /// Fractional digits of the column, for display.
    pub fsp: u8,
}

impl Timestamp {
    /// The UTC date and time, as MariaDB shows it under `time_zone = '+00:00'`.
    #[must_use]
    pub fn to_utc(&self) -> DateTime {
        if self.secs == 0 && self.micros == 0 {
            return DateTime {
                date: Date {
                    year: 0,
                    month: 0,
                    day: 0,
                },
                hour: 0,
                minute: 0,
                second: 0,
                micros: 0,
                fsp: self.fsp,
            };
        }
        let days = i64::from(self.secs / 86_400);
        let rem = self.secs % 86_400;
        let (year, month, day) = civil_from_days(days);
        DateTime {
            date: Date { year, month, day },
            hour: narrow(rem / 3600),
            minute: narrow(rem / 60 % 60),
            second: narrow(rem % 60),
            micros: self.micros,
            fsp: self.fsp,
        }
    }
}

impl fmt::Display for Timestamp {
    /// UTC, in MariaDB's text form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.to_utc().fmt(f)
    }
}

fn write_frac(f: &mut fmt::Formatter<'_>, micros: u32, fsp: u8) -> fmt::Result {
    if fsp == 0 {
        return Ok(());
    }
    let digits = format!("{micros:06}");
    write!(f, ".{}", &digits[..usize::from(fsp.min(6))])
}

fn narrow<T: TryInto<u8>>(v: T) -> u8 {
    v.try_into().unwrap_or(u8::MAX)
}

/// Days since 1970-01-01 to (year, month, day), proleptic Gregorian
/// (Howard Hinnant's algorithm).
fn civil_from_days(z: i64) -> (u16, u8, u8) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (
        u16::try_from(y).unwrap_or(0),
        u8::try_from(m).unwrap_or(0),
        u8::try_from(d).unwrap_or(0),
    )
}

/// One row image: a value per table column, `None` for columns the event
/// didn't include (`binlog_row_image = MINIMAL` or `NOBLOB`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RowImage(pub Vec<Option<Value>>);

impl RowImage {
    /// The value of column `i`, if the image includes it.
    #[must_use]
    pub fn get(&self, i: usize) -> Option<&Value> {
        self.0.get(i).and_then(Option::as_ref)
    }

    /// Number of table columns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the table has no columns.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Decodes one row image: the null bitmap over the present columns, then
/// each present, non-null value.
pub(crate) fn decode_row(
    cols: &[Column],
    present: &[u8],
    b: &mut Buf<'_>,
) -> Result<RowImage, SprigError> {
    let n_present = (0..cols.len()).filter(|&i| bit(present, i)).count();
    let nulls = b.take(n_present.div_ceil(8))?;
    let mut values = Vec::with_capacity(cols.len());
    let mut k = 0;
    for (i, col) in cols.iter().enumerate() {
        if !bit(present, i) {
            values.push(None);
            continue;
        }
        let is_null = bit(nulls, k);
        k += 1;
        values.push(Some(if is_null {
            Value::Null
        } else {
            decode_value(col, b)?
        }));
    }
    Ok(RowImage(values))
}

/// Decodes one non-null value of `col`: `ER_SPRIG_BINLOG_CORRUPT` on short
/// input, `ER_SPRIG_BINLOG_UNSUPPORTED` for an unknown column type.
// Sign reinterpretation is the point; one arm per column type.
#[allow(clippy::cast_possible_wrap, clippy::too_many_lines)]
pub(crate) fn decode_value(col: &Column, b: &mut Buf<'_>) -> Result<Value, SprigError> {
    let unsigned = col.unsigned == Some(true);
    Ok(match col.kind {
        ColumnType::TinyInt => {
            let v = b.u8()?;
            if unsigned {
                Value::UInt(u64::from(v))
            } else {
                Value::Int(i64::from(v as i8))
            }
        }
        ColumnType::SmallInt => {
            let v = b.u16()?;
            if unsigned {
                Value::UInt(u64::from(v))
            } else {
                Value::Int(i64::from(v as i16))
            }
        }
        ColumnType::MediumInt => {
            let v = b.u24()?;
            if unsigned {
                Value::UInt(u64::from(v))
            } else {
                Value::Int(i64::from(((v << 8) as i32) >> 8))
            }
        }
        ColumnType::Int => {
            let v = b.u32()?;
            if unsigned {
                Value::UInt(u64::from(v))
            } else {
                Value::Int(i64::from(v as i32))
            }
        }
        ColumnType::BigInt => {
            let v = b.u64()?;
            if unsigned {
                Value::UInt(v)
            } else {
                Value::Int(v as i64)
            }
        }
        ColumnType::Float => {
            let raw = b.take(4)?;
            Value::Float(f32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
        }
        ColumnType::Double => {
            let raw = b.take(8)?;
            let mut a = [0u8; 8];
            a.copy_from_slice(raw);
            Value::Double(f64::from_le_bytes(a))
        }
        ColumnType::Decimal { precision, scale } => {
            Value::Decimal(decode_decimal(b, precision, scale)?)
        }
        ColumnType::Bit { .. } => {
            let [rem_bits, bytes] = col.meta.to_le_bytes();
            let len = usize::from(bytes) + usize::from(rem_bits > 0);
            Value::Bit(b.uint_be(len.min(8))?)
        }
        ColumnType::Year => {
            let v = b.u8()?;
            Value::Year(if v == 0 { 0 } else { 1900 + u16::from(v) })
        }
        ColumnType::Date => {
            let v = b.u24()?;
            Value::Date(Date {
                year: u16::try_from(v >> 9).unwrap_or(0),
                month: narrow((v >> 5) & 0x0f),
                day: narrow(v & 0x1f),
            })
        }
        ColumnType::Time { fsp, legacy: true } => {
            let v = b.u24()?;
            let i = ((v << 8) as i32) >> 8;
            let a = i.unsigned_abs();
            Value::Time(Time {
                negative: i < 0,
                hours: u16::try_from(a / 10_000).unwrap_or(0),
                minutes: narrow(a / 100 % 100),
                seconds: narrow(a % 100),
                micros: 0,
                fsp,
            })
        }
        ColumnType::Time { fsp, legacy: false } => Value::Time(decode_time2(b, fsp)?),
        ColumnType::DateTime { fsp, legacy: true } => {
            let v = b.u64()?;
            let (d, t) = (v / 1_000_000, v % 1_000_000);
            Value::DateTime(DateTime {
                date: Date {
                    year: u16::try_from(d / 10_000).unwrap_or(0),
                    month: narrow(d / 100 % 100),
                    day: narrow(d % 100),
                },
                hour: narrow(t / 10_000),
                minute: narrow(t / 100 % 100),
                second: narrow(t % 100),
                micros: 0,
                fsp,
            })
        }
        ColumnType::DateTime { fsp, legacy: false } => {
            let int = b.uint_be(5)?.wrapping_sub(0x80_0000_0000);
            let micros = read_frac(b, fsp)?;
            let ymd = int >> 17;
            let ym = ymd >> 5;
            let hms = int % (1 << 17);
            Value::DateTime(DateTime {
                date: Date {
                    year: u16::try_from(ym / 13).unwrap_or(0),
                    month: narrow(ym % 13),
                    day: narrow(ymd % 32),
                },
                hour: narrow(hms >> 12),
                minute: narrow((hms >> 6) % 64),
                second: narrow(hms % 64),
                micros,
                fsp,
            })
        }
        ColumnType::Timestamp { fsp, legacy: true } => Value::Timestamp(Timestamp {
            secs: b.u32()?,
            micros: 0,
            fsp,
        }),
        ColumnType::Timestamp { fsp, legacy: false } => {
            let secs = u32::try_from(b.uint_be(4)?).unwrap_or(0);
            Value::Timestamp(Timestamp {
                secs,
                micros: read_frac(b, fsp)?,
                fsp,
            })
        }
        ColumnType::VarChar {
            max_len,
            compressed,
        } => {
            let len = if max_len < 256 {
                usize::from(b.u8()?)
            } else {
                usize::from(b.u16()?)
            };
            string_value(col, b.take(len)?, compressed)?
        }
        ColumnType::Char { max_len } => {
            let len = if max_len < 256 {
                usize::from(b.u8()?)
            } else {
                usize::from(b.u16()?)
            };
            let v = string_value(col, b.take(len)?, false)?;
            // The binlog strips trailing pad bytes. Text columns read back
            // without trailing spaces anyway; binary ones (BINARY(n),
            // INET4, INET6, UUID) are padded with zero bytes again.
            match v {
                Value::Bytes(mut bytes) if bytes.len() < usize::from(max_len) => {
                    bytes.resize(usize::from(max_len), 0);
                    Value::Bytes(bytes)
                }
                v => v,
            }
        }
        ColumnType::Enum { size } => {
            Value::Enum(u16::try_from(b.uint_le(usize::from(size.min(2)))?).unwrap_or(0))
        }
        ColumnType::Set { size } => Value::Set(b.uint_le(usize::from(size.min(8)))?),
        ColumnType::Blob {
            pack_len,
            compressed,
        } => {
            let len = usize::try_from(b.uint_le(usize::from(pack_len.min(4)))?)
                .map_err(|_| unsupported("blob length overflows"))?;
            string_value(col, b.take(len)?, compressed)?
        }
        ColumnType::Geometry { pack_len } => {
            let len = usize::try_from(b.uint_le(usize::from(pack_len.min(4)))?)
                .map_err(|_| unsupported("geometry length overflows"))?;
            Value::Geometry(b.take(len)?.to_vec())
        }
        ColumnType::MysqlJson { pack_len } => {
            let len = usize::try_from(b.uint_le(usize::from(pack_len.min(4)))?)
                .map_err(|_| unsupported("json length overflows"))?;
            Value::Bytes(b.take(len)?.to_vec())
        }
        ColumnType::Null => Value::Null,
        ColumnType::Unknown(t) => {
            return Err(unsupported(format!(
                "column {} has binlog type {t}, which the decoder doesn't know",
                col.name.as_deref().unwrap_or("?")
            )));
        }
    })
}

fn string_value(col: &Column, raw: &[u8], compressed: bool) -> Result<Value, SprigError> {
    let bytes = if compressed {
        uncompress_column(raw)?
    } else {
        raw.to_vec()
    };
    Ok(if col.is_binary() {
        Value::Bytes(bytes)
    } else {
        Value::String(bytes)
    })
}

/// Fractional seconds of `TIME2`/`DATETIME2`/`TIMESTAMP2`, unsigned.
fn read_frac(b: &mut Buf<'_>, fsp: u8) -> Result<u32, SprigError> {
    let v = match fsp {
        1 | 2 => b.uint_be(1)? * 10_000,
        3 | 4 => b.uint_be(2)? * 100,
        5 | 6 => b.uint_be(3)?,
        _ => 0,
    };
    Ok(u32::try_from(v).unwrap_or(0))
}

/// `TIME2`: MariaDB's `my_time_packed_from_binary` and
/// `TIME_from_longlong_time_packed`.
#[allow(clippy::cast_possible_wrap)]
fn decode_time2(b: &mut Buf<'_>, fsp: u8) -> Result<Time, SprigError> {
    const TIMEF_INT_OFS: i64 = 0x80_0000;
    const TIMEF_OFS: i64 = 0x8000_0000_0000;
    let packed: i64 = match fsp {
        1 | 2 => {
            let mut int = b.uint_be(3)? as i64 - TIMEF_INT_OFS;
            let mut frac = i64::from(b.u8()?);
            if int < 0 && frac != 0 {
                int += 1;
                frac -= 0x100;
            }
            (int << 24) + frac * 10_000
        }
        3 | 4 => {
            let mut int = b.uint_be(3)? as i64 - TIMEF_INT_OFS;
            let mut frac = b.uint_be(2)? as i64;
            if int < 0 && frac != 0 {
                int += 1;
                frac -= 0x1_0000;
            }
            (int << 24) + frac * 100
        }
        5 | 6 => b.uint_be(6)? as i64 - TIMEF_OFS,
        _ => (b.uint_be(3)? as i64 - TIMEF_INT_OFS) << 24,
    };
    let negative = packed < 0;
    let a = packed.unsigned_abs();
    let hms = a >> 24;
    Ok(Time {
        negative,
        hours: u16::try_from((hms >> 12) % (1 << 10)).unwrap_or(0),
        minutes: narrow((hms >> 6) % 64),
        seconds: narrow(hms % 64),
        micros: u32::try_from(a % (1 << 24)).unwrap_or(0),
        fsp,
    })
}

/// MariaDB's binary `DECIMAL` (`bin2decimal`) as text with exactly `scale`
/// fractional digits.
fn decode_decimal(b: &mut Buf<'_>, precision: u8, scale: u8) -> Result<String, SprigError> {
    const DIG2BYTES: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
    let intg = usize::from(precision.saturating_sub(scale));
    let scale = usize::from(scale);
    let (intg0, intg0x) = (intg / 9, intg % 9);
    let (frac0, frac0x) = (scale / 9, scale % 9);
    let size = intg0 * 4 + DIG2BYTES[intg0x] + frac0 * 4 + DIG2BYTES[frac0x];
    let mut d = b.take(size)?.to_vec();
    if d.is_empty() {
        return Ok("0".to_owned());
    }
    let negative = d[0] & 0x80 == 0;
    d[0] ^= 0x80;
    if negative {
        for x in &mut d {
            *x = !*x;
        }
    }
    let mut pos = 0;
    let mut next = |n: usize| {
        let v = be_uint(&d[pos..pos + n]);
        pos += n;
        v
    };
    let mut int = String::new();
    if intg0x > 0 {
        int.push_str(&next(DIG2BYTES[intg0x]).to_string());
    }
    for _ in 0..intg0 {
        let _ = write!(int, "{:09}", next(4));
    }
    let int = int.trim_start_matches('0');
    let mut out = String::with_capacity(intg + scale + 2);
    if negative {
        out.push('-');
    }
    out.push_str(if int.is_empty() { "0" } else { int });
    if scale > 0 {
        out.push('.');
        for _ in 0..frac0 {
            let _ = write!(out, "{:09}", next(4));
        }
        if frac0x > 0 {
            let _ = write!(out, "{:0w$}", next(DIG2BYTES[frac0x]), w = frac0x);
        }
    }
    Ok(out)
}

/// MariaDB's `UUID` column bytes (16, as logged) in the canonical text
/// form. The binlog carries the bytes in text order for every UUID version
/// (verified on 11.8 and 12.3).
#[must_use]
pub fn uuid_text(b: &[u8]) -> Option<String> {
    if b.len() != 16 {
        return None;
    }
    let hex = |s: &[u8]| crate::buf::hex(s, false);
    Some(format!(
        "{}-{}-{}-{}-{}",
        hex(&b[0..4]),
        hex(&b[4..6]),
        hex(&b[6..8]),
        hex(&b[8..10]),
        hex(&b[10..16])
    ))
}

/// MariaDB's `INET4` column bytes (4, network order) as dotted quad.
#[must_use]
pub fn inet4_text(b: &[u8]) -> Option<String> {
    let a: [u8; 4] = b.try_into().ok()?;
    Some(std::net::Ipv4Addr::from(a).to_string())
}

/// MariaDB's `INET6` column bytes (16, network order) as text; IPv4-mapped
/// addresses print as `::ffff:1.2.3.4`, as MariaDB does.
#[must_use]
pub fn inet6_text(b: &[u8]) -> Option<String> {
    let a: [u8; 16] = b.try_into().ok()?;
    Some(std::net::Ipv6Addr::from(a).to_string())
}

/// MariaDB's `VECTOR(N)` column bytes (`4N` little-endian `f32`s) as floats.
#[must_use]
pub fn vector_values(b: &[u8]) -> Option<Vec<f32>> {
    if !b.len().is_multiple_of(4) {
        return None;
    }
    Some(
        b.chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(kind: ColumnType, meta: u16, unsigned: Option<bool>) -> Column {
        Column {
            type_code: 0,
            meta,
            kind,
            nullable: true,
            name: None,
            unsigned,
            collation: None,
            members: None,
            geometry_type: None,
        }
    }

    fn dec(bytes: &[u8], p: u8, s: u8) -> String {
        decode_decimal(&mut Buf::new(bytes), p, s).unwrap()
    }

    #[test]
    fn decimals_match_mariadb_text() {
        // DECIMAL(10,4): 1234.5678 and -1234.5678 (from MySQL's documentation).
        assert_eq!(dec(&[0x80, 0x04, 0xd2, 0x16, 0x2e], 10, 4), "1234.5678");
        assert_eq!(dec(&[0x7f, 0xfb, 0x2d, 0xe9, 0xd1], 10, 4), "-1234.5678");
        // DECIMAL(14,4) 1234567890.1234: 4 + 1 + 2 bytes.
        assert_eq!(
            dec(&[0x81, 0x0d, 0xfb, 0x38, 0xd2, 0x04, 0xd2], 14, 4),
            "1234567890.1234"
        );
        // DECIMAL(5,2) 0.00 and DECIMAL(3,0) 7.
        assert_eq!(dec(&[0x80, 0x00, 0x00], 5, 2), "0.00");
        assert_eq!(dec(&[0x80, 0x07], 3, 0), "7");
    }

    #[test]
    fn integers_respect_signedness() {
        let v = |kind, uns, bytes: &[u8]| {
            decode_value(&col(kind, 0, uns), &mut Buf::new(bytes)).unwrap()
        };
        assert_eq!(v(ColumnType::TinyInt, None, &[0xff]), Value::Int(-1));
        assert_eq!(
            v(ColumnType::TinyInt, Some(true), &[0xff]),
            Value::UInt(255)
        );
        assert_eq!(
            v(ColumnType::MediumInt, Some(false), &[0xff, 0xff, 0xff]),
            Value::Int(-1)
        );
        assert_eq!(
            v(ColumnType::MediumInt, Some(true), &[0xff, 0xff, 0xff]),
            Value::UInt(0xff_ffff)
        );
        assert_eq!(
            v(ColumnType::BigInt, Some(true), &[0xff; 8]),
            Value::UInt(u64::MAX)
        );
        assert_eq!(v(ColumnType::BigInt, None, &[0xff; 8]), Value::Int(-1));
    }

    #[test]
    fn temporal_values() {
        let d =
            |kind, bytes: &[u8]| decode_value(&col(kind, 0, None), &mut Buf::new(bytes)).unwrap();
        // DATETIME2(0) 2026-11-07 16:49:34 and DATETIME2(6) with .123456.
        let ymd = (2026u64 * 13 + 11) << 5 | 7;
        let hms = (16u64 << 12) + (49 << 6) + 34;
        let packed = (ymd << 17 | hms) + 0x80_0000_0000;
        let raw = &packed.to_be_bytes()[3..];
        let Value::DateTime(dt) = d(
            ColumnType::DateTime {
                fsp: 0,
                legacy: false,
            },
            raw,
        ) else {
            panic!()
        };
        assert_eq!(dt.to_string(), "2026-11-07 16:49:34");
        let mut raw6 = raw.to_vec();
        raw6.extend_from_slice(&123_456u32.to_be_bytes()[1..]);
        let Value::DateTime(dt) = d(
            ColumnType::DateTime {
                fsp: 6,
                legacy: false,
            },
            &raw6,
        ) else {
            panic!()
        };
        assert_eq!(dt.to_string(), "2026-11-07 16:49:34.123456");
        // TIME2(0) -838:59:59 and TIME2(2) -00:00:00.01.
        let neg = (0x80_0000i64 - ((838 << 12) + (59 << 6) + 59)).to_be_bytes();
        let Value::Time(t) = d(
            ColumnType::Time {
                fsp: 0,
                legacy: false,
            },
            &neg[5..],
        ) else {
            panic!()
        };
        assert_eq!(t.to_string(), "-838:59:59");
        let Value::Time(t) = d(
            ColumnType::Time {
                fsp: 2,
                legacy: false,
            },
            &[0x7f, 0xff, 0xff, 0xff],
        ) else {
            panic!()
        };
        assert_eq!(t.to_string(), "-00:00:00.01");
        // DATE 2024-02-29; YEAR 2155; TIMESTAMP2(3) 1 s .5.
        let date = ((2024u32 << 9) + (2 << 5) + 29).to_le_bytes();
        assert_eq!(
            d(ColumnType::Date, &date[..3]),
            Value::Date(Date {
                year: 2024,
                month: 2,
                day: 29
            })
        );
        assert_eq!(d(ColumnType::Year, &[255]), Value::Year(2155));
        let Value::Timestamp(ts) = d(
            ColumnType::Timestamp {
                fsp: 3,
                legacy: false,
            },
            &[0, 0, 0, 1, 0x13, 0x88],
        ) else {
            panic!()
        };
        assert_eq!(ts.to_string(), "1970-01-01 00:00:01.500");
        let ts = Timestamp {
            secs: 1_709_164_800,
            micros: 0,
            fsp: 0,
        };
        assert_eq!(ts.to_string(), "2024-02-29 00:00:00");
        assert_eq!(
            Timestamp {
                secs: 0,
                micros: 0,
                fsp: 0
            }
            .to_string(),
            "0000-00-00 00:00:00"
        );
    }

    #[test]
    fn rows_skip_absent_columns_and_honour_nulls() {
        let cols = vec![
            col(ColumnType::Int, 0, None),
            col(ColumnType::TinyInt, 0, None),
            col(
                ColumnType::VarChar {
                    max_len: 10,
                    compressed: false,
                },
                10,
                None,
            ),
        ];
        // Columns 0 and 2 present; column 2 null.
        let data = [0b10, 5, 0, 0, 0];
        let row = decode_row(&cols, &[0b101], &mut Buf::new(&data)).unwrap();
        assert_eq!(row.0, vec![Some(Value::Int(5)), None, Some(Value::Null)]);
    }

    #[test]
    fn type_plugin_helpers() {
        assert_eq!(inet4_text(&[10, 0, 0, 1]).unwrap(), "10.0.0.1");
        let mut v6 = [0u8; 16];
        v6[10] = 0xff;
        v6[11] = 0xff;
        v6[12..].copy_from_slice(&[1, 2, 3, 4]);
        assert_eq!(inet6_text(&v6).unwrap(), "::ffff:1.2.3.4");
        assert_eq!(vector_values(&1.5f32.to_le_bytes()).unwrap(), vec![1.5]);
        assert!(vector_values(&[1, 2, 3]).is_none());
        let u = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0x1d, 0xef, 0x81, 0x23, 0x45, 0x67, 0x89, 0xab,
            0xcd, 0xef,
        ];
        assert_eq!(
            uuid_text(&u).unwrap(),
            "12345678-9abc-1def-8123-456789abcdef"
        );
    }
}
