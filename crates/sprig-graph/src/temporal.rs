//! Temporal and spatial values (chapter 43, "Values"; build steps 10 and
//! 19): Neo4j's `Date`, `LocalTime`, `Time`, `LocalDateTime`, `DateTime`,
//! `Duration` and `Point`, their constructors and accessors, arithmetic,
//! comparison and text forms.
//!
//! - **Stored** as ISO 8601 strings (points as `{"srid", "x", "y"[, "z"]}`
//!   objects) with a `_types` entry naming the type (chapter 31's map), so
//!   they read back as themselves.
//! - **Text** follows Neo4j: seconds always shown, fractions without
//!   trailing zeros, `Z` for UTC, a named zone after its offset
//!   (`+01:00[Europe/Berlin]`), durations as `P1Y2M3DT4H5M6.5S` with days
//!   and seconds unnormalized.
//! - **Time zones**: offsets and IANA names (chrono-tz).
//! - **Points**: SRIDs 7203 (cartesian), 9157 (cartesian-3d), 4326
//!   (wgs-84) and 4979 (wgs-84-3d); `point.distance` is Euclidean, or the
//!   haversine on a sphere of radius 6,378,140 m for WGS-84, as Neo4j.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Write as _;

use chrono::{
    DateTime, Datelike, Duration as ChronoDur, LocalResult, Months, NaiveDate, NaiveDateTime,
    NaiveTime, Offset, TimeZone, Timelike, Utc,
};
use sprig_common::{Result, SprigCode, SprigError};

use crate::value::Value;

const NANOS: i64 = 1_000_000_000;

/// A `DateTime`: the wall clock in its zone, the zone's offset then, and
/// the zone's name when it has one.
#[derive(Debug, Clone, PartialEq)]
pub struct Zoned {
    /// The local date and time.
    pub local: NaiveDateTime,
    /// Seconds east of UTC.
    pub offset: i32,
    /// An IANA zone name (`Europe/Berlin`), else the offset is the zone.
    pub zone: Option<String>,
}

impl Zoned {
    /// Seconds and nanoseconds since the epoch, UTC.
    #[must_use]
    pub fn epoch(&self) -> (i64, u32) {
        let utc = self.local - ChronoDur::seconds(i64::from(self.offset));
        (utc.and_utc().timestamp(), utc.nanosecond())
    }
}

/// A `Duration`: months, days, seconds and nanoseconds kept apart, as
/// Neo4j keeps them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Dur {
    /// Months.
    pub months: i64,
    /// Days.
    pub days: i64,
    /// Seconds.
    pub secs: i64,
    /// Nanoseconds, 0 to 999,999,999.
    pub nanos: i64,
}

impl Dur {
    /// A duration from Bolt's fields, with the nanoseconds brought into
    /// 0..999,999,999 as Neo4j does (drivers may send them with the
    /// seconds' sign: the Python driver sends -1 s -854,775,808 ns for
    /// -9,223,372,037 s 145,224,192 ns); `None` when the seconds overflow.
    #[must_use]
    pub fn from_fields(months: i64, days: i64, secs: i64, nanos: i64) -> Option<Dur> {
        Some(Dur {
            months,
            days,
            secs: secs.checked_add(nanos.div_euclid(NANOS))?,
            nanos: nanos.rem_euclid(NANOS),
        })
    }

    fn norm(months: i64, days: i64, secs: i64, nanos: i64) -> Dur {
        let secs = secs + nanos.div_euclid(NANOS);
        Dur {
            months,
            days,
            secs,
            nanos: nanos.rem_euclid(NANOS),
        }
    }
}

/// A point.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    /// 7203, 9157, 4326 or 4979.
    pub srid: u32,
    /// x or longitude.
    pub x: f64,
    /// y or latitude.
    pub y: f64,
    /// z or height.
    pub z: Option<f64>,
}

impl Point {
    /// Whether the CRS is geographic.
    #[must_use]
    pub fn geographic(&self) -> bool {
        matches!(self.srid, 4326 | 4979)
    }

    /// The CRS name.
    #[must_use]
    pub fn crs(&self) -> &'static str {
        match self.srid {
            9157 => "cartesian-3d",
            4326 => "wgs-84",
            4979 => "wgs-84-3d",
            _ => "cartesian",
        }
    }
}

fn bad(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::GraphPropertyType, msg.into())
}

fn unsupported(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::GraphUnsupported, msg.into())
}

// ----------------------------------------------------------------------
// Text.
// ----------------------------------------------------------------------

fn frac(nanos: u32) -> String {
    if nanos == 0 {
        return String::new();
    }
    let s = format!(".{nanos:09}");
    s.trim_end_matches('0').to_owned()
}

fn time_text(t: NaiveTime) -> String {
    format!(
        "{:02}:{:02}:{:02}{}",
        t.hour(),
        t.minute(),
        t.second(),
        frac(t.nanosecond())
    )
}

fn date_text(d: NaiveDate) -> String {
    let y = d.year();
    if (0..=9999).contains(&y) {
        format!("{y:04}-{:02}-{:02}", d.month(), d.day())
    } else {
        format!("{y:+05}-{:02}-{:02}", d.month(), d.day())
    }
}

fn offset_text(secs: i32) -> String {
    if secs == 0 {
        return "Z".into();
    }
    let sign = if secs < 0 { '-' } else { '+' };
    let a = secs.unsigned_abs();
    let (h, m, s) = (a / 3600, (a / 60) % 60, a % 60);
    if s == 0 {
        format!("{sign}{h:02}:{m:02}")
    } else {
        format!("{sign}{h:02}:{m:02}:{s:02}")
    }
}

fn float_text(f: f64) -> String {
    crate::value::fmt_float(f)
}

/// Neo4j's text of a temporal or point value.
#[must_use]
pub fn text(v: &Value) -> Option<String> {
    Some(match v {
        Value::Date(d) => date_text(*d),
        Value::LocalTime(t) => time_text(*t),
        Value::Time(t, o) => format!("{}{}", time_text(*t), offset_text(*o)),
        Value::LocalDateTime(dt) => format!("{}T{}", date_text(dt.date()), time_text(dt.time())),
        Value::DateTime(z) => {
            let mut s = format!(
                "{}T{}{}",
                date_text(z.local.date()),
                time_text(z.local.time()),
                offset_text(z.offset)
            );
            if let Some(name) = &z.zone {
                let _ = write!(s, "[{name}]");
            }
            s
        }
        Value::Duration(d) => dur_text(d),
        Value::Point(p) => {
            let mut s = format!("point({{x: {}, y: {}", float_text(p.x), float_text(p.y));
            if let Some(z) = p.z {
                let _ = write!(s, ", z: {}", float_text(z));
            }
            let _ = write!(s, ", crs: '{}'}})", p.crs());
            s
        }
        _ => return None,
    })
}

fn dur_text(d: &Dur) -> String {
    let mut s = String::from("P");
    let (y, m) = (d.months / 12, d.months % 12);
    if y != 0 {
        let _ = write!(s, "{y}Y");
    }
    if m != 0 {
        let _ = write!(s, "{m}M");
    }
    if d.days != 0 {
        let _ = write!(s, "{}D", d.days);
    }
    // Seconds and nanoseconds as one signed quantity.
    let total = i128::from(d.secs) * i128::from(NANOS) + i128::from(d.nanos);
    if total != 0 {
        s.push('T');
        let neg = total < 0;
        let a = total.unsigned_abs();
        let n = u128::try_from(NANOS).unwrap_or(1);
        let secs = a / n;
        let nanos = u32::try_from(a % n).unwrap_or(0);
        let (h, mi, se) = (secs / 3600, (secs / 60) % 60, secs % 60);
        let sign = if neg { "-" } else { "" };
        if h != 0 {
            let _ = write!(s, "{sign}{h}H");
        }
        if mi != 0 {
            let _ = write!(s, "{sign}{mi}M");
        }
        if se != 0 || nanos != 0 {
            let _ = write!(s, "{sign}{se}{}S", frac(nanos));
        }
    }
    if s == "P" {
        s.push_str("T0S");
    }
    s
}

// ----------------------------------------------------------------------
// Parsing.
// ----------------------------------------------------------------------

fn parse_time(s: &str) -> Option<NaiveTime> {
    let (main, nanos) = match s.split_once(['.', ',']) {
        Some((a, f)) => {
            if f.is_empty() || f.len() > 9 || !f.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let n: u32 = format!("{f:0<9}").parse().ok()?;
            (a, n)
        }
        None => (s, 0),
    };
    let parts: Vec<&str> = if main.contains(':') {
        main.split(':').collect()
    } else {
        // hhmm[ss]
        let b = main.as_bytes();
        if !main.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        match b.len() {
            2 => vec![&main[0..2]],
            4 => vec![&main[0..2], &main[2..4]],
            6 => vec![&main[0..2], &main[2..4], &main[4..6]],
            _ => return None,
        }
    };
    let n = |i: usize| -> Option<u32> { parts.get(i).map_or(Some(0), |p| p.parse().ok()) };
    if parts.is_empty() || parts.len() > 3 {
        return None;
    }
    NaiveTime::from_hms_nano_opt(n(0)?, n(1)?, n(2)?, nanos)
}

fn parse_date(s: &str) -> Option<NaiveDate> {
    if s.len() == 8 && s.bytes().all(|b| b.is_ascii_digit()) {
        return NaiveDate::parse_from_str(s, "%Y%m%d").ok();
    }
    let parts: Vec<&str> = s.split('-').collect();
    match parts.as_slice() {
        [y, m, d] => NaiveDate::from_ymd_opt(y.parse().ok()?, m.parse().ok()?, d.parse().ok()?),
        [y, m] => NaiveDate::from_ymd_opt(y.parse().ok()?, m.parse().ok()?, 1),
        [y] if y.len() == 4 => NaiveDate::from_ymd_opt(y.parse().ok()?, 1, 1),
        _ => None,
    }
}

/// Splits a trailing offset (`Z`, `+01:00`, `-0530`) and zone name
/// (`[Europe/Berlin]`) off a time text.
fn split_zone(s: &str) -> (&str, Option<i32>, Option<String>) {
    let (s, zone) = match s.find('[') {
        Some(i) if s.ends_with(']') => (&s[..i], Some(s[i + 1..s.len() - 1].to_owned())),
        _ => (s, None),
    };
    if let Some(rest) = s.strip_suffix('Z').or_else(|| s.strip_suffix('z')) {
        return (rest, Some(0), zone);
    }
    if let Some(i) = s.rfind(['+', '-']) {
        let (body, off) = s.split_at(i);
        let sign = if off.starts_with('-') { -1 } else { 1 };
        let digits: String = off[1..].chars().filter(char::is_ascii_digit).collect();
        let (h, m, sec) = match digits.len() {
            2 => (digits[..2].parse().ok(), Some(0), Some(0)),
            4 => (digits[..2].parse().ok(), digits[2..].parse().ok(), Some(0)),
            6 => (
                digits[..2].parse().ok(),
                digits[2..4].parse().ok(),
                digits[4..].parse().ok(),
            ),
            _ => (None, None, None),
        };
        if let (Some(h), Some(m), Some(sec)) = (h, m, sec) {
            let h: i32 = h;
            let m: i32 = m;
            let sec: i32 = sec;
            return (body, Some(sign * (h * 3600 + m * 60 + sec)), zone);
        }
    }
    (s, None, zone)
}

/// A zone's offset at a local date and time.
fn zone_offset(name: &str, local: NaiveDateTime) -> Result<i32> {
    if let Some(off) = fixed_offset(name) {
        return Ok(off);
    }
    let tz: chrono_tz::Tz = name
        .parse()
        .map_err(|_| bad(format!("Invalid value for TimeZone: Text '{name}'")))?;
    Ok(match tz.from_local_datetime(&local) {
        LocalResult::Single(d) | LocalResult::Ambiguous(d, _) => d.offset().fix().local_minus_utc(),
        LocalResult::None => tz
            .from_utc_datetime(&local)
            .offset()
            .fix()
            .local_minus_utc(),
    })
}

fn fixed_offset(name: &str) -> Option<i32> {
    if name.eq_ignore_ascii_case("Z") || name.eq_ignore_ascii_case("UTC") {
        return Some(0);
    }
    let (_, off, _) = split_zone(&format!("00:00{name}"));
    off.filter(|_| name.starts_with(['+', '-']))
}

/// Whether a zone name is only an offset (`Z`, `+01:00`), which a value
/// keeps as its offset alone. `UTC` is a named zone, as in Neo4j (Java's
/// `ZoneId.of("UTC")` is a region): `datetime('...[UTC]')` keeps `[UTC]`.
fn offset_only(name: &str) -> bool {
    name.eq_ignore_ascii_case("Z") || name.starts_with(['+', '-'])
}

fn zoned(local: NaiveDateTime, offset: Option<i32>, zone: Option<String>) -> Result<Zoned> {
    match (offset, zone) {
        (Some(o), z) => Ok(Zoned {
            local,
            offset: o,
            zone: z,
        }),
        (None, Some(z)) => {
            let off = zone_offset(&z, local)?;
            Ok(Zoned {
                local,
                offset: off,
                zone: if offset_only(&z) { None } else { Some(z) },
            })
        }
        (None, None) => Ok(Zoned {
            local,
            offset: 0,
            zone: None,
        }),
    }
}

/// Parses `kind`'s text form (`date`, `localtime`, `time`,
/// `localdatetime`, `datetime`, `duration`).
///
/// # Errors
/// 39909 for text that isn't the type.
pub fn parse(kind: &str, s: &str) -> Result<Value> {
    let err = || bad(format!("Text cannot be parsed to a {kind}: \"{s}\""));
    let s = s.trim();
    Ok(match kind {
        "date" => Value::Date(parse_date(s).ok_or_else(err)?),
        "localtime" => Value::LocalTime(parse_time(s).ok_or_else(err)?),
        "time" => {
            let (body, off, _) = split_zone(s);
            Value::Time(parse_time(body).ok_or_else(err)?, off.unwrap_or(0))
        }
        "localdatetime" => {
            let (d, t) = s.split_once('T').unwrap_or((s, "00:00"));
            Value::LocalDateTime(
                parse_date(d)
                    .ok_or_else(err)?
                    .and_time(parse_time(t).ok_or_else(err)?),
            )
        }
        "datetime" => {
            let (d, t) = s.split_once('T').unwrap_or((s, "00:00"));
            let (body, off, zone) = split_zone(t);
            let local = parse_date(d)
                .ok_or_else(err)?
                .and_time(parse_time(body).ok_or_else(err)?);
            Value::DateTime(Box::new(zoned(local, off, zone)?))
        }
        "duration" => Value::Duration(parse_duration(s).ok_or_else(err)?),
        _ => return Err(err()),
    })
}

fn parse_duration(s: &str) -> Option<Dur> {
    let (neg, s) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s),
    };
    let s = s.strip_prefix(['P', 'p'])?;
    let (date, time) = match s.split_once(['T', 't']) {
        Some((a, b)) => (a, Some(b)),
        None => (s, None),
    };
    let mut d = Dur::default();
    let mut num = String::new();
    for c in date.chars() {
        if c.is_ascii_digit() || c == '-' || c == '.' {
            num.push(c);
            continue;
        }
        let n: f64 = num.parse().ok()?;
        num.clear();
        #[allow(clippy::cast_possible_truncation)]
        match c.to_ascii_uppercase() {
            'Y' => d.months += (n * 12.0) as i64,
            'M' => d.months += n as i64,
            'W' => d.days += (n * 7.0) as i64,
            'D' => d.days += n as i64,
            _ => return None,
        }
    }
    if !num.is_empty() {
        return None;
    }
    let mut nanos: i128 = 0;
    if let Some(t) = time {
        for c in t.chars() {
            if c.is_ascii_digit() || c == '-' || c == '.' || c == ',' {
                num.push(if c == ',' { '.' } else { c });
                continue;
            }
            let unit: i128 = match c.to_ascii_uppercase() {
                'H' => 3600 * i128::from(NANOS),
                'M' => 60 * i128::from(NANOS),
                'S' => i128::from(NANOS),
                _ => return None,
            };
            nanos += decimal_nanos(&num, unit)?;
            num.clear();
        }
        if !num.is_empty() {
            return None;
        }
    }
    let n = i128::from(NANOS);
    let secs = i64::try_from(nanos.div_euclid(n)).ok()?;
    let rem = i64::try_from(nanos.rem_euclid(n)).ok()?;
    let mut d = Dur::norm(d.months, d.days, secs, rem);
    if neg {
        d = negate(&d);
    }
    Some(d)
}

/// `num` (a decimal) times `unit` nanoseconds, exactly.
fn decimal_nanos(num: &str, unit: i128) -> Option<i128> {
    let (neg, num) = match num.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, num),
    };
    let (int, frac) = num.split_once('.').unwrap_or((num, ""));
    let i: i128 = if int.is_empty() { 0 } else { int.parse().ok()? };
    let mut v = i * unit;
    if !frac.is_empty() {
        let digits = i32::try_from(frac.len()).ok()?;
        let f: i128 = frac.parse().ok()?;
        v += f * unit / 10i128.pow(u32::try_from(digits).ok()?);
    }
    Some(if neg { -v } else { v })
}

fn negate(d: &Dur) -> Dur {
    Dur::norm(-d.months, -d.days, -d.secs, -d.nanos)
}

// ----------------------------------------------------------------------
// Constructors.
// ----------------------------------------------------------------------

fn num_of(m: &BTreeMap<String, Value>, k: &str) -> Result<Option<f64>> {
    #[allow(clippy::cast_precision_loss)]
    match m.get(k) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Int(i)) => Ok(Some(*i as f64)),
        Some(Value::Float(f)) => Ok(Some(*f)),
        Some(other) => Err(bad(format!(
            "{k} must be a number, got {}",
            other.type_name()
        ))),
    }
}

#[allow(clippy::cast_possible_truncation)]
fn int_of(m: &BTreeMap<String, Value>, k: &str, default: i64) -> Result<i64> {
    Ok(num_of(m, k)?.map_or(default, |f| f as i64))
}

fn u32_of(m: &BTreeMap<String, Value>, k: &str, default: i64) -> Result<u32> {
    u32::try_from(int_of(m, k, default)?).map_err(|_| bad(format!("{k} is out of range")))
}

fn time_from_map(m: &BTreeMap<String, Value>) -> Result<NaiveTime> {
    let nanos = int_of(m, "millisecond", 0)? * 1_000_000
        + int_of(m, "microsecond", 0)? * 1000
        + int_of(m, "nanosecond", 0)?;
    NaiveTime::from_hms_nano_opt(
        u32_of(m, "hour", 0)?,
        u32_of(m, "minute", 0)?,
        u32_of(m, "second", 0)?,
        u32::try_from(nanos).map_err(|_| bad("nanoseconds out of range"))?,
    )
    .ok_or_else(|| bad("invalid time"))
}

fn date_from_map(m: &BTreeMap<String, Value>) -> Result<NaiveDate> {
    let y = i32::try_from(int_of(m, "year", 1970)?).map_err(|_| bad("year out of range"))?;
    if m.contains_key("week") {
        let w = u32_of(m, "week", 1)?;
        let dow = u32_of(m, "dayOfWeek", 1)?;
        let wd = chrono::Weekday::try_from(u8::try_from(dow.saturating_sub(1)).unwrap_or(0))
            .map_err(|_| bad("invalid dayOfWeek"))?;
        return NaiveDate::from_isoywd_opt(y, w, wd).ok_or_else(|| bad("invalid week date"));
    }
    if m.contains_key("ordinalDay") {
        return NaiveDate::from_yo_opt(y, u32_of(m, "ordinalDay", 1)?)
            .ok_or_else(|| bad("invalid ordinal date"));
    }
    if m.contains_key("quarter") {
        let q = u32_of(m, "quarter", 1)?;
        let first =
            NaiveDate::from_ymd_opt(y, (q - 1) * 3 + 1, 1).ok_or_else(|| bad("invalid quarter"))?;
        let d = u64::from(u32_of(m, "dayOfQuarter", 1)?.saturating_sub(1));
        return first
            .checked_add_days(chrono::Days::new(d))
            .ok_or_else(|| bad("invalid quarter date"));
    }
    NaiveDate::from_ymd_opt(y, u32_of(m, "month", 1)?, u32_of(m, "day", 1)?)
        .ok_or_else(|| bad("invalid date"))
}

fn now_utc() -> DateTime<Utc> {
    Utc::now()
}

/// The current instant in zone `tz` (`None`: UTC).
fn now_in(tz: Option<&str>) -> Result<Zoned> {
    let now = now_utc().naive_utc();
    match tz {
        None => Ok(Zoned {
            local: now,
            offset: 0,
            zone: None,
        }),
        Some(z) => {
            let off = if let Some(o) = fixed_offset(z) {
                o
            } else {
                let tz: chrono_tz::Tz = z
                    .parse()
                    .map_err(|_| bad(format!("Invalid value for TimeZone: Text '{z}'")))?;
                tz.from_utc_datetime(&now).offset().fix().local_minus_utc()
            };
            Ok(Zoned {
                local: now + ChronoDur::seconds(i64::from(off)),
                offset: off,
                zone: (!offset_only(z)).then(|| z.to_owned()),
            })
        }
    }
}

/// `date()`, `time()`, ..., `duration()`, `point()`: `None` when `name`
/// isn't one of them.
///
/// # Errors
/// 39909 for arguments that don't make the value.
pub fn construct(name: &str, args: &[Value]) -> Result<Option<Value>> {
    let kind = match name {
        "date" | "localtime" | "time" | "localdatetime" | "datetime" | "duration" | "point" => name,
        "date.realtime" | "date.statement" | "date.transaction" => "date",
        "localtime.realtime" | "localtime.statement" | "localtime.transaction" => "localtime",
        "time.realtime" | "time.statement" | "time.transaction" => "time",
        "localdatetime.realtime" | "localdatetime.statement" | "localdatetime.transaction" => {
            "localdatetime"
        }
        "datetime.realtime" | "datetime.statement" | "datetime.transaction" => "datetime",
        "datetime.fromepoch" => {
            let (Some(Value::Int(s)), Some(Value::Int(n))) = (args.first(), args.get(1)) else {
                return Err(bad("datetime.fromepoch(seconds, nanoseconds)"));
            };
            let dt = DateTime::from_timestamp(*s, u32::try_from(*n).unwrap_or(0))
                .ok_or_else(|| bad("epoch out of range"))?;
            return Ok(Some(Value::DateTime(Box::new(Zoned {
                local: dt.naive_utc(),
                offset: 0,
                zone: None,
            }))));
        }
        "date.truncate"
        | "localtime.truncate"
        | "time.truncate"
        | "localdatetime.truncate"
        | "datetime.truncate" => {
            let kind = name.trim_end_matches(".truncate");
            return truncate(kind, args).map(Some);
        }
        "datetime.fromepochmillis" => {
            let Some(Value::Int(ms)) = args.first() else {
                return Err(bad("datetime.fromepochmillis(milliseconds)"));
            };
            let dt =
                DateTime::from_timestamp_millis(*ms).ok_or_else(|| bad("epoch out of range"))?;
            return Ok(Some(Value::DateTime(Box::new(Zoned {
                local: dt.naive_utc(),
                offset: 0,
                zone: None,
            }))));
        }
        _ => return Ok(None),
    };
    let arg = args.first().cloned().unwrap_or(Value::Null);
    if args.is_empty() && kind != "point" && kind != "duration" {
        let now = now_in(None)?;
        return Ok(Some(of_kind(kind, &now)));
    }
    Ok(Some(match (kind, arg) {
        (_, Value::Null) if !args.is_empty() => Value::Null,
        ("point", Value::Map(m)) => point_from_map(&m)?,
        ("duration", Value::Map(m)) => duration_from_map(&m)?,
        (_, Value::String(s)) => parse(kind, &s)?,
        (_, Value::Map(m)) => temporal_from_map(kind, &m)?,
        (k, v) if same_kind(k, &v) => v,
        (k, v) => match convert(k, &v) {
            Some(x) => x,
            None => return Err(bad(format!("{name}() can't take a {}", v.type_name()))),
        },
    }))
}

/// Another temporal value's parts as `kind` (Neo4j 5: `date(dt)`,
/// `localdatetime(datetime(...))`, ...; local values become zoned at UTC).
/// `None` when `v` lacks a part `kind` needs (a date has no time).
fn convert(kind: &str, v: &Value) -> Option<Value> {
    Some(match (kind, v) {
        ("date", Value::DateTime(z)) => Value::Date(z.local.date()),
        ("date", Value::LocalDateTime(l)) => Value::Date(l.date()),
        ("localtime", Value::DateTime(z)) => Value::LocalTime(z.local.time()),
        ("localtime", Value::LocalDateTime(l)) => Value::LocalTime(l.time()),
        ("localtime", Value::Time(t, _)) => Value::LocalTime(*t),
        ("time", Value::DateTime(z)) => Value::Time(z.local.time(), z.offset),
        ("time", Value::LocalDateTime(l)) => Value::Time(l.time(), 0),
        ("time", Value::LocalTime(t)) => Value::Time(*t, 0),
        ("localdatetime", Value::DateTime(z)) => Value::LocalDateTime(z.local),
        ("datetime", Value::LocalDateTime(l)) => Value::DateTime(Box::new(Zoned {
            local: *l,
            offset: 0,
            zone: None,
        })),
        _ => return None,
    })
}

/// `<kind>.truncate(unit, value [, map])` (round 3): the value cut down to
/// `unit` (`millennium`, `century`, `decade`, `year`, `weekYear`,
/// `quarter`, `month`, `week`, `day`, `hour`, `minute`, `second`,
/// `millisecond`, `microsecond`), then the map's fields on top, as Neo4j
/// does.
fn truncate(kind: &str, args: &[Value]) -> Result<Value> {
    let (Some(Value::String(unit)), Some(v)) = (args.first(), args.get(1)) else {
        if matches!(args.get(1), Some(Value::Null)) {
            return Ok(Value::Null);
        }
        return Err(bad(format!("{kind}.truncate(unit, value [, map])")));
    };
    let extra = match args.get(2) {
        Some(Value::Map(m)) => m.clone(),
        None | Some(Value::Null) => BTreeMap::new(),
        Some(o) => {
            return Err(bad(format!(
                "{kind}.truncate's third argument is a {}",
                o.type_name()
            )));
        }
    };
    let (date, time, zone): (Option<NaiveDate>, Option<NaiveTime>, Option<String>) = match v {
        Value::Date(d) => (Some(*d), None, None),
        Value::LocalTime(t) => (None, Some(*t), None),
        Value::Time(t, o) => (None, Some(*t), Some(offset_text(*o))),
        Value::LocalDateTime(dt) => (Some(dt.date()), Some(dt.time()), None),
        Value::DateTime(z) => (
            Some(z.local.date()),
            Some(z.local.time()),
            Some(z.zone.clone().unwrap_or_else(|| offset_text(z.offset))),
        ),
        o => {
            return Err(bad(format!(
                "{kind}.truncate can't take a {}",
                o.type_name()
            )));
        }
    };
    let needs_date = matches!(kind, "date" | "localdatetime" | "datetime");
    if needs_date && date.is_none() {
        return Err(bad(format!("{kind}.truncate needs a value with a date")));
    }
    let date_unit = matches!(
        unit.as_str(),
        "millennium"
            | "century"
            | "decade"
            | "year"
            | "weekYear"
            | "quarter"
            | "month"
            | "week"
            | "day"
    );
    if !needs_date && date_unit && unit != "day" {
        return Err(bad(format!("{kind}.truncate can't truncate to {unit}")));
    }
    let mut m: BTreeMap<String, Value> = BTreeMap::new();
    if let Some(d) = date {
        let y = d.year();
        let nd = match unit.as_str() {
            "millennium" => NaiveDate::from_ymd_opt(y - y.rem_euclid(1000), 1, 1),
            "century" => NaiveDate::from_ymd_opt(y - y.rem_euclid(100), 1, 1),
            "decade" => NaiveDate::from_ymd_opt(y - y.rem_euclid(10), 1, 1),
            "year" => NaiveDate::from_ymd_opt(y, 1, 1),
            "weekYear" => NaiveDate::from_isoywd_opt(d.iso_week().year(), 1, chrono::Weekday::Mon),
            "quarter" => NaiveDate::from_ymd_opt(y, (d.month0() / 3) * 3 + 1, 1),
            "month" => NaiveDate::from_ymd_opt(y, d.month(), 1),
            "week" => d.checked_sub_days(chrono::Days::new(u64::from(
                d.weekday().num_days_from_monday(),
            ))),
            _ => Some(d),
        }
        .ok_or_else(|| bad("date out of range"))?;
        // `dayOfWeek` on a week truncation counts from its Monday.
        let nd = match (unit.as_str(), extra.get("dayOfWeek")) {
            ("week" | "weekYear", Some(Value::Int(dow))) => nd
                .checked_add_days(chrono::Days::new(u64::try_from(dow - 1).unwrap_or(0)))
                .ok_or_else(|| bad("date out of range"))?,
            _ => nd,
        };
        m.insert("year".into(), Value::Int(i64::from(nd.year())));
        m.insert("month".into(), Value::Int(i64::from(nd.month())));
        m.insert("day".into(), Value::Int(i64::from(nd.day())));
    }
    if let Some(t) = time {
        let ns = i64::from(t.nanosecond());
        let (h, mi, sec, nanos) = match unit.as_str() {
            u if date_unit || u == "day" => (0, 0, 0, 0),
            "hour" => (t.hour(), 0, 0, 0),
            "minute" => (t.hour(), t.minute(), 0, 0),
            "second" => (t.hour(), t.minute(), t.second(), 0),
            "millisecond" => (t.hour(), t.minute(), t.second(), ns - ns % 1_000_000),
            "microsecond" => (t.hour(), t.minute(), t.second(), ns - ns % 1000),
            other => return Err(bad(format!("unit {other} isn't supported"))),
        };
        m.insert("hour".into(), Value::Int(i64::from(h)));
        m.insert("minute".into(), Value::Int(i64::from(mi)));
        m.insert("second".into(), Value::Int(i64::from(sec)));
        m.insert("nanosecond".into(), Value::Int(nanos));
    } else if !date_unit {
        return Err(bad(format!(
            "{kind}.truncate can't truncate a date to {unit}"
        )));
    }
    if matches!(kind, "time" | "datetime")
        && let Some(z) = zone
    {
        m.insert("timezone".into(), Value::String(z));
    }
    for (k, x) in extra {
        if k == "dayOfWeek" {
            continue;
        }
        // A finer field given replaces the truncated sub-second fields.
        if matches!(k.as_str(), "millisecond" | "microsecond" | "nanosecond") {
            let base = match m.get("nanosecond") {
                Some(Value::Int(n)) => *n,
                _ => 0,
            };
            let add = match (&k[..], &x) {
                ("millisecond", Value::Int(i)) => i * 1_000_000 - (base - base % 1_000_000),
                ("microsecond", Value::Int(i)) => i * 1000 - (base % 1_000_000 - base % 1000),
                ("nanosecond", Value::Int(i)) => *i - base % 1000,
                _ => 0,
            };
            m.insert("nanosecond".into(), Value::Int(base + add));
            continue;
        }
        m.insert(k, x);
    }
    temporal_from_map(kind, &m)
}

fn same_kind(k: &str, v: &Value) -> bool {
    matches!(
        (k, v),
        ("date", Value::Date(_))
            | ("localtime", Value::LocalTime(_))
            | ("time", Value::Time(..))
            | ("localdatetime", Value::LocalDateTime(_))
            | ("datetime", Value::DateTime(_))
            | ("duration", Value::Duration(_))
    )
}

/// `kind`'s value of the instant `z`.
fn of_kind(kind: &str, z: &Zoned) -> Value {
    match kind {
        "date" => Value::Date(z.local.date()),
        "localtime" => Value::LocalTime(z.local.time()),
        "time" => Value::Time(z.local.time(), z.offset),
        "localdatetime" => Value::LocalDateTime(z.local),
        _ => Value::DateTime(Box::new(z.clone())),
    }
}

fn temporal_from_map(kind: &str, m: &BTreeMap<String, Value>) -> Result<Value> {
    let tz = match m.get("timezone") {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    };
    // Only a time zone: the current instant there.
    if m.len() == 1 && tz.is_some() {
        return Ok(of_kind(kind, &now_in(tz.as_deref())?));
    }
    if m.contains_key("epochSeconds") || m.contains_key("epochMillis") {
        return datetime_from_epoch_map(kind, m, tz);
    }
    Ok(match kind {
        "date" => Value::Date(date_from_map(m)?),
        "localtime" => Value::LocalTime(time_from_map(m)?),
        "time" => {
            let off = match &tz {
                Some(z) => fixed_offset(z)
                    .ok_or_else(|| unsupported("time() with a named zone (use an offset)"))?,
                None => 0,
            };
            Value::Time(time_from_map(m)?, off)
        }
        "localdatetime" => Value::LocalDateTime(date_from_map(m)?.and_time(time_from_map(m)?)),
        _ => {
            let local = date_from_map(m)?.and_time(time_from_map(m)?);
            Value::DateTime(Box::new(zoned(local, None, tz)?))
        }
    })
}

/// `datetime({epochSeconds, ...})` and `datetime({epochMillis, ...})`
/// (R8 round 10, LDBC IC10): the instant, in `timezone` when given (else
/// UTC); other fields given with it replace that part of the wall clock,
/// and `millisecond`, `microsecond` or `nanosecond` the whole fraction, as
/// Neo4j 5.26 does. Only `datetime` takes them; the epoch must be an
/// integer.
fn datetime_from_epoch_map(
    kind: &str,
    m: &BTreeMap<String, Value>,
    tz: Option<String>,
) -> Result<Value> {
    let which = if m.contains_key("epochSeconds") {
        "epochSeconds"
    } else {
        "epochMillis"
    };
    if kind != "datetime" {
        return Err(unsupported(which));
    }
    if m.contains_key("epochSeconds") && m.contains_key("epochMillis") {
        return Err(bad(
            "epochMillis cannot be selected together with epochSeconds.",
        ));
    }
    let utc = match (which, m.get(which)) {
        ("epochSeconds", Some(Value::Int(s))) => DateTime::from_timestamp(*s, 0),
        (_, Some(Value::Int(ms))) => DateTime::from_timestamp_millis(*ms),
        (_, v) => {
            return Err(bad(format!(
                "Cannot construct date time from: {}",
                v.filter(|v| !v.is_null())
                    .map_or("NO_VALUE", Value::type_name)
            )));
        }
    }
    .ok_or_else(|| bad("epoch out of range"))?
    .naive_utc();
    let offset = match &tz {
        Some(z) => zone_offset_at(z, utc.and_utc().timestamp())?,
        None => 0,
    };
    let mut local = utc + ChronoDur::seconds(i64::from(offset));
    let has = |k: &str| m.get(k).is_some_and(|v| !v.is_null());
    if ["year", "month", "day", "hour", "minute", "second"]
        .iter()
        .any(|k| has(k))
        || ["millisecond", "microsecond", "nanosecond"]
            .iter()
            .any(|k| has(k))
    {
        let d = local.date();
        let t = local.time();
        let date = NaiveDate::from_ymd_opt(
            i32::try_from(int_of(m, "year", i64::from(d.year()))?)
                .map_err(|_| bad("year out of range"))?,
            u32_of(m, "month", i64::from(d.month()))?,
            u32_of(m, "day", i64::from(d.day()))?,
        )
        .ok_or_else(|| bad("invalid date"))?;
        let fraction = if ["millisecond", "microsecond", "nanosecond"]
            .iter()
            .any(|k| has(k))
        {
            u32::try_from(
                int_of(m, "millisecond", 0)? * 1_000_000
                    + int_of(m, "microsecond", 0)? * 1000
                    + int_of(m, "nanosecond", 0)?,
            )
            .map_err(|_| bad("nanoseconds out of range"))?
        } else {
            t.nanosecond()
        };
        let time = NaiveTime::from_hms_nano_opt(
            u32_of(m, "hour", i64::from(t.hour()))?,
            u32_of(m, "minute", i64::from(t.minute()))?,
            u32_of(m, "second", i64::from(t.second()))?,
            fraction,
        )
        .ok_or_else(|| bad("invalid time"))?;
        local = date.and_time(time);
    }
    let zone = tz.filter(|z| !offset_only(z));
    Ok(Value::DateTime(Box::new(Zoned {
        local,
        offset,
        zone,
    })))
}

fn duration_from_map(m: &BTreeMap<String, Value>) -> Result<Value> {
    let f = |k: &str| num_of(m, k).map(|v| v.unwrap_or(0.0));
    #[allow(clippy::cast_possible_truncation)]
    let months = (f("years")? * 12.0 + f("quarters")? * 3.0 + f("months")?) as i64;
    let days_f = f("weeks")? * 7.0 + f("days")?;
    #[allow(clippy::cast_possible_truncation)]
    let days = days_f.trunc() as i64;
    let secs_f =
        days_f.fract() * 86_400.0 + f("hours")? * 3600.0 + f("minutes")? * 60.0 + f("seconds")?;
    #[allow(clippy::cast_possible_truncation)]
    let secs = secs_f.trunc() as i64;
    #[allow(clippy::cast_possible_truncation)]
    let nanos = (secs_f.fract() * 1e9).round() as i64
        + int_of(m, "milliseconds", 0)? * 1_000_000
        + int_of(m, "microseconds", 0)? * 1000
        + int_of(m, "nanoseconds", 0)?;
    Ok(Value::Duration(Dur::norm(months, days, secs, nanos)))
}

fn point_from_map(m: &BTreeMap<String, Value>) -> Result<Value> {
    let crs = match m.get("crs") {
        Some(Value::String(s)) => Some(s.to_ascii_lowercase()),
        _ => None,
    };
    let srid = match m.get("srid") {
        Some(Value::Int(i)) => Some(u32::try_from(*i).map_err(|_| bad("bad srid"))?),
        _ => None,
    };
    let geo = m.contains_key("latitude") || m.contains_key("longitude");
    let (x, y, z) = if geo {
        (
            num_of(m, "longitude")?,
            num_of(m, "latitude")?,
            num_of(m, "height")?,
        )
    } else {
        (num_of(m, "x")?, num_of(m, "y")?, num_of(m, "z")?)
    };
    let (Some(x), Some(y)) = (x, y) else {
        return Err(bad("A point needs x and y, or longitude and latitude"));
    };
    let srid = srid.unwrap_or(match crs.as_deref() {
        Some("wgs-84") => 4326,
        Some("wgs-84-3d") => 4979,
        Some("cartesian-3d") => 9157,
        Some("cartesian") => 7203,
        _ => match (geo, z.is_some()) {
            (true, false) => 4326,
            (true, true) => 4979,
            (false, true) => 9157,
            (false, false) => 7203,
        },
    });
    if !matches!(srid, 7203 | 9157 | 4326 | 4979) {
        return Err(bad(format!(
            "Unknown coordinate reference system: srid {srid}"
        )));
    }
    let three = matches!(srid, 9157 | 4979);
    if three != z.is_some() {
        return Err(bad("the point's dimension doesn't match its CRS"));
    }
    Ok(Value::Point(Point { srid, x, y, z }))
}

// ----------------------------------------------------------------------
// Accessors, functions, arithmetic, comparison.
// ----------------------------------------------------------------------

/// A temporal value's date, time, and offset with its zone name.
type Parts<'a> = (
    Option<NaiveDate>,
    Option<NaiveTime>,
    Option<(i32, Option<&'a String>)>,
);

/// `v.key` of a temporal value, duration or point; `None` when `v` isn't
/// one.
///
/// # Errors
/// 39909 for a key the type doesn't have.
#[allow(clippy::too_many_lines)]
pub fn property(v: &Value, key: &str) -> Result<Option<Value>> {
    let (date, time, zone): Parts<'_> = match v {
        Value::Date(d) => (Some(*d), None, None),
        Value::LocalTime(t) => (None, Some(*t), None),
        Value::Time(t, o) => (None, Some(*t), Some((*o, None))),
        Value::LocalDateTime(dt) => (Some(dt.date()), Some(dt.time()), None),
        Value::DateTime(z) => (
            Some(z.local.date()),
            Some(z.local.time()),
            Some((z.offset, z.zone.as_ref())),
        ),
        Value::Duration(d) => return dur_property(d, key).map(Some),
        Value::Point(p) => return point_property(p, key).map(Some),
        _ => return Ok(None),
    };
    let i = |x: i64| Ok(Some(Value::Int(x)));
    if let Some(d) = date {
        match key {
            "year" => return i(i64::from(d.year())),
            "quarter" => return i(i64::from((d.month() - 1) / 3 + 1)),
            "month" => return i(i64::from(d.month())),
            "week" => return i(i64::from(d.iso_week().week())),
            "weekYear" => return i(i64::from(d.iso_week().year())),
            "day" => return i(i64::from(d.day())),
            "ordinalDay" => return i(i64::from(d.ordinal())),
            "dayOfWeek" | "weekDay" => {
                return i(i64::from(d.weekday().number_from_monday()));
            }
            "dayOfQuarter" => {
                let first =
                    NaiveDate::from_ymd_opt(d.year(), (d.month() - 1) / 3 * 3 + 1, 1).unwrap_or(d);
                return i((d - first).num_days() + 1);
            }
            _ => {}
        }
    }
    if let Some(t) = time {
        match key {
            "hour" => return i(i64::from(t.hour())),
            "minute" => return i(i64::from(t.minute())),
            "second" => return i(i64::from(t.second())),
            "millisecond" => return i(i64::from(t.nanosecond() / 1_000_000)),
            "microsecond" => return i(i64::from(t.nanosecond() / 1000)),
            "nanosecond" => return i(i64::from(t.nanosecond())),
            _ => {}
        }
    }
    if let Some((off, name)) = zone {
        match key {
            "timezone" => {
                return Ok(Some(Value::String(
                    name.cloned().unwrap_or_else(|| offset_text(off)),
                )));
            }
            "offset" => return Ok(Some(Value::String(offset_text(off)))),
            "offsetMinutes" => return i(i64::from(off / 60)),
            "offsetSeconds" => return i(i64::from(off)),
            _ => {}
        }
    }
    if let Value::DateTime(z) = v {
        let (s, n) = z.epoch();
        match key {
            "epochSeconds" => return i(s),
            "epochMillis" => return i(s * 1000 + i64::from(n / 1_000_000)),
            _ => {}
        }
    }
    Err(bad(format!("No such field: {key} on a {}", v.type_name())))
}

fn dur_property(d: &Dur, key: &str) -> Result<Value> {
    let total_nanos = i128::from(d.secs) * i128::from(NANOS) + i128::from(d.nanos);
    let tn = |div: i128| Value::Int(i64::try_from(total_nanos / div).unwrap_or(i64::MAX));
    Ok(match key {
        "years" => Value::Int(d.months / 12),
        "quarters" => Value::Int(d.months / 3),
        "months" => Value::Int(d.months),
        "weeks" => Value::Int(d.days / 7),
        "days" => Value::Int(d.days),
        "hours" => Value::Int(d.secs / 3600),
        "minutes" => Value::Int(d.secs / 60),
        "seconds" => Value::Int(d.secs),
        "milliseconds" => tn(1_000_000),
        "microseconds" => tn(1000),
        "nanoseconds" => tn(1),
        "quartersOfYear" => Value::Int((d.months / 3) % 4),
        "monthsOfQuarter" => Value::Int(d.months % 3),
        "monthsOfYear" => Value::Int(d.months % 12),
        "daysOfWeek" => Value::Int(d.days % 7),
        "minutesOfHour" => Value::Int((d.secs / 60) % 60),
        "secondsOfMinute" => Value::Int(d.secs % 60),
        "millisecondsOfSecond" => Value::Int(d.nanos / 1_000_000),
        "microsecondsOfSecond" => Value::Int(d.nanos / 1000),
        "nanosecondsOfSecond" => Value::Int(d.nanos),
        _ => return Err(bad(format!("No such field: {key} on a DURATION"))),
    })
}

fn point_property(p: &Point, key: &str) -> Result<Value> {
    Ok(match key {
        "x" => Value::Float(p.x),
        "y" => Value::Float(p.y),
        "z" => p.z.map_or(Value::Null, Value::Float),
        "longitude" if p.geographic() => Value::Float(p.x),
        "latitude" if p.geographic() => Value::Float(p.y),
        "height" if p.geographic() => p.z.map_or(Value::Null, Value::Float),
        "crs" => Value::str(p.crs()),
        "srid" => Value::Int(i64::from(p.srid)),
        _ => return Err(bad(format!("No such field: {key} on a POINT"))),
    })
}

/// `point.distance(a, b)`: `null` for points of different CRSs.
#[must_use]
pub fn distance(a: &Point, b: &Point) -> Option<f64> {
    if a.srid != b.srid {
        return None;
    }
    if a.geographic() {
        // Haversine on Neo4j's sphere.
        const R: f64 = 6_378_140.0;
        let (la1, la2) = (a.y.to_radians(), b.y.to_radians());
        let dla = la2 - la1;
        let dlo = b.x.to_radians() - a.x.to_radians();
        let h = (dla / 2.0).sin().powi(2) + la1.cos() * la2.cos() * (dlo / 2.0).sin().powi(2);
        let d = 2.0 * R * h.sqrt().atan2((1.0 - h).sqrt());
        return Some(match (a.z, b.z) {
            (Some(z1), Some(z2)) => (d * d + (z2 - z1).powi(2)).sqrt(),
            _ => d,
        });
    }
    let dz = match (a.z, b.z) {
        (Some(z1), Some(z2)) => z2 - z1,
        _ => 0.0,
    };
    Some(((b.x - a.x).powi(2) + (b.y - a.y).powi(2) + dz * dz).sqrt())
}

/// The spatial and temporal functions: `None` when `name` isn't one.
///
/// # Errors
/// 39909 for bad arguments.
pub fn call(name: &str, args: &[Value]) -> Result<Option<Value>> {
    if let Some(v) = construct(name, args)? {
        return Ok(Some(v));
    }
    let a = args.first().unwrap_or(&Value::Null);
    let b = args.get(1).unwrap_or(&Value::Null);
    Ok(Some(match name {
        "point.distance" | "distance" => match (a, b) {
            (Value::Point(p), Value::Point(q)) => distance(p, q).map_or(Value::Null, Value::Float),
            (Value::Null, _) | (_, Value::Null) => Value::Null,
            _ => return Err(bad("point.distance() takes two points")),
        },
        "point.withinbbox" => match (a, b, args.get(2).unwrap_or(&Value::Null)) {
            (Value::Point(p), Value::Point(lo), Value::Point(hi)) => {
                if p.srid != lo.srid || p.srid != hi.srid {
                    Value::Null
                } else {
                    Value::Bool(p.x >= lo.x && p.x <= hi.x && p.y >= lo.y && p.y <= hi.y)
                }
            }
            _ => Value::Null,
        },
        "duration.between" | "duration.inmonths" | "duration.indays" | "duration.inseconds" => {
            if a.is_null() || b.is_null() {
                return Ok(Some(Value::Null));
            }
            Value::Duration(between(name, a, b)?)
        }
        _ => return Ok(None),
    }))
}

/// The local date, time and instant of a temporal value.
fn parts(v: &Value) -> Option<(Option<NaiveDate>, Option<NaiveTime>, Option<i32>)> {
    Some(match v {
        Value::Date(d) => (Some(*d), None, None),
        Value::LocalTime(t) => (None, Some(*t), None),
        Value::Time(t, o) => (None, Some(*t), Some(*o)),
        Value::LocalDateTime(dt) => (Some(dt.date()), Some(dt.time()), None),
        Value::DateTime(z) => (Some(z.local.date()), Some(z.local.time()), Some(z.offset)),
        _ => return None,
    })
}

fn between(name: &str, a: &Value, b: &Value) -> Result<Dur> {
    let (Some((da, ta, oa)), Some((db, tb, ob))) = (parts(a), parts(b)) else {
        return Err(bad(format!("{name}() takes two temporal values")));
    };
    // Compare in one zone: b's wall clock moved to a's offset.
    let shift = match (oa, ob) {
        (Some(x), Some(y)) => i64::from(x - y),
        _ => 0,
    };
    let t0 = ta.unwrap_or(NaiveTime::MIN);
    let t1 = tb.unwrap_or(NaiveTime::MIN);
    if let (Some(d0), Some(d1)) = (da, db) {
        {
            let start = d0.and_time(t0);
            let end = d1.and_time(t1) + ChronoDur::seconds(shift);
            let total = end - start;
            Ok(match name {
                "duration.inseconds" => nanos_dur(total),
                "duration.indays" => Dur::norm(0, total.num_days(), 0, 0),
                _ => {
                    let mut months = i64::from(end.year() - start.year()) * 12
                        + i64::from(end.month())
                        - i64::from(start.month());
                    let mut probe = add_months(start, months);
                    if (months > 0 && probe > end) || (months < 0 && probe < end) {
                        months -= months.signum();
                        probe = add_months(start, months);
                    }
                    if name == "duration.inmonths" {
                        return Ok(Dur::norm(months, 0, 0, 0));
                    }
                    let rest = end - probe;
                    let days = rest.num_days();
                    let rem = rest - ChronoDur::days(days);
                    let r = nanos_dur(rem);
                    Dur::norm(months, days, r.secs, r.nanos)
                }
            })
        }
    } else {
        {
            let total = t1.signed_duration_since(t0) + ChronoDur::seconds(shift);
            Ok(match name {
                "duration.inmonths" | "duration.indays" => Dur::default(),
                _ => nanos_dur(total),
            })
        }
    }
}

fn nanos_dur(d: ChronoDur) -> Dur {
    let n = d.num_nanoseconds().unwrap_or(i64::MAX);
    Dur::norm(0, 0, n.div_euclid(NANOS), n.rem_euclid(NANOS))
}

fn add_months(dt: NaiveDateTime, months: i64) -> NaiveDateTime {
    let m = u32::try_from(months.unsigned_abs()).unwrap_or(u32::MAX);
    if months >= 0 {
        dt.checked_add_months(Months::new(m)).unwrap_or(dt)
    } else {
        dt.checked_sub_months(Months::new(m)).unwrap_or(dt)
    }
}

fn shift_dt(dt: NaiveDateTime, d: &Dur, sign: i64) -> NaiveDateTime {
    let dt = add_months(dt, sign * d.months);
    dt + ChronoDur::days(sign * d.days)
        + ChronoDur::seconds(sign * d.secs)
        + ChronoDur::nanoseconds(sign * d.nanos)
}

/// `a + b` and `a - b` (`sign` -1) with a temporal value or a duration;
/// `None` when neither is temporal.
///
/// # Errors
/// 39909 for combinations Cypher refuses.
pub fn add(a: &Value, b: &Value, sign: i64) -> Result<Option<Value>> {
    let (t, d) = match (a, b) {
        (Value::Duration(x), Value::Duration(y)) => {
            return Ok(Some(Value::Duration(Dur::norm(
                x.months + sign * y.months,
                x.days + sign * y.days,
                x.secs + sign * y.secs,
                x.nanos + sign * y.nanos,
            ))));
        }
        (t, Value::Duration(d)) => (t, d),
        (Value::Duration(d), t) if sign > 0 => (t, d),
        _ => return Ok(None),
    };
    Ok(Some(match t {
        Value::Date(x) => {
            let dt = shift_dt(
                x.and_time(NaiveTime::MIN),
                &Dur {
                    secs: d.secs - d.secs % 86_400,
                    nanos: 0,
                    ..*d
                },
                sign,
            );
            Value::Date(dt.date())
        }
        Value::LocalTime(x) => Value::LocalTime(shift_time(*x, d, sign)),
        Value::Time(x, o) => Value::Time(shift_time(*x, d, sign), *o),
        Value::LocalDateTime(x) => Value::LocalDateTime(shift_dt(*x, d, sign)),
        Value::DateTime(z) => {
            let local = shift_dt(z.local, d, sign);
            let offset = match &z.zone {
                Some(name) => zone_offset(name, local)?,
                None => z.offset,
            };
            Value::DateTime(Box::new(Zoned {
                local,
                offset,
                zone: z.zone.clone(),
            }))
        }
        _ => return Ok(None),
    }))
}

fn shift_time(t: NaiveTime, d: &Dur, sign: i64) -> NaiveTime {
    let (t, _) = t.overflowing_add_signed(
        ChronoDur::seconds(sign * d.secs) + ChronoDur::nanoseconds(sign * d.nanos),
    );
    t
}

/// `d * n` (and `d / n` as `d * (1 / n)`).
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
pub fn scale(d: &Dur, n: f64) -> Dur {
    let months = d.months as f64 * n;
    let days = d.days as f64 * n + months.fract() * 30.436_875;
    let secs = d.secs as f64 * n + days.fract() * 86_400.0;
    let nanos = d.nanos as f64 * n + secs.fract() * 1e9;
    Dur::norm(
        months.trunc() as i64,
        days.trunc() as i64,
        secs.trunc() as i64,
        nanos.round() as i64,
    )
}

/// Cypher `<` between two temporal values of one type; `None` otherwise
/// (durations aren't ordered).
#[must_use]
pub fn compare(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Date(x), Value::Date(y)) => Some(x.cmp(y)),
        (Value::LocalTime(x), Value::LocalTime(y)) => Some(x.cmp(y)),
        (Value::Time(x, o), Value::Time(y, p)) => {
            let ux = i64::from(x.num_seconds_from_midnight()) - i64::from(*o);
            let uy = i64::from(y.num_seconds_from_midnight()) - i64::from(*p);
            Some(ux.cmp(&uy).then(x.nanosecond().cmp(&y.nanosecond())))
        }
        (Value::LocalDateTime(x), Value::LocalDateTime(y)) => Some(x.cmp(y)),
        (Value::DateTime(x), Value::DateTime(y)) => Some(x.epoch().cmp(&y.epoch())),
        _ => None,
    }
}

// ----------------------------------------------------------------------
// Storage.
// ----------------------------------------------------------------------

/// The `_types` name and stored JSON of a temporal or point value.
#[must_use]
pub fn to_stored(v: &Value) -> Option<(&'static str, serde_json::Value)> {
    let kind = match v {
        Value::Date(_) => "date",
        Value::LocalTime(_) => "localtime",
        Value::Time(..) => "time",
        Value::LocalDateTime(_) => "localdatetime",
        Value::DateTime(_) => "datetime",
        Value::Duration(_) => "duration",
        Value::Bytes(b) => {
            use base64::Engine as _;
            return Some((
                "bytes",
                serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(b)),
            ));
        }
        Value::Point(p) => {
            let mut m = serde_json::Map::new();
            m.insert("srid".into(), serde_json::Value::from(p.srid));
            m.insert("x".into(), serde_json::json!(p.x));
            m.insert("y".into(), serde_json::json!(p.y));
            if let Some(z) = p.z {
                m.insert("z".into(), serde_json::json!(z));
            }
            return Some(("point", serde_json::Value::Object(m)));
        }
        _ => return None,
    };
    Some((kind, serde_json::Value::String(text(v)?)))
}

/// A stored value of `_types` kind `kind`; `None` for other kinds.
#[must_use]
pub fn from_stored(kind: &str, j: &serde_json::Value) -> Option<Value> {
    match (kind, j) {
        ("point", serde_json::Value::Object(o)) => {
            let f = |k: &str| o.get(k).and_then(serde_json::Value::as_f64);
            Some(Value::Point(Point {
                srid: u32::try_from(o.get("srid")?.as_u64()?).ok()?,
                x: f("x")?,
                y: f("y")?,
                z: f("z"),
            }))
        }
        (
            "date" | "localtime" | "time" | "localdatetime" | "datetime" | "duration",
            serde_json::Value::String(s),
        ) => parse(kind, s).ok(),
        ("bytes", serde_json::Value::String(s)) => {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD
                .decode(s)
                .ok()
                .map(Value::Bytes)
        }
        _ => None,
    }
}

/// A `DateTime` from epoch seconds, nanoseconds and an offset (Bolt).
#[must_use]
pub fn datetime_from_epoch(
    secs: i64,
    nanos: u32,
    offset: i32,
    zone: Option<String>,
) -> Option<Zoned> {
    let utc = DateTime::from_timestamp(secs, nanos)?.naive_utc();
    Some(Zoned {
        local: utc + ChronoDur::seconds(i64::from(offset)),
        offset,
        zone,
    })
}

/// A zone's offset at an instant (Bolt's `DateTimeZoneId`).
///
/// # Errors
/// 39909 for an unknown zone.
pub fn zone_offset_at(name: &str, secs: i64) -> Result<i32> {
    if let Some(o) = fixed_offset(name) {
        return Ok(o);
    }
    let tz: chrono_tz::Tz = name
        .parse()
        .map_err(|_| bad(format!("Invalid value for TimeZone: Text '{name}'")))?;
    let utc = DateTime::from_timestamp(secs, 0)
        .ok_or_else(|| bad("instant out of range"))?
        .naive_utc();
    Ok(tz.from_utc_datetime(&utc).offset().fix().local_minus_utc())
}

/// The epoch day of a date and the like, for Bolt.
#[must_use]
pub fn epoch_day(d: NaiveDate) -> i64 {
    (d - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or(d)).num_days()
}

/// A date from an epoch day (Bolt).
#[must_use]
pub fn from_epoch_day(days: i64) -> Option<NaiveDate> {
    NaiveDate::from_ymd_opt(1970, 1, 1)?.checked_add_signed(ChronoDur::days(days))
}

/// A wall clock as seconds and nanoseconds as if it were UTC (Bolt's
/// `LocalDateTime`).
#[must_use]
pub fn local_epoch(dt: NaiveDateTime) -> (i64, u32) {
    (dt.and_utc().timestamp(), dt.nanosecond())
}

/// A wall clock from seconds and nanoseconds as if UTC.
#[must_use]
pub fn local_from_epoch(secs: i64, nanos: u32) -> Option<NaiveDateTime> {
    Some(DateTime::from_timestamp(secs, nanos)?.naive_utc())
}

/// A time of day from nanoseconds since midnight.
#[must_use]
pub fn time_from_nanos(n: i64) -> Option<NaiveTime> {
    let secs = u32::try_from(n.div_euclid(NANOS)).ok()?;
    let nanos = u32::try_from(n.rem_euclid(NANOS)).ok()?;
    NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
}

/// Nanoseconds since midnight.
#[must_use]
pub fn nanos_of_day(t: NaiveTime) -> i64 {
    i64::from(t.num_seconds_from_midnight()) * NANOS + i64::from(t.nanosecond())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(kind: &str, s: &str) -> Value {
        parse(kind, s).unwrap_or_else(|e| panic!("{kind} {s}: {e}"))
    }

    #[test]
    fn texts_match_neo4j() {
        assert_eq!(text(&p("date", "2020-01-05")).unwrap(), "2020-01-05");
        assert_eq!(text(&p("localtime", "12:00")).unwrap(), "12:00:00");
        assert_eq!(
            text(&p("time", "12:30:15.5+02:00")).unwrap(),
            "12:30:15.5+02:00"
        );
        assert_eq!(
            text(&p("datetime", "2020-01-05T10:00:00Z")).unwrap(),
            "2020-01-05T10:00:00Z"
        );
        assert_eq!(
            text(&p("datetime", "2020-01-05T10:00:00[Europe/Berlin]")).unwrap(),
            "2020-01-05T10:00:00+01:00[Europe/Berlin]"
        );
        assert_eq!(
            text(&p("localdatetime", "2020-01-05T10:00")).unwrap(),
            "2020-01-05T10:00:00"
        );
        assert_eq!(
            text(&p("duration", "P1Y2M3DT4H5M6.5S")).unwrap(),
            "P1Y2M3DT4H5M6.5S"
        );
        let m = BTreeMap::from([
            ("days".to_owned(), Value::Int(1)),
            ("hours".to_owned(), Value::Int(25)),
        ]);
        assert_eq!(text(&duration_from_map(&m).unwrap()).unwrap(), "P1DT25H");
    }

    #[test]
    fn bolt_durations_are_normalized() {
        let d = Dur::from_fields(0, 0, -9_223_372_036, -854_775_808).expect("fits");
        assert_eq!((d.secs, d.nanos), (-9_223_372_037, 145_224_192));
        let d = Dur::from_fields(3, 4, 999, 123_456_789).expect("fits");
        assert_eq!(
            (d.months, d.days, d.secs, d.nanos),
            (3, 4, 999, 123_456_789)
        );
        assert!(Dur::from_fields(0, 0, i64::MIN, -1).is_none());
    }

    #[test]
    fn utc_is_a_named_zone() {
        // Neo4j keeps `[UTC]` (the Go driver's testkit run, R8); `Z` and
        // offsets stay offsets.
        let Value::DateTime(z) = p("datetime", "1976-06-13T12:34:56[UTC]") else {
            panic!("datetime");
        };
        assert_eq!((z.offset, z.zone.as_deref()), (0, Some("UTC")));
        assert_eq!(
            text(&Value::DateTime(z)).unwrap(),
            "1976-06-13T12:34:56Z[UTC]"
        );
        for s in ["1976-06-13T12:34:56Z", "1976-06-13T12:34:56+01:00"] {
            let Value::DateTime(z) = p("datetime", s) else {
                panic!("datetime");
            };
            assert_eq!(z.zone, None, "{s}");
        }
    }

    #[test]
    fn arithmetic_and_between() {
        let d = add(&p("date", "2020-01-31"), &p("duration", "P1M"), 1)
            .unwrap()
            .unwrap();
        assert_eq!(text(&d).unwrap(), "2020-02-29");
        let b = between(
            "duration.between",
            &p("date", "2020-01-01"),
            &p("date", "2021-03-04"),
        )
        .unwrap();
        assert_eq!(dur_text(&b), "P1Y2M3D");
        assert_eq!(
            property(&p("datetime", "2020-01-05T10:00:00+01:00"), "epochSeconds").unwrap(),
            Some(Value::Int(1_578_214_800))
        );
        assert_eq!(
            property(&p("date", "2020-02-29"), "week").unwrap(),
            Some(Value::Int(9))
        );
        assert_eq!(
            property(&p("duration", "PT90M"), "minutes").unwrap(),
            Some(Value::Int(90))
        );
    }

    #[test]
    fn points_and_distance() {
        let a = Point {
            srid: 4326,
            x: 12.5,
            y: 55.6,
            z: None,
        };
        let b = Point {
            srid: 4326,
            x: 13.0,
            y: 56.0,
            z: None,
        };
        let d = distance(&a, &b).unwrap();
        assert!((d - 54_419.396_771_531_865).abs() < 1e-6, "{d}");
        assert_eq!(
            text(&Value::Point(Point {
                srid: 7203,
                x: 1.0,
                y: 2.0,
                z: None
            }))
            .unwrap(),
            "point({x: 1.0, y: 2.0, crs: 'cartesian'})"
        );
    }
}
