//! Date expressions (M1): `$dateAdd`, `$dateSubtract`, `$dateDiff`,
//! `$dateTrunc`, `$dateFromParts`, `$dateFromString`, `$dateToParts`,
//! `$dateToString`, `$week`, `$isoWeek`, `$isoWeekYear`,
//! `$isoDayOfWeek`, `$toDate`, and the older part extractors (`$year` …
//! `$millisecond`) with time zones: Olson names (`America/New_York`) and
//! UTC offsets (`+05:30`, `-0300`, `+05`), as MongoDB accepts them.

use bson::{Bson, DateTime, Document, doc};
use chrono::{LocalResult, Offset, TimeZone};
use sprig_proto_mongo::errors::MongoError;

use super::expr_common::{err, integral_i64, is_nullish, show, tname};
use super::{Vars, eval};

const DAY: i64 = 86_400_000;

/// A time zone: a fixed offset (seconds) or a named zone.
#[derive(Clone, Copy, Debug)]
pub(super) enum Zone {
    Fixed(i64),
    Named(chrono_tz::Tz),
}

fn parse_offset(s: &str) -> Option<i64> {
    let (sign, rest) = match s.as_bytes().first()? {
        b'+' => (1, &s[1..]),
        b'-' => (-1, &s[1..]),
        _ => return None,
    };
    let digits: String = rest.chars().filter(|c| *c != ':').collect();
    if !digits.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let colon = rest.contains(':');
    let (h, m) = match (digits.len(), colon) {
        (2, false) => (digits.parse::<i64>().ok()?, 0),
        (4, _) => (
            digits[..2].parse::<i64>().ok()?,
            digits[2..].parse::<i64>().ok()?,
        ),
        _ => return None,
    };
    if colon && rest.find(':') != Some(2) {
        return None;
    }
    Some(sign * (h * 3600 + m * 60))
}

impl Zone {
    pub(super) const UTC: Zone = Zone::Fixed(0);

    /// Parses a `timezone` argument.
    pub(super) fn parse(s: &str) -> Result<Zone, MongoError> {
        if let Some(o) = parse_offset(s) {
            return Ok(Zone::Fixed(o));
        }
        if s == "UTC" || s == "GMT" || s == "Z" {
            return Ok(Zone::UTC);
        }
        s.parse::<chrono_tz::Tz>()
            .map(Zone::Named)
            .map_err(|_| err(40485, format!("unrecognized time zone identifier: \"{s}\"")))
    }

    /// The zone's offset from UTC (ms) at an instant.
    pub(super) fn offset_at(self, utc_ms: i64) -> i64 {
        match self {
            Zone::Fixed(s) => s * 1000,
            Zone::Named(tz) => chrono::DateTime::from_timestamp_millis(utc_ms).map_or(0, |d| {
                i64::from(
                    tz.offset_from_utc_datetime(&d.naive_utc())
                        .fix()
                        .local_minus_utc(),
                ) * 1000
            }),
        }
    }

    /// Local wall-clock milliseconds of an instant.
    pub(super) fn local(self, utc_ms: i64) -> i64 {
        utc_ms + self.offset_at(utc_ms)
    }

    /// The instant of a local wall-clock time: the earlier one when the
    /// time happens twice, the offset before the change when it doesn't
    /// happen at all (as MongoDB's timelib does).
    pub(super) fn utc(self, local_ms: i64) -> i64 {
        match self {
            Zone::Fixed(s) => local_ms - s * 1000,
            Zone::Named(tz) => {
                let Some(naive) =
                    chrono::DateTime::from_timestamp_millis(local_ms).map(|d| d.naive_utc())
                else {
                    return local_ms;
                };
                match tz.offset_from_local_datetime(&naive) {
                    LocalResult::Single(o) | LocalResult::Ambiguous(o, _) => {
                        local_ms - i64::from(o.fix().local_minus_utc()) * 1000
                    }
                    LocalResult::None => {
                        let before = naive - chrono::Duration::days(1);
                        let o = tz.offset_from_utc_datetime(&before).fix().local_minus_utc();
                        local_ms - i64::from(o) * 1000
                    }
                }
            }
        }
    }
}

/// Howard Hinnant's days from civil date.
pub(super) fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Civil date from days since the epoch.
pub(super) fn civil_from_days(z: i64) -> (i64, i64, i64) {
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

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
    }
}

/// The fields of a local time.
#[derive(Debug, Clone, Copy)]
pub(super) struct Parts {
    pub year: i64,
    pub month: i64,
    pub day: i64,
    pub hour: i64,
    pub minute: i64,
    pub second: i64,
    pub ms: i64,
    /// 1 = Sunday … 7 = Saturday.
    pub dow: i64,
    /// 1-based.
    pub yday: i64,
    pub iso_year: i64,
    pub iso_week: i64,
    /// 1 = Monday … 7 = Sunday.
    pub iso_dow: i64,
}

pub(super) fn parts(local_ms: i64) -> Parts {
    let days = local_ms.div_euclid(DAY);
    let rem = local_ms.rem_euclid(DAY);
    let (year, month, day) = civil_from_days(days);
    let iso_dow = (days + 3).rem_euclid(7) + 1;
    let thursday = days - iso_dow + 4;
    let (iso_year, _, _) = civil_from_days(thursday);
    let iso_week = (thursday - days_from_civil(iso_year, 1, 1)) / 7 + 1;
    Parts {
        year,
        month,
        day,
        hour: rem / 3_600_000,
        minute: rem % 3_600_000 / 60_000,
        second: rem % 60_000 / 1000,
        ms: rem % 1000,
        dow: (days + 4).rem_euclid(7) + 1,
        yday: days - days_from_civil(year, 1, 1) + 1,
        iso_year,
        iso_week,
        iso_dow,
    }
}

/// A value as a date's milliseconds: dates, ObjectIds and timestamps.
pub(super) fn date_ms(v: &Bson) -> Option<i64> {
    match v {
        Bson::DateTime(d) => Some(d.timestamp_millis()),
        Bson::ObjectId(o) => Some(o.timestamp().timestamp_millis() / 1000 * 1000),
        Bson::Timestamp(t) => Some(i64::from(t.time) * 1000),
        _ => None,
    }
}

fn i32b(v: i64) -> Bson {
    Bson::Int32(i32::try_from(v).unwrap_or(i32::MAX))
}

/// Evaluates a `timezone` argument: `Ok(None)` when it is null.
fn zone_arg(op: &str, e: Option<&Bson>, vars: &Vars) -> Result<Option<Zone>, MongoError> {
    let Some(e) = e else {
        return Ok(Some(Zone::UTC));
    };
    match eval(e, vars)? {
        Bson::String(s) => Zone::parse(&s).map(Some),
        v if is_nullish(&v) => Ok(None),
        v => Err(err(
            40533,
            format!(
                "{op} requires a string for the timezone argument, but was given a {} ({})",
                tname(&v),
                show(&v)
            ),
        )),
    }
}

fn part_value(op: &str, p: &Parts) -> Bson {
    match op {
        "$year" => i32b(p.year),
        "$month" => i32b(p.month),
        "$dayOfMonth" => i32b(p.day),
        "$hour" => i32b(p.hour),
        "$minute" => i32b(p.minute),
        "$second" => i32b(p.second),
        "$millisecond" => i32b(p.ms),
        "$dayOfWeek" => i32b(p.dow),
        "$dayOfYear" => i32b(p.yday),
        "$week" => i32b((p.yday - 1 + 7 - (p.dow - 1)) / 7),
        "$isoWeek" => i32b(p.iso_week),
        "$isoWeekYear" => Bson::Int64(p.iso_year),
        _ => i32b(p.iso_dow),
    }
}

/// `$year` … `$isoDayOfWeek`: a date expression, a one-element array of
/// one, or `{date, timezone}`.
fn date_part(op: &str, arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let (date_e, tz_e): (Bson, Option<&Bson>) = match arg {
        Bson::Document(d) if d.keys().next().is_some_and(|k| !k.starts_with('$')) => {
            for k in d.keys() {
                if k != "date" && k != "timezone" {
                    return Err(err(40535, format!("unrecognized option to {op}: \"{k}\"")));
                }
            }
            let Some(date) = d.get("date") else {
                return Err(err(
                    40539,
                    format!("missing 'date' argument to {op}, provided: {op}: {d}"),
                ));
            };
            (date.clone(), d.get("timezone"))
        }
        Bson::Array(a) => {
            if a.len() != 1 {
                return Err(err(
                    40536,
                    format!(
                        "{op} accepts exactly one argument if given an array, but was given {}",
                        a.len()
                    ),
                ));
            }
            (a[0].clone(), None)
        }
        other => (other.clone(), None),
    };
    let v = eval(&date_e, vars)?;
    let zone = zone_arg(op, tz_e, vars)?;
    if is_nullish(&v) {
        return Ok(Bson::Null);
    }
    let Some(ms) = date_ms(&v) else {
        return Err(err(
            16006,
            format!("can't convert from BSON type {} to Date", tname(&v)),
        ));
    };
    let Some(zone) = zone else {
        return Ok(Bson::Null);
    };
    Ok(part_value(op, &parts(zone.local(ms))))
}

fn options<'a>(
    op: &str,
    arg: &'a Bson,
    allowed: &[&str],
    not_obj: i32,
    unknown: (i32, &str),
) -> Result<&'a Document, MongoError> {
    let Bson::Document(d) = arg else {
        return Err(err(
            not_obj,
            format!("{op} only supports an object as its argument"),
        ));
    };
    for k in d.keys() {
        if !allowed.contains(&k.as_str()) {
            return Err(err(
                unknown.0,
                format!(
                    "{}{k}{}",
                    unknown.1,
                    if unknown.0 == 5_166_401 {
                        ". Expected arguments are startDate, unit, amount, and optionally timezone."
                    } else if unknown.0 == 5_439_008 {
                        ". Expected arguments are date, unit, and optionally, binSize, timezone, startOfWeek"
                    } else {
                        ""
                    }
                ),
            ));
        }
    }
    Ok(d)
}

fn to_parts(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let op = "$dateToParts";
    let d = options(
        op,
        arg,
        &["date", "timezone", "iso8601"],
        40524,
        (40520, "Unrecognized argument to $dateToParts: "),
    )?;
    let Some(date) = d.get("date") else {
        return Err(err(40522, "Missing 'date' parameter to $dateToParts"));
    };
    let v = eval(date, vars)?;
    let zone = zone_arg(op, d.get("timezone"), vars)?;
    let iso = match d.get("iso8601") {
        None => false,
        Some(e) => match eval(e, vars)? {
            Bson::Boolean(b) => b,
            x if is_nullish(&x) => return Ok(Bson::Null),
            x => {
                return Err(err(
                    40521,
                    format!("iso8601 must evaluate to a bool, found {}", tname(&x)),
                ));
            }
        },
    };
    if is_nullish(&v) {
        return Ok(Bson::Null);
    }
    let Some(ms) = date_ms(&v) else {
        return Err(err(
            16006,
            format!("can't convert from BSON type {} to Date", tname(&v)),
        ));
    };
    let Some(zone) = zone else {
        return Ok(Bson::Null);
    };
    let p = parts(zone.local(ms));
    Ok(Bson::Document(if iso {
        doc! {
            "isoWeekYear": i32b(p.iso_year), "isoWeek": i32b(p.iso_week),
            "isoDayOfWeek": i32b(p.iso_dow), "hour": i32b(p.hour), "minute": i32b(p.minute),
            "second": i32b(p.second), "millisecond": i32b(p.ms),
        }
    } else {
        doc! {
            "year": i32b(p.year), "month": i32b(p.month), "day": i32b(p.day),
            "hour": i32b(p.hour), "minute": i32b(p.minute), "second": i32b(p.second),
            "millisecond": i32b(p.ms),
        }
    }))
}

fn from_parts(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    const NAT: [&str; 7] = [
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
        "millisecond",
    ];
    const ISO: [&str; 3] = ["isoWeekYear", "isoWeek", "isoDayOfWeek"];
    let op = "$dateFromParts";
    let d = options(
        op,
        arg,
        &[
            "year",
            "month",
            "day",
            "hour",
            "minute",
            "second",
            "millisecond",
            "isoWeekYear",
            "isoWeek",
            "isoDayOfWeek",
            "timezone",
        ],
        40519,
        (40518, "Unrecognized argument to $dateFromParts: "),
    )?;
    let natural = d.contains_key("year");
    let iso = d.contains_key("isoWeekYear");
    if !natural && !iso {
        return Err(err(
            40516,
            "$dateFromParts requires either 'year' or 'isoWeekYear' to be present",
        ));
    }
    let mixes = if natural {
        ISO.iter().any(|k| d.contains_key(*k))
    } else {
        ["month", "day"].iter().any(|k| d.contains_key(*k))
    };
    if mixes {
        return Err(err(
            40489,
            "$dateFromParts does not allow mixing natural dates with ISO dates",
        ));
    }
    let mut found: Vec<(&str, i64)> = Vec::new();
    let mut null = false;
    let fields: Vec<&str> = if natural {
        NAT.to_vec()
    } else {
        let mut f = ISO.to_vec();
        f.extend(["hour", "minute", "second", "millisecond"]);
        f
    };
    for f in fields {
        let Some(e) = d.get(f) else { continue };
        let v = eval(e, vars)?;
        if is_nullish(&v) {
            null = true;
            continue;
        }
        let Some(n) = integral_i64(&v) else {
            return Err(err(
                40515,
                format!(
                    "'{f}' must evaluate to an integer, found {} with value {}",
                    tname(&v),
                    show(&v)
                ),
            ));
        };
        if f == "year" || f == "isoWeekYear" {
            if !(1..=9999).contains(&n) {
                return Err(err(
                    if f == "year" { 40523 } else { 31095 },
                    format!("'{f}' must evaluate to an integer in the range 1 to 9999, found {n}"),
                ));
            }
        } else if f != "millisecond" && !(-32768..=32767).contains(&n) {
            return Err(err(
                31034,
                format!(
                    "'{f}' must evaluate to a value in the range [-32768, 32767]; value {n} is not in range"
                ),
            ));
        }
        found.push((f, n));
    }
    let zone = zone_arg(op, d.get("timezone"), vars)?;
    if null {
        return Ok(Bson::Null);
    }
    let Some(zone) = zone else {
        return Ok(Bson::Null);
    };
    let get = |k: &str, dflt: i64| {
        found
            .iter()
            .find(|(f, _)| *f == k)
            .map_or(dflt, |(_, v)| *v)
    };
    let days = if natural {
        let total_m = get("year", 1970) * 12 + get("month", 1) - 1;
        let (y, m) = (total_m.div_euclid(12), total_m.rem_euclid(12) + 1);
        days_from_civil(y, m, 1) + get("day", 1) - 1
    } else {
        let y = get("isoWeekYear", 1970);
        let jan4 = days_from_civil(y, 1, 4);
        let monday = jan4 - ((jan4 + 3).rem_euclid(7));
        monday + (get("isoWeek", 1) - 1) * 7 + get("isoDayOfWeek", 1) - 1
    };
    let local = days * DAY
        + get("hour", 0) * 3_600_000
        + get("minute", 0) * 60_000
        + get("second", 0) * 1000
        + get("millisecond", 0);
    Ok(Bson::DateTime(DateTime::from_millis(zone.utc(local))))
}

// --- parsing and formatting --------------------------------------------------

/// A parsed date string: local milliseconds and the string's own offset.
struct Parsed {
    local: i64,
    offset_s: Option<i64>,
}

fn parse_err(s: &str, why: &str) -> MongoError {
    err(241, format!("Error parsing date string '{s}'; {why}"))
}

fn take_digits(b: &[u8], i: &mut usize, min: usize, max: usize) -> Option<i64> {
    let start = *i;
    while *i < b.len() && *i - start < max && b[*i].is_ascii_digit() {
        *i += 1;
    }
    if *i - start < min {
        *i = start;
        return None;
    }
    std::str::from_utf8(&b[start..*i]).ok()?.parse().ok()
}

fn check_civil(
    s: &str,
    y: i64,
    m: i64,
    d: i64,
    h: i64,
    mi: i64,
    sec: i64,
) -> Result<(), MongoError> {
    if !(1..=12).contains(&m) || d < 1 || d > days_in_month(y, m) {
        return Err(parse_err(s, "The parsed date was invalid"));
    }
    if h > 23 || mi > 59 || sec > 59 {
        return Err(parse_err(s, "Unexpected data found."));
    }
    Ok(())
}

const MONTHS: [&str; 12] = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
];


/// US zone abbreviations timelib knows, with their offsets (seconds).
const ABBREVIATIONS: [(&str, i64); 8] = [
    ("est", -5 * 3600),
    ("edt", -4 * 3600),
    ("cst", -6 * 3600),
    ("cdt", -5 * 3600),
    ("mst", -7 * 3600),
    ("mdt", -6 * 3600),
    ("pst", -8 * 3600),
    ("pdt", -7 * 3600),
];

const WEEKDAYS: [&str; 7] = [
    "sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday",
];

/// A cursor over a free-form date string.
struct Cur<'a> {
    s: &'a str,
    b: &'a [u8],
    i: usize,
}

impl Cur<'_> {
    fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }
    fn eat(&mut self, c: u8) -> bool {
        if self.peek() == Some(c) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn spaces(&mut self) -> bool {
        let start = self.i;
        while self.peek() == Some(b' ') {
            self.i += 1;
        }
        self.i > start
    }
    fn digits(&mut self) -> (i64, usize) {
        let start = self.i;
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            self.i += 1;
        }
        let n = self.i - start;
        (self.s[start..self.i].parse().unwrap_or(-1), n)
    }
    fn word(&mut self) -> String {
        let start = self.i;
        while self.peek().is_some_and(|c| c.is_ascii_alphabetic()) {
            self.i += 1;
        }
        self.s[start..self.i].to_ascii_lowercase()
    }
}

fn month_of(w: &str) -> Option<i64> {
    MONTHS
        .iter()
        .position(|n| *n == w || (w.len() == 3 && n.starts_with(w)) || (w == "sept" && *n == "september"))
        .map(|p| i64::try_from(p).unwrap_or(0) + 1)
}

/// Parses a date string without a format, as MongoDB's parser reads the
/// common forms: `2017-02-08`, `2017-2-8`, `2017/02/08`, `20170208`,
/// `2017-W06-3`, `02/08/2017`, `Feb 8 2017`, `8 February 2017`,
/// `Wed, 08 Feb 2017`, each with an optional time (`T12`, `12:10`,
/// `12:10:40.787`) and zone (`Z`, `UTC`, `GMT`, `EST`, `+05:30`, `-08`).
/// Zone names (`America/New_York`) are refused, as MongoDB refuses them.
fn parse_free(s: &str) -> Result<Parsed, MongoError> {
    let bad = |why: &str| parse_err(s, why);
    let mut c = Cur { s, b: s.as_bytes(), i: 0 };
    let (y, m, d, days_override);
    if c.peek().is_some_and(|x| x.is_ascii_alphabetic()) {
        let mut w = c.word();
        if WEEKDAYS.iter().any(|n| *n == w || (w.len() == 3 && n.starts_with(&w))) {
            c.eat(b',');
            c.spaces();
            if c.peek().is_some_and(|x| x.is_ascii_digit()) {
                // `Wed, 08 Feb 2017`.
                let (dd, _) = c.digits();
                c.spaces();
                let mw = c.word();
                let mm = month_of(&mw).ok_or_else(|| bad("Unexpected character"))?;
                c.spaces();
                let (yy, n) = c.digits();
                if n != 4 {
                    return Err(bad("Unexpected character"));
                }
                (y, m, d, days_override) = (yy, mm, dd, None);
            } else {
                w = c.word();
                let mm = month_of(&w).ok_or_else(|| bad("Unexpected character"))?;
                c.spaces();
                let (dd, _) = c.digits();
                c.eat(b',');
                c.spaces();
                let (yy, n) = c.digits();
                if n != 4 {
                    return Err(bad("Unexpected character"));
                }
                (y, m, d, days_override) = (yy, mm, dd, None);
            }
        } else {
            // `Feb 8 2017`, `February 8, 2017`.
            let mm = month_of(&w)
                .ok_or_else(|| bad("passing a time zone identifier as part of the string is not allowed"))?;
            c.spaces();
            let (dd, dn) = c.digits();
            if dn == 0 || dn > 2 {
                return Err(bad("Unexpected character"));
            }
            c.eat(b',');
            c.spaces();
            let (yy, n) = c.digits();
            if n != 4 {
                return Err(bad("Unexpected character"));
            }
            (y, m, d, days_override) = (yy, mm, dd, None);
        }
    } else {
        let (n1, l1) = c.digits();
        match (l1, c.peek()) {
            (8, _) => {
                (y, m, d, days_override) = (n1 / 10000, n1 / 100 % 100, n1 % 100, None);
            }
            (4, Some(sep @ (b'-' | b'/'))) => {
                c.i += 1;
                if sep == b'-' && c.eat(b'W') {
                    // `2017-W06-3`.
                    let (wk, wn) = c.digits();
                    if wn != 2 {
                        return Err(bad("Unexpected character"));
                    }
                    let dow = if c.eat(b'-') { c.digits().0 } else { 1 };
                    if !(1..=53).contains(&wk) || !(1..=7).contains(&dow) {
                        return Err(bad("The parsed date was invalid"));
                    }
                    let jan4 = days_from_civil(n1, 1, 4);
                    let monday = jan4 - ((jan4 + 3).rem_euclid(7));
                    (y, m, d, days_override) = (n1, 1, 1, Some(monday + (wk - 1) * 7 + dow - 1));
                } else {
                    let (mm, ml) = c.digits();
                    if ml == 0 || ml > 2 || !c.eat(sep) {
                        return Err(bad("Unexpected character"));
                    }
                    let (dd, dl) = c.digits();
                    if dl == 0 || dl > 2 {
                        return Err(bad("Unexpected character"));
                    }
                    (y, m, d, days_override) = (n1, mm, dd, None);
                }
            }
            (1 | 2, Some(b'/')) => {
                // `02/08/2017`: month, day, year.
                c.i += 1;
                let (dd, dl) = c.digits();
                if dl == 0 || dl > 2 || !c.eat(b'/') {
                    return Err(bad("Unexpected character"));
                }
                let (yy, yl) = c.digits();
                if yl != 4 {
                    return Err(bad("Unexpected character"));
                }
                (y, m, d, days_override) = (yy, n1, dd, None);
            }
            (1 | 2, Some(b' ')) => {
                // `8 Feb 2017`.
                c.spaces();
                let w = c.word();
                let mm = month_of(&w).ok_or_else(|| bad("Unexpected character"))?;
                c.spaces();
                let (yy, yl) = c.digits();
                if yl != 4 {
                    return Err(bad("Unexpected character"));
                }
                (y, m, d, days_override) = (yy, mm, n1, None);
            }
            _ => return Err(bad("Unexpected character")),
        }
    }
    // The time.
    let (mut h, mut mi, mut sec, mut ms) = (0, 0, 0, 0);
    let save = c.i;
    let had_sep = c.eat(b'T') || c.eat(b't') || c.spaces();
    if had_sep && c.peek().is_some_and(|x| x.is_ascii_digit()) {
        let (hh, hl) = c.digits();
        if hl != 2 && !(hl == 1 && c.peek() == Some(b':')) {
            return Err(bad("Unexpected character"));
        }
        h = hh;
        if c.eat(b':') {
            let (mm, ml) = c.digits();
            if ml != 2 {
                return Err(bad("Unexpected character"));
            }
            mi = mm;
            if c.eat(b':') {
                let (ss, sl) = c.digits();
                if sl != 2 {
                    return Err(bad("Unexpected character"));
                }
                sec = ss;
                if c.eat(b'.') || c.eat(b',') {
                    let start = c.i;
                    let (frac, fl) = c.digits();
                    if fl == 0 {
                        return Err(bad("Unexpected character"));
                    }
                    let _ = start;
                    ms = if fl >= 3 {
                        let t = &s[c.i - fl..c.i - fl + 3];
                        t.parse().unwrap_or(0)
                    } else {
                        frac * 10i64.pow(u32::try_from(3 - fl).unwrap_or(0))
                    };
                }
            }
        }
    } else {
        c.i = save;
    }
    // The zone.
    c.spaces();
    let offset_s = match c.peek() {
        None => None,
        Some(b'Z' | b'z') => {
            c.i += 1;
            Some(0)
        }
        Some(b'+' | b'-') => {
            let rest = &s[c.i..];
            let end = rest
                .find(|ch: char| !(ch.is_ascii_digit() || ch == ':' || ch == '+' || ch == '-'))
                .unwrap_or(rest.len());
            let o = parse_offset(&rest[..end]).ok_or_else(|| bad("Unexpected character"))?;
            c.i += end;
            Some(o)
        }
        Some(x) if x.is_ascii_alphabetic() => {
            let w = c.word();
            if c.peek() == Some(b'/') {
                return Err(bad(
                    "passing a time zone identifier as part of the string is not allowed",
                ));
            }
            if w == "utc" || w == "gmt" {
                Some(0)
            } else {
                Some(
                    ABBREVIATIONS
                        .iter()
                        .find(|(n, _)| *n == w)
                        .map(|(_, o)| *o)
                        .ok_or_else(|| bad("The timezone could not be found in the database"))?,
                )
            }
        }
        _ => return Err(bad("Unexpected character")),
    };
    c.spaces();
    if c.i != c.b.len() {
        return Err(bad("Trailing data"));
    }
    let days = match days_override {
        Some(dd) => {
            check_civil(s, 2000, 1, 1, h, mi, sec)?;
            dd
        }
        None => {
            check_civil(s, y, m, d, h, mi, sec)?;
            days_from_civil(y, m, d)
        }
    };
    Ok(Parsed {
        local: days * DAY + h * 3_600_000 + mi * 60_000 + sec * 1000 + ms,
        offset_s,
    })
}

const FORMAT_CHARS: &str = "bBdGHjLmMSuUVwYzZ%";

fn check_format(fmt: &str, parse: bool) -> Result<(), MongoError> {
    let mut it = fmt.chars().peekable();
    while let Some(c) = it.next() {
        if c != '%' {
            continue;
        }
        match it.next() {
            None => return Err(err(18535, "Unmatched '%' at end of format string")),
            Some(x)
                if FORMAT_CHARS.contains(x) && !(parse && matches!(x, 'U' | 'w' | 'b' | 'B')) => {}
            Some(x) => {
                return Err(err(
                    18536,
                    format!("Invalid format character '%{x}' in format string"),
                ));
            }
        }
    }
    Ok(())
}

fn parse_with_format(s: &str, fmt: &str) -> Result<Parsed, MongoError> {
    let b = s.as_bytes();
    let mut i = 0;
    let (mut y, mut m, mut d, mut h, mut mi, mut sec, mut ms) = (1970, 1, 1, 0, 0, 0, 0);
    let (mut yday, mut iso_y, mut iso_w, mut iso_d) = (None, None, None, None);
    let mut offset_s = None;
    let mut fi = fmt.chars();
    let need = |i: usize| -> Result<(), MongoError> {
        if i >= b.len() {
            Err(parse_err(s, "Not enough data available to satisfy format"))
        } else {
            Ok(())
        }
    };
    let bad = || parse_err(s, "Unexpected data found.");
    while let Some(c) = fi.next() {
        if c != '%' {
            need(i)?;
            let mut buf = [0u8; 4];
            let lit = c.encode_utf8(&mut buf).as_bytes();
            if !b[i..].starts_with(lit) {
                return Err(bad());
            }
            i += lit.len();
            continue;
        }
        let fmt_char = fi.next().unwrap_or('%');
        need(i)?;
        match fmt_char {
            'Y' => y = take_digits(b, &mut i, 1, 4).ok_or_else(bad)?,
            'm' => m = take_digits(b, &mut i, 1, 2).ok_or_else(bad)?,
            'd' => d = take_digits(b, &mut i, 1, 2).ok_or_else(bad)?,
            'H' => h = take_digits(b, &mut i, 1, 2).ok_or_else(bad)?,
            'M' => mi = take_digits(b, &mut i, 1, 2).ok_or_else(bad)?,
            'S' => sec = take_digits(b, &mut i, 1, 2).ok_or_else(bad)?,
            'L' => {
                let start = i;
                let f = take_digits(b, &mut i, 1, 3).ok_or_else(bad)?;
                ms = f * 10i64.pow(u32::try_from(3 - (i - start)).unwrap_or(0));
            }
            'j' => yday = Some(take_digits(b, &mut i, 1, 3).ok_or_else(bad)?),
            'G' => iso_y = Some(take_digits(b, &mut i, 1, 4).ok_or_else(bad)?),
            'V' => iso_w = Some(take_digits(b, &mut i, 1, 2).ok_or_else(bad)?),
            'u' => iso_d = Some(take_digits(b, &mut i, 1, 1).ok_or_else(bad)?),
            'z' => {
                let sign = match b[i] {
                    b'+' => 1,
                    b'-' => -1,
                    _ => return Err(bad()),
                };
                i += 1;
                let hh = take_digits(b, &mut i, 2, 2).ok_or_else(bad)?;
                if b.get(i) == Some(&b':') {
                    i += 1;
                }
                let mm = take_digits(b, &mut i, 2, 2).ok_or_else(bad)?;
                offset_s = Some(sign * (hh * 3600 + mm * 60));
            }
            'Z' => {
                let sign = match b[i] {
                    b'+' => 1,
                    b'-' => -1,
                    _ => {
                        return Err(parse_err(s, "Invalid timezone offset in minutes"));
                    }
                };
                i += 1;
                let mins = take_digits(b, &mut i, 1, 4).ok_or_else(bad)?;
                offset_s = Some(sign * mins * 60);
            }
            _ => {
                if b[i] != b'%' {
                    return Err(bad());
                }
                i += 1;
            }
        }
    }
    if i < b.len() {
        return Err(parse_err(s, "Trailing data"));
    }
    let days = if let Some(iy) = iso_y {
        let jan4 = days_from_civil(iy, 1, 4);
        let monday = jan4 - ((jan4 + 3).rem_euclid(7));
        monday + (iso_w.unwrap_or(1) - 1) * 7 + iso_d.unwrap_or(1) - 1
    } else if let Some(yd) = yday {
        // MongoDB's parser counts `%j` from 0 (day 166 is 16 June).
        days_from_civil(y, 1, 1) + yd
    } else {
        check_civil(s, y, m, d, h, mi, sec)?;
        days_from_civil(y, m, d)
    };
    if h > 23 || mi > 59 || sec > 59 {
        return Err(bad());
    }
    Ok(Parsed {
        local: days * DAY + h * 3_600_000 + mi * 60_000 + sec * 1000 + ms,
        offset_s,
    })
}

/// A date string as `$toDate` and `$convert` read it (UTC unless the
/// string has an offset).
pub(super) fn parse_date_string(s: &str) -> Result<i64, MongoError> {
    let p = parse_free(s)?;
    Ok(p.local - p.offset_s.unwrap_or(0) * 1000)
}

fn from_string(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let op = "$dateFromString";
    let d = options(
        op,
        arg,
        &["dateString", "format", "timezone", "onError", "onNull"],
        40540,
        (40541, "Unrecognized argument to $dateFromString: "),
    )?;
    let Some(ds) = d.get("dateString") else {
        return Err(err(
            40542,
            "Missing 'dateString' parameter to $dateFromString",
        ));
    };
    let fmt = match d.get("format") {
        None => None,
        Some(f) => match eval(f, vars)? {
            Bson::String(f) => {
                check_format(&f, true)?;
                Some(f)
            }
            v if is_nullish(&v) => return Ok(Bson::Null),
            v => {
                return Err(err(
                    40684,
                    format!(
                        "$dateFromString requires that 'format' be a string, found: {} with value {}",
                        tname(&v),
                        show(&v)
                    ),
                ));
            }
        },
    };
    let zone = match d.get("timezone") {
        None => None,
        Some(e) => match zone_arg(op, Some(e), vars)? {
            Some(z) => Some(z),
            None => return Ok(Bson::Null),
        },
    };
    let v = eval(ds, vars)?;
    if is_nullish(&v) {
        return match d.get("onNull") {
            Some(e) => eval(e, vars),
            None => Ok(Bson::Null),
        };
    }
    let r = (|| -> Result<Bson, MongoError> {
        let Bson::String(s) = &v else {
            return Err(err(
                241,
                format!(
                    "$dateFromString requires that 'dateString' be a string, found: {} with value {}",
                    tname(&v),
                    show(&v)
                ),
            ));
        };
        let p = match &fmt {
            Some(f) => parse_with_format(s, f)?,
            None => parse_free(s)?,
        };
        let ms = match (p.offset_s, zone) {
            (Some(_), Some(_)) => {
                return Err(err(
                    241,
                    "you cannot pass in a date/time string with time zone information ('Z') together with a timezone argument",
                ));
            }
            (Some(o), None) => p.local - o * 1000,
            (None, Some(z)) => z.utc(p.local),
            (None, None) => p.local,
        };
        Ok(Bson::DateTime(DateTime::from_millis(ms)))
    })();
    match (r, d.get("onError")) {
        (Err(e), Some(on)) if e.code == 241 => eval(on, vars),
        (r, _) => r,
    }
}

fn format_date(fmt: &str, utc_ms: i64, zone: Zone) -> String {
    let off_ms = zone.offset_at(utc_ms);
    let p = parts(utc_ms + off_ms);
    let mut out = String::new();
    let mut it = fmt.chars();
    while let Some(c) = it.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let w = |n: i64, width: usize| format!("{n:0width$}");
        match it.next() {
            Some('Y') => out.push_str(&w(p.year, 4)),
            Some('m') => out.push_str(&w(p.month, 2)),
            Some('d') => out.push_str(&w(p.day, 2)),
            Some('H') => out.push_str(&w(p.hour, 2)),
            Some('M') => out.push_str(&w(p.minute, 2)),
            Some('S') => out.push_str(&w(p.second, 2)),
            Some('L') => out.push_str(&w(p.ms, 3)),
            Some('j') => out.push_str(&w(p.yday, 3)),
            Some('w') => out.push_str(&p.dow.to_string()),
            Some('u') => out.push_str(&p.iso_dow.to_string()),
            Some('U') => out.push_str(&w((p.yday - 1 + 7 - (p.dow - 1)) / 7, 2)),
            Some('V') => out.push_str(&w(p.iso_week, 2)),
            Some('G') => out.push_str(&w(p.iso_year, 4)),
            Some('z') => {
                let m = off_ms / 60_000;
                out.push(if m < 0 { '-' } else { '+' });
                out.push_str(&w(m.abs() / 60, 2));
                out.push_str(&w(m.abs() % 60, 2));
            }
            Some('Z') => out.push_str(&(off_ms / 60_000).to_string()),
            Some('b') => {
                let n = MONTHS[usize::try_from(p.month - 1).unwrap_or(0)];
                out.push_str(&n[..1].to_ascii_uppercase());
                out.push_str(&n[1..3]);
            }
            Some('B') => {
                let n = MONTHS[usize::try_from(p.month - 1).unwrap_or(0)];
                out.push_str(&n[..1].to_ascii_uppercase());
                out.push_str(&n[1..]);
            }
            Some(x) => out.push(x),
            None => {}
        }
    }
    out
}

/// A date as `$toString` writes one (`2020-01-01T00:00:00.000Z`).
pub(super) fn iso_utc(ms: i64) -> String {
    format_date("%Y-%m-%dT%H:%M:%S.%LZ", ms, Zone::UTC)
}

fn to_string(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let op = "$dateToString";
    let d = options(
        op,
        arg,
        &["date", "format", "timezone", "onNull"],
        18629,
        (18534, "Unrecognized argument to $dateToString: "),
    )?;
    let Some(date) = d.get("date") else {
        return Err(err(18628, "Missing 'date' parameter to $dateToString"));
    };
    let fmt = match d.get("format") {
        None => None,
        Some(f) => match eval(f, vars)? {
            Bson::String(f) => {
                check_format(&f, false)?;
                Some(f)
            }
            v if is_nullish(&v) => return Ok(Bson::Null),
            v => {
                return Err(err(
                    18533,
                    format!(
                        "$dateToString requires that 'format' be a string, found: {} with value {}",
                        tname(&v),
                        show(&v)
                    ),
                ));
            }
        },
    };
    let zone = zone_arg(op, d.get("timezone"), vars)?;
    let v = eval(date, vars)?;
    if is_nullish(&v) {
        return match d.get("onNull") {
            Some(e) => eval(e, vars),
            None => Ok(Bson::Null),
        };
    }
    let Some(ms) = date_ms(&v) else {
        return Err(err(
            16006,
            format!("can't convert from BSON type {} to Date", tname(&v)),
        ));
    };
    let Some(zone) = zone else {
        return Ok(Bson::Null);
    };
    let fmt = fmt.unwrap_or_else(|| {
        if d.contains_key("timezone") {
            "%Y-%m-%dT%H:%M:%S.%L".to_owned()
        } else {
            "%Y-%m-%dT%H:%M:%S.%LZ".to_owned()
        }
    });
    Ok(Bson::String(format_date(&fmt, ms, zone)))
}

// --- arithmetic ----------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Unit {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Ms,
}

fn unit_arg(op: &str, v: &Bson) -> Result<Unit, MongoError> {
    let Bson::String(s) = v else {
        return Err(err(
            5_439_013,
            format!("{op} requires 'unit' to be a string, but got {}", tname(v)),
        ));
    };
    Ok(match s.as_str() {
        "year" => Unit::Year,
        "quarter" => Unit::Quarter,
        "month" => Unit::Month,
        "week" => Unit::Week,
        "day" => Unit::Day,
        "hour" => Unit::Hour,
        "minute" => Unit::Minute,
        "second" => Unit::Second,
        "millisecond" => Unit::Ms,
        _ => {
            return Err(err(
                9,
                format!(
                    "{op} parameter 'unit' value parsing failed :: caused by :: unknown time unit value: {s}"
                ),
            ));
        }
    })
}

fn unit_ms(u: Unit) -> i64 {
    match u {
        Unit::Week => 7 * DAY,
        Unit::Day => DAY,
        Unit::Hour => 3_600_000,
        Unit::Minute => 60_000,
        Unit::Second => 1000,
        _ => 1,
    }
}

/// `local` plus `n` months, the day clamped to the month's last.
fn add_months(local: i64, n: i64) -> Option<i64> {
    let days = local.div_euclid(DAY);
    let rem = local.rem_euclid(DAY);
    let (y, m, d) = civil_from_days(days);
    let total = (y * 12 + m - 1).checked_add(n)?;
    let (ny, nm) = (total.div_euclid(12), total.rem_euclid(12) + 1);
    if !(-300_000..=300_000).contains(&ny) {
        return None;
    }
    let nd = d.min(days_in_month(ny, nm));
    Some(days_from_civil(ny, nm, nd) * DAY + rem)
}

fn date_add(op: &str, arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let d = options(
        op,
        arg,
        &["startDate", "unit", "amount", "timezone"],
        5_166_400,
        (5_166_401, &format!("Unrecognized argument to {op}: ")),
    )?;
    let (Some(sd), Some(un), Some(am)) = (d.get("startDate"), d.get("unit"), d.get("amount"))
    else {
        return Err(err(
            5_166_402,
            format!("{op} requires startDate, unit, and amount to be present"),
        ));
    };
    let start = eval(sd, vars)?;
    let unit = eval(un, vars)?;
    let amount = eval(am, vars)?;
    let zone = zone_arg(op, d.get("timezone"), vars)?;
    if is_nullish(&start) || is_nullish(&unit) || is_nullish(&amount) {
        return Ok(Bson::Null);
    }
    let Some(ms) = date_ms(&start) else {
        return Err(err(
            5_166_403,
            format!("{op} requires startDate to be convertible to a date"),
        ));
    };
    let unit = unit_arg(op, &unit)?;
    let Some(mut n) = integral_i64(&amount) else {
        return Err(err(
            5_166_405,
            format!("{op} expects integer amount of time units"),
        ));
    };
    let Some(zone) = zone else {
        return Ok(Bson::Null);
    };
    if op == "$dateSubtract" {
        n = n.checked_neg().unwrap_or(i64::MAX);
    }
    let overflow = || {
        err(
            5_976_500,
            format!(
                "invalid dateAdd 'amount' parameter value: {} {}",
                n,
                [
                    "year",
                    "quarter",
                    "month",
                    "week",
                    "day",
                    "hour",
                    "minute",
                    "second",
                    "millisecond"
                ][unit as usize]
            ),
        )
    };
    let out = match unit {
        Unit::Year | Unit::Quarter | Unit::Month => {
            let months = n
                .checked_mul(match unit {
                    Unit::Year => 12,
                    Unit::Quarter => 3,
                    _ => 1,
                })
                .ok_or_else(overflow)?;
            let local = zone.local(ms);
            zone.utc(add_months(local, months).ok_or_else(overflow)?)
        }
        Unit::Week | Unit::Day => {
            let delta = n.checked_mul(unit_ms(unit)).ok_or_else(overflow)?;
            let local = zone.local(ms);
            zone.utc(local.checked_add(delta).ok_or_else(overflow)?)
        }
        _ => {
            let delta = n.checked_mul(unit_ms(unit)).ok_or_else(overflow)?;
            ms.checked_add(delta).ok_or_else(overflow)?
        }
    };
    Ok(Bson::DateTime(DateTime::from_millis(out)))
}

fn start_of_week(op: &str, v: &Bson) -> Result<i64, MongoError> {
    let Bson::String(s) = v else {
        return Err(err(
            5_439_015,
            format!(
                "{op} requires 'startOfWeek' to be a string, but got {}",
                tname(v)
            ),
        ));
    };
    let l = s.to_ascii_lowercase();
    let days = [
        "sunday",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
    ];
    days.iter()
        .position(|d| *d == l || (l.len() == 3 && d.starts_with(&l)))
        .map(|p| i64::try_from(p).unwrap_or(0))
        .ok_or_else(|| {
            err(
                5_439_016,
                format!(
                    "{op} parameter 'startOfWeek' value cannot be recognized as a day of a week: {s}"
                ),
            )
        })
}

/// The first day (days since the epoch) of the week holding `day`, weeks
/// starting on `sow` (0 = Sunday).
fn week_start(day: i64, sow: i64) -> i64 {
    let dow = (day + 4).rem_euclid(7);
    day - (dow - sow).rem_euclid(7)
}

fn date_diff(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let op = "$dateDiff";
    let d = options(
        op,
        arg,
        &["startDate", "endDate", "unit", "timezone", "startOfWeek"],
        5_166_301,
        (5_166_302, "Unrecognized argument to $dateDiff: "),
    )?;
    let Some(sd) = d.get("startDate") else {
        return Err(err(5_166_303, "Missing 'startDate' parameter to $dateDiff"));
    };
    let Some(ed) = d.get("endDate") else {
        return Err(err(5_166_304, "Missing 'endDate' parameter to $dateDiff"));
    };
    let Some(un) = d.get("unit") else {
        return Err(err(5_166_305, "Missing 'unit' parameter to $dateDiff"));
    };
    let s = eval(sd, vars)?;
    let e = eval(ed, vars)?;
    let u = eval(un, vars)?;
    let zone = zone_arg(op, d.get("timezone"), vars)?;
    let sow = match d.get("startOfWeek") {
        Some(x) => Some(eval(x, vars)?),
        None => None,
    };
    if is_nullish(&s) || is_nullish(&e) || is_nullish(&u) || sow.as_ref().is_some_and(is_nullish) {
        return Ok(Bson::Null);
    }
    let Some(sm) = date_ms(&s) else {
        return Err(err(
            5_166_307,
            format!(
                "{op} requires 'startDate' to be a date, but got {}",
                tname(&s)
            ),
        ));
    };
    let Some(em) = date_ms(&e) else {
        return Err(err(
            5_166_307,
            format!(
                "{op} requires 'endDate' to be a date, but got {}",
                tname(&e)
            ),
        ));
    };
    let unit = unit_arg(op, &u)?;
    let sow = match (&sow, unit) {
        (Some(v), Unit::Week) => start_of_week(op, v)?,
        _ => 0,
    };
    let Some(zone) = zone else {
        return Ok(Bson::Null);
    };
    let (ls, le) = (zone.local(sm), zone.local(em));
    let (ps, pe) = (parts(ls), parts(le));
    let r = match unit {
        Unit::Year => pe.year - ps.year,
        Unit::Quarter => (pe.year * 4 + (pe.month - 1) / 3) - (ps.year * 4 + (ps.month - 1) / 3),
        Unit::Month => (pe.year * 12 + pe.month) - (ps.year * 12 + ps.month),
        Unit::Week => {
            (week_start(le.div_euclid(DAY), sow) - week_start(ls.div_euclid(DAY), sow)) / 7
        }
        Unit::Day => le.div_euclid(DAY) - ls.div_euclid(DAY),
        Unit::Ms => em - sm,
        // Hours, minutes and seconds: both ends truncated to the unit in
        // local time, then the elapsed time between them.
        u => {
            let n = unit_ms(u);
            let (s0, e0) = (sm - ls.rem_euclid(n), em - le.rem_euclid(n));
            (e0 - s0) / n
        }
    };
    Ok(Bson::Int64(r))
}

/// 2000-01-01T00:00:00, the reference of `$dateTrunc`'s bins.
const REF_DAYS: i64 = 10_957;

fn date_trunc(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let op = "$dateTrunc";
    let d = options(
        op,
        arg,
        &["date", "unit", "binSize", "timezone", "startOfWeek"],
        5_439_007,
        (5_439_008, "Unrecognized argument to $dateTrunc: "),
    )?;
    let Some(de) = d.get("date") else {
        return Err(err(5_439_009, "Missing 'date' parameter to $dateTrunc"));
    };
    let Some(ue) = d.get("unit") else {
        return Err(err(5_439_010, "Missing 'unit' parameter to $dateTrunc"));
    };
    let v = eval(de, vars)?;
    let u = eval(ue, vars)?;
    let bin = match d.get("binSize") {
        Some(b) => Some(eval(b, vars)?),
        None => None,
    };
    let zone = zone_arg(op, d.get("timezone"), vars)?;
    let sow = match d.get("startOfWeek") {
        Some(x) => Some(eval(x, vars)?),
        None => None,
    };
    if is_nullish(&v) || is_nullish(&u) || bin.as_ref().is_some_and(is_nullish) {
        return Ok(Bson::Null);
    }
    let Some(ms) = date_ms(&v) else {
        return Err(err(
            5_439_012,
            format!("{op} requires 'date' to be a date, but got {}", tname(&v)),
        ));
    };
    let unit = unit_arg(op, &u)?;
    let bin = match &bin {
        None => 1,
        Some(b) => {
            let n = integral_i64(b).ok_or_else(|| {
                err(
                    5_439_017,
                    format!(
                        "{op} requires 'binSize' to be a 64-bit integer, but got value '{}' of type {}",
                        super::expr_common::fmt_num6(b),
                        tname(b)
                    ),
                )
            })?;
            if n <= 0 {
                return Err(err(
                    5_439_018,
                    format!("{op} requires 'binSize' to be greater than 0, but got value {n}"),
                ));
            }
            n
        }
    };
    let sow = match (&sow, unit) {
        (Some(x), Unit::Week) if !is_nullish(x) => start_of_week(op, x)?,
        (Some(x), _) if is_nullish(x) => return Ok(Bson::Null),
        _ => 0,
    };
    let Some(zone) = zone else {
        return Ok(Bson::Null);
    };
    let local = zone.local(ms);
    let out_local = match unit {
        Unit::Year | Unit::Quarter | Unit::Month => {
            let p = parts(local);
            let per = match unit {
                Unit::Year => 12,
                Unit::Quarter => 3,
                _ => 1,
            } * bin;
            let months = (p.year - 2000) * 12 + p.month - 1;
            let start = months.div_euclid(per) * per;
            let (y, m) = (2000 + start.div_euclid(12), start.rem_euclid(12) + 1);
            days_from_civil(y, m, 1) * DAY
        }
        Unit::Week => {
            // The first start of a week on or after the reference.
            let ref_day = REF_DAYS + (sow - (REF_DAYS + 4).rem_euclid(7)).rem_euclid(7);
            let day = local.div_euclid(DAY);
            let weeks = (day - ref_day).div_euclid(7);
            (ref_day + weeks.div_euclid(bin) * bin * 7) * DAY
        }
        u => {
            let size = unit_ms(u).saturating_mul(bin);
            let base = REF_DAYS * DAY;
            base + (local - base).div_euclid(size) * size
        }
    };
    let out = match (unit, zone) {
        // Hours and smaller bins are fixed lengths of time.
        (Unit::Hour | Unit::Minute | Unit::Second | Unit::Ms, Zone::Named(_)) => {
            out_local - (local - ms)
        }
        _ => zone.utc(out_local),
    };
    Ok(Bson::DateTime(DateTime::from_millis(out)))
}

/// This module's operators.
pub(super) fn eval_op(op: &str, arg: &Bson, vars: &Vars) -> Option<Result<Bson, MongoError>> {
    Some(match op {
        "$year" | "$month" | "$dayOfMonth" | "$hour" | "$minute" | "$second" | "$millisecond"
        | "$dayOfWeek" | "$dayOfYear" | "$week" | "$isoWeek" | "$isoWeekYear" | "$isoDayOfWeek" => {
            date_part(op, arg, vars)
        }
        "$dateToParts" => to_parts(arg, vars),
        "$dateFromParts" => from_parts(arg, vars),
        "$dateFromString" => from_string(arg, vars),
        "$dateToString" => to_string(arg, vars),
        "$dateAdd" | "$dateSubtract" => date_add(op, arg, vars),
        "$dateDiff" => date_diff(arg, vars),
        "$dateTrunc" => date_trunc(arg, vars),
        _ => return None,
    })
}
