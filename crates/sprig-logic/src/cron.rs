//! Cron schedules (chapter 41, "Scheduled jobs").
//!
//! Five fields, `minute hour day-of-month month day-of-week`, each `*`, a
//! number, a range `a-b`, a list `a,b`, or a step `*/n`, `a-b/n`; months
//! and days may be named (`JAN`, `MON`); day 7 is Sunday, like 0. When both
//! day fields are restricted, a day matching either fires (cron's rule).
//! `@hourly`, `@daily`, `@weekly`, `@monthly` and `@yearly` are short
//! forms.
//!
//! Times are local to the schedule's IANA time zone and stored in UTC.
//! Daylight-saving changes follow cron: a time the clocks skip fires once,
//! at the next valid time; a time they repeat fires once, the first time.

use chrono::{
    DateTime, Datelike, Duration, LocalResult, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc,
};
use chrono_tz::Tz;
use sprig_common::{Result, SprigCode, SprigError};

/// A parsed schedule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cron {
    minutes: Vec<u32>,
    hours: Vec<u32>,
    days: Vec<u32>,
    months: Vec<u32>,
    weekdays: Vec<u32>,
    any_day: bool,
    any_weekday: bool,
    tz: Tz,
}

fn bad(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::LogicScheduleInvalid, m)
}

const MONTHS: [&str; 12] = [
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
];
const DAYS: [&str; 7] = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

fn value(s: &str, names: &[&str], base: u32) -> Option<u32> {
    if let Ok(n) = s.parse() {
        return Some(n);
    }
    let up = s.to_ascii_uppercase();
    names
        .iter()
        .position(|n| *n == up)
        .and_then(|i| u32::try_from(i).ok())
        .map(|i| i + base)
}

/// One field: the sorted values it allows, and whether it was `*`.
fn field(text: &str, lo: u32, hi: u32, names: &[&str], base: u32) -> Result<(Vec<u32>, bool)> {
    let mut out = Vec::new();
    let mut star = false;
    for part in text.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((r, s)) => (
                r,
                s.parse::<u32>()
                    .ok()
                    .filter(|s| *s > 0)
                    .ok_or_else(|| bad(format!("bad step in {text:?}")))?,
            ),
            None => (part, 1),
        };
        let (a, b) = if range == "*" {
            if step == 1 && text == "*" {
                star = true;
            }
            (lo, hi)
        } else if let Some((a, b)) = range.split_once('-') {
            (
                value(a, names, base).ok_or_else(|| bad(format!("bad value {a:?}")))?,
                value(b, names, base).ok_or_else(|| bad(format!("bad value {b:?}")))?,
            )
        } else {
            let v = value(range, names, base).ok_or_else(|| bad(format!("bad value {range:?}")))?;
            (v, if part.contains('/') { hi } else { v })
        };
        if a < lo || b > hi || a > b {
            return Err(bad(format!("{text:?} is outside {lo}-{hi}")));
        }
        let mut v = a;
        while v <= b {
            out.push(v);
            v += step;
        }
    }
    out.sort_unstable();
    out.dedup();
    Ok((out, star))
}

impl Cron {
    /// Parses `text` in time zone `tz`.
    ///
    /// # Errors
    /// 39873 for bad text or an unknown time zone.
    pub fn parse(text: &str, tz: &str) -> Result<Self> {
        let tz: Tz = tz
            .parse()
            .map_err(|_| bad(format!("unknown time zone {tz:?}")))?;
        let t = text.trim();
        let expanded = match t.to_ascii_lowercase().as_str() {
            "@hourly" => "0 * * * *".to_owned(),
            "@daily" | "@midnight" => "0 0 * * *".to_owned(),
            "@weekly" => "0 0 * * 0".to_owned(),
            "@monthly" => "0 0 1 * *".to_owned(),
            "@yearly" | "@annually" => "0 0 1 1 *".to_owned(),
            _ => t.to_owned(),
        };
        let f: Vec<&str> = expanded.split_whitespace().collect();
        let [mi, h, dom, mon, dow] = f.as_slice() else {
            return Err(bad(format!(
                "cron text {text:?} needs five fields: minute hour day month weekday"
            )));
        };
        let (minutes, _) = field(mi, 0, 59, &[], 0)?;
        let (hours, _) = field(h, 0, 23, &[], 0)?;
        let (days, any_day) = field(dom, 1, 31, &[], 0)?;
        let (months, _) = field(mon, 1, 12, &MONTHS, 1)?;
        let (mut weekdays, any_weekday) = field(dow, 0, 7, &DAYS, 0)?;
        if weekdays.contains(&7) {
            weekdays.retain(|d| *d != 7);
            if !weekdays.contains(&0) {
                weekdays.insert(0, 0);
            }
        }
        Ok(Self {
            minutes,
            hours,
            days,
            months,
            weekdays,
            any_day,
            any_weekday,
            tz,
        })
    }

    fn day_matches(&self, d: NaiveDate) -> bool {
        if !self.months.contains(&d.month()) {
            return false;
        }
        let dom = self.days.contains(&d.day());
        let dow = self.weekdays.contains(&d.weekday().num_days_from_sunday());
        match (self.any_day, self.any_weekday) {
            (true, true) => true,
            (true, false) => dow,
            (false, true) => dom,
            (false, false) => dom || dow,
        }
    }

    /// The first fire time strictly after `after`, in UTC; `None` if
    /// there is none within five years (`0 0 31 2 *`).
    #[must_use]
    pub fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let local = after.with_timezone(&self.tz).naive_local();
        let start = local
            .with_second(0)
            .and_then(|t| t.with_nanosecond(0))
            .unwrap_or(local);
        let mut day = start.date();
        let last = day + Duration::days(366 * 5);
        while day <= last {
            if self.day_matches(day) {
                for &h in &self.hours {
                    for &m in &self.minutes {
                        let Some(t) = day.and_hms_opt(h, m, 0) else {
                            continue;
                        };
                        if t + Duration::minutes(1) <= start {
                            continue;
                        }
                        if let Some(u) = self.resolve(t)
                            && u > after
                        {
                            return Some(u);
                        }
                    }
                }
            }
            day = day.succ_opt()?;
        }
        None
    }

    /// A local time in UTC: the first of a repeated time, and for a time
    /// the clocks skip, the first valid minute after it.
    fn resolve(&self, t: NaiveDateTime) -> Option<DateTime<Utc>> {
        match self.tz.from_local_datetime(&t) {
            LocalResult::Single(x) => Some(x.with_timezone(&Utc)),
            LocalResult::Ambiguous(a, b) => Some(a.min(b).with_timezone(&Utc)),
            LocalResult::None => {
                let mut x = t;
                for _ in 0..(24 * 60) {
                    x += Duration::minutes(1);
                    if let LocalResult::Single(v) | LocalResult::Ambiguous(v, _) =
                        self.tz.from_local_datetime(&x)
                    {
                        return Some(v.with_timezone(&Utc));
                    }
                }
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn utc(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn every_minute_and_steps() {
        let c = Cron::parse("* * * * *", "UTC").unwrap();
        assert_eq!(
            c.next_after(utc("2026-11-09T10:00:30Z")),
            Some(utc("2026-11-09T10:01:00Z"))
        );
        assert_eq!(
            c.next_after(utc("2026-11-09T10:01:00Z")),
            Some(utc("2026-11-09T10:02:00Z"))
        );
        let c = Cron::parse("*/15 9-17 * * MON-FRI", "UTC").unwrap();
        // 2026-11-07 is a Saturday.
        assert_eq!(
            c.next_after(utc("2026-11-07T12:00:00Z")),
            Some(utc("2026-11-09T09:00:00Z"))
        );
        assert_eq!(
            c.next_after(utc("2026-11-09T09:00:00Z")),
            Some(utc("2026-11-09T09:15:00Z"))
        );
    }

    #[test]
    fn time_zones() {
        // 02:00 in Kolkata (UTC+5:30) is 20:30 UTC the day before.
        let c = Cron::parse("0 2 * * *", "Asia/Kolkata").unwrap();
        assert_eq!(
            c.next_after(utc("2026-11-09T00:00:00Z")),
            Some(utc("2026-11-09T20:30:00Z"))
        );
    }

    #[test]
    fn daylight_saving() {
        // New York skips 02:00-03:00 on 8 March 2026: 02:30 fires once, at
        // 03:00 local (07:00 UTC).
        let c = Cron::parse("30 2 * * *", "America/New_York").unwrap();
        assert_eq!(
            c.next_after(utc("2026-03-08T05:00:00Z")),
            Some(utc("2026-03-08T07:00:00Z"))
        );
        // And repeats 01:00-02:00 on 1 November 2026: 01:30 fires once,
        // the first time (05:30 UTC), not again at 06:30 UTC.
        let c = Cron::parse("30 1 * * *", "America/New_York").unwrap();
        let first = c.next_after(utc("2026-11-01T04:00:00Z")).unwrap();
        assert_eq!(first, utc("2026-11-01T05:30:00Z"));
        assert_eq!(c.next_after(first), Some(utc("2026-11-02T06:30:00Z")));
    }

    #[test]
    fn days_and_names() {
        // Day 13 or a Friday (either restricted field matches).
        let c = Cron::parse("0 0 13 * FRI", "UTC").unwrap();
        assert_eq!(
            c.next_after(utc("2026-11-09T00:00:00Z")),
            Some(utc("2026-11-13T00:00:00Z"))
        );
        let c = Cron::parse("@monthly", "UTC").unwrap();
        assert_eq!(
            c.next_after(utc("2026-11-09T00:00:00Z")),
            Some(utc("2026-12-01T00:00:00Z"))
        );
        let c = Cron::parse("0 0 * * 7", "UTC").unwrap();
        assert_eq!(
            c.next_after(utc("2026-11-09T00:00:00Z")),
            Some(utc("2026-11-15T00:00:00Z"))
        );
        assert_eq!(
            Cron::parse("0 0 31 2 *", "UTC")
                .unwrap()
                .next_after(utc("2026-01-01T00:00:00Z")),
            None
        );
    }

    #[test]
    fn errors() {
        for (t, z) in [
            ("* * * *", "UTC"),
            ("60 * * * *", "UTC"),
            ("* * * * *", "Mars/Olympus"),
            ("*/0 * * * *", "UTC"),
            ("5-1 * * * *", "UTC"),
        ] {
            assert_eq!(
                Cron::parse(t, z).unwrap_err().code(),
                Some(SprigCode::LogicScheduleInvalid),
                "{t} {z}"
            );
        }
    }
}
