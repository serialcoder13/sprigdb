//! MariaDB's `JSON_HB` histograms (chapter 13, "Statistics").
//!
//! From 11.0 `ANALYZE ... PERSISTENT` stores, in `mysql.column_stats.histogram`
//! with `hist_type = 'JSON_HB'`, a height-balanced histogram as JSON.
//! Measured on 11.8.9 (live test `json_hb_format_on_11_8`):
//!
//! ```json
//! {"target_histogram_size": 254,
//!  "collected_at": "2026-11-08 10:15:50",
//!  "collected_by": "11.8.9-MariaDB-ubu2404-log",
//!  "histogram_hb": [
//!    {"start": "hold", "size": 0.01, "ndv": 1},
//!    {"start": "open", "size": 0.04, "ndv": 1},
//!    {"start": "paid", "end": "paid", "size": 0.95, "ndv": 1}]}
//! ```
//!
//! - `size` is the bucket's fraction of the column's **non-NULL** rows
//!   (measured: a column half NULL, with `a` in 10% of all rows, has
//!   `a` at `size` 0.2), and `ndv` its distinct values; a bucket with
//!   `ndv = 1` is a popular value, and a popular value gets one bucket
//!   however large (`paid`, 0.95 above). The estimates here are fractions
//!   of non-NULL rows; [`super::ColumnStats`] scales them by
//!   `1 - nulls_ratio`.
//! - Only the last bucket has `end`; a bucket ends where the next starts.
//!   A value can sit at the end of one bucket and the start of the next
//!   (`{"start": "0", "ndv": 2}` then `{"start": "1"}` for an integer
//!   column), so a point estimate also takes its share of the previous
//!   bucket.
//! - Every value is a string, numbers and dates included (`"8.00"`,
//!   `"2026-01-01 01:20:00"`). `hist_size` in `column_stats` is the number
//!   of buckets, not the target size.
//!
//! Values are compared as [`Key`]s: numbers and temporal values as `f64`,
//! so a partly covered bucket can be interpolated linearly; text as bytes,
//! which are the column collation's `WEIGHT_STRING()` once
//! [`reweigh`](Histogram::reweigh) has replaced the raw UTF-8.

use std::cmp::Ordering;

use serde::Deserialize;
use sprig_common::{Result, SprigError};

/// How a column's values are ordered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyClass {
    /// Integers, decimals, floats, `BIT`, `YEAR`.
    Numeric,
    /// `DATE`, `DATETIME`, `TIMESTAMP`, `TIME`: compared as seconds.
    Temporal,
    /// Strings: compared as bytes (weight strings under the column's
    /// collation once reweighed).
    Text,
}

/// A comparable value.
#[derive(Debug, Clone, PartialEq)]
pub enum Key {
    /// A number or a temporal value in seconds.
    Num(f64),
    /// Text as bytes.
    Bytes(Vec<u8>),
}

impl Key {
    /// Parses a histogram or parameter value of `class`. `None` if it
    /// doesn't parse (a number column holding text).
    #[must_use]
    pub fn parse(class: KeyClass, s: &str) -> Option<Key> {
        match class {
            KeyClass::Numeric => s.trim().parse::<f64>().ok().map(Key::Num),
            KeyClass::Temporal => temporal_seconds(s).map(Key::Num),
            KeyClass::Text => Some(Key::Bytes(s.as_bytes().to_vec())),
        }
    }

    fn num(&self) -> Option<f64> {
        match self {
            Key::Num(x) => Some(*x),
            Key::Bytes(_) => None,
        }
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Key::Num(a), Key::Num(b)) => a.partial_cmp(b),
            (Key::Bytes(a), Key::Bytes(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
}

/// One bucket.
#[derive(Debug, Clone, PartialEq)]
pub struct Bucket {
    /// The raw `start` text.
    pub start_text: String,
    /// The start as a key.
    pub start: Key,
    /// Fraction of rows.
    pub size: f64,
    /// Distinct values.
    pub ndv: u64,
}

/// A parsed `JSON_HB` histogram of one column.
#[derive(Debug, Clone, PartialEq)]
pub struct Histogram {
    /// How values compare.
    pub class: KeyClass,
    /// Buckets, ascending.
    pub buckets: Vec<Bucket>,
    /// The raw `end` of the last bucket.
    pub end_text: String,
    /// The last bucket's end as a key.
    pub end: Key,
    /// `collected_at` (`YYYY-MM-DD HH:MM:SS`, server time), when present.
    pub collected_at: Option<String>,
}

#[derive(Deserialize)]
struct RawHistogram {
    #[serde(default)]
    collected_at: Option<String>,
    histogram_hb: Vec<RawBucket>,
}

#[derive(Deserialize)]
struct RawBucket {
    start: String,
    #[serde(default)]
    end: Option<String>,
    size: f64,
    ndv: u64,
}

impl Histogram {
    /// Parses `json` for a column of `class`.
    ///
    /// # Errors
    /// `Internal` if the JSON isn't a `JSON_HB` histogram or a bound
    /// doesn't parse as `class`.
    pub fn parse(class: KeyClass, json: &[u8]) -> Result<Self> {
        let raw: RawHistogram = serde_json::from_slice(json)
            .map_err(|e| SprigError::internal(format!("JSON_HB histogram: {e}")))?;
        let Some(last) = raw.histogram_hb.last() else {
            return Err(SprigError::internal("JSON_HB histogram has no buckets"));
        };
        let end_text = last.end.clone().unwrap_or_else(|| last.start.clone());
        let key = |s: &str| {
            Key::parse(class, s).ok_or_else(|| {
                SprigError::internal(format!("JSON_HB bound {s:?} is not {class:?}"))
            })
        };
        let end = key(&end_text)?;
        let buckets = raw
            .histogram_hb
            .iter()
            .map(|b| {
                Ok(Bucket {
                    start: key(&b.start)?,
                    start_text: b.start.clone(),
                    size: b.size.clamp(0.0, 1.0),
                    ndv: b.ndv.max(1),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            class,
            buckets,
            end_text,
            end,
            collected_at: raw.collected_at,
        })
    }

    /// The raw texts of every bound (bucket starts, then the end), in the
    /// order [`reweigh`](Self::reweigh) expects their weights.
    #[must_use]
    pub fn bound_texts(&self) -> Vec<&str> {
        self.buckets
            .iter()
            .map(|b| b.start_text.as_str())
            .chain(std::iter::once(self.end_text.as_str()))
            .collect()
    }

    /// Replaces text bounds with their weight strings (one per
    /// [`bound_texts`](Self::bound_texts) entry, in order). Ignored for
    /// other classes or a wrong count.
    pub fn reweigh(&mut self, weights: Vec<Vec<u8>>) {
        if self.class != KeyClass::Text || weights.len() != self.buckets.len() + 1 {
            return;
        }
        let mut w = weights.into_iter();
        for b in &mut self.buckets {
            if let Some(x) = w.next() {
                b.start = Key::Bytes(x);
            }
        }
        if let Some(x) = w.next() {
            self.end = Key::Bytes(x);
        }
    }

    /// The fraction of rows equal to `v`, from the buckets that can hold
    /// it: the last bucket starting at or before `v` (its share: all of
    /// it when `ndv = 1` and it starts at `v`, else `size / ndv`), plus
    /// the previous bucket's share when `v` is exactly a bucket start (the
    /// value may end the previous bucket). Values outside the histogram,
    /// or between the values of a one-value bucket, get 0.
    #[must_use]
    pub fn point(&self, v: &Key) -> f64 {
        let Some(i) = self.bucket_of(v) else {
            return 0.0;
        };
        let b = &self.buckets[i];
        let at_start = b.start == *v;
        let mut sel = if b.ndv == 1 {
            if at_start || self.is_last_value(i, v) {
                b.size
            } else {
                0.0
            }
        } else {
            b.size / to_f64(b.ndv)
        };
        if at_start && i > 0 {
            let p = &self.buckets[i - 1];
            if p.ndv > 1 {
                sel += p.size / to_f64(p.ndv);
            }
        }
        sel
    }

    fn is_last_value(&self, i: usize, v: &Key) -> bool {
        i + 1 == self.buckets.len() && self.end == *v
    }

    /// The bucket holding `v`: the last with `start <= v`, or `None`
    /// below the first start or above the end.
    fn bucket_of(&self, v: &Key) -> Option<usize> {
        if self
            .buckets
            .first()
            .is_none_or(|b| b.start.partial_cmp(v) == Some(Ordering::Greater))
        {
            return None;
        }
        if self.end.partial_cmp(v) == Some(Ordering::Less) {
            return None;
        }
        let n = self.buckets.partition_point(|b| {
            matches!(
                b.start.partial_cmp(v),
                Some(Ordering::Less | Ordering::Equal)
            )
        });
        n.checked_sub(1)
    }

    /// The fraction of rows in `[lo, hi]` (either bound open when `None`):
    /// the sum of the buckets inside, and a linear share of a partly
    /// covered numeric or temporal bucket. A partly covered text bucket
    /// counts half.
    #[must_use]
    pub fn range(&self, lo: Option<&Key>, hi: Option<&Key>) -> f64 {
        let mut total = 0.0;
        for (i, b) in self.buckets.iter().enumerate() {
            let b_lo = &b.start;
            let b_hi = self.buckets.get(i + 1).map_or(&self.end, |n| &n.start);
            total += b.size * self.covered(b_lo, b_hi, lo, hi, b.ndv);
        }
        total.clamp(0.0, 1.0)
    }

    /// The covered fraction of bucket `[b_lo, b_hi]` by `[lo, hi]`.
    fn covered(&self, b_lo: &Key, b_hi: &Key, lo: Option<&Key>, hi: Option<&Key>, ndv: u64) -> f64 {
        let below = |a: &Key, b: &Key| a.partial_cmp(b) == Some(Ordering::Less);
        // Entirely outside.
        if lo.is_some_and(|l| below(b_hi, l)) || hi.is_some_and(|h| below(h, b_lo)) {
            return 0.0;
        }
        let from = match lo {
            Some(l) if below(b_lo, l) => Some(l),
            _ => None,
        };
        let to = match hi {
            Some(h) if below(h, b_hi) => Some(h),
            _ => None,
        };
        if from.is_none() && to.is_none() {
            return 1.0;
        }
        if ndv == 1 {
            // One value at the bucket start: inside iff the start is.
            return if from.is_none() { 1.0 } else { 0.0 };
        }
        match self.class {
            KeyClass::Numeric | KeyClass::Temporal => {
                let (Some(a), Some(b)) = (b_lo.num(), b_hi.num()) else {
                    return 0.5;
                };
                let width = b - a;
                if width <= 0.0 {
                    return 1.0;
                }
                let s = from.and_then(Key::num).unwrap_or(a).max(a);
                let e = to.and_then(Key::num).unwrap_or(b).min(b);
                ((e - s) / width).clamp(0.0, 1.0)
            }
            KeyClass::Text => 0.5,
        }
    }

    /// Number of buckets.
    #[must_use]
    pub fn len(&self) -> usize {
        self.buckets.len()
    }

    /// Never true for a parsed histogram.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }
}

#[allow(clippy::cast_precision_loss)]
fn to_f64(n: u64) -> f64 {
    n as f64
}

/// Seconds of a temporal text: `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS[.f]`
/// (from 1970, proleptic Gregorian) or a `TIME` `[-]HHH:MM:SS[.f]`.
#[must_use]
#[allow(clippy::many_single_char_names)] // y, m, d, h: the calendar's own names
pub fn temporal_seconds(s: &str) -> Option<f64> {
    let s = s.trim();
    let (date, time) = match s.split_once([' ', 'T']) {
        Some((d, t)) => (Some(d), Some(t)),
        None if s.matches('-').count() >= 2 && !s.starts_with('-') => (Some(s), None),
        None => (None, Some(s)),
    };
    let mut secs = 0.0;
    if let Some(d) = date {
        let mut it = d.split('-');
        let y: i64 = it.next()?.parse().ok()?;
        let m: u32 = it.next()?.parse().ok()?;
        let day: u32 = it.next()?.parse().ok()?;
        if it.next().is_some() || !(1..=12).contains(&m) || day > 31 {
            return None;
        }
        #[allow(clippy::cast_precision_loss)]
        {
            secs += days_from_civil(y, m, day) as f64 * 86_400.0;
        }
    }
    if let Some(t) = time {
        let neg = t.starts_with('-');
        let t = t.trim_start_matches('-');
        let mut it = t.split(':');
        let h: f64 = it.next()?.parse().ok()?;
        let m: f64 = it.next().map_or(Some(0.0), |x| x.parse().ok())?;
        let sec: f64 = it.next().map_or(Some(0.0), |x| x.parse().ok())?;
        let v = h * 3600.0 + m * 60.0 + sec;
        secs += if neg { -v } else { v };
    }
    Some(secs)
}

/// Days since 1970-01-01 (Howard Hinnant's `days_from_civil`).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = i64::from(m);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}
