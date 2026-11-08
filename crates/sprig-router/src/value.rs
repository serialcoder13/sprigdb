//! Values in the gateway: how a column of a shard result compares, exact
//! decimal arithmetic for split aggregates, and canonical keys for
//! grouping and joining.

use std::cmp::Ordering;

use sprig_proto_mysql::ColumnDef;
use sprig_proto_mysql::resultset::ColumnFlags;

use crate::collation;

/// How a result column's values compare, read from its definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColClass {
    /// Integers and `DECIMAL`: exact decimal comparison.
    Exact,
    /// `FLOAT` and `DOUBLE`.
    Float,
    /// `DATE`, `DATETIME`, `TIMESTAMP`, `YEAR`'s text: fixed-width, so
    /// bytes order as values.
    FixedText,
    /// `TIME`: signed, possibly over 24 hours.
    Time,
    /// A string with a non-binary collation: compared by weights.
    String {
        /// Collation id.
        collation: u16,
    },
    /// Binary strings and `BIT`: bytes.
    Binary,
    /// The `NULL` type: every value is NULL.
    Null,
    /// Something the router can't order or group exactly (`ENUM`, `SET`,
    /// `GEOMETRY`).
    Unsupported,
}

/// The class of a column definition.
#[must_use]
pub fn class_of(def: &ColumnDef) -> ColClass {
    let flags = ColumnFlags::from_bits_truncate(def.flags);
    if flags.intersects(ColumnFlags::ENUM | ColumnFlags::SET) {
        return ColClass::Unsupported;
    }
    match def.col_type {
        0 | 1 | 2 | 3 | 8 | 9 | 246 => ColClass::Exact,
        4 | 5 => ColClass::Float,
        7 | 10 | 12 | 13 | 14 => ColClass::FixedText,
        11 => ColClass::Time,
        6 => ColClass::Null,
        16 => ColClass::Binary,
        15 | 249..=254 | 245 => {
            if def.charset == 63 {
                ColClass::Binary
            } else {
                ColClass::String {
                    collation: def.charset,
                }
            }
        }
        _ => ColClass::Unsupported,
    }
}

/// An exact decimal: `mant * 10^-scale`, within `i128`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dec {
    /// The digits, signed.
    pub mant: i128,
    /// Digits after the point.
    pub scale: u32,
}

impl Dec {
    /// Zero at `scale`.
    #[must_use]
    pub fn zero(scale: u32) -> Self {
        Self { mant: 0, scale }
    }

    /// Parses MariaDB's text form (`-12.340`, `5`, `1e3` is refused).
    #[must_use]
    pub fn parse(s: &[u8]) -> Option<Self> {
        let s = std::str::from_utf8(s).ok()?.trim();
        let (neg, body) = match s.as_bytes().first()? {
            b'-' => (true, &s[1..]),
            b'+' => (false, &s[1..]),
            _ => (false, s),
        };
        let (int, frac) = body.split_once('.').unwrap_or((body, ""));
        if int.is_empty() && frac.is_empty() {
            return None;
        }
        if !int.bytes().all(|b| b.is_ascii_digit()) || !frac.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let mut mant: i128 = 0;
        for b in int.bytes().chain(frac.bytes()) {
            mant = mant.checked_mul(10)?.checked_add(i128::from(b - b'0'))?;
        }
        let scale = u32::try_from(frac.len()).ok()?;
        Some(Self {
            mant: if neg { -mant } else { mant },
            scale,
        })
    }

    /// The same value at a larger `scale`.
    #[must_use]
    pub fn rescale(self, scale: u32) -> Option<Self> {
        if scale < self.scale {
            return None;
        }
        let f = 10i128.checked_pow(scale - self.scale)?;
        Some(Self {
            mant: self.mant.checked_mul(f)?,
            scale,
        })
    }

    /// `self + o`, at the larger scale.
    #[must_use]
    pub fn plus(self, o: Self) -> Option<Self> {
        let s = self.scale.max(o.scale);
        let (a, b) = (self.rescale(s)?, o.rescale(s)?);
        Some(Self {
            mant: a.mant.checked_add(b.mant)?,
            scale: s,
        })
    }

    /// `self / n` at `scale`, rounded half away from zero, as MariaDB's
    /// decimal division rounds.
    #[must_use]
    pub fn div_int(self, n: i128, scale: u32) -> Option<Self> {
        if n == 0 {
            return None;
        }
        // value * 10^scale / n, with one guard digit for rounding.
        let up = scale.checked_add(1)?.checked_sub(self.scale);
        let num = match up {
            Some(k) => self.mant.checked_mul(10i128.checked_pow(k)?)?,
            None => return None,
        };
        let q = num / n;
        let r = if q >= 0 { (q + 5) / 10 } else { (q - 5) / 10 };
        Some(Self { mant: r, scale })
    }

    /// The text MariaDB prints for a `DECIMAL` at this scale.
    #[must_use]
    pub fn to_text(self) -> String {
        let neg = self.mant < 0;
        let digits = self.mant.unsigned_abs().to_string();
        let scale = self.scale as usize;
        let mut out = String::with_capacity(digits.len() + 3);
        if neg {
            out.push('-');
        }
        if scale == 0 {
            out.push_str(&digits);
        } else if digits.len() > scale {
            let (i, f) = digits.split_at(digits.len() - scale);
            out.push_str(i);
            out.push('.');
            out.push_str(f);
        } else {
            out.push_str("0.");
            out.push_str(&"0".repeat(scale - digits.len()));
            out.push_str(&digits);
        }
        out
    }
}

/// Compares two exact decimal texts without limits on their size.
#[must_use]
pub fn cmp_decimal_text(a: &[u8], b: &[u8]) -> Ordering {
    fn split(s: &[u8]) -> (bool, &[u8], &[u8]) {
        let s = s.trim_ascii();
        let (neg, body) = match s.first() {
            Some(b'-') => (true, &s[1..]),
            Some(b'+') => (false, &s[1..]),
            _ => (false, s),
        };
        let (int, frac) = match body.iter().position(|&c| c == b'.') {
            Some(p) => (&body[..p], &body[p + 1..]),
            None => (body, &b""[..]),
        };
        let int = &int[int.iter().position(|&c| c != b'0').unwrap_or(int.len())..];
        let frac = &frac[..frac.len() - frac.iter().rev().take_while(|&&c| c == b'0').count()];
        let zero = int.is_empty() && frac.is_empty();
        (neg && !zero, int, frac)
    }
    let (an, ai, af) = split(a);
    let (bn, bi, bf) = split(b);
    let mag = ai
        .len()
        .cmp(&bi.len())
        .then_with(|| ai.cmp(bi))
        .then_with(|| af.cmp(bf));
    match (an, bn) {
        (false, false) => mag,
        (true, true) => mag.reverse(),
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
    }
}

/// `TIME` text to signed microseconds.
#[must_use]
pub fn time_micros(s: &[u8]) -> Option<i64> {
    let s = std::str::from_utf8(s).ok()?.trim();
    let (neg, body) = s.strip_prefix('-').map_or((false, s), |b| (true, b));
    let (hms, frac) = body.split_once('.').unwrap_or((body, ""));
    let mut parts = hms.split(':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let sec: i64 = parts.next()?.parse().ok()?;
    let mut micros: i64 = 0;
    for (i, c) in frac.bytes().take(6).enumerate() {
        micros += i64::from(c - b'0') * 10i64.pow(5 - u32::try_from(i).ok()?);
    }
    let v = ((h * 60 + m) * 60 + sec) * 1_000_000 + micros;
    Some(if neg { -v } else { v })
}

fn parse_f64(s: &[u8]) -> f64 {
    std::str::from_utf8(s)
        .ok()
        .and_then(|t| t.trim().parse().ok())
        .unwrap_or(0.0)
}

/// Compares two non-NULL values of class `class` by value (strings by
/// their bytes: callers compare weights for those).
#[must_use]
pub fn cmp_value(class: ColClass, a: &[u8], b: &[u8]) -> Ordering {
    match class {
        ColClass::Exact => cmp_decimal_text(a, b),
        ColClass::Float => parse_f64(a).total_cmp(&parse_f64(b)),
        ColClass::Time => time_micros(a).cmp(&time_micros(b)),
        _ => a.cmp(b),
    }
}

/// Compares two strings' weights, level by level, under `collation`.
/// `None` for a collation the table lacks.
#[must_use]
pub fn cmp_weights(collation: u16, a: &[&[u8]; 3], b: &[&[u8]; 3]) -> Option<Ordering> {
    let info = collation::info(collation)?;
    for l in 0..3 {
        // MariaDB ignores trailing space weights on levels 2 and 3 even
        // under NO PAD (chapter 15's collation table).
        match collation::cmp_level(a[l], b[l], info.pad_space || l > 0, info.space[l]) {
            Ordering::Equal => {}
            o => return Some(o),
        }
    }
    Some(Ordering::Equal)
}

/// A canonical key for equality: equal keys exactly when MariaDB would
/// find the values equal under the column's comparison.
#[must_use]
pub fn canonical(class: ColClass, v: &[u8], weights: Option<&[&[u8]; 3]>) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    match class {
        ColClass::Exact => {
            let s = std::str::from_utf8(v).ok()?.trim();
            let (neg, body) = s.strip_prefix('-').map_or((false, s), |b| (true, b));
            let (int, frac) = body.split_once('.').unwrap_or((body, ""));
            let int = int.trim_start_matches('0');
            let frac = frac.trim_end_matches('0');
            if (int.len() + frac.len()) > 0 && neg {
                out.push(b'-');
            }
            out.extend_from_slice(if int.is_empty() { b"0" } else { int.as_bytes() });
            if !frac.is_empty() {
                out.push(b'.');
                out.extend_from_slice(frac.as_bytes());
            }
            out.insert(0, b'N');
        }
        ColClass::Float => {
            let f = parse_f64(v);
            let f = if f == 0.0 { 0.0 } else { f };
            out.push(b'F');
            out.extend_from_slice(&f.to_bits().to_be_bytes());
        }
        ColClass::Time => {
            out.push(b'T');
            out.extend_from_slice(&time_micros(v)?.to_be_bytes());
        }
        ColClass::String { collation } => {
            let info = collation::info(collation)?;
            let w = weights?;
            out.push(b'S');
            for (l, (wl, sl)) in w.iter().zip(info.space).enumerate() {
                let t = collation::trim_level(wl, info.pad_space || l > 0, sl);
                out.extend_from_slice(&u32::try_from(t.len()).ok()?.to_be_bytes());
                out.extend_from_slice(t);
            }
        }
        ColClass::FixedText | ColClass::Binary => {
            out.push(b'B');
            out.extend_from_slice(v);
        }
        ColClass::Null => out.push(b'0'),
        ColClass::Unsupported => return None,
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimals_parse_add_divide_and_print() {
        let a = Dec::parse(b"12.50").unwrap_or_else(|| panic!());
        let b = Dec::parse(b"-0.125").unwrap_or_else(|| panic!());
        let s = a.plus(b).unwrap_or_else(|| panic!());
        assert_eq!(s.to_text(), "12.375");
        assert_eq!(
            Dec::parse(b"10")
                .and_then(|d| d.div_int(4, 4))
                .map(Dec::to_text)
                .as_deref(),
            Some("2.5000")
        );
        assert_eq!(
            Dec::parse(b"2")
                .and_then(|d| d.div_int(3, 4))
                .map(Dec::to_text)
                .as_deref(),
            Some("0.6667")
        );
        assert_eq!(
            Dec::parse(b"-2")
                .and_then(|d| d.div_int(3, 4))
                .map(Dec::to_text)
                .as_deref(),
            Some("-0.6667")
        );
        assert_eq!(
            Dec::parse(b"-0.5").map(Dec::to_text).as_deref(),
            Some("-0.5")
        );
        assert_eq!(
            Dec::parse(b"0.05").map(Dec::to_text).as_deref(),
            Some("0.05")
        );
        assert!(Dec::parse(b"1e3").is_none());
    }

    #[test]
    fn decimal_text_order() {
        assert_eq!(cmp_decimal_text(b"-1", b"0"), Ordering::Less);
        assert_eq!(cmp_decimal_text(b"10", b"9.99"), Ordering::Greater);
        assert_eq!(cmp_decimal_text(b"-10", b"-9.99"), Ordering::Less);
        assert_eq!(cmp_decimal_text(b"1.50", b"1.5"), Ordering::Equal);
        assert_eq!(cmp_decimal_text(b"-0.00", b"0"), Ordering::Equal);
        assert_eq!(
            cmp_decimal_text(b"18446744073709551615", b"9223372036854775807"),
            Ordering::Greater
        );
    }

    #[test]
    fn time_values_order_by_value() {
        assert_eq!(
            cmp_value(ColClass::Time, b"-01:00:00", b"00:00:00"),
            Ordering::Less
        );
        assert_eq!(
            cmp_value(ColClass::Time, b"100:00:00", b"99:59:59.999999"),
            Ordering::Greater
        );
    }

    #[test]
    fn canonical_numbers_ignore_format() {
        assert_eq!(
            canonical(ColClass::Exact, b"1.50", None),
            canonical(ColClass::Exact, b"1.5", None)
        );
        assert_eq!(
            canonical(ColClass::Exact, b"-0.0", None),
            canonical(ColClass::Exact, b"0", None)
        );
    }
}
