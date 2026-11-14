//! Normalization: key values to canonical bytes, including values from
//! different models.
//!
//! A key part becomes `tag (1 byte) ‖ length (u32 BE) ‖ payload`, and a
//! composite key concatenates its parts. Equal values under MySQL's exact
//! rules (or MongoDB's) give equal bytes, whatever model or type they came
//! from; a value of another class than the group's is hashed under its own
//! tag and reported as off-class.

use std::fmt::Write as _;

use sprig_common::{Result, SprigCode, SprigError};

use crate::collation::{self, Charset};
use crate::key::{DateTimeParts, KeyClass, KeySpec, KeyValue, parse_uuid};

/// `NULL`.
pub const TAG_NULL: u8 = 0x00;
/// A numeric value that is an integer fitting `i128`.
pub const TAG_INT: u8 = 0x01;
/// Any other finite number: sign, exponent, coefficient.
pub const TAG_DECIMAL: u8 = 0x02;
/// +∞, −∞, NaN.
pub const TAG_SPECIAL: u8 = 0x03;
/// A string: its collation weight string.
pub const TAG_STRING: u8 = 0x10;
/// Bytes.
pub const TAG_BINARY: u8 = 0x11;
/// A UUID's 16 bytes.
pub const TAG_UUID: u8 = 0x12;
/// Calendar fields.
pub const TAG_DATETIME: u8 = 0x20;
/// An instant.
pub const TAG_TIMESTAMP: u8 = 0x21;

/// What normalization found besides the bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NormOutcome {
    /// A value was of another class than its key part and was hashed under
    /// its own tag.
    pub off_class: bool,
}

/// Turns key values into canonical bytes.
pub trait Normalizer: Send + Sync {
    /// Appends the normalized bytes of `v` (one value per part of `spec`)
    /// to `out`.
    ///
    /// # Errors
    /// 39400 if the number of values doesn't match the spec; 39405 if a
    /// string can't be converted to its part's character set.
    fn normalize(
        &self,
        spec: &KeySpec,
        v: &[KeyValue<'_>],
        out: &mut Vec<u8>,
    ) -> Result<NormOutcome>;
}

/// The one normalizer.
#[derive(Debug, Clone, Copy, Default)]
pub struct StdNormalizer;

impl Normalizer for StdNormalizer {
    fn normalize(
        &self,
        spec: &KeySpec,
        v: &[KeyValue<'_>],
        out: &mut Vec<u8>,
    ) -> Result<NormOutcome> {
        if v.len() != spec.parts.len() {
            return Err(SprigError::new(
                SprigCode::UnsupportedKeyType,
                format!(
                    "the key has {} parts but {} values were given",
                    spec.parts.len(),
                    v.len()
                ),
            ));
        }
        let mut outcome = NormOutcome::default();
        for (class, value) in spec.parts.iter().zip(v) {
            if !part(*class, value, out)? {
                outcome.off_class = true;
            }
        }
        Ok(outcome)
    }
}

/// Appends one framed part; returns whether the value was on-class.
fn part(class: KeyClass, v: &KeyValue<'_>, out: &mut Vec<u8>) -> Result<bool> {
    use KeyValue as V;
    Ok(match (class, v) {
        (_, V::Null) => {
            frame(out, TAG_NULL, |_| {});
            true
        }
        (KeyClass::Numeric, V::Int(_) | V::Decimal(_) | V::Double(_) | V::Bool(_)) => {
            numeric(v, out)?;
            true
        }
        (KeyClass::String { collation }, V::Str { bytes, charset }) => {
            let c = collation::require(collation)?;
            string_part(out, |o| c.hash_key(bytes, *charset, o))?;
            true
        }
        (KeyClass::String { collation }, V::Bytes(b)) => {
            let c = collation::require(collation)?;
            string_part(out, |o| c.hash_key(b, Charset::Binary, o))?;
            true
        }
        (KeyClass::String { collation }, V::ObjectId(o)) => {
            let c = collation::require(collation)?;
            let hex = hex_lower(o);
            string_part(out, |w| c.hash_key(hex.as_bytes(), Charset::Ascii, w))?;
            true
        }
        (KeyClass::Binary, V::Bytes(b) | V::Str { bytes: b, .. }) => {
            frame(out, TAG_BINARY, |o| o.extend_from_slice(b));
            true
        }
        (KeyClass::Binary, V::ObjectId(o)) => {
            frame(out, TAG_BINARY, |w| w.extend_from_slice(o));
            true
        }
        (KeyClass::Uuid, V::Uuid(u)) => {
            frame(out, TAG_UUID, |o| o.extend_from_slice(u));
            true
        }
        (KeyClass::Uuid, V::Str { bytes, charset }) => {
            if let Some(u) = std::str::from_utf8(bytes).ok().and_then(parse_uuid) {
                frame(out, TAG_UUID, |o| o.extend_from_slice(&u));
                true
            } else {
                {
                    own_tag(
                        &V::Str {
                            bytes,
                            charset: *charset,
                        },
                        out,
                    )?;
                    false
                }
            }
        }
        (KeyClass::DateTime, V::DateTime(d)) => {
            datetime(*d, out);
            true
        }
        (KeyClass::DateTime, V::Str { bytes, .. })
            if std::str::from_utf8(bytes)
                .ok()
                .and_then(DateTimeParts::parse)
                .is_some() =>
        {
            let d = std::str::from_utf8(bytes)
                .ok()
                .and_then(DateTimeParts::parse)
                .unwrap_or_default();
            datetime(d, out);
            true
        }
        (KeyClass::Timestamp, V::Timestamp { .. }) => {
            own_tag(v, out)?;
            true
        }
        _ => {
            own_tag(v, out)?;
            false
        }
    })
}

/// Encodes a value under its own tag (off-class values, and timestamps).
fn own_tag(v: &KeyValue<'_>, out: &mut Vec<u8>) -> Result<()> {
    use KeyValue as V;
    match v {
        V::Null => frame(out, TAG_NULL, |_| {}),
        V::Int(_) | V::Decimal(_) | V::Double(_) | V::Bool(_) => numeric(v, out)?,
        V::Str { bytes, charset } => {
            let utf8 = collation::to_utf8(bytes, *charset)?;
            frame(out, TAG_STRING, |o| o.extend_from_slice(&utf8));
        }
        V::Bytes(b) => frame(out, TAG_BINARY, |o| o.extend_from_slice(b)),
        V::ObjectId(b) => frame(out, TAG_BINARY, |o| o.extend_from_slice(b)),
        V::Uuid(u) => frame(out, TAG_UUID, |o| o.extend_from_slice(u)),
        V::DateTime(d) => datetime(*d, out),
        V::Timestamp { secs, micros } => frame(out, TAG_TIMESTAMP, |o| {
            o.extend_from_slice(&secs.to_be_bytes());
            o.extend_from_slice(&micros.to_be_bytes());
        }),
    }
    Ok(())
}

fn string_part(out: &mut Vec<u8>, f: impl FnOnce(&mut Vec<u8>) -> Result<()>) -> Result<()> {
    let at = begin(out, TAG_STRING);
    f(out)?;
    end(out, at);
    Ok(())
}

fn frame(out: &mut Vec<u8>, tag: u8, f: impl FnOnce(&mut Vec<u8>)) {
    let at = begin(out, tag);
    f(out);
    end(out, at);
}

fn begin(out: &mut Vec<u8>, tag: u8) -> usize {
    out.push(tag);
    let at = out.len();
    out.extend_from_slice(&[0; 4]);
    at
}

fn end(out: &mut [u8], at: usize) {
    let len = u32::try_from(out.len() - at - 4).unwrap_or(u32::MAX);
    out[at..at + 4].copy_from_slice(&len.to_be_bytes());
}

fn datetime(d: DateTimeParts, out: &mut Vec<u8>) {
    frame(out, TAG_DATETIME, |o| {
        o.extend_from_slice(&d.year.to_be_bytes());
        o.extend_from_slice(&[d.month, d.day, d.hour, d.minute, d.second]);
        o.extend_from_slice(&d.micros.to_be_bytes());
    });
}

fn hex_lower(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

// ---- numbers ---------------------------------------------------------------

/// An exact number: an integer, or `±coef × 10^exp` with `coef` free of
/// trailing zeros.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Num {
    Int(i128),
    Dec { neg: bool, digits: String, exp: i64 },
    Special(u8),
}

fn numeric(v: &KeyValue<'_>, out: &mut Vec<u8>) -> Result<()> {
    let n = match v {
        KeyValue::Int(i) => Num::Int(*i),
        KeyValue::Bool(b) => Num::Int(i128::from(*b)),
        KeyValue::Decimal(d) => parse_decimal(d.0).ok_or_else(|| {
            SprigError::new(
                SprigCode::KeyValueUnroutable,
                format!("{:?} is not a decimal number", d.0),
            )
        })?,
        KeyValue::Double(d) => from_double(*d),
        _ => return Err(SprigError::internal("numeric() on a non-number")),
    };
    match n {
        Num::Int(i) => frame(out, TAG_INT, |o| o.extend_from_slice(&i.to_be_bytes())),
        Num::Special(s) => frame(out, TAG_SPECIAL, |o| o.push(s)),
        Num::Dec { neg, digits, exp } => {
            let exp = i32::try_from(exp).map_err(|_| {
                SprigError::new(
                    SprigCode::KeyValueUnroutable,
                    "a number's exponent is too large",
                )
            })?;
            frame(out, TAG_DECIMAL, |o| {
                o.push(u8::from(!neg));
                o.extend_from_slice(&exp.to_be_bytes());
                o.extend_from_slice(&digits_to_be_bytes(&digits));
            });
        }
    }
    Ok(())
}

/// Reduces `±digits × 10^exp`: strips zeros, and returns an integer when
/// the value is integral and fits `i128`.
fn reduce(neg: bool, digits: &str, mut exp: i64) -> Num {
    let d = digits.trim_start_matches('0');
    let trimmed = d.trim_end_matches('0');
    if trimmed.is_empty() {
        return Num::Int(0);
    }
    exp += i64::try_from(d.len() - trimmed.len()).unwrap_or(i64::MAX);
    if (0..=39).contains(&exp) {
        let mut v: Option<i128> = trimmed.parse::<i128>().ok();
        for _ in 0..exp {
            v = v.and_then(|x| x.checked_mul(10));
        }
        if let Some(v) = v {
            return Num::Int(if neg { -v } else { v });
        }
    }
    Num::Dec {
        neg,
        digits: trimmed.to_owned(),
        exp,
    }
}

/// Parses `[+-]digits[.digits][e[+-]digits]`.
fn parse_decimal(s: &str) -> Option<Num> {
    let s = s.trim();
    let (neg, s) = match s.as_bytes().first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    let (mant, e) = match s.find(['e', 'E']) {
        Some(i) => (&s[..i], s[i + 1..].parse::<i64>().ok()?),
        None => (s, 0),
    };
    let (ip, fp) = mant.split_once('.').unwrap_or((mant, ""));
    if ip.is_empty() && fp.is_empty() {
        return None;
    }
    if !ip.bytes().chain(fp.bytes()).all(|b| b.is_ascii_digit()) {
        return None;
    }
    let digits = format!("{ip}{fp}");
    let exp = e.checked_sub(i64::try_from(fp.len()).ok()?)?;
    Some(reduce(neg, &digits, exp))
}

/// The exact value of a double (every finite binary fraction has a finite
/// decimal expansion).
fn from_double(d: f64) -> Num {
    if d.is_nan() {
        return Num::Special(3);
    }
    if d.is_infinite() {
        return Num::Special(if d > 0.0 { 1 } else { 2 });
    }
    if d == 0.0 {
        return Num::Int(0);
    }
    let bits = d.to_bits();
    let neg = bits >> 63 == 1;
    #[allow(clippy::cast_possible_wrap)]
    let raw_exp = ((bits >> 52) & 0x7ff) as i64;
    let frac = bits & ((1u64 << 52) - 1);
    let (m, e) = if raw_exp == 0 {
        (frac, -1074)
    } else {
        (frac | (1u64 << 52), raw_exp - 1075)
    };
    // d = ±m × 2^e
    let mut big = Big::from_u64(m);
    if e >= 0 {
        for _ in 0..e {
            big.mul_small(2);
        }
        reduce(neg, &big.to_decimal(), 0)
    } else {
        for _ in 0..(-e) {
            big.mul_small(5);
        }
        reduce(neg, &big.to_decimal(), e)
    }
}

/// A non-negative integer in base 10^9, least significant limb first.
struct Big(Vec<u32>);

const BASE: u64 = 1_000_000_000;

impl Big {
    fn from_u64(mut v: u64) -> Self {
        let mut limbs = Vec::new();
        while v > 0 {
            #[allow(clippy::cast_possible_truncation)]
            limbs.push((v % BASE) as u32);
            v /= BASE;
        }
        Self(limbs)
    }

    fn mul_small(&mut self, k: u32) {
        let mut carry = 0u64;
        for l in &mut self.0 {
            let x = u64::from(*l) * u64::from(k) + carry;
            #[allow(clippy::cast_possible_truncation)]
            {
                *l = (x % BASE) as u32;
            }
            carry = x / BASE;
        }
        while carry > 0 {
            #[allow(clippy::cast_possible_truncation)]
            self.0.push((carry % BASE) as u32);
            carry /= BASE;
        }
    }

    fn to_decimal(&self) -> String {
        let Some((last, rest)) = self.0.split_last() else {
            return "0".to_owned();
        };
        let mut s = last.to_string();
        for l in rest.iter().rev() {
            let _ = write!(s, "{l:09}");
        }
        s
    }
}

/// A decimal digit string as minimal big-endian bytes.
fn digits_to_be_bytes(digits: &str) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::new(); // little-endian while building
    for d in digits.bytes() {
        let mut carry = u32::from(d - b'0');
        for b in &mut out {
            let x = u32::from(*b) * 10 + carry;
            #[allow(clippy::cast_possible_truncation)]
            {
                *b = (x & 0xff) as u8;
            }
            carry = x >> 8;
        }
        while carry > 0 {
            #[allow(clippy::cast_possible_truncation)]
            out.push((carry & 0xff) as u8);
            carry >>= 8;
        }
    }
    if out.is_empty() {
        out.push(0);
    }
    out.reverse();
    out
}

#[cfg(test)]
mod tests;
