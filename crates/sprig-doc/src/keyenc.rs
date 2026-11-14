//! The order-preserving key encoding and MongoDB's comparison order.
//!
//! A key is a class byte, then a body, so `memcmp` gives MongoDB's order.
//! Classes, in MongoDB order: MinKey 10, undefined 15, null 20, numbers
//! 30, strings and symbols 40, objects 50, arrays 60, binary 70, ObjectId
//! 80, booleans 90, dates 100, Timestamps 110, regex 120, DBPointer 130,
//! JavaScript 140, MaxKey 250.
//!
//! Numbers encode by exact decimal value, so `1`, `NumberLong(1)`, `1.0`
//! and `NumberDecimal("1.00")` are equal, while `NumberDecimal("0.1")`
//! and the double `0.1` (exactly 0.1000000000000000055…) differ:
//!
//! ```text
//! 30 | 00 NaN | 01 -inf | 02 negative: !(exp | digits) | 03 zero
//!    | 04 positive: exp u16 BE (biased 0x8000) | digit nibbles d+1, 0-terminated
//!    | 05 +inf
//! ```
//!
//! Strings escape `00` as `00 FF` and end with `00 00`. Objects are
//! (class, name, value) per field and end with `00`; arrays are their
//! elements' keys and end with `00`.

use std::cmp::Ordering;

use bson::Bson;

/// Class codes in MongoDB order.
pub mod class {
    #![allow(missing_docs)]
    pub const MIN_KEY: u8 = 10;
    pub const UNDEFINED: u8 = 15;
    pub const NULL: u8 = 20;
    pub const NUMBER: u8 = 30;
    pub const STRING: u8 = 40;
    pub const OBJECT: u8 = 50;
    pub const ARRAY: u8 = 60;
    pub const BINARY: u8 = 70;
    pub const OBJECT_ID: u8 = 80;
    pub const BOOL: u8 = 90;
    pub const DATE: u8 = 100;
    pub const TIMESTAMP: u8 = 110;
    pub const REGEX: u8 = 120;
    pub const DB_POINTER: u8 = 130;
    pub const JAVASCRIPT: u8 = 140;
    pub const JAVASCRIPT_SCOPE: u8 = 145;
    pub const MAX_KEY: u8 = 250;
}

/// The class of a value.
#[must_use]
pub fn class_of(v: &Bson) -> u8 {
    match v {
        Bson::MinKey => class::MIN_KEY,
        Bson::Undefined => class::UNDEFINED,
        Bson::Null => class::NULL,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => class::NUMBER,
        Bson::String(_) | Bson::Symbol(_) => class::STRING,
        Bson::Document(_) => class::OBJECT,
        Bson::Array(_) => class::ARRAY,
        Bson::Binary(_) => class::BINARY,
        Bson::ObjectId(_) => class::OBJECT_ID,
        Bson::Boolean(_) => class::BOOL,
        Bson::DateTime(_) => class::DATE,
        Bson::Timestamp(_) => class::TIMESTAMP,
        Bson::RegularExpression(_) => class::REGEX,
        Bson::DbPointer(_) => class::DB_POINTER,
        Bson::JavaScriptCode(_) => class::JAVASCRIPT,
        Bson::JavaScriptCodeWithScope(_) => class::JAVASCRIPT_SCOPE,
        Bson::MaxKey => class::MAX_KEY,
    }
}

/// The class used for comparisons: undefined compares as null in queries
/// and sorts, as in MongoDB.
#[must_use]
pub fn cmp_class(v: &Bson) -> u8 {
    match v {
        Bson::Undefined => class::NULL,
        _ => class_of(v),
    }
}

/// A number's exact decimal value: `0.d1 d2 … × 10^exp` with `d1 != 0`
/// and no trailing zeros. `digits` empty means zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Num {
    /// NaN.
    NaN,
    /// −∞.
    NegInf,
    /// A finite value.
    Finite {
        /// Negative.
        neg: bool,
        /// Digits 0-9, the first nonzero, the last nonzero; empty for zero.
        digits: Vec<u8>,
        /// Decimal exponent of `0.digits`.
        exp: i32,
    },
    /// +∞.
    PosInf,
}

fn normalize(neg: bool, mut digits: Vec<u8>, mut exp: i32) -> Num {
    let lead = digits.iter().take_while(|d| **d == 0).count();
    digits.drain(..lead);
    exp -= i32::try_from(lead).unwrap_or(0);
    while digits.last() == Some(&0) {
        digits.pop();
    }
    if digits.is_empty() {
        return Num::Finite {
            neg: false,
            digits,
            exp: 0,
        };
    }
    Num::Finite { neg, digits, exp }
}

fn int_digits(mut v: u128) -> Vec<u8> {
    if v == 0 {
        return Vec::new();
    }
    let mut d = Vec::new();
    while v > 0 {
        #[allow(clippy::cast_possible_truncation)]
        d.push((v % 10) as u8);
        v /= 10;
    }
    d.reverse();
    d
}

fn from_int(i: i128) -> Num {
    let d = int_digits(i.unsigned_abs());
    let n = i32::try_from(d.len()).unwrap_or(0);
    normalize(i < 0, d, n)
}

/// A big unsigned integer in base 10^9, least significant limb first.
struct Big(Vec<u32>);

impl Big {
    fn from_u64(v: u64) -> Self {
        let mut l = Vec::new();
        let mut v = v;
        while v > 0 {
            #[allow(clippy::cast_possible_truncation)]
            l.push((v % 1_000_000_000) as u32);
            v /= 1_000_000_000;
        }
        Self(l)
    }

    fn mul(&mut self, m: u32) {
        let mut carry = 0u64;
        for x in &mut self.0 {
            let p = u64::from(*x) * u64::from(m) + carry;
            #[allow(clippy::cast_possible_truncation)]
            {
                *x = (p % 1_000_000_000) as u32;
            }
            carry = p / 1_000_000_000;
        }
        while carry > 0 {
            #[allow(clippy::cast_possible_truncation)]
            self.0.push((carry % 1_000_000_000) as u32);
            carry /= 1_000_000_000;
        }
    }

    fn digits(&self) -> Vec<u8> {
        let mut s = String::new();
        for (i, l) in self.0.iter().rev().enumerate() {
            if i == 0 {
                s.push_str(&l.to_string());
            } else {
                let _ = std::fmt::Write::write_fmt(&mut s, format_args!("{l:09}"));
            }
        }
        s.bytes().map(|b| b - b'0').collect()
    }
}

/// The exact decimal value of a double.
#[must_use]
pub fn num_of_f64(f: f64) -> Num {
    if f.is_nan() {
        return Num::NaN;
    }
    if f.is_infinite() {
        return if f > 0.0 { Num::PosInf } else { Num::NegInf };
    }
    if f == 0.0 {
        return normalize(false, Vec::new(), 0);
    }
    let bits = f.to_bits();
    let neg = bits >> 63 == 1;
    let e = i32::try_from((bits >> 52) & 0x7ff).unwrap_or(0);
    let frac = bits & ((1u64 << 52) - 1);
    let (m, e2) = if e == 0 {
        (frac, -1074)
    } else {
        (frac | (1u64 << 52), e - 1075)
    };
    let mut b = Big::from_u64(m);
    if e2 >= 0 {
        for _ in 0..e2 {
            b.mul(2);
        }
        let d = b.digits();
        let n = i32::try_from(d.len()).unwrap_or(0);
        normalize(neg, d, n)
    } else {
        // m × 2^e2 = m × 5^k / 10^k with k = -e2.
        let k = -e2;
        let mut left = k;
        while left >= 13 {
            b.mul(1_220_703_125); // 5^13
            left -= 13;
        }
        for _ in 0..left {
            b.mul(5);
        }
        let d = b.digits();
        let n = i32::try_from(d.len()).unwrap_or(0);
        normalize(neg, d, n - k)
    }
}

/// `(negative, digits, exp)` of a Decimal128's text (as `Display` writes
/// it: `59.90`, `-1.5E+10`, `0E-6176`), normalized as [`Num`]. `None`
/// for NaN and infinities.
#[must_use]
pub fn decimal_parts(t: &str) -> Option<(bool, Vec<u8>, i32)> {
    let (neg, t) = match t.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    if !t.starts_with(|c: char| c.is_ascii_digit()) {
        return None;
    }
    let (mant, e) = match t.find(['e', 'E']) {
        Some(i) => (&t[..i], t[i + 1..].parse::<i32>().ok()?),
        None => (t, 0),
    };
    let (ip, fp) = mant.split_once('.').unwrap_or((mant, ""));
    let mut digits: Vec<u8> = ip.bytes().chain(fp.bytes()).map(|b| b - b'0').collect();
    if digits.iter().any(|d| *d > 9) {
        return None;
    }
    let mut exp = i32::try_from(ip.len()).ok()? + e;
    let lead = digits.iter().take_while(|d| **d == 0).count();
    digits.drain(..lead);
    exp -= i32::try_from(lead).ok()?;
    while digits.last() == Some(&0) {
        digits.pop();
    }
    if digits.is_empty() {
        return Some((false, digits, 0));
    }
    Some((neg, digits, exp))
}

/// The exact value of a numeric BSON value; `None` for other types.
#[must_use]
pub fn num_of(v: &Bson) -> Option<Num> {
    Some(match v {
        Bson::Int32(i) => from_int(i128::from(*i)),
        Bson::Int64(i) => from_int(i128::from(*i)),
        Bson::Double(f) => num_of_f64(*f),
        Bson::Decimal128(d) => {
            let t = d.to_string();
            match t.as_str() {
                "NaN" | "-NaN" => Num::NaN,
                "Infinity" => Num::PosInf,
                "-Infinity" => Num::NegInf,
                _ => {
                    let (neg, digits, exp) = decimal_parts(&t)?;
                    normalize(neg, digits, exp)
                }
            }
        }
        _ => return None,
    })
}

fn encode_num(n: &Num, out: &mut Vec<u8>) {
    match n {
        Num::NaN => out.push(0),
        Num::NegInf => out.push(1),
        Num::PosInf => out.push(5),
        Num::Finite { digits, .. } if digits.is_empty() => out.push(3),
        Num::Finite { neg, digits, exp } => {
            out.push(if *neg { 2 } else { 4 });
            let start = out.len();
            #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
            let e = (i64::from(*exp) + 0x8000).clamp(0, 0xFFFF) as u16;
            out.extend_from_slice(&e.to_be_bytes());
            let mut nib: Vec<u8> = digits.iter().map(|d| d + 1).collect();
            nib.push(0);
            if nib.len() % 2 == 1 {
                nib.push(0);
            }
            for p in nib.chunks(2) {
                out.push((p[0] << 4) | p[1]);
            }
            if *neg {
                for b in &mut out[start..] {
                    *b = !*b;
                }
            }
        }
    }
}

fn encode_str(s: &[u8], out: &mut Vec<u8>) {
    for b in s {
        out.push(*b);
        if *b == 0 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0, 0]);
}

/// Appends the key of `v`.
pub fn encode_key(v: &Bson, out: &mut Vec<u8>) {
    let c = class_of(v);
    out.push(c);
    match v {
        Bson::MinKey | Bson::MaxKey | Bson::Null | Bson::Undefined => {}
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => {
            if let Some(n) = num_of(v) {
                encode_num(&n, out);
            }
        }
        Bson::String(s) | Bson::Symbol(s) | Bson::JavaScriptCode(s) => {
            encode_str(s.as_bytes(), out);
        }
        Bson::Document(d) => {
            for (k, x) in d {
                out.push(class_of(x));
                encode_str(k.as_bytes(), out);
                encode_key(x, out);
            }
            out.push(0);
        }
        Bson::Array(a) => {
            for x in a {
                encode_key(x, out);
            }
            out.push(0);
        }
        Bson::Binary(b) => {
            let n = u32::try_from(b.bytes.len()).unwrap_or(u32::MAX);
            out.extend_from_slice(&n.to_be_bytes());
            out.push(u8::from(b.subtype));
            out.extend_from_slice(&b.bytes);
        }
        Bson::ObjectId(o) => out.extend_from_slice(&o.bytes()),
        Bson::Boolean(b) => out.push(u8::from(*b)),
        Bson::DateTime(d) => {
            #[allow(clippy::cast_sign_loss)]
            let u = (d.timestamp_millis() as u64) ^ (1 << 63);
            out.extend_from_slice(&u.to_be_bytes());
        }
        Bson::Timestamp(t) => {
            out.extend_from_slice(&t.time.to_be_bytes());
            out.extend_from_slice(&t.increment.to_be_bytes());
        }
        Bson::RegularExpression(r) => {
            encode_str(r.pattern.as_bytes(), out);
            encode_str(r.options.as_bytes(), out);
        }
        Bson::DbPointer(_) => {
            let j = v.clone().into_canonical_extjson().to_string();
            encode_str(j.as_bytes(), out);
        }
        Bson::JavaScriptCodeWithScope(c) => {
            encode_str(c.code.as_bytes(), out);
            encode_key(&Bson::Document(c.scope.clone()), out);
        }
    }
}

/// The key of `v` as a new vector.
#[must_use]
pub fn key(v: &Bson) -> Vec<u8> {
    let mut k = Vec::with_capacity(16);
    encode_key(v, &mut k);
    k
}

/// MongoDB's comparison of two values (undefined as null).
#[must_use]
pub fn compare(a: &Bson, b: &Bson) -> Ordering {
    let (ca, cb) = (cmp_class(a), cmp_class(b));
    if ca != cb {
        return ca.cmp(&cb);
    }
    if ca == class::NULL {
        return Ordering::Equal;
    }
    match (a, b) {
        (Bson::Int32(x), Bson::Int32(y)) => x.cmp(y),
        (Bson::String(x), Bson::String(y)) => match crate::collation::active() {
            Some(c) => c.compare(x, y),
            None => x.as_bytes().cmp(y.as_bytes()),
        },
        _ => key(a).cmp(&key(b)),
    }
}

/// MongoDB equality (numbers by value across types).
#[must_use]
pub fn equal(a: &Bson, b: &Bson) -> bool {
    compare(a, b) == Ordering::Equal
}

/// A hash key for [`equal`]: two values are equal exactly when their
/// keys are (null and undefined share one), for hash sets and joins.
#[must_use]
pub fn eq_key(v: &Bson) -> Vec<u8> {
    if cmp_class(v) == class::NULL {
        return vec![class::NULL];
    }
    key(v)
}

/// Inverts every byte (descending key components).
pub fn complement(k: &mut [u8]) {
    for b in k {
        *b = !*b;
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bson::{Decimal128, doc, oid::ObjectId};

    use super::*;

    #[test]
    fn numbers_by_exact_value() {
        let one = [
            Bson::Int32(1),
            Bson::Int64(1),
            Bson::Double(1.0),
            Bson::Decimal128(Decimal128::from_str("1.00").unwrap()),
        ];
        for a in &one {
            for b in &one {
                assert_eq!(key(a), key(b), "{a:?} vs {b:?}");
            }
        }
        let d01 = Bson::Decimal128(Decimal128::from_str("0.1").unwrap());
        assert_ne!(key(&d01), key(&Bson::Double(0.1)));
        assert_eq!(compare(&d01, &Bson::Double(0.1)), Ordering::Less);
        let order = [
            Bson::Double(f64::NAN),
            Bson::Double(f64::NEG_INFINITY),
            Bson::Int64(i64::MIN),
            Bson::Double(-1.5),
            Bson::Int32(-1),
            Bson::Double(-0.25),
            Bson::Double(-0.0),
            Bson::Double(1e-300),
            Bson::Double(0.12),
            Bson::Double(0.123),
            Bson::Int32(2),
            Bson::Int64(9_007_199_254_740_993),
            Bson::Decimal128(Decimal128::from_str("1E+400").unwrap()),
            Bson::Double(f64::INFINITY),
        ];
        for w in order.windows(2) {
            assert!(key(&w[0]) < key(&w[1]), "{:?} < {:?}", w[0], w[1]);
        }
        assert_eq!(key(&Bson::Double(-0.0)), key(&Bson::Int32(0)));
        // 2^53 + 1 isn't a double: the int64 and the nearest double differ.
        assert_ne!(
            key(&Bson::Int64(9_007_199_254_740_993)),
            key(&Bson::Double(9_007_199_254_740_992.0))
        );
    }

    #[test]
    fn classes_in_mongodb_order() {
        let order = [
            Bson::MinKey,
            Bson::Null,
            Bson::Int32(5),
            Bson::String(String::new()),
            Bson::String("a".into()),
            Bson::String("a\0".into()),
            Bson::String("ab".into()),
            Bson::Document(doc! {}),
            Bson::Document(doc! {"a": 1}),
            Bson::Array(vec![]),
            Bson::ObjectId(ObjectId::from_bytes([0; 12])),
            Bson::Boolean(false),
            Bson::Boolean(true),
            Bson::DateTime(bson::DateTime::from_millis(-5)),
            Bson::DateTime(bson::DateTime::from_millis(5)),
            Bson::MaxKey,
        ];
        for w in order.windows(2) {
            assert!(key(&w[0]) < key(&w[1]), "{:?} < {:?}", w[0], w[1]);
            assert_eq!(compare(&w[0], &w[1]), Ordering::Less);
        }
        assert!(equal(&Bson::Undefined, &Bson::Null));
        assert!(equal(&Bson::Symbol("x".into()), &Bson::String("x".into())));
    }

    #[test]
    fn exact_double_digits() {
        let Num::Finite { digits, exp, .. } = num_of_f64(0.1) else {
            panic!()
        };
        let s: String = digits.iter().map(|d| char::from(b'0' + d)).collect();
        assert_eq!(s, "1000000000000000055511151231257827021181583404541015625");
        assert_eq!(exp, 0);
        assert!(matches!(
            num_of_f64(f64::MIN_POSITIVE / 4.0),
            Num::Finite { .. }
        ));
        assert_eq!(decimal_parts("59.90"), Some((false, vec![5, 9, 9], 2)));
        assert_eq!(decimal_parts("-1.5E+10"), Some((true, vec![1, 5], 11)));
        assert_eq!(decimal_parts("0E-6176"), Some((false, vec![], 0)));
    }
}
