//! Comparison keys and values for analytic reads (chapter 17, "Analytic
//! reads (R3)"): what DataFusion compares, groups and sorts is never a
//! MariaDB value itself but a key whose plain byte or number order is
//! MariaDB's order for that column, and what it returns for a column is the
//! shard's own text bytes.
//!
//! | Column | Key | Exact because |
//! |---|---|---|
//! | Integers, `YEAR` | `Int64` (`BIGINT UNSIGNED`: `Decimal128(20, 0)`) | numbers |
//! | `DECIMAL(p, s)`, p ≤ 38 | `Decimal128(p, s)` | fixed point |
//! | Strings with a collation | the three `WEIGHT_STRING` levels, encoded ([`encode_weights`]) | MariaDB's own weights, `PAD SPACE` kept |
//! | `BINARY`, `VARBINARY`, `BLOB` | the bytes | byte order |
//! | `DATE`, `DATETIME(n)` | the text (fixed width per column) | text order is time order |
//! | `TIMESTAMP(n)` | `UNIX_TIMESTAMP(c)` as `Decimal128(20 + n, n)` | UTC instants, no daylight-saving folds |
//!
//! Every other type (`FLOAT`, `DOUBLE`, `TIME`, `ENUM`, `SET`, `BIT`,
//! `JSON`, geometry, …) can be passed through but not compared: a statement
//! that compares, groups, sorts or sums one is refused by the guard and
//! runs as a gathered read instead.

use std::cmp::Ordering;
use std::fmt::Write as _;

use sprig_proto_mysql::ColumnDef;
use sprig_proto_mysql::resultset::{ColumnFlags, column_type as ct};

use crate::collation;

/// How a column compares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// An integer that fits `i64` (every integer type but `BIGINT
    /// UNSIGNED`), or `YEAR`.
    Int,
    /// `BIGINT UNSIGNED`.
    BigUnsigned,
    /// `DECIMAL(p, s)`.
    Decimal {
        /// Precision.
        p: u8,
        /// Scale.
        s: u8,
    },
    /// A character string under collation `coll`.
    Str {
        /// The collation id.
        coll: u16,
    },
    /// Bytes compared as bytes.
    Bytes,
    /// `DATE` or `DATETIME(n)`: the text.
    Temporal {
        /// `DATE` (true) or `DATETIME` (false).
        date: bool,
        /// Fractional digits.
        fsp: u8,
    },
    /// `TIMESTAMP(n)`: the Unix time.
    Timestamp {
        /// Fractional digits.
        fsp: u8,
    },
    /// Passed through only.
    Opaque,
}

impl Kind {
    /// The kind of a shard column, from its definition and, for a
    /// character string, its own collation (`coll`, from
    /// `information_schema.COLUMNS`: a result's column definition carries
    /// the connection's collation, not the column's).
    pub(crate) fn of(c: &ColumnDef, coll: Option<u16>) -> Self {
        let flags = ColumnFlags::from_bits_truncate(c.flags);
        let unsigned = flags.contains(ColumnFlags::UNSIGNED);
        match c.col_type {
            ct::LONGLONG if unsigned => Kind::BigUnsigned,
            ct::TINY | ct::SHORT | ct::INT24 | ct::LONG | ct::YEAR | ct::LONGLONG => Kind::Int,
            ct::NEWDECIMAL | ct::DECIMAL => {
                let s = c.decimals;
                // Display length: precision, a point when s > 0, a sign
                // unless unsigned.
                let extra = u32::from(s > 0) + u32::from(!unsigned);
                let p = c.length.saturating_sub(extra);
                match u8::try_from(p) {
                    Ok(p) if (1..=38).contains(&p) && s <= p => Kind::Decimal { p, s },
                    _ => Kind::Opaque,
                }
            }
            ct::VARCHAR
            | ct::VAR_STRING
            | ct::STRING
            | ct::TINY_BLOB
            | ct::MEDIUM_BLOB
            | ct::LONG_BLOB
            | ct::BLOB => {
                if flags.contains(ColumnFlags::ENUM) || flags.contains(ColumnFlags::SET) {
                    Kind::Opaque
                } else if c.charset == 63 {
                    Kind::Bytes
                } else if let Some(coll) = coll.filter(|&k| collation::info(k).is_some()) {
                    Kind::Str { coll }
                } else {
                    Kind::Opaque
                }
            }
            ct::DATE | ct::NEWDATE => Kind::Temporal { date: true, fsp: 0 },
            ct::DATETIME | ct::DATETIME2 => Kind::Temporal {
                date: false,
                fsp: fsp_of(c),
            },
            ct::TIMESTAMP | ct::TIMESTAMP2 => Kind::Timestamp { fsp: fsp_of(c) },
            _ => Kind::Opaque,
        }
    }

    /// Whether the guard may compare, group or sort it.
    pub(crate) fn comparable(self) -> bool {
        !matches!(self, Kind::Opaque)
    }

    /// Whether `SUM` and `AVG` take it.
    pub(crate) fn numeric(self) -> bool {
        matches!(self, Kind::Int | Kind::BigUnsigned | Kind::Decimal { .. })
    }

    /// The scale of a number (0 for integers).
    pub(crate) fn scale(self) -> u8 {
        match self {
            Kind::Decimal { s, .. } => s,
            _ => 0,
        }
    }

    /// Digits before the point (for a common join type).
    fn int_digits(self) -> Option<u8> {
        match self {
            Kind::Int => Some(19),
            Kind::BigUnsigned => Some(20),
            Kind::Decimal { p, s } => Some(p - s),
            _ => None,
        }
    }

    /// What a shard selects for the key besides the value (the value
    /// itself serves the others): the three weight levels of a string, a
    /// `TIMESTAMP`'s Unix time.
    pub(crate) fn key_exprs(self, col: &str) -> Vec<String> {
        match self {
            Kind::Str { .. } => (1..=3)
                .map(|k| format!("WEIGHT_STRING({col} LEVEL {k})"))
                .collect(),
            Kind::Timestamp { .. } => vec![format!("UNIX_TIMESTAMP({col})")],
            _ => Vec::new(),
        }
    }
}

fn fsp_of(c: &ColumnDef) -> u8 {
    // MariaDB sends the fractional digits as `decimals` (39 or 31 when
    // unknown; never for a column).
    if c.decimals <= 6 { c.decimals } else { 0 }
}

/// The type both sides of a join equality compare in, as DataFusion SQL
/// casts them, or `None` when MariaDB's comparison isn't one the keys
/// reproduce (a string with a number, two collations, `DATE` with
/// `DATETIME`, …). `Some("")`: the keys compare as they are.
pub(crate) fn join_cast(a: Kind, b: Kind) -> Option<String> {
    match (a, b) {
        (Kind::Int, Kind::Int) => Some(String::new()),
        (x, y) if x.numeric() && y.numeric() => {
            let s = x.scale().max(y.scale());
            let int = x.int_digits()?.max(y.int_digits()?);
            let p = int.checked_add(s)?;
            (p <= 38).then(|| format!("DECIMAL({p},{s})"))
        }
        (Kind::Str { coll: x }, Kind::Str { coll: y }) if x == y => Some(String::new()),
        (Kind::Bytes, Kind::Bytes) => Some(String::new()),
        (Kind::Temporal { date: d1, fsp: f1 }, Kind::Temporal { date: d2, fsp: f2 })
            if d1 == d2 && f1 == f2 =>
        {
            Some(String::new())
        }
        (Kind::Timestamp { fsp: f1 }, Kind::Timestamp { fsp: f2 }) => {
            let s = f1.max(f2);
            Some(format!("DECIMAL({},{s})", 20 + s))
        }
        _ => None,
    }
}

/// One weight level as a byte string whose plain order is MariaDB's
/// `PAD SPACE` (or `NO PAD`) order for that level.
///
/// The weights are cut into units as wide as the space weight. Under
/// `PAD SPACE` a string compares as if followed by spaces forever, so
/// each run of `k` space units followed by a unit `x` becomes a token
/// `(class, k, x)`: class 0 when `x` sorts below a space (a shorter run
/// sorts first), class 2 when above (a longer run sorts first, so `k` is
/// stored inverted), and the end of the string is the token of class 1
/// (spaces forever). Equal strings, trailing spaces aside, give equal
/// bytes. Under `NO PAD` each unit is prefixed with 1 and the end is 0.
/// `None` when the weights aren't whole units.
pub(crate) fn encode_level(w: &[u8], pad_space: bool, space: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(w.len() * 2 + 8);
    if !pad_space || space.is_empty() {
        for &b in w {
            out.extend_from_slice(&[1, b]);
        }
        out.push(0);
        return Some(out);
    }
    let width = space.len();
    if !w.len().is_multiple_of(width) {
        return None;
    }
    let mut run: u32 = 0;
    for unit in w.chunks(width) {
        if unit == space {
            run = run.checked_add(1)?;
            continue;
        }
        let below = unit.cmp(space) == Ordering::Less;
        out.push(if below { 0 } else { 2 });
        let k = if below { run } else { u32::MAX - run };
        out.extend_from_slice(&k.to_be_bytes());
        out.extend_from_slice(unit);
        run = 0;
    }
    out.push(1);
    out.extend_from_slice(&[0; 4]);
    out.extend(std::iter::repeat_n(0, width));
    Some(out)
}

/// The key of a string from its three `WEIGHT_STRING` levels: the levels
/// encoded ([`encode_level`]) one after the other, so the first level
/// decides first. `None` for an unknown collation or broken weights.
pub(crate) fn encode_weights(coll: u16, levels: [&[u8]; 3]) -> Option<Vec<u8>> {
    let info = collation::info(coll)?;
    let mut out = Vec::new();
    for (k, w) in levels.iter().enumerate() {
        out.extend(encode_level(w, info.pad_space, info.space[k])?);
    }
    Some(out)
}

/// A decimal's text (`-12.30`, `7`) as an integer of `scale` digits after
/// the point, or `None` when it isn't one or has more digits.
pub(crate) fn parse_decimal(text: &[u8], scale: u8) -> Option<i128> {
    let t = std::str::from_utf8(text).ok()?.trim();
    let (neg, t) = match t.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let (int, frac) = t.split_once('.').unwrap_or((t, ""));
    if int.is_empty() && frac.is_empty() {
        return None;
    }
    if !int.bytes().all(|b| b.is_ascii_digit()) || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if frac.len() > usize::from(scale) {
        return None;
    }
    let mut v: i128 = 0;
    for b in int.bytes().chain(frac.bytes()) {
        v = v.checked_mul(10)?.checked_add(i128::from(b - b'0'))?;
    }
    for _ in frac.len()..usize::from(scale) {
        v = v.checked_mul(10)?;
    }
    Some(if neg { -v } else { v })
}

/// `v` with `scale` digits after the point, as MariaDB prints a `DECIMAL`.
pub(crate) fn decimal_text(v: i128, scale: u8) -> String {
    let neg = v < 0;
    let digits = v.unsigned_abs().to_string();
    let s = usize::from(scale);
    let mut out = String::with_capacity(digits.len() + 3);
    if neg {
        out.push('-');
    }
    if s == 0 {
        out.push_str(&digits);
        return out;
    }
    if digits.len() <= s {
        out.push_str("0.");
        for _ in digits.len()..s {
            out.push('0');
        }
        out.push_str(&digits);
    } else {
        let (a, b) = digits.split_at(digits.len() - s);
        let _ = write!(out, "{a}.{b}");
    }
    out
}

/// `sum / count` rounded half away from zero to `out_scale` digits, the
/// sum having `in_scale` digits: MariaDB's `AVG` of an integer or
/// `DECIMAL` (its scale is the input's plus `div_precision_increment`).
/// `None` on overflow or a zero count.
pub(crate) fn avg(sum: i128, in_scale: u8, count: i64, out_scale: u8) -> Option<i128> {
    if count <= 0 || out_scale < in_scale {
        return None;
    }
    let mut n = sum;
    for _ in in_scale..out_scale {
        n = n.checked_mul(10)?;
    }
    let d = i128::from(count);
    let q = n / d;
    let r = n % d;
    // Half away from zero: |r| * 2 >= d.
    if r.unsigned_abs().checked_mul(2)? >= d.unsigned_abs() {
        Some(if n < 0 { q - 1 } else { q + 1 })
    } else {
        Some(q)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn ws(s: &str) -> Vec<u8> {
        // utf8mb4_general_ci level-1 weights of ASCII letters, roughly:
        // upper case, two bytes per character.
        s.chars()
            .flat_map(|c| [0u8, c.to_ascii_uppercase() as u8])
            .collect()
    }

    #[test]
    fn pad_space_keys_order_and_equal_like_mariadb() {
        let sp = b"\x00\x20";
        let k = |s: &str| encode_level(&ws(s), true, sp).unwrap();
        // Trailing spaces don't count.
        assert_eq!(k("ab"), k("ab  "));
        assert_eq!(k("Ab"), k("aB"));
        // 'a' + tab sorts before 'a' (the tab is below a space), 'a b'
        // after it, 'a \t' before it.
        assert!(k("a\t") < k("a"));
        assert!(k("a") < k("a b"));
        assert!(k("a \t") < k("a"));
        assert!(k("a  \t") < k("a \t") || k("a  \t") > k("a \t"));
        // Against MariaDB's own comparison (collation::cmp_level).
        let words = [
            "", " ", "a", "a ", "a\t", "a \t", "a  \t", "a b", "ab", "b", "\t", "a  b",
        ];
        for x in words {
            for y in words {
                let want = collation::cmp_level(&ws(x), &ws(y), true, sp);
                assert_eq!(k(x).cmp(&k(y)), want, "{x:?} vs {y:?}");
            }
        }
    }

    #[test]
    fn no_pad_keys_put_prefixes_first() {
        let k = |s: &[u8]| encode_level(s, false, b" ").unwrap();
        assert!(k(b"a") < k(b"a "));
        assert!(k(b"a ") < k(b"ab"));
        assert!(k(b"") < k(b"\0"));
    }

    #[test]
    fn decimals_parse_print_and_average_like_mariadb() {
        assert_eq!(parse_decimal(b"-12.3", 2), Some(-1230));
        assert_eq!(parse_decimal(b"7", 0), Some(7));
        assert_eq!(parse_decimal(b"0.125", 2), None);
        assert_eq!(parse_decimal(b"x", 0), None);
        assert_eq!(decimal_text(-1230, 2), "-12.30");
        assert_eq!(decimal_text(5, 3), "0.005");
        assert_eq!(decimal_text(-5, 1), "-0.5");
        assert_eq!(decimal_text(0, 2), "0.00");
        // AVG(1, 2) = 1.5000; AVG(1, 1, 2) = 1.3333; half away from zero.
        assert_eq!(avg(3, 0, 2, 4), Some(15000));
        assert_eq!(avg(4, 0, 3, 4), Some(13333));
        assert_eq!(avg(5, 0, 3, 4), Some(16667));
        assert_eq!(avg(-5, 0, 3, 4), Some(-16667));
        assert_eq!(avg(1, 0, 0, 4), None);
    }

    #[test]
    fn join_types_follow_mysqls_comparison_rules() {
        assert_eq!(join_cast(Kind::Int, Kind::Int).as_deref(), Some(""));
        assert_eq!(
            join_cast(Kind::Int, Kind::Decimal { p: 10, s: 2 }).as_deref(),
            Some("DECIMAL(21,2)")
        );
        assert_eq!(
            join_cast(Kind::Str { coll: 45 }, Kind::Str { coll: 46 }),
            None
        );
        assert_eq!(join_cast(Kind::Str { coll: 45 }, Kind::Int), None);
        assert_eq!(
            join_cast(
                Kind::Temporal { date: true, fsp: 0 },
                Kind::Temporal {
                    date: false,
                    fsp: 0
                }
            ),
            None
        );
    }
}
