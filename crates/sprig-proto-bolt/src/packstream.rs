//! PackStream version 1: the
//! value encoding inside Bolt messages.
//!
//! | Marker | Value |
//! |---|---|
//! | `C0` | null |
//! | `C2`, `C3` | false, true |
//! | `C1` + 8 bytes | float, IEEE 754 big-endian |
//! | `F0`..`7F`, `C8`..`CB` | integers: tiny, then 8, 16, 32, 64 bits |
//! | `CC`..`CE` | bytes, with an 8-, 16- or 32-bit size |
//! | `80`..`8F`, `D0`..`D2` | strings (UTF-8): tiny, then 8-, 16-, 32-bit size |
//! | `90`..`9F`, `D4`..`D6` | lists |
//! | `A0`..`AF`, `D8`..`DA` | maps (string keys) |
//! | `B0`..`BF` + tag | structures of up to 15 fields |
//!
//! Integers always take the smallest form, as the specification asks.
//! Decoding is bounded: nesting at most [`MAX_DEPTH`] levels, sizes checked
//! against the bytes left, so malformed input fails cleanly.

use std::fmt;

use bytes::{BufMut, BytesMut};

/// Deepest nesting a decoder accepts.
pub const MAX_DEPTH: usize = 64;

/// A PackStream value.
#[derive(Debug, Clone, PartialEq)]
pub enum PackValue {
    /// `null`.
    Null,
    /// A boolean.
    Bool(bool),
    /// A 64-bit signed integer.
    Int(i64),
    /// A 64-bit float.
    Float(f64),
    /// A byte array.
    Bytes(Vec<u8>),
    /// A UTF-8 string.
    String(String),
    /// A list.
    List(Vec<PackValue>),
    /// A map with string keys, in wire order.
    Map(Vec<(String, PackValue)>),
    /// A structure: a tag byte and up to 15 fields.
    Struct {
        /// The tag (`4E` Node, `52` Relationship, a message type, ...).
        tag: u8,
        /// The fields.
        fields: Vec<PackValue>,
    },
}

impl PackValue {
    /// A string value.
    #[must_use]
    pub fn str(s: impl Into<String>) -> Self {
        PackValue::String(s.into())
    }

    /// A map from pairs.
    #[must_use]
    pub fn map<K: Into<String>>(pairs: impl IntoIterator<Item = (K, PackValue)>) -> Self {
        PackValue::Map(pairs.into_iter().map(|(k, v)| (k.into(), v)).collect())
    }

    /// The value under `key` of a map.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&PackValue> {
        match self {
            PackValue::Map(m) => m.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    /// The string, if this is one.
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            PackValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// The integer, if this is one.
    #[must_use]
    pub fn as_int(&self) -> Option<i64> {
        match self {
            PackValue::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// The pairs, if this is a map.
    #[must_use]
    pub fn as_map(&self) -> Option<&[(String, PackValue)]> {
        match self {
            PackValue::Map(m) => Some(m),
            _ => None,
        }
    }

    /// The items, if this is a list.
    #[must_use]
    pub fn as_list(&self) -> Option<&[PackValue]> {
        match self {
            PackValue::List(l) => Some(l),
            _ => None,
        }
    }

    /// Bytes this value takes encoded (an upper bound for budgets is
    /// enough, but this is exact).
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        let mut b = BytesMut::new();
        encode(self, &mut b);
        b.len()
    }
}

/// A malformed PackStream value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackError(pub String);

impl fmt::Display for PackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PackStream: {}", self.0)
    }
}

impl std::error::Error for PackError {}

fn size_header(out: &mut BytesMut, n: usize, tiny: u8, m8: u8, m16: u8, m32: u8) {
    if n < 16 && tiny != 0 {
        out.put_u8(tiny | u8::try_from(n).unwrap_or(0));
    } else if let Ok(n8) = u8::try_from(n) {
        out.put_u8(m8);
        out.put_u8(n8);
    } else if let Ok(n16) = u16::try_from(n) {
        out.put_u8(m16);
        out.put_u16(n16);
    } else {
        out.put_u8(m32);
        out.put_u32(u32::try_from(n).unwrap_or(u32::MAX));
    }
}

/// Appends `v` to `out`.
pub fn encode(v: &PackValue, out: &mut BytesMut) {
    match v {
        PackValue::Null => out.put_u8(0xC0),
        PackValue::Bool(false) => out.put_u8(0xC2),
        PackValue::Bool(true) => out.put_u8(0xC3),
        PackValue::Int(i) => encode_int(*i, out),
        PackValue::Float(f) => {
            out.put_u8(0xC1);
            out.put_f64(*f);
        }
        PackValue::Bytes(b) => {
            size_header(out, b.len(), 0, 0xCC, 0xCD, 0xCE);
            out.put_slice(b);
        }
        PackValue::String(s) => {
            size_header(out, s.len(), 0x80, 0xD0, 0xD1, 0xD2);
            out.put_slice(s.as_bytes());
        }
        PackValue::List(l) => {
            size_header(out, l.len(), 0x90, 0xD4, 0xD5, 0xD6);
            for x in l {
                encode(x, out);
            }
        }
        PackValue::Map(m) => {
            size_header(out, m.len(), 0xA0, 0xD8, 0xD9, 0xDA);
            for (k, x) in m {
                size_header(out, k.len(), 0x80, 0xD0, 0xD1, 0xD2);
                out.put_slice(k.as_bytes());
                encode(x, out);
            }
        }
        PackValue::Struct { tag, fields } => {
            out.put_u8(0xB0 | u8::try_from(fields.len().min(15)).unwrap_or(15));
            out.put_u8(*tag);
            for x in fields.iter().take(15) {
                encode(x, out);
            }
        }
    }
}

fn encode_int(i: i64, out: &mut BytesMut) {
    if (-16..=127).contains(&i) {
        // Two's complement in one byte: F0..FF for -16..-1.
        out.put_i8(i8::try_from(i).unwrap_or(0));
    } else if let Ok(b) = i8::try_from(i) {
        out.put_u8(0xC8);
        out.put_i8(b);
    } else if let Ok(s) = i16::try_from(i) {
        out.put_u8(0xC9);
        out.put_i16(s);
    } else if let Ok(w) = i32::try_from(i) {
        out.put_u8(0xCA);
        out.put_i32(w);
    } else {
        out.put_u8(0xCB);
        out.put_i64(i);
    }
}

/// A cursor over bytes being decoded.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], PackError> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|e| *e <= self.buf.len())
            .ok_or_else(|| PackError(format!("{n} bytes needed at offset {}", self.pos)))?;
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8, PackError> {
        Ok(self.take(1)?[0])
    }

    fn be<const N: usize>(&mut self) -> Result<[u8; N], PackError> {
        let s = self.take(N)?;
        let mut a = [0u8; N];
        a.copy_from_slice(s);
        Ok(a)
    }

    fn size(&mut self, marker: u8, m8: u8, m16: u8, m32: u8) -> Result<usize, PackError> {
        let n = if marker == m8 {
            usize::from(self.u8()?)
        } else if marker == m16 {
            usize::from(u16::from_be_bytes(self.be()?))
        } else if marker == m32 {
            usize::try_from(u32::from_be_bytes(self.be()?)).unwrap_or(usize::MAX)
        } else {
            return Err(PackError(format!("bad size marker {marker:#04X}")));
        };
        Ok(n)
    }

    fn string(&mut self, n: usize) -> Result<String, PackError> {
        let s = self.take(n)?;
        String::from_utf8(s.to_vec()).map_err(|_| PackError("string is not UTF-8".into()))
    }

    /// At least `n` more bytes must remain for `n` items of one byte each.
    fn check_items(&self, n: usize) -> Result<(), PackError> {
        if n > self.buf.len() - self.pos {
            return Err(PackError(format!(
                "{n} items announced, {} bytes left",
                self.buf.len() - self.pos
            )));
        }
        Ok(())
    }

    fn value(&mut self, depth: usize) -> Result<PackValue, PackError> {
        if depth > MAX_DEPTH {
            return Err(PackError(format!("nested deeper than {MAX_DEPTH}")));
        }
        let m = self.u8()?;
        Ok(match m {
            0x00..=0x7F => PackValue::Int(i64::from(m)),
            0xF0..=0xFF => PackValue::Int(i64::from(i8::from_be_bytes([m]))),
            0x80..=0x8F => PackValue::String(self.string(usize::from(m & 0x0F))?),
            0x90..=0x9F => self.list(usize::from(m & 0x0F), depth)?,
            0xA0..=0xAF => self.map(usize::from(m & 0x0F), depth)?,
            0xB0..=0xBF => {
                let n = usize::from(m & 0x0F);
                let tag = self.u8()?;
                self.check_items(n)?;
                let mut fields = Vec::with_capacity(n);
                for _ in 0..n {
                    fields.push(self.value(depth + 1)?);
                }
                PackValue::Struct { tag, fields }
            }
            0xC0 => PackValue::Null,
            0xC1 => PackValue::Float(f64::from_be_bytes(self.be()?)),
            0xC2 => PackValue::Bool(false),
            0xC3 => PackValue::Bool(true),
            0xC8 => PackValue::Int(i64::from(i8::from_be_bytes(self.be()?))),
            0xC9 => PackValue::Int(i64::from(i16::from_be_bytes(self.be()?))),
            0xCA => PackValue::Int(i64::from(i32::from_be_bytes(self.be()?))),
            0xCB => PackValue::Int(i64::from_be_bytes(self.be()?)),
            0xCC..=0xCE => {
                let n = self.size(m, 0xCC, 0xCD, 0xCE)?;
                PackValue::Bytes(self.take(n)?.to_vec())
            }
            0xD0..=0xD2 => {
                let n = self.size(m, 0xD0, 0xD1, 0xD2)?;
                PackValue::String(self.string(n)?)
            }
            0xD4..=0xD6 => {
                let n = self.size(m, 0xD4, 0xD5, 0xD6)?;
                self.list(n, depth)?
            }
            0xD8..=0xDA => {
                let n = self.size(m, 0xD8, 0xD9, 0xDA)?;
                self.map(n, depth)?
            }
            _ => return Err(PackError(format!("unknown marker {m:#04X}"))),
        })
    }

    fn list(&mut self, n: usize, depth: usize) -> Result<PackValue, PackError> {
        self.check_items(n)?;
        let mut l = Vec::with_capacity(n);
        for _ in 0..n {
            l.push(self.value(depth + 1)?);
        }
        Ok(PackValue::List(l))
    }

    fn map(&mut self, n: usize, depth: usize) -> Result<PackValue, PackError> {
        self.check_items(n.saturating_mul(2))?;
        let mut m = Vec::with_capacity(n);
        for _ in 0..n {
            let k = match self.value(depth + 1)? {
                PackValue::String(s) => s,
                other => return Err(PackError(format!("map key is not a string: {other:?}"))),
            };
            m.push((k, self.value(depth + 1)?));
        }
        Ok(PackValue::Map(m))
    }
}

/// Decodes exactly one value from `buf`.
///
/// # Errors
/// [`PackError`] for truncated or malformed input, or trailing bytes.
pub fn decode(buf: &[u8]) -> Result<PackValue, PackError> {
    let mut r = Reader { buf, pos: 0 };
    let v = r.value(0)?;
    if r.pos != buf.len() {
        return Err(PackError(format!(
            "{} trailing bytes after the value",
            buf.len() - r.pos
        )));
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(v: &PackValue) -> Vec<u8> {
        let mut b = BytesMut::new();
        encode(v, &mut b);
        b.to_vec()
    }

    fn hex(s: &str) -> Vec<u8> {
        s.split_whitespace()
            .map(|h| u8::from_str_radix(h, 16).expect("hex"))
            .collect()
    }

    /// The examples of the PackStream specification, both ways.
    #[test]
    fn golden_bytes_from_the_specification() {
        let cases: Vec<(PackValue, &str)> = vec![
            (PackValue::Null, "C0"),
            (PackValue::Bool(true), "C3"),
            (PackValue::Bool(false), "C2"),
            (PackValue::Int(1), "01"),
            (PackValue::Int(42), "2A"),
            (PackValue::Int(127), "7F"),
            (PackValue::Int(-16), "F0"),
            (PackValue::Int(-1), "FF"),
            (PackValue::Int(-17), "C8 EF"),
            (PackValue::Int(-128), "C8 80"),
            (PackValue::Int(128), "C9 00 80"),
            (PackValue::Int(-129), "C9 FF 7F"),
            (PackValue::Int(1234), "C9 04 D2"),
            (PackValue::Int(32_768), "CA 00 00 80 00"),
            (PackValue::Int(-2_147_483_648), "CA 80 00 00 00"),
            (PackValue::Int(2_147_483_648), "CB 00 00 00 00 80 00 00 00"),
            (PackValue::Int(i64::MIN), "CB 80 00 00 00 00 00 00 00"),
            (PackValue::Float(1.23), "C1 3F F3 AE 14 7A E1 47 AE"),
            (PackValue::Float(-1.1), "C1 BF F1 99 99 99 99 99 9A"),
            (PackValue::Bytes(vec![1, 2, 3]), "CC 03 01 02 03"),
            (PackValue::str(""), "80"),
            (PackValue::str("A"), "81 41"),
            (
                PackValue::str("Größenmaßstäbe"),
                "D0 12 47 72 C3 B6 C3 9F 65 6E 6D 61 C3 9F 73 74 C3 A4 62 65",
            ),
            (PackValue::List(vec![]), "90"),
            (
                PackValue::List(vec![
                    PackValue::Int(1),
                    PackValue::Int(2),
                    PackValue::Int(3),
                ]),
                "93 01 02 03",
            ),
            (PackValue::Map(vec![]), "A0"),
            (
                PackValue::map([("one", PackValue::str("eins"))]),
                "A1 83 6F 6E 65 84 65 69 6E 73",
            ),
            (
                PackValue::Struct {
                    tag: 0x70,
                    fields: vec![PackValue::Map(vec![])],
                },
                "B1 70 A0",
            ),
        ];
        for (v, h) in cases {
            assert_eq!(enc(&v), hex(h), "encode {v:?}");
            assert_eq!(decode(&hex(h)).expect("decode"), v, "decode {h}");
        }
    }

    #[test]
    fn long_strings_lists_and_maps_use_sized_markers() {
        let s = "x".repeat(16);
        assert_eq!(&enc(&PackValue::str(s.clone()))[..2], &[0xD0, 16]);
        let s = "x".repeat(256);
        assert_eq!(&enc(&PackValue::str(s.clone()))[..3], &[0xD1, 1, 0]);
        let s = "x".repeat(65_536);
        assert_eq!(&enc(&PackValue::str(s.clone()))[..5], &[0xD2, 0, 1, 0, 0]);
        let l = PackValue::List((0..20).map(PackValue::Int).collect());
        assert_eq!(&enc(&l)[..2], &[0xD4, 20]);
        assert_eq!(decode(&enc(&l)).expect("decode"), l);
        let m = PackValue::Map(
            (0..300)
                .map(|i| (format!("k{i}"), PackValue::Int(i)))
                .collect(),
        );
        assert_eq!(&enc(&m)[..3], &[0xD9, 1, 0x2C]);
        assert_eq!(decode(&enc(&m)).expect("decode"), m);
        // Integers decode from any width, as the specification allows.
        assert_eq!(
            decode(&hex("CB 00 00 00 00 00 00 00 01")).expect("decode"),
            PackValue::Int(1)
        );
    }

    #[test]
    fn malformed_input_fails_cleanly() {
        for bad in [
            "",
            "C1 00",
            "D0 05 41",
            "93 01 02",
            "A1 01 02",
            "D6 FF FF FF FF",
            "DA FF FF FF FF",
            "C4",
            "82 C3 28",
            "01 02",
            "B2 4E 01",
        ] {
            assert!(decode(&hex(bad)).is_err(), "{bad}");
        }
        // Too deep.
        let mut deep = vec![0x91; MAX_DEPTH + 2];
        deep.push(0xC0);
        assert!(decode(&deep).is_err());
    }

    /// A small fuzz: random byte strings never panic, and every value
    /// decoded re-encodes to bytes that decode to the same value.
    #[test]
    fn random_bytes_never_panic() {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..50_000 {
            let len = usize::try_from(next() % 24).unwrap_or(0);
            let bytes: Vec<u8> = (0..len)
                .map(|_| u8::try_from(next() & 0xFF).unwrap_or(0))
                .collect();
            if let Ok(v) = decode(&bytes) {
                let again = decode(&enc(&v)).expect("re-decode");
                // NaN floats aren't equal to themselves.
                if !format!("{v:?}").contains("NaN") {
                    assert_eq!(again, v);
                }
            }
        }
    }
}
