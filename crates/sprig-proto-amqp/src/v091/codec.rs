//! AMQP 0-9-1 primitive types: octets, shorts, longs, long-longs, bits
//! packed into octets, short and long strings, and field tables with
//! RabbitMQ's type tags (the 0-9-1 errata).
//!
//! | Tag | Type | Tag | Type |
//! |---|---|---|---|
//! | `t` | boolean | `f` | float |
//! | `b` | signed 8 | `d` | double |
//! | `B` | unsigned 8 | `D` | decimal |
//! | `s` | signed 16 | `S` | long string |
//! | `u` | unsigned 16 | `A` | array |
//! | `I` | signed 32 | `T` | timestamp |
//! | `i` | unsigned 32 | `F` | table |
//! | `l` | signed 64 | `V` | void |
//! | `x` | byte array | | |
//!
//! These are RabbitMQ's tags, not the 0-9-1 spec's (which has `s` for a
//! short string and `U`/`L` for signed 16/64): every RabbitMQ client uses
//! them.

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{MqError, reply};

/// A decoding error: malformed arguments (502 `syntax-error`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct DecodeError(pub String);

impl From<DecodeError> for MqError {
    fn from(e: DecodeError) -> Self {
        MqError::frame(reply::SYNTAX_ERROR, e.0)
    }
}

/// Decoding result.
pub type DResult<T> = Result<T, DecodeError>;

fn short(what: &str) -> DecodeError {
    DecodeError(format!("truncated {what}"))
}

/// An encoder that packs consecutive bits into octets, as 0-9-1 method
/// arguments require.
#[derive(Debug)]
pub struct Enc<'a> {
    buf: &'a mut BytesMut,
    /// The octet holding the current run of bits, and how many it holds.
    bits: Option<(usize, u8)>,
}

impl<'a> Enc<'a> {
    /// Appends to `buf`.
    pub fn new(buf: &'a mut BytesMut) -> Self {
        Self { buf, bits: None }
    }

    /// One bit.
    pub fn bit(&mut self, b: bool) {
        match self.bits {
            Some((at, n)) if n < 8 => {
                if b {
                    self.buf[at] |= 1 << n;
                }
                self.bits = Some((at, n + 1));
            }
            _ => {
                self.buf.put_u8(u8::from(b));
                self.bits = Some((self.buf.len() - 1, 1));
            }
        }
    }

    /// An octet.
    pub fn u8(&mut self, v: u8) {
        self.bits = None;
        self.buf.put_u8(v);
    }

    /// A short.
    pub fn u16(&mut self, v: u16) {
        self.bits = None;
        self.buf.put_u16(v);
    }

    /// A long.
    pub fn u32(&mut self, v: u32) {
        self.bits = None;
        self.buf.put_u32(v);
    }

    /// A long-long.
    pub fn u64(&mut self, v: u64) {
        self.bits = None;
        self.buf.put_u64(v);
    }

    /// A short string, cut to 255 bytes.
    pub fn shortstr(&mut self, s: &str) {
        self.bits = None;
        let b = s.as_bytes();
        let n = b.len().min(255);
        #[allow(clippy::cast_possible_truncation)]
        self.buf.put_u8(n as u8);
        self.buf.put_slice(&b[..n]);
    }

    /// A long string.
    pub fn longstr(&mut self, b: &[u8]) {
        self.bits = None;
        put_len(self.buf, b.len());
        self.buf.put_slice(b);
    }

    /// A field table.
    pub fn table(&mut self, t: &FieldTable) {
        self.bits = None;
        t.encode(self.buf);
    }
}

#[allow(clippy::cast_possible_truncation)]
fn put_len(buf: &mut BytesMut, n: usize) {
    buf.put_u32(n.min(u32::MAX as usize) as u32);
}

/// A decoder that unpacks bits the way [`Enc`] packs them.
#[derive(Debug)]
pub struct Dec<'a> {
    buf: &'a [u8],
    pos: usize,
    bits: Option<(u8, u8)>,
}

impl<'a> Dec<'a> {
    /// Reads `buf`.
    #[must_use]
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            pos: 0,
            bits: None,
        }
    }

    /// Bytes not read yet.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// The position.
    #[must_use]
    pub fn pos(&self) -> usize {
        self.pos
    }

    fn take(&mut self, n: usize, what: &str) -> DResult<&'a [u8]> {
        self.bits = None;
        if self.remaining() < n {
            return Err(short(what));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    /// One bit.
    ///
    /// # Errors
    /// Truncated input.
    pub fn bit(&mut self) -> DResult<bool> {
        match self.bits {
            Some((v, n)) if n < 8 => {
                self.bits = Some((v, n + 1));
                Ok(v & (1 << n) != 0)
            }
            _ => {
                let v = self.take(1, "bit")?[0];
                self.bits = Some((v, 1));
                Ok(v & 1 != 0)
            }
        }
    }

    /// An octet.
    ///
    /// # Errors
    /// Truncated input.
    pub fn u8(&mut self) -> DResult<u8> {
        Ok(self.take(1, "octet")?[0])
    }

    /// A short.
    ///
    /// # Errors
    /// Truncated input.
    pub fn u16(&mut self) -> DResult<u16> {
        let b = self.take(2, "short")?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    /// A long.
    ///
    /// # Errors
    /// Truncated input.
    pub fn u32(&mut self) -> DResult<u32> {
        let b = self.take(4, "long")?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// A long-long.
    ///
    /// # Errors
    /// Truncated input.
    pub fn u64(&mut self) -> DResult<u64> {
        let b = self.take(8, "long-long")?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(u64::from_be_bytes(a))
    }

    /// A short string's bytes.
    ///
    /// # Errors
    /// Truncated input.
    pub fn shortstr_bytes(&mut self) -> DResult<&'a [u8]> {
        let n = self.u8()? as usize;
        self.take(n, "short string")
    }

    /// A short string, which must be UTF-8.
    ///
    /// # Errors
    /// Truncated input or invalid UTF-8.
    pub fn shortstr(&mut self) -> DResult<String> {
        let b = self.shortstr_bytes()?;
        String::from_utf8(b.to_vec())
            .map_err(|_| DecodeError("short string is not valid UTF-8".into()))
    }

    /// A long string.
    ///
    /// # Errors
    /// Truncated input.
    pub fn longstr(&mut self) -> DResult<Bytes> {
        let n = self.u32()? as usize;
        Ok(Bytes::copy_from_slice(self.take(n, "long string")?))
    }

    /// A field table.
    ///
    /// # Errors
    /// Malformed table.
    pub fn table(&mut self) -> DResult<FieldTable> {
        let n = self.u32()? as usize;
        let body = self.take(n, "field table")?;
        FieldTable::decode_entries(body, 0)
    }
}

/// Field tables nest at most this deep (a decoding bomb guard).
pub const MAX_DEPTH: usize = 64;

/// A field value.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    /// `t`.
    Bool(bool),
    /// `b`.
    I8(i8),
    /// `B`.
    U8(u8),
    /// `s`.
    I16(i16),
    /// `u`.
    U16(u16),
    /// `I`.
    I32(i32),
    /// `i`.
    U32(u32),
    /// `l`.
    I64(i64),
    /// `f`.
    F32(f32),
    /// `d`.
    F64(f64),
    /// `D`: scale and unscaled value.
    Decimal(u8, u32),
    /// `S`.
    LongStr(Bytes),
    /// `A`.
    Array(Vec<FieldValue>),
    /// `T`: seconds since the epoch.
    Timestamp(u64),
    /// `F`.
    Table(FieldTable),
    /// `V`.
    Void,
    /// `x`.
    Bytes(Bytes),
}

impl FieldValue {
    /// A long string from text.
    #[must_use]
    pub fn str(s: &str) -> Self {
        FieldValue::LongStr(Bytes::copy_from_slice(s.as_bytes()))
    }

    /// The text of a long string (lossy), or `None`.
    #[must_use]
    pub fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        match self {
            FieldValue::LongStr(b) | FieldValue::Bytes(b) => Some(String::from_utf8_lossy(b)),
            _ => None,
        }
    }

    /// The value as a signed integer, if it is one.
    #[must_use]
    pub fn as_i64(&self) -> Option<i64> {
        Some(match *self {
            FieldValue::I8(v) => v.into(),
            FieldValue::U8(v) => v.into(),
            FieldValue::I16(v) => v.into(),
            FieldValue::U16(v) => v.into(),
            FieldValue::I32(v) => v.into(),
            FieldValue::U32(v) => v.into(),
            FieldValue::I64(v) => v,
            FieldValue::Timestamp(v) => i64::try_from(v).ok()?,
            _ => return None,
        })
    }

    /// The value as a boolean, if it is one.
    #[must_use]
    pub fn as_bool(&self) -> Option<bool> {
        match *self {
            FieldValue::Bool(b) => Some(b),
            _ => None,
        }
    }

    fn tag(&self) -> u8 {
        match self {
            FieldValue::Bool(_) => b't',
            FieldValue::I8(_) => b'b',
            FieldValue::U8(_) => b'B',
            FieldValue::I16(_) => b's',
            FieldValue::U16(_) => b'u',
            FieldValue::I32(_) => b'I',
            FieldValue::U32(_) => b'i',
            FieldValue::I64(_) => b'l',
            FieldValue::F32(_) => b'f',
            FieldValue::F64(_) => b'd',
            FieldValue::Decimal(..) => b'D',
            FieldValue::LongStr(_) => b'S',
            FieldValue::Array(_) => b'A',
            FieldValue::Timestamp(_) => b'T',
            FieldValue::Table(_) => b'F',
            FieldValue::Void => b'V',
            FieldValue::Bytes(_) => b'x',
        }
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.tag());
        match self {
            FieldValue::Bool(b) => buf.put_u8(u8::from(*b)),
            FieldValue::I8(v) => buf.put_i8(*v),
            FieldValue::U8(v) => buf.put_u8(*v),
            FieldValue::I16(v) => buf.put_i16(*v),
            FieldValue::U16(v) => buf.put_u16(*v),
            FieldValue::I32(v) => buf.put_i32(*v),
            FieldValue::U32(v) => buf.put_u32(*v),
            FieldValue::I64(v) => buf.put_i64(*v),
            FieldValue::F32(v) => buf.put_f32(*v),
            FieldValue::F64(v) => buf.put_f64(*v),
            FieldValue::Decimal(s, v) => {
                buf.put_u8(*s);
                buf.put_u32(*v);
            }
            FieldValue::LongStr(b) | FieldValue::Bytes(b) => {
                put_len(buf, b.len());
                buf.put_slice(b);
            }
            FieldValue::Array(a) => {
                let at = buf.len();
                buf.put_u32(0);
                for v in a {
                    v.encode(buf);
                }
                patch_len(buf, at);
            }
            FieldValue::Timestamp(v) => buf.put_u64(*v),
            FieldValue::Table(t) => t.encode(buf),
            FieldValue::Void => {}
        }
    }

    fn decode(d: &mut Dec<'_>, depth: usize) -> DResult<Self> {
        if depth > MAX_DEPTH {
            return Err(DecodeError("field table nested too deeply".into()));
        }
        let tag = d.u8()?;
        Ok(match tag {
            b't' => FieldValue::Bool(d.u8()? != 0),
            b'b' => FieldValue::I8(d.u8()?.cast_signed()),
            b'B' => FieldValue::U8(d.u8()?),
            b's' => FieldValue::I16(d.u16()?.cast_signed()),
            b'u' => FieldValue::U16(d.u16()?),
            b'I' => FieldValue::I32(d.u32()?.cast_signed()),
            b'i' => FieldValue::U32(d.u32()?),
            b'l' => FieldValue::I64(d.u64()?.cast_signed()),
            b'f' => FieldValue::F32(f32::from_bits(d.u32()?)),
            b'd' => FieldValue::F64(f64::from_bits(d.u64()?)),
            b'D' => FieldValue::Decimal(d.u8()?, d.u32()?),
            b'S' => FieldValue::LongStr(d.longstr()?),
            b'x' => FieldValue::Bytes(d.longstr()?),
            b'A' => {
                let n = d.u32()? as usize;
                let body = d.take(n, "field array")?;
                let mut inner = Dec::new(body);
                let mut out = Vec::new();
                while inner.remaining() > 0 {
                    out.push(FieldValue::decode(&mut inner, depth + 1)?);
                }
                FieldValue::Array(out)
            }
            b'T' => FieldValue::Timestamp(d.u64()?),
            b'F' => {
                let n = d.u32()? as usize;
                let body = d.take(n, "field table")?;
                FieldValue::Table(FieldTable::decode_entries(body, depth + 1)?)
            }
            b'V' => FieldValue::Void,
            other => {
                return Err(DecodeError(format!(
                    "unknown field value type '{}'",
                    char::from(other).escape_default()
                )));
            }
        })
    }
}

#[allow(clippy::cast_possible_truncation)]
fn patch_len(buf: &mut BytesMut, at: usize) {
    let n = (buf.len() - at - 4) as u32;
    buf[at..at + 4].copy_from_slice(&n.to_be_bytes());
}

/// A field table: ordered entries, so tables round-trip byte for byte.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FieldTable(pub Vec<(String, FieldValue)>);

impl FieldTable {
    /// An empty table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The value of `key`.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&FieldValue> {
        self.0.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    /// Sets `key`, replacing an earlier value.
    pub fn insert(&mut self, key: impl Into<String>, v: FieldValue) {
        let key = key.into();
        if let Some(e) = self.0.iter_mut().find(|(k, _)| *k == key) {
            e.1 = v;
        } else {
            self.0.push((key, v));
        }
    }

    /// Removes `key`.
    pub fn remove(&mut self, key: &str) -> Option<FieldValue> {
        let i = self.0.iter().position(|(k, _)| k == key)?;
        Some(self.0.remove(i).1)
    }

    /// No entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// A boolean entry, `false` when absent or of another type.
    #[must_use]
    pub fn flag(&self, key: &str) -> bool {
        self.get(key).and_then(FieldValue::as_bool).unwrap_or(false)
    }

    /// Encodes with its length prefix.
    pub fn encode(&self, buf: &mut BytesMut) {
        let at = buf.len();
        buf.put_u32(0);
        self.encode_entries(buf);
        patch_len(buf, at);
    }

    /// Encodes the entries without a length prefix (AMQPLAIN's response).
    pub fn encode_entries(&self, buf: &mut BytesMut) {
        for (k, v) in &self.0 {
            let kb = k.as_bytes();
            let n = kb.len().min(255);
            #[allow(clippy::cast_possible_truncation)]
            buf.put_u8(n as u8);
            buf.put_slice(&kb[..n]);
            v.encode(buf);
        }
    }

    /// Decodes entries that fill `body` (no length prefix).
    ///
    /// # Errors
    /// Malformed entries.
    pub fn decode_entries(body: &[u8], depth: usize) -> DResult<Self> {
        if depth > MAX_DEPTH {
            return Err(DecodeError("field table nested too deeply".into()));
        }
        let mut d = Dec::new(body);
        let mut out = Vec::new();
        while d.remaining() > 0 {
            let k = String::from_utf8_lossy(d.shortstr_bytes()?).into_owned();
            let v = FieldValue::decode(&mut d, depth)?;
            out.push((k, v));
        }
        Ok(FieldTable(out))
    }
}

impl<const N: usize> From<[(&str, FieldValue); N]> for FieldTable {
    fn from(a: [(&str, FieldValue); N]) -> Self {
        FieldTable(a.into_iter().map(|(k, v)| (k.to_owned(), v)).collect())
    }
}

/// A method argument type.
pub trait Arg: Sized {
    /// Writes it.
    fn put(&self, e: &mut Enc<'_>);
    /// Reads it.
    ///
    /// # Errors
    /// Malformed input.
    fn get(d: &mut Dec<'_>) -> DResult<Self>;
}

impl Arg for bool {
    fn put(&self, e: &mut Enc<'_>) {
        e.bit(*self);
    }
    fn get(d: &mut Dec<'_>) -> DResult<Self> {
        d.bit()
    }
}
impl Arg for u8 {
    fn put(&self, e: &mut Enc<'_>) {
        e.u8(*self);
    }
    fn get(d: &mut Dec<'_>) -> DResult<Self> {
        d.u8()
    }
}
impl Arg for u16 {
    fn put(&self, e: &mut Enc<'_>) {
        e.u16(*self);
    }
    fn get(d: &mut Dec<'_>) -> DResult<Self> {
        d.u16()
    }
}
impl Arg for u32 {
    fn put(&self, e: &mut Enc<'_>) {
        e.u32(*self);
    }
    fn get(d: &mut Dec<'_>) -> DResult<Self> {
        d.u32()
    }
}
impl Arg for u64 {
    fn put(&self, e: &mut Enc<'_>) {
        e.u64(*self);
    }
    fn get(d: &mut Dec<'_>) -> DResult<Self> {
        d.u64()
    }
}
/// A short string.
impl Arg for String {
    fn put(&self, e: &mut Enc<'_>) {
        e.shortstr(self);
    }
    fn get(d: &mut Dec<'_>) -> DResult<Self> {
        d.shortstr()
    }
}
/// A long string.
impl Arg for Bytes {
    fn put(&self, e: &mut Enc<'_>) {
        e.longstr(self);
    }
    fn get(d: &mut Dec<'_>) -> DResult<Self> {
        d.longstr()
    }
}
impl Arg for FieldTable {
    fn put(&self, e: &mut Enc<'_>) {
        e.table(self);
    }
    fn get(d: &mut Dec<'_>) -> DResult<Self> {
        d.table()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bits_pack_into_one_octet() {
        let mut b = BytesMut::new();
        let mut e = Enc::new(&mut b);
        e.u16(0);
        for v in [true, false, true, true, false] {
            e.bit(v);
        }
        e.u8(7);
        e.bit(true);
        assert_eq!(&b[..], &[0, 0, 0b0000_1101, 7, 1]);
        let mut d = Dec::new(&b);
        assert_eq!(d.u16().unwrap(), 0);
        let got: Vec<bool> = (0..5).map(|_| d.bit().unwrap()).collect();
        assert_eq!(got, [true, false, true, true, false]);
        assert_eq!(d.u8().unwrap(), 7);
        assert!(d.bit().unwrap());
        assert_eq!(d.remaining(), 0);
    }

    #[test]
    fn nine_bits_take_two_octets() {
        let mut b = BytesMut::new();
        let mut e = Enc::new(&mut b);
        for _ in 0..9 {
            e.bit(true);
        }
        assert_eq!(&b[..], &[0xff, 0x01]);
    }

    #[test]
    fn table_golden_bytes() {
        // {"a": S "b", "n": I 1}: RabbitMQ's encoding.
        let t = FieldTable::from([("a", FieldValue::str("b")), ("n", FieldValue::I32(1))]);
        let mut b = BytesMut::new();
        t.encode(&mut b);
        let want: &[u8] = &[
            0, 0, 0, 15, // length
            1, b'a', b'S', 0, 0, 0, 1, b'b', // a = "b"
            1, b'n', b'I', 0, 0, 0, 1, // n = 1
        ];
        assert_eq!(&b[..], want);
        let mut d = Dec::new(&b);
        assert_eq!(d.table().unwrap(), t);
    }

    #[test]
    fn every_type_round_trips() {
        let t = FieldTable::from([
            ("t", FieldValue::Bool(true)),
            ("b", FieldValue::I8(-2)),
            ("B", FieldValue::U8(250)),
            ("s", FieldValue::I16(-300)),
            ("u", FieldValue::U16(60000)),
            ("I", FieldValue::I32(-70000)),
            ("i", FieldValue::U32(4_000_000_000)),
            ("l", FieldValue::I64(-1 << 40)),
            ("f", FieldValue::F32(1.5)),
            ("d", FieldValue::F64(-2.25)),
            ("D", FieldValue::Decimal(2, 12345)),
            ("S", FieldValue::str("héllo")),
            (
                "A",
                FieldValue::Array(vec![FieldValue::I32(1), FieldValue::str("x")]),
            ),
            ("T", FieldValue::Timestamp(1_700_000_000)),
            (
                "F",
                FieldValue::Table(FieldTable::from([("k", FieldValue::Void)])),
            ),
            ("V", FieldValue::Void),
            ("x", FieldValue::Bytes(Bytes::from_static(&[0, 1, 2]))),
        ]);
        let mut b = BytesMut::new();
        t.encode(&mut b);
        assert_eq!(Dec::new(&b).table().unwrap(), t);
    }

    #[test]
    fn bad_tables_are_refused() {
        // Unknown tag.
        assert!(Dec::new(&[0, 0, 0, 3, 1, b'k', b'Z']).table().is_err());
        // Length beyond the input.
        assert!(Dec::new(&[0, 0, 0, 9, 1, b'k']).table().is_err());
        // Deep nesting.
        let mut v = FieldValue::Void;
        for _ in 0..100 {
            v = FieldValue::Array(vec![v]);
        }
        let mut b = BytesMut::new();
        FieldTable::from([("k", v)]).encode(&mut b);
        assert!(Dec::new(&b).table().is_err());
    }
}
