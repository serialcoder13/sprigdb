//! The Kafka protocol's primitive types (the protocol guide's "Protocol
//! Primitive Types"): fixed-width big-endian integers, zig-zag and unsigned
//! varints, strings, bytes and arrays in their classic (`INT16`/`INT32`
//! length) and compact (`UNSIGNED_VARINT` length + 1) forms, UUIDs and
//! tagged fields.
//!
//! [`Reader`] never panics on any input: every length is checked against the
//! bytes left before anything is allocated, as Kafka's own decoder does, so
//! a hostile length can't make the gateway allocate more than the frame it
//! already holds.

use uuid::Uuid;

/// A codec error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    /// The input ended inside a value.
    #[error("message truncated")]
    Truncated,
    /// A malformed value: a bad varint, a negative length, invalid UTF-8,
    /// a length larger than what is left.
    #[error("invalid {0}")]
    Invalid(&'static str),
    /// A null in a field that isn't nullable in this version.
    #[error("{0} is null in a version where it can't be")]
    NullNotAllowed(&'static str),
    /// A request version the codec doesn't have.
    #[error("version {version} of API {api} is not supported")]
    UnsupportedVersion {
        /// The API key.
        api: i16,
        /// The version asked for.
        version: i16,
    },
    /// A frame above the listener's limit.
    #[error("frame of {0} bytes is larger than allowed")]
    FrameTooLarge(usize),
    /// A record batch compressed with a codec SprigDB doesn't have
    /// (snappy, LZ4), or an unknown one.
    #[error("compression codec {0} is not supported")]
    UnsupportedCompression(u8),
    /// A record batch that fails its checks (CRC32C, lengths, magic).
    #[error("corrupt record batch: {0}")]
    CorruptRecord(&'static str),
}

/// The codec's result.
pub type Result<T, E = CodecError> = std::result::Result<T, E>;

/// A tagged field the codec doesn't know, kept so it survives a decode and
/// re-encode.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RawTaggedField {
    /// The tag.
    pub tag: u32,
    /// The field's bytes.
    pub data: Vec<u8>,
}

/// A cursor over one message's bytes.
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

macro_rules! fixed {
    ($name:ident, $t:ty, $n:literal) => {
        #[doc = concat!("Reads a big-endian `", stringify!($t), "`.")]
        ///
        /// # Errors
        /// [`CodecError::Truncated`].
        pub fn $name(&mut self) -> Result<$t> {
            let b = self.take($n)?;
            let mut a = [0u8; $n];
            a.copy_from_slice(b);
            Ok(<$t>::from_be_bytes(a))
        }
    };
}

impl<'a> Reader<'a> {
    /// A reader at the start of `buf`.
    #[must_use]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// Bytes not yet read.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Whether every byte has been read.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    /// How far the reader is into its buffer.
    #[must_use]
    pub fn position(&self) -> usize {
        self.pos
    }

    /// The unread bytes, without consuming them.
    #[must_use]
    pub fn rest(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }

    /// Takes `n` bytes.
    ///
    /// # Errors
    /// [`CodecError::Truncated`].
    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.remaining() {
            return Err(CodecError::Truncated);
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fixed!(i8, i8, 1);
    fixed!(i16, i16, 2);
    fixed!(u16, u16, 2);
    fixed!(i32, i32, 4);
    fixed!(u32, u32, 4);
    fixed!(i64, i64, 8);

    /// Reads a `FLOAT64`.
    ///
    /// # Errors
    /// [`CodecError::Truncated`].
    pub fn f64(&mut self) -> Result<f64> {
        Ok(f64::from_bits(self.i64()?.cast_unsigned()))
    }

    /// Reads a `BOOLEAN` (any non-zero byte is true, as in Kafka).
    ///
    /// # Errors
    /// [`CodecError::Truncated`].
    pub fn bool(&mut self) -> Result<bool> {
        Ok(self.i8()? != 0)
    }

    /// Reads a `UUID`.
    ///
    /// # Errors
    /// [`CodecError::Truncated`].
    pub fn uuid(&mut self) -> Result<Uuid> {
        let b = self.take(16)?;
        let mut a = [0u8; 16];
        a.copy_from_slice(b);
        Ok(Uuid::from_bytes(a))
    }

    /// Reads an `UNSIGNED_VARINT` (at most 5 bytes).
    ///
    /// # Errors
    /// [`CodecError::Truncated`]; `Invalid` for a varint longer than 5
    /// bytes.
    pub fn uvarint(&mut self) -> Result<u32> {
        let mut v: u32 = 0;
        for i in 0..5 {
            let b = self.take(1)?[0];
            if i == 4 && b > 0x0f {
                return Err(CodecError::Invalid("varint"));
            }
            v |= u32::from(b & 0x7f) << (7 * i);
            if b & 0x80 == 0 {
                return Ok(v);
            }
        }
        Err(CodecError::Invalid("varint"))
    }

    /// Reads an unsigned varlong (at most 10 bytes).
    ///
    /// # Errors
    /// [`CodecError::Truncated`]; `Invalid` for a varlong longer than 10
    /// bytes.
    pub fn uvarlong(&mut self) -> Result<u64> {
        let mut v: u64 = 0;
        for i in 0..10 {
            let b = self.take(1)?[0];
            if i == 9 && b > 0x01 {
                return Err(CodecError::Invalid("varlong"));
            }
            v |= u64::from(b & 0x7f) << (7 * i);
            if b & 0x80 == 0 {
                return Ok(v);
            }
        }
        Err(CodecError::Invalid("varlong"))
    }

    /// Reads a zig-zag `VARINT`.
    ///
    /// # Errors
    /// As [`Reader::uvarint`].
    pub fn varint(&mut self) -> Result<i32> {
        let u = self.uvarint()?;
        Ok((u >> 1).cast_signed() ^ -((u & 1).cast_signed()))
    }

    /// Reads a zig-zag `VARLONG`.
    ///
    /// # Errors
    /// As [`Reader::uvarlong`].
    pub fn varlong(&mut self) -> Result<i64> {
        let u = self.uvarlong()?;
        Ok((u >> 1).cast_signed() ^ -((u & 1).cast_signed()))
    }

    /// A length prefix: `None` for null. Classic strings use `INT16`,
    /// classic bytes and arrays `INT32`, compact ones `UNSIGNED_VARINT`
    /// length + 1. The length must fit in what is left.
    fn len(&mut self, compact: bool, wide: bool, what: &'static str) -> Result<Option<usize>> {
        let n: i64 = if compact {
            i64::from(self.uvarint()?) - 1
        } else if wide {
            i64::from(self.i32()?)
        } else {
            i64::from(self.i16()?)
        };
        if n == -1 {
            return Ok(None);
        }
        let n = usize::try_from(n).map_err(|_| CodecError::Invalid(what))?;
        if n > self.remaining() {
            return Err(CodecError::Invalid(what));
        }
        Ok(Some(n))
    }

    /// Reads a non-null `STRING` or `COMPACT_STRING`.
    ///
    /// # Errors
    /// `Truncated`, `Invalid`, or `NullNotAllowed` for a null.
    pub fn string(&mut self, compact: bool) -> Result<String> {
        self.nullable_string(compact)?
            .ok_or(CodecError::NullNotAllowed("string"))
    }

    /// Reads a `NULLABLE_STRING` or `COMPACT_NULLABLE_STRING`.
    ///
    /// # Errors
    /// `Truncated`; `Invalid` for a bad length or invalid UTF-8.
    pub fn nullable_string(&mut self, compact: bool) -> Result<Option<String>> {
        match self.len(compact, false, "string length")? {
            None => Ok(None),
            Some(n) => {
                let b = self.take(n)?;
                String::from_utf8(b.to_vec())
                    .map(Some)
                    .map_err(|_| CodecError::Invalid("UTF-8 string"))
            }
        }
    }

    /// Reads non-null `BYTES` or `COMPACT_BYTES`.
    ///
    /// # Errors
    /// `Truncated`, `Invalid`, or `NullNotAllowed` for a null.
    pub fn bytes(&mut self, compact: bool) -> Result<Vec<u8>> {
        self.nullable_bytes(compact)?
            .ok_or(CodecError::NullNotAllowed("bytes"))
    }

    /// Reads `NULLABLE_BYTES` or `COMPACT_NULLABLE_BYTES`.
    ///
    /// # Errors
    /// `Truncated`; `Invalid` for a bad length.
    pub fn nullable_bytes(&mut self, compact: bool) -> Result<Option<Vec<u8>>> {
        match self.len(compact, true, "bytes length")? {
            None => Ok(None),
            Some(n) => Ok(Some(self.take(n)?.to_vec())),
        }
    }

    /// Reads a non-null `ARRAY` or `COMPACT_ARRAY`, each element with `f`.
    ///
    /// # Errors
    /// `Truncated`, `Invalid`, `NullNotAllowed`, or `f`'s.
    pub fn array<T>(
        &mut self,
        compact: bool,
        f: impl FnMut(&mut Self) -> Result<T>,
    ) -> Result<Vec<T>> {
        self.nullable_array(compact, f)?
            .ok_or(CodecError::NullNotAllowed("array"))
    }

    /// Reads a nullable `ARRAY` or `COMPACT_ARRAY`.
    ///
    /// # Errors
    /// `Truncated`, `Invalid`, or `f`'s.
    pub fn nullable_array<T>(
        &mut self,
        compact: bool,
        mut f: impl FnMut(&mut Self) -> Result<T>,
    ) -> Result<Option<Vec<T>>> {
        // Every element takes at least one byte, so the count can't exceed
        // what is left (Kafka checks the same).
        let Some(n) = self.len(compact, true, "array length")? else {
            return Ok(None);
        };
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(f(self)?);
        }
        Ok(Some(out))
    }

    /// Reads a tagged-field section. `known` is called for each tag with a
    /// reader over the field's bytes and returns whether it decoded it;
    /// the others are returned raw.
    ///
    /// # Errors
    /// `Truncated`, `Invalid` (tags out of order, lengths), or `known`'s.
    pub fn tagged_fields(
        &mut self,
        mut known: impl FnMut(u32, &mut Reader<'a>) -> Result<bool>,
    ) -> Result<Vec<RawTaggedField>> {
        let count = self.uvarint()?;
        if count as usize > self.remaining() {
            return Err(CodecError::Invalid("tagged field count"));
        }
        let mut unknown = Vec::new();
        let mut last: Option<u32> = None;
        for _ in 0..count {
            let tag = self.uvarint()?;
            if last.is_some_and(|l| tag <= l) {
                return Err(CodecError::Invalid("tagged field order"));
            }
            last = Some(tag);
            let size = self.uvarint()? as usize;
            let data = self.take(size)?;
            let mut sub = Reader::new(data);
            if !known(tag, &mut sub)? {
                unknown.push(RawTaggedField {
                    tag,
                    data: data.to_vec(),
                });
            }
        }
        Ok(unknown)
    }
}

/// An output buffer for one message.
#[derive(Debug, Clone, Default)]
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    /// An empty writer.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A writer that appends to `buf`.
    #[must_use]
    pub fn from_vec(buf: Vec<u8>) -> Self {
        Self { buf }
    }

    /// The bytes written.
    #[must_use]
    pub fn into_inner(self) -> Vec<u8> {
        self.buf
    }

    /// The bytes written so far.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    /// How many bytes are written.
    #[must_use]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether nothing is written.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Mutable access, for patching lengths and checksums.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buf
    }

    /// Appends raw bytes.
    pub fn raw(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
    }

    /// Writes an `INT8`.
    pub fn i8(&mut self, v: i8) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }
    /// Writes an `INT16`.
    pub fn i16(&mut self, v: i16) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }
    /// Writes a `UINT16`.
    pub fn u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }
    /// Writes an `INT32`.
    pub fn i32(&mut self, v: i32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }
    /// Writes a `UINT32`.
    pub fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }
    /// Writes an `INT64`.
    pub fn i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }
    /// Writes a `FLOAT64`.
    pub fn f64(&mut self, v: f64) {
        self.i64(v.to_bits().cast_signed());
    }
    /// Writes a `BOOLEAN`.
    pub fn bool(&mut self, v: bool) {
        self.buf.push(u8::from(v));
    }
    /// Writes a `UUID`.
    pub fn uuid(&mut self, v: &Uuid) {
        self.buf.extend_from_slice(v.as_bytes());
    }

    /// Writes an `UNSIGNED_VARINT`.
    pub fn uvarint(&mut self, mut v: u32) {
        while v >= 0x80 {
            self.buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }

    /// Writes an unsigned varlong.
    pub fn uvarlong(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }

    /// Writes a zig-zag `VARINT`.
    pub fn varint(&mut self, v: i32) {
        self.uvarint(((v << 1) ^ (v >> 31)).cast_unsigned());
    }

    /// Writes a zig-zag `VARLONG`.
    pub fn varlong(&mut self, v: i64) {
        self.uvarlong(((v << 1) ^ (v >> 63)).cast_unsigned());
    }

    fn put_len(&mut self, n: Option<usize>, compact: bool, wide: bool) -> Result<()> {
        match (n, compact) {
            (None, true) => self.uvarint(0),
            (Some(n), true) => {
                self.uvarint(u32::try_from(n + 1).map_err(|_| CodecError::Invalid("length"))?);
            }
            (None, false) if wide => self.i32(-1),
            (None, false) => self.i16(-1),
            (Some(n), false) if wide => {
                self.i32(i32::try_from(n).map_err(|_| CodecError::Invalid("length"))?);
            }
            (Some(n), false) => {
                self.i16(i16::try_from(n).map_err(|_| CodecError::Invalid("string length"))?);
            }
        }
        Ok(())
    }

    /// Writes a `STRING` or `COMPACT_STRING`.
    ///
    /// # Errors
    /// `Invalid` for a classic string longer than 32767 bytes.
    pub fn string(&mut self, s: &str, compact: bool) -> Result<()> {
        self.put_len(Some(s.len()), compact, false)?;
        self.raw(s.as_bytes());
        Ok(())
    }

    /// Writes a nullable string.
    ///
    /// # Errors
    /// As [`Writer::string`].
    pub fn nullable_string(&mut self, s: Option<&str>, compact: bool) -> Result<()> {
        match s {
            None => self.put_len(None, compact, false),
            Some(s) => self.string(s, compact),
        }
    }

    /// Writes `BYTES` or `COMPACT_BYTES`.
    ///
    /// # Errors
    /// `Invalid` above `i32::MAX` bytes.
    pub fn bytes(&mut self, b: &[u8], compact: bool) -> Result<()> {
        self.put_len(Some(b.len()), compact, true)?;
        self.raw(b);
        Ok(())
    }

    /// Writes nullable bytes.
    ///
    /// # Errors
    /// As [`Writer::bytes`].
    pub fn nullable_bytes(&mut self, b: Option<&[u8]>, compact: bool) -> Result<()> {
        match b {
            None => self.put_len(None, compact, true),
            Some(b) => self.bytes(b, compact),
        }
    }

    /// Writes an `ARRAY` or `COMPACT_ARRAY`, each element with `f`.
    ///
    /// # Errors
    /// `f`'s.
    pub fn array<T>(
        &mut self,
        items: &[T],
        compact: bool,
        mut f: impl FnMut(&mut Self, &T) -> Result<()>,
    ) -> Result<()> {
        self.put_len(Some(items.len()), compact, true)?;
        for i in items {
            f(self, i)?;
        }
        Ok(())
    }

    /// Writes a nullable array.
    ///
    /// # Errors
    /// `f`'s.
    pub fn nullable_array<T>(
        &mut self,
        items: Option<&[T]>,
        compact: bool,
        f: impl FnMut(&mut Self, &T) -> Result<()>,
    ) -> Result<()> {
        match items {
            None => self.put_len(None, compact, true),
            Some(items) => self.array(items, compact, f),
        }
    }

    /// Writes a tagged-field section: `known` (tag and encoded bytes) and
    /// `unknown`, in tag order.
    ///
    /// # Errors
    /// `Invalid` for a duplicate tag.
    pub fn tagged_fields(
        &mut self,
        mut known: Vec<(u32, Vec<u8>)>,
        unknown: &[RawTaggedField],
    ) -> Result<()> {
        known.extend(unknown.iter().map(|f| (f.tag, f.data.clone())));
        known.sort_by_key(|(t, _)| *t);
        if known.windows(2).any(|w| w[0].0 == w[1].0) {
            return Err(CodecError::Invalid("duplicate tagged field"));
        }
        self.uvarint(u32::try_from(known.len()).map_err(|_| CodecError::Invalid("tags"))?);
        for (tag, data) in known {
            self.uvarint(tag);
            self.uvarint(u32::try_from(data.len()).map_err(|_| CodecError::Invalid("tag"))?);
            self.raw(&data);
        }
        Ok(())
    }
}

/// Encodes one value into its own buffer (a tagged field's bytes).
///
/// # Errors
/// `f`'s.
pub fn encode_with(f: impl FnOnce(&mut Writer) -> Result<()>) -> Result<Vec<u8>> {
    let mut w = Writer::new();
    f(&mut w)?;
    Ok(w.into_inner())
}

/// A versioned message of the protocol (generated from Kafka's message
/// definitions, see [`crate::messages`]).
pub trait Message: Sized {
    /// The API key; `-1` for headers and data structures.
    const API_KEY: i16;
    /// The lowest version the definition has.
    const MIN_VERSION: i16;
    /// The highest version the definition has.
    const MAX_VERSION: i16;
    /// The first flexible version (compact types, tagged fields);
    /// `i16::MAX` for none.
    const FLEXIBLE_MIN: i16;

    /// Encodes the message body at `version`.
    ///
    /// # Errors
    /// `NullNotAllowed` for a null where `version` doesn't allow one;
    /// `Invalid` for an over-long value.
    fn encode(&self, w: &mut Writer, version: i16) -> Result<()>;

    /// Decodes the message body at `version`.
    ///
    /// # Errors
    /// Any [`CodecError`]; never panics.
    fn decode(r: &mut Reader<'_>, version: i16) -> Result<Self>;

    /// Encodes into a new buffer.
    ///
    /// # Errors
    /// As [`Message::encode`].
    fn to_bytes(&self, version: i16) -> Result<Vec<u8>> {
        let mut w = Writer::new();
        self.encode(&mut w, version)?;
        Ok(w.into_inner())
    }

    /// Decodes a whole buffer; trailing bytes are an error.
    ///
    /// # Errors
    /// As [`Message::decode`], and `Invalid` for trailing bytes.
    fn from_bytes(b: &[u8], version: i16) -> Result<Self> {
        let mut r = Reader::new(b);
        let m = Self::decode(&mut r, version)?;
        if !r.is_empty() {
            return Err(CodecError::Invalid("trailing bytes"));
        }
        Ok(m)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_match_the_protocol_guide() {
        // Zig-zag: 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2147483647 -> 4294967294.
        let cases: &[(i32, &[u8])] = &[
            (0, &[0x00]),
            (-1, &[0x01]),
            (1, &[0x02]),
            (-2, &[0x03]),
            (63, &[0x7e]),
            (64, &[0x80, 0x01]),
            (300, &[0xd8, 0x04]),
            (i32::MAX, &[0xfe, 0xff, 0xff, 0xff, 0x0f]),
            (i32::MIN, &[0xff, 0xff, 0xff, 0xff, 0x0f]),
        ];
        for (v, bytes) in cases {
            let mut w = Writer::new();
            w.varint(*v);
            assert_eq!(w.as_slice(), *bytes, "{v}");
            assert_eq!(Reader::new(bytes).varint().unwrap(), *v);
        }
        for v in [0i64, -1, 1, i64::MAX, i64::MIN, 1 << 40] {
            let mut w = Writer::new();
            w.varlong(v);
            assert_eq!(Reader::new(w.as_slice()).varlong().unwrap(), v);
        }
    }

    #[test]
    fn over_long_varints_are_invalid() {
        assert_eq!(
            Reader::new(&[0xff, 0xff, 0xff, 0xff, 0x7f]).uvarint(),
            Err(CodecError::Invalid("varint"))
        );
        assert_eq!(
            Reader::new(&[0x80, 0x80]).uvarint(),
            Err(CodecError::Truncated)
        );
    }

    #[test]
    fn lengths_beyond_the_buffer_are_refused_before_allocating() {
        // An array claiming 2^31-1 elements in a 4-byte buffer.
        let mut r = Reader::new(&[0x7f, 0xff, 0xff, 0xff]);
        assert_eq!(
            r.array(false, Reader::i8),
            Err(CodecError::Invalid("array length"))
        );
        let mut r = Reader::new(&[0x00, 0x05, b'a']);
        assert_eq!(r.string(false), Err(CodecError::Invalid("string length")));
        let mut r = Reader::new(&[0xff, 0xff]);
        assert_eq!(r.nullable_string(false), Ok(None));
        let mut r = Reader::new(&[0x00]);
        assert_eq!(r.nullable_string(true), Ok(None));
        assert_eq!(
            Reader::new(&[0xff, 0xfe]).nullable_string(false),
            Err(CodecError::Invalid("string length"))
        );
    }

    #[test]
    fn tagged_fields_round_trip_and_keep_order() {
        let mut w = Writer::new();
        w.tagged_fields(
            vec![(5, vec![1, 2]), (1, vec![9])],
            &[RawTaggedField {
                tag: 3,
                data: vec![7],
            }],
        )
        .unwrap();
        assert_eq!(w.as_slice(), &[3, 1, 1, 9, 3, 1, 7, 5, 2, 1, 2]);
        let mut r = Reader::new(w.as_slice());
        let mut seen = Vec::new();
        let unknown = r
            .tagged_fields(|t, sub| {
                if t == 1 {
                    seen.push(sub.i8()?);
                    Ok(true)
                } else {
                    Ok(false)
                }
            })
            .unwrap();
        assert_eq!(seen, vec![9]);
        assert_eq!(unknown.len(), 2);
        // Out of order is refused.
        let mut r = Reader::new(&[2, 5, 0, 1, 0]);
        assert_eq!(
            r.tagged_fields(|_, _| Ok(false)),
            Err(CodecError::Invalid("tagged field order"))
        );
    }
}
