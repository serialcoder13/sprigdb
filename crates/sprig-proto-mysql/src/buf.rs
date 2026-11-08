//! [`Reader`] and [`Writer`]: the protocol's primitive types (fixed-width
//! little-endian integers, length-encoded integers and strings,
//! NUL-terminated strings) over byte buffers.
//!
//! `Reader` is bounds-checked and never panics; short input is a
//! [`ProtoError::Truncated`]. It borrows a slice; decoders that return owned
//! [`Bytes`] cut them from the payload with [`Bytes::slice_ref`].

use bytes::{BufMut, Bytes, BytesMut};

use crate::proto_error::{ProtoError, ProtoResult};

/// The length-encoded integer prefix for NULL in a text row.
pub const LENENC_NULL: u8 = 0xFB;

/// Reads protocol fields from a byte slice.
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    /// A reader at the start of `data`.
    #[must_use]
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    /// Offset of the next byte.
    #[must_use]
    pub fn pos(&self) -> usize {
        self.pos
    }

    /// Bytes not yet read.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    /// Whether everything has been read.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    /// The unread bytes, without consuming them.
    #[must_use]
    pub fn peek_rest(&self) -> &'a [u8] {
        &self.data[self.pos..]
    }

    /// The next byte, without consuming it.
    #[must_use]
    pub fn peek_u8(&self) -> Option<u8> {
        self.data.get(self.pos).copied()
    }

    /// Consumes and returns the unread bytes (`string<EOF>`).
    pub fn rest(&mut self) -> &'a [u8] {
        let r = &self.data[self.pos..];
        self.pos = self.data.len();
        r
    }

    /// Consumes `n` bytes.
    pub fn take(&mut self, n: usize) -> ProtoResult<&'a [u8]> {
        self.take_named(n, "bytes")
    }

    fn take_named(&mut self, n: usize, what: &'static str) -> ProtoResult<&'a [u8]> {
        if self.remaining() < n {
            return Err(ProtoError::Truncated {
                what,
                at: self.pos,
                need: n,
                have: self.remaining(),
            });
        }
        let r = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(r)
    }

    /// Skips `n` bytes.
    pub fn skip(&mut self, n: usize) -> ProtoResult<()> {
        self.take_named(n, "filler").map(|_| ())
    }

    fn array<const N: usize>(&mut self, what: &'static str) -> ProtoResult<[u8; N]> {
        let s = self.take_named(N, what)?;
        let mut a = [0u8; N];
        a.copy_from_slice(s);
        Ok(a)
    }

    /// `int<1>`.
    pub fn u8(&mut self) -> ProtoResult<u8> {
        Ok(self.array::<1>("int<1>")?[0])
    }

    /// `int<2>`.
    pub fn u16(&mut self) -> ProtoResult<u16> {
        Ok(u16::from_le_bytes(self.array("int<2>")?))
    }

    /// `int<3>`.
    pub fn u24(&mut self) -> ProtoResult<u32> {
        let [a, b, c] = self.array("int<3>")?;
        Ok(u32::from_le_bytes([a, b, c, 0]))
    }

    /// `int<4>`.
    pub fn u32(&mut self) -> ProtoResult<u32> {
        Ok(u32::from_le_bytes(self.array("int<4>")?))
    }

    /// `int<8>`.
    pub fn u64(&mut self) -> ProtoResult<u64> {
        Ok(u64::from_le_bytes(self.array("int<8>")?))
    }

    /// A fixed-size array.
    pub fn bytes_n<const N: usize>(&mut self) -> ProtoResult<[u8; N]> {
        self.array("bytes")
    }

    /// `int<lenenc>`, or `None` for the NULL marker 0xFB. 0xFF is an error.
    pub fn lenenc_int_or_null(&mut self) -> ProtoResult<Option<u64>> {
        let at = self.pos;
        match self.u8()? {
            n @ 0..=0xFA => Ok(Some(u64::from(n))),
            LENENC_NULL => Ok(None),
            0xFC => Ok(Some(u64::from(self.u16()?))),
            0xFD => Ok(Some(u64::from(self.u24()?))),
            0xFE => Ok(Some(self.u64()?)),
            0xFF => Err(ProtoError::malformed(format!(
                "0xFF is not a length-encoded integer (offset {at})"
            ))),
        }
    }

    /// `int<lenenc>`; NULL is an error.
    pub fn lenenc_int(&mut self) -> ProtoResult<u64> {
        let at = self.pos;
        self.lenenc_int_or_null()?
            .ok_or_else(|| ProtoError::malformed(format!("unexpected NULL at offset {at}")))
    }

    /// A length-encoded length, checked against the bytes left, so a caller
    /// may allocate it.
    pub fn lenenc_len(&mut self) -> ProtoResult<usize> {
        let at = self.pos;
        let n = self.lenenc_int()?;
        match usize::try_from(n) {
            Ok(n) if n <= self.remaining() => Ok(n),
            _ => Err(ProtoError::Truncated {
                what: "string<lenenc>",
                at,
                need: usize::try_from(n).unwrap_or(usize::MAX),
                have: self.remaining(),
            }),
        }
    }

    /// `string<lenenc>`.
    pub fn lenenc_bytes(&mut self) -> ProtoResult<&'a [u8]> {
        let n = self.lenenc_len()?;
        self.take_named(n, "string<lenenc>")
    }

    /// `string<lenenc>`, or `None` for NULL (0xFB).
    pub fn lenenc_bytes_or_null(&mut self) -> ProtoResult<Option<&'a [u8]>> {
        if self.peek_u8() == Some(LENENC_NULL) {
            self.pos += 1;
            return Ok(None);
        }
        self.lenenc_bytes().map(Some)
    }

    /// `string<NUL>`: the bytes up to a NUL, which is consumed.
    pub fn nul_bytes(&mut self) -> ProtoResult<&'a [u8]> {
        let rest = self.peek_rest();
        let Some(n) = rest.iter().position(|&b| b == 0) else {
            return Err(ProtoError::Truncated {
                what: "string<NUL>",
                at: self.pos,
                need: rest.len() + 1,
                have: rest.len(),
            });
        };
        self.pos += n + 1;
        Ok(&rest[..n])
    }

    /// `string<NUL>` if a NUL follows, otherwise the rest of the input
    /// (some clients omit the final NUL).
    pub fn nul_bytes_or_rest(&mut self) -> &'a [u8] {
        match self.nul_bytes() {
            Ok(s) => s,
            Err(_) => self.rest(),
        }
    }
}

/// Builds a payload.
#[derive(Debug, Default, Clone)]
pub struct Writer {
    buf: BytesMut,
}

impl Writer {
    /// An empty writer.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty writer with room for `n` bytes.
    #[must_use]
    pub fn with_capacity(n: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(n),
        }
    }

    /// Bytes written so far.
    #[must_use]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether nothing has been written.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// The finished payload.
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        self.buf.freeze()
    }

    /// The underlying buffer.
    pub fn buf_mut(&mut self) -> &mut BytesMut {
        &mut self.buf
    }

    /// `int<1>`.
    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.buf.put_u8(v);
        self
    }

    /// `int<2>`.
    pub fn u16(&mut self, v: u16) -> &mut Self {
        self.buf.put_u16_le(v);
        self
    }

    /// `int<3>`; the top byte of `v` is dropped.
    pub fn u24(&mut self, v: u32) -> &mut Self {
        self.buf.put_slice(&v.to_le_bytes()[..3]);
        self
    }

    /// `int<4>`.
    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.buf.put_u32_le(v);
        self
    }

    /// `int<8>`.
    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.buf.put_u64_le(v);
        self
    }

    /// Raw bytes.
    pub fn bytes(&mut self, v: &[u8]) -> &mut Self {
        self.buf.put_slice(v);
        self
    }

    /// `n` zero bytes.
    pub fn zeros(&mut self, n: usize) -> &mut Self {
        self.buf.put_bytes(0, n);
        self
    }

    /// `int<lenenc>`.
    pub fn lenenc_int(&mut self, v: u64) -> &mut Self {
        put_lenenc_int(&mut self.buf, v);
        self
    }

    /// `string<lenenc>`.
    pub fn lenenc_bytes(&mut self, v: &[u8]) -> &mut Self {
        put_lenenc_int(&mut self.buf, v.len() as u64);
        self.buf.put_slice(v);
        self
    }

    /// `string<lenenc>`, or the NULL marker 0xFB for `None`.
    pub fn lenenc_bytes_or_null(&mut self, v: Option<&[u8]>) -> &mut Self {
        match v {
            Some(v) => self.lenenc_bytes(v),
            None => self.u8(LENENC_NULL),
        }
    }

    /// `string<NUL>`. `v` must not contain a NUL; one inside would end the
    /// string early on the reading side.
    pub fn nul_bytes(&mut self, v: &[u8]) -> &mut Self {
        self.buf.put_slice(v);
        self.buf.put_u8(0);
        self
    }
}

/// Appends `int<lenenc>` to any buffer.
#[allow(clippy::cast_possible_truncation)] // each arm's range fits its width
pub fn put_lenenc_int(buf: &mut impl BufMut, v: u64) {
    match v {
        0..0xFB => buf.put_u8(v as u8),
        0xFB..=0xFFFF => {
            buf.put_u8(0xFC);
            buf.put_u16_le(v as u16);
        }
        0x1_0000..=0xFF_FFFF => {
            buf.put_u8(0xFD);
            buf.put_slice(&(v as u32).to_le_bytes()[..3]);
        }
        _ => {
            buf.put_u8(0xFE);
            buf.put_u64_le(v);
        }
    }
}

/// The encoded size of `int<lenenc>` for `v`.
#[must_use]
pub fn lenenc_int_len(v: u64) -> usize {
    match v {
        0..0xFB => 1,
        0xFB..=0xFFFF => 3,
        0x1_0000..=0xFF_FFFF => 4,
        _ => 9,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lenenc_int_round_trips_at_every_boundary() {
        for v in [
            0,
            1,
            0xFA,
            0xFB,
            0xFC,
            0xFFFF,
            0x1_0000,
            0xFF_FFFF,
            0x100_0000,
            u64::MAX,
        ] {
            let mut w = Writer::new();
            w.lenenc_int(v);
            let b = w.into_bytes();
            assert_eq!(b.len(), lenenc_int_len(v));
            let mut r = Reader::new(&b);
            assert_eq!(r.lenenc_int().ok(), Some(v));
            assert!(r.is_empty());
        }
    }

    #[test]
    fn short_input_is_an_error() {
        let mut r = Reader::new(&[0xFC, 1]);
        assert!(matches!(r.lenenc_int(), Err(ProtoError::Truncated { .. })));
        let mut r = Reader::new(&[5, b'a']);
        assert!(r.lenenc_bytes().is_err());
        let mut r = Reader::new(b"abc");
        assert!(r.nul_bytes().is_err());
        let mut r = Reader::new(&[0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
        assert!(r.lenenc_len().is_err());
        let mut r = Reader::new(&[0xFF]);
        assert!(r.lenenc_int().is_err());
    }

    #[test]
    fn strings() {
        let mut w = Writer::new();
        w.nul_bytes(b"user")
            .lenenc_bytes(b"pw")
            .lenenc_bytes_or_null(None);
        let b = w.into_bytes();
        let mut r = Reader::new(&b);
        assert_eq!(r.nul_bytes().ok(), Some(&b"user"[..]));
        assert_eq!(r.lenenc_bytes().ok(), Some(&b"pw"[..]));
        assert_eq!(r.lenenc_bytes_or_null().ok(), Some(None));
        assert!(r.is_empty());
    }
}
