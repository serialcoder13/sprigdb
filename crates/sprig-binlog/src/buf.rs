//! A bounds-checked little-endian byte cursor, and the crate's error helpers.

use sprig_common::{SprigCode, SprigError};

/// A malformed event or packet.
pub(crate) fn corrupt(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::BinlogCorrupt, msg)
}

/// Something the reader does not handle.
pub(crate) fn unsupported(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::BinlogUnsupported, msg)
}

/// The connection broke; the caller reconnects.
pub(crate) fn disconnected(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::BinlogDisconnected, msg)
}

/// Reads fields from a byte slice, failing with `ER_SPRIG_BINLOG_CORRUPT`
/// instead of panicking when the input is short.
#[derive(Debug, Clone)]
pub(crate) struct Buf<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Buf<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    pub(crate) fn rest(&mut self) -> &'a [u8] {
        let r = &self.data[self.pos..];
        self.pos = self.data.len();
        r
    }

    pub(crate) fn peek_rest(&self) -> &'a [u8] {
        &self.data[self.pos..]
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8], SprigError> {
        if self.remaining() < n {
            return Err(corrupt(format!(
                "need {n} bytes at offset {}, have {}",
                self.pos,
                self.remaining()
            )));
        }
        let r = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(r)
    }

    pub(crate) fn skip(&mut self, n: usize) -> Result<(), SprigError> {
        self.take(n).map(|_| ())
    }

    pub(crate) fn u8(&mut self) -> Result<u8, SprigError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u16(&mut self) -> Result<u16, SprigError> {
        let b = self.take(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    pub(crate) fn u24(&mut self) -> Result<u32, SprigError> {
        let b = self.take(3)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], 0]))
    }

    pub(crate) fn u32(&mut self) -> Result<u32, SprigError> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub(crate) fn u48(&mut self) -> Result<u64, SprigError> {
        self.uint_le(6)
    }

    pub(crate) fn u64(&mut self) -> Result<u64, SprigError> {
        self.uint_le(8)
    }

    /// An unsigned little-endian integer of `n` (at most 8) bytes.
    pub(crate) fn uint_le(&mut self, n: usize) -> Result<u64, SprigError> {
        Ok(le_uint(self.take(n)?))
    }

    /// An unsigned big-endian integer of `n` (at most 8) bytes.
    pub(crate) fn uint_be(&mut self, n: usize) -> Result<u64, SprigError> {
        Ok(be_uint(self.take(n)?))
    }

    /// A length-encoded integer (MySQL protocol).
    pub(crate) fn lenenc(&mut self) -> Result<u64, SprigError> {
        match self.u8()? {
            n @ 0..=0xfa => Ok(u64::from(n)),
            0xfc => Ok(u64::from(self.u16()?)),
            0xfd => Ok(u64::from(self.u24()?)),
            0xfe => self.u64(),
            b => Err(corrupt(format!("bad length-encoded integer prefix {b:#x}"))),
        }
    }

    pub(crate) fn lenenc_usize(&mut self) -> Result<usize, SprigError> {
        let n = self.lenenc()?;
        usize::try_from(n).map_err(|_| corrupt(format!("length {n} too large")))
    }

    /// A length-encoded string.
    pub(crate) fn lenenc_bytes(&mut self) -> Result<&'a [u8], SprigError> {
        let n = self.lenenc_usize()?;
        self.take(n)
    }

    /// Bytes up to (not including) the next NUL, which is consumed.
    pub(crate) fn nul_terminated(&mut self) -> Result<&'a [u8], SprigError> {
        let rest = self.peek_rest();
        let end = rest
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| corrupt("missing NUL terminator"))?;
        let r = &rest[..end];
        self.pos += end + 1;
        Ok(r)
    }
}

/// Little-endian unsigned integer of up to 8 bytes.
pub(crate) fn le_uint(b: &[u8]) -> u64 {
    b.iter()
        .rev()
        .fold(0u64, |acc, &x| (acc << 8) | u64::from(x))
}

/// Big-endian unsigned integer of up to 8 bytes.
pub(crate) fn be_uint(b: &[u8]) -> u64 {
    b.iter().fold(0u64, |acc, &x| (acc << 8) | u64::from(x))
}

/// Hexadecimal digits of `b`, upper or lower case.
pub(crate) fn hex(b: &[u8], upper: bool) -> String {
    use std::fmt::Write as _;
    b.iter()
        .fold(String::with_capacity(b.len() * 2), |mut s, x| {
            let _ = if upper {
                write!(s, "{x:02X}")
            } else {
                write!(s, "{x:02x}")
            };
            s
        })
}

/// Appends a length-encoded integer.
pub(crate) fn put_lenenc(out: &mut Vec<u8>, n: u64) {
    match n {
        0..=0xfa => out.push(u8::try_from(n).unwrap_or_default()),
        0xfb..=0xffff => {
            out.push(0xfc);
            out.extend_from_slice(&n.to_le_bytes()[..2]);
        }
        0x1_0000..=0xff_ffff => {
            out.push(0xfd);
            out.extend_from_slice(&n.to_le_bytes()[..3]);
        }
        _ => {
            out.push(0xfe);
            out.extend_from_slice(&n.to_le_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lenenc_round_trips() {
        for n in [
            0u64,
            250,
            251,
            0xffff,
            0x1_0000,
            0xff_ffff,
            0x100_0000,
            u64::MAX,
        ] {
            let mut v = Vec::new();
            put_lenenc(&mut v, n);
            assert_eq!(Buf::new(&v).lenenc().unwrap(), n, "{n}");
        }
        assert!(Buf::new(&[0xfb]).lenenc().is_err());
    }

    #[test]
    fn short_input_is_an_error_not_a_panic() {
        let mut b = Buf::new(&[1, 2, 3]);
        assert_eq!(b.u16().unwrap(), 0x0201);
        assert!(b.u32().is_err());
        assert_eq!(b.u8().unwrap(), 3);
        assert!(b.nul_terminated().is_err());
        assert_eq!(be_uint(&[1, 2]), 0x0102);
        assert_eq!(le_uint(&[1, 2]), 0x0201);
    }
}
