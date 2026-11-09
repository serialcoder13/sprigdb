//! Record batch compression (the low three bits of a batch's attributes).
//!
//! SprigDB has gzip (flate2, pure-Rust backend) and zstd. Snappy and LZ4
//! have no crate in the tree and the build takes no new downloads, so a
//! batch using them is refused with Kafka's `UNSUPPORTED_COMPRESSION_TYPE`
//! (76); producers configured for them see that error at their first
//! produce (chapter 45, decisions file "R9 Kafka front end").
//!
//! Decompression is bounded: a batch may not inflate beyond the caller's
//! limit, so a small compressed batch can't make the gateway allocate
//! gigabytes.

use std::io::{Read, Write};

use crate::wire::{CodecError, Result};

/// A compression codec of record batches v2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Compression {
    /// 0: none.
    None,
    /// 1: gzip.
    Gzip,
    /// 2: snappy (xerial framing): refused.
    Snappy,
    /// 3: LZ4 frames: refused.
    Lz4,
    /// 4: zstd.
    Zstd,
}

impl Compression {
    /// The codec of attribute bits `0..3`.
    ///
    /// # Errors
    /// `UnsupportedCompression` for 5, 6 and 7.
    pub fn from_attributes(attributes: i16) -> Result<Self> {
        match attributes & 0x07 {
            0 => Ok(Self::None),
            1 => Ok(Self::Gzip),
            2 => Ok(Self::Snappy),
            3 => Ok(Self::Lz4),
            4 => Ok(Self::Zstd),
            n => Err(CodecError::UnsupportedCompression(n as u8)),
        }
    }

    /// The attribute bits.
    #[must_use]
    pub const fn id(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Gzip => 1,
            Self::Snappy => 2,
            Self::Lz4 => 3,
            Self::Zstd => 4,
        }
    }

    /// Whether this build can read and write it.
    #[must_use]
    pub const fn supported(self) -> bool {
        matches!(self, Self::None | Self::Gzip | Self::Zstd)
    }

    /// The `compression.type` name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Gzip => "gzip",
            Self::Snappy => "snappy",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }

    /// Compresses `data`.
    ///
    /// # Errors
    /// `UnsupportedCompression` for snappy and LZ4.
    pub fn compress(self, data: &[u8]) -> Result<Vec<u8>> {
        match self {
            Self::None => Ok(data.to_vec()),
            Self::Gzip => {
                let mut e =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                e.write_all(data)
                    .and_then(|()| e.finish())
                    .map_err(|_| CodecError::CorruptRecord("gzip"))
            }
            // Level 3, zstd's and Kafka's default.
            Self::Zstd => zstd::bulk::compress(data, 3).map_err(|_| CodecError::CorruptRecord("zstd")),
            Self::Snappy | Self::Lz4 => Err(CodecError::UnsupportedCompression(self.id())),
        }
    }

    /// Decompresses `data`, refusing output larger than `limit` bytes.
    ///
    /// # Errors
    /// `UnsupportedCompression` for snappy and LZ4; `CorruptRecord` for
    /// bad input or output above `limit`.
    pub fn decompress(self, data: &[u8], limit: usize) -> Result<Vec<u8>> {
        let bounded = |mut r: Box<dyn Read + '_>, what: &'static str| -> Result<Vec<u8>> {
            let mut out = Vec::new();
            let cap = u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1);
            (&mut r)
                .take(cap)
                .read_to_end(&mut out)
                .map_err(|_| CodecError::CorruptRecord(what))?;
            if out.len() > limit {
                return Err(CodecError::CorruptRecord("decompressed batch too large"));
            }
            Ok(out)
        };
        match self {
            Self::None => Ok(data.to_vec()),
            Self::Gzip => bounded(Box::new(flate2::read::MultiGzDecoder::new(data)), "gzip"),
            Self::Zstd => {
                let d = zstd::stream::read::Decoder::new(data)
                    .map_err(|_| CodecError::CorruptRecord("zstd"))?;
                bounded(Box::new(d), "zstd")
            }
            Self::Snappy | Self::Lz4 => Err(CodecError::UnsupportedCompression(self.id())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gzip_and_zstd_round_trip() {
        let data: Vec<u8> = b"sprigdb event ".iter().copied().cycle().take(10_000).collect();
        for c in [Compression::None, Compression::Gzip, Compression::Zstd] {
            let z = c.compress(&data).unwrap();
            if c != Compression::None {
                assert!(z.len() < data.len() / 10, "{c:?}");
            }
            assert_eq!(c.decompress(&z, data.len()).unwrap(), data);
            // One byte under the real size is refused.
            if c != Compression::None {
                assert_eq!(
                    c.decompress(&z, data.len() - 1),
                    Err(CodecError::CorruptRecord("decompressed batch too large"))
                );
            }
        }
    }

    #[test]
    fn snappy_lz4_and_unknown_codecs_are_refused() {
        assert_eq!(
            Compression::Snappy.decompress(b"x", 10),
            Err(CodecError::UnsupportedCompression(2))
        );
        assert_eq!(
            Compression::Lz4.compress(b"x"),
            Err(CodecError::UnsupportedCompression(3))
        );
        assert_eq!(
            Compression::from_attributes(0x0005),
            Err(CodecError::UnsupportedCompression(5))
        );
        assert_eq!(Compression::from_attributes(0x0014).unwrap(), Compression::Zstd);
    }

    #[test]
    fn garbage_is_corrupt_not_a_panic() {
        assert!(Compression::Gzip.decompress(b"not gzip", 100).is_err());
        assert!(Compression::Zstd.decompress(b"not zstd", 100).is_err());
    }
}
