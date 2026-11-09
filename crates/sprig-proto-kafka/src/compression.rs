//! Record batch compression (the low three bits of a batch's attributes).
//!
//! All four of Kafka's codecs:
//!
//! - gzip (flate2, pure-Rust backend);
//! - snappy (`snap`): written in the xerial framing the Java client uses
//!   (`\x82SNAPPY\0`, versions, then length-prefixed raw blocks); read in
//!   that framing or as one raw snappy block, which librdkafka sends;
//! - LZ4 (`lz4_flex`): the LZ4 frame format, written as the Java client
//!   does (independent 64 KB blocks, no checksums), read with any frame
//!   options;
//! - zstd.
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
    /// 2: snappy (xerial framing).
    Snappy,
    /// 3: LZ4 frames.
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
            n => Err(CodecError::UnsupportedCompression(
                u8::try_from(n).unwrap_or(u8::MAX),
            )),
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

    /// Whether this build can read and write it (all four codecs).
    #[must_use]
    pub const fn supported(self) -> bool {
        true
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
    /// `CorruptRecord` if the codec fails.
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
            Self::Zstd => {
                zstd::bulk::compress(data, 3).map_err(|_| CodecError::CorruptRecord("zstd"))
            }
            Self::Snappy => snappy_xerial(data),
            Self::Lz4 => {
                let info = lz4_flex::frame::FrameInfo::new()
                    .block_size(lz4_flex::frame::BlockSize::Max64KB)
                    .block_mode(lz4_flex::frame::BlockMode::Independent);
                let mut e = lz4_flex::frame::FrameEncoder::with_frame_info(info, Vec::new());
                e.write_all(data)
                    .map_err(|_| CodecError::CorruptRecord("lz4"))?;
                e.finish().map_err(|_| CodecError::CorruptRecord("lz4"))
            }
        }
    }

    /// Decompresses `data`, refusing output larger than `limit` bytes.
    ///
    /// # Errors
    /// `CorruptRecord` for bad input or output above `limit`.
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
            Self::Snappy => unsnappy(data, limit),
            Self::Lz4 => bounded(Box::new(lz4_flex::frame::FrameDecoder::new(data)), "lz4"),
        }
    }
}

/// The xerial framing's header: magic, version 1, compatible version 1.
const XERIAL_HEADER: [u8; 16] = [
    0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0, 0, 0, 0, 1, 0, 0, 0, 1,
];
/// The Java client's block size (`SnappyOutputStream`'s 32 KB).
const XERIAL_BLOCK: usize = 32 * 1024;

fn snappy_xerial(data: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(data.len() / 2 + 32);
    out.extend_from_slice(&XERIAL_HEADER);
    let mut enc = snap::raw::Encoder::new();
    for chunk in data.chunks(XERIAL_BLOCK) {
        let block = enc
            .compress_vec(chunk)
            .map_err(|_| CodecError::CorruptRecord("snappy"))?;
        let n = u32::try_from(block.len()).map_err(|_| CodecError::CorruptRecord("snappy"))?;
        out.extend_from_slice(&n.to_be_bytes());
        out.extend_from_slice(&block);
    }
    Ok(out)
}

/// One raw snappy block into `out`, checking its stated length against
/// what is left of `limit` before allocating.
fn unsnappy_block(
    dec: &mut snap::raw::Decoder,
    block: &[u8],
    out: &mut Vec<u8>,
    limit: usize,
) -> Result<()> {
    let n = snap::raw::decompress_len(block).map_err(|_| CodecError::CorruptRecord("snappy"))?;
    if out.len() + n > limit {
        return Err(CodecError::CorruptRecord("decompressed batch too large"));
    }
    let at = out.len();
    out.resize(at + n, 0);
    let got = dec
        .decompress(block, &mut out[at..])
        .map_err(|_| CodecError::CorruptRecord("snappy"))?;
    out.truncate(at + got);
    Ok(())
}

fn unsnappy(data: &[u8], limit: usize) -> Result<Vec<u8>> {
    let mut dec = snap::raw::Decoder::new();
    let mut out = Vec::new();
    if data.len() >= 8 && data[..8] == XERIAL_HEADER[..8] {
        // Xerial: the header (16 bytes), then [length][block] pairs.
        let mut rest = data.get(16..).ok_or(CodecError::CorruptRecord("snappy"))?;
        while !rest.is_empty() {
            let len: [u8; 4] = rest
                .get(..4)
                .and_then(|b| b.try_into().ok())
                .ok_or(CodecError::CorruptRecord("snappy"))?;
            let len = u32::from_be_bytes(len) as usize;
            let block = rest
                .get(4..4 + len)
                .ok_or(CodecError::CorruptRecord("snappy"))?;
            unsnappy_block(&mut dec, block, &mut out, limit)?;
            rest = &rest[4 + len..];
        }
        return Ok(out);
    }
    // librdkafka: one raw block.
    unsnappy_block(&mut dec, data, &mut out, limit)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [Compression; 5] = [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ];

    #[test]
    fn every_codec_round_trips() {
        let data: Vec<u8> = b"sprigdb event "
            .iter()
            .copied()
            .cycle()
            .take(10_000)
            .collect();
        for c in ALL {
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
    fn snappy_framing() {
        let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let z = Compression::Snappy.compress(&data).unwrap();
        assert_eq!(z[..16], XERIAL_HEADER);
        // Several 32 KB blocks.
        assert!(z.len() > 16 + 3 * 4);
        // librdkafka's raw form reads too.
        let raw = snap::raw::Encoder::new().compress_vec(&data).unwrap();
        assert_eq!(
            Compression::Snappy.decompress(&raw, data.len()).unwrap(),
            data
        );
        assert!(
            Compression::Snappy
                .decompress(&raw, data.len() - 1)
                .is_err()
        );
        // A truncated xerial block is corrupt, not a panic.
        assert!(
            Compression::Snappy
                .decompress(&z[..z.len() - 3], data.len())
                .is_err()
        );
    }

    #[test]
    fn lz4_reads_frames_with_checksums() {
        let data = b"lz4 frame with checksums ".repeat(500);
        let info = lz4_flex::frame::FrameInfo::new()
            .content_checksum(true)
            .block_checksums(true)
            .block_mode(lz4_flex::frame::BlockMode::Linked);
        let mut e = lz4_flex::frame::FrameEncoder::with_frame_info(info, Vec::new());
        e.write_all(&data).unwrap();
        let z = e.finish().unwrap();
        assert_eq!(Compression::Lz4.decompress(&z, data.len()).unwrap(), data);
        assert!(Compression::Lz4.decompress(&z, 100).is_err());
    }

    #[test]
    fn unknown_codecs_are_refused() {
        assert_eq!(
            Compression::from_attributes(0x0005),
            Err(CodecError::UnsupportedCompression(5))
        );
        assert_eq!(
            Compression::from_attributes(0x0014).unwrap(),
            Compression::Zstd
        );
    }

    #[test]
    fn garbage_is_corrupt_not_a_panic() {
        for c in [
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            assert!(c.decompress(b"\x82SNAPPY\0garbage", 100).is_err(), "{c:?}");
        }
    }
}
