//! zlib decompression for MariaDB's compressed events (`log_bin_compress`)
//! and compressed columns (`... COMPRESSED`).

use flate2::{Decompress, FlushDecompress, Status};
use sprig_common::SprigError;

use crate::buf::{be_uint, corrupt, unsupported};

/// Largest decompressed size accepted, as a guard against corrupt headers:
/// MariaDB caps events and column values at 4 GiB, and SprigDB at 1 GiB.
const MAX_UNCOMPRESSED: u64 = 1 << 30;

/// Decompresses the payload of a compressed event (`QUERY_COMPRESSED`, the
/// row data of `*_ROWS_COMPRESSED`).
///
/// The format (`binlog_buf_compress` in MariaDB): one header byte, `0x80 |
/// algorithm << 4 | lenlen`, with algorithm 0 for zlib; then the original
/// length in `lenlen` (1 to 4) big-endian bytes; then a zlib stream.
pub(crate) fn uncompress_event_payload(src: &[u8]) -> Result<Vec<u8>, SprigError> {
    let (&head, rest) = src
        .split_first()
        .ok_or_else(|| corrupt("empty compressed event payload"))?;
    if head & 0x80 == 0 {
        return Err(corrupt(format!(
            "compressed event header {head:#x} lacks bit 7"
        )));
    }
    let algorithm = (head & 0x70) >> 4;
    if algorithm != 0 {
        return Err(unsupported(format!(
            "compressed event uses algorithm {algorithm}; only zlib (0) is known"
        )));
    }
    let lenlen = usize::from(head & 0x07);
    if lenlen == 0 || lenlen > 4 || rest.len() < lenlen {
        return Err(corrupt(format!("compressed event length size {lenlen}")));
    }
    let len = be_uint(&rest[..lenlen]);
    inflate(&rest[lenlen..], len, true)
}

/// Decompresses a value of a compressed column, as stored in the row image.
///
/// The format (`compress_zlib` in MariaDB's `field.cc`): one header byte.
/// High nibble 0: the value follows uncompressed. High nibble 8 (zlib):
/// bit 3 set means a raw deflate stream (`column_compression_zlib_wrap =
/// OFF`, the default), bits 0-2 give the size of the big-endian original
/// length that follows.
pub(crate) fn uncompress_column(src: &[u8]) -> Result<Vec<u8>, SprigError> {
    let Some((&head, rest)) = src.split_first() else {
        return Ok(Vec::new());
    };
    match head >> 4 {
        0 => Ok(rest.to_vec()),
        8 => {
            let lenlen = usize::from(head & 0x07);
            if lenlen > 4 || rest.len() < lenlen {
                return Err(corrupt(format!("compressed column length size {lenlen}")));
            }
            let len = be_uint(&rest[..lenlen]);
            inflate(&rest[lenlen..], len, head & 0x08 == 0)
        }
        m => Err(unsupported(format!(
            "compressed column uses method {m}; only zlib (8) is known"
        ))),
    }
}

fn inflate(data: &[u8], expected: u64, zlib_header: bool) -> Result<Vec<u8>, SprigError> {
    if expected > MAX_UNCOMPRESSED {
        return Err(corrupt(format!(
            "compressed data claims {expected} bytes uncompressed"
        )));
    }
    let cap = usize::try_from(expected).map_err(|_| corrupt("compressed length overflows"))?;
    let mut out = Vec::with_capacity(cap);
    let mut d = Decompress::new(zlib_header);
    loop {
        let before_in = d.total_in();
        let before_out = d.total_out();
        let consumed = usize::try_from(before_in).unwrap_or(usize::MAX);
        let input = data.get(consumed..).unwrap_or_default();
        if out.len() == out.capacity() {
            out.reserve(4096);
        }
        let status = d
            .decompress_vec(input, &mut out, FlushDecompress::Finish)
            .map_err(|e| corrupt(format!("zlib: {e}")))?;
        match status {
            Status::StreamEnd => break,
            Status::Ok | Status::BufError => {
                if d.total_in() == before_in && d.total_out() == before_out {
                    if out.len() < out.capacity() {
                        return Err(corrupt("zlib stream is truncated"));
                    }
                    out.reserve(4096);
                }
            }
        }
        if out.len() as u64 > MAX_UNCOMPRESSED {
            return Err(corrupt("compressed data inflates past the limit"));
        }
    }
    if out.len() as u64 != expected {
        return Err(corrupt(format!(
            "compressed data inflated to {} bytes, header says {expected}",
            out.len()
        )));
    }
    Ok(out)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::io::Write;

    use flate2::Compression;
    use flate2::write::{DeflateEncoder, ZlibEncoder};

    use super::*;

    /// Compresses like MariaDB's `binlog_buf_compress`, for fixtures.
    pub(crate) fn compress_event_payload(src: &[u8]) -> Vec<u8> {
        let len = u32::try_from(src.len()).unwrap();
        let lenbytes: Vec<u8> = len
            .to_be_bytes()
            .into_iter()
            .skip_while(|&b| b == 0)
            .collect();
        let lenbytes = if lenbytes.is_empty() {
            vec![0]
        } else {
            lenbytes
        };
        let mut out = vec![0x80 | u8::try_from(lenbytes.len()).unwrap()];
        out.extend_from_slice(&lenbytes);
        let mut z = ZlibEncoder::new(Vec::new(), Compression::default());
        z.write_all(src).unwrap();
        out.extend(z.finish().unwrap());
        out
    }

    #[test]
    fn event_payload_round_trips() {
        for n in [0usize, 1, 300, 70_000] {
            let data: Vec<u8> = (0..n).map(|i| u8::try_from(i % 251).unwrap()).collect();
            let c = compress_event_payload(&data);
            assert_eq!(uncompress_event_payload(&c).unwrap(), data, "{n}");
        }
        assert!(uncompress_event_payload(&[0x01, 0, 0]).is_err());
        assert!(uncompress_event_payload(&[0x81, 9, 1, 2, 3]).is_err());
    }

    #[test]
    fn column_values_raw_deflate_and_zlib() {
        assert_eq!(uncompress_column(&[0, b'h', b'i']).unwrap(), b"hi");
        assert_eq!(uncompress_column(&[]).unwrap(), b"");
        let data = b"hello hello hello hello hello hello".repeat(10);
        let mut raw = DeflateEncoder::new(Vec::new(), Compression::default());
        raw.write_all(&data).unwrap();
        let mut v = vec![0x80 | 0x08 | 2, 0x01, 0x5e];
        v.extend(raw.finish().unwrap());
        assert_eq!(uncompress_column(&v).unwrap(), data);
        let mut z = ZlibEncoder::new(Vec::new(), Compression::default());
        z.write_all(&data).unwrap();
        let mut v = vec![0x80 | 2, 0x01, 0x5e];
        v.extend(z.finish().unwrap());
        assert_eq!(uncompress_column(&v).unwrap(), data);
    }
}
