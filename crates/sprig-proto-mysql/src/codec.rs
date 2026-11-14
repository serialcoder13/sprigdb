//! The packet codec: framing and compression.
//!
//! [`PacketCodec`] turns a byte stream into logical [`Packet`]s and back. It
//! splits payloads of 16 MiB - 1 bytes or more into chunks, reassembles them
//! on read, numbers packets with the shared sequence id, and refuses a
//! logical packet above `max_packet`. With [`Framing::Zlib`] or
//! [`Framing::Zstd`] it adds the compressed-packet header and its separate
//! sequence id.
//!
//! **Sequence ids.** The codec keeps one counter for both directions, as
//! MySQL does: each packet read or written takes the next id. Callers reset
//! it with [`PacketCodec::reset_seq`] when a command starts (the server
//! before reading a command, the client before writing one). Encoding a
//! [`Packet`] uses its `seq` for the first chunk; encoding plain [`Bytes`]
//! uses the counter. A plain-framing read whose sequence id isn't the
//! expected one fails with [`ProtoError::OutOfOrder`]; with compression only
//! the compressed header's id is checked, as in MySQL.
//!
//! **Switching framing.** Compression starts after the auth OK packet:
//! `framed.codec_mut().set_framing(..)` on a live `Framed` switches both
//! directions from the next frame on.

use std::io::Write as _;

use bytes::{Buf as _, BufMut as _, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::proto_error::ProtoError;

/// The largest chunk payload, 16 MiB - 1. A payload this size or larger is
/// split.
pub const MAX_CHUNK: usize = 0xFF_FFFF;

/// Batches shorter than this are sent uncompressed under compression.
pub const MIN_COMPRESS_LENGTH: usize = 50;

/// The default `max_packet`: 64 MiB, MySQL 8.4's `max_allowed_packet`.
pub const DEFAULT_MAX_PACKET: usize = 64 << 20;

/// The default zlib level.
pub const DEFAULT_ZLIB_LEVEL: u32 = 6;

/// The default zstd level when the client names none.
pub const DEFAULT_ZSTD_LEVEL: i32 = 3;

const HEADER: usize = 4;
const COMP_HEADER: usize = 7;

/// A logical packet: one payload, reassembled from its chunks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    /// Sequence id of the first chunk.
    pub seq: u8,
    /// The payload, without headers.
    pub payload: Bytes,
}

impl Packet {
    /// A packet with this sequence id and payload.
    pub fn new(seq: u8, payload: impl Into<Bytes>) -> Self {
        Self {
            seq,
            payload: payload.into(),
        }
    }
}

/// How packets are framed on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Framing {
    /// Plain packets.
    #[default]
    Plain,
    /// `CLIENT_COMPRESS`: zlib in compressed packets.
    Zlib {
        /// Compression level, 0-9.
        level: u32,
    },
    /// `CLIENT_ZSTD_COMPRESSION_ALGORITHM`: zstd in compressed packets.
    Zstd {
        /// Compression level, 1-22.
        level: i32,
    },
}

impl Framing {
    /// Whether packets travel inside compressed frames.
    #[must_use]
    pub fn is_compressed(self) -> bool {
        !matches!(self, Framing::Plain)
    }
}

/// Encodes and decodes MySQL packets. See the module docs.
#[derive(Debug)]
pub struct PacketCodec {
    /// Largest logical packet accepted on read; larger fails with
    /// [`ProtoError::PacketTooLarge`] (1153).
    pub max_packet: usize,
    framing: Framing,
    next_seq: u8,
    comp_seq: u8,
    /// First sequence id and payload so far of a split packet being read.
    partial: Option<(u8, BytesMut)>,
    /// Inflated bytes not yet decoded as packets (compressed framing).
    inflated: BytesMut,
}

impl Default for PacketCodec {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_PACKET)
    }
}

impl PacketCodec {
    /// A plain codec with sequence ids at 0.
    #[must_use]
    pub fn new(max_packet: usize) -> Self {
        Self {
            max_packet,
            framing: Framing::Plain,
            next_seq: 0,
            comp_seq: 0,
            partial: None,
            inflated: BytesMut::new(),
        }
    }

    /// The current framing.
    #[must_use]
    pub fn framing(&self) -> Framing {
        self.framing
    }

    /// Switches framing from the next frame on, in both directions.
    pub fn set_framing(&mut self, framing: Framing) {
        self.framing = framing;
        self.comp_seq = self.next_seq;
    }

    /// Starts a new command: both sequence ids go back to 0.
    pub fn reset_seq(&mut self) {
        self.next_seq = 0;
        self.comp_seq = 0;
    }

    /// The sequence id the next packet read or written takes.
    #[must_use]
    pub fn next_seq(&self) -> u8 {
        self.next_seq
    }

    /// Sets the next sequence id.
    pub fn set_next_seq(&mut self, seq: u8) {
        self.next_seq = seq;
        self.comp_seq = seq;
    }

    /// The compressed sequence id the next frame takes.
    #[must_use]
    pub fn compressed_seq(&self) -> u8 {
        self.comp_seq
    }

    /// Decodes one logical packet from plain chunks in `src`.
    fn decode_plain(
        &mut self,
        compressed: bool,
        src_is_inflated: bool,
        src_outer: &mut BytesMut,
    ) -> Result<Option<Packet>, ProtoError> {
        loop {
            let src = if src_is_inflated {
                &mut self.inflated
            } else {
                &mut *src_outer
            };
            if src.len() < HEADER {
                src.reserve(HEADER - src.len());
                return Ok(None);
            }
            let len = usize::from(src[0]) | usize::from(src[1]) << 8 | usize::from(src[2]) << 16;
            let seq = src[3];
            if !compressed && seq != self.next_seq {
                return Err(ProtoError::OutOfOrder {
                    expected: self.next_seq,
                    got: seq,
                });
            }
            let so_far = self.partial.as_ref().map_or(0, |(_, p)| p.len());
            if so_far + len > self.max_packet {
                return Err(ProtoError::PacketTooLarge {
                    size: so_far + len,
                    max: self.max_packet,
                });
            }
            if src.len() < HEADER + len {
                if !src_is_inflated {
                    src.reserve(HEADER + len - src.len());
                }
                return Ok(None);
            }
            src.advance(HEADER);
            let chunk = src.split_to(len);
            if !compressed {
                self.next_seq = seq.wrapping_add(1);
            }
            let last = len < MAX_CHUNK;
            match self.partial.take() {
                None if last => {
                    return Ok(Some(Packet {
                        seq,
                        payload: chunk.freeze(),
                    }));
                }
                None => self.partial = Some((seq, chunk)),
                Some((first, mut acc)) => {
                    acc.extend_from_slice(&chunk);
                    if last {
                        return Ok(Some(Packet {
                            seq: first,
                            payload: acc.freeze(),
                        }));
                    }
                    self.partial = Some((first, acc));
                }
            }
        }
    }

    /// Moves one compressed frame from `src` into `self.inflated`. Returns
    /// false when the frame isn't complete yet.
    fn read_frame(&mut self, src: &mut BytesMut) -> Result<bool, ProtoError> {
        if src.len() < COMP_HEADER {
            src.reserve(COMP_HEADER - src.len());
            return Ok(false);
        }
        let comp_len = usize::from(src[0]) | usize::from(src[1]) << 8 | usize::from(src[2]) << 16;
        let seq = src[3];
        let raw_len = usize::from(src[4]) | usize::from(src[5]) << 8 | usize::from(src[6]) << 16;
        if seq != self.comp_seq {
            return Err(ProtoError::OutOfOrder {
                expected: self.comp_seq,
                got: seq,
            });
        }
        if src.len() < COMP_HEADER + comp_len {
            src.reserve(COMP_HEADER + comp_len - src.len());
            return Ok(false);
        }
        src.advance(COMP_HEADER);
        let data = src.split_to(comp_len);
        self.comp_seq = seq.wrapping_add(1);
        self.next_seq = self.comp_seq;
        if raw_len == 0 {
            self.inflated.extend_from_slice(&data);
        } else {
            let out = inflate(self.framing, &data, raw_len)?;
            self.inflated.extend_from_slice(&out);
        }
        Ok(true)
    }

    /// Appends `payload` as plain chunks starting at `seq`; returns the next
    /// sequence id.
    fn write_chunks(payload: &[u8], mut seq: u8, dst: &mut BytesMut) -> u8 {
        dst.reserve(payload.len() + HEADER * (payload.len() / MAX_CHUNK + 1));
        let mut rest = payload;
        loop {
            let n = rest.len().min(MAX_CHUNK);
            dst.put_slice(&u24_le(n));
            dst.put_u8(seq);
            dst.put_slice(&rest[..n]);
            seq = seq.wrapping_add(1);
            rest = &rest[n..];
            if n < MAX_CHUNK {
                return seq;
            }
        }
    }

    fn encode_payload(
        &mut self,
        seq: u8,
        payload: &[u8],
        dst: &mut BytesMut,
    ) -> Result<(), ProtoError> {
        if !self.framing.is_compressed() {
            self.next_seq = Self::write_chunks(payload, seq, dst);
            return Ok(());
        }
        let mut plain = BytesMut::new();
        Self::write_chunks(payload, seq, &mut plain);
        for block in plain.chunks(MAX_CHUNK) {
            let packed = if block.len() < MIN_COMPRESS_LENGTH {
                None
            } else {
                deflate(self.framing, block)?.filter(|c| c.len() < block.len())
            };
            if let Some(c) = packed {
                dst.put_slice(&u24_le(c.len()));
                dst.put_u8(self.comp_seq);
                dst.put_slice(&u24_le(block.len()));
                dst.put_slice(&c);
            } else {
                dst.put_slice(&u24_le(block.len()));
                dst.put_u8(self.comp_seq);
                dst.put_slice(&[0, 0, 0]);
                dst.put_slice(block);
            }
            self.comp_seq = self.comp_seq.wrapping_add(1);
        }
        self.next_seq = self.comp_seq;
        Ok(())
    }
}

#[allow(clippy::cast_possible_truncation)] // callers pass at most MAX_CHUNK
fn u24_le(n: usize) -> [u8; 3] {
    let b = (n as u32).to_le_bytes();
    [b[0], b[1], b[2]]
}

/// Compresses one frame's bytes; `None` for plain framing.
fn deflate(framing: Framing, data: &[u8]) -> Result<Option<Vec<u8>>, ProtoError> {
    match framing {
        Framing::Plain => Ok(None),
        Framing::Zlib { level } => {
            let mut e = flate2::write::ZlibEncoder::new(
                Vec::with_capacity(data.len() / 2),
                flate2::Compression::new(level.min(9)),
            );
            e.write_all(data)
                .map_err(|e| ProtoError::Compression(e.to_string()))?;
            e.finish()
                .map(Some)
                .map_err(|e| ProtoError::Compression(e.to_string()))
        }
        Framing::Zstd { level } => zstd::bulk::compress(data, level)
            .map(Some)
            .map_err(|e| ProtoError::Compression(e.to_string())),
    }
}

/// Inflates one frame to exactly `expected` bytes (the decompression-bomb
/// guard: the output buffer never grows past the declared length).
fn inflate(framing: Framing, data: &[u8], expected: usize) -> Result<Vec<u8>, ProtoError> {
    let out = match framing {
        Framing::Plain => {
            return Err(ProtoError::Compression(
                "compressed frame on a plain codec".into(),
            ));
        }
        Framing::Zlib { .. } => {
            let mut out = Vec::with_capacity(expected);
            let mut d = flate2::Decompress::new(true);
            let status = d
                .decompress_vec(data, &mut out, flate2::FlushDecompress::Finish)
                .map_err(|e| ProtoError::Compression(e.to_string()))?;
            if status != flate2::Status::StreamEnd {
                return Err(ProtoError::Compression(format!(
                    "zlib frame inflates past its declared {expected} bytes or is cut short"
                )));
            }
            out
        }
        Framing::Zstd { .. } => zstd::bulk::decompress(data, expected)
            .map_err(|e| ProtoError::Compression(e.to_string()))?,
    };
    if out.len() != expected {
        return Err(ProtoError::Compression(format!(
            "frame inflated to {} bytes, declared {expected}",
            out.len()
        )));
    }
    Ok(out)
}

impl Decoder for PacketCodec {
    type Item = Packet;
    type Error = ProtoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Packet>, ProtoError> {
        if !self.framing.is_compressed() {
            return self.decode_plain(false, false, src);
        }
        loop {
            if let Some(p) = self.decode_plain(true, true, src)? {
                return Ok(Some(p));
            }
            if !self.read_frame(src)? {
                return Ok(None);
            }
        }
    }
}

impl Encoder<Packet> for PacketCodec {
    type Error = ProtoError;

    fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), ProtoError> {
        self.encode_payload(item.seq, &item.payload, dst)
    }
}

impl Encoder<Bytes> for PacketCodec {
    type Error = ProtoError;

    /// Encodes a payload with the codec's next sequence id.
    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<(), ProtoError> {
        self.encode_payload(self.next_seq, &item, dst)
    }
}

#[cfg(test)]
mod tests;
