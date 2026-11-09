//! The MongoDB wire codec (chapter 30, "Framing and compression"; build
//! step 1): `OP_MSG`, `OP_QUERY` and `OP_REPLY` for the legacy handshake,
//! and `OP_COMPRESSED` with snappy, zlib and zstd.
//!
//! ```text
//! OP_MSG  : header(16) | flagBits u32 | section* | [CRC-32C u32 if bit 0]
//!           section kind 0: BSON body                       (exactly one)
//!           section kind 1: i32 size | cstring identifier | BSON documents
//! OP_QUERY: header | flags i32 | "admin.$cmd" | skip i32 | return i32 | BSON query
//! OP_REPLY: header | flags i32 | cursorId i64 | start i32 | count i32 | BSON document
//! OP_COMPRESSED: header | originalOpcode i32 | uncompressedSize i32 | compressorId u8 | data
//! ```
//!
//! Every integer is little-endian. [`decode`] takes one whole message (the
//! caller read `length` bytes); [`read_message`] does that from a stream,
//! refusing lengths outside 16..=48,000,000 before allocating.

use std::io::{Read, Write};

use bson::RawDocumentBuf;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::crc32c;

/// `OP_REPLY`.
pub const OP_REPLY: i32 = 1;
/// `OP_QUERY`.
pub const OP_QUERY: i32 = 2004;
/// `OP_COMPRESSED`.
pub const OP_COMPRESSED: i32 = 2012;
/// `OP_MSG`.
pub const OP_MSG: i32 = 2013;

/// The largest message: `maxMessageSizeBytes`.
pub const MAX_MESSAGE: usize = 48_000_000;
/// The largest BSON object: `maxBsonObjectSize`.
pub const MAX_BSON: usize = 16 * 1024 * 1024;
/// A command body may exceed `MAX_BSON` by this much (MongoDB's overhead).
pub const BODY_OVERHEAD: usize = 16 * 1024;

/// `OP_MSG` flag: a CRC-32C follows the sections.
pub const FLAG_CHECKSUM: u32 = 1;
/// `OP_MSG` flag: the sender sends another message without waiting.
pub const FLAG_MORE_TO_COME: u32 = 1 << 1;
/// `OP_MSG` flag: the client accepts exhaust replies.
pub const FLAG_EXHAUST_ALLOWED: u32 = 1 << 16;

/// The 16-byte header of every message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MsgHeader {
    /// The whole message's length, header included.
    pub length: i32,
    /// The sender's id for this message.
    pub request_id: i32,
    /// The id of the request this replies to; 0 in requests.
    pub response_to: i32,
    /// The opcode.
    pub op_code: i32,
}

/// A kind 1 section: a named sequence of documents (`documents`,
/// `updates`, `deletes`, `ops`, `nsInfo`).
#[derive(Debug, Clone, PartialEq)]
pub struct DocSequence {
    /// The field the documents belong to.
    pub identifier: String,
    /// The documents.
    pub docs: Vec<RawDocumentBuf>,
}

/// An `OP_MSG`.
#[derive(Debug, Clone, PartialEq)]
pub struct OpMsg {
    /// Flag bits: 0 checksum, 1 moreToCome, 16 exhaustAllowed.
    pub flags: u32,
    /// The single kind 0 section.
    pub body: RawDocumentBuf,
    /// The kind 1 sections.
    pub sequences: Vec<DocSequence>,
}

/// An `OP_QUERY`, accepted only for the legacy handshake.
#[derive(Debug, Clone, PartialEq)]
pub struct OpQuery {
    /// Flags.
    pub flags: i32,
    /// `<db>.$cmd`.
    pub full_collection_name: String,
    /// Documents to skip.
    pub number_to_skip: i32,
    /// Documents to return.
    pub number_to_return: i32,
    /// The command.
    pub query: RawDocumentBuf,
}

/// An `OP_REPLY`, the answer to an `OP_QUERY`.
#[derive(Debug, Clone, PartialEq)]
pub struct OpReply {
    /// Response flags (bit 3: `AwaitCapable`).
    pub flags: i32,
    /// Always 0 here.
    pub cursor_id: i64,
    /// Always 0 here.
    pub starting_from: i32,
    /// The documents.
    pub docs: Vec<RawDocumentBuf>,
}

/// A decoded message.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// `OP_MSG` (2013).
    Msg(OpMsg),
    /// `OP_QUERY` (2004), handshake only.
    Query(OpQuery),
    /// `OP_REPLY` (1).
    Reply(OpReply),
}

/// A compressor of `OP_COMPRESSED`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Compressor {
    /// No compression (id 0).
    Noop,
    /// Snappy, raw format (id 1).
    Snappy,
    /// zlib (id 2).
    Zlib,
    /// zstd (id 3).
    Zstd,
}

impl Compressor {
    /// The wire id.
    #[must_use]
    pub const fn id(self) -> u8 {
        match self {
            Self::Noop => 0,
            Self::Snappy => 1,
            Self::Zlib => 2,
            Self::Zstd => 3,
        }
    }

    /// The compressor with wire id `id`.
    #[must_use]
    pub const fn from_id(id: u8) -> Option<Self> {
        match id {
            0 => Some(Self::Noop),
            1 => Some(Self::Snappy),
            2 => Some(Self::Zlib),
            3 => Some(Self::Zstd),
            _ => None,
        }
    }

    /// The name in `hello`'s `compression` list.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Noop => "noop",
            Self::Snappy => "snappy",
            Self::Zlib => "zlib",
            Self::Zstd => "zstd",
        }
    }

    /// The compressor named `name` in a `hello`.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "noop" => Some(Self::Noop),
            "snappy" => Some(Self::Snappy),
            "zlib" => Some(Self::Zlib),
            "zstd" => Some(Self::Zstd),
            _ => None,
        }
    }
}

/// A framing error. Every one closes the connection.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    /// The header's length is outside 16..=48,000,000.
    #[error("message length {0} out of bounds")]
    Length(i64),
    /// An opcode the gateway doesn't take (legacy opcodes were removed in
    /// MongoDB 6.0).
    #[error("unsupported opcode {0}")]
    OpCode(i32),
    /// A malformed message.
    #[error("malformed message: {0}")]
    Malformed(String),
    /// A required flag bit the gateway doesn't know.
    #[error("unknown required OP_MSG flag bits {0:#x}")]
    Flags(u32),
    /// The CRC-32C doesn't match.
    #[error("OP_MSG checksum mismatch")]
    Checksum,
    /// `OP_COMPRESSED` failed to decompress, or names an unknown
    /// compressor.
    #[error("decompression failed: {0}")]
    Compression(String),
    /// The stream ended inside a message.
    #[error("connection closed")]
    Eof,
    /// An I/O error.
    #[error("i/o: {0}")]
    Io(String),
}

fn bad(what: impl Into<String>) -> CodecError {
    CodecError::Malformed(what.into())
}

struct Reader<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn new(b: &'a [u8]) -> Self {
        Self { b, at: 0 }
    }

    fn rest(&self) -> usize {
        self.b.len() - self.at
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        if self.rest() < n {
            return Err(bad("truncated"));
        }
        let s = &self.b[self.at..self.at + n];
        self.at += n;
        Ok(s)
    }

    fn i32(&mut self) -> Result<i32, CodecError> {
        let s = self.take(4)?;
        Ok(i32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        let s = self.take(4)?;
        Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }

    fn i64(&mut self) -> Result<i64, CodecError> {
        let s = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(s);
        Ok(i64::from_le_bytes(a))
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }

    fn cstring(&mut self) -> Result<String, CodecError> {
        let rest = &self.b[self.at..];
        let end = rest
            .iter()
            .position(|c| *c == 0)
            .ok_or_else(|| bad("unterminated cstring"))?;
        let s = std::str::from_utf8(&rest[..end])
            .map_err(|_| bad("cstring is not UTF-8"))?
            .to_owned();
        self.at += end + 1;
        Ok(s)
    }

    fn document(&mut self) -> Result<RawDocumentBuf, CodecError> {
        if self.rest() < 5 {
            return Err(bad("truncated document"));
        }
        let s = &self.b[self.at..self.at + 4];
        let len = i32::from_le_bytes([s[0], s[1], s[2], s[3]]);
        let len = usize::try_from(len).map_err(|_| bad("negative document length"))?;
        if len < 5 || len > self.rest() {
            return Err(bad("document length"));
        }
        let bytes = self.take(len)?.to_vec();
        RawDocumentBuf::from_bytes(bytes).map_err(|e| bad(format!("BSON: {e}")))
    }
}

/// Decodes the header of a message from its first 16 bytes.
#[must_use]
pub fn header(b: &[u8; 16]) -> MsgHeader {
    let i = |o: usize| i32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]]);
    MsgHeader {
        length: i(0),
        request_id: i(4),
        response_to: i(8),
        op_code: i(12),
    }
}

/// Checks a header's length before the body is read.
///
/// # Errors
/// [`CodecError::Length`] outside 16..=48,000,000.
pub fn check_length(h: &MsgHeader) -> Result<usize, CodecError> {
    let len = usize::try_from(h.length).map_err(|_| CodecError::Length(i64::from(h.length)))?;
    if !(16..=MAX_MESSAGE).contains(&len) {
        return Err(CodecError::Length(i64::from(h.length)));
    }
    Ok(len)
}

/// Decodes one whole message (`msg` is `length` bytes, header included).
/// An `OP_COMPRESSED` is decompressed and returned with the compressor it
/// used, so the reply can use the same one.
///
/// # Errors
/// [`CodecError`]; each closes the connection.
pub fn decode(msg: &[u8]) -> Result<(MsgHeader, Frame, Option<Compressor>), CodecError> {
    if msg.len() < 16 {
        return Err(CodecError::Length(i64::try_from(msg.len()).unwrap_or(0)));
    }
    let mut hb = [0u8; 16];
    hb.copy_from_slice(&msg[..16]);
    let h = header(&hb);
    let len = check_length(&h)?;
    if len != msg.len() {
        return Err(bad("length doesn't match the message"));
    }
    match h.op_code {
        OP_COMPRESSED => {
            let mut r = Reader::new(&msg[16..]);
            let original = r.i32()?;
            let size = r.i32()?;
            let id = r.u8()?;
            let size = usize::try_from(size)
                .ok()
                .filter(|s| *s <= MAX_MESSAGE)
                .ok_or_else(|| CodecError::Compression(format!("uncompressedSize {size}")))?;
            let c = Compressor::from_id(id)
                .ok_or_else(|| CodecError::Compression(format!("unknown compressor {id}")))?;
            let body = decompress(c, &msg[16 + 9..], size)?;
            if original == OP_COMPRESSED {
                return Err(bad("nested OP_COMPRESSED"));
            }
            let inner = MsgHeader {
                length: i32::try_from(16 + body.len()).map_err(|_| CodecError::Length(0))?,
                op_code: original,
                ..h
            };
            let f = body_frame(&inner, &body)?;
            Ok((inner, f, Some(c)))
        }
        _ => {
            let f = body_frame(&h, &msg[16..])?;
            Ok((h, f, None))
        }
    }
}

fn body_frame(h: &MsgHeader, body: &[u8]) -> Result<Frame, CodecError> {
    match h.op_code {
        OP_MSG => decode_msg(h, body).map(Frame::Msg),
        OP_QUERY => {
            let mut r = Reader::new(body);
            let flags = r.i32()?;
            let full_collection_name = r.cstring()?;
            let number_to_skip = r.i32()?;
            let number_to_return = r.i32()?;
            let query = r.document()?;
            // An optional returnFieldsSelector may follow; it is ignored.
            Ok(Frame::Query(OpQuery {
                flags,
                full_collection_name,
                number_to_skip,
                number_to_return,
                query,
            }))
        }
        OP_REPLY => {
            let mut r = Reader::new(body);
            let flags = r.i32()?;
            let cursor_id = r.i64()?;
            let starting_from = r.i32()?;
            let n = r.i32()?;
            let mut docs = Vec::new();
            for _ in 0..n.max(0) {
                docs.push(r.document()?);
            }
            Ok(Frame::Reply(OpReply {
                flags,
                cursor_id,
                starting_from,
                docs,
            }))
        }
        other => Err(CodecError::OpCode(other)),
    }
}

fn decode_msg(h: &MsgHeader, body: &[u8]) -> Result<OpMsg, CodecError> {
    let mut r = Reader::new(body);
    let flags = r.u32()?;
    // Bits 0-15 are required: unknown ones are an error; 16-31 optional.
    let unknown = flags & 0xFFFF & !(FLAG_CHECKSUM | FLAG_MORE_TO_COME);
    if unknown != 0 {
        return Err(CodecError::Flags(unknown));
    }
    let end = if flags & FLAG_CHECKSUM != 0 {
        if body.len() < 8 {
            return Err(bad("no room for the checksum"));
        }
        let at = body.len() - 4;
        let want = u32::from_le_bytes([body[at], body[at + 1], body[at + 2], body[at + 3]]);
        // The checksum covers the header and everything before it.
        let mut hb = Vec::with_capacity(16 + at);
        hb.extend_from_slice(&h.length.to_le_bytes());
        hb.extend_from_slice(&h.request_id.to_le_bytes());
        hb.extend_from_slice(&h.response_to.to_le_bytes());
        hb.extend_from_slice(&h.op_code.to_le_bytes());
        hb.extend_from_slice(&body[..at]);
        if crc32c::checksum(&hb) != want {
            return Err(CodecError::Checksum);
        }
        at
    } else {
        body.len()
    };
    let mut body_doc = None;
    let mut sequences = Vec::new();
    while r.at < end {
        match r.u8()? {
            0 => {
                if body_doc.is_some() {
                    return Err(bad("more than one kind 0 section"));
                }
                body_doc = Some(r.document()?);
            }
            1 => {
                let start = r.at;
                let size = r.i32()?;
                let size = usize::try_from(size).map_err(|_| bad("negative section size"))?;
                if size < 5 || start + size > end {
                    return Err(bad("section size"));
                }
                let identifier = r.cstring()?;
                let mut docs = Vec::new();
                while r.at < start + size {
                    docs.push(r.document()?);
                }
                if r.at != start + size {
                    return Err(bad("section overruns its size"));
                }
                sequences.push(DocSequence { identifier, docs });
            }
            k => return Err(bad(format!("section kind {k}"))),
        }
    }
    if r.at != end {
        return Err(bad("trailing bytes"));
    }
    let body = body_doc.ok_or_else(|| bad("no kind 0 section"))?;
    if body.as_bytes().len() > MAX_BSON + BODY_OVERHEAD {
        return Err(bad("command body over 16 MiB"));
    }
    Ok(OpMsg {
        flags,
        body,
        sequences,
    })
}

fn decompress(c: Compressor, data: &[u8], size: usize) -> Result<Vec<u8>, CodecError> {
    let err = |e: &dyn std::fmt::Display| CodecError::Compression(e.to_string());
    let out = match c {
        Compressor::Noop => data.to_vec(),
        Compressor::Snappy => {
            let n = snap::raw::decompress_len(data).map_err(|e| err(&e))?;
            if n != size {
                return Err(CodecError::Compression("snappy length mismatch".into()));
            }
            snap::raw::Decoder::new()
                .decompress_vec(data)
                .map_err(|e| err(&e))?
        }
        Compressor::Zlib => {
            let mut out = Vec::with_capacity(size);
            flate2::read::ZlibDecoder::new(data)
                .take(u64::try_from(size).unwrap_or(0) + 1)
                .read_to_end(&mut out)
                .map_err(|e| err(&e))?;
            out
        }
        Compressor::Zstd => zstd::bulk::decompress(data, size).map_err(|e| err(&e))?,
    };
    if out.len() != size {
        return Err(CodecError::Compression(
            "uncompressedSize doesn't match".into(),
        ));
    }
    Ok(out)
}

fn compress(c: Compressor, data: &[u8]) -> Result<Vec<u8>, CodecError> {
    let err = |e: &dyn std::fmt::Display| CodecError::Compression(e.to_string());
    match c {
        Compressor::Noop => Ok(data.to_vec()),
        Compressor::Snappy => snap::raw::Encoder::new()
            .compress_vec(data)
            .map_err(|e| err(&e)),
        Compressor::Zlib => {
            let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
            e.write_all(data).map_err(|x| err(&x))?;
            e.finish().map_err(|x| err(&x))
        }
        Compressor::Zstd => zstd::bulk::compress(data, 1).map_err(|e| err(&e)),
    }
}

fn put_header(out: &mut Vec<u8>, request_id: i32, response_to: i32, op_code: i32) {
    out.extend_from_slice(&[0, 0, 0, 0]);
    out.extend_from_slice(&request_id.to_le_bytes());
    out.extend_from_slice(&response_to.to_le_bytes());
    out.extend_from_slice(&op_code.to_le_bytes());
}

fn set_length(out: &mut [u8]) -> Result<(), CodecError> {
    let len = i32::try_from(out.len()).map_err(|_| CodecError::Length(0))?;
    out[..4].copy_from_slice(&len.to_le_bytes());
    Ok(())
}

/// Encodes `f` as the reply to `response_to`, compressed with `c` when
/// given (and not `Noop`).
///
/// # Errors
/// [`CodecError::Length`] past `i32`; compression failures.
pub fn encode(
    request_id: i32,
    response_to: i32,
    f: &Frame,
    c: Option<Compressor>,
) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::with_capacity(256);
    match f {
        Frame::Msg(m) => {
            put_header(&mut out, request_id, response_to, OP_MSG);
            out.extend_from_slice(&(m.flags & !FLAG_CHECKSUM).to_le_bytes());
            out.push(0);
            out.extend_from_slice(m.body.as_bytes());
            for s in &m.sequences {
                out.push(1);
                let start = out.len();
                out.extend_from_slice(&[0, 0, 0, 0]);
                out.extend_from_slice(s.identifier.as_bytes());
                out.push(0);
                for d in &s.docs {
                    out.extend_from_slice(d.as_bytes());
                }
                let size =
                    i32::try_from(out.len() - start).map_err(|_| CodecError::Length(0))?;
                out[start..start + 4].copy_from_slice(&size.to_le_bytes());
            }
        }
        Frame::Query(q) => {
            put_header(&mut out, request_id, response_to, OP_QUERY);
            out.extend_from_slice(&q.flags.to_le_bytes());
            out.extend_from_slice(q.full_collection_name.as_bytes());
            out.push(0);
            out.extend_from_slice(&q.number_to_skip.to_le_bytes());
            out.extend_from_slice(&q.number_to_return.to_le_bytes());
            out.extend_from_slice(q.query.as_bytes());
        }
        Frame::Reply(r) => {
            put_header(&mut out, request_id, response_to, OP_REPLY);
            out.extend_from_slice(&r.flags.to_le_bytes());
            out.extend_from_slice(&r.cursor_id.to_le_bytes());
            out.extend_from_slice(&r.starting_from.to_le_bytes());
            let n = i32::try_from(r.docs.len()).map_err(|_| CodecError::Length(0))?;
            out.extend_from_slice(&n.to_le_bytes());
            for d in &r.docs {
                out.extend_from_slice(d.as_bytes());
            }
        }
    }
    set_length(&mut out)?;
    match c {
        Some(c) if c != Compressor::Noop => {
            let op = i32::from_le_bytes([out[12], out[13], out[14], out[15]]);
            let body = &out[16..];
            let packed = compress(c, body)?;
            let mut z = Vec::with_capacity(packed.len() + 25);
            put_header(&mut z, request_id, response_to, OP_COMPRESSED);
            z.extend_from_slice(&op.to_le_bytes());
            let n = i32::try_from(body.len()).map_err(|_| CodecError::Length(0))?;
            z.extend_from_slice(&n.to_le_bytes());
            z.push(c.id());
            z.extend_from_slice(&packed);
            set_length(&mut z)?;
            Ok(z)
        }
        _ => Ok(out),
    }
}

/// Reads one whole message from `r`: `None` at a clean end of stream.
///
/// # Errors
/// [`CodecError::Length`] before allocating; [`CodecError::Eof`] inside a
/// message; I/O errors.
pub async fn read_message<R: AsyncRead + Unpin>(r: &mut R) -> Result<Option<Vec<u8>>, CodecError> {
    let mut hb = [0u8; 16];
    let mut got = 0;
    while got < 16 {
        let n = r
            .read(&mut hb[got..])
            .await
            .map_err(|e| CodecError::Io(e.to_string()))?;
        if n == 0 {
            return if got == 0 {
                Ok(None)
            } else {
                Err(CodecError::Eof)
            };
        }
        got += n;
    }
    let h = header(&hb);
    let len = check_length(&h)?;
    let mut msg = vec![0u8; len];
    msg[..16].copy_from_slice(&hb);
    r.read_exact(&mut msg[16..]).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            CodecError::Eof
        } else {
            CodecError::Io(e.to_string())
        }
    })?;
    Ok(Some(msg))
}

#[cfg(test)]
mod tests;
