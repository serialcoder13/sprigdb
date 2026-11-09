//! Record batches, magic v2 (the protocol guide's "Record Batch"): the
//! 61-byte batch header, CRC-32C over everything from the attributes on,
//! records with varint fields and headers, and compressed record sections.
//!
//! Produce (step 10) decodes what producers send and Fetch (step 11)
//! encodes what consumers read; both use [`RecordBatch`]. Older message
//! formats (magic 0 and 1) are refused: SprigDB serves Produce from v3 and
//! Fetch from v4, which carry v2 batches only.

use crate::compression::Compression;
use crate::crc32c;
use crate::wire::{CodecError, Reader, Result, Writer};

/// The batch header's size, up to and including the record count.
pub const BATCH_OVERHEAD: usize = 61;
/// Bytes from the start of a batch to its CRC field's end; the CRC covers
/// the rest.
const CRC_END: usize = 21;
/// Bytes of `baseOffset` and `batchLength`, which `batchLength` doesn't
/// count.
const LOG_OVERHEAD: usize = 12;

/// Attribute bit: timestamps are log-append time.
pub const ATTR_LOG_APPEND_TIME: i16 = 0x08;
/// Attribute bit: a transactional batch.
pub const ATTR_TRANSACTIONAL: i16 = 0x10;
/// Attribute bit: a control batch (transaction markers).
pub const ATTR_CONTROL: i16 = 0x20;

/// One record header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// The header key.
    pub key: String,
    /// The value; `None` for a null value.
    pub value: Option<Vec<u8>>,
}

/// One record of a batch.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Record {
    /// Record attributes (unused by Kafka, always 0).
    pub attributes: i8,
    /// Milliseconds after the batch's base timestamp.
    pub timestamp_delta: i64,
    /// Position after the batch's base offset.
    pub offset_delta: i32,
    /// The key; `None` for no key.
    pub key: Option<Vec<u8>>,
    /// The value; `None` for a tombstone.
    pub value: Option<Vec<u8>>,
    /// Headers, in order.
    pub headers: Vec<Header>,
}

/// A record batch, magic 2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatch {
    /// The first record's offset (the producer sends 0).
    pub base_offset: i64,
    /// The leader epoch the batch was appended under (-1 from producers).
    pub partition_leader_epoch: i32,
    /// Attributes: compression in bits 0-2, timestamp type, transactional,
    /// control.
    pub attributes: i16,
    /// The last record's offset delta.
    pub last_offset_delta: i32,
    /// The first record's timestamp.
    pub base_timestamp: i64,
    /// The largest timestamp in the batch.
    pub max_timestamp: i64,
    /// The idempotent producer's id, or -1.
    pub producer_id: i64,
    /// The producer's epoch, or -1.
    pub producer_epoch: i16,
    /// The first record's sequence number, or -1.
    pub base_sequence: i32,
    /// The records.
    pub records: Vec<Record>,
}

impl Default for RecordBatch {
    fn default() -> Self {
        Self {
            base_offset: 0,
            partition_leader_epoch: -1,
            attributes: 0,
            last_offset_delta: -1,
            base_timestamp: -1,
            max_timestamp: -1,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: Vec::new(),
        }
    }
}

impl RecordBatch {
    /// The batch's compression codec.
    ///
    /// # Errors
    /// `UnsupportedCompression` for an unknown codec id.
    pub fn compression(&self) -> Result<Compression> {
        Compression::from_attributes(self.attributes)
    }

    /// Whether the batch is transactional.
    #[must_use]
    pub fn is_transactional(&self) -> bool {
        self.attributes & ATTR_TRANSACTIONAL != 0
    }

    /// Whether the batch holds control records.
    #[must_use]
    pub fn is_control(&self) -> bool {
        self.attributes & ATTR_CONTROL != 0
    }

    /// The offset of the batch's last record.
    #[must_use]
    pub fn last_offset(&self) -> i64 {
        self.base_offset + i64::from(self.last_offset_delta)
    }

    /// Encodes the batch, compressing its records with the codec in its
    /// attributes, and computes the CRC.
    ///
    /// # Errors
    /// `UnsupportedCompression` (snappy, LZ4); `Invalid` for more than
    /// `i32::MAX` records or bytes.
    pub fn encode(&self, w: &mut Writer) -> Result<()> {
        let codec = self.compression()?;
        let mut body = Writer::new();
        for r in &self.records {
            encode_record(&mut body, r)?;
        }
        let body = codec.compress(body.as_slice())?;
        let start = w.len();
        w.i64(self.base_offset);
        let len = BATCH_OVERHEAD - LOG_OVERHEAD + body.len();
        w.i32(i32::try_from(len).map_err(|_| CodecError::Invalid("batch length"))?);
        w.i32(self.partition_leader_epoch);
        w.i8(2);
        w.u32(0); // CRC, patched below
        w.i16(self.attributes);
        w.i32(self.last_offset_delta);
        w.i64(self.base_timestamp);
        w.i64(self.max_timestamp);
        w.i64(self.producer_id);
        w.i16(self.producer_epoch);
        w.i32(self.base_sequence);
        w.i32(i32::try_from(self.records.len()).map_err(|_| CodecError::Invalid("record count"))?);
        w.raw(&body);
        let crc = crc32c::crc32c(&w.as_slice()[start + CRC_END..]);
        w.as_mut_slice()[start + CRC_END - 4..start + CRC_END].copy_from_slice(&crc.to_be_bytes());
        Ok(())
    }

    /// Encodes into a new buffer.
    ///
    /// # Errors
    /// As [`RecordBatch::encode`].
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut w = Writer::new();
        self.encode(&mut w)?;
        Ok(w.into_inner())
    }

    /// Decodes one batch from the start of `r`, checking its magic, CRC
    /// and lengths, and decompressing its records into at most
    /// `max_uncompressed` bytes.
    ///
    /// # Errors
    /// `Truncated` if `r` ends inside the batch; `CorruptRecord` for a bad
    /// magic, CRC or record; `UnsupportedCompression`.
    pub fn decode(r: &mut Reader<'_>, max_uncompressed: usize) -> Result<Self> {
        let whole = r.rest();
        let base_offset = r.i64()?;
        let len = r.i32()?;
        let len = usize::try_from(len).map_err(|_| CodecError::CorruptRecord("batch length"))?;
        if len < BATCH_OVERHEAD - LOG_OVERHEAD {
            return Err(CodecError::CorruptRecord("batch length"));
        }
        let batch = r.take(len)?;
        let mut b = Reader::new(batch);
        let partition_leader_epoch = b.i32()?;
        let magic = b.i8()?;
        if magic != 2 {
            return Err(CodecError::CorruptRecord(
                "magic (only v2 batches are served)",
            ));
        }
        let crc = b.u32()?;
        if crc32c::crc32c(&whole[CRC_END..LOG_OVERHEAD + len]) != crc {
            return Err(CodecError::CorruptRecord("CRC"));
        }
        let attributes = b.i16()?;
        let last_offset_delta = b.i32()?;
        let base_timestamp = b.i64()?;
        let max_timestamp = b.i64()?;
        let producer_id = b.i64()?;
        let producer_epoch = b.i16()?;
        let base_sequence = b.i32()?;
        let count = b.i32()?;
        let count =
            usize::try_from(count).map_err(|_| CodecError::CorruptRecord("record count"))?;
        let codec = Compression::from_attributes(attributes)?;
        let raw = codec.decompress(b.rest(), max_uncompressed)?;
        let mut rr = Reader::new(&raw);
        // Every record takes at least 7 bytes.
        if count > rr.remaining() / 7 + 1 {
            return Err(CodecError::CorruptRecord("record count"));
        }
        let mut records = Vec::with_capacity(count);
        for _ in 0..count {
            records.push(decode_record(&mut rr)?);
        }
        if !rr.is_empty() {
            return Err(CodecError::CorruptRecord("bytes after the last record"));
        }
        Ok(Self {
            base_offset,
            partition_leader_epoch,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }

    /// Decodes every batch of a `records` field. A partial batch at the end
    /// (as fetch responses may carry) is skipped when `allow_partial`, and
    /// an error otherwise.
    ///
    /// # Errors
    /// As [`RecordBatch::decode`].
    pub fn decode_all(
        buf: &[u8],
        max_uncompressed: usize,
        allow_partial: bool,
    ) -> Result<Vec<Self>> {
        let mut r = Reader::new(buf);
        let mut out = Vec::new();
        while !r.is_empty() {
            if allow_partial && r.remaining() < LOG_OVERHEAD {
                break;
            }
            let mut peek = r.clone();
            if allow_partial {
                let _ = peek.i64()?;
                let len = peek.i32()?;
                if usize::try_from(len).is_ok_and(|l| l > peek.remaining()) {
                    break;
                }
            }
            out.push(Self::decode(&mut r, max_uncompressed)?);
        }
        Ok(out)
    }
}

fn varint_len(v: usize) -> Result<i32> {
    i32::try_from(v).map_err(|_| CodecError::Invalid("record field length"))
}

fn encode_record(w: &mut Writer, r: &Record) -> Result<()> {
    let mut body = Writer::new();
    body.i8(r.attributes);
    body.varlong(r.timestamp_delta);
    body.varint(r.offset_delta);
    match &r.key {
        None => body.varint(-1),
        Some(k) => {
            body.varint(varint_len(k.len())?);
            body.raw(k);
        }
    }
    match &r.value {
        None => body.varint(-1),
        Some(v) => {
            body.varint(varint_len(v.len())?);
            body.raw(v);
        }
    }
    body.varint(varint_len(r.headers.len())?);
    for h in &r.headers {
        body.varint(varint_len(h.key.len())?);
        body.raw(h.key.as_bytes());
        match &h.value {
            None => body.varint(-1),
            Some(v) => {
                body.varint(varint_len(v.len())?);
                body.raw(v);
            }
        }
    }
    w.varint(varint_len(body.len())?);
    w.raw(body.as_slice());
    Ok(())
}

fn varbytes(r: &mut Reader<'_>) -> Result<Option<Vec<u8>>> {
    let n = r.varint()?;
    if n == -1 {
        return Ok(None);
    }
    let n = usize::try_from(n).map_err(|_| CodecError::CorruptRecord("record field length"))?;
    Ok(Some(
        r.take(n)
            .map_err(|_| CodecError::CorruptRecord("record field length"))?
            .to_vec(),
    ))
}

fn decode_record(r: &mut Reader<'_>) -> Result<Record> {
    let len = r.varint()?;
    let len = usize::try_from(len).map_err(|_| CodecError::CorruptRecord("record length"))?;
    let body = r
        .take(len)
        .map_err(|_| CodecError::CorruptRecord("record length"))?;
    let mut b = Reader::new(body);
    let attributes = b.i8()?;
    let timestamp_delta = b.varlong()?;
    let offset_delta = b.varint()?;
    let key = varbytes(&mut b)?;
    let value = varbytes(&mut b)?;
    let n = b.varint()?;
    let n = usize::try_from(n).map_err(|_| CodecError::CorruptRecord("header count"))?;
    if n > b.remaining() {
        return Err(CodecError::CorruptRecord("header count"));
    }
    let mut headers = Vec::with_capacity(n);
    for _ in 0..n {
        let key = varbytes(&mut b)?.ok_or(CodecError::CorruptRecord("null header key"))?;
        let key = String::from_utf8(key).map_err(|_| CodecError::CorruptRecord("header key"))?;
        let value = varbytes(&mut b)?;
        headers.push(Header { key, value });
    }
    if !b.is_empty() {
        return Err(CodecError::CorruptRecord("bytes after a record"));
    }
    Ok(Record {
        attributes,
        timestamp_delta,
        offset_delta,
        key,
        value,
        headers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(attributes: i16) -> RecordBatch {
        RecordBatch {
            base_offset: 1205,
            partition_leader_epoch: 3,
            attributes,
            last_offset_delta: 1,
            base_timestamp: 1_762_508_100_123,
            max_timestamp: 1_762_508_100_125,
            producer_id: 42,
            producer_epoch: 0,
            base_sequence: 7,
            records: vec![
                Record {
                    key: Some(b"7001".to_vec()),
                    value: Some(br#"{"type":"order.paid"}"#.to_vec()),
                    headers: vec![Header {
                        key: "source".into(),
                        value: Some(b"checkout".to_vec()),
                    }],
                    ..Record::default()
                },
                Record {
                    timestamp_delta: 2,
                    offset_delta: 1,
                    key: None,
                    value: None,
                    headers: vec![Header {
                        key: "h".into(),
                        value: None,
                    }],
                    ..Record::default()
                },
            ],
        }
    }

    #[test]
    fn golden_uncompressed_batch() {
        // Built by hand from the protocol guide's layout.
        let b = RecordBatch {
            base_offset: 0,
            records: vec![Record {
                key: Some(b"k".to_vec()),
                value: Some(b"v".to_vec()),
                ..Record::default()
            }],
            last_offset_delta: 0,
            base_timestamp: 5,
            max_timestamp: 5,
            ..RecordBatch::default()
        };
        let bytes = b.to_bytes().unwrap();
        let record: &[u8] = &[
            0x10, // length 8 (zig-zag)
            0x00, // attributes
            0x00, // timestamp delta 0
            0x00, // offset delta 0
            0x02, b'k', // key
            0x02, b'v', // value
            0x00, // no headers
        ];
        let mut want = vec![];
        want.extend_from_slice(&0i64.to_be_bytes());
        want.extend_from_slice(&i32::try_from(49 + record.len()).unwrap().to_be_bytes());
        want.extend_from_slice(&(-1i32).to_be_bytes()); // leader epoch
        want.push(2); // magic
        let crc_at = want.len();
        want.extend_from_slice(&[0; 4]);
        want.extend_from_slice(&0i16.to_be_bytes()); // attributes
        want.extend_from_slice(&0i32.to_be_bytes()); // last offset delta
        want.extend_from_slice(&5i64.to_be_bytes());
        want.extend_from_slice(&5i64.to_be_bytes());
        want.extend_from_slice(&(-1i64).to_be_bytes());
        want.extend_from_slice(&(-1i16).to_be_bytes());
        want.extend_from_slice(&(-1i32).to_be_bytes());
        want.extend_from_slice(&1i32.to_be_bytes());
        want.extend_from_slice(record);
        let crc = crc32c::crc32c(&want[crc_at + 4..]);
        want[crc_at..crc_at + 4].copy_from_slice(&crc.to_be_bytes());
        assert_eq!(bytes, want);
        assert_eq!(bytes.len(), BATCH_OVERHEAD + record.len());
        let back = RecordBatch::decode(&mut Reader::new(&bytes), 1 << 20).unwrap();
        assert_eq!(back, b);
    }

    #[test]
    fn round_trip_every_supported_codec() {
        for codec in [Compression::None, Compression::Gzip, Compression::Zstd] {
            let b = sample(i16::from(codec.id()));
            let bytes = b.to_bytes().unwrap();
            let back = RecordBatch::decode(&mut Reader::new(&bytes), 1 << 20).unwrap();
            assert_eq!(back, b, "{codec:?}");
            assert_eq!(back.compression().unwrap(), codec);
        }
    }

    #[test]
    fn a_flipped_bit_fails_the_crc() {
        let mut bytes = sample(0).to_bytes().unwrap();
        let n = bytes.len();
        bytes[n - 3] ^= 0x01;
        assert_eq!(
            RecordBatch::decode(&mut Reader::new(&bytes), 1 << 20),
            Err(CodecError::CorruptRecord("CRC"))
        );
    }

    #[test]
    fn snappy_and_lz4_batches_are_refused() {
        let mut bytes = sample(0).to_bytes().unwrap();
        // Rewrite the attributes to snappy and fix the CRC: the codec check
        // comes after the CRC.
        bytes[22] = 0x02;
        let crc = crc32c::crc32c(&bytes[21..]);
        bytes[17..21].copy_from_slice(&crc.to_be_bytes());
        assert_eq!(
            RecordBatch::decode(&mut Reader::new(&bytes), 1 << 20),
            Err(CodecError::UnsupportedCompression(2))
        );
        assert_eq!(
            sample(3).to_bytes(),
            Err(CodecError::UnsupportedCompression(3))
        );
    }

    #[test]
    fn several_batches_and_a_partial_tail() {
        let a = sample(0).to_bytes().unwrap();
        let b = sample(4).to_bytes().unwrap();
        let mut all = [a.clone(), b].concat();
        assert_eq!(
            RecordBatch::decode_all(&all, 1 << 20, false).unwrap().len(),
            2
        );
        all.extend_from_slice(&a[..30]);
        assert_eq!(
            RecordBatch::decode_all(&all, 1 << 20, true).unwrap().len(),
            2
        );
        assert!(RecordBatch::decode_all(&all, 1 << 20, false).is_err());
    }
}
