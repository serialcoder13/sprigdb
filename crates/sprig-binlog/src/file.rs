//! File mode: reading binlog files, such as archived ones (chapter 38,
//! "Retention and archive"), with the same [`Decoder`] as the stream.

use std::path::Path;

use sprig_common::Result;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};

use crate::buf::{corrupt, disconnected};
use crate::consts::{BINLOG_MAGIC, HEADER_LEN};
use crate::event::{Decoder, Event, EventHeader, TableFilter};
use crate::gtid::GtidPos;

/// Largest event accepted from a file (1 GiB, MariaDB's `max_allowed_packet` cap).
const MAX_EVENT: usize = 1 << 30;

/// Reads events from a binlog file.
///
/// Encrypted binlog files fail with `ER_SPRIG_BINLOG_UNSUPPORTED`: only the
/// server can decrypt them, so read those through [`crate::BinlogStream`].
pub struct BinlogFile<R> {
    reader: BufReader<R>,
    decoder: Decoder,
    offset: u64,
}

impl<R> std::fmt::Debug for BinlogFile<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinlogFile")
            .field("offset", &self.offset)
            .field("decoder", &self.decoder)
            .finish_non_exhaustive()
    }
}

impl BinlogFile<tokio::fs::File> {
    /// Opens a binlog file and checks its magic number.
    ///
    /// # Errors
    /// If the file can't be read or isn't a binlog.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let f = tokio::fs::File::open(path).await.map_err(|e| {
            corrupt(format!("opening binlog file {}: {e}", path.display())).with_source(e)
        })?;
        Self::new(f).await
    }
}

impl<R: AsyncRead + Unpin> BinlogFile<R> {
    /// Reads a binlog from any reader positioned at its start.
    ///
    /// # Errors
    /// If the magic number is wrong.
    pub async fn new(reader: R) -> Result<Self> {
        let mut reader = BufReader::with_capacity(256 * 1024, reader);
        let mut magic = [0u8; 4];
        reader
            .read_exact(&mut magic)
            .await
            .map_err(|e| corrupt(format!("reading binlog magic: {e}")))?;
        if magic != BINLOG_MAGIC {
            return Err(corrupt("not a binlog file (bad magic number)"));
        }
        Ok(Self {
            reader,
            // The format description says whether events carry checksums.
            decoder: Decoder::new(false),
            offset: 4,
        })
    }

    /// Decodes rows only for tables `filter` accepts.
    pub fn set_filter(&mut self, filter: Option<TableFilter>) {
        self.decoder.set_filter(filter);
    }

    /// The next event, or `None` at the end of the file.
    ///
    /// # Errors
    /// `ER_SPRIG_BINLOG_CORRUPT` on a bad or truncated event.
    pub async fn next(&mut self) -> Result<Option<Event>> {
        loop {
            let mut head = [0u8; HEADER_LEN];
            match self.reader.read_exact(&mut head[..1]).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(disconnected(format!("reading binlog: {e}"))),
            }
            self.reader
                .read_exact(&mut head[1..])
                .await
                .map_err(|e| corrupt(format!("truncated event header at {}: {e}", self.offset)))?;
            let size = u32::from_le_bytes([head[9], head[10], head[11], head[12]]) as usize;
            if !(HEADER_LEN..=MAX_EVENT).contains(&size) {
                return Err(corrupt(format!(
                    "event size {size} at offset {}",
                    self.offset
                )));
            }
            let mut raw = vec![0u8; size];
            raw[..HEADER_LEN].copy_from_slice(&head);
            self.reader
                .read_exact(&mut raw[HEADER_LEN..])
                .await
                .map_err(|e| corrupt(format!("truncated event at {}: {e}", self.offset)))?;
            self.offset += size as u64;
            if let Some(ev) = self.decoder.decode(&raw)? {
                return Ok(Some(ev));
            }
        }
    }

    /// Offset of the next event in the file.
    #[must_use]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// The position after the last complete transaction group read.
    #[must_use]
    pub fn position(&self) -> &GtidPos {
        self.decoder.position()
    }

    /// The header of the last event read.
    #[must_use]
    pub fn last_header(&self) -> &EventHeader {
        self.decoder.last_header()
    }
}
