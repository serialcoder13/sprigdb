//! [`ProtoError`]: why a packet couldn't be framed, parsed or built.

use sprig_common::SprigError;

/// A framing or parsing failure. Every decoder in this crate returns one
/// instead of panicking, whatever the input.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProtoError {
    /// The input ended inside a field.
    #[error("truncated packet: {what} needs {need} bytes at offset {at}, {have} left")]
    Truncated {
        /// The field being read.
        what: &'static str,
        /// Offset of the field in the payload.
        at: usize,
        /// Bytes the field needs.
        need: usize,
        /// Bytes left in the payload.
        have: usize,
    },
    /// A field holds a value the protocol doesn't allow.
    #[error("malformed packet: {0}")]
    Malformed(String),
    /// A logical packet is larger than the negotiated maximum (1153).
    #[error("packet of {size} bytes exceeds max_packet {max}")]
    PacketTooLarge {
        /// Size reached so far, in bytes.
        size: usize,
        /// The limit.
        max: usize,
    },
    /// A sequence id isn't the expected one (1156).
    #[error("packets out of order: expected sequence {expected}, got {got}")]
    OutOfOrder {
        /// The sequence id the codec expected.
        expected: u8,
        /// The sequence id that arrived.
        got: u8,
    },
    /// A compressed frame doesn't inflate to its declared length, or the
    /// compressor failed.
    #[error("compressed frame: {0}")]
    Compression(String),
    /// The transport failed.
    #[error("i/o: {0}")]
    Io(#[from] std::io::Error),
}

impl ProtoError {
    /// A [`ProtoError::Malformed`] with this text.
    pub fn malformed(msg: impl Into<String>) -> Self {
        Self::Malformed(msg.into())
    }

    /// The MySQL error number and SQLSTATE a server sends for this failure
    /// before it closes the connection: 1156, 1153, or 1835 (malformed).
    #[must_use]
    pub fn errno(&self) -> (u16, [u8; 5]) {
        match self {
            Self::OutOfOrder { .. } => (1156, *b"08S01"),
            Self::PacketTooLarge { .. } => (1153, *b"08S01"),
            Self::Io(_) | Self::Compression(_) => (1158, *b"08S01"),
            Self::Truncated { .. } | Self::Malformed(_) => (1835, *b"HY000"),
        }
    }
}

impl From<ProtoError> for SprigError {
    fn from(e: ProtoError) -> Self {
        let (errno, sqlstate) = e.errno();
        let message = match &e {
            ProtoError::OutOfOrder { .. } => "Got packets out of order".to_owned(),
            ProtoError::PacketTooLarge { .. } => {
                "Got a packet bigger than 'max_allowed_packet' bytes".to_owned()
            }
            ProtoError::Io(_) | ProtoError::Compression(_) => {
                "Got an error reading communication packets".to_owned()
            }
            ProtoError::Truncated { .. } | ProtoError::Malformed(_) => {
                "Malformed communication packet".to_owned()
            }
        };
        SprigError::standard(errno, sqlstate, message).with_source(e)
    }
}

/// The result type of this crate's codecs.
pub type ProtoResult<T> = Result<T, ProtoError>;
