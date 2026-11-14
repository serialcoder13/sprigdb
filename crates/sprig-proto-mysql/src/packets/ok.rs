//! OK and EOF packets, and the session-state tracker entries an OK packet
//! carries under `CLIENT_SESSION_TRACK`.

use bytes::Bytes;

use super::{Caps, EOF_HEADER, OK_HEADER, StatusFlags};
use crate::buf::{Reader, Writer};
use crate::proto_error::{ProtoError, ProtoResult};

/// Session-state tracker entry types.
pub mod session_track {
    /// A system variable changed: name and value.
    pub const SYSTEM_VARIABLES: u8 = 0;
    /// The default schema changed.
    pub const SCHEMA: u8 = 1;
    /// Some tracked state changed: `"1"`.
    pub const STATE_CHANGE: u8 = 2;
    /// GTIDs of the committed transaction.
    pub const GTIDS: u8 = 3;
    /// `SET TRANSACTION` characteristics to replay.
    pub const TRANSACTION_CHARACTERISTICS: u8 = 4;
    /// The 8-character transaction state.
    pub const TRANSACTION_STATE: u8 = 5;
}

/// An OK packet, or the OK-form terminator (header 0xFE) of a resultset
/// under `DEPRECATE_EOF`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OkPacket {
    /// 0x00, or 0xFE when the packet ends a resultset.
    pub header: u8,
    /// Affected rows.
    pub affected_rows: u64,
    /// Last insert id.
    pub last_insert_id: u64,
    /// Status flags.
    pub status: StatusFlags,
    /// Warning count.
    pub warnings: u16,
    /// Human-readable info, such as `Rows matched: 1  Changed: 1`.
    pub info: Bytes,
    /// The raw session-state block (tracker entries) when
    /// `SESSION_STATE_CHANGED` is set; see [`OkPacket::session_changes`].
    pub session_state: Bytes,
}

impl OkPacket {
    /// An empty OK (header 0x00) with these status flags.
    #[must_use]
    pub fn new(status: StatusFlags) -> Self {
        Self {
            status,
            ..Self::default()
        }
    }

    /// Decodes an OK packet (header 0x00 or 0xFE) under the negotiated
    /// capabilities.
    pub fn decode(payload: &Bytes, caps: Caps) -> ProtoResult<Self> {
        let mut r = Reader::new(payload);
        let header = r.u8()?;
        if header != OK_HEADER && header != EOF_HEADER {
            return Err(ProtoError::malformed(format!(
                "OK packet header {header:#04x}"
            )));
        }
        let affected_rows = r.lenenc_int()?;
        let last_insert_id = r.lenenc_int()?;
        let (status, warnings) = if caps.contains(Caps::PROTOCOL_41) {
            (StatusFlags::from_bits_retain(r.u16()?), r.u16()?)
        } else if caps.contains(Caps::TRANSACTIONS) {
            (StatusFlags::from_bits_retain(r.u16()?), 0)
        } else {
            (StatusFlags::empty(), 0)
        };
        let mut info = Bytes::new();
        let mut session_state = Bytes::new();
        if caps.contains(Caps::SESSION_TRACK) {
            if !r.is_empty() {
                info = payload.slice_ref(r.lenenc_bytes()?);
            }
            if status.contains(StatusFlags::SESSION_STATE_CHANGED) && !r.is_empty() {
                session_state = payload.slice_ref(r.lenenc_bytes()?);
            }
        } else {
            // Servers write the info as a length-encoded string here too
            // (MariaDB's and MySQL's net_store_data); accept a bare one.
            let rest = r.peek_rest();
            let mut l = Reader::new(rest);
            info = match l.lenenc_bytes() {
                Ok(b) if l.is_empty() && !rest.is_empty() => payload.slice_ref(b),
                _ => payload.slice_ref(rest),
            };
        }
        Ok(Self {
            header,
            affected_rows,
            last_insert_id,
            status,
            warnings,
            info,
            session_state,
        })
    }

    /// Encodes for a peer with these capabilities. Without `SESSION_TRACK`
    /// the session-state block is dropped and `SESSION_STATE_CHANGED`
    /// cleared.
    #[must_use]
    pub fn encode(&self, caps: Caps) -> Bytes {
        let mut w = Writer::with_capacity(16 + self.info.len() + self.session_state.len());
        w.u8(self.header)
            .lenenc_int(self.affected_rows)
            .lenenc_int(self.last_insert_id);
        let track = caps.contains(Caps::SESSION_TRACK);
        let mut status = self.status;
        if !track {
            status.remove(StatusFlags::SESSION_STATE_CHANGED);
        }
        if caps.contains(Caps::PROTOCOL_41) {
            w.u16(status.bits()).u16(self.warnings);
        } else if caps.contains(Caps::TRANSACTIONS) {
            w.u16(status.bits());
        }
        if track {
            let changed = status.contains(StatusFlags::SESSION_STATE_CHANGED);
            if changed || !self.info.is_empty() {
                w.lenenc_bytes(&self.info);
            }
            if changed {
                w.lenenc_bytes(&self.session_state);
            }
        } else {
            // Length-encoded even without SESSION_TRACK, as MariaDB and MySQL
            // send it: libmysql, mysql_async and the JDBC drivers read it so.
            if !self.info.is_empty() {
                w.lenenc_bytes(&self.info);
            }
        }
        w.into_bytes()
    }

    /// Decodes the session-state block.
    pub fn session_changes(&self) -> ProtoResult<Vec<SessionChange>> {
        SessionChange::decode_all(&self.session_state)
    }

    /// Replaces the session-state block and sets or clears
    /// `SESSION_STATE_CHANGED` to match.
    pub fn set_session_changes(&mut self, changes: &[SessionChange]) {
        self.session_state = SessionChange::encode_all(changes);
        self.status
            .set(StatusFlags::SESSION_STATE_CHANGED, !changes.is_empty());
    }
}

/// One session-state tracker entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionChange {
    /// A system variable's new value.
    SystemVariable {
        /// Variable name.
        name: Bytes,
        /// New value.
        value: Bytes,
    },
    /// The new default schema.
    Schema(Bytes),
    /// Tracked state changed (`true` for `"1"`).
    StateChange(bool),
    /// GTIDs of the committed transaction.
    Gtids {
        /// Encoding specification; 0 is the only one defined.
        encoding: u8,
        /// The GTID set, as text.
        gtids: Bytes,
    },
    /// `SET TRANSACTION` statements that recreate the characteristics.
    TransactionCharacteristics(Bytes),
    /// The 8-character transaction state, such as `T_______`.
    TransactionState(Bytes),
    /// An entry type this crate doesn't know, kept as its raw data.
    Unknown {
        /// Entry type.
        kind: u8,
        /// The entry's data.
        data: Bytes,
    },
}

impl SessionChange {
    /// Decodes every entry in a session-state block.
    pub fn decode_all(block: &Bytes) -> ProtoResult<Vec<SessionChange>> {
        let mut r = Reader::new(block);
        let mut out = Vec::new();
        while !r.is_empty() {
            let kind = r.u8()?;
            let data = block.slice_ref(r.lenenc_bytes()?);
            let mut d = Reader::new(&data);
            let change = match kind {
                // MySQL sends one name and value per entry; MariaDB packs
                // every changed variable into one entry (`timestamp` and
                // `last_gtid` together after a COMMIT under a fixed
                // timestamp: reading only the first pair lost the commit's
                // GTID, and with it read-your-writes).
                session_track::SYSTEM_VARIABLES => loop {
                    let name = data.slice_ref(d.lenenc_bytes()?);
                    let value = data.slice_ref(d.lenenc_bytes()?);
                    let v = SessionChange::SystemVariable { name, value };
                    if d.is_empty() {
                        break v;
                    }
                    out.push(v);
                },
                session_track::SCHEMA => SessionChange::Schema(data.slice_ref(d.lenenc_bytes()?)),
                // Servers send the flag bare (`02 01 31`: length 1, then
                // '1'); libmysql rejects any other length. A nested lenenc
                // form (`02 02 01 31`) is accepted too, to be lenient.
                session_track::STATE_CHANGE => {
                    SessionChange::StateChange(matches!(&data[..], b"1" | [1, b'1']))
                }
                session_track::GTIDS => {
                    let encoding = d.u8()?;
                    let gtids = data.slice_ref(d.lenenc_bytes()?);
                    SessionChange::Gtids { encoding, gtids }
                }
                session_track::TRANSACTION_CHARACTERISTICS => {
                    SessionChange::TransactionCharacteristics(data.slice_ref(d.lenenc_bytes()?))
                }
                session_track::TRANSACTION_STATE => {
                    SessionChange::TransactionState(data.slice_ref(d.lenenc_bytes()?))
                }
                _ => SessionChange::Unknown {
                    kind,
                    data: data.clone(),
                },
            };
            out.push(change);
        }
        Ok(out)
    }

    /// Encodes entries as a session-state block.
    #[must_use]
    pub fn encode_all(changes: &[SessionChange]) -> Bytes {
        let mut w = Writer::new();
        for c in changes {
            let mut d = Writer::new();
            let kind = match c {
                SessionChange::SystemVariable { name, value } => {
                    d.lenenc_bytes(name).lenenc_bytes(value);
                    session_track::SYSTEM_VARIABLES
                }
                SessionChange::Schema(s) => {
                    d.lenenc_bytes(s);
                    session_track::SCHEMA
                }
                SessionChange::StateChange(on) => {
                    d.bytes(if *on { b"1" } else { b"0" });
                    session_track::STATE_CHANGE
                }
                SessionChange::Gtids { encoding, gtids } => {
                    d.u8(*encoding).lenenc_bytes(gtids);
                    session_track::GTIDS
                }
                SessionChange::TransactionCharacteristics(s) => {
                    d.lenenc_bytes(s);
                    session_track::TRANSACTION_CHARACTERISTICS
                }
                SessionChange::TransactionState(s) => {
                    d.lenenc_bytes(s);
                    session_track::TRANSACTION_STATE
                }
                SessionChange::Unknown { kind, data } => {
                    d.bytes(data);
                    *kind
                }
            };
            w.u8(kind).lenenc_bytes(&d.into_bytes());
        }
        w.into_bytes()
    }
}

/// An EOF packet: ends column definitions and rows when `DEPRECATE_EOF`
/// isn't negotiated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EofPacket {
    /// Warning count.
    pub warnings: u16,
    /// Status flags.
    pub status: StatusFlags,
}

impl EofPacket {
    /// Decodes an EOF packet.
    pub fn decode(payload: &[u8], caps: Caps) -> ProtoResult<Self> {
        let mut r = Reader::new(payload);
        let header = r.u8()?;
        if header != EOF_HEADER {
            return Err(ProtoError::malformed(format!(
                "EOF packet header {header:#04x}"
            )));
        }
        if !caps.contains(Caps::PROTOCOL_41) {
            return Ok(Self::default());
        }
        let warnings = r.u16()?;
        let status = StatusFlags::from_bits_retain(r.u16()?);
        Ok(Self { warnings, status })
    }

    /// Encodes the packet.
    #[must_use]
    pub fn encode(&self, caps: Caps) -> Bytes {
        let mut w = Writer::with_capacity(5);
        w.u8(EOF_HEADER);
        if caps.contains(Caps::PROTOCOL_41) {
            w.u16(self.warnings).u16(self.status.bits());
        }
        w.into_bytes()
    }
}
