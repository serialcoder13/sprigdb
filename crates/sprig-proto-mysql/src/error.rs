//! ERR packets and the persona error maps (chapter 05, "Error packets",
//! build step 11).
//!
//! [`errmap`] turns a [`SprigError`] into the (errno, SQLSTATE, message)
//! triple a persona sends; [`StdErr`] lists the standard errors the wire
//! layer raises itself. Messages are cut to 512 bytes at a UTF-8 boundary.

use bytes::Bytes;
use sprig_common::{ErrorKind, SprigCode, SprigError};

use crate::buf::{Reader, Writer};
use crate::packets::{Caps, ERR_HEADER, Persona};
use crate::proto_error::{ProtoError, ProtoResult};

/// Longest error message sent, in bytes (MySQL's `MYSQL_ERRMSG_SIZE`).
pub const MAX_MESSAGE_BYTES: usize = 512;

/// SQLSTATE for general errors.
pub const SQLSTATE_GENERAL: [u8; 5] = *b"HY000";

/// An ERR packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrPacket {
    /// Error number.
    pub code: u16,
    /// SQLSTATE, five ASCII characters.
    pub sqlstate: [u8; 5],
    /// Message, at most [`MAX_MESSAGE_BYTES`] when built with
    /// [`ErrPacket::new`].
    pub message: String,
}

impl ErrPacket {
    /// An ERR packet; the message is truncated to 512 bytes at a UTF-8
    /// boundary.
    pub fn new(code: u16, sqlstate: [u8; 5], message: impl Into<String>) -> Self {
        let mut message = message.into();
        let n = truncate_message(&message, MAX_MESSAGE_BYTES).len();
        message.truncate(n);
        Self {
            code,
            sqlstate,
            message,
        }
    }

    /// The ERR packet a persona sends for this error (see [`errmap`]).
    #[must_use]
    pub fn from_error(persona: Persona, err: &SprigError) -> Self {
        let (code, sqlstate, message) = errmap(persona, err);
        Self::new(code, sqlstate, message)
    }

    /// A standard error with its default message.
    #[must_use]
    pub fn std(persona: Persona, e: StdErr) -> Self {
        Self::new(e.code(persona), e.sqlstate_for(persona), e.message(persona))
    }

    /// Decodes an ERR packet. The SQLSTATE marker `#` is optional (absent
    /// before the handshake and without `PROTOCOL_41`); without it the
    /// SQLSTATE reads as `HY000`.
    pub fn decode(payload: &[u8]) -> ProtoResult<Self> {
        let mut r = Reader::new(payload);
        if r.u8()? != ERR_HEADER {
            return Err(ProtoError::malformed("ERR packet header"));
        }
        let code = r.u16()?;
        let mut sqlstate = SQLSTATE_GENERAL;
        if r.peek_u8() == Some(b'#') && r.remaining() >= 6 {
            r.skip(1)?;
            sqlstate = r.bytes_n::<5>()?;
        }
        Ok(Self {
            code,
            sqlstate,
            message: message_from_wire(r.rest()),
        })
    }

    /// Encodes for a peer with these capabilities: the SQLSTATE is sent only
    /// with `PROTOCOL_41`. Pass `Caps::empty()` before the handshake
    /// response, when the client's capabilities aren't known.
    #[must_use]
    pub fn encode(&self, caps: Caps) -> Bytes {
        let msg = truncate_message(&self.message, MAX_MESSAGE_BYTES);
        let mut w = Writer::with_capacity(9 + msg.len());
        w.u8(ERR_HEADER).u16(self.code);
        if caps.contains(Caps::PROTOCOL_41) {
            w.u8(b'#').bytes(&self.sqlstate);
        }
        w.bytes(&message_to_wire(msg));
        w.into_bytes()
    }

    /// 1045: access denied.
    #[must_use]
    pub fn access_denied(persona: Persona, user: &str, host: &str, using_password: bool) -> Self {
        let yes_no = if using_password { "YES" } else { "NO" };
        Self::new(
            StdErr::AccessDenied.code(persona),
            StdErr::AccessDenied.sqlstate(),
            format!("Access denied for user '{user}'@'{host}' (using password: {yes_no})"),
        )
    }

    /// 1227: a server-wide privilege is needed.
    #[must_use]
    pub fn specific_access_denied(persona: Persona, privilege: &str) -> Self {
        Self::new(
            StdErr::SpecificAccessDenied.code(persona),
            StdErr::SpecificAccessDenied.sqlstate(),
            format!(
                "Access denied; you need (at least one of) the {privilege} privilege(s) for this operation"
            ),
        )
    }

    /// 1243: unknown statement id, as `COM_STMT_EXECUTE` etc. report it.
    #[must_use]
    pub fn unknown_stmt_handler(persona: Persona, stmt_id: u32, command: &str) -> Self {
        Self::new(
            StdErr::UnknownStmtHandler.code(persona),
            StdErr::UnknownStmtHandler.sqlstate(),
            format!("Unknown prepared statement handler ({stmt_id}) given to {command}"),
        )
    }

    /// 1461: too many prepared statements in the session.
    #[must_use]
    pub fn max_prepared(persona: Persona, limit: u32) -> Self {
        Self::new(
            StdErr::MaxPreparedStmts.code(persona),
            StdErr::MaxPreparedStmts.sqlstate(),
            format!(
                "Can't create more than max_prepared_stmt_count statements (current value: {limit})"
            ),
        )
    }

    /// 1064: syntax error, with the parser's message.
    #[must_use]
    pub fn parse_error(persona: Persona, message: &str) -> Self {
        Self::new(
            StdErr::ParseError.code(persona),
            StdErr::ParseError.sqlstate(),
            message,
        )
    }
}

/// The first of 256 private-use code points (plane 16) that stand for the
/// bytes of an error message that aren't UTF-8. A server writes its messages
/// in the session's `character_set_results`, which may be `koi8r` or
/// `latin1`: the gateway keeps such bytes exactly, so a relayed message
/// reaches the client as the server wrote it.
const RAW_BYTE_BASE: u32 = 0x10_FF00;

/// An error message's bytes as a `String`: UTF-8 as is, every other byte as
/// its private-use stand-in ([`message_to_wire`] reverses it).
#[must_use]
pub fn message_from_wire(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    let mut rest = bytes;
    loop {
        match std::str::from_utf8(rest) {
            Ok(s) => {
                out.push_str(s);
                return out;
            }
            Err(e) => {
                let (good, bad) = rest.split_at(e.valid_up_to());
                out.push_str(std::str::from_utf8(good).unwrap_or_default());
                let n = e.error_len().unwrap_or(bad.len());
                for &b in &bad[..n] {
                    if let Some(c) = char::from_u32(RAW_BYTE_BASE + u32::from(b)) {
                        out.push(c);
                    }
                }
                rest = &bad[n..];
            }
        }
    }
}

/// The bytes to send for an error message: the private-use stand-ins of
/// [`message_from_wire`] become their bytes again.
#[must_use]
pub fn message_to_wire(s: &str) -> std::borrow::Cow<'_, [u8]> {
    let raw = |c: char| (RAW_BYTE_BASE..=RAW_BYTE_BASE + 0xFF).contains(&u32::from(c));
    if !s.chars().any(raw) {
        return std::borrow::Cow::Borrowed(s.as_bytes());
    }
    let mut out = Vec::with_capacity(s.len());
    for c in s.chars() {
        if raw(c) {
            out.push(u8::try_from(u32::from(c) - RAW_BYTE_BASE).unwrap_or(b'?'));
        } else {
            let mut buf = [0u8; 4];
            out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
        }
    }
    std::borrow::Cow::Owned(out)
}

/// The longest prefix of `s` that fits in `max` bytes without splitting a
/// character.
#[must_use]
pub fn truncate_message(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut n = max;
    while !s.is_char_boundary(n) {
        n -= 1;
    }
    &s[..n]
}

/// Standard errors the wire layer raises itself (chapter 05).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum StdErr {
    /// 1040 `ER_CON_COUNT_ERROR`.
    TooManyConnections,
    /// 1043 `ER_HANDSHAKE_ERROR`.
    BadHandshake,
    /// 1045 `ER_ACCESS_DENIED_ERROR`; prefer [`ErrPacket::access_denied`].
    AccessDenied,
    /// 1047 `ER_UNKNOWN_COM_ERROR`.
    UnknownCommand,
    /// 1064 `ER_PARSE_ERROR`; prefer [`ErrPacket::parse_error`].
    ParseError,
    /// 1153 `ER_NET_PACKET_TOO_LARGE`.
    PacketTooLarge,
    /// 1156 `ER_NET_PACKETS_OUT_OF_ORDER`.
    PacketsOutOfOrder,
    /// 1158 `ER_NET_READ_ERROR`.
    NetReadError,
    /// 1227 `ER_SPECIFIC_ACCESS_DENIED_ERROR`; prefer
    /// [`ErrPacket::specific_access_denied`].
    SpecificAccessDenied,
    /// 1243 `ER_UNKNOWN_STMT_HANDLER`; prefer
    /// [`ErrPacket::unknown_stmt_handler`].
    UnknownStmtHandler,
    /// 1251 `ER_NOT_SUPPORTED_AUTH_MODE`.
    AuthModeNotSupported,
    /// 1461 `ER_MAX_PREPARED_STMT_COUNT_REACHED`; prefer
    /// [`ErrPacket::max_prepared`].
    MaxPreparedStmts,
    /// 1835 `ER_MALFORMED_PACKET`.
    MalformedPacket,
    /// 3159 `ER_SECURE_TRANSPORT_REQUIRED`.
    SecureTransportRequired,
    /// 3948 `ER_CLIENT_LOCAL_FILES_DISABLED`.
    LocalFilesDisabled,
    /// 39010 [`SprigCode::LocalInfileRejected`].
    LocalInfileRejected,
    /// 39011 [`SprigCode::LongDataLimit`].
    LongDataLimit,
    /// 39012 [`SprigCode::BackendProtocol`].
    BackendProtocol,
}

impl StdErr {
    /// The error number in this persona. MySQL and MariaDB share every
    /// number but one: MariaDB reports disabled local infile as 4166
    /// `ER_LOAD_INFILE_CAPABILITY_DISABLED` where MySQL 8.4 uses 3948
    /// (measured on MariaDB 11.8 and 12.3; chapter 05 open question 5).
    #[must_use]
    pub fn code(self, persona: Persona) -> u16 {
        if persona == Persona::MariaDb && self == StdErr::LocalFilesDisabled {
            return MARIADB_LOAD_INFILE_DISABLED;
        }
        match self {
            StdErr::TooManyConnections => 1040,
            StdErr::BadHandshake => 1043,
            StdErr::AccessDenied => 1045,
            StdErr::UnknownCommand => 1047,
            StdErr::ParseError => 1064,
            StdErr::PacketTooLarge => 1153,
            StdErr::PacketsOutOfOrder => 1156,
            StdErr::NetReadError => 1158,
            StdErr::SpecificAccessDenied => 1227,
            StdErr::UnknownStmtHandler => 1243,
            StdErr::AuthModeNotSupported => 1251,
            StdErr::MaxPreparedStmts => 1461,
            StdErr::MalformedPacket => 1835,
            StdErr::SecureTransportRequired => 3159,
            StdErr::LocalFilesDisabled => 3948,
            StdErr::LocalInfileRejected => SprigCode::LocalInfileRejected.number(),
            StdErr::LongDataLimit => SprigCode::LongDataLimit.number(),
            StdErr::BackendProtocol => SprigCode::BackendProtocol.number(),
        }
    }

    /// The SQLSTATE in this persona. MariaDB sends 3159 with `08004` (MySQL
    /// 8.4: `HY000`) and its 4166 with `HY000` (MySQL's 3948: `42000`), as
    /// measured on MariaDB 12.3 and MySQL 8.4 (chapter 05 open questions 5
    /// and 8).
    #[must_use]
    pub fn sqlstate_for(self, persona: Persona) -> [u8; 5] {
        match (persona, self) {
            (Persona::MariaDb, StdErr::SecureTransportRequired) => *b"08004",
            (Persona::MariaDb, StdErr::LocalFilesDisabled) => SQLSTATE_GENERAL,
            _ => self.sqlstate(),
        }
    }

    /// The SQLSTATE, as MySQL 8.4 sends it (see [`StdErr::sqlstate_for`]).
    #[must_use]
    pub fn sqlstate(self) -> [u8; 5] {
        match self {
            StdErr::TooManyConnections | StdErr::AuthModeNotSupported => *b"08004",
            StdErr::BadHandshake
            | StdErr::UnknownCommand
            | StdErr::PacketTooLarge
            | StdErr::PacketsOutOfOrder
            | StdErr::NetReadError
            | StdErr::BackendProtocol => *b"08S01",
            StdErr::AccessDenied => *b"28000",
            StdErr::ParseError
            | StdErr::SpecificAccessDenied
            | StdErr::MaxPreparedStmts
            | StdErr::LocalFilesDisabled => *b"42000",
            StdErr::UnknownStmtHandler
            | StdErr::MalformedPacket
            | StdErr::SecureTransportRequired
            | StdErr::LocalInfileRejected
            | StdErr::LongDataLimit => SQLSTATE_GENERAL,
        }
    }

    /// The default message in this persona's wording.
    #[must_use]
    pub fn message(self, persona: Persona) -> &'static str {
        match self {
            StdErr::TooManyConnections => "Too many connections",
            StdErr::BadHandshake => "Bad handshake",
            StdErr::AccessDenied => "Access denied",
            StdErr::UnknownCommand => "Unknown command",
            StdErr::ParseError => "You have an error in your SQL syntax",
            StdErr::PacketTooLarge => "Got a packet bigger than 'max_allowed_packet' bytes",
            StdErr::PacketsOutOfOrder => "Got packets out of order",
            StdErr::NetReadError => "Got an error reading communication packets",
            StdErr::SpecificAccessDenied => {
                "Access denied; you need (at least one of) the SUPER privilege(s) for this operation"
            }
            StdErr::UnknownStmtHandler => "Unknown prepared statement handler",
            StdErr::AuthModeNotSupported => match persona {
                Persona::MySql84 => {
                    "Client does not support authentication protocol requested by server; consider upgrading MySQL client"
                }
                Persona::MariaDb => {
                    "Client does not support authentication protocol requested by server; consider upgrading MariaDB client"
                }
            },
            StdErr::MaxPreparedStmts => "Can't create more than max_prepared_stmt_count statements",
            StdErr::MalformedPacket => "Malformed communication packet",
            StdErr::SecureTransportRequired => {
                "Connections using insecure transport are prohibited while --require_secure_transport=ON."
            }
            StdErr::LocalFilesDisabled => match persona {
                Persona::MySql84 => {
                    "Loading local data is disabled; this must be enabled on both the client and server sides"
                }
                Persona::MariaDb => {
                    "The used command is not allowed because the MariaDB server or client has disabled the local infile capability"
                }
            },
            StdErr::LocalInfileRejected => {
                "LOAD DATA LOCAL INFILE rejected: the server asked for a file the statement did not name"
            }
            StdErr::LongDataLimit => "Long data for a prepared statement exceeds the limit",
            StdErr::BackendProtocol => "The backend sent an unexpected packet",
        }
    }
}

/// The (errno, SQLSTATE, message) a persona sends for `err`.
///
/// SprigDB errors keep their 39xxx number and SQLSTATE; backend and
/// standard errors keep theirs (MariaDB-only codes on 3306 are mapped by the
/// translator in R4). The message is the error's client-safe text, cut to
/// 512 bytes; chapter 07's name rewriting happens before this.
#[must_use]
pub fn errmap(persona: Persona, err: &SprigError) -> (u16, [u8; 5], String) {
    let (code, sqlstate) = match err.kind {
        ErrorKind::Sprig(code) => (code.number(), code.sqlstate()),
        ErrorKind::Backend { errno, sqlstate } => (errno, sqlstate),
        ErrorKind::Standard { errno, sqlstate } => persona_std(persona, errno, sqlstate),
    };
    let message = truncate_message(&err.message, MAX_MESSAGE_BYTES).to_owned();
    (code, sqlstate, message)
}

/// MariaDB's `ER_LOAD_INFILE_CAPABILITY_DISABLED`, its counterpart of
/// MySQL's 3948.
pub const MARIADB_LOAD_INFILE_DISABLED: u16 = 4166;

/// The number a persona uses for a standard MySQL error number: 3948
/// becomes 4166 on the MariaDB persona; every other number is shared.
#[must_use]
pub fn persona_errno(persona: Persona, errno: u16) -> u16 {
    match (persona, errno) {
        (Persona::MariaDb, 3948) => MARIADB_LOAD_INFILE_DISABLED,
        _ => errno,
    }
}

/// The (errno, SQLSTATE) a persona sends for a standard MySQL error.
fn persona_std(persona: Persona, errno: u16, sqlstate: [u8; 5]) -> (u16, [u8; 5]) {
    match (persona, errno) {
        (Persona::MariaDb, 3159) => (3159, *b"08004"),
        (Persona::MariaDb, 3948) => (MARIADB_LOAD_INFILE_DISABLED, SQLSTATE_GENERAL),
        _ => (errno, sqlstate),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn non_utf8_messages_keep_their_bytes() {
        // KOI8-R "МояТаблица" inside a MariaDB 1064 message (mysql2 #433).
        let mut payload = vec![0xFF, 0x28, 0x04, b'#'];
        payload.extend_from_slice(b"42000near '`");
        let koi8r = [0xED, 0xCF, 0xD1, 0xF4, 0xC1, 0xC2, 0xCC, 0xC9, 0xC3, 0xC1];
        payload.extend_from_slice(&koi8r);
        payload.extend_from_slice(b"' at line 1");
        let e = super::ErrPacket::decode(&payload).unwrap();
        assert!(e.message.starts_with("near '`"));
        let again = e.encode(super::Caps::PROTOCOL_41);
        assert_eq!(&again[..], &payload[..]);
        // UTF-8 stays UTF-8.
        let utf8 = "Таблица".as_bytes();
        assert_eq!(super::message_from_wire(utf8), "Таблица");
        assert_eq!(&*super::message_to_wire("Таблица"), utf8);
    }

    use super::*;

    #[test]
    fn err_round_trip_with_and_without_sqlstate() {
        let e = ErrPacket::std(Persona::MySql84, StdErr::PacketsOutOfOrder);
        let b = e.encode(Caps::PROTOCOL_41);
        assert_eq!(&b[..9], b"\xff\x84\x04#08S01");
        assert_eq!(ErrPacket::decode(&b).ok(), Some(e.clone()));
        let b = e.encode(Caps::empty());
        let d = ErrPacket::decode(&b).expect("decode");
        assert_eq!(d.code, 1156);
        assert_eq!(d.sqlstate, *b"HY000");
        assert_eq!(d.message, e.message);
    }

    #[test]
    fn messages_truncate_at_char_boundaries() {
        let long = "é".repeat(400); // 800 bytes
        let e = ErrPacket::new(1064, *b"42000", long);
        assert_eq!(e.message.len(), 512);
        assert!(e.message.chars().all(|c| c == 'é'));
        let odd = format!("x{}", "é".repeat(400));
        assert_eq!(truncate_message(&odd, 512).len(), 511);
    }

    #[test]
    fn errmap_keeps_sprig_and_backend_numbers() {
        let e = SprigError::new(SprigCode::GatewayDraining, "draining");
        assert_eq!(
            errmap(Persona::MariaDb, &e),
            (39000, *b"08S01", "draining".to_owned())
        );
        let e = SprigError::backend(1146, *b"42S02", "Table 'db.t' doesn't exist");
        let p = ErrPacket::from_error(Persona::MySql84, &e);
        assert_eq!((p.code, p.sqlstate), (1146, *b"42S02"));
    }

    #[test]
    fn builders() {
        let p = ErrPacket::access_denied(Persona::MySql84, "acme.alice", "10.0.0.1", true);
        assert_eq!(p.code, 1045);
        assert!(p.message.ends_with("(using password: YES)"));
        assert_eq!(
            ErrPacket::unknown_stmt_handler(Persona::MariaDb, 7, "mysqld_stmt_execute").code,
            1243
        );
        assert!(
            StdErr::AuthModeNotSupported
                .message(Persona::MariaDb)
                .contains("MariaDB")
        );
    }

    #[test]
    fn mariadb_persona_uses_mariadb_numbers_and_states() {
        let tls = ErrPacket::std(Persona::MariaDb, StdErr::SecureTransportRequired);
        assert_eq!((tls.code, tls.sqlstate), (3159, *b"08004"));
        let tls = ErrPacket::std(Persona::MySql84, StdErr::SecureTransportRequired);
        assert_eq!((tls.code, tls.sqlstate), (3159, *b"HY000"));
        let infile = ErrPacket::std(Persona::MariaDb, StdErr::LocalFilesDisabled);
        assert_eq!((infile.code, infile.sqlstate), (4166, *b"HY000"));
        let infile = ErrPacket::std(Persona::MySql84, StdErr::LocalFilesDisabled);
        assert_eq!((infile.code, infile.sqlstate), (3948, *b"42000"));
        assert_eq!(persona_errno(Persona::MariaDb, 3948), 4166);
        assert_eq!(persona_errno(Persona::MySql84, 3948), 3948);
    }
}
