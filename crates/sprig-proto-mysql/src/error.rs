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
        Self::new(e.code(persona), e.sqlstate(), e.message(persona))
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
            message: String::from_utf8_lossy(r.rest()).into_owned(),
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
        w.bytes(msg.as_bytes());
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
    /// number listed so far; MariaDB's equivalents of 3159 and 3948 are
    /// chapter 05 open questions 5 and 8.
    #[must_use]
    pub fn code(self, _persona: Persona) -> u16 {
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

    /// The SQLSTATE.
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
            StdErr::LocalFilesDisabled => {
                "Loading local data is disabled; this must be enabled on both the client and server sides"
            }
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
        ErrorKind::Standard { errno, sqlstate } => (persona_errno(persona, errno), sqlstate),
    };
    let message = truncate_message(&err.message, MAX_MESSAGE_BYTES).to_owned();
    (code, sqlstate, message)
}

/// The number a persona uses for a standard MySQL error number. The
/// identity until chapter 05's open questions 5 and 8 settle MariaDB's
/// numbers for 3159 and 3948.
#[must_use]
pub fn persona_errno(_persona: Persona, errno: u16) -> u16 {
    errno
}

#[cfg(test)]
mod tests {
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
}
