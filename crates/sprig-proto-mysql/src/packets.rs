//! Basic packets (chapter 05, "Other packets", "Capability flags" and the
//! handshake tables): flags, personas, OK, ERR, EOF and the connection-phase
//! packets. Each has `decode` and `encode`, since the gateway is both a
//! server and a client.

pub(crate) mod handshake;
mod ok;

pub use handshake::{
    AuthMoreData, AuthSwitchRequest, HandshakeResponse, HandshakeV10, SSL_REQUEST_LEN, SslRequest,
    is_ssl_request,
};
pub use ok::{EofPacket, OkPacket, SessionChange, session_track};

pub use crate::error::ErrPacket;

/// Header byte of an OK packet.
pub const OK_HEADER: u8 = 0x00;
/// Header byte of an EOF packet, an OK terminator under `DEPRECATE_EOF`, and
/// an `AuthSwitchRequest`.
pub const EOF_HEADER: u8 = 0xFE;
/// Header byte of an ERR packet.
pub const ERR_HEADER: u8 = 0xFF;
/// Header byte of a `LOCAL INFILE` request.
pub const LOCAL_INFILE_HEADER: u8 = 0xFB;
/// Header byte of `AuthMoreData`.
pub const AUTH_MORE_DATA_HEADER: u8 = 0x01;

bitflags::bitflags! {
    /// Capability flags. Bits 0-31 are MySQL's; bits 32-63 are MariaDB's
    /// extended capabilities, sent in the greeting's and response's
    /// reserved bytes when bit 0 (`CLIENT_MYSQL`) is clear.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct Caps: u64 {
        /// `CLIENT_LONG_PASSWORD`; MariaDB calls it `CLIENT_MYSQL` and clears
        /// it to announce extended capabilities.
        const LONG_PASSWORD = 1;
        /// Affected rows count found rows, not changed rows.
        const FOUND_ROWS = 1 << 1;
        /// Column flags are sent in full.
        const LONG_FLAG = 1 << 2;
        /// A default database is named at login.
        const CONNECT_WITH_DB = 1 << 3;
        /// `db.table.column` is not allowed.
        const NO_SCHEMA = 1 << 4;
        /// zlib compression.
        const COMPRESS = 1 << 5;
        /// ODBC client; ignored.
        const ODBC = 1 << 6;
        /// `LOAD DATA LOCAL INFILE`.
        const LOCAL_FILES = 1 << 7;
        /// Spaces are allowed before `(`.
        const IGNORE_SPACE = 1 << 8;
        /// The 4.1 protocol.
        const PROTOCOL_41 = 1 << 9;
        /// Interactive client: `interactive_timeout`.
        const INTERACTIVE = 1 << 10;
        /// TLS; the client sends an `SSLRequest`.
        const SSL = 1 << 11;
        /// Unused by the protocol.
        const IGNORE_SIGPIPE = 1 << 12;
        /// Status flags carry transaction state.
        const TRANSACTIONS = 1 << 13;
        /// Old 4.1 flag.
        const RESERVED = 1 << 14;
        /// 4.1 authentication.
        const SECURE_CONNECTION = 1 << 15;
        /// Several statements per `COM_QUERY`.
        const MULTI_STATEMENTS = 1 << 16;
        /// Several resultsets per `COM_QUERY`.
        const MULTI_RESULTS = 1 << 17;
        /// Several resultsets per `COM_STMT_EXECUTE`.
        const PS_MULTI_RESULTS = 1 << 18;
        /// Authentication plugins.
        const PLUGIN_AUTH = 1 << 19;
        /// Connection attributes.
        const CONNECT_ATTRS = 1 << 20;
        /// A length-encoded auth response.
        const PLUGIN_AUTH_LENENC_CLIENT_DATA = 1 << 21;
        /// Sandbox mode for expired passwords.
        const CAN_HANDLE_EXPIRED_PASSWORDS = 1 << 22;
        /// Session-state tracking in OK packets.
        const SESSION_TRACK = 1 << 23;
        /// OK instead of EOF packets.
        const DEPRECATE_EOF = 1 << 24;
        /// Resultset metadata may be omitted (R4).
        const OPTIONAL_RESULTSET_METADATA = 1 << 25;
        /// zstd compression.
        const ZSTD_COMPRESSION_ALGORITHM = 1 << 26;
        /// Query attributes.
        const QUERY_ATTRIBUTES = 1 << 27;
        /// Multi-factor authentication.
        const MULTI_FACTOR_AUTHENTICATION = 1 << 28;
        /// Reserved for a capability extension.
        const CAPABILITY_EXTENSION = 1 << 29;
        /// Client verifies the server certificate.
        const SSL_VERIFY_SERVER_CERT = 1 << 30;
        /// Client-only: remember options after a failed connect.
        const REMEMBER_OPTIONS = 1 << 31;
        /// MariaDB: progress reports. Never advertised.
        const MARIADB_CLIENT_PROGRESS = 1 << 32;
        /// MariaDB: `COM_MULTI` (removed upstream).
        const MARIADB_CLIENT_COM_MULTI = 1 << 33;
        /// MariaDB: `COM_STMT_BULK_EXECUTE`.
        const MARIADB_CLIENT_STMT_BULK_OPERATIONS = 1 << 34;
        /// MariaDB: extended column metadata (type names).
        const MARIADB_CLIENT_EXTENDED_METADATA = 1 << 35;
        /// MariaDB: cached resultset metadata (R4).
        const MARIADB_CLIENT_CACHE_METADATA = 1 << 36;
        /// MariaDB: per-row results from bulk execution.
        const MARIADB_CLIENT_BULK_UNIT_RESULTS = 1 << 37;
    }
}

impl Caps {
    /// MariaDB's name for bit 0.
    pub const CLIENT_MYSQL: Caps = Caps::LONG_PASSWORD;

    /// Capabilities every client must have (1251 otherwise).
    pub const REQUIRED: Caps = Caps::PROTOCOL_41
        .union(Caps::SECURE_CONNECTION)
        .union(Caps::PLUGIN_AUTH);

    /// The low 32 bits.
    #[must_use]
    pub fn low(self) -> u32 {
        (self.bits() & 0xFFFF_FFFF) as u32
    }

    /// The MariaDB extended bits, shifted down to 0-31.
    #[must_use]
    pub fn extended(self) -> u32 {
        (self.bits() >> 32) as u32
    }

    /// Capabilities from the low and extended 32-bit halves, keeping
    /// unknown bits.
    #[must_use]
    pub fn from_parts(low: u32, extended: u32) -> Self {
        Self::from_bits_retain(u64::from(low) | u64::from(extended) << 32)
    }

    /// What a gateway listener of this persona advertises (chapter 05's
    /// capability table), without `SSL`, which depends on the listener's
    /// TLS mode.
    #[must_use]
    pub fn server_default(persona: Persona) -> Self {
        let common = Caps::FOUND_ROWS
            | Caps::LONG_FLAG
            | Caps::CONNECT_WITH_DB
            | Caps::COMPRESS
            | Caps::ODBC
            | Caps::LOCAL_FILES
            | Caps::IGNORE_SPACE
            | Caps::PROTOCOL_41
            | Caps::INTERACTIVE
            | Caps::TRANSACTIONS
            | Caps::SECURE_CONNECTION
            | Caps::MULTI_STATEMENTS
            | Caps::MULTI_RESULTS
            | Caps::PS_MULTI_RESULTS
            | Caps::PLUGIN_AUTH
            | Caps::CONNECT_ATTRS
            | Caps::PLUGIN_AUTH_LENENC_CLIENT_DATA
            | Caps::CAN_HANDLE_EXPIRED_PASSWORDS
            | Caps::SESSION_TRACK
            | Caps::DEPRECATE_EOF;
        match persona {
            Persona::MySql84 => {
                common
                    | Caps::LONG_PASSWORD
                    | Caps::ZSTD_COMPRESSION_ALGORITHM
                    | Caps::QUERY_ATTRIBUTES
            }
            Persona::MariaDb => {
                common
                    | Caps::MARIADB_CLIENT_STMT_BULK_OPERATIONS
                    | Caps::MARIADB_CLIENT_EXTENDED_METADATA
            }
        }
    }
}

bitflags::bitflags! {
    /// Server status flags, in OK and EOF packets.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct StatusFlags: u16 {
        /// A transaction is open.
        const IN_TRANS = 1;
        /// Autocommit is on.
        const AUTOCOMMIT = 1 << 1;
        /// Another resultset follows.
        const MORE_RESULTS_EXISTS = 1 << 3;
        /// The query used a bad index.
        const NO_GOOD_INDEX_USED = 1 << 4;
        /// The query used no index.
        const NO_INDEX_USED = 1 << 5;
        /// A cursor is open (`COM_STMT_EXECUTE` with a cursor).
        const CURSOR_EXISTS = 1 << 6;
        /// The cursor's last row was sent.
        const LAST_ROW_SENT = 1 << 7;
        /// A database was dropped.
        const DB_DROPPED = 1 << 8;
        /// `NO_BACKSLASH_ESCAPES` is in `sql_mode`.
        const NO_BACKSLASH_ESCAPES = 1 << 9;
        /// Prepared statement metadata changed.
        const METADATA_CHANGED = 1 << 10;
        /// The query was slow.
        const QUERY_WAS_SLOW = 1 << 11;
        /// The resultset holds OUT parameters.
        const PS_OUT_PARAMS = 1 << 12;
        /// A read-only transaction is open.
        const IN_TRANS_READONLY = 1 << 13;
        /// Session state changed; the OK packet carries the changes.
        const SESSION_STATE_CHANGED = 1 << 14;
        /// MariaDB: `ANSI_QUOTES` is in `sql_mode`.
        const ANSI_QUOTES = 1 << 15;
    }
}

/// The server a gateway listener impersonates (chapter 05).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Persona {
    /// MySQL 8.4, port 3306.
    MySql84,
    /// MariaDB, port 3307.
    MariaDb,
}

impl Persona {
    /// The default greeting version string.
    #[must_use]
    pub fn default_version(self) -> &'static str {
        match self {
            Persona::MySql84 => "8.4.8-SprigDB",
            Persona::MariaDb => "11.8.3-MariaDB-SprigDB",
        }
    }

    /// The collation id in the greeting: `utf8mb4_0900_ai_ci` (255) or
    /// `utf8mb4_general_ci` (45).
    #[must_use]
    pub fn greeting_collation(self) -> u8 {
        match self {
            Persona::MySql84 => 255,
            Persona::MariaDb => 45,
        }
    }

    /// Label for metrics.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Persona::MySql84 => "mysql",
            Persona::MariaDb => "mariadb",
        }
    }
}

#[cfg(test)]
mod tests;
