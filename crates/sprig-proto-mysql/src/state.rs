//! Per-connection protocol state: what the handshake negotiated and what later
//! commands change.

use crate::codec::Framing;
use crate::packets::{Caps, Persona};

/// The TLS parameters of a client connection, as the protocol layer reports
/// them (this layer only reports `ProtoState.tls`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsInfo {
    /// `1.2` or `1.3`.
    pub version: &'static str,
    /// The IANA cipher suite name.
    pub cipher: &'static str,
    /// The server name the client asked for (SNI), if any.
    pub sni: Option<String>,
}

/// One client connection's negotiated protocol state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtoState {
    /// The listener's persona.
    pub persona: Persona,
    /// Effective capabilities: the server's and the client's intersection.
    pub caps: Caps,
    /// The client's collation id from the handshake response.
    pub collation: u16,
    /// `CLIENT_MULTI_STATEMENTS`, then toggled by `COM_SET_OPTION`.
    pub multi_statements: bool,
    /// Framing in force: plain until the auth OK, then compressed when both
    /// sides agreed.
    pub framing: Framing,
    /// The largest logical packet accepted from the client:
    /// `gateway.max_allowed_packet_bytes` (the client's own `max_packet`
    /// is what it reads, not a limit on what it sends).
    pub max_packet: usize,
    /// TLS parameters; `None` for plaintext.
    pub tls: Option<TlsInfo>,
    /// The nonce of the current authentication exchange (fresh after an
    /// `AuthSwitchRequest` or `COM_CHANGE_USER`).
    pub nonce: [u8; 20],
}

impl ProtoState {
    /// Whether the connection is encrypted.
    #[must_use]
    pub fn is_tls(&self) -> bool {
        self.tls.is_some()
    }

    /// Whether the client and server agreed on `cap`.
    #[must_use]
    pub fn has(&self, cap: Caps) -> bool {
        self.caps.contains(cap)
    }
}
