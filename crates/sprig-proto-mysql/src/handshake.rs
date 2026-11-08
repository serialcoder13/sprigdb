//! The server side of the connection phase (chapter 05, "Greeting and
//! version strings", "Capability negotiation", "TLS upgrade and connection
//! attributes"; build step 3).
//!
//! The pieces, in the order a front end uses them:
//!
//! 1. [`new_nonce`] and [`greeting`]: the `HandshakeV10` for a persona, with
//!    `SSL` advertised only when the listener offers TLS
//!    ([`server_caps`]).
//! 2. [`read_packet`] / [`write_packet`] (or [`RawIo`]): connection-phase
//!    packets read with exact reads, so nothing past the `SSLRequest` is
//!    buffered and the caller can hand the bare stream to its TLS acceptor.
//! 3. [`parse_client_hello`] and [`check_transport`]: an `SSLRequest` or a
//!    `HandshakeResponse41`, and the listener's TLS rule (1043 for an
//!    `SSLRequest` on an `off` listener, 3159 for plaintext on a `required`
//!    one), decided from the capability bits alone, before the user name is
//!    read.
//! 4. [`negotiate`]: the effective capabilities (1251 without 4.1 auth),
//!    `max_packet`, multi-statements and the compressed framing that starts
//!    after the auth OK packet.
//!
//! The TLS handshake itself belongs to the caller: the gateway's listener
//! owns the certificate resolver (chapter 04).
//!
//! [`PacketIo`] is the packet transport the auth plugins ([`crate::auth`])
//! use: [`RawIo`] during login, and a `Framed` [`PacketCodec`] for
//! `COM_CHANGE_USER` in the command phase.

use std::io;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{SinkExt as _, StreamExt as _};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio_util::codec::Framed;

use crate::codec::{DEFAULT_ZLIB_LEVEL, DEFAULT_ZSTD_LEVEL, Framing, MAX_CHUNK, PacketCodec};
use crate::error::StdErr;
use crate::packets::{
    Caps, HandshakeResponse, HandshakeV10, Persona, SslRequest, StatusFlags, is_ssl_request,
};
use crate::proto_error::{ProtoError, ProtoResult};

/// Length of the auth nonce both MySQL plugins use.
pub const NONCE_LEN: usize = 20;

/// The plugin the greeting offers on both personas.
pub const GREETING_PLUGIN: &str = "caching_sha2_password";

/// The largest connection-phase packet read before the client is known:
/// room for `gateway.max_connect_attrs_bytes` at its 64 KiB default, a
/// long user name, database and plugin, with margin. A larger response is
/// refused (1153) rather than buffered.
pub const MAX_CONNECTION_PACKET: usize = 1 << 20;

/// A fresh 20-byte nonce from the OS CSPRNG, each byte in 0x01-0x7F and
/// none equal to `$`, as MySQL generates it (chapter 05: no zero bytes,
/// since part 2 of the greeting's nonce is NUL-terminated).
///
/// # Errors
/// [`ProtoError::Io`] if the OS generator fails.
pub fn new_nonce() -> ProtoResult<[u8; NONCE_LEN]> {
    let mut out = [0u8; NONCE_LEN];
    let mut filled = 0;
    let mut pool = [0u8; 64];
    while filled < NONCE_LEN {
        getrandom::fill(&mut pool).map_err(|e| ProtoError::Io(io::Error::other(e.to_string())))?;
        for b in pool {
            // Rejection sampling keeps the bytes uniform over 0x01..=0x7F
            // without '$'.
            let b = b & 0x7F;
            if b != 0 && b != b'$' && filled < NONCE_LEN {
                out[filled] = b;
                filled += 1;
            }
        }
    }
    Ok(out)
}

/// The compression algorithms a listener offers (`gateway.compression`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionOffer {
    /// zlib (`CLIENT_COMPRESS`).
    pub zlib: bool,
    /// zstd (`CLIENT_ZSTD_COMPRESSION_ALGORITHM`; MySQL persona only).
    pub zstd: bool,
}

impl CompressionOffer {
    /// Both algorithms: `gateway.compression`'s default.
    pub const ALL: CompressionOffer = CompressionOffer {
        zlib: true,
        zstd: true,
    };

    /// Parses `gateway.compression`: a comma-separated list of `zlib` and
    /// `zstd` (unknown names are ignored; an empty list offers none).
    #[must_use]
    pub fn parse(list: &str) -> Self {
        let mut o = CompressionOffer {
            zlib: false,
            zstd: false,
        };
        for name in list.split(',').map(str::trim) {
            match name.to_ascii_lowercase().as_str() {
                "zlib" => o.zlib = true,
                "zstd" => o.zstd = true,
                _ => {}
            }
        }
        o
    }
}

/// The capabilities a listener of `persona` advertises (chapter 05's
/// capability table): `SSL` only when TLS is offered, and the compression
/// bits `offer` allows.
#[must_use]
pub fn server_caps(persona: Persona, tls_offered: bool, offer: CompressionOffer) -> Caps {
    let mut caps = Caps::server_default(persona);
    caps.set(Caps::SSL, tls_offered);
    if !offer.zlib {
        caps.remove(Caps::COMPRESS);
    }
    if !offer.zstd {
        caps.remove(Caps::ZSTD_COMPRESSION_ALGORITHM);
    }
    caps
}

/// The greeting: protocol 10, the persona's version string, the connection
/// id, the nonce, `caps`, the persona's collation, `AUTOCOMMIT`, and
/// `caching_sha2_password`.
#[must_use]
pub fn greeting(
    persona: Persona,
    server_version: &str,
    connection_id: u32,
    nonce: &[u8; NONCE_LEN],
    caps: Caps,
) -> HandshakeV10 {
    HandshakeV10 {
        server_version: server_version.to_owned(),
        connection_id,
        auth_data: Bytes::copy_from_slice(nonce),
        caps,
        collation: persona.greeting_collation(),
        status: StatusFlags::AUTOCOMMIT,
        auth_plugin: GREETING_PLUGIN.to_owned(),
    }
}

/// Reads one logical packet with exact reads (no read-ahead), checking its
/// sequence id against `*seq` and advancing it. A payload above `max`
/// fails with [`ProtoError::PacketTooLarge`] before it is read.
pub async fn read_packet<S>(stream: &mut S, seq: &mut u8, max: usize) -> ProtoResult<Bytes>
where
    S: AsyncRead + Unpin + ?Sized,
{
    let mut payload = Vec::new();
    loop {
        let mut header = [0u8; 4];
        stream.read_exact(&mut header).await?;
        let len =
            usize::from(header[0]) | usize::from(header[1]) << 8 | usize::from(header[2]) << 16;
        if header[3] != *seq {
            return Err(ProtoError::OutOfOrder {
                expected: *seq,
                got: header[3],
            });
        }
        *seq = seq.wrapping_add(1);
        if payload.len() + len > max {
            return Err(ProtoError::PacketTooLarge {
                size: payload.len() + len,
                max,
            });
        }
        let start = payload.len();
        payload.resize(start + len, 0);
        stream.read_exact(&mut payload[start..]).await?;
        if len < MAX_CHUNK {
            return Ok(payload.into());
        }
    }
}

/// Writes `payload` as one logical packet starting at sequence `*seq`
/// (split into 16 MiB chunks as needed), advances `*seq`, and flushes.
pub async fn write_packet<S>(stream: &mut S, seq: &mut u8, payload: &[u8]) -> ProtoResult<()>
where
    S: AsyncWrite + Unpin + ?Sized,
{
    let mut rest = payload;
    loop {
        let n = rest.len().min(MAX_CHUNK);
        let len = u32::try_from(n).unwrap_or(0).to_le_bytes();
        let mut frame = Vec::with_capacity(4 + n);
        frame.extend_from_slice(&[len[0], len[1], len[2], *seq]);
        frame.extend_from_slice(&rest[..n]);
        stream.write_all(&frame).await?;
        *seq = seq.wrapping_add(1);
        rest = &rest[n..];
        if n < MAX_CHUNK {
            break;
        }
    }
    stream.flush().await?;
    Ok(())
}

/// The packet transport the auth plugins drive. Each side counts its own
/// sequence ids; implementations check and advance them.
#[async_trait]
pub trait PacketIo: Send {
    /// Reads the next packet's payload.
    async fn read_packet(&mut self) -> ProtoResult<Bytes>;
    /// Writes one packet with the next sequence id and flushes it.
    async fn write_packet(&mut self, payload: Bytes) -> ProtoResult<()>;
}

/// [`PacketIo`] over a borrowed stream with exact reads: the connection
/// phase before (and right after) a TLS upgrade.
#[derive(Debug)]
pub struct RawIo<'a, S: ?Sized> {
    /// The stream.
    pub stream: &'a mut S,
    /// The next sequence id.
    pub seq: u8,
    /// The largest packet read.
    pub max: usize,
}

impl<'a, S: ?Sized> RawIo<'a, S> {
    /// A transport on `stream` whose next packet has sequence `seq`.
    pub fn new(stream: &'a mut S, seq: u8) -> Self {
        Self {
            stream,
            seq,
            max: MAX_CONNECTION_PACKET,
        }
    }
}

#[async_trait]
impl<S> PacketIo for RawIo<'_, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    async fn read_packet(&mut self) -> ProtoResult<Bytes> {
        read_packet(self.stream, &mut self.seq, self.max).await
    }

    async fn write_packet(&mut self, payload: Bytes) -> ProtoResult<()> {
        write_packet(self.stream, &mut self.seq, &payload).await
    }
}

#[async_trait]
impl<S> PacketIo for Framed<S, PacketCodec>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    async fn read_packet(&mut self) -> ProtoResult<Bytes> {
        match self.next().await {
            Some(r) => r.map(|p| p.payload),
            None => Err(ProtoError::Io(io::ErrorKind::UnexpectedEof.into())),
        }
    }

    async fn write_packet(&mut self, payload: Bytes) -> ProtoResult<()> {
        self.send(payload).await
    }
}

/// The client's first answer to the greeting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientHello {
    /// An `SSLRequest`: the TLS handshake follows, then the full response
    /// with sequence id 2.
    SslRequest(SslRequest),
    /// A `HandshakeResponse41` (in plaintext, or after the upgrade).
    Response(HandshakeResponse),
}

/// Why the connection phase refuses a client; the caller sends the ERR
/// packet ([`Refusal::err`]) and closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// 1043: an `SSLRequest` on an `off` listener, or a response that
    /// doesn't parse.
    BadHandshake,
    /// 3159: a plaintext response on a `required` listener.
    SecureTransportRequired,
    /// 1251: the client lacks `PROTOCOL_41`, `SECURE_CONNECTION` or
    /// `PLUGIN_AUTH`.
    AuthModeNotSupported,
}

impl Refusal {
    /// The standard error.
    #[must_use]
    pub fn std_err(self) -> StdErr {
        match self {
            Refusal::BadHandshake => StdErr::BadHandshake,
            Refusal::SecureTransportRequired => StdErr::SecureTransportRequired,
            Refusal::AuthModeNotSupported => StdErr::AuthModeNotSupported,
        }
    }

    /// The ERR packet in `persona`'s numbering.
    #[must_use]
    pub fn err(self, persona: Persona) -> crate::error::ErrPacket {
        crate::error::ErrPacket::std(persona, self.std_err())
    }
}

/// Classifies the client's answer to the greeting. A pre-4.1 response is
/// refused with 1251 before anything past the capabilities is read.
pub fn parse_client_hello(payload: &Bytes) -> Result<ClientHello, Refusal> {
    if is_ssl_request(payload) {
        return SslRequest::decode(payload)
            .map(ClientHello::SslRequest)
            .map_err(|_| Refusal::BadHandshake);
    }
    let low = payload
        .get(..4)
        .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
        .ok_or(Refusal::BadHandshake)?;
    if !Caps::from_bits_retain(u64::from(low)).contains(Caps::PROTOCOL_41) {
        return Err(Refusal::AuthModeNotSupported);
    }
    HandshakeResponse::decode(payload)
        .map(ClientHello::Response)
        .map_err(|_| Refusal::BadHandshake)
}

/// A listener's TLS mode as the handshake sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsPolicy {
    /// TLS is not offered: `SSL` isn't advertised and an `SSLRequest` is
    /// refused with 1043.
    Off,
    /// TLS if the client asks.
    Optional,
    /// TLS or nothing: a plaintext response is refused with 3159.
    Required,
}

impl TlsPolicy {
    /// Whether `SSL` is advertised.
    #[must_use]
    pub fn offered(self) -> bool {
        self != TlsPolicy::Off
    }
}

/// The listener's transport rule (chapter 05, "Capability negotiation"),
/// enforced from the capability bits alone, before the user name is read
/// and identically for every tenant. `tls_active` says whether the stream
/// is already TLS (the response that follows an upgrade).
pub fn check_transport(
    policy: TlsPolicy,
    hello: &ClientHello,
    tls_active: bool,
) -> Result<(), Refusal> {
    match hello {
        ClientHello::SslRequest(_) if tls_active || policy == TlsPolicy::Off => {
            Err(Refusal::BadHandshake)
        }
        ClientHello::Response(_) if !tls_active && policy == TlsPolicy::Required => {
            Err(Refusal::SecureTransportRequired)
        }
        ClientHello::SslRequest(_) | ClientHello::Response(_) => Ok(()),
    }
}

/// What [`negotiate`] settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Negotiated {
    /// Effective capabilities. At most one compression bit remains: the
    /// algorithm chosen.
    pub caps: Caps,
    /// The largest packet accepted from the client:
    /// `gateway.max_allowed_packet_bytes`. As on MySQL and MariaDB, the
    /// client's own `max_packet` doesn't lower it: it is what the client
    /// reads, and drivers send small defaults (`mysql_async` 4 MiB) while
    /// uploading more.
    pub max_packet: usize,
    /// The client's collation.
    pub collation: u16,
    /// `CLIENT_MULTI_STATEMENTS` was agreed.
    pub multi_statements: bool,
    /// The framing to switch to after the auth OK packet.
    pub framing: Framing,
}

/// Capability negotiation (chapter 05): intersects the server's and the
/// client's capabilities, refuses clients without 4.1 authentication
/// (1251), and picks the framing. zlib wins when the client set both
/// compression bits, as in MySQL; zstd uses the client's level (3 if it
/// sent none).
pub fn negotiate(
    server: Caps,
    resp: &HandshakeResponse,
    max_allowed: usize,
) -> Result<Negotiated, Refusal> {
    let mut caps = server & resp.caps;
    if !caps.contains(Caps::REQUIRED) {
        return Err(Refusal::AuthModeNotSupported);
    }
    let framing = if caps.contains(Caps::COMPRESS) {
        caps.remove(Caps::ZSTD_COMPRESSION_ALGORITHM);
        Framing::Zlib {
            level: DEFAULT_ZLIB_LEVEL,
        }
    } else if caps.contains(Caps::ZSTD_COMPRESSION_ALGORITHM) {
        Framing::Zstd {
            level: resp
                .zstd_level
                .map_or(DEFAULT_ZSTD_LEVEL, |l| i32::from(l).clamp(1, 22)),
        }
    } else {
        Framing::Plain
    };
    let max_packet = max_allowed;
    Ok(Negotiated {
        caps,
        max_packet,
        collation: u16::from(resp.collation),
        multi_statements: caps.contains(Caps::MULTI_STATEMENTS),
        framing,
    })
}

/// Keeps connection attributes up to `max_bytes` in total (key plus value
/// lengths, in order); returns the kept ones and how many were dropped
/// (chapter 05: "anything beyond is dropped and counted").
#[must_use]
pub fn limit_attrs(
    attrs: Vec<(String, String)>,
    max_bytes: usize,
) -> (Vec<(String, String)>, usize) {
    let mut used = 0usize;
    let mut kept = Vec::with_capacity(attrs.len());
    let mut dropped = 0;
    for (k, v) in attrs {
        let n = k.len() + v.len();
        if used + n <= max_bytes {
            used += n;
            kept.push((k, v));
        } else {
            dropped += 1;
        }
    }
    (kept, dropped)
}

#[cfg(test)]
mod tests;
