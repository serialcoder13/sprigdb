//! Connection-phase packets: the `HandshakeV10` greeting,
//! `HandshakeResponse41`, `SSLRequest`, `AuthSwitchRequest` and `AuthMoreData`.

use bytes::Bytes;

use super::{AUTH_MORE_DATA_HEADER, Caps, EOF_HEADER, StatusFlags};
use crate::buf::{Reader, Writer};
use crate::proto_error::{ProtoError, ProtoResult};

/// Length of an `SSLRequest`, the first 32 bytes of a handshake response.
pub const SSL_REQUEST_LEN: usize = 32;

/// The server greeting, protocol version 10.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeV10 {
    /// Version string, such as `8.4.8-SprigDB`.
    pub server_version: String,
    /// Connection id.
    pub connection_id: u32,
    /// The auth nonce, normally 20 bytes, without the trailing NUL.
    pub auth_data: Bytes,
    /// Server capabilities, MariaDB extended bits included.
    pub caps: Caps,
    /// Default collation id (low byte).
    pub collation: u8,
    /// Status flags.
    pub status: StatusFlags,
    /// Auth plugin name; empty without `PLUGIN_AUTH`.
    pub auth_plugin: String,
}

impl HandshakeV10 {
    /// Encodes the greeting. The extended capabilities go in the reserved
    /// bytes only when `CLIENT_MYSQL` (bit 0) is clear, as MariaDB does.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut w = Writer::with_capacity(96);
        let low = self.caps.low();
        let nonce = &self.auth_data[..];
        let (part1, part2) = nonce.split_at(nonce.len().min(8));
        w.u8(10)
            .nul_bytes(self.server_version.as_bytes())
            .u32(self.connection_id)
            .bytes(part1)
            .zeros(8 - part1.len())
            .u8(0)
            .u16((low & 0xFFFF) as u16)
            .u8(self.collation)
            .u16(self.status.bits())
            .u16((low >> 16) as u16);
        let plugin_auth = self.caps.contains(Caps::PLUGIN_AUTH);
        let auth_len = if plugin_auth {
            u8::try_from(nonce.len().max(20) + 1).unwrap_or(u8::MAX)
        } else {
            0
        };
        w.u8(auth_len).zeros(6);
        if self.caps.contains(Caps::CLIENT_MYSQL) {
            w.u32(0);
        } else {
            w.u32(self.caps.extended());
        }
        if self.caps.contains(Caps::SECURE_CONNECTION) {
            w.bytes(part2)
                .zeros(12usize.saturating_sub(part2.len()))
                .u8(0);
        }
        if plugin_auth {
            w.nul_bytes(self.auth_plugin.as_bytes());
        }
        w.into_bytes()
    }

    /// Decodes a greeting. A payload starting with 0xFF is an ERR packet,
    /// which the caller checks first.
    pub fn decode(payload: &Bytes) -> ProtoResult<Self> {
        let mut r = Reader::new(payload);
        let proto = r.u8()?;
        if proto != 10 {
            return Err(ProtoError::malformed(format!(
                "handshake protocol version {proto}"
            )));
        }
        let server_version = String::from_utf8_lossy(r.nul_bytes()?).into_owned();
        let connection_id = r.u32()?;
        let mut nonce = r.take(8)?.to_vec();
        r.skip(1)?;
        let caps_lo = u32::from(r.u16()?);
        let mut hs = Self {
            server_version,
            connection_id,
            auth_data: Bytes::new(),
            caps: Caps::from_parts(caps_lo, 0),
            collation: 0,
            status: StatusFlags::empty(),
            auth_plugin: String::new(),
        };
        if r.is_empty() {
            hs.auth_data = nonce.into();
            return Ok(hs);
        }
        hs.collation = r.u8()?;
        hs.status = StatusFlags::from_bits_retain(r.u16()?);
        let low = caps_lo | u32::from(r.u16()?) << 16;
        let auth_len = usize::from(r.u8()?);
        r.skip(6)?;
        let ext = r.u32()?;
        hs.caps = Caps::from_parts(low, if low & 1 == 0 { ext } else { 0 });
        if hs.caps.contains(Caps::SECURE_CONNECTION) {
            let n = auth_len.saturating_sub(8).max(13).min(r.remaining());
            let mut part2 = r.take(n)?;
            if let Some((0, head)) = part2.split_last() {
                part2 = head;
            }
            nonce.extend_from_slice(part2);
        }
        if hs.caps.contains(Caps::PLUGIN_AUTH) {
            hs.auth_plugin = String::from_utf8_lossy(r.nul_bytes_or_rest()).into_owned();
        }
        hs.auth_data = nonce.into();
        Ok(hs)
    }
}

/// `HandshakeResponse41`, the client's answer to the greeting.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HandshakeResponse {
    /// Client capabilities, with MariaDB's extended bits merged in from the
    /// filler when bit 0 is clear.
    pub caps: Caps,
    /// The client's largest packet.
    pub max_packet: u32,
    /// Collation id.
    pub collation: u8,
    /// User name.
    pub user: Vec<u8>,
    /// The auth plugin's first response (the scramble).
    pub auth_response: Bytes,
    /// Default database (`CONNECT_WITH_DB`); an empty name reads as `None`.
    pub database: Option<Vec<u8>>,
    /// Client auth plugin (`PLUGIN_AUTH`).
    pub plugin: Option<String>,
    /// Connection attributes (`CONNECT_ATTRS`), in order.
    pub attrs: Vec<(String, String)>,
    /// zstd level (`ZSTD_COMPRESSION_ALGORITHM`).
    pub zstd_level: Option<u8>,
}

/// Reads the 32-byte fixed part shared by the response and the `SSLRequest`.
fn decode_fixed(r: &mut Reader<'_>) -> ProtoResult<(Caps, u32, u8)> {
    let low = r.u32()?;
    let max_packet = r.u32()?;
    let collation = r.u8()?;
    let filler = r.bytes_n::<23>()?;
    let ext = u32::from_le_bytes([filler[19], filler[20], filler[21], filler[22]]);
    let caps = Caps::from_parts(low, if low & 1 == 0 { ext } else { 0 });
    Ok((caps, max_packet, collation))
}

fn encode_fixed(w: &mut Writer, caps: Caps, max_packet: u32, collation: u8) {
    w.u32(caps.low()).u32(max_packet).u8(collation).zeros(19);
    if caps.contains(Caps::CLIENT_MYSQL) {
        w.u32(0);
    } else {
        w.u32(caps.extended());
    }
}

/// Reads a length-encoded block of length-encoded key and value pairs.
pub(crate) fn decode_attrs(r: &mut Reader<'_>) -> ProtoResult<Vec<(String, String)>> {
    let block = r.lenenc_bytes()?;
    let mut b = Reader::new(block);
    let mut attrs = Vec::new();
    while !b.is_empty() {
        let k = String::from_utf8_lossy(b.lenenc_bytes()?).into_owned();
        let v = String::from_utf8_lossy(b.lenenc_bytes()?).into_owned();
        attrs.push((k, v));
    }
    Ok(attrs)
}

pub(crate) fn encode_attrs(w: &mut Writer, attrs: &[(String, String)]) {
    let mut b = Writer::new();
    for (k, v) in attrs {
        b.lenenc_bytes(k.as_bytes()).lenenc_bytes(v.as_bytes());
    }
    w.lenenc_bytes(&b.into_bytes());
}

impl HandshakeResponse {
    /// Decodes a `HandshakeResponse41`. Fields are read according to the
    /// client's own capability bits; the caller intersects them with the
    /// server's afterwards. Pre-4.1 responses are refused.
    pub fn decode(payload: &Bytes) -> ProtoResult<Self> {
        let mut r = Reader::new(payload);
        let (caps, max_packet, collation) = decode_fixed(&mut r)?;
        if !caps.contains(Caps::PROTOCOL_41) {
            return Err(ProtoError::malformed("pre-4.1 handshake response"));
        }
        let user = r.nul_bytes()?.to_vec();
        let auth = if caps.contains(Caps::PLUGIN_AUTH_LENENC_CLIENT_DATA) {
            r.lenenc_bytes()?
        } else if caps.contains(Caps::SECURE_CONNECTION) {
            let n = usize::from(r.u8()?);
            r.take(n)?
        } else {
            r.nul_bytes()?
        };
        let mut resp = Self {
            caps,
            max_packet,
            collation,
            user,
            auth_response: payload.slice_ref(auth),
            ..Self::default()
        };
        if caps.contains(Caps::CONNECT_WITH_DB) && !r.is_empty() {
            let db = r.nul_bytes_or_rest();
            resp.database = (!db.is_empty()).then(|| db.to_vec());
        }
        if caps.contains(Caps::PLUGIN_AUTH) && !r.is_empty() {
            resp.plugin = Some(String::from_utf8_lossy(r.nul_bytes_or_rest()).into_owned());
        }
        if caps.contains(Caps::CONNECT_ATTRS) && !r.is_empty() {
            resp.attrs = decode_attrs(&mut r)?;
        }
        if caps.contains(Caps::ZSTD_COMPRESSION_ALGORITHM) && !r.is_empty() {
            resp.zstd_level = Some(r.u8()?);
        }
        Ok(resp)
    }

    /// Encodes the response according to `self.caps`.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut w = Writer::with_capacity(128 + self.auth_response.len());
        encode_fixed(&mut w, self.caps, self.max_packet, self.collation);
        w.nul_bytes(&self.user);
        if self.caps.contains(Caps::PLUGIN_AUTH_LENENC_CLIENT_DATA) {
            w.lenenc_bytes(&self.auth_response);
        } else if self.caps.contains(Caps::SECURE_CONNECTION) {
            let n = self.auth_response.len().min(255);
            w.u8(u8::try_from(n).unwrap_or(u8::MAX))
                .bytes(&self.auth_response[..n]);
        } else {
            w.nul_bytes(&self.auth_response);
        }
        if self.caps.contains(Caps::CONNECT_WITH_DB) {
            w.nul_bytes(self.database.as_deref().unwrap_or_default());
        }
        if self.caps.contains(Caps::PLUGIN_AUTH) {
            w.nul_bytes(self.plugin.as_deref().unwrap_or_default().as_bytes());
        }
        if self.caps.contains(Caps::CONNECT_ATTRS) {
            encode_attrs(&mut w, &self.attrs);
        }
        if self.caps.contains(Caps::ZSTD_COMPRESSION_ALGORITHM) {
            w.u8(self.zstd_level.unwrap_or(3));
        }
        w.into_bytes()
    }
}

/// Whether a connection-phase payload is an `SSLRequest`: exactly 32 bytes
/// with the `SSL` bit set.
#[must_use]
pub fn is_ssl_request(payload: &[u8]) -> bool {
    payload.len() == SSL_REQUEST_LEN && payload[1] & 0x08 != 0
}

/// The `SSLRequest`: the fixed 32-byte head of a handshake response, sent
/// before the TLS handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SslRequest {
    /// Client capabilities, `SSL` included.
    pub caps: Caps,
    /// The client's largest packet.
    pub max_packet: u32,
    /// Collation id.
    pub collation: u8,
}

impl SslRequest {
    /// Decodes an `SSLRequest` (or the head of any handshake response).
    pub fn decode(payload: &[u8]) -> ProtoResult<Self> {
        let mut r = Reader::new(payload);
        let (caps, max_packet, collation) = decode_fixed(&mut r)?;
        Ok(Self {
            caps,
            max_packet,
            collation,
        })
    }

    /// Encodes the 32 bytes.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut w = Writer::with_capacity(SSL_REQUEST_LEN);
        encode_fixed(
            &mut w,
            self.caps | Caps::SSL,
            self.max_packet,
            self.collation,
        );
        w.into_bytes()
    }
}

/// `AuthSwitchRequest`: 0xFE, the plugin name, then its data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthSwitchRequest {
    /// Plugin to switch to.
    pub plugin: String,
    /// Plugin data. MySQL servers send the 20-byte nonce followed by a NUL;
    /// include the NUL here when sending.
    pub data: Bytes,
}

impl AuthSwitchRequest {
    /// Decodes the packet. A bare 0xFE (the pre-4.1 switch) decodes with an
    /// empty plugin name.
    pub fn decode(payload: &Bytes) -> ProtoResult<Self> {
        let mut r = Reader::new(payload);
        if r.u8()? != EOF_HEADER {
            return Err(ProtoError::malformed("AuthSwitchRequest header"));
        }
        if r.is_empty() {
            return Ok(Self {
                plugin: String::new(),
                data: Bytes::new(),
            });
        }
        let plugin = String::from_utf8_lossy(r.nul_bytes()?).into_owned();
        Ok(Self {
            plugin,
            data: payload.slice_ref(r.rest()),
        })
    }

    /// Encodes the packet.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut w = Writer::with_capacity(2 + self.plugin.len() + self.data.len());
        w.u8(EOF_HEADER)
            .nul_bytes(self.plugin.as_bytes())
            .bytes(&self.data);
        w.into_bytes()
    }
}

/// `AuthMoreData`: 0x01, then plugin data (such as 0x03 fast-auth success,
/// 0x04 perform full authentication, or a PEM public key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthMoreData {
    /// The data after the header byte.
    pub data: Bytes,
}

impl AuthMoreData {
    /// Decodes the packet.
    pub fn decode(payload: &Bytes) -> ProtoResult<Self> {
        let mut r = Reader::new(payload);
        if r.u8()? != AUTH_MORE_DATA_HEADER {
            return Err(ProtoError::malformed("AuthMoreData header"));
        }
        Ok(Self {
            data: payload.slice_ref(r.rest()),
        })
    }

    /// Encodes the packet.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut w = Writer::with_capacity(1 + self.data.len());
        w.u8(AUTH_MORE_DATA_HEADER).bytes(&self.data);
        w.into_bytes()
    }
}
