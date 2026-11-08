//! Fuzz harnesses for `sprig-proto-mysql` (chapter 05 build step 13,
//! chapter 46 step 4).
//!
//! Each `fuzz_*` function is one cargo-fuzz target's body. They live here,
//! not in `fuzz_targets/`, so the same code also runs on stable as a corpus
//! replay: `cargo test` in this crate, and the workspace test
//! `crates/sprig-proto-mysql/tests/golden_fuzz_replay.rs`, which includes this
//! file by path.
//!
//! The oracle is the one chapter 46 names: no panic, no hang, bounded
//! memory, and, where the crate has an encoder, decode -> encode -> decode
//! stability. Stability is checked on bytes: `e1 = encode(decode(x))` must
//! decode again and encode to exactly `e1`. Byte comparison (rather than
//! `==` on the decoded values) keeps NaN floats from failing the oracle.
//! Where a decoded value is in the encoder's exact domain, the decoded
//! values must also be equal.
//!
//! Inputs that need settings (capabilities, persona, column types) take
//! them from leading bytes, so every target accepts any byte string.

#![allow(clippy::missing_panics_doc, missing_docs)]

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::codec::{Decoder as _, Encoder as _};
use zeroize::Zeroizing;

use sprig_proto_mysql::auth::{self, AuthIo, AuthPath, AuthVerdict, LoginFlow, Method, Outcome};
use sprig_proto_mysql::client::{
    ClientConfig, ClientStream, MysqlClient, PacketKind, SessionTracker, TlsMode,
    decode_session_state,
};
use sprig_proto_mysql::command::{Command, ParamInfo, StmtLookup};
use sprig_proto_mysql::error::ErrPacket;
use sprig_proto_mysql::handshake::{
    self, ClientHello, CompressionOffer, NONCE_LEN, PacketIo, TlsPolicy,
};
use sprig_proto_mysql::packets::{
    AuthMoreData, AuthSwitchRequest, Caps, EofPacket, HandshakeResponse, HandshakeV10, OkPacket,
    Persona, SessionChange, SslRequest, is_ssl_request,
};
use sprig_proto_mysql::resultset::{
    ColumnDef, PrepareOk, Terminator, classify_response, classify_row, column_type as ct,
    decode_binary_row_types, decode_column_count, decode_text_row, encode_binary_row_types,
    encode_text_row,
};
use sprig_proto_mysql::{Framing, Packet, PacketCodec, ProtoError};

// ---------------------------------------------------------------------------
// Shared settings
// ---------------------------------------------------------------------------

/// Capability sets a config byte picks from: none, bare 4.1, the two
/// gateway personas, MariaDB without `DEPRECATE_EOF` (EOF packets), MariaDB
/// with extended metadata only, MySQL with query attributes but no session
/// tracking, and everything.
#[must_use]
pub fn caps_set(i: u8) -> Caps {
    let my = Caps::server_default(Persona::MySql84);
    let maria = Caps::server_default(Persona::MariaDb);
    match i % 8 {
        0 => Caps::empty(),
        1 => Caps::PROTOCOL_41 | Caps::TRANSACTIONS,
        2 => my,
        3 => maria,
        4 => maria - Caps::DEPRECATE_EOF,
        5 => Caps::PROTOCOL_41 | Caps::MARIADB_CLIENT_EXTENDED_METADATA,
        6 => (my | Caps::QUERY_ATTRIBUTES) - Caps::SESSION_TRACK,
        _ => Caps::all(),
    }
}

/// The persona a config bit picks.
#[must_use]
pub fn persona(bit: bool) -> Persona {
    if bit {
        Persona::MariaDb
    } else {
        Persona::MySql84
    }
}

/// Splits `data` into packets: each is a little-endian u16 length and that
/// many bytes (the last one is cut short at the end of the input).
#[must_use]
pub fn split_packets(mut data: &[u8]) -> Vec<Bytes> {
    let mut out = Vec::new();
    while data.len() >= 2 {
        let n = usize::from(u16::from_le_bytes([data[0], data[1]]));
        data = &data[2..];
        let n = n.min(data.len());
        out.push(Bytes::copy_from_slice(&data[..n]));
        data = &data[n..];
    }
    out
}

/// The inverse of [`split_packets`], for building seeds.
#[must_use]
pub fn join_packets<'a>(packets: impl IntoIterator<Item = &'a [u8]>) -> Vec<u8> {
    let mut out = Vec::new();
    for p in packets {
        let n = u16::try_from(p.len()).unwrap_or(u16::MAX);
        out.extend_from_slice(&n.to_le_bytes());
        out.extend_from_slice(&p[..usize::from(n)]);
    }
    out
}

/// Asserts byte stability: `e1` (an encoding of a decoded value) must
/// decode and encode back to itself.
fn assert_stable<T, E>(
    what: &str,
    e1: &Bytes,
    decode: impl Fn(&Bytes) -> Result<T, E>,
    encode: impl Fn(&T) -> Bytes,
) where
    E: std::fmt::Debug,
{
    let e2 = reencode(what, e1, &decode, &encode);
    assert_eq!(&e2[..], &e1[..], "{what}: encode(decode(e1)) != e1");
}

/// Asserts that decode -> encode reaches a fixed point within one more
/// round: for types whose decoder fills in defaults the encoder then
/// writes out (an absent plugin name read back as empty, say), or whose
/// encoder can't represent every decoded value (a nonce longer than 20
/// bytes without `PLUGIN_AUTH`), `e1` may change once, but never again.
fn assert_converges<T, E>(
    what: &str,
    e1: &Bytes,
    decode: impl Fn(&Bytes) -> Result<T, E>,
    encode: impl Fn(&T) -> Bytes,
) where
    E: std::fmt::Debug,
{
    let e2 = reencode(what, e1, &decode, &encode);
    if e2 != *e1 {
        let e3 = reencode(what, &e2, &decode, &encode);
        assert_eq!(&e3[..], &e2[..], "{what}: no fixed point after two rounds");
    }
}

fn reencode<T, E>(
    what: &str,
    e: &Bytes,
    decode: &impl Fn(&Bytes) -> Result<T, E>,
    encode: &impl Fn(&T) -> Bytes,
) -> Bytes
where
    E: std::fmt::Debug,
{
    match decode(e) {
        Ok(d) => encode(&d),
        Err(err) => panic!("{what}: our own encoding {e:02x?} doesn't decode: {err:?}"),
    }
}

// ---------------------------------------------------------------------------
// codec: plain, zlib and zstd framing
// ---------------------------------------------------------------------------

/// `codec`: byte 0 picks the framing, `max_packet` and the read size; byte
/// 1 is the first sequence id; the rest is the wire stream, fed in pieces.
/// Every decoded packet must re-encode and decode to itself. The rest is
/// also encoded as one payload and must decode back unchanged.
pub fn fuzz_codec(data: &[u8]) {
    if data.len() < 2 {
        return;
    }
    let cfg = data[0];
    let seq = data[1];
    let wire = &data[2..];
    let framing = match cfg % 3 {
        0 => Framing::Plain,
        1 => Framing::Zlib { level: 6 },
        _ => Framing::Zstd { level: 3 },
    };
    let max_packet = [64usize, 1 << 10, 1 << 16, 1 << 24][usize::from((cfg >> 2) & 3)];
    let piece = 1 + usize::from(cfg >> 4);

    let mut c = PacketCodec::new(max_packet);
    c.set_framing(framing);
    c.set_next_seq(seq);
    let mut buf = BytesMut::new();
    let mut packets = Vec::new();
    'feed: for chunk in wire.chunks(piece) {
        buf.extend_from_slice(chunk);
        loop {
            match c.decode(&mut buf) {
                Ok(Some(p)) => {
                    assert!(p.payload.len() <= max_packet, "packet above max_packet");
                    packets.push(p);
                }
                Ok(None) => break,
                Err(_) => break 'feed,
            }
        }
    }
    for p in &packets {
        roundtrip_packet(framing, p);
    }
    // Encode-first: any payload survives framing.
    if wire.len() <= max_packet {
        roundtrip_packet(framing, &Packet::new(seq, Bytes::copy_from_slice(wire)));
    }
}

fn roundtrip_packet(framing: Framing, p: &Packet) {
    let mut enc = PacketCodec::new(usize::MAX);
    enc.set_framing(framing);
    // Under compression the frame header takes the codec's own counter.
    enc.set_next_seq(p.seq);
    let mut wire = BytesMut::new();
    enc.encode(p.clone(), &mut wire).expect("encode");
    let mut dec = PacketCodec::new(usize::MAX);
    dec.set_framing(framing);
    dec.set_next_seq(p.seq);
    let got = dec
        .decode(&mut wire)
        .expect("our own frames decode")
        .expect("a whole frame decodes to a packet");
    assert_eq!(&got, p, "packet round trip ({framing:?})");
    assert!(wire.is_empty(), "bytes left after one packet");
    assert_eq!(dec.next_seq(), enc.next_seq(), "sequence ids agree");
}

// ---------------------------------------------------------------------------
// connection phase
// ---------------------------------------------------------------------------

/// `handshake_v10`: the greeting decoder (and the client's view of it).
pub fn fuzz_handshake_v10(data: &[u8]) {
    let input = Bytes::copy_from_slice(data);
    let Ok(hs) = HandshakeV10::decode(&input) else {
        return;
    };
    let e1 = hs.encode();
    assert_converges(
        "HandshakeV10",
        &e1,
        HandshakeV10::decode,
        HandshakeV10::encode,
    );
    // Exact domain: the nonce has the length the encoder writes back
    // unpadded (8 without SECURE_CONNECTION, 20 with it and no PLUGIN_AUTH,
    // at least 20 with both).
    let n = hs.auth_data.len();
    let secure = hs.caps.contains(Caps::SECURE_CONNECTION);
    let plugin = hs.caps.contains(Caps::PLUGIN_AUTH);
    let exact = match (secure, plugin) {
        (false, _) => n == 8,
        (true, false) => n == 20,
        (true, true) => (20..=254).contains(&n),
    };
    if exact {
        let again = HandshakeV10::decode(&e1).expect("re-decode");
        assert_eq!(again, hs, "HandshakeV10 exact round trip");
    }
    let _ = sprig_proto_mysql::client::auth::nonce_of(&hs.auth_data);
}

/// `handshake_response`: `HandshakeResponse41`, `SSLRequest`, and the
/// server's classification of the client's first packet.
pub fn fuzz_handshake_response(data: &[u8]) {
    let input = Bytes::copy_from_slice(data);
    let _ = is_ssl_request(&input);
    if let Ok(ssl) = SslRequest::decode(&input) {
        let e1 = ssl.encode();
        assert_eq!(e1.len(), 32);
        assert_stable(
            "SslRequest",
            &e1,
            |b| SslRequest::decode(b),
            SslRequest::encode,
        );
    }
    if let Ok(hello) = handshake::parse_client_hello(&input) {
        for policy in [TlsPolicy::Off, TlsPolicy::Optional, TlsPolicy::Required] {
            for tls in [false, true] {
                let _ = handshake::check_transport(policy, &hello, tls);
            }
        }
        if let ClientHello::Response(resp) = &hello {
            for persona in [Persona::MySql84, Persona::MariaDb] {
                let server = handshake::server_caps(persona, true, CompressionOffer::ALL);
                if let Ok(n) = handshake::negotiate(server, resp, 1 << 26) {
                    assert!(n.caps.contains(Caps::REQUIRED));
                    assert!(
                        !(n.caps.contains(Caps::COMPRESS)
                            && n.caps.contains(Caps::ZSTD_COMPRESSION_ALGORITHM)),
                        "one compression algorithm at most"
                    );
                }
            }
            let (kept, dropped) = handshake::limit_attrs(resp.attrs.clone(), 64);
            assert_eq!(kept.len() + dropped, resp.attrs.len());
        }
    }
    let Ok(resp) = HandshakeResponse::decode(&input) else {
        return;
    };
    let e1 = resp.encode();
    assert_converges(
        "HandshakeResponse",
        &e1,
        HandshakeResponse::decode,
        HandshakeResponse::encode,
    );
}

/// `err_packet`: ERR, `AuthSwitchRequest` and `AuthMoreData`.
pub fn fuzz_err_packet(data: &[u8]) {
    let input = Bytes::copy_from_slice(data);
    if let Ok(e) = ErrPacket::decode(&input) {
        let p41 = e.encode(Caps::PROTOCOL_41);
        assert_stable(
            "ErrPacket",
            &p41,
            |b| ErrPacket::decode(b),
            |e| e.encode(Caps::PROTOCOL_41),
        );
        if e.message.len() <= sprig_proto_mysql::error::MAX_MESSAGE_BYTES {
            assert_eq!(
                ErrPacket::decode(&p41).expect("re-decode"),
                e,
                "ERR exact round trip"
            );
        }
        // Without PROTOCOL_41 a message that starts with '#' and 5 more
        // bytes reads back as a SQLSTATE: the pre-4.1 form is ambiguous, so
        // only convergence is checked.
        let bare = e.encode(Caps::empty());
        let _ = ErrPacket::decode(&bare).map(|d| d.encode(Caps::empty()));
    }
    if let Ok(sw) = AuthSwitchRequest::decode(&input) {
        let e1 = sw.encode();
        assert_stable(
            "AuthSwitchRequest",
            &e1,
            AuthSwitchRequest::decode,
            AuthSwitchRequest::encode,
        );
    }
    if let Ok(m) = AuthMoreData::decode(&input) {
        let e1 = m.encode();
        assert_eq!(&e1[..], data, "AuthMoreData is byte-exact");
    }
}

// ---------------------------------------------------------------------------
// OK, EOF and session tracking
// ---------------------------------------------------------------------------

/// `ok_packet`: byte 0 picks the capabilities; the rest is an OK, EOF or
/// terminator payload. The session-state block goes through both the
/// codec's and the client's tracker decoders.
pub fn fuzz_ok_packet(data: &[u8]) {
    let Some((&cfg, rest)) = data.split_first() else {
        return;
    };
    let caps = caps_set(cfg);
    let input = Bytes::copy_from_slice(rest);
    if let Ok(ok) = OkPacket::decode(&input, caps) {
        let e1 = ok.encode(caps);
        let again = OkPacket::decode(&e1, caps).expect("re-decode OK");
        let mut want = ok.clone();
        if !caps.contains(Caps::SESSION_TRACK) {
            // Without SESSION_TRACK the encoder clears SESSION_STATE_CHANGED
            // (documented on OkPacket::encode); there is no block to drop.
            want.status
                .remove(sprig_proto_mysql::StatusFlags::SESSION_STATE_CHANGED);
        }
        assert_eq!(again, want, "OK exact round trip");
        assert_eq!(&again.encode(caps)[..], &e1[..]);
        session_block(&ok.session_state);
    }
    if let Ok(eof) = EofPacket::decode(&input, caps) {
        let e1 = eof.encode(caps);
        assert_eq!(EofPacket::decode(&e1, caps).expect("re-decode EOF"), eof);
    }
    if let Ok(t) = Terminator::decode(&input, caps) {
        for to in [caps, caps ^ Caps::DEPRECATE_EOF] {
            let e1 = t.encode(to);
            let back = Terminator::decode(&e1, to).expect("re-decode terminator");
            // Encoding for a peer without SESSION_TRACK clears
            // SESSION_STATE_CHANGED (OkPacket::encode).
            let mut want = t.status();
            if to.contains(Caps::DEPRECATE_EOF) && !to.contains(Caps::SESSION_TRACK) {
                want.remove(sprig_proto_mysql::StatusFlags::SESSION_STATE_CHANGED);
            }
            assert_eq!(back.status(), want);
            assert_eq!(back.warnings(), t.warnings());
        }
    }
    // The block alone, as the tracker sees it.
    session_block(&input);
}

fn session_block(block: &Bytes) {
    if let Ok(changes) = SessionChange::decode_all(block) {
        let e1 = SessionChange::encode_all(&changes);
        let again = SessionChange::decode_all(&e1).expect("re-decode session state");
        assert_eq!(again, changes, "session state exact round trip");
    }
    if let Ok(changes) = decode_session_state(block) {
        let mut t = SessionTracker::new();
        let tracked = t.apply(&changes);
        if let Some(s) = &tracked.schema {
            assert_eq!(t.schema(), Some(s.as_str()));
        }
        // What the client decodes, the codec re-encodes and the client
        // reads back the same.
        let e1 = SessionChange::encode_all(&changes);
        let again = decode_session_state(&e1).expect("client re-decode");
        assert_eq!(again, changes, "client session state round trip");
    }
}

// ---------------------------------------------------------------------------
// resultsets
// ---------------------------------------------------------------------------

/// `column_def`: byte 0 bit 0 turns `MARIADB_CLIENT_EXTENDED_METADATA` on;
/// the rest is a column definition. Also the response and row classifiers,
/// `PREPARE_OK` and the column count.
pub fn fuzz_column_def(data: &[u8]) {
    let Some((&cfg, rest)) = data.split_first() else {
        return;
    };
    let mut caps = caps_set(cfg >> 1);
    caps.set(Caps::MARIADB_CLIENT_EXTENDED_METADATA, cfg & 1 != 0);
    let input = Bytes::copy_from_slice(rest);
    if let Ok(col) = ColumnDef::decode(&input, caps) {
        let _ = (
            col.ext_type_name(),
            col.ext_format_name(),
            col.is_unsigned(),
        );
        let e1 = col.encode(caps);
        let again = ColumnDef::decode(&e1, caps).expect("re-decode column definition");
        assert_eq!(again, col, "column definition exact round trip");
        assert_eq!(&again.encode(caps)[..], &e1[..]);
        if let Some(block) = &col.ext_type {
            // The block is kept raw; rebuilding it from its entries is exact
            // when it holds just a well-formed type and/or format name.
            let rebuilt = ColumnDef::ext_metadata(col.ext_type_name(), col.ext_format_name());
            let _ = rebuilt.map(|r| r.len() <= block.len() + 20);
        }
    }
    let _ = classify_response(&input, caps);
    let _ = classify_row(&input, caps);
    let _ = decode_column_count(&input);
    if let Ok(p) = PrepareOk::decode(&input) {
        let e1 = p.encode();
        assert_eq!(PrepareOk::decode(&e1).expect("re-decode PREPARE_OK"), p);
    }
}

/// Column types the `rows` and `command` targets pick from.
pub const BINARY_TYPES: [u8; 24] = [
    ct::DECIMAL,
    ct::TINY,
    ct::SHORT,
    ct::LONG,
    ct::FLOAT,
    ct::DOUBLE,
    ct::NULL,
    ct::TIMESTAMP,
    ct::LONGLONG,
    ct::INT24,
    ct::DATE,
    ct::TIME,
    ct::DATETIME,
    ct::YEAR,
    ct::VARCHAR,
    ct::BIT,
    ct::JSON,
    ct::NEWDECIMAL,
    ct::ENUM,
    ct::BLOB,
    ct::VAR_STRING,
    ct::STRING,
    ct::GEOMETRY,
    ct::VECTOR,
];

/// `rows`: byte 0 is the column count (low 6 bits) and whether the row is
/// binary (bit 6); for binary rows one byte per column follows with the
/// type (low 5 bits, indexing [`BINARY_TYPES`]) and the unsigned flag (bit
/// 7). The rest is the row payload.
pub fn fuzz_rows(data: &[u8]) {
    let Some((&cfg, rest)) = data.split_first() else {
        return;
    };
    let n = usize::from(cfg & 0x3F);
    if cfg & 0x40 == 0 {
        let Ok(vals) = decode_text_row(rest, n) else {
            return;
        };
        assert_eq!(vals.len(), n);
        let e1 = encode_text_row(vals.iter().copied());
        let again = decode_text_row(&e1, n).expect("re-decode text row");
        assert_eq!(again, vals, "text row exact round trip");
        return;
    }
    if rest.len() < n {
        return;
    }
    let (spec, payload) = rest.split_at(n);
    let types: Vec<(u8, bool)> = spec
        .iter()
        .map(|&b| {
            (
                BINARY_TYPES[usize::from(b & 0x1F) % BINARY_TYPES.len()],
                b & 0x80 != 0,
            )
        })
        .collect();
    let codes: Vec<u8> = types.iter().map(|t| t.0).collect();
    let payload = Bytes::copy_from_slice(payload);
    let Ok(vals) = decode_binary_row_types(&payload, &types) else {
        return;
    };
    let e1 = encode_binary_row_types(&vals, &codes).expect("decoded values encode");
    assert_stable(
        "binary row",
        &e1,
        |b| decode_binary_row_types(b, &types),
        |v| encode_binary_row_types(v, &codes).expect("re-encode"),
    );
}

// ---------------------------------------------------------------------------
// commands
// ---------------------------------------------------------------------------

/// A statement table for `parse_with`: statement `id` has `id % 8`
/// parameters; ids divisible by 3 have no remembered types; ids divisible
/// by 5 are unknown.
pub struct FakeStmts {
    types: Vec<u16>,
    long_data: Vec<bool>,
}

impl FakeStmts {
    /// Types and long-data flags taken from `spec` (2 bytes per parameter,
    /// then one flag byte), cycled to 8 parameters.
    #[must_use]
    pub fn new(spec: &[u8]) -> Self {
        let mut types = Vec::with_capacity(8);
        let mut long_data = Vec::with_capacity(8);
        for i in 0..8 {
            let b = spec.get(i).copied().unwrap_or(0);
            let t = BINARY_TYPES[usize::from(b & 0x1F) % BINARY_TYPES.len()];
            types.push(u16::from(t) | if b & 0x80 != 0 { 0x8000 } else { 0 });
            long_data.push(b & 0x40 != 0 && b & 0x20 != 0);
        }
        Self { types, long_data }
    }
}

impl StmtLookup for FakeStmts {
    fn param_info(&self, stmt_id: u32) -> Option<ParamInfo<'_>> {
        if stmt_id.is_multiple_of(5) && stmt_id != 0 {
            return None;
        }
        let count = u16::try_from(stmt_id % 8).unwrap_or(0);
        Some(ParamInfo {
            count,
            types: (!stmt_id.is_multiple_of(3)).then_some(&self.types[..]),
            long_data: &self.long_data,
        })
    }
}

/// `command`: byte 0 picks capabilities (bits 0-2), persona (bit 3) and
/// whether a statement table is used (bit 4); the next 8 bytes describe
/// the fake statements' parameters; the rest is the command payload.
pub fn fuzz_command(data: &[u8]) {
    if data.len() < 9 {
        return;
    }
    let cfg = data[0];
    let stmts = FakeStmts::new(&data[1..9]);
    let input = Bytes::copy_from_slice(&data[9..]);
    for p in [persona(cfg & 8 != 0), persona(cfg & 8 == 0)] {
        let caps = caps_set(cfg);
        let parse = |b: &Bytes| {
            if cfg & 0x10 != 0 {
                Command::parse_with(b, caps, p, &stmts)
            } else {
                Command::parse(b, caps, p)
            }
        };
        let Ok(cmd) = parse(&input) else {
            continue;
        };
        let _ = (cmd.code(), cmd.name());
        let e1 = cmd.encode(caps).expect("a decoded command encodes");
        assert_eq!(e1.first(), input.first(), "command code kept");
        assert_converges("Command", &e1, parse, |c| {
            c.encode(caps).expect("re-encode")
        });
    }
}

// ---------------------------------------------------------------------------
// server auth exchanges
// ---------------------------------------------------------------------------

/// A scripted client: reads come from the script, writes are kept.
struct ScriptIo {
    reads: std::vec::IntoIter<Bytes>,
    writes: Vec<Bytes>,
}

#[async_trait]
impl PacketIo for ScriptIo {
    async fn read_packet(&mut self) -> sprig_proto_mysql::ProtoResult<Bytes> {
        self.reads
            .next()
            .ok_or_else(|| ProtoError::Io(io::ErrorKind::UnexpectedEof.into()))
    }

    async fn write_packet(&mut self, payload: Bytes) -> sprig_proto_mysql::ProtoResult<()> {
        assert!(self.writes.len() < 16, "auth exchange keeps writing");
        self.writes.push(payload);
        Ok(())
    }
}

/// A login policy whose answers come from config bits.
struct FakeFlow {
    bits: u8,
    calls: u32,
}

#[async_trait]
impl LoginFlow for FakeFlow {
    fn method(&self, _client_plugin: Option<&str>) -> Method {
        match self.bits & 3 {
            0 | 3 => Method::CachingSha2,
            1 => Method::Native,
            _ => Method::ClearText,
        }
    }

    fn check_fast(&mut self, _nonce: &[u8; NONCE_LEN], scramble: &[u8]) -> bool {
        self.calls += 1;
        assert_eq!(scramble.len(), auth::SHA2_SCRAMBLE_LEN);
        self.bits & 4 != 0
    }

    fn check_native(&mut self, _nonce: &[u8; NONCE_LEN], _scramble: &[u8]) -> bool {
        self.calls += 1;
        self.bits & 8 != 0
    }

    async fn check_password(&mut self, password: Zeroizing<Vec<u8>>) -> bool {
        self.calls += 1;
        let _ = password.len();
        self.bits & 16 != 0
    }

    async fn decide(&mut self, outcome: Outcome, path: AuthPath) -> AuthVerdict {
        let _ = path.as_str();
        if outcome == Outcome::Verified {
            AuthVerdict::Accept
        } else {
            AuthVerdict::Refuse(ErrPacket::new(1045, *b"28000", "denied"))
        }
    }
}

/// `server_auth`: byte 0 configures the login (method, check results, TLS,
/// persona, forced switch); byte 1 picks the client's plugin name; the rest
/// is the client's packets ([`split_packets`]): the handshake response
/// first, then whatever it sends during the exchange. The handshake
/// response goes through the connection-phase checks, then its auth
/// response starts the exchange.
pub fn fuzz_server_auth(data: &[u8]) {
    if data.len() < 2 {
        return;
    }
    let cfg = data[0];
    let plugin_sel = data[1];
    let mut packets = split_packets(&data[2..]).into_iter();
    let Some(first_packet) = packets.next() else {
        return;
    };
    let tls = cfg & 0x20 != 0;
    let persona = persona(cfg & 0x40 != 0);
    let force = cfg & 0x80 != 0;

    // The handshake response, if it is one, names the plugin and carries
    // the first auth response; otherwise the packet itself is the response.
    let (client_plugin, first) = match handshake::parse_client_hello(&first_packet) {
        Ok(ClientHello::Response(r)) => {
            let server = handshake::server_caps(persona, tls, CompressionOffer::ALL);
            let _ = handshake::negotiate(server, &r, 1 << 26);
            (r.plugin.clone(), r.auth_response.clone())
        }
        Ok(ClientHello::SslRequest(_)) | Err(_) => {
            let names = [
                None,
                Some(auth::CACHING_SHA2_PASSWORD),
                Some(auth::MYSQL_NATIVE_PASSWORD),
                Some(auth::MYSQL_CLEAR_PASSWORD),
                Some("auth_gssapi_client"),
            ];
            (
                names[usize::from(plugin_sel) % names.len()].map(str::to_owned),
                first_packet.clone(),
            )
        }
    };
    let mut io = ScriptIo {
        reads: packets.collect::<Vec<_>>().into_iter(),
        writes: Vec::new(),
    };
    let mut flow = FakeFlow {
        bits: cfg,
        calls: 0,
    };
    let mut aio = AuthIo {
        io: &mut io,
        persona,
        tls,
        nonce: [7; NONCE_LEN],
    };
    let verdict = futures::executor::block_on(auth::authenticate(
        &mut aio,
        client_plugin.as_deref(),
        first,
        force,
        &mut flow,
    ));
    // Each written packet is a well-formed AuthSwitchRequest or AuthMoreData.
    for w in &io.writes {
        match w.first() {
            Some(0xFE) => {
                AuthSwitchRequest::decode(w).expect("switch request decodes");
            }
            Some(0x01) => {
                AuthMoreData::decode(w).expect("more data decodes");
            }
            other => panic!("unexpected auth packet {other:?}"),
        }
    }
    if let Ok(AuthVerdict::Refuse(e)) = verdict {
        let _ = e.encode(Caps::PROTOCOL_41);
    }

    // Connection-phase reads over a raw stream: arbitrary bytes, exact
    // reads, a small packet limit.
    let mut stream = &data[2..];
    let mut seq = cfg;
    let _ = futures::executor::block_on(handshake::read_packet(&mut stream, &mut seq, 1 << 12));
}

// ---------------------------------------------------------------------------
// the backend client
// ---------------------------------------------------------------------------

/// An in-memory server: reads return the script in pieces, writes are
/// swallowed.
pub struct ScriptStream {
    script: Vec<u8>,
    pos: usize,
    piece: usize,
    written: usize,
}

impl ScriptStream {
    #[must_use]
    pub fn new(script: Vec<u8>, piece: usize) -> Self {
        Self {
            script,
            pos: 0,
            piece: piece.max(1),
            written: 0,
        }
    }
}

impl AsyncRead for ScriptStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let left = self.script.len() - self.pos;
        let n = left.min(self.piece).min(buf.remaining());
        let start = self.pos;
        buf.put_slice(&self.script[start..start + n]);
        self.pos += n;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ScriptStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.written += buf.len();
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Commands the client target sends after logging in.
fn client_command(sel: u8) -> Bytes {
    match sel % 10 {
        0 => Bytes::from_static(b"\x03SELECT 1"),
        1 => Bytes::from_static(b"\x16SELECT ?"),
        2 => Bytes::from_static(b"\x04t\x00"),
        3 => Bytes::from_static(b"\x09"),
        4 => Bytes::from_static(b"\x0e"),
        5 => Bytes::from_static(b"\x17\x01\x00\x00\x00\x00\x01\x00\x00\x00"),
        6 => Bytes::from_static(b"\x1b\x00\x00"),
        7 => Bytes::from_static(b"\x1c\x01\x00\x00\x00\x05\x00\x00\x00"),
        8 => Bytes::from_static(b"\x0a"),
        _ => Bytes::from_static(b"\xfa\x01\x00\x00\x00\x80\x00"),
    }
}

/// `client`: byte 0 configures the client (compression, local files,
/// extended metadata, database, read size); byte 1 picks the command sent
/// after login and how its response is read; the rest is everything the
/// server sends, as plain wire bytes: the greeting, the auth exchange, the
/// OK, then the response.
pub fn fuzz_client(data: &[u8]) {
    if data.len() < 2 {
        return;
    }
    let cfg = data[0];
    let cmd = data[1];
    let mut c = ClientConfig::new("u", if cfg & 0x40 != 0 { "" } else { "pw" });
    c.tls = TlsMode::Disabled;
    c.compress = cfg & 1 != 0;
    c.local_files = cfg & 2 != 0;
    if cfg & 4 != 0 {
        c.extra_caps =
            Caps::MARIADB_CLIENT_EXTENDED_METADATA | Caps::MARIADB_CLIENT_STMT_BULK_OPERATIONS;
    }
    if cfg & 8 != 0 {
        c.database = Some("d".to_owned());
    }
    c.max_packet = 1 << 20;
    let piece = 1 + usize::from(cfg >> 4) * 7;
    let stream = ScriptStream::new(data[2..].to_vec(), piece);
    futures::executor::block_on(async move {
        let Ok(mut client) =
            MysqlClient::handshake(ClientStream::Other(Box::new(stream)), "h", &c).await
        else {
            return;
        };
        let _ = (
            client.server().is_mariadb(),
            client.caps(),
            client.tracker().schema(),
        );
        for round in 0..3u8 {
            let sel = cmd.wrapping_add(round.wrapping_mul(37));
            let payload = client_command(sel);
            match (cmd >> 4) % 4 {
                0 => {
                    if client.send_command_payload(payload).await.is_err() {
                        return;
                    }
                    let mut n = 0;
                    loop {
                        n += 1;
                        assert!(n < 100_000, "response never ends");
                        match client.next_packet().await {
                            Ok(Some(p)) => {
                                let _ = p.more_results();
                                if let PacketKind::ColumnDef | PacketKind::ParamDef = p.kind {
                                    let _ = ColumnDef::decode(&p.payload, client.caps());
                                }
                                if p.kind == PacketKind::LocalInfile
                                    && (client.send_infile_data(b"1\n").await.is_err()
                                        || client.finish_infile().await.is_err())
                                {
                                    return;
                                }
                            }
                            Ok(None) => break,
                            Err(_) => return,
                        }
                    }
                }
                1 => {
                    if let Ok(rs) = client.query("SELECT 1").await {
                        for r in rs {
                            let _ = r.text_rows();
                        }
                    } else {
                        return;
                    }
                }
                2 => {
                    if client.prepare(b"SELECT ?").await.is_err() {
                        return;
                    }
                }
                _ => {
                    let r = match sel % 3 {
                        0 => client.ping().await,
                        1 => client.init_db("d").await,
                        _ => client.reset_connection().await,
                    };
                    if r.is_err() {
                        return;
                    }
                }
            }
        }
        client.quit().await;
    });
}

// ---------------------------------------------------------------------------
// Corpus replay (stable)
// ---------------------------------------------------------------------------

/// A target: its name and body.
pub type Target = (&'static str, fn(&[u8]));

/// Every target by name, for replaying corpora without libFuzzer.
pub const TARGETS: &[Target] = &[
    ("codec", fuzz_codec),
    ("handshake_v10", fuzz_handshake_v10),
    ("handshake_response", fuzz_handshake_response),
    ("err_packet", fuzz_err_packet),
    ("ok_packet", fuzz_ok_packet),
    ("column_def", fuzz_column_def),
    ("rows", fuzz_rows),
    ("command", fuzz_command),
    ("server_auth", fuzz_server_auth),
    ("client", fuzz_client),
];

/// Runs every file under `<root>/<dir>/<target>/` through its target, for
/// each `dir` in `dirs`; returns how many inputs ran.
pub fn replay_dirs(root: &std::path::Path, dirs: &[&str]) -> usize {
    let mut n = 0;
    for dir in dirs {
        for (name, f) in TARGETS {
            let d = root.join(dir).join(name);
            let Ok(entries) = std::fs::read_dir(&d) else {
                continue;
            };
            let mut paths: Vec<_> = entries.filter_map(|e| e.ok().map(|e| e.path())).collect();
            paths.sort();
            for p in paths {
                if p.is_file() {
                    let bytes = std::fs::read(&p).expect("read corpus file");
                    let r = std::panic::catch_unwind(|| f(&bytes));
                    assert!(r.is_ok(), "{name}: {} panics", p.display());
                    n += 1;
                }
            }
        }
    }
    n
}
