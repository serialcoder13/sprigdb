//! Seeded random bytes into every decoder: none may panic.
//!
//! Inputs are pure noise plus valid packets with random bytes flipped,
//! truncated or extended, so the decoders get past their first checks.

use bytes::{Bytes, BytesMut};
use tokio_util::codec::Decoder as _;

use crate::command::{Command, ParamInfo, StmtLookup};
use crate::error::ErrPacket;
use crate::packets::{
    AuthMoreData, AuthSwitchRequest, Caps, EofPacket, HandshakeResponse, HandshakeV10, OkPacket,
    Persona, SessionChange, SslRequest, StatusFlags,
};
use crate::resultset::{
    ColumnDef, PrepareOk, Terminator, classify_response, classify_row, column_type as ct,
    decode_binary_row_types, decode_column_count, decode_text_row,
};
use crate::{Framing, PacketCodec};

/// xorshift64*: deterministic and dependency-free.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % (n as u64).max(1)).unwrap_or(0)
    }

    fn byte(&mut self) -> u8 {
        self.next().to_le_bytes()[0]
    }

    fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| self.byte()).collect()
    }
}

/// Any statement id has 0-7 parameters with random bound types.
struct AnyStmt(Vec<u16>, Vec<bool>);

impl StmtLookup for AnyStmt {
    fn param_info(&self, stmt_id: u32) -> Option<ParamInfo<'_>> {
        let count = u16::try_from(stmt_id % 8).unwrap_or(0);
        Some(ParamInfo {
            count,
            types: (!stmt_id.is_multiple_of(3)).then_some(&self.0[..]),
            long_data: &self.1,
        })
    }
}

fn seeds() -> Vec<Bytes> {
    let my = Caps::server_default(Persona::MySql84);
    let mut out = vec![
        OkPacket::new(StatusFlags::AUTOCOMMIT).encode(my),
        EofPacket::default().encode(my),
        ErrPacket::new(1064, *b"42000", "syntax").encode(my),
        HandshakeV10 {
            server_version: "8.4.8-SprigDB".into(),
            connection_id: 1,
            auth_data: Bytes::from_static(&[5; 20]),
            caps: my,
            collation: 255,
            status: StatusFlags::AUTOCOMMIT,
            auth_plugin: "caching_sha2_password".into(),
        }
        .encode(),
        HandshakeResponse {
            caps: my,
            user: b"u".to_vec(),
            attrs: vec![("a".into(), "b".into())],
            database: Some(b"d".to_vec()),
            ..HandshakeResponse::default()
        }
        .encode(),
        ColumnDef::default().encode(Caps::MARIADB_CLIENT_EXTENDED_METADATA),
        PrepareOk::default().encode(),
        SessionChange::encode_all(&[SessionChange::Schema(Bytes::from_static(b"s"))]),
    ];
    for cmd in [
        &b"\x03\x01\x01\x00\x01\xfd\x00\x01aabcSELECT"[..],
        b"\x17\x01\x00\x00\x00\x08\x01\x00\x00\x00\x02\x00\x01\x08\x00\x00\x03\x00\x01x",
        b"\x17\x05\x00\x00\x00\x00\x01\x00\x00\x00\x00\x01\x03\x00\x03\x00\x03\x00\x03\x00\x03\x00",
        b"\xfa\x02\x00\x00\x00\x80\x00\x03\x00\x0c\x00\x00\x01\x00\x00\x00\x00\x00",
        b"\x11user\x00\x03abcdb\x00\x2d\x00plugin\x00\x05\x01a\x02bc",
    ] {
        out.push(Bytes::copy_from_slice(cmd));
    }
    out
}

fn mutate(rng: &mut Rng, seed: &[u8]) -> Vec<u8> {
    let mut v = seed.to_vec();
    match rng.below(4) {
        0 => {
            for _ in 0..=rng.below(4) {
                if !v.is_empty() {
                    let i = rng.below(v.len());
                    v[i] = rng.byte();
                }
            }
        }
        1 => v.truncate(rng.below(v.len() + 1)),
        2 => {
            let extra = rng.below(16);
            v.extend(rng.bytes(extra));
        }
        _ => {
            let n = rng.below(64);
            v = rng.bytes(n);
        }
    }
    v
}

fn decode_everything(input: &Bytes, lookup: &AnyStmt) {
    let caps_sets = [
        Caps::empty(),
        Caps::PROTOCOL_41,
        Caps::server_default(Persona::MySql84),
        Caps::server_default(Persona::MariaDb) | Caps::QUERY_ATTRIBUTES,
    ];
    for caps in caps_sets {
        for persona in [Persona::MySql84, Persona::MariaDb] {
            let _ = Command::parse(input, caps, persona);
            let _ = Command::parse_with(input, caps, persona, lookup);
        }
        let _ = OkPacket::decode(input, caps).map(|ok| ok.session_changes());
        let _ = EofPacket::decode(input, caps);
        let _ = ColumnDef::decode(input, caps).map(|c| c.ext_type_name().map(<[u8]>::len));
        let _ = Terminator::decode(input, caps);
        let _ = classify_row(input, caps);
        let _ = classify_response(input, caps);
    }
    let _ = SessionChange::decode_all(input);
    let _ = ErrPacket::decode(input);
    let _ = HandshakeV10::decode(input);
    let _ = HandshakeResponse::decode(input);
    let _ = SslRequest::decode(input);
    let _ = AuthSwitchRequest::decode(input);
    let _ = AuthMoreData::decode(input);
    let _ = PrepareOk::decode(input);
    let _ = decode_column_count(input);
    for n in [0, 1, 3, 9] {
        let _ = decode_text_row(input, n);
    }
    let types: Vec<(u8, bool)> = [
        ct::TINY,
        ct::LONGLONG,
        ct::DOUBLE,
        ct::DATETIME,
        ct::TIME,
        ct::VAR_STRING,
    ]
    .iter()
    .map(|&t| (t, t % 2 == 0))
    .collect();
    for n in 0..=types.len() {
        let _ = decode_binary_row_types(input, &types[..n]);
    }
}

fn feed_codec(rng: &mut Rng, input: &[u8]) {
    for framing in [
        Framing::Plain,
        Framing::Zlib { level: 6 },
        Framing::Zstd { level: 3 },
    ] {
        let mut c = PacketCodec::new(1 + rng.below(1 << 16));
        c.set_framing(framing);
        if rng.below(2) == 0 {
            c.set_next_seq(input.get(3).copied().unwrap_or(0));
        }
        let mut buf = BytesMut::new();
        for piece in input.chunks(1 + rng.below(16)) {
            buf.extend_from_slice(piece);
            while let Ok(Some(_)) = c.decode(&mut buf) {}
        }
    }
}

#[test]
fn random_bytes_never_panic_a_decoder() {
    let mut rng = Rng(0x5EED_5EED_1234_5678);
    let lookup = AnyStmt(
        vec![
            u16::from(ct::LONGLONG),
            u16::from(ct::VAR_STRING),
            u16::from(ct::DATETIME) | 0x8000,
            u16::from(ct::TIME),
            u16::from(ct::DOUBLE),
            u16::from(ct::TINY),
            u16::from(ct::NULL),
            u16::from(ct::BLOB),
        ],
        vec![false, true, false],
    );
    let seeds = seeds();
    for i in 0..20_000 {
        let input = if i % 2 == 0 {
            let n = rng.below(48);
            rng.bytes(n)
        } else {
            let s = &seeds[rng.below(seeds.len())];
            mutate(&mut rng, s)
        };
        let input = Bytes::from(input);
        decode_everything(&input, &lookup);
        if i % 4 == 0 {
            feed_codec(&mut rng, &input);
        }
    }
}
