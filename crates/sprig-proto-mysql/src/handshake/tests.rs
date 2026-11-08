use bytes::Bytes;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::*;
use crate::packets::HandshakeResponse;
use TlsPolicy::{Off, Optional, Required};

fn response(caps: Caps) -> HandshakeResponse {
    HandshakeResponse {
        caps,
        max_packet: 16 << 20,
        collation: 255,
        user: b"app".to_vec(),
        auth_response: Bytes::from_static(&[7; 32]),
        plugin: Some("caching_sha2_password".to_owned()),
        ..HandshakeResponse::default()
    }
}

fn client_caps() -> Caps {
    (Caps::server_default(Persona::MySql84) | Caps::SSL)
        - Caps::COMPRESS
        - Caps::ZSTD_COMPRESSION_ALGORITHM
}

#[test]
fn nonces_are_twenty_printable_non_dollar_bytes_and_differ() {
    let a = new_nonce().expect("nonce");
    let b = new_nonce().expect("nonce");
    assert_ne!(a, b);
    for n in [a, b] {
        assert!(n.iter().all(|&c| (1..=0x7F).contains(&c) && c != b'$'));
    }
}

#[test]
fn greeting_per_persona_and_tls_offer() {
    let nonce = [b'a'; NONCE_LEN];
    for persona in [Persona::MySql84, Persona::MariaDb] {
        for tls in [false, true] {
            let caps = server_caps(persona, tls, CompressionOffer::ALL);
            let hs = greeting(persona, persona.default_version(), 7, &nonce, caps);
            let back = HandshakeV10::decode(&hs.encode()).expect("decode");
            assert_eq!(back.caps.contains(Caps::SSL), tls);
            assert_eq!(back.server_version, persona.default_version());
            assert_eq!(&back.auth_data[..], &nonce);
            assert_eq!(back.auth_plugin, GREETING_PLUGIN);
            assert_eq!(back.collation, persona.greeting_collation());
            assert_eq!(back.status, StatusFlags::AUTOCOMMIT);
            assert_eq!(back.connection_id, 7);
        }
    }
    let my = server_caps(Persona::MySql84, false, CompressionOffer::parse("zlib"));
    assert!(my.contains(Caps::COMPRESS) && !my.contains(Caps::ZSTD_COMPRESSION_ALGORITHM));
    let plain = server_caps(Persona::MySql84, false, CompressionOffer::parse(""));
    assert!(!plain.intersects(Caps::COMPRESS | Caps::ZSTD_COMPRESSION_ALGORITHM));
    assert_eq!(
        CompressionOffer::parse(" ZSTD , zlib,lz4"),
        CompressionOffer::ALL
    );
}

#[test]
fn client_hello_kinds() {
    let ssl = SslRequest {
        caps: client_caps(),
        max_packet: 1 << 24,
        collation: 255,
    }
    .encode();
    assert!(matches!(
        parse_client_hello(&ssl),
        Ok(ClientHello::SslRequest(r)) if r.caps.contains(Caps::SSL)
    ));
    let resp = response(client_caps()).encode();
    assert!(matches!(
        parse_client_hello(&resp),
        Ok(ClientHello::Response(r)) if r.user == b"app"
    ));
    // Pre-4.1: refused from the capability bits alone.
    let mut old = resp.to_vec();
    old[1] &= !0x02; // clear PROTOCOL_41 (bit 9)
    assert_eq!(
        parse_client_hello(&Bytes::from(old)),
        Err(Refusal::AuthModeNotSupported)
    );
    assert_eq!(
        parse_client_hello(&Bytes::from_static(b"\x01")),
        Err(Refusal::BadHandshake)
    );
}

#[test]
fn transport_rules_per_listener_mode() {
    let ssl = ClientHello::SslRequest(SslRequest {
        caps: client_caps(),
        max_packet: 0,
        collation: 255,
    });
    let plain = ClientHello::Response(response(client_caps()));
    assert_eq!(
        check_transport(Off, &ssl, false),
        Err(Refusal::BadHandshake)
    );
    assert_eq!(check_transport(Optional, &ssl, false), Ok(()));
    assert_eq!(check_transport(Required, &ssl, false), Ok(()));
    assert_eq!(
        check_transport(Required, &ssl, true),
        Err(Refusal::BadHandshake)
    );
    assert_eq!(check_transport(Off, &plain, false), Ok(()));
    assert_eq!(check_transport(Optional, &plain, false), Ok(()));
    assert_eq!(
        check_transport(Required, &plain, false),
        Err(Refusal::SecureTransportRequired)
    );
    assert_eq!(check_transport(Required, &plain, true), Ok(()));
    assert_eq!(
        Refusal::SecureTransportRequired.err(Persona::MySql84).code,
        3159
    );
    assert_eq!(Refusal::BadHandshake.err(Persona::MariaDb).code, 1043);
    assert_eq!(
        Refusal::AuthModeNotSupported.err(Persona::MySql84).code,
        1251
    );
}

#[test]
fn negotiation() {
    let server = server_caps(Persona::MySql84, true, CompressionOffer::ALL);
    let n = negotiate(server, &response(client_caps()), 64 << 20).expect("ok");
    assert!(n.caps.contains(Caps::REQUIRED | Caps::SSL));
    // The server's limit, not the client's (16 MiB here).
    assert_eq!(n.max_packet, 64 << 20);
    assert!(n.multi_statements);
    assert_eq!(n.framing, Framing::Plain);

    // Missing PLUGIN_AUTH: 1251.
    let bad = response(client_caps() - Caps::PLUGIN_AUTH);
    assert_eq!(
        negotiate(server, &bad, 1 << 20),
        Err(Refusal::AuthModeNotSupported)
    );

    // Compression: zlib wins over zstd; zstd takes the client's level.
    let both = response(client_caps() | Caps::COMPRESS | Caps::ZSTD_COMPRESSION_ALGORITHM);
    let n = negotiate(server, &both, 1 << 20).expect("ok");
    assert_eq!(n.framing, Framing::Zlib { level: 6 });
    assert!(!n.caps.contains(Caps::ZSTD_COMPRESSION_ALGORITHM));
    let mut z = response((client_caps() - Caps::COMPRESS) | Caps::ZSTD_COMPRESSION_ALGORITHM);
    z.zstd_level = Some(7);
    assert_eq!(
        negotiate(server, &z, 1 << 20).expect("ok").framing,
        Framing::Zstd { level: 7 }
    );
    // The MariaDB persona offers no zstd.
    let maria = server_caps(Persona::MariaDb, false, CompressionOffer::ALL);
    let n = negotiate(maria, &z, 1 << 20).expect("ok");
    assert_eq!(n.framing, Framing::Plain);
    // max_packet: always the gateway's.
    let mut r = response(client_caps());
    r.max_packet = 0;
    assert_eq!(negotiate(server, &r, 1234).expect("ok").max_packet, 1234);
    r.max_packet = 1000;
    assert_eq!(negotiate(server, &r, 1234).expect("ok").max_packet, 1234);
    r.max_packet = u32::MAX;
    assert_eq!(negotiate(server, &r, 1234).expect("ok").max_packet, 1234);
}

#[test]
fn connect_attrs_are_capped() {
    let attrs = vec![
        ("_client_name".to_owned(), "libmysql".to_owned()),
        ("big".to_owned(), "x".repeat(100)),
        ("sprig_dc".to_owned(), "mum1".to_owned()),
    ];
    let (kept, dropped) = limit_attrs(attrs, 40);
    assert_eq!(dropped, 1);
    assert_eq!(
        kept.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
        ["_client_name", "sprig_dc"]
    );
}

#[tokio::test]
async fn raw_reads_never_read_ahead() {
    let (mut client, mut server) = tokio::io::duplex(4096);
    let ssl = SslRequest {
        caps: client_caps(),
        max_packet: 0,
        collation: 255,
    }
    .encode();
    let mut seq = 1;
    write_packet(&mut client, &mut seq, &ssl)
        .await
        .expect("write");
    // The TLS ClientHello follows at once, before any answer.
    client.write_all(b"\x16\x03\x01hello").await.expect("write");
    let mut rseq = 1;
    let got = read_packet(&mut server, &mut rseq, MAX_CONNECTION_PACKET)
        .await
        .expect("read");
    assert_eq!(got, ssl);
    assert_eq!(rseq, 2);
    let mut rest = [0u8; 8];
    server.read_exact(&mut rest).await.expect("rest");
    assert_eq!(&rest, b"\x16\x03\x01hello");
}

#[tokio::test]
async fn raw_reads_check_sequence_and_size() {
    let (mut client, mut server) = tokio::io::duplex(4096);
    let mut seq = 5;
    write_packet(&mut client, &mut seq, b"abc")
        .await
        .expect("write");
    let mut rseq = 1;
    assert!(matches!(
        read_packet(&mut server, &mut rseq, 100).await,
        Err(ProtoError::OutOfOrder {
            expected: 1,
            got: 5
        })
    ));
    let (mut client, mut server) = tokio::io::duplex(4096);
    let mut seq = 0;
    write_packet(&mut client, &mut seq, &[0u8; 200])
        .await
        .expect("write");
    let mut rseq = 0;
    assert!(matches!(
        read_packet(&mut server, &mut rseq, 100).await,
        Err(ProtoError::PacketTooLarge { .. })
    ));
}

#[tokio::test]
async fn raw_io_and_framed_io_share_the_wire_format() {
    let (client, mut server) = tokio::io::duplex(4096);
    let mut framed = Framed::new(client, PacketCodec::default());
    framed.codec_mut().set_next_seq(3);
    framed
        .write_packet(Bytes::from_static(b"hi"))
        .await
        .expect("send");
    let mut raw = RawIo::new(&mut server, 3);
    assert_eq!(raw.read_packet().await.expect("read"), &b"hi"[..]);
    raw.write_packet(Bytes::from_static(b"yo"))
        .await
        .expect("write");
    assert_eq!(framed.read_packet().await.expect("read"), &b"yo"[..]);
}
