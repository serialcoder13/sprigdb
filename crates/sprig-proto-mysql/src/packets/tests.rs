use bytes::Bytes;

use super::*;

fn nonce() -> Bytes {
    Bytes::from((1u8..=20).collect::<Vec<_>>())
}

#[test]
fn server_defaults_follow_the_capability_table() {
    let my = Caps::server_default(Persona::MySql84);
    let maria = Caps::server_default(Persona::MariaDb);
    for c in [my, maria] {
        assert!(c.contains(Caps::REQUIRED));
        assert!(c.contains(Caps::DEPRECATE_EOF | Caps::SESSION_TRACK));
        assert!(!c.contains(Caps::SSL));
        assert!(!c.contains(Caps::MARIADB_CLIENT_PROGRESS));
    }
    assert!(my.contains(Caps::CLIENT_MYSQL | Caps::QUERY_ATTRIBUTES));
    assert_eq!(my.extended(), 0);
    assert!(!maria.contains(Caps::CLIENT_MYSQL));
    assert!(!maria.contains(Caps::ZSTD_COMPRESSION_ALGORITHM));
    assert!(maria.contains(Caps::MARIADB_CLIENT_EXTENDED_METADATA));
    assert_eq!(maria.extended(), 0b1100);
}

#[test]
fn greeting_layout_and_round_trip() {
    for persona in [Persona::MySql84, Persona::MariaDb] {
        let hs = HandshakeV10 {
            server_version: persona.default_version().to_owned(),
            connection_id: 42,
            auth_data: nonce(),
            caps: Caps::server_default(persona) | Caps::SSL,
            collation: persona.greeting_collation(),
            status: StatusFlags::AUTOCOMMIT,
            auth_plugin: "caching_sha2_password".to_owned(),
        };
        let b = hs.encode();
        let v = persona.default_version().len();
        assert_eq!(b[0], 10);
        // Auth data part 1, filler, then the auth data length (21) after
        // caps, collation, status and caps high.
        let p = 1 + v + 1 + 4;
        assert_eq!(&b[p..p + 8], &nonce()[..8]);
        assert_eq!(b[p + 8], 0);
        assert_eq!(b[p + 9 + 2], persona.greeting_collation());
        assert_eq!(b[p + 9 + 7], 21);
        let ext = u32::from_le_bytes([b[p + 23], b[p + 24], b[p + 25], b[p + 26]]);
        assert_eq!(ext, hs.caps.extended());
        // Part 2: 12 bytes and a NUL.
        assert_eq!(&b[p + 27..p + 39], &nonce()[8..]);
        assert_eq!(b[p + 39], 0);
        assert_eq!(HandshakeV10::decode(&b).expect("decode"), hs);
    }
}

#[test]
fn response_round_trips() {
    let resp = HandshakeResponse {
        caps: Caps::server_default(Persona::MySql84),
        max_packet: 1 << 24,
        collation: 255,
        user: b"acme.alice".to_vec(),
        auth_response: Bytes::from_static(&[7; 32]),
        database: Some(b"shop".to_vec()),
        plugin: Some("caching_sha2_password".to_owned()),
        attrs: vec![
            ("_client_name".to_owned(), "libmysql".to_owned()),
            ("sprig_dc".to_owned(), "mum1".to_owned()),
        ],
        zstd_level: Some(7),
    };
    let b = resp.encode();
    assert_eq!(HandshakeResponse::decode(&b).expect("decode"), resp);

    // 1-byte auth length, no db, MariaDB extended caps in the filler.
    let mut caps = Caps::server_default(Persona::MariaDb);
    caps.remove(Caps::PLUGIN_AUTH_LENENC_CLIENT_DATA | Caps::CONNECT_WITH_DB);
    let resp = HandshakeResponse {
        caps,
        max_packet: 1 << 30,
        collation: 45,
        user: b"bob".to_vec(),
        auth_response: Bytes::from_static(b"scramble-of-20-bytes"),
        plugin: Some("mysql_native_password".to_owned()),
        ..HandshakeResponse::default()
    };
    let b = resp.encode();
    assert_eq!(&b[28..32], &caps.extended().to_le_bytes());
    let d = HandshakeResponse::decode(&b).expect("decode");
    assert!(d.caps.contains(Caps::MARIADB_CLIENT_STMT_BULK_OPERATIONS));
    assert_eq!(d, resp);
}

#[test]
fn ssl_request_is_the_first_32_bytes() {
    let req = SslRequest {
        caps: Caps::server_default(Persona::MySql84),
        max_packet: 1 << 24,
        collation: 255,
    };
    let b = req.encode();
    assert_eq!(b.len(), SSL_REQUEST_LEN);
    assert!(is_ssl_request(&b));
    let d = SslRequest::decode(&b).expect("decode");
    assert!(d.caps.contains(Caps::SSL));
    let full = HandshakeResponse {
        caps: req.caps | Caps::SSL,
        max_packet: req.max_packet,
        collation: 255,
        user: b"u".to_vec(),
        ..HandshakeResponse::default()
    }
    .encode();
    assert_eq!(&full[..32], &b[..]);
    assert!(!is_ssl_request(&full));
}

#[test]
fn ok_with_session_state() {
    // OK after `USE test`: status AUTOCOMMIT | SESSION_STATE_CHANGED, empty
    // info, one schema entry.
    let wire = Bytes::from_static(&[
        0x00, 0x00, 0x00, 0x02, 0x40, 0x00, 0x00, 0x00, 0x07, 0x01, 0x05, 0x04, b't', b'e', b's',
        b't',
    ]);
    let caps = Caps::PROTOCOL_41 | Caps::SESSION_TRACK;
    let ok = OkPacket::decode(&wire, caps).expect("decode");
    assert!(ok.status.contains(StatusFlags::SESSION_STATE_CHANGED));
    assert_eq!(
        ok.session_changes().expect("entries"),
        vec![SessionChange::Schema(Bytes::from_static(b"test"))]
    );
    assert_eq!(ok.encode(caps), wire);

    // Without SESSION_TRACK the block and the flag are dropped.
    let plain = ok.encode(Caps::PROTOCOL_41);
    assert_eq!(&plain[..], &[0, 0, 0, 0x02, 0x00, 0, 0]);
}

#[test]
fn every_tracker_entry_round_trips() {
    let changes = vec![
        SessionChange::SystemVariable {
            name: Bytes::from_static(b"autocommit"),
            value: Bytes::from_static(b"OFF"),
        },
        SessionChange::Schema(Bytes::from_static(b"shop")),
        SessionChange::StateChange(true),
        SessionChange::Gtids {
            encoding: 0,
            gtids: Bytes::from_static(b"3E11FA47-71CA-11E1-9E33-C80AA9429562:23"),
        },
        SessionChange::TransactionCharacteristics(Bytes::from_static(b"START TRANSACTION;")),
        SessionChange::TransactionState(Bytes::from_static(b"T_______")),
        SessionChange::Unknown {
            kind: 9,
            data: Bytes::from_static(b"\x01x"),
        },
    ];
    let mut ok = OkPacket {
        affected_rows: 3,
        last_insert_id: 70000,
        warnings: 1,
        info: Bytes::from_static(b"Rows matched: 3"),
        ..OkPacket::new(StatusFlags::AUTOCOMMIT | StatusFlags::IN_TRANS)
    };
    ok.set_session_changes(&changes);
    let caps = Caps::PROTOCOL_41 | Caps::SESSION_TRACK;
    let d = OkPacket::decode(&ok.encode(caps), caps).expect("decode");
    assert_eq!(d, ok);
    assert_eq!(d.session_changes().expect("entries"), changes);
}

#[test]
fn ok_info_without_session_track_is_the_rest() {
    let ok = OkPacket {
        info: Bytes::from_static(b"Records: 2  Duplicates: 0  Warnings: 0"),
        ..OkPacket::new(StatusFlags::AUTOCOMMIT)
    };
    let d = OkPacket::decode(&ok.encode(Caps::PROTOCOL_41), Caps::PROTOCOL_41).expect("decode");
    assert_eq!(d.info, ok.info);
}

#[test]
fn eof_and_auth_packets() {
    let eof = EofPacket {
        warnings: 2,
        status: StatusFlags::AUTOCOMMIT | StatusFlags::MORE_RESULTS_EXISTS,
    };
    let b = eof.encode(Caps::PROTOCOL_41);
    assert_eq!(&b[..], &[0xFE, 2, 0, 0x0A, 0]);
    assert_eq!(EofPacket::decode(&b, Caps::PROTOCOL_41).ok(), Some(eof));

    let sw = AuthSwitchRequest {
        plugin: "mysql_clear_password".to_owned(),
        data: Bytes::new(),
    };
    assert_eq!(AuthSwitchRequest::decode(&sw.encode()).ok(), Some(sw));
    let more = AuthMoreData {
        data: Bytes::from_static(&[0x04]),
    };
    assert_eq!(&more.encode()[..], &[0x01, 0x04]);
    assert_eq!(AuthMoreData::decode(&more.encode()).ok(), Some(more));
}

#[test]
fn state_change_entry_is_a_bare_flag() {
    // MariaDB 11.8 and MySQL 8.4 send `02 01 31` after `SET`, `USE` or a
    // temporary table: type 2, length 1, then '1' (no nested lenenc).
    let wire = Bytes::from_static(&[0x02, 0x01, b'1']);
    assert_eq!(
        SessionChange::decode_all(&wire).expect("decode"),
        vec![SessionChange::StateChange(true)]
    );
    assert_eq!(SessionChange::encode_all(&[SessionChange::StateChange(true)]), wire);
    // The nested form is still accepted when reading.
    let nested = Bytes::from_static(&[0x02, 0x02, 0x01, b'1']);
    assert_eq!(
        SessionChange::decode_all(&nested).expect("decode"),
        vec![SessionChange::StateChange(true)]
    );
}
