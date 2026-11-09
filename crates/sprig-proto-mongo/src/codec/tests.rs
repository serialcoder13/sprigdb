use bson::{doc, rawdoc};

use super::*;

/// `{ping: 1, $db: "admin"}` as an OP_MSG with request id 1, written out
/// byte by byte from the OP_MSG and BSON specifications.
fn ping_bytes() -> Vec<u8> {
    let h = concat!(
        "33000000", "01000000", "00000000", "dd070000", "00000000", "00", "1e000000", "10",
        "70696e6700", "01000000", "02", "24646200", "06000000", "61646d696e00", "00"
    );
    hex::decode(h).unwrap()
}

#[test]
fn golden_ping_decodes_and_reencodes() {
    let b = ping_bytes();
    let (h, f, c) = decode(&b).unwrap();
    assert_eq!(h.length, 51);
    assert_eq!(h.request_id, 1);
    assert_eq!(h.op_code, OP_MSG);
    assert_eq!(c, None);
    let Frame::Msg(m) = &f else { panic!("not OP_MSG") };
    assert_eq!(m.flags, 0);
    assert_eq!(m.body, rawdoc! {"ping": 1, "$db": "admin"});
    assert!(m.sequences.is_empty());
    assert_eq!(encode(1, 0, &f, None).unwrap(), b);
}

#[test]
fn golden_checksum() {
    let mut b = ping_bytes();
    b[16] = 1; // checksumPresent
    let crc = crate::crc32c::checksum(&{
        let mut x = b.clone();
        x[0] = 55;
        x
    });
    b[0] = 55;
    b.extend_from_slice(&crc.to_le_bytes());
    let (_, f, _) = decode(&b).unwrap();
    assert!(matches!(f, Frame::Msg(_)));
    let n = b.len();
    b[n - 1] ^= 0xff;
    assert_eq!(decode(&b).unwrap_err(), CodecError::Checksum);
}

#[test]
fn kind1_sequence_round_trip() {
    let m = OpMsg {
        flags: 0,
        body: rawdoc! {"insert": "c", "$db": "d"},
        sequences: vec![DocSequence {
            identifier: "documents".into(),
            docs: vec![rawdoc! {"_id": 1}, rawdoc! {"_id": 2, "x": "y"}],
        }],
    };
    let b = encode(7, 0, &Frame::Msg(m.clone()), None).unwrap();
    // kind 1 section: 1 | i32 size | "documents\0" | docs
    let at = 16 + 4 + 1 + m.body.as_bytes().len();
    assert_eq!(b[at], 1);
    let size = i32::from_le_bytes([b[at + 1], b[at + 2], b[at + 3], b[at + 4]]);
    assert_eq!(usize::try_from(size).unwrap(), b.len() - at - 1);
    let (_, f, _) = decode(&b).unwrap();
    assert_eq!(f, Frame::Msg(m));
}

#[test]
fn compressors_round_trip() {
    let body = rawdoc! {"find": "c", "filter": {"a": "x".repeat(500)}, "$db": "d"};
    let f = Frame::Msg(OpMsg {
        flags: 0,
        body,
        sequences: Vec::new(),
    });
    for c in [Compressor::Snappy, Compressor::Zlib, Compressor::Zstd] {
        let b = encode(3, 9, &f, Some(c)).unwrap();
        assert_eq!(i32::from_le_bytes([b[12], b[13], b[14], b[15]]), OP_COMPRESSED);
        assert_eq!(b[24], c.id());
        assert!(b.len() < 400, "{c:?} didn't compress");
        let (h, g, used) = decode(&b).unwrap();
        assert_eq!(used, Some(c));
        assert_eq!(h.op_code, OP_MSG);
        assert_eq!(h.response_to, 9);
        assert_eq!(g, f);
    }
}

#[test]
fn compression_bomb_and_garbage_refused() {
    let f = Frame::Msg(OpMsg {
        flags: 0,
        body: rawdoc! {"ping": 1},
        sequences: Vec::new(),
    });
    let mut b = encode(1, 0, &f, Some(Compressor::Zlib)).unwrap();
    // uncompressedSize past 48 MB
    b[20..24].copy_from_slice(&50_000_000i32.to_le_bytes());
    assert!(matches!(decode(&b), Err(CodecError::Compression(_))));
    let mut b = encode(1, 0, &f, Some(Compressor::Snappy)).unwrap();
    // The snappy preamble (the varint length) no longer matches.
    b[25] ^= 0x07;
    assert!(decode(&b).is_err());
    // A compressor id nobody knows.
    let mut b = encode(1, 0, &f, Some(Compressor::Zstd)).unwrap();
    b[24] = 9;
    assert!(matches!(decode(&b), Err(CodecError::Compression(_))));
}

#[test]
fn op_query_hello() {
    // The legacy handshake: OP_QUERY on admin.$cmd.
    let q = Frame::Query(OpQuery {
        flags: 0,
        full_collection_name: "admin.$cmd".into(),
        number_to_skip: 0,
        number_to_return: -1,
        query: rawdoc! {"isMaster": 1, "helloOk": true},
    });
    let b = encode(5, 0, &q, None).unwrap();
    assert_eq!(&b[16..20], &[0, 0, 0, 0]);
    assert_eq!(&b[20..31], b"admin.$cmd\0");
    let (_, f, _) = decode(&b).unwrap();
    assert_eq!(f, q);
    let r = Frame::Reply(OpReply {
        flags: 8,
        cursor_id: 0,
        starting_from: 0,
        docs: vec![rawdoc! {"ok": 1.0}],
    });
    let b = encode(6, 5, &r, None).unwrap();
    // header 16 + flags 4 + cursor 8 + start 4 + count 4
    assert_eq!(i32::from_le_bytes([b[32], b[33], b[34], b[35]]), 1);
    assert_eq!(decode(&b).unwrap().1, r);
}

#[test]
fn refusals() {
    let mut b = ping_bytes();
    b[16] = 0x04; // unknown required flag bit 2
    assert_eq!(decode(&b).unwrap_err(), CodecError::Flags(4));
    let mut b = ping_bytes();
    b[18] = 0x01; // bit 16 exhaustAllowed: optional, accepted
    assert!(decode(&b).is_ok());
    let mut b = ping_bytes();
    b[12..16].copy_from_slice(&2002i32.to_le_bytes()); // OP_INSERT
    assert_eq!(decode(&b).unwrap_err(), CodecError::OpCode(2002));
    let mut b = ping_bytes();
    b[0..4].copy_from_slice(&15i32.to_le_bytes());
    assert!(matches!(check_length(&header(&b[..16].try_into().unwrap())), Err(CodecError::Length(15))));
    let mut b = ping_bytes();
    b[0..4].copy_from_slice(&48_000_001i32.to_le_bytes());
    assert!(check_length(&header(&b[..16].try_into().unwrap())).is_err());
    // two kind 0 sections
    let mut b = ping_bytes();
    let body = b[21..].to_vec();
    b.push(0);
    b.extend_from_slice(&body);
    let len = i32::try_from(b.len()).unwrap();
    b[0..4].copy_from_slice(&len.to_le_bytes());
    assert!(matches!(decode(&b), Err(CodecError::Malformed(_))));
}

#[tokio::test]
async fn read_message_from_stream() {
    let mut b = ping_bytes();
    b.extend_from_slice(&ping_bytes());
    let mut r = &b[..];
    assert_eq!(read_message(&mut r).await.unwrap().unwrap(), ping_bytes());
    assert_eq!(read_message(&mut r).await.unwrap().unwrap(), ping_bytes());
    assert_eq!(read_message(&mut r).await.unwrap(), None);
    let half = &ping_bytes()[..30];
    let mut r = half;
    assert_eq!(read_message(&mut r).await.unwrap_err(), CodecError::Eof);
    let _ = doc! {};
}
