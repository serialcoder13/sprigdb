use std::io::Write as _;

use bytes::{BufMut as _, Bytes, BytesMut};
use proptest::prelude::*;
use tokio_util::codec::{Decoder as _, Encoder as _};

use super::*;

fn pattern(n: usize) -> Bytes {
    (0..n)
        .map(|i| u8::try_from((i * 7 + i / 251) % 256).unwrap_or(0))
        .collect::<Vec<_>>()
        .into()
}

/// Encodes `payloads` with one codec and decodes them with another,
/// feeding the wire bytes in `step`-sized pieces.
fn round_trip(framing: Framing, payloads: &[Bytes], step: usize) -> Vec<Packet> {
    let mut tx = PacketCodec::new(usize::MAX);
    tx.set_framing(framing);
    let mut wire = BytesMut::new();
    for p in payloads {
        tx.encode(p.clone(), &mut wire).expect("encode");
    }
    let mut rx = PacketCodec::new(usize::MAX);
    rx.set_framing(framing);
    let mut buf = BytesMut::new();
    let mut out = Vec::new();
    for piece in wire.chunks(step.max(1)) {
        buf.extend_from_slice(piece);
        while let Some(p) = rx.decode(&mut buf).expect("decode") {
            out.push(p);
        }
    }
    assert!(buf.is_empty(), "{} bytes left over", buf.len());
    assert_eq!(rx.next_seq(), tx.next_seq());
    out
}

const SIZES: [usize; 6] = [0, 1, 0xFF_FFFE, 0xFF_FFFF, 0x100_0000, 2 * 0xFF_FFFF];

#[test]
fn boundary_sizes_round_trip_plain() {
    let payloads: Vec<Bytes> = SIZES.iter().map(|&n| pattern(n)).collect();
    let got = round_trip(Framing::Plain, &payloads, 1 << 20);
    assert_eq!(got.len(), payloads.len());
    for (g, p) in got.iter().zip(&payloads) {
        assert_eq!(g.payload, *p);
    }
}

#[test]
fn boundary_sizes_round_trip_compressed() {
    for framing in [Framing::Zlib { level: 1 }, Framing::Zstd { level: 1 }] {
        let payloads: Vec<Bytes> = SIZES.iter().map(|&n| pattern(n)).collect();
        let got = round_trip(framing, &payloads, 1 << 20);
        assert_eq!(got.len(), payloads.len());
        for (g, p) in got.iter().zip(&payloads) {
            assert_eq!(g.payload.len(), p.len());
            assert!(
                g.payload == *p,
                "{framing:?} payload of {} differs",
                p.len()
            );
        }
    }
}

#[test]
fn exact_chunk_sends_a_trailing_empty_chunk() {
    let mut c = PacketCodec::default();
    let mut wire = BytesMut::new();
    c.encode(pattern(MAX_CHUNK), &mut wire).expect("encode");
    assert_eq!(wire.len(), MAX_CHUNK + 8);
    assert_eq!(&wire[..4], &[0xFF, 0xFF, 0xFF, 0]);
    assert_eq!(&wire[MAX_CHUNK + 4..], &[0, 0, 0, 1]);
    assert_eq!(c.next_seq(), 2);

    let mut c = PacketCodec::default();
    let mut wire = BytesMut::new();
    c.encode(Bytes::new(), &mut wire).expect("encode");
    assert_eq!(&wire[..], &[0, 0, 0, 0]);
}

#[test]
fn sequence_ids_are_shared_and_checked() {
    let mut c = PacketCodec::default();
    let mut wire = BytesMut::new();
    c.encode(Packet::new(0, &b"\x03select 1"[..]), &mut wire)
        .expect("encode");
    assert_eq!(c.next_seq(), 1);
    // A response with sequence 1 is accepted, then 2 is expected.
    let mut src = BytesMut::from(&[1, 0, 0, 1, 0x00][..]);
    let p = c.decode(&mut src).expect("decode").expect("packet");
    assert_eq!(p.seq, 1);
    assert_eq!(c.next_seq(), 2);
    // Sequence 5 instead of 2 is out of order.
    let mut src = BytesMut::from(&[1, 0, 0, 5, 0x00][..]);
    assert!(matches!(
        c.decode(&mut src),
        Err(ProtoError::OutOfOrder {
            expected: 2,
            got: 5
        })
    ));
    c.reset_seq();
    assert_eq!(c.next_seq(), 0);
}

#[test]
fn sequence_wraps_at_255() {
    let mut c = PacketCodec::default();
    c.set_next_seq(255);
    let mut wire = BytesMut::new();
    c.encode(Bytes::from_static(b"x"), &mut wire)
        .expect("encode");
    assert_eq!(wire[3], 255);
    assert_eq!(c.next_seq(), 0);
}

#[test]
fn oversized_packets_fail_before_buffering() {
    let mut c = PacketCodec::new(10);
    // Header announces 11 bytes; only the header has arrived.
    let mut src = BytesMut::from(&[11, 0, 0, 0][..]);
    assert!(matches!(
        c.decode(&mut src),
        Err(ProtoError::PacketTooLarge { size: 11, max: 10 })
    ));
    // Split packets are charged as they accumulate.
    let mut c = PacketCodec::new(MAX_CHUNK + 5);
    let mut tx = PacketCodec::default();
    let mut wire = BytesMut::new();
    tx.encode(pattern(MAX_CHUNK + 6), &mut wire)
        .expect("encode");
    let mut res = c.decode(&mut wire);
    while let Ok(None) = res {
        res = c.decode(&mut wire);
        if wire.is_empty() {
            break;
        }
    }
    assert!(matches!(res, Err(ProtoError::PacketTooLarge { .. })));
}

#[test]
fn small_frames_go_uncompressed() {
    let mut c = PacketCodec::default();
    c.set_framing(Framing::Zlib { level: 6 });
    let mut wire = BytesMut::new();
    c.encode(Bytes::from_static(b"\x0e"), &mut wire)
        .expect("encode");
    // 7-byte compressed header with uncompressed length 0, then the packet.
    assert_eq!(&wire[..], &[5, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0x0e]);
    assert_eq!(c.compressed_seq(), 1);
    assert_eq!(c.next_seq(), 1);
}

#[test]
fn large_frames_compress() {
    let mut c = PacketCodec::default();
    c.set_framing(Framing::Zstd { level: 3 });
    let mut wire = BytesMut::new();
    c.encode(Bytes::from(vec![b'a'; 1000]), &mut wire)
        .expect("encode");
    assert!(wire.len() < 100);
    assert_eq!(&wire[4..7], &[0xEC, 0x03, 0]); // 1004 bytes uncompressed
}

#[test]
fn framing_switches_on_a_live_codec() {
    let mut tx = PacketCodec::default();
    let mut rx = PacketCodec::default();
    let mut wire = BytesMut::new();
    tx.encode(
        Bytes::from_static(b"\x00\x00\x00\x02\x00\x00\x00"),
        &mut wire,
    )
    .expect("encode");
    tx.reset_seq();
    tx.set_framing(Framing::Zlib { level: 6 });
    tx.encode(Bytes::from(vec![b'q'; 200]), &mut wire)
        .expect("encode");
    let first = rx.decode(&mut wire).expect("decode").expect("ok packet");
    assert_eq!(first.payload.len(), 7);
    rx.reset_seq();
    rx.set_framing(Framing::Zlib { level: 6 });
    let second = rx.decode(&mut wire).expect("decode").expect("packet");
    assert_eq!(second.payload, Bytes::from(vec![b'q'; 200]));
}

#[test]
fn compressed_frames_must_inflate_to_their_declared_length() {
    let mut z = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    z.write_all(&[0u8; 5000]).expect("zlib");
    let data = z.finish().expect("zlib");
    for declared in [100usize, 6000] {
        let mut src = BytesMut::new();
        src.put_slice(&u24_le(data.len()));
        src.put_u8(0);
        src.put_slice(&u24_le(declared));
        src.put_slice(&data);
        let mut c = PacketCodec::default();
        c.set_framing(Framing::Zlib { level: 6 });
        assert!(matches!(
            c.decode(&mut src),
            Err(ProtoError::Compression(_))
        ));
    }
}

#[test]
fn compressed_sequence_is_checked() {
    let mut c = PacketCodec::default();
    c.set_framing(Framing::Zlib { level: 6 });
    let mut src = BytesMut::from(&[5, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 0x0e][..]);
    assert!(matches!(
        c.decode(&mut src),
        Err(ProtoError::OutOfOrder {
            expected: 0,
            got: 3
        })
    ));
}

fn framing_strategy() -> impl Strategy<Value = Framing> {
    prop_oneof![
        Just(Framing::Plain),
        (0u32..=9).prop_map(|level| Framing::Zlib { level }),
        (1i32..=9).prop_map(|level| Framing::Zstd { level }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn prop_round_trip(
        framing in framing_strategy(),
        payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..300), 1..8),
        repeat in 0usize..4,
        step in 1usize..64,
    ) {
        // Mix random bytes with compressible runs so both frame kinds occur.
        let payloads: Vec<Bytes> = payloads
            .into_iter()
            .map(|mut p| {
                let fill = p.first().copied().unwrap_or(0);
                p.extend(std::iter::repeat_n(fill, repeat * 40));
                Bytes::from(p)
            })
            .collect();
        let got = round_trip(framing, &payloads, step);
        prop_assert_eq!(got.len(), payloads.len());
        for (g, p) in got.iter().zip(&payloads) {
            prop_assert_eq!(&g.payload, p);
        }
    }

}

#[test]
fn sizes_around_chunk_multiples_round_trip() {
    for n in 0..3 {
        for extra in 0..3 {
            let size = MAX_CHUNK * n + extra;
            let got = round_trip(Framing::Plain, &[pattern(size)], 1 << 22);
            assert_eq!(got.len(), 1);
            assert_eq!(got[0].payload, pattern(size));
        }
    }
}
