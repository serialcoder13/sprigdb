//! Fuzz-style decoder tests (chapter 45 build step 8): random bytes,
//! truncations and bit flips must never panic or allocate beyond the
//! input, and whatever decodes must re-encode to something that decodes to
//! the same message.

#![allow(
    clippy::many_single_char_names,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown
)]

use proptest::prelude::*;
use sprig_proto_kafka::messages::*;
use sprig_proto_kafka::protocol::RequestHeader;
use sprig_proto_kafka::records::RecordBatch;
use sprig_proto_kafka::wire::{Message, Reader};

fn stable<M: Message + PartialEq + std::fmt::Debug>(bytes: &[u8], v: i16) {
    if let Ok(m) = M::from_bytes(bytes, v) {
        let again = m.to_bytes(v).expect("a decoded message re-encodes");
        let m2 = M::from_bytes(&again, v).expect("a re-encoded message decodes");
        assert_eq!(m, m2);
    }
}

macro_rules! all_messages {
    ($bytes:expr, $v:expr) => {{
        let (b, v) = ($bytes, $v);
        stable::<ApiVersionsRequest>(b, v.min(4));
        stable::<ApiVersionsResponse>(b, v.min(4));
        stable::<MetadataRequest>(b, v.min(12));
        stable::<MetadataResponse>(b, v.min(12));
        stable::<FindCoordinatorRequest>(b, v.min(4));
        stable::<FindCoordinatorResponse>(b, v.min(4));
        stable::<JoinGroupRequest>(b, v.min(9));
        stable::<JoinGroupResponse>(b, v.min(9));
        stable::<SyncGroupRequest>(b, v.min(5));
        stable::<SyncGroupResponse>(b, v.min(5));
        stable::<HeartbeatRequest>(b, v.min(4));
        stable::<HeartbeatResponse>(b, v.min(4));
        stable::<LeaveGroupRequest>(b, v.min(5));
        stable::<LeaveGroupResponse>(b, v.min(5));
        stable::<OffsetCommitRequest>(b, v.min(8));
        stable::<OffsetCommitResponse>(b, v.min(8));
        stable::<OffsetFetchRequest>(b, v.min(8));
        stable::<OffsetFetchResponse>(b, v.min(8));
        stable::<DescribeGroupsRequest>(b, v.min(5));
        stable::<DescribeGroupsResponse>(b, v.min(5));
        stable::<ListGroupsRequest>(b, v.min(5));
        stable::<ListGroupsResponse>(b, v.min(5));
        stable::<DeleteGroupsRequest>(b, v.min(2));
        stable::<DeleteGroupsResponse>(b, v.min(2));
        stable::<SaslHandshakeRequest>(b, v.min(1));
        stable::<SaslHandshakeResponse>(b, v.min(1));
        stable::<SaslAuthenticateRequest>(b, v.min(2));
        stable::<SaslAuthenticateResponse>(b, v.min(2));
        stable::<InitProducerIdRequest>(b, v.min(4));
        stable::<InitProducerIdResponse>(b, v.min(4));
        stable::<CreateTopicsRequest>(b, v.min(7));
        stable::<CreateTopicsResponse>(b, v.min(7));
        stable::<DeleteTopicsRequest>(b, v.min(6));
        stable::<DeleteTopicsResponse>(b, v.min(6));
        stable::<CreatePartitionsRequest>(b, v.min(3));
        stable::<CreatePartitionsResponse>(b, v.min(3));
        stable::<DescribeConfigsRequest>(b, v.min(4));
        stable::<DescribeConfigsResponse>(b, v.min(4));
        stable::<IncrementalAlterConfigsRequest>(b, v.min(1));
        stable::<IncrementalAlterConfigsResponse>(b, v.min(1));
        let _ = RequestHeader::decode(&mut Reader::new(b));
        let _ = RecordBatch::decode_all(b, 1 << 20, true);
    }};
}

/// A valid flexible JoinGroup v9 request, to mutate.
fn seed() -> Vec<u8> {
    JoinGroupRequest {
        group_id: "billing".into(),
        session_timeout_ms: 45_000,
        rebalance_timeout_ms: 300_000,
        member_id: "m-1".into(),
        group_instance_id: Some("i".into()),
        protocol_type: "consumer".into(),
        protocols: vec![join_group_request::JoinGroupRequestProtocol {
            name: "range".into(),
            metadata: vec![0, 3, 0, 0, 0, 1, 0, 1, b'x'],
            ..Default::default()
        }],
        reason: Some("r".into()),
        ..Default::default()
    }
    .to_bytes(9)
    .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    #[test]
    fn random_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..256), v in 0i16..13) {
        all_messages!(&bytes, v);
    }

    #[test]
    fn mutated_messages_never_panic(flips in proptest::collection::vec((0usize..64, any::<u8>()), 1..4), cut in 0usize..64) {
        let mut b = seed();
        for (i, x) in flips {
            let n = b.len();
            b[i % n] ^= x;
        }
        all_messages!(&b, 9);
        let n = b.len();
        all_messages!(&b[..cut.min(n)], 9);
    }
}

#[test]
fn huge_claimed_lengths_fail_fast() {
    // A compact array claiming 2^32-2 elements, a classic one claiming
    // 2^31-1, and a string claiming 32767 bytes: all refused at once.
    for (bytes, v) in [
        (vec![0xff, 0xff, 0xff, 0xff, 0x0f], 9),
        (vec![0x7f, 0xff, 0xff, 0xff], 0),
        (vec![0x7f, 0xff, 0x00], 0),
    ] {
        assert!(MetadataRequest::from_bytes(&bytes, v).is_err());
        assert!(DescribeGroupsRequest::from_bytes(&bytes, v.min(5)).is_err());
    }
}
