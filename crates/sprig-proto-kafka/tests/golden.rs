//! Golden bytes for every served API at every served version (chapter 45
//! build step 8).
//!
//! Each case builds a message with a non-default value in every field that
//! exists at that version, and writes the bytes the protocol guide and
//! Kafka 3.9.0's message definitions give for it, by hand with [`B`], a
//! byte builder independent of the codec. The codec must encode the message
//! to exactly those bytes and decode them back to the same message. The
//! generated code and these cases were written from the definitions
//! separately, so a mistake in either shows up here.

#![allow(
    clippy::many_single_char_names,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::field_reassign_with_default
)]

use sprig_proto_kafka::messages::*;
use sprig_proto_kafka::protocol::{ApiRange, DATA_PLANE, SERVED};
use sprig_proto_kafka::wire::Message;
use uuid::Uuid;

/// A hand byte builder: classic encodings, or compact ones in flexible
/// versions.
struct B {
    out: Vec<u8>,
    flex: bool,
}

impl B {
    fn new(flex: bool) -> Self {
        Self {
            out: Vec::new(),
            flex,
        }
    }
    fn uv(&mut self, mut v: u32) {
        while v >= 0x80 {
            self.out.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.out.push(v as u8);
    }
    fn i8(&mut self, v: i8) {
        self.out.extend_from_slice(&v.to_be_bytes());
    }
    fn i16(&mut self, v: i16) {
        self.out.extend_from_slice(&v.to_be_bytes());
    }
    fn i32(&mut self, v: i32) {
        self.out.extend_from_slice(&v.to_be_bytes());
    }
    fn i64(&mut self, v: i64) {
        self.out.extend_from_slice(&v.to_be_bytes());
    }
    fn bool(&mut self, v: bool) {
        self.out.push(u8::from(v));
    }
    fn uuid(&mut self, u: Uuid) {
        self.out.extend_from_slice(u.as_bytes());
    }
    fn str(&mut self, s: &str) {
        if self.flex {
            self.uv(s.len() as u32 + 1);
        } else {
            self.i16(s.len() as i16);
        }
        self.out.extend_from_slice(s.as_bytes());
    }
    fn null_str(&mut self) {
        if self.flex {
            self.uv(0);
        } else {
            self.i16(-1);
        }
    }
    fn bytes(&mut self, b: &[u8]) {
        if self.flex {
            self.uv(b.len() as u32 + 1);
        } else {
            self.i32(b.len() as i32);
        }
        self.out.extend_from_slice(b);
    }
    fn arr(&mut self, n: usize) {
        if self.flex {
            self.uv(n as u32 + 1);
        } else {
            self.i32(n as i32);
        }
    }
    fn null_arr(&mut self) {
        if self.flex {
            self.uv(0);
        } else {
            self.i32(-1);
        }
    }
    /// An empty tagged-field section, in flexible versions only.
    fn tags(&mut self) {
        if self.flex {
            self.uv(0);
        }
    }
}

const U: Uuid = Uuid::from_u128(0x0192_f3a4_5b6c_7d8e_9fa0_b1c2_d3e4_f506);

fn check<M: Message + PartialEq + std::fmt::Debug>(name: &str, v: i16, m: &M, want: &[u8]) {
    let got = m
        .to_bytes(v)
        .unwrap_or_else(|e| panic!("{name} v{v}: encode: {e}"));
    assert_eq!(got, want, "{name} v{v}: encoded bytes differ");
    let back = M::from_bytes(want, v).unwrap_or_else(|e| panic!("{name} v{v}: decode: {e}"));
    assert_eq!(&back, m, "{name} v{v}: decoded message differs");
}

fn range(key: i16) -> &'static ApiRange {
    SERVED
        .iter()
        .chain(DATA_PLANE)
        .find(|r| r.key == key)
        .unwrap()
}

fn versions(key: i16) -> std::ops::RangeInclusive<i16> {
    let r = range(key);
    r.min..=r.max
}

fn flex(key: i16, v: i16) -> bool {
    v >= range(key).flexible_min
}

#[test]
fn api_versions() {
    for v in versions(18) {
        let mut b = B::new(flex(18, v));
        let mut q = ApiVersionsRequest::default();
        if v >= 3 {
            q.client_software_name = "sprig-test".into();
            q.client_software_version = "0.1".into();
            b.str("sprig-test");
            b.str("0.1");
        }
        b.tags();
        check("ApiVersionsRequest", v, &q, &b.out);

        let mut b = B::new(flex(18, v));
        let mut p = ApiVersionsResponse {
            error_code: 0,
            ..Default::default()
        };
        p.api_keys = vec![api_versions_response::ApiVersion {
            api_key: 3,
            min_version: 0,
            max_version: 12,
            ..Default::default()
        }];
        b.i16(0);
        b.arr(1);
        b.i16(3);
        b.i16(0);
        b.i16(12);
        b.tags();
        if v >= 1 {
            p.throttle_time_ms = 5;
            b.i32(5);
        }
        if v >= 3 {
            // A tagged field: FinalizedFeaturesEpoch (tag 1) = 7.
            p.finalized_features_epoch = 7;
            b.uv(1);
            b.uv(1);
            b.uv(8);
            b.i64(7);
        }
        check("ApiVersionsResponse", v, &p, &b.out);
    }
}

#[test]
fn metadata() {
    for v in versions(3) {
        let f = flex(3, v);
        let mut b = B::new(f);
        let mut q = MetadataRequest::default();
        let mut t = metadata_request::MetadataRequestTopic {
            name: Some("shop.orders".into()),
            ..Default::default()
        };
        b.arr(1);
        if v >= 10 {
            t.topic_id = U;
            b.uuid(U);
        }
        b.str("shop.orders");
        b.tags();
        q.topics = Some(vec![t]);
        if v >= 4 {
            q.allow_auto_topic_creation = false;
            b.bool(false);
        }
        if (8..=10).contains(&v) {
            q.include_cluster_authorized_operations = true;
            b.bool(true);
        }
        if v >= 8 {
            q.include_topic_authorized_operations = true;
            b.bool(true);
        }
        b.tags();
        check("MetadataRequest", v, &q, &b.out);
        if v >= 1 {
            // Null topics: all topics.
            let mut b = B::new(f);
            let mut q = MetadataRequest {
                topics: None,
                ..Default::default()
            };
            b.null_arr();
            if v >= 4 {
                q.allow_auto_topic_creation = true;
                b.bool(true);
            }
            if (8..=10).contains(&v) {
                b.bool(false);
            }
            if v >= 8 {
                b.bool(false);
            }
            b.tags();
            check("MetadataRequest(all)", v, &q, &b.out);
        }

        let mut b = B::new(f);
        let mut p = MetadataResponse::default();
        if v >= 3 {
            p.throttle_time_ms = 1;
            b.i32(1);
        }
        let mut br = metadata_response::MetadataResponseBroker {
            node_id: 2,
            host: "gw2.example".into(),
            port: 9092,
            ..Default::default()
        };
        b.arr(1);
        b.i32(2);
        b.str("gw2.example");
        b.i32(9092);
        if v >= 1 {
            br.rack = Some("dc-a".into());
            b.str("dc-a");
        }
        b.tags();
        p.brokers = vec![br];
        if v >= 2 {
            p.cluster_id = Some("sprig-dc-a".into());
            b.str("sprig-dc-a");
        }
        if v >= 1 {
            p.controller_id = 2;
            b.i32(2);
        }
        let mut t = metadata_response::MetadataResponseTopic {
            error_code: 0,
            name: Some("shop.orders".into()),
            ..Default::default()
        };
        b.arr(1);
        b.i16(0);
        b.str("shop.orders");
        if v >= 10 {
            t.topic_id = U;
            b.uuid(U);
        }
        if v >= 1 {
            t.is_internal = false;
            b.bool(false);
        }
        let mut part = metadata_response::MetadataResponsePartition {
            error_code: 0,
            partition_index: 5,
            leader_id: 2,
            replica_nodes: vec![2],
            isr_nodes: vec![2],
            ..Default::default()
        };
        b.arr(1);
        b.i16(0);
        b.i32(5);
        b.i32(2);
        if v >= 7 {
            part.leader_epoch = 3;
            b.i32(3);
        }
        b.arr(1);
        b.i32(2);
        b.arr(1);
        b.i32(2);
        if v >= 5 {
            b.arr(0);
        }
        b.tags();
        t.partitions = vec![part];
        if v >= 8 {
            t.topic_authorized_operations = 0x0f8;
            b.i32(0x0f8);
        }
        b.tags();
        p.topics = vec![t];
        if (8..=10).contains(&v) {
            p.cluster_authorized_operations = -2_147_483_648;
            b.i32(-2_147_483_648);
        }
        b.tags();
        check("MetadataResponse", v, &p, &b.out);
    }
}

#[test]
fn find_coordinator() {
    for v in versions(10) {
        let mut b = B::new(flex(10, v));
        let mut q = FindCoordinatorRequest::default();
        if v <= 3 {
            q.key = "billing".into();
            b.str("billing");
        }
        if v >= 1 {
            q.key_type = 0;
            b.i8(0);
        }
        if v >= 4 {
            q.coordinator_keys = vec!["billing".into(), "audit".into()];
            b.arr(2);
            b.str("billing");
            b.str("audit");
        }
        b.tags();
        check("FindCoordinatorRequest", v, &q, &b.out);

        let mut b = B::new(flex(10, v));
        let mut p = FindCoordinatorResponse::default();
        if v >= 1 {
            p.throttle_time_ms = 0;
            b.i32(0);
        }
        if v <= 3 {
            p.error_code = 0;
            b.i16(0);
            if v >= 1 {
                p.error_message = None;
                b.null_str();
            }
            p.node_id = 4;
            p.host = "gw4".into();
            p.port = 9092;
            b.i32(4);
            b.str("gw4");
            b.i32(9092);
        }
        if v >= 4 {
            p.coordinators = vec![find_coordinator_response::Coordinator {
                key: "billing".into(),
                node_id: 4,
                host: "gw4".into(),
                port: 9092,
                error_code: 16,
                error_message: Some("39954".into()),
                ..Default::default()
            }];
            b.arr(1);
            b.str("billing");
            b.i32(4);
            b.str("gw4");
            b.i32(9092);
            b.i16(16);
            b.str("39954");
            b.tags();
        }
        b.tags();
        check("FindCoordinatorResponse", v, &p, &b.out);
    }
}

#[test]
fn join_group() {
    for v in versions(11) {
        let mut b = B::new(flex(11, v));
        let mut q = JoinGroupRequest {
            group_id: "billing".into(),
            session_timeout_ms: 45_000,
            member_id: String::new(),
            protocol_type: "consumer".into(),
            ..Default::default()
        };
        b.str("billing");
        b.i32(45_000);
        if v >= 1 {
            q.rebalance_timeout_ms = 300_000;
            b.i32(300_000);
        }
        b.str("");
        if v >= 5 {
            q.group_instance_id = Some("c1".into());
            b.str("c1");
        }
        b.str("consumer");
        q.protocols = vec![join_group_request::JoinGroupRequestProtocol {
            name: "range".into(),
            metadata: vec![0, 1, 2],
            ..Default::default()
        }];
        b.arr(1);
        b.str("range");
        b.bytes(&[0, 1, 2]);
        b.tags();
        if v >= 8 {
            q.reason = Some("rejoin".into());
            b.str("rejoin");
        }
        b.tags();
        check("JoinGroupRequest", v, &q, &b.out);

        let mut b = B::new(flex(11, v));
        let mut p = JoinGroupResponse::default();
        if v >= 2 {
            p.throttle_time_ms = 0;
            b.i32(0);
        }
        p.error_code = 0;
        p.generation_id = 3;
        b.i16(0);
        b.i32(3);
        if v >= 7 {
            p.protocol_type = Some("consumer".into());
            b.str("consumer");
        }
        p.protocol_name = Some("range".into());
        b.str("range");
        p.leader = "m1".into();
        b.str("m1");
        if v >= 9 {
            p.skip_assignment = false;
            b.bool(false);
        }
        p.member_id = "m1".into();
        b.str("m1");
        let mut mem = join_group_response::JoinGroupResponseMember {
            member_id: "m1".into(),
            metadata: vec![9],
            ..Default::default()
        };
        b.arr(1);
        b.str("m1");
        if v >= 5 {
            mem.group_instance_id = None;
            b.null_str();
        }
        b.bytes(&[9]);
        b.tags();
        p.members = vec![mem];
        b.tags();
        check("JoinGroupResponse", v, &p, &b.out);
    }
}

#[test]
fn sync_group() {
    for v in versions(14) {
        let mut b = B::new(flex(14, v));
        let mut q = SyncGroupRequest {
            group_id: "billing".into(),
            generation_id: 3,
            member_id: "m1".into(),
            ..Default::default()
        };
        b.str("billing");
        b.i32(3);
        b.str("m1");
        if v >= 3 {
            b.null_str();
        }
        if v >= 5 {
            q.protocol_type = Some("consumer".into());
            q.protocol_name = Some("range".into());
            b.str("consumer");
            b.str("range");
        }
        q.assignments = vec![sync_group_request::SyncGroupRequestAssignment {
            member_id: "m1".into(),
            assignment: vec![1, 2],
            ..Default::default()
        }];
        b.arr(1);
        b.str("m1");
        b.bytes(&[1, 2]);
        b.tags();
        b.tags();
        check("SyncGroupRequest", v, &q, &b.out);

        let mut b = B::new(flex(14, v));
        let mut p = SyncGroupResponse::default();
        if v >= 1 {
            p.throttle_time_ms = 0;
            b.i32(0);
        }
        p.error_code = 27;
        b.i16(27);
        if v >= 5 {
            p.protocol_type = Some("consumer".into());
            b.str("consumer");
            b.null_str();
        }
        p.assignment = vec![1, 2];
        b.bytes(&[1, 2]);
        b.tags();
        check("SyncGroupResponse", v, &p, &b.out);
    }
}

#[test]
fn heartbeat_and_leave_group() {
    for v in versions(12) {
        let mut b = B::new(flex(12, v));
        let mut q = HeartbeatRequest {
            group_id: "billing".into(),
            generation_id: 3,
            member_id: "m1".into(),
            ..Default::default()
        };
        b.str("billing");
        b.i32(3);
        b.str("m1");
        if v >= 3 {
            q.group_instance_id = Some("c1".into());
            b.str("c1");
        }
        b.tags();
        check("HeartbeatRequest", v, &q, &b.out);
        let mut b = B::new(flex(12, v));
        let mut p = HeartbeatResponse::default();
        if v >= 1 {
            p.throttle_time_ms = 3;
            b.i32(3);
        }
        p.error_code = 27;
        b.i16(27);
        b.tags();
        check("HeartbeatResponse", v, &p, &b.out);
    }
    for v in versions(13) {
        let mut b = B::new(flex(13, v));
        let mut q = LeaveGroupRequest {
            group_id: "billing".into(),
            ..Default::default()
        };
        b.str("billing");
        if v <= 2 {
            q.member_id = "m1".into();
            b.str("m1");
        } else {
            let mut m = leave_group_request::MemberIdentity {
                member_id: "m1".into(),
                group_instance_id: Some("c1".into()),
                ..Default::default()
            };
            b.arr(1);
            b.str("m1");
            b.str("c1");
            if v >= 5 {
                m.reason = Some("bye".into());
                b.str("bye");
            }
            b.tags();
            q.members = vec![m];
        }
        b.tags();
        check("LeaveGroupRequest", v, &q, &b.out);
        let mut b = B::new(flex(13, v));
        let mut p = LeaveGroupResponse::default();
        if v >= 1 {
            b.i32(0);
        }
        b.i16(0);
        if v >= 3 {
            p.members = vec![leave_group_response::MemberResponse {
                member_id: "m1".into(),
                group_instance_id: None,
                error_code: 25,
                ..Default::default()
            }];
            b.arr(1);
            b.str("m1");
            b.null_str();
            b.i16(25);
            b.tags();
        }
        b.tags();
        check("LeaveGroupResponse", v, &p, &b.out);
    }
}

#[test]
fn offset_commit() {
    for v in versions(8) {
        let mut b = B::new(flex(8, v));
        let mut q = OffsetCommitRequest {
            group_id: "billing".into(),
            ..Default::default()
        };
        b.str("billing");
        if v >= 1 {
            q.generation_id_or_member_epoch = 3;
            q.member_id = "m1".into();
            b.i32(3);
            b.str("m1");
        }
        if v >= 7 {
            b.null_str();
        }
        if (2..=4).contains(&v) {
            q.retention_time_ms = 86_400_000;
            b.i64(86_400_000);
        }
        let mut part = offset_commit_request::OffsetCommitRequestPartition {
            partition_index: 5,
            committed_offset: 1206,
            committed_metadata: Some("m".into()),
            ..Default::default()
        };
        b.arr(1);
        b.str("shop.order_events");
        b.arr(1);
        b.i32(5);
        b.i64(1206);
        if v >= 6 {
            part.committed_leader_epoch = 3;
            b.i32(3);
        }
        if v == 1 {
            part.commit_timestamp = 1_762_508_100_123;
            b.i64(1_762_508_100_123);
        }
        b.str("m");
        b.tags();
        b.tags();
        q.topics = vec![offset_commit_request::OffsetCommitRequestTopic {
            name: "shop.order_events".into(),
            partitions: vec![part],
            ..Default::default()
        }];
        b.tags();
        check("OffsetCommitRequest", v, &q, &b.out);

        let mut b = B::new(flex(8, v));
        let mut p = OffsetCommitResponse::default();
        if v >= 3 {
            b.i32(0);
        }
        p.topics = vec![offset_commit_response::OffsetCommitResponseTopic {
            name: "shop.order_events".into(),
            partitions: vec![offset_commit_response::OffsetCommitResponsePartition {
                partition_index: 5,
                error_code: 22,
                ..Default::default()
            }],
            ..Default::default()
        }];
        b.arr(1);
        b.str("shop.order_events");
        b.arr(1);
        b.i32(5);
        b.i16(22);
        b.tags();
        b.tags();
        b.tags();
        check("OffsetCommitResponse", v, &p, &b.out);
    }
}

#[test]
fn offset_fetch() {
    for v in versions(9) {
        let mut b = B::new(flex(9, v));
        let mut q = OffsetFetchRequest::default();
        if v <= 7 {
            q.group_id = "billing".into();
            b.str("billing");
            if v >= 2 {
                q.topics = None;
                b.null_arr();
            } else {
                q.topics = Some(vec![offset_fetch_request::OffsetFetchRequestTopic {
                    name: "shop.e".into(),
                    partition_indexes: vec![0, 1],
                    ..Default::default()
                }]);
                b.arr(1);
                b.str("shop.e");
                b.arr(2);
                b.i32(0);
                b.i32(1);
                b.tags();
            }
        } else {
            q.groups = vec![offset_fetch_request::OffsetFetchRequestGroup {
                group_id: "billing".into(),
                topics: Some(vec![offset_fetch_request::OffsetFetchRequestTopics {
                    name: "shop.e".into(),
                    partition_indexes: vec![3],
                    ..Default::default()
                }]),
                ..Default::default()
            }];
            b.arr(1);
            b.str("billing");
            b.arr(1);
            b.str("shop.e");
            b.arr(1);
            b.i32(3);
            b.tags();
            b.tags();
        }
        if v >= 7 {
            q.require_stable = true;
            b.bool(true);
        }
        b.tags();
        check("OffsetFetchRequest", v, &q, &b.out);

        let mut b = B::new(flex(9, v));
        let mut p = OffsetFetchResponse::default();
        if v >= 3 {
            b.i32(0);
        }
        if v <= 7 {
            let mut part = offset_fetch_response::OffsetFetchResponsePartition {
                partition_index: 3,
                committed_offset: 1206,
                metadata: Some(String::new()),
                ..Default::default()
            };
            b.arr(1);
            b.str("shop.e");
            b.arr(1);
            b.i32(3);
            b.i64(1206);
            if v >= 5 {
                part.committed_leader_epoch = 3;
                b.i32(3);
            }
            b.str("");
            b.i16(0);
            b.tags();
            b.tags();
            p.topics = vec![offset_fetch_response::OffsetFetchResponseTopic {
                name: "shop.e".into(),
                partitions: vec![part],
                ..Default::default()
            }];
            if v >= 2 {
                p.error_code = 0;
                b.i16(0);
            }
        } else {
            p.groups = vec![offset_fetch_response::OffsetFetchResponseGroup {
                group_id: "billing".into(),
                topics: vec![offset_fetch_response::OffsetFetchResponseTopics {
                    name: "shop.e".into(),
                    partitions: vec![offset_fetch_response::OffsetFetchResponsePartitions {
                        partition_index: 3,
                        committed_offset: -1,
                        committed_leader_epoch: -1,
                        metadata: None,
                        error_code: 0,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                error_code: 0,
                ..Default::default()
            }];
            b.arr(1);
            b.str("billing");
            b.arr(1);
            b.str("shop.e");
            b.arr(1);
            b.i32(3);
            b.i64(-1);
            b.i32(-1);
            b.null_str();
            b.i16(0);
            b.tags();
            b.tags();
            b.i16(0);
            b.tags();
        }
        b.tags();
        check("OffsetFetchResponse", v, &p, &b.out);
    }
}

#[test]
fn describe_list_delete_groups() {
    for v in versions(15) {
        let mut b = B::new(flex(15, v));
        let mut q = DescribeGroupsRequest {
            groups: vec!["billing".into()],
            ..Default::default()
        };
        b.arr(1);
        b.str("billing");
        if v >= 3 {
            q.include_authorized_operations = true;
            b.bool(true);
        }
        b.tags();
        check("DescribeGroupsRequest", v, &q, &b.out);

        let mut b = B::new(flex(15, v));
        let mut p = DescribeGroupsResponse::default();
        if v >= 1 {
            b.i32(0);
        }
        let mut mem = describe_groups_response::DescribedGroupMember {
            member_id: "m1".into(),
            client_id: "c".into(),
            client_host: "/10.0.0.1".into(),
            member_metadata: vec![1],
            member_assignment: vec![2],
            ..Default::default()
        };
        let mut g = describe_groups_response::DescribedGroup {
            error_code: 0,
            group_id: "billing".into(),
            group_state: "Stable".into(),
            protocol_type: "consumer".into(),
            protocol_data: "range".into(),
            ..Default::default()
        };
        b.arr(1);
        b.i16(0);
        b.str("billing");
        b.str("Stable");
        b.str("consumer");
        b.str("range");
        b.arr(1);
        b.str("m1");
        if v >= 4 {
            mem.group_instance_id = Some("i1".into());
            b.str("i1");
        }
        b.str("c");
        b.str("/10.0.0.1");
        b.bytes(&[1]);
        b.bytes(&[2]);
        b.tags();
        g.members = vec![mem];
        if v >= 3 {
            g.authorized_operations = 0;
            b.i32(0);
        }
        b.tags();
        p.groups = vec![g];
        b.tags();
        check("DescribeGroupsResponse", v, &p, &b.out);
    }
    for v in versions(16) {
        let mut b = B::new(flex(16, v));
        let mut q = ListGroupsRequest::default();
        if v >= 4 {
            q.states_filter = vec!["Stable".into()];
            b.arr(1);
            b.str("Stable");
        }
        if v >= 5 {
            q.types_filter = vec!["classic".into()];
            b.arr(1);
            b.str("classic");
        }
        b.tags();
        check("ListGroupsRequest", v, &q, &b.out);
        let mut b = B::new(flex(16, v));
        let mut p = ListGroupsResponse::default();
        if v >= 1 {
            b.i32(0);
        }
        b.i16(0);
        let mut g = list_groups_response::ListedGroup {
            group_id: "billing".into(),
            protocol_type: "consumer".into(),
            ..Default::default()
        };
        b.arr(1);
        b.str("billing");
        b.str("consumer");
        if v >= 4 {
            g.group_state = "Empty".into();
            b.str("Empty");
        }
        if v >= 5 {
            g.group_type = "classic".into();
            b.str("classic");
        }
        b.tags();
        p.groups = vec![g];
        b.tags();
        check("ListGroupsResponse", v, &p, &b.out);
    }
    for v in versions(42) {
        let mut b = B::new(flex(42, v));
        let q = DeleteGroupsRequest {
            groups_names: vec!["billing".into()],
            ..Default::default()
        };
        b.arr(1);
        b.str("billing");
        b.tags();
        check("DeleteGroupsRequest", v, &q, &b.out);
        let mut b = B::new(flex(42, v));
        let p = DeleteGroupsResponse {
            throttle_time_ms: 0,
            results: vec![delete_groups_response::DeletableGroupResult {
                group_id: "billing".into(),
                error_code: 68,
                ..Default::default()
            }],
            ..Default::default()
        };
        b.i32(0);
        b.arr(1);
        b.str("billing");
        b.i16(68);
        b.tags();
        b.tags();
        check("DeleteGroupsResponse", v, &p, &b.out);
    }
}

#[test]
fn sasl_and_init_producer_id() {
    for v in versions(17) {
        let mut b = B::new(false);
        let q = SaslHandshakeRequest {
            mechanism: "SCRAM-SHA-512".into(),
            ..Default::default()
        };
        b.str("SCRAM-SHA-512");
        check("SaslHandshakeRequest", v, &q, &b.out);
        let mut b = B::new(false);
        let p = SaslHandshakeResponse {
            error_code: 0,
            mechanisms: vec!["SCRAM-SHA-512".into(), "PLAIN".into()],
            ..Default::default()
        };
        b.i16(0);
        b.arr(2);
        b.str("SCRAM-SHA-512");
        b.str("PLAIN");
        check("SaslHandshakeResponse", v, &p, &b.out);
    }
    for v in versions(36) {
        let mut b = B::new(flex(36, v));
        let q = SaslAuthenticateRequest {
            auth_bytes: b"\0acme.app\0secret".to_vec(),
            ..Default::default()
        };
        b.bytes(b"\0acme.app\0secret");
        b.tags();
        check("SaslAuthenticateRequest", v, &q, &b.out);
        let mut b = B::new(flex(36, v));
        let mut p = SaslAuthenticateResponse {
            error_code: 58,
            error_message: Some("39953".into()),
            auth_bytes: Vec::new(),
            ..Default::default()
        };
        b.i16(58);
        b.str("39953");
        b.bytes(&[]);
        if v >= 1 {
            p.session_lifetime_ms = 0;
            b.i64(0);
        }
        b.tags();
        check("SaslAuthenticateResponse", v, &p, &b.out);
    }
    for v in versions(22) {
        let mut b = B::new(flex(22, v));
        let mut q = InitProducerIdRequest {
            transactional_id: None,
            transaction_timeout_ms: 60_000,
            ..Default::default()
        };
        b.null_str();
        b.i32(60_000);
        if v >= 3 {
            q.producer_id = -1;
            q.producer_epoch = -1;
            b.i64(-1);
            b.i16(-1);
        }
        b.tags();
        check("InitProducerIdRequest", v, &q, &b.out);
        let mut b = B::new(flex(22, v));
        let p = InitProducerIdResponse {
            throttle_time_ms: 0,
            error_code: 0,
            producer_id: 0x0001_9a2b_3c4d_0005,
            producer_epoch: 0,
            ..Default::default()
        };
        b.i32(0);
        b.i16(0);
        b.i64(0x0001_9a2b_3c4d_0005);
        b.i16(0);
        b.tags();
        check("InitProducerIdResponse", v, &p, &b.out);
    }
}

#[test]
fn create_and_delete_topics() {
    for v in versions(19) {
        let mut b = B::new(flex(19, v));
        let mut q = CreateTopicsRequest::default();
        q.topics = vec![create_topics_request::CreatableTopic {
            name: "shop.order_events".into(),
            num_partitions: 12,
            replication_factor: -1,
            assignments: vec![],
            configs: vec![create_topics_request::CreatableTopicConfig {
                name: "retention.ms".into(),
                value: Some("604800000".into()),
                ..Default::default()
            }],
            ..Default::default()
        }];
        b.arr(1);
        b.str("shop.order_events");
        b.i32(12);
        b.i16(-1);
        b.arr(0);
        b.arr(1);
        b.str("retention.ms");
        b.str("604800000");
        b.tags();
        b.tags();
        q.timeout_ms = 30_000;
        b.i32(30_000);
        if v >= 1 {
            q.validate_only = true;
            b.bool(true);
        }
        b.tags();
        check("CreateTopicsRequest", v, &q, &b.out);

        let mut b = B::new(flex(19, v));
        let mut p = CreateTopicsResponse::default();
        if v >= 2 {
            b.i32(0);
        }
        let mut t = create_topics_response::CreatableTopicResult {
            name: "shop.order_events".into(),
            error_code: 0,
            ..Default::default()
        };
        b.arr(1);
        b.str("shop.order_events");
        if v >= 7 {
            t.topic_id = U;
            b.uuid(U);
        }
        b.i16(0);
        if v >= 1 {
            t.error_message = None;
            b.null_str();
        }
        if v >= 5 {
            t.num_partitions = 12;
            t.replication_factor = 1;
            b.i32(12);
            b.i16(1);
            t.configs = Some(vec![create_topics_response::CreatableTopicConfigs {
                name: "cleanup.policy".into(),
                value: Some("delete".into()),
                read_only: false,
                config_source: 5,
                is_sensitive: false,
                ..Default::default()
            }]);
            b.arr(1);
            b.str("cleanup.policy");
            b.str("delete");
            b.bool(false);
            b.i8(5);
            b.bool(false);
            b.tags();
            // TopicConfigErrorCode, tag 0, = 40.
            t.topic_config_error_code = 40;
            b.uv(1);
            b.uv(0);
            b.uv(2);
            b.i16(40);
        }
        p.topics = vec![t];
        b.tags();
        check("CreateTopicsResponse", v, &p, &b.out);
    }
    for v in versions(20) {
        let mut b = B::new(flex(20, v));
        let mut q = DeleteTopicsRequest::default();
        if v >= 6 {
            q.topics = vec![delete_topics_request::DeleteTopicState {
                name: Some("shop.order_events".into()),
                topic_id: Uuid::nil(),
                ..Default::default()
            }];
            b.arr(1);
            b.str("shop.order_events");
            b.uuid(Uuid::nil());
            b.tags();
        } else {
            q.topic_names = vec!["shop.order_events".into()];
            b.arr(1);
            b.str("shop.order_events");
        }
        q.timeout_ms = 30_000;
        b.i32(30_000);
        b.tags();
        check("DeleteTopicsRequest", v, &q, &b.out);

        let mut b = B::new(flex(20, v));
        let mut p = DeleteTopicsResponse::default();
        if v >= 1 {
            b.i32(0);
        }
        let mut r = delete_topics_response::DeletableTopicResult {
            name: Some("shop.order_events".into()),
            error_code: 3,
            ..Default::default()
        };
        b.arr(1);
        b.str("shop.order_events");
        if v >= 6 {
            r.topic_id = U;
            b.uuid(U);
        }
        b.i16(3);
        if v >= 5 {
            r.error_message = Some("39940".into());
            b.str("39940");
        }
        b.tags();
        p.responses = vec![r];
        b.tags();
        check("DeleteTopicsResponse", v, &p, &b.out);
    }
}

#[test]
fn partitions_and_configs() {
    for v in versions(37) {
        let mut b = B::new(flex(37, v));
        let q = CreatePartitionsRequest {
            topics: vec![create_partitions_request::CreatePartitionsTopic {
                name: "shop.e".into(),
                count: 24,
                assignments: None,
                ..Default::default()
            }],
            timeout_ms: 30_000,
            validate_only: false,
            ..Default::default()
        };
        b.arr(1);
        b.str("shop.e");
        b.i32(24);
        b.null_arr();
        b.tags();
        b.i32(30_000);
        b.bool(false);
        b.tags();
        check("CreatePartitionsRequest", v, &q, &b.out);
        let mut b = B::new(flex(37, v));
        let p = CreatePartitionsResponse {
            throttle_time_ms: 0,
            results: vec![create_partitions_response::CreatePartitionsTopicResult {
                name: "shop.e".into(),
                error_code: 37,
                error_message: Some("39943".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        b.i32(0);
        b.arr(1);
        b.str("shop.e");
        b.i16(37);
        b.str("39943");
        b.tags();
        b.tags();
        check("CreatePartitionsResponse", v, &p, &b.out);
    }
    for v in versions(32) {
        let mut b = B::new(flex(32, v));
        let mut q = DescribeConfigsRequest {
            resources: vec![describe_configs_request::DescribeConfigsResource {
                resource_type: 2,
                resource_name: "shop.e".into(),
                configuration_keys: None,
                ..Default::default()
            }],
            ..Default::default()
        };
        b.arr(1);
        b.i8(2);
        b.str("shop.e");
        b.null_arr();
        b.tags();
        if v >= 1 {
            q.include_synonyms = true;
            b.bool(true);
        }
        if v >= 3 {
            q.include_documentation = true;
            b.bool(true);
        }
        b.tags();
        check("DescribeConfigsRequest", v, &q, &b.out);

        let mut b = B::new(flex(32, v));
        let mut c = describe_configs_response::DescribeConfigsResourceResult {
            name: "retention.ms".into(),
            value: Some("604800000".into()),
            read_only: false,
            is_sensitive: false,
            ..Default::default()
        };
        b.i32(0);
        b.arr(1);
        b.i16(0);
        b.null_str();
        b.i8(2);
        b.str("shop.e");
        b.arr(1);
        b.str("retention.ms");
        b.str("604800000");
        b.bool(false);
        if v == 0 {
            c.is_default = true;
            b.bool(true);
        }
        if v >= 1 {
            c.config_source = 5;
            b.i8(5);
        }
        b.bool(false);
        if v >= 1 {
            c.synonyms = vec![describe_configs_response::DescribeConfigsSynonym {
                name: "stream.retention_hours".into(),
                value: Some("168".into()),
                source: 5,
                ..Default::default()
            }];
            b.arr(1);
            b.str("stream.retention_hours");
            b.str("168");
            b.i8(5);
            b.tags();
        }
        if v >= 3 {
            c.config_type = 5;
            c.documentation = None;
            b.i8(5);
            b.null_str();
        }
        b.tags();
        b.tags();
        let p = DescribeConfigsResponse {
            throttle_time_ms: 0,
            results: vec![describe_configs_response::DescribeConfigsResult {
                error_code: 0,
                error_message: None,
                resource_type: 2,
                resource_name: "shop.e".into(),
                configs: vec![c],
                ..Default::default()
            }],
            ..Default::default()
        };
        b.tags();
        check("DescribeConfigsResponse", v, &p, &b.out);
    }
    for v in versions(44) {
        let mut b = B::new(flex(44, v));
        let q = IncrementalAlterConfigsRequest {
            resources: vec![incremental_alter_configs_request::AlterConfigsResource {
                resource_type: 2,
                resource_name: "shop.e".into(),
                configs: vec![incremental_alter_configs_request::AlterableConfig {
                    name: "cleanup.policy".into(),
                    config_operation: 0,
                    value: Some("compact".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            validate_only: false,
            ..Default::default()
        };
        b.arr(1);
        b.i8(2);
        b.str("shop.e");
        b.arr(1);
        b.str("cleanup.policy");
        b.i8(0);
        b.str("compact");
        b.tags();
        b.tags();
        b.bool(false);
        b.tags();
        check("IncrementalAlterConfigsRequest", v, &q, &b.out);
        let mut b = B::new(flex(44, v));
        let p = IncrementalAlterConfigsResponse {
            throttle_time_ms: 0,
            responses: vec![
                incremental_alter_configs_response::AlterConfigsResourceResponse {
                    error_code: 40,
                    error_message: Some("bad".into()),
                    resource_type: 2,
                    resource_name: "shop.e".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        b.i32(0);
        b.arr(1);
        b.i16(40);
        b.str("bad");
        b.i8(2);
        b.str("shop.e");
        b.tags();
        b.tags();
        check("IncrementalAlterConfigsResponse", v, &p, &b.out);
    }
}

#[test]
fn every_served_api_has_a_golden_case() {
    // Keep this list in step with the tests above.
    let covered = [
        18, 3, 10, 11, 14, 12, 13, 8, 9, 15, 16, 42, 17, 36, 22, 19, 20, 37, 32, 44, 0, 1, 2, 23,
    ];
    for r in SERVED.iter().chain(DATA_PLANE) {
        assert!(covered.contains(&r.key), "API {} has no golden case", r.key);
    }
}

#[test]
fn unknown_tagged_fields_survive_a_round_trip() {
    // A JoinGroup v6 request with a tagged field this codec doesn't know.
    let mut b = B::new(true);
    b.str("g");
    b.i32(10_000);
    b.i32(10_000);
    b.str("");
    b.null_str();
    b.str("consumer");
    b.arr(0);
    b.uv(1); // one tagged field
    b.uv(42);
    b.uv(2);
    b.out.extend_from_slice(&[0xab, 0xcd]);
    let m = JoinGroupRequest::from_bytes(&b.out, 6).unwrap();
    assert_eq!(m.unknown_tagged_fields.len(), 1);
    assert_eq!(m.unknown_tagged_fields[0].tag, 42);
    assert_eq!(m.to_bytes(6).unwrap(), b.out);
}

#[test]
fn produce() {
    let batch = [0x00u8, 0x01, 0x02, 0x03, 0x04];
    for v in versions(0) {
        let f = flex(0, v);
        let mut b = B::new(f);
        let q = ProduceRequest {
            transactional_id: None,
            acks: -1,
            timeout_ms: 30_000,
            topic_data: vec![produce_request::TopicProduceData {
                name: "shop.order_events".into(),
                partition_data: vec![produce_request::PartitionProduceData {
                    index: 5,
                    records: Some(batch.to_vec()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        b.null_str();
        b.i16(-1);
        b.i32(30_000);
        b.arr(1);
        b.str("shop.order_events");
        b.arr(1);
        b.i32(5);
        b.bytes(&batch);
        b.tags();
        b.tags();
        b.tags();
        check("ProduceRequest", v, &q, &b.out);

        let mut b = B::new(f);
        let mut pr = produce_response::PartitionProduceResponse {
            index: 5,
            error_code: 0,
            base_offset: 1205,
            log_append_time_ms: -1,
            ..Default::default()
        };
        b.arr(1);
        b.str("shop.order_events");
        b.arr(1);
        b.i32(5);
        b.i16(0);
        b.i64(1205);
        b.i64(-1);
        if v >= 5 {
            pr.log_start_offset = 7;
            b.i64(7);
        }
        if v >= 8 {
            pr.record_errors = vec![produce_response::BatchIndexAndErrorMessage {
                batch_index: 0,
                batch_index_error_message: None,
                ..Default::default()
            }];
            pr.error_message = Some("39944".into());
            b.arr(1);
            b.i32(0);
            b.null_str();
            b.tags();
            b.str("39944");
        }
        b.tags();
        b.tags();
        b.i32(0);
        b.tags();
        let p = ProduceResponse {
            responses: vec![produce_response::TopicProduceResponse {
                name: "shop.order_events".into(),
                partition_responses: vec![pr],
                ..Default::default()
            }],
            throttle_time_ms: 0,
            ..Default::default()
        };
        check("ProduceResponse", v, &p, &b.out);
    }
}

#[test]
fn fetch() {
    let records = [0xaau8, 0xbb];
    for v in versions(1) {
        let f = flex(1, v);
        let mut b = B::new(f);
        let mut part = fetch_request::FetchPartition {
            partition: 5,
            fetch_offset: 1205,
            partition_max_bytes: 1_048_576,
            ..Default::default()
        };
        let mut topic = fetch_request::FetchTopic::default();
        let mut q = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 52_428_800,
            isolation_level: 1,
            ..Default::default()
        };
        b.i32(-1);
        b.i32(500);
        b.i32(1);
        b.i32(52_428_800);
        b.i8(1);
        if v >= 7 {
            q.session_id = 0;
            q.session_epoch = -1;
            b.i32(0);
            b.i32(-1);
        }
        b.arr(1);
        if v >= 13 {
            topic.topic_id = U;
            b.uuid(U);
        } else {
            topic.topic = "shop.order_events".into();
            b.str("shop.order_events");
        }
        b.arr(1);
        b.i32(5);
        if v >= 9 {
            part.current_leader_epoch = 3;
            b.i32(3);
        }
        b.i64(1205);
        if v >= 12 {
            part.last_fetched_epoch = -1;
            b.i32(-1);
        }
        if v >= 5 {
            part.log_start_offset = -1;
            b.i64(-1);
        }
        b.i32(1_048_576);
        b.tags();
        b.tags();
        topic.partitions = vec![part];
        q.topics = vec![topic];
        if v >= 7 {
            b.arr(0);
        }
        if v >= 11 {
            q.rack_id = "dc-a".into();
            b.str("dc-a");
        }
        b.tags();
        check("FetchRequest", v, &q, &b.out);

        let mut b = B::new(f);
        let mut p = FetchResponse {
            throttle_time_ms: 0,
            ..Default::default()
        };
        b.i32(0);
        if v >= 7 {
            p.error_code = 0;
            p.session_id = 0;
            b.i16(0);
            b.i32(0);
        }
        let mut t = fetch_response::FetchableTopicResponse::default();
        b.arr(1);
        if v >= 13 {
            t.topic_id = U;
            b.uuid(U);
        } else {
            t.topic = "shop.order_events".into();
            b.str("shop.order_events");
        }
        let mut pd = fetch_response::PartitionData {
            partition_index: 5,
            error_code: 0,
            high_watermark: 1206,
            last_stable_offset: 1206,
            aborted_transactions: Some(Vec::new()),
            records: Some(records.to_vec()),
            ..Default::default()
        };
        b.arr(1);
        b.i32(5);
        b.i16(0);
        b.i64(1206);
        b.i64(1206);
        if v >= 5 {
            pd.log_start_offset = 0;
            b.i64(0);
        }
        b.arr(0);
        if v >= 11 {
            pd.preferred_read_replica = -1;
            b.i32(-1);
        }
        b.bytes(&records);
        b.tags();
        b.tags();
        b.tags();
        t.partitions = vec![pd];
        p.responses = vec![t];
        check("FetchResponse", v, &p, &b.out);
        // read_uncommitted: no aborted transactions list at all (null).
        if v >= 4 {
            let mut p2 = p.clone();
            p2.responses[0].partitions[0].aborted_transactions = None;
            let bytes = p2.to_bytes(v).unwrap();
            assert_eq!(FetchResponse::from_bytes(&bytes, v).unwrap(), p2);
            assert_eq!(bytes.len(), b.out.len());
            assert_ne!(bytes, b.out);
        }
    }
}

#[test]
fn list_offsets() {
    for v in versions(2) {
        let f = flex(2, v);
        let mut b = B::new(f);
        let mut part = list_offsets_request::ListOffsetsPartition {
            partition_index: 5,
            timestamp: -2,
            ..Default::default()
        };
        let mut q = ListOffsetsRequest {
            replica_id: -1,
            ..Default::default()
        };
        b.i32(-1);
        if v >= 2 {
            q.isolation_level = 1;
            b.i8(1);
        }
        b.arr(1);
        b.str("shop.order_events");
        b.arr(1);
        b.i32(5);
        if v >= 4 {
            part.current_leader_epoch = 3;
            b.i32(3);
        }
        b.i64(-2);
        b.tags();
        b.tags();
        b.tags();
        q.topics = vec![list_offsets_request::ListOffsetsTopic {
            name: "shop.order_events".into(),
            partitions: vec![part],
            ..Default::default()
        }];
        check("ListOffsetsRequest", v, &q, &b.out);

        let mut b = B::new(f);
        let mut p = ListOffsetsResponse::default();
        if v >= 2 {
            p.throttle_time_ms = 0;
            b.i32(0);
        }
        let mut pr = list_offsets_response::ListOffsetsPartitionResponse {
            partition_index: 5,
            error_code: 0,
            timestamp: -1,
            offset: 1200,
            ..Default::default()
        };
        b.arr(1);
        b.str("shop.order_events");
        b.arr(1);
        b.i32(5);
        b.i16(0);
        b.i64(-1);
        b.i64(1200);
        if v >= 4 {
            pr.leader_epoch = 3;
            b.i32(3);
        }
        b.tags();
        b.tags();
        b.tags();
        p.topics = vec![list_offsets_response::ListOffsetsTopicResponse {
            name: "shop.order_events".into(),
            partitions: vec![pr],
            ..Default::default()
        }];
        check("ListOffsetsResponse", v, &p, &b.out);
    }
}

#[test]
fn offset_for_leader_epoch() {
    for v in versions(23) {
        let f = flex(23, v);
        let mut b = B::new(f);
        let mut q = OffsetForLeaderEpochRequest::default();
        if v >= 3 {
            q.replica_id = -1;
            b.i32(-1);
        }
        b.arr(1);
        b.str("shop.order_events");
        b.arr(1);
        b.i32(5);
        b.i32(4);
        b.i32(3);
        b.tags();
        b.tags();
        b.tags();
        q.topics = vec![offset_for_leader_epoch_request::OffsetForLeaderTopic {
            topic: "shop.order_events".into(),
            partitions: vec![offset_for_leader_epoch_request::OffsetForLeaderPartition {
                partition: 5,
                current_leader_epoch: 4,
                leader_epoch: 3,
                ..Default::default()
            }],
            ..Default::default()
        }];
        check("OffsetForLeaderEpochRequest", v, &q, &b.out);

        let mut b = B::new(f);
        b.i32(0);
        b.arr(1);
        b.str("shop.order_events");
        b.arr(1);
        b.i16(0);
        b.i32(5);
        b.i32(3);
        b.i64(1180);
        b.tags();
        b.tags();
        b.tags();
        let p = OffsetForLeaderEpochResponse {
            throttle_time_ms: 0,
            topics: vec![
                offset_for_leader_epoch_response::OffsetForLeaderTopicResult {
                    topic: "shop.order_events".into(),
                    partitions: vec![offset_for_leader_epoch_response::EpochEndOffset {
                        error_code: 0,
                        partition: 5,
                        leader_epoch: 3,
                        end_offset: 1180,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        check("OffsetForLeaderEpochResponse", v, &p, &b.out);
    }
}

#[test]
fn fetch_tagged_fields_of_newer_versions_are_kept() {
    // A v12 partition with CurrentLeader (tag 1), which SprigDB never
    // sends but a client reading another broker's response decodes.
    let mut b = B::new(true);
    b.i32(0);
    b.i16(0);
    b.i32(0);
    b.arr(1);
    b.str("t.x");
    b.arr(1);
    b.i32(0);
    b.i16(74);
    b.i64(10);
    b.i64(10);
    b.i64(0);
    b.null_arr();
    b.i32(-1);
    b.bytes(&[]);
    b.uv(1); // one tagged field
    b.uv(1); // CurrentLeader
    b.uv(9); // its size
    b.i32(2);
    b.i32(7);
    b.uv(0);
    b.tags();
    b.tags();
    let m = FetchResponse::from_bytes(&b.out, 12).unwrap();
    let p = &m.responses[0].partitions[0];
    assert_eq!(
        (p.current_leader.leader_id, p.current_leader.leader_epoch),
        (2, 7)
    );
    assert_eq!(m.to_bytes(12).unwrap(), b.out);
}
