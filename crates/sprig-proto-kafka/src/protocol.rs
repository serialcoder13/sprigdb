//! API keys, the versions SprigDB serves, Kafka error codes, and request
//! and response headers.

use crate::messages;
use crate::wire::{CodecError, RawTaggedField, Reader, Result, Writer};

/// Kafka API keys SprigDB knows (served or planned).
#[allow(missing_docs)]
pub mod api {
    pub const PRODUCE: i16 = 0;
    pub const FETCH: i16 = 1;
    pub const LIST_OFFSETS: i16 = 2;
    pub const METADATA: i16 = 3;
    pub const OFFSET_COMMIT: i16 = 8;
    pub const OFFSET_FETCH: i16 = 9;
    pub const FIND_COORDINATOR: i16 = 10;
    pub const JOIN_GROUP: i16 = 11;
    pub const HEARTBEAT: i16 = 12;
    pub const LEAVE_GROUP: i16 = 13;
    pub const SYNC_GROUP: i16 = 14;
    pub const DESCRIBE_GROUPS: i16 = 15;
    pub const LIST_GROUPS: i16 = 16;
    pub const SASL_HANDSHAKE: i16 = 17;
    pub const API_VERSIONS: i16 = 18;
    pub const CREATE_TOPICS: i16 = 19;
    pub const DELETE_TOPICS: i16 = 20;
    pub const INIT_PRODUCER_ID: i16 = 22;
    pub const OFFSET_FOR_LEADER_EPOCH: i16 = 23;
    pub const DESCRIBE_CONFIGS: i16 = 32;
    pub const SASL_AUTHENTICATE: i16 = 36;
    pub const CREATE_PARTITIONS: i16 = 37;
    pub const DELETE_GROUPS: i16 = 42;
    pub const INCREMENTAL_ALTER_CONFIGS: i16 = 44;
}

/// Kafka error codes (`org.apache.kafka.common.protocol.Errors`) that
/// SprigDB returns.
#[allow(missing_docs)]
pub mod err {
    pub const UNKNOWN_SERVER_ERROR: i16 = -1;
    pub const NONE: i16 = 0;
    pub const OFFSET_OUT_OF_RANGE: i16 = 1;
    pub const CORRUPT_MESSAGE: i16 = 2;
    pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
    pub const INVALID_FETCH_SIZE: i16 = 4;
    pub const LEADER_NOT_AVAILABLE: i16 = 5;
    pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
    pub const REQUEST_TIMED_OUT: i16 = 7;
    pub const MESSAGE_TOO_LARGE: i16 = 10;
    pub const RECORD_LIST_TOO_LARGE: i16 = 18;
    pub const INVALID_REQUIRED_ACKS: i16 = 21;
    pub const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
    pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
    pub const NOT_COORDINATOR: i16 = 16;
    pub const INVALID_TOPIC_EXCEPTION: i16 = 17;
    pub const ILLEGAL_GENERATION: i16 = 22;
    pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
    pub const INVALID_GROUP_ID: i16 = 24;
    pub const UNKNOWN_MEMBER_ID: i16 = 25;
    pub const INVALID_SESSION_TIMEOUT: i16 = 26;
    pub const REBALANCE_IN_PROGRESS: i16 = 27;
    pub const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
    pub const GROUP_AUTHORIZATION_FAILED: i16 = 30;
    pub const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
    pub const INVALID_TIMESTAMP: i16 = 32;
    pub const UNSUPPORTED_SASL_MECHANISM: i16 = 33;
    pub const ILLEGAL_SASL_STATE: i16 = 34;
    pub const UNSUPPORTED_VERSION: i16 = 35;
    pub const TOPIC_ALREADY_EXISTS: i16 = 36;
    pub const INVALID_PARTITIONS: i16 = 37;
    pub const INVALID_REPLICATION_FACTOR: i16 = 38;
    pub const INVALID_REPLICA_ASSIGNMENT: i16 = 39;
    pub const INVALID_CONFIG: i16 = 40;
    pub const INVALID_REQUEST: i16 = 42;
    pub const POLICY_VIOLATION: i16 = 44;
    pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
    pub const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
    pub const INVALID_PRODUCER_EPOCH: i16 = 47;
    pub const INVALID_TXN_STATE: i16 = 48;
    pub const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;
    pub const SASL_AUTHENTICATION_FAILED: i16 = 58;
    pub const KAFKA_STORAGE_ERROR: i16 = 56;
    pub const UNKNOWN_PRODUCER_ID: i16 = 59;
    pub const NON_EMPTY_GROUP: i16 = 68;
    pub const GROUP_ID_NOT_FOUND: i16 = 69;
    pub const FENCED_LEADER_EPOCH: i16 = 74;
    pub const UNKNOWN_LEADER_EPOCH: i16 = 75;
    pub const UNSUPPORTED_COMPRESSION_TYPE: i16 = 76;
    pub const OFFSET_NOT_AVAILABLE: i16 = 78;
    pub const MEMBER_ID_REQUIRED: i16 = 79;
    pub const GROUP_MAX_SIZE_REACHED: i16 = 81;
    pub const FENCED_INSTANCE_ID: i16 = 82;
    pub const INVALID_RECORD: i16 = 87;
    pub const UNKNOWN_TOPIC_ID: i16 = 100;
}

/// SprigDB's own error numbers for the Kafka front end (chapter 45,
/// "Errors"), carried in responses' message fields.
#[allow(missing_docs)]
pub mod sprig {
    pub const STREAM_NOT_FOUND: u16 = 39940;
    pub const STREAM_APPEND_ONLY: u16 = 39941;
    pub const STREAM_EXISTS: u16 = 39942;
    pub const STREAM_PARTITIONS_DECREASE: u16 = 39943;
    pub const STREAM_RECORD_TOO_LARGE: u16 = 39944;
    pub const STREAM_SEQUENCING_TIMEOUT: u16 = 39946;
    pub const STREAM_OFFSET_OUT_OF_RANGE: u16 = 39947;
    pub const STREAM_TRUNCATED: u16 = 39948;
    pub const KAFKA_ACKS_ZERO: u16 = 39950;
    pub const KAFKA_TRANSACTIONS_UNSUPPORTED: u16 = 39951;
    pub const KAFKA_UNSUPPORTED_API: u16 = 39952;
    pub const KAFKA_SASL_FAILED: u16 = 39953;
    pub const KAFKA_NOT_COORDINATOR: u16 = 39954;
    pub const STREAM_COMPACT_NEEDS_KEY: u16 = 39955;
}

/// One served API: its key and version range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiRange {
    /// The API key.
    pub key: i16,
    /// The lowest served version.
    pub min: i16,
    /// The highest served version.
    pub max: i16,
    /// The first flexible version of the API.
    pub flexible_min: i16,
}

impl ApiRange {
    /// Whether `version` is served.
    #[must_use]
    pub fn serves(&self, version: i16) -> bool {
        (self.min..=self.max).contains(&version)
    }
}

macro_rules! range {
    ($key:expr, $min:expr, $max:expr, $m:ident) => {
        ApiRange {
            key: $key,
            min: $min,
            max: $max,
            flexible_min: messages::$m::FLEXIBLE_MIN,
        }
    };
}

/// The APIs the broker itself serves and advertises in ApiVersions
/// (chapter 45, "Kafka APIs served"). Produce, Fetch, ListOffsets and
/// OffsetForLeaderEpoch are [`DATA_PLANE`]'s, advertised when the broker
/// has a [`crate::broker::DataPlane`].
pub const SERVED: &[ApiRange] = &[
    range!(api::METADATA, 0, 12, metadata_request),
    range!(api::OFFSET_COMMIT, 0, 8, offset_commit_request),
    range!(api::OFFSET_FETCH, 0, 8, offset_fetch_request),
    range!(api::FIND_COORDINATOR, 0, 4, find_coordinator_request),
    range!(api::JOIN_GROUP, 0, 9, join_group_request),
    range!(api::HEARTBEAT, 0, 4, heartbeat_request),
    range!(api::LEAVE_GROUP, 0, 5, leave_group_request),
    range!(api::SYNC_GROUP, 0, 5, sync_group_request),
    range!(api::DESCRIBE_GROUPS, 0, 5, describe_groups_request),
    range!(api::LIST_GROUPS, 0, 5, list_groups_request),
    range!(api::SASL_HANDSHAKE, 0, 1, sasl_handshake_request),
    range!(api::API_VERSIONS, 0, 4, api_versions_request),
    range!(api::CREATE_TOPICS, 0, 7, create_topics_request),
    range!(api::DELETE_TOPICS, 0, 6, delete_topics_request),
    range!(api::INIT_PRODUCER_ID, 0, 4, init_producer_id_request),
    range!(api::DESCRIBE_CONFIGS, 0, 4, describe_configs_request),
    range!(api::SASL_AUTHENTICATE, 0, 2, sasl_authenticate_request),
    range!(api::CREATE_PARTITIONS, 0, 3, create_partitions_request),
    range!(api::DELETE_GROUPS, 0, 2, delete_groups_request),
    range!(
        api::INCREMENTAL_ALTER_CONFIGS,
        0,
        1,
        incremental_alter_configs_request
    ),
];

/// The data plane's APIs (chapter 45 build steps 10 and 11), served by
/// [`crate::broker::data::DataService`].
pub const DATA_PLANE: &[ApiRange] = &[
    range!(api::PRODUCE, 3, 9, produce_request),
    range!(api::FETCH, 4, 13, fetch_request),
    range!(api::LIST_OFFSETS, 1, 7, list_offsets_request),
    range!(
        api::OFFSET_FOR_LEADER_EPOCH,
        2,
        4,
        offset_for_leader_epoch_request
    ),
];

/// The served range of `key`, if any (the broker's own APIs).
#[must_use]
pub fn served(key: i16) -> Option<&'static ApiRange> {
    SERVED.iter().find(|r| r.key == key)
}

/// The first flexible version of every API SprigDB knows, served by the
/// broker or by the data plane: request and response headers follow it
/// whether or not the API is advertised. `None` for an unknown API.
#[must_use]
pub fn flexible_min(key: i16) -> Option<i16> {
    SERVED
        .iter()
        .chain(DATA_PLANE)
        .find(|r| r.key == key)
        .map(|r| r.flexible_min)
}

/// The metric and log name of an API key.
#[must_use]
pub fn api_name(key: i16) -> &'static str {
    match key {
        api::PRODUCE => "Produce",
        api::FETCH => "Fetch",
        api::LIST_OFFSETS => "ListOffsets",
        api::METADATA => "Metadata",
        api::OFFSET_COMMIT => "OffsetCommit",
        api::OFFSET_FETCH => "OffsetFetch",
        api::FIND_COORDINATOR => "FindCoordinator",
        api::JOIN_GROUP => "JoinGroup",
        api::HEARTBEAT => "Heartbeat",
        api::LEAVE_GROUP => "LeaveGroup",
        api::SYNC_GROUP => "SyncGroup",
        api::DESCRIBE_GROUPS => "DescribeGroups",
        api::LIST_GROUPS => "ListGroups",
        api::SASL_HANDSHAKE => "SaslHandshake",
        api::API_VERSIONS => "ApiVersions",
        api::CREATE_TOPICS => "CreateTopics",
        api::DELETE_TOPICS => "DeleteTopics",
        api::INIT_PRODUCER_ID => "InitProducerId",
        api::OFFSET_FOR_LEADER_EPOCH => "OffsetForLeaderEpoch",
        api::DESCRIBE_CONFIGS => "DescribeConfigs",
        api::SASL_AUTHENTICATE => "SaslAuthenticate",
        api::CREATE_PARTITIONS => "CreatePartitions",
        api::DELETE_GROUPS => "DeleteGroups",
        api::INCREMENTAL_ALTER_CONFIGS => "IncrementalAlterConfigs",
        _ => "unknown",
    }
}

/// A request header (versions 1 and 2; version 0, without the client id,
/// is only ControlledShutdown v0, which SprigDB doesn't serve).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RequestHeader {
    /// The API key.
    pub api_key: i16,
    /// The API version.
    pub api_version: i16,
    /// Echoed in the response.
    pub correlation_id: i32,
    /// The client's id (a classic nullable string even in header v2).
    pub client_id: Option<String>,
    /// Header v2's tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl RequestHeader {
    /// The header version a request of `api_key` at `api_version` uses: 2
    /// for flexible versions, 1 otherwise. Unknown APIs are read as 1.
    #[must_use]
    pub fn version_for(api_key: i16, api_version: i16) -> i16 {
        match flexible_min(api_key) {
            Some(f) if api_version >= f => 2,
            _ => 1,
        }
    }

    /// Decodes a header. The version comes from the first four bytes, so
    /// the caller passes the whole request.
    ///
    /// # Errors
    /// Any [`CodecError`].
    pub fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let api_key = r.i16()?;
        let api_version = r.i16()?;
        let correlation_id = r.i32()?;
        let client_id = r.nullable_string(false)?;
        let unknown_tagged_fields = if Self::version_for(api_key, api_version) >= 2 {
            r.tagged_fields(|_, _| Ok(false))?
        } else {
            Vec::new()
        };
        Ok(Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
            unknown_tagged_fields,
        })
    }

    /// Encodes a header (for clients and tests).
    ///
    /// # Errors
    /// `Invalid` for a client id over 32767 bytes.
    pub fn encode(&self, w: &mut Writer) -> Result<()> {
        w.i16(self.api_key);
        w.i16(self.api_version);
        w.i32(self.correlation_id);
        w.nullable_string(self.client_id.as_deref(), false)?;
        if Self::version_for(self.api_key, self.api_version) >= 2 {
            w.tagged_fields(Vec::new(), &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
}

/// The response header version for a response to `api_key` at
/// `api_version`: 1 (with tagged fields) for flexible versions, except
/// ApiVersions, whose responses always use 0 so a client can read one
/// whatever version it asked for.
#[must_use]
pub fn response_header_version(api_key: i16, api_version: i16) -> i16 {
    if api_key == api::API_VERSIONS {
        return 0;
    }
    match flexible_min(api_key) {
        Some(f) if api_version >= f => 1,
        _ => 0,
    }
}

/// Encodes a complete response frame: the 4-byte size, the header and the
/// body.
///
/// # Errors
/// As the body's encoder.
pub fn response_frame(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    body: impl FnOnce(&mut Writer) -> Result<()>,
) -> Result<Vec<u8>> {
    let mut w = Writer::from_vec(vec![0; 4]);
    w.i32(correlation_id);
    if response_header_version(api_key, api_version) >= 1 {
        w.uvarint(0);
    }
    body(&mut w)?;
    let size = i32::try_from(w.len() - 4).map_err(|_| CodecError::Invalid("frame size"))?;
    w.as_mut_slice()[..4].copy_from_slice(&size.to_be_bytes());
    Ok(w.into_inner())
}

/// Encodes a complete request frame (for clients and tests).
///
/// # Errors
/// As the body's encoder.
pub fn request_frame(
    header: &RequestHeader,
    body: impl FnOnce(&mut Writer) -> Result<()>,
) -> Result<Vec<u8>> {
    let mut w = Writer::from_vec(vec![0; 4]);
    header.encode(&mut w)?;
    body(&mut w)?;
    let size = i32::try_from(w.len() - 4).map_err(|_| CodecError::Invalid("frame size"))?;
    w.as_mut_slice()[..4].copy_from_slice(&size.to_be_bytes());
    Ok(w.into_inner())
}

/// Splits a response frame body (after the size) into its correlation id
/// and the body, for clients and tests.
///
/// # Errors
/// `Truncated` or `Invalid`.
pub fn read_response_header(
    frame: &[u8],
    api_key: i16,
    api_version: i16,
) -> Result<(i32, Reader<'_>)> {
    let mut r = Reader::new(frame);
    let correlation_id = r.i32()?;
    if response_header_version(api_key, api_version) >= 1 {
        r.tagged_fields(|_, _| Ok(false))?;
    }
    Ok((correlation_id, r))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::Message;

    #[test]
    fn served_ranges_are_within_the_definitions() {
        use messages::*;
        let defs: &[(i16, i16, i16)] = &[
            (
                api::METADATA,
                MetadataRequest::MIN_VERSION,
                MetadataRequest::MAX_VERSION,
            ),
            (
                api::API_VERSIONS,
                ApiVersionsRequest::MIN_VERSION,
                ApiVersionsRequest::MAX_VERSION,
            ),
            (
                api::JOIN_GROUP,
                JoinGroupRequest::MIN_VERSION,
                JoinGroupRequest::MAX_VERSION,
            ),
            (
                api::OFFSET_COMMIT,
                OffsetCommitRequest::MIN_VERSION,
                OffsetCommitRequest::MAX_VERSION,
            ),
            (
                api::OFFSET_FETCH,
                OffsetFetchRequest::MIN_VERSION,
                OffsetFetchRequest::MAX_VERSION,
            ),
            (
                api::LIST_GROUPS,
                ListGroupsRequest::MIN_VERSION,
                ListGroupsRequest::MAX_VERSION,
            ),
            (
                api::INIT_PRODUCER_ID,
                InitProducerIdRequest::MIN_VERSION,
                InitProducerIdRequest::MAX_VERSION,
            ),
        ];
        let data: &[(i16, i16, i16)] = &[
            (
                api::PRODUCE,
                ProduceRequest::MIN_VERSION,
                ProduceRequest::MAX_VERSION,
            ),
            (
                api::FETCH,
                FetchRequest::MIN_VERSION,
                FetchRequest::MAX_VERSION,
            ),
            (
                api::LIST_OFFSETS,
                ListOffsetsRequest::MIN_VERSION,
                ListOffsetsRequest::MAX_VERSION,
            ),
            (
                api::OFFSET_FOR_LEADER_EPOCH,
                OffsetForLeaderEpochRequest::MIN_VERSION,
                OffsetForLeaderEpochRequest::MAX_VERSION,
            ),
        ];
        for (k, lo, hi) in data {
            let r = DATA_PLANE.iter().find(|r| r.key == *k).unwrap();
            assert!(r.min >= *lo && r.max <= *hi, "{}", api_name(*k));
            assert!(served(*k).is_none());
        }
        for (k, lo, hi) in defs {
            let r = served(*k).unwrap();
            assert!(r.min >= *lo && r.max <= *hi, "{}", api_name(*k));
        }
        // No transaction APIs, no duplicates.
        for k in [24, 25, 26, 27, 28] {
            assert!(served(k).is_none());
        }
        let mut keys: Vec<i16> = SERVED.iter().map(|r| r.key).collect();
        keys.sort_unstable();
        keys.dedup();
        assert_eq!(keys.len(), SERVED.len());
    }

    #[test]
    fn header_versions() {
        assert_eq!(RequestHeader::version_for(api::METADATA, 8), 1);
        assert_eq!(RequestHeader::version_for(api::METADATA, 9), 2);
        assert_eq!(RequestHeader::version_for(api::API_VERSIONS, 3), 2);
        assert_eq!(response_header_version(api::API_VERSIONS, 3), 0);
        assert_eq!(response_header_version(api::METADATA, 9), 1);
        assert_eq!(response_header_version(api::SASL_HANDSHAKE, 1), 0);
        // The data plane's APIs follow their own flexible versions, even
        // when no data plane is attached.
        assert_eq!(RequestHeader::version_for(api::PRODUCE, 8), 1);
        assert_eq!(RequestHeader::version_for(api::PRODUCE, 9), 2);
        assert_eq!(response_header_version(api::PRODUCE, 9), 1);
        assert_eq!(RequestHeader::version_for(api::FETCH, 11), 1);
        assert_eq!(RequestHeader::version_for(api::FETCH, 12), 2);
        assert_eq!(response_header_version(api::FETCH, 12), 1);
        assert_eq!(response_header_version(api::LIST_OFFSETS, 6), 1);
        assert_eq!(response_header_version(api::LIST_OFFSETS, 5), 0);
        assert_eq!(RequestHeader::version_for(api::OFFSET_FOR_LEADER_EPOCH, 4), 2);
        // An API SprigDB doesn't know reads as header v1.
        assert_eq!(RequestHeader::version_for(57, 9), 1);
    }

    #[test]
    fn golden_api_versions_v3_request_frame() {
        // What a librdkafka 2.x client sends first (layout from the
        // protocol guide): header v2 with client id "rdkafka", then the
        // compact strings "librdkafka" and "2.3.0".
        let header = RequestHeader {
            api_key: 18,
            api_version: 3,
            correlation_id: 1,
            client_id: Some("rdkafka".into()),
            unknown_tagged_fields: Vec::new(),
        };
        let body = messages::ApiVersionsRequest {
            client_software_name: "librdkafka".into(),
            client_software_version: "2.3.0".into(),
            ..Default::default()
        };
        let frame = request_frame(&header, |w| body.encode(w, 3)).unwrap();
        let mut want = vec![0, 0, 0, 0x24, 0, 18, 0, 3, 0, 0, 0, 1, 0, 7];
        want.extend_from_slice(b"rdkafka");
        want.push(0); // header tagged fields
        want.push(11);
        want.extend_from_slice(b"librdkafka");
        want.push(6);
        want.extend_from_slice(b"2.3.0");
        want.push(0); // body tagged fields
        assert_eq!(frame, want);
        let mut r = Reader::new(&frame[4..]);
        assert_eq!(RequestHeader::decode(&mut r).unwrap(), header);
        assert_eq!(
            messages::ApiVersionsRequest::decode(&mut r, 3).unwrap(),
            body
        );
    }
}
