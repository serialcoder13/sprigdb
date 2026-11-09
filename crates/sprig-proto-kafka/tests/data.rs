//! The data plane end to end (chapter 45 build steps 10 and 11): Produce,
//! Fetch, ListOffsets and OffsetForLeaderEpoch through the broker, with the
//! crate's own client, over [`MemData`].

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::many_single_char_names
)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sprig_common::CancellationToken;
use sprig_proto_kafka::broker::data::DataService;
use sprig_proto_kafka::broker::{
    AuthFailure, Broker, BrokerConfig, BrokerNode, Cluster, ConnInfo, Identity, ProducerIds,
    ScramFinish, ScramLookup,
};
use sprig_proto_kafka::client::{Client, ClientError};
use sprig_proto_kafka::compression::Compression;
use sprig_proto_kafka::group::{Coordinator, GroupConfig};
use sprig_proto_kafka::mem::{MemData, MemGroupStore, MemLeaseTable, MemLeases, MemTopics};
use sprig_proto_kafka::messages::*;
use sprig_proto_kafka::protocol::err;
use sprig_proto_kafka::records::{Header, Record, RecordBatch};
use sprig_proto_kafka::sasl::{PlainCredentials, ScramCredential, ScramMechanism};
use sprig_proto_kafka::topics::{NewTopic, Principal, TopicConfigs, Topics};
use tokio::io::DuplexStream;

const TENANT: u64 = 41;

fn principal() -> Principal {
    Principal {
        tenant_id: TENANT,
        tenant: "acme".into(),
        user: "app".into(),
        user_id: Some(7),
        handle: None,
    }
}

struct Id;

struct Fin;

#[async_trait]
impl ScramFinish for Fin {
    async fn finish(self: Box<Self>, _verified: bool) -> Result<Principal, AuthFailure> {
        Err(AuthFailure("no SCRAM here".into()))
    }
}

#[async_trait]
impl Identity for Id {
    async fn plain(&self, _c: &ConnInfo, _p: PlainCredentials) -> Result<Principal, AuthFailure> {
        Ok(principal())
    }
    async fn scram(&self, _c: &ConnInfo, mech: ScramMechanism, user: &str) -> ScramLookup {
        ScramLookup {
            credential: ScramCredential::decoy(mech, b"d", user, 4096),
            finish: Box::new(Fin),
        }
    }
}

struct One;

#[async_trait]
impl Cluster for One {
    fn node_id(&self) -> i32 {
        1
    }
    fn cluster_id(&self) -> String {
        "sprig-dc-a".into()
    }
    async fn brokers(&self, _p: &Principal) -> Vec<BrokerNode> {
        vec![BrokerNode {
            node_id: 1,
            host: "gw1".into(),
            port: 9092,
            rack: None,
        }]
    }
}

struct Env {
    topics: Arc<MemTopics>,
    data: Arc<MemData>,
    broker: Arc<Broker>,
}

async fn env() -> Env {
    let topics = Arc::new(MemTopics::default());
    let data = Arc::new(MemData::new(Arc::clone(&topics)));
    let groups = Arc::new(Coordinator::new(
        Arc::new(MemGroupStore::default()),
        Arc::new(MemLeases {
            table: Arc::new(MemLeaseTable::default()),
            node: 1,
        }),
        GroupConfig::default(),
    ));
    let broker = Arc::new(Broker {
        identity: Arc::new(Id),
        cluster: Arc::new(One),
        topics: topics.clone(),
        groups,
        data: Some(Arc::new(DataService::new(data.clone()))),
        config: Arc::new(BrokerConfig::default),
        producer_ids: ProducerIds::new(1),
    });
    let p = principal();
    for (name, cleanup) in [("shop.events", None), ("shop.latest", Some(true))] {
        topics
            .create(
                &p,
                NewTopic {
                    name: name.into(),
                    partitions: 2,
                    configs: TopicConfigs {
                        cleanup: cleanup.map(|_| sprig_proto_kafka::topics::Cleanup::Compact),
                        ..TopicConfigs::default()
                    },
                },
                false,
            )
            .await
            .unwrap();
    }
    topics.add_change_topic(TENANT, "shop.orders.changes", 1);
    Env {
        topics,
        data,
        broker,
    }
}

async fn client(b: &Arc<Broker>) -> Client<DuplexStream> {
    let (a, mut s) = tokio::io::duplex(1 << 22);
    let b = Arc::clone(b);
    tokio::spawn(async move {
        let info = ConnInfo {
            peer: "10.0.0.9:51000".parse().unwrap(),
            tls: true,
            sni: None,
            client_cert: false,
        };
        let _ = b.serve(&mut s, info, CancellationToken::new()).await;
    });
    let mut c = Client::new(a, "sprig-test");
    c.sasl_plain("acme.app", "x").await.unwrap();
    c
}

fn batch(values: &[&str], codec: Compression, producer: Option<(i64, i16, i32)>) -> Vec<u8> {
    let (pid, epoch, seq) = producer.unwrap_or((-1, -1, -1));
    RecordBatch {
        attributes: i16::from(codec.id()),
        last_offset_delta: i32::try_from(values.len()).unwrap() - 1,
        base_timestamp: 1_762_000_000_000,
        max_timestamp: 1_762_000_000_000,
        producer_id: pid,
        producer_epoch: epoch,
        base_sequence: seq,
        records: values
            .iter()
            .enumerate()
            .map(|(i, v)| Record {
                timestamp_delta: i64::try_from(i).unwrap(),
                offset_delta: i32::try_from(i).unwrap(),
                key: Some(format!("k{i}").into_bytes()),
                value: Some(v.as_bytes().to_vec()),
                headers: vec![Header {
                    key: "h".into(),
                    value: Some(b"x".to_vec()),
                }],
                ..Record::default()
            })
            .collect(),
        ..RecordBatch::default()
    }
    .to_bytes()
    .unwrap()
}

fn produce_req(topic: &str, partition: i32, records: Vec<u8>, acks: i16) -> ProduceRequest {
    ProduceRequest {
        acks,
        timeout_ms: 5000,
        topic_data: vec![produce_request::TopicProduceData {
            name: topic.into(),
            partition_data: vec![produce_request::PartitionProduceData {
                index: partition,
                records: Some(records),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn part0(r: &ProduceResponse) -> &produce_response::PartitionProduceResponse {
    &r.responses[0].partition_responses[0]
}

async fn produce(
    c: &mut Client<DuplexStream>,
    v: i16,
    topic: &str,
    partition: i32,
    records: Vec<u8>,
) -> produce_response::PartitionProduceResponse {
    let r: ProduceResponse = c
        .call(v, &produce_req(topic, partition, records, -1))
        .await
        .unwrap();
    part0(&r).clone()
}

fn fetch_req(topic: &str, partition: i32, offset: i64, max_wait_ms: i32) -> FetchRequest {
    FetchRequest {
        replica_id: -1,
        max_wait_ms,
        min_bytes: 1,
        max_bytes: 1 << 20,
        isolation_level: 1,
        session_epoch: -1,
        topics: vec![fetch_request::FetchTopic {
            topic: topic.into(),
            partitions: vec![fetch_request::FetchPartition {
                partition,
                current_leader_epoch: -1,
                fetch_offset: offset,
                partition_max_bytes: 1 << 20,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn records_of(p: &fetch_response::PartitionData) -> Vec<(i64, Vec<u8>)> {
    RecordBatch::decode_all(p.records.as_deref().unwrap_or(&[]), 1 << 24, true)
        .unwrap()
        .into_iter()
        .flat_map(|b| {
            let base = b.base_offset;
            b.records.into_iter().map(move |r| {
                (
                    base + i64::from(r.offset_delta),
                    r.value.unwrap_or_default(),
                )
            })
        })
        .collect()
}

#[tokio::test]
async fn data_plane_apis_are_advertised() {
    let e = env().await;
    let mut c = client(&e.broker).await;
    let r: ApiVersionsResponse = c.call(3, &ApiVersionsRequest::default()).await.unwrap();
    let range = |k: i16| {
        r.api_keys
            .iter()
            .find(|a| a.api_key == k)
            .map(|a| (a.min_version, a.max_version))
    };
    assert_eq!(range(0), Some((3, 9)));
    assert_eq!(range(1), Some((4, 13)));
    assert_eq!(range(2), Some((1, 7)));
    assert_eq!(range(23), Some((2, 4)));
    for k in [24, 25, 26, 28] {
        assert_eq!(range(k), None);
    }
}

#[tokio::test]
async fn produce_every_codec_and_version_then_fetch_every_version() {
    let e = env().await;
    let mut c = client(&e.broker).await;
    let mut expected = Vec::new();
    let mut offset = 0i64;
    for v in 3..=9 {
        for codec in [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            let vals: Vec<String> = (0..3)
                .map(|i| format!("v{v}-{}-{i}", codec.name()))
                .collect();
            let refs: Vec<&str> = vals.iter().map(String::as_str).collect();
            let r = produce(&mut c, v, "shop.events", 1, batch(&refs, codec, None)).await;
            assert_eq!(r.error_code, 0, "v{v} {codec:?}: {:?}", r.error_message);
            assert_eq!(r.base_offset, offset);
            for (i, s) in vals.iter().enumerate() {
                expected.push((offset + i64::try_from(i).unwrap(), s.clone().into_bytes()));
            }
            offset += 3;
        }
    }
    let topic_id = e
        .topics
        .get(&principal(), "shop.events")
        .await
        .unwrap()
        .unwrap()
        .topic_id;
    for v in 4..=13 {
        let mut q = fetch_req("shop.events", 1, 0, 0);
        q.topics[0].topic_id = topic_id;
        let r: FetchResponse = c.call(v, &q).await.unwrap();
        let p = &r.responses[0].partitions[0];
        assert_eq!(p.error_code, 0, "fetch v{v}");
        assert_eq!(p.high_watermark, offset);
        assert_eq!(p.last_stable_offset, offset);
        let got = records_of(p);
        assert_eq!(got, expected, "fetch v{v}");
        if v >= 13 {
            assert_eq!(r.responses[0].topic_id, topic_id);
        }
    }
    // Headers and timestamps come back.
    let r: FetchResponse = c
        .call(11, &fetch_req("shop.events", 1, 4, 0))
        .await
        .unwrap();
    let b = RecordBatch::decode_all(
        r.responses[0].partitions[0].records.as_ref().unwrap(),
        1 << 20,
        true,
    )
    .unwrap();
    assert_eq!(b[0].base_offset, 4);
    assert_eq!(b[0].records[0].headers[0].key, "h");
    assert_eq!(
        b[0].base_timestamp + b[0].records[0].timestamp_delta,
        1_762_000_000_001
    );
}

#[tokio::test]
async fn produce_refusals() {
    let e = env().await;
    let mut c = client(&e.broker).await;
    // A change topic, an unknown topic and partition, a compacted stream
    // without keys, a transactional id.
    let r = produce(
        &mut c,
        9,
        "shop.orders.changes",
        0,
        batch(&["a"], Compression::None, None),
    )
    .await;
    assert_eq!(r.error_code, err::INVALID_TOPIC_EXCEPTION);
    assert!(r.error_message.unwrap().starts_with("39941"));
    let r = produce(
        &mut c,
        9,
        "shop.nope",
        0,
        batch(&["a"], Compression::None, None),
    )
    .await;
    assert_eq!(r.error_code, err::UNKNOWN_TOPIC_OR_PARTITION);
    let r = produce(
        &mut c,
        9,
        "shop.events",
        2,
        batch(&["a"], Compression::None, None),
    )
    .await;
    assert_eq!(r.error_code, err::UNKNOWN_TOPIC_OR_PARTITION);
    let mut keyless =
        RecordBatch::decode_all(&batch(&["a"], Compression::None, None), 1 << 20, false)
            .unwrap()
            .remove(0);
    keyless.records[0].key = None;
    let r = produce(&mut c, 9, "shop.latest", 0, keyless.to_bytes().unwrap()).await;
    assert_eq!(r.error_code, err::INVALID_RECORD);
    assert!(r.error_message.unwrap().starts_with("39955"));
    let mut q = produce_req("shop.events", 0, batch(&["a"], Compression::None, None), -1);
    q.transactional_id = Some("tx".into());
    let r: ProduceResponse = c.call(9, &q).await.unwrap();
    assert_eq!(
        part0(&r).error_code,
        err::TRANSACTIONAL_ID_AUTHORIZATION_FAILED
    );
    let q = produce_req("shop.events", 0, batch(&["a"], Compression::None, None), 2);
    let r: ProduceResponse = c.call(9, &q).await.unwrap();
    assert_eq!(part0(&r).error_code, err::INVALID_REQUIRED_ACKS);
    // acks=0 closes the connection (39950).
    let q = produce_req("shop.events", 0, batch(&["a"], Compression::None, None), 0);
    let r: Result<ProduceResponse, ClientError> = c.call(9, &q).await;
    assert!(r.is_err());
    // Nothing was appended by any of them.
    let mut c = client(&e.broker).await;
    let r: FetchResponse = c
        .call(12, &fetch_req("shop.events", 0, 0, 0))
        .await
        .unwrap();
    assert_eq!(r.responses[0].partitions[0].high_watermark, 0);
}

#[tokio::test]
async fn idempotent_producer_retries_and_sequence_checks() {
    let e = env().await;
    let mut c = client(&e.broker).await;
    let r: InitProducerIdResponse = c
        .call(
            4,
            &InitProducerIdRequest {
                transaction_timeout_ms: -1,
                producer_id: -1,
                producer_epoch: -1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let pid = r.producer_id;
    let b0 = batch(&["a", "b"], Compression::Lz4, Some((pid, 0, 0)));
    let b1 = batch(&["c"], Compression::Snappy, Some((pid, 0, 2)));
    assert_eq!(
        produce(&mut c, 9, "shop.events", 0, b0.clone())
            .await
            .base_offset,
        0
    );
    assert_eq!(
        produce(&mut c, 9, "shop.events", 0, b1.clone())
            .await
            .base_offset,
        2
    );
    // Retries: the original offsets, nothing appended twice.
    let r = produce(&mut c, 9, "shop.events", 0, b0.clone()).await;
    assert_eq!((r.error_code, r.base_offset), (0, 0));
    let r = produce(&mut c, 7, "shop.events", 0, b1.clone()).await;
    assert_eq!((r.error_code, r.base_offset), (0, 2));
    let f: FetchResponse = c
        .call(11, &fetch_req("shop.events", 0, 0, 0))
        .await
        .unwrap();
    let got: Vec<Vec<u8>> = records_of(&f.responses[0].partitions[0])
        .into_iter()
        .map(|(_, v)| v)
        .collect();
    assert_eq!(got, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    // A gap and an older epoch.
    let r = produce(
        &mut c,
        9,
        "shop.events",
        0,
        batch(&["x"], Compression::None, Some((pid, 0, 5))),
    )
    .await;
    assert_eq!(r.error_code, err::OUT_OF_ORDER_SEQUENCE_NUMBER);
    let r = produce(
        &mut c,
        9,
        "shop.events",
        0,
        batch(&["y"], Compression::None, Some((pid, 1, 0))),
    )
    .await;
    assert_eq!((r.error_code, r.base_offset), (0, 3));
    let r = produce(
        &mut c,
        9,
        "shop.events",
        0,
        batch(&["z"], Compression::None, Some((pid, 0, 3))),
    )
    .await;
    assert_eq!(r.error_code, err::INVALID_PRODUCER_EPOCH);
    // Sequences are per partition.
    let r = produce(
        &mut c,
        9,
        "shop.events",
        1,
        batch(&["p1"], Compression::None, Some((pid, 1, 0))),
    )
    .await;
    assert_eq!((r.error_code, r.base_offset), (0, 0));
}

#[tokio::test]
async fn pipelined_produces_wait_together_and_answer_in_order() {
    let e = env().await;
    let mut c = client(&e.broker).await;
    e.data.hold(true);
    let mut ids = Vec::new();
    for i in 0..5 {
        let b = batch(&[&format!("p{i}")], Compression::None, Some((99, 0, i)));
        ids.push(
            c.send(9, &produce_req("shop.events", 0, b, -1))
                .await
                .unwrap(),
        );
    }
    // A metadata request behind them waits its turn.
    let meta = c.send(12, &MetadataRequest::default()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let t0 = Instant::now();
    e.data.hold(false);
    for (i, id) in ids.into_iter().enumerate() {
        let r: ProduceResponse = c.recv(9, id).await.unwrap();
        assert_eq!(part0(&r).error_code, 0);
        assert_eq!(part0(&r).base_offset, i64::try_from(i).unwrap());
    }
    let _: MetadataResponse = c.recv(12, meta).await.unwrap();
    assert!(t0.elapsed() < Duration::from_millis(400));
}

#[tokio::test]
async fn a_sequencing_timeout_is_retryable_and_the_retry_gets_the_offsets() {
    let e = env().await;
    let mut c = client(&e.broker).await;
    e.data.hold(true);
    let b = batch(&["slow"], Compression::None, Some((5, 0, 0)));
    let r = produce(&mut c, 9, "shop.events", 1, b.clone()).await;
    assert_eq!(r.error_code, err::REQUEST_TIMED_OUT);
    assert!(r.error_message.unwrap().starts_with("39946"));
    e.data.hold(false);
    let r = produce(&mut c, 9, "shop.events", 1, b).await;
    assert_eq!((r.error_code, r.base_offset), (0, 0));
    let f: FetchResponse = c
        .call(12, &fetch_req("shop.events", 1, 0, 0))
        .await
        .unwrap();
    assert_eq!(f.responses[0].partitions[0].high_watermark, 1);
}

#[tokio::test]
async fn fetch_long_polls_until_records_arrive() {
    let e = env().await;
    let mut c = client(&e.broker).await;
    let mut p = client(&e.broker).await;
    // Nothing: the full wait (capped by the limit, 500 ms here).
    let t0 = Instant::now();
    let r: FetchResponse = c
        .call(12, &fetch_req("shop.events", 0, 0, 2000))
        .await
        .unwrap();
    assert!(
        t0.elapsed() >= Duration::from_millis(450),
        "{:?}",
        t0.elapsed()
    );
    assert!(records_of(&r.responses[0].partitions[0]).is_empty());
    // A record produced meanwhile ends the wait early.
    let id = c
        .send(12, &fetch_req("shop.events", 0, 0, 2000))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let t0 = Instant::now();
    produce(
        &mut p,
        9,
        "shop.events",
        0,
        batch(&["wake"], Compression::None, None),
    )
    .await;
    let r: FetchResponse = c.recv(12, id).await.unwrap();
    assert!(
        t0.elapsed() < Duration::from_millis(300),
        "{:?}",
        t0.elapsed()
    );
    assert_eq!(
        records_of(&r.responses[0].partitions[0]),
        vec![(0, b"wake".to_vec())]
    );
    // Out of range.
    let r: FetchResponse = c
        .call(12, &fetch_req("shop.events", 0, 5, 0))
        .await
        .unwrap();
    assert_eq!(
        r.responses[0].partitions[0].error_code,
        err::OFFSET_OUT_OF_RANGE
    );
    assert_eq!(r.responses[0].partitions[0].high_watermark, 1);
    // An unknown topic id (v13).
    let mut q = fetch_req("", 0, 0, 0);
    q.topics[0].topic_id = uuid::Uuid::from_u64_pair(1, 2);
    let r: FetchResponse = c.call(13, &q).await.unwrap();
    assert_eq!(
        r.responses[0].partitions[0].error_code,
        err::UNKNOWN_TOPIC_ID
    );
}

#[tokio::test]
async fn list_offsets_and_leader_epochs() {
    let e = env().await;
    let mut c = client(&e.broker).await;
    let mut b = RecordBatch::decode_all(
        &batch(&["a", "b", "c"], Compression::None, None),
        1 << 20,
        false,
    )
    .unwrap()
    .remove(0);
    for (i, r) in b.records.iter_mut().enumerate() {
        r.timestamp_delta = i64::try_from(i).unwrap() * 1000;
    }
    produce(&mut c, 9, "shop.events", 0, b.to_bytes().unwrap()).await;
    e.data.retain_from(TENANT, 1, 0, 1);
    let lo = |ts: i64, epoch: i32| ListOffsetsRequest {
        replica_id: -1,
        topics: vec![list_offsets_request::ListOffsetsTopic {
            name: "shop.events".into(),
            partitions: vec![list_offsets_request::ListOffsetsPartition {
                partition_index: 0,
                current_leader_epoch: epoch,
                timestamp: ts,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let one = |r: &ListOffsetsResponse| r.topics[0].partitions[0].clone();
    for v in 1..=7 {
        let r: ListOffsetsResponse = c.call(v, &lo(-2, -1)).await.unwrap();
        assert_eq!(one(&r).offset, 1, "earliest v{v}");
        let r: ListOffsetsResponse = c.call(v, &lo(-1, -1)).await.unwrap();
        assert_eq!(one(&r).offset, 3, "latest v{v}");
        let r: ListOffsetsResponse = c.call(v, &lo(1_762_000_001_500, -1)).await.unwrap();
        assert_eq!(
            (one(&r).offset, one(&r).timestamp),
            (2, 1_762_000_002_000),
            "by time v{v}"
        );
        let r: ListOffsetsResponse = c.call(v, &lo(1_900_000_000_000, -1)).await.unwrap();
        assert_eq!(one(&r).offset, -1, "after the last v{v}");
    }
    let r: ListOffsetsResponse = c.call(7, &lo(-3, -1)).await.unwrap();
    assert_eq!(one(&r).offset, 2, "max timestamp");
    // A failover: the epoch rises, and the tail from offset 2 is lost;
    // a new record takes offset 2 in the new epoch.
    e.data.failover(TENANT, 1, Some(2));
    let r: ListOffsetsResponse = c.call(7, &lo(-1, 0)).await.unwrap();
    assert_eq!(one(&r).error_code, err::FENCED_LEADER_EPOCH);
    let r: ListOffsetsResponse = c.call(7, &lo(-1, 5)).await.unwrap();
    assert_eq!(one(&r).error_code, err::UNKNOWN_LEADER_EPOCH);
    produce(
        &mut c,
        9,
        "shop.events",
        0,
        batch(&["new"], Compression::None, None),
    )
    .await;
    let ofle = |epoch: i32| OffsetForLeaderEpochRequest {
        replica_id: -1,
        topics: vec![offset_for_leader_epoch_request::OffsetForLeaderTopic {
            topic: "shop.events".into(),
            partitions: vec![offset_for_leader_epoch_request::OffsetForLeaderPartition {
                partition: 0,
                current_leader_epoch: 1,
                leader_epoch: epoch,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    for v in 2..=4 {
        // A consumer that read up to offset 3 in epoch 0 learns that epoch
        // 0 ends at 2: its offsets 2.. were truncated.
        let r: OffsetForLeaderEpochResponse = c.call(v, &ofle(0)).await.unwrap();
        let p = &r.topics[0].partitions[0];
        assert_eq!(
            (p.error_code, p.leader_epoch, p.end_offset),
            (0, 0, 2),
            "v{v}"
        );
        let r: OffsetForLeaderEpochResponse = c.call(v, &ofle(1)).await.unwrap();
        let p = &r.topics[0].partitions[0];
        assert_eq!((p.leader_epoch, p.end_offset), (1, 3), "v{v}");
    }
    // Fetch with the old epoch is fenced; the batch carries its epoch.
    let mut q = fetch_req("shop.events", 0, 2, 0);
    q.topics[0].partitions[0].current_leader_epoch = 0;
    let r: FetchResponse = c.call(12, &q).await.unwrap();
    assert_eq!(
        r.responses[0].partitions[0].error_code,
        err::FENCED_LEADER_EPOCH
    );
    q.topics[0].partitions[0].current_leader_epoch = 1;
    let r: FetchResponse = c.call(12, &q).await.unwrap();
    let p = &r.responses[0].partitions[0];
    let b = RecordBatch::decode_all(p.records.as_ref().unwrap(), 1 << 20, true).unwrap();
    assert_eq!(b[0].partition_leader_epoch, 1);
    // Metadata v7+ shows the epoch.
    let m: MetadataResponse = c
        .call(
            12,
            &MetadataRequest {
                topics: None,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let t = m
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some("shop.events"))
        .unwrap();
    let p0 = t
        .partitions
        .iter()
        .find(|p| p.partition_index == 0)
        .unwrap();
    assert_eq!(p0.leader_epoch, 1);
}
