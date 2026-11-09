//! Live test against a running gateway's `kafka` listener (chapter 45
//! steps 9, 12 and 13), with this crate's own client: no Kafka client
//! library is installed on the build machines.
//!
//! Ignored by default. Run with
//!
//! ```text
//! SPRIG_KAFKA_ADDR=127.0.0.1:9092 SPRIG_KAFKA_CA=deploy/compose/gateway-certs/ca.pem \
//! SPRIG_KAFKA_USER=acme.app SPRIG_KAFKA_PASSWORD=... SPRIG_KAFKA_DB=<an existing database> \
//! cargo test -p sprig-proto-kafka --test live -- --ignored --nocapture
//! ```
//!
//! The listener must have `listener.kafka.tls = optional` (the test uses
//! both plaintext and TLS). With `SPRIG_KAFKA_SCRAM=1` the user's password
//! was set by a build that stores SCRAM verifiers (chapter 14), and
//! SCRAM-SHA-256 and SCRAM-SHA-512 must both succeed.
//!
//! `live_scram_upgrade_on_login` checks chapter 14's upgrade on login: with
//! `SPRIG_KAFKA_LEGACY_USER` and `SPRIG_KAFKA_LEGACY_PASSWORD` naming an
//! account whose SCRAM verifiers were deleted, SCRAM fails, a PLAIN login
//! over TLS succeeds, and SCRAM then succeeds with both mechanisms.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::many_single_char_names
)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_proto_kafka::client::{Client, ClientError, assignment, parse_assignment, subscription};
use sprig_proto_kafka::messages::*;
use sprig_proto_kafka::protocol::err;
use sprig_proto_kafka::sasl::ScramMechanism;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

async fn connect(tls: bool) -> Client<Box<dyn Io>> {
    let addr = env("SPRIG_KAFKA_ADDR").unwrap();
    let tcp = TcpStream::connect(&addr).await.unwrap();
    if !tls {
        return Client::new(Box::new(tcp), "r9kafka-live");
    }
    let mut roots = rustls::RootCertStore::empty();
    let pem = std::fs::read(env("SPRIG_KAFKA_CA").unwrap()).unwrap();
    for c in rustls_pemfile::certs(&mut pem.as_slice()) {
        roots.add(c.unwrap()).unwrap();
    }
    let cfg = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let s = tokio_rustls::TlsConnector::from(Arc::new(cfg))
        .connect(name, tcp)
        .await
        .unwrap();
    Client::new(Box::new(s), "r9kafka-live")
}

async fn login() -> Client<Box<dyn Io>> {
    let mut c = connect(true).await;
    c.sasl_plain(
        &env("SPRIG_KAFKA_USER").unwrap(),
        &env("SPRIG_KAFKA_PASSWORD").unwrap(),
    )
    .await
    .unwrap();
    c
}

fn join_req(group: &str, member: &str, topic: &str) -> JoinGroupRequest {
    JoinGroupRequest {
        group_id: group.into(),
        session_timeout_ms: 10_000,
        rebalance_timeout_ms: 20_000,
        member_id: member.into(),
        protocol_type: "consumer".into(),
        protocols: vec![join_group_request::JoinGroupRequestProtocol {
            name: "range".into(),
            metadata: subscription(&[topic]),
            ..Default::default()
        }],
        ..Default::default()
    }
}

async fn join(
    c: &mut Client<Box<dyn Io>>,
    group: &str,
    member: &str,
    topic: &str,
) -> JoinGroupResponse {
    let r: JoinGroupResponse = c.call(9, &join_req(group, member, topic)).await.unwrap();
    if r.error_code == err::MEMBER_ID_REQUIRED {
        return c
            .call(9, &join_req(group, &r.member_id, topic))
            .await
            .unwrap();
    }
    r
}

async fn sync(
    c: &mut Client<Box<dyn Io>>,
    group: &str,
    j: &JoinGroupResponse,
    plan: Vec<(String, Vec<u8>)>,
) -> SyncGroupResponse {
    c.call(
        5,
        &SyncGroupRequest {
            group_id: group.into(),
            generation_id: j.generation_id,
            member_id: j.member_id.clone(),
            protocol_type: Some("consumer".into()),
            protocol_name: j.protocol_name.clone(),
            assignments: plan
                .into_iter()
                .map(|(m, a)| sync_group_request::SyncGroupRequestAssignment {
                    member_id: m,
                    assignment: a,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

async fn heartbeat(c: &mut Client<Box<dyn Io>>, group: &str, j: &JoinGroupResponse) -> i16 {
    let r: HeartbeatResponse = c
        .call(
            4,
            &HeartbeatRequest {
                group_id: group.into(),
                generation_id: j.generation_id,
                member_id: j.member_id.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    r.error_code
}

fn metadata_for(topic: &str) -> MetadataRequest {
    MetadataRequest {
        topics: Some(vec![metadata_request::MetadataRequestTopic {
            name: Some(topic.into()),
            ..Default::default()
        }]),
        ..Default::default()
    }
}

#[tokio::test]
#[ignore = "needs a running gateway with the kafka listener"]
async fn live_kafka_front_end() {
    let db = env("SPRIG_KAFKA_DB").unwrap();
    let topic = format!("{db}.r9kafka_events");
    let group = "r9kafka_billing";
    let t0 = Instant::now();

    // ApiVersions before login.
    let mut c = connect(false).await;
    let started = Instant::now();
    let r: ApiVersionsResponse = c
        .call(
            3,
            &ApiVersionsRequest {
                client_software_name: "r9kafka-live".into(),
                client_software_version: "0.1".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    println!(
        "ApiVersions v3: {} APIs in {:?}",
        r.api_keys.len(),
        started.elapsed()
    );
    assert_eq!(r.error_code, 0);
    assert!(
        r.api_keys
            .iter()
            .any(|k| k.api_key == 3 && k.max_version == 12)
    );
    // The data plane (steps 10 and 11) is advertised.
    assert!(
        r.api_keys
            .iter()
            .any(|k| k.api_key == 0 && k.max_version == 9)
    );

    // SASL: PLAIN isn't offered in plaintext; wrong passwords fail.
    match c.sasl_plain(&env("SPRIG_KAFKA_USER").unwrap(), "x").await {
        Err(ClientError::Refused(code, _)) => assert_eq!(code, err::UNSUPPORTED_SASL_MECHANISM),
        other => panic!("{other:?}"),
    }
    let mut bad = connect(true).await;
    match bad
        .sasl_plain(&env("SPRIG_KAFKA_USER").unwrap(), "wrong-password")
        .await
    {
        Err(ClientError::Refused(58, m)) => println!("PLAIN with a wrong password: 58 {m}"),
        other => panic!("{other:?}"),
    }
    for mech in [ScramMechanism::Sha256, ScramMechanism::Sha512] {
        let mut s = connect(false).await;
        let r = s
            .sasl_scram(
                mech,
                &env("SPRIG_KAFKA_USER").unwrap(),
                &env("SPRIG_KAFKA_PASSWORD").unwrap(),
            )
            .await;
        println!(
            "{} in plaintext: {:?}",
            mech.name(),
            r.as_ref().map(|()| "ok")
        );
        if env("SPRIG_KAFKA_SCRAM").is_some() {
            r.unwrap();
            let m: MetadataResponse = s.call(12, &MetadataRequest::default()).await.unwrap();
            assert!(!m.brokers.is_empty());
        } else {
            assert!(matches!(r, Err(ClientError::Refused(58, _))));
        }
    }
    let started = Instant::now();
    let mut c = login().await;
    println!("PLAIN over TLS: {:?}", started.elapsed());

    // Metadata: this gateway as a broker.
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
    println!(
        "brokers: {:?}, cluster {:?}",
        m.brokers
            .iter()
            .map(|b| (b.node_id, &b.host, b.port))
            .collect::<Vec<_>>(),
        m.cluster_id
    );
    assert!(!m.brokers.is_empty());

    // CreateTopics: a stream through stream DDL.
    let started = Instant::now();
    let r: CreateTopicsResponse = c
        .call(
            7,
            &CreateTopicsRequest {
                topics: vec![create_topics_request::CreatableTopic {
                    name: topic.clone(),
                    num_partitions: 6,
                    replication_factor: -1,
                    configs: vec![create_topics_request::CreatableTopicConfig {
                        name: "retention.ms".into(),
                        value: Some("172800000".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                timeout_ms: 30_000,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    println!("CreateTopics: {:?} in {:?}", r.topics[0], started.elapsed());
    assert_eq!(r.topics[0].error_code, 0, "{:?}", r.topics[0].error_message);
    assert_eq!(r.topics[0].num_partitions, 6);
    let r: CreateTopicsResponse = c
        .call(
            7,
            &CreateTopicsRequest {
                topics: vec![create_topics_request::CreatableTopic {
                    name: topic.clone(),
                    num_partitions: 6,
                    replication_factor: -1,
                    ..Default::default()
                }],
                timeout_ms: 30_000,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.topics[0].error_code, err::TOPIC_ALREADY_EXISTS);
    let m: MetadataResponse = c.call(12, &metadata_for(&topic)).await.unwrap();
    let t = &m.topics[0];
    assert_eq!(t.error_code, 0);
    assert_eq!(t.partitions.len(), 6);
    println!(
        "metadata: topic id {}, leaders {:?}",
        t.topic_id,
        t.partitions.iter().map(|p| p.leader_id).collect::<Vec<_>>()
    );

    // Configs and partitions.
    let r: DescribeConfigsResponse = c
        .call(
            4,
            &DescribeConfigsRequest {
                resources: vec![describe_configs_request::DescribeConfigsResource {
                    resource_type: 2,
                    resource_name: topic.clone(),
                    configuration_keys: None,
                    ..Default::default()
                }],
                include_synonyms: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    for cfg in &r.results[0].configs {
        println!(
            "  {} = {:?} (source {})",
            cfg.name, cfg.value, cfg.config_source
        );
    }
    assert!(
        r.results[0]
            .configs
            .iter()
            .any(|c| c.name == "retention.ms" && c.value.as_deref() == Some("172800000"))
    );
    let r: IncrementalAlterConfigsResponse = c
        .call(
            1,
            &IncrementalAlterConfigsRequest {
                resources: vec![incremental_alter_configs_request::AlterConfigsResource {
                    resource_type: 2,
                    resource_name: topic.clone(),
                    configs: vec![incremental_alter_configs_request::AlterableConfig {
                        name: "retention.ms".into(),
                        config_operation: 0,
                        value: Some("259200000".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    println!("IncrementalAlterConfigs: {:?}", r.responses[0]);
    assert_eq!(r.responses[0].error_code, 0);
    let r: CreatePartitionsResponse = c
        .call(
            3,
            &CreatePartitionsRequest {
                topics: vec![create_partitions_request::CreatePartitionsTopic {
                    name: topic.clone(),
                    count: 8,
                    assignments: None,
                    ..Default::default()
                }],
                timeout_ms: 30_000,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    println!("CreatePartitions: {:?}", r.results[0]);
    assert_eq!(r.results[0].error_code, 0);
    let m: MetadataResponse = c.call(9, &metadata_for(&topic)).await.unwrap();
    assert_eq!(m.topics[0].partitions.len(), 8);

    // A consumer group of two members, with a rebalance.
    let fc: FindCoordinatorResponse = c
        .call(
            4,
            &FindCoordinatorRequest {
                key_type: 0,
                coordinator_keys: vec![group.into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    println!("coordinator: {:?}", fc.coordinators[0]);
    assert_eq!(fc.coordinators[0].error_code, 0);
    let mut c1 = login().await;
    let started = Instant::now();
    let j1 = join(&mut c1, group, "", &topic).await;
    println!(
        "member 1 joined in {:?} (generation {}, leader {})",
        started.elapsed(),
        j1.generation_id,
        j1.leader == j1.member_id
    );
    assert_eq!(j1.error_code, 0);
    let all: Vec<i32> = (0..8).collect();
    let s1 = sync(
        &mut c1,
        group,
        &j1,
        vec![(j1.member_id.clone(), assignment(&[(&topic, all.clone())]))],
    )
    .await;
    assert_eq!(s1.error_code, 0);
    let topic2 = topic.clone();
    let t2 = tokio::spawn(async move {
        let mut c2 = login().await;
        let j2 = join(&mut c2, "r9kafka_billing", "", &topic2).await;
        (c2, j2)
    });
    let started = Instant::now();
    let mut hb = 0;
    while started.elapsed() < Duration::from_secs(10) {
        hb = heartbeat(&mut c1, group, &j1).await;
        if hb == err::REBALANCE_IN_PROGRESS {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(hb, err::REBALANCE_IN_PROGRESS);
    let j1b = join(&mut c1, group, &j1.member_id, &topic).await;
    let (mut c2, j2) = t2.await.unwrap();
    println!(
        "rebalance done in {:?}: generation {} -> {}",
        started.elapsed(),
        j1.generation_id,
        j1b.generation_id
    );
    assert_eq!(j1b.error_code, 0);
    assert_eq!(j2.generation_id, j1b.generation_id);
    assert_eq!(j1b.members.len(), 2);
    let mut ids: Vec<String> = j1b.members.iter().map(|m| m.member_id.clone()).collect();
    ids.sort();
    let plan = vec![
        (ids[0].clone(), assignment(&[(&topic, vec![0, 1, 2, 3])])),
        (ids[1].clone(), assignment(&[(&topic, vec![4, 5, 6, 7])])),
    ];
    let jj2 = j2.clone();
    let t2 = tokio::spawn(async move {
        let s = sync(&mut c2, "r9kafka_billing", &jj2, Vec::new()).await;
        (c2, s)
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let s1 = sync(&mut c1, group, &j1b, plan).await;
    let (mut c2, s2) = t2.await.unwrap();
    let a1 = parse_assignment(&s1.assignment).unwrap();
    let a2 = parse_assignment(&s2.assignment).unwrap();
    println!("assignments: {a1:?} / {a2:?}");
    assert_eq!(a1[0].1.len() + a2[0].1.len(), 8);

    // Offsets into stream_offsets.
    let r: OffsetCommitResponse = c2
        .call(
            8,
            &OffsetCommitRequest {
                group_id: group.into(),
                generation_id_or_member_epoch: j2.generation_id,
                member_id: j2.member_id.clone(),
                topics: vec![offset_commit_request::OffsetCommitRequestTopic {
                    name: topic.clone(),
                    partitions: vec![offset_commit_request::OffsetCommitRequestPartition {
                        partition_index: a2[0].1[0],
                        committed_offset: 1206,
                        committed_leader_epoch: 0,
                        committed_metadata: Some("r9kafka".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.topics[0].partitions[0].error_code, 0);
    let r: OffsetFetchResponse = c1
        .call(
            8,
            &OffsetFetchRequest {
                groups: vec![offset_fetch_request::OffsetFetchRequestGroup {
                    group_id: group.into(),
                    topics: None,
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    println!("OffsetFetch: {:?}", r.groups[0].topics);
    assert_eq!(r.groups[0].topics[0].partitions[0].committed_offset, 1206);

    let r: DescribeGroupsResponse = c
        .call(
            5,
            &DescribeGroupsRequest {
                groups: vec![group.into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    println!(
        "DescribeGroups: state {}, {} members",
        r.groups[0].group_state,
        r.groups[0].members.len()
    );
    assert_eq!(r.groups[0].group_state, "Stable");
    let r: ListGroupsResponse = c.call(4, &ListGroupsRequest::default()).await.unwrap();
    assert!(r.groups.iter().any(|g| g.group_id == group));

    // Both leave; the group empties and is deleted with its offsets.
    for (cl, j) in [(&mut c1, &j1b), (&mut c2, &j2)] {
        let r: LeaveGroupResponse = cl
            .call(
                5,
                &LeaveGroupRequest {
                    group_id: group.into(),
                    members: vec![leave_group_request::MemberIdentity {
                        member_id: j.member_id.clone(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(r.error_code, 0);
    }
    let r: DeleteGroupsResponse = c
        .call(
            2,
            &DeleteGroupsRequest {
                groups_names: vec![group.into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    println!("DeleteGroups: {:?}", r.results[0]);
    assert_eq!(r.results[0].error_code, 0);

    // DeleteTopics: DROP STREAM.
    if env("SPRIG_KAFKA_KEEP").is_none() {
        let r: DeleteTopicsResponse = c
            .call(
                6,
                &DeleteTopicsRequest {
                    topics: vec![delete_topics_request::DeleteTopicState {
                        name: Some(topic.clone()),
                        ..Default::default()
                    }],
                    timeout_ms: 30_000,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        println!("DeleteTopics: {:?}", r.responses[0]);
        assert_eq!(r.responses[0].error_code, 0);
        let m: MetadataResponse = c.call(12, &metadata_for(&topic)).await.unwrap();
        assert_eq!(m.topics[0].error_code, err::UNKNOWN_TOPIC_OR_PARTITION);
    }
    println!("live run: {:?}", t0.elapsed());
}

async fn scram_login(mech: ScramMechanism, user: &str, password: &str) -> Result<(), ClientError> {
    let mut s = connect(false).await;
    s.sasl_scram(mech, user, password).await?;
    let m: MetadataResponse = s.call(12, &MetadataRequest::default()).await.unwrap();
    assert!(!m.brokers.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "needs a running gateway with the kafka listener"]
async fn live_scram_upgrade_on_login() {
    let (Some(user), Some(pw)) = (
        env("SPRIG_KAFKA_LEGACY_USER"),
        env("SPRIG_KAFKA_LEGACY_PASSWORD"),
    ) else {
        println!("skipped: set SPRIG_KAFKA_LEGACY_USER and SPRIG_KAFKA_LEGACY_PASSWORD");
        return;
    };
    // No SCRAM verifiers yet: both mechanisms fail at the proof.
    for mech in [ScramMechanism::Sha256, ScramMechanism::Sha512] {
        match scram_login(mech, &user, &pw).await {
            Err(ClientError::Refused(58, m)) => println!("{} before: 58 {m}", mech.name()),
            other => panic!("{} before the upgrade: {other:?}", mech.name()),
        }
    }
    // A PLAIN login over TLS sees the password and stores them.
    let mut c = connect(true).await;
    let t0 = Instant::now();
    c.sasl_plain(&user, &pw).await.unwrap();
    println!("PLAIN over TLS: {:?}", t0.elapsed());
    for mech in [ScramMechanism::Sha512, ScramMechanism::Sha256] {
        loop {
            match scram_login(mech, &user, &pw).await {
                Ok(()) => break,
                Err(ClientError::Refused(58, _)) if t0.elapsed() < Duration::from_secs(10) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                other => panic!("{} after the upgrade: {other:?}", mech.name()),
            }
        }
        println!(
            "{} works {:?} after the PLAIN login",
            mech.name(),
            t0.elapsed()
        );
    }
    // A wrong password still fails.
    assert!(matches!(
        scram_login(ScramMechanism::Sha512, &user, "wrong-password").await,
        Err(ClientError::Refused(58, _))
    ));
}

/// Chapter 14's dual passwords over SCRAM: with `SPRIG_KAFKA_DUAL_USER` and
/// `SPRIG_KAFKA_DUAL_PASSWORDS` (`new,old`) naming an account changed with
/// `RETAIN CURRENT PASSWORD`, both passwords log in with both mechanisms.
#[tokio::test]
#[ignore = "needs a running gateway with the kafka listener"]
async fn live_scram_dual_passwords() {
    let (Some(user), Some(pws)) = (
        env("SPRIG_KAFKA_DUAL_USER"),
        env("SPRIG_KAFKA_DUAL_PASSWORDS"),
    ) else {
        println!("skipped: set SPRIG_KAFKA_DUAL_USER and SPRIG_KAFKA_DUAL_PASSWORDS");
        return;
    };
    for pw in pws.split(',') {
        for mech in [ScramMechanism::Sha256, ScramMechanism::Sha512] {
            scram_login(mech, &user, pw).await.unwrap();
            println!("{} with {pw}: ok", mech.name());
        }
    }
    assert!(matches!(
        scram_login(ScramMechanism::Sha256, &user, "neither").await,
        Err(ClientError::Refused(58, _))
    ));
}

// ---- The data plane (chapter 45 build steps 10 and 11) ----

use sprig_proto_kafka::compression::Compression;
use sprig_proto_kafka::records::{Header, Record, RecordBatch};

fn data_batch(
    first: usize,
    n: usize,
    size: usize,
    codec: Compression,
    producer: Option<(i64, i16, i32)>,
) -> Vec<u8> {
    let (pid, epoch, seq) = producer.unwrap_or((-1, -1, -1));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    RecordBatch {
        attributes: i16::from(codec.id()),
        last_offset_delta: n as i32 - 1,
        base_timestamp: now,
        max_timestamp: now,
        producer_id: pid,
        producer_epoch: epoch,
        base_sequence: seq,
        records: (0..n)
            .map(|i| {
                let mut v = format!("r{}-", first + i).into_bytes();
                v.resize(size.max(v.len()), b'x');
                Record {
                    offset_delta: i as i32,
                    key: Some(format!("k{}", (first + i) % 97).into_bytes()),
                    value: Some(v),
                    headers: vec![Header {
                        key: "src".into(),
                        value: Some(b"live".to_vec()),
                    }],
                    ..Record::default()
                }
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
        timeout_ms: 10_000,
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

async fn produce_one(
    c: &mut Client<Box<dyn Io>>,
    v: i16,
    topic: &str,
    partition: i32,
    records: Vec<u8>,
) -> produce_response::PartitionProduceResponse {
    let r: ProduceResponse = c
        .call(v, &produce_req(topic, partition, records, -1))
        .await
        .unwrap();
    r.responses[0].partition_responses[0].clone()
}

fn fetch_req(topic: &str, partition: i32, offset: i64, max_wait_ms: i32) -> FetchRequest {
    FetchRequest {
        replica_id: -1,
        max_wait_ms,
        min_bytes: 1,
        max_bytes: 50 << 20,
        isolation_level: 1,
        session_epoch: -1,
        topics: vec![fetch_request::FetchTopic {
            topic: topic.into(),
            partitions: vec![fetch_request::FetchPartition {
                partition,
                current_leader_epoch: -1,
                fetch_offset: offset,
                partition_max_bytes: 4 << 20,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn values_of(p: &fetch_response::PartitionData) -> Vec<(i64, Vec<u8>)> {
    RecordBatch::decode_all(p.records.as_deref().unwrap_or(&[]), 1 << 26, true)
        .unwrap()
        .into_iter()
        .flat_map(|b| {
            let base = b.base_offset;
            b.records
                .into_iter()
                .map(move |r| (base + i64::from(r.offset_delta), r.value.unwrap_or_default()))
        })
        .collect()
}

async fn create_topic(c: &mut Client<Box<dyn Io>>, topic: &str, partitions: i32) {
    let r: CreateTopicsResponse = c
        .call(
            7,
            &CreateTopicsRequest {
                topics: vec![create_topics_request::CreatableTopic {
                    name: topic.into(),
                    num_partitions: partitions,
                    replication_factor: -1,
                    ..Default::default()
                }],
                timeout_ms: 30_000,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.topics[0].error_code, 0, "{:?}", r.topics[0].error_message);
}

async fn delete_topic(c: &mut Client<Box<dyn Io>>, topic: &str) {
    let r: DeleteTopicsResponse = c
        .call(
            6,
            &DeleteTopicsRequest {
                topics: vec![delete_topics_request::DeleteTopicState {
                    name: Some(topic.into()),
                    ..Default::default()
                }],
                timeout_ms: 30_000,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    println!("DeleteTopics {topic}: {}", r.responses[0].error_code);
}

async fn alter_config(c: &mut Client<Box<dyn Io>>, topic: &str, name: &str, value: &str) -> i16 {
    let r: IncrementalAlterConfigsResponse = c
        .call(
            1,
            &IncrementalAlterConfigsRequest {
                resources: vec![incremental_alter_configs_request::AlterConfigsResource {
                    resource_type: 2,
                    resource_name: topic.into(),
                    configs: vec![incremental_alter_configs_request::AlterableConfig {
                        name: name.into(),
                        config_operation: 0,
                        value: Some(value.into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    println!("alter {name}={value}: {:?}", r.responses[0]);
    r.responses[0].error_code
}

/// One record through an idempotent producer of its own, retried while the
/// new stream's sequencer starts (a retry appends nothing); returns the
/// partition's next offset.
async fn warm_up(c: &mut Client<Box<dyn Io>>, topic: &str, partition: i32) -> i64 {
    let ip: InitProducerIdResponse = c
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
    let t0 = Instant::now();
    loop {
        let b = data_batch(0, 1, 10, Compression::None, Some((ip.producer_id, 0, 0)));
        let r = produce_one(c, 9, topic, partition, b).await;
        if r.error_code == 0 {
            println!(
                "warm-up of partition {partition}: offset {} after {:?}",
                r.base_offset,
                t0.elapsed()
            );
            return r.base_offset + 1;
        }
        assert_eq!(r.error_code, err::REQUEST_TIMED_OUT, "{:?}", r.error_message);
        assert!(t0.elapsed() < Duration::from_secs(60), "no sequencer");
    }
}

#[tokio::test]
#[ignore = "needs a running gateway with the kafka listener and a sequencer"]
async fn live_data_plane() {
    let db = env("SPRIG_KAFKA_DB").unwrap();
    let topic = format!("{db}.r9k2_data_{}", std::process::id());
    // A failed SCRAM login says a password was used.
    let user = env("SPRIG_KAFKA_USER").unwrap();
    match scram_login(ScramMechanism::Sha512, &user, "not-the-password").await {
        Err(ClientError::Refused(58, m)) => {
            println!("SCRAM with a wrong password: {m}");
            assert!(m.contains("(using password: YES)"), "{m}");
        }
        other => panic!("{other:?}"),
    }
    let mut c = login().await;
    let av: ApiVersionsResponse = c.call(3, &ApiVersionsRequest::default()).await.unwrap();
    for (k, lo, hi) in [(0, 3, 9), (1, 4, 13), (2, 1, 7), (23, 2, 4)] {
        let a = av.api_keys.iter().find(|a| a.api_key == k).unwrap();
        assert_eq!((a.min_version, a.max_version), (lo, hi), "api {k}");
    }
    create_topic(&mut c, &topic, 2).await;
    let start0 = warm_up(&mut c, &topic, 0).await;
    let start1 = warm_up(&mut c, &topic, 1).await;

    // Every codec, at the oldest and newest produce versions.
    let mut want = Vec::new();
    let mut next = start0;
    for v in [3, 7, 9] {
        for codec in [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            let t0 = Instant::now();
            let r = produce_one(&mut c, v, &topic, 0, data_batch(want.len(), 4, 40, codec, None)).await;
            assert_eq!(r.error_code, 0, "v{v} {codec:?}: {:?}", r.error_message);
            assert_eq!(r.base_offset, next, "v{v} {codec:?}");
            println!("produce v{v} {:>6}: base offset {} in {:?}", codec.name(), r.base_offset, t0.elapsed());
            next += 4;
            for i in 0..4 {
                let mut v = format!("r{}-", want.len()).into_bytes();
                v.resize(40, b'x');
                let _ = i;
                want.push(v);
            }
        }
    }
    for v in [4, 11, 12, 13] {
        let mut q = fetch_req(&topic, 0, start0, 0);
        if v >= 13 {
            let m: MetadataResponse = c.call(12, &metadata_for(&topic)).await.unwrap();
            q.topics[0].topic_id = m.topics[0].topic_id;
        }
        let r: FetchResponse = c.call(v, &q).await.unwrap();
        let p = &r.responses[0].partitions[0];
        assert_eq!(p.error_code, 0, "fetch v{v}");
        assert_eq!(p.high_watermark, next);
        let got: Vec<Vec<u8>> = values_of(p).into_iter().map(|(_, v)| v).collect();
        assert_eq!(got, want, "fetch v{v}");
    }
    println!("fetch v4..13: {} records in order", want.len());

    // Idempotence: retries return the original offsets and append nothing.
    let ip: InitProducerIdResponse = c
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
    let pid = ip.producer_id;
    let b0 = data_batch(1000, 3, 10, Compression::Lz4, Some((pid, 0, 0)));
    let b1 = data_batch(1003, 2, 10, Compression::Snappy, Some((pid, 0, 3)));
    let r0 = produce_one(&mut c, 9, &topic, 1, b0.clone()).await;
    let r1 = produce_one(&mut c, 9, &topic, 1, b1.clone()).await;
    assert_eq!((r0.error_code, r0.base_offset), (0, start1));
    assert_eq!((r1.error_code, r1.base_offset), (0, start1 + 3));
    let d0 = produce_one(&mut c, 9, &topic, 1, b0).await;
    let d1 = produce_one(&mut c, 9, &topic, 1, b1).await;
    assert_eq!((d0.error_code, d0.base_offset), (0, start1));
    assert_eq!((d1.error_code, d1.base_offset), (0, start1 + 3));
    let gap = produce_one(&mut c, 9, &topic, 1, data_batch(0, 1, 10, Compression::None, Some((pid, 0, 9)))).await;
    assert_eq!(gap.error_code, err::OUT_OF_ORDER_SEQUENCE_NUMBER);
    let f: FetchResponse = c.call(12, &fetch_req(&topic, 1, 0, 0)).await.unwrap();
    assert_eq!(f.responses[0].partitions[0].high_watermark, start1 + 5);
    println!(
        "idempotent producer {pid}: two retried batches kept their offsets {start1} and {}; 5 records, not 10",
        start1 + 3
    );

    // Long poll: a fetch at the high watermark waits for the next record.
    let mut p = login().await;
    let id = c.send(12, &fetch_req(&topic, 1, start1 + 5, 5000)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let t0 = Instant::now();
    produce_one(&mut p, 9, &topic, 1, data_batch(2000, 1, 10, Compression::None, None)).await;
    let produced = t0.elapsed();
    let r: FetchResponse = c.recv(12, id).await.unwrap();
    let woke = t0.elapsed();
    assert_eq!(values_of(&r.responses[0].partitions[0]).len(), 1);
    println!("long poll: produce acknowledged in {produced:?}, the waiting fetch answered {woke:?} after the produce started");

    // ListOffsets and OffsetForLeaderEpoch.
    let lo = |ts: i64| ListOffsetsRequest {
        replica_id: -1,
        topics: vec![list_offsets_request::ListOffsetsTopic {
            name: topic.clone(),
            partitions: vec![list_offsets_request::ListOffsetsPartition {
                partition_index: 0,
                current_leader_epoch: -1,
                timestamp: ts,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let r: ListOffsetsResponse = c.call(7, &lo(-2)).await.unwrap();
    assert_eq!(r.topics[0].partitions[0].offset, 0);
    let r: ListOffsetsResponse = c.call(7, &lo(-1)).await.unwrap();
    assert_eq!(r.topics[0].partitions[0].offset, next);
    let epoch = r.topics[0].partitions[0].leader_epoch;
    let r: ListOffsetsResponse = c.call(5, &lo(0)).await.unwrap();
    assert_eq!(r.topics[0].partitions[0].offset, 0);
    let r: ListOffsetsResponse = c.call(5, &lo(4_102_444_800_000)).await.unwrap();
    assert_eq!(r.topics[0].partitions[0].offset, -1);
    let r: OffsetForLeaderEpochResponse = c
        .call(
            4,
            &OffsetForLeaderEpochRequest {
                replica_id: -1,
                topics: vec![offset_for_leader_epoch_request::OffsetForLeaderTopic {
                    topic: topic.clone(),
                    partitions: vec![offset_for_leader_epoch_request::OffsetForLeaderPartition {
                        partition: 0,
                        current_leader_epoch: epoch,
                        leader_epoch: epoch,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let e = &r.topics[0].partitions[0];
    assert_eq!((e.error_code, e.leader_epoch, e.end_offset), (0, epoch, next));
    println!("leader epoch {epoch}; ListOffsets and OffsetForLeaderEpoch agree on {next}");

    // Per-topic byte configs (ALTER STREAM ... SET).
    assert_eq!(alter_config(&mut c, &topic, "max.message.bytes", "100").await, 0);
    assert_eq!(alter_config(&mut c, &topic, "retention.bytes", "1073741824").await, 0);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let big = produce_one(&mut c, 9, &topic, 0, data_batch(0, 1, 500, Compression::None, None)).await;
    assert_eq!(big.error_code, err::MESSAGE_TOO_LARGE, "{:?}", big.error_message);
    println!("max.message.bytes=100: {:?}", big.error_message);

    // acks=0 is refused by closing the connection (39950).
    let mut z = login().await;
    let r: Result<ProduceResponse, ClientError> = z
        .call(9, &produce_req(&topic, 0, data_batch(0, 1, 10, Compression::None, None), 0))
        .await;
    assert!(r.is_err());

    delete_topic(&mut c, &topic).await;
}

/// Produce and fetch throughput of one partition: `SPRIG_KAFKA_TP_RECORDS`
/// records of `SPRIG_KAFKA_TP_SIZE` bytes in batches of
/// `SPRIG_KAFKA_TP_BATCH`, `SPRIG_KAFKA_TP_INFLIGHT` batches in flight, as
/// an idempotent producer; then one consumer reads them all.
#[tokio::test]
#[ignore = "needs a running gateway with the kafka listener and a sequencer"]
async fn live_throughput() {
    let n: usize = env("SPRIG_KAFKA_TP_RECORDS").map_or(20_000, |v| v.parse().unwrap());
    let size: usize = env("SPRIG_KAFKA_TP_SIZE").map_or(100, |v| v.parse().unwrap());
    let batch: usize = env("SPRIG_KAFKA_TP_BATCH").map_or(500, |v| v.parse().unwrap());
    let inflight: usize = env("SPRIG_KAFKA_TP_INFLIGHT").map_or(5, |v| v.parse().unwrap());
    let parts: i32 = env("SPRIG_KAFKA_TP_PARTITIONS").map_or(1, |v| v.parse().unwrap());
    let db = env("SPRIG_KAFKA_DB").unwrap();
    let topic = format!("{db}.r9k2_tp_{}", std::process::id());
    let mut admin = login().await;
    create_topic(&mut admin, &topic, parts).await;
    for part in 0..parts {
        warm_up(&mut admin, &topic, part).await;
    }
    let t0 = Instant::now();
    let mut tasks = Vec::new();
    for part in 0..parts {
        let topic = topic.clone();
        tasks.push(tokio::spawn(async move {
            let mut c = login().await;
            let ip: InitProducerIdResponse = c
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
            let mut pending = std::collections::VecDeque::new();
            let mut sent = 0usize;
            let mut acked = 0usize;
            let mut retries = 0usize;
            let mut lat = Vec::new();
            while acked < n {
                while sent < n && pending.len() < inflight {
                    let k = batch.min(n - sent);
                    let b = data_batch(sent, k, size, Compression::Lz4, Some((ip.producer_id, 0, sent as i32)));
                    let id = c.send(9, &produce_req(&topic, part, b.clone(), -1)).await.unwrap();
                    pending.push_back((id, k, Instant::now(), b));
                    sent += k;
                }
                let (id, k, at, b) = pending.pop_front().unwrap();
                let r: ProduceResponse = c.recv(9, id).await.unwrap();
                let p = &r.responses[0].partition_responses[0];
                if p.error_code == err::REQUEST_TIMED_OUT || p.error_code == err::KAFKA_STORAGE_ERROR {
                    // Retryable: send the same batch again (a duplicate
                    // if it was appended), behind the ones in flight.
                    println!("retry after {}: {:?}", p.error_code, p.error_message);
                    retries += 1;
                    let id = c.send(9, &produce_req(&topic, part, b.clone(), -1)).await.unwrap();
                    pending.push_back((id, k, at, b));
                    continue;
                }
                assert_eq!(p.error_code, 0, "{:?}", p.error_message);
                lat.push(at.elapsed());
                acked += k;
            }
            lat.sort();
            (lat[lat.len() / 2], retries)
        }));
    }
    let mut medians = Vec::new();
    for t in tasks {
        medians.push(t.await.unwrap());
    }
    let secs = t0.elapsed().as_secs_f64();
    let total = n * parts as usize;
    println!(
        "produce: {total} records of {size} B in {secs:.2} s: {:.0} records/s ({:.0} per partition), batches of {batch}, {inflight} in flight, (median batch latency, retries) {:?}",
        total as f64 / secs,
        total as f64 / secs / f64::from(parts),
        medians
    );
    // Fetch everything back from one partition.
    let mut c = login().await;
    let t0 = Instant::now();
    let mut off = 1i64;
    let mut got = 0usize;
    while got < n {
        let r: FetchResponse = c.call(12, &fetch_req(&topic, 0, off, 500)).await.unwrap();
        let p = &r.responses[0].partitions[0];
        assert_eq!(p.error_code, 0);
        let v = values_of(p);
        if let Some((o, _)) = v.last() {
            off = o + 1;
        }
        got += v.len();
    }
    let secs = t0.elapsed().as_secs_f64();
    println!(
        "fetch: {got} records of partition 0 in {secs:.2} s: {:.0} records/s",
        got as f64 / secs
    );
    delete_topic(&mut admin, &topic).await;
}
