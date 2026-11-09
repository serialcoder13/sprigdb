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
    assert!(
        !r.api_keys.iter().any(|k| k.api_key == 0),
        "Produce isn't built yet"
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
