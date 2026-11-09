//! The broker front end end to end, over in-memory streams, with the
//! crate's own client and in-memory identity, topics, groups and leases
//! (chapter 45 build steps 9, 12 and 13).

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::many_single_char_names
)]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sprig_common::CancellationToken;
use sprig_proto_kafka::broker::{
    AuthFailure, Broker, BrokerConfig, BrokerNode, Cluster, ConnInfo, Identity, ProducerIds,
    ScramFinish, ScramLookup,
};
use sprig_proto_kafka::client::{Client, ClientError, assignment, parse_assignment, subscription};
use sprig_proto_kafka::group::{Coordinator, GroupConfig};
use sprig_proto_kafka::mem::{MemGroupStore, MemLeaseTable, MemLeases, MemTopics};
use sprig_proto_kafka::messages::*;
use sprig_proto_kafka::protocol::{RequestHeader, err, request_frame};
use sprig_proto_kafka::sasl::{PlainCredentials, ScramCredential, ScramMechanism};
use sprig_proto_kafka::topics::Principal;
use sprig_proto_kafka::wire::{Message, Reader};
use tokio::io::DuplexStream;

const PW: &str = "sprig-dev-app";

fn principal(user: &str) -> Option<Principal> {
    let (tenant, name) = user.split_once('.')?;
    let tenant_id = match tenant {
        "acme" => 41,
        "globex" => 42,
        _ => return None,
    };
    Some(Principal {
        tenant_id,
        tenant: tenant.into(),
        user: name.into(),
        user_id: Some(7),
        handle: None,
    })
}

struct TestIdentity;

struct Finish(Option<Principal>);

#[async_trait]
impl ScramFinish for Finish {
    async fn finish(self: Box<Self>, verified: bool) -> Result<Principal, AuthFailure> {
        match (verified, self.0) {
            (true, Some(p)) => Ok(p),
            _ => Err(AuthFailure(
                "Authentication failed: invalid credentials".into(),
            )),
        }
    }
}

#[async_trait]
impl Identity for TestIdentity {
    async fn plain(
        &self,
        _c: &ConnInfo,
        creds: PlainCredentials,
    ) -> Result<Principal, AuthFailure> {
        match principal(&creds.user) {
            Some(p) if creds.password == PW.as_bytes() => Ok(p),
            _ => Err(AuthFailure(
                "Authentication failed: invalid credentials".into(),
            )),
        }
    }

    async fn scram(&self, _c: &ConnInfo, mech: ScramMechanism, user: &str) -> ScramLookup {
        let p = principal(user);
        let credential = if p.is_some() {
            ScramCredential::derive(mech, PW.as_bytes(), b"0123456789abcdef0123456789ab", 4096)
        } else {
            ScramCredential::decoy(mech, b"decoy", user, 4096)
        };
        ScramLookup {
            credential,
            finish: Box::new(Finish(p)),
        }
    }
}

struct TestCluster {
    node: i32,
    brokers: Vec<BrokerNode>,
}

#[async_trait]
impl Cluster for TestCluster {
    fn node_id(&self) -> i32 {
        self.node
    }
    fn cluster_id(&self) -> String {
        "sprig-dc-a".into()
    }
    async fn brokers(&self, _p: &Principal) -> Vec<BrokerNode> {
        self.brokers.clone()
    }
}

fn nodes(n: i32) -> Vec<BrokerNode> {
    (1..=n)
        .map(|i| BrokerNode {
            node_id: i,
            host: format!("gw{i}"),
            port: 9092,
            rack: Some("dc-a".into()),
        })
        .collect()
}

struct Env {
    topics: Arc<MemTopics>,
    store: Arc<MemGroupStore>,
    leases: Arc<MemLeaseTable>,
    brokers: Vec<BrokerNode>,
}

impl Env {
    fn new(n: i32) -> Self {
        Self {
            topics: Arc::new(MemTopics::default()),
            store: Arc::new(MemGroupStore::default()),
            leases: Arc::new(MemLeaseTable::default()),
            brokers: nodes(n),
        }
    }

    fn broker(&self, node: i32) -> Arc<Broker> {
        let groups = Arc::new(Coordinator::new(
            self.store.clone(),
            Arc::new(MemLeases {
                table: self.leases.clone(),
                node,
            }),
            GroupConfig {
                min_session: Duration::from_millis(100),
                max_session: Duration::from_secs(60),
                initial_rebalance_delay: Duration::from_millis(50),
                tick: Duration::from_millis(20),
            },
        ));
        tokio::spawn(Arc::clone(&groups).run(CancellationToken::new()));
        Arc::new(Broker {
            identity: Arc::new(TestIdentity),
            cluster: Arc::new(TestCluster {
                node,
                brokers: self.brokers.clone(),
            }),
            topics: self.topics.clone(),
            groups,
            data: None,
            config: Arc::new(BrokerConfig::default),
            producer_ids: ProducerIds::new(node),
        })
    }
}

fn connect_with(b: &Arc<Broker>, tls: bool) -> Client<DuplexStream> {
    let (a, mut s) = tokio::io::duplex(1 << 20);
    let b = Arc::clone(b);
    tokio::spawn(async move {
        let info = ConnInfo {
            peer: "10.0.0.9:51000".parse().unwrap(),
            tls,
            sni: None,
            client_cert: false,
        };
        let _ = b.serve(&mut s, info, CancellationToken::new()).await;
    });
    Client::new(a, "sprig-test")
}

async fn login(b: &Arc<Broker>, user: &str) -> Client<DuplexStream> {
    let mut c = connect_with(b, true);
    c.sasl_scram(ScramMechanism::Sha512, user, PW)
        .await
        .unwrap();
    c
}

#[tokio::test]
async fn api_versions_advertises_only_what_is_built() {
    let env = Env::new(1);
    let b = env.broker(1);
    let mut c = connect_with(&b, true);
    for v in 0..=4 {
        let r: ApiVersionsResponse = c
            .call(
                v,
                &ApiVersionsRequest {
                    client_software_name: "t".into(),
                    client_software_version: "1".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(r.error_code, 0);
        let keys: Vec<i16> = r.api_keys.iter().map(|k| k.api_key).collect();
        for k in [
            3, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 22, 32, 36, 37, 42, 44,
        ] {
            assert!(keys.contains(&k), "v{v}: key {k} missing");
        }
        // Not built yet (steps 10 and 11), and never: transactions.
        for k in [0, 1, 2, 23, 24, 25, 26, 28] {
            assert!(!keys.contains(&k), "v{v}: key {k} advertised");
        }
    }
    // A version above ours: the v0 shape with UNSUPPORTED_VERSION.
    let h = RequestHeader {
        api_key: 18,
        api_version: 99,
        correlation_id: 77,
        client_id: Some("future".into()),
        unknown_tagged_fields: Vec::new(),
    };
    let resp = c
        .raw(&request_frame(&h, |_| Ok(())).unwrap())
        .await
        .unwrap();
    let mut r = Reader::new(&resp);
    assert_eq!(r.i32().unwrap(), 77);
    let body = ApiVersionsResponse::decode(&mut r, 0).unwrap();
    assert_eq!(body.error_code, err::UNSUPPORTED_VERSION);
    assert!(
        body.api_keys
            .iter()
            .any(|k| k.api_key == 18 && k.max_version == 4)
    );
}

#[tokio::test]
async fn requests_before_sasl_close_the_connection() {
    let env = Env::new(1);
    let b = env.broker(1);
    let mut c = connect_with(&b, true);
    let r: Result<MetadataResponse, _> = c.call(9, &MetadataRequest::default()).await;
    assert!(matches!(r, Err(ClientError::Io(_))), "{r:?}");
}

#[tokio::test]
async fn sasl_plain_and_scram() {
    let env = Env::new(1);
    let b = env.broker(1);
    // PLAIN over TLS.
    let mut c = connect_with(&b, true);
    c.sasl_plain("acme.app", PW).await.unwrap();
    let m: MetadataResponse = c.call(12, &MetadataRequest::default()).await.unwrap();
    assert_eq!(m.brokers.len(), 1);
    // A wrong password: 58 with 39953, then the connection closes.
    let mut c = connect_with(&b, true);
    match c.sasl_plain("acme.app", "nope").await {
        Err(ClientError::Refused(58, m)) => assert!(m.starts_with("39953"), "{m}"),
        other => panic!("{other:?}"),
    }
    assert!(
        c.call::<_, MetadataResponse>(12, &MetadataRequest::default())
            .await
            .is_err()
    );
    // PLAIN is not offered without TLS; SCRAM is.
    let mut c = connect_with(&b, false);
    match c.sasl_plain("acme.app", PW).await {
        Err(ClientError::Refused(code, mechs)) => {
            assert_eq!(code, err::UNSUPPORTED_SASL_MECHANISM);
            assert!(!mechs.contains("PLAIN"), "{mechs}");
        }
        other => panic!("{other:?}"),
    }
    for mech in [ScramMechanism::Sha256, ScramMechanism::Sha512] {
        let mut c = connect_with(&b, false);
        c.sasl_scram(mech, "acme.app", PW).await.unwrap();
        let mut c = connect_with(&b, true);
        assert!(matches!(
            c.sasl_scram(mech, "acme.app", "wrong").await,
            Err(ClientError::Refused(58, _))
        ));
        let mut c = connect_with(&b, true);
        assert!(matches!(
            c.sasl_scram(mech, "nobody.app", PW).await,
            Err(ClientError::Refused(58, _))
        ));
    }
}

#[tokio::test]
async fn sasl_handshake_v0_takes_raw_tokens() {
    let env = Env::new(1);
    let b = env.broker(1);
    let mut c = connect_with(&b, true);
    let r: SaslHandshakeResponse = c
        .call(
            0,
            &SaslHandshakeRequest {
                mechanism: "PLAIN".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.error_code, 0);
    let token = format!("\0acme.app\0{PW}");
    let mut frame = u32::try_from(token.len()).unwrap().to_be_bytes().to_vec();
    frame.extend_from_slice(token.as_bytes());
    let resp = c.raw(&frame).await.unwrap();
    assert!(resp.is_empty());
    let m: MetadataResponse = c.call(1, &MetadataRequest::default()).await.unwrap();
    assert_eq!(m.controller_id, 1);
}

#[tokio::test]
async fn topics_metadata_and_administration() {
    let env = Env::new(3);
    let b = env.broker(1);
    let mut c = login(&b, "acme.app").await;
    let create = |name: &str, n: i32, cfg: &[(&str, &str)], validate: bool| CreateTopicsRequest {
        topics: vec![create_topics_request::CreatableTopic {
            name: name.into(),
            num_partitions: n,
            replication_factor: -1,
            configs: cfg
                .iter()
                .map(|(k, v)| create_topics_request::CreatableTopicConfig {
                    name: (*k).into(),
                    value: Some((*v).into()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        timeout_ms: 30_000,
        validate_only: validate,
        ..Default::default()
    };
    let r: CreateTopicsResponse = c
        .call(
            7,
            &create(
                "shop.order_events",
                12,
                &[("retention.ms", "86400000")],
                false,
            ),
        )
        .await
        .unwrap();
    let t = &r.topics[0];
    assert_eq!(t.error_code, 0, "{:?}", t.error_message);
    assert_eq!(t.num_partitions, 12);
    assert_eq!(t.error_message, None, "success carries a null message");
    let cfg = t.configs.as_ref().unwrap();
    assert!(cfg.iter().any(|c| c.name == "retention.ms"
        && c.value.as_deref() == Some("86400000")
        && c.config_source == 1));
    // Exists, invalid name, bad config, no database part, validate only.
    for (req, code) in [
        (
            create("shop.order_events", 3, &[], false),
            err::TOPIC_ALREADY_EXISTS,
        ),
        (
            create("bad name", 3, &[], false),
            err::INVALID_TOPIC_EXCEPTION,
        ),
        (
            create("shop.x", 3, &[("cleanup.policy", "compact,delete")], false),
            err::INVALID_CONFIG,
        ),
        (
            create("shop.x", 3, &[("message.format.version", "3")], false),
            err::INVALID_CONFIG,
        ),
        (create("nodb", 3, &[], false), err::INVALID_TOPIC_EXCEPTION),
        (create("shop.x", 0, &[], false), err::INVALID_PARTITIONS),
    ] {
        let r: CreateTopicsResponse = c.call(4, &req).await.unwrap();
        assert_eq!(r.topics[0].error_code, code, "{:?}", r.topics[0]);
    }
    let r: CreateTopicsResponse = c
        .call(1, &create("shop.validated", -1, &[], true))
        .await
        .unwrap();
    assert_eq!(r.topics[0].error_code, 0);
    // Default partitions, ignored replication configs.
    let r: CreateTopicsResponse = c
        .call(
            5,
            &create("shop.audit", -1, &[("min.insync.replicas", "2")], false),
        )
        .await
        .unwrap();
    assert_eq!(r.topics[0].num_partitions, 12);

    // Metadata: every version; leaders spread over the three gateways.
    for v in 0..=12 {
        let q = MetadataRequest {
            topics: Some(vec![
                metadata_request::MetadataRequestTopic {
                    name: Some("shop.order_events".into()),
                    ..Default::default()
                },
                metadata_request::MetadataRequestTopic {
                    name: Some("shop.missing".into()),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };
        let m: MetadataResponse = c.call(v, &q).await.unwrap();
        assert_eq!(m.brokers.len(), 3, "v{v}");
        assert_eq!(m.topics.len(), 2);
        let t = &m.topics[0];
        assert_eq!(t.error_code, 0);
        assert_eq!(t.partitions.len(), 12);
        let mut leaders: Vec<i32> = t.partitions.iter().map(|p| p.leader_id).collect();
        leaders.sort_unstable();
        leaders.dedup();
        assert!(
            leaders.len() >= 2,
            "rendezvous should spread 12 partitions: {leaders:?}"
        );
        assert_eq!(m.topics[1].error_code, err::UNKNOWN_TOPIC_OR_PARTITION);
    }
    // validate_only created nothing; all topics listed.
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
    let names: Vec<_> = m.topics.iter().filter_map(|t| t.name.clone()).collect();
    assert_eq!(names, vec!["shop.audit", "shop.order_events"]);
    // Another tenant sees none of them.
    let mut g = login(&b, "globex.app").await;
    let m: MetadataResponse = g
        .call(
            12,
            &MetadataRequest {
                topics: None,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(m.topics.is_empty());

    // Partitions grow only.
    let grow = |n| CreatePartitionsRequest {
        topics: vec![create_partitions_request::CreatePartitionsTopic {
            name: "shop.order_events".into(),
            count: n,
            assignments: None,
            ..Default::default()
        }],
        timeout_ms: 1000,
        ..Default::default()
    };
    let r: CreatePartitionsResponse = c.call(3, &grow(24)).await.unwrap();
    assert_eq!(r.results[0].error_code, 0);
    let r: CreatePartitionsResponse = c.call(0, &grow(6)).await.unwrap();
    assert_eq!(r.results[0].error_code, err::INVALID_PARTITIONS);
    assert!(
        r.results[0]
            .error_message
            .as_deref()
            .unwrap()
            .starts_with("39943")
    );

    // Configs: describe, alter, describe.
    let describe = DescribeConfigsRequest {
        resources: vec![describe_configs_request::DescribeConfigsResource {
            resource_type: 2,
            resource_name: "shop.order_events".into(),
            configuration_keys: None,
            ..Default::default()
        }],
        include_synonyms: true,
        ..Default::default()
    };
    let r: DescribeConfigsResponse = c.call(4, &describe).await.unwrap();
    let cfgs = &r.results[0].configs;
    assert_eq!(cfgs.len(), 4);
    let ret = cfgs.iter().find(|c| c.name == "retention.ms").unwrap();
    assert_eq!(ret.synonyms[0].name, "stream.retention_hours");
    assert_eq!(ret.synonyms[0].value.as_deref(), Some("24"));
    let alter = |op: i8, name: &str, value: Option<&str>| IncrementalAlterConfigsRequest {
        resources: vec![incremental_alter_configs_request::AlterConfigsResource {
            resource_type: 2,
            resource_name: "shop.order_events".into(),
            configs: vec![incremental_alter_configs_request::AlterableConfig {
                name: name.into(),
                config_operation: op,
                value: value.map(Into::into),
                ..Default::default()
            }],
            ..Default::default()
        }],
        validate_only: false,
        ..Default::default()
    };
    let r: IncrementalAlterConfigsResponse = c
        .call(1, &alter(0, "cleanup.policy", Some("compact")))
        .await
        .unwrap();
    assert_eq!(r.responses[0].error_code, 0);
    let r: IncrementalAlterConfigsResponse = c
        .call(0, &alter(2, "cleanup.policy", Some("delete")))
        .await
        .unwrap();
    assert_eq!(r.responses[0].error_code, err::INVALID_CONFIG);
    let r: IncrementalAlterConfigsResponse =
        c.call(1, &alter(1, "retention.ms", None)).await.unwrap();
    assert_eq!(r.responses[0].error_code, 0);
    let r: DescribeConfigsResponse = c.call(0, &describe).await.unwrap();
    let cfgs = &r.results[0].configs;
    assert!(cfgs.iter().any(|c| c.name == "cleanup.policy"
        && c.value.as_deref() == Some("compact")
        && !c.is_default));
    assert!(cfgs.iter().any(|c| c.name == "retention.ms"
        && c.value.as_deref() == Some("604800000")
        && c.is_default));

    // Delete, by name and by id.
    let id = m_topic_id(&mut c, "shop.audit").await;
    let r: DeleteTopicsResponse = c
        .call(
            6,
            &DeleteTopicsRequest {
                topics: vec![delete_topics_request::DeleteTopicState {
                    name: None,
                    topic_id: id,
                    ..Default::default()
                }],
                timeout_ms: 1000,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.responses[0].error_code, 0);
    assert_eq!(r.responses[0].name.as_deref(), Some("shop.audit"));
    let r: DeleteTopicsResponse = c
        .call(
            1,
            &DeleteTopicsRequest {
                topic_names: vec!["shop.order_events".into(), "shop.gone".into()],
                timeout_ms: 1000,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.responses[0].error_code, 0);
    assert_eq!(r.responses[1].error_code, err::UNKNOWN_TOPIC_OR_PARTITION);
}

async fn m_topic_id(c: &mut Client<DuplexStream>, name: &str) -> uuid::Uuid {
    let m: MetadataResponse = c
        .call(
            12,
            &MetadataRequest {
                topics: Some(vec![metadata_request::MetadataRequestTopic {
                    name: Some(name.into()),
                    ..Default::default()
                }]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    m.topics[0].topic_id
}

async fn create_topic(c: &mut Client<DuplexStream>, name: &str, n: i32) {
    let r: CreateTopicsResponse = c
        .call(
            7,
            &CreateTopicsRequest {
                topics: vec![create_topics_request::CreatableTopic {
                    name: name.into(),
                    num_partitions: n,
                    replication_factor: 1,
                    ..Default::default()
                }],
                timeout_ms: 1000,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.topics[0].error_code, 0);
}

fn join_req(group: &str, member: &str) -> JoinGroupRequest {
    JoinGroupRequest {
        group_id: group.into(),
        session_timeout_ms: 1_000,
        rebalance_timeout_ms: 2_000,
        member_id: member.into(),
        protocol_type: "consumer".into(),
        protocols: vec![join_group_request::JoinGroupRequestProtocol {
            name: "range".into(),
            metadata: subscription(&["shop.e"]),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// JoinGroup v9 with the MEMBER_ID_REQUIRED round trip.
async fn join(c: &mut Client<DuplexStream>, group: &str, member: &str) -> JoinGroupResponse {
    let r: JoinGroupResponse = c.call(9, &join_req(group, member)).await.unwrap();
    if r.error_code == err::MEMBER_ID_REQUIRED {
        assert!(!r.member_id.is_empty());
        return c.call(9, &join_req(group, &r.member_id)).await.unwrap();
    }
    r
}

async fn sync(
    c: &mut Client<DuplexStream>,
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
            protocol_name: Some("range".into()),
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

async fn heartbeat(c: &mut Client<DuplexStream>, group: &str, j: &JoinGroupResponse) -> i16 {
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

/// The range assignor over 4 partitions, as a leader computes it.
fn plan(members: &[String]) -> Vec<(String, Vec<u8>)> {
    let mut ms = members.to_vec();
    ms.sort();
    let per = 4 / ms.len();
    ms.iter()
        .enumerate()
        .map(|(i, m)| {
            let ps: Vec<i32> = (i * per..(i + 1) * per)
                .map(|p| i32::try_from(p).unwrap())
                .collect();
            (m.clone(), assignment(&[("shop.e", ps)]))
        })
        .collect()
}

#[tokio::test]
async fn consumer_group_join_rebalance_and_offsets() {
    let env = Env::new(1);
    let b = env.broker(1);
    let mut admin = login(&b, "acme.app").await;
    create_topic(&mut admin, "shop.e", 4).await;
    let group = "billing";

    // Member 1 alone.
    let mut c1 = login(&b, "acme.app").await;
    let j1 = join(&mut c1, group, "").await;
    assert_eq!(j1.error_code, 0);
    assert_eq!(j1.leader, j1.member_id);
    assert_eq!(j1.members.len(), 1);
    assert_eq!(j1.protocol_name.as_deref(), Some("range"));
    let s1 = sync(
        &mut c1,
        group,
        &j1,
        plan(std::slice::from_ref(&j1.member_id)),
    )
    .await;
    assert_eq!(s1.error_code, 0);
    assert_eq!(
        parse_assignment(&s1.assignment).unwrap()[0].1,
        vec![0, 1, 2, 3]
    );
    assert_eq!(heartbeat(&mut c1, group, &j1).await, 0);

    // Member 2 joins: a rebalance. Its join waits until member 1 rejoins.
    let b2 = Arc::clone(&b);
    let t2 = tokio::spawn(async move {
        let mut c2 = login(&b2, "acme.app").await;
        let j2 = join(&mut c2, "billing", "").await;
        (c2, j2)
    });
    let mut hb = 0;
    for _ in 0..50 {
        hb = heartbeat(&mut c1, group, &j1).await;
        if hb == err::REBALANCE_IN_PROGRESS {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(hb, err::REBALANCE_IN_PROGRESS);
    let j1b = join(&mut c1, group, &j1.member_id).await;
    let (mut c2, j2) = t2.await.unwrap();
    assert_eq!(j1b.error_code, 0);
    assert_eq!(j2.error_code, 0);
    assert_eq!(j1b.generation_id, j1.generation_id + 1);
    assert_eq!(j2.generation_id, j1b.generation_id);
    assert_eq!(j1b.leader, j1.member_id, "the leader stays");
    assert_eq!(j1b.members.len(), 2);
    assert!(j2.members.is_empty(), "followers don't get the member list");

    // The follower's sync waits for the leader's.
    let jj2 = j2.clone();
    let t2 = tokio::spawn(async move {
        let s = sync(&mut c2, "billing", &jj2, Vec::new()).await;
        (c2, s)
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    let ids: Vec<String> = j1b.members.iter().map(|m| m.member_id.clone()).collect();
    let s1 = sync(&mut c1, group, &j1b, plan(&ids)).await;
    let (mut c2, s2) = t2.await.unwrap();
    assert_eq!(s1.error_code, 0);
    assert_eq!(s2.error_code, 0);
    let mut got: Vec<i32> = parse_assignment(&s1.assignment).unwrap()[0].1.clone();
    got.extend(parse_assignment(&s2.assignment).unwrap()[0].1.clone());
    got.sort_unstable();
    assert_eq!(got, vec![0, 1, 2, 3], "every partition assigned once");

    // Offsets: commit with the generation, fetch back (v1 and v8).
    let commit = |m: &JoinGroupResponse, generation: i32, p: i32, o: i64| OffsetCommitRequest {
        group_id: "billing".into(),
        generation_id_or_member_epoch: generation,
        member_id: m.member_id.clone(),
        topics: vec![offset_commit_request::OffsetCommitRequestTopic {
            name: "shop.e".into(),
            partitions: vec![offset_commit_request::OffsetCommitRequestPartition {
                partition_index: p,
                committed_offset: o,
                committed_leader_epoch: 0,
                committed_metadata: Some("m".into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let r: OffsetCommitResponse = c1
        .call(8, &commit(&j1b, j1b.generation_id, 0, 1206))
        .await
        .unwrap();
    assert_eq!(r.topics[0].partitions[0].error_code, 0);
    let r: OffsetCommitResponse = c2
        .call(2, &commit(&j2, j2.generation_id - 1, 3, 9))
        .await
        .unwrap();
    assert_eq!(
        r.topics[0].partitions[0].error_code,
        err::ILLEGAL_GENERATION
    );
    let r: OffsetCommitResponse = c2
        .call(6, &commit(&j2, j2.generation_id, 9, 9))
        .await
        .unwrap();
    assert_eq!(
        r.topics[0].partitions[0].error_code,
        err::UNKNOWN_TOPIC_OR_PARTITION
    );
    let r: OffsetFetchResponse = c2
        .call(
            1,
            &OffsetFetchRequest {
                group_id: "billing".into(),
                topics: Some(vec![offset_fetch_request::OffsetFetchRequestTopic {
                    name: "shop.e".into(),
                    partition_indexes: vec![0, 1],
                    ..Default::default()
                }]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let ps = &r.topics[0].partitions;
    assert_eq!((ps[0].committed_offset, ps[1].committed_offset), (1206, -1));
    let r: OffsetFetchResponse = c2
        .call(
            8,
            &OffsetFetchRequest {
                groups: vec![offset_fetch_request::OffsetFetchRequestGroup {
                    group_id: "billing".into(),
                    topics: None,
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let g = &r.groups[0];
    assert_eq!(g.topics[0].partitions[0].committed_offset, 1206);
    assert_eq!(g.topics[0].partitions[0].committed_leader_epoch, 0);

    // Describe and list.
    let r: DescribeGroupsResponse = c1
        .call(
            5,
            &DescribeGroupsRequest {
                groups: vec!["billing".into(), "nobody".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.groups[0].group_state, "Stable");
    assert_eq!(r.groups[0].members.len(), 2);
    assert_eq!(r.groups[0].members[0].client_host, "/10.0.0.9");
    assert_eq!(r.groups[1].group_state, "Dead");
    let r: ListGroupsResponse = c1.call(4, &ListGroupsRequest::default()).await.unwrap();
    assert!(
        r.groups
            .iter()
            .any(|g| g.group_id == "billing" && g.group_state == "Stable")
    );
    // Deleting a group with members is refused.
    let del = DeleteGroupsRequest {
        groups_names: vec!["billing".into()],
        ..Default::default()
    };
    let r: DeleteGroupsResponse = c1.call(2, &del).await.unwrap();
    assert_eq!(r.results[0].error_code, err::NON_EMPTY_GROUP);

    // Member 2 leaves: member 1 is told to rejoin, and gets everything.
    let r: LeaveGroupResponse = c2
        .call(
            5,
            &LeaveGroupRequest {
                group_id: "billing".into(),
                members: vec![leave_group_request::MemberIdentity {
                    member_id: j2.member_id.clone(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.members[0].error_code, 0);
    assert_eq!(
        heartbeat(&mut c1, group, &j1b).await,
        err::REBALANCE_IN_PROGRESS
    );
    let j1c = join(&mut c1, group, &j1.member_id).await;
    assert_eq!(j1c.members.len(), 1);
    assert_eq!(j1c.generation_id, j1b.generation_id + 1);

    // Member 1 stops heartbeating: its session (1 s) expires; the group
    // empties and can then be deleted.
    let mut state = String::new();
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let r: DescribeGroupsResponse = admin
            .call(
                0,
                &DescribeGroupsRequest {
                    groups: vec!["billing".into()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        state.clone_from(&r.groups[0].group_state);
        if state == "Empty" {
            break;
        }
    }
    assert_eq!(state, "Empty");
    let r: DeleteGroupsResponse = admin.call(2, &del).await.unwrap();
    assert_eq!(r.results[0].error_code, 0);
    let r: DeleteGroupsResponse = admin.call(0, &del).await.unwrap();
    assert_eq!(r.results[0].error_code, err::GROUP_ID_NOT_FOUND);
}

#[tokio::test]
async fn coordinators_and_lease_moves() {
    let env = Env::new(2);
    let a = env.broker(1);
    let b = env.broker(2);
    let mut ca = login(&a, "acme.app").await;
    let mut cb = login(&b, "acme.app").await;
    // Before anyone holds the lease, both answer the rendezvous choice.
    let fc = |k: &str| FindCoordinatorRequest {
        key: k.into(),
        key_type: 0,
        coordinator_keys: vec![k.into()],
        ..Default::default()
    };
    let ra: FindCoordinatorResponse = ca.call(3, &fc("g1")).await.unwrap();
    let rb: FindCoordinatorResponse = cb.call(4, &fc("g1")).await.unwrap();
    assert_eq!(ra.node_id, rb.coordinators[0].node_id);
    // A joins first, so A holds the lease; B sends members to A.
    let j = join(&mut ca, "g1", "").await;
    assert_eq!(j.error_code, 0);
    let rb: FindCoordinatorResponse = cb.call(2, &fc("g1")).await.unwrap();
    assert_eq!(rb.node_id, 1);
    let r: JoinGroupResponse = cb.call(9, &join_req("g1", "")).await.unwrap();
    assert_eq!(r.error_code, err::NOT_COORDINATOR);
    // A's lease expires: B takes over; the old member must rejoin.
    env.leases.expire(1);
    let r: HeartbeatResponse = cb
        .call(
            4,
            &HeartbeatRequest {
                group_id: "g1".into(),
                generation_id: j.generation_id,
                member_id: j.member_id.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.error_code, err::UNKNOWN_MEMBER_ID);
    let r: HeartbeatResponse = ca
        .call(
            4,
            &HeartbeatRequest {
                group_id: "g1".into(),
                generation_id: j.generation_id,
                member_id: j.member_id.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.error_code, err::NOT_COORDINATOR);
    let j2 = join(&mut cb, "g1", "").await;
    assert_eq!(j2.error_code, 0);
    assert!(
        j2.generation_id > j.generation_id,
        "the generation survives the move"
    );
    // Transactions: refused at FindCoordinator and InitProducerId.
    let r: FindCoordinatorResponse = ca
        .call(
            1,
            &FindCoordinatorRequest {
                key: "tx".into(),
                key_type: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.error_code, err::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
    assert!(r.error_message.unwrap().starts_with("39951"));
    let r: InitProducerIdResponse = ca
        .call(
            4,
            &InitProducerIdRequest {
                transactional_id: Some("tx".into()),
                transaction_timeout_ms: 1000,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.error_code, err::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
    let mut ids = HashSet::new();
    for (c, node) in [(&mut ca, 1), (&mut cb, 2)] {
        for _ in 0..3 {
            let r: InitProducerIdResponse = c
                .call(
                    2,
                    &InitProducerIdRequest {
                        transactional_id: None,
                        transaction_timeout_ms: 1000,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(r.error_code, 0);
            assert_eq!(r.producer_id & 0xffff, node);
            assert!(ids.insert(r.producer_id), "producer ids are unique");
        }
    }
}

#[tokio::test]
async fn unserved_apis_close_the_connection() {
    let env = Env::new(1);
    let b = env.broker(1);
    let mut c = login(&b, "acme.app").await;
    // Produce v9: not built yet, not advertised.
    let h = RequestHeader {
        api_key: 0,
        api_version: 9,
        correlation_id: 5,
        client_id: None,
        unknown_tagged_fields: Vec::new(),
    };
    assert!(
        c.raw(&request_frame(&h, |_| Ok(())).unwrap())
            .await
            .is_err()
    );
}
