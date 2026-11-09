//! Metadata, FindCoordinator and InitProducerId.

use crate::group::GroupKey;
use crate::messages::{
    FindCoordinatorRequest, FindCoordinatorResponse, InitProducerIdRequest, InitProducerIdResponse,
    MetadataRequest, MetadataResponse, find_coordinator_response, metadata_response,
};
use crate::protocol::err;
use crate::topics::{TopicInfo, legal_topic_name};

use super::{
    Broker, BrokerNode, ConnError, Reply, Session, body, group_key, partition_key, rendezvous,
};

/// Kafka's "authorized operations not requested".
const OPS_UNKNOWN: i32 = i32::MIN;

impl Broker {
    pub(super) async fn metadata(
        &self,
        s: &Session,
        q: MetadataRequest,
        v: i16,
    ) -> Result<Reply, ConnError> {
        let brokers = self.cluster.brokers(&s.principal).await;
        let mut resp = MetadataResponse {
            brokers: brokers
                .iter()
                .map(|b| metadata_response::MetadataResponseBroker {
                    node_id: b.node_id,
                    host: b.host.clone(),
                    port: b.port,
                    rack: b.rack.clone(),
                    ..Default::default()
                })
                .collect(),
            cluster_id: Some(self.cluster.cluster_id()),
            // A stable gateway: the lowest broker id. Topic administration
            // goes through any gateway anyway.
            controller_id: brokers.iter().map(|b| b.node_id).min().unwrap_or(-1),
            ..Default::default()
        };
        // v0: an empty list means all topics; v1+: null means all.
        let wanted = match &q.topics {
            None => None,
            Some(t) if t.is_empty() && v == 0 => None,
            Some(t) => Some(t.clone()),
        };
        let all = match self.topics.list(&s.principal).await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, "kafka metadata: listing topics failed");
                Vec::new()
            }
        };
        match wanted {
            None => {
                for t in &all {
                    resp.topics.push(self.topic_metadata(s, t, &brokers).await);
                }
            }
            Some(list) => {
                for want in list {
                    let found = match (&want.name, want.topic_id.is_nil()) {
                        (Some(n), _) => all.iter().find(|t| t.name == *n),
                        (None, false) => all.iter().find(|t| t.topic_id == want.topic_id),
                        (None, true) => None,
                    };
                    if let Some(t) = found {
                        resp.topics.push(self.topic_metadata(s, t, &brokers).await);
                    } else {
                        let code = match &want.name {
                            Some(n) if !legal_topic_name(n) => err::INVALID_TOPIC_EXCEPTION,
                            Some(_) => err::UNKNOWN_TOPIC_OR_PARTITION,
                            None => err::UNKNOWN_TOPIC_ID,
                        };
                        resp.topics.push(metadata_response::MetadataResponseTopic {
                            error_code: code,
                            name: want.name.clone(),
                            topic_id: want.topic_id,
                            topic_authorized_operations: OPS_UNKNOWN,
                            ..Default::default()
                        });
                    }
                }
            }
        }
        if v < 12 {
            // Names are only nullable from v12.
            for t in &mut resp.topics {
                if t.name.is_none() {
                    t.name = Some(String::new());
                }
            }
        }
        body(&resp, v)
    }

    async fn topic_metadata(
        &self,
        s: &Session,
        t: &TopicInfo,
        brokers: &[BrokerNode],
    ) -> metadata_response::MetadataResponseTopic {
        let mut partitions = Vec::with_capacity(usize::try_from(t.partitions).unwrap_or(0));
        for p in 0..t.partitions {
            let leader = rendezvous(&partition_key(t.stream_id, p), brokers);
            let epoch = match &self.data {
                Some(d) => d.leader_epoch(&s.principal, t.stream_id, p).await,
                None => 0,
            };
            partitions.push(match leader {
                Some(l) => metadata_response::MetadataResponsePartition {
                    error_code: err::NONE,
                    partition_index: p,
                    leader_id: l.node_id,
                    leader_epoch: epoch,
                    replica_nodes: vec![l.node_id],
                    isr_nodes: vec![l.node_id],
                    ..Default::default()
                },
                None => metadata_response::MetadataResponsePartition {
                    error_code: err::LEADER_NOT_AVAILABLE,
                    partition_index: p,
                    leader_id: -1,
                    leader_epoch: epoch,
                    ..Default::default()
                },
            });
        }
        metadata_response::MetadataResponseTopic {
            error_code: err::NONE,
            name: Some(t.name.clone()),
            topic_id: t.topic_id,
            is_internal: false,
            partitions,
            topic_authorized_operations: OPS_UNKNOWN,
            ..Default::default()
        }
    }

    pub(super) async fn find_coordinator(
        &self,
        s: &Session,
        q: FindCoordinatorRequest,
        v: i16,
    ) -> Result<Reply, ConnError> {
        let keys = if v >= 4 {
            q.coordinator_keys.clone()
        } else {
            vec![q.key.clone()]
        };
        let brokers = self.cluster.brokers(&s.principal).await;
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let mut c = find_coordinator_response::Coordinator {
                key: key.clone(),
                node_id: -1,
                host: String::new(),
                port: -1,
                error_code: err::NONE,
                error_message: None,
                ..Default::default()
            };
            if q.key_type == 1 {
                c.error_code = err::TRANSACTIONAL_ID_AUTHORIZATION_FAILED;
                c.error_message = Some(
                    "39951 ER_SPRIG_KAFKA_TRANSACTIONS_UNSUPPORTED: SprigDB has no Kafka transactions; \
                     publish inside a database transaction (INSERT INTO <stream> in SQL, or db.publish \
                     in logic) for atomic writes"
                        .into(),
                );
            } else if q.key_type != 0 {
                c.error_code = err::INVALID_REQUEST;
                c.error_message = Some(format!(
                    "coordinator key type {} is not supported",
                    q.key_type
                ));
            } else if key.is_empty() {
                c.error_code = err::INVALID_REQUEST;
                c.error_message = Some("empty group id".into());
            } else {
                let gk = GroupKey {
                    tenant: s.principal.tenant_id,
                    group: key.clone(),
                };
                let holder = self.groups.leases().holder(&gk).await.ok().flatten();
                let node = holder
                    .and_then(|h| brokers.iter().find(|b| b.node_id == h))
                    .or_else(|| rendezvous(&group_key(gk.tenant, &gk.group), &brokers));
                if let Some(n) = node {
                    c.node_id = n.node_id;
                    c.host.clone_from(&n.host);
                    c.port = n.port;
                } else {
                    c.error_code = err::COORDINATOR_NOT_AVAILABLE;
                    c.error_message = Some("no gateway serves Kafka in this data center".into());
                }
            }
            out.push(c);
        }
        let mut resp = FindCoordinatorResponse::default();
        if v >= 4 {
            resp.coordinators = out;
        } else if let Some(c) = out.pop() {
            resp.error_code = c.error_code;
            resp.error_message = c.error_message;
            resp.node_id = c.node_id;
            resp.host = c.host;
            resp.port = c.port;
        }
        body(&resp, v)
    }

    pub(super) fn init_producer_id(
        &self,
        q: &InitProducerIdRequest,
        v: i16,
    ) -> Result<Reply, ConnError> {
        let mut resp = InitProducerIdResponse::default();
        if q.transactional_id.as_deref().is_some_and(|t| !t.is_empty()) {
            // The response has no message field; the client reports the
            // Kafka error. The log carries SprigDB's number.
            tracing::info!(
                code = 39951,
                "kafka InitProducerId with transactional.id refused (ER_SPRIG_KAFKA_TRANSACTIONS_UNSUPPORTED)"
            );
            resp.error_code = err::TRANSACTIONAL_ID_AUTHORIZATION_FAILED;
            resp.producer_id = -1;
            resp.producer_epoch = -1;
            return body(&resp, v);
        }
        // v3+: a producer that already has an id asks for its epoch to be
        // bumped (KIP-360); it keeps its id while the epoch fits.
        if v >= 3 && q.producer_id >= 0 && q.producer_epoch >= 0 && q.producer_epoch < i16::MAX - 1
        {
            resp.producer_id = q.producer_id;
            resp.producer_epoch = q.producer_epoch + 1;
        } else {
            resp.producer_id = self.producer_ids.next();
            resp.producer_epoch = 0;
        }
        body(&resp, v)
    }
}
