//! The group APIs: JoinGroup, SyncGroup, Heartbeat, LeaveGroup,
//! OffsetCommit, OffsetFetch, DescribeGroups, ListGroups and DeleteGroups.

use std::collections::HashMap;

use crate::group::{CommittedOffset, GroupKey, GroupState, JoinArgs, SyncArgs};
use crate::messages::{
    DeleteGroupsRequest, DeleteGroupsResponse, DescribeGroupsRequest, DescribeGroupsResponse,
    HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest,
    LeaveGroupResponse, ListGroupsRequest, ListGroupsResponse, OffsetCommitRequest,
    OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse, SyncGroupRequest,
    SyncGroupResponse, delete_groups_response, describe_groups_response, join_group_response,
    leave_group_response, list_groups_response, offset_commit_response, offset_fetch_response,
};
use crate::protocol::err;
use crate::topics::TopicInfo;

use super::{Broker, ConnError, Reply, Session, body, group_key, rendezvous};

/// Kafka's `OFFSET_METADATA_TOO_LARGE`; `stream_offsets.metadata` is
/// `VARCHAR(1024)`.
const OFFSET_METADATA_TOO_LARGE: i16 = 12;
const MAX_OFFSET_METADATA: usize = 1024;

/// One fetched offset: partition, offset, leader epoch, metadata, error.
type FetchedOffset = (i32, i64, i32, Option<String>, i16);

fn key(s: &Session, group: &str) -> GroupKey {
    GroupKey {
        tenant: s.principal.tenant_id,
        group: group.to_owned(),
    }
}

impl Broker {
    pub(super) async fn join_group(
        &self,
        s: &Session,
        q: JoinGroupRequest,
        v: i16,
    ) -> Result<Reply, ConnError> {
        let k = key(s, &q.group_id);
        let o = self
            .groups
            .join(
                &k,
                JoinArgs {
                    member_id: q.member_id,
                    group_instance_id: q.group_instance_id,
                    client_id: s.client_id.clone(),
                    client_host: format!("/{}", s.conn.peer.ip()),
                    session_timeout_ms: q.session_timeout_ms,
                    rebalance_timeout_ms: if v >= 1 {
                        q.rebalance_timeout_ms
                    } else {
                        q.session_timeout_ms
                    },
                    protocol_type: q.protocol_type,
                    protocols: q
                        .protocols
                        .into_iter()
                        .map(|p| (p.name, p.metadata))
                        .collect(),
                    require_known_member_id: v >= 4,
                },
            )
            .await;
        let resp = JoinGroupResponse {
            error_code: o.error,
            generation_id: o.generation,
            protocol_type: o.protocol_type,
            // Nullable only from v7.
            protocol_name: if v >= 7 {
                o.protocol_name
            } else {
                Some(o.protocol_name.unwrap_or_default())
            },
            leader: o.leader,
            member_id: o.member_id,
            members: o
                .members
                .into_iter()
                .map(|m| join_group_response::JoinGroupResponseMember {
                    member_id: m.member_id,
                    group_instance_id: m.group_instance_id,
                    metadata: m.metadata,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        body(&resp, v)
    }

    pub(super) async fn sync_group(
        &self,
        s: &Session,
        q: SyncGroupRequest,
        v: i16,
    ) -> Result<Reply, ConnError> {
        let k = key(s, &q.group_id);
        let o = self
            .groups
            .sync(
                &k,
                SyncArgs {
                    member_id: q.member_id,
                    generation: q.generation_id,
                    group_instance_id: q.group_instance_id,
                    protocol_type: q.protocol_type,
                    protocol_name: q.protocol_name,
                    assignments: q
                        .assignments
                        .into_iter()
                        .map(|a| (a.member_id, a.assignment))
                        .collect(),
                },
            )
            .await;
        let resp = SyncGroupResponse {
            error_code: o.error,
            protocol_type: o.protocol_type,
            protocol_name: o.protocol_name,
            assignment: o.assignment,
            ..Default::default()
        };
        body(&resp, v)
    }

    pub(super) async fn heartbeat(
        &self,
        s: &Session,
        q: HeartbeatRequest,
        v: i16,
    ) -> Result<Reply, ConnError> {
        let k = key(s, &q.group_id);
        let e = self
            .groups
            .heartbeat(
                &k,
                &q.member_id,
                q.generation_id,
                q.group_instance_id.as_deref(),
            )
            .await;
        body(
            &HeartbeatResponse {
                error_code: e,
                ..Default::default()
            },
            v,
        )
    }

    pub(super) async fn leave_group(
        &self,
        s: &Session,
        q: LeaveGroupRequest,
        v: i16,
    ) -> Result<Reply, ConnError> {
        let k = key(s, &q.group_id);
        let members: Vec<(String, Option<String>)> = if v >= 3 {
            q.members
                .iter()
                .map(|m| (m.member_id.clone(), m.group_instance_id.clone()))
                .collect()
        } else {
            vec![(q.member_id.clone(), None)]
        };
        let (top, each) = self.groups.leave(&k, &members).await;
        let mut resp = LeaveGroupResponse {
            error_code: top,
            ..Default::default()
        };
        if v >= 3 {
            // A group-level failure (not the coordinator) is top-level only.
            if members.len() > 1 && each.iter().all(|e| *e == err::NOT_COORDINATOR) {
                resp.error_code = err::NOT_COORDINATOR;
            } else {
                resp.error_code = err::NONE;
                resp.members = members
                    .into_iter()
                    .zip(each)
                    .map(|((id, inst), e)| leave_group_response::MemberResponse {
                        member_id: id,
                        group_instance_id: inst,
                        error_code: e,
                        ..Default::default()
                    })
                    .collect();
                if resp.members.len() == 1 && resp.members[0].error_code == err::NOT_COORDINATOR {
                    resp.error_code = err::NOT_COORDINATOR;
                    resp.members.clear();
                }
            }
        }
        body(&resp, v)
    }

    async fn topic(
        &self,
        s: &Session,
        cache: &mut HashMap<String, Option<TopicInfo>>,
        name: &str,
    ) -> Option<TopicInfo> {
        if let Some(t) = cache.get(name) {
            return t.clone();
        }
        let t = self.topics.get(&s.principal, name).await.ok().flatten();
        cache.insert(name.to_owned(), t.clone());
        t
    }

    pub(super) async fn offset_commit(
        &self,
        s: &Session,
        q: OffsetCommitRequest,
        v: i16,
    ) -> Result<Reply, ConnError> {
        let k = key(s, &q.group_id);
        let (member, generation) = if v >= 1 {
            (q.member_id.as_str(), q.generation_id_or_member_epoch)
        } else {
            ("", -1)
        };
        let check = if q.group_id.is_empty() {
            err::INVALID_GROUP_ID
        } else {
            self.groups
                .check_commit(&k, member, generation, q.group_instance_id.as_deref())
                .await
        };
        let mut cache = HashMap::new();
        let mut writes = Vec::new();
        let mut topics = Vec::with_capacity(q.topics.len());
        for t in &q.topics {
            let info = if check == err::NONE {
                self.topic(s, &mut cache, &t.name).await
            } else {
                None
            };
            let mut parts = Vec::with_capacity(t.partitions.len());
            for p in &t.partitions {
                let e = if check == err::NONE {
                    match &info {
                        None => err::UNKNOWN_TOPIC_OR_PARTITION,
                        Some(i) if p.partition_index < 0 || p.partition_index >= i.partitions => {
                            err::UNKNOWN_TOPIC_OR_PARTITION
                        }
                        Some(_)
                            if p.committed_metadata
                                .as_ref()
                                .is_some_and(|m| m.chars().count() > MAX_OFFSET_METADATA) =>
                        {
                            OFFSET_METADATA_TOO_LARGE
                        }
                        Some(i) => {
                            writes.push(CommittedOffset {
                                stream_id: i.stream_id,
                                partition: p.partition_index,
                                offset: p.committed_offset,
                                leader_epoch: (p.committed_leader_epoch >= 0)
                                    .then_some(p.committed_leader_epoch),
                                metadata: p.committed_metadata.clone(),
                            });
                            err::NONE
                        }
                    }
                } else {
                    check
                };
                parts.push((p.partition_index, e));
            }
            topics.push((t.name.clone(), parts));
        }
        let store_failed = !writes.is_empty()
            && match self
                .groups
                .store()
                .commit(k.tenant, &k.group, &writes)
                .await
            {
                Ok(()) => false,
                Err(e) => {
                    tracing::warn!(group = %k.group, error = %e, "kafka offset commit failed");
                    true
                }
            };
        let resp = OffsetCommitResponse {
            topics: topics
                .into_iter()
                .map(
                    |(name, parts)| offset_commit_response::OffsetCommitResponseTopic {
                        name,
                        partitions: parts
                            .into_iter()
                            .map(
                                |(i, e)| offset_commit_response::OffsetCommitResponsePartition {
                                    partition_index: i,
                                    error_code: if e == err::NONE && store_failed {
                                        err::COORDINATOR_NOT_AVAILABLE
                                    } else {
                                        e
                                    },
                                    ..Default::default()
                                },
                            )
                            .collect(),
                        ..Default::default()
                    },
                )
                .collect(),
            ..Default::default()
        };
        body(&resp, v)
    }

    /// One group's offsets: `(error, [(topic, [(partition, offset, epoch,
    /// metadata, error)])])`.
    async fn fetch_group(
        &self,
        s: &Session,
        group: &str,
        topics: Option<Vec<(String, Vec<i32>)>>,
    ) -> (i16, Vec<(String, Vec<FetchedOffset>)>) {
        let store = self.groups.store();
        let tenant = s.principal.tenant_id;
        let Ok(all) = self.topics.list(&s.principal).await else {
            return (err::COORDINATOR_NOT_AVAILABLE, Vec::new());
        };
        match topics {
            None => {
                let Ok(rows) = store.fetch(tenant, group, None).await else {
                    return (err::COORDINATOR_NOT_AVAILABLE, Vec::new());
                };
                let mut by: std::collections::BTreeMap<String, Vec<_>> =
                    std::collections::BTreeMap::new();
                for r in rows {
                    if let Some(t) = all.iter().find(|t| t.stream_id == r.stream_id) {
                        by.entry(t.name.clone()).or_default().push((
                            r.partition,
                            r.offset,
                            r.leader_epoch.unwrap_or(-1),
                            r.metadata,
                            err::NONE,
                        ));
                    }
                }
                (err::NONE, by.into_iter().collect())
            }
            Some(list) => {
                let mut keys = Vec::new();
                for (name, parts) in &list {
                    if let Some(t) = all.iter().find(|t| t.name == *name) {
                        keys.extend(parts.iter().map(|p| (t.stream_id, *p)));
                    }
                }
                let Ok(rows) = store.fetch(tenant, group, Some(&keys)).await else {
                    return (err::COORDINATOR_NOT_AVAILABLE, Vec::new());
                };
                let out = list
                    .into_iter()
                    .map(|(name, parts)| {
                        let sid = all.iter().find(|t| t.name == name).map(|t| t.stream_id);
                        let ps = parts
                            .into_iter()
                            .map(|p| {
                                match rows
                                    .iter()
                                    .find(|r| Some(r.stream_id) == sid && r.partition == p)
                                {
                                    Some(r) => (
                                        p,
                                        r.offset,
                                        r.leader_epoch.unwrap_or(-1),
                                        r.metadata.clone(),
                                        err::NONE,
                                    ),
                                    None => (p, -1, -1, Some(String::new()), err::NONE),
                                }
                            })
                            .collect();
                        (name, ps)
                    })
                    .collect();
                (err::NONE, out)
            }
        }
    }

    pub(super) async fn offset_fetch(
        &self,
        s: &Session,
        q: OffsetFetchRequest,
        v: i16,
    ) -> Result<Reply, ConnError> {
        let mut resp = OffsetFetchResponse::default();
        if v >= 8 {
            for g in q.groups {
                let topics = g.topics.map(|ts| {
                    ts.into_iter()
                        .map(|t| (t.name, t.partition_indexes))
                        .collect()
                });
                let (e, ts) = self.fetch_group(s, &g.group_id, topics).await;
                resp.groups
                    .push(offset_fetch_response::OffsetFetchResponseGroup {
                        group_id: g.group_id,
                        error_code: e,
                        topics: ts
                            .into_iter()
                            .map(
                                |(name, ps)| offset_fetch_response::OffsetFetchResponseTopics {
                                    name,
                                    partitions: ps
                                        .into_iter()
                                        .map(|(p, o, ep, md, e)| {
                                            offset_fetch_response::OffsetFetchResponsePartitions {
                                                partition_index: p,
                                                committed_offset: o,
                                                committed_leader_epoch: ep,
                                                metadata: md,
                                                error_code: e,
                                                ..Default::default()
                                            }
                                        })
                                        .collect(),
                                    ..Default::default()
                                },
                            )
                            .collect(),
                        ..Default::default()
                    });
            }
        } else {
            let topics = q.topics.map(|ts| {
                ts.into_iter()
                    .map(|t| (t.name, t.partition_indexes))
                    .collect()
            });
            let (e, ts) = self.fetch_group(s, &q.group_id, topics).await;
            resp.error_code = e;
            resp.topics = ts
                .into_iter()
                .map(
                    |(name, ps)| offset_fetch_response::OffsetFetchResponseTopic {
                        name,
                        partitions: ps
                            .into_iter()
                            .map(|(p, o, ep, md, pe)| {
                                offset_fetch_response::OffsetFetchResponsePartition {
                                    partition_index: p,
                                    committed_offset: o,
                                    committed_leader_epoch: ep,
                                    // Non-nullable metadata before v8 is fine as
                                    // empty.
                                    metadata: Some(md.unwrap_or_default()),
                                    // v0-1 have no top-level error: put it here.
                                    error_code: if v < 2 && e != err::NONE { e } else { pe },
                                    ..Default::default()
                                }
                            })
                            .collect(),
                        ..Default::default()
                    },
                )
                .collect();
        }
        body(&resp, v)
    }

    pub(super) async fn describe_groups(
        &self,
        s: &Session,
        q: DescribeGroupsRequest,
        v: i16,
    ) -> Result<Reply, ConnError> {
        let mut resp = DescribeGroupsResponse::default();
        for g in q.groups {
            let d = self.groups.describe(&key(s, &g)).await;
            resp.groups.push(describe_groups_response::DescribedGroup {
                error_code: d.error,
                group_id: d.group,
                group_state: d.state.name().into(),
                protocol_type: d.protocol_type,
                protocol_data: d.protocol,
                members: d
                    .members
                    .into_iter()
                    .map(|m| describe_groups_response::DescribedGroupMember {
                        member_id: m.member_id,
                        group_instance_id: m.group_instance_id,
                        client_id: m.client_id,
                        client_host: m.client_host,
                        member_metadata: m.metadata,
                        member_assignment: m.assignment,
                        ..Default::default()
                    })
                    .collect(),
                authorized_operations: i32::MIN,
                ..Default::default()
            });
        }
        body(&resp, v)
    }

    pub(super) async fn list_groups(
        &self,
        s: &Session,
        q: ListGroupsRequest,
        v: i16,
    ) -> Result<Reply, ConnError> {
        let mut resp = ListGroupsResponse::default();
        let me = self.cluster.node_id();
        if !q.types_filter.is_empty()
            && !q
                .types_filter
                .iter()
                .any(|t| t.eq_ignore_ascii_case("classic"))
        {
            return body(&resp, v);
        }
        let Ok(stored) = self.groups.store().list_groups(s.principal.tenant_id).await else {
            resp.error_code = err::COORDINATOR_NOT_AVAILABLE;
            return body(&resp, v);
        };
        let brokers = self.cluster.brokers(&s.principal).await;
        for g in stored {
            let k = key(s, &g.group);
            // Each gateway lists the groups it coordinates, so a client
            // that asks every broker sees each group once.
            let mine = match self.groups.leases().holder(&k).await {
                Ok(Some(h)) => h == me,
                Ok(None) => rendezvous(&group_key(k.tenant, &k.group), &brokers)
                    .is_some_and(|b| b.node_id == me),
                Err(_) => false,
            };
            if !mine {
                continue;
            }
            let (state, pt) = self
                .groups
                .local_state(&k)
                .await
                .unwrap_or((GroupState::Empty, g.protocol_type.clone()));
            if !q.states_filter.is_empty()
                && !q
                    .states_filter
                    .iter()
                    .any(|f| f.eq_ignore_ascii_case(state.name()))
            {
                continue;
            }
            resp.groups.push(list_groups_response::ListedGroup {
                group_id: g.group,
                protocol_type: pt,
                group_state: state.name().into(),
                group_type: "classic".into(),
                ..Default::default()
            });
        }
        body(&resp, v)
    }

    pub(super) async fn delete_groups(
        &self,
        s: &Session,
        q: DeleteGroupsRequest,
        v: i16,
    ) -> Result<Reply, ConnError> {
        let mut resp = DeleteGroupsResponse::default();
        for g in q.groups_names {
            let e = self.groups.delete(&key(s, &g)).await;
            resp.results
                .push(delete_groups_response::DeletableGroupResult {
                    group_id: g,
                    error_code: e,
                    ..Default::default()
                });
        }
        body(&resp, v)
    }
}
