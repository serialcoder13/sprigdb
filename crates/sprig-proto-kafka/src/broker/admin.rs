//! Topic administration: CreateTopics, DeleteTopics, CreatePartitions,
//! DescribeConfigs and IncrementalAlterConfigs, mapped onto stream DDL
//! through [`Topics`](crate::topics::Topics).

use std::collections::HashSet;

use crate::messages::{
    CreatePartitionsRequest, CreatePartitionsResponse, CreateTopicsRequest, CreateTopicsResponse,
    DeleteTopicsRequest, DeleteTopicsResponse, DescribeConfigsRequest, DescribeConfigsResponse,
    IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse, create_partitions_response,
    create_topics_response, delete_topics_response, describe_configs_response,
    incremental_alter_configs_response,
};
use crate::protocol::err;
use crate::topics::{NewTopic, TopicConfigs, TopicError, legal_topic_name};

use super::{Broker, ConnError, Reply, Session, body};

/// Kafka's resource types.
const RESOURCE_TOPIC: i8 = 2;
const RESOURCE_BROKER: i8 = 4;
/// Config sources.
const SOURCE_DYNAMIC_TOPIC: i8 = 1;
const SOURCE_DEFAULT: i8 = 5;
/// The most partitions one CreateTopics or CreatePartitions may ask for.
const MAX_PARTITIONS: i32 = 10_000;

fn invalid_name(name: &str) -> TopicError {
    TopicError::new(
        err::INVALID_TOPIC_EXCEPTION,
        format!("topic name '{name}' is not legal: 1 to 249 of [a-zA-Z0-9._-]"),
    )
}

impl Broker {
    pub(super) async fn create_topics(
        &self,
        s: &Session,
        q: CreateTopicsRequest,
        v: i16,
    ) -> Result<Reply, ConnError> {
        let mut seen = HashSet::new();
        let dups: HashSet<String> = q
            .topics
            .iter()
            .filter(|t| !seen.insert(t.name.clone()))
            .map(|t| t.name.clone())
            .collect();
        let mut resp = CreateTopicsResponse::default();
        for t in q.topics {
            let mut r = create_topics_response::CreatableTopicResult {
                name: t.name.clone(),
                error_message: None,
                ..Default::default()
            };
            let result: Result<_, TopicError> = async {
                if dups.contains(&t.name) {
                    return Err(TopicError::new(err::INVALID_REQUEST, "topic named twice in one request"));
                }
                if !legal_topic_name(&t.name) {
                    return Err(invalid_name(&t.name));
                }
                if t.replication_factor == 0 || t.replication_factor < -1 {
                    return Err(TopicError::new(
                        err::INVALID_REPLICATION_FACTOR,
                        "replication factor must be -1 or at least 1 (MariaDB replicates streams; the value is ignored)",
                    ));
                }
                let defaults = self.topics.defaults(&s.principal, &t.name).await?;
                let partitions = if !t.assignments.is_empty() {
                    if t.num_partitions != -1 {
                        return Err(TopicError::new(
                            err::INVALID_REQUEST,
                            "both partitions and a replica assignment given",
                        ));
                    }
                    i32::try_from(t.assignments.len()).unwrap_or(i32::MAX)
                } else if t.num_partitions == -1 {
                    defaults.partitions
                } else {
                    t.num_partitions
                };
                if partitions <= 0 || partitions > MAX_PARTITIONS {
                    return Err(TopicError::new(
                        err::INVALID_PARTITIONS,
                        format!("partitions must be 1 to {MAX_PARTITIONS}"),
                    ));
                }
                let mut configs = TopicConfigs::default();
                for c in &t.configs {
                    configs.set(&c.name, c.value.as_deref())?;
                }
                self.topics
                    .create(
                        &s.principal,
                        NewTopic {
                            name: t.name.clone(),
                            partitions,
                            configs,
                        },
                        q.validate_only,
                    )
                    .await
            }
            .await;
            match result {
                Ok(info) => {
                    r.topic_id = info.topic_id;
                    r.num_partitions = info.partitions;
                    r.replication_factor = 1;
                    r.configs = Some(
                        info.effective()
                            .into_iter()
                            .map(|c| create_topics_response::CreatableTopicConfigs {
                                name: c.name.into(),
                                value: Some(c.value),
                                read_only: false,
                                config_source: if c.set {
                                    SOURCE_DYNAMIC_TOPIC
                                } else {
                                    SOURCE_DEFAULT
                                },
                                is_sensitive: false,
                                ..Default::default()
                            })
                            .collect(),
                    );
                }
                Err(e) => {
                    r.error_code = e.code;
                    r.error_message = Some(e.message);
                    r.configs = None;
                }
            }
            resp.topics.push(r);
        }
        body(&resp, v)
    }

    pub(super) async fn delete_topics(
        &self,
        s: &Session,
        q: DeleteTopicsRequest,
        v: i16,
    ) -> Result<Reply, ConnError> {
        let mut wanted: Vec<(Option<String>, uuid::Uuid)> = q
            .topic_names
            .into_iter()
            .map(|n| (Some(n), uuid::Uuid::nil()))
            .collect();
        wanted.extend(q.topics.into_iter().map(|t| (t.name, t.topic_id)));
        let mut resp = DeleteTopicsResponse::default();
        for (name, id) in wanted {
            let mut r = delete_topics_response::DeletableTopicResult {
                name: name.clone(),
                topic_id: id,
                ..Default::default()
            };
            let name = match name {
                Some(n) => Some(n),
                None => self
                    .topics
                    .list(&s.principal)
                    .await
                    .ok()
                    .and_then(|ts| ts.into_iter().find(|t| t.topic_id == id))
                    .map(|t| t.name),
            };
            match name {
                None => r.error_code = err::UNKNOWN_TOPIC_ID,
                Some(n) => {
                    r.name = Some(n.clone());
                    match self.topics.delete(&s.principal, &n).await {
                        Ok(info) => r.topic_id = info.topic_id,
                        Err(e) => {
                            r.error_code = e.code;
                            r.error_message = Some(e.message);
                        }
                    }
                }
            }
            if v < 6 && r.name.is_none() {
                r.name = Some(String::new());
            }
            resp.responses.push(r);
        }
        body(&resp, v)
    }

    pub(super) async fn create_partitions(
        &self,
        s: &Session,
        q: CreatePartitionsRequest,
        v: i16,
    ) -> Result<Reply, ConnError> {
        let mut seen = HashSet::new();
        let dups: HashSet<String> = q
            .topics
            .iter()
            .filter(|t| !seen.insert(t.name.clone()))
            .map(|t| t.name.clone())
            .collect();
        let mut resp = CreatePartitionsResponse::default();
        for t in q.topics {
            let res = if dups.contains(&t.name) {
                Err(TopicError::new(
                    err::INVALID_REQUEST,
                    "topic named twice in one request",
                ))
            } else if t.count <= 0 || t.count > MAX_PARTITIONS {
                Err(TopicError::new(
                    err::INVALID_PARTITIONS,
                    format!("partitions must be 1 to {MAX_PARTITIONS}"),
                ))
            } else {
                self.topics
                    .grow(&s.principal, &t.name, t.count, q.validate_only)
                    .await
            };
            let (code, msg) = match res {
                Ok(()) => (err::NONE, None),
                Err(e) => (e.code, Some(e.message)),
            };
            resp.results
                .push(create_partitions_response::CreatePartitionsTopicResult {
                    name: t.name,
                    error_code: code,
                    error_message: msg,
                    ..Default::default()
                });
        }
        body(&resp, v)
    }

    pub(super) async fn describe_configs(
        &self,
        s: &Session,
        q: DescribeConfigsRequest,
        v: i16,
    ) -> Result<Reply, ConnError> {
        let mut resp = DescribeConfigsResponse::default();
        for res in q.resources {
            let mut r = describe_configs_response::DescribeConfigsResult {
                resource_type: res.resource_type,
                resource_name: res.resource_name.clone(),
                error_message: None,
                ..Default::default()
            };
            match res.resource_type {
                RESOURCE_TOPIC => match self.topics.get(&s.principal, &res.resource_name).await {
                    Ok(Some(t)) => {
                        for c in t.effective() {
                            if res
                                .configuration_keys
                                .as_ref()
                                .is_some_and(|k| !k.iter().any(|k| k == c.name))
                            {
                                continue;
                            }
                            let source = if c.set {
                                SOURCE_DYNAMIC_TOPIC
                            } else {
                                SOURCE_DEFAULT
                            };
                            r.configs.push(
                                describe_configs_response::DescribeConfigsResourceResult {
                                    name: c.name.into(),
                                    value: Some(c.value.clone()),
                                    read_only: false,
                                    is_default: !c.set,
                                    config_source: source,
                                    is_sensitive: false,
                                    synonyms: if q.include_synonyms {
                                        vec![describe_configs_response::DescribeConfigsSynonym {
                                            name: c.synonym.into(),
                                            value: Some(c.synonym_value.clone()),
                                            source,
                                            ..Default::default()
                                        }]
                                    } else {
                                        Vec::new()
                                    },
                                    config_type: c.config_type,
                                    documentation: q.include_documentation.then(|| c.doc.into()),
                                    ..Default::default()
                                },
                            );
                        }
                    }
                    Ok(None) => {
                        r.error_code = err::UNKNOWN_TOPIC_OR_PARTITION;
                        r.error_message = Some(format!(
                            "39940 ER_SPRIG_STREAM_NOT_FOUND: no stream {}",
                            res.resource_name
                        ));
                    }
                    Err(e) => {
                        r.error_code = e.code;
                        r.error_message = Some(e.message);
                    }
                },
                // Broker configs are SprigDB settings; Kafka tools that
                // read them get an empty list.
                RESOURCE_BROKER => {}
                other => {
                    r.error_code = err::INVALID_REQUEST;
                    r.error_message = Some(format!("resource type {other} is not supported"));
                }
            }
            resp.results.push(r);
        }
        body(&resp, v)
    }

    pub(super) async fn alter_configs(
        &self,
        s: &Session,
        q: IncrementalAlterConfigsRequest,
        v: i16,
    ) -> Result<Reply, ConnError> {
        let mut resp = IncrementalAlterConfigsResponse::default();
        for res in q.resources {
            let result: Result<(), TopicError> = async {
                match res.resource_type {
                    RESOURCE_TOPIC => {}
                    RESOURCE_BROKER => {
                        return Err(TopicError::new(
                            err::POLICY_VIOLATION,
                            "broker configs are SprigDB settings: use sprigctl config set",
                        ));
                    }
                    other => {
                        return Err(TopicError::new(
                            err::INVALID_REQUEST,
                            format!("resource type {other} is not supported"),
                        ));
                    }
                }
                let t = self
                    .topics
                    .get(&s.principal, &res.resource_name)
                    .await?
                    .ok_or_else(|| {
                        TopicError::new(
                            err::UNKNOWN_TOPIC_OR_PARTITION,
                            format!("39940 ER_SPRIG_STREAM_NOT_FOUND: no stream {}", res.resource_name),
                        )
                    })?;
                let mut c = t.configs;
                for a in &res.configs {
                    match a.config_operation {
                        0 => c.set(&a.name, a.value.as_deref())?,
                        1 => c.set(&a.name, None)?,
                        2 | 3 => {
                            // APPEND and SUBTRACT are for list configs; the
                            // only one, cleanup.policy, holds one value here.
                            let current = t.effective();
                            let cur = current.iter().find(|x| x.name == a.name);
                            match (a.config_operation, cur, a.value.as_deref()) {
                                (2, Some(cv), Some(val)) if cv.config_type == 7 && cv.value == val => {}
                                _ => {
                                    return Err(TopicError::new(
                                        err::INVALID_CONFIG,
                                        format!(
                                            "{}: APPEND and SUBTRACT can't change it; a stream has one cleanup policy",
                                            a.name
                                        ),
                                    ));
                                }
                            }
                        }
                        op => {
                            return Err(TopicError::new(
                                err::INVALID_REQUEST,
                                format!("config operation {op} is not supported"),
                            ));
                        }
                    }
                }
                self.topics
                    .set_configs(&s.principal, &res.resource_name, c, q.validate_only)
                    .await
            }
            .await;
            let (code, msg) = match result {
                Ok(()) => (err::NONE, None),
                Err(e) => (e.code, Some(e.message)),
            };
            resp.responses.push(
                incremental_alter_configs_response::AlterConfigsResourceResponse {
                    error_code: code,
                    error_message: msg,
                    resource_type: res.resource_type,
                    resource_name: res.resource_name,
                    ..Default::default()
                },
            );
        }
        body(&resp, v)
    }
}
