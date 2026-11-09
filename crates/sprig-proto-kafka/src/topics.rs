//! Topics as the broker sees them, and the [`Topics`] trait that maps
//! Kafka's topic administration onto stream DDL (chapter 45 build step 13).
//!
//! A Kafka topic is a stream (`streams` in the tenant catalog). The
//! gateway implements [`Topics`]; until `sprig-streams`' catalog API is in,
//! it uses the interim in-memory adapter [`crate::mem::MemTopics`].
//!
//! **Topic configs.** Four Kafka configs map onto stream settings:
//!
//! | Kafka config | Stream setting | Conversion |
//! |---|---|---|
//! | `retention.ms` | `stream.retention_hours` | Rounded up to whole hours; `-1` (no time limit) is 0 |
//! | `retention.bytes` | `stream.retention_bytes` | Bytes per partition; `-1` (no limit) is 0 |
//! | `cleanup.policy` | `CLEANUP` (`streams.cleanup`) | `delete` or `compact`; both at once is refused |
//! | `max.message.bytes` | `stream.max_message_bytes` | As is |
//!
//! Replication and segment configs that have no meaning in SprigDB
//! ([`IGNORED_CONFIGS`]) are accepted and ignored, so tools that always
//! send them keep working; any other config is refused with
//! `INVALID_CONFIG`.

use async_trait::async_trait;
use uuid::Uuid;

use crate::protocol::err;

/// What kind of stream a topic is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicKind {
    /// An event stream (`kind = 'events'`).
    Events,
    /// A change topic (`<object>.changes`).
    Changes,
}

/// `cleanup.policy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cleanup {
    /// Retention by time and size.
    Delete,
    /// Compaction by key.
    Compact,
}

impl Cleanup {
    /// The Kafka value.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Delete => "delete",
            Self::Compact => "compact",
        }
    }
}

/// Configs set on the stream itself (`None`: inherited).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TopicConfigs {
    /// `retention.ms`.
    pub retention_ms: Option<i64>,
    /// `retention.bytes`.
    pub retention_bytes: Option<i64>,
    /// `cleanup.policy`.
    pub cleanup: Option<Cleanup>,
    /// `max.message.bytes`.
    pub max_message_bytes: Option<i32>,
}

/// The inherited values (database, tenant, global settings, defaults).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicDefaults {
    /// `stream.retention_hours` in milliseconds.
    pub retention_ms: i64,
    /// `stream.retention_bytes` (`-1` for none).
    pub retention_bytes: i64,
    /// Delete.
    pub cleanup: Cleanup,
    /// `stream.max_message_bytes`.
    pub max_message_bytes: i32,
    /// `stream.partitions_default`.
    pub partitions: i32,
}

impl Default for TopicDefaults {
    fn default() -> Self {
        Self {
            retention_ms: 168 * 3_600_000,
            retention_bytes: -1,
            cleanup: Cleanup::Delete,
            max_message_bytes: 1_048_576,
            partitions: 12,
        }
    }
}

/// Configs Kafka tools often send that have no meaning in SprigDB:
/// accepted and ignored.
pub const IGNORED_CONFIGS: &[&str] = &[
    "min.insync.replicas",
    "unclean.leader.election.enable",
    "segment.bytes",
    "segment.ms",
    "segment.index.bytes",
    "segment.jitter.ms",
    "index.interval.bytes",
    "flush.messages",
    "flush.ms",
    "preallocate",
    "file.delete.delay.ms",
    "leader.replication.throttled.replicas",
    "follower.replication.throttled.replicas",
    "message.downconversion.enable",
    "compression.type",
    "message.timestamp.type",
    "delete.retention.ms",
    "min.compaction.lag.ms",
    "max.compaction.lag.ms",
    "min.cleanable.dirty.ratio",
];

/// The four mapped configs, in the order DescribeConfigs lists them.
pub const CONFIG_NAMES: &[&str] = &[
    "cleanup.policy",
    "max.message.bytes",
    "retention.bytes",
    "retention.ms",
];

/// A topic administration error: a Kafka error code, SprigDB's number when
/// there is one, and a message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct TopicError {
    /// The Kafka error code.
    pub code: i16,
    /// The message, starting with SprigDB's error number when there is one.
    pub message: String,
}

impl TopicError {
    /// An error with `code` and `message`.
    #[must_use]
    pub fn new(code: i16, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    fn config(message: impl Into<String>) -> Self {
        Self::new(err::INVALID_CONFIG, message)
    }
}

impl TopicConfigs {
    /// Sets (`Some`) or resets (`None`) one Kafka config.
    ///
    /// # Errors
    /// `INVALID_CONFIG` for an unknown config or a bad value.
    pub fn set(&mut self, name: &str, value: Option<&str>) -> Result<(), TopicError> {
        let int = |v: &str| -> Result<i64, TopicError> {
            v.trim()
                .parse::<i64>()
                .map_err(|_| TopicError::config(format!("{name}: '{v}' is not a number")))
        };
        match name {
            "retention.ms" | "retention.bytes" => {
                let v = value.map(int).transpose()?;
                if v.is_some_and(|v| v < -1) {
                    return Err(TopicError::config(format!("{name} must be -1 or more")));
                }
                if name == "retention.ms" {
                    self.retention_ms = v;
                } else {
                    self.retention_bytes = v;
                }
            }
            "max.message.bytes" => {
                let v = value.map(int).transpose()?;
                let v = v
                    .map(|v| i32::try_from(v).ok().filter(|v| *v >= 0))
                    .map(|v| {
                        v.ok_or_else(|| TopicError::config("max.message.bytes is out of range"))
                    })
                    .transpose()?;
                self.max_message_bytes = v;
            }
            "cleanup.policy" => {
                self.cleanup = match value.map(|v| v.trim().to_ascii_lowercase()) {
                    None => None,
                    Some(v) if v == "delete" => Some(Cleanup::Delete),
                    Some(v) if v == "compact" => Some(Cleanup::Compact),
                    Some(v) if v.contains(',') => {
                        return Err(TopicError::config(
                            "cleanup.policy: a SprigDB stream is either delete or compact, not both",
                        ));
                    }
                    Some(v) => {
                        return Err(TopicError::config(format!(
                            "cleanup.policy: '{v}' is not delete or compact"
                        )));
                    }
                };
            }
            n if IGNORED_CONFIGS.contains(&n) => {}
            n => {
                return Err(TopicError::config(format!(
                    "{n} is not a topic config SprigDB supports (retention.ms, retention.bytes, \
                     cleanup.policy, max.message.bytes)"
                )));
            }
        }
        Ok(())
    }
}

/// `retention.ms` as `stream.retention_hours`: rounded up to whole hours,
/// `-1` (no time limit) as 0.
#[must_use]
pub fn retention_hours(retention_ms: i64) -> u64 {
    if retention_ms < 0 {
        0
    } else {
        u64::try_from(retention_ms)
            .unwrap_or(0)
            .div_ceil(3_600_000)
            .max(1)
    }
}

/// A topic: a stream of the tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicInfo {
    /// The Kafka topic name, `<database>.<stream>`.
    pub name: String,
    /// `streams.stream_id` (= `objects.object_id`).
    pub stream_id: u64,
    /// The topic id Metadata v10+ shows.
    pub topic_id: Uuid,
    /// Partitions.
    pub partitions: i32,
    /// Event stream or change topic.
    pub kind: TopicKind,
    /// Configs set on the stream.
    pub configs: TopicConfigs,
    /// Inherited values.
    pub defaults: TopicDefaults,
}

/// One config of a topic, as DescribeConfigs shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigValue {
    /// The Kafka name.
    pub name: &'static str,
    /// The effective value.
    pub value: String,
    /// Set on the stream (`DYNAMIC_TOPIC_CONFIG`), else inherited
    /// (`DEFAULT_CONFIG`).
    pub set: bool,
    /// Kafka's config type: 3 INT, 5 LONG, 7 LIST.
    pub config_type: i8,
    /// The SprigDB setting it maps onto.
    pub synonym: &'static str,
    /// The setting's value in its own unit.
    pub synonym_value: String,
    /// A one-line description.
    pub doc: &'static str,
}

impl TopicInfo {
    /// The effective configs, in [`CONFIG_NAMES`] order.
    #[must_use]
    pub fn effective(&self) -> Vec<ConfigValue> {
        let c = &self.configs;
        let d = &self.defaults;
        let ms = c.retention_ms.unwrap_or(d.retention_ms);
        let bytes = c.retention_bytes.unwrap_or(d.retention_bytes);
        let cleanup = c.cleanup.unwrap_or(d.cleanup);
        let max = c.max_message_bytes.unwrap_or(d.max_message_bytes);
        vec![
            ConfigValue {
                name: "cleanup.policy",
                value: cleanup.name().into(),
                set: c.cleanup.is_some(),
                config_type: 7,
                synonym: "streams.cleanup",
                synonym_value: cleanup.name().into(),
                doc: "delete: retention by time and size; compact: keep the newest record per key",
            },
            ConfigValue {
                name: "max.message.bytes",
                value: max.to_string(),
                set: c.max_message_bytes.is_some(),
                config_type: 3,
                synonym: "stream.max_message_bytes",
                synonym_value: max.to_string(),
                doc: "The largest record",
            },
            ConfigValue {
                name: "retention.bytes",
                value: bytes.to_string(),
                set: c.retention_bytes.is_some(),
                config_type: 5,
                synonym: "stream.retention_bytes",
                synonym_value: bytes.max(0).to_string(),
                doc: "Size retention per partition; -1 for none",
            },
            ConfigValue {
                name: "retention.ms",
                value: ms.to_string(),
                set: c.retention_ms.is_some(),
                config_type: 5,
                synonym: "stream.retention_hours",
                synonym_value: retention_hours(ms).to_string(),
                doc: "Time retention, kept in whole hours; -1 for none",
            },
        ]
    }
}

/// A topic to create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTopic {
    /// `<database>.<stream>`.
    pub name: String,
    /// Partitions (resolved: never -1).
    pub partitions: i32,
    /// Configs to set on the stream.
    pub configs: TopicConfigs,
}

/// Who asks: the tenant and user of the connection.
#[derive(Clone)]
pub struct Principal {
    /// The tenant id.
    pub tenant_id: u64,
    /// The tenant name.
    pub tenant: String,
    /// The user name.
    pub user: String,
    /// The user id in the tenant catalog, if any.
    pub user_id: Option<u64>,
    /// The identity layer's own principal (the gateway keeps chapter 14's
    /// `Principal` here, to run statements as the user).
    pub handle: Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>,
}

impl std::fmt::Debug for Principal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Principal")
            .field("tenant_id", &self.tenant_id)
            .field("tenant", &self.tenant)
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

/// Topic administration over streams. Every call carries the principal,
/// so the implementation checks chapter 29's privileges (`CREATE`, `ALTER`,
/// `DROP` on the database or stream, `SELECT` to see a topic).
#[async_trait]
pub trait Topics: Send + Sync + 'static {
    /// Every topic the principal may see.
    async fn list(&self, p: &Principal) -> Result<Vec<TopicInfo>, TopicError>;
    /// One topic by name.
    async fn get(&self, p: &Principal, name: &str) -> Result<Option<TopicInfo>, TopicError>;
    /// The values a new topic inherits.
    async fn defaults(&self, p: &Principal, name: &str) -> Result<TopicDefaults, TopicError>;
    /// `CREATE STREAM`; with `validate_only`, only the checks.
    async fn create(
        &self,
        p: &Principal,
        t: NewTopic,
        validate_only: bool,
    ) -> Result<TopicInfo, TopicError>;
    /// `DROP STREAM`.
    async fn delete(&self, p: &Principal, name: &str) -> Result<TopicInfo, TopicError>;
    /// `ALTER STREAM ... PARTITIONS n` (growth only).
    async fn grow(
        &self,
        p: &Principal,
        name: &str,
        partitions: i32,
        validate_only: bool,
    ) -> Result<(), TopicError>;
    /// Replaces the configs set on the stream.
    async fn set_configs(
        &self,
        p: &Principal,
        name: &str,
        c: TopicConfigs,
        validate_only: bool,
    ) -> Result<(), TopicError>;
}

/// Whether `name` is a legal Kafka topic name: 1 to 249 characters of
/// `[a-zA-Z0-9._-]`, not `.` or `..`.
#[must_use]
pub fn legal_topic_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 249
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configs_map_and_refuse() {
        let mut c = TopicConfigs::default();
        c.set("retention.ms", Some("86400000")).unwrap();
        c.set("cleanup.policy", Some("compact")).unwrap();
        c.set("min.insync.replicas", Some("2")).unwrap();
        assert_eq!(c.retention_ms, Some(86_400_000));
        assert_eq!(c.cleanup, Some(Cleanup::Compact));
        assert_eq!(
            c.set("cleanup.policy", Some("delete,compact"))
                .unwrap_err()
                .code,
            40
        );
        assert_eq!(c.set("retention.ms", Some("-2")).unwrap_err().code, 40);
        assert_eq!(
            c.set("message.format.version", Some("3.0"))
                .unwrap_err()
                .code,
            40
        );
        c.set("retention.ms", None).unwrap();
        assert_eq!(c.retention_ms, None);
    }

    #[test]
    fn retention_rounds_up_to_hours() {
        assert_eq!(retention_hours(-1), 0);
        assert_eq!(retention_hours(60_000), 1);
        assert_eq!(retention_hours(604_800_000), 168);
        assert_eq!(retention_hours(3_600_001), 2);
    }

    #[test]
    fn topic_names() {
        assert!(legal_topic_name("shop.order_events"));
        assert!(legal_topic_name("shop.orders.changes"));
        assert!(!legal_topic_name(".."));
        assert!(!legal_topic_name("a b"));
        assert!(!legal_topic_name(&"x".repeat(250)));
    }
}
