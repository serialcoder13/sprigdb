//! Conversations, their messages and token usage, in the tenant's catalog
//! (`t<id>_sprig`, module `ai`, created the first time a gateway needs it
//! for a tenant).
//!
//! - **What is kept.** Message bodies hold what the user saw; tool
//!   results their gated form (shape only, unless sharing is on). Each
//!   model call adds a `system_note` with the SHA-256 of the exact payload
//!   sent (`sent_hash`) and, for `ai.audit_payload_days`, the payload
//!   itself.
//! - **Usage.** `ai_usage` counts tokens per UTC day, user and role:
//!   input read or written uncached, and output. Budgets read it.
//! - **Whose.** A conversation belongs to one user; every read checks it.
//! - The audit stream gets one `ai.turn` event per turn: the user, the
//!   role, the tools called and the hashes sent ([`crate::turn`]).

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use mysql_async::prelude::Queryable as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sprig_catalog::migrate::{MigrateOpts, Migration, Migrator, Module, Phase};
use sprig_common::ids::UserId;
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use tokio::sync::OnceCell;

use crate::budget::UsageToday;
use crate::provider::Usage;
use crate::role::Role;

/// The catalog module.
pub const MODULE: &str = "ai";

const DDL_CONVERSATIONS: &str = "CREATE TABLE IF NOT EXISTS ai_conversations (
  conv_id    BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  user_id    INT UNSIGNED NOT NULL,
  role       ENUM('dba','data') NOT NULL,
  title      VARCHAR(200) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  KEY by_user (user_id, updated_at)
) ENGINE=InnoDB";

const DDL_MESSAGES: &str = "CREATE TABLE IF NOT EXISTS ai_messages (
  msg_id     BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  conv_id    BIGINT UNSIGNED NOT NULL,
  kind       ENUM('user','assistant','tool_call','tool_result','proposal','system_note') NOT NULL,
  body       JSON NOT NULL,
  sent_hash  BINARY(32) NULL,
  created_at DATETIME(3) NOT NULL,
  KEY by_conv (conv_id, msg_id)
) ENGINE=InnoDB";

const DDL_USAGE: &str = "CREATE TABLE IF NOT EXISTS ai_usage (
  day           DATE NOT NULL,
  user_id       INT UNSIGNED NOT NULL,
  role          VARCHAR(16) NOT NULL,
  input_tokens  BIGINT UNSIGNED NOT NULL,
  output_tokens BIGINT UNSIGNED NOT NULL,
  calls         INT UNSIGNED NOT NULL,
  PRIMARY KEY (day, user_id, role)
) ENGINE=InnoDB";

const DDL_FEEDBACK: &str = "CREATE TABLE IF NOT EXISTS ai_feedback (
  fb_id       BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  user_id     INT UNSIGNED NOT NULL,
  msg_id      BIGINT UNSIGNED NULL,
  proposal_id BIGINT UNSIGNED NULL,
  vote        TINYINT NULL,
  outcome     ENUM('accepted','dismissed','failed') NULL,
  note        VARCHAR(500) NULL,
  created_at  DATETIME(3) NOT NULL
) ENGINE=InnoDB";

/// The module's migrations.
pub static AI_MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "ai_conversations, ai_messages, ai_usage, ai_feedback",
    statements: &[DDL_CONVERSATIONS, DDL_MESSAGES, DDL_USAGE, DDL_FEEDBACK],
}];

/// The module, for migrators.
pub const AI_MODULE: Module = Module {
    name: MODULE,
    migrations: AI_MIGRATIONS,
};

/// A conversation's id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ConvId(pub u64);

/// What a stored message is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MsgKind {
    /// The user's message.
    User,
    /// The model's answer (its whole message: text, thinking, tool calls).
    Assistant,
    /// A tool call, as run.
    ToolCall,
    /// A tool's gated result.
    ToolResult,
    /// A proposal shown to the user.
    Proposal,
    /// A note: what was sent, a stop, an error.
    SystemNote,
}

impl MsgKind {
    /// The catalog's name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            MsgKind::User => "user",
            MsgKind::Assistant => "assistant",
            MsgKind::ToolCall => "tool_call",
            MsgKind::ToolResult => "tool_result",
            MsgKind::Proposal => "proposal",
            MsgKind::SystemNote => "system_note",
        }
    }

    /// The kind named `s`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        [
            MsgKind::User,
            MsgKind::Assistant,
            MsgKind::ToolCall,
            MsgKind::ToolResult,
            MsgKind::Proposal,
            MsgKind::SystemNote,
        ]
        .into_iter()
        .find(|k| k.as_str() == s)
    }
}

/// A conversation's header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Conversation {
    /// Its id.
    pub id: ConvId,
    /// Its owner.
    pub user: u32,
    /// `dba` or `data`.
    pub role: Role,
    /// Its title.
    pub title: String,
    /// Created, ms since the Unix epoch.
    pub created_ms: i64,
    /// Last changed, ms since the Unix epoch.
    pub updated_ms: i64,
}

/// One stored message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredMessage {
    /// Its id.
    pub id: u64,
    /// What it is.
    pub kind: MsgKind,
    /// Its body.
    pub body: Value,
    /// For a payload note, the SHA-256 of what was sent.
    pub sent_hash: Option<[u8; 32]>,
    /// When, ms since the Unix epoch.
    pub created_ms: i64,
}

/// Where conversations, messages and usage are kept.
#[async_trait]
pub trait AuditStore: Send + Sync {
    /// Starts a conversation for `user` (tenant roles only).
    async fn create(&self, user: UserId, role: Role, title: &str) -> Result<ConvId>;
    /// The conversation `id`, if it is `user`'s.
    async fn get(&self, user: UserId, id: ConvId) -> Result<Option<Conversation>>;
    /// `user`'s conversations, newest first.
    async fn list(&self, user: UserId, limit: u32) -> Result<Vec<Conversation>>;
    /// Appends a message; returns its id.
    async fn append(
        &self,
        conv: ConvId,
        kind: MsgKind,
        body: &Value,
        sent_hash: Option<[u8; 32]>,
    ) -> Result<u64>;
    /// The conversation's messages, oldest first.
    async fn messages(&self, conv: ConvId) -> Result<Vec<StoredMessage>>;
    /// Adds one model call's tokens to today's usage.
    async fn add_usage(&self, user: UserId, role: Role, usage: &Usage) -> Result<()>;
    /// Tokens used today by the tenant and by `user`.
    async fn usage_today(&self, user: UserId) -> Result<UsageToday>;
    /// Drops payloads older than `days` from their notes, keeping the
    /// hashes; returns how many.
    async fn purge_payloads(&self, days: u64) -> Result<u64>;
}

fn tenant_role(role: Role) -> Result<&'static str> {
    if role.is_tenant() {
        Ok(role.as_str())
    } else {
        Err(SprigError::internal(
            "only tenant assistants keep conversations in the catalog",
        ))
    }
}

fn title_of(t: &str) -> String {
    let t = t.trim();
    let t: String = t.chars().take(200).collect();
    if t.is_empty() {
        "Untitled".to_owned()
    } else {
        t
    }
}

// ---- in memory ----

#[derive(Default)]
struct Mem {
    convs: BTreeMap<u64, Conversation>,
    msgs: Vec<(u64, StoredMessage)>,
    usage: BTreeMap<(u32, Role), (u64, u64, u32)>,
    next: u64,
}

/// An in-memory store, for tests and evaluation runs.
#[derive(Default)]
pub struct MemoryAudit {
    inner: Mutex<Mem>,
}

impl std::fmt::Debug for MemoryAudit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryAudit").finish_non_exhaustive()
    }
}

impl MemoryAudit {
    fn lock(&self) -> std::sync::MutexGuard<'_, Mem> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Sets the tokens `user` used today (input, output), for tests.
    pub fn set_usage(&self, user: UserId, role: Role, input: u64, output: u64) {
        self.lock().usage.insert((user.0, role), (input, output, 1));
    }
}

#[async_trait]
impl AuditStore for MemoryAudit {
    async fn create(&self, user: UserId, role: Role, title: &str) -> Result<ConvId> {
        tenant_role(role)?;
        let mut m = self.lock();
        m.next += 1;
        let id = m.next;
        m.convs.insert(
            id,
            Conversation {
                id: ConvId(id),
                user: user.0,
                role,
                title: title_of(title),
                created_ms: 0,
                updated_ms: 0,
            },
        );
        Ok(ConvId(id))
    }

    async fn get(&self, user: UserId, id: ConvId) -> Result<Option<Conversation>> {
        Ok(self
            .lock()
            .convs
            .get(&id.0)
            .filter(|c| c.user == user.0)
            .cloned())
    }

    async fn list(&self, user: UserId, limit: u32) -> Result<Vec<Conversation>> {
        Ok(self
            .lock()
            .convs
            .values()
            .rev()
            .filter(|c| c.user == user.0)
            .take(limit as usize)
            .cloned()
            .collect())
    }

    async fn append(
        &self,
        conv: ConvId,
        kind: MsgKind,
        body: &Value,
        sent_hash: Option<[u8; 32]>,
    ) -> Result<u64> {
        let mut m = self.lock();
        m.next += 1;
        let id = m.next;
        m.msgs.push((
            conv.0,
            StoredMessage {
                id,
                kind,
                body: body.clone(),
                sent_hash,
                created_ms: 0,
            },
        ));
        Ok(id)
    }

    async fn messages(&self, conv: ConvId) -> Result<Vec<StoredMessage>> {
        Ok(self
            .lock()
            .msgs
            .iter()
            .filter(|(c, _)| *c == conv.0)
            .map(|(_, m)| m.clone())
            .collect())
    }

    async fn add_usage(&self, user: UserId, role: Role, usage: &Usage) -> Result<()> {
        let mut m = self.lock();
        let e = m.usage.entry((user.0, role)).or_default();
        e.0 += usage.input_tokens + usage.cache_write_tokens;
        e.1 += usage.output_tokens;
        e.2 += 1;
        Ok(())
    }

    async fn usage_today(&self, user: UserId) -> Result<UsageToday> {
        let m = self.lock();
        let mut u = UsageToday::default();
        for ((who, _), (i, o, _)) in &m.usage {
            u.tenant += i + o;
            if *who == user.0 {
                u.user += i + o;
            }
        }
        Ok(u)
    }

    async fn purge_payloads(&self, _days: u64) -> Result<u64> {
        Ok(0)
    }
}

// ---- the tenant catalog ----

/// The store in a tenant's catalog schema.
pub struct CatalogAudit {
    pool: mysql_async::Pool,
    schema: String,
    ready: OnceCell<()>,
}

impl std::fmt::Debug for CatalogAudit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogAudit")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

fn unavailable(e: mysql_async::Error) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        "the tenant's catalog is unreachable",
    )
    .with_source(e)
}

type ConvRow = (u64, u32, String, String, i64, i64);
type MsgRow = (u64, String, String, Option<Vec<u8>>, i64);

const MS: &str = "CAST(TIMESTAMPDIFF(MICROSECOND, '1970-01-01', {}) DIV 1000 AS SIGNED)";

fn ms(col: &str) -> String {
    MS.replace("{}", col)
}

impl CatalogAudit {
    /// The store in `schema` (`t<id>_sprig`), reached through `pool` (the
    /// catalog writer's).
    #[must_use]
    pub fn new(pool: mysql_async::Pool, schema: impl Into<String>) -> Self {
        Self {
            pool,
            schema: schema.into(),
            ready: OnceCell::new(),
        }
    }

    fn t(&self, table: &str) -> String {
        format!("{}.{}", quote(&self.schema), quote(table))
    }

    /// A connection, after creating the module the first time.
    async fn conn(&self) -> Result<mysql_async::Conn> {
        let mut conn = self.pool.get_conn().await.map_err(unavailable)?;
        self.ready
            .get_or_try_init(|| async {
                let have: Option<u64> = conn
                    .exec_first(
                        "SELECT COUNT(*) FROM information_schema.TABLES \
                         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'ai_messages'",
                        (self.schema.as_str(),),
                    )
                    .await
                    .map_err(unavailable)?;
                if have.unwrap_or(0) == 0 {
                    Migrator::new(&[AI_MODULE])?
                        .run(&mut conn, &self.schema, &MigrateOpts::default())
                        .await?;
                }
                Ok::<(), SprigError>(())
            })
            .await?;
        Ok(conn)
    }

    fn conv(r: ConvRow) -> Option<Conversation> {
        Some(Conversation {
            id: ConvId(r.0),
            user: r.1,
            role: Role::parse(&r.2)?,
            title: r.3,
            created_ms: r.4,
            updated_ms: r.5,
        })
    }
}

#[async_trait]
impl AuditStore for CatalogAudit {
    async fn create(&self, user: UserId, role: Role, title: &str) -> Result<ConvId> {
        let role = tenant_role(role)?;
        let mut c = self.conn().await?;
        c.exec_drop(
            format!(
                "INSERT INTO {} (user_id, role, title, created_at, updated_at) \
                 VALUES (?, ?, ?, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))",
                self.t("ai_conversations")
            ),
            (user.0, role, title_of(title)),
        )
        .await
        .map_err(unavailable)?;
        Ok(ConvId(c.last_insert_id().unwrap_or(0)))
    }

    async fn get(&self, user: UserId, id: ConvId) -> Result<Option<Conversation>> {
        let mut c = self.conn().await?;
        let r: Option<ConvRow> = c
            .exec_first(
                format!(
                    "SELECT conv_id, user_id, role, title, {}, {} FROM {} \
                     WHERE conv_id = ? AND user_id = ?",
                    ms("created_at"),
                    ms("updated_at"),
                    self.t("ai_conversations")
                ),
                (id.0, user.0),
            )
            .await
            .map_err(unavailable)?;
        Ok(r.and_then(Self::conv))
    }

    async fn list(&self, user: UserId, limit: u32) -> Result<Vec<Conversation>> {
        let mut c = self.conn().await?;
        let rows: Vec<ConvRow> = c
            .exec(
                format!(
                    "SELECT conv_id, user_id, role, title, {}, {} FROM {} \
                     WHERE user_id = ? ORDER BY updated_at DESC, conv_id DESC LIMIT ?",
                    ms("created_at"),
                    ms("updated_at"),
                    self.t("ai_conversations")
                ),
                (user.0, limit),
            )
            .await
            .map_err(unavailable)?;
        Ok(rows.into_iter().filter_map(Self::conv).collect())
    }

    async fn append(
        &self,
        conv: ConvId,
        kind: MsgKind,
        body: &Value,
        sent_hash: Option<[u8; 32]>,
    ) -> Result<u64> {
        let mut c = self.conn().await?;
        c.exec_drop(
            format!(
                "INSERT INTO {} (conv_id, kind, body, sent_hash, created_at) \
                 VALUES (?, ?, ?, ?, UTC_TIMESTAMP(3))",
                self.t("ai_messages")
            ),
            (
                conv.0,
                kind.as_str(),
                body.to_string(),
                sent_hash.map(|h| h.to_vec()),
            ),
        )
        .await
        .map_err(unavailable)?;
        let id = c.last_insert_id().unwrap_or(0);
        c.exec_drop(
            format!(
                "UPDATE {} SET updated_at = UTC_TIMESTAMP(3) WHERE conv_id = ?",
                self.t("ai_conversations")
            ),
            (conv.0,),
        )
        .await
        .map_err(unavailable)?;
        Ok(id)
    }

    async fn messages(&self, conv: ConvId) -> Result<Vec<StoredMessage>> {
        let mut c = self.conn().await?;
        let rows: Vec<MsgRow> = c
            .exec(
                format!(
                    "SELECT msg_id, kind, body, sent_hash, {} FROM {} \
                     WHERE conv_id = ? ORDER BY msg_id",
                    ms("created_at"),
                    self.t("ai_messages")
                ),
                (conv.0,),
            )
            .await
            .map_err(unavailable)?;
        Ok(rows
            .into_iter()
            .filter_map(|(id, kind, body, hash, at)| {
                Some(StoredMessage {
                    id,
                    kind: MsgKind::parse(&kind)?,
                    body: serde_json::from_str(&body).unwrap_or(Value::Null),
                    sent_hash: hash.and_then(|h| <[u8; 32]>::try_from(h.as_slice()).ok()),
                    created_ms: at,
                })
            })
            .collect())
    }

    async fn add_usage(&self, user: UserId, role: Role, usage: &Usage) -> Result<()> {
        let mut c = self.conn().await?;
        c.exec_drop(
            format!(
                "INSERT INTO {} (day, user_id, role, input_tokens, output_tokens, calls) \
                 VALUES (UTC_DATE(), ?, ?, ?, ?, 1) \
                 ON DUPLICATE KEY UPDATE input_tokens = input_tokens + VALUES(input_tokens), \
                 output_tokens = output_tokens + VALUES(output_tokens), calls = calls + 1",
                self.t("ai_usage")
            ),
            (
                user.0,
                role.as_str(),
                usage.input_tokens + usage.cache_write_tokens,
                usage.output_tokens,
            ),
        )
        .await
        .map_err(unavailable)?;
        Ok(())
    }

    async fn usage_today(&self, user: UserId) -> Result<UsageToday> {
        let mut c = self.conn().await?;
        let r: Option<(Option<u64>, Option<u64>)> = c
            .exec_first(
                format!(
                    "SELECT CAST(SUM(input_tokens + output_tokens) AS UNSIGNED), \
                     CAST(SUM(IF(user_id = ?, input_tokens + output_tokens, 0)) AS UNSIGNED) \
                     FROM {} WHERE day = UTC_DATE()",
                    self.t("ai_usage")
                ),
                (user.0,),
            )
            .await
            .map_err(unavailable)?;
        let (t, u) = r.unwrap_or_default();
        Ok(UsageToday {
            tenant: t.unwrap_or(0),
            user: u.unwrap_or(0),
        })
    }

    async fn purge_payloads(&self, days: u64) -> Result<u64> {
        let mut c = self.conn().await?;
        c.exec_drop(
            format!(
                "UPDATE {} SET body = JSON_OBJECT('sha256', LOWER(HEX(sent_hash)), 'payload_purged', TRUE) \
                 WHERE kind = 'system_note' AND sent_hash IS NOT NULL \
                 AND JSON_EXISTS(body, '$.payload') \
                 AND created_at < UTC_TIMESTAMP(3) - INTERVAL ? DAY",
                self.t("ai_messages")
            ),
            (days,),
        )
        .await
        .map_err(unavailable)?;
        Ok(c.affected_rows())
    }
}

#[cfg(test)]
mod live_tests;
