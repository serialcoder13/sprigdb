//! The `ai.*` keys, registered here. Everything is off by default: no
//! request reaches a provider until an operator configures one (a provider
//! record and `ai.model.<role>`) and a tenant admin turns `ai.enabled` on.
//!
//! | Key | Default | Scopes |
//! |---|---|---|
//! | `ai.enabled` | `false` | tenant, global |
//! | `ai.share_data` | `false` | database, tenant, global |
//! | `ai.model.admin`, `.triage`, `.dba`, `.data` | empty | global |
//! | `ai.effort.admin`, `.triage`, `.dba`, `.data` | `medium` (`low` for triage) | global |
//! | `ai.budget.tenant_tokens_day` | `2000000` | tenant, global |
//! | `ai.budget.user_tokens_day` | `200000` | tenant, global |
//! | `ai.max_tool_rounds` | `12` | global |
//! | `ai.timeout_seconds` | `120` | global |
//! | `ai.max_output_tokens` | `16000` | global |
//! | `ai.audit_payload_days` | `30` | tenant, global |
//! | `ai.mcp.enabled` | `false` | tenant, global |
//! | `ai.mcp.tools` | `read` | tenant, global |
//! | `ai.mcp.calls_per_minute` | `120` | tenant, global |
//!
//! Values are read through [`Lookup`], which the gateway implements over a
//! caller's tenant (and database) context; [`TurnSettings::read`] reads
//! the ones a turn needs at once.

use std::borrow::Cow;
use std::time::Duration;

use sprig_settings::{Apply, HardBounds, Scopes, Setting, SettingCtx};

use crate::budget::Budget;
use crate::provider::Effort;
use crate::role::Role;

type Text = Cow<'static, str>;

/// Tenant and global.
const TENANT_GLOBAL: Scopes = Scopes(0b10_0100);
/// Database, tenant and global.
const DB_TENANT_GLOBAL: Scopes = Scopes(0b10_0110);
/// Global only.
const GLOBAL: Scopes = Scopes(0b10_0000);

const EFFORTS: &[&str] = &["low", "medium", "high", "xhigh", "max"];

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $hard:expr, $op:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: $hard,
            apply: Apply::Live,
            operator_only: $op,
            owner: 54,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key!(
    /// `ai.enabled`.
    ENABLED: bool = "ai.enabled", false, TENANT_GLOBAL, HardBounds::NONE, false,
    "Turn on the tenant's AI assistants. Off until a provider is configured and a tenant admin turns it on"
);
key!(
    /// `ai.share_data`.
    SHARE_DATA: bool = "ai.share_data", false, DB_TENANT_GLOBAL, HardBounds::NONE, false,
    "Let the assistants send rows, results and value statistics to the AI provider; off sends schema, plans and statistics without values"
);
key!(
    /// `ai.model.admin`.
    MODEL_ADMIN: Text = "ai.model.admin", Cow::Borrowed(""), GLOBAL, HardBounds::NONE, true,
    "provider/model for the administration agent, such as anthropic/claude-opus-5-5; empty: none"
);
key!(
    /// `ai.model.triage`.
    MODEL_TRIAGE: Text = "ai.model.triage", Cow::Borrowed(""), GLOBAL, HardBounds::NONE, true,
    "provider/model that screens administration intake for noise; empty: none"
);
key!(
    /// `ai.model.dba`.
    MODEL_DBA: Text = "ai.model.dba", Cow::Borrowed(""), GLOBAL, HardBounds::NONE, true,
    "provider/model for the DBA assistant; empty: none"
);
key!(
    /// `ai.model.data`.
    MODEL_DATA: Text = "ai.model.data", Cow::Borrowed(""), GLOBAL, HardBounds::NONE, true,
    "provider/model for the data specialist; empty: none"
);
key!(
    /// `ai.effort.admin`.
    EFFORT_ADMIN: Text = "ai.effort.admin", Cow::Borrowed("medium"), GLOBAL, HardBounds::one_of(EFFORTS), true,
    "The administration agent's effort: low, medium, high, xhigh or max, for models that support it"
);
key!(
    /// `ai.effort.triage`.
    EFFORT_TRIAGE: Text = "ai.effort.triage", Cow::Borrowed("low"), GLOBAL, HardBounds::one_of(EFFORTS), true,
    "The triage model's effort: low, medium, high, xhigh or max"
);
key!(
    /// `ai.effort.dba`.
    EFFORT_DBA: Text = "ai.effort.dba", Cow::Borrowed("medium"), GLOBAL, HardBounds::one_of(EFFORTS), true,
    "The DBA assistant's effort: low, medium, high, xhigh or max"
);
key!(
    /// `ai.effort.data`.
    EFFORT_DATA: Text = "ai.effort.data", Cow::Borrowed("medium"), GLOBAL, HardBounds::one_of(EFFORTS), true,
    "The data specialist's effort: low, medium, high, xhigh or max"
);
key!(
    /// `ai.budget.tenant_tokens_day`.
    BUDGET_TENANT_TOKENS_DAY: u64 = "ai.budget.tenant_tokens_day", 2_000_000, TENANT_GLOBAL,
    HardBounds::range(0, 1_000_000_000_000), false,
    "Tokens all of a tenant's assistant turns may use per UTC day before they stop; 0 allows none"
);
key!(
    /// `ai.budget.user_tokens_day`.
    BUDGET_USER_TOKENS_DAY: u64 = "ai.budget.user_tokens_day", 200_000, TENANT_GLOBAL,
    HardBounds::range(0, 1_000_000_000_000), false,
    "Tokens one user's assistant turns may use per UTC day before they stop; 0 allows none"
);
key!(
    /// `ai.max_tool_rounds`.
    MAX_TOOL_ROUNDS: u64 = "ai.max_tool_rounds", 12, GLOBAL, HardBounds::range(0, 100), true,
    "Tool rounds one assistant turn may take"
);
key!(
    /// `ai.timeout_seconds`.
    TIMEOUT_SECONDS: u64 = "ai.timeout_seconds", 120, GLOBAL, HardBounds::range(5, 3600), true,
    "Wall time of one assistant turn, model calls and tools together"
);
key!(
    /// `ai.max_output_tokens`.
    MAX_OUTPUT_TOKENS: u64 = "ai.max_output_tokens", 16_000, GLOBAL, HardBounds::range(256, 128_000), true,
    "Output tokens one model call may produce, reasoning included"
);
key!(
    /// `ai.audit_payload_days`.
    AUDIT_PAYLOAD_DAYS: u64 = "ai.audit_payload_days", 30, TENANT_GLOBAL, HardBounds::range(0, 3650), false,
    "Days the exact payloads sent to the AI provider are kept with the conversation; 0 keeps only their hashes"
);

key!(
    /// `ai.mcp.enabled`.
    MCP_ENABLED: bool = "ai.mcp.enabled", false, TENANT_GLOBAL, HardBounds::NONE, false,
    "Turn on the MCP server (/v1/mcp) for the tenant, so outside AI agents can use the assistants' tools as the signed-in user; ai.enabled must be on too"
);
key!(
    /// `ai.mcp.tools`.
    MCP_TOOLS: Text = "ai.mcp.tools", Cow::Borrowed("read"), TENANT_GLOBAL, HardBounds::NONE, false,
    "The tools MCP clients may call, comma-separated: tool names, read (every read tool of the role) and propose (every proposal tool of the role)"
);
key!(
    /// `ai.mcp.calls_per_minute`.
    MCP_CALLS_PER_MINUTE: u64 = "ai.mcp.calls_per_minute", 120, TENANT_GLOBAL,
    HardBounds::range(1, 100_000), false,
    "MCP tool calls one user may make per minute; more are refused until the minute is over"
);

/// What the MCP server reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpSettings {
    /// `ai.enabled`.
    pub ai_enabled: bool,
    /// `ai.mcp.enabled`.
    pub enabled: bool,
    /// `ai.mcp.tools`.
    pub tools: String,
    /// `ai.mcp.calls_per_minute`.
    pub calls_per_minute: u64,
}

impl McpSettings {
    /// Reads them from `l` (a tenant's context).
    #[must_use]
    pub fn read(l: &dyn Lookup) -> Self {
        Self {
            ai_enabled: l.flag(&ENABLED),
            enabled: l.flag(&MCP_ENABLED),
            tools: l.text(&MCP_TOOLS),
            calls_per_minute: l.number(&MCP_CALLS_PER_MINUTE),
        }
    }

    /// `Ok` when both `ai.enabled` and `ai.mcp.enabled` are on.
    ///
    /// # Errors
    /// 39991, naming the key that is off.
    pub fn check(&self) -> sprig_common::Result<()> {
        if !self.ai_enabled {
            return Err(crate::error::disabled());
        }
        if !self.enabled {
            return Err(crate::error::mcp_disabled());
        }
        Ok(())
    }
}

/// Where values come from: a caller's settings context, a live snapshot,
/// or the defaults.
pub trait Lookup {
    /// A boolean key.
    fn flag(&self, s: &'static Setting<bool>) -> bool;
    /// A number key.
    fn number(&self, s: &'static Setting<u64>) -> u64;
    /// A text key.
    fn text(&self, s: &'static Setting<Text>) -> String;
}

/// The registry defaults.
#[derive(Debug, Default, Clone, Copy)]
pub struct Defaults;

impl Lookup for Defaults {
    fn flag(&self, s: &'static Setting<bool>) -> bool {
        s.default
    }
    fn number(&self, s: &'static Setting<u64>) -> u64 {
        s.default
    }
    fn text(&self, s: &'static Setting<Text>) -> String {
        s.default.to_string()
    }
}

/// Values from a live snapshot, in one context (a tenant's, with its
/// database when known).
pub struct Live<'a> {
    /// The snapshot.
    pub snap: sprig_settings::Snapshot,
    /// The context.
    pub ctx: SettingCtx<'a>,
}

impl Lookup for Live<'_> {
    fn flag(&self, s: &'static Setting<bool>) -> bool {
        self.snap.get(s, &self.ctx)
    }
    fn number(&self, s: &'static Setting<u64>) -> u64 {
        self.snap.get(s, &self.ctx)
    }
    fn text(&self, s: &'static Setting<Text>) -> String {
        self.snap.get(s, &self.ctx).to_string()
    }
}

/// `ai.model.<role>`.
#[must_use]
pub fn model_key(role: Role) -> &'static Setting<Text> {
    match role {
        Role::Admin => &MODEL_ADMIN,
        Role::Triage => &MODEL_TRIAGE,
        Role::Dba => &MODEL_DBA,
        Role::Data => &MODEL_DATA,
    }
}

/// `ai.effort.<role>`.
#[must_use]
pub fn effort_key(role: Role) -> &'static Setting<Text> {
    match role {
        Role::Admin => &EFFORT_ADMIN,
        Role::Triage => &EFFORT_TRIAGE,
        Role::Dba => &EFFORT_DBA,
        Role::Data => &EFFORT_DATA,
    }
}

/// What one turn reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnSettings {
    /// `ai.enabled` (always `true` for the administration roles, which
    /// `ai.admin.enabled` governs instead).
    pub enabled: bool,
    /// `ai.share_data`.
    pub share_data: bool,
    /// `ai.model.<role>`: `provider/model`, or empty.
    pub model: String,
    /// `ai.effort.<role>`.
    pub effort: Effort,
    /// The budgets.
    pub budget: Budget,
    /// `ai.max_tool_rounds`.
    pub max_tool_rounds: u32,
    /// `ai.timeout_seconds`.
    pub timeout: Duration,
    /// `ai.max_output_tokens`.
    pub max_output_tokens: u32,
    /// `ai.audit_payload_days`.
    pub audit_payload_days: u64,
}

impl TurnSettings {
    /// Reads every key a turn of `role` needs from `l`.
    #[must_use]
    pub fn read(l: &dyn Lookup, role: Role) -> Self {
        Self {
            enabled: !role.is_tenant() || l.flag(&ENABLED),
            share_data: role.is_tenant() && l.flag(&SHARE_DATA),
            model: l.text(model_key(role)),
            effort: Effort::parse(&l.text(effort_key(role))).unwrap_or(Effort::Medium),
            budget: Budget {
                tenant_tokens_day: l.number(&BUDGET_TENANT_TOKENS_DAY),
                user_tokens_day: l.number(&BUDGET_USER_TOKENS_DAY),
            },
            max_tool_rounds: u32::try_from(l.number(&MAX_TOOL_ROUNDS)).unwrap_or(12),
            timeout: Duration::from_secs(l.number(&TIMEOUT_SECONDS)),
            max_output_tokens: u32::try_from(l.number(&MAX_OUTPUT_TOKENS)).unwrap_or(16_000),
            audit_payload_days: l.number(&AUDIT_PAYLOAD_DAYS),
        }
    }

    /// Reads them from `snap` in `ctx`: a tenant's context (with its
    /// database when the turn is about one), so the tenant's own values
    /// apply.
    #[must_use]
    pub fn resolve(snap: sprig_settings::Snapshot, ctx: SettingCtx<'_>, role: Role) -> Self {
        Self::read(&Live { snap, ctx }, role)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn everything_is_off_by_default() {
        let t = TurnSettings::read(&Defaults, Role::Data);
        assert!(!t.enabled);
        assert!(!t.share_data);
        assert!(t.model.is_empty());
        assert_eq!(t.effort, Effort::Medium);
        assert_eq!(t.budget.user_tokens_day, 200_000);
        assert_eq!(t.max_tool_rounds, 12);
        let a = TurnSettings::read(&Defaults, Role::Triage);
        assert_eq!(a.effort, Effort::Low);
        assert!(!a.share_data);
        let m = McpSettings::read(&Defaults);
        assert!(!m.enabled && !m.ai_enabled);
        assert_eq!(m.tools, "read");
        assert_eq!(m.check().unwrap_err().protocol().mysql_errno, 39991);
        let reg = sprig_settings::Registry::global().unwrap();
        for k in [
            "ai.enabled",
            "ai.share_data",
            "ai.model.dba",
            "ai.effort.triage",
            "ai.budget.user_tokens_day",
            "ai.audit_payload_days",
            "ai.mcp.enabled",
            "ai.mcp.tools",
            "ai.mcp.calls_per_minute",
        ] {
            assert!(reg.get(k).is_some(), "{k}");
        }
    }
}
