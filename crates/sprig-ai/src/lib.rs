//! `sprig-ai`: the core of SprigDB's AI assistants.
//!
//! Layer 2.
//!
//! Every assistant follows the same rules, and this crate is where they
//! hold:
//! - **tools run as the signed-in user**: a [`tools::ToolHost`] is built
//!   for one caller and runs read tools with that caller's rights;
//! - **anything that changes state is a proposal**: `propose_*` tools
//!   never reach the host; they come back as [`tools::Proposal`]s for the
//!   user to run;
//! - **what leaves passes one gate**: [`egress`] reduces results to their
//!   shape unless `ai.share_data` is on, scrubs the administration
//!   agent's input, redacts secrets and marks tool output untrusted;
//! - **every call is audited and budgeted**: [`audit`] and [`budget`];
//! - **answers cite sources**: tool outputs carry [`tools::Source`]s.
//!
//! Modules:
//! - [`provider`]: the [`Provider`] trait, the Anthropic Messages and
//!   OpenAI-compatible Chat Completions adapters, and a scripted stub;
//! - [`providers`]: provider records in etcd, with keys sealed by [`keys`];
//! - [`tools`]: tool specs, the built-in registry, input checks;
//! - [`egress`]: the egress gate;
//! - [`settings`]: the `ai.*` keys;
//! - [`budget`]: daily token budgets;
//! - [`audit`]: conversations, messages and usage in the tenant catalog;
//! - [`turn`]: one chat turn, provider and tools in a loop;
//! - [`mod@metrics`]: the `sprig_ai_*` metrics.

pub mod audit;
pub mod budget;
pub mod egress;
pub mod error;
pub mod keys;
pub mod metrics;
pub mod prompt;
pub mod provider;
pub mod providers;
pub mod role;
pub mod settings;
pub mod sql_guard;
pub mod tools;
pub mod turn;

pub use audit::{AuditStore, CatalogAudit, ConvId, MemoryAudit, MsgKind};
pub use budget::{Budget, UsageToday};
pub use egress::{EgressPolicy, Scrubber};
pub use provider::{
    Block, ChatOutcome, ChatRequest, ChatSink, Delta, Effort, Message, MsgRole, Provider,
    ProviderKind, Sent, Stop, ToolDef, Usage,
};
pub use role::Role;
pub use tools::{
    Effect, Proposal, ProposalKind, Shares, Source, ToolBody, ToolHost, ToolOutput, ToolRegistry,
    ToolSpec,
};
pub use turn::{Agent, TurnConfig, TurnEvent, TurnRequest, TurnResult, TurnSink};
