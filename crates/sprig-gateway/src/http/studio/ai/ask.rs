//! `ASK '<question>' [IN DATABASE db] [ROLE data|dba]`: one assistant turn
//! whose answer is rows, never a run.
//!
//! The MySQL and MariaDB front ends, REST and gRPC `Sql.Query` all reach
//! this through the executor, as the session's own user. The turn is the
//! chat route's, with the same checks in the same order: `ai.enabled`
//! (39991), the role (39792 without a tenant login), the role's model and
//! provider (39990), the daily budgets (39992). Tools run as the caller
//! ([`super::host`]); the conversation is kept like a chat's, so "What was
//! sent" shows it.
//!
//! **Rows.** One per proposed statement:
//!
//! | Column | Holds |
//! |---|---|
//! | `query` | one statement, never run; `NULL` when the answer has none |
//! | `database` | the database the assistant meant it for, or `NULL` |
//! | `explanation` | why, in a few sentences (the whole answer when there is no query) |
//! | `notes` | whether it only reads, its estimate, a cut-off answer |
//! | `sources` | what the answer drew on: docs pages, tables, plans |
//! | `conversation_id` | the stored conversation |
//!
//! Statements come from the turn's `propose_sql` proposals, or, when there
//! are none, from fenced SQL blocks in its text. Each is split with the
//! gateway's own statement splitter, so a proposal holding two statements
//! becomes two rows and `query` always holds exactly one. A refusal is
//! 39994; provider failures are 39993.

use std::fmt::Write as _;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;
use sprig_ai::audit::{AuditStore, ConvId};
use sprig_ai::feedback::{self, FeedbackStore};
use sprig_ai::glossary::{self, GlossaryStore, Status};
use sprig_ai::provider::{Provider, Stop};
use sprig_ai::settings::TurnSettings;
use sprig_ai::tools::Source;
use sprig_ai::{
    Agent, ProposalKind, Role, ToolHost, ToolRegistry, TurnConfig, TurnEvent, TurnRequest,
    TurnResult,
};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{Result, SprigCode, SprigError};

use super::super::super::Core;
use super::super::super::sql::Caller;
use super::{GatewayLookup, MAX_MESSAGE, chat, host, invalid, service, stores};

/// The question's longest length, in bytes (the chat route's).
pub const MAX_QUESTION: usize = MAX_MESSAGE;

/// One `ASK`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AskRequest {
    /// The question.
    pub question: String,
    /// `data` or `dba`; `None` for `data`.
    pub role: Option<String>,
    /// `IN DATABASE`, else the session's database.
    pub database: Option<String>,
}

/// One row of the answer.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AskRow {
    /// One statement, never run.
    pub query: Option<String>,
    /// The database it is meant for.
    pub database: Option<String>,
    /// Why.
    pub explanation: String,
    /// Reads only, the estimate, a cut-off answer.
    pub notes: Option<String>,
    /// What the answer drew on.
    pub sources: Option<String>,
}

/// The rows, and the conversation they are kept in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AskAnswer {
    /// At least one.
    pub rows: Vec<AskRow>,
    /// The stored conversation.
    pub conversation: Option<ConvId>,
}

// ---- where a turn's parts come from ----

/// The stores a tenant turn keeps its conversation, usage and glossary in.
pub struct AskStores {
    /// Conversations and usage.
    pub audit: Arc<dyn AuditStore>,
    /// The tenant glossary, for the prompt.
    pub glossary: Option<Arc<dyn GlossaryStore>>,
    /// Users' feedback, for the prompt's notes.
    pub feedback: Option<Arc<dyn FeedbackStore>>,
}

/// What an `ASK` needs from its surroundings: the gateway's for real
/// sessions, scripted ones in tests.
#[async_trait]
pub trait AskEnv: Send + Sync {
    /// The `ai.*` values of the caller's tenant (and `db`) for `role`.
    fn settings(&self, role: Role, db: Option<&str>) -> TurnSettings;
    /// The caller is a tenant's user.
    fn tenant_login(&self) -> bool;
    /// The caller's tenant.
    fn tenant(&self) -> Option<TenantId>;
    /// The caller.
    fn user(&self) -> UserId;
    /// The provider and model `ai.model.<role>` names.
    ///
    /// # Errors
    /// 39990 when there is none.
    async fn provider(&self, role: Role, model: &str) -> Result<(Arc<dyn Provider>, String)>;
    /// The tenant's stores.
    ///
    /// # Errors
    /// The catalog's.
    async fn stores(&self) -> Result<AskStores>;
    /// Runs read tools as the caller.
    fn host(&self, share_data: bool) -> Arc<dyn ToolHost>;
}

static CORE: OnceLock<Arc<Core>> = OnceLock::new();

/// Gives `ASK` the HTTP core it runs turns and tools with (set when HTTP
/// starts, like the chat route's).
pub fn install(core: Arc<Core>) {
    let _ = CORE.set(core);
}

struct GatewayEnv {
    core: Arc<Core>,
    caller: Caller,
}

#[async_trait]
impl AskEnv for GatewayEnv {
    fn settings(&self, role: Role, db: Option<&str>) -> TurnSettings {
        TurnSettings::read(
            &GatewayLookup {
                st: &self.core.studio,
                tenant: self.caller.principal.tenant,
                db,
            },
            role,
        )
    }
    fn tenant_login(&self) -> bool {
        self.caller.principal.tenant.is_some() && self.caller.principal.user_id.is_some()
    }
    fn tenant(&self) -> Option<TenantId> {
        self.caller.principal.tenant
    }
    fn user(&self) -> UserId {
        self.caller.principal.user_id.unwrap_or(UserId(0))
    }
    async fn provider(&self, role: Role, model: &str) -> Result<(Arc<dyn Provider>, String)> {
        let svc = service()?;
        svc.providers.for_role(role, model, &*svc.keys).await
    }
    async fn stores(&self) -> Result<AskStores> {
        let st = stores(&self.core, &self.caller).await?;
        Ok(AskStores {
            audit: Arc::clone(&st.audit) as Arc<dyn AuditStore>,
            feedback: Some(st.audit as Arc<dyn FeedbackStore>),
            glossary: Some(st.glossary),
        })
    }
    fn host(&self, share_data: bool) -> Arc<dyn ToolHost> {
        Arc::new(host::GatewayHost::new(
            Arc::clone(&self.core),
            self.caller.clone(),
            share_data,
        ))
    }
}

/// Answers `req` as `caller`, whose current database is `current`.
///
/// # Errors
/// 39990-39996 as the module docs say; 39792 for a role other than `data`
/// or `dba`, a question that is empty or too long, or a login without a
/// tenant.
pub async fn ask(caller: Caller, req: &AskRequest, current: Option<&str>) -> Result<AskAnswer> {
    let core = CORE.get().cloned().ok_or_else(|| {
        SprigError::new(
            SprigCode::AiNotConfigured,
            "this gateway can't run assistants (its HTTP side didn't start)",
        )
    })?;
    ask_with(&GatewayEnv { core, caller }, req, current).await
}

/// [`ask`] over any [`AskEnv`].
///
/// # Errors
/// As [`ask`].
pub async fn ask_with(
    env: &dyn AskEnv,
    req: &AskRequest,
    current: Option<&str>,
) -> Result<AskAnswer> {
    let asked = req.role.as_deref().unwrap_or("data").trim();
    if !matches!(asked, "data" | "dba") {
        return Err(invalid("ASK ... ROLE takes data or dba"));
    }
    let question = req.question.trim();
    if question.is_empty() || question.len() > MAX_QUESTION {
        return Err(invalid(format!(
            "ASK takes a question of 1 to {MAX_QUESTION} bytes"
        )));
    }
    let db = req
        .database
        .as_deref()
        .or(current)
        .map(str::trim)
        .filter(|d| !d.is_empty());
    let settings = env.settings(Role::parse(asked).unwrap_or(Role::Data), db);
    // 1. Off for the tenant: before anything else.
    if !settings.enabled {
        return Err(sprig_ai::error::disabled());
    }
    // 2. The role.
    let role = chat::role_for(asked, env.tenant_login(), false)?;
    // 3. The model.
    let (provider, model) = env.provider(role, &settings.model).await?;
    // 4. The budgets.
    let st = env.stores().await?;
    let user = env.user();
    let spent = st.audit.usage_today(user).await?;
    settings.budget.check(spent, 0)?;

    let mut system = sprig_ai::prompt::base(role);
    if let Some(g) = &st.glossary
        && let Ok(terms) = g.list(Some(Status::Accepted)).await
        && let Some(p) = glossary::prompt(&terms)
    {
        system.push(sprig_ai::Block::text(p));
    }
    if let Some(f) = &st.feedback
        && let Ok(notes) = f.recent(feedback::PROMPT_NOTES).await
        && let Some(p) = feedback::prompt(&notes)
    {
        system.push(sprig_ai::Block::text(p));
    }
    system.push(sprig_ai::Block::text(sprig_ai::prompt::ASK));
    let title = sprig_ai::egress::redact_secrets(question);
    let conv = st.audit.create(user, role, &title).await?;
    let mut message = question.to_owned();
    if let Some(d) = db {
        let _ = write!(message, "\n\n[The user's current database]\n{d}");
    }
    let cfg = TurnConfig::from_settings(role, &settings, model);
    let agent = Agent::new(provider, ToolRegistry::builtin(), Arc::clone(&st.audit));
    let mut events: Vec<TurnEvent> = Vec::new();
    let result = agent
        .run(
            &cfg,
            TurnRequest {
                tenant: env.tenant(),
                user,
                conv: Some(conv),
                system,
                history: Vec::new(),
                message,
                host: env.host(settings.share_data),
                scrubber: None,
                cancel: None,
            },
            &mut events,
        )
        .await?;
    if let Some(e) = result.refusal() {
        return Err(e);
    }
    Ok(AskAnswer {
        rows: rows_of(&result),
        conversation: Some(conv),
    })
}

// ---- the answer as rows ----

/// One statement the turn proposed, before splitting.
struct Proposed {
    sql: String,
    database: Option<String>,
    why: String,
    estimate: Option<String>,
}

fn text_field(v: &Value, k: &str) -> Option<String> {
    v.get(k)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

/// Fenced blocks of SQL in `text` (` ```sql `, ` ```mysql `, ` ```mariadb `
/// or no language), and the text around them.
fn fenced(text: &str) -> (Vec<String>, String) {
    let mut blocks = Vec::new();
    let mut prose = String::new();
    let mut open: Option<(bool, String)> = None;
    for line in text.lines() {
        let t = line.trim_start();
        match &mut open {
            None => {
                if let Some(info) = t.strip_prefix("```") {
                    let lang = info.trim().to_ascii_lowercase();
                    open = Some((
                        matches!(lang.as_str(), "" | "sql" | "mysql" | "mariadb"),
                        String::new(),
                    ));
                } else {
                    prose.push_str(line);
                    prose.push('\n');
                }
            }
            Some((is_sql, body)) => {
                if t.starts_with("```") {
                    if *is_sql && !body.trim().is_empty() {
                        blocks.push(body.trim().to_owned());
                    }
                    open = None;
                } else {
                    body.push_str(line);
                    body.push('\n');
                }
            }
        }
    }
    // An unclosed block (a cut-off answer) still counts.
    if let Some((true, body)) = open
        && !body.trim().is_empty()
    {
        blocks.push(body.trim().to_owned());
    }
    (blocks, tidy(&prose))
}

/// Trims, and keeps at most one blank line between paragraphs.
fn tidy(s: &str) -> String {
    let mut out = String::new();
    let mut blank = false;
    for line in s.trim().lines() {
        if line.trim().is_empty() {
            blank = true;
            continue;
        }
        if blank && !out.is_empty() {
            out.push('\n');
        }
        blank = false;
        out.push_str(line.trim_end());
        out.push('\n');
    }
    out.trim_end().to_owned()
}

/// The statements of `sql`, as the gateway splits a client's text.
fn statements(sql: &str) -> Vec<String> {
    crate::exec::split::split(&Bytes::copy_from_slice(sql.as_bytes()))
        .into_iter()
        .map(|b| {
            String::from_utf8_lossy(&b)
                .trim()
                .trim_end_matches(';')
                .trim_end()
                .to_owned()
        })
        .filter(|s| !s.is_empty())
        .collect()
}

fn sources_text(sources: &[Source]) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();
    for s in sources {
        let p = if s.reference.is_empty() || s.reference == s.title {
            format!("{}: {}", s.kind, s.title)
        } else {
            format!("{}: {} ({})", s.kind, s.title, s.reference)
        };
        if !parts.contains(&p) {
            parts.push(p);
        }
    }
    (!parts.is_empty()).then(|| parts.join("; "))
}

fn stop_note(stop: &Stop) -> Option<&'static str> {
    match stop {
        Stop::MaxTokens => Some(
            "The answer was cut off at ai.max_output_tokens; check the query before you run it.",
        ),
        Stop::Other(d) if d == "max_tool_rounds" => {
            Some("The turn stopped at ai.max_tool_rounds before it finished.")
        }
        Stop::Other(d) if d == "stopped" => Some("The turn was stopped before it finished."),
        _ => None,
    }
}

/// The rows of a finished turn.
#[must_use]
pub fn rows_of(result: &TurnResult) -> Vec<AskRow> {
    let text = result.text();
    let (blocks, prose) = fenced(&text);
    let mut proposed: Vec<Proposed> = result
        .proposals
        .iter()
        .filter(|p| p.kind == ProposalKind::Sql)
        .filter_map(|p| {
            Some(Proposed {
                sql: text_field(&p.body, "sql")?,
                database: text_field(&p.body, "db"),
                why: text_field(&p.body, "why").unwrap_or_else(|| prose.clone()),
                estimate: text_field(&p.body, "estimate"),
            })
        })
        .collect();
    let others = result
        .proposals
        .iter()
        .filter(|p| p.kind != ProposalKind::Sql)
        .count();
    if proposed.is_empty() {
        proposed = blocks
            .into_iter()
            .map(|sql| Proposed {
                sql,
                database: None,
                why: prose.clone(),
                estimate: None,
            })
            .collect();
    }
    let sources = sources_text(&result.sources);
    let mut common: Vec<String> = Vec::new();
    if let Some(n) = stop_note(&result.stop) {
        common.push(n.to_owned());
    }
    if others > 0 {
        common.push(
            "The assistant also proposed a tile or glossary entry, which ASK doesn't show."
                .to_owned(),
        );
    }
    let mut rows = Vec::new();
    for p in proposed {
        let stmts = statements(&p.sql);
        let n = stmts.len();
        for (i, q) in stmts.into_iter().enumerate() {
            let mut notes: Vec<String> = Vec::new();
            notes.push(if sprig_ai::sql_guard::read_only(&q).is_ok() {
                "Reads only.".to_owned()
            } else {
                "Changes data, schema or settings, or isn't a plain query: review it before you run it."
                    .to_owned()
            });
            if n > 1 {
                notes.push(format!("Statement {} of {n} in one proposal.", i + 1));
            }
            if let Some(e) = &p.estimate {
                notes.push(format!("Estimate: {e}"));
            }
            notes.extend(common.iter().cloned());
            rows.push(AskRow {
                query: Some(q),
                database: p.database.clone(),
                explanation: p.why.clone(),
                notes: Some(notes.join(" ")),
                sources: sources.clone(),
            });
        }
    }
    if rows.is_empty() {
        let mut notes = vec!["No query: the assistant answered in words.".to_owned()];
        notes.extend(common);
        rows.push(AskRow {
            query: None,
            database: None,
            explanation: if prose.is_empty() {
                "The assistant gave no answer.".to_owned()
            } else {
                prose
            },
            notes: Some(notes.join(" ")),
            sources,
        });
    }
    rows
}

#[cfg(test)]
mod tests;
