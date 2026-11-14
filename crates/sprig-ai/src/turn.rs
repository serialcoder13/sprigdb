//! One chat turn: the model and the tools in a loop.
//!
//! ```text
//! check: ai.enabled, a model for the role, budgets left (else 39991, 39990, 39992)
//! append the user's text, through the egress gate
//! loop:
//!     budgets left, else 39992; output capped at what is left
//!     request := system + conversation + the role's tools; the gate checks it
//!     stream provider.chat(request) → text and thinking events
//!     record usage, the payload's hash (and the payload), the answer
//!     stop = tool_use and rounds < ai.max_tool_rounds:
//!         every call checked (role, schema, SQL), then run side by side:
//!           read    → the host, as the caller → the gate → a wrapped result
//!           propose → a proposal event; the model is told it was shown, not run
//!           refused → an error result; three refusals in a turn end it (39996)
//!         append the results; again
//!     else: done
//! sources, end; one `ai.turn` audit event
//! ```
//!
//! The conversation is only ever appended to: a tool call the turn can't
//! run (the round limit, a cut-off or refused answer) still gets an error
//! result, so the history stays valid for the next turn.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use sprig_common::audit::AuditStream;
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{CancellationToken, Result, SprigError};

use crate::audit::{AuditStore, ConvId, MsgKind};
use crate::budget::Budget;
use crate::egress::{EgressGate, EgressPolicy, Scrubber, wrap};
use crate::error;
use crate::provider::{
    Block, ChatRequest, ChatSink, Delta, Effort, Message, MsgRole, Provider, Stop, Usage,
};
use crate::role::Role;
use crate::settings::TurnSettings;
use crate::tools::{Effect, Proposal, Source, ToolHost, ToolRegistry};

/// Refused tool calls (role, input) after which a turn ends with 39996.
pub const MAX_DENIALS: u32 = 3;
/// The longest one read tool may take.
pub const TOOL_TIMEOUT: Duration = Duration::from_secs(60);

/// How a turn runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnConfig {
    /// The assistant.
    pub role: Role,
    /// `ai.enabled` for the tenant.
    pub enabled: bool,
    /// The model id, as the provider names it.
    pub model: String,
    /// Effort, for providers that support it.
    pub effort: Option<Effort>,
    /// Output tokens per call.
    pub max_output_tokens: u32,
    /// Tool rounds per turn.
    pub max_tool_rounds: u32,
    /// Wall time of the turn.
    pub timeout: Duration,
    /// The daily budgets.
    pub budget: Budget,
    /// `ai.share_data`.
    pub share_data: bool,
    /// Keep exact payloads with the conversation (`ai.audit_payload_days`
    /// above 0); hashes are always kept.
    pub keep_payloads: bool,
}

impl TurnConfig {
    /// The configuration from the settings, with `model` (the model part
    /// of `ai.model.<role>`).
    #[must_use]
    pub fn from_settings(role: Role, s: &TurnSettings, model: impl Into<String>) -> Self {
        Self {
            role,
            enabled: s.enabled,
            model: model.into(),
            effort: Some(s.effort),
            max_output_tokens: s.max_output_tokens,
            max_tool_rounds: s.max_tool_rounds,
            timeout: s.timeout,
            budget: s.budget,
            share_data: s.share_data,
            keep_payloads: s.audit_payload_days > 0,
        }
    }
}

/// One turn's input.
pub struct TurnRequest {
    /// The caller's tenant (`None` for the administration roles).
    pub tenant: Option<TenantId>,
    /// The caller.
    pub user: UserId,
    /// The conversation messages are stored in; `None` keeps nothing.
    pub conv: Option<ConvId>,
    /// The system prompt: [`crate::prompt::base`] and whatever the surface
    /// adds. Keep it identical across a conversation.
    pub system: Vec<Block>,
    /// The conversation so far, as earlier turns returned it.
    pub history: Vec<Message>,
    /// The user's new text.
    pub message: String,
    /// Runs read tools as the caller.
    pub host: Arc<dyn ToolHost>,
    /// The administration scrubber (its key keeps placeholders stable).
    pub scrubber: Option<Arc<Scrubber>>,
    /// Stops the turn (the user pressed stop, the client went away).
    pub cancel: Option<CancellationToken>,
}

impl std::fmt::Debug for TurnRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TurnRequest")
            .field("tenant", &self.tenant)
            .field("user", &self.user)
            .field("conv", &self.conv)
            .field("history", &self.history.len())
            .finish_non_exhaustive()
    }
}

/// What a turn streams to its surface (the chat route's NDJSON events).
#[derive(Debug, Clone, PartialEq)]
pub enum TurnEvent {
    /// More answer text.
    Text(String),
    /// More reasoning text.
    Thinking(String),
    /// A tool call starts.
    ToolStart {
        /// The call's id.
        id: String,
        /// The tool.
        name: String,
        /// Its input.
        input: Value,
    },
    /// A tool call ended.
    ToolEnd {
        /// The call's id.
        id: String,
        /// The tool.
        name: String,
        /// It ran without error.
        ok: bool,
        /// The gate withheld rows or values, or cut the result.
        reduced: bool,
    },
    /// A proposal for the user.
    Proposal(Proposal),
    /// Usage so far in this call.
    Usage(Usage),
    /// What the answer drew on.
    Sources(Vec<Source>),
    /// The turn ended.
    End {
        /// Why.
        stop: Stop,
    },
}

/// Where a turn streams [`TurnEvent`]s.
pub trait TurnSink: Send {
    /// One event; an error stops the turn.
    ///
    /// # Errors
    /// Whatever stops the turn; it is returned from [`Agent::run`].
    fn event(&mut self, e: TurnEvent) -> Result<()>;
}

impl TurnSink for Vec<TurnEvent> {
    fn event(&mut self, e: TurnEvent) -> Result<()> {
        self.push(e);
        Ok(())
    }
}

/// How a turn ended.
#[derive(Debug, Clone, PartialEq)]
pub struct TurnResult {
    /// The messages this turn added, the user's first: append them to the
    /// history for the next turn.
    pub messages: Vec<Message>,
    /// Why the turn ended.
    pub stop: Stop,
    /// Tokens used.
    pub usage: Usage,
    /// Tool rounds taken.
    pub rounds: u32,
    /// Proposals shown.
    pub proposals: Vec<Proposal>,
    /// Sources the tools reported.
    pub sources: Vec<Source>,
    /// The SHA-256 of each payload sent, hexadecimal.
    pub sent: Vec<String>,
}

impl TurnResult {
    /// The final answer's text.
    #[must_use]
    pub fn text(&self) -> String {
        self.messages
            .iter()
            .rev()
            .find(|m| m.role == MsgRole::Assistant)
            .map(Message::text)
            .unwrap_or_default()
    }

    /// 39994 when the model refused; for surfaces that answer with an
    /// error (`ASK`).
    #[must_use]
    pub fn refusal(&self) -> Option<SprigError> {
        match &self.stop {
            Stop::Refusal(t) => Some(error::refused(t)),
            _ => None,
        }
    }
}

/// Runs turns with one provider, tool registry and store.
#[derive(Clone)]
pub struct Agent {
    provider: Arc<dyn Provider>,
    tools: Arc<ToolRegistry>,
    audit: Arc<dyn AuditStore>,
}

impl std::fmt::Debug for Agent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Agent")
            .field("provider", &self.provider.name())
            .finish_non_exhaustive()
    }
}

/// Forwards a provider's deltas as turn events.
struct Forward<'a> {
    sink: &'a mut dyn TurnSink,
}

impl ChatSink for Forward<'_> {
    fn delta(&mut self, d: Delta) -> Result<()> {
        match d {
            Delta::Text(t) => self.sink.event(TurnEvent::Text(t)),
            Delta::Thinking(t) => self.sink.event(TurnEvent::Thinking(t)),
            Delta::Usage(u) => self.sink.event(TurnEvent::Usage(u)),
            // Announced when the call runs.
            Delta::ToolCall { .. } => Ok(()),
        }
    }
}

/// One tool call's outcome.
struct Called {
    result: Block,
    ok: bool,
    reduced: bool,
    denied: bool,
    proposal: Option<Proposal>,
    sources: Vec<Source>,
    stored: Value,
}

/// Appends user text: into the last message when it is the user's (after
/// tool results), else as a new message.
fn push_user_text(messages: &mut Vec<Message>, text: String) {
    match messages.last_mut() {
        Some(m) if m.role == MsgRole::User => m.content.push(Block::Text { text }),
        _ => messages.push(Message {
            role: MsgRole::User,
            content: vec![Block::Text { text }],
        }),
    }
}

impl Agent {
    /// An agent calling `provider`, offering `tools`, recording in `audit`.
    #[must_use]
    pub fn new(
        provider: Arc<dyn Provider>,
        tools: Arc<ToolRegistry>,
        audit: Arc<dyn AuditStore>,
    ) -> Self {
        Self {
            provider,
            tools,
            audit,
        }
    }

    async fn record(
        &self,
        conv: Option<ConvId>,
        kind: MsgKind,
        body: Value,
        hash: Option<[u8; 32]>,
    ) {
        if let Some(c) = conv
            && let Err(e) = self.audit.append(c, kind, &body, hash).await
        {
            tracing::warn!(error = %e, kind = kind.as_str(), "an AI message wasn't recorded");
        }
    }

    /// Runs one turn, streaming events to `sink`.
    ///
    /// # Errors
    /// 39991 (`ai.enabled` off), 39990 (no model), 39992 (a budget is
    /// spent), 39993 (the provider failed, or the turn ran out of time),
    /// 39996 (too many refused tool calls), the store's errors before the
    /// first call, or the sink's error.
    #[allow(clippy::too_many_lines)] // the loop reads best in one piece
    pub async fn run(
        &self,
        cfg: &TurnConfig,
        mut req: TurnRequest,
        sink: &mut dyn TurnSink,
    ) -> Result<TurnResult> {
        let started = Instant::now();
        let role = cfg.role;
        if !cfg.enabled {
            return Err(error::disabled());
        }
        if cfg.model.trim().is_empty() {
            return Err(error::not_configured(
                role,
                format!("ai.model.{role} is empty"),
            ));
        }
        let gate = EgressGate::new(
            EgressPolicy {
                role,
                share_data: cfg.share_data,
            },
            req.scrubber.clone(),
        );
        let used = if role.is_tenant() {
            let u = self.audit.usage_today(req.user).await?;
            cfg.budget.check(u, 0)?;
            Some(u)
        } else {
            None
        };
        let deadline = tokio::time::Instant::now() + cfg.timeout;
        let cancel = req.cancel.clone().unwrap_or_default();
        let defs: Vec<_> = self.tools.for_role(role).iter().map(|s| s.def()).collect();

        let mut messages = std::mem::take(&mut req.history);
        let first_new = messages.len();
        let user_text = gate.user_text(&req.message);
        self.record(req.conv, MsgKind::User, json!({"text": user_text}), None)
            .await;
        push_user_text(&mut messages, user_text);

        let mut usage = Usage::default();
        let mut rounds = 0u32;
        let mut denials = 0u32;
        let mut proposals = Vec::new();
        let mut sources: Vec<Source> = Vec::new();
        let mut sent = Vec::new();
        let mut tools_called: Vec<String> = Vec::new();
        let stop = loop {
            let mut max_tokens = cfg.max_output_tokens.max(1);
            if let Some(u) = used {
                cfg.budget.check(u, usage.billable())?;
                let left = cfg.budget.remaining(u, usage.billable());
                max_tokens = max_tokens.min(u32::try_from(left).unwrap_or(u32::MAX).max(1));
            }
            let mut creq = ChatRequest {
                model: cfg.model.clone(),
                system: req.system.clone(),
                messages: messages.clone(),
                tools: defs.clone(),
                max_tokens,
                effort: cfg.effort,
                cache_prefix: true,
            };
            gate.check(&mut creq)?;
            let call = {
                let mut fwd = Forward { sink: &mut *sink };
                let chat = self.provider.chat(creq, &mut fwd);
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => None,
                    r = tokio::time::timeout_at(deadline, chat) => match r {
                        Ok(r) => Some(r),
                        Err(_) => Some(Err(error::provider_error(
                            self.provider.name(),
                            format!("the turn ran past ai.timeout_seconds ({}s)", cfg.timeout.as_secs()),
                            true,
                        ))),
                    },
                }
            };
            let outcome = match call {
                None => break Stop::Other("stopped".to_owned()),
                Some(Err(e)) => {
                    crate::metrics::request(role, self.provider.name(), &cfg.model, "error");
                    self.record(
                        req.conv,
                        MsgKind::SystemNote,
                        json!({"error": e.message, "code": e.code().map(sprig_common::SprigCode::number)}),
                        None,
                    )
                    .await;
                    return Err(e);
                }
                Some(Ok(o)) => o,
            };
            let result = if matches!(outcome.stop, Stop::Refusal(_)) {
                "refused"
            } else {
                "ok"
            };
            crate::metrics::request(role, self.provider.name(), &cfg.model, result);
            crate::metrics::tokens(role, &outcome.usage);
            usage.add(&outcome.usage);
            if role.is_tenant()
                && let Err(e) = self.audit.add_usage(req.user, role, &outcome.usage).await
            {
                tracing::warn!(error = %e, "AI usage wasn't recorded");
            }
            let mut note = json!({
                "sha256": outcome.sent.hex(),
                "provider": self.provider.name(),
                "model": outcome.model,
                "usage": outcome.usage,
                "stop": outcome.stop,
            });
            if cfg.keep_payloads {
                note["payload"] = json!(&*outcome.sent.body);
            }
            sent.push(outcome.sent.hex());
            self.record(
                req.conv,
                MsgKind::SystemNote,
                note,
                Some(outcome.sent.sha256),
            )
            .await;
            self.record(
                req.conv,
                MsgKind::Assistant,
                serde_json::to_value(&outcome.message).unwrap_or(Value::Null),
                None,
            )
            .await;
            let calls: Vec<(String, String, Value)> = outcome
                .message
                .tool_calls()
                .into_iter()
                .map(|(i, n, v)| (i.to_owned(), n.to_owned(), v.clone()))
                .collect();
            messages.push(outcome.message);

            let stop = match outcome.stop {
                Stop::ToolUse if calls.is_empty() => Stop::EndTurn,
                s => s,
            };
            if stop != Stop::ToolUse {
                if !calls.is_empty() {
                    // A cut-off or refused answer's calls never run.
                    let why = format!("not run: the answer ended with {}", stop.label());
                    self.unrun(&gate, req.conv, &mut messages, &calls, &why)
                        .await;
                }
                break stop;
            }
            if rounds >= cfg.max_tool_rounds {
                self.unrun(
                    &gate,
                    req.conv,
                    &mut messages,
                    &calls,
                    "not run: the turn reached ai.max_tool_rounds",
                )
                .await;
                break Stop::Other("max_tool_rounds".to_owned());
            }
            rounds += 1;
            for (id, name, input) in &calls {
                sink.event(TurnEvent::ToolStart {
                    id: id.clone(),
                    name: name.clone(),
                    input: input.clone(),
                })?;
                self.record(
                    req.conv,
                    MsgKind::ToolCall,
                    json!({"id": id, "name": name, "input": input}),
                    None,
                )
                .await;
                tools_called.push(name.clone());
            }
            let host = Arc::clone(&req.host);
            let runs = calls
                .iter()
                .map(|(id, name, input)| self.call(&gate, role, &*host, id, name, input));
            let done = tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    self.unrun(&gate, req.conv, &mut messages, &calls, "not run: the user stopped the turn").await;
                    break Stop::Other("stopped".to_owned());
                }
                r = tokio::time::timeout_at(deadline, futures::future::join_all(runs)) => match r {
                    Ok(d) => d,
                    Err(_) => return Err(error::provider_error(
                        self.provider.name(),
                        format!("the turn ran past ai.timeout_seconds ({}s) while tools ran", cfg.timeout.as_secs()),
                        true,
                    )),
                },
            };
            let mut results = Vec::with_capacity(done.len());
            for ((id, name, _), c) in calls.iter().zip(done) {
                if c.denied {
                    denials += 1;
                }
                if let Some(p) = c.proposal {
                    self.record(
                        req.conv,
                        MsgKind::Proposal,
                        serde_json::to_value(&p).unwrap_or(Value::Null),
                        None,
                    )
                    .await;
                    sink.event(TurnEvent::Proposal(p.clone()))?;
                    proposals.push(p);
                }
                self.record(
                    req.conv,
                    MsgKind::ToolResult,
                    json!({"id": id, "name": name, "ok": c.ok, "reduced": c.reduced, "result": c.stored}),
                    None,
                )
                .await;
                sink.event(TurnEvent::ToolEnd {
                    id: id.clone(),
                    name: name.clone(),
                    ok: c.ok,
                    reduced: c.reduced,
                })?;
                for s in c.sources {
                    if !sources.contains(&s) {
                        sources.push(s);
                    }
                }
                results.push(c.result);
            }
            messages.push(Message {
                role: MsgRole::User,
                content: results,
            });
            if denials >= MAX_DENIALS {
                self.audit_event(&req, role, &tools_called, &sent, "tool_denied");
                return Err(error::tool_denied(format!(
                    "the {role} assistant made {denials} tool calls that were refused (outside its role or with invalid input); the turn was stopped"
                )));
            }
        };

        if !sources.is_empty() {
            sink.event(TurnEvent::Sources(sources.clone()))?;
        }
        sink.event(TurnEvent::End { stop: stop.clone() })?;
        self.audit_event(&req, role, &tools_called, &sent, stop.label());
        crate::metrics::turn(role, started.elapsed());
        Ok(TurnResult {
            messages: messages.split_off(first_new),
            stop,
            usage,
            rounds,
            proposals,
            sources,
            sent,
        })
    }

    fn audit_event(
        &self,
        req: &TurnRequest,
        role: Role,
        tools: &[String],
        sent: &[String],
        stop: &str,
    ) {
        let stream = req
            .tenant
            .map_or(AuditStream::Platform, AuditStream::Tenant);
        sprig_common::audit::record(
            stream,
            "ai.turn",
            json!({
                "user": req.user.0,
                "role": role.as_str(),
                "conversation": req.conv.map(|c| c.0),
                "provider": self.provider.name(),
                "tools": tools,
                "sent_hash": sent,
                "stop": stop,
            }),
        );
    }

    /// Answers calls that won't run with error results, keeping the
    /// conversation valid.
    async fn unrun(
        &self,
        gate: &EgressGate,
        conv: Option<ConvId>,
        messages: &mut Vec<Message>,
        calls: &[(String, String, Value)],
        why: &str,
    ) {
        let mut results = Vec::new();
        for (id, name, _) in calls {
            crate::metrics::tool_call(name, "error");
            self.record(
                conv,
                MsgKind::ToolResult,
                json!({"id": id, "name": name, "ok": false, "result": {"error": why}}),
                None,
            )
            .await;
            results.push(Block::ToolResult {
                tool_use_id: id.clone(),
                content: gate.tool_error(why),
                is_error: true,
            });
        }
        messages.push(Message {
            role: MsgRole::User,
            content: results,
        });
    }

    /// Checks and runs one call.
    async fn call(
        &self,
        gate: &EgressGate,
        role: Role,
        host: &dyn ToolHost,
        id: &str,
        name: &str,
        input: &Value,
    ) -> Called {
        let fail = |msg: &str, denied: bool| Called {
            result: Block::ToolResult {
                tool_use_id: id.to_owned(),
                content: gate.tool_error(msg),
                is_error: true,
            },
            ok: false,
            reduced: false,
            denied,
            proposal: None,
            sources: Vec::new(),
            stored: json!({"error": msg}),
        };
        let spec = match self.tools.check(role, name, input) {
            Ok(s) => s,
            Err(e) => {
                crate::metrics::tool_call(name, "denied");
                return fail(&e.message, true);
            }
        };
        if spec.effect == Effect::Propose {
            return match ToolRegistry::propose(&spec, id, input) {
                Ok(p) => {
                    crate::metrics::tool_call(name, "proposed");
                    crate::metrics::proposal(p.kind.as_str(), "shown");
                    let note = ToolRegistry::proposal_note(&p);
                    Called {
                        result: Block::ToolResult {
                            tool_use_id: id.to_owned(),
                            content: wrap(&note.to_string()),
                            is_error: false,
                        },
                        ok: true,
                        reduced: false,
                        denied: false,
                        proposal: Some(p),
                        sources: Vec::new(),
                        stored: note,
                    }
                }
                Err(e) => fail(&e.message, false),
            };
        }
        match tokio::time::timeout(TOOL_TIMEOUT, host.call(&spec, input)).await {
            Ok(Ok(out)) => {
                let g = gate.tool_result(&spec, &out);
                if g.reduced {
                    crate::metrics::egress_reduced(role);
                }
                crate::metrics::tool_call(name, "ok");
                Called {
                    result: Block::ToolResult {
                        tool_use_id: id.to_owned(),
                        content: g.content,
                        is_error: false,
                    },
                    ok: true,
                    reduced: g.reduced,
                    denied: false,
                    proposal: None,
                    sources: out.sources,
                    stored: g.stored,
                }
            }
            Ok(Err(e)) => {
                crate::metrics::tool_call(name, "error");
                fail(&e.message, false)
            }
            Err(_) => {
                crate::metrics::tool_call(name, "error");
                fail(
                    &format!("{name} took longer than {}s", TOOL_TIMEOUT.as_secs()),
                    false,
                )
            }
        }
    }
}

#[cfg(test)]
mod tests;
