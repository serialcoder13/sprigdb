//! The Anthropic Messages API: `POST {base}/v1/messages` with
//! `stream: true`.
//!
//! - **Headers:** `x-api-key`, `anthropic-version` (2026-10-21 by
//!   default), and `anthropic-beta` when betas are configured.
//! - **Tools:** JSON-schema tools with `strict: true`; tool choice is left
//!   to the model (`auto`), since newer models refuse forced tool use.
//! - **Caching:** with `cache_prefix`, a `cache_control` breakpoint on the
//!   last system block (it covers the tools, which come first) and the
//!   request-level `cache_control` that moves with the conversation.
//! - **Thinking:** adaptive, summarized so progress can be shown; effort
//!   goes in `output_config.effort`. Thinking blocks come back with a
//!   signature and are kept in the message unchanged.
//! - **Stops:** `end_turn` and `stop_sequence` end the turn, `tool_use`
//!   asks for tools, `max_tokens` and `model_context_window_exceeded` cut
//!   the answer, and `refusal` is surfaced with its explanation.
//! - **Stream events:** `message_start` (model, input usage),
//!   `content_block_start|delta|stop` (text, thinking, signature,
//!   tool-input JSON), `message_delta` (stop reason, output usage),
//!   `message_stop`, `ping`, and `error` (overload and API errors at the
//!   start of a stream are retried).

use std::collections::BTreeMap;
use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sprig_common::Result;
use zeroize::Zeroizing;

use super::http::{self, Endpoint};
use super::sse;
use super::{
    AttemptError, Backoff, Block, ChatOutcome, ChatRequest, ChatSink, Delta, HttpLimits, Message,
    MsgRole, Provider, ProviderKind, Sent, Stop, Usage, Watch,
};
use crate::error::provider_error;

/// The API version sent in `anthropic-version`.
pub const API_VERSION: &str = "2026-10-21";
/// The public endpoint.
pub const DEFAULT_BASE_URL: &str = "https://api.anthropic.com";
/// The model the examples and the default configuration name.
pub const DEFAULT_MODEL: &str = "claude-opus-5-5";

/// An Anthropic Messages provider's settings (its record, without the
/// key).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AnthropicConfig {
    /// The provider's name.
    pub name: String,
    /// `https://api.anthropic.com`, or a compatible gateway.
    pub base_url: String,
    /// `anthropic-version`.
    pub version: String,
    /// `anthropic-beta` values.
    pub betas: Vec<String>,
    /// Ask for adaptive thinking. Off for models without it.
    pub adaptive_thinking: bool,
    /// Ask for summarized thinking text (otherwise it is omitted and only
    /// the signature comes back).
    pub show_thinking: bool,
    /// Send tools with `strict: true`.
    pub strict_tools: bool,
    /// A CA file instead of the Mozilla roots (a TLS-inspecting proxy).
    pub ca_file: Option<PathBuf>,
    /// Timeouts and retries.
    pub limits: HttpLimits,
}

impl Default for AnthropicConfig {
    fn default() -> Self {
        Self {
            name: "anthropic".to_owned(),
            base_url: DEFAULT_BASE_URL.to_owned(),
            version: API_VERSION.to_owned(),
            betas: Vec::new(),
            adaptive_thinking: true,
            show_thinking: true,
            strict_tools: true,
            ca_file: None,
            limits: HttpLimits::default(),
        }
    }
}

/// The Anthropic Messages adapter.
pub struct AnthropicProvider {
    cfg: AnthropicConfig,
    ep: Endpoint,
    key: Zeroizing<String>,
}

impl std::fmt::Debug for AnthropicProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnthropicProvider")
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

impl AnthropicProvider {
    /// A provider calling `cfg.base_url` with `key`.
    ///
    /// # Errors
    /// 39990 for a bad base URL or CA file.
    pub fn new(cfg: AnthropicConfig, key: Zeroizing<String>) -> Result<Self> {
        let ep = Endpoint::parse(&cfg.name, &cfg.base_url, cfg.ca_file.as_deref())?;
        Ok(Self { cfg, ep, key })
    }

    /// The request body for `req`, exactly as it is sent.
    ///
    /// # Errors
    /// Internal if the body can't be serialized (a bug).
    pub fn encode(&self, req: &ChatRequest) -> Result<String> {
        serde_json::to_string(&encode_request(&self.cfg, req))
            .map_err(|e| sprig_common::SprigError::internal(format!("encoding a request: {e}")))
    }

    async fn attempt(
        &self,
        sent: &Sent,
        out: &mut Watch<'_>,
    ) -> std::result::Result<ChatOutcome, AttemptError> {
        let betas = self.cfg.betas.join(",");
        let mut headers = vec![
            ("x-api-key", self.key.as_str()),
            ("anthropic-version", self.cfg.version.as_str()),
        ];
        if !betas.is_empty() {
            headers.push(("anthropic-beta", betas.as_str()));
        }
        let limits = &self.cfg.limits;
        let mut resp = self
            .ep
            .post("/v1/messages", None, &headers, &sent.body, limits)
            .await?;
        let status = resp.status().as_u16();
        if status != 200 {
            let after = http::retry_after(&resp);
            let body = http::error_body(&self.ep, resp.body_mut(), limits.idle_timeout).await;
            let mut e = self.ep.fail(
                http::error_message(status, &body),
                http::retryable_status(status),
            );
            e.retry_after = after;
            return Err(e);
        }
        let mut parser = sse::Parser::default();
        let mut dec = Decoder::default();
        let body = resp.body_mut();
        while let Some(chunk) = http::next_chunk(&self.ep, body, limits.idle_timeout).await? {
            let events = parser.feed(&chunk).map_err(|m| self.ep.fail(m, false))?;
            for ev in events {
                dec.event(&ev.name, &ev.data, out)
                    .map_err(|e| self.attempt_error(e))?;
            }
            if dec.finished {
                break;
            }
        }
        if !dec.finished
            && let Some(ev) = parser.finish()
        {
            dec.event(&ev.name, &ev.data, out)
                .map_err(|e| self.attempt_error(e))?;
        }
        dec.finish(sent.clone()).map_err(|e| self.attempt_error(e))
    }

    fn attempt_error(&self, e: DecodeError) -> AttemptError {
        match e {
            DecodeError::Sink(error) => AttemptError {
                error,
                retryable: false,
                retry_after: None,
            },
            DecodeError::Provider { msg, retryable } => self.ep.fail(msg, retryable),
        }
    }
}

#[async_trait]
impl Provider for AnthropicProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::AnthropicMessages
    }

    fn name(&self) -> &str {
        &self.cfg.name
    }

    async fn chat(&self, req: ChatRequest, out: &mut dyn ChatSink) -> Result<ChatOutcome> {
        let sent = Sent::of(self.encode(&req)?);
        let mut watch = Watch {
            inner: out,
            streamed: false,
        };
        let mut backoff = Backoff::new(&self.cfg.limits);
        loop {
            match self.attempt(&sent, &mut watch).await {
                Ok(o) => return Ok(o),
                Err(e) => match backoff.next(&e, watch.streamed) {
                    Some(pause) => tokio::time::sleep(pause).await,
                    None => return Err(e.error),
                },
            }
        }
    }
}

fn ephemeral() -> Value {
    json!({"type": "ephemeral"})
}

/// The Messages API body for `req`.
#[must_use]
pub fn encode_request(cfg: &AnthropicConfig, req: &ChatRequest) -> Value {
    let mut body = Map::new();
    body.insert("model".into(), json!(req.model));
    body.insert("max_tokens".into(), json!(req.max_tokens));
    body.insert("stream".into(), json!(true));
    let mut system: Vec<Value> = req
        .system
        .iter()
        .filter_map(|b| match b {
            Block::Text { text } => Some(json!({"type": "text", "text": text})),
            _ => None,
        })
        .collect();
    let mut tools: Vec<Value> = req
        .tools
        .iter()
        .map(|t| {
            let mut v = json!({
                "name": t.name,
                "description": t.description,
                "input_schema": t.input_schema,
            });
            if t.strict && cfg.strict_tools {
                v["strict"] = json!(true);
            }
            v
        })
        .collect();
    if req.cache_prefix {
        if let Some(last) = system.last_mut() {
            last["cache_control"] = ephemeral();
        } else if let Some(last) = tools.last_mut() {
            last["cache_control"] = ephemeral();
        }
    }
    if !tools.is_empty() {
        body.insert("tools".into(), Value::Array(tools));
    }
    if !system.is_empty() {
        body.insert("system".into(), Value::Array(system));
    }
    body.insert(
        "messages".into(),
        Value::Array(req.messages.iter().map(encode_message).collect()),
    );
    if cfg.adaptive_thinking {
        let mut t = json!({"type": "adaptive"});
        if cfg.show_thinking {
            t["display"] = json!("summarized");
        }
        body.insert("thinking".into(), t);
    }
    if let Some(e) = req.effort {
        body.insert("output_config".into(), json!({"effort": e.as_str()}));
    }
    if req.cache_prefix {
        body.insert("cache_control".into(), ephemeral());
    }
    Value::Object(body)
}

fn encode_message(m: &Message) -> Value {
    let role = match m.role {
        MsgRole::User => "user",
        MsgRole::Assistant => "assistant",
    };
    let content: Vec<Value> = m
        .content
        .iter()
        .map(|b| match b {
            Block::Text { text } => json!({"type": "text", "text": text}),
            Block::ToolUse { id, name, input } => {
                json!({"type": "tool_use", "id": id, "name": name, "input": input})
            }
            Block::ToolResult {
                tool_use_id,
                content,
                is_error,
            } => {
                let mut v =
                    json!({"type": "tool_result", "tool_use_id": tool_use_id, "content": content});
                if *is_error {
                    v["is_error"] = json!(true);
                }
                v
            }
            Block::Thinking { text, signature } => {
                json!({"type": "thinking", "thinking": text, "signature": signature})
            }
            Block::RedactedThinking { data } => json!({"type": "redacted_thinking", "data": data}),
        })
        .collect();
    json!({"role": role, "content": content})
}

/// A stream decoding failure.
#[derive(Debug)]
pub(crate) enum DecodeError {
    /// The sink stopped the call.
    Sink(sprig_common::SprigError),
    /// The provider sent an error or something unreadable.
    Provider {
        /// What happened.
        msg: String,
        /// Worth another attempt.
        retryable: bool,
    },
}

enum Partial {
    Text(String),
    Thinking {
        text: String,
        signature: String,
    },
    Redacted(String),
    Tool {
        id: String,
        name: String,
        json: String,
    },
    Other,
}

/// Turns stream events into deltas and the final message.
#[derive(Default)]
pub(crate) struct Decoder {
    open: BTreeMap<u64, Partial>,
    done: BTreeMap<u64, Block>,
    usage: Usage,
    stop: Option<String>,
    details: Option<Value>,
    model: String,
    started: bool,
    pub(crate) finished: bool,
}

fn n(v: &Value, k: &str) -> Option<u64> {
    v.get(k).and_then(Value::as_u64)
}

fn bad(msg: impl Into<String>) -> DecodeError {
    DecodeError::Provider {
        msg: msg.into(),
        retryable: false,
    }
}

impl Decoder {
    fn usage_from(&mut self, u: &Value) {
        if let Some(x) = n(u, "input_tokens") {
            self.usage.input_tokens = x;
        }
        if let Some(x) = n(u, "output_tokens") {
            self.usage.output_tokens = x;
        }
        if let Some(x) = n(u, "cache_read_input_tokens") {
            self.usage.cache_read_tokens = x;
        }
        if let Some(x) = n(u, "cache_creation_input_tokens") {
            self.usage.cache_write_tokens = x;
        }
    }

    /// One event.
    #[allow(clippy::too_many_lines)] // one match over the event types
    pub(crate) fn event(
        &mut self,
        name: &str,
        data: &str,
        out: &mut dyn ChatSink,
    ) -> std::result::Result<(), DecodeError> {
        let v: Value = serde_json::from_str(data)
            .map_err(|e| bad(format!("an unreadable stream event ({e})")))?;
        let kind = v
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or(name)
            .to_owned();
        let sink = |r: Result<()>| r.map_err(DecodeError::Sink);
        match kind.as_str() {
            "message_start" => {
                self.started = true;
                let m = v.get("message").cloned().unwrap_or(Value::Null);
                if let Some(model) = m.get("model").and_then(Value::as_str) {
                    model.clone_into(&mut self.model);
                }
                if let Some(u) = m.get("usage") {
                    self.usage_from(u);
                }
            }
            "content_block_start" => {
                let idx = n(&v, "index").unwrap_or(0);
                let b = v.get("content_block").cloned().unwrap_or(Value::Null);
                let s = |k: &str| {
                    b.get(k)
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned()
                };
                let p = match b.get("type").and_then(Value::as_str) {
                    Some("text") => {
                        let t = s("text");
                        if !t.is_empty() {
                            sink(out.delta(Delta::Text(t.clone())))?;
                        }
                        Partial::Text(t)
                    }
                    Some("thinking") => Partial::Thinking {
                        text: s("thinking"),
                        signature: s("signature"),
                    },
                    Some("redacted_thinking") => Partial::Redacted(s("data")),
                    Some("tool_use") => Partial::Tool {
                        id: s("id"),
                        name: s("name"),
                        json: String::new(),
                    },
                    _ => Partial::Other,
                };
                self.open.insert(idx, p);
            }
            "content_block_delta" => {
                let idx = n(&v, "index").unwrap_or(0);
                let d = v.get("delta").cloned().unwrap_or(Value::Null);
                let s = |k: &str| d.get(k).and_then(Value::as_str).unwrap_or_default();
                match (
                    self.open.get_mut(&idx),
                    d.get("type").and_then(Value::as_str),
                ) {
                    (Some(Partial::Text(t)), Some("text_delta")) => {
                        let piece = s("text");
                        t.push_str(piece);
                        if !piece.is_empty() {
                            sink(out.delta(Delta::Text(piece.to_owned())))?;
                        }
                    }
                    (Some(Partial::Thinking { text, .. }), Some("thinking_delta")) => {
                        let piece = s("thinking");
                        text.push_str(piece);
                        if !piece.is_empty() {
                            sink(out.delta(Delta::Thinking(piece.to_owned())))?;
                        }
                    }
                    (Some(Partial::Thinking { signature, .. }), Some("signature_delta")) => {
                        signature.push_str(s("signature"));
                    }
                    (Some(Partial::Tool { json, .. }), Some("input_json_delta")) => {
                        json.push_str(s("partial_json"));
                    }
                    _ => {}
                }
            }
            "content_block_stop" => {
                let idx = n(&v, "index").unwrap_or(0);
                let block = match self.open.remove(&idx) {
                    Some(Partial::Text(text)) => Some(Block::Text { text }),
                    Some(Partial::Thinking { text, signature }) => {
                        Some(Block::Thinking { text, signature })
                    }
                    Some(Partial::Redacted(data)) => Some(Block::RedactedThinking { data }),
                    Some(Partial::Tool { id, name, json }) => {
                        let input = parse_input(&json);
                        sink(out.delta(Delta::ToolCall {
                            id: id.clone(),
                            name: name.clone(),
                            input: input.clone(),
                        }))?;
                        Some(Block::ToolUse { id, name, input })
                    }
                    Some(Partial::Other) | None => None,
                };
                if let Some(b) = block {
                    self.done.insert(idx, b);
                }
            }
            "message_delta" => {
                let d = v.get("delta").cloned().unwrap_or(Value::Null);
                if let Some(r) = d.get("stop_reason").and_then(Value::as_str) {
                    self.stop = Some(r.to_owned());
                }
                if let Some(sd) = d.get("stop_details").or_else(|| v.get("stop_details"))
                    && !sd.is_null()
                {
                    self.details = Some(sd.clone());
                }
                if let Some(u) = v.get("usage") {
                    self.usage_from(u);
                    sink(out.delta(Delta::Usage(self.usage)))?;
                }
            }
            "message_stop" => self.finished = true,
            "error" => {
                let e = v.get("error").cloned().unwrap_or(Value::Null);
                let t = e.get("type").and_then(Value::as_str).unwrap_or("error");
                let m = e.get("message").and_then(Value::as_str).unwrap_or_default();
                return Err(DecodeError::Provider {
                    msg: format!("stream error {t}: {m}"),
                    retryable: matches!(t, "overloaded_error" | "api_error" | "rate_limit_error"),
                });
            }
            _ => {}
        }
        Ok(())
    }

    /// The outcome, once the stream ended.
    pub(crate) fn finish(self, sent: Sent) -> std::result::Result<ChatOutcome, DecodeError> {
        if !self.started || !self.finished {
            return Err(DecodeError::Provider {
                msg: "the stream ended before message_stop".to_owned(),
                retryable: true,
            });
        }
        let reason = self.stop.unwrap_or_default();
        let stop = match reason.as_str() {
            "end_turn" | "stop_sequence" => Stop::EndTurn,
            "tool_use" => Stop::ToolUse,
            "max_tokens" | "model_context_window_exceeded" => Stop::MaxTokens,
            "refusal" => Stop::Refusal(refusal_text(self.details.as_ref())),
            "" => Stop::Other("none".to_owned()),
            other => Stop::Other(other.to_owned()),
        };
        Ok(ChatOutcome {
            stop,
            usage: self.usage,
            message: Message {
                role: MsgRole::Assistant,
                content: self.done.into_values().collect(),
            },
            model: self.model,
            sent,
        })
    }
}

/// The text of a refusal: the provider's explanation, else its category.
fn refusal_text(details: Option<&Value>) -> String {
    let s = |k: &str| {
        details
            .and_then(|d| d.get(k))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
    };
    match (s("explanation"), s("category")) {
        (Some(e), _) => e.to_owned(),
        (None, Some(c)) => format!("declined by the provider's {c} policy"),
        (None, None) => "the model declined the request".to_owned(),
    }
}

/// A tool call's accumulated input; `{}` when empty. Input that isn't a
/// JSON object is kept under `invalid_json`, which no tool schema allows,
/// so the call is answered with an error the model can correct.
pub(crate) fn parse_input(json: &str) -> Value {
    if json.trim().is_empty() {
        return json!({});
    }
    match serde_json::from_str::<Value>(json) {
        Ok(v @ Value::Object(_)) => v,
        _ => json!({"invalid_json": json}),
    }
}

/// Decodes a recorded stream (the whole SSE text) as `chat` would.
///
/// # Errors
/// 39993 when the stream holds an error or ends early; the sink's error
/// when it stops.
pub fn decode_recorded(sse_text: &str, out: &mut dyn ChatSink) -> Result<ChatOutcome> {
    let mut p = sse::Parser::default();
    let mut dec = Decoder::default();
    let mut events = p
        .feed(sse_text.as_bytes())
        .map_err(|m| provider_error("recorded", m, false))?;
    events.extend(p.finish());
    let fail = |e: DecodeError| match e {
        DecodeError::Sink(e) => e,
        DecodeError::Provider { msg, retryable } => provider_error("recorded", msg, retryable),
    };
    for ev in events {
        dec.event(&ev.name, &ev.data, out).map_err(fail)?;
    }
    dec.finish(Sent::of("")).map_err(fail)
}
