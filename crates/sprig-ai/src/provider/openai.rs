//! OpenAI-compatible Chat Completions: `POST {base}/chat/completions` with
//! `stream: true`. One adapter covers `OpenAI`, Azure `OpenAI`, Ollama, `vLLM`,
//! Mistral and Groq; they differ only in their base URL, how the key is
//! sent, an optional query (Azure's `api-version`) and the name of the
//! output-token limit.
//!
//! - **Messages:** the system prompt is one `system` message; tool calls
//!   are `tool_calls` on the assistant message (arguments as a JSON
//!   string); each tool result is a `tool` message. Reasoning blocks from
//!   other providers aren't sent.
//! - **Tools:** `{"type":"function","function":{name, description,
//!   parameters}}`; `strict` only when configured and the schema allows
//!   it (every property required).
//! - **Stream:** `choices[0].delta` carries `content`, `refusal`,
//!   `reasoning_content` (or `reasoning`) and `tool_calls` fragments,
//!   joined by `index`; `finish_reason` is `stop`, `length`,
//!   `tool_calls` or `content_filter`; the last chunk carries `usage`
//!   (`stream_options.include_usage`); `data: [DONE]` ends it. A server
//!   that ends with `stop` after sending tool calls is read as asking for
//!   them.

use std::collections::BTreeMap;
use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sprig_common::Result;
use zeroize::Zeroizing;

use super::anthropic::{DecodeError, parse_input};
use super::http::{self, Endpoint};
use super::sse;
use super::{
    AttemptError, Backoff, Block, ChatOutcome, ChatRequest, ChatSink, Delta, HttpLimits, Message,
    MsgRole, Provider, ProviderKind, Sent, Stop, Usage, Watch,
};
use crate::error::provider_error;

/// How the key is sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyHeader {
    /// `Authorization: Bearer <key>` (`OpenAI`, Mistral, Groq, `vLLM`).
    Bearer,
    /// `api-key: <key>` (Azure `OpenAI`).
    ApiKey,
    /// No key (a local Ollama).
    None,
}

/// An OpenAI-compatible provider's settings (its record, without the key).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct OpenAiConfig {
    /// The provider's name.
    pub name: String,
    /// Up to, not including, `/chat/completions`:
    /// `https://api.openai.com/v1`, `http://127.0.0.1:11434/v1`,
    /// `https://<resource>.openai.azure.com/openai/deployments/<deployment>`.
    pub base_url: String,
    /// A query string without `?` (`api-version=2024-10-21` for Azure).
    pub query: Option<String>,
    /// How the key is sent.
    pub key_header: KeyHeader,
    /// `max_tokens` (most servers) or `max_completion_tokens` (`OpenAI`'s
    /// reasoning models).
    pub max_tokens_field: String,
    /// Send effort as `reasoning_effort` (`low`, `medium` or `high`).
    pub send_effort: bool,
    /// Send `strict: true` on tools whose schemas allow it.
    pub strict_tools: bool,
    /// Ask for usage in the last chunk.
    pub include_usage: bool,
    /// A CA file instead of the Mozilla roots.
    pub ca_file: Option<PathBuf>,
    /// Timeouts and retries.
    pub limits: HttpLimits,
}

impl Default for OpenAiConfig {
    fn default() -> Self {
        Self {
            name: "openai".to_owned(),
            base_url: "https://api.openai.com/v1".to_owned(),
            query: None,
            key_header: KeyHeader::Bearer,
            max_tokens_field: "max_tokens".to_owned(),
            send_effort: false,
            strict_tools: false,
            include_usage: true,
            ca_file: None,
            limits: HttpLimits::default(),
        }
    }
}

/// The Chat Completions adapter.
pub struct OpenAiProvider {
    cfg: OpenAiConfig,
    ep: Endpoint,
    key: Zeroizing<String>,
}

impl std::fmt::Debug for OpenAiProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenAiProvider")
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

impl OpenAiProvider {
    /// A provider calling `cfg.base_url`; `key` may be empty with
    /// [`KeyHeader::None`].
    ///
    /// # Errors
    /// 39990 for a bad base URL or CA file.
    pub fn new(cfg: OpenAiConfig, key: Zeroizing<String>) -> Result<Self> {
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
        let bearer;
        let mut headers = Vec::new();
        match self.cfg.key_header {
            KeyHeader::Bearer => {
                bearer = format!("Bearer {}", self.key.as_str());
                headers.push(("authorization", bearer.as_str()));
            }
            KeyHeader::ApiKey => headers.push(("api-key", self.key.as_str())),
            KeyHeader::None => {}
        }
        let limits = &self.cfg.limits;
        let mut resp = self
            .ep
            .post(
                "/chat/completions",
                self.cfg.query.as_deref(),
                &headers,
                &sent.body,
                limits,
            )
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
            for ev in parser.feed(&chunk).map_err(|m| self.ep.fail(m, false))? {
                dec.event(&ev.data, out)
                    .map_err(|e| self.attempt_error(e))?;
            }
            if dec.done {
                break;
            }
        }
        if !dec.done
            && let Some(ev) = parser.finish()
        {
            dec.event(&ev.data, out)
                .map_err(|e| self.attempt_error(e))?;
        }
        dec.finish(sent.clone(), out)
            .map_err(|e| self.attempt_error(e))
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
impl Provider for OpenAiProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::OpenAiChat
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

/// Whether a schema can be sent with `strict: true` here: every object
/// lists all its properties as required and allows no others.
fn strict_ok(schema: &Value) -> bool {
    match schema {
        Value::Object(m) => {
            if m.get("type").and_then(Value::as_str) == Some("object") {
                let props = m.get("properties").and_then(Value::as_object);
                let required: Vec<&str> = m
                    .get("required")
                    .and_then(Value::as_array)
                    .map(|a| a.iter().filter_map(Value::as_str).collect())
                    .unwrap_or_default();
                let all = props.is_none_or(|p| p.keys().all(|k| required.contains(&k.as_str())));
                if !all || m.get("additionalProperties") != Some(&Value::Bool(false)) {
                    return false;
                }
            }
            m.values().all(strict_ok)
        }
        Value::Array(a) => a.iter().all(strict_ok),
        _ => true,
    }
}

/// The Chat Completions body for `req`.
#[must_use]
pub fn encode_request(cfg: &OpenAiConfig, req: &ChatRequest) -> Value {
    let mut body = Map::new();
    body.insert("model".into(), json!(req.model));
    body.insert("stream".into(), json!(true));
    if cfg.include_usage {
        body.insert("stream_options".into(), json!({"include_usage": true}));
    }
    body.insert(cfg.max_tokens_field.clone(), json!(req.max_tokens));
    let mut messages = Vec::new();
    let system: String = req
        .system
        .iter()
        .filter_map(|b| match b {
            Block::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    if !system.is_empty() {
        messages.push(json!({"role": "system", "content": system}));
    }
    for m in &req.messages {
        encode_message(m, &mut messages);
    }
    body.insert("messages".into(), Value::Array(messages));
    if !req.tools.is_empty() {
        let tools: Vec<Value> = req
            .tools
            .iter()
            .map(|t| {
                let mut f = json!({
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.input_schema,
                });
                if t.strict && cfg.strict_tools && strict_ok(&t.input_schema) {
                    f["strict"] = json!(true);
                }
                json!({"type": "function", "function": f})
            })
            .collect();
        body.insert("tools".into(), Value::Array(tools));
    }
    if cfg.send_effort
        && let Some(e) = req.effort
    {
        // Chat Completions knows low, medium and high.
        let level = match e {
            super::Effort::Low => "low",
            super::Effort::Medium => "medium",
            _ => "high",
        };
        body.insert("reasoning_effort".into(), json!(level));
    }
    Value::Object(body)
}

fn encode_message(m: &Message, out: &mut Vec<Value>) {
    match m.role {
        MsgRole::User => {
            // Tool results first, each its own `tool` message; any text
            // after them is a user message.
            let mut text = String::new();
            for b in &m.content {
                match b {
                    Block::ToolResult {
                        tool_use_id,
                        content,
                        is_error,
                    } => {
                        let content = if *is_error {
                            format!("error: {content}")
                        } else {
                            content.clone()
                        };
                        out.push(json!({"role": "tool", "tool_call_id": tool_use_id, "content": content}));
                    }
                    Block::Text { text: t } => {
                        if !text.is_empty() {
                            text.push_str("\n\n");
                        }
                        text.push_str(t);
                    }
                    _ => {}
                }
            }
            if !text.is_empty() {
                out.push(json!({"role": "user", "content": text}));
            }
        }
        MsgRole::Assistant => {
            let text = m.text();
            let calls: Vec<Value> = m
                .tool_calls()
                .into_iter()
                .map(|(id, name, input)| {
                    json!({
                        "id": id,
                        "type": "function",
                        "function": {"name": name, "arguments": input.to_string()},
                    })
                })
                .collect();
            let mut v = json!({"role": "assistant"});
            v["content"] = if text.is_empty() {
                Value::Null
            } else {
                json!(text)
            };
            if !calls.is_empty() {
                v["tool_calls"] = Value::Array(calls);
            }
            out.push(v);
        }
    }
}

#[derive(Default)]
struct ToolAcc {
    id: String,
    name: String,
    args: String,
}

/// Turns stream chunks into deltas and the final message.
#[derive(Default)]
pub(crate) struct Decoder {
    text: String,
    refusal: String,
    tools: BTreeMap<u64, ToolAcc>,
    finish: Option<String>,
    usage: Usage,
    model: String,
    chunks: u64,
    pub(crate) done: bool,
}

impl Decoder {
    /// One `data:` payload.
    pub(crate) fn event(
        &mut self,
        data: &str,
        out: &mut dyn ChatSink,
    ) -> std::result::Result<(), DecodeError> {
        let data = data.trim();
        if data == "[DONE]" {
            self.done = true;
            return Ok(());
        }
        let chunk: Value = serde_json::from_str(data).map_err(|e| DecodeError::Provider {
            msg: format!("an unreadable stream chunk ({e})"),
            retryable: false,
        })?;
        if let Some(e) = chunk.get("error") {
            let msg = e
                .get("message")
                .and_then(Value::as_str)
                .map_or_else(|| e.to_string(), str::to_owned);
            let code = e
                .get("code")
                .or_else(|| e.get("type"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            return Err(DecodeError::Provider {
                msg: format!("stream error {code}: {msg}"),
                retryable: matches!(code, "server_error" | "overloaded" | "rate_limit_exceeded"),
            });
        }
        self.chunks += 1;
        if let Some(m) = chunk.get("model").and_then(Value::as_str) {
            m.clone_into(&mut self.model);
        }
        if let Some(u) = chunk.get("usage").filter(|u| !u.is_null()) {
            let g = |k: &str| u.get(k).and_then(Value::as_u64).unwrap_or(0);
            let cached = u
                .get("prompt_tokens_details")
                .and_then(|d| d.get("cached_tokens"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            self.usage = Usage {
                input_tokens: g("prompt_tokens").saturating_sub(cached),
                output_tokens: g("completion_tokens"),
                cache_read_tokens: cached,
                cache_write_tokens: 0,
            };
            out.delta(Delta::Usage(self.usage))
                .map_err(DecodeError::Sink)?;
        }
        let Some(choice) = chunk
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|c| c.first())
        else {
            return Ok(());
        };
        let delta = choice.get("delta").cloned().unwrap_or(Value::Null);
        let field = |k: &str| {
            delta
                .get(k)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
        };
        if let Some(t) = field("content") {
            self.text.push_str(t);
            out.delta(Delta::Text(t.to_owned()))
                .map_err(DecodeError::Sink)?;
        }
        if let Some(t) = field("reasoning_content").or_else(|| field("reasoning")) {
            out.delta(Delta::Thinking(t.to_owned()))
                .map_err(DecodeError::Sink)?;
        }
        if let Some(t) = field("refusal") {
            self.refusal.push_str(t);
        }
        if let Some(calls) = delta.get("tool_calls").and_then(Value::as_array) {
            for (pos, c) in calls.iter().enumerate() {
                let idx = c.get("index").and_then(Value::as_u64).unwrap_or(pos as u64);
                let acc = self.tools.entry(idx).or_default();
                if let Some(id) = c
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    id.clone_into(&mut acc.id);
                }
                let f = c.get("function").cloned().unwrap_or(Value::Null);
                if let Some(n) = f
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    acc.name.push_str(n);
                }
                match f.get("arguments") {
                    Some(Value::String(a)) => acc.args.push_str(a),
                    // Some servers send the arguments as an object.
                    Some(o @ Value::Object(_)) => acc.args.push_str(&o.to_string()),
                    _ => {}
                }
            }
        }
        if let Some(f) = choice.get("finish_reason").and_then(Value::as_str) {
            self.finish = Some(f.to_owned());
        }
        Ok(())
    }

    /// The outcome, once the stream ended.
    pub(crate) fn finish(
        self,
        sent: Sent,
        out: &mut dyn ChatSink,
    ) -> std::result::Result<ChatOutcome, DecodeError> {
        let Some(finish) = self.finish else {
            return Err(DecodeError::Provider {
                msg: if self.chunks == 0 {
                    "the stream held no chunks".to_owned()
                } else {
                    "the stream ended without a finish reason".to_owned()
                },
                retryable: true,
            });
        };
        let mut content = Vec::new();
        if !self.text.is_empty() {
            content.push(Block::Text { text: self.text });
        }
        let has_tools = !self.tools.is_empty();
        for (i, (_, t)) in self.tools.into_iter().enumerate() {
            let id = if t.id.is_empty() {
                format!("call_{i}")
            } else {
                t.id
            };
            let input = parse_input(&t.args);
            out.delta(Delta::ToolCall {
                id: id.clone(),
                name: t.name.clone(),
                input: input.clone(),
            })
            .map_err(DecodeError::Sink)?;
            content.push(Block::ToolUse {
                id,
                name: t.name,
                input,
            });
        }
        let stop = if self.refusal.is_empty() {
            match finish.as_str() {
                "tool_calls" | "function_call" => Stop::ToolUse,
                "stop" if has_tools => Stop::ToolUse,
                "stop" => Stop::EndTurn,
                "length" => Stop::MaxTokens,
                "content_filter" => {
                    Stop::Refusal("declined by the provider's content filter".to_owned())
                }
                other => Stop::Other(other.to_owned()),
            }
        } else {
            Stop::Refusal(self.refusal)
        };
        Ok(ChatOutcome {
            stop,
            usage: self.usage,
            message: Message {
                role: MsgRole::Assistant,
                content,
            },
            model: self.model,
            sent,
        })
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
        dec.event(&ev.data, out).map_err(fail)?;
    }
    dec.finish(Sent::of(""), out).map_err(fail)
}
