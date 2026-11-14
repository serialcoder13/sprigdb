//! Providers: one trait for streaming chat with tools, and its adapters.
//!
//! - [`Provider`]: `chat` sends one request and streams [`Delta`]s to a
//!   [`ChatSink`] as they arrive; it returns the [`ChatOutcome`]: why the
//!   model stopped, the usage, the whole assistant [`Message`] (to append
//!   to the conversation as is) and exactly what was [`Sent`].
//! - [`anthropic`]: the Anthropic Messages API (`POST /v1/messages`, SSE).
//! - [`openai`]: OpenAI-compatible Chat Completions (`OpenAI`, Azure
//!   `OpenAI`, Ollama, `vLLM`, Mistral, Groq).
//! - [`scripted`]: a provider that plays back scripted replies, for tests
//!   and evaluation runs.
//!
//! **Retries.** A request that fails before anything reached the sink
//! (network, 429, 5xx, 529, an overload event at the start of the
//! stream) is retried twice, with backoff; after that, or once deltas
//! have been streamed, the call ends with 39993.

pub mod anthropic;
pub(crate) mod http;
pub mod openai;
pub mod scripted;
pub(crate) mod sse;

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Digest as _;
use sprig_common::Result;

pub use anthropic::{AnthropicConfig, AnthropicProvider};
pub use openai::{OpenAiConfig, OpenAiProvider};
pub use scripted::{Reply, ScriptedProvider};

/// The wire API a provider speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    /// The Anthropic Messages API.
    AnthropicMessages,
    /// OpenAI-compatible Chat Completions.
    OpenAiChat,
    /// A scripted stub (tests and evaluation runs).
    Scripted,
}

impl ProviderKind {
    /// `anthropic_messages`, `open_ai_chat` or `scripted`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            ProviderKind::AnthropicMessages => "anthropic_messages",
            ProviderKind::OpenAiChat => "open_ai_chat",
            ProviderKind::Scripted => "scripted",
        }
    }
}

/// How hard the model should think, for providers that support it
/// (`ai.effort.<role>`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Effort {
    /// Fastest, cheapest.
    Low,
    /// The default.
    Medium,
    /// Harder problems.
    High,
    /// Harder still.
    Xhigh,
    /// The most the model can.
    Max,
}

impl Effort {
    /// The wire name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Effort::Low => "low",
            Effort::Medium => "medium",
            Effort::High => "high",
            Effort::Xhigh => "xhigh",
            Effort::Max => "max",
        }
    }

    /// The effort named `s`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Effort> {
        [
            Effort::Low,
            Effort::Medium,
            Effort::High,
            Effort::Xhigh,
            Effort::Max,
        ]
        .into_iter()
        .find(|e| e.as_str() == s)
    }
}

/// Who wrote a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MsgRole {
    /// The user, or tool results sent back for the model.
    User,
    /// The model.
    Assistant,
}

/// One block of a message, in the providers' common shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Block {
    /// Text.
    Text {
        /// The text.
        text: String,
    },
    /// The model calls a tool.
    ToolUse {
        /// The call's id, answered by a [`Block::ToolResult`].
        id: String,
        /// The tool.
        name: String,
        /// Its input.
        input: Value,
    },
    /// A tool's result, sent back to the model.
    ToolResult {
        /// The call answered.
        tool_use_id: String,
        /// The result, as the egress gate left it.
        content: String,
        /// The tool failed or was refused.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        is_error: bool,
    },
    /// The model's reasoning. Kept and sent back unchanged: some providers
    /// check the signature against the conversation.
    Thinking {
        /// The summarized reasoning; empty when the provider omits it.
        text: String,
        /// The provider's signature.
        signature: String,
    },
    /// Reasoning the provider returned encrypted. Sent back unchanged.
    RedactedThinking {
        /// The opaque data.
        data: String,
    },
}

impl Block {
    /// A text block.
    #[must_use]
    pub fn text(t: impl Into<String>) -> Block {
        Block::Text { text: t.into() }
    }
}

/// One message of a conversation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// Who wrote it.
    pub role: MsgRole,
    /// Its blocks, in order.
    pub content: Vec<Block>,
}

impl Message {
    /// A user message of one text block.
    #[must_use]
    pub fn user(text: impl Into<String>) -> Message {
        Message {
            role: MsgRole::User,
            content: vec![Block::text(text)],
        }
    }

    /// An assistant message of one text block.
    #[must_use]
    pub fn assistant(text: impl Into<String>) -> Message {
        Message {
            role: MsgRole::Assistant,
            content: vec![Block::text(text)],
        }
    }

    /// The text blocks joined.
    #[must_use]
    pub fn text(&self) -> String {
        self.content
            .iter()
            .filter_map(|b| match b {
                Block::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }

    /// The tool calls, in order.
    #[must_use]
    pub fn tool_calls(&self) -> Vec<(&str, &str, &Value)> {
        self.content
            .iter()
            .filter_map(|b| match b {
                Block::ToolUse { id, name, input } => Some((id.as_str(), name.as_str(), input)),
                _ => None,
            })
            .collect()
    }
}

/// A tool as a provider sees it: name, description and JSON schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolDef {
    /// `snake_case`.
    pub name: String,
    /// When and why to call it.
    pub description: String,
    /// A JSON schema of the input: an object whose objects all have
    /// `additionalProperties: false`.
    pub input_schema: Value,
    /// Ask the provider to guarantee schema-valid input, where it can.
    pub strict: bool,
}

/// One request.
#[derive(Debug, Clone, PartialEq)]
pub struct ChatRequest {
    /// The model id, as the provider names it (`claude-opus-5-5`).
    pub model: String,
    /// The system prompt's text blocks. Kept byte-identical across a
    /// conversation, so caching and reasoning carry over.
    pub system: Vec<Block>,
    /// The conversation, oldest first; only ever appended to.
    pub messages: Vec<Message>,
    /// The tools the model may call.
    pub tools: Vec<ToolDef>,
    /// The most output tokens.
    pub max_tokens: u32,
    /// Effort, for providers that support it.
    pub effort: Option<Effort>,
    /// Ask the provider to cache the system prompt and tools (and the
    /// conversation so far, where it can).
    pub cache_prefix: bool,
}

/// Token counts of one call (or a sum of calls).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    /// Input tokens read without the cache.
    pub input_tokens: u64,
    /// Output tokens, reasoning included.
    pub output_tokens: u64,
    /// Input tokens read from the provider's cache.
    pub cache_read_tokens: u64,
    /// Input tokens written to the provider's cache.
    pub cache_write_tokens: u64,
}

impl Usage {
    /// Adds `o`.
    pub fn add(&mut self, o: &Usage) {
        self.input_tokens += o.input_tokens;
        self.output_tokens += o.output_tokens;
        self.cache_read_tokens += o.cache_read_tokens;
        self.cache_write_tokens += o.cache_write_tokens;
    }

    /// The tokens budgets count: input read or written uncached, and
    /// output. Cache reads cost a small fraction of an input token and
    /// aren't counted.
    #[must_use]
    pub const fn billable(&self) -> u64 {
        self.input_tokens + self.cache_write_tokens + self.output_tokens
    }
}

/// What a provider streams while it answers.
#[derive(Debug, Clone, PartialEq)]
pub enum Delta {
    /// More answer text.
    Text(String),
    /// A complete tool call (sent once its input is whole).
    ToolCall {
        /// The call's id.
        id: String,
        /// The tool.
        name: String,
        /// Its input.
        input: Value,
    },
    /// More reasoning text, for showing progress.
    Thinking(String),
    /// Usage so far.
    Usage(Usage),
}

/// Where a provider streams [`Delta`]s.
pub trait ChatSink: Send {
    /// One delta. An error stops the call (the client went away, or the
    /// user stopped the turn).
    ///
    /// # Errors
    /// Whatever stops the call; it is returned from `chat` unchanged.
    fn delta(&mut self, d: Delta) -> Result<()>;
}

impl ChatSink for Vec<Delta> {
    fn delta(&mut self, d: Delta) -> Result<()> {
        self.push(d);
        Ok(())
    }
}

/// A sink that drops everything.
#[derive(Debug, Default, Clone, Copy)]
pub struct Discard;

impl ChatSink for Discard {
    fn delta(&mut self, _d: Delta) -> Result<()> {
        Ok(())
    }
}

/// Why the model stopped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "stop", content = "detail", rename_all = "snake_case")]
pub enum Stop {
    /// It finished its answer.
    EndTurn,
    /// It wants the tool calls in its message run.
    ToolUse,
    /// It ran out of output tokens; the answer (and any tool call) may be
    /// cut short and must not be acted on.
    MaxTokens,
    /// It declined; the text says why, as far as the provider tells.
    Refusal(String),
    /// Any other reason, as the provider named it.
    Other(String),
}

impl Stop {
    /// The metric and audit label.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Stop::EndTurn => "end_turn",
            Stop::ToolUse => "tool_use",
            Stop::MaxTokens => "max_tokens",
            Stop::Refusal(_) => "refusal",
            Stop::Other(_) => "other",
        }
    }
}

/// Exactly what went to the provider: the request body (never the key,
/// which travels in a header) and its SHA-256.
#[derive(Clone, PartialEq, Eq)]
pub struct Sent {
    /// SHA-256 of `body`.
    pub sha256: [u8; 32],
    /// The JSON body.
    pub body: Arc<str>,
}

impl Sent {
    /// The record of `body`.
    #[must_use]
    pub fn of(body: impl Into<Arc<str>>) -> Sent {
        let body = body.into();
        let sha256 = sha2::Sha256::digest(body.as_bytes()).into();
        Sent { sha256, body }
    }

    /// The hash in hexadecimal.
    #[must_use]
    pub fn hex(&self) -> String {
        hex(&self.sha256)
    }
}

impl fmt::Debug for Sent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sent")
            .field("sha256", &self.hex())
            .field("bytes", &self.body.len())
            .finish()
    }
}

/// Lower-case hexadecimal.
#[must_use]
pub fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

/// How one call ended.
#[derive(Debug, Clone, PartialEq)]
pub struct ChatOutcome {
    /// Why the model stopped.
    pub stop: Stop,
    /// Tokens used.
    pub usage: Usage,
    /// The whole assistant message, to append to the conversation as is.
    pub message: Message,
    /// The model that answered, as the provider reports it.
    pub model: String,
    /// What was sent.
    pub sent: Sent,
}

/// A chat provider.
#[async_trait]
pub trait Provider: Send + Sync {
    /// Its wire API.
    fn kind(&self) -> ProviderKind;
    /// Its configured name (`anthropic`, `ollama-local`).
    fn name(&self) -> &str;
    /// Sends `req`, streams deltas to `out`, and returns the outcome.
    ///
    /// # Errors
    /// 39993 when the provider fails (after the retries), or whatever
    /// `out` returned to stop the call.
    async fn chat(&self, req: ChatRequest, out: &mut dyn ChatSink) -> Result<ChatOutcome>;
}

/// Connection limits shared by the HTTP adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpLimits {
    /// The most time to connect and finish TLS.
    pub connect_timeout: Duration,
    /// The most time between two chunks of the response.
    pub idle_timeout: Duration,
    /// Retries after the first attempt, for failures before anything
    /// streamed.
    pub retries: u32,
    /// The first backoff; doubled each retry, at most 8 seconds (or the
    /// provider's `retry-after`, at most 30).
    pub backoff: Duration,
}

impl Default for HttpLimits {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(90),
            retries: 2,
            backoff: Duration::from_millis(500),
        }
    }
}

/// A sink that remembers whether anything was streamed, so a failed
/// attempt can tell whether a retry is still invisible to the user.
pub(crate) struct Watch<'a> {
    pub(crate) inner: &'a mut dyn ChatSink,
    pub(crate) streamed: bool,
}

impl ChatSink for Watch<'_> {
    fn delta(&mut self, d: Delta) -> Result<()> {
        if !matches!(d, Delta::Usage(_)) {
            self.streamed = true;
        }
        self.inner.delta(d)
    }
}

/// One failed attempt.
#[derive(Debug)]
pub(crate) struct AttemptError {
    pub(crate) error: sprig_common::SprigError,
    /// Worth another attempt, if nothing was streamed yet.
    pub(crate) retryable: bool,
    /// The provider's `retry-after`.
    pub(crate) retry_after: Option<Duration>,
}

/// The retry schedule of one call.
pub(crate) struct Backoff {
    wait: Duration,
    left: u32,
}

impl Backoff {
    pub(crate) fn new(limits: &HttpLimits) -> Self {
        Self {
            wait: limits.backoff,
            left: limits.retries,
        }
    }

    /// How long to pause before trying again after `e`, or `None` to give
    /// up: the failure isn't retryable, something already streamed, or the
    /// retries are spent.
    pub(crate) fn next(&mut self, e: &AttemptError, streamed: bool) -> Option<Duration> {
        if !e.retryable || streamed || self.left == 0 {
            return None;
        }
        self.left -= 1;
        let pause = e
            .retry_after
            .map_or(self.wait, |r| r.min(Duration::from_secs(30)));
        self.wait = (self.wait * 2).min(Duration::from_secs(8));
        tracing::debug!(error = %e.error, "AI provider call failed; retrying");
        Some(pause)
    }
}

#[cfg(test)]
mod tests;
