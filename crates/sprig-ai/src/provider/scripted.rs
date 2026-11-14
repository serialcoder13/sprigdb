//! A provider that plays back scripted replies: for tests of the turn
//! loop, the routes built on it, and evaluation runs in CI, where no
//! request may leave the machine.
//!
//! Each `chat` call takes the next [`Reply`], streams it as a real
//! provider would (text in pieces, tool calls whole, usage last) and
//! records the request it was given, so a test can check exactly what
//! would have been sent.

use std::collections::VecDeque;
use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::Value;
use sprig_common::{Result, SprigError};

use super::{
    Block, ChatOutcome, ChatRequest, ChatSink, Delta, Message, MsgRole, Provider, ProviderKind,
    Sent, Stop, Usage,
};
use crate::error::provider_error;

/// One scripted answer.
#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    /// Text, then `end_turn`.
    Text(String),
    /// Optional text, then tool calls `(id, name, input)`, then `tool_use`.
    Tools(String, Vec<(String, String, Value)>),
    /// A refusal with this explanation.
    Refuse(String),
    /// Text cut off at the output limit.
    MaxTokens(String),
    /// The provider fails (39993), retryable or not.
    Fail(String, bool),
}

/// The scripted provider.
#[derive(Debug, Default)]
pub struct ScriptedProvider {
    name: String,
    replies: Mutex<VecDeque<Reply>>,
    seen: Mutex<Vec<ChatRequest>>,
    usage: Usage,
}

impl ScriptedProvider {
    /// A provider that answers with `replies` in order, each call
    /// reporting `usage`.
    #[must_use]
    pub fn new(replies: impl IntoIterator<Item = Reply>, usage: Usage) -> Self {
        Self {
            name: "scripted".to_owned(),
            replies: Mutex::new(replies.into_iter().collect()),
            seen: Mutex::new(Vec::new()),
            usage,
        }
    }

    /// Every request received, oldest first.
    #[must_use]
    pub fn requests(&self) -> Vec<ChatRequest> {
        self.seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Replies not yet used.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.replies
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

fn stream_text(text: &str, out: &mut dyn ChatSink) -> Result<()> {
    // In pieces of up to 16 characters, as a provider would.
    let chars: Vec<char> = text.chars().collect();
    for piece in chars.chunks(16) {
        out.delta(Delta::Text(piece.iter().collect()))?;
    }
    Ok(())
}

#[async_trait]
impl Provider for ScriptedProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Scripted
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn chat(&self, req: ChatRequest, out: &mut dyn ChatSink) -> Result<ChatOutcome> {
        let body = serde_json::json!({
            "model": req.model,
            "system": req.system,
            "messages": req.messages,
            "tools": req.tools,
            "max_tokens": req.max_tokens,
        })
        .to_string();
        let model = req.model.clone();
        self.seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(req);
        let reply = self
            .replies
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
            .ok_or_else(|| SprigError::internal("the scripted provider has no reply left"))?;
        let mut content = Vec::new();
        let stop = match reply {
            Reply::Text(t) => {
                stream_text(&t, out)?;
                content.push(Block::Text { text: t });
                Stop::EndTurn
            }
            Reply::MaxTokens(t) => {
                stream_text(&t, out)?;
                content.push(Block::Text { text: t });
                Stop::MaxTokens
            }
            Reply::Tools(t, calls) => {
                if !t.is_empty() {
                    stream_text(&t, out)?;
                    content.push(Block::Text { text: t });
                }
                for (id, name, input) in calls {
                    out.delta(Delta::ToolCall {
                        id: id.clone(),
                        name: name.clone(),
                        input: input.clone(),
                    })?;
                    content.push(Block::ToolUse { id, name, input });
                }
                Stop::ToolUse
            }
            Reply::Refuse(why) => Stop::Refusal(why),
            Reply::Fail(msg, retryable) => {
                return Err(provider_error(&self.name, msg, retryable));
            }
        };
        out.delta(Delta::Usage(self.usage))?;
        Ok(ChatOutcome {
            stop,
            usage: self.usage,
            message: Message {
                role: MsgRole::Assistant,
                content,
            },
            model,
            sent: Sent::of(body),
        })
    }
}
