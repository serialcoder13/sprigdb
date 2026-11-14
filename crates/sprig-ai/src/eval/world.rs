//! The fake tool world: a [`ToolHost`] that answers from a case's world
//! entries and records every call it gets, and a provider wrapper that
//! records every body sent.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;
use sprig_common::{Result, SprigError};

use super::case::WorldEntry;
use crate::provider::{ChatOutcome, ChatRequest, ChatSink, Provider, ProviderKind};
use crate::tools::{ToolHost, ToolOutput, ToolSpec};

/// Serves a case's world, as the signed-in caller's host would.
#[derive(Debug)]
pub struct WorldHost {
    entries: Vec<WorldEntry>,
    calls: Mutex<Vec<(String, Value)>>,
}

impl WorldHost {
    /// A host serving `entries`.
    #[must_use]
    pub fn new(entries: Vec<WorldEntry>) -> Self {
        Self {
            entries,
            calls: Mutex::new(Vec::new()),
        }
    }

    /// Every call the host got: tool and input, in order.
    #[must_use]
    pub fn calls(&self) -> Vec<(String, Value)> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl ToolHost for WorldHost {
    async fn call(&self, spec: &ToolSpec, input: &Value) -> Result<ToolOutput> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((spec.name.to_owned(), input.clone()));
        let entry = self
            .entries
            .iter()
            .filter(|e| e.tool == spec.name)
            .find(|e| e.when.is_some() && e.serves(input))
            .or_else(|| {
                self.entries
                    .iter()
                    .find(|e| e.tool == spec.name && e.when.is_none())
            });
        let Some(e) = entry else {
            return Err(world_error(format!(
                "{} found nothing for that input",
                spec.name
            )));
        };
        match e.body() {
            Ok(body) => Ok(ToolOutput {
                body,
                sources: e.sources.clone(),
            }),
            Err(m) => Err(world_error(m)),
        }
    }
}

/// A tool failure as the database would report it (`ER_UNKNOWN_ERROR`):
/// the model sees only the message.
fn world_error(m: impl Into<String>) -> SprigError {
    SprigError::standard(1105, *b"HY000", m)
}

/// Wraps a provider and keeps every body it sent.
pub struct Recording {
    inner: Arc<dyn Provider>,
    sent: Mutex<Vec<Arc<str>>>,
}

impl std::fmt::Debug for Recording {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Recording")
            .field("provider", &self.inner.name())
            .finish_non_exhaustive()
    }
}

impl Recording {
    /// Records what `inner` sends.
    #[must_use]
    pub fn new(inner: Arc<dyn Provider>) -> Self {
        Self {
            inner,
            sent: Mutex::new(Vec::new()),
        }
    }

    /// The bodies sent, oldest first.
    #[must_use]
    pub fn bodies(&self) -> Vec<Arc<str>> {
        self.sent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl Provider for Recording {
    fn kind(&self) -> ProviderKind {
        self.inner.kind()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn chat(&self, req: ChatRequest, out: &mut dyn ChatSink) -> Result<ChatOutcome> {
        let o = self.inner.chat(req, out).await?;
        self.sent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Arc::clone(&o.sent.body));
        Ok(o)
    }
}

/// The part of a sent body that carries the conversation: the system
/// prompt and the tool definitions (fixed text) are left out, in both the
/// Anthropic and the OpenAI-compatible shapes.
#[must_use]
pub fn conversation_part(body: &str) -> String {
    let Ok(mut v) = serde_json::from_str::<Value>(body) else {
        return body.to_owned();
    };
    if let Some(m) = v.as_object_mut() {
        m.remove("system");
        m.remove("tools");
        if let Some(Value::Array(msgs)) = m.get_mut("messages") {
            msgs.retain(|x| {
                !matches!(
                    x.get("role").and_then(Value::as_str),
                    Some("system" | "developer")
                )
            });
        }
    }
    v.to_string()
}
