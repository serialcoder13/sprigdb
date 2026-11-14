//! Providers for live runs.
//!
//! Live runs call real models, so they are opt-in twice over: the
//! environment variable [`LIVE_ENV`] must be `1`, and the provider needs a
//! key in the environment. Providers come from the TOML file named by
//! [`PROVIDERS_ENV`], or else from three built-ins:
//!
//! | Name | API | Key |
//! |---|---|---|
//! | `anthropic` | Anthropic Messages, `https://api.anthropic.com` | `ANTHROPIC_API_KEY` |
//! | `openai` | Chat Completions, `https://api.openai.com/v1` | `OPENAI_API_KEY` |
//! | `ollama` | Chat Completions, `http://127.0.0.1:11434/v1` | none |
//!
//! A providers file holds the same settings as a provider record:
//!
//! ```toml
//! [providers.claude]
//! kind = "anthropic_messages"
//! key_env = "ANTHROPIC_API_KEY"
//!
//! [providers.local]
//! kind = "open_ai_chat"
//! base_url = "http://127.0.0.1:8000/v1"
//! key_header = "none"
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Deserialize;
use zeroize::Zeroizing;

use crate::provider::anthropic::{AnthropicConfig, AnthropicProvider};
use crate::provider::openai::{KeyHeader, OpenAiConfig, OpenAiProvider};
use crate::provider::Provider;
use crate::providers::ProviderSettings;

/// Must be `1` for a live run.
pub const LIVE_ENV: &str = "SPRIG_AI_EVAL_LIVE";
/// A providers file, instead of the built-ins.
pub const PROVIDERS_ENV: &str = "SPRIG_AI_EVAL_PROVIDERS";

/// One provider of a providers file.
#[derive(Debug, Clone, Deserialize)]
pub struct ProviderEntry {
    /// The adapter and its settings.
    #[serde(flatten)]
    pub settings: ProviderSettings,
    /// The environment variable holding the key; none for a provider
    /// without one.
    #[serde(default)]
    pub key_env: Option<String>,
}

/// A providers file.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ProviderFile {
    /// By name.
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderEntry>,
}

/// Reads a providers file.
///
/// # Errors
/// What doesn't parse.
pub fn parse_providers(text: &str) -> Result<ProviderFile, String> {
    toml::from_str(text).map_err(|e| e.to_string())
}

/// The built-in provider named `name`.
#[must_use]
pub fn builtin(name: &str) -> Option<ProviderEntry> {
    Some(match name {
        "anthropic" => ProviderEntry {
            settings: ProviderSettings::AnthropicMessages(AnthropicConfig::default()),
            key_env: Some("ANTHROPIC_API_KEY".to_owned()),
        },
        "openai" => ProviderEntry {
            settings: ProviderSettings::OpenAiChat(OpenAiConfig::default()),
            key_env: Some("OPENAI_API_KEY".to_owned()),
        },
        "ollama" => ProviderEntry {
            settings: ProviderSettings::OpenAiChat(OpenAiConfig {
                name: "ollama".to_owned(),
                base_url: "http://127.0.0.1:11434/v1".to_owned(),
                key_header: KeyHeader::None,
                ..OpenAiConfig::default()
            }),
            key_env: None,
        },
        _ => return None,
    })
}

/// `provider/model` split in two.
///
/// # Errors
/// When there is no `/`.
pub fn split_model(spec: &str) -> Result<(&str, &str), String> {
    match spec.split_once('/') {
        Some((p, m)) if !p.is_empty() && !m.is_empty() => Ok((p, m)),
        _ => Err(format!("{spec:?} isn't provider/model")),
    }
}

/// The provider and model of `spec` (`provider/model`), from `file` or
/// the built-ins, with its key read from the environment.
///
/// # Errors
/// Live runs not enabled, an unknown provider, a missing key or bad
/// settings.
pub fn provider(spec: &str, file: Option<&ProviderFile>) -> Result<(Arc<dyn Provider>, String), String> {
    if std::env::var(LIVE_ENV).as_deref() != Ok("1") {
        return Err(format!(
            "live runs call a real model; set {LIVE_ENV}=1 to allow them"
        ));
    }
    let (name, model) = split_model(spec)?;
    let entry = file
        .and_then(|f| f.providers.get(name).cloned())
        .or_else(|| builtin(name))
        .ok_or_else(|| format!("no provider {name} (built-ins: anthropic, openai, ollama)"))?;
    let key = match &entry.key_env {
        Some(var) => Zeroizing::new(
            std::env::var(var).map_err(|_| format!("provider {name} needs its key in {var}"))?,
        ),
        None => Zeroizing::new(String::new()),
    };
    let p: Arc<dyn Provider> = match entry.settings {
        ProviderSettings::AnthropicMessages(mut c) => {
            c.name = name.to_owned();
            Arc::new(AnthropicProvider::new(c, key).map_err(|e| e.message)?)
        }
        ProviderSettings::OpenAiChat(mut c) => {
            c.name = name.to_owned();
            Arc::new(OpenAiProvider::new(c, key).map_err(|e| e.message)?)
        }
    };
    Ok((p, model.to_owned()))
}
