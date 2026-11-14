//! The operator's providers: records in the global tier at
//! `/sprig/g/ai/providers/<name>`, each with its key sealed under purpose
//! `ai` ([`crate::keys`]).
//!
//! - A role's model is the setting `ai.model.<role>`: `provider/model`
//!   ([`RoleModel`]), naming one of these records.
//! - Keys can be written but not read: [`ProviderRecord::public`] is what
//!   any listing returns.
//! - [`build`] opens the key and makes the [`Provider`], in the process
//!   that calls it.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Consistency, ListOpts, MetaStore, Op, Record, Tier, Txn, Versioned};
use zeroize::Zeroizing;

use crate::keys::{self, AiKeyring, SealedKey};
use crate::provider::anthropic::{AnthropicConfig, AnthropicProvider};
use crate::provider::openai::{KeyHeader, OpenAiConfig, OpenAiProvider};
use crate::provider::{Provider, ProviderKind};
use crate::role::Role;

/// The adapter settings of a record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderSettings {
    /// The Anthropic Messages API.
    AnthropicMessages(AnthropicConfig),
    /// OpenAI-compatible Chat Completions.
    OpenAiChat(OpenAiConfig),
}

/// One provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderRecord {
    /// `[a-z0-9_-]{1,64}`.
    pub name: String,
    /// The adapter and its settings (the `name` inside is overwritten by
    /// the record's).
    pub settings: ProviderSettings,
    /// The sealed key; `None` for a provider that takes none (a local
    /// Ollama).
    #[serde(default)]
    pub key: Option<SealedKey>,
    /// When it was last written, ms since the Unix epoch.
    #[serde(default)]
    pub updated_ms: u64,
}

impl Versioned for ProviderRecord {
    const V: u32 = 1;
}

impl ProviderRecord {
    /// The wire API.
    #[must_use]
    pub fn kind(&self) -> ProviderKind {
        match self.settings {
            ProviderSettings::AnthropicMessages(_) => ProviderKind::AnthropicMessages,
            ProviderSettings::OpenAiChat(_) => ProviderKind::OpenAiChat,
        }
    }

    /// What a listing shows: everything but the key, and whether one is
    /// set.
    #[must_use]
    pub fn public(&self) -> Value {
        let settings = serde_json::to_value(&self.settings).unwrap_or(Value::Null);
        json!({
            "name": self.name,
            "kind": self.kind().as_str(),
            "settings": settings,
            "has_key": self.key.is_some(),
            "key_version": self.key.as_ref().map(|k| k.key_version),
            "updated_ms": self.updated_ms,
        })
    }
}

/// A role's model: `ai.model.<role>` = `provider/model`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleModel {
    /// The provider record's name.
    pub provider: String,
    /// The model id, as the provider names it.
    pub model: String,
}

impl RoleModel {
    /// Parses `provider/model` (the model may itself hold `/`, as Ollama
    /// and vLLM names do).
    ///
    /// # Errors
    /// 39990 for an empty value or a missing part.
    pub fn parse(role: Role, value: &str) -> Result<Self> {
        let v = value.trim();
        if v.is_empty() {
            return Err(crate::error::not_configured(
                role,
                format!("ai.model.{role} is empty"),
            ));
        }
        match v.split_once('/') {
            Some((p, m)) if !p.is_empty() && !m.is_empty() => Ok(Self {
                provider: p.to_owned(),
                model: m.to_owned(),
            }),
            _ => Err(crate::error::not_configured(
                role,
                format!("ai.model.{role} must be provider/model, not {v:?}"),
            )),
        }
    }
}

/// Checks a provider name.
///
/// # Errors
/// 39790-style invalid request.
pub fn check_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            format!("a provider name is 1 to 64 of a-z, 0-9, _ and -, not {name:?}"),
        ))
    }
}

/// Makes the provider of `rec`, opening its key.
///
/// # Errors
/// 39990 when the key doesn't open or the settings are invalid.
pub fn build(rec: &ProviderRecord, keys: &dyn AiKeyring) -> Result<Arc<dyn Provider>> {
    let key = match &rec.key {
        Some(s) => keys::open(keys, keys::PLATFORM, &rec.name, s)?,
        None => Zeroizing::new(String::new()),
    };
    Ok(match &rec.settings {
        ProviderSettings::AnthropicMessages(c) => {
            if key.is_empty() {
                return Err(crate::error::not_configured(
                    Role::Admin,
                    format!("provider {} has no key", rec.name),
                ));
            }
            let mut c = c.clone();
            c.name.clone_from(&rec.name);
            Arc::new(AnthropicProvider::new(c, key)?)
        }
        ProviderSettings::OpenAiChat(c) => {
            let mut c = c.clone();
            c.name.clone_from(&rec.name);
            if key.is_empty() && c.key_header != KeyHeader::None {
                c.key_header = KeyHeader::None;
            }
            Arc::new(OpenAiProvider::new(c, key)?)
        }
    })
}

/// Provider records in the global tier.
#[derive(Clone)]
pub struct ProviderStore {
    store: Arc<dyn MetaStore>,
}

impl std::fmt::Debug for ProviderStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderStore").finish_non_exhaustive()
    }
}

impl ProviderStore {
    /// Records in `store`, which must be the global tier.
    ///
    /// # Errors
    /// Internal for a store of another tier.
    pub fn new(store: Arc<dyn MetaStore>) -> Result<Self> {
        if *store.tier() != Tier::Global {
            return Err(SprigError::internal("AI providers live in the global tier"));
        }
        Ok(Self { store })
    }

    fn key(name: &str) -> Result<sprig_meta::Key> {
        check_name(name)?;
        Tier::Global.key(&format!("ai/providers/{name}"))
    }

    /// Writes `rec`, replacing a record of that name.
    ///
    /// # Errors
    /// An invalid name; the store's errors.
    pub async fn put(&self, rec: &ProviderRecord) -> Result<()> {
        let key = Self::key(&rec.name)?;
        let value = Record::new(rec.clone()).encode()?;
        self.store
            .txn(Txn {
                when: Vec::new(),
                then: vec![Op::Put {
                    key,
                    value,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        Ok(())
    }

    /// The record named `name`.
    ///
    /// # Errors
    /// The store's errors; Internal for a record that doesn't decode.
    pub async fn get(&self, name: &str) -> Result<Option<ProviderRecord>> {
        let Some(kv) = self
            .store
            .get(&Self::key(name)?, Consistency::Linearizable)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(Record::<ProviderRecord>::decode(&kv.value)?.body))
    }

    /// Every record, by name. Records that don't decode are skipped and
    /// logged.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn list(&self) -> Result<Vec<ProviderRecord>> {
        let prefix = Tier::Global.key("ai/providers/")?;
        let mut out = Vec::new();
        let mut opts = ListOpts::default();
        loop {
            let page = self.store.list(&prefix, opts.clone()).await?;
            for kv in &page.kvs {
                match Record::<ProviderRecord>::decode(&kv.value) {
                    Ok(r) => out.push(r.body),
                    Err(e) => {
                        tracing::warn!(key = %kv.key, error = %e, "skipping an AI provider record");
                    }
                }
            }
            if !page.more {
                break;
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
        Ok(out)
    }

    /// Removes the record named `name`; `false` if there was none.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn remove(&self, name: &str) -> Result<bool> {
        let r = self
            .store
            .txn(Txn {
                when: Vec::new(),
                then: vec![Op::Delete(Self::key(name)?)],
                otherwise: Vec::new(),
            })
            .await?;
        Ok(matches!(
            r.results.first(),
            Some(sprig_meta::OpResult::Delete(true))
        ))
    }

    /// The provider and model for `role`, from `ai.model.<role>`'s value.
    ///
    /// # Errors
    /// 39990 when the value is empty or names no provider; the key's and
    /// the store's errors.
    pub async fn for_role(
        &self,
        role: Role,
        model_setting: &str,
        keys: &dyn AiKeyring,
    ) -> Result<(Arc<dyn Provider>, String)> {
        let rm = RoleModel::parse(role, model_setting)?;
        let rec = self.get(&rm.provider).await?.ok_or_else(|| {
            crate::error::not_configured(role, format!("there is no provider {}", rm.provider))
        })?;
        Ok((build(&rec, keys)?, rm.model))
    }
}

#[cfg(test)]
mod tests {
    use sprig_meta::MemStore;
    use sprig_workload::keyring::MasterKeyring;

    use super::*;

    #[tokio::test]
    async fn records_round_trip_and_never_show_their_key() {
        let mem: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Global));
        let store = ProviderStore::new(mem).unwrap();
        let ring = MasterKeyring::new([(1, [9u8; 32])]).unwrap();
        let rec = ProviderRecord {
            name: "anthropic".into(),
            settings: ProviderSettings::AnthropicMessages(AnthropicConfig::default()),
            key: Some(keys::seal(&ring, keys::PLATFORM, "anthropic", "sk-ant-abc").unwrap()),
            updated_ms: 1,
        };
        store.put(&rec).await.unwrap();
        let local = ProviderRecord {
            name: "ollama".into(),
            settings: ProviderSettings::OpenAiChat(OpenAiConfig {
                base_url: "http://127.0.0.1:11434/v1".into(),
                key_header: KeyHeader::None,
                ..OpenAiConfig::default()
            }),
            key: None,
            updated_ms: 2,
        };
        store.put(&local).await.unwrap();
        assert_eq!(store.get("anthropic").await.unwrap().unwrap(), rec);
        let all = store.list().await.unwrap();
        assert_eq!(all.len(), 2);
        let shown = all[0].public().to_string();
        assert!(shown.contains("\"has_key\":true"));
        assert!(!shown.contains(&rec.key.as_ref().unwrap().sealed));
        let (p, model) = store
            .for_role(Role::Dba, "anthropic/claude-opus-5-5", &ring)
            .await
            .unwrap();
        assert_eq!(p.kind(), ProviderKind::AnthropicMessages);
        assert_eq!(p.name(), "anthropic");
        assert_eq!(model, "claude-opus-5-5");
        let (p, model) = store
            .for_role(Role::Data, "ollama/qwen3:8b", &ring)
            .await
            .unwrap();
        assert_eq!(p.kind(), ProviderKind::OpenAiChat);
        assert_eq!(model, "qwen3:8b");
        let e = store.for_role(Role::Data, "", &ring).await.err().unwrap();
        assert_eq!(e.code(), Some(SprigCode::AiNotConfigured));
        let e = store
            .for_role(Role::Data, "gone/x", &ring)
            .await
            .err()
            .unwrap();
        assert_eq!(e.code(), Some(SprigCode::AiNotConfigured));
        assert!(store.remove("ollama").await.unwrap());
        assert!(!store.remove("ollama").await.unwrap());
        assert!(check_name("Bad Name").is_err());
    }
}
