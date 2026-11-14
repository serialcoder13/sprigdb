//! The AI assistants' operator side: providers in the Admin API, and the
//! purge of exact payloads past `ai.audit_payload_days`.
//!
//! | Route | Does |
//! |---|---|
//! | `GET /admin/v1/ai/providers` | Every provider, without keys (`has_key`, `key_version` only) |
//! | `POST /admin/v1/ai/providers` | Adds or replaces `{name, settings: {kind, ...}, key?}`; the key is sealed with the master key file's `ai` keys, and kept when omitted on a replace |
//! | `GET /admin/v1/ai/providers/{name}` | One provider, without its key |
//! | `DELETE /admin/v1/ai/providers/{name}` | Removes it |
//! | `POST /admin/v1/ai/providers/{name}:test` | One tiny call `{model}`: the provider answers, or the error says why not |
//!
//! Providers are global records, so changes need a platform operator (the
//! bootstrap token, or an operator with platform rights); every change is
//! audited on the platform stream, never with the key. Keys can be written
//! and never read.
//!
//! [`AiPurgeLoop`] runs on the DC leader: every hour, for each active
//! tenant homed here whose catalog has the `ai` module, payloads older
//! than the tenant's `ai.audit_payload_days` are dropped from their
//! notes, keeping each payload's SHA-256.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path as UrlPath, State};
use axum::http::HeaderMap;
use axum::response::Response;
use axum::routing::get;
use mysql_async::prelude::Queryable as _;
use serde_json::{Value, json};
use sprig_ai::keys::{self, AiKeyring};
use sprig_ai::provider::{ChatRequest, Delta, Message};
use sprig_ai::providers::{ProviderRecord, ProviderSettings, ProviderStore, check_name};
use sprig_catalog::CatalogLocator;
use sprig_catalog::cache::{Loaded, TenantCatalog};
use sprig_common::audit::AuditStream;
use sprig_common::ids::{DcId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::MetaStore;
use sprig_settings::{LiveSettings, MemSettings, Registry, Value as SettingValue};

use crate::admin::http::respond;
use crate::admin::invalid;
use crate::operators::http::principal;
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};
use crate::tree::{Directory, DirectoryEntry, TenantState};

/// How often each tenant's payloads are purged.
pub const PURGE_INTERVAL: Duration = Duration::from_secs(3600);
/// The most output a `:test` call asks for.
const TEST_TOKENS: u32 = 16;

/// The provider routes' state.
pub struct AiAdmin {
    store: ProviderStore,
    keys: Option<Arc<dyn AiKeyring>>,
}

impl std::fmt::Debug for AiAdmin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AiAdmin")
            .field("keys", &self.keys.is_some())
            .finish_non_exhaustive()
    }
}

/// No master key file: providers without a key only.
struct NoKeys;

impl AiKeyring for NoKeys {
    fn current(&self, _owner: TenantId) -> Option<(u32, [u8; 32])> {
        None
    }
    fn key(&self, _owner: TenantId, _version: u32) -> Option<[u8; 32]> {
        None
    }
}

impl AiAdmin {
    /// Providers in `global`, keys sealed and opened with `keys` (the
    /// master key file's).
    ///
    /// # Errors
    /// Internal for a store that isn't the global tier.
    pub fn new(global: Arc<dyn MetaStore>, keys: Option<Arc<dyn AiKeyring>>) -> Result<Self> {
        Ok(Self {
            store: ProviderStore::new(global)?,
            keys,
        })
    }

    fn keyring(&self) -> Arc<dyn AiKeyring> {
        self.keys
            .clone()
            .unwrap_or_else(|| Arc::new(NoKeys) as Arc<dyn AiKeyring>)
    }

    /// Every provider, as listings show them.
    ///
    /// # Errors
    /// The store's.
    pub async fn list(&self) -> Result<Value> {
        let all = self.store.list().await?;
        Ok(json!({"providers": all.iter().map(ProviderRecord::public).collect::<Vec<_>>()}))
    }

    /// One provider, without its key.
    ///
    /// # Errors
    /// 39792 when there is none; the store's.
    pub async fn show(&self, name: &str) -> Result<Value> {
        self.store
            .get(name)
            .await?
            .map(|r| r.public())
            .ok_or_else(|| invalid(format!("there is no AI provider {name}")))
    }

    /// Adds or replaces a provider from a request body.
    ///
    /// # Errors
    /// 39792 for a bad body; 39990 without a master key to seal a key
    /// with; the store's.
    pub async fn put(&self, body: &Value) -> Result<Value> {
        let name = body
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_owned();
        check_name(&name)?;
        let settings = settings_of(body.get("settings"))?;
        let old = self.store.get(&name).await?;
        let key = match body.get("key") {
            Some(Value::String(k)) if !k.trim().is_empty() => {
                let keys = self.keys.as_deref().ok_or_else(|| {
                    SprigError::new(
                        SprigCode::AiNotConfigured,
                        "provider keys are sealed with the master key file: set [controller] workload_key_file",
                    )
                })?;
                Some(keys::seal(keys, keys::PLATFORM, &name, k.trim())?)
            }
            Some(Value::Null) | None => old.and_then(|o| o.key),
            Some(_) => return Err(invalid("key is a string")),
        };
        let rec = ProviderRecord {
            name: name.clone(),
            settings,
            key,
            updated_ms: u64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
            )
            .unwrap_or(0),
        };
        // The record must make a provider before it is kept.
        sprig_ai::providers::build(&rec, &*self.keyring())
            .map_err(|e| invalid(format!("provider {name} can't be used: {}", e.message)))?;
        self.store.put(&rec).await?;
        Ok(rec.public())
    }

    /// Removes a provider.
    ///
    /// # Errors
    /// The store's.
    pub async fn remove(&self, name: &str) -> Result<Value> {
        Ok(json!({"name": name, "removed": self.store.remove(name).await?}))
    }

    /// One tiny call to `name` with `model`.
    ///
    /// # Errors
    /// 39792 for an unknown provider or no model; 39990 when the key
    /// doesn't open; 39993 when the provider fails.
    pub async fn test(&self, name: &str, body: &Value) -> Result<Value> {
        let model = body
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_owned();
        if model.is_empty() {
            return Err(invalid(
                "model names the model to try, such as claude-opus-5-5",
            ));
        }
        let rec = self
            .store
            .get(name)
            .await?
            .ok_or_else(|| invalid(format!("there is no AI provider {name}")))?;
        let p = sprig_ai::providers::build(&rec, &*self.keyring())?;
        let started = Instant::now();
        let mut text = String::new();
        let out = p
            .chat(
                ChatRequest {
                    model: model.clone(),
                    system: vec![sprig_ai::Block::text(
                        "This is a connection test. Reply with the single word OK.",
                    )],
                    messages: vec![Message::user("ping")],
                    tools: Vec::new(),
                    max_tokens: TEST_TOKENS,
                    effort: None,
                    cache_prefix: false,
                },
                &mut Collect(&mut text),
            )
            .await?;
        Ok(json!({
            "provider": name,
            "model": out.model,
            "ok": true,
            "stop": out.stop.label(),
            "text": text.chars().take(200).collect::<String>(),
            "usage": out.usage,
            "ms": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        }))
    }
}

/// Collects a test call's text.
struct Collect<'a>(&'a mut String);

impl sprig_ai::ChatSink for Collect<'_> {
    fn delta(&mut self, d: Delta) -> Result<()> {
        if let Delta::Text(t) = d {
            self.0.push_str(&t);
        }
        Ok(())
    }
}

/// A record's settings from `{kind, ...}`; `kind` is `anthropic` (or
/// `anthropic_messages`) or `openai` (or `open_ai_chat`), the rest the
/// adapter's fields, defaults for any left out.
fn settings_of(v: Option<&Value>) -> Result<ProviderSettings> {
    let mut m = match v {
        Some(Value::Object(m)) => m.clone(),
        None | Some(Value::Null) => serde_json::Map::new(),
        Some(_) => return Err(invalid("settings is an object with a kind")),
    };
    let kind = match m.get("kind").and_then(Value::as_str).unwrap_or("") {
        "anthropic" | "anthropic_messages" => "anthropic_messages",
        "openai" | "open_ai_chat" | "openai_chat" => "open_ai_chat",
        k => {
            return Err(invalid(format!(
                "settings.kind is anthropic or openai, not {k:?}"
            )));
        }
    };
    m.insert("kind".into(), json!(kind));
    serde_json::from_value(Value::Object(m)).map_err(|e| invalid(format!("settings: {e}")))
}

fn may_change(headers: &HeaderMap) -> Result<String> {
    let p = principal(headers)?;
    if !p.platform || p.is_gateway() {
        return Err(SprigError::new(
            SprigCode::OperatorForbidden,
            "AI providers are global: changing them needs platform rights",
        ));
    }
    Ok(p.name)
}

fn audited(actor: &str, action: &str, name: &str) {
    sprig_common::audit::record(
        AuditStream::Platform,
        "ai.provider",
        json!({"actor": actor, "action": action, "provider": name}),
    );
}

fn body(b: &Bytes) -> Result<Value> {
    if b.iter().all(u8::is_ascii_whitespace) {
        return Ok(json!({}));
    }
    serde_json::from_slice(b).map_err(|e| invalid(format!("request body: {e}")))
}

/// The routes.
pub fn router(ai: Arc<AiAdmin>) -> Router {
    Router::new()
        .route("/admin/v1/ai/providers", get(list).post(put))
        .route(
            "/admin/v1/ai/providers/{name}",
            get(show).delete(remove).post(method),
        )
        .with_state(ai)
}

async fn list(State(ai): State<Arc<AiAdmin>>, headers: HeaderMap) -> Response {
    respond(
        async {
            principal(&headers)?;
            ai.list().await
        }
        .await,
    )
}

async fn put(State(ai): State<Arc<AiAdmin>>, headers: HeaderMap, b: Bytes) -> Response {
    respond(
        async {
            let actor = may_change(&headers)?;
            let v = ai.put(&body(&b)?).await?;
            audited(&actor, "put", v["name"].as_str().unwrap_or_default());
            Ok::<_, SprigError>(v)
        }
        .await,
    )
}

async fn show(
    State(ai): State<Arc<AiAdmin>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    respond(
        async {
            principal(&headers)?;
            ai.show(&name).await
        }
        .await,
    )
}

async fn remove(
    State(ai): State<Arc<AiAdmin>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    respond(
        async {
            let actor = may_change(&headers)?;
            let v = ai.remove(&name).await?;
            audited(&actor, "remove", &name);
            Ok::<_, SprigError>(v)
        }
        .await,
    )
}

async fn method(
    State(ai): State<Arc<AiAdmin>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
    b: Bytes,
) -> Response {
    respond(
        async {
            let Some((name, "test")) = name.rsplit_once(':') else {
                return Err(invalid(format!(
                    "POST /admin/v1/ai/providers/{name}: the method is :test"
                )));
            };
            let actor = may_change(&headers)?;
            audited(&actor, "test", name);
            ai.test(name, &body(&b)?).await
        }
        .await,
    )
}

// ---- the purge ----

/// The payload purge (module docs).
pub struct AiPurgeLoop {
    dir: Directory,
    locator: Arc<dyn CatalogLocator>,
    dc: DcId,
    live: Option<LiveSettings>,
}

impl std::fmt::Debug for AiPurgeLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AiPurgeLoop")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

impl AiPurgeLoop {
    /// The loop over the tenants homed in `dc`.
    #[must_use]
    pub fn new(
        dir: Directory,
        locator: Arc<dyn CatalogLocator>,
        dc: DcId,
        live: Option<LiveSettings>,
    ) -> Self {
        Self {
            dir,
            locator,
            dc,
            live,
        }
    }

    fn ours(&self, e: &DirectoryEntry) -> bool {
        e.home_dc == self.dc && e.state == TenantState::Active
    }

    fn registry(&self) -> Result<&'static Registry> {
        match &self.live {
            Some(l) => Ok(l.registry()),
            None => Registry::global(),
        }
    }

    /// `ai.audit_payload_days` for the tenant of `snap`.
    fn days(&self, tenant: TenantId, snap: sprig_catalog::store::Snapshot) -> u64 {
        let key = &sprig_ai::settings::AUDIT_PAYLOAD_DAYS;
        let Ok(reg) = self.registry() else {
            return key.default;
        };
        let cat = TenantCatalog::build(reg, tenant, Loaded::core(snap));
        let empty = MemSettings::new(reg);
        let held = self.live.as_ref().map(LiveSettings::snapshot);
        let etcd = held.as_deref().unwrap_or(&empty);
        match cat
            .settings
            .resolve(etcd, key.key, &cat.setting_ctx(&self.dc))
            .map(|r| r.value)
        {
            Ok(SettingValue::Int(n)) => u64::try_from(n).unwrap_or(key.default),
            _ => key.default,
        }
    }
}

#[async_trait::async_trait]
impl Reconciler for AiPurgeLoop {
    type Key = TenantId;

    fn name(&self) -> &'static str {
        "ai_purge"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        vec![WatchSpec::global("tenants/")]
    }

    fn keys_for(&self, ev: &MetaEvent) -> Vec<TenantId> {
        self.dir.tenant_of(&ev.key).into_iter().collect()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<TenantId>> {
        Ok(self
            .dir
            .list()
            .await?
            .iter()
            .map(crate::tree::StoredEntry::body)
            .filter(|e| self.ours(e))
            .map(|e| e.tenant_id)
            .collect())
    }

    async fn reconcile(&self, _cx: &LoopCx, key: &TenantId) -> Result<Outcome> {
        let Some(entry) = self.dir.get(*key).await? else {
            return Ok(Outcome::Done);
        };
        if !self.ours(entry.body()) {
            return Ok(Outcome::Done);
        }
        let ep = self.locator.writer(*key).await?;
        let schema = ep.store.schema().to_owned();
        // Only tenants that used the assistants: the check never creates
        // the module.
        let have: Option<u64> = {
            let mut conn = ep.pool.get_conn().await.map_err(|e| {
                SprigError::new(
                    SprigCode::CatalogUnavailable,
                    "the tenant's catalog is unreachable",
                )
                .with_source(e)
            })?;
            conn.exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'ai_messages'",
                (schema.as_str(),),
            )
            .await
            .map_err(|e| {
                SprigError::new(
                    SprigCode::CatalogUnavailable,
                    "the tenant's catalog is unreachable",
                )
                .with_source(e)
            })?
        };
        if have.unwrap_or(0) == 0 {
            return Ok(Outcome::RequeueAfter(PURGE_INTERVAL));
        }
        let snap = ep.store.snapshot(&ep.pool, None).await?;
        let days = self.days(*key, snap);
        let n = sprig_ai::audit::AuditStore::purge_payloads(
            &sprig_ai::CatalogAudit::new(ep.pool.clone(), schema),
            days,
        )
        .await?;
        if n > 0 {
            sprig_ai::metrics::payloads_purged(n);
            tracing::debug!(tenant = %key, n, days, "AI payloads past their retention purged");
        }
        Ok(Outcome::RequeueAfter(PURGE_INTERVAL))
    }
}

#[cfg(test)]
mod tests {
    use sprig_meta::{MemStore, Tier};
    use sprig_workload::keyring::MasterKeyring;

    use super::*;

    #[tokio::test]
    async fn providers_keep_their_keys_sealed() {
        let mem: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Global));
        let ring: Arc<dyn AiKeyring> = Arc::new(MasterKeyring::new([(1, [7u8; 32])]).unwrap());
        let ai = AiAdmin::new(Arc::clone(&mem), Some(ring)).unwrap();
        let v = ai
            .put(&json!({"name": "anthropic", "settings": {"kind": "anthropic"}, "key": "sk-ant-secret"}))
            .await
            .unwrap();
        assert_eq!(v["has_key"], true);
        assert!(!v.to_string().contains("sk-ant-secret"));
        // A replace without a key keeps the old one.
        let v = ai
            .put(&json!({"name": "anthropic", "settings": {"kind": "anthropic", "show_thinking": false}}))
            .await
            .unwrap();
        assert_eq!(v["has_key"], true);
        let local = ai
            .put(&json!({"name": "ollama", "settings": {"kind": "openai",
                "base_url": "http://127.0.0.1:11434/v1", "key_header": "none"}}))
            .await
            .unwrap();
        assert_eq!(local["has_key"], false);
        let all = ai.list().await.unwrap();
        assert_eq!(all["providers"].as_array().unwrap().len(), 2);
        assert!(!all.to_string().contains("sk-ant-secret"));
        assert!(
            ai.put(&json!({"name": "Bad Name", "settings": {"kind": "openai"}}))
                .await
                .is_err()
        );
        assert!(
            ai.put(&json!({"name": "x", "settings": {"kind": "gemini"}}))
                .await
                .is_err()
        );
        // An Anthropic provider needs a key.
        assert!(
            ai.put(&json!({"name": "nokey", "settings": {"kind": "anthropic"}}))
                .await
                .is_err()
        );
        assert!(
            ai.test("anthropic", &json!({})).await.is_err(),
            "a model is needed"
        );
        assert_eq!(ai.remove("ollama").await.unwrap()["removed"], true);
        assert!(ai.show("ollama").await.is_err());

        // Without a master key file, keys can't be sealed.
        let bare = AiAdmin::new(mem, None).unwrap();
        let e = bare
            .put(&json!({"name": "openai", "settings": {"kind": "openai"}, "key": "sk-x"}))
            .await
            .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::AiNotConfigured));
    }
}
