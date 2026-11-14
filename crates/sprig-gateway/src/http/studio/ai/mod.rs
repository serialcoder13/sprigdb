//! The AI assistants' routes.
//!
//! | Route | Does |
//! |---|---|
//! | `POST /v1/ai/chat` | One turn `{role, message, conversation?, database?, context?}`, streamed as NDJSON ([`chat`]) |
//! | `GET /v1/ai/conversations?limit=` | The caller's conversations, newest first |
//! | `GET /v1/ai/conversations/{id}?payloads=` | One of them with its messages, including what was sent to the provider (`payloads=false` leaves the payloads out) |
//! | `POST /v1/ai/feedback` | `{message_id, vote: up\|down, note?}` on an answer, or `{proposal_id, outcome: accepted\|dismissed\|failed, note?}` |
//! | `GET /v1/ai/glossary?status=` | The tenant glossary (`accepted`, `proposed` or `all`) |
//! | `POST /v1/ai/glossary` | Adds or replaces an accepted term `{term, definition, sql?, tables?}` (tenant administrators) |
//! | `POST /v1/ai/glossary/{term}:accept`, `:reject`, `:delete` | Decides a proposal, or deletes a term (tenant administrators) |
//! | `POST`, `DELETE /v1/mcp` | The MCP server: the tenant assistants' tools for outside agents ([`mcp`]) |
//!
//! **Who.** Every route runs as the signed-in caller (a Studio session,
//! Basic or an API key). `dba` and `data` turns need a tenant login and
//! keep their conversations in the tenant's catalog, readable only by
//! their owner; `admin` turns need a platform operator (a login in
//! `studio.operator_logins` with operator mode on) and keep nothing but
//! the audit event. Tools run with the caller's rights ([`host`]).
//!
//! **Before a turn**, in this order: `ai.enabled` (39991), the role
//! (1227 or 39790), context that would send rows with `ai.share_data` off
//! (39995), the role's model and provider (39990), and the daily budgets
//! (39992). These are plain HTTP errors; anything later is an `error`
//! line in the stream.
//!
//! **Prompts.** Each turn's system prompt is the role's base prompt, then
//! for tenant roles the tenant's accepted glossary terms and its users'
//! recent feedback ([`sprig_ai::glossary::prompt`],
//! [`sprig_ai::feedback::prompt`]).

pub mod ask;
pub mod chat;
pub mod host;
pub mod mcp;

#[cfg(test)]
mod tests;

use std::fmt::Write as _;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use hyper::body::Incoming;
use hyper::{Request, Response};
use serde_json::{Value, json};
use sprig_ai::audit::{AuditStore, CatalogAudit, ConvId};
use sprig_ai::feedback::{self, FeedbackStore, Outcome};
use sprig_ai::glossary::{self, CatalogGlossary, GlossaryStore, NewTerm, Status};
use sprig_ai::keys::AiKeyring;
use sprig_ai::providers::ProviderStore;
use sprig_ai::settings::{Lookup, TurnSettings};
use sprig_ai::{Agent, Role, ToolRegistry, TurnConfig, TurnRequest};
use sprig_common::audit::AuditStream;
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{SprigCode, SprigError};
use sprig_meta::MetaStore;
use sprig_settings::Setting;
use tonic::body::Body;

use super::super::sql::Caller;
use super::super::{ConnInfo, Core};
use super::{StudioState, hdr, json_response, parse_json, read_body, rows_of, run_sql};

/// The longest message or context item, in bytes.
pub const MAX_MESSAGE: usize = 32 * 1024;
/// Conversations one listing returns at most.
const MAX_LIST: u32 = 200;

fn invalid(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::AdminRequestInvalid, m)
}

/// Percent-encodes a query value.
pub(crate) fn pct(s: &str) -> String {
    let mut o = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            o.push(char::from(b));
        } else {
            let _ = write!(o, "%{b:02X}");
        }
    }
    o
}

// ---- the service ----

/// Where providers come from and the keys that open them.
pub struct AiService {
    providers: ProviderStore,
    keys: Arc<dyn AiKeyring>,
}

impl std::fmt::Debug for AiService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AiService").finish_non_exhaustive()
    }
}

/// No master key file: only providers without a key (a local server) open.
struct NoKeys;

impl AiKeyring for NoKeys {
    fn current(&self, _owner: TenantId) -> Option<(u32, [u8; 32])> {
        None
    }
    fn key(&self, _owner: TenantId, _version: u32) -> Option<[u8; 32]> {
        None
    }
}

static SERVICE: OnceLock<Arc<AiService>> = OnceLock::new();

/// Makes providers in `global` usable by this process's assistants, with
/// keys opened by the master key file at `key_file` (`[gateway]
/// workload_key_file`). Once; later calls are ignored.
pub fn install(global: Arc<dyn MetaStore>, key_file: Option<&Path>) {
    let providers = match ProviderStore::new(global) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "the AI assistants have no provider store");
            return;
        }
    };
    let keys: Arc<dyn AiKeyring> = match key_file {
        Some(p) => match sprig_workload::keyring::MasterKeyring::from_file(p) {
            Ok(k) => Arc::new(k),
            Err(e) => {
                tracing::warn!(error = %e, "AI provider keys can't be opened on this gateway");
                Arc::new(NoKeys)
            }
        },
        None => Arc::new(NoKeys),
    };
    let _ = SERVICE.set(Arc::new(AiService { providers, keys }));
}

fn service() -> Result<Arc<AiService>, SprigError> {
    SERVICE.get().cloned().ok_or_else(|| {
        SprigError::new(
            SprigCode::AiNotConfigured,
            "this gateway has no AI provider store (it runs without the global tier)",
        )
    })
}

// ---- settings ----

/// The `ai.*` values for a caller: the tenant's (and database's) scopes,
/// then dc and global.
pub struct GatewayLookup<'a> {
    st: &'a StudioState,
    tenant: Option<TenantId>,
    db: Option<&'a str>,
}

impl Lookup for GatewayLookup<'_> {
    fn flag(&self, s: &'static Setting<bool>) -> bool {
        self.st.tenant_get(s, self.tenant, self.db)
    }
    fn number(&self, s: &'static Setting<u64>) -> u64 {
        self.st.tenant_get(s, self.tenant, self.db)
    }
    fn text(&self, s: &'static Setting<std::borrow::Cow<'static, str>>) -> String {
        self.st.tenant_get(s, self.tenant, self.db).to_string()
    }
}

// ---- tenant stores ----

struct Stores {
    audit: Arc<CatalogAudit>,
    glossary: Arc<CatalogGlossary>,
    tenant: TenantId,
    user: UserId,
}

async fn stores(core: &Arc<Core>, caller: &Caller) -> Result<Stores, SprigError> {
    let (Some(svc), Some(tid), Some(user)) = (
        &core.studio.tenants,
        caller.principal.tenant,
        caller.principal.user_id,
    ) else {
        return Err(invalid("the assistants' conversations need a tenant login"));
    };
    let ep = sprig_catalog::CatalogLocator::writer(&*svc.locator, tid).await?;
    let schema = ep.store.schema().to_owned();
    Ok(Stores {
        audit: Arc::new(CatalogAudit::new(ep.pool.clone(), schema.clone())),
        glossary: Arc::new(CatalogGlossary::new(ep.pool.clone(), schema)),
        tenant: tid,
        user,
    })
}

async fn tenant_admin(core: &Arc<Core>, caller: &Caller) -> Result<bool, SprigError> {
    let r = run_sql(core, caller, "-", "SHOW GRANTS".to_owned()).await?;
    let lines: Vec<String> = rows_of(&r)
        .into_iter()
        .filter_map(|row| row.into_iter().next().flatten())
        .collect();
    Ok(super::grants::Privileges::of(&lines).tenant_admin)
}

fn not_admin() -> SprigError {
    SprigError::standard(
        1227,
        *b"42000",
        "Access denied; changing the glossary needs SPRIG_TENANT_ADMIN",
    )
}

// ---- routes ----

/// Whether `path` is one of these routes.
#[must_use]
pub fn is_ai_path(path: &str) -> bool {
    path.starts_with("/v1/ai/")
}

/// Answers `/v1/ai/...`.
pub(super) async fn route(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let method = req.method().as_str().to_owned();
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or("").to_owned();
    let caller = core
        .caller(
            info,
            hdr(&req, "authorization"),
            hdr(&req, super::super::rest::TENANT_HEADER),
            hdr(&req, "host"),
        )
        .await?;
    let rest = path.trim_start_matches("/v1/ai/");
    match (method.as_str(), rest) {
        ("POST", "chat") => {
            let b = parse_json(&read_body(req).await?)?;
            chat_route(core, caller, &b).await
        }
        ("GET", "conversations") => list(core, &caller, &query).await,
        ("GET", r) if r.starts_with("conversations/") => {
            let id = r.trim_start_matches("conversations/");
            show(core, &caller, id, &query).await
        }
        ("POST", "feedback") => {
            let b = parse_json(&read_body(req).await?)?;
            feedback_route(core, &caller, &b).await
        }
        ("GET", "glossary") => glossary_list(core, &caller, &query).await,
        ("POST", "glossary") => {
            let b = parse_json(&read_body(req).await?)?;
            glossary_put(core, &caller, &b).await
        }
        ("POST", r) if r.starts_with("glossary/") => {
            let seg = super::catalog::percent_decode(r.trim_start_matches("glossary/"));
            let Some((term, verb)) = seg.rsplit_once(':') else {
                return Err(invalid("POST /v1/ai/glossary/{term}:accept|reject|delete"));
            };
            glossary_verb(core, &caller, term, verb).await
        }
        _ => Err(invalid(format!("no route {method} {path}"))),
    }
}

fn query_value<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == key).then_some(v)
    })
}

/// The text of a JSON field that may be a string or a number.
fn id_of(v: &Value, k: &str) -> Option<String> {
    match v.get(k) {
        Some(Value::String(s)) if !s.trim().is_empty() => Some(s.trim().to_owned()),
        Some(Value::Number(n)) => Some(n.to_string()),
        _ => None,
    }
}

/// The conversation a turn continues (the caller's, with the same
/// assistant), or a new one titled by the message.
async fn open_conversation(
    st: &Stores,
    role: Role,
    b: &Value,
    message: &str,
) -> Result<(ConvId, Vec<sprig_ai::Message>), SprigError> {
    let Some(id) = id_of(b, "conversation") else {
        let title = sprig_ai::egress::redact_secrets(message);
        return Ok((st.audit.create(st.user, role, &title).await?, Vec::new()));
    };
    let id = id
        .parse::<u64>()
        .map_err(|_| invalid("conversation is a conversation id"))?;
    let c = st
        .audit
        .get(st.user, ConvId(id))
        .await?
        .ok_or_else(|| invalid(format!("you have no conversation {id}")))?;
    if c.role != role {
        return Err(invalid(format!(
            "conversation {id} is with the {} assistant",
            c.role
        )));
    }
    let msgs = st.audit.messages(c.id).await?;
    Ok((c.id, sprig_ai::audit::history(&msgs)))
}

/// What a turn starts from: its store, glossary, conversation and history.
type Prepared = (
    Arc<dyn AuditStore>,
    Option<Arc<dyn GlossaryStore>>,
    Option<ConvId>,
    Vec<sprig_ai::Message>,
);

#[allow(clippy::too_many_lines)] // the checks in the module docs' order
async fn chat_route(
    core: &Arc<Core>,
    caller: Caller,
    b: &Value,
) -> Result<Response<Body>, SprigError> {
    let asked = b.get("role").and_then(Value::as_str).unwrap_or("data");
    if !matches!(asked.trim(), "data" | "dba" | "admin") {
        return Err(invalid("role is data, dba or admin"));
    }
    let message = b
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_owned();
    if message.is_empty() || message.len() > MAX_MESSAGE {
        return Err(invalid(format!("message is 1 to {MAX_MESSAGE} bytes")));
    }
    let database = b
        .get("database")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|d| !d.is_empty() && *d != "-")
        .map(str::to_owned);
    let tenant_login = caller.principal.tenant.is_some() && caller.principal.user_id.is_some();
    let tenant = if asked.trim() == "admin" {
        None
    } else {
        caller.principal.tenant
    };
    let settings = TurnSettings::read(
        &GatewayLookup {
            st: &core.studio,
            tenant,
            db: database.as_deref(),
        },
        Role::parse(asked.trim()).unwrap_or(Role::Data),
    );
    // 1. Off for the tenant: before anything else.
    if !settings.enabled {
        return Err(sprig_ai::error::disabled());
    }
    // 2. The role.
    let operator = !super::ops::roles(core, &caller).is_empty();
    let role = chat::role_for(asked, tenant_login, operator)?;
    // 3. Context that would send rows.
    let context = chat::context_text(b.get("context"), settings.share_data)?;
    // 4. The model.
    let svc = service()?;
    let (provider, model) = svc
        .providers
        .for_role(role, &settings.model, &*svc.keys)
        .await?;
    let user = caller.principal.user_id.unwrap_or(UserId(0));
    let mut system = sprig_ai::prompt::base(role);
    let (audit, glossary, conv, history): Prepared = if role.is_tenant() {
        let st = stores(core, &caller).await?;
        // 5. The budgets, before the stream starts.
        let spent = st.audit.usage_today(st.user).await?;
        settings.budget.check(spent, 0)?;
        let (conv, history) = open_conversation(&st, role, b, &message).await?;
        if let Ok(terms) = st.glossary.list(Some(Status::Accepted)).await
            && let Some(p) = glossary::prompt(&terms)
        {
            system.push(sprig_ai::Block::text(p));
        }
        if let Ok(notes) = st.audit.recent(feedback::PROMPT_NOTES).await
            && let Some(p) = feedback::prompt(&notes)
        {
            system.push(sprig_ai::Block::text(p));
        }
        (
            st.audit as Arc<dyn AuditStore>,
            Some(st.glossary as Arc<dyn GlossaryStore>),
            Some(conv),
            history,
        )
    } else {
        (
            Arc::new(sprig_ai::MemoryAudit::default()) as Arc<dyn AuditStore>,
            None,
            None,
            Vec::new(),
        )
    };
    let cfg = TurnConfig::from_settings(role, &settings, model);
    let host = Arc::new(host::GatewayHost::new(
        Arc::clone(core),
        caller.clone(),
        settings.share_data,
    ));
    let turn = chat::Turn {
        agent: Agent::new(provider, ToolRegistry::builtin(), audit),
        cfg,
        req: TurnRequest {
            tenant: if role.is_tenant() {
                caller.principal.tenant
            } else {
                None
            },
            user,
            conv,
            system,
            history,
            message: format!("{message}{context}"),
            host,
            scrubber: None,
            cancel: None,
        },
        glossary,
        author: caller.principal.user.clone(),
    };
    let (rx, cancel) = chat::start(turn, conv);
    Ok(chat::response(rx, cancel))
}

async fn list(
    core: &Arc<Core>,
    caller: &Caller,
    query: &str,
) -> Result<Response<Body>, SprigError> {
    let st = stores(core, caller).await?;
    let limit = query_value(query, "limit")
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(50)
        .clamp(1, MAX_LIST);
    let convs = st.audit.list(st.user, limit).await?;
    Ok(json_response(
        200,
        &json!({"conversations": convs.into_iter().map(|c| json!({
            "id": c.id.0.to_string(), "role": c.role, "title": c.title,
            "created_ms": c.created_ms, "updated_ms": c.updated_ms,
        })).collect::<Vec<_>>()}),
    ))
}

fn hex(h: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in h {
        let _ = write!(s, "{b:02x}");
    }
    s
}

async fn show(
    core: &Arc<Core>,
    caller: &Caller,
    id: &str,
    query: &str,
) -> Result<Response<Body>, SprigError> {
    let st = stores(core, caller).await?;
    let id: u64 = id
        .parse()
        .map_err(|_| invalid("conversations/{id} takes a conversation id"))?;
    let c = st
        .audit
        .get(st.user, ConvId(id))
        .await?
        .ok_or_else(|| invalid(format!("you have no conversation {id}")))?;
    let payloads = query_value(query, "payloads") != Some("false");
    let msgs = st.audit.messages(c.id).await?;
    let messages: Vec<Value> = msgs
        .into_iter()
        .map(|m| {
            let mut body = m.body;
            if !payloads && let Some(o) = body.as_object_mut() {
                o.remove("payload");
            }
            json!({
                "id": m.id.to_string(), "kind": m.kind, "body": body,
                "sent_hash": m.sent_hash.as_ref().map(hex), "created_ms": m.created_ms,
            })
        })
        .collect();
    Ok(json_response(
        200,
        &json!({
            "conversation": {
                "id": c.id.0.to_string(), "role": c.role, "title": c.title,
                "created_ms": c.created_ms, "updated_ms": c.updated_ms,
            },
            "messages": messages,
        }),
    ))
}

fn proposal_kind(k: &str) -> &'static str {
    match k {
        "sql" => "sql",
        "recommendation" => "recommendation",
        "tile" => "tile",
        "glossary" => "glossary",
        "generate_rows" => "generate_rows",
        _ => "other",
    }
}

async fn feedback_route(
    core: &Arc<Core>,
    caller: &Caller,
    b: &Value,
) -> Result<Response<Body>, SprigError> {
    let st = stores(core, caller).await?;
    let note = b.get("note").and_then(Value::as_str);
    if note.is_some_and(|n| n.chars().count() > feedback::MAX_NOTE) {
        return Err(invalid(format!(
            "note is at most {} characters",
            feedback::MAX_NOTE
        )));
    }
    match (id_of(b, "message_id"), id_of(b, "proposal_id")) {
        (Some(m), None) => {
            let vote: i8 = match b.get("vote") {
                Some(Value::String(s)) if s == "up" => 1,
                Some(Value::String(s)) if s == "down" => -1,
                Some(Value::Number(n)) if n.as_i64() == Some(1) => 1,
                Some(Value::Number(n)) if n.as_i64() == Some(-1) => -1,
                _ => return Err(invalid("vote is up or down")),
            };
            let id: u64 = m
                .parse()
                .map_err(|_| invalid("message_id is a message id"))?;
            let role = st
                .audit
                .vote(st.user, id, vote, note)
                .await?
                .ok_or_else(|| {
                    invalid(format!(
                        "message {id} isn't an answer in your conversations"
                    ))
                })?;
            sprig_ai::metrics::feedback(role, if vote > 0 { "up" } else { "down" });
        }
        (None, Some(p)) => {
            let outcome = b
                .get("outcome")
                .and_then(Value::as_str)
                .and_then(Outcome::parse)
                .ok_or_else(|| invalid("outcome is accepted, dismissed or failed"))?;
            let (_, kind) = st
                .audit
                .outcome(st.user, &p, outcome, note)
                .await?
                .ok_or_else(|| invalid(format!("proposal {p} isn't in your conversations")))?;
            sprig_ai::metrics::proposal(proposal_kind(&kind), outcome.as_str());
        }
        _ => {
            return Err(invalid(
                "send {message_id, vote} or {proposal_id, outcome}, with an optional note",
            ));
        }
    }
    Ok(json_response(200, &json!({"recorded": true})))
}

async fn glossary_list(
    core: &Arc<Core>,
    caller: &Caller,
    query: &str,
) -> Result<Response<Body>, SprigError> {
    let st = stores(core, caller).await?;
    let status = match query_value(query, "status").unwrap_or("all") {
        "all" => None,
        s => Some(Status::parse(s).ok_or_else(|| invalid("status is accepted, proposed or all"))?),
    };
    let terms = st.glossary.list(status).await?;
    let can_edit = tenant_admin(core, caller).await.unwrap_or(false);
    Ok(json_response(
        200,
        &json!({"terms": terms, "can_edit": can_edit}),
    ))
}

fn audit_glossary(tenant: TenantId, caller: &Caller, action: &str, term: &str) {
    sprig_common::audit::record(
        AuditStream::Tenant(tenant),
        "ai.glossary",
        json!({"user": caller.principal.user, "action": action, "term": term}),
    );
}

async fn glossary_put(
    core: &Arc<Core>,
    caller: &Caller,
    b: &Value,
) -> Result<Response<Body>, SprigError> {
    let st = stores(core, caller).await?;
    if !tenant_admin(core, caller).await? {
        return Err(not_admin());
    }
    let t = NewTerm::from_json(b)?;
    st.glossary.put(&t, st.user, &caller.principal.user).await?;
    audit_glossary(st.tenant, caller, "put", &t.term);
    Ok(json_response(
        200,
        &json!({"term": t.term, "status": "accepted"}),
    ))
}

async fn glossary_verb(
    core: &Arc<Core>,
    caller: &Caller,
    term: &str,
    verb: &str,
) -> Result<Response<Body>, SprigError> {
    let st = stores(core, caller).await?;
    if !tenant_admin(core, caller).await? {
        return Err(not_admin());
    }
    let done = match verb {
        "accept" => st.glossary.accept(term, st.user).await?,
        "reject" => st.glossary.reject(term).await?,
        "delete" => st.glossary.delete(term).await?,
        _ => {
            return Err(invalid(
                "the glossary's verbs are accept, reject and delete",
            ));
        }
    };
    if !done {
        return Err(invalid(match verb {
            "delete" => format!("the glossary has no term {term}"),
            _ => format!("there is no proposal for {term}"),
        }));
    }
    audit_glossary(st.tenant, caller, verb, term);
    Ok(json_response(200, &json!({"term": term, "done": verb})))
}
