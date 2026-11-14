//! Studio on the HTTP listeners and the API additions it needs.
//!
//! The HTTP front end hands every non-gRPC request to [`handle`] first; it
//! answers:
//!
//! | Request | Does |
//! |---|---|
//! | `GET`/`HEAD` under `studio.path`, `POST <path>/_/errors` | Studio's files ([`sprig_studio::serve()`]) |
//! | `POST /v1/auth/login` | Password login: an access token, and the refresh token as an `HttpOnly` cookie |
//! | `POST /v1/auth/refresh`, `/v1/auth/logout` | Rotate or end the session (cookie, `X-Sprig-CSRF`, same origin) |
//! | `POST /v1/auth/mfa`, `/v1/auth/factors...`, `POST /v1/auth/impersonate` | Second factors and impersonation ([`mfa`]) |
//! | `GET /v1/auth/me` | `Auth.WhoAmI`: the principal, the session, the tenant tree, grants, `studio.*` |
//! | `POST /v1/exports:ticket`, `POST /v1/exports` | `Exports.CreateTicket`; the form post that streams the file |
//! | `POST /v1/users:setPassword` | `Users.SetPassword`, so a password never enters statement text |
//! | `GET /v1/users`, `GET`/`POST /v1/api-keys`, `POST /v1/api-keys/{id}:revoke` | Accounts and API keys ([`accounts`]) |
//! | `/v1/indexing/...` | The caller's tenant's automatic indexes, recommendations, actions and explanations |
//! | `POST /v1/databases/{db}/cursors`, `POST /v1/cursors/{h}:fetch`, `:close` | `Cursors` ([`cursors`]) |
//! | `/v1/ops/...` | Operator mode: the Admin API for logins in `studio.operator_logins` ([`ops`]) |
//! | `/v1/library`, `/v1/library/saved[:delete]`, `/v1/library/history[:clear]` | The caller's saved queries and history ([`library`]) |
//! | `GET /v1/shards`, `/v1/shards/advisor[/{run}]` | The shards view: table groups, keys, maps, advisor runs ([`shards`]) |
//! | `GET /v1/openapi.json` | The data API's document with these routes added |
//!
//! Anything else goes on to the REST router. Every call runs with the
//! caller's own credential and rights: statements run through the same
//! [`SqlEngine`](super::sql::SqlEngine) path as `Sql.Execute`, and the
//! indexing routes are limited to the caller's own tenant and, for
//! actions, to users who could create an index there.
//!
//! **REST only.** These routes are dispatched ahead of the proto route
//! table and described in a hand-written `OpenAPI` fragment
//! ([`sprig_studio::api`]); they have no gRPC forms yet. Token sessions
//! are signed tokens ([`session`]): any gateway accepts them.

pub mod accounts;
pub mod catalog;
pub mod cursors;
pub mod exports;
pub mod grants;
pub mod library;
// The logic editors.
pub mod logic;
// The stream, queue and sink views.
pub mod messaging;
pub mod mfa;
pub mod ops;
// The row-level security tester.
pub mod policies;
// The approvals inbox.
pub mod recommendations;
pub mod session;
pub mod shards;
pub mod workload;

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::header::{self, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use serde_json::{Value, json};
use sprig_api::proto::v1::ExecuteResponse;
use sprig_auth::token::{CatalogSessions, KeyRing, SessionStore, Tokens};
use sprig_auth::{LoginService, LoginSource};
use sprig_common::ids::DcId;
use sprig_common::{SprigCode, SprigError};
use sprig_meta::{ListOpts, MetaStore, Record, Versioned};
use sprig_settings::{Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx};
use sprig_studio::{AssetStore, StudioConfig, StudioRequest};
use tokio::sync::mpsc;
use tonic::body::Body;

use self::session::{ACCESS_PREFIX, EndReason, Limits, MemorySessions, Verified};
use super::rest::error_response;
use super::sql::{CHANNEL_DEPTH, Caller, Caps, SqlCall, collect};
use super::{ConnInfo, Core};

/// `auth.token_ttl_seconds`: access-token lifetime.
pub static TOKEN_TTL_SECONDS: Setting<u64> = Setting {
    key: "auth.token_ttl_seconds",
    default: 900,
    scopes: Scopes::TREE,
    hard: HardBounds::range(60, 86_400),
    apply: Apply::Live,
    operator_only: false,
    owner: 14,
    doc: "Access-token lifetime",
};
sprig_settings::register!(TOKEN_TTL_SECONDS);

/// `auth.refresh_ttl_hours`: token-session lifetime.
pub static REFRESH_TTL_HOURS: Setting<u64> = Setting {
    key: "auth.refresh_ttl_hours",
    default: 168,
    scopes: Scopes::TREE,
    hard: HardBounds::range(1, 8760),
    apply: Apply::Live,
    operator_only: false,
    owner: 14,
    doc: "Token-session lifetime",
};
sprig_settings::register!(REFRESH_TTL_HOURS);

/// `auth.token_key_rotation_days`: token signing-key rotation. Each
/// gateway rotates its own key.
pub static TOKEN_KEY_ROTATION_DAYS: Setting<u64> = Setting {
    key: "auth.token_key_rotation_days",
    default: 30,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(1, 365),
    apply: Apply::Live,
    operator_only: true,
    owner: 14,
    doc: "Signing-key rotation",
};
sprig_settings::register!(TOKEN_KEY_ROTATION_DAYS);

/// The refresh cookie over TLS (`Secure`, so the `__Secure-` prefix
/// applies).
pub const COOKIE_SECURE: &str = "__Secure-sprig-refresh";
/// The refresh cookie on plain HTTP: loopback, or another host only with
/// `studio.allow_insecure_http`.
pub const COOKIE_PLAIN: &str = "sprig-refresh";
/// The header every cookie request must carry (forces a CORS preflight).
pub const CSRF_HEADER: &str = "x-sprig-csrf";
/// Largest body these routes read (a login, a ticket request).
const MAX_BODY: usize = 1 << 20;

/// Where Studio's settings come from.
#[derive(Clone)]
pub enum StudioSource {
    /// Fixed values (tests).
    Fixed(Box<StudioConfig>, Limits),
    /// The live settings mirror at this DC's process scopes.
    Live {
        /// The mirror.
        live: LiveSettings,
        /// This gateway's DC.
        dc: DcId,
    },
}

/// What Studio's routes share.
pub struct StudioState {
    /// Token sessions: signing, verifying, refreshing.
    pub tokens: Arc<Tokens>,
    source: StudioSource,
    store: &'static AssetStore,
    /// The login service, to re-check accounts of live sessions.
    login: Option<Arc<LoginService>>,
    /// The global tier, for the tenant tree.
    global: Option<Arc<dyn MetaStore>>,
    /// Tenant services: the catalog writer for API keys.
    tenants: Option<Arc<crate::tenant::TenantServices>>,
    /// Open cursors.
    pub cursors: Arc<cursors::CursorRegistry>,
    /// Second factors in the tenant catalogs.
    factors: Option<sprig_auth::mfa::FactorStore>,
    /// The keys sealing TOTP seeds.
    factor_keys: Option<Arc<dyn sprig_auth::mfa::FactorKeys>>,
    /// The Admin API token operator mode forwards with.
    operator_token: Option<String>,
}

impl std::fmt::Debug for StudioState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StudioState")
            .field("tokens", &self.tokens)
            .finish_non_exhaustive()
    }
}

impl StudioState {
    /// Studio over `source`, with the embedded files.
    #[must_use]
    pub fn new(
        source: StudioSource,
        login: Option<Arc<LoginService>>,
        global: Option<Arc<dyn MetaStore>>,
    ) -> Self {
        Self::with_store(source, login, global, AssetStore::embedded())
    }

    /// Studio over `source` serving `store`. Sessions are in memory until
    /// [`StudioState::with_tenants`] and [`StudioState::with_gateway`].
    ///
    /// # Panics
    /// If the OS random source fails (the signing key).
    #[must_use]
    pub fn with_store(
        source: StudioSource,
        login: Option<Arc<LoginService>>,
        global: Option<Arc<dyn MetaStore>>,
        store: &'static AssetStore,
    ) -> Self {
        let dc = match &source {
            StudioSource::Live { dc, .. } => dc.clone(),
            StudioSource::Fixed(..) => DcId::new("dc-a").expect("a valid DC id"),
        };
        let keys = Arc::new(
            KeyRing::new(dc, "gateway", None, Duration::from_secs(30 * 86_400))
                .expect("a token signing key"),
        );
        Self {
            tokens: Arc::new(Tokens::new(keys, Arc::new(MemorySessions::default()))),
            source,
            store,
            login,
            global,
            tenants: None,
            cursors: Arc::default(),
            factors: None,
            factor_keys: None,
            operator_token: None,
        }
    }

    /// With the tenant services (API keys, and token sessions in the tenant
    /// catalogs when logins come from them).
    #[must_use]
    pub fn with_tenants(mut self, tenants: Option<Arc<crate::tenant::TenantServices>>) -> Self {
        if let (Some(t), Some(login)) = (&tenants, &self.login)
            && matches!(login.source(), LoginSource::Catalog(_))
        {
            let store: Arc<dyn SessionStore> =
                Arc::new(CatalogSessions::new(Arc::clone(login), t.catalog.clone()));
            self.tokens = Arc::new(Tokens::new(Arc::clone(self.tokens.keys()), store));
            self.factors = Some(sprig_auth::mfa::FactorStore::new(t.catalog.clone()));
        }
        self.tenants = tenants;
        self
    }

    /// With the Admin API token operator mode uses (`[gateway]
    /// operator_token_file`); without it, operator calls answer 503.
    #[must_use]
    pub fn with_operator_token_file(mut self, path: Option<&std::path::Path>) -> Self {
        self.operator_token = path.and_then(|p| {
            match sprig_common::secret_file::check(p, "[gateway] operator_token_file")
                .map_err(|e| std::io::Error::other(e.message))
                .and_then(|()| std::fs::read_to_string(p))
            {
                Ok(t) if !t.trim().is_empty() => {
                    // The older operator mode is deprecated.
                    tracing::warn!(
                        path = %p.display(),
                        "Studio's older operator mode ([gateway] operator_token_file with \
                         studio.operator_logins) is deprecated and will be removed in a later release: \
                         sign operators in as named operators (`sprigctl operator`) instead"
                    );
                    Some(t.trim().to_owned())
                }
                Ok(_) => None,
                Err(e) => {
                    tracing::warn!(error = %e, path = %p.display(), "operator token not loaded");
                    None
                }
            }
        });
        self
    }

    /// With the keys that seal TOTP seeds, derived from the tenant-data-key
    /// file (`[gateway] workload_key_file`); without one, second factors
    /// answer 39301.
    #[must_use]
    pub fn with_factor_keys(mut self, path: Option<&std::path::Path>) -> Self {
        self.factor_keys =
            path.and_then(
                |p| match sprig_workload::keyring::MasterKeyring::from_file(p) {
                    Ok(k) => {
                        Some(Arc::new(mfa::KeyringFactorKeys(k))
                            as Arc<dyn sprig_auth::mfa::FactorKeys>)
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "second-factor keys not loaded");
                        None
                    }
                },
            );
        self
    }

    /// This gateway's signing key, published in the global tier so every
    /// gateway verifies its tokens; a task publishes it, rotates it every
    /// `auth.token_key_rotation_days` and deletes retired keys' records.
    ///
    /// # Panics
    /// If the OS random source fails.
    #[must_use]
    pub fn with_gateway(mut self, gateway_id: &str) -> Self {
        let (dc, rotation) = match &self.source {
            StudioSource::Live { live, dc } => (
                dc.clone(),
                live.snapshot()
                    .get(&TOKEN_KEY_ROTATION_DAYS, &SettingCtx::process(dc)),
            ),
            StudioSource::Fixed(..) => (DcId::new("dc-a").expect("a valid DC id"), 30),
        };
        let keys = Arc::new(
            KeyRing::new(
                dc.clone(),
                gateway_id,
                self.global.clone(),
                Duration::from_secs(rotation * 86_400),
            )
            .expect("a token signing key"),
        );
        let store = self.tokens.store();
        self.tokens = Arc::new(Tokens::new(Arc::clone(&keys), store));
        if let Ok(h) = tokio::runtime::Handle::try_current() {
            let weak = Arc::downgrade(&keys);
            let source = self.source.clone();
            h.spawn(key_task(weak, source));
        }
        self
    }

    /// The `studio.*` values now.
    #[must_use]
    pub fn config(&self) -> StudioConfig {
        match &self.source {
            StudioSource::Fixed(c, _) => (**c).clone(),
            StudioSource::Live { live, dc } => StudioConfig::resolve(live, dc),
        }
    }

    /// The session limits now.
    #[must_use]
    pub fn limits(&self) -> Limits {
        match &self.source {
            StudioSource::Fixed(_, l) => *l,
            StudioSource::Live { live, dc } => {
                let snap = live.snapshot();
                let ctx = SettingCtx::process(dc);
                let c = StudioConfig::resolve(live, dc);
                let max_h = c.session_max_hours.min(snap.get(&REFRESH_TTL_HOURS, &ctx));
                Limits {
                    idle: Duration::from_secs(c.session_idle_minutes * 60),
                    max_age: Duration::from_secs(max_h * 3600),
                    access_ttl: Duration::from_secs(snap.get(&TOKEN_TTL_SECONDS, &ctx)),
                }
            }
        }
    }

    /// The session limits for `tenant`'s users: `studio.session_*` and
    /// `auth.token_ttl_seconds`, `auth.refresh_ttl_hours` resolved through
    /// the tenant tree.
    #[must_use]
    pub fn limits_for(&self, tenant: Option<sprig_common::ids::TenantId>) -> Limits {
        if let StudioSource::Fixed(_, l) = &self.source {
            return *l;
        }
        let idle = self.tenant_get(&sprig_studio::settings::SESSION_IDLE_MINUTES, tenant, None);
        let max_h = self
            .tenant_get(&sprig_studio::settings::SESSION_MAX_HOURS, tenant, None)
            .min(self.tenant_get(&REFRESH_TTL_HOURS, tenant, None));
        Limits {
            idle: Duration::from_secs(idle * 60),
            max_age: Duration::from_secs(max_h * 3600),
            access_ttl: Duration::from_secs(self.tenant_get(&TOKEN_TTL_SECONDS, tenant, None)),
        }
    }

    /// [`StudioState::limits_for`] the tenant a refresh token names.
    async fn limits_for_refresh(&self, token: &str) -> Limits {
        let name = sprig_auth::token::parse_refresh(token).map(|(t, _)| t.to_owned());
        let tid = match (name, &self.login) {
            (Some(n), Some(login)) => match login.source() {
                LoginSource::Catalog(cl) => {
                    cl.tenants().lookup(&n).await.ok().flatten().map(|t| t.id)
                }
                LoginSource::File(_) => None,
            },
            _ => None,
        };
        self.limits_for(tid)
    }

    /// The `studio.*` values for the browser (`WhoAmI`'s `studio`): this
    /// gateway's for the process keys, `tenant`'s for the keys that accept
    /// tenant scope.
    #[must_use]
    pub fn client_for(
        &self,
        cfg: &StudioConfig,
        tenant: Option<sprig_common::ids::TenantId>,
    ) -> Value {
        use sprig_studio::settings as k;
        let mut v = cfg.client.clone();
        let n = |s: &'static Setting<u64>| json!(self.tenant_get(s, tenant, None));
        v["session_idle_minutes"] = n(&k::SESSION_IDLE_MINUTES);
        v["session_max_hours"] = n(&k::SESSION_MAX_HOURS);
        v["max_result_rows"] = n(&k::MAX_RESULT_ROWS);
        v["statement_timeout_ms"] = n(&k::STATEMENT_TIMEOUT_MS);
        v["import_batch_rows"] = n(&k::IMPORT_BATCH_ROWS);
        v["import_max_mb"] = n(&k::IMPORT_MAX_MB);
        v["feed_view_max_events"] = n(&k::FEED_VIEW_MAX_EVENTS);
        v["graph_max_elements"] = n(&k::GRAPH_MAX_ELEMENTS);
        v["persist_drafts"] = json!(self.tenant_get(&k::PERSIST_DRAFTS, tenant, None));
        v["history_max"] = n(&k::HISTORY_MAX);
        v["confirm_writes"] = json!(self.tenant_get(&k::CONFIRM_WRITES, tenant, None).as_ref());
        v["view_as"] = json!(self.tenant_get(&k::VIEW_AS, tenant, None).as_ref());
        v
    }

    /// A `studio.*` value for `tenant` (and its database `db`), from the
    /// tenant's catalog settings and its ancestors', then dc and global. Falls
    /// back to this gateway's scopes when the tenant isn't in this gateway's
    /// cache.
    pub fn tenant_get<T: sprig_settings::SettingValue>(
        &self,
        setting: &'static Setting<T>,
        tenant: Option<sprig_common::ids::TenantId>,
        db: Option<&str>,
    ) -> T {
        if let (Some(svc), Some(tid)) = (&self.tenants, tenant)
            && let Some(cat) = svc.cache.cached(tid)
        {
            let mut ctx = cat.setting_ctx(&svc.dc);
            if let Some(d) = db
                .filter(|d| !d.is_empty() && *d != "-")
                .and_then(|d| cat.database(&d.to_lowercase()).or_else(|| cat.database(d)))
            {
                ctx = ctx.with_db(d.db_id);
            }
            return svc.settings.get(setting, &ctx, Some(&cat.settings));
        }
        match &self.source {
            StudioSource::Live { live, dc } => {
                live.snapshot().get(setting, &SettingCtx::process(dc))
            }
            StudioSource::Fixed(..) => setting.default.clone(),
        }
    }

    /// The caps a call gets: what it asked for, and for a Studio session no
    /// more than `studio.max_result_rows`, `studio.max_result_mb` and
    /// `studio.statement_timeout_ms` at the tenant's and database's scopes,
    /// so a modified browser gains nothing.
    #[must_use]
    pub fn caps_for(&self, caller: &Caller, database: &str, asked: Caps) -> Caps {
        if session_client(caller.authorization.as_deref()).as_deref() != Some("studio") {
            return asked;
        }
        let t = caller.principal.tenant;
        let rows = self.tenant_get(&sprig_studio::settings::MAX_RESULT_ROWS, t, Some(database));
        let mb = self.tenant_get(&sprig_studio::settings::MAX_RESULT_MB, t, None);
        let ms = self.tenant_get(
            &sprig_studio::settings::STATEMENT_TIMEOUT_MS,
            t,
            Some(database),
        );
        Caps {
            max_rows: Caps::clamp(asked.max_rows, rows),
            max_bytes: Caps::clamp(asked.max_bytes, mb.saturating_mul(1 << 20)),
            timeout: Some(Duration::from_millis(Caps::clamp(
                asked
                    .timeout
                    .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
                ms,
            ))),
        }
    }

    /// The embedded files.
    #[must_use]
    pub fn store(&self) -> &'static AssetStore {
        self.store
    }

    /// The session of an access token: signature (any gateway's key),
    /// expiry, the session live and the account allowed now.
    ///
    /// # Errors
    /// 1045; 3118, 3955 and 39303 for a locked account or a suspended
    /// tenant; 39301.
    pub async fn verify(
        &self,
        token: &str,
        client_ip: std::net::IpAddr,
        tls: bool,
    ) -> Result<Verified, SprigError> {
        self.tokens
            .verify(token, client_ip, tls, SystemTime::now())
            .await
    }
}

/// Publishes this gateway's signing key (retrying until the global tier
/// takes it), then checks every hour for rotation and every six hours for
/// retired keys of any gateway. Ends when the gateway drops its keys.
async fn key_task(keys: std::sync::Weak<KeyRing>, source: StudioSource) {
    let mut published = false;
    let mut ticks: u64 = 0;
    loop {
        let Some(k) = keys.upgrade() else { return };
        if published {
            let days = match &source {
                StudioSource::Live { live, dc } => live
                    .snapshot()
                    .get(&TOKEN_KEY_ROTATION_DAYS, &SettingCtx::process(dc)),
                StudioSource::Fixed(..) => 30,
            };
            let now = SystemTime::now();
            if let Err(e) = k
                .rotate_if_due(Duration::from_secs(days * 86_400), now)
                .await
            {
                tracing::warn!(error = %e, "token signing key rotation failed; retried in an hour");
            }
            if ticks.is_multiple_of(6) {
                match k.sweep(now).await {
                    Ok(n) if n > 0 => tracing::info!(n, "retired token signing keys deleted"),
                    Ok(_) => {}
                    Err(e) => tracing::debug!(error = %e, "token key sweep failed"),
                }
            }
            ticks += 1;
        } else {
            match k.publish().await {
                Ok(()) => published = true,
                Err(e) => {
                    tracing::warn!(error = %e, "token signing key not published yet; retrying");
                }
            }
        }
        drop(k);
        let wait = if published { 3600 } else { 5 };
        tokio::time::sleep(Duration::from_secs(wait)).await;
    }
}

// ---- small HTTP helpers ----

fn hdr(req: &Request<Incoming>, name: &str) -> Option<String> {
    req.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

fn json_response(status: u16, v: &Value) -> Response<Body> {
    let mut r = Response::new(Body::new(Full::new(Bytes::from(
        serde_json::to_vec(v).unwrap_or_default(),
    ))));
    *r.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
    let h = r.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

fn error(e: &SprigError) -> Response<Body> {
    let mut r = error_response(e);
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

fn invalid(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::AdminRequestInvalid, msg)
}

async fn read_body(req: Request<Incoming>) -> Result<Bytes, SprigError> {
    match Limited::new(req.into_body(), MAX_BODY).collect().await {
        Ok(c) => Ok(c.to_bytes()),
        Err(e) if e.is::<http_body_util::LengthLimitError>() => Err(SprigError::new(
            SprigCode::RequestTooLarge,
            "the request body is too large",
        )),
        Err(e) => Err(invalid(format!("can't read the request body: {e}"))),
    }
}

fn parse_json(body: &[u8]) -> Result<Value, SprigError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(json!({}));
    }
    serde_json::from_slice(body).map_err(|e| invalid(format!("the body isn't JSON: {e}")))
}

fn text(v: &Value, k: &str) -> String {
    v.get(k).and_then(Value::as_str).unwrap_or("").to_owned()
}

/// The token of `Authorization: Bearer <token>`.
#[must_use]
pub fn bearer(authorization: Option<&str>) -> Option<&str> {
    let (scheme, rest) = authorization?.trim().split_once(' ')?;
    scheme.eq_ignore_ascii_case("bearer").then(|| rest.trim())
}

/// The `cli` claim (`studio`, `api`) of the session token in
/// `authorization`, read without checking the signature: only for a
/// caller already verified.
#[must_use]
pub fn session_client(authorization: Option<&str>) -> Option<String> {
    let t = bearer(authorization)?.strip_prefix(ACCESS_PREFIX)?;
    let payload = t.split('.').nth(1)?;
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let v: Value = serde_json::from_slice(&raw).ok()?;
    v.get("cli").and_then(Value::as_str).map(str::to_owned)
}

/// Every claim of the session token in `authorization`, read without
/// checking the signature: only for a caller already verified. What
/// `SPRIG_CLAIM('k')` reads in that caller's statements.
#[must_use]
pub fn session_claims(authorization: Option<&str>) -> Option<Value> {
    let t = bearer(authorization)?.strip_prefix(ACCESS_PREFIX)?;
    let payload = t.split('.').nth(1)?;
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    serde_json::from_slice::<Value>(&raw)
        .ok()
        .filter(Value::is_object)
}

/// The caps a REST or gRPC body asks for: `max_rows`, `max_bytes`,
/// `timeout_ms` (numbers, or strings as proto3 JSON writes 64-bit
/// integers).
#[must_use]
pub fn asked_caps(body: &Value) -> Caps {
    let n = |k: &str| -> u64 {
        match body.get(k) {
            Some(Value::Number(n)) => n.as_u64().unwrap_or(0),
            Some(Value::String(s)) => s.parse().unwrap_or(0),
            _ => 0,
        }
    };
    let ms = n("timeout_ms");
    Caps {
        max_rows: n("max_rows"),
        max_bytes: n("max_bytes"),
        timeout: (ms > 0).then(|| Duration::from_millis(ms)),
    }
}

/// Whether `authorization` carries a Studio access token.
#[must_use]
pub fn is_session_token(authorization: Option<&str>) -> bool {
    bearer(authorization).is_some_and(|t| t.starts_with(ACCESS_PREFIX))
}

fn cookie(req: &Request<Incoming>, name: &str) -> Option<String> {
    req.headers()
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|kv| kv.trim().split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v.to_owned())
}

/// Whether the refresh cookie gets `Secure`: over TLS, and on loopback unless
/// the page itself is plain `http://`. Safari's engine drops a `Secure` cookie
/// set for an `http://127.0.0.1` page (Chrome and Firefox keep it), which
/// signed Studio out on every reload; a loopback connection never leaves the
/// machine, so the flag adds nothing there. A TLS proxy on the same host (an
/// `https://` origin) still gets `Secure`.
fn cookie_secure(info: &ConnInfo, req: &Request<Incoming>) -> bool {
    cookie_secure_for(info, hdr(req, "origin").as_deref())
}

/// [`cookie_secure`] from the request's `Origin`.
pub(super) fn cookie_secure_for(info: &ConnInfo, origin: Option<&str>) -> bool {
    info.tls.is_some()
        || (info.peer.ip().is_loopback() && !origin.is_some_and(|o| o.starts_with("http://")))
}

fn set_cookie(secure: bool, value: &str, max_age: u64) -> String {
    if secure {
        format!(
            "{COOKIE_SECURE}={value}; HttpOnly; Secure; SameSite=Strict; Path=/v1/auth; Max-Age={max_age}"
        )
    } else {
        format!(
            "{COOKIE_PLAIN}={value}; HttpOnly; SameSite=Strict; Path=/v1/auth; Max-Age={max_age}"
        )
    }
}

fn csrf_reject(reason: &'static str, e: SprigError) -> SprigError {
    metrics::counter!("sprig_studio_csrf_rejections_total", "reason" => reason).increment(1);
    e
}

/// The request comes from Studio's own origin: `Sec-Fetch-Site` is
/// `same-origin` when sent, and `Origin`, when sent, is this host's or one
/// of `studio.public_origins`.
///
/// # Errors
/// 39922.
pub fn same_origin(
    cfg: &StudioConfig,
    tls: bool,
    host: Option<&str>,
    origin: Option<&str>,
    fetch_site: Option<&str>,
) -> Result<(), SprigError> {
    if let Some(site) = fetch_site
        && !site.eq_ignore_ascii_case("same-origin")
    {
        return Err(csrf_reject(
            "fetch_site",
            SprigError::new(
                SprigCode::StudioOriginRejected,
                format!("a request from another site ({site}) can't use the session cookie"),
            ),
        ));
    }
    // A form post from a page with `Referrer-Policy: no-referrer` (Studio's)
    // carries `Origin: null`; the browser's `Sec-Fetch-Site: same-origin`
    // then vouches for it.
    let same_site = fetch_site.is_some_and(|s| s.eq_ignore_ascii_case("same-origin"));
    if let Some(o) = origin.filter(|o| !(same_site && *o == "null")) {
        let own = host.map(|h| format!("{}://{h}", if tls { "https" } else { "http" }));
        let ok = own.as_deref() == Some(o) || cfg.public_origins.iter().any(|p| p == o);
        if !ok {
            return Err(csrf_reject(
                "origin",
                SprigError::new(
                    SprigCode::StudioOriginRejected,
                    format!("origin {o} may not use the session cookie"),
                ),
            ));
        }
    }
    Ok(())
}

/// Same origin, and the `X-Sprig-CSRF` header (which a cross-site page
/// can't add without a CORS preflight that SprigDB never allows).
fn cookie_checks(
    cfg: &StudioConfig,
    info: &ConnInfo,
    req: &Request<Incoming>,
) -> Result<(), SprigError> {
    same_origin(
        cfg,
        info.tls.is_some(),
        hdr(req, "host").as_deref(),
        hdr(req, "origin").as_deref(),
        hdr(req, "sec-fetch-site").as_deref(),
    )?;
    if req.headers().get(CSRF_HEADER).is_none() {
        return Err(csrf_reject(
            "header",
            SprigError::new(
                SprigCode::StudioCsrfHeaderMissing,
                "cookie requests need the X-Sprig-CSRF header",
            ),
        ));
    }
    Ok(())
}

/// Runs one statement as `caller` and buffers the reply.
async fn run_sql(
    core: &Arc<Core>,
    caller: &Caller,
    database: &str,
    sql: String,
) -> Result<ExecuteResponse, SprigError> {
    let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);
    let engine = Arc::clone(&core.engine);
    let c2 = caller.clone();
    let call = SqlCall {
        database: database.to_owned(),
        sql,
        multi: false,
    };
    tokio::spawn(async move { engine.run(&c2, call, None, tx).await });
    collect(
        rx,
        core.engine.settings().max_buffered,
        core.engine.charge(caller),
    )
    .await
}

/// The text cells of the first result's rows.
fn rows_of(r: &ExecuteResponse) -> Vec<Vec<Option<String>>> {
    use prost_types::value::Kind;
    r.results
        .iter()
        .find(|x| !x.columns.is_empty())
        .map(|x| {
            x.rows
                .iter()
                .map(|row| {
                    row.values
                        .iter()
                        .map(|v| match &v.kind {
                            Some(Kind::StringValue(s)) => Some(s.clone()),
                            Some(Kind::NumberValue(n)) => Some(n.to_string()),
                            Some(Kind::BoolValue(b)) => Some(b.to_string()),
                            _ => None,
                        })
                        .collect()
                })
                .collect()
        })
        .unwrap_or_default()
}

// ---- dispatch ----

/// Answers Studio's requests; hands back any other request.
///
/// # Errors
/// The request, unchanged, when it isn't Studio's.
#[allow(clippy::result_large_err)] // the request goes back to the REST router
pub async fn handle(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, Request<Incoming>> {
    let path = req.uri().path().to_owned();
    let method = req.method().clone();
    let st = &core.studio;
    let cfg = st.config();
    if sprig_studio::serve::is_studio_path(&cfg, &path) {
        return Ok(assets(st, &cfg, info, req).await);
    }
    let r = match (method.as_str(), path.as_str()) {
        ("POST", "/v1/auth/login") => login(core, &cfg, info, req).await,
        ("POST", "/v1/auth/refresh") => refresh(st, &cfg, info, req).await,
        ("POST", "/v1/auth/logout") => logout(st, &cfg, info, &req).await,
        ("POST", "/v1/auth/mfa") => mfa::complete(core, &cfg, info, req).await,
        ("POST", "/v1/auth/impersonate") => mfa::impersonate(core, info, req).await,
        ("GET", "/v1/auth/factors") => mfa::factors(core, &cfg, info, req, "").await,
        ("POST", p) if p.starts_with("/v1/auth/factors:") => {
            let verb = p.trim_start_matches("/v1/auth/factors:").to_owned();
            mfa::factors(core, &cfg, info, req, &verb).await
        }
        ("GET", "/v1/auth/me") => whoami(core, &cfg, info, req).await,
        ("POST", "/v1/exports:ticket") => exports::ticket(core, &cfg, info, req).await,
        ("POST", "/v1/exports") => exports::download(core, &cfg, info, req).await,
        ("POST", "/v1/users:setPassword") => set_password(core, info, req).await,
        ("GET", "/v1/users") => accounts::list_users(core, info, req).await,
        ("GET", "/v1/api-keys") => accounts::list_keys(core, info, req).await,
        ("POST", "/v1/api-keys") => accounts::create_key(core, info, req).await,
        ("POST", p) if p.starts_with("/v1/api-keys/") && p.ends_with(":revoke") => {
            let id = p
                .trim_start_matches("/v1/api-keys/")
                .trim_end_matches(":revoke")
                .to_owned();
            accounts::revoke_key(core, info, req, &id).await
        }
        ("GET", "/v1/openapi.json") => Ok(json_response(200, &openapi())),
        (_, p) if p.starts_with("/v1/indexing/") => indexing(core, info, req).await,
        (_, p) if p.starts_with("/v1/recommendations") => {
            recommendations::handle(core, info, req).await
        }
        ("GET", p) if p.starts_with("/v1/workload/fingerprints") => {
            let query = req.uri().query().unwrap_or("").to_owned();
            let rest = p.trim_start_matches("/v1/workload/fingerprints").to_owned();
            match core
                .caller(info, hdr(&req, "authorization"), None, hdr(&req, "host"))
                .await
            {
                Err(e) => Err(e),
                Ok(caller) => match rest.trim_start_matches('/').split_once('/') {
                    None if rest.is_empty() => workload::list(core, &caller, &query).await,
                    Some((id, "series")) => workload::series(core, &caller, id, &query).await,
                    _ => Err(invalid(format!("no route GET {p}"))),
                },
            }
        }
        (_, p) if p == "/v1/ops" || p.starts_with("/v1/ops/") || p.starts_with("/v1/ops:") => {
            ops::route(core, info, req).await
        }
        (_, p) if logic::is_logic_path(p) => logic::route(core, info, req).await,
        (_, p) if messaging::is_messaging_path(p) => messaging::route(core, info, req).await,
        (_, p) if library::is_library_path(p) => library::route(core, info, req).await,
        ("POST", "/v1/policies:testAs") => policies::test_as(core, info, req).await,
        ("POST", crate::docs::http::PATH) => {
            return Ok(crate::docs::http::search(core, info, req).await);
        }
        ("GET", p) if p == "/v1/shards" || p.starts_with("/v1/shards/") => {
            match core
                .caller(info, hdr(&req, "authorization"), None, hdr(&req, "host"))
                .await
            {
                Err(e) => Err(e),
                Ok(caller) => shards::route(core, &caller, p).await,
            }
        }
        ("POST", p)
            if p.starts_with("/v1/cursors/")
                || (p.starts_with("/v1/databases/") && p.ends_with("/cursors")) =>
        {
            match cursors::route(core, info, req).await {
                Ok(r) => r,
                Err(req) => return Err(req),
            }
        }
        (_, p)
            if p == "/v1/catalog"
                || (p.starts_with("/v1/databases/")
                    && (p.contains("/catalog") || p.ends_with("/sql:check"))) =>
        {
            match catalog::route(core, info, req).await {
                Ok(r) => r,
                Err(req) => return Err(req),
            }
        }
        _ => return Err(req),
    };
    Ok(r.unwrap_or_else(|e| error(&e)))
}

/// The data API's `OpenAPI` document with Studio's routes.
#[must_use]
pub fn openapi() -> Value {
    let mut doc = sprig_api::rest::openapi(false);
    sprig_studio::api::merge_into(&mut doc);
    doc
}

async fn assets(
    st: &StudioState,
    cfg: &StudioConfig,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Response<Body> {
    let method = req.method().as_str().to_owned();
    let path = req.uri().path().to_owned();
    let ae = hdr(&req, "accept-encoding");
    let inm = hdr(&req, "if-none-match");
    let body = if method == "POST" {
        match Limited::new(req.into_body(), sprig_studio::serve::MAX_ERROR_REPORT + 1)
            .collect()
            .await
        {
            Ok(c) => c.to_bytes(),
            Err(_) => Bytes::from(vec![0u8; sprig_studio::serve::MAX_ERROR_REPORT + 1]),
        }
    } else {
        Bytes::new()
    };
    let out = sprig_studio::serve(
        st.store(),
        cfg,
        &StudioRequest {
            method: &method,
            path: &path,
            listener: info.listener,
            tls: info.tls.is_some(),
            accept_encoding: ae.as_deref(),
            if_none_match: inm.as_deref(),
            body: &body,
        },
    );
    let mut r = Response::new(Body::new(Full::new(out.body)));
    *r.status_mut() = StatusCode::from_u16(out.status).unwrap_or(StatusCode::OK);
    for (k, v) in out.headers {
        if let Ok(v) = HeaderValue::from_str(&v) {
            r.headers_mut().append(k, v);
        }
    }
    r
}

// ---- Auth ----

async fn login(
    core: &Arc<Core>,
    cfg: &StudioConfig,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let host = hdr(&req, "host");
    let origin = hdr(&req, "origin");
    let secure = cookie_secure(info, &req);
    let fetch_site = hdr(&req, "sec-fetch-site");
    let tenant = hdr(&req, super::rest::TENANT_HEADER);
    let has_csrf = req.headers().get(CSRF_HEADER).is_some();
    let body = parse_json(&read_body(req).await?)?;
    let cookie_transport = text(&body, "refresh_transport") != "body";
    // 1. A password over plain HTTP from another host: refused before it
    //    is checked (Studio refuses before sending it, too).
    let behind_tls_proxy = origin
        .as_deref()
        .is_some_and(|o| o.starts_with("https://") && cfg.public_origins.iter().any(|p| p == o));
    if info.tls.is_none()
        && !info.peer.ip().is_loopback()
        && !cfg.allow_insecure_http
        && !behind_tls_proxy
    {
        return Err(SprigError::new(
            SprigCode::StudioInsecureTransport,
            "password logins over plain HTTP are refused from other hosts: use https \
             (8443), or set studio.allow_insecure_http",
        ));
    }
    if cookie_transport {
        // Login CSRF: a foreign page must not sign the browser in.
        same_origin(
            cfg,
            info.tls.is_some(),
            host.as_deref(),
            origin.as_deref(),
            fetch_site.as_deref(),
        )?;
        if !has_csrf {
            return Err(csrf_reject(
                "header",
                SprigError::new(
                    SprigCode::StudioCsrfHeaderMissing,
                    "cookie requests need the X-Sprig-CSRF header",
                ),
            ));
        }
    }
    let login_name = text(&body, "login");
    let password = zeroize::Zeroizing::new(text(&body, "password"));
    if login_name.is_empty() || login_name.contains(':') {
        return Err(session::denied("a login name is required"));
    }
    let basic = zeroize::Zeroizing::new(format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{login_name}:{}", *password))
    ));
    let caller = core
        .caller(info, Some(basic.to_string()), tenant, host)
        .await?;
    let st = &core.studio;
    let client = if text(&body, "client") == "api" {
        "api"
    } else {
        "studio"
    };
    // A second factor, or its enrolment, comes before any token.
    if let Some(r) = mfa::after_password(core, &caller, client).await? {
        return Ok(r);
    }
    let limits = st.limits_for(caller.principal.tenant);
    let issued = st
        .tokens
        .login(caller.principal, client, limits, SystemTime::now())
        .await?;
    metrics::gauge!("sprig_studio_sessions").increment(1.0);
    tracing::info!(target: "sprig_audit", event = "studio.session_started", sid = %issued.sid,
        user = %login_name, client_ip = %info.peer.ip(), "Studio session started");
    let mut out = json!({
        "access_token": issued.access_token,
        "token_type": "Bearer",
        "expires_in": issued.expires_in,
        "session_id": issued.sid,
    });
    let mut r;
    if cookie_transport {
        r = json_response(200, &out);
        let c = set_cookie(secure, &issued.refresh_token, limits.max_age.as_secs());
        if let Ok(v) = HeaderValue::from_str(&c) {
            r.headers_mut().append(header::SET_COOKIE, v);
        }
    } else {
        out["refresh_token"] = Value::String(issued.refresh_token);
        r = json_response(200, &out);
    }
    Ok(r)
}

/// The refresh cookie, the one this transport sets first: a browser that kept
/// a `Secure` cookie set by an older gateway on an `http://` loopback page
/// then sends both, and the older one must not count as a reused token.
fn refresh_cookie(req: &Request<Incoming>, secure: bool) -> Option<String> {
    let (a, b) = if secure {
        (COOKIE_SECURE, COOKIE_PLAIN)
    } else {
        (COOKIE_PLAIN, COOKIE_SECURE)
    };
    cookie(req, a).or_else(|| cookie(req, b))
}

async fn refresh(
    st: &StudioState,
    cfg: &StudioConfig,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let secure = cookie_secure(info, &req);
    let from_cookie = refresh_cookie(&req, secure);
    let token = if let Some(c) = from_cookie.clone() {
        cookie_checks(cfg, info, &req)?;
        c
    } else {
        // API clients send the refresh token in the body.
        text(&parse_json(&read_body(req).await?)?, "refresh_token")
    };
    if token.is_empty() {
        return Err(session::denied("no refresh token; sign in"));
    }
    let limits = st.limits_for_refresh(&token).await;
    let issued = match st.tokens.refresh(&token, limits, SystemTime::now()).await {
        Ok(i) => i,
        Err(e) => {
            if let Some(end) = EndReason::of_refusal(&e) {
                end.count();
            }
            let mut r = error(&e);
            if from_cookie.is_some()
                && let Ok(v) = HeaderValue::from_str(&set_cookie(secure, "", 0))
            {
                r.headers_mut().append(header::SET_COOKIE, v);
            }
            return Ok(r);
        }
    };
    let mut out = json!({
        "access_token": issued.access_token,
        "token_type": "Bearer",
        "expires_in": issued.expires_in,
        "session_id": issued.sid,
    });
    if from_cookie.is_some() {
        let mut r = json_response(200, &out);
        let c = set_cookie(secure, &issued.refresh_token, limits.max_age.as_secs());
        if let Ok(v) = HeaderValue::from_str(&c) {
            r.headers_mut().append(header::SET_COOKIE, v);
        }
        Ok(r)
    } else {
        out["refresh_token"] = Value::String(issued.refresh_token);
        Ok(json_response(200, &out))
    }
}

async fn logout(
    st: &StudioState,
    cfg: &StudioConfig,
    info: &ConnInfo,
    req: &Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let auth = hdr(req, "authorization");
    let secure = cookie_secure(info, req);
    let now = SystemTime::now();
    let mut sid = None;
    if let Some(c) = refresh_cookie(req, secure) {
        cookie_checks(cfg, info, req)?;
        sid = st.tokens.logout_refresh(&c, now).await?;
    }
    if sid.is_none()
        && let Some(t) = bearer(auth.as_deref()).filter(|t| t.starts_with(ACCESS_PREFIX))
        && let Ok(v) = st.verify(t, info.peer.ip(), info.tls.is_some()).await
        && st.tokens.logout_access(&v.claims, now).await?
    {
        sid = Some(v.claims.sid);
    }
    if let Some(sid) = &sid {
        EndReason::Logout.count();
        metrics::gauge!("sprig_studio_sessions").decrement(1.0);
        tracing::info!(target: "sprig_audit", event = "studio.session_ended", sid = %sid,
            reason = "logout", "Studio session ended");
    }
    let mut r = json_response(200, &json!({ "ended": sid.is_some() }));
    if let Ok(v) = HeaderValue::from_str(&set_cookie(secure, "", 0)) {
        r.headers_mut().append(header::SET_COOKIE, v);
    }
    Ok(r)
}

/// The caller of a request that must carry a Studio access token (the
/// session matters: `WhoAmI`, export tickets).
async fn session_caller(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: &Request<Incoming>,
) -> Result<(Caller, Verified), SprigError> {
    let auth = hdr(req, "authorization");
    let Some(token) = bearer(auth.as_deref()).filter(|t| t.starts_with(ACCESS_PREFIX)) else {
        return Err(session::denied("sign in: this call needs a session token"));
    };
    let v = core
        .studio
        .verify(token, info.peer.ip(), info.tls.is_some())
        .await?;
    let caller = Caller {
        principal: v.principal.clone(),
        peer: info.peer,
        tls: info.tls.clone(),
        listener: info.listener,
        generation: info.generation,
        authorization: auth,
        client_dc: None,
    };
    Ok((caller, v))
}

/// `Auth.WhoAmI`.
async fn whoami(
    core: &Arc<Core>,
    cfg: &StudioConfig,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let (caller, s) = session_caller(core, info, &req).await?;
    let p = &caller.principal;
    let grants: Vec<String> = match run_sql(core, &caller, "-", "SHOW GRANTS".to_owned()).await {
        Ok(r) => rows_of(&r)
            .into_iter()
            .filter_map(|row| row.into_iter().next().flatten())
            .collect(),
        Err(e) => {
            tracing::debug!(error = %e, "WhoAmI: SHOW GRANTS failed");
            Vec::new()
        }
    };
    let privs = grants::Privileges::of(&grants);
    let tenants = tenant_tree(core, p, privs.tenant_admin).await;
    let limits = core.studio.limits_for(p.tenant);
    let out = json!({
        "principal": {
            "tenant": p.tenant_name,
            "tenant_id": p.tenant.map(|t| t.0.to_string()),
            "user": p.user,
            "host": p.host,
            "login": match &p.tenant_name {
                Some(t) => format!("{t}.{}", p.user),
                None => p.user.clone(),
            },
        },
        "amr": s.claims.amr,
        "impersonator": s.claims.act.as_ref().map(|a| a.sub.clone()),
        "session": {
            "id": s.claims.sid,
            "client": s.claims.cli,
            "created_at_ms": s.created_ms.to_string(),
            "idle_minutes": limits.idle.as_secs() / 60,
            "max_hours": limits.max_age.as_secs() / 3600,
            "access_ttl_seconds": limits.access_ttl.as_secs(),
        },
        "tenants": tenants,
        "grants": grants,
        "privileges": privs.to_json(),
        "platform_roles": ops::roles(core, &caller),
        "studio": core.studio.client_for(cfg, p.tenant),
        "gateway": {
            "id": core.engine.gateway_id,
            "dc": core.engine.dc,
            "studio_build": core.studio.store().build_version(),
        },
    });
    Ok(json_response(200, &out))
}

/// The directory fields the tenant tree reads.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct DirEntry {
    #[serde(default)]
    name: String,
    #[serde(default)]
    path: String,
}

impl Versioned for DirEntry {
    const V: u32 = 1;
}

/// The home tenant, and the descendants an administrator manages
/// (`SPRIG_TENANT_ADMIN` flows down the tree).
async fn tenant_tree(core: &Arc<Core>, p: &sprig_auth::Principal, admin: bool) -> Value {
    let home = json!({
        "name": p.tenant_name,
        "id": p.tenant.map(|t| t.0.to_string()),
        "path": p.tenant_name,
        "relation": "home",
    });
    let (Some(store), Some(me)) = (&core.studio.global, p.tenant) else {
        return json!([home]);
    };
    if !admin {
        return json!([home]);
    }
    let Ok(prefix) = store.tier().key("tenants/") else {
        return json!([home]);
    };
    let mut entries = Vec::new();
    let mut opts = ListOpts {
        at: None,
        after: None,
        limit: 500,
    };
    // A bounded walk: the switcher shows at most 5,000 tenants.
    for _ in 0..10 {
        let Ok(page) = store.list(&prefix, opts.clone()).await else {
            break;
        };
        opts.at = Some(page.revision);
        opts.after = page.kvs.last().map(|kv| kv.key.clone());
        for kv in &page.kvs {
            let id = kv.key.as_str().rsplit('/').next().unwrap_or("").to_owned();
            if let Ok(r) = Record::<DirEntry>::decode(&kv.value) {
                entries.push((id, r.body));
            }
        }
        if !page.more || opts.after.is_none() {
            break;
        }
    }
    let names: std::collections::HashMap<&str, &str> = entries
        .iter()
        .map(|(id, e)| (id.as_str(), e.name.as_str()))
        .collect();
    let me_s = me.0.to_string();
    let mut out = vec![home];
    for (id, e) in &entries {
        if *id == me_s || !e.path.contains(&format!("/{me_s}/")) {
            continue;
        }
        // The path from the home tenant down, by name.
        let below: Vec<&str> = e
            .path
            .split('/')
            .filter(|s| !s.is_empty())
            .skip_while(|s| *s != me_s)
            .map(|s| names.get(s).copied().unwrap_or(s))
            .collect();
        out.push(json!({
            "name": e.name,
            "id": id,
            "path": below.join("/"),
            "relation": "admin",
        }));
    }
    Value::Array(out)
}

// ---- Users.SetPassword ----

/// Quotes a MySQL string literal (backslash and quote escaped).
#[must_use]
pub fn quote_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\'' => out.push_str("''"),
            '\\' => out.push_str("\\\\"),
            '\0' => out.push_str("\\0"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

/// `Users.SetPassword`: `ALTER USER 'u'@'h' IDENTIFIED BY '...'` built
/// here and run as the caller, so the browser never puts a password in
/// statement text, history or drafts; workload capture redacts it as it
/// does for any client.
async fn set_password(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let auth = hdr(&req, "authorization");
    mfa::not_impersonating(auth.as_deref(), "change passwords")?;
    let host = hdr(&req, "host");
    let body = parse_json(&read_body(req).await?)?;
    let caller = core.caller(info, auth, None, host).await?;
    let user = text(&body, "user");
    let account_host = match text(&body, "host") {
        h if h.is_empty() => "%".to_owned(),
        h => h,
    };
    let password = zeroize::Zeroizing::new(text(&body, "password"));
    if user.is_empty() || password.is_empty() {
        return Err(invalid("user and password are required"));
    }
    let sql = zeroize::Zeroizing::new(format!(
        "ALTER USER {}@{} IDENTIFIED BY {}",
        quote_literal(&user),
        quote_literal(&account_host),
        quote_literal(&password)
    ));
    run_sql(core, &caller, "-", sql.to_string()).await?;
    Ok(json_response(
        200,
        &json!({ "user": user, "host": account_host, "changed": true }),
    ))
}

// ---- automatic indexing for the caller's tenant ----

/// `/v1/indexing/...` → the controller's `/admin/v1/tenants/{t}/...` for
/// the caller's own tenant. Reads need only a signed-in user (the views
/// show names and plans, not data); actions need a privilege to create
/// indexes in the index's database ([`grants::Privileges::may_index`]).
#[allow(clippy::too_many_lines)] // the route table and the privilege check
async fn indexing(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let auth = hdr(&req, "authorization");
    let host = hdr(&req, "host");
    let method = req.method().clone();
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or("").to_owned();
    let body = read_body(req).await?;
    let caller = core.caller(info, auth, None, host).await?;
    let Some(tenant) = caller.principal.tenant_name.clone() else {
        return Err(invalid("automatic indexing needs a tenant login"));
    };
    let rest = path.trim_start_matches("/v1/indexing/");
    let seg: Vec<&str> = rest.split('/').collect();
    let keep = |allowed: &[&str]| -> String {
        let kept: Vec<&str> = query
            .split('&')
            .filter(|kv| kv.split('=').next().is_some_and(|k| allowed.contains(&k)))
            .collect();
        if kept.is_empty() {
            String::new()
        } else {
            format!("?{}", kept.join("&"))
        }
    };
    let safe = |s: &str| {
        !s.is_empty()
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-' | '$'))
    };
    let base = format!("/admin/v1/tenants/{tenant}");
    let target = match (method.as_str(), seg.as_slice()) {
        ("GET", ["indexes"]) => format!("{base}/indexes{}", keep(&["db", "table", "state", "all"])),
        ("GET", ["recommendations"]) => {
            format!("{base}/recommendations{}", keep(&["db", "table"]))
        }
        ("GET", ["indexes", i]) if safe(i) => format!("{base}/indexes/{i}"),
        ("GET", ["queries", fp]) if safe(fp) && fp.ends_with(":explain") => {
            format!("{base}/queries/{fp}")
        }
        ("GET", ["decisions", id]) if safe(id) => format!("{base}/decisions/{id}"),
        ("POST", ["indexes", i]) if safe(i) => {
            let Some((index, action)) = i.rsplit_once(':') else {
                return Err(invalid("POST /v1/indexing/indexes/{index}:{action}"));
            };
            if !matches!(
                action,
                "approve"
                    | "apply"
                    | "reject"
                    | "dismiss"
                    | "pin"
                    | "freeze"
                    | "unpin"
                    | "unfreeze"
                    | "drop"
            ) {
                return Err(invalid(format!("unknown index action {action}")));
            }
            // Which database the index is in: ask, then check the
            // caller's privileges there.
            let detail = core
                .admin
                .forward_internal(
                    &Method::GET,
                    &format!("{base}/indexes/{index}"),
                    Bytes::new(),
                )
                .await?;
            if detail.status != 200 {
                return Ok(passthrough(detail.status, detail.body));
            }
            let d: Value = serde_json::from_slice(&detail.body).unwrap_or(Value::Null);
            let db = d["index"]["db"].as_str().unwrap_or("").to_owned();
            let grants = run_sql(core, &caller, "-", "SHOW GRANTS".to_owned()).await?;
            let lines: Vec<String> = rows_of(&grants)
                .into_iter()
                .filter_map(|r| r.into_iter().next().flatten())
                .collect();
            if !grants::Privileges::of(&lines).may_index(&db) {
                return Err(SprigError::standard(
                    1142,
                    *b"42000",
                    format!(
                        "INDEX command denied to user '{}' for database '{db}'",
                        caller.principal.user
                    ),
                ));
            }
            let req_body = parse_json(&body)?;
            let fwd = json!({
                "reason": text(&req_body, "reason"),
                "actor": format!("studio:{tenant}.{}", caller.principal.user),
            });
            let r = core
                .admin
                .forward_internal(
                    &Method::POST,
                    &format!("{base}/indexes/{i}"),
                    Bytes::from(fwd.to_string()),
                )
                .await?;
            return Ok(passthrough(r.status, r.body));
        }
        _ => return Err(invalid(format!("no route {method} {path}"))),
    };
    let r = core
        .admin
        .forward_internal(&Method::GET, &target, Bytes::new())
        .await?;
    Ok(passthrough(r.status, r.body))
}

fn passthrough(status: u16, body: Bytes) -> Response<Body> {
    if status == 401 {
        return error(&SprigError::new(
            SprigCode::NotLeader,
            "the controller's Admin API refused this gateway's internal credential: \
             set [gateway] controller_token_file to the controller's \
             gateway_token_file (or admin_token_file)",
        ));
    }
    let mut r = Response::new(Body::new(Full::new(body)));
    *r.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

#[cfg(test)]
mod tests;
