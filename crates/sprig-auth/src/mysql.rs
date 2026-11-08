//! MySQL login flows (chapter 14, "MySQL logins"; build step 4): the
//! [`LoginFlow`] chapter 05's auth plugins drive, over the tenant catalog.
//!
//! ```text
//! 1. resolve the tenant; match the account for (user, client_ip). If there is none,
//!    run the decoy: full-authentication shape, a dummy sha256crypt at the
//!    default cost, then 1045.
//! 2. Credentials (the plugin exchange always runs to the end):
//!    fast path: the scramble checks against the fast cache → verified
//!    full path, clear text over TLS: check primary, then secondary
//!       caching_sha2 verifiers (CPU slot)
//!    native: check the native verifier (only if the tenant enabled it)
//! 3. Policy, in MySQL's order:
//!    a. locked_until > now                → 3955 (even with correct credentials)
//!    b. credentials wrong                 → 1045
//!       (tenant suspended                 → 39303; see below)
//!    c. account_locked                    → 3118
//!    d. no TLS, and auth.require_tls or REQUIRE SSL/X509 asks for it → 1045
//!    e. password expired (flag, or lifetime passed):
//!         client has CAN_HANDLE_EXPIRED_PASSWORDS → sandbox mode; else 1862
//! 4. Success: the Principal, with the backend target.
//! ```
//!
//! How the gateway uses it: one [`LoginService`] per process (the login
//! source, the fast cache, the hashing slots); per connection
//! [`LoginService::start`] with the handshake's [`LoginRequest`], then
//! `sprig_proto_mysql::auth::authenticate(.., &mut login)`, then on
//! `Accept` [`MysqlLogin::principal`] (tenant, user, backend target,
//! sandbox flag).
//!
//! **Login sources** ([`LoginSource`]): `Catalog` resolves the tenant
//! ([`crate::account`]) and reads the account from the cached tenant
//! catalog ([`TenantAuth`] through [`AuthCatalogs`], normally the
//! gateway's `TenantCache`). `File` is the interim accounts file (decision
//! D1), kept behind the switch until the gateway is rewired; it has no
//! tenants and no policy columns.
//!
//! **Backend targets** (interim until chapter 29, M0.5): per-tenant backend
//! accounts `t<tenant>_u<user_id>` don't exist yet, so a catalog login is
//! mapped by a [`BackendResolver`] to a cluster and one configured backend
//! account per tenant ([`StaticBackends`]).
//!
//! **Decisions** where the chapter is open: suspension is reported (39303)
//! only after the credentials verified, so it doesn't reveal the tenant; a
//! catalog or directory that can't be read gives 39301 after the exchange;
//! `REQUIRE X509` needs a client certificate the gateway verified
//! ([`LoginRequest::client_cert`]); a waiting hashing slot gives 39301
//! after 2 s (chapter 14, "Rate limits"). Lockout counting and automatic
//! unlock writes are step 5: a temporary lock whose time has passed lets
//! the login through without clearing the row.

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::Path;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use serde::Deserialize;
use sprig_catalog::{Freshness, TenantCache, TenantCatalog};
use sprig_common::ids::{ClusterId, DcId, TenantId, UserId};
use sprig_common::time::{Clock, SystemClock};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::auth::{AuthPath, AuthVerdict, LoginFlow, Method, Outcome};
use sprig_proto_mysql::handshake::NONCE_LEN;
use sprig_proto_mysql::{ErrPacket, Persona};
use sprig_provision::credentials::{SecretRef, SecretSource};
use sprig_settings::{SettingCtx, Settings, Value};
use tokio::sync::Semaphore;
use zeroize::Zeroizing;

use crate::account::{TenantDirectory, TenantInfo, parse_login};
use crate::accounts::{Account, AccountStore, BackendTarget};
use crate::cache::{AuthUser, TenantAuth};
use crate::fast_cache::{FastCache, FastKey};
use crate::settings::{DEFAULT_PASSWORD_LIFETIME_DAYS, MYSQL_NATIVE_PASSWORD, REQUIRE_TLS};
use crate::store::{AccountLimits, AuthMethods, RequireTls, TempLock, UNBOUNDED_LOCK_MICROS};
use crate::verifier::{FastCacheValue, decoy_verify, verify_native_scramble, verify_scramble};

/// How long a full-path hash waits for a CPU slot before 39301.
pub const HASH_SLOT_WAIT: Duration = Duration::from_secs(2);

/// The fast-path value unknown accounts are checked against, so a probe
/// costs the same scramble check as a real account.
static DECOY_STAGE2: LazyLock<FastCacheValue> =
    LazyLock::new(|| FastCacheValue::from_password(b"sprig-decoy"));

/// What the handshake tells the login (chapter 14's `LoginRequest`, MySQL
/// fields).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginRequest {
    /// The listener's persona.
    pub persona: Persona,
    /// The user name as sent: `acme.alice` on shared endpoints, `alice` on
    /// tenant endpoints.
    pub login_name: Vec<u8>,
    /// The TLS SNI, if any.
    pub sni: Option<String>,
    /// The client address (after a PROXY header).
    pub client_ip: IpAddr,
    /// TLS at the gateway, or a trusted PROXY v2 SSL TLV.
    pub tls: bool,
    /// The client presented a certificate the gateway verified
    /// (`REQUIRE X509`).
    pub client_cert: bool,
    /// The client set `CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS`.
    pub can_handle_expired_password: bool,
}

/// Who logged in (the MySQL subset of chapter 14's `Principal`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    /// The tenant; `None` for the interim accounts file.
    pub tenant: Option<TenantId>,
    /// The tenant's name.
    pub tenant_name: Option<String>,
    /// The user's id; `None` for the accounts file.
    pub user_id: Option<UserId>,
    /// The user name (without the tenant prefix).
    pub user: String,
    /// The account's host pattern (`CURRENT_USER()`'s host); `%` for the
    /// accounts file.
    pub host: String,
    /// The client address.
    pub client_ip: IpAddr,
    /// The password expired: sandbox mode (only `ALTER USER ... IDENTIFIED
    /// BY`, `SET PASSWORD` and `SET` run; everything else 1820).
    pub must_change_password: bool,
    /// The account's resource limits.
    pub limits: AccountLimits,
    /// The auth version the login was decided at.
    pub auth_version: u64,
    /// Where the session's statements go.
    pub backend: BackendTarget,
}

// ---- dependencies of catalog logins ----

/// Tenant catalogs with their auth state, for logins.
#[async_trait]
pub trait AuthCatalogs: Send + Sync + 'static {
    /// `tenant`'s catalog, at least at the auth version the process has
    /// seen in the directory.
    ///
    /// # Errors
    /// The cache's (39070, 39072, 39073); logins then fail with 39301.
    async fn catalog(&self, tenant: TenantId) -> Result<Arc<TenantCatalog>>;
}

#[async_trait]
impl AuthCatalogs for TenantCache {
    async fn catalog(&self, tenant: TenantId) -> Result<Arc<TenantCatalog>> {
        self.get(tenant, Freshness::Latest).await
    }
}

/// Maps a logged-in catalog user to a backend target.
pub trait BackendResolver: Send + Sync + 'static {
    /// The backend target of `user` of `tenant`.
    ///
    /// # Errors
    /// 39998 if nothing is configured for the tenant.
    fn backend(&self, tenant: &TenantInfo, user: &AuthUser) -> Result<BackendTarget>;
}

/// One backend account per tenant, from configuration: the interim mapping
/// until chapter 29's per-user backend accounts (M0.5).
#[derive(Debug, Clone, Default)]
pub struct StaticBackends {
    default: Option<BackendTarget>,
    tenants: HashMap<TenantId, BackendTarget>,
}

/// A backend target as configured.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BackendSpec {
    /// A registered cluster id.
    pub cluster: String,
    /// The MariaDB account.
    pub backend_user: String,
    /// Its credential, a `credentials_ref` (`file:` or `env:`).
    pub backend_credentials: String,
}

impl BackendSpec {
    /// The target; a relative `file:` path is taken relative to `base`.
    ///
    /// # Errors
    /// 39998 for a bad cluster id, an empty user or a bad reference.
    pub fn target(&self, base: &Path) -> Result<BackendTarget> {
        let cluster = ClusterId::new(&self.cluster)
            .map_err(|e| SprigError::new(SprigCode::ConfigInvalid, e.to_string()))?;
        if self.backend_user.is_empty() {
            return Err(SprigError::new(
                SprigCode::ConfigInvalid,
                "backend_user is empty",
            ));
        }
        let mut credentials: SecretRef = self.backend_credentials.parse()?;
        if let SecretSource::File(p) = &credentials.source
            && p.is_relative()
        {
            credentials.source = SecretSource::File(base.join(p));
        }
        Ok(BackendTarget {
            cluster,
            user: self.backend_user.clone(),
            credentials,
        })
    }
}

impl StaticBackends {
    /// Every tenant goes to `default` unless given its own.
    #[must_use]
    pub fn new(default: Option<BackendTarget>) -> Self {
        Self {
            default,
            tenants: HashMap::new(),
        }
    }

    /// `tenant` goes to `target`.
    #[must_use]
    pub fn with(mut self, tenant: TenantId, target: BackendTarget) -> Self {
        self.tenants.insert(tenant, target);
        self
    }
}

impl BackendResolver for StaticBackends {
    fn backend(&self, tenant: &TenantInfo, _user: &AuthUser) -> Result<BackendTarget> {
        self.tenants
            .get(&tenant.id)
            .or(self.default.as_ref())
            .cloned()
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::ConfigInvalid,
                    format!("no backend configured for tenant {}", tenant.name),
                )
            })
    }
}

/// Resolves `auth.*` keys for a tenant.
pub trait PolicySettings: Send + Sync + 'static {
    /// The value of `key` for `catalog`'s tenant; `None` for the key's
    /// default.
    fn resolve(&self, key: &'static str, catalog: &TenantCatalog) -> Option<Value>;
}

/// Every key at its default: tests, and processes without settings.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultPolicy;

impl PolicySettings for DefaultPolicy {
    fn resolve(&self, _key: &'static str, _catalog: &TenantCatalog) -> Option<Value> {
        None
    }
}

/// Fixed values for some keys: tests.
#[derive(Debug, Clone, Default)]
pub struct FixedPolicy(pub HashMap<&'static str, Value>);

impl PolicySettings for FixedPolicy {
    fn resolve(&self, key: &'static str, _catalog: &TenantCatalog) -> Option<Value> {
        self.0.get(key).cloned()
    }
}

/// Resolution through the process's [`Settings`] in this DC: global, dc,
/// every ancestor, then the tenant (chapter 08).
#[derive(Debug, Clone)]
pub struct SettingsPolicy {
    settings: Settings,
    dc: DcId,
}

impl SettingsPolicy {
    /// Resolves with `settings`, as evaluated in `dc`.
    #[must_use]
    pub fn new(settings: Settings, dc: DcId) -> Self {
        Self { settings, dc }
    }
}

impl PolicySettings for SettingsPolicy {
    fn resolve(&self, key: &'static str, catalog: &TenantCatalog) -> Option<Value> {
        let ancestry: Vec<TenantId> = catalog.ancestry.iter().map(|a| a.id).collect();
        let ctx = SettingCtx::process(&self.dc).for_tenant(catalog.tenant, &ancestry);
        self.settings
            .resolve(key, &ctx, Some(&catalog.settings))
            .ok()
            .map(|r| r.value)
    }
}

fn policy_bool(
    p: &dyn PolicySettings,
    s: &'static sprig_settings::Setting<bool>,
    c: &TenantCatalog,
) -> bool {
    p.resolve(s.key, c)
        .and_then(|v| <bool as sprig_settings::SettingValue>::from_value(&v))
        .unwrap_or(s.default)
}

fn policy_u64(
    p: &dyn PolicySettings,
    s: &'static sprig_settings::Setting<u64>,
    c: &TenantCatalog,
) -> u64 {
    p.resolve(s.key, c)
        .and_then(|v| <u64 as sprig_settings::SettingValue>::from_value(&v))
        .unwrap_or(s.default)
}

/// Catalog-backed logins: tenant resolution, the cached tenant catalogs,
/// policy settings and backend targets.
pub struct CatalogLogins {
    tenants: Arc<dyn TenantDirectory>,
    catalogs: Arc<dyn AuthCatalogs>,
    backends: Arc<dyn BackendResolver>,
    policy: Arc<dyn PolicySettings>,
    sni_suffixes: ArcSwap<Vec<String>>,
}

impl std::fmt::Debug for CatalogLogins {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogLogins")
            .field("sni_suffixes", &self.sni_suffixes.load())
            .finish_non_exhaustive()
    }
}

impl CatalogLogins {
    /// Logins through `tenants` and `catalogs`, mapped by `backends`, with
    /// `policy` for the `auth.*` keys, and no tenant endpoints.
    #[must_use]
    pub fn new(
        tenants: Arc<dyn TenantDirectory>,
        catalogs: Arc<dyn AuthCatalogs>,
        backends: Arc<dyn BackendResolver>,
        policy: Arc<dyn PolicySettings>,
    ) -> Self {
        Self {
            tenants,
            catalogs,
            backends,
            policy,
            sni_suffixes: ArcSwap::from_pointee(Vec::new()),
        }
    }

    /// Sets `listener.tenant_sni_suffixes` (live).
    pub fn set_sni_suffixes(&self, suffixes: Vec<String>) {
        self.sni_suffixes.store(Arc::new(suffixes));
    }

    /// Steps 1 and the account match: the target of a login.
    async fn find(&self, req: &LoginRequest) -> Target {
        let Ok(name) = std::str::from_utf8(&req.login_name) else {
            return Target::Decoy;
        };
        let suffixes = self.sni_suffixes.load();
        let Some(parsed) = parse_login(
            name,
            req.sni.as_deref(),
            &suffixes,
            max_user_len(req.persona),
        ) else {
            return Target::Decoy;
        };
        let tenant = match self.tenants.lookup(&parsed.tenant).await {
            Ok(Some(t)) if t.can_log_in => t,
            Ok(_) => return Target::Decoy,
            Err(e) => return Target::Unavailable(e),
        };
        let catalog = match self.catalogs.catalog(tenant.id).await {
            Ok(c) => c,
            Err(e) => return Target::Unavailable(e),
        };
        let Some(auth) = TenantAuth::of(&catalog) else {
            tracing::error!(tenant = %tenant.name, "tenant snapshot has no auth module: is AuthExtension registered with the catalog source?");
            return Target::Unavailable(SprigError::internal("auth module not loaded"));
        };
        match auth.match_account(&parsed.user, req.client_ip) {
            Some(user) => Target::Catalog(Box::new(CatalogTarget {
                user: Arc::clone(user),
                lock: auth.temp_lock(user.row.user_id),
                tenant,
                catalog,
            })),
            None => Target::Decoy,
        }
    }
}

/// Where accounts come from: the switch between the tenant catalog and the
/// interim accounts file.
#[derive(Clone)]
pub enum LoginSource {
    /// Chapter 14: tenants, the tenant catalog, policies.
    Catalog(Arc<CatalogLogins>),
    /// Decision D1's accounts file (`FileAccounts`), until the gateway is
    /// rewired.
    File(Arc<dyn AccountStore>),
}

impl std::fmt::Debug for LoginSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoginSource::Catalog(c) => f.debug_tuple("Catalog").field(c).finish(),
            LoginSource::File(_) => f.write_str("File(..)"),
        }
    }
}

/// The configuration switch: which [`LoginSource`] a gateway builds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoginMode {
    /// The interim accounts file (the default until the gateway is
    /// rewired).
    #[default]
    AccountsFile,
    /// Tenant catalogs.
    Catalog,
}

/// What every login shares.
pub struct LoginService {
    source: LoginSource,
    fast_cache: Arc<FastCache>,
    hash_slots: Arc<Semaphore>,
    slot_wait: Duration,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for LoginService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoginService")
            .field("source", &self.source)
            .field("fast_cache", &self.fast_cache)
            .finish_non_exhaustive()
    }
}

impl LoginService {
    /// Logins from `source` with `fast_cache`; `gateway.auth_cpu_slots`
    /// at its default, CPUs / 2 (at least 1).
    #[must_use]
    pub fn new(source: LoginSource, fast_cache: Arc<FastCache>) -> Self {
        let cpus = std::thread::available_parallelism().map_or(2, std::num::NonZero::get);
        Self {
            source,
            fast_cache,
            hash_slots: Arc::new(Semaphore::new((cpus / 2).max(1))),
            slot_wait: HASH_SLOT_WAIT,
            clock: Arc::new(SystemClock),
        }
    }

    /// The same service with `slots` concurrent full-path hashes.
    #[must_use]
    pub fn with_hash_slots(mut self, slots: Arc<Semaphore>) -> Self {
        self.hash_slots = slots;
        self
    }

    /// The same service with wall-clock time from `clock` (expiry and
    /// lock checks).
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// The source.
    #[must_use]
    pub fn source(&self) -> &LoginSource {
        &self.source
    }

    /// The fast cache.
    #[must_use]
    pub fn fast_cache(&self) -> &Arc<FastCache> {
        &self.fast_cache
    }

    /// Starts one login: resolves the tenant and account (I/O on a cache
    /// miss). Never fails: an unknown tenant or account, or one that can't
    /// be loaded, runs the same exchange and is refused in
    /// [`LoginFlow::decide`].
    pub async fn start(self: &Arc<Self>, req: LoginRequest) -> MysqlLogin {
        let target = match &self.source {
            LoginSource::Catalog(c) => c.find(&req).await,
            LoginSource::File(store) => std::str::from_utf8(&req.login_name)
                .ok()
                .filter(|u| !u.is_empty() && u.chars().count() <= max_user_len(req.persona))
                .and_then(|u| store.lookup(u))
                .map_or(Target::Decoy, Target::File),
        };
        MysqlLogin {
            svc: Arc::clone(self),
            shown: String::from_utf8_lossy(&req.login_name).into_owned(),
            host: req.client_ip.to_string(),
            req,
            target,
            used_password: false,
            slot_timeout: false,
            principal: None,
        }
    }
}

/// The longest user name a persona accepts (chapter 14).
#[must_use]
pub fn max_user_len(persona: Persona) -> usize {
    match persona {
        Persona::MySql84 => 32,
        Persona::MariaDb => 80,
    }
}

struct CatalogTarget {
    tenant: TenantInfo,
    catalog: Arc<TenantCatalog>,
    user: Arc<AuthUser>,
    lock: Option<TempLock>,
}

enum Target {
    /// No tenant, no account: the decoy.
    Decoy,
    /// The tenant's credentials can't be read: 39301 after the exchange.
    Unavailable(SprigError),
    /// An interim file account.
    File(Arc<Account>),
    /// A catalog account.
    Catalog(Box<CatalogTarget>),
}

impl Target {
    fn label(&self) -> &'static str {
        match self {
            Target::Decoy => "decoy",
            Target::Unavailable(_) => "unavailable",
            Target::File(_) => "file",
            Target::Catalog(_) => "catalog",
        }
    }
}

/// One MySQL login conversation.
pub struct MysqlLogin {
    svc: Arc<LoginService>,
    req: LoginRequest,
    /// The login name as sent (lossy UTF-8), for messages.
    shown: String,
    host: String,
    target: Target,
    used_password: bool,
    slot_timeout: bool,
    principal: Option<Principal>,
}

impl std::fmt::Debug for MysqlLogin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlLogin")
            .field("login_name", &self.shown)
            .field("host", &self.host)
            .field("principal", &self.principal)
            .finish_non_exhaustive()
    }
}

impl MysqlLogin {
    /// Who logged in, once [`LoginFlow::decide`] accepted.
    #[must_use]
    pub fn principal(&self) -> Option<&Principal> {
        self.principal.as_ref()
    }

    /// The interim file account, once accepted (the gateway's M0.2 code
    /// keeps it for its backend).
    #[must_use]
    pub fn file_account(&self) -> Option<&Arc<Account>> {
        match &self.target {
            Target::File(a) if self.principal.is_some() => Some(a),
            _ => None,
        }
    }

    fn denied(&self) -> ErrPacket {
        ErrPacket::access_denied(
            self.req.persona,
            &self.shown,
            &self.host,
            self.used_password,
        )
    }

    fn native_allowed(&self) -> bool {
        match &self.target {
            Target::Catalog(c) => {
                !c.user.native.is_empty() && self.policy_bool(c, &MYSQL_NATIVE_PASSWORD)
            }
            Target::File(a) => a.native.is_some(),
            _ => false,
        }
    }

    fn policy_bool(&self, c: &CatalogTarget, s: &'static sprig_settings::Setting<bool>) -> bool {
        match &self.svc.source {
            LoginSource::Catalog(cl) => policy_bool(&*cl.policy, s, &c.catalog),
            LoginSource::File(_) => s.default,
        }
    }

    fn policy_u64(&self, c: &CatalogTarget, s: &'static sprig_settings::Setting<u64>) -> u64 {
        match &self.svc.source {
            LoginSource::Catalog(cl) => policy_u64(&*cl.policy, s, &c.catalog),
            LoginSource::File(_) => s.default,
        }
    }

    fn fast_keys(c: &CatalogTarget) -> impl Iterator<Item = FastKey> + '_ {
        c.user.caching_sha2.iter().map(|v| FastKey {
            tenant: c.tenant.id,
            user: c.user.row.user_id,
            verifier: v.id,
        })
    }

    fn refuse(&mut self, reason: &'static str, path: AuthPath, err: ErrPacket) -> AuthVerdict {
        metrics::counter!(
            "sprig_auth_logins_total",
            "protocol" => "mysql",
            "mechanism" => path.as_str(),
            "outcome" => reason,
        )
        .increment(1);
        let tenant = match &self.target {
            Target::Catalog(c) => c.tenant.name.as_str(),
            _ => "",
        };
        tracing::info!(event = "auth.login_failed", login = %self.shown, tenant, host = %self.host, path = path.as_str(), reason, source = self.target.label(), "login refused");
        AuthVerdict::Refuse(err)
    }

    fn accept(&mut self, principal: Principal, path: AuthPath) -> AuthVerdict {
        metrics::counter!(
            "sprig_auth_logins_total",
            "protocol" => "mysql",
            "mechanism" => path.as_str(),
            "outcome" => if principal.must_change_password { "sandbox" } else { "ok" },
        )
        .increment(1);
        tracing::debug!(event = "auth.login_succeeded", login = %self.shown, host = %self.host, path = path.as_str(), sandbox = principal.must_change_password, "login accepted");
        self.principal = Some(principal);
        AuthVerdict::Accept
    }

    fn decide_catalog(&mut self, outcome: Outcome, path: AuthPath) -> AuthVerdict {
        let Target::Catalog(c) = &self.target else {
            return AuthVerdict::Refuse(self.denied());
        };
        let persona = self.req.persona;
        let now = self.svc.clock.wall();
        let row = &c.user.row;
        // a. A temporary lock, even with correct credentials.
        if let Some(lock) = c.lock.filter(|l| l.locked_until > now) {
            let e = temp_locked(persona, &row.name, &row.host, &lock, now);
            return self.refuse("temp_locked", path, e);
        }
        // b. Credentials (and the password method).
        if outcome != Outcome::Verified || !row.auth_methods.contains(AuthMethods::PASSWORD) {
            let mut e = self.denied();
            if outcome == Outcome::NeedsTls {
                e = needs_tls_hint(&e);
            }
            return self.refuse("bad_credentials", path, e);
        }
        // Suspension, once the caller proved it knows the password.
        if c.tenant.suspended {
            let e = ErrPacket::from_error(
                persona,
                &SprigError::new(
                    SprigCode::TenantSuspended,
                    format!("tenant {} is suspended", c.tenant.name),
                ),
            );
            return self.refuse("suspended", path, e);
        }
        // c. ACCOUNT LOCK.
        if row.account_locked {
            let e = locked(persona, &row.name, &row.host);
            return self.refuse("locked", path, e);
        }
        // d. TLS requirements.
        let need_tls = row.require_tls != RequireTls::None || self.policy_bool(c, &REQUIRE_TLS);
        if (need_tls && !self.req.tls)
            || (row.require_tls == RequireTls::X509 && !self.req.client_cert)
        {
            let e = self.denied();
            return self.refuse("tls_required", path, e);
        }
        // e. Expiry: sandbox mode or 1862.
        let lifetime_days = match row.password_lifetime {
            Some(d) => u64::try_from(d).unwrap_or(0),
            None => self.policy_u64(c, &DEFAULT_PASSWORD_LIFETIME_DAYS),
        };
        let changed = row.password_changed_at.unwrap_or(row.created_at);
        let aged = lifetime_days > 0
            && now
                .duration_since(changed)
                .is_ok_and(|age| age >= Duration::from_secs(lifetime_days.saturating_mul(86_400)));
        let expired = row.password_expired || aged;
        if expired && !self.req.can_handle_expired_password {
            return self.refuse("expired", path, must_change(persona));
        }
        let backend = match &self.svc.source {
            LoginSource::Catalog(cl) => cl.backends.backend(&c.tenant, &c.user),
            LoginSource::File(_) => Err(SprigError::internal("catalog target from a file source")),
        };
        let backend = match backend {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(tenant = %c.tenant.name, error = %e, "no backend for a verified login");
                let pkt = ErrPacket::from_error(persona, &e);
                return self.refuse("no_backend", path, pkt);
            }
        };
        let principal = Principal {
            tenant: Some(c.tenant.id),
            tenant_name: Some(c.tenant.name.clone()),
            user_id: Some(row.user_id),
            user: row.name.clone(),
            host: row.host.clone(),
            client_ip: self.req.client_ip,
            must_change_password: expired,
            limits: row.limits,
            auth_version: c.catalog.auth_version,
            backend,
        };
        self.accept(principal, path)
    }
}

#[async_trait]
impl LoginFlow for MysqlLogin {
    fn method(&self, client_plugin: Option<&str>) -> Method {
        if client_plugin == Some(Method::ClearText.plugin_name()) {
            return Method::ClearText;
        }
        let native = self.native_allowed();
        let (has_sha2, native_plugin) = match &self.target {
            Target::Catalog(c) => (
                !c.user.caching_sha2.is_empty(),
                c.user.row.plugin == Method::Native.plugin_name(),
            ),
            Target::File(a) => (a.caching_sha2.is_some(), false),
            _ => return Method::CachingSha2,
        };
        if native
            && (!has_sha2 || native_plugin || client_plugin == Some(Method::Native.plugin_name()))
        {
            Method::Native
        } else {
            Method::CachingSha2
        }
    }

    fn check_fast(&mut self, nonce: &[u8; NONCE_LEN], scramble: &[u8]) -> bool {
        self.used_password |= !scramble.is_empty();
        let cache = &self.svc.fast_cache;
        match &self.target {
            Target::Catalog(c) => {
                Self::fast_keys(c).any(|k| cache.verify_scramble_key(&k, nonce, scramble))
            }
            Target::File(a) if a.caching_sha2.is_some() => {
                cache.verify_scramble(a, nonce, scramble)
            }
            _ => {
                let _ = verify_scramble(nonce, scramble, &DECOY_STAGE2);
                false
            }
        }
    }

    fn check_native(&mut self, nonce: &[u8; NONCE_LEN], scramble: &[u8]) -> bool {
        self.used_password |= !scramble.is_empty();
        if !self.native_allowed() {
            return false;
        }
        match &self.target {
            Target::Catalog(c) => c
                .user
                .native
                .iter()
                .any(|v| verify_native_scramble(nonce, scramble, &v.verifier)),
            Target::File(a) => a
                .native
                .as_ref()
                .is_some_and(|v| verify_native_scramble(nonce, scramble, v)),
            _ => false,
        }
    }

    async fn check_password(&mut self, password: Zeroizing<Vec<u8>>) -> bool {
        enum Work {
            Catalog(Arc<AuthUser>, TenantId, bool),
            File(Arc<Account>),
            Decoy,
        }
        self.used_password |= !password.is_empty();
        let native = self.native_allowed();
        let work = match &self.target {
            Target::Catalog(c) => Work::Catalog(Arc::clone(&c.user), c.tenant.id, native),
            Target::File(a) => Work::File(Arc::clone(a)),
            _ => Work::Decoy,
        };
        if password.is_empty() {
            // The empty password's verifier is empty: no hashing either way.
            return match &work {
                Work::Catalog(u, _, native) => {
                    u.caching_sha2.iter().any(|v| v.verifier.is_empty())
                        || (*native && u.native.iter().any(|v| v.verifier.is_empty()))
                }
                Work::File(a) => a.verify_password(&password),
                Work::Decoy => false,
            };
        }
        let slot = tokio::time::timeout(
            self.svc.slot_wait,
            Arc::clone(&self.svc.hash_slots).acquire_owned(),
        )
        .await;
        let Ok(Ok(_slot)) = slot else {
            self.slot_timeout = true;
            return false;
        };
        let cache = Arc::clone(&self.svc.fast_cache);
        tokio::task::spawn_blocking(move || match work {
            Work::Catalog(u, tenant, native) => {
                for v in &u.caching_sha2 {
                    if v.verifier.verify(&password) {
                        let key = FastKey {
                            tenant,
                            user: u.row.user_id,
                            verifier: v.id,
                        };
                        cache.fill_key(&key, &password);
                        return true;
                    }
                }
                if u.caching_sha2.is_empty() {
                    if native {
                        return u.native.iter().any(|v| v.verifier.verify(&password));
                    }
                    // No verifier this login can use: the same cost as one.
                    return decoy_verify(&password);
                }
                false
            }
            Work::File(a) => {
                let ok = a.verify_password(&password);
                if ok && a.caching_sha2.is_some() {
                    cache.fill(&a, &password);
                }
                ok
            }
            Work::Decoy => decoy_verify(&password),
        })
        .await
        .unwrap_or(false)
    }

    async fn decide(&mut self, outcome: Outcome, path: AuthPath) -> AuthVerdict {
        let persona = self.req.persona;
        if self.slot_timeout {
            let e = ErrPacket::from_error(
                persona,
                &SprigError::new(SprigCode::AuthUnavailable, "authentication is busy; retry"),
            );
            return self.refuse("rate_limited", path, e);
        }
        match &self.target {
            Target::Decoy => {
                let mut e = self.denied();
                if outcome == Outcome::NeedsTls {
                    e = needs_tls_hint(&e);
                }
                self.refuse("bad_credentials", path, e)
            }
            Target::Unavailable(err) => {
                tracing::warn!(login = %self.shown, error = %err, "login credentials unavailable");
                let e = ErrPacket::from_error(
                    persona,
                    &SprigError::new(
                        SprigCode::AuthUnavailable,
                        "authentication unavailable; retry",
                    ),
                );
                self.refuse("unavailable", path, e)
            }
            Target::File(a) => {
                if outcome == Outcome::Verified {
                    let principal = Principal {
                        tenant: None,
                        tenant_name: None,
                        user_id: None,
                        user: a.user.clone(),
                        host: "%".to_owned(),
                        client_ip: self.req.client_ip,
                        must_change_password: false,
                        limits: AccountLimits::default(),
                        auth_version: 0,
                        backend: a.backend.clone(),
                    };
                    self.accept(principal, path)
                } else {
                    let mut e = self.denied();
                    if outcome == Outcome::NeedsTls {
                        e = needs_tls_hint(&e);
                    }
                    self.refuse("bad_credentials", path, e)
                }
            }
            Target::Catalog(_) => self.decide_catalog(outcome, path),
        }
    }
}

fn needs_tls_hint(e: &ErrPacket) -> ErrPacket {
    ErrPacket::new(
        e.code,
        e.sqlstate,
        format!(
            "{}; caching_sha2_password needs TLS for a full authentication \
             (RSA key exchange is not supported): connect with TLS",
            e.message
        ),
    )
}

/// 3118 (MySQL) or 4151 (MariaDB): `ACCOUNT LOCK`.
#[must_use]
pub fn locked(persona: Persona, user: &str, host: &str) -> ErrPacket {
    match persona {
        Persona::MySql84 => ErrPacket::new(
            3118,
            *b"HY000",
            format!("Access denied for user '{user}'@'{host}'. Account is locked."),
        ),
        Persona::MariaDb => {
            ErrPacket::new(4151, *b"HY000", "Access denied, this account is locked")
        }
    }
}

/// 3955 (MySQL) or 4150 (MariaDB, `ER_USER_IS_BLOCKED`): a temporary lock
/// after failed logins.
#[must_use]
pub fn temp_locked(
    persona: Persona,
    user: &str,
    host: &str,
    lock: &TempLock,
    now: SystemTime,
) -> ErrPacket {
    match persona {
        Persona::MySql84 => {
            let unbounded = crate::store::micros(lock.locked_until) >= UNBOUNDED_LOCK_MICROS;
            let days = if unbounded {
                "unlimited".to_owned()
            } else {
                let left = lock.locked_until.duration_since(now).unwrap_or_default();
                left.as_secs().div_ceil(86_400).max(1).to_string()
            };
            ErrPacket::new(
                3955,
                *b"HY000",
                format!(
                    "Access denied for user '{user}'@'{host}'. Account is blocked for {days} day(s) \
                     ({days} day(s) remaining) due to {} consecutive failed logins.",
                    lock.consecutive_failures
                ),
            )
        }
        Persona::MariaDb => ErrPacket::new(
            4150,
            *b"HY000",
            "User is blocked because of too many credential errors; unblock with 'ALTER USER / FLUSH PRIVILEGES'",
        ),
    }
}

/// 1862: the password expired and the client can't handle sandbox mode.
#[must_use]
pub fn must_change(_persona: Persona) -> ErrPacket {
    ErrPacket::new(
        1862,
        *b"HY000",
        "Your password has expired. To log in you must change it using a client that supports expired passwords.",
    )
}

#[cfg(test)]
pub(crate) mod tests;
