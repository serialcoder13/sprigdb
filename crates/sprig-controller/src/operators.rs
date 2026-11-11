//! Named platform operators (chapter 14, "Operators"; chapter 23's global
//! tier; chapter 47's audit): the people who run the platform log in as
//! themselves, hold a role, and issue their own tokens.
//!
//! - **Directory**: `/sprig/g/operators/<name>` (account, verifier, sealed
//!   TOTP seed, recovery-code hashes), `/sprig/g/operator-tokens/<key_id>`
//!   (login sessions, issued tokens and service identities: only the
//!   secret's SHA-256) and `/sprig/g/operator-state` (when named operators
//!   took over from the bootstrap token). Every change is a compare-and-swap
//!   on the record's mod revision.
//! - **Roles**: `platform_operator` (everything) and `dc_operator` of named
//!   DCs (its DCs' controllers only; writes limited to clusters, jobs,
//!   moves, in-doubt transactions, recommendations, its DCs' drains and
//!   `dc`/`cluster` settings; see [`guard`]).
//! - **Login**: password ([`sprig_auth::operator`]), then a TOTP code or a
//!   recovery code ([`sprig_auth::mfa`]); without a factor while
//!   `operators.mfa_required` is on, an enrolment ticket. A login session is
//!   a `sprigop_` token of kind `session`, valid `operators.session_hours`.
//! - **Tokens** are issued by a person's login session: scoped
//!   (`<area>:read|write`), optionally narrowed to DCs, expiring within
//!   `operators.token_max_days`, revocable one by one. Service identities
//!   (`sprigsvc_`) are tokens of kind `service` that platform operators
//!   issue for gateways or CI.
//! - **Bootstrap**: until the first operator logs in, the controller's
//!   `admin_token_file` works as before; afterwards it is refused (39310)
//!   unless `[controller] admin_token_break_glass` is set.

pub mod guard;
pub mod http;
pub mod keys;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::UNIX_EPOCH;

use serde::{Deserialize, Serialize};
use sprig_auth::mfa::{self, FactorKeys};
use sprig_auth::operator as op;
use sprig_common::audit::{self, AuditStream};
use sprig_common::ids::{DcId, TenantId, UserId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Cmp, Consistency, Key, ListOpts, MetaStore, Op, Record, Tier, Txn, Versioned};
use sprig_settings::{Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx};

/// Global scope only.
const GLOBAL: Scopes = Scopes(0b10_0000);

macro_rules! keys {
    ($(
        $(#[$doc:meta])*
        $name:ident: $t:ty = $key:literal, $default:expr, $hard:expr, $text:literal;
    )*) => {
        $(
            $(#[$doc])*
            pub static $name: Setting<$t> = Setting {
                key: $key,
                default: $default,
                scopes: GLOBAL,
                hard: $hard,
                apply: Apply::Live,
                operator_only: true,
                owner: 14,
                doc: $text,
            };
            sprig_settings::register!($name);
        )*
    };
}

keys! {
    /// `operators.session_hours`.
    SESSION_HOURS: u32 = "operators.session_hours", 12, HardBounds::range(1, 168),
        "Lifetime of an operator's login session (sprigctl login, sprig, Studio)";
    /// `operators.token_max_days`.
    TOKEN_MAX_DAYS: u32 = "operators.token_max_days", 90, HardBounds::range(1, 3650),
        "Longest lifetime of a token an operator issues";
    /// `operators.mfa_required`.
    MFA_REQUIRED: bool = "operators.mfa_required", true, HardBounds::NONE,
        "Operators log in with a second factor (TOTP or a recovery code)";
    /// `operators.lockout_failures`.
    LOCKOUT_FAILURES: u32 = "operators.lockout_failures", 5, HardBounds::range(0, 100),
        "Failed logins in a row before an operator is locked for operators.lockout_minutes (0: never)";
    /// `operators.lockout_minutes`.
    LOCKOUT_MINUTES: u32 = "operators.lockout_minutes", 15, HardBounds::range(1, 1440),
        "How long a locked operator waits";
}

/// An operator's role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Everything, everywhere.
    PlatformOperator,
    /// The named DCs only.
    DcOperator,
}

/// `active` or `disabled`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OperatorState {
    /// May log in.
    #[default]
    Active,
    /// May not; every login session and token of theirs is revoked.
    Disabled,
}

/// An enrolled TOTP factor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Totp {
    /// The seed, sealed ([`sprig_auth::mfa::seal`]), hex.
    pub sealed: String,
    /// The last accepted step: a code works once.
    pub last_step: u64,
    /// When it was enrolled.
    pub enrolled_ms: u64,
}

/// A factor being enrolled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingTotp {
    /// The seed, sealed, hex.
    pub sealed: String,
    /// SHA-256 of the enrolment ticket.
    pub ticket_sha256: String,
    /// The ticket's end.
    pub expires_ms: u64,
}

/// `operators/<name>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorRecord {
    /// The login name.
    pub name: String,
    /// A number for the seed's sealing (the "user" of tenant 0).
    pub number: u32,
    /// For people.
    #[serde(default)]
    pub display_name: String,
    /// For notifications.
    #[serde(default)]
    pub email: String,
    /// The role.
    pub role: Role,
    /// A DC operator's DCs.
    #[serde(default)]
    pub dcs: Vec<DcId>,
    /// `active` or `disabled`.
    #[serde(default)]
    pub state: OperatorState,
    /// Chapter 14's `caching_sha2_password` verifier.
    pub verifier: String,
    /// When the password was last set.
    #[serde(default)]
    pub password_changed_ms: u64,
    /// The active factor.
    #[serde(default)]
    pub totp: Option<Totp>,
    /// A factor being enrolled.
    #[serde(default)]
    pub pending: Option<PendingTotp>,
    /// SHA-256 (hex) of each unused recovery code.
    #[serde(default)]
    pub recovery: Vec<String>,
    /// Failed logins in a row.
    #[serde(default)]
    pub failed_logins: u32,
    /// Locked until (ms), after too many failures.
    #[serde(default)]
    pub locked_until_ms: u64,
    /// When the account was created.
    pub created_ms: u64,
    /// Who created it.
    pub created_by: String,
    /// The last successful login.
    #[serde(default)]
    pub last_login_ms: u64,
}

impl Versioned for OperatorRecord {
    const V: u32 = 1;
}

/// The kind of an operator token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenKind {
    /// A login session (`sprigctl login`, `sprig`, Studio).
    Session,
    /// A token a person issued.
    Token,
    /// A service identity.
    Service,
}

/// What a service identity is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceKind {
    /// A gateway's own calls (Studio's views, the vector gate): the
    /// gateway routes only, always with the caller's claim.
    Gateway,
    /// Automation (CI, scripts): the issuer's role, narrowed by scopes and
    /// DCs.
    Ci,
}

/// `operator-tokens/<key_id>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenRecord {
    /// The id (in the token's text).
    pub key_id: String,
    /// Session, token or service.
    pub kind: TokenKind,
    /// The person who logged in or issued it.
    pub owner: String,
    /// A service identity's name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    /// A service identity's kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_kind: Option<ServiceKind>,
    /// A note.
    #[serde(default)]
    pub label: String,
    /// `<area>:read|write`; empty for sessions and gateway identities.
    #[serde(default)]
    pub scopes: Vec<String>,
    /// Narrower DCs; empty: the owner's.
    #[serde(default)]
    pub dcs: Vec<DcId>,
    /// A service identity's rights, fixed at issue (it outlives its
    /// issuer's account).
    #[serde(default)]
    pub platform: bool,
    /// How the session's person proved who they are (`pwd`, `otp`, `rec`).
    #[serde(default)]
    pub amr: Vec<String>,
    /// When it was made.
    pub created_ms: u64,
    /// When it ends; `None` only for service identities issued `never`.
    #[serde(default)]
    pub expires_ms: Option<u64>,
    /// When it was revoked.
    #[serde(default)]
    pub revoked_ms: Option<u64>,
    /// By whom.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_by: Option<String>,
    /// SHA-256 (hex) of the secret.
    pub secret_sha256: String,
}

impl Versioned for TokenRecord {
    const V: u32 = 1;
}

/// `operator-state`: the switch from the bootstrap token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateRecord {
    /// When the first operator logged in.
    pub switched_ms: u64,
    /// Who.
    pub by: String,
}

impl Versioned for StateRecord {
    const V: u32 = 1;
}

/// Who made a call, as the guard established it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    /// A named operator (a login session or a token they issued).
    Operator,
    /// A service identity (a `sprigsvc_` token or a configured file).
    Service,
    /// The bootstrap token (or no token on loopback) before the switch.
    Bootstrap,
    /// The bootstrap token after the switch, under break glass.
    BreakGlass,
    /// `[controller] dc_operator_token_file` before the switch.
    LegacyDcOperator,
}

/// The caller of one Admin API call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    /// What kind of caller.
    pub kind: PrincipalKind,
    /// The operator's or service's name; `bootstrap` otherwise.
    pub name: String,
    /// Platform rights.
    pub platform: bool,
    /// Otherwise, these DCs.
    #[serde(default)]
    pub dcs: Vec<DcId>,
    /// The token's id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
    /// The token's kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_kind: Option<TokenKind>,
    /// The token's scopes; empty: no narrowing.
    #[serde(default)]
    pub scopes: Vec<String>,
    /// A service identity's kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_kind: Option<ServiceKind>,
    /// Who issued a service identity's token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
}

impl Principal {
    /// The bootstrap principal.
    #[must_use]
    pub fn bootstrap() -> Self {
        Self {
            kind: PrincipalKind::Bootstrap,
            name: "bootstrap".into(),
            platform: true,
            dcs: Vec::new(),
            key_id: None,
            token_kind: None,
            scopes: Vec::new(),
            service_kind: None,
            owner: None,
        }
    }

    /// A configured service identity of kind gateway.
    #[must_use]
    pub fn gateway_service(name: &str) -> Self {
        Self {
            kind: PrincipalKind::Service,
            name: name.to_owned(),
            platform: false,
            dcs: Vec::new(),
            key_id: None,
            token_kind: None,
            scopes: Vec::new(),
            service_kind: Some(ServiceKind::Gateway),
            owner: None,
        }
    }

    /// Whether this is a gateway's service identity.
    #[must_use]
    pub fn is_gateway(&self) -> bool {
        self.service_kind == Some(ServiceKind::Gateway)
    }

    /// Whether this is a person logged in (a session, not a token).
    #[must_use]
    pub fn is_person_session(&self) -> bool {
        self.kind == PrincipalKind::Operator && self.token_kind == Some(TokenKind::Session)
    }

    /// Whether this caller may manage operators and tokens of others: a
    /// platform operator's login session, the bootstrap token before the
    /// switch, or break glass.
    #[must_use]
    pub fn manages_operators(&self) -> bool {
        match self.kind {
            PrincipalKind::Operator => self.is_person_session() && self.platform,
            PrincipalKind::Bootstrap | PrincipalKind::BreakGlass => true,
            PrincipalKind::Service | PrincipalKind::LegacyDcOperator => false,
        }
    }

    /// The actor recorded for this caller's changes (settings record it as
    /// `op:<actor>`); `None` keeps what the request says (the bootstrap
    /// token names no person).
    #[must_use]
    pub fn actor(&self) -> Option<String> {
        match self.kind {
            PrincipalKind::Operator => Some(self.name.clone()),
            PrincipalKind::Service if !self.is_gateway() => Some(format!("svc:{}", self.name)),
            PrincipalKind::BreakGlass => Some("breakglass".into()),
            PrincipalKind::Service | PrincipalKind::Bootstrap | PrincipalKind::LegacyDcOperator => {
                None
            }
        }
    }

    /// The name the audit records.
    #[must_use]
    pub fn audit_name(&self) -> String {
        match self.kind {
            PrincipalKind::Operator => format!("op:{}", self.name),
            PrincipalKind::Service => format!("svc:{}", self.name),
            PrincipalKind::Bootstrap => "bootstrap".into(),
            PrincipalKind::BreakGlass => "breakglass".into(),
            PrincipalKind::LegacyDcOperator => "dc-token".into(),
        }
    }
}

/// An operator as the API shows it (no secrets).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorView {
    /// The login name.
    pub name: String,
    /// For people.
    pub display_name: String,
    /// For notifications.
    pub email: String,
    /// The role.
    pub role: Role,
    /// A DC operator's DCs.
    pub dcs: Vec<DcId>,
    /// `active` or `disabled`.
    pub state: OperatorState,
    /// Whether a TOTP factor is enrolled.
    pub totp: bool,
    /// Unused recovery codes.
    pub recovery_codes_left: usize,
    /// Locked after failures until (ms); 0 when not.
    pub locked_until_ms: u64,
    /// When created.
    pub created_ms: u64,
    /// By whom.
    pub created_by: String,
    /// The last successful login.
    pub last_login_ms: u64,
}

impl From<&OperatorRecord> for OperatorView {
    fn from(r: &OperatorRecord) -> Self {
        Self {
            name: r.name.clone(),
            display_name: r.display_name.clone(),
            email: r.email.clone(),
            role: r.role,
            dcs: r.dcs.clone(),
            state: r.state,
            totp: r.totp.is_some(),
            recovery_codes_left: r.recovery.len(),
            locked_until_ms: r.locked_until_ms,
            created_ms: r.created_ms,
            created_by: r.created_by.clone(),
            last_login_ms: r.last_login_ms,
        }
    }
}

/// A token as the API shows it (no hash).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenView {
    /// The id.
    pub key_id: String,
    /// Session, token or service.
    pub kind: TokenKind,
    /// The person.
    pub owner: String,
    /// A service identity's name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    /// A service identity's kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_kind: Option<ServiceKind>,
    /// The note.
    pub label: String,
    /// The scopes.
    pub scopes: Vec<String>,
    /// Narrower DCs.
    pub dcs: Vec<DcId>,
    /// When made.
    pub created_ms: u64,
    /// When it ends.
    pub expires_ms: Option<u64>,
    /// When revoked.
    pub revoked_ms: Option<u64>,
    /// By whom.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_by: Option<String>,
}

impl From<&TokenRecord> for TokenView {
    fn from(t: &TokenRecord) -> Self {
        Self {
            key_id: t.key_id.clone(),
            kind: t.kind,
            owner: t.owner.clone(),
            service: t.service.clone(),
            service_kind: t.service_kind,
            label: t.label.clone(),
            scopes: t.scopes.clone(),
            dcs: t.dcs.clone(),
            created_ms: t.created_ms,
            expires_ms: t.expires_ms,
            revoked_ms: t.revoked_ms,
            revoked_by: t.revoked_by.clone(),
        }
    }
}

/// `POST /admin/v1/operators`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateOperatorRequest {
    /// The login name.
    pub name: String,
    /// `platform_operator` or `dc_operator`.
    pub role: Option<Role>,
    /// A DC operator's DCs.
    #[serde(default)]
    pub dcs: Vec<DcId>,
    /// The first password (the operator changes it).
    pub password: String,
    /// For people.
    #[serde(default)]
    pub display_name: String,
    /// For notifications.
    #[serde(default)]
    pub email: String,
    /// Why (audited).
    #[serde(default)]
    pub reason: String,
    /// Ignored: the guard records the caller.
    #[serde(default)]
    pub actor: Option<String>,
}

/// `POST /admin/v1/operators:login`.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoginRequest {
    /// The login name.
    pub name: String,
    /// The password.
    pub password: String,
    /// A TOTP code.
    #[serde(default)]
    pub code: Option<String>,
    /// Or a recovery code.
    #[serde(default)]
    pub recovery_code: Option<String>,
    /// What logs in (`sprigctl`, `sprig`, `studio`), for the audit.
    #[serde(default)]
    pub client: Option<String>,
}

impl std::fmt::Debug for LoginRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoginRequest")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// `POST /admin/v1/operators:enrol`.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnrolRequest {
    /// The login name.
    pub name: String,
    /// The ticket the login returned.
    pub ticket: String,
    /// The first code from the authenticator.
    pub code: String,
    /// What logs in.
    #[serde(default)]
    pub client: Option<String>,
}

impl std::fmt::Debug for EnrolRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnrolRequest")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// What a login returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginResponse {
    /// The session token (`sprigop_...`), when logged in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// Its end.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_ms: Option<u64>,
    /// The operator.
    pub operator: OperatorView,
    /// A factor must be enrolled first: the ticket for `operators:enrol`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enrol_ticket: Option<String>,
    /// The `otpauth://` URI to add to an authenticator (shown once).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub otpauth_uri: Option<String>,
    /// After an enrolment: the recovery codes (shown once).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recovery_codes: Vec<String>,
}

/// `POST /admin/v1/operator-tokens`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTokenRequest {
    /// A note.
    #[serde(default)]
    pub label: String,
    /// `<area>:read|write`; at least one (not for a gateway identity).
    #[serde(default)]
    pub scopes: Vec<String>,
    /// Narrower DCs.
    #[serde(default)]
    pub dcs: Vec<DcId>,
    /// Lifetime in days (default 30, at most `operators.token_max_days`).
    #[serde(default)]
    pub expires_in_days: Option<u32>,
    /// A service identity's name: issues a `sprigsvc_` token.
    #[serde(default)]
    pub service: Option<String>,
    /// Its kind (default `ci`).
    #[serde(default)]
    pub service_kind: Option<ServiceKind>,
    /// A service identity that never expires (gateways).
    #[serde(default)]
    pub never_expires: bool,
    /// Why (audited).
    #[serde(default)]
    pub reason: String,
    /// Ignored: the guard records the caller.
    #[serde(default)]
    pub actor: Option<String>,
}

/// A new token: its text, once, and its record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTokenResponse {
    /// `sprigop_...` or `sprigsvc_...`: shown once.
    pub token: String,
    /// The record.
    pub record: TokenView,
}

/// `POST /admin/v1/operators/{name}:setPassword`.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetPasswordRequest {
    /// The current password (one's own).
    #[serde(default)]
    pub current: Option<String>,
    /// The new one.
    pub password: String,
    /// Why (audited).
    #[serde(default)]
    pub reason: String,
    /// Ignored.
    #[serde(default)]
    pub actor: Option<String>,
}

impl std::fmt::Debug for SetPasswordRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SetPasswordRequest").finish_non_exhaustive()
    }
}

/// What `whoami` answers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhoAmI {
    /// The caller.
    pub principal: Principal,
    /// The operator's account, for people.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator: Option<OperatorView>,
    /// The token's end.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_ms: Option<u64>,
    /// Whether named operators have taken over from the bootstrap token.
    pub switched: bool,
    /// The DC this controller serves.
    pub dc: DcId,
}

fn invalid(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::AdminRequestInvalid, m)
}

/// 39313.
pub fn forbidden(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::OperatorForbidden, m)
}

fn login_failed() -> SprigError {
    SprigError::new(
        SprigCode::OperatorLoginFailed,
        "the name, password or code is wrong, or the account is disabled or locked",
    )
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter()
        .fold(String::with_capacity(b.len() * 2), |mut s, x| {
            let _ = write!(s, "{x:02x}");
            s
        })
}

fn unhex(s: &str) -> Result<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return Err(SprigError::internal("a sealed seed isn't hex"));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|_| SprigError::internal("a sealed seed isn't hex"))
        })
        .collect()
}

const DAY_MS: u64 = 86_400_000;
/// A token's default lifetime.
const DEFAULT_TOKEN_DAYS: u32 = 30;
/// An enrolment ticket's lifetime.
const ENROL_MS: u64 = 10 * 60_000;

/// The operator directory and its calls.
pub struct Operators {
    global: Arc<dyn MetaStore>,
    clock: Arc<dyn Clock>,
    keys: Option<Arc<dyn FactorKeys>>,
    live: Option<LiveSettings>,
    dc: DcId,
    switched: AtomicBool,
}

impl std::fmt::Debug for Operators {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Operators")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

impl Operators {
    /// Over the global tier `global`, for the controller of `dc`; `keys`
    /// seal TOTP seeds (none: no second factors can be enrolled).
    #[must_use]
    pub fn new(
        global: Arc<dyn MetaStore>,
        clock: Arc<dyn Clock>,
        keys: Option<Arc<dyn FactorKeys>>,
        live: Option<LiveSettings>,
        dc: DcId,
    ) -> Self {
        Self {
            global,
            clock,
            keys,
            live,
            dc,
            switched: AtomicBool::new(false),
        }
    }

    /// The DC this controller serves.
    #[must_use]
    pub fn dc(&self) -> &DcId {
        &self.dc
    }

    fn now_ms(&self) -> u64 {
        self.clock
            .wall()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }

    fn setting<T: sprig_settings::SettingValue + Clone>(&self, s: &'static Setting<T>) -> T {
        match &self.live {
            Some(l) => l.snapshot().get(s, &SettingCtx::process(&self.dc)),
            None => s.default.clone(),
        }
    }

    fn op_key(name: &str) -> Result<Key> {
        Tier::Global.key(&format!("operators/{name}"))
    }

    fn token_key(id: &str) -> Result<Key> {
        Tier::Global.key(&format!("operator-tokens/{id}"))
    }

    fn state_key() -> Result<Key> {
        Tier::Global.key("operator-state")
    }

    async fn get(&self, name: &str) -> Result<Option<(OperatorRecord, sprig_meta::Revision)>> {
        if !op::valid_name(name) {
            return Ok(None);
        }
        let Some(kv) = self
            .global
            .get(&Self::op_key(name)?, Consistency::Linearizable)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some((
            Record::<OperatorRecord>::decode(&kv.value)?.body,
            kv.mod_revision,
        )))
    }

    async fn list_prefix<T: Versioned>(&self, rel: &str) -> Result<Vec<(T, sprig_meta::Revision)>> {
        let prefix = Tier::Global.key(rel)?;
        let mut out = Vec::new();
        let mut opts = ListOpts::default();
        loop {
            let page = self.global.list(&prefix, opts.clone()).await?;
            for kv in &page.kvs {
                match Record::<T>::decode(&kv.value) {
                    Ok(r) => out.push((r.body, kv.mod_revision)),
                    Err(e) => tracing::warn!(key = %kv.key, error = %e, "skipping a bad record"),
                }
            }
            if !page.more {
                return Ok(out);
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
    }

    /// Every operator.
    ///
    /// # Errors
    /// The store's.
    pub async fn list(&self) -> Result<Vec<OperatorView>> {
        let mut v: Vec<OperatorView> = self
            .list_prefix::<OperatorRecord>("operators/")
            .await?
            .iter()
            .map(|(r, _)| OperatorView::from(r))
            .collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(v)
    }

    async fn token(&self, id: &str) -> Result<Option<(TokenRecord, sprig_meta::Revision)>> {
        let Some(kv) = self
            .global
            .get(&Self::token_key(id)?, Consistency::Linearizable)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some((
            Record::<TokenRecord>::decode(&kv.value)?.body,
            kv.mod_revision,
        )))
    }

    /// Writes `rec` if the record still has `rev` (`None`: doesn't exist),
    /// with `extra` puts in the same transaction. `false` when it changed.
    async fn put(
        &self,
        rec: &OperatorRecord,
        rev: Option<sprig_meta::Revision>,
        extra: Vec<Op>,
    ) -> Result<bool> {
        let key = Self::op_key(&rec.name)?;
        let when = match rev {
            Some(r) => vec![Cmp::ModRev(key.clone(), r)],
            None => vec![Cmp::Missing(key.clone())],
        };
        let mut then = vec![Op::Put {
            key,
            value: Record::new(rec.clone()).encode()?,
            lease: None,
        }];
        then.extend(extra);
        Ok(self
            .global
            .txn(Txn {
                when,
                then,
                otherwise: vec![],
            })
            .await?
            .succeeded)
    }

    fn token_put(t: &TokenRecord) -> Result<Op> {
        Ok(Op::Put {
            key: Self::token_key(&t.key_id)?,
            value: Record::new(t.clone()).encode()?,
            lease: None,
        })
    }

    /// Whether named operators have taken over from the bootstrap token.
    ///
    /// # Errors
    /// The store's.
    pub async fn switched(&self) -> Result<bool> {
        if self.switched.load(Ordering::Relaxed) {
            return Ok(true);
        }
        let s = self
            .global
            .get(&Self::state_key()?, Consistency::Linearizable)
            .await?
            .is_some();
        if s {
            self.switched.store(true, Ordering::Relaxed);
        }
        Ok(s)
    }

    async fn switch(&self, by: &str) -> Result<()> {
        if self.switched().await? {
            return Ok(());
        }
        let key = Self::state_key()?;
        let r = self
            .global
            .txn(Txn {
                when: vec![Cmp::Missing(key.clone())],
                then: vec![Op::Put {
                    key,
                    value: Record::new(StateRecord {
                        switched_ms: self.now_ms(),
                        by: by.to_owned(),
                    })
                    .encode()?,
                    lease: None,
                }],
                otherwise: vec![],
            })
            .await?;
        self.switched.store(true, Ordering::Relaxed);
        if r.succeeded {
            tracing::info!(target: "sprig_audit", event = "operator.switched", operator = by,
                "named operators are in use: the bootstrap token is refused from now on");
            audit::record(
                AuditStream::Platform,
                "operator.switched",
                serde_json::json!({ "operator": by, "dc": self.dc }),
            );
        }
        Ok(())
    }

    fn event(&self, event: &str, actor: &str, mut body: serde_json::Value) {
        if let serde_json::Value::Object(o) = &mut body {
            o.insert("actor".into(), actor.into());
            o.insert("dc".into(), self.dc.as_str().into());
        }
        tracing::info!(target: "sprig_audit", event, actor, body = %body, "operator event");
        audit::record(AuditStream::Platform, event, body);
    }

    fn sealing_keys(&self) -> Result<&Arc<dyn FactorKeys>> {
        self.keys.as_ref().ok_or_else(|| {
            SprigError::new(
                SprigCode::AuthUnavailable,
                "operators' second factors need [controller] workload_key_file (the master key file)",
            )
        })
    }

    /// `CreateOperator`.
    ///
    /// # Errors
    /// 39313 unless a platform operator's login session (or the bootstrap
    /// token before the switch); 39792 for a bad name, role or password;
    /// 39314 for a name in use.
    pub async fn create(
        &self,
        who: &Principal,
        req: CreateOperatorRequest,
    ) -> Result<OperatorView> {
        if !who.manages_operators() {
            return Err(forbidden(
                "creating operators needs a platform operator's login session (sprigctl login)",
            ));
        }
        if !op::valid_name(&req.name) {
            return Err(invalid(format!(
                "operator name {:?}: a lower-case letter, then up to 62 letters, digits, '.', '_' or '-'",
                req.name
            )));
        }
        let role = req
            .role
            .ok_or_else(|| invalid("role: platform_operator or dc_operator"))?;
        match role {
            Role::PlatformOperator if !req.dcs.is_empty() => {
                return Err(invalid("a platform operator names no DCs"));
            }
            Role::DcOperator if req.dcs.is_empty() => {
                return Err(invalid("a DC operator names at least one DC (--dc)"));
            }
            _ => {}
        }
        let mut dcs = req.dcs.clone();
        dcs.sort();
        dcs.dedup();
        let now = self.now_ms();
        let rec = OperatorRecord {
            name: req.name.clone(),
            number: u32::try_from(uuid::Uuid::now_v7().as_u64_pair().1 % u64::from(u32::MAX))
                .unwrap_or(1),
            display_name: req.display_name,
            email: req.email,
            role,
            dcs,
            state: OperatorState::Active,
            verifier: op::hash_password(&req.password)?,
            password_changed_ms: now,
            totp: None,
            pending: None,
            recovery: Vec::new(),
            failed_logins: 0,
            locked_until_ms: 0,
            created_ms: now,
            created_by: who.audit_name(),
            last_login_ms: 0,
        };
        if !self.put(&rec, None, vec![]).await? {
            return Err(SprigError::new(
                SprigCode::OperatorExists,
                format!("an operator named {} exists", req.name),
            ));
        }
        self.event(
            "operator.created",
            &who.audit_name(),
            serde_json::json!({ "operator": rec.name, "role": rec.role, "dcs": rec.dcs,
                "reason": req.reason }),
        );
        Ok(OperatorView::from(&rec))
    }

    /// `GetOperator`: anyone for themselves, platform operators for all.
    ///
    /// # Errors
    /// 39315; 39313.
    pub async fn show(&self, who: &Principal, name: &str) -> Result<OperatorView> {
        if !(who.platform || who.name == name && who.kind == PrincipalKind::Operator) {
            return Err(forbidden("platform operators see other operators"));
        }
        self.get(name)
            .await?
            .map(|(r, _)| OperatorView::from(&r))
            .ok_or_else(|| not_found(name))
    }

    /// `Login`.
    ///
    /// # Errors
    /// 39312 for anything wrong; 39311 when a code is needed; 39301
    /// without the sealing key for an enrolment.
    #[allow(clippy::too_many_lines)] // one check after another
    pub async fn login(&self, req: LoginRequest) -> Result<LoginResponse> {
        let now = self.now_ms();
        let client = req.client.clone().unwrap_or_default();
        let found = self.get(&req.name).await?;
        let Some((mut rec, rev)) = found else {
            let _ = op::verify_password(None, &req.password);
            self.event(
                "operator.login_failed",
                &format!("op:{}", req.name),
                serde_json::json!({ "operator": req.name, "why": "unknown", "client": client }),
            );
            return Err(login_failed());
        };
        let ok = op::verify_password(Some(&rec.verifier), &req.password);
        let why = if rec.state == OperatorState::Disabled {
            Some("disabled")
        } else if rec.locked_until_ms > now {
            Some("locked")
        } else if !ok {
            Some("password")
        } else {
            None
        };
        if let Some(why) = why {
            if why == "password" {
                self.count_failure(&mut rec, rev, now).await?;
            }
            self.event(
                "operator.login_failed",
                &format!("op:{}", rec.name),
                serde_json::json!({ "operator": rec.name, "why": why, "client": client }),
            );
            return Err(login_failed());
        }
        let mut amr = vec!["pwd".to_owned()];
        if let Some(t) = rec.totp.clone() {
            if let Some(code) = req.code.as_deref().filter(|c| !c.trim().is_empty()) {
                let seed = mfa::open(
                    &**self.sealing_keys()?,
                    TenantId(0),
                    UserId(rec.number),
                    &unhex(&t.sealed)?,
                )?;
                let step = mfa::check(&seed, code, self.clock.wall(), Some(t.last_step));
                let Some(step) = step else {
                    self.count_failure(&mut rec, rev, now).await?;
                    self.event(
                        "operator.login_failed",
                        &format!("op:{}", rec.name),
                        serde_json::json!({ "operator": rec.name, "why": "code", "client": client }),
                    );
                    return Err(login_failed());
                };
                rec.totp = Some(Totp {
                    last_step: step,
                    ..t
                });
                amr.push("otp".into());
            } else if let Some(code) = req
                .recovery_code
                .as_deref()
                .filter(|c| !c.trim().is_empty())
            {
                let h = hex(&mfa::recovery_hash(code));
                let Some(i) = rec.recovery.iter().position(|r| *r == h) else {
                    self.count_failure(&mut rec, rev, now).await?;
                    self.event(
                        "operator.login_failed",
                        &format!("op:{}", rec.name),
                        serde_json::json!({ "operator": rec.name, "why": "recovery_code",
                            "client": client }),
                    );
                    return Err(login_failed());
                };
                rec.recovery.remove(i);
                amr.push("rec".into());
            } else {
                return Err(SprigError::new(
                    SprigCode::OperatorMfaRequired,
                    "a second-factor code is required (--code, or a recovery code)",
                ));
            }
        } else if self.setting(&MFA_REQUIRED) {
            // Enrolment: a fresh seed, sealed, and a ticket.
            let keys = self.sealing_keys()?;
            let seed = op::new_seed()?;
            let sealed = mfa::seal(&**keys, TenantId(0), UserId(rec.number), &seed)?;
            let (ticket, ticket_sha256) = op::new_ticket()?;
            rec.pending = Some(PendingTotp {
                sealed: hex(&sealed),
                ticket_sha256,
                expires_ms: now + ENROL_MS,
            });
            rec.failed_logins = 0;
            if !self.put(&rec, Some(rev), vec![]).await? {
                return Err(conflict());
            }
            return Ok(LoginResponse {
                token: None,
                expires_ms: None,
                operator: OperatorView::from(&rec),
                enrol_ticket: Some(ticket),
                otpauth_uri: Some(mfa::otpauth_uri("SprigDB operators", &rec.name, &seed)),
                recovery_codes: Vec::new(),
            });
        }
        self.finish_login(rec, rev, amr, &client, Vec::new()).await
    }

    async fn count_failure(
        &self,
        rec: &mut OperatorRecord,
        rev: sprig_meta::Revision,
        now: u64,
    ) -> Result<()> {
        let limit = self.setting(&LOCKOUT_FAILURES);
        rec.failed_logins += 1;
        if limit > 0 && rec.failed_logins >= limit {
            rec.locked_until_ms = now + u64::from(self.setting(&LOCKOUT_MINUTES)) * 60_000;
            rec.failed_logins = 0;
            self.event(
                "operator.locked",
                &format!("op:{}", rec.name),
                serde_json::json!({ "operator": rec.name, "until_ms": rec.locked_until_ms }),
            );
        }
        // A lost race only loses one count.
        let _ = self.put(rec, Some(rev), vec![]).await?;
        Ok(())
    }

    async fn finish_login(
        &self,
        mut rec: OperatorRecord,
        rev: sprig_meta::Revision,
        amr: Vec<String>,
        client: &str,
        recovery_codes: Vec<String>,
    ) -> Result<LoginResponse> {
        let now = self.now_ms();
        let t = op::new_token(false)?;
        let expires = now + u64::from(self.setting(&SESSION_HOURS)) * 3_600_000;
        let tok = TokenRecord {
            key_id: t.key_id.clone(),
            kind: TokenKind::Session,
            owner: rec.name.clone(),
            service: None,
            service_kind: None,
            label: if client.is_empty() {
                "login".into()
            } else {
                format!("login ({client})")
            },
            scopes: Vec::new(),
            dcs: Vec::new(),
            platform: false,
            amr: amr.clone(),
            created_ms: now,
            expires_ms: Some(expires),
            revoked_ms: None,
            revoked_by: None,
            secret_sha256: t.secret_sha256.clone(),
        };
        rec.failed_logins = 0;
        rec.locked_until_ms = 0;
        rec.last_login_ms = now;
        if !self
            .put(&rec, Some(rev), vec![Self::token_put(&tok)?])
            .await?
        {
            return Err(conflict());
        }
        self.event(
            "operator.login",
            &format!("op:{}", rec.name),
            serde_json::json!({ "operator": rec.name, "amr": amr, "key_id": tok.key_id,
                "client": client, "expires_ms": expires }),
        );
        if rec.totp.is_some() || !self.setting(&MFA_REQUIRED) {
            self.switch(&rec.name).await?;
        }
        Ok(LoginResponse {
            token: Some(t.text),
            expires_ms: Some(expires),
            operator: OperatorView::from(&rec),
            enrol_ticket: None,
            otpauth_uri: None,
            recovery_codes,
        })
    }

    /// `Enrol`: the first code of a new factor; logs in.
    ///
    /// # Errors
    /// 39312 for a wrong ticket or code.
    pub async fn enrol(&self, req: EnrolRequest) -> Result<LoginResponse> {
        let now = self.now_ms();
        let Some((mut rec, rev)) = self.get(&req.name).await? else {
            return Err(login_failed());
        };
        let Some(p) = rec.pending.clone() else {
            return Err(login_failed());
        };
        if rec.state == OperatorState::Disabled
            || p.expires_ms < now
            || !op::secret_matches(&p.ticket_sha256, &req.ticket)
        {
            return Err(login_failed());
        }
        let sealed = unhex(&p.sealed)?;
        let seed = mfa::open(
            &**self.sealing_keys()?,
            TenantId(0),
            UserId(rec.number),
            &sealed,
        )?;
        let Some(step) = mfa::check(&seed, &req.code, self.clock.wall(), None) else {
            self.event(
                "operator.login_failed",
                &format!("op:{}", rec.name),
                serde_json::json!({ "operator": rec.name, "why": "enrolment_code" }),
            );
            return Err(login_failed());
        };
        let codes = mfa::recovery_codes()?;
        rec.recovery = codes.iter().map(|c| hex(&mfa::recovery_hash(c))).collect();
        rec.totp = Some(Totp {
            sealed: p.sealed,
            last_step: step,
            enrolled_ms: now,
        });
        rec.pending = None;
        self.event(
            "operator.factor_enrolled",
            &format!("op:{}", rec.name),
            serde_json::json!({ "operator": rec.name, "factor": "totp" }),
        );
        let client = req.client.unwrap_or_default();
        self.finish_login(rec, rev, vec!["pwd".into(), "otp".into()], &client, codes)
            .await
    }

    /// `Logout`: revokes the session token used.
    ///
    /// # Errors
    /// 39313 for anything but a login session.
    pub async fn logout(&self, who: &Principal) -> Result<TokenView> {
        if !who.is_person_session() {
            return Err(forbidden("logout ends a login session"));
        }
        let id = who.key_id.clone().unwrap_or_default();
        self.revoke_inner(who, &id).await
    }

    /// `WhoAmI`.
    ///
    /// # Errors
    /// The store's.
    pub async fn whoami(&self, who: &Principal) -> Result<WhoAmI> {
        let operator = if who.kind == PrincipalKind::Operator {
            self.get(&who.name)
                .await?
                .map(|(r, _)| OperatorView::from(&r))
        } else {
            None
        };
        let expires_ms = match &who.key_id {
            Some(id) => self.token(id).await?.and_then(|(t, _)| t.expires_ms),
            None => None,
        };
        Ok(WhoAmI {
            principal: who.clone(),
            operator,
            expires_ms,
            switched: self.switched().await?,
            dc: self.dc.clone(),
        })
    }

    /// `DisableOperator` and `EnableOperator`. Disabling revokes the
    /// operator's sessions and tokens (not service identities they
    /// issued); the last active platform operator can't be disabled.
    ///
    /// # Errors
    /// 39313; 39315; 39792 for the last platform operator.
    pub async fn set_state(
        &self,
        who: &Principal,
        name: &str,
        disabled: bool,
        reason: &str,
    ) -> Result<OperatorView> {
        if !who.manages_operators() {
            return Err(forbidden(
                "platform operators' login sessions manage operators",
            ));
        }
        let (mut rec, rev) = self.get(name).await?.ok_or_else(|| not_found(name))?;
        if disabled && rec.role == Role::PlatformOperator {
            let others = self
                .list_prefix::<OperatorRecord>("operators/")
                .await?
                .iter()
                .filter(|(r, _)| {
                    r.name != name
                        && r.role == Role::PlatformOperator
                        && r.state == OperatorState::Active
                })
                .count();
            if others == 0 {
                return Err(invalid(
                    "this is the last active platform operator: create another one first",
                ));
            }
        }
        rec.state = if disabled {
            OperatorState::Disabled
        } else {
            OperatorState::Active
        };
        let mut extra = Vec::new();
        if disabled {
            extra = self
                .revoke_ops(&rec.name, &who.audit_name(), false, None)
                .await?;
        } else {
            rec.failed_logins = 0;
            rec.locked_until_ms = 0;
        }
        if !self.put(&rec, Some(rev), extra).await? {
            return Err(conflict());
        }
        self.event(
            if disabled {
                "operator.disabled"
            } else {
                "operator.enabled"
            },
            &who.audit_name(),
            serde_json::json!({ "operator": name, "reason": reason }),
        );
        Ok(OperatorView::from(&rec))
    }

    /// Puts revoking `owner`'s live sessions and tokens (with
    /// `services`, also service identities they issued), except `keep`.
    async fn revoke_ops(
        &self,
        owner: &str,
        by: &str,
        services: bool,
        keep: Option<&str>,
    ) -> Result<Vec<Op>> {
        let now = self.now_ms();
        let mut ops = Vec::new();
        for (mut t, _) in self.list_prefix::<TokenRecord>("operator-tokens/").await? {
            if t.owner != owner
                || t.revoked_ms.is_some()
                || (t.kind == TokenKind::Service && !services)
                || keep == Some(t.key_id.as_str())
                || t.expires_ms.is_some_and(|e| e < now)
            {
                continue;
            }
            t.revoked_ms = Some(now);
            t.revoked_by = Some(by.to_owned());
            ops.push(Self::token_put(&t)?);
        }
        Ok(ops)
    }

    /// `ResetOperatorFactor`: the next login enrols a new factor; the
    /// operator's sessions end.
    ///
    /// # Errors
    /// 39313; 39315.
    pub async fn reset_factor(
        &self,
        who: &Principal,
        name: &str,
        reason: &str,
    ) -> Result<OperatorView> {
        if !who.manages_operators() {
            return Err(forbidden(
                "platform operators' login sessions reset factors",
            ));
        }
        let (mut rec, rev) = self.get(name).await?.ok_or_else(|| not_found(name))?;
        rec.totp = None;
        rec.pending = None;
        rec.recovery.clear();
        let extra = self
            .revoke_ops(name, &who.audit_name(), false, None)
            .await?;
        if !self.put(&rec, Some(rev), extra).await? {
            return Err(conflict());
        }
        self.event(
            "operator.factor_reset",
            &who.audit_name(),
            serde_json::json!({ "operator": name, "reason": reason }),
        );
        Ok(OperatorView::from(&rec))
    }

    /// `SetOperatorPassword`: one's own with the current password, or any
    /// as a platform operator. Ends the operator's other sessions.
    ///
    /// # Errors
    /// 39313; 39315; 39312 for a wrong current password; 39792 for a short
    /// one.
    pub async fn set_password(
        &self,
        who: &Principal,
        name: &str,
        req: SetPasswordRequest,
    ) -> Result<OperatorView> {
        let own = who.kind == PrincipalKind::Operator && who.name == name;
        if !own && !who.manages_operators() {
            return Err(forbidden("an operator sets their own password"));
        }
        if own && !who.is_person_session() {
            return Err(forbidden("passwords are changed from a login session"));
        }
        let (mut rec, rev) = self.get(name).await?.ok_or_else(|| not_found(name))?;
        if own && !op::verify_password(Some(&rec.verifier), req.current.as_deref().unwrap_or("")) {
            return Err(login_failed());
        }
        rec.verifier = op::hash_password(&req.password)?;
        rec.password_changed_ms = self.now_ms();
        let keep = if own { who.key_id.as_deref() } else { None };
        let extra = self
            .revoke_ops(name, &who.audit_name(), false, keep)
            .await?;
        if !self.put(&rec, Some(rev), extra).await? {
            return Err(conflict());
        }
        self.event(
            "operator.password_changed",
            &who.audit_name(),
            serde_json::json!({ "operator": name, "reason": req.reason }),
        );
        Ok(OperatorView::from(&rec))
    }

    /// `CreateToken`: a person's login session issues a token in their own
    /// name; platform operators also issue service identities.
    ///
    /// # Errors
    /// 39313; 39792 for bad scopes, DCs or lifetime.
    #[allow(clippy::too_many_lines)] // one check after another
    pub async fn create_token(
        &self,
        who: &Principal,
        req: CreateTokenRequest,
    ) -> Result<CreateTokenResponse> {
        if !who.is_person_session() {
            return Err(forbidden(
                "tokens are issued from a login session (sprigctl login), never from another token",
            ));
        }
        let (owner, _) = self
            .get(&who.name)
            .await?
            .ok_or_else(|| not_found(&who.name))?;
        let service_kind = req
            .service
            .as_ref()
            .map(|_| req.service_kind.unwrap_or(ServiceKind::Ci));
        if let Some(s) = &req.service {
            if !op::valid_name(s) {
                return Err(invalid(format!("service name {s:?} isn't valid")));
            }
            if !who.platform {
                return Err(forbidden("platform operators issue service identities"));
            }
        }
        let gateway = service_kind == Some(ServiceKind::Gateway);
        let mut scopes = Vec::new();
        for s in &req.scopes {
            scopes.push(op::Scope::parse(s)?.to_string());
        }
        if scopes.is_empty() && !gateway {
            return Err(invalid(
                "name at least one scope: <area>:read or <area>:write (for example clusters:read, *:read)",
            ));
        }
        if gateway && !scopes.is_empty() {
            return Err(invalid(
                "a gateway identity has no scopes: it reaches the gateway routes only",
            ));
        }
        let mut dcs = req.dcs.clone();
        dcs.sort();
        dcs.dedup();
        if owner.role == Role::DcOperator && dcs.iter().any(|d| !owner.dcs.contains(d)) {
            return Err(forbidden("a token's DCs are among its owner's"));
        }
        let max = self.setting(&TOKEN_MAX_DAYS);
        let days = req.expires_in_days.unwrap_or(DEFAULT_TOKEN_DAYS.min(max));
        if days == 0 || days > max {
            return Err(invalid(format!(
                "a token lives 1 to {max} days (operators.token_max_days)"
            )));
        }
        if req.never_expires && req.service.is_none() {
            return Err(invalid("only service identities may never expire"));
        }
        let now = self.now_ms();
        let t = op::new_token(req.service.is_some())?;
        let rec = TokenRecord {
            key_id: t.key_id.clone(),
            kind: if req.service.is_some() {
                TokenKind::Service
            } else {
                TokenKind::Token
            },
            owner: owner.name.clone(),
            service: req.service.clone(),
            service_kind,
            label: req.label,
            scopes,
            dcs: if owner.role == Role::DcOperator && req.dcs.is_empty() {
                owner.dcs.clone()
            } else {
                dcs
            },
            platform: owner.role == Role::PlatformOperator,
            amr: Vec::new(),
            created_ms: now,
            expires_ms: (!req.never_expires).then(|| now + u64::from(days) * DAY_MS),
            revoked_ms: None,
            revoked_by: None,
            secret_sha256: t.secret_sha256,
        };
        let r = self
            .global
            .txn(Txn {
                when: vec![Cmp::Missing(Self::token_key(&rec.key_id)?)],
                then: vec![Self::token_put(&rec)?],
                otherwise: vec![],
            })
            .await?;
        if !r.succeeded {
            return Err(conflict());
        }
        self.event(
            if rec.kind == TokenKind::Service {
                "operator.service_identity_issued"
            } else {
                "operator.token_issued"
            },
            &who.audit_name(),
            serde_json::json!({ "key_id": rec.key_id, "service": rec.service,
                "service_kind": rec.service_kind, "scopes": rec.scopes, "dcs": rec.dcs,
                "expires_ms": rec.expires_ms, "label": rec.label, "reason": req.reason }),
        );
        Ok(CreateTokenResponse {
            token: t.text,
            record: TokenView::from(&rec),
        })
    }

    /// `ListTokens`: one's own; platform operators any owner's (`*`: all).
    ///
    /// # Errors
    /// 39313.
    pub async fn list_tokens(
        &self,
        who: &Principal,
        owner: Option<&str>,
    ) -> Result<Vec<TokenView>> {
        let owner = match owner {
            Some(o) if o != who.name => {
                if !who.platform || who.kind == PrincipalKind::Service && who.is_gateway() {
                    return Err(forbidden("platform operators list others' tokens"));
                }
                Some(o)
            }
            Some(o) => Some(o),
            None if who.kind == PrincipalKind::Operator => Some(who.name.as_str()),
            None if who.platform => None,
            None => return Err(forbidden("name an owner")),
        };
        let mut v: Vec<TokenView> = self
            .list_prefix::<TokenRecord>("operator-tokens/")
            .await?
            .iter()
            .filter(|(t, _)| owner.is_none_or(|o| o == "*" || t.owner == o))
            .map(|(t, _)| TokenView::from(t))
            .collect();
        v.sort_by_key(|t| std::cmp::Reverse(t.created_ms));
        Ok(v)
    }

    /// `RevokeToken`: one's own; platform operators any.
    ///
    /// # Errors
    /// 39315; 39313.
    pub async fn revoke_token(&self, who: &Principal, key_id: &str) -> Result<TokenView> {
        self.revoke_inner(who, key_id).await
    }

    async fn revoke_inner(&self, who: &Principal, key_id: &str) -> Result<TokenView> {
        let (mut t, rev) = self.token(key_id).await?.ok_or_else(|| not_found(key_id))?;
        let own = who.kind == PrincipalKind::Operator && who.name == t.owner;
        if !own && !who.manages_operators() {
            return Err(forbidden("an operator revokes their own tokens"));
        }
        if t.revoked_ms.is_none() {
            t.revoked_ms = Some(self.now_ms());
            t.revoked_by = Some(who.audit_name());
            let key = Self::token_key(key_id)?;
            let r = self
                .global
                .txn(Txn {
                    when: vec![Cmp::ModRev(key, rev)],
                    then: vec![Self::token_put(&t)?],
                    otherwise: vec![],
                })
                .await?;
            if !r.succeeded {
                return Err(conflict());
            }
            self.event(
                "operator.token_revoked",
                &who.audit_name(),
                serde_json::json!({ "key_id": key_id, "owner": t.owner, "kind": t.kind }),
            );
        }
        Ok(TokenView::from(&t))
    }

    /// The caller a `sprigop_`/`sprigsvc_` token names.
    ///
    /// # Errors
    /// 39793 for an unknown token or a wrong secret; 39316 for one that is
    /// expired or revoked, or whose owner is disabled.
    pub async fn authenticate(&self, text: &str) -> Result<Principal> {
        let unknown = || {
            SprigError::new(
                SprigCode::AdminUnauthenticated,
                "the operator token isn't valid",
            )
        };
        let (_, id, secret) = op::parse_token(text).ok_or_else(unknown)?;
        let (t, _) = self.token(id).await?.ok_or_else(unknown)?;
        if !op::secret_matches(&t.secret_sha256, secret) {
            return Err(unknown());
        }
        let now = self.now_ms();
        let expired = |why: &str| {
            SprigError::new(
                SprigCode::OperatorTokenExpired,
                format!("the operator token {id} is {why}: log in again (sprigctl login)"),
            )
        };
        if t.revoked_ms.is_some() {
            return Err(expired("revoked"));
        }
        if t.expires_ms.is_some_and(|e| e <= now) {
            return Err(expired("expired"));
        }
        if t.kind == TokenKind::Service {
            let name = t.service.clone().unwrap_or_default();
            return Ok(Principal {
                kind: PrincipalKind::Service,
                name,
                platform: t.platform && t.dcs.is_empty(),
                dcs: t.dcs.clone(),
                key_id: Some(t.key_id.clone()),
                token_kind: Some(TokenKind::Service),
                scopes: t.scopes.clone(),
                service_kind: t.service_kind,
                owner: Some(t.owner.clone()),
            });
        }
        let Some((owner, _)) = self.get(&t.owner).await? else {
            return Err(expired("of a removed operator"));
        };
        if owner.state == OperatorState::Disabled {
            return Err(expired("of a disabled operator"));
        }
        let platform = owner.role == Role::PlatformOperator && t.dcs.is_empty();
        let dcs = if platform {
            Vec::new()
        } else if t.dcs.is_empty() {
            owner.dcs.clone()
        } else {
            t.dcs.clone()
        };
        Ok(Principal {
            kind: PrincipalKind::Operator,
            name: owner.name,
            platform,
            dcs,
            key_id: Some(t.key_id.clone()),
            token_kind: Some(t.kind),
            scopes: t.scopes.clone(),
            service_kind: None,
            owner: None,
        })
    }

    /// Whether any operator exists (for start-up checks).
    ///
    /// # Errors
    /// The store's.
    pub async fn any(&self) -> Result<bool> {
        let prefix = Tier::Global.key("operators/")?;
        let page = self
            .global
            .list(
                &prefix,
                ListOpts {
                    limit: 1,
                    ..ListOpts::default()
                },
            )
            .await?;
        Ok(!page.kvs.is_empty())
    }
}

fn not_found(what: &str) -> SprigError {
    SprigError::new(
        SprigCode::OperatorNotFound,
        format!("no operator or token {what:?}"),
    )
}

fn conflict() -> SprigError {
    SprigError::new(
        SprigCode::CatalogConflict,
        "the operator record changed meanwhile; try again",
    )
}

#[cfg(test)]
mod tests;
