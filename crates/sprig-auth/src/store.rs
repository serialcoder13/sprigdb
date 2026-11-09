//! The auth tables of the tenant catalog (chapter 14, "Catalog tables";
//! build step 1), as the `auth` migration module, with typed rows and
//! access.
//!
//! The module registers with `sprig-catalog`'s migrator ([`AUTH_MODULE`],
//! or through [`AuthExtension`](crate::cache::AuthExtension)), so every
//! tenant catalog gets these tables with its own: `sprig-auth` depends on
//! `sprig-catalog`, never the reverse (chapter 08, "Tables owned by other
//! chapters").
//!
//! Format 1 creates `users`, `verifiers` and `login_state` exactly as
//! Appendix C defines them, plus the user id counter
//! `sprig.next_user_id`; format 2 adds `api_keys`, and format 3 adds
//! `scram_sha_512` to `verifiers.mechanism`. The other chapter 14 tables
//! (`user_factors`, `token_sessions`, `oidc_issuers`, `claim_mappings`)
//! arrive as later expand migrations with the steps that use them.
//!
//! **Versioning.** `users` and `verifiers` are versioned by the tenant's
//! auth version ([`VersionKind::Auth`](sprig_catalog::VersionKind)).
//! `verifiers` has no `changed_version` column, so every change of a
//! user's verifiers also rewrites the user's row with the change's
//! version ([`AuthStore::touch_user`]); the cache reloads a changed user's
//! verifiers with it. `login_state` is not versioned (chapter 08: read on
//! demand); only lock transitions bump the auth version, and the cache
//! re-reads every locked row on each load.
//!
//! **Times.** `DATETIME(6)` columns hold UTC. They travel as microseconds
//! since the epoch (`TIMESTAMPDIFF` and `TIMESTAMPADD` against
//! `'1970-01-01'`), so no calendar arithmetic happens here and the session
//! time zone doesn't matter.

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mysql_async::prelude::Queryable;
use mysql_async::{Params, Row, Value};
use sprig_catalog::{Counter, Migration, Module, Phase, Store};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::names::{SchemaName, quote};
use sprig_common::{Result, SprigCode, SprigError};

/// The module name in the catalog's format records.
pub const MODULE: &str = "auth";

/// `users` (chapter 14, Appendix C). Exactly the appendix's columns.
pub const DDL_USERS: &str = "CREATE TABLE IF NOT EXISTS users (
  user_id            INT UNSIGNED  NOT NULL,
  name               VARCHAR(255)  NOT NULL COLLATE utf8mb4_bin,
  host               VARCHAR(255)  NOT NULL COLLATE ascii_general_ci DEFAULT '%',
  kind               ENUM('user','service') NOT NULL DEFAULT 'user',
  auth_methods       SET('password','token','api_key') NOT NULL DEFAULT 'password',
  plugin             VARCHAR(64)   NOT NULL DEFAULT 'caching_sha2_password',
  mongo_db           VARCHAR(64)   NULL,
  home_space_id      BIGINT UNSIGNED NOT NULL DEFAULT 1,
  require_tls        ENUM('none','ssl','x509') NOT NULL DEFAULT 'none',
  account_locked     TINYINT(1)    NOT NULL DEFAULT 0,
  password_expired   TINYINT(1)    NOT NULL DEFAULT 0,
  password_changed_at DATETIME(6)  NULL,
  password_lifetime  SMALLINT      NULL,
  password_history   SMALLINT      NULL,
  password_reuse_days SMALLINT     NULL,
  require_current    ENUM('default','yes','optional') NOT NULL DEFAULT 'default',
  failed_login_attempts SMALLINT UNSIGNED NULL,
  password_lock_minutes INT        NULL,
  max_queries_per_hour     INT UNSIGNED NOT NULL DEFAULT 0,
  max_updates_per_hour     INT UNSIGNED NOT NULL DEFAULT 0,
  max_connections_per_hour INT UNSIGNED NOT NULL DEFAULT 0,
  max_user_connections     INT UNSIGNED NOT NULL DEFAULT 0,
  attributes         JSON          NULL,
  source     ENUM('sql','mongo','redis','api','oidc_jit','bootstrap') NOT NULL,
  created_by_tenant  BIGINT UNSIGNED NOT NULL DEFAULT 0,
  state              ENUM('active','dropped') NOT NULL DEFAULT 'active',
  created_at         DATETIME(6)   NOT NULL,
  changed_version    BIGINT UNSIGNED NOT NULL,
  live_name VARCHAR(255) AS (IF(state = 'dropped', NULL, name)) PERSISTENT,
  PRIMARY KEY (user_id),
  UNIQUE KEY uk_account (live_name, host)
) ENGINE=InnoDB";

/// `verifiers` (chapter 14, Appendix C).
pub const DDL_VERIFIERS: &str = "CREATE TABLE IF NOT EXISTS verifiers (
  verifier_id     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id         INT UNSIGNED    NOT NULL,
  mechanism       ENUM('caching_sha2_password','mysql_native_password',
                       'scram_sha_256','scram_sha_1') NOT NULL,
  slot            ENUM('primary','secondary','history') NOT NULL,
  login_form      ENUM('any','qualified','plain') NOT NULL DEFAULT 'any',
  auth_string     VARBINARY(255)  NULL,
  salt            VARBINARY(64)   NULL,
  iterations      INT UNSIGNED    NULL,
  stored_key      VARBINARY(64)   NULL,
  server_key      VARBINARY(64)   NULL,
  generation      BIGINT UNSIGNED NOT NULL,
  created_at      DATETIME(6)     NOT NULL,
  PRIMARY KEY (verifier_id),
  KEY k_user (user_id, slot, mechanism)
) ENGINE=InnoDB";

/// `login_state` (chapter 14, Appendix C).
pub const DDL_LOGIN_STATE: &str = "CREATE TABLE IF NOT EXISTS login_state (
  user_id              INT UNSIGNED      NOT NULL,
  consecutive_failures INT UNSIGNED      NOT NULL DEFAULT 0,
  locked_until         DATETIME(6)       NULL,
  last_failure_at      DATETIME(6)       NULL,
  last_success_at      DATETIME(6)       NULL,
  last_success_ip      VARBINARY(16)     NULL,
  updated_at           DATETIME(6)       NOT NULL,
  PRIMARY KEY (user_id)
) ENGINE=InnoDB";

/// The user id counter, `sprig.next_user_id`, starting at 1. Idempotent.
pub const INSERT_USER_COUNTER: &str = "INSERT INTO settings
  (scope_kind, scope_id, setting_key, value, bounds, set_by, set_at, version) VALUES
  ('tenant', 0, 'sprig.next_user_id', '1', NULL, 'system', UTC_TIMESTAMP(6), 0)
  ON DUPLICATE KEY UPDATE setting_key = setting_key";

/// The auth module's migrations.
pub static AUTH_MIGRATIONS: &[Migration] = &[
    Migration {
        format: 1,
        phase: Phase::Expand,
        name: "users, verifiers, login_state and the user id counter",
        statements: &[
            DDL_USERS,
            DDL_VERIFIERS,
            DDL_LOGIN_STATE,
            INSERT_USER_COUNTER,
        ],
    },
    Migration {
        format: 2,
        phase: Phase::Expand,
        name: "api_keys",
        statements: &[crate::apikey::DDL_API_KEYS],
    },
    Migration {
        format: 3,
        phase: Phase::Expand,
        name: "verifiers.mechanism gains scram_sha_512",
        statements: &[ALTER_VERIFIERS_SCRAM_SHA_512],
    },
];

/// Format 3: `scram_sha_512` joins `verifiers.mechanism` (chapter 14,
/// "SCRAM verifiers"). A new last `ENUM` member: MariaDB changes only the
/// table's metadata, and rerunning it changes nothing.
pub const ALTER_VERIFIERS_SCRAM_SHA_512: &str = "ALTER TABLE verifiers MODIFY mechanism
  ENUM('caching_sha2_password','mysql_native_password',
       'scram_sha_256','scram_sha_1','scram_sha_512') NOT NULL";

/// The auth module, for [`Migrator::new`](sprig_catalog::Migrator::new).
pub const AUTH_MODULE: Module = Module {
    name: MODULE,
    migrations: AUTH_MIGRATIONS,
};

/// The id counter of users (`sprig.next_user_id`).
pub const USER_COUNTER: Counter = Counter::Other("user");

/// `locked_until` of an `UNBOUNDED` lock: 9999-12-31.
pub const UNBOUNDED_LOCK_MICROS: i64 = 253_402_214_400_000_000;

macro_rules! sql_enum {
    ($(#[$doc:meta])* $name:ident { $($(#[$vdoc:meta])* $variant:ident = $text:literal,)* }) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum $name { $($(#[$vdoc])* $variant,)* }

        impl $name {
            /// Every value, in declaration order.
            pub const ALL: &'static [$name] = &[$($name::$variant),*];

            /// The SQL text.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $($name::$variant => $text,)* }
            }

            /// Parses the SQL text.
            ///
            /// # Errors
            /// `Internal` for any other text.
            pub fn parse(s: &str) -> Result<Self> {
                match s {
                    $($text => Ok($name::$variant),)*
                    _ => Err(SprigError::internal(format!(
                        concat!("auth catalog: bad ", stringify!($name), " {:?}"), s
                    ))),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

sql_enum! {
    /// `users.kind`.
    UserKind {
        /// A person or application logging in with a password.
        User = "user",
        /// An account meant for API keys.
        Service = "service",
    }
}

sql_enum! {
    /// `users.require_tls`: MySQL's `REQUIRE` clause.
    RequireTls {
        /// No requirement.
        None = "none",
        /// `REQUIRE SSL`: the connection must use TLS.
        Ssl = "ssl",
        /// `REQUIRE X509`: TLS with a verified client certificate.
        X509 = "x509",
    }
}

sql_enum! {
    /// `users.require_current`: `PASSWORD REQUIRE CURRENT`.
    RequireCurrent {
        /// From `auth.password_require_current`.
        Default = "default",
        /// Always.
        Yes = "yes",
        /// `PASSWORD REQUIRE CURRENT OPTIONAL`.
        Optional = "optional",
    }
}

sql_enum! {
    /// `users.source`: how the user was created.
    UserSource {
        /// `CREATE USER` over MySQL.
        Sql = "sql",
        /// `createUser` over MongoDB.
        Mongo = "mongo",
        /// `ACL SETUSER` over Redis.
        Redis = "redis",
        /// The HTTP or gRPC API.
        Api = "api",
        /// Just-in-time from an external token.
        OidcJit = "oidc_jit",
        /// Tenant bootstrap (the admin user) or tools.
        Bootstrap = "bootstrap",
    }
}

sql_enum! {
    /// `users.state`. A dropped user is a tombstone: its name is free and
    /// its id never reused.
    UserState {
        /// Can log in (subject to the statuses).
        Active = "active",
        /// Dropped: logs in as an unknown user.
        Dropped = "dropped",
    }
}

sql_enum! {
    /// `verifiers.mechanism`.
    VerifierKind {
        /// `caching_sha2_password`: `$A$RRR$` + salt + SHA-256 crypt.
        CachingSha2 = "caching_sha2_password",
        /// `mysql_native_password`: `*` + HEX(SHA1(SHA1(password))).
        Native = "mysql_native_password",
        /// SCRAM-SHA-256 (RFC 7677).
        ScramSha256 = "scram_sha_256",
        /// SCRAM-SHA-1 (MongoDB's variant).
        ScramSha1 = "scram_sha_1",
        /// SCRAM-SHA-512 (Kafka; format 3).
        ScramSha512 = "scram_sha_512",
    }
}

sql_enum! {
    /// `verifiers.slot`: MySQL's dual passwords plus history.
    Slot {
        /// The current password.
        Primary = "primary",
        /// The retained previous password (`RETAIN CURRENT PASSWORD`);
        /// logins accept it until `DISCARD OLD PASSWORD`.
        Secondary = "secondary",
        /// Earlier passwords, for `PASSWORD HISTORY` and `REUSE INTERVAL`
        /// checks only; never accepted at login.
        History = "history",
    }
}

sql_enum! {
    /// `verifiers.login_form`: which login name a SCRAM-SHA-1 verifier was
    /// computed over (chapter 14, "Verifier formats").
    LoginForm {
        /// Any form (every mechanism but SCRAM-SHA-1).
        Any = "any",
        /// The qualified form, `acme.alice`, of shared endpoints.
        Qualified = "qualified",
        /// The plain form, `alice`, of tenant endpoints.
        Plain = "plain",
    }
}

/// `users.auth_methods`: the login methods an account accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AuthMethods(u8);

impl AuthMethods {
    /// Passwords (every password mechanism).
    pub const PASSWORD: AuthMethods = AuthMethods(1);
    /// Tokens (SprigDB and external).
    pub const TOKEN: AuthMethods = AuthMethods(2);
    /// API keys.
    pub const API_KEY: AuthMethods = AuthMethods(4);
    const NAMES: [(&'static str, AuthMethods); 3] = [
        ("password", AuthMethods::PASSWORD),
        ("token", AuthMethods::TOKEN),
        ("api_key", AuthMethods::API_KEY),
    ];

    /// No method.
    #[must_use]
    pub const fn empty() -> Self {
        AuthMethods(0)
    }

    /// Whether every method of `other` is in `self`.
    #[must_use]
    pub const fn contains(self, other: AuthMethods) -> bool {
        self.0 & other.0 == other.0
    }

    /// Both sets.
    #[must_use]
    pub const fn union(self, other: AuthMethods) -> Self {
        AuthMethods(self.0 | other.0)
    }

    /// The SQL `SET` text.
    #[must_use]
    pub fn to_sql(self) -> String {
        Self::NAMES
            .iter()
            .filter(|(_, m)| self.contains(*m))
            .map(|(n, _)| *n)
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Parses the SQL `SET` text.
    ///
    /// # Errors
    /// `Internal` for an unknown member.
    pub fn parse(s: &str) -> Result<Self> {
        let mut out = AuthMethods::empty();
        for part in s.split(',').filter(|p| !p.is_empty()) {
            let (_, m) = Self::NAMES
                .iter()
                .find(|(n, _)| *n == part)
                .ok_or_else(|| {
                    SprigError::internal(format!("auth catalog: bad auth method {part:?}"))
                })?;
            out = out.union(*m);
        }
        Ok(out)
    }
}

impl Default for AuthMethods {
    fn default() -> Self {
        AuthMethods::PASSWORD
    }
}

/// An account's resource limits (`MAX_QUERIES_PER_HOUR` and the others);
/// 0 means unlimited.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct AccountLimits {
    /// `MAX_QUERIES_PER_HOUR`.
    pub max_queries_per_hour: u32,
    /// `MAX_UPDATES_PER_HOUR`.
    pub max_updates_per_hour: u32,
    /// `MAX_CONNECTIONS_PER_HOUR`.
    pub max_connections_per_hour: u32,
    /// `MAX_USER_CONNECTIONS`.
    pub max_user_connections: u32,
}

/// A `users` row.
#[derive(Debug, Clone, PartialEq)]
pub struct UserRow {
    /// The id; never reused. The backend account is `t<tenant>_u<user_id>`.
    pub user_id: UserId,
    /// The user name, compared exactly.
    pub name: String,
    /// The host pattern, as MySQL writes it (`'%'`, `'10.%'`,
    /// `'10.0.0.0/8'`, ...).
    pub host: String,
    /// User or service account.
    pub kind: UserKind,
    /// Accepted login methods.
    pub auth_methods: AuthMethods,
    /// The account's MySQL plugin (`caching_sha2_password` by default).
    pub plugin: String,
    /// MongoDB authentication database (R5).
    pub mongo_db: Option<String>,
    /// Chapter 42's home space.
    pub home_space_id: u64,
    /// `REQUIRE NONE | SSL | X509`.
    pub require_tls: RequireTls,
    /// `ACCOUNT LOCK`.
    pub account_locked: bool,
    /// `PASSWORD EXPIRE` (the flag; the lifetime is checked separately).
    pub password_expired: bool,
    /// When the password last changed.
    pub password_changed_at: Option<SystemTime>,
    /// `PASSWORD EXPIRE INTERVAL n DAY`: `None` from `auth.*` settings,
    /// `Some(0)` never.
    pub password_lifetime: Option<i16>,
    /// `PASSWORD HISTORY n`.
    pub password_history: Option<i16>,
    /// `PASSWORD REUSE INTERVAL n DAY`.
    pub password_reuse_days: Option<i16>,
    /// `PASSWORD REQUIRE CURRENT`.
    pub require_current: RequireCurrent,
    /// `FAILED_LOGIN_ATTEMPTS n`; `None`: `auth.failed_login_attempts`.
    pub failed_login_attempts: Option<u16>,
    /// `PASSWORD_LOCK_TIME` in minutes; `None`: setting; -1: unbounded.
    pub password_lock_minutes: Option<i32>,
    /// Resource limits.
    pub limits: AccountLimits,
    /// `ATTRIBUTE` / `COMMENT` JSON, for `SPRIG_USER_ATTR()` (chapter 29).
    pub attributes: Option<serde_json::Value>,
    /// How the user was created.
    pub source: UserSource,
    /// 0: created by this tenant; else the ancestor tenant that did.
    pub created_by_tenant: u64,
    /// Active or dropped.
    pub state: UserState,
    /// When created.
    pub created_at: SystemTime,
    /// The auth version of the last change to the row or its verifiers.
    pub changed_version: u64,
}

impl UserRow {
    /// A new active user with every policy column at its default.
    #[must_use]
    pub fn new(
        user_id: UserId,
        name: impl Into<String>,
        host: impl Into<String>,
        source: UserSource,
        created_at: SystemTime,
        changed_version: u64,
    ) -> Self {
        Self {
            user_id,
            name: name.into(),
            host: host.into(),
            kind: UserKind::User,
            auth_methods: AuthMethods::PASSWORD,
            plugin: "caching_sha2_password".to_owned(),
            mongo_db: None,
            home_space_id: 1,
            require_tls: RequireTls::None,
            account_locked: false,
            password_expired: false,
            password_changed_at: None,
            password_lifetime: None,
            password_history: None,
            password_reuse_days: None,
            require_current: RequireCurrent::Default,
            failed_login_attempts: None,
            password_lock_minutes: None,
            limits: AccountLimits::default(),
            attributes: None,
            source,
            created_by_tenant: 0,
            state: UserState::Active,
            created_at,
            changed_version,
        }
    }

    /// The account as MySQL prints it: `'alice'@'%'`.
    #[must_use]
    pub fn account(&self) -> String {
        format!("'{}'@'{}'", self.name, self.host)
    }

    /// Whether the row is live (not a tombstone).
    #[must_use]
    pub fn is_live(&self) -> bool {
        self.state == UserState::Active
    }
}

/// A `verifiers` row. `Debug` hides the secret-derived columns.
#[derive(Clone, PartialEq, Eq)]
pub struct VerifierRow {
    /// The id (`AUTO_INCREMENT`; 0 when inserting), never reused: the fast
    /// cache keys on it.
    pub verifier_id: u64,
    /// The user.
    pub user_id: UserId,
    /// The mechanism.
    pub kind: VerifierKind,
    /// Primary, secondary or history.
    pub slot: Slot,
    /// The login name form (SCRAM-SHA-1 only; else `Any`).
    pub login_form: LoginForm,
    /// The stored string of `caching_sha2_password` and
    /// `mysql_native_password`.
    pub auth_string: Option<Vec<u8>>,
    /// SCRAM salt.
    pub salt: Option<Vec<u8>>,
    /// SCRAM iterations.
    pub iterations: Option<u32>,
    /// SCRAM `StoredKey`.
    pub stored_key: Option<Vec<u8>>,
    /// SCRAM `ServerKey`.
    pub server_key: Option<Vec<u8>>,
    /// Increases with every password change of the user.
    pub generation: u64,
    /// When created.
    pub created_at: SystemTime,
}

impl fmt::Debug for VerifierRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifierRow")
            .field("verifier_id", &self.verifier_id)
            .field("user_id", &self.user_id)
            .field("kind", &self.kind)
            .field("slot", &self.slot)
            .field("login_form", &self.login_form)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl VerifierRow {
    /// A `caching_sha2_password` or `mysql_native_password` verifier with
    /// its stored string.
    #[must_use]
    pub fn mysql(
        user_id: UserId,
        kind: VerifierKind,
        slot: Slot,
        auth_string: Vec<u8>,
        generation: u64,
        created_at: SystemTime,
    ) -> Self {
        Self {
            verifier_id: 0,
            user_id,
            kind,
            slot,
            login_form: LoginForm::Any,
            auth_string: Some(auth_string),
            salt: None,
            iterations: None,
            stored_key: None,
            server_key: None,
            generation,
            created_at,
        }
    }
}

/// A `login_state` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginStateRow {
    /// The user.
    pub user_id: UserId,
    /// Wrong credentials in a row (saturates).
    pub consecutive_failures: u32,
    /// The temporary lock's end; 9999-12-31 for `UNBOUNDED`.
    pub locked_until: Option<SystemTime>,
    /// Last failure.
    pub last_failure_at: Option<SystemTime>,
    /// Last success.
    pub last_success_at: Option<SystemTime>,
    /// The last successful login's address (4 or 16 bytes).
    pub last_success_ip: Option<Vec<u8>>,
    /// Last write.
    pub updated_at: SystemTime,
}

/// A temporary lock as the cache keeps it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TempLock {
    /// The user.
    pub user_id: UserId,
    /// When it ends (9999-12-31: at `ACCOUNT UNLOCK`).
    pub locked_until: SystemTime,
    /// Failures in a row that caused it.
    pub consecutive_failures: u32,
}

/// Typed access to one catalog's auth tables. Every method takes the
/// connection or transaction to run on: a [`Change`](sprig_catalog::Change)'s
/// transaction for writes, a snapshot transaction for cache loads.
#[derive(Debug, Clone)]
pub struct AuthStore {
    schema: String,
    users: String,
    verifiers: String,
    login_state: String,
}

const USER_COLS: &str = "user_id, name, host, kind, auth_methods, plugin, mongo_db, \
    home_space_id, require_tls, account_locked, password_expired, \
    TIMESTAMPDIFF(MICROSECOND, '1970-01-01', password_changed_at), password_lifetime, \
    password_history, password_reuse_days, require_current, failed_login_attempts, \
    password_lock_minutes, max_queries_per_hour, max_updates_per_hour, \
    max_connections_per_hour, max_user_connections, attributes, source, created_by_tenant, \
    state, TIMESTAMPDIFF(MICROSECOND, '1970-01-01', created_at), changed_version";

const VERIFIER_COLS: &str = "verifier_id, user_id, mechanism, slot, login_form, auth_string, \
    salt, iterations, stored_key, server_key, generation, \
    TIMESTAMPDIFF(MICROSECOND, '1970-01-01', created_at)";

/// `TIMESTAMPADD` of a microsecond parameter: a time parameter.
pub(crate) const AT: &str = "TIMESTAMPADD(MICROSECOND, ?, '1970-01-01')";

/// Rows per `IN (...)` list.
const IN_CHUNK: usize = 500;

impl AuthStore {
    /// The auth tables of catalog schema `schema`.
    #[must_use]
    pub fn new(schema: impl Into<String>) -> Self {
        let schema = schema.into();
        let q = quote(&schema);
        Self {
            users: format!("{q}.users"),
            verifiers: format!("{q}.verifiers"),
            login_state: format!("{q}.login_state"),
            schema,
        }
    }

    /// The auth tables of `tenant`'s catalog, `t<tenant>_sprig`.
    #[must_use]
    pub fn for_tenant(tenant: TenantId) -> Self {
        Self::new(SchemaName::catalog(tenant).to_string())
    }

    /// The auth tables next to `store`'s core tables.
    #[must_use]
    pub fn of(store: &Store) -> Self {
        Self::new(store.schema())
    }

    /// The catalog schema.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    // ---- users ----

    /// Reserves the next user id (`sprig.next_user_id`). Not versioned.
    /// Inside a change's transaction the reservation commits or rolls back
    /// with the change, so a failed create leaves no gap; ids of users that
    /// existed (dropped ones included) are never handed out again.
    ///
    /// # Errors
    /// 39072 if the counter row is missing (the auth module isn't
    /// migrated); 39070 or a backend error.
    pub async fn next_user_id<Q: Queryable + Send>(
        &self,
        core: &Store,
        q: &mut Q,
    ) -> Result<UserId> {
        let id = core.next_ids(q, USER_COUNTER, 1).await?;
        u32::try_from(id)
            .map(UserId)
            .map_err(|_| SprigError::internal("user ids exhausted"))
    }

    /// Inserts a user. A live `(name, host)` already in use is MariaDB's
    /// 1062.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn insert_user<Q: Queryable + Send>(&self, q: &mut Q, r: &UserRow) -> Result<()> {
        let sql = format!(
            "INSERT INTO {} (user_id, name, host, kind, auth_methods, plugin, mongo_db, \
             home_space_id, require_tls, account_locked, password_expired, password_changed_at, \
             password_lifetime, password_history, password_reuse_days, require_current, \
             failed_login_attempts, password_lock_minutes, max_queries_per_hour, \
             max_updates_per_hour, max_connections_per_hour, max_user_connections, attributes, \
             source, created_by_tenant, state, created_at, changed_version) VALUES \
             (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {}, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {AT}, ?)",
            self.users,
            opt_at(r.password_changed_at.is_some()),
        );
        let mut p = vec![Value::from(r.user_id.0)];
        p.extend(user_values(r));
        exec(q, sql, p).await
    }

    /// Rewrites every column of the user `r.user_id`. Returns whether the
    /// row exists.
    ///
    /// # Errors
    /// 39070 or a backend error (1062 for a live name in use).
    pub async fn update_user<Q: Queryable + Send>(&self, q: &mut Q, r: &UserRow) -> Result<bool> {
        let sql = format!(
            "UPDATE {} SET name = ?, host = ?, kind = ?, auth_methods = ?, plugin = ?, \
             mongo_db = ?, home_space_id = ?, require_tls = ?, account_locked = ?, \
             password_expired = ?, password_changed_at = {}, password_lifetime = ?, \
             password_history = ?, password_reuse_days = ?, require_current = ?, \
             failed_login_attempts = ?, password_lock_minutes = ?, max_queries_per_hour = ?, \
             max_updates_per_hour = ?, max_connections_per_hour = ?, max_user_connections = ?, \
             attributes = ?, source = ?, created_by_tenant = ?, state = ?, created_at = {AT}, \
             changed_version = ? WHERE user_id = ?",
            self.users,
            opt_at(r.password_changed_at.is_some()),
        );
        let mut p = user_values(r);
        p.push(Value::from(r.user_id.0));
        exec(q, sql, p).await?;
        self.exists(q, r.user_id).await
    }

    /// Sets the user's `changed_version` to `version`, so the cache
    /// reloads it and its verifiers (every verifier change must call it in
    /// the same change). Returns whether the user exists.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn touch_user<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        user: UserId,
        version: u64,
    ) -> Result<bool> {
        exec(
            q,
            format!(
                "UPDATE {} SET changed_version = ? WHERE user_id = ?",
                self.users
            ),
            vec![Value::from(version), Value::from(user.0)],
        )
        .await?;
        self.exists(q, user).await
    }

    async fn exists<Q: Queryable + Send>(&self, q: &mut Q, user: UserId) -> Result<bool> {
        let found: Option<u32> = q
            .exec_first(
                format!("SELECT user_id FROM {} WHERE user_id = ?", self.users),
                (user.0,),
            )
            .await
            .map_err(map_err)?;
        Ok(found.is_some())
    }

    /// The user `id`, live or dropped.
    ///
    /// # Errors
    /// 39070 or a backend error; `Internal` for a malformed row.
    pub async fn user<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        id: UserId,
    ) -> Result<Option<UserRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!("SELECT {USER_COLS} FROM {} WHERE user_id = ?", self.users),
                (id.0,),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().next().map(user_row).transpose()
    }

    /// The live account `'name'@'host'` (host compared as MySQL does,
    /// case-insensitively).
    ///
    /// # Errors
    /// As [`user`](Self::user).
    pub async fn user_by_account<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        name: &str,
        host: &str,
    ) -> Result<Option<UserRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {USER_COLS} FROM {} WHERE live_name = ? AND host = ?",
                    self.users
                ),
                (name, host),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().next().map(user_row).transpose()
    }

    /// Users changed after `since` (every user, dropped ones included, for
    /// `None`), by id.
    ///
    /// # Errors
    /// As [`user`](Self::user).
    pub async fn users_since<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        since: Option<u64>,
    ) -> Result<Vec<UserRow>> {
        let rows: Vec<Row> = match since {
            Some(v) => {
                q.exec(
                    format!(
                        "SELECT {USER_COLS} FROM {} WHERE changed_version > ? ORDER BY user_id",
                        self.users
                    ),
                    (v,),
                )
                .await
            }
            None => {
                q.query(format!(
                    "SELECT {USER_COLS} FROM {} ORDER BY user_id",
                    self.users
                ))
                .await
            }
        }
        .map_err(map_err)?;
        rows.into_iter().map(user_row).collect()
    }

    // ---- verifiers ----

    /// Inserts a verifier and returns its id.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn insert_verifier<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        r: &VerifierRow,
    ) -> Result<u64> {
        let sql = format!(
            "INSERT INTO {} (user_id, mechanism, slot, login_form, auth_string, salt, iterations, \
             stored_key, server_key, generation, created_at) VALUES \
             (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {AT})",
            self.verifiers
        );
        let p = vec![
            Value::from(r.user_id.0),
            Value::from(r.kind.as_str()),
            Value::from(r.slot.as_str()),
            Value::from(r.login_form.as_str()),
            Value::from(r.auth_string.clone()),
            Value::from(r.salt.clone()),
            Value::from(r.iterations),
            Value::from(r.stored_key.clone()),
            Value::from(r.server_key.clone()),
            Value::from(r.generation),
            Value::from(micros(r.created_at)),
        ];
        exec(q, sql, p).await?;
        let id: Option<u64> = q
            .exec_first("SELECT LAST_INSERT_ID()", ())
            .await
            .map_err(map_err)?;
        id.filter(|&id| id > 0)
            .ok_or_else(|| SprigError::internal("verifier insert returned no id"))
    }

    /// The verifiers of `users`, by user, slot and id.
    ///
    /// # Errors
    /// As [`user`](Self::user).
    pub async fn verifiers_of<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        users: &[UserId],
    ) -> Result<Vec<VerifierRow>> {
        let mut out = Vec::new();
        for chunk in users.chunks(IN_CHUNK) {
            let marks = vec!["?"; chunk.len()].join(", ");
            let rows: Vec<Row> = q
                .exec(
                    format!(
                        "SELECT {VERIFIER_COLS} FROM {} WHERE user_id IN ({marks}) \
                         ORDER BY user_id, slot, verifier_id",
                        self.verifiers
                    ),
                    Params::Positional(chunk.iter().map(|u| Value::from(u.0)).collect()),
                )
                .await
                .map_err(map_err)?;
            for row in rows {
                out.push(verifier_row(row)?);
            }
        }
        Ok(out)
    }

    /// Every verifier of every user (a full cache load).
    ///
    /// # Errors
    /// As [`user`](Self::user).
    pub async fn all_verifiers<Q: Queryable + Send>(&self, q: &mut Q) -> Result<Vec<VerifierRow>> {
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT {VERIFIER_COLS} FROM {} ORDER BY user_id, slot, verifier_id",
                self.verifiers
            ))
            .await
            .map_err(map_err)?;
        rows.into_iter().map(verifier_row).collect()
    }

    /// Deletes the user's verifiers in `slot`. Returns how many.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn delete_verifiers<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        user: UserId,
        slot: Slot,
    ) -> Result<u64> {
        exec_count(
            q,
            format!(
                "DELETE FROM {} WHERE user_id = ? AND slot = ?",
                self.verifiers
            ),
            vec![Value::from(user.0), Value::from(slot.as_str())],
        )
        .await
    }

    /// Moves the user's verifiers from slot `from` to `to`. Returns how
    /// many.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn move_verifiers<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        user: UserId,
        from: Slot,
        to: Slot,
    ) -> Result<u64> {
        exec_count(
            q,
            format!(
                "UPDATE {} SET slot = ? WHERE user_id = ? AND slot = ?",
                self.verifiers
            ),
            vec![
                Value::from(to.as_str()),
                Value::from(user.0),
                Value::from(from.as_str()),
            ],
        )
        .await
    }

    /// Deletes the user's history verifiers beyond the newest `keep`
    /// generations. Returns how many.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn trim_history<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        user: UserId,
        keep: u64,
    ) -> Result<u64> {
        let gens: Vec<u64> = q
            .exec(
                format!(
                    "SELECT DISTINCT generation FROM {} WHERE user_id = ? AND slot = 'history' \
                     ORDER BY generation DESC",
                    self.verifiers
                ),
                (user.0,),
            )
            .await
            .map_err(map_err)?;
        let Some(&cut) = usize::try_from(keep).ok().and_then(|k| gens.get(k)) else {
            return Ok(0);
        };
        exec_count(
            q,
            format!(
                "DELETE FROM {} WHERE user_id = ? AND slot = 'history' AND generation <= ?",
                self.verifiers
            ),
            vec![Value::from(user.0), Value::from(cut)],
        )
        .await
    }

    /// The user's newest verifier generation, 0 if none.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn max_generation<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        user: UserId,
    ) -> Result<u64> {
        let g: Option<Option<u64>> = q
            .exec_first(
                format!(
                    "SELECT MAX(generation) FROM {} WHERE user_id = ?",
                    self.verifiers
                ),
                (user.0,),
            )
            .await
            .map_err(map_err)?;
        Ok(g.flatten().unwrap_or(0))
    }

    // ---- login_state ----

    /// The user's login state, if any was written.
    ///
    /// # Errors
    /// 39070 or a backend error; `Internal` for a malformed row.
    pub async fn login_state<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        user: UserId,
    ) -> Result<Option<LoginStateRow>> {
        self.read_login_state(q, user, "").await
    }

    /// The user's login state, locked for update (`SELECT ... FOR UPDATE`)
    /// in the caller's transaction: the failure counter's read (chapter 14,
    /// "Counter writes").
    ///
    /// # Errors
    /// As [`login_state`](Self::login_state).
    pub async fn login_state_for_update<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        user: UserId,
    ) -> Result<Option<LoginStateRow>> {
        self.read_login_state(q, user, " FOR UPDATE").await
    }

    /// Clears the user's temporary lock and failure count (`ACCOUNT
    /// UNLOCK`, or an expired lock). Returns whether a row changed.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn clear_login_lock<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        user: UserId,
        now: SystemTime,
    ) -> Result<bool> {
        let n = exec_count(
            q,
            format!(
                "UPDATE {} SET locked_until = NULL, consecutive_failures = 0, updated_at = {AT} \
                 WHERE user_id = ? AND (locked_until IS NOT NULL OR consecutive_failures > 0)",
                self.login_state
            ),
            vec![Value::from(micros(now)), Value::from(user.0)],
        )
        .await?;
        Ok(n > 0)
    }

    /// Resets a non-zero failure count of an unlocked user (a successful
    /// login). Returns whether a row changed.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn reset_failures<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        user: UserId,
        now: SystemTime,
    ) -> Result<bool> {
        let n = exec_count(
            q,
            format!(
                "UPDATE {} SET consecutive_failures = 0, updated_at = {AT} \
                 WHERE user_id = ? AND consecutive_failures > 0 AND locked_until IS NULL",
                self.login_state
            ),
            vec![Value::from(micros(now)), Value::from(user.0)],
        )
        .await?;
        Ok(n > 0)
    }

    async fn read_login_state<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        user: UserId,
        suffix: &str,
    ) -> Result<Option<LoginStateRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT user_id, consecutive_failures, \
                     TIMESTAMPDIFF(MICROSECOND, '1970-01-01', locked_until), \
                     TIMESTAMPDIFF(MICROSECOND, '1970-01-01', last_failure_at), \
                     TIMESTAMPDIFF(MICROSECOND, '1970-01-01', last_success_at), \
                     last_success_ip, TIMESTAMPDIFF(MICROSECOND, '1970-01-01', updated_at) \
                     FROM {} WHERE user_id = ?{suffix}",
                    self.login_state
                ),
                (user.0,),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter()
            .next()
            .map(|mut r| {
                Ok(LoginStateRow {
                    user_id: UserId(col(&mut r, 0, "user_id")?),
                    consecutive_failures: col(&mut r, 1, "consecutive_failures")?,
                    locked_until: opt_time(&mut r, 2, "locked_until")?,
                    last_failure_at: opt_time(&mut r, 3, "last_failure_at")?,
                    last_success_at: opt_time(&mut r, 4, "last_success_at")?,
                    last_success_ip: col(&mut r, 5, "last_success_ip")?,
                    updated_at: time(&mut r, 6, "updated_at")?,
                })
            })
            .transpose()
    }

    /// Writes a login state row (step 5's counters use it; tests too).
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn put_login_state<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        r: &LoginStateRow,
    ) -> Result<()> {
        let sql = format!(
            "REPLACE INTO {} (user_id, consecutive_failures, locked_until, last_failure_at, \
             last_success_at, last_success_ip, updated_at) VALUES (?, ?, {}, {}, {}, ?, {AT})",
            self.login_state,
            opt_at(r.locked_until.is_some()),
            opt_at(r.last_failure_at.is_some()),
            opt_at(r.last_success_at.is_some()),
        );
        let p = vec![
            Value::from(r.user_id.0),
            Value::from(r.consecutive_failures),
            opt_micros(r.locked_until),
            opt_micros(r.last_failure_at),
            opt_micros(r.last_success_at),
            Value::from(r.last_success_ip.clone()),
            Value::from(micros(r.updated_at)),
        ];
        exec(q, sql, p).await
    }

    /// Every user with a temporary lock set (expired ones included: the
    /// login unlocks them).
    ///
    /// # Errors
    /// As [`login_state`](Self::login_state).
    pub async fn temp_locks<Q: Queryable + Send>(&self, q: &mut Q) -> Result<Vec<TempLock>> {
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT user_id, TIMESTAMPDIFF(MICROSECOND, '1970-01-01', locked_until), \
                 consecutive_failures FROM {} WHERE locked_until IS NOT NULL",
                self.login_state
            ))
            .await
            .map_err(map_err)?;
        rows.into_iter()
            .map(|mut r| {
                Ok(TempLock {
                    user_id: UserId(col(&mut r, 0, "user_id")?),
                    locked_until: time(&mut r, 1, "locked_until")?,
                    consecutive_failures: col(&mut r, 2, "consecutive_failures")?,
                })
            })
            .collect()
    }
}

/// `TIMESTAMPADD(...)` for a time parameter, or `?` for a NULL one.
pub(crate) fn opt_at(some: bool) -> &'static str {
    if some { AT } else { "?" }
}

/// The parameters of every user column but `user_id`, in `insert_user`'s
/// order.
fn user_values(r: &UserRow) -> Vec<Value> {
    vec![
        Value::from(r.name.as_str()),
        Value::from(r.host.as_str()),
        Value::from(r.kind.as_str()),
        Value::from(r.auth_methods.to_sql()),
        Value::from(r.plugin.as_str()),
        Value::from(r.mongo_db.clone()),
        Value::from(r.home_space_id),
        Value::from(r.require_tls.as_str()),
        Value::from(r.account_locked),
        Value::from(r.password_expired),
        opt_micros(r.password_changed_at),
        Value::from(r.password_lifetime),
        Value::from(r.password_history),
        Value::from(r.password_reuse_days),
        Value::from(r.require_current.as_str()),
        Value::from(r.failed_login_attempts),
        Value::from(r.password_lock_minutes),
        Value::from(r.limits.max_queries_per_hour),
        Value::from(r.limits.max_updates_per_hour),
        Value::from(r.limits.max_connections_per_hour),
        Value::from(r.limits.max_user_connections),
        Value::from(r.attributes.as_ref().map(ToString::to_string)),
        Value::from(r.source.as_str()),
        Value::from(r.created_by_tenant),
        Value::from(r.state.as_str()),
        Value::from(micros(r.created_at)),
        Value::from(r.changed_version),
    ]
}

fn user_row(mut r: Row) -> Result<UserRow> {
    let text = |r: &mut Row, i: usize, n: &str| -> Result<String> {
        let b: Vec<u8> = col(r, i, n)?;
        String::from_utf8(b)
            .map_err(|_| SprigError::internal(format!("auth catalog: {n} is not UTF-8")))
    };
    let attributes: Option<Vec<u8>> = col(&mut r, 22, "attributes")?;
    Ok(UserRow {
        user_id: UserId(col(&mut r, 0, "user_id")?),
        name: text(&mut r, 1, "name")?,
        host: text(&mut r, 2, "host")?,
        kind: UserKind::parse(&text(&mut r, 3, "kind")?)?,
        auth_methods: AuthMethods::parse(&text(&mut r, 4, "auth_methods")?)?,
        plugin: text(&mut r, 5, "plugin")?,
        mongo_db: col::<Option<Vec<u8>>>(&mut r, 6, "mongo_db")?
            .map(|b| String::from_utf8_lossy(&b).into_owned()),
        home_space_id: col(&mut r, 7, "home_space_id")?,
        require_tls: RequireTls::parse(&text(&mut r, 8, "require_tls")?)?,
        account_locked: col::<i64>(&mut r, 9, "account_locked")? != 0,
        password_expired: col::<i64>(&mut r, 10, "password_expired")? != 0,
        password_changed_at: opt_time(&mut r, 11, "password_changed_at")?,
        password_lifetime: col(&mut r, 12, "password_lifetime")?,
        password_history: col(&mut r, 13, "password_history")?,
        password_reuse_days: col(&mut r, 14, "password_reuse_days")?,
        require_current: RequireCurrent::parse(&text(&mut r, 15, "require_current")?)?,
        failed_login_attempts: col(&mut r, 16, "failed_login_attempts")?,
        password_lock_minutes: col(&mut r, 17, "password_lock_minutes")?,
        limits: AccountLimits {
            max_queries_per_hour: col(&mut r, 18, "max_queries_per_hour")?,
            max_updates_per_hour: col(&mut r, 19, "max_updates_per_hour")?,
            max_connections_per_hour: col(&mut r, 20, "max_connections_per_hour")?,
            max_user_connections: col(&mut r, 21, "max_user_connections")?,
        },
        attributes: attributes
            .map(|b| {
                serde_json::from_slice(&b).map_err(|e| {
                    SprigError::internal(format!("auth catalog: bad attributes JSON: {e}"))
                })
            })
            .transpose()?,
        source: UserSource::parse(&text(&mut r, 23, "source")?)?,
        created_by_tenant: col(&mut r, 24, "created_by_tenant")?,
        state: UserState::parse(&text(&mut r, 25, "state")?)?,
        created_at: time(&mut r, 26, "created_at")?,
        changed_version: col(&mut r, 27, "changed_version")?,
    })
}

fn verifier_row(mut r: Row) -> Result<VerifierRow> {
    let text = |r: &mut Row, i: usize, n: &str| -> Result<String> {
        let b: Vec<u8> = col(r, i, n)?;
        String::from_utf8(b)
            .map_err(|_| SprigError::internal(format!("auth catalog: {n} is not UTF-8")))
    };
    Ok(VerifierRow {
        verifier_id: col(&mut r, 0, "verifier_id")?,
        user_id: UserId(col(&mut r, 1, "user_id")?),
        kind: VerifierKind::parse(&text(&mut r, 2, "mechanism")?)?,
        slot: Slot::parse(&text(&mut r, 3, "slot")?)?,
        login_form: LoginForm::parse(&text(&mut r, 4, "login_form")?)?,
        auth_string: col(&mut r, 5, "auth_string")?,
        salt: col(&mut r, 6, "salt")?,
        iterations: col(&mut r, 7, "iterations")?,
        stored_key: col(&mut r, 8, "stored_key")?,
        server_key: col(&mut r, 9, "server_key")?,
        generation: col(&mut r, 10, "generation")?,
        created_at: time(&mut r, 11, "created_at")?,
    })
}

/// Microseconds since the epoch; times before it clamp to 0.
pub(crate) fn micros(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
}

pub(crate) fn opt_micros(t: Option<SystemTime>) -> Value {
    Value::from(t.map(micros))
}

/// The time `m` microseconds after the epoch.
pub(crate) fn from_micros(m: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(u64::try_from(m).unwrap_or(0))
}

pub(crate) fn col<T: mysql_async::prelude::FromValue>(
    r: &mut Row,
    i: usize,
    name: &str,
) -> Result<T> {
    match r.take_opt::<T, usize>(i) {
        Some(Ok(v)) => Ok(v),
        Some(Err(e)) => Err(SprigError::internal(format!(
            "auth catalog column {name}: unexpected value: {e}"
        ))),
        None => Err(SprigError::internal(format!(
            "auth catalog column {name} missing from the result"
        ))),
    }
}

pub(crate) fn time(r: &mut Row, i: usize, name: &str) -> Result<SystemTime> {
    Ok(from_micros(col::<i64>(r, i, name)?))
}

pub(crate) fn opt_time(r: &mut Row, i: usize, name: &str) -> Result<Option<SystemTime>> {
    Ok(col::<Option<i64>>(r, i, name)?.map(from_micros))
}

/// Maps a driver error the way `sprig-catalog` does: lock waits and
/// deadlocks are 39071, other server errors pass through with their number
/// (the driver's text, which names the physical schema, stays in the
/// source), anything else is 39070.
pub(crate) fn map_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => match s.code {
            1205 | 1213 => SprigError::new(SprigCode::CatalogConflict, "catalog row busy; retry"),
            code => {
                let mut state = *b"HY000";
                if s.state.len() == 5 {
                    state.copy_from_slice(s.state.as_bytes());
                }
                SprigError::backend(code, state, format!("catalog statement failed ({code})"))
            }
        }
        .with_source(e),
        _ => SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e),
    }
}

pub(crate) async fn exec<Q: Queryable + Send>(q: &mut Q, sql: String, p: Vec<Value>) -> Result<()> {
    q.exec_drop(sql, Params::Positional(p))
        .await
        .map_err(map_err)
}

pub(crate) async fn exec_count<Q: Queryable + Send>(
    q: &mut Q,
    sql: String,
    p: Vec<Value>,
) -> Result<u64> {
    exec(q, sql, p).await?;
    let n: Option<i64> = q.query_first("SELECT ROW_COUNT()").await.map_err(map_err)?;
    Ok(n.and_then(|n| u64::try_from(n).ok()).unwrap_or(0))
}

#[cfg(test)]
mod tests;
