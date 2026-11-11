//! `sprig-auth`: principals, verifiers, privileges and backend-grant derivation.
//!
//! Layer: 2 data layer. Design: [chapter 14](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/14-identity.md) (Identity) and [chapter 29](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/29-authorization.md) (Authorization) of the SprigDB low-level design.
//!
//! `sprig-auth` depends on `sprig-catalog` (same layer, chapter 08: "never
//! the reverse"): its tables are a catalog module and its cached state is
//! a catalog extension.
//!
//! What exists:
//! - [`store`]: chapter 14 step 1's catalog tables (`users`, `verifiers`
//!   with dual passwords and history, `login_state`) as the `auth`
//!   migration module, with typed rows and access;
//! - [`cache`]: [`AuthExtension`], the catalog extension that loads
//!   [`TenantAuth`] (accounts in matching order, login verifiers,
//!   temporary locks) into every tenant snapshot, incrementally by auth
//!   version;
//! - [`account`]: step 3's tenant resolution (dotted login names, SNI on
//!   tenant endpoints, the global tier's directory, suspended ancestors)
//!   and account matching by host;
//! - [`mysql`]: step 4's MySQL login flows: [`MysqlLogin`] implements
//!   chapter 05's `LoginFlow` over the catalog (or the interim accounts
//!   file), with the fast cache, the decoy, the policy order (temporary
//!   lock, credentials, suspension, `ACCOUNT LOCK`, TLS requirements,
//!   expiry and sandbox mode) and the backend target;
//! - [`admin`]: creating users with passwords, setting passwords, locking,
//!   expiring and dropping, each one auth change;
//! - [`verifier`]: step 2's `caching_sha2_password` and
//!   `mysql_native_password` verifiers, scramble checks and the decoy;
//! - [`scram`]: step 2's SCRAM-SHA-256 and SCRAM-SHA-512 verifiers and
//!   `SASLprep`, stored with every password and backfilled at logins that
//!   see the clear password;
//! - [`fast_cache`]: the `caching_sha2_password` fast cache;
//! - [`settings`]: the `auth.*` keys read so far;
//! - [`policy`]: step 5's lockout (failure counters, temporary locks,
//!   automatic unlock), password rules (strength, history, reuse,
//!   `REQUIRE CURRENT`) and account clauses within the tree's bounds;
//! - [`admin::AccountOps`]: step 6's audited account changes on behalf of
//!   an actor, possibly of an ancestor tenant;
//! - [`ratelimit`]: step 7's per-address limit and failure delays;
//! - [`revoke`]: step 8's auth change feed (revocation, session closing);
//! - [`handoff`]: step 9's residency handoff, as a library;
//! - [`audit`]: step 10's audit events;
//! - [`http`] and [`apikey`]: step 11's HTTP Basic and API keys, as a
//!   library for the HTTP front end;
//! - [`accounts`]: the interim M0.2 account source (decision D1), kept
//!   behind [`LoginMode`] until the gateway is rewired;
//! - `testing` (feature `testing`): helpers for other crates' tests.
//!
//! Not yet: SCRAM-SHA-1 verifiers (MongoDB, step 15), the 7403 transport
//! of handoffs, tokens, second factors and the other protocols (12-19).

pub mod account;
pub mod accounts;
pub mod admin;
pub mod apikey;
pub mod audit;
pub mod authz;
pub mod cache;
mod crypt;
pub mod fast_cache;
pub mod handoff;
pub mod http;
pub mod mfa;
pub mod mysql;
pub mod operator;
pub mod policy;
pub mod portable;
pub mod ratelimit;
pub mod revoke;
pub mod scram;
pub mod settings;
pub mod store;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
pub mod token;
pub mod verifier;

pub use account::{
    MetaTenants, StaticTenants, TenantDirectory, TenantInfo, TenantResidency, parse_login,
};
pub use accounts::{Account, AccountStore, BackendTarget, FileAccounts, StaticAccounts};
pub use cache::{AuthExtension, AuthUser, TenantAuth};
pub use fast_cache::{FastCache, FastKey};
pub use mysql::{
    AuthCatalogs, BackendResolver, BackendSpec, CatalogLogins, LOGIN_DELAY_MS, LoginMode,
    LoginRequest, LoginService, LoginSource, MysqlLogin, PolicySettings, Principal, SettingsPolicy,
    StaticBackends,
};
pub use scram::{ScramKind, ScramVerifier};
pub use store::{AUTH_MODULE, AuthStore, UserRow, VerifierRow};
pub use verifier::{
    CachingSha2Verifier, FastCacheValue, NativeVerifier, PasswordVerifiers, verify_native_scramble,
    verify_scramble,
};

#[cfg(test)]
mod live_tests;
