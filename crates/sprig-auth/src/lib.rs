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
//! - [`fast_cache`]: the `caching_sha2_password` fast cache;
//! - [`settings`]: the `auth.*` keys read so far;
//! - [`accounts`]: the interim M0.2 account source (decision D1), kept
//!   behind [`LoginMode`] until the gateway is rewired;
//! - `testing` (feature `testing`): helpers for other crates' tests.
//!
//! Not yet: SCRAM verifiers and `SASLprep` (step 2's other half), lockout
//! counting and automatic unlock, password policies and history checks
//! (step 5), management from ancestors (6), rate limits (7), revocation and
//! session closing (8), residency handoff (9), audit (10), HTTP, tokens,
//! second factors and the other protocols (11-19), and all of chapter 29.

pub mod account;
pub mod accounts;
pub mod admin;
pub mod apikey;
pub mod audit;
pub mod authz;
pub mod cache;
mod crypt;
pub mod fast_cache;
pub mod mysql;
pub mod policy;
pub mod ratelimit;
pub mod revoke;
pub mod settings;
pub mod store;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
pub mod verifier;

pub use account::{MetaTenants, StaticTenants, TenantDirectory, TenantInfo, parse_login};
pub use accounts::{Account, AccountStore, BackendTarget, FileAccounts, StaticAccounts};
pub use cache::{AuthExtension, AuthUser, TenantAuth};
pub use fast_cache::{FastCache, FastKey};
pub use mysql::{
    AuthCatalogs, BackendResolver, BackendSpec, CatalogLogins, LoginMode, LoginRequest,
    LoginService, LoginSource, MysqlLogin, PolicySettings, Principal, SettingsPolicy,
    StaticBackends,
};
pub use store::{AUTH_MODULE, AuthStore, UserRow, VerifierRow};
pub use verifier::{
    CachingSha2Verifier, FastCacheValue, NativeVerifier, PasswordVerifiers, verify_native_scramble,
    verify_scramble,
};

#[cfg(test)]
mod live_tests;
