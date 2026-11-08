//! `sprig-auth`: principals, verifiers, privileges and backend-grant derivation.
//!
//! Layer: 2 data layer. Design: [chapter 14](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/14-identity.md) (Identity) and [chapter 29](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/29-authorization.md) (Authorization) of the SprigDB low-level design.
//!
//! What exists:
//! - [`verifier`]: chapter 14 build step 2's password verifiers for the
//!   MySQL mechanisms: `caching_sha2_password` (`$A$RRR$` + salt +
//!   SHA-256 crypt) and `mysql_native_password` (`*` + SHA1(SHA1)), the
//!   fast-path value and both scramble checks, the decoy check;
//! - [`fast_cache`]: the gateway's `caching_sha2_password` fast cache;
//! - [`accounts`]: the interim M0.2 account source (decision D1): an
//!   [`AccountStore`] over `[gateway] accounts_file`, re-read on change.
//!
//! Not yet: SCRAM verifiers and `SASLprep` (step 2's other half), the catalog
//! tables, tenant resolution, the login flows, policies, tokens (steps 1 and
//! 3-19), and all of chapter 29.

pub mod accounts;
mod crypt;
pub mod fast_cache;
pub mod verifier;

pub use accounts::{Account, AccountStore, BackendTarget, FileAccounts, StaticAccounts};
pub use fast_cache::FastCache;
pub use verifier::{
    CachingSha2Verifier, FastCacheValue, NativeVerifier, PasswordVerifiers, verify_native_scramble,
    verify_scramble,
};
