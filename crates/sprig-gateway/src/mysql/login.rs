//! The gateway's [`LoginFlow`]: chapter 14's MySQL login decisions over the
//! interim accounts (decision D1) and the fast cache.
//!
//! - **Account lookup** by the login name as sent. A name longer than the
//!   persona allows (32 on 3306, 80 on 3307) or one that isn't UTF-8 is
//!   treated as unknown.
//! - **Unknown accounts** take the decoy: [`LoginFlow::method`] picks
//!   `caching_sha2_password` as for a real account, the fast path runs a
//!   scramble check against a dummy value, the full path runs a dummy
//!   SHA-256 crypt at the default cost ([`sprig_auth::verifier::decoy_verify`]),
//!   and the verdict is 1045 with the same message as a wrong password.
//! - **Full-path hashing** runs on the blocking pool, at most
//!   `gateway.auth_cpu_slots` at once (CPUs / 2; a fixed size until the key
//!   is registered), and fills the fast cache on success.
//! - **Policy** (chapter 14 step 3): the accounts file has no locks, expiry
//!   or `REQUIRE SSL` yet, so a verified credential is accepted.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::LazyLock;

use async_trait::async_trait;
use sprig_auth::verifier::{FastCacheValue, decoy_verify, verify_native_scramble, verify_scramble};
use sprig_auth::{Account, AccountStore, FastCache};
use sprig_proto_mysql::auth::{AuthPath, AuthVerdict, LoginFlow, Method, Outcome};
use sprig_proto_mysql::handshake::NONCE_LEN;
use sprig_proto_mysql::{ErrPacket, Persona};
use tokio::sync::Semaphore;
use zeroize::Zeroizing;

use crate::session::Principal;

/// The fast-path value unknown accounts are checked against, so a probe
/// costs the same scramble check as a real account.
static DECOY_STAGE2: LazyLock<FastCacheValue> =
    LazyLock::new(|| FastCacheValue::from_password(b"sprig-decoy"));

/// What every login shares: the account source, the fast cache and the
/// hashing slots.
pub struct LoginDeps {
    /// Where accounts come from (`FileAccounts` in M0.2).
    pub accounts: Arc<dyn AccountStore>,
    /// The `caching_sha2_password` fast cache.
    pub fast_cache: Arc<FastCache>,
    /// Concurrent full-path hashes (`gateway.auth_cpu_slots`).
    pub hash_slots: Arc<Semaphore>,
}

impl std::fmt::Debug for LoginDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoginDeps")
            .field("fast_cache", &self.fast_cache)
            .finish_non_exhaustive()
    }
}

impl LoginDeps {
    /// Deps with `gateway.auth_cpu_slots` at its default, CPUs / 2 (at
    /// least 1).
    #[must_use]
    pub fn new(accounts: Arc<dyn AccountStore>, fast_cache: Arc<FastCache>) -> Self {
        let cpus = std::thread::available_parallelism().map_or(2, std::num::NonZero::get);
        Self {
            accounts,
            fast_cache,
            hash_slots: Arc::new(Semaphore::new((cpus / 2).max(1))),
        }
    }
}

/// One login conversation.
pub struct AccountLogin {
    deps: Arc<LoginDeps>,
    persona: Persona,
    /// The login name as sent (lossy UTF-8, for messages).
    user: String,
    host: String,
    account: Option<Arc<Account>>,
    used_password: bool,
}

impl std::fmt::Debug for AccountLogin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountLogin")
            .field("user", &self.user)
            .field("host", &self.host)
            .field("known", &self.account.is_some())
            .finish_non_exhaustive()
    }
}

/// The longest user name a persona accepts (chapter 14).
fn max_user_len(persona: Persona) -> usize {
    match persona {
        Persona::MySql84 => 32,
        Persona::MariaDb => 80,
    }
}

impl AccountLogin {
    /// Starts a login for `user` (as sent) from `peer`.
    #[must_use]
    pub fn new(deps: Arc<LoginDeps>, persona: Persona, user: &[u8], peer: SocketAddr) -> Self {
        let name = String::from_utf8_lossy(user).into_owned();
        let account = std::str::from_utf8(user)
            .ok()
            .filter(|u| !u.is_empty() && u.chars().count() <= max_user_len(persona))
            .and_then(|u| deps.accounts.lookup(u));
        Self {
            deps,
            persona,
            user: name,
            host: peer.ip().to_string(),
            account,
            used_password: false,
        }
    }

    /// The account, once [`LoginFlow::decide`] accepted.
    #[must_use]
    pub fn account(&self) -> Option<&Arc<Account>> {
        self.account.as_ref()
    }

    /// The session principal for the accepted account.
    #[must_use]
    pub fn principal(&self) -> Option<Principal> {
        self.account.as_ref().map(|a| Principal {
            user: a.user.clone(),
            cluster: a.backend.cluster.clone(),
        })
    }

    fn denied(&self, extra: Option<&str>) -> ErrPacket {
        let mut e =
            ErrPacket::access_denied(self.persona, &self.user, &self.host, self.used_password);
        if let Some(extra) = extra {
            e = ErrPacket::new(e.code, e.sqlstate, format!("{}; {extra}", e.message));
        }
        e
    }
}

#[async_trait]
impl LoginFlow for AccountLogin {
    fn method(&self, client_plugin: Option<&str>) -> Method {
        if client_plugin == Some(Method::ClearText.plugin_name()) {
            return Method::ClearText;
        }
        let Some(a) = &self.account else {
            return Method::CachingSha2;
        };
        match (&a.caching_sha2, &a.native) {
            (_, Some(_)) if client_plugin == Some(Method::Native.plugin_name()) => Method::Native,
            (None, Some(_)) => Method::Native,
            _ => Method::CachingSha2,
        }
    }

    fn check_fast(&mut self, nonce: &[u8; NONCE_LEN], scramble: &[u8]) -> bool {
        self.used_password |= !scramble.is_empty();
        match &self.account {
            Some(a) if a.caching_sha2.is_some() => {
                self.deps.fast_cache.verify_scramble(a, nonce, scramble)
            }
            _ => {
                let _ = verify_scramble(nonce, scramble, &DECOY_STAGE2);
                false
            }
        }
    }

    fn check_native(&mut self, nonce: &[u8; NONCE_LEN], scramble: &[u8]) -> bool {
        self.used_password |= !scramble.is_empty();
        match self.account.as_ref().and_then(|a| a.native.as_ref()) {
            Some(v) => verify_native_scramble(nonce, scramble, v),
            None => false,
        }
    }

    async fn check_password(&mut self, password: Zeroizing<Vec<u8>>) -> bool {
        self.used_password |= !password.is_empty();
        let account = self.account.clone();
        if password.is_empty() {
            // The empty password's verifier is empty: no hashing either way.
            return account.is_some_and(|a| a.verify_password(&password));
        }
        let Ok(_slot) = self.deps.hash_slots.clone().acquire_owned().await else {
            return false;
        };
        let cache = Arc::clone(&self.deps.fast_cache);
        tokio::task::spawn_blocking(move || match account {
            Some(a) => {
                let ok = a.verify_password(&password);
                if ok && a.caching_sha2.is_some() {
                    cache.fill(&a, &password);
                }
                ok
            }
            None => decoy_verify(&password),
        })
        .await
        .unwrap_or(false)
    }

    async fn decide(&mut self, outcome: Outcome, path: AuthPath) -> AuthVerdict {
        let accepted = outcome == Outcome::Verified && self.account.is_some();
        metrics::counter!(
            "sprig_mysql_auth_total",
            "persona" => self.persona.as_str(),
            "path" => if self.account.is_some() { path.as_str() } else { "decoy" },
            "outcome" => if accepted { "ok" } else { "refused" },
        )
        .increment(1);
        if accepted {
            tracing::debug!(user = %self.user, host = %self.host, path = path.as_str(), "login accepted");
            return AuthVerdict::Accept;
        }
        tracing::info!(event = "auth.login_failed", user = %self.user, host = %self.host, path = path.as_str(), reason = "bad_credentials", "login refused");
        AuthVerdict::Refuse(match outcome {
            Outcome::NeedsTls => self.denied(Some(
                "caching_sha2_password needs TLS for a full authentication \
                 (RSA key exchange is not supported): connect with TLS",
            )),
            _ => self.denied(None),
        })
    }
}

#[cfg(test)]
mod tests;
