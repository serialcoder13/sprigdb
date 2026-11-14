//! File accounts: logins from a TOML file, a stopgap from before the
//! tenant catalog held the verifier tables.
//!
//! In this mode the gateway reads `[gateway] accounts_file` and asks an
//! [`AccountStore`] for each login. The file and [`FileAccounts`] go once
//! the gateway reads every login from the catalog.
//!
//! ```toml
//! [[account]]
//! user = "app"
//! # The verifier formats. At least one of the two. A MySQL hash
//! # whose salt holds control characters can be given as "0x" and the hex of
//! # authentication_string (SELECT HEX(authentication_string) ...).
//! caching_sha2_password = "$A$005$<20 salt bytes><43-character digest>"
//! mysql_native_password = "*<40 hex digits>"       # optional
//! cluster = "dc-a-c1"                              # a registered cluster id
//! backend_user = "root"
//! # A credentials_ref: file:<path>[#section] or env:<VAR>[#section].
//! # A relative file path is relative to this file's directory.
//! backend_credentials = "file:backend-creds.toml#app"
//! ```
//!
//! Other top-level tables are ignored, so a development file can carry the
//! backend credential itself (`file:<this file>#<table>`).
//!
//! The file is polled every second ([`FileAccounts::spawn_watcher`]) and a
//! changed file swaps in atomically. A missing file means no accounts, so
//! every login fails with 1045; an invalid one is logged and the previous
//! accounts are kept.

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use arc_swap::ArcSwap;
use serde::Deserialize;
use sprig_common::ids::ClusterId;
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_provision::credentials::{Credential, SecretRef, SecretSource};

use crate::verifier::{CachingSha2Verifier, NativeVerifier, decode_hex};

/// How often [`FileAccounts::spawn_watcher`] re-reads the file by default.
pub const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The longest user name: MariaDB's limit, the larger of the two personas'
/// (32 through 3306, 80 through 3307).
pub const MAX_USER_LEN: usize = 80;

/// Where a login's statements go (one cluster per login).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendTarget {
    /// A cluster registered in the DC tier.
    pub cluster: ClusterId,
    /// The MariaDB account the gateway connects as.
    pub user: String,
    /// The account's credential, by reference; resolved at each connect so
    /// a rotated secret is picked up.
    pub credentials: SecretRef,
}

impl BackendTarget {
    /// Resolves the backend credential. A credential naming no user gets
    /// [`BackendTarget::user`].
    ///
    /// # Errors
    /// 39998 if the secret can't be read or parsed, or names a different
    /// user than `backend_user`. The message never contains the secret.
    pub fn credential(&self) -> Result<Credential> {
        let c = self.credentials.resolve(&self.user)?;
        if c.user != self.user {
            return Err(invalid(format!(
                "backend credential {} is for user {:?}, but backend_user is {:?}",
                self.credentials, c.user, self.user
            )));
        }
        Ok(c)
    }
}

/// One login account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Account {
    /// The login name, as the client sends it (no tenant prefix).
    pub user: String,
    /// The `caching_sha2_password` verifier.
    pub caching_sha2: Option<CachingSha2Verifier>,
    /// The `mysql_native_password` verifier.
    pub native: Option<NativeVerifier>,
    /// Where its statements go.
    pub backend: BackendTarget,
}

impl Account {
    /// Checks a clear-text password (the full path): the
    /// `caching_sha2_password` verifier when there is one, else the native one.
    /// CPU-heavy: call it off the async runtime.
    #[must_use]
    pub fn verify_password(&self, password: &[u8]) -> bool {
        match (&self.caching_sha2, &self.native) {
            (Some(v), _) => v.verify(password),
            (None, Some(n)) => n.verify(password),
            (None, None) => false,
        }
    }

    /// A key that changes whenever the account's verifiers do: the fast
    /// cache's entries are bound to it.
    #[must_use]
    pub fn verifier_key(&self) -> String {
        let sha2 = self
            .caching_sha2
            .as_ref()
            .map(CachingSha2Verifier::to_string)
            .unwrap_or_default();
        let native = self
            .native
            .as_ref()
            .map(NativeVerifier::to_string)
            .unwrap_or_default();
        format!("{sha2}|{native}")
    }
}

/// Answers "which account is this login name?" for the accounts file;
/// catalog logins don't use it.
pub trait AccountStore: Send + Sync {
    /// The account for `user`, or `None`, in which case the front end runs
    /// the decoy exchange and refuses with 1045.
    fn lookup(&self, user: &str) -> Option<Arc<Account>>;
}

/// A fixed set of accounts: for tests and tools.
#[derive(Debug, Default)]
pub struct StaticAccounts(HashMap<String, Arc<Account>>);

impl StaticAccounts {
    /// The accounts, keyed by user. A later duplicate replaces an earlier
    /// one.
    #[must_use]
    pub fn new(accounts: impl IntoIterator<Item = Account>) -> Self {
        Self(
            accounts
                .into_iter()
                .map(|a| (a.user.clone(), Arc::new(a)))
                .collect(),
        )
    }
}

impl AccountStore for StaticAccounts {
    fn lookup(&self, user: &str) -> Option<Arc<Account>> {
        self.0.get(user).cloned()
    }
}

type AccountMap = HashMap<String, Arc<Account>>;

/// What one poll of the accounts file found.
#[derive(Debug)]
pub enum PollOutcome {
    /// The file is as last read.
    Unchanged,
    /// The file changed and its accounts were swapped in.
    Reloaded {
        /// How many accounts it holds.
        accounts: usize,
    },
    /// The file is gone: no accounts now.
    Missing,
    /// The file changed but doesn't parse, or can't be read; the previous
    /// accounts are kept until it changes again.
    Rejected(SprigError),
}

/// The file contents last seen, so a poll can tell a change.
#[derive(Debug, PartialEq, Eq)]
enum Seen {
    Missing,
    Contents(Vec<u8>),
}

/// The accounts from `[gateway] accounts_file`, re-read on change.
pub struct FileAccounts {
    path: PathBuf,
    current: ArcSwap<AccountMap>,
    seen: Mutex<Option<Seen>>,
}

impl fmt::Debug for FileAccounts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileAccounts")
            .field("path", &self.path)
            .field("accounts", &self.len())
            .finish_non_exhaustive()
    }
}

impl FileAccounts {
    /// Loads `path`. A missing file gives no accounts (logged); the file
    /// may appear later.
    ///
    /// # Errors
    /// 39998 if the file exists but can't be read or doesn't parse: at
    /// start-up a broken file is refused rather than silently ignored.
    pub fn open(path: impl Into<PathBuf>) -> Result<Arc<Self>> {
        let store = Arc::new(Self {
            path: path.into(),
            current: ArcSwap::from_pointee(AccountMap::new()),
            seen: Mutex::new(None),
        });
        match store.poll_inner() {
            PollOutcome::Rejected(e) => Err(e),
            PollOutcome::Missing => {
                tracing::warn!(path = %store.path.display(), "accounts file not found: every login is refused until it appears");
                Ok(store)
            }
            PollOutcome::Reloaded { accounts } => {
                tracing::info!(path = %store.path.display(), accounts, "accounts loaded");
                Ok(store)
            }
            PollOutcome::Unchanged => Ok(store),
        }
    }

    /// The file this store reads.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// How many accounts are loaded.
    #[must_use]
    pub fn len(&self) -> usize {
        self.current.load().len()
    }

    /// Whether no accounts are loaded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Re-reads the file and swaps in its accounts if it changed. Blocking
    /// file I/O: [`FileAccounts::spawn_watcher`] runs it on a blocking
    /// thread.
    pub fn poll(&self) -> PollOutcome {
        let outcome = self.poll_inner();
        match &outcome {
            PollOutcome::Reloaded { accounts } => {
                tracing::info!(event = "auth.accounts_reloaded", path = %self.path.display(), accounts, "accounts file reloaded");
            }
            PollOutcome::Missing => {
                tracing::warn!(event = "auth.accounts_reloaded", path = %self.path.display(), accounts = 0, "accounts file removed: every login is refused");
            }
            PollOutcome::Rejected(e) => {
                tracing::error!(event = "auth.accounts_rejected", path = %self.path.display(), error = %e, "accounts file rejected; keeping the previous accounts");
            }
            PollOutcome::Unchanged => {}
        }
        outcome
    }

    fn poll_inner(&self) -> PollOutcome {
        let read = std::fs::read(&self.path);
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        let now = match &read {
            Ok(bytes) => Seen::Contents(bytes.clone()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Seen::Missing,
            Err(e) => {
                return PollOutcome::Rejected(invalid(format!(
                    "accounts file {}: can't read it: {e}",
                    self.path.display()
                )));
            }
        };
        if seen.as_ref() == Some(&now) {
            return PollOutcome::Unchanged;
        }
        let outcome = match &now {
            Seen::Missing => {
                self.current.store(Arc::new(AccountMap::new()));
                PollOutcome::Missing
            }
            Seen::Contents(bytes) => match self.parse(bytes) {
                Ok(map) => {
                    let accounts = map.len();
                    self.current.store(Arc::new(map));
                    PollOutcome::Reloaded { accounts }
                }
                Err(e) => PollOutcome::Rejected(e),
            },
        };
        // A rejected file is remembered too, so it is reported once, not on
        // every poll.
        *seen = Some(now);
        outcome
    }

    fn parse(&self, bytes: &[u8]) -> Result<AccountMap> {
        let text = std::str::from_utf8(bytes)
            .map_err(|_| invalid(format!("accounts file {}: not UTF-8", self.path.display())))?;
        let base = self.path.parent().unwrap_or_else(|| Path::new("."));
        let accounts = parse_accounts(text, base).map_err(|e| {
            invalid(format!(
                "accounts file {}: {}",
                self.path.display(),
                e.message
            ))
        })?;
        Ok(accounts
            .into_iter()
            .map(|a| (a.user.clone(), Arc::new(a)))
            .collect())
    }

    /// Polls every `interval` on a blocking thread until `stop` is
    /// cancelled.
    pub fn spawn_watcher(
        self: &Arc<Self>,
        interval: Duration,
        stop: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let store = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await; // the first tick is immediate; open() just read the file
            loop {
                tokio::select! {
                    () = stop.cancelled() => return,
                    _ = tick.tick() => {}
                }
                let s = Arc::clone(&store);
                if let Err(e) = tokio::task::spawn_blocking(move || s.poll()).await {
                    tracing::error!(error = %e, "accounts file poll failed");
                }
            }
        })
    }
}

impl AccountStore for FileAccounts {
    fn lookup(&self, user: &str) -> Option<Arc<Account>> {
        self.current.load().get(user).cloned()
    }
}

/// One `[[account]]` as written.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAccount {
    user: String,
    #[serde(default)]
    caching_sha2_password: Option<String>,
    #[serde(default)]
    mysql_native_password: Option<String>,
    cluster: String,
    backend_user: String,
    backend_credentials: String,
}

/// The file: `[[account]]` tables; anything else is ignored.
#[derive(Deserialize)]
struct RawFile {
    #[serde(default)]
    account: Vec<RawAccount>,
}

/// Parses an accounts file's text. Relative `file:` credential paths are
/// taken relative to `base`.
///
/// # Errors
/// 39998 for invalid TOML, an unknown field, a bad verifier, cluster id or
/// credentials reference, an account with no verifier, or a duplicate user.
/// Messages never contain verifiers or secrets.
pub fn parse_accounts(text: &str, base: &Path) -> Result<Vec<Account>> {
    let raw: RawFile = toml::from_str(text).map_err(|e: toml::de::Error| {
        invalid(format!("not a valid accounts file: {}", e.message()))
    })?;
    let mut users = std::collections::HashSet::new();
    let mut out = Vec::with_capacity(raw.account.len());
    for (i, r) in raw.account.into_iter().enumerate() {
        let at = |why: String| invalid(format!("account {} ({:?}): {why}", i + 1, r.user));
        if r.user.is_empty() || r.user.len() > MAX_USER_LEN {
            return Err(at(format!("user must be 1 to {MAX_USER_LEN} bytes")));
        }
        if !users.insert(r.user.clone()) {
            return Err(at("duplicate user".to_owned()));
        }
        let caching_sha2 = r
            .caching_sha2_password
            .as_deref()
            .map(parse_sha2_field)
            .transpose()
            .map_err(|e| at(e.message))?;
        let native = r
            .mysql_native_password
            .as_deref()
            .map(NativeVerifier::parse)
            .transpose()
            .map_err(|e| at(e.message))?;
        if caching_sha2.is_none() && native.is_none() {
            return Err(at(
                "needs caching_sha2_password or mysql_native_password".to_owned()
            ));
        }
        let cluster = ClusterId::new(&r.cluster).map_err(|e| at(e.to_string()))?;
        if r.backend_user.is_empty() {
            return Err(at("backend_user is empty".to_owned()));
        }
        let mut credentials: SecretRef = r
            .backend_credentials
            .parse()
            .map_err(|e: SprigError| at(e.message))?;
        if let SecretSource::File(p) = &credentials.source
            && p.is_relative()
        {
            credentials.source = SecretSource::File(base.join(p));
        }
        out.push(Account {
            user: r.user,
            caching_sha2,
            native,
            backend: BackendTarget {
                cluster,
                user: r.backend_user,
                credentials,
            },
        });
    }
    Ok(out)
}

/// A `caching_sha2_password` field: the verifier, or `0x` and its hex.
fn parse_sha2_field(s: &str) -> Result<CachingSha2Verifier> {
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        let mut bytes = vec![0u8; hex.len() / 2];
        decode_hex(hex, &mut bytes)
            .ok_or_else(|| invalid("caching_sha2_password: \"0x\" must be followed by hex"))?;
        CachingSha2Verifier::parse(&bytes)
    } else {
        s.parse()
    }
}

fn invalid(message: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::ConfigInvalid, message)
}

#[cfg(test)]
mod tests;
