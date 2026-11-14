//! Secrets per profile.
//!
//! The OS keychain (service `sprig`, account = the profile), or
//! `secrets.toml` with mode 0600 where there is none. A [`RefreshLock`]
//! keeps two processes from rotating one session's refresh token at once.

use std::collections::BTreeMap;
use std::fmt;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use serde::{Deserialize, Serialize};

use crate::error::{Result, ShellCode, ShellError};

/// The keychain service name.
pub const SERVICE: &str = "sprig";

/// What is stored for one profile.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Secret {
    /// A saved password (`sprig login --save-password` only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    /// An API key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    /// A session token given by the user.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// The password login's refresh token.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    /// Its current access token.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_token: Option<String>,
    /// When the access token expires, Unix milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_expires_ms: Option<u64>,
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let m = |o: &Option<String>| if o.is_some() { "***" } else { "-" };
        f.debug_struct("Secret")
            .field("password", &m(&self.password))
            .field("api_key", &m(&self.api_key))
            .field("token", &m(&self.token))
            .field("refresh_token", &m(&self.refresh_token))
            .field("access_token", &m(&self.access_token))
            .field("access_expires_ms", &self.access_expires_ms)
            .finish()
    }
}

impl Secret {
    /// Whether nothing is stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// The access token, if it is valid for `margin` more.
    #[must_use]
    pub fn fresh_access(&self, margin: Duration) -> Option<&str> {
        let exp = self.access_expires_ms?;
        let now = now_ms();
        let margin = u64::try_from(margin.as_millis()).unwrap_or(u64::MAX);
        (now.saturating_add(margin) < exp)
            .then_some(())
            .and(self.access_token.as_deref())
    }
}

/// Now, in Unix milliseconds.
#[must_use]
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Where a profile's secret is kept.
pub trait SecretStore: Send + Sync {
    /// The profile's secret; empty when nothing is stored.
    ///
    /// # Errors
    /// 39981 when the store can't be read.
    fn get(&self, profile: &str) -> Result<Secret>;
    /// Replaces the profile's secret (removes it when empty).
    ///
    /// # Errors
    /// 39981 when the store can't be written.
    fn set(&self, profile: &str, secret: &Secret) -> Result<()>;
    /// What this store is, for messages.
    fn describe(&self) -> String;
}

fn unavailable(msg: impl Into<String>) -> ShellError {
    ShellError::shell(ShellCode::SecretUnavailable, msg)
}

/// The OS keychain.
#[derive(Debug, Default)]
pub struct Keychain;

impl SecretStore for Keychain {
    fn get(&self, profile: &str) -> Result<Secret> {
        let entry = keyring::Entry::new(SERVICE, profile)
            .map_err(|e| unavailable(format!("keychain: {e}")))?;
        match entry.get_password() {
            Ok(json) => serde_json::from_str(&json)
                .map_err(|e| unavailable(format!("keychain entry {profile}: {e}"))),
            Err(keyring::Error::NoEntry) => Ok(Secret::default()),
            Err(e) => Err(unavailable(format!(
                "keychain: {e} (set secrets = \"file\" in profiles.toml to use a 0600 file)"
            ))),
        }
    }

    fn set(&self, profile: &str, secret: &Secret) -> Result<()> {
        let entry = keyring::Entry::new(SERVICE, profile)
            .map_err(|e| unavailable(format!("keychain: {e}")))?;
        if secret.is_empty() {
            return match entry.delete_credential() {
                Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
                Err(e) => Err(unavailable(format!("keychain: {e}"))),
            };
        }
        let json = serde_json::to_string(secret).map_err(|e| unavailable(e.to_string()))?;
        entry
            .set_password(&json)
            .map_err(|e| unavailable(format!("keychain: {e}")))
    }

    fn describe(&self) -> String {
        "the OS keychain".to_owned()
    }
}

/// `secrets.toml`, mode 0600.
#[derive(Debug)]
pub struct FileStore {
    path: PathBuf,
    lock: Mutex<()>,
}

impl FileStore {
    /// The store at `dir/secrets.toml`.
    #[must_use]
    pub fn new(dir: &Path) -> Self {
        Self {
            path: dir.join("secrets.toml"),
            lock: Mutex::new(()),
        }
    }

    fn read_all(&self) -> Result<BTreeMap<String, Secret>> {
        let text = match std::fs::read_to_string(&self.path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
            Err(e) => return Err(unavailable(format!("{}: {e}", self.path.display()))),
        };
        check_private(&self.path)?;
        toml::from_str(&text).map_err(|e| unavailable(format!("{}: {e}", self.path.display())))
    }
}

impl SecretStore for FileStore {
    fn get(&self, profile: &str) -> Result<Secret> {
        let _g = self
            .lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(self.read_all()?.remove(profile).unwrap_or_default())
    }

    fn set(&self, profile: &str, secret: &Secret) -> Result<()> {
        let _g = self
            .lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut all = self.read_all()?;
        if secret.is_empty() {
            all.remove(profile);
        } else {
            all.insert(profile.to_owned(), secret.clone());
        }
        let text = toml::to_string_pretty(&all).map_err(|e| unavailable(e.to_string()))?;
        if let Some(dir) = self.path.parent() {
            write_private_dir(dir)?;
        }
        write_private(&self.path, text.as_bytes())
    }

    fn describe(&self) -> String {
        self.path.display().to_string()
    }
}

/// Secrets in memory, for tests and for `SPRIG_*` variables.
#[derive(Debug, Default)]
pub struct Memory(Mutex<BTreeMap<String, Secret>>);

impl SecretStore for Memory {
    fn get(&self, profile: &str) -> Result<Secret> {
        let m = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(m.get(profile).cloned().unwrap_or_default())
    }

    fn set(&self, profile: &str, secret: &Secret) -> Result<()> {
        let mut m = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if secret.is_empty() {
            m.remove(profile);
        } else {
            m.insert(profile.to_owned(), secret.clone());
        }
        Ok(())
    }

    fn describe(&self) -> String {
        "memory".to_owned()
    }
}

/// Creates `dir` (mode 0700 on Unix) if it is missing.
///
/// # Errors
/// 39981 when it can't be created.
pub fn write_private_dir(dir: &Path) -> Result<()> {
    if dir.is_dir() {
        return Ok(());
    }
    let mut b = std::fs::DirBuilder::new();
    b.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut b, 0o700);
    b.create(dir)
        .map_err(|e| unavailable(format!("{}: {e}", dir.display())))
}

/// Writes `path` with mode 0600, through a temporary file and a rename.
///
/// # Errors
/// 39981 when it can't be written.
pub fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    let _ = std::fs::remove_file(&tmp);
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut o, 0o600);
    let r = o
        .open(&tmp)
        .and_then(|mut f| f.write_all(bytes).and_then(|()| f.sync_all()))
        .and_then(|()| std::fs::rename(&tmp, path));
    r.map_err(|e| unavailable(format!("{}: {e}", path.display())))
}

/// Refuses a file that others can read or write (Unix).
///
/// # Errors
/// 39981 when its mode allows group or other access.
pub fn check_private(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta =
            std::fs::metadata(path).map_err(|e| unavailable(format!("{}: {e}", path.display())))?;
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(unavailable(format!(
                "{} has mode {mode:o}; it holds secrets, so it must be 0600 (chmod 600 it)",
                path.display()
            )));
        }
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

/// The store a profile file asks for.
#[must_use]
pub fn store_for(kind: crate::profile::SecretsKind, dir: &Path) -> Box<dyn SecretStore> {
    match kind {
        crate::profile::SecretsKind::Keychain => Box::new(Keychain),
        crate::profile::SecretsKind::File => Box::new(FileStore::new(dir)),
    }
}

/// Held while a profile's refresh token is rotated: `dir/<profile>.lock`, created exclusively, removed on drop.
#[derive(Debug)]
pub struct RefreshLock {
    path: PathBuf,
}

/// A lock file older than this is taken over.
const STALE: Duration = Duration::from_secs(30);

impl RefreshLock {
    /// Takes the lock, waiting up to `wait`.
    ///
    /// # Errors
    /// 39981 when it can't be taken in time.
    pub fn take(dir: &Path, profile: &str, wait: Duration) -> Result<Self> {
        write_private_dir(dir)?;
        let path = dir.join(format!("{profile}.lock"));
        let start = Instant::now();
        loop {
            let mut o = std::fs::OpenOptions::new();
            o.write(true).create_new(true);
            #[cfg(unix)]
            std::os::unix::fs::OpenOptionsExt::mode(&mut o, 0o600);
            match o.open(&path) {
                Ok(mut f) => {
                    let _ = writeln!(f, "{}", std::process::id());
                    return Ok(Self { path });
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    let old = std::fs::metadata(&path)
                        .and_then(|m| m.modified())
                        .ok()
                        .and_then(|t| t.elapsed().ok())
                        .is_some_and(|age| age > STALE);
                    if old {
                        let _ = std::fs::remove_file(&path);
                        continue;
                    }
                    if start.elapsed() > wait {
                        return Err(unavailable(format!(
                            "{} is held by another sprig; remove it if no sprig is running",
                            path.display()
                        )));
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => return Err(unavailable(format!("{}: {e}", path.display()))),
            }
        }
    }
}

impl Drop for RefreshLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("sprig-sec-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn file_store_is_0600_and_round_trips() {
        let d = tmp("file");
        let s = FileStore::new(&d);
        let sec = Secret {
            api_key: Some("sprig_abc".into()),
            ..Secret::default()
        };
        s.set("dev", &sec).expect("set");
        assert_eq!(s.get("dev").expect("get"), sec);
        assert!(s.get("other").expect("get").is_empty());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let p = d.join("secrets.toml");
            let mode = std::fs::metadata(&p).expect("meta").permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).expect("chmod");
            assert_eq!(s.get("dev").expect_err("wide").code, 39981);
        }
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn debug_never_shows_secrets() {
        let sec = Secret {
            password: Some("hunter2".into()),
            ..Secret::default()
        };
        assert!(!format!("{sec:?}").contains("hunter2"));
    }

    #[test]
    fn fresh_access_needs_margin() {
        let mut s = Secret {
            access_token: Some("sprigat_x".into()),
            access_expires_ms: Some(now_ms() + 10_000),
            ..Secret::default()
        };
        assert!(s.fresh_access(Duration::from_secs(5)).is_some());
        assert!(s.fresh_access(Duration::from_secs(30)).is_none());
        s.access_expires_ms = None;
        assert!(s.fresh_access(Duration::ZERO).is_none());
    }

    #[test]
    fn refresh_lock_excludes_and_releases() {
        let d = tmp("lock");
        let a = RefreshLock::take(&d, "dev", Duration::from_millis(10)).expect("first");
        assert!(RefreshLock::take(&d, "dev", Duration::from_millis(100)).is_err());
        drop(a);
        let _b = RefreshLock::take(&d, "dev", Duration::from_millis(10)).expect("again");
        let _ = std::fs::remove_dir_all(&d);
    }
}
