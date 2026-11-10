//! Connection profiles (chapter 53, "Profiles").
//!
//! `profiles.toml` in the config directory holds named profiles and the
//! default's name. It never holds a secret: those are in [`crate::secrets`].

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Result, ShellCode, ShellError};

/// How a profile signs in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AuthKind {
    /// A password login: a token session, refreshed by the shell.
    #[default]
    Password,
    /// A session token (`sprigat_...`) given by the user.
    Token,
    /// An API key (`sprig_...`).
    ApiKey,
}

/// When the TUI asks before running a statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Confirm {
    /// Destructive statements only.
    #[default]
    Destructive,
    /// Every write, DDL and account statement too.
    Writes,
    /// Never (the CLI's `--yes` rule still applies).
    Off,
}

/// One profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Profile {
    /// The gateway's HTTP listener, `http://host:8080` or `https://host:8443`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// The login name, as for Studio.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// How to sign in.
    pub auth: AuthKind,
    /// The default database.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    /// The tenant to act in (`x-sprig-tenant`, chapter 29's `USE TENANT`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// Allow password logins over plain HTTP to another host.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub allow_insecure_http: bool,
    /// Extra trust roots (PEM) for https.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<String>,
    /// `sprigctl --endpoint`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub admin_url: Option<String>,
    /// `sprigctl --token-file`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub admin_token_file: Option<String>,
    /// Native MySQL endpoint, `host:port`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mysql: Option<String>,
    /// Native MongoDB endpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mongo: Option<String>,
    /// Native Redis endpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redis: Option<String>,
    /// Native Bolt endpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bolt: Option<String>,
    /// Row cap per statement in the TUI (default 10,000).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<u64>,
    /// Statement timeout in milliseconds sent with each statement.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    /// Dashboard refresh in seconds, clamped to 1–5.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_seconds: Option<u64>,
    /// History entries kept.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history_size: Option<usize>,
    /// When the TUI asks before running a statement.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirm: Option<Confirm>,
}

impl Profile {
    /// The dashboard refresh interval, clamped to 1–5 s.
    #[must_use]
    pub fn refresh_seconds(&self) -> u64 {
        self.refresh_seconds.unwrap_or(2).clamp(1, 5)
    }
}

/// Where secrets go.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SecretsKind {
    /// The OS keychain.
    #[default]
    Keychain,
    /// `secrets.toml`, mode 0600.
    File,
}

/// `profiles.toml`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct ProfileFile {
    /// The profile used without `-p`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    /// Where secrets go.
    #[serde(skip_serializing_if = "is_keychain")]
    pub secrets: SecretsKind,
    /// The profiles by name.
    pub profiles: BTreeMap<String, Profile>,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_keychain(k: &SecretsKind) -> bool {
    *k == SecretsKind::Keychain
}

/// The config directory: `$SPRIG_CONFIG_DIR`, `$XDG_CONFIG_HOME/sprig` or
/// `~/.config/sprig`.
#[must_use]
pub fn config_dir() -> PathBuf {
    if let Some(d) = std::env::var_os("SPRIG_CONFIG_DIR") {
        return PathBuf::from(d);
    }
    if let Some(d) = std::env::var_os("XDG_CONFIG_HOME") {
        return PathBuf::from(d).join("sprig");
    }
    home().join(".config").join("sprig")
}

/// The home directory.
#[must_use]
pub fn home() -> PathBuf {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map_or_else(|| PathBuf::from("."), PathBuf::from)
}

/// `~/x` expanded.
#[must_use]
pub fn expand(p: &str) -> PathBuf {
    match p.strip_prefix("~/") {
        Some(rest) => home().join(rest),
        None => PathBuf::from(p),
    }
}

fn invalid(msg: String) -> ShellError {
    ShellError::shell(ShellCode::ProfileInvalid, msg)
}

/// A profile name: letters, digits, `-`, `_` and `.`, at most 64.
///
/// # Errors
/// 39980 for anything else.
pub fn check_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));
    if ok {
        Ok(())
    } else {
        Err(invalid(format!(
            "profile name {name:?}: use letters, digits, '-', '_' or '.', at most 64"
        )))
    }
}

impl ProfileFile {
    /// The file's path in `dir`.
    #[must_use]
    pub fn path(dir: &Path) -> PathBuf {
        dir.join("profiles.toml")
    }

    /// Reads `dir/profiles.toml`; a missing file is an empty one.
    ///
    /// # Errors
    /// 39980 for an unreadable or invalid file.
    pub fn load(dir: &Path) -> Result<Self> {
        let p = Self::path(dir);
        let text = match std::fs::read_to_string(&p) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(e) => return Err(invalid(format!("{}: {e}", p.display()))),
        };
        Self::parse(&text).map_err(|e| invalid(format!("{}: {}", p.display(), e.message)))
    }

    /// Parses the file's text.
    ///
    /// # Errors
    /// 39980 for invalid TOML or unknown keys.
    pub fn parse(text: &str) -> Result<Self> {
        let f: Self = toml::from_str(text).map_err(|e| invalid(e.to_string()))?;
        for name in f.profiles.keys() {
            check_name(name)?;
        }
        Ok(f)
    }

    /// Writes `dir/profiles.toml` (creating `dir`, mode 0700).
    ///
    /// # Errors
    /// 39980 when it can't be written.
    pub fn save(&self, dir: &Path) -> Result<()> {
        let text = toml::to_string_pretty(self).map_err(|e| invalid(e.to_string()))?;
        crate::secrets::write_private_dir(dir).map_err(|e| invalid(e.message))?;
        let p = Self::path(dir);
        let tmp = p.with_extension("toml.tmp");
        std::fs::write(&tmp, text)
            .and_then(|()| std::fs::rename(&tmp, &p))
            .map_err(|e| invalid(format!("{}: {e}", p.display())))
    }

    /// The profile named `name`, or the default one, or an empty profile
    /// called "default" when there are none.
    ///
    /// # Errors
    /// 39980 for a name that isn't there.
    pub fn resolve(&self, name: Option<&str>) -> Result<(String, Profile)> {
        let name = name
            .map(str::to_owned)
            .or_else(|| std::env::var("SPRIG_PROFILE").ok())
            .or_else(|| self.default.clone());
        match name {
            Some(n) => self
                .profiles
                .get(&n)
                .map(|p| (n.clone(), p.clone()))
                .ok_or_else(|| {
                    invalid(format!(
                        "no profile {n:?} (have: {})",
                        self.profiles.keys().cloned().collect::<Vec<_>>().join(", ")
                    ))
                }),
            None => match self.profiles.iter().next() {
                Some((n, p)) if self.profiles.len() == 1 => Ok((n.clone(), p.clone())),
                _ => Ok(("default".to_owned(), Profile::default())),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
default = "dev"

[profiles.dev]
url = "http://127.0.0.1:8080"
user = "acme.alice"
auth = "password"
database = "shop"
max_rows = 500

[profiles.ci]
url = "https://gw:8443"
auth = "api_key"
"#;

    #[test]
    fn parse_resolve_and_round_trip() {
        let f = ProfileFile::parse(SAMPLE).expect("parse");
        let (n, p) = f.resolve(None).expect("default");
        assert_eq!(n, "dev");
        assert_eq!(p.user.as_deref(), Some("acme.alice"));
        assert_eq!(p.max_rows, Some(500));
        let (_, ci) = f.resolve(Some("ci")).expect("ci");
        assert_eq!(ci.auth, AuthKind::ApiKey);
        assert!(f.resolve(Some("nope")).is_err());
        let again = ProfileFile::parse(&toml::to_string_pretty(&f).expect("ser")).expect("re");
        assert_eq!(again, f);
    }

    #[test]
    fn refuses_secrets_and_bad_names() {
        let e = ProfileFile::parse("[profiles.dev]\npassword = \"x\"\n").expect_err("secret");
        assert_eq!(e.code, 39980);
        assert!(ProfileFile::parse("[profiles.\"a b\"]\n").is_err());
    }

    #[test]
    fn save_and_load() {
        let dir = std::env::temp_dir().join(format!("sprig-prof-{}", std::process::id()));
        let f = ProfileFile::parse(SAMPLE).expect("parse");
        f.save(&dir).expect("save");
        assert_eq!(ProfileFile::load(&dir).expect("load"), f);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
