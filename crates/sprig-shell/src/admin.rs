//! Operator mode (chapter 53, "Operator mode"): `sprig admin ...` runs
//! `sprigctl` with the same arguments, so every command, flag and output is
//! `sprigctl`'s own; the profile supplies `--endpoint` and `--token-file`
//! through `SPRIGCTL_ENDPOINT` and `SPRIGCTL_TOKEN_FILE` unless they are
//! given. Dashboards read the Admin API directly ([`AdminApi`]).

use std::path::{Path, PathBuf};
use std::process::Stdio;

use hyper::Method;
use serde_json::Value;

use crate::api::error_from;
use crate::error::{Result, ShellCode, ShellError};
use crate::http::HttpClient;
use crate::profile::Profile;

/// Where `sprigctl` is: `$SPRIGCTL`, beside this binary, or on the PATH.
///
/// # Errors
/// 39984 when it is nowhere.
pub fn find_sprigctl() -> Result<PathBuf> {
    let name = if cfg!(windows) {
        "sprigctl.exe"
    } else {
        "sprigctl"
    };
    let mut looked = Vec::new();
    if let Some(p) = std::env::var_os("SPRIGCTL") {
        let p = PathBuf::from(p);
        if p.is_file() {
            return Ok(p);
        }
        looked.push(p.display().to_string());
    }
    if let Ok(me) = std::env::current_exe()
        && let Some(dir) = me.parent()
    {
        let p = dir.join(name);
        if p.is_file() {
            return Ok(p);
        }
        looked.push(dir.display().to_string());
    }
    if let Some(path) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path) {
            let p = dir.join(name);
            if p.is_file() {
                return Ok(p);
            }
        }
        looked.push("$PATH".to_owned());
    }
    Err(ShellError::shell(
        ShellCode::SprigctlMissing,
        format!(
            "sprig admin runs sprigctl, which isn't installed (looked in {}); install it \
             beside sprig or set SPRIGCTL",
            looked.join(", ")
        ),
    ))
}

/// The environment `sprigctl` gets from a profile.
#[must_use]
pub fn sprigctl_env(p: &Profile) -> Vec<(&'static str, String)> {
    let mut env = Vec::new();
    if std::env::var_os("SPRIGCTL_ENDPOINT").is_none()
        && let Some(u) = &p.admin_url
    {
        env.push(("SPRIGCTL_ENDPOINT", u.clone()));
    }
    if std::env::var_os("SPRIGCTL_TOKEN_FILE").is_none()
        && let Some(f) = &p.admin_token_file
    {
        env.push((
            "SPRIGCTL_TOKEN_FILE",
            crate::profile::expand(f).display().to_string(),
        ));
    }
    env
}

/// Runs `sprigctl args...` with the terminal; returns its exit code.
///
/// # Errors
/// 39984 when it can't be started.
pub fn run_sprigctl(exe: &Path, args: &[String], p: &Profile) -> Result<u8> {
    let status = std::process::Command::new(exe)
        .args(args)
        .envs(sprigctl_env(p))
        .status()
        .map_err(|e| {
            ShellError::shell(
                ShellCode::SprigctlMissing,
                format!("{}: {e}", exe.display()),
            )
        })?;
    Ok(status
        .code()
        .map_or(1, |c| u8::try_from(c.clamp(0, 255)).unwrap_or(1)))
}

/// Output of one `sprigctl` run, for the Admin tab.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Captured {
    /// Exit code.
    pub code: i32,
    /// Standard output.
    pub stdout: String,
    /// Standard error.
    pub stderr: String,
}

/// Runs `sprigctl args...` capturing its output (the Admin tab). `--yes`
/// is passed only when the user typed it; a command that would ask gets
/// no terminal, so `sprigctl` refuses it rather than hang.
///
/// # Errors
/// 39984 when it can't be started.
pub async fn capture_sprigctl(exe: &Path, args: &[String], p: &Profile) -> Result<Captured> {
    let out = tokio::process::Command::new(exe)
        .args(args)
        .envs(sprigctl_env(p))
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|e| {
            ShellError::shell(
                ShellCode::SprigctlMissing,
                format!("{}: {e}", exe.display()),
            )
        })?;
    Ok(Captured {
        code: out.status.code().unwrap_or(1),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    })
}

/// Splits an Admin tab command line into arguments (shell-like quotes).
///
/// # Errors
/// 39985 for an open quote.
pub fn split_args(line: &str) -> Result<Vec<String>> {
    crate::lang::redis_argv(line)
}

/// The unexpired session token `sprigctl login` keeps for `endpoint`
/// (`$SPRIGCTL_SESSIONS`, default `~/.config/sprigctl/sessions.json`).
#[must_use]
pub fn sprigctl_session(endpoint: &str) -> Option<String> {
    let path = std::env::var_os("SPRIGCTL_SESSIONS")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .map(|h| PathBuf::from(h).join(".config/sprigctl/sessions.json"))
        })?;
    session_in(&std::fs::read(path).ok()?, endpoint)
}

/// The unexpired token for `endpoint` in a sessions file's bytes.
fn session_in(bytes: &[u8], endpoint: &str) -> Option<String> {
    let all: Value = serde_json::from_slice(bytes).ok()?;
    let s = all.get(endpoint.trim_end_matches('/'))?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    let expires = u128::from(s.get("expires_ms")?.as_u64()?);
    (expires > now)
        .then(|| s.get("token")?.as_str().map(str::to_owned))
        .flatten()
}

/// Admin API reads for the dashboards (the same routes `sprigctl` calls).
#[derive(Debug, Clone)]
pub struct AdminApi {
    http: HttpClient,
    token: Option<String>,
}

impl AdminApi {
    /// The Admin API of a profile, if it has one.
    ///
    /// # Errors
    /// 39980 for a bad URL; 39981 for an unreadable token file.
    pub fn of(p: &Profile) -> Result<Option<Self>> {
        let url = std::env::var("SPRIGCTL_ENDPOINT")
            .ok()
            .or_else(|| p.admin_url.clone());
        let Some(url) = url else {
            return Ok(None);
        };
        let token_file = std::env::var("SPRIGCTL_TOKEN_FILE")
            .ok()
            .or_else(|| p.admin_token_file.clone());
        let token = match token_file {
            Some(f) => {
                let path = crate::profile::expand(&f);
                Some(
                    std::fs::read_to_string(&path)
                        .map_err(|e| {
                            ShellError::shell(
                                ShellCode::SecretUnavailable,
                                format!("{}: {e}", path.display()),
                            )
                        })?
                        .trim()
                        .to_owned(),
                )
            }
            // Chapter 14: the operator's login session, kept by `sprig
            // admin login` (`sprigctl login`) for this endpoint.
            None => sprigctl_session(&url),
        };
        let ca = p.ca_file.as_deref().map(crate::profile::expand);
        Ok(Some(Self {
            http: HttpClient::new(&url, ca.as_deref())?,
            token,
        }))
    }

    /// `POST path` with a JSON body on the Admin API.
    ///
    /// # Errors
    /// The API's error, or 39987.
    pub async fn post(&self, path: &str, body: &Value) -> Result<Value> {
        self.call(
            Method::POST,
            path,
            Some(serde_json::to_vec(body).unwrap_or_default()),
        )
        .await
    }

    /// `GET path` on the Admin API.
    ///
    /// # Errors
    /// The API's error, or 39987.
    pub async fn get(&self, path: &str) -> Result<Value> {
        self.call(Method::GET, path, None).await
    }

    async fn call(&self, method: Method, path: &str, body: Option<Vec<u8>>) -> Result<Value> {
        let mut h = vec![("accept", "application/json".to_owned())];
        if let Some(t) = &self.token {
            h.push(("authorization", format!("Bearer {t}")));
        }
        let r = self.http.request(method, path, &h, body).await?;
        let status = r.status;
        let b = r.bytes().await?;
        if !(200..300).contains(&status) {
            return Err(error_from(status, &b));
        }
        if b.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&b)
            .map_err(|e| ShellError::shell(ShellCode::Parse, format!("{path}: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn login_sessions_by_endpoint() {
        let f = br#"{"http://127.0.0.1:7480": {"token": "sprigop_a", "operator": "alice",
            "expires_ms": 99999999999999},
            "http://old:7480": {"token": "sprigop_b", "operator": "bob", "expires_ms": 1}}"#;
        assert_eq!(
            session_in(f, "http://127.0.0.1:7480/").as_deref(),
            Some("sprigop_a")
        );
        assert_eq!(session_in(f, "http://old:7480"), None, "expired");
        assert_eq!(session_in(f, "http://other:7480"), None);
        assert_eq!(session_in(b"not json", "http://127.0.0.1:7480"), None);
    }

    #[test]
    fn env_from_profile() {
        let p = Profile {
            admin_url: Some("http://127.0.0.1:7480".into()),
            admin_token_file: Some("/x/token".into()),
            ..Profile::default()
        };
        if std::env::var_os("SPRIGCTL_ENDPOINT").is_none()
            && std::env::var_os("SPRIGCTL_TOKEN_FILE").is_none()
        {
            assert_eq!(
                sprigctl_env(&p),
                vec![
                    ("SPRIGCTL_ENDPOINT", "http://127.0.0.1:7480".to_owned()),
                    ("SPRIGCTL_TOKEN_FILE", "/x/token".to_owned())
                ]
            );
        }
        assert_eq!(
            split_args("tenant show 'my t'").expect("args"),
            ["tenant", "show", "my t"]
        );
    }
}
