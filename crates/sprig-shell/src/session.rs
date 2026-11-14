//! Signing in: the credential for a profile,
//! refreshing a password login's session under the refresh lock.

use std::path::PathBuf;
use std::time::Duration;

use crate::api::{Auth, DataClient, Tokens};
use crate::error::{Result, ShellCode, ShellError};
use crate::http::HttpClient;
use crate::profile::{AuthKind, Profile, expand};
use crate::secrets::{RefreshLock, Secret, SecretStore, now_ms};

/// An access token valid for less than this is refreshed first.
const MARGIN: Duration = Duration::from_secs(30);
/// How long to wait for another process's refresh.
const LOCK_WAIT: Duration = Duration::from_secs(10);

/// Asks for a password (a terminal prompt in the binary; fixed in tests).
pub type PasswordPrompt<'a> = &'a mut dyn FnMut(&str) -> Option<String>;

/// Everything needed to sign a profile in.
pub struct Signer<'a> {
    /// The profile's name.
    pub name: String,
    /// The profile.
    pub profile: Profile,
    /// Where its secret is.
    pub store: &'a dyn SecretStore,
    /// The config directory (for the lock).
    pub dir: PathBuf,
}

/// The gateway URL of a profile, or the default `http://127.0.0.1:8080`.
#[must_use]
pub fn url_of(p: &Profile) -> String {
    std::env::var("SPRIG_URL")
        .ok()
        .or_else(|| p.url.clone())
        .unwrap_or_else(|| "http://127.0.0.1:8080".to_owned())
}

/// The HTTP client of a profile.
///
/// # Errors
/// 39980 for a bad URL or CA file.
pub fn http_of(p: &Profile) -> Result<HttpClient> {
    let ca = p.ca_file.as_deref().map(expand);
    HttpClient::new(&url_of(p), ca.as_deref())
}

fn auth_error(msg: &str) -> ShellError {
    let mut e = ShellError::server(1045, "", msg);
    e.http_status = 401;
    e
}

impl Signer<'_> {
    fn user(&self) -> Result<String> {
        std::env::var("SPRIG_USER")
            .ok()
            .or_else(|| self.profile.user.clone())
            .ok_or_else(|| {
                ShellError::shell(
                    ShellCode::ProfileInvalid,
                    format!("profile {}: no user (set user = \"...\")", self.name),
                )
            })
    }

    /// Refuses a password over plain HTTP to another host (39988), as the
    /// gateway's 39921 and Studio do.
    ///
    /// # Errors
    /// 39988.
    pub fn check_transport(&self, http: &HttpClient) -> Result<()> {
        let ep = http.endpoint();
        if !ep.tls && !ep.is_loopback() && !self.profile.allow_insecure_http {
            return Err(ShellError::shell(
                ShellCode::InsecureTransport,
                format!(
                    "won't send a password over plain http to {}: use https (8443), or set \
                     allow_insecure_http = true in profile {}",
                    ep.authority(),
                    self.name
                ),
            ));
        }
        Ok(())
    }

    /// The credential for data calls.
    ///
    /// # Errors
    /// 1045 when no session can be had, 39981 for the store, 39988.
    pub async fn auth(&self, http: &HttpClient, prompt: PasswordPrompt<'_>) -> Result<Auth> {
        if let Ok(k) = std::env::var("SPRIG_API_KEY") {
            return Ok(Auth::Bearer(k));
        }
        if let Ok(t) = std::env::var("SPRIG_TOKEN") {
            return Ok(Auth::Bearer(t));
        }
        let env_password = std::env::var("SPRIG_PASSWORD").ok();
        match self.profile.auth {
            AuthKind::ApiKey if env_password.is_none() => {
                let s = self.store.get(&self.name)?;
                s.api_key.map(Auth::Bearer).ok_or_else(|| {
                    auth_error(&format!(
                        "profile {}: no API key stored (sprig login -p {} --api-key)",
                        self.name, self.name
                    ))
                })
            }
            AuthKind::Token if env_password.is_none() => {
                let s = self.store.get(&self.name)?;
                s.token.map(Auth::Bearer).ok_or_else(|| {
                    auth_error(&format!(
                        "profile {}: no token stored (sprig login -p {} --token)",
                        self.name, self.name
                    ))
                })
            }
            _ => self.session(http, env_password, prompt).await,
        }
    }

    async fn session(
        &self,
        http: &HttpClient,
        env_password: Option<String>,
        prompt: PasswordPrompt<'_>,
    ) -> Result<Auth> {
        if let Some(t) = self.store.get(&self.name)?.fresh_access(MARGIN) {
            return Ok(Auth::Bearer(t.to_owned()));
        }
        let _lock = RefreshLock::take(&self.dir, &self.name, LOCK_WAIT)?;
        // Another process may have refreshed while we waited.
        let mut s = self.store.get(&self.name)?;
        if let Some(t) = s.fresh_access(MARGIN) {
            return Ok(Auth::Bearer(t.to_owned()));
        }
        let client = DataClient::new(http.clone(), Auth::None);
        if let Some(rt) = s.refresh_token.clone() {
            match client.refresh(&rt).await {
                Ok(t) => return self.keep(&mut s, &t),
                Err(e) if e.class() == crate::error::ExitClass::Auth => {
                    s.refresh_token = None;
                    s.access_token = None;
                    s.access_expires_ms = None;
                    self.store.set(&self.name, &s)?;
                }
                Err(e) => return Err(e),
            }
        }
        let user = self.user()?;
        let password = match env_password.or_else(|| s.password.clone()) {
            Some(p) => p,
            None => prompt(&format!("Password for {user}: ")).ok_or_else(|| {
                auth_error(&format!(
                    "profile {}: not signed in (run sprig login -p {})",
                    self.name, self.name
                ))
            })?,
        };
        self.check_transport(http)?;
        let t = client.login(&user, &password).await?;
        self.keep(&mut s, &t)
    }

    fn keep(&self, s: &mut Secret, t: &Tokens) -> Result<Auth> {
        s.access_token = Some(t.access_token.clone());
        if !t.refresh_token.is_empty() {
            s.refresh_token = Some(t.refresh_token.clone());
        }
        s.access_expires_ms = Some(now_ms() + t.expires_in.saturating_mul(1000));
        self.store.set(&self.name, s)?;
        Ok(Auth::Bearer(t.access_token.clone()))
    }

    /// `sprig login`: a password login stored as a session (and the
    /// password too with `save_password`).
    ///
    /// # Errors
    /// 1045, 39988, 39981.
    pub async fn login_password(
        &self,
        http: &HttpClient,
        password: &str,
        save_password: bool,
    ) -> Result<()> {
        self.check_transport(http)?;
        let user = self.user()?;
        let _lock = RefreshLock::take(&self.dir, &self.name, LOCK_WAIT)?;
        let t = DataClient::new(http.clone(), Auth::None)
            .login(&user, password)
            .await?;
        let mut s = self.store.get(&self.name)?;
        s.password = save_password.then(|| password.to_owned());
        self.keep(&mut s, &t).map(|_| ())
    }

    /// Stores an API key or a token for the profile.
    ///
    /// # Errors
    /// 39981.
    pub fn store_bearer(&self, api_key: Option<String>, token: Option<String>) -> Result<()> {
        let mut s = self.store.get(&self.name)?;
        if api_key.is_some() {
            s.api_key = api_key;
        }
        if token.is_some() {
            s.token = token;
        }
        self.store.set(&self.name, &s)
    }

    /// `sprig logout`: ends the session on the gateway (best effort) and
    /// forgets every secret of the profile.
    ///
    /// # Errors
    /// 39981.
    pub async fn logout(&self, http: &HttpClient) -> Result<()> {
        let s = self.store.get(&self.name)?;
        if let Some(t) = s.access_token.clone() {
            let _ = DataClient::new(http.clone(), Auth::Bearer(t))
                .logout()
                .await;
        }
        self.store.set(&self.name, &Secret::default())
    }
}

/// The admin endpoint and token file of a profile, for `sprigctl`.
#[must_use]
pub fn admin_of(p: &Profile) -> (Option<String>, Option<PathBuf>) {
    (
        p.admin_url.clone(),
        p.admin_token_file.as_deref().map(expand),
    )
}
