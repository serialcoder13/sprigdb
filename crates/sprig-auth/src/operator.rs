//! Platform operators' credentials: the pieces
//! the controller's operator directory builds on, kept here beside the
//! tenant users' verifiers, API keys and second factors they reuse.
//!
//! - **Passwords** are the `caching_sha2_password` verifier
//!   (SHA-256 crypt) at [`PASSWORD_ROUNDS`], ten times a tenant user's
//!   default: operators log in rarely and hold the platform.
//! - **Second factors** are [`crate::mfa`]'s TOTP, with seeds sealed under
//!   a key derived for [`KEY_PURPOSE`] (tenant 0, the operator's number as
//!   the user).
//! - **Tokens** look like tenant API keys: `sprigop_<key_id>_<secret>` for
//!   people (login sessions and the tokens they issue) and
//!   `sprigsvc_<key_id>_<secret>` for service identities. `key_id` is 12
//!   base32 characters, the secret 256 bits as 64 hex digits; only
//!   `SHA256(secret)` is stored.
//! - **Scopes** narrow a token: `<area>:read` or `<area>:write`, where the
//!   area is the first segment of the Admin API path (`clusters`,
//!   `settings`, `recommendations`, ...) and `*` is every area; `write`
//!   includes `read`.

use std::sync::OnceLock;

use sha2::{Digest, Sha256};
use sprig_common::{Result, SprigError};
use subtle::ConstantTimeEq;

use crate::verifier::CachingSha2Verifier;

/// SHA-256 crypt rounds of an operator's password verifier.
pub const PASSWORD_ROUNDS: u32 = 50_000;
/// The shortest operator password.
pub const MIN_PASSWORD_LEN: usize = 12;
/// The prefix of a person's token.
pub const TOKEN_PREFIX: &str = "sprigop_";
/// The prefix of a service identity's token.
pub const SERVICE_PREFIX: &str = "sprigsvc_";
/// The purpose operators' TOTP seed keys are derived for.
pub const KEY_PURPOSE: &[u8] = b"sprig-operator-totp-seed";
/// The longest operator or service name.
pub const MAX_NAME_LEN: usize = 63;

fn random<const N: usize>() -> Result<[u8; N]> {
    let mut b = [0u8; N];
    getrandom::fill(&mut b)
        .map_err(|e| SprigError::internal(format!("the OS random generator failed: {e}")))?;
    Ok(b)
}

/// Whether `name` is a valid operator or service name: a lower-case
/// letter, then up to 62 lower-case letters, digits, `.`, `_` or `-`.
#[must_use]
pub fn valid_name(name: &str) -> bool {
    let b = name.as_bytes();
    !b.is_empty()
        && b.len() <= MAX_NAME_LEN
        && b[0].is_ascii_lowercase()
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || b"._-".contains(c))
}

/// The stored verifier of `password`.
///
/// # Errors
/// 39792 for a password shorter than [`MIN_PASSWORD_LEN`]; `Internal`
/// without randomness.
pub fn hash_password(password: &str) -> Result<String> {
    if password.chars().count() < MIN_PASSWORD_LEN {
        return Err(SprigError::new(
            sprig_common::SprigCode::AdminRequestInvalid,
            format!("an operator password has at least {MIN_PASSWORD_LEN} characters"),
        ));
    }
    Ok(CachingSha2Verifier::create(password.as_bytes(), PASSWORD_ROUNDS)?.to_string())
}

/// Whether `password` matches `stored`. `None` (an unknown operator) does
/// the same work and fails, so timing doesn't tell which names exist.
#[must_use]
pub fn verify_password(stored: Option<&str>, password: &str) -> bool {
    static DECOY: OnceLock<Option<CachingSha2Verifier>> = OnceLock::new();
    if let Some(v) = stored.and_then(|s| s.parse::<CachingSha2Verifier>().ok()) {
        return v.verify(password.as_bytes());
    }
    if let Some(d) = DECOY
        .get_or_init(|| CachingSha2Verifier::create(b"sprig-operator-decoy", PASSWORD_ROUNDS).ok())
    {
        std::hint::black_box(d.verify(password.as_bytes()));
    }
    false
}

/// A new token: the text shown once, its id and the stored hash.
#[derive(Clone, PartialEq, Eq)]
pub struct NewToken {
    /// `sprigop_<key_id>_<secret>` or `sprigsvc_...`.
    pub text: String,
    /// The 12-character id.
    pub key_id: String,
    /// Lower-case hex SHA-256 of the secret.
    pub secret_sha256: String,
}

impl std::fmt::Debug for NewToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NewToken")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter()
        .fold(String::with_capacity(b.len() * 2), |mut s, x| {
            let _ = write!(s, "{x:02x}");
            s
        })
}

/// Lower-case hex SHA-256 of `secret`.
#[must_use]
pub fn secret_hash(secret: &str) -> String {
    hex(&Sha256::digest(secret.as_bytes()))
}

/// Whether `secret` hashes to `stored` (constant time).
#[must_use]
pub fn secret_matches(stored: &str, secret: &str) -> bool {
    bool::from(secret_hash(secret).as_bytes().ct_eq(stored.as_bytes()))
}

/// A fresh token; `service` picks [`SERVICE_PREFIX`].
///
/// # Errors
/// `Internal` without randomness.
pub fn new_token(service: bool) -> Result<NewToken> {
    let key_id = crate::mfa::base32(&random::<8>()?)[..12].to_lowercase();
    let secret = hex(&random::<32>()?);
    let prefix = if service {
        SERVICE_PREFIX
    } else {
        TOKEN_PREFIX
    };
    Ok(NewToken {
        text: format!("{prefix}{key_id}_{secret}"),
        secret_sha256: secret_hash(&secret),
        key_id,
    })
}

/// A presented token's parts: whether it is a service token, its id and
/// its secret. `None` for anything else.
#[must_use]
pub fn parse_token(text: &str) -> Option<(bool, &str, &str)> {
    let (service, rest) = if let Some(r) = text.strip_prefix(TOKEN_PREFIX) {
        (false, r)
    } else {
        (true, text.strip_prefix(SERVICE_PREFIX)?)
    };
    let (id, secret) = rest.split_once('_')?;
    let id_ok = id.len() == 12
        && id
            .bytes()
            .all(|c| c.is_ascii_lowercase() || (b'2'..=b'7').contains(&c));
    let secret_ok = secret.len() == 64 && secret.bytes().all(|c| c.is_ascii_hexdigit());
    (id_ok && secret_ok).then_some((service, id, secret))
}

/// A fresh TOTP seed.
///
/// # Errors
/// `Internal` without randomness.
pub fn new_seed() -> Result<[u8; crate::mfa::SEED_BYTES]> {
    random()
}

/// A random enrolment ticket (shown once) and its stored hash.
///
/// # Errors
/// `Internal` without randomness.
pub fn new_ticket() -> Result<(String, String)> {
    let t = hex(&random::<24>()?);
    let h = secret_hash(&t);
    Ok((t, h))
}

/// One token scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    /// An Admin API area, or `*`.
    pub area: String,
    /// `write` (which includes `read`).
    pub write: bool,
}

impl Scope {
    /// Parses `<area>:read` or `<area>:write`.
    ///
    /// # Errors
    /// 39792 for anything else.
    pub fn parse(text: &str) -> Result<Self> {
        let bad = || {
            SprigError::new(
                sprig_common::SprigCode::AdminRequestInvalid,
                format!(
                    "scope {text:?}: expected <area>:read or <area>:write \
                     (an area such as clusters, settings or recommendations, or *)"
                ),
            )
        };
        let (area, access) = text.trim().split_once(':').ok_or_else(bad)?;
        let area_ok = area == "*"
            || (!area.is_empty()
                && area
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c == b'-' || c == b'_'));
        if !area_ok {
            return Err(bad());
        }
        let write = match access {
            "read" => false,
            "write" => true,
            _ => return Err(bad()),
        };
        Ok(Self {
            area: area.to_owned(),
            write,
        })
    }

    /// Whether this scope allows `write` (or a read) in `area`.
    #[must_use]
    pub fn covers(&self, area: &str, write: bool) -> bool {
        (self.area == "*" || self.area == area) && (self.write || !write)
    }
}

impl std::fmt::Display for Scope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}",
            self.area,
            if self.write { "write" } else { "read" }
        )
    }
}

/// Whether any of `scopes` (texts) allows the call; unparsable entries
/// allow nothing.
#[must_use]
pub fn scopes_cover(scopes: &[String], area: &str, write: bool) -> bool {
    scopes
        .iter()
        .filter_map(|s| Scope::parse(s).ok())
        .any(|s| s.covers(area, write))
}

/// The scope area of an Admin API path: its first segment after
/// `/admin/v1/`, without a `:method` suffix (`/admin/v1/dcs:latency` is
/// `dcs`).
#[must_use]
pub fn area_of(path: &str) -> &str {
    let rest = path
        .strip_prefix("/admin/v1/")
        .unwrap_or_else(|| path.trim_start_matches('/'));
    let seg = rest.split(['/', '?']).next().unwrap_or("");
    seg.split(':').next().unwrap_or("")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn names() {
        assert!(valid_name("alice"));
        assert!(valid_name("ops.oncall-1"));
        assert!(!valid_name("Alice"));
        assert!(!valid_name("1alice"));
        assert!(!valid_name(""));
        assert!(!valid_name("a b"));
        assert!(!valid_name(&"a".repeat(64)));
    }

    #[test]
    fn passwords() {
        assert!(hash_password("short").is_err());
        let v = hash_password("correct horse battery").unwrap();
        assert!(v.starts_with("$A$032$"), "{v}");
        assert!(verify_password(Some(&v), "correct horse battery"));
        assert!(!verify_password(Some(&v), "correct horse batterY"));
        assert!(!verify_password(None, "correct horse battery"));
        assert!(!verify_password(Some("garbage"), "x"));
    }

    #[test]
    fn tokens_round_trip() {
        let t = new_token(false).unwrap();
        assert!(t.text.starts_with("sprigop_"));
        let (svc, id, secret) = parse_token(&t.text).unwrap();
        assert!(!svc);
        assert_eq!(id, t.key_id);
        assert!(secret_matches(&t.secret_sha256, secret));
        assert!(!secret_matches(&t.secret_sha256, &secret.replace('a', "b")));
        let s = new_token(true).unwrap();
        assert!(parse_token(&s.text).unwrap().0);
        assert!(parse_token("sprig_acme_abc_def").is_none());
        assert!(parse_token("sprigop_short_00").is_none());
        assert!(parse_token(&format!("{}x", t.text)).is_none());
        assert_ne!(new_token(false).unwrap().key_id, t.key_id);
    }

    #[test]
    fn scopes() {
        assert!(Scope::parse("clusters:read").is_ok());
        assert!(Scope::parse("*:write").is_ok());
        assert!(Scope::parse("clusters").is_err());
        assert!(Scope::parse("clusters:admin").is_err());
        assert!(Scope::parse("Clusters:read").is_err());
        let s = vec!["clusters:write".to_owned(), "settings:read".to_owned()];
        assert!(scopes_cover(&s, "clusters", true));
        assert!(scopes_cover(&s, "settings", false));
        assert!(!scopes_cover(&s, "settings", true));
        assert!(!scopes_cover(&s, "tenants", false));
        assert!(scopes_cover(&["*:read".into()], "tenants", false));
        assert!(!scopes_cover(&["*:read".into()], "tenants", true));
        assert!(!scopes_cover(&["bad".into()], "tenants", false));
    }

    #[test]
    fn areas() {
        assert_eq!(area_of("/admin/v1/clusters/dc-a-c1:probe"), "clusters");
        assert_eq!(area_of("/admin/v1/dcs:latency"), "dcs");
        assert_eq!(area_of("/admin/v1/recommendations:bulk"), "recommendations");
        assert_eq!(area_of("/admin/v1/settings?prefix=x"), "settings");
        assert_eq!(area_of("/admin/v1/operator-tokens"), "operator-tokens");
    }
}
