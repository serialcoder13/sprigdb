//! The SASL layer both AMQP versions share (chapter 50, "AMQP 0-9-1" and
//! "AMQP 1.0"): mechanisms `PLAIN`, `AMQPLAIN` (0-9-1 only) and
//! `EXTERNAL`; `ANONYMOUS` is never accepted. The gateway implements
//! [`Identity`] over chapter 14's logins.

use std::any::Any;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;

use crate::v091::codec::{FieldTable, FieldValue};

/// The connection, as a login needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnInfo {
    /// The client address (after a PROXY header).
    pub peer: SocketAddr,
    /// TLS at the gateway.
    pub tls: bool,
    /// The TLS server name: it can name the tenant (chapter 14).
    pub sni: Option<String>,
    /// The verified client certificate's login name (its common name or
    /// distinguished name, per `amqp.cert_login_field`), for `EXTERNAL`.
    pub cert_login: Option<String>,
}

/// Who logged in.
#[derive(Clone)]
pub struct Principal {
    /// The tenant id.
    pub tenant_id: u64,
    /// The tenant name.
    pub tenant: String,
    /// The user name, without a tenant prefix.
    pub user: String,
    /// The user's id in the tenant catalog.
    pub user_id: Option<u64>,
    /// The identity layer's own principal (chapter 14's), for the engine's
    /// privilege checks.
    pub handle: Option<Arc<dyn Any + Send + Sync>>,
}

impl std::fmt::Debug for Principal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Principal")
            .field("tenant_id", &self.tenant_id)
            .field("tenant", &self.tenant)
            .field("user", &self.user)
            .field("user_id", &self.user_id)
            .finish_non_exhaustive()
    }
}

/// Credentials from a SASL exchange.
#[derive(Clone, PartialEq, Eq)]
pub enum Credentials {
    /// `PLAIN` or `AMQPLAIN`: a user name and a password (or API key or
    /// JWT, chapter 14).
    Password {
        /// The mechanism, for metrics and audit.
        mechanism: &'static str,
        /// The user name as sent.
        user: String,
        /// The secret.
        password: Vec<u8>,
    },
    /// `EXTERNAL`: the client certificate's login name.
    External {
        /// From [`ConnInfo::cert_login`].
        login: String,
    },
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Credentials::Password {
                mechanism, user, ..
            } => f
                .debug_struct("Password")
                .field("mechanism", mechanism)
                .field("user", user)
                .finish_non_exhaustive(),
            Credentials::External { login } => {
                f.debug_struct("External").field("login", login).finish()
            }
        }
    }
}

impl Credentials {
    /// The mechanism name.
    #[must_use]
    pub fn mechanism(&self) -> &'static str {
        match self {
            Credentials::Password { mechanism, .. } => mechanism,
            Credentials::External { .. } => "EXTERNAL",
        }
    }
}

/// A refused login; the text is logged, not sent (clients get RabbitMQ's
/// fixed refusal text).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct AuthFailure(pub String);

/// Logins (chapter 14) for the AMQP listeners.
#[async_trait]
pub trait Identity: Send + Sync + 'static {
    /// Checks credentials: the tenant from the TLS server name, a
    /// `tenant.user` name, or the listener's bound tenant.
    async fn login(&self, c: &ConnInfo, creds: Credentials) -> Result<Principal, AuthFailure>;
}

/// Parses a SASL `PLAIN` response: `[authzid] NUL authcid NUL passwd`. An
/// authorization id other than empty or the user is refused.
#[must_use]
pub fn parse_plain(resp: &[u8]) -> Option<(String, Vec<u8>)> {
    let mut parts = resp.splitn(3, |b| *b == 0);
    let authz = parts.next()?;
    let user = parts.next()?;
    let pass = parts.next()?;
    if user.is_empty() || (!authz.is_empty() && authz != user) {
        return None;
    }
    Some((String::from_utf8(user.to_vec()).ok()?, pass.to_vec()))
}

/// Parses an `AMQPLAIN` response: field-table entries without the length
/// prefix, `LOGIN` and `PASSWORD` as long strings.
#[must_use]
pub fn parse_amqplain(resp: &[u8]) -> Option<(String, Vec<u8>)> {
    let t = FieldTable::decode_entries(resp, 0).ok()?;
    let get = |k: &str| match t.get(k)? {
        FieldValue::LongStr(b) | FieldValue::Bytes(b) => Some(b.to_vec()),
        _ => None,
    };
    let user = String::from_utf8(get("LOGIN")?).ok()?;
    if user.is_empty() {
        return None;
    }
    Some((user, get("PASSWORD")?))
}

/// The credentials a mechanism's response carries, or `None` when the
/// mechanism isn't offered or the response is malformed (both refused as
/// a failed login).
#[must_use]
pub fn credentials(
    mechanism: &str,
    response: &[u8],
    offered: &[String],
    info: &ConnInfo,
) -> Option<Credentials> {
    if !offered.iter().any(|m| m.eq_ignore_ascii_case(mechanism)) {
        return None;
    }
    match mechanism.to_ascii_uppercase().as_str() {
        "PLAIN" => parse_plain(response).map(|(user, password)| Credentials::Password {
            mechanism: "PLAIN",
            user,
            password,
        }),
        "AMQPLAIN" => parse_amqplain(response).map(|(user, password)| Credentials::Password {
            mechanism: "AMQPLAIN",
            user,
            password,
        }),
        "EXTERNAL" => {
            let login = info.cert_login.clone()?;
            // An authorization id, if sent, must name the same user.
            (response.is_empty() || response == login.as_bytes())
                .then_some(Credentials::External { login })
        }
        _ => None,
    }
}

/// The mechanisms to offer: the configured ones, minus `EXTERNAL` without
/// a client certificate and minus `ANONYMOUS` always; `AMQPLAIN` only on
/// 0-9-1.
#[must_use]
pub fn offered(configured: &[String], info: &ConnInfo, amqp10: bool) -> Vec<String> {
    configured
        .iter()
        .map(|m| m.trim().to_ascii_uppercase())
        .filter(|m| match m.as_str() {
            "PLAIN" => true,
            "AMQPLAIN" => !amqp10,
            "EXTERNAL" => info.cert_login.is_some(),
            _ => false,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    fn info(cert: Option<&str>) -> ConnInfo {
        ConnInfo {
            peer: "127.0.0.1:1".parse().unwrap(),
            tls: true,
            sni: None,
            cert_login: cert.map(str::to_owned),
        }
    }

    #[test]
    fn plain() {
        assert_eq!(
            parse_plain(b"\0acme.app\0secret"),
            Some(("acme.app".into(), b"secret".to_vec()))
        );
        assert_eq!(parse_plain(b"u\0u\0p"), Some(("u".into(), b"p".to_vec())));
        assert_eq!(parse_plain(b"other\0u\0p"), None);
        assert_eq!(parse_plain(b"\0u"), None);
        assert_eq!(parse_plain(b"\0\0p"), None);
        // A NUL inside the password is kept.
        assert_eq!(
            parse_plain(b"\0u\0p\0q"),
            Some(("u".into(), b"p\0q".to_vec()))
        );
    }

    #[test]
    fn amqplain() {
        let t = FieldTable::from([
            ("LOGIN", FieldValue::str("guest")),
            ("PASSWORD", FieldValue::str("pw")),
        ]);
        let mut b = BytesMut::new();
        t.encode_entries(&mut b);
        assert_eq!(parse_amqplain(&b), Some(("guest".into(), b"pw".to_vec())));
        assert_eq!(parse_amqplain(b"\x05LOGIN"), None);
    }

    #[test]
    fn mechanisms() {
        let cfg: Vec<String> = ["PLAIN", "AMQPLAIN", "EXTERNAL", "ANONYMOUS"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        assert_eq!(offered(&cfg, &info(None), false), ["PLAIN", "AMQPLAIN"]);
        assert_eq!(
            offered(&cfg, &info(Some("app")), true),
            ["PLAIN", "EXTERNAL"]
        );
        let o = offered(&cfg, &info(Some("app")), false);
        assert!(credentials("ANONYMOUS", b"", &o, &info(Some("app"))).is_none());
        assert_eq!(
            credentials("EXTERNAL", b"", &o, &info(Some("app"))),
            Some(Credentials::External {
                login: "app".into()
            })
        );
        assert!(credentials("EXTERNAL", b"x", &o, &info(Some("app"))).is_none());
        assert!(credentials("PLAIN", b"\0u\0p", &["AMQPLAIN".into()], &info(None)).is_none());
    }
}
