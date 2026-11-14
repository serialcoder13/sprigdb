//! SASL on the AMQP listeners onto the gateway's logins.
//!
//! `PLAIN` and `AMQPLAIN` run the same login as HTTP Basic and Kafka's
//! PLAIN: the fast cache, then the `caching_sha2_password` verifiers, then
//! the policy order, lockout counters, failure delays and audit. Passwords
//! may be API keys or JWTs where the login accepts them.
//!
//! The tenant comes from the TLS server name (under
//! `listener.tenant_sni_suffixes`), else a `tenant.user` name, else
//! `listener.amqp.tenant`: a plain name on a bound listener is logged in
//! as `<tenant>.<name>`. SASL runs before `connection.open` names the
//! vhost, so the vhost can't name the tenant.
//!
//! `EXTERNAL` takes the user name from a verified client certificate
//! (`amqp.cert_login_field`); only an account that asks for a certificate
//! may log in that way, without a password.

use std::sync::Arc;

use async_trait::async_trait;
use sprig_auth::LoginSource;
use sprig_auth::account::parse_login;
use sprig_auth::mysql::{LoginRequest, LoginService, MysqlLogin, Principal as AuthPrincipal};
use sprig_auth::store::LoginForm;
use sprig_common::ids::DcId;
use sprig_proto_amqp::auth::{AuthFailure, ConnInfo, Credentials, Identity, Principal};
use sprig_proto_mysql::Persona;
use sprig_proto_mysql::auth::{AuthPath, AuthVerdict, LoginFlow, Outcome};
use sprig_settings::{LiveSettings, SettingCtx};
use zeroize::Zeroizing;

/// Logins for the AMQP listeners.
pub struct AmqpIdentity {
    svc: Arc<LoginService>,
    live: LiveSettings,
    dc: DcId,
}

impl std::fmt::Debug for AmqpIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpIdentity").finish_non_exhaustive()
    }
}

impl AmqpIdentity {
    /// Logins through `svc`.
    #[must_use]
    pub fn new(svc: Arc<LoginService>, live: LiveSettings, dc: DcId) -> Self {
        Self { svc, live, dc }
    }

    /// The login name the login sees: `user` as sent, or
    /// `<listener tenant>.<user>` when the listener is bound to a tenant
    /// and neither the server name nor the user name names one.
    fn login_name(&self, c: &ConnInfo, user: &str) -> String {
        let bound = self
            .live
            .get(&super::LISTENER_TENANT, &SettingCtx::process(&self.dc));
        if bound.is_empty() || user.contains('.') {
            return user.to_owned();
        }
        let sni_names_tenant = match (self.svc.source(), c.sni.as_deref()) {
            (LoginSource::Catalog(cl), Some(sni)) => {
                parse_login(user, Some(sni), &cl.sni_suffixes(), 255)
                    .is_some_and(|p| matches!(p.form, LoginForm::Plain))
            }
            _ => false,
        };
        if sni_names_tenant {
            user.to_owned()
        } else {
            format!("{bound}.{user}")
        }
    }
}

/// The user name a client certificate's `subject` (MySQL's form,
/// `/O=Acme/CN=acme.alice`) gives `EXTERNAL`, per `amqp.cert_login_field`:
/// `common_name`, its last `CN`; `distinguished_name`, the whole name as
/// `RabbitMQ` prints it (RFC 4514 order, `CN=acme.alice,O=Acme`).
/// `None` when the certificate has no common name.
pub(super) fn cert_login(subject: &str, field: &str) -> Option<String> {
    let parts: Vec<&str> = subject.split('/').filter(|p| !p.is_empty()).collect();
    if field == "distinguished_name" {
        let rdns: Vec<String> = parts.iter().rev().map(|p| p.replace(',', "\\,")).collect();
        return (!rdns.is_empty()).then(|| rdns.join(","));
    }
    parts
        .iter()
        .rev()
        .find_map(|p| p.strip_prefix("CN="))
        .filter(|cn| !cn.is_empty())
        .map(str::to_owned)
}

fn principal(p: &AuthPrincipal) -> Principal {
    Principal {
        tenant_id: p.tenant.map_or(0, |t| t.0),
        tenant: p.tenant_name.clone().unwrap_or_default(),
        user: p.user.clone(),
        user_id: p.user_id.map(|u| u64::from(u.0)),
        handle: Some(Arc::new(p.clone())),
    }
}

async fn decide(
    mut login: MysqlLogin,
    outcome: Outcome,
    path: AuthPath,
) -> Result<Principal, AuthFailure> {
    let verdict = login.decide(outcome, path).await;
    match (&verdict, login.principal()) {
        (AuthVerdict::Accept, Some(p)) if p.tenant.is_some() => Ok(principal(p)),
        (AuthVerdict::Accept, _) => Err(AuthFailure(
            "AMQP needs a tenant account (catalog login mode)".into(),
        )),
        (AuthVerdict::Refuse(p), _) => Err(AuthFailure(format!(
            "{}: {}",
            login.refusal_reason().unwrap_or("bad_credentials"),
            p.message
        ))),
    }
}

#[async_trait]
impl Identity for AmqpIdentity {
    async fn login(&self, c: &ConnInfo, creds: Credentials) -> Result<Principal, AuthFailure> {
        if let Err(e) = self.svc.admit(c.peer.ip()) {
            return Err(AuthFailure(e.to_string()));
        }
        let client_dn = c
            .client_dn
            .clone()
            .map(|(subject, issuer)| sprig_auth::mysql::ClientDn { subject, issuer });
        let request = |name: String| LoginRequest {
            persona: Persona::MySql84,
            login_name: name.into_bytes(),
            sni: c.sni.clone(),
            client_ip: c.peer.ip(),
            tls: c.tls,
            client_cert: client_dn.is_some(),
            client_dn: client_dn.clone(),
            can_handle_expired_password: false,
        };
        let (user, password) = match creds {
            Credentials::Password { user, password, .. } => (user, password),
            // RabbitMQ's EXTERNAL (rabbitmq_auth_mechanism_ssl): the
            // certificate names the user. Only an account that asks for a
            // certificate (`REQUIRE X509`, `SUBJECT`, `ISSUER`) may log in
            // without a password, and its clauses are checked as on every
            // login.
            Credentials::External { login } => {
                let name = self.login_name(c, &login);
                let l = self.svc.start_http(request(name)).await;
                let outcome = if l.certificate_account() {
                    Outcome::Verified
                } else {
                    Outcome::WrongCredentials
                };
                return decide(l, outcome, AuthPath::Full).await;
            }
        };
        let name = self.login_name(c, &user);
        let mut login = self.svc.start_http(request(name)).await;
        let (outcome, path) = if login.check_clear_fast(&password) {
            (Outcome::Verified, AuthPath::Fast)
        } else if login.check_password(Zeroizing::new(password)).await {
            (Outcome::Verified, AuthPath::Full)
        } else {
            (Outcome::WrongCredentials, AuthPath::Full)
        };
        decide(login, outcome, path).await
    }
}

#[cfg(test)]
mod cert_tests {
    use super::cert_login;

    #[test]
    fn certificate_names_give_external_its_user() {
        assert_eq!(
            cert_login("/O=Acme/CN=acme.alice", "common_name").as_deref(),
            Some("acme.alice")
        );
        assert_eq!(
            cert_login("/O=Acme/CN=acme.alice", "distinguished_name").as_deref(),
            Some("CN=acme.alice,O=Acme")
        );
        assert_eq!(cert_login("/O=Acme", "common_name"), None);
        assert_eq!(
            cert_login("/O=A, Inc/CN=x", "distinguished_name").as_deref(),
            Some("CN=x,O=A\\, Inc")
        );
    }
}
