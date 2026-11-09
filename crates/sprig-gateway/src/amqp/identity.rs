//! SASL on the AMQP listeners onto chapter 14's logins (chapter 50,
//! "AMQP 0-9-1"; chapter 14).
//!
//! `PLAIN` and `AMQPLAIN` run the same login as HTTP Basic and Kafka's
//! PLAIN: the fast cache, then the `caching_sha2_password` verifiers, then
//! the policy order, lockout counters, failure delays and audit. Passwords
//! may be API keys or JWTs where chapter 14's login accepts them.
//!
//! The tenant comes from the TLS server name (under
//! `listener.tenant_sni_suffixes`), else a `tenant.user` name, else
//! `listener.amqp.tenant`: a plain name on a bound listener is logged in
//! as `<tenant>.<name>`. SASL runs before `connection.open` names the
//! vhost, so the vhost can't name the tenant.
//!
//! `EXTERNAL` is refused: listeners don't request client certificates
//! yet, and chapter 37's certificate-to-account mapping isn't built.

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

    /// The login name chapter 14 sees: `user` as sent, or
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
        let (user, password) = match creds {
            Credentials::Password { user, password, .. } => (user, password),
            Credentials::External { .. } => {
                return Err(AuthFailure(
                    "EXTERNAL is not available: no client certificate mapping".into(),
                ));
            }
        };
        let name = self.login_name(c, &user);
        let mut login = self
            .svc
            .start_http(LoginRequest {
                persona: Persona::MySql84,
                login_name: name.into_bytes(),
                sni: c.sni.clone(),
                client_ip: c.peer.ip(),
                tls: c.tls,
                client_cert: c.cert_login.is_some(),
                can_handle_expired_password: false,
            })
            .await;
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
