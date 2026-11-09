//! SASL on the Kafka listener onto chapter 14's logins (chapter 45,
//! "Authentication").
//!
//! - **PLAIN** runs the same login as HTTP Basic: the tenant from the TLS
//!   server name under `listener.tenant_sni_suffixes`, else the user name's
//!   prefix (`acme.app`); the fast cache, then the `caching_sha2_password`
//!   verifiers; then the policy order, lockout counters, failure delays and
//!   audit.
//! - **SCRAM-SHA-256 and SCRAM-SHA-512** resolve the account the same way,
//!   take its `scram_sha_256` (or `scram_sha_512`) verifier from the
//!   tenant's auth cache (chapter 14 stores both with every password), run
//!   the exchange, and then the same policy order on the result. With a
//!   retained second password (`RETAIN CURRENT PASSWORD`) sharing the
//!   primary's salt, the proof may match either. An account without such
//!   a verifier, or an unknown one, gets a decoy salt (stable per name) and
//!   fails at the proof, so probes can't tell them apart.
//!
//! Accounts whose password was set before SCRAM verifiers were stored get
//! them at their next successful PLAIN login (or MySQL full or HTTP Basic
//! login): chapter 14's upgrade on login. Logins are labelled `kafka` in
//! metrics and audit events.

use std::sync::Arc;

use async_trait::async_trait;
use sprig_auth::mysql::{LoginRequest, LoginService, MysqlLogin, Principal as AuthPrincipal};
use sprig_auth::scram::{DEFAULT_SCRAM_ITERATIONS, ScramKind};
use sprig_proto_kafka::broker::{AuthFailure, ConnInfo, Identity, ScramFinish, ScramLookup};
use sprig_proto_kafka::sasl::{PlainCredentials, ScramCredential, ScramMechanism};
use sprig_proto_kafka::topics::Principal;
use sprig_proto_mysql::Persona;
use sprig_proto_mysql::auth::{AuthPath, AuthVerdict, LoginFlow, Outcome};
use zeroize::Zeroizing;

/// SCRAM iterations of decoys (chapter 14's default).
const DECOY_ITERATIONS: u32 = DEFAULT_SCRAM_ITERATIONS;

/// Logins for the Kafka listener.
pub struct KafkaIdentity {
    svc: Arc<LoginService>,
    decoy_secret: [u8; 32],
}

impl std::fmt::Debug for KafkaIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaIdentity").finish_non_exhaustive()
    }
}

impl KafkaIdentity {
    /// Logins through `svc`, with SCRAM verifiers from its tenant caches.
    #[must_use]
    pub fn new(svc: Arc<LoginService>) -> Self {
        let mut decoy_secret = [0u8; 32];
        let _ = ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut decoy_secret);
        Self { svc, decoy_secret }
    }

    async fn start(&self, c: &ConnInfo, user: &str) -> MysqlLogin {
        self.svc
            .start_kafka(LoginRequest {
                persona: Persona::MySql84,
                login_name: user.as_bytes().to_vec(),
                sni: c.sni.clone(),
                client_ip: c.peer.ip(),
                tls: c.tls,
                client_cert: c.client_cert,
                can_handle_expired_password: false,
            })
            .await
    }
}

/// The broker's principal for a login, keeping chapter 14's.
fn principal(p: &AuthPrincipal) -> Principal {
    Principal {
        tenant_id: p.tenant.map_or(0, |t| t.0),
        tenant: p.tenant_name.clone().unwrap_or_default(),
        user: p.user.clone(),
        user_id: p.user_id.map(|u| u64::from(u.0)),
        handle: Some(Arc::new(p.clone())),
    }
}

fn refused(login: &MysqlLogin, verdict: &AuthVerdict) -> AuthFailure {
    let reason = login.refusal_reason().unwrap_or("bad_credentials");
    let text = match verdict {
        AuthVerdict::Refuse(p) => p.message.clone(),
        AuthVerdict::Accept => String::new(),
    };
    AuthFailure(format!("Authentication failed ({reason}): {text}"))
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
            "Authentication failed: Kafka needs a tenant account (catalog login mode)".into(),
        )),
        _ => Err(refused(&login, &verdict)),
    }
}

#[async_trait]
impl Identity for KafkaIdentity {
    async fn plain(&self, c: &ConnInfo, creds: PlainCredentials) -> Result<Principal, AuthFailure> {
        if let Err(e) = self.svc.admit(c.peer.ip()) {
            return Err(AuthFailure(format!("Authentication failed: {e}")));
        }
        let mut login = self.start(c, &creds.user).await;
        let (outcome, path) = if login.check_clear_fast(&creds.password) {
            (Outcome::Verified, AuthPath::Fast)
        } else if login
            .check_password(Zeroizing::new(creds.password.clone()))
            .await
        {
            (Outcome::Verified, AuthPath::Full)
        } else {
            (Outcome::WrongCredentials, AuthPath::Full)
        };
        decide(login, outcome, path).await
    }

    async fn scram(&self, c: &ConnInfo, mech: ScramMechanism, user: &str) -> ScramLookup {
        let admitted = self.svc.admit(c.peer.ip()).is_ok();
        let login = self.start(c, user).await;
        let kind = match mech {
            ScramMechanism::Sha256 => ScramKind::Sha256,
            ScramMechanism::Sha512 => ScramKind::Sha512,
        };
        let real = login
            .scram_credential(kind)
            .filter(|_| admitted)
            .map(|(v, secondary)| ScramCredential {
                mechanism: mech,
                salt: v.salt().to_vec(),
                iterations: v.iterations(),
                stored_key: v.stored_key().to_vec(),
                server_key: v.server_key().to_vec(),
                secondary: secondary.map(|s| (s.stored_key().to_vec(), s.server_key().to_vec())),
            });
        let seed = format!("{}/{user}", c.sni.as_deref().unwrap_or(""));
        let credential = real.clone().unwrap_or_else(|| {
            ScramCredential::decoy(mech, &self.decoy_secret, &seed, DECOY_ITERATIONS)
        });
        ScramLookup {
            credential,
            finish: Box::new(ScramEnd {
                login,
                real: real.is_some(),
                admitted,
            }),
        }
    }
}

struct ScramEnd {
    login: MysqlLogin,
    real: bool,
    admitted: bool,
}

#[async_trait]
impl ScramFinish for ScramEnd {
    async fn finish(self: Box<Self>, verified: bool) -> Result<Principal, AuthFailure> {
        if !self.admitted {
            return Err(AuthFailure(
                "Authentication failed: too many failed logins from this address".into(),
            ));
        }
        let outcome = if verified && self.real {
            Outcome::Verified
        } else {
            Outcome::WrongCredentials
        };
        let mut login = self.login;
        // The client sent a proof of a password: a refusal says "using
        // password: YES", not the MySQL login's default "NO".
        login.proof_presented();
        decide(login, outcome, AuthPath::Full).await
    }
}
