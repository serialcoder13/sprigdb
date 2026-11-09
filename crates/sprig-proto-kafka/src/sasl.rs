//! SASL for the Kafka listener (chapter 45, "Authentication"): PLAIN
//! (RFC 4616) and SCRAM-SHA-256 and SCRAM-SHA-512 (RFC 5802, RFC 7677),
//! server side.
//!
//! These are the conversations only. Who the user is, which tenant, and
//! whether the account may log in is chapter 14's: the broker passes the
//! PLAIN credentials, or the SCRAM user name, to its
//! [`Identity`](crate::broker::Identity), which answers with a principal
//! or with the account's SCRAM verifier (a decoy for unknown accounts, so
//! the exchange looks the same and fails only at the proof).

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use ring::{digest, hmac, pbkdf2};

/// A SCRAM mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScramMechanism {
    /// `SCRAM-SHA-256`.
    Sha256,
    /// `SCRAM-SHA-512`.
    Sha512,
}

impl ScramMechanism {
    /// The SASL mechanism name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }

    /// The mechanism named `name`, if it is a SCRAM one.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "SCRAM-SHA-256" => Some(Self::Sha256),
            "SCRAM-SHA-512" => Some(Self::Sha512),
            _ => None,
        }
    }

    fn hmac_alg(self) -> hmac::Algorithm {
        match self {
            Self::Sha256 => hmac::HMAC_SHA256,
            Self::Sha512 => hmac::HMAC_SHA512,
        }
    }

    fn pbkdf2_alg(self) -> pbkdf2::Algorithm {
        match self {
            Self::Sha256 => pbkdf2::PBKDF2_HMAC_SHA256,
            Self::Sha512 => pbkdf2::PBKDF2_HMAC_SHA512,
        }
    }

    fn digest_alg(self) -> &'static digest::Algorithm {
        match self {
            Self::Sha256 => &digest::SHA256,
            Self::Sha512 => &digest::SHA512,
        }
    }

    /// The hash's output length.
    #[must_use]
    pub const fn len(self) -> usize {
        match self {
            Self::Sha256 => 32,
            Self::Sha512 => 64,
        }
    }

    /// Never zero; for clippy's `len_without_is_empty`.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        false
    }
}

/// A stored SCRAM verifier (chapter 14's `verifiers` row): what the server
/// needs, never the password.
#[derive(Clone, PartialEq, Eq)]
pub struct ScramCredential {
    /// The mechanism.
    pub mechanism: ScramMechanism,
    /// The salt.
    pub salt: Vec<u8>,
    /// PBKDF2 iterations.
    pub iterations: u32,
    /// `H(HMAC(SaltedPassword, "Client Key"))`.
    pub stored_key: Vec<u8>,
    /// `HMAC(SaltedPassword, "Server Key")`.
    pub server_key: Vec<u8>,
    /// `(StoredKey, ServerKey)` of a retained second password with the same
    /// salt and iterations (chapter 14's dual passwords, `RETAIN CURRENT
    /// PASSWORD`): the proof may match either.
    pub secondary: Option<(Vec<u8>, Vec<u8>)>,
}

impl std::fmt::Debug for ScramCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScramCredential")
            .field("mechanism", &self.mechanism)
            .field("iterations", &self.iterations)
            .finish_non_exhaustive()
    }
}

impl ScramCredential {
    /// Derives the verifier of `password` (already SASLprep-normalized by
    /// the caller, as chapter 14 stores it).
    #[must_use]
    pub fn derive(
        mechanism: ScramMechanism,
        password: &[u8],
        salt: &[u8],
        iterations: u32,
    ) -> Self {
        let mut salted = vec![0u8; mechanism.len()];
        pbkdf2::derive(
            mechanism.pbkdf2_alg(),
            std::num::NonZeroU32::new(iterations.max(1)).unwrap_or(std::num::NonZeroU32::MIN),
            salt,
            password,
            &mut salted,
        );
        let key = hmac::Key::new(mechanism.hmac_alg(), &salted);
        let client_key = hmac::sign(&key, b"Client Key");
        let stored_key = digest::digest(mechanism.digest_alg(), client_key.as_ref());
        let server_key = hmac::sign(&key, b"Server Key");
        Self {
            mechanism,
            salt: salt.to_vec(),
            iterations,
            stored_key: stored_key.as_ref().to_vec(),
            server_key: server_key.as_ref().to_vec(),
            secondary: None,
        }
    }

    /// A decoy for an account that doesn't exist, or has no verifier of
    /// this mechanism: a salt stable per (`seed`, user), so repeated probes
    /// see the same one, and keys no proof matches.
    #[must_use]
    pub fn decoy(mechanism: ScramMechanism, secret: &[u8], seed: &str, iterations: u32) -> Self {
        let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
        let salt = hmac::sign(&key, seed.as_bytes());
        let mut stored_key = vec![0u8; mechanism.len()];
        let _ = getrandom::fill(&mut stored_key);
        Self {
            mechanism,
            salt: salt.as_ref()[..28].to_vec(),
            iterations,
            stored_key,
            server_key: vec![0u8; mechanism.len()],
            secondary: None,
        }
    }
}

/// A SASL failure; the message goes into the SaslAuthenticate response.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct SaslError(pub String);

impl SaslError {
    fn bad(what: &str) -> Self {
        Self(format!("Malformed SASL message: {what}"))
    }
}

/// PLAIN credentials.
#[derive(Clone, PartialEq, Eq)]
pub struct PlainCredentials {
    /// The authorization identity; empty or equal to `user`.
    pub authzid: String,
    /// The user name as sent (`tenant.user`, or `user` with a tenant SNI).
    pub user: String,
    /// The password.
    pub password: Vec<u8>,
}

impl std::fmt::Debug for PlainCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlainCredentials")
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

/// Parses a PLAIN message, `[authzid] NUL authcid NUL passwd`. An authzid
/// other than the user is refused: SprigDB has no proxy authorization.
///
/// # Errors
/// [`SaslError`] for a malformed message or a different authzid.
pub fn parse_plain(msg: &[u8]) -> Result<PlainCredentials, SaslError> {
    let mut parts = msg.splitn(3, |b| *b == 0);
    let (Some(authz), Some(user), Some(pw)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(SaslError::bad("PLAIN needs authzid NUL user NUL password"));
    };
    if pw.contains(&0) {
        return Err(SaslError::bad("NUL in the PLAIN password"));
    }
    let authzid = String::from_utf8(authz.to_vec()).map_err(|_| SaslError::bad("authzid"))?;
    let user = String::from_utf8(user.to_vec()).map_err(|_| SaslError::bad("user name"))?;
    if user.is_empty() {
        return Err(SaslError::bad("empty user name"));
    }
    if !authzid.is_empty() && authzid != user {
        return Err(SaslError(
            "Authentication failed: an authorization id other than the user is not supported"
                .into(),
        ));
    }
    Ok(PlainCredentials {
        authzid,
        user,
        password: pw.to_vec(),
    })
}

/// The server side of one SCRAM exchange.
///
/// 1. [`ScramServer::client_first`] parses the client's first message and
///    returns the user name;
/// 2. [`ScramServer::server_first`] takes that user's verifier (or a
///    decoy) and returns the server's first message;
/// 3. [`ScramServer::client_final`] checks the proof and returns the
///    server's final message, or an error.
#[derive(Debug)]
pub struct ScramServer {
    mechanism: ScramMechanism,
    gs2_header: String,
    client_first_bare: String,
    client_nonce: String,
    nonce: String,
    server_first: String,
    credential: Option<ScramCredential>,
}

fn attr(part: &str, name: char) -> Option<&str> {
    let mut c = part.chars();
    (c.next() == Some(name) && c.next() == Some('=')).then(|| &part[2..])
}

/// Undoes SCRAM's `saslname` escaping (`=2C` is `,`, `=3D` is `=`).
fn unescape(name: &str) -> Result<String, SaslError> {
    let mut out = String::with_capacity(name.len());
    let mut rest = name;
    while let Some(i) = rest.find('=') {
        out.push_str(&rest[..i]);
        match rest.get(i..i + 3) {
            Some("=2C") => out.push(','),
            Some("=3D") => out.push('='),
            _ => return Err(SaslError::bad("user name escape")),
        }
        rest = &rest[i + 3..];
    }
    out.push_str(rest);
    Ok(out)
}

impl ScramServer {
    /// A new exchange for `mechanism`.
    #[must_use]
    pub fn new(mechanism: ScramMechanism) -> Self {
        Self {
            mechanism,
            gs2_header: String::new(),
            client_first_bare: String::new(),
            client_nonce: String::new(),
            nonce: String::new(),
            server_first: String::new(),
            credential: None,
        }
    }

    /// The mechanism.
    #[must_use]
    pub fn mechanism(&self) -> ScramMechanism {
        self.mechanism
    }

    /// Parses `client-first-message` and returns the user name.
    ///
    /// # Errors
    /// [`SaslError`] for a malformed message, channel binding (SprigDB
    /// offers none), or Kafka's delegation-token extension.
    pub fn client_first(&mut self, msg: &[u8]) -> Result<String, SaslError> {
        let msg = std::str::from_utf8(msg).map_err(|_| SaslError::bad("not UTF-8"))?;
        // gs2-header: cbind-flag "," [authzid] ","
        let mut it = msg.splitn(3, ',');
        let (Some(cbind), Some(authz), Some(bare)) = (it.next(), it.next(), it.next()) else {
            return Err(SaslError::bad("client-first-message"));
        };
        match cbind {
            "n" | "y" => {}
            _ if cbind.starts_with("p=") => {
                return Err(SaslError("Channel binding is not supported".into()));
            }
            _ => return Err(SaslError::bad("gs2 header")),
        }
        if !authz.is_empty() && attr(authz, 'a').is_none() {
            return Err(SaslError::bad("gs2 authzid"));
        }
        let mut parts = bare.split(',');
        let user = parts
            .next()
            .and_then(|p| attr(p, 'n'))
            .ok_or_else(|| SaslError::bad("missing n="))?;
        let cnonce = parts
            .next()
            .and_then(|p| attr(p, 'r'))
            .ok_or_else(|| SaslError::bad("missing r="))?;
        if cnonce.is_empty() || cnonce.contains(',') {
            return Err(SaslError::bad("client nonce"));
        }
        for ext in parts {
            if ext.starts_with("tokenauth=") {
                return Err(SaslError(
                    "Authentication failed: delegation tokens are not supported".into(),
                ));
            }
        }
        let user = unescape(user)?;
        if let Some(a) = attr(authz, 'a')
            && unescape(a)? != user
        {
            return Err(SaslError(
                "Authentication failed: an authorization id other than the user is not supported"
                    .into(),
            ));
        }
        if user.is_empty() {
            return Err(SaslError::bad("empty user name"));
        }
        self.gs2_header = format!("{cbind},{authz},");
        bare.clone_into(&mut self.client_first_bare);
        cnonce.clone_into(&mut self.client_nonce);
        Ok(user)
    }

    /// Returns `server-first-message` for `credential`, with a fresh
    /// server nonce.
    pub fn server_first(&mut self, credential: ScramCredential) -> Vec<u8> {
        let mut raw = [0u8; 18];
        let _ = getrandom::fill(&mut raw);
        let snonce = B64.encode(raw);
        self.server_first_with_nonce(credential, &snonce)
    }

    /// [`ScramServer::server_first`] with a given server nonce (tests).
    pub fn server_first_with_nonce(
        &mut self,
        credential: ScramCredential,
        snonce: &str,
    ) -> Vec<u8> {
        self.nonce = format!("{}{snonce}", self.client_nonce);
        self.server_first = format!(
            "r={},s={},i={}",
            self.nonce,
            B64.encode(&credential.salt),
            credential.iterations
        );
        self.credential = Some(credential);
        self.server_first.clone().into_bytes()
    }

    /// Checks `client-final-message`; on success returns
    /// `server-final-message` (`v=` the server signature).
    ///
    /// # Errors
    /// [`SaslError`] for a malformed message, a nonce or channel-binding
    /// mismatch, or a wrong proof.
    pub fn client_final(&mut self, msg: &[u8]) -> Result<Vec<u8>, SaslError> {
        let fail = || {
            SaslError("Authentication failed during authentication due to invalid credentials with SASL mechanism".into())
        };
        let cred = self
            .credential
            .take()
            .ok_or_else(|| SaslError::bad("out of order"))?;
        let msg = std::str::from_utf8(msg).map_err(|_| SaslError::bad("not UTF-8"))?;
        let (without_proof, proof) = msg
            .rsplit_once(",p=")
            .ok_or_else(|| SaslError::bad("missing p="))?;
        let mut parts = without_proof.split(',');
        let cb = parts
            .next()
            .and_then(|p| attr(p, 'c'))
            .ok_or_else(|| SaslError::bad("missing c="))?;
        let nonce = parts
            .next()
            .and_then(|p| attr(p, 'r'))
            .ok_or_else(|| SaslError::bad("missing r="))?;
        if B64.decode(cb).ok().as_deref() != Some(self.gs2_header.as_bytes()) {
            return Err(SaslError::bad("channel binding"));
        }
        // Kafka's broker accepts a final nonce that ends with its own:
        // librdkafka (up to at least 1.8) sends the client nonce followed
        // by the whole server nonce, which already starts with it. The
        // proof covers the message as sent either way.
        if nonce != self.nonce && !nonce.ends_with(&self.nonce) {
            return Err(SaslError::bad("nonce"));
        }
        let proof = B64.decode(proof).map_err(|_| SaslError::bad("proof"))?;
        if proof.len() != self.mechanism.len() {
            return Err(fail());
        }
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, without_proof
        );
        let alg = self.mechanism.hmac_alg();
        // The primary password's keys, then a retained secondary's (same
        // salt and iterations); both are always computed.
        let candidates = std::iter::once((&cred.stored_key, &cred.server_key))
            .chain(cred.secondary.as_ref().map(|(st, sv)| (st, sv)));
        let mut matched = None;
        for (stored_key, server_key) in candidates {
            let sig = hmac::sign(&hmac::Key::new(alg, stored_key), auth_message.as_bytes());
            let client_key: Vec<u8> = proof.iter().zip(sig.as_ref()).map(|(a, b)| a ^ b).collect();
            let stored = digest::digest(self.mechanism.digest_alg(), &client_key);
            if bool::from(subtle::ConstantTimeEq::ct_eq(
                stored.as_ref(),
                stored_key.as_slice(),
            )) && matched.is_none()
            {
                matched = Some(server_key);
            }
        }
        let Some(server_key) = matched else {
            return Err(fail());
        };
        let server_sig = hmac::sign(&hmac::Key::new(alg, server_key), auth_message.as_bytes());
        Ok(format!("v={}", B64.encode(server_sig.as_ref())).into_bytes())
    }
}

/// The client side of SCRAM, for tests and the gateway's own live tests.
#[derive(Debug)]
pub struct ScramClient {
    mechanism: ScramMechanism,
    password: Vec<u8>,
    client_first_bare: String,
    cnonce: String,
    server_signature: Vec<u8>,
}

impl ScramClient {
    /// A client for `user` with `password`, using `cnonce`.
    #[must_use]
    pub fn new(mechanism: ScramMechanism, user: &str, password: &[u8], cnonce: &str) -> Self {
        let user = user.replace('=', "=3D").replace(',', "=2C");
        Self {
            mechanism,
            password: password.to_vec(),
            client_first_bare: format!("n={user},r={cnonce}"),
            cnonce: cnonce.to_owned(),
            server_signature: Vec::new(),
        }
    }

    /// `client-first-message`.
    #[must_use]
    pub fn first(&self) -> Vec<u8> {
        format!("n,,{}", self.client_first_bare).into_bytes()
    }

    /// `client-final-message` for the server's first message.
    ///
    /// # Errors
    /// [`SaslError`] for a malformed server message.
    pub fn last(&mut self, server_first: &[u8]) -> Result<Vec<u8>, SaslError> {
        let sf = std::str::from_utf8(server_first).map_err(|_| SaslError::bad("server-first"))?;
        let mut parts = sf.split(',');
        let nonce = parts
            .next()
            .and_then(|p| attr(p, 'r'))
            .ok_or_else(|| SaslError::bad("r"))?;
        let salt = parts
            .next()
            .and_then(|p| attr(p, 's'))
            .ok_or_else(|| SaslError::bad("s"))?;
        let iters = parts
            .next()
            .and_then(|p| attr(p, 'i'))
            .ok_or_else(|| SaslError::bad("i"))?;
        if !nonce.starts_with(&self.cnonce) {
            return Err(SaslError::bad("server nonce"));
        }
        let salt = B64.decode(salt).map_err(|_| SaslError::bad("salt"))?;
        let iters: u32 = iters.parse().map_err(|_| SaslError::bad("iterations"))?;
        let mut salted = vec![0u8; self.mechanism.len()];
        pbkdf2::derive(
            self.mechanism.pbkdf2_alg(),
            std::num::NonZeroU32::new(iters.max(1)).unwrap_or(std::num::NonZeroU32::MIN),
            &salt,
            &self.password,
            &mut salted,
        );
        let alg = self.mechanism.hmac_alg();
        let key = hmac::Key::new(alg, &salted);
        let client_key = hmac::sign(&key, b"Client Key");
        let stored = digest::digest(self.mechanism.digest_alg(), client_key.as_ref());
        let without_proof = format!("c=biws,r={nonce}");
        let auth = format!("{},{sf},{without_proof}", self.client_first_bare);
        let sig = hmac::sign(&hmac::Key::new(alg, stored.as_ref()), auth.as_bytes());
        let proof: Vec<u8> = client_key
            .as_ref()
            .iter()
            .zip(sig.as_ref())
            .map(|(a, b)| a ^ b)
            .collect();
        let server_key = hmac::sign(&key, b"Server Key");
        self.server_signature =
            hmac::sign(&hmac::Key::new(alg, server_key.as_ref()), auth.as_bytes())
                .as_ref()
                .to_vec();
        Ok(format!("{without_proof},p={}", B64.encode(proof)).into_bytes())
    }

    /// Whether the server's final message proves it knew the verifier.
    #[must_use]
    pub fn verify(&self, server_final: &[u8]) -> bool {
        std::str::from_utf8(server_final)
            .ok()
            .and_then(|s| attr(s, 'v'))
            .and_then(|v| B64.decode(v).ok())
            .is_some_and(|v| v == self.server_signature)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_parsing() {
        let c = parse_plain(b"\0acme.app\0s3cret").unwrap();
        assert_eq!(c.user, "acme.app");
        assert_eq!(c.password, b"s3cret");
        assert!(parse_plain(b"acme.app\0acme.app\0x").is_ok());
        assert!(parse_plain(b"bob\0acme.app\0x").is_err());
        assert!(parse_plain(b"\0acme.app").is_err());
        assert!(parse_plain(b"\0\0x").is_err());
    }

    /// RFC 7677 section 3: user "user", password "pencil", the nonces and
    /// salt given there, 4096 iterations.
    #[test]
    fn rfc7677_scram_sha_256_vector() {
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let cred = ScramCredential::derive(ScramMechanism::Sha256, b"pencil", &salt, 4096);
        let mut s = ScramServer::new(ScramMechanism::Sha256);
        let user = s.client_first(b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO").unwrap();
        assert_eq!(user, "user");
        let first = s.server_first_with_nonce(cred, "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0");
        assert_eq!(
            first,
            b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
        );
        let fin = s
            .client_final(b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=")
            .unwrap();
        assert_eq!(fin, b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=");
    }

    #[test]
    fn scram_sha_512_round_trip_and_wrong_password() {
        for mech in [ScramMechanism::Sha256, ScramMechanism::Sha512] {
            let cred = ScramCredential::derive(mech, b"right", b"salt-salt-salt", 4096);
            for (pw, ok) in [(&b"right"[..], true), (&b"wrong"[..], false)] {
                let mut c = ScramClient::new(mech, "acme.a=b,c", pw, "cnonce123");
                let mut s = ScramServer::new(mech);
                assert_eq!(s.client_first(&c.first()).unwrap(), "acme.a=b,c");
                let sf = s.server_first(cred.clone());
                let cf = c.last(&sf).unwrap();
                match s.client_final(&cf) {
                    Ok(fin) => {
                        assert!(ok);
                        assert!(c.verify(&fin));
                    }
                    Err(_) => assert!(!ok),
                }
            }
        }
    }

    /// librdkafka's final message repeats the client nonce before the
    /// server's whole nonce; Kafka accepts it (the nonce ends with the
    /// server's), and so does SprigDB. Anything else is still refused.
    #[test]
    fn librdkafka_final_nonce_is_accepted() {
        let mech = ScramMechanism::Sha256;
        let pw = b"sprig-dev-app";
        let salt = b"0123456789abcdef";
        let cred = ScramCredential::derive(mech, pw, salt, 4096);
        let final_msg = |nonce: &str, s: &ScramServer| {
            let mut salted = vec![0u8; mech.len()];
            pbkdf2::derive(
                mech.pbkdf2_alg(),
                std::num::NonZeroU32::new(4096).unwrap(),
                salt,
                pw,
                &mut salted,
            );
            let key = hmac::Key::new(mech.hmac_alg(), &salted);
            let ck = hmac::sign(&key, b"Client Key");
            let sk = digest::digest(mech.digest_alg(), ck.as_ref());
            let wo = format!("c=biws,r={nonce}");
            let auth = format!("{},{},{wo}", s.client_first_bare, s.server_first);
            let sig = hmac::sign(&hmac::Key::new(mech.hmac_alg(), sk.as_ref()), auth.as_bytes());
            let p: Vec<u8> = ck.as_ref().iter().zip(sig.as_ref()).map(|(a, b)| a ^ b).collect();
            format!("{wo},p={}", B64.encode(p)).into_bytes()
        };
        let cnonce = "Kd^wepcSBU43;b{K~R-Zt<6aQ2f-w9JS";
        let mut s = ScramServer::new(mech);
        s.client_first(format!("n,,n=acme.app,r={cnonce}").as_bytes())
            .unwrap();
        s.server_first(cred.clone());
        let doubled = format!("{cnonce}{}", s.nonce);
        let m = final_msg(&doubled, &s);
        assert!(s.client_final(&m).is_ok());
        // A nonce that doesn't end with the server's is refused.
        let mut s = ScramServer::new(mech);
        s.client_first(format!("n,,n=acme.app,r={cnonce}").as_bytes())
            .unwrap();
        s.server_first(cred);
        let wrong = format!("{}x", s.nonce);
        let m = final_msg(&wrong, &s);
        assert!(s.client_final(&m).is_err());
    }

    /// Chapter 14's dual passwords: the new password's verifier shares the
    /// retained one's salt, so one exchange accepts either password.
    #[test]
    fn a_retained_password_with_the_same_salt_also_logs_in() {
        for mech in [ScramMechanism::Sha256, ScramMechanism::Sha512] {
            let old = ScramCredential::derive(mech, b"old-pw", b"shared-salt", 4096);
            let mut cred = ScramCredential::derive(mech, b"new-pw", b"shared-salt", 4096);
            cred.secondary = Some((old.stored_key.clone(), old.server_key.clone()));
            for (pw, ok) in [
                (&b"new-pw"[..], true),
                (&b"old-pw"[..], true),
                (&b"other"[..], false),
            ] {
                let mut c = ScramClient::new(mech, "acme.app", pw, "cn");
                let mut s = ScramServer::new(mech);
                s.client_first(&c.first()).unwrap();
                let cf = c.last(&s.server_first(cred.clone())).unwrap();
                match s.client_final(&cf) {
                    // The server signs with the matching password's key.
                    Ok(fin) => assert!(ok && c.verify(&fin), "{pw:?}"),
                    Err(_) => assert!(!ok, "{pw:?}"),
                }
            }
        }
    }

    #[test]
    fn decoys_fail_at_the_proof_with_a_stable_salt() {
        let a = ScramCredential::decoy(ScramMechanism::Sha512, b"k", "acme/nobody", 4096);
        let b = ScramCredential::decoy(ScramMechanism::Sha512, b"k", "acme/nobody", 4096);
        assert_eq!(a.salt, b.salt);
        let mut c = ScramClient::new(ScramMechanism::Sha512, "acme.nobody", b"x", "n1");
        let mut s = ScramServer::new(ScramMechanism::Sha512);
        s.client_first(&c.first()).unwrap();
        let cf = c.last(&s.server_first(a)).unwrap();
        assert!(s.client_final(&cf).is_err());
    }

    #[test]
    fn refused_client_first_messages() {
        let mut s = ScramServer::new(ScramMechanism::Sha256);
        assert!(s.client_first(b"p=tls-unique,,n=u,r=x").is_err());
        assert!(s.client_first(b"n,,n=u,r=x,tokenauth=true").is_err());
        assert!(s.client_first(b"n,a=other,n=u,r=x").is_err());
        assert!(s.client_first(b"n,,n=u=ZZ,r=x").is_err());
        assert!(s.client_first(b"garbage").is_err());
        assert!(s.client_final(b"c=biws,r=x,p=AAAA").is_err());
    }
}
