//! SCRAM-SHA-256 for `saslStart` and `saslContinue` (RFC 5802 and
//! RFC 7677), server side.
//!
//! This is the conversation only. The account, its tenant and whether it
//! may log in are the login service's: the front end passes the user name from
//! the client's first message to the login service and hands back the
//! account's stored verifier, or a decoy for unknown accounts, so the
//! exchange looks the same and fails only at the proof.
//!
//! MongoDB's SCRAM-SHA-256 hashes the SASLprep-normalized password, which
//! is exactly what SprigDB stores for every password, so the verifiers
//! work unchanged. (SCRAM-SHA-1 hashes MongoDB's MD5 digest of
//! `user:mongo:password` instead; it needs its own verifier and isn't
//! offered here.)

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use ring::{digest, hmac, pbkdf2};
use subtle::ConstantTimeEq;

/// A stored SCRAM-SHA-256 verifier: what the server needs, never the
/// password.
#[derive(Clone, PartialEq, Eq)]
pub struct Credential {
    /// The salt.
    pub salt: Vec<u8>,
    /// PBKDF2 iterations.
    pub iterations: u32,
    /// `H(HMAC(SaltedPassword, "Client Key"))`.
    pub stored_key: Vec<u8>,
    /// `HMAC(SaltedPassword, "Server Key")`.
    pub server_key: Vec<u8>,
    /// `(StoredKey, ServerKey)` of a retained second password with the
    /// same salt and iterations (dual passwords).
    pub secondary: Option<(Vec<u8>, Vec<u8>)>,
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credential")
            .field("iterations", &self.iterations)
            .finish_non_exhaustive()
    }
}

impl Credential {
    /// Derives the verifier of `password` (already SASLprep-normalized).
    #[must_use]
    pub fn derive(password: &[u8], salt: &[u8], iterations: u32) -> Self {
        let mut salted = [0u8; 32];
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            std::num::NonZeroU32::new(iterations.max(1)).unwrap_or(std::num::NonZeroU32::MIN),
            salt,
            password,
            &mut salted,
        );
        let key = hmac::Key::new(hmac::HMAC_SHA256, &salted);
        let client_key = hmac::sign(&key, b"Client Key");
        let stored_key = digest::digest(&digest::SHA256, client_key.as_ref());
        let server_key = hmac::sign(&key, b"Server Key");
        Self {
            salt: salt.to_vec(),
            iterations,
            stored_key: stored_key.as_ref().to_vec(),
            server_key: server_key.as_ref().to_vec(),
            secondary: None,
        }
    }

    /// A decoy for an account that doesn't exist or has no verifier: a
    /// salt stable per `seed`, so repeated probes see the same one, and
    /// keys no proof matches.
    #[must_use]
    pub fn decoy(secret: &[u8], seed: &str, iterations: u32) -> Self {
        let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
        let salt = hmac::sign(&key, seed.as_bytes());
        let mut stored_key = vec![0u8; 32];
        let _ = getrandom::fill(&mut stored_key);
        Self {
            salt: salt.as_ref()[..28].to_vec(),
            iterations,
            stored_key,
            server_key: vec![0u8; 32],
            secondary: None,
        }
    }
}

/// A SCRAM failure: the text of the 18 `AuthenticationFailed` reply, or
/// of a malformed message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScramError {
    /// A malformed message.
    #[error("Malformed SCRAM message: {0}")]
    Malformed(&'static str),
    /// Channel binding or another unsupported option.
    #[error("{0}")]
    Unsupported(&'static str),
    /// The proof doesn't match.
    #[error("Authentication failed.")]
    Failed,
}

fn attr(part: &str, name: char) -> Option<&str> {
    let mut c = part.chars();
    (c.next() == Some(name) && c.next() == Some('=')).then(|| &part[2..])
}

/// Undoes SCRAM's `saslname` escaping (`=2C` is `,`, `=3D` is `=`).
fn unescape(name: &str) -> Result<String, ScramError> {
    let mut out = String::with_capacity(name.len());
    let mut rest = name;
    while let Some(i) = rest.find('=') {
        out.push_str(&rest[..i]);
        match rest.get(i..i + 3) {
            Some("=2C") => out.push(','),
            Some("=3D") => out.push('='),
            _ => return Err(ScramError::Malformed("user name escape")),
        }
        rest = &rest[i + 3..];
    }
    out.push_str(rest);
    Ok(out)
}

/// The server side of one exchange:
///
/// 1. [`ScramServer::client_first`] parses the client's first message and
///    returns the user name;
/// 2. [`ScramServer::server_first`] takes that user's verifier (or a
///    decoy) and returns the server's first message;
/// 3. [`ScramServer::client_final`] checks the proof and returns the
///    server's final message.
#[derive(Debug, Default)]
pub struct ScramServer {
    gs2_header: String,
    client_first_bare: String,
    client_nonce: String,
    nonce: String,
    server_first: String,
    credential: Option<Credential>,
    user: String,
}

impl ScramServer {
    /// A new exchange.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The user name the client sent.
    #[must_use]
    pub fn user(&self) -> &str {
        &self.user
    }

    /// Parses `client-first-message` and returns the user name.
    ///
    /// # Errors
    /// [`ScramError`] for a malformed message or channel binding.
    pub fn client_first(&mut self, msg: &[u8]) -> Result<String, ScramError> {
        let msg = std::str::from_utf8(msg).map_err(|_| ScramError::Malformed("not UTF-8"))?;
        let mut it = msg.splitn(3, ',');
        let (Some(cbind), Some(authz), Some(bare)) = (it.next(), it.next(), it.next()) else {
            return Err(ScramError::Malformed("client-first-message"));
        };
        match cbind {
            "n" | "y" => {}
            _ if cbind.starts_with("p=") => {
                return Err(ScramError::Unsupported("Channel binding is not supported"));
            }
            _ => return Err(ScramError::Malformed("gs2 header")),
        }
        let mut parts = bare.split(',');
        let user = parts
            .next()
            .and_then(|p| attr(p, 'n'))
            .ok_or(ScramError::Malformed("missing n="))?;
        let cnonce = parts
            .next()
            .and_then(|p| attr(p, 'r'))
            .ok_or(ScramError::Malformed("missing r="))?;
        if cnonce.is_empty() || cnonce.contains(',') {
            return Err(ScramError::Malformed("client nonce"));
        }
        let user = unescape(user)?;
        if user.is_empty() {
            return Err(ScramError::Malformed("empty user name"));
        }
        if let Some(a) = attr(authz, 'a')
            && unescape(a)? != user
        {
            return Err(ScramError::Unsupported(
                "an authorization id other than the user is not supported",
            ));
        }
        self.gs2_header = format!("{cbind},{authz},");
        bare.clone_into(&mut self.client_first_bare);
        cnonce.clone_into(&mut self.client_nonce);
        self.user.clone_from(&user);
        Ok(user)
    }

    /// Returns `server-first-message` for `credential`, with 24 random
    /// bytes of server nonce.
    pub fn server_first(&mut self, credential: Credential) -> Vec<u8> {
        let mut raw = [0u8; 24];
        let _ = getrandom::fill(&mut raw);
        let snonce = B64.encode(raw);
        self.server_first_with_nonce(credential, &snonce)
    }

    /// [`ScramServer::server_first`] with a given server nonce (tests).
    pub fn server_first_with_nonce(&mut self, credential: Credential, snonce: &str) -> Vec<u8> {
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
    /// [`ScramError::Failed`] for a wrong proof; [`ScramError::Malformed`]
    /// for a malformed message or a nonce mismatch.
    pub fn client_final(&mut self, msg: &[u8]) -> Result<Vec<u8>, ScramError> {
        let cred = self
            .credential
            .take()
            .ok_or(ScramError::Malformed("out of order"))?;
        let msg = std::str::from_utf8(msg).map_err(|_| ScramError::Malformed("not UTF-8"))?;
        let (without_proof, proof) = msg
            .rsplit_once(",p=")
            .ok_or(ScramError::Malformed("missing p="))?;
        let mut parts = without_proof.split(',');
        let cb = parts
            .next()
            .and_then(|p| attr(p, 'c'))
            .ok_or(ScramError::Malformed("missing c="))?;
        let nonce = parts
            .next()
            .and_then(|p| attr(p, 'r'))
            .ok_or(ScramError::Malformed("missing r="))?;
        if B64.decode(cb).ok().as_deref() != Some(self.gs2_header.as_bytes()) {
            return Err(ScramError::Malformed("channel binding"));
        }
        if nonce != self.nonce {
            return Err(ScramError::Malformed("nonce mismatch"));
        }
        let proof = B64
            .decode(proof)
            .map_err(|_| ScramError::Malformed("proof"))?;
        if proof.len() != 32 {
            return Err(ScramError::Failed);
        }
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, without_proof
        );
        let check = |stored: &[u8], server: &[u8]| -> Option<Vec<u8>> {
            let key = hmac::Key::new(hmac::HMAC_SHA256, stored);
            let sig = hmac::sign(&key, auth_message.as_bytes());
            let client_key: Vec<u8> = proof.iter().zip(sig.as_ref()).map(|(a, b)| a ^ b).collect();
            let h = digest::digest(&digest::SHA256, &client_key);
            if bool::from(h.as_ref().ct_eq(stored)) {
                let skey = hmac::Key::new(hmac::HMAC_SHA256, server);
                let v = hmac::sign(&skey, auth_message.as_bytes());
                Some(format!("v={}", B64.encode(v.as_ref())).into_bytes())
            } else {
                None
            }
        };
        if let Some(v) = check(&cred.stored_key, &cred.server_key) {
            return Ok(v);
        }
        if let Some((s, k)) = &cred.secondary
            && let Some(v) = check(s, k)
        {
            return Ok(v);
        }
        Err(ScramError::Failed)
    }
}

/// The client side, for tests and `sprigctl`: computes the final message
/// for `password`.
#[derive(Debug)]
pub struct ScramClient {
    user: String,
    password: Vec<u8>,
    cnonce: String,
    client_first_bare: String,
    server_signature: Vec<u8>,
}

impl ScramClient {
    /// A client for `user` with `password` (already SASLprep-normalized).
    #[must_use]
    pub fn new(user: &str, password: &[u8], cnonce: &str) -> Self {
        let name = user.replace('=', "=3D").replace(',', "=2C");
        Self {
            user: user.to_owned(),
            password: password.to_vec(),
            cnonce: cnonce.to_owned(),
            client_first_bare: format!("n={name},r={cnonce}"),
            server_signature: Vec::new(),
        }
    }

    /// `client-first-message`.
    #[must_use]
    pub fn first(&self) -> Vec<u8> {
        format!("n,,{}", self.client_first_bare).into_bytes()
    }

    /// `client-final-message` for `server_first`.
    ///
    /// # Errors
    /// [`ScramError::Malformed`] for a bad server message.
    pub fn finish(&mut self, server_first: &[u8]) -> Result<Vec<u8>, ScramError> {
        let sf = std::str::from_utf8(server_first).map_err(|_| ScramError::Malformed("utf8"))?;
        let mut nonce = None;
        let mut salt = None;
        let mut iters = None;
        for p in sf.split(',') {
            if let Some(v) = attr(p, 'r') {
                nonce = Some(v);
            } else if let Some(v) = attr(p, 's') {
                salt = B64.decode(v).ok();
            } else if let Some(v) = attr(p, 'i') {
                iters = v.parse::<u32>().ok();
            }
        }
        let (Some(nonce), Some(salt), Some(iters)) = (nonce, salt, iters) else {
            return Err(ScramError::Malformed("server-first-message"));
        };
        if !nonce.starts_with(&self.cnonce) {
            return Err(ScramError::Malformed("nonce"));
        }
        let mut salted = [0u8; 32];
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            std::num::NonZeroU32::new(iters.max(1)).unwrap_or(std::num::NonZeroU32::MIN),
            &salt,
            &self.password,
            &mut salted,
        );
        let key = hmac::Key::new(hmac::HMAC_SHA256, &salted);
        let client_key = hmac::sign(&key, b"Client Key");
        let stored = digest::digest(&digest::SHA256, client_key.as_ref());
        let without_proof = format!("c=biws,r={nonce}");
        let auth = format!("{},{sf},{without_proof}", self.client_first_bare);
        let sig = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, stored.as_ref()),
            auth.as_bytes(),
        );
        let proof: Vec<u8> = client_key
            .as_ref()
            .iter()
            .zip(sig.as_ref())
            .map(|(a, b)| a ^ b)
            .collect();
        let server_key = hmac::sign(&key, b"Server Key");
        self.server_signature = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, server_key.as_ref()),
            auth.as_bytes(),
        )
        .as_ref()
        .to_vec();
        Ok(format!("{without_proof},p={}", B64.encode(proof)).into_bytes())
    }

    /// Whether `server_final` carries the expected server signature.
    #[must_use]
    pub fn verify(&self, server_final: &[u8]) -> bool {
        server_final == format!("v={}", B64.encode(&self.server_signature)).as_bytes()
    }

    /// The user name.
    #[must_use]
    pub fn user(&self) -> &str {
        &self.user
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7677's example: user `user`, password `pencil`, 4096 iterations.
    #[test]
    fn rfc7677_vector() {
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let cred = Credential::derive(b"pencil", &salt, 4096);
        let mut s = ScramServer::new();
        let user = s.client_first(b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO").unwrap();
        assert_eq!(user, "user");
        let sf = s.server_first_with_nonce(cred, "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0");
        assert_eq!(
            sf,
            b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
        );
        let fin = s
            .client_final(b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=")
            .unwrap();
        assert_eq!(fin, b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=");
    }

    #[test]
    fn client_and_server_agree_and_wrong_password_fails() {
        let cred = Credential::derive(b"sprig-dev-app", b"0123456789abcdef", 15000);
        for (pw, ok) in [(&b"sprig-dev-app"[..], true), (&b"nope"[..], false)] {
            let mut c = ScramClient::new("acme.app", pw, "abcDEF123");
            let mut s = ScramServer::new();
            assert_eq!(s.client_first(&c.first()).unwrap(), "acme.app");
            let sf = s.server_first(cred.clone());
            let cf = c.finish(&sf).unwrap();
            match s.client_final(&cf) {
                Ok(v) => {
                    assert!(ok);
                    assert!(c.verify(&v));
                }
                Err(e) => {
                    assert!(!ok);
                    assert_eq!(e, ScramError::Failed);
                }
            }
        }
    }

    #[test]
    fn secondary_password_and_decoy() {
        let salt = b"saltsaltsaltsalt";
        let mut cred = Credential::derive(b"new", salt, 4096);
        let old = Credential::derive(b"old", salt, 4096);
        cred.secondary = Some((old.stored_key, old.server_key));
        let mut c = ScramClient::new("u", b"old", "n1");
        let mut s = ScramServer::new();
        s.client_first(&c.first()).unwrap();
        let cf = c.finish(&s.server_first(cred)).unwrap();
        assert!(s.client_final(&cf).is_ok());

        let d1 = Credential::decoy(b"secret", "acme.ghost", 15000);
        let d2 = Credential::decoy(b"secret", "acme.ghost", 15000);
        assert_eq!(d1.salt, d2.salt);
        let mut c = ScramClient::new("acme.ghost", b"x", "n2");
        let mut s = ScramServer::new();
        s.client_first(&c.first()).unwrap();
        let cf = c.finish(&s.server_first(d1)).unwrap();
        assert_eq!(s.client_final(&cf), Err(ScramError::Failed));
    }

    #[test]
    fn malformed() {
        let mut s = ScramServer::new();
        assert!(s.client_first(b"p=tls-unique,,n=u,r=x").is_err());
        assert!(s.client_first(b"n,,r=x").is_err());
        assert!(s.client_first(b"n,,n=a=2Cb,r=x").is_ok());
        assert_eq!(s.user(), "a,b");
        assert!(s.client_final(b"c=biws,r=x,p=AAAA").is_err());
    }
}
