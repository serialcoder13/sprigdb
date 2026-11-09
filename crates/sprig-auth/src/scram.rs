//! SCRAM verifiers (chapter 14, "Verifier formats"; build step 2's other
//! half): SCRAM-SHA-256 (RFC 7677) and SCRAM-SHA-512, stored with every
//! password so Kafka (chapter 45) and later MongoDB (chapter 30) logins can
//! run SCRAM without the password.
//!
//! ```text
//! SaltedPassword = PBKDF2-HMAC-H(P, salt, iterations)          (RFC 5802)
//! ClientKey = HMAC(SaltedPassword, "Client Key");  StoredKey = H(ClientKey)
//! ServerKey = HMAC(SaltedPassword, "Server Key")
//! P = SASLprep(password)       for SCRAM-SHA-256 (RFC 7677; MongoDB too)
//! P = the password's bytes     for SCRAM-SHA-512 (Kafka's clients skip
//!                              SASLprep, and only Kafka offers SHA-512)
//! salt = 28 random bytes; iterations from auth.scram_sha256_iterations or
//! auth.scram_sha512_iterations (15000 by default)
//! ```
//!
//! A `verifiers` row keeps `salt`, `iterations`, `stored_key` and
//! `server_key`; `auth_string` is NULL. Only `StoredKey` and `ServerKey`
//! are kept, never `SaltedPassword` or `ClientKey`, so a stolen row can't
//! log in (RFC 5802 section 9) but it can be attacked offline like any
//! password hash: rows stay in the catalog, which tenant accounts can't
//! read, and `Debug` prints the mechanism and iterations only.
//!
//! A password that `SASLprep` refuses (control characters, unassigned code
//! points, bad bidirectional text, or bytes that aren't UTF-8) gets no
//! SCRAM-SHA-256 verifier: it can't be used over SCRAM-SHA-256.
//!
//! [`ScramVerifier::derive`] costs `2 × iterations` HMAC computations (a
//! few milliseconds at 15000): call it off the async runtime.

use std::fmt;
use std::num::NonZeroU32;
use std::time::SystemTime;

use ring::{digest, hmac, pbkdf2};
use sprig_common::ids::UserId;
use sprig_common::{Result, SprigCode, SprigError};
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

use crate::store::{LoginForm, Slot, VerifierKind, VerifierRow};

/// `auth.scram_sha256_iterations`' and `auth.scram_sha512_iterations`'
/// default: MongoDB's SCRAM-SHA-256 default, above Kafka's minimum of 4096.
pub const DEFAULT_SCRAM_ITERATIONS: u32 = 15_000;

/// The fewest iterations the settings allow (MongoDB's minimum).
pub const MIN_SCRAM_ITERATIONS: u32 = 5_000;

/// The most iterations the settings allow.
pub const MAX_SCRAM_ITERATIONS: u32 = 1_000_000;

/// The salt length of new verifiers (MongoDB's for SCRAM-SHA-256).
pub const SCRAM_SALT_LEN: usize = 28;

/// The longest salt a stored verifier may have (`VARBINARY(64)`).
pub const MAX_SCRAM_SALT_LEN: usize = 64;

/// A SCRAM mechanism SprigDB stores verifiers for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ScramKind {
    /// SCRAM-SHA-256 (RFC 7677): Kafka, and MongoDB from R5.
    Sha256,
    /// SCRAM-SHA-512: Kafka.
    Sha512,
}

impl ScramKind {
    /// Both, in the order verifiers are written.
    pub const ALL: [ScramKind; 2] = [ScramKind::Sha256, ScramKind::Sha512];

    /// The `verifiers.mechanism` value.
    #[must_use]
    pub const fn verifier_kind(self) -> VerifierKind {
        match self {
            Self::Sha256 => VerifierKind::ScramSha256,
            Self::Sha512 => VerifierKind::ScramSha512,
        }
    }

    /// The SCRAM kind of a `verifiers.mechanism`, if it is one of these.
    #[must_use]
    pub const fn of(kind: VerifierKind) -> Option<Self> {
        match kind {
            VerifierKind::ScramSha256 => Some(Self::Sha256),
            VerifierKind::ScramSha512 => Some(Self::Sha512),
            _ => None,
        }
    }

    /// The SASL mechanism name.
    #[must_use]
    pub const fn mechanism(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }

    /// The length of the hash, and of `StoredKey` and `ServerKey`.
    #[must_use]
    pub const fn key_len(self) -> usize {
        self.alg().key_len()
    }

    const fn alg(self) -> Alg {
        match self {
            Self::Sha256 => Alg::Sha256,
            Self::Sha512 => Alg::Sha512,
        }
    }

    /// The password as SCRAM hashes it for this mechanism: `SASLprep` for
    /// SCRAM-SHA-256, the bytes as given for SCRAM-SHA-512. `None` when
    /// `SASLprep` refuses the password.
    #[must_use]
    pub fn prepare(self, password: &[u8]) -> Option<Zeroizing<Vec<u8>>> {
        match self {
            Self::Sha256 => saslprep(password),
            Self::Sha512 => Some(Zeroizing::new(password.to_vec())),
        }
    }
}

impl fmt::Display for ScramKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.mechanism())
    }
}

/// The hash of a SCRAM computation. SHA-1 is only for RFC 5802's own test
/// vector (MongoDB's SCRAM-SHA-1 arrives with chapter 30).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Alg {
    #[cfg(test)]
    Sha1,
    Sha256,
    Sha512,
}

impl Alg {
    const fn key_len(self) -> usize {
        match self {
            #[cfg(test)]
            Self::Sha1 => 20,
            Self::Sha256 => 32,
            Self::Sha512 => 64,
        }
    }

    fn hmac(self) -> hmac::Algorithm {
        match self {
            #[cfg(test)]
            Self::Sha1 => hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY,
            Self::Sha256 => hmac::HMAC_SHA256,
            Self::Sha512 => hmac::HMAC_SHA512,
        }
    }

    fn pbkdf2(self) -> pbkdf2::Algorithm {
        match self {
            #[cfg(test)]
            Self::Sha1 => pbkdf2::PBKDF2_HMAC_SHA1,
            Self::Sha256 => pbkdf2::PBKDF2_HMAC_SHA256,
            Self::Sha512 => pbkdf2::PBKDF2_HMAC_SHA512,
        }
    }

    fn digest(self) -> &'static digest::Algorithm {
        match self {
            #[cfg(test)]
            Self::Sha1 => &digest::SHA1_FOR_LEGACY_USE_ONLY,
            Self::Sha256 => &digest::SHA256,
            Self::Sha512 => &digest::SHA512,
        }
    }

    /// `SaltedPassword`, zeroized on drop.
    fn salted(self, prepared: &[u8], salt: &[u8], iterations: u32) -> Zeroizing<Vec<u8>> {
        let mut out = Zeroizing::new(vec![0u8; self.key_len()]);
        let n = NonZeroU32::new(iterations).unwrap_or(NonZeroU32::MIN);
        pbkdf2::derive(self.pbkdf2(), n, salt, prepared, &mut out);
        out
    }

    /// `(StoredKey, ServerKey)` of a `SaltedPassword`.
    fn keys(self, salted: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let key = hmac::Key::new(self.hmac(), salted);
        let client_key = Zeroizing::new(hmac::sign(&key, b"Client Key").as_ref().to_vec());
        let stored = digest::digest(self.digest(), &client_key).as_ref().to_vec();
        let server = hmac::sign(&key, b"Server Key").as_ref().to_vec();
        (stored, server)
    }

    /// The client's proof: `ClientKey XOR HMAC(StoredKey, AuthMessage)`.
    fn proof(self, salted: &[u8], auth_message: &[u8]) -> Vec<u8> {
        let key = hmac::Key::new(self.hmac(), salted);
        let client_key = Zeroizing::new(hmac::sign(&key, b"Client Key").as_ref().to_vec());
        let stored = digest::digest(self.digest(), &client_key);
        let sig = hmac::sign(&hmac::Key::new(self.hmac(), stored.as_ref()), auth_message);
        client_key
            .iter()
            .zip(sig.as_ref())
            .map(|(a, b)| a ^ b)
            .collect()
    }

    /// Checks a proof against `stored_key`; on success, the server
    /// signature `HMAC(ServerKey, AuthMessage)`.
    fn check(
        self,
        stored_key: &[u8],
        server_key: &[u8],
        auth_message: &[u8],
        proof: &[u8],
    ) -> Option<Vec<u8>> {
        if proof.len() != self.key_len() {
            return None;
        }
        let sig = hmac::sign(&hmac::Key::new(self.hmac(), stored_key), auth_message);
        let client_key: Zeroizing<Vec<u8>> =
            Zeroizing::new(proof.iter().zip(sig.as_ref()).map(|(a, b)| a ^ b).collect());
        let candidate = digest::digest(self.digest(), &client_key);
        bool::from(candidate.as_ref().ct_eq(stored_key)).then(|| {
            hmac::sign(&hmac::Key::new(self.hmac(), server_key), auth_message)
                .as_ref()
                .to_vec()
        })
    }
}

/// `SASLprep` (RFC 4013) of a password; `None` if it isn't UTF-8 or
/// `SASLprep` refuses it. Passwords of printable ASCII come back unchanged.
#[must_use]
pub fn saslprep(password: &[u8]) -> Option<Zeroizing<Vec<u8>>> {
    let s = std::str::from_utf8(password).ok()?;
    let prepared = stringprep::saslprep(s).ok()?;
    Some(Zeroizing::new(prepared.as_bytes().to_vec()))
}

/// A stored SCRAM verifier: the salt, the iteration count, `StoredKey` and
/// `ServerKey`. `Debug` prints the mechanism and iterations only.
#[derive(Clone, PartialEq, Eq)]
pub struct ScramVerifier {
    kind: ScramKind,
    salt: Vec<u8>,
    iterations: u32,
    stored_key: Vec<u8>,
    server_key: Vec<u8>,
}

impl fmt::Debug for ScramVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScramVerifier")
            .field("kind", &self.kind)
            .field("iterations", &self.iterations)
            .finish_non_exhaustive()
    }
}

fn invalid(message: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::ConfigInvalid, message)
}

/// Checks a setting's iteration count.
///
/// # Errors
/// 39998 outside [`MIN_SCRAM_ITERATIONS`]..=[`MAX_SCRAM_ITERATIONS`].
pub fn check_iterations(iterations: u32) -> Result<()> {
    if (MIN_SCRAM_ITERATIONS..=MAX_SCRAM_ITERATIONS).contains(&iterations) {
        Ok(())
    } else {
        Err(invalid(format!(
            "SCRAM iterations {iterations}: expected {MIN_SCRAM_ITERATIONS} to \
             {MAX_SCRAM_ITERATIONS}"
        )))
    }
}

impl ScramVerifier {
    /// The verifier of an already prepared password ([`ScramKind::prepare`])
    /// with `salt` and `iterations`. CPU-heavy; see the module notes.
    #[must_use]
    pub fn derive(kind: ScramKind, prepared: &[u8], salt: &[u8], iterations: u32) -> Self {
        let alg = kind.alg();
        let salted = alg.salted(prepared, salt, iterations);
        let (stored_key, server_key) = alg.keys(&salted);
        Self {
            kind,
            salt: salt.to_vec(),
            iterations,
            stored_key,
            server_key,
        }
    }

    /// The verifier of `password` with `salt` (a fresh one when `None`):
    /// prepares it for `kind`, and returns `Ok(None)` when `SASLprep`
    /// refuses it. CPU-heavy.
    ///
    /// # Errors
    /// 39998 for iterations outside the settings' range or a salt longer
    /// than 64 bytes; 39999 if the OS random generator fails.
    pub fn create(
        kind: ScramKind,
        password: &[u8],
        salt: Option<&[u8]>,
        iterations: u32,
    ) -> Result<Option<Self>> {
        check_iterations(iterations)?;
        let Some(prepared) = kind.prepare(password) else {
            return Ok(None);
        };
        let fresh;
        let salt = if let Some(s) = salt {
            if s.is_empty() || s.len() > MAX_SCRAM_SALT_LEN {
                return Err(invalid("SCRAM salt: 1 to 64 bytes"));
            }
            s
        } else {
            let mut s = [0u8; SCRAM_SALT_LEN];
            getrandom::fill(&mut s).map_err(|e| {
                SprigError::internal(format!("the OS random generator failed: {e}"))
            })?;
            fresh = s;
            &fresh[..]
        };
        Ok(Some(Self::derive(kind, &prepared, salt, iterations)))
    }

    /// Reads a `verifiers` row of a SCRAM mechanism.
    ///
    /// # Errors
    /// `Internal` for another mechanism, a missing column, keys of the
    /// wrong length, or zero iterations.
    pub fn from_row(row: &VerifierRow) -> Result<Self> {
        let bad = |why: &str| {
            SprigError::internal(format!(
                "SCRAM verifier {} of user {}: {why}",
                row.verifier_id, row.user_id
            ))
        };
        let kind = ScramKind::of(row.kind).ok_or_else(|| bad("not a SCRAM-SHA-256/512 row"))?;
        let (Some(salt), Some(iterations), Some(stored_key), Some(server_key)) = (
            row.salt.as_ref(),
            row.iterations,
            row.stored_key.as_ref(),
            row.server_key.as_ref(),
        ) else {
            return Err(bad("a column is NULL"));
        };
        if salt.is_empty() || iterations == 0 {
            return Err(bad("empty salt or zero iterations"));
        }
        if stored_key.len() != kind.key_len() || server_key.len() != kind.key_len() {
            return Err(bad("keys of the wrong length"));
        }
        Ok(Self {
            kind,
            salt: salt.clone(),
            iterations,
            stored_key: stored_key.clone(),
            server_key: server_key.clone(),
        })
    }

    /// The `verifiers` row for `user`.
    #[must_use]
    pub fn to_row(
        &self,
        user_id: UserId,
        slot: Slot,
        generation: u64,
        created_at: SystemTime,
    ) -> VerifierRow {
        VerifierRow {
            verifier_id: 0,
            user_id,
            kind: self.kind.verifier_kind(),
            slot,
            login_form: LoginForm::Any,
            auth_string: None,
            salt: Some(self.salt.clone()),
            iterations: Some(self.iterations),
            stored_key: Some(self.stored_key.clone()),
            server_key: Some(self.server_key.clone()),
            generation,
            created_at,
        }
    }

    /// The mechanism.
    #[must_use]
    pub fn kind(&self) -> ScramKind {
        self.kind
    }

    /// The salt.
    #[must_use]
    pub fn salt(&self) -> &[u8] {
        &self.salt
    }

    /// The PBKDF2 iteration count.
    #[must_use]
    pub fn iterations(&self) -> u32 {
        self.iterations
    }

    /// `StoredKey`.
    #[must_use]
    pub fn stored_key(&self) -> &[u8] {
        &self.stored_key
    }

    /// `ServerKey`.
    #[must_use]
    pub fn server_key(&self) -> &[u8] {
        &self.server_key
    }

    /// Whether `password` is this verifier's: recomputes it with the stored
    /// salt and iterations and compares in constant time. CPU-heavy.
    #[must_use]
    pub fn verify_password(&self, password: &[u8]) -> bool {
        let Some(prepared) = self.kind.prepare(password) else {
            return false;
        };
        let other = Self::derive(self.kind, &prepared, &self.salt, self.iterations);
        bool::from(other.stored_key.ct_eq(&self.stored_key))
            & bool::from(other.server_key.ct_eq(&self.server_key))
    }

    /// The server side of the proof check (RFC 5802 section 3):
    /// `H(proof XOR HMAC(StoredKey, AuthMessage)) == StoredKey`. On success,
    /// the server signature for `v=`.
    #[must_use]
    pub fn check_proof(&self, auth_message: &[u8], proof: &[u8]) -> Option<Vec<u8>> {
        self.kind
            .alg()
            .check(&self.stored_key, &self.server_key, auth_message, proof)
    }
}

/// The client's proof for `password` (unprepared): what a SCRAM client
/// sends. For tests and SprigDB's own clients.
#[must_use]
pub fn client_proof(
    kind: ScramKind,
    password: &[u8],
    salt: &[u8],
    iterations: u32,
    auth_message: &[u8],
) -> Option<Vec<u8>> {
    let prepared = kind.prepare(password)?;
    let alg = kind.alg();
    Some(alg.proof(&alg.salted(&prepared, salt, iterations), auth_message))
}

#[cfg(test)]
mod tests;
