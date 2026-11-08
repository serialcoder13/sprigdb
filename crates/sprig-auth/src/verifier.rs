//! Password verifiers for the MySQL mechanisms (chapter 14, "Verifier
//! formats"; build step 2).
//!
//! ```text
//! caching_sha2_password (70 chars):  "$A$" || RRR || "$" || salt || digest
//!   RRR    = rounds / 1000 as 3 hex digits ("005" = 5000 rounds; up to 0xFFF)
//!   salt   = 20 bytes in 0x01..0x7F, excluding '$'
//!   digest = SHA-256 crypt (Drepper), 43 crypt-base64 characters, using all
//!            20 salt bytes
//!   fast-path cache value (memory only) = SHA256(SHA256(password))
//!
//! mysql_native_password:  "*" || HEX(SHA1(SHA1(password)))
//! ```
//!
//! Both match what MySQL 8.4 stores in `mysql.user.authentication_string`,
//! so a MySQL hash imports unchanged and the tests check against hashes a
//! MySQL 8.4 server produced. An empty password has an empty verifier, as in
//! MySQL.
//!
//! The scramble checks are the server side of chapter 05's plugin exchange:
//! [`verify_scramble`] for the `caching_sha2_password` fast path and
//! [`verify_native_scramble`] for `mysql_native_password`. Every comparison
//! of secret-derived bytes is constant-time.
//!
//! [`CachingSha2Verifier::verify`] costs `rounds` SHA-256 computations (about
//! a millisecond at the default 5000): call it off the async runtime
//! (`spawn_blocking`).

use std::fmt;
use std::str::FromStr;

use sha1::Sha1;
use sha2::{Digest, Sha256};
use sprig_common::{Result, SprigCode, SprigError};
use subtle::ConstantTimeEq;
use zeroize::Zeroize;

use crate::crypt;

/// `auth.caching_sha2_rounds`' default (chapter 14) and MySQL's
/// `caching_sha2_password_digest_rounds` default.
pub const DEFAULT_CACHING_SHA2_ROUNDS: u32 = 5000;

/// The fewest rounds a verifier may have: SHA-256 crypt's minimum.
pub const MIN_CACHING_SHA2_ROUNDS: u32 = 1000;

/// The most rounds the three-hex-digit field can hold.
pub const MAX_CACHING_SHA2_ROUNDS: u32 = 0xFFF * 1000;

/// Salt length of a `caching_sha2_password` verifier.
pub const SALT_LEN: usize = 20;

/// Length of a stored `caching_sha2_password` verifier.
pub const CACHING_SHA2_LEN: usize = 70;

/// Length of the scramble nonce both MySQL plugins use.
pub const NONCE_LEN: usize = 20;

const PREFIX: &[u8] = b"$A$";
/// `$A$` + 3 hex digits + `$`.
const HEADER_LEN: usize = 7;

fn invalid(message: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::ConfigInvalid, message)
}

/// A stored `caching_sha2_password` verifier.
///
/// `Debug` prints the rounds only.
#[derive(Clone, PartialEq, Eq)]
pub struct CachingSha2Verifier(Sha2Repr);

#[derive(Clone, PartialEq, Eq)]
enum Sha2Repr {
    /// The empty password: MySQL stores an empty string.
    Empty,
    Hashed {
        rounds: u32,
        salt: [u8; SALT_LEN],
        digest: [u8; crypt::ENCODED_LEN],
    },
}

impl fmt::Debug for CachingSha2Verifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Sha2Repr::Empty => f.write_str("CachingSha2Verifier(empty)"),
            Sha2Repr::Hashed { rounds, .. } => {
                write!(f, "CachingSha2Verifier(rounds = {rounds})")
            }
        }
    }
}

impl CachingSha2Verifier {
    /// A verifier for `password` with a fresh random salt and `rounds`
    /// (a multiple of 1000, from [`MIN_CACHING_SHA2_ROUNDS`] to
    /// [`MAX_CACHING_SHA2_ROUNDS`]).
    ///
    /// The salt is drawn from the crypt-base64 alphabet (`./0-9A-Za-z`,
    /// 120 bits over 20 bytes): a subset of chapter 14's range, so the
    /// verifier is printable and needs no escaping in TOML or SQL.
    ///
    /// # Errors
    /// 39998 for rounds outside the range; 39999 if the operating system's
    /// random generator fails.
    pub fn create(password: &[u8], rounds: u32) -> Result<Self> {
        check_rounds(rounds)?;
        if password.is_empty() {
            return Ok(Self(Sha2Repr::Empty));
        }
        let mut salt = [0u8; SALT_LEN];
        getrandom::fill(&mut salt)
            .map_err(|e| SprigError::internal(format!("the OS random generator failed: {e}")))?;
        for b in &mut salt {
            *b = crypt::B64[usize::from(*b & 0x3f)];
        }
        Self::with_salt(password, salt, rounds)
    }

    /// A verifier for `password` with a given salt: for tests and imports.
    ///
    /// # Errors
    /// 39998 for rounds outside the range or a salt byte outside
    /// 0x01..0x7F or equal to `$`.
    pub fn with_salt(password: &[u8], salt: [u8; SALT_LEN], rounds: u32) -> Result<Self> {
        check_rounds(rounds)?;
        check_salt(&salt)?;
        if password.is_empty() {
            return Ok(Self(Sha2Repr::Empty));
        }
        Ok(Self(Sha2Repr::Hashed {
            rounds,
            salt,
            digest: crypt::sha256_crypt(password, &salt, rounds),
        }))
    }

    /// The verifier of the empty password.
    #[must_use]
    pub const fn empty() -> Self {
        Self(Sha2Repr::Empty)
    }

    /// Parses a stored verifier: 70 bytes as above, or empty for the empty
    /// password.
    ///
    /// # Errors
    /// 39998 if `stored` is not in chapter 14's format.
    pub fn parse(stored: &[u8]) -> Result<Self> {
        if stored.is_empty() {
            return Ok(Self(Sha2Repr::Empty));
        }
        let bad = |why: &str| invalid(format!("caching_sha2_password verifier: {why}"));
        if stored.len() != CACHING_SHA2_LEN {
            return Err(bad(&format!(
                "{} bytes, expected {CACHING_SHA2_LEN}",
                stored.len()
            )));
        }
        if &stored[..3] != PREFIX || stored[6] != b'$' {
            return Err(bad("expected \"$A$RRR$\" first"));
        }
        let rounds = std::str::from_utf8(&stored[3..6])
            .ok()
            .and_then(|h| u32::from_str_radix(h, 16).ok())
            .ok_or_else(|| bad("the rounds field isn't 3 hex digits"))?
            * 1000;
        check_rounds(rounds)?;
        let mut salt = [0u8; SALT_LEN];
        salt.copy_from_slice(&stored[HEADER_LEN..HEADER_LEN + SALT_LEN]);
        check_salt(&salt)?;
        let mut digest = [0u8; crypt::ENCODED_LEN];
        digest.copy_from_slice(&stored[HEADER_LEN + SALT_LEN..]);
        if !digest.iter().all(|b| crypt::B64.contains(b)) {
            return Err(bad("the digest isn't crypt base64"));
        }
        Ok(Self(Sha2Repr::Hashed {
            rounds,
            salt,
            digest,
        }))
    }

    /// The stored form: 70 bytes, or empty for the empty password.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        match &self.0 {
            Sha2Repr::Empty => Vec::new(),
            Sha2Repr::Hashed {
                rounds,
                salt,
                digest,
            } => {
                let mut out = Vec::with_capacity(CACHING_SHA2_LEN);
                out.extend_from_slice(PREFIX);
                // MySQL writes the field with "%03X".
                out.extend_from_slice(format!("{:03X}", rounds / 1000).as_bytes());
                out.push(b'$');
                out.extend_from_slice(salt);
                out.extend_from_slice(digest);
                out
            }
        }
    }

    /// Whether this is the empty password's verifier.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        matches!(self.0, Sha2Repr::Empty)
    }

    /// The SHA-256 crypt rounds; 0 for the empty password.
    #[must_use]
    pub fn rounds(&self) -> u32 {
        match &self.0 {
            Sha2Repr::Empty => 0,
            Sha2Repr::Hashed { rounds, .. } => *rounds,
        }
    }

    /// Checks a clear-text password (the full path, chapter 14 step 2):
    /// recomputes the digest with the stored salt and rounds and compares
    /// in constant time. CPU-heavy; see the module notes.
    #[must_use]
    pub fn verify(&self, password: &[u8]) -> bool {
        match &self.0 {
            Sha2Repr::Empty => password.is_empty(),
            // MySQL refuses an empty password for a hashed account without
            // hashing.
            Sha2Repr::Hashed { .. } if password.is_empty() => false,
            Sha2Repr::Hashed {
                rounds,
                salt,
                digest,
            } => {
                let computed = crypt::sha256_crypt(password, salt, *rounds);
                bool::from(computed.ct_eq(digest))
            }
        }
    }
}

impl fmt::Display for CachingSha2Verifier {
    /// The stored form. Always ASCII: salt bytes are below 0x80.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&String::from_utf8_lossy(&self.to_bytes()))
    }
}

impl FromStr for CachingSha2Verifier {
    type Err = SprigError;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s.as_bytes())
    }
}

fn check_rounds(rounds: u32) -> Result<()> {
    if !rounds.is_multiple_of(1000)
        || !(MIN_CACHING_SHA2_ROUNDS..=MAX_CACHING_SHA2_ROUNDS).contains(&rounds)
    {
        return Err(invalid(format!(
            "caching_sha2_password rounds {rounds}: expected a multiple of 1000 from \
             {MIN_CACHING_SHA2_ROUNDS} to {MAX_CACHING_SHA2_ROUNDS}"
        )));
    }
    Ok(())
}

fn check_salt(salt: &[u8; SALT_LEN]) -> Result<()> {
    if salt.iter().any(|&b| b == 0 || b >= 0x80 || b == b'$') {
        return Err(invalid(
            "caching_sha2_password salt: bytes must be in 0x01..0x7F and not '$'",
        ));
    }
    Ok(())
}

/// A stored `mysql_native_password` verifier: `SHA1(SHA1(password))`.
///
/// `Debug` prints nothing secret.
#[derive(Clone, PartialEq, Eq)]
pub struct NativeVerifier(Option<[u8; 20]>);

impl fmt::Debug for NativeVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_some() {
            "NativeVerifier(..)"
        } else {
            "NativeVerifier(empty)"
        })
    }
}

impl NativeVerifier {
    /// The verifier of `password`.
    #[must_use]
    pub fn create(password: &[u8]) -> Self {
        if password.is_empty() {
            return Self(None);
        }
        let mut stage1: [u8; 20] = Sha1::digest(password).into();
        let stage2: [u8; 20] = Sha1::digest(stage1).into();
        stage1.zeroize();
        Self(Some(stage2))
    }

    /// The verifier of the empty password.
    #[must_use]
    pub const fn empty() -> Self {
        Self(None)
    }

    /// Parses `*` followed by 40 hex digits (either case), or empty for the
    /// empty password.
    ///
    /// # Errors
    /// 39998 if `stored` is not in that format.
    pub fn parse(stored: &str) -> Result<Self> {
        if stored.is_empty() {
            return Ok(Self(None));
        }
        let hex = stored
            .strip_prefix('*')
            .filter(|h| h.len() == 40)
            .ok_or_else(|| {
                invalid("mysql_native_password verifier: expected '*' and 40 hex digits")
            })?;
        let mut out = [0u8; 20];
        decode_hex(hex, &mut out)
            .ok_or_else(|| invalid("mysql_native_password verifier: not hex"))?;
        Ok(Self(Some(out)))
    }

    /// Whether this is the empty password's verifier.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    /// Checks a clear-text password in constant time.
    #[must_use]
    pub fn verify(&self, password: &[u8]) -> bool {
        match &self.0 {
            None => password.is_empty(),
            Some(_) if password.is_empty() => false,
            Some(stored) => bool::from(Self::create(password).stage2().ct_eq(stored)),
        }
    }

    fn stage2(&self) -> &[u8] {
        self.0.as_ref().map_or(&[], |s| s.as_slice())
    }
}

impl fmt::Display for NativeVerifier {
    /// The stored form: `*` and 40 upper-case hex digits, as MySQL writes it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(s) = &self.0 {
            f.write_str("*")?;
            for b in s {
                write!(f, "{b:02X}")?;
            }
        }
        Ok(())
    }
}

impl FromStr for NativeVerifier {
    type Err = SprigError;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

/// Decodes hex (either case) into `out`, which must be exactly half as long.
pub(crate) fn decode_hex(hex: &str, out: &mut [u8]) -> Option<()> {
    if hex.len() != out.len() * 2 {
        return None;
    }
    for (o, pair) in out.iter_mut().zip(hex.as_bytes().chunks_exact(2)) {
        let hi = char::from(pair[0]).to_digit(16)?;
        let lo = char::from(pair[1]).to_digit(16)?;
        *o = u8::try_from(hi << 4 | lo).ok()?;
    }
    Some(())
}

/// The `caching_sha2_password` fast-path cache value, `SHA256(SHA256(password))`
/// (chapter 14, "Gateway caches"). It is password-equivalent for the fast
/// path, so it lives in gateway memory only: it is zeroized on drop and
/// `Debug` hides it. There is no `PartialEq`: compare through
/// [`verify_scramble`], which is constant-time.
#[derive(Clone)]
pub struct FastCacheValue([u8; 32]);

impl Drop for FastCacheValue {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl fmt::Debug for FastCacheValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FastCacheValue(..)")
    }
}

impl FastCacheValue {
    /// `SHA256(SHA256(password))`. Fill the cache only after a full
    /// authentication with this password succeeded.
    #[must_use]
    pub fn from_password(password: &[u8]) -> Self {
        let mut stage1: [u8; 32] = Sha256::digest(password).into();
        let stage2: [u8; 32] = Sha256::digest(stage1).into();
        stage1.zeroize();
        Self(stage2)
    }

    /// The raw 32 bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Checks a `caching_sha2_password` fast-path response (chapter 05,
/// "Authentication plugins", step 3): the client sent
/// `SHA256(password) XOR SHA256(SHA256(SHA256(password)) || nonce)`, so
/// `candidate = response XOR SHA256(stage2 || nonce)` must hash to `stage2`.
///
/// `nonce` is the 20-byte scramble the server sent (without the greeting's
/// trailing NUL). A response that isn't 32 bytes fails; the empty password
/// takes chapter 05's step 2 instead.
#[must_use]
pub fn verify_scramble(nonce: &[u8], client_response: &[u8], stage2: &FastCacheValue) -> bool {
    let Ok(response) = <&[u8; 32]>::try_from(client_response) else {
        return false;
    };
    let mut ctx = Sha256::new();
    ctx.update(stage2.0);
    ctx.update(nonce);
    let mut candidate: [u8; 32] = ctx.finalize().into();
    for (c, r) in candidate.iter_mut().zip(response) {
        *c ^= r;
    }
    let check: [u8; 32] = Sha256::digest(candidate).into();
    candidate.zeroize();
    bool::from(check.ct_eq(&stage2.0))
}

/// Checks a `mysql_native_password` response (chapter 05): the client sent
/// `SHA1(password) XOR SHA1(nonce || SHA1(SHA1(password)))`, so
/// `candidate = response XOR SHA1(nonce || stored)` must hash to `stored`.
///
/// An empty verifier accepts only an empty response, and a non-empty one
/// never does.
#[must_use]
pub fn verify_native_scramble(
    nonce: &[u8],
    client_response: &[u8],
    verifier: &NativeVerifier,
) -> bool {
    let Some(stored) = &verifier.0 else {
        return client_response.is_empty();
    };
    let Ok(response) = <&[u8; 20]>::try_from(client_response) else {
        return false;
    };
    let mut ctx = Sha1::new();
    ctx.update(nonce);
    ctx.update(stored);
    let mut candidate: [u8; 20] = ctx.finalize().into();
    for (c, r) in candidate.iter_mut().zip(response) {
        *c ^= r;
    }
    let check: [u8; 20] = Sha1::digest(candidate).into();
    candidate.zeroize();
    bool::from(check.ct_eq(stored))
}

/// The client's `caching_sha2_password` scramble for `password` and `nonce`:
/// what a MySQL client sends. For tests and for SprigDB's own clients.
#[must_use]
pub fn caching_sha2_scramble(password: &[u8], nonce: &[u8]) -> [u8; 32] {
    if password.is_empty() {
        return [0; 32];
    }
    let mut stage1: [u8; 32] = Sha256::digest(password).into();
    let stage2: [u8; 32] = Sha256::digest(stage1).into();
    let mut ctx = Sha256::new();
    ctx.update(stage2);
    ctx.update(nonce);
    let mask: [u8; 32] = ctx.finalize().into();
    for (s, m) in stage1.iter_mut().zip(mask) {
        *s ^= m;
    }
    stage1
}

/// The client's `mysql_native_password` scramble for `password` and `nonce`;
/// empty for the empty password. For tests and SprigDB's own clients.
#[must_use]
pub fn native_scramble(password: &[u8], nonce: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let mut stage1: [u8; 20] = Sha1::digest(password).into();
    let stage2: [u8; 20] = Sha1::digest(stage1).into();
    let mut ctx = Sha1::new();
    ctx.update(nonce);
    ctx.update(stage2);
    let mask: [u8; 20] = ctx.finalize().into();
    let out = stage1.iter().zip(mask).map(|(s, m)| s ^ m).collect();
    stage1.zeroize();
    out
}

/// The decoy check for an unknown account (chapter 14, "MySQL logins" step
/// 1): the same SHA-256 crypt work as a real full authentication at the
/// default cost, always failing, so timing doesn't reveal which accounts
/// exist.
#[must_use]
pub fn decoy_verify(password: &[u8]) -> bool {
    const DECOY_SALT: &[u8; SALT_LEN] = b"sprigdb-decoy-salt..";
    let out = crypt::sha256_crypt(password, DECOY_SALT, DEFAULT_CACHING_SHA2_ROUNDS);
    std::hint::black_box(out);
    false
}

/// The verifiers stored for one password: what `sprigctl` (or the accounts
/// file's author) writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PasswordVerifiers {
    /// The `caching_sha2_password` verifier.
    pub caching_sha2: CachingSha2Verifier,
    /// The `mysql_native_password` verifier.
    pub native: NativeVerifier,
}

impl PasswordVerifiers {
    /// Both verifiers of `password`, the `caching_sha2_password` one with
    /// `rounds` (normally [`DEFAULT_CACHING_SHA2_ROUNDS`]).
    ///
    /// # Errors
    /// As [`CachingSha2Verifier::create`].
    pub fn from_password(password: &[u8], rounds: u32) -> Result<Self> {
        Ok(Self {
            caching_sha2: CachingSha2Verifier::create(password, rounds)?,
            native: NativeVerifier::create(password),
        })
    }
}

/// The `caching_sha2_password` verifier string of `password` at the default
/// rounds: the one-call helper for tools such as `sprigctl`.
///
/// # Errors
/// As [`CachingSha2Verifier::create`].
pub fn caching_sha2_string(password: &str) -> Result<String> {
    Ok(CachingSha2Verifier::create(password.as_bytes(), DEFAULT_CACHING_SHA2_ROUNDS)?.to_string())
}

#[cfg(test)]
mod tests;
