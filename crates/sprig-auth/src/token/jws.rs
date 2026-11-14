//! Compact JWS with `EdDSA` (Ed25519, RFC 8037) for SprigDB access tokens
//! and export tickets.
//!
//! A token is `base64url(header) . base64url(payload) . base64url(sig)`;
//! the header is `{"alg":"EdDSA","kid":...,"typ":...}`. Only `EdDSA` is
//! accepted: a token naming any other algorithm (`none`, `HS256`) is
//! refused before a key is looked up, so there is no algorithm confusion.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use sprig_common::{Result, SprigError};

/// The only algorithm.
pub const ALG: &str = "EdDSA";
/// `typ` of access tokens.
pub const TYP_ACCESS: &str = "JWT";
/// `typ` of export tickets.
pub const TYP_EXPORT: &str = "sprig-export";
/// Longest token accepted (a few hundred bytes in practice).
pub const MAX_TOKEN_BYTES: usize = 8192;

/// The protected header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Header {
    /// Always `EdDSA`.
    pub alg: String,
    /// The signing key: `<dc>/<id>`.
    pub kid: String,
    /// `JWT` or `sprig-export`.
    pub typ: String,
}

/// A token split into its parts, not yet verified.
#[derive(Debug, Clone)]
pub struct Parsed {
    /// The header.
    pub header: Header,
    /// The payload's JSON bytes.
    pub payload: Vec<u8>,
    /// `header.payload` as sent: what the signature covers.
    pub signing_input: String,
    /// The signature.
    pub signature: Vec<u8>,
}

fn bad(why: &str) -> SprigError {
    SprigError::standard(1045, *b"28000", format!("Access denied: {why}"))
}

/// Signs `payload` as a compact JWS with `sign` (the Ed25519 signer) under
/// key `kid`.
///
/// # Errors
/// `Internal` if the payload doesn't serialize.
pub fn encode<T: Serialize>(
    kid: &str,
    typ: &str,
    payload: &T,
    sign: impl FnOnce(&[u8]) -> Vec<u8>,
) -> Result<String> {
    let header = Header {
        alg: ALG.to_owned(),
        kid: kid.to_owned(),
        typ: typ.to_owned(),
    };
    let h = serde_json::to_vec(&header).map_err(|e| SprigError::internal(e.to_string()))?;
    let p = serde_json::to_vec(payload).map_err(|e| SprigError::internal(e.to_string()))?;
    let input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(h),
        URL_SAFE_NO_PAD.encode(p)
    );
    let sig = sign(input.as_bytes());
    Ok(format!("{input}.{}", URL_SAFE_NO_PAD.encode(sig)))
}

/// Splits a compact JWS and checks its header: three base64url parts,
/// `alg` `EdDSA`, `typ` as `want_typ`, and a well-formed `kid`.
///
/// # Errors
/// 1045 for anything malformed.
pub fn parse(token: &str, want_typ: &str) -> Result<Parsed> {
    if token.len() > MAX_TOKEN_BYTES {
        return Err(bad("the token is too long"));
    }
    let mut parts = token.split('.');
    let (Some(h), Some(p), Some(s), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(bad("the token is malformed"));
    };
    let dec = |x: &str| {
        URL_SAFE_NO_PAD
            .decode(x)
            .map_err(|_| bad("the token is malformed"))
    };
    let header: Header =
        serde_json::from_slice(&dec(h)?).map_err(|_| bad("the token's header is malformed"))?;
    if header.alg != ALG {
        return Err(bad("the token's algorithm is not accepted"));
    }
    if header.typ != want_typ {
        return Err(bad("the token is of another kind"));
    }
    if !valid_kid(&header.kid) {
        return Err(bad("the token's key id is malformed"));
    }
    Ok(Parsed {
        header,
        payload: dec(p)?,
        signing_input: format!("{h}.{p}"),
        signature: dec(s)?,
    })
}

/// `<dc>/<id>`, each part 1 to 64 of `[A-Za-z0-9._-]`.
#[must_use]
pub fn valid_kid(kid: &str) -> bool {
    let ok = |s: &str| {
        !s.is_empty()
            && s.len() <= 64
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    };
    matches!(kid.split_once('/'), Some((dc, id)) if ok(dc) && ok(id) && !id.contains('/'))
}

/// Checks the Ed25519 signature of `p` with the raw 32-byte `public` key.
///
/// # Errors
/// 1045 when it doesn't verify.
pub fn verify_signature(p: &Parsed, public: &[u8]) -> Result<()> {
    ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, public)
        .verify(p.signing_input.as_bytes(), &p.signature)
        .map_err(|_| bad("the token's signature is not valid"))
}
