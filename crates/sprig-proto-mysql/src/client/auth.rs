//! Client side of the auth plugins (chapter 05, "Authentication plugins"):
//! the scrambles a client sends.

use sha1::Sha1;
use sha2::{Digest as _, Sha256};

/// `mysql_native_password`.
pub const NATIVE: &str = "mysql_native_password";
/// `caching_sha2_password`.
pub const CACHING_SHA2: &str = "caching_sha2_password";
/// `mysql_clear_password`.
pub const CLEAR: &str = "mysql_clear_password";

/// `caching_sha2_password` `AuthMoreData`: the fast path succeeded.
pub const FAST_AUTH_SUCCESS: u8 = 0x03;
/// `caching_sha2_password` `AuthMoreData`: send the password in full.
pub const PERFORM_FULL_AUTH: u8 = 0x04;

/// `SHA1(password) XOR SHA1(nonce || SHA1(SHA1(password)))`; empty for an
/// empty password.
#[must_use]
pub fn native_scramble(password: &[u8], nonce: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let h1 = Sha1::digest(password);
    let h2 = Sha1::digest(h1);
    let mut h = Sha1::new();
    h.update(nonce);
    h.update(h2);
    let h3 = h.finalize();
    h1.iter().zip(h3.iter()).map(|(a, b)| a ^ b).collect()
}

/// `SHA256(password) XOR SHA256(SHA256(SHA256(password)) || nonce)`; empty
/// for an empty password.
#[must_use]
pub fn caching_sha2_scramble(password: &[u8], nonce: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let h1 = Sha256::digest(password);
    let h2 = Sha256::digest(h1);
    let mut h = Sha256::new();
    h.update(h2);
    h.update(nonce);
    let h3 = h.finalize();
    h1.iter().zip(h3.iter()).map(|(a, b)| a ^ b).collect()
}

/// The first response for `plugin`, or `None` when the client doesn't
/// implement it. `tls` tells whether a clear-text password may be sent.
#[must_use]
pub fn first_response(plugin: &str, password: &[u8], nonce: &[u8], tls: bool) -> Option<Vec<u8>> {
    match plugin {
        NATIVE => Some(native_scramble(password, nonce)),
        CACHING_SHA2 => Some(caching_sha2_scramble(password, nonce)),
        CLEAR if tls => {
            let mut v = password.to_vec();
            v.push(0);
            Some(v)
        }
        _ => None,
    }
}

/// The nonce in plugin data: without the trailing NUL MySQL servers add.
#[must_use]
pub fn nonce_of(data: &[u8]) -> &[u8] {
    data.strip_suffix(&[0]).unwrap_or(data)
}
