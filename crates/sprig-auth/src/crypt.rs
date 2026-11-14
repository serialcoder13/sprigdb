//! SHA-256 crypt (Ulrich Drepper's "Unix crypt using SHA-256 and SHA-512",
//! the `$5$` scheme), with any salt length.
//!
//! MySQL's `caching_sha2_password` runs this
//! algorithm over all 20 salt bytes, where the reference implementation and
//! common libraries cut the salt at 16, so SprigDB has its own. Only the
//! digest is computed here; [`crate::verifier`] owns the stored format.

use sha2::{Digest, Sha256};
use zeroize::Zeroize;

/// SHA-256 output length.
const SHA256_LEN: usize = 32;

/// Length of the crypt-base64 encoding of a SHA-256 crypt digest.
pub(crate) const ENCODED_LEN: usize = 43;

/// The crypt base64 alphabet (not RFC 4648's).
pub(crate) const B64: &[u8; 64] =
    b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/// The raw SHA-256 crypt digest of `password` with `salt` and `rounds`.
///
/// `salt` is used whole: callers that need the `$5$` scheme's 16-byte limit
/// cut it themselves. Variable names follow the specification (digests A,
/// B, C, DP, DS; sequences P and S).
#[allow(clippy::many_single_char_names)]
pub(crate) fn sha256_crypt_raw(password: &[u8], salt: &[u8], rounds: u32) -> [u8; SHA256_LEN] {
    let pw_len = password.len();

    // Digest B = SHA256(password || salt || password).
    let mut alt_ctx = Sha256::new();
    alt_ctx.update(password);
    alt_ctx.update(salt);
    alt_ctx.update(password);
    let mut b: [u8; SHA256_LEN] = alt_ctx.finalize().into();

    // Digest A.
    let mut ctx = Sha256::new();
    ctx.update(password);
    ctx.update(salt);
    let mut n = pw_len;
    while n > SHA256_LEN {
        ctx.update(b);
        n -= SHA256_LEN;
    }
    ctx.update(&b[..n]);
    let mut n = pw_len;
    while n > 0 {
        if n & 1 == 1 {
            ctx.update(b);
        } else {
            ctx.update(password);
        }
        n >>= 1;
    }
    let mut a: [u8; SHA256_LEN] = ctx.finalize().into();
    b.zeroize();

    // Digest DP: the password repeated once per password byte; P is DP
    // stretched (or cut) to the password's length.
    let mut dp_ctx = Sha256::new();
    for _ in 0..pw_len {
        dp_ctx.update(password);
    }
    let mut dp: [u8; SHA256_LEN] = dp_ctx.finalize().into();
    let mut p = stretch(&dp, pw_len);
    dp.zeroize();

    // Digest DS: the salt repeated 16 + A[0] times; S is DS stretched to the
    // salt's length.
    let mut ds_ctx = Sha256::new();
    for _ in 0..16 + usize::from(a[0]) {
        ds_ctx.update(salt);
    }
    let ds: [u8; SHA256_LEN] = ds_ctx.finalize().into();
    let s = stretch(&ds, salt.len());

    for i in 0..rounds {
        let mut c = Sha256::new();
        if i & 1 == 1 {
            c.update(&p);
        } else {
            c.update(a);
        }
        if i % 3 != 0 {
            c.update(&s);
        }
        if i % 7 != 0 {
            c.update(&p);
        }
        if i & 1 == 1 {
            c.update(a);
        } else {
            c.update(&p);
        }
        a = c.finalize().into();
    }
    p.zeroize();
    a
}

/// `digest` repeated to `len` bytes.
fn stretch(digest: &[u8; SHA256_LEN], len: usize) -> Vec<u8> {
    digest.iter().copied().cycle().take(len).collect()
}

/// The 43-character crypt-base64 form of a SHA-256 crypt digest, with the
/// algorithm's byte permutation.
pub(crate) fn encode(d: &[u8; SHA256_LEN]) -> [u8; ENCODED_LEN] {
    const ORDER: [(usize, usize, usize); 10] = [
        (0, 10, 20),
        (21, 1, 11),
        (12, 22, 2),
        (3, 13, 23),
        (24, 4, 14),
        (15, 25, 5),
        (6, 16, 26),
        (27, 7, 17),
        (18, 28, 8),
        (9, 19, 29),
    ];
    let mut out = [0u8; ENCODED_LEN];
    let mut pos = 0;
    let mut put = |b2: u8, b1: u8, b0: u8, n: usize| {
        let mut w = (u32::from(b2) << 16) | (u32::from(b1) << 8) | u32::from(b0);
        for _ in 0..n {
            out[pos] = B64[(w & 0x3f) as usize];
            pos += 1;
            w >>= 6;
        }
    };
    for (x, y, z) in ORDER {
        put(d[x], d[y], d[z], 4);
    }
    put(0, d[31], d[30], 3);
    out
}

/// The encoded SHA-256 crypt digest: what a stored hash carries.
pub(crate) fn sha256_crypt(password: &[u8], salt: &[u8], rounds: u32) -> [u8; ENCODED_LEN] {
    let mut raw = sha256_crypt_raw(password, salt, rounds);
    let out = encode(&raw);
    raw.zeroize();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn crypt(pw: &str, salt: &str, rounds: u32) -> String {
        let salt = &salt.as_bytes()[..salt.len().min(16)];
        String::from_utf8(sha256_crypt(pw.as_bytes(), salt, rounds).to_vec()).unwrap()
    }

    /// The test vectors from Drepper's specification (the `$5$` scheme, so
    /// salts are cut at 16 bytes and rounds clamped to at least 1000).
    #[test]
    fn drepper_vectors() {
        assert_eq!(
            crypt("Hello world!", "saltstring", 5000),
            "5B8vYYiY.CVt1RlTTf8KbXBH3hsxY/GNooZaBBGWEc5"
        );
        assert_eq!(
            crypt("Hello world!", "saltstringsaltstring", 10000),
            "3xv.VbSHBb41AL9AvLeujZkZRBAwqFMz2.opqey6IcA"
        );
        assert_eq!(
            crypt("This is just a test", "toolongsaltstring", 5000),
            "Un/5jzAHMgOGZ5.mWJpuVolil07guHPvOW8mGRcvxa5"
        );
        assert_eq!(
            crypt(
                "a very much longer text to encrypt.  This one even stretches over morethan one line.",
                "anotherlongsaltstring",
                1400
            ),
            "Rx.j8H.h8HjEDGomFU8bDkXm3XIUnzyxf12oP84Bnq1"
        );
        assert_eq!(
            crypt(
                "we have a short salt string but not a short password",
                "short",
                77777
            ),
            "JiO1O3ZpDAxGJeaDIuqCoEFysAe1mZNJRs3pw0KQRd/"
        );
        assert_eq!(
            crypt("a short string", "asaltof16chars..", 123_456),
            "gP3VQ/6X7UUEW3HkBn2w1/Ptq2jxPyzV/cZKmF/wJvD"
        );
        assert_eq!(
            crypt("the minimum number is still observed", "roundstoolow", 1000),
            "yfvwcWrQ8l/K0DAWyuPMDNHpIVlTQebY9l/gL972bIC"
        );
    }
}
