//! Provider keys, sealed with the master keyring under purpose `ai`:
//!
//! ```text
//! seal(owner O, provider p, secret s):
//!   (kv, K) := keyring.current_for("ai", O)       -- a key for AI keys only
//!   nonce := 12 random bytes
//!   sealed := nonce ‖ AES-256-GCM(K, nonce, s, aad = "sprigai1" ‖ O ‖ p ‖ 0)
//! ```
//!
//! The owner is [`PLATFORM`] for the operator's providers. A sealed key is
//! bound to its owner and provider name: it opens for nothing else, and a
//! key sealed for sinks or workload samples doesn't open here. Keys are
//! opened only in the process that calls the provider, into zeroized
//! buffers, and never returned by any read.

use base64::Engine as _;
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use serde::{Deserialize, Serialize};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use zeroize::Zeroizing;

/// The keyring purpose of provider keys.
pub const PURPOSE: &[u8] = b"ai";
/// The owner of the operator's providers.
pub const PLATFORM: TenantId = TenantId(0);

const AAD_PREFIX: &[u8] = b"sprigai1";

/// Where provider keys' sealing keys come from.
pub trait AiKeyring: Send + Sync {
    /// The key new secrets of `owner` are sealed with, and its version.
    fn current(&self, owner: TenantId) -> Option<(u32, [u8; 32])>;
    /// The key of `version`.
    fn key(&self, owner: TenantId, version: u32) -> Option<[u8; 32]>;
}

impl AiKeyring for sprig_workload::keyring::MasterKeyring {
    fn current(&self, owner: TenantId) -> Option<(u32, [u8; 32])> {
        self.current_for(PURPOSE, owner)
            .map(|(v, k)| (v, *k.bytes()))
    }
    fn key(&self, owner: TenantId, version: u32) -> Option<[u8; 32]> {
        self.key_for(PURPOSE, owner, version).map(|k| *k.bytes())
    }
}

/// A sealed provider key, as stored in the provider's record.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedKey {
    /// The master key version it was sealed under.
    pub key_version: u32,
    /// `nonce ‖ ciphertext ‖ tag`, base64.
    pub sealed: String,
}

impl std::fmt::Debug for SealedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealedKey")
            .field("key_version", &self.key_version)
            .finish_non_exhaustive()
    }
}

fn aad(owner: TenantId, provider: &str) -> Vec<u8> {
    let mut a = Vec::with_capacity(AAD_PREFIX.len() + 9 + provider.len());
    a.extend_from_slice(AAD_PREFIX);
    a.extend_from_slice(&owner.0.to_le_bytes());
    a.extend_from_slice(provider.as_bytes());
    a.push(0);
    a
}

fn invalid(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::AiNotConfigured, msg)
}

fn no_key() -> SprigError {
    invalid("no AI key-sealing key is configured on this process (the master key file)")
}

/// Seals `secret` for `owner`'s provider `provider`.
///
/// # Errors
/// 39990 without a master key.
pub fn seal(
    keys: &dyn AiKeyring,
    owner: TenantId,
    provider: &str,
    secret: &str,
) -> Result<SealedKey> {
    let (kv, k) = keys.current(owner).ok_or_else(no_key)?;
    let k = Zeroizing::new(k);
    let lk = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &*k).map_err(|_| no_key())?);
    let mut nonce = [0u8; NONCE_LEN];
    getrandom::fill(&mut nonce).map_err(|_| SprigError::internal("no random bytes"))?;
    let mut buf = Zeroizing::new(secret.as_bytes().to_vec());
    lk.seal_in_place_append_tag(
        Nonce::assume_unique_for_key(nonce),
        Aad::from(aad(owner, provider)),
        &mut *buf,
    )
    .map_err(|_| SprigError::internal("sealing failed"))?;
    let mut out = nonce.to_vec();
    out.extend_from_slice(&buf);
    Ok(SealedKey {
        key_version: kv,
        sealed: base64::engine::general_purpose::STANDARD.encode(out),
    })
}

/// Opens a sealed key.
///
/// # Errors
/// 39990 when the master key version is gone or the bytes don't open
/// (another owner or provider name, or tampering).
pub fn open(
    keys: &dyn AiKeyring,
    owner: TenantId,
    provider: &str,
    s: &SealedKey,
) -> Result<Zeroizing<String>> {
    let k = Zeroizing::new(keys.key(owner, s.key_version).ok_or_else(no_key)?);
    let raw = base64::engine::general_purpose::STANDARD
        .decode(&s.sealed)
        .map_err(|_| {
            invalid(format!(
                "AI provider {provider}: the sealed key isn't base64"
            ))
        })?;
    if raw.len() < NONCE_LEN + 16 {
        return Err(invalid(format!(
            "AI provider {provider}: the sealed key is too short"
        )));
    }
    let lk = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &*k).map_err(|_| no_key())?);
    let mut nonce = [0u8; NONCE_LEN];
    nonce.copy_from_slice(&raw[..NONCE_LEN]);
    let mut buf = Zeroizing::new(raw[NONCE_LEN..].to_vec());
    let plain = lk
        .open_in_place(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(aad(owner, provider)),
            &mut buf,
        )
        .map_err(|_| {
            invalid(format!(
                "AI provider {provider}: the key doesn't open under master key version {}",
                s.key_version
            ))
        })?;
    let text = std::str::from_utf8(plain)
        .map_err(|_| invalid(format!("AI provider {provider}: the key isn't UTF-8")))?;
    Ok(Zeroizing::new(text.to_owned()))
}

#[cfg(test)]
mod tests {
    use sprig_workload::keyring::MasterKeyring;

    use super::*;

    #[test]
    fn keys_open_only_for_their_owner_and_provider() {
        let ring = MasterKeyring::new([(1, [3u8; 32]), (2, [4u8; 32])]).unwrap();
        let s = seal(&ring, PLATFORM, "anthropic", "sk-ant-secret").unwrap();
        assert_eq!(s.key_version, 2);
        assert!(!s.sealed.contains("secret"));
        assert!(!format!("{s:?}").contains(&s.sealed));
        assert_eq!(
            open(&ring, PLATFORM, "anthropic", &s).unwrap().as_str(),
            "sk-ant-secret"
        );
        assert!(open(&ring, PLATFORM, "openai", &s).is_err());
        assert!(open(&ring, TenantId(7), "anthropic", &s).is_err());
        let mut t = s.clone();
        t.key_version = 1;
        assert!(open(&ring, PLATFORM, "anthropic", &t).is_err());
        // The AI key is neither the sink key nor the workload key.
        let ai = AiKeyring::current(&ring, PLATFORM).unwrap().1;
        let sink = ring.current_for(b"sink", PLATFORM).unwrap().1;
        assert_ne!(ai, *sink.bytes());
        let old = MasterKeyring::new([(1, [3u8; 32])]).unwrap();
        assert!(open(&old, PLATFORM, "anthropic", &s).is_err());
    }
}
