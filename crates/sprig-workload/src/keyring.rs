//! Tenant data keys for sealed parameter samples (chapter 12, "Parameter
//! samples for replay": "the tenant's workload data key").
//!
//! Chapter 12 names `sprig_common::crypto::TenantKeyring`, and chapter 14
//! defines no keyring yet (only `auth.factor_key_ref` for TOTP seeds), so
//! M0.4 builds the smallest one samples need, here *(choice: in the crate
//! that seals, until chapter 14 grows a KMS-backed keyring that other
//! chapters share; the trait is the seam)*:
//!
//! - [`TenantKeyring`]: the key new samples are sealed with, and any key
//!   by version, to open older ones;
//! - [`MasterKeyring`]: versioned 256-bit **master secrets** from a file
//!   only the gateway and the workers can read; each tenant's data key is
//!   `HKDF-SHA256(salt = "sprigdb workload data key", ikm = master_v,
//!   info = "workload" ‖ tenant id as u64 LE)`, so no per-tenant key is
//!   stored anywhere and a tenant's key is useless for another tenant.
//!
//! **Rotation.** Add a master secret with a higher version: new samples
//! are sealed under it at once (the highest version is current), older
//! samples still open under theirs. Remove a version after
//! `workload.sample_retention_days` (7), when every sample sealed under it
//! has expired; a sample whose version is gone can't be opened and is
//! skipped. The version is stored with each sample (`key_version`).
//!
//! **Key file.** One secret per line: `<version> <64 hex digits>`; blank
//! lines and `#` comments are ignored. Generate a secret with
//! `openssl rand -hex 32`.

use std::collections::BTreeMap;
use std::path::Path;

use ring::hkdf;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use zeroize::Zeroizing;

/// One tenant's 256-bit workload data key. Zeroed on drop; never printed.
#[derive(Clone)]
pub struct DataKey(Zeroizing<[u8; 32]>);

impl DataKey {
    /// A key from its bytes.
    #[must_use]
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(Zeroizing::new(bytes))
    }

    /// The bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Debug for DataKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DataKey(..)")
    }
}

/// Where sample keys come from (chapter 12's `TenantKeyring`).
pub trait TenantKeyring: Send + Sync {
    /// The key new samples of `tenant` are sealed with, with its version.
    /// `None` when no key is available: samples are then not captured,
    /// and statistics continue (chapter 12, "Failure handling").
    fn current(&self, tenant: TenantId) -> Option<(u32, DataKey)>;

    /// The key of `version`, to open a sample sealed under it. `None` when
    /// that version is gone.
    fn key(&self, tenant: TenantId, version: u32) -> Option<DataKey>;
}

/// A keyring with no keys: nothing is captured, nothing opens.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoKeys;

impl TenantKeyring for NoKeys {
    fn current(&self, _tenant: TenantId) -> Option<(u32, DataKey)> {
        None
    }

    fn key(&self, _tenant: TenantId, _version: u32) -> Option<DataKey> {
        None
    }
}

/// Versioned master secrets; per-tenant keys derived with HKDF-SHA256
/// (module docs).
pub struct MasterKeyring {
    masters: BTreeMap<u32, Zeroizing<[u8; 32]>>,
}

impl std::fmt::Debug for MasterKeyring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MasterKeyring")
            .field("versions", &self.masters.keys().collect::<Vec<_>>())
            .finish()
    }
}

fn bad(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::ConfigInvalid, msg)
}

/// HKDF's output length, as `ring` wants it.
struct Len32;

impl hkdf::KeyType for Len32 {
    fn len(&self) -> usize {
        32
    }
}

const SALT: &[u8] = b"sprigdb workload data key";

impl MasterKeyring {
    /// A keyring of `(version, secret)` pairs.
    ///
    /// # Errors
    /// 39998 for version 0 or a repeated version.
    pub fn new(masters: impl IntoIterator<Item = (u32, [u8; 32])>) -> Result<Self> {
        let mut m = BTreeMap::new();
        for (v, s) in masters {
            if v == 0 {
                return Err(bad("workload sample keys: version 0 is reserved"));
            }
            if m.insert(v, Zeroizing::new(s)).is_some() {
                return Err(bad(format!("workload sample keys: version {v} twice")));
            }
        }
        Ok(Self { masters: m })
    }

    /// Parses a key file's text (module docs).
    ///
    /// # Errors
    /// 39998 for a malformed line; the secret itself is never in the
    /// message.
    pub fn parse(text: &str) -> Result<Self> {
        let mut pairs = Vec::new();
        for (n, line) in text.lines().enumerate() {
            let line = line.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            let mut it = line.split_whitespace();
            let (Some(v), Some(hex), None) = (it.next(), it.next(), it.next()) else {
                return Err(bad(format!(
                    "workload sample keys, line {}: expected `<version> <64 hex digits>`",
                    n + 1
                )));
            };
            let v: u32 = v
                .parse()
                .map_err(|_| bad(format!("workload sample keys, line {}: bad version", n + 1)))?;
            let secret = parse_hex32(hex).ok_or_else(|| {
                bad(format!(
                    "workload sample keys, line {}: the secret must be 64 hex digits",
                    n + 1
                ))
            })?;
            pairs.push((v, secret));
        }
        Self::new(pairs)
    }

    /// Reads a key file (module docs).
    ///
    /// # Errors
    /// 39998 if it can't be read or is malformed.
    pub fn from_file(path: &Path) -> Result<Self> {
        let text = Zeroizing::new(std::fs::read_to_string(path).map_err(|e| {
            bad(format!(
                "workload sample keys: can't read {}: {e}",
                path.display()
            ))
        })?);
        Self::parse(&text)
    }

    /// The versions held, ascending.
    #[must_use]
    pub fn versions(&self) -> Vec<u32> {
        self.masters.keys().copied().collect()
    }

    fn derive(master: &[u8; 32], tenant: TenantId) -> Option<DataKey> {
        let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, SALT).extract(master);
        let tid = tenant.0.to_le_bytes();
        let info: [&[u8]; 2] = [b"workload", &tid];
        let okm = prk.expand(&info, Len32).ok()?;
        let mut out = [0u8; 32];
        okm.fill(&mut out).ok()?;
        let k = DataKey::new(out);
        zeroize::Zeroize::zeroize(&mut out);
        Some(k)
    }
}

impl TenantKeyring for MasterKeyring {
    fn current(&self, tenant: TenantId) -> Option<(u32, DataKey)> {
        let (v, m) = self.masters.iter().next_back()?;
        Some((*v, Self::derive(m, tenant)?))
    }

    fn key(&self, tenant: TenantId, version: u32) -> Option<DataKey> {
        Self::derive(self.masters.get(&version)?, tenant)
    }
}

fn parse_hex32(s: &str) -> Option<[u8; 32]> {
    let b = s.as_bytes();
    if b.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, pair) in b.chunks_exact(2).enumerate() {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out[i] = u8::try_from(hi * 16 + lo).ok()?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_per_tenant_and_per_version() {
        let k = MasterKeyring::new([(1, [7u8; 32]), (2, [9u8; 32])]).unwrap();
        let (v, cur) = k.current(TenantId(1)).unwrap();
        assert_eq!(v, 2);
        assert_eq!(cur.bytes(), k.key(TenantId(1), 2).unwrap().bytes());
        assert_ne!(cur.bytes(), k.key(TenantId(2), 2).unwrap().bytes());
        assert_ne!(cur.bytes(), k.key(TenantId(1), 1).unwrap().bytes());
        assert!(k.key(TenantId(1), 3).is_none());
        assert_eq!(format!("{cur:?}"), "DataKey(..)");
        assert!(NoKeys.current(TenantId(1)).is_none());
    }

    #[test]
    fn key_files_parse_and_never_echo_secrets() {
        let hex = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        let k = MasterKeyring::parse(&format!("# dev\n\n1 {hex}\n2 {hex} # rotated\n")).unwrap();
        assert_eq!(k.versions(), vec![1, 2]);
        for bad in [
            "1",
            "x 00",
            "1 0011",
            &format!("0 {hex}"),
            &format!("1 {hex}\n1 {hex}"),
        ] {
            let e = MasterKeyring::parse(bad).unwrap_err();
            assert!(!e.to_string().contains("0011223344"), "{e}");
        }
        assert!(!format!("{k:?}").contains("0011"));
    }
}
