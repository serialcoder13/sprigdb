//! Credentials (chapter 51, "Credentials"; build step 3):
//!
//! ```text
//! seal(tenant T, connection c, name n, value v):
//!   (kv, K) := keyring.current_for("sink", T)        -- chapter 14, with a purpose
//!   nonce := 12 random bytes
//!   sealed := nonce ‖ AES-256-GCM(K, nonce, v, aad = "sprigsk1" ‖ T ‖ c ‖ n ‖ version)
//! ```
//!
//! The keyring is chapter 12's master-secret file (`MasterKeyring`), which
//! derives a tenant key per purpose with HKDF; sink credentials use the
//! purpose `sink`, so their key opens nothing else. Values are opened only
//! in sink workers, into zeroized buffers.

use ring::aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use zeroize::Zeroizing;

use crate::model::err;

/// The purpose label of sink credential keys.
pub const PURPOSE: &[u8] = b"sink";

const AAD_PREFIX: &[u8] = b"sprigsk1";

/// Where sink credential keys come from.
pub trait SinkKeyring: Send + Sync {
    /// The key new credentials of `tenant` are sealed with, and its
    /// version.
    fn current(&self, tenant: TenantId) -> Option<(u32, [u8; 32])>;
    /// The key of `version`.
    fn key(&self, tenant: TenantId, version: u32) -> Option<[u8; 32]>;
}

impl SinkKeyring for sprig_workload::keyring::MasterKeyring {
    fn current(&self, tenant: TenantId) -> Option<(u32, [u8; 32])> {
        self.current_for(PURPOSE, tenant).map(|(v, k)| (v, *k.bytes()))
    }
    fn key(&self, tenant: TenantId, version: u32) -> Option<[u8; 32]> {
        self.key_for(PURPOSE, tenant, version).map(|k| *k.bytes())
    }
}

fn aad(tenant: TenantId, conn: u64, name: &str, version: u32) -> Vec<u8> {
    let mut a = Vec::with_capacity(AAD_PREFIX.len() + 24 + name.len());
    a.extend_from_slice(AAD_PREFIX);
    a.extend_from_slice(&tenant.0.to_le_bytes());
    a.extend_from_slice(&conn.to_le_bytes());
    a.extend_from_slice(name.as_bytes());
    a.push(0);
    a.extend_from_slice(&version.to_le_bytes());
    a
}

fn no_key() -> SprigError {
    err(
        SprigCode::SinkCredentialsInvalid,
        "no sink credential key is configured on this process (the master key file)",
    )
}

/// Seals one credential: `(key version, nonce ‖ ciphertext ‖ tag)`.
///
/// # Errors
/// 39216 without a key.
pub fn seal(
    keys: &dyn SinkKeyring,
    tenant: TenantId,
    conn: u64,
    name: &str,
    version: u32,
    value: &str,
) -> Result<(u32, Vec<u8>)> {
    let (kv, k) = keys.current(tenant).ok_or_else(no_key)?;
    let key = Zeroizing::new(k);
    let lk = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &*key).map_err(|_| no_key())?);
    let mut nonce = [0u8; NONCE_LEN];
    getrandom::fill(&mut nonce).map_err(|_| SprigError::internal("no random bytes"))?;
    let mut buf = value.as_bytes().to_vec();
    lk.seal_in_place_append_tag(
        Nonce::assume_unique_for_key(nonce),
        Aad::from(aad(tenant, conn, name, version)),
        &mut buf,
    )
    .map_err(|_| SprigError::internal("sealing failed"))?;
    let mut out = nonce.to_vec();
    out.extend_from_slice(&buf);
    zeroize::Zeroize::zeroize(&mut buf);
    Ok((kv, out))
}

/// Opens one credential.
///
/// # Errors
/// 39216 when the key version is gone or the bytes don't open (another
/// tenant, connection, name or version).
pub fn open(
    keys: &dyn SinkKeyring,
    tenant: TenantId,
    conn: u64,
    name: &str,
    version: u32,
    key_version: u32,
    sealed: &[u8],
) -> Result<Zeroizing<String>> {
    let k = Zeroizing::new(keys.key(tenant, key_version).ok_or_else(no_key)?);
    if sealed.len() < NONCE_LEN + 16 {
        return Err(err(SprigCode::SinkCredentialsInvalid, "a sealed credential is too short"));
    }
    let lk = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &*k).map_err(|_| no_key())?);
    let mut nonce = [0u8; NONCE_LEN];
    nonce.copy_from_slice(&sealed[..NONCE_LEN]);
    let mut buf = Zeroizing::new(sealed[NONCE_LEN..].to_vec());
    let plain = lk
        .open_in_place(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(aad(tenant, conn, name, version)),
            &mut buf,
        )
        .map_err(|_| {
            err(
                SprigCode::SinkCredentialsInvalid,
                format!("credential {name} doesn't open under key version {key_version}"),
            )
        })?;
    let s = std::str::from_utf8(plain)
        .map_err(|_| err(SprigCode::SinkCredentialsInvalid, "a credential isn't UTF-8"))?;
    Ok(Zeroizing::new(s.to_owned()))
}

/// The credentials each connection type takes, and which it needs
/// (chapter 51: 39216 for any other, or a missing one).
#[must_use]
pub fn allowed_credentials(kind: crate::model::ConnKind) -> &'static [&'static str] {
    use crate::model::ConnKind;
    match kind {
        ConnKind::Webhook => &[
            "token",
            "username",
            "password",
            "header_value",
            "client_id",
            "client_secret",
            "signing_secret",
            "tls_cert",
            "tls_key",
        ],
        ConnKind::Kafka => &["username", "password", "tls_cert", "tls_key", "client_secret"],
        ConnKind::S3 => &["access_key_id", "secret_access_key", "session_token"],
        ConnKind::Mysql | ConnKind::Postgresql => &["username", "password", "tls_cert", "tls_key"],
    }
}

/// Checks credential names against the connection type.
///
/// # Errors
/// 39216.
pub fn check_names(kind: crate::model::ConnKind, names: &[&str]) -> Result<()> {
    let ok = allowed_credentials(kind);
    for n in names {
        if !ok.contains(n) {
            return Err(err(
                SprigCode::SinkCredentialsInvalid,
                format!(
                    "{kind} connections take no credential {n} (they take {})",
                    ok.join(", ")
                ),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct K;
    impl SinkKeyring for K {
        fn current(&self, t: TenantId) -> Option<(u32, [u8; 32])> {
            self.key(t, 3).map(|k| (3, k))
        }
        fn key(&self, t: TenantId, v: u32) -> Option<[u8; 32]> {
            let mut k = [7u8; 32];
            k[0] = u8::try_from(t.0 % 256).ok()?;
            k[1] = u8::try_from(v).ok()?;
            Some(k)
        }
    }

    #[test]
    fn seal_and_open_bind_tenant_connection_name_and_version() {
        let (kv, s) = seal(&K, TenantId(5), 9, "password", 1, "hunter2").unwrap();
        assert_eq!(kv, 3);
        assert!(!s.windows(7).any(|w| w == b"hunter2"));
        assert_eq!(
            open(&K, TenantId(5), 9, "password", 1, kv, &s).unwrap().as_str(),
            "hunter2"
        );
        for (t, c, n, v) in [(6, 9, "password", 1), (5, 8, "password", 1), (5, 9, "token", 1), (5, 9, "password", 2)] {
            assert!(open(&K, TenantId(t), c, n, v, kv, &s).is_err());
        }
        let real = sprig_workload::keyring::MasterKeyring::new([(1, [1u8; 32])]).unwrap();
        let (kv, s) = seal(&real, TenantId(1), 1, "token", 1, "t").unwrap();
        assert_eq!(open(&real, TenantId(1), 1, "token", 1, kv, &s).unwrap().as_str(), "t");
        // The sink key isn't the workload key.
        let w = sprig_workload::keyring::TenantKeyring::current(&real, TenantId(1)).unwrap().1;
        assert_ne!(*w.bytes(), SinkKeyring::current(&real, TenantId(1)).unwrap().1);
        assert!(check_names(crate::model::ConnKind::S3, &["password"]).is_err());
        assert!(check_names(crate::model::ConnKind::Kafka, &["password"]).is_ok());
    }
}
