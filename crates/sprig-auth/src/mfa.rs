//! Second factors (chapter 14 step 13, "Second factors"; R2): TOTP with
//! recovery codes, kept in the tenant catalog's `user_factors` (the auth
//! module's format 5), and the account lookups chapter 14's challenge and
//! impersonation (step 14) need.
//!
//! ```text
//! enrol(user):    a 20-byte seed, sealed (AES-256-GCM under the tenant's
//!                 factor key), stored `pending`; shown once as otpauth://
//! confirm(code):  the code is right → the factor is `active` and 10
//!                 recovery codes (SHA-256 only) replace any earlier ones,
//!                 in one auth change
//! challenge:      TOTP code: RFC 6238, HMAC-SHA-1, 6 digits, 30 s, one
//!                 step either way, the step must be newer than the last
//!                 accepted one (compare-and-set: a code works once);
//!                 recovery code: its row is deleted (works once)
//! reset(user):    every factor deleted and every session revoked, in one
//!                 auth change (an administrator's reset, or the user's own
//!                 "remove")
//! ```
//!
//! Seeds are sealed with a per-tenant key the caller supplies
//! ([`FactorKeys`]): the gateway derives it from its tenant-data-key file
//! (chapter 14, "Tenant data keys"; open question 8), so no seed is ever
//! stored in the clear and a catalog copy alone can't produce codes.

use std::net::IpAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mysql_async::prelude::Queryable as _;
use mysql_async::{Row, Value};
use ring::aead;
use sha2::{Digest as _, Sha256};
use sprig_catalog::migrate::MigrateOpts;
use sprig_catalog::{ChangeOpts, MariaCatalog, Migrator, VersionKind};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::Persona;
use subtle::ConstantTimeEq as _;

use crate::cache::TenantAuth;
use crate::mysql::{CatalogLogins, Principal, locked, temp_locked};
use crate::store::{AT, AuthStore, RequireTls, exec_count, map_err, micros};

/// `user_factors` (chapter 14, Appendix C): the auth module's format 5.
pub const DDL_USER_FACTORS: &str = "CREATE TABLE IF NOT EXISTS user_factors (
  factor_id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id         INT UNSIGNED    NOT NULL,
  kind            ENUM('totp','webauthn','recovery') NOT NULL,
  state           ENUM('pending','active') NOT NULL,
  secret_enc      VARBINARY(128)  NULL,
  last_step       BIGINT UNSIGNED NULL,
  credential_id   VARBINARY(1024) NULL,
  public_key      VARBINARY(1024) NULL,
  sign_count      BIGINT UNSIGNED NULL,
  code_sha256     BINARY(32)      NULL,
  created_at      DATETIME(6)     NOT NULL,
  changed_version BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (factor_id),
  KEY k_user (user_id, kind)
) ENGINE=InnoDB";

/// TOTP step.
pub const STEP: u64 = 30;
/// Seed bytes.
pub const SEED_BYTES: usize = 20;
/// Recovery codes per enrolment.
pub const RECOVERY_CODES: usize = 10;
/// The purpose the factor key is derived for.
pub const KEY_PURPOSE: &[u8] = b"sprig-auth-totp-seed";

// ---- TOTP (RFC 6238 over RFC 4226) ----

/// The 6-digit code of `seed` at `step`.
#[must_use]
pub fn code_at(seed: &[u8], step: u64) -> u32 {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, seed);
    let tag = ring::hmac::sign(&key, &step.to_be_bytes());
    let h = tag.as_ref();
    let off = usize::from(h[h.len() - 1] & 0x0f);
    let bin = (u32::from(h[off]) & 0x7f) << 24
        | u32::from(h[off + 1]) << 16
        | u32::from(h[off + 2]) << 8
        | u32::from(h[off + 3]);
    bin % 1_000_000
}

/// The step of `t`.
#[must_use]
pub fn step_of(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs()) / STEP
}

/// The step `code` matches at `now` (one step of drift either way) and
/// that is newer than `last`; `None` otherwise.
#[must_use]
pub fn check(seed: &[u8], code: &str, now: SystemTime, last: Option<u64>) -> Option<u64> {
    let code = code.trim().replace(' ', "");
    if code.len() != 6 || !code.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let want: u32 = code.parse().ok()?;
    let s = step_of(now);
    [s.saturating_sub(1), s, s + 1]
        .into_iter()
        .find(|&st| last.is_none_or(|l| st > l) && bool::from(code_at(seed, st).ct_eq(&want)))
}

/// RFC 4648 base32 without padding (authenticator apps' secret form).
#[must_use]
pub fn base32(b: &[u8]) -> String {
    const A: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut out = String::new();
    let (mut buf, mut bits) = (0u32, 0u32);
    for &x in b {
        buf = (buf << 8) | u32::from(x);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(char::from(A[((buf >> bits) & 31) as usize]));
        }
    }
    if bits > 0 {
        out.push(char::from(A[((buf << (5 - bits)) & 31) as usize]));
    }
    out
}

fn uri_escape(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
                char::from(b).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

/// `otpauth://totp/<issuer>:<account>?secret=...&issuer=...`.
#[must_use]
pub fn otpauth_uri(issuer: &str, account: &str, seed: &[u8]) -> String {
    format!(
        "otpauth://totp/{}:{}?secret={}&issuer={}&algorithm=SHA1&digits=6&period={STEP}",
        uri_escape(issuer),
        uri_escape(account),
        base32(seed),
        uri_escape(issuer)
    )
}

fn random<const N: usize>() -> Result<[u8; N]> {
    let mut b = [0u8; N];
    getrandom::fill(&mut b)
        .map_err(|e| SprigError::internal(format!("no randomness for a factor: {e}")))?;
    Ok(b)
}

/// Ten recovery codes, `xxxxx-xxxxx` in base32 (50 bits each).
///
/// # Errors
/// `Internal` without randomness.
pub fn recovery_codes() -> Result<Vec<String>> {
    (0..RECOVERY_CODES)
        .map(|_| {
            let b = base32(&random::<7>()?).to_lowercase();
            Ok(format!("{}-{}", &b[..5], &b[5..10]))
        })
        .collect()
}

/// The stored form of a recovery code: SHA-256 of its normalized text.
#[must_use]
pub fn recovery_hash(code: &str) -> [u8; 32] {
    let n: String = code
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|c| c.to_ascii_lowercase())
        .collect();
    Sha256::digest(n.as_bytes()).into()
}

// ---- sealing ----

/// The per-tenant keys that seal seeds.
pub trait FactorKeys: Send + Sync {
    /// The current key version and key for `tenant`.
    fn current(&self, tenant: TenantId) -> Option<(u32, [u8; 32])>;
    /// The key `version` for `tenant`.
    fn key(&self, tenant: TenantId, version: u32) -> Option<[u8; 32]>;
}

fn aad(tenant: TenantId, user: UserId) -> Vec<u8> {
    let mut a = b"sprig-totp:".to_vec();
    a.extend_from_slice(&tenant.0.to_be_bytes());
    a.extend_from_slice(&user.0.to_be_bytes());
    a
}

fn no_key() -> SprigError {
    SprigError::new(
        SprigCode::AuthUnavailable,
        "second factors need a factor key: set [gateway] workload_key_file (tenant data keys)",
    )
}

/// `version (4) | nonce (12) | ciphertext and tag`.
///
/// # Errors
/// 39301 without a key; `Internal`.
pub fn seal(keys: &dyn FactorKeys, tenant: TenantId, user: UserId, seed: &[u8]) -> Result<Vec<u8>> {
    let (version, key) = keys.current(tenant).ok_or_else(no_key)?;
    let k = aead::LessSafeKey::new(
        aead::UnboundKey::new(&aead::AES_256_GCM, &key)
            .map_err(|_| SprigError::internal("factor key"))?,
    );
    let nonce = random::<12>()?;
    let mut buf = seed.to_vec();
    k.seal_in_place_append_tag(
        aead::Nonce::assume_unique_for_key(nonce),
        aead::Aad::from(aad(tenant, user)),
        &mut buf,
    )
    .map_err(|_| SprigError::internal("sealing a seed"))?;
    let mut out = version.to_be_bytes().to_vec();
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&buf);
    Ok(out)
}

/// The seed in `sealed`.
///
/// # Errors
/// 39301 without the key; `Internal` for a damaged or foreign seal.
pub fn open(
    keys: &dyn FactorKeys,
    tenant: TenantId,
    user: UserId,
    sealed: &[u8],
) -> Result<Vec<u8>> {
    if sealed.len() < 4 + 12 + 16 {
        return Err(SprigError::internal("a sealed seed is too short"));
    }
    let version = u32::from_be_bytes([sealed[0], sealed[1], sealed[2], sealed[3]]);
    let key = keys.key(tenant, version).ok_or_else(no_key)?;
    let k = aead::LessSafeKey::new(
        aead::UnboundKey::new(&aead::AES_256_GCM, &key)
            .map_err(|_| SprigError::internal("factor key"))?,
    );
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&sealed[4..16]);
    let mut buf = sealed[16..].to_vec();
    let plain = k
        .open_in_place(
            aead::Nonce::assume_unique_for_key(nonce),
            aead::Aad::from(aad(tenant, user)),
            &mut buf,
        )
        .map_err(|_| SprigError::internal("a sealed seed doesn't open with its key"))?;
    Ok(plain.to_vec())
}

// ---- storage ----

/// A factor row (no secret).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Factor {
    /// The id.
    pub id: u64,
    /// `totp`, `webauthn`, `recovery`.
    pub kind: String,
    /// `pending` or `active`.
    pub state: String,
    /// When it was made, Unix milliseconds.
    pub created_ms: u64,
}

/// `user_factors` in the tenant catalogs.
#[derive(Clone)]
pub struct FactorStore {
    cat: MariaCatalog,
}

impl std::fmt::Debug for FactorStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FactorStore").finish_non_exhaustive()
    }
}

fn tbl(store: &AuthStore, t: &str) -> String {
    format!("{}.{t}", sprig_common::names::quote(store.schema()))
}

impl FactorStore {
    /// Factors written through `cat`.
    #[must_use]
    pub fn new(cat: MariaCatalog) -> Self {
        Self { cat }
    }

    async fn conn(&self, tenant: TenantId) -> Result<(mysql_async::Conn, String)> {
        let ep = self.cat.backend().locator().writer(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
        // Format 5 (`user_factors`) on first use, as format 4 is.
        Migrator::new(&[crate::store::AUTH_MODULE])?
            .run(&mut conn, ep.store.schema(), &MigrateOpts::default())
            .await?;
        Ok((conn, ep.store.schema().to_owned()))
    }

    /// The user's factors, newest first.
    ///
    /// # Errors
    /// The catalog's.
    pub async fn list(&self, tenant: TenantId, user: UserId) -> Result<Vec<Factor>> {
        let (mut c, schema) = self.conn(tenant).await?;
        let rows: Vec<Row> = c
            .exec(
                format!(
                    "SELECT factor_id, kind, state, \
                     TIMESTAMPDIFF(MICROSECOND, '1970-01-01', created_at) \
                     FROM {}.user_factors WHERE user_id = ? ORDER BY factor_id DESC",
                    sprig_common::names::quote(&schema)
                ),
                (user.0,),
            )
            .await
            .map_err(map_err)?;
        Ok(rows
            .into_iter()
            .map(|mut r| Factor {
                id: r.take::<u64, _>(0).unwrap_or(0),
                kind: r.take::<String, _>(1).unwrap_or_default(),
                state: r.take::<String, _>(2).unwrap_or_default(),
                created_ms: r.take::<u64, _>(3).unwrap_or(0) / 1000,
            })
            .collect())
    }

    /// Whether the user has an active TOTP factor.
    ///
    /// # Errors
    /// The catalog's.
    pub async fn has_active(&self, tenant: TenantId, user: UserId) -> Result<bool> {
        Ok(self
            .list(tenant, user)
            .await?
            .iter()
            .any(|f| f.kind == "totp" && f.state == "active"))
    }

    /// The sealed seed and last step of the user's active (or, with
    /// `pending`, pending) TOTP factor.
    async fn totp(
        &self,
        tenant: TenantId,
        user: UserId,
        pending: bool,
    ) -> Result<Option<(u64, Vec<u8>, Option<u64>)>> {
        let (mut c, schema) = self.conn(tenant).await?;
        let r: Option<(u64, Vec<u8>, Option<u64>)> = c
            .exec_first(
                format!(
                    "SELECT factor_id, secret_enc, last_step FROM {}.user_factors \
                     WHERE user_id = ? AND kind = 'totp' AND state = ? \
                     ORDER BY factor_id DESC LIMIT 1",
                    sprig_common::names::quote(&schema)
                ),
                (user.0, if pending { "pending" } else { "active" }),
            )
            .await
            .map_err(map_err)?;
        Ok(r)
    }

    /// Starts an enrolment: a new seed, stored sealed and `pending` (an
    /// earlier pending one is replaced). Returns the seed, to show once.
    ///
    /// # Errors
    /// 39301 without a factor key; the catalog's.
    pub async fn enrol(
        &self,
        keys: &dyn FactorKeys,
        tenant: TenantId,
        user: UserId,
        now: SystemTime,
    ) -> Result<Vec<u8>> {
        let seed = random::<SEED_BYTES>()?.to_vec();
        let sealed = seal(keys, tenant, user, &seed)?;
        let (mut c, schema) = self.conn(tenant).await?;
        let t = format!("{}.user_factors", sprig_common::names::quote(&schema));
        c.exec_drop(
            format!("DELETE FROM {t} WHERE user_id = ? AND kind = 'totp' AND state = 'pending'"),
            (user.0,),
        )
        .await
        .map_err(map_err)?;
        c.exec_drop(
            format!(
                "INSERT INTO {t} (user_id, kind, state, secret_enc, created_at, changed_version) \
                 VALUES (?, 'totp', 'pending', ?, {AT}, 0)"
            ),
            (user.0, sealed, micros(now)),
        )
        .await
        .map_err(map_err)?;
        Ok(seed)
    }

    /// Completes an enrolment with a code from the pending seed: the
    /// factor becomes active (replacing an earlier active one) and new
    /// recovery codes replace the old, in one auth change. Returns the
    /// recovery codes, to show once; `None` for a wrong code.
    ///
    /// # Errors
    /// 39301; the catalog's.
    pub async fn confirm(
        &self,
        keys: &dyn FactorKeys,
        tenant: TenantId,
        user: UserId,
        submitted: &str,
        now: SystemTime,
    ) -> Result<Option<Vec<String>>> {
        let Some((id, sealed, _)) = self.totp(tenant, user, true).await? else {
            return Err(SprigError::new(
                SprigCode::AdminRequestInvalid,
                "no enrolment is pending: start one first",
            ));
        };
        let seed = open(keys, tenant, user, &sealed)?;
        let Some(step) = check(&seed, submitted, now, None) else {
            return Ok(None);
        };
        let codes = recovery_codes()?;
        let mut ch = self
            .cat
            .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
            .await?;
        let version = ch.version();
        let r = async {
            let (core, q) = ch.tx().parts();
            let store = AuthStore::of(core);
            let factors = tbl(&store, "user_factors");
            exec_count(
                q,
                format!(
                    "DELETE FROM {factors} WHERE user_id = ? AND (kind = 'recovery' \
                     OR (kind = 'totp' AND factor_id <> ?))"
                ),
                vec![Value::from(user.0), Value::from(id)],
            )
            .await?;
            exec_count(
                q,
                format!(
                    "UPDATE {factors} SET state = 'active', last_step = ?, changed_version = ? \
                     WHERE factor_id = ?"
                ),
                vec![Value::from(step), Value::from(version), Value::from(id)],
            )
            .await?;
            for code in &codes {
                exec_count(
                    q,
                    format!(
                        "INSERT INTO {factors} (user_id, kind, state, code_sha256, created_at, \
                         changed_version) VALUES (?, 'recovery', 'active', ?, {AT}, ?)"
                    ),
                    vec![
                        Value::from(user.0),
                        Value::from(recovery_hash(code).to_vec()),
                        Value::from(micros(now)),
                        Value::from(version),
                    ],
                )
                .await?;
            }
            Ok::<_, SprigError>(())
        }
        .await;
        match r {
            Ok(()) => {
                ch.commit().await?;
                Ok(Some(codes))
            }
            Err(e) => {
                let _ = ch.rollback().await;
                let _ = self.cat.settle(tenant, VersionKind::Auth).await;
                Err(e)
            }
        }
    }

    /// A second factor at login: a TOTP code (accepted once: the step is
    /// stored with a compare-and-set) or a recovery code (its row is
    /// deleted). `Some("otp" | "rec")` when it passes.
    ///
    /// # Errors
    /// 39301; the catalog's.
    pub async fn verify(
        &self,
        keys: &dyn FactorKeys,
        tenant: TenantId,
        user: UserId,
        code: &str,
        now: SystemTime,
    ) -> Result<Option<&'static str>> {
        let digits = code.trim().replace(' ', "");
        if digits.len() == 6 && digits.bytes().all(|b| b.is_ascii_digit()) {
            let Some((id, sealed, last)) = self.totp(tenant, user, false).await? else {
                return Ok(None);
            };
            let seed = open(keys, tenant, user, &sealed)?;
            let Some(step) = check(&seed, &digits, now, last) else {
                return Ok(None);
            };
            let (mut c, schema) = self.conn(tenant).await?;
            let n = exec_count(
                &mut c,
                format!(
                    "UPDATE {}.user_factors SET last_step = ? WHERE factor_id = ? \
                     AND (last_step IS NULL OR last_step < ?)",
                    sprig_common::names::quote(&schema)
                ),
                vec![Value::from(step), Value::from(id), Value::from(step)],
            )
            .await?;
            return Ok((n == 1).then_some("otp"));
        }
        let (mut c, schema) = self.conn(tenant).await?;
        let n = exec_count(
            &mut c,
            format!(
                "DELETE FROM {}.user_factors WHERE user_id = ? AND kind = 'recovery' \
                 AND code_sha256 = ? LIMIT 1",
                sprig_common::names::quote(&schema)
            ),
            vec![
                Value::from(user.0),
                Value::from(recovery_hash(code).to_vec()),
            ],
        )
        .await?;
        Ok((n == 1).then_some("rec"))
    }

    /// Recovery codes left.
    ///
    /// # Errors
    /// The catalog's.
    pub async fn recovery_left(&self, tenant: TenantId, user: UserId) -> Result<usize> {
        Ok(self
            .list(tenant, user)
            .await?
            .iter()
            .filter(|f| f.kind == "recovery")
            .count())
    }

    /// Deletes every factor of the user and revokes the user's sessions,
    /// in one auth change; the number of factors deleted.
    ///
    /// # Errors
    /// The catalog's.
    pub async fn reset(&self, tenant: TenantId, user: UserId, now: SystemTime) -> Result<u64> {
        let _ = self.conn(tenant).await?;
        let mut ch = self
            .cat
            .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
            .await?;
        let version = ch.version();
        let r = async {
            let (core, q) = ch.tx().parts();
            let store = AuthStore::of(core);
            let n = exec_count(
                q,
                format!(
                    "DELETE FROM {} WHERE user_id = ?",
                    tbl(&store, "user_factors")
                ),
                vec![Value::from(user.0)],
            )
            .await?;
            exec_count(
                q,
                format!(
                    "UPDATE {} SET revoked_at = {AT}, changed_version = ? \
                     WHERE user_id = ? AND revoked_at IS NULL",
                    tbl(&store, "token_sessions")
                ),
                vec![
                    Value::from(micros(now)),
                    Value::from(version),
                    Value::from(user.0),
                ],
            )
            .await?;
            Ok::<_, SprigError>(n)
        }
        .await;
        match r {
            Ok(n) => {
                ch.commit().await?;
                Ok(n)
            }
            Err(e) => {
                let _ = ch.rollback().await;
                let _ = self.cat.settle(tenant, VersionKind::Auth).await;
                Err(e)
            }
        }
    }
}

// ---- accounts without a password (a completed challenge, impersonation) ----

fn denied(why: &str) -> SprigError {
    SprigError::standard(1045, *b"28000", format!("Access denied: {why}"))
}

/// The principal of `tenant`'s user `user` (by id, or by name and host),
/// with the checks a login makes after the password: the tenant can log
/// in and isn't suspended, the account exists and isn't locked, TLS.
///
/// # Errors
/// 1045, 3118, 3955, 39303; 39301.
pub async fn account_principal(
    cl: &CatalogLogins,
    tenant: &str,
    user: AccountRef<'_>,
    client_ip: IpAddr,
    tls: bool,
    now: SystemTime,
) -> Result<Principal> {
    let t = match cl.tenants().lookup(tenant).await {
        Ok(Some(t)) if t.can_log_in => t,
        Ok(_) => return Err(denied("no such tenant")),
        Err(e) => {
            tracing::warn!(error = %e, "mfa: tenant directory unreadable");
            return Err(SprigError::new(
                SprigCode::AuthUnavailable,
                "authentication unavailable; retry",
            ));
        }
    };
    let catalog = cl.catalogs().catalog(t.id).await.map_err(|e| {
        tracing::warn!(error = %e, "mfa: catalog unreadable");
        SprigError::new(
            SprigCode::AuthUnavailable,
            "authentication unavailable; retry",
        )
    })?;
    let Some(auth) = TenantAuth::of(&catalog) else {
        return Err(denied("the tenant has no accounts"));
    };
    let u = match user {
        AccountRef::Id(id) => auth.user(id).cloned(),
        AccountRef::Name(name, host) => auth
            .accounts(name)
            .iter()
            .find(|a| host.is_none_or(|h| a.row.host == h))
            .cloned(),
    }
    .ok_or_else(|| denied("no such account"))?;
    let (name, host) = (u.row.name.as_str(), u.row.host.as_str());
    if let Some(lock) = auth
        .temp_lock(u.row.user_id)
        .filter(|l| l.locked_until > now)
    {
        let p = temp_locked(Persona::MySql84, name, host, &lock, now);
        return Err(SprigError::standard(p.code, p.sqlstate, p.message));
    }
    if t.suspended {
        return Err(SprigError::new(
            SprigCode::TenantSuspended,
            format!("tenant {} is suspended", t.name),
        ));
    }
    if u.row.account_locked {
        let p = locked(Persona::MySql84, name, host);
        return Err(SprigError::standard(p.code, p.sqlstate, p.message));
    }
    let need_tls = u.row.require_tls != RequireTls::None || cl.require_tls(&catalog);
    if (need_tls && !tls) || u.row.require_tls == RequireTls::X509 {
        return Err(denied("the account requires TLS"));
    }
    let backend = cl.backend_of(&t, &u)?;
    Ok(Principal {
        tenant: Some(t.id),
        tenant_name: Some(t.name.clone()),
        user_id: Some(u.row.user_id),
        user: u.row.name.clone(),
        host: u.row.host.clone(),
        client_ip,
        must_change_password: false,
        limits: u.row.limits,
        auth_version: catalog.auth_version,
        backend,
    })
}

/// Which account [`account_principal`] looks up.
#[derive(Debug, Clone, Copy)]
pub enum AccountRef<'a> {
    /// By id.
    Id(UserId),
    /// By name, and host when given (else the first account of the name).
    Name(&'a str, Option<&'a str>),
}

/// How long a challenge lives, by default (`auth.mfa_challenge_seconds`).
pub const CHALLENGE_DEFAULT: Duration = Duration::from_secs(300);

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 6238 appendix B, SHA-1 with the 20-byte ASCII seed: the last
    /// six digits of the eight-digit values.
    #[test]
    fn rfc6238_vectors() {
        let seed = b"12345678901234567890";
        for (t, want) in [
            (59u64, 94_287_082u32),
            (1_111_111_109, 7_081_804),
            (1_111_111_111, 14_050_471),
            (1_234_567_890, 89_005_924),
            (2_000_000_000, 69_279_037),
        ] {
            assert_eq!(code_at(seed, t / STEP), want % 1_000_000, "t = {t}");
        }
    }

    #[test]
    fn codes_work_once_and_within_one_step() {
        let seed = b"12345678901234567890";
        let now = UNIX_EPOCH + Duration::from_secs(1_111_111_111);
        let s = step_of(now);
        let code = format!("{:06}", code_at(seed, s));
        assert_eq!(check(seed, &code, now, None), Some(s));
        // Used: the same step again is refused.
        assert_eq!(check(seed, &code, now, Some(s)), None);
        // The previous step's code is still accepted (drift), once.
        let prev = format!("{:06}", code_at(seed, s - 1));
        assert_eq!(check(seed, &prev, now, None), Some(s - 1));
        // Two steps away: refused.
        let old = format!("{:06}", code_at(seed, s - 2));
        assert_eq!(check(seed, &old, now, None), None);
        for bad in ["", "12345", "1234567", "abcdef"] {
            assert_eq!(check(seed, bad, now, None), None);
        }
    }

    #[test]
    fn base32_and_uri() {
        assert_eq!(base32(b"foobar"), "MZXW6YTBOI");
        assert_eq!(
            base32(b"12345678901234567890"),
            "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ"
        );
        let u = otpauth_uri("SprigDB acme", "acme.app", b"12345678901234567890");
        assert!(u.starts_with("otpauth://totp/SprigDB%20acme:acme.app?secret=GEZDGNBV"));
        assert!(u.contains("&digits=6&period=30"));
    }

    #[test]
    fn recovery_codes_normalize() {
        let codes = recovery_codes().unwrap();
        assert_eq!(codes.len(), RECOVERY_CODES);
        assert!(
            codes
                .iter()
                .all(|c| c.len() == 11 && c.as_bytes()[5] == b'-')
        );
        let c = &codes[0];
        assert_eq!(
            recovery_hash(c),
            recovery_hash(&c.to_uppercase().replace('-', " "))
        );
        assert_ne!(recovery_hash(&codes[0]), recovery_hash(&codes[1]));
    }

    struct Fixed;
    impl FactorKeys for Fixed {
        fn current(&self, t: TenantId) -> Option<(u32, [u8; 32])> {
            Some((2, [u8::try_from(t.0 % 200).unwrap(); 32]))
        }
        fn key(&self, t: TenantId, v: u32) -> Option<[u8; 32]> {
            (v == 2).then(|| [u8::try_from(t.0 % 200).unwrap(); 32])
        }
    }

    #[test]
    fn seeds_seal_per_tenant_and_user() {
        let seed = random::<SEED_BYTES>().unwrap();
        let s = seal(&Fixed, TenantId(7), UserId(3), &seed).unwrap();
        assert_eq!(&s[..4], &2u32.to_be_bytes());
        assert_eq!(open(&Fixed, TenantId(7), UserId(3), &s).unwrap(), seed);
        // Another user's or tenant's row can't be swapped in.
        assert!(open(&Fixed, TenantId(7), UserId(4), &s).is_err());
        assert!(open(&Fixed, TenantId(8), UserId(3), &s).is_err());
        let mut bad = s.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert!(open(&Fixed, TenantId(7), UserId(3), &bad).is_err());
    }
}
