//! API keys (chapter 14, "HTTP on 8080 and 8443"; build step 11).
//!
//! A key is `sprig_<tenant>_<key_id>_<secret>`: the tenant name (a visible
//! prefix for secret scanners), a 12-character base32 id, and 256 bits of
//! secret as 64 hex digits. The catalog keeps only `SHA256(secret)`
//! (`api_keys.secret_sha256`); the secret is shown once, when the key is
//! created ([`create_api_key`]).
//!
//! `api_keys` is the auth module's format 2 (an expand migration). Rows are
//! versioned by the auth version (`changed_version`), so creating or
//! revoking a key is an auth change and every gateway's cache follows it;
//! a catalog not yet migrated to format 2 simply has no keys.
//!
//! Verification ([`crate::http`]): the key's row in the cached
//! [`TenantAuth`](crate::cache::TenantAuth), `SHA256(secret)` compared in
//! constant time, not revoked, not expired, the client inside
//! `allowed_cidrs` when set, and the user live with the `api_key` method,
//! then the user's statuses as for a token (temporary lock, `ACCOUNT
//! LOCK`, TLS requirement, suspension). `scopes` limit what the key may do
//! on top of the user's grants; chapter 29 applies them.

use std::fmt::Write as _;
use std::net::IpAddr;
use std::time::SystemTime;

use mysql_async::prelude::Queryable;
use mysql_async::{Row, Value};
use sha2::{Digest, Sha256};
use sprig_catalog::{ChangeOpts, MariaCatalog, VersionKind};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};
use subtle::ConstantTimeEq as _;
use zeroize::{Zeroize as _, Zeroizing};

use crate::account::HostPattern;
use crate::store::{
    AT, AuthMethods, AuthStore, col, exec, exec_count, map_err, micros, opt_at, opt_micros,
    opt_time, time,
};

/// `api_keys` (chapter 14, Appendix C): the auth module's format 2.
pub const DDL_API_KEYS: &str = "CREATE TABLE IF NOT EXISTS api_keys (
  key_id          CHAR(12)        NOT NULL,
  user_id         INT UNSIGNED    NOT NULL,
  name            VARCHAR(128)    NOT NULL,
  secret_sha256   BINARY(32)      NOT NULL,
  scopes          JSON            NOT NULL,
  allowed_cidrs   JSON            NULL,
  expires_at      DATETIME(6)     NULL,
  revoked_at      DATETIME(6)     NULL,
  created_by      INT UNSIGNED    NULL,
  created_at      DATETIME(6)     NOT NULL,
  last_used_at    DATETIME(6)     NULL,
  changed_version BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (key_id),
  KEY k_user (user_id)
) ENGINE=InnoDB";

/// The visible id's length.
pub const KEY_ID_LEN: usize = 12;
/// The secret's length in bytes (256 bits).
pub const SECRET_LEN: usize = 32;
const BASE32: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

/// An `api_keys` row.
#[derive(Clone, PartialEq, Eq)]
pub struct ApiKeyRow {
    /// The visible id.
    pub key_id: String,
    /// The user the key logs in as.
    pub user_id: UserId,
    /// A name for people.
    pub name: String,
    /// `SHA256(secret)`.
    pub secret_sha256: [u8; 32],
    /// Limits on top of the user's grants (chapter 29), as stored.
    pub scopes: serde_json::Value,
    /// Client addresses allowed, as host patterns (`10.0.0.0/8`); `None`
    /// for any.
    pub allowed_cidrs: Option<Vec<String>>,
    /// When it stops working.
    pub expires_at: Option<SystemTime>,
    /// When it was revoked.
    pub revoked_at: Option<SystemTime>,
    /// Who created it.
    pub created_by: Option<UserId>,
    /// When.
    pub created_at: SystemTime,
    /// The auth version of its last change.
    pub changed_version: u64,
}

impl std::fmt::Debug for ApiKeyRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiKeyRow")
            .field("key_id", &self.key_id)
            .field("user_id", &self.user_id)
            .field("name", &self.name)
            .field("revoked_at", &self.revoked_at)
            .finish_non_exhaustive()
    }
}

/// A key as the cache keeps it, with its parsed address patterns.
#[derive(Debug, Clone, PartialEq)]
pub struct ApiKey {
    /// The row.
    pub row: ApiKeyRow,
    /// `allowed_cidrs`, parsed.
    pub allowed: Option<Vec<HostPattern>>,
}

impl ApiKey {
    /// The cached form of `row`.
    #[must_use]
    pub fn new(row: ApiKeyRow) -> Self {
        let allowed = row
            .allowed_cidrs
            .as_ref()
            .map(|v| v.iter().map(|c| HostPattern::parse(c)).collect());
        Self { row, allowed }
    }

    /// Whether `secret` is this key's, compared in constant time.
    #[must_use]
    pub fn secret_matches(&self, secret: &[u8]) -> bool {
        let h: [u8; 32] = Sha256::digest(secret).into();
        bool::from(h.ct_eq(&self.row.secret_sha256))
    }

    /// Why the key can't be used now from `ip`, if it can't.
    #[must_use]
    pub fn unusable(&self, ip: IpAddr, now: SystemTime) -> Option<&'static str> {
        if self.row.revoked_at.is_some() {
            return Some("key_revoked");
        }
        if self.row.expires_at.is_some_and(|e| e <= now) {
            return Some("key_expired");
        }
        if self
            .allowed
            .as_ref()
            .is_some_and(|a| !a.iter().any(|p| p.matches(ip)))
        {
            return Some("key_address");
        }
        None
    }
}

/// A presented key, split.
#[derive(PartialEq, Eq)]
pub struct PresentedKey {
    /// The tenant name.
    pub tenant: String,
    /// The visible id.
    pub key_id: String,
    /// The secret's text (64 hex digits).
    pub secret: Zeroizing<String>,
}

impl std::fmt::Debug for PresentedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PresentedKey")
            .field("tenant", &self.tenant)
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

/// Splits `sprig_<tenant>_<key_id>_<secret>`; `None` if it isn't one.
#[must_use]
pub fn parse_key(s: &str) -> Option<PresentedKey> {
    let rest = s.strip_prefix("sprig_")?;
    let (tenant, rest) = rest.split_once('_')?;
    let (key_id, secret) = rest.split_once('_')?;
    let tenant_ok = !tenant.is_empty()
        && tenant.len() <= 63
        && tenant.as_bytes()[0].is_ascii_lowercase()
        && tenant
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    let id_ok = key_id.len() == KEY_ID_LEN && key_id.bytes().all(|b| BASE32.contains(&b));
    let secret_ok = secret.len() == SECRET_LEN * 2 && secret.bytes().all(|b| b.is_ascii_hexdigit());
    (tenant_ok && id_ok && secret_ok).then(|| PresentedKey {
        tenant: tenant.to_owned(),
        key_id: key_id.to_owned(),
        secret: Zeroizing::new(secret.to_owned()),
    })
}

/// A new key's parts: the visible id, the secret's text and the full key.
fn generate(tenant: &str) -> Result<(String, Zeroizing<String>, Zeroizing<String>)> {
    let mut raw = [0_u8; KEY_ID_LEN + SECRET_LEN];
    getrandom::fill(&mut raw)
        .map_err(|e| SprigError::internal(format!("no randomness for an API key: {e}")))?;
    let key_id: String = raw[..KEY_ID_LEN]
        .iter()
        .map(|b| BASE32[usize::from(b & 31)] as char)
        .collect();
    let mut secret = Zeroizing::new(String::with_capacity(SECRET_LEN * 2));
    for b in &raw[KEY_ID_LEN..] {
        let _ = write!(secret, "{b:02x}");
    }
    raw.zeroize();
    let full = Zeroizing::new(format!("sprig_{tenant}_{key_id}_{}", secret.as_str()));
    Ok((key_id, secret, full))
}

// ---- store ----

const COLS: &str = "key_id, user_id, name, secret_sha256, scopes, allowed_cidrs, \
    TIMESTAMPDIFF(MICROSECOND, '1970-01-01', expires_at), \
    TIMESTAMPDIFF(MICROSECOND, '1970-01-01', revoked_at), created_by, \
    TIMESTAMPDIFF(MICROSECOND, '1970-01-01', created_at), changed_version";

fn table(store: &AuthStore) -> String {
    format!("{}.api_keys", sprig_common::names::quote(store.schema()))
}

fn json_col(r: &mut Row, i: usize, name: &str) -> Result<Option<serde_json::Value>> {
    let raw: Option<Vec<u8>> = col(r, i, name)?;
    raw.map(|b| {
        serde_json::from_slice(&b)
            .map_err(|e| SprigError::internal(format!("auth catalog column {name}: {e}")))
    })
    .transpose()
}

fn key_row(mut r: Row) -> Result<ApiKeyRow> {
    let secret: Vec<u8> = col(&mut r, 3, "secret_sha256")?;
    let allowed = json_col(&mut r, 5, "allowed_cidrs")?
        .map(serde_json::from_value::<Vec<String>>)
        .transpose()
        .map_err(|e| SprigError::internal(format!("auth catalog column allowed_cidrs: {e}")))?;
    Ok(ApiKeyRow {
        key_id: col(&mut r, 0, "key_id")?,
        user_id: UserId(col(&mut r, 1, "user_id")?),
        name: col(&mut r, 2, "name")?,
        secret_sha256: secret
            .try_into()
            .map_err(|_| SprigError::internal("auth catalog column secret_sha256: not 32 bytes"))?,
        scopes: json_col(&mut r, 4, "scopes")?.unwrap_or(serde_json::Value::Null),
        allowed_cidrs: allowed,
        expires_at: opt_time(&mut r, 6, "expires_at")?,
        revoked_at: opt_time(&mut r, 7, "revoked_at")?,
        created_by: col::<Option<u32>>(&mut r, 8, "created_by")?.map(UserId),
        created_at: time(&mut r, 9, "created_at")?,
        changed_version: col(&mut r, 10, "changed_version")?,
    })
}

/// Keys changed after `since` (every key with `None`). A catalog whose auth
/// module is below format 2 has no `api_keys` table: no keys.
///
/// # Errors
/// 39070 or a backend error; `Internal` for a malformed row.
pub async fn keys_since<Q: Queryable + Send>(
    store: &AuthStore,
    q: &mut Q,
    since: Option<u64>,
) -> Result<Vec<ApiKeyRow>> {
    let sql = match since {
        Some(_) => format!(
            "SELECT {COLS} FROM {} WHERE changed_version > ? ORDER BY key_id",
            table(store)
        ),
        None => format!("SELECT {COLS} FROM {} ORDER BY key_id", table(store)),
    };
    let rows: std::result::Result<Vec<Row>, _> = match since {
        Some(v) => q.exec(sql, (v,)).await,
        None => q.query(sql).await,
    };
    match rows {
        Ok(rows) => rows.into_iter().map(key_row).collect(),
        Err(mysql_async::Error::Server(s)) if s.code == 1146 => Ok(Vec::new()),
        Err(e) => Err(map_err(e)),
    }
}

async fn insert<Q: Queryable + Send>(store: &AuthStore, q: &mut Q, r: &ApiKeyRow) -> Result<()> {
    let sql = format!(
        "INSERT INTO {} (key_id, user_id, name, secret_sha256, scopes, allowed_cidrs, \
         expires_at, revoked_at, created_by, created_at, changed_version) \
         VALUES (?, ?, ?, ?, ?, ?, {}, NULL, ?, {AT}, ?)",
        table(store),
        opt_at(r.expires_at.is_some()),
    );
    let json = |v: &serde_json::Value| Value::from(v.to_string());
    let p = vec![
        Value::from(r.key_id.as_str()),
        Value::from(r.user_id.0),
        Value::from(r.name.as_str()),
        Value::from(r.secret_sha256.to_vec()),
        json(&r.scopes),
        r.allowed_cidrs.as_ref().map_or(Value::NULL, |c| {
            Value::from(serde_json::json!(c).to_string())
        }),
        opt_micros(r.expires_at),
        r.created_by.map_or(Value::NULL, |u| Value::from(u.0)),
        Value::from(micros(r.created_at)),
        Value::from(r.changed_version),
    ];
    exec(q, sql, p).await
}

// ---- admin ----

/// A key to create.
#[derive(Debug, Clone, Default)]
pub struct NewApiKey {
    /// A name for people.
    pub name: String,
    /// Scopes on top of the user's grants (chapter 29); `[]` for none.
    pub scopes: Vec<String>,
    /// Client addresses allowed (host patterns, `10.0.0.0/8`); empty for
    /// any.
    pub allowed_cidrs: Vec<String>,
    /// When it stops working.
    pub expires_at: Option<SystemTime>,
    /// Who creates it.
    pub created_by: Option<UserId>,
}

/// A created key: the only time its secret is visible.
pub struct CreatedKey {
    /// The visible id.
    pub key_id: String,
    /// The whole key to hand to the client, `sprig_<tenant>_<id>_<secret>`.
    pub key: Zeroizing<String>,
    /// The auth version it committed at.
    pub auth_version: u64,
}

impl std::fmt::Debug for CreatedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreatedKey")
            .field("key_id", &self.key_id)
            .field("auth_version", &self.auth_version)
            .finish_non_exhaustive()
    }
}

/// Creates an API key for live user `user` of `tenant` (named
/// `tenant_name`, the key's prefix). The user needs the `api_key` method
/// (`users.auth_methods`); this call adds it.
///
/// # Errors
/// 1396 if the user doesn't exist; 39998 for a bad name; the change's
/// errors; a backend 1146 if the catalog isn't migrated to the auth
/// module's format 2.
pub async fn create_api_key(
    cat: &MariaCatalog,
    tenant: TenantId,
    tenant_name: &str,
    user: UserId,
    new: &NewApiKey,
    now: SystemTime,
) -> Result<CreatedKey> {
    if new.name.is_empty() || new.name.chars().count() > 128 {
        return Err(SprigError::new(
            SprigCode::ConfigInvalid,
            "an API key's name must be 1 to 128 characters",
        ));
    }
    let (key_id, secret, full) = generate(tenant_name)?;
    let row = ApiKeyRow {
        key_id: key_id.clone(),
        user_id: user,
        name: new.name.clone(),
        secret_sha256: Sha256::digest(secret.as_bytes()).into(),
        scopes: serde_json::json!(new.scopes),
        allowed_cidrs: (!new.allowed_cidrs.is_empty()).then(|| new.allowed_cidrs.clone()),
        expires_at: new.expires_at,
        revoked_at: None,
        created_by: new.created_by,
        created_at: now,
        changed_version: 0,
    };
    let version = change(cat, tenant, user, move |store, q, version, urow| {
        let mut row = row.clone();
        row.changed_version = version;
        urow.auth_methods = urow.auth_methods.union(AuthMethods::API_KEY);
        Box::pin(async move { insert(store, q, &row).await })
    })
    .await?;
    Ok(CreatedKey {
        key_id,
        key: full,
        auth_version: version,
    })
}

/// Revokes key `key_id` of `tenant`. Returns `false` if there is no such
/// live key.
///
/// # Errors
/// The change's errors.
pub async fn revoke_api_key(
    cat: &MariaCatalog,
    tenant: TenantId,
    key_id: &str,
    now: SystemTime,
) -> Result<bool> {
    let mut c = cat
        .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
        .await?;
    let version = c.version();
    let r = {
        let (core, q) = c.tx().parts();
        let store = AuthStore::of(core);
        exec_count(
            q,
            format!(
                "UPDATE {} SET revoked_at = {AT}, changed_version = ? \
                 WHERE key_id = ? AND revoked_at IS NULL",
                table(&store)
            ),
            vec![
                Value::from(micros(now)),
                Value::from(version),
                Value::from(key_id),
            ],
        )
        .await
    };
    match r {
        Ok(n) if n > 0 => {
            c.commit().await?;
            Ok(true)
        }
        Ok(_) => {
            let _ = c.rollback().await;
            let _ = cat.settle(tenant, VersionKind::Auth).await;
            Ok(false)
        }
        Err(e) => {
            let _ = c.rollback().await;
            let _ = cat.settle(tenant, VersionKind::Auth).await;
            Err(e)
        }
    }
}

type Body<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>>;

/// One auth change on live user `user`, rewriting its row.
async fn change<F>(cat: &MariaCatalog, tenant: TenantId, user: UserId, body: F) -> Result<u64>
where
    F: for<'a> FnOnce(
            &'a AuthStore,
            &'a mut mysql_async::Transaction<'static>,
            u64,
            &'a mut crate::store::UserRow,
        ) -> Body<'a>
        + Send,
{
    let mut c = cat
        .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
        .await?;
    let version = c.version();
    let r = async {
        let (core, q) = c.tx().parts();
        let store = AuthStore::of(core);
        let mut row = match store.user(q, user).await? {
            Some(r) if r.is_live() => r,
            _ => {
                return Err(SprigError::backend(
                    1396,
                    *b"HY000",
                    format!("Operation CREATE API KEY failed for '#{user}'@'%'"),
                ));
            }
        };
        body(&store, q, version, &mut row).await?;
        row.changed_version = version;
        store.update_user(q, &row).await?;
        Ok(())
    }
    .await;
    match r {
        Ok(()) => c.commit().await,
        Err(e) => {
            let _ = c.rollback().await;
            let _ = cat.settle(tenant, VersionKind::Auth).await;
            if matches!(e.kind, ErrorKind::Backend { errno: 1146, .. }) {
                return Err(SprigError::new(
                    SprigCode::TenantNotReady,
                    "the tenant's catalog has no api_keys table yet: migrate its auth module",
                ));
            }
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn generated_keys_parse_back() {
        let (id, secret, full) = generate("acme-eu").unwrap();
        let p = parse_key(&full).unwrap();
        assert_eq!(p.tenant, "acme-eu");
        assert_eq!(p.key_id, id);
        assert_eq!(*p.secret, *secret);
        assert_eq!(full.len(), "sprig_acme-eu_".len() + 12 + 1 + 64);
        let (id2, _, _) = generate("acme-eu").unwrap();
        assert_ne!(id, id2);
    }

    #[test]
    fn malformed_keys_are_refused() {
        let good = format!("sprig_acme_{}_{}", "a".repeat(12), "0".repeat(64));
        assert!(parse_key(&good).is_some());
        for bad in [
            format!("sprig_acme_{}_{}", "a".repeat(11), "0".repeat(64)),
            format!("sprig_acme_{}_{}", "a".repeat(12), "0".repeat(63)),
            format!("sprig_Acme_{}_{}", "a".repeat(12), "0".repeat(64)),
            format!("sprig_acme_{}_{}", "A".repeat(12), "0".repeat(64)),
            format!("sprig_acme_{}_{}", "a".repeat(12), "g".repeat(64)),
            format!("token_acme_{}_{}", "a".repeat(12), "0".repeat(64)),
            "sprig_acme".to_owned(),
        ] {
            assert!(parse_key(&bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn usability_checks() {
        let now = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000);
        let secret = "0".repeat(64);
        let row = ApiKeyRow {
            key_id: "a".repeat(12),
            user_id: UserId(1),
            name: "ci".into(),
            secret_sha256: Sha256::digest(secret.as_bytes()).into(),
            scopes: serde_json::json!([]),
            allowed_cidrs: Some(vec!["10.0.0.0/8".into()]),
            expires_at: Some(now + std::time::Duration::from_secs(60)),
            revoked_at: None,
            created_by: None,
            created_at: now,
            changed_version: 1,
        };
        let k = ApiKey::new(row.clone());
        assert!(k.secret_matches(secret.as_bytes()));
        assert!(!k.secret_matches(b"nope"));
        let inside: IpAddr = "10.1.2.3".parse().unwrap();
        let outside: IpAddr = "192.0.2.1".parse().unwrap();
        assert_eq!(k.unusable(inside, now), None);
        assert_eq!(k.unusable(outside, now), Some("key_address"));
        assert_eq!(
            k.unusable(inside, now + std::time::Duration::from_secs(60)),
            Some("key_expired")
        );
        let mut r = row;
        r.revoked_at = Some(now);
        assert_eq!(ApiKey::new(r).unusable(inside, now), Some("key_revoked"));
    }
}
