//! Accounts as a tenant export carries them (chapter 47, "Logical export";
//! build step 8): each live user with its policy columns and its verifier
//! strings (hashes, never passwords), and the SQL roles, with no ids, so
//! they load into any tenant on any cluster.
//!
//! Privileges and role grants travel as `SHOW GRANTS` text, replayed
//! through the gateway after the databases exist, so they bind to the
//! target's own database and object ids; this module carries only what
//! SQL can't express (verifier hashes) and the role names `CREATE ROLE`
//! needs first.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use sprig_catalog::{ChangeOpts, MariaCatalog, VersionKind};
use sprig_common::ids::TenantId;
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};

use crate::admin::Changed;
use crate::authz::store::AuthzStore;
use crate::store::{
    AccountLimits, AuthStore, LoginForm, RequireCurrent, RequireTls, Slot, UserKind, UserRow,
    UserSource, VerifierKind, VerifierRow,
};

/// One verifier: the stored string of `caching_sha2_password` or
/// `mysql_native_password`, hex-encoded.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortableVerifier {
    /// The mechanism (`caching_sha2_password`, `mysql_native_password`).
    pub mechanism: String,
    /// `primary`, `secondary` or `history`.
    pub slot: String,
    /// The stored string, hex.
    pub auth_string_hex: String,
    /// The password generation it belongs to.
    pub generation: u64,
}

impl std::fmt::Debug for PortableVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PortableVerifier")
            .field("mechanism", &self.mechanism)
            .field("slot", &self.slot)
            .finish_non_exhaustive()
    }
}

/// One live user.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PortableAccount {
    /// The user name.
    pub name: String,
    /// The host pattern.
    pub host: String,
    /// `user` or `service`.
    pub kind: String,
    /// The account's MySQL plugin.
    pub plugin: String,
    /// `none`, `ssl` or `x509`.
    pub require_tls: String,
    /// `ACCOUNT LOCK`.
    pub account_locked: bool,
    /// `PASSWORD EXPIRE`.
    pub password_expired: bool,
    /// `PASSWORD EXPIRE INTERVAL`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_lifetime: Option<i16>,
    /// `PASSWORD HISTORY`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_history: Option<i16>,
    /// `PASSWORD REUSE INTERVAL`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_reuse_days: Option<i16>,
    /// `PASSWORD REQUIRE CURRENT`: `default`, `yes` or `optional`.
    pub require_current: String,
    /// `FAILED_LOGIN_ATTEMPTS`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_login_attempts: Option<u16>,
    /// `PASSWORD_LOCK_TIME` minutes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_lock_minutes: Option<i32>,
    /// `MAX_QUERIES_PER_HOUR`, `MAX_UPDATES_PER_HOUR`,
    /// `MAX_CONNECTIONS_PER_HOUR`, `MAX_USER_CONNECTIONS`.
    pub limits: [u32; 4],
    /// `ATTRIBUTE` / `COMMENT` JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes: Option<serde_json::Value>,
    /// Its verifiers.
    pub verifiers: Vec<PortableVerifier>,
}

/// A tenant's accounts.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PortableAccounts {
    /// Live users, by id.
    pub users: Vec<PortableAccount>,
    /// SQL roles as `(name, host)`.
    pub roles: Vec<(String, String)>,
    /// On import: the users made tenant administrators (`ALL` and
    /// `SPRIG_TENANT_ADMIN` on `*.*`) when created, as `(name, host)`, so
    /// they can replay the other grants through the gateway.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tenant_admins: Vec<(String, String)>,
}

fn hex(b: &[u8]) -> String {
    b.iter()
        .fold(String::with_capacity(b.len() * 2), |mut s, x| {
            let _ = write!(s, "{x:02x}");
            s
        })
}

fn unhex(s: &str) -> Result<Vec<u8>> {
    let bad = || {
        SprigError::new(
            SprigCode::AdminRequestInvalid,
            "a verifier's auth_string_hex is not hex",
        )
    };
    if !s.len().is_multiple_of(2) {
        return Err(bad());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2).ok_or_else(bad)?, 16).map_err(|_| bad()))
        .collect()
}

fn bad(what: &str, e: &SprigError) -> SprigError {
    SprigError::new(
        SprigCode::AdminRequestInvalid,
        format!("{what}: {}", e.message),
    )
}

/// Reads `tenant`'s live users, their verifiers and its SQL roles from the
/// catalog's writer.
///
/// # Errors
/// The catalog's errors (39070, 39072).
pub async fn export(cat: &MariaCatalog, tenant: TenantId) -> Result<PortableAccounts> {
    let ep = cat.backend().locator().writer(tenant).await?;
    let mut conn = ep.pool.get_conn().await.map_err(crate::store::map_err)?;
    let store = AuthStore::of(&ep.store);
    let users = store.users_since(&mut conn, None).await?;
    let verifiers = store.all_verifiers(&mut conn).await?;
    let authz = AuthzStore::new(ep.store.schema());
    let roles = if authz.migrated(&mut conn).await? {
        authz
            .load(&mut conn)
            .await?
            .roles
            .into_iter()
            .map(|r| (r.name, r.host))
            .collect()
    } else {
        Vec::new()
    };
    let mut out = PortableAccounts {
        users: Vec::new(),
        roles,
        tenant_admins: Vec::new(),
    };
    for u in users.into_iter().filter(UserRow::is_live) {
        let vs = verifiers
            .iter()
            .filter(|v| v.user_id == u.user_id)
            .filter_map(|v| {
                Some(PortableVerifier {
                    mechanism: v.kind.as_str().to_owned(),
                    slot: v.slot.as_str().to_owned(),
                    auth_string_hex: hex(v.auth_string.as_deref()?),
                    generation: v.generation,
                })
            })
            .collect();
        out.users.push(PortableAccount {
            name: u.name,
            host: u.host,
            kind: u.kind.as_str().to_owned(),
            plugin: u.plugin,
            require_tls: u.require_tls.as_str().to_owned(),
            account_locked: u.account_locked,
            password_expired: u.password_expired,
            password_lifetime: u.password_lifetime,
            password_history: u.password_history,
            password_reuse_days: u.password_reuse_days,
            require_current: u.require_current.as_str().to_owned(),
            failed_login_attempts: u.failed_login_attempts,
            password_lock_minutes: u.password_lock_minutes,
            limits: [
                u.limits.max_queries_per_hour,
                u.limits.max_updates_per_hour,
                u.limits.max_connections_per_hour,
                u.limits.max_user_connections,
            ],
            attributes: u.attributes,
            verifiers: vs,
        });
    }
    Ok(out)
}

/// Creates `a` in `tenant` with its verifiers as they are (logins keep
/// working with the same passwords). `Ok(None)` when the live account
/// already exists: it is left alone.
///
/// # Errors
/// 39792 for a malformed account; the change's errors.
pub async fn import(
    cat: &MariaCatalog,
    tenant: TenantId,
    a: &PortableAccount,
    clock: &dyn Clock,
) -> Result<Option<Changed>> {
    if crate::admin::find_user(cat, tenant, &a.name, &a.host)
        .await?
        .is_some()
    {
        return Ok(None);
    }
    let kind = UserKind::parse(&a.kind).map_err(|e| bad("kind", &e))?;
    let require_tls = RequireTls::parse(&a.require_tls).map_err(|e| bad("require_tls", &e))?;
    let require_current =
        RequireCurrent::parse(&a.require_current).map_err(|e| bad("require_current", &e))?;
    let mut verifiers = Vec::new();
    for v in &a.verifiers {
        verifiers.push((
            VerifierKind::parse(&v.mechanism).map_err(|e| bad("mechanism", &e))?,
            Slot::parse(&v.slot).map_err(|e| bad("slot", &e))?,
            unhex(&v.auth_string_hex)?,
            v.generation,
        ));
    }
    let now = clock.wall();
    let mut c = cat
        .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
        .await?;
    let version = c.version();
    let result = async {
        let (core, q) = c.tx().parts();
        let store = AuthStore::of(core);
        let id = store.next_user_id(core, q).await?;
        let mut row = UserRow::new(id, &a.name, &a.host, UserSource::Api, now, version);
        row.kind = kind;
        row.plugin.clone_from(&a.plugin);
        row.require_tls = require_tls;
        row.account_locked = a.account_locked;
        row.password_expired = a.password_expired;
        row.password_changed_at = Some(now);
        row.password_lifetime = a.password_lifetime;
        row.password_history = a.password_history;
        row.password_reuse_days = a.password_reuse_days;
        row.require_current = require_current;
        row.failed_login_attempts = a.failed_login_attempts;
        row.password_lock_minutes = a.password_lock_minutes;
        row.limits = AccountLimits {
            max_queries_per_hour: a.limits[0],
            max_updates_per_hour: a.limits[1],
            max_connections_per_hour: a.limits[2],
            max_user_connections: a.limits[3],
        };
        row.attributes.clone_from(&a.attributes);
        store.insert_user(q, &row).await?;
        for (kind, slot, auth, generation) in &verifiers {
            let mut v = VerifierRow::mysql(id, *kind, *slot, auth.clone(), *generation, now);
            v.login_form = LoginForm::Any;
            store.insert_verifier(q, &v).await?;
        }
        Ok(id)
    }
    .await;
    match result {
        Ok(user_id) => Ok(Some(Changed {
            user_id,
            auth_version: c.commit().await?,
        })),
        Err(e) => {
            let _ = c.rollback().await;
            let _ = cat.settle(tenant, VersionKind::Auth).await;
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn hex_round_trips_and_rejects_junk() {
        let b = b"$A$005$\x00\xff abc".to_vec();
        assert_eq!(unhex(&hex(&b)).unwrap(), b);
        assert!(unhex("abc").is_err());
        assert!(unhex("zz").is_err());
    }

    #[test]
    fn verifier_debug_hides_the_string() {
        let v = PortableVerifier {
            mechanism: "mysql_native_password".into(),
            slot: "primary".into(),
            auth_string_hex: "2a41424344".into(),
            generation: 1,
        };
        assert!(!format!("{v:?}").contains("2a41"));
    }
}
