//! Users and API keys for Studio's access views (chapter 44 step 13;
//! chapter 37's `Users` and `ApiKeys`, the R0 part Studio needs).
//!
//! | Route | Who | Does |
//! |---|---|---|
//! | `GET /v1/users` | any user (themselves); `CREATE USER` or `SPRIG_TENANT_ADMIN` (all) | The tenant's accounts, without secrets |
//! | `GET /v1/api-keys` | as above | Keys: id, name, user, expiry, revocation; never a secret |
//! | `POST /v1/api-keys` | the user for themselves; `CREATE USER` or admin for another user | Creates a key; the reply is the only time the key is shown |
//! | `POST /v1/api-keys/{key_id}:revoke` | the key's user; `CREATE USER` or admin | Revokes it |
//!
//! Account changes themselves (`CREATE USER`, `GRANT`, `DROP USER`) are
//! statements, which Studio's forms generate and show before running, so
//! there is no second path; passwords go through `Users.SetPassword`.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hyper::Request;
use hyper::Response;
use hyper::body::Incoming;
use serde_json::{Value, json};
use sprig_auth::apikey::{NewApiKey, create_api_key, revoke_api_key};
use sprig_auth::{LoginSource, TenantAuth};
use sprig_catalog::TenantCatalog;
use sprig_common::{SprigCode, SprigError};
use tonic::body::Body;

use super::super::sql::Caller;
use super::super::{ConnInfo, Core};
use super::grants::Privileges;
use super::{hdr, json_response, parse_json, read_body, rows_of, run_sql, text};

fn ms(t: SystemTime) -> String {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis())
        .to_string()
}

fn denied(what: &str) -> SprigError {
    SprigError::standard(
        1227,
        *b"42000",
        format!(
            "Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation ({what})"
        ),
    )
}

/// The caller, their privileges, and their tenant's catalog snapshot.
async fn context(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: &Request<Incoming>,
) -> Result<(Caller, Privileges, Arc<TenantCatalog>), SprigError> {
    let caller = core
        .caller(info, hdr(req, "authorization"), None, hdr(req, "host"))
        .await?;
    let grants = run_sql(core, &caller, "-", "SHOW GRANTS".to_owned()).await?;
    let lines: Vec<String> = rows_of(&grants)
        .into_iter()
        .filter_map(|r| r.into_iter().next().flatten())
        .collect();
    let privs = Privileges::of(&lines);
    let Some(login) = &core.studio.login else {
        return Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            "accounts need catalog logins",
        ));
    };
    let LoginSource::Catalog(cl) = login.source() else {
        return Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            "accounts need catalog logins (not the accounts file)",
        ));
    };
    let Some(tid) = caller.principal.tenant else {
        return Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            "accounts need a tenant login",
        ));
    };
    let cat = cl.catalogs().catalog(tid).await?;
    Ok((caller, privs, cat))
}

fn auth_of(cat: &TenantCatalog) -> Result<&TenantAuth, SprigError> {
    TenantAuth::of(cat).ok_or_else(|| {
        SprigError::new(
            SprigCode::CatalogUnavailable,
            "the tenant's accounts aren't loaded",
        )
    })
}

/// `GET /v1/users`.
pub(super) async fn list_users(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let (caller, privs, cat) = context(core, info, &req).await?;
    let auth = auth_of(&cat)?;
    let all = privs.create_user || privs.tenant_admin;
    let mut users: Vec<Value> = auth
        .users()
        .filter(|u| all || Some(u.row.user_id) == caller.principal.user_id)
        .map(|u| {
            let r = &u.row;
            json!({
                "user_id": r.user_id.0.to_string(),
                "user": r.name,
                "host": r.host,
                "kind": r.kind.as_str(),
                "auth_methods": r.auth_methods.to_sql(),
                "plugin": r.plugin,
                "require_tls": r.require_tls.as_str(),
                "account_locked": r.account_locked,
                "password_expired": r.password_expired,
                "password_changed_at_ms": r.password_changed_at.map(ms),
                "created_at_ms": ms(r.created_at),
                "created_by_tenant": r.created_by_tenant.to_string(),
            })
        })
        .collect();
    users.sort_by(|a, b| {
        (a["user"].as_str(), a["host"].as_str()).cmp(&(b["user"].as_str(), b["host"].as_str()))
    });
    Ok(json_response(200, &json!({ "users": users, "all": all })))
}

/// `GET /v1/api-keys`.
pub(super) async fn list_keys(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let (caller, privs, cat) = context(core, info, &req).await?;
    let auth = auth_of(&cat)?;
    let all = privs.create_user || privs.tenant_admin;
    let mut keys: Vec<Value> = auth
        .api_keys()
        .filter(|k| all || Some(k.row.user_id) == caller.principal.user_id)
        .map(|k| {
            let r = &k.row;
            let user = auth.user(r.user_id);
            json!({
                "key_id": r.key_id,
                "name": r.name,
                "user": user.map(|u| u.row.name.clone()),
                "host": user.map(|u| u.row.host.clone()),
                "allowed_cidrs": r.allowed_cidrs,
                "expires_at_ms": r.expires_at.map(ms),
                "revoked_at_ms": r.revoked_at.map(ms),
                "created_at_ms": ms(r.created_at),
            })
        })
        .collect();
    keys.sort_by(|a, b| {
        b["created_at_ms"]
            .as_str()
            .cmp(&a["created_at_ms"].as_str())
    });
    Ok(json_response(200, &json!({ "keys": keys, "all": all })))
}

/// `POST /v1/api-keys`: `{name, user?, host?, expires_days?,
/// allowed_cidrs?}`.
pub(super) async fn create_key(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let (caller, privs, cat) = context(core, info, &req).await?;
    let body = parse_json(&read_body(req).await?)?;
    let auth = auth_of(&cat)?;
    let name = text(&body, "name");
    let who = text(&body, "user");
    let host = match text(&body, "host") {
        h if h.is_empty() => "%".to_owned(),
        h => h,
    };
    let target =
        if who.is_empty() || (who == caller.principal.user && host == caller.principal.host) {
            caller.principal.user_id
        } else {
            if !(privs.create_user || privs.tenant_admin) {
                return Err(denied("an API key for another user"));
            }
            auth.users()
                .find(|u| u.row.name == who && u.row.host == host)
                .map(|u| u.row.user_id)
        };
    let Some(user_id) = target else {
        return Err(SprigError::standard(
            1396,
            *b"HY000",
            format!("Operation CREATE API KEY failed for '{who}'@'{host}'"),
        ));
    };
    let Some(svc) = &core.studio.tenants else {
        return Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            "API keys need the tenant catalog",
        ));
    };
    let (Some(tid), Some(tname)) = (
        caller.principal.tenant,
        caller.principal.tenant_name.clone(),
    ) else {
        return Err(denied("no tenant"));
    };
    let expires_at = body
        .get("expires_days")
        .and_then(Value::as_u64)
        .filter(|d| *d > 0)
        .map(|d| SystemTime::now() + Duration::from_secs(d.min(3650) * 86_400));
    let allowed: Vec<String> = body
        .get("allowed_cidrs")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let created = create_api_key(
        &svc.catalog,
        tid,
        &tname,
        user_id,
        &NewApiKey {
            name: name.clone(),
            scopes: Vec::new(),
            allowed_cidrs: allowed,
            expires_at,
            created_by: caller.principal.user_id,
        },
        SystemTime::now(),
    )
    .await?;
    tracing::info!(target: "sprig_audit", event = "auth.api_key_created", key = %created.key_id,
        by = %caller.principal.user, client = "studio", "API key created");
    Ok(json_response(
        200,
        &json!({ "key_id": created.key_id, "key": created.key.as_str(), "name": name }),
    ))
}

/// `POST /v1/api-keys/{key_id}:revoke`.
pub(super) async fn revoke_key(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
    key_id: &str,
) -> Result<Response<Body>, SprigError> {
    let (caller, privs, cat) = context(core, info, &req).await?;
    let auth = auth_of(&cat)?;
    let Some(k) = auth.api_key(key_id) else {
        return Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            format!("no API key {key_id}"),
        ));
    };
    let own = Some(k.row.user_id) == caller.principal.user_id;
    if !(own || privs.create_user || privs.tenant_admin) {
        return Err(denied("revoking another user's API key"));
    }
    let (Some(svc), Some(tid)) = (&core.studio.tenants, caller.principal.tenant) else {
        return Err(denied("no tenant"));
    };
    let changed = revoke_api_key(&svc.catalog, tid, key_id, SystemTime::now()).await?;
    tracing::info!(target: "sprig_audit", event = "auth.api_key_revoked", key = %key_id,
        by = %caller.principal.user, client = "studio", "API key revoked");
    Ok(json_response(
        200,
        &json!({ "key_id": key_id, "revoked": changed }),
    ))
}
