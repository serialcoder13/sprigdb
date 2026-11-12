//! Second factors and impersonation over HTTP (chapter 14 steps 13–14;
//! chapter 44 step 16), on `sprig-auth::mfa`.
//!
//! | Route | Does |
//! |---|---|
//! | `POST /v1/auth/login` | After the password: an active TOTP factor → `{challenge, methods, expires_in}` and no tokens; none, but `auth.mfa_required` covers the user → `{enrol, expires_in}` (a ticket that only enrols) |
//! | `POST /v1/auth/mfa` | `{challenge, code}`: a TOTP code or a recovery code completes the login (`amr` `pwd` plus `otp` or `rec`); cookie rules as login |
//! | `GET /v1/auth/factors` | The caller's factors (never secrets), recovery codes left, whether a factor is required |
//! | `POST /v1/auth/factors:enrol` | A new TOTP seed, shown once (base32 and `otpauth://`), stored sealed and `pending`; with a session, or an `enrol` ticket |
//! | `POST /v1/auth/factors:confirm` | `{code}`: the factor becomes active, 10 recovery codes are shown once; with an `enrol` ticket it also signs in |
//! | `POST /v1/auth/factors:reset` | `{code}` for one's own (a current code or a recovery code), or `{user, host}` for an administrator (`SPRIG_TENANT_ADMIN` or `CREATE USER`): every factor deleted, every session of the user revoked |
//! | `POST /v1/auth/impersonate` | `{tenant?, user, host?, reason, minutes}`: a session as the user, with the administrator in its `act` claim |
//!
//! Challenge and enrol tickets are JWS signed like export tickets (any
//! gateway verifies them) with `kind` `mfa` or `enrol`, the tenant and
//! user ids and an expiry of `auth.mfa_challenge_seconds`. A code works
//! once: TOTP steps are stored with a compare-and-set, recovery codes are
//! deleted when used.
//!
//! **Impersonation** (decision ST34) needs a session (no Basic or API
//! key), `SPRIG_TENANT_ADMIN` over the user's tenant (the caller's own or a
//! descendant), a user who doesn't administer the caller's tenant,
//! `auth.impersonation` on for that tenant, a reason, at most
//! `auth.impersonation_max_minutes`, and a second factor in the caller's
//! session when `auth.mfa_required` covers the caller. The session has no
//! refresh token and ends with its access token; an impersonation session
//! can't impersonate, change passwords or factors, or create API keys.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hyper::body::Incoming;
use hyper::header::{self, HeaderValue};
use hyper::{Request, Response};
use serde_json::{Value, json};
use sprig_auth::LoginSource;
use sprig_auth::mfa::{AccountRef, FactorKeys, FactorStore};
use sprig_auth::token::{Act, Limits, sessions::Impersonator};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{SprigCode, SprigError};
use sprig_studio::StudioConfig;
use tonic::body::Body;

use super::super::sql::Caller;
use super::super::{ConnInfo, Core};
use super::session::ACCESS_PREFIX;
use super::{
    CSRF_HEADER, bearer, cookie_secure_for, csrf_reject, grants, hdr, invalid, json_response,
    parse_json, read_body, rows_of, run_sql, same_origin, session, session_caller, set_cookie,
    tenant_tree, text,
};

/// Seeds sealed with keys derived from the tenant-data-key file.
pub struct KeyringFactorKeys(pub sprig_workload::keyring::MasterKeyring);

impl FactorKeys for KeyringFactorKeys {
    fn current(&self, tenant: TenantId) -> Option<(u32, [u8; 32])> {
        self.0
            .current_for(sprig_auth::mfa::KEY_PURPOSE, tenant)
            .map(|(v, k)| (v, *k.bytes()))
    }

    fn key(&self, tenant: TenantId, version: u32) -> Option<[u8; 32]> {
        self.0
            .key_for(sprig_auth::mfa::KEY_PURPOSE, tenant, version)
            .map(|k| *k.bytes())
    }
}

fn now_s() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn denied(why: &str) -> SprigError {
    SprigError::standard(1045, *b"28000", format!("Access denied: {why}"))
}

fn imp_denied(why: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::ImpersonationDenied, why)
}

fn unavailable() -> SprigError {
    SprigError::new(
        SprigCode::AuthUnavailable,
        "second factors need tenant catalogs and a factor key ([gateway] workload_key_file)",
    )
}

fn parts(core: &Arc<Core>) -> Result<(&FactorStore, &dyn FactorKeys), SprigError> {
    match (&core.studio.factors, &core.studio.factor_keys) {
        (Some(s), Some(k)) => Ok((s, k.as_ref())),
        _ => Err(unavailable()),
    }
}

fn ids(c: &Caller) -> Result<(TenantId, UserId), SprigError> {
    match (c.principal.tenant, c.principal.user_id) {
        (Some(t), Some(u)) => Ok((t, u)),
        _ => Err(invalid("second factors need a tenant catalog login")),
    }
}

/// The `act` claim of a verified bearer token, read without checking the
/// signature: only for a caller already verified.
#[must_use]
pub fn acting(authorization: Option<&str>) -> Option<Act> {
    use base64::Engine as _;
    let t = bearer(authorization)?.strip_prefix(ACCESS_PREFIX)?;
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(t.split('.').nth(1)?)
        .ok()?;
    let v: Value = serde_json::from_slice(&raw).ok()?;
    serde_json::from_value(v.get("act")?.clone()).ok()
}

/// Refuses an impersonation session (chapter 14: it can't impersonate,
/// change passwords or factors, or create API keys).
///
/// # Errors
/// 39305.
pub fn not_impersonating(authorization: Option<&str>, what: &str) -> Result<(), SprigError> {
    match acting(authorization) {
        Some(a) => Err(imp_denied(format!(
            "an impersonation session (by {}) can't {what}",
            a.sub
        ))),
        None => Ok(()),
    }
}

/// Whether `auth.mfa_required` covers `caller` (`admins`: holders of
/// `SPRIG_TENANT_ADMIN`).
async fn required(core: &Arc<Core>, caller: &Caller) -> bool {
    let v = core.studio.tenant_get(
        &sprig_auth::settings::MFA_REQUIRED,
        caller.principal.tenant,
        None,
    );
    match v.as_ref() {
        "all" => true,
        "admins" => privileges(core, caller).await.tenant_admin,
        _ => false,
    }
}

async fn privileges(core: &Arc<Core>, caller: &Caller) -> grants::Privileges {
    let lines: Vec<String> = match run_sql(core, caller, "-", "SHOW GRANTS".to_owned()).await {
        Ok(r) => rows_of(&r)
            .into_iter()
            .filter_map(|row| row.into_iter().next().flatten())
            .collect(),
        Err(_) => Vec::new(),
    };
    grants::Privileges::of(&lines)
}

fn challenge_seconds(core: &Arc<Core>, tenant: Option<TenantId>) -> u64 {
    core.studio
        .tenant_get(&sprig_auth::settings::MFA_CHALLENGE_SECONDS, tenant, None)
}

fn sign(
    core: &Arc<Core>,
    kind: &str,
    caller: &Caller,
    client: &str,
) -> Result<(String, u64), SprigError> {
    let (t, u) = ids(caller)?;
    let ttl = challenge_seconds(core, Some(t));
    let ticket = core.studio.tokens.sign_ticket(&json!({
        "kind": kind,
        "ten": caller.principal.tenant_name,
        "tid": t.0.to_string(),
        "uid": u.0,
        "cli": client,
        "exp": now_s() + ttl,
    }))?;
    Ok((ticket, ttl))
}

/// A verified challenge or enrol ticket: `(tenant name, tenant, user, client)`.
async fn open_ticket(
    core: &Arc<Core>,
    ticket: &str,
    kind: &str,
) -> Result<(String, TenantId, UserId, String), SprigError> {
    let v: Value = core
        .studio
        .tokens
        .verify_ticket(ticket, SystemTime::now())
        .await
        .map_err(|_| denied("the challenge is not valid; sign in again"))?;
    if v["kind"] != kind || v["exp"].as_u64().unwrap_or(0) <= now_s() {
        return Err(denied("the challenge expired; sign in again"));
    }
    let tid: u64 = v["tid"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
    let uid = v["uid"]
        .as_u64()
        .and_then(|u| u32::try_from(u).ok())
        .unwrap_or(0);
    Ok((
        v["ten"].as_str().unwrap_or("").to_owned(),
        TenantId(tid),
        UserId(uid),
        v["cli"].as_str().unwrap_or("studio").to_owned(),
    ))
}

/// After a correct password at `/v1/auth/login`: the challenge or the
/// enrol ticket when a second factor is involved; `None` to sign in.
///
/// # Errors
/// The factor store's.
pub(super) async fn after_password(
    core: &Arc<Core>,
    caller: &Caller,
    client: &str,
) -> Result<Option<Response<Body>>, SprigError> {
    let (Some(store), Ok((t, u))) = (&core.studio.factors, ids(caller)) else {
        return Ok(None);
    };
    if store.has_active(t, u).await? {
        let (ticket, ttl) = sign(core, "mfa", caller, client)?;
        return Ok(Some(json_response(
            200,
            &json!({ "challenge": ticket, "methods": ["totp", "recovery"], "expires_in": ttl }),
        )));
    }
    if required(core, caller).await {
        let (ticket, ttl) = sign(core, "enrol", caller, client)?;
        tracing::info!(target: "sprig_audit", event = "auth.login_failed", reason = "mfa_enrolment_required",
            user = %caller.principal.user, "a second factor must be enrolled first");
        return Ok(Some(json_response(
            200,
            &json!({ "enrol": ticket, "expires_in": ttl }),
        )));
    }
    Ok(None)
}

/// Issues the session after a second factor, as login does (cookie or
/// body).
#[allow(clippy::too_many_arguments)] // one session's parts, from two call sites
async fn sign_in(
    core: &Arc<Core>,
    info: &ConnInfo,
    tenant: &str,
    user: UserId,
    client: &str,
    amr: Vec<String>,
    cookie: bool,
    secure: bool,
    extra: Value,
) -> Result<Response<Body>, SprigError> {
    let login = core.studio.login.as_ref().ok_or_else(unavailable)?;
    let LoginSource::Catalog(cl) = login.source() else {
        return Err(unavailable());
    };
    let now = SystemTime::now();
    let p = sprig_auth::mfa::account_principal(
        cl,
        tenant,
        AccountRef::Id(user),
        info.peer.ip(),
        info.tls.is_some(),
        now,
    )
    .await?;
    let limits = core.studio.limits_for(p.tenant);
    let issued = core
        .studio
        .tokens
        .issue(p, client, limits, now, amr.clone(), None)
        .await?;
    metrics::gauge!("sprig_studio_sessions").increment(1.0);
    tracing::info!(target: "sprig_audit", event = "studio.session_started", sid = %issued.sid,
        amr = %amr.join(","), client_ip = %info.peer.ip(), "Studio session started");
    let mut out = json!({
        "access_token": issued.access_token,
        "token_type": "Bearer",
        "expires_in": issued.expires_in,
        "session_id": issued.sid,
    });
    if let (Value::Object(o), Value::Object(e)) = (&mut out, extra) {
        o.extend(e);
    }
    if cookie {
        let mut r = json_response(200, &out);
        let c = set_cookie(secure, &issued.refresh_token, limits.max_age.as_secs());
        if let Ok(v) = HeaderValue::from_str(&c) {
            r.headers_mut().append(header::SET_COOKIE, v);
        }
        Ok(r)
    } else {
        out["refresh_token"] = Value::String(issued.refresh_token);
        Ok(json_response(200, &out))
    }
}

/// The headers the cookie rules and the subject need, read before the
/// body.
struct Hdrs {
    host: Option<String>,
    origin: Option<String>,
    site: Option<String>,
    csrf: bool,
    auth: Option<String>,
}

impl Hdrs {
    fn of(req: &Request<Incoming>) -> Self {
        Self {
            host: hdr(req, "host"),
            origin: hdr(req, "origin"),
            site: hdr(req, "sec-fetch-site"),
            csrf: req.headers().get(CSRF_HEADER).is_some(),
            auth: hdr(req, "authorization"),
        }
    }

    /// The cookie rules of a login (`refresh_transport` `cookie`); whether
    /// the refresh token goes in the cookie.
    fn cookie_rules(
        &self,
        cfg: &StudioConfig,
        info: &ConnInfo,
        body: &Value,
    ) -> Result<bool, SprigError> {
        let cookie = text(body, "refresh_transport") != "body";
        if cookie {
            same_origin(
                cfg,
                info.tls.is_some(),
                self.host.as_deref(),
                self.origin.as_deref(),
                self.site.as_deref(),
            )?;
            if !self.csrf {
                return Err(csrf_reject(
                    "header",
                    SprigError::new(
                        SprigCode::StudioCsrfHeaderMissing,
                        "cookie requests need the X-Sprig-CSRF header",
                    ),
                ));
            }
        }
        Ok(cookie)
    }
}

/// `POST /v1/auth/mfa`.
pub(super) async fn complete(
    core: &Arc<Core>,
    cfg: &StudioConfig,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let hdrs = Hdrs::of(&req);
    let body = parse_json(&read_body(req).await?)?;
    let cookie = hdrs.cookie_rules(cfg, info, &body)?;
    let (tenant, tid, uid, client) = open_ticket(core, &text(&body, "challenge"), "mfa").await?;
    let (store, keys) = parts(core)?;
    let factor_code = text(&body, "code");
    let Some(method) = store
        .verify(keys, tid, uid, &factor_code, SystemTime::now())
        .await?
    else {
        metrics::counter!("sprig_auth_mfa_total", "method" => "any", "outcome" => "failed")
            .increment(1);
        tracing::info!(target: "sprig_audit", event = "auth.login_failed", reason = "mfa_failed",
            tenant = %tenant, user_id = uid.0, client_ip = %info.peer.ip(), "second factor refused");
        return Err(denied("the code is not valid, or was already used"));
    };
    metrics::counter!("sprig_auth_mfa_total", "method" => method, "outcome" => "ok").increment(1);
    let left = if method == "rec" {
        json!({ "recovery_left": store.recovery_left(tid, uid).await? })
    } else {
        json!({})
    };
    sign_in(
        core,
        info,
        &tenant,
        uid,
        &client,
        vec!["pwd".into(), method.into()],
        cookie,
        cookie_secure_for(info, hdrs.origin.as_deref()),
        left,
    )
    .await
}

/// Who a factors call is for: a session's user, or an enrol ticket's.
enum Subject {
    Session(Box<Caller>),
    Ticket {
        tenant: String,
        t: TenantId,
        u: UserId,
        client: String,
    },
}

/// `/v1/auth/factors...`.
#[allow(clippy::too_many_lines)] // the four verbs and their two subjects
pub(super) async fn factors(
    core: &Arc<Core>,
    cfg: &StudioConfig,
    info: &ConnInfo,
    req: Request<Incoming>,
    verb: &str,
) -> Result<Response<Body>, SprigError> {
    let (store, keys) = parts(core)?;
    if verb.is_empty() {
        let (caller, _) = session_caller(core, info, &req).await?;
        let (tid, uid) = ids(&caller)?;
        let list = store.list(tid, uid).await?;
        let shown: Vec<Value> = list
            .iter()
            .filter(|f| f.kind != "recovery")
            .map(|f| {
                json!({"id": f.id.to_string(), "kind": f.kind, "state": f.state,
                "created_ms": f.created_ms.to_string()})
            })
            .collect();
        let left = list.iter().filter(|f| f.kind == "recovery").count();
        return Ok(json_response(
            200,
            &json!({ "factors": shown, "recovery_left": left, "required": required(core, &caller).await }),
        ));
    }
    let hdrs = Hdrs::of(&req);
    let body = parse_json(&read_body(req).await?)?;
    let sub = subject(core, info, &body, hdrs.auth.as_deref()).await?;
    let now = SystemTime::now();
    let (tenant_name, tid, uid, label) = match &sub {
        Subject::Session(c) => {
            let (tid, uid) = ids(c)?;
            (
                c.principal.tenant_name.clone().unwrap_or_default(),
                tid,
                uid,
                format!(
                    "{}.{}",
                    c.principal.tenant_name.clone().unwrap_or_default(),
                    c.principal.user
                ),
            )
        }
        Subject::Ticket {
            tenant,
            t: tid,
            u: uid,
            ..
        } => (tenant.clone(), *tid, *uid, format!("{tenant}#{}", uid.0)),
    };
    match verb {
        "enrol" => {
            let seed = store.enrol(keys, tid, uid, now).await?;
            let uri =
                sprig_auth::mfa::otpauth_uri(&format!("SprigDB {tenant_name}"), &label, &seed);
            Ok(json_response(
                200,
                &json!({ "secret": sprig_auth::mfa::base32(&seed), "otpauth_uri": uri,
                    "period": sprig_auth::mfa::STEP, "digits": 6 }),
            ))
        }
        "confirm" => {
            let Some(codes) = store
                .confirm(keys, tid, uid, &text(&body, "code"), now)
                .await?
            else {
                return Err(denied(
                    "the code is not valid; check the device's clock and try again",
                ));
            };
            tracing::info!(target: "sprig_audit", event = "auth.factor_enrolled", kind = "totp",
                tenant = %tenant_name, user_id = uid.0, "TOTP factor enrolled");
            let extra = json!({ "recovery_codes": codes });
            match sub {
                Subject::Ticket { tenant, client, .. } => {
                    // The enrolment ticket's login: signing in with the
                    // code just shown.
                    let cookie = hdrs.cookie_rules(cfg, info, &body)?;
                    sign_in(
                        core,
                        info,
                        &tenant,
                        uid,
                        &client,
                        vec!["pwd".into(), "otp".into()],
                        cookie,
                        cookie_secure_for(info, hdrs.origin.as_deref()),
                        extra,
                    )
                    .await
                }
                Subject::Session(_) => Ok(json_response(200, &extra)),
            }
        }
        "reset" => {
            let Subject::Session(caller) = sub else {
                return Err(denied("sign in to remove factors"));
            };
            let target_user = text(&body, "user");
            let (rt, ru) = if target_user.is_empty() || target_user == caller.principal.user {
                // One's own: prove it with a current code or a recovery code.
                if store
                    .verify(keys, tid, uid, &text(&body, "code"), now)
                    .await?
                    .is_none()
                {
                    return Err(denied(
                        "removing your factor needs a current code or a recovery code",
                    ));
                }
                (tid, uid)
            } else {
                // An administrator's reset.
                let p = privileges(core, &caller).await;
                if !(p.tenant_admin || p.create_user) {
                    return Err(SprigError::standard(
                        1227,
                        *b"42000",
                        "Access denied; you need (at least one of) the CREATE USER or SPRIG_TENANT_ADMIN privilege(s) for this operation",
                    ));
                }
                let login = core.studio.login.as_ref().ok_or_else(unavailable)?;
                let LoginSource::Catalog(cl) = login.source() else {
                    return Err(unavailable());
                };
                let host = text(&body, "host");
                let p = sprig_auth::mfa::account_principal(
                    cl,
                    &tenant_name,
                    AccountRef::Name(&target_user, (!host.is_empty()).then_some(host.as_str())),
                    info.peer.ip(),
                    true,
                    now,
                )
                .await?;
                (tid, p.user_id.ok_or_else(|| denied("no such account"))?)
            };
            let n = store.reset(rt, ru, now).await?;
            tracing::info!(target: "sprig_audit", event = "auth.factor_reset", tenant = %tenant_name,
                user_id = ru.0, by = %label, factors = n, "second factors reset; sessions revoked");
            Ok(json_response(200, &json!({ "reset": n })))
        }
        _ => Err(invalid(format!("no route POST /v1/auth/factors:{verb}"))),
    }
}

async fn subject(
    core: &Arc<Core>,
    info: &ConnInfo,
    body: &Value,
    authorization: Option<&str>,
) -> Result<Subject, SprigError> {
    let ticket = text(body, "enrol");
    if !ticket.is_empty() {
        let (tenant, t, u, client) = open_ticket(core, &ticket, "enrol").await?;
        return Ok(Subject::Ticket {
            tenant,
            t,
            u,
            client,
        });
    }
    not_impersonating(authorization, "change second factors")?;
    let Some(token) = bearer(authorization).filter(|t| t.starts_with(ACCESS_PREFIX)) else {
        return Err(session::denied("sign in: this call needs a session token"));
    };
    let v = core
        .studio
        .verify(token, info.peer.ip(), info.tls.is_some())
        .await?;
    Ok(Subject::Session(Box::new(Caller {
        principal: v.principal,
        peer: info.peer,
        tls: info.tls.clone(),
        listener: info.listener,
        generation: info.generation,
        authorization: authorization.map(str::to_owned),
        client_dc: None,
    })))
}

/// `POST /v1/auth/impersonate`.
#[allow(clippy::too_many_lines)] // chapter 14's checks, one after another
pub(super) async fn impersonate(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let auth = hdr(&req, "authorization");
    not_impersonating(auth.as_deref(), "start another impersonation")?;
    let (caller, s) = session_caller(core, info, &req).await?;
    let b = parse_json(&read_body(req).await?)?;
    let me = caller.principal.clone();
    let home = me.tenant_name.clone().unwrap_or_default();
    let target_tenant = Some(text(&b, "tenant"))
        .filter(|t| !t.is_empty())
        .unwrap_or_else(|| home.clone());
    let user = text(&b, "user");
    let host = text(&b, "host");
    let reason = text(&b, "reason");
    if user.is_empty() {
        return Err(invalid("user is required"));
    }
    if reason.trim().is_empty() {
        return Err(imp_denied("impersonation needs a reason"));
    }
    // An administrator over the tenant: the caller's own or a descendant.
    let privs = privileges(core, &caller).await;
    if !privs.tenant_admin {
        return Err(imp_denied(
            "impersonation needs SPRIG_TENANT_ADMIN over the user's tenant",
        ));
    }
    let tree = tenant_tree(core, &me, true).await;
    let node = tree
        .as_array()
        .into_iter()
        .flatten()
        .find(|n| n["name"].as_str() == Some(target_tenant.as_str()))
        .cloned()
        .ok_or_else(|| {
            imp_denied(format!(
                "tenant {target_tenant} is not yours or below yours"
            ))
        })?;
    let target_tid = node["id"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .map(TenantId);
    // auth.impersonation at the user's tenant (an ancestor may lock it off).
    let on = core
        .studio
        .tenant_get(&sprig_auth::settings::IMPERSONATION, target_tid, None);
    if on.as_ref() != "on" {
        return Err(imp_denied(format!(
            "auth.impersonation is off for tenant {target_tenant}"
        )));
    }
    let max = core.studio.tenant_get(
        &sprig_auth::settings::IMPERSONATION_MAX_MINUTES,
        target_tid,
        None,
    );
    let minutes = b["minutes"].as_u64().unwrap_or(max.min(15));
    if minutes == 0 || minutes > max {
        return Err(imp_denied(format!(
            "an impersonation lasts 1 to {max} minutes (auth.impersonation_max_minutes)"
        )));
    }
    // A second factor first, when one is required of the caller.
    if required(core, &caller).await && !s.claims.amr.iter().any(|m| m == "otp" || m == "rec") {
        return Err(SprigError::new(
            SprigCode::MfaRequired,
            "impersonation needs a session signed in with a second factor (auth.mfa_required)",
        ));
    }
    let login = core.studio.login.as_ref().ok_or_else(unavailable)?;
    let LoginSource::Catalog(cl) = login.source() else {
        return Err(unavailable());
    };
    let now = SystemTime::now();
    let target = sprig_auth::mfa::account_principal(
        cl,
        &target_tenant,
        AccountRef::Name(&user, (!host.is_empty()).then_some(host.as_str())),
        info.peer.ip(),
        info.tls.is_some(),
        now,
    )
    .await?;
    if target.tenant == me.tenant && target.user_id == me.user_id {
        return Err(imp_denied("you can't impersonate yourself"));
    }
    // The user must not administer the caller's tenant: in a descendant
    // that can't happen (grants point down); in the same tenant, check.
    if target.tenant == me.tenant {
        let tc = Caller {
            principal: target.clone(),
            ..caller.clone()
        };
        if privileges(core, &tc).await.tenant_admin {
            return Err(imp_denied(format!(
                "{user} administers your tenant: administrators can't be impersonated"
            )));
        }
    }
    let ttl = Duration::from_secs(minutes * 60);
    let limits = Limits {
        idle: ttl,
        max_age: ttl,
        access_ttl: ttl,
    };
    let act = Act {
        sub: format!("{home}.{}", me.user),
        id: format!(
            "{}:{}",
            me.tenant.map_or(0, |t| t.0),
            me.user_id.map_or(0, |u| u.0)
        ),
    };
    let imp = Impersonator {
        tenant_id: me.tenant.map_or(0, |t| t.0),
        user_id: me.user_id.map_or(0, |u| u.0),
        reason: reason.clone(),
    };
    let target_login = format!("{target_tenant}.{}", target.user);
    let issued = core
        .studio
        .tokens
        .issue(
            target,
            "studio",
            limits,
            now,
            s.claims.amr.clone(),
            Some((act, imp)),
        )
        .await?;
    metrics::counter!("sprig_auth_impersonations_total").increment(1);
    tracing::info!(target: "sprig_audit", event = "auth.impersonation_started", sid = %issued.sid,
        tenant = %target_tenant, user = %target_login, by = %format!("{home}.{}", me.user),
        reason = %reason, minutes, "impersonation started");
    Ok(json_response(
        200,
        &json!({
            "access_token": issued.access_token,
            "token_type": "Bearer",
            "expires_in": issued.expires_in,
            "session_id": issued.sid,
            "impersonating": target_login,
            "by": format!("{home}.{}", me.user),
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;

    fn token(payload: &Value) -> String {
        let b = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
        format!("Bearer {ACCESS_PREFIX}e30.{b}.sig")
    }

    #[test]
    fn impersonation_sessions_are_refused_sensitive_calls() {
        let plain = token(&json!({"sub": "1:2"}));
        assert!(not_impersonating(Some(&plain), "x").is_ok());
        assert!(not_impersonating(None, "x").is_ok());
        let imp = token(&json!({"sub": "1:3", "act": {"sub": "acme.admin", "id": "1:2"}}));
        let e = not_impersonating(Some(&imp), "create API keys").unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::ImpersonationDenied));
        assert!(e.to_string().contains("acme.admin"));
        assert_eq!(acting(Some(&imp)).unwrap().id, "1:2");
    }
}
