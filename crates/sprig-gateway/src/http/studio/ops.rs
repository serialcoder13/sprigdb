//! Studio's operator mode (chapter 44 step 17; chapters 23, 25, 47):
//! platform operators sign in like everyone else, and operator mode opens
//! when their login holds platform operator rights (decided 7 November
//! 2026).
//!
//! **Who is an operator** (decision ST35): logins listed in
//! `studio.operator_logins` (global scope, so only someone with the Admin
//! API's token can list one). `WhoAmI` reports them with the platform role
//! `operator`.
//!
//! **What they reach**: `/v1/ops/<path>` is the Admin API's
//! `/admin/v1/<path>` on the DC leader's controller, sent with the
//! gateway's `[gateway] operator_token_file` (the controller's
//! `admin_token_file`), after these checks on every call:
//!
//! ```text
//! 1. studio.operator_mode off → 39928
//! 2. a Studio session (no Basic, no API key, no impersonation), its login
//!    in studio.operator_logins → else 39928
//! 3. studio.operator_require_mfa and no otp/rec in the session → 39926
//! 4. studio.operator_mtls = required and no verified client cert → 39927
//! 5. writes (POST, PUT, DELETE) carry a reason; the gateway adds
//!    actor = studio:<tenant>.<user> to JSON bodies and audits
//!    studio.operator_action
//! 6. no operator_token_file → 503 naming it
//! ```
//!
//! **Named operators** (chapter 14, "Operators"; this is how operator mode
//! works once the deployment has operators): Studio signs the operator in
//! with `POST /v1/ops:login` (and `:enrol` for the first second factor),
//! which the gateway relays to the controller's `operators:login` with no
//! credential of its own. Studio keeps the operator's session token and
//! sends it as `x-sprig-operator` on every `/v1/ops/` call; the gateway
//! forwards it as the bearer token, so the controller checks the
//! operator's role, DCs and scopes and audits the person. Only checks 1
//! and 4 above apply; no Studio login list and no operator token file.
//! `POST /v1/ops:logout` ends the session.

use std::sync::Arc;

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::{Method, Request, Response};
use serde_json::{Value, json};
use sprig_common::{SprigCode, SprigError};
use tonic::body::Body;

use super::super::sql::Caller;
use super::super::{ConnInfo, Core};
use super::{mfa, parse_json, passthrough, read_body, session_caller};

/// Whether `login` (`tenant.user`) is in `logins` (comma-separated).
#[must_use]
pub fn listed(logins: &str, login: &str) -> bool {
    logins
        .split(',')
        .map(str::trim)
        .any(|l| !l.is_empty() && l.eq_ignore_ascii_case(login))
}

fn login_of(c: &Caller) -> String {
    match &c.principal.tenant_name {
        Some(t) => format!("{t}.{}", c.principal.user),
        None => c.principal.user.clone(),
    }
}

/// The platform roles of `caller`: `["operator"]` or none.
#[must_use]
pub fn roles(core: &Arc<Core>, caller: &Caller) -> Vec<&'static str> {
    use sprig_studio::settings as k;
    let st = &core.studio;
    if st.tenant_get(&k::OPERATOR_MODE, None, None).as_ref() != "on" {
        return Vec::new();
    }
    if mfa::acting(caller.authorization.as_deref()).is_some() {
        return Vec::new();
    }
    let logins = st.tenant_get(&k::OPERATOR_LOGINS, None, None);
    if listed(&logins, &login_of(caller)) {
        vec!["operator"]
    } else {
        Vec::new()
    }
}

fn disabled(why: &str) -> SprigError {
    SprigError::new(SprigCode::StudioOperatorModeDisabled, why.to_owned())
}

/// The checks of the module notes for one call; the session's methods.
fn check(
    core: &Arc<Core>,
    info: &ConnInfo,
    caller: &Caller,
    amr: &[String],
) -> Result<(), SprigError> {
    use sprig_studio::settings as k;
    let st = &core.studio;
    if st.tenant_get(&k::OPERATOR_MODE, None, None).as_ref() != "on" {
        return Err(disabled("operator mode is off (studio.operator_mode)"));
    }
    if roles(core, caller).is_empty() {
        return Err(disabled(
            "operator mode is not open to this login (studio.operator_logins)",
        ));
    }
    if st.tenant_get(&k::OPERATOR_REQUIRE_MFA, None, None)
        && !amr.iter().any(|m| m == "otp" || m == "rec")
    {
        return Err(SprigError::new(
            SprigCode::StudioOperatorMfaRequired,
            "operator mode needs a session signed in with a second factor (studio.operator_require_mfa)",
        ));
    }
    if st.tenant_get(&k::OPERATOR_MTLS, None, None).as_ref() == "required"
        && !info.tls.as_ref().is_some_and(|t| t.client_cert)
    {
        return Err(SprigError::new(
            SprigCode::StudioOperatorCertRequired,
            "operator mode needs a client certificate (studio.operator_mtls)",
        ));
    }
    Ok(())
}

/// The body sent on: `actor` set to the Studio login; writes need a
/// reason.
///
/// # Errors
/// 39792 for a write without a reason, or a body that isn't a JSON object.
pub fn forward_body(method: &Method, body: &[u8], login: &str) -> Result<Bytes, SprigError> {
    let write = *method != Method::GET && *method != Method::HEAD;
    if !write {
        return Ok(Bytes::new());
    }
    let mut v = parse_json(body)?;
    let Value::Object(o) = &mut v else {
        return Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            "an operator action's body is a JSON object",
        ));
    };
    let reason = o.get("reason").and_then(Value::as_str).unwrap_or("").trim();
    if reason.is_empty() {
        return Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            "operator actions need a reason (it is audited)",
        ));
    }
    o.insert("actor".into(), json!(format!("studio:{login}")));
    Ok(Bytes::from(v.to_string()))
}

/// The header that carries a named operator's session token.
pub const OPERATOR_HEADER: &str = "x-sprig-operator";

fn mode_and_cert(core: &Arc<Core>, info: &ConnInfo) -> Result<(), SprigError> {
    use sprig_studio::settings as k;
    let st = &core.studio;
    if st.tenant_get(&k::OPERATOR_MODE, None, None).as_ref() != "on" {
        return Err(disabled("operator mode is off (studio.operator_mode)"));
    }
    if st.tenant_get(&k::OPERATOR_MTLS, None, None).as_ref() == "required"
        && !info.tls.as_ref().is_some_and(|t| t.client_cert)
    {
        return Err(SprigError::new(
            SprigCode::StudioOperatorCertRequired,
            "operator mode needs a client certificate (studio.operator_mtls)",
        ));
    }
    Ok(())
}

/// `POST /v1/ops:login`, `:enrol` and `:logout` (named operators).
async fn session_call(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
    call: &str,
) -> Result<Response<Body>, SprigError> {
    mode_and_cert(core, info)?;
    if req.method() != Method::POST {
        return Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            "operator sign-in calls are POST",
        ));
    }
    let token = req
        .headers()
        .get(OPERATOR_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|t| format!("Bearer {}", t.trim()));
    let body = read_body(req).await?;
    let (path, auth, body) = match call {
        "login" | "enrol" => {
            let mut v = parse_json(&body)?;
            if let Value::Object(o) = &mut v {
                o.insert("client".into(), json!("studio"));
            }
            (
                format!("/admin/v1/operators:{call}"),
                None,
                Bytes::from(v.to_string()),
            )
        }
        "logout" => (
            "/admin/v1/operators:logout".to_owned(),
            token,
            Bytes::from_static(b"{}"),
        ),
        other => {
            return Err(SprigError::new(
                SprigCode::AdminRequestInvalid,
                format!("no route /v1/ops:{other}"),
            ));
        }
    };
    tracing::info!(target: "sprig_audit", event = "studio.operator_session", call,
        client_ip = %info.peer.ip(), "operator sign-in call from Studio");
    let r = core
        .admin
        .forward(&Method::POST, &path, auth.as_deref(), body)
        .await?;
    Ok(operator_reply(r.status, r.body))
}

/// The controller's answer to a named operator's call as it is: a 401
/// (an ended session, 39316) is the operator's to see, not a gateway
/// credential problem as [`passthrough`] reports it for internal calls.
fn operator_reply(status: u16, body: Bytes) -> Response<Body> {
    if status == 401 {
        let v: Value = serde_json::from_slice(&body).unwrap_or_else(|_| json!({}));
        return super::json_response(401, &v);
    }
    passthrough(status, body)
}

/// `/v1/ops/...` with a named operator's session token.
async fn named(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
    token: String,
    rest: String,
    query: Option<String>,
) -> Result<Response<Body>, SprigError> {
    mode_and_cert(core, info)?;
    let method = req.method().clone();
    let auth = format!("Bearer {token}");
    if rest.is_empty() {
        // Entering operator mode: who the controller takes the token for.
        let r = core
            .admin
            .forward(
                &Method::GET,
                "/admin/v1/operators:whoami",
                Some(&auth),
                Bytes::new(),
            )
            .await?;
        return Ok(operator_reply(r.status, r.body));
    }
    let body = read_body(req).await?;
    let body = if method == Method::GET || method == Method::HEAD {
        Bytes::new()
    } else {
        // Writes still carry a reason; the controller records the operator.
        forward_body(&method, &body, "operator")?
    };
    let path = match query {
        Some(q) => format!("/admin/v1/{rest}?{q}"),
        None => format!("/admin/v1/{rest}"),
    };
    metrics::counter!("sprig_studio_requests_total", "area" => "operator").increment(1);
    let r = core
        .admin
        .forward(&method, &path, Some(&auth), body)
        .await?;
    Ok(operator_reply(r.status, r.body))
}

/// Answers `/v1/ops/...` and `/v1/ops:...`.
///
/// # Errors
/// As the module notes; the controller's answer otherwise.
pub(super) async fn route(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    if let Some(call) = req.uri().path().strip_prefix("/v1/ops:") {
        let call = call.to_owned();
        return session_call(core, info, req, &call).await;
    }
    let method = req.method().clone();
    let rest = req
        .uri()
        .path()
        .trim_start_matches("/v1/ops")
        .trim_start_matches('/')
        .to_owned();
    let query = req.uri().query().map(str::to_owned);
    if rest
        .split('/')
        .any(|s| s == ".." || s == "." || s.is_empty())
        && !rest.is_empty()
    {
        return Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            "bad operator path",
        ));
    }
    if let Some(token) = req
        .headers()
        .get(OPERATOR_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|t| t.trim().to_owned())
        .filter(|t| !t.is_empty())
    {
        return named(core, info, req, token, rest, query).await;
    }
    let (caller, s) = session_caller(core, info, &req).await?;
    check(core, info, &caller, &s.claims.amr)?;
    let login = login_of(&caller);
    let body = forward_body(&method, &read_body(req).await?, &login)?;
    let Some(token) = core.studio.operator_token.as_deref() else {
        return Err(SprigError::new(
            SprigCode::NotLeader,
            "operator mode needs [gateway] operator_token_file (the controller's admin token)",
        ));
    };
    if rest.is_empty() {
        // Entering operator mode.
        tracing::info!(target: "sprig_audit", event = "studio.operator_mode_entered",
            login = %login, client_ip = %info.peer.ip(), "operator mode entered");
        return Ok(super::json_response(
            200,
            &json!({ "operator": login, "roles": ["operator"] }),
        ));
    }
    let path = match query {
        Some(q) => format!("/admin/v1/{rest}?{q}"),
        None => format!("/admin/v1/{rest}"),
    };
    if method != Method::GET {
        tracing::info!(target: "sprig_audit", event = "studio.operator_action", login = %login,
            method = %method, path = %path, client_ip = %info.peer.ip(), "operator action from Studio");
    }
    metrics::counter!("sprig_studio_requests_total", "area" => "operator").increment(1);
    let r = core
        .admin
        .forward(&method, &path, Some(&format!("Bearer {token}")), body)
        .await?;
    Ok(passthrough(r.status, r.body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logins_are_listed_exactly() {
        assert!(listed("platform.ops, acme.root", "acme.root"));
        assert!(listed("Platform.Ops", "platform.ops"));
        assert!(!listed("platform.ops", "platform.op"));
        assert!(!listed("", ""));
        assert!(!listed(" , ", "acme.app"));
    }

    #[test]
    fn writes_need_a_reason_and_carry_the_actor() {
        assert!(
            forward_body(&Method::GET, b"anything", "a.b")
                .unwrap()
                .is_empty()
        );
        let e = forward_body(&Method::POST, br#"{"kind":"split"}"#, "a.b").unwrap_err();
        assert!(e.to_string().contains("reason"));
        assert!(forward_body(&Method::POST, b"[1]", "a.b").is_err());
        let b = forward_body(
            &Method::POST,
            br#"{"reason":"hot shard","actor":"forged"}"#,
            "p.ops",
        )
        .unwrap();
        let v: Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(v["actor"], "studio:p.ops");
        assert_eq!(v["reason"], "hot shard");
    }
}
