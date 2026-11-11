//! The `Recommendations` service's REST routes (chapter 52, "Admin API"),
//! merged into the controller's Admin API router.
//!
//! Credentials: the admin token is a platform operator; `[controller]
//! dc_operator_token_file` is an operator of this controller's DC; the
//! gateways' internal credential must carry the caller as a claim (header
//! `x-sprig-caller` on reads, `caller` in action bodies), which the
//! service checks against each item.

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path as UrlPath, State};
use axum::http::{HeaderMap, Uri};
use axum::response::Response;
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use sprig_approvals::catalog::Subscription;
use sprig_approvals::{Act, Caller};
use sprig_common::Result;
use sprig_common::ids::DcId;

use super::service::{ActRequest, BulkRequest, ListQuery, RecommendationsService, denied};
use crate::admin::http::{query, respond, token_matches, unauthenticated};
use crate::admin::types::{ScopeRef, SettingContextQuery, SettingOp, SettingWrite};
use crate::admin::{AdminService, invalid};

/// The routes' state.
pub struct RecApp {
    svc: Arc<RecommendationsService>,
    admin: Option<Arc<AdminService>>,
    token: Option<String>,
    gateway_token: Option<String>,
    dc_token: Option<String>,
    dc: DcId,
}

impl std::fmt::Debug for RecApp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecApp").finish_non_exhaustive()
    }
}

enum Who {
    Platform,
    DcOperator,
    Gateway,
}

impl RecApp {
    /// The state.
    #[must_use]
    pub fn new(
        svc: Arc<RecommendationsService>,
        admin: Option<Arc<AdminService>>,
        token: Option<String>,
        gateway_token: Option<String>,
        dc_token: Option<String>,
        dc: DcId,
    ) -> Self {
        Self {
            svc,
            admin,
            token,
            gateway_token,
            dc_token,
            dc,
        }
    }

    fn who(&self, headers: &HeaderMap) -> Result<Who> {
        // Behind the operators' guard (chapter 14, "Operators"): the
        // caller it established, by role.
        if headers.contains_key(crate::operators::guard::AUTHN_HEADER) {
            let p = crate::operators::http::principal(headers)?;
            return if p.is_gateway() {
                Ok(Who::Gateway)
            } else if p.platform {
                Ok(Who::Platform)
            } else if p.dcs.contains(&self.dc) {
                Ok(Who::DcOperator)
            } else {
                Err(denied(&format!(
                    "{} doesn't operate dc {}",
                    p.audit_name(),
                    self.dc
                )))
            };
        }
        if self
            .gateway_token
            .as_deref()
            .is_some_and(|g| token_matches(headers, g))
        {
            return Ok(Who::Gateway);
        }
        if self
            .dc_token
            .as_deref()
            .is_some_and(|g| token_matches(headers, g))
        {
            return Ok(Who::DcOperator);
        }
        match &self.token {
            None => Ok(Who::Platform),
            Some(t) if token_matches(headers, t) => Ok(Who::Platform),
            Some(_) => Err(unauthenticated()),
        }
    }

    /// The caller: from the credential, or the gateway's claim. A claim
    /// is honoured on any credential (it can only narrow what the caller
    /// may do: it never carries operator rights), so deployments without
    /// tokens still check tenant users.
    fn caller(
        &self,
        headers: &HeaderMap,
        claim: Option<Caller>,
        actor: Option<&str>,
    ) -> Result<Caller> {
        let named = crate::operators::http::principal(headers)
            .ok()
            .and_then(|p| p.actor());
        let actor = named
            .as_deref()
            .or(actor.filter(|a| !a.is_empty()))
            .unwrap_or("admin");
        let who = self.who(headers)?;
        let header_claim = headers
            .get("x-sprig-caller")
            .and_then(|v| v.to_str().ok())
            .map(|h| {
                serde_json::from_str::<Caller>(h).map_err(|e| invalid(format!("caller claim: {e}")))
            })
            .transpose()?;
        if let Some(c) = claim.or(header_claim) {
            if c.platform || c.dc_operator.is_some() {
                return Err(denied("a claim can't carry operator rights"));
            }
            return Ok(c);
        }
        match who {
            Who::Platform => Ok(Caller::platform(format!("op:{actor}"))),
            Who::DcOperator => Ok(Caller::dc_operator(
                // A named DC operator is a person like any other operator.
                if named.is_some() {
                    format!("op:{actor}")
                } else {
                    format!("dc:{actor}")
                },
                self.dc.clone(),
            )),
            // The gateway credential always carries a claim (handled above).
            Who::Gateway => Err(invalid("the gateway credential needs a caller claim")),
        }
    }
}

/// The routes.
pub fn router(app: Arc<RecApp>) -> Router {
    Router::new()
        .route("/admin/v1/recommendations", get(list))
        .route("/admin/v1/recommendations:bulk", post(bulk))
        .route(
            "/admin/v1/recommendations:policy",
            get(policy).post(set_policy),
        )
        .route(
            "/admin/v1/recommendations:subscriptions",
            get(subscriptions).post(set_subscription),
        )
        .route("/admin/v1/recommendations:gate", post(gate))
        .route("/admin/v1/recommendations/{id}", get(get_one).post(act))
        .with_state(app)
}

fn claim(text: Option<&String>) -> Result<Option<Caller>> {
    text.map(|t| serde_json::from_str(t).map_err(|e| invalid(format!("caller claim: {e}"))))
        .transpose()
}

#[derive(Debug, Default, Deserialize)]
struct CallerQ {
    #[serde(default)]
    caller: Option<String>,
}

async fn list(State(app): State<Arc<RecApp>>, headers: HeaderMap, uri: Uri) -> Response {
    let r = async {
        let q: ListQuery = query(&uri)?;
        let c = app.caller(&headers, claim(q.caller.as_ref())?, None)?;
        app.svc.list(&c, &q).await
    };
    respond(r.await)
}

async fn get_one(
    State(app): State<Arc<RecApp>>,
    headers: HeaderMap,
    UrlPath(id): UrlPath<String>,
    uri: Uri,
) -> Response {
    let r = async {
        let q: CallerQ = query(&uri)?;
        let c = app.caller(&headers, claim(q.caller.as_ref())?, None)?;
        app.svc.get(&c, &id).await
    };
    respond(r.await)
}

async fn act(
    State(app): State<Arc<RecApp>>,
    headers: HeaderMap,
    UrlPath(id): UrlPath<String>,
    body: Bytes,
) -> Response {
    let r = async {
        let Some((id, action)) = id.rsplit_once(':') else {
            return Err(invalid(
                "POST /admin/v1/recommendations/{id}:approve, :ignore, :snooze, :reopen, :comment or :act",
            ));
        };
        let mut v: serde_json::Value = if body.iter().all(u8::is_ascii_whitespace) {
            serde_json::json!({})
        } else {
            serde_json::from_slice(&body).map_err(|e| invalid(format!("request body: {e}")))?
        };
        // `{id}:act` (gRPC's `ActOnRecommendation`) names the action in
        // the body; `{id}:approve` and the others in the path.
        if let serde_json::Value::Object(m) = &mut v
            && action != "act"
        {
            m.insert(
                "action".into(),
                serde_json::Value::String(action.to_owned()),
            );
        }
        let req: ActRequest =
            serde_json::from_value(v).map_err(|e| invalid(format!("request body: {e}")))?;
        let c = app.caller(&headers, req.caller.clone(), req.actor.as_deref())?;
        app.svc.act(&c, id, &req.act).await
    };
    respond(r.await)
}

/// A gateway's own decisions (`vector.ann`): the gateway credential, or
/// the admin token.
async fn gate(State(app): State<Arc<RecApp>>, headers: HeaderMap, body: Bytes) -> Response {
    let r = async {
        match app.who(&headers)? {
            Who::Gateway | Who::Platform => {}
            Who::DcOperator => return Err(denied("the gate is for gateways")),
        }
        let req: super::service::GateRequest =
            serde_json::from_slice(&body).map_err(|e| invalid(format!("request body: {e}")))?;
        app.svc.gate(req).await
    };
    respond(r.await)
}

async fn bulk(State(app): State<Arc<RecApp>>, headers: HeaderMap, body: Bytes) -> Response {
    let r = async {
        let req: BulkRequest =
            serde_json::from_slice(&body).map_err(|e| invalid(format!("request body: {e}")))?;
        let c = app.caller(&headers, req.caller.clone(), req.actor.as_deref())?;
        app.svc.bulk(&c, &req.ids, &req.act).await
    };
    respond(r.await)
}

async fn policy(State(app): State<Arc<RecApp>>, headers: HeaderMap, uri: Uri) -> Response {
    let r = async {
        let c = app.caller(&headers, None, None)?;
        let q: SettingContextQuery = query(&uri)?;
        if !c.platform && c.dc_operator.is_none() && q.tenant.is_none() {
            return Err(invalid("name a tenant"));
        }
        app.svc.policy(&q).await
    };
    respond(r.await)
}

/// `SetApprovalPolicy`'s body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SetPolicyRequest {
    key: String,
    scope: ScopeRef,
    value: String,
    #[serde(default)]
    reason: String,
    #[serde(default)]
    actor: Option<String>,
}

async fn set_policy(State(app): State<Arc<RecApp>>, headers: HeaderMap, body: Bytes) -> Response {
    let r = async {
        let req: SetPolicyRequest =
            serde_json::from_slice(&body).map_err(|e| invalid(format!("request body: {e}")))?;
        let who = app.who(&headers)?;
        let mode_key = sprig_approvals::Kind::ALL
            .iter()
            .any(|k| k.mode_key() == req.key);
        if !mode_key && !req.key.starts_with("approvals.") {
            return Err(invalid("only mode keys and approvals.* keys are set here"));
        }
        match (&who, &req.scope) {
            (Who::Platform, _) | (Who::DcOperator, ScopeRef::Dc(_)) => {}
            _ => {
                return Err(denied(
                    "policy writes need the admin token (or a DC operator's, for the dc scope); \
                     tenants use ALTER ... SPRIG SET",
                ));
            }
        }
        let admin = app
            .admin
            .as_ref()
            .ok_or_else(|| invalid("this controller doesn't serve settings"))?;
        admin
            .write_setting(
                &req.key,
                SettingWrite {
                    scope: req.scope,
                    op: SettingOp::Set {
                        value: None,
                        value_text: Some(req.value),
                    },
                    actor: req.actor.unwrap_or_else(|| "admin".into()),
                    reason: req.reason,
                },
            )
            .await
    };
    respond(r.await)
}

#[derive(Debug, Deserialize)]
struct TenantQ {
    tenant: String,
}

async fn subscriptions(State(app): State<Arc<RecApp>>, headers: HeaderMap, uri: Uri) -> Response {
    let r = async {
        let _ = app.caller(&headers, None, None)?;
        let q: TenantQ = query(&uri)?;
        app.svc.subscriptions(&q.tenant).await
    };
    respond(r.await)
}

#[derive(Debug, Deserialize)]
struct SetSub {
    tenant: String,
    #[serde(flatten)]
    sub: Subscription,
    #[serde(default)]
    caller: Option<Caller>,
}

async fn set_subscription(
    State(app): State<Arc<RecApp>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let req: SetSub =
            serde_json::from_slice(&body).map_err(|e| invalid(format!("request body: {e}")))?;
        let c = app.caller(&headers, req.caller.clone(), None)?;
        // A tenant user sets only their own address; operators any.
        if !c.platform && !c.name.ends_with(&format!(".{}", req.sub.user)) {
            return Err(denied("a user sets only their own subscription"));
        }
        app.svc.set_subscription(&req.tenant, &req.sub).await?;
        Ok(serde_json::json!({"ok": true}))
    };
    respond(r.await)
}

#[allow(dead_code)]
fn _act_is_serde(a: &Act) -> String {
    a.name().to_owned()
}
