//! The `Operators` service's REST routes (chapter 14, "Operators"),
//! merged into the Admin API behind the [`super::guard`] layer.
//!
//! | Call | Route |
//! |---|---|
//! | `Login`, `Enrol` | `POST /admin/v1/operators:login`, `:enrol` (no credential) |
//! | `Logout`, `WhoAmI` | `POST /admin/v1/operators:logout`, `GET /admin/v1/operators:whoami` |
//! | `CreateOperator`, `ListOperators` | `POST`, `GET /admin/v1/operators` |
//! | `GetOperator` | `GET /admin/v1/operators/{name}` |
//! | `DisableOperator`, `EnableOperator`, `ResetOperatorFactor`, `SetOperatorPassword` | `POST /admin/v1/operators/{name}:disable`, `:enable`, `:resetFactor`, `:setPassword` |
//! | `CreateToken`, `ListTokens` | `POST`, `GET /admin/v1/operator-tokens[?owner=]` |
//! | `RevokeToken` | `POST /admin/v1/operator-tokens/{key_id}:revoke` |

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path as UrlPath, State};
use axum::http::{HeaderMap, Uri};
use axum::response::Response;
use axum::routing::{get, post};
use serde::Deserialize;
use sprig_common::Result;

use super::guard::{AUTHN_HEADER, CLIENT_HEADER};
use super::{Operators, Principal};
use crate::admin::http::{query, respond, unauthenticated};
use crate::admin::invalid;

/// The caller the guard established.
///
/// # Errors
/// 39793 when the guard isn't in front (no principal).
pub fn principal(headers: &HeaderMap) -> Result<Principal> {
    headers
        .get(AUTHN_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| serde_json::from_str(v).ok())
        .ok_or_else(unauthenticated)
}

fn body<T: for<'de> Deserialize<'de>>(b: &Bytes) -> Result<T> {
    serde_json::from_slice(b).map_err(|e| invalid(format!("request body: {e}")))
}

/// The routes.
pub fn router(ops: Arc<Operators>) -> Router {
    Router::new()
        .route("/admin/v1/operators", get(list).post(create))
        .route("/admin/v1/operators/{name}", get(show).post(method))
        .route(
            "/admin/v1/operator-tokens",
            get(list_tokens).post(create_token),
        )
        .route("/admin/v1/operator-tokens/{id}", post(token_method))
        .route("/admin/v1/operators:login", post(login))
        .route("/admin/v1/operators:enrol", post(enrol))
        .route("/admin/v1/operators:logout", post(logout))
        .route("/admin/v1/operators:whoami", get(whoami))
        .with_state(ops)
}

async fn list(State(ops): State<Arc<Operators>>, headers: HeaderMap) -> Response {
    let r = async {
        let p = principal(&headers)?;
        if !p.platform || p.is_gateway() {
            return Err(super::forbidden("platform operators list operators"));
        }
        ops.list().await
    };
    respond(r.await)
}

async fn create(State(ops): State<Arc<Operators>>, headers: HeaderMap, b: Bytes) -> Response {
    let r = async {
        let p = principal(&headers)?;
        ops.create(&p, body(&b)?).await
    };
    respond(r.await)
}

#[derive(Debug, Default, Deserialize)]
struct Reason {
    #[serde(default)]
    reason: String,
}

async fn show(
    State(ops): State<Arc<Operators>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    let r = async {
        let p = principal(&headers)?;
        ops.show(&p, &name).await
    };
    respond(r.await)
}

async fn method(
    State(ops): State<Arc<Operators>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
    b: Bytes,
) -> Response {
    let r = async {
        let p = principal(&headers)?;
        let Some((who, m)) = name.rsplit_once(':') else {
            return Err(invalid(
                "POST /admin/v1/operators/{name}:disable, :enable, :resetFactor or :setPassword",
            ));
        };
        let reason = || -> Result<String> {
            Ok(if b.iter().all(u8::is_ascii_whitespace) {
                String::new()
            } else {
                body::<Reason>(&b)?.reason
            })
        };
        match m {
            "disable" => ops.set_state(&p, who, true, &reason()?).await,
            "enable" => ops.set_state(&p, who, false, &reason()?).await,
            "resetFactor" => ops.reset_factor(&p, who, &reason()?).await,
            "setPassword" => ops.set_password(&p, who, body(&b)?).await,
            _ => Err(invalid(format!(
                "POST /admin/v1/operators/{name}: expected :disable, :enable, :resetFactor or :setPassword"
            ))),
        }
    };
    respond(r.await)
}

#[derive(Debug, Default, Deserialize)]
struct OwnerQ {
    owner: Option<String>,
}

async fn list_tokens(State(ops): State<Arc<Operators>>, headers: HeaderMap, uri: Uri) -> Response {
    let r = async {
        let p = principal(&headers)?;
        let q: OwnerQ = query(&uri)?;
        ops.list_tokens(&p, q.owner.as_deref()).await
    };
    respond(r.await)
}

async fn create_token(State(ops): State<Arc<Operators>>, headers: HeaderMap, b: Bytes) -> Response {
    let r = async {
        let p = principal(&headers)?;
        ops.create_token(&p, body(&b)?).await
    };
    respond(r.await)
}

async fn token_method(
    State(ops): State<Arc<Operators>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    let r = async {
        let p = principal(&headers)?;
        match name.rsplit_once(':') {
            Some((id, "revoke")) => ops.revoke_token(&p, id).await,
            _ => Err(invalid(format!(
                "POST /admin/v1/operator-tokens/{name}: expected {{key_id}}:revoke"
            ))),
        }
    };
    respond(r.await)
}

/// The client's address the guard established (ST47).
fn client_addr(headers: &HeaderMap) -> Option<String> {
    headers
        .get(CLIENT_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

async fn login(State(ops): State<Arc<Operators>>, headers: HeaderMap, b: Bytes) -> Response {
    let addr = client_addr(&headers);
    let r = async { ops.login_from(body(&b)?, addr.as_deref()).await };
    respond(r.await)
}

async fn enrol(State(ops): State<Arc<Operators>>, headers: HeaderMap, b: Bytes) -> Response {
    let addr = client_addr(&headers);
    let r = async { ops.enrol_from(body(&b)?, addr.as_deref()).await };
    respond(r.await)
}

async fn logout(State(ops): State<Arc<Operators>>, headers: HeaderMap) -> Response {
    let r = async {
        let p = principal(&headers)?;
        ops.logout(&p).await
    };
    respond(r.await)
}

async fn whoami(State(ops): State<Arc<Operators>>, headers: HeaderMap) -> Response {
    let r = async {
        let p = principal(&headers)?;
        ops.whoami(&p).await
    };
    respond(r.await)
}
