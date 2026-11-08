//! The Admin API's REST face over HTTP/1.1 (chapter 37, "REST
//! transcoding"): JSON bodies, errors as `{code, name, message,
//! trace_id}` with the HTTP status of the code (chapter 03's mapping), and
//! `authorization: Bearer <token>` when a token is configured.
//!
//! Custom methods follow the `google.api.http` convention:
//! `POST /admin/v1/clusters/{cluster_id}:probe`.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Path as UrlPath, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use axum::routing::get;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sprig_common::{CancellationToken, ErrorKind, Result, SprigCode, SprigError};
use tokio::net::TcpListener;

use super::types::{ErrorBody, ProbeClusterRequest};
use super::{AdminService, invalid};

/// The routes' shared state.
struct App {
    svc: Arc<AdminService>,
    token: Option<String>,
}

/// The `/admin/v1/` routes over `svc`; with `token`, every call must
/// present it as a bearer token.
pub fn router(svc: Arc<AdminService>, token: Option<String>) -> Router {
    let app = Arc::new(App { svc, token });
    Router::new()
        .route(
            "/admin/v1/clusters",
            get(list_clusters).post(register_cluster),
        )
        .route(
            "/admin/v1/clusters/{name}",
            get(get_cluster).post(cluster_method),
        )
        .route("/admin/v1/dcs", get(list_dcs).post(register_dc))
        .route("/admin/v1/dcs/{dc}", get(get_dc))
        .fallback(not_routed)
        .with_state(app)
}

/// Reads the bearer token from `path`.
///
/// # Errors
/// 39998 if the file can't be read or is empty.
pub fn load_token(path: &Path) -> Result<String> {
    let t = std::fs::read_to_string(path).map_err(|e| {
        SprigError::new(
            SprigCode::ConfigInvalid,
            format!("[controller] admin_token_file {}: {e}", path.display()),
        )
    })?;
    let t = t.trim().to_owned();
    if t.is_empty() {
        return Err(SprigError::new(
            SprigCode::ConfigInvalid,
            format!("[controller] admin_token_file {} is empty", path.display()),
        ));
    }
    Ok(t)
}

/// Binds `addr`. Without a token, only a loopback address is allowed: the
/// Admin API changes the fleet.
///
/// # Errors
/// 39998 for a public address without a token, or one that can't be bound.
pub async fn bind(addr: SocketAddr, token: Option<&str>) -> Result<TcpListener> {
    if token.is_none() && !addr.ip().is_loopback() {
        return Err(SprigError::new(
            SprigCode::ConfigInvalid,
            format!(
                "[controller] admin_addr {addr} isn't a loopback address: set admin_token_file"
            ),
        ));
    }
    TcpListener::bind(addr).await.map_err(|e| {
        SprigError::new(
            SprigCode::ConfigInvalid,
            format!("can't bind the Admin API on {addr}: {e}"),
        )
    })
}

/// Serves until `shutdown`.
///
/// # Errors
/// Internal if the server fails.
pub async fn serve(
    listener: TcpListener,
    router: Router,
    shutdown: CancellationToken,
) -> Result<()> {
    axum::serve(listener, router)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
        .map_err(|e| SprigError::internal("the Admin API server failed").with_source(e))
}

fn json_response<T: Serialize>(status: StatusCode, body: &T) -> Response {
    let bytes = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(bytes))
        .unwrap_or_default()
}

fn error_response(e: &SprigError) -> Response {
    let p = e.protocol();
    let name = match e.kind {
        ErrorKind::Sprig(code) => code.name().to_owned(),
        ErrorKind::Backend { .. } | ErrorKind::Standard { .. } => String::new(),
    };
    let trace_id = uuid::Uuid::now_v7().simple().to_string();
    if p.http_status >= 500 {
        tracing::warn!(trace_id, error = %e, source = ?std::error::Error::source(e), "admin call failed");
    } else {
        tracing::debug!(trace_id, error = %e, "admin call refused");
    }
    let status = StatusCode::from_u16(p.http_status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    json_response(
        status,
        &ErrorBody {
            code: p.mysql_errno,
            name,
            message: e.message.clone(),
            trace_id,
        },
    )
}

fn respond<T: Serialize>(r: Result<T>) -> Response {
    match r {
        Ok(v) => json_response(StatusCode::OK, &v),
        Err(e) => error_response(&e),
    }
}

/// Constant-time comparison of the presented token.
fn authorize(app: &App, headers: &HeaderMap) -> Result<()> {
    let Some(want) = &app.token else {
        return Ok(());
    };
    let got = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or_default();
    let (a, b) = (got.as_bytes(), want.as_bytes());
    let mut diff = u8::from(a.len() != b.len());
    for (i, x) in b.iter().enumerate() {
        diff |= x ^ a.get(i).copied().unwrap_or(0);
    }
    if diff == 0 {
        Ok(())
    } else {
        Err(SprigError::new(
            SprigCode::AdminUnauthenticated,
            "a valid bearer token is required",
        ))
    }
}

fn parse<T: DeserializeOwned + Default>(body: &Bytes, allow_empty: bool) -> Result<T> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return if allow_empty {
            Ok(T::default())
        } else {
            Err(invalid("a JSON body is required"))
        };
    }
    serde_json::from_slice(body).map_err(|e| invalid(format!("request body: {e}")))
}

fn parse_required<T: DeserializeOwned>(body: &Bytes) -> Result<T> {
    serde_json::from_slice(body).map_err(|e| invalid(format!("request body: {e}")))
}

async fn list_clusters(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    let r = match authorize(&app, &headers) {
        Ok(()) => app.svc.list_clusters().await,
        Err(e) => Err(e),
    };
    respond(r)
}

async fn register_cluster(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.register_cluster(parse_required(&body)?).await
    };
    respond(r.await)
}

async fn get_cluster(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.get_cluster(&name).await
    };
    respond(r.await)
}

/// `{cluster_id}:probe` and `{cluster_id}:drain`.
async fn cluster_method(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
    body: Bytes,
) -> Response {
    if let Err(e) = authorize(&app, &headers) {
        return error_response(&e);
    }
    match name.rsplit_once(':') {
        Some((id, "probe")) => {
            let r = async {
                let req: ProbeClusterRequest = parse(&body, true)?;
                app.svc.probe_cluster(id, req).await
            };
            respond(r.await)
        }
        Some((id, "drain")) => respond(app.svc.drain_cluster(id).await),
        _ => error_response(&invalid(format!(
            "POST /admin/v1/clusters/{name}: expected {{cluster_id}}:probe or {{cluster_id}}:drain"
        ))),
    }
}

async fn list_dcs(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.list_dcs().await
    };
    respond(r.await)
}

async fn register_dc(State(app): State<Arc<App>>, headers: HeaderMap, body: Bytes) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.register_dc(parse_required(&body)?).await
    };
    respond(r.await)
}

async fn get_dc(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(dc): UrlPath<String>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.get_dc(&dc).await
    };
    respond(r.await)
}

async fn not_routed() -> Response {
    let mut r = error_response(&invalid("no such Admin API route"));
    *r.status_mut() = StatusCode::NOT_FOUND;
    r
}
