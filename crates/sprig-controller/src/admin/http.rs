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
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::response::Response;
use axum::routing::{get, post};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sprig_common::{CancellationToken, ErrorKind, Result, SprigCode, SprigError};
use tokio::net::TcpListener;

use super::types::{
    ErrorBody, LockSettingRequest, ProbeClusterRequest, ResetSettingRequest, ScopeRef,
    SetBoundsRequest, SetClusterNodesRequest, SetSettingRequest, SettingOp, SettingWrite,
};
use super::{AdminService, invalid};
use crate::autoindex_admin::view::{Action as IndexAction, IndexActionRequest};

/// The routes' shared state.
struct App {
    svc: Arc<AdminService>,
    token: Option<String>,
    /// `[controller] gateway_token_file`: accepted on the tenant routes
    /// gateways call on their own behalf ([`authorize_gateway`]).
    gateway_token: Option<String>,
    /// Behind the operators' guard (chapter 14, "Operators"): the caller
    /// comes in its header instead of the tokens above.
    guarded: bool,
}

/// The `/admin/v1/` routes over `svc`; with `token`, every call must
/// present it as a bearer token.
pub fn router(svc: Arc<AdminService>, token: Option<String>) -> Router {
    router_with(svc, token, None)
}

/// [`router`], also accepting `gateway_token` on the tenant routes gateways
/// call for Studio (decision ST12).
pub fn router_with(
    svc: Arc<AdminService>,
    token: Option<String>,
    gateway_token: Option<String>,
) -> Router {
    routes(Arc::new(App {
        svc,
        token,
        gateway_token,
        guarded: false,
    }))
}

/// The routes behind [`crate::operators::guard::layer`], which
/// authenticates every call and hands the caller over in its header.
pub fn router_guarded(svc: Arc<AdminService>) -> Router {
    routes(Arc::new(App {
        svc,
        token: None,
        gateway_token: None,
        guarded: true,
    }))
}

fn routes(app: Arc<App>) -> Router {
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
        .route("/admin/v1/dcs/{dc}", get(get_dc).post(dc_method))
        .route("/admin/v1/tenants", get(list_tenants).post(create_tenant))
        .route(
            "/admin/v1/tenants/{name}",
            get(get_tenant).delete(delete_tenant).post(tenant_method),
        )
        .route("/admin/v1/tenants/{name}/move", get(tenant_move))
        .route("/admin/v1/tenants/{name}/residency", get(tenant_residency))
        .route("/admin/v1/tenants/{name}/failovers", get(tenant_failovers))
        .route("/admin/v1/failovers", get(list_failovers))
        .route("/admin/v1/dcs:latency", get(latency_matrix))
        .route("/admin/v1/failovers:all", post(failover_all))
        .route("/admin/v1/tenants/{name}/traffic", get(tenant_traffic))
        .route("/admin/v1/traffic/reports", post(traffic_report))
        .route("/admin/v1/tenants/{name}/settings", get(tenant_settings))
        .route("/admin/v1/tenants/{name}/users", post(create_user))
        .route(
            "/admin/v1/tenants/{name}/accounts",
            get(export_accounts).post(import_accounts),
        )
        .route("/admin/v1/tenants/{name}/indexes", get(list_indexes))
        .route(
            "/admin/v1/tenants/{name}/indexes/{index}",
            get(get_index).post(index_method),
        )
        .route(
            "/admin/v1/tenants/{name}/recommendations",
            get(list_recommendations),
        )
        .route(
            "/admin/v1/tenants/{name}/queries/{fingerprint}",
            get(explain_query),
        )
        .route("/admin/v1/mover/jobs", get(list_moves).post(submit_move))
        .route("/admin/v1/mover/jobs/{id}", get(get_move).post(move_method))
        .route("/admin/v1/jobs", get(list_jobs))
        .route("/admin/v1/jobs/{id}", get(get_job).post(job_method))
        .route("/admin/v1/tenants/{name}/placement", get(explain_placement))
        .route(
            "/admin/v1/tenants/{name}/shards",
            get(list_shards).post(submit_shard_job),
        )
        .route("/admin/v1/tenants/{name}/graphs", get(list_graphs))
        .route("/admin/v1/tenants/{name}/graphs/{graph}", get(show_graph))
        .route(
            "/admin/v1/tenants/{name}/graphs/{graph}/partition",
            get(graph_partition),
        )
        .route("/admin/v1/tenants/{name}/advisor", get(advisor_show))
        .route("/admin/v1/tenants/{name}/advisor:run", post(advisor_run))
        .route("/admin/v1/tenants/{name}/advisor:apply", post(advisor_apply))
        .route(
            "/admin/v1/tenants/{name}/pin",
            post(pin_tenant_cluster).delete(unpin_tenant_cluster),
        )
        .route(
            "/admin/v1/tenants/{name}/decisions/{id}",
            get(explain_decision),
        )
        // Chapter 18: in-doubt distributed transactions.
        .route("/admin/v1/txns", get(list_txns))
        .route("/admin/v1/txns/{dtid}", get(show_txn).post(txn_method))
        .route("/admin/v1/settings", get(list_setting_keys))
        .route(
            "/admin/v1/settings/{key}",
            get(get_setting)
                .put(set_setting)
                .delete(unset_setting)
                .post(setting_method),
        )
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

pub(crate) fn error_response(e: &SprigError) -> Response {
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

pub(crate) fn respond<T: Serialize>(r: Result<T>) -> Response {
    match r {
        Ok(v) => json_response(StatusCode::OK, &v),
        Err(e) => error_response(&e),
    }
}

/// Constant-time comparison of a presented token with `want`.
pub(crate) fn token_matches(headers: &HeaderMap, want: &str) -> bool {
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
    diff == 0
}

pub(crate) fn unauthenticated() -> SprigError {
    SprigError::new(
        SprigCode::AdminUnauthenticated,
        "a valid bearer token is required",
    )
}

/// The Admin token, when one is configured; behind the guard, any caller
/// but a gateway's service identity (the guard checked roles and scopes).
fn authorize(app: &App, headers: &HeaderMap) -> Result<()> {
    if app.guarded {
        let p = crate::operators::http::principal(headers)?;
        if p.is_gateway() {
            return Err(crate::operators::forbidden(format!(
                "service identity {} reaches only the routes gateways call",
                p.name
            )));
        }
        return Ok(());
    }
    match &app.token {
        None => Ok(()),
        Some(want) if token_matches(headers, want) => Ok(()),
        Some(_) => Err(unauthenticated()),
    }
}

/// The Admin token, or the gateways' internal credential (decision ST12):
/// the tenant routes Studio's views read and act through. A gateway calls
/// them only for the caller's own tenant, after checking the caller's
/// privileges, and records the caller as the actor.
fn authorize_gateway(app: &App, headers: &HeaderMap) -> Result<()> {
    if app.guarded {
        return crate::operators::http::principal(headers).map(|_| ());
    }
    if app
        .gateway_token
        .as_deref()
        .is_some_and(|g| token_matches(headers, g))
    {
        return Ok(());
    }
    authorize(app, headers)
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

/// `{cluster_id}:probe`, `:drain`, `:retire`, `:setNodes`, `:extract` and
/// `:rebuild`.
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
        Some((id, "retire")) => respond(app.svc.retire_cluster(id).await),
        Some((id, "setNodes")) => {
            let r = async {
                let req: SetClusterNodesRequest = parse(&body, false)?;
                app.svc.set_cluster_nodes(id, req).await
            };
            respond(r.await)
        }
        Some((id, "extract")) => {
            let r = async {
                let req: super::recovery::ExtractNodeRequest = parse(&body, false)?;
                app.svc.extract_node(id, req).await
            };
            respond(r.await)
        }
        Some((id, "rebuild")) => {
            let r = async {
                let req: super::recovery::RebuildNodeRequest = parse(&body, false)?;
                app.svc.rebuild_node(id, req).await
            };
            respond(r.await)
        }
        _ => error_response(&invalid(format!(
            "POST /admin/v1/clusters/{name}: expected {{cluster_id}}:probe, :drain, :retire, \
             :setNodes, :extract or :rebuild"
        ))),
    }
}

/// `GET /admin/v1/jobs?state=&kind=`.
#[derive(Debug, Default, serde::Deserialize)]
struct JobsQuery {
    state: Option<String>,
    kind: Option<String>,
}

async fn list_jobs(State(app): State<Arc<App>>, headers: HeaderMap, uri: Uri) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        let q: JobsQuery = query(&uri)?;
        app.svc
            .list_jobs(q.state.as_deref(), q.kind.as_deref())
            .await
    };
    respond(r.await)
}

async fn get_job(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(id): UrlPath<String>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.get_job(&id).await
    };
    respond(r.await)
}

/// `{job_id}:cancel`.
async fn job_method(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    if let Err(e) = authorize(&app, &headers) {
        return error_response(&e);
    }
    match name.rsplit_once(':') {
        Some((id, "cancel")) => respond(app.svc.cancel_job(id).await),
        _ => error_response(&invalid(format!(
            "POST /admin/v1/jobs/{name}: expected {{job_id}}:cancel"
        ))),
    }
}

/// `?tenant=` of `GET /admin/v1/mover/jobs`.
#[derive(Debug, Default, serde::Deserialize)]
struct MovesQuery {
    tenant: Option<String>,
}

async fn list_moves(State(app): State<Arc<App>>, headers: HeaderMap, uri: Uri) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        let q: MovesQuery = query(&uri)?;
        app.svc.list_moves(q.tenant.as_deref()).await
    };
    respond(r.await)
}

async fn submit_move(State(app): State<Arc<App>>, headers: HeaderMap, body: Bytes) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        let req: super::mover::MoveRequest = parse_required(&body)?;
        app.svc.submit_move(req).await
    };
    respond(r.await)
}

async fn get_move(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(id): UrlPath<String>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.get_move(&id).await
    };
    respond(r.await)
}

/// `{job_id}:cancel` and `{job_id}:cutover`.
async fn move_method(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    if let Err(e) = authorize(&app, &headers) {
        return error_response(&e);
    }
    match name.rsplit_once(':') {
        Some((id, "cancel")) => respond(app.svc.cancel_move(id).await),
        Some((id, "cutover")) => respond(app.svc.cutover_move(id).await),
        _ => error_response(&invalid(format!(
            "POST /admin/v1/mover/jobs/{name}: expected {{job_id}}:cancel or :cutover"
        ))),
    }
}

/// `{name}:move`, `{name}:pin` and `{name}:unpin` (chapter 24).
async fn tenant_method(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
    body: Bytes,
) -> Response {
    if let Err(e) = authorize(&app, &headers) {
        return error_response(&e);
    }
    match name.rsplit_once(':') {
        Some((t, "move")) => {
            let r = async {
                let req: crate::migrate::MoveTenantRequest = parse_required(&body)?;
                app.svc.move_tenant(t, req).await
            };
            respond(r.await)
        }
        Some((t, "failover")) => {
            let r = async {
                let req: crate::failover::manual::FailoverRequest = parse_required(&body)?;
                app.svc.failover_tenant(t, req).await
            };
            respond(r.await)
        }
        Some((t, "standby")) => {
            let r = async {
                let req: crate::failover::manual::StandbyRequest = parse_required(&body)?;
                app.svc.add_standby(t, req).await
            };
            respond(r.await)
        }
        Some((t, "residency")) => {
            let r = async {
                let req: crate::residency::ResidencyRequest = parse_required(&body)?;
                app.svc.set_residency(t, req).await
            };
            respond(r.await)
        }
        Some((t, "pin")) => respond(app.svc.pin_tenant_dc(t, true).await),
        Some((t, "unpin")) => respond(app.svc.pin_tenant_dc(t, false).await),
        _ => error_response(&invalid(format!(
            "POST /admin/v1/tenants/{name}: expected {{name}}:move, :failover, :standby, :residency, :pin or :unpin"
        ))),
    }
}

async fn dc_method(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(dc): UrlPath<String>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        match dc.rsplit_once(':') {
            Some((d, "drain")) => serde_json::to_value(app.svc.set_dc_draining(d, true).await?)
                .map_err(|e| invalid(e.to_string())),
            Some((d, "undrain")) => serde_json::to_value(app.svc.set_dc_draining(d, false).await?)
                .map_err(|e| invalid(e.to_string())),
            _ => Err(invalid(format!(
                "POST /admin/v1/dcs/{dc}: expected {{dc}}:drain or :undrain"
            ))),
        }
    };
    respond(r.await)
}

async fn latency_matrix(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.latency_matrix().await
    };
    respond(r.await)
}

async fn tenant_residency(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.get_residency(&name).await
    };
    respond(r.await)
}

async fn tenant_failovers(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.tenant_failovers(&name).await
    };
    respond(r.await)
}

async fn list_failovers(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.failovers().await
    };
    respond(r.await)
}

async fn failover_all(State(app): State<Arc<App>>, headers: HeaderMap, body: Bytes) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.failover_all(parse_required(&body)?).await
    };
    respond(r.await)
}

async fn tenant_move(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.tenant_move(&name).await
    };
    respond(r.await)
}

async fn tenant_traffic(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.tenant_traffic(&name).await
    };
    respond(r.await)
}

/// Gateways' traffic counts (chapter 24). No bearer token: counts only, on
/// the controller's private Admin port (gateways hold no admin token).
async fn traffic_report(State(app): State<Arc<App>>, body: Bytes) -> Response {
    let r = parse_required::<crate::traffic::TrafficBatch>(&body)
        .and_then(|b| app.svc.traffic_report(&b));
    respond(r)
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

async fn list_tenants(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.list_tenants().await
    };
    respond(r.await)
}

async fn create_tenant(State(app): State<Arc<App>>, headers: HeaderMap, body: Bytes) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.create_tenant(parse_required(&body)?).await
    };
    respond(r.await)
}

async fn get_tenant(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.get_tenant(&name).await
    };
    respond(r.await)
}

async fn delete_tenant(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.delete_tenant(&name).await
    };
    respond(r.await)
}

async fn create_user(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
    body: Bytes,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.create_user(&name, parse_required(&body)?).await
    };
    respond(r.await)
}

async fn export_accounts(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.export_accounts(&name).await
    };
    respond(r.await)
}

async fn import_accounts(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
    body: Bytes,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.import_accounts(&name, parse_required(&body)?).await
    };
    respond(r.await)
}

async fn tenant_settings(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.tenant_settings(&name).await
    };
    respond(r.await)
}

/// `%XX` and `+` decoded.
fn percent_decode(s: &str) -> Result<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' => {
                let hex = b
                    .get(i + 1..i + 3)
                    .and_then(|h| std::str::from_utf8(h).ok())
                    .and_then(|h| u8::from_str_radix(h, 16).ok())
                    .ok_or_else(|| invalid(format!("query string: bad escape in {s:?}")))?;
                out.push(hex);
                i += 2;
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8(out).map_err(|_| invalid("query string: not UTF-8"))
}

/// The query string as `T`: each `name=value` a string field; empty values
/// are left out. 39792 when it doesn't fit.
pub(crate) fn query<T: DeserializeOwned>(uri: &Uri) -> Result<T> {
    let mut m = serde_json::Map::new();
    for part in uri.query().unwrap_or_default().split('&') {
        if part.is_empty() {
            continue;
        }
        let (k, v) = part.split_once('=').unwrap_or((part, ""));
        let v = percent_decode(v)?;
        if !v.is_empty() {
            m.insert(percent_decode(k)?, serde_json::Value::String(v));
        }
    }
    serde_json::from_value(serde_json::Value::Object(m))
        .map_err(|e| invalid(format!("query string: {e}")))
}

async fn list_setting_keys(State(app): State<Arc<App>>, headers: HeaderMap, uri: Uri) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.list_setting_keys(&query(&uri)?).await
    };
    respond(r.await)
}

/// `{key}` and `{key}:explain`: the same answer.
async fn get_setting(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(key): UrlPath<String>,
    uri: Uri,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        let key = match key.rsplit_once(':') {
            Some((k, "explain")) => k,
            Some(_) => {
                return Err(invalid(format!(
                    "GET /admin/v1/settings/{key}: expected {{key}} or {{key}}:explain"
                )));
            }
            None => key.as_str(),
        };
        app.svc.explain_setting(key, &query(&uri)?).await
    };
    respond(r.await)
}

fn scope_of(scope: Option<ScopeRef>) -> Result<ScopeRef> {
    scope.ok_or_else(|| {
        invalid("scope is required: global, dc[:<id>], cluster:<id>, tenant:<name>, db:<tenant>.<db> or object:<tenant>.<db>.<object>")
    })
}

fn write(
    scope: Option<ScopeRef>,
    op: SettingOp,
    actor: Option<String>,
    reason: String,
) -> Result<SettingWrite> {
    Ok(SettingWrite {
        scope: scope_of(scope)?,
        op,
        actor: actor.unwrap_or_default(),
        reason,
    })
}

async fn set_setting(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(key): UrlPath<String>,
    body: Bytes,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        let req: SetSettingRequest = parse_required(&body)?;
        let w = write(
            req.scope,
            SettingOp::Set {
                value: req.value,
                value_text: req.value_text,
            },
            req.actor,
            req.reason,
        )?;
        app.svc.write_setting(&key, w).await
    };
    respond(r.await)
}

/// `DELETE /admin/v1/settings/{key}?scope=...`: `Settings.UnsetSetting`,
/// the value's reset.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct UnsetQuery {
    scope: Option<ScopeRef>,
    #[serde(default)]
    reason: String,
    actor: Option<String>,
}

async fn unset_setting(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(key): UrlPath<String>,
    uri: Uri,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        let q: UnsetQuery = query(&uri)?;
        let w = write(
            q.scope,
            SettingOp::Reset {
                value: true,
                bounds: false,
            },
            q.actor,
            q.reason,
        )?;
        app.svc.write_setting(&key, w).await
    };
    respond(r.await)
}

/// `{key}:reset`, `{key}:bounds`, `{key}:lock` and `{key}:unlock`.
async fn setting_method(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
    body: Bytes,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        let (key, w) = match name.rsplit_once(':') {
            Some((key, "reset")) => {
                let req: ResetSettingRequest = parse_required(&body)?;
                let op = SettingOp::Reset {
                    value: req.all || !req.bounds,
                    bounds: req.all || req.bounds,
                };
                (key, write(req.scope, op, req.actor, req.reason)?)
            }
            Some((key, "bounds")) => {
                let req: SetBoundsRequest = parse_required(&body)?;
                let op = SettingOp::Bounds {
                    min: req.min,
                    max: req.max,
                    allowed: req.allowed,
                    allowed_text: req.allowed_text,
                    clear: req.clear,
                };
                (key, write(req.scope, op, req.actor, req.reason)?)
            }
            Some((key, m @ ("lock" | "unlock"))) => {
                let req: LockSettingRequest = parse_required(&body)?;
                let op = SettingOp::Lock {
                    locked: m == "lock",
                };
                (key, write(req.scope, op, req.actor, req.reason)?)
            }
            _ => {
                return Err(invalid(format!(
                    "POST /admin/v1/settings/{name}: expected {{key}}:reset, :bounds, :lock or :unlock"
                )));
            }
        };
        app.svc.write_setting(key, w).await
    };
    respond(r.await)
}

async fn list_indexes(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
    uri: Uri,
) -> Response {
    let r = async {
        authorize_gateway(&app, &headers)?;
        app.svc.list_indexes(&name, query(&uri)?, false).await
    };
    respond(r.await)
}

async fn list_recommendations(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
    uri: Uri,
) -> Response {
    let r = async {
        authorize_gateway(&app, &headers)?;
        app.svc.list_indexes(&name, query(&uri)?, true).await
    };
    respond(r.await)
}

async fn get_index(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath((name, index)): UrlPath<(String, String)>,
) -> Response {
    let r = async {
        authorize_gateway(&app, &headers)?;
        app.svc.get_index(&name, &index).await
    };
    respond(r.await)
}

/// `{index}:approve`, `:reject`, `:drop`, `:pin`, `:unpin` (and chapter
/// 13's and 37's names `:apply`, `:dismiss`, `:freeze`, `:unfreeze`).
async fn index_method(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath((name, index)): UrlPath<(String, String)>,
    body: Bytes,
) -> Response {
    let r = async {
        authorize_gateway(&app, &headers)?;
        let Some((index, action)) = index
            .rsplit_once(':')
            .and_then(|(i, m)| Some((i, IndexAction::parse(m)?)))
        else {
            return Err(invalid(format!(
                "POST /admin/v1/tenants/{name}/indexes/{index}: expected {{index}}:approve, \
                 :reject, :drop, :pin or :unpin"
            )));
        };
        let req: IndexActionRequest = parse(&body, true)?;
        app.svc.index_action(&name, index, action, &req).await
    };
    respond(r.await)
}

/// `{fingerprint}:explain`.
async fn explain_query(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath((name, fp)): UrlPath<(String, String)>,
) -> Response {
    let r = async {
        authorize_gateway(&app, &headers)?;
        let Some((fp, "explain")) = fp.rsplit_once(':') else {
            return Err(invalid(format!(
                "GET /admin/v1/tenants/{name}/queries/{fp}: expected {{fingerprint}}:explain"
            )));
        };
        app.svc.explain_query(&name, fp).await
    };
    respond(r.await)
}

async fn pin_tenant_cluster(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
    body: Bytes,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        let req: super::placement_admin::PinRequest = if body.is_empty() {
            super::placement_admin::PinRequest::default()
        } else {
            parse_required(&body)?
        };
        app.svc.pin_tenant_cluster(&name, req).await
    };
    respond(r.await)
}

async fn unpin_tenant_cluster(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.unpin_tenant_cluster(&name).await
    };
    respond(r.await)
}

async fn explain_placement(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.explain_placement(&name).await
    };
    respond(r.await)
}

async fn explain_decision(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath((name, id)): UrlPath<(String, String)>,
) -> Response {
    let r = async {
        authorize_gateway(&app, &headers)?;
        let id: u64 = id
            .parse()
            .map_err(|_| invalid(format!("decision id {id:?} is not a number")))?;
        app.svc.explain_decision(&name, id).await
    };
    respond(r.await)
}

async fn list_graphs(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.list_graphs(&name).await
    };
    respond(r.await)
}

async fn show_graph(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath((name, graph)): UrlPath<(String, String)>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.show_graph(&name, &graph).await
    };
    respond(r.await)
}

async fn graph_partition(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath((name, graph)): UrlPath<(String, String)>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.graph_partition(&name, &graph).await
    };
    respond(r.await)
}

async fn list_shards(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    let r = async {
        // Studio's shards view reads maps over the gateway credential.
        authorize_gateway(&app, &headers)?;
        app.svc.list_shards(&name).await
    };
    respond(r.await)
}

async fn submit_shard_job(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
    body: Bytes,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        let req: super::sharding_admin::ShardJobRequest = parse_required(&body)?;
        app.svc.submit_shard_job(&name, req).await
    };
    respond(r.await)
}

/// `GET /admin/v1/tenants/{name}/advisor?run=`.
#[derive(Debug, Default, serde::Deserialize)]
struct AdvisorQuery {
    run: Option<String>,
}

async fn advisor_show(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
    uri: Uri,
) -> Response {
    let r = async {
        authorize_gateway(&app, &headers)?;
        let q: AdvisorQuery = query(&uri)?;
        let run = q
            .run
            .map(|r| {
                r.parse::<u64>()
                    .map_err(|_| invalid(format!("run {r:?} is not a number")))
            })
            .transpose()?;
        app.svc.advisor_show(&name, run).await
    };
    respond(r.await)
}

async fn advisor_run(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.advisor_run(&name).await
    };
    respond(r.await)
}

async fn advisor_apply(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
    body: Bytes,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        let req: super::sharding_admin::AdvisorApplyRequest = parse_required(&body)?;
        app.svc.advisor_apply(&name, req).await
    };
    respond(r.await)
}

/// `GET /admin/v1/txns?tenant=`.
#[derive(Debug, Default, serde::Deserialize)]
struct TxnsQuery {
    tenant: Option<String>,
}

async fn list_txns(State(app): State<Arc<App>>, headers: HeaderMap, uri: Uri) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        let q: TxnsQuery = query(&uri)?;
        app.svc.list_txns(q.tenant.as_deref()).await
    };
    respond(r.await)
}

async fn show_txn(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(dtid): UrlPath<String>,
) -> Response {
    let r = async {
        authorize(&app, &headers)?;
        app.svc.show_txn(&dtid).await
    };
    respond(r.await)
}

/// `{dtid}:resolve`.
async fn txn_method(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Response {
    if let Err(e) = authorize(&app, &headers) {
        return error_response(&e);
    }
    match name.rsplit_once(':') {
        Some((dtid, "resolve")) => respond(app.svc.resolve_txn(dtid).await),
        _ => error_response(&invalid(format!(
            "POST /admin/v1/txns/{name}: expected {{dtid}}:resolve"
        ))),
    }
}

async fn not_routed() -> Response {
    let mut r = error_response(&invalid("no such Admin API route"));
    *r.status_mut() = StatusCode::NOT_FOUND;
    r
}
