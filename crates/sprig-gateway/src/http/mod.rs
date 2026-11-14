//! HTTP on 8080 and 8443: REST and gRPC on one port.
//!
//! ```text
//! serve(listener):                        -- listener "http" (8080) or "https" (8443)
//!  1. accept a TCP connection; admission check
//!  2. https: TLS handshake, ALPN offers h2 and http/1.1; certificate from the live store
//!  3. hand the connection to hyper's auto server: HTTP/1.1, or HTTP/2 when the client
//!     sends the h2 preface (h2c with prior knowledge on 8080, ALPN h2 on 8443)
//!  4. per request:
//!       content-type starts with "application/grpc":
//!           HTTP/1.1 → 415 with 39791 (gRPC needs HTTP/2)
//!           else      → tonic router (data, admin, grpc.health.v1)
//!       otherwise → the REST router (/v1/, /admin/v1/, openapi.json)
//! ```
//!
//! Steps 1 and 2 are the listener manager's ([`crate::listener`]): the
//! front end receives a [`ClientConn`] with TLS done (ALPN set on HTTP
//! listeners). A listener change the connection no longer meets, or a
//! drain, asks it to close at its next safe point
//! ([`ClientConn::close_soon`]): HTTP/2 gets `GOAWAY` at once and its
//! streams finish; HTTP/1.1 gets `connection: close` on its next response,
//! or is closed after a second idle ([`serve_conn`]). No request the
//! server has read fails; the forced close follows only if a request
//! outlives the drain time. A request the client sends while the `GOAWAY`
//! is on its way never reaches the server, and HTTP/2 clients retry it
//! (tonic does not on its own).
//!
//! The services: [`sql`] (SQL and transaction handles through the
//! executor), [`admin`] (the Admin API, forwarded to the controllers, and
//! `Gateways`), [`grpc`] (the tonic services over both), and REST in
//! [`rest`]. Authentication is [`auth`]'s.

pub mod admin;
pub mod analytics;
pub mod auth;
// Change streams (gRPC, SSE, WebSocket, NDJSON).
pub mod changes;
pub mod functions;
pub mod grpc;
pub mod kv;
/// `language` on `Sql.Query`: Cypher, GQL and MongoDB commands.
pub mod lang;
/// Live `vector.*` and `analytics.*` settings.
pub mod model_settings;
pub mod rest;
// Search.Query.
pub mod search;
pub mod settings;
pub mod sink;
pub mod sql;
pub mod studio;
pub mod vectors;

#[cfg(test)]
mod drain_tests;

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use hyper::body::Incoming;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use sprig_common::Result;
use tonic::body::Body;

use self::admin::AdminApi;
use self::auth::{HttpAuthenticator, HttpCredentials};
use self::sql::{Caller, SqlEngine};
use crate::frontend::{ClientConn, Frontend, GatewayContext, MaybeTls, Protocol, TlsInfo};

/// What a request knows of its connection, in the request's extensions.
#[derive(Debug, Clone)]
pub struct ConnInfo {
    /// The client address.
    pub peer: SocketAddr,
    /// TLS parameters; `None` for plaintext.
    pub tls: Option<TlsInfo>,
    /// The listener.
    pub listener: &'static str,
    /// Its generation at accept.
    pub generation: u64,
}

impl ConnInfo {
    /// What authentication sees of a request with these headers.
    #[must_use]
    pub fn credentials(
        &self,
        authorization: Option<String>,
        tenant: Option<String>,
        host: Option<String>,
    ) -> HttpCredentials {
        HttpCredentials {
            authorization,
            tenant_header: tenant,
            host,
            sni: self.tls.as_ref().and_then(|t| t.sni.clone()),
            client_ip: self.peer.ip(),
            tls: self.tls.is_some(),
            client_cert: self.tls.as_ref().is_some_and(|t| t.client_cert),
            client_dn: self
                .tls
                .as_ref()
                .filter(|t| t.client_cert)
                .and_then(|t| t.client_dn.clone()),
        }
    }
}

/// What every request handler shares.
pub struct Core {
    /// SQL and transactions.
    pub engine: Arc<SqlEngine>,
    /// Authentication of data calls.
    pub auth: Arc<dyn HttpAuthenticator>,
    /// The Admin API.
    pub admin: Arc<AdminApi>,
    /// Studio: its files, browser sessions and API additions.
    pub studio: Arc<studio::StudioState>,
}

impl std::fmt::Debug for Core {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Core")
            .field("engine", &self.engine)
            .finish_non_exhaustive()
    }
}

impl Core {
    /// Authenticates a data call.
    ///
    /// # Errors
    /// The authenticator's error (1045, 39786, 39301).
    pub async fn caller(
        &self,
        info: &ConnInfo,
        authorization: Option<String>,
        tenant: Option<String>,
        host: Option<String>,
    ) -> Result<Caller> {
        // A Studio session token; anything else is Basic or an API key.
        if studio::is_session_token(authorization.as_deref()) {
            let token = studio::bearer(authorization.as_deref()).unwrap_or_default();
            let s = self
                .studio
                .verify(token, info.peer.ip(), info.tls.is_some())
                .await?;
            return Ok(Caller {
                principal: s.principal,
                peer: info.peer,
                tls: info.tls.clone(),
                listener: info.listener,
                generation: info.generation,
                authorization,
                client_dc: None,
            });
        }
        let creds = info.credentials(authorization.clone(), tenant, host);
        let principal = self.auth.authenticate(&creds).await?;
        Ok(Caller {
            principal,
            peer: info.peer,
            tls: info.tls.clone(),
            listener: info.listener,
            generation: info.generation,
            authorization,
            client_dc: None,
        })
    }
}

/// The HTTP front end for the `http` and `https` listeners.
pub struct HttpFrontend {
    core: Arc<Core>,
    grpc: tonic::service::Routes,
}

impl std::fmt::Debug for HttpFrontend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpFrontend").finish_non_exhaustive()
    }
}

impl HttpFrontend {
    /// The front end over `core`.
    #[must_use]
    pub fn new(core: Arc<Core>) -> Self {
        let grpc = grpc::routes(&core);
        Self { core, grpc }
    }

    /// The shared state.
    #[must_use]
    pub fn core(&self) -> &Arc<Core> {
        &self.core
    }
}

fn is_grpc(req: &Request<Incoming>) -> bool {
    req.headers()
        .get(hyper::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/grpc"))
}

async fn route(
    core: Arc<Core>,
    mut grpc: tonic::service::Routes,
    info: ConnInfo,
    mut req: Request<Incoming>,
) -> Response<Body> {
    let protocol = if is_grpc(&req) { "grpc" } else { "rest" };
    let started = std::time::Instant::now();
    let resp = if protocol == "grpc" {
        if req.version() < hyper::Version::HTTP_2 {
            rest::error_response(&sprig_common::SprigError::new(
                sprig_common::SprigCode::TransportUnsupported,
                "gRPC needs HTTP/2: use h2c with prior knowledge on 8080, or ALPN h2 on 8443",
            ))
        } else {
            req.extensions_mut().insert(info);
            match tonic::codegen::Service::call(&mut grpc, req).await {
                Ok(r) => r,
                Err(never) => match never {},
            }
        }
    } else {
        // Studio's files and API additions first.
        match studio::handle(&core, &info, req).await {
            Ok(r) => r,
            Err(req) => rest::handle(&core, &info, req).await,
        }
    };
    metrics::histogram!("sprig_api_request_duration_seconds", "protocol" => protocol)
        .record(started.elapsed().as_secs_f64());
    resp
}

#[async_trait]
impl Frontend for HttpFrontend {
    fn protocol(&self) -> Protocol {
        Protocol::Http
    }

    async fn serve(&self, conn: ClientConn, _ctx: Arc<GatewayContext>) -> Result<()> {
        let info = ConnInfo {
            peer: conn.peer,
            tls: conn.tls_info(),
            listener: conn.listener.name,
            generation: conn.generation,
        };
        let core = Arc::clone(&self.core);
        let grpc = self.grpc.clone();
        let svc = hyper::service::service_fn(move |req: Request<Incoming>| {
            let core = Arc::clone(&core);
            let grpc = grpc.clone();
            let info = info.clone();
            async move { Ok::<_, Infallible>(route(core, grpc, info, req).await) }
        });
        let streams = self.core.engine.settings().max_streams;
        serve_conn(conn, svc, streams).await
    }
}

/// How long an HTTP/1.1 connection asked to close waits for its next
/// request (answered with `connection: close`) before it is closed while
/// idle. A client in a loop sends within this time and learns of the close
/// from the response; closing an idle connection at once races the
/// client's next request, which then fails though the server never read
/// it.
const H1_IDLE_GRACE: std::time::Duration = std::time::Duration::from_secs(1);

/// What a connection's requests and its close share.
#[derive(Default)]
struct CloseState {
    /// The connection was asked to close: HTTP/1.1 responses carry
    /// `connection: close`.
    closing: std::sync::atomic::AtomicBool,
    /// The client speaks HTTP/2.
    h2: std::sync::atomic::AtomicBool,
    /// Wakes the close when an HTTP/2 request shows up after it began.
    h2_seen: tokio::sync::Notify,
}

/// `svc`, marking HTTP/1.1 responses `connection: close` once the
/// connection is closing, and noting HTTP/2.
#[derive(Clone)]
struct CloseAware<S> {
    inner: S,
    state: Arc<CloseState>,
}

impl<S> hyper::service::Service<Request<Incoming>> for CloseAware<S>
where
    S: hyper::service::Service<Request<Incoming>, Response = Response<Body>, Error = Infallible>,
    S::Future: Send + 'static,
{
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::result::Result<Response<Body>, Infallible>>
                + Send,
        >,
    >;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        use std::sync::atomic::Ordering;
        let h1 = req.version() < hyper::Version::HTTP_2;
        if !h1
            && !self.state.h2.swap(true, Ordering::SeqCst)
            && self.state.closing.load(Ordering::SeqCst)
        {
            self.state.h2_seen.notify_one();
        }
        let fut = self.inner.call(req);
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let mut resp = fut.await?;
            if h1 && state.closing.load(Ordering::SeqCst) {
                resp.headers_mut().insert(
                    hyper::header::CONNECTION,
                    hyper::header::HeaderValue::from_static("close"),
                );
            }
            Ok(resp)
        })
    }
}

/// Serves one HTTP connection (HTTP/1.1 or HTTP/2) with `svc` until it
/// ends. When the listener asks it to close ([`ClientConn::close_soon`])
/// it closes without failing a request the client has sent:
///
/// - HTTP/2: `GOAWAY` at once. hyper's h2 server sends the two-step
///   `GOAWAY` (last stream id 2^31-1, a `PING`, then the last stream it
///   accepted), serves every stream up to that id to the end, then closes.
/// - HTTP/1.1: the next response carries `connection: close` and the
///   connection closes after it; a connection idle for [`H1_IDLE_GRACE`]
///   closes then (a request already being read is answered first).
///
/// The forced close ([`ClientConn::retired`]) is the listener's.
pub(crate) async fn serve_conn<S>(conn: ClientConn, svc: S, max_streams: u32) -> Result<()>
where
    S: hyper::service::Service<Request<Incoming>, Response = Response<Body>, Error = Infallible>
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    use std::sync::atomic::Ordering;
    let close_soon = conn.close_soon.clone();
    let state = Arc::new(CloseState::default());
    if let MaybeTls::Tls(t) = &conn.stream
        && t.get_ref().1.alpn_protocol() == Some(b"h2".as_slice())
    {
        state.h2.store(true, Ordering::SeqCst);
    }
    let svc = CloseAware {
        inner: svc,
        state: Arc::clone(&state),
    };
    let mut builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
    builder.http2().max_concurrent_streams(max_streams);
    // With upgrades: `GET .../changes` may become a WebSocket.
    let served = builder.serve_connection_with_upgrades(TokioIo::new(conn.stream), svc);
    tokio::pin!(served);
    let r = tokio::select! {
        r = served.as_mut() => r,
        () = close_soon.cancelled() => {
            state.closing.store(true, Ordering::SeqCst);
            if !state.h2.load(Ordering::SeqCst) {
                // HTTP/1.1, or no request yet: let the next response say
                // `connection: close`; close an idle connection after the
                // grace, or at once if the client turns out to speak h2.
                tokio::select! {
                    r = served.as_mut() => return finish(r),
                    () = state.h2_seen.notified() => {}
                    () = tokio::time::sleep(H1_IDLE_GRACE) => {}
                }
            }
            served.as_mut().graceful_shutdown();
            served.await
        }
    };
    finish(r)
}

fn finish(r: std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>) -> Result<()> {
    r.map_err(|e| sprig_common::SprigError::internal(format!("HTTP connection: {e}")))
}
