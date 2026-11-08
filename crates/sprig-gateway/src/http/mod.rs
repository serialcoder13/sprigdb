//! HTTP on 8080 and 8443 (chapter 04 step 4; chapter 37 steps 2-6): REST
//! and gRPC on one port.
//!
//! ```text
//! serve(listener):                        -- listener "http" (8080) or "https" (8443)
//!  1. accept a TCP connection; admission check (chapter 04)
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
//! listeners). A listener change or a drain asks the connection to close
//! at its next safe point ([`ClientConn::close_soon`]): hyper then sends
//! `GOAWAY` (HTTP/2) or `connection: close` (HTTP/1.1) after the requests
//! in flight, so no request fails; the forced close follows only if a
//! request outlives the drain time.
//!
//! The services: [`sql`] (SQL and transaction handles through the
//! executor), [`admin`] (the Admin API, forwarded to the controllers, and
//! `Gateways`), [`grpc`] (the tonic services over both), and REST in
//! [`rest`]. Authentication is [`auth`]'s.

pub mod admin;
pub mod auth;
pub mod grpc;
pub mod rest;
pub mod settings;
pub mod sink;
pub mod sql;

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
use crate::frontend::{ClientConn, Frontend, GatewayContext, Protocol, TlsInfo};

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
    ) -> HttpCredentials {
        HttpCredentials {
            authorization,
            tenant_header: tenant,
            sni: self.tls.as_ref().and_then(|t| t.sni.clone()),
            client_ip: self.peer.ip(),
            tls: self.tls.is_some(),
            client_cert: self.tls.as_ref().is_some_and(|t| t.client_cert),
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
    ) -> Result<Caller> {
        let creds = info.credentials(authorization.clone(), tenant);
        let principal = self.auth.authenticate(&creds).await?;
        Ok(Caller {
            principal,
            peer: info.peer,
            tls: info.tls.clone(),
            listener: info.listener,
            generation: info.generation,
            authorization,
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
        rest::handle(&core, &info, req).await
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
        let close_soon = conn.close_soon.clone();
        let core = Arc::clone(&self.core);
        let grpc = self.grpc.clone();
        let svc = hyper::service::service_fn(move |req: Request<Incoming>| {
            let core = Arc::clone(&core);
            let grpc = grpc.clone();
            let info = info.clone();
            async move { Ok::<_, Infallible>(route(core, grpc, info, req).await) }
        });
        let streams = self.core.engine.settings().max_streams;
        let mut builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
        builder.http2().max_concurrent_streams(streams);
        let served = builder.serve_connection(TokioIo::new(conn.stream), svc);
        tokio::pin!(served);
        let r = tokio::select! {
            r = served.as_mut() => r,
            () = close_soon.cancelled() => {
                // A safe point for HTTP: finish what is in flight, accept
                // nothing new on this connection.
                served.as_mut().graceful_shutdown();
                served.await
            }
        };
        r.map_err(|e| sprig_common::SprigError::internal(format!("HTTP connection: {e}")))
    }
}
