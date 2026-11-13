//! The peer service on 7403 (chapter 04, "Interfaces"; build step 10).
//!
//! Gateways call each other through `sprig.internal.v1.GatewayPeer`:
//!
//! - `Kill`: `KILL [QUERY] id` where `id`'s slot belongs to another gateway
//!   (the owner is found through `gateway-slots/<slot>`, chapter 04
//!   "Registration, KILL and write holds");
//! - `ListSessions`: the gateway's sessions;
//! - `Drain`: `sprigctl gateway drain` reaching a gateway through any other;
//! - `Forward`: a transaction-handle call that landed on the wrong gateway
//!   (chapter 37, "Transactions").
//!
//! - `QueuePeer` (a second service on the same listener): chapter 50's
//!   queue relay, [`crate::amqp::peer`].
//! - `RedisPeer`: chapter 40's pub/sub hubs, [`crate::redis::hub`].
//! - `HandoffLogin`: a login of a tenant whose residency (chapter 26)
//!   doesn't allow the calling gateway's DC, decided here (chapter 14,
//!   "Credential residency"); [`PeerHandoff`] is the calling side. A
//!   gateway of another DC is found in that DC's tier of the one etcd
//!   (single-etcd mode, like the locator's remote maps).
//!
//! **Transport.** With `[tls]` in `sprigdb.toml` (the process identity from
//! the internal CA) the listener requires client certificates signed by
//! that CA and clients present theirs: mutual TLS, not a setting. Without
//! `[tls]` (development) the listener serves plaintext and only on a
//! loopback address; a non-loopback `listener.peer.bind` without `[tls]`
//! leaves the peer service off.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use sprig_api::proto::internal::gateway_peer_client::GatewayPeerClient;
use sprig_api::proto::internal::gateway_peer_server::{GatewayPeer, GatewayPeerServer};
use sprig_api::proto::internal::{
    DrainReply, DrainRequest, ForwardReply, ForwardRequest, HandoffLoginReply, HandoffLoginRequest,
    KillReply, KillRequest, ListSessionsReply, ListSessionsRequest, SessionRow,
};
use sprig_auth::handoff::{HandoffResult, HandoffStart, HandoffTarget, PrincipalWire};
use sprig_common::ids::{ConnId, DcId, TenantId};
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_meta::MetaStore;
use sprig_meta::{Tier, TierView};
use sprig_proto_mysql::{ErrPacket, Persona};
use sprig_settings::{Apply, HardBounds, Scopes, Setting};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use zeroize::Zeroizing;

use crate::registry::{self, GatewayRecord};
use crate::session::{SessionTable, slot_of};

/// `listener.peer.bind`.
pub static PEER_BIND: Setting<std::borrow::Cow<'static, str>> = Setting {
    key: "listener.peer.bind",
    default: std::borrow::Cow::Borrowed("0.0.0.0:7403"),
    scopes: Scopes::PROCESS,
    hard: HardBounds::NONE,
    apply: Apply::Rebind,
    operator_only: true,
    owner: 4,
    doc: "Address of the internal peer service (always mutual TLS); read at start",
};
sprig_settings::register!(PEER_BIND);

/// Timeout of one peer call.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(5);

/// The process identity used for mutual TLS (`[tls]`).
#[derive(Clone)]
pub struct PeerTls {
    /// The internal CA, PEM.
    pub ca_pem: Vec<u8>,
    /// This process's chain, PEM.
    pub cert_pem: Vec<u8>,
    /// Its key, PEM.
    pub key_pem: Vec<u8>,
}

impl std::fmt::Debug for PeerTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerTls").finish_non_exhaustive()
    }
}

impl PeerTls {
    /// Reads `[tls]`'s files.
    ///
    /// # Errors
    /// 39998 for an unreadable file.
    pub fn from_config(cfg: &sprig_common::config::TlsConfig) -> Result<Self> {
        let read = |p: &std::path::Path| {
            std::fs::read(p).map_err(|e| {
                SprigError::new(
                    SprigCode::ConfigInvalid,
                    format!("[tls] {}: {e}", p.display()),
                )
            })
        };
        Ok(Self {
            ca_pem: read(&cfg.ca)?,
            cert_pem: read(&cfg.cert)?,
            key_pem: read(&cfg.key)?,
        })
    }

    fn server_config(&self) -> Result<Arc<rustls::ServerConfig>> {
        let bad = |w: &str, e: String| {
            SprigError::new(SprigCode::ConfigInvalid, format!("[tls] {w}: {e}"))
        };
        let mut roots = rustls::RootCertStore::empty();
        for c in CertificateDer::pem_slice_iter(&self.ca_pem) {
            let c = c.map_err(|e| bad("ca", e.to_string()))?;
            roots.add(c).map_err(|e| bad("ca", e.to_string()))?;
        }
        let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&self.cert_pem)
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| bad("cert", e.to_string()))?;
        let key =
            PrivateKeyDer::from_pem_slice(&self.key_pem).map_err(|e| bad("key", e.to_string()))?;
        let provider = crate::tls::provider();
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::clone(&provider),
        )
        .build()
        .map_err(|e| bad("ca", e.to_string()))?;
        let mut cfg = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| bad("versions", e.to_string()))?
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain, key)
            .map_err(|e| bad("cert", e.to_string()))?;
        cfg.alpn_protocols = vec![b"h2".to_vec()];
        Ok(Arc::new(cfg))
    }
}

/// What the peer service needs from the rest of the gateway.
#[async_trait]
pub trait PeerHost: Send + Sync + 'static {
    /// This gateway's sessions.
    fn sessions(&self) -> &Arc<SessionTable>;
    /// This gateway's id.
    fn gateway_id(&self) -> String;
    /// Starts the drain; `false` if it was already draining.
    fn drain(&self, reason: &str) -> bool;
    /// Runs a forwarded transaction-handle call.
    async fn forwarded(&self, req: ForwardRequest) -> ForwardReply;
    /// The queue relay's service (chapter 50, `QueuePeer`), when this
    /// gateway runs the queue engine.
    fn queue_peer(&self) -> Option<crate::amqp::peer::QueuePeerService> {
        None
    }
    /// Workers' runs of triggers on streams and queues (chapter 41,
    /// `LogicPeer`), when this gateway serves tenant catalogs.
    fn logic_peer(&self) -> Option<crate::triggers::LogicPeerService> {
        None
    }
    /// Chapter 40's pub/sub hubs (`RedisPeer`), when this gateway runs
    /// the Redis front end.
    fn redis_peer(&self) -> Option<crate::redis::hub::RedisPeerService> {
        None
    }
    /// Decides a handed-off login (chapter 14, "Credential residency").
    /// Hosts without logins refuse it with 39301.
    async fn handoff(&self, start: HandoffStart, password: Zeroizing<Vec<u8>>) -> HandoffResult {
        let _ = (start, password);
        HandoffResult::Refused(ErrPacket::from_error(
            Persona::MySql84,
            &SprigError::new(SprigCode::AuthUnavailable, "this gateway decides no logins"),
        ))
    }
}

/// A handoff's result as the reply carries it.
fn reply_of(r: &HandoffResult) -> HandoffLoginReply {
    match r {
        HandoffResult::Accepted(p) => HandoffLoginReply {
            accepted: true,
            principal_json: serde_json::to_string(&PrincipalWire::of(p)).unwrap_or_default(),
            ..HandoffLoginReply::default()
        },
        HandoffResult::Refused(e) => HandoffLoginReply {
            accepted: false,
            principal_json: String::new(),
            error_code: u32::from(e.code),
            sql_state: String::from_utf8_lossy(&e.sqlstate).into_owned(),
            error_message: e.message.clone(),
        },
    }
}

/// The result a reply carries.
///
/// # Errors
/// Internal for a malformed reply.
fn result_of(r: &HandoffLoginReply) -> Result<HandoffResult> {
    if r.accepted {
        let w: PrincipalWire = serde_json::from_str(&r.principal_json)
            .map_err(|e| SprigError::internal(format!("handoff reply: {e}")))?;
        return Ok(HandoffResult::Accepted(Box::new(w.principal()?)));
    }
    let mut sqlstate = *b"HY000";
    if r.sql_state.len() == 5 {
        sqlstate.copy_from_slice(r.sql_state.as_bytes());
    }
    Ok(HandoffResult::Refused(ErrPacket {
        code: u16::try_from(r.error_code).unwrap_or(1045),
        sqlstate,
        message: r.error_message.clone(),
    }))
}

struct Service {
    host: Arc<dyn PeerHost>,
}

#[async_trait]
impl GatewayPeer for Service {
    async fn kill(
        &self,
        req: tonic::Request<KillRequest>,
    ) -> std::result::Result<tonic::Response<KillReply>, tonic::Status> {
        let r = req.into_inner();
        let sessions = self.host.sessions();
        let Some(target) = sessions.get(ConnId(r.connection_id)) else {
            return Ok(tonic::Response::new(KillReply::default()));
        };
        let row = target.process_row(sessions.clock().now());
        let tenant = (r.tenant_id != 0).then_some(TenantId(r.tenant_id));
        // An empty user is a `CONNECTION_ADMIN` caller's: any user's
        // session of its tenant (never without a tenant).
        let owner = row.tenant == tenant
            && row.user.is_some()
            && (row.user.as_deref() == Some(r.user.as_str())
                || (r.user.is_empty() && tenant.is_some()));
        if owner {
            if r.query_only {
                target.flags().kill_query();
            } else {
                target.flags().kill_conn();
            }
        }
        Ok(tonic::Response::new(KillReply {
            found: true,
            killed: owner,
        }))
    }

    async fn list_sessions(
        &self,
        req: tonic::Request<ListSessionsRequest>,
    ) -> std::result::Result<tonic::Response<ListSessionsReply>, tonic::Status> {
        let tenant = req.into_inner().tenant_id;
        let sessions = self.host.sessions();
        let rows = sessions
            .list()
            .into_iter()
            .filter(|r| tenant == 0 || r.tenant.map(|t| t.0) == Some(tenant))
            .map(|r| SessionRow {
                id: r.id.0,
                user: r.user.unwrap_or_default(),
                host: r.host.to_string(),
                db: r.db.unwrap_or_default(),
                command: r.command.to_owned(),
                time_s: r.time.as_secs(),
                state: r.state.as_str().to_owned(),
                info: r.info.unwrap_or_default(),
                tenant_id: r.tenant.map_or(0, |t| t.0),
                protocol: r.protocol.as_str().to_owned(),
            })
            .collect();
        Ok(tonic::Response::new(ListSessionsReply {
            gateway_id: self.host.gateway_id(),
            sessions: rows,
        }))
    }

    async fn drain(
        &self,
        req: tonic::Request<DrainRequest>,
    ) -> std::result::Result<tonic::Response<DrainReply>, tonic::Status> {
        let reason = req.into_inner().reason;
        Ok(tonic::Response::new(DrainReply {
            started: self.host.drain(&reason),
        }))
    }

    async fn forward(
        &self,
        req: tonic::Request<ForwardRequest>,
    ) -> std::result::Result<tonic::Response<ForwardReply>, tonic::Status> {
        Ok(tonic::Response::new(
            self.host.forwarded(req.into_inner()).await,
        ))
    }

    async fn handoff_login(
        &self,
        req: tonic::Request<HandoffLoginRequest>,
    ) -> std::result::Result<tonic::Response<HandoffLoginReply>, tonic::Status> {
        let r = req.into_inner();
        let start: HandoffStart = serde_json::from_str(&r.start_json)
            .map_err(|e| tonic::Status::invalid_argument(format!("start_json: {e}")))?;
        let result = self.host.handoff(start, Zeroizing::new(r.password)).await;
        Ok(tonic::Response::new(reply_of(&result)))
    }
}

/// Binds the peer listener and serves it until `stop`. Returns the bound
/// address, or `None` when the service stays off (see the module notes).
///
/// # Errors
/// 39998 if `[tls]` doesn't load or the address can't be bound.
pub async fn serve(
    bind: SocketAddr,
    tls: Option<PeerTls>,
    host: Arc<dyn PeerHost>,
    stop: CancellationToken,
) -> Result<Option<(SocketAddr, tokio::task::JoinHandle<()>)>> {
    if tls.is_none() && !bind.ip().is_loopback() {
        tracing::warn!(%bind, "no [tls]: the peer service needs mutual TLS off loopback; it stays off (cross-gateway KILL, drain, forwarding and the queue relay are unavailable)");
        return Ok(None);
    }
    let acceptor = tls
        .as_ref()
        .map(PeerTls::server_config)
        .transpose()?
        .map(TlsAcceptor::from);
    let listener = TcpListener::bind(bind).await.map_err(|e| {
        SprigError::new(
            SprigCode::ConfigInvalid,
            format!("can't bind the peer service on {bind}: {e}"),
        )
    })?;
    let local = listener
        .local_addr()
        .map_err(|e| SprigError::internal("peer listener address").with_source(e))?;
    let queue_peer = host.queue_peer();
    let logic_peer = host.logic_peer();
    let redis_peer = host.redis_peer();
    let mut routes = tonic::service::Routes::new(GatewayPeerServer::new(Service { host }));
    if let Some(q) = queue_peer {
        routes = routes.add_service(q.server());
    }
    if let Some(l) = logic_peer {
        routes = routes.add_service(l.server());
    }
    if let Some(r) = redis_peer {
        routes = routes.add_service(r.server());
    }
    // Chapter 39 (R5 round 6): seeds of search copies on workers.
    routes = routes.add_service(crate::search::seed::SeedService::server());
    let routes = routes.prepare();
    tracing::info!(bind = %local, mtls = acceptor.is_some(), "peer service listening");
    let task = tokio::spawn(async move {
        let tracker = tokio_util::task::TaskTracker::new();
        loop {
            let (sock, peer) = tokio::select! {
                () = stop.cancelled() => break,
                r = listener.accept() => match r {
                    Ok(a) => a,
                    Err(e) => {
                        tracing::warn!(error = %e, "peer accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                },
            };
            let routes = routes.clone();
            let acceptor = acceptor.clone();
            let stop = stop.clone();
            tracker.spawn(async move {
                let _ = sock.set_nodelay(true);
                let svc = hyper_util::service::TowerToHyperService::new(routes);
                let builder = hyper_util::server::conn::auto::Builder::new(
                    hyper_util::rt::TokioExecutor::new(),
                )
                .http2_only();
                let r = if let Some(a) = acceptor {
                    match a.accept(sock).await {
                        Ok(tls) => {
                            let conn =
                                builder.serve_connection(hyper_util::rt::TokioIo::new(tls), svc);
                            tokio::select! { r = conn => r, () = stop.cancelled() => Ok(()) }
                        }
                        Err(e) => {
                            tracing::debug!(%peer, error = %e, "peer TLS handshake failed");
                            return;
                        }
                    }
                } else {
                    let conn = builder.serve_connection(hyper_util::rt::TokioIo::new(sock), svc);
                    tokio::select! { r = conn => r, () = stop.cancelled() => Ok(()) }
                };
                if let Err(e) = r {
                    tracing::debug!(%peer, error = %e, "peer connection ended");
                }
            });
        }
        tracker.close();
        tracker.wait().await;
    });
    Ok(Some((local, task)))
}

/// What a remote `KILL` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillOutcome {
    /// Killed.
    Done,
    /// The session belongs to another user or tenant.
    NotOwner,
    /// No such session (or no gateway holds its slot).
    Unknown,
}

/// Clients of the other gateways of this DC.
pub struct Peers {
    store: Arc<dyn MetaStore>,
    tls: Option<PeerTls>,
    channels: Mutex<HashMap<String, Channel>>,
}

impl std::fmt::Debug for Peers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Peers")
            .field("mtls", &self.tls.is_some())
            .finish_non_exhaustive()
    }
}

fn unavailable(what: &str, e: impl std::fmt::Display) -> SprigError {
    SprigError::new(SprigCode::ForwardFailed, format!("{what}: {e}"))
}

impl Peers {
    /// Clients that find gateways in `store` (the DC tier) and connect with
    /// `tls` (`None`: plaintext, development).
    #[must_use]
    pub fn new(store: Arc<dyn MetaStore>, tls: Option<PeerTls>) -> Self {
        Self {
            store,
            tls,
            channels: Mutex::new(HashMap::new()),
        }
    }

    /// The registration record of gateway `id`.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn gateway(&self, id: &str) -> Result<Option<GatewayRecord>> {
        registry::get(self.store.as_ref(), id).await
    }

    fn client(&self, rec: &GatewayRecord) -> Result<GatewayPeerClient<Channel>> {
        let port = rec.listeners.get("peer").ok_or_else(|| {
            unavailable(
                "peer service",
                format!("gateway {} serves none", rec.gateway_id),
            )
        })?;
        let scheme = if self.tls.is_some() { "https" } else { "http" };
        let uri = format!("{scheme}://{}:{port}", rec.addr);
        let mut channels = self.channels.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(c) = channels.get(&uri) {
            return Ok(GatewayPeerClient::new(c.clone()));
        }
        let mut ep = Endpoint::from_shared(uri.clone())
            .map_err(|e| unavailable("peer address", e))?
            .connect_timeout(CALL_TIMEOUT)
            .timeout(CALL_TIMEOUT);
        if let Some(t) = &self.tls {
            let cfg = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(&t.ca_pem))
                .identity(Identity::from_pem(&t.cert_pem, &t.key_pem))
                .domain_name(rec.addr.clone());
            ep = ep.tls_config(cfg).map_err(|e| unavailable("peer TLS", e))?;
        }
        let ch = ep.connect_lazy();
        channels.insert(uri, ch.clone());
        Ok(GatewayPeerClient::new(ch))
    }

    /// `KILL [QUERY] id` on the gateway holding `id`'s slot, as `user` of
    /// `tenant`.
    ///
    /// # Errors
    /// 39788 when the owner can't be reached.
    pub async fn kill(
        &self,
        id: ConnId,
        query_only: bool,
        tenant: Option<TenantId>,
        user: &str,
    ) -> Result<KillOutcome> {
        let Some(rec) = registry::slot_holder(self.store.as_ref(), slot_of(id)).await? else {
            return Ok(KillOutcome::Unknown);
        };
        let mut c = self.client(&rec)?;
        let r = c
            .kill(KillRequest {
                connection_id: id.0,
                query_only,
                tenant_id: tenant.map_or(0, |t| t.0),
                user: user.to_owned(),
            })
            .await
            .map_err(|e| unavailable("peer kill", e))?
            .into_inner();
        Ok(match (r.found, r.killed) {
            (true, true) => KillOutcome::Done,
            (true, false) => KillOutcome::NotOwner,
            _ => KillOutcome::Unknown,
        })
    }

    /// Asks gateway `id` to drain.
    ///
    /// # Errors
    /// 39792 for an unknown gateway; 39788 when it can't be reached.
    pub async fn drain(&self, id: &str, reason: &str) -> Result<bool> {
        let rec = self.gateway(id).await?.ok_or_else(|| {
            SprigError::new(
                SprigCode::AdminRequestInvalid,
                format!("no gateway {id} in this DC"),
            )
        })?;
        let mut c = self.client(&rec)?;
        Ok(c.drain(DrainRequest {
            reason: reason.to_owned(),
        })
        .await
        .map_err(|e| unavailable("peer drain", e))?
        .into_inner()
        .started)
    }

    /// Forwards a call to gateway `id`.
    ///
    /// # Errors
    /// 39788 when the gateway is gone or can't be reached.
    pub async fn forward(&self, id: &str, req: ForwardRequest) -> Result<ForwardReply> {
        let rec = self
            .gateway(id)
            .await?
            .ok_or_else(|| unavailable("forward", format!("gateway {id} is not registered")))?;
        let mut c = self.client(&rec)?;
        Ok(c.forward(req)
            .await
            .map_err(|e| unavailable("forward", e))?
            .into_inner())
    }

    /// The ready gateways of DC `dc` that serve the peer service, from that
    /// DC's tier (single-etcd mode).
    ///
    /// # Errors
    /// The store's errors.
    pub async fn gateways_in(&self, dc: &DcId) -> Result<Vec<GatewayRecord>> {
        let tier = TierView::over(&self.store, Tier::Dc(dc.clone()));
        Ok(registry::list(&*tier)
            .await?
            .into_iter()
            .filter(|r| {
                r.state == registry::GatewayState::Ready && r.listeners.contains_key("peer")
            })
            .collect())
    }

    /// `HandoffLogin` on gateway `rec`.
    ///
    /// # Errors
    /// 39788 when it can't be reached; internal for a malformed reply.
    pub async fn handoff(
        &self,
        rec: &GatewayRecord,
        start: &HandoffStart,
        password: &[u8],
    ) -> Result<HandoffResult> {
        let mut c = self.client(rec)?;
        let reply = c
            .handoff_login(HandoffLoginRequest {
                start_json: serde_json::to_string(start)
                    .map_err(|e| SprigError::internal(e.to_string()))?,
                password: password.to_vec(),
            })
            .await
            .map_err(|e| unavailable("handoff", e))?
            .into_inner();
        result_of(&reply)
    }

    /// Every gateway's sessions (for operators).
    ///
    /// # Errors
    /// The store's errors; unreachable gateways are skipped.
    pub async fn list_sessions(&self, tenant: u64) -> Result<Vec<ListSessionsReply>> {
        let mut out = Vec::new();
        for rec in registry::list(self.store.as_ref()).await? {
            let Ok(mut c) = self.client(&rec) else {
                continue;
            };
            if let Ok(r) = c
                .list_sessions(ListSessionsRequest { tenant_id: tenant })
                .await
            {
                out.push(r.into_inner());
            }
        }
        Ok(out)
    }
}

/// [`HandoffTarget`] over the peer service: a login handed to DC `dc` goes
/// to one of its ready gateways, trying each until one answers.
#[derive(Debug)]
pub struct PeerHandoff {
    peers: Arc<Peers>,
}

impl PeerHandoff {
    /// Hand-offs through `peers`.
    #[must_use]
    pub fn new(peers: Arc<Peers>) -> Self {
        Self { peers }
    }
}

#[async_trait]
impl HandoffTarget for PeerHandoff {
    async fn login(
        &self,
        dc: &DcId,
        start: HandoffStart,
        password: Zeroizing<Vec<u8>>,
    ) -> Result<HandoffResult> {
        let gateways = self.peers.gateways_in(dc).await?;
        let mut last = None;
        for rec in &gateways {
            match self.peers.handoff(rec, &start, &password).await {
                Ok(r) => return Ok(r),
                Err(e) => {
                    tracing::warn!(%dc, gateway = %rec.gateway_id, error = %e, "handoff target unreachable");
                    last = Some(e);
                }
            }
        }
        Err(last.unwrap_or_else(|| unavailable("handoff", format!("no ready gateway in {dc}"))))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::net::IpAddr;

    use sprig_auth::Principal;
    use sprig_auth::accounts::BackendTarget;
    use sprig_common::time::SystemClock;
    use sprig_meta::{MemStore, Op, Record, Txn};

    use super::*;

    fn dc(s: &str) -> DcId {
        DcId::new(s).unwrap()
    }

    /// A gateway of fra1 that accepts `pw` for anyone.
    struct Fra1 {
        sessions: Arc<SessionTable>,
        seen: Mutex<Vec<DcId>>,
    }

    #[async_trait]
    impl PeerHost for Fra1 {
        fn sessions(&self) -> &Arc<SessionTable> {
            &self.sessions
        }
        fn gateway_id(&self) -> String {
            "fra1-gw".into()
        }
        fn drain(&self, _reason: &str) -> bool {
            false
        }
        async fn forwarded(&self, _req: ForwardRequest) -> ForwardReply {
            ForwardReply::default()
        }
        async fn handoff(
            &self,
            start: HandoffStart,
            password: Zeroizing<Vec<u8>>,
        ) -> HandoffResult {
            self.seen.lock().unwrap().push(start.source_dc.clone());
            if password.as_slice() != b"pw" {
                return HandoffResult::Refused(ErrPacket::access_denied(
                    Persona::MySql84,
                    "acme.alice",
                    "192.0.2.10",
                    true,
                ));
            }
            HandoffResult::Accepted(Box::new(Principal {
                tenant: Some(TenantId(42)),
                tenant_name: Some("acme".into()),
                user_id: Some(sprig_common::ids::UserId(1)),
                user: "alice".into(),
                host: "%".into(),
                client_ip: start.client_ip,
                must_change_password: false,
                limits: sprig_auth::store::AccountLimits::default(),
                auth_version: 7,
                backend: BackendTarget {
                    cluster: sprig_common::ids::ClusterId::new("fra1-c1").unwrap(),
                    user: "t42_u1".into(),
                    credentials: "file:/etc/sprigdb/creds/fra1.toml".parse().unwrap(),
                },
            }))
        }
    }

    fn start(ip: IpAddr) -> HandoffStart {
        serde_json::from_value(serde_json::json!({
            "persona": "mysql", "login_name": b"acme.alice".to_vec(), "sni": null,
            "client_ip": ip, "tls": true, "client_cert": false,
            "can_handle_expired_password": false, "source_dc": "nyc1"
        }))
        .unwrap()
    }

    /// `HandoffLogin` over real gRPC on loopback: nyc1's `PeerHandoff`
    /// finds fra1's ready gateway in fra1's tier, and only the decision
    /// and the principal's fields come back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_login_is_handed_to_a_gateway_of_the_allowed_dc() {
        let mem: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(dc("nyc1"))));
        let host = Arc::new(Fra1 {
            sessions: Arc::new(SessionTable::new(Arc::new(SystemClock))),
            seen: Mutex::new(Vec::new()),
        });
        let stop = CancellationToken::new();
        let (addr, task) = serve(
            "127.0.0.1:0".parse().unwrap(),
            None,
            Arc::clone(&host) as Arc<dyn PeerHost>,
            stop.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        let fra1 = TierView::over(&mem, Tier::Dc(dc("fra1")));
        let mut rec = GatewayRecord {
            gateway_id: "fra1-gw".into(),
            slot: 1,
            version: "test".into(),
            addr: "127.0.0.1".into(),
            listeners: [("peer".to_owned(), addr.port())].into(),
            state: registry::GatewayState::Ready,
            catalog_formats: Vec::new(),
            authorizers: Vec::new(),
            load: registry::Load::default(),
            kafka_host: None,
        };
        let put = |r: &GatewayRecord| {
            let fra1 = Arc::clone(&fra1);
            let value = Record::new(r.clone()).encode().unwrap();
            async move {
                fra1.txn(Txn {
                    when: Vec::new(),
                    then: vec![Op::Put {
                        key: fra1.tier().key("gateways/fra1-gw").unwrap(),
                        value,
                        lease: None,
                    }],
                    otherwise: Vec::new(),
                })
                .await
                .unwrap();
            }
        };
        put(&rec).await;
        let target = PeerHandoff::new(Arc::new(Peers::new(Arc::clone(&mem), None)));
        let ip: IpAddr = "192.0.2.10".parse().unwrap();
        let r = target
            .login(&dc("fra1"), start(ip), Zeroizing::new(b"pw".to_vec()))
            .await
            .unwrap();
        let HandoffResult::Accepted(p) = r else {
            panic!("refused: {r:?}");
        };
        assert_eq!(
            (p.user.as_str(), p.tenant, p.client_ip),
            ("alice", Some(TenantId(42)), ip)
        );
        assert_eq!(
            p.backend.credentials.to_string(),
            "file:/etc/sprigdb/creds/fra1.toml"
        );
        let r = target
            .login(&dc("fra1"), start(ip), Zeroizing::new(b"no".to_vec()))
            .await
            .unwrap();
        assert!(
            matches!(r, HandoffResult::Refused(ref e) if e.code == 1045 && &e.sqlstate == b"28000"),
            "{r:?}"
        );
        assert_eq!(
            host.seen.lock().unwrap().as_slice(),
            [dc("nyc1"), dc("nyc1")]
        );
        // No ready gateway in the DC: an error, so the source tries the
        // next allowed DC (and 39301 when none answers).
        rec.state = registry::GatewayState::Draining;
        put(&rec).await;
        let e = target
            .login(&dc("fra1"), start(ip), Zeroizing::new(b"pw".to_vec()))
            .await
            .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::ForwardFailed));
        assert!(
            target
                .login(&dc("ams1"), start(ip), Zeroizing::new(b"pw".to_vec()))
                .await
                .is_err()
        );
        stop.cancel();
        let _ = task.await;
    }
}
