//! The broker front end (chapter 45, "The Kafka front end", build steps 9,
//! 12 and 13): one connection's loop of size-prefixed requests, SASL, and
//! the API handlers.
//!
//! The broker is protocol code over traits the gateway implements:
//!
//! - [`Identity`]: SASL PLAIN and SCRAM onto chapter 14's logins;
//! - [`Cluster`]: this broker's id and the DC's gateways (the brokers
//!   Metadata lists);
//! - [`Topics`]: topic administration over streams;
//! - [`GroupStore`](crate::group::GroupStore) and
//!   [`Leases`](crate::group::Leases), through the group
//!   [`Coordinator`];
//! - [`DataPlane`]: Produce, Fetch, ListOffsets and OffsetForLeaderEpoch
//!   (build steps 10 and 11; [`data::DataService`]). Without one, those
//!   APIs are not advertised and a request for them closes the connection,
//!   as Kafka does for an API it doesn't serve.
//!
//! **Requests are handled one at a time per connection, in order**, as a
//! Kafka broker does (it mutes a connection while a request is in flight),
//! so responses keep the order clients pipeline them in; a JoinGroup that
//! waits for a rebalance holds its connection, not others. The one
//! exception is a produce's wait for sequencing ([`DataReply::Later`]): it
//! runs while the connection's next requests are read and handled, at most
//! [`MAX_IN_FLIGHT`] at once, and its response still goes out in request
//! order. An idempotent producer's batches are therefore appended in the
//! order it sent them, but wait for their offsets together.
//!
//! **Authentication is required.** Before it, only ApiVersions,
//! SaslHandshake and SaslAuthenticate are served; anything else closes the
//! connection (Kafka's `IllegalSaslStateException`). PLAIN is offered only
//! over TLS. SaslHandshake v0 (raw SASL tokens after the handshake) and v1
//! (tokens in SaslAuthenticate) both work.

mod admin;
pub mod data;
mod groups;
mod meta;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

use async_trait::async_trait;
use sprig_common::CancellationToken;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::group::Coordinator;
use crate::messages::{
    ApiVersionsResponse, SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
    SaslHandshakeResponse, api_versions_response,
};
use crate::protocol::{self, ApiRange, RequestHeader, api, err};
use crate::sasl::{
    PlainCredentials, SaslError, ScramCredential, ScramMechanism, ScramServer, parse_plain,
};
use crate::topics::{Principal, Topics};
use crate::wire::{CodecError, Message, Reader};

/// The connection, as SASL needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnInfo {
    /// The client address (after a PROXY header).
    pub peer: SocketAddr,
    /// TLS at the gateway.
    pub tls: bool,
    /// The TLS server name, which can name the tenant (chapter 14).
    pub sni: Option<String>,
    /// A verified client certificate.
    pub client_cert: bool,
}

/// A refused login: the message for the SaslAuthenticate response.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct AuthFailure(pub String);

/// The end of a SCRAM login, once the proof is checked: chapter 14's
/// policy checks, counters and audit.
#[async_trait]
pub trait ScramFinish: Send {
    /// `verified`: whether the proof matched.
    async fn finish(self: Box<Self>, verified: bool) -> Result<Principal, AuthFailure>;
}

/// A SCRAM account lookup: the verifier (or a decoy), and how to finish.
pub struct ScramLookup {
    /// The account's verifier, or a decoy for unknown accounts.
    pub credential: ScramCredential,
    /// Called with the proof's result.
    pub finish: Box<dyn ScramFinish>,
}

impl std::fmt::Debug for ScramLookup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScramLookup").finish_non_exhaustive()
    }
}

/// Logins (chapter 14) for the Kafka listener.
#[async_trait]
pub trait Identity: Send + Sync + 'static {
    /// A PLAIN login.
    async fn plain(&self, c: &ConnInfo, creds: PlainCredentials) -> Result<Principal, AuthFailure>;
    /// The SCRAM verifier of `user` (as sent), or a decoy.
    async fn scram(&self, c: &ConnInfo, mech: ScramMechanism, user: &str) -> ScramLookup;
}

/// A broker in Metadata: one gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerNode {
    /// The broker id (the gateway's slot).
    pub node_id: i32,
    /// `kafka.advertised_host`.
    pub host: String,
    /// The `kafka` listener's port.
    pub port: i32,
    /// The rack (the DC).
    pub rack: Option<String>,
}

/// The brokers: the gateways of the client's data center.
#[async_trait]
pub trait Cluster: Send + Sync + 'static {
    /// This broker's id.
    fn node_id(&self) -> i32;
    /// The cluster id Metadata v2+ shows.
    fn cluster_id(&self) -> String;
    /// The ready gateways serving Kafka for `p`'s tenant, this one included
    /// when it is ready.
    async fn brokers(&self, p: &Principal) -> Vec<BrokerNode>;
}

/// An authenticated connection, as handlers see it.
#[derive(Debug, Clone)]
pub struct Session {
    /// Who.
    pub principal: Principal,
    /// From where.
    pub conn: ConnInfo,
    /// The client id of the request being served.
    pub client_id: String,
}

/// Responses waiting to be written, per connection.
pub const MAX_IN_FLIGHT: usize = 16;

/// A data plane response.
pub enum DataReply {
    /// The response body.
    Now(Vec<u8>),
    /// The response body, once this finishes; it runs while the
    /// connection's next requests are served (a produce's wait for
    /// sequencing).
    Later(std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, CodecError>> + Send>>),
}

impl std::fmt::Debug for DataReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Now(b) => f.debug_tuple("Now").field(&b.len()).finish(),
            Self::Later(_) => f.write_str("Later"),
        }
    }
}

/// Produce, Fetch, ListOffsets and OffsetForLeaderEpoch: chapter 45 build
/// steps 10 and 11. The broker advertises [`DataPlane::apis`] and hands
/// their requests over whole.
#[async_trait]
pub trait DataPlane: Send + Sync + 'static {
    /// The APIs and versions it serves.
    fn apis(&self) -> Vec<ApiRange>;
    /// Serves one request: the response body (without header), now or
    /// later. An error closes the connection.
    async fn handle(
        &self,
        s: &Session,
        h: &RequestHeader,
        body: &[u8],
    ) -> Result<DataReply, CodecError>;
    /// The current leader epoch of a partition, for Metadata v7+.
    async fn leader_epoch(&self, p: &Principal, stream_id: u64, partition: i32) -> i32;
}

/// Broker settings (live: read at each connection and request).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerConfig {
    /// `kafka.sasl_mechanisms`, in order.
    pub mechanisms: Vec<String>,
    /// Offer PLAIN on plaintext connections (only when the listener's TLS
    /// is off; never by default).
    pub plain_without_tls: bool,
    /// The largest request after login (Kafka's
    /// `socket.request.max.bytes`).
    pub max_frame: usize,
    /// The largest request before login (Kafka's
    /// `sasl.server.max.receive.size`).
    pub max_frame_unauthenticated: usize,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            mechanisms: vec![
                "SCRAM-SHA-512".into(),
                "SCRAM-SHA-256".into(),
                "PLAIN".into(),
            ],
            plain_without_tls: false,
            max_frame: 100 * 1024 * 1024,
            max_frame_unauthenticated: 512 * 1024,
        }
    }
}

/// Producer ids for InitProducerId without `transactional.id`: unique
/// without storage, as `(milliseconds << 16) | broker id`, increasing per
/// broker, so ids never repeat across brokers or restarts.
#[derive(Debug)]
pub struct ProducerIds {
    node: i64,
    last: AtomicI64,
}

impl ProducerIds {
    /// Ids for broker `node`.
    #[must_use]
    pub fn new(node: i32) -> Self {
        Self {
            node: i64::from(node) & 0xffff,
            last: AtomicI64::new(0),
        }
    }

    /// The next id.
    pub fn next(&self) -> i64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| {
                i64::try_from(d.as_millis()).unwrap_or(i64::MAX >> 16)
            });
        let fresh = (now << 16) | self.node;
        let mut cur = self.last.load(Ordering::Relaxed);
        loop {
            let next = fresh.max(cur + (1 << 16));
            match self
                .last
                .compare_exchange(cur, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return next,
                Err(c) => cur = c,
            }
        }
    }
}

/// The broker: shared by every connection of the listener.
pub struct Broker {
    /// Logins.
    pub identity: Arc<dyn Identity>,
    /// Brokers and this one's id.
    pub cluster: Arc<dyn Cluster>,
    /// Topic administration.
    pub topics: Arc<dyn Topics>,
    /// The group coordinator.
    pub groups: Arc<Coordinator>,
    /// Produce and Fetch, once built.
    pub data: Option<Arc<dyn DataPlane>>,
    /// Live settings.
    pub config: Arc<dyn Fn() -> BrokerConfig + Send + Sync>,
    /// InitProducerId.
    pub producer_ids: ProducerIds,
}

impl std::fmt::Debug for Broker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Broker").finish_non_exhaustive()
    }
}

/// Why a connection ended badly.
#[derive(Debug, thiserror::Error)]
pub enum ConnError {
    /// The socket failed.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    /// A request that can't be served; Kafka closes the connection too.
    #[error("{0}")]
    Protocol(String),
    /// Authentication failed (after the failure response was sent).
    #[error("authentication failed: {0}")]
    Auth(String),
}

enum Mech {
    Plain,
    Scram(ScramMechanism),
}

enum SaslState {
    Start,
    Handshaken {
        mech: Mech,
        raw: bool,
    },
    ScramFinal {
        server: Box<ScramServer>,
        finish: Box<dyn ScramFinish>,
        raw: bool,
    },
    Done,
}

struct Conn {
    info: ConnInfo,
    sasl: SaslState,
    principal: Option<Principal>,
}

/// What a handler returns: a response body, or close the connection.
pub(crate) enum Reply {
    Body(Vec<u8>),
    /// Send this body, then close (failed authentication).
    BodyThenClose(Vec<u8>, String),
    /// The body once this task ends.
    Later(tokio::task::JoinHandle<Result<Vec<u8>, CodecError>>),
}

/// What the connection's writer sends, in order.
enum Out {
    /// A raw SASL token frame (SaslHandshake v0).
    Raw(Vec<u8>),
    /// A response to `(api, version, correlation id)`.
    Reply(i16, i16, i32, Reply),
}

fn body<M: Message>(m: &M, v: i16) -> Result<Reply, ConnError> {
    m.to_bytes(v)
        .map(Reply::Body)
        .map_err(|e| ConnError::Protocol(format!("encoding a response: {e}")))
}

fn decode<M: Message>(b: &[u8], v: i16) -> Result<M, ConnError> {
    M::from_bytes(b, v).map_err(|e| ConnError::Protocol(format!("malformed request: {e}")))
}

impl Broker {
    /// The APIs to advertise: the served ones and the data plane's.
    #[must_use]
    pub fn api_ranges(&self) -> Vec<ApiRange> {
        let mut v: Vec<ApiRange> = protocol::SERVED.to_vec();
        if let Some(d) = &self.data {
            v.extend(d.apis());
        }
        v.sort_by_key(|r| r.key);
        v
    }

    fn range(&self, key: i16) -> Option<ApiRange> {
        self.api_ranges().into_iter().find(|r| r.key == key)
    }

    fn offered(&self, info: &ConnInfo) -> Vec<String> {
        let cfg = (self.config)();
        cfg.mechanisms
            .into_iter()
            .filter(|m| m != "PLAIN" || info.tls || cfg.plain_without_tls)
            .filter(|m| m == "PLAIN" || ScramMechanism::from_name(m).is_some())
            .collect()
    }

    /// Serves one connection until the client closes it, it fails, or
    /// `close` is cancelled (checked between requests).
    ///
    /// # Errors
    /// [`ConnError`]; the caller logs it and closes the socket.
    pub async fn serve<S: AsyncRead + AsyncWrite + Unpin + Send>(
        &self,
        stream: &mut S,
        info: ConnInfo,
        close: CancellationToken,
    ) -> Result<(), ConnError> {
        let (mut rd, mut wr) = tokio::io::split(stream);
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Out>(MAX_IN_FLIGHT);
        let reader = self.read_loop(&mut rd, info, close, tx);
        let writer = async {
            while let Some(o) = rx.recv().await {
                match o {
                    Out::Raw(b) => wr.write_all(&b).await?,
                    Out::Reply(api_key, api_version, correlation_id, reply) => {
                        let (b, close_after) = match reply {
                            Reply::Body(b) => (b, None),
                            Reply::BodyThenClose(b, why) => (b, Some(why)),
                            Reply::Later(h) => match h.await {
                                Ok(Ok(b)) => (b, None),
                                Ok(Err(e)) => {
                                    return Err(ConnError::Protocol(format!(
                                        "encoding a response: {e}"
                                    )));
                                }
                                Err(e) => {
                                    return Err(ConnError::Protocol(format!(
                                        "a {} request failed: {e}",
                                        protocol::api_name(api_key)
                                    )));
                                }
                            },
                        };
                        let frame =
                            protocol::response_frame(api_key, api_version, correlation_id, |w| {
                                w.raw(&b);
                                Ok(())
                            })
                            .map_err(|e| ConnError::Protocol(e.to_string()))?;
                        wr.write_all(&frame).await?;
                        if let Some(why) = close_after {
                            wr.flush().await?;
                            return Err(ConnError::Auth(why));
                        }
                    }
                }
            }
            wr.flush().await?;
            Ok::<(), ConnError>(())
        };
        tokio::pin!(reader);
        tokio::pin!(writer);
        tokio::select! {
            w = &mut writer => w,
            r = &mut reader => {
                // The reader is done (the client closed, or a request
                // can't be served): send what is queued first.
                let w = writer.await;
                r.and(w)
            }
        }
    }

    /// Reads and handles requests, queueing their responses for the
    /// writer in order.
    async fn read_loop<R: AsyncRead + Unpin + Send>(
        &self,
        stream: &mut R,
        info: ConnInfo,
        close: CancellationToken,
        tx: tokio::sync::mpsc::Sender<Out>,
    ) -> Result<(), ConnError> {
        let mut c = Conn {
            info,
            sasl: SaslState::Start,
            principal: None,
        };
        let send = |o: Out| {
            let tx = tx.clone();
            async move {
                tx.send(o)
                    .await
                    .map_err(|_| ConnError::Protocol("the connection's writer stopped".into()))
            }
        };
        loop {
            let mut size = [0u8; 4];
            tokio::select! {
                () = close.cancelled() => return Ok(()),
                r = stream.read_exact(&mut size) => match r {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                    Err(e) => return Err(e.into()),
                },
            }
            let size = i32::from_be_bytes(size);
            let cfg = (self.config)();
            let limit = if c.principal.is_some() {
                cfg.max_frame
            } else {
                cfg.max_frame_unauthenticated
            };
            let size = usize::try_from(size)
                .ok()
                .filter(|s| *s <= limit)
                .ok_or_else(|| {
                    ConnError::Protocol(format!(
                        "request of {size} bytes is over the limit of {limit}"
                    ))
                })?;
            let mut frame = vec![0u8; size];
            stream.read_exact(&mut frame).await?;
            // SaslHandshake v0: the next frames are bare SASL tokens.
            if let SaslState::Handshaken { raw: true, .. }
            | SaslState::ScramFinal { raw: true, .. } = c.sasl
            {
                let token = match self.sasl_step(&mut c, &frame).await {
                    Ok(t) => t,
                    Err(e) => return Err(ConnError::Auth(e)),
                };
                let mut out = Vec::with_capacity(token.len() + 4);
                out.extend_from_slice(&u32::try_from(token.len()).unwrap_or(0).to_be_bytes());
                out.extend_from_slice(&token);
                send(Out::Raw(out)).await?;
                continue;
            }
            let mut r = Reader::new(&frame);
            let header = RequestHeader::decode(&mut r)
                .map_err(|e| ConnError::Protocol(format!("malformed request header: {e}")))?;
            let started = Instant::now();
            let name = protocol::api_name(header.api_key);
            let reply = self.dispatch(&mut c, &header, r.rest()).await;
            let result = match &reply {
                Ok(Reply::Body(_) | Reply::Later(_)) => "ok",
                Ok(Reply::BodyThenClose(..)) => "auth_failed",
                Err(_) => "error",
            };
            metrics::counter!("sprig_kafka_requests_total", "api" => name, "result" => result)
                .increment(1);
            metrics::histogram!("sprig_kafka_request_duration_seconds", "api" => name)
                .record(started.elapsed().as_secs_f64());
            let reply = reply?;
            let closing = matches!(reply, Reply::BodyThenClose(..));
            send(Out::Reply(
                header.api_key,
                header.api_version,
                header.correlation_id,
                reply,
            ))
            .await?;
            if closing {
                return Ok(());
            }
        }
    }

    async fn dispatch(
        &self,
        c: &mut Conn,
        h: &RequestHeader,
        b: &[u8],
    ) -> Result<Reply, ConnError> {
        let v = h.api_version;
        if h.api_key == api::API_VERSIONS {
            return self.api_versions(v);
        }
        let Some(range) = self.range(h.api_key) else {
            return Err(ConnError::Protocol(format!(
                "39952 ER_SPRIG_KAFKA_UNSUPPORTED_API: API {} ({}) is not served",
                h.api_key,
                protocol::api_name(h.api_key)
            )));
        };
        if !range.serves(v) {
            return Err(ConnError::Protocol(format!(
                "39952 ER_SPRIG_KAFKA_UNSUPPORTED_API: {} v{v} is not served (versions {}-{})",
                protocol::api_name(h.api_key),
                range.min,
                range.max
            )));
        }
        match h.api_key {
            api::SASL_HANDSHAKE => return self.sasl_handshake(c, &decode(b, v)?, v),
            api::SASL_AUTHENTICATE => return self.sasl_authenticate(c, decode(b, v)?, v).await,
            _ => {}
        }
        let Some(principal) = c.principal.clone() else {
            return Err(ConnError::Protocol(format!(
                "{} before authentication: SASL is required",
                protocol::api_name(h.api_key)
            )));
        };
        let s = Session {
            principal,
            conn: c.info.clone(),
            client_id: h.client_id.clone().unwrap_or_default(),
        };
        match h.api_key {
            api::METADATA => self.metadata(&s, decode(b, v)?, v).await,
            api::FIND_COORDINATOR => self.find_coordinator(&s, decode(b, v)?, v).await,
            api::INIT_PRODUCER_ID => self.init_producer_id(&decode(b, v)?, v),
            api::JOIN_GROUP => self.join_group(&s, decode(b, v)?, v).await,
            api::SYNC_GROUP => self.sync_group(&s, decode(b, v)?, v).await,
            api::HEARTBEAT => self.heartbeat(&s, decode(b, v)?, v).await,
            api::LEAVE_GROUP => self.leave_group(&s, decode(b, v)?, v).await,
            api::OFFSET_COMMIT => self.offset_commit(&s, decode(b, v)?, v).await,
            api::OFFSET_FETCH => self.offset_fetch(&s, decode(b, v)?, v).await,
            api::DESCRIBE_GROUPS => self.describe_groups(&s, decode(b, v)?, v).await,
            api::LIST_GROUPS => self.list_groups(&s, decode(b, v)?, v).await,
            api::DELETE_GROUPS => self.delete_groups(&s, decode(b, v)?, v).await,
            api::CREATE_TOPICS => self.create_topics(&s, decode(b, v)?, v).await,
            api::DELETE_TOPICS => self.delete_topics(&s, decode(b, v)?, v).await,
            api::CREATE_PARTITIONS => self.create_partitions(&s, decode(b, v)?, v).await,
            api::DESCRIBE_CONFIGS => self.describe_configs(&s, decode(b, v)?, v).await,
            api::INCREMENTAL_ALTER_CONFIGS => self.alter_configs(&s, decode(b, v)?, v).await,
            _ => match &self.data {
                Some(d) => match d.handle(&s, h, b).await {
                    Ok(DataReply::Now(b)) => Ok(Reply::Body(b)),
                    Ok(DataReply::Later(f)) => Ok(Reply::Later(tokio::spawn(f))),
                    Err(e) => Err(ConnError::Protocol(e.to_string())),
                },
                None => Err(ConnError::Protocol("no data plane".into())),
            },
        }
    }

    fn api_versions(&self, v: i16) -> Result<Reply, ConnError> {
        let ranges = self.api_ranges();
        let api_keys = ranges
            .iter()
            .map(|r| api_versions_response::ApiVersion {
                api_key: r.key,
                min_version: r.min,
                max_version: r.max,
                ..Default::default()
            })
            .collect();
        let max = self.range(api::API_VERSIONS).map_or(0, |r| r.max);
        if !(0..=max).contains(&v) {
            // A version we don't have: answer in v0, which every client
            // reads, with UNSUPPORTED_VERSION and our ranges; the client
            // retries with a version we serve.
            let resp = ApiVersionsResponse {
                error_code: err::UNSUPPORTED_VERSION,
                api_keys,
                ..Default::default()
            };
            return body(&resp, 0);
        }
        let resp = ApiVersionsResponse {
            error_code: err::NONE,
            api_keys,
            ..Default::default()
        };
        body(&resp, v)
    }

    fn sasl_handshake(
        &self,
        c: &mut Conn,
        q: &SaslHandshakeRequest,
        v: i16,
    ) -> Result<Reply, ConnError> {
        let offered = self.offered(&c.info);
        let mut resp = SaslHandshakeResponse {
            error_code: err::NONE,
            mechanisms: offered.clone(),
            ..Default::default()
        };
        if !matches!(c.sasl, SaslState::Start) {
            resp.error_code = err::ILLEGAL_SASL_STATE;
            return body(&resp, v);
        }
        let mech = if !offered.contains(&q.mechanism) {
            None
        } else if q.mechanism == "PLAIN" {
            Some(Mech::Plain)
        } else {
            ScramMechanism::from_name(&q.mechanism).map(Mech::Scram)
        };
        match mech {
            None => resp.error_code = err::UNSUPPORTED_SASL_MECHANISM,
            Some(mech) => c.sasl = SaslState::Handshaken { mech, raw: v == 0 },
        }
        body(&resp, v)
    }

    async fn sasl_authenticate(
        &self,
        c: &mut Conn,
        q: SaslAuthenticateRequest,
        v: i16,
    ) -> Result<Reply, ConnError> {
        if !matches!(
            c.sasl,
            SaslState::Handshaken { raw: false, .. } | SaslState::ScramFinal { raw: false, .. }
        ) {
            let resp = SaslAuthenticateResponse {
                error_code: err::ILLEGAL_SASL_STATE,
                error_message: Some("SaslAuthenticate without SaslHandshake v1".into()),
                ..Default::default()
            };
            let b = resp
                .to_bytes(v)
                .map_err(|e| ConnError::Protocol(e.to_string()))?;
            return Ok(Reply::BodyThenClose(b, "SASL state".into()));
        }
        match self.sasl_step(c, &q.auth_bytes).await {
            Ok(token) => body(
                &SaslAuthenticateResponse {
                    error_code: err::NONE,
                    error_message: None,
                    auth_bytes: token,
                    session_lifetime_ms: 0,
                    ..Default::default()
                },
                v,
            ),
            Err(msg) => {
                let resp = SaslAuthenticateResponse {
                    error_code: err::SASL_AUTHENTICATION_FAILED,
                    error_message: Some(msg.clone()),
                    ..Default::default()
                };
                let b = resp
                    .to_bytes(v)
                    .map_err(|e| ConnError::Protocol(e.to_string()))?;
                Ok(Reply::BodyThenClose(b, msg))
            }
        }
    }

    /// One SASL token in, one out. `Err` is the failure message (the
    /// connection then closes).
    async fn sasl_step(&self, c: &mut Conn, token: &[u8]) -> Result<Vec<u8>, String> {
        let failed = |m: &str| format!("39953 ER_SPRIG_KAFKA_SASL_FAILED: {m}");
        match std::mem::replace(&mut c.sasl, SaslState::Start) {
            SaslState::Handshaken {
                mech: Mech::Plain, ..
            } => {
                let creds = parse_plain(token).map_err(|SaslError(m)| failed(&m))?;
                let p = self
                    .identity
                    .plain(&c.info, creds)
                    .await
                    .map_err(|AuthFailure(m)| failed(&m))?;
                tracing::debug!(tenant = %p.tenant, user = %p.user, "kafka PLAIN login");
                c.principal = Some(p);
                c.sasl = SaslState::Done;
                Ok(Vec::new())
            }
            SaslState::Handshaken {
                mech: Mech::Scram(m),
                raw,
            } => {
                let mut server = ScramServer::new(m);
                let user = server
                    .client_first(token)
                    .map_err(|SaslError(e)| failed(&e))?;
                let lookup = self.identity.scram(&c.info, m, &user).await;
                let first = server.server_first(lookup.credential);
                c.sasl = SaslState::ScramFinal {
                    server: Box::new(server),
                    finish: lookup.finish,
                    raw,
                };
                Ok(first)
            }
            SaslState::ScramFinal {
                mut server, finish, ..
            } => {
                let res = server.client_final(token);
                let p = finish
                    .finish(res.is_ok())
                    .await
                    .map_err(|AuthFailure(m)| failed(&m))?;
                let fin = res.map_err(|SaslError(m)| failed(&m))?;
                tracing::debug!(tenant = %p.tenant, user = %p.user, mechanism = server.mechanism().name(), "kafka SCRAM login");
                c.principal = Some(p);
                c.sasl = SaslState::Done;
                Ok(fin)
            }
            SaslState::Start | SaslState::Done => Err(failed("unexpected SASL token")),
        }
    }
}

/// Rendezvous (highest random weight) hashing: the node with the highest
/// `xxh3(key, node id)`, so a key's choice moves only when its node comes
/// or goes.
#[must_use]
pub fn rendezvous<'a>(key: &[u8], nodes: &'a [BrokerNode]) -> Option<&'a BrokerNode> {
    nodes.iter().max_by_key(|n| {
        let mut b = Vec::with_capacity(key.len() + 4);
        b.extend_from_slice(key);
        b.extend_from_slice(&n.node_id.to_be_bytes());
        (twox_hash::XxHash3_64::oneshot(&b), n.node_id)
    })
}

/// The rendezvous key of a partition's leader.
#[must_use]
pub fn partition_key(stream_id: u64, partition: i32) -> Vec<u8> {
    let mut k = stream_id.to_be_bytes().to_vec();
    k.extend_from_slice(&partition.to_be_bytes());
    k
}

/// The rendezvous key of a group's coordinator.
#[must_use]
pub fn group_key(tenant: u64, group: &str) -> Vec<u8> {
    let mut k = tenant.to_be_bytes().to_vec();
    k.push(0);
    k.extend_from_slice(group.as_bytes());
    k
}
