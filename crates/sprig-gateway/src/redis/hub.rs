//! Chapter 40's pub/sub hubs over the peer service: the transport of
//! `sprig_redis::fabric` (`sprig.internal.v1.RedisPeer` on 7403, mutual TLS
//! as every peer call).
//!
//! - **Members** are the DC's registered gateways in state `ready` that
//!   serve the peer service, read from `gateways/` every second, plus this
//!   gateway. Every gateway computes the same hub from the same list.
//! - **Off** when this gateway's peer service is off (no `[tls]` and a
//!   non-loopback `listener.peer.bind`): pub/sub and wake-ups then stay on
//!   this gateway, as before.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_api::proto::internal::redis_peer_client::RedisPeerClient;
use sprig_api::proto::internal::redis_peer_server::{RedisPeer, RedisPeerServer};
use sprig_api::proto::internal::{
    RedisChannelCount, RedisInterest, RedisInterestReply, RedisInterestRequest,
    RedisIntrospectReply, RedisIntrospectRequest, RedisPublishReply, RedisPublishRequest,
    RedisWakeReply, RedisWakeRequest,
};
use sprig_common::CancellationToken;
use sprig_meta::MetaStore;
use sprig_redis::fabric::{
    Interest, InterestMsg, Introspection, Mesh, MeshOpts, PublishMsg, Transport, WakeMsg,
};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::peer::PeerTls;
use crate::registry::{self, GatewayRecord, GatewayState};

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// How often the member list is re-read.
const MEMBERS_EVERY: Duration = Duration::from_secs(1);

/// The peer service as the hubs' transport.
pub struct PeerTransport {
    me: String,
    store: Arc<dyn MetaStore>,
    tls: Option<PeerTls>,
    members: Mutex<BTreeMap<String, GatewayRecord>>,
    channels: Mutex<HashMap<String, Channel>>,
}

impl std::fmt::Debug for PeerTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerTransport")
            .field("me", &self.me)
            .finish_non_exhaustive()
    }
}

impl PeerTransport {
    fn new(me: String, store: Arc<dyn MetaStore>, tls: Option<PeerTls>) -> Self {
        Self {
            me,
            store,
            tls,
            members: Mutex::new(BTreeMap::new()),
            channels: Mutex::new(HashMap::new()),
        }
    }

    /// Re-reads the DC's ready gateways.
    async fn load_members(&self) {
        match registry::list(self.store.as_ref()).await {
            Ok(all) => {
                let m: BTreeMap<String, GatewayRecord> = all
                    .into_iter()
                    .filter(|r| {
                        r.gateway_id == self.me
                            || (r.state == GatewayState::Ready && r.listeners.contains_key("peer"))
                    })
                    .map(|r| (r.gateway_id.clone(), r))
                    .collect();
                *lock(&self.members) = m;
            }
            Err(e) => tracing::debug!(error = %e, "redis hubs: gateway list unreadable"),
        }
    }

    fn client(&self, to: &str) -> Result<RedisPeerClient<Channel>, String> {
        let rec = lock(&self.members)
            .get(to)
            .cloned()
            .ok_or_else(|| format!("gateway {to} is not a member"))?;
        let port = rec
            .listeners
            .get("peer")
            .ok_or_else(|| format!("gateway {to} serves no peer service"))?;
        let scheme = if self.tls.is_some() { "https" } else { "http" };
        let uri = format!("{scheme}://{}:{port}", rec.addr);
        if let Some(c) = lock(&self.channels).get(&uri) {
            return Ok(RedisPeerClient::new(c.clone()));
        }
        let mut ep = Endpoint::from_shared(uri.clone())
            .map_err(|e| e.to_string())?
            .connect_timeout(Duration::from_secs(2))
            .http2_keep_alive_interval(Duration::from_secs(5))
            .keep_alive_timeout(Duration::from_secs(5))
            .keep_alive_while_idle(true)
            .tcp_nodelay(true);
        if let Some(t) = &self.tls {
            let cfg = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(&t.ca_pem))
                .identity(Identity::from_pem(&t.cert_pem, &t.key_pem))
                .domain_name(rec.addr.clone());
            ep = ep.tls_config(cfg).map_err(|e| e.to_string())?;
        }
        let ch = ep.connect_lazy();
        lock(&self.channels).insert(uri, ch.clone());
        Ok(RedisPeerClient::new(ch))
    }
}

fn interest_pb(i: &Interest) -> RedisInterest {
    let (kind, name, db) = match i {
        Interest::Channel(c) => (0, c.to_vec(), 0),
        Interest::Pattern(c) => (1, c.to_vec(), 0),
        Interest::Shard(c) => (2, c.to_vec(), 0),
        Interest::Wait(db, k) => (3, k.to_vec(), *db),
    };
    RedisInterest { kind, name, db }
}

fn interest_of(p: RedisInterest) -> Option<Interest> {
    let n = Bytes::from(p.name);
    Some(match p.kind {
        0 => Interest::Channel(n),
        1 => Interest::Pattern(n),
        2 => Interest::Shard(n),
        3 => Interest::Wait(p.db, n),
        _ => return None,
    })
}

fn publish_pb(m: PublishMsg) -> RedisPublishRequest {
    RedisPublishRequest {
        tenant: m.scope,
        channel: m.channel.to_vec(),
        data: m.data.to_vec(),
        shard: m.shard,
    }
}

fn publish_of(r: RedisPublishRequest) -> PublishMsg {
    PublishMsg {
        scope: r.tenant,
        channel: Bytes::from(r.channel),
        data: Bytes::from(r.data),
        shard: r.shard,
    }
}

#[async_trait]
impl Transport for PeerTransport {
    fn me(&self) -> String {
        self.me.clone()
    }

    fn members(&self) -> Vec<String> {
        let mut v: Vec<String> = lock(&self.members).keys().cloned().collect();
        if !v.contains(&self.me) {
            v.push(self.me.clone());
            v.sort();
        }
        v
    }

    async fn interest(&self, to: &str, m: InterestMsg) -> Result<(), String> {
        let req = RedisInterestRequest {
            tenant: m.scope,
            hub_key: m.hub_key,
            from_gateway: m.from,
            full: m.full,
            add: m.add.iter().map(interest_pb).collect(),
            remove: m.remove.iter().map(interest_pb).collect(),
        };
        self.client(to)?
            .interest(req)
            .await
            .map(|_| ())
            .map_err(|e| e.message().to_owned())
    }

    async fn publish(&self, to: &str, m: PublishMsg) -> Result<i64, String> {
        self.client(to)?
            .publish(publish_pb(m))
            .await
            .map(|r| r.into_inner().receivers)
            .map_err(|e| e.message().to_owned())
    }

    async fn deliver(&self, to: &str, m: PublishMsg) -> Result<i64, String> {
        self.client(to)?
            .deliver(publish_pb(m))
            .await
            .map(|r| r.into_inner().receivers)
            .map_err(|e| e.message().to_owned())
    }

    async fn wake(&self, to: &str, m: WakeMsg) -> Result<bool, String> {
        let req = RedisWakeRequest {
            tenant: m.scope,
            db: m.db,
            keys: m.keys.iter().map(|k| k.to_vec()).collect(),
            forward: m.forward,
            from_gateway: m.from,
            appeared: m.appeared,
        };
        self.client(to)?
            .wake(req)
            .await
            .map(|r| r.into_inner().waiters)
            .map_err(|e| e.message().to_owned())
    }

    async fn introspect(
        &self,
        to: &str,
        scope: &str,
        shard: bool,
    ) -> Result<Introspection, String> {
        let r = self
            .client(to)?
            .introspect(RedisIntrospectRequest {
                tenant: scope.to_owned(),
                shard,
            })
            .await
            .map_err(|e| e.message().to_owned())?
            .into_inner();
        Ok(Introspection {
            channels: r
                .channels
                .into_iter()
                .map(|c| (Bytes::from(c.channel), c.subscribers))
                .collect(),
            patterns: r.patterns.into_iter().map(Bytes::from).collect(),
        })
    }
}

/// This gateway's side of the hubs: the `RedisPeer` service.
#[derive(Clone)]
pub struct RedisPeerService {
    mesh: Arc<Mesh>,
    transport: Arc<PeerTransport>,
}

impl std::fmt::Debug for RedisPeerService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPeerService").finish_non_exhaustive()
    }
}

impl RedisPeerService {
    /// The service and its mesh, not yet joined to the front end (see
    /// [`Self::start`]).
    #[must_use]
    pub fn new(
        gateway_id: String,
        store: Arc<dyn MetaStore>,
        tls: Option<PeerTls>,
        opts: MeshOpts,
    ) -> Self {
        let transport = Arc::new(PeerTransport::new(gateway_id, store, tls));
        let mesh = Mesh::new(Arc::clone(&transport) as Arc<dyn Transport>, opts);
        Self { mesh, transport }
    }

    /// The tonic service.
    #[must_use]
    pub fn server(self) -> RedisPeerServer<Self> {
        RedisPeerServer::new(self)
            .max_decoding_message_size(64 << 20)
            .max_encoding_message_size(64 << 20)
    }

    /// Joins the Redis front end's state to the hubs and keeps the member
    /// list and this gateway's interests current until `stop`. Called once
    /// the peer service listens.
    pub fn start(&self, env: &Arc<sprig_redis::Env>, stop: CancellationToken) {
        self.mesh.attach(env);
        let t = Arc::clone(&self.transport);
        let s = stop.clone();
        tokio::spawn(async move {
            loop {
                t.load_members().await;
                tokio::select! {
                    () = s.cancelled() => return,
                    () = tokio::time::sleep(MEMBERS_EVERY) => {}
                }
            }
        });
        let m = Arc::clone(&self.mesh);
        tokio::spawn(m.run(async move { stop.cancelled().await }));
        tracing::info!("redis pub/sub hubs on the peer service");
    }
}

#[async_trait]
impl RedisPeer for RedisPeerService {
    async fn interest(
        &self,
        req: tonic::Request<RedisInterestRequest>,
    ) -> Result<tonic::Response<RedisInterestReply>, tonic::Status> {
        let r = req.into_inner();
        self.mesh.on_interest(&InterestMsg {
            scope: r.tenant,
            hub_key: r.hub_key,
            from: r.from_gateway,
            full: r.full,
            add: r.add.into_iter().filter_map(interest_of).collect(),
            remove: r.remove.into_iter().filter_map(interest_of).collect(),
        });
        Ok(tonic::Response::new(RedisInterestReply {}))
    }

    async fn publish(
        &self,
        req: tonic::Request<RedisPublishRequest>,
    ) -> Result<tonic::Response<RedisPublishReply>, tonic::Status> {
        let receivers = self.mesh.on_publish(publish_of(req.into_inner())).await;
        metrics::counter!("sprig_redis_pubsub_messages_total", "kind" => "hub").increment(1);
        Ok(tonic::Response::new(RedisPublishReply { receivers }))
    }

    async fn deliver(
        &self,
        req: tonic::Request<RedisPublishRequest>,
    ) -> Result<tonic::Response<RedisPublishReply>, tonic::Status> {
        let receivers = self.mesh.on_deliver(&publish_of(req.into_inner()));
        Ok(tonic::Response::new(RedisPublishReply { receivers }))
    }

    async fn wake(
        &self,
        req: tonic::Request<RedisWakeRequest>,
    ) -> Result<tonic::Response<RedisWakeReply>, tonic::Status> {
        let r = req.into_inner();
        let forward = r.forward;
        let waiters = self
            .mesh
            .on_wake(WakeMsg {
                scope: r.tenant,
                db: r.db,
                keys: r.keys.into_iter().map(Bytes::from).collect(),
                forward: r.forward,
                from: r.from_gateway,
                appeared: r.appeared,
            })
            .await;
        if !forward {
            metrics::counter!("sprig_redis_block_wakeups_total", "source" => "peer").increment(1);
        }
        Ok(tonic::Response::new(RedisWakeReply { waiters }))
    }

    async fn introspect(
        &self,
        req: tonic::Request<RedisIntrospectRequest>,
    ) -> Result<tonic::Response<RedisIntrospectReply>, tonic::Status> {
        let r = req.into_inner();
        let i = self.mesh.on_introspect(&r.tenant, r.shard);
        Ok(tonic::Response::new(RedisIntrospectReply {
            channels: i
                .channels
                .into_iter()
                .map(|(c, n)| RedisChannelCount {
                    channel: c.to_vec(),
                    subscribers: n,
                })
                .collect(),
            patterns: i.patterns.into_iter().map(|p| p.to_vec()).collect(),
        }))
    }
}
