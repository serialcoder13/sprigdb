//! The in-process server the client tests share: `sprig-proto-amqp`'s
//! front end over the queue engine with the in-memory catalog.

#![allow(dead_code, unreachable_pub, clippy::unwrap_used, missing_docs)]

use std::sync::Arc;

use async_trait::async_trait;
use sprig_common::CancellationToken;
use sprig_proto_amqp::auth::{AuthFailure, ConnInfo, Credentials, Identity, Principal};
use sprig_proto_amqp::mem::Perm;
use sprig_proto_amqp::reply_to::DirectReplyTo;
use sprig_proto_amqp::server::{Config, Services, serve};
use sprig_queues::engine::{EngineDeps, NoTemps, QueueEngine};
use sprig_queues::mem::{MemCatalog, MemLeases};
use sprig_queues::owner::SystemClock;
use sprig_queues::settings::QueueSettings;
use tokio::net::TcpListener;

struct TestIdentity;

#[async_trait]
impl Identity for TestIdentity {
    async fn login(&self, _c: &ConnInfo, creds: Credentials) -> Result<Principal, AuthFailure> {
        let Credentials::Password { user, password, .. } = creds else {
            return Err(AuthFailure("no".into()));
        };
        if !matches!(user.as_str(), "acme.app" | "acme.ro") || password != b"secret" {
            return Err(AuthFailure("bad credentials".into()));
        }
        Ok(Principal {
            tenant_id: 42,
            tenant: "acme".into(),
            user: user.trim_start_matches("acme.").into(),
            user_id: Some(if user == "acme.app" { 1 } else { 2 }),
            handle: None,
        })
    }
}

pub fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

/// A running in-process server.
pub struct Server {
    pub addr: String,
    pub user: String,
    pub pass: String,
    pub catalog: Option<Arc<MemCatalog>>,
    /// The in-process engine (`None` against a gateway).
    pub engine: Option<Arc<QueueEngine>>,
}

/// The server: `SPRIG_AMQP_ADDR`'s gateway, or one in process with
/// `settings` (user `acme.ro` may only read).
pub async fn server_with(settings: QueueSettings) -> Server {
    if let Some(addr) = var("SPRIG_AMQP_ADDR") {
        return Server {
            addr,
            user: var("SPRIG_AMQP_USER").unwrap(),
            pass: var("SPRIG_AMQP_PASSWORD").unwrap(),
            catalog: None,
            engine: None,
        };
    }
    let catalog = Arc::new(MemCatalog::new(
        Arc::new(|_, v| Ok(if v == "/" { "mq".into() } else { v.to_owned() })),
        Arc::new(|p, perm, _| p.user != "ro" || perm == Perm::Read),
    ));
    catalog.set_settings(settings);
    let engine = Arc::new(QueueEngine::new(EngineDeps {
        catalog: Arc::clone(&catalog) as Arc<dyn sprig_queues::engine::MqCatalog>,
        leases: Arc::new(MemLeases::new().for_gateway("gw-test")),
        temps: Arc::new(NoTemps),
        clock: Arc::new(SystemClock),
        slot: 1,
        peers: None,
        placement: None,
    }));
    let svc = Services {
        identity: Arc::new(TestIdentity),
        backend: Arc::clone(&engine) as Arc<dyn sprig_proto_amqp::backend::QueueBackend>,
        replies: DirectReplyTo::new(1),
    };
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (s, peer) = l.accept().await.unwrap();
            let svc = svc.clone();
            tokio::spawn(async move {
                let info = ConnInfo {
                    peer,
                    tls: false,
                    sni: None,
                    cert_login: None,
                };
                let _ = serve(s, info, &svc, Config::default(), CancellationToken::new()).await;
            });
        }
    });
    Server {
        addr,
        user: "acme.app".into(),
        pass: "secret".into(),
        catalog: Some(catalog),
        engine: Some(engine),
    }
}

/// [`server_with`] the default settings.
pub async fn server() -> Server {
    server_with(QueueSettings::default()).await
}

pub fn uniq(p: &str) -> String {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros();
    format!("r9queues-{p}-{t}")
}

pub async fn lapin_to(s: &Server, user: &str, pass: &str) -> lapin::Connection {
    lapin::Connection::connect(
        &format!("amqp://{user}:{pass}@{}/%2f", s.addr),
        lapin::ConnectionProperties::default(),
    )
    .await
    .unwrap()
}

pub async fn lapin(s: &Server) -> lapin::Connection {
    lapin_to(s, &s.user, &s.pass).await
}
