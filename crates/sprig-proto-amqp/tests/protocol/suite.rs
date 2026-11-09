// The protocol suite, shared by the engines (chapter 50 step 21).
//
// Included by `sprig-proto-amqp/tests/protocol.rs` (the in-memory engine)
// and `sprig-queues/tests/protocol.rs` (the queue engine), each of which
// defines `engine_under_test` and the `Blocker` type before including it.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_common::CancellationToken;
use sprig_proto_amqp::auth::{AuthFailure, ConnInfo, Credentials, Identity, Principal};
use sprig_proto_amqp::mem::Perm;
use sprig_proto_amqp::reply_to::DirectReplyTo;
use sprig_proto_amqp::server::{Config, Services, serve};
use sprig_proto_amqp::v10::client::{Client as Client10, ClientError as Client10Error};
use sprig_proto_amqp::v10::message::{
    Body, Properties, Sections, application_properties_section, data_section, encode_sections,
};
use sprig_proto_amqp::v10::performative::{DeliveryState, Performative};
use sprig_proto_amqp::v10::types::Value;
use sprig_proto_amqp::v091::client::{Client, ClientError, Incoming, default_capabilities, plain};
use sprig_proto_amqp::v091::codec::{FieldTable, FieldValue};
use sprig_proto_amqp::v091::method::*;
use sprig_proto_amqp::v091::props::BasicProperties;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

struct TestIdentity;

#[async_trait]
impl Identity for TestIdentity {
    async fn login(&self, _c: &ConnInfo, creds: Credentials) -> Result<Principal, AuthFailure> {
        let (user, ok) = match creds {
            Credentials::Password { user, password, .. } => {
                let ok = matches!(
                    (user.as_str(), password.as_slice()),
                    ("acme.app" | "acme.ro", b"secret")
                );
                (user, ok)
            }
            Credentials::External { login } => (login, true),
        };
        if !ok {
            return Err(AuthFailure("bad credentials".into()));
        }
        let (tenant, user) = user.split_once('.').unwrap_or(("acme", &user));
        Ok(Principal {
            tenant_id: 42,
            tenant: tenant.into(),
            user: user.into(),
            user_id: Some(1),
            handle: None,
        })
    }
}

struct Env {
    addr: String,
    user: String,
    pass: String,
    live: bool,
    /// Sends `connection.blocked` (true) or unblocked (false) to every
    /// connection of the engine under test.
    blocker: Option<Blocker>,
    _stop: Option<tokio::sync::oneshot::Sender<()>>,
}

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

async fn env() -> Env {
    env_with(Config::default()).await
}

async fn env_with(mut cfg: Config) -> Env {
    if let Some(addr) = var("SPRIG_AMQP_ADDR") {
        return Env {
            addr,
            user: var("SPRIG_AMQP_USER").unwrap(),
            pass: var("SPRIG_AMQP_PASSWORD").unwrap(),
            live: true,
            blocker: None,
            _stop: None,
        };
    }
    let (backend, blocker) = engine_under_test(
        Arc::new(|_, v| {
            if v == "/" || v == "other" {
                Ok(v.to_owned())
            } else {
                Err(sprig_proto_amqp::error::MqError::not_found(format!(
                    "vhost {v} not found"
                )))
            }
        }),
        Arc::new(|p, perm, _| p.user != "ro" || perm == Perm::Read),
    );
    let svc = Services {
        identity: Arc::new(TestIdentity),
        backend,
        replies: DirectReplyTo::new(1),
    };
    cfg.handshake_timeout = Duration::from_secs(3);
    if cfg.max_message_bytes == Config::default().max_message_bytes {
        cfg.max_message_bytes = 1 << 20;
    }
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => return,
                r = l.accept() => {
                    let (s, peer) = r.unwrap();
                    let svc = svc.clone();
                    let cfg = cfg.clone();
                    tokio::spawn(async move {
                        let info = ConnInfo { peer, tls: false, sni: None, cert_login: None };
                        let _ = serve(s, info, &svc, cfg, CancellationToken::new()).await;
                    });
                }
            }
        }
    });
    Env {
        addr,
        user: "acme.app".into(),
        pass: "secret".into(),
        live: false,
        blocker: Some(blocker),
        _stop: Some(stop_tx),
    }
}

fn uniq(p: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros();
    format!("r9amqp-{p}-{t}-{}", N.fetch_add(1, Ordering::Relaxed))
}

impl Env {
    async fn tcp(&self) -> TcpStream {
        TcpStream::connect(&self.addr).await.unwrap()
    }

    async fn connect(&self) -> Client<TcpStream> {
        Client::connect(
            self.tcp().await,
            "PLAIN",
            plain(&self.user, &self.pass),
            "/",
            default_capabilities(),
            None,
        )
        .await
        .unwrap()
    }

    async fn channel(&self, ch: u16) -> Client<TcpStream> {
        let mut c = self.connect().await;
        open(&mut c, ch).await;
        c
    }

    async fn connect10(&self) -> Client10<TcpStream> {
        let mut c = Client10::connect(
            self.tcp().await,
            "PLAIN",
            Some(plain(&self.user, &self.pass)),
            Some("vhost:/"),
        )
        .await
        .unwrap();
        c.begin(0).await.unwrap();
        c
    }
}

async fn open(c: &mut Client<TcpStream>, ch: u16) {
    let m = c.call(ch, ChannelOpen::default()).await.unwrap();
    assert!(matches!(m, Method::ChannelOpenOk(_)), "{m:?}");
}

async fn declare_queue(
    c: &mut Client<TcpStream>,
    ch: u16,
    name: &str,
    args: FieldTable,
) -> QueueDeclareOk {
    match c
        .call(
            ch,
            QueueDeclare {
                queue: name.into(),
                auto_delete: false,
                durable: args
                    .get("x-queue-type")
                    .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned))
                    .as_deref()
                    == Some("quorum"),
                arguments: args,
                ..Default::default()
            },
        )
        .await
        .unwrap()
    {
        Method::QueueDeclareOk(ok) => ok,
        other => panic!("{other:?}"),
    }
}

async fn delete_queue(c: &mut Client<TcpStream>, ch: u16, name: &str) {
    let m = c
        .call(
            ch,
            QueueDelete {
                queue: name.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(matches!(m, Method::QueueDeleteOk(_)), "{m:?}");
}

fn text(p: &str) -> BasicProperties {
    BasicProperties {
        content_type: Some(p.into()),
        ..Default::default()
    }
}

async fn expect_channel_close(c: &mut Client<TcpStream>, ch: u16, code: u16) -> String {
    let i = c.next_method(ch).await.unwrap();
    let Method::ChannelClose(cl) = i.method else {
        panic!("expected channel.close, got {:?}", i.method)
    };
    assert_eq!(cl.reply_code, code, "{}", cl.reply_text);
    c.send(ch, ChannelCloseOk {}).await.unwrap();
    cl.reply_text
}

async fn expect_connection_close(c: &mut Client<TcpStream>, code: u16) -> String {
    loop {
        match c.recv().await {
            Err(ClientError::Closed(got, t)) => {
                assert_eq!(got, code, "{t}");
                return t;
            }
            Ok(_) => {}
            Err(e) => panic!("expected connection.close {code}, got {e}"),
        }
    }
}

async fn deliver(c: &mut Client<TcpStream>) -> (BasicDeliver, BasicProperties, Bytes) {
    loop {
        let i = c.recv().await.unwrap();
        if let Method::BasicDeliver(d) = i.method {
            let (p, b) = i.content.unwrap();
            return (d, p, b);
        }
    }
}

/// No delivery arrives within `ms`.
async fn quiet(c: &mut Client<TcpStream>, ms: u64) {
    c.timeout = Duration::from_millis(ms);
    match c.recv().await {
        Err(ClientError::Timeout) => {}
        Ok(i) => panic!("unexpected {:?}", i.method),
        Err(e) => panic!("{e}"),
    }
    c.timeout = Duration::from_secs(10);
}

// ------------------------------------------------------------------
// Sniffing and the handshake.
// ------------------------------------------------------------------

#[tokio::test]
async fn protocol_headers() {
    let e = env().await;
    for (sent, want) in [
        (&b"GET / HTTP/1.1\r\n"[..], &b"AMQP\x00\x00\x09\x01"[..]),
        (b"AMQP\x01\x01\x00\x09", b"AMQP\x00\x00\x09\x01"),
        (b"AMQP\x00\x01\x00\x00", b"AMQP\x03\x01\x00\x00"),
        (b"AMQP\x02\x01\x00\x00", b"AMQP\x03\x01\x00\x00"),
    ] {
        let mut s = e.tcp().await;
        s.write_all(sent).await.unwrap();
        let mut got = [0u8; 8];
        s.read_exact(&mut got).await.unwrap();
        assert_eq!(&got[..], want, "for {sent:?}");
        // Then the server closes.
        let mut rest = Vec::new();
        let n = tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut rest))
            .await
            .unwrap()
            .unwrap_or(0);
        assert_eq!(n, 0);
    }
}

#[tokio::test]
async fn handshake_and_capabilities() {
    let e = env().await;
    let c = e.connect().await;
    let Some(FieldValue::Table(caps)) = c.server_properties.get("capabilities") else {
        panic!("no capabilities")
    };
    for k in [
        "publisher_confirms",
        "exchange_exchange_bindings",
        "basic.nack",
        "consumer_cancel_notify",
        "connection.blocked",
        "authentication_failure_close",
        "per_consumer_qos",
        "direct_reply_to",
    ] {
        assert_eq!(caps.get(k), Some(&FieldValue::Bool(true)), "{k}");
    }
    assert!(
        c.mechanisms.contains("PLAIN") && c.mechanisms.contains("AMQPLAIN"),
        "{}",
        c.mechanisms
    );
    assert!(
        !c.mechanisms.contains("EXTERNAL"),
        "EXTERNAL needs a client certificate"
    );
    assert_eq!(c.frame_max, 131_072);
    c.close().await.unwrap();
}

#[tokio::test]
async fn amqplain_login() {
    let e = env().await;
    let t = FieldTable::from([
        ("LOGIN", FieldValue::str(&e.user)),
        ("PASSWORD", FieldValue::str(&e.pass)),
    ]);
    let mut b = bytes::BytesMut::new();
    t.encode_entries(&mut b);
    let c = Client::connect(
        e.tcp().await,
        "AMQPLAIN",
        b.freeze(),
        "/",
        default_capabilities(),
        None,
    )
    .await
    .unwrap();
    c.close().await.unwrap();
}

#[tokio::test]
async fn refused_logins() {
    let e = env().await;
    // With authentication_failure_close: connection.close 403.
    let r = Client::connect(
        e.tcp().await,
        "PLAIN",
        plain(&e.user, "wrong"),
        "/",
        default_capabilities(),
        None,
    )
    .await;
    match r {
        Err(ClientError::Closed(403, t)) => {
            assert!(
                t.starts_with(
                    "ACCESS_REFUSED - Login was refused using authentication mechanism PLAIN"
                ),
                "{t}"
            );
            assert!(t.contains("39269"), "{t}");
        }
        other => panic!("{other:?}"),
    }
    // Without it: the socket just closes.
    let r = Client::connect(
        e.tcp().await,
        "PLAIN",
        plain(&e.user, "wrong"),
        "/",
        FieldTable::new(),
        None,
    )
    .await;
    assert!(matches!(r, Err(ClientError::Io(_))), "{r:?}");
    // A mechanism that isn't offered.
    let r = Client::connect(
        e.tcp().await,
        "ANONYMOUS",
        Bytes::new(),
        "/",
        default_capabilities(),
        None,
    )
    .await;
    assert!(matches!(r, Err(ClientError::Closed(403, _))), "{r:?}");
    // Unknown vhost: 530.
    let r = Client::connect(
        e.tcp().await,
        "PLAIN",
        plain(&e.user, &e.pass),
        "no-such-vhost-r9amqp",
        default_capabilities(),
        None,
    )
    .await;
    assert!(matches!(r, Err(ClientError::Closed(530, _))), "{r:?}");
}

#[tokio::test]
async fn tune_negotiation() {
    let e = env().await;
    // frame_max below 4096: refused with 530.
    let mut c = Client::raw(e.tcp().await).await.unwrap();
    assert!(matches!(
        c.next_method(0).await.unwrap().method,
        Method::ConnectionStart(_)
    ));
    let mut caps = FieldTable::new();
    caps.insert("capabilities", FieldValue::Table(default_capabilities()));
    c.send(
        0,
        ConnectionStartOk {
            client_properties: caps,
            mechanism: "PLAIN".into(),
            response: plain(&e.user, &e.pass),
            locale: "en_US".into(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        c.next_method(0).await.unwrap().method,
        Method::ConnectionTune(_)
    ));
    c.send(
        0,
        ConnectionTuneOk {
            channel_max: 10,
            frame_max: 1024,
            heartbeat: 0,
        },
    )
    .await
    .unwrap();
    let t = expect_connection_close(&mut c, 530).await;
    assert!(t.contains("4096"), "{t}");
    // Channels above the negotiated channel_max: 530.
    let mut c = Client::raw(e.tcp().await).await.unwrap();
    c.next_method(0).await.unwrap();
    let mut caps = FieldTable::new();
    caps.insert("capabilities", FieldValue::Table(default_capabilities()));
    c.send(
        0,
        ConnectionStartOk {
            client_properties: caps,
            mechanism: "PLAIN".into(),
            response: plain(&e.user, &e.pass),
            locale: "en_US".into(),
        },
    )
    .await
    .unwrap();
    c.next_method(0).await.unwrap();
    c.send(
        0,
        ConnectionTuneOk {
            channel_max: 2,
            frame_max: 8192,
            heartbeat: 0,
        },
    )
    .await
    .unwrap();
    c.call(
        0,
        ConnectionOpen {
            virtual_host: "/".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    c.frame_max = 8192;
    open(&mut c, 2).await;
    c.send(3, ChannelOpen::default()).await.unwrap();
    expect_connection_close(&mut c, 530).await;
}

#[tokio::test]
async fn heartbeats_are_sent() {
    let e = env().await;
    let mut c = Client::connect(
        e.tcp().await,
        "PLAIN",
        plain(&e.user, &e.pass),
        "/",
        default_capabilities(),
        Some(2),
    )
    .await
    .unwrap();
    // The server sends heartbeats about every second; the client answers.
    let start = std::time::Instant::now();
    c.timeout = Duration::from_millis(500);
    while start.elapsed() < Duration::from_secs(5) {
        let _ = c.heartbeat().await;
        match c.recv().await {
            Err(ClientError::Timeout) | Ok(_) => {}
            Err(err) => panic!("connection lost with heartbeats on: {err}"),
        }
    }
    c.timeout = Duration::from_secs(10);
    open(&mut c, 1).await;
    c.close().await.unwrap();
}

#[tokio::test]
async fn missed_heartbeats_close_the_connection() {
    if var("SPRIG_AMQP_ADDR").is_some() {
        return; // 2 x 1 s of silence is the same live; skipped to keep the live run short.
    }
    let e = env().await;
    let mut c = Client::connect(
        e.tcp().await,
        "PLAIN",
        plain(&e.user, &e.pass),
        "/",
        default_capabilities(),
        Some(1),
    )
    .await
    .unwrap();
    c.timeout = Duration::from_secs(5);
    // Read without answering heartbeats: the server gives up after two
    // intervals.
    let r = loop {
        match c.recv().await {
            Ok(_) => {}
            Err(e) => break e,
        }
    };
    assert!(matches!(r, ClientError::Io(_)), "{r}");
}

// ------------------------------------------------------------------
// Frame and command errors.
// ------------------------------------------------------------------

#[tokio::test]
async fn frame_errors() {
    let e = env().await;
    // Bad frame-end: 501.
    let mut c = e.connect().await;
    c.send_raw(&[1, 0, 1, 0, 0, 0, 4, 0, 20, 0, 10, 0xAB])
        .await
        .unwrap();
    expect_connection_close(&mut c, 501).await;
    // A method on a channel that isn't open: 504.
    let mut c = e.connect().await;
    c.send(5, BasicQos::default()).await.unwrap();
    expect_connection_close(&mut c, 504).await;
    // Unknown method: 540.
    let mut c = e.connect().await;
    c.send_raw(&[1, 0, 0, 0, 0, 0, 4, 0, 99, 0, 99, 0xCE])
        .await
        .unwrap();
    expect_connection_close(&mut c, 540).await;
    // Truncated arguments: 502.
    let mut c = e.channel(1).await;
    c.send_raw(&[1, 0, 1, 0, 0, 0, 6, 0, 50, 0, 10, 0, 0, 0xCE])
        .await
        .unwrap();
    expect_connection_close(&mut c, 502).await;
    // A content header without basic.publish: 505.
    let mut c = e.channel(1).await;
    c.send_raw(&[
        2, 0, 1, 0, 0, 0, 14, 0, 60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xCE,
    ])
    .await
    .unwrap();
    expect_connection_close(&mut c, 505).await;
    // A method while content is expected: 505.
    let mut c = e.channel(1).await;
    c.send(
        1,
        BasicPublish {
            routing_key: "x".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    c.send(1, BasicQos::default()).await.unwrap();
    expect_connection_close(&mut c, 505).await;
    // A heartbeat on a channel: 501.
    let mut c = e.channel(1).await;
    c.send_raw(&[8, 0, 1, 0, 0, 0, 0, 0xCE]).await.unwrap();
    expect_connection_close(&mut c, 501).await;
    // channel.open twice: 504.
    let mut c = e.channel(1).await;
    c.send(1, ChannelOpen::default()).await.unwrap();
    expect_connection_close(&mut c, 504).await;
    // A server-only method from the client: 503.
    let mut c = e.channel(1).await;
    c.send(1, BasicGetEmpty::default()).await.unwrap();
    expect_connection_close(&mut c, 503).await;
}

#[tokio::test]
async fn rabbitmq_not_implemented_cases() {
    let e = env().await;
    let mut c = e.channel(1).await;
    c.send(
        1,
        BasicPublish {
            routing_key: "x".into(),
            immediate: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let t = expect_connection_close(&mut c, 540).await;
    assert!(t.contains("immediate=true"), "{t}");
    let mut c = e.channel(1).await;
    c.send(
        1,
        BasicQos {
            prefetch_size: 10,
            prefetch_count: 1,
            global: false,
        },
    )
    .await
    .unwrap();
    expect_connection_close(&mut c, 540).await;
    let mut c = e.channel(1).await;
    c.send(1, BasicRecover { requeue: false }).await.unwrap();
    expect_connection_close(&mut c, 540).await;
    let mut c = e.channel(1).await;
    c.send(1, ChannelFlow { active: false }).await.unwrap();
    expect_connection_close(&mut c, 540).await;
    let mut c = e.channel(1).await;
    assert!(matches!(
        c.call(1, ChannelFlow { active: true }).await.unwrap(),
        Method::ChannelFlowOk(ChannelFlowOk { active: true })
    ));
    c.close().await.unwrap();
}

// ------------------------------------------------------------------
// Topology.
// ------------------------------------------------------------------

#[tokio::test]
async fn declare_semantics() {
    let e = env().await;
    let mut c = e.channel(1).await;
    let x = uniq("x");
    let m = c
        .call(
            1,
            ExchangeDeclare {
                exchange: x.clone(),
                kind: "direct".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(matches!(m, Method::ExchangeDeclareOk(_)));
    // Same again: fine. Different type: 406.
    c.call(
        1,
        ExchangeDeclare {
            exchange: x.clone(),
            kind: "direct".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    c.send(
        1,
        ExchangeDeclare {
            exchange: x.clone(),
            kind: "fanout".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let t = expect_channel_close(&mut c, 1, 406).await;
    assert!(t.contains("inequivalent arg 'type'"), "{t}");
    open(&mut c, 1).await;
    // Passive on a missing exchange: 404.
    c.send(
        1,
        ExchangeDeclare {
            exchange: uniq("missing"),
            kind: "direct".into(),
            passive: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    expect_channel_close(&mut c, 1, 404).await;
    open(&mut c, 1).await;
    // Reserved names: 403.
    c.send(
        1,
        ExchangeDeclare {
            exchange: "amq.mine".into(),
            kind: "direct".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    expect_channel_close(&mut c, 1, 403).await;
    open(&mut c, 1).await;
    // Unknown type: 503, a connection error.
    c.send(
        1,
        ExchangeDeclare {
            exchange: uniq("t"),
            kind: "nonsense".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    expect_connection_close(&mut c, 503).await;

    let mut c = e.channel(1).await;
    // Server-named queue.
    let ok = declare_queue(&mut c, 1, "", FieldTable::new()).await;
    assert!(
        ok.queue.starts_with("amq.gen-") && ok.queue.len() == 8 + 22,
        "{}",
        ok.queue
    );
    // Queue arguments must match on redeclare.
    let q = uniq("q");
    declare_queue(
        &mut c,
        1,
        &q,
        FieldTable::from([("x-max-length", FieldValue::I32(10))]),
    )
    .await;
    c.send(
        1,
        QueueDeclare {
            queue: q.clone(),
            arguments: FieldTable::from([("x-max-length", FieldValue::I32(20))]),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let t = expect_channel_close(&mut c, 1, 406).await;
    assert!(t.contains("x-max-length"), "{t}");
    open(&mut c, 1).await;
    // Quorum queues can't be exclusive or non-durable.
    c.send(
        1,
        QueueDeclare {
            queue: uniq("qq"),
            durable: false,
            arguments: FieldTable::from([("x-queue-type", FieldValue::str("quorum"))]),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    expect_channel_close(&mut c, 1, 406).await;
    open(&mut c, 1).await;
    // Bind to a missing exchange: 404; delete of a missing queue: ok.
    c.send(
        1,
        QueueBind {
            queue: q.clone(),
            exchange: uniq("none"),
            routing_key: "k".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    expect_channel_close(&mut c, 1, 404).await;
    open(&mut c, 1).await;
    assert!(matches!(
        c.call(
            1,
            QueueDelete {
                queue: uniq("gone"),
                ..Default::default()
            }
        )
        .await
        .unwrap(),
        Method::QueueDeleteOk(QueueDeleteOk { message_count: 0 })
    ));
    // Delete if-empty with messages: 406.
    c.publish(1, "", &q, false, &text("t"), b"m").await.unwrap();
    c.send(
        1,
        QueueDelete {
            queue: q.clone(),
            if_empty: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    expect_channel_close(&mut c, 1, 406).await;
    open(&mut c, 1).await;
    let m = c
        .call(
            1,
            QueuePurge {
                queue: q.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(m, Method::QueuePurgeOk(QueuePurgeOk { message_count: 1 })),
        "{m:?}"
    );
    delete_queue(&mut c, 1, &q).await;
    c.call(
        1,
        ExchangeDelete {
            exchange: x,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    c.close().await.unwrap();
}

#[tokio::test]
async fn exclusive_queues() {
    let e = env().await;
    let mut a = e.channel(1).await;
    let q = uniq("excl");
    let m = a
        .call(
            1,
            QueueDeclare {
                queue: q.clone(),
                exclusive: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(matches!(m, Method::QueueDeclareOk(_)));
    let mut b = e.channel(1).await;
    b.send(
        1,
        QueueDeclare {
            queue: q.clone(),
            passive: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let t = expect_channel_close(&mut b, 1, 405).await;
    assert!(t.contains("exclusive"), "{t}");
    // It goes with its connection.
    a.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    open(&mut b, 1).await;
    b.send(
        1,
        QueueDeclare {
            queue: q,
            passive: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    expect_channel_close(&mut b, 1, 404).await;
}

#[tokio::test]
async fn routing_by_exchange_type() {
    let e = env().await;
    let mut c = e.channel(1).await;
    let names: Vec<String> = (0..6).map(|i| uniq(&format!("rq{i}"))).collect();
    for n in &names {
        declare_queue(&mut c, 1, n, FieldTable::new()).await;
    }
    let topic = uniq("topic");
    let fan = uniq("fan");
    let hdr = uniq("hdr");
    let e2e = uniq("e2e");
    for (x, k) in [
        (&topic, "topic"),
        (&fan, "fanout"),
        (&hdr, "headers"),
        (&e2e, "direct"),
    ] {
        c.call(
            1,
            ExchangeDeclare {
                exchange: x.clone(),
                kind: k.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    let bind = |q: &str, x: &str, k: &str, args: FieldTable| QueueBind {
        queue: q.into(),
        exchange: x.into(),
        routing_key: k.into(),
        arguments: args,
        ..Default::default()
    };
    c.call(
        1,
        bind(&names[0], &topic, "order.paid.*", FieldTable::new()),
    )
    .await
    .unwrap();
    c.call(1, bind(&names[1], &topic, "order.#", FieldTable::new()))
        .await
        .unwrap();
    c.call(1, bind(&names[2], &fan, "", FieldTable::new()))
        .await
        .unwrap();
    c.call(1, bind(&names[3], &fan, "", FieldTable::new()))
        .await
        .unwrap();
    c.call(
        1,
        bind(
            &names[4],
            &hdr,
            "",
            FieldTable::from([
                ("x-match", FieldValue::str("any")),
                ("kind", FieldValue::str("pdf")),
                ("size", FieldValue::I32(5)),
            ]),
        ),
    )
    .await
    .unwrap();
    // Exchange to exchange: e2e (direct, key "k") -> fan.
    let m = c
        .call(
            1,
            ExchangeBind {
                destination: fan.clone(),
                source: e2e.clone(),
                routing_key: "k".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(matches!(m, Method::ExchangeBindOk(_)));
    c.publish(1, &topic, "order.paid.eu", false, &text("t"), b"1")
        .await
        .unwrap();
    c.publish(1, &topic, "order.shipped", false, &text("t"), b"2")
        .await
        .unwrap();
    c.publish(1, &fan, "anything", false, &text("t"), b"3")
        .await
        .unwrap();
    let headers = BasicProperties {
        headers: Some(FieldTable::from([("size", FieldValue::I64(5))])),
        ..Default::default()
    };
    c.publish(1, &hdr, "", false, &headers, b"4").await.unwrap();
    c.publish(1, &e2e, "k", false, &text("t"), b"5")
        .await
        .unwrap();
    c.publish(1, &e2e, "other", false, &text("t"), b"6")
        .await
        .unwrap();
    // Default exchange.
    c.publish(1, "", &names[5], false, &text("t"), b"7")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut got = Vec::new();
    for n in &names {
        got.push(
            declare_queue(&mut c, 1, n, FieldTable::new())
                .await
                .message_count,
        );
    }
    assert_eq!(got, [1, 2, 2, 2, 1, 1]);
    let m = c
        .call(
            1,
            ExchangeUnbind {
                destination: fan.clone(),
                source: e2e.clone(),
                routing_key: "k".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(matches!(m, Method::ExchangeUnbindOk(_)));
    for n in &names {
        delete_queue(&mut c, 1, n).await;
    }
    for x in [topic, fan, hdr, e2e] {
        c.call(
            1,
            ExchangeDelete {
                exchange: x,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    c.close().await.unwrap();
}

#[tokio::test]
async fn alternate_exchange() {
    let e = env().await;
    let mut c = e.channel(1).await;
    let (x, alt, q) = (uniq("ax"), uniq("alt"), uniq("altq"));
    c.call(
        1,
        ExchangeDeclare {
            exchange: alt.clone(),
            kind: "fanout".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    c.call(
        1,
        ExchangeDeclare {
            exchange: x.clone(),
            kind: "direct".into(),
            arguments: FieldTable::from([("alternate-exchange", FieldValue::str(&alt))]),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    declare_queue(&mut c, 1, &q, FieldTable::new()).await;
    c.call(
        1,
        QueueBind {
            queue: q.clone(),
            exchange: alt.clone(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    c.call(1, ConfirmSelect::default()).await.unwrap();
    // Mandatory but routed through the alternate: no return.
    c.publish(1, &x, "nokey", true, &text("t"), b"a")
        .await
        .unwrap();
    let i = c.recv().await.unwrap();
    assert!(
        matches!(
            i.method,
            Method::BasicAck(BasicAck {
                delivery_tag: 1,
                ..
            })
        ),
        "{:?}",
        i.method
    );
    let m = c
        .call(
            1,
            BasicGet {
                queue: q.clone(),
                no_ack: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(matches!(m, Method::BasicGetOk(_)), "{m:?}");
    delete_queue(&mut c, 1, &q).await;
    c.call(
        1,
        ExchangeDelete {
            exchange: x,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    c.call(
        1,
        ExchangeDelete {
            exchange: alt,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    c.close().await.unwrap();
}

// ------------------------------------------------------------------
// Publishing.
// ------------------------------------------------------------------

#[tokio::test]
async fn confirms_returns_and_missing_exchanges() {
    let e = env().await;
    let mut c = e.channel(1).await;
    let q = uniq("conf");
    declare_queue(&mut c, 1, &q, FieldTable::new()).await;
    assert!(matches!(
        c.call(1, ConfirmSelect::default()).await.unwrap(),
        Method::ConfirmSelectOk(_)
    ));
    for i in 0..50u8 {
        c.publish(1, "", &q, false, &text("t"), &[i]).await.unwrap();
    }
    let mut acked = Vec::new();
    while acked.len() < 50 {
        match c.recv().await.unwrap().method {
            Method::BasicAck(a) => {
                assert!(!a.multiple);
                acked.push(a.delivery_tag);
            }
            other => panic!("{other:?}"),
        }
    }
    acked.sort_unstable();
    assert_eq!(acked, (1..=50).collect::<Vec<u64>>());
    // Mandatory and unroutable: basic.return 312, then the ack.
    c.publish(
        1,
        "amq.direct",
        "no-such-key-r9amqp",
        true,
        &text("t"),
        b"lost",
    )
    .await
    .unwrap();
    let i = c.recv().await.unwrap();
    let Method::BasicReturn(r) = i.method else {
        panic!("{:?}", i.method)
    };
    assert_eq!((r.reply_code, r.reply_text.as_str()), (312, "NO_ROUTE"));
    assert_eq!(&i.content.unwrap().1[..], b"lost");
    assert!(matches!(
        c.recv().await.unwrap().method,
        Method::BasicAck(BasicAck {
            delivery_tag: 51,
            ..
        })
    ));
    // Not mandatory: just the ack.
    c.publish(
        1,
        "amq.direct",
        "no-such-key-r9amqp",
        false,
        &text("t"),
        b"x",
    )
    .await
    .unwrap();
    assert!(matches!(
        c.recv().await.unwrap().method,
        Method::BasicAck(BasicAck {
            delivery_tag: 52,
            ..
        })
    ));
    // A missing exchange closes the channel with 404.
    c.publish(1, &uniq("nox"), "k", false, &text("t"), b"x")
        .await
        .unwrap();
    let t = expect_channel_close(&mut c, 1, 404).await;
    assert!(t.contains("no exchange"), "{t}");
    // tx and confirm don't mix.
    open(&mut c, 1).await;
    c.call(1, TxSelect {}).await.unwrap();
    c.send(1, ConfirmSelect::default()).await.unwrap();
    expect_channel_close(&mut c, 1, 406).await;
    open(&mut c, 1).await;
    delete_queue(&mut c, 1, &q).await;
    c.close().await.unwrap();
}

#[tokio::test]
async fn large_messages_and_limits() {
    let e = env().await;
    let mut c = e.channel(1).await;
    let q = uniq("big");
    declare_queue(&mut c, 1, &q, FieldTable::new()).await;
    // 600 KB: split into ~5 body frames each way.
    let body: Vec<u8> = (0..600_000u32).map(|i| (i % 251) as u8).collect();
    c.publish(1, "", &q, false, &text("bin"), &body)
        .await
        .unwrap();
    let m = c.next_method(1);
    drop(m);
    let i = loop {
        c.send(
            1,
            BasicGet {
                queue: q.clone(),
                no_ack: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let i = c.next_method(1).await.unwrap();
        if matches!(i.method, Method::BasicGetOk(_)) {
            break i;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(i.content.unwrap().1.len(), body.len());
    if !e.live {
        // In process max_message_bytes is 1 MiB: 406 for more.
        let big = vec![0u8; (1 << 20) + 1];
        c.publish(1, "", &q, false, &text("bin"), &big)
            .await
            .unwrap();
        let t = expect_channel_close(&mut c, 1, 406).await;
        assert!(t.contains("39257"), "{t}");
        open(&mut c, 1).await;
    }
    delete_queue(&mut c, 1, &q).await;
    c.close().await.unwrap();
}

#[tokio::test]
async fn transactions() {
    let e = env().await;
    let mut c = e.channel(1).await;
    let q = uniq("tx");
    declare_queue(&mut c, 1, &q, FieldTable::new()).await;
    // tx.commit without select: 406.
    c.send(1, TxCommit {}).await.unwrap();
    expect_channel_close(&mut c, 1, 406).await;
    open(&mut c, 1).await;
    assert!(matches!(
        c.call(1, TxSelect {}).await.unwrap(),
        Method::TxSelectOk(_)
    ));
    c.publish(1, "", &q, false, &text("t"), b"a").await.unwrap();
    c.publish(1, "", &q, false, &text("t"), b"b").await.unwrap();
    let mut other = e.channel(1).await;
    let n = declare_queue(&mut other, 1, &q, FieldTable::new())
        .await
        .message_count;
    assert_eq!(n, 0, "uncommitted publishes are invisible");
    assert!(matches!(
        c.call(1, TxRollback {}).await.unwrap(),
        Method::TxRollbackOk(_)
    ));
    assert!(matches!(
        c.call(1, TxCommit {}).await.unwrap(),
        Method::TxCommitOk(_)
    ));
    assert_eq!(
        declare_queue(&mut other, 1, &q, FieldTable::new())
            .await
            .message_count,
        0
    );
    c.publish(1, "", &q, false, &text("t"), b"c").await.unwrap();
    assert!(matches!(
        c.call(1, TxCommit {}).await.unwrap(),
        Method::TxCommitOk(_)
    ));
    assert_eq!(
        declare_queue(&mut other, 1, &q, FieldTable::new())
            .await
            .message_count,
        1
    );
    // Acks in a transaction apply at commit; rollback restores them.
    let m = c
        .call(
            1,
            BasicGet {
                queue: q.clone(),
                no_ack: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let Method::BasicGetOk(g) = m else {
        panic!("{m:?}")
    };
    c.send(
        1,
        BasicAck {
            delivery_tag: g.delivery_tag,
            multiple: false,
        },
    )
    .await
    .unwrap();
    c.call(1, TxRollback {}).await.unwrap();
    c.send(
        1,
        BasicAck {
            delivery_tag: g.delivery_tag,
            multiple: false,
        },
    )
    .await
    .unwrap();
    c.call(1, TxCommit {}).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        declare_queue(&mut other, 1, &q, FieldTable::new())
            .await
            .message_count,
        0
    );
    // The message is gone: closing the channel doesn't requeue it.
    c.call(
        1,
        ChannelClose {
            reply_code: 200,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        declare_queue(&mut other, 1, &q, FieldTable::new())
            .await
            .message_count,
        0
    );
    delete_queue(&mut other, 1, &q).await;
    other.close().await.unwrap();
    c.close().await.unwrap();
}

// ------------------------------------------------------------------
// Consuming.
// ------------------------------------------------------------------

#[tokio::test]
async fn consume_prefetch_ack_nack_reject() {
    let e = env().await;
    let mut c = e.channel(1).await;
    let q = uniq("work");
    declare_queue(&mut c, 1, &q, FieldTable::new()).await;
    for i in 0..5u8 {
        c.publish(1, "", &q, false, &text("t"), &[b'0' + i])
            .await
            .unwrap();
    }
    assert!(matches!(
        c.call(
            1,
            BasicQos {
                prefetch_count: 2,
                ..Default::default()
            }
        )
        .await
        .unwrap(),
        Method::BasicQosOk(_)
    ));
    let m = c
        .call(
            1,
            BasicConsume {
                queue: q.clone(),
                consumer_tag: String::new(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let Method::BasicConsumeOk(ok) = m else {
        panic!("{m:?}")
    };
    assert!(
        ok.consumer_tag.starts_with("amq.ctag-"),
        "{}",
        ok.consumer_tag
    );
    let (d1, _, b1) = deliver(&mut c).await;
    let (d2, _, b2) = deliver(&mut c).await;
    assert_eq!((&b1[..], &b2[..]), (&b"0"[..], &b"1"[..]));
    assert_eq!((d1.delivery_tag, d2.delivery_tag), (1, 2));
    assert!(!d1.redelivered);
    // Prefetch 2: nothing more until an ack.
    quiet(&mut c, 300).await;
    c.send(
        1,
        BasicAck {
            delivery_tag: 1,
            multiple: false,
        },
    )
    .await
    .unwrap();
    let (d3, _, b3) = deliver(&mut c).await;
    assert_eq!(&b3[..], b"2");
    // Nack with requeue: back at its place, redelivered.
    c.send(
        1,
        BasicNack {
            delivery_tag: d2.delivery_tag,
            multiple: false,
            requeue: true,
        },
    )
    .await
    .unwrap();
    let (d4, _, b4) = deliver(&mut c).await;
    assert_eq!(&b4[..], b"1");
    assert!(d4.redelivered);
    // Ack multiple up to d4.
    c.send(
        1,
        BasicAck {
            delivery_tag: d4.delivery_tag,
            multiple: true,
        },
    )
    .await
    .unwrap();
    let (d5, _, _) = deliver(&mut c).await;
    let (d6, _, _) = deliver(&mut c).await;
    // Reject without requeue drops it; recover requeues the other.
    c.send(
        1,
        BasicReject {
            delivery_tag: d5.delivery_tag,
            requeue: false,
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        c.call(1, BasicRecover { requeue: true }).await.unwrap(),
        Method::BasicRecoverOk(_)
    ));
    let (d7, _, _) = deliver(&mut c).await;
    assert!(d7.redelivered);
    assert!(d6.delivery_tag < d7.delivery_tag);
    // An unknown tag: 406, and the channel closes.
    c.send(
        1,
        BasicAck {
            delivery_tag: 999,
            multiple: false,
        },
    )
    .await
    .unwrap();
    let t = expect_channel_close(&mut c, 1, 406).await;
    assert!(t.contains("unknown delivery tag 999"), "{t}");
    // The channel's unacked message went back to the queue.
    open(&mut c, 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let ok = declare_queue(&mut c, 1, &q, FieldTable::new()).await;
    assert_eq!((ok.message_count, ok.consumer_count), (1, 0));
    delete_queue(&mut c, 1, &q).await;
    let _ = d3;
    c.close().await.unwrap();
}

#[tokio::test]
async fn competing_consumers_get_round_robin() {
    let e = env().await;
    let mut a = e.channel(1).await;
    let mut b = e.channel(1).await;
    let q = uniq("rr");
    declare_queue(&mut a, 1, &q, FieldTable::new()).await;
    a.call(
        1,
        BasicQos {
            prefetch_count: 100,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    b.call(
        1,
        BasicQos {
            prefetch_count: 100,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    a.call(
        1,
        BasicConsume {
            queue: q.clone(),
            consumer_tag: "a".into(),
            no_ack: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    b.call(
        1,
        BasicConsume {
            queue: q.clone(),
            consumer_tag: "b".into(),
            no_ack: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut p = e.channel(1).await;
    for i in 0..20u8 {
        p.publish(1, "", &q, false, &text("t"), &[i]).await.unwrap();
    }
    let mut na = 0;
    let mut nb = 0;
    for _ in 0..10 {
        deliver(&mut a).await;
        na += 1;
        deliver(&mut b).await;
        nb += 1;
    }
    assert_eq!((na, nb), (10, 10));
    quiet(&mut a, 200).await;
    delete_queue(&mut p, 1, &q).await;
}

#[tokio::test]
async fn get_and_get_empty() {
    let e = env().await;
    let mut c = e.channel(1).await;
    let q = uniq("get");
    declare_queue(&mut c, 1, &q, FieldTable::new()).await;
    assert!(matches!(
        c.call(
            1,
            BasicGet {
                queue: q.clone(),
                ..Default::default()
            }
        )
        .await
        .unwrap(),
        Method::BasicGetEmpty(_)
    ));
    let props = BasicProperties {
        content_type: Some("application/json".into()),
        headers: Some(FieldTable::from([("k", FieldValue::str("v"))])),
        delivery_mode: Some(2),
        correlation_id: Some("c1".into()),
        message_id: Some("m1".into()),
        ..Default::default()
    };
    c.publish(1, "", &q, false, &props, b"{}").await.unwrap();
    c.publish(1, "", &q, false, &props, b"{}").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    c.send(
        1,
        BasicGet {
            queue: q.clone(),
            no_ack: false,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let i = c.next_method(1).await.unwrap();
    let Method::BasicGetOk(g) = i.method else {
        panic!("{:?}", i.method)
    };
    assert_eq!(g.message_count, 1);
    let (p, b) = i.content.unwrap();
    assert_eq!(&b[..], b"{}");
    assert_eq!(p.message_id.as_deref(), Some("m1"));
    assert_eq!(p.headers.unwrap().get("k"), Some(&FieldValue::str("v")));
    c.send(
        1,
        BasicAck {
            delivery_tag: g.delivery_tag,
            multiple: false,
        },
    )
    .await
    .unwrap();
    delete_queue(&mut c, 1, &q).await;
    c.close().await.unwrap();
}

#[tokio::test]
async fn cancel_and_cancel_notify() {
    let e = env().await;
    let mut c = e.channel(1).await;
    let q = uniq("cancel");
    declare_queue(&mut c, 1, &q, FieldTable::new()).await;
    c.call(
        1,
        BasicConsume {
            queue: q.clone(),
            consumer_tag: "t1".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    // Reusing a tag: 530.
    let m = c
        .call(
            1,
            BasicCancel {
                consumer_tag: "t1".into(),
                no_wait: false,
            },
        )
        .await
        .unwrap();
    assert!(matches!(m, Method::BasicCancelOk(ok) if ok.consumer_tag == "t1"));
    c.call(
        1,
        BasicConsume {
            queue: q.clone(),
            consumer_tag: "t2".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    // Deleting the queue from another connection cancels the consumer.
    let mut other = e.channel(1).await;
    delete_queue(&mut other, 1, &q).await;
    let i = c.recv().await.unwrap();
    assert!(
        matches!(&i.method, Method::BasicCancel(bc) if bc.consumer_tag == "t2"),
        "{:?}",
        i.method
    );
    // Duplicate tags close the connection.
    let q2 = uniq("dup");
    declare_queue(&mut c, 1, &q2, FieldTable::new()).await;
    c.call(
        1,
        BasicConsume {
            queue: q2.clone(),
            consumer_tag: "same".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    c.send(
        1,
        BasicConsume {
            queue: q2.clone(),
            consumer_tag: "same".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    expect_connection_close(&mut c, 530).await;
    delete_queue(&mut other, 1, &q2).await;
    other.close().await.unwrap();
}

#[tokio::test]
async fn exclusive_consumers() {
    let e = env().await;
    let mut c = e.channel(1).await;
    let q = uniq("exclc");
    declare_queue(&mut c, 1, &q, FieldTable::new()).await;
    c.call(
        1,
        BasicConsume {
            queue: q.clone(),
            consumer_tag: "x".into(),
            exclusive: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut other = e.channel(1).await;
    other
        .send(
            1,
            BasicConsume {
                queue: q.clone(),
                consumer_tag: "y".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let t = expect_channel_close(&mut other, 1, 403).await;
    assert!(t.contains("exclusive use"), "{t}");
    open(&mut other, 1).await;
    delete_queue(&mut other, 1, &q).await;
    other.close().await.unwrap();
}

#[tokio::test]
async fn global_qos_limits_the_channel() {
    let e = env().await;
    let mut c = e.channel(1).await;
    let q1 = uniq("g1");
    let q2 = uniq("g2");
    declare_queue(&mut c, 1, &q1, FieldTable::new()).await;
    declare_queue(&mut c, 1, &q2, FieldTable::new()).await;
    for _ in 0..3 {
        c.publish(1, "", &q1, false, &text("t"), b"1")
            .await
            .unwrap();
        c.publish(1, "", &q2, false, &text("t"), b"2")
            .await
            .unwrap();
    }
    c.call(
        1,
        BasicQos {
            prefetch_count: 3,
            global: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    c.call(
        1,
        BasicConsume {
            queue: q1.clone(),
            consumer_tag: "c1".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    c.call(
        1,
        BasicConsume {
            queue: q2.clone(),
            consumer_tag: "c2".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    for _ in 0..3 {
        deliver(&mut c).await;
    }
    quiet(&mut c, 300).await;
    c.send(
        1,
        BasicAck {
            delivery_tag: 0,
            multiple: true,
        },
    )
    .await
    .unwrap();
    for _ in 0..3 {
        deliver(&mut c).await;
    }
    delete_queue(&mut c, 1, &q1).await;
    delete_queue(&mut c, 1, &q2).await;
    c.close().await.unwrap();
}

#[tokio::test]
async fn closing_requeues_unacked() {
    let e = env().await;
    let mut c = e.channel(1).await;
    let q = uniq("crash");
    declare_queue(&mut c, 1, &q, FieldTable::new()).await;
    c.publish(1, "", &q, false, &text("t"), b"job")
        .await
        .unwrap();
    c.call(
        1,
        BasicConsume {
            queue: q.clone(),
            consumer_tag: "w".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    deliver(&mut c).await;
    drop(c); // the consumer's connection dies
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut d = e.channel(1).await;
    d.send(
        1,
        BasicGet {
            queue: q.clone(),
            no_ack: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let i = d.next_method(1).await.unwrap();
    let Method::BasicGetOk(g) = i.method else {
        panic!("{:?}", i.method)
    };
    assert!(g.redelivered);
    delete_queue(&mut d, 1, &q).await;
    d.close().await.unwrap();
}

#[tokio::test]
async fn dead_lettering_and_ttl() {
    let e = env().await;
    let mut c = e.channel(1).await;
    let (dlx, dlq, q) = (uniq("dlx"), uniq("dlq"), uniq("src"));
    c.call(
        1,
        ExchangeDeclare {
            exchange: dlx.clone(),
            kind: "fanout".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    declare_queue(&mut c, 1, &dlq, FieldTable::new()).await;
    c.call(
        1,
        QueueBind {
            queue: dlq.clone(),
            exchange: dlx.clone(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    declare_queue(
        &mut c,
        1,
        &q,
        FieldTable::from([
            ("x-dead-letter-exchange", FieldValue::str(&dlx)),
            ("x-message-ttl", FieldValue::I32(100)),
        ]),
    )
    .await;
    c.publish(1, "", &q, false, &text("t"), b"rejected")
        .await
        .unwrap();
    let m = c
        .call(
            1,
            BasicGet {
                queue: q.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let Method::BasicGetOk(g) = m else {
        panic!("{m:?}")
    };
    c.send(
        1,
        BasicReject {
            delivery_tag: g.delivery_tag,
            requeue: false,
        },
    )
    .await
    .unwrap();
    c.publish(1, "", &q, false, &text("t"), b"expired")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    // The expired head is dead-lettered when reached.
    assert!(matches!(
        c.call(
            1,
            BasicGet {
                queue: q.clone(),
                no_ack: true,
                ..Default::default()
            }
        )
        .await
        .unwrap(),
        Method::BasicGetEmpty(_)
    ));
    let mut reasons = Vec::new();
    for _ in 0..2 {
        c.send(
            1,
            BasicGet {
                queue: dlq.clone(),
                no_ack: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let i = c.next_method(1).await.unwrap();
        assert!(matches!(i.method, Method::BasicGetOk(_)), "{:?}", i.method);
        let h = i.content.unwrap().0.headers.unwrap();
        let Some(FieldValue::Array(deaths)) = h.get("x-death") else {
            panic!("{h:?}")
        };
        let FieldValue::Table(d) = &deaths[0] else {
            panic!()
        };
        reasons.push(d.get("reason").unwrap().as_str().unwrap().into_owned());
        assert_eq!(d.get("queue").unwrap().as_str().unwrap(), q.as_str());
    }
    assert_eq!(reasons, ["rejected", "expired"]);
    for n in [&q, &dlq] {
        delete_queue(&mut c, 1, n).await;
    }
    c.call(
        1,
        ExchangeDelete {
            exchange: dlx,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    c.close().await.unwrap();
}

#[tokio::test]
async fn priorities_and_max_length() {
    let e = env().await;
    let mut c = e.channel(1).await;
    let q = uniq("prio");
    declare_queue(
        &mut c,
        1,
        &q,
        FieldTable::from([("x-max-priority", FieldValue::I32(10))]),
    )
    .await;
    for p in [1u8, 9, 5] {
        let props = BasicProperties {
            priority: Some(p),
            ..Default::default()
        };
        c.publish(1, "", &q, false, &props, &[p]).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut order = Vec::new();
    for _ in 0..3 {
        c.send(
            1,
            BasicGet {
                queue: q.clone(),
                no_ack: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        order.push(c.next_method(1).await.unwrap().content.unwrap().1[0]);
    }
    assert_eq!(order, [9, 5, 1]);
    let q2 = uniq("maxlen");
    declare_queue(
        &mut c,
        1,
        &q2,
        FieldTable::from([
            ("x-max-length", FieldValue::I32(2)),
            ("x-overflow", FieldValue::str("reject-publish")),
        ]),
    )
    .await;
    c.call(1, ConfirmSelect::default()).await.unwrap();
    for i in 0..3u8 {
        c.publish(1, "", &q2, false, &text("t"), &[i])
            .await
            .unwrap();
    }
    let mut acks = 0;
    let mut nacks = 0;
    while acks + nacks < 3 {
        match c.recv().await.unwrap().method {
            Method::BasicAck(_) => acks += 1,
            Method::BasicNack(n) => {
                assert_eq!(n.delivery_tag, 3);
                nacks += 1;
            }
            other => panic!("{other:?}"),
        }
    }
    assert_eq!((acks, nacks), (2, 1));
    delete_queue(&mut c, 1, &q).await;
    delete_queue(&mut c, 1, &q2).await;
    c.close().await.unwrap();
}

#[tokio::test]
async fn direct_reply_to_rpc() {
    let e = env().await;
    let mut server = e.channel(1).await;
    let rpc = uniq("rpc");
    declare_queue(&mut server, 1, &rpc, FieldTable::new()).await;
    server
        .call(
            1,
            BasicConsume {
                queue: rpc.clone(),
                consumer_tag: "srv".into(),
                no_ack: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut client = e.channel(1).await;
    // Manual ack on the pseudo-queue: 406.
    client
        .send(
            1,
            BasicConsume {
                queue: "amq.rabbitmq.reply-to".into(),
                consumer_tag: "r".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let t = expect_channel_close(&mut client, 1, 406).await;
    assert!(t.contains("39275"), "{t}");
    open(&mut client, 1).await;
    // Publishing with reply_to before consuming: 406.
    let props = BasicProperties {
        reply_to: Some("amq.rabbitmq.reply-to".into()),
        correlation_id: Some("42".into()),
        ..Default::default()
    };
    client
        .publish(1, "", &rpc, false, &props, b"ping")
        .await
        .unwrap();
    expect_channel_close(&mut client, 1, 406).await;
    open(&mut client, 1).await;
    client
        .call(
            1,
            BasicConsume {
                queue: "amq.rabbitmq.reply-to".into(),
                consumer_tag: "r".into(),
                no_ack: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    client
        .publish(1, "", &rpc, false, &props, b"ping")
        .await
        .unwrap();
    let (_, p, b) = deliver(&mut server).await;
    assert_eq!(&b[..], b"ping");
    let reply_to = p.reply_to.unwrap();
    assert!(reply_to.starts_with("amq.rabbitmq.reply-to."), "{reply_to}");
    // The name is alive (passive declare), and replies reach the client.
    let m = server
        .call(
            1,
            QueueDeclare {
                queue: reply_to.clone(),
                passive: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(matches!(m, Method::QueueDeclareOk(_)), "{m:?}");
    let rp = BasicProperties {
        correlation_id: Some("42".into()),
        ..Default::default()
    };
    server
        .publish(1, "", &reply_to, true, &rp, b"pong")
        .await
        .unwrap();
    let (d, p, b) = deliver(&mut client).await;
    assert_eq!(
        (&b[..], p.correlation_id.as_deref(), d.consumer_tag.as_str()),
        (&b"pong"[..], Some("42"), "r")
    );
    // A forged name is unroutable (returned when mandatory), not delivered.
    let mut forged = reply_to.clone();
    forged.pop();
    forged.push('Q');
    server
        .publish(1, "", &forged, true, &rp, b"x")
        .await
        .unwrap();
    let i = server.recv().await.unwrap();
    assert!(matches!(i.method, Method::BasicReturn(_)), "{:?}", i.method);
    quiet(&mut client, 200).await;
    delete_queue(&mut server, 1, &rpc).await;
    client.close().await.unwrap();
}

#[tokio::test]
async fn permissions_and_blocked() {
    let e = env().await;
    if e.live {
        return;
    }
    // A read-only user can't declare (403).
    let mut ro = Client::connect(
        e.tcp().await,
        "PLAIN",
        plain("acme.ro", "secret"),
        "/",
        default_capabilities(),
        None,
    )
    .await
    .unwrap();
    open(&mut ro, 1).await;
    ro.send(
        1,
        QueueDeclare {
            queue: uniq("ro"),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let t = expect_channel_close(&mut ro, 1, 403).await;
    assert!(t.contains("refused for user 'ro'"), "{t}");
    // connection.blocked and unblocked reach clients that asked for them.
    let mut c = e.connect().await;
    (e.blocker.as_ref().unwrap())(true);
    assert!(matches!(
        c.recv().await.unwrap().method,
        Method::ConnectionBlocked(_)
    ));
    (e.blocker.as_ref().unwrap())(false);
    assert!(matches!(
        c.recv().await.unwrap().method,
        Method::ConnectionUnblocked(_)
    ));
    c.close().await.unwrap();
}

#[tokio::test]
async fn vhosts_are_separate() {
    let e = env().await;
    if e.live {
        return;
    }
    let mut a = e.channel(1).await;
    let q = uniq("vh");
    declare_queue(&mut a, 1, &q, FieldTable::new()).await;
    let mut b = Client::connect(
        e.tcp().await,
        "PLAIN",
        plain(&e.user, &e.pass),
        "other",
        default_capabilities(),
        None,
    )
    .await
    .unwrap();
    open(&mut b, 1).await;
    b.send(
        1,
        QueueDeclare {
            queue: q,
            passive: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    expect_channel_close(&mut b, 1, 404).await;
}

// ------------------------------------------------------------------
// AMQP 1.0.
// ------------------------------------------------------------------

fn msg10(body: &[u8], to: Option<&str>, app: Vec<(String, Value)>) -> Bytes {
    let mut parts = vec![
        Properties {
            message_id: Some(Value::str("m-10")),
            to: to.map(str::to_owned),
            content_type: Some("text/plain".into()),
            ..Default::default()
        }
        .to_value(),
    ];
    if !app.is_empty() {
        parts.push(application_properties_section(app));
    }
    parts.push(data_section(Bytes::copy_from_slice(body)));
    encode_sections(&parts)
}

async fn disposition(c: &mut Client10<TcpStream>, id: u32) -> DeliveryState {
    let i = c
        .wait_for(|i| matches!(&i.perf, Performative::Disposition { first, .. } if *first == id))
        .await
        .unwrap();
    let Performative::Disposition { state, settled, .. } = i.perf else {
        unreachable!()
    };
    assert!(settled);
    state.unwrap()
}

async fn transfer(c: &mut Client10<TcpStream>) -> (u32, Sections) {
    let i = c
        .wait_for(|i| matches!(i.perf, Performative::Transfer { .. }))
        .await
        .unwrap();
    let Performative::Transfer { delivery_id, .. } = i.perf else {
        unreachable!()
    };
    (delivery_id.unwrap(), Sections::parse(&i.payload).unwrap())
}

#[tokio::test]
async fn amqp10_sasl() {
    let e = env().await;
    let r = Client10::connect(e.tcp().await, "PLAIN", Some(plain(&e.user, "wrong")), None).await;
    assert!(matches!(r, Err(Client10Error::Sasl(1))), "{r:?}");
    let r = Client10::connect(e.tcp().await, "ANONYMOUS", None, None).await;
    assert!(matches!(r, Err(Client10Error::Sasl(1))), "{r:?}");
    // An unknown vhost: open, then close with not-found.
    let mut r = Client10::connect(
        e.tcp().await,
        "PLAIN",
        Some(plain(&e.user, &e.pass)),
        Some("vhost:no-such-vhost-r9amqp"),
    )
    .await
    .unwrap();
    let i = r.expect(0).await.unwrap();
    let Performative::Close { error: Some(err) } = i.perf else {
        panic!("{:?}", i.perf)
    };
    assert_eq!(err.condition, "amqp:not-found");
    let mut c = Client10::connect(
        e.tcp().await,
        "PLAIN",
        Some(plain(&e.user, &e.pass)),
        Some("vhost:/"),
    )
    .await
    .unwrap();
    let Some(Performative::Open { max_frame_size, .. }) = &c.server_open else {
        panic!()
    };
    assert_eq!(*max_frame_size, 131_072);
    assert!(matches!(
        c.begin(0).await.unwrap(),
        Performative::Begin {
            remote_channel: Some(0),
            ..
        }
    ));
    c.close().await.unwrap();
}

#[tokio::test]
async fn amqp10_publish_outcomes() {
    let e = env().await;
    let mut setup = e.channel(1).await;
    let q = uniq("q10");
    declare_queue(&mut setup, 1, &q, FieldTable::new()).await;
    let mut c = e.connect10().await;
    // A link to /queues/q: credit, then accepted.
    let a = c
        .attach(
            0,
            0,
            "pub",
            true,
            Some(&format!("/queues/{q}")),
            false,
            vec![],
        )
        .await
        .unwrap();
    assert!(
        matches!(a, Performative::Attach { role: true, .. }),
        "{a:?}"
    );
    let f = c
        .wait_for(|i| {
            matches!(
                i.perf,
                Performative::Flow {
                    handle: Some(0),
                    ..
                }
            )
        })
        .await
        .unwrap();
    let Performative::Flow { link_credit, .. } = f.perf else {
        unreachable!()
    };
    assert_eq!(link_credit, Some(256));
    let id = c
        .transfer(0, 0, false, &msg10(b"hello", None, vec![]))
        .await
        .unwrap();
    assert_eq!(disposition(&mut c, id).await, DeliveryState::Accepted);
    // A link to a missing exchange is refused with not-found.
    let d = c
        .attach(
            0,
            1,
            "bad",
            true,
            Some(&format!("/exchanges/{}", uniq("nox"))),
            false,
            vec![],
        )
        .await
        .unwrap();
    let Performative::Detach {
        error: Some(err),
        closed: true,
        ..
    } = d
    else {
        panic!("{d:?}")
    };
    assert_eq!(err.condition, "amqp:not-found");
    // A bad address: invalid-field.
    let d = c
        .attach(0, 2, "bad2", true, Some("orders"), false, vec![])
        .await
        .unwrap();
    let Performative::Detach {
        error: Some(err), ..
    } = d
    else {
        panic!("{d:?}")
    };
    assert_eq!(err.condition, "amqp:invalid-field");
    // The anonymous terminus: `to` per message; unroutable is released,
    // a missing exchange rejected.
    c.attach(0, 3, "anon", true, None, false, vec![])
        .await
        .unwrap();
    let id = c
        .transfer(
            0,
            3,
            false,
            &msg10(b"x", Some(&format!("/queues/{q}")), vec![]),
        )
        .await
        .unwrap();
    assert_eq!(disposition(&mut c, id).await, DeliveryState::Accepted);
    let id = c
        .transfer(
            0,
            3,
            false,
            &msg10(
                b"x",
                Some("/exchanges/amq.direct/no-such-key-r9amqp"),
                vec![],
            ),
        )
        .await
        .unwrap();
    assert_eq!(disposition(&mut c, id).await, DeliveryState::Released);
    let id = c
        .transfer(
            0,
            3,
            false,
            &msg10(b"x", Some(&format!("/exchanges/{}/k", uniq("nox"))), vec![]),
        )
        .await
        .unwrap();
    let DeliveryState::Rejected(Some(err)) = disposition(&mut c, id).await else {
        panic!()
    };
    assert_eq!(err.condition, "amqp:not-found");
    let id = c
        .transfer(0, 3, false, &msg10(b"x", None, vec![]))
        .await
        .unwrap();
    assert!(matches!(
        disposition(&mut c, id).await,
        DeliveryState::Rejected(_)
    ));
    // Pre-settled: no disposition, but the message arrives.
    c.transfer(0, 0, true, &msg10(b"presettled", None, vec![]))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let ok = declare_queue(&mut setup, 1, &q, FieldTable::new()).await;
    assert_eq!(ok.message_count, 3);
    // Transactions are refused (a coordinator target).
    delete_queue(&mut setup, 1, &q).await;
    c.close().await.unwrap();
}

#[tokio::test]
async fn amqp10_consume_credit_outcomes_drain() {
    let e = env().await;
    let mut setup = e.channel(1).await;
    let q = uniq("c10");
    declare_queue(
        &mut setup,
        1,
        &q,
        FieldTable::from([("x-queue-type", FieldValue::str("quorum"))]),
    )
    .await;
    for i in 0..4u8 {
        setup
            .publish(1, "", &q, false, &text("t"), &[b'a' + i])
            .await
            .unwrap();
    }
    let mut c = e.connect10().await;
    let a = c
        .attach(
            0,
            0,
            "sub",
            false,
            Some(&format!("/queues/{q}")),
            false,
            vec![],
        )
        .await
        .unwrap();
    assert!(
        matches!(
            a,
            Performative::Attach {
                role: false,
                initial_delivery_count: Some(0),
                ..
            }
        ),
        "{a:?}"
    );
    c.flow(0, 0, 0, 2, false).await.unwrap();
    let (d0, s0) = transfer(&mut c).await;
    let (d1, s1) = transfer(&mut c).await;
    assert_eq!(s0.body, Body::Data(vec![Bytes::from_static(b"a")]));
    assert_eq!(s1.body, Body::Data(vec![Bytes::from_static(b"b")]));
    assert_eq!(s0.annotation("x-routing-key"), Some(&Value::str(&q)));
    // Credit 2: nothing more.
    c.timeout = Duration::from_millis(300);
    assert!(matches!(
        c.wait_for(|i| matches!(i.perf, Performative::Transfer { .. }))
            .await,
        Err(Client10Error::Timeout)
    ));
    c.timeout = Duration::from_secs(10);
    // Accept the first; modify the second with delivery-failed: it comes
    // back with delivery-count 1.
    c.settle(0, d0, d0, DeliveryState::Accepted).await.unwrap();
    c.settle(
        0,
        d1,
        d1,
        DeliveryState::Modified {
            delivery_failed: true,
            undeliverable_here: false,
            annotations: None,
        },
    )
    .await
    .unwrap();
    c.flow(0, 0, 2, 3, false).await.unwrap();
    let (d2, s2) = transfer(&mut c).await;
    assert_eq!(s2.body, Body::Data(vec![Bytes::from_static(b"b")]));
    assert_eq!(s2.header.as_ref().unwrap().delivery_count, 1);
    let (d3, _) = transfer(&mut c).await;
    let (d4, _) = transfer(&mut c).await;
    // Release requeues; reject drops (no dead-letter exchange).
    c.settle(0, d2, d2, DeliveryState::Released).await.unwrap();
    c.settle(0, d3, d4, DeliveryState::Rejected(None))
        .await
        .unwrap();
    // Drain with credit 5: one message (the released one), then the rest
    // of the credit comes back.
    c.flow(0, 0, 5, 5, true).await.unwrap();
    let (d5, s5) = transfer(&mut c).await;
    assert_eq!(s5.body, Body::Data(vec![Bytes::from_static(b"b")]));
    let f = c
        .wait_for(|i| {
            matches!(
                i.perf,
                Performative::Flow {
                    handle: Some(0),
                    ..
                }
            )
        })
        .await
        .unwrap();
    let Performative::Flow {
        drain,
        link_credit,
        delivery_count,
        ..
    } = f.perf
    else {
        unreachable!()
    };
    assert!(drain);
    assert_eq!((link_credit, delivery_count), (Some(0), Some(10)));
    c.settle(0, d5, d5, DeliveryState::Accepted).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let ok = declare_queue(
        &mut setup,
        1,
        &q,
        FieldTable::from([("x-queue-type", FieldValue::str("quorum"))]),
    )
    .await;
    assert_eq!(ok.message_count, 0);
    // A source that is an exchange is refused.
    let d = c
        .attach(
            0,
            1,
            "bad",
            false,
            Some("/exchanges/amq.direct"),
            false,
            vec![],
        )
        .await
        .unwrap();
    assert!(
        matches!(d, Performative::Detach { error: Some(_), .. }),
        "{d:?}"
    );
    // Detach of the consumer, then the queue is free to delete.
    c.send(
        0,
        &Performative::Detach {
            handle: 0,
            closed: true,
            error: None,
        },
    )
    .await
    .unwrap();
    c.expect(0).await.unwrap();
    delete_queue(&mut setup, 1, &q).await;
    c.close().await.unwrap();
}

#[tokio::test]
async fn amqp10_presettled_and_large_messages() {
    let e = env().await;
    let mut setup = e.channel(1).await;
    let q = uniq("big10");
    declare_queue(&mut setup, 1, &q, FieldTable::new()).await;
    let mut c = e.connect10().await;
    c.attach(
        0,
        0,
        "pub",
        true,
        Some(&format!("/queues/{q}")),
        false,
        vec![],
    )
    .await
    .unwrap();
    let body: Vec<u8> = (0..300_000u32).map(|i| (i % 253) as u8).collect();
    let id = c
        .transfer(0, 0, false, &msg10(&body, None, vec![]))
        .await
        .unwrap();
    assert_eq!(disposition(&mut c, id).await, DeliveryState::Accepted);
    c.attach(
        0,
        1,
        "sub",
        false,
        Some(&format!("/queues/{q}")),
        true,
        vec![],
    )
    .await
    .unwrap();
    c.flow(0, 1, 0, 10, false).await.unwrap();
    let i = c
        .wait_for(|i| matches!(i.perf, Performative::Transfer { .. }))
        .await
        .unwrap();
    let Performative::Transfer { settled, .. } = i.perf else {
        unreachable!()
    };
    assert_eq!(settled, Some(true));
    let s = Sections::parse(&i.payload).unwrap();
    assert_eq!(s.body, Body::Data(vec![Bytes::from(body)]));
    // Pre-settled: gone from the queue.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        declare_queue(&mut setup, 1, &q, FieldTable::new())
            .await
            .message_count,
        0
    );
    delete_queue(&mut setup, 1, &q).await;
    c.close().await.unwrap();
}

#[tokio::test]
async fn cross_protocol_conversion() {
    let e = env().await;
    let mut a = e.channel(1).await;
    let q = uniq("x10");
    declare_queue(&mut a, 1, &q, FieldTable::new()).await;
    // 0-9-1 publish, 1.0 consume.
    let props = BasicProperties {
        content_type: Some("application/json".into()),
        headers: Some(FieldTable::from([
            ("tenant", FieldValue::str("acme")),
            ("x-retries", FieldValue::I32(2)),
        ])),
        delivery_mode: Some(2),
        priority: Some(3),
        message_id: Some("m-091".into()),
        reply_to: Some("replies".into()),
        ..Default::default()
    };
    a.publish(1, "", &q, false, &props, b"{\"n\":1}")
        .await
        .unwrap();
    let mut c = e.connect10().await;
    c.attach(
        0,
        0,
        "sub",
        false,
        Some(&format!("/queues/{q}")),
        false,
        vec![],
    )
    .await
    .unwrap();
    c.flow(0, 0, 0, 10, false).await.unwrap();
    let (id, s) = transfer(&mut c).await;
    assert!(s.header.as_ref().unwrap().durable);
    assert_eq!(s.header.as_ref().unwrap().priority, 3);
    assert_eq!(s.properties.message_id, Some(Value::str("m-091")));
    assert_eq!(s.properties.reply_to.as_deref(), Some("/queues/replies"));
    assert_eq!(
        s.properties.content_type.as_deref(),
        Some("application/json")
    );
    assert_eq!(
        s.application_properties,
        vec![("tenant".into(), Value::str("acme"))]
    );
    assert_eq!(s.annotation("x-retries"), Some(&Value::Int(2)));
    assert_eq!(s.body, Body::Data(vec![Bytes::from_static(b"{\"n\":1}")]));
    c.settle(0, id, id, DeliveryState::Accepted).await.unwrap();
    // 1.0 publish, 0-9-1 consume.
    c.attach(
        0,
        1,
        "pub",
        true,
        Some(&format!("/queues/{q}")),
        false,
        vec![],
    )
    .await
    .unwrap();
    let id = c
        .transfer(
            0,
            1,
            false,
            &msg10(b"from 1.0", None, vec![("k".into(), Value::Long(7))]),
        )
        .await
        .unwrap();
    assert_eq!(disposition(&mut c, id).await, DeliveryState::Accepted);
    c.send(
        0,
        &Performative::Detach {
            handle: 0,
            closed: true,
            error: None,
        },
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    a.send(
        1,
        BasicGet {
            queue: q.clone(),
            no_ack: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let i = a.next_method(1).await.unwrap();
    let (p, b) = i.content.unwrap();
    assert_eq!(&b[..], b"from 1.0");
    assert_eq!(p.message_id.as_deref(), Some("m-10"));
    assert_eq!(p.content_type.as_deref(), Some("text/plain"));
    assert_eq!(p.headers.unwrap().get("k"), Some(&FieldValue::I64(7)));
    delete_queue(&mut a, 1, &q).await;
    c.close().await.unwrap();
}

#[tokio::test]
async fn amqp10_direct_reply_to() {
    let e = env().await;
    let mut c = e.connect10().await;
    // A volatile receiving link gets a reply address.
    let a = c
        .attach(
            0,
            0,
            "replies",
            false,
            None,
            true,
            vec!["rabbitmq:volatile-queue".into()],
        )
        .await
        .unwrap();
    let Performative::Attach {
        source: Some(src), ..
    } = a
    else {
        panic!("{a:?}")
    };
    let addr = src.address.unwrap();
    assert!(addr.starts_with("/queues/amq.rabbitmq.reply-to."), "{addr}");
    c.flow(0, 0, 0, 10, false).await.unwrap();
    // A 0-9-1 responder replies to the name.
    let name =
        sprig_proto_amqp::v10::message::percent_decode(addr.strip_prefix("/queues/").unwrap())
            .unwrap();
    let mut r = e.channel(1).await;
    r.publish(
        1,
        "",
        &name,
        false,
        &BasicProperties {
            correlation_id: Some("7".into()),
            ..Default::default()
        },
        b"pong",
    )
    .await
    .unwrap();
    let (_, s) = transfer(&mut c).await;
    assert_eq!(s.body, Body::Data(vec![Bytes::from_static(b"pong")]));
    assert_eq!(s.properties.correlation_id, Some(Value::str("7")));
    // An unsettled volatile link is refused.
    let d = c
        .attach(
            0,
            1,
            "bad",
            false,
            None,
            false,
            vec!["rabbitmq:volatile-queue".into()],
        )
        .await
        .unwrap();
    assert!(
        matches!(d, Performative::Detach { error: Some(_), .. }),
        "{d:?}"
    );
    c.close().await.unwrap();
    r.close().await.unwrap();
}

#[tokio::test]
async fn amqp10_errors() {
    let e = env().await;
    // A transfer on a handle that isn't attached closes the connection.
    let mut c = e.connect10().await;
    c.transfer(0, 9, false, &msg10(b"x", None, vec![]))
        .await
        .unwrap();
    let i = c.expect(0).await.unwrap();
    let Performative::Close { error: Some(err) } = i.perf else {
        panic!("{:?}", i.perf)
    };
    assert_eq!(err.condition, "amqp:session:unattached-handle");
    // rcv-settle-mode second is refused with not-implemented.
    let mut c = e.connect10().await;
    let mut setup = e.channel(1).await;
    let q = uniq("second");
    declare_queue(&mut setup, 1, &q, FieldTable::new()).await;
    c.send(
        0,
        &Performative::Attach {
            name: "s".into(),
            handle: 0,
            role: true,
            snd_settle_mode: 0,
            rcv_settle_mode: 1,
            source: Some(sprig_proto_amqp::v10::performative::Source {
                address: Some(format!("/queues/{q}")),
                ..Default::default()
            }),
            target: None,
            coordinator: false,
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: None,
            max_message_size: None,
            offered_capabilities: vec![],
            desired_capabilities: vec![],
            properties: None,
        },
    )
    .await
    .unwrap();
    c.expect(0).await.unwrap();
    let d = c.expect(0).await.unwrap();
    let Performative::Detach {
        error: Some(err), ..
    } = d.perf
    else {
        panic!("{:?}", d.perf)
    };
    assert_eq!(err.condition, "amqp:not-implemented");
    // Garbage after the header: a framing error closes.
    c.send_raw(&[0, 0, 0, 9, 2, 0, 0, 0, 0x0f]).await.unwrap();
    let i = c.expect(0).await.unwrap();
    assert!(
        matches!(i.perf, Performative::Close { error: Some(_) }),
        "{:?}",
        i.perf
    );
    delete_queue(&mut setup, 1, &q).await;
}

#[allow(dead_code)]
fn unused(_: Incoming, _: SocketAddr) {}

// ------------------------------------------------------------------
// Live-only: TLS on the amqps listener and the listener-bound tenant.
// ------------------------------------------------------------------

async fn tls_stream(addr: &str) -> tokio_rustls::client::TlsStream<TcpStream> {
    let mut roots = rustls::RootCertStore::empty();
    let pem = std::fs::read(var("SPRIG_AMQP_CA").unwrap()).unwrap();
    for c in rustls_pemfile::certs(&mut pem.as_slice()) {
        roots.add(c.unwrap()).unwrap();
    }
    let cfg = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    tokio_rustls::TlsConnector::from(Arc::new(cfg))
        .connect(name, TcpStream::connect(addr).await.unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn amqps_listener_serves_both_versions_over_tls() {
    let Some(addr) = var("SPRIG_AMQP_TLS_ADDR") else {
        return;
    };
    let (user, pass) = (
        var("SPRIG_AMQP_USER").unwrap(),
        var("SPRIG_AMQP_PASSWORD").unwrap(),
    );
    let mut c = Client::connect(
        tls_stream(&addr).await,
        "PLAIN",
        plain(&user, &pass),
        "/",
        default_capabilities(),
        None,
    )
    .await
    .unwrap();
    let m = c.call(1, ChannelOpen::default()).await.unwrap();
    assert!(matches!(m, Method::ChannelOpenOk(_)));
    let q = uniq("tls");
    let m = c
        .call(
            1,
            QueueDeclare {
                queue: q.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(matches!(m, Method::QueueDeclareOk(_)));
    c.publish(1, "", &q, false, &text("t"), b"over tls")
        .await
        .unwrap();
    let mut c10 = Client10::connect(
        tls_stream(&addr).await,
        "PLAIN",
        Some(plain(&user, &pass)),
        Some("vhost:/"),
    )
    .await
    .unwrap();
    c10.begin(0).await.unwrap();
    c10.attach(
        0,
        0,
        "sub",
        false,
        Some(&format!("/queues/{q}")),
        true,
        vec![],
    )
    .await
    .unwrap();
    c10.flow(0, 0, 0, 1, false).await.unwrap();
    let i = c10
        .wait_for(|i| matches!(i.perf, Performative::Transfer { .. }))
        .await
        .unwrap();
    let s = Sections::parse(&i.payload).unwrap();
    assert_eq!(s.body, Body::Data(vec![Bytes::from_static(b"over tls")]));
    let m = c
        .call(
            1,
            QueueDelete {
                queue: q,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(matches!(m, Method::QueueDeleteOk(_)));
    c.close().await.unwrap();
    c10.close().await.unwrap();
}

/// With `listener.amqp.tenant` set to the user's tenant, the plain user
/// name logs in (`SPRIG_AMQP_PLAIN_USER`, for example `app`).
#[tokio::test]
async fn listener_bound_tenant_takes_plain_names() {
    let (Some(addr), Some(user)) = (var("SPRIG_AMQP_ADDR"), var("SPRIG_AMQP_PLAIN_USER")) else {
        return;
    };
    let pass = var("SPRIG_AMQP_PASSWORD").unwrap();
    let c = Client::connect(
        TcpStream::connect(&addr).await.unwrap(),
        "PLAIN",
        plain(&user, &pass),
        "/",
        default_capabilities(),
        None,
    )
    .await
    .unwrap();
    c.close().await.unwrap();
    let mut c10 = Client10::connect(
        TcpStream::connect(&addr).await.unwrap(),
        "PLAIN",
        Some(plain(&user, &pass)),
        None,
    )
    .await
    .unwrap();
    c10.begin(0).await.unwrap();
    c10.close().await.unwrap();
}

/// A rough throughput number for the front end over the in-memory engine
/// (or a gateway): `cargo test --release --test protocol throughput -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement, not a check"]
async fn throughput() {
    let e = env().await;
    let mut c = e.channel(1).await;
    let q = uniq("tput");
    declare_queue(&mut c, 1, &q, FieldTable::new()).await;
    c.call(1, ConfirmSelect::default()).await.unwrap();
    let n = 20_000u64;
    let body = vec![b'x'; 128];
    let t = std::time::Instant::now();
    for _ in 0..n {
        c.publish(1, "", &q, false, &text("t"), &body)
            .await
            .unwrap();
    }
    let mut acked = 0;
    while acked < n {
        if let Method::BasicAck(_) = c.recv().await.unwrap().method {
            acked += 1;
        }
    }
    let publish = t.elapsed();
    c.call(
        1,
        BasicQos {
            prefetch_count: 500,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    c.call(
        1,
        BasicConsume {
            queue: q.clone(),
            consumer_tag: "t".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let t = std::time::Instant::now();
    for i in 1..=n {
        let (d, _, _) = deliver(&mut c).await;
        if i % 100 == 0 {
            c.send(
                1,
                BasicAck {
                    delivery_tag: d.delivery_tag,
                    multiple: true,
                },
            )
            .await
            .unwrap();
        }
    }
    let consume = t.elapsed();
    #[allow(clippy::cast_precision_loss)]
    let rate = |d: Duration| n as f64 / d.as_secs_f64();
    println!(
        "{n} messages of 128 bytes: publish with confirms {:.0}/s, consume with acks {:.0}/s",
        rate(publish),
        rate(consume)
    );
    delete_queue(&mut c, 1, &q).await;
}
