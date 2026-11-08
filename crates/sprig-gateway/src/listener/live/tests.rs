//! Listener settings changed in the store apply live: the port moves, the
//! TLS mode switches, the certificate path changes, and a change that can't
//! be applied keeps the running listener. Runs on `MemStore`, and on etcd
//! when `SPRIG_TEST_ETCD` names one (under private tiers, deleted
//! afterwards).

// One scenario, step by step.
#![allow(clippy::too_many_lines)]

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use sprig_common::CancellationToken;
use sprig_common::time::{Clock, SystemClock};
use sprig_meta::conformance::{ETCD_ENV, delete_prefix, unique_tier};
use sprig_meta::elect::{Election, Elector, LeaderRecord, Leadership};
use sprig_meta::session::SessionOpts;
use sprig_meta::{EtcdConfig, EtcdStore, MemStore, MetaStore};
use sprig_settings::{
    LiveSettings, Registry, Scope, SettingChange, SettingsStores, SettingsWriter, Value,
};
use tokio::net::TcpStream;

use super::*;
use crate::frontend::{GatewayContext, Protocol};
use crate::listener::Frontends;
use crate::testutil::{
    Echo, TempDir, TestCa, closed_by_server, roundtrip, server_cert, tls_connect,
};

const WAIT: Duration = Duration::from_secs(10);
const FAR: (i32, u8, u8) = (2045, 1, 1);

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn lead(store: &Arc<dyn MetaStore>) -> Leadership {
    let e = Election::new(
        Arc::clone(store),
        Arc::new(SystemClock),
        SessionOpts::new(Duration::from_secs(5)),
    );
    let me = LeaderRecord {
        holder: "test".to_owned(),
        dc: None,
        build: "test".to_owned(),
    };
    e.campaign(&store.tier().key("leader").unwrap(), &me)
        .await
        .unwrap()
}

/// Polls until listener `name` is bound to `want` (or unbound for `None`).
async fn until_bound(mgr: &ListenerManager, name: &str, want: Option<SocketAddr>) {
    tokio::time::timeout(WAIT, async {
        while mgr.local_addr(name).await != want {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{name} bound to {want:?} in time"));
}

async fn until_generation(mgr: &ListenerManager, name: &str, at_least: u64) {
    tokio::time::timeout(WAIT, async {
        while mgr.generation(name).await < at_least {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{name} reached generation {at_least} in time"));
}

/// The scenario, against `stores`.
async fn listener_follows_settings(stores: SettingsStores) {
    let registry: &'static Registry = Registry::global().unwrap();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let writer = SettingsWriter::new(registry, stores.clone(), clock.clone());
    let dl = lead(&stores.dc).await;
    let gl = lead(&stores.global).await;
    let dc_id = stores.dc_id().clone();
    let dc = Scope::Dc(dc_id.clone());
    let set = |key: &'static str, v: Value| {
        let (writer, dl, dc) = (&writer, &dl, &dc);
        async move {
            writer
                .set(
                    dl,
                    dc,
                    key,
                    SettingChange::Set(v),
                    "op:test",
                    "live listener test",
                )
                .await
                .unwrap();
        }
    };

    let live = LiveSettings::start(registry, stores.clone()).unwrap();
    tokio::time::timeout(WAIT, live.ready()).await.unwrap();
    let certs = Arc::new(CertStore::new(clock.clone()));
    let mgr = Arc::new(ListenerManager::new(
        Frontends::new().with(Arc::new(Echo(Protocol::Redis))),
        Arc::new(GatewayContext::new(clock.clone())),
    ));
    let changes = live.subscribe(LISTENER_KEYS);
    let report = apply_current(&live, &dc_id, &certs, &mgr).await;
    assert!(
        !report.applied.contains(&"redis"),
        "redis is off by default"
    );
    let stop = CancellationToken::new();
    let follower = tokio::spawn(follow(
        live.clone(),
        dc_id.clone(),
        Arc::clone(&certs),
        Arc::clone(&mgr),
        changes,
        rejections(&report),
        stop.clone(),
    ));

    // Enable the listener on a port: it binds without a restart.
    let p1: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    set("listener.redis.tls", "off".into()).await;
    set("listener.redis.bind", p1.to_string().as_str().into()).await;
    // A one-second drain, set globally by the global leader.
    writer
        .set(
            &gl,
            &Scope::Global,
            "listener.drain_seconds",
            SettingChange::Set(Value::Int(1)),
            "op:test",
            "",
        )
        .await
        .unwrap();
    set("listener.redis.enabled", Value::Bool(true)).await;
    until_bound(&mgr, "redis", Some(p1)).await;
    let mut old = TcpStream::connect(p1).await.unwrap();
    let reply = roundtrip(&mut old, "ping").await.unwrap();
    assert!(reply.starts_with("ping "), "{reply}");
    let gen_before = mgr.generation("redis").await;

    // Move the port: the new socket is bound, the old one closed, and the
    // old generation's connection drained after listener.drain_seconds.
    let p2: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    set("listener.redis.bind", p2.to_string().as_str().into()).await;
    until_bound(&mgr, "redis", Some(p2)).await;
    assert!(mgr.generation("redis").await > gen_before);
    assert!(
        TcpStream::connect(p1).await.is_err(),
        "the old port is closed"
    );
    let mut moved = TcpStream::connect(p2).await.unwrap();
    assert!(roundtrip(&mut moved, "ping").await.is_some());
    assert!(closed_by_server(&mut old).await, "drained after 1 s");

    // Switch the TLS mode: certificate paths first, then `required`.
    let ca = TestCa::new();
    let dir1 = TempDir::new();
    let leaf1 = ca.leaf("gateway-1", FAR);
    let (c1, k1) = dir1.write_leaf(&leaf1);
    set("tls.bundles.default.cert_path", c1.to_str().unwrap().into()).await;
    set("tls.bundles.default.key_path", k1.to_str().unwrap().into()).await;
    let g = mgr.generation("redis").await;
    set("listener.redis.tls", "required".into()).await;
    until_generation(&mgr, "redis", g + 1).await;
    let mut tls = tls_connect(p2, &ca.der).await.unwrap();
    assert_eq!(server_cert(&tls), leaf1.der);
    let reply = roundtrip(&mut tls, "ping").await.unwrap();
    assert!(reply.contains("tls=1."), "{reply}");
    // A stricter mode drains the plaintext connection of the old generation.
    assert!(closed_by_server(&mut moved).await);

    // Point the bundle at other files: new handshakes use the new chain.
    let dir2 = TempDir::new();
    let leaf2 = ca.leaf("gateway-2", FAR);
    let (c2, k2) = dir2.write_leaf(&leaf2);
    let g = mgr.generation("redis").await;
    set("tls.bundles.default.key_path", k2.to_str().unwrap().into()).await;
    set("tls.bundles.default.cert_path", c2.to_str().unwrap().into()).await;
    // The intermediate state (new key, old certificate) doesn't validate
    // and is kept out; the final one applies.
    until_generation(&mgr, "redis", g + 1).await;
    let tls2 = tokio::time::timeout(WAIT, async {
        loop {
            let s = tls_connect(p2, &ca.der).await.unwrap();
            if server_cert(&s) == leaf2.der {
                return s;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    drop(tls2);

    // A port already in use can't be applied: the running listener stays.
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let busy = taken.local_addr().unwrap();
    let g = mgr.generation("redis").await;
    set("listener.redis.bind", busy.to_string().as_str().into()).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(mgr.local_addr("redis").await, Some(p2));
    assert_eq!(mgr.generation("redis").await, g);
    assert!(tls_connect(p2, &ca.der).await.is_ok());

    // Disable it: the socket closes.
    set("listener.redis.enabled", Value::Bool(false)).await;
    until_bound(&mgr, "redis", None).await;

    stop.cancel();
    follower.await.unwrap();
    mgr.shutdown(Duration::from_secs(1)).await;
    drop(tls);
    gl.resign().await.unwrap();
    dl.resign().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listener_follows_settings_in_memstore() {
    let dc = MemStore::new(unique_tier());
    let global = dc.with_tier(unique_tier());
    let stores = SettingsStores::new(Arc::new(global), Arc::new(dc)).unwrap();
    listener_follows_settings(stores).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listener_follows_settings_in_etcd() {
    let Ok(endpoints) = std::env::var(ETCD_ENV) else {
        eprintln!("skipping: set {ETCD_ENV} to run the etcd half");
        return;
    };
    let cfg = EtcdConfig::new(endpoints.split(',').map(str::to_owned).collect());
    let dc = EtcdStore::connect(unique_tier(), &cfg).await.unwrap();
    // A private tier stands in for the global one, so the shared etcd's
    // real global keys are never touched.
    let global = dc.with_tier(unique_tier());
    let roots = [dc.tier().root(), global.tier().root()];
    let cleanup = dc.clone();
    let stores = SettingsStores::new(Arc::new(global), Arc::new(dc)).unwrap();
    let outcome = std::panic::AssertUnwindSafe(listener_follows_settings(stores))
        .catch_unwind()
        .await;
    for root in &roots {
        delete_prefix(&cleanup, root).await;
    }
    if let Err(p) = outcome {
        std::panic::resume_unwind(p);
    }
}

#[test]
fn rejections_are_compared_by_name_and_reason() {
    let report = ApplyReport {
        applied: vec!["redis"],
        rejected: vec![super::super::ListenerRejected {
            name: "mysql",
            reason: "no front end".to_owned(),
        }],
    };
    let want: BTreeSet<_> = [("mysql", "no front end".to_owned())].into_iter().collect();
    assert_eq!(rejections(&report), want);
}
