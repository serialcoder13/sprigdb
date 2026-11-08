#![allow(clippy::unwrap_used)]

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use mysql_async::prelude::*;
use mysql_async::{Conn, OptsBuilder};
use sprig_auth::verifier::NativeVerifier;
use sprig_auth::{FastCache, FileAccounts};
use sprig_common::telemetry::trace::{TraceLayer, Tracer};
use sprig_common::time::SystemClock;
use sprig_proto_mysql::Persona;
use tracing_subscriber::Layer as _;
use tracing_subscriber::layer::SubscriberExt as _;

use super::*;
use crate::frontend::{GatewayContext, Protocol};
use crate::listener::{Frontends, ListenerManager, TlsMode};
use crate::mysql::listener_hooks::ListenerHooks;
use crate::mysql::login::LoginDeps;
use crate::mysql::settings::{FrontendSettings, SettingsSource};
use crate::mysql::{MySqlConfig, MySqlFrontend};
use crate::pipeline::canned::CannedExecutor;
use crate::testutil::{TempDir, resolution, spec};

const PW: &str = "sprig-test-pw";

#[test]
fn errors_are_classified_by_who_caused_them() {
    let backend = |errno| SprigError {
        kind: ErrorKind::Backend {
            errno,
            sqlstate: *b"HY000",
        },
        message: String::new(),
        retryable: false,
        source: None,
    };
    assert_eq!(classify(&backend(1064)), "client");
    assert_eq!(classify(&backend(1062)), "client");
    assert_eq!(classify(&backend(2013)), "backend");
    assert_eq!(classify(&backend(1053)), "backend");
    assert_eq!(
        classify(&SprigError::new(SprigCode::BackendUnavailable, "x")),
        "backend"
    );
    assert_eq!(
        classify(&SprigError::new(SprigCode::ReservedName, "x")),
        "client"
    );
    assert_eq!(
        classify(&SprigError::new(SprigCode::MetaUnavailable, "x")),
        "sprig"
    );
    assert_eq!(classify(&SprigError::internal("x")), "sprig");
}

#[test]
fn backend_time_outside_a_request_is_ignored() {
    backend_time(Duration::from_millis(5));
}

/// The MySQL front end with the canned executor behind [`observed`]: each
/// statement makes one `gateway.request` root, failures are kept by tail
/// sampling with `result` and `error` recorded.
#[tokio::test]
async fn every_statement_gets_a_root_span_with_its_result() {
    let tracer = Arc::new(Tracer::new(1.0, 60_000));
    let layer = TraceLayer::new(Arc::clone(&tracer)).with_filter(
        tracing_subscriber::filter::filter_fn(|m| m.target() == TARGET),
    );
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let dir = TempDir::new();
    let path = dir.path().join("accounts.toml");
    let mut toml = String::new();
    // A native verifier: a full caching_sha2 login needs TLS.
    let native = NativeVerifier::create(PW.as_bytes()).to_string();
    let _ = writeln!(
        toml,
        "[[account]]\nuser = \"app\"\nmysql_native_password = \"{native}\"\ncluster = \"dc-a-c1\"\nbackend_user = \"root\"\nbackend_credentials = \"env:SPRIG_TEST_BACKEND\"\n"
    );
    std::fs::write(&path, toml).unwrap();
    let clock = Arc::new(SystemClock);
    let login = Arc::new(LoginDeps::new(
        sprig_auth::LoginSource::File(FileAccounts::open(&path).unwrap()),
        Arc::new(FastCache::new(
            clock.clone(),
            sprig_auth::fast_cache::DEFAULT_TTL,
        )),
    ));
    let fe = Arc::new(MySqlFrontend::new(MySqlConfig {
        persona: Persona::MySql84,
        settings: SettingsSource::Fixed(FrontendSettings::default()),
        login,
        executor: observed(Arc::new(CannedExecutor)),
        hooks: ListenerHooks::new(),
        hostname: "gw-test".to_owned(),
    }));
    let ctx = Arc::new(GatewayContext::new(clock));
    let mgr = ListenerManager::new(Frontends::new().with(fe), ctx);
    let mut s = spec("mysql", Protocol::MySql84);
    s.tls = TlsMode::Off;
    let report = mgr
        .apply(resolution(vec![s], Duration::from_secs(60)))
        .await;
    assert!(report.rejected.is_empty(), "{:?}", report.rejected);
    let addr = mgr.local_addr("mysql").await.unwrap();

    let mut c = Conn::new(
        OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(addr.port())
            .user(Some("app"))
            .pass(Some(PW))
            .prefer_socket(false),
    )
    .await
    .unwrap();
    let one: Option<i64> = c.query_first("SELECT 41").await.unwrap();
    assert_eq!(one, Some(41));
    assert!(c.query_drop("SELECT nonsense FROM nowhere").await.is_err());
    c.disconnect().await.unwrap();
    mgr.shutdown(Duration::from_secs(1)).await;

    let traces = tracer.recent();
    let roots: Vec<_> = traces.iter().filter_map(|t| t.root()).collect();
    assert!(roots.iter().all(|r| r.name == "gateway.request"));
    let attr = |r: &sprig_common::telemetry::trace::SpanRecord, k: &str| {
        r.attributes
            .iter()
            .find(|(a, _)| a == k)
            .map(|(_, v)| v.clone())
    };
    assert!(
        roots
            .iter()
            .any(|r| attr(r, "result").as_deref() == Some("ok") && !r.error),
        "{roots:?}"
    );
    let failed: Vec<_> = roots.iter().filter(|r| r.error).collect();
    assert_eq!(failed.len(), 1, "{roots:?}");
    assert_eq!(attr(failed[0], "result").as_deref(), Some("client"));
    assert_eq!(attr(failed[0], "protocol").as_deref(), Some("mysql"));
    assert_eq!(attr(failed[0], "request").as_deref(), Some("query"));
    // No SQL text anywhere in a span.
    for t in &traces {
        for s in &t.spans {
            for (_, v) in &s.attributes {
                assert!(!v.contains("nowhere"), "{s:?}");
            }
        }
    }
}
