//! `sprig-gateway`: the gateway role, listeners, sessions, front-end glue, backend pools and read routing.
//!
//! Layer 3 (roles). Design: [chapter 04](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/04-gateway.md) (Gateway) and [chapter 06](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/06-backend.md) (Backend) of the SprigDB low-level design.
//!
//! What exists:
//! - [`listener`]: the listener manager (build step 2): specs from
//!   `listener.<name>.*`, accept loops, PROXY v1/v2, TLS modes with
//!   first-byte detection, live re-binding with generations and draining;
//! - [`tls`]: certificate bundles (build step 3): polled PEM files, validation,
//!   atomic resolver swap, `sprig_gateway_cert_expiry_seconds`;
//! - [`frontend`]: the [`Frontend`](frontend::Frontend) trait the protocol
//!   front ends plug into, and the [`ClientConn`](frontend::ClientConn)
//!   they receive;
//! - [`session`]: sessions (build step 5): `Session`, the `SessionTable`
//!   with connection ids, `SHOW PROCESSLIST` and `KILL`, the connection
//!   task wrapper with panic isolation, and disconnect detection;
//! - [`mysql`]: the MySQL 8.4 and MariaDB front ends (chapter 05 steps 3,
//!   4, 6, 10 and 12): handshake and TLS upgrade, the auth plugins over
//!   chapter 14's `LoginService` (tenant catalogs, or the interim accounts
//!   file with `[gateway] login_mode = "accounts_file"`), command dispatch,
//!   compression and live listener hooks;
//! - [`tenant`]: tenants (M0.3): the catalog cache with the auth module,
//!   name virtualization (chapter 07) per statement, DDL through the
//!   catalog (chapter 08 step 10), settings in SQL (step 8) and
//!   `information_schema.SPRIG_SETTINGS`, the interim backend accounts;
//! - [`pipeline`]: the `Request`, `Executor` and `ResultSink` boundary
//!   between front ends and the execution layer (step 9's interfaces), a
//!   `NullExecutor` and a canned executor for tests.
//!
//! Listener settings come from the control plane: [`run`] mirrors the
//! `global`, `dc` and `cluster` settings scopes from etcd
//! ([`LiveSettings`]) and re-applies the listeners whenever their keys
//! change ([`listener::live`]). Without `[meta] dc_seeds` (development
//! without etcd) it runs on the built-in defaults, which then never change.
//! Listeners without a front end in this build (HTTP, MongoDB, Redis, ...)
//! are reported as not applied.
//!
//! - [`exec`]: the pass-through executor (decision D3) and the relay
//!   engine (chapter 05 steps 7-9): statements to the login's cluster,
//!   prepared statements across pooled connections, multi-statements,
//!   `LOCAL INFILE`; requests reach it through the workload hook. For a
//!   catalog login it plans every statement with chapter 07's rewrite and
//!   maps results, errors and session tracking back to logical names.
//!
//! - [`amqp`]: the AMQP listeners on 5672 and 5671 (chapter 50 steps 12,
//!   14 and 23): `sprig-proto-amqp`'s server, both AMQP versions told apart
//!   by the protocol header, over chapter 14's logins and, until
//!   `sprig-queues` exists, the in-memory queue engine.
//!
//! - [`kafka`]: the Kafka listener on 9092 (chapter 45 steps 9, 12, 13):
//!   `sprig-proto-kafka`'s broker over chapter 14's logins, the DC's
//!   gateway records, etcd coordinator leases and the tenant's streams.
//!
//! - [`mongo`]: the MongoDB listener (chapter 30, R5): `OP_MSG` and
//!   SCRAM-SHA-256 over chapter 14's logins, commands run by `sprig-doc`'s
//!   document engine through the user's executor session.
//!
//! Not yet: the other front ends (chapters 37, 40), limits,
//! registration and the other `MetaStore` caches (steps 4-10).

// The compiler's `Send` check of a session's future walks the inherited-sink
// deploy path down into sqlparser's AST (`TEST SINK`); since 12 Nov the chain
// is deeper than the default limit of 128 (E0275; bugfix-37 saw it close,
// bugfix-49 added this when a build needed it).
#![recursion_limit = "256"]

pub mod amqp;
pub mod backend;
// Chapter 43 (R8): the Bolt listener on 7687.
pub mod bolt;
pub mod dc;
pub mod exec;
pub mod failover;
pub mod frontend;
// Chapter 43 (R8): native graphs in the tenant catalog.
pub mod graphs;
pub mod http;
pub mod inherited;
pub mod kafka;
pub mod lifecycle;
pub mod limits;
pub mod listener;
pub mod mongo;
pub mod mysql;
pub mod observe;
pub mod offload;
// R9 (r9fwd): the feed-driven outbox forwarder's gateway side.
pub mod outbox_feed;
pub mod peer;
pub mod pipeline;
pub mod reads;
pub mod redis;
pub mod registry;
// Chapter 39 (R5 round 5): full-text search.
pub mod search;
pub mod session;
pub mod sink_peer;
pub mod tenant;
pub mod tls;
// Chapter 41 (R9): workers' runs of triggers on streams and queues.
pub mod triggers;
pub mod workload;
pub mod write_load;

use std::sync::Arc;
use std::time::Duration;

use sprig_common::time::{Clock, SystemClock};
use sprig_common::{Result, Role, RoleContext, SprigError};
use sprig_meta::etcd::{DEFAULT_REQUEST_TIMEOUT, EtcdTls, connect_tiers};
use sprig_meta::{MemStore, MetaStore, Tier};
use sprig_settings::{LiveSettings, Registry, SettingCtx, SettingsStores};

use crate::frontend::GatewayContext;
use crate::listener::{Frontends, ListenerManager};
use crate::session::SessionTable;
use crate::tls::CertStore;

/// Runs the gateway role until `ctx.shutdown` is cancelled or a drain is
/// asked for through the Admin API.
///
/// The `sprigdb` binary has already bound the telemetry port, so
/// `/healthz` answers and `/readyz` reports `starting` until this marks the
/// process ready. Startup follows chapter 04: load the settings mirrors,
/// register (lease, slot, record in state `starting`), apply the listeners,
/// then report ready; listener settings then apply live. Drain follows
/// chapter 04's five steps ([`lifecycle`]).
///
/// # Errors
/// `Internal` if the setting registry is invalid (a bug); 39998 for
/// unreadable `[tls]` files; 39500 if no etcd seed answers at start-up
/// (chapter 23: refuse to start).
#[allow(clippy::too_many_lines)] // start-up and drain, step by step
pub async fn run(ctx: RoleContext) -> Result<()> {
    let role = Role::Gateway;
    let registry = Registry::global()?;
    let dc = ctx.config.node.dc.clone();
    let sctx = SettingCtx::process(&dc);
    let gateway_id = ctx.config.node.id();
    // Components stop on this token, at the end of the drain, whether
    // SIGTERM or the Admin API started it. It is not a child of
    // `ctx.shutdown`: on SIGTERM the permit loop, the backends and the
    // registration must keep running while the gateway drains, or writes
    // that still arrive during the drain get 39501 (hardening round 2: a
    // stopping gateway refused writes for 5-6 s before it closed its
    // listeners).
    let stop = sprig_common::CancellationToken::new();
    let rctx = RoleContext {
        config: ctx.config.clone(),
        health: ctx.health.clone(),
        shutdown: stop.clone(),
    };
    let lifecycle = lifecycle::Lifecycle::new();

    let live = LiveSettings::start(registry, settings_stores(&ctx).await?)?;
    tokio::select! {
        () = ctx.shutdown.cancelled() => return Ok(()),
        () = live.ready() => {}
    }
    // bugfix-62 (Q39): no start with a commit-resolve window the resolver
    // could sweep under.
    sprig_txn::settings::check_live(&live, &dc)?;
    // Chapter 47: trace sampling, the OTLP endpoint and the audit spool
    // limit follow their settings.
    let _telemetry_settings = sprig_settings::ops::apply_live(&live, &dc);

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let certs = Arc::new(CertStore::new(clock.clone()));
    let watcher = certs.spawn_watcher(tls::POLL_INTERVAL, stop.clone());
    // Backend pools and read routing (chapter 06): clusters from the DC
    // tier's registry, settings from the live mirror.
    let stores = live.stores();
    let backends = backend::Backends::new(
        dc.clone(),
        // The DC tier also holds each cluster's writer lease (chapter 25).
        backend::RegistryClusters::with_leases(
            sprig_cluster::registry::ClusterRegistry::new(
                Arc::clone(&stores.global),
                Arc::clone(&stores.dc),
            )?,
            Some(Arc::clone(&stores.dc)),
            Duration::from_secs(2),
        ),
        backend::LiveBackendSettings::new(live.clone(), dc.clone()),
    );
    let _reaper = backends.start();
    let sessions = Arc::new(SessionTable::new(clock.clone()));

    // Registration (chapter 04 step 6): the slot first, so connection ids
    // carry it from the first session on.
    let registrar = registry::Registrar::new(registry::GatewayRecord {
        gateway_id: gateway_id.clone(),
        slot: 0,
        version: env!("CARGO_PKG_VERSION").to_owned(),
        addr: ctx
            .config
            .gateway
            .advertise_addr
            .clone()
            .unwrap_or_else(|| gethostname::gethostname().to_string_lossy().into_owned()),
        listeners: std::collections::BTreeMap::new(),
        state: registry::GatewayState::Starting,
        catalog_formats: Vec::new(),
        authorizers: sprig_auth::authz::authorizer::KNOWN
            .iter()
            .map(|s| (*s).to_owned())
            .collect(),
        load: registry::Load::default(),
        kafka_host: ctx.config.gateway.kafka_advertise_host.clone(),
    });
    let lease_live = live.clone();
    let lease_dc = dc.clone();
    let registration = registrar
        .start(
            Arc::clone(&stores.dc),
            clock.clone(),
            Arc::clone(&sessions),
            move || {
                Duration::from_secs(lease_live.get(
                    &registry::LEASE_TTL_SECONDS,
                    &SettingCtx::process(&lease_dc),
                ))
            },
            stop.clone(),
        )
        .await;

    // Chapter 25 (R2): the DC permit, its watchdog and admission while the
    // DC returns from an outage.
    let _fence = failover::start(
        Arc::clone(&stores.dc),
        dc.clone(),
        gateway_id.clone(),
        Some(&live),
        stop.clone(),
    );

    // The peer service (chapter 04 step 10).
    let peer_tls = ctx
        .config
        .tls
        .as_ref()
        .map(peer::PeerTls::from_config)
        .transpose()?;
    let peers = Arc::new(peer::Peers::new(Arc::clone(&stores.dc), peer_tls.clone()));
    // Chapter 37, "Admin API over TLS": how this gateway checks a
    // controller whose Admin API serves HTTPS.
    http::admin::set_controller_tls(
        ctx.config
            .gateway
            .controller_ca_file
            .as_deref()
            .or_else(|| ctx.config.tls.as_ref().map(|t| t.ca.as_path())),
        ctx.config
            .tls
            .as_ref()
            .map(|t| (t.cert.as_path(), t.key.as_path())),
    )?;

    // Logins (chapter 14) and, for catalog logins, the tenant services
    // (chapters 07 and 08).
    // Chapter 26: logins of tenants pinned elsewhere go to a gateway of an
    // allowed DC over the peer service.
    let handoff = Arc::new(sprig_auth::handoff::HandoffConfig {
        here: dc.clone(),
        peers: Arc::new(peer::PeerHandoff::new(Arc::clone(&peers))),
    });
    let (login, tenants, login_tasks) = logins(
        &rctx,
        &live,
        Arc::clone(&backends),
        Arc::clone(&sessions),
        clock.clone(),
        handoff,
    )?;
    let mut executor = exec::PassThrough::new(
        Arc::clone(&backends),
        exec::ExecSettingsSource::Live {
            live: live.clone(),
            dc: dc.clone(),
        },
    );
    if let Some(t) = &tenants {
        executor = executor.with_tenants(Arc::clone(t));
    }
    // Chapter 51: sink credentials are sealed with the master key file's
    // `sink` keys.
    exec::sinks::install_keyring(rctx.config.gateway.workload_key_file.as_deref());
    // Workload capture (chapter 12, M0.4): the hook and its flusher.
    let (workload, flusher) = workload::start(
        &rctx.config,
        &live,
        tenants.as_ref(),
        clock.clone(),
        stop.clone(),
    )?;
    // Every front end's requests pass the workload hook (chapter 12) into
    // the one executor.
    // Admission (chapter 04 step 7) in front of everything, so a statement
    // waiting for a slot holds no backend connection.
    // Chapter 04: the `limits.*` keys per tenant, from its cached catalog.
    let limiter = limits::Limiter::new(limits::LimitSource::Live {
        live: live.clone(),
        dc: dc.clone(),
        tenants: tenants.as_ref().map(|t| {
            let cache = t.cache.clone();
            let lookup: limits::TenantSettingsLookup =
                Arc::new(move |id| cache.cached(id).map(|c| Arc::clone(&c.settings)));
            lookup
        }),
    });
    // Chapter 47's SLIs and root trace span outermost, so admission waits
    // count in statement latency.
    let shared_exec = observe::observed(limits::limited(
        pipeline::with_workload(Arc::new(executor), workload),
        Arc::clone(&limiter),
    ));
    // The MySQL and MariaDB front ends (chapter 05).
    let mysql = mysql::MySqlStack::start(
        mysql::StackConfig {
            login: Arc::clone(&login),
            live: live.clone(),
            dc: dc.clone(),
            hostname: ctx.config.node.id(),
            clock: clock.clone(),
            executor: Arc::clone(&shared_exec),
        },
        stop.clone(),
    )?;
    // HTTP on 8080 and 8443 (chapter 04 step 4, chapter 37).
    let engine = Arc::new(http::sql::SqlEngine {
        executor: Arc::clone(&shared_exec),
        sessions: Arc::clone(&sessions),
        api: http::settings::ApiSettingsSource::Live {
            live: live.clone(),
            dc: dc.clone(),
        },
        frontend: mysql::settings::SettingsSource::Live {
            live: live.clone(),
            dc: dc.clone(),
        },
        hostname: ctx.config.node.id(),
        dc: dc.to_string(),
        gateway_id: gateway_id.clone(),
        peers: Some(Arc::clone(&peers)),
        txns: http::sql::TxnRegistry::default(),
        draining: lifecycle.flag(),
        limiter: Some(Arc::clone(&limiter)),
        tenants: tenants.clone(),
        languages: http::lang::Languages::default(),
    });
    let txn_reaper = tokio::spawn(Arc::clone(&engine).reap(stop.clone()));
    let drain_lc = lifecycle.clone();
    let core = Arc::new(http::Core {
        engine: Arc::clone(&engine),
        auth: Arc::new(http::auth::ChapterFourteen::new(Arc::clone(&login))),
        admin: Arc::new(
            http::admin::AdminApi::new(
                Arc::clone(&stores.dc),
                gateway_id.clone(),
                Some(Arc::clone(&peers)),
                Arc::new(move |reason: &str| drain_lc.request_drain(reason)),
            )
            .with_internal_token_file(ctx.config.gateway.controller_token_file.as_deref())?,
        ),
        // Studio (chapter 44): files, browser sessions, API additions.
        studio: Arc::new(
            http::studio::StudioState::new(
                http::studio::StudioSource::Live {
                    live: live.clone(),
                    dc: dc.clone(),
                },
                Some(Arc::clone(&login)),
                Some(Arc::clone(&stores.global)),
            )
            .with_tenants(tenants.clone())
            .with_factor_keys(ctx.config.gateway.workload_key_file.as_deref())
            .with_operator_token_file(ctx.config.gateway.operator_token_file.as_deref())
            .with_gateway(&gateway_id),
        ),
    });
    // Chapter 52: SQL's SPRIG APPROVE|IGNORE|SNOOZE RECOMMENDATION.
    tenant::rec_sql::install(Arc::clone(&core.admin));
    let http_fe = Arc::new(http::HttpFrontend::new(Arc::clone(&core)));
    // Kafka on 9092 (chapter 45, R9): catalog logins only, since topics are
    // a tenant's streams.
    let kafka_fe = tenants.as_ref().map(|t| {
        let cluster = Arc::new(kafka::cluster::GatewayCluster::new(
            Arc::clone(&stores.dc),
            registrar.clone(),
            live.clone(),
            dc.clone(),
        ));
        let data = Arc::new(kafka::data::GatewayData::start(Arc::clone(t), stop.clone()));
        // Chapter 29 (RL44): filtered change-topic reads run as the reader.
        data.set_engine(Arc::clone(&engine));
        Arc::new(kafka::KafkaFrontend::start(
            kafka::KafkaDeps {
                live: live.clone(),
                dc: dc.clone(),
                identity: Arc::new(kafka::identity::KafkaIdentity::new(Arc::clone(&login))),
                cluster,
                topics: Arc::new(kafka::catalog::StreamTopics::new(
                    Arc::clone(t),
                    Arc::clone(&engine),
                    Arc::clone(&data),
                )),
                groups: Arc::new(kafka::catalog::CatalogGroups::new(Arc::clone(t))),
                data: Some(data),
            },
            stop.clone(),
        ))
    });
    // AMQP on 5672 and 5671 (chapter 50, R9). The queue relay to other
    // gateways' owners and the direct reply-to key the DC's gateways share
    // go over the peer service.
    let queue_relay = amqp::peer::GatewayRelay::new(Arc::clone(&stores.dc), peer_tls.clone());
    let replies = match amqp::peer::reply_key(stores.dc.as_ref()).await {
        Some(k) => {
            sprig_proto_amqp::reply_to::DirectReplyTo::with_key(u32::from(sessions.slot()), &k)
        }
        None => sprig_proto_amqp::reply_to::DirectReplyTo::new(u32::from(sessions.slot())),
    };
    replies.set_relay(Arc::new(queue_relay.clone()));
    let amqp_queues = Arc::new(match &tenants {
        // Chapter 50 steps 2-10: the queue engine over the catalog.
        Some(t) => amqp::GatewayQueues::with_engine(
            Arc::clone(t),
            live.clone(),
            dc.clone(),
            Arc::clone(&stores.dc),
            gateway_id.clone(),
            u32::from(sessions.slot()),
            Some(queue_relay),
            stop.clone(),
        ),
        None => amqp::GatewayQueues::new(None, live.clone(), dc.clone()),
    });
    let queue_peer = amqp_queues.engine().map(|e| {
        amqp::peer::QueuePeerService::new(
            Arc::clone(e),
            Arc::clone(&replies),
            u32::from(sessions.slot()),
        )
    });
    // Full-text search (chapter 39, R5 round 5): index copies in this
    // gateway, fed by the change feed; SQL and MongoDB use it.
    if let Some(t) = &tenants
        && let Err(e) = search::SearchService::start(
            search::SearchDeps {
                tenants: Arc::clone(t),
                dc_store: Arc::clone(&stores.dc),
                login: Arc::clone(&login),
                executor: Arc::clone(&shared_exec),
                sessions: Arc::clone(&sessions),
                live: live.clone(),
                dc: dc.clone(),
                gateway_id: gateway_id.clone(),
                peer_tls: peer_tls.clone(),
            },
            stop.clone(),
        )
    {
        tracing::warn!(error = %e, "full-text search unavailable");
    }
    // MongoDB (chapter 30, R5): catalog logins through SCRAM; the
    // document engine runs in the user's executor session.
    let mongo_fe = Arc::new(mongo::MongoFrontend::start(
        mongo::MongoDeps {
            login: Arc::clone(&login),
            executor: Arc::clone(&shared_exec),
            sessions: Arc::clone(&sessions),
            live: Some((live.clone(), dc.clone())),
            gateway_id: gateway_id.clone(),
            draining: lifecycle.flag(),
            tenants: tenants.clone(),
            dc_store: Some(Arc::clone(&stores.dc)),
        },
        stop.clone(),
    ));
    // Redis (chapter 40, R6): catalog logins through AUTH; commands run as
    // SQL in the user's executor session.
    let redis_fe = redis::RedisFrontend::new(redis::RedisDeps {
        login: Arc::clone(&login),
        executor: Arc::clone(&shared_exec),
        sessions: Arc::clone(&sessions),
        live: live.clone(),
        dc: dc.clone(),
        gateway_id: gateway_id.clone(),
    });
    // RL51: keyspace notifications from the change feed.
    let redis_fe = Arc::new(match &tenants {
        Some(t) => redis_fe.with_feed(Arc::clone(t)),
        None => redis_fe,
    });
    let amqp_fe = Arc::new(amqp::AmqpFrontend::with_replies(
        Arc::new(amqp::identity::AmqpIdentity::new(
            Arc::clone(&login),
            live.clone(),
            dc.clone(),
        )),
        amqp_queues,
        live.clone(),
        dc.clone(),
        gateway_id.clone(),
        replies,
    ));
    // Bolt on 7687 (chapter 43, R8): catalog logins only, since graphs
    // are a tenant's objects.
    let bolt_svc = Arc::new(bolt::BoltGraphService::new(
        Arc::clone(&login),
        tenants.as_ref().map(graphs::GatewayGraphs::shared),
        live.clone(),
        dc.clone(),
    ));
    // Sql.Query's `language` (tui T2): Cypher and GQL through the graph
    // service, MongoDB commands through the document engine's front end.
    engine.languages.set_graphs(Arc::clone(&bolt_svc));
    engine.languages.set_mongo(Arc::clone(mongo_fe.shared()));
    let bolt_fe = Arc::new(bolt::BoltFrontend::new(bolt_svc));
    // Chapter 40's pub/sub hubs between gateways, on the peer service.
    let redis_peer = redis::hub::RedisPeerService::new(
        gateway_id.clone(),
        Arc::clone(&stores.dc),
        peer_tls.clone(),
        redis::mesh_opts(&live, &dc),
    );
    let redis_env = Arc::clone(redis_fe.env());
    let peer_host = Arc::new(PeerHost {
        login: Arc::clone(&login),
        core: Arc::clone(&core),
        sessions: Arc::clone(&sessions),
        gateway_id: gateway_id.clone(),
        lifecycle: lifecycle.clone(),
        queue_peer,
        redis_peer: Some(redis_peer.clone()),
    });
    let binds = bind_overrides(&ctx)?;
    let peer_bind = match binds.get("peer") {
        Some(a) => *a,
        None => live.get(&peer::PEER_BIND, &sctx).parse().map_err(|e| {
            SprigError::new(
                sprig_common::SprigCode::ConfigInvalid,
                format!("listener.peer.bind: {e}"),
            )
        })?,
    };
    let peer_server = peer::serve(peer_bind, peer_tls, peer_host, stop.clone()).await?;
    if peer_server.is_some() {
        redis_peer.start(&redis_env, stop.clone());
    }
    let peer_port = peer_server.as_ref().map(|(a, _)| a.port());

    let listeners = Arc::new(
        ListenerManager::new(
            {
                let fes = mysql
                    .register(Frontends::new())
                    .with(http_fe as Arc<dyn frontend::Frontend>)
                    .with(amqp_fe as Arc<dyn frontend::Frontend>)
                    .with(mongo_fe as Arc<dyn frontend::Frontend>)
                    .with(redis_fe as Arc<dyn frontend::Frontend>)
                    .with(bolt_fe as Arc<dyn frontend::Frontend>);
                match kafka_fe {
                    Some(k) => fes.with(k as Arc<dyn frontend::Frontend>),
                    None => fes,
                }
            },
            Arc::new(
                GatewayContext::with_sessions(clock.clone(), Arc::clone(&sessions))
                    .with_backends(Arc::clone(&backends))
                    .with_peers(Arc::clone(&peers))
                    .with_limits(Arc::clone(&limiter)),
            ),
        )
        .with_bind_overrides(binds),
    );
    // Subscribe before the first apply, so no change falls in between.
    let changes = live.subscribe(listener::live::LISTENER_KEYS);
    let report = listener::live::apply_current(&live, &dc, &certs, &listeners).await;
    tracing::info!(%role, applied = ?report.applied, not_applied = report.rejected.len(), "listeners applied");
    let follower = tokio::spawn(listener::live::follow(
        live.clone(),
        dc.clone(),
        Arc::clone(&certs),
        Arc::clone(&listeners),
        changes,
        listener::live::rejections(&report),
        stop.clone(),
    ));
    // The record's listener ports and load, kept current.
    let reporter = tokio::spawn(report_load(
        registrar.clone(),
        Arc::clone(&listeners),
        Arc::clone(&sessions),
        peer_port,
        stop.clone(),
    ));
    registrar.set_listeners(listener_ports(&listeners, peer_port).await);
    registrar.set_state(registry::GatewayState::Ready);

    metrics::gauge!("sprig_gateway_draining").set(0.0);
    ctx.health.set_ready();
    tracing::info!(%role, gateway = %gateway_id, slot = sessions.slot(), "ready");

    tokio::select! {
        () = ctx.shutdown.cancelled() => {}
        () = lifecycle.requested() => {}
    }

    // Drain (chapter 04, "Drain").
    // 1. Draining: readiness off, the record says so; give load balancers
    //    time to notice.
    lifecycle.mark_draining();
    ctx.health.set_draining();
    registrar.set_state(registry::GatewayState::Draining);
    metrics::gauge!("sprig_gateway_draining").set(1.0);
    let drain = Duration::from_secs(live.get(&listener::settings::DRAIN_TIMEOUT_SECONDS, &sctx));
    let lb_delay = Duration::from_secs(live.get(&lifecycle::DRAIN_LB_DELAY_SECONDS, &sctx));
    tracing::info!(event = "gateway.drain_started", %role, lb_delay_s = lb_delay.as_secs(), timeout_s = drain.as_secs(), "draining");
    tokio::time::sleep(lb_delay).await;
    // 2-4. Close the listeners; idle sessions close now, busy ones at their
    //    next safe point; open transaction handles get the same time.
    let marked = sessions.close_all_at_safe_point();
    tracing::info!(
        sessions = marked,
        "draining: listeners closed, sessions closing at safe points"
    );
    tokio::join!(listeners.shutdown(drain), engine.txns.wait_empty(drain));
    // 5. Stop everything; the registration lease is revoked as the
    //    registrar stops.
    stop.cancel();
    let _ = follower.await;
    let _ = reporter.await;
    let _ = txn_reaper.await;
    backends.shutdown();
    let _ = watcher.await;
    mysql.join().await;
    for t in login_tasks {
        let _ = t.await;
    }
    // The flusher's last flush, before the catalog pools close.
    if let Some(f) = flusher {
        let _ = f.await;
    }
    if let Some(t) = tenants {
        t.locator.close().await;
    }
    if let Some((_, t)) = peer_server {
        let _ = t.await;
    }
    let _ = registration.await;
    tracing::info!(%role, "drained");
    Ok(())
}

/// The port of every running listener, and the peer service's.
async fn listener_ports(
    listeners: &ListenerManager,
    peer: Option<u16>,
) -> std::collections::BTreeMap<String, u16> {
    let mut out = std::collections::BTreeMap::new();
    for keys in &listener::LISTENERS {
        if let Some(a) = listeners.local_addr(keys.name).await {
            out.insert(keys.name.to_owned(), a.port());
        }
    }
    if let Some(p) = peer {
        out.insert("peer".to_owned(), p);
    }
    out
}

/// Keeps the registration record's ports and load current.
async fn report_load(
    registrar: registry::Registrar,
    listeners: Arc<ListenerManager>,
    sessions: Arc<SessionTable>,
    peer: Option<u16>,
    stop: sprig_common::CancellationToken,
) {
    let mut tick = tokio::time::interval(registry::LOAD_PERIOD);
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = tick.tick() => {}
        }
        let rows = sessions.list();
        let active = rows.iter().filter(|r| r.command != "Sleep").count();
        registrar.set_load(registry::Load {
            connections: rows.len() as u64,
            active: active as u64,
            cpu_pct: None,
        });
        let ports = listener_ports(&listeners, peer).await;
        if ports != registrar.record().listeners {
            registrar.set_listeners(ports);
        }
    }
}

/// The gateway's side of the peer service.
struct PeerHost {
    login: Arc<sprig_auth::LoginService>,
    core: Arc<http::Core>,
    sessions: Arc<SessionTable>,
    gateway_id: String,
    lifecycle: lifecycle::Lifecycle,
    /// Chapter 50's queue relay, when the queue engine runs here.
    queue_peer: Option<amqp::peer::QueuePeerService>,
    /// Chapter 40's pub/sub hubs.
    redis_peer: Option<redis::hub::RedisPeerService>,
}

#[async_trait::async_trait]
impl peer::PeerHost for PeerHost {
    fn queue_peer(&self) -> Option<amqp::peer::QueuePeerService> {
        self.queue_peer.clone()
    }

    fn redis_peer(&self) -> Option<redis::hub::RedisPeerService> {
        self.redis_peer.clone()
    }

    fn logic_peer(&self) -> Option<triggers::LogicPeerService> {
        Some(triggers::LogicPeerService::new(
            Arc::clone(&self.login),
            Arc::clone(&self.core.engine),
        ))
    }

    fn sessions(&self) -> &Arc<SessionTable> {
        &self.sessions
    }

    fn gateway_id(&self) -> String {
        self.gateway_id.clone()
    }

    fn drain(&self, reason: &str) -> bool {
        self.lifecycle.request_drain(reason)
    }

    async fn forwarded(
        &self,
        req: sprig_api::proto::internal::ForwardRequest,
    ) -> sprig_api::proto::internal::ForwardReply {
        http::rest::forwarded(&self.core, req).await
    }

    async fn handoff(
        &self,
        start: sprig_auth::handoff::HandoffStart,
        password: zeroize::Zeroizing<Vec<u8>>,
    ) -> sprig_auth::handoff::HandoffResult {
        sprig_auth::handoff::serve_handoff(&self.login, start, &password).await
    }
}

/// `[gateway] listen_override`: this process's own listener addresses.
fn bind_overrides(
    ctx: &RoleContext,
) -> Result<std::collections::HashMap<String, std::net::SocketAddr>> {
    let mut binds = std::collections::HashMap::new();
    for (name, addr) in &ctx.config.gateway.listen_override {
        let a = addr.parse().map_err(|e| {
            SprigError::new(
                sprig_common::SprigCode::ConfigInvalid,
                format!("[gateway] listen_override.{name} = {addr:?}: {e}"),
            )
        })?;
        tracing::info!(listener = %name, bind = %a, "listener address overridden for this process");
        binds.insert(name.clone(), a);
    }
    Ok(binds)
}

/// The process's logins by `[gateway] login_mode`: tenant catalogs (the
/// default, chapter 14) with the tenant services, or the interim accounts
/// file (decision D1) with its watcher.
type Logins = (
    Arc<mysql::login::LoginService>,
    Option<Arc<tenant::TenantServices>>,
    Vec<tokio::task::JoinHandle<()>>,
);

fn logins(
    ctx: &RoleContext,
    live: &LiveSettings,
    backends: Arc<backend::Backends>,
    sessions: Arc<SessionTable>,
    clock: Arc<dyn Clock>,
    handoff: Arc<sprig_auth::handoff::HandoffConfig>,
) -> Result<Logins> {
    use sprig_auth::{CatalogLogins, FastCache, LoginService, LoginSource, MetaTenants};
    use sprig_common::config::LoginModeConfig;
    let fast_cache = Arc::new(FastCache::new(
        clock.clone(),
        sprig_auth::fast_cache::DEFAULT_TTL,
    ));
    let cfg = &ctx.config.gateway;
    let audit: Arc<dyn sprig_auth::audit::AuditSink> =
        Arc::new(sprig_auth::audit::LogAudit::new(ctx.config.node.id()));
    match cfg.effective_login_mode() {
        LoginModeConfig::AccountsFile => {
            let mut tasks = Vec::new();
            let accounts: Arc<dyn sprig_auth::AccountStore> = if let Some(path) = &cfg.accounts_file
            {
                let file = sprig_auth::FileAccounts::open(path)?;
                tasks.push(
                    file.spawn_watcher(sprig_auth::accounts::POLL_INTERVAL, ctx.shutdown.clone()),
                );
                file
            } else {
                tracing::warn!("no [gateway] accounts_file: every MySQL login is refused (1045)");
                Arc::new(sprig_auth::StaticAccounts::default())
            };
            tracing::info!(
                "logins from the accounts file (login_mode = accounts_file): names pass through"
            );
            let svc = LoginService::new(LoginSource::File(accounts), fast_cache)
                .with_clock(clock)
                .with_audit(audit);
            Ok((Arc::new(svc), None, tasks))
        }
        LoginModeConfig::Catalog => {
            let tenants = tenant::TenantServices::start(live, backends, sessions, clock.clone())?;
            let resolver = if let Some(p) = &cfg.backends_file {
                tenant::backends::TenantBackends::open(p, Arc::clone(&tenants.locator))?
            } else {
                tracing::warn!(
                    "no [gateway] backends_file: catalog logins fail with 39998 until one names a backend account"
                );
                tenant::backends::TenantBackends::none(Arc::clone(&tenants.locator))
            };
            // Chapter 29: per-user backend accounts, when configured.
            tenants.authz.configure(resolver.per_user());
            let catalogs = Arc::new(CatalogLogins::new(
                Arc::new(MetaTenants::new(
                    Arc::clone(&tenants.global),
                    sprig_auth::account::META_TENANTS_TTL,
                )),
                Arc::new(tenants.cache.clone()),
                Arc::new(resolver),
                Arc::new(sprig_auth::SettingsPolicy::new(
                    tenants.settings.clone(),
                    tenants.dc.clone(),
                )),
            ));
            // Chapter 14: tenant endpoints by TLS SNI, live.
            let sni = follow_sni_suffixes(
                Arc::clone(&catalogs),
                live.clone(),
                tenants.dc.clone(),
                ctx.shutdown.clone(),
            );
            tracing::info!("logins from tenant catalogs (login_mode = catalog)");
            // Chapter 14: failure counters and temporary locks in the
            // tenants' catalogs, audit events in the log (target
            // sprig_audit) until chapter 47's pipeline.
            let state = sprig_auth::policy::CatalogLoginState::new(tenants.catalog.clone());
            let svc = LoginService::new(LoginSource::Catalog(catalogs), fast_cache)
                .with_clock(clock)
                .with_login_state(Arc::new(state))
                .with_audit(audit)
                .with_handoff(handoff);
            Ok((Arc::new(svc), Some(tenants), vec![sni]))
        }
    }
}

/// Applies `listener.tenant_sni_suffixes` to `logins` now and after every
/// change (chapter 14, "Finding the tenant"), until `shutdown`. A value
/// that isn't a list of DNS suffixes (only possible when it was written
/// around the controller's check) keeps the old list and raises
/// `SettingNotApplied`.
fn follow_sni_suffixes(
    logins: Arc<sprig_auth::CatalogLogins>,
    live: LiveSettings,
    dc: sprig_common::ids::DcId,
    shutdown: sprig_common::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    use sprig_auth::sni::{TENANT_SNI_SUFFIXES, parse};
    let apply = move |live: &LiveSettings| {
        let raw = live.get(
            &TENANT_SNI_SUFFIXES,
            &sprig_settings::SettingCtx::process(&dc),
        );
        match parse(&raw) {
            Ok(list) => {
                if *logins.sni_suffixes() != list {
                    tracing::info!(suffixes = ?list, "tenant endpoints by SNI changed");
                }
                logins.set_sni_suffixes(list);
            }
            Err(e) => sprig_settings::not_applied(
                "listener.tenant_sni_suffixes",
                &format!("{raw:?}: {e}"),
            ),
        }
    };
    let mut rx = live.watch(&TENANT_SNI_SUFFIXES);
    apply(&live);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                r = rx.changed() => if r.is_err() { return; },
            }
            apply(&live);
        }
    })
}

/// How often the gateway re-reads etcd's member list.
const DISCOVERY_PERIOD: Duration = Duration::from_secs(60);

/// The stores the settings mirrors read: the etcd tiers named by `[meta]`
/// (single-etcd mode when the global seeds are empty or the same), or, with
/// no seeds, an empty in-memory store, so every key keeps its default.
async fn settings_stores(ctx: &RoleContext) -> Result<SettingsStores> {
    let dc = ctx.config.node.dc.clone();
    let meta = &ctx.config.meta;
    if meta.dc_seeds.is_empty() {
        tracing::warn!(
            "no [meta] dc_seeds: settings are the built-in defaults and won't change live"
        );
        let mem = MemStore::new(Tier::Dc(dc));
        return SettingsStores::new(Arc::new(mem.with_tier(Tier::Global)), Arc::new(mem));
    }
    let tls = ctx
        .config
        .tls
        .as_ref()
        .map(EtcdTls::from_files)
        .transpose()?;
    // Until the relay (chapter 23 step 11), the gateway reads the global
    // settings straight from the global tier.
    let tiers = connect_tiers(dc, meta, tls, DEFAULT_REQUEST_TIMEOUT, true).await?;
    // Endpoint discovery (chapter 23 step 9), every 60 s
    // (control.etcd_member_refresh_seconds' default; the key is the
    // controller's).
    let discovery = Arc::new(sprig_meta::discovery::Discovery::new(&tiers.dc));
    drop(discovery.spawn(|| DISCOVERY_PERIOD, ctx.shutdown.clone()));
    let global = tiers
        .global
        .ok_or_else(|| SprigError::internal("connect_tiers returned no global store"))?;
    let global: Arc<dyn MetaStore> = Arc::new(global);
    let dc: Arc<dyn MetaStore> = Arc::new(tiers.dc);
    // Two tiers (r2next): other DCs' shard maps and cluster records come
    // through their controllers' relay (chapter 23 step 11); one etcd
    // serves them directly.
    let dc: Arc<dyn MetaStore> = if two_tier(meta) {
        Arc::new(sprig_meta::relay::RoutedStore::new(dc, Arc::clone(&global)))
    } else {
        dc
    };
    SettingsStores::new(global, dc)
}

/// Whether `[meta]` names a global tier apart from the DC's.
fn two_tier(meta: &sprig_common::config::MetaConfig) -> bool {
    let norm = |v: &[String]| {
        let mut v: Vec<String> = v
            .iter()
            .map(|s| s.trim_end_matches('/').to_owned())
            .collect();
        v.sort();
        v
    };
    !meta.global_seeds.is_empty() && norm(&meta.global_seeds) != norm(&meta.dc_seeds)
}

#[cfg(test)]
mod testutil;
