//! The MongoDB front end (chapter 30, R5): the gateway answers on the
//! `mongo` listener as the `mongos` of a MongoDB 8.0 sharded cluster.
//!
//! - [`conn`]: framing, the handshake (`hello`, legacy `isMaster` over
//!   `OP_QUERY`, compression, client metadata, the data center from
//!   `appName`), SCRAM-SHA-256 against chapter 14's verifiers (also
//!   speculative), and command dispatch;
//! - [`commands`]: the command handlers over `sprig-doc`'s engine;
//! - [`cursors`]: the cursor table;
//! - [`sql`]: the engine's SQL through the user's own executor session.
//!
//! - [`writes`]: the write commands, statement by statement;
//! - [`retry`]: retryable writes, exactly once, with their records.
//!
//! - [`catalog`]: collections in the tenant catalog;
//! - [`publish`]: the publication wait for new array paths;
//! - [`changes`]: change streams over the change feed;
//! - [`users`]: user and role commands over chapter 29's accounts.
//! - [`logic`]: `sprigCall`, logic-layer functions (chapter 41, R7).
//! - [`snapshot`]: snapshot reads outside transactions (R5 round 4).
//!
//! Interim scope (R5 interim, `decisions/r5-interim.md`): transactions on one executor
//! session per `lsid`, and no `$clusterTime` signing.

pub mod catalog;
pub mod changes;
pub mod commands;
pub mod conn;
pub mod cursors;
pub mod failpoint;
pub mod logic;
pub mod publish;
pub mod retry;
pub mod search;
pub mod snapshot;
pub mod sql;
pub mod txn;
pub mod users;
pub mod writes;

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sprig_auth::mysql::LoginService;
use sprig_common::ids::DcId;
use sprig_common::{CancellationToken, Result};
use sprig_settings::{Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx};

use crate::frontend::{ClientConn, Frontend, GatewayContext, Protocol};
use crate::pipeline::SharedExecutor;
use crate::session::SessionTable;

type Text = Cow<'static, str>;

/// dc and global: gateway behaviour.
const GW: Scopes = Scopes::PROCESS;

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $hard:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: GW,
            hard: $hard,
            apply: Apply::Live,
            operator_only: false,
            owner: 30,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key!(
    /// `mongo.max_wire_version`: MongoDB 8.0's 25.
    MAX_WIRE_VERSION: u64 = "mongo.max_wire_version", 25, HardBounds::range(6, 99),
    "Reported maxWireVersion (MongoDB 8.0)"
);
key!(
    /// `mongo.server_version`.
    SERVER_VERSION: Text = "mongo.server_version", Cow::Borrowed("8.0.0"), HardBounds::NONE,
    "buildInfo version"
);
key!(
    /// `mongo.compressors`, in server preference order.
    COMPRESSORS: Text = "mongo.compressors", Cow::Borrowed("zstd,snappy,zlib"), HardBounds::NONE,
    "Server preference order of compressors"
);
key!(
    /// `mongo.cursor_timeout_ms`.
    CURSOR_TIMEOUT_MS: u64 = "mongo.cursor_timeout_ms", 600_000, HardBounds::range(1_000, 86_400_000),
    "Idle cursor lifetime"
);
key!(
    /// `mongo.max_cursors_per_tenant` (per gateway).
    MAX_CURSORS_PER_TENANT: u64 = "mongo.max_cursors_per_tenant", 10_000, HardBounds::range(1, 10_000_000),
    "Open cursors per tenant per gateway"
);
key!(
    /// `mongo.session_timeout_minutes`: `logicalSessionTimeoutMinutes`.
    SESSION_TIMEOUT_MINUTES: u64 = "mongo.session_timeout_minutes", 30, HardBounds::range(1, 1440),
    "logicalSessionTimeoutMinutes"
);
key!(
    /// `mongo.session_record_retention_minutes`: retryable-write records
    /// older than this are purged (above `mongo.session_timeout_minutes`).
    SESSION_RECORD_RETENTION_MINUTES: u64 = "mongo.session_record_retention_minutes", 35, HardBounds::range(2, 10_080),
    "Age at which retryable-write records are purged"
);
key!(
    /// `mongo.write_conflict_retries`: internal retries of a retryable
    /// write that met a deadlock or a write conflict.
    WRITE_CONFLICT_RETRIES: u64 = "mongo.write_conflict_retries", 5, HardBounds::range(0, 100),
    "Internal retries outside transactions"
);
key!(
    /// `mongo.txn_lifetime_seconds`.
    TXN_LIFETIME_SECONDS: u64 = "mongo.txn_lifetime_seconds", 60, HardBounds::range(1, 86_400),
    "Transaction lifetime limit"
);

/// The values a connection reads.
#[derive(Debug, Clone)]
pub struct MongoSettings {
    /// `mongo.max_wire_version`.
    pub max_wire_version: i32,
    /// `mongo.server_version`.
    pub server_version: String,
    /// `mongo.compressors`.
    pub compressors: Vec<sprig_proto_mongo::Compressor>,
    /// `mongo.cursor_timeout_ms`.
    pub cursor_timeout: Duration,
    /// `mongo.max_cursors_per_tenant`.
    pub max_cursors: usize,
    /// `mongo.session_timeout_minutes`.
    pub session_timeout_minutes: i32,
    /// `mongo.txn_lifetime_seconds`.
    pub txn_lifetime: Duration,
    /// `mongo.session_record_retention_minutes`, at least the session
    /// timeout plus one minute.
    pub session_record_retention_minutes: u64,
    /// `mongo.write_conflict_retries`.
    pub write_conflict_retries: u32,
}

impl MongoSettings {
    fn read(live: &LiveSettings, dc: &DcId) -> Self {
        let cx = SettingCtx::process(dc);
        Self {
            max_wire_version: i32::try_from(live.get(&MAX_WIRE_VERSION, &cx)).unwrap_or(25),
            server_version: live.get(&SERVER_VERSION, &cx).into_owned(),
            compressors: live
                .get(&COMPRESSORS, &cx)
                .split(',')
                .filter_map(|c| sprig_proto_mongo::Compressor::from_name(c.trim()))
                .collect(),
            cursor_timeout: Duration::from_millis(live.get(&CURSOR_TIMEOUT_MS, &cx)),
            max_cursors: usize::try_from(live.get(&MAX_CURSORS_PER_TENANT, &cx)).unwrap_or(10_000),
            session_timeout_minutes: i32::try_from(live.get(&SESSION_TIMEOUT_MINUTES, &cx))
                .unwrap_or(30),
            txn_lifetime: Duration::from_secs(live.get(&TXN_LIFETIME_SECONDS, &cx)),
            session_record_retention_minutes: live
                .get(&SESSION_RECORD_RETENTION_MINUTES, &cx)
                .max(live.get(&SESSION_TIMEOUT_MINUTES, &cx) + 1),
            write_conflict_retries: u32::try_from(live.get(&WRITE_CONFLICT_RETRIES, &cx))
                .unwrap_or(5),
        }
    }
}

impl Default for MongoSettings {
    fn default() -> Self {
        Self {
            max_wire_version: 25,
            server_version: "8.0.0".into(),
            compressors: vec![
                sprig_proto_mongo::Compressor::Zstd,
                sprig_proto_mongo::Compressor::Snappy,
                sprig_proto_mongo::Compressor::Zlib,
            ],
            cursor_timeout: Duration::from_secs(600),
            max_cursors: 10_000,
            session_timeout_minutes: 30,
            txn_lifetime: Duration::from_secs(60),
            session_record_retention_minutes: 35,
            write_conflict_retries: 5,
        }
    }
}

/// What the front end is built from.
pub struct MongoDeps {
    /// Logins (chapter 14).
    pub login: Arc<LoginService>,
    /// The shared executor.
    pub executor: SharedExecutor,
    /// The gateway's sessions.
    pub sessions: Arc<SessionTable>,
    /// Live settings; `None` for defaults (tests).
    pub live: Option<(LiveSettings, DcId)>,
    /// This gateway's id (cursor ids, `serviceId`).
    pub gateway_id: String,
    /// Draining (91 `ShutdownInProgress` on `hello`).
    pub draining: Arc<std::sync::atomic::AtomicBool>,
    /// The tenant services (catalog logins): collection metadata in the
    /// tenant catalog. `None` keeps it in each database's hidden table.
    pub tenants: Option<Arc<crate::tenant::TenantServices>>,
    /// This DC's tier (shard maps and handoff records for change streams);
    /// `None`: no change streams.
    pub dc_store: Option<Arc<dyn sprig_meta::MetaStore>>,
}

impl std::fmt::Debug for MongoDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoDeps")
            .field("gateway_id", &self.gateway_id)
            .finish_non_exhaustive()
    }
}

/// The shared state of the front end.
pub struct Shared {
    pub(crate) d: MongoDeps,
    pub(crate) cursors: cursors::CursorTable,
    pub(crate) txns: commands::TxnTable,
    pub(crate) snapshots: snapshot::Snapshots,
    pub(crate) records: retry::Records,
    pub(crate) failpoints: failpoint::FailPoints,
    pub(crate) feed: Option<changes::ChangeFeed>,
    pub(crate) docs: Option<Arc<catalog::CatalogDocs>>,
    pub(crate) engine: sprig_doc::engine::Engine,
    pub(crate) decoy_secret: [u8; 32],
    pub(crate) process_id: bson::oid::ObjectId,
    pub(crate) conn_ids: std::sync::atomic::AtomicI32,
    pub(crate) started: std::time::Instant,
}

impl Shared {
    /// `mongo.test_commands`: the `failCommand` fail point is accepted.
    #[must_use]
    pub fn test_commands(&self) -> bool {
        self.d
            .live
            .as_ref()
            .is_some_and(|(l, dc)| l.get(&failpoint::TEST_COMMANDS, &SettingCtx::process(dc)))
    }

    /// `gateway.large_statement_bytes`: requests this big run on a
    /// blocking thread ([`crate::offload`]).
    #[must_use]
    pub fn large_request_bytes(&self) -> u64 {
        match &self.d.live {
            Some((l, dc)) => l.get(
                &crate::offload::LARGE_STATEMENT_BYTES,
                &SettingCtx::process(dc),
            ),
            None => crate::offload::LARGE_STATEMENT_BYTES.default,
        }
    }

    /// The settings now.
    #[must_use]
    pub fn settings(&self) -> MongoSettings {
        match &self.d.live {
            Some((l, dc)) => MongoSettings::read(l, dc),
            None => MongoSettings::default(),
        }
    }
}

/// The MongoDB front end.
pub struct MongoFrontend {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for MongoFrontend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoFrontend").finish_non_exhaustive()
    }
}

impl MongoFrontend {
    /// The shared state, for `Sql.Query`'s `language: mongo` (chapter 37,
    /// tui T2; added by bugfix4).
    #[must_use]
    pub fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }

    /// Builds the front end and starts the cursor and transaction reaper,
    /// which stops on `stop`.
    #[must_use]
    pub fn start(d: MongoDeps, stop: CancellationToken) -> Self {
        // The publication wait (chapter 31's check_flags) needs the DC
        // tier, where gateways acknowledge catalog versions.
        let publisher = match (&d.tenants, &d.dc_store) {
            (Some(t), Some(dc)) => {
                let p = Arc::new(publish::Publisher::new(
                    Arc::clone(dc),
                    Arc::clone(t),
                    d.gateway_id.clone(),
                ));
                tokio::spawn(Arc::clone(&p).run(stop.clone()));
                Some(p)
            }
            _ => None,
        };
        let docs = d.tenants.as_ref().map(|t| {
            let c = catalog::CatalogDocs::new(Arc::clone(t));
            Arc::new(match publisher {
                Some(p) => c.with_publisher(p),
                None => c,
            })
        });
        let feed = match (&d.tenants, &d.dc_store) {
            (Some(t), Some(dc)) => {
                let locator: Arc<dyn sprig_catalog::CatalogLocator> = t.locator.clone();
                match changes::ChangeFeed::new(Arc::clone(&t.global), Arc::clone(dc), locator) {
                    Ok(f) => Some(f),
                    Err(e) => {
                        tracing::warn!(error = %e, "change streams unavailable");
                        None
                    }
                }
            }
            _ => None,
        };
        let mut decoy_secret = [0u8; 32];
        let _ = getrandom::fill(&mut decoy_secret);
        let shared = Arc::new(Shared {
            cursors: cursors::CursorTable::new(&d.gateway_id),
            txns: commands::TxnTable::default(),
            snapshots: snapshot::Snapshots::default(),
            records: retry::Records::default(),
            failpoints: failpoint::FailPoints::default(),
            engine: match &docs {
                Some(c) => sprig_doc::engine::Engine::with_catalog(
                    sprig_doc::engine::EngineConfig::default(),
                    Arc::clone(c) as Arc<dyn sprig_doc::catalog::DocCatalog>,
                ),
                None => sprig_doc::engine::Engine::default(),
            },
            feed,
            docs,
            decoy_secret,
            process_id: bson::oid::ObjectId::new(),
            conn_ids: std::sync::atomic::AtomicI32::new(1),
            started: std::time::Instant::now(),
            d,
        });
        let s = Arc::clone(&shared);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = stop.cancelled() => return,
                    () = tokio::time::sleep(Duration::from_secs(5)) => {}
                }
                let cfg = s.settings();
                s.cursors.reap(cfg.cursor_timeout);
                s.txns.reap(cfg.txn_lifetime).await;
                s.snapshots.reap().await;
            }
        });
        Self { shared }
    }
}

#[async_trait]
impl Frontend for MongoFrontend {
    fn protocol(&self) -> Protocol {
        Protocol::Mongo
    }

    async fn serve(&self, conn: ClientConn, _ctx: Arc<GatewayContext>) -> Result<()> {
        let peer = conn.peer;
        if let Err(e) = Box::pin(conn::serve(Arc::clone(&self.shared), conn)).await {
            tracing::debug!(%peer, error = %e, "mongo connection ended");
        }
        Ok(())
    }
}
