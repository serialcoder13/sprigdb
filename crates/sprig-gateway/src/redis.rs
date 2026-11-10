//! The Redis front end (chapter 40, R6): the gateway answers RESP2 and
//! RESP3 on the `redis` listener, with `sprig-redis` doing the commands
//! over chapter 33's key-value tables.
//!
//! - **Logins** (chapter 14): `AUTH user pass` and `HELLO ... AUTH` run
//!   the same login as HTTP Basic and AMQP PLAIN (`ClearText`, the fast
//!   cache first). The tenant comes from the TLS server name, a dotted
//!   user name (`acme.alice`), or `listener.redis.tenant`. One-argument
//!   `AUTH` logs in as `redis.default_user`.
//! - **SQL**: a login opens the principal's own executor session, so name
//!   virtualization, chapter 29's privileges, the router and workload
//!   capture apply to every statement a command runs; the statements run
//!   in internal mode so the `__sprig_` element tables pass chapter 07's
//!   reserved-name check.

pub mod hub;

use std::borrow::Cow;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_auth::LoginSource;
use sprig_auth::account::parse_login;
use sprig_auth::mysql::{LoginRequest, LoginService};
use sprig_auth::store::LoginForm;
use sprig_common::ids::DcId;
use sprig_common::{Result, SprigError};
use sprig_kv::{Row, SqlError, SqlRunner};
use sprig_proto_mysql::auth::{AuthPath, AuthVerdict, LoginFlow, Outcome};
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::state::ProtoState;
use sprig_proto_mysql::{Caps, ColumnDef, ErrPacket, Framing, Persona};
use sprig_proto_redis::Frame;
use sprig_redis::session::{Auth, Login, wrongpass};
use sprig_settings::{Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx};
use zeroize::Zeroizing;

use crate::frontend::{ClientConn, Frontend, GatewayContext, Protocol, TlsInfo};
use crate::pipeline::{
    EndOfRows, ExecCtx, LoginInfo, Request, ResultSink, RowFormat, RowValues, SessionExec,
    SharedExecutor, Wire,
};
use crate::session::{Session, SessionState, SessionTable};

type Text = Cow<'static, str>;

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $hard:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: Scopes::PROCESS,
            hard: $hard,
            apply: Apply::Live,
            operator_only: false,
            owner: 40,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key!(
    /// `listener.redis.tenant`: the tenant of plain user names.
    LISTENER_TENANT: Text = "listener.redis.tenant", Cow::Borrowed(""), HardBounds::NONE,
    "Tenant of plain user names on the Redis listener"
);
key!(
    /// `redis.server_version`.
    SERVER_VERSION: Text = "redis.server_version", Cow::Borrowed("7.2.0"), HardBounds::NONE,
    "HELLO and INFO version"
);
key!(
    /// `redis.databases`.
    DATABASES: u64 = "redis.databases", 16, HardBounds::range(1, 1024),
    "Highest SELECT index + 1"
);
key!(
    /// `redis.logical_database`.
    LOGICAL_DATABASE: Text = "redis.logical_database", Cow::Borrowed("redis"), HardBounds::NONE,
    "Home of the Redis databases' namespaces"
);
key!(
    /// `redis.default_user`: the login of one-argument `AUTH`.
    DEFAULT_USER: Text = "redis.default_user", Cow::Borrowed(""), HardBounds::NONE,
    "User for one-argument AUTH; empty refuses it"
);
key!(
    /// `redis.max_bulk_bytes`.
    MAX_BULK_BYTES: u64 = "redis.max_bulk_bytes", 16 << 20, HardBounds::range(1024, 512 << 20),
    "Largest bulk string"
);
key!(
    /// `redis.max_command_bytes`.
    MAX_COMMAND_BYTES: u64 = "redis.max_command_bytes", 64 << 20, HardBounds::range(1024, 1 << 30),
    "Largest command"
);
key!(
    /// `redis.max_multi_commands`.
    MAX_MULTI_COMMANDS: u64 = "redis.max_multi_commands", 10_000, HardBounds::range(1, 1_000_000),
    "Queued commands per MULTI"
);
key!(
    /// `redis.write_retries`.
    WRITE_RETRIES: u64 = "redis.write_retries", 10, HardBounds::range(0, 100),
    "Internal retries on conflicts"
);
key!(
    /// `redis.block_poll_ms`.
    BLOCK_POLL_MS: u64 = "redis.block_poll_ms", 1000, HardBounds::range(10, 60_000),
    "Safety poll for blocked clients"
);
key!(
    /// `redis.max_blocked_per_tenant`.
    MAX_BLOCKED_PER_TENANT: u64 = "redis.max_blocked_per_tenant", 10_000, HardBounds::range(1, 10_000_000),
    "Blocked clients per tenant per gateway"
);
key!(
    /// `redis.scan_small_key_elements`.
    SCAN_SMALL_KEY_ELEMENTS: u64 = "redis.scan_small_key_elements", 128, HardBounds::range(0, 100_000),
    "Element scans returned whole"
);
key!(
    /// `redis.lua_time_limit_ms`.
    LUA_TIME_LIMIT_MS: u64 = "redis.lua_time_limit_ms", 5000, HardBounds::range(10, 3_600_000),
    "Wall-time budget of one script"
);
key!(
    /// `redis.lua_memory_limit_mb`.
    LUA_MEMORY_LIMIT_MB: u64 = "redis.lua_memory_limit_mb", 32, HardBounds::range(1, 4096),
    "Lua heap of one run"
);
key!(
    /// `redis.lua_kill_after_ms`.
    LUA_KILL_AFTER_MS: u64 = "redis.lua_kill_after_ms", 1000, HardBounds::range(0, 3_600_000),
    "SCRIPT KILL stops only runs older than this"
);
key!(
    /// `redis.lua_undeclared_keys`: `refuse` or `same_shard`.
    LUA_UNDECLARED_KEYS: Text = "redis.lua_undeclared_keys", Cow::Borrowed("refuse"), HardBounds::NONE,
    "Keys a script may touch outside KEYS"
);

key!(
    /// `redis.sharded_pubsub_hubs`: hubs for sharded channels.
    SHARDED_PUBSUB_HUBS: u64 = "redis.sharded_pubsub_hubs", 8, HardBounds::range(1, 1024),
    "Hubs for sharded channels"
);

/// The hubs' timing from settings (read when the gateway starts).
#[must_use]
pub fn mesh_opts(live: &LiveSettings, dc: &DcId) -> sprig_redis::fabric::MeshOpts {
    let cx = SettingCtx::process(dc);
    sprig_redis::fabric::MeshOpts {
        sharded_hubs: u32::try_from(live.get(&SHARDED_PUBSUB_HUBS, &cx)).unwrap_or(8),
        ..sprig_redis::fabric::MeshOpts::default()
    }
}

fn read_config(live: &LiveSettings, dc: &DcId) -> sprig_redis::Config {
    let cx = SettingCtx::process(dc);
    let us = |x: u64| usize::try_from(x).unwrap_or(usize::MAX);
    sprig_redis::Config {
        server_version: live.get(&SERVER_VERSION, &cx).into_owned(),
        databases: u32::try_from(live.get(&DATABASES, &cx)).unwrap_or(16),
        logical_database: live.get(&LOGICAL_DATABASE, &cx).into_owned(),
        max_multi_commands: us(live.get(&MAX_MULTI_COMMANDS, &cx)),
        write_retries: u32::try_from(live.get(&WRITE_RETRIES, &cx)).unwrap_or(3),
        block_poll: Duration::from_millis(live.get(&BLOCK_POLL_MS, &cx)),
        max_blocked_per_tenant: us(live.get(&MAX_BLOCKED_PER_TENANT, &cx)),
        scan_small_key_elements: us(live.get(&SCAN_SMALL_KEY_ELEMENTS, &cx)),
        lua_time_limit: Duration::from_millis(live.get(&LUA_TIME_LIMIT_MS, &cx)),
        lua_memory_bytes: us(live.get(&LUA_MEMORY_LIMIT_MB, &cx) << 20),
        lua_kill_after: Duration::from_millis(live.get(&LUA_KILL_AFTER_MS, &cx)),
        lua_undeclared_same_shard: live.get(&LUA_UNDECLARED_KEYS, &cx) == "same_shard",
        max_bulk_bytes: us(live.get(&MAX_BULK_BYTES, &cx)),
        max_command_bytes: us(live.get(&MAX_COMMAND_BYTES, &cx)),
        ..sprig_redis::Config::default()
    }
}

/// What the front end is built from.
pub struct RedisDeps {
    /// Logins (chapter 14).
    pub login: Arc<LoginService>,
    /// The shared executor.
    pub executor: SharedExecutor,
    /// The gateway's sessions.
    pub sessions: Arc<SessionTable>,
    /// Live settings.
    pub live: LiveSettings,
    /// This gateway's data center.
    pub dc: DcId,
    /// This gateway's id.
    pub gateway_id: String,
}

/// The Redis front end.
pub struct RedisFrontend {
    d: Arc<RedisDeps>,
    env: Arc<sprig_redis::Env>,
}

impl std::fmt::Debug for RedisFrontend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisFrontend").finish_non_exhaustive()
    }
}

impl RedisFrontend {
    /// Builds the front end.
    #[must_use]
    pub fn new(d: RedisDeps) -> Self {
        let env = Arc::new(sprig_redis::Env::new(
            read_config(&d.live, &d.dc),
            &d.gateway_id,
        ));
        Self {
            d: Arc::new(d),
            env,
        }
    }

    /// The gateway's shared Redis state (the hubs join it).
    #[must_use]
    pub fn env(&self) -> &Arc<sprig_redis::Env> {
        &self.env
    }
}

#[async_trait]
impl Frontend for RedisFrontend {
    fn protocol(&self) -> Protocol {
        Protocol::Redis
    }

    async fn serve(&self, conn: ClientConn, _ctx: Arc<GatewayContext>) -> Result<()> {
        let cfg = read_config(&self.d.live, &self.d.dc);
        let limits = sprig_proto_redis::Limits {
            max_bulk: cfg.max_bulk_bytes,
            max_command: cfg.max_command_bytes,
        };
        self.env.set_config(cfg);
        let auth: Arc<dyn Auth> = Arc::new(GatewayAuth {
            d: Arc::clone(&self.d),
            peer: conn.peer,
            listener: conn.listener.name,
            generation: conn.generation,
            tls: conn.tls_info(),
        });
        let peer = conn.peer;
        let sess = sprig_redis::Session::new(Arc::clone(&self.env), auth, &peer.to_string());
        let stop = conn.close_soon.clone();
        metrics::gauge!("sprig_redis_connections", "dc" => self.d.dc.to_string()).increment(1.0);
        let r =
            sprig_redis::conn::serve(
                conn.stream,
                sess,
                limits,
                async move { stop.cancelled().await },
            )
            .await;
        metrics::gauge!("sprig_redis_connections", "dc" => self.d.dc.to_string()).decrement(1.0);
        if let Err(e) = r {
            tracing::debug!(%peer, error = %e, "redis connection ended");
        }
        Ok(())
    }
}

/// Chapter 14's login for one connection.
struct GatewayAuth {
    d: Arc<RedisDeps>,
    peer: SocketAddr,
    listener: &'static str,
    generation: u64,
    tls: Option<TlsInfo>,
}

impl GatewayAuth {
    /// `user` as sent, or `<listener tenant>.<user>`.
    fn login_name(&self, user: &str) -> String {
        let bound = self
            .d
            .live
            .get(&LISTENER_TENANT, &SettingCtx::process(&self.d.dc));
        if bound.is_empty() || user.contains('.') {
            return user.to_owned();
        }
        let sni_names_tenant = match (
            self.d.login.source(),
            self.tls.as_ref().and_then(|t| t.sni.as_deref()),
        ) {
            (LoginSource::Catalog(cl), Some(sni)) => {
                parse_login(user, Some(sni), &cl.sni_suffixes(), 255)
                    .is_some_and(|p| matches!(p.form, LoginForm::Plain))
            }
            _ => false,
        };
        if sni_names_tenant {
            user.to_owned()
        } else {
            format!("{bound}.{user}")
        }
    }
}

#[async_trait]
impl Auth for GatewayAuth {
    async fn login(&self, user: Option<&[u8]>, pass: &[u8]) -> std::result::Result<Login, Frame> {
        let user = if let Some(u) = user {
            String::from_utf8_lossy(u).into_owned()
        } else {
            let d = self
                .d
                .live
                .get(&DEFAULT_USER, &SettingCtx::process(&self.d.dc))
                .into_owned();
            if d.is_empty() {
                return Err(Frame::err(
                    "AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?",
                ));
            }
            d
        };
        if self.d.login.admit(self.peer.ip()).is_err() {
            return Err(wrongpass());
        }
        let name = self.login_name(&user);
        let mut login = self
            .d
            .login
            .start_http(LoginRequest {
                persona: Persona::MySql84,
                login_name: name.into_bytes(),
                sni: self.tls.as_ref().and_then(|t| t.sni.clone()),
                client_ip: self.peer.ip(),
                tls: self.tls.is_some(),
                client_cert: self.tls.as_ref().is_some_and(|t| t.client_cert),
                can_handle_expired_password: false,
            })
            .await;
        let password = pass.to_vec();
        let (outcome, path) = if login.check_clear_fast(&password) {
            (Outcome::Verified, AuthPath::Fast)
        } else if login.check_password(Zeroizing::new(password)).await {
            (Outcome::Verified, AuthPath::Full)
        } else {
            (Outcome::WrongCredentials, AuthPath::Full)
        };
        let verdict = login.decide(outcome, path).await;
        let principal = match (&verdict, login.principal()) {
            (AuthVerdict::Accept, Some(p)) if p.tenant.is_some() => p.clone(),
            _ => {
                metrics::counter!("sprig_redis_auth_total", "result" => "failed").increment(1);
                return Err(wrongpass());
            }
        };
        metrics::counter!("sprig_redis_auth_total", "result" => "ok").increment(1);
        let scope = principal
            .tenant
            .map_or_else(String::new, |t| t.0.to_string());
        let user_name = principal.user.clone();
        let sql = SqlSession::open(
            &self.d.sessions,
            &self.d.executor,
            &principal,
            self.listener,
            self.generation,
            self.peer,
            self.tls.clone(),
        )
        .map_err(|e| Frame::err(&e.message))?;
        Ok(Login {
            sql: Box::new(sql),
            scope,
            user: user_name,
        })
    }
}

/// The capabilities the executor sees.
fn caps() -> Caps {
    let mut c = Caps::server_default(Persona::MySql84);
    c.remove(Caps::SSL | Caps::COMPRESS | Caps::ZSTD_COMPRESSION_ALGORITHM | Caps::LOCAL_FILES);
    c
}

/// Collects one statement's result.
#[derive(Default)]
struct Collect {
    columns: usize,
    rows: Vec<Row>,
    affected: u64,
    error: Option<SprigError>,
    done: bool,
}

#[async_trait]
impl ResultSink for Collect {
    fn wire(&self) -> Wire {
        Wire {
            persona: Persona::MySql84,
            caps: caps(),
        }
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], _format: RowFormat) -> Result<()> {
        self.columns = columns.len();
        Ok(())
    }

    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        match values {
            RowValues::Text(v) => self
                .rows
                .push(v.iter().map(|c| c.map(<[u8]>::to_vec)).collect()),
            RowValues::Binary(_) => {
                return Err(SprigError::internal("a binary row for the Redis front end"));
            }
        }
        Ok(())
    }

    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        let r = sprig_proto_mysql::resultset::decode_text_row(&payload, self.columns)?
            .into_iter()
            .map(|c| c.map(<[u8]>::to_vec))
            .collect();
        self.rows.push(r);
        Ok(())
    }

    async fn end_rows(&mut self, _end: EndOfRows) -> Result<()> {
        self.done = true;
        Ok(())
    }

    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.affected = ok.affected_rows;
        self.done = true;
        Ok(())
    }

    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.error = Some(SprigError {
            kind: err.kind,
            message: err.message.clone(),
            retryable: err.retryable,
            source: None,
        });
        self.done = true;
        Ok(())
    }

    async fn column_list(&mut self, _c: &[ColumnDef], _e: EndOfRows) -> Result<()> {
        Err(SprigError::internal(
            "COM_FIELD_LIST from the Redis front end",
        ))
    }

    async fn local_infile_request(&mut self, _f: &[u8]) -> Result<()> {
        Err(SprigError::standard(
            3948,
            *b"42000",
            "Loading local data is disabled",
        ))
    }

    async fn local_infile_read(&mut self) -> Result<Bytes> {
        Ok(Bytes::new())
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn finished(&self) -> bool {
        self.done
    }
}

fn sql_error(e: &SprigError) -> SqlError {
    let p = ErrPacket::from_error(Persona::MySql84, e);
    SqlError {
        errno: Some(p.code),
        message: p.message,
    }
}

/// One executor session of a Redis login (also the API's key-value calls).
pub(crate) struct SqlSession {
    session: Session,
    exec: Box<dyn SessionExec>,
    proto: ProtoState,
}

impl SqlSession {
    /// Opens the principal's own executor session.
    pub(crate) fn open(
        sessions: &Arc<SessionTable>,
        executor: &SharedExecutor,
        principal: &crate::session::Principal,
        listener: &'static str,
        generation: u64,
        peer: SocketAddr,
        tls: Option<TlsInfo>,
    ) -> Result<Self> {
        let mut session =
            sessions.open_parts(Protocol::Redis, listener, generation, peer, tls.clone())?;
        session.set_principal(principal.clone());
        session.set_state(SessionState::Ready);
        let proto = ProtoState {
            persona: Persona::MySql84,
            caps: caps(),
            collation: u16::from(Persona::MySql84.greeting_collation()),
            multi_statements: false,
            framing: Framing::Plain,
            max_packet: 64 * 1024 * 1024,
            tls: tls.map(|t| sprig_proto_mysql::state::TlsInfo {
                version: t.version,
                cipher: t.cipher,
                sni: t.sni,
            }),
            nonce: [0; 20],
        };
        let exec = executor.open(&LoginInfo {
            principal: principal.clone(),
            backend: principal.backend.clone(),
            persona: Persona::MySql84,
        });
        Ok(Self {
            session,
            exec,
            proto,
        })
    }

    async fn run(&mut self, sql: &str) -> std::result::Result<Collect, SqlError> {
        let mut sink = Collect::default();
        self.session
            .begin_command("Query", Some(sql.chars().take(1024).collect()));
        self.exec.set_internal(true);
        let r = {
            let mut cx = ExecCtx {
                session: &mut self.session,
                proto: &self.proto,
                db: None,
            };
            let req = Request::Query {
                sql: Bytes::copy_from_slice(sql.as_bytes()),
                attrs: QueryAttrs::default(),
            };
            self.exec.execute(&mut cx, req, &mut sink).await
        };
        self.exec.set_internal(false);
        self.session.end_command();
        if let Err(e) = r {
            return Err(sql_error(&e));
        }
        if let Some(e) = sink.error.take() {
            return Err(sql_error(&e));
        }
        Ok(sink)
    }
}

impl Drop for SqlSession {
    fn drop(&mut self) {
        self.session.set_state(SessionState::Closing);
    }
}

#[async_trait]
impl SqlRunner for SqlSession {
    async fn query(&mut self, sql: &str) -> std::result::Result<Vec<Row>, SqlError> {
        Ok(self.run(sql).await?.rows)
    }

    async fn exec(&mut self, sql: &str) -> std::result::Result<u64, SqlError> {
        Ok(self.run(sql).await?.affected)
    }
}
