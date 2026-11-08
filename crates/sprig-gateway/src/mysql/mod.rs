//! The MySQL 8.4 (3306) and MariaDB (3307) front end (chapter 05; chapter
//! 04, "Session", "Dispatch into the layers", "Live listener changes").
//!
//! [`MySqlFrontend::serve`] runs one client connection inside the session
//! table's task wrapper (panic isolation, `KILL`):
//!
//! 1. **Connection phase** (chapter 05 step 3): a session and its
//!    connection id; the greeting with the persona's version string (read
//!    live) and a fresh nonce; `SSL` advertised per the listener snapshot's
//!    TLS mode; on an `SSLRequest` the TLS upgrade with the listener's
//!    acceptor ([`ClientConn::start_tls`]); 1043 / 3159 / 1251 refusals;
//!    capability negotiation and connection attributes; all within
//!    `gateway.handshake_timeout_ms`.
//! 2. **Login** (step 4): [`sprig_proto_mysql::auth::authenticate`] with
//!    the gateway's [`login::AccountLogin`] flow over the interim accounts
//!    file (decision D1) and the fast cache; the executor's session opens;
//!    `CONNECT_WITH_DB` runs `USE`; then the OK packet, after which the
//!    codec switches to zlib or zstd if agreed (step 10).
//! 3. **Command phase** (step 6, [`dispatch`]): one command at a time,
//!    statements through the [`pipeline`](crate::pipeline) executor.
//!
//! [`listener_hooks`] retires sessions of a tightened listener (step 12).

pub mod dispatch;
pub mod listener_hooks;
pub(crate) mod local;
pub mod login;
pub mod settings;
pub(crate) mod sink;
pub(crate) mod watch;

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_auth::Account;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::auth::{AuthIo, AuthVerdict, authenticate};
use sprig_proto_mysql::handshake::{
    ClientHello, MAX_CONNECTION_PACKET, RawIo, Refusal, TlsPolicy, check_transport, greeting,
    limit_attrs, negotiate, new_nonce, parse_client_hello, read_packet, server_caps, write_packet,
};
use sprig_proto_mysql::packets::{OkPacket, SessionChange};
use sprig_proto_mysql::state::{ProtoState, TlsInfo as ProtoTlsInfo};
use sprig_proto_mysql::{Caps, ErrPacket, Persona, StatusFlags};
use tokio::net::TcpStream;

use crate::frontend::{
    ClientConn, Frontend, GatewayContext, InProtocolTls, MaybeTls, Protocol, TlsInfo,
};
use crate::pipeline::{ExecCtx, LoginInfo, SessionExec, SharedExecutor};
use crate::session::{Session, SessionState, SessionTable};

use self::listener_hooks::ListenerHooks;
use self::login::{AccountLogin, LoginDeps};
use self::settings::{FrontendSettings, SettingsSource};

/// What a MySQL-family front end is built from.
pub struct MySqlConfig {
    /// The persona, fixed by the listener.
    pub persona: Persona,
    /// Where the live settings come from.
    pub settings: SettingsSource,
    /// Accounts, fast cache and hashing slots, shared by both personas.
    pub login: Arc<LoginDeps>,
    /// The executor statements go to.
    pub executor: SharedExecutor,
    /// The registry the listener hooks retire sessions through.
    pub hooks: ListenerHooks,
    /// `@@hostname`.
    pub hostname: String,
}

impl fmt::Debug for MySqlConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MySqlConfig")
            .field("persona", &self.persona)
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

/// State shared by a front end's connections.
pub(crate) struct Shared {
    pub(crate) cfg: MySqlConfig,
    pub(crate) started: Instant,
    /// `COM_STATISTICS`' `Questions`.
    pub(crate) questions: AtomicU64,
}

/// The MySQL or MariaDB front end.
pub struct MySqlFrontend {
    shared: Arc<Shared>,
}

impl fmt::Debug for MySqlFrontend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MySqlFrontend")
            .field("persona", &self.shared.cfg.persona)
            .finish_non_exhaustive()
    }
}

impl MySqlFrontend {
    /// A front end for `cfg.persona`.
    #[must_use]
    pub fn new(cfg: MySqlConfig) -> Self {
        Self {
            shared: Arc::new(Shared {
                cfg,
                started: Instant::now(),
                questions: AtomicU64::new(0),
            }),
        }
    }
}

/// What [`MySqlStack::start`] needs from the process.
pub struct StackConfig {
    /// `[gateway] accounts_file` (decision D1); `None` refuses every login.
    pub accounts_file: Option<std::path::PathBuf>,
    /// The live settings mirror.
    pub live: sprig_settings::LiveSettings,
    /// This gateway's DC.
    pub dc: sprig_common::ids::DcId,
    /// `@@hostname`: the node id.
    pub hostname: String,
    /// The clock (fast-cache expiry).
    pub clock: Arc<dyn sprig_common::time::Clock>,
    /// Where statements go.
    pub executor: SharedExecutor,
}

impl fmt::Debug for StackConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StackConfig")
            .field("accounts_file", &self.accounts_file)
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

/// Both MySQL-family front ends with what they share: the accounts (and
/// their file watcher), the fast cache, and the listener hooks' follower.
pub struct MySqlStack {
    mysql: Arc<MySqlFrontend>,
    mariadb: Arc<MySqlFrontend>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl fmt::Debug for MySqlStack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MySqlStack").finish_non_exhaustive()
    }
}

impl MySqlStack {
    /// Loads the accounts file and starts its watcher and the listener
    /// hooks, until `stop` is cancelled.
    ///
    /// # Errors
    /// 39998 if the accounts file exists but can't be read or parsed (a
    /// broken file is refused at start-up).
    pub fn start(cfg: StackConfig, stop: sprig_common::CancellationToken) -> Result<Self> {
        let mut tasks = Vec::new();
        let accounts: Arc<dyn sprig_auth::AccountStore> = if let Some(path) = &cfg.accounts_file {
            let file = sprig_auth::FileAccounts::open(path)?;
            tasks.push(file.spawn_watcher(sprig_auth::accounts::POLL_INTERVAL, stop.clone()));
            file
        } else {
            tracing::warn!("no [gateway] accounts_file: every MySQL login is refused (1045)");
            Arc::new(sprig_auth::StaticAccounts::default())
        };
        let fast_cache = Arc::new(sprig_auth::FastCache::new(
            cfg.clock,
            sprig_auth::fast_cache::DEFAULT_TTL,
        ));
        let login = Arc::new(LoginDeps::new(accounts, fast_cache));
        let hooks = ListenerHooks::new();
        tasks.push(hooks.spawn_follower(cfg.live.clone(), cfg.dc.clone(), stop));
        let settings = SettingsSource::Live {
            live: cfg.live,
            dc: cfg.dc,
        };
        let fe = |persona| {
            Arc::new(MySqlFrontend::new(MySqlConfig {
                persona,
                settings: settings.clone(),
                login: Arc::clone(&login),
                executor: Arc::clone(&cfg.executor),
                hooks: hooks.clone(),
                hostname: cfg.hostname.clone(),
            }))
        };
        Ok(Self {
            mysql: fe(Persona::MySql84),
            mariadb: fe(Persona::MariaDb),
            tasks,
        })
    }

    /// Adds both front ends to `frontends`.
    #[must_use]
    pub fn register(&self, frontends: crate::listener::Frontends) -> crate::listener::Frontends {
        frontends
            .with(Arc::clone(&self.mysql) as Arc<dyn Frontend>)
            .with(Arc::clone(&self.mariadb) as Arc<dyn Frontend>)
    }

    /// Waits for the background tasks after `stop` was cancelled.
    pub async fn join(self) {
        for t in self.tasks {
            let _ = t.await;
        }
    }
}

/// The front-end protocol of a persona.
#[must_use]
pub fn protocol_of(persona: Persona) -> Protocol {
    match persona {
        Persona::MySql84 => Protocol::MySql84,
        Persona::MariaDb => Protocol::MariaDb,
    }
}

#[async_trait]
impl Frontend for MySqlFrontend {
    fn protocol(&self) -> Protocol {
        protocol_of(self.shared.cfg.persona)
    }

    async fn serve(&self, mut conn: ClientConn, ctx: Arc<GatewayContext>) -> Result<()> {
        let session = match ctx.sessions.open(&conn, self.protocol()) {
            Ok(s) => s,
            Err(e) => {
                // 1040 in place of the greeting, without a SQLSTATE: the
                // client's capabilities aren't known yet.
                let p = ErrPacket::from_error(self.shared.cfg.persona, &e);
                let mut seq = 0;
                let _ = write_packet(&mut conn.stream, &mut seq, &p.encode(Caps::empty())).await;
                return Err(e);
            }
        };
        let shared = Arc::clone(&self.shared);
        SessionTable::run_session(session, conn, move |session, conn| {
            connection(shared, ctx, session, conn)
        })
        .await
    }
}

/// A logged-in connection, ready for the command phase.
pub(crate) struct LoggedIn {
    pub(crate) stream: MaybeTls<TcpStream>,
    pub(crate) proto: ProtoState,
    pub(crate) account: Arc<Account>,
    pub(crate) exec: Box<dyn SessionExec>,
    pub(crate) db: Option<String>,
    pub(crate) attrs: Vec<(String, String)>,
}

async fn connection(
    shared: Arc<Shared>,
    ctx: Arc<GatewayContext>,
    mut session: Session,
    conn: ClientConn,
) -> Result<()> {
    let _registered = shared.cfg.hooks.register(session.handle(), &conn.listener);
    let settings = shared.cfg.settings.current();
    let timeout = conn.listener.handshake_timeout;
    let phase =
        tokio::time::timeout(timeout, login_phase(&shared, &settings, &mut session, conn)).await;
    let logged_in = match phase {
        Err(_) => {
            tracing::debug!(session = %session.id, "connection phase timed out");
            return Ok(());
        }
        Ok(r) => match r? {
            Some(l) => l,
            None => return Ok(()),
        },
    };
    dispatch::CommandLoop::new(shared, ctx, session, logged_in, settings)
        .run()
        .await
}

fn proto_tls(info: Option<&TlsInfo>) -> Option<ProtoTlsInfo> {
    info.map(|i| ProtoTlsInfo {
        version: i.version,
        cipher: i.cipher,
        sni: i.sni.clone(),
    })
}

fn count_refusal(persona: Persona, r: Refusal) {
    let reason = match r {
        Refusal::SecureTransportRequired => "listener_required",
        Refusal::BadHandshake => "listener_off",
        Refusal::AuthModeNotSupported => return,
    };
    metrics::counter!("sprig_mysql_plaintext_refused_total", "persona" => persona.as_str(), "reason" => reason).increment(1);
}

/// Greeting, TLS, negotiation and login, in the order chapter 05's state
/// machine runs them. `Ok(None)`: the client was refused
/// (the ERR packet is sent) or left.
#[allow(clippy::too_many_lines)] // the connection phase, step by step
async fn login_phase(
    shared: &Shared,
    settings: &FrontendSettings,
    session: &mut Session,
    mut conn: ClientConn,
) -> Result<Option<LoggedIn>> {
    let persona = shared.cfg.persona;
    let policy = match conn.in_protocol_tls {
        InProtocolTls::NotOffered => TlsPolicy::Off,
        InProtocolTls::Allowed => TlsPolicy::Optional,
        InProtocolTls::Required => TlsPolicy::Required,
    };
    let nonce = new_nonce()?;
    let server = server_caps(persona, policy.offered(), settings.compression);
    let hs = greeting(
        persona,
        settings.version(persona),
        session.id.0,
        &nonce,
        server,
    );
    let mut seq = 0u8;
    write_packet(&mut conn.stream, &mut seq, &hs.encode()).await?;

    let refuse = async |stream: &mut MaybeTls<TcpStream>, seq: &mut u8, r: Refusal, caps: Caps| {
        count_refusal(persona, r);
        tracing::debug!(reason = ?r, "handshake refused");
        let _ = write_packet(stream, seq, &r.err(persona).encode(caps)).await;
    };

    // An error here: the client left (or probed the port).
    let Ok(first) = read_packet(&mut conn.stream, &mut seq, MAX_CONNECTION_PACKET).await else {
        return Ok(None);
    };
    let hello = match parse_client_hello(&first) {
        Ok(h) => h,
        Err(r) => {
            refuse(&mut conn.stream, &mut seq, r, Caps::empty()).await;
            return Ok(None);
        }
    };
    let hello_caps = match &hello {
        ClientHello::SslRequest(s) => s.caps,
        ClientHello::Response(r) => r.caps,
    } & Caps::PROTOCOL_41;
    if let Err(r) = check_transport(policy, &hello, false) {
        refuse(&mut conn.stream, &mut seq, r, hello_caps).await;
        return Ok(None);
    }
    let resp = match hello {
        ClientHello::Response(r) => r,
        ClientHello::SslRequest(_) => {
            conn = match conn.start_tls().await {
                Ok(c) => c,
                Err(e) => {
                    tracing::debug!(error = %e, "TLS handshake failed");
                    return Ok(None);
                }
            };
            session.tls = conn.tls_info();
            let Ok(p) = read_packet(&mut conn.stream, &mut seq, MAX_CONNECTION_PACKET).await else {
                return Ok(None);
            };
            match parse_client_hello(&p) {
                Ok(hello @ ClientHello::Response(_)) => {
                    if let Err(r) = check_transport(policy, &hello, true) {
                        refuse(&mut conn.stream, &mut seq, r, hello_caps).await;
                        return Ok(None);
                    }
                    let ClientHello::Response(r) = hello else {
                        return Ok(None);
                    };
                    r
                }
                Ok(ClientHello::SslRequest(_)) | Err(_) => {
                    refuse(
                        &mut conn.stream,
                        &mut seq,
                        Refusal::BadHandshake,
                        hello_caps,
                    )
                    .await;
                    return Ok(None);
                }
            }
        }
    };
    let negotiated = match negotiate(server, &resp, settings.max_allowed_packet) {
        Ok(n) => n,
        Err(r) => {
            refuse(&mut conn.stream, &mut seq, r, resp.caps & Caps::PROTOCOL_41).await;
            return Ok(None);
        }
    };
    let caps = negotiated.caps;
    let (attrs, dropped) = limit_attrs(resp.attrs.clone(), settings.max_connect_attrs);
    if dropped > 0 {
        metrics::counter!("sprig_mysql_connect_attrs_dropped_total", "persona" => persona.as_str())
            .increment(dropped as u64);
    }

    // Login (chapter 05 step 4, chapter 14 "MySQL logins").
    session.set_state(SessionState::Authenticating);
    let tls = matches!(conn.stream, MaybeTls::Tls(_));
    let mut login = AccountLogin::new(
        Arc::clone(&shared.cfg.login),
        persona,
        &resp.user,
        conn.peer,
    );
    let (verdict, nonce, seq) = {
        let mut raw = RawIo::new(&mut conn.stream, seq);
        let mut io = AuthIo {
            io: &mut raw,
            persona,
            tls,
            nonce,
        };
        let v = authenticate(
            &mut io,
            resp.plugin.as_deref(),
            resp.auth_response.clone(),
            false,
            &mut login,
        )
        .await?;
        let nonce = io.nonce;
        (v, nonce, raw.seq)
    };
    let mut seq = seq;
    let account = match verdict {
        AuthVerdict::Refuse(e) => {
            let _ = write_packet(&mut conn.stream, &mut seq, &e.encode(caps)).await;
            return Ok(None);
        }
        AuthVerdict::Accept => match (login.account().cloned(), login.principal()) {
            (Some(a), Some(p)) => {
                session.set_principal(p);
                a
            }
            _ => return Err(SprigError::internal("login accepted without an account")),
        },
    };

    let proto = ProtoState {
        persona,
        caps,
        collation: negotiated.collation,
        multi_statements: negotiated.multi_statements,
        framing: negotiated.framing,
        max_packet: negotiated.max_packet,
        tls: proto_tls(session.tls.as_ref()),
        nonce,
    };
    let principal = session
        .principal
        .clone()
        .ok_or_else(|| SprigError::internal("no principal after login"))?;
    let mut exec = shared.cfg.executor.open(&LoginInfo {
        principal,
        backend: account.backend.clone(),
        persona,
    });
    let db = resp
        .database
        .as_ref()
        .map(|d| String::from_utf8_lossy(d).into_owned());
    if let Some(schema) = &db {
        let mut cx = ExecCtx {
            session: &mut *session,
            proto: &proto,
            db: None,
        };
        if let Err(e) = exec.init_db(&mut cx, schema).await {
            let p = ErrPacket::from_error(persona, &e);
            let _ = write_packet(&mut conn.stream, &mut seq, &p.encode(caps)).await;
            exec.close().await;
            return Ok(None);
        }
        session.set_db(db.clone());
    }
    let ok = auth_ok(caps, db.as_deref());
    write_packet(&mut conn.stream, &mut seq, &ok).await?;
    session.set_state(SessionState::Ready);
    tracing::debug!(
        session = %session.id,
        user = %account.user,
        tls = tls,
        client = attrs.iter().find(|(k, _)| k == "_client_name").map(|(_, v)| v.as_str()),
        "logged in"
    );
    Ok(Some(LoggedIn {
        stream: conn.stream,
        proto,
        account,
        exec,
        db,
        attrs,
    }))
}

/// The OK packet that ends a login, with the default schema in session
/// tracking when the client named one.
pub(crate) fn auth_ok(caps: Caps, db: Option<&str>) -> Bytes {
    let mut ok = OkPacket::new(StatusFlags::AUTOCOMMIT);
    if let Some(db) = db.filter(|_| caps.contains(Caps::SESSION_TRACK)) {
        ok.set_session_changes(&[SessionChange::Schema(Bytes::copy_from_slice(db.as_bytes()))]);
    }
    ok.encode(caps)
}

#[cfg(test)]
mod tests;
