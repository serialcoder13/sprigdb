//! The Bolt listener on 7687 (chapter 43, "Bolt front end"; build steps
//! 17–19): `sprig-proto-bolt`'s server over chapter 14's logins and the
//! graph executor.
//!
//! - **Logins.** `basic` runs the same login as HTTP Basic and AMQP
//!   `PLAIN`: fast cache, verifiers, policy, lockout, audit. The tenant
//!   comes from the TLS server name or a dotted principal (`acme.alice`).
//!   `bearer` and other schemes fail with 39915; `imp_user` with 39916.
//! - **Databases.** A Bolt database is `<logical db>.<graph>`, or a graph
//!   in `bolt.logical_database`; without `db`, `bolt.default_graph`
//!   (`neo4j`), created natively on its first write. Reading a default
//!   graph that doesn't exist yet answers no rows.
//! - **Transactions** ([`session`]). Each auto-commit `RUN` is one
//!   MariaDB transaction; `BEGIN` holds one `sprig_ctl` connection of the
//!   tenant's cluster until `COMMIT`/`ROLLBACK`, at `READ COMMITTED`
//!   (`bolt.isolation`). The commit check refuses nodes deleted with their
//!   relationships left (39906).
//! - **Routing.** `ROUTE` answers one address for `ROUTE`, `READ` and
//!   `WRITE`: `bolt.advertise_address`, else the address the driver used.
//! - **Bookmarks and replica reads** ([`session`]): signed `SB1:`
//!   bookmarks carry the commit's GTID positions; reads in `mode: "r"` go
//!   to a replica that has waited for them (chapter 06's read-your-writes),
//!   else to the writer.
//! - **Streaming** ([`stream`]): reads whose rows don't depend on each
//!   other stream to `PULL` with back-pressure; others are buffered.

pub(crate) mod session;
mod stream;

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sprig_auth::LoginSource;
use sprig_auth::account::parse_login;
use sprig_auth::mysql::{LoginRequest, LoginService};
use sprig_auth::store::LoginForm;
use sprig_common::ids::DcId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_graph::temporal;
use sprig_graph::value::{Node, Value, node_element_id};
use sprig_proto_bolt::server::{BoltFailure, BoltService, BoltSession, Config, ConnInfo};
use sprig_proto_bolt::{BoltVersion, PackValue, structs};
use sprig_proto_mysql::Persona;
use sprig_proto_mysql::auth::{AuthPath, AuthVerdict, LoginFlow, Outcome};
use sprig_settings::{Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx};
use zeroize::Zeroizing;

use crate::frontend::{ClientConn, Frontend, GatewayContext, Protocol};
use crate::graphs::GatewayGraphs;

type Text = Cow<'static, str>;

/// Tenant and global.
const TENANT_GLOBAL: Scopes = Scopes(0b10_0100);
/// Database, tenant and global.
const DB_TENANT_GLOBAL: Scopes = Scopes(0b10_0110);

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $hard:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: $hard,
            apply: Apply::Live,
            operator_only: false,
            owner: 43,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key!(
    /// `bolt.max_version`: the highest Bolt version offered. This build
    /// speaks 5.0 to 5.8 (5.5 to 5.8 since R8 round 8).
    MAX_VERSION: Text = "bolt.max_version", Cow::Borrowed("5.8"), Scopes::PROCESS,
    HardBounds::one_of(&["5.0", "5.1", "5.2", "5.3", "5.4", "5.5", "5.6", "5.7", "5.8"]),
    "Highest Bolt version offered"
);
key!(
    /// `bolt.server_agent`: `server` in `HELLO`'s answer.
    SERVER_AGENT: Text = "bolt.server_agent", Cow::Borrowed("Neo4j/5.26.0"), Scopes::PROCESS,
    HardBounds::NONE, "server in HELLO's answer; drivers check the Neo4j/ prefix"
);
key!(
    /// `bolt.advertise_address`: the address in `ROUTE` answers; empty for
    /// the address the driver connected to.
    ADVERTISE_ADDRESS: Text = "bolt.advertise_address", Cow::Borrowed(""), Scopes::PROCESS,
    HardBounds::NONE, "Address in Bolt ROUTE answers; empty: the one the driver used"
);
key!(
    /// `bolt.routing_ttl_seconds`.
    ROUTING_TTL_SECONDS: u64 = "bolt.routing_ttl_seconds", 300, Scopes::PROCESS,
    HardBounds::range(1, 86_400), "Lifetime of a Bolt routing table, in seconds"
);
key!(
    /// `bolt.logical_database`: home of graphs named without a database.
    LOGICAL_DATABASE: Text = "bolt.logical_database", Cow::Borrowed("graph"), TENANT_GLOBAL,
    HardBounds::NONE, "Logical database of Bolt graphs named without a database"
);
key!(
    /// `bolt.default_graph`: the graph when `db` is absent.
    DEFAULT_GRAPH: Text = "bolt.default_graph", Cow::Borrowed("neo4j"), DB_TENANT_GLOBAL,
    HardBounds::NONE, "Graph of Bolt requests without db"
);
key!(
    /// `bolt.isolation`.
    ISOLATION: Text = "bolt.isolation", Cow::Borrowed("read_committed"), TENANT_GLOBAL,
    HardBounds::one_of(&["read_committed", "repeatable_read"]),
    "Isolation level of Bolt transactions"
);
key!(
    /// `bolt.max_buffered_result_mb`.
    MAX_BUFFERED_RESULT_MB: u64 = "bolt.max_buffered_result_mb", 64, TENANT_GLOBAL,
    HardBounds::range(1, 4096), "Buffered Bolt result size, in MiB"
);
key!(
    /// `bolt.recv_timeout_seconds`.
    RECV_TIMEOUT_SECONDS: u64 = "bolt.recv_timeout_seconds", 120, Scopes::PROCESS,
    HardBounds::range(1, 86_400), "Bolt receive-timeout hint; idle connections close after twice it"
);

fn failure(e: &SprigError) -> BoltFailure {
    BoltFailure::new(
        sprig_graph::neo4j::status_code(e),
        sprig_graph::neo4j::message(e),
    )
}

fn refuse(code: SprigCode, msg: impl Into<String>) -> BoltFailure {
    failure(&SprigError::new(code, msg))
}

// ----------------------------------------------------------------------
// Values.
// ----------------------------------------------------------------------

fn props_pack(p: &sprig_graph::value::Props) -> PackValue {
    PackValue::Map(p.iter().map(|(k, v)| (k.clone(), to_pack(v))).collect())
}

fn id_i64(id: u64) -> i64 {
    i64::try_from(id).unwrap_or(i64::MAX)
}

fn node_pack(n: &Node) -> PackValue {
    structs::node(
        id_i64(n.id),
        &n.labels,
        props_pack(&n.props),
        &n.element_id(),
    )
}

/// A graph value as PackStream: nodes, relationships and paths as Bolt
/// 5.x structures.
#[must_use]
#[allow(clippy::too_many_lines)] // one arm per value kind
pub fn to_pack(v: &Value) -> PackValue {
    match v {
        Value::Null => PackValue::Null,
        Value::Bool(b) => PackValue::Bool(*b),
        Value::Int(i) => PackValue::Int(*i),
        Value::Float(f) => PackValue::Float(*f),
        Value::String(s) => PackValue::String(s.clone()),
        Value::Bytes(b) => PackValue::Bytes(b.clone()),
        Value::List(l) => PackValue::List(l.iter().map(to_pack).collect()),
        Value::Map(m) => PackValue::Map(m.iter().map(|(k, x)| (k.clone(), to_pack(x))).collect()),
        Value::Node(n) => node_pack(n),
        Value::Rel(r) => {
            let s = node_element_id(r.graph, r.src_rk, r.src);
            let d = node_element_id(r.graph, r.dst_rk, r.dst);
            structs::relationship(
                id_i64(r.id),
                &structs::Ends {
                    start: id_i64(r.src),
                    end: id_i64(r.dst),
                    start_eid: &s,
                    end_eid: &d,
                },
                &r.rtype,
                props_pack(&r.props),
                &r.element_id(),
            )
        }
        Value::Path(p) => {
            let mut nodes: Vec<&Arc<Node>> = Vec::new();
            let mut rels: Vec<&Arc<sprig_graph::value::Rel>> = Vec::new();
            let mut idx = Vec::new();
            for n in &p.nodes {
                if !nodes.iter().any(|x| x.id == n.id) {
                    nodes.push(n);
                }
            }
            for (i, r) in p.rels.iter().enumerate() {
                let ri = if let Some(k) = rels.iter().position(|x| x.id == r.id) {
                    k
                } else {
                    rels.push(r);
                    rels.len() - 1
                };
                let prev = &p.nodes[i];
                let next = &p.nodes[i + 1];
                let forward = r.src == prev.id && r.dst == next.id;
                let ri = i64::try_from(ri + 1).unwrap_or(i64::MAX);
                idx.push(if forward { ri } else { -ri });
                idx.push(
                    i64::try_from(nodes.iter().position(|x| x.id == next.id).unwrap_or(0))
                        .unwrap_or(0),
                );
            }
            structs::path(
                nodes.iter().map(|n| node_pack(n)).collect(),
                rels.iter()
                    .map(|r| {
                        structs::unbound(
                            id_i64(r.id),
                            &r.rtype,
                            props_pack(&r.props),
                            &r.element_id(),
                        )
                    })
                    .collect(),
                idx,
            )
        }
        Value::Date(d) => st(0x44, vec![PackValue::Int(temporal::epoch_day(*d))]),
        Value::LocalTime(t) => st(0x74, vec![PackValue::Int(temporal::nanos_of_day(*t))]),
        Value::Time(t, o) => st(
            0x54,
            vec![
                PackValue::Int(temporal::nanos_of_day(*t)),
                PackValue::Int(i64::from(*o)),
            ],
        ),
        Value::LocalDateTime(dt) => {
            let (s, n) = temporal::local_epoch(*dt);
            st(0x64, vec![PackValue::Int(s), PackValue::Int(i64::from(n))])
        }
        Value::DateTime(z) => {
            let (s, n) = z.epoch();
            match &z.zone {
                Some(name) => st(
                    0x69,
                    vec![
                        PackValue::Int(s),
                        PackValue::Int(i64::from(n)),
                        PackValue::String(name.clone()),
                    ],
                ),
                None => st(
                    0x49,
                    vec![
                        PackValue::Int(s),
                        PackValue::Int(i64::from(n)),
                        PackValue::Int(i64::from(z.offset)),
                    ],
                ),
            }
        }
        Value::Duration(d) => st(
            0x45,
            vec![
                PackValue::Int(d.months),
                PackValue::Int(d.days),
                PackValue::Int(d.secs),
                PackValue::Int(d.nanos),
            ],
        ),
        Value::Point(p) => {
            let mut f = vec![
                PackValue::Int(i64::from(p.srid)),
                PackValue::Float(p.x),
                PackValue::Float(p.y),
            ];
            match p.z {
                Some(z) => {
                    f.push(PackValue::Float(z));
                    st(0x59, f)
                }
                None => st(0x58, f),
            }
        }
    }
}

fn st(tag: u8, fields: Vec<PackValue>) -> PackValue {
    PackValue::Struct { tag, fields }
}

fn int_field(f: &[PackValue], i: usize) -> Result<i64> {
    f.get(i).and_then(PackValue::as_int).ok_or_else(|| {
        SprigError::new(
            SprigCode::GraphPropertyType,
            "a temporal or spatial parameter is malformed",
        )
    })
}

fn float_field(f: &[PackValue], i: usize) -> Result<f64> {
    match f.get(i) {
        Some(PackValue::Float(x)) => Ok(*x),
        #[allow(clippy::cast_precision_loss)]
        Some(PackValue::Int(x)) => Ok(*x as f64),
        _ => Err(SprigError::new(
            SprigCode::GraphPropertyType,
            "a point parameter is malformed",
        )),
    }
}

/// A temporal or spatial parameter from its Bolt structure.
fn struct_value(tag: u8, f: &[PackValue]) -> Result<Value> {
    let bad = || {
        SprigError::new(
            SprigCode::GraphPropertyType,
            "a temporal parameter is out of range",
        )
    };
    let u32_of = |x: i64| u32::try_from(x).map_err(|_| bad());
    let i32_of = |x: i64| i32::try_from(x).map_err(|_| bad());
    Ok(match tag {
        0x44 => Value::Date(temporal::from_epoch_day(int_field(f, 0)?).ok_or_else(bad)?),
        0x74 => Value::LocalTime(temporal::time_from_nanos(int_field(f, 0)?).ok_or_else(bad)?),
        0x54 => Value::Time(
            temporal::time_from_nanos(int_field(f, 0)?).ok_or_else(bad)?,
            i32_of(int_field(f, 1)?)?,
        ),
        0x64 => Value::LocalDateTime(
            temporal::local_from_epoch(int_field(f, 0)?, u32_of(int_field(f, 1)?)?)
                .ok_or_else(bad)?,
        ),
        0x49 => Value::DateTime(Box::new(
            temporal::datetime_from_epoch(
                int_field(f, 0)?,
                u32_of(int_field(f, 1)?)?,
                i32_of(int_field(f, 2)?)?,
                None,
            )
            .ok_or_else(bad)?,
        )),
        0x69 => {
            let secs = int_field(f, 0)?;
            let zone = f
                .get(2)
                .and_then(PackValue::as_str)
                .ok_or_else(bad)?
                .to_owned();
            let off = temporal::zone_offset_at(&zone, secs)?;
            Value::DateTime(Box::new(
                temporal::datetime_from_epoch(secs, u32_of(int_field(f, 1)?)?, off, Some(zone))
                    .ok_or_else(bad)?,
            ))
        }
        0x45 => Value::Duration(
            temporal::Dur::from_fields(
                int_field(f, 0)?,
                int_field(f, 1)?,
                int_field(f, 2)?,
                int_field(f, 3)?,
            )
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::GraphPropertyType,
                    "duration seconds out of range",
                )
            })?,
        ),
        0x58 | 0x59 => Value::Point(temporal::Point {
            srid: u32_of(int_field(f, 0)?)?,
            x: float_field(f, 1)?,
            y: float_field(f, 2)?,
            z: if tag == 0x59 {
                Some(float_field(f, 3)?)
            } else {
                None
            },
        }),
        _ => {
            return Err(SprigError::new(
                SprigCode::GraphPropertyType,
                "byte arrays and graph structures are not supported as parameters",
            ));
        }
    })
}

/// A parameter from PackStream.
///
/// # Errors
/// 39909 for bytes and structures, which this build doesn't take as
/// parameters.
pub fn from_pack(v: &PackValue) -> Result<Value> {
    Ok(match v {
        PackValue::Null => Value::Null,
        PackValue::Bool(b) => Value::Bool(*b),
        PackValue::Int(i) => Value::Int(*i),
        PackValue::Float(f) => Value::Float(*f),
        PackValue::String(s) => Value::String(s.clone()),
        PackValue::List(l) => Value::List(l.iter().map(from_pack).collect::<Result<_>>()?),
        PackValue::Map(m) => Value::Map(
            m.iter()
                .map(|(k, x)| Ok((k.clone(), from_pack(x)?)))
                .collect::<Result<BTreeMap<_, _>>>()?,
        ),
        PackValue::Struct { tag, fields } => struct_value(*tag, fields)?,
        // R8 round 8: byte arrays (Neo4j's `byte[]`).
        PackValue::Bytes(b) => Value::Bytes(b.clone()),
    })
}

// ----------------------------------------------------------------------
// The front end.
// ----------------------------------------------------------------------

/// Logins and sessions for Bolt.
pub struct BoltGraphService {
    login: Arc<LoginService>,
    graphs: Option<Arc<GatewayGraphs>>,
    live: LiveSettings,
    dc: DcId,
}

impl std::fmt::Debug for BoltGraphService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoltGraphService").finish_non_exhaustive()
    }
}

impl BoltGraphService {
    /// Logins through `login`, graphs through `graphs` (`None` without
    /// catalog logins: every login is refused).
    #[must_use]
    pub fn new(
        login: Arc<LoginService>,
        graphs: Option<Arc<GatewayGraphs>>,
        live: LiveSettings,
        dc: DcId,
    ) -> Self {
        Self {
            login,
            graphs,
            live,
            dc,
        }
    }

    fn sctx(&self) -> SettingCtx<'_> {
        SettingCtx::process(&self.dc)
    }

    /// A session for `principal` outside Bolt: `Sql.Query` with `language:
    /// cypher` or `gql` over REST and gRPC (chapter 37; tui decision T2).
    /// The same statements, graphs, privileges and budgets as a Bolt
    /// login's; `None` without tenant catalogs.
    #[must_use]
    pub(crate) fn api_session(
        &self,
        principal: sprig_auth::mysql::Principal,
    ) -> Option<session::GwSession> {
        let graphs = self.graphs.clone()?;
        let ctx = self.sctx();
        Some(session::GwSession::new(
            principal,
            graphs,
            self.dc.clone(),
            self.live.get(&ADVERTISE_ADDRESS, &ctx).into_owned(),
            self.live.get(&ROUTING_TTL_SECONDS, &ctx),
        ))
    }
}

#[async_trait]
impl BoltService for BoltGraphService {
    async fn logon(
        &self,
        info: &ConnInfo,
        auth: &PackValue,
    ) -> std::result::Result<Box<dyn BoltSession>, BoltFailure> {
        Ok(Box::new(self.logon_session(info, auth).await?))
    }
}

/// One connection's service: [`BoltGraphService`]'s logins, and the
/// connection's entry in the gateway's session table (round 3), so Bolt
/// sessions show in `SHOW PROCESSLIST` and `KILL` stops them.
struct ConnService {
    inner: Arc<BoltGraphService>,
    session: std::sync::Mutex<Option<crate::session::Session>>,
}

#[async_trait]
impl BoltService for ConnService {
    async fn logon(
        &self,
        info: &ConnInfo,
        auth: &PackValue,
    ) -> std::result::Result<Box<dyn BoltSession>, BoltFailure> {
        let mut gw = self.inner.logon_session(info, auth).await?;
        let reg = self
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(mut s) = reg {
            s.set_principal(gw.principal().clone());
            s.set_state(crate::session::SessionState::Ready);
            gw.attach(s);
        }
        Ok(Box::new(gw))
    }
}

impl BoltGraphService {
    async fn logon_session(
        &self,
        info: &ConnInfo,
        auth: &PackValue,
    ) -> std::result::Result<session::GwSession, BoltFailure> {
        let scheme = auth
            .get("scheme")
            .and_then(PackValue::as_str)
            .unwrap_or("none");
        if scheme != "basic" {
            return Err(refuse(
                SprigCode::BoltAuthScheme,
                format!("authentication scheme '{scheme}' is not supported: use basic"),
            ));
        }
        let Some(graphs) = self.graphs.clone() else {
            return Err(refuse(
                SprigCode::BoltAuthScheme,
                "Bolt needs tenant accounts (catalog login mode)",
            ));
        };
        if let Err(e) = self.login.admit(info.peer.ip()) {
            return Err(failure(&e));
        }
        let user = auth
            .get("principal")
            .and_then(PackValue::as_str)
            .unwrap_or_default();
        let password = auth
            .get("credentials")
            .and_then(PackValue::as_str)
            .unwrap_or_default();
        // The tenant: TLS server name, else `tenant.user`.
        let named_by_sni = match (self.login.source(), info.sni.as_deref()) {
            (LoginSource::Catalog(cl), Some(sni)) => {
                parse_login(user, Some(sni), &cl.sni_suffixes(), 255)
                    .is_some_and(|p| matches!(p.form, LoginForm::Plain))
            }
            _ => false,
        };
        if !named_by_sni && !user.contains('.') {
            return Err(refuse(
                SprigCode::BoltAuthScheme,
                "name the tenant: log in as <tenant>.<user>, or connect with the tenant's TLS server name",
            ));
        }
        let mut login = self
            .login
            .start_http(LoginRequest {
                persona: Persona::MySql84,
                login_name: user.as_bytes().to_vec(),
                sni: info.sni.clone(),
                client_ip: info.peer.ip(),
                tls: info.tls,
                client_cert: false,
                can_handle_expired_password: false,
            })
            .await;
        let (outcome, path) = if login.check_clear_fast(password.as_bytes()) {
            (Outcome::Verified, AuthPath::Fast)
        } else if login
            .check_password(Zeroizing::new(password.as_bytes().to_vec()))
            .await
        {
            (Outcome::Verified, AuthPath::Full)
        } else {
            (Outcome::WrongCredentials, AuthPath::Full)
        };
        let verdict = login.decide(outcome, path).await;
        let principal = match (&verdict, login.principal()) {
            (AuthVerdict::Accept, Some(p)) if p.tenant.is_some() => p.clone(),
            (AuthVerdict::Accept, _) => {
                return Err(refuse(
                    SprigCode::BoltAuthScheme,
                    "Bolt needs a tenant account",
                ));
            }
            (AuthVerdict::Refuse(p), _) => {
                return Err(BoltFailure::new(
                    "Neo.ClientError.Security.Unauthorized",
                    format!(
                        "The client is unauthorized due to authentication failure ({}: {})",
                        login.refusal_reason().unwrap_or("bad_credentials"),
                        p.message
                    ),
                ));
            }
        };
        let ctx = self.sctx();
        Ok(session::GwSession::new(
            principal,
            graphs,
            self.dc.clone(),
            self.live.get(&ADVERTISE_ADDRESS, &ctx).into_owned(),
            self.live.get(&ROUTING_TTL_SECONDS, &ctx),
        ))
    }
}

/// The Bolt front end.
pub struct BoltFrontend {
    service: Arc<BoltGraphService>,
}

impl std::fmt::Debug for BoltFrontend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoltFrontend").finish_non_exhaustive()
    }
}

impl BoltFrontend {
    /// The front end over `service`.
    #[must_use]
    pub fn new(service: Arc<BoltGraphService>) -> Self {
        Self { service }
    }

    fn config(&self, handshake: Duration) -> Config {
        let s = &self.service;
        let ctx = s.sctx();
        let max =
            BoltVersion::parse(&s.live.get(&MAX_VERSION, &ctx)).unwrap_or(BoltVersion::new(5, 8));
        Config {
            server_agent: s.live.get(&SERVER_AGENT, &ctx).into_owned(),
            versions: sprig_proto_bolt::message::SUPPORTED
                .iter()
                .copied()
                .filter(|v| *v <= max)
                .collect(),
            recv_timeout: Duration::from_secs(s.live.get(&RECV_TIMEOUT_SECONDS, &ctx)),
            handshake_timeout: handshake,
            max_message: 16 << 20,
            id_prefix: "bolt-".into(),
            max_buffered: usize::try_from(s.live.get(&MAX_BUFFERED_RESULT_MB, &ctx))
                .unwrap_or(64)
                .saturating_mul(1 << 20),
        }
    }
}

#[async_trait]
impl Frontend for BoltFrontend {
    fn protocol(&self) -> Protocol {
        Protocol::Bolt
    }

    async fn serve(&self, conn: ClientConn, ctx: Arc<GatewayContext>) -> Result<()> {
        // The session table's entry: `SHOW PROCESSLIST` lists it, `KILL`
        // stops its statement (`KILL QUERY`) or closes it.
        let reg = ctx.sessions.open(&conn, Protocol::Bolt)?;
        let close = conn.close_soon.child_token();
        if let Some(h) = ctx.sessions.get(reg.id) {
            let close = close.clone();
            tokio::spawn(async move {
                tokio::select! {
                    () = h.flags().killed() => close.cancel(),
                    () = close.cancelled() => {}
                }
            });
        }
        let svc = ConnService {
            inner: Arc::clone(&self.service),
            session: std::sync::Mutex::new(Some(reg)),
        };
        let tls = conn.stream.tls_info();
        let cfg = self.config(conn.listener.handshake_timeout);
        let peer = conn.peer;
        let r = sprig_proto_bolt::server::serve(
            conn.stream,
            peer,
            tls.is_some(),
            tls.and_then(|t| t.sni),
            &svc,
            &cfg,
            close.clone(),
        )
        .await;
        close.cancel();
        match r {
            Ok(end) => tracing::debug!(%peer, ?end, "Bolt connection ended"),
            Err(e) => tracing::debug!(%peer, error = %e, "Bolt connection ended"),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_graph::value::{Path, Rel};

    #[test]
    #[allow(clippy::many_single_char_names)] // nodes a, b; relationship r; path p; value v
    fn values_become_bolt_structures() {
        let a = Arc::new(Node {
            graph: 9,
            id: 1,
            rk: 0xa,
            labels: vec!["P".into()],
            props: BTreeMap::from([("n".into(), Value::Int(1))]),
        });
        let b = Arc::new(Node {
            graph: 9,
            id: 2,
            rk: 0xb,
            labels: vec![],
            props: BTreeMap::new(),
        });
        // b -> a, walked from a: index negative.
        let r = Arc::new(Rel {
            graph: 9,
            id: 7,
            src: 2,
            dst: 1,
            src_rk: 0xb,
            dst_rk: 0xa,
            rtype: "R".into(),
            props: BTreeMap::new(),
        });
        let PackValue::Struct { tag, fields } = to_pack(&Value::Node(a.clone())) else {
            panic!("node")
        };
        assert_eq!(tag, 0x4E);
        assert_eq!(fields[3], PackValue::str("n:9:a:1"));
        let PackValue::Struct { tag, fields } = to_pack(&Value::Rel(r.clone())) else {
            panic!("rel")
        };
        assert_eq!(tag, 0x52);
        assert_eq!(fields[6], PackValue::str("n:9:b:2"));
        let p = Value::Path(Arc::new(Path {
            nodes: vec![a.clone(), b, a],
            rels: vec![r.clone(), r],
        }));
        let PackValue::Struct { tag, fields } = to_pack(&p) else {
            panic!("path")
        };
        assert_eq!(tag, 0x50);
        assert_eq!(fields[0].as_list().map(<[PackValue]>::len), Some(2));
        assert_eq!(fields[1].as_list().map(<[PackValue]>::len), Some(1));
        assert_eq!(
            fields[2],
            PackValue::List(vec![
                PackValue::Int(-1),
                PackValue::Int(1),
                PackValue::Int(1),
                PackValue::Int(0)
            ])
        );
        assert_eq!(
            from_pack(&PackValue::Bytes(vec![1, 255])).expect("bytes"),
            Value::Bytes(vec![1, 255])
        );
        assert_eq!(to_pack(&Value::Bytes(vec![7])), PackValue::Bytes(vec![7]));
        // Temporal and spatial values round-trip through Bolt structures.
        for (kind, text) in [
            ("date", "2020-01-05"),
            ("localtime", "12:30:15.5"),
            ("time", "12:30:15.5+02:00"),
            ("localdatetime", "2020-01-05T10:00:00"),
            ("datetime", "2020-01-05T10:00:00+01:00"),
            ("datetime", "2020-07-05T10:00:00+02:00[Europe/Berlin]"),
            ("duration", "P1Y2M3DT4H5M6.5S"),
        ] {
            let v = temporal::parse(kind, text).expect("parse");
            let back = from_pack(&to_pack(&v)).expect("unpack");
            assert_eq!(temporal::text(&back).as_deref(), Some(text), "{kind}");
        }
        let p = Value::Point(temporal::Point {
            srid: 9157,
            x: 1.0,
            y: 2.0,
            z: Some(3.0),
        });
        assert_eq!(from_pack(&to_pack(&p)).expect("point"), p);
    }
}
