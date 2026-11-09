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
//! - **Transactions.** Each auto-commit `RUN` is one MariaDB transaction;
//!   `BEGIN` holds one `sprig_ctl` connection of the tenant's cluster until
//!   `COMMIT`/`ROLLBACK`, at `READ COMMITTED` (`bolt.isolation`). The
//!   commit check refuses nodes deleted with their relationships left
//!   (39906).
//! - **Routing.** `ROUTE` answers one address for `ROUTE`, `READ` and
//!   `WRITE`: `bolt.advertise_address`, else the address the driver used.
//! - **Bookmarks** are `SB1:` and base64url of the tenant and the commit
//!   time. As built they are not signed or waited on: every read goes to
//!   the writer, which has every commit (decision G9).

use std::borrow::Cow;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use mysql_async::prelude::Queryable;
use sprig_auth::LoginSource;
use sprig_auth::account::parse_login;
use sprig_auth::authz::Action;
use sprig_auth::mysql::{LoginRequest, LoginService, Principal};
use sprig_auth::store::LoginForm;
use sprig_catalog::Freshness;
use sprig_common::ids::DcId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_graph::ast::{Clause, Query};
use sprig_graph::catalog::backend_error;
use sprig_graph::eval::Params;
use sprig_graph::maria::{MariaStore, check_deleted};
use sprig_graph::value::{Node, Value, node_element_id};
use sprig_proto_bolt::server::{
    BoltFailure, BoltService, BoltSession, Config, ConnInfo, QueryResult,
};
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
    /// speaks 5.0 to 5.4.
    MAX_VERSION: Text = "bolt.max_version", Cow::Borrowed("5.4"), Scopes::PROCESS,
    HardBounds::one_of(&["5.0", "5.1", "5.2", "5.3", "5.4"]),
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
pub fn to_pack(v: &Value) -> PackValue {
    match v {
        Value::Null => PackValue::Null,
        Value::Bool(b) => PackValue::Bool(*b),
        Value::Int(i) => PackValue::Int(*i),
        Value::Float(f) => PackValue::Float(*f),
        Value::String(s) => PackValue::String(s.clone()),
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
    }
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
        PackValue::Bytes(_) | PackValue::Struct { .. } => {
            return Err(SprigError::new(
                SprigCode::GraphPropertyType,
                "byte arrays and structures (temporal, spatial) are not supported as parameters yet",
            ));
        }
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
}

#[async_trait]
impl BoltService for BoltGraphService {
    async fn logon(
        &self,
        info: &ConnInfo,
        auth: &PackValue,
    ) -> std::result::Result<Box<dyn BoltSession>, BoltFailure> {
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
        Ok(Box::new(GwSession {
            principal,
            graphs,
            live: self.live.clone(),
            dc: self.dc.clone(),
            tx: None,
            last_bookmark: None,
            advertise: self.live.get(&ADVERTISE_ADDRESS, &ctx).into_owned(),
            ttl: self.live.get(&ROUTING_TTL_SECONDS, &ctx),
        }))
    }
}

/// An open transaction: one connection, the graphs it touched and the
/// nodes it deleted.
struct Tx {
    conn: mysql_async::Conn,
    db: Option<String>,
    timeout: Option<Duration>,
    deleted: Vec<(sprig_graph::catalog::Tables, Vec<u64>)>,
}

struct GwSession {
    principal: Principal,
    graphs: Arc<GatewayGraphs>,
    live: LiveSettings,
    dc: DcId,
    tx: Option<Tx>,
    last_bookmark: Option<String>,
    advertise: String,
    ttl: u64,
}

/// The privileges a statement needs.
fn actions(q: &Query) -> Vec<Action> {
    let mut a = vec![];
    for part in &q.parts {
        for c in &part.clauses {
            match c {
                Clause::Match { .. }
                | Clause::Return(_)
                | Clause::With(_)
                | Clause::Unwind { .. }
                | Clause::Call { .. } => {
                    if !a.contains(&Action::Select) {
                        a.push(Action::Select);
                    }
                }
                Clause::Create(_) | Clause::Merge { .. } => {
                    for x in [Action::Insert, Action::Select] {
                        if !a.contains(&x) {
                            a.push(x);
                        }
                    }
                }
                Clause::Set(_) | Clause::Remove(_) => {
                    if !a.contains(&Action::Update) {
                        a.push(Action::Update);
                    }
                }
                Clause::Delete { .. } => {
                    if !a.contains(&Action::Delete) {
                        a.push(Action::Delete);
                    }
                }
            }
        }
    }
    a
}

fn is_default(db: Option<&str>) -> bool {
    db.is_none_or(str::is_empty)
}

fn has_writes(q: &Query) -> bool {
    actions(q).iter().any(|a| *a != Action::Select)
}

fn bookmark(tenant: u64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros());
    let body = serde_json::json!({ "t": tenant, "hlc": u64::try_from(now).unwrap_or(0) });
    format!(
        "SB1:{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(body.to_string())
    )
}

impl GwSession {
    fn tenant(&self) -> sprig_common::ids::TenantId {
        self.principal
            .tenant
            .unwrap_or(sprig_common::ids::TenantId(0))
    }

    /// `(logical db, graph)` of a Bolt database name.
    fn split_db(&self, db: Option<&str>, cat: &sprig_catalog::TenantCatalog) -> (String, String) {
        let ctx = cat.setting_ctx(&self.dc);
        let logical = self
            .graphs
            .svc
            .settings
            .get(&LOGICAL_DATABASE, &ctx, Some(&cat.settings))
            .into_owned();
        if let Some(d) = db.filter(|d| !d.is_empty()) {
            match d.split_once('.') {
                Some((a, b)) => (a.to_owned(), b.to_owned()),
                None => (logical, d.to_owned()),
            }
        } else {
            {
                let g = self
                    .graphs
                    .svc
                    .settings
                    .get(&DEFAULT_GRAPH, &ctx, Some(&cat.settings))
                    .into_owned();
                (logical, g)
            }
        }
    }

    async fn open_conn(
        &self,
        cluster: &sprig_common::ids::ClusterId,
        read_only: bool,
        cat: &sprig_catalog::TenantCatalog,
    ) -> Result<mysql_async::Conn> {
        let pool = self.graphs.session_pool(cluster).await?;
        let mut c = pool.get_conn().await.map_err(backend_error)?;
        let iso = self.graphs.svc.settings.get(
            &ISOLATION,
            &cat.setting_ctx(&self.dc),
            Some(&cat.settings),
        );
        let level = if iso == "repeatable_read" {
            "REPEATABLE READ"
        } else {
            "READ COMMITTED"
        };
        c.query_drop(format!("SET TRANSACTION ISOLATION LEVEL {level}"))
            .await
            .map_err(backend_error)?;
        c.query_drop(if read_only {
            "START TRANSACTION READ ONLY"
        } else {
            "START TRANSACTION"
        })
        .await
        .map_err(backend_error)?;
        Ok(c)
    }

    #[allow(clippy::too_many_lines)] // resolve, authorize, run, convert
    #[allow(clippy::many_single_char_names)] // q, p, g: query, parameters, graph
    async fn run_inner(
        &mut self,
        query: &str,
        params: &PackValue,
        extra: &PackValue,
    ) -> Result<QueryResult> {
        if extra
            .get("imp_user")
            .is_some_and(|v| !matches!(v, PackValue::Null))
        {
            return Err(SprigError::new(
                SprigCode::BoltImpersonation,
                "impersonation (imp_user) is not supported",
            ));
        }
        let q = sprig_graph::parser::parse(query)?;
        let mut p = Params::new();
        for (k, v) in params.as_map().unwrap_or_default() {
            p.insert(k.clone(), from_pack(v)?);
        }
        let tenant = self.tenant();
        let cat = self.graphs.catalog(tenant, Freshness::Latest).await?;
        let db_req: Option<String> = match &self.tx {
            Some(t) => t.db.clone(),
            None => extra
                .get("db")
                .and_then(PackValue::as_str)
                .map(str::to_owned),
        };
        let (db, name) = self.split_db(db_req.as_deref(), &cat);
        let grants = self.graphs.grants(&cat, self.principal.user_id);
        let mut graph = self.graphs.resolve(&cat, &db, &name).await?;
        let mut cat = cat;
        if graph.is_none() {
            if !(is_default(db_req.as_deref()) && has_writes(&q)) {
                if is_default(db_req.as_deref()) {
                    // The default graph before its first write: empty.
                    let mut mem = sprig_graph::mem::MemStore::new(1);
                    let out =
                        sprig_graph::exec::run(&mut mem, &q, &p, &sprig_graph::Budgets::default())
                            .await?;
                    return self.result(out, &name, &cat);
                }
                return Err(SprigError::new(
                    SprigCode::GraphNotFound,
                    format!("Graph not found: {db}.{name}"),
                ));
            }
            let lctn = self.graphs.lctn(&cat);
            let db_row = cat
                .database(&sprig_catalog::ddl::database_key(&db, lctn))
                .map(|r| r.db_id)
                .ok_or_else(|| {
                    SprigError::new(
                        SprigCode::GraphNotFound,
                        format!("Database '{db}' does not exist"),
                    )
                })?;
            if !self
                .graphs
                .allows(grants.as_deref(), &cat, &db, db_row, None, Action::Create)
            {
                return Err(SprigError::standard(
                    1142,
                    *b"42000",
                    format!(
                        "CREATE command denied to user '{}' for graph '{db}.{name}'",
                        self.principal.user
                    ),
                ));
            }
            let (g, v) = self.graphs.create(tenant, &db, &name, true).await?;
            cat = self
                .graphs
                .catalog(tenant, Freshness::Cached { min_version: v })
                .await?;
            graph = Some(g);
        }
        let Some(g) = graph else {
            return Err(SprigError::internal("graph missing"));
        };
        for a in actions(&q) {
            if !self
                .graphs
                .allows(grants.as_deref(), &cat, &g.db, g.db_id, Some(g.graph_id), a)
            {
                return Err(SprigError::standard(
                    1142,
                    *b"42000",
                    format!(
                        "{} command denied to user '{}' for graph '{}.{}'",
                        format!("{a:?}").to_uppercase(),
                        self.principal.user,
                        g.db,
                        g.name
                    ),
                ));
            }
        }
        let ctx = cat.setting_ctx(&self.dc).with_db(g.db_id);
        let settings = &self.graphs.svc.settings;
        let mut budgets =
            sprig_graph::settings::budgets(&|s| settings.get(s, &ctx, Some(&cat.settings)));
        let timeout = match &self.tx {
            Some(t) => t.timeout,
            None => extra
                .get("tx_timeout")
                .and_then(PackValue::as_int)
                .filter(|t| *t > 0)
                .map(|t| Duration::from_millis(u64::try_from(t).unwrap_or(0))),
        };
        if let Some(t) = timeout {
            budgets.max_time = budgets.max_time.min(t);
        }
        let dict = self.graphs.dictionary(&g);
        let (labels, types) = self.graphs.names_of(&g);
        let ids = self.graphs.counters(&g).await?;
        let auto = self.tx.is_none();
        let mut own = if auto {
            let ro = extra.get("mode").and_then(PackValue::as_str) == Some("r");
            Some(
                self.open_conn(&g.cluster, ro && !has_writes(&q), &cat)
                    .await?,
            )
        } else {
            None
        };
        let r = {
            let conn: &mut mysql_async::Conn = match (&mut own, &mut self.tx) {
                (Some(c), _) => c,
                (None, Some(t)) => &mut t.conn,
                (None, None) => return Err(SprigError::internal("no connection")),
            };
            let mut store = MariaStore::new(conn, g.graph_id, g.tables(), labels, types, dict, ids);
            sprig_graph::exec::run(&mut store, &q, &p, &budgets).await
        };
        if let Some(mut c) = own {
            let r = match r {
                Ok(o) => match check_deleted(&mut c, &g.tables(), &o.deleted_nodes).await {
                    Ok(()) => Ok(o),
                    Err(e) => Err(e),
                },
                Err(e) => Err(e),
            };
            let end = if r.is_ok() { "COMMIT" } else { "ROLLBACK" };
            c.query_drop(end).await.map_err(backend_error)?;
            let out = r?;
            self.last_bookmark = Some(bookmark(tenant.0));
            return self.result(out, &g.name, &cat);
        }
        let out = r?;
        if let Some(t) = &mut self.tx
            && !out.deleted_nodes.is_empty()
        {
            t.deleted.push((g.tables(), out.deleted_nodes.clone()));
        }
        self.result(out, &g.name, &cat)
    }

    fn result(
        &self,
        out: sprig_graph::Output,
        graph: &str,
        cat: &sprig_catalog::TenantCatalog,
    ) -> Result<QueryResult> {
        let limit = usize::try_from(self.graphs.svc.settings.get(
            &MAX_BUFFERED_RESULT_MB,
            &cat.setting_ctx(&self.dc),
            Some(&cat.settings),
        ))
        .unwrap_or(64)
        .saturating_mul(1 << 20);
        let mut bytes = 0usize;
        let mut records = VecDeque::with_capacity(out.rows.len());
        for r in out.rows {
            let rec: Vec<PackValue> = r.iter().map(to_pack).collect();
            bytes += rec.iter().map(PackValue::encoded_len).sum::<usize>();
            if bytes > limit {
                return Err(SprigError::new(
                    SprigCode::BoltResultBufferLimit,
                    format!(
                        "the result is larger than bolt.max_buffered_result_mb = {}",
                        limit >> 20
                    ),
                ));
            }
            records.push_back(rec);
        }
        Ok(QueryResult {
            fields: out.columns,
            records,
            qtype: out.qtype,
            stats: out.stats.into_iter().collect(),
            db: Some(graph.to_owned()),
            t_first: 0,
        })
    }
}

#[async_trait]
impl BoltSession for GwSession {
    async fn run(
        &mut self,
        query: &str,
        params: &PackValue,
        extra: &PackValue,
    ) -> std::result::Result<QueryResult, BoltFailure> {
        self.run_inner(query, params, extra)
            .await
            .map_err(|e| failure(&e))
    }

    async fn begin(&mut self, extra: &PackValue) -> std::result::Result<(), BoltFailure> {
        if extra
            .get("imp_user")
            .is_some_and(|v| !matches!(v, PackValue::Null))
        {
            return Err(refuse(
                SprigCode::BoltImpersonation,
                "impersonation (imp_user) is not supported",
            ));
        }
        let tenant = self.tenant();
        let r: Result<Tx> = async {
            let cat = self.graphs.catalog(tenant, Freshness::Latest).await?;
            let cluster = self.graphs.svc.locator.cluster(tenant).await?;
            let ro = extra.get("mode").and_then(PackValue::as_str) == Some("r");
            let conn = self.open_conn(&cluster, ro, &cat).await?;
            Ok(Tx {
                conn,
                db: extra
                    .get("db")
                    .and_then(PackValue::as_str)
                    .map(str::to_owned),
                timeout: extra
                    .get("tx_timeout")
                    .and_then(PackValue::as_int)
                    .filter(|t| *t > 0)
                    .map(|t| Duration::from_millis(u64::try_from(t).unwrap_or(0))),
                deleted: Vec::new(),
            })
        }
        .await;
        self.tx = Some(r.map_err(|e| failure(&e))?);
        Ok(())
    }

    async fn commit(&mut self) -> std::result::Result<Option<String>, BoltFailure> {
        let Some(mut t) = self.tx.take() else {
            return Err(BoltFailure::invalid("no open transaction"));
        };
        for (tables, ids) in &t.deleted {
            if let Err(e) = check_deleted(&mut t.conn, tables, ids).await {
                let _ = t.conn.query_drop("ROLLBACK").await;
                return Err(failure(&e));
            }
        }
        t.conn
            .query_drop("COMMIT")
            .await
            .map_err(|e| failure(&backend_error(e)))?;
        let b = bookmark(self.tenant().0);
        self.last_bookmark = Some(b.clone());
        Ok(Some(b))
    }

    async fn rollback(&mut self) -> std::result::Result<(), BoltFailure> {
        if let Some(mut t) = self.tx.take() {
            let _ = t.conn.query_drop("ROLLBACK").await;
        }
        Ok(())
    }

    fn last_bookmark(&self) -> Option<String> {
        self.last_bookmark.clone()
    }

    async fn reset(&mut self) {
        if let Some(mut t) = self.tx.take() {
            let _ = t.conn.query_drop("ROLLBACK").await;
        }
    }

    async fn route(
        &mut self,
        routing: &PackValue,
        extra: &PackValue,
    ) -> std::result::Result<PackValue, BoltFailure> {
        let addr = if self.advertise.is_empty() {
            routing
                .get("address")
                .and_then(PackValue::as_str)
                .unwrap_or("localhost:7687")
                .to_owned()
        } else {
            self.advertise.clone()
        };
        let db = extra
            .get("db")
            .and_then(PackValue::as_str)
            .unwrap_or("neo4j")
            .to_owned();
        let server = |role: &str| {
            PackValue::map([
                (
                    "addresses",
                    PackValue::List(vec![PackValue::str(addr.clone())]),
                ),
                ("role", PackValue::str(role)),
            ])
        };
        let _ = &self.live;
        Ok(PackValue::map([
            (
                "ttl",
                PackValue::Int(i64::try_from(self.ttl).unwrap_or(300)),
            ),
            ("db", PackValue::String(db)),
            (
                "servers",
                PackValue::List(vec![server("ROUTE"), server("READ"), server("WRITE")]),
            ),
        ]))
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
            BoltVersion::parse(&s.live.get(&MAX_VERSION, &ctx)).unwrap_or(BoltVersion::new(5, 4));
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
        }
    }
}

#[async_trait]
impl Frontend for BoltFrontend {
    fn protocol(&self) -> Protocol {
        Protocol::Bolt
    }

    async fn serve(&self, conn: ClientConn, _ctx: Arc<GatewayContext>) -> Result<()> {
        let tls = conn.stream.tls_info();
        let cfg = self.config(conn.listener.handshake_timeout);
        let peer = conn.peer;
        match sprig_proto_bolt::server::serve(
            conn.stream,
            peer,
            tls.is_some(),
            tls.and_then(|t| t.sni),
            self.service.as_ref(),
            &cfg,
            conn.close_soon.clone(),
        )
        .await
        {
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
        assert!(from_pack(&PackValue::Bytes(vec![1])).is_err());
    }
}
