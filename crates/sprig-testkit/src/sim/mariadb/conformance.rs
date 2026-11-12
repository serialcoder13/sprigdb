//! Model conformance (chapter 46, "Model conformance tests"): the same
//! scripted scenarios run against the [`Model`] and against real MariaDB,
//! and every observed outcome must match. A divergence is a bug in the
//! model (or a MariaDB behaviour SprigDB must learn about).
//!
//! A [`Scenario`] names its target topology, tables, users and sessions,
//! then lists [`Action`]s. `Exec` sends a statement and records either its
//! outcome or that it blocked (on the real server: no reply within
//! [`SETTLE`]); `Await` records a blocked statement's outcome. Results are
//! normalised so that both sides compare: GTIDs (`@@last_gtid` and each
//! GTID of `@@gtid_binlog_pos`) keep only `domain-server`, XIDs and user names lose the run's unique suffix, and
//! every client-side connection failure is `2013`.
//!
//! Targets on the shared Compose servers ([`Target::Single`],
//! [`Target::Replication`], [`Target::Galera`]) only touch the run's own
//! schema, users and XIDs. Scenarios that crash a server or set
//! `read_only` use [`Target::Throwaway`]: a `sprig-sim-*` container of
//! their own.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::Duration;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Value};
use sprig_common::{Result, SprigError};

use super::model::{ConnId, Model, ServerConfig, Step};
use super::{Gtid, Isolation, Outcome, Row, Stmt, errno};
use crate::util::{Retry, unique_ident};

/// How long a real statement may take before it counts as blocked.
pub const SETTLE: Duration = Duration::from_millis(400);

/// How long `Await` waits on a real server.
pub const AWAIT_LIMIT: Duration = Duration::from_secs(30);

/// Password of the users scenarios create.
const USER_PASSWORD: &str = "sprig-sim-pw";

/// Where a scenario runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    /// One node (`SPRIG_TEST_MARIADB_SINGLE`).
    Single,
    /// A semi-sync primary and two replicas
    /// (`SPRIG_TEST_MARIADB_REPLICATION`): nodes 0, 1, 2.
    Replication,
    /// Three Galera nodes (`SPRIG_TEST_MARIADB_GALERA`).
    Galera,
    /// A throwaway single-node container the scenario may crash or fence.
    Throwaway,
}

/// One client session of a scenario.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    /// The node it connects to (index into the target's nodes).
    pub node: usize,
    /// The scenario user it logs in as; `None` for root.
    pub user: Option<&'static str>,
}

impl Session {
    /// Root on `node`.
    #[must_use]
    pub fn root(node: usize) -> Self {
        Self { node, user: None }
    }

    /// `user` on `node`.
    #[must_use]
    pub fn user(node: usize, user: &'static str) -> Self {
        Self {
            node,
            user: Some(user),
        }
    }
}

/// One step of a scenario.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Run a statement on a session.
    Exec(usize, Stmt),
    /// `KILL QUERY` (or `KILL CONNECTION` when the flag is set) of the
    /// second session's connection, from the first.
    Kill(usize, usize, bool),
    /// `MASTER_GTID_WAIT` on the first session for the GTID the second
    /// session last read with `Stmt::LastGtid`.
    WaitGtidOf(usize, usize),
    /// Wait for the session's blocked statement and record its outcome.
    Await(usize),
    /// Let time pass.
    Sleep(Duration),
    /// Close the session (a prepared XA branch detaches).
    Disconnect(usize),
    /// Open the session again.
    Connect(usize),
    /// `SIGKILL` the node's server (throwaway targets only).
    Crash(usize),
    /// Start it again and wait until it accepts connections.
    Restart(usize),
}

/// A scripted scenario.
#[derive(Debug, Clone)]
pub struct Scenario {
    /// Its name.
    pub name: &'static str,
    /// Where it runs.
    pub target: Target,
    /// Tables created before it starts.
    pub tables: Vec<&'static str>,
    /// Users created before it starts (no grants).
    pub users: Vec<&'static str>,
    /// Sessions, connected before it starts.
    pub sessions: Vec<Session>,
    /// The script.
    pub actions: Vec<Action>,
}

/// What one action observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observed {
    /// The statement finished.
    Done(Outcome),
    /// It blocked.
    Blocked,
    /// An `Await` that never finished, or an action that couldn't run.
    Stuck(String),
    /// The action has no outcome (sleep, connect, crash...).
    Nothing,
}

impl std::fmt::Display for Observed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Observed::Done(o) => write!(f, "{o}"),
            Observed::Blocked => f.write_str("blocked"),
            Observed::Stuck(s) => write!(f, "stuck: {s}"),
            Observed::Nothing => f.write_str("-"),
        }
    }
}

/// Identities of the target's nodes: `(server_id, gtid domain)`.
pub type NodeIds = Vec<(u32, u32)>;

/// The Compose defaults (chapter 27): single 101/1; primary and replicas
/// 201-203/2; Galera 301-303 in domain 3; throwaway 901/9.
#[must_use]
pub fn default_ids(target: Target) -> NodeIds {
    match target {
        Target::Single => vec![(101, 1)],
        Target::Replication => vec![(201, 2), (202, 2), (203, 2)],
        Target::Galera => vec![(301, 3), (302, 3), (303, 3)],
        Target::Throwaway => vec![(901, 9)],
    }
}

/// The node names the model uses.
fn node_name(i: usize) -> String {
    format!("n{i}")
}

// ----------------------------------------------------------------------
// The model side.

/// Runs `sc` on the model, with nodes identified by `ids`.
///
/// # Panics
/// If `ids` has fewer nodes than the scenario uses.
#[must_use]
#[allow(clippy::too_many_lines, clippy::many_single_char_names)]
pub fn run_model(sc: &Scenario, ids: &NodeIds) -> Vec<Observed> {
    let schema = "s";
    let mut m = Model::new(0);
    let ms = Duration::from_millis;
    m.set_delay(Box::new(move |_, _| ms(1)));
    match sc.target {
        Target::Single | Target::Throwaway => {
            m.add_server(ServerConfig::new(&node_name(0), ids[0].0, ids[0].1));
        }
        Target::Replication => {
            let mut p = ServerConfig::new(&node_name(0), ids[0].0, ids[0].1);
            p.semisync_master = true;
            m.add_server(p);
            for (i, id) in ids.iter().enumerate().skip(1) {
                m.add_replica(
                    ServerConfig::new(&node_name(i), id.0, id.1),
                    &node_name(0),
                    ms(5),
                );
            }
        }
        Target::Galera => {
            let nodes = ids
                .iter()
                .enumerate()
                .map(|(i, id)| ServerConfig::new(&node_name(i), id.0, id.1))
                .collect();
            m.add_galera("g", nodes, ids[0].1);
        }
    }
    let mut now = Duration::ZERO;
    let root = m.connect(&node_name(0), None, schema, now).expect("root");
    let wait = |m: &mut Model, c: ConnId, s: Step, now: &mut Duration| -> Outcome {
        match s {
            Step::Done(o) => o,
            Step::Blocked => loop {
                if let Some(o) = m.take_done(c) {
                    break o;
                }
                let Some(t) = m.next_event_time() else {
                    break Outcome::Err(0);
                };
                *now = t;
                m.advance(t);
            },
        }
    };
    for t in &sc.tables {
        let s = m.exec(root, &Stmt::CreateTable((*t).to_owned()), now);
        wait(&mut m, root, s, &mut now);
    }
    // Users exist on every node, as replication or Galera would make them.
    for server in m.servers() {
        let c = m.connect(&server, None, schema, now).expect("root");
        for u in &sc.users {
            let _ = m.exec(c, &Stmt::CreateUser((*u).to_owned()), now);
        }
        m.disconnect(c, now);
    }
    now += Duration::from_secs(1);
    m.advance(now);
    m.disconnect(root, now);

    let connect = |m: &mut Model, s: &Session, now: Duration| {
        m.connect(&node_name(s.node), s.user, schema, now).ok()
    };
    let mut conns: Vec<Option<ConnId>> = sc
        .sessions
        .iter()
        .map(|s| connect(&mut m, s, now))
        .collect();
    let mut last_gtid: BTreeMap<usize, Gtid> = BTreeMap::new();
    let mut out = Vec::with_capacity(sc.actions.len());
    for a in &sc.actions {
        let dead = match a {
            Action::Exec(s, _) | Action::Kill(s, _, _) | Action::WaitGtidOf(s, _) => {
                conns[*s].is_none()
            }
            _ => false,
        };
        let obs = match a {
            _ if dead => Observed::Done(Outcome::Err(errno::SERVER_LOST)),
            Action::Exec(s, stmt) => {
                let c = conns[*s].expect("checked");
                // Like the real runner: a statement that finishes within
                // SETTLE didn't block.
                let first = m.exec(c, stmt, now);
                let step = settle(&mut m, c, first, &mut now);
                match step {
                    Step::Done(o) => {
                        if *stmt == Stmt::LastGtid
                            && let Some(g) = o.scalar().and_then(|g| g.parse().ok())
                        {
                            last_gtid.insert(*s, g);
                        }
                        Observed::Done(o)
                    }
                    Step::Blocked => Observed::Blocked,
                }
            }
            Action::Kill(s, target, connection) => {
                let c = conns[*s].expect("checked");
                let t = conns[*target].unwrap_or(ConnId(0));
                let stmt = if *connection {
                    Stmt::KillConnection(t)
                } else {
                    Stmt::KillQuery(t)
                };
                let first = m.exec(c, &stmt, now);
                match settle(&mut m, c, first, &mut now) {
                    Step::Done(o) => Observed::Done(o),
                    Step::Blocked => Observed::Blocked,
                }
            }
            Action::WaitGtidOf(s, of) => match last_gtid.get(of) {
                None => Observed::Stuck(format!("session {of} has no GTID")),
                Some(g) => {
                    let c = conns[*s].expect("checked");
                    let stmt = Stmt::MasterGtidWait {
                        gtid: *g,
                        timeout: 10,
                    };
                    let step = m.exec(c, &stmt, now);
                    Observed::Done(wait(&mut m, c, step, &mut now))
                }
            },
            Action::Await(s) => match conns[*s] {
                None => Observed::Stuck("no connection".into()),
                Some(c) => {
                    let o = wait(&mut m, c, Step::Blocked, &mut now);
                    if o == Outcome::Err(0) {
                        Observed::Stuck("never finishes".into())
                    } else {
                        Observed::Done(o)
                    }
                }
            },
            Action::Sleep(d) => {
                now += *d;
                m.advance(now);
                Observed::Nothing
            }
            Action::Disconnect(s) => {
                if let Some(c) = conns[*s].take() {
                    m.disconnect(c, now);
                }
                Observed::Nothing
            }
            Action::Connect(s) => {
                conns[*s] = connect(&mut m, &sc.sessions[*s], now);
                Observed::Nothing
            }
            Action::Crash(n) => {
                m.crash(&node_name(*n), now);
                Observed::Nothing
            }
            Action::Restart(n) => {
                now += Duration::from_secs(1);
                m.restart(&node_name(*n), now);
                Observed::Nothing
            }
        };
        out.push(normalize(a, obs));
    }
    out
}

/// Runs the model's events for up to [`SETTLE`] after a statement
/// blocked; returns its outcome if it finished by then.
fn settle(m: &mut Model, c: ConnId, step: Step, now: &mut Duration) -> Step {
    if step != Step::Blocked {
        return step;
    }
    let limit = *now + SETTLE;
    loop {
        if let Some(o) = m.take_done(c) {
            return Step::Done(o);
        }
        match m.next_event_time() {
            Some(t) if t <= limit => {
                *now = t;
                m.advance(t);
            }
            _ => {
                *now = limit;
                m.advance(limit);
                return match m.take_done(c) {
                    Some(o) => Step::Done(o),
                    None => Step::Blocked,
                };
            }
        }
    }
}

fn stmt_of(a: &Action) -> Option<&Stmt> {
    match a {
        Action::Exec(_, s) => Some(s),
        _ => None,
    }
}

/// Normalises an observation for comparison.
fn normalize(a: &Action, obs: Observed) -> Observed {
    let Observed::Done(Outcome::Ok { rows, affected }) = obs else {
        return obs;
    };
    let returns_rows = matches!(
        stmt_of(a),
        Some(
            Stmt::Get { .. }
                | Stmt::GetForUpdate { .. }
                | Stmt::Scan(_)
                | Stmt::XaRecover
                | Stmt::LastGtid
                | Stmt::GtidBinlogPos
                | Stmt::MasterGtidWait { .. }
        )
    ) || matches!(a, Action::WaitGtidOf(..));
    // GTID positions keep `domain-server` per GTID: sequence numbers depend
    // on the server's history.
    let rows = match stmt_of(a) {
        Some(Stmt::LastGtid | Stmt::GtidBinlogPos) => rows
            .into_iter()
            .map(|r| {
                r.into_iter()
                    .map(|v| {
                        v.map(|pos| {
                            pos.split(',')
                                .map(|g| match g.trim().parse::<Gtid>() {
                                    Ok(g) => format!("{}-{}", g.domain, g.server),
                                    Err(_) => g.to_owned(),
                                })
                                .collect::<Vec<_>>()
                                .join(",")
                        })
                    })
                    .collect()
            })
            .collect(),
        _ => rows,
    };
    if returns_rows {
        Observed::Done(Outcome::Ok { rows, affected: 0 })
    } else {
        Observed::Done(Outcome::Ok {
            rows: Vec::new(),
            affected,
        })
    }
}

// ----------------------------------------------------------------------
// The real side.

/// A real target: a root URL per node, and (for throwaway targets) the
/// container to crash.
#[derive(Debug, Clone)]
pub struct RealTarget {
    /// Root URLs, node 0 first.
    pub urls: Vec<String>,
    /// The container of node 0, for `Crash` and `Restart`.
    pub container: Option<String>,
}

struct Real {
    target: RealTarget,
    schema: String,
    suffix: String,
    conns: Vec<Option<Conn>>,
    pending: BTreeMap<usize, tokio::task::JoinHandle<(Conn, Outcome)>>,
    ids: BTreeMap<usize, u32>,
    last_gtid: BTreeMap<usize, String>,
}

fn opts(url: &str, user: Option<&str>) -> Result<Opts> {
    let base = Opts::from_url(url).map_err(|e| SprigError::internal(format!("bad URL: {e}")))?;
    let mut b = OptsBuilder::from_opts(base).prefer_socket(false);
    if let Some(u) = user {
        b = b.user(Some(u)).pass(Some(USER_PASSWORD));
    }
    Ok(b.into())
}

async fn connect(url: &str, user: Option<&str>) -> Result<Conn> {
    let o = opts(url, user)?;
    tokio::time::timeout(Duration::from_secs(10), Conn::new(o))
        .await
        .map_err(|_| SprigError::internal(format!("connecting to {url}: timed out")))?
        .map_err(|e| SprigError::internal(format!("connecting: {e}")))
}

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

impl Real {
    fn xid(&self, x: &str) -> String {
        format!("{x}_{}", self.suffix)
    }

    fn user(&self, u: &str) -> String {
        format!("{u}_{}", self.suffix)
    }

    fn table(&self, t: &str) -> String {
        format!("`{}`.`{t}`", self.schema)
    }

    fn sql(&self, stmt: &Stmt) -> String {
        match stmt {
            Stmt::CreateTable(t) => format!(
                "CREATE TABLE {} (k BIGINT PRIMARY KEY, v VARCHAR(255)) ENGINE=InnoDB",
                self.table(t)
            ),
            Stmt::Begin => "BEGIN".into(),
            Stmt::BeginSnapshot => "START TRANSACTION WITH CONSISTENT SNAPSHOT".into(),
            Stmt::Commit => "COMMIT".into(),
            Stmt::Rollback => "ROLLBACK".into(),
            Stmt::SetIsolation(i) => format!(
                "SET SESSION TRANSACTION ISOLATION LEVEL {}",
                match i {
                    Isolation::ReadCommitted => "READ COMMITTED",
                    Isolation::RepeatableRead => "REPEATABLE READ",
                }
            ),
            Stmt::SetLockWaitTimeout(n) => format!("SET SESSION innodb_lock_wait_timeout = {n}"),
            Stmt::Get { t, k } => format!("SELECT k, v FROM {} WHERE k = {k}", self.table(t)),
            Stmt::GetForUpdate { t, k } => {
                format!(
                    "SELECT k, v FROM {} WHERE k = {k} FOR UPDATE",
                    self.table(t)
                )
            }
            Stmt::Scan(t) => format!("SELECT k, v FROM {} ORDER BY k", self.table(t)),
            Stmt::Insert { t, k, v } => format!(
                "INSERT INTO {} (k, v) VALUES ({k}, {})",
                self.table(t),
                quote(v)
            ),
            Stmt::Update { t, k, v } => {
                format!(
                    "UPDATE {} SET v = {} WHERE k = {k}",
                    self.table(t),
                    quote(v)
                )
            }
            Stmt::Delete { t, k } => format!("DELETE FROM {} WHERE k = {k}", self.table(t)),
            Stmt::XaStart(x) => format!("XA START {}", quote(&self.xid(x))),
            Stmt::XaEnd(x) => format!("XA END {}", quote(&self.xid(x))),
            Stmt::XaPrepare(x) => format!("XA PREPARE {}", quote(&self.xid(x))),
            Stmt::XaCommit(x) => format!("XA COMMIT {}", quote(&self.xid(x))),
            Stmt::XaCommitOnePhase(x) => format!("XA COMMIT {} ONE PHASE", quote(&self.xid(x))),
            Stmt::XaRollback(x) => format!("XA ROLLBACK {}", quote(&self.xid(x))),
            Stmt::XaRecover => "XA RECOVER".into(),
            Stmt::LastGtid => "SELECT @@last_gtid".into(),
            Stmt::GtidBinlogPos => "SELECT @@gtid_binlog_pos".into(),
            Stmt::MasterGtidWait { gtid, timeout } => {
                format!(
                    "SELECT MASTER_GTID_WAIT({}, {timeout})",
                    quote(&gtid.to_string())
                )
            }
            Stmt::KillQuery(c) => format!("KILL QUERY {}", c.0),
            Stmt::KillConnection(c) => format!("KILL CONNECTION {}", c.0),
            Stmt::SetReadOnly(on) => {
                format!("SET GLOBAL read_only = {}", if *on { "ON" } else { "OFF" })
            }
            Stmt::CreateUser(u) => format!(
                "CREATE USER {}@'%' IDENTIFIED BY {}",
                quote(&self.user(u)),
                quote(USER_PASSWORD)
            ),
            Stmt::GrantAll(u) => format!(
                "GRANT ALL ON `{}`.* TO {}@'%'",
                self.schema,
                quote(&self.user(u))
            ),
            Stmt::RevokeAll(u) => format!(
                "REVOKE ALL ON `{}`.* FROM {}@'%'",
                self.schema,
                quote(&self.user(u))
            ),
            Stmt::Use => format!("USE `{}`", self.schema),
        }
    }

    /// Maps real rows back to scenario terms.
    fn unmap(&self, stmt: &Stmt, rows: Vec<Row>) -> Vec<Row> {
        match stmt {
            Stmt::XaRecover => {
                let tail = format!("_{}", self.suffix);
                // Columns: formatID, gtrid_length, bqual_length, data.
                let mut xs: Vec<Row> = rows
                    .into_iter()
                    .filter_map(|r| r.get(3).cloned().flatten())
                    .filter_map(|d| d.strip_suffix(&tail).map(str::to_owned))
                    .map(|x| vec![Some(x)])
                    .collect();
                xs.sort();
                xs
            }
            _ => rows,
        }
    }

    async fn session_conn(&self, sc: &Scenario, s: usize) -> Option<Conn> {
        let spec = &sc.sessions[s];
        let user = spec.user.map(|u| self.user(u));
        connect(&self.target.urls[spec.node], user.as_deref())
            .await
            .ok()
    }

    async fn exec_on(mut conn: Conn, sql: String, query: bool) -> (Conn, Outcome) {
        let r = if query {
            conn.query::<mysql_async::Row, _>(sql).await.map(|rows| {
                let rows = rows
                    .into_iter()
                    .map(|r| r.unwrap().into_iter().map(text).collect())
                    .collect();
                Outcome::rows(rows)
            })
        } else {
            match conn.query_drop(sql).await {
                Ok(()) => Ok(Outcome::affected(conn.affected_rows())),
                Err(e) => Err(e),
            }
        };
        let o = match r {
            Ok(o) => o,
            Err(mysql_async::Error::Server(e)) => Outcome::Err(e.code),
            Err(_) => Outcome::Err(errno::SERVER_LOST),
        };
        (conn, o)
    }
}

fn text(v: Value) -> Option<String> {
    match v {
        Value::NULL => None,
        Value::Bytes(b) => Some(String::from_utf8_lossy(&b).into_owned()),
        Value::Int(i) => Some(i.to_string()),
        Value::UInt(u) => Some(u.to_string()),
        other => Some(other.as_sql(true)),
    }
}

/// The `(server_id, gtid domain)` of each node of a real target. Galera
/// nodes report `wsrep_gtid_domain_id`.
///
/// # Errors
/// If a node can't be reached.
pub async fn probe_ids(target: &RealTarget, galera: bool) -> Result<NodeIds> {
    let mut ids = Vec::new();
    for url in &target.urls {
        let mut c = connect(url, None).await?;
        let q = if galera {
            "SELECT @@server_id, @@wsrep_gtid_domain_id"
        } else {
            "SELECT @@server_id, @@gtid_domain_id"
        };
        let row: Option<(u32, u32)> = c
            .query_first(q)
            .await
            .map_err(|e| SprigError::internal(format!("probing {url}: {e}")))?;
        let _ = c.disconnect().await;
        ids.push(row.ok_or_else(|| SprigError::internal("no ids"))?);
    }
    Ok(ids)
}

/// Runs `sc` on a real target, in a schema and with users and XIDs unique
/// to this run, and cleans up afterwards.
///
/// # Errors
/// If setup fails; statement errors are observations, not failures.
pub async fn run_real(sc: &Scenario, target: &RealTarget) -> Result<Vec<Observed>> {
    let schema = unique_ident(&format!("sim_{}", sc.name));
    let suffix = schema.rsplit('_').next().unwrap_or("x").to_owned();
    let mut r = Real {
        target: target.clone(),
        schema,
        suffix,
        conns: Vec::new(),
        pending: BTreeMap::new(),
        ids: BTreeMap::new(),
        last_gtid: BTreeMap::new(),
    };
    let mut root = connect(&target.urls[0], None).await?;
    let setup = async {
        root.query_drop(format!("CREATE DATABASE `{}`", r.schema))
            .await?;
        for t in &sc.tables {
            root.query_drop(r.sql(&Stmt::CreateTable((*t).to_owned())))
                .await?;
        }
        for u in &sc.users {
            root.query_drop(r.sql(&Stmt::CreateUser((*u).to_owned())))
                .await?;
        }
        Ok::<(), mysql_async::Error>(())
    };
    setup
        .await
        .map_err(|e| SprigError::internal(format!("scenario {} setup: {e}", sc.name)))?;
    // Wait until every node has the tables and users (replication, Galera).
    for url in &target.urls {
        let want = sc.tables.len();
        let schema = r.schema.clone();
        Retry::new(Duration::from_secs(20))
            .run("tables replicated", || {
                let (url, schema) = (url.clone(), schema.clone());
                async move {
                    let mut c = connect(&url, None).await?;
                    let n: Option<usize> = c
                        .exec_first(
                            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ?",
                            (schema,),
                        )
                        .await
                        .map_err(|e| SprigError::internal(e.to_string()))?;
                    let _ = c.disconnect().await;
                    if n == Some(want) {
                        Ok(())
                    } else {
                        Err(SprigError::internal(format!("{n:?} of {want} tables")))
                    }
                }
            })
            .await?;
    }
    for s in 0..sc.sessions.len() {
        let c = r.session_conn(sc, s).await;
        if let Some(c) = &c {
            r.ids.insert(s, c.id());
        }
        r.conns.push(c);
    }
    let result = run_actions(sc, &mut r).await;
    cleanup(&mut r, sc).await;
    let _ = root.disconnect().await;
    Ok(result)
}

async fn run_actions(sc: &Scenario, r: &mut Real) -> Vec<Observed> {
    let mut out = Vec::with_capacity(sc.actions.len());
    for a in &sc.actions {
        let obs = match a {
            Action::Exec(s, stmt) => {
                let sql = r.sql(stmt);
                exec_real(r, *s, stmt, sql).await
            }
            Action::Kill(s, target, connection) => {
                let id = r.ids.get(target).copied().unwrap_or(0);
                let stmt = if *connection {
                    Stmt::KillConnection(ConnId(u64::from(id)))
                } else {
                    Stmt::KillQuery(ConnId(u64::from(id)))
                };
                let sql = r.sql(&stmt);
                exec_real(r, *s, &stmt, sql).await
            }
            Action::WaitGtidOf(s, of) => {
                match r.last_gtid.get(of).and_then(|g| g.parse::<Gtid>().ok()) {
                    None => Observed::Stuck(format!("session {of} has no GTID")),
                    Some(gtid) => {
                        let stmt = Stmt::MasterGtidWait { gtid, timeout: 10 };
                        let sql = r.sql(&stmt);
                        match exec_real(r, *s, &stmt, sql).await {
                            Observed::Blocked => await_real(r, *s, &stmt).await,
                            o => o,
                        }
                    }
                }
            }
            Action::Await(s) => {
                let stmt = Stmt::Commit;
                await_real(r, *s, &stmt).await
            }
            Action::Sleep(d) => {
                tokio::time::sleep(*d).await;
                Observed::Nothing
            }
            Action::Disconnect(s) => {
                if let Some(c) = r.conns[*s].take() {
                    let _ = c.disconnect().await;
                }
                Observed::Nothing
            }
            Action::Connect(s) => {
                let c = r.session_conn(sc, *s).await;
                if let Some(c) = &c {
                    r.ids.insert(*s, c.id());
                }
                r.conns[*s] = c;
                Observed::Nothing
            }
            Action::Crash(_) => {
                if let Some(name) = &r.target.container {
                    let _ = crate::faults::compose::docker(&["kill", "--signal", "KILL", name]);
                }
                Observed::Nothing
            }
            Action::Restart(_) => {
                if let Some(name) = &r.target.container {
                    let _ = crate::faults::compose::docker(&["start", name]);
                    let url = r.target.urls[0].clone();
                    let _ = Retry::new(Duration::from_secs(90))
                        .run("restarted server accepts connections", || {
                            let url = url.clone();
                            async move {
                                let c = connect(&url, None).await?;
                                let _ = c.disconnect().await;
                                Ok::<(), SprigError>(())
                            }
                        })
                        .await;
                }
                Observed::Nothing
            }
        };
        out.push(normalize(a, obs));
    }
    out
}

async fn exec_real(r: &mut Real, s: usize, stmt: &Stmt, sql: String) -> Observed {
    if r.pending.contains_key(&s) {
        return Observed::Stuck(format!("session {s} still waits"));
    }
    let Some(conn) = r.conns[s].take() else {
        return Observed::Done(Outcome::Err(errno::SERVER_LOST));
    };
    let query = matches!(
        stmt,
        Stmt::Get { .. }
            | Stmt::GetForUpdate { .. }
            | Stmt::Scan(_)
            | Stmt::XaRecover
            | Stmt::LastGtid
            | Stmt::GtidBinlogPos
            | Stmt::MasterGtidWait { .. }
    );
    let mut h = tokio::spawn(Real::exec_on(conn, sql, query));
    match tokio::time::timeout(SETTLE, &mut h).await {
        Ok(Ok((conn, o))) => finish_real(r, s, stmt, conn, o),
        Ok(Err(e)) => Observed::Stuck(format!("task failed: {e}")),
        Err(_) => {
            r.pending.insert(s, h);
            Observed::Blocked
        }
    }
}

fn finish_real(r: &mut Real, s: usize, stmt: &Stmt, conn: Conn, o: Outcome) -> Observed {
    let alive =
        o != Outcome::Err(errno::SERVER_LOST) && o != Outcome::Err(errno::CONNECTION_KILLED);
    if alive {
        r.conns[s] = Some(conn);
    }
    if *stmt == Stmt::LastGtid
        && let Some(g) = o.scalar()
    {
        r.last_gtid.insert(s, g.to_owned());
    }
    let o = match o {
        Outcome::Ok { rows, affected } => Outcome::Ok {
            rows: r.unmap(stmt, rows),
            affected,
        },
        e @ Outcome::Err(_) => e,
    };
    Observed::Done(o)
}

async fn await_real(r: &mut Real, s: usize, stmt: &Stmt) -> Observed {
    let Some(h) = r.pending.remove(&s) else {
        return Observed::Stuck(format!("session {s} has nothing pending"));
    };
    match tokio::time::timeout(AWAIT_LIMIT, h).await {
        Ok(Ok((conn, o))) => finish_real(r, s, stmt, conn, o),
        Ok(Err(e)) => Observed::Stuck(format!("task failed: {e}")),
        Err(_) => Observed::Stuck("never finishes".into()),
    }
}

/// Drops the run's sessions, prepared XIDs, users and schema.
async fn cleanup(r: &mut Real, sc: &Scenario) {
    for (_, h) in std::mem::take(&mut r.pending) {
        h.abort();
    }
    for c in r.conns.drain(..).flatten() {
        let _ = c.disconnect().await;
    }
    let Ok(mut root) = connect(&r.target.urls[0], None).await else {
        return;
    };
    // Prepared branches hold locks that would block DROP DATABASE.
    if let Ok(rows) = root.query::<mysql_async::Row, _>("XA RECOVER").await {
        let tail = format!("_{}", r.suffix);
        for row in rows {
            let data: Vec<Option<String>> = row.unwrap().into_iter().map(text).collect();
            if let Some(Some(x)) = data.get(3)
                && x.ends_with(&tail)
            {
                let _ = root.query_drop(format!("XA ROLLBACK {}", quote(x))).await;
            }
        }
    }
    if sc.target == Target::Throwaway {
        let _ = root.query_drop("SET GLOBAL read_only = OFF").await;
    }
    for u in &sc.users {
        let _ = root
            .query_drop(format!("DROP USER IF EXISTS {}@'%'", quote(&r.user(u))))
            .await;
    }
    let _ = root
        .query_drop(format!("DROP DATABASE IF EXISTS `{}`", r.schema))
        .await;
    let _ = root.disconnect().await;
}

/// Compares model and real observations; returns one line per difference.
#[must_use]
pub fn diff(sc: &Scenario, model: &[Observed], real: &[Observed]) -> Vec<String> {
    let mut out = Vec::new();
    for (i, a) in sc.actions.iter().enumerate() {
        let (m, r) = (model.get(i), real.get(i));
        if m != r {
            out.push(format!(
                "{}: step {i} {a:?}: model {} / real {}",
                sc.name,
                m.map_or("-".into(), ToString::to_string),
                r.map_or("-".into(), ToString::to_string)
            ));
        }
    }
    out
}

/// Renders observations next to their actions, for failure messages.
#[must_use]
pub fn render(sc: &Scenario, obs: &[Observed]) -> String {
    let mut s = String::new();
    for (i, (a, o)) in sc.actions.iter().zip(obs).enumerate() {
        let _ = writeln!(s, "  {i:>2} {a:?} => {o}");
    }
    s
}

// ----------------------------------------------------------------------
// The scenarios.

fn tbl() -> String {
    "t".to_owned()
}

fn ins(k: i64, v: &str) -> Stmt {
    Stmt::Insert {
        t: tbl(),
        k,
        v: v.into(),
    }
}

fn upd(k: i64, v: &str) -> Stmt {
    Stmt::Update {
        t: tbl(),
        k,
        v: v.into(),
    }
}

fn get(k: i64) -> Stmt {
    Stmt::Get { t: tbl(), k }
}

fn x(s: &str) -> String {
    s.to_owned()
}

/// Every scenario of the suite.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn scenarios() -> Vec<Scenario> {
    use Action::{Await, Connect, Crash, Disconnect, Exec, Kill, Restart, Sleep, WaitGtidOf};
    let ms = Duration::from_millis;
    vec![
        Scenario {
            name: "autocommit_and_gtids",
            target: Target::Single,
            tables: vec!["t"],
            users: vec![],
            sessions: vec![Session::root(0)],
            actions: vec![
                Exec(0, ins(1, "a")),
                Exec(0, ins(1, "b")),
                Exec(0, upd(1, "a")),
                Exec(0, upd(1, "c")),
                Exec(0, upd(9, "none")),
                Exec(0, get(1)),
                Exec(0, Stmt::LastGtid),
                Exec(0, Stmt::Delete { t: tbl(), k: 1 }),
                Exec(0, Stmt::Scan(tbl())),
            ],
        },
        Scenario {
            name: "lock_wait_timeout",
            target: Target::Single,
            tables: vec!["t"],
            users: vec![],
            sessions: vec![Session::root(0), Session::root(0), Session::root(0)],
            actions: vec![
                Exec(0, ins(1, "x")),
                Exec(0, ins(2, "x")),
                Exec(1, Stmt::SetLockWaitTimeout(1)),
                Exec(0, Stmt::Begin),
                Exec(0, upd(1, "a")),
                Exec(1, Stmt::Begin),
                Exec(1, upd(2, "b")),
                Exec(1, upd(1, "b")),
                Await(1),
                // Only the statement rolled back: b's earlier write stays.
                Exec(1, get(2)),
                Exec(0, Stmt::Commit),
                Exec(1, Stmt::Commit),
                Exec(2, Stmt::Scan(tbl())),
            ],
        },
        Scenario {
            name: "deadlock_lighter_victim",
            target: Target::Single,
            tables: vec!["t"],
            users: vec![],
            sessions: vec![Session::root(0), Session::root(0), Session::root(0)],
            actions: vec![
                Exec(0, ins(1, "x")),
                Exec(0, ins(2, "x")),
                Exec(0, ins(3, "x")),
                Exec(0, Stmt::Begin),
                Exec(0, upd(1, "a")),
                Exec(0, upd(3, "a")),
                Exec(1, Stmt::Begin),
                Exec(1, upd(2, "b")),
                Exec(0, upd(2, "a")),
                Exec(1, upd(1, "b")),
                Await(0),
                Exec(0, Stmt::Commit),
                Exec(2, Stmt::Scan(tbl())),
            ],
        },
        Scenario {
            name: "deadlock_equal_weights",
            target: Target::Single,
            tables: vec!["t"],
            users: vec![],
            sessions: vec![Session::root(0), Session::root(0), Session::root(0)],
            actions: vec![
                Exec(0, ins(1, "x")),
                Exec(0, ins(2, "x")),
                Exec(0, Stmt::Begin),
                Exec(0, upd(1, "a")),
                Exec(1, Stmt::Begin),
                Exec(1, upd(2, "b")),
                Exec(0, upd(2, "a")),
                Exec(1, upd(1, "b")),
                Await(0),
                Exec(0, Stmt::Commit),
                Exec(1, Stmt::Commit),
                Exec(2, Stmt::Scan(tbl())),
            ],
        },
        Scenario {
            name: "snapshot_isolation",
            target: Target::Single,
            tables: vec!["t"],
            users: vec![],
            sessions: vec![Session::root(0), Session::root(0)],
            actions: vec![
                Exec(0, ins(1, "old")),
                Exec(0, Stmt::Begin),
                Exec(0, get(1)),
                Exec(1, upd(1, "new")),
                Exec(0, get(1)),
                Exec(0, Stmt::GetForUpdate { t: tbl(), k: 1 }),
                // Whether the error ended the transaction shows here.
                Exec(0, get(1)),
                Exec(0, Stmt::Commit),
                Exec(0, Stmt::SetIsolation(Isolation::ReadCommitted)),
                Exec(0, Stmt::Begin),
                Exec(0, get(1)),
                Exec(1, upd(1, "newer")),
                Exec(0, get(1)),
                Exec(0, upd(1, "mine")),
                Exec(0, Stmt::Commit),
            ],
        },
        Scenario {
            name: "xa_lifecycle",
            target: Target::Single,
            tables: vec!["t"],
            users: vec![],
            sessions: vec![Session::root(0), Session::root(0)],
            actions: vec![
                Exec(0, ins(1, "x")),
                Exec(0, Stmt::XaStart(x("x1"))),
                Exec(0, upd(1, "xa")),
                Exec(0, Stmt::Commit),
                Exec(0, Stmt::XaEnd(x("x1"))),
                Exec(0, Stmt::XaPrepare(x("x1"))),
                Exec(0, get(1)),
                // Variables stay readable in the prepared state.
                Exec(0, Stmt::LastGtid),
                Exec(0, Stmt::GtidBinlogPos),
                Exec(1, Stmt::XaRecover),
                Exec(1, Stmt::XaCommit(x("x1"))),
                Exec(1, Stmt::SetLockWaitTimeout(1)),
                Exec(1, upd(1, "other")),
                Await(1),
                Disconnect(0),
                Sleep(ms(200)),
                Exec(1, Stmt::XaRecover),
                Exec(1, Stmt::XaCommit(x("x1"))),
                Exec(1, get(1)),
                Exec(1, Stmt::XaRecover),
                Exec(1, Stmt::XaStart(x("x2"))),
                Exec(1, ins(2, "one-phase")),
                Exec(1, Stmt::XaEnd(x("x2"))),
                Exec(1, Stmt::XaCommitOnePhase(x("x2"))),
                Exec(1, Stmt::Scan(tbl())),
            ],
        },
        Scenario {
            name: "kill_query_and_connection",
            target: Target::Single,
            tables: vec!["t"],
            users: vec![],
            sessions: vec![Session::root(0), Session::root(0), Session::root(0)],
            actions: vec![
                Exec(0, ins(1, "x")),
                Exec(0, Stmt::Begin),
                Exec(0, upd(1, "a")),
                Exec(1, upd(1, "b")),
                Kill(2, 1, false),
                Await(1),
                Exec(1, upd(1, "b")),
                Kill(2, 0, true),
                Await(1),
                Exec(0, get(1)),
                Exec(2, get(1)),
            ],
        },
        Scenario {
            name: "revoke_misses_current_db",
            target: Target::Single,
            tables: vec!["t"],
            users: vec!["app"],
            sessions: vec![
                Session::root(0),
                Session::user(0, "app"),
                Session::user(0, "app"),
            ],
            actions: vec![
                Exec(0, Stmt::GrantAll(x("app"))),
                Exec(1, Stmt::Use),
                Exec(2, ins(1, "before")),
                Exec(0, Stmt::RevokeAll(x("app"))),
                Exec(1, ins(2, "used")),
                Exec(2, ins(3, "qualified")),
                Exec(1, Stmt::Use),
                Exec(0, Stmt::Scan(tbl())),
            ],
        },
        Scenario {
            name: "replicas_apply_gtids",
            target: Target::Replication,
            tables: vec!["t"],
            users: vec![],
            sessions: vec![Session::root(0), Session::root(1), Session::root(2)],
            actions: vec![
                Exec(0, ins(1, "a")),
                Exec(0, Stmt::LastGtid),
                WaitGtidOf(1, 0),
                Exec(1, get(1)),
                WaitGtidOf(2, 0),
                Exec(2, get(1)),
                Exec(1, Stmt::LastGtid),
            ],
        },
        Scenario {
            name: "galera_certification",
            target: Target::Galera,
            tables: vec!["t"],
            users: vec![],
            sessions: vec![Session::root(0), Session::root(1), Session::root(2)],
            actions: vec![
                Exec(0, ins(1, "x")),
                Sleep(ms(300)),
                Exec(0, Stmt::Begin),
                Exec(1, Stmt::Begin),
                Exec(0, upd(1, "a")),
                Exec(1, upd(1, "b")),
                Exec(0, Stmt::Commit),
                Sleep(ms(300)),
                Exec(1, Stmt::Commit),
                Exec(0, Stmt::LastGtid),
                Exec(2, get(1)),
                Exec(1, get(1)),
            ],
        },
        Scenario {
            // Two transactions on one node, ordered by a row lock: the
            // second wrote before the first committed, but certification
            // doesn't fail it (only other nodes' writesets conflict).
            name: "galera_local_lock_order",
            target: Target::Galera,
            tables: vec!["t"],
            users: vec![],
            sessions: vec![Session::root(0), Session::root(0)],
            actions: vec![
                Exec(0, ins(1, "x")),
                Exec(1, Stmt::Begin),
                Exec(1, ins(2, "b")),
                Exec(0, Stmt::Begin),
                Exec(0, upd(1, "a")),
                Exec(0, Stmt::Commit),
                Exec(1, upd(1, "b")),
                Exec(1, Stmt::Commit),
                Exec(1, get(1)),
            ],
        },
        Scenario {
            name: "crash_keeps_committed_and_prepared",
            target: Target::Throwaway,
            tables: vec!["t"],
            users: vec![],
            sessions: vec![
                Session::root(0),
                Session::root(0),
                Session::root(0),
                Session::root(0),
            ],
            actions: vec![
                Exec(0, ins(1, "committed")),
                Exec(1, Stmt::XaStart(x("p1"))),
                Exec(1, ins(2, "prepared")),
                Exec(1, Stmt::XaEnd(x("p1"))),
                Exec(1, Stmt::XaPrepare(x("p1"))),
                Exec(2, Stmt::Begin),
                Exec(2, ins(3, "open")),
                Crash(0),
                Restart(0),
                Exec(0, get(1)),
                Connect(3),
                Exec(3, Stmt::Scan(tbl())),
                Exec(3, Stmt::XaRecover),
                Exec(3, Stmt::XaCommit(x("p1"))),
                Exec(3, Stmt::Scan(tbl())),
            ],
        },
        Scenario {
            name: "read_only_fences_users",
            target: Target::Throwaway,
            tables: vec!["t"],
            users: vec!["app"],
            sessions: vec![Session::root(0), Session::user(0, "app")],
            actions: vec![
                Exec(0, Stmt::GrantAll(x("app"))),
                Exec(1, ins(1, "before")),
                Exec(0, Stmt::SetReadOnly(true)),
                Exec(1, ins(2, "fenced")),
                Exec(1, get(1)),
                Exec(0, ins(3, "root")),
                Exec(0, Stmt::SetReadOnly(false)),
                Exec(1, ins(4, "after")),
                Exec(0, Stmt::Scan(tbl())),
            ],
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The model's side of every scenario, pinned: when the conformance
    /// suite finds a divergence and the model is fixed, this changes too.
    #[test]
    fn model_outcomes() {
        for sc in scenarios() {
            let obs = run_model(&sc, &default_ids(sc.target));
            assert_eq!(obs.len(), sc.actions.len());
            for (a, o) in sc.actions.iter().zip(&obs) {
                assert!(!matches!(o, Observed::Stuck(_)), "{}: {a:?} stuck", sc.name);
            }
            let text = render(&sc, &obs);
            match sc.name {
                "lock_wait_timeout" => {
                    assert!(text.contains("Await(1) => error 1205"), "{text}");
                }
                "deadlock_lighter_victim" => {
                    assert!(text.contains("error 1213"), "{text}");
                }
                "kill_query_and_connection" => {
                    assert!(text.contains("error 1317"), "{text}");
                }
                "galera_certification" => {
                    assert!(text.contains("Exec(1, Commit) => error 1213"), "{text}");
                }
                "galera_local_lock_order" => {
                    assert!(!text.contains("error 1213"), "{text}");
                    assert!(text.contains("Exec(1, Commit) => ok"), "{text}");
                }
                _ => {}
            }
        }
    }
}
