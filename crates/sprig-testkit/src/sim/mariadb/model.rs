//! The synchronous core of the MariaDB model. See the parent module for
//! what it covers.
//!
//! Time is an argument: every call takes `now`, the virtual time since the
//! model started, and [`Model::advance`] runs the timed events due by then
//! (replication deliveries and acknowledgements, replica apply, Galera
//! apply, lock-wait and semi-sync timeouts). A statement either finishes
//! ([`Step::Done`]) or blocks ([`Step::Blocked`]); a blocked statement
//! finishes during a later call, and [`Model::take_done`] returns its
//! outcome.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::{Gtid, Isolation, Outcome, Row, Stmt, errno};
use crate::sim::rng::SimRng;

/// Extra error numbers used only here.
mod more_errno {
    pub(super) const DB_ACCESS_DENIED: u16 = 1044;
    pub(super) const ACCESS_DENIED: u16 = 1045;
    pub(super) const TABLE_EXISTS: u16 = 1050;
    pub(super) const UNKNOWN_THREAD: u16 = 1094;
    pub(super) const XAER_DUPID: u16 = 1440;
    /// `ER_NOT_SUPPORTED_YET`: "XA transactions with Galera replication"
    /// (spike-xa).
    pub(super) const NOT_SUPPORTED_YET: u16 = 1235;
    pub(super) const COMMANDS_OUT_OF_SYNC: u16 = 2014;
}

/// Whether `ER_CHECKREAD` rolls the whole transaction back, as a deadlock
/// does (checked against 11.8 and 12.3 by the conformance suite).
const CHECKREAD_ROLLS_BACK: bool = true;

/// How often a replica's IO thread retries an unreachable source.
const IO_RETRY: Duration = Duration::from_secs(1);

/// A connection to a model server; unique across the model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ConnId(pub u64);

impl fmt::Display for ConnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "conn-{}", self.0)
    }
}

/// What [`Model::exec`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// The statement finished.
    Done(Outcome),
    /// The statement waits (a row lock, a semi-sync acknowledgement, a
    /// GTID); [`Model::take_done`] returns its outcome once it finishes.
    Blocked,
}

/// One server's identity and settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)] // independent server settings
pub struct ServerConfig {
    /// The server's name: its node on the simulated network.
    pub name: String,
    /// `server_id`.
    pub server_id: u32,
    /// `gtid_domain_id` (for Galera, `wsrep_gtid_domain_id`).
    pub domain_id: u32,
    /// `innodb_snapshot_isolation` (ON by default from 11.6).
    pub snapshot_isolation: bool,
    /// `innodb_lock_wait_timeout` for new connections.
    pub lock_wait_timeout: Duration,
    /// `rpl_semi_sync_master_timeout`.
    pub semisync_timeout: Duration,
    /// `rpl_semi_sync_master_enabled` (primary side).
    pub semisync_master: bool,
    /// `read_only` at boot (provisioned servers boot fenced).
    pub read_only_on_boot: bool,
    /// `binlog_storage_engine = innodb` (MariaDB 12.3+): `XA PREPARE` is not
    /// binlogged, so it takes no GTID and replicas never see the prepared
    /// branch; `XA COMMIT` logs the whole transaction as one ordinary group
    /// and `XA ROLLBACK` logs nothing (measured on 12.3.3). Semi-sync
    /// `AFTER_SYNC` doesn't exist with it, so a prepare never
    /// waits for an ack. Off (the classic file binlog) by default, as on
    /// the Compose servers.
    pub binlog_in_innodb: bool,
}

impl ServerConfig {
    /// A server with MariaDB 11.8's defaults and the provisioned semi-sync
    /// timeout (10 s).
    #[must_use]
    pub fn new(name: &str, server_id: u32, domain_id: u32) -> Self {
        Self {
            name: name.to_owned(),
            server_id,
            domain_id,
            snapshot_isolation: true,
            lock_wait_timeout: Duration::from_secs(50),
            semisync_timeout: Duration::from_secs(10),
            semisync_master: false,
            read_only_on_boot: false,
            binlog_in_innodb: false,
        }
    }
}

type TxnId = u64;

/// `schema.table` and a key.
type RowRef = (String, i64);

#[derive(Debug, Clone)]
struct Version {
    seq: u64,
    value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum GroupKind {
    Commit,
    Ddl,
    XaPrepare(String),
    XaCommit(String),
    XaRollback(String),
}

/// One binlog event group.
#[derive(Debug, Clone)]
struct Group {
    gtid: Gtid,
    kind: GroupKind,
    writes: Vec<(RowRef, Option<String>)>,
    /// Tables a DDL group creates.
    creates: Vec<String>,
    /// For grants: (user, schema, granted).
    grant: Option<(String, String, bool)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum XaState {
    Active,
    Idle,
    Prepared,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AfterAck {
    Install,
    Prepared,
}

#[derive(Debug, Clone)]
struct Txn {
    conn: Option<ConnId>,
    implicit: bool,
    iso: Isolation,
    view: Option<u64>,
    writes: BTreeMap<RowRef, Option<String>>,
    locks: BTreeSet<RowRef>,
    xa: Option<(String, XaState)>,
    galera_base: Option<u64>,
    awaiting_ack: Option<(Gtid, AfterAck)>,
    /// The group to install when the ack comes (commit) or at XA COMMIT.
    gtid: Option<Gtid>,
}

impl Txn {
    fn weight(&self) -> usize {
        self.writes.len() + self.locks.len()
    }
}

#[derive(Debug, Clone)]
enum PendingKind {
    Lock(RowRef),
    Ack,
    GtidWait(Gtid),
}

#[derive(Debug, Clone)]
struct Pending {
    stmt: Stmt,
    kind: PendingKind,
    token: u64,
    order: (Duration, u64),
    /// The statement opened an implicit (autocommit) transaction.
    implicit: bool,
}

#[derive(Debug, Clone)]
struct Conn {
    server: String,
    user: Option<String>,
    schema: String,
    current_db: bool,
    db_access: bool,
    txn: Option<TxnId>,
    iso: Isolation,
    lock_wait: Duration,
    pending: Option<Pending>,
    done: Option<Outcome>,
    alive: bool,
    last_gtid: Option<Gtid>,
    poisoned: Option<u16>,
}

#[derive(Debug, Clone)]
struct Replica {
    source: String,
    io_running: bool,
    sql_running: bool,
    semisync_slave: bool,
    lag: Duration,
    /// Highest seq per domain received (in relay or in flight).
    requested: BTreeMap<u32, u64>,
    relay: VecDeque<(Duration, Group)>,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct Server {
    cfg: ServerConfig,
    up: bool,
    read_only: bool,
    tables: BTreeMap<String, BTreeMap<i64, Vec<Version>>>,
    commit_seq: u64,
    locks: BTreeMap<RowRef, TxnId>,
    txns: BTreeMap<TxnId, Txn>,
    binlog: Vec<Group>,
    /// Last GTID per domain in this server's binlog.
    binlog_state: BTreeMap<u32, Gtid>,
    /// Last GTID per domain applied by replication (`gtid_slave_pos`).
    slave_pos: BTreeMap<u32, Gtid>,
    users: BTreeMap<String, BTreeSet<String>>,
    /// Prepared XA branches by XID.
    prepared: BTreeMap<String, TxnId>,
    semisync_on: bool,
    replica: Option<Replica>,
    galera: Option<String>,
    galera_applied: u64,
}

impl Server {
    fn new(cfg: ServerConfig) -> Self {
        Self {
            read_only: cfg.read_only_on_boot,
            semisync_on: cfg.semisync_master,
            cfg,
            up: true,
            tables: BTreeMap::new(),
            commit_seq: 0,
            locks: BTreeMap::new(),
            txns: BTreeMap::new(),
            binlog: Vec::new(),
            binlog_state: BTreeMap::new(),
            slave_pos: BTreeMap::new(),
            users: BTreeMap::new(),
            prepared: BTreeMap::new(),
            replica: None,
            galera: None,
            galera_applied: 0,
        }
    }

    fn latest(&self, row: &RowRef) -> Option<&Version> {
        self.tables.get(&row.0)?.get(&row.1)?.last()
    }

    fn visible(&self, row: &RowRef, view: u64) -> Option<String> {
        self.tables
            .get(&row.0)?
            .get(&row.1)?
            .iter()
            .rev()
            .find(|v| v.seq <= view)
            .and_then(|v| v.value.clone())
    }

    fn next_gtid(&mut self, domain: u32, server: u32) -> Gtid {
        let seq = self.binlog_state.get(&domain).map_or(0, |g| g.seq) + 1;
        Gtid {
            domain,
            server,
            seq,
        }
    }

    fn log(&mut self, g: Group) {
        self.binlog_state.insert(g.gtid.domain, g.gtid);
        self.binlog.push(g);
    }

    fn install(&mut self, writes: &[(RowRef, Option<String>)]) {
        if writes.is_empty() {
            return;
        }
        self.commit_seq += 1;
        let seq = self.commit_seq;
        for ((table, k), v) in writes {
            self.tables
                .entry(table.clone())
                .or_default()
                .entry(*k)
                .or_default()
                .push(Version {
                    seq,
                    value: v.clone(),
                });
        }
    }
}

#[derive(Debug, Clone)]
struct Galera {
    nodes: Vec<String>,
    domain: u32,
    seqno: u64,
    gtid_seq: u64,
    /// The last writeset to change each row: its seqno and its node.
    cert: BTreeMap<RowRef, (u64, String)>,
}

#[derive(Debug, Clone)]
enum Event {
    LockTimeout {
        conn: ConnId,
        token: u64,
    },
    AckTimeout {
        server: String,
        gtid: Gtid,
    },
    GtidWaitTimeout {
        conn: ConnId,
        token: u64,
    },
    IoPoll {
        replica: String,
    },
    Arrive {
        replica: String,
        source: String,
        groups: Vec<Group>,
    },
    Ack {
        primary: String,
        replica: String,
        gtid: Gtid,
    },
    Apply {
        replica: String,
    },
    GaleraApply {
        node: String,
        seqno: u64,
        group: Group,
    },
}

/// Reachability between two servers, by name.
pub type Reach = Box<dyn Fn(&str, &str) -> bool + Send>;
/// One-way delay between two servers, by name.
pub type Delay = Box<dyn Fn(&str, &str) -> Duration + Send>;

/// The MariaDB model: servers, replication and Galera clusters,
/// connections, and the queue of timed events.
pub struct Model {
    servers: BTreeMap<String, Server>,
    conns: BTreeMap<ConnId, Conn>,
    galeras: BTreeMap<String, Galera>,
    next_conn: u64,
    next_txn: u64,
    next_token: u64,
    events: BTreeMap<(Duration, u64), Event>,
    next_event: u64,
    now: Duration,
    reach: Reach,
    delay: Delay,
    rng: SimRng,
    completed: Vec<ConnId>,
    log: Vec<String>,
    orphans_run: bool,
    /// Connections whose client went away while a statement waited for a
    /// lock ([`Model::set_orphans_run`]): the statement still runs when the
    /// lock frees, then the connection ends.
    orphans: BTreeSet<ConnId>,
}

impl fmt::Debug for Model {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Model")
            .field("servers", &self.servers.keys().collect::<Vec<_>>())
            .field("now", &self.now)
            .finish_non_exhaustive()
    }
}

fn ok() -> Step {
    Step::Done(Outcome::ok())
}

fn err(e: u16) -> Step {
    Step::Done(Outcome::Err(e))
}

fn qualified(schema: &str, t: &str) -> String {
    format!("{schema}.{t}")
}

impl Model {
    /// An empty model. `seed` drives replication jitter.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            servers: BTreeMap::new(),
            conns: BTreeMap::new(),
            galeras: BTreeMap::new(),
            next_conn: 0,
            next_txn: 0,
            next_token: 0,
            events: BTreeMap::new(),
            next_event: 0,
            now: Duration::ZERO,
            reach: Box::new(|_, _| true),
            delay: Box::new(|_, _| Duration::from_micros(500)),
            rng: SimRng::new(seed),
            completed: Vec::new(),
            log: Vec::new(),
            orphans_run: false,
            orphans: BTreeSet::new(),
        }
    }

    /// Sets who can reach whom (the simulated network's partitions).
    pub fn set_reachability(&mut self, f: Reach) {
        self.reach = f;
    }

    /// Sets the one-way delay between servers.
    pub fn set_delay(&mut self, f: Delay) {
        self.delay = f;
    }

    /// Whether a statement waiting for a row lock keeps running after its
    /// client goes away, as MariaDB's does: the server notices the closed
    /// socket only when it next reads from it, so the statement runs once
    /// the lock frees (an autocommit write commits), and only then does
    /// the connection end and roll back what is left open. Off by default
    /// (the waiting statement is dropped with the connection), which is
    /// kinder than MariaDB; the Sprig 2PC fence simulation turns it on.
    pub fn set_orphans_run(&mut self, on: bool) {
        self.orphans_run = on;
    }

    /// Whether `conn` is still open on its server.
    #[must_use]
    pub fn is_alive(&self, conn: ConnId) -> bool {
        self.conns.get(&conn).is_some_and(|c| c.alive)
    }

    /// Adds a standalone server (a primary or a single node).
    pub fn add_server(&mut self, cfg: ServerConfig) {
        self.servers.insert(cfg.name.clone(), Server::new(cfg));
    }

    /// Adds a replica of `source`, replicating from the start of its
    /// binlog, applying each event `lag` after it arrives.
    ///
    /// # Panics
    /// If `source` doesn't exist.
    pub fn add_replica(&mut self, cfg: ServerConfig, source: &str, lag: Duration) {
        assert!(self.servers.contains_key(source), "no server {source}");
        let name = cfg.name.clone();
        let mut s = Server::new(cfg);
        s.replica = Some(Replica {
            source: source.to_owned(),
            io_running: true,
            sql_running: true,
            semisync_slave: true,
            lag,
            requested: BTreeMap::new(),
            relay: VecDeque::new(),
            error: None,
        });
        self.servers.insert(name.clone(), s);
        self.schedule(self.now, Event::IoPoll { replica: name });
    }

    /// Adds a Galera cluster of `nodes`, sharing `domain`.
    pub fn add_galera(&mut self, cluster: &str, nodes: Vec<ServerConfig>, domain: u32) {
        let names: Vec<String> = nodes.iter().map(|c| c.name.clone()).collect();
        for cfg in nodes {
            let mut s = Server::new(cfg);
            s.galera = Some(cluster.to_owned());
            self.servers.insert(s.cfg.name.clone(), s);
        }
        self.galeras.insert(
            cluster.to_owned(),
            Galera {
                nodes: names,
                domain,
                seqno: 0,
                gtid_seq: 0,
                cert: BTreeMap::new(),
            },
        );
    }

    /// The model's own log of notable transitions (for traces).
    pub fn take_log(&mut self) -> Vec<String> {
        std::mem::take(&mut self.log)
    }

    fn note(&mut self, s: String) {
        self.log.push(s);
    }

    fn schedule(&mut self, at: Duration, ev: Event) {
        self.next_event += 1;
        self.events.insert((at.max(self.now), self.next_event), ev);
    }

    fn token(&mut self) -> u64 {
        self.next_token += 1;
        self.next_token
    }

    fn reach(&self, a: &str, b: &str) -> bool {
        a == b || (self.reach)(a, b)
    }

    fn delay(&self, a: &str, b: &str) -> Duration {
        (self.delay)(a, b)
    }

    /// When the next timed event is due, if any.
    #[must_use]
    pub fn next_event_time(&self) -> Option<Duration> {
        self.events.keys().next().map(|(t, _)| *t)
    }

    /// Runs every event due by `now`.
    ///
    /// # Panics
    /// Only if the model's own state is inconsistent (a bug).
    pub fn advance(&mut self, now: Duration) {
        while let Some((&(t, n), _)) = self.events.iter().next() {
            if t > now {
                break;
            }
            let ev = self.events.remove(&(t, n)).expect("present");
            self.now = self.now.max(t);
            self.handle(ev);
        }
        self.now = self.now.max(now);
    }

    /// Connections whose blocked statements finished since the last call.
    pub fn take_completed(&mut self) -> Vec<ConnId> {
        std::mem::take(&mut self.completed)
    }

    /// The outcome of `conn`'s blocked statement, once it finished.
    pub fn take_done(&mut self, conn: ConnId) -> Option<Outcome> {
        self.conns.get_mut(&conn)?.done.take()
    }

    /// Whether `conn` has a statement waiting.
    #[must_use]
    pub fn is_blocked(&self, conn: ConnId) -> bool {
        self.conns.get(&conn).is_some_and(|c| c.pending.is_some())
    }

    fn finish(&mut self, conn: ConnId, o: Outcome) {
        if let Some(c) = self.conns.get_mut(&conn) {
            c.pending = None;
            c.done = Some(o);
            self.completed.push(conn);
        }
        if self.orphans.remove(&conn) {
            // Its client is gone: the server reads EOF next and ends the
            // connection.
            self.drop_conn(conn, None);
        }
    }

    // ------------------------------------------------------------------
    // Connections.

    /// Opens a connection to `server` as `user` (`None` for root, which
    /// has every privilege), working in `schema`.
    ///
    /// # Errors
    /// `2013` if the server is down, `1045` for an unknown user.
    pub fn connect(
        &mut self,
        server: &str,
        user: Option<&str>,
        schema: &str,
        now: Duration,
    ) -> Result<ConnId, u16> {
        self.advance(now);
        let s = self.servers.get(server).ok_or(errno::SERVER_LOST)?;
        if !s.up {
            return Err(errno::SERVER_LOST);
        }
        if let Some(u) = user
            && !s.users.contains_key(u)
        {
            return Err(more_errno::ACCESS_DENIED);
        }
        let lock_wait = s.cfg.lock_wait_timeout;
        self.next_conn += 1;
        let id = ConnId(self.next_conn);
        self.conns.insert(
            id,
            Conn {
                server: server.to_owned(),
                user: user.map(str::to_owned),
                schema: schema.to_owned(),
                current_db: false,
                db_access: false,
                txn: None,
                iso: Isolation::RepeatableRead,
                lock_wait,
                pending: None,
                done: None,
                alive: true,
                last_gtid: None,
                poisoned: None,
            },
        );
        Ok(id)
    }

    /// Closes a connection: its transaction rolls back, a prepared XA
    /// branch stays (detached).
    pub fn disconnect(&mut self, conn: ConnId, now: Duration) {
        self.advance(now);
        if self.orphans_run
            && let Some(c) = self.conns.get_mut(&conn)
            && c.alive
            && c.pending
                .as_ref()
                .is_some_and(|p| matches!(p.kind, PendingKind::Lock(_)))
        {
            self.orphans.insert(conn);
            self.note(format!(
                "{conn} disconnected while waiting for a lock: the statement runs on"
            ));
            return;
        }
        self.drop_conn(conn, None);
    }

    fn drop_conn(&mut self, conn: ConnId, pending_err: Option<u16>) {
        self.orphans.remove(&conn);
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        if !c.alive {
            return;
        }
        c.alive = false;
        let had_pending = c.pending.take().is_some();
        let txn = c.txn.take();
        let server = c.server.clone();
        if had_pending && let Some(e) = pending_err {
            c.done = Some(Outcome::Err(e));
            self.completed.push(conn);
        }
        if let Some(t) = txn {
            let s = self.servers.get_mut(&server).expect("server");
            let keep = s.txns.get(&t).is_some_and(|x| {
                x.awaiting_ack.is_some() || matches!(x.xa, Some((_, XaState::Prepared)))
            });
            if keep {
                if let Some(x) = s.txns.get_mut(&t) {
                    x.conn = None;
                }
            } else {
                self.rollback_txn(&server, t);
            }
        }
    }

    // ------------------------------------------------------------------
    // Statements.

    /// Runs `stmt` on `conn` at `now`.
    pub fn exec(&mut self, conn: ConnId, stmt: &Stmt, now: Duration) -> Step {
        self.advance(now);
        let Some(c) = self.conns.get_mut(&conn) else {
            return err(errno::SERVER_LOST);
        };
        if !c.alive {
            return err(errno::SERVER_LOST);
        }
        if c.pending.is_some() {
            return err(more_errno::COMMANDS_OUT_OF_SYNC);
        }
        c.done = None;
        let server = c.server.clone();
        if !self.servers[&server].up {
            self.drop_conn(conn, None);
            return err(errno::SERVER_LOST);
        }
        if let Some(e) = self.conns.get_mut(&conn).and_then(|c| c.poisoned.take()) {
            return err(e);
        }
        if !self.galera_primary(&server) && !matches!(stmt, Stmt::LastGtid) {
            return err(errno::WSREP_NOT_READY);
        }
        let order = (self.now, self.token());
        self.run(conn, stmt, order)
    }

    fn galera_primary(&self, server: &str) -> bool {
        let Some(cluster) = &self.servers[server].galera else {
            return true;
        };
        let g = &self.galeras[cluster];
        let n = g
            .nodes
            .iter()
            .filter(|o| self.servers[o.as_str()].up && self.reach(server, o))
            .count();
        n * 2 > g.nodes.len()
    }

    fn conn(&self, conn: ConnId) -> &Conn {
        &self.conns[&conn]
    }

    fn txn_of(&self, conn: ConnId) -> Option<&Txn> {
        let c = self.conn(conn);
        self.servers[&c.server].txns.get(&c.txn?)
    }

    fn may_access(&self, conn: ConnId) -> bool {
        let c = self.conn(conn);
        let Some(u) = &c.user else { return true };
        if c.current_db {
            return c.db_access;
        }
        self.servers[&c.server]
            .users
            .get(u)
            .is_some_and(|g| g.contains(&c.schema))
    }

    fn table(&self, conn: ConnId, t: &str) -> Result<String, u16> {
        let c = self.conn(conn);
        let q = qualified(&c.schema, t);
        if !self.may_access(conn) {
            return Err(errno::TABLE_ACCESS_DENIED);
        }
        if !self.servers[&c.server].tables.contains_key(&q) {
            return Err(errno::NO_SUCH_TABLE);
        }
        Ok(q)
    }

    /// The connection's transaction, opening one: explicit if `BEGIN` ran,
    /// else implicit (autocommit). Returns (txn, implicit).
    fn ensure_txn(&mut self, conn: ConnId) -> (TxnId, bool) {
        if let Some(t) = self.conn(conn).txn {
            let implicit = self.txn_of(conn).is_some_and(|t| t.implicit);
            return (t, implicit);
        }
        let id = self.new_txn(conn, true);
        (id, true)
    }

    fn new_txn(&mut self, conn: ConnId, implicit: bool) -> TxnId {
        self.next_txn += 1;
        let id = self.next_txn;
        let c = self.conns.get_mut(&conn).expect("conn");
        c.txn = Some(id);
        let iso = c.iso;
        let server = c.server.clone();
        self.servers.get_mut(&server).expect("server").txns.insert(
            id,
            Txn {
                conn: Some(conn),
                implicit,
                iso,
                view: None,
                writes: BTreeMap::new(),
                locks: BTreeSet::new(),
                xa: None,
                galera_base: None,
                awaiting_ack: None,
                gtid: None,
            },
        );
        id
    }

    fn xa_state(&self, conn: ConnId) -> Option<(String, XaState)> {
        self.txn_of(conn).and_then(|t| t.xa.clone())
    }

    /// Runs (or reruns, after a wait) a statement. `order` keeps a waiter's
    /// place in lock queues.
    #[allow(clippy::too_many_lines)] // one arm per statement
    fn run(&mut self, conn: ConnId, stmt: &Stmt, order: (Duration, u64)) -> Step {
        let server = self.conn(conn).server.clone();
        let xa = self.xa_state(conn);
        // A prepared or idle XA branch accepts only XA statements and
        // reads of variables (`@@last_gtid`, `@@gtid_binlog_pos`).
        if let Some((_, st)) = &xa
            && *st != XaState::Active
            && !matches!(
                stmt,
                Stmt::XaPrepare(_)
                    | Stmt::XaCommit(_)
                    | Stmt::XaCommitOnePhase(_)
                    | Stmt::XaRollback(_)
                    | Stmt::XaRecover
                    | Stmt::LastGtid
                    | Stmt::GtidBinlogPos
            )
        {
            return err(errno::XAER_RMFAIL);
        }
        match stmt {
            Stmt::CreateTable(t) => self.ddl(conn, Some(t), None),
            Stmt::Begin | Stmt::BeginSnapshot => {
                if xa.is_some() {
                    return err(errno::XAER_RMFAIL);
                }
                if self.conn(conn).txn.is_some() {
                    // BEGIN commits the open transaction first.
                    if let Step::Blocked = self.commit(conn, order) {
                        return Step::Blocked;
                    }
                }
                let t = self.new_txn(conn, false);
                if *stmt == Stmt::BeginSnapshot {
                    let s = self.servers.get_mut(&server).expect("server");
                    let seq = s.commit_seq;
                    s.txns.get_mut(&t).expect("txn").view = Some(seq);
                }
                ok()
            }
            Stmt::Commit => {
                if xa.is_some() {
                    return err(errno::XAER_RMFAIL);
                }
                self.commit(conn, order)
            }
            Stmt::Rollback => {
                if xa.is_some() {
                    return err(errno::XAER_RMFAIL);
                }
                if let Some(t) = self.conns.get_mut(&conn).and_then(|c| c.txn.take()) {
                    self.rollback_txn(&server, t);
                }
                ok()
            }
            Stmt::SetIsolation(i) => {
                self.conns.get_mut(&conn).expect("conn").iso = *i;
                ok()
            }
            Stmt::SetLockWaitTimeout(s) => {
                self.conns.get_mut(&conn).expect("conn").lock_wait =
                    Duration::from_secs(u64::from(*s));
                ok()
            }
            Stmt::Get { t, k } => self.read(conn, t, Some(*k)),
            Stmt::Scan(t) => self.read(conn, t, None),
            Stmt::GetForUpdate { t, k } | Stmt::Delete { t, k } => {
                self.write(conn, stmt, t, *k, None, order)
            }
            Stmt::Insert { t, k, v } | Stmt::Update { t, k, v } => {
                self.write(conn, stmt, t, *k, Some(v.clone()), order)
            }
            Stmt::XaStart(x) => self.xa_start(conn, x),
            Stmt::XaEnd(x) => self.xa_end(conn, x),
            Stmt::XaPrepare(x) => self.xa_prepare(conn, x, order),
            Stmt::XaCommit(x) => self.xa_finish(conn, x, true),
            Stmt::XaCommitOnePhase(x) => self.xa_one_phase(conn, x, order),
            Stmt::XaRollback(x) => self.xa_finish(conn, x, false),
            Stmt::XaRecover => {
                let rows = self.servers[&server]
                    .prepared
                    .keys()
                    .map(|x| vec![Some(x.clone())])
                    .collect();
                Step::Done(Outcome::rows(rows))
            }
            Stmt::LastGtid => {
                let g = self
                    .conn(conn)
                    .last_gtid
                    .map_or(String::new(), |g| g.to_string());
                Step::Done(Outcome::rows(vec![vec![Some(g)]]))
            }
            Stmt::GtidBinlogPos => {
                let pos = self.servers[&server]
                    .binlog_state
                    .values()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                Step::Done(Outcome::rows(vec![vec![Some(pos)]]))
            }
            Stmt::MasterGtidWait { gtid, timeout } => {
                self.gtid_wait(conn, *gtid, *timeout, stmt, order)
            }
            Stmt::KillQuery(target) => self.kill(*target, false),
            Stmt::KillConnection(target) => self.kill(*target, true),
            Stmt::SetReadOnly(on) => {
                self.servers.get_mut(&server).expect("server").read_only = *on;
                ok()
            }
            Stmt::CreateUser(u) => {
                let s = self.servers.get_mut(&server).expect("server");
                if s.users.contains_key(u) {
                    return err(1396);
                }
                s.users.insert(u.clone(), BTreeSet::new());
                ok()
            }
            Stmt::GrantAll(u) => self.ddl(conn, None, Some((u.clone(), true))),
            Stmt::RevokeAll(u) => self.ddl(conn, None, Some((u.clone(), false))),
            Stmt::Use => {
                let schema = self.conn(conn).schema.clone();
                let c = self.conn(conn);
                let access = match &c.user {
                    None => true,
                    Some(u) => self.servers[&server]
                        .users
                        .get(u)
                        .is_some_and(|g| g.contains(&schema)),
                };
                if !access {
                    return err(more_errno::DB_ACCESS_DENIED);
                }
                let c = self.conns.get_mut(&conn).expect("conn");
                c.current_db = true;
                c.db_access = true;
                ok()
            }
        }
    }

    /// DDL and grants: commit the open transaction, write a binlog group.
    fn ddl(
        &mut self,
        conn: ConnId,
        create: Option<&String>,
        grant: Option<(String, bool)>,
    ) -> Step {
        let server = self.conn(conn).server.clone();
        let schema = self.conn(conn).schema.clone();
        if self.conn(conn).user.is_some() && grant.is_some() {
            return err(more_errno::DB_ACCESS_DENIED);
        }
        if self.conn(conn).txn.is_some() {
            let order = (self.now, self.token());
            if let Step::Blocked = self.commit(conn, order) {
                return Step::Blocked;
            }
        }
        let mut creates = Vec::new();
        if let Some(t) = create {
            let q = qualified(&schema, t);
            if self.servers[&server].tables.contains_key(&q) {
                return err(more_errno::TABLE_EXISTS);
            }
            creates.push(q);
        }
        let grant = grant.map(|(u, on)| (u, schema.clone(), on));
        if let Some((u, _, _)) = &grant
            && !self.servers[&server].users.contains_key(u)
        {
            return err(1133);
        }
        let group = Group {
            gtid: Gtid {
                domain: 0,
                server: 0,
                seq: 0,
            },
            kind: GroupKind::Ddl,
            writes: Vec::new(),
            creates,
            grant,
        };
        self.apply_ddl(&server, &group);
        let gtid = self.binlog(&server, group);
        self.conns.get_mut(&conn).expect("conn").last_gtid = Some(gtid);
        ok()
    }

    fn apply_ddl(&mut self, server: &str, g: &Group) {
        let s = self.servers.get_mut(server).expect("server");
        for t in &g.creates {
            s.tables.entry(t.clone()).or_default();
        }
        if let Some((u, schema, on)) = &g.grant
            && let Some(set) = s.users.get_mut(u)
        {
            if *on {
                set.insert(schema.clone());
            } else {
                set.remove(schema);
            }
        }
    }

    /// Assigns a GTID, appends the group to `server`'s binlog, and ships it
    /// (replicas poll; Galera nodes apply).
    fn binlog(&mut self, server: &str, mut g: Group) -> Gtid {
        let s = &self.servers[server];
        let (domain, sid) = (s.cfg.domain_id, s.cfg.server_id);
        if let Some(cluster) = s.galera.clone() {
            let gal = self.galeras.get_mut(&cluster).expect("galera");
            gal.gtid_seq += 1;
            gal.seqno += 1;
            g.gtid = Gtid {
                domain: gal.domain,
                server: sid,
                seq: gal.gtid_seq,
            };
            for (row, _) in &g.writes {
                gal.cert.insert(row.clone(), (gal.seqno, server.to_owned()));
            }
            let seqno = gal.seqno;
            let others: Vec<String> = gal.nodes.iter().filter(|n| *n != server).cloned().collect();
            self.servers.get_mut(server).expect("server").galera_applied = seqno;
            for n in others {
                let at = self.now + self.delay(server, &n);
                self.schedule(
                    at,
                    Event::GaleraApply {
                        node: n,
                        seqno,
                        group: g.clone(),
                    },
                );
            }
        } else {
            g.gtid = self
                .servers
                .get_mut(server)
                .expect("server")
                .next_gtid(domain, sid);
        }
        let gtid = g.gtid;
        self.servers.get_mut(server).expect("server").log(g);
        self.note(format!("{server} binlog {gtid}"));
        self.poll_replicas_of(server);
        gtid
    }

    fn poll_replicas_of(&mut self, source: &str) {
        let replicas: Vec<String> = self
            .servers
            .iter()
            .filter(|(_, s)| {
                s.replica
                    .as_ref()
                    .is_some_and(|r| r.source == source && r.io_running)
            })
            .map(|(n, _)| n.clone())
            .collect();
        for r in replicas {
            self.schedule(self.now, Event::IoPoll { replica: r });
        }
    }

    fn read(&mut self, conn: ConnId, t: &str, key: Option<i64>) -> Step {
        let table = match self.table(conn, t) {
            Ok(q) => q,
            Err(e) => return err(e),
        };
        let server = self.conn(conn).server.clone();
        let (txn, implicit) = self.ensure_txn(conn);
        let s = self.servers.get_mut(&server).expect("server");
        let commit_seq = s.commit_seq;
        let tx = s.txns.get_mut(&txn).expect("txn");
        let view = match tx.iso {
            Isolation::RepeatableRead => *tx.view.get_or_insert(commit_seq),
            Isolation::ReadCommitted => commit_seq,
        };
        let tx = tx.clone();
        let s = &self.servers[&server];
        let value = |k: i64| -> Option<String> {
            let row = (table.clone(), k);
            match tx.writes.get(&row) {
                Some(v) => v.clone(),
                None => s.visible(&row, view),
            }
        };
        let rows: Vec<Row> = if let Some(k) = key {
            value(k)
                .map(|v| vec![Some(k.to_string()), Some(v)])
                .into_iter()
                .collect()
        } else {
            {
                let mut keys: BTreeSet<i64> = s
                    .tables
                    .get(&table)
                    .map(|m| m.keys().copied().collect())
                    .unwrap_or_default();
                keys.extend(
                    tx.writes
                        .keys()
                        .filter(|(t, _)| *t == table)
                        .map(|(_, k)| *k),
                );
                keys.into_iter()
                    .filter_map(|k| value(k).map(|v| vec![Some(k.to_string()), Some(v)]))
                    .collect()
            }
        };
        if implicit {
            self.end_implicit(conn, true);
        }
        Step::Done(Outcome::rows(rows))
    }

    /// Ends an autocommit statement's transaction: commit (reads only, so
    /// never blocks) or roll back.
    fn end_implicit(&mut self, conn: ConnId, commit: bool) {
        let server = self.conn(conn).server.clone();
        if let Some(t) = self.conns.get_mut(&conn).and_then(|c| c.txn.take()) {
            if commit {
                let s = self.servers.get_mut(&server).expect("server");
                if let Some(tx) = s.txns.remove(&t) {
                    for r in &tx.locks {
                        s.locks.remove(r);
                    }
                }
                self.wake(&server);
            } else {
                self.rollback_txn(&server, t);
            }
        }
    }

    /// A locking read or a write of one key.
    fn write(
        &mut self,
        conn: ConnId,
        stmt: &Stmt,
        t: &str,
        k: i64,
        value: Option<String>,
        order: (Duration, u64),
    ) -> Step {
        let table = match self.table(conn, t) {
            Ok(q) => q,
            Err(e) => return err(e),
        };
        let server = self.conn(conn).server.clone();
        let is_write = !matches!(stmt, Stmt::GetForUpdate { .. });
        if is_write && self.servers[&server].read_only && self.conn(conn).user.is_some() {
            return err(errno::READ_ONLY);
        }
        let (txn, implicit) = self.ensure_txn(conn);
        let row = (table, k);
        // Acquire the row lock.
        match self.lock(conn, txn, &row, stmt, order, implicit) {
            Ok(()) => {}
            Err(step) => return step,
        }
        let s = self.servers.get_mut(&server).expect("server");
        let snapshot_iso = s.cfg.snapshot_isolation;
        let latest_seq = s.latest(&row).map(|v| v.seq);
        let latest_val = s.latest(&row).and_then(|v| v.value.clone());
        let galera_applied = s.galera_applied;
        let is_galera = s.galera.is_some();
        let tx = s.txns.get_mut(&txn).expect("txn");
        if is_galera && tx.galera_base.is_none() {
            tx.galera_base = Some(galera_applied);
        }
        // innodb_snapshot_isolation: locking a row changed after the read
        // view was taken fails.
        let own = tx.writes.get(&row).cloned();
        if snapshot_iso
            && own.is_none()
            && tx.iso == Isolation::RepeatableRead
            && let (Some(view), Some(seq)) = (tx.view, latest_seq)
            && seq > view
        {
            if CHECKREAD_ROLLS_BACK {
                self.conns.get_mut(&conn).expect("conn").txn = None;
                self.rollback_txn(&server, txn);
            } else if implicit {
                self.end_implicit(conn, false);
            }
            return err(errno::CHECKREAD);
        }
        let current = match own {
            Some(v) => v,
            None => latest_val,
        };
        let result = match stmt {
            Stmt::GetForUpdate { .. } => Outcome::rows(
                current
                    .map(|v| vec![Some(k.to_string()), Some(v)])
                    .into_iter()
                    .collect(),
            ),
            Stmt::Insert { .. } => {
                if current.is_some() {
                    if implicit {
                        self.end_implicit(conn, false);
                    }
                    return err(errno::DUP_ENTRY);
                }
                tx.writes.insert(row, value);
                Outcome::affected(1)
            }
            Stmt::Update { .. } => match current {
                None => Outcome::affected(0),
                Some(old) if Some(&old) == value.as_ref() => Outcome::affected(0),
                Some(_) => {
                    tx.writes.insert(row, value);
                    Outcome::affected(1)
                }
            },
            Stmt::Delete { .. } => {
                if current.is_none() {
                    Outcome::affected(0)
                } else {
                    tx.writes.insert(row, None);
                    Outcome::affected(1)
                }
            }
            _ => Outcome::ok(),
        };
        if implicit {
            // Autocommit: the statement's transaction commits now.
            return match self.commit(conn, order) {
                Step::Done(Outcome::Err(e)) => err(e),
                Step::Done(_) => Step::Done(result),
                Step::Blocked => {
                    // The commit waits for an ack; it reports `result`
                    // when it finishes (DML returns its count).
                    if let Some(p) = self.conns.get_mut(&conn).and_then(|c| c.pending.as_mut()) {
                        p.stmt = stmt.clone();
                    }
                    Step::Blocked
                }
            };
        }
        Step::Done(result)
    }

    /// Takes `row`'s lock for `txn`, or blocks `conn` (detecting
    /// deadlocks). `Err` carries the step to return.
    fn lock(
        &mut self,
        conn: ConnId,
        txn: TxnId,
        row: &RowRef,
        stmt: &Stmt,
        order: (Duration, u64),
        implicit: bool,
    ) -> Result<(), Step> {
        let server = self.conn(conn).server.clone();
        loop {
            let s = self.servers.get_mut(&server).expect("server");
            match s.locks.get(row) {
                None => {
                    s.locks.insert(row.clone(), txn);
                    s.txns.get_mut(&txn).expect("txn").locks.insert(row.clone());
                    return Ok(());
                }
                Some(h) if *h == txn => return Ok(()),
                Some(_) => {}
            }
            // Wait: record it, then look for a cycle.
            let token = self.token();
            let lock_wait = self.conn(conn).lock_wait;
            let c = self.conns.get_mut(&conn).expect("conn");
            c.pending = Some(Pending {
                stmt: stmt.clone(),
                kind: PendingKind::Lock(row.clone()),
                token,
                order,
                implicit,
            });
            match self.deadlock_victim(&server, txn) {
                None => {
                    let at = order.0 + lock_wait;
                    self.schedule(at, Event::LockTimeout { conn, token });
                    return Err(Step::Blocked);
                }
                Some(v) if v == txn => {
                    self.conns.get_mut(&conn).expect("conn").pending = None;
                    self.note(format!("{server} deadlock: {conn} chosen as victim"));
                    self.conns.get_mut(&conn).expect("conn").txn = None;
                    self.rollback_txn(&server, txn);
                    return Err(err(errno::DEADLOCK));
                }
                Some(v) => {
                    self.conns.get_mut(&conn).expect("conn").pending = None;
                    let vconn = self.servers[&server].txns[&v].conn;
                    self.note(format!("{server} deadlock: {vconn:?} chosen as victim"));
                    if let Some(vc) = vconn {
                        self.conns.get_mut(&vc).expect("conn").txn = None;
                        self.finish(vc, Outcome::Err(errno::DEADLOCK));
                    }
                    self.rollback_txn(&server, v);
                    // Try again: the victim's locks are gone.
                }
            }
        }
    }

    /// The transaction to roll back if `txn`'s wait closes a cycle: the
    /// lightest (fewest rows written and locked); the waiter on a tie.
    fn deadlock_victim(&self, server: &str, txn: TxnId) -> Option<TxnId> {
        let s = &self.servers[server];
        let waits_for = |t: TxnId| -> Option<TxnId> {
            let c = s.txns.get(&t)?.conn?;
            match &self.conns.get(&c)?.pending.as_ref()?.kind {
                PendingKind::Lock(r) => s.locks.get(r).copied(),
                _ => None,
            }
        };
        let mut cycle = vec![txn];
        let mut cur = waits_for(txn)?;
        while cur != txn {
            if cycle.contains(&cur) || cycle.len() > s.txns.len() {
                return None; // a cycle not through `txn`
            }
            cycle.push(cur);
            cur = waits_for(cur)?;
        }
        let weight = |t: &TxnId| s.txns.get(t).map_or(0, Txn::weight);
        let min = cycle.iter().map(weight).min()?;
        if weight(&txn) == min {
            Some(txn)
        } else {
            cycle.into_iter().find(|t| weight(t) == min)
        }
    }

    fn rollback_txn(&mut self, server: &str, txn: TxnId) {
        let s = self.servers.get_mut(server).expect("server");
        if let Some(tx) = s.txns.remove(&txn) {
            for r in &tx.locks {
                if s.locks.get(r) == Some(&txn) {
                    s.locks.remove(r);
                }
            }
            if let Some((x, _)) = &tx.xa {
                s.prepared.remove(x);
            }
        }
        self.wake(server);
    }

    /// Reruns lock waits on `server` in queue order after locks were freed.
    fn wake(&mut self, server: &str) {
        let mut waiting: Vec<((Duration, u64), ConnId)> = self
            .conns
            .iter()
            .filter(|(_, c)| c.alive && c.server == server)
            .filter_map(|(id, c)| match &c.pending {
                Some(p) if matches!(p.kind, PendingKind::Lock(_)) => Some((p.order, *id)),
                _ => None,
            })
            .collect();
        waiting.sort();
        for (_, id) in waiting {
            let Some(p) = self.conns.get(&id).and_then(|c| c.pending.clone()) else {
                continue;
            };
            let PendingKind::Lock(row) = &p.kind else {
                continue;
            };
            let free = !self.servers[server].locks.contains_key(row);
            if !free {
                continue;
            }
            self.conns.get_mut(&id).expect("conn").pending = None;
            match self.run(id, &p.stmt, p.order) {
                Step::Done(o) => self.finish(id, o),
                Step::Blocked => {
                    // Blocked again (another lock or an ack): keep waiting.
                }
            }
        }
    }

    fn commit(&mut self, conn: ConnId, order: (Duration, u64)) -> Step {
        let server = self.conn(conn).server.clone();
        let Some(txn) = self.conns.get_mut(&conn).and_then(|c| c.txn.take()) else {
            return ok();
        };
        let tx = self.servers[&server].txns[&txn].clone();
        if tx.writes.is_empty() {
            self.rollback_txn(&server, txn);
            return ok();
        }
        // Galera certification: first committer wins against writesets
        // from other nodes. Two transactions on one node are already
        // ordered by InnoDB's row locks, so a writeset this node committed
        // never fails a later local one (measured on the dev Galera, 11.8,
        // `innodb_snapshot_isolation` on: a transaction that wrote first,
        // then updated a row another local transaction committed meanwhile,
        // commits).
        if let Some(cluster) = self.servers[&server].galera.clone() {
            let g = &self.galeras[&cluster];
            let base = tx.galera_base.unwrap_or(0);
            if tx.writes.keys().any(|r| {
                g.cert
                    .get(r)
                    .is_some_and(|(s, origin)| *s > base && *origin != server)
            }) {
                self.note(format!("{server} certification failed for {conn}"));
                self.rollback_txn(&server, txn);
                return err(errno::DEADLOCK);
            }
        }
        let writes: Vec<(RowRef, Option<String>)> = tx.writes.clone().into_iter().collect();
        let gtid = self.binlog(
            &server,
            Group {
                gtid: Gtid {
                    domain: 0,
                    server: 0,
                    seq: 0,
                },
                kind: GroupKind::Commit,
                writes,
                creates: Vec::new(),
                grant: None,
            },
        );
        self.conns.get_mut(&conn).expect("conn").last_gtid = Some(gtid);
        if self.wait_ack(conn, txn, gtid, AfterAck::Install, order) {
            return Step::Blocked;
        }
        self.install_txn(&server, txn);
        ok()
    }

    /// Semi-sync: makes `conn` wait for an ack of `gtid` if the server
    /// waits for one. Returns whether it waits.
    fn wait_ack(
        &mut self,
        conn: ConnId,
        txn: TxnId,
        gtid: Gtid,
        after: AfterAck,
        order: (Duration, u64),
    ) -> bool {
        let server = self.conn(conn).server.clone();
        let s = &self.servers[&server];
        if !(s.cfg.semisync_master && s.semisync_on) {
            return false;
        }
        let timeout = s.cfg.semisync_timeout;
        let token = self.token();
        let s = self.servers.get_mut(&server).expect("server");
        let tx = s.txns.get_mut(&txn).expect("txn");
        tx.awaiting_ack = Some((gtid, after));
        tx.gtid = Some(gtid);
        let implicit = tx.implicit;
        self.conns.get_mut(&conn).expect("conn").pending = Some(Pending {
            stmt: Stmt::Commit,
            kind: PendingKind::Ack,
            token,
            order,
            implicit,
        });
        self.schedule(self.now + timeout, Event::AckTimeout { server, gtid });
        true
    }

    fn install_txn(&mut self, server: &str, txn: TxnId) {
        let s = self.servers.get_mut(server).expect("server");
        let Some(tx) = s.txns.get(&txn).cloned() else {
            return;
        };
        let writes: Vec<(RowRef, Option<String>)> = tx.writes.into_iter().collect();
        s.install(&writes);
        self.rollback_txn(server, txn); // frees locks; the writes are in
    }

    /// Ends the semi-sync wait of every transaction on `server` whose GTID
    /// is at or below `upto` in its domain (all of them when `None`).
    fn release_acks(&mut self, server: &str, upto: Option<Gtid>) {
        let waiting: Vec<(TxnId, Gtid, AfterAck, Option<ConnId>)> = self.servers[server]
            .txns
            .iter()
            .filter_map(|(id, t)| t.awaiting_ack.map(|(g, a)| (*id, g, a, t.conn)))
            .filter(|(_, g, _, _)| upto.is_none_or(|u| u.domain == g.domain && g.seq <= u.seq))
            .collect();
        for (txn, _g, after, conn) in waiting {
            let s = self.servers.get_mut(server).expect("server");
            s.txns.get_mut(&txn).expect("txn").awaiting_ack = None;
            let result = conn
                .and_then(|c| self.conns.get(&c))
                .and_then(|c| c.pending.as_ref().map(|p| p.stmt.clone()));
            match after {
                AfterAck::Install => self.install_txn(server, txn),
                AfterAck::Prepared => {}
            }
            if let Some(c) = conn {
                let o = match result {
                    Some(Stmt::Insert { .. } | Stmt::Update { .. } | Stmt::Delete { .. }) => {
                        Outcome::affected(1)
                    }
                    _ => Outcome::ok(),
                };
                self.finish(c, o);
            }
        }
    }

    // ------------------------------------------------------------------
    // XA.

    fn xa_start(&mut self, conn: ConnId, x: &str) -> Step {
        let server = self.conn(conn).server.clone();
        if self.servers[&server].galera.is_some() {
            // Galera has no XA (spike-xa: error 1235).
            return err(more_errno::NOT_SUPPORTED_YET);
        }
        if let Some(t) = self.txn_of(conn) {
            return err(if t.xa.is_some() {
                errno::XAER_RMFAIL
            } else {
                errno::XAER_OUTSIDE
            });
        }
        if self.servers[&server].prepared.contains_key(x) {
            return err(more_errno::XAER_DUPID);
        }
        let t = self.new_txn(conn, false);
        self.servers
            .get_mut(&server)
            .expect("server")
            .txns
            .get_mut(&t)
            .expect("txn")
            .xa = Some((x.to_owned(), XaState::Active));
        ok()
    }

    fn set_xa(&mut self, conn: ConnId, st: XaState) {
        let server = self.conn(conn).server.clone();
        let t = self.conn(conn).txn.expect("txn");
        if let Some(x) = &mut self
            .servers
            .get_mut(&server)
            .expect("server")
            .txns
            .get_mut(&t)
            .expect("txn")
            .xa
        {
            x.1 = st;
        }
    }

    fn xa_end(&mut self, conn: ConnId, x: &str) -> Step {
        match self.xa_state(conn) {
            Some((id, XaState::Active)) if id == x => {
                self.set_xa(conn, XaState::Idle);
                ok()
            }
            Some((id, _)) if id == x => err(errno::XAER_RMFAIL),
            Some(_) | None => err(errno::XAER_NOTA),
        }
    }

    fn xa_prepare(&mut self, conn: ConnId, x: &str, order: (Duration, u64)) -> Step {
        match self.xa_state(conn) {
            Some((id, XaState::Idle)) if id == x => {}
            Some((id, _)) if id == x => return err(errno::XAER_RMFAIL),
            _ => return err(errno::XAER_NOTA),
        }
        let server = self.conn(conn).server.clone();
        let txn = self.conn(conn).txn.expect("txn");
        self.set_xa(conn, XaState::Prepared);
        self.servers
            .get_mut(&server)
            .expect("server")
            .prepared
            .insert(x.to_owned(), txn);
        if self.servers[&server].cfg.binlog_in_innodb {
            // Not binlogged: no GTID (`@@last_gtid` keeps its old value),
            // nothing shipped, nothing to acknowledge.
            return ok();
        }
        let writes: Vec<(RowRef, Option<String>)> = self.servers[&server].txns[&txn]
            .writes
            .clone()
            .into_iter()
            .collect();
        let gtid = self.binlog(
            &server,
            Group {
                gtid: Gtid {
                    domain: 0,
                    server: 0,
                    seq: 0,
                },
                kind: GroupKind::XaPrepare(x.to_owned()),
                writes,
                creates: Vec::new(),
                grant: None,
            },
        );
        self.conns.get_mut(&conn).expect("conn").last_gtid = Some(gtid);
        if self.wait_ack(conn, txn, gtid, AfterAck::Prepared, order) {
            return Step::Blocked;
        }
        ok()
    }

    fn xa_one_phase(&mut self, conn: ConnId, x: &str, order: (Duration, u64)) -> Step {
        match self.xa_state(conn) {
            Some((id, XaState::Idle)) if id == x => {}
            Some((id, _)) if id == x => return err(errno::XAER_RMFAIL),
            _ => return err(errno::XAER_NOTA),
        }
        let server = self.conn(conn).server.clone();
        let txn = self.conn(conn).txn.expect("txn");
        self.servers
            .get_mut(&server)
            .expect("server")
            .txns
            .get_mut(&txn)
            .expect("txn")
            .xa = None;
        self.commit(conn, order)
    }

    /// `XA COMMIT` (of a prepared branch) or `XA ROLLBACK`.
    fn xa_finish(&mut self, conn: ConnId, x: &str, commit: bool) -> Step {
        let server = self.conn(conn).server.clone();
        let own = self.xa_state(conn);
        let txn = match &own {
            Some((id, st)) if id == x => {
                if commit && *st != XaState::Prepared {
                    return err(errno::XAER_RMFAIL);
                }
                if !commit && *st == XaState::Active {
                    return err(errno::XAER_RMFAIL);
                }
                self.conn(conn).txn.expect("txn")
            }
            Some(_) => return err(errno::XAER_RMFAIL),
            None => {
                if self.conn(conn).txn.is_some() {
                    return err(errno::XAER_OUTSIDE);
                }
                // A prepared branch: only once detached from its session.
                let s = &self.servers[&server];
                match s.prepared.get(x) {
                    Some(t) if s.txns.get(t).is_some_and(|t| t.conn.is_none()) => *t,
                    _ => return err(errno::XAER_NOTA),
                }
            }
        };
        let prepared = matches!(
            self.servers[&server].txns[&txn].xa,
            Some((_, XaState::Prepared))
        );
        if let Some(c) = self.servers[&server].txns[&txn].conn {
            self.conns.get_mut(&c).expect("conn").txn = None;
        }
        let innodb_binlog = self.servers[&server].cfg.binlog_in_innodb;
        // With the binlog in InnoDB the prepare wasn't logged: a commit logs
        // the transaction as an ordinary group, a rollback nothing.
        if prepared && (commit || !innodb_binlog) {
            let kind = match (commit, innodb_binlog) {
                (true, true) => GroupKind::Commit,
                (true, false) => GroupKind::XaCommit(x.to_owned()),
                (false, _) => GroupKind::XaRollback(x.to_owned()),
            };
            let writes = if commit {
                self.servers[&server].txns[&txn]
                    .writes
                    .clone()
                    .into_iter()
                    .collect()
            } else {
                Vec::new()
            };
            let gtid = self.binlog(
                &server,
                Group {
                    gtid: Gtid {
                        domain: 0,
                        server: 0,
                        seq: 0,
                    },
                    kind,
                    writes,
                    creates: Vec::new(),
                    grant: None,
                },
            );
            self.conns.get_mut(&conn).expect("conn").last_gtid = Some(gtid);
        }
        if commit {
            self.install_txn(&server, txn);
        } else {
            self.rollback_txn(&server, txn);
        }
        ok()
    }

    // ------------------------------------------------------------------
    // KILL, GTID waits.

    fn kill(&mut self, target: ConnId, connection: bool) -> Step {
        let Some(t) = self.conns.get(&target) else {
            return err(more_errno::UNKNOWN_THREAD);
        };
        if !t.alive {
            return err(more_errno::UNKNOWN_THREAD);
        }
        if connection {
            self.drop_conn(target, Some(errno::CONNECTION_KILLED));
        } else if let Some(p) = t.pending.clone() {
            let server = t.server.clone();
            if matches!(p.kind, PendingKind::Lock(_) | PendingKind::GtidWait(_)) {
                if p.implicit {
                    self.end_implicit(target, false);
                }
                self.finish(target, Outcome::Err(errno::QUERY_INTERRUPTED));
                self.wake(&server);
            }
        }
        ok()
    }

    fn applied(&self, server: &str, g: Gtid) -> bool {
        self.servers[server]
            .slave_pos
            .get(&g.domain)
            .is_some_and(|p| p.seq >= g.seq)
    }

    fn gtid_wait(
        &mut self,
        conn: ConnId,
        gtid: Gtid,
        timeout: u32,
        stmt: &Stmt,
        order: (Duration, u64),
    ) -> Step {
        let server = self.conn(conn).server.clone();
        if self.applied(&server, gtid) {
            return Step::Done(Outcome::rows(vec![vec![Some("0".into())]]));
        }
        let token = self.token();
        self.conns.get_mut(&conn).expect("conn").pending = Some(Pending {
            stmt: stmt.clone(),
            kind: PendingKind::GtidWait(gtid),
            token,
            order,
            implicit: false,
        });
        let at = self.now + Duration::from_secs(u64::from(timeout));
        self.schedule(at, Event::GtidWaitTimeout { conn, token });
        Step::Blocked
    }

    fn wake_gtid_waits(&mut self, server: &str) {
        let waiting: Vec<(ConnId, Gtid)> = self
            .conns
            .iter()
            .filter(|(_, c)| c.alive && c.server == server)
            .filter_map(|(id, c)| match &c.pending {
                Some(Pending {
                    kind: PendingKind::GtidWait(g),
                    ..
                }) => Some((*id, *g)),
                _ => None,
            })
            .collect();
        for (c, g) in waiting {
            if self.applied(server, g) {
                self.finish(c, Outcome::rows(vec![vec![Some("0".into())]]));
            }
        }
    }

    // ------------------------------------------------------------------
    // Events.

    fn handle(&mut self, ev: Event) {
        match ev {
            Event::LockTimeout { conn, token } => {
                let Some(p) = self.conns.get(&conn).and_then(|c| c.pending.clone()) else {
                    return;
                };
                if p.token != token || !matches!(p.kind, PendingKind::Lock(_)) {
                    return;
                }
                let server = self.conn(conn).server.clone();
                // Only the statement rolls back (innodb_rollback_on_timeout
                // is OFF); an autocommit statement's transaction is the
                // statement.
                if p.implicit {
                    self.end_implicit(conn, false);
                }
                self.finish(conn, Outcome::Err(errno::LOCK_WAIT_TIMEOUT));
                self.wake(&server);
            }
            Event::GtidWaitTimeout { conn, token } => {
                let Some(p) = self.conns.get(&conn).and_then(|c| c.pending.clone()) else {
                    return;
                };
                if p.token == token {
                    self.finish(conn, Outcome::rows(vec![vec![Some("-1".into())]]));
                }
            }
            Event::AckTimeout { server, gtid } => {
                let waiting = self.servers.get(&server).is_some_and(|s| {
                    s.up && s
                        .txns
                        .values()
                        .any(|t| t.awaiting_ack.is_some_and(|(g, _)| g == gtid))
                });
                if waiting {
                    self.note(format!("{server} semi-sync off: no ack for {gtid}"));
                    self.servers.get_mut(&server).expect("server").semisync_on = false;
                    self.release_acks(&server, None);
                }
            }
            Event::IoPoll { replica } => self.io_poll(&replica),
            Event::Arrive {
                replica,
                source,
                groups,
            } => self.arrive(&replica, &source, groups),
            Event::Ack {
                primary,
                replica,
                gtid,
            } => {
                let ok = self.servers.get(&primary).is_some_and(|s| s.up)
                    && self.reach(&replica, &primary);
                if ok {
                    let s = self.servers.get_mut(&primary).expect("server");
                    if s.cfg.semisync_master && !s.semisync_on {
                        s.semisync_on = true;
                        self.note(format!("{primary} semi-sync back on"));
                    }
                    self.release_acks(&primary, Some(gtid));
                }
            }
            Event::Apply { replica } => self.apply_relay(&replica),
            Event::GaleraApply { node, seqno, group } => self.galera_apply(&node, seqno, &group),
        }
    }

    fn io_poll(&mut self, replica: &str) {
        let Some(s) = self.servers.get(replica) else {
            return;
        };
        let Some(r) = &s.replica else { return };
        if !s.up || !r.io_running {
            return;
        }
        let source = r.source.clone();
        let src_ok =
            self.servers.get(&source).is_some_and(|x| x.up) && self.reach(&source, replica);
        if !src_ok {
            let at = self.now + IO_RETRY;
            // One retry tick at a time.
            let pending = self
                .events
                .values()
                .any(|e| matches!(e, Event::IoPoll { replica: x } if x == replica));
            if !pending {
                self.schedule(
                    at,
                    Event::IoPoll {
                        replica: replica.to_owned(),
                    },
                );
            }
            return;
        }
        // gtid_strict_mode: a replica ahead of its source in a domain stops.
        let src_state = self.servers[&source].binlog_state.clone();
        let ahead = self.servers[replica]
            .binlog_state
            .iter()
            .find(|(d, g)| src_state.get(d).is_none_or(|sg| g.seq > sg.seq))
            .map(|(_, g)| *g);
        if let Some(g) = ahead {
            let msg = format!("1236: replica has {g}, which source {source} lacks");
            self.note(format!("{replica} IO thread stopped: {msg}"));
            let r = self
                .servers
                .get_mut(replica)
                .expect("server")
                .replica
                .as_mut()
                .expect("replica");
            r.io_running = false;
            r.error = Some(msg);
            return;
        }
        let requested = self.servers[replica]
            .replica
            .as_ref()
            .expect("replica")
            .requested
            .clone();
        let groups: Vec<Group> = self.servers[&source]
            .binlog
            .iter()
            .filter(|g| {
                requested
                    .get(&g.gtid.domain)
                    .is_none_or(|s| g.gtid.seq > *s)
            })
            .cloned()
            .collect();
        if groups.is_empty() {
            return;
        }
        let r = self
            .servers
            .get_mut(replica)
            .expect("server")
            .replica
            .as_mut()
            .expect("replica");
        for g in &groups {
            let e = r.requested.entry(g.gtid.domain).or_insert(0);
            *e = (*e).max(g.gtid.seq);
        }
        let at = self.now + self.delay(&source, replica);
        self.schedule(
            at,
            Event::Arrive {
                replica: replica.to_owned(),
                source,
                groups,
            },
        );
    }

    fn arrive(&mut self, replica: &str, source: &str, groups: Vec<Group>) {
        let now = self.now;
        let Some(s) = self.servers.get_mut(replica) else {
            return;
        };
        let up = s.up;
        let Some(r) = s.replica.as_mut() else { return };
        if !up || r.source != source || !r.io_running {
            return;
        }
        let jitter = self.rng.duration(Duration::ZERO, r.lag / 10);
        let ready = now + r.lag + jitter;
        let last = groups.last().map(|g| g.gtid);
        let semisync = r.semisync_slave;
        for g in groups {
            r.relay.push_back((ready, g));
        }
        self.schedule(
            ready,
            Event::Apply {
                replica: replica.to_owned(),
            },
        );
        if semisync && let Some(g) = last {
            let at = now + self.delay(replica, source);
            self.schedule(
                at,
                Event::Ack {
                    primary: source.to_owned(),
                    replica: replica.to_owned(),
                    gtid: g,
                },
            );
        }
    }

    fn apply_relay(&mut self, replica: &str) {
        let now = self.now;
        loop {
            let Some(s) = self.servers.get_mut(replica) else {
                return;
            };
            if !s.up {
                return;
            }
            let Some(r) = s.replica.as_mut() else { return };
            if !r.sql_running {
                return;
            }
            match r.relay.front() {
                Some((ready, _)) if *ready <= now => {}
                _ => return,
            }
            let (_, g) = r.relay.pop_front().expect("front");
            self.apply_group(replica, &g);
        }
    }

    /// Applies a replicated group on `server` (replica SQL thread).
    fn apply_group(&mut self, server: &str, g: &Group) {
        match &g.kind {
            GroupKind::Commit => {
                self.servers
                    .get_mut(server)
                    .expect("server")
                    .install(&g.writes);
            }
            GroupKind::Ddl => self.apply_ddl(server, g),
            GroupKind::XaPrepare(x) => {
                self.next_txn += 1;
                let id = self.next_txn;
                let s = self.servers.get_mut(server).expect("server");
                let mut tx = Txn {
                    conn: None,
                    implicit: false,
                    iso: Isolation::RepeatableRead,
                    view: None,
                    writes: g.writes.iter().cloned().collect(),
                    locks: BTreeSet::new(),
                    xa: Some((x.clone(), XaState::Prepared)),
                    galera_base: None,
                    awaiting_ack: None,
                    gtid: Some(g.gtid),
                };
                for (row, _) in &g.writes {
                    s.locks.insert(row.clone(), id);
                    tx.locks.insert(row.clone());
                }
                s.txns.insert(id, tx);
                s.prepared.insert(x.clone(), id);
            }
            GroupKind::XaCommit(x) | GroupKind::XaRollback(x) => {
                let commit = matches!(g.kind, GroupKind::XaCommit(_));
                if let Some(t) = self.servers[server].prepared.get(x).copied() {
                    if commit {
                        self.install_txn(server, t);
                    } else {
                        self.rollback_txn(server, t);
                    }
                }
            }
        }
        let s = self.servers.get_mut(server).expect("server");
        s.log(g.clone());
        s.slave_pos.insert(g.gtid.domain, g.gtid);
        self.wake_gtid_waits(server);
        // log_slave_updates: this server's own replicas get it too.
        self.poll_replicas_of(server);
    }

    fn galera_apply(&mut self, node: &str, seqno: u64, g: &Group) {
        if !self.servers.get(node).is_some_and(|s| s.up) {
            return;
        }
        // Brute-force abort: local transactions holding or writing rows
        // of the writeset lose.
        let rows: BTreeSet<RowRef> = g.writes.iter().map(|(r, _)| r.clone()).collect();
        let victims: Vec<(TxnId, Option<ConnId>)> = self.servers[node]
            .txns
            .iter()
            .filter(|(_, t)| {
                t.locks
                    .iter()
                    .chain(t.writes.keys())
                    .any(|r| rows.contains(r))
            })
            .filter(|(_, t)| !matches!(t.xa, Some((_, XaState::Prepared))))
            .map(|(id, t)| (*id, t.conn))
            .collect();
        for (t, c) in victims {
            self.note(format!("{node} brute-force abort of {c:?}"));
            if let Some(c) = c {
                let pending = self.conns.get(&c).is_some_and(|x| x.pending.is_some());
                self.conns.get_mut(&c).expect("conn").txn = None;
                if pending {
                    self.finish(c, Outcome::Err(errno::DEADLOCK));
                } else {
                    self.conns.get_mut(&c).expect("conn").poisoned = Some(errno::DEADLOCK);
                }
            }
            self.rollback_txn(node, t);
        }
        match &g.kind {
            GroupKind::Ddl => self.apply_ddl(node, g),
            _ => self
                .servers
                .get_mut(node)
                .expect("server")
                .install(&g.writes),
        }
        let s = self.servers.get_mut(node).expect("server");
        s.galera_applied = s.galera_applied.max(seqno);
        s.log(g.clone());
    }

    // ------------------------------------------------------------------
    // Faults and topology changes.

    /// Crashes `server`: connections drop, uncommitted work is lost,
    /// committed and prepared transactions stay, and a transaction waiting
    /// for its semi-sync ack commits (it is in the binlog).
    ///
    /// # Panics
    /// Only if the model's own state is inconsistent (a bug).
    pub fn crash(&mut self, server: &str, now: Duration) {
        self.advance(now);
        let Some(s) = self.servers.get_mut(server) else {
            return;
        };
        if !s.up {
            return;
        }
        s.up = false;
        self.note(format!("{server} crashed"));
        let conns: Vec<ConnId> = self
            .conns
            .iter()
            .filter(|(_, c)| c.alive && c.server == server)
            .map(|(id, _)| *id)
            .collect();
        for c in conns {
            self.drop_conn(c, Some(errno::SERVER_LOST));
        }
        let s = self.servers.get_mut(server).expect("server");
        let ids: Vec<TxnId> = s.txns.keys().copied().collect();
        for t in ids {
            let s = self.servers.get_mut(server).expect("server");
            let tx = s.txns[&t].clone();
            if let Some((_, after)) = tx.awaiting_ack {
                s.txns.get_mut(&t).expect("txn").awaiting_ack = None;
                if after == AfterAck::Install {
                    self.install_txn(server, t);
                }
                continue;
            }
            if matches!(tx.xa, Some((_, XaState::Prepared))) {
                s.txns.get_mut(&t).expect("txn").conn = None;
                continue;
            }
            self.rollback_txn(server, t);
        }
        let s = self.servers.get_mut(server).expect("server");
        s.semisync_on = s.cfg.semisync_master;
        s.read_only = s.cfg.read_only_on_boot;
        if let Some(r) = s.replica.as_mut() {
            // relay_log_recovery: refetch from what was applied.
            r.relay.clear();
            r.requested = s.slave_pos.iter().map(|(d, g)| (*d, g.seq)).collect();
        }
    }

    /// Restarts a crashed server.
    ///
    /// # Panics
    /// Only if the model's own state is inconsistent (a bug).
    pub fn restart(&mut self, server: &str, now: Duration) {
        self.advance(now);
        let Some(s) = self.servers.get_mut(server) else {
            return;
        };
        if s.up {
            return;
        }
        s.up = true;
        let is_replica = s.replica.is_some();
        self.note(format!("{server} restarted"));
        if is_replica {
            self.schedule(
                now,
                Event::IoPoll {
                    replica: server.to_owned(),
                },
            );
        }
        // Replicas of this server reconnect.
        self.poll_replicas_of(server);
        // A Galera node rejoining catches up by state transfer.
        if let Some(cluster) = self.servers[server].galera.clone() {
            let donor = self.galeras[&cluster]
                .nodes
                .iter()
                .filter(|n| n.as_str() != server && self.servers[n.as_str()].up)
                .max_by_key(|n| self.servers[n.as_str()].galera_applied)
                .cloned();
            if let Some(d) = donor {
                let src = self.servers[&d].clone();
                let s = self.servers.get_mut(server).expect("server");
                s.tables = src.tables;
                s.commit_seq = src.commit_seq;
                s.binlog = src.binlog;
                s.binlog_state = src.binlog_state;
                s.galera_applied = src.galera_applied;
            }
        }
    }

    /// Whether `server` is up.
    #[must_use]
    pub fn is_up(&self, server: &str) -> bool {
        self.servers.get(server).is_some_and(|s| s.up)
    }

    /// Stops or starts a replica's SQL thread.
    pub fn set_replica_sql(&mut self, replica: &str, running: bool, now: Duration) {
        self.advance(now);
        if let Some(r) = self
            .servers
            .get_mut(replica)
            .and_then(|s| s.replica.as_mut())
        {
            r.sql_running = running;
        }
        if running {
            self.schedule(
                now,
                Event::Apply {
                    replica: replica.to_owned(),
                },
            );
        }
    }

    /// Sets a replica's apply lag for events that arrive from now on.
    pub fn set_replica_lag(&mut self, replica: &str, lag: Duration) {
        if let Some(r) = self
            .servers
            .get_mut(replica)
            .and_then(|s| s.replica.as_mut())
        {
            r.lag = lag;
        }
    }

    /// Promotes a replica to primary: it applies what its relay log holds
    /// (`drain`) or drops it, stops replicating, and turns semi-sync
    /// (primary side) on as failover does. Returns the GTIDs it dropped.
    ///
    /// # Panics
    /// Only if the model's own state is inconsistent (a bug).
    pub fn promote(&mut self, replica: &str, drain: bool, now: Duration) -> Vec<Gtid> {
        self.advance(now);
        let Some(r) = self.servers.get_mut(replica).and_then(|s| s.replica.take()) else {
            return Vec::new();
        };
        let mut dropped = Vec::new();
        for (_, g) in r.relay {
            if drain {
                self.apply_group(replica, &g);
            } else {
                dropped.push(g.gtid);
            }
        }
        let s = self.servers.get_mut(replica).expect("server");
        s.cfg.semisync_master = true;
        s.semisync_on = true;
        s.read_only = false;
        self.note(format!("{replica} promoted"));
        dropped
    }

    /// Points a server at a new source (`CHANGE MASTER ... MASTER_USE_GTID
    /// = slave_pos`), turning a former primary into a replica.
    pub fn change_source(&mut self, server: &str, source: &str, lag: Duration, now: Duration) {
        self.advance(now);
        let Some(s) = self.servers.get_mut(server) else {
            return;
        };
        let requested = s.binlog_state.iter().map(|(d, g)| (*d, g.seq)).collect();
        s.cfg.semisync_master = false;
        s.semisync_on = false;
        s.replica = Some(Replica {
            source: source.to_owned(),
            io_running: true,
            sql_running: true,
            semisync_slave: true,
            lag,
            requested,
            relay: VecDeque::new(),
            error: None,
        });
        self.schedule(
            now,
            Event::IoPoll {
                replica: server.to_owned(),
            },
        );
    }

    /// The replica's IO error, if replication stopped.
    #[must_use]
    pub fn replica_error(&self, replica: &str) -> Option<String> {
        self.servers.get(replica)?.replica.as_ref()?.error.clone()
    }

    /// Whether semi-sync is active on a primary (`Rpl_semi_sync_master_status`).
    #[must_use]
    pub fn semisync_status(&self, server: &str) -> bool {
        self.servers.get(server).is_some_and(|s| s.semisync_on)
    }

    /// Every GTID in `server`'s binlog, in order.
    #[must_use]
    pub fn gtids(&self, server: &str) -> Vec<Gtid> {
        self.servers
            .get(server)
            .map(|s| s.binlog.iter().map(|g| g.gtid).collect())
            .unwrap_or_default()
    }

    /// GTIDs in `old`'s binlog that `new` doesn't have: what a failover
    /// from `old` to `new` lost (its diverged transactions).
    #[must_use]
    pub fn lost_gtids(&self, old: &str, new: &str) -> Vec<Gtid> {
        let have: BTreeSet<Gtid> = self.gtids(new).into_iter().collect();
        self.gtids(old)
            .into_iter()
            .filter(|g| !have.contains(g))
            .collect()
    }

    /// Committed rows of `schema.table` on `server`, in key order.
    #[must_use]
    pub fn rows(&self, server: &str, schema: &str, table: &str) -> Vec<(i64, String)> {
        let Some(s) = self.servers.get(server) else {
            return Vec::new();
        };
        s.tables
            .get(&qualified(schema, table))
            .map(|m| {
                m.iter()
                    .filter_map(|(k, vs)| vs.last()?.value.clone().map(|v| (*k, v)))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The server a connection talks to.
    #[must_use]
    pub fn server_of(&self, conn: ConnId) -> Option<&str> {
        self.conns.get(&conn).map(|c| c.server.as_str())
    }

    /// Server names.
    #[must_use]
    pub fn servers(&self) -> Vec<String> {
        self.servers.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests;
