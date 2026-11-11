//! [`BackendSession`]: one client session's backend side (see the
//! [module docs](super) for the protocol an executor follows).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use sprig_auth::accounts::BackendTarget;
use sprig_cluster::record::TopologyKind;
use sprig_common::ids::{NodeId, ShardId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::client::{ClientError, PacketKind, RespPacket, TrackedChanges};
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::resultset::Terminator;
use sprig_proto_mysql::{Caps, ErrPacket, Persona, ProtoResult, StatusFlags};
use tokio::time::Instant;

use super::classify::{Effect, StmtClass, StmtInfo};
use super::cluster::{ClusterView, NodeKey, NodeRef};
use super::conn::{self, BackendStmt, ConnectSpec, Warning};
use super::pins::{PinReason, Pins};
use super::pool::{AcquireError, BackendLease, PoolKey, Release};
use super::settings::{BackendSettings, CausalScope, PoolMode};
use super::state::{self, SessionVars};
use super::writer::WriterSelector as _;
use super::{Backends, unavailable};
use crate::reads::{self, GtidSet, ReadTarget};

/// The rendezvous key whose Galera writer is the cluster's lock node
/// (chapter 28, "Locks"; R4 round 4): not a shard, a fixed key every
/// gateway hashes the same way.
pub(crate) const LOCK_NODE_KEY: ShardId = ShardId(u32::MAX);

/// Options of a session's backend side, from its login.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOpts {
    /// The listener's persona: the MySQL persona starts with MySQL 8.4's
    /// `sql_mode`.
    pub persona: Persona,
    /// The client negotiated `CLIENT_FOUND_ROWS`.
    pub found_rows: bool,
    /// MariaDB extended capabilities to ask backend connections for (3307),
    /// among `MARIADB_CLIENT_EXTENDED_METADATA` and
    /// `MARIADB_CLIENT_STMT_BULK_OPERATIONS`. A backend may grant fewer:
    /// [`BackendSession::backend_caps`] says what it did.
    pub mariadb_caps: Caps,
    /// The handshake collation id.
    pub collation: u16,
    /// The schema named at login.
    pub schema: Option<String>,
    /// The shard (M0.2: the login's cluster has one, shard 0).
    pub shard: ShardId,
}

impl Default for SessionOpts {
    fn default() -> Self {
        Self {
            persona: Persona::MariaDb,
            found_rows: false,
            mariadb_caps: Caps::empty(),
            collation: 45,
            schema: None,
            shard: ShardId(0),
        }
    }
}

/// How a statement was routed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteKind {
    /// On the session's pinned connection.
    Pinned,
    /// On the writer.
    Writer,
    /// On a replica whose position covered the causal target.
    Replica,
    /// On a replica, after the wait-or-fail prefix.
    ReplicaWait,
    /// On a Galera node other than the writer, with no check: the read
    /// had no write to see, or the node's polled position covered it.
    Galera,
    /// On a Galera node other than the writer whose position covered the
    /// causal target, after the bounded sync-wait check
    /// ([`reads::GALERA_SYNC_CHECK`]).
    GaleraSyncWait,
    /// On a Galera node other than the writer whose polled position didn't
    /// show the causal target yet, after the bounded check
    /// (`reads.galera_unconfirmed = check`, decision D14).
    GaleraUnconfirmed,
}

impl RouteKind {
    /// Off the writer: state must never appear on such a connection.
    fn reader(self) -> bool {
        matches!(
            self,
            RouteKind::Replica
                | RouteKind::ReplicaWait
                | RouteKind::Galera
                | RouteKind::GaleraSyncWait
                | RouteKind::GaleraUnconfirmed
        )
    }
}

/// What a read waits for before the client's statement.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Wait {
    /// Nothing.
    None,
    /// A replica: the wait-or-fail prefix for this target.
    Replica(GtidSet),
    /// A Galera node: [`reads::GALERA_SYNC_CHECK`], bounded by
    /// `reads.galera_sync_wait_timeout_ms`.
    Galera,
}

/// How the sync (and wait) before a statement went.
enum Sent {
    /// The statement is on its way.
    Ready,
    /// A sync statement failed.
    SyncErr(ErrPacket),
    /// The wait failed (replica 1242, or the Galera check's error); the
    /// connection is idle and reusable.
    WaitFailed,
    /// The Galera check didn't finish in time; the connection is still
    /// busy with it and must be dropped.
    WaitTimedOut,
}

/// What the gateway answers itself after a release (chapter 06,
/// "Acquire, sync and release").
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Captured {
    /// `SHOW WARNINGS` of the last statement (empty when it had none).
    pub warnings: Vec<Warning>,
    /// `FOUND_ROWS()`: captured after `SQL_CALC_FOUND_ROWS`, else the rows
    /// the last statement returned.
    pub found_rows: u64,
    /// `ROW_COUNT()`: affected rows, or -1 after a resultset.
    pub row_count: i64,
}

/// The outcome of a finished statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StmtOutcome {
    /// Status flags of the final packet.
    pub status: StatusFlags,
    /// Warning count of the final packet.
    pub warnings: u16,
    /// Affected rows of the final OK.
    pub affected_rows: u64,
    /// Last insert id of the final OK.
    pub last_insert_id: u64,
    /// The server's error, if the statement failed.
    pub error: Option<ErrPacket>,
    /// The node it ran on.
    pub node: NodeKey,
    /// How it was routed.
    pub route: RouteKind,
    /// Whether the session now holds its connection.
    pub pinned: bool,
    /// Rows of the last resultset.
    pub rows: u64,
    /// Rows examined, when a `Handler_read_*` probe ran around the
    /// statement ([`BackendSession::probe_next_statement`]).
    pub examined: Option<u64>,
}

/// The statement in flight.
#[allow(clippy::struct_excessive_bools)] // independent flags of one statement
struct Current {
    lease: BackendLease,
    stmt: StmtInfo,
    route: RouteKind,
    first: Option<RespPacket>,
    relayed: bool,
    done: bool,
    failed: bool,
    in_trx_before: bool,
    status: Option<StatusFlags>,
    warnings: u16,
    last_ok: Option<OkPacket>,
    error: Option<ErrPacket>,
    tracked: TrackedChanges,
    rows: u64,
    resultset: bool,
    /// The first `Handler_read_*` reading of a probe.
    probe: Option<u64>,
    /// The statement is a `USE`: it may run on a connection whose default
    /// schema differs from the session's, so a failed one leaves that
    /// connection dirty.
    sets_schema: bool,
    /// It writes generated `AUTO_INCREMENT` values explicitly
    /// ([`BackendSession::explicit_ids_next_statement`]).
    explicit_ids: bool,
    /// The statement's `backend.query` trace span (chapter 47): open from
    /// `run` until `finish` drops the statement.
    span: Option<tracing::Span>,
}

impl Current {
    fn observe(&mut self, p: &RespPacket) {
        match &p.kind {
            PacketKind::Ok(ok) => {
                self.status = Some(ok.status);
                self.warnings = ok.warnings;
                self.last_ok = Some(ok.clone());
            }
            PacketKind::End(t) => {
                self.status = Some(t.status());
                self.warnings = t.warnings();
                if let Terminator::Ok(ok) = t {
                    self.last_ok = Some(OkPacket {
                        affected_rows: 0,
                        last_insert_id: 0,
                        ..ok.clone()
                    });
                }
            }
            PacketKind::Err(e) => self.error = Some(e.clone()),
            PacketKind::Row => self.rows += 1,
            PacketKind::ColumnCount(_) => {
                self.resultset = true;
                self.rows = 0;
            }
            _ => {}
        }
        if !p.tracked.is_empty() {
            self.tracked.merge(p.tracked.clone());
        }
    }
}

/// A request to run.
/// Whether a connection must be closed after a statement: a `USE` that
/// left the connection's default schema different from the session's
/// (it failed on a connection that had another one).
fn dirty_after_use(sets_schema: bool, conn: Option<&str>, session: Option<&str>) -> bool {
    sets_schema && conn != session
}

/// Whether `sql` is a `USE <schema>` statement.
fn is_use(sql: &[u8]) -> bool {
    let t = sql.trim_ascii_start();
    t.len() > 4 && t[..3].eq_ignore_ascii_case(b"USE") && t[3].is_ascii_whitespace()
}

enum Req<'a> {
    Text(&'a [u8]),
    Binary {
        sql: &'a [u8],
        build: &'a mut (dyn FnMut(&BackendStmt) -> ProtoResult<Bytes> + Send + Sync),
    },
    Command(Bytes),
    /// Prepare only (into the connection's statement cache); the
    /// statement is copied out and nothing else is sent.
    Prepare {
        sql: &'a [u8],
        out: &'a mut Option<BackendStmt>,
    },
}

/// One client session's backend side: its client-visible state, pins,
/// held connection, causal position and captures.
pub struct BackendSession {
    backends: Arc<Backends>,
    target: BackendTarget,
    opts: SessionOpts,
    vars: SessionVars,
    pins: Pins,
    held: Option<(BackendLease, RouteKind)>,
    causal: GtidSet,
    captured: Captured,
    pending_capture: Vec<String>,
    cur: Option<Current>,
    /// The connection `run` is sending on or waiting for the first packet
    /// from (before `cur` exists), so `KILL QUERY` can reach a statement
    /// abandoned in that window.
    inflight: Option<(NodeKey, u64)>,
    /// The error the next statement reports: an abandoned statement took
    /// the session's transaction or pinned state with its connection.
    deferred: Option<SprigError>,
    /// Probe the next statement's rows examined.
    probe_next: bool,
    /// The next statement writes `AUTO_INCREMENT` values the gateway
    /// generated (chapter 19's ID blocks): its OK packet's insert id is
    /// an explicit value, not the connection's `LAST_INSERT_ID()`.
    explicit_ids_next: bool,
    /// Sprig 2PC (R3): a held branch's record of its writes
    /// (`sprig_txn::BranchConn::start_redo`, in `backend::txn`).
    pub(super) redo: Option<super::txn::Recording>,
}

impl std::fmt::Debug for BackendSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendSession")
            .field("cluster", &self.target.cluster)
            .field("account", &self.target.user)
            .field("pins", &self.pins)
            .field("causal", &self.causal)
            .finish_non_exhaustive()
    }
}

fn pool_timeout() -> SprigError {
    SprigError::new(
        SprigCode::BackendPoolTimeout,
        "no backend connection became free in time",
    )
}

fn backend_err(e: &ErrPacket) -> SprigError {
    SprigError::backend(e.code, e.sqlstate, e.message.clone())
}

/// Server errors after which the server closes the connection.
fn conn_fatal(code: u16) -> bool {
    // 1927 ER_CONNECTION_KILLED, 1053 ER_SERVER_SHUTDOWN, 1152
    // ER_ABORTING_CONNECTION, 4031 (idle disconnect).
    matches!(code, 1927 | 1053 | 1152 | 4031)
}

/// 1213 (40001): the connection was lost and the open transaction rolled
/// back, so the client retries it as it would after a deadlock.
fn trx_rolled_back() -> SprigError {
    SprigError::standard(
        1213,
        *b"40001",
        "Deadlock found when trying to get lock; try restarting transaction \
         (the backend connection was lost and the transaction rolled back)",
    )
}

fn internal(e: &ClientError) -> SprigError {
    SprigError::new(SprigCode::BackendProtocol, e.to_string())
}

/// ST31 (bugfix5): a session dropped with a statement under way stops that
/// statement. The router's per-shard streams (scatter, gather, analytic
/// reads) are sessions of their own, dropped with the request when its
/// REST or gRPC client goes away; their connections close with them, but
/// MariaDB would run a statement such as `SELECT SLEEP(30)` to its end.
/// The connection is never reused (a dropped lease closes it), so the
/// `KILL QUERY` can't reach another statement.
impl Drop for BackendSession {
    fn drop(&mut self) {
        if !self.in_flight() {
            return;
        }
        let Some((node, id)) = self.thread_id() else {
            return;
        };
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let backends = Arc::clone(&self.backends);
        let target = self.target.clone();
        // The connection stays open until the kill is in: MariaDB's `KILL`
        // of a thread whose client has just closed its socket can wait up
        // to 2 s for that thread (seen live), one of a live client's doesn't.
        let conn = self.cur.take();
        drop(rt.spawn(async move {
            let t0 = std::time::Instant::now();
            let r =
                tokio::time::timeout(Duration::from_secs(3), backends.kill_query(&node, &target, id))
                    .await;
            drop(conn);
            let ms = t0.elapsed().as_millis();
            match r {
                Ok(Ok(())) if ms < 500 => {
                    tracing::debug!(node = %node, thread = id, ms, "statement of a dropped backend session killed");
                }
                Ok(Ok(())) => {
                    tracing::info!(node = %node, thread = id, ms, "statement of a dropped backend session killed, slowly");
                }
                Ok(Err(e)) => {
                    tracing::info!(node = %node, thread = id, ms, error = %e, "KILL QUERY for a dropped backend session failed");
                }
                Err(_) => {
                    tracing::info!(node = %node, thread = id, "KILL QUERY for a dropped backend session timed out");
                }
            }
        }));
    }
}

impl BackendSession {
    pub(super) fn new(backends: Arc<Backends>, target: BackendTarget, opts: SessionOpts) -> Self {
        let settings = backends.settings(&target.cluster);
        let mut pins = Pins::default();
        pins.set_session_mode(settings.pool_mode == PoolMode::Session);
        Self {
            vars: SessionVars::login(opts.persona, opts.collation, opts.schema.clone()),
            backends,
            target,
            opts,
            pins,
            held: None,
            causal: GtidSet::new(),
            captured: Captured::default(),
            pending_capture: Vec::new(),
            cur: None,
            inflight: None,
            deferred: None,
            probe_next: false,
            explicit_ids_next: false,
            redo: None,
        }
    }

    /// The client-visible session state.
    #[must_use]
    pub fn vars(&self) -> &SessionVars {
        &self.vars
    }

    /// The client-visible session state, for the executor to change what
    /// the gateway answers itself (such as `SET NAMES` at login).
    pub fn vars_mut(&mut self) -> &mut SessionVars {
        &mut self.vars
    }

    /// The pins.
    #[must_use]
    pub fn pins(&self) -> &Pins {
        &self.pins
    }

    /// Whether the session holds a backend connection (or will at its next
    /// statement).
    #[must_use]
    pub fn is_pinned(&self) -> bool {
        !self.pins.is_empty()
    }

    /// The session's last commit GTIDs.
    #[must_use]
    pub fn causal(&self) -> &GtidSet {
        &self.causal
    }

    /// Sets the causal position (a router stream on its own connection
    /// waits for what the session's connection to that cluster saw).
    pub fn set_causal(&mut self, g: GtidSet) {
        self.causal = g;
    }

    /// The statement in flight's first packet, when it is an ERR (after
    /// [`query`](Self::query) returned): the router reads it before
    /// relaying, to retry after a stale shard map (chapter 17).
    #[must_use]
    pub fn first_error(&self) -> Option<&ErrPacket> {
        match &self.cur.as_ref()?.first.as_ref()?.kind {
            PacketKind::Err(e) => Some(e),
            _ => None,
        }
    }

    /// What the gateway answers after a release: warnings, `FOUND_ROWS()`,
    /// `ROW_COUNT()`.
    #[must_use]
    pub fn captured(&self) -> &Captured {
        &self.captured
    }

    /// The backend target.
    #[must_use]
    pub fn target(&self) -> &BackendTarget {
        &self.target
    }

    /// The backend thread id of the statement in flight or the held
    /// connection.
    #[must_use]
    pub fn thread_id(&self) -> Option<(NodeKey, u64)> {
        let Some(lease) = self
            .cur
            .as_ref()
            .map(|c| &c.lease)
            .or(self.held.as_ref().map(|(l, _)| l))
        else {
            return self.inflight.clone();
        };
        Some((lease.key().node_key(), lease.conn_ref().thread_id))
    }

    /// The capabilities the backend connection serving the statement in
    /// flight negotiated (or the held connection's, between statements);
    /// `None` without either. Call it after [`query`](Self::query),
    /// [`execute`](Self::execute) or [`command`](Self::command) returns:
    /// column definitions in the response are encoded with these (only
    /// `MARIADB_CLIENT_EXTENDED_METADATA` changes them), whatever
    /// [`SessionOpts::mariadb_caps`] asked for.
    #[must_use]
    pub fn backend_caps(&self) -> Option<Caps> {
        self.cur
            .as_ref()
            .map(|c| &c.lease)
            .or(self.held.as_ref().map(|(l, _)| l))
            .map(|l| l.conn_ref().client.caps())
    }

    /// Whether a statement is under way: sent, or its response not read to
    /// its end.
    #[must_use]
    pub fn in_flight(&self) -> bool {
        self.cur.as_ref().is_some_and(|c| !c.done && !c.failed) || self.inflight.is_some()
    }

    /// `KILL QUERY` on the backend thread of the statement in flight (or
    /// the held connection).
    ///
    /// # Errors
    /// As [`Backends::kill_query`].
    pub async fn kill_query(&self) -> Result<()> {
        match self.thread_id() {
            Some((node, id)) => self.backends.kill_query(&node, &self.target, id).await,
            None => Ok(()),
        }
    }

    /// A server-side cursor opened (`COM_STMT_EXECUTE` with a cursor
    /// flag): pins until [`close_cursor`](Self::close_cursor).
    pub fn open_cursor(&mut self) {
        self.pins.open_cursor();
    }

    /// A cursor closed.
    pub fn close_cursor(&mut self) {
        self.pins.close_cursor();
    }

    /// Measures the rows the next statement (`query` or `execute`)
    /// examines (chapter 12, "Sampling rows examined"): the connection's
    /// `Handler_read_*` sum is read before it runs and after it ends, on
    /// the same connection, and the difference less the reading's own
    /// cost lands in [`StmtOutcome::examined`].
    ///
    /// Only statements that leave the connection free are measured *(the
    /// reading after would replace the diagnostics a pinned session reads
    /// next: `ROW_COUNT()`, `FOUND_ROWS()`, `SHOW WARNINGS`)*: in a
    /// transaction or with pinned state, no probe runs. The reading after
    /// comes once the release captures are done. A probe that fails is
    /// dropped; the statement's result is never affected.
    pub fn probe_next_statement(&mut self) {
        self.probe_next = true;
    }

    /// Marks the next statement as one that writes `AUTO_INCREMENT`
    /// values the gateway generated (chapter 19): its OK packet's insert
    /// id then changes neither the session's `LAST_INSERT_ID()` nor what
    /// the connection is known to hold (MariaDB keeps the old value after
    /// explicit values). The router sets the session's value itself.
    pub fn explicit_ids_next_statement(&mut self) {
        self.explicit_ids_next = true;
    }

    /// Runs one text statement (`COM_QUERY`). On success the statement's
    /// first response packet is ready for [`next`](Self::next).
    ///
    /// # Errors
    /// 39030, 39031, 39032, 39033, 1213 (see the module docs); the
    /// server's error for a failed sync (after one retry); 39100 without a
    /// writer.
    pub async fn query(&mut self, info: &StmtInfo, sql: &[u8]) -> Result<()> {
        let views = self.policy_views(sql);
        let sql = views.as_deref().unwrap_or(sql);
        self.check_packet(sql.len() + 1)?;
        if let Some(r) = self.redo.as_mut() {
            r.before(info, sql);
        }
        self.run(info, Req::Text(sql)).await
    }

    /// A statement the gateway's rewrites made longer than the cluster's
    /// `max_allowed_packet` is refused with 1153 before it is sent:
    /// MariaDB would drop the connection, leaving the outcome unknown
    /// (chapter 05, "Packet limit").
    fn check_packet(&self, len: usize) -> Result<()> {
        match self.backends.packet_limit(&self.target.cluster) {
            Some(limit) if len > limit => Err(super::packet_too_large()),
            _ => Ok(()),
        }
    }

    /// Chapter 29 (decision RL18): `sql` reading the session's protected
    /// tables through their policy views, if it reads any. The session's
    /// account can't read the tables themselves, so a reference this
    /// misses fails (1142) rather than reading unfiltered rows.
    fn policy_views(&self, sql: &[u8]) -> Option<Vec<u8>> {
        let v = self.vars();
        let swap = v.rls_views.as_ref()?;
        let mode = v.vars.get("sql_mode").map(|m| m.to_ascii_uppercase());
        let lex = sprig_sql::lex::LexOptions {
            ansi_quotes: mode.as_deref().is_some_and(|m| m.contains("ANSI_QUOTES")),
            no_backslash_escapes: mode
                .as_deref()
                .is_some_and(|m| m.contains("NO_BACKSLASH_ESCAPES")),
            ..sprig_sql::lex::LexOptions::mariadb()
        };
        swap.apply(sql, &lex, v.schema.as_deref())
    }

    /// Whether this connection records its writes for a held branch's
    /// redo (chapter 18's Sprig 2PC).
    #[must_use]
    pub fn recording(&self) -> bool {
        self.redo.is_some()
    }

    /// The next [`execute`](Self::execute)'s statement with its bound
    /// values written in, for the redo record (decision Q17): a prepared
    /// write on a recording branch is then replayable.
    pub fn bind_text(&mut self, text: String) {
        if let Some(r) = self.redo.as_mut() {
            r.bound = Some(text);
        }
    }

    /// Runs a prepared statement (binary protocol): prepares `sql` on the
    /// chosen connection if needed (lazily, under the node's budget) and
    /// sends the payload `build` makes for that backend statement. Set the
    /// new-params-bound flag when [`BackendStmt::types_sent`] is false.
    ///
    /// # Errors
    /// As [`query`](Self::query); the server's error for a bad prepare.
    pub async fn execute(
        &mut self,
        info: &StmtInfo,
        sql: &[u8],
        build: &mut (dyn FnMut(&BackendStmt) -> ProtoResult<Bytes> + Send + Sync),
    ) -> Result<()> {
        let views = self.policy_views(sql);
        let sql = views.as_deref().unwrap_or(sql);
        if let Some(r) = self.redo.as_mut() {
            r.before_binary(info, sql);
        }
        self.run(info, Req::Binary { sql, build }).await
    }

    /// Prepares `sql` on the chosen connection's statement cache (for
    /// `COM_STMT_PREPARE`: the client's `PREPARE_OK` needs the metadata)
    /// and ends the statement: the connection is released, or kept when
    /// the session is pinned. Executes then find it cached.
    ///
    /// # Errors
    /// As [`query`](Self::query); the server's error for a bad statement.
    pub async fn prepare(&mut self, info: &StmtInfo, sql: &[u8]) -> Result<BackendStmt> {
        let views = self.policy_views(sql);
        let sql = views.as_deref().unwrap_or(sql);
        let mut out = None;
        self.run(info, Req::Prepare { sql, out: &mut out }).await?;
        self.finish().await?;
        out.ok_or_else(|| SprigError::internal("prepare returned no statement"))
    }

    /// Sends any other command payload (its first byte is the command
    /// code), after syncing state in its own round trip.
    ///
    /// # Errors
    /// As [`query`](Self::query).
    pub async fn command(&mut self, info: &StmtInfo, payload: Bytes) -> Result<()> {
        self.check_packet(payload.len())?;
        self.run(info, Req::Command(payload)).await
    }

    /// The next packet of the response, or `None` when it is complete.
    ///
    /// # Errors
    /// The lost-connection errors of the module docs.
    pub async fn next(&mut self) -> Result<Option<RespPacket>> {
        let Some(cur) = self.cur.as_mut() else {
            return Ok(None);
        };
        if let Some(p) = cur.first.take() {
            cur.relayed = true;
            return Ok(Some(p));
        }
        if cur.done || cur.failed {
            return Ok(None);
        }
        let waited = Instant::now();
        let packet = cur.lease.conn().client.next_packet().await;
        crate::observe::backend_time(waited.elapsed());
        match packet {
            Ok(Some(p)) => {
                cur.observe(&p);
                cur.relayed = true;
                Ok(Some(p))
            }
            Ok(None) => {
                cur.done = true;
                Ok(None)
            }
            Err(e) if e.is_connection_lost() => {
                cur.failed = true;
                let node = cur.lease.key().node_key();
                let (stmt, in_trx, relayed) = (cur.stmt.clone(), cur.in_trx_before, cur.relayed);
                self.backends.note_failure(&node, "io");
                Err(self.lost(&stmt, in_trx, relayed, &node))
            }
            Err(e) => Err(internal(&e)),
        }
    }

    /// Sends a chunk of a `LOCAL INFILE` file.
    ///
    /// # Errors
    /// 39012 if no request is pending; lost-connection errors.
    pub async fn send_infile_data(&mut self, data: &[u8]) -> Result<()> {
        let cur = self
            .cur
            .as_mut()
            .ok_or_else(|| SprigError::internal("no statement"))?;
        cur.lease
            .conn()
            .client
            .send_infile_data(data)
            .await
            .map_err(|e| internal(&e))
    }

    /// Ends (or declines) a `LOCAL INFILE` file.
    ///
    /// # Errors
    /// As [`send_infile_data`](Self::send_infile_data).
    pub async fn finish_infile(&mut self) -> Result<()> {
        let cur = self
            .cur
            .as_mut()
            .ok_or_else(|| SprigError::internal("no statement"))?;
        cur.lease
            .conn()
            .client
            .finish_infile()
            .await
            .map_err(|e| internal(&e))
    }

    /// Ends the statement: reads what the caller left, applies session
    /// tracking and pins, then keeps the connection (pinned) or captures
    /// and releases it.
    ///
    /// # Errors
    /// Lost-connection errors while draining; `Internal` without a
    /// statement.
    pub async fn finish(&mut self) -> Result<StmtOutcome> {
        let pending = self.redo.as_mut().and_then(|r| r.pending.take());
        let r = self.finish_statement().await;
        if let (Some(sql), Ok(o)) = (pending, &r)
            && o.error.is_none()
        {
            let vars = self.vars.clone();
            if let Some(rec) = self.redo.as_mut() {
                rec.record(sql, o.affected_rows, o.last_insert_id, &vars);
            }
        }
        r
    }

    #[allow(clippy::too_many_lines)]
    async fn finish_statement(&mut self) -> Result<StmtOutcome> {
        let Some(mut cur) = self.cur.take() else {
            return Err(SprigError::internal("finish without a statement"));
        };
        let node = cur.lease.key().node_key();
        let draining = Instant::now();
        if !cur.failed && !cur.done {
            if let Some(p) = cur.first.take() {
                drop(p);
            }
            loop {
                match cur.lease.conn().client.next_packet().await {
                    Ok(Some(p)) => cur.observe(&p),
                    Ok(None) => break,
                    Err(ClientError::Busy(_)) => {
                        if let Err(e) = cur.lease.conn().client.finish_infile().await {
                            cur.failed = true;
                            tracing::debug!(error = %e, "declining LOCAL INFILE failed");
                            break;
                        }
                    }
                    Err(e) => {
                        cur.failed = true;
                        if e.is_connection_lost() {
                            self.backends.note_failure(&node, "io");
                            let (stmt, in_trx, relayed) =
                                (cur.stmt.clone(), cur.in_trx_before, cur.relayed);
                            drop(cur);
                            return Err(self.lost(&stmt, in_trx, relayed, &node));
                        }
                        break;
                    }
                }
            }
        }
        crate::observe::backend_time(draining.elapsed());
        let outcome = |cur: &Current, pinned: bool| StmtOutcome {
            status: cur.status.unwrap_or_default(),
            warnings: cur.warnings,
            affected_rows: cur.last_ok.as_ref().map_or(0, |o| o.affected_rows),
            last_insert_id: cur.last_ok.as_ref().map_or(0, |o| o.last_insert_id),
            error: cur.error.clone(),
            node: node.clone(),
            route: cur.route,
            pinned,
            rows: cur.rows,
            examined: None,
        };
        if cur.failed {
            let o = outcome(&cur, false);
            drop(cur);
            return Ok(o);
        }
        if cur.error.as_ref().is_some_and(|e| conn_fatal(e.code)) {
            // The server ended this connection (killed, shutting down): never
            // reuse it; what it held is gone.
            self.pins.lose_connection();
            self.pending_capture.clear();
            let o = outcome(&cur, false);
            drop(cur);
            return Ok(o);
        }

        // Session tracking: schema, variables, commit GTIDs, untracked state.
        let mut observed = state::observe(&cur.tracked);
        if Self::drop_set_statement_reports(&mut cur, &mut observed) {
            observed.unexplained = false;
        }
        self.vars.apply(&observed);
        // Chapter 28 (R4): the MySQL persona's sql_mode view.
        self.vars.mysql_sql_mode_after(&observed);
        cur.lease.conn().applied.apply(&observed);
        if observed.charset_changed() {
            // Tracking names the character set only: read the collation
            // back (chapter 06, "Login collation").
            let set_names = cur.stmt.sets_names();
            let conn = cur.lease.conn();
            match conn.capture_collation().await {
                Ok(coll) => {
                    conn.applied.charset_changed(&observed, &coll, set_names);
                    self.vars.charset_changed(&observed, &coll, set_names);
                }
                Err(e) => {
                    tracing::warn!(node = %node, error = %e, "reading collation_connection failed; closing the connection");
                    let o = outcome(&cur, false);
                    drop(cur);
                    self.connection_gone();
                    return Ok(o);
                }
            }
        }
        let unexplained = observed.unexplained && !cur.stmt.expects_state_change();
        if !observed.untracked.is_empty() || unexplained {
            if !self.pins.reasons().contains(&PinReason::Untracked) {
                self.count_pin(PinReason::Untracked);
            }
            tracing::info!(
                cluster = %self.target.cluster,
                untracked = ?observed.untracked,
                unexplained,
                "session state SprigDB can't replay: pinning the backend connection"
            );
            self.pins.set_untracked();
        }
        if !cur.stmt.sets_gateway_vars.is_empty()
            && cur.error.is_none()
            && let Err(e) = self.restore_gateway_vars(&mut cur).await
        {
            tracing::warn!(node = %node, error = %e, "restoring the gateway's session variables failed; closing the connection");
            let o = outcome(&cur, false);
            drop(cur);
            self.connection_gone();
            return Ok(o);
        }
        // MariaDB 11.8 reports another variable (`timestamp`, on a
        // connection with a fixed clock: a held branch, or a client's `SET
        // timestamp`) in place of `last_gtid` after a commit. A statement
        // that may have committed and reported variables but no GTID reads
        // the GTID itself, or the session's next read could go to a
        // replica that hasn't applied its own commit (R3, agent r3d).
        let committing = cur.stmt.effects.contains(&Effect::Commit)
            || (cur.stmt.class == StmtClass::Write
                && !cur
                    .status
                    .is_some_and(|st| st.contains(StatusFlags::IN_TRANS)));
        if committing
            && cur.error.is_none()
            && observed.gtid.is_none()
            && !cur.tracked.vars.is_empty()
        {
            match cur.lease.conn().capture_last_gtid().await {
                Ok(g) if !g.is_empty() => observed.gtid = Some(g),
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(node = %node, error = %e, "reading last_gtid after a commit failed; closing the connection");
                    let o = outcome(&cur, false);
                    drop(cur);
                    self.connection_gone();
                    return Ok(o);
                }
            }
            metrics::counter!("sprig_backend_gtid_reads_total", "cluster" => self.target.cluster.to_string())
                .increment(1);
        }
        if let Some(g) = observed
            .gtid
            .as_deref()
            .and_then(|g| GtidSet::parse(g).ok())
        {
            self.causal.union(&g);
            self.backends
                .watermarks
                .advance(&self.target.cluster, &self.target.user, &g);
        }
        if let Some(ok) = &cur.last_ok {
            if ok.last_insert_id != 0 && !cur.explicit_ids {
                self.vars.last_insert_id = ok.last_insert_id;
                cur.lease.conn().applied.last_insert_id = ok.last_insert_id;
            }
            self.captured.row_count = if cur.resultset {
                -1
            } else {
                i64::try_from(ok.affected_rows).unwrap_or(i64::MAX)
            };
        }
        for r in self.pins.apply_effects(&cur.stmt.effects) {
            self.count_pin(r);
        }
        if let Some(st) = cur.status
            && self.pins.after_statement(st)
        {
            self.count_pin(PinReason::Transaction);
        }
        for v in &cur.stmt.assigns_user_vars {
            if !self.pending_capture.contains(v) {
                self.pending_capture.push(v.clone());
            }
        }

        let route = cur.route;
        if dirty_after_use(
            cur.sets_schema,
            cur.lease.conn().applied.schema.as_deref(),
            self.vars.schema.as_deref(),
        ) {
            // A failed `USE` (unknown database, access denied, killed) on a
            // connection that had another default schema: the connection
            // still has it and the session doesn't, so it must not serve
            // this session again, or anyone: close it.
            tracing::debug!(node = %node, "a USE failed on a connection with another default schema; closing it");
            self.pins.lose_connection();
            let o = outcome(&cur, false);
            drop(cur);
            return Ok(o);
        }
        if route.reader() && !self.pins.is_empty() {
            // State appeared on a reader: it can't follow the session to the
            // writer. Drop that connection and keep the session conservative.
            tracing::warn!(
                node = %node,
                pins = ?self.pins.reasons(),
                tracked = ?cur.tracked,
                "unexpected session state on a reader; discarding it"
            );
            self.pins.lose_connection();
            self.pins.set_untracked();
            let o = outcome(&cur, true);
            drop(cur);
            return Ok(o);
        }
        if !self.pins.is_empty() {
            let o = outcome(&cur, true);
            self.held = Some((cur.lease, route));
            return Ok(o);
        }
        let mut o = outcome(&cur, false);
        let mut ok = self.capture(&mut cur).await;
        if let Some(before) = cur.probe.filter(|_| ok && cur.error.is_none()) {
            match cur.lease.conn().probe_end(before).await {
                Ok(n) => {
                    o.examined = Some(n);
                    metrics::counter!("sprig_workload_probes_run_total").increment(1);
                }
                Err(e) => {
                    tracing::debug!(node = %node, error = %e, "Handler_read_* probe failed; not measuring");
                    ok = !e.is_connection_lost();
                }
            }
        }
        let limits = self.backends.limits(&self.target.cluster);
        self.backends.pools.release(
            cur.lease,
            if ok { Release::Reuse } else { Release::Discard },
            limits,
        );
        Ok(o)
    }

    /// The captures before a release; false if one failed (the
    /// connection is then discarded).
    async fn capture(&mut self, cur: &mut Current) -> bool {
        let conn = cur.lease.conn();
        let mut ran = false;
        let mut ok = true;
        if cur.warnings > 0 {
            ran = true;
            match conn.capture_warnings().await {
                Ok(w) => self.captured.warnings = w,
                Err(_) => ok = false,
            }
        } else {
            self.captured.warnings.clear();
        }
        if cur.stmt.calc_found_rows && ok {
            ran = true;
            match conn.capture_found_rows().await {
                Ok(n) => self.captured.found_rows = n,
                Err(_) => ok = false,
            }
        } else if cur.resultset {
            self.captured.found_rows = cur.rows;
        }
        if !self.pending_capture.is_empty() && ok {
            ran = true;
            let names = std::mem::take(&mut self.pending_capture);
            match conn.capture_user_vars(&names).await {
                Ok(vals) => {
                    for (n, v) in vals {
                        conn.applied.user_vars.insert(n.clone(), v.clone());
                        self.vars.user_vars.insert(n, v);
                    }
                }
                Err(_) => ok = false,
            }
        }
        if ran {
            metrics::counter!("sprig_backend_state_syncs_total", "cluster" => self.target.cluster.to_string(), "kind" => "capture")
                .increment(1);
        }
        conn.client.take_changes();
        ok
    }

    /// `SET STATEMENT` quirks of MariaDB's tracking: it reports the
    /// temporary values as changes, though they revert when the statement
    /// ends, and reports each revert later, with the next statement on that
    /// connection that changes any session state. Neither is a change by
    /// the statement at hand: temporary values are dropped, and so are
    /// reverts of variables outside the allowed list, which would pin the
    /// session `Untracked` (an allowed variable's revert is its true value
    /// and applies as usual). A `CALL` may change any variable itself, so
    /// its reports are kept. Returns whether a report was dropped.
    fn drop_set_statement_reports(cur: &mut Current, observed: &mut state::Observed) -> bool {
        let applied = &mut cur.lease.conn().applied;
        let pending = std::mem::take(&mut applied.scoped_pending);
        let scoped = &cur.stmt.scoped_vars;
        let call = cur.stmt.effects.contains(&Effect::Call);
        let before = observed.vars.len() + observed.untracked.len();
        observed.vars.retain(|(n, _)| !scoped.contains(n));
        observed
            .untracked
            .retain(|n| !scoped.contains(n) && (call || !pending.contains(n)));
        let reported = |n: &String| cur.tracked.vars.iter().any(|(r, _)| r == n);
        applied.scoped_pending = pending
            .into_iter()
            .filter(|n| !reported(n) || scoped.contains(n))
            .chain(scoped.iter().cloned())
            .collect();
        observed.vars.len() + observed.untracked.len() != before
    }

    /// A client statement set gateway-owned variables: keep the client's
    /// values for the gateway to answer, put the gateway's back on the
    /// connection, and resync the allowed variables tracking may have
    /// missed while the client's settings were in force.
    async fn restore_gateway_vars(
        &mut self,
        cur: &mut Current,
    ) -> std::result::Result<(), ClientError> {
        let names = cur.stmt.sets_gateway_vars.clone();
        let conn = cur.lease.conn();
        let restored = conn.restore_gateway_vars(&names).await?;
        for (n, v) in restored.client {
            self.vars.gateway_vars.insert(n, v);
        }
        let base = Arc::clone(&conn.baseline);
        for (n, v) in restored.allowed {
            let have = conn
                .applied
                .vars
                .get(&n)
                .map(String::as_str)
                .or(base.get(&n));
            if have != Some(v.as_str()) {
                conn.applied.vars.insert(n.clone(), v.clone());
                self.vars.vars.insert(n, v);
            }
        }
        conn.applied.wsrep_sync_wait = 0;
        Ok(())
    }

    /// The session's connection is gone outside a statement: what it held
    /// is lost, and the next statement reports a lost transaction (1213)
    /// or other pinned state (39033).
    fn connection_gone(&mut self) {
        self.held = None;
        self.pending_capture.clear();
        let in_trx = self.pins.in_transaction();
        let lost_state = self.pins.holds_state();
        self.pins.lose_connection();
        self.deferred = if lost_state {
            Some(SprigError::new(
                SprigCode::SessionStateLost,
                "the backend connection holding this session's state was lost",
            ))
        } else if in_trx {
            Some(trx_rolled_back())
        } else {
            None
        };
    }

    /// The client set `wsrep_sync_wait` itself: it asks for causal reads
    /// across gateways, which the shard's writer gives (decision D13), so
    /// its plain reads stay there.
    fn client_sync_wait(&self) -> bool {
        self.vars
            .gateway_vars
            .get("wsrep_sync_wait")
            .is_some_and(|v| !v.parse::<u32>().is_ok_and(|n| n == 0))
    }

    fn count_pin(&self, r: PinReason) {
        metrics::counter!("sprig_backend_pins_total", "cluster" => self.target.cluster.to_string(), "reason" => r.as_str())
            .increment(1);
    }

    /// The error for a connection lost mid-statement (chapter 06, "Failure
    /// handling"); the session forgets what the connection held.
    fn lost(&mut self, stmt: &StmtInfo, in_trx: bool, relayed: bool, node: &NodeKey) -> SprigError {
        let lost_state = self.pins.holds_state();
        self.pins.lose_connection();
        self.held = None;
        self.pending_capture.clear();
        let (code, kind) = if stmt.commits() || (stmt.class == StmtClass::Write && !in_trx) {
            tracing::warn!(
                event = "backend.outcome_unknown",
                cluster = %self.target.cluster,
                node = %node,
                "backend lost during a commit or an autocommit write"
            );
            (Some(SprigCode::OutcomeUnknown), "outcome_unknown")
        } else if in_trx && !lost_state {
            (None, "transaction_rolled_back")
        } else if lost_state {
            (Some(SprigCode::SessionStateLost), "state_lost")
        } else if relayed {
            (Some(SprigCode::BackendLost), "lost_mid_result")
        } else {
            (Some(SprigCode::BackendUnavailable), "lost")
        };
        metrics::counter!("sprig_backend_failures_total", "cluster" => self.target.cluster.to_string(), "kind" => kind)
            .increment(1);
        match code {
            Some(SprigCode::OutcomeUnknown) if stmt.commits() => SprigError::new(
                SprigCode::OutcomeUnknown,
                "the backend connection was lost during a commit; the outcome is unknown",
            ),
            Some(SprigCode::OutcomeUnknown) => SprigError::new(
                SprigCode::OutcomeUnknown,
                "the backend connection was lost while the statement ran; whether it took effect is unknown",
            ),
            Some(SprigCode::SessionStateLost) => SprigError::new(
                SprigCode::SessionStateLost,
                "the backend connection holding this session's state was lost",
            ),
            Some(SprigCode::BackendLost) => SprigError::new(
                SprigCode::BackendLost,
                "the backend connection was lost while sending the result",
            ),
            Some(c) => SprigError::new(c, "the backend connection was lost"),
            None => trx_rolled_back(),
        }
    }

    /// The error for a held connection the server closed before this
    /// statement was sent (chapter 06, "Failure handling"): nothing ran, so
    /// only what the connection held is lost. 1213 for an open transaction
    /// (rolled back), 39033 for other pinned state.
    fn lost_unsent(&mut self, node: &NodeKey) -> SprigError {
        let lost_state = self.pins.holds_state();
        self.pins.lose_connection();
        self.held = None;
        self.pending_capture.clear();
        let kind = if lost_state {
            "state_lost"
        } else {
            "transaction_rolled_back"
        };
        tracing::debug!(node = %node, kind, "held backend connection lost between statements");
        metrics::counter!("sprig_backend_failures_total", "cluster" => self.target.cluster.to_string(), "kind" => kind)
            .increment(1);
        if lost_state {
            SprigError::new(
                SprigCode::SessionStateLost,
                "the backend connection holding this session's state was lost",
            )
        } else {
            trx_rolled_back()
        }
    }

    /// Whether the session runs with `autocommit = 0`, so its next
    /// statement opens a transaction.
    fn implicit_transactions(&self) -> bool {
        self.vars
            .vars
            .get("autocommit")
            .is_some_and(|v| v.eq_ignore_ascii_case("OFF") || v == "0")
    }

    /// The causal target of a read.
    fn causal_target(&self, settings: &BackendSettings) -> GtidSet {
        let mut t = self.causal.clone();
        if settings.causal_scope == CausalScope::TenantGateway {
            t.union(
                &self
                    .backends
                    .watermarks
                    .get(&self.target.cluster, &self.target.user),
            );
        }
        t
    }

    async fn lease(
        &self,
        view: &ClusterView,
        node: &NodeRef,
        settings: &BackendSettings,
        deadline: Instant,
    ) -> std::result::Result<BackendLease, AcquireError<ClientError>> {
        let b = &self.backends;
        let key = PoolKey {
            cluster: view.id.clone(),
            node: node.node.clone(),
            account: self.target.user.clone(),
            found_rows: self.opts.found_rows,
            mariadb_caps: self.opts.mariadb_caps.bits(),
        };
        let node_key = view.key(&node.node);
        let budget = b.budget(&node_key);
        let vars = &self.vars;
        let started = Instant::now();
        let r = b
            .pools
            .acquire(
                &key,
                // The gateway keeps wsrep_sync_wait at 0: the Galera check
                // sets it for one statement only.
                |base| vars.digest(0, base),
                b.limits(&view.id),
                deadline,
                || async {
                    let cred = self
                        .target
                        .credential()
                        .map_err(|e| ClientError::Auth(e.message))?;
                    let tls = b.tls_for(&cred).map_err(|e| ClientError::Tls(e.message))?;
                    let spec = ConnectSpec {
                        node: node_key.clone(),
                        addr: node.addr.clone(),
                        user: cred.user.clone(),
                        password: zeroize::Zeroizing::new(cred.password().to_owned()),
                        tls,
                        found_rows: self.opts.found_rows,
                        mariadb_caps: self.opts.mariadb_caps,
                        galera: view.topology == TopologyKind::Galera,
                        compress: false,
                        connect_timeout: settings.connect_timeout,
                        net_write_timeout_secs: settings.net_write_timeout_secs,
                        wait_timeout_secs: settings.idle_timeout.as_secs().max(300) + 300,
                    };
                    let r = conn::connect(&spec, Arc::clone(&budget)).await;
                    let outcome = if r.is_ok() { "ok" } else { "error" };
                    metrics::counter!("sprig_backend_connects_total", "cluster" => view.id.to_string(), "outcome" => outcome)
                        .increment(1);
                    if let Ok(c) = &r {
                        budget.set_limit(c.max_prepared, settings.prepared_budget_pct, b.gateways_in_dc);
                    }
                    r
                },
            )
            .await;
        metrics::histogram!("sprig_backend_pool_wait_seconds", "cluster" => view.id.to_string())
            .record(started.elapsed().as_secs_f64());
        r
    }

    /// Chooses the node (writer, or a reader for a plain read) and leases a
    /// connection to it. Readers that can't be reached are skipped; the
    /// writer is retried until the deadline.
    #[allow(clippy::too_many_lines)]
    async fn route_lease(
        &self,
        view: &ClusterView,
        info: &StmtInfo,
        settings: &BackendSettings,
        force_writer: bool,
        exclude: &mut Vec<NodeId>,
        deadline: Instant,
    ) -> Result<(BackendLease, RouteKind, Wait)> {
        // Chapter 28, "Locks" (R4 round 4): on Galera, user-level lock
        // functions run on one node of the cluster, the writer of
        // `LOCK_NODE_KEY` that every gateway's rendezvous agrees on, so
        // sessions on any gateway (and any shard) meet on the same locks.
        // Lock names are scoped per tenant, so tenants share the node.
        let shard = if info.user_lock && view.topology == TopologyKind::Galera {
            LOCK_NODE_KEY
        } else {
            self.opts.shard
        };
        // With autocommit = 0 every statement, a plain read too, opens a
        // transaction (chapter 06, "Pins"): it belongs on the writer, where
        // the rest of that transaction runs.
        let plain = info.class == StmtClass::Read
            && self.pins.is_empty()
            && !force_writer
            && !self.implicit_transactions()
            && !self.client_sync_wait();
        let mut last_err: Option<String> = None;
        loop {
            let writer = self.backends.writers.writer(view, shard);
            let (node, route, wait) = if plain {
                let target = self.causal_target(settings);
                let pool = &self.backends.pools;
                let load = |k: &NodeKey| pool.node_counts(k).1;
                let choice = reads::choose(
                    view,
                    writer.as_ref().ok(),
                    &self.backends.nodes,
                    &load,
                    &target,
                    settings,
                    exclude,
                    self.backends
                        .read_spread
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                );
                metrics::counter!(
                    "sprig_backend_reads_total",
                    "cluster" => view.id.to_string(),
                    "target" => choice.as_str(),
                    "causal" => if target.is_empty() { "no" } else { "yes" }
                )
                .increment(1);
                match choice {
                    ReadTarget::Writer => match &writer {
                        Ok(w) => (w.clone(), RouteKind::Writer, Wait::None),
                        Err(e) => {
                            if Instant::now() >= deadline {
                                return Err(unavailable(
                                    last_err.unwrap_or_else(|| e.message.clone()),
                                ));
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    },
                    ReadTarget::Replica { node, wait_for } => match wait_for {
                        Some(t) => (node, RouteKind::ReplicaWait, Wait::Replica(t)),
                        None => (node, RouteKind::Replica, Wait::None),
                    },
                    ReadTarget::GaleraNode {
                        node,
                        sync_wait,
                        confirmed,
                    } => {
                        let galera_writer =
                            writer.as_ref().ok().is_some_and(|w| w.node == node.node);
                        if sync_wait && !confirmed {
                            (node, RouteKind::GaleraUnconfirmed, Wait::Galera)
                        } else if sync_wait {
                            (node, RouteKind::GaleraSyncWait, Wait::Galera)
                        } else if galera_writer {
                            (node, RouteKind::Writer, Wait::None)
                        } else {
                            (node, RouteKind::Galera, Wait::None)
                        }
                    }
                }
            } else {
                match writer {
                    Ok(w) => (w, RouteKind::Writer, Wait::None),
                    Err(e) => {
                        if Instant::now() >= deadline {
                            return Err(unavailable(last_err.unwrap_or(e.message)));
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }
            };
            match self.lease(view, &node, settings, deadline).await {
                Ok(l) => return Ok((l, route, wait)),
                Err(AcquireError::Timeout) => return Err(pool_timeout()),
                Err(AcquireError::Connect(e)) => {
                    if let Some(err) = e.server_error() {
                        // The node answered: a login or setup problem, not a
                        // node failure.
                        return Err(unavailable(format!(
                            "backend login to {} failed: {} {}",
                            node.node, err.code, err.message
                        )));
                    }
                    let key = view.key(&node.node);
                    tracing::debug!(node = %key, error = %e, "backend connect failed");
                    self.backends.note_failure(&key, "connect");
                    last_err = Some(format!("{}: {e}", node.addr));
                    if route != RouteKind::Writer {
                        exclude.push(node.node.clone());
                        continue;
                    }
                    if Instant::now() >= deadline {
                        return Err(unavailable(last_err.unwrap_or_default()));
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    /// [`run_statement`](Self::run_statement) inside a `backend.query`
    /// span (chapter 47), adding its time to the request's backend time.
    async fn run(&mut self, info: &StmtInfo, req: Req<'_>) -> Result<()> {
        use tracing::Instrument as _;
        let span = tracing::info_span!(
            target: sprig_common::telemetry::trace::TARGET,
            "backend.query",
            cluster = %self.target.cluster,
            node = tracing::field::Empty,
            route = tracing::field::Empty,
            error = tracing::field::Empty,
        );
        let started = Instant::now();
        let r = self.run_statement(info, req).instrument(span.clone()).await;
        crate::observe::backend_time(started.elapsed());
        if r.is_err() {
            span.record("error", true);
        }
        if let Some(c) = self.cur.as_mut() {
            span.record("node", tracing::field::display(c.lease.key().node_key()));
            span.record("route", tracing::field::debug(c.route));
            c.span = Some(span);
        }
        r
    }

    #[allow(clippy::too_many_lines)]
    async fn run_statement(&mut self, info: &StmtInfo, mut req: Req<'_>) -> Result<()> {
        if self.cur.is_some() {
            let _ = self.finish().await;
        }
        self.inflight = None;
        if let Some(e) = self.deferred.take() {
            return Err(e);
        }
        let cluster = self.target.cluster.clone();
        let view = self.backends.view(&cluster).await?;
        self.backends.ensure_monitor(&view, &self.target);
        let settings = self.backends.settings(&cluster);
        let deadline = Instant::now() + settings.acquire_timeout;
        let in_trx_before = self.pins.in_transaction();
        let mut exclude: Vec<NodeId> = Vec::new();
        let mut force_writer = false;
        let mut sync_retried = false;
        let mut io_retried = false;
        let text = matches!(req, Req::Text(_));
        // `USE x` sets the schema itself, so any pooled connection serves
        // it, including one that already has a default schema.
        let sets_schema = matches!(req, Req::Text(sql) if is_use(sql));
        // Running it twice changes nothing: a lost connection retries it
        // (chapter 06, "Failure handling").
        let harmless = info.class == StmtClass::Read || matches!(req, Req::Prepare { .. });
        let explicit_ids = std::mem::take(&mut self.explicit_ids_next);
        let probe = std::mem::take(&mut self.probe_next)
            && matches!(req, Req::Text(_) | Req::Binary { .. });
        loop {
            let (mut lease, route, wait) = match self.held.take() {
                Some((mut l, _)) => {
                    if l.conn().client.closed_while_idle() {
                        // The server closed the held connection between
                        // statements (KILL CONNECTION, a restart): this
                        // statement never reached it.
                        let node = l.key().node_key();
                        drop(l);
                        tracing::debug!(node = %node, "held backend connection closed while idle");
                        if self.pins.in_transaction() || self.pins.holds_state() {
                            return Err(self.lost_unsent(&node));
                        }
                        // Only the session mode held it: the state replays.
                        continue;
                    }
                    (l, RouteKind::Pinned, Wait::None)
                }
                None => {
                    self.route_lease(&view, info, &settings, force_writer, &mut exclude, deadline)
                        .await?
                }
            };
            let node_key = lease.key().node_key();
            self.inflight = Some((node_key.clone(), lease.conn_ref().thread_id));
            let conn = lease.conn();
            if self.vars.schema.is_none()
                && conn.applied.schema.is_some()
                && route != RouteKind::Pinned
                && !sets_schema
            {
                // MariaDB can't unset the default schema: this connection can't
                // serve a session without one. New connections have none.
                // (Before M0.7 this also dropped the connection for the
                // session's own `USE`, so every session opened with a default
                // database closed a pooled connection and opened a new one.)
                drop(lease);
                continue;
            }
            // The probe's first reading, on a connection that will be free
            // again after the statement (see `probe_next_statement`).
            let probe_before = if probe && route != RouteKind::Pinned && self.pins.is_empty() {
                match conn.probe_start().await {
                    Ok(v) => Some(v),
                    Err(e) => {
                        tracing::debug!(node = %node_key, error = %e, "Handler_read_* probe failed; not measuring");
                        if e.is_connection_lost() {
                            drop(lease);
                            continue;
                        }
                        None
                    }
                }
            } else {
                None
            };
            let base = Arc::clone(&conn.baseline);
            self.vars.settle_login(&base);
            let (prefix, next_applied) =
                match state::sync_prefix(&conn.applied, &base, &self.vars, info, 0) {
                    Ok(x) => x,
                    Err(state::Unreplayable(why)) => {
                        tracing::warn!(%why, "session state can't be replayed");
                        drop(lease);
                        self.pins.lose_connection();
                        return Err(SprigError::new(
                            SprigCode::SessionStateLost,
                            "this session's state can't be replayed on a backend connection",
                        ));
                    }
                };
            let kind = match (prefix.is_empty(), text) {
                (true, _) => "none",
                (false, true) => "prefix",
                (false, false) => "round_trip",
            };
            metrics::counter!("sprig_backend_state_syncs_total", "cluster" => cluster.to_string(), "kind" => kind)
                .increment(1);

            // Sync (and wait), then send.
            let wait_started = Instant::now();
            let galera_timeout = settings.galera_sync_wait_timeout;
            let sent: std::result::Result<Sent, ClientError> = async {
                if let Req::Text(sql) = &req {
                    let mut all = Vec::with_capacity(sql.len() + 256);
                    for p in &prefix {
                        all.extend_from_slice(p.as_bytes());
                        all.extend_from_slice(b"; ");
                    }
                    match &wait {
                        Wait::Replica(t) => {
                            all.extend_from_slice(
                                reads::wait_prefix(t, settings.gtid_wait_timeout).as_bytes(),
                            );
                            all.extend_from_slice(b"; ");
                        }
                        Wait::Galera => {
                            all.extend_from_slice(reads::GALERA_SYNC_CHECK.as_bytes());
                            all.extend_from_slice(b"; ");
                        }
                        Wait::None => {}
                    }
                    // Sampled statements carry their trace context to
                    // MariaDB (chapter 47, "Propagation"), after
                    // fingerprinting.
                    if let Some(tc) = sprig_common::telemetry::trace::current()
                        && tc.sampled
                    {
                        all.extend_from_slice(b"/* traceparent='");
                        all.extend_from_slice(tc.traceparent().as_bytes());
                        all.extend_from_slice(b"' */ ");
                    }
                    all.extend_from_slice(sql);
                    conn.client.send_query(&all).await?;
                    for _ in 0..prefix.len() {
                        if let Some(e) = consume_statement(conn).await? {
                            return Ok(Sent::SyncErr(e));
                        }
                    }
                    return Ok(match &wait {
                        Wait::None => Sent::Ready,
                        Wait::Replica(_) => match consume_statement(conn).await? {
                            None => Sent::Ready,
                            Some(e) if e.code == reads::WAIT_TIMEOUT_ERRNO => Sent::WaitFailed,
                            Some(e) => Sent::SyncErr(e),
                        },
                        Wait::Galera => {
                            match tokio::time::timeout(galera_timeout, consume_statement(conn))
                                .await
                            {
                                Err(_) => Sent::WaitTimedOut,
                                Ok(r) => match r? {
                                    None => Sent::Ready,
                                    Some(_) => Sent::WaitFailed,
                                },
                            }
                        }
                    });
                }
                if !prefix.is_empty() {
                    match conn.client.query(&prefix.join("; ")).await {
                        Ok(_) => {}
                        Err(ClientError::Server(e)) => return Ok(Sent::SyncErr(e)),
                        Err(e) => return Err(e),
                    }
                }
                match &wait {
                    Wait::None => Ok(Sent::Ready),
                    Wait::Replica(t) => {
                        let row = conn
                            .client
                            .query_first(&reads::wait_query(t, settings.gtid_wait_timeout))
                            .await?;
                        let v = row
                            .and_then(|r| r.into_iter().next().flatten())
                            .map(|b| String::from_utf8_lossy(&b).into_owned());
                        Ok(if v.as_deref() == Some("0") {
                            Sent::Ready
                        } else {
                            Sent::WaitFailed
                        })
                    }
                    Wait::Galera => {
                        match tokio::time::timeout(
                            galera_timeout,
                            conn.client.query_drop(reads::GALERA_SYNC_CHECK),
                        )
                        .await
                        {
                            Err(_) => Ok(Sent::WaitTimedOut),
                            Ok(Ok(())) => Ok(Sent::Ready),
                            Ok(Err(ClientError::Server(_))) => Ok(Sent::WaitFailed),
                            Ok(Err(e)) => Err(e),
                        }
                    }
                }
            }
            .await;
            let waited = !matches!(wait, Wait::None);
            match sent {
                Ok(Sent::Ready) => {}
                Ok(Sent::WaitFailed | Sent::WaitTimedOut) => {
                    // The read reruns on the writer (chapter 06: "Replica too
                    // slow", and decisions D13 and D14 for Galera).
                    metrics::counter!("sprig_backend_gtid_wait_timeouts_total", "cluster" => cluster.to_string())
                        .increment(1);
                    metrics::histogram!("sprig_backend_gtid_wait_seconds", "cluster" => cluster.to_string())
                        .record(wait_started.elapsed().as_secs_f64());
                    if matches!(sent, Ok(Sent::WaitTimedOut)) {
                        // Still waiting on the server: never reuse it.
                        tracing::debug!(node = %node_key, "Galera sync-wait check timed out; reading from the writer");
                        drop(lease);
                    } else {
                        // The sync statements before the wait ran.
                        conn.applied = next_applied;
                        conn.client.take_changes();
                        let limits = self.backends.limits(&cluster);
                        self.backends.pools.release(lease, Release::Reuse, limits);
                    }
                    force_writer = true;
                    continue;
                }
                Ok(Sent::SyncErr(e)) => {
                    metrics::counter!("sprig_backend_failures_total", "cluster" => cluster.to_string(), "kind" => "sync")
                        .increment(1);
                    drop(lease);
                    if route == RouteKind::Pinned {
                        let err = backend_err(&e);
                        self.pins.lose_connection();
                        return Err(SprigError::new(SprigCode::SessionStateLost, err.message));
                    }
                    if !sync_retried {
                        sync_retried = true;
                        continue;
                    }
                    return Err(backend_err(&e));
                }
                Err(e) => {
                    drop(lease);
                    if let Some(r) = self.retry_after_loss(
                        info,
                        harmless,
                        route,
                        &node_key,
                        &e,
                        &mut io_retried,
                        &mut exclude,
                        in_trx_before,
                    ) {
                        return Err(r);
                    }
                    continue;
                }
            }
            if waited {
                metrics::histogram!("sprig_backend_gtid_wait_seconds", "cluster" => cluster.to_string())
                    .record(wait_started.elapsed().as_secs_f64());
            }
            conn.applied = next_applied;
            conn.client.take_changes();

            // Prepared statements and other commands go out now.
            let outgoing = match &mut req {
                Req::Text(_) => Ok(()),
                Req::Binary { sql, build } => match conn.prepare_cached(sql).await {
                    Ok(st) => match build(st) {
                        Ok(payload) => {
                            st.types_sent = true;
                            conn.client.send_command_payload(payload).await
                        }
                        Err(e) => {
                            let limits = self.backends.limits(&cluster);
                            self.backends.pools.release(lease, Release::Reuse, limits);
                            return Err(SprigError::from(e));
                        }
                    },
                    Err(ClientError::Server(e)) => {
                        let limits = self.backends.limits(&cluster);
                        if route == RouteKind::Pinned {
                            self.held = Some((lease, route));
                        } else {
                            self.backends.pools.release(lease, Release::Reuse, limits);
                        }
                        return Err(backend_err(&e));
                    }
                    Err(e) => Err(e),
                },
                Req::Command(payload) => conn.client.send_command_payload(payload.clone()).await,
                Req::Prepare { sql, out } => match conn.prepare_cached(sql).await {
                    Ok(st) => {
                        **out = Some(st.clone());
                        Ok(())
                    }
                    Err(ClientError::Server(e)) => {
                        let limits = self.backends.limits(&cluster);
                        if route == RouteKind::Pinned {
                            self.held = Some((lease, route));
                        } else {
                            self.backends.pools.release(lease, Release::Reuse, limits);
                        }
                        return Err(backend_err(&e));
                    }
                    Err(e) => Err(e),
                },
            };
            let prepared_only = matches!(req, Req::Prepare { .. });
            let mut cur = Current {
                lease,
                stmt: info.clone(),
                route,
                first: None,
                relayed: false,
                done: false,
                failed: false,
                in_trx_before,
                status: None,
                warnings: 0,
                last_ok: None,
                error: None,
                tracked: TrackedChanges::default(),
                rows: 0,
                resultset: false,
                probe: probe_before,
                sets_schema,
                explicit_ids,
                span: None,
            };
            let first = match outgoing {
                // A prepare has no response beyond what prepare_cached read.
                Ok(()) if prepared_only => Ok(None),
                Ok(()) => {
                    // Park the statement while it runs: if this future is
                    // dropped (KILL QUERY), `abandon` still has the
                    // connection to kill and drain, and an open transaction
                    // survives.
                    self.cur = Some(cur);
                    let r = match self.cur.as_mut() {
                        Some(c) => c.lease.conn().client.next_packet().await,
                        None => Err(ClientError::Closed),
                    };
                    let Some(c) = self.cur.take() else {
                        return Err(SprigError::internal("the statement in flight vanished"));
                    };
                    cur = c;
                    r
                }
                Err(e) => Err(e),
            };
            let fatal_first = matches!(
                &first,
                Ok(Some(RespPacket { kind: PacketKind::Err(e), .. })) if conn_fatal(e.code)
            );
            if fatal_first
                && harmless
                && route != RouteKind::Pinned
                && self.pins.is_empty()
                && !io_retried
            {
                // The server killed the connection before any result: a
                // plain read retries once elsewhere, as for an I/O error.
                drop(cur);
                io_retried = true;
                exclude.push(node_key.node.clone());
                continue;
            }
            match first {
                Ok(first) => {
                    cur.done = first.is_none();
                    if let Some(p) = &first {
                        cur.observe(p);
                    }
                    cur.first = first;
                    self.cur = Some(cur);
                    self.inflight = None;
                    return Ok(());
                }
                Err(e) if e.is_connection_lost() => {
                    drop(cur);
                    if let Some(r) = self.retry_after_loss(
                        info,
                        harmless,
                        route,
                        &node_key,
                        &e,
                        &mut io_retried,
                        &mut exclude,
                        in_trx_before,
                    ) {
                        return Err(r);
                    }
                }
                Err(e) => return Err(internal(&e)),
            }
        }
    }

    /// A connection lost before the statement's first packet: a plain read
    /// or a prepare (`harmless`: running it twice changes nothing) of an
    /// unpinned session retries once elsewhere (`None`); anything else
    /// fails with the mapped error.
    #[allow(clippy::too_many_arguments)]
    fn retry_after_loss(
        &mut self,
        info: &StmtInfo,
        harmless: bool,
        route: RouteKind,
        node: &NodeKey,
        e: &ClientError,
        io_retried: &mut bool,
        exclude: &mut Vec<NodeId>,
        in_trx: bool,
    ) -> Option<SprigError> {
        tracing::debug!(node = %node, error = %e, "backend connection lost before the first packet");
        self.backends.note_failure(node, "io");
        let retry = harmless && route != RouteKind::Pinned && self.pins.is_empty() && !*io_retried;
        if retry {
            *io_retried = true;
            exclude.push(node.node.clone());
            return None;
        }
        if harmless && (in_trx || self.pins.holds_state()) {
            // A prepare never changes data: only what the connection held
            // is lost, never the outcome of a write.
            return Some(self.lost_unsent(node));
        }
        Some(self.lost(info, in_trx, false, node))
    }

    /// The executor dropped the future of a statement (`KILL QUERY`, chapter
    /// 04's `abandoned`): `KILL QUERY` on its backend thread, then end it.
    /// A statement with a response under way is drained (up to `drain`), so
    /// the connection and an open transaction survive, as on MySQL. One
    /// dropped before its first packet took its connection with it: an
    /// open transaction or pinned state is then lost, and the session's
    /// next statement reports it (1213 or 39033) instead of running
    /// outside the transaction the client thinks it has.
    pub async fn abandon(&mut self, drain: Duration) {
        if let Err(e) = tokio::time::timeout(Duration::from_secs(3), self.kill_query())
            .await
            .unwrap_or_else(|_| Err(unavailable("KILL QUERY timed out")))
        {
            tracing::debug!(error = %e, "KILL QUERY for an abandoned statement failed");
        }
        if self.cur.is_some() {
            match tokio::time::timeout(drain, self.finish()).await {
                Ok(Ok(_)) => return,
                Ok(Err(e)) => {
                    tracing::debug!(error = %e, "abandoned statement ended with an error");
                    return;
                }
                // finish() had taken the statement: its connection is gone.
                Err(_) => tracing::warn!(
                    "abandoned backend statement didn't end; its connection is closed"
                ),
            }
        } else if self.inflight.take().is_none() {
            return;
        }
        // The connection is gone (dropped mid-send, or drained too slowly).
        self.held = None;
        self.pending_capture.clear();
        let in_trx = self.pins.in_transaction();
        let lost_state = self.pins.holds_state();
        self.pins.lose_connection();
        self.deferred = if lost_state {
            Some(SprigError::new(
                SprigCode::SessionStateLost,
                "the backend connection holding this session's state was lost when a statement was killed",
            ))
        } else if in_trx {
            Some(SprigError::standard(
                1213,
                *b"40001",
                "Deadlock found when trying to get lock; try restarting transaction \
                 (the transaction was rolled back when a statement was killed)",
            ))
        } else {
            None
        };
    }

    /// `COM_RESET_CONNECTION` or `COM_CHANGE_USER`: a held connection gets
    /// `COM_RESET_CONNECTION` and the baseline, then returns to the pool;
    /// the session's variables, user variables, pins and captures are
    /// cleared (the schema stays).
    ///
    /// # Errors
    /// Never fails the session: a connection that can't be reset is closed.
    pub async fn reset(&mut self) -> Result<()> {
        if self.cur.is_some() {
            let _ = self.finish().await;
        }
        if let Some((mut lease, _)) = self.held.take() {
            let ok = lease.conn().reset().await.is_ok();
            let limits = self.backends.limits(&self.target.cluster);
            self.backends.pools.release(
                lease,
                if ok { Release::Reuse } else { Release::Discard },
                limits,
            );
        }
        let schema = self.vars.schema.clone();
        self.vars = SessionVars::login(self.opts.persona, self.opts.collation, schema);
        let session_mode = self.pins.reasons().contains(&PinReason::SessionMode);
        self.pins = Pins::default();
        self.pins.set_session_mode(session_mode);
        self.captured = Captured::default();
        self.pending_capture.clear();
        self.deferred = None;
        self.inflight = None;
        Ok(())
    }

    /// Points the session at another cluster: its tenant moved (chapter
    /// 10, "Cutover"). A held connection is discarded (the old cluster
    /// rolls back an open transaction, which the next statement reports as
    /// 1213, as for any lost connection); tracked session state is replayed
    /// on the next connection, which comes from the new cluster's pools.
    /// The causal position named the old cluster's GTIDs and is dropped.
    pub fn retarget(&mut self, cluster: sprig_common::ids::ClusterId) {
        if self.target.cluster == cluster {
            return;
        }
        drop(self.cur.take());
        if let Some((lease, _)) = self.held.take() {
            let limits = self.backends.limits(&self.target.cluster);
            self.backends.pools.release(lease, Release::Discard, limits);
            self.connection_gone();
            // Chapter 24: state that can't follow the tenant to another
            // DC ends the session with 39502 (SQLSTATE 08S01).
            let other_dc = sprig_cluster::registry::dc_of(&self.target.cluster)
                != sprig_cluster::registry::dc_of(&cluster);
            if other_dc
                && self
                    .deferred
                    .as_ref()
                    .is_some_and(|e| e.code() == Some(SprigCode::SessionStateLost))
            {
                self.deferred = Some(SprigError::new(
                    SprigCode::SessionMoved,
                    format!(
                        "the tenant moved to data center {}; this session held state that \
                         can't follow it (temporary tables, locks or XA): reconnect",
                        sprig_cluster::registry::dc_of(&cluster)
                            .map_or_else(|| "?".to_owned(), |d| d.to_string())
                    ),
                ));
            }
        }
        self.causal = GtidSet::default();
        tracing::info!(from = %self.target.cluster, to = %cluster, "session follows its tenant to another cluster");
        self.target.cluster = cluster;
    }

    /// Ends the session: a held connection is closed (the server rolls back
    /// an open transaction), anything in flight is abandoned.
    #[allow(clippy::unused_async)] // async in the API: closing may send COM_QUIT later
    pub async fn close(&mut self) {
        if let Some(cur) = self.cur.take() {
            drop(cur);
        }
        if let Some((lease, _)) = self.held.take() {
            let limits = self.backends.limits(&self.target.cluster);
            self.backends.pools.release(lease, Release::Discard, limits);
        }
    }
}

/// Reads one statement's response of a multi-statement prefix; returns its
/// ERR, if any.
async fn consume_statement(
    conn: &mut conn::BackendConn,
) -> std::result::Result<Option<ErrPacket>, ClientError> {
    loop {
        match conn.client.next_packet().await? {
            Some(p) => match p.kind {
                PacketKind::Ok(_) | PacketKind::End(_) => return Ok(None),
                PacketKind::Err(e) => return Ok(Some(e)),
                _ => {}
            },
            None => {
                return Err(ClientError::Unexpected(
                    "the response ended inside the sync prefix".to_owned(),
                ));
            }
        }
    }
}

#[cfg(test)]
mod use_tests {
    use super::{dirty_after_use, is_use};

    #[test]
    fn use_statements_are_recognized() {
        for (sql, yes) in [
            (&b"USE shop"[..], true),
            (b"  use\tshop", true),
            (b"USE", false),
            (b"USER()", false),
            (b"SELECT 1", false),
            (b"used", false),
        ] {
            assert_eq!(is_use(sql), yes, "{}", String::from_utf8_lossy(sql));
        }
    }

    #[test]
    fn a_failed_use_on_another_schema_closes_the_connection() {
        // USE missing on a connection that had t1_a: the connection keeps
        // t1_a, the session has none: close it.
        assert!(dirty_after_use(true, Some("t1_a"), None));
        // USE t1_b succeeded: both agree.
        assert!(!dirty_after_use(true, Some("t1_b"), Some("t1_b")));
        // USE failed on a connection without a schema, for a session without one.
        assert!(!dirty_after_use(true, None, None));
        // Not a USE: the routing guard handles schema mismatches.
        assert!(!dirty_after_use(false, Some("t1_a"), None));
    }
}
