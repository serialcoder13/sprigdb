//! Sessions (chapter 04, "Session"; build step 5): the [`Session`] a
//! connection task owns, the [`SessionTable`] behind `SHOW PROCESSLIST` and
//! `KILL`, connection-id allocation, the connection task wrapper with panic
//! isolation ([`SessionTable::run`]) and client-disconnect detection
//! ([`peer_closed`], [`watch_statement`]).
//!
//! How a front end uses it:
//!
//! ```text
//! serve(conn, ctx):
//!   ctx.sessions.run(conn, Protocol::MySql84, |mut session, conn| async move {
//!       greeting with session.id ... login ...
//!       session.set_state(SessionState::Ready);
//!       loop {
//!           if session.should_close() {
//!               send flags().close_reason()'s error() where the protocol allows; break
//!           }
//!           read a command; session.begin_command("Query", Some(text))
//!           match watch_statement(session.handle(), &conn.stream, run_it).await {
//!               Done(r) => reply,
//!               ClientGone => KILL QUERY on the backend, discard it, return,
//!               Killed => KILL QUERY on the backend, ERR 1317,
//!           }
//!           session.end_command()
//!       }
//!   })
//! ```

mod disconnect;
pub mod redact;
mod table;
mod task;

use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_common::ids::{ConnId, DcId, TenantId, UserId};
use sprig_common::{CancellationToken, SprigCode, SprigError};
use tokio::sync::Notify;

use crate::frontend::{Protocol, TlsInfo};

pub use disconnect::{StatementEnd, client_gone, peer_closed, tcp_of, watch_statement};
pub use table::SessionTable;
pub use task::PANICS_METRIC;

/// Bits of a connection id that hold the gateway's registration slot.
pub const SLOT_BITS: u32 = 12;
/// Bits of a connection id that hold the per-gateway counter.
pub const COUNTER_BITS: u32 = 20;
/// The largest slot: 4096 gateways per DC.
pub const MAX_SLOT: u16 = (1 << SLOT_BITS) - 1;
/// The largest counter value.
pub const MAX_COUNTER: u32 = (1 << COUNTER_BITS) - 1;

/// The connection id for `slot` and `counter`: `slot << 20 | counter`
/// (chapter 04: a 32-bit MySQL connection id).
///
/// # Panics
/// Never: both parts are masked to their widths.
#[must_use]
pub const fn conn_id(slot: u16, counter: u32) -> ConnId {
    ConnId(((slot & MAX_SLOT) as u32) << COUNTER_BITS | (counter & MAX_COUNTER))
}

/// The registration slot a connection id was allocated under.
#[must_use]
pub const fn slot_of(id: ConnId) -> u16 {
    // The shift leaves 12 bits, which fit a u16.
    #[allow(clippy::cast_possible_truncation)]
    let slot = (id.0 >> COUNTER_BITS) as u16;
    slot
}

/// The counter part of a connection id.
#[must_use]
pub const fn counter_of(id: ConnId) -> u32 {
    id.0 & MAX_COUNTER
}

/// Where a session is in its life (chapter 04, "State machines").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionState {
    /// Accepted; the protocol handshake is running.
    Handshake,
    /// The client sent credentials; the login is being decided.
    Authenticating,
    /// Logged in, outside a transaction.
    Ready,
    /// Inside a transaction (MySQL: the backend's `SERVER_STATUS_IN_TRANS`).
    InTransaction,
    /// Closing: rolling back, releasing the backend, leaving the table.
    Closing,
}

impl SessionState {
    /// The log and `SHOW PROCESSLIST` name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            SessionState::Handshake => "handshake",
            SessionState::Authenticating => "authenticating",
            SessionState::Ready => "ready",
            SessionState::InTransaction => "in_transaction",
            SessionState::Closing => "closing",
        }
    }
}

impl fmt::Display for SessionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Who a session logged in as: chapter 14's principal (tenant, user id,
/// host, sandbox flag, backend target). Accounts-file logins have no
/// tenant.
pub use sprig_auth::Principal;

/// Why a session was asked to close at its next safe point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    /// The gateway is draining (chapter 04, "Drain"): 39000.
    Drain,
    /// The session's listener moved, was disabled or tightened (chapter 04,
    /// "Live listener changes"): 39004.
    ListenerRetired,
    /// The session's account was dropped or locked (chapter 14,
    /// `auth.close_sessions_on_revoke`): 39306.
    Revoked,
    /// A distributed `COMMIT`'s outcome is unknown (chapter 18): 39441.
    OutcomeUnknown,
}

impl CloseReason {
    /// The error to send where the protocol allows one before closing.
    #[must_use]
    pub fn error(self) -> SprigError {
        match self {
            CloseReason::Drain => {
                SprigError::new(SprigCode::GatewayDraining, "the gateway is shutting down")
            }
            CloseReason::ListenerRetired => SprigError::new(
                SprigCode::ListenerRetired,
                "this listener changed; reconnect",
            ),
            CloseReason::Revoked => SprigError::new(
                SprigCode::SessionRevoked,
                "this session was closed: its account was dropped or locked",
            ),
            CloseReason::OutcomeUnknown => SprigError::new(
                SprigCode::CommitOutcomeUnknown,
                "the last COMMIT's outcome is unknown; this connection is closed",
            ),
        }
    }

    const fn to_u8(self) -> u8 {
        match self {
            CloseReason::Drain => 1,
            CloseReason::ListenerRetired => 2,
            CloseReason::Revoked => 3,
            CloseReason::OutcomeUnknown => 4,
        }
    }

    const fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(CloseReason::Drain),
            2 => Some(CloseReason::ListenerRetired),
            3 => Some(CloseReason::Revoked),
            4 => Some(CloseReason::OutcomeUnknown),
            _ => None,
        }
    }
}

/// The kill and close requests other tasks make of a session (chapter 04:
/// `kill_query`, `kill_conn`, `close_at_safe_point`).
#[derive(Debug, Default)]
pub struct SessionFlags {
    kill_query: AtomicBool,
    kill_conn: CancellationToken,
    /// 0, or a [`CloseReason`]; the first reason set is kept.
    close_at_safe_point: AtomicU8,
    wake: Notify,
}

impl SessionFlags {
    /// `KILL QUERY`: stop the running statement; the session stays.
    pub fn kill_query(&self) {
        self.kill_query.store(true, Ordering::SeqCst);
        self.wake.notify_waiters();
    }

    /// Whether `KILL QUERY` (or `KILL`) is pending.
    #[must_use]
    pub fn kill_query_requested(&self) -> bool {
        self.kill_query.load(Ordering::SeqCst)
    }

    /// Clears a pending `KILL QUERY` and says whether there was one. A front
    /// end calls it when a statement starts (a kill aimed at the previous
    /// statement doesn't stop this one) and when it reports the kill.
    pub fn take_kill_query(&self) -> bool {
        self.kill_query.swap(false, Ordering::SeqCst)
    }

    /// `KILL [CONNECTION]`: stop the statement and close the connection.
    /// [`SessionTable::run`] drops the session's task as soon as this is
    /// set.
    pub fn kill_conn(&self) {
        self.kill_query.store(true, Ordering::SeqCst);
        self.kill_conn.cancel();
        self.wake.notify_waiters();
    }

    /// Whether the connection was killed.
    #[must_use]
    pub fn conn_killed(&self) -> bool {
        self.kill_conn.is_cancelled()
    }

    /// Resolves when the connection is killed.
    pub async fn killed(&self) {
        self.kill_conn.cancelled().await;
    }

    /// Ask the session to close at its next safe point: between statements
    /// and outside a transaction (chapter 04). The first reason given is
    /// kept.
    pub fn close_at_safe_point(&self, reason: CloseReason) {
        let _ = self.close_at_safe_point.compare_exchange(
            0,
            reason.to_u8(),
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        self.wake.notify_waiters();
    }

    /// Whether a close at the next safe point was asked for.
    #[must_use]
    pub fn close_requested(&self) -> bool {
        self.close_reason().is_some()
    }

    /// Why the session should close at its next safe point, if it should.
    #[must_use]
    pub fn close_reason(&self) -> Option<CloseReason> {
        CloseReason::from_u8(self.close_at_safe_point.load(Ordering::SeqCst))
    }

    /// Resolves once a close at the next safe point is asked for (or the
    /// connection is killed). A front end waiting idle for the next command
    /// selects on it, so an idle session closes at once (chapter 04, "Drain"
    /// step 3).
    pub async fn close_wanted(&self) {
        loop {
            let notified = self.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.close_requested() || self.conn_killed() {
                return;
            }
            notified.await;
        }
    }

    /// Resolves once a `KILL QUERY` or `KILL` is pending.
    pub async fn query_killed(&self) {
        loop {
            let notified = self.wake.notified();
            tokio::pin!(notified);
            // Register before checking, so a kill between the check and the
            // wait isn't missed.
            notified.as_mut().enable();
            if self.kill_query_requested() {
                return;
            }
            notified.await;
        }
    }
}

/// What `SHOW PROCESSLIST` shows of a session while it runs.
#[derive(Debug, Clone, Default)]
struct Activity {
    user: Option<String>,
    db: Option<String>,
    command: &'static str,
    info: Option<String>,
    since: Option<Instant>,
    tenant: Option<TenantId>,
    /// The logged-in catalog user, for revocation (chapter 14).
    user_id: Option<UserId>,
}

/// A session's shared half: what the [`SessionTable`] holds and other tasks
/// see (`SHOW PROCESSLIST`, `KILL`, drain). The connection task owns the
/// [`Session`].
pub struct SessionHandle {
    id: ConnId,
    protocol: Protocol,
    listener: &'static str,
    listener_gen: u64,
    peer: SocketAddr,
    connected_at: Instant,
    flags: SessionFlags,
    state: Mutex<SessionState>,
    activity: Mutex<Activity>,
}

impl fmt::Debug for SessionHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionHandle")
            .field("id", &self.id)
            .field("protocol", &self.protocol)
            .field("listener", &self.listener)
            .field("listener_gen", &self.listener_gen)
            .field("peer", &self.peer)
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

impl SessionHandle {
    /// The connection id.
    #[must_use]
    pub fn id(&self) -> ConnId {
        self.id
    }

    /// The protocol.
    #[must_use]
    pub fn protocol(&self) -> Protocol {
        self.protocol
    }

    /// The listener the connection arrived on.
    #[must_use]
    pub fn listener(&self) -> &'static str {
        self.listener
    }

    /// The listener generation at accept.
    #[must_use]
    pub fn listener_gen(&self) -> u64 {
        self.listener_gen
    }

    /// The client address, after the PROXY header.
    #[must_use]
    pub fn peer(&self) -> SocketAddr {
        self.peer
    }

    /// The kill and close flags.
    #[must_use]
    pub fn flags(&self) -> &SessionFlags {
        &self.flags
    }

    /// The current state.
    #[must_use]
    pub fn state(&self) -> SessionState {
        *self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn activity(&self) -> std::sync::MutexGuard<'_, Activity> {
        self.activity.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The `SHOW PROCESSLIST` row as of `now`.
    #[must_use]
    pub fn process_row(&self, now: Instant) -> ProcessRow {
        let a = self.activity().clone();
        let since = a.since.unwrap_or(self.connected_at);
        ProcessRow {
            id: self.id,
            tenant: a.tenant,
            user: a.user,
            host: self.peer,
            db: a.db,
            command: if a.command.is_empty() {
                "Connect"
            } else {
                a.command
            },
            time: now.saturating_duration_since(since),
            state: self.state(),
            info: a.info,
            protocol: self.protocol,
            listener: self.listener,
            listener_gen: self.listener_gen,
        }
    }
}

/// One `SHOW PROCESSLIST` (or Redis `CLIENT LIST`) row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessRow {
    /// `Id`.
    pub id: ConnId,
    /// The tenant, for catalog logins.
    pub tenant: Option<TenantId>,
    /// `User`; `None` before login.
    pub user: Option<String>,
    /// `Host`: the client address.
    pub host: SocketAddr,
    /// `db`.
    pub db: Option<String>,
    /// `Command`: `Sleep`, `Query`, `Connect`, ...
    pub command: &'static str,
    /// `Time`: since the current command started, or since connect.
    pub time: Duration,
    /// The session state (`State`).
    pub state: SessionState,
    /// `Info`: the running statement's text.
    pub info: Option<String>,
    /// The protocol.
    pub protocol: Protocol,
    /// The listener.
    pub listener: &'static str,
    /// The listener generation at accept.
    pub listener_gen: u64,
}

/// One client session, owned by its connection task (chapter 04,
/// "Session"). It leaves the [`SessionTable`] when it is dropped and, under
/// [`SessionTable::run`], when the connection task ends.
///
/// Fields whose types arrive with later steps are placeholders (`Option<()>`,
/// always `None`), each naming the chapter that defines it.
pub struct Session {
    /// The connection id: `slot << 20 | counter`.
    pub id: ConnId,
    /// The protocol.
    pub protocol: Protocol,
    /// The client address, after the PROXY header.
    pub peer: SocketAddr,
    /// Version, cipher and SNI; `None` for plaintext. A MySQL front end
    /// updates it after its in-protocol TLS upgrade.
    pub tls: Option<TlsInfo>,
    /// The listener generation at accept.
    pub listener_gen: u64,
    /// The client's DC, a placement hint only (chapter 24).
    pub client_dc: Option<DcId>,
    /// Who the session logged in as (interim; see [`Principal`]).
    pub principal: Option<Principal>,
    /// TODO(chapter 08): `TenantHandle`, an `Arc` snapshot of the tenant
    /// catalog (M0.3).
    pub tenant: Option<()>,
    /// TODO(chapter 06): `SessionVars`, the tracked session variables.
    pub vars: Option<()>,
    /// TODO(chapter 18): `TxnState`.
    pub txn: Option<()>,
    /// TODO(chapter 06): `PinSet`, what pins the session to a backend
    /// connection.
    pub pins: Option<()>,
    /// TODO(chapter 06): `BackendLease`, the leased backend connection.
    pub backend: Option<()>,
    /// TODO(chapter 04 step 7): `MemCharge`, memory charged to the gateway,
    /// tenant and user.
    pub mem: Option<()>,
    /// Statements the front end answered itself (`SELECT @@version_comment`,
    /// `KILL`, `SHOW PROCESSLIST`), and how many were `SELECT`s: counted
    /// in the session's `SHOW STATUS` (gaps row 46).
    pub local_questions: u64,
    /// See [`Self::local_questions`].
    pub local_selects: u64,
    /// The MySQL client's connection attributes (`_client_name`,
    /// `program_name`, ...), as kept after `gateway.max_connect_attrs_bytes`:
    /// 3306's `performance_schema.session_connect_attrs` (chapter 28, R4
    /// round 4).
    pub connect_attrs: Vec<(String, String)>,
    state: SessionState,
    handle: Arc<SessionHandle>,
    registration: Arc<Registration>,
}

/// A session's entry in the table: removed when the last holder drops it,
/// the [`Session`] or [`SessionTable::run`]. So the entry lives as long as
/// the connection task, even if a front end drops its `Session` early.
struct Registration {
    handle: Arc<SessionHandle>,
    table: Arc<SessionTable>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.table.remove_handle(&self.handle);
    }
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("id", &self.id)
            .field("protocol", &self.protocol)
            .field("state", &self.state)
            .field("peer", &self.peer)
            .field("listener_gen", &self.listener_gen)
            .field("principal", &self.principal)
            .finish_non_exhaustive()
    }
}

impl Session {
    /// The current state.
    #[must_use]
    pub fn state(&self) -> SessionState {
        self.state
    }

    /// Moves to `state`; `SHOW PROCESSLIST` sees it at once.
    pub fn set_state(&mut self, state: SessionState) {
        self.state = state;
        *self
            .handle
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = state;
    }

    /// The shared half, for [`watch_statement`] and other tasks.
    #[must_use]
    pub fn handle(&self) -> &Arc<SessionHandle> {
        &self.handle
    }

    /// The kill and close flags (chapter 04's `flags`).
    #[must_use]
    pub fn flags(&self) -> &SessionFlags {
        &self.handle.flags
    }

    /// Whether the session should close now that it is at a safe point:
    /// killed, or asked to close at a safe point while outside a
    /// transaction.
    #[must_use]
    pub fn should_close(&self) -> bool {
        let f = self.flags();
        f.conn_killed() || (f.close_requested() && self.state != SessionState::InTransaction)
    }

    /// Records the login: the principal and the `User` column.
    pub fn set_principal(&mut self, principal: Principal) {
        {
            let mut a = self.handle.activity();
            a.user = Some(principal.user.clone());
            a.tenant = principal.tenant;
            a.user_id = principal.user_id;
        }
        self.principal = Some(principal);
    }

    /// Records the default database (the `db` column).
    pub fn set_db(&self, db: Option<String>) {
        self.handle.activity().db = db;
    }

    /// A command starts: `Command` and `Info` show it, and `Time` restarts.
    /// Clears a `KILL QUERY` aimed at an earlier statement. Passwords and
    /// credentials in `info` are shown as `<secret>` ([`redact::info`]).
    pub fn begin_command(&self, command: &'static str, info: Option<String>) {
        self.flags().take_kill_query();
        let info = info.map(|i| match redact::info(&i) {
            std::borrow::Cow::Borrowed(_) => i,
            std::borrow::Cow::Owned(r) => r,
        });
        let now = self.registration.table.clock().now();
        let mut a = self.handle.activity();
        a.command = command;
        a.info = info;
        a.since = Some(now);
    }

    /// The command ended: the session sleeps.
    pub fn end_command(&self) {
        let now = self.registration.table.clock().now();
        let mut a = self.handle.activity();
        a.command = "Sleep";
        a.info = None;
        a.since = Some(now);
    }
}

#[cfg(test)]
mod tests;
