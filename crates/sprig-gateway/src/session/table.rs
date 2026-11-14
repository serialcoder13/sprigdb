//! The gateway's sessions: connection-id
//! allocation, lookup, `SHOW PROCESSLIST`, `KILL`, and the per-generation
//! counts the listener manager's drain needs.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use sprig_common::ids::{ConnId, TenantId, UserId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigError};

use super::{
    Activity, CloseReason, MAX_COUNTER, MAX_SLOT, ProcessRow, Registration, Session, SessionFlags,
    SessionHandle, SessionState, conn_id,
};
use crate::frontend::{ClientConn, Protocol, TlsInfo};

/// Every open session of this gateway.
pub struct SessionTable {
    clock: Arc<dyn Clock>,
    /// The registration slot; 0 until registered.
    slot: AtomicU16,
    /// The last counter handed out.
    counter: Mutex<u32>,
    sessions: RwLock<HashMap<ConnId, Arc<SessionHandle>>>,
}

impl fmt::Debug for SessionTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionTable")
            .field("slot", &self.slot())
            .field("sessions", &self.len())
            .finish_non_exhaustive()
    }
}

impl SessionTable {
    /// An empty table on slot 0.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            slot: AtomicU16::new(0),
            counter: Mutex::new(0),
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// The clock sessions are timed with.
    #[must_use]
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    /// The registration slot new ids are allocated under.
    #[must_use]
    pub fn slot(&self) -> u16 {
        self.slot.load(Ordering::SeqCst)
    }

    /// Sets the slot registration claimed. Sessions
    /// already open keep their ids.
    ///
    /// # Errors
    /// `Internal` if `slot` is over 4095.
    pub fn set_slot(&self, slot: u16) -> Result<()> {
        if slot > MAX_SLOT {
            return Err(SprigError::internal(format!(
                "gateway slot {slot} is over {MAX_SLOT}"
            )));
        }
        self.slot.store(slot, Ordering::SeqCst);
        Ok(())
    }

    /// Opens a session for an accepted connection: allocates its id and
    /// enters it in the table in state `Handshake`.
    ///
    /// # Errors
    /// MySQL 1040 (`Too many connections`, 08004) if every id of the slot is
    /// in use.
    pub fn open(self: &Arc<Self>, conn: &ClientConn, protocol: Protocol) -> Result<Session> {
        self.open_parts(
            protocol,
            conn.listener.name,
            conn.generation,
            conn.peer,
            conn.tls_info(),
        )
    }

    /// [`SessionTable::open`] from parts, for callers without a
    /// [`ClientConn`] (tests, the HTTP front end's per-connection sessions).
    ///
    /// # Errors
    /// As [`SessionTable::open`].
    pub fn open_parts(
        self: &Arc<Self>,
        protocol: Protocol,
        listener: &'static str,
        listener_gen: u64,
        peer: SocketAddr,
        tls: Option<TlsInfo>,
    ) -> Result<Session> {
        let slot = self.slot();
        let mut sessions = self
            .sessions
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let mut counter = self.counter.lock().unwrap_or_else(PoisonError::into_inner);
        // Counters run 1..=MAX_COUNTER and wrap, skipping ids in use; 0 is
        // never used, so no session has id 0.
        let mut id = None;
        for _ in 0..MAX_COUNTER {
            *counter = if *counter >= MAX_COUNTER {
                1
            } else {
                *counter + 1
            };
            let candidate = conn_id(slot, *counter);
            if !sessions.contains_key(&candidate) {
                id = Some(candidate);
                break;
            }
        }
        drop(counter);
        let Some(id) = id else {
            return Err(SprigError::standard(
                1040,
                *b"08004",
                "Too many connections",
            ));
        };
        let handle = Arc::new(SessionHandle {
            id,
            protocol,
            listener,
            listener_gen,
            peer,
            connected_at: self.clock.now(),
            flags: SessionFlags::default(),
            state: Mutex::new(SessionState::Handshake),
            activity: Mutex::new(Activity::default()),
        });
        sessions.insert(id, Arc::clone(&handle));
        drop(sessions);
        Ok(Session {
            id,
            protocol,
            peer,
            tls,
            listener_gen,
            client_dc: None,
            principal: None,
            tenant: None,
            vars: None,
            txn: None,
            pins: None,
            local_questions: 0,
            local_selects: 0,
            connect_attrs: Vec::new(),
            backend: None,
            mem: None,
            state: SessionState::Handshake,
            registration: Arc::new(Registration {
                handle: Arc::clone(&handle),
                table: Arc::clone(self),
            }),
            handle,
        })
    }

    /// The session with `id`, if open.
    #[must_use]
    pub fn get(&self, id: ConnId) -> Option<Arc<SessionHandle>> {
        self.read().get(&id).cloned()
    }

    /// Removes `id`'s entry. Dropping the [`Session`] does this already.
    pub fn remove(&self, id: ConnId) -> Option<Arc<SessionHandle>> {
        self.sessions
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&id)
    }

    /// Removes `handle`'s entry, and only it: an id reused since is kept.
    pub(crate) fn remove_handle(&self, handle: &Arc<SessionHandle>) {
        let mut sessions = self
            .sessions
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if sessions
            .get(&handle.id)
            .is_some_and(|h| Arc::ptr_eq(h, handle))
        {
            sessions.remove(&handle.id);
        }
    }

    /// How many sessions are open.
    #[must_use]
    pub fn len(&self) -> usize {
        self.read().len()
    }

    /// Whether no session is open.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `SHOW PROCESSLIST`: one row per session, by id.
    #[must_use]
    pub fn list(&self) -> Vec<ProcessRow> {
        let now = self.clock.now();
        let handles: Vec<_> = self.read().values().cloned().collect();
        let mut rows: Vec<ProcessRow> = handles.iter().map(|h| h.process_row(now)).collect();
        rows.sort_by_key(|r| r.id);
        rows
    }

    /// `KILL QUERY id`. `false` if no such session. Privilege checks are
    /// the caller's.
    pub fn kill_query(&self, id: ConnId) -> bool {
        self.get(id).map(|h| h.flags().kill_query()).is_some()
    }

    /// `KILL [CONNECTION] id`. `false` if no such session.
    pub fn kill_conn(&self, id: ConnId) -> bool {
        self.get(id).map(|h| h.flags().kill_conn()).is_some()
    }

    /// Sessions per generation of `listener`.
    #[must_use]
    pub fn count_by_generation(&self, listener: &str) -> BTreeMap<u64, usize> {
        let mut out = BTreeMap::new();
        for h in self.read().values().filter(|h| h.listener == listener) {
            *out.entry(h.listener_gen).or_insert(0) += 1;
        }
        out
    }

    /// Marks `close_at_safe_point` on every session of `listener` accepted
    /// under a generation older than `generation` (a live listener change,
    /// such as a stricter listener). Returns how many.
    pub fn close_older_than(&self, listener: &str, generation: u64) -> usize {
        self.mark(CloseReason::ListenerRetired, |h| {
            h.listener == listener && h.listener_gen < generation
        })
    }

    /// Marks `close_at_safe_point` on every session of `user` of `tenant`
    /// (a dropped or locked account). Returns how many.
    pub fn close_user(&self, tenant: TenantId, user: UserId) -> usize {
        self.mark(CloseReason::Revoked, |h| {
            let a = h.activity();
            a.tenant == Some(tenant) && a.user_id == Some(user)
        })
    }

    /// Marks `close_at_safe_point` on every session of `tenant` (a suspended
    /// tenant). Returns how many.
    pub fn close_tenant(&self, tenant: TenantId) -> usize {
        self.mark(CloseReason::Suspended, |h| {
            h.activity().tenant == Some(tenant)
        })
    }

    /// Marks `close_at_safe_point` on every session (the gateway is
    /// draining). Returns how many.
    pub fn close_all_at_safe_point(&self) -> usize {
        self.mark(CloseReason::Drain, |_| true)
    }

    fn mark(&self, reason: CloseReason, which: impl Fn(&SessionHandle) -> bool) -> usize {
        let handles: Vec<_> = self.read().values().cloned().collect();
        let mut n = 0;
        for h in handles.iter().filter(|h| which(h)) {
            h.flags().close_at_safe_point(reason);
            n += 1;
        }
        n
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<ConnId, Arc<SessionHandle>>> {
        self.sessions.read().unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(test)]
    pub(crate) fn set_counter(&self, value: u32) {
        *self.counter.lock().unwrap_or_else(PoisonError::into_inner) = value;
    }
}
