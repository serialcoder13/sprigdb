//! Pins (chapter 06, "Pinning and pool modes"; build step 4): while a
//! session has state that can't move, it holds its backend connection.

use std::collections::BTreeSet;

use sprig_proto_mysql::StatusFlags;

use super::classify::Effect;

/// Why a session holds its backend connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PinReason {
    /// A transaction is open (or `SET TRANSACTION` waits for the next one).
    Transaction,
    /// Temporary tables exist.
    TempTables(u32),
    /// `LOCK TABLES` is in force.
    TableLocks,
    /// `GET_LOCK()` locks are held.
    UserLocks(u32),
    /// Server-side cursors are open.
    Cursor(u32),
    /// `HANDLER ... OPEN`.
    Handler,
    /// A client `XA` transaction.
    ClientXa,
    /// State SprigDB can't replay; lasts until the session ends.
    Untracked,
    /// `backend.pool_mode = session`.
    SessionMode,
    /// A compiled logic routine is running (chapter 41, R7).
    Routine,
}

impl PinReason {
    /// The metric label.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            PinReason::Transaction => "transaction",
            PinReason::TempTables(_) => "temp_tables",
            PinReason::TableLocks => "table_locks",
            PinReason::UserLocks(_) => "user_locks",
            PinReason::Cursor(_) => "cursor",
            PinReason::Handler => "handler",
            PinReason::ClientXa => "client_xa",
            PinReason::Untracked => "untracked",
            PinReason::SessionMode => "session_mode",
            PinReason::Routine => "routine",
        }
    }
}

/// The pins of one session.
#[allow(clippy::struct_excessive_bools)] // one flag per pin reason
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Pins {
    in_trans: bool,
    next_trx: bool,
    temp: BTreeSet<String>,
    table_locks: bool,
    user_locks: u32,
    cursors: u32,
    handlers: u32,
    xa: bool,
    untracked: bool,
    session_mode: bool,
    routine: bool,
}

impl Pins {
    /// Whether nothing pins the session.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.reasons().is_empty()
    }

    /// Every current reason.
    #[must_use]
    pub fn reasons(&self) -> Vec<PinReason> {
        let mut r = Vec::new();
        if self.in_trans || self.next_trx {
            r.push(PinReason::Transaction);
        }
        if !self.temp.is_empty() {
            r.push(PinReason::TempTables(
                u32::try_from(self.temp.len()).unwrap_or(u32::MAX),
            ));
        }
        if self.table_locks {
            r.push(PinReason::TableLocks);
        }
        if self.user_locks > 0 {
            r.push(PinReason::UserLocks(self.user_locks));
        }
        if self.cursors > 0 {
            r.push(PinReason::Cursor(self.cursors));
        }
        if self.handlers > 0 {
            r.push(PinReason::Handler);
        }
        if self.xa {
            r.push(PinReason::ClientXa);
        }
        if self.untracked {
            r.push(PinReason::Untracked);
        }
        if self.session_mode {
            r.push(PinReason::SessionMode);
        }
        if self.routine {
            r.push(PinReason::Routine);
        }
        r
    }

    /// Whether a transaction is open.
    #[must_use]
    pub fn in_transaction(&self) -> bool {
        self.in_trans
    }

    /// Whether anything but an open transaction (and the session mode)
    /// pins: losing the connection then loses state.
    #[must_use]
    pub fn holds_state(&self) -> bool {
        !self.temp.is_empty()
            || self.table_locks
            || self.user_locks > 0
            || self.cursors > 0
            || self.handlers > 0
            || self.xa
            || self.untracked
            || self.routine
            || self.next_trx
    }

    /// Applies a statement's parsed effects, before its outcome is known
    /// (conservative: a `GET_LOCK()` that fails still pins until released).
    /// Returns the pins it added, for metrics.
    pub fn apply_effects(&mut self, effects: &[Effect]) -> Vec<PinReason> {
        let before = self.reasons();
        for e in effects {
            match e {
                Effect::Begin => {
                    self.next_trx = false;
                    // START TRANSACTION releases table locks.
                    self.table_locks = false;
                }
                Effect::Commit | Effect::Rollback | Effect::ImplicitCommit => self.next_trx = false,
                Effect::NextTransaction => self.next_trx = true,
                Effect::CreateTemp(t) => {
                    self.temp.insert(t.to_ascii_lowercase());
                }
                Effect::DropTables(ts) => {
                    for t in ts {
                        self.temp.remove(&t.to_ascii_lowercase());
                    }
                }
                Effect::LockTables => self.table_locks = true,
                Effect::UnlockTables => self.table_locks = false,
                Effect::GetLock => self.user_locks += 1,
                Effect::ReleaseLock => self.user_locks = self.user_locks.saturating_sub(1),
                Effect::ReleaseAllLocks => self.user_locks = 0,
                Effect::HandlerOpen => self.handlers += 1,
                Effect::HandlerClose => self.handlers = self.handlers.saturating_sub(1),
                Effect::XaStart => self.xa = true,
                Effect::XaEnd => self.xa = false,
                Effect::Untracked(_) => self.untracked = true,
                Effect::Call | Effect::SetNames => {}
            }
        }
        let after = self.reasons();
        after
            .into_iter()
            .filter(|r| {
                !before
                    .iter()
                    .any(|b| std::mem::discriminant(b) == std::mem::discriminant(r))
            })
            .collect()
    }

    /// Takes the server's word on the transaction from a statement's final
    /// status flags (`SERVER_STATUS_IN_TRANS`): `COMMIT`, `ROLLBACK` and
    /// implicit commits end it; `autocommit = 0` starts one with the first
    /// statement.
    pub fn after_statement(&mut self, status: StatusFlags) -> bool {
        let was = self.in_trans;
        self.in_trans = status.contains(StatusFlags::IN_TRANS);
        if self.in_trans {
            self.next_trx = false;
        }
        !was && self.in_trans
    }

    /// Pins `Untracked` until the session ends.
    pub fn set_untracked(&mut self) {
        self.untracked = true;
    }

    /// Sets or clears the session-mode pin.
    pub fn set_session_mode(&mut self, on: bool) {
        self.session_mode = on;
    }

    /// Opens a server-side cursor (protocol level).
    pub fn open_cursor(&mut self) {
        self.cursors += 1;
    }

    /// Closes one.
    pub fn close_cursor(&mut self) {
        self.cursors = self.cursors.saturating_sub(1);
    }

    /// Sets or clears the `Routine` pin (chapter 41).
    pub fn set_routine(&mut self, on: bool) {
        self.routine = on;
    }

    /// Forgets everything the lost connection held; the session mode
    /// stays.
    pub fn lose_connection(&mut self) {
        let session_mode = self.session_mode;
        *self = Pins {
            session_mode,
            ..Pins::default()
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pins_start_and_end_with_their_events() {
        let mut p = Pins::default();
        assert!(p.is_empty());
        p.apply_effects(&[Effect::CreateTemp("T".into())]);
        assert_eq!(p.reasons(), vec![PinReason::TempTables(1)]);
        p.apply_effects(&[Effect::DropTables(vec!["other".into()])]);
        assert!(!p.is_empty());
        p.apply_effects(&[Effect::DropTables(vec!["t".into()])]);
        assert!(p.is_empty());

        let added = p.apply_effects(&[Effect::GetLock, Effect::GetLock]);
        assert_eq!(added, vec![PinReason::UserLocks(2)]);
        p.apply_effects(&[Effect::ReleaseLock]);
        assert_eq!(p.reasons(), vec![PinReason::UserLocks(1)]);
        p.apply_effects(&[Effect::ReleaseAllLocks]);
        assert!(p.is_empty());

        p.apply_effects(&[Effect::LockTables]);
        p.apply_effects(&[Effect::Begin]);
        assert!(
            p.reasons().is_empty(),
            "START TRANSACTION releases table locks"
        );
        assert!(p.after_statement(StatusFlags::IN_TRANS));
        assert_eq!(p.reasons(), vec![PinReason::Transaction]);
        assert!(!p.holds_state());
        p.after_statement(StatusFlags::AUTOCOMMIT);
        assert!(p.is_empty());

        p.apply_effects(&[Effect::NextTransaction]);
        p.after_statement(StatusFlags::AUTOCOMMIT);
        assert_eq!(
            p.reasons(),
            vec![PinReason::Transaction],
            "SET TRANSACTION holds"
        );
        p.after_statement(StatusFlags::IN_TRANS);
        p.after_statement(StatusFlags::AUTOCOMMIT);
        assert!(p.is_empty());

        p.set_session_mode(true);
        p.apply_effects(&[Effect::Untracked("x".into())]);
        p.lose_connection();
        assert_eq!(p.reasons(), vec![PinReason::SessionMode]);
    }
}
