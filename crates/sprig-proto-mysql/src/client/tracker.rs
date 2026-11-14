//! Session-state tracking on the client side: what the server reported in the
//! session-state blocks of its OK packets.

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::buf::Reader;
use crate::packets::{SessionChange, session_track};
use crate::proto_error::ProtoResult;

/// Decodes a session-state block as servers send it.
///
/// Unlike [`SessionChange::decode_all`], a `STATE_CHANGE` entry's data is
/// read as the bare `'1'` that MariaDB and MySQL send (entry length 1); a
/// length-prefixed `'1'` is accepted too.
pub fn decode_session_state(block: &Bytes) -> ProtoResult<Vec<SessionChange>> {
    let mut r = Reader::new(block);
    let mut out = Vec::new();
    while !r.is_empty() {
        let kind = r.u8()?;
        let data = block.slice_ref(r.lenenc_bytes()?);
        if kind == session_track::STATE_CHANGE {
            let on = match &data[..] {
                [b] | [1, b] => *b == b'1',
                _ => false,
            };
            out.push(SessionChange::StateChange(on));
            continue;
        }
        let mut one = Vec::with_capacity(data.len() + 10);
        one.push(kind);
        crate::buf::put_lenenc_int(&mut one, data.len() as u64);
        one.extend_from_slice(&data);
        out.extend(SessionChange::decode_all(&Bytes::from(one))?);
    }
    Ok(out)
}

/// Changes reported by one or more OK packets, merged in order (a later
/// value wins).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrackedChanges {
    /// The new default schema.
    pub schema: Option<String>,
    /// System variables and their new values, lower-case names, in report
    /// order; a variable appears once, with its last value.
    pub vars: Vec<(String, String)>,
    /// GTID of the last commit: MariaDB's `last_gtid` variable or MySQL's
    /// GTID tracker.
    pub gtid: Option<String>,
    /// The 8-character transaction state (`session_track_transaction_info`).
    pub trx_state: Option<String>,
    /// `SET TRANSACTION` statements that recreate the characteristics.
    pub trx_characteristics: Option<String>,
    /// The server flagged a change of some tracked state
    /// (`session_track_state_change`).
    pub state_changed: bool,
}

impl TrackedChanges {
    /// Whether nothing was reported.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// The reported value of `var` (lower case), if any.
    #[must_use]
    pub fn var(&self, var: &str) -> Option<&str> {
        self.vars
            .iter()
            .find(|(n, _)| n == var)
            .map(|(_, v)| v.as_str())
    }

    /// Adds `later` on top of these changes.
    pub fn merge(&mut self, later: TrackedChanges) {
        if later.schema.is_some() {
            self.schema = later.schema;
        }
        for (n, v) in later.vars {
            match self.vars.iter_mut().find(|(k, _)| *k == n) {
                Some(slot) => slot.1 = v,
                None => self.vars.push((n, v)),
            }
        }
        if later.gtid.is_some() {
            self.gtid = later.gtid;
        }
        if later.trx_state.is_some() {
            self.trx_state = later.trx_state;
        }
        if later.trx_characteristics.is_some() {
            self.trx_characteristics = later.trx_characteristics;
        }
        self.state_changed |= later.state_changed;
    }
}

/// The session state a connection's server has reported so far.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionTracker {
    schema: Option<String>,
    vars: BTreeMap<String, String>,
    last_gtid: Option<String>,
    trx_state: Option<String>,
    trx_characteristics: Option<String>,
}

impl SessionTracker {
    /// An empty tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies one OK packet's entries; returns what they changed.
    pub fn apply(&mut self, changes: &[SessionChange]) -> TrackedChanges {
        let mut out = TrackedChanges::default();
        let text = |b: &[u8]| String::from_utf8_lossy(b).into_owned();
        for c in changes {
            match c {
                SessionChange::SystemVariable { name, value } => {
                    let name = text(name).to_ascii_lowercase();
                    let value = text(value);
                    if name == "last_gtid" && !value.is_empty() {
                        self.last_gtid = Some(value.clone());
                        out.gtid = Some(value.clone());
                    }
                    self.vars.insert(name.clone(), value.clone());
                    match out.vars.iter_mut().find(|(k, _)| *k == name) {
                        Some(slot) => slot.1 = value,
                        None => out.vars.push((name, value)),
                    }
                }
                SessionChange::Schema(s) => {
                    let s = text(s);
                    self.schema = Some(s.clone());
                    out.schema = Some(s);
                }
                SessionChange::StateChange(on) => out.state_changed |= *on,
                SessionChange::Gtids { gtids, .. } => {
                    let g = text(gtids);
                    if !g.is_empty() {
                        self.last_gtid = Some(g.clone());
                        out.gtid = Some(g);
                    }
                }
                SessionChange::TransactionCharacteristics(s) => {
                    let s = text(s);
                    self.trx_characteristics = Some(s.clone());
                    out.trx_characteristics = Some(s);
                }
                SessionChange::TransactionState(s) => {
                    let s = text(s);
                    self.trx_state = Some(s.clone());
                    out.trx_state = Some(s);
                }
                SessionChange::Unknown { .. } => {}
            }
        }
        out
    }

    /// The default schema last reported.
    #[must_use]
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// The last reported value of a system variable (lower-case name).
    #[must_use]
    pub fn var(&self, name: &str) -> Option<&str> {
        self.vars.get(name).map(String::as_str)
    }

    /// Every system variable reported so far.
    #[must_use]
    pub fn vars(&self) -> &BTreeMap<String, String> {
        &self.vars
    }

    /// The GTID of the last commit reported.
    #[must_use]
    pub fn last_gtid(&self) -> Option<&str> {
        self.last_gtid.as_deref()
    }

    /// The last transaction state reported, such as `T_______`.
    #[must_use]
    pub fn trx_state(&self) -> Option<&str> {
        self.trx_state.as_deref()
    }

    /// The last transaction characteristics reported.
    #[must_use]
    pub fn trx_characteristics(&self) -> Option<&str> {
        self.trx_characteristics.as_deref()
    }

    /// Forgets everything (after `COM_RESET_CONNECTION`).
    pub fn clear(&mut self) {
        *self = Self::default();
    }
}
