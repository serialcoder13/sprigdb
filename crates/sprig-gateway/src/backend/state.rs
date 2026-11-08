//! Session state, its replay and its digest (chapter 06, "Replay rules",
//! "Acquire, sync and release"; build step 3).
//!
//! The client-visible state is [`SessionVars`]; what a backend connection
//! holds is [`AppliedState`]. Before a statement runs on a connection,
//! [`sync_prefix`] lists the statements that make the connection match the
//! session: `USE`, one `SET SESSION` for every allowed variable that
//! differs (a variable the session never set takes the pool's
//! [`Baseline`] value), `wsrep_sync_wait`, and lazily the user variables
//! the statement reads and `LAST_INSERT_ID`. Sync statements use typed
//! literals and quoted identifiers; client text is never pasted in. User
//! variable values travel as hex (`X'..'`, wrapped in `CONVERT(.. USING
//! utf8mb4)` for text), so no escaping is needed.
//!
//! Names pass through unchanged (decision D2): the session's schema is the
//! physical one until chapter 07.

use std::collections::BTreeMap;
use std::hash::Hasher as _;

use sprig_proto_mysql::Persona;
use sprig_proto_mysql::client::TrackedChanges;
use sprig_proto_mysql::resultset::column_type;
use twox_hash::XxHash3_64;

use super::classify::StmtInfo;

/// How a session variable's value is written in `SET SESSION`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VarKind {
    /// `ON` or `OFF`.
    Bool,
    /// A number.
    Number,
    /// Text, possibly `NULL` (an empty tracked value).
    Text,
}

/// The session variables that replay by value (chapter 06's allowed list,
/// plus the character-set variables `SET NAMES` changes).
const ALLOWED: &[(&str, VarKind)] = &[
    ("autocommit", VarKind::Bool),
    ("sql_mode", VarKind::Text),
    ("time_zone", VarKind::Text),
    ("character_set_client", VarKind::Text),
    ("character_set_connection", VarKind::Text),
    ("character_set_results", VarKind::Text),
    ("collation_connection", VarKind::Text),
    ("transaction_isolation", VarKind::Text),
    ("tx_isolation", VarKind::Text),
    ("transaction_read_only", VarKind::Bool),
    ("tx_read_only", VarKind::Bool),
    ("foreign_key_checks", VarKind::Bool),
    ("unique_checks", VarKind::Bool),
    ("max_statement_time", VarKind::Number),
    ("optimizer_switch", VarKind::Text),
    ("sql_select_limit", VarKind::Number),
    ("group_concat_max_len", VarKind::Number),
    ("div_precision_increment", VarKind::Number),
    ("lc_time_names", VarKind::Text),
    ("sql_auto_is_null", VarKind::Bool),
    ("sql_safe_updates", VarKind::Bool),
    ("sql_big_selects", VarKind::Bool),
    ("sql_quote_show_create", VarKind::Bool),
    ("sql_notes", VarKind::Bool),
    ("sql_warnings", VarKind::Bool),
    ("auto_increment_increment", VarKind::Number),
    ("auto_increment_offset", VarKind::Number),
    ("big_tables", VarKind::Bool),
    ("default_storage_engine", VarKind::Text),
    ("default_tmp_storage_engine", VarKind::Text),
    ("character_set_collations", VarKind::Text),
];

/// Variables the gateway owns or that change by themselves: never replayed,
/// never a reason to pin.
const GATEWAY: &[&str] = &[
    "last_gtid",
    "wait_timeout",
    "interactive_timeout",
    "net_write_timeout",
    "net_read_timeout",
    "session_track_system_variables",
    "session_track_schema",
    "session_track_state_change",
    "session_track_transaction_info",
    "session_track_user_variables",
    "redirect_url",
    "wsrep_sync_wait",
    "warning_count",
    "error_count",
];

/// The replay kind of an allowed variable, or `None` if it can't be
/// replayed by value.
#[must_use]
pub fn var_kind(name: &str) -> Option<VarKind> {
    ALLOWED.iter().find(|(n, _)| *n == name).map(|(_, k)| *k)
}

/// Whether the gateway owns `name` (or it changes by itself).
#[must_use]
pub fn gateway_var(name: &str) -> bool {
    GATEWAY.contains(&name)
}

/// The names of every allowed variable.
pub fn allowed_vars() -> impl Iterator<Item = &'static str> {
    ALLOWED.iter().map(|(n, _)| *n)
}

/// A typed user-variable value, as captured with `SELECT @v`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum VarValue {
    /// `NULL` (also an unset variable).
    Null,
    /// A signed or unsigned integer, as decimal text.
    Integer(String),
    /// A `DECIMAL` or floating-point value, as decimal text.
    Decimal(String),
    /// Text in `utf8mb4`.
    Text(Vec<u8>),
    /// A binary string.
    Binary(Vec<u8>),
}

impl VarValue {
    /// The value of a captured column: its type decides the literal.
    #[must_use]
    pub fn from_column(col_type: u8, charset: u16, value: Option<&[u8]>) -> Self {
        let Some(v) = value else {
            return VarValue::Null;
        };
        let numeric = |v: &[u8]| {
            !v.is_empty()
                && v.iter()
                    .all(|c| c.is_ascii_digit() || matches!(c, b'-' | b'+' | b'.' | b'e' | b'E'))
        };
        match col_type {
            column_type::TINY
            | column_type::SHORT
            | column_type::LONG
            | column_type::INT24
            | column_type::LONGLONG
                if numeric(v) =>
            {
                VarValue::Integer(String::from_utf8_lossy(v).into_owned())
            }
            column_type::DECIMAL
            | column_type::NEWDECIMAL
            | column_type::FLOAT
            | column_type::DOUBLE
                if numeric(v) =>
            {
                VarValue::Decimal(String::from_utf8_lossy(v).into_owned())
            }
            _ if charset == 63 => VarValue::Binary(v.to_vec()),
            _ => VarValue::Text(v.to_vec()),
        }
    }

    /// The SQL literal for this value.
    #[must_use]
    pub fn literal(&self) -> String {
        match self {
            VarValue::Null => "NULL".to_owned(),
            VarValue::Integer(s) | VarValue::Decimal(s) => s.clone(),
            VarValue::Text(b) => format!("CONVERT(X'{}' USING utf8mb4)", hex(b)),
            VarValue::Binary(b) => format!("X'{}'", hex(b)),
        }
    }
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02X}");
    }
    s
}

/// A quoted identifier: back quotes, inner ones doubled.
#[must_use]
pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// The literal for an allowed system variable's tracked value, or `None`
/// if the value doesn't fit its kind (then the session can't be synced and
/// pins `Untracked`).
#[must_use]
pub fn sysvar_literal(name: &str, value: &str) -> Option<String> {
    match var_kind(name)? {
        VarKind::Bool => match value.to_ascii_uppercase().as_str() {
            "ON" | "1" | "TRUE" => Some("ON".to_owned()),
            "OFF" | "0" | "FALSE" => Some("OFF".to_owned()),
            _ => None,
        },
        VarKind::Number => {
            let ok = !value.is_empty()
                && value
                    .bytes()
                    .all(|c| c.is_ascii_digit() || matches!(c, b'.' | b'-'));
            ok.then(|| value.to_owned())
        }
        VarKind::Text if value.is_empty() && name == "character_set_results" => {
            Some("NULL".to_owned())
        }
        VarKind::Text => Some(format!(
            "CONVERT(X'{}' USING utf8mb4)",
            hex(value.as_bytes())
        )),
    }
}

/// The values of the allowed variables right after a pool's
/// initialization: what a variable the session never set must be.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Baseline {
    /// Value per allowed variable (lower-case names), as the server
    /// reports it.
    pub vars: BTreeMap<String, String>,
}

impl Baseline {
    /// The baseline value of `name`.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.vars.get(name).map(String::as_str)
    }
}

/// The session state a backend connection holds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AppliedState {
    /// Default schema (physical; D2: as the client named it).
    pub schema: Option<String>,
    /// Allowed variables set away from the baseline (or set at all).
    pub vars: BTreeMap<String, String>,
    /// `wsrep_sync_wait` (Galera).
    pub wsrep_sync_wait: u8,
    /// The backend role (chapter 29); unused until R1.
    pub role: Option<String>,
    /// User variables known to hold these values.
    pub user_vars: BTreeMap<String, VarValue>,
    /// `LAST_INSERT_ID()` on this connection.
    pub last_insert_id: u64,
}

/// The client-visible session state the gateway keeps for one session.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionVars {
    /// Default schema.
    pub schema: Option<String>,
    /// Allowed variables the session has set (or that its persona sets at
    /// login).
    pub vars: BTreeMap<String, String>,
    /// The backend role (chapter 29); unused until R1.
    pub role: Option<String>,
    /// User variables with known values.
    pub user_vars: BTreeMap<String, VarValue>,
    /// `LAST_INSERT_ID()`.
    pub last_insert_id: u64,
}

/// The persona baselines (chapter 06, "New connections").
pub mod persona {
    /// MySQL 8.4's `sql_mode`.
    pub const MYSQL84_SQL_MODE: &str = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,\
        NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";
    /// MariaDB's `sql_mode`.
    pub const MARIADB_SQL_MODE: &str =
        "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION";
    /// The pool's connection collation.
    pub const COLLATION: &str = "utf8mb4_uca1400_ai_ci";
}

/// `(character set, collation)` for a handshake collation id, mapped onto
/// MariaDB names (the first entries of chapter 28's table); unknown ids
/// keep the pool's `utf8mb4` baseline.
#[must_use]
pub fn collation_for(id: u16) -> Option<(&'static str, &'static str)> {
    Some(match id {
        8 => ("latin1", "latin1_swedish_ci"),
        33 => ("utf8mb3", "utf8mb3_general_ci"),
        45 => ("utf8mb4", "utf8mb4_general_ci"),
        46 => ("utf8mb4", "utf8mb4_bin"),
        47 => ("latin1", "latin1_bin"),
        63 => ("binary", "binary"),
        83 => ("utf8mb3", "utf8mb3_bin"),
        192 => ("utf8mb3", "utf8mb3_unicode_ci"),
        224 => ("utf8mb4", "utf8mb4_unicode_ci"),
        // utf8mb4_0900_ai_ci: MariaDB 11.4+ maps it to uca1400.
        255 => ("utf8mb4", "utf8mb4_uca1400_ai_ci"),
        _ => return None,
    })
}

impl SessionVars {
    /// The state of a new session: the persona's `sql_mode` and the client
    /// collation from the handshake, and the schema named at login.
    #[must_use]
    pub fn login(persona: Persona, collation_id: u16, schema: Option<String>) -> Self {
        let mut vars = BTreeMap::new();
        if persona == Persona::MySql84 {
            vars.insert("sql_mode".to_owned(), persona::MYSQL84_SQL_MODE.to_owned());
        }
        if let Some((cs, coll)) = collation_for(collation_id) {
            for v in [
                "character_set_client",
                "character_set_connection",
                "character_set_results",
            ] {
                vars.insert(v.to_owned(), cs.to_owned());
            }
            vars.insert("collation_connection".to_owned(), coll.to_owned());
        }
        Self {
            schema,
            vars,
            ..Self::default()
        }
    }
}

/// The value `name` must have for the session: its own, else the
/// baseline's.
fn want<'a>(vars: &'a BTreeMap<String, String>, base: &'a Baseline, name: &str) -> Option<&'a str> {
    vars.get(name)
        .map(String::as_str)
        .or_else(|| base.get(name))
}

/// The 64-bit digest of the state that sync must match (everything but
/// user variables and `LAST_INSERT_ID`), normalized against `base` so a
/// variable set to its baseline value counts as unset.
#[must_use]
pub fn digest(
    schema: Option<&str>,
    vars: &BTreeMap<String, String>,
    wsrep_sync_wait: u8,
    role: Option<&str>,
    base: &Baseline,
) -> u64 {
    let mut h = XxHash3_64::default();
    h.write(schema.unwrap_or("\u{0}").as_bytes());
    h.write_u8(0xFF);
    for (k, v) in vars {
        if base.get(k) == Some(v.as_str()) {
            continue;
        }
        h.write(k.as_bytes());
        h.write_u8(b'=');
        h.write(v.as_bytes());
        h.write_u8(0xFF);
    }
    h.write_u8(wsrep_sync_wait);
    h.write(role.unwrap_or("").as_bytes());
    h.finish()
}

impl AppliedState {
    /// The digest of this connection's state.
    #[must_use]
    pub fn digest(&self, base: &Baseline) -> u64 {
        digest(
            self.schema.as_deref(),
            &self.vars,
            self.wsrep_sync_wait,
            self.role.as_deref(),
            base,
        )
    }
}

impl SessionVars {
    /// The digest a connection must have to need no sync for this session.
    #[must_use]
    pub fn digest(&self, wsrep_sync_wait: u8, base: &Baseline) -> u64 {
        digest(
            self.schema.as_deref(),
            &self.vars,
            wsrep_sync_wait,
            self.role.as_deref(),
            base,
        )
    }
}

/// Why a session's state can't be replayed onto a connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unreplayable(pub String);

/// The statements that make a connection holding `applied` match the
/// session, and the state it holds once they succeed.
///
/// # Errors
/// [`Unreplayable`] if a variable's value can't be written as a literal.
pub fn sync_prefix(
    applied: &AppliedState,
    base: &Baseline,
    session: &SessionVars,
    stmt: &StmtInfo,
    wsrep_sync_wait: u8,
) -> Result<(Vec<String>, AppliedState), Unreplayable> {
    let mut out = Vec::new();
    let mut next = applied.clone();
    if session.schema != applied.schema {
        if let Some(s) = &session.schema {
            out.push(format!("USE {}", quote_ident(s)));
        }
        // A session without a schema never gets a connection with one:
        // the session layer discards such a connection (MariaDB can't unset
        // the default schema).
        next.schema.clone_from(&session.schema);
        if session.schema.is_none() {
            next.schema.clone_from(&applied.schema);
        }
    }
    let mut sets = Vec::new();
    let names: std::collections::BTreeSet<&String> =
        session.vars.keys().chain(applied.vars.keys()).collect();
    for name in names {
        let w = want(&session.vars, base, name);
        let a = want(&applied.vars, base, name);
        if w == a {
            continue;
        }
        let Some(w) = w else {
            // Nothing to go back to (no baseline): can't replay.
            return Err(Unreplayable(format!("no baseline value for {name}")));
        };
        let lit = sysvar_literal(name, w)
            .ok_or_else(|| Unreplayable(format!("{name} = {w:?} has no literal")))?;
        sets.push(format!("{name} = {lit}"));
        next.vars.insert(name.clone(), w.to_owned());
    }
    if applied.wsrep_sync_wait != wsrep_sync_wait {
        sets.push(format!("wsrep_sync_wait = {wsrep_sync_wait}"));
        next.wsrep_sync_wait = wsrep_sync_wait;
    }
    if !sets.is_empty() {
        out.push(format!("SET SESSION {}", sets.join(", ")));
    }
    if session.role != applied.role {
        out.push(match &session.role {
            Some(r) => format!("SET ROLE {}", quote_ident(r)),
            None => "SET ROLE NONE".to_owned(),
        });
        next.role.clone_from(&session.role);
    }
    let reads: Vec<&String> = if stmt.reads_all_user_vars {
        session
            .user_vars
            .keys()
            .chain(applied.user_vars.keys())
            .collect()
    } else {
        stmt.reads_user_vars.iter().collect()
    };
    let mut user_sets = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for v in reads {
        if !seen.insert(v) {
            continue;
        }
        let want = session.user_vars.get(v).unwrap_or(&VarValue::Null);
        let have = applied.user_vars.get(v);
        // Connections get user variables only through sync and captures,
        // both recorded: one the connection never had is NULL there.
        if have == Some(want) || (have.is_none() && *want == VarValue::Null) {
            continue;
        }
        user_sets.push(format!("@{} = {}", quote_ident(v), want.literal()));
        next.user_vars.insert(v.clone(), want.clone());
    }
    if !user_sets.is_empty() {
        out.push(format!("SET {}", user_sets.join(", ")));
    }
    if stmt.uses_last_insert_id && applied.last_insert_id != session.last_insert_id {
        out.push(format!("DO LAST_INSERT_ID({})", session.last_insert_id));
        next.last_insert_id = session.last_insert_id;
    }
    Ok((out, next))
}

/// What session tracking reported for one client statement, sorted by the
/// replay rules.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Observed {
    /// Allowed variables and their new values.
    pub vars: Vec<(String, String)>,
    /// Variables that can't be replayed: the session pins `Untracked`.
    pub untracked: Vec<String>,
    /// The new schema.
    pub schema: Option<String>,
    /// GTID of a commit.
    pub gtid: Option<String>,
    /// The server flagged a state change no reported entry explains.
    pub unexplained: bool,
}

/// Sorts tracked changes by the replay rules.
#[must_use]
pub fn observe(changes: &TrackedChanges) -> Observed {
    let mut o = Observed {
        schema: changes.schema.clone(),
        gtid: changes.gtid.clone(),
        ..Observed::default()
    };
    for (name, value) in &changes.vars {
        if gateway_var(name) {
            continue;
        }
        if sysvar_literal(name, value).is_some() {
            o.vars.push((name.clone(), value.clone()));
        } else {
            o.untracked.push(name.clone());
        }
    }
    o.unexplained = changes.state_changed && o.vars.is_empty() && o.schema.is_none();
    o
}

impl SessionVars {
    /// Applies what tracking reported for a statement of this session.
    pub fn apply(&mut self, o: &Observed) {
        if let Some(s) = &o.schema {
            self.schema = Some(s.clone());
        }
        for (n, v) in &o.vars {
            self.vars.insert(n.clone(), v.clone());
        }
    }
}

impl AppliedState {
    /// Applies what tracking reported for a statement on this connection.
    pub fn apply(&mut self, o: &Observed) {
        if let Some(s) = &o.schema {
            self.schema = Some(s.clone());
        }
        for (n, v) in &o.vars {
            self.vars.insert(n.clone(), v.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> Baseline {
        Baseline {
            vars: [
                ("autocommit", "ON"),
                ("sql_mode", persona::MARIADB_SQL_MODE),
                ("time_zone", "SYSTEM"),
                ("character_set_results", "utf8mb4"),
            ]
            .into_iter()
            .map(|(a, b)| (a.to_owned(), b.to_owned()))
            .collect(),
        }
    }

    #[test]
    fn literals_are_typed_and_never_raw() {
        assert_eq!(sysvar_literal("autocommit", "OFF").as_deref(), Some("OFF"));
        assert_eq!(sysvar_literal("autocommit", "maybe"), None);
        assert_eq!(
            sysvar_literal("max_statement_time", "2.500000").as_deref(),
            Some("2.500000")
        );
        assert_eq!(sysvar_literal("max_statement_time", "1; DROP"), None);
        assert_eq!(
            sysvar_literal("time_zone", "+01:00").as_deref(),
            Some("CONVERT(X'2B30313A3030' USING utf8mb4)")
        );
        assert_eq!(
            sysvar_literal("character_set_results", "").as_deref(),
            Some("NULL")
        );
        assert_eq!(sysvar_literal("timestamp", "1"), None);
        assert_eq!(
            VarValue::Text(b"a'b".to_vec()).literal(),
            "CONVERT(X'612762' USING utf8mb4)"
        );
        assert_eq!(VarValue::Binary(vec![0, 255]).literal(), "X'00FF'");
        assert_eq!(
            VarValue::from_column(column_type::LONGLONG, 63, Some(b"-7")).literal(),
            "-7"
        );
        assert_eq!(
            VarValue::from_column(column_type::NEWDECIMAL, 63, Some(b"1.50")).literal(),
            "1.50"
        );
        assert_eq!(
            VarValue::from_column(column_type::VAR_STRING, 63, Some(b"x")),
            VarValue::Binary(b"x".to_vec())
        );
        assert_eq!(
            VarValue::from_column(column_type::VAR_STRING, 45, None),
            VarValue::Null
        );
        assert_eq!(quote_ident("a`b"), "`a``b`");
    }

    #[test]
    fn digest_ignores_baseline_values_and_user_vars() {
        let b = base();
        let mut a = AppliedState::default();
        let mut s = SessionVars::default();
        assert_eq!(a.digest(&b), s.digest(0, &b));
        s.vars.insert("autocommit".into(), "ON".into());
        assert_eq!(a.digest(&b), s.digest(0, &b), "baseline value = unset");
        s.vars.insert("time_zone".into(), "+01:00".into());
        assert_ne!(a.digest(&b), s.digest(0, &b));
        a.vars.insert("time_zone".into(), "+01:00".into());
        a.user_vars
            .insert("x".into(), VarValue::Integer("1".into()));
        assert_eq!(a.digest(&b), s.digest(0, &b));
        assert_ne!(a.digest(&b), s.digest(1, &b), "wsrep_sync_wait counts");
        s.schema = Some("db".into());
        assert_ne!(a.digest(&b), s.digest(0, &b));
    }

    #[test]
    fn sync_prefix_follows_the_replay_rules() {
        let b = base();
        let mut applied = AppliedState {
            schema: Some("old".into()),
            ..AppliedState::default()
        };
        applied.vars.insert("time_zone".into(), "+05:00".into());
        let mut s = SessionVars {
            schema: Some("new`db".into()),
            last_insert_id: 42,
            ..SessionVars::default()
        };
        s.vars.insert("autocommit".into(), "OFF".into());
        s.user_vars
            .insert("a".into(), VarValue::Integer("5".into()));
        let mut stmt = StmtInfo::read();
        stmt.reads_user_vars = vec!["a".into(), "unset".into()];
        stmt.uses_last_insert_id = true;
        let (p, next) = sync_prefix(&applied, &b, &s, &stmt, 1).unwrap();
        assert_eq!(
            p,
            vec![
                "USE `new``db`".to_owned(),
                "SET SESSION autocommit = OFF, time_zone = CONVERT(X'53595354454D' USING utf8mb4), wsrep_sync_wait = 1".to_owned(),
                "SET @`a` = 5".to_owned(),
                "DO LAST_INSERT_ID(42)".to_owned(),
            ]
        );
        assert_eq!(next.digest(&b), s.digest(1, &b));
        // Synced: nothing more to send.
        let (p2, _) = sync_prefix(&next, &b, &s, &stmt, 1).unwrap();
        assert!(p2.is_empty(), "{p2:?}");
        // A different connection knew @unset = 3: reset it to NULL.
        let mut other = next.clone();
        other
            .user_vars
            .insert("unset".into(), VarValue::Integer("3".into()));
        let (p3, _) = sync_prefix(&other, &b, &s, &stmt, 1).unwrap();
        assert_eq!(p3, vec!["SET @`unset` = NULL".to_owned()]);
    }

    #[test]
    fn observe_sorts_tracked_changes() {
        let changes = TrackedChanges {
            schema: None,
            vars: vec![
                ("time_zone".into(), "+01:00".into()),
                ("last_gtid".into(), "2-201-5".into()),
                ("sql_log_off".into(), "ON".into()),
            ],
            gtid: Some("2-201-5".into()),
            state_changed: true,
            ..TrackedChanges::default()
        };
        let o = observe(&changes);
        assert_eq!(o.vars, vec![("time_zone".to_owned(), "+01:00".to_owned())]);
        assert_eq!(o.untracked, vec!["sql_log_off".to_owned()]);
        assert!(!o.unexplained);
        let o = observe(&TrackedChanges {
            state_changed: true,
            ..TrackedChanges::default()
        });
        assert!(o.unexplained);
    }

    #[test]
    fn login_state_per_persona() {
        let m = SessionVars::login(Persona::MySql84, 255, Some("app".into()));
        assert_eq!(m.vars["sql_mode"], persona::MYSQL84_SQL_MODE);
        assert_eq!(m.vars["collation_connection"], "utf8mb4_uca1400_ai_ci");
        let d = SessionVars::login(Persona::MariaDb, 45, None);
        assert!(!d.vars.contains_key("sql_mode"));
        assert_eq!(d.vars["collation_connection"], "utf8mb4_general_ci");
    }
}
