//! Session state, its replay and its digest.
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
//! Names pass through unchanged: the session's schema is the physical one.

use std::collections::BTreeMap;
use std::hash::Hasher as _;

use sprig_proto_mysql::Persona;
use sprig_proto_mysql::client::TrackedChanges;
use sprig_proto_mysql::resultset::column_type;
use twox_hash::XxHash3_64;

use super::charset;
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

/// The session variables that replay by value (the allowed list, plus the
/// character-set variables `SET NAMES` changes).
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
    // The 3306 persona turns it off (MySQL never fails a REPEATABLE READ
    // update with 1020); a session may turn it back on.
    ("innodb_snapshot_isolation", VarKind::Bool),
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

/// Gateway-owned variables a client can't set (the server refuses).
const READ_ONLY_GATEWAY: &[&str] = &["last_gtid", "warning_count", "error_count"];

/// Whether `name` is a gateway-owned variable a client statement can set:
/// its value is the client's to see, the connection keeps the gateway's.
#[must_use]
pub fn settable_gateway_var(name: &str) -> bool {
    gateway_var(name) && !READ_ONLY_GATEWAY.contains(&name)
}

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
    /// Text (sent in `utf8mb4`) and the collation the variable held:
    /// replayed with it, so a variable read on another backend connection
    /// compares as it did where it was set.
    Collated(Vec<u8>, String),
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
            VarValue::Collated(b, coll) => {
                let cs = coll.split('_').next().unwrap_or_default();
                if cs.eq_ignore_ascii_case("utf8mb4") {
                    format!("CONVERT(X'{}' USING utf8mb4) COLLATE {coll}", hex(b))
                } else {
                    format!(
                        "CONVERT(CONVERT(X'{}' USING utf8mb4) USING {cs}) COLLATE {coll}",
                        hex(b)
                    )
                }
            }
        }
    }

    /// The value with the collation `COLLATION(@v)` reported, for text. A
    /// name that isn't a plain collation name, and `binary`, leave the
    /// value as it is.
    #[must_use]
    pub fn with_collation(self, collation: Option<&[u8]>) -> Self {
        let Some(c) = collation.and_then(|c| std::str::from_utf8(c).ok()) else {
            return self;
        };
        let plain = !c.is_empty()
            && c.contains('_')
            && c.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
        match self {
            VarValue::Text(b) if plain => VarValue::Collated(b, c.to_owned()),
            other => other,
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
    /// Default schema (physical: as the client named it).
    pub schema: Option<String>,
    /// Allowed variables set away from the baseline (or set at all).
    pub vars: BTreeMap<String, String>,
    /// `wsrep_sync_wait` (Galera).
    pub wsrep_sync_wait: u8,
    /// The collation the client character set holds; `None`: the pool's
    /// ([`persona::COLLATION`], from the initialization's `SET NAMES`).
    pub client_collation: Option<String>,
    /// The backend role (not used yet).
    pub role: Option<String>,
    /// User variables known to hold these values.
    pub user_vars: BTreeMap<String, VarValue>,
    /// `LAST_INSERT_ID()` on this connection.
    pub last_insert_id: u64,
    /// Variables a `SET STATEMENT` on this connection set for one
    /// statement. MariaDB reports each one's revert later, with the next
    /// statement that changes any session state; that report is not a
    /// change by that statement.
    pub scoped_pending: std::collections::BTreeSet<String>,
}

/// The client-visible session state the gateway keeps for one session.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionVars {
    /// Default schema.
    pub schema: Option<String>,
    /// Allowed variables the session has set (or that its persona sets at
    /// login).
    pub vars: BTreeMap<String, String>,
    /// The collation the client character set holds: no variable shows it
    /// (`character_set_client` shows the character set), but string
    /// parameters of prepared statements get it. Only `SET NAMES` can give it
    /// a collation other than the character set's compiled default. `None`:
    /// the pool's.
    pub client_collation: Option<String>,
    /// The character set of a login collation that is its compiled
    /// default: the server's `character_set_collations` decides the
    /// collation, known from the first connection's [`Baseline`] (see
    /// [`SessionVars::settle_login`]).
    pub login_charset: Option<String>,
    /// The backend role (not used yet).
    pub role: Option<String>,
    /// User variables with known values.
    pub user_vars: BTreeMap<String, VarValue>,
    /// `LAST_INSERT_ID()`.
    pub last_insert_id: u64,
    /// The client's own values of gateway-owned variables it set
    /// (`session_track_*`, `wait_timeout`, `net_*_timeout`,
    /// `wsrep_sync_wait`, ...): the gateway answers these; backend
    /// connections keep the gateway's values.
    pub gateway_vars: BTreeMap<String, String>,
    /// The MySQL persona's view of `sql_mode`: what `@@sql_mode` answers.
    /// `vars["sql_mode"]` holds the backend value.
    pub client_sql_mode: Option<String>,
    /// A literal `SET sql_mode` on its way: its view and backend value,
    /// recorded when the backend reports the same value.
    pub pending_sql_mode: Option<(String, String)>,
    /// Row-level security's policy views: the protected tables this
    /// session's policies bind, whose views its statements read instead.
    /// Not a variable: [`super::session::BackendSession`] applies it to
    /// every statement it sends.
    pub rls_views: Option<std::sync::Arc<sprig_sql::rls::views::ViewSwap>>,
}

/// The persona baselines.
pub mod persona {
    /// MySQL 8.4's `sql_mode`.
    pub const MYSQL84_SQL_MODE: &str = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,\
        NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";
    /// MariaDB's `sql_mode`.
    pub const MARIADB_SQL_MODE: &str =
        "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION";
    /// The pool's connection collation.
    pub const COLLATION: &str = "utf8mb4_uca1400_ai_ci";
    /// `innodb_snapshot_isolation` for the MySQL persona; the MariaDB persona
    /// keeps the server's default.
    pub const MYSQL84_SNAPSHOT_ISOLATION: &str = "OFF";
    /// `character_set_collations` for the MySQL persona: MySQL 8.4's default
    /// collation of each character set MariaDB maps to a UCA 14.0 one.
    /// `utf8mb4` keeps the backend's `utf8mb4_uca1400_ai_ci`, which 3306
    /// shows as `utf8mb4_0900_ai_ci`; the others are MySQL's `*_general_ci`.
    pub const MYSQL84_CHARSET_COLLATIONS: &str = "utf8mb3=utf8mb3_general_ci,\
        ucs2=ucs2_general_ci,utf8mb4=utf8mb4_uca1400_ai_ci,utf16=utf16_general_ci,\
        utf32=utf32_general_ci";
}

/// The variables `SET NAMES` sets (and the login collation). MariaDB's
/// session tracking reports a `collation_connection` change as
/// `character_set_connection`, with the character set's name only.
const CHARSET_VARS: &[&str] = &[
    "character_set_client",
    "character_set_connection",
    "character_set_results",
    "collation_connection",
];

/// Whether `name` is one of the character-set variables.
#[must_use]
pub fn charset_var(name: &str) -> bool {
    CHARSET_VARS.contains(&name)
}

impl SessionVars {
    /// The state of a new session: the persona's `sql_mode`, the character
    /// sets MariaDB would give a direct client with the same handshake
    /// collation, and the schema named at login. A compiled default collation
    /// is mapped through MariaDB 11.8's default `character_set_collations`
    /// until [`Self::settle_login`] learns the server's.
    #[must_use]
    pub fn login(persona: Persona, collation_id: u16, schema: Option<String>) -> Self {
        let mut vars = BTreeMap::new();
        let mut client_sql_mode = None;
        if persona == Persona::MySql84 {
            // MariaDB runs the backend value of MySQL's
            // default; `@@sql_mode` shows MySQL's.
            let m = sprig_sql::compat::sqlmode::MYSQL84_DEFAULT;
            vars.insert("sql_mode".to_owned(), m.backend());
            client_sql_mode = Some(m.render());
            // MySQL 8.4 has no snapshot-isolation check; MariaDB 11.8's
            // default (ON) fails conflicting REPEATABLE READ updates with
            // 1020, which MySQL applications don't retry.
            vars.insert(
                "innodb_snapshot_isolation".to_owned(),
                persona::MYSQL84_SNAPSHOT_ISOLATION.to_owned(),
            );
            // Gaps row 58: `CHARACTER SET utf8mb3` without a collation is
            // `utf8mb3_general_ci` (id 33), as on MySQL.
            vars.insert(
                "character_set_collations".to_owned(),
                persona::MYSQL84_CHARSET_COLLATIONS.to_owned(),
            );
        }
        let login_map = vars
            .get("character_set_collations")
            .cloned()
            .unwrap_or_else(|| charset::DEFAULT_CHARSET_COLLATIONS.to_owned());
        let mut s = Self {
            schema,
            vars,
            client_sql_mode,
            ..Self::default()
        };
        if let Some((cs, coll)) = charset::login(collation_id, &login_map) {
            s.set_names(&cs, &coll);
            if charset::by_id(collation_id).is_some_and(|(_, primary)| primary) {
                s.login_charset = Some(cs);
            }
        }
        s
    }

    /// The state `SET NAMES cs COLLATE coll` leaves.
    fn set_names(&mut self, cs: &str, coll: &str) {
        for v in [
            "character_set_client",
            "character_set_connection",
            "character_set_results",
        ] {
            self.vars.insert(v.to_owned(), cs.to_owned());
        }
        self.vars
            .insert("collation_connection".to_owned(), coll.to_owned());
        self.client_collation = Some(coll.to_owned());
    }

    /// Maps a compiled-default login collation through the server's own
    /// `character_set_collations` (the baseline's, which is the global
    /// value a direct login would use). Runs once, before the session's
    /// first sync.
    pub fn settle_login(&mut self, base: &Baseline) {
        let Some(cs) = self.login_charset.take() else {
            return;
        };
        // The session's own map (the MySQL persona's), else the server's.
        let own = self.vars.get("character_set_collations").cloned();
        let Some(map) = own
            .as_deref()
            .or_else(|| base.get("character_set_collations"))
        else {
            return;
        };
        let coll = charset::mapped(map, &cs).or_else(|| charset::compiled_default(&cs));
        if let Some(coll) = coll {
            let coll = coll.to_owned();
            self.set_names(&cs, &coll);
        }
    }
}

/// The collation `SET NAMES` must give the client character set, if it
/// differs between the session and the connection.
fn client_names<'a>(applied: &AppliedState, session: &'a SessionVars) -> Option<&'a str> {
    let want = session
        .client_collation
        .as_deref()
        .unwrap_or(persona::COLLATION);
    let have = applied
        .client_collation
        .as_deref()
        .unwrap_or(persona::COLLATION);
    (want != have).then_some(want)
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
    client_collation: Option<&str>,
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
    if let Some(c) = client_collation.filter(|c| *c != persona::COLLATION) {
        h.write(b"\0names=");
        h.write(c.as_bytes());
    }
    h.write_u8(wsrep_sync_wait);
    h.write(role.unwrap_or("").as_bytes());
    h.finish()
}

impl AppliedState {
    /// The session state a statement is parsed under, for the prepared
    /// statement cache: MariaDB fixes a prepared statement's literals in
    /// `collation_connection` and its string parameters in the client
    /// character set's collation at prepare time, and `sql_mode` changes
    /// how the text parses (`ANSI_QUOTES`, `PIPES_AS_CONCAT`, ...). A
    /// statement prepared under other values is prepared again.
    #[must_use]
    pub fn parse_context(&self, base: &Baseline) -> String {
        let v = |n: &str| want(&self.vars, base, n).unwrap_or("");
        format!(
            "{}\u{1}{}\u{1}{}\u{1}{}",
            v("collation_connection"),
            v("character_set_client"),
            self.client_collation
                .as_deref()
                .unwrap_or(persona::COLLATION),
            v("sql_mode"),
        )
    }

    /// The digest of this connection's state.
    #[must_use]
    pub fn digest(&self, base: &Baseline) -> u64 {
        digest(
            self.schema.as_deref(),
            &self.vars,
            self.client_collation.as_deref(),
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
            self.client_collation.as_deref(),
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
#[allow(clippy::too_many_lines)] // one step per kind of session state, in replay order
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
    // The client character set's collation first: only `SET NAMES` sets
    // it, and it sets the other three character-set variables too, which
    // the assignments after it then adjust.
    let mut have = applied.vars.clone();
    let mut set_names = None;
    if let Some(coll) = client_names(applied, session) {
        if !charset::plain_name(coll) {
            return Err(Unreplayable(format!("client collation {coll:?}")));
        }
        let cs = charset::charset_of(coll);
        set_names = Some(format!("NAMES {cs} COLLATE {coll}"));
        for (n, v) in [
            ("character_set_client", cs),
            ("character_set_connection", cs),
            ("character_set_results", cs),
            ("collation_connection", coll),
        ] {
            have.insert(n.to_owned(), v.to_owned());
            next.vars.insert(n.to_owned(), v.to_owned());
        }
        next.client_collation = Some(coll.to_owned());
    }
    let mut sets = Vec::new();
    let names: std::collections::BTreeSet<&String> =
        session.vars.keys().chain(have.keys()).collect();
    for name in names {
        let w = want(&session.vars, base, name);
        let a = want(&have, base, name);
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
        if name == "character_set_client" {
            // Without SET NAMES the client character set takes its
            // compiled default.
            next.client_collation = charset::compiled_default(w).map(str::to_owned);
        }
    }
    if applied.wsrep_sync_wait != wsrep_sync_wait {
        sets.push(format!("wsrep_sync_wait = {wsrep_sync_wait}"));
        next.wsrep_sync_wait = wsrep_sync_wait;
    }
    match (set_names, sets.is_empty()) {
        (Some(n), true) => out.push(format!("SET {n}")),
        (Some(n), false) => out.push(format!("SET {n}, SESSION {}", sets.join(", "))),
        (None, false) => out.push(format!("SET SESSION {}", sets.join(", "))),
        (None, true) => {}
    }
    if session.role != applied.role {
        out.push(match &session.role {
            Some(r) => format!("SET ROLE {}", quote_ident(r)),
            None => "SET ROLE NONE".to_owned(),
        });
        next.role.clone_from(&session.role);
    }
    let mut reads: Vec<&String> = if stmt.reads_all_user_vars {
        session
            .user_vars
            .keys()
            .chain(applied.user_vars.keys())
            .collect()
    } else {
        stmt.reads_user_vars.iter().collect()
    };
    // Row-level security's context, which MariaDB's triggers read for any
    // write, follows the session on every connection.
    reads.extend(
        session
            .user_vars
            .keys()
            .chain(applied.user_vars.keys())
            .filter(|v| v.as_str() == RLS_CONTEXT_VAR),
    );
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

/// The user variable holding a session's row-level security context
/// (`sprig_sql::rls::triggers::CONTEXT_VAR`).
pub const RLS_CONTEXT_VAR: &str = "sprig_rls";

/// What session tracking reported for one client statement, sorted by the
/// replay rules.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Observed {
    /// Allowed variables and their new values.
    pub vars: Vec<(String, String)>,
    /// Variables that can't be replayed: the session pins `Untracked`.
    pub untracked: Vec<String>,
    /// The new schema; `Some("")` when the connection has none any more
    /// (see [`tracked_schema`]).
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

/// The default schema after a tracked schema change. MariaDB reports an
/// empty name when the connection is left without one: `CALL db.p()` from a
/// session without a default schema switches to `db` for the routine and
/// back to none, and reports the change. That is no schema, not a schema
/// with an empty name (replaying it as `USE` fails with 1046 on every
/// later statement).
fn tracked_schema(name: &str) -> Option<String> {
    (!name.is_empty()).then(|| name.to_owned())
}

impl Observed {
    /// Whether the statement changed a character-set variable: then
    /// `collation_connection` must be read back from the connection
    /// (tracking reports only the character set's name) and passed to
    /// [`SessionVars::charset_changed`].
    #[must_use]
    pub fn charset_changed(&self) -> bool {
        self.vars.iter().any(|(n, _)| charset_var(n))
    }
}

/// The character-set state after a statement that changed it:
/// `collation_connection` as read back from the connection, and the client
/// character set's collation. `SET NAMES` gives the client character set
/// the collation it gives the connection (its `COLLATE`, or the character
/// set's `character_set_collations` default); any other change of
/// `character_set_client` (`SET character_set_client`, `SET CHARACTER
/// SET`) gives it the compiled default, as MariaDB 11.8 does.
fn charset_changed(
    vars: &mut BTreeMap<String, String>,
    client: &mut Option<String>,
    o: &Observed,
    collation_connection: &str,
    set_names: bool,
) {
    vars.insert(
        "collation_connection".to_owned(),
        collation_connection.to_owned(),
    );
    if set_names {
        *client = Some(collation_connection.to_owned());
    } else if let Some((_, cs)) = o.vars.iter().find(|(n, _)| n == "character_set_client") {
        *client = charset::compiled_default(cs).map(str::to_owned);
    }
}

impl SessionVars {
    /// A literal `SET sql_mode` is about to run on the MySQL persona: its
    /// client view and backend value.
    pub fn expect_sql_mode(&mut self, view: String, backend: String) {
        self.pending_sql_mode = Some((view, backend));
    }

    /// The MySQL persona after a statement: when it changed `sql_mode`,
    /// the client's view and the backend value follow from what MariaDB
    /// reported. A literal `SET` (rewritten to its backend value) keeps the
    /// view it was given; any other change (an expression, a restored
    /// variable) is read as MySQL names, and the backend value replaces
    /// what the connection holds at the next sync.
    pub fn mysql_sql_mode_after(&mut self, o: &Observed) {
        let pending = self.pending_sql_mode.take();
        if self.client_sql_mode.is_none() {
            return;
        }
        let Some((_, tracked)) = o.vars.iter().find(|(n, _)| n == "sql_mode") else {
            return;
        };
        if let Some((view, backend)) = pending
            && sprig_sql::compat::sqlmode::same_modes(&backend, tracked)
        {
            self.client_sql_mode = Some(view);
            return;
        }
        let m = sprig_sql::compat::sqlmode::from_mariadb(tracked);
        self.client_sql_mode = Some(m.render());
        self.vars.insert("sql_mode".to_owned(), m.backend());
    }

    /// Applies what tracking reported for a statement of this session.
    pub fn apply(&mut self, o: &Observed) {
        if let Some(s) = &o.schema {
            self.schema = tracked_schema(s);
        }
        for (n, v) in &o.vars {
            self.vars.insert(n.clone(), v.clone());
        }
        if o.charset_changed() {
            self.login_charset = None;
        }
    }

    /// See [`Observed::charset_changed`].
    pub fn charset_changed(&mut self, o: &Observed, collation_connection: &str, set_names: bool) {
        charset_changed(
            &mut self.vars,
            &mut self.client_collation,
            o,
            collation_connection,
            set_names,
        );
    }
}

impl AppliedState {
    /// Applies what tracking reported for a statement on this connection.
    pub fn apply(&mut self, o: &Observed) {
        if let Some(s) = &o.schema {
            self.schema = tracked_schema(s);
        }
        for (n, v) in &o.vars {
            self.vars.insert(n.clone(), v.clone());
        }
    }

    /// See [`Observed::charset_changed`].
    pub fn charset_changed(&mut self, o: &Observed, collation_connection: &str, set_names: bool) {
        charset_changed(
            &mut self.vars,
            &mut self.client_collation,
            o,
            collation_connection,
            set_names,
        );
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
    fn an_empty_tracked_schema_means_none() {
        let o = Observed {
            schema: Some(String::new()),
            ..Observed::default()
        };
        let mut s = SessionVars::default();
        s.apply(&o);
        assert_eq!(s.schema, None);
        let mut a = AppliedState {
            schema: Some("db".into()),
            ..AppliedState::default()
        };
        a.apply(&o);
        assert_eq!(a.schema, None);
        s.apply(&Observed {
            schema: Some("db".into()),
            ..Observed::default()
        });
        assert_eq!(s.schema.as_deref(), Some("db"));
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
        // The variable's collation comes along.
        assert_eq!(
            VarValue::Text(b"ab".to_vec())
                .with_collation(Some(b"utf8mb4_0900_ai_ci"))
                .literal(),
            "CONVERT(X'6162' USING utf8mb4) COLLATE utf8mb4_0900_ai_ci"
        );
        assert_eq!(
            VarValue::Text(b"ab".to_vec())
                .with_collation(Some(b"latin1_swedish_ci"))
                .literal(),
            "CONVERT(CONVERT(X'6162' USING utf8mb4) USING latin1) COLLATE latin1_swedish_ci"
        );
        assert_eq!(
            VarValue::Text(b"ab".to_vec()).with_collation(Some(b"x; DROP")),
            VarValue::Text(b"ab".to_vec())
        );
        assert_eq!(
            VarValue::Binary(b"ab".to_vec()).with_collation(Some(b"binary")),
            VarValue::Binary(b"ab".to_vec())
        );
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
    fn sql_mode_views_follow_the_backend() {
        let mut m = SessionVars::login(Persona::MySql84, 255, None);
        // A literal SET, rewritten to its backend value.
        m.expect_sql_mode(
            "ANSI_VIEW".into(),
            "REAL_AS_FLOAT,TIME_ROUND_FRACTIONAL".into(),
        );
        let o = Observed {
            vars: vec![(
                "sql_mode".into(),
                "REAL_AS_FLOAT,TIME_ROUND_FRACTIONAL".into(),
            )],
            ..Observed::default()
        };
        m.apply(&o);
        m.mysql_sql_mode_after(&o);
        assert_eq!(m.client_sql_mode.as_deref(), Some("ANSI_VIEW"));
        // An expression: MySQL's names read back, the backend value fixed.
        let o = Observed {
            vars: vec![(
                "sql_mode".into(),
                "ONLY_FULL_GROUP_BY,NO_AUTO_VALUE_ON_ZERO".into(),
            )],
            ..Observed::default()
        };
        m.apply(&o);
        m.mysql_sql_mode_after(&o);
        assert_eq!(
            m.client_sql_mode.as_deref(),
            Some("ONLY_FULL_GROUP_BY,NO_AUTO_VALUE_ON_ZERO")
        );
        assert_eq!(
            m.vars["sql_mode"],
            "NO_AUTO_VALUE_ON_ZERO,TIME_ROUND_FRACTIONAL"
        );
        // The MariaDB persona has no view.
        let mut d = SessionVars::login(Persona::MariaDb, 45, None);
        d.apply(&o);
        d.mysql_sql_mode_after(&o);
        assert!(d.client_sql_mode.is_none());
    }

    #[test]
    fn login_state_per_persona() {
        let m = SessionVars::login(Persona::MySql84, 255, Some("app".into()));
        // The client sees MySQL's default; MariaDB runs it without
        // ONLY_FULL_GROUP_BY and with TIME_ROUND_FRACTIONAL.
        assert_eq!(
            m.client_sql_mode.as_deref(),
            Some(persona::MYSQL84_SQL_MODE)
        );
        assert_eq!(
            m.vars["sql_mode"],
            "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,\
             NO_ENGINE_SUBSTITUTION,TIME_ROUND_FRACTIONAL"
        );
        // MySQL 8 clients send utf8mb4_0900_ai_ci (255): MariaDB keeps it.
        assert_eq!(m.vars["collation_connection"], "utf8mb4_0900_ai_ci");
        assert_eq!(m.vars["innodb_snapshot_isolation"], "OFF");
        let d = SessionVars::login(Persona::MariaDb, 45, None);
        assert!(!d.vars.contains_key("sql_mode"));
        assert!(!d.vars.contains_key("innodb_snapshot_isolation"));
        // Replayed by value, so a session's own setting survives pooling
        // without pinning.
        assert_eq!(var_kind("innodb_snapshot_isolation"), Some(VarKind::Bool));
        assert_eq!(
            sysvar_literal("innodb_snapshot_isolation", "ON").as_deref(),
            Some("ON")
        );
        let mut base = Baseline::default();
        base.vars
            .insert("innodb_snapshot_isolation".into(), "ON".into());
        let (prefix, _) =
            sync_prefix(&AppliedState::default(), &base, &m, &StmtInfo::write(), 0).unwrap();
        assert!(
            prefix
                .iter()
                .any(|p| p.contains("innodb_snapshot_isolation = OFF")),
            "{prefix:?}"
        );
        // 45 is utf8mb4's compiled default: character_set_collations maps it.
        assert_eq!(d.vars["collation_connection"], "utf8mb4_uca1400_ai_ci");
    }

    fn charset_base(map: &str) -> Baseline {
        let mut b = base();
        for (n, v) in [
            ("character_set_client", "utf8mb4"),
            ("character_set_connection", "utf8mb4"),
            ("collation_connection", persona::COLLATION),
            ("character_set_collations", map),
        ] {
            b.vars.insert(n.to_owned(), v.to_owned());
        }
        b
    }

    #[test]
    fn login_collation_matches_mariadb() {
        let b = charset_base(charset::DEFAULT_CHARSET_COLLATIONS);
        let stmt = StmtInfo::read();
        let conn = AppliedState::default();
        // 45 under the default mapping is exactly the pool's state.
        let mut s = SessionVars::login(Persona::MariaDb, 45, None);
        s.settle_login(&b);
        assert_eq!(s.client_collation.as_deref(), Some(persona::COLLATION));
        assert_eq!(s.digest(0, &b), conn.digest(&b));
        let (p, _) = sync_prefix(&conn, &b, &s, &stmt, 0).unwrap();
        assert!(p.is_empty(), "{p:?}");
        // Another collation: SET NAMES, so the client character set holds
        // it too, and a fresh connection gets the same.
        let s = SessionVars::login(Persona::MariaDb, 224, None);
        let (p, next) = sync_prefix(&conn, &b, &s, &stmt, 0).unwrap();
        assert_eq!(
            p,
            vec!["SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci".to_owned()]
        );
        assert_eq!(next.digest(&b), s.digest(0, &b));
        let s = SessionVars::login(Persona::MariaDb, 8, None);
        let (p, next) = sync_prefix(&conn, &b, &s, &stmt, 0).unwrap();
        assert_eq!(
            p,
            vec!["SET NAMES latin1 COLLATE latin1_swedish_ci".to_owned()]
        );
        assert_eq!(next.digest(&b), s.digest(0, &b));
        // A server whose character_set_collations maps utf8mb4 elsewhere.
        let other = charset_base("utf8mb4=utf8mb4_unicode_ci");
        let mut s = SessionVars::login(Persona::MariaDb, 45, None);
        s.settle_login(&other);
        assert_eq!(s.vars["collation_connection"], "utf8mb4_unicode_ci");
        assert_eq!(s.client_collation.as_deref(), Some("utf8mb4_unicode_ci"));
        // And one with no mapping: the compiled default.
        let mut s = SessionVars::login(Persona::MariaDb, 45, None);
        s.settle_login(&charset_base(""));
        assert_eq!(s.vars["collation_connection"], "utf8mb4_general_ci");
    }

    #[test]
    fn character_set_changes_replay_in_order() {
        let b = charset_base(charset::DEFAULT_CHARSET_COLLATIONS);
        let stmt = StmtInfo::read();
        let mut s = SessionVars::login(Persona::MariaDb, 45, None);
        s.settle_login(&b);
        let reported = |names: &[&str], cs: &str| Observed {
            vars: names
                .iter()
                .map(|n| ((*n).to_owned(), cs.to_owned()))
                .collect(),
            ..Observed::default()
        };
        // SET NAMES utf8mb4 COLLATE utf8mb4_general_ci, then
        // SET collation_connection = utf8mb4_unicode_ci.
        let names = reported(
            &[
                "character_set_client",
                "character_set_connection",
                "character_set_results",
            ],
            "utf8mb4",
        );
        assert!(names.charset_changed());
        s.apply(&names);
        s.charset_changed(&names, "utf8mb4_general_ci", true);
        let conn = reported(&["character_set_connection"], "utf8mb4");
        s.apply(&conn);
        s.charset_changed(&conn, "utf8mb4_unicode_ci", false);
        assert_eq!(s.client_collation.as_deref(), Some("utf8mb4_general_ci"));
        let (p, next) = sync_prefix(&AppliedState::default(), &b, &s, &stmt, 0).unwrap();
        assert_eq!(
            p,
            vec![
                "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci, SESSION collation_connection = \
                 CONVERT(X'757466386D62345F756E69636F64655F6369' USING utf8mb4)"
                    .to_owned()
            ]
        );
        assert_eq!(next.digest(&b), s.digest(0, &b));
        // SET character_set_client = latin1: its compiled default.
        let client = reported(&["character_set_client"], "latin1");
        s.apply(&client);
        s.charset_changed(&client, "utf8mb4_unicode_ci", false);
        assert_eq!(s.client_collation.as_deref(), Some("latin1_swedish_ci"));
        let (p, _) = sync_prefix(&next, &b, &s, &stmt, 0).unwrap();
        assert_eq!(
            p,
            vec![
                "SET NAMES latin1 COLLATE latin1_swedish_ci, SESSION character_set_connection = \
                 CONVERT(X'757466386D6234' USING utf8mb4), character_set_results = \
                 CONVERT(X'757466386D6234' USING utf8mb4), collation_connection = \
                 CONVERT(X'757466386D62345F756E69636F64655F6369' USING utf8mb4)"
                    .to_owned()
            ]
        );
        // A client collation that isn't a plain name is never pasted.
        s.client_collation = Some("x' OR 1".into());
        assert!(sync_prefix(&next, &b, &s, &stmt, 0).is_err());
    }
}
