//! Emulated system variables and version strings.
//!
//! The persona answers the variables that say which server a client talks
//! to, never the backend:
//!
//! | Variable | MySQL persona | MariaDB persona |
//! |---|---|---|
//! | `version`, `VERSION()` | `gateway.mysql_server_version` | `gateway.mariadb_server_version` |
//! | `version_comment` | `SprigDB <release>` | the same |
//! | `gtid_mode` | `OFF` | (MariaDB's own: no such variable) |
//! | `enforce_gtid_consistency` | `OFF` | (none) |
//! | `sql_mode` | the client's view ([`super::sqlmode`]); `GLOBAL`: MySQL 8.4's default | (the backend's) |
//! | `max_execution_time` | the session's `max_statement_time` in ms | (none) |
//! | `terminology_use_previous` | `NONE` | (none) |
//!
//! Reads in a `SELECT` become literals ([`rewrite_reads`]), with the
//! column named as the client wrote it; `SHOW VARIABLES` rows for them are
//! rewritten by the gateway with [`PersonaVars::show_rows`]. Version strings
//! never contain "MariaDB" on 3306 and always do on 3307: Prisma, Django,
//! Hibernate and the connectors choose dialects from that substring.
//!
//! The persona `sql_mode` defaults live here too; the backend session
//! applies them at login as part of the persona baseline.

use sprig_proto_mysql::packets::Persona;

use std::fmt::Write as _;

use crate::lex::{self, LexOptions, TokenKind};

/// MySQL 8.4's default `sql_mode`.
pub const MYSQL84_SQL_MODE: &str = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,\
                                    NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";

/// MariaDB 11.8's default `sql_mode`.
pub const MARIADB_SQL_MODE: &str =
    "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION";

/// The persona's `version_comment`: `SprigDB` and the release, on both
/// ports (the gateway's local answer to `SELECT @@version_comment` says
/// the same).
#[must_use]
pub const fn version_comment(_persona: Persona) -> &'static str {
    concat!("SprigDB ", env!("CARGO_PKG_VERSION"))
}

/// A version string as a number, `Mmmrr` or `MMmmrr`: 80408 for
/// `8.4.8-SprigDB`, 110803 for `11.8.3-MariaDB-SprigDB`. Executable
/// comments are judged against it. `None` when it doesn't start with
/// `major.minor.patch`.
#[must_use]
pub fn version_number(version: &str) -> Option<u32> {
    let mut parts = version.split(|c: char| !c.is_ascii_digit());
    let major: u32 = parts.next()?.parse().ok()?;
    let minor: u32 = parts.next()?.parse().ok()?;
    let patch: u32 = parts.next()?.parse().ok()?;
    if minor > 99 || patch > 99 {
        return None;
    }
    Some(major * 10_000 + minor * 100 + patch)
}

/// The variables one session's persona answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonaVars {
    /// The listener's persona.
    pub persona: Persona,
    /// The persona's version string (the greeting's).
    pub version: String,
    /// 3306: the client's view of the session's `sql_mode`
    /// ([`super::sqlmode`]); `None` leaves reads to the backend.
    pub sql_mode: Option<String>,
    /// 3306: `max_execution_time` in milliseconds, from the session's
    /// `max_statement_time`.
    pub max_execution_time: Option<u64>,
    /// `CURRENT_ROLE()`: `Some(None)` is `NULL`; `None` leaves it to the
    /// backend.
    pub current_role: Option<Option<String>>,
    /// `max_allowed_packet`: the session's packet limit, the gateway's no
    /// more than its cluster's; `None` leaves it to the backend.
    pub max_allowed_packet: Option<u64>,
    /// `max_connections`: a tenant session's connection limit
    /// (`limits.tenant_connections`); `None` leaves it to the backend, whose
    /// limit doesn't bound the client's sessions.
    pub max_connections: Option<u64>,
    /// The tenant's own defaults of the translator's server-wide allow-list
    /// (`compat.global.<variable>`, set with `SET GLOBAL`): what
    /// `@@global.<variable>` reads answer.
    pub tenant_globals: std::collections::BTreeMap<String, String>,
}

impl PersonaVars {
    /// The variables of `persona` with this version string.
    #[must_use]
    pub fn new(persona: Persona, version: impl Into<String>) -> Self {
        Self {
            persona,
            version: version.into(),
            sql_mode: None,
            max_execution_time: None,
            current_role: None,
            max_allowed_packet: None,
            max_connections: None,
            tenant_globals: std::collections::BTreeMap::new(),
        }
    }

    /// With the tenant's defaults of the server-wide allow-list (variable
    /// name to value).
    #[must_use]
    pub fn with_tenant_globals(mut self, g: std::collections::BTreeMap<String, String>) -> Self {
        self.tenant_globals = g;
        self
    }

    /// With the tenant's connection limit.
    #[must_use]
    pub fn with_max_connections(mut self, n: Option<u64>) -> Self {
        self.max_connections = n;
        self
    }

    /// With the session's packet limit.
    #[must_use]
    pub fn with_max_allowed_packet(mut self, bytes: Option<u64>) -> Self {
        self.max_allowed_packet = bytes;
        self
    }

    /// With the session's `CURRENT_ROLE()` answer (a tenant session's).
    #[must_use]
    pub fn with_current_role(mut self, role: Option<Option<String>>) -> Self {
        self.current_role = role;
        self
    }

    /// With the session's `sql_mode` view and `max_statement_time` (the
    /// backend's seconds), on the MySQL persona.
    #[must_use]
    pub fn with_session(
        mut self,
        sql_mode: Option<&str>,
        max_statement_time: Option<&str>,
    ) -> Self {
        if self.persona == Persona::MySql84 {
            self.sql_mode = sql_mode.map(str::to_owned);
            self.max_execution_time = Some(
                max_statement_time
                    .and_then(|v| v.trim().parse::<f64>().ok())
                    .map_or(0, |s| {
                        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                        let ms = (s * 1000.0).round().max(0.0) as u64;
                        ms
                    }),
            );
        }
        self
    }

    /// The value the persona gives `name` (lower case, without `@@` or a
    /// scope), or `None` when the backend answers.
    #[must_use]
    pub fn value(&self, name: &str) -> Option<std::borrow::Cow<'_, str>> {
        self.value_at(name, false)
    }

    /// [`Self::value`] at session or global scope.
    #[must_use]
    pub fn value_at(&self, name: &str, global: bool) -> Option<std::borrow::Cow<'_, str>> {
        use std::borrow::Cow;
        if global {
            let n = if name == "tx_isolation" {
                "transaction_isolation"
            } else {
                name
            };
            if let Some(v) = self.tenant_globals.get(n) {
                return Some(Cow::Borrowed(v));
            }
        }
        match (self.persona, name) {
            (_, "version") => Some(Cow::Borrowed(&self.version)),
            (p, "version_comment") => Some(Cow::Borrowed(version_comment(p))),
            (Persona::MySql84, "gtid_mode" | "enforce_gtid_consistency") => {
                Some(Cow::Borrowed("OFF"))
            }
            // mysqldump 8.4 sets it; MySQL's default.
            (Persona::MySql84, "terminology_use_previous") => Some(Cow::Borrowed("NONE")),
            // 3306 answers `performance_schema` and `sys`
            // (`compat::perfschema`), so the switch reads `ON`.
            (Persona::MySql84, "performance_schema") => Some(Cow::Borrowed("ON")),
            (Persona::MySql84, "sql_mode") if global => {
                Some(Cow::Owned(super::sqlmode::MYSQL84_DEFAULT.render()))
            }
            (Persona::MySql84, "sql_mode") => self.sql_mode.as_deref().map(Cow::Borrowed),
            (Persona::MySql84, "max_execution_time") if global => Some(Cow::Borrowed("0")),
            (Persona::MySql84, "max_execution_time") => {
                self.max_execution_time.map(|v| Cow::Owned(v.to_string()))
            }
            (_, "max_allowed_packet") => self.max_allowed_packet.map(|v| Cow::Owned(v.to_string())),
            (_, "max_connections") => self.max_connections.map(|v| Cow::Owned(v.to_string())),
            _ => None,
        }
    }

    /// The `SHOW VARIABLES` rows the gateway rewrites: `(Variable_name,
    /// Value)`. Rows MariaDB doesn't have (`gtid_mode`) are not added.
    #[must_use]
    pub fn show_rows(&self) -> Vec<(&'static str, String)> {
        let mut rows = vec![
            ("version", self.version.clone()),
            ("version_comment", version_comment(self.persona).to_owned()),
        ];
        if let Some(m) = &self.sql_mode {
            rows.push(("sql_mode", m.clone()));
        }
        if self.persona == Persona::MySql84 {
            rows.push(("performance_schema", "ON".to_owned()));
        }
        if let Some(n) = self.max_connections {
            rows.push(("max_connections", n.to_string()));
        }
        if let Some(n) = self.max_allowed_packet {
            rows.push(("max_allowed_packet", n.to_string()));
        }
        rows
    }
}

/// The collation variables whose values 3306 shows in MySQL's names (gaps
/// row 60): MariaDB's UCA 14.0 defaults become MySQL's `*_0900_*`.
const MYSQL_NAMED_COLLATIONS: &[&str] = &[
    "collation_connection",
    "collation_database",
    "collation_server",
];

/// Whether the token at `sig[k]` is a whole select item without an alias:
/// at nesting depth 0, after `SELECT` (or `DISTINCT`) or a comma, and
/// followed by the end, a comma, `;`, or a clause keyword.
fn whole_item(tokens: &[lex::Token], sig: &[usize], sql: &[u8], k: usize) -> bool {
    let punct =
        |i: usize, c: u8| tokens[i].kind == TokenKind::Punct && sql[tokens[i].start as usize] == c;
    let word = |i: usize, w: &[&[u8]]| {
        tokens[i].kind == TokenKind::Word
            && w.iter()
                .any(|x| tokens[i].bytes(sql).eq_ignore_ascii_case(x))
    };
    let mut depth = 0i32;
    for &i in &sig[..k] {
        if punct(i, b'(') {
            depth += 1;
        } else if punct(i, b')') {
            depth -= 1;
        }
    }
    if depth != 0 || k == 0 {
        return false;
    }
    let prev = sig[k - 1];
    if !(punct(prev, b',') || word(prev, &[b"SELECT", b"DISTINCT", b"ALL"])) {
        return false;
    }
    match sig.get(k + 1) {
        None => true,
        Some(&n) => {
            punct(n, b',')
                || punct(n, b';')
                || word(
                    n,
                    &[
                        b"FROM",
                        b"INTO",
                        b"UNION",
                        b"WHERE",
                        b"LIMIT",
                        b"ORDER",
                        b"GROUP",
                        b"HAVING",
                        b"WINDOW",
                        b"FOR",
                        b"EXCEPT",
                        b"INTERSECT",
                    ],
                )
        }
    }
}

/// A replaced read: the literal sent, and the column name the client's
/// text gives it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replaced {
    /// The name MariaDB gives the literal's column (a string's value).
    pub column: String,
    /// The text the client wrote (`@@version`, `VERSION()`).
    pub written: String,
}

fn sql_literal(v: &str, nbe: bool) -> String {
    crate::names::rewrite::sql_string(v, nbe)
}

/// Replaces reads of the persona's variables, and `VERSION()`, in a
/// `SELECT` with literals. `@@global.` reads are replaced too: the
/// persona's values are server-wide. `None` when nothing changes or the
/// statement is not a `SELECT`.
#[must_use]
#[allow(clippy::too_many_lines)] // one pass over the tokens
pub fn rewrite_reads(
    sql: &[u8],
    lex_opts: &LexOptions,
    vars: &PersonaVars,
) -> Option<(Vec<u8>, Vec<Replaced>)> {
    // Cheap test first: most statements name neither.
    let has = |needle: &[u8]| {
        sql.windows(needle.len())
            .any(|w| w.eq_ignore_ascii_case(needle))
    };
    let role = vars.current_role.is_some() && has(b"current_role");
    if !has(b"@@") && !has(b"version") && !role {
        return None;
    }
    let tokens = lex::lex(sql, lex_opts).ok()?;
    let sig: Vec<usize> = (0..tokens.len())
        .filter(|&i| !tokens[i].kind.is_trivia())
        .collect();
    let first = *sig.first()?;
    if tokens[first].kind != TokenKind::Word
        && !(tokens[first].kind == TokenKind::Punct && sql[tokens[first].start as usize] == b'(')
    {
        return None;
    }
    let first_word = sig
        .iter()
        .find(|&&i| tokens[i].kind == TokenKind::Word)
        .map(|&i| tokens[i].bytes(sql))?;
    // `SET @old = @@sql_mode` (mysqldump's) reads the client's view
    // too; the assignment targets themselves stay.
    let is_set = first_word.eq_ignore_ascii_case(b"SET");
    if !first_word.eq_ignore_ascii_case(b"SELECT") && !is_set {
        return None;
    }
    let nbe = lex_opts.no_backslash_escapes;
    let punct = |k: usize, c: u8| {
        sig.get(k).is_some_and(|&i| {
            tokens[i].kind == TokenKind::Punct && sql[tokens[i].start as usize] == c
        })
    };
    let mut edits: Vec<(std::ops::Range<usize>, String, String)> = Vec::new();
    for (k, &i) in sig.iter().enumerate() {
        let t = tokens[i];
        match t.kind {
            TokenKind::SysVar
                if !(is_set
                    && sig.get(k + 1).is_some_and(|&n| {
                        tokens[n].kind == TokenKind::Op
                            && matches!(tokens[n].bytes(sql), b"=" | b":=")
                    })) =>
            {
                let text = String::from_utf8_lossy(t.bytes(sql)).to_ascii_lowercase();
                let name = text.trim_start_matches('@');
                let global = name.starts_with("global.");
                let name = name
                    .strip_prefix("session.")
                    .or_else(|| name.strip_prefix("local."))
                    .or_else(|| name.strip_prefix("global."))
                    .unwrap_or(name)
                    .trim_matches('`');
                if let Some(v) = vars.value_at(name, global) {
                    // A number stays a number (its column's type).
                    let lit = if name == "max_allowed_packet" || name == "max_connections" {
                        v.into_owned()
                    } else {
                        sql_literal(&v, nbe)
                    };
                    edits.push((
                        t.range(),
                        lit,
                        String::from_utf8_lossy(t.bytes(sql)).into_owned(),
                    ));
                } else if vars.persona == Persona::MySql84 && MYSQL_NAMED_COLLATIONS.contains(&name)
                {
                    // The backend's collation names read as MySQL's. A
                    // whole select item keeps the name the client wrote.
                    let written = String::from_utf8_lossy(t.bytes(sql)).into_owned();
                    let mut expr = format!("CASE {written}");
                    for (m, my) in super::collation::output_pairs() {
                        // MySQL has its own PAD SPACE `utf8mb4_bin`.
                        if my.starts_with("utf8mb4_0900_") && m != "utf8mb4_bin" {
                            let _ = write!(
                                expr,
                                " WHEN {} THEN {}",
                                sql_literal(&m, nbe),
                                sql_literal(my, nbe)
                            );
                        }
                    }
                    let _ = write!(expr, " ELSE {written} END");
                    if whole_item(&tokens, &sig, sql, k) {
                        let _ = write!(expr, " AS `{}`", written.replace('`', "``"));
                    }
                    edits.push((t.range(), expr, written));
                }
            }
            TokenKind::Word
                if t.bytes(sql).eq_ignore_ascii_case(b"CURRENT_ROLE")
                    && punct(k + 1, b'(')
                    && punct(k + 2, b')')
                    && !(k > 0 && punct(k - 1, b'.'))
                    && vars.current_role.is_some() =>
            {
                let close = tokens[sig[k + 2]];
                let lit = match vars.current_role.as_ref().and_then(Option::as_deref) {
                    Some(r) => sql_literal(r, nbe),
                    None => "NULL".to_owned(),
                };
                edits.push((
                    t.start as usize..close.end as usize,
                    lit,
                    String::from_utf8_lossy(&sql[t.start as usize..close.end as usize])
                        .into_owned(),
                ));
            }
            TokenKind::Word
                if t.bytes(sql).eq_ignore_ascii_case(b"VERSION")
                    && punct(k + 1, b'(')
                    && punct(k + 2, b')')
                    && !(k > 0 && punct(k - 1, b'.')) =>
            {
                let close = tokens[sig[k + 2]];
                edits.push((
                    t.start as usize..close.end as usize,
                    sql_literal(&vars.version, nbe),
                    String::from_utf8_lossy(&sql[t.start as usize..close.end as usize])
                        .into_owned(),
                ));
            }
            _ => {}
        }
    }
    if edits.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(sql.len() + 32);
    let mut at = 0;
    let mut replaced = Vec::with_capacity(edits.len());
    for (r, lit, written) in edits {
        out.extend_from_slice(&sql[at..r.start]);
        out.extend_from_slice(lit.as_bytes());
        at = r.end;
        let column = lex::string_value(lit.as_bytes(), nbe)
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .unwrap_or(lit);
        replaced.push(Replaced { column, written });
    }
    out.extend_from_slice(&sql[at..]);
    Some((out, replaced))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn my() -> PersonaVars {
        PersonaVars::new(Persona::MySql84, "8.4.8-SprigDB")
    }

    fn rw(sql: &str, vars: &PersonaVars) -> Option<(String, Vec<Replaced>)> {
        rewrite_reads(sql.as_bytes(), &LexOptions::mysql84(), vars)
            .map(|(s, r)| (String::from_utf8(s).unwrap(), r))
    }

    #[test]
    fn collation_reads_use_mysql_names() {
        let (sql, r) = rw(
            "SELECT @@collation_connection, @@session.collation_database AS d, \
             CONCAT(@@collation_server, 'x') FROM dual",
            &my(),
        )
        .unwrap();
        assert!(
            sql.starts_with("SELECT CASE @@collation_connection WHEN "),
            "{sql}"
        );
        assert!(
            sql.contains(" WHEN 'utf8mb4_uca1400_ai_ci' THEN 'utf8mb4_0900_ai_ci' "),
            "{sql}"
        );
        assert!(!sql.contains("'utf8mb4_bin'"), "{sql}");
        assert!(sql.contains(" ELSE @@collation_connection END AS `@@collation_connection`, CASE @@session.collation_database"), "{sql}");
        assert!(
            sql.contains(" ELSE @@session.collation_database END AS d, "),
            "{sql}"
        );
        assert!(sql.contains("CONCAT(CASE @@collation_server WHEN"), "{sql}");
        assert!(!sql.contains("AS `@@collation_server`"), "{sql}");
        assert_eq!(r[0].written, "@@collation_connection");
        // `SET @old = @@collation_connection` reads the MySQL name too.
        let (sql, _) = rw("SET @c = @@collation_connection", &my()).unwrap();
        assert!(
            sql.starts_with("SET @c = CASE @@collation_connection WHEN"),
            "{sql}"
        );
        assert!(!sql.contains(" AS `"), "{sql}");
        // 3307 shows MariaDB's names.
        assert!(
            rw(
                "SELECT @@collation_connection",
                &PersonaVars::new(Persona::MariaDb, "11.8.3-MariaDB-SprigDB")
            )
            .is_none()
        );
    }

    #[test]
    fn version_numbers() {
        assert_eq!(version_number("8.4.8-SprigDB"), Some(80408));
        assert_eq!(version_number("8.4.11"), Some(80411));
        assert_eq!(version_number("11.8.3-MariaDB-SprigDB"), Some(110_803));
        assert_eq!(version_number("SprigDB"), None);
        assert_eq!(version_number("8.4"), None);
    }

    #[test]
    fn reads_become_the_personas_values() {
        let (sql, r) = rw(
            "SELECT @@version, @@GLOBAL.version AS v, VERSION(), @@version_comment, \
             @@session.gtid_mode, @@sql_mode, '@@version', x.version()",
            &my(),
        )
        .unwrap();
        assert_eq!(
            sql,
            format!(
                "SELECT '8.4.8-SprigDB', '8.4.8-SprigDB' AS v, '8.4.8-SprigDB', \
                 '{}', 'OFF', @@sql_mode, '@@version', x.version()",
                version_comment(Persona::MySql84)
            )
        );
        assert_eq!(r[0].column, "8.4.8-SprigDB");
        assert_eq!(r[0].written, "@@version");
        assert_eq!(r[2].written, "VERSION()");
        assert_eq!(r[4].written, "@@session.gtid_mode");
        // Prisma's flavour probe.
        let (sql, _) = rw("SELECT @@GLOBAL.version version", &my()).unwrap();
        assert_eq!(sql, "SELECT '8.4.8-SprigDB' version");
    }

    #[test]
    fn the_packet_limit_is_the_sessions_number() {
        let vars = my().with_max_allowed_packet(Some(15 << 20));
        let (sql, r) = rw(
            "SELECT @@max_allowed_packet, @@global.max_allowed_packet x",
            &vars,
        )
        .unwrap();
        assert_eq!(sql, "SELECT 15728640, 15728640 x");
        assert_eq!(r[0].column, "15728640");
        assert_eq!(r[0].written, "@@max_allowed_packet");
        assert!(
            vars.show_rows()
                .contains(&("max_allowed_packet", "15728640".to_owned()))
        );
        // Without a limit the backend answers.
        assert!(rw("SELECT @@max_allowed_packet", &my()).is_none());
    }

    #[test]
    fn max_connections_is_the_tenants_limit() {
        let vars = my().with_max_connections(Some(2000));
        let (sql, _) = rw("SELECT @@max_connections", &vars).unwrap();
        assert_eq!(sql, "SELECT 2000");
        assert!(
            vars.show_rows()
                .contains(&("max_connections", "2000".to_owned()))
        );
        assert!(rw("SELECT @@max_connections", &my()).is_none());
    }

    #[test]
    fn the_mariadb_persona_answers_its_own_and_not_gtid_mode() {
        let ma = PersonaVars::new(Persona::MariaDb, "11.8.3-MariaDB-SprigDB");
        let (sql, _) = rw("SELECT version(), @@gtid_mode", &ma).unwrap();
        assert_eq!(sql, "SELECT '11.8.3-MariaDB-SprigDB', @@gtid_mode");
        assert_eq!(ma.show_rows()[1].1, version_comment(Persona::MariaDb));
    }

    #[test]
    fn only_selects_change() {
        assert_eq!(
            rw("SET @v = @@version, @@session.sql_mode = 'x'", &my())
                .unwrap()
                .0,
            "SET @v = '8.4.8-SprigDB', @@session.sql_mode = 'x'"
        );
        let m = my().with_session(Some("ANSI"), Some("1.5"));
        assert_eq!(
            rw("SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='X'", &m)
                .unwrap()
                .0,
            "SET @OLD_SQL_MODE='ANSI', SQL_MODE='X'"
        );
        assert_eq!(
            rw("SELECT @@max_execution_time, @@global.sql_mode", &m)
                .unwrap()
                .0,
            "SELECT '1500', 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'"
        );
        assert!(rw("SELECT 1", &my()).is_none());
        assert!(rw("SHOW VARIABLES LIKE 'version'", &my()).is_none());
        assert!(rw("/* c */ SELECT @@version", &my()).is_some());
        assert!(rw("(SELECT @@version)", &my()).is_some());
    }
}
