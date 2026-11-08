//! Emulated system variables and version strings (chapter 28, "Emulated
//! system variables"; build step 2).
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
//!
//! Reads in a `SELECT` become literals ([`rewrite_reads`]), with the
//! column named as the client wrote it; `SHOW VARIABLES` rows for them are
//! rewritten by the gateway with [`PersonaVars::show_rows`]. Version strings
//! never contain "MariaDB" on 3306 and always do on 3307: Prisma, Django,
//! Hibernate and the connectors choose dialects from that substring.
//!
//! The persona `sql_mode` defaults live here too; the backend session
//! applies them at login (chapter 06's persona baseline).

use sprig_proto_mysql::packets::Persona;

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
}

impl PersonaVars {
    /// The variables of `persona` with this version string.
    #[must_use]
    pub fn new(persona: Persona, version: impl Into<String>) -> Self {
        Self {
            persona,
            version: version.into(),
        }
    }

    /// The value the persona gives `name` (lower case, without `@@` or a
    /// scope), or `None` when the backend answers.
    #[must_use]
    pub fn value(&self, name: &str) -> Option<&str> {
        match (self.persona, name) {
            (_, "version") => Some(&self.version),
            (p, "version_comment") => Some(version_comment(p)),
            (Persona::MySql84, "gtid_mode" | "enforce_gtid_consistency") => Some("OFF"),
            _ => None,
        }
    }

    /// The `SHOW VARIABLES` rows the gateway rewrites: `(Variable_name,
    /// Value)`. Rows MariaDB doesn't have (`gtid_mode`) are not added.
    #[must_use]
    pub fn show_rows(&self) -> Vec<(&'static str, String)> {
        vec![
            ("version", self.version.clone()),
            (
                "version_comment",
                version_comment(self.persona).to_owned(),
            ),
        ]
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
    if !has(b"@@") && !has(b"version") {
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
    if !first_word.eq_ignore_ascii_case(b"SELECT") {
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
            TokenKind::SysVar => {
                let text = String::from_utf8_lossy(t.bytes(sql)).to_ascii_lowercase();
                let name = text.trim_start_matches('@');
                let name = name
                    .strip_prefix("session.")
                    .or_else(|| name.strip_prefix("local."))
                    .or_else(|| name.strip_prefix("global."))
                    .unwrap_or(name)
                    .trim_matches('`');
                if let Some(v) = vars.value(name) {
                    edits.push((
                        t.range(),
                        sql_literal(v, nbe),
                        String::from_utf8_lossy(t.bytes(sql)).into_owned(),
                    ));
                }
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
    fn the_mariadb_persona_answers_its_own_and_not_gtid_mode() {
        let ma = PersonaVars::new(Persona::MariaDb, "11.8.3-MariaDB-SprigDB");
        let (sql, _) = rw("SELECT version(), @@gtid_mode", &ma).unwrap();
        assert_eq!(sql, "SELECT '11.8.3-MariaDB-SprigDB', @@gtid_mode");
        assert_eq!(ma.show_rows()[1].1, version_comment(Persona::MariaDb));
    }

    #[test]
    fn only_selects_change() {
        assert!(rw("SET @v = @@version", &my()).is_none());
        assert!(rw("SELECT 1", &my()).is_none());
        assert!(rw("SHOW VARIABLES LIKE 'version'", &my()).is_none());
        assert!(rw("/* c */ SELECT @@version", &my()).is_some());
        assert!(rw("(SELECT @@version)", &my()).is_some());
    }
}
