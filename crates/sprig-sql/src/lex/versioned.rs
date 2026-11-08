//! Executable comments: `/*!NNNNN ... */` and MariaDB's `/*M!NNNNNN ... */`
//! (chapter 07, "Lexing per persona"; chapter 28, "One statement").
//!
//! The rules:
//!
//! | Marker | MySQL 8.4 persona | MariaDB persona and backend |
//! |---|---|---|
//! | `/*! body */` | runs | runs |
//! | `/*!NNNNN body */` | runs when `NNNNN` ≤ version | runs when `NNNNN` ≤ version, except 50700–99999 (MySQL-only) |
//! | `/*M! body */`, `/*M!NNNNNN body */` | plain comment | runs when `NNNNNN` ≤ version |
//!
//! The version is five or six digits right after the `!`; fewer digits are
//! part of the body.
//!
//! The gateway forwards the client's text, so a comment the persona skips
//! but MariaDB would run is a hole: the gateway never looked inside it for
//! names. [`neutralize_edits`] finds such comments and turns each into a
//! plain comment by blanking its `!`. A comment the persona runs and the
//! backend skips is left alone here: the gateway did rewrite its body, and
//! whether MariaDB should run it is the translator's business (chapter 28).

use sprig_proto_mysql::packets::Persona;

use super::{Token, TokenKind};

/// A parsed executable-comment opening marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecMarker {
    /// `/*M!`: MariaDB only.
    pub mariadb_only: bool,
    /// The version gate, if digits follow the `!`.
    pub version: Option<u32>,
    /// Length of the marker in bytes, `/*!` plus `M` and the digits.
    pub len: usize,
}

impl ExecMarker {
    /// Parses the marker at the start of `text`, which must begin with
    /// `/*!` or `/*M!`.
    #[must_use]
    pub fn parse(text: &[u8]) -> Option<Self> {
        let (mariadb_only, at) = if text.starts_with(b"/*!") {
            (false, 3)
        } else if text.starts_with(b"/*M!") {
            (true, 4)
        } else {
            return None;
        };
        let ndig = text[at..]
            .iter()
            .take(6)
            .take_while(|c| c.is_ascii_digit())
            .count();
        let (version, len) = if ndig >= 5 {
            let v = text[at..at + ndig]
                .iter()
                .fold(0u32, |acc, d| acc * 10 + u32::from(d - b'0'));
            (Some(v), at + ndig)
        } else {
            (None, at)
        };
        Some(Self {
            mariadb_only,
            version,
            len,
        })
    }

    /// Whether the body runs on `persona` at `version`.
    #[must_use]
    pub fn runs_on(&self, persona: Persona, version: u32) -> bool {
        match persona {
            Persona::MySql84 => !self.mariadb_only && self.version.is_none_or(|v| v <= version),
            Persona::MariaDb => self.runs_on_mariadb(version),
        }
    }

    /// MariaDB's rule, which also decides what the backend runs.
    #[must_use]
    pub fn runs_on_mariadb(&self, version: u32) -> bool {
        match self.version {
            None => true,
            Some(v) if self.mariadb_only => v <= version,
            Some(v) => v <= version && !(50700..=99999).contains(&v),
        }
    }
}

/// The byte offsets of `!` characters to blank so the backend, a MariaDB
/// server at `backend_version`, skips every executable comment the persona
/// skipped. Each offset is replaced by a space.
#[must_use]
pub fn neutralize_edits(src: &[u8], tokens: &[Token], backend_version: u32) -> Vec<usize> {
    tokens
        .iter()
        .filter(|t| t.kind == TokenKind::InactiveExec)
        .filter_map(|t| {
            let text = t.bytes(src);
            let m = ExecMarker::parse(text)?;
            m.runs_on_mariadb(backend_version)
                .then(|| t.start as usize + if m.mariadb_only { 3 } else { 2 })
        })
        .collect()
}

/// A MySQL-only executable comment's body MariaDB understands, spelled
/// MariaDB's way (chapter 28 build step 1, the R0 subset of the DDL rules):
/// `mysqldump` and `SHOW CREATE TABLE` on MySQL 8.4 write
/// `/*!80000 INVISIBLE */` after an invisible index (MariaDB's `IGNORED`),
/// `/*!80023 INVISIBLE */` after an invisible column (the same word on
/// MariaDB) and `/*!80003 SRID n */` after a spatial column (MariaDB's
/// `REF_SYSTEM_ID=n`).
fn mariadb_body(version: u32, words: &[&[u8]]) -> Option<String> {
    let up: Vec<String> = words
        .iter()
        .map(|w| String::from_utf8_lossy(w).to_ascii_uppercase())
        .collect();
    match (version, up.as_slice()) {
        (80000..=80022, [w]) if w == "INVISIBLE" => Some("IGNORED".to_owned()),
        (80023.., [w]) if w == "INVISIBLE" => Some("INVISIBLE".to_owned()),
        (80003.., [srid, n]) if srid == "SRID" && n.bytes().all(|b| b.is_ascii_digit()) => {
            Some(format!("REF_SYSTEM_ID={n}"))
        }
        _ => None,
    }
}

/// On the MySQL persona: rewrites each executable comment the persona runs
/// but MariaDB (at `backend_version`) would skip, when its body is one
/// MariaDB understands with the same meaning (see [`mariadb_body`]); the
/// comment becomes that text, so the backend applies it. Other such
/// comments stay, and MariaDB skips them, as before: the R4 rules cover
/// the rest (`DEFAULT ENCRYPTION`, `SET` of MySQL-only variables). `None`
/// when nothing changes.
#[must_use]
pub fn expand_for_mariadb(src: &[u8], tokens: &[Token], backend_version: u32) -> Option<Vec<u8>> {
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        let t = tokens[i];
        if t.kind != TokenKind::ExecOpen {
            i += 1;
            continue;
        }
        let Some(close) = tokens[i + 1..]
            .iter()
            .position(|t| t.kind == TokenKind::ExecClose)
            .map(|p| i + 1 + p)
        else {
            break;
        };
        if let Some(m) = ExecMarker::parse(t.bytes(src))
            && let Some(v) = m.version
            && !m.mariadb_only
            && !m.runs_on_mariadb(backend_version)
        {
            let words: Vec<&[u8]> = tokens[i + 1..close]
                .iter()
                .filter(|t| !t.kind.is_trivia())
                .map(|t| t.bytes(src))
                .collect();
            let plain = tokens[i + 1..close]
                .iter()
                .filter(|t| !t.kind.is_trivia())
                .all(|t| matches!(t.kind, TokenKind::Word | TokenKind::Number));
            if plain && let Some(body) = mariadb_body(v, &words) {
                edits.push((t.start as usize, tokens[close].end as usize, body));
            }
        }
        i = close + 1;
    }
    if edits.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(src.len());
    let mut at = 0;
    for (start, end, body) in edits {
        out.extend_from_slice(&src[at..start]);
        out.extend_from_slice(body.as_bytes());
        at = end;
    }
    out.extend_from_slice(&src[at..]);
    Some(out)
}

/// [`expand_for_mariadb`] on statement text, lexed with `opts`; only the
/// MySQL persona expands. `None` when nothing changes.
#[must_use]
pub fn expand_text_for_mariadb(
    src: &[u8],
    opts: &super::LexOptions,
    backend_version: u32,
) -> Option<Vec<u8>> {
    if opts.persona != Persona::MySql84 || !src.windows(3).any(|w| w == b"/*!") {
        return None;
    }
    let tokens = super::lex(src, opts).ok()?;
    expand_for_mariadb(src, &tokens, backend_version)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql_only_bodies_mariadb_understands_are_expanded() {
        let opts = crate::lex::LexOptions::mysql84();
        let run = |s: &str| {
            let toks = crate::lex::lex(s.as_bytes(), &opts).unwrap();
            expand_for_mariadb(s.as_bytes(), &toks, 999_999).map(|v| String::from_utf8(v).unwrap())
        };
        assert_eq!(
            run("CREATE TABLE t (a int /*!80023 INVISIBLE */, KEY k (a) /*!80000 INVISIBLE */)")
                .as_deref(),
            Some("CREATE TABLE t (a int INVISIBLE, KEY k (a) IGNORED)")
        );
        assert_eq!(
            run("CREATE TABLE g (p point NOT NULL /*!80003 SRID 4326 */)").as_deref(),
            Some("CREATE TABLE g (p point NOT NULL REF_SYSTEM_ID=4326)")
        );
        // Bodies MariaDB lacks stay comments (MariaDB skips them).
        assert_eq!(run("CREATE DATABASE d /*!80016 DEFAULT ENCRYPTION='N' */"), None);
        assert_eq!(run("/*!80000 SET SESSION information_schema_stats_expiry=0 */"), None);
        // Comments MariaDB runs anyway are left alone.
        assert_eq!(run("/*!40101 SET NAMES utf8mb4 */"), None);
        // Above the persona's version: a comment for both.
        assert_eq!(run("CREATE TABLE t (a int /*!90000 INVISIBLE */)"), None);
    }

    fn m(s: &str) -> ExecMarker {
        ExecMarker::parse(s.as_bytes()).unwrap()
    }

    #[test]
    fn markers_parse() {
        assert_eq!(m("/*!50001 x*/").version, Some(50001));
        assert_eq!(m("/*!50001 x*/").len, 8);
        assert_eq!(m("/*!100100 x*/").version, Some(100_100));
        assert_eq!(m("/*!1001001 x*/").version, Some(100_100));
        assert_eq!(m("/*!12 x*/").version, None);
        assert_eq!(m("/*!12 x*/").len, 3);
        assert!(m("/*M!100100 x*/").mariadb_only);
        assert_eq!(m("/*M! x*/").version, None);
        assert!(ExecMarker::parse(b"/* x */").is_none());
    }

    #[test]
    fn version_gates_follow_each_persona() {
        let my = Persona::MySql84;
        let ma = Persona::MariaDb;
        assert!(m("/*! x*/").runs_on(my, 80406));
        assert!(m("/*!50001 x*/").runs_on(my, 80406));
        assert!(m("/*!80016 x*/").runs_on(my, 80406));
        assert!(!m("/*!80500 x*/").runs_on(my, 80406));
        assert!(!m("/*M!100100 x*/").runs_on(my, 80406));
        assert!(!m("/*M! x*/").runs_on(my, 80406));
        assert!(m("/*!50001 x*/").runs_on(ma, 110_803));
        assert!(!m("/*!50700 x*/").runs_on(ma, 110_803));
        assert!(!m("/*!80016 x*/").runs_on(ma, 110_803));
        assert!(m("/*!100100 x*/").runs_on(ma, 110_803));
        assert!(m("/*M!100100 x*/").runs_on(ma, 110_803));
        assert!(!m("/*M!120000 x*/").runs_on(ma, 110_803));
        assert!(m("/*M! x*/").runs_on(ma, 110_803));
    }
}
