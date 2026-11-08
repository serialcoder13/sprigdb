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

#[cfg(test)]
mod tests {
    use super::*;

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
