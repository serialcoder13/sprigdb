//! Statements the gateway answers itself (chapter 05: the persona's version
//! variables "from the persona, never from the backend"; chapter 04:
//! `KILL` and `SHOW PROCESSLIST` over the `SessionTable`, since connection
//! ids are the gateway's).
//!
//! Without the parser (chapter 07) these are recognized by a small,
//! conservative matcher: a statement is local only if it is exactly one of
//! the forms below (case-insensitive, extra spaces and one trailing `;`
//! allowed); everything else goes to the executor.
//!
//! - `SELECT item [, item ...] [LIMIT 1]` where every item is `VERSION()`,
//!   `@@version`, `@@version_comment`, `@@protocol_version`,
//!   `@@max_allowed_packet` or `@@hostname` (optionally `@@session.` or
//!   `@@global.`). A select mixing these with other items goes to the
//!   executor whole (the router splits them in R1).
//! - `KILL [CONNECTION | QUERY] id`, where `id` may be `CONNECTION_ID()`.
//! - `SHOW [FULL] PROCESSLIST`.

use sprig_common::ids::ConnId;

/// The id [`classify`] gives `KILL CONNECTION_ID()`: no session has id 0.
pub(crate) const KILL_SELF: ConnId = ConnId(0);

/// A statement the gateway answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Local {
    /// `SELECT` of persona variables: the items as written.
    Variables(Vec<(String, PersonaVar)>),
    /// `KILL [CONNECTION | QUERY] id`.
    Kill {
        /// The target session.
        id: ConnId,
        /// `KILL QUERY`.
        query_only: bool,
    },
    /// `SHOW [FULL] PROCESSLIST`.
    ProcessList {
        /// `FULL`: don't truncate `Info`.
        full: bool,
    },
}

/// A variable the persona answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PersonaVar {
    /// `VERSION()`, `@@version`.
    Version,
    /// `@@version_comment`.
    VersionComment,
    /// `@@protocol_version`.
    ProtocolVersion,
    /// `@@max_allowed_packet`.
    MaxAllowedPacket,
    /// `@@hostname`.
    Hostname,
}

impl PersonaVar {
    fn parse(item: &str) -> Option<Self> {
        let l = item.to_ascii_lowercase();
        if l == "version()" {
            return Some(PersonaVar::Version);
        }
        let v = l.strip_prefix("@@")?;
        let v = v
            .strip_prefix("session.")
            .or_else(|| v.strip_prefix("global."))
            .unwrap_or(v);
        Some(match v {
            "version" => PersonaVar::Version,
            "version_comment" => PersonaVar::VersionComment,
            "protocol_version" => PersonaVar::ProtocolVersion,
            "max_allowed_packet" => PersonaVar::MaxAllowedPacket,
            "hostname" => PersonaVar::Hostname,
            _ => return None,
        })
    }

    /// Whether the value is a number.
    pub(crate) fn is_int(self) -> bool {
        matches!(
            self,
            PersonaVar::ProtocolVersion | PersonaVar::MaxAllowedPacket
        )
    }
}

/// Splits on ASCII whitespace into lowercase words.
fn words(s: &str) -> Vec<String> {
    s.split_ascii_whitespace()
        .map(str::to_ascii_lowercase)
        .collect()
}

/// Recognizes a local statement.
pub(crate) fn classify(sql: &[u8]) -> Option<Local> {
    // Every local statement is short; a large statement isn't scanned here
    // on the worker thread (chapter 04, "Large statements").
    if sql.len() > 4096 {
        return None;
    }
    let text = std::str::from_utf8(sql).ok()?.trim();
    let text = text.strip_suffix(';').unwrap_or(text).trim_end();
    if text.contains(';') || text.len() > 512 {
        return None;
    }
    let w = words(text);
    match w.first().map(String::as_str) {
        Some("kill") => {
            let (query_only, id) = match w.as_slice() {
                [_, id] => (false, id),
                [_, k, id] if k == "connection" => (false, id),
                [_, k, id] if k == "query" => (true, id),
                _ => return None,
            };
            // `KILL CONNECTION_ID()`: the session's own id (answer.rs
            // reads [`KILL_SELF`] as the caller's).
            let id = if id == "connection_id()" {
                KILL_SELF
            } else {
                ConnId(id.parse().ok()?)
            };
            Some(Local::Kill { id, query_only })
        }
        Some("show") => match w.as_slice() {
            [_, p] if p == "processlist" => Some(Local::ProcessList { full: false }),
            [_, f, p] if f == "full" && p == "processlist" => {
                Some(Local::ProcessList { full: true })
            }
            _ => None,
        },
        Some("select") => {
            let mut rest = text[6..].trim();
            let lower = rest.to_ascii_lowercase();
            if let Some(i) = lower.rfind(" limit ") {
                if lower[i + 7..].trim() != "1" {
                    return None;
                }
                rest = rest[..i].trim();
            }
            let mut items = Vec::new();
            for item in rest.split(',').map(str::trim) {
                items.push((item.to_owned(), PersonaVar::parse(item)?));
            }
            (!items.is_empty()).then_some(Local::Variables(items))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_local_statements_only() {
        assert_eq!(
            classify(b"select @@version_comment limit 1"),
            Some(Local::Variables(vec![(
                "@@version_comment".to_owned(),
                PersonaVar::VersionComment
            )]))
        );
        assert!(matches!(
            classify(b"SELECT VERSION(), @@SESSION.max_allowed_packet;"),
            Some(Local::Variables(v)) if v.len() == 2 && v[1].1 == PersonaVar::MaxAllowedPacket
        ));
        assert_eq!(
            classify(b"SELECT @@max_allowed_packet, @@wait_timeout"),
            None
        );
        assert_eq!(classify(b"SELECT @@version limit 2"), None);
        assert_eq!(classify(b"SELECT 1; SELECT @@version"), None);
        assert_eq!(
            classify(b"KILL QUERY 1048577"),
            Some(Local::Kill {
                id: ConnId(1_048_577),
                query_only: true
            })
        );
        assert_eq!(
            classify(b"kill  connection 5"),
            Some(Local::Kill {
                id: ConnId(5),
                query_only: false
            })
        );
        assert_eq!(
            classify(b"kill 5"),
            Some(Local::Kill {
                id: ConnId(5),
                query_only: false
            })
        );
        assert_eq!(classify(b"kill x"), None);
        assert_eq!(
            classify(b"KILL CONNECTION_ID()"),
            Some(Local::Kill {
                id: KILL_SELF,
                query_only: false
            })
        );
        assert_eq!(
            classify(b"show full processlist"),
            Some(Local::ProcessList { full: true })
        );
        assert_eq!(classify(b"show processlist like 'x'"), None);
        assert_eq!(classify(b"select * from t"), None);
    }
}
