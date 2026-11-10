//! Statement history per profile (chapter 53, "Data structures and
//! storage"): `history/<profile>` in the config directory, one JSON line per
//! entry, redacted before it is written, mode 0600.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::lang::Lang;
use crate::redact::redact;

/// One entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// `sql`, `mongo`, `redis` or `cypher`.
    pub lang: String,
    /// The redacted statement.
    pub text: String,
}

/// A profile's history.
#[derive(Debug, Clone)]
pub struct History {
    path: Option<PathBuf>,
    /// Oldest first.
    pub entries: Vec<Entry>,
    max: usize,
    /// Where `Ctrl-Up` is: `entries.len()` when not browsing.
    pub pos: usize,
}

impl History {
    /// Loads `dir/history/<profile>`; a missing or unreadable file is
    /// empty. `None` keeps history in memory only.
    #[must_use]
    pub fn load(dir: Option<&Path>, profile: &str, max: usize) -> Self {
        let path = dir.map(|d| d.join("history").join(profile));
        let entries: Vec<Entry> = path
            .as_ref()
            .and_then(|p| std::fs::read_to_string(p).ok())
            .map(|t| {
                t.lines()
                    .filter_map(|l| serde_json::from_str(l).ok())
                    .collect()
            })
            .unwrap_or_default();
        let pos = entries.len();
        Self {
            path,
            entries,
            max: max.max(1),
            pos,
        }
    }

    /// Adds a statement (redacted first); skips a repeat of the last one.
    pub fn add(&mut self, stmt: &str, lang: Lang) {
        let text = redact(stmt.trim(), lang);
        if text.is_empty() || self.entries.last().is_some_and(|e| e.text == text) {
            self.pos = self.entries.len();
            return;
        }
        let e = Entry {
            lang: lang.name().to_owned(),
            text,
        };
        self.entries.push(e.clone());
        let trimmed = self.entries.len() > self.max;
        if trimmed {
            let drop = self.entries.len() - self.max;
            self.entries.drain(..drop);
        }
        self.pos = self.entries.len();
        let _ = self.save(trimmed, &e);
    }

    fn save(&self, rewrite: bool, last: &Entry) -> std::io::Result<()> {
        let Some(p) = &self.path else {
            return Ok(());
        };
        if let Some(d) = p.parent() {
            crate::secrets::write_private_dir(d).map_err(|e| std::io::Error::other(e.message))?;
        }
        if rewrite {
            let mut body = String::new();
            for e in &self.entries {
                body.push_str(&serde_json::to_string(e).unwrap_or_default());
                body.push('\n');
            }
            return crate::secrets::write_private(p, body.as_bytes())
                .map_err(|e| std::io::Error::other(e.message));
        }
        let mut o = std::fs::OpenOptions::new();
        o.append(true).create(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut o, 0o600);
        let mut f = o.open(p)?;
        writeln!(f, "{}", serde_json::to_string(last).unwrap_or_default())
    }

    /// The previous entry (`Ctrl-Up`).
    pub fn older(&mut self) -> Option<&str> {
        if self.pos == 0 {
            return None;
        }
        self.pos -= 1;
        self.entries.get(self.pos).map(|e| e.text.as_str())
    }

    /// The next entry (`Ctrl-Down`); `None` past the newest.
    pub fn newer(&mut self) -> Option<&str> {
        if self.pos + 1 >= self.entries.len() {
            self.pos = self.entries.len();
            return None;
        }
        self.pos += 1;
        self.entries.get(self.pos).map(|e| e.text.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_caps_and_persists() {
        let dir = std::env::temp_dir().join(format!("sprig-hist-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut h = History::load(Some(&dir), "dev", 3);
        h.add("CREATE USER bob IDENTIFIED BY 'pw'", Lang::Sql);
        h.add("SELECT 1", Lang::Sql);
        h.add("SELECT 1", Lang::Sql);
        h.add("SELECT 2", Lang::Sql);
        h.add("SELECT 3", Lang::Sql);
        assert_eq!(h.entries.len(), 3);
        let again = History::load(Some(&dir), "dev", 3);
        assert_eq!(again.entries, h.entries);
        let body = std::fs::read_to_string(dir.join("history/dev")).expect("file");
        assert!(!body.contains("'pw'"));
        assert_eq!(h.older(), Some("SELECT 3"));
        assert_eq!(h.older(), Some("SELECT 2"));
        assert_eq!(h.newer(), Some("SELECT 3"));
        assert_eq!(h.newer(), None);
        let mut h2 = History::load(Some(&dir), "other", 10);
        h2.add("ALTER USER x IDENTIFIED BY 'secret'", Lang::Sql);
        let b2 = std::fs::read_to_string(dir.join("history/other")).expect("file");
        assert!(b2.contains("BY '***'") && !b2.contains("secret'"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let m = std::fs::metadata(dir.join("history/other")).expect("meta");
            assert_eq!(m.permissions().mode() & 0o777, 0o600);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
