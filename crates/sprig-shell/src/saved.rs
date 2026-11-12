//! Saved queries per profile (decision R4 of
//! `sprigdb-plan/decisions/studio-tui-redesign.md`).
//!
//! The tenant's catalog keeps them per user (`/v1/library`), shared with
//! Studio: the TUI loads them at start ([`Saved::adopt`]) and sends every
//! save there too. `saved/<profile>.json` in the config directory (mode
//! 0600, redacted like history) is the copy for gateways that can't keep
//! them (an older gateway, an accounts-file login) and for working offline;
//! queries only in it are sent to the catalog the first time it answers.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::lang::Lang;
use crate::redact::redact;

/// One saved query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedQuery {
    /// Its name, unique per profile.
    pub name: String,
    /// `sql`, `mongo`, `redis` or `cypher`.
    pub lang: String,
    /// The redacted text.
    pub text: String,
    /// The database it was saved in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
}

/// A run the catalog keeps (from Studio, or `sprig` elsewhere).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedRun {
    /// The language.
    pub lang: String,
    /// The redacted text.
    pub text: String,
    /// `studio`, `shell` or `other`.
    pub client: String,
}

/// A profile's saved queries, newest first.
#[derive(Debug, Clone, Default)]
pub struct Saved {
    path: Option<PathBuf>,
    /// The queries, newest first.
    pub items: Vec<SavedQuery>,
    /// The tenant's catalog keeps them: saves and runs go there too.
    pub shared: bool,
    /// The catalog's runs at start, newest first.
    pub shared_history: Vec<SharedRun>,
}

impl Saved {
    /// Loads `dir/saved/<profile>.json`; missing or unreadable is empty.
    /// `None` keeps them in memory only.
    #[must_use]
    pub fn load(dir: Option<&Path>, profile: &str) -> Self {
        let path = dir.map(|d| d.join("saved").join(format!("{profile}.json")));
        let items = path
            .as_ref()
            .and_then(|p| std::fs::read_to_string(p).ok())
            .and_then(|t| serde_json::from_str(&t).ok())
            .unwrap_or_default();
        Self {
            path,
            items,
            ..Self::default()
        }
    }

    /// Takes the catalog's library (the reply of `GET /v1/library`): its
    /// saved queries and runs, with the queries only kept here first.
    /// Returns those, to send to the catalog.
    pub fn adopt(&mut self, v: &Value) -> Vec<SavedQuery> {
        let s = |x: &Value, k: &str| x.get(k).and_then(Value::as_str).unwrap_or("").to_owned();
        let remote: Vec<SavedQuery> = v
            .get("saved")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|q| SavedQuery {
                name: s(q, "name"),
                lang: s(q, "language"),
                text: s(q, "text"),
                database: q.get("database").and_then(Value::as_str).map(str::to_owned),
            })
            .filter(|q| !q.name.is_empty())
            .collect();
        let only_here: Vec<SavedQuery> = self
            .items
            .iter()
            .filter(|q| !remote.iter().any(|r| r.name == q.name))
            .cloned()
            .collect();
        self.items = only_here.iter().cloned().chain(remote).collect();
        self.shared_history = v
            .get("history")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|h| SharedRun {
                lang: s(h, "language"),
                text: s(h, "text"),
                client: s(h, "client"),
            })
            .filter(|h| !h.text.is_empty())
            .collect();
        self.shared = true;
        let _ = self.write();
        only_here
    }

    /// Saves `text` as `name` (replacing a query of that name), redacted
    /// first. Returns the query saved, to send to the catalog.
    ///
    /// # Errors
    /// The file couldn't be written.
    pub fn save(
        &mut self,
        name: &str,
        text: &str,
        lang: Lang,
        database: Option<&str>,
    ) -> std::io::Result<Option<SavedQuery>> {
        let name = name.trim();
        let text = redact(text.trim(), lang);
        if name.is_empty() || text.is_empty() {
            return Ok(None);
        }
        self.items.retain(|q| q.name != name);
        self.items.insert(
            0,
            SavedQuery {
                name: name.to_owned(),
                lang: lang.name().to_owned(),
                text,
                database: database.map(str::to_owned),
            },
        );
        self.write()?;
        Ok(self.items.first().cloned())
    }

    /// Removes the query named `name`.
    ///
    /// # Errors
    /// The file couldn't be written.
    pub fn remove(&mut self, name: &str) -> std::io::Result<()> {
        self.items.retain(|q| q.name != name);
        self.write()
    }

    fn write(&self) -> std::io::Result<()> {
        let Some(p) = &self.path else {
            return Ok(());
        };
        if let Some(d) = p.parent() {
            crate::secrets::write_private_dir(d).map_err(|e| std::io::Error::other(e.message))?;
        }
        let body = serde_json::to_vec_pretty(&self.items).unwrap_or_default();
        crate::secrets::write_private(p, &body).map_err(|e| std::io::Error::other(e.message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saves_replaces_and_reloads() {
        let dir = std::env::temp_dir().join(format!("sprig-saved-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut s = Saved::load(Some(&dir), "dev");
        s.save(
            "open orders",
            "SELECT * FROM orders WHERE status = 'open'",
            Lang::Sql,
            Some("shop"),
        )
        .expect("save");
        s.save("paid", "SELECT 2", Lang::Sql, None).expect("save");
        s.save("open orders", "SELECT 3", Lang::Sql, None)
            .expect("save");
        assert_eq!(
            s.items
                .iter()
                .map(|q| (q.name.as_str(), q.text.as_str()))
                .collect::<Vec<_>>(),
            [("open orders", "SELECT 3"), ("paid", "SELECT 2")]
        );
        let again = Saved::load(Some(&dir), "dev");
        assert_eq!(again.items, s.items);
        s.remove("paid").expect("remove");
        assert_eq!(Saved::load(Some(&dir), "dev").items.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn secrets_are_redacted() {
        let mut s = Saved::default();
        s.save(
            "make user",
            "CREATE USER bob IDENTIFIED BY 'hunter2'",
            Lang::Sql,
            None,
        )
        .expect("save");
        assert!(!s.items[0].text.contains("hunter2"), "{}", s.items[0].text);
    }

    #[test]
    fn adopting_the_catalog_keeps_local_only_queries_first() {
        let mut s = Saved::default();
        s.save("mine", "SELECT 1", Lang::Sql, None).expect("save");
        s.save("both", "SELECT 0", Lang::Sql, None).expect("save");
        let up = s.adopt(&serde_json::json!({
            "saved": [{"name": "both", "language": "sql", "database": "shop", "text": "SELECT 2"}],
            "history": [{"language": "sql", "text": "SELECT 9", "client": "studio"}, {"text": ""}],
        }));
        assert_eq!(
            up.iter().map(|q| q.name.as_str()).collect::<Vec<_>>(),
            ["mine"]
        );
        assert!(s.shared);
        assert_eq!(
            s.items
                .iter()
                .map(|q| (q.name.as_str(), q.text.as_str()))
                .collect::<Vec<_>>(),
            [("mine", "SELECT 1"), ("both", "SELECT 2")]
        );
        assert_eq!(s.items[1].database.as_deref(), Some("shop"));
        assert_eq!(s.shared_history.len(), 1);
        assert_eq!(s.shared_history[0].client, "studio");
    }
}
