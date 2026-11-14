//! The tenant glossary: business terms ("revenue excludes refunds") that
//! every tenant assistant's prompt carries, in the tenant's catalog
//! (`t<id>_sprig`, module `ai_glossary`, created the first time a gateway
//! needs it for a tenant).
//!
//! - **Who writes.** Tenant administrators add, accept and delete terms;
//!   everyone in the tenant reads them. An assistant's `propose_glossary`
//!   files a `proposed` entry, which an administrator accepts or rejects;
//!   it never becomes part of a prompt before that.
//! - **One accepted and one proposed entry per term.** A proposal for a
//!   term that is already accepted waits beside it; accepting it replaces
//!   the accepted entry.
//! - **In the prompt.** [`prompt`] renders the accepted terms, secrets
//!   masked, cut at [`PROMPT_BYTES`] (about 4,000 tokens). Terms are text
//!   the tenant's administrators wrote, so they go out like schema: with
//!   `ai.share_data` on or off.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Mutex;

use async_trait::async_trait;
use mysql_async::prelude::Queryable as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sprig_catalog::migrate::{MigrateOpts, Migration, Migrator, Module, Phase};
use sprig_common::ids::UserId;
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use tokio::sync::OnceCell;

/// The catalog module.
pub const MODULE: &str = "ai_glossary";

/// The longest term, in characters.
pub const MAX_TERM: usize = 128;
/// The longest definition or SQL expression, in characters.
pub const MAX_TEXT: usize = 4000;
/// Tables one entry may name.
pub const MAX_TABLES: usize = 20;
/// The most the glossary adds to a prompt, in bytes.
pub const PROMPT_BYTES: usize = 16 * 1024;

const DDL_GLOSSARY: &str = "CREATE TABLE IF NOT EXISTS ai_glossary (
  entry_id    BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  term        VARCHAR(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  status      ENUM('proposed','accepted') NOT NULL,
  definition  TEXT NOT NULL,
  sql_expr    TEXT NULL,
  tables      JSON NULL,
  author_id   INT UNSIGNED NOT NULL,
  author      VARCHAR(64) NOT NULL,
  accepted_by INT UNSIGNED NULL,
  proposal_id VARCHAR(36) NULL,
  created_at  DATETIME(3) NOT NULL,
  updated_at  DATETIME(3) NOT NULL,
  UNIQUE KEY by_term (term, status)
) ENGINE=InnoDB";

/// The module's migrations.
pub static GLOSSARY_MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "ai_glossary",
    statements: &[DDL_GLOSSARY],
}];

/// The module, for migrators.
pub const GLOSSARY_MODULE: Module = Module {
    name: MODULE,
    migrations: GLOSSARY_MIGRATIONS,
};

/// An entry's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    /// Filed by an assistant (or a user who isn't an administrator),
    /// waiting for an administrator.
    Proposed,
    /// In every assistant's prompt.
    Accepted,
}

impl Status {
    /// The catalog's name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Status::Proposed => "proposed",
            Status::Accepted => "accepted",
        }
    }

    /// The status named `s`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "proposed" => Some(Status::Proposed),
            "accepted" => Some(Status::Accepted),
            _ => None,
        }
    }
}

/// A term as written, before it is stored.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct NewTerm {
    /// The term.
    pub term: String,
    /// What it means for this tenant.
    pub definition: String,
    /// An SQL expression for it, such as `SUM(amount) - SUM(refunds)`.
    #[serde(default)]
    pub sql: Option<String>,
    /// The tables it applies to.
    #[serde(default)]
    pub tables: Vec<String>,
}

impl NewTerm {
    /// From a JSON body or a `propose_glossary` input: `term`,
    /// `definition`, `sql` (or `sql_expr`) and `tables`, trimmed and
    /// checked.
    ///
    /// # Errors
    /// 39790-style invalid request naming the field.
    pub fn from_json(v: &Value) -> Result<Self> {
        let s = |k: &str| {
            v.get(k)
                .and_then(Value::as_str)
                .map_or("", str::trim)
                .to_owned()
        };
        let sql = Some(s("sql"))
            .filter(|x| !x.is_empty())
            .or_else(|| Some(s("sql_expr")).filter(|x| !x.is_empty()));
        let tables = match v.get("tables") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Array(a)) => a
                .iter()
                .map(|t| {
                    t.as_str()
                        .map(|t| t.trim().to_owned())
                        .ok_or_else(|| invalid("tables must be an array of names"))
                })
                .collect::<Result<Vec<_>>>()?,
            Some(_) => return Err(invalid("tables must be an array of names")),
        };
        let t = NewTerm {
            term: s("term"),
            definition: s("definition"),
            sql,
            tables,
        };
        t.check()?;
        Ok(t)
    }

    /// Checks the lengths.
    ///
    /// # Errors
    /// An invalid request naming the field.
    pub fn check(&self) -> Result<()> {
        let n = self.term.chars().count();
        if n == 0 || n > MAX_TERM {
            return Err(invalid(format!("a term is 1 to {MAX_TERM} characters")));
        }
        let d = self.definition.chars().count();
        if d == 0 || d > MAX_TEXT {
            return Err(invalid(format!(
                "a definition is 1 to {MAX_TEXT} characters"
            )));
        }
        if self
            .sql
            .as_ref()
            .is_some_and(|s| s.chars().count() > MAX_TEXT)
        {
            return Err(invalid(format!(
                "the SQL expression is longer than {MAX_TEXT} characters"
            )));
        }
        if self.tables.len() > MAX_TABLES
            || self
                .tables
                .iter()
                .any(|t| t.is_empty() || t.chars().count() > 128)
        {
            return Err(invalid(format!(
                "tables names at most {MAX_TABLES} tables of 1 to 128 characters"
            )));
        }
        Ok(())
    }
}

/// A stored entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// The term.
    pub term: String,
    /// Its state.
    pub status: Status,
    /// What it means.
    pub definition: String,
    /// Its SQL expression.
    pub sql: Option<String>,
    /// The tables it applies to.
    pub tables: Vec<String>,
    /// Who wrote it (for a proposal, the user whose assistant filed it).
    pub author_id: u32,
    /// Their user name.
    pub author: String,
    /// The administrator who accepted it.
    pub accepted_by: Option<u32>,
    /// The assistant proposal it came from.
    pub proposal_id: Option<String>,
    /// Last changed, ms since the Unix epoch.
    pub updated_ms: i64,
}

/// Where the glossary is kept.
#[async_trait]
pub trait GlossaryStore: Send + Sync {
    /// Entries, by term; only those in `status` when given.
    async fn list(&self, status: Option<Status>) -> Result<Vec<Entry>>;
    /// Writes `t` as accepted (an administrator's own entry), replacing
    /// an accepted entry of that term and dropping a proposal for it.
    async fn put(&self, t: &NewTerm, author: UserId, author_name: &str) -> Result<()>;
    /// Files `t` as a proposal, replacing an earlier proposal of that term.
    async fn propose(
        &self,
        t: &NewTerm,
        author: UserId,
        author_name: &str,
        proposal_id: Option<&str>,
    ) -> Result<()>;
    /// Accepts the proposal for `term`; `false` if there is none.
    async fn accept(&self, term: &str, by: UserId) -> Result<bool>;
    /// Drops the proposal for `term`; `false` if there is none.
    async fn reject(&self, term: &str) -> Result<bool>;
    /// Drops `term`, accepted and proposed; `false` if there was nothing.
    async fn delete(&self, term: &str) -> Result<bool>;
}

fn invalid(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::AdminRequestInvalid, m)
}

/// The accepted terms as a prompt block, secrets masked; `None` when there
/// are none. Cut at [`PROMPT_BYTES`], keeping whole entries.
#[must_use]
pub fn prompt(entries: &[Entry]) -> Option<String> {
    let mut out = String::from(
        "The tenant's glossary: business terms its administrators accepted. Use these meanings, and the SQL when one is given, whenever a question uses a term.\n",
    );
    let head = out.len();
    let mut left = 0usize;
    for e in entries.iter().filter(|e| e.status == Status::Accepted) {
        let mut line = format!("- {}: {}", e.term, e.definition.replace('\n', " "));
        if let Some(s) = &e.sql {
            let _ = write!(line, " SQL: `{}`.", s.replace('\n', " "));
        }
        if !e.tables.is_empty() {
            let _ = write!(line, " Tables: {}.", e.tables.join(", "));
        }
        let line = crate::egress::redact_secrets(&line);
        if out.len() + line.len() + 1 > PROMPT_BYTES {
            left += 1;
            continue;
        }
        out.push_str(&line);
        out.push('\n');
    }
    if out.len() == head {
        return None;
    }
    if left > 0 {
        let _ = writeln!(
            out,
            "({left} more terms didn't fit; ask the user when a term is unclear.)"
        );
    }
    Some(out)
}

// ---- in memory ----

/// An in-memory glossary, for tests and evaluation runs.
#[derive(Debug, Default)]
pub struct MemoryGlossary {
    inner: Mutex<BTreeMap<(String, Status), Entry>>,
}

impl MemoryGlossary {
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<(String, Status), Entry>> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn key(term: &str) -> String {
    term.trim().to_lowercase()
}

fn entry(t: &NewTerm, status: Status, author: UserId, name: &str, p: Option<&str>) -> Entry {
    Entry {
        term: t.term.clone(),
        status,
        definition: t.definition.clone(),
        sql: t.sql.clone(),
        tables: t.tables.clone(),
        author_id: author.0,
        author: name.to_owned(),
        accepted_by: (status == Status::Accepted).then_some(author.0),
        proposal_id: p.map(str::to_owned),
        updated_ms: 0,
    }
}

#[async_trait]
impl GlossaryStore for MemoryGlossary {
    async fn list(&self, status: Option<Status>) -> Result<Vec<Entry>> {
        Ok(self
            .lock()
            .values()
            .filter(|e| status.is_none_or(|s| e.status == s))
            .cloned()
            .collect())
    }

    async fn put(&self, t: &NewTerm, author: UserId, author_name: &str) -> Result<()> {
        t.check()?;
        let mut m = self.lock();
        m.remove(&(key(&t.term), Status::Proposed));
        m.insert(
            (key(&t.term), Status::Accepted),
            entry(t, Status::Accepted, author, author_name, None),
        );
        Ok(())
    }

    async fn propose(
        &self,
        t: &NewTerm,
        author: UserId,
        author_name: &str,
        proposal_id: Option<&str>,
    ) -> Result<()> {
        t.check()?;
        self.lock().insert(
            (key(&t.term), Status::Proposed),
            entry(t, Status::Proposed, author, author_name, proposal_id),
        );
        Ok(())
    }

    async fn accept(&self, term: &str, by: UserId) -> Result<bool> {
        let mut m = self.lock();
        let Some(mut e) = m.remove(&(key(term), Status::Proposed)) else {
            return Ok(false);
        };
        e.status = Status::Accepted;
        e.accepted_by = Some(by.0);
        m.insert((key(term), Status::Accepted), e);
        Ok(true)
    }

    async fn reject(&self, term: &str) -> Result<bool> {
        Ok(self.lock().remove(&(key(term), Status::Proposed)).is_some())
    }

    async fn delete(&self, term: &str) -> Result<bool> {
        let mut m = self.lock();
        let a = m.remove(&(key(term), Status::Accepted)).is_some();
        let p = m.remove(&(key(term), Status::Proposed)).is_some();
        Ok(a || p)
    }
}

// ---- the tenant catalog ----

/// The glossary in a tenant's catalog schema.
pub struct CatalogGlossary {
    pool: mysql_async::Pool,
    schema: String,
    ready: OnceCell<()>,
}

impl std::fmt::Debug for CatalogGlossary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogGlossary")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

fn unavailable(e: mysql_async::Error) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        "the tenant's catalog is unreachable",
    )
    .with_source(e)
}

type EntryRow = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    u32,
    String,
    Option<u32>,
    Option<String>,
    i64,
);

impl CatalogGlossary {
    /// The glossary in `schema` (`t<id>_sprig`), reached through `pool`
    /// (the catalog writer's).
    #[must_use]
    pub fn new(pool: mysql_async::Pool, schema: impl Into<String>) -> Self {
        Self {
            pool,
            schema: schema.into(),
            ready: OnceCell::new(),
        }
    }

    fn t(&self) -> String {
        format!("{}.{}", quote(&self.schema), quote("ai_glossary"))
    }

    async fn conn(&self) -> Result<mysql_async::Conn> {
        let mut conn = self.pool.get_conn().await.map_err(unavailable)?;
        self.ready
            .get_or_try_init(|| async {
                let have: Option<u64> = conn
                    .exec_first(
                        "SELECT COUNT(*) FROM information_schema.TABLES \
                         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'ai_glossary'",
                        (self.schema.as_str(),),
                    )
                    .await
                    .map_err(unavailable)?;
                if have.unwrap_or(0) == 0 {
                    Migrator::new(&[GLOSSARY_MODULE])?
                        .run(&mut conn, &self.schema, &MigrateOpts::default())
                        .await?;
                }
                Ok::<(), SprigError>(())
            })
            .await?;
        Ok(conn)
    }

    fn row(r: EntryRow) -> Option<Entry> {
        Some(Entry {
            term: r.0,
            status: Status::parse(&r.1)?,
            definition: r.2,
            sql: r.3,
            tables: r
                .4
                .and_then(|j| serde_json::from_str(&j).ok())
                .unwrap_or_default(),
            author_id: r.5,
            author: r.6,
            accepted_by: r.7,
            proposal_id: r.8,
            updated_ms: r.9,
        })
    }

    async fn upsert(
        &self,
        c: &mut mysql_async::Conn,
        t: &NewTerm,
        status: Status,
        author: UserId,
        name: &str,
        proposal_id: Option<&str>,
    ) -> Result<()> {
        let tables = (!t.tables.is_empty())
            .then(|| serde_json::to_string(&t.tables).unwrap_or_else(|_| "[]".to_owned()));
        let accepted_by = (status == Status::Accepted).then_some(author.0);
        c.exec_drop(
            format!(
                "INSERT INTO {} (term, status, definition, sql_expr, tables, author_id, author, \
                 accepted_by, proposal_id, created_at, updated_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3)) \
                 ON DUPLICATE KEY UPDATE term = VALUES(term), definition = VALUES(definition), \
                 sql_expr = VALUES(sql_expr), tables = VALUES(tables), author_id = VALUES(author_id), \
                 author = VALUES(author), accepted_by = VALUES(accepted_by), \
                 proposal_id = VALUES(proposal_id), updated_at = VALUES(updated_at)",
                self.t()
            ),
            (
                &t.term,
                status.as_str(),
                &t.definition,
                &t.sql,
                tables,
                author.0,
                name,
                accepted_by,
                proposal_id,
            ),
        )
        .await
        .map_err(unavailable)
    }
}

#[async_trait]
impl GlossaryStore for CatalogGlossary {
    async fn list(&self, status: Option<Status>) -> Result<Vec<Entry>> {
        let mut c = self.conn().await?;
        let sql = format!(
            "SELECT term, status, definition, sql_expr, CAST(tables AS CHAR), author_id, author, \
             accepted_by, proposal_id, \
             CAST(TIMESTAMPDIFF(MICROSECOND, '1970-01-01', updated_at) DIV 1000 AS SIGNED) \
             FROM {} {} ORDER BY term, status",
            self.t(),
            if status.is_some() {
                "WHERE status = ?"
            } else {
                ""
            }
        );
        let rows: Vec<EntryRow> = match status {
            Some(s) => c.exec(sql, (s.as_str(),)).await,
            None => c.exec(sql, ()).await,
        }
        .map_err(unavailable)?;
        Ok(rows.into_iter().filter_map(Self::row).collect())
    }

    async fn put(&self, t: &NewTerm, author: UserId, author_name: &str) -> Result<()> {
        t.check()?;
        let mut c = self.conn().await?;
        self.upsert(&mut c, t, Status::Accepted, author, author_name, None)
            .await?;
        c.exec_drop(
            format!(
                "DELETE FROM {} WHERE term = ? AND status = 'proposed'",
                self.t()
            ),
            (&t.term,),
        )
        .await
        .map_err(unavailable)
    }

    async fn propose(
        &self,
        t: &NewTerm,
        author: UserId,
        author_name: &str,
        proposal_id: Option<&str>,
    ) -> Result<()> {
        t.check()?;
        let mut c = self.conn().await?;
        self.upsert(
            &mut c,
            t,
            Status::Proposed,
            author,
            author_name,
            proposal_id,
        )
        .await
    }

    async fn accept(&self, term: &str, by: UserId) -> Result<bool> {
        let mut c = self.conn().await?;
        let t = self.t();
        let mut tx = c
            .start_transaction(mysql_async::TxOpts::default())
            .await
            .map_err(unavailable)?;
        let have: Option<u64> = tx
            .exec_first(
                format!(
                    "SELECT entry_id FROM {t} WHERE term = ? AND status = 'proposed' FOR UPDATE"
                ),
                (term,),
            )
            .await
            .map_err(unavailable)?;
        let Some(id) = have else {
            tx.rollback().await.map_err(unavailable)?;
            return Ok(false);
        };
        tx.exec_drop(
            format!("DELETE FROM {t} WHERE term = ? AND status = 'accepted'"),
            (term,),
        )
        .await
        .map_err(unavailable)?;
        tx.exec_drop(
            format!(
                "UPDATE {t} SET status = 'accepted', accepted_by = ?, updated_at = UTC_TIMESTAMP(3) \
                 WHERE entry_id = ?"
            ),
            (by.0, id),
        )
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(true)
    }

    async fn reject(&self, term: &str) -> Result<bool> {
        let mut c = self.conn().await?;
        c.exec_drop(
            format!(
                "DELETE FROM {} WHERE term = ? AND status = 'proposed'",
                self.t()
            ),
            (term,),
        )
        .await
        .map_err(unavailable)?;
        Ok(c.affected_rows() > 0)
    }

    async fn delete(&self, term: &str) -> Result<bool> {
        let mut c = self.conn().await?;
        c.exec_drop(format!("DELETE FROM {} WHERE term = ?", self.t()), (term,))
            .await
            .map_err(unavailable)?;
        Ok(c.affected_rows() > 0)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn terms_are_checked() {
        let t = NewTerm::from_json(&json!({
            "term": " revenue ", "definition": "paid orders minus refunds",
            "sql": "SUM(amount) - SUM(refund)", "tables": ["orders"]
        }))
        .unwrap();
        assert_eq!(t.term, "revenue");
        assert_eq!(t.sql.as_deref(), Some("SUM(amount) - SUM(refund)"));
        assert!(NewTerm::from_json(&json!({"term": "", "definition": "x"})).is_err());
        assert!(NewTerm::from_json(&json!({"term": "x", "definition": ""})).is_err());
        assert!(
            NewTerm::from_json(&json!({"term": "x", "definition": "y", "tables": "t"})).is_err()
        );
        assert!(NewTerm::from_json(&json!({"term": "x".repeat(129), "definition": "y"})).is_err());
    }

    #[tokio::test]
    async fn proposals_wait_for_an_administrator() {
        let g = MemoryGlossary::default();
        let t = |d: &str| NewTerm {
            term: "Active customer".into(),
            definition: d.into(),
            sql: None,
            tables: vec!["customers".into()],
        };
        g.propose(&t("ordered in 90 days"), UserId(7), "ann", Some("p1"))
            .await
            .unwrap();
        let all = g.list(None).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(prompt(&all), None, "a proposal isn't in prompts");
        assert!(g.accept("active customer", UserId(1)).await.unwrap());
        assert!(!g.accept("active customer", UserId(1)).await.unwrap());
        g.propose(&t("ordered in 30 days"), UserId(7), "ann", None)
            .await
            .unwrap();
        let acc = g.list(Some(Status::Accepted)).await.unwrap();
        assert_eq!(acc[0].definition, "ordered in 90 days");
        assert_eq!(acc[0].accepted_by, Some(1));
        let p = prompt(&g.list(None).await.unwrap()).unwrap();
        assert!(p.contains("Active customer: ordered in 90 days"), "{p}");
        assert!(!p.contains("30 days"), "{p}");
        assert!(g.reject("Active customer").await.unwrap());
        g.put(
            &NewTerm {
                term: "token".into(),
                definition: "the API token is sprig_acme_abcdefghjkmn_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
                sql: None,
                tables: vec![],
            },
            UserId(1),
            "admin",
        )
        .await
        .unwrap();
        let p = prompt(&g.list(None).await.unwrap()).unwrap();
        assert!(!p.contains("sprig_acme_abcdefghjkmn_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"), "secrets masked: {p}");
        assert!(g.delete("active customer").await.unwrap());
        assert!(!g.delete("active customer").await.unwrap());
    }

    #[test]
    fn the_prompt_block_is_capped() {
        let entries: Vec<Entry> = (0..400)
            .map(|i| Entry {
                term: format!("term{i}"),
                status: Status::Accepted,
                definition: "d".repeat(100),
                sql: None,
                tables: vec![],
                author_id: 1,
                author: "a".into(),
                accepted_by: Some(1),
                proposal_id: None,
                updated_ms: 0,
            })
            .collect();
        let p = prompt(&entries).unwrap();
        assert!(p.len() <= PROMPT_BYTES + 200);
        assert!(p.contains("more terms didn't fit"));
    }
}
