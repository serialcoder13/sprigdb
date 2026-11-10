//! `information_schema` branches over schemas on other clusters (chapter
//! 07, "Physical names"; bugfix-5 and bugfix-8).
//!
//! A table group with no shard on the session's cluster has its
//! representative schema elsewhere ([`GroupedTables`]), so the inline
//! view's branch for that schema, run on the session's cluster, finds
//! nothing and the group's tables vanish from `SHOW TABLES` and
//! `information_schema`. Planning therefore runs in two passes:
//!
//! ```text
//! pass 1: the branch over a remote schema is planned as usual and asked
//!         for:  RemoteBranches::wanted += (cluster, branch SQL)
//! gateway:     runs each wanted branch on its cluster, renders the rows
//!              (inline_rows) and stores them by the branch's text
//! pass 2: the branch is planned as  <branch> UNION ALL SELECT <row 1>
//!         UNION ALL SELECT <row 2> ...
//! ```
//!
//! The local branch stays in front: it finds no rows, but fixes the
//! columns' types and names exactly as MariaDB gives them.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::{Mutex, PoisonError};

use sprig_common::ids::{DbId, TenantId};
use sprig_proto_mysql::resultset::column_type as ct;

use super::NameError;
use super::map::{GroupedTables, LogicalDb, NameMap, Purpose, SchemaName, SchemaSet};

/// What the gateway read of remote branches, and what pass 1 asked for.
#[derive(Debug, Default)]
pub struct RemoteBranches {
    /// Per branch text: its rows as `UNION ALL SELECT ...` text.
    rows: HashMap<String, String>,
    /// Branches asked for that have no rows yet: (cluster, branch text).
    wanted: Mutex<Vec<(String, String)>>,
}

impl RemoteBranches {
    /// The branches pass 1 asked for, once each.
    #[must_use]
    pub fn take_wanted(&self) -> Vec<(String, String)> {
        let mut w =
            std::mem::take(&mut *self.wanted.lock().unwrap_or_else(PoisonError::into_inner));
        w.dedup();
        w
    }

    /// Stores the rows of branch `sql`, rendered by [`inline_rows`].
    pub fn insert(&mut self, sql: String, rows: String) {
        self.rows.insert(sql, rows);
    }
}

/// A [`NameMap`] whose `information_schema` branches over another
/// cluster's schemas carry the rows read there (module docs).
pub struct WithRemote<'a> {
    /// The map planned with.
    pub inner: &'a dyn NameMap,
    /// Where the grouped tables' schemas are.
    pub groups: &'a GroupedTables,
    /// The rows read so far.
    pub branches: &'a RemoteBranches,
}

impl NameMap for WithRemote<'_> {
    fn tenant(&self) -> TenantId {
        self.inner.tenant()
    }

    fn version(&self) -> u64 {
        self.inner.version()
    }

    fn database(&self, db: &str) -> Option<&LogicalDb> {
        self.inner.database(db)
    }

    fn database_by_id(&self, id: DbId) -> Option<&LogicalDb> {
        self.inner.database_by_id(id)
    }

    fn list(&self) -> Vec<&LogicalDb> {
        self.inner.list()
    }

    fn grouped_schemas(&self, db: DbId) -> &[SchemaName] {
        self.inner.grouped_schemas(db)
    }

    fn table_schema(&self, db: DbId, table: &str) -> Option<SchemaName> {
        self.inner.table_schema(db, table)
    }

    fn physical(&self, db: &str, purpose: Purpose) -> Result<SchemaSet, NameError> {
        self.inner.physical(db, purpose)
    }

    fn remote_branch(&self, schema: &SchemaName, sql: &str) -> Option<String> {
        let cluster = self.groups.remote_cluster(schema)?;
        if let Some(rows) = self.branches.rows.get(sql) {
            return Some(rows.clone());
        }
        self.branches
            .wanted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((cluster.to_owned(), sql.to_owned()));
        None
    }
}

/// Rows a branch returned on its cluster, as text to append to it:
/// ` UNION ALL SELECT v1, v2, ...` per row. `types` are the columns' wire
/// types and whether each is binary: numbers stay numbers, temporal
/// values become typed literals, strings hexadecimal literals in
/// `information_schema`'s collation (`_utf8mb3 X'..' COLLATE
/// utf8mb3_general_ci`; `utf8mb4` with 4-byte characters; `X'..'` for
/// binary columns), so `UNION ALL` keeps each column's type and
/// collation.
#[must_use]
pub fn inline_rows(types: &[(u8, bool)], rows: &[Vec<Option<&[u8]>>]) -> String {
    let mut out = String::new();
    for row in rows {
        out.push_str(" UNION ALL SELECT ");
        for (i, v) in row.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            let (t, binary) = types.get(i).copied().unwrap_or((ct::VAR_STRING, false));
            out.push_str(&value(t, binary, *v));
        }
    }
    out
}

fn hex(b: &[u8]) -> String {
    let mut h = String::with_capacity(b.len() * 2 + 3);
    h.push_str("X'");
    for x in b {
        let _ = write!(h, "{x:02X}");
    }
    h.push('\'');
    h
}

fn value(t: u8, binary: bool, v: Option<&[u8]>) -> String {
    let Some(b) = v else {
        return "NULL".to_owned();
    };
    let text = std::str::from_utf8(b).ok();
    let number = text.filter(|s| {
        !s.is_empty()
            && s.bytes()
                .all(|c| c.is_ascii_digit() || matches!(c, b'-' | b'+' | b'.' | b'e' | b'E'))
    });
    let temporal = text.filter(|s| {
        !s.is_empty()
            && s.bytes()
                .all(|c| c.is_ascii_digit() || matches!(c, b'-' | b':' | b' ' | b'.'))
    });
    match t {
        ct::DECIMAL
        | ct::NEWDECIMAL
        | ct::TINY
        | ct::SHORT
        | ct::LONG
        | ct::FLOAT
        | ct::DOUBLE
        | ct::LONGLONG
        | ct::INT24
        | ct::YEAR
            if number.is_some() =>
        {
            number.unwrap_or_default().to_owned()
        }
        ct::DATE | ct::NEWDATE if temporal.is_some() => {
            format!("DATE'{}'", temporal.unwrap_or_default())
        }
        ct::TIME | ct::TIME2 if temporal.is_some() => {
            format!("TIME'{}'", temporal.unwrap_or_default())
        }
        ct::DATETIME | ct::DATETIME2 | ct::TIMESTAMP | ct::TIMESTAMP2 if temporal.is_some() => {
            format!("TIMESTAMP'{}'", temporal.unwrap_or_default())
        }
        _ if binary => hex(b),
        // `information_schema` text is utf8mb3_general_ci, and the view's
        // own expressions name that collation: the literal does too,
        // explicitly, so neither `UNION` nor a condition MariaDB pushes
        // into this branch mixes it with the session's default (1267,
        // 1271).
        _ if b.iter().any(|c| *c >= 0xF0) => {
            format!("_utf8mb4 {} COLLATE utf8mb4_general_ci", hex(b))
        }
        _ => format!("_utf8mb3 {} COLLATE utf8mb3_general_ci", hex(b)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_common::ids::{GroupId, ShardId};

    #[test]
    fn rows_keep_their_types() {
        let types = [
            (ct::VAR_STRING, false),
            (ct::LONGLONG, false),
            (ct::DATETIME, false),
            (ct::BLOB, true),
            (ct::LONGLONG, false),
        ];
        let rows: Vec<Vec<Option<&[u8]>>> = vec![
            vec![
                Some(b"it's"),
                Some(b"42"),
                Some(b"2026-11-10 12:00:01"),
                Some(b"\x00\x01"),
                None,
            ],
            vec![
                Some(b""),
                Some(b"x1"),
                Some("\u{1F600}a'".as_bytes()),
                None,
                Some(b"-7"),
            ],
        ];
        assert_eq!(
            inline_rows(&types, &rows),
            " UNION ALL SELECT _utf8mb3 X'69742773' COLLATE utf8mb3_general_ci, 42, \
             TIMESTAMP'2026-11-10 12:00:01', X'0001', NULL \
             UNION ALL SELECT _utf8mb3 X'' COLLATE utf8mb3_general_ci, \
             _utf8mb3 X'7831' COLLATE utf8mb3_general_ci, \
             _utf8mb4 X'F09F98806127' COLLATE utf8mb4_general_ci, NULL, -7"
        );
        assert_eq!(inline_rows(&types, &[]), "");
    }

    #[test]
    fn only_remote_schemas_are_asked_for_once_then_inlined() {
        let t = TenantId(7);
        let local = SchemaName::shard(t, DbId(1), GroupId(3), ShardId(1));
        let remote = SchemaName::shard(t, DbId(1), GroupId(4), ShardId(2));
        let mut g = GroupedTables::default();
        g.add(DbId(1), "a", local);
        g.add(DbId(1), "b", remote);
        g.set_remote(remote, "c2");
        let base = super::super::map::MemoryNameMap::new(t).with_db(1, "shop", 1, 1);
        let mut br = RemoteBranches::default();
        {
            let w = WithRemote {
                inner: &base,
                groups: &g,
                branches: &br,
            };
            assert_eq!(w.remote_branch(&local, "SELECT 1"), None);
            assert_eq!(w.remote_branch(&remote, "SELECT 2"), None);
            assert_eq!(w.remote_branch(&remote, "SELECT 2"), None);
        }
        assert_eq!(br.take_wanted(), [("c2".to_owned(), "SELECT 2".to_owned())]);
        assert!(br.take_wanted().is_empty());
        br.insert("SELECT 2".into(), " UNION ALL SELECT 5".into());
        let w = WithRemote {
            inner: &base,
            groups: &g,
            branches: &br,
        };
        assert_eq!(
            w.remote_branch(&remote, "SELECT 2").as_deref(),
            Some(" UNION ALL SELECT 5")
        );
        assert!(br.take_wanted().is_empty());
    }
}
