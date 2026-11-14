//! MySQL 8.4's answers to DDL that MariaDB answers differently, where the
//! gateway has to read the table first. The statement analysis is
//! `sprig_sql::compat::mysqlddl`; this module does the lookups, through
//! the session with logical names, as `fxindex` does.
//!
//! - Before a statement runs ([`PassSession::mysql_ddl_checks`]): a name
//!   longer than 64 characters (1059 on 3306, 1103 on 3307), a descending
//!   key part on a `MyISAM` or `MEMORY` table (1178), and an `ALTER TABLE ...
//!   DROP COLUMN` of a column in a multi-column unique index, which
//!   becomes a rebuild of that index without the column.
//! - After a DDL statement failed ([`PassSession::mysql_ddl_error`]):
//!   MariaDB's 1005 for a foreign key becomes MySQL's 1824 when a
//!   referenced table doesn't exist, or 1826 when a constraint name is
//!   taken in the database.

use bytes::Bytes;
use sprig_common::SprigError;
use sprig_proto_mysql::Persona;
use sprig_sql::compat::errmap::WireError;
use sprig_sql::compat::mysqlddl;

use super::PassSession;
use crate::pipeline::ExecCtx;

/// What [`PassSession::mysql_ddl_checks`] decided.
pub(super) enum DdlCheck {
    /// The statement runs as it is.
    Run,
    /// The statement runs as this text.
    Rewritten(Bytes),
    /// The statement fails with this error.
    Refused(SprigError),
}

fn refused(e: WireError) -> DdlCheck {
    DdlCheck::Refused(SprigError::standard(e.code, e.sqlstate, e.message))
}

/// A text column of a looked-up row.
fn col(r: &[Option<Vec<u8>>], i: usize) -> Option<String> {
    r.get(i)
        .and_then(|v| v.as_deref())
        .map(|v| String::from_utf8_lossy(v).into_owned())
}

impl PassSession {
    /// The checks of the module docs, before the statement runs. A lookup
    /// that fails leaves the statement as it is.
    pub(super) async fn mysql_ddl_checks(&mut self, cx: &mut ExecCtx<'_>, text: &[u8]) -> DdlCheck {
        let (lex, _) = self.lex_opts();
        if let Some(e) = mysqlddl::long_name(text, &lex) {
            return refused(e);
        }
        if self.persona != Persona::MySql84 {
            return DdlCheck::Run;
        }
        if let Some(e) = mysqlddl::engine_descending(text, &lex) {
            return refused(e);
        }
        if let Some(table) = mysqlddl::descending_index_table(text, &lex)
            && let Some((schema, name)) = self.table_literals(&table)
        {
            let q = format!(
                "SELECT ENGINE FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {name}"
            );
            if let Ok(got) = self.run_checked(cx, &q, false).await
                && got
                    .rows
                    .first()
                    .and_then(|r| col(r, 0))
                    .is_some_and(|e| mysqlddl::engine_lacks_descending(&e))
            {
                return refused(mysqlddl::no_descending());
            }
            return DdlCheck::Run;
        }
        if let Some((table, at, dropped)) = mysqlddl::dropped_columns(text, &lex)
            && let Some((schema, name)) = self.table_literals(&table)
        {
            let q = format!(
                "SELECT INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, SUB_PART, COLLATION, NON_UNIQUE, \
                 INDEX_TYPE, INDEX_COMMENT, IS_VISIBLE FROM information_schema.STATISTICS \
                 WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {name}"
            );
            let Ok(got) = self.run_checked(cx, &q, false).await else {
                return DdlCheck::Run;
            };
            let parts: Vec<mysqlddl::KeyPart> = got
                .rows
                .iter()
                .filter_map(|r| {
                    Some(mysqlddl::KeyPart {
                        index: col(r, 0)?,
                        seq: col(r, 1)?.parse().ok()?,
                        column: col(r, 2),
                        sub_part: col(r, 3).and_then(|v| v.parse().ok()),
                        desc: col(r, 4).as_deref() == Some("D"),
                        unique: col(r, 5).as_deref() == Some("0"),
                        index_type: col(r, 6).unwrap_or_default(),
                        comment: col(r, 7).unwrap_or_default(),
                        visible: col(r, 8).as_deref() != Some("NO"),
                    })
                })
                .collect();
            if let Some(sql) =
                mysqlddl::unique_rebuild(text, at, &dropped, &parts, lex.no_backslash_escapes)
            {
                return DdlCheck::Rewritten(Bytes::from(sql));
            }
        }
        DdlCheck::Run
    }

    /// A failed DDL statement's error as MySQL reports it (see the module
    /// docs); `e` itself when nothing applies. `original` is the client's
    /// statement.
    pub(super) async fn mysql_ddl_error(
        &mut self,
        cx: &mut ExecCtx<'_>,
        original: &[u8],
        e: SprigError,
    ) -> SprigError {
        if self.persona != Persona::MySql84 {
            return e;
        }
        let (sprig_common::ErrorKind::Backend { errno: code, .. }
        | sprig_common::ErrorKind::Standard { errno: code, .. }) = e.kind
        else {
            return e;
        };
        let formed = mysqlddl::is_fk_formed_error(code, &e.message);
        let duplicate = mysqlddl::is_fk_duplicate_error(code, &e.message);
        if !formed && !duplicate {
            return e;
        }
        let (lex, _) = self.lex_opts();
        let Some(fks) = mysqlddl::fk_refs(original, &lex) else {
            return e;
        };
        let nbe = lex.no_backslash_escapes;
        let lit = |v: &str| sprig_sql::names::rewrite::sql_string(v, nbe);
        if formed {
            for (db, table) in &fks.refs {
                // A table referencing itself is the one being created.
                if db.is_none() && table.eq_ignore_ascii_case(&fks.table) {
                    continue;
                }
                let schema = db.as_deref().map_or_else(|| "DATABASE()".to_owned(), lit);
                let q = format!(
                    "SELECT COUNT(*) FROM information_schema.TABLES \
                     WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {}",
                    lit(table)
                );
                match self.run_checked(cx, &q, false).await {
                    Ok(got) if got.rows.first().and_then(|r| col(r, 0)).as_deref() == Some("0") => {
                        let m = mysqlddl::fk_missing_table(db.as_deref(), table);
                        return SprigError::standard(m.code, m.sqlstate, m.message);
                    }
                    Ok(_) => {}
                    Err(_) => return e,
                }
            }
            return e;
        }
        for (i, name) in fks.names.iter().enumerate() {
            if fks.names[..i].iter().any(|n| n.eq_ignore_ascii_case(name)) {
                let m = mysqlddl::fk_duplicate_name(name);
                return SprigError::standard(m.code, m.sqlstate, m.message);
            }
            let q = format!(
                "SELECT COUNT(*) FROM information_schema.REFERENTIAL_CONSTRAINTS \
                 WHERE CONSTRAINT_SCHEMA = DATABASE() AND CONSTRAINT_NAME = {}",
                lit(name)
            );
            match self.run_checked(cx, &q, false).await {
                Ok(got) if got.rows.first().and_then(|r| col(r, 0)).as_deref() != Some("0") => {
                    let m = mysqlddl::fk_duplicate_name(name);
                    return SprigError::standard(m.code, m.sqlstate, m.message);
                }
                Ok(_) => {}
                Err(_) => return e,
            }
        }
        e
    }
}
