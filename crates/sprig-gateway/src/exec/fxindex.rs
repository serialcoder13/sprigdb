//! Functional key parts in `CREATE INDEX` and `ALTER TABLE ... ADD INDEX`
//! on port 3306 (chapter 28, "Functional index parts"; R4 round 4).
//!
//! The hidden column standing for a part needs a type, which the text
//! rewrite (`sprig_sql::compat::fxindex`) infers from the statement; in
//! `CREATE TABLE` the columns' types are in the text, here they aren't.
//! The gateway reads them from the table's `information_schema.COLUMNS`
//! through the session and rewrites the statement with them
//! (`fxindex::rewrite_with`). Django creates functional unique
//! constraints this way (`CREATE UNIQUE INDEX u ON t ((LOWER(name)), rank)`).

use std::collections::BTreeMap;
use std::fmt::Write as _;

use bytes::Bytes;
use sprig_common::Result;
use sprig_proto_mysql::Persona;
use sprig_sql::compat::fxindex;

use super::PassSession;
use crate::pipeline::ExecCtx;

impl PassSession {
    /// The statement with its functional parts typed from the table's
    /// columns; `None` when it needs nothing (or the types don't help).
    pub(super) async fn fx_typed(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &[u8],
    ) -> Result<Option<Bytes>> {
        if self.persona != Persona::MySql84 {
            return Ok(None);
        }
        let (lex, _) = self.lex_opts();
        // R4 round 5: a descending index over a foreign key's columns.
        if let Some((table, index)) = fxindex::drop_index_target(text, &lex) {
            return Ok(Box::pin(self.fk_desc_drop(cx, &table, &index))
                .await
                .unwrap_or(None));
        }
        let Some(table) = fxindex::needs_types(text, &lex) else {
            return Ok(None);
        };
        let Some((schema, name)) = self.table_literals(&table) else {
            return Ok(None);
        };
        let q = format!(
            "SELECT LOWER(COLUMN_NAME), COLUMN_TYPE FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {name}"
        );
        let got = self.run_checked(cx, &q, false).await?;
        let known: BTreeMap<String, String> = got
            .rows
            .iter()
            .filter_map(|r| {
                let n = r.first()?.as_deref()?;
                let t = r.get(1)?.as_deref()?;
                Some((
                    String::from_utf8_lossy(n).into_owned(),
                    String::from_utf8_lossy(t).into_owned(),
                ))
            })
            .collect();
        Ok(fxindex::rewrite_with(text, &lex, &known).map(Bytes::from))
    }

    /// The table `table` (as written, perhaps qualified) as two SQL
    /// literals for `information_schema`: its schema (`DATABASE()` when
    /// unqualified) and name.
    fn table_literals(&self, table: &str) -> Option<(String, String)> {
        let (lex, _) = self.lex_opts();
        let names: Vec<String> = sprig_sql::lex::lex(table.as_bytes(), &lex)
            .map(|t| {
                t.iter()
                    .filter(|t| t.kind.is_ident_like())
                    .map(|t| {
                        String::from_utf8_lossy(&sprig_sql::lex::ident_value(
                            t.bytes(table.as_bytes()),
                        ))
                        .into_owned()
                    })
                    .collect()
            })
            .unwrap_or_default();
        let lit = |v: &str| sprig_sql::names::rewrite::sql_string(v, lex.no_backslash_escapes);
        match names.as_slice() {
            [t] => Some(("DATABASE()".to_owned(), lit(t))),
            [d, t] => Some((lit(d), lit(t))),
            _ => None,
        }
    }

    /// `DROP INDEX i` where `i` leads with a foreign key's columns, one of
    /// them descending, and no other index serves the key: MySQL kept the
    /// key's own index when `i` was created, MariaDB dropped it and now
    /// refuses with 1553. The drop becomes `ALTER TABLE t ADD INDEX
    /// <key> (cols), DROP INDEX i`, MySQL's end state
    /// (`fxindex::fk_indexes_kept`; R4 round 5). `None` otherwise, and
    /// when reading the table fails (the statement then runs as written).
    async fn fk_desc_drop(
        &mut self,
        cx: &mut ExecCtx<'_>,
        table: &str,
        index: &str,
    ) -> Result<Option<Bytes>> {
        let Some((schema, name)) = self.table_literals(table) else {
            return Ok(None);
        };
        let text = |r: &Vec<Option<Vec<u8>>>, i: usize| {
            r.get(i)
                .and_then(|v| v.as_deref())
                .map(|v| String::from_utf8_lossy(v).into_owned())
        };
        let got = self
            .run_checked(
                cx,
                &format!(
                    "SELECT INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, COLLATION \
                     FROM information_schema.STATISTICS \
                     WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {name}"
                ),
                false,
            )
            .await?;
        let parts: Vec<fxindex::IndexPart> = got
            .rows
            .iter()
            .filter_map(|r| {
                Some(fxindex::IndexPart {
                    index: text(r, 0)?,
                    seq: text(r, 1)?.parse().ok()?,
                    column: text(r, 2),
                    desc: text(r, 3).as_deref() == Some("D"),
                })
            })
            .collect();
        if !parts
            .iter()
            .any(|p| p.desc && p.index.eq_ignore_ascii_case(index))
        {
            return Ok(None);
        }
        let got = self
            .run_checked(
                cx,
                &format!(
                    "SELECT CONSTRAINT_NAME, COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE \
                     WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {name} \
                     AND REFERENCED_TABLE_NAME IS NOT NULL \
                     ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION"
                ),
                false,
            )
            .await?;
        let mut fks: Vec<(String, Vec<String>)> = Vec::new();
        for r in &got.rows {
            let (Some(c), Some(col)) = (text(r, 0), text(r, 1)) else {
                continue;
            };
            match fks.last_mut() {
                Some((n, cols)) if *n == c => cols.push(col),
                _ => fks.push((c, vec![col])),
            }
        }
        let kept = fxindex::fk_indexes_kept(index, &parts, &fks);
        if kept.is_empty() {
            return Ok(None);
        }
        let q = |n: &str| format!("`{}`", n.replace('`', "``"));
        let mut sql = format!("ALTER TABLE {table} ");
        for (fk, cols) in &kept {
            let cols: Vec<String> = cols.iter().map(|c| q(c)).collect();
            let _ = write!(sql, "ADD INDEX {} ({}), ", q(fk), cols.join(", "));
        }
        let _ = write!(sql, "DROP INDEX {}", q(index));
        Ok(Some(Bytes::from(sql)))
    }
}
