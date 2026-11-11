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
        let Some(table) = fxindex::needs_types(text, &lex) else {
            return Ok(None);
        };
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
        let (schema, name) = match names.as_slice() {
            [t] => ("DATABASE()".to_owned(), lit(t)),
            [d, t] => (lit(d), lit(t)),
            _ => return Ok(None),
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
}
