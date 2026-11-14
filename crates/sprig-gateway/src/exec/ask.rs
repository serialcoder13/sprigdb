//! The `ASK` step of a tenant statement: a question for an AI assistant,
//! answered with rows holding a proposed query that never runs
//! (`crate::http::studio::ai::ask` has the turn and the columns).

use bytes::Bytes;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::{ColumnDef, StatusFlags};
use sprig_sql::names::LocalAnswer;

use super::PassSession;
use super::tenant::{Step, send_local};
use crate::http::sql::Caller;
use crate::http::studio::ai::ask::{self, AskAnswer, AskRequest};
use crate::pipeline::{ExecCtx, ResultSink, RowFormat, int_column, text_column};

/// The columns, in order.
pub const COLUMNS: [&str; 6] = [
    "query",
    "database",
    "explanation",
    "notes",
    "sources",
    "conversation_id",
];

/// The longest text a cell's metadata announces (MEDIUMTEXT's).
const TEXT_LENGTH: u32 = 16_777_215;

fn columns(charset: u16) -> Vec<ColumnDef> {
    COLUMNS
        .iter()
        .map(|n| {
            if *n == "conversation_id" {
                int_column(n, true)
            } else {
                let mut c = text_column(n, charset);
                c.length = TEXT_LENGTH;
                c
            }
        })
        .collect()
}

/// The answer as a result set.
#[must_use]
pub fn answer_rows(a: &AskAnswer, charset: u16) -> LocalAnswer {
    let text = |s: &str| Some(Bytes::copy_from_slice(s.as_bytes()));
    let conv = a.conversation.map(|c| c.0.to_string());
    LocalAnswer {
        columns: columns(charset),
        rows: a
            .rows
            .iter()
            .map(|r| {
                vec![
                    r.query.as_deref().and_then(text),
                    r.database.as_deref().and_then(text),
                    text(&r.explanation),
                    r.notes.as_deref().and_then(text),
                    r.sources.as_deref().and_then(text),
                    conv.as_deref().and_then(text),
                ]
            })
            .collect(),
    }
}

impl PassSession {
    /// Answers `ASK '<question>' ...`; `None` for any other statement.
    pub(super) async fn tenant_ask(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let (lex, _) = self.lex_opts();
        let Some(parsed) = sprig_sql::ask_stmt::parse(text, &lex) else {
            return Ok(None);
        };
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(None);
        };
        // Sandbox sessions get the usual refusal; the gateway's own
        // statements never ask.
        if ts.types.internal || ts.principal.must_change_password {
            return Ok(None);
        }
        let stmt = match parsed {
            Ok(s) => s,
            Err(e) => return self.fail(out, &e.into()).await.map(Some),
        };
        if format == RowFormat::Binary {
            let e = SprigError::new(
                SprigCode::NameRewriteUnsupported,
                "ASK in a prepared statement: run it as a plain statement",
            );
            return self.fail(out, &e).await.map(Some);
        }
        let caller = Caller {
            principal: ts.principal.clone(),
            peer: cx.session.peer,
            tls: cx.session.tls.clone(),
            listener: cx.session.handle().listener(),
            generation: cx.session.listener_gen,
            authorization: None,
            client_dc: cx.session.client_dc.clone(),
        };
        let current = self.current_logical();
        let req = AskRequest {
            question: stmt.question,
            role: stmt.role,
            database: stmt.database,
        };
        let r = ask::ask(caller, &req, current.as_deref()).await;
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        match r {
            Err(e) => self.fail(out, &e).await.map(Some),
            Ok(a) => {
                let ans = answer_rows(&a, cx.proto.collation);
                send_local(out, &ans, format, status).await?;
                Ok(Some(Step {
                    errored: false,
                    status: Some(status),
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use sprig_ai::ConvId;

    use super::*;
    use crate::http::studio::ai::ask::AskRow;

    #[test]
    fn the_answer_has_six_columns_and_nulls_where_nothing_is_known() {
        let a = AskAnswer {
            rows: vec![
                AskRow {
                    query: Some("SELECT 1".into()),
                    database: Some("shop".into()),
                    explanation: "One.".into(),
                    notes: Some("Reads only.".into()),
                    sources: None,
                },
                AskRow {
                    explanation: "No query here.".into(),
                    ..AskRow::default()
                },
            ],
            conversation: Some(ConvId(42)),
        };
        let ans = answer_rows(&a, 45);
        let names: Vec<_> = ans
            .columns
            .iter()
            .map(|c| String::from_utf8_lossy(&c.name).into_owned())
            .collect();
        assert_eq!(names, COLUMNS);
        assert_eq!(ans.columns[0].length, TEXT_LENGTH);
        assert_eq!(ans.rows[0][0].as_deref(), Some(&b"SELECT 1"[..]));
        assert_eq!(ans.rows[0][4], None);
        assert_eq!(ans.rows[0][5].as_deref(), Some(&b"42"[..]));
        assert_eq!(ans.rows[1][0], None);
        assert_eq!(ans.rows[1][2].as_deref(), Some(&b"No query here."[..]));
    }
}
