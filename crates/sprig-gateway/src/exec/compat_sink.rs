//! MySQL's key names in 1062 on the MySQL persona (chapter 28, "Error
//! mapping"; R4).
//!
//! MariaDB names the key alone (`'PRIMARY'`); MySQL 8.4 names it with its
//! table (`'p.PRIMARY'`). Neither MariaDB's message nor the router's own
//! 1062 (chapter 19's lookup tables) says the table, so the statement
//! does: [`sprig_sql::compat::errmap::dup_key_table`] reads the target of
//! a single-table `INSERT`, `REPLACE`, `UPDATE` or `LOAD DATA`, and this
//! sink adds it to any 1062 the statement's response carries. Everything
//! else passes through.

use async_trait::async_trait;
use bytes::Bytes;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::ColumnDef;
use sprig_proto_mysql::packets::OkPacket;

use crate::pipeline::{EndOfRows, ResultSink, RowFormat, RowValues, Wire};

/// A sink that names the table in 1062 messages.
pub(super) struct DupKeySink<'a> {
    pub(super) inner: &'a mut dyn ResultSink,
    pub(super) table: &'a str,
}

/// `e` with the table in a 1062's key name, if it is one.
pub(super) fn map(e: SprigError, table: Option<&str>) -> SprigError {
    match table.and_then(|t| sprig_sql::compat::errmap::with_dup_key_table(&e, t)) {
        Some(m) => m,
        None => e,
    }
}

#[async_trait]
impl ResultSink for DupKeySink<'_> {
    fn wire(&self) -> Wire {
        self.inner.wire()
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()> {
        self.inner.begin_rows(columns, format).await
    }

    async fn begin_rows_raw(&mut self, defs: &[Bytes], format: RowFormat) -> Result<()> {
        self.inner.begin_rows_raw(defs, format).await
    }

    async fn cursor_opened(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.inner.cursor_opened(columns, end).await
    }

    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        self.inner.row(values).await
    }

    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        self.inner.raw_row(payload).await
    }

    async fn end_rows(&mut self, end: EndOfRows) -> Result<()> {
        self.inner.end_rows(end).await
    }

    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.inner.ok(ok).await
    }

    async fn error(&mut self, err: &SprigError) -> Result<()> {
        match sprig_sql::compat::errmap::with_dup_key_table(err, self.table) {
            Some(m) => self.inner.error(&m).await,
            None => self.inner.error(err).await,
        }
    }

    async fn column_list(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.inner.column_list(columns, end).await
    }

    async fn local_infile_request(&mut self, file_name: &[u8]) -> Result<()> {
        self.inner.local_infile_request(file_name).await
    }

    async fn local_infile_read(&mut self) -> Result<Bytes> {
        self.inner.local_infile_read().await
    }

    async fn flush(&mut self) -> Result<()> {
        self.inner.flush().await
    }

    fn finished(&self) -> bool {
        self.inner.finished()
    }
}

/// MySQL's integer result types on the MySQL persona (chapter 28, "Output
/// on port 3306"; gaps row 34): computed integers are `LONGLONG` and
/// `YEAR(..)` is `YEAR`, where MariaDB sends `LONG`
/// ([`sprig_sql::compat::intmeta`]). The statement is parsed only when a
/// resultset has an expression column of a small integer type; binary rows
/// are re-encoded for the changed columns. A cursor's columns are left as
/// MariaDB's, since its rows come with later fetches.
pub(super) struct IntMetaSink<'a> {
    inner: &'a mut dyn ResultSink,
    sql: &'a [u8],
    lex: sprig_sql::lex::LexOptions,
    /// The statement's plan, once read (`Some(None)`: none).
    plan: Option<Option<sprig_sql::compat::intmeta::Plan>>,
    /// The backend's and the client's columns of the current binary
    /// resultset, when they differ.
    binary: Option<(Vec<ColumnDef>, Vec<ColumnDef>)>,
}

impl<'a> IntMetaSink<'a> {
    pub(super) fn new(
        inner: &'a mut dyn ResultSink,
        sql: &'a [u8],
        lex: sprig_sql::lex::LexOptions,
    ) -> Self {
        Self {
            inner,
            sql,
            lex,
            plan: None,
            binary: None,
        }
    }

    /// Whether a statement can have a resultset worth checking: a query.
    pub(super) fn wanted(sql: &[u8]) -> bool {
        let t = sql.trim_ascii_start();
        let t = t.strip_prefix(b"(").unwrap_or(t).trim_ascii_start();
        t.len() >= 4 && (t[..4].eq_ignore_ascii_case(b"WITH") || t.len() >= 6 && t[..6].eq_ignore_ascii_case(b"SELECT"))
    }

    /// `columns` as MySQL types them, if any changes.
    fn fixed(&mut self, columns: &[ColumnDef]) -> Option<Vec<ColumnDef>> {
        use sprig_sql::compat::intmeta;
        if !intmeta::candidates(columns) {
            return None;
        }
        let (sql, lex) = (self.sql, self.lex);
        let plan = self
            .plan
            .get_or_insert_with(|| intmeta::plan(sql, &lex))
            .as_ref()?;
        let mut cols = columns.to_vec();
        intmeta::apply(plan, &mut cols).then_some(cols)
    }
}

#[async_trait]
impl ResultSink for IntMetaSink<'_> {
    fn wire(&self) -> Wire {
        self.inner.wire()
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()> {
        self.binary = None;
        match self.fixed(columns) {
            Some(cols) => {
                self.inner.begin_rows(&cols, format).await?;
                if format == RowFormat::Binary {
                    self.binary = Some((columns.to_vec(), cols));
                }
                Ok(())
            }
            None => self.inner.begin_rows(columns, format).await,
        }
    }

    async fn begin_rows_raw(&mut self, defs: &[Bytes], format: RowFormat) -> Result<()> {
        self.binary = None;
        let caps = self.inner.wire().caps;
        let decoded: Option<Vec<ColumnDef>> = defs
            .iter()
            .map(|d| ColumnDef::decode(d, caps).ok())
            .collect();
        match decoded.and_then(|c| self.fixed(&c).map(|f| (c, f))) {
            Some((before, after)) => {
                self.inner.begin_rows(&after, format).await?;
                if format == RowFormat::Binary {
                    self.binary = Some((before, after));
                }
                Ok(())
            }
            None => self.inner.begin_rows_raw(defs, format).await,
        }
    }

    async fn cursor_opened(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.binary = None;
        self.inner.cursor_opened(columns, end).await
    }

    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        self.inner.row(values).await
    }

    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        match &self.binary {
            Some((before, after)) => {
                let row = sprig_sql::compat::intmeta::widen_binary_row(&payload, before, after)
                    .unwrap_or(payload);
                self.inner.raw_row(row).await
            }
            None => self.inner.raw_row(payload).await,
        }
    }

    async fn end_rows(&mut self, end: EndOfRows) -> Result<()> {
        self.binary = None;
        self.inner.end_rows(end).await
    }

    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.inner.ok(ok).await
    }

    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.inner.error(err).await
    }

    async fn column_list(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.inner.column_list(columns, end).await
    }

    async fn local_infile_request(&mut self, file_name: &[u8]) -> Result<()> {
        self.inner.local_infile_request(file_name).await
    }

    async fn local_infile_read(&mut self) -> Result<Bytes> {
        self.inner.local_infile_read().await
    }

    async fn flush(&mut self) -> Result<()> {
        self.inner.flush().await
    }

    fn finished(&self) -> bool {
        self.inner.finished()
    }
}

/// The prepared statement's columns as MySQL types them (see
/// [`IntMetaSink`]).
pub(super) fn prepared_columns(sql: &[u8], lex: &sprig_sql::lex::LexOptions, cols: &mut [ColumnDef]) {
    use sprig_sql::compat::intmeta;
    if IntMetaSink::wanted(sql)
        && intmeta::candidates(cols)
        && let Some(plan) = intmeta::plan(sql, lex)
    {
        intmeta::apply(&plan, cols);
    }
}
