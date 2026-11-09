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
