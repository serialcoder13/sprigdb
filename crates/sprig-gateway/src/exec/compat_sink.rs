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

/// Resultsets as MySQL 8.4 sends them on the MySQL persona (chapter 28,
/// "Output on port 3306"):
///
/// - computed integers are `LONGLONG` and `YEAR(..)` is `YEAR`, where
///   MariaDB sends `LONG` (gaps row 34, [`sprig_sql::compat::intmeta`]):
///   the statement is parsed only when a resultset has an expression
///   column of a small integer type, and binary rows are re-encoded for
///   the changed columns;
/// - JSON values (type 245) are printed MySQL's way (gaps row 36,
///   [`sprig_sql::compat::jsontext`]), in text and binary rows.
///
/// A cursor's columns are left as MariaDB's, since its rows come with
/// later fetches.
pub(super) struct IntMetaSink<'a> {
    inner: &'a mut dyn ResultSink,
    sql: &'a [u8],
    lex: sprig_sql::lex::LexOptions,
    /// The statement's plan, once read (`Some(None)`: none).
    plan: Option<Option<sprig_sql::compat::intmeta::Plan>>,
    /// The current resultset, when its rows change.
    cur: Option<Rows>,
}

/// A resultset whose rows the sink rewrites.
struct Rows {
    format: RowFormat,
    /// The backend's columns (how rows arrive) and the client's.
    before: Vec<ColumnDef>,
    after: Vec<ColumnDef>,
    widened: bool,
    /// JSON columns.
    json: Vec<usize>,
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
            cur: None,
        }
    }

    /// Whether a statement can have a resultset worth checking: a query.
    pub(super) fn wanted(sql: &[u8]) -> bool {
        let t = sql.trim_ascii_start();
        let t = t.strip_prefix(b"(").unwrap_or(t).trim_ascii_start();
        let starts = |w: &[u8]| t.len() >= w.len() && t[..w.len()].eq_ignore_ascii_case(w);
        starts(b"WITH") || starts(b"SELECT")
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

    /// Starts a resultset: the client's columns, and the row state.
    fn start(&mut self, columns: &[ColumnDef], format: RowFormat) -> Option<Vec<ColumnDef>> {
        self.cur = None;
        let fixed = self.fixed(columns);
        let after = fixed.as_deref().unwrap_or(columns);
        let json: Vec<usize> = after
            .iter()
            .enumerate()
            .filter(|(_, c)| c.col_type == sprig_proto_mysql::resultset::column_type::JSON)
            .map(|(i, _)| i)
            .collect();
        let widened = fixed.is_some() && format == RowFormat::Binary;
        if widened || !json.is_empty() {
            self.cur = Some(Rows {
                format,
                before: columns.to_vec(),
                after: after.to_vec(),
                widened,
                json,
            });
        }
        fixed
    }
}

/// One JSON value printed MySQL's way, if that changes it.
fn json_value(v: &[u8]) -> Option<Vec<u8>> {
    use sprig_sql::compat::jsontext;
    if !jsontext::may_change(v) {
        return None;
    }
    jsontext::canonical(v).filter(|c| c.as_slice() != v)
}

impl Rows {
    /// A row as the client gets it; `None` when it is unchanged.
    fn rewrite(&self, payload: &Bytes) -> Option<Bytes> {
        use sprig_proto_mysql::Value;
        use sprig_proto_mysql::resultset::{
            decode_binary_row, decode_text_row, encode_binary_row, encode_text_row,
        };
        match self.format {
            RowFormat::Text => {
                if self.json.is_empty() {
                    return None;
                }
                let vals = decode_text_row(payload, self.after.len()).ok()?;
                let mut fixed: Vec<Option<Vec<u8>>> = vec![None; vals.len()];
                let mut any = false;
                for &i in &self.json {
                    if let Some(Some(v)) = vals.get(i)
                        && let Some(c) = json_value(v)
                    {
                        fixed[i] = Some(c);
                        any = true;
                    }
                }
                if !any {
                    return None;
                }
                Some(encode_text_row(
                    vals.iter().zip(&fixed).map(|(v, f)| f.as_deref().or(*v)),
                ))
            }
            RowFormat::Binary => {
                let mut vals = decode_binary_row(payload, &self.before).ok()?;
                let mut any = self.widened;
                for &i in &self.json {
                    if let Some(Value::Bytes(b)) = vals.get(i)
                        && let Some(c) = json_value(b)
                    {
                        vals[i] = Value::Bytes(Bytes::from(c));
                        any = true;
                    }
                }
                if !any {
                    return None;
                }
                encode_binary_row(&vals, &self.after).ok()
            }
        }
    }
}

#[async_trait]
impl ResultSink for IntMetaSink<'_> {
    fn wire(&self) -> Wire {
        self.inner.wire()
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()> {
        match self.start(columns, format) {
            Some(cols) => self.inner.begin_rows(&cols, format).await,
            None => self.inner.begin_rows(columns, format).await,
        }
    }

    async fn begin_rows_raw(&mut self, defs: &[Bytes], format: RowFormat) -> Result<()> {
        self.cur = None;
        let caps = self.inner.wire().caps;
        let decoded: Option<Vec<ColumnDef>> = defs
            .iter()
            .map(|d| ColumnDef::decode(d, caps).ok())
            .collect();
        let Some(cols) = decoded else {
            return self.inner.begin_rows_raw(defs, format).await;
        };
        match self.start(&cols, format) {
            Some(after) => self.inner.begin_rows(&after, format).await,
            None => self.inner.begin_rows_raw(defs, format).await,
        }
    }

    async fn cursor_opened(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.cur = None;
        self.inner.cursor_opened(columns, end).await
    }

    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        // Rows the gateway built (sharded tenants): JSON text values.
        if let (Some(cur), RowValues::Text(vals)) = (&self.cur, &values)
            && !cur.json.is_empty()
        {
            let mut fixed: Vec<Option<Vec<u8>>> = vec![None; vals.len()];
            for &i in &cur.json {
                if let Some(Some(v)) = vals.get(i) {
                    fixed[i] = json_value(v);
                }
            }
            if fixed.iter().any(Option::is_some) {
                let out: Vec<Option<&[u8]>> = vals
                    .iter()
                    .zip(&fixed)
                    .map(|(v, f)| f.as_deref().or(*v))
                    .collect();
                return self.inner.row(RowValues::Text(&out)).await;
            }
        }
        self.inner.row(values).await
    }

    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        match self.cur.as_ref().and_then(|c| c.rewrite(&payload)) {
            Some(row) => self.inner.raw_row(row).await,
            None => self.inner.raw_row(payload).await,
        }
    }

    async fn end_rows(&mut self, end: EndOfRows) -> Result<()> {
        self.cur = None;
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
pub(super) fn prepared_columns(
    sql: &[u8],
    lex: &sprig_sql::lex::LexOptions,
    cols: &mut [ColumnDef],
) {
    use sprig_sql::compat::intmeta;
    if IntMetaSink::wanted(sql)
        && intmeta::candidates(cols)
        && let Some(plan) = intmeta::plan(sql, lex)
    {
        intmeta::apply(&plan, cols);
    }
}
