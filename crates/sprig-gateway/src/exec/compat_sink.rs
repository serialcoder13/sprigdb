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
    /// The statement's plan, once read.
    plan: PlanState,
    /// The current resultset, when its rows change.
    cur: Option<Rows>,
}

/// Whether the statement's plan has been read.
enum PlanState {
    Unread,
    Read(Option<sprig_sql::compat::intmeta::Plan>),
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
    /// Functional key parts' expressions of `SHOW INDEX` and
    /// `STATISTICS.EXPRESSION` (R4 round 5, gaps row 64): printed MySQL's
    /// way.
    exprs: Vec<usize>,
    /// Stored programs' text as written (R4 round 5, gaps row 69): every
    /// value with a marked segment (`sprig_sql::compat::stored::
    /// restore_original`), for statements that can read stored code.
    originals: bool,
    /// A traditional `EXPLAIN` (R4 round 3): `partitions` goes in at
    /// [`sprig_sql::compat::explain::PARTITIONS_AT`] (text rows).
    explain: bool,
    /// `EXPLAIN FORMAT=TREE` or `EXPLAIN ANALYZE` (R4 round 4): MariaDB's
    /// JSON plan rendered as MySQL's tree; `(analyze, outer LIMIT)`.
    tree: Option<(bool, Option<u64>)>,
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
            plan: PlanState::Unread,
            cur: None,
        }
    }

    /// Whether a statement can have a resultset worth checking: a query,
    /// `EXECUTE`, `CALL` or `TABLE`.
    pub(super) fn wanted(sql: &[u8]) -> bool {
        let t = sql.trim_ascii_start();
        let t = t.strip_prefix(b"(").unwrap_or(t).trim_ascii_start();
        let starts = |w: &[u8]| t.len() >= w.len() && t[..w.len()].eq_ignore_ascii_case(w);
        // `EXECUTE`, `CALL` and `TABLE`: no plan, but JSON values.
        starts(b"WITH")
            || starts(b"EXPLAIN")
            || starts(b"SELECT")
            || starts(b"EXECUTE")
            || starts(b"CALL")
            || starts(b"TABLE")
            || starts(b"SHOW")
    }

    /// `columns` as MySQL types them, if any changes.
    fn fixed(&mut self, columns: &[ColumnDef]) -> Option<Vec<ColumnDef>> {
        use sprig_sql::compat::intmeta;
        // `SHOW` is wanted only for `SHOW INDEX`'s expressions.
        if !intmeta::candidates(columns)
            || self
                .sql
                .trim_ascii_start()
                .get(..4)
                .is_some_and(|w| w.eq_ignore_ascii_case(b"SHOW"))
        {
            return None;
        }
        if matches!(self.plan, PlanState::Unread) {
            self.plan = PlanState::Read(intmeta::plan(self.sql, &self.lex));
        }
        let PlanState::Read(Some(plan)) = &self.plan else {
            return None;
        };
        let mut cols = columns.to_vec();
        intmeta::apply(plan, &mut cols).then_some(cols)
    }

    /// Starts a resultset: the client's columns, and the row state.
    fn start(&mut self, columns: &[ColumnDef], format: RowFormat) -> Option<Vec<ColumnDef>> {
        self.cur = None;
        // MySQL's twelve EXPLAIN columns (chapter 28, R4 round 3).
        // Binary rows too (a prepared EXPLAIN, R4 round 4).
        if sprig_sql::compat::explain::is_explain_extended(columns.iter().map(|c| &c.name[..])) {
            let after = explain_columns(columns);
            self.cur = Some(Rows {
                format,
                before: columns.to_vec(),
                after: after.clone(),
                widened: false,
                json: Vec::new(),
                exprs: Vec::new(),
                originals: false,
                explain: true,
                tree: None,
            });
            return Some(after);
        }
        // MySQL's tree formats from MariaDB's JSON (R4 round 4).
        if format == RowFormat::Text
            && columns.len() == 1
            && (&columns[0].name[..] == b"EXPLAIN" || &columns[0].name[..] == b"ANALYZE")
            && let Some(analyze) = sprig_sql::compat::explain::tree_request(self.sql, &self.lex)
        {
            let limit = sprig_sql::compat::explain::outer_limit(self.sql, &self.lex);
            let mut after = columns.to_vec();
            after[0].name = Bytes::from_static(b"EXPLAIN");
            self.cur = Some(Rows {
                format,
                before: columns.to_vec(),
                after: after.clone(),
                widened: false,
                json: Vec::new(),
                exprs: Vec::new(),
                originals: false,
                explain: false,
                tree: Some((analyze, limit)),
            });
            return Some(after);
        }
        let fixed = self.fixed(columns);
        let after = fixed.as_deref().unwrap_or(columns);
        let json: Vec<usize> = after
            .iter()
            .enumerate()
            .filter(|(_, c)| c.col_type == sprig_proto_mysql::resultset::column_type::JSON)
            .map(|(i, _)| i)
            .collect();
        let show = self
            .sql
            .trim_ascii_start()
            .get(..4)
            .is_some_and(|w| w.eq_ignore_ascii_case(b"SHOW"));
        let exprs: Vec<usize> = after
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                c.name.eq_ignore_ascii_case(b"expression")
                    && (show || c.table.eq_ignore_ascii_case(b"STATISTICS"))
            })
            .map(|(i, _)| i)
            .collect();
        let has = |w: &[u8]| self.sql.windows(w.len()).any(|x| x.eq_ignore_ascii_case(w));
        let originals = (show && has(b"CREATE"))
            || has(b"ROUTINE")
            || has(b"TRIGGER")
            || has(b"EVENT")
            || has(b"PROC");
        let widened = fixed.is_some() && format == RowFormat::Binary;
        if widened || !json.is_empty() || !exprs.is_empty() || originals {
            self.cur = Some(Rows {
                format,
                before: columns.to_vec(),
                after: after.to_vec(),
                widened,
                json,
                exprs,
                originals,
                explain: false,
                tree: None,
            });
        }
        fixed
    }
}

/// MariaDB's `EXPLAIN EXTENDED` columns as MySQL 8.4's `EXPLAIN` has
/// them: `partitions` (`MEDIUM_BLOB`) after `table`, `rows` a `LONGLONG`.
fn explain_columns(columns: &[ColumnDef]) -> Vec<ColumnDef> {
    use sprig_proto_mysql::resultset::column_type;
    let mut after = columns.to_vec();
    if let Some(rows) = after.get_mut(8) {
        rows.col_type = column_type::LONGLONG;
        rows.charset = 63;
        rows.length = 21;
        rows.decimals = 0;
        // NUM and BINARY, as MySQL sends them.
        rows.flags = (1 << 15) | (1 << 7);
    }
    let mut partitions = columns[2].clone();
    partitions.name = Bytes::from_static(b"partitions");
    partitions.org_name = Bytes::from_static(b"partitions");
    partitions.col_type = column_type::MEDIUM_BLOB;
    partitions.length = 25_264_128;
    partitions.flags = 1 << 4;
    after.insert(sprig_sql::compat::explain::PARTITIONS_AT, partitions);
    after
}

/// A prepared EXPLAIN's binary row as MySQL 8.4 sends it: `partitions`
/// (`NULL`) after `table`, and `rows` an integer (MariaDB sends it as
/// text). R4 round 4.
fn explain_binary(mut vals: Vec<sprig_proto_mysql::Value>) -> Vec<sprig_proto_mysql::Value> {
    use sprig_proto_mysql::Value;
    if let Some(v) = vals.get_mut(8)
        && let Value::Bytes(b) = v
        && let Some(n) = std::str::from_utf8(b)
            .ok()
            .and_then(|t| t.parse::<i64>().ok())
    {
        *v = Value::Int(n);
    }
    vals.insert(sprig_sql::compat::explain::PARTITIONS_AT, Value::Null);
    vals
}

/// One JSON value printed MySQL's way, if that changes it.
fn json_value(v: &[u8]) -> Option<Vec<u8>> {
    use sprig_sql::compat::jsontext;
    if !jsontext::may_change(v) {
        return None;
    }
    jsontext::canonical(v).filter(|c| c.as_slice() != v)
}

/// A functional key part's expression (MariaDB's text) as MySQL prints
/// it in `SHOW INDEX` and `STATISTICS`, if that changes it: its item text
/// with each `'` written `\'` (MySQL 8.4.11 does), MariaDB's text when the
/// printer doesn't know a construct.
fn expr_value(v: &[u8]) -> Option<Vec<u8>> {
    let t = std::str::from_utf8(v).ok()?;
    let p = sprig_sql::compat::exprprint::mysql_expr(t, "utf8mb4").unwrap_or_else(|| t.to_owned());
    let p = p.replace('\'', "\\'").into_bytes();
    (p.as_slice() != v).then_some(p)
}

impl Rows {
    /// The columns whose values may change.
    fn checked(&self) -> Vec<usize> {
        if self.originals {
            (0..self.after.len()).collect()
        } else {
            self.json.iter().chain(&self.exprs).copied().collect()
        }
    }

    /// Column `i`'s value `v` as the client gets it, if it changes.
    fn fix(&self, i: usize, v: &[u8]) -> Option<Vec<u8>> {
        if self.originals && sprig_sql::compat::stored::has_original(v) {
            return sprig_sql::compat::stored::restore_original(v);
        }
        if self.exprs.contains(&i) {
            expr_value(v)
        } else {
            json_value(v)
        }
    }

    /// A row as the client gets it; `None` when it is unchanged.
    fn rewrite(&self, payload: &Bytes) -> Option<Bytes> {
        use sprig_proto_mysql::Value;
        use sprig_proto_mysql::resultset::{
            decode_binary_row, decode_text_row, encode_binary_row, encode_text_row,
        };
        if let (RowFormat::Text, Some((analyze, limit))) = (self.format, self.tree) {
            let vals = decode_text_row(payload, 1).ok()?;
            let plan = vals.first().copied().flatten()?;
            let t = sprig_sql::compat::explain::tree(plan, analyze, limit)?;
            return Some(encode_text_row([Some(t.as_bytes())]));
        }
        match self.format {
            RowFormat::Text if self.explain => {
                let vals = decode_text_row(payload, self.before.len()).ok()?;
                let mut out: Vec<Option<&[u8]>> = vals;
                out.insert(sprig_sql::compat::explain::PARTITIONS_AT, None);
                Some(encode_text_row(out))
            }
            RowFormat::Text => {
                if self.json.is_empty() && self.exprs.is_empty() && !self.originals {
                    return None;
                }
                let vals = decode_text_row(payload, self.after.len()).ok()?;
                let mut fixed: Vec<Option<Vec<u8>>> = vec![None; vals.len()];
                let mut any = false;
                for i in self.checked() {
                    if let Some(Some(v)) = vals.get(i)
                        && let Some(c) = self.fix(i, v)
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
            RowFormat::Binary if self.explain => {
                let vals = decode_binary_row(payload, &self.before).ok()?;
                encode_binary_row(&explain_binary(vals), &self.after).ok()
            }
            RowFormat::Binary => {
                let mut vals = decode_binary_row(payload, &self.before).ok()?;
                let mut any = self.widened;
                for i in self.checked() {
                    if let Some(Value::Bytes(b)) = vals.get(i)
                        && let Some(c) = self.fix(i, b)
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
        if let (Some(cur), RowValues::Text(vals)) = (&self.cur, &values)
            && cur.explain
        {
            let mut out: Vec<Option<&[u8]>> = vals.to_vec();
            out.insert(sprig_sql::compat::explain::PARTITIONS_AT, None);
            return self.inner.row(RowValues::Text(&out)).await;
        }
        // A prepared EXPLAIN's decoded binary rows (R4 round 4).
        if let (Some(cur), RowValues::Binary(vals)) = (&self.cur, &values)
            && cur.explain
        {
            let out = explain_binary(vals.to_vec());
            return self.inner.row(RowValues::Binary(&out)).await;
        }
        if let (Some(cur), RowValues::Text(vals)) = (&self.cur, &values)
            && let Some((analyze, limit)) = cur.tree
            && let Some(Some(plan)) = vals.first()
            && let Some(t) = sprig_sql::compat::explain::tree(plan, analyze, limit)
        {
            return self.inner.row(RowValues::Text(&[Some(t.as_bytes())])).await;
        }
        // Rows the gateway built (sharded tenants): JSON text values.
        if let (Some(cur), RowValues::Text(vals)) = (&self.cur, &values)
            && !(cur.json.is_empty() && cur.exprs.is_empty() && !cur.originals)
        {
            let mut fixed: Vec<Option<Vec<u8>>> = vec![None; vals.len()];
            for i in cur.checked() {
                if let Some(Some(v)) = vals.get(i) {
                    fixed[i] = cur.fix(i, v);
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
    lex: sprig_sql::lex::LexOptions,
    cols: &mut Vec<ColumnDef>,
) {
    use sprig_sql::compat::intmeta;
    // A prepared EXPLAIN gets MySQL's twelve columns too (R4 round 4: PDO
    // compares it with the emulated one).
    if sprig_sql::compat::explain::is_explain_extended(cols.iter().map(|c| &c.name[..])) {
        *cols = explain_columns(cols);
        return;
    }
    if IntMetaSink::wanted(sql)
        && intmeta::candidates(cols)
        && let Some(plan) = intmeta::plan(sql, &lex)
    {
        intmeta::apply(&plan, cols);
    }
}

#[cfg(test)]
mod tests {
    use super::expr_value;

    #[test]
    fn functional_part_expressions_as_mysql_prints_them() {
        // R4 round 5 (gaps row 64): MySQL 8.4.11's `STATISTICS.EXPRESSION`
        // and `SHOW INDEX` text for the same parts.
        let p = |v: &str| expr_value(v.as_bytes()).map(|b| String::from_utf8(b).unwrap());
        assert_eq!(p("`a` * 2 + 1").as_deref(), Some("((`a` * 2) + 1)"));
        assert_eq!(
            p("concat(`s`,'x')").as_deref(),
            Some("concat(`s`,_utf8mb4\\'x\\')")
        );
        assert_eq!(
            p("cast(json_unquote(json_extract(`data`,'$.\"some_key\"')) as signed)").as_deref(),
            Some("cast(json_unquote(json_extract(`data`,_utf8mb4\\'$.\"some_key\"\\')) as signed)")
        );
        assert_eq!(p("lower(`title`)"), None);
    }
}
